import asyncio
import json
import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from typing import Any, Dict, List, Tuple, Optional

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery, pubsub_v1 # Updated
from google.oauth2 import service_account
from google.api_core import exceptions
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from shapely import wkt
from shapely.geometry import Point

# =======================
# Configuration & Setup
# =======================
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("post-process")

app = Flask(__name__)

PROJECT_ID = os.environ.get("GCP_PROJECT", "houzr-280014")
DATASET_ID = os.environ.get("DATASET_ID", "poc_binod")
PORT = int(os.environ.get("PORT", 8080))
VENEFICUS_SA_PATH = os.environ.get("VENEFICUS_SA_PATH", "/app/sa.json")

# Pub/Sub Configuration
TOPIC_ID = os.environ.get("DUPLICATION_TRIGGER_TOPIC", "poc-duplication-trigger-topic")
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

# GCS Cache Configuration
CACHE_BUCKET = "poc_enrich" 
GEOMETRY_CACHE_PATH = "parameter_files/geometry_cache.json"

TABLE_PROJECTS = f"{PROJECT_ID}.{DATASET_ID}.projects_newsworthy"
TABLE_EVENTS = f"{PROJECT_ID}.{DATASET_ID}.events_newsworthy"
TABLE_AREAS = f"{PROJECT_ID}.{DATASET_ID}.expansion_areas_newsworthy"
BQ_STAGING_TABLE = f"{PROJECT_ID}.{DATASET_ID}.poc_postprocessing_staging"

storage_client = storage.Client()
bq_client = bigquery.Client()

CACHE = {"municipalities": None, "centroids": None, "phases": None}
executor = ThreadPoolExecutor(max_workers=5)
geolocator = Nominatim(user_agent="poc-houzr-pipeline-v4")

# =======================
# GCS Distributed Lock
# =======================

def acquire_lock_gcs(bucket_name: str, blob_name: str) -> bool:
    t_lock_start = time.perf_counter()
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        if not blob: return False
        
        metadata = blob.metadata or {}
        current_status = metadata.get("status")
        
        if current_status in ["processing", "completed"]:
            logger.info(f"[lock-skip] {blob_name} status '{current_status}'. Skipping trigger.")
            return False

        metageneration = blob.metageneration
        blob.metadata = {**metadata, "status": "processing", "lock_at": datetime.utcnow().isoformat()}
        blob.patch(if_metageneration_match=metageneration)
        
        logger.info(f"[lock-acquire] Lock granted for {blob_name} in {time.perf_counter()-t_lock_start:.3f}s.")
        return True
    except exceptions.PreconditionFailed:
        logger.warning(f"[lock-race] Race condition: {blob_name} already locked.")
        return False
    except Exception as e:
        logger.error(f"[lock-error] {e}")
        return False

def release_lock_gcs(bucket_name: str, blob_name: str, status: str):
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)
        if blob:
            metadata = blob.metadata or {}
            blob.metadata = {**metadata, "status": status, "finished_at": datetime.utcnow().isoformat()}
            blob.patch()
            logger.info(f"[lock-release] Final status '{status}' for {blob_name}.")
    except Exception as e:
        logger.error(f"[lock-release-error] {e}")

# =======================
# Optimized Cache Logic
# =======================

def standardize_gm_names(name: str) -> str:
    if not name or not isinstance(name, str): return "Onbekend"
    mapping = {"Den Haag": "'s-Gravenhage", "Bergen (NH)": "Bergen (NH.)", "Den Bosch": "'s-Hertogenbosch", "Gemeente Den Haag": "'s-Gravenhage"}
    clean = name.replace("gemeente", "").replace("gem.", "").strip()
    return mapping.get(clean, clean)

def load_global_data():
    if CACHE["municipalities"]: return
    
    t_start = time.perf_counter()
    bucket = storage_client.bucket(CACHE_BUCKET)
    cache_blob = bucket.blob(GEOMETRY_CACHE_PATH)

    if cache_blob.exists():
        try:
            logger.info(f"[cache-load] Fetching cache from gs://{CACHE_BUCKET}/{GEOMETRY_CACHE_PATH}")
            data = json.loads(cache_blob.download_as_text())
            CACHE["municipalities"] = data["municipalities"]
            CACHE["centroids"] = data["centroids"]
            CACHE["phases"] = data["phases"]
            logger.info(f"[cache-success] GCS Cache loaded in {time.perf_counter() - t_start:.2f}s")
            return
        except Exception as e:
            logger.warning(f"[cache-fallback] GCS Cache read failed: {e}")

    logger.info("[cache-refresh] Building Cache from BigQuery...")
    t_bq_start = time.perf_counter()
    try:
        news_bucket = storage_client.bucket("newsradar")
        phases_blob = news_bucket.blob("parameter_files/phases.json")
        CACHE["phases"] = {p["name"]: p["situations"] for p in json.loads(phases_blob.download_as_text())["phases"]}

        if os.path.exists(VENEFICUS_SA_PATH):
            creds = service_account.Credentials.from_service_account_file(VENEFICUS_SA_PATH)
            v_client = bigquery.Client(credentials=creds, project="veneficus")
        else:
            v_client = bigquery.Client()
            
        rows_s = v_client.query("SELECT GM_NAAM, geometry FROM `veneficus.cbs.gemeente_shapes_2024`").result()
        CACHE["municipalities"] = {standardize_gm_names(r["GM_NAAM"]): r["geometry"] for r in rows_s}
        
        rows_c = bq_client.query(f"SELECT municipality, ST_Y(ST_CENTROID(geometry)) as lat, ST_X(ST_CENTROID(geometry)) as lon FROM `{PROJECT_ID}.hzr_cbs.city_center_shapes`").result()
        CACHE["centroids"] = {r["municipality"]: {"lat": float(r["lat"]), "lon": float(r["lon"])} for r in rows_c}

        cache_data = {"municipalities": CACHE["municipalities"], "centroids": CACHE["centroids"], "phases": CACHE["phases"]}
        cache_blob.upload_from_string(json.dumps(cache_data))
        logger.info(f"[cache-save] SUCCESS Created Cache. BQ Time: {time.perf_counter() - t_bq_start:.2f}s")
    except Exception as e:
        logger.error(f"[cache-error] Critical Cache Failure: {e}")

# =======================
# Processing Logic
# =======================

def clean_phase_sync(phase: str) -> str:
    if not phase or not CACHE["phases"]: return "Onbekend"
    if phase in CACHE["phases"]: return phase
    for p_name, situations in CACHE["phases"].items():
        if phase in situations or phase.startswith(p_name): return p_name
    return "Onbekend"

def geocode_sync(query: str) -> Tuple[Optional[float], Optional[float]]:
    try:
        loc = geolocator.geocode(query, timeout=5)
        if loc: return loc.latitude, loc.longitude
    except: pass
    return None, None

async def get_coordinates_async(location: str, municipality: str) -> Tuple[Optional[float], Optional[float]]:
    loop = asyncio.get_event_loop()
    if location:
        lat, lon = await loop.run_in_executor(executor, geocode_sync, f"{location}, {municipality}, Netherlands")
        if lat: return lat, lon
        lat, lon = await loop.run_in_executor(executor, geocode_sync, f"{location}, Netherlands")
        if lat and (50.5 < lat < 53.7) and (3.2 < lon < 7.3): return lat, lon
    c = CACHE["centroids"].get(municipality)
    return (c["lat"], c["lon"]) if c else (None, None)

async def process_single_item(doc_id: str, doc_meta: Dict, item: Dict, category: str) -> Optional[Dict]:
    muni = standardize_gm_names(item.get("municipality") or doc_meta.get("issuing_body"))
    loc_q = item.get("address") or item.get("name") if category == "project" else item.get("location") or item.get("event_name") if category == "event" else item.get("location_in_text") or item.get("name")
    
    lat, lon = await get_coordinates_async(loc_q, muni)
    
    wkt_poly = CACHE["municipalities"].get(muni)
    is_valid = False
    if lat and lon and wkt_poly:
        try:
            is_valid = Point(lon, lat).within(wkt.loads(wkt_poly))
        except: pass
    
    if category == "project" and not is_valid:
        logger.warning(f"[validation-fail] Project '{item.get('name')}' coords ({lat}, {lon}) outside {muni}.")
        return None 

    row = {
        "source_id": doc_id, "title": doc_meta.get("title"), "issuing_body": doc_meta.get("issuing_body"),
        "document_date": doc_meta.get("document_date"), "relevance": doc_meta.get("relevance"),
        "is_relevant": str(doc_meta.get("is_relevant")), "relevance_answer": doc_meta.get("relevance_answer"),
        "gcs_file_path": doc_meta.get("gcs_file_path"), "municipality": muni, "latitude": lat, "longitude": lon,
        "description": item.get("description"), "quotation": item.get("quotation"), "justification": item.get("justification"),
        "confidence_value": float(item.get("confidence_value") or 0.0), "inserted_at": datetime.utcnow().isoformat()
    }
    
    if category == "project":
        row.update({"name": item.get("name"), "phase": clean_phase_sync(item.get("phase")), "address": item.get("address"), "unit_count": int(item.get("unit_count") or 0)})
    elif category == "event":
        row.update({"event_name": item.get("event_name"), "event_type": item.get("event_type"), "location": item.get("location")})
    elif category == "area":
        row.update({"name": item.get("name"), "expansion_type": item.get("expansion_type"), "phase": item.get("phase"), "number_of_properties": str(item.get("number_of_properties") or ""), "location_in_text": item.get("location_in_text")})
    
    return row

# =======================
# Main Entry Point
# =======================

async def pipeline():
    t_pipeline_start = time.perf_counter()
    event = request.get_json(silent=True) or {}
    bucket, name = event.get("bucket"), event.get("name")
    if not name or not name.endswith(".json"): return jsonify({"status": "ignored"}), 200

    if not acquire_lock_gcs(bucket, name):
        return jsonify({"status": "skipped"}), 200

    doc_id = "unknown"
    try:
        load_global_data()
        
        blob = storage_client.bucket(bucket).blob(name)
        data = json.loads(await asyncio.get_event_loop().run_in_executor(None, blob.download_as_text))
        doc_id = data.get("id", "unknown")
        
        info = data.get("info", {})
        if isinstance(info, str): info = json.loads(info)
        
        projs, evts, areas = info.get("projects", []) + info.get("potential_projects", []), info.get("events", []), info.get("expansion_areas", [])
        
        tasks = [process_single_item(doc_id, data, p, "project") for p in projs] + \
                [process_single_item(doc_id, data, e, "event") for e in evts] + \
                [process_single_item(doc_id, data, a, "area") for a in areas]
        
        results = await asyncio.gather(*tasks)
        
        v_p = [r for r in results[:len(projs)] if r]
        v_e = [r for r in results[len(projs):len(projs)+len(evts)] if r]
        v_a = [r for r in results[len(projs)+len(evts):] if r]

        def finalize():
            try:
                insertion_success = True
                # 1. Main Data Insert
                for table, rows in [(TABLE_PROJECTS, v_p), (TABLE_EVENTS, v_e), (TABLE_AREAS, v_a)]:
                    if rows:
                        err = bq_client.insert_rows_json(table, rows)
                        if err: 
                            logger.error(f"[bq-error] {table}: {err}")
                            insertion_success = False

                # 2. Optimized Staging Insert
                staging_row = {
                    "document_id": doc_id, "json_file": name, "stage": "NEWSWORTHY",
                    "status": "SUCCESS" if insertion_success else "FAILED", 
                    "message": f"P:{len(v_p)}, E:{len(v_e)}, A:{len(v_a)}",
                    "created_at": datetime.utcnow().isoformat()
                }
                logger.info(f"[status-log-attempt] Writing heartbeat for {doc_id}...")
                err_staging = bq_client.insert_rows_json(BQ_STAGING_TABLE, [staging_row])
                
                # 3. Pub/Sub Conditional Trigger
                if insertion_success:
                    logger.info(f"[insert-success] BigQuery data successfully committed for {doc_id}")
                    
                    active_categories = []
                    if v_p: active_categories.append("project")
                    if v_e: active_categories.append("event")
                    if v_a: active_categories.append("area")

                    for cat in active_categories:
                        message_payload = {
                            "document_id": doc_id,
                            "category": cat,
                            "timestamp": datetime.utcnow().isoformat()
                        }
                        message_bytes = json.dumps(message_payload).encode("utf-8")
                        
                        # Publish message
                        future = publisher.publish(topic_path, data=message_bytes)
                        msg_id = future.result()
                        logger.info(f"[pubsub-sent] Triggered duplication check for {doc_id} category '{cat}' to topic '{TOPIC_ID}'. MsgID: {msg_id}")
                else:
                    logger.warning(f"[insert-failure] Pub/Sub trigger skipped for {doc_id} due to BigQuery errors")

                if err_staging:
                    logger.error(f"[status-log-error] BQ Error: {err_staging}")
                else:
                    logger.info(f"[status-log-success] Heartbeat confirmed in {BQ_STAGING_TABLE}")

                release_lock_gcs(bucket, name, "completed")
            except Exception as e:
                logger.error(f"[finalize-error] {str(e)}")

        await asyncio.get_event_loop().run_in_executor(None, finalize)

        logger.info(f"[pipeline-finish] {doc_id} TOTAL DURATION: {time.perf_counter() - t_pipeline_start:.2f}s.")
        return jsonify({"status": "ok"}), 200
        
    except Exception as e:
        logger.error(f"[pipeline-failure] {e}")
        try:
            fail_row = {
                "document_id": doc_id, "json_file": name, "stage": "NEWSWORTHY",
                "status": "FAILED", "message": str(e)[:250],
                "created_at": datetime.utcnow().isoformat()
            }
            bq_client.insert_rows_json(BQ_STAGING_TABLE, [fail_row])
            logger.info(f"[status-log-fail] Logged failure heartbeat")
        except: pass
        
        release_lock_gcs(bucket, name, "failed")
        return jsonify({"status": "error"}), 200

@app.route("/", methods=["POST"])
def handler(): return asyncio.run(pipeline())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)