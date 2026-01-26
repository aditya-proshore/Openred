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
from google.cloud import storage, bigquery
from google.oauth2 import service_account
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

# Environment Variables (with Defaults)
PROJECT_ID = os.environ.get("GCP_PROJECT", "houzr-280014")
DATASET_ID = os.environ.get("DATASET_ID", "poc_binod")
PORT = int(os.environ.get("PORT", 8080))
# Defaults to /app/sa.json, which matches the Docker COPY command
VENEFICUS_SA_PATH = os.environ.get("VENEFICUS_SA_PATH", "/app/sa.json")

# BigQuery Destination Tables
TABLE_PROJECTS = f"{PROJECT_ID}.{DATASET_ID}.projects_newsworthy"
TABLE_EVENTS = f"{PROJECT_ID}.{DATASET_ID}.events_newsworthy"
TABLE_AREAS = f"{PROJECT_ID}.{DATASET_ID}.expansion_areas_newsworthy"

# Clients
storage_client = storage.Client()
bq_client = bigquery.Client() # Default client for our project

# Global Caches & Workers
CACHE = {
    "municipalities": None, # {name: polygon_wkt}
    "centroids": None,      # {name: {lat, lon}}
    "phases": None          # {phase_name: [synonyms]}
}
executor = ThreadPoolExecutor(max_workers=5)
geolocator = Nominatim(user_agent="houzr-pipeline-production-v1")

# =======================
# Auth & Data Loading
# =======================

def get_veneficus_client() -> bigquery.Client:
    """
    Returns a BigQuery client specifically for the external Veneficus project.
    Uses the local Service Account JSON copied into the Docker image.
    """
    if os.path.exists(VENEFICUS_SA_PATH):
        try:
            creds = service_account.Credentials.from_service_account_file(VENEFICUS_SA_PATH)
            return bigquery.Client(credentials=creds, project="veneficus")
        except Exception as e:
            logger.error(f"[system] Failed to load Veneficus SA from {VENEFICUS_SA_PATH}: {e}")
    else:
        logger.warning(f"[system] Service Account file not found at {VENEFICUS_SA_PATH}. Trying default creds.")
    
    return bigquery.Client()

def standardize_gm_names(name: str) -> str:
    """Normalizes Dutch municipality names."""
    if not name or not isinstance(name, str): 
        return "Onbekend"
    
    mapping = {
        "Den Haag": "'s-Gravenhage", 
        "Bergen (NH)": "Bergen (NH.)", 
        "Den Bosch": "'s-Hertogenbosch",
        "Gemeente Den Haag": "'s-Gravenhage"
    }
    
    clean = name.replace("gemeente", "").replace("gem.", "").strip()
    return mapping.get(clean, clean)

def clean_phase_sync(phase: str) -> str:
    """Matches raw phase strings to standardized phases using the loaded cache."""
    if not phase or not CACHE["phases"]: 
        return "Onbekend"
    
    if phase in CACHE["phases"]: 
        return phase
        
    for p_name, situations in CACHE["phases"].items():
        if phase in situations or phase.startswith(p_name): 
            return p_name
            
    return "Onbekend"

def load_global_data():
    """Sync function to load shapes and centroids into memory once."""
    if CACHE["municipalities"] and CACHE["centroids"]:
        return

    logger.info("[system] Loading Global Geometry Cache...")
    t_start = time.perf_counter()

    # 1. Load Shapes (Using Veneficus Client)
    try:
        v_client = get_veneficus_client()
        q_shapes = "SELECT GM_NAAM, geometry FROM `veneficus.cbs.gemeente_shapes_2024`"
        rows_s = v_client.query(q_shapes).result()
        CACHE["municipalities"] = {standardize_gm_names(r["GM_NAAM"]): r["geometry"] for r in rows_s}
    except Exception as e:
        logger.error(f"[system] CRITICAL: Failed to load shapes: {e}")
        CACHE["municipalities"] = {}

    # 2. Load Centroids (Using Default Client)
    try:
        q_centers = """
            SELECT municipality, ST_Y(ST_CENTROID(geometry)) as lat, ST_X(ST_CENTROID(geometry)) as lon 
            FROM `houzr-280014.hzr_cbs.city_center_shapes`
        """
        rows_c = bq_client.query(q_centers).result()
        CACHE["centroids"] = {
            r["municipality"]: {"lat": float(r["lat"]), "lon": float(r["lon"])}
            for r in rows_c
        }
    except Exception as e:
        logger.error(f"[system] Failed to load centroids: {e}")
        CACHE["centroids"] = {}

    # 3. Load Phases (From GCS)
    try:
        b = storage_client.bucket("newsradar")
        d = json.loads(b.blob("parameter_files/phases.json").download_as_text())
        CACHE["phases"] = {p["name"]: p["situations"] for p in d["phases"]}
    except Exception as e:
        logger.warning(f"[system] Failed to load phases.json: {e}")
        CACHE["phases"] = {}
    
    duration = time.perf_counter() - t_start
    logger.info(f"[system] Cache Loaded in {duration:.2f}s: {len(CACHE['municipalities'])} Munis.")

# =======================
# Async Geocoding Logic
# =======================

def geocode_sync(query: str) -> Tuple[Optional[float], Optional[float]]:
    """Blocking geocode call with retry logic."""
    delay = 1.0
    for _ in range(2):
        try:
            loc = geolocator.geocode(query, timeout=5)
            if loc: 
                return loc.latitude, loc.longitude
        except (GeocoderTimedOut, GeocoderUnavailable):
            time.sleep(delay)
            delay *= 2
        except Exception:
            break
    return None, None

async def get_coordinates_async(location: str, municipality: str) -> Tuple[Optional[float], Optional[float]]:
    """Runs blocking geocode in thread pool to prevent blocking main loop."""
    loop = asyncio.get_event_loop()
    
    # Strategy 1: Specific Query
    if location:
        query = f"{location}, {municipality}, Netherlands"
        lat, lon = await loop.run_in_executor(executor, geocode_sync, query)
        if lat: return lat, lon
        
        # Strategy 2: Relaxed Query
        query_relaxed = f"{location}, Netherlands"
        lat, lon = await loop.run_in_executor(executor, geocode_sync, query_relaxed)
        # Check if result is roughly within NL bounds
        if lat and (50.5 < lat < 53.7) and (3.2 < lon < 7.3): 
             return lat, lon

    # Strategy 3: Municipality Centroid Fallback
    c = CACHE["centroids"].get(municipality)
    if c: 
        return c["lat"], c["lon"]
    
    return None, None

# =======================
# Item Processing
# =======================

async def process_single_item(doc_id: str, doc_meta: Dict, item: Dict, category: str) -> Optional[Dict]:
    """Async worker to standardize, geocode, and validate one item."""
    
    # 1. Standardize
    raw_muni = item.get("municipality") or doc_meta.get("issuing_body")
    municipality = standardize_gm_names(raw_muni)
    
    # 2. Identify Location String
    loc_query = None
    if category == "project": 
        loc_query = item.get("address") or item.get("name")
    elif category == "event": 
        loc_query = item.get("location") or item.get("event_name")
    elif category == "area": 
        loc_query = item.get("location_in_text") or item.get("name")

    # 3. Async Geocode
    lat, lon = await get_coordinates_async(loc_query, municipality)

    # 4. Validation (CPU Bound)
    # Projects MUST be geographically valid within the municipality shape
    wkt_poly = CACHE["municipalities"].get(municipality)
    is_valid_location = False
    
    if lat and lon and wkt_poly:
        try:
            is_valid_location = Point(lon, lat).within(wkt.loads(wkt_poly))
        except Exception:
            is_valid_location = False
    
    # Filter Logic: Projects strict, others lenient
    if category == "project" and not is_valid_location:
        return None 

    # 5. Build Row
    row = {
        "source_id": doc_id,
        "title": doc_meta.get("title"),
        "issuing_body": doc_meta.get("issuing_body"),
        "document_date": doc_meta.get("document_date"),
        "relevance": doc_meta.get("relevance"),
        "is_relevant": str(doc_meta.get("is_relevant")),
        "relevance_answer": doc_meta.get("relevance_answer"),
        "gcs_file_path": doc_meta.get("gcs_file_path"),
        "municipality": municipality,
        "latitude": lat,
        "longitude": lon,
        "description": item.get("description"),
        "quotation": item.get("quotation"),
        "justification": item.get("justification"),
        "confidence_value": float(item.get("confidence_value") or 0.0),
        "inserted_at": datetime.utcnow().isoformat()
    }

    # Category Specific Extensions
    if category == "project":
        row.update({
            "name": item.get("name"),
            "phase": clean_phase_sync(item.get("phase")),
            "address": item.get("address"),
            "unit_count": int(item.get("unit_count") or 0)
        })
    elif category == "event":
        row.update({
            "event_name": item.get("event_name"),
            "event_type": item.get("event_type"),
            "location": item.get("location")
        })
    elif category == "area":
        row.update({
            "name": item.get("name"),
            "expansion_type": item.get("expansion_type"),
            "phase": item.get("phase"),
            "number_of_properties": str(item.get("number_of_properties") or ""),
            "location_in_text": item.get("location_in_text")
        })
        
    return row

# =======================
# Main Pipeline
# =======================

async def pipeline():
    t_start = time.perf_counter()
    
    # 1. Parse Event
    event = request.get_json(silent=True) or {}
    bucket = event.get("bucket")
    name = event.get("name") # e.g., enriched/doc_hash.json
    
    if not name or not name.endswith(".json"):
        return jsonify({"status": "ignored", "reason": "not a json file"}), 200

    # 2. Load Data (Lazy)
    load_global_data()

    # 3. Fetch Enriched JSON
    try:
        blob = storage_client.bucket(bucket).blob(name)
        content = await asyncio.get_event_loop().run_in_executor(None, blob.download_as_text)
        data = json.loads(content)
    except Exception as e:
        logger.error(f"Failed to download/parse GCS file {name}: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500
    
    doc_id = data.get("id", "unknown")
    logger.info(f"[doc_id: {doc_id}] Processing file: {name}")

    # 4. Extract Items
    info_raw = data.get("info", {})
    if isinstance(info_raw, str): 
        info_raw = json.loads(info_raw)
    
    projects = info_raw.get("projects", []) + info_raw.get("potential_projects", [])
    events = info_raw.get("events", [])
    areas = info_raw.get("expansion_areas", [])
    
    # 5. Parallel Processing
    # Map back results by index since gather preserves order
    tasks = []
    tasks += [process_single_item(doc_id, data, p, "project") for p in projects]
    tasks += [process_single_item(doc_id, data, e, "event") for e in events]
    tasks += [process_single_item(doc_id, data, a, "area") for a in areas]
    
    if not tasks:
        logger.info(f"[doc_id: {doc_id}] No items found. Done.")
        return jsonify({"status": "ok", "items": 0}), 200

    results = await asyncio.gather(*tasks)
    
    # 6. Segregate Results
    valid_projects = []
    valid_events = []
    valid_areas = []
    
    cursor = 0
    # Collect Projects
    for _ in projects:
        if results[cursor]: valid_projects.append(results[cursor])
        cursor += 1
    # Collect Events
    for _ in events:
        if results[cursor]: valid_events.append(results[cursor])
        cursor += 1
    # Collect Areas
    for _ in areas:
        if results[cursor]: valid_areas.append(results[cursor])
        cursor += 1

    # 7. Insert to BigQuery (Parallel I/O)
    def bq_insert(table, rows):
        if not rows: return []
        errors = bq_client.insert_rows_json(table, rows)
        if errors: logger.error(f"BQ Insert Errors for {table}: {errors}")
        return errors

    loop = asyncio.get_event_loop()
    await asyncio.gather(
        loop.run_in_executor(None, bq_insert, TABLE_PROJECTS, valid_projects),
        loop.run_in_executor(None, bq_insert, TABLE_EVENTS, valid_events),
        loop.run_in_executor(None, bq_insert, TABLE_AREAS, valid_areas)
    )

    duration = time.perf_counter() - t_start
    logger.info(f"[doc_id: {doc_id}] COMPLETED in {duration:.2f}s. Valid: P={len(valid_projects)}, E={len(valid_events)}, A={len(valid_areas)}")
    
    return jsonify({"status": "ok"}), 200

@app.route("/", methods=["POST"])
def handler():
    """Wrapper to run async pipeline in Flask."""
    return asyncio.run(pipeline())

if __name__ == "__main__":
    # Ensure app runs on the port Cloud Run injects
    app.run(host="0.0.0.0", port=PORT)