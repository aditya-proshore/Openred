import asyncio
import json
import os
import logging
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Tuple
from collections import defaultdict

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from google.api_core.exceptions import PreconditionFailed
from google import genai
from google.genai.types import GenerateContentConfig, HttpOptions

# =======================
# Configuration & Clients
# =======================
logging.basicConfig(level=logging.INFO, stream=sys.stdout, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("document-pipeline")

app = Flask(__name__)

# Constants from Environment
TXT_BUCKET = os.environ.get("TXT_BUCKET", "poc_extracted")
ENRICH_BUCKET = os.environ.get("ENRICH_BUCKET", "poc_enrich")
STATUS_BUCKET = os.environ.get("STATUS_BUCKET", "poc_status")
BQ_STAGING_TABLE = os.environ.get("BQ_STAGING_TABLE", "houzr-280014.poc_binod.document_pipeline_staging")
BQ_ENRICHED_TABLE = os.environ.get("BQ_ENRICHED_TABLE", "houzr-280014.poc_binod.enriched")

storage_client = storage.Client()
bq_client = bigquery.Client()

PROMPT_CACHE = {}
PHASE_LIST_CACHE = None

# =======================
# Robust Helpers
# =======================

def build_names_string(items: list, key: str) -> str:
    """Safely extracts names from items, handling cases where 'details' is a list."""
    if not items or not isinstance(items, list): 
        return None
        
    names = []
    for item in items:
        if not isinstance(item, dict): continue
        details = item.get("details", {})
        
        # Normalize details to dict
        if isinstance(details, list):
            details = details[0] if len(details) > 0 else {}
        elif not isinstance(details, dict):
            details = {}

        name = (
            details.get(key) or 
            details.get("name") or 
            details.get("event_name") or 
            item.get("name") or 
            item.get("brief_description")
        )
        if name:
            names.append(str(name).strip())
            
    return "; ".join(f"{i + 1}. {n}" for i, n in enumerate(names)) if names else None

async def get_cached_prompt(filename: str) -> str:
    if filename not in PROMPT_CACHE:
        loop = asyncio.get_event_loop()
        blob = storage_client.bucket("newsradar").blob(f"parameter_files/prompts/{filename}")
        content = await loop.run_in_executor(None, blob.download_as_text)
        PROMPT_CACHE[filename] = content
    return PROMPT_CACHE[filename]

async def get_phase_list() -> str:
    global PHASE_LIST_CACHE
    if PHASE_LIST_CACHE is None:
        try:
            loop = asyncio.get_event_loop()
            blob = storage_client.bucket("newsradar").blob("parameter_files/phases.json")
            content = await loop.run_in_executor(None, blob.download_as_text)
            PHASE_LIST_CACHE = content
        except: PHASE_LIST_CACHE = "Onbekend"
    return PHASE_LIST_CACHE

def acquire_lock(doc_id: str) -> bool:
    blob = storage_client.bucket(STATUS_BUCKET).blob(f"locks/{doc_id}.lock")
    try:
        blob.upload_from_string(json.dumps({"ts": datetime.utcnow().isoformat()}),
                                content_type="application/json", if_generation_match=0)
        return True
    except PreconditionFailed: return False

# =======================
# Logic Steps
# =======================

async def run_incremental_relevance_llm(client, text: str, doc_id: str):
    chunk_size = 1500
    tmpl = await get_cached_prompt("relevance_prompt.txt")
    for i in range(0, len(text), chunk_size):
        chunk = text[i : i + chunk_size]
        prompt = tmpl.format(title="", document_text=chunk)
        resp = await client.aio.models.generate_content(model="gemini-2.0-flash-lite", contents=prompt)
        ans = resp.text.strip()
        if "ja" in ans.lower():
            logger.info(f"[doc={doc_id}] RELEVANCE: Ja identified at chunk index {i}")
            return True, ans, ans.replace("Ja, ", "").replace("ja, ", "").strip()
    return False, "Nee", "Nee"

async def multi_label_classification_async(client, text: str, doc_id: str):
    tmpl = await get_cached_prompt("classification_prompt.txt")
    resp = await client.aio.models.generate_content(
        model="gemini-2.5-flash", 
        contents=tmpl.format(title="", document_text=text),
        config=GenerateContentConfig(response_mime_type="application/json", temperature=0)
    )
    try:
        data = json.loads(resp.text)
        items = data if isinstance(data, list) else data.get("items", [])
        logger.info(f"[doc={doc_id}] MLC: Identified {len(items)} items")
        return items
    except: 
        logger.error(f"[doc={doc_id}] MLC: Failed to parse JSON")
        return []

async def extract_details_batched(client, text: str, items: list, doc_id: str):
    phases = await get_phase_list()
    prompts = {
        "project": "project_details_prompt.txt", 
        "potential_project": "project_details_prompt.txt",
        "expansion_area": "expansion_area_prompt.txt", 
        "event": "event_details_prompt.txt"
    }
    async def enrich(item):
        cat = str(item.get("category", "")).lower().strip()
        p_file = prompts.get(cat) or "event_details_prompt.txt"
        tmpl = await get_cached_prompt(p_file)
        try:
            resp = await client.aio.models.generate_content(
                model="gemini-2.5-flash",
                contents=tmpl.format(evidence_quote=item.get("evidence_quote",""), document_text=text, phases=phases),
                config=GenerateContentConfig(response_mime_type="application/json")
            )
            item["details"] = json.loads(resp.text)
            logger.info(f"[doc={doc_id}] DETAIL: Extracted {cat}")
        except Exception as e: 
            logger.error(f"[doc={doc_id}] Detail Error ({cat}): {e}")
        return item
    return await asyncio.gather(*[enrich(i) for i in items])

# =======================
# Main Processing
# =======================

async def process_document():
    start_time = time.perf_counter()
    client = genai.Client(http_options=HttpOptions(api_version="v1"), vertexai=True,
                          project=os.environ.get("GCP_PROJECT"), location=os.environ.get("GCP_REGION", "europe-west4"))

    event = request.get_json(silent=True) or {}
    record = event.get("name")
    if not record or not record.endswith(".txt"): 
        return jsonify({"status": "ignored"}), 200

    blob = storage_client.bucket(TXT_BUCKET).blob(record)
    await asyncio.get_event_loop().run_in_executor(None, blob.reload)
    doc_id = (blob.metadata or {}).get("document_id", "unknown")
    
    logger.info(f"[doc={doc_id}] PIPELINE START: {record}")

    if not acquire_lock(doc_id): 
        logger.warning(f"[doc={doc_id}] LOCK: Document is currently being processed elsewhere")
        return jsonify({"status": "locked"}), 200

    try:
        text = await asyncio.get_event_loop().run_in_executor(None, blob.download_as_text)
        logger.info(f"[doc={doc_id}] DOWNLOAD: {len(text)} characters retrieved")

        is_rel, rel_ans, clean_rel = await run_incremental_relevance_llm(client, text, doc_id)
        
        results = []
        if is_rel:
            items = await multi_label_classification_async(client, text, doc_id)
            if items:
                results = await extract_details_batched(client, text, items, doc_id)
        else:
            logger.info(f"[doc={doc_id}] RELEVANCE: Skipping - marked as irrelevant")

        # AGGREGATION
        grouped = defaultdict(list)
        issuing_body = "Onbekend"
        doc_date = "Onbekend"
        
        for r in results:
            cat = str(r.get("category", "event")).lower().strip()
            grouped[cat].append(r)
            # Try to grab municipality/date from the first valid detail found
            det = r.get("details", {})
            if isinstance(det, list) and det: det = det[0]
            if isinstance(det, dict):
                issuing_body = det.get("municipality") or issuing_body
                doc_date = det.get("date") or doc_date

        final_payload = {
            "id": doc_id,
            "title": record.split('/')[-1], # Filename only
            "issuing_body": issuing_body,
            "document_date": doc_date,
            "relevance": clean_rel,
            "relevance_answer": rel_ans,
            "is_relevant": str(is_rel),
            "project_names": build_names_string(grouped.get("project", []), "name"),
            "potential_project_names": build_names_string(grouped.get("potential_project", []), "name"),
            "event_names": build_names_string(grouped.get("event", []), "event_name"),
            "expansion_area_names": build_names_string(grouped.get("expansion_area", []), "name"),
            "info": json.dumps(results, ensure_ascii=False) if results else "No",
            "gcs_file_path": f"gs://{TXT_BUCKET}/{record}",
            "processed_at": datetime.utcnow().isoformat()
        }

        # FINALIZATION: GCS Upload and BQ Inserts
        def finalize():
            # 1. Upload JSON to GCS
            dest_blob = storage_client.bucket(ENRICH_BUCKET).blob(f"enriched/{record.replace('.txt','.json')}")
            dest_blob.upload_from_string(json.dumps(final_payload, ensure_ascii=False), content_type="application/json")
            logger.info(f"[doc={doc_id}] GCS: Enriched JSON uploaded")

            # 2. Insert into ENRICHED table
            errs_enrich = bq_client.insert_rows_json(BQ_ENRICHED_TABLE, [final_payload])
            if errs_enrich: logger.error(f"[doc={doc_id}] BQ ENRICH ERR: {errs_enrich}")
            else: logger.info(f"[doc={doc_id}] BQ ENRICH: Successfully inserted")

            # 3. Insert into STAGING table (Status Log)
            staging_row = {
                "document_id": doc_id, "txt": record, "enrich_status": "SUCCESS",
                "message": f"Rel: {is_rel} | Items: {len(results)}", "created_at": datetime.utcnow().isoformat()
            }
            errs_staging = bq_client.insert_rows_json(BQ_STAGING_TABLE, [staging_row])
            if errs_staging: logger.error(f"[doc={doc_id}] BQ STAGING ERR: {errs_staging}")

        await asyncio.get_event_loop().run_in_executor(None, finalize)
        
        duration = time.perf_counter() - start_time
        logger.info(f"[doc={doc_id}] COMPLETED in {duration:.2f}s")
        return jsonify({"status": "ok", "duration": duration}), 200

    except Exception as e:
        logger.error(f"[doc={doc_id}] PIPELINE CRITICAL ERROR: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 200

@app.route("/", methods=["POST"])
def handler():
    return asyncio.run(process_document())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)