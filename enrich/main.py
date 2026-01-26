import asyncio
import json
import os
import logging
import sys
import time
import hashlib
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

# Constants
TXT_BUCKET = os.environ.get("TXT_BUCKET", "poc_extracted")
ENRICH_BUCKET = os.environ.get("ENRICH_BUCKET", "poc_enrich")
STATUS_BUCKET = os.environ.get("STATUS_BUCKET", "poc_status")
BQ_ENRICHED_TABLE = os.environ.get("BQ_ENRICHED_TABLE", "houzr-280014.poc_binod.enriched")
BQ_STAGING_TABLE = os.environ.get("BQ_STAGING_TABLE", "houzr-280014.poc_binod.document_pipeline_staging")

storage_client = storage.Client()
bq_client = bigquery.Client()

PROMPT_CACHE = {}
PHASE_LIST_CACHE = None

# =======================
# Helper Logic & Lock
# =======================

def acquire_lock(doc_id: str) -> bool:
    blob = storage_client.bucket(STATUS_BUCKET).blob(f"locks/{doc_id}.lock")
    try:
        blob.upload_from_string(json.dumps({"ts": datetime.utcnow().isoformat()}), if_generation_match=0)
        return True
    except PreconditionFailed: return False

def standardize_gm_names(name: str) -> str:
    if not name or not isinstance(name, str): return "Onbekend"
    clean = name.lower().replace("gemeente", "").replace("gem.", "").strip()
    return clean.capitalize()

def extract_municipality_from_issuing_body(text: str) -> str:
    if not text: return "Onbekend"
    if "gemeente" in text.lower():
        parts = text.lower().split("gemeente")
        return standardize_gm_names(parts[-1].strip())
    return standardize_gm_names(text)

def _create_dedup_key(item: Dict) -> str:
    cat = str(item.get("category", "unknown")).lower().strip()
    details = item.get("details", {})
    if isinstance(details, list): details = details[0] if details else {}
    name = str(details.get("name") or details.get("event_name") or item.get("name") or "none").lower().strip()
    loc = str(details.get("location") or "none").lower().strip()
    return hashlib.md5(f"{cat}_{name}_{loc}".encode()).hexdigest()

def build_names_string(items: list, key: str) -> str:
    if not items: return None
    names = []
    for item in items:
        d = item.get("details", {})
        if isinstance(d, list): d = d[0] if d else {}
        n = d.get(key) or d.get("name") or d.get("event_name")
        if n: names.append(str(n).strip())
    return "; ".join(f"{i+1}. {n}" for i, n in enumerate(names)) if names else None

# =======================
# Caching & Extraction Logic
# =======================

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

async def run_incremental_relevance_llm(client, text: str, doc_id: str):
    """Beam Logic: Creates the detailed relevance summary."""
    tmpl = await get_cached_prompt("relevance_prompt.txt")
    prompt = tmpl.format(title="", document_text=text)
    resp = await client.aio.models.generate_content(model="gemini-2.0-flash", contents=prompt)
    ans = resp.text.strip()
    is_rel = "ja" in ans.lower()[:10]
    clean_rel = ans.replace("Ja, ", "").replace("ja, ", "").strip()
    return is_rel, ans, clean_rel

async def multi_label_classification_async(client, text: str, doc_id: str, municipality: str):
    """Beam Logic: Greedy discovery of all mentioned locations."""
    tmpl = await get_cached_prompt("classification_prompt.txt")
    context_rules = (
        f"Analyseer dit document voor de gemeente {municipality}. "
        "Benoem ELK project, gebied of event, inclusief voorbeelden uit het verleden (zoals Boeckhorst of Bronsgeest).\n\n"
    )
    resp = await client.aio.models.generate_content(
        model="gemini-2.0-flash", 
        contents=context_rules + tmpl.format(title="", document_text=text),
        config=GenerateContentConfig(response_mime_type="application/json", temperature=0.2)
    )
    try:
        data = json.loads(resp.text)
        return data if isinstance(data, list) else data.get("items", [])
    except: return []

async def extract_details_batched(client, text: str, items: list, doc_id: str, municipality: str):
    phases = await get_phase_list()
    prompts_map = {"project": "project_details_prompt.txt", "potential_project": "project_details_prompt.txt",
                   "expansion_area": "expansion_area_prompt.txt", "event": "event_details_prompt.txt"}
    
    async def beam_style_enrich(item):
        cat = str(item.get("category", "")).lower().strip()
        p_file = prompts_map.get(cat, "event_details_prompt.txt")
        tmpl = await get_cached_prompt(p_file)
        
        # Exact Beam Replacement logic
        prompt = tmpl.replace("{{MUNICIPALITY}}", municipality).format(
            evidence_quote=item.get("evidence_quote",""), 
            document_text=text, 
            phases=phases
        )
        try:
            resp = await client.aio.models.generate_content(
                model="gemini-2.0-flash",
                contents=prompt,
                config=GenerateContentConfig(response_mime_type="application/json", temperature=0.1)
            )
            details = json.loads(resp.text)
            if isinstance(details, list): details = details[0] if details else {}
            details["municipality"] = municipality
            item["details"] = details
        except: item["details"] = {}
        return item
    return await asyncio.gather(*[beam_style_enrich(i) for i in items])

# =======================
# Execution Pipeline
# =======================

async def process_document():
    start_time = time.perf_counter()
    client = genai.Client(http_options=HttpOptions(api_version="v1"), vertexai=True,
                          project=os.environ.get("GCP_PROJECT"), location="europe-west4")

    event = request.get_json(silent=True) or {}
    record = event.get("name")
    if not record or not record.endswith(".txt"): return jsonify({"status": "ignored"}), 200

    blob = storage_client.bucket(TXT_BUCKET).blob(record)
    await asyncio.get_event_loop().run_in_executor(None, blob.reload)
    
    meta = blob.metadata or {}
    doc_id = meta.get("document_id", "unknown")
    standard_municipality = extract_municipality_from_issuing_body(meta.get("issuing_body", "Onbekend"))
    
    logger.info(f"[doc={doc_id}] START: {record} | Municipality: {standard_municipality}")

    if not acquire_lock(doc_id):
        logger.warning(f"[doc={doc_id}] LOCKED: Skipping duplicate trigger.")
        return jsonify({"status": "locked"}), 200

    try:
        text = await asyncio.get_event_loop().run_in_executor(None, blob.download_as_text)
        logger.info(f"[doc={doc_id}] DOWNLOADED: {len(text)} characters.")

        is_rel, rel_ans, clean_rel = await run_incremental_relevance_llm(client, text, doc_id)
        logger.info(f"[doc={doc_id}] RELEVANCE: {is_rel}")
        
        final_results = []
        if is_rel:
            raw_items = await multi_label_classification_async(client, text, doc_id, standard_municipality)
            logger.info(f"[doc={doc_id}] DISCOVERED: Found {len(raw_items)} potential items.")

            if raw_items:
                enriched = await extract_details_batched(client, text, raw_items, doc_id, standard_municipality)
                seen_keys = set()
                for item in enriched:
                    key = _create_dedup_key(item)
                    if key not in seen_keys:
                        seen_keys.add(key)
                        final_results.append(item)
                
                logger.info(f"[doc={doc_id}] DEDUP: Kept {len(final_results)} items.")

        # Aggregation by category
        grouped = defaultdict(list)
        for r in final_results:
            grouped[str(r.get("category")).lower().strip()].append(r)

        # Beam-style structured info object
        info_payload = {
            "projects": [i['details'] for i in grouped['project']] + [i['details'] for i in grouped['potential_project']],
            "events": [i['details'] for i in grouped['event']],
            "expansion_areas": [i['details'] for i in grouped['expansion_area']]
        }

        final_payload = {
            "id": doc_id,
            "title": record.split('/')[-1],
            "issuing_body": standard_municipality,
            "document_date": meta.get("document_date", "Onbekend"),
            "relevance": clean_rel,
            "relevance_answer": rel_ans,
            "is_relevant": str(is_rel),
            "project_names": build_names_string(grouped['project'] + grouped['potential_project'], "name"),
            "event_names": build_names_string(grouped['event'], "event_name"),
            "expansion_area_names": build_names_string(grouped['expansion_area'], "name"),
            "info": json.dumps(info_payload, ensure_ascii=False),
            "gcs_file_path": f"gs://{TXT_BUCKET}/{record}",
            "processed_at": datetime.utcnow().isoformat()
        }

        def finalize():
            dest_blob = storage_client.bucket(ENRICH_BUCKET).blob(f"enriched/{record.replace('.txt','.json')}")
            dest_blob.upload_from_string(json.dumps(final_payload, ensure_ascii=False), content_type="application/json")
            bq_client.insert_rows_json(BQ_ENRICHED_TABLE, [final_payload])
            
            staging_row = {
                "document_id": doc_id, "txt": record, "enrich_status": "SUCCESS",
                "message": f"Items: {len(final_results)}", "created_at": datetime.utcnow().isoformat()
            }
            bq_client.insert_rows_json(BQ_STAGING_TABLE, [staging_row])

        await asyncio.get_event_loop().run_in_executor(None, finalize)
        
        duration = time.perf_counter() - start_time
        logger.info(f"[doc={doc_id}] COMPLETED in {duration:.2f}s")
        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logger.error(f"[doc={doc_id}] CRITICAL ERROR: {str(e)}", exc_info=True)
        return jsonify({"status": "error", "message": str(e)}), 200

@app.route("/", methods=["POST"])
def handler():
    return asyncio.run(process_document())

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)