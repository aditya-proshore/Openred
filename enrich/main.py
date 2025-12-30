import base64
import json
import os
import re
import tempfile
import logging
import sys
from datetime import datetime
from time import sleep
import random

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from google.api_core.exceptions import PreconditionFailed

# =======================
# Logging
# =======================
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s:%(name)s:%(message)s",
)
logger = logging.getLogger()

# =======================
# Instance identity
# =======================
INSTANCE_ID = f"{os.environ.get('K_REVISION','local')}-{os.environ.get('K_INSTANCE_ID','0')}"

# =======================
# App & Config
# =======================
app = Flask(__name__)

TXT_BUCKET = os.environ.get("TXT_BUCKET", "poc_extracted")
ENRICH_BUCKET = os.environ.get("ENRICH_BUCKET", "poc_enrich")
STATUS_BUCKET = os.environ.get("STATUS_BUCKET", "poc_status")
TXT_PREFIX = "extracted/"
ENRICH_PREFIX = "enriched/"
STATUS_PREFIX = "status/"
BQ_STAGING_TABLE = "houzr-280014.poc_binod.document_pipeline_staging"

storage_client = storage.Client()
bq_client = bigquery.Client()

# =======================
# Helpers
# =======================
def enrich_blob_name(txt_name):
    return ENRICH_PREFIX + txt_name.rsplit("/",1)[-1].replace(".txt",".json")

def status_marker_name(txt_name):
    return STATUS_PREFIX + txt_name.rsplit("/",1)[-1].replace(".txt",".status.json")

def write_status_marker(txt_name, status, message, document_id):
    payload = {
        "document_id": document_id,
        "txt": txt_name,
        "status": status,
        "message": message,
        "ts": datetime.utcnow().isoformat()
    }
    storage_client.bucket(STATUS_BUCKET).blob(status_marker_name(txt_name)).upload_from_string(
        json.dumps(payload), content_type="application/json"
    )

def enrich_dutch_text(text: str) -> dict:
    def find(pattern):
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(2).strip() if m else None
    sleep(random.uniform(0.05,0.2))  # simulated delay
    return {
        "project_name": find(r"(project|projectnaam)\s*[:\-]\s*(.+)"),
        "city": find(r"(stad|plaats|gemeente)\s*[:\-]\s*(.+)"),
        "organization": find(r"(organisatie|opdrachtgever)\s*[:\-]\s*(.+)"),
        "date": find(r"(datum|publicatie)\s*[:\-]\s*(.+)"),
        "budget": find(r"(budget)\s*[:\-]\s*([€\d\.,\s]+)"),
    }

def upload_json_to_gcs(blob_name, data, metadata):
    blob = storage_client.bucket(ENRICH_BUCKET).blob(blob_name)
    blob.metadata = metadata
    try:
        blob.upload_from_string(json.dumps(data), content_type="application/json", if_generation_match=0)
        return True
    except PreconditionFailed:
        return False

# =======================
# Direct insert into staging table (prevent duplicates)
# =======================
def insert_staging_row(document_id, txt_name, status, message):
    # Check if document_id already exists
    check_query = f"""
    SELECT 1 FROM `{BQ_STAGING_TABLE}`
    WHERE document_id = @document_id
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("document_id", "STRING", document_id)]
    )
    result = bq_client.query(check_query, job_config=job_config).result()
    if result.total_rows > 0:
        logger.info(f"[doc={document_id}] already exists in staging, skipping insert")
        return

    # Insert row
    row = {
        "document_id": document_id,
        "txt": txt_name,
        "enrich_status": status,
        "message": message,
        "created_at": datetime.utcnow().isoformat(),
        "updated_at": datetime.utcnow().isoformat()
    }
    errors = bq_client.insert_rows_json(BQ_STAGING_TABLE, [row])
    if errors:
        raise RuntimeError(f"BQ staging insert failed: {errors}")
    logger.info(f"[doc={document_id}] inserted into staging table: {status}")

# =======================
# Cloud Run handler
# =======================
@app.route("/", methods=["POST"])
def handler():
    event = request.get_json(silent=True) or {}
    record = event.get("name") or event.get("data", {}).get("name")
    if not record or not record.startswith(TXT_PREFIX) or not record.endswith(".txt"):
        return jsonify({"status": "ignored"}), 200

    blob = storage_client.bucket(TXT_BUCKET).blob(record)
    blob.reload()
    metadata = blob.metadata or {}
    document_id = metadata.get("document_id")
    out_name = enrich_blob_name(record)

    # Skip if enrichment already exists
    if storage_client.bucket(ENRICH_BUCKET).blob(out_name).exists():
        return jsonify({"status": "skipped"}), 200

    try:
        tmp = tempfile.mkdtemp()
        local = os.path.join(tmp, os.path.basename(record))
        blob.download_to_filename(local)

        with open(local, "r", encoding="utf-8") as f:
            enriched = enrich_dutch_text(f.read())

        if upload_json_to_gcs(out_name, enriched, {**metadata, "stage":"ENRICH"}):
            write_status_marker(record, "ENRICHED", out_name, document_id)
            insert_staging_row(document_id, record, "SUCCESS", "enriched JSON uploaded")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        write_status_marker(record, "FAILED", str(e), document_id)
        try:
            insert_staging_row(document_id, record, "FAILED", str(e))
        except Exception as bq_err:
            logger.exception(f"[doc={document_id}] Failed to insert FAILED row: {bq_err}")
        return jsonify({"status": "failed"}), 200

# =======================
# Local run
# =======================
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
