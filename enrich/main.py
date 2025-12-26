import base64
import json
import os
import re
import tempfile
import logging
import time
from datetime import datetime

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery

# ------------------------
# Logging
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("enrich")

# ------------------------
# App setup
# ------------------------
app = Flask(__name__)
storage_client = storage.Client()
bq_client = bigquery.Client()

# ------------------------
# Config
# ------------------------
BUCKET = os.environ.get("BUCKET_NAME", "poc_binod_nl_pdfs")
IN_PREFIX = "extracted/"
OUT_PREFIX = "enriched/"
STATUS_TABLE_ID = f"{bq_client.project}.poc_binod.document_pipeline_status"

# ------------------------
# Simple Dutch enrichment
# ------------------------
def enrich_dutch_text(text: str) -> dict:
    def find(pattern):
        m = re.search(pattern, text, re.IGNORECASE)
        return m.group(1).strip() if m else None

    return {
        "project_name": find(r"(project|projectnaam)\s*[:\-]\s*(.+)"),
        "city": find(r"(stad|plaats|gemeente)\s*[:\-]\s*(.+)"),
        "organization": find(r"(organisatie|opdrachtgever)\s*[:\-]\s*(.+)"),
        "date": find(r"(datum|publicatie)\s*[:\-]\s*(.+)"),
        "budget": find(r"(budget)\s*[:\-]\s*([€\d\.,\s]+)")
    }

# ------------------------
# BigQuery update with retry
# ------------------------
def update_bq_status(document_id, status, message="", max_retries=3):
    query = f"""
    UPDATE {STATUS_TABLE_ID}
    SET enrich_status = @status,
        message = @message,
        updated_at = CURRENT_TIMESTAMP()
    WHERE document_id = @document_id
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("document_id", "STRING", document_id),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("message", "STRING", message),
        ]
    )
    for attempt in range(1, max_retries + 1):
        job = bq_client.query(query, job_config=job_config)
        job.result()
        affected = job.num_dml_affected_rows or 0
        if affected > 0:
            logger.info(f"[doc={document_id}] BQ STATUS UPDATED: {status} (attempt {attempt})")
            return True
        logger.warning(f"[doc={document_id}] BQ update affected 0 rows (attempt {attempt}/{max_retries})")
        time.sleep(0.5 * (2 ** (attempt - 1)))
    logger.error(f"[doc={document_id}] BQ STATUS UPDATE FAILED after {max_retries} retries")
    return False

# ------------------------
# Cloud Run handler
# ------------------------
@app.route("/", methods=["POST"])
def handler():
    event = request.get_json(silent=True) or {}
    bucket = None
    record = None
    document_id = None
    source_pdf = None

    try:
        # Event parsing
        if "bucket" in event and "name" in event:
            bucket = event["bucket"]
            record = event["name"]
        elif event.get("type") and event.get("data"):
            bucket = event["data"].get("bucket")
            record = event["data"].get("name")
        elif "message" in event and "data" in event["message"]:
            payload = json.loads(base64.b64decode(event["message"]["data"]))
            bucket = payload.get("bucket")
            record = payload.get("name")

        if not bucket or not record:
            logger.info("Ignored event with no bucket/name")
            return jsonify({"status": "ignored"}), 200

        if not record.startswith(IN_PREFIX) or not record.lower().endswith(".txt"):
            logger.info(f"Ignored non-txt object: {record}")
            return jsonify({"status": "ignored"}), 200

        blob = storage_client.bucket(bucket).blob(record)
        blob.reload()
        metadata = blob.metadata or {}
        document_id = metadata.get("document_id")
        source_pdf = metadata.get("source_pdf")
        log_prefix = f"[doc={document_id}]" if document_id else "[doc=unknown]"

        logger.info(f"{log_prefix} TXT RECEIVED: {record}")

        # Download
        with tempfile.TemporaryDirectory() as tmp:
            local_txt = os.path.join(tmp, record.split("/")[-1])
            blob.download_to_filename(local_txt)
            logger.info(f"{log_prefix} TXT DOWNLOADED")

            # Read and enrich
            with open(local_txt, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
            enriched = enrich_dutch_text(text)
            logger.info(f"{log_prefix} TXT ENRICHED")

            # Upload enriched JSON
            out_name = f"{OUT_PREFIX}{record.split('/')[-1].replace('.txt', '.json')}"
            out_blob = storage_client.bucket(bucket).blob(out_name)
            out_blob.metadata = {
                "document_id": document_id,
                "source_txt": record,
                "source_pdf": source_pdf,
                "stage": "ENRICH",
            }
            out_blob.upload_from_string(
                json.dumps({
                    "document_id": document_id,
                    "source_pdf": source_pdf,
                    "source_txt": record,
                    "bucket": bucket,
                    "language": "nl",
                    "generated_at": datetime.utcnow().isoformat(),
                    "data": enriched,
                }, indent=2, ensure_ascii=False),
                content_type="application/json",
            )
            logger.info(f"{log_prefix} JSON UPLOADED TO BUCKET: {out_name}")

            # Update status table
            if document_id:
                update_bq_status(document_id, "SUCCESS", "enrich completed")

        return jsonify({"status": "ok", "document_id": document_id}), 200

    except Exception as e:
        logger.exception(f"{log_prefix} ENRICH FAILED")
        if document_id:
            update_bq_status(document_id, "FAILED", str(e))
        return jsonify({"status": "error"}), 200
