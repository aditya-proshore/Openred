import base64
import json
import logging
import random
import time
from datetime import datetime
from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from google.api_core.exceptions import BadRequest

# ------------------------
# Logging
# ------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("json_to_bq")

# ------------------------
# App setup
# ------------------------
app = Flask(__name__)
storage_client = storage.Client()
bq_client = bigquery.Client()

IN_PREFIX = "enriched/"
TABLE_ID = f"{bq_client.project}.poc_binod.enrich_projects"
STATUS_TABLE_ID = f"{bq_client.project}.poc_binod.document_pipeline_status"

# ------------------------
# Status update helper with retry
# ------------------------
def update_bq_status_with_retry(document_id, status, message, retries=3):
    query = f"""
    UPDATE {STATUS_TABLE_ID}
    SET bq_inserted_status=@status,
        message=@message,
        updated_at=CURRENT_TIMESTAMP()
    WHERE document_id=@document_id
    """
    cfg = bigquery.QueryJobConfig(query_parameters=[
        bigquery.ScalarQueryParameter("document_id", "STRING", document_id),
        bigquery.ScalarQueryParameter("status", "STRING", status),
        bigquery.ScalarQueryParameter("message", "STRING", message),
    ])
    for attempt in range(1, retries + 1):
        try:
            job = bq_client.query(query, cfg)
            job.result(timeout=5)
            if job.num_dml_affected_rows:
                logger.info(f"[doc={document_id}] STATUS TABLE UPDATED: {status} (attempt {attempt})")
                return
            else:
                logger.warning(f"[doc={document_id}] BQ update affected 0 rows (attempt {attempt}/{retries})")
        except BadRequest as e:
            if "Too many DML" in str(e):
                logger.warning(f"[doc={document_id}] DML LIMIT — skipping")
                return
            raise
        time.sleep(0.3 * (2 ** (attempt - 1)) + random.random() * 0.2)
    logger.error(f"[doc={document_id}] STATUS TABLE UPDATE FAILED after {retries} retries")

# ------------------------
# Cloud Run handler
# ------------------------
@app.route("/", methods=["POST"])
def handler():
    event = request.get_json(silent=True) or {}
    bucket = None
    record = None
    document_id = None

    try:
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

        if not record.startswith(IN_PREFIX) or not record.endswith(".json"):
            logger.info(f"Ignored non-json object: {record}")
            return jsonify({"status": "ignored"}), 200

        blob = storage_client.bucket(bucket).blob(record)
        blob.reload()
        metadata = blob.metadata or {}
        document_id = metadata.get("document_id")
        log_prefix = f"[doc={document_id}]" if document_id else "[doc=unknown]"

        logger.info(f"{log_prefix} JSON RECEIVED: {record}")

        payload = json.loads(blob.download_as_text())
        logger.info(f"{log_prefix} JSON DOWNLOADED")

        row = {
            "source_txt": payload.get("source_txt"),
            "bucket": payload.get("bucket"),
            "language": payload.get("language"),
            "generated_at": payload.get("generated_at"),
            "project_name": payload.get("data", {}).get("project_name"),
            "city": payload.get("data", {}).get("city"),
            "organization": payload.get("data", {}).get("organization"),
            "date": payload.get("data", {}).get("date"),
            "budget": payload.get("data", {}).get("budget"),
            "ingested_at": datetime.utcnow().isoformat(),
        }

        errors = bq_client.insert_rows_json(TABLE_ID, [row])
        if errors:
            logger.error(f"{log_prefix} BQ INSERT ERRORS: {errors}")
            if document_id:
                update_bq_status_with_retry(document_id, "FAILED", "bq insert failed")
        else:
            logger.info(f"{log_prefix} BQ INSERT SUCCESS")
            if document_id:
                update_bq_status_with_retry(document_id, "SUCCESS", "bq insert completed")

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logger.exception(f"{log_prefix} BQ INGEST FAILED")
        if document_id:
            update_bq_status_with_retry(document_id, "FAILED", str(e))
        return jsonify({"status": "error"}), 200
