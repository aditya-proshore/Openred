import base64
import json
import os
import tempfile
import hashlib
import logging
import time
from datetime import datetime

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from pypdf import PdfReader
from google.api_core.exceptions import PreconditionFailed

# -----------------------
# App + logging
# -----------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract")

# -----------------------
# Config
# -----------------------
RAW_BUCKET = os.environ.get("RAW_BUCKET", "poc_raw_binod")
EXTRACTED_BUCKET = os.environ.get("EXTRACTED_BUCKET", "poc_extracted")
STATUS_BUCKET = os.environ.get("STATUS_BUCKET", "poc_status")

RAW_PREFIX = "raw/"
OUT_PREFIX = "extracted/"
STATUS_PREFIX = "extract/" # As requested: poc_status/extract/

BQ_TABLE_ID = "houzr-280014.poc_binod.document_pipeline_status"

storage_client = storage.Client()
bq_client = bigquery.Client()

# -----------------------
# Helpers
# -----------------------
def make_document_id(bucket, name, generation):
    raw = f"{bucket}/{name}:{generation}"
    return hashlib.sha1(raw.encode()).hexdigest()

def out_txt_name(pdf_name):
    return OUT_PREFIX + pdf_name.rsplit("/", 1)[-1].replace(".pdf", ".txt")

def status_marker_name(pdf_name):
    return STATUS_PREFIX + pdf_name.rsplit("/", 1)[-1].replace(".pdf", ".status.json")

def has_terminal_marker(pdf_name):
    return storage_client.bucket(STATUS_BUCKET).blob(status_marker_name(pdf_name)).exists()

def has_been_extracted(pdf_name):
    return storage_client.bucket(EXTRACTED_BUCKET).blob(out_txt_name(pdf_name)).exists()

def write_status_marker(pdf_name, status, message="", document_id=None):
    payload = {
        "document_id": document_id,
        "pdf": pdf_name,
        "status": status,
        "message": message,
        "ts": datetime.utcnow().isoformat(),
    }
    storage_client.bucket(STATUS_BUCKET).blob(
        status_marker_name(pdf_name)
    ).upload_from_string(json.dumps(payload), content_type="application/json")

def extract_pdf_to_text(local_path):
    reader = PdfReader(local_path)
    texts = []
    for page in reader.pages:
        txt = page.extract_text()
        if txt:
            texts.append(txt)
    return "\n\n".join(texts)

# -----------------------
# Race-safe BigQuery insert
# -----------------------
def insert_extract_status_safe(document_id, pdf_name, status, message=""):
    """Insert row with explicit logging to debug missing records"""
    try:
        # 1. Check if exists
        check_query = f"SELECT document_id FROM `{BQ_TABLE_ID}` WHERE document_id = @document_id LIMIT 1"
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("document_id", "STRING", document_id)]
        )
        
        query_job = bq_client.query(check_query, job_config=job_config)
        result = query_job.result()

        if result.total_rows > 0:
            logger.info(f"[doc={document_id}] BQ VERIFY: Row already exists. Skipping insert.")
            return False

        # 2. Insert if not exists
        insert_query = f"""
        INSERT INTO `{BQ_TABLE_ID}` 
        (document_id, pdf_name, extracted_status, enrich_status, bq_inserted_status,
         extracted_retries, enrich_retries, bq_insert_retries, message, created_at, updated_at)
        VALUES (@document_id, @pdf_name, @status, "PENDING", "PENDING", 0, 0, 0, @message, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
        """
        insert_job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("document_id", "STRING", document_id),
                bigquery.ScalarQueryParameter("pdf_name", "STRING", pdf_name),
                bigquery.ScalarQueryParameter("status", "STRING", status),
                bigquery.ScalarQueryParameter("message", "STRING", message),
            ]
        )

        bq_client.query(insert_query, job_config=insert_job_config).result()
        logger.info(f"[doc={document_id}] BQ INSERT SUCCESS: Record created for {pdf_name}")
        return True

    except Exception as e:
        # This will now print the EXACT reason why BQ is failing (Permissions, Quota, or Table Schema)
        logger.error(f"[doc={document_id}] BQ INSERT FAILED: {str(e)}")
        return False

# -----------------------
# Download / Upload
# -----------------------
def download_blob_to_file(bucket_name, blob_name, file_path):
    blob = storage_client.bucket(bucket_name).get_blob(blob_name)
    if blob:
        blob.download_to_filename(file_path)
    return blob

def upload_text_to_gcs(blob_name, text, metadata=None):
    blob = storage_client.bucket(EXTRACTED_BUCKET).blob(blob_name)
    blob.metadata = metadata or {}
    try:
        blob.upload_from_string(text, content_type="text/plain", if_generation_match=0)
        return True
    except PreconditionFailed:
        return False

# -----------------------
# Cloud Run Handler
# -----------------------
@app.route("/", methods=["POST"])
def handler():
    event = request.get_json(silent=True) or {}
    bucket = None
    record = None
    generation = event.get("generation")

    # Parse event
    if "bucket" in event and "name" in event:
        bucket = event["bucket"]
        record = event["name"]
    elif event.get("type") and event.get("data"):
        data = event["data"]
        bucket = data.get("bucket")
        record = data.get("name")
        generation = data.get("generation")
    elif "message" in event and "data" in event["message"]:
        try:
            payload = base64.b64decode(event["message"]["data"]).decode()
            payload_json = json.loads(payload)
            bucket = payload_json.get("bucket")
            record = payload_json.get("name")
            generation = payload_json.get("generation")
        except Exception:
            logger.exception("FAILED TO PARSE PUBSUB EVENT")
            return jsonify({"status": "ignored"}), 200

    if not bucket or not record:
        return jsonify({"status": "ignored"}), 200

    if not record.startswith(RAW_PREFIX) or not record.lower().endswith(".pdf"):
        return jsonify({"status": "ignored"}), 200

    document_id = make_document_id(bucket, record, generation)
    log_prefix = f"[doc={document_id}]"

    logger.info(f"{log_prefix} PDF RECEIVED")

    if has_been_extracted(record) or has_terminal_marker(record):
        logger.info(f"{log_prefix} PDF SKIPPED (already extracted)")
        return jsonify({"status": "skipped", "document_id": document_id}), 200

    try:
        tmpdir = tempfile.mkdtemp()
        pdf_file = record.split("/")[-1]
        local_pdf = os.path.join(tmpdir, pdf_file)

        # Download and get source blob for metadata access
        source_blob = download_blob_to_file(RAW_BUCKET, record, local_pdf)
        if not source_blob:
            return jsonify({"status": "error", "message": "source not found"}), 404
            
        source_metadata = source_blob.metadata or {}
        logger.info(f"{log_prefix} PDF DOWNLOADED & METADATA CAPTURED")

        # Validate PDF
        with open(local_pdf, "rb") as f:
            if f.read(5) != b"%PDF-":
                logger.warning(f"{log_prefix} INVALID PDF")
                write_status_marker(record, "INVALID_PDF", document_id=document_id)
                insert_extract_status_safe(document_id, record, "FAILED", "invalid pdf")
                return jsonify({"status": "ignored"}), 200

        text = extract_pdf_to_text(local_pdf)
        logger.info(f"{log_prefix} PDF CONVERTED TO TEXT")

        out_name = out_txt_name(pdf_file)
        
        # Metadata Forwarding
        uploaded = upload_text_to_gcs(
            out_name,
            text,
            metadata={
                "document_id": document_id,
                "issuing_body": source_metadata.get("issuing_body", "Unknown"),
                "document_date": source_metadata.get("document_date", ""),
                "source_url": source_metadata.get("source_url", ""),
                "original_pdf": f"gs://{bucket}/{record}",
                "stage": "EXTRACT",
                "processed_at": str(time.time())
            },
        )

        if uploaded:
            write_status_marker(record, "EXTRACTED", out_name, document_id)
            # This call now has the enhanced logging from above
            bq_success = insert_extract_status_safe(document_id, record, "SUCCESS", "text extracted")
            
            if bq_success:
                logger.info(f"{log_prefix} FLOW COMPLETE: GCS + BQ UPDATED")
            else:
                logger.warning(f"{log_prefix} FLOW PARTIAL: GCS OK, BQ FAILED (Check Logs Above)")
        else:
            logger.info(f"{log_prefix} TXT SKIPPED (race condition)")

        return jsonify({"status": "ok", "document_id": document_id}), 200

    except Exception as e:
        logger.exception(f"{log_prefix} DOCUMENT EXTRACT FAILED")
        write_status_marker(record, "FAILED", str(e), document_id)
        insert_extract_status_safe(document_id, record, "FAILED", "exception during extract")
        return jsonify({"status": "failed", "document_id": document_id}), 200