import base64
import json
import os
import tempfile
import hashlib
import logging
from datetime import datetime

from flask import Flask, request, jsonify
from google.cloud import storage, bigquery
from pypdf import PdfReader

# -----------------------
# App + logging
# -----------------------
app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract")

# -----------------------
# Config
# -----------------------
RAW_BUCKET = os.environ.get("RAW_BUCKET", "poc_binod_nl_pdfs")        # raw PDFs
EXTRACTED_BUCKET = os.environ.get("EXTRACTED_BUCKET", "poc_extracted") # extracted text
STATUS_BUCKET = os.environ.get("STATUS_BUCKET", "poc_status")          # status JSONs

RAW_PREFIX = "raw/"
OUT_PREFIX = "extracted/"
STATUS_PREFIX = "status/"  # status JSONs

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

def insert_extract_status(document_id, pdf_name, status, message=""):
    """
    Directly insert a row into the main BigQuery table.
    """
    query = f"""
    INSERT INTO `{BQ_TABLE_ID}` 
    (document_id, pdf_name, extracted_status, enrich_status, bq_inserted_status,
     extracted_retries, enrich_retries, bq_insert_retries, message, created_at, updated_at)
    VALUES (@document_id, @pdf_name, @status, "PENDING", "PENDING", 0, 0, 0, @message, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP())
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("document_id", "STRING", document_id),
            bigquery.ScalarQueryParameter("pdf_name", "STRING", pdf_name),
            bigquery.ScalarQueryParameter("status", "STRING", status),
            bigquery.ScalarQueryParameter("message", "STRING", message),
        ]
    )

    bq_client.query(query, job_config=job_config).result()

def download_blob_to_file(bucket_name, blob_name, file_path):
    blob = storage_client.bucket(bucket_name).blob(blob_name)
    blob.download_to_filename(file_path)

def upload_text_to_gcs(blob_name, text, metadata=None):
    blob = storage_client.bucket(EXTRACTED_BUCKET).blob(blob_name)
    blob.metadata = metadata or {}
    blob.upload_from_string(text, content_type="text/plain")

# -----------------------
# Cloud Run Handler
# -----------------------
@app.route("/", methods=["POST"])
def handler():
    event = request.get_json(silent=True) or {}
    bucket = None
    record = None
    generation = event.get("generation")

    # Parse GCS / PubSub events
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
        logger.warning("Ignored event with no bucket/name")
        return jsonify({"status": "ignored"}), 200

    if not record.startswith(RAW_PREFIX) or not record.lower().endswith(".pdf"):
        logger.info(f"Ignored non-raw PDF object: {record}")
        return jsonify({"status": "ignored"}), 200

    document_id = make_document_id(bucket, record, generation)
    log_prefix = f"[doc={document_id}]"

    logger.info(f"{log_prefix} PDF RECEIVED")

    if has_been_extracted(record) or has_terminal_marker(record):
        write_status_marker(record, "SKIPPED", "already extracted", document_id)
        logger.info(f"{log_prefix} PDF SKIPPED (already extracted)")
        return jsonify({"status": "skipped", "document_id": document_id}), 200

    try:
        tmpdir = tempfile.mkdtemp()
        pdf_file = record.split("/")[-1]
        local_pdf = os.path.join(tmpdir, pdf_file)

        download_blob_to_file(RAW_BUCKET, record, local_pdf)
        logger.info(f"{log_prefix} PDF DOWNLOADED")

        # Validate PDF
        with open(local_pdf, "rb") as f:
            if f.read(5) != b"%PDF-":
                logger.warning(f"{log_prefix} INVALID PDF")
                write_status_marker(record, "INVALID_PDF", document_id=document_id)
                insert_extract_status(document_id, record, "FAILED", "invalid pdf")
                return jsonify({"status": "ignored"}), 200

        logger.info(f"{log_prefix} PDF VALIDATED")

        text = extract_pdf_to_text(local_pdf)
        logger.info(f"{log_prefix} PDF CONVERTED TO TEXT")

        out_name = out_txt_name(pdf_file)
        upload_text_to_gcs(
            out_name,
            text,
            metadata={
                "document_id": document_id,
                "source_pdf": record,
                "stage": "EXTRACT",
            },
        )
        logger.info(f"{log_prefix} TXT UPLOADED TO BUCKET: {out_name}")

        write_status_marker(record, "EXTRACTED", out_name, document_id)
        logger.info(f"{log_prefix} STATUS MARKER UPDATED")

        insert_extract_status(document_id, record, "SUCCESS", "text extracted")
        logger.info(f"{log_prefix} STATUS TABLE UPDATED")

        return jsonify({"status": "ok", "document_id": document_id, "output": out_name}), 200

    except Exception:
        logger.exception(f"{log_prefix} DOCUMENT EXTRACT FAILED")
        write_status_marker(record, "FAILED", "exception", document_id)
        logger.info(f"{log_prefix} STATUS MARKER UPDATED TO FAILED")
        insert_extract_status(document_id, record, "FAILED", "exception during extract")
        logger.info(f"{log_prefix} STATUS TABLE UPDATED TO FAILED")
        return jsonify({"status": "failed", "document_id": document_id}), 200
