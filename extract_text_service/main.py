import os
import time
import logging
import threading
import tempfile
import hashlib
from flask import Flask, request, jsonify
from google.cloud import storage
import pymupdf4llm
from google.api_core.exceptions import PreconditionFailed

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("extract-process")

# Configuration
EXTRACTED_BUCKET = os.environ.get("EXTRACTED_BUCKET", "poc_extracted")
storage_client = storage.Client()

def extraction_worker(bucket_name, blob_name, doc_id):
    """Background worker: Download -> Extract -> Forward Metadata -> Upload"""
    t_start = time.perf_counter()
    log_prefix = f"[extract][doc_id: {doc_id}]"
    
    try:
        # 1. GET THE BLOB OBJECT (This fetches the metadata)
        bucket = storage_client.bucket(bucket_name)
        source_blob = bucket.get_blob(blob_name) # Important: get_blob fetches metadata, bucket.blob() does not
        
        if not source_blob:
            logger.error(f"{log_prefix} File not found: {blob_name}")
            return

        # 2. CAPTURE METADATA FROM SOURCE
        # This is where we grab your 'issuing_body', 'source_url', etc.
        source_metadata = source_blob.metadata or {}
        logger.info(f"{log_prefix} Captured Metadata: {source_metadata}")

        # 3. DOWNLOAD & CONVERT
        with tempfile.NamedTemporaryFile(suffix=".pdf") as tmp_pdf:
            source_blob.download_to_filename(tmp_pdf.name)
            logger.info(f"{log_prefix} PDF Downloaded")
            
            # Using PyMuPDF4LLM for markdown extraction
            markdown_content = pymupdf4llm.to_markdown(tmp_pdf.name)
            
            # 4. UPLOAD WITH FORWARDED METADATA
            out_filename = f"extracted/{blob_name.rsplit('.', 1)[0]}.txt"
            out_blob = storage_client.bucket(EXTRACTED_BUCKET).blob(out_filename)
            
            # Pass all metadata forward so the 'Enrich' service has it
            out_blob.metadata = {
                "document_id": source_metadata.get("document_id", doc_id),
                "issuing_body": source_metadata.get("issuing_body", "Unknown"),
                "document_date": source_metadata.get("document_date", ""),
                "source_url": source_metadata.get("source_url", ""),
                "original_pdf": f"gs://{bucket_name}/{blob_name}",
                "extraction_status": "success",
                "processed_at": str(time.time())
            }
            
            # Atomic upload (replaces the "LOCKED" placeholder)
            out_blob.upload_from_string(markdown_content, content_type="text/plain")
            
        logger.info(f"{log_prefix} SUCCESS: {out_filename} uploaded in {time.perf_counter()-t_start:.2f}s")

    except Exception as e:
        logger.error(f"{log_prefix} FATAL ERROR: {str(e)}")

@app.route("/", methods=["POST"])
def handler():
    # Parse Eventarc GCS Payload
    event = request.get_json(silent=True) or {}
    
    # Eventarc GCS sends 'bucket' and 'name'
    bucket_name = event.get("bucket") or event.get("data", {}).get("bucket")
    blob_name = event.get("name") or event.get("data", {}).get("name")

    if not bucket_name or not blob_name or not blob_name.endswith(".pdf"):
        return jsonify({"status": "ignored"}), 200

    doc_id = hashlib.sha1(blob_name.encode()).hexdigest()[:12]
    out_filename = f"extracted/{blob_name.rsplit('.', 1)[0]}.txt"
    out_blob = storage_client.bucket(EXTRACTED_BUCKET).blob(out_filename)

    # 5. IDEMPOTENCY / LOCKING
    try:
        # upload_from_string with if_generation_match=0 is an atomic "create if not exists"
        out_blob.metadata = {"extraction_status": "locked"}
        out_blob.upload_from_string("LOCKED", if_generation_match=0)
    except PreconditionFailed:
        logger.info(f"[extract][doc_id: {doc_id}] SKIP: File already processing or done.")
        return jsonify({"status": "skipped"}), 200

    # 6. INSTANT 200 RESPONSE (< 15s)
    # Threading prevents Cloud Run from timing out the request
    thread = threading.Thread(target=extraction_worker, args=(bucket_name, blob_name, doc_id))
    thread.start()

    return jsonify({"status": "accepted", "doc_id": doc_id}), 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)