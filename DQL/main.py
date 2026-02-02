import os
import datetime
import logging
import requests
from flask import Flask, jsonify
from google.cloud import bigquery
from google.cloud import storage
import google.auth.transport.requests
import google.oauth2.id_token

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)

# Configuration
PROJECT_ID = "houzr-280014"
DATASET_ID = "poc_binod"
TABLE_ID = "document_pipeline_status"
RETRY_WINDOW_DAYS = os.environ.get("RETRY_WINDOW_DAYS", "1")

BUCKETS = {
    "raw": "poc_raw_binod",
    "extracted": "poc_extracted",
    "enriched": "poc_enrich"
}

SERVICES = {
    "extract": "https://poc-extract-service-v2-160180396428.europe-west4.run.app",
    "enrich": "https://poc-binod-enrich-text-160180396428.europe-west4.run.app",
    "newsworthy": "https://postprocess-service-160180396428.europe-west4.run.app"
}

client_bq = bigquery.Client()
client_gcs = storage.Client()

def get_auth_headers(target_url):
    try:
        auth_req = google.auth.transport.requests.Request()
        id_token = google.oauth2.id_token.fetch_id_token(auth_req, target_url)
        return {"Authorization": f"Bearer {id_token}"}
    except Exception as e:
        logger.error(f"[AUTH ERROR] Failed to get OIDC token for {target_url}: {e}")
        return None

def trigger_service(service_url, bucket, name, phase_name):
    payload = {
        "bucket": bucket,
        "name": name,
        "dlq_retry": True 
    }
    logger.info(f"[RETRY-PHASE: {phase_name}] Calling {service_url} for file {name}")
    
    headers = get_auth_headers(service_url)
    if not headers:
        return None

    try:
        resp = requests.post(service_url, json=payload, headers=headers, timeout=30)
        logger.info(f"[RESPONSE] {phase_name} Service returned {resp.status_code}: {resp.text[:100]}")
        
        try:
            return {"status_code": resp.status_code, "data": resp.json()}
        except:
            return {"status_code": resp.status_code, "data": {}}
    except Exception as e:
        logger.error(f"[HTTP ERROR] Failed to reach {phase_name} service: {e}")
        return None

def update_bq_status(doc_id, column, status):
    """Force updates a specific status column to a value (e.g., success)."""
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        SET {column} = '{status}',
            updated_at = CURRENT_TIMESTAMP()
        WHERE document_id = '{doc_id}'
    """
    client_bq.query(query).result()
    logger.info(f"[BQ-UPDATE] Set {column} to {status} for {doc_id}")

def update_bq_retry(doc_id, new_count):
    query = f"""
        UPDATE `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        SET extracted_retries = {new_count},
            updated_at = CURRENT_TIMESTAMP()
        WHERE document_id = '{doc_id}'
    """
    client_bq.query(query).result()

@app.route("/", methods=["GET", "POST"])
def run_dlq_process():
    logger.info(f"DLQ Process started. Window: {RETRY_WINDOW_DAYS} days.")
    
    query = f"""
        SELECT 
            document_id, pdf_name, extracted_status, 
            enrich_status, bq_inserted_status, extracted_retries
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE updated_at >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {RETRY_WINDOW_DAYS} DAY)
        AND IFNULL(extracted_retries, 0) < 3
        AND (
            IFNULL(extracted_status, '') != 'success' 
            OR (extracted_status = 'success' AND IFNULL(enrich_status, '') != 'success')
            OR (extracted_status = 'success' AND enrich_status = 'success' AND IFNULL(bq_inserted_status, '') != 'success')
        )
    """
    
    try:
        rows = list(client_bq.query(query).result())
        retries_count = 0
        logger.info(f"Found {len(rows)} documents requiring attention.")

        for row in rows:
            doc_id = row['document_id']
            filename = (row['pdf_name'] or "").split('/')[-1]
            if not filename: continue

            new_retry_val = (row['extracted_retries'] or 0) + 1
            
            # Phase Detection
            if row['extracted_status'] != 'success':
                service_url, bucket, phase = SERVICES['extract'], BUCKETS['raw'], "EXTRACTION"
                path = filename 
            elif row['enrich_status'] != 'success' or row['enrich_status'] is None:
                service_url, bucket, phase = SERVICES['enrich'], BUCKETS['extracted'], "ENRICHMENT"
                path = f"extracted/{filename.replace('.pdf', '.txt')}"
            elif row['bq_inserted_status'] != 'success' or row['bq_inserted_status'] is None:
                service_url, bucket, phase = SERVICES['newsworthy'], BUCKETS['enriched'], "POST-PROCESS"
                path = f"enriched/extracted/{filename.replace('.pdf', '.json')}"
            else:
                continue

            # Execute Trigger
            result = trigger_service(service_url, bucket, path, phase)

            if result and result['status_code'] in [200, 201, 202, 204]:
                service_data = result.get('data', {})
                service_status = str(service_data.get('status', '')).lower()

                # --- Handle "Already Done" Responses ---
                if phase == "EXTRACTION" and service_status == "skipped":
                    logger.info(f"[AUTO-FIX] Extraction returned skipped. Marking success.")
                    update_bq_status(doc_id, "extracted_status", "SUCCESS")
                
                elif phase == "ENRICHMENT" and service_status == "locked":
                    logger.info(f"[AUTO-FIX] Enrichment returned locked. Marking success.")
                    update_bq_status(doc_id, "enrich_status", "SUCCESS")
                
                else:
                    # Standard behavior: increment retry count
                    update_bq_retry(doc_id, new_retry_val)
                
                retries_count += 1
                logger.info(f"[SUCCESS] Handled {doc_id} in {phase} phase.")

        return jsonify({"status": "SUCCESS", "triggered": retries_count}), 200

    except Exception as e:
        logger.error(f"DLQ Fatal Error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))