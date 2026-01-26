import os
import logging
from datetime import datetime
from flask import Flask, jsonify
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

PROJECT_ID = "houzr-280014"
STAGING = f"{PROJECT_ID}.poc_binod.document_pipeline_staging"
STATUS = f"{PROJECT_ID}.poc_binod.document_pipeline_status"

bq = bigquery.Client()
app = Flask(__name__)

@app.route("/", methods=["POST", "GET"])
def merge_and_cleanup():
    job_started_at = datetime.utcnow()
    logger.info("Job started at %s", job_started_at.isoformat())

    # 1️⃣ MERGE
    merge_sql = f"""
    MERGE `{STATUS}` T
    USING (
      SELECT document_id, enrich_status, message, updated_at
      FROM `{STAGING}`
      WHERE created_at <= @cutoff
    ) S
    ON T.document_id = S.document_id
    WHEN MATCHED THEN
      UPDATE SET
        T.enrich_status = S.enrich_status,
        T.message = S.message,
        T.updated_at = S.updated_at
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter(
                "cutoff", "TIMESTAMP", job_started_at
            )
        ]
    )

    merge_job = bq.query(merge_sql, job_config=job_config)
    merge_job.result()
    updated = merge_job.num_dml_affected_rows or 0

    logger.info("MERGE completed, rows updated=%d", updated)

    # 2️⃣ DELETE ONLY SAFE ROWS
    delete_sql = f"""
    DELETE FROM `{STAGING}`
    WHERE document_id IN (
      SELECT S.document_id
      FROM `{STAGING}` S
      JOIN `{STATUS}` T
      ON S.document_id = T.document_id
      WHERE S.created_at <= @cutoff
    )
    """

    delete_job = bq.query(delete_sql, job_config=job_config)
    delete_job.result()
    deleted = delete_job.num_dml_affected_rows or 0

    logger.info("Deleted %d staging rows", deleted)

    return jsonify({
        "status": "success",
        "job_started_at": job_started_at.isoformat(),
        "rows_updated": updated,
        "rows_deleted": deleted
    }), 200


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
