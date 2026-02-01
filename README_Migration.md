# Duplication Preprocessing Adapter

## 1. Overview
The **Duplication Preprocessing Adapter** is a stateless Cloud Run microservice that acts as the technical "Glue" between the **Newsworthy Extraction Service** and the **Duplication Process stage**. 

It solves the schema and structural gaps between modern BigQuery tables and legacy processing scripts by performing a real-time **Pull-Transform-Handoff** workflow.

---

## 2. Migration Plan & Flow Logic
The system follows an asynchronous **Event-Pull** pattern to ensure the duplication script always works with the most accurate, geocoded data committed to the database.

### Step-by-Step Flow:
1.  **Commit**: The Extraction Service writes enriched records to BigQuery (`projects_newsworthy`).
2.  **Notify**: Upon success, a Pub/Sub message is published containing only the `document_id` and `category`.
3.  **Trigger**: The **Preprocessing Adapter** is invoked via a Pub/Sub Push subscription.
4.  **Fetch**: The Adapter queries the specific row from BigQuery using the `document_id`.
5.  **Transform**: The Adapter maps the modern schema to the legacy JSON format.
6.  **Handoff**: The Adapter saves the JSON to GCS and triggers the 1042-line duplication job.

---

## 3. Adapter Technical Blueprint

### Core Functions & Responsibilities
| Function Component | Task | Technical Detail |
| :--- | :--- | :--- |
| **`fetch_source_record`** | Data Retrieval | Executes a parameterized `SELECT` from BQ using `source_id`. |
| **`schema_transformer`** | Compatibility | Maps `unit_count` → `number_of_properties` and `name` → `project_name`. |
| **`spatial_handler`** | Coordinate Sync | Ensures `latitude`/`longitude` are extracted as float values for legacy math. |
| **`serialization_helper`** | Type Casting | Converts `DATE` objects to `YYYY-MM-DD` strings via `format_timestamp`. |
| **`filter_logic`** | Validation | Executes `is_about_refugees` (AZC) and `extract_houses` (min. 6 units) checks. |
| **`gcs_sink_finalizer`** | Trigger Handoff | Uploads JSON to `gs://newsradar/project_duplicate_check_input/{id}.json`. |

---

## 4. Operational Excellence

### Resilience & Error Handling
- **Dead Letter Queue (DLQ)**: Failed transformations are automatically routed to `topic-duplication-dlq` after 5 retries to prevent pipeline blockages.
- **State Reconciliation (Safety Net)**: A scheduled job identifies any `source_id` present in extraction tables but missing from duplication status tables to trigger manual replays.
- **Acknowledge Management**: The Adapter only ACKs the Pub/Sub message *after* the GCS upload and the next-stage trigger are confirmed.

### Filtering & Quality Control
The Adapter performs an automated quality gate before passing data to Phase 2:
- **Project Scale**: Skips non-tender/non-housing projects with fewer than 6 units.
- **Content Filtering**: Uses regex patterns to identify and skip Asylum Seeker Centers (AZC).
- **Location Validation**: Compares geocodes against municipality centroids to flag imputed vs. actual project locations.

---

## 5. Deployment Strategy

```bash
# Deploying the Adapter to Cloud Run
gcloud run deploy duplication-adapter \
  --image gcr.io/houzr-280014/duplication-adapter \
  --region europe-west4 \
  --set-env-vars ENVIRONMENT=production,TARGET_BUCKET=newsradar

# Migration Plan: Dataflow to Serverless Cloud Run

This document outlines the strategic transition from the legacy monolithic Apache Beam (Dataflow) pipeline to the new event-driven, microservices-based Cloud Run pipeline.

## Executive Summary
* **Legacy:** Dataflow (Java/Python Beam) - Batch/Streaming, high cold-start cost, complex maintenance.
* **Target:** Cloud Run (Python/Flask/AsyncIO) - Reactive, pay-per-use, sub-second scaling, decoupled stages.

---

## Phase 1: Infrastructure & Environment Setup
Before initiating data flow, the following environment must be "frozen" and verified.

- [ ] **Service Account (SA):** Create `news-pipeline-sa` with the IAM roles specified in `README.md`.
- [ ] **Storage Buckets:** Initialize the four-tier bucket system (`raw`, `extracted`, `enriched`, `status`).
- [ ] **Secret Management:** Move API keys (Vertex AI/Maps) from hardcoded configs to GCP Secret Manager.
- [ ] **Baseline Snapshot:** Take a final export of the current BigQuery production tables for parity comparison.

---

## Phase 2: Shadow Execution (Dual-Run)
*Goal: Run both pipelines in parallel to verify data integrity without affecting production.*

1. **Scraper Forking:** Modify the upstream Scraper to upload PDFs to **both** the legacy bucket and the new `gs://raw_pdf_ingest`.
2. **Shadow Sink:** Direct the Cloud Run pipeline to a **Shadow Dataset** (`[PROJECT]:poc_binod`).
3. **Monitoring:** * Compare processing time per document (Target: < 60s).
    * Compare AI extraction accuracy (Field-by-field parity check).
    * Monitor "Point-in-Polygon" rejection rates.

---

## Phase 3: Gradual Cutover (Canary)
*Goal: Shift a percentage of traffic to the new pipeline.*

- [ ] **Step 1:** Disable Dataflow for a low-volume municipality/source.
- [ ] **Step 2:** Observe the `document_pipeline_staging` table for 24 hours.
- [ ] **Step 3:** Validate that the "Merger Job" is successfully promoting staging data to production.
- [ ] **Step 4:** Scale to 50%, then 100% of municipal sources.

---

## Phase 4: Decommissioning & Cleanup
Once 100% parity is confirmed over a 7-day window:

1. **Stop Dataflow Jobs:** Cancel all remaining streaming or batch Dataflow jobs.
2. **Bucket Redirection:** Point all scraper uploads exclusively to the new `raw_pdf_ingest` bucket.
3. **IAM Cleanup:** Revoke legacy permissions used by Dataflow workers (e.g., Worker roles).
4. **Archive Code:** Move the legacy Beam codebase to an `archive/` branch in Git.

---

## Rollback Plan
In the event of a critical failure (e.g., Geocoding API limits reached, BQ merge conflicts):

1. **Immediate Action:** Disable Eventarc triggers via `gcloud eventarc triggers delete [TRIGGER_NAME]`.
2. **Resume Legacy:** Re-deploy the Dataflow template using the stored JAR/Python package in GCS.
3. **Data Recovery:** The original PDFs remain in `gs://raw_pdf_ingest` and can be re-processed by Dataflow by moving them back to the legacy source bucket.

---

## Success Metrics
| Metric | Success Threshold |
| :--- | :--- |
| **Latency** | End-to-end processing < 2 minutes |
| **Accuracy** | > 98% parity with legacy extraction |
| **Uptime** | 99.9% (Eventarc/Cloud Run availability) |