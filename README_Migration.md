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
| **Cost** | > 50% reduction vs Dataflow instance costs |
| **Accuracy** | > 98% parity with legacy extraction |
| **Uptime** | 99.9% (Eventarc/Cloud Run availability) |