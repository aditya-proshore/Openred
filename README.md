# 📑 News Analysis Pipeline (Serverless v2.0)

A high-performance, event-driven microservices architecture on Google Cloud Platform (GCP). This pipeline migrates legacy Dataflow/Beam workloads to a reactive **Cloud Run + Eventarc** model, reducing costs and cold-start latency.

---

## 🏗️ Architecture Overview

The system utilizes a **Chained Reactive Pattern**. Each service is decoupled and triggered via Google Cloud Storage (GCS) events managed by Eventarc.

### 🔄 The Document Flow
1. **Extract Service:** PDF binary → Markdown text.
2. **Enrich Service:** Markdown text → AI-Structured JSON (Gemini 2.0).
3. **Newsworthy Service:** JSON → Spatial Validation & BigQuery Ingestion.
4. **Merger Service:** Staging tables → Production Status synchronization.

---

## 🔒 Engineering Principles

### 1. Distributed Locking & Idempotency
To handle "at-least-once" delivery from Eventarc, services implement **Atomic GCS Locking**:
* **Mechanism:** Uses `if_generation_match=0` during file creation.
* **Result:** If an output file already exists (or is being created by another instance), the service receives a `412 Precondition Failed` and exits with `200 OK` to prevent duplicate processing.

### 2. The "Metadata Passport"
Context provided by the initial scraper (Issuing Body, Source URL, Date) is preserved throughout the pipeline.
* **Persistence:** Each stage reads the `blob.metadata` from its input and manually re-attaches it to its output. 
* **Benefit:** Eliminates the need for downstream services to re-query the source database for basic context.

---

## 🛠️ Service Deep-Dive

### 📂 Phase 1: Extract Service
* **Runtime:** Python / Flask
* **Engine:** `PyMuPDF4LLM`
* **Key Function:** `extraction_worker()`
    * Downloads PDF to `tempfile`.
    * Converts to Markdown (superior to raw text for preserving document structure/headers).
    * Forwards the "Passport" metadata to the next bucket.
    * Uses `threading.Thread` to acknowledge Eventarc triggers in < 15s.

### 🧠 Phase 2: Enrich Service
* **Runtime:** Python / AsyncIO
* **Engine:** Gemini 2.0 Flash (Vertex AI)
* **Key Functions:**
    * `run_incremental_relevance_llm()`: Early exit for irrelevant documents.
    * `multi_label_classification_async()`: Identifies project mentions.
    * `extract_details_batched()`: Uses **`asyncio.gather`** for parallel AI extraction of multiple projects simultaneously.

### 📍 Phase 3: Newsworthy Service
* **Runtime:** Python / Shapely / GeoPy
* **Logic:**
    * **Cache-Aside Pattern:** Loads municipality geometry from `geometry_cache.json` in GCS to avoid slow (11s) BigQuery lookups.
    * **Spatial Guard:** Performs Point-in-Polygon checks to ensure geocoded addresses are within the municipality boundary.

---

## 🔍 Observability & Traceability

Every log entry across all services follows a standardized pattern using a unique `doc_id` (SHA-1 hash of the filename).

### Debugging Trace (Logs Explorer)
Query by `textPayload:"doc_id: <ID>"` to see the full lifecycle:
1. `[extract][doc_id: a1b2...] SUCCESS: File converted.`
2. `[doc=a1b2...] RELEVANCE: True`
3. `[validation-fail] Project 'X' outside Boundary`
4. `[pipeline-finish] Duration: 4.8s`

---

## 🚀 Deployment & Migration

### Prerequisites
* **APIs:** `run.googleapis.com`, `eventarc.googleapis.com`, `aiplatform.googleapis.com`.
* **IAM:** Service accounts must have `Vertex AI User`, `Storage Object Admin`, and `BigQuery Data Editor`.
