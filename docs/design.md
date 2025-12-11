# FX Pipeline (CSV Source) – Detailed Design & Implementation Reasoning

## 1. Problem Statement

The provider exposes a downloadable **full‑history ZIP file** containing a **complete CSV of FX rates** for all working dates and currencies. 
The file is refreshed **once per day** (usually around 16:00), but always contains **the entire history**.

Our objective is to build an ingestion pipeline that:

- safely ingests the daily full-history ZIP file,
- stores every version of the file in GCS,
- maintains a single **latest full-history BigQuery table** that reflects the most recent snapshot,
- extracts only the data for the current `run_date` from this latest full-history table,
- performs an idempotent MERGE into the core fact table,
- supports **late data detection** and **backfills**,
- builds analytics-ready marts using dbt,
- exposes reporting models in Looker Studio.


---

## 2. Decomposition into Sub-Tasks

### Phase A - Snapshot Management (File-Level Operations)

Phase A is responsible for managing file-level snapshots and keeping **one up-to-date full-history table** in BigQuery based on the most recent available ZIP. Heavy operations (ZIP→CSV→raw→long) are executed **only when a new snapshot is detected (new hash)**; otherwise the existing latest full-history table is reused.

1. Ingest ZIP file (check + hash + download-if-changed + GCS upload)  
2. Extract the CSV file from the ZIP and store it in GCS  (if new)
3. Load the CSV into a BigQuery raw table (as text) (if new)
4. Parse and clean the raw data in BigQuery and transform the wide data into a long format as the latest full-history table + save snapshot and full-history table metadata (if new)
5. Check that the latest full-history table contains run_date (readiness check)

In the current version the pipeline always uses the **latest available snapshot** to build the full-history table consumed by Phase B. As a possible future extension, Phase A may accept a specific historical `snapshot_id` and rebuild the full-history table from it (e.g. for debugging, audit or time‑travel scenarios).

### Phase B - Daily / Backfill Processing (Date-Level Operations)

Phase B always reads from the **latest full-history table** produced by Phase A (it does not select or manage snapshots explicitly) and processes the required `run_date`.

5. Filter by `run_date` and idempotently MERGE into the core table  
6. Detect late data by comparing recent historical values with the existing core table and emit alerts  
7. Build dbt models (staging and marts)  
8. Expose marts to Looker Studio  
9. Implement alerting, retries, and backfill logic 


---

## 3. Implementation (step-by-step reasoning)

Below each task is documented with:
- **Goal** - what this step is supposed to achieve  
- **Potential Problems** - likely real-world issues  
- **Mitigations** - how we address them  
- **Implementation Notes** - technical details of the solution  

---

# Step 1 – Ingest ZIP file (scheduled runs only)

## Goal

Download the daily full‑history ZIP, detect if its content changed (via hash), and upload it to the correct GCS bucket only when it is new.

---

## Potential Problems

* Provider unreachable or slow (HTTP/timeout issues).
* New file not yet published.
* File published without any change.
* Corrupted or incomplete ZIP downloads.

---

## Mitigations

* Use HTTP timeouts + Airflow retries for transient errors.
* Always download ZIP to a temp directory and compute SHA‑256.
* Compare new hash against all stored hashes.
* Insert snapshot and metadata only when hash changes.
* Validate the temp ZIP before upload (size + basic integrity check).

---

## Implementation Notes

Implement as `ingest_zip_snapshot` (PythonOperator).

Steps:

* Download ZIP to a temp file. If download fails → trigger Airflow retry.
* Validate ZIP (size + basic integrity).
* Compute SHA-256 hash of the temp file.
* Check if this hash already exists in metadata_log
* If such hash exists → reuse that snapshot, skip upload.
* If hash is new:
  * upload temp ZIP to GCS using deterministic path (`zip/hash_id=.../file.zip`);
  * insert into metadata_log a new row (`hash_id`, `gcs_zip_uri`, `file_size`, `ingested_at`);
* Remove temp file.

Output for downstream steps: `{hash_id, gcs_zip_uri, new_snapshot}`.
