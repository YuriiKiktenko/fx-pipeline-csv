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
4. Transform the raw snapshot wide table into a latest full-history long table (raw, no typing) and stamp it with snapshot_hash_id (if new)
5. Parse, clean, and type the latest full-history long table 

In the current version the pipeline always uses the **latest available snapshot** to build the full-history table consumed by Phase B. As a possible future extension, Phase A may accept a specific historical `snapshot_id` and rebuild the full-history table from it (e.g. for debugging, audit or time‑travel scenarios).

### Phase B - Daily / Backfill Processing (Date-Level Operations)

Phase B always reads from the **latest full-history table** produced by Phase A (it does not select or manage snapshots explicitly) and processes the required `rate_date`.

6. Filter by `rate_date` and idempotently MERGE into the core table  
7. Detect late data by comparing recent historical values with the existing core table and emit alerts  
8. Check data for fullfilness and quality (post-merge monitoring)
9. Build dbt models (staging and marts)  
10. Expose marts to Looker Studio  
11. Implement alerting, retries, and backfill logic 


---

## 3. Implementation (step-by-step reasoning)

Below each task is documented with:
- **Goal** - what this step is supposed to achieve  
- **Potential Problems** - likely real-world issues  
- **Mitigations** - how we address them  
- **Implementation Notes** - technical details of the solution  

---

# Step 1 - Ingest ZIP file

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
  * insert into metadata_log a new row (`hash_id`, `gcs_zip_uri`, `zip_size`, `ingested_at`);
* Remove temp file.

Output for downstream steps: `{hash_id}`.

## Step 2 - Extract CSV from ZIP

### Goal

Take the ZIP snapshot referenced by hash_id already stored in GCS, extract the single CSV file it contains, and upload that CSV back to GCS in a deterministic location, update metadata.
If CSV for this hash_id is already present (metadata + optional blob existence check), return immediately.

---

### Potential Problems

* ZIP object missing or inaccessible in GCS.
* CSV extraction fails.
* GCS upload of the CSV partially fails or leaves inconsistent state.
* Metadata points to missing artifact (CSV or ZIP) due to manual deletion or partial previous runs

---

### Mitigations

* Use the `hash_id` coming from Step 1 XCom.
* Get the `gcs_zip_uri` from metadata.
* Log CSV size and key metadata.
* Post-update verification: ensure metadata was updated (affected_rows > 0) and gcs_csv_uri is readable.

---

### Implementation Notes

Implement as `extract_csv_from_zip` (PythonOperator).

Input: function receives explicit hash_id (plus optional config if needed). The DAG is responsible for pulling this value from XCom (result of ingest_zip_snapshot) 

Steps:
  * Get the `gcs_zip_uri` and `gcs_csv_uri` from metadata.
  * If `gcs_csv_uri` exists and blob exist in the bucket - early return `{hash_id}`
  * Verify ZIP blob exists in GCS, then download it to a temp file.
  * Extract that CSV into another temp file.
  * Upload the CSV temp file to GCS bucket under `csv/hash_id=.../file.csv`.
  * Update metadata_log with (`gcs_csv_uri`, `csv_size`, `unpacked_at`) and validate update result.
  * Remove temp files.

Output for downstream steps: `{hash_id}`.

## Step 3 - Load CSV into BigQuery Raw

### Goal

Load the extracted CSV snapshot (identified by `hash_id`) from GCS into a **dedicated BigQuery raw snapshot table** (wide format), preserving the file **as-is** (minimal parsing).

Each snapshot is materialized as its **own table**, whose name is deterministically derived from `hash_id`.

---

### Potential Problems

* `gcs_csv_uri` missing in metadata, or CSV blob missing in GCS.
* CSV file missing, empty, or structurally corrupted.
* CSV schema drift.
* Concurrent runs attempt to load the **same hash** into the same snapshot table.
* BigQuery load job fails mid-run.
* Snapshot tables accumulate if dataset TTL is misconfigured.

---

### Mitigations

* Resolve `gcs_csv_uri` from `metadata_log` by `hash_id` and verify the blob exists.
* Perform pre-load CSV structural validation. Handle an optional trailing empty column name by renaming it to _trailing_empty.
* Generate schema deterministically from the header (all STRING).
* Use a deterministic **hash-based table name** (with a stable prefix).
* Use `WRITE_TRUNCATE` so repeated loads of the same snapshot are safe.
* Store snapshot tables in a dataset with **default table expiration = 1 day**

---

### Implementation Notes

Implement as `load_raw_from_csv` (PythonOperator).

Input: function receives explicit `hash_id`. The DAG passes it from XCom.

Steps:

* Compute target snapshot table name from `hash_id`.
* If the raw snapshot table already exists and is populated, return early.
* Resolve `gcs_csv_uri` from `metadata_log` by `hash_id`.
* Verify CSV blob exists in GCS.
* Validate CSV structure and header domain rules (readable, header present, delimiter correct, first column Date, currency columns [A-Z]{3}, consistent column count, optional trailing empty column).
* Extract header and generate BigQuery schema (all columns `STRING`).
* Load CSV into the snapshot table using a BigQuery load job:
  * `WRITE_TRUNCATE`
  * `skip_leading_rows = 1`
  * `max_bad_records = 0`
  * schema from header

Output for downstream steps: `{hash_id}`.

## Step 4 - Build Full-History Long Table (Raw)

### Goal

Transform the BigQuery raw snapshot table (wide full-history, identified by `hash_id`) into a **latest full-history snapshot** in long format with hash_id column

This step performs **no type casting and no semantic validation**. It only:
* reshapes wide → long (UNPIVOT),
* adds a `hash_id` column,
* writes to a `fx_raw_long` table.

---

### Potential Problems

* Raw snapshot table for the given `hash_id` does not exist.
* Concurrent runs attempt to rebuild the latest long table at the same time.
* UNPIVOT fails due to unexpected columns or schema drift.
* The latest long table exists but corresponds to a different snapshot.

---

### Mitigations

* Derive the raw snapshot table name deterministically from `hash_id`. Raise if snapshot table not found.
* Perform an early exit only if the latest long table already contains the same `hash_id`.
* Explicitly exclude non-currency columns from UNPIVOT (`Date`, `_trailing_empty`).
* Use `CREATE OR REPLACE TABLE ... AS SELECT` for atomic replacement.

---

### Implementation Notes

Implement as `build_long_raw` (PythonOperator).

Input: the function receives an explicit `hash_id` (passed from XCom).

Steps:

* If table `fx_raw_long` exists and contains `hash_id` = <hash_id>, return early.
* Compute raw snapshot table FQN `raw_snapshot_fqn` from `hash_id`.
* Check that snapshot table <raw_snapshot_fqn> exists, raise if not.
* CREATE OR REPLACE TABLE `fx_raw_long`:
    * UNPIVOT currency columns
    * Add constant column `hash_id`
    * Exclude `_trailing_empty` from UNPIVOT
    * Keep all as STRING
* Validate resulting `fx_raw_long`.

Output for downstream steps `{hash_id}`

# Step 5 — Parse, Clean, and Type the Latest Full-History Long Table

## Goal

Prepare a typed, clean, deduplicated dataset for the requested `hash_id` from the `fx_raw_long` and publish it as a validated latest-only table `fx_stage_long`

---

## Potential Problems

* Snapshot mismatch: `fx_raw_long` does not contain the requested `hash_id`.
* `date_str` contains empty or malformed values.
* `rate_str` contains empty values, non-numeric text, or markers such as `N/A`.
* `currency` may include whitespace, lowercase, or unexpected values.
* Duplicate rows for the same `(date, currency)`.

---

## Mitigations

* Use explicit `hash_id` from XCom and validate `fx_raw_long` contains that hash.
* Always filter the source: `WHERE hash_id = <hash_id>`
* Explicitly treat `N/A` (and empty strings) as NULL before casting.
* Filter out rows where typed fields are NULL after casting.
* Normalize currency via `UPPER(TRIM(currency))`.
* Deduplicate.
* Validate result
* Use candidate table + promote pattern so the final table is updated only if validations pass

---

## Implementation Notes

Implement as `build_long_stage` (PythonOperator).

Input: function receives explicit `hash_id`. The DAG passes it from XCom.

Steps:
* Verify `fx_raw_long` contains rows for the requested `hash_id`.
* Parse:
  * `rate_date = SAFE_CAST(date_str AS DATE)`
  * `currency = UPPER(TRIM(currency))`
  * `rate = SAFE_CAST(NULLIF(NULLIF(TRIM(rate_str), ''), 'N/A') AS NUMERIC)`
* Filter:
  * `rate_date IS NOT NULL`
  * `currency IS NOT NULL AND currency != ''`
  * `rate IS NOT NULL`
* Deduplicate data (`SELECT DISTINCT rate_date, currency, rate, hash_id`)
* Build candidate: CREATE OR REPLACE TABLE `fx_stage_long_candidate` AS <typed/filtered/distinct>
* Validation checks (fail-fast):
  * regex validation for `currency` (`^[A-Z]{3}$`)
  * `rate > 0`
  * uniqueness on `(rate_date, currency)`
* Promote candidate: CREATE OR REPLACE TABLE `fx_stage_long` AS SELECT * FROM `fx_stage_long_candidate`
* Cleanup: DROP TABLE IF EXISTS `fx_stage_long_candidate`

Output for downstream steps `{hash_id}`

# Step 6 — Merge Daily Rates into Core Table

## Goal

Idempotently replace FX rates for `rate_date` in the `fx_core_daily` fact table from the staging table `fx_stage_long` .

---

## Potential Problems

* No rows exist in `fx_stage_long` for the requested `rate_date`.
* Partial data for rate_date (necessary currencies missing).
* Re-running the same `rate_date` causes duplicate inserts.
* Late-arriving corrections for historical dates.

---

## Mitigations

* If no rows exist for `rate_date`, skip insert.
* Check for the necessary currencies with the list in config, fail if any of required currencies missing.
* Always filter staging data by explicit `rate_date`.
* Execute delete+insert in one BigQuery script job to avoid partial updates.
* Support safe re-runs and backfills.

---

## Implementation Notes

Implement as a single `merge_daily_rates` (PythonOperator).

Input:

* `hash_id` — snapshot identifier (from XCom)
* `rate_date` — business date to process (str: 'YYYY-MM-DD')

Steps:

* Use explicit `hash_id` from XCom and validate `fx_stage_long` contains that hash.
* Query `fx_stage_long` for rows matching `rate_date`.
* If zero rows are found:
  * Log informational message.
  * Exit successfully.
* If rows exist:
  * Validate with the list in config if it contains all necessary currencies, raise error if not.
  * Delete all existing rows in the `fx_core_daily` table for the `rate_date`
  * Insert new rows (Execute delete+insert in one job).
* Optionally log number of affected rows.

---

Output for downstream steps `{hash_id}`
