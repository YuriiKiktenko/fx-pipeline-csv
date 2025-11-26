# FX Pipeline (CSV Source) – Detailed Design & Implementation Reasoning

## 1. Problem Statement

The provider exposes a downloadable ZIP file containing a **full historical CSV** of FX rates.  
The file is refreshed once per day (usually around 16:00).

The goal is to build a reliable, idempotent ingestion pipeline that:

- checks availability and downloads the ZIP file **only if it has changed**,  
- stores both the ZIP and the extracted CSV in GCS (bronze layer),  
- safely processes and loads the raw CSV into BigQuery,  
- ingests **only the FX rates for the run date** on each run,   
- detects any changes to **past dates (late data)** and reports them via alerts,  
- allows manual or dedicated backfill runs for affected dates,  
- exposes analytics-ready tables to BI tools such as Looker Studio.

---

## 2. Decomposition into Sub-Tasks

1. Check that the new file is available  
2. Download the ZIP file and store it in GCS  
3. Extract the CSV file from the ZIP and store it in GCS  
4. Load the full CSV (as text) into a BigQuery raw table  
5. Parse and clean the raw data in BigQuery and transform the wide data into a long format  
6. Select only the run date and MERGE it into the core fact table with idempotent logic  
7. Detect late data by comparing recent historical values with the existing core table and emit alerts  
8. Use dbt to build analytics-ready marts on top of the core fact table  
9. Expose the final marts to BI tools in Looker Studio  
10. Implement alerting, retries, and backfill logic 


---

## 3. Implementation (step-by-step reasoning)

Below each task is documented with:
- **Goal** — what this step is supposed to achieve  
- **Potential Problems** — likely real-world issues  
- **Mitigations** — how we address them  
- **Implementation Notes** — technical details of the solution  

---
