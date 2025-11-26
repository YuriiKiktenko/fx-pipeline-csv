# FX Pipeline (CSV Source) — Airflow + GCS + BigQuery + dbt + Looker Studio

## Problem

The data provider exposes a **daily ZIP file** containing a **full historical CSV** with FX rates per date and currency.  
This file is updated once per day and must be ingested, validated, normalised, and stored in an analytics-friendly format.

The goals of this project are:
- automatically ingest the daily ZIP→CSV FX file from a remote endpoint,
- store the raw file in a data lake (GCS),
- transform the historical file into normalised BigQuery tables,
- update historical data when the provider republishes corrected values,
- ensure idempotent loading through MERGE strategies,
- expose a clean analytics layer for BI consumption in Looker Studio.

## Solution Overview

This repository implements an end-to-end batch data pipeline:

1. **Airflow ingestion DAG**
   - download ZIP file and validate its availability  
   - check if the new file differs from the previous version (hash/size comparison)  
   - store the raw file in GCS (bronze layer)  
   - extract and parse the CSV  
   - load to a staging table in BigQuery (silver)  
   - normalise the wide CSV into a long format (`fx_date`, `currency`, `rate`)  
   - MERGE the snapshot into a core fact table (gold) using idempotent logic  

2. **dbt transformation layer**
   - staging models  
   - fact/dim marts  
   - dbt tests (schema, nulls, accepted values)  
   - optional forward-fill and “latest rate” flags for analytics  

3. **Looker Studio dashboard**
   - FX rate time series per currency  
   - latest available rates  
   - change over periods (7/30/90 days)  
   - EUR-to-currency converter  
   - table of all current FX rates  

For detailed design decisions, risks, edge cases and trade-offs, see `docs/design.md`.
