# FX Pipeline (CSV Source): Airflow + GCS + BigQuery + dbt + Looker Studio

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
   - check if the new file differs from the previous version (skip re-processing if unchanged)  
   - store the raw ZIP in GCS (bronze), then extract CSV and load to a raw BigQuery table  
   - normalise the wide CSV into a long format (`rate_date`, `currency`, `rate`) via raw → staged long tables  
   - MERGE the daily slice into a core fact table (gold) using idempotent logic  
   - detect late data (compare provider snapshot vs core) and run post-merge data quality checks  
   - run `dbt build` to refresh staging and mart models  

2. **dbt transformation layer**
   - staging models (long and wide) reading from the core table  
   - dbt tests (schema, nulls, accepted values)  
   - marts for time series, percent change, quality and reporting

3. **Looker Studio (future reports)**
      Reports will be built in Looker Studio by connecting to the BigQuery dataset where dbt writes. 
   
   Planned reports: 
   - time series of FX rates per currency;
   - change over periods; 
   - volatility and range dashboards (rolling 7d and 30d min/max/avg); 
   - data quality monitoring (daily row and currency counts, missing-data and alert flags). 


## Design

**[Design document](docs/design.md)** — Detailed design and implementation reasoning for the pipeline. It covers the problem statement, decomposition into Phase A (snapshot management: ZIP ingest, CSV extract, raw load, full-history table) and Phase B (daily/backfill: MERGE, late-data detection, dbt, Looker Studio), plus step-by-step implementation notes with goals, potential problems, mitigations, and technical details for each step.
