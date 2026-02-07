import logging

from google.cloud import bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    table_contains_matching_hash,
    table_contains_matching_hash_and_date,
    get_existing_table_row_count,
    get_missing_required_currencies,
)

logger = logging.getLogger(__name__)


def merge_daily_rates(hash_id, rate_date, run_type):
    """
    Load one business day of FX rates into the core fact table and meta table (idempotent).

    Workflow:
        1) Validate the staging table exists and contains the requested `hash_id`.
        2) If there are no staging rows for the requested `rate_date`, skip the load
           and exit successfully (no-op).
        3) Validate data completeness for this day using a required currencies list.
           If any required currency is missing, fail-fast.
        4) Execute a single BigQuery script job that:
            - deletes existing core rows for `rate_date`
            - inserts into the core table the staging rows for (hash_id, rate_date)
            - deletes existing meta rows for `rate_date`
            - inserts into the meta table the info rows for rate_date

    Guarantees:
        - Core table contains exactly the rates loaded for `rate_date` from the given
          snapshot `hash_id` after successful completion.
        - Safe to re-run for the same inputs.

    Returns:
        dict: {"hash_id": str}
    """
    bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
    stage_long_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_STAGE_DATASET}.{cfg.BQ_STAGE_LONG_TABLE}"
    core_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_CORE_DATASET}.{cfg.BQ_CORE_DAILY_TABLE}"
    core_meta_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_CORE_TABLE}"

    stage_rows = get_existing_table_row_count(bq_client, stage_long_fqn)
    if stage_rows is None or stage_rows == 0:
        raise RuntimeError(f"Stage table not found or empty: {stage_long_fqn}")

    if not table_contains_matching_hash(bq_client, stage_long_fqn, hash_id):
        raise RuntimeError(f"{stage_long_fqn} table does not contain requested hash_id: {hash_id}")

    if not table_contains_matching_hash_and_date(bq_client, stage_long_fqn, hash_id, rate_date):
        logger.info(f"No staging rows for rate_date: {rate_date} and hash_id: {hash_id} in {stage_long_fqn}. Skipping insert.")
        return {"hash_id": hash_id}

    logger.info(f"Stage table found: {stage_long_fqn}, rows={stage_rows}")

    missing = get_missing_required_currencies(
        bq_client,
        stage_long_fqn,
        hash_id,
        rate_date,
        cfg.REQUIRED_CURRENCIES,
    )
    if missing:
        raise RuntimeError(f"Missing required currencies for rate_date: {rate_date} in {stage_long_fqn}: {missing}")
    logger.info(f"Required currencies check passed for rate_date: {rate_date}")

    script = f"""
    BEGIN TRANSACTION;

    DELETE FROM `{core_fqn}`
    WHERE rate_date = @rate_date;

    INSERT INTO `{core_fqn}` (rate_date, currency, rate, hash_id)
    SELECT
      rate_date,
      currency,
      rate,
      hash_id
    FROM `{stage_long_fqn}`
    WHERE hash_id = @hash_id
      AND rate_date = @rate_date;
    
    DELETE FROM `{core_meta_fqn}`
    WHERE rate_date = @rate_date;

    INSERT INTO `{core_meta_fqn}` (rate_date, run_type, hash_id, row_count, currency_count, rates, required_curr, inserted_at)
    SELECT
      @rate_date as rate_date,
      @run_type as run_type,
      @hash_id as hash_id,
      COUNT(*) as row_count,
      COUNT(DISTINCT currency) as currency_count,
      ARRAY_AGG(STRUCT(currency, rate) ORDER BY currency) as rates,
      @required as required_curr,
      CURRENT_TIMESTAMP() as inserted_at
    FROM `{stage_long_fqn}`
    WHERE hash_id = @hash_id
      AND rate_date = @rate_date;

    COMMIT TRANSACTION;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
            bigquery.ScalarQueryParameter("rate_date", "DATE", rate_date),
            bigquery.ScalarQueryParameter("run_type", "STRING", run_type),
            bigquery.ArrayQueryParameter("required", "STRING", cfg.REQUIRED_CURRENCIES),
        ],
    )

    logger.info(f"Running replace-day load into core: {core_fqn} for rate_date: {rate_date}")
    job = bq_client.query(script, job_config=job_config)
    job.result()

    logger.info(f"Core updated successfully for rate_date: {rate_date}")
    return {"hash_id": hash_id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_hash = "6cd1b238e88aca464c326267c23dcb5f50961a502e0d80e84d9a2d93f8a48bc5"
    test_rate_date = "2026-02-04"
    test_run_type = "test"
    result = merge_daily_rates(test_hash, test_rate_date, test_run_type)
    logger.info(f"Done. Returned value was: {result}")
