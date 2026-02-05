import logging
from datetime import date, timedelta

from google.cloud import bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    table_contains_matching_hash,
    get_existing_table_row_count,
)

logger = logging.getLogger(__name__)


def detect_late_data(hash_id, check_date, run_type):
    """
    Detect Late / Corrected Data (Scheduled Reconciliation Diff).

    Compare staged rates (fx_stage_long) from the explicit snapshot `hash_id`
    against core fact (fx_core_daily) over a bounded lookback window, and
    persist a reconciliation diff log for this scheduled run date.

    Workflow (scheduled runs):
        1) Check `LATE_DATA_DIFF_ENABLED` if False: log and return early (no-op).
        2) Validate stage exists and contains requested hash_id.
        3) Compute lookback_date = check_date - LATE_DATA_LOOKBACK_DAYS
        4) Replace diff for this check_date in fx_meta.fx_late_data_diff using one script job:
             DELETE WHERE check_date = @check_date;
             INSERT diff rows classified as:
                - late_insert (stage present, core missing)
                - late_update (both present, ABS(diff) > eps)
                - core_extra  (core present, stage missing)
        5) Summarize counts by diff_type for this check_date and log.

    Guarantees:
      - Idempotent per check_date: existing diff rows for check_date are deleted then re-inserted.

    Returns:
        dict: {"hash_id": str}
    """
    if not cfg.LATE_DATA_DIFF_ENABLED:
        logger.info("Step skipped by config settings: LATE_DATA_DIFF_ENABLED = False")
        return {"hash_id": hash_id}

    if run_type != "scheduled":
        logger.info(f"Step skipped (run_type={run_type!r}); only 'scheduled' runs produce diffs.")
        return {"hash_id": hash_id}

    bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
    stage_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_STAGE_DATASET}.{cfg.BQ_STAGE_LONG_TABLE}"
    core_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_CORE_DATASET}.{cfg.BQ_CORE_DAILY_TABLE}"
    diff_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_LATE_DATA_TABLE}"

    stage_rows = get_existing_table_row_count(bq_client, stage_fqn)
    if stage_rows is None or stage_rows == 0:
        raise RuntimeError(f"Stage table not found or empty: {stage_fqn}")

    if not table_contains_matching_hash(bq_client, stage_fqn, hash_id):
        raise RuntimeError(f"{stage_fqn} does not contain requested hash_id: {hash_id}")

    check_dt = date.fromisoformat(check_date)
    lookback_dt = check_dt - timedelta(days=cfg.LATE_DATA_LOOKBACK_DAYS)

    script = f"""
    BEGIN TRANSACTION;

    DELETE FROM `{diff_fqn}`
    WHERE check_date = @check_date;
    
    INSERT INTO `{diff_fqn}` (
      check_date,
      lookback_date,
      rate_date,
      currency,
      stage_hash_id,
      core_hash_id,
      stage_rate,
      core_rate,
      diff_type,
      detected_at
    )
    WITH
      stage AS (
        SELECT
          rate_date,
          currency,
          rate AS stage_rate,
          hash_id AS stage_hash_id
        FROM `{stage_fqn}`
        WHERE hash_id = @hash_id
          AND rate_date BETWEEN @lookback_date AND @check_date
      ),
      core AS (
        SELECT
          rate_date,
          currency,
          rate AS core_rate,
          hash_id AS core_hash_id
        FROM `{core_fqn}`
        WHERE rate_date BETWEEN @lookback_date AND @check_date
      ),
      joined AS (
        SELECT
          COALESCE(s.rate_date, c.rate_date) AS rate_date,
          COALESCE(s.currency, c.currency) AS currency,
          s.stage_hash_id,
          c.core_hash_id,
          s.stage_rate,
          c.core_rate
        FROM stage s
        FULL JOIN core c
          USING (rate_date, currency)
      )
    SELECT
      @check_date AS check_date,
      @lookback_date AS lookback_date,
      rate_date,
      currency,
      stage_hash_id,
      core_hash_id,
      stage_rate,
      core_rate,
      CASE
        WHEN stage_rate IS NOT NULL AND core_rate IS NULL THEN 'late_insert'
        WHEN stage_rate IS NULL AND core_rate IS NOT NULL THEN 'core_extra'
        WHEN stage_rate IS NOT NULL AND core_rate IS NOT NULL AND ABS(stage_rate - core_rate) > @eps THEN 'late_update'
        ELSE NULL
      END AS diff_type,
      CURRENT_TIMESTAMP() AS detected_at
    FROM joined
    WHERE
      (
        (stage_rate IS NOT NULL AND core_rate IS NULL)
        OR (stage_rate IS NULL AND core_rate IS NOT NULL)
        OR (stage_rate IS NOT NULL AND core_rate IS NOT NULL AND ABS(stage_rate - core_rate) > @eps)
      );
    
    COMMIT TRANSACTION;
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
            bigquery.ScalarQueryParameter("check_date", "DATE", check_dt),
            bigquery.ScalarQueryParameter("lookback_date", "DATE", lookback_dt),
            bigquery.ScalarQueryParameter("eps", "NUMERIC", cfg.LATE_DATA_RATE_EPS),
        ]
    )

    logger.info(f"Building late-data diff for check_date: {check_date} into {diff_fqn} ")

    job = bq_client.query(script, job_config=job_config)
    job.result()

    summary_sql = f"""
    SELECT diff_type, COUNT(*) AS cnt
    FROM `{diff_fqn}`
    WHERE check_date = @check_date
    GROUP BY diff_type
    ORDER BY diff_type
    """
    summary_cfg = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("check_date", "DATE", check_dt),
        ]
    )

    summary_rows = list(bq_client.query(summary_sql, job_config=summary_cfg).result())
    counts = {row["diff_type"]: int(row["cnt"]) for row in summary_rows}

    if counts:
        logger.warning(f"Late-data diff for check_date: {check_date}: {counts}")
    else:
        logger.info(f"No late data detected for check_date: {check_date} (diff rows = 0).")

    return {"hash_id": hash_id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_hash = "7cb71af19d2dc781c9ab3a40316559d0757af1b52199db5bd3fac2742ce0722f"
    test_rate_date = "2026-02-03"
    test_run_type = "scheduled"
    result = detect_late_data(test_hash, test_rate_date, test_run_type)
    logger.info(f"Done. Returned value was: {result}")
