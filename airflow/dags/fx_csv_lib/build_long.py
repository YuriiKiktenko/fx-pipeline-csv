import logging

from google.cloud import bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    get_currency_columns_from_wide_snapshot,
    table_contains_matching_hash,
    get_existing_table_row_count,
)

logger = logging.getLogger(__name__)


def build_long_raw(hash_id):
    """
    Build the latest full-history long table (raw) from a raw snapshot wide table.

    Workflow:
        1) Compute wide raw snapshot table FQN from hash_id.
        2) Early exit if fx_raw_long already contains this hash_id.
        3) Validate snapshot table exists.
        4) Get currency columns from snapshot schema (exclude Date, _trailing_empty).
        5) CREATE OR REPLACE fx_raw_long using UNPIVOT (keep everything as STRING).
        6) Sanity check table is populated.

    Guarantees:
        - Output table name is stable (latest-only).
        - Atomic replacement via CREATE OR REPLACE TABLE.
        - Idempotent for same hash_id via early exit.

    Success contract:
        - Returns {"hash_id": <sha256>}.
        - fx_raw_long exists and contains rows stamped with hash_id.

    Returns:
        dict: {"hash_id": str}
    """
    bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
    raw_snapshot_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_RAW_SNAPSHOT_DATASET}.{cfg.BQ_RAW_SNAPSHOT_TABLE_PREFIX}{hash_id}"
    raw_long_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_RAW_DATASET}.{cfg.BQ_RAW_LONG_TABLE}"

    rows = get_existing_table_row_count(bq_client, raw_long_fqn)
    if rows is not None and rows > 0 and table_contains_matching_hash(bq_client, raw_long_fqn, hash_id):
        logger.info(f"fx_raw_long already built: {raw_long_fqn} for hash_id={hash_id} and has {rows} rows, early return")
        return {"hash_id": hash_id}

    logger.info(f"fx_raw_long is not built for hash_id={hash_id}, starting build")

    snap_rows = get_existing_table_row_count(bq_client, raw_snapshot_fqn)
    if snap_rows is None or snap_rows == 0:
        raise RuntimeError(f"Raw snapshot table not found or empty: {raw_snapshot_fqn}")
    logger.info(f"Raw snapshot table found: {raw_snapshot_fqn}, rows={snap_rows}")

    currency_cols = get_currency_columns_from_wide_snapshot(bq_client, raw_snapshot_fqn)
    logger.info(f"Currency columns detected: {len(currency_cols)}")

    in_list = ", ".join(f"`{c}`" for c in currency_cols)

    query = f"""
    CREATE OR REPLACE TABLE `{raw_long_fqn}` AS
    SELECT
      CAST(Date AS STRING) AS date_str,
      currency,
      CAST(rate AS STRING) AS rate_str,
      @hash_id AS hash_id
    FROM `{raw_snapshot_fqn}`
    UNPIVOT (rate FOR currency IN ({in_list}))
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
    )

    logger.info(f"Building long table: {raw_long_fqn} from {raw_snapshot_fqn}")
    job = bq_client.query(query, job_config=job_config)
    job.result()

    num_rows = get_existing_table_row_count(bq_client, raw_long_fqn)
    if num_rows is None or num_rows == 0:
        raise RuntimeError(f"fx_raw_long created but empty or missing: {raw_long_fqn}")

    if not table_contains_matching_hash(bq_client, raw_long_fqn, hash_id):
        raise RuntimeError(f"fx_raw_long does not contain matching hash_id: {raw_long_fqn}")

    logger.info(f"fx_raw_long for hash_id={hash_id} built successfully: {raw_long_fqn}, rows={num_rows}")
    return {"hash_id": hash_id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_hash = "5ed23bf70aad0a61c4e7c312883aa351c0496ed8bc16ca9a3369ee75b5e280fa"
    result = build_long_raw(test_hash)
    logger.info(f"Done. Returned value was: {result}")
