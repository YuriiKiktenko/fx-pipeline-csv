import logging

from google.cloud import bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    table_contains_matching_hash,
    get_existing_table_row_count,
)

logger = logging.getLogger(__name__)


def build_long_stage(hash_id):
    """
    Build typed & validated full-history long staging table from fx_raw_long.

    Workflow:
        1) Early exit if fx_stage_long already contains this hash_id and is populated.
        2) Validate fx_raw_long exists, is non-empty, and contains the requested hash_id.
        3) CREATE OR REPLACE candidate table with:
            - SAFE_CAST(date_str AS DATE) AS rate_date
            - currency = UPPER(TRIM(currency))
            - rate = SAFE_CAST(NULLIF(NULLIF(TRIM(rate_str), ''), 'N/A') AS NUMERIC)
            - Filter out NULL typed fields and empty currency
            - SELECT DISTINCT to remove full-duplicate rows
        4) Validate candidate (fail-fast):
            - currency format matches ^[A-Z]{3}$
            - rate > 0
            - no duplicate keys on (rate_date, currency)
        5) Promote candidate into fx_stage_long and validate it is populated and contains hash_id.

    Guarantees:
        - Final table is updated only if validations pass (candidate/promote pattern).
        - Atomic replacement of fx_stage_long.
        - Idempotent for the same hash_id.

    Success contract:
        - Returns {"hash_id": <sha256>}.
        - fx_stage_long exists, is non-empty, and contains only validated rows for hash_id.

    Returns:
        dict: {"hash_id": str}
    """
    bq_client = None
    stage_long_candidate_fqn = None
    try:
        bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
        raw_long_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_RAW_DATASET}.{cfg.BQ_RAW_LONG_TABLE}"
        stage_long_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_STAGE_DATASET}.{cfg.BQ_STAGE_LONG_TABLE}"
        stage_long_candidate_fqn = f"{stage_long_fqn}_candidate"

        stage_rows = get_existing_table_row_count(bq_client, stage_long_fqn)
        if stage_rows is not None and stage_rows > 0 and table_contains_matching_hash(bq_client, stage_long_fqn, hash_id):
            logger.info(
                f"{cfg.BQ_STAGE_LONG_TABLE} already built: {stage_long_fqn} for hash_id={hash_id} and has {stage_rows} rows, early return"
            )
            return {"hash_id": hash_id}

        logger.info(f"{cfg.BQ_STAGE_LONG_TABLE} is not built for hash_id={hash_id}, starting build")

        raw_rows = get_existing_table_row_count(bq_client, raw_long_fqn)
        if raw_rows is None or raw_rows == 0:
            raise RuntimeError(f"{cfg.BQ_RAW_LONG_TABLE} table not found or empty: {raw_long_fqn}")
        if not table_contains_matching_hash(bq_client, raw_long_fqn, hash_id):
            raise RuntimeError(f"{raw_long_fqn} table does not contain searched hash_id: {hash_id}")
        logger.info(f"{cfg.BQ_RAW_LONG_TABLE} table found: {raw_long_fqn}, rows={raw_rows}")

        build_candidate_sql = f"""
        CREATE OR REPLACE TABLE `{stage_long_candidate_fqn}` AS
        WITH typed AS (
        SELECT
            SAFE_CAST(date_str AS DATE) AS rate_date,
            UPPER(TRIM(currency)) AS currency,
            SAFE_CAST(NULLIF(NULLIF(TRIM(rate_str), ''), 'N/A') AS NUMERIC) AS rate,
            hash_id
        FROM `{raw_long_fqn}`
        WHERE hash_id = @hash_id
        ),
        filtered AS (
        SELECT
            rate_date,
            currency,
            rate,
            hash_id
        FROM typed
        WHERE rate_date IS NOT NULL
            AND currency IS NOT NULL AND currency != ''
            AND rate IS NOT NULL
        )
        SELECT DISTINCT
        rate_date,
        currency,
        rate,
        hash_id
        FROM filtered
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
        )

        logger.info(f"Building candidate stage table: {stage_long_candidate_fqn} from {raw_long_fqn}")

        job = bq_client.query(build_candidate_sql, job_config=job_config)
        job.result()

        cand_rows = get_existing_table_row_count(bq_client, stage_long_candidate_fqn)
        if cand_rows is None or cand_rows == 0:
            raise RuntimeError(f"Candidate stage table created but empty or missing: {stage_long_candidate_fqn}")

        if not table_contains_matching_hash(bq_client, stage_long_candidate_fqn, hash_id):
            raise RuntimeError(f"Candidate stage table does not contain matching hash_id: {stage_long_candidate_fqn}")

        logger.info(f"Candidate built: {stage_long_candidate_fqn}, rows={cand_rows}")

        bad_currency_sql = f"""
        SELECT 1
        FROM `{stage_long_candidate_fqn}`
        WHERE hash_id = @hash_id
        AND NOT REGEXP_CONTAINS(currency, r'^[A-Z]{{3}}$')
        LIMIT 1
        """
        bad_currency_job = bq_client.query(bad_currency_sql, job_config=job_config)
        if list(bad_currency_job.result()):
            raise RuntimeError(
                f"Validation failed: invalid currency format in {stage_long_candidate_fqn} (expected ^[A-Z]{{3}}$)"
            )

        bad_rate_sql = f"""
        SELECT 1
        FROM `{stage_long_candidate_fqn}`
        WHERE hash_id = @hash_id
        AND rate <= 0
        LIMIT 1
        """
        bad_rate_job = bq_client.query(bad_rate_sql, job_config=job_config)
        if list(bad_rate_job.result()):
            raise RuntimeError(f"Validation failed: rate <= 0 found in {stage_long_candidate_fqn}")

        dup_key_sql = f"""
        SELECT 1
        FROM `{stage_long_candidate_fqn}`
        WHERE hash_id = @hash_id
        GROUP BY rate_date, currency
        HAVING COUNT(*) > 1
        LIMIT 1
        """
        dup_key_job = bq_client.query(dup_key_sql, job_config=job_config)
        if list(dup_key_job.result()):
            raise RuntimeError(f"Validation failed: duplicate keys (rate_date,currency) in {stage_long_candidate_fqn}")

        logger.info(f"Candidate validated successfully for hash_id={hash_id}: {stage_long_candidate_fqn}")

        promote_sql = f"""
        CREATE OR REPLACE TABLE `{stage_long_fqn}` AS
        SELECT * FROM `{stage_long_candidate_fqn}`
        WHERE hash_id = @hash_id
        """
        logger.info(f"Promoting candidate to final stage table: {stage_long_fqn}")

        promote_job = bq_client.query(promote_sql, job_config=job_config)
        promote_job.result()

        num_rows = get_existing_table_row_count(bq_client, stage_long_fqn)
        if num_rows is None or num_rows == 0:
            raise RuntimeError(f"Final stage table created but empty or missing: {stage_long_fqn}")

        if not table_contains_matching_hash(bq_client, stage_long_fqn, hash_id):
            raise RuntimeError(f"Final stage table does not contain matching hash_id: {stage_long_fqn}")

        logger.info(f"Published stage table: {stage_long_fqn} for hash_id={hash_id}, rows={num_rows}")
        return {"hash_id": hash_id}

    finally:
        if bq_client is not None and stage_long_candidate_fqn is not None:
            try:
                bq_client.query(f"DROP TABLE IF EXISTS `{stage_long_candidate_fqn}`").result()
            except Exception as e:
                logger.warning(f"Failed to drop candidate table {stage_long_candidate_fqn}: {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_hash = "5ed23bf70aad0a61c4e7c312883aa351c0496ed8bc16ca9a3369ee75b5e280fa"
    result = build_long_stage(test_hash)
    logger.info(f"Done. Returned value was: {result}")
