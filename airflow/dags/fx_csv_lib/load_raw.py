import logging

from google.cloud import storage, bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    create_schema,
    get_meta_row_by_hash,
    table_exists_and_populated,
    validate_meta_row,
    validate_gcs_blob,
    validate_csv_structure,
)

logger = logging.getLogger(__name__)


def load_raw_from_csv(hash_id):
    """
    Load a CSV snapshot (identified by hash_id) from GCS into a dedicated BigQuery raw snapshot table.

    Workflow:
        1) Compute deterministic target snapshot table name from hash_id.
        2) If snapshot table already exists and is populated -> return early.
        3) Read metadata row for hash_id (gcs_csv_uri, csv_size) and validate it.
        4) Validate that the referenced CSV object exists in GCS and matches expected size and hash_id metadata.
        5) Perform pre-load CSV validation on a small prefix:
            - detect delimiter,
            - validate header (Date + 3-letter currency codes),
            - handle trailing empty column (renamed to '_trailing_empty' if values are empty),
            - reject duplicates / empty column names.
        6) Generate BigQuery schema from the validated header (all STRING, NULLABLE).
        7) Load CSV into the snapshot table using a BigQuery load job:
            - CREATE_IF_NEEDED
            - WRITE_TRUNCATE (safe for retries)
            - skip_leading_rows=1
            - max_bad_records=0

    Guarantees:
        - Snapshot table identity is bound to hash_id by deterministic naming.
        - Step is idempotent (retries converge).

    Success contract:
        - Returns {"hash_id": <sha256>}.
        - A corresponding snapshot table exists in BigQuery and is populated.

    Returns:
        dict: {"hash_id": str}
    """

    bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
    log_table_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_LOG_TABLE}"
    raw_table_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_RAW_SNAPSHOT_DATASET}.{cfg.BQ_RAW_SNAPSHOT_TABLE_PREFIX}{hash_id}"

    storage_client = storage.Client(project=cfg.GC_PROJECT_ID)

    if table_exists_and_populated(bq_client, raw_table_fqn):
        logger.info(f"Raw snapshot table already exists and populated: {raw_table_fqn}")
        return {"hash_id": hash_id}

    logger.info(f"No existing raw snapshot table, target raw snapshot table: {raw_table_fqn}")
    meta_row = get_meta_row_by_hash(bq_client, log_table_fqn, hash_id, ["gcs_csv_uri", "csv_size"])
    validate_meta_row(meta_row, hash_id, ["gcs_csv_uri", "csv_size"])
    validate_gcs_blob(storage_client, meta_row["gcs_csv_uri"], hash_id, meta_row["csv_size"])
    gcs_csv_uri = meta_row["gcs_csv_uri"]
    logger.info(f"Source CSV: {gcs_csv_uri}")
    
    header_cols, delimiter = validate_csv_structure(storage_client, gcs_csv_uri)
    logger.info("CSV structure validated")

    schema = create_schema(header_cols)
    logger.info("Raw table schema created")

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        schema=schema,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        skip_leading_rows=1,
        field_delimiter=delimiter,
        quote_character='"',
        allow_quoted_newlines=False,
        max_bad_records=0,
        encoding="UTF-8",
    )

    job = bq_client.load_table_from_uri(gcs_csv_uri, raw_table_fqn, job_config=job_config)
    job.result()

    if not table_exists_and_populated(bq_client, raw_table_fqn):
        raise RuntimeError(f"Load completed but snapshot table not populated: {raw_table_fqn}")

    logger.info(f"Snapshot table loaded successfully: {raw_table_fqn}")
    return {"hash_id": hash_id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    hash_id = "5ed23bf70aad0a61c4e7c312883aa351c0496ed8bc16ca9a3369ee75b5e280fa"
    result = load_raw_from_csv(hash_id)
    logger.info(f"Done. Returned value was: {result}")
