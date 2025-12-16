import os
import tempfile
import datetime as dt
import logging

from google.cloud import storage, bigquery
from google.api_core.exceptions import PreconditionFailed

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    compute_sha256,
    download_stream_to_file,
    validate_zip,
    get_meta_row_by_hash,
    validate_gcs_blob,
    validate_meta_row,
)

logger = logging.getLogger(__name__)


def ingest_zip_snapshot():
    """
    Ingest a full-history ECB FX ZIP snapshot in an idempotent and integrity-safe manner.

    Workflow:
        1) Download the ECB ZIP to a local temporary file (streamed, size-limited).
        2) Validate ZIP integrity and structure (single CSV at root).
        3) Compute a SHA-256 hash_id of the ZIP content.
        4) If metadata for this hash_id already exists:
            - Validate required metadata fields.
            - Verify the referenced GCS object exists and matches hash_id and size.
            - Return early.
        5) Otherwise:
            - Upload the ZIP to a content-addressed GCS location.
            - Validate the final object.
            - Insert metadata using an insert-only MERGE.

    Guarantees:
        - GCS objects are immutable and content-addressed by SHA-256.
        - Concurrent or repeated runs converge to a single consistent snapshot.
        - No existing data is overwritten.

    Success contract:
        - Returns {"hash_id": <sha256>} for the processed ZIP.
        - A corresponding GCS object and metadata row exist and are mutually consistent.

    Returns:
        dict: {"hash_id": str}
    """

    tmp_path = None
    try:
        bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
        log_table_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_LOG_TABLE}"

        storage_client = storage.Client(project=cfg.GC_PROJECT_ID)
        raw_bucket = storage_client.bucket(cfg.GS_RAW_BUCKET)

        with tempfile.NamedTemporaryFile(prefix="fx_zip_", suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name
        logger.info(f"Temp file {tmp_path} created")

        file_size = download_stream_to_file(cfg.FX_ZIP_URL, tmp_path, cfg.MAX_ZIP_SIZE)
        logger.info(f"Download finished: {file_size} bytes written to {tmp_path}")

        validate_zip(tmp_path)
        logger.info(f"ZIP {tmp_path} validated successfully")

        hash_id = compute_sha256(tmp_path)
        logger.info(f"SHA256 for {tmp_path}: {hash_id}")

        zip_blob_path = f"zip/hash_id={hash_id}/eurofxref-hist.zip"
        gcs_zip_uri = f"gs://{cfg.GS_RAW_BUCKET}/{zip_blob_path}"

        if meta_row := get_meta_row_by_hash(bq_client, log_table_fqn, hash_id, ["gcs_zip_uri", "file_size"]):
            validate_meta_row(meta_row, hash_id, ["gcs_zip_uri", "file_size"], {"gcs_zip_uri": gcs_zip_uri})
            validate_gcs_blob(storage_client, meta_row["gcs_zip_uri"], hash_id, meta_row["file_size"])

            logger.info("Existing snapshot found in meta and correct ZIP exists in GCS")
            return {"hash_id": hash_id}

        logger.info("No existing snapshot found in meta, proceeding with upload")
        zip_blob = raw_bucket.blob(zip_blob_path)
        zip_blob.metadata = {"sha256": hash_id}

        try:
            zip_blob.upload_from_filename(tmp_path, if_generation_match=0)
            logger.info("Uploaded new ZIP")
        except PreconditionFailed:
            logger.info("ZIP already exists in GCS (possibly from a previous run or a concurrent ingestion), reusing it")

        validate_gcs_blob(storage_client, gcs_zip_uri, hash_id, file_size)
        ingested_at = dt.datetime.now(dt.timezone.utc)

        merge_query = f"""
        MERGE `{log_table_fqn}` T
        USING (
            SELECT
                @hash_id     AS hash_id,
                @gcs_zip_uri AS gcs_zip_uri,
                @file_size   AS file_size,
                @ingested_at AS ingested_at
        ) S
        ON T.hash_id = S.hash_id
        WHEN NOT MATCHED THEN
        INSERT (hash_id, gcs_zip_uri, file_size, ingested_at)
        VALUES (S.hash_id, S.gcs_zip_uri, S.file_size, S.ingested_at)
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
                bigquery.ScalarQueryParameter("gcs_zip_uri", "STRING", gcs_zip_uri),
                bigquery.ScalarQueryParameter("file_size", "INT64", file_size),
                bigquery.ScalarQueryParameter("ingested_at", "TIMESTAMP", ingested_at),
            ]
        )

        job = bq_client.query(merge_query, job_config=job_config)
        job.result()

        affected = job.num_dml_affected_rows or 0
        if affected == 0:
            logger.info(f"MERGE inserted 0 rows for hash_id={hash_id} (likely concurrent ingestion already inserted metadata)")
        else:
            logger.info(f"MERGE insert-only affected {job.num_dml_affected_rows} rows in metadata for hash_id={hash_id}")

        if meta_row := get_meta_row_by_hash(bq_client, log_table_fqn, hash_id, ["gcs_zip_uri", "file_size"]):
            validate_meta_row(meta_row, hash_id, ["gcs_zip_uri", "file_size"], {"gcs_zip_uri": gcs_zip_uri})

            logger.info("Ingestion completed")
            return {"hash_id": hash_id}
        raise RuntimeError(f"Ingestion completed but meta row missing for hash_id={hash_id}")

    finally:
        if tmp_path and os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
                logger.info(f"Temp file {tmp_path} deleted")
            except Exception as err:
                logger.warning(f"Failed to delete temp file {tmp_path}: {err}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    result = ingest_zip_snapshot()
    logger.info(f"Done. Returned value was: {result}")
