import os
import tempfile
import datetime as dt
import logging

from google.cloud import storage, bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    download_blob_to_file,
    extract_csv,
    get_meta_row_by_hash,
    upload_file_to_blob,
    validate_gcs_blob,
    validate_meta_row,
)

logger = logging.getLogger(__name__)


def extract_csv_from_zip(hash_id):
    """
    Extract a CSV file from an already ingested ZIP snapshot in GCS.

    Workflow:
        1) Load metadata for the given hash_id and validate the ZIP snapshot.
        2) If CSV metadata already exists:
            - Validate metadata fields.
            - Verify the referenced CSV object exists in GCS and matches size and hash_id.
            - Return early.
        3) Otherwise:
            - Download the ZIP from GCS to a local temporary file.
            - Extract the single CSV file.
            - Upload the CSV to a deterministic, hash_id based GCS location.
            - Update metadata using a conditional MERGE.

    Guarantees:
        - CSV objects are immutable and content-addressed by ZIP hash_id.
        - Concurrent or repeated runs converge to a single consistent CSV snapshot.
        - No existing data is overwritten.

    Success contract:
        - Returns {"hash_id": <sha256>}.
        - A corresponding CSV object exists in GCS.
        - Metadata row contains consistent gcs_csv_uri and csv_size.

    Returns:
        dict: {"hash_id": str}
    """

    zip_tmp_path = None
    csv_tmp_path = None
    try:
        bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
        log_table_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_LOG_TABLE}"

        storage_client = storage.Client(project=cfg.GC_PROJECT_ID)

        csv_blob_path = f"csv/hash_id={hash_id}/eurofxref-hist.csv"
        gcs_csv_uri = f"gs://{cfg.GS_RAW_BUCKET}/{csv_blob_path}"

        meta_row = get_meta_row_by_hash(bq_client, log_table_fqn, hash_id, ["gcs_zip_uri", "zip_size", "gcs_csv_uri", "csv_size"])
        validate_meta_row(meta_row, hash_id, ["gcs_zip_uri", "zip_size"])
        validate_gcs_blob(storage_client, meta_row["gcs_zip_uri"], hash_id, meta_row["zip_size"])

        if meta_row["gcs_csv_uri"]:
            validate_meta_row(meta_row, hash_id, ["gcs_csv_uri", "csv_size"], {"gcs_csv_uri": gcs_csv_uri})
            validate_gcs_blob(storage_client, meta_row["gcs_csv_uri"], hash_id, meta_row["csv_size"])

            logger.info("CSV already extracted for this hash_id (meta + GCS confirmed)")
            return {"hash_id": hash_id}

        logger.info("No existing CSV found in meta, proceeding with extraction")
        with tempfile.NamedTemporaryFile(prefix="fx_zip_", suffix=".zip", delete=False) as tmp_zip:
            zip_tmp_path = tmp_zip.name
        logger.info(f"Temp file {zip_tmp_path} created")

        with tempfile.NamedTemporaryFile(prefix="fx_csv_", suffix=".csv", delete=False) as tmp_csv:
            csv_tmp_path = tmp_csv.name
        logger.info(f"Temp file {csv_tmp_path} created")

        download_blob_to_file(storage_client, meta_row["gcs_zip_uri"], zip_tmp_path)
        logger.info(f"ZIP downloaded to {zip_tmp_path}")

        total_bytes = extract_csv(zip_tmp_path, csv_tmp_path)
        logger.info(f"CSV extracted to {csv_tmp_path}, size={total_bytes} bytes")

        log_msg = upload_file_to_blob(storage_client, gcs_csv_uri, csv_tmp_path, hash_id)
        validate_gcs_blob(storage_client, gcs_csv_uri, hash_id, total_bytes)

        logger.info(f"{log_msg}: {gcs_csv_uri}")
        unpacked_at = dt.datetime.now(dt.timezone.utc)

        merge_query = f"""
        MERGE `{log_table_fqn}` T
        USING (
            SELECT
                @hash_id     AS hash_id,
                @gcs_csv_uri AS gcs_csv_uri,
                @csv_size    AS csv_size,
                @unpacked_at AS unpacked_at
        ) S
        ON T.hash_id = S.hash_id
        WHEN MATCHED AND T.gcs_csv_uri IS NULL THEN
            UPDATE SET
                gcs_csv_uri = S.gcs_csv_uri,
                csv_size    = S.csv_size,
                unpacked_at = S.unpacked_at
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
                bigquery.ScalarQueryParameter("gcs_csv_uri", "STRING", gcs_csv_uri),
                bigquery.ScalarQueryParameter("csv_size", "INT64", total_bytes),
                bigquery.ScalarQueryParameter("unpacked_at", "TIMESTAMP", unpacked_at),
            ]
        )

        job = bq_client.query(merge_query, job_config=job_config)
        job.result()

        rows_affected = job.num_dml_affected_rows or 0
        if rows_affected == 0:
            logger.info(f"MERGE updated 0 rows for hash_id={hash_id} (likely concurrent extraction already updated metadata)")
        else:
            logger.info(f"MERGE updated {rows_affected} rows in metadata for hash_id={hash_id}")

        if meta_row := get_meta_row_by_hash(bq_client, log_table_fqn, hash_id, ["gcs_csv_uri", "csv_size"]):
            validate_meta_row(
                meta_row, hash_id, ["gcs_csv_uri", "csv_size"], {"gcs_csv_uri": gcs_csv_uri, "csv_size": total_bytes}
            )

            logger.info("Extraction completed")
            return {"hash_id": hash_id}
        raise RuntimeError(f"Extraction completed but meta row missing for hash_id={hash_id}")

    finally:
        if zip_tmp_path and os.path.exists(zip_tmp_path):
            try:
                os.remove(zip_tmp_path)
                logger.info(f"Temp ZIP file {zip_tmp_path} deleted")
            except Exception as err:
                logger.warning(f"Failed to delete temp ZIP file {zip_tmp_path}: {err}")

        if csv_tmp_path and os.path.exists(csv_tmp_path):
            try:
                os.remove(csv_tmp_path)
                logger.info(f"Temp CSV file {csv_tmp_path} deleted")
            except Exception as err:
                logger.warning(f"Failed to delete temp CSV file {csv_tmp_path}: {err}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    hash_id = "004fd4f892e51885594fcc47d411fad7b012bc34c1608ca0c38560934d268516"
    result = extract_csv_from_zip(hash_id)
    logger.info(f"Done. Returned value was: {result}")
