import os
import tempfile
import datetime as dt
import zipfile
import logging

from google.cloud import storage, bigquery

import fx_csv_lib.config as cfg

logger = logging.getLogger(__name__)

CHUNK_SIZE = 8192


def _parse_gcs_uri(uri):
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")

    no_scheme = uri[5:]
    if "/" not in no_scheme:
        raise ValueError(f"Invalid GCS URI, expected gs://bucket/blob: {uri}")

    bucket, blob = no_scheme.split("/", 1)
    return bucket, blob


def extract_csv_from_zip(hash_id):
    """
    Download ZIP from GCS, extract the single CSV, upload it back to GCS
    under a deterministic hash-based path, and update metadata_log.

    Args:
        hash_id (str): snapshot (zip) hash

    Returns:
        dict: {
            "hash_id": str,
        }
    """
    zip_tmp_path = None
    csv_tmp_path = None

    try:
        bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
        storage_client = storage.Client(project=cfg.GC_PROJECT_ID)

        log_table_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_LOG_TABLE}"

        select_query = f"""
        SELECT gcs_zip_uri, gcs_csv_uri
        FROM `{log_table_fqn}`
        WHERE hash_id = @hash_id
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
        )
        job = bq_client.query(select_query, job_config=job_config)
        row = next(job.result(), None)
        if not row or not row["gcs_zip_uri"]:
            raise RuntimeError(f"metadata_log has no gcs_zip_uri for hash_id={hash_id}.")

        if gcs_csv_uri := row["gcs_csv_uri"]:
            bucket, blob = _parse_gcs_uri(gcs_csv_uri)
            if storage_client.bucket(bucket).blob(blob).exists():
                logger.info("CSV already extracted for this hash_id (meta + GCS confirmed)")
                return {"hash_id": hash_id}
            raise RuntimeError(f"Metadata points to missing CSV object: {gcs_csv_uri}")

        logger.info("No existing CSV found in meta, proceeding with extraction")
        with tempfile.NamedTemporaryFile(prefix="fx_zip_", suffix=".zip", delete=False) as tmp_zip:
            zip_tmp_path = tmp_zip.name
        logger.info(f"Temp file {zip_tmp_path} created")

        with tempfile.NamedTemporaryFile(prefix="fx_csv_", suffix=".csv", delete=False) as tmp_csv:
            csv_tmp_path = tmp_csv.name
        logger.info(f"Temp file {csv_tmp_path} created")

        gcs_zip_uri = row["gcs_zip_uri"]
        bucket_name, blob_name = _parse_gcs_uri(gcs_zip_uri)

        zip_blob = storage_client.bucket(bucket_name).blob(blob_name)
        if not zip_blob.exists():
            raise RuntimeError(f"Metadata points to missing ZIP object: {gcs_zip_uri}")
        zip_blob.download_to_filename(zip_tmp_path)

        logger.info(f"ZIP downloaded to {zip_tmp_path}")

        with zipfile.ZipFile(zip_tmp_path, "r") as zf:
            names = zf.namelist()
            if len(names) != 1:
                raise ValueError(f"ZIP must contain exactly one file, found {len(names)} files")

            csv_name = names[0]
            if not csv_name.lower().endswith(".csv"):
                raise ValueError(f"ZIP file must contain a .csv file, found '{csv_name}'")

            with zf.open(csv_name, "r") as src, open(csv_tmp_path, "wb") as dst:
                total_bytes = 0
                while chunk := src.read(CHUNK_SIZE):
                    dst.write(chunk)
                    total_bytes += len(chunk)
            if total_bytes == 0:
                raise RuntimeError("Extracted CSV has size 0 bytes")
        
        logger.info(f"CSV extracted to {csv_tmp_path}, size={total_bytes} bytes")

        raw_bucket = storage_client.bucket(cfg.GS_RAW_BUCKET)
        csv_blob_path = f"csv/hash_id={hash_id}/eurofxref-hist.csv"
        csv_blob = raw_bucket.blob(csv_blob_path)
        csv_blob.upload_from_filename(csv_tmp_path)
        csv_blob.reload()
        if csv_blob.size is None or csv_blob.size != total_bytes:
            raise RuntimeError(f"Uploaded CSV size mismatch: local={total_bytes}, gcs={csv_blob.size}")

        gcs_csv_uri = f"gs://{cfg.GS_RAW_BUCKET}/{csv_blob_path}"
        unpacked_at = dt.datetime.now(dt.timezone.utc)

        logger.info(f"CSV uploaded to {gcs_csv_uri}")

        update_query = f"""
        UPDATE `{log_table_fqn}`
        SET
          gcs_csv_uri = @gcs_csv_uri,
          csv_size    = @csv_size,
          unpacked_at = @unpacked_at
        WHERE hash_id = @hash_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
                bigquery.ScalarQueryParameter("gcs_csv_uri", "STRING", gcs_csv_uri),
                bigquery.ScalarQueryParameter("csv_size", "INT64", total_bytes),
                bigquery.ScalarQueryParameter("unpacked_at", "TIMESTAMP", unpacked_at),
            ]
        )

        job = bq_client.query(update_query, job_config=job_config)
        job.result()
        if job.num_dml_affected_rows == 0:
            raise RuntimeError(f"UPDATE affected 0 rows for hash_id={hash_id}")

        logger.info(f"Updating metadata_log, affected {job.num_dml_affected_rows} rows for hash_id={hash_id}")

        check_query = f"""
        SELECT gcs_csv_uri
        FROM `{log_table_fqn}`
        WHERE hash_id = @hash_id
        LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
            ]
        )
        job = bq_client.query(check_query, job_config=job_config)
        meta_row = next(job.result(), None)

        if meta_row:
            meta_uri = meta_row["gcs_csv_uri"]
            if meta_uri == gcs_csv_uri:
                logger.info("Extraction completed")
                return {"hash_id": hash_id}
            raise RuntimeError(f"Meta CSV URI mismatch for hash_id={hash_id}: meta={meta_uri}, expected={gcs_csv_uri}")
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
    hash_id = "474130726dc57aa39a422640c8359dd92b8131f3b14828e727a3ff3b1030dced"
    result = extract_csv_from_zip(hash_id)
    logger.info(f"Done. Returned value was: {result}")
