import os
import hashlib
import tempfile
import datetime as dt
import zipfile
import logging

import requests
from google.cloud import storage, bigquery

import fx_csv_lib.config as cfg

logger = logging.getLogger(__name__)

CHUNK_SIZE = 8192


def _download_zip_to_temp(path):
    resp = requests.get(cfg.FX_ZIP_URL, stream=True, timeout=30)
    resp.raise_for_status()

    with open(path, "wb") as f:
        total_bytes = 0
        for chunk in resp.iter_content(CHUNK_SIZE):
            if not chunk:
                continue
            total_bytes += len(chunk)
            if total_bytes > cfg.MAX_ZIP_SIZE:
                raise ValueError(f"Downloaded ZIP exceeds maximum size {cfg.MAX_ZIP_SIZE} bytes, received {total_bytes} bytes")
            f.write(chunk)
    if total_bytes == 0:
        raise ValueError("Downloaded ZIP has size 0 bytes")
    logger.info(f"Download finished: {total_bytes} bytes written to {path}")
    return total_bytes


def _compute_sha256(path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    digest = h.hexdigest()
    logger.info(f"SHA256 for {path}: {digest}")
    return digest


def _validate_zip(path):
    with zipfile.ZipFile(path, "r") as zf:
        bad_member = zf.testzip()
        if bad_member is not None:
            raise ValueError(f"Corrupted ZIP file: bad member '{bad_member}'")

        names = zf.namelist()

        if len(names) != 1:
            raise ValueError(f"ZIP must contain exactly one file and without nested folders, found {len(names)}: {names}")

        csv_name = names[0]

        if "/" in csv_name:
            raise ValueError(f"ZIP must contain a single file in the root folder, found nested path '{csv_name}'")

        if not csv_name.lower().endswith(".csv"):
            raise ValueError(f"ZIP file must contain a .csv file, found '{csv_name}'")
    logger.info(f"ZIP {path} validated successfully")


def ingest_zip_snapshot():
    """
    Download the ECB FX ZIP, validate it, upload to GCS and log metadata in BigQuery.

    Returns:
        dict: {
            "hash_id": str,
            "gcs_zip_uri": str,
            "new_snapshot": bool,
        }
    """
    tmp_path = None
    try:
        bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
        storage_client = storage.Client(project=cfg.GC_PROJECT_ID)

        log_table_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_LOG_TABLE}"

        with tempfile.NamedTemporaryFile(prefix="fx_zip_", suffix=".zip", delete=False) as tmp:
            tmp_path = tmp.name
        logger.info(f"Temp file {tmp_path} created")

        file_size = _download_zip_to_temp(tmp_path)
        _validate_zip(tmp_path)
        hash_id = _compute_sha256(tmp_path)

        query = f"""
        SELECT hash_id, gcs_zip_uri
        FROM `{log_table_fqn}`
        WHERE hash_id = @hash_id
        LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
        )

        job = bq_client.query(query, job_config=job_config)
        result = next(job.result(), None)
        if result:
            result_dict = {
                "hash_id": result["hash_id"],
                "gcs_zip_uri": result["gcs_zip_uri"],
                "new_snapshot": False,
            }
            logger.info("Existing snapshot found")
            return result_dict

        logger.info("No existing snapshot found, proceeding with upload")
        raw_bucket = storage_client.bucket(cfg.GS_RAW_BUCKET)
        zip_blob_path = f"zip/hash_id={hash_id}/eurofxref-hist.zip"
        zip_blob = raw_bucket.blob(zip_blob_path)
        zip_blob.upload_from_filename(tmp_path)

        gcs_zip_uri = f"gs://{cfg.GS_RAW_BUCKET}/{zip_blob_path}"
        ingested_at = dt.datetime.now(dt.timezone.utc)

        query = f"""
        INSERT INTO `{log_table_fqn}` (hash_id, gcs_zip_uri, file_size, ingested_at)
        VALUES (@hash_id, @gcs_zip_uri, @file_size, @ingested_at)
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
                bigquery.ScalarQueryParameter("gcs_zip_uri", "STRING", gcs_zip_uri),
                bigquery.ScalarQueryParameter("file_size", "INT64", file_size),
                bigquery.ScalarQueryParameter("ingested_at", "TIMESTAMP", ingested_at),
            ]
        )

        job = bq_client.query(query, job_config=job_config)
        job.result()

        result_dict = {
            "hash_id": hash_id,
            "gcs_zip_uri": gcs_zip_uri,
            "new_snapshot": True,
        }

        logger.info("Ingestion completed")
        return result_dict

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
