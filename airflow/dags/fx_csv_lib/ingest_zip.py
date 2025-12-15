import os
import hashlib
import tempfile
import datetime as dt
import zipfile
import logging

import requests
from google.cloud import storage, bigquery
from google.api_core.exceptions import PreconditionFailed

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


def _parse_gcs_uri(uri):
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")

    no_scheme = uri[5:]
    if "/" not in no_scheme:
        raise ValueError(f"Invalid GCS URI, expected gs://bucket/blob: {uri}")

    bucket, blob = no_scheme.split("/", 1)
    return bucket, blob


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


def _get_zip_uri_from_meta(bq_client, log_table_fqn, hash_id):
    select_query = f"""
    SELECT gcs_zip_uri
    FROM `{log_table_fqn}`
    WHERE hash_id = @hash_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
    )

    job = bq_client.query(select_query, job_config=job_config)
    row = next(job.result(), None)

    if row is None:
        return None

    if not row["gcs_zip_uri"]:
        raise RuntimeError(f"Metadata integrity error: bad gcs_zip_uri for hash_id={hash_id}")

    return row["gcs_zip_uri"]


def ingest_zip_snapshot():
    """
    Ingest a full-history ECB FX ZIP snapshot in an idempotent and integrity-safe manner.

    Processing steps:
      1) Download the ECB ZIP to a local temporary file (streamed, size-limited).
      2) Validate ZIP integrity and structure (exactly one CSV file at the root).
      3) Compute a SHA-256 hash_id of the ZIP content.
      4) Check the metadata table (BigQuery) for an existing record with this hash_id:
         - If found, verify that the referenced GCS object exists and that its custom
           metadata 'sha256' matches hash_id, then return early.
         - If metadata exists but the GCS object is missing or has a different hash,
           raise an integrity error.
      5) If no metadata record exists:
         - Upload is attempted atomically using if_generation_match=0 to avoid overwriting
           an existing object.
         - If the object already exists in GCS (from a previous ingestion or a concurrent run),
           it is reused without re-uploading.
         - The GCS object is validated by checking its size and custom metadata 'sha256'.
      6) Insert metadata into BigQuery using an insert-only MERGE.
         - If another process has already inserted the metadata row, the MERGE affects
           0 rows and is treated as a successful concurrent or recovered ingestion.

    Concurrency and idempotency guarantees:
      - GCS objects are content-addressed by SHA-256 and treated as immutable.
      - Multiple runs may execute concurrently or recover from missing metadata,
        but converge to a single consistent GCS object and metadata row.
      - No existing data is overwritten once successfully ingested.

    Returns:
        dict: {"hash_id": str}
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

        if gcs_zip_uri := _get_zip_uri_from_meta(bq_client, log_table_fqn, hash_id):
            bucket, blob_path = _parse_gcs_uri(gcs_zip_uri)
            blob = storage_client.bucket(bucket).blob(blob_path)
            if blob.exists():
                blob.reload()
                remote_sha = (blob.metadata or {}).get("sha256")
                if remote_sha == hash_id:
                    logger.info("Existing snapshot found in meta and correct ZIP exists in GCS")
                    return {"hash_id": hash_id}
                else:
                    raise RuntimeError(f"GCS sha256 mismatch for {gcs_zip_uri}: remote={remote_sha}, expected={hash_id}")
            raise RuntimeError(f"Metadata points to missing ZIP object: {gcs_zip_uri}")

        logger.info("No existing snapshot found in meta, proceeding with upload")
        raw_bucket = storage_client.bucket(cfg.GS_RAW_BUCKET)
        zip_blob_path = f"zip/hash_id={hash_id}/eurofxref-hist.zip"
        zip_blob = raw_bucket.blob(zip_blob_path)
        zip_blob.metadata = {"sha256": hash_id}

        try:
            zip_blob.upload_from_filename(tmp_path, if_generation_match=0)
            logger.info("Uploaded new ZIP")
        except PreconditionFailed:
            logger.info("ZIP already exists in GCS (possibly from a previous run or a concurrent ingestion), reusing it")

        zip_blob.reload()
        if zip_blob.size is None or zip_blob.size != file_size:
            raise RuntimeError(f"ZIP size mismatch: local={file_size}, gcs={zip_blob.size}")

        remote_sha = (zip_blob.metadata or {}).get("sha256")
        if remote_sha != hash_id:
            raise RuntimeError(f"ZIP hash mismatch: local={hash_id}, gcs={remote_sha}")

        gcs_zip_uri = f"gs://{cfg.GS_RAW_BUCKET}/{zip_blob_path}"
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
            logger.info(f"MERGE insert-only affected {job.num_dml_affected_rows} rows for hash_id={hash_id}")

        if meta_uri := _get_zip_uri_from_meta(bq_client, log_table_fqn, hash_id):
            if meta_uri == gcs_zip_uri:
                logger.info("Ingestion completed")
                return {"hash_id": hash_id}
            raise RuntimeError(f"Meta URI mismatch for hash_id={hash_id}: meta={meta_uri}, expected={gcs_zip_uri}")
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
