import requests
import hashlib
import zipfile

from google.cloud import bigquery
from google.cloud.storage import Client as StorageClient
from google.cloud.bigquery import Client as BigQueryClient
from google.api_core.exceptions import PreconditionFailed

CHUNK_SIZE = 8192
ALLOWED_META_FIELDS = {
    "hash_id",
    "gcs_zip_uri",
    "zip_size",
    "ingested_at",
    "gcs_csv_uri",
    "csv_size",
    "unpacked_at",
}


def parse_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    """
    Parse 'gs://bucket/blob' into (bucket, blob_path). Raises ValueError if invalid.
    """
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {gcs_uri}")

    no_scheme = gcs_uri[5:]
    if "/" not in no_scheme:
        raise ValueError(f"Invalid GCS URI, expected gs://bucket/blob: {gcs_uri}")

    bucket, blob = no_scheme.split("/", 1)
    if not bucket or not blob:
        raise ValueError(f"Invalid GCS URI, expected gs://bucket/blob: {gcs_uri}")
    return bucket, blob


def compute_sha256(local_path: str) -> str:
    """
    Return SHA-256 hex digest of the file at given path.
    """
    h = hashlib.sha256()
    with open(local_path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    digest = h.hexdigest()
    return digest


def download_stream_to_file(url: str, local_path: str, max_size: int) -> int:
    """
    Download URL to local file with size limit.
    Returns bytes written.
    """
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()

    with open(local_path, "wb") as f:
        total_bytes = 0
        for chunk in resp.iter_content(CHUNK_SIZE):
            if not chunk:
                continue
            total_bytes += len(chunk)
            if total_bytes > max_size:
                raise ValueError(f"Downloaded file exceeds limit: max={max_size} bytes, got={total_bytes} bytes")
            f.write(chunk)
    if total_bytes == 0:
        raise ValueError("Downloaded file has size 0 bytes")
    return total_bytes


def download_blob_to_file(storage_client: StorageClient, gcs_uri: str, path: str) -> None:
    """
    Download a GCS object (gs://bucket/blob) to a local file.
    """
    bucket_name, blob_name = parse_gcs_uri(gcs_uri)
    blob = storage_client.bucket(bucket_name).blob(blob_name)
    blob.download_to_filename(path)


def validate_zip(local_path: str) -> str:
    """
    Validate ZIP integrity and structure.
    Ensures exactly one CSV file at the root and returns its name.
    Raises if invalid.
    """
    with zipfile.ZipFile(local_path, "r") as zf:
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
    return csv_name


def get_meta_row_by_hash(bq_client: BigQueryClient, table_fqn: str, hash_id: str, fields: list[str]) -> dict | None:
    """
    Fetch a metadata row by hash_id.
    Returns a dict with requested fields or None.
    """
    if not fields:
        raise ValueError("fields must be a non-empty list")

    bad_fields = set(fields) - ALLOWED_META_FIELDS
    if bad_fields:
        raise ValueError(f"Unsupported fields requested: {sorted(bad_fields)}")

    select_fields = ", ".join(fields)

    query = f"""
    SELECT {select_fields}
    FROM `{table_fqn}`
    WHERE hash_id = @hash_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
    )
    job = bq_client.query(query, job_config=job_config)
    row = next(job.result(), None)

    if row is None:
        return None

    return {field: row[field] for field in fields}


def validate_gcs_blob(storage_client: StorageClient, gcs_uri: str, expected_hash_id: str, expected_size: int) -> None:
    """
    Validate that a GCS object exists and matches expected size and hash_id.
    Raises if invalid.
    """
    bucket, blob_path = parse_gcs_uri(gcs_uri)
    blob = storage_client.bucket(bucket).blob(blob_path)

    if not blob.exists():
        raise RuntimeError(f"GCS object missing: {gcs_uri}")

    blob.reload()
    if blob.size is None:
        raise RuntimeError(f"GCS size is unknown for {gcs_uri}")

    if blob.size != expected_size:
        raise RuntimeError(f"GCS size mismatch for {gcs_uri}: gcs={blob.size}, expected={expected_size}")

    remote_hash_id = (blob.metadata or {}).get("hash_id")
    if expected_hash_id != remote_hash_id:
        raise RuntimeError(f"GCS hash_id mismatch for {gcs_uri}: gcs={remote_hash_id}, expected={expected_hash_id}")


def validate_meta_row(meta_row: dict | None, hash_id: str, required: list[str], expected: dict | None = None) -> None:
    """
    Validate metadata row fields and optional expected values.
    Raises if invalid.
    """
    if meta_row is None:
        raise RuntimeError(f"Metadata missing for hash_id={hash_id}")

    for key in required:
        if key not in meta_row:
            raise RuntimeError(f"Metadata integrity error: missing {key} for hash_id={hash_id}")
        val = meta_row[key]
        if val is None or val == "":
            raise RuntimeError(f"Metadata integrity error: missing {key} value for hash_id={hash_id}")

    if expected:
        for key, exp in expected.items():
            got = meta_row.get(key)
            if got != exp:
                raise RuntimeError(f"Metadata integrity error: {key} mismatch for hash_id={hash_id}: got={got}, expected={exp}")


def extract_csv(zip_path: str, csv_path: str) -> int:
    """
    Extract the single CSV file from a validated ZIP archive to a local file.
    Returns the number of bytes written.
    """
    csv_name = validate_zip(zip_path)

    with zipfile.ZipFile(zip_path, "r") as zf:
        with zf.open(csv_name, "r") as src, open(csv_path, "wb") as dst:
            total_bytes = 0
            while chunk := src.read(CHUNK_SIZE):
                dst.write(chunk)
                total_bytes += len(chunk)

    if total_bytes == 0:
        raise RuntimeError("Extracted CSV has size 0 bytes")

    return total_bytes


def upload_file_to_blob(storage_client: StorageClient, gcs_uri: str, local_path: str, hash_id: str) -> str:
    """
    Upload a local file to GCS atomically if the object does not exist.
    Sets custom metadata {"hash_id": hash_id}.
    Returns a short status message for logging.
    """
    bucket, blob_path = parse_gcs_uri(gcs_uri)
    blob = storage_client.bucket(bucket).blob(blob_path)
    blob.metadata = {"hash_id": hash_id}

    try:
        blob.upload_from_filename(local_path, if_generation_match=0)
        return "Uploaded new object to GCS"
    except PreconditionFailed:
        return "GCS object already exists"
