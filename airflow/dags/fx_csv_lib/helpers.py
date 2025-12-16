import requests
import hashlib
import zipfile

from google.cloud import bigquery

CHUNK_SIZE = 8192
ALLOWED_META_FIELDS = {"hash_id", "gcs_zip_uri", "gcs_csv_uri", "file_size"}


def parse_gcs_uri(uri):
    """
    Parse 'gs://bucket/blob' URI into (bucket, blob_path).
    """
    if not uri.startswith("gs://"):
        raise ValueError(f"Invalid GCS URI: {uri}")

    no_scheme = uri[5:]
    if "/" not in no_scheme:
        raise ValueError(f"Invalid GCS URI, expected gs://bucket/blob: {uri}")

    bucket, blob = no_scheme.split("/", 1)
    if not bucket or not blob:
        raise ValueError(f"Invalid GCS URI, expected gs://bucket/blob: {uri}")
    return bucket, blob


def compute_sha256(path):
    """
    Return SHA-256 hex digest of the file at given path.
    """
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(CHUNK_SIZE):
            h.update(chunk)
    digest = h.hexdigest()
    return digest


def download_stream_to_file(url, path, max_size):
    """
    Download a URL to a local file via streaming.
    Returns the number of bytes written.
    """
    resp = requests.get(url, stream=True, timeout=30)
    resp.raise_for_status()

    with open(path, "wb") as f:
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


def validate_zip(path):
    """
    Validate ZIP integrity and structure.
    Ensures exactly one CSV file at the root.
    """
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


def get_meta_row_by_hash(bq_client, table_fqn, hash_id, fields):
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


def validate_gcs_blob(storage_client, gcs_uri, expected_sha256, expected_size):
    """
    Validate that a GCS object exists and matches expected size and sha256.
    """
    bucket, blob_path = parse_gcs_uri(gcs_uri)
    blob = storage_client.bucket(bucket).blob(blob_path)

    if not blob.exists():
        raise RuntimeError(f"GCS object missing: {gcs_uri}")

    blob.reload()
    if blob.size != expected_size:
        raise RuntimeError(f"GCS size mismatch for {gcs_uri}: gcs={blob.size}, expected={expected_size}")

    remote_sha = (blob.metadata or {}).get("sha256")
    if expected_sha256 != remote_sha:
        raise RuntimeError(f"GCS sha256 mismatch for {gcs_uri}: gcs={remote_sha}, expected={expected_sha256}")


def validate_meta_row(meta_row, hash_id, required, expected=None):
    """
    Validate metadata row fields and optional expected values.
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
