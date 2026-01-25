import requests
import hashlib
import zipfile
import csv

from google.cloud import bigquery
from google.cloud.storage import Client as StorageClient
from google.cloud.bigquery import Client as BigQueryClient
from google.api_core.exceptions import PreconditionFailed
from google.api_core.exceptions import NotFound

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
    Returns a dict with hash_id and requested fields or None.
    """
    if not fields:
        raise ValueError("fields must be a non-empty list")

    select_list = list(dict.fromkeys(["hash_id", *fields]))

    bad_fields = set(select_list) - ALLOWED_META_FIELDS
    if bad_fields:
        raise ValueError(f"Unsupported fields requested: {sorted(bad_fields)}")

    select_fields = ", ".join(select_list)

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

    return {field: row[field] for field in select_list}


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

    if "hash_id" not in meta_row:
        raise RuntimeError("Missing 'hash_id' field in meta row")

    got_hash = meta_row["hash_id"]
    if got_hash != hash_id:
        raise RuntimeError(f"Metadata integrity error: hash_id mismatch: got={got_hash!r}, expected={hash_id!r}")

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
                raise RuntimeError(
                    f"Metadata integrity error: {key} mismatch for hash_id={hash_id}: got={got!r}, expected={exp!r}"
                )


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


def get_existing_table_row_count(bq_client: bigquery.Client, table_fqn: str) -> int | None:
    """
    Returns row count if table exists or None.
    """
    try:
        table = bq_client.get_table(table_fqn)
    except NotFound:
        return None
    return table.num_rows


def validate_csv_structure(storage_client: StorageClient, gcs_csv_uri: str) -> tuple[list[str], str]:
    """
    Validates and returns list of columns names and delimiter in the csv.
    """
    bucket, blob_path = parse_gcs_uri(gcs_csv_uri)
    blob = storage_client.bucket(bucket).blob(blob_path)

    prefix_bytes = blob.download_as_bytes(start=0, end=64 * 1024)

    text = prefix_bytes.decode("utf-8-sig", errors="strict")

    lines = [ln for ln in text.splitlines() if ln.strip()]
    if len(lines) < 2:
        raise ValueError("CSV must contain header and at least one data row")

    for delimiter in [",", ";", "\t"]:
        reader = csv.reader([lines[0], lines[1]], delimiter=delimiter, quotechar='"')
        header = next(reader)
        row = next(reader)

        if len(header) >= 2 and len(header) == len(row):
            header_cols = [c.strip() for c in header]
            if header_cols[-1] == "":
                if row[-1].strip() == "":
                    header_cols[-1] = "_trailing_empty"
                else:
                    raise ValueError("Trailing empty column contains values")

            if any(col == "" for col in header_cols):
                raise ValueError("Empty column names in header")

            if len(set(header_cols)) != len(header_cols):
                raise ValueError("Duplicate column names in header")

            if header_cols[0] != "Date":
                raise ValueError(f"First column must be 'Date', got {header_cols[0]!r}")

            for col in header_cols[1:]:
                if col == "_trailing_empty":
                    continue
                if len(col) != 3 or not col.isalpha() or not col.isupper():
                    raise ValueError(f"Invalid currency column name {col!r}; expected 3-letter code like 'USD'")

            return header_cols, delimiter
    raise ValueError("Unable to detect CSV delimiter")


def create_schema(header_cols: list[str]) -> list[bigquery.SchemaField]:
    """
    Returns all strings schema.
    """
    if not header_cols:
        raise ValueError("Cannot create schema from empty header")

    return [bigquery.SchemaField(col, "STRING", mode="NULLABLE") for col in header_cols]


def table_contains_matching_hash(bq_client: bigquery.Client, table_fqn: str, hash_id: str) -> bool:
    """
    Returns True if table contains at least one row with hash_id = given hash_id.
    """
    query = f"""
    SELECT 1
    FROM `{table_fqn}`
    WHERE hash_id = @hash_id
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id)],
    )

    job = bq_client.query(query, job_config=job_config)
    row = next(job.result(), None)

    return row is not None


def get_currency_columns_from_wide_snapshot(bq_client: bigquery.Client, raw_snapshot_fqn: str) -> list[str]:
    """
    Read snapshot wide table schema and return list of currency columns to UNPIVOT.
    Excludes Date and _trailing_empty.
    """
    table = bq_client.get_table(raw_snapshot_fqn)
    cols = [f.name for f in table.schema]

    excluded = {"Date", "_trailing_empty"}
    currency_cols = [c for c in cols if c not in excluded]

    if not currency_cols:
        raise RuntimeError(f"No currency columns found in snapshot table schema: {raw_snapshot_fqn}")

    return currency_cols


def table_contains_matching_hash_and_date(
    bq_client: bigquery.Client,
    table_fqn: str,
    hash_id: str,
    rate_date: str,
) -> bool:
    """
    Returns True if table contains at least one row with rate_date = given rate_date and hash_id = given hash_id.
    """
    query = f"""
    SELECT 1
    FROM `{table_fqn}`
    WHERE hash_id = @hash_id
    AND rate_date = @rate_date
    LIMIT 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
            bigquery.ScalarQueryParameter("rate_date", "DATE", rate_date),
        ],
    )

    job = bq_client.query(query, job_config=job_config)
    row = next(job.result(), None)

    return row is not None


def get_missing_required_currencies(
    bq_client: bigquery.Client,
    table_fqn: str,
    hash_id: str,
    rate_date: str,
    required: list[str],
) -> list[str]:
    """
    Returns a list of missing required currencies for (hash_id, rate_date).
    """
    if not required:
        return []

    query = f"""
    WITH required AS (
      SELECT currency
      FROM UNNEST(@required_currencies) AS currency
    ),
    present AS (
      SELECT DISTINCT currency
      FROM `{table_fqn}`
      WHERE hash_id = @hash_id
        AND rate_date = @rate_date
    )
    SELECT r.currency AS missing_currency
    FROM required r
    LEFT JOIN present p
      ON p.currency = r.currency
    WHERE p.currency IS NULL
    ORDER BY missing_currency
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("hash_id", "STRING", hash_id),
            bigquery.ScalarQueryParameter("rate_date", "DATE", rate_date),
            bigquery.ArrayQueryParameter("required_currencies", "STRING", list(required)),
        ],
    )
    rows = bq_client.query(query, job_config=job_config).result()
    return [row["missing_currency"] for row in rows]
