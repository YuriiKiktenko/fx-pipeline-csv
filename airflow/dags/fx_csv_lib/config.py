import os


def _require_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Environment variable {name} is missing")
    return value


# ENV-based
GC_PROJECT_ID = _require_env("GC_PROJECT_ID")
FX_ZIP_URL = _require_env("FX_ZIP_URL")

# BQ fixed structure
BQ_META_DATASET = "fx_meta"
BQ_META_LOG_TABLE = "fx_ingestion_log"

BQ_RAW_SNAPSHOT_DATASET = "fx_raw_snapshots"
BQ_RAW_SNAPSHOT_TABLE_PREFIX = "fxsnap_"

BQ_RAW_DATASET = "fx_raw"
BQ_RAW_LONG_TABLE = "fx_raw_long"

BQ_STAGE_DATASET = "fx_stage"
BQ_STAGE_LONG_TABLE = "fx_stage_long"

BQ_CORE_DATASET = "fx_core"
BQ_CORE_DAILY_TABLE = "fx_core_daily"

# Google Buckets (depends on project_id)
GS_RAW_BUCKET = f"fx-raw-{GC_PROJECT_ID}"

# settings
MAX_ZIP_SIZE = 10 * 1024 * 1024  # 10 MB

REQUIRED_CURRENCIES = [
    "USD",
    "AUD",
    "CAD",
]

if __name__ == "__main__":
    print("Running config directly")
    print("GC_PROJECT_ID:", GC_PROJECT_ID)
    print("FX_ZIP_URL", FX_ZIP_URL)
