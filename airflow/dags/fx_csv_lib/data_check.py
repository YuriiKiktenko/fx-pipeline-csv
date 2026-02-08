import logging
from datetime import date

from google.cloud import bigquery

import fx_csv_lib.config as cfg
from fx_csv_lib.helpers import (
    fetch_baseline_date,
    fetch_core_meta_row,
    is_expected_data_day,
    convert_to_dict,
)

logger = logging.getLogger(__name__)


def check_data_quality(hash_id, rate_date):
    """
    Check Data Availability & Quality.

    Uses fx_meta.<core_meta_table> to validate that the loaded day looks consistent
    vs the previous available day in meta.

    Current policy (simplified):
      - FAIL if required currencies are missing or have large jumps.
      - FAIL if volume drift is above configured thresholds.
      - Skip on non-expected data days (weekend/holiday).
      - If no baseline day exists -> skip comparisons (first day in meta).

    Returns:
        dict: {"hash_id": str}
    """
    bq_client = bigquery.Client(project=cfg.GC_PROJECT_ID)
    meta_fqn = f"{cfg.GC_PROJECT_ID}.{cfg.BQ_META_DATASET}.{cfg.BQ_META_CORE_TABLE}"

    rate_dt = date.fromisoformat(rate_date)

    if not is_expected_data_day(rate_dt, cfg.HOLIDAYS):
        logger.info(f"Day not expected to have data (weekend/holiday): {rate_date}. Skipping monitoring.")
        return {"hash_id": hash_id}

    new_data = fetch_core_meta_row(bq_client, meta_fqn, rate_dt)
    if new_data is None:
        raise RuntimeError(f"Post-merge monitoring FAIL: meta row is missing for rate_date={rate_date} in {meta_fqn}")

    found_hash = new_data["hash_id"]
    if found_hash != hash_id:
        raise RuntimeError(
            f"Wrong hash_id found in {meta_fqn} for rate_date: {rate_date}, expected: {hash_id}, found: {found_hash}"
        )

    baseline_dt = fetch_baseline_date(bq_client, meta_fqn, rate_dt)
    if baseline_dt is None:
        logger.info("No baseline meta row available; skipping comparisons.")
        return {"hash_id": hash_id}

    base_data = fetch_core_meta_row(bq_client, meta_fqn, baseline_dt)
    if base_data is None:
        raise RuntimeError(
            f"Post-merge monitoring FAIL: meta row is missing for baseline_date={baseline_dt.isoformat()} in {meta_fqn}"
        )

    base_row_count = int(base_data["row_count"])
    new_row_count = int(new_data["row_count"])
    if base_row_count <= 0:
        raise RuntimeError(f"Post-merge monitoring FAIL: baseline row_count <= 0 for {baseline_dt.isoformat()}")

    volume_drift = abs(base_row_count - new_row_count) / base_row_count
    if volume_drift > cfg.COUNT_DRIFT_WARN_PCT:
        raise RuntimeError(f"row_count_drift:{new_row_count} vs {base_row_count} ({volume_drift:.1%})")

    base_currency_count = int(base_data["currency_count"])
    new_currency_count = int(new_data["currency_count"])
    if base_currency_count <= 0:
        raise RuntimeError(f"Post-merge monitoring FAIL: baseline currency_count <=0 for {baseline_dt.isoformat()}")

    currency_drift = abs(base_currency_count - new_currency_count) / base_currency_count
    if currency_drift > cfg.CURRENCY_COUNT_DRIFT_WARN_PCT:
        raise RuntimeError(f"currency_count_drift:{new_currency_count} vs {base_currency_count} ({currency_drift:.1%})")

    new_rates = convert_to_dict(new_data["rates"])
    base_rates = convert_to_dict(base_data["rates"])

    new_set = set(new_rates.keys())
    required_set = set(new_data["required_curr"] or [])

    missing = required_set - new_set
    if missing:
        raise RuntimeError(f"Missing required currency: {sorted(missing)}")

    for cur in required_set:
        base_rate = base_rates.get(cur)
        new_rate = new_rates.get(cur)

        if base_rate is None or new_rate is None:
            continue

        if float(base_rate) == 0.0:
            continue

        rate_change = abs((float(new_rate) / float(base_rate)) - 1.0)

        if rate_change > cfg.RATE_JUMP_WARN_PCT:
            raise RuntimeError(
                f"Rate change {rate_change:.1%} over threshold {cfg.RATE_JUMP_WARN_PCT:.1%} for required currency {cur} "
                f"({baseline_dt.isoformat()}->{rate_dt.isoformat()} {base_rate}->{new_rate})"
            )

    logger.info(f"Post-merge monitoring OK rate_date={rate_date}")
    return {"hash_id": hash_id}


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    test_hash = "6cd1b238e88aca464c326267c23dcb5f50961a502e0d80e84d9a2d93f8a48bc5"
    test_rate_date = "2026-02-04"
    result = check_data_quality(test_hash, test_rate_date)
    logger.info(f"Done. Returned value was: {result}")
