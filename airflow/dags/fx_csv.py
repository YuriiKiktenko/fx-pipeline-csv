from datetime import date, datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore
from airflow.timetables.interval import CronDataIntervalTimetable  # type: ignore
from airflow.exceptions import AirflowFailException  # type: ignore

from fx_csv_lib.ingest_zip import ingest_zip_snapshot
from fx_csv_lib.extract_csv import extract_csv_from_zip
from fx_csv_lib.load_raw import load_raw_from_csv
from fx_csv_lib.build_long import build_long_raw
from fx_csv_lib.build_stage import build_long_stage
from fx_csv_lib.update_core import merge_daily_rates


def merge_daily_rates_wrapper(hash_id, dag_run, data_interval_start=None, data_interval_end=None):
    run_type = getattr(dag_run.run_type, "value", dag_run.run_type).lower()

    if run_type == "manual":
        rate_date = (dag_run.conf or {}).get("rate_date")
        if not rate_date:
            raise AirflowFailException('Manual run requires conf {"rate_date": "YYYY-MM-DD"}')
        rate_date = date.fromisoformat(str(rate_date)).isoformat()
        return merge_daily_rates(hash_id=hash_id, rate_date=rate_date)

    if run_type in {"scheduled", "backfill"}:
        if not data_interval_start or not data_interval_end:
            raise AirflowFailException("data_interval_start/end are required")
        rate_date = data_interval_start.date().isoformat()
        return merge_daily_rates(hash_id=hash_id, rate_date=rate_date)

    raise AirflowFailException(f"Unsupported run_type={dag_run.run_type!r}")


default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fx_csv",
    start_date=datetime(2025, 1, 1),
    schedule=CronDataIntervalTimetable(
        cron="0 0 * * *",
        timezone="UTC",
    ),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["fx", "csv"],
) as dag:
    ingest_zip = PythonOperator(
        task_id="ingest_zip_snapshot",
        python_callable=ingest_zip_snapshot,
    )

    extract_csv = PythonOperator(
        task_id="extract_csv_from_zip",
        python_callable=extract_csv_from_zip,
        op_kwargs={"hash_id": "{{ ti.xcom_pull(task_ids='ingest_zip_snapshot')['hash_id'] }}"},
    )

    load_raw = PythonOperator(
        task_id="load_raw_from_csv",
        python_callable=load_raw_from_csv,
        op_kwargs={"hash_id": "{{ ti.xcom_pull(task_ids='extract_csv_from_zip')['hash_id'] }}"},
    )

    build_long = PythonOperator(
        task_id="build_long_raw",
        python_callable=build_long_raw,
        op_kwargs={"hash_id": "{{ ti.xcom_pull(task_ids='load_raw_from_csv')['hash_id'] }}"},
    )

    build_stage = PythonOperator(
        task_id="build_long_stage",
        python_callable=build_long_stage,
        op_kwargs={"hash_id": "{{ ti.xcom_pull(task_ids='build_long_raw')['hash_id'] }}"},
    )

    update_core = PythonOperator(
        task_id="merge_daily_rates",
        python_callable=merge_daily_rates_wrapper,
        op_kwargs={"hash_id": "{{ ti.xcom_pull(task_ids='build_long_stage')['hash_id'] }}"},
    )

    ingest_zip >> extract_csv >> load_raw >> build_long >> build_stage >> update_core
