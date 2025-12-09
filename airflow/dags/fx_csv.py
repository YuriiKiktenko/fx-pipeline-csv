from datetime import datetime, timedelta

from airflow import DAG  # type: ignore
from airflow.providers.standard.operators.python import PythonOperator  # type: ignore


from fx_csv_lib.ingest_zip import ingest_zip_snapshot

default_args = {
    "owner": "airflow",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}

with DAG(
    dag_id="fx_csv",
    start_date=datetime(2025, 1, 1),
    schedule="0 0 * * *",
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=["fx", "csv"],
) as dag:
    ingest_zip = PythonOperator(
        task_id="ingest_zip_snapshot",
        python_callable=ingest_zip_snapshot,
    )

    ingest_zip
