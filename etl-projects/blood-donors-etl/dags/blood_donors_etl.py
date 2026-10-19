import subprocess
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator


def run_blood_donors_etl():
    """Запускает PySpark-скрипт: MinIO -> преобразование -> PostgreSQL."""
    result = subprocess.run(
        [sys.executable, "/opt/airflow/scripts/transform_blood_data.py"],
        check=True,  # если скрипт упал, задача в Airflow тоже станет красной
        capture_output=True,
        text=True,
    )
    print(result.stdout)
    if result.stderr:
        print(result.stderr)


with DAG(
    dag_id="blood_donors_etl",
    description="MinIO -> PySpark -> PostgreSQL ETL pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="0 15 * * *",  # каждый день в 15:00
    catchup=False,
    tags=["blood-donors", "pyspark", "minio", "postgres"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_blood_donors_etl",
        python_callable=run_blood_donors_etl,
    )
