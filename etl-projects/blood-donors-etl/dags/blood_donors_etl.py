import os
import subprocess
import sys
import logging
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID = os.environ["TELEGRAM_CHAT_ID"]


def send_telegram_message(message):
    """Отправляет сообщение в Telegram."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    response = requests.post(
        url,
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
        },
        timeout=10,
    )
    response.raise_for_status()

    result = response.json()
    if not result.get("ok"):
        raise RuntimeError("Telegram не подтвердил отправку сообщения")


def run_blood_donors_etl():
    """Запускает ETL и отправляет результат в Telegram."""
    try:
        send_telegram_message(
            "🚀 Blood Donors ETL запущен\n"
            f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

        result = subprocess.run(
    [
        sys.executable,
        "/opt/airflow/scripts/transform_blood_data.py",
    ],
    capture_output=True,
    text=True,
    )

        if result.stdout:
            logger.info("ETL stdout:\n%s", result.stdout)

        if result.stderr:
            logger.info("ETL stderr:\n%s", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(
                f"ETL script failed with exit code {result.returncode}"
    )

        send_telegram_message(
            "✅ Blood Donors ETL успешно завершён\n"
            f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )

    except Exception as error:
        logger.exception("Blood Donors ETL failed")

        try:
            send_telegram_message(
                "❌ Blood Donors ETL завершился с ошибкой\n"
                f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"Ошибка: {str(error)[:700]}"
            )
        except Exception:
            logger.exception("Failed to send Telegram notification")

        raise


with DAG(
    dag_id="blood_donors_etl",
    description="MinIO -> PySpark -> PostgreSQL ETL pipeline",
    start_date=datetime(2026, 10, 1),
    schedule="0 15 * * *",
    catchup=False,
    tags=["blood-donors", "pyspark", "minio", "postgres"],
) as dag:

    run_etl = PythonOperator(
        task_id="run_blood_donors_etl",
        python_callable=run_blood_donors_etl,
    )