import os
import shutil
import urllib.request
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DOWNLOAD_DIR = "/opt/airflow/data/downloads"
MIN_BYTES = 1_000_000


def target_path(month):
    return f"{DOWNLOAD_DIR}/yellow_tripdata_{month}.parquet"


def download_month(month):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    target = target_path(month)
    if os.path.exists(target) and os.path.getsize(target) > 0:
        print(f"{target} already exists, skipping download")
        return

    url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
    tmp = target + ".part"
    print(f"downloading {url}")
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request) as response, open(tmp, "wb") as out:
        shutil.copyfileobj(response, out)
    os.replace(tmp, target)
    print(f"saved {target} ({os.path.getsize(target)} bytes)")


def validate_file(month):
    target = target_path(month)
    size = os.path.getsize(target)
    if size < MIN_BYTES:
        raise ValueError(f"{target} is only {size} bytes")
    with open(target, "rb") as f:
        head = f.read(4)
        f.seek(-4, os.SEEK_END)
        tail = f.read(4)
    if head != b"PAR1" or tail != b"PAR1":
        raise ValueError(f"{target} is not a valid Parquet file")
    print(f"{target} looks valid: {size} bytes")


with DAG(
    dag_id="taxi_download",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    params={"month": "2026-08"},
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20)},
) as dag:
    download = PythonOperator(
        task_id="download_month",
        python_callable=download_month,
        op_kwargs={"month": "{{ params.month }}"},
    )
    validate = PythonOperator(
        task_id="validate_file",
        python_callable=validate_file,
        op_kwargs={"month": "{{ params.month }}"},
    )

    download >> validate