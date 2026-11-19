import os
from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

MARKER = "/tmp/retry_demo_marker"


def flaky():
    if not os.path.exists(MARKER):
        open(MARKER, "w").close()
        raise RuntimeError("first attempt fails on purpose")
    os.remove(MARKER)
    print("second attempt succeeded")


with DAG(
    dag_id="retry_demo",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
) as dag:
    PythonOperator(
        task_id="flaky",
        python_callable=flaky,
        retries=2,
        retry_delay=timedelta(seconds=15),
    )