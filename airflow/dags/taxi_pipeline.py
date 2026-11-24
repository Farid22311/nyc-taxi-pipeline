import os
import shutil
import json
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.sensors.python import PythonSensor
from airflow.sdk import DAG

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DOWNLOAD_DIR = "/opt/airflow/data/downloads"
CLEAN_DIR = "/opt/airflow/data/clean"
MIN_BYTES = 1_000_000
POKE_INTERVAL = 6 * 3600
SENSOR_TIMEOUT = 14 * 24 * 3600
ALERT_LOG = "/opt/airflow/data/alerts.log"


def source_path(month):
    return f"{DOWNLOAD_DIR}/yellow_tripdata_{month}.parquet"


def clean_path(month):
    return f"{CLEAN_DIR}/clean_{month}.parquet"


def download_month(month):
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    target = source_path(month)
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
    target = source_path(month)
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


def file_published(month):
    target = source_path(month)
    if os.path.exists(target) and os.path.getsize(target) > 0:
        print(f"{target} already downloaded")
        return True
    url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
    request = urllib.request.Request(url, method="HEAD", headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urllib.request.urlopen(request) as response:
            print(f"{url} is published (HTTP {response.status})")
            return True
    except urllib.error.HTTPError as err:
        if err.code in (403, 404):
            print(f"{url} not published yet (HTTP {err.code})")
            return False
        raise


def notify_failure(context):
    ti = context.get("task_instance")
    record = {
        "time": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "dag_id": getattr(ti, "dag_id", None),
        "task_id": getattr(ti, "task_id", None),
        "run_id": context.get("run_id"),
        "error": str(context.get("exception")),
    }
    with open(ALERT_LOG, "a") as f:
        f.write(json.dumps(record) + "\n")
    print("ALERT written:", record)


def transform_month(month):
    import pandas as pd

    os.makedirs(CLEAN_DIR, exist_ok=True)
    df = pd.read_parquet(source_path(month), dtype_backend="pyarrow")
    raw_rows = len(df)
    df = df[df["tpep_pickup_datetime"] >= pd.Timestamp("2020-01-01")]
    df = df.assign(
        has_passenger_count=df["passenger_count"].notna(),
        is_reversal=(df["fare_amount"] < 0) | (df["total_amount"] < 0),
    )
    out = clean_path(month)
    tmp = out + ".part"
    df.to_parquet(tmp, index=False)
    os.replace(tmp, out)
    print(f"raw rows {raw_rows}, clean rows {len(df)}, reversals {int(df['is_reversal'].sum())}")


def load_month(month):
    import psycopg
    import pyarrow.parquet as pq
    from psycopg import sql

    pf = pq.ParquetFile(clean_path(month))
    with psycopg.connect(
        host=os.environ["PROJECT_PG_HOST"],
        port=os.environ["PROJECT_PG_PORT"],
        dbname=os.environ["PROJECT_PG_DB"],
        user=os.environ["PROJECT_PG_USER"],
        password=os.environ["PROJECT_PG_PASSWORD"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE TEMP TABLE tmp_month (LIKE staging_yellow_trips) ON COMMIT DROP")
            cur.execute(
                "ALTER TABLE tmp_month ADD COLUMN has_passenger_count BOOLEAN, "
                "ADD COLUMN is_reversal BOOLEAN"
            )
            names = [n.lower() for n in pf.schema_arrow.names]
            copy_sql = sql.SQL("COPY tmp_month ({}) FROM STDIN").format(
                sql.SQL(", ").join(sql.Identifier(n) for n in names)
            )
            for batch in pf.iter_batches(batch_size=100_000):
                columns = [col.to_pylist() for col in batch.columns]
                with cur.copy(copy_sql) as copy:
                    for row in zip(*columns):
                        copy.write_row(row)

            cur.execute("DELETE FROM clean_yellow_trips WHERE source_month = %s", (month,))
            deleted = cur.rowcount
            cur.execute(
                """
                INSERT INTO clean_yellow_trips (
                    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
                    pulocationid, dolocationid, payment_type,
                    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge,
                    airport_fee, cbd_congestion_fee, request_source,
                    has_passenger_count, is_reversal, source_month
                )
                SELECT
                    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count::INTEGER, trip_distance, ratecodeid::INTEGER, store_and_fwd_flag,
                    pulocationid, dolocationid, payment_type::INTEGER,
                    fare_amount::NUMERIC(10,2), extra::NUMERIC(10,2), mta_tax::NUMERIC(10,2),
                    tip_amount::NUMERIC(10,2), tolls_amount::NUMERIC(10,2),
                    improvement_surcharge::NUMERIC(10,2), total_amount::NUMERIC(10,2),
                    congestion_surcharge::NUMERIC(10,2), airport_fee::NUMERIC(10,2),
                    cbd_congestion_fee::NUMERIC(10,2), request_source,
                    has_passenger_count, is_reversal, %s
                FROM tmp_month
                """,
                (month,),
            )
            inserted = cur.rowcount
    print(f"month {month}: deleted {deleted} rows, inserted {inserted} rows")


with DAG(
    dag_id="taxi_pipeline",
    start_date=datetime(2026, 5, 1),
    schedule="@monthly",
    catchup=False,
    max_active_runs=1,
    params={"month": ""},
    default_args={"retries": 2, "retry_delay": timedelta(seconds=20), "on_failure_callback": notify_failure},
) as dag:
    month_arg = {"month": "{{ params.month or data_interval_start.strftime('%Y-%m') }}"}
    download = PythonOperator(task_id="download_month", python_callable=download_month, op_kwargs=month_arg)
    validate = PythonOperator(task_id="validate_file", python_callable=validate_file, op_kwargs=month_arg)
    transform = PythonOperator(task_id="transform_month", python_callable=transform_month, op_kwargs=month_arg)
    load = PythonOperator(task_id="load_month", python_callable=load_month, op_kwargs=month_arg)

    wait = PythonSensor(
        task_id="wait_for_file",
        python_callable=file_published,
        op_kwargs=month_arg,
        mode="reschedule",
        poke_interval=POKE_INTERVAL,
        timeout=SENSOR_TIMEOUT,
    )

    wait >> download >> validate >> transform >> load