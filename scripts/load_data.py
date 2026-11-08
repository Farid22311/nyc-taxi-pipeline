import os

import psycopg
import pyarrow.parquet as pq
from dotenv import load_dotenv

load_dotenv()

HOST = "localhost"
PORT = 5433

POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DB = os.getenv("POSTGRES_DB")

PARQUET_FILE = "data/yellow_tripdata_2026-08.parquet"

print("user:", POSTGRES_USER)
print("db:", POSTGRES_DB)
print("host:", HOST)
print("port:", PORT)

pf = pq.ParquetFile(PARQUET_FILE)
total = 0

with psycopg.connect(
    host=HOST,
    port=PORT,
    dbname=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
) as conn:
    with conn.cursor() as cur:
        cur.execute("TRUNCATE staging_yellow_trips")

        for batch in pf.iter_batches(batch_size=100_000):
            columns = [col.to_pylist() for col in batch.columns]
            with cur.copy("COPY staging_yellow_trips FROM STDIN") as copy:
                for row in zip(*columns):
                    copy.write_row(row)
            total += batch.num_rows
            print(f"loaded {total} rows")

    conn.commit()