import os

import pandas as pd

MIN_PICKUP = pd.Timestamp("2020-01-01")


def clean_frame(df):
    df = df[df["tpep_pickup_datetime"] >= MIN_PICKUP]
    return df.assign(
        has_passenger_count=df["passenger_count"].notna(),
        is_reversal=(df["fare_amount"] < 0) | (df["total_amount"] < 0),
    )


def check_parquet_file(path, min_bytes):
    size = os.path.getsize(path)
    if size < min_bytes:
        raise ValueError(f"{path} is only {size} bytes")
    with open(path, "rb") as f:
        head = f.read(4)
        f.seek(-4, os.SEEK_END)
        tail = f.read(4)
    if head != b"PAR1" or tail != b"PAR1":
        raise ValueError(f"{path} is not a valid Parquet file")
    return size


def is_not_published_status(code):
    return code in (403, 404)