import glob
import os
import time

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

FILES = sorted(glob.glob("data/yellow_tripdata_2026-*.parquet"))
COLS = ["PULocationID", "fare_amount", "total_amount"]

print("files:", len(FILES), "| cpu cores:", os.cpu_count())


def run_pandas():
    df = pd.concat((pd.read_parquet(f, columns=COLS) for f in FILES), ignore_index=True)
    df = df[(df["fare_amount"] >= 0) & (df["total_amount"] >= 0)]
    res = df.groupby("PULocationID")["total_amount"].agg(trips="count", revenue="sum")
    return int(res["trips"].sum()), float(res["revenue"].sum())


spark = SparkSession.builder.appName("bench").master("local[*]").getOrCreate()


def run_spark():
    df = spark.read.parquet(*FILES).select(*COLS)
    df = df.filter((F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0))
    res = df.groupBy("PULocationID").agg(
        F.count("*").alias("trips"), F.sum("total_amount").alias("revenue")
    )
    row = res.agg(F.sum("trips"), F.sum("revenue")).first()
    return int(row[0]), float(row[1])


def timed(name, fn, runs=3):
    for i in range(runs):
        start = time.perf_counter()
        trips, revenue = fn()
        secs = time.perf_counter() - start
        print(f"{name} run {i + 1}: {secs:.1f}s | trips={trips} revenue={revenue:.2f}")


timed("pandas", run_pandas)
timed("spark ", run_spark)

spark.stop()