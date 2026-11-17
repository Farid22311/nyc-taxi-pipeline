import glob
import resource
import sys
import time

FILES = sorted(glob.glob("data/yellow_tripdata_2026-*.parquet"))
tool = sys.argv[1]
start = time.perf_counter()

if tool == "pandas":
    import pandas as pd

    df = pd.concat((pd.read_parquet(f) for f in FILES), ignore_index=True)
    print("rows loaded:", len(df), "| in RAM:", round(df.memory_usage(deep=True).sum() / 1024**2), "MB")
    df = df[(df["fare_amount"] >= 0) & (df["total_amount"] >= 0)]
    res = df.groupby("PULocationID")["total_amount"].agg(trips="count", revenue="sum")
    trips, revenue = int(res["trips"].sum()), float(res["revenue"].sum())
else:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.master("local[*]")
        .config("spark.driver.memory", "3g")
        .getOrCreate()
    )
    df = spark.read.parquet(*FILES)
    df = df.filter((F.col("fare_amount") >= 0) & (F.col("total_amount") >= 0))
    res = df.groupBy("PULocationID").agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"))
    res.explain()
    row = res.agg(F.sum("trips"), F.sum("revenue")).first()
    trips, revenue = int(row[0]), float(row[1])

secs = time.perf_counter() - start
own = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
print(f"{tool}: {secs:.1f}s | trips={trips} revenue={revenue:.2f}")
if tool == "pandas":
    print(f"peak memory: {own:.0f} MB")