from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("aggregate").master("local[*]").getOrCreate()

clean = spark.read.parquet("output/clean_yellow_trips")

# Reversals are refunds, not trips (same rule as the SQL queries).
# Money is cast to DECIMAL so the sums are exact.
trips = (
    clean
    .filter(~F.col("is_reversal"))
    .withColumn("total_amount", F.col("total_amount").cast("decimal(10,2)"))
    .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
)

# 1. Trips per hour (Spark version of q01)
hourly = (
    trips.groupBy(F.hour("tpep_pickup_datetime").alias("pickup_hour"))
    .agg(F.count("*").alias("trips"))
    .orderBy("pickup_hour")
)
hourly.show(24)

# 2. Revenue per pickup zone (Spark version of q02)
zones = (
    trips.groupBy("pulocationid")
    .agg(F.count("*").alias("trips"), F.sum("total_amount").alias("revenue"))
    .withColumn("revenue_per_trip", F.round(F.col("revenue") / F.col("trips"), 2))
    .orderBy(F.col("trips").desc())
)
zones.show(10)
zones.agg(F.sum("revenue").alias("total_revenue")).show()

# 3. Partitioned output by pickup date
trips.write.mode("overwrite").partitionBy("pickup_date").parquet("output/trips_by_date")

# 4. Partition pruning: read back only Aug 19-21
by_date = spark.read.parquet("output/trips_by_date")
subset = by_date.filter(F.col("pickup_date").between("2026-08-19", "2026-08-21"))
subset.explain()
print("rows Aug 19-21:", subset.count())

spark.stop()