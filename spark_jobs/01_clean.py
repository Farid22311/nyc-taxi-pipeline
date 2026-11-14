from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("clean").master("local[*]").getOrCreate()

raw = spark.read.parquet("data/yellow_tripdata_2026-08.parquet")

clean = (
    raw
    .filter(F.col("tpep_pickup_datetime") >= "2020-01-01")
    .withColumn("has_passenger_count", F.col("passenger_count").isNotNull())
    .withColumn("is_reversal", (F.col("fare_amount") < 0) | (F.col("total_amount") < 0))
)

clean.write.mode("overwrite").parquet("output/clean_yellow_trips")

check = spark.read.parquet("output/clean_yellow_trips")
print("rows:", check.count())
check.groupBy("is_reversal").count().show()
check.groupBy("has_passenger_count").count().show()
check.filter(~F.col("is_reversal")).agg(F.round(F.sum("total_amount"), 2).alias("revenue")).show()

spark.stop()