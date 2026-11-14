from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("hello").master("local[*]").getOrCreate()

df = spark.read.parquet("data/yellow_tripdata_2026-08.parquet")
df.printSchema()
print("rows:", df.count())
df.show(5)

spark.stop()