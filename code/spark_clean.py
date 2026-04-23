import os
import time
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, udf
from pyspark.sql.types import DoubleType

MASTER_URL = "spark://MASTER_IP:7077"
DATA_PATH = "/Users/YOUR_USERNAME/a8_project/data/nyc_taxi/*.parquet"
LOCATION_PATH = "/Users/YOUR_USERNAME/a8_project/data/taxi_zone_lookup.csv"
OUTPUT_PATH = "/Users/YOUR_USERNAME/a8_project/data/output/spark_result.parquet"

spark = SparkSession.builder \
    .appName("NYC_Taxi_Spark") \
    .master(MASTER_URL) \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# 1. Ingest

t0 = time.time()
trips = spark.read.parquet(DATA_PATH)
locations = spark.read.option("header", True).csv(LOCATION_PATH)
t_ingest = time.time() - t0
print(f"[TIMER] Ingest: {t_ingest:.2f}s")



# 2. Cleanse 

t1 = time.time()
trips = trips.dropna().dropDuplicates()
trips = trips.withColumn(
    "tpep_pickup_datetime",
    unix_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
).withColumn(
    "tpep_dropoff_datetime",
    unix_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss").cast("timestamp")
)
t_cleanse = time.time() - t1
print(f"[TIMER] Cleanse: {t_cleanse:.2f}s")



# 3. Heavy Join + UDF

t2 = time.time()
joined = trips.join(
    locations.withColumnRenamed("LocationID", "PULocationID"),
    on="PULocationID", how="left"
)

@udf(DoubleType())
def avg_speed_udf(distance, pickup_ts, dropoff_ts):
    try:
        hours = (dropoff_ts.timestamp() - pickup_ts.timestamp()) / 3600.0
        return float(distance / hours) if hours > 0 else 0.0
    except:
        return 0.0

t_udf_start = time.time()
joined = joined.withColumn(
    "avg_speed_mph",
    avg_speed_udf(col("trip_distance"),
                  col("tpep_pickup_datetime"),
                  col("tpep_dropoff_datetime"))
)
joined.cache()
joined.count()
t_udf = time.time() - t_udf_start
t_transform = time.time() - t2
print(f"[TIMER] UDF only: {t_udf:.2f}s  |  Total transform: {t_transform:.2f}s")



# 4. Export 

t3 = time.time()
joined.write.mode("overwrite").parquet(OUTPUT_PATH)
t_export = time.time() - t3
print(f"[TIMER] Export: {t_export:.2f}s")

total = time.time() - t0
print(f"\n[TOTAL WALL CLOCK] {total:.2f}s")
spark.stop()