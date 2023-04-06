import argparse
import json
import logging as log
import time
import uuid

import psutil
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.window import Window

log.basicConfig(format="%(asctime)s - [%(levelname)s] %(message)s", level=log.INFO)

################## metadata ##################
RUN_ID = str(uuid.uuid4())
ENGINE = "spark"
MODE = "default"

log.info(f"start run - {ENGINE}-{MODE}: {RUN_ID}")

################## init ##################
spark = (
    SparkSession.builder.appName("Dataframe Frameworks Showdown")
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .getOrCreate()
)

### get input params ###
parser = argparse.ArgumentParser()
parser.add_argument("--input_path")
args = parser.parse_args()
input_path = args.input_path

trial_rows = int(input_path.split("=")[-1])
log.info(f"trial_rows: {trial_rows}")

start_time = time.time()  # start timer

################## main ##################
df = spark.read.parquet(input_path)

w = Window().partitionBy("partition").orderBy("trip_length_minute")

df_out = (
    df.select(
        [
            "VendorID",
            "payment_type",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "total_amount",
        ]
    )
    #### create dummy partition column####
    .withColumn("partition", F.lit("dummy"))
    #### create trip_length_minute ####
    .withColumn(
        "tpep_pickup_datetime",
        F.to_timestamp(col("tpep_pickup_datetime"), "yyyy-MM-dd HH:mm:ss"),
    )
    .withColumn(
        "tpep_dropoff_datetime",
        F.to_timestamp(col("tpep_dropoff_datetime"), "yyyy-MM-dd HH:mm:ss"),
    )
    .withColumn(
        "trip_length_minute",
        (
            F.unix_timestamp(col("tpep_dropoff_datetime"))
            - F.unix_timestamp(col("tpep_pickup_datetime"))
        )
        / 60,
    )
    #### filter by percentile ####
    .withColumn("trip_length_minute_percentile", F.percent_rank().over(w))
    .where(col("trip_length_minute_percentile").between(0.2, 0.8))
    #### aggregate ####
    .groupBy(
        [
            "VendorID",
            "payment_type",
        ]
    )
    .agg(
        # passenger_count
        F.min(col("passenger_count")).alias("min_passenger_count"),
        F.max(col("passenger_count")).alias("max_passenger_count"),
        F.avg(col("passenger_count")).alias("avg_passenger_count"),
        # trip_distance
        F.min(col("trip_distance")).alias("min_trip_distance"),
        F.max(col("trip_distance")).alias("max_trip_distance"),
        F.avg(col("trip_distance")).alias("avg_trip_distance"),
        # total_amount
        F.min(col("total_amount")).alias("min_total_amount"),
        F.max(col("total_amount")).alias("max_total_amount"),
        F.avg(col("total_amount")).alias("avg_total_amount"),
    )
)

log.info(f"output rows: {df_out.count()}")  # trigger actions
end_time = time.time()  # end timer

elapsed_time = end_time - start_time
log.info(f"Elapsed time was {elapsed_time} seconds")

################## logging ##################
with open("data/runs.json", "a") as f:
    r = {
        "uuid": RUN_ID,
        "engine": ENGINE,
        "mode": MODE,
        "processed_rows": trial_rows,
        "duration": elapsed_time,
        "swap_usage": psutil.swap_memory().total,
    }

    f.write(json.dumps(r))
    f.write("\n")
