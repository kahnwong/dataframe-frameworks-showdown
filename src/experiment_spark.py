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
parser.add_argument("--experiment_id")
args = parser.parse_args()
input_path = args.input_path
experiment_id = int(args.experiment_id)

if experiment_id == 1:
    trial_rows = int(input_path.split("=")[-1])
    log.info(f"trial_rows: {trial_rows}")

    partitions = ["partition"]
    log.info(f"partitions: {partitions}")
elif experiment_id == 21:
    partitions = ["partition"]
    log.info(f"partitions: {partitions}")
elif experiment_id == 22:
    partitions = ["year", "month"]
    log.info(f"partitions: {partitions}")

log.info(f"start run - {ENGINE}-EXPT{experiment_id}: {RUN_ID}")

start_time = time.time()  # start timer

################## main ##################
df = spark.read.parquet(input_path)

w = Window().partitionBy(*partitions).orderBy("trip_length_minute")

df_out = (
    df
    #### create dummy partition column####
    .withColumn("partition", F.lit("dummy"))
    .select(
        partitions
        + [
            "VendorID",
            "payment_type",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "total_amount",
        ]
    )
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
        partitions
        + [
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
        "experiment_id": experiment_id,
        "engine": ENGINE,
        "duration": elapsed_time,
        "swap_usage": psutil.swap_memory().total,
    }

    if experiment_id == 1:
        r["processed_rows"] = trial_rows
        r["mode"] = "lazy"
    elif experiment_id == 21:
        r["mode"] = "single-key partition"
    elif experiment_id == 22:
        r["mode"] = "multi-key partition"

    f.write(json.dumps(r))
    f.write("\n")
