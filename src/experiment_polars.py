import argparse
import json
import logging as log
import time
import uuid

import polars as F
import polars as ps
import psutil
from polars import col


log.basicConfig(format="%(asctime)s - [%(levelname)s] %(message)s", level=log.INFO)

################## metadata ##################
RUN_ID = str(uuid.uuid4())
ENGINE = "polars"
MODE = "lazy"

log.info(f"start run - {ENGINE}-{MODE}: {RUN_ID}")

################## init ##################
### get input params ###
parser = argparse.ArgumentParser()
parser.add_argument("--input_path")
args = parser.parse_args()
input_path = args.input_path

trial_rows = int(input_path.split("=")[-1])
log.info(f"trial_rows: {trial_rows}")

start_time = time.time()  # start timer

################## main ##################
df = ps.scan_parquet(f"{input_path}/*.parquet")

df_out = (
    df.select(
        [
            "VendorID",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "payment_type",
            "passenger_count",
            "trip_distance",
            "total_amount",
        ]
    )
    #### create dummy partition ####
    .with_columns(F.lit("dummy").alias("partition"))
    #### create trip_length_minute ####
    .with_columns(
        (col("tpep_pickup_datetime") - col("tpep_dropoff_datetime"))
        .dt.minutes()
        .alias("trip_length_minute")
    )
    #### filter by percentile ####
    # .with_columns(col("trip_length_minute").max().over("a").suffix("_max"))
    .with_columns(
        (col("trip_length_minute").rank() / col("trip_length_minute").count())
        .over("partition")
        .alias("trip_length_minute_percentile")
    )
    .filter(col("trip_length_minute_percentile").is_between(0.2, 0.8))
    #### aggregate ####
    .groupby(
        [
            "VendorID",
            "payment_type",
        ]
    )
    .agg(
        # passenger_count
        col("passenger_count").min().alias("min_passenger_count"),
        col("passenger_count").max().alias("max_passenger_count"),
        col("passenger_count").mean().alias("avg_passenger_count"),
        # trip_distance
        col("trip_distance").min().alias("min_trip_distance"),
        col("trip_distance").max().alias("max_trip_distance"),
        col("trip_distance").mean().alias("avg_trip_distance"),
        # total_amount
        col("total_amount").min().alias("min_total_amount"),
        col("total_amount").max().alias("max_total_amount"),
        col("total_amount").mean().alias("avg_total_amount"),
    )
)

log.info(f"output rows: {len(df_out.collect())}")  # trigger actions
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
