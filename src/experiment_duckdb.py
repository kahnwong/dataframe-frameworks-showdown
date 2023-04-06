import argparse
import json
import logging as log
import time
import uuid

import duckdb
import psutil


log.basicConfig(format="%(asctime)s - [%(levelname)s] %(message)s", level=log.INFO)

################## metadata ##################
RUN_ID = str(uuid.uuid4())
ENGINE = "duckdb"
MODE = "default"

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
path = f"{input_path}/*.parquet"

df = duckdb.sql(
    f"""
    SELECT  VendorID,
            payment_type,
            --- passenger_count ---
            min(passenger_count) AS min_passenger_count,
            max(passenger_count) AS max_passenger_count,
            avg(passenger_count) AS avg_passenger_count,
            --- trip_distance ---
            min(trip_distance) AS min_trip_distance,
            max(trip_distance) AS max_trip_distance,
            avg(trip_distance) AS avg_trip_distance,
            --- total_amount ---
            min(total_amount) AS min_total_amount,
            max(total_amount) AS max_total_amount,
            avg(total_amount) AS avg_total_amount

    FROM (
        SELECT 'dummy' AS partition,
                --- filter ---
                (epoch(tpep_dropoff_datetime)-epoch(tpep_pickup_datetime))/60 AS trip_length_minute,
                percent_rank() OVER (PARTITION BY partition ORDER BY trip_length_minute) AS trip_length_minute_percentile,
                --- groupby cols ---
                VendorID,
                payment_type,
                --- agg cols ---
                passenger_count,
                trip_distance,
                total_amount
        FROM read_parquet('{path}')
    )
    WHERE trip_length_minute_percentile BETWEEN 0.2 AND 0.8
    GROUP BY VendorID, payment_type
    """
).pl()

log.info(f"output rows: {len(df)}")  # trigger actions
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
