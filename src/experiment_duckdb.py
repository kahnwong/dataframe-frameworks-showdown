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

################## init ##################
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

    partitions = ",".join(["partition"])
    log.info(f"partitions: {partitions}")
elif experiment_id == 21:
    partitions = ",".join(["partition"])
    log.info(f"partitions: {partitions}")
elif experiment_id == 22:
    partitions = ",".join(["year", "month"])
    log.info(f"partitions: {partitions}")

log.info(f"start run - {ENGINE}-EXPT{experiment_id}: {RUN_ID}")

start_time = time.time()  # start timer

################## main ##################
path = f"{input_path}/*.parquet"

df = duckdb.sql(
    f"""
    SELECT  {partitions},
            VendorID,
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
                percent_rank() OVER (PARTITION BY {partitions} ORDER BY trip_length_minute) AS trip_length_minute_percentile,
                --- groupby cols ---
                {partitions},
                VendorID,
                payment_type,
                --- agg cols ---
                passenger_count,
                trip_distance,
                total_amount
        FROM read_parquet('{path}')
    )
    WHERE trip_length_minute_percentile BETWEEN 0.2 AND 0.8
    GROUP BY {partitions}, VendorID, payment_type
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
        "experiment_id": experiment_id,
        "engine": ENGINE,
        "duration": elapsed_time,
        "swap_usage": psutil.swap_memory().total,
    }

    if experiment_id == 1:
        r["processed_rows"] = trial_rows
        r["mode"] = "default"
    elif experiment_id == 21:
        r["mode"] = "single-key partition"
    elif experiment_id == 22:
        r["mode"] = "multi-key partition"

    f.write(json.dumps(r))
    f.write("\n")
