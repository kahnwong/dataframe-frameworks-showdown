# import numpy as np
# total_rows = 1195313202 # 1200 million
# step = 100000000  # total_rows / 10, round down; 100M
# trial_rows = [1000, 100000, 1000000, 10000000, 50000000] + list(
#     range(step, total_rows + 1, step)
# )
# ----------------------------------------------------------------
# mb per 10,000,000 rows
# 16 gb * 1024 mb / 1195313202 rows * 10000000 rows = 137 mb / 10 million rows
# total_rows = 1195313202 # 1200 million
# base_partitions = 8
# step = 10000000*base_partitions # more data, need to adjust partition size accordingly
# trial_rows = list(range(step, total_rows + 1, step))
# for index, i in enumerate(trial_rows, 1):
#     partitions = index*base_partitions
#     # print(partitions, i)
#     break
import glob

paths = glob.glob("data/input/nyc-trip-data/*", recursive=True)

with open("run.sh", "w") as f:
    f.write("#!/bin/bash")
    f.write("\n")

    for i in paths:
        for framework in ["polars", "duckdb", "spark"]:
            f.write(
                f"pipenv run python3 src/experiment_{framework}.py --input_path {i}"
            )
            f.write("\n")

print("Successfully created run script")
