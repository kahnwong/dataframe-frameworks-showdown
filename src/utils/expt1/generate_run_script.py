import glob

paths = glob.glob("data/input/expt1/nyc-trip-data/*", recursive=True)

with open("run.sh", "w") as f:
    f.write("#!/bin/bash")
    f.write("\n")

    for i in paths:
        for framework in ["polars", "duckdb", "spark"]:
            f.write(
                f"pipenv run python3 src/experiment_{framework}.py --input_path {i} --experiment_id 1"
            )
            f.write("\n")

print("Successfully created run script")
