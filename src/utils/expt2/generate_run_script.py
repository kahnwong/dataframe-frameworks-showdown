with open("run.sh", "w") as f:
    f.write("#!/bin/bash")
    f.write("\n")

    # polars can't read hive partition
    f.writelines(
        [
            "pipenv run python3 src/experiment_polars.py --input_path data/prep/nyc-trip-data --experiment_id 21\n",
            "pipenv run python3 src/experiment_polars.py --input_path data/prep/nyc-trip-data --experiment_id 22\n",
        ]
    )

    # spark can read hive partition
    f.writelines(
        [
            "pipenv run python3 src/experiment_spark.py --input_path data/input/expt2/nyc-trip-data --experiment_id 21\n",
            "pipenv run python3 src/experiment_spark.py --input_path data/input/expt2/nyc-trip-data --experiment_id 22\n",
        ]
    )

print("Successfully created run script")
