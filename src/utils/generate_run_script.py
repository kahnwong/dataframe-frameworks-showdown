# import numpy as np

total_rows = 1195313202
step = 100000000  # total_rows / 10, round down; 100M

trial_rows = [1000, 100000, 1000000, 10000000, 50000000] + list(
    range(step, total_rows + 1, step)
)

with open("run.sh", "w") as f:
    f.write("#!/bin/bash")
    f.write("\n")

    for i in trial_rows:
        for framework in ["polars", "spark"]:
            f.write(
                f"pipenv run python3 src/experiment_{framework}.py --trial_rows {i}"
            )
            f.write("\n")

print("Successfully created run script")
