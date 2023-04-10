import logging as log
import os

import polars as pl
import seaborn as sns
from matplotlib import pyplot as plt

log.basicConfig(format="%(asctime)s - [%(levelname)s] %(message)s", level=log.INFO)

### prep data ###
df = pl.read_ndjson("data/runs.json")

# in case multiple experiments with same run parameters are present
df = (
    df.filter(pl.col("experiment_id").is_in({21, 22}))
    .groupby(["engine", "mode"])
    .agg(
        pl.col("duration").mean().alias("duration"),
        pl.col("swap_usage").mean().alias("swap_usage"),
    )
)

# make the values more readable
df = df.with_columns(pl.col("swap_usage") / 1000000000)  # convert to GB

# convert to pandas for seaborn
df = df.to_pandas()

### viz ###
palette = {
    "duckdb": "yellow",
    "polars": "lightsteelblue",
    "spark": "orange",
}

fig, ax1 = plt.subplots()
# fig.set_size_inches(5, 5)
ax2 = ax1.twinx()

sns.barplot(
    data=df,
    x="mode",
    y="duration",
    hue="engine",
    ax=ax1,
)
ax1.set_title("Dataframe frameworks performance: EXPT 2 - multi-key partition")
ax1.set_xlabel("")
ax1.set_ylabel("Bar: Duration (in seconds)")

sns.pointplot(
    data=df,
    x="mode",
    y="swap_usage",
    hue="engine",
    ax=ax2,
)
ax2.set_ylabel("Line: Swap Usage (in GB)")


os.makedirs("images", exist_ok=True)
fig.savefig("images/result_expt2.png")

log.info("Successfully visualized experiment 2")
