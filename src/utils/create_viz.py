import os

import polars as pl
import seaborn as sns
from matplotlib import pyplot as plt

### prep data ###
df = pl.read_ndjson("data/runs.json")

# in case multiple experiments with same run parameters are present
df = df.groupby(["engine", "mode", "processed_rows"]).agg(
    pl.col("duration").mean().alias("duration"),
    pl.col("swap_usage").mean().alias("swap_usage"),
)

# remove a run against full dataset
df = df.filter(pl.col("processed_rows") < 1095313202)

# make the values more readable
df = df.with_columns(  # divided by 1 million
    pl.col("processed_rows") / 1000000
).with_columns(  # convert to GB
    pl.col("swap_usage") / 1000000000
)

# convert to pandas for seaborn
df = df.to_pandas()

### viz ###
palette = {
    "duckdb": "yellow",
    "polars": "lightsteelblue",
    "spark": "orange",
}

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()


sns.barplot(data=df, x="processed_rows", y="duration", hue="engine", ax=ax1)
ax1.set_xticklabels(labels="processed_rows", rotation=45)
ax1.set_title("Dataframe frameworks performance")
ax1.set_xlabel("Rows (in millions)")
ax1.set_ylabel("Bar: Duration (in seconds)")

sns.pointplot(data=df, x="processed_rows", y="swap_usage", hue="engine", ax=ax2)
ax2.set_ylabel("Point: Swap Usage (in GB)")


os.makedirs("images", exist_ok=True)
fig.savefig("images/result.png")
