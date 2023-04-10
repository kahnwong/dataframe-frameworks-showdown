import logging as log

from pyspark.sql import SparkSession

log.basicConfig(format="%(asctime)s - [%(levelname)s] %(message)s", level=log.INFO)


################## init ##################
spark = (
    SparkSession.builder.appName("Dataframe Frameworks Showdown")
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .getOrCreate()
)

################## read data ##################
df = spark.read.parquet("data/prep/nyc-trip-data")

################## repartition ##################
# total_rows: 1195313202 # 1200 million
# mb per 10,000,000 rows
# 16 gb * 1024 mb / 1195313202 rows * 10000000 rows = `137 mb / 10 million rows`
# total table size in parquet should be `4-5GB` max, seeing I'm using a 16GB RAM machine

capped_rows = 300000000  # 300 million; this would yield around 4GB parquet max
trials = 10
step = capped_rows // trials  # 30 million

trial_rows_base = [
    100000,
    500000,
    1000000,
    5000000,
    10000000,
]  # 100k, 500k, 1m, 5m, 10m
trial_rows = trial_rows_base + list(range(step, capped_rows, step))

"""
hack: probably have to run in batch, if you notice
spark is getting slower due to multiple loops
"""
partition_size = 8
for limit in trial_rows[:2]:
    log.info(f"Writing df of {limit} rows...")
    df.limit(limit).repartition(partition_size).write.parquet(
        f"data/input/expt1/nyc-trip-data/limit={limit}", mode="overwrite"
    )

log.info("Successfully repartitioned dataframe for experiment 1")
