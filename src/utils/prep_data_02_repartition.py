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

################## repartition ##################
df = spark.read.parquet("data/prep/nyc-trip-data")
df.coalesce(32).write.parquet("data/input/nyc-trip-data", mode="overwrite")

log.info("Successfully repartitioned dataframe for experiment")
