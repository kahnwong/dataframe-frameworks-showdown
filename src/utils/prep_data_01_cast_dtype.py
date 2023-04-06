import glob
import logging as log
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DoubleType
from tqdm import tqdm

log.basicConfig(format="%(asctime)s - [%(levelname)s] %(message)s", level=log.INFO)


################## init ##################
spark = (
    SparkSession.builder.appName("Dataframe Frameworks Showdown")
    .config("spark.executor.memory", "16g")
    .config("spark.driver.memory", "16g")
    .getOrCreate()
)

################## prep data; fix column with mismatched dtype ##################
files = glob.glob("data/raw/nyc-trip-data/**/*.parquet", recursive=True)


def sanitize(filename: str, spark):
    df = spark.read.parquet(filename)
    df = df.select(
        [
            "VendorID",
            "payment_type",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "total_amount",
        ]
    ).withColumn("passenger_count", col("passenger_count").cast(DoubleType()))

    return df


# df = sanitize(files[0], spark)
path = "data/prep/nyc-trip-data"
shutil.rmtree(path, ignore_errors=True)

for i in tqdm(files):
    df = sanitize(i, spark)
    df.write.parquet(path, mode="append")

log.info("Successfully cast dtype")
