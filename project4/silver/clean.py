"""Silver Layer: Clean, filter, and deduplicate taxi trip data."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from config.settings import BRONZE_DIR, SILVER_DIR


def run(spark: SparkSession):
    df = spark.read.parquet(str(BRONZE_DIR / "trips"))

    # Drop rows missing critical fields
    df = df.dropna(subset=["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID"])

    # Filter out bad trips (negative or zero fare/distance)
    df = df.filter((col("trip_distance") > 0) & (col("fare_amount") > 0))

    # Filter unreasonable values
    df = df.filter((col("passenger_count") > 0) & (col("passenger_count") <= 9))
    df = df.filter(col("fare_amount") < 1000)

    # Deduplicate
    df = df.dropDuplicates()

    out = SILVER_DIR / "trips"
    df.write.mode("overwrite").parquet(str(out))

    print(f"[Silver] Cleaned down to {df.count()} rows -> {out}")
    return df
