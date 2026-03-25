"""Gold Layer: Aggregate taxi data into business-ready tables and load to ClickHouse."""
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofweek, count, round as _round,
    sum as _sum, avg as _avg,
)
from clickhouse_driver import Client
from config.settings import SILVER_DIR, GOLD_DIR, CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_DB


def _load_to_clickhouse(client, table, columns, rows):
    client.execute(f"TRUNCATE TABLE IF EXISTS {CLICKHOUSE_DB}.{table}")
    client.execute(f"INSERT INTO {CLICKHOUSE_DB}.{table} ({columns}) VALUES", rows)


def run(spark: SparkSession):
    df = spark.read.parquet(str(SILVER_DIR / "trips"))

    # --- Table 1: Hourly trip summary ---
    hourly = (
        df.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
        .groupBy("pickup_hour")
        .agg(
            count("*").alias("trip_count"),
            _round(_avg("fare_amount"), 2).alias("avg_fare"),
            _round(_avg("trip_distance"), 2).alias("avg_distance"),
            _round(_avg("tip_amount"), 2).alias("avg_tip"),
        )
        .orderBy("pickup_hour")
    )
    hourly.write.mode("overwrite").parquet(str(GOLD_DIR / "hourly_summary"))

    # --- Table 2: Revenue by pickup location ---
    location = (
        df.groupBy("PULocationID")
        .agg(
            count("*").alias("trip_count"),
            _round(_sum("fare_amount"), 2).alias("total_revenue"),
            _round(_avg("fare_amount"), 2).alias("avg_fare"),
        )
        .orderBy(col("total_revenue").desc())
    )
    location.write.mode("overwrite").parquet(str(GOLD_DIR / "location_revenue"))

    # Load to ClickHouse
    client = Client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT)

    _load_to_clickhouse(
        client, "hourly_summary",
        "pickup_hour, trip_count, avg_fare, avg_distance, avg_tip",
        [tuple(r) for r in hourly.collect()],
    )
    _load_to_clickhouse(
        client, "location_revenue",
        "pu_location_id, trip_count, total_revenue, avg_fare",
        [tuple(r) for r in location.collect()],
    )

    print(f"[Gold] Loaded {hourly.count()} hourly rows + {location.count()} location rows -> ClickHouse")
    return hourly, location
