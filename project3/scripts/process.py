"""
Step 2 — Process: Read raw JSON from S3, clean/transform, write Parquet.

This is the classic "Bronze -> Silver" transformation in a data lakehouse:
  - Read raw newline-delimited JSON from the landing zone
  - Parse timestamps, add derived columns (order_date, day_of_week)
  - Drop duplicates, filter bad records
  - Write partitioned Parquet to the processed zone

Usage:
    python scripts/process.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Expected schema for raw events ──────────────────────────────────
RAW_SCHEMA = StructType([
    StructField("order_id",         StringType(),  False),
    StructField("customer_id",      StringType(),  False),
    StructField("product_id",       StringType(),  False),
    StructField("product_name",     StringType(),  True),
    StructField("category",         StringType(),  True),
    StructField("quantity",         IntegerType(), True),
    StructField("unit_price",       DoubleType(),  True),
    StructField("total_amount",     DoubleType(),  True),
    StructField("payment_method",   StringType(),  True),
    StructField("order_status",     StringType(),  True),
    StructField("shipping_city",    StringType(),  True),
    StructField("shipping_country", StringType(),  True),
    StructField("event_timestamp",  StringType(),  False),
])


def create_spark(master_url: str) -> SparkSession:
    """Build a SparkSession configured for S3 access."""
    return (
        SparkSession.builder
        .appName("lab02-process")
        .master(master_url)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )


def main():
    bucket     = os.environ["S3_BUCKET"]
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")

    raw_path       = f"s3a://{bucket}/orders/"
    processed_path = f"s3a://{bucket}/processed/orders/"

    spark = create_spark(master_url)
    print(f"Reading raw JSON from {raw_path} ...")

    # ── Read raw JSON ────────────────────────────────────────────────
    df_raw = (
        spark.read
        .schema(RAW_SCHEMA)
        .json(raw_path)
    )

    raw_count = df_raw.count()
    print(f"  Raw records loaded: {raw_count}")

    # ── Transform: Bronze -> Silver ──────────────────────────────────
    df_clean = (
        df_raw
        # Parse timestamp string to proper timestamp type
        .withColumn("event_ts", F.to_timestamp("event_timestamp"))
        # Derived columns useful for analytics
        .withColumn("order_date", F.to_date("event_ts"))
        .withColumn("order_hour", F.hour("event_ts"))
        .withColumn("day_of_week", F.dayofweek("event_ts"))
        .withColumn("is_weekend", F.when(F.dayofweek("event_ts").isin(1, 7), True).otherwise(False))
        # Recalculate total for data quality
        .withColumn("calc_total", F.round(F.col("unit_price") * F.col("quantity"), 2))
        # Drop the raw timestamp string, keep the parsed one
        .drop("event_timestamp")
        # Remove duplicates by order_id
        .dropDuplicates(["order_id"])
        # Filter out records with null keys
        .filter(F.col("order_id").isNotNull())
        .filter(F.col("event_ts").isNotNull())
    )

    clean_count = df_clean.count()
    dropped = raw_count - clean_count
    print(f"  After cleaning: {clean_count} records ({dropped} dropped)")

    # ── Write partitioned Parquet ────────────────────────────────────
    print(f"Writing Parquet to {processed_path} ...")
    (
        df_clean
        .repartition("order_date")
        .write
        .mode("overwrite")
        .partitionBy("order_date")
        .parquet(processed_path)
    )

    print(f"Done. Parquet files written to {processed_path}")

    # Quick sanity check: read back and show stats
    df_verify = spark.read.parquet(processed_path)
    print(f"\nVerification — Parquet record count: {df_verify.count()}")
    print("Schema:")
    df_verify.printSchema()

    spark.stop()


if __name__ == "__main__":
    main()
