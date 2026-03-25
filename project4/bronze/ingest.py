"""Bronze Layer: Ingest raw NYC taxi parquet into the lake (no transformation)."""
from pyspark.sql import SparkSession
from config.settings import RAW_DATA, BRONZE_DIR


def run(spark: SparkSession):
    df = spark.read.parquet(str(RAW_DATA))

    out = BRONZE_DIR / "trips"
    df.write.mode("overwrite").parquet(str(out))

    print(f"[Bronze] Ingested {df.count()} raw rows -> {out}")
    return df
