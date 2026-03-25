"""Medallion Architecture Pipeline: Bronze -> Silver -> Gold (NYC Taxi Data)"""
from pyspark.sql import SparkSession
from config.settings import BRONZE_DIR, SILVER_DIR, GOLD_DIR
from bronze import ingest
from silver import clean
from gold import aggregate

STAGES = [
    ("Bronze", ingest),
    ("Silver", clean),
    ("Gold",   aggregate),
]


def _stage_ready(module) -> bool:
    """Check if a stage module has a run() function implemented."""
    return callable(getattr(module, "run", None))


def main():
    for d in [BRONZE_DIR, SILVER_DIR, GOLD_DIR]:
        d.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("MedallionPipeline")
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 50)
        print("  Medallion Pipeline: Bronze -> Silver -> Gold")
        print("  Dataset: NYC Yellow Taxi (Jan 2024)")
        print("=" * 50)

        for name, module in STAGES:
            if _stage_ready(module):
                module.run(spark)
            else:
                print(f"[{name}] Skipped — run() not implemented yet")

        print("=" * 50)
        print("  Pipeline complete! Query ClickHouse:")
        print("  docker exec -it lab03-clickhouse-1 clickhouse-client")
        print("  SELECT * FROM gold_db.hourly_summary ORDER BY pickup_hour;")
        print("  SELECT * FROM gold_db.location_revenue LIMIT 10;")
        print("=" * 50)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
