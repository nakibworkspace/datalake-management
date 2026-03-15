"""
Step 3 — Query: Run Spark SQL analytics on the processed Parquet data.

Demonstrates querying the data lake directly using SQL — no separate
data warehouse needed.  This is the pattern behind tools like
AWS Athena, Databricks SQL, and Trino.

Usage:
    python scripts/query.py
"""

import os

from pyspark.sql import SparkSession


def create_spark(master_url: str) -> SparkSession:
    """Build a SparkSession configured for S3 access."""
    return (
        SparkSession.builder
        .appName("lab02-query")
        .master(master_url)
        .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
        .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
        .config("spark.hadoop.fs.s3a.endpoint", f"s3.{os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}.amazonaws.com")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.path.style.access", "false")
        .getOrCreate()
    )


def run_query(spark, name: str, sql: str):
    """Execute and display a named query."""
    print(f"  {name}")
    spark.sql(sql).show(truncate=False)


def main():
    bucket     = os.environ["S3_BUCKET"]
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")

    processed_path = f"s3a://{bucket}/processed/orders/"

    spark = create_spark(master_url)

    # Register Parquet data as a SQL table
    df = spark.read.parquet(processed_path)
    df.createOrReplaceTempView("orders")

    print(f"Loaded {df.count()} records from {processed_path}")
    print("Registered as table: orders")

    # ── Query 1: Revenue by Category ─────────────────────────────────
    run_query(spark, "Revenue by Product Category", """
        SELECT
            category,
            COUNT(*)              AS total_orders,
            SUM(calc_total)       AS total_revenue,
            ROUND(AVG(calc_total), 2) AS avg_order_value
        FROM orders
        WHERE order_status = 'completed'
        GROUP BY category
        ORDER BY total_revenue DESC
    """)

    # ── Query 2: Top 5 Products by Units Sold ────────────────────────
    run_query(spark, "Top 5 Products by Units Sold", """
        SELECT
            product_name,
            SUM(quantity)         AS units_sold,
            SUM(calc_total)       AS revenue
        FROM orders
        WHERE order_status = 'completed'
        GROUP BY product_name
        ORDER BY units_sold DESC
        LIMIT 5
    """)

    # ── Query 3: Orders by Country ───────────────────────────────────
    run_query(spark, "Order Distribution by Country", """
        SELECT
            shipping_country,
            COUNT(*)              AS total_orders,
            SUM(calc_total)       AS total_revenue,
            COUNT(DISTINCT customer_id) AS unique_customers
        FROM orders
        GROUP BY shipping_country
        ORDER BY total_orders DESC
    """)

    # ── Query 4: Payment Method Breakdown ────────────────────────────
    run_query(spark, "Payment Method Breakdown", """
        SELECT
            payment_method,
            COUNT(*)              AS usage_count,
            ROUND(SUM(calc_total), 2) AS total_amount,
            ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM orders), 1) AS pct
        FROM orders
        GROUP BY payment_method
        ORDER BY usage_count DESC
    """)

    # ── Query 5: Weekend vs Weekday Sales ────────────────────────────
    run_query(spark, "Weekend vs Weekday Performance", """
        SELECT
            CASE WHEN is_weekend THEN 'Weekend' ELSE 'Weekday' END AS period,
            COUNT(*)              AS total_orders,
            ROUND(SUM(calc_total), 2)   AS total_revenue,
            ROUND(AVG(calc_total), 2)   AS avg_order_value
        FROM orders
        WHERE order_status = 'completed'
        GROUP BY is_weekend
        ORDER BY period
    """)

    # ── Query 6: Daily Order Trend ───────────────────────────────────
    run_query(spark, "Daily Order Trend (Last 7 Days)", """
        SELECT
            order_date,
            COUNT(*)              AS orders,
            ROUND(SUM(calc_total), 2) AS revenue
        FROM orders
        GROUP BY order_date
        ORDER BY order_date DESC
        LIMIT 7
    """)

    spark.stop()
    print("\nAll queries completed.")


if __name__ == "__main__":
    main()
