# Lab 02 — Quick Testing & Verification Guide

A step-by-step checklist to verify every component of the pipeline.

---

## Prerequisites

```bash
# 1. Copy and fill in your AWS credentials
cp .env.example .env
# Edit .env with your real AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET

# 2. Create the S3 bucket (if it doesn't exist yet)
aws s3 mb s3://my-datalake-lab02 --region us-east-1
```

---

## 1 — Build and Start the Cluster

```bash
docker compose build
docker compose up -d
```

**Verify:**

```bash
# All 3 containers should be running (spark-master, spark-worker, pipeline)
docker compose ps

# Spark Master UI should be accessible
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080
# Expected: 200

# Check spark worker registered with master
docker compose logs spark-worker | grep -i "registered"
```

---

## 2 — Test Ingestion (Raw JSON → S3)

```bash
docker compose exec pipeline python3 scripts/ingest.py
```

To generate more events (e.g. 5000):

```bash
docker compose exec pipeline python3 scripts/ingest.py --count 5000
```

**Verify:**

```bash
# List raw files in S3
aws s3 ls s3://my-datalake-lab02/orders/ --recursive

# Check a sample file's content (pick any file from the listing above)
aws s3 cp s3://my-datalake-lab02/orders/date=2026-03-12/batch-xxxxxxxx.json - | head -1 | python3 -m json.tool

# Count total raw files
aws s3 ls s3://my-datalake-lab02/orders/ --recursive | wc -l

# Verify partitioning — you should see date=YYYY-MM-DD folders
aws s3 ls s3://my-datalake-lab02/orders/
```

**Expected:** ~20-30 date-partitioned folders, each containing a batch JSON file. Each line in the JSON file is a valid order event.

---

## 3 — Test Processing (JSON → Parquet)

```bash
docker compose exec pipeline python3 scripts/process.py
```

**Verify:**

```bash
# Parquet files should exist in the processed path
aws s3 ls s3://my-datalake-lab02/processed/orders/ --recursive | head -10

# Check partition structure
aws s3 ls s3://my-datalake-lab02/processed/orders/

# Verify file sizes (Parquet should be smaller than raw JSON)
aws s3 ls s3://my-datalake-lab02/orders/ --recursive --summarize | tail -2
aws s3 ls s3://my-datalake-lab02/processed/orders/ --recursive --summarize | tail -2
```

**Expected:** Parquet files partitioned by `order_date=YYYY-MM-DD`. Total size should be noticeably smaller than the raw JSON due to columnar compression.

---

## 4 — Test Queries (Spark SQL on Parquet)

```bash
docker compose exec pipeline python3 scripts/query.py
```

**Expected output:** Six analytics tables printed to the console:

| # | Query | What to check |
|---|-------|---------------|
| 1 | Revenue by Category | Electronics should be highest revenue |
| 2 | Top 5 Products | 5 rows, units_sold > 0 |
| 3 | Orders by Country | ~10 countries, US likely the most |
| 4 | Payment Method | 5 methods, percentages sum to ~100% |
| 5 | Weekend vs Weekday | 2 rows: Weekend and Weekday |
| 6 | Daily Trend | 7 rows, recent dates |

---

## 5 — Run the Full Pipeline End-to-End

```bash
docker compose exec pipeline python3 scripts/ingest.py && \
docker compose exec pipeline python3 scripts/process.py && \
docker compose exec pipeline python3 scripts/query.py
```

This runs `ingest → process → query` sequentially. All checks from sections 2-4 apply.

---

## 6 — Interactive Exploration

```bash
# Open a PySpark shell connected to the Spark cluster
docker compose exec pipeline pyspark --master spark://spark-master:7077
```

Then inside PySpark:

```python
# Read the processed Parquet
df = spark.read.parquet("s3a://my-datalake-lab02/processed/orders/")
df.printSchema()
df.show(5)

# Run ad-hoc SQL
df.createOrReplaceTempView("orders")
spark.sql("SELECT shipping_country, COUNT(*) as cnt FROM orders GROUP BY shipping_country").show()

# Check for data quality
df.filter(df.calc_total != df.total_amount).count()  # should be 0

# Exit
exit()
```

---

## 7 — Verify Spark Cluster

```bash
# Open Spark Master UI in browser
# http://localhost:8080

# Check worker is registered
docker compose logs spark-worker 2>&1 | grep -i "registered"

# Check running applications
docker compose logs spark-master 2>&1 | grep -i "registered app"
```

---

## 8 — Debugging

```bash
# Tail logs from all services
docker compose logs -f

# Open a bash shell in the pipeline container
docker compose exec pipeline bash

# Check container resource usage
docker stats --no-stream
```

---

## 9 — Cleanup

```bash
# Stop containers
docker compose down

# Stop containers and remove Docker volumes
docker compose down -v

# Delete S3 data (optional — careful!)
aws s3 rm s3://my-datalake-lab02/ --recursive
aws s3 rb s3://my-datalake-lab02
```

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `NoCredentialsError` | Check `.env` has valid `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| `NoSuchBucket` | Run `aws s3 mb s3://your-bucket-name` |
| `Connection refused` on Spark | Wait 10-15s after `docker compose up -d`, then retry |
| Spark worker not showing in UI | Run `docker compose logs spark-worker` to check errors |
| Parquet write fails | Check IAM permissions — need `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` |
| `AccessDenied` | Verify your IAM user/role has the correct S3 policy attached |

---

## Quick Smoke Test (one-liner)

```bash
docker compose build && docker compose up -d && sleep 15 && \
docker compose exec pipeline python scripts/ingest.py && \
docker compose exec pipeline python scripts/process.py && \
docker compose exec pipeline python scripts/query.py && \
echo "ALL GOOD"
```
