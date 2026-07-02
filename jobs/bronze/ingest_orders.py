"""
bronze/ingest_orders.py — Ingest raw orders CSV into HDFS Bronze layer

Responsibilities:
  1. Read source CSV with an explicit schema (no schema inference)
  2. Attach ingestion metadata (_ingested_at, _source_file)
  3. Write to HDFS as Parquet, partitioned by year/month/day
  4. Register the partition in Hive Metastore

This job is idempotent: re-running with the same --partition-date
overwrites that day's partition only, leaving other partitions untouched.

Usage:
    spark-submit jobs/bronze/ingest_orders.py \\
        --source-path data/sample/orders.csv \\
        --partition-date 2026-07-01

    # Via Makefile:
    make ingest-orders DATE=2026-07-01
"""

import argparse
import logging
import sys
from datetime import datetime

from pyspark.sql import functions as F

from jobs.common.spark_session import create_spark_session
from jobs.common.schema.bronze import ORDERS_SCHEMA

# ── Constants ─────────────────────────────────────────────────────────────────

LAYER         = "bronze"
ENTITY        = "orders"
HDFS_BASE     = f"/data/{LAYER}/{ENTITY}"
HIVE_DATABASE = LAYER
HIVE_TABLE    = f"{HIVE_DATABASE}.{ENTITY}"

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(f"{LAYER}.ingest_{ENTITY}")


# ── Argument parsing ──────────────────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw orders CSV into HDFS Bronze layer"
    )
    parser.add_argument(
        "--source-path",
        required=True,
        help="Path to the source CSV file (local or HDFS)",
    )
    parser.add_argument(
        "--partition-date",
        default=datetime.today().strftime("%Y-%m-%d"),
        help="Target partition date in YYYY-MM-DD format (default: today)",
    )
    return parser.parse_args()


# ── Core logic ────────────────────────────────────────────────────────────────

def read_source(spark, source_path: str):
    logger.info("Reading source file: %s", source_path)
    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")        # treat empty string as null
        .option("mode", "PERMISSIVE")   # log malformed rows instead of failing
        .schema(ORDERS_SCHEMA)
        .csv(source_path)
    )


def add_ingestion_metadata(df, source_path: str):
    """
    Attach audit columns so we can always trace when and from where
    a record was loaded — critical for data lineage.
    """
    return (
        df
        .withColumn("_ingested_at",  F.current_timestamp())
        .withColumn("_source_file",  F.lit(source_path))
    )


def write_to_hdfs(df, output_path: str) -> int:
    row_count = df.count()
    logger.info("Writing %d rows to HDFS: %s", row_count, output_path)

    (
        df.write
        .mode("overwrite")      # idempotent: safe to re-run for the same partition
        .parquet(output_path)
    )
    return row_count


def register_hive_partition(spark, year: str, month: str, day: str, output_path: str) -> None:
    """
    Create the Hive table if it doesn't exist, then register this partition
    so Spark SQL / Trino can query it immediately after ingestion.
    """
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
            order_id        STRING,
            order_date      STRING,
            customer_id     STRING,
            product_id      STRING,
            store_id        STRING,
            channel         STRING,
            quantity        INT,
            unit_price      DOUBLE,
            discount_amount DOUBLE,
            is_returned     BOOLEAN,
            created_at      STRING,
            _ingested_at    TIMESTAMP,
            _source_file    STRING
        )
        PARTITIONED BY (year STRING, month STRING, day STRING)
        STORED AS PARQUET
        LOCATION '{HDFS_BASE}'
    """)

    spark.sql(f"""
        ALTER TABLE {HIVE_TABLE}
        ADD IF NOT EXISTS PARTITION (year='{year}', month='{month}', day='{day}')
        LOCATION '{output_path}'
    """)
    logger.info(
        "Hive partition registered: %s year=%s/month=%s/day=%s",
        HIVE_TABLE, year, month, day,
    )


def run(spark, source_path: str, partition_date: str) -> None:
    year, month, day = partition_date.split("-")
    output_path = f"{HDFS_BASE}/year={year}/month={month}/day={day}"

    df = read_source(spark, source_path)
    df = add_ingestion_metadata(df, source_path)

    row_count = write_to_hdfs(df, output_path)
    register_hive_partition(spark, year, month, day, output_path)

    logger.info(
        "Job complete — entity=%s partition=%s rows=%d path=%s",
        ENTITY, partition_date, row_count, output_path,
    )


# ── Entry point ───────────────────────────────────────────────────────────────

def main() -> None:
    args = parse_args()
    spark = create_spark_session(app_name=f"{LAYER}.ingest_{ENTITY}")

    try:
        run(spark, args.source_path, args.partition_date)
    except Exception:
        logger.exception("Job failed — see traceback above")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
