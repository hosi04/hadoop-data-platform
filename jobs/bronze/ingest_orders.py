import argparse
import sys
from pyspark.sql import functions as F
from jobs.common.cli_utils import add_partition_date_arg
from jobs.common.spark_session import create_spark_session
from jobs.common.schema.bronze import ORDERS_SCHEMA
from jobs.common.logging_utils import configure_logging


# ── Constants ─────────────────────────────────────────────────────────────────
LAYER         = "bronze"
ENTITY        = "orders"
HDFS_BASE     = f"/data/{LAYER}/{ENTITY}"
HIVE_DATABASE = LAYER
HIVE_TABLE    = f"{HIVE_DATABASE}.{ENTITY}"

# ── Logging ───────────────────────────────────────────────────────────────────
logger = configure_logging(layer=LAYER, entity=ENTITY)


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
    add_partition_date_arg(parser)
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
            created_at      STRING,
            updated_at      STRING,
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
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")

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
