import argparse
import sys
from pyspark.sql import functions as F
from jobs.common.logging_utils import configure_logging
from jobs.common.spark_session import create_spark_session
from jobs.common.schema.bronze import PRODUCTS_SCHEMA


# ── Constants ─────────────────────────────────────────────────────────────────
LAYER = "bronze"
ENTITY = "products"
HDFS_BASE = f"/data/{LAYER}/{ENTITY}"
HIVE_DATABASE = LAYER
HIVE_TABLE = f"{HIVE_DATABASE}.{ENTITY}"

# ── Logging ───────────────────────────────────────────────────────────────────
logger = configure_logging(layer=LAYER, entity=ENTITY)


# ── Argument parsing ──────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest raw products CSV into HDFS Bronze layer"
    )
    parser.add_argument(
        "--source-path",
        required=True,
        help="Path to the source CSV file (local or HDFS)",
    )
    return parser.parse_args()


# ── Core logic ────────────────────────────────────────────────────────────────
def read_source(spark, source_path: str):
    logger.info("Reading source file: %s", source_path)
    return (
        spark.read
        .option("header", "true")
        .option("nullValue", "")
        .option("mode", "PERMISSIVE")
        .schema(PRODUCTS_SCHEMA)
        .csv(source_path)
    )


def add_ingestion_metadata(df, source_path: str):
    return (
        df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_path))
    )


def write_to_hdfs(df, output_path: str) -> int:
    row_count = df.count()
    logger.info("Writing %d rows to HDFS: %s", row_count, output_path)

    (
        df.write
        .mode("overwrite")   # full snapshot (SCD1) — dimension table, not partitioned by date
        .parquet(output_path)
    )
    return row_count


def register_hive_table(spark, output_path: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
            product_id STRING,
            product_name STRING,
            brand STRING,
            category_l1 STRING,
            category_l2 STRING,
            category_l3 STRING,
            supplier STRING,
            cost_price DOUBLE,
            created_at STRING,
            updated_at STRING,
            _ingested_at TIMESTAMP,
            _source_file STRING
        )
        STORED AS PARQUET
        LOCATION '{output_path}'
    """)
    logger.info("Hive table registered: %s", HIVE_TABLE)


def run(spark, source_path: str) -> None:
    df = read_source(spark, source_path)
    df = add_ingestion_metadata(df, source_path)

    row_count = write_to_hdfs(df, HDFS_BASE)
    register_hive_table(spark, HDFS_BASE)

    logger.info(
        "Job complete — entity=%s rows=%d path=%s",
        ENTITY, row_count, HDFS_BASE,
    )


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()
    spark = create_spark_session(app_name=f"{LAYER}.ingest_{ENTITY}")

    try:
        run(spark, args.source_path)
    except Exception:
        logger.exception("Job failed — see traceback above")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
