import argparse
import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from jobs.common.cli_utils import add_partition_date_arg
from jobs.common.logging_utils import configure_logging
from jobs.common.spark_session import create_spark_session
from jobs.common.schema.silver import ORDERS_SCHEMA


# ── Constants ─────────────────────────────────────────────────────────────────
LAYER = "silver"
ENTITY = "orders"
HDFS_BASE = f"/data/{LAYER}/{ENTITY}"
HIVE_DATABASE = LAYER
HIVE_TABLE = f"{HIVE_DATABASE}.{ENTITY}"

BRONZE_ORDERS_TABLE = "bronze.orders"
BRONZE_STATUS_EVENTS_TABLE = "bronze.order_status_events"


# ── Logging ───────────────────────────────────────────────────────────────────
logger = configure_logging(layer=LAYER, entity=f"clean_{ENTITY}")


# ── Argument parsing ──────────────────────────────────────────────────────────
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Clean and enrich orders into HDFS Silver layer"
    )
    add_partition_date_arg(parser)
    return parser.parse_args()


# ── Core logic ────────────────────────────────────────────────────────────────
def read_orders(spark, partition_date: str):
    if not partition_date:
        raise ValueError("partition_date is required")

    return spark.table(BRONZE_ORDERS_TABLE).where(F.col("order_date") == partition_date)


def read_status_events(spark):
    return spark.table(BRONZE_STATUS_EVENTS_TABLE)


def deduplicate_orders(df):
    order_window = Window.partitionBy("order_id").orderBy(F.col("_ingested_at").desc())

    return (
        df
        .withColumn("_row_number", F.row_number().over(order_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def get_current_status(status_events_df):
    current_window = Window.partitionBy("order_id").orderBy(F.col("created_at").desc())

    return (
        status_events_df
        .withColumn("_row_number", F.row_number().over(current_window))
        .filter(F.col("_row_number") == 1)
        .select(
            F.col("order_id"),
            F.col("status"),
        )
    )


def standardize_types(df):
    return (
        df
        .withColumn("order_date", F.to_date(F.col("order_date"), "yyyy-MM-dd"))
        .withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"))
    )


def fill_guest_customer(df):
    return df.withColumn("customer_id", F.coalesce(F.col("customer_id"), F.lit("GUEST")))


def join_current_status(orders_df, current_status_df):
    return orders_df.join(current_status_df, on="order_id", how="left")


def validate(df):
    null_order_id_count = df.filter(F.col("order_id").isNull()).count()
    if null_order_id_count > 0:
        raise ValueError(f"Found {null_order_id_count} rows with NULL order_id")

    missing_store_count = df.filter(
        (F.col("channel") == "offline") & F.col("store_id").isNull()
    ).count()
    if missing_store_count > 0:
        logger.warning(
            "%d offline orders have NULL store_id — data quality issue",
            missing_store_count,
        )

    return df


def write_to_hdfs(df, output_path: str) -> int:
    row_count = df.count()
    logger.info("Writing %d rows to HDFS: %s", row_count, output_path)

    (
        df.write
        .mode("overwrite")
        .parquet(output_path)
    )
    return row_count


def register_hive_partition(spark, year: str, month: str, day: str, output_path: str) -> None:
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {HIVE_DATABASE}")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {HIVE_TABLE} (
            order_id STRING,
            order_date DATE,
            customer_id STRING,
            product_id STRING,
            store_id STRING,
            channel STRING,
            quantity INT,
            unit_price DOUBLE,
            discount_amount DOUBLE,
            status STRING,
            created_at TIMESTAMP
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


def run(spark, partition_date: str) -> None:
    year, month, day = partition_date.split("-")
    output_path = f"{HDFS_BASE}/year={year}/month={month}/day={day}"

    orders_df = read_orders(spark, partition_date)
    orders_df = deduplicate_orders(orders_df)

    status_events_df = read_status_events(spark)
    current_status_df = get_current_status(status_events_df)

    df = join_current_status(orders_df, current_status_df)
    df = standardize_types(df)
    df = fill_guest_customer(df)
    df = validate(df)

    row_count = write_to_hdfs(df, output_path)
    register_hive_partition(spark, year, month, day, output_path)

    logger.info(
        "Job complete — entity=%s partition=%s rows=%d path=%s",
        ENTITY, partition_date, row_count, output_path,
    )


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    args = parse_args()
    spark = create_spark_session(app_name=f"{LAYER}.clean_{ENTITY}")

    try:
        run(spark, args.partition_date)
    except Exception:
        logger.exception("Job failed — see traceback above")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
