import sys
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from jobs.common.logging_utils import configure_logging
from jobs.common.spark_session import create_spark_session


# ── Constants ─────────────────────────────────────────────────────────────────
LAYER = "silver"
ENTITY = "products"
HDFS_BASE = f"/data/{LAYER}/{ENTITY}"
HIVE_DATABASE = LAYER
HIVE_TABLE = f"{HIVE_DATABASE}.{ENTITY}"

BRONZE_PRODUCTS_TABLE = "bronze.products"


# ── Logging ───────────────────────────────────────────────────────────────────
logger = configure_logging(layer=LAYER, entity=f"clean_{ENTITY}")


# ── Core logic ────────────────────────────────────────────────────────────────
def read_products(spark):
    return spark.table(BRONZE_PRODUCTS_TABLE)


def deduplicate_products(df):
    product_window = Window.partitionBy("product_id").orderBy(F.col("_ingested_at").desc())

    return (
        df
        .withColumn("_row_number", F.row_number().over(product_window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def standardize_types(df):
    return (
        df
        .withColumn("created_at", F.to_timestamp(F.col("created_at"), "yyyy-MM-dd HH:mm:ss"))
        .withColumn("updated_at", F.to_timestamp(F.col("updated_at"), "yyyy-MM-dd HH:mm:ss"))
    )


def validate(df):
    null_product_id_count = df.filter(F.col("product_id").isNull()).count()
    if null_product_id_count > 0:
        raise ValueError(f"Found {null_product_id_count} rows with NULL product_id")

    return df


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
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        )
        STORED AS PARQUET
        LOCATION '{output_path}'
    """)
    logger.info("Hive table registered: %s", HIVE_TABLE)


def run(spark) -> None:
    df = read_products(spark)
    df = deduplicate_products(df)
    df = standardize_types(df)
    df = validate(df)

    row_count = write_to_hdfs(df, HDFS_BASE)
    register_hive_table(spark, HDFS_BASE)

    logger.info(
        "Job complete — entity=%s rows=%d path=%s",
        ENTITY, row_count, HDFS_BASE,
    )


# ── Entry point ───────────────────────────────────────────────────────────────
def main() -> None:
    spark = create_spark_session(app_name=f"{LAYER}.clean_{ENTITY}")

    try:
        run(spark)
    except Exception:
        logger.exception("Job failed — see traceback above")
        sys.exit(1)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
