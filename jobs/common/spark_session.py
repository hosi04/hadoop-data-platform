"""
spark_session.py — Centralized SparkSession factory

All jobs import from here. Config is driven by environment variables
so the same job binary works across dev / staging / prod without changes.
"""

import os
import logging

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()

logger = logging.getLogger(__name__)

HDFS_HOST    = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT    = os.getenv("HDFS_PORT", "8020")
HIVE_HOST    = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT    = os.getenv("HIVE_PORT", "9083")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
LOG_LEVEL    = os.getenv("LOG_LEVEL", "WARN")


def create_spark_session(app_name: str) -> SparkSession:
    """
    Build and return a SparkSession configured for the Hasaki DWH stack.

    Connection targets (HDFS + Hive Metastore) are resolved from environment
    variables, making this factory environment-agnostic.

    Args:
        app_name: Shown in Spark UI and logs. Convention: "<layer>.<entity>"
                  e.g. "bronze.ingest_orders", "gold.build_fact_orders"

    Returns:
        A configured SparkSession with Hive support enabled.
    """
    logger.info(
        "Creating SparkSession: app=%s master=%s hdfs=%s:%s hive=%s:%s",
        app_name, SPARK_MASTER, HDFS_HOST, HDFS_PORT, HIVE_HOST, HIVE_PORT,
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(SPARK_MASTER)
        # ── HDFS ──────────────────────────────────────────
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{HDFS_HOST}:{HDFS_PORT}")
        # ── Hive Metastore ────────────────────────────────
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{HIVE_HOST}:{HIVE_PORT}")
        .config(
            "spark.sql.warehouse.dir",
            f"hdfs://{HDFS_HOST}:{HDFS_PORT}/user/hive/warehouse",
        )
        # ── Adaptive Query Execution (Spark 3+) ───────────
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        # ── Logging ───────────────────────────────────────
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.rootLogger={LOG_LEVEL}")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(LOG_LEVEL)
    return spark
