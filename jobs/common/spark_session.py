import logging
from pyspark.sql import SparkSession
from jobs.common import env_config


logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    logger.info(
        "Creating SparkSession: app=%s master=%s hdfs=%s:%s hive=%s:%s",
        app_name, env_config.SPARK_MASTER, env_config.HDFS_HOST, env_config.HDFS_PORT,
        env_config.HIVE_HOST, env_config.HIVE_PORT,
    )

    spark = (
        SparkSession.builder
        .appName(app_name)
        .master(env_config.SPARK_MASTER)
        .config("spark.hadoop.fs.defaultFS", f"hdfs://{env_config.HDFS_HOST}:{env_config.HDFS_PORT}")
        .config("spark.sql.catalogImplementation", "hive")
        .config("spark.hadoop.hive.metastore.uris", f"thrift://{env_config.HIVE_HOST}:{env_config.HIVE_PORT}")
        .config(
            "spark.sql.warehouse.dir",
            f"hdfs://{env_config.HDFS_HOST}:{env_config.HDFS_PORT}/user/hive/warehouse",
        )
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.driver.extraJavaOptions", f"-Dlog4j.rootLogger={env_config.LOG_LEVEL}")
        .enableHiveSupport()
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel(env_config.LOG_LEVEL)
    return spark
