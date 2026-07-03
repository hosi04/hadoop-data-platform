import os

from dotenv import load_dotenv

load_dotenv()

HDFS_HOST = os.getenv("HDFS_HOST", "localhost")
HDFS_PORT = os.getenv("HDFS_PORT", "8020")
HIVE_HOST = os.getenv("HIVE_HOST", "localhost")
HIVE_PORT = os.getenv("HIVE_PORT", "9083")
SPARK_MASTER = os.getenv("SPARK_MASTER", "local[*]")
LOG_LEVEL = os.getenv("LOG_LEVEL", "WARN")
