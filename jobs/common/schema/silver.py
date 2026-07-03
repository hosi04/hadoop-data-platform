from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_date", DateType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("discount_amount", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),
    StructField("created_at", TimestampType(), nullable=True),
])

ORDER_STATUS_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),
    StructField("updated_at", TimestampType(), nullable=False)
])