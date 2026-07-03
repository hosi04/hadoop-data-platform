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
    StructField("customer_id", StringType(), nullable=False),  # guest orders filled with "GUEST"
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),  # nullable: online orders have no store
    StructField("channel", StringType(), nullable=False),  # "online" | "offline"
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("discount_amount", DoubleType(), nullable=True),
    StructField("status", StringType(), nullable=True),  # current status, joined from order_status_events
    StructField("created_at", TimestampType(), nullable=True),
])

ORDER_STATUS_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False),  # "pending" | "shipped" | "delivered" | "cancelled"
    StructField("updated_at", TimestampType(), nullable=False)
])