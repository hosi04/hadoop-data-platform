from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)


ORDER_STATUSES = (
    "pending", "confirmed", "shipped", "delivered",
    "cancelled", "returned", "refunded",
)


ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("order_date", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=True),
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),
    StructField("channel", StringType(), nullable=False),  # "online" | "offline"
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("discount_amount", DoubleType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])


ORDER_STATUS_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),
    StructField("status", StringType(), nullable=False), 
    StructField("created_at", StringType(), nullable=False),  
])


CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("full_name", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("date_of_birth", StringType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("tier", StringType(), nullable=True), 
    StructField("acquisition_channel", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])


PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("brand", StringType(), nullable=True),
    StructField("category_l1", StringType(), nullable=True),
    StructField("category_l2", StringType(), nullable=True), 
    StructField("category_l3", StringType(), nullable=True), 
    StructField("supplier", StringType(), nullable=True),
    StructField("cost_price", DoubleType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])
