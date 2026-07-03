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
    StructField("order_date", StringType(), nullable=False),  # kept as string, cast in Silver
    StructField("customer_id", StringType(), nullable=True),  # nullable: guest checkout allowed
    StructField("product_id", StringType(), nullable=False),
    StructField("store_id", StringType(), nullable=True),  # nullable: online orders have no store
    StructField("channel", StringType(), nullable=False),  # "online" | "offline"
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DoubleType(), nullable=False),
    StructField("discount_amount", DoubleType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])


ORDER_STATUS_EVENTS_SCHEMA = StructType([
    StructField("order_id", StringType(), nullable=False),  # FK -> orders.order_id
    StructField("status", StringType(), nullable=False),  # see ORDER_STATUSES
    StructField("created_at", StringType(), nullable=False),  # kept as string, cast in Silver
])


CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType(), nullable=False),
    StructField("full_name", StringType(), nullable=True),
    StructField("gender", StringType(), nullable=True),
    StructField("date_of_birth", StringType(), nullable=True),
    StructField("province", StringType(), nullable=True),
    StructField("tier", StringType(), nullable=True),  # Bronze/Silver/Gold/VIP
    StructField("acquisition_channel", StringType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])


PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType(), nullable=False),
    StructField("product_name", StringType(), nullable=False),
    StructField("brand", StringType(), nullable=True),
    StructField("category_l1", StringType(), nullable=True),  # e.g. "Skincare"
    StructField("category_l2", StringType(), nullable=True),  # e.g. "Moisturizer"
    StructField("category_l3", StringType(), nullable=True),  # e.g. "Gel cream"
    StructField("supplier", StringType(), nullable=True),
    StructField("cost_price", DoubleType(), nullable=True),
    StructField("created_at", StringType(), nullable=True),
    StructField("updated_at", StringType(), nullable=True),
])
