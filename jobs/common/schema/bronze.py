"""
schema/bronze.py — Explicit schema definitions for Bronze layer

Bronze = raw ingestion layer. Schemas mirror the source system structure
exactly — no business logic, no type coercion beyond what's necessary to parse.

Design principles:
- All date/timestamp fields kept as StringType to preserve raw source values.
  Type casting happens in the Silver transformation step.
- nullable=False only on fields that are truly non-nullable in the source system.
- Adding new source fields: append to the schema, never rename or remove
  (Bronze is append-only and immutable by convention).
"""

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

ORDERS_SCHEMA = StructType([
    StructField("order_id",        StringType(),  nullable=False),
    StructField("order_date",      StringType(),  nullable=False),  # kept as string, cast in Silver
    StructField("customer_id",     StringType(),  nullable=True),   # nullable: guest checkout allowed
    StructField("product_id",      StringType(),  nullable=False),
    StructField("store_id",        StringType(),  nullable=True),   # nullable: online orders have no store
    StructField("channel",         StringType(),  nullable=False),  # "online" | "offline"
    StructField("quantity",        IntegerType(), nullable=False),
    StructField("unit_price",      DoubleType(),  nullable=False),
    StructField("discount_amount", DoubleType(),  nullable=True),
    StructField("is_returned",     BooleanType(), nullable=True),
    StructField("created_at",      StringType(),  nullable=True),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id",          StringType(), nullable=False),
    StructField("full_name",            StringType(), nullable=True),
    StructField("gender",               StringType(), nullable=True),
    StructField("date_of_birth",        StringType(), nullable=True),
    StructField("province",             StringType(), nullable=True),
    StructField("tier",                 StringType(), nullable=True),  # Bronze/Silver/Gold/VIP
    StructField("acquisition_channel",  StringType(), nullable=True),
    StructField("registered_at",        StringType(), nullable=True),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id",    StringType(), nullable=False),
    StructField("product_name",  StringType(), nullable=False),
    StructField("brand",         StringType(), nullable=True),
    StructField("category_l1",   StringType(), nullable=True),  # e.g. "Skincare"
    StructField("category_l2",   StringType(), nullable=True),  # e.g. "Moisturizer"
    StructField("category_l3",   StringType(), nullable=True),  # e.g. "Gel cream"
    StructField("supplier",      StringType(), nullable=True),
    StructField("cost_price",    DoubleType(), nullable=True),
    StructField("created_at",    StringType(), nullable=True),
])
