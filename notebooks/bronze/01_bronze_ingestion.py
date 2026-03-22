# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Bronze Layer (Raw Ingestion)
# MAGIC
# MAGIC **Purpose**: Ingest raw data from source systems into the Bronze (landing) layer with
# MAGIC minimal transformation. Data is stored as-is to preserve full lineage and enable
# MAGIC reprocessing from source.
# MAGIC
# MAGIC **Unity Catalog**: All objects are created under a **dedicated catalog** (`zomato_analytics`)
# MAGIC with schema `bronze`. This ensures **zero impact** on any existing catalogs, schemas, or tables.
# MAGIC
# MAGIC **Namespace**: `zomato_analytics.bronze.<table_name>`
# MAGIC
# MAGIC **Data Sources**:
# MAGIC - `customers` — customer registration & profile data
# MAGIC - `restaurants` — restaurant onboarding & metadata
# MAGIC - `orders` — order transactions
# MAGIC - `deliveries` — delivery tracking events
# MAGIC - `reviews` — customer reviews & ratings
# MAGIC
# MAGIC **SLA**: Runs every 15 minutes (near real-time) | Full refresh: daily at 02:00 UTC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Imports

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, lit, sha2, concat_ws,
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    IntegerType, TimestampType, ArrayType, FloatType,
)
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Parameters

# COMMAND ----------

# Notebook widgets for parameterized runs
dbutils.widgets.text("env", "dev", "Environment (dev/staging/prod)")
dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
dbutils.widgets.text("source_path", "/Volumes/zomato_analytics/raw/landing", "Source data path")
dbutils.widgets.dropdown("load_type", "incremental", ["full", "incremental"], "Load type")

ENV = dbutils.widgets.get("env")
CATALOG = dbutils.widgets.get("catalog_name")
SOURCE_PATH = dbutils.widgets.get("source_path")
LOAD_TYPE = dbutils.widgets.get("load_type")

# Fixed schema name — all Bronze tables live here
SCHEMA = "bronze"

# Fully qualified namespace: catalog.schema
FQN = f"{CATALOG}.{SCHEMA}"

print(f"Environment  : {ENV}")
print(f"Catalog      : {CATALOG}")
print(f"Schema       : {SCHEMA}")
print(f"Namespace    : {FQN}")
print(f"Source Path  : {SOURCE_PATH}")
print(f"Load Type    : {LOAD_TYPE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Dedicated Catalog & Schema (Idempotent)
# MAGIC
# MAGIC These statements use `IF NOT EXISTS` — they will **never** modify or drop
# MAGIC any existing catalog, schema, or table in your workspace.

# COMMAND ----------

# Create dedicated catalog — isolated from all other catalogs
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"COMMENT ON CATALOG {CATALOG} IS 'Zomato Analytics DE Project — Isolated catalog for medallion architecture'")

# Create bronze schema inside our catalog
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {FQN}")
spark.sql(f"COMMENT ON SCHEMA {FQN} IS 'Bronze layer — raw ingested data with audit columns'")

# Set current catalog & schema for this notebook
spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

print(f"✓ Catalog '{CATALOG}' ready")
print(f"✓ Schema '{FQN}' ready")
print(f"✓ Current context: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Schema Definitions
# MAGIC Explicit schemas enforce data contracts at ingestion time.

# COMMAND ----------

SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("city", StringType(), True),
        StructField("locality", StringType(), True),
        StructField("pincode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("signup_date", StringType(), True),
        StructField("subscription_tier", StringType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("preferred_payment", StringType(), True),
        StructField("preferred_cuisine", StringType(), True),
        StructField("total_orders_lifetime", IntegerType(), True),
        StructField("avg_order_value", DoubleType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_system", StringType(), True),
    ]),
    "restaurants": StructType([
        StructField("restaurant_id", StringType(), False),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True),
        StructField("locality", StringType(), True),
        StructField("address", StringType(), True),
        StructField("pincode", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("cuisines", ArrayType(StringType()), True),
        StructField("restaurant_type", StringType(), True),
        StructField("avg_cost_for_two", IntegerType(), True),
        StructField("rating", FloatType(), True),
        StructField("total_reviews", IntegerType(), True),
        StructField("is_delivering_now", BooleanType(), True),
        StructField("has_online_delivery", BooleanType(), True),
        StructField("has_table_booking", BooleanType(), True),
        StructField("is_premium_partner", BooleanType(), True),
        StructField("onboarded_date", StringType(), True),
        StructField("owner_name", StringType(), True),
        StructField("owner_phone", StringType(), True),
        StructField("gst_number", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_system", StringType(), True),
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), True),
        StructField("restaurant_id", StringType(), True),
        StructField("order_placed_at", StringType(), True),
        StructField("order_status", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("payment_status", StringType(), True),
        StructField("platform", StringType(), True),
        StructField("subtotal", DoubleType(), True),
        StructField("discount_pct", DoubleType(), True),
        StructField("discount_amount", DoubleType(), True),
        StructField("coupon_code", StringType(), True),
        StructField("delivery_fee", DoubleType(), True),
        StructField("tax_amount", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("num_items", IntegerType(), True),
        StructField("special_instructions", StringType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_system", StringType(), True),
    ]),
    "deliveries": StructType([
        StructField("delivery_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("delivery_partner_id", StringType(), True),
        StructField("delivery_partner_name", StringType(), True),
        StructField("partner_phone", StringType(), True),
        StructField("vehicle_type", StringType(), True),
        StructField("pickup_timestamp", StringType(), True),
        StructField("delivered_timestamp", StringType(), True),
        StructField("delivery_time_mins", IntegerType(), True),
        StructField("delivery_status", StringType(), True),
        StructField("delivery_distance_km", DoubleType(), True),
        StructField("delivery_rating", IntegerType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_system", StringType(), True),
    ]),
    "reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("restaurant_id", StringType(), True),
        StructField("rating", IntegerType(), True),
        StructField("review_text", StringType(), True),
        StructField("review_timestamp", StringType(), True),
        StructField("sentiment_label", StringType(), True),
        StructField("is_verified_order", BooleanType(), True),
        StructField("upvotes", IntegerType(), True),
        StructField("has_photo", BooleanType(), True),
        StructField("_ingested_at", StringType(), True),
        StructField("_source_system", StringType(), True),
    ]),
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze Ingestion Functions

# COMMAND ----------

def ingest_to_bronze(
    table_name: str,
    source_format: str = "parquet",
    merge_keys: list = None,
) -> int:
    """
    Ingest a raw dataset into the Bronze Delta table using Unity Catalog.

    All tables are created under: {CATALOG}.{SCHEMA}.{table_name}

    Adds audit columns:
      - _bronze_loaded_at : ingestion timestamp
      - _source_file      : originating file path
      - _row_hash         : SHA-256 hash for dedup / CDC

    Returns the number of records written.
    """
    source_file = f"{SOURCE_PATH}/{table_name}"
    schema = SCHEMAS.get(table_name)

    # Read raw data with enforced schema
    raw_df = (
        spark.read
        .format(source_format)
        .schema(schema)
        .option("mergeSchema", "true")
        .load(source_file)
    )

    # Add audit / lineage columns
    enriched_df = (
        raw_df
        .withColumn("_bronze_loaded_at", current_timestamp())
        .withColumn("_source_file", col("_metadata.file_path"))
        .withColumn(
            "_row_hash",
            sha2(concat_ws("||", *[col(c) for c in raw_df.columns]), 256),
        )
    )

    # Fully qualified table name: catalog.schema.table
    target_table = f"{FQN}.{table_name}"

    if LOAD_TYPE == "full" or not spark.catalog.tableExists(target_table):
        # Full load — overwrite (creates table if not exists)
        (
            enriched_df.write
            .format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")
            .saveAsTable(target_table)
        )
    else:
        # Incremental merge (upsert)
        delta_target = DeltaTable.forName(spark, target_table)
        pk = merge_keys[0] if merge_keys else f"{table_name[:-1]}_id"
        (
            delta_target.alias("tgt")
            .merge(enriched_df.alias("src"), f"tgt.{pk} = src.{pk}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    record_count = enriched_df.count()
    print(f"  ✓ {target_table}: {record_count:,} records ingested ({LOAD_TYPE})")
    return record_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Execute Bronze Ingestion Pipeline

# COMMAND ----------

pipeline_tables = [
    {"table_name": "customers",   "merge_keys": ["customer_id"]},
    {"table_name": "restaurants",  "merge_keys": ["restaurant_id"]},
    {"table_name": "orders",       "merge_keys": ["order_id"]},
    {"table_name": "deliveries",   "merge_keys": ["delivery_id"]},
    {"table_name": "reviews",      "merge_keys": ["review_id"]},
]

total_records = 0
pipeline_status = []

for tbl_cfg in pipeline_tables:
    try:
        count = ingest_to_bronze(**tbl_cfg)
        total_records += count
        pipeline_status.append({"table": tbl_cfg["table_name"], "status": "SUCCESS", "records": count})
    except Exception as e:
        pipeline_status.append({"table": tbl_cfg["table_name"], "status": "FAILED", "error": str(e)})
        print(f"  ✗ {tbl_cfg['table_name']}: FAILED — {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Pipeline Summary & Data Quality Checks

# COMMAND ----------

import json
from datetime import datetime

summary = {
    "pipeline": "bronze_ingestion",
    "catalog": CATALOG,
    "schema": SCHEMA,
    "env": ENV,
    "load_type": LOAD_TYPE,
    "run_timestamp": datetime.utcnow().isoformat(),
    "total_records_ingested": total_records,
    "table_results": pipeline_status,
}

print(json.dumps(summary, indent=2))

# COMMAND ----------

# Basic row-count validation
for tbl_cfg in pipeline_tables:
    tbl = tbl_cfg["table_name"]
    fq_table = f"{FQN}.{tbl}"
    count = spark.table(fq_table).count()
    assert count > 0, f"Data quality check FAILED: {fq_table} has 0 records!"
    print(f"  ✓ {fq_table}: {count:,} rows — PASS")

print(f"\n🟢 Bronze ingestion pipeline completed successfully.")
print(f"   All tables created under: {FQN}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Verify Isolation — List Only Our Objects

# COMMAND ----------

print(f"Tables in {FQN}:")
spark.sql(f"SHOW TABLES IN {FQN}").show(truncate=False)
