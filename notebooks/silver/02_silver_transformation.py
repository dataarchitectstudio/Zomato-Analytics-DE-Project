# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Silver Layer (Cleansing & Transformation)
# MAGIC
# MAGIC **Purpose**: Cleanse, deduplicate, standardize, and enrich Bronze data into a
# MAGIC conformed, analytics-ready Silver layer. Applies business rules, data-type casting,
# MAGIC null handling, and referential integrity checks.
# MAGIC
# MAGIC **Unity Catalog**: `zomato_analytics.silver.*`
# MAGIC
# MAGIC **Input**: `zomato_analytics.bronze.*`
# MAGIC **Output**: `zomato_analytics.silver.*`
# MAGIC
# MAGIC **SLA**: Triggered after Bronze ingestion completes | Max latency: 30 min

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration & Imports

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, trim, lower, upper, initcap,
    to_timestamp, to_date, datediff, current_timestamp,
    regexp_replace, concat_ws, sha2, row_number, explode,
    coalesce, round as spark_round, expr, size, split,
    year, month, dayofweek, hour, date_format,
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Parameters

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")

ENV = dbutils.widgets.get("env")
CATALOG = dbutils.widgets.get("catalog_name")

# Schema names within our dedicated catalog
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# Fully qualified namespaces
BRONZE_FQN = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER_FQN = f"{CATALOG}.{SILVER_SCHEMA}"

print(f"Environment   : {ENV}")
print(f"Catalog       : {CATALOG}")
print(f"Bronze source : {BRONZE_FQN}")
print(f"Silver target : {SILVER_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create Silver Schema (Idempotent — No Impact on Existing Objects)

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SILVER_FQN}")
spark.sql(f"COMMENT ON SCHEMA {SILVER_FQN} IS 'Silver layer — cleansed, deduplicated, and conformed data'")

print(f"✓ Schema '{SILVER_FQN}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Shared Transformation Utilities

# COMMAND ----------

class DataQualityMetrics:
    """Track data quality metrics across transformations."""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.input_count = 0
        self.output_count = 0
        self.duplicates_removed = 0
        self.nulls_filled = 0
        self.invalid_records_quarantined = 0

    def summary(self) -> dict:
        return {
            "table": self.table_name,
            "input_rows": self.input_count,
            "output_rows": self.output_count,
            "duplicates_removed": self.duplicates_removed,
            "nulls_filled": self.nulls_filled,
            "quarantined": self.invalid_records_quarantined,
            "pass_rate_pct": round(self.output_count / max(self.input_count, 1) * 100, 2),
        }


def deduplicate(df: DataFrame, key_columns: list, order_col: str = "_bronze_loaded_at") -> DataFrame:
    """Remove duplicates keeping the latest record per key."""
    window = Window.partitionBy(*key_columns).orderBy(col(order_col).desc())
    return df.withColumn("_rn", row_number().over(window)).filter(col("_rn") == 1).drop("_rn")


def standardize_text_columns(df: DataFrame, columns: list) -> DataFrame:
    """Trim whitespace and apply proper casing."""
    for c in columns:
        df = df.withColumn(c, initcap(trim(col(c))))
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver — Customers

# COMMAND ----------

def transform_customers() -> DataQualityMetrics:
    """Cleanse and transform customer data."""
    metrics = DataQualityMetrics("silver_customers")

    df = spark.table(f"{BRONZE_FQN}.customers")
    metrics.input_count = df.count()

    # Deduplicate
    df = deduplicate(df, ["customer_id"])
    metrics.duplicates_removed = metrics.input_count - df.count()

    # Cleanse & transform
    df_clean = (
        df
        .withColumn("first_name", initcap(trim(col("first_name"))))
        .withColumn("last_name", initcap(trim(col("last_name"))))
        .withColumn("full_name", concat_ws(" ", col("first_name"), col("last_name")))
        .withColumn("email", lower(trim(col("email"))))
        .withColumn("phone_cleaned", regexp_replace(col("phone"), "[^0-9+]", ""))
        .withColumn("signup_date", to_timestamp(col("signup_date")))
        .withColumn("signup_year", year(col("signup_date")))
        .withColumn("signup_month", month(col("signup_date")))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("subscription_tier", coalesce(col("subscription_tier"), lit("Free")))
        .withColumn("is_active", coalesce(col("is_active"), lit(True)))
        .withColumn("total_orders_lifetime", coalesce(col("total_orders_lifetime"), lit(0)))
        .withColumn("avg_order_value", coalesce(col("avg_order_value"), lit(0.0)))
        .withColumn(
            "customer_segment",
            when(col("total_orders_lifetime") >= 100, "Power User")
            .when(col("total_orders_lifetime") >= 30, "Regular")
            .when(col("total_orders_lifetime") >= 5, "Occasional")
            .otherwise("New"),
        )
        .withColumn("_silver_loaded_at", current_timestamp())
        .withColumn("_data_quality_score", lit(1.0))
        .drop("_ingested_at", "_source_system", "_source_file", "_row_hash", "_bronze_loaded_at")
    )

    valid_df = df_clean.filter(
        col("customer_id").isNotNull()
        & col("email").isNotNull()
        & col("email").rlike("^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z]{2,}$")
    )
    metrics.invalid_records_quarantined = df_clean.count() - valid_df.count()

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_FQN}.customers")

    metrics.output_count = valid_df.count()
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — Restaurants

# COMMAND ----------

def transform_restaurants() -> DataQualityMetrics:
    """Cleanse and transform restaurant data."""
    metrics = DataQualityMetrics("silver_restaurants")

    df = spark.table(f"{BRONZE_FQN}.restaurants")
    metrics.input_count = df.count()

    df = deduplicate(df, ["restaurant_id"])
    metrics.duplicates_removed = metrics.input_count - df.count()

    df_clean = (
        df
        .withColumn("name", initcap(trim(col("name"))))
        .withColumn("city", initcap(trim(col("city"))))
        .withColumn("locality", initcap(trim(col("locality"))))
        .withColumn("cuisines_list", concat_ws(", ", col("cuisines")))
        .withColumn("num_cuisines", size(col("cuisines")))
        .withColumn("onboarded_date", to_timestamp(col("onboarded_date")))
        .withColumn(
            "price_tier",
            when(col("avg_cost_for_two") <= 300, "Budget")
            .when(col("avg_cost_for_two") <= 700, "Mid-Range")
            .when(col("avg_cost_for_two") <= 1500, "Premium")
            .otherwise("Luxury"),
        )
        .withColumn(
            "rating_tier",
            when(col("rating") >= 4.5, "Excellent")
            .when(col("rating") >= 4.0, "Very Good")
            .when(col("rating") >= 3.5, "Good")
            .when(col("rating") >= 3.0, "Average")
            .otherwise("Below Average"),
        )
        .withColumn("rating", spark_round(col("rating"), 1))
        .withColumn("is_delivering_now", coalesce(col("is_delivering_now"), lit(False)))
        .withColumn("has_online_delivery", coalesce(col("has_online_delivery"), lit(False)))
        .withColumn("has_table_booking", coalesce(col("has_table_booking"), lit(False)))
        .withColumn("is_premium_partner", coalesce(col("is_premium_partner"), lit(False)))
        .withColumn("_silver_loaded_at", current_timestamp())
        .drop("_ingested_at", "_source_system", "_source_file", "_row_hash", "_bronze_loaded_at")
    )

    valid_df = df_clean.filter(col("restaurant_id").isNotNull() & col("name").isNotNull())
    metrics.invalid_records_quarantined = df_clean.count() - valid_df.count()

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_FQN}.restaurants")
    metrics.output_count = valid_df.count()
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver — Orders

# COMMAND ----------

def transform_orders() -> DataQualityMetrics:
    """Cleanse and transform order data."""
    metrics = DataQualityMetrics("silver_orders")

    df = spark.table(f"{BRONZE_FQN}.orders")
    metrics.input_count = df.count()

    df = deduplicate(df, ["order_id"])
    metrics.duplicates_removed = metrics.input_count - df.count()

    df_clean = (
        df
        .withColumn("order_placed_at", to_timestamp(col("order_placed_at")))
        .withColumn("order_date", to_date(col("order_placed_at")))
        .withColumn("order_year", year(col("order_placed_at")))
        .withColumn("order_month", month(col("order_placed_at")))
        .withColumn("order_day_of_week", dayofweek(col("order_placed_at")))
        .withColumn("order_hour", hour(col("order_placed_at")))
        .withColumn("order_day_name", date_format(col("order_placed_at"), "EEEE"))
        .withColumn(
            "order_time_slot",
            when(col("order_hour").between(6, 10), "Breakfast")
            .when(col("order_hour").between(11, 14), "Lunch")
            .when(col("order_hour").between(15, 17), "Snacks")
            .when(col("order_hour").between(18, 22), "Dinner")
            .otherwise("Late Night"),
        )
        .withColumn("order_status", upper(trim(col("order_status"))))
        .withColumn("payment_status", upper(trim(col("payment_status"))))
        .withColumn("is_delivered", when(col("order_status") == "DELIVERED", True).otherwise(False))
        .withColumn("is_cancelled", when(col("order_status").isin("CANCELLED", "REFUNDED"), True).otherwise(False))
        .withColumn("has_discount", when(col("discount_pct") > 0, True).otherwise(False))
        .withColumn("total_amount", when(col("total_amount") < 0, lit(0.0)).otherwise(col("total_amount")))
        .withColumn("subtotal", spark_round(col("subtotal"), 2))
        .withColumn("total_amount", spark_round(col("total_amount"), 2))
        .withColumn("discount_amount", spark_round(col("discount_amount"), 2))
        .withColumn("tax_amount", spark_round(col("tax_amount"), 2))
        .withColumn("delivery_fee", spark_round(col("delivery_fee"), 2))
        .withColumn("_silver_loaded_at", current_timestamp())
        .drop("_ingested_at", "_source_system", "_source_file", "_row_hash", "_bronze_loaded_at")
    )

    # Referential integrity — only keep orders with valid customer & restaurant IDs
    valid_customers = spark.table(f"{SILVER_FQN}.customers").select("customer_id")
    valid_restaurants = spark.table(f"{SILVER_FQN}.restaurants").select("restaurant_id")

    valid_df = (
        df_clean
        .join(valid_customers, "customer_id", "inner")
        .join(valid_restaurants, "restaurant_id", "inner")
    )
    metrics.invalid_records_quarantined = df_clean.count() - valid_df.count()

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_FQN}.orders")
    metrics.output_count = valid_df.count()
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver — Deliveries

# COMMAND ----------

def transform_deliveries() -> DataQualityMetrics:
    """Cleanse and transform delivery data."""
    metrics = DataQualityMetrics("silver_deliveries")

    df = spark.table(f"{BRONZE_FQN}.deliveries")
    metrics.input_count = df.count()

    df = deduplicate(df, ["delivery_id"])
    metrics.duplicates_removed = metrics.input_count - df.count()

    df_clean = (
        df
        .withColumn("pickup_timestamp", to_timestamp(col("pickup_timestamp")))
        .withColumn("delivered_timestamp", to_timestamp(col("delivered_timestamp")))
        .withColumn("delivery_status", upper(trim(col("delivery_status"))))
        .withColumn("vehicle_type", initcap(trim(col("vehicle_type"))))
        .withColumn(
            "delivery_sla_status",
            when(col("delivery_time_mins") <= 30, "Within SLA")
            .when(col("delivery_time_mins") <= 45, "Near SLA")
            .otherwise("SLA Breached"),
        )
        .withColumn(
            "distance_bucket",
            when(col("delivery_distance_km") <= 3, "Short (0-3 km)")
            .when(col("delivery_distance_km") <= 7, "Medium (3-7 km)")
            .when(col("delivery_distance_km") <= 12, "Long (7-12 km)")
            .otherwise("Very Long (12+ km)"),
        )
        .withColumn("delivery_rating", when(col("delivery_rating").between(1, 5), col("delivery_rating")).otherwise(lit(None)))
        .withColumn("delivery_distance_km", spark_round(col("delivery_distance_km"), 2))
        .withColumn("_silver_loaded_at", current_timestamp())
        .drop("_ingested_at", "_source_system", "_source_file", "_row_hash", "_bronze_loaded_at")
    )

    # Referential integrity check against orders
    valid_orders = spark.table(f"{SILVER_FQN}.orders").select("order_id")
    valid_df = df_clean.join(valid_orders, "order_id", "inner")
    metrics.invalid_records_quarantined = df_clean.count() - valid_df.count()

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_FQN}.deliveries")
    metrics.output_count = valid_df.count()
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver — Reviews

# COMMAND ----------

def transform_reviews() -> DataQualityMetrics:
    """Cleanse and transform review data."""
    metrics = DataQualityMetrics("silver_reviews")

    df = spark.table(f"{BRONZE_FQN}.reviews")
    metrics.input_count = df.count()

    df = deduplicate(df, ["review_id"])
    metrics.duplicates_removed = metrics.input_count - df.count()

    df_clean = (
        df
        .withColumn("review_timestamp", to_timestamp(col("review_timestamp")))
        .withColumn("review_date", to_date(col("review_timestamp")))
        .withColumn("rating", when(col("rating").between(1, 5), col("rating")).otherwise(lit(None)))
        .withColumn("review_text", trim(col("review_text")))
        .withColumn("review_text_length", when(col("review_text").isNotNull(), expr("length(review_text)")).otherwise(lit(0)))
        .withColumn("sentiment_label", initcap(trim(col("sentiment_label"))))
        .withColumn("_silver_loaded_at", current_timestamp())
        .drop("_ingested_at", "_source_system", "_source_file", "_row_hash", "_bronze_loaded_at")
    )

    valid_df = df_clean.filter(col("review_id").isNotNull() & col("rating").isNotNull())
    metrics.invalid_records_quarantined = df_clean.count() - valid_df.count()

    valid_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{SILVER_FQN}.reviews")
    metrics.output_count = valid_df.count()
    return metrics

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Execute Silver Transformation Pipeline

# COMMAND ----------

import json

transform_functions = [
    transform_customers,
    transform_restaurants,
    transform_orders,
    transform_deliveries,
    transform_reviews,
]

all_metrics = []
for func in transform_functions:
    print(f"\n{'='*50}")
    print(f"Processing: {func.__name__}")
    print(f"{'='*50}")
    metrics = func()
    all_metrics.append(metrics.summary())
    print(json.dumps(metrics.summary(), indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Silver Layer Validation Summary

# COMMAND ----------

print("\n" + "=" * 70)
print(f"SILVER LAYER — TRANSFORMATION SUMMARY ({SILVER_FQN})")
print("=" * 70)

for m in all_metrics:
    status = "PASS" if m["pass_rate_pct"] >= 95 else "WARN" if m["pass_rate_pct"] >= 80 else "FAIL"
    print(f"  [{status}] {m['table']}: {m['input_rows']:,} → {m['output_rows']:,} "
          f"(dedup: {m['duplicates_removed']:,}, quarantined: {m['quarantined']:,}, "
          f"pass: {m['pass_rate_pct']}%)")

print("=" * 70)
print(f"🟢 Silver transformation pipeline completed successfully.")
print(f"   All tables created under: {SILVER_FQN}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Audit Log Entry

# COMMAND ----------

from datetime import date, datetime
from pyspark.sql import Row

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _workflow_name = _ctx.jobName().getOrElse("manual_run")
except Exception:
    _workflow_name = "manual_run"

_load_date = date.today()
_run_dt = datetime.utcnow()

audit_rows = [
    Row(
        run_datetime=_run_dt,
        workflow_name=_workflow_name,
        task_name="silver_transformation",
        layer="silver",
        table_name=m["table"],
        total_records=int(m["output_rows"]),
        load_date=_load_date,
        status="SUCCESS" if m["pass_rate_pct"] >= 80 else "FAILED",
        error_message=None if m["pass_rate_pct"] >= 80 else f"Low pass rate: {m['pass_rate_pct']}%",
    )
    for m in all_metrics
]

(
    spark.createDataFrame(audit_rows)
    .write.format("delta")
    .mode("append")
    .saveAsTable(f"{CATALOG}.audit.pipeline_audit_log")
)

print(f"✓ Audit log: {len(audit_rows)} entries → {CATALOG}.audit.pipeline_audit_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Verify Isolation — List Only Our Objects

# COMMAND ----------

print(f"Tables in {SILVER_FQN}:")
spark.sql(f"SHOW TABLES IN {SILVER_FQN}").show(truncate=False)
