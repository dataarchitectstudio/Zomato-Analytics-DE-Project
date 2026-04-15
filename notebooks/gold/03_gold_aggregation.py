# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Gold Layer (Business Aggregations)
# MAGIC
# MAGIC **Purpose**: Build business-level aggregated tables and KPI datasets from the Silver
# MAGIC layer. These Gold tables power dashboards, reports, and ML feature stores.
# MAGIC
# MAGIC **Unity Catalog**: `zomato_analytics.gold.*`
# MAGIC
# MAGIC **Input**: `zomato_analytics.silver.*`
# MAGIC **Output**: `zomato_analytics.gold.*`
# MAGIC
# MAGIC **Gold Tables**:
# MAGIC 1. `dim_customers` — Customer dimension with lifetime metrics
# MAGIC 2. `dim_restaurants` — Restaurant dimension with performance scores
# MAGIC 3. `fact_orders` — Order fact table (denormalized)
# MAGIC 4. `agg_daily_city_metrics` — City-level daily KPIs
# MAGIC 5. `agg_restaurant_performance` — Restaurant performance scorecard
# MAGIC 6. `agg_customer_cohorts` — Monthly customer cohort analysis
# MAGIC 7. `agg_delivery_sla_report` — Delivery SLA compliance report
# MAGIC 8. `agg_revenue_summary` — Revenue & GMV summary

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Imports & Configuration

# COMMAND ----------

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, lit, count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, countDistinct, round as spark_round,
    current_timestamp, datediff, to_date, concat_ws, collect_set,
    first, last, percent_rank, dense_rank, ntile, lag,
    year, month, quarter, date_format, expr, coalesce,
    row_number, stddev, percentile_approx,
)
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")

ENV = dbutils.widgets.get("env")
CATALOG = dbutils.widgets.get("catalog_name")

# Schema names within our dedicated catalog
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# Fully qualified namespaces
SILVER_FQN = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD_FQN = f"{CATALOG}.{GOLD_SCHEMA}"

print(f"Environment   : {ENV}")
print(f"Catalog       : {CATALOG}")
print(f"Silver source : {SILVER_FQN}")
print(f"Gold target   : {GOLD_FQN}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Gold Schema (Idempotent — No Impact on Existing Objects)

# COMMAND ----------

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_FQN}")
spark.sql(f"COMMENT ON SCHEMA {GOLD_FQN} IS 'Gold layer — business aggregations, dimensions, and fact tables'")

print(f"✓ Schema '{GOLD_FQN}' ready")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load Silver Tables

# COMMAND ----------

silver_customers = spark.table(f"{SILVER_FQN}.customers")
silver_restaurants = spark.table(f"{SILVER_FQN}.restaurants")
silver_orders = spark.table(f"{SILVER_FQN}.orders")
silver_deliveries = spark.table(f"{SILVER_FQN}.deliveries")
silver_reviews = spark.table(f"{SILVER_FQN}.reviews")

print(f"Silver customers   : {silver_customers.count():,}")
print(f"Silver restaurants  : {silver_restaurants.count():,}")
print(f"Silver orders       : {silver_orders.count():,}")
print(f"Silver deliveries   : {silver_deliveries.count():,}")
print(f"Silver reviews      : {silver_reviews.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_customers — Customer Dimension

# COMMAND ----------

def build_dim_customers() -> int:
    """
    Build customer dimension with lifetime order metrics, segmentation,
    and recency-frequency-monetary (RFM) scoring.
    """
    order_metrics = (
        silver_orders
        .groupBy("customer_id")
        .agg(
            count("order_id").alias("total_orders"),
            spark_sum(when(col("is_delivered"), 1).otherwise(0)).alias("delivered_orders"),
            spark_sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_orders"),
            spark_sum("total_amount").alias("lifetime_revenue"),
            avg("total_amount").alias("avg_order_value"),
            spark_min("order_placed_at").alias("first_order_at"),
            spark_max("order_placed_at").alias("last_order_at"),
            countDistinct("restaurant_id").alias("unique_restaurants"),
            countDistinct("order_date").alias("distinct_order_days"),
            spark_sum("discount_amount").alias("total_discount_availed"),
        )
    )

    review_metrics = (
        silver_reviews
        .groupBy("customer_id")
        .agg(
            count("review_id").alias("total_reviews"),
            avg("rating").alias("avg_rating_given"),
        )
    )

    dim = (
        silver_customers
        .join(order_metrics, "customer_id", "left")
        .join(review_metrics, "customer_id", "left")
        .withColumn("total_orders", coalesce(col("total_orders"), lit(0)))
        .withColumn("lifetime_revenue", coalesce(col("lifetime_revenue"), lit(0.0)))
        .withColumn("avg_order_value", spark_round(coalesce(col("avg_order_value"), lit(0.0)), 2))
        .withColumn("lifetime_revenue", spark_round(col("lifetime_revenue"), 2))
        .withColumn("total_discount_availed", spark_round(coalesce(col("total_discount_availed"), lit(0.0)), 2))
        .withColumn("days_since_last_order",
                     when(col("last_order_at").isNotNull(),
                          datediff(current_timestamp(), col("last_order_at")))
                     .otherwise(lit(None)))
        .withColumn("cancel_rate_pct",
                     spark_round(
                         coalesce(col("cancelled_orders"), lit(0)) / when(col("total_orders") > 0, col("total_orders")).otherwise(lit(1)) * 100,
                         2,
                     ))
    )

    # Compute RFM quintiles
    dim = (
        dim
        .withColumn("recency_score", ntile(5).over(Window.orderBy(col("days_since_last_order").asc_nulls_last())))
        .withColumn("frequency_score", ntile(5).over(Window.orderBy(col("total_orders").desc())))
        .withColumn("monetary_score", ntile(5).over(Window.orderBy(col("lifetime_revenue").desc())))
        .withColumn("rfm_score", col("recency_score") + col("frequency_score") + col("monetary_score"))
        .withColumn(
            "rfm_segment",
            when(col("rfm_score") >= 13, "Champions")
            .when(col("rfm_score") >= 10, "Loyal Customers")
            .when(col("rfm_score") >= 7, "Potential Loyalists")
            .when(col("rfm_score") >= 4, "At Risk")
            .otherwise("Churned"),
        )
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.dim_customers")
    row_count = dim.count()
    print(f"  ✓ {GOLD_FQN}.dim_customers: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_restaurants — Restaurant Dimension

# COMMAND ----------

def build_dim_restaurants() -> int:
    """Build restaurant dimension with performance metrics."""
    order_metrics = (
        silver_orders
        .groupBy("restaurant_id")
        .agg(
            count("order_id").alias("total_orders_received"),
            spark_sum(when(col("is_delivered"), 1).otherwise(0)).alias("total_delivered"),
            spark_sum(when(col("is_cancelled"), 1).otherwise(0)).alias("total_cancelled"),
            spark_sum("total_amount").alias("total_gmv"),
            avg("total_amount").alias("avg_order_value"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("order_date").alias("active_days"),
        )
    )

    review_metrics = (
        silver_reviews
        .groupBy("restaurant_id")
        .agg(
            count("review_id").alias("review_count"),
            avg("rating").alias("avg_customer_rating"),
            spark_sum(when(col("sentiment_label") == "Positive", 1).otherwise(0)).alias("positive_reviews"),
            spark_sum(when(col("sentiment_label") == "Negative", 1).otherwise(0)).alias("negative_reviews"),
        )
    )

    delivery_metrics = (
        silver_deliveries
        .join(silver_orders.select("order_id", "restaurant_id"), "order_id", "inner")
        .groupBy("restaurant_id")
        .agg(
            avg("delivery_time_mins").alias("avg_delivery_time_mins"),
            spark_sum(when(col("delivery_sla_status") == "Within SLA", 1).otherwise(0)).alias("sla_met_count"),
            count("delivery_id").alias("total_delivery_count"),
        )
    )

    dim = (
        silver_restaurants
        .join(order_metrics, "restaurant_id", "left")
        .join(review_metrics, "restaurant_id", "left")
        .join(delivery_metrics, "restaurant_id", "left")
        .withColumn("total_gmv", spark_round(coalesce(col("total_gmv"), lit(0.0)), 2))
        .withColumn("avg_order_value", spark_round(coalesce(col("avg_order_value"), lit(0.0)), 2))
        .withColumn("avg_customer_rating", spark_round(coalesce(col("avg_customer_rating"), lit(0.0)), 1))
        .withColumn("avg_delivery_time_mins", spark_round(coalesce(col("avg_delivery_time_mins"), lit(0.0)), 1))
        .withColumn("fulfillment_rate_pct",
                     spark_round(
                         coalesce(col("total_delivered"), lit(0)) /
                         when(col("total_orders_received") > 0, col("total_orders_received")).otherwise(lit(1)) * 100,
                         2,
                     ))
        .withColumn("sla_compliance_pct",
                     spark_round(
                         coalesce(col("sla_met_count"), lit(0)) /
                         when(col("total_delivery_count") > 0, col("total_delivery_count")).otherwise(lit(1)) * 100,
                         2,
                     ))
        .withColumn(
            "restaurant_health_score",
            spark_round(
                (coalesce(col("avg_customer_rating"), lit(0)) / 5 * 30)
                + (coalesce(col("fulfillment_rate_pct"), lit(0)) / 100 * 30)
                + (coalesce(col("sla_compliance_pct"), lit(0)) / 100 * 20)
                + (when(col("positive_reviews") > 0,
                        col("positive_reviews") / (col("positive_reviews") + coalesce(col("negative_reviews"), lit(1))) * 20)
                   .otherwise(lit(10))),
                1,
            ),
        )
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.dim_restaurants")
    row_count = dim.count()
    print(f"  ✓ {GOLD_FQN}.dim_restaurants: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. fact_orders — Denormalized Order Fact

# COMMAND ----------

def build_fact_orders() -> int:
    """Build denormalized order fact table with customer & restaurant context."""
    fact = (
        silver_orders
        .join(
            silver_customers.select("customer_id", "full_name", "city", "customer_segment", "subscription_tier"),
            "customer_id", "left",
        )
        .join(
            silver_restaurants.select("restaurant_id", "name", "restaurant_type", "cuisines_list", "price_tier", "rating_tier"),
            "restaurant_id", "left",
        )
        .join(
            silver_deliveries.select("order_id", "delivery_time_mins", "delivery_sla_status", "delivery_distance_km", "delivery_rating"),
            "order_id", "left",
        )
        .withColumnRenamed("name", "restaurant_name")
        .withColumnRenamed("city", "customer_city")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    fact.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.fact_orders")
    row_count = fact.count()
    print(f"  ✓ {GOLD_FQN}.fact_orders: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. agg_daily_city_metrics — City-Level Daily KPIs

# COMMAND ----------

def build_daily_city_metrics() -> int:
    """Aggregate daily order KPIs by city."""
    fact = spark.table(f"{GOLD_FQN}.fact_orders")

    agg = (
        fact
        .groupBy("customer_city", "order_date", "order_year", "order_month")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("restaurant_id").alias("active_restaurants"),
            spark_sum("total_amount").alias("total_gmv"),
            avg("total_amount").alias("avg_order_value"),
            spark_sum(when(col("is_delivered"), 1).otherwise(0)).alias("delivered_orders"),
            spark_sum(when(col("is_cancelled"), 1).otherwise(0)).alias("cancelled_orders"),
            avg("delivery_time_mins").alias("avg_delivery_time_mins"),
            spark_sum("discount_amount").alias("total_discounts"),
        )
        .withColumn("total_gmv", spark_round(col("total_gmv"), 2))
        .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
        .withColumn("avg_delivery_time_mins", spark_round(col("avg_delivery_time_mins"), 1))
        .withColumn("total_discounts", spark_round(col("total_discounts"), 2))
        .withColumn("cancel_rate_pct", spark_round(col("cancelled_orders") / col("total_orders") * 100, 2))
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.agg_daily_city_metrics")
    row_count = agg.count()
    print(f"  ✓ {GOLD_FQN}.agg_daily_city_metrics: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. agg_restaurant_performance — Restaurant Scorecard

# COMMAND ----------

def build_restaurant_performance() -> int:
    """Build restaurant performance ranking."""
    dim_rest = spark.table(f"{GOLD_FQN}.dim_restaurants")

    ranked = (
        dim_rest
        .select(
            "restaurant_id", "name", "city", "restaurant_type", "price_tier",
            "rating_tier", "total_orders_received", "total_gmv", "avg_order_value",
            "unique_customers", "avg_customer_rating", "avg_delivery_time_mins",
            "fulfillment_rate_pct", "sla_compliance_pct", "restaurant_health_score",
        )
        .withColumn("city_rank", dense_rank().over(Window.partitionBy("city").orderBy(col("restaurant_health_score").desc())))
        .withColumn("overall_rank", dense_rank().over(Window.orderBy(col("restaurant_health_score").desc())))
        .withColumn(
            "performance_tier",
            when(col("overall_rank") <= 50, "Top 50")
            .when(percent_rank().over(Window.orderBy(col("restaurant_health_score").desc())) <= 0.1, "Top 10%")
            .when(percent_rank().over(Window.orderBy(col("restaurant_health_score").desc())) <= 0.25, "Top 25%")
            .otherwise("Standard"),
        )
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    ranked.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.agg_restaurant_performance")
    row_count = ranked.count()
    print(f"  ✓ {GOLD_FQN}.agg_restaurant_performance: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. agg_customer_cohorts — Monthly Cohort Analysis

# COMMAND ----------

def build_customer_cohorts() -> int:
    """Build monthly customer cohort analysis for retention tracking."""
    fact = spark.table(f"{GOLD_FQN}.fact_orders")
    dim_cust = spark.table(f"{GOLD_FQN}.dim_customers")

    cohort_base = (
        dim_cust
        .select("customer_id", "signup_date")
        .withColumn("cohort_month", date_format(col("signup_date"), "yyyy-MM"))
    )

    cohort_orders = (
        fact
        .select("customer_id", "order_date", "total_amount")
        .withColumn("activity_month", date_format(col("order_date"), "yyyy-MM"))
        .join(cohort_base, "customer_id", "inner")
    )

    cohort_agg = (
        cohort_orders
        .groupBy("cohort_month", "activity_month")
        .agg(
            countDistinct("customer_id").alias("active_customers"),
            count("*").alias("total_orders"),
            spark_sum("total_amount").alias("cohort_revenue"),
        )
        .withColumn("cohort_revenue", spark_round(col("cohort_revenue"), 2))
        .withColumn("_gold_loaded_at", current_timestamp())
        .orderBy("cohort_month", "activity_month")
    )

    cohort_agg.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.agg_customer_cohorts")
    row_count = cohort_agg.count()
    print(f"  ✓ {GOLD_FQN}.agg_customer_cohorts: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. agg_delivery_sla_report — SLA Compliance Report

# COMMAND ----------

def build_delivery_sla_report() -> int:
    """Build delivery SLA compliance report by city and vehicle type."""
    delivery_with_context = (
        silver_deliveries
        .join(silver_orders.select("order_id", "restaurant_id"), "order_id", "inner")
        .join(silver_restaurants.select("restaurant_id", "city"), "restaurant_id", "inner")
    )

    sla_report = (
        delivery_with_context
        .groupBy("city", "vehicle_type", "delivery_sla_status")
        .agg(
            count("delivery_id").alias("delivery_count"),
            avg("delivery_time_mins").alias("avg_delivery_mins"),
            avg("delivery_distance_km").alias("avg_distance_km"),
            avg("delivery_rating").alias("avg_delivery_rating"),
        )
        .withColumn("avg_delivery_mins", spark_round(col("avg_delivery_mins"), 1))
        .withColumn("avg_distance_km", spark_round(col("avg_distance_km"), 2))
        .withColumn("avg_delivery_rating", spark_round(col("avg_delivery_rating"), 2))
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    sla_report.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.agg_delivery_sla_report")
    row_count = sla_report.count()
    print(f"  ✓ {GOLD_FQN}.agg_delivery_sla_report: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. agg_revenue_summary — Revenue & GMV Summary

# COMMAND ----------

def build_revenue_summary() -> int:
    """Build monthly revenue summary with growth metrics."""
    fact = spark.table(f"{GOLD_FQN}.fact_orders")

    monthly = (
        fact
        .filter(col("is_delivered"))
        .groupBy("order_year", "order_month")
        .agg(
            count("order_id").alias("total_orders"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("restaurant_id").alias("active_restaurants"),
            spark_sum("total_amount").alias("total_gmv"),
            spark_sum("subtotal").alias("total_subtotal"),
            spark_sum("discount_amount").alias("total_discounts"),
            spark_sum("delivery_fee").alias("total_delivery_fees"),
            spark_sum("tax_amount").alias("total_tax_collected"),
            avg("total_amount").alias("avg_order_value"),
            percentile_approx("total_amount", 0.5).alias("median_order_value"),
        )
        .withColumn("total_gmv", spark_round(col("total_gmv"), 2))
        .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
        .withColumn("net_revenue", spark_round(col("total_subtotal") - col("total_discounts"), 2))
        .withColumn("discount_penetration_pct", spark_round(col("total_discounts") / col("total_gmv") * 100, 2))
        .orderBy("order_year", "order_month")
    )

    # Add month-over-month growth
    w = Window.orderBy("order_year", "order_month")
    monthly_with_growth = (
        monthly
        .withColumn("prev_month_gmv", lag("total_gmv").over(w))
        .withColumn("gmv_growth_pct",
                     spark_round(
                         (col("total_gmv") - coalesce(col("prev_month_gmv"), col("total_gmv")))
                         / when(col("prev_month_gmv") > 0, col("prev_month_gmv")).otherwise(lit(1)) * 100,
                         2,
                     ))
        .drop("prev_month_gmv")
        .withColumn("_gold_loaded_at", current_timestamp())
    )

    monthly_with_growth.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{GOLD_FQN}.agg_revenue_summary")
    row_count = monthly_with_growth.count()
    print(f"  ✓ {GOLD_FQN}.agg_revenue_summary: {row_count:,} rows")
    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Execute Gold Layer Pipeline

# COMMAND ----------

gold_builders = [
    ("dim_customers", build_dim_customers),
    ("dim_restaurants", build_dim_restaurants),
    ("fact_orders", build_fact_orders),
    ("agg_daily_city_metrics", build_daily_city_metrics),
    ("agg_restaurant_performance", build_restaurant_performance),
    ("agg_customer_cohorts", build_customer_cohorts),
    ("agg_delivery_sla_report", build_delivery_sla_report),
    ("agg_revenue_summary", build_revenue_summary),
]

results = []
for name, builder in gold_builders:
    print(f"\n{'='*50}")
    print(f"Building: {GOLD_FQN}.{name}")
    print(f"{'='*50}")
    try:
        row_count = builder()
        results.append({"table": name, "status": "SUCCESS", "rows": row_count})
    except Exception as e:
        results.append({"table": name, "status": "FAILED", "error": str(e)})
        print(f"  ✗ FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Gold Layer Summary

# COMMAND ----------

import json

print("\n" + "=" * 70)
print(f"GOLD LAYER — AGGREGATION SUMMARY ({GOLD_FQN})")
print("=" * 70)

for r in results:
    status_icon = "✓" if r["status"] == "SUCCESS" else "✗"
    row_info = f"{r.get('rows', 'N/A'):,} rows" if r["status"] == "SUCCESS" else r.get("error", "")
    print(f"  [{status_icon}] {GOLD_FQN}.{r['table']}: {row_info}")

success_count = sum(1 for r in results if r["status"] == "SUCCESS")
print(f"\n{success_count}/{len(results)} tables built successfully.")
print("=" * 70)
print(f"🟢 Gold aggregation pipeline completed.")
print(f"   All tables created under: {GOLD_FQN}.*")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Verify Isolation — List Only Our Objects

# COMMAND ----------

print(f"\nAll schemas in catalog '{CATALOG}':")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show(truncate=False)

print(f"\nTables in {GOLD_FQN}:")
spark.sql(f"SHOW TABLES IN {GOLD_FQN}").show(truncate=False)
