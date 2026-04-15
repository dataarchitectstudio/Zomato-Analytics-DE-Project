# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Executive Dashboard
# MAGIC
# MAGIC **Purpose**: Interactive analytics dashboard powered by Gold layer tables.
# MAGIC Provides executive-level visibility into platform KPIs, operational health,
# MAGIC and business growth metrics.
# MAGIC
# MAGIC **Unity Catalog**: `zomato_analytics.gold.*`
# MAGIC
# MAGIC **Sections**:
# MAGIC 1. Platform Overview KPIs
# MAGIC 2. Revenue & GMV Trends
# MAGIC 3. City-Level Performance Heatmap
# MAGIC 4. Restaurant Performance Leaderboard
# MAGIC 5. Customer Segmentation & Cohorts
# MAGIC 6. Delivery SLA Monitoring
# MAGIC 7. Cuisine & Category Insights
# MAGIC 8. Operational Alerts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup & Configuration

# COMMAND ----------

import json
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, avg, min as spark_min,
    max as spark_max, countDistinct, round as spark_round,
    when, lit, date_format, desc, asc, expr, datediff,
    current_date, current_timestamp, collect_list, concat_ws,
    percent_rank, coalesce, explode, split, trim as spark_trim,
)
from pyspark.sql.window import Window

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
dbutils.widgets.text("report_date", "", "Report date (YYYY-MM-DD, blank = today)")

CATALOG = dbutils.widgets.get("catalog_name")
REPORT_DATE = dbutils.widgets.get("report_date") or str(datetime.utcnow().date())

# All Gold tables live under this namespace
GOLD_FQN = f"{CATALOG}.gold"

# Set catalog context
spark.sql(f"USE CATALOG {CATALOG}")

print(f"Catalog     : {CATALOG}")
print(f"Gold source : {GOLD_FQN}")
print(f"Report Date : {REPORT_DATE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Gold Tables

# COMMAND ----------

fact_orders = spark.table(f"{GOLD_FQN}.fact_orders")
dim_customers = spark.table(f"{GOLD_FQN}.dim_customers")
dim_restaurants = spark.table(f"{GOLD_FQN}.dim_restaurants")
agg_daily_city = spark.table(f"{GOLD_FQN}.agg_daily_city_metrics")
agg_rest_perf = spark.table(f"{GOLD_FQN}.agg_restaurant_performance")
agg_cohorts = spark.table(f"{GOLD_FQN}.agg_customer_cohorts")
agg_delivery_sla = spark.table(f"{GOLD_FQN}.agg_delivery_sla_report")
agg_revenue = spark.table(f"{GOLD_FQN}.agg_revenue_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 3. Platform Overview — Key Performance Indicators

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3.1 Headline KPIs

# COMMAND ----------

total_orders = fact_orders.count()
total_gmv = fact_orders.agg(spark_sum("total_amount")).collect()[0][0] or 0
total_customers = dim_customers.count()
total_restaurants = dim_restaurants.count()
avg_order_val = fact_orders.agg(avg("total_amount")).collect()[0][0] or 0
total_delivered = fact_orders.filter(col("is_delivered")).count()
total_cancelled = fact_orders.filter(col("is_cancelled")).count()

delivery_rate = round(total_delivered / max(total_orders, 1) * 100, 2)
cancel_rate = round(total_cancelled / max(total_orders, 1) * 100, 2)

print("=" * 70)
print("       ZOMATO ANALYTICS — PLATFORM OVERVIEW")
print(f"       Catalog: {CATALOG}")
print("=" * 70)
print(f"  Total Orders          : {total_orders:>15,}")
print(f"  Total GMV (INR)       : ₹{total_gmv:>14,.2f}")
print(f"  Total Customers       : {total_customers:>15,}")
print(f"  Total Restaurants     : {total_restaurants:>15,}")
print(f"  Avg Order Value (INR) : ₹{avg_order_val:>14,.2f}")
print(f"  Delivery Rate         : {delivery_rate:>14}%")
print(f"  Cancellation Rate     : {cancel_rate:>14}%")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 4. Revenue & GMV Trends

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.1 Monthly Revenue Trend

# COMMAND ----------

revenue_trend = (
    agg_revenue
    .orderBy("order_year", "order_month")
    .select(
        "order_year", "order_month", "total_orders", "total_gmv",
        "avg_order_value", "unique_customers", "net_revenue",
        "total_discounts", "discount_penetration_pct",
    )
)

display(revenue_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.2 Revenue by Payment Method

# COMMAND ----------

payment_breakdown = (
    fact_orders
    .filter(col("is_delivered"))
    .groupBy("payment_method")
    .agg(
        count("order_id").alias("order_count"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_value"),
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("avg_value", spark_round(col("avg_value"), 2))
    .withColumn("share_pct",
                spark_round(col("total_revenue") / lit(total_gmv) * 100, 2))
    .orderBy(desc("total_revenue"))
)

display(payment_breakdown)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 4.3 Revenue by Platform

# COMMAND ----------

platform_breakdown = (
    fact_orders
    .filter(col("is_delivered"))
    .groupBy("platform")
    .agg(
        count("order_id").alias("order_count"),
        spark_sum("total_amount").alias("total_revenue"),
        countDistinct("customer_id").alias("unique_users"),
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .orderBy(desc("total_revenue"))
)

display(platform_breakdown)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 5. City-Level Performance

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.1 Top Cities by GMV

# COMMAND ----------

city_performance = (
    agg_daily_city
    .groupBy("customer_city")
    .agg(
        spark_sum("total_orders").alias("total_orders"),
        spark_sum("total_gmv").alias("total_gmv"),
        spark_sum("unique_customers").alias("total_unique_customers"),
        avg("avg_order_value").alias("avg_aov"),
        avg("avg_delivery_time_mins").alias("avg_delivery_time"),
        avg("cancel_rate_pct").alias("avg_cancel_rate"),
    )
    .withColumn("total_gmv", spark_round(col("total_gmv"), 2))
    .withColumn("avg_aov", spark_round(col("avg_aov"), 2))
    .withColumn("avg_delivery_time", spark_round(col("avg_delivery_time"), 1))
    .withColumn("avg_cancel_rate", spark_round(col("avg_cancel_rate"), 2))
    .orderBy(desc("total_gmv"))
)

display(city_performance)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5.2 City-Level Order Volume Trend

# COMMAND ----------

city_trend = (
    agg_daily_city
    .groupBy("customer_city", "order_year", "order_month")
    .agg(
        spark_sum("total_orders").alias("monthly_orders"),
        spark_sum("total_gmv").alias("monthly_gmv"),
    )
    .withColumn("monthly_gmv", spark_round(col("monthly_gmv"), 2))
    .orderBy("customer_city", "order_year", "order_month")
)

display(city_trend)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 6. Restaurant Performance Leaderboard

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.1 Top 20 Restaurants by Health Score

# COMMAND ----------

top_restaurants = (
    agg_rest_perf
    .filter(col("total_orders_received") >= 50)
    .orderBy(desc("restaurant_health_score"))
    .select(
        "overall_rank", "name", "city", "restaurant_type",
        "total_orders_received", "total_gmv", "avg_customer_rating",
        "avg_delivery_time_mins", "fulfillment_rate_pct",
        "sla_compliance_pct", "restaurant_health_score", "performance_tier",
    )
    .limit(20)
)

display(top_restaurants)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6.2 Restaurant Type Distribution

# COMMAND ----------

rest_type_dist = (
    dim_restaurants
    .groupBy("restaurant_type", "price_tier")
    .agg(
        count("restaurant_id").alias("count"),
        avg("total_gmv").alias("avg_gmv"),
        avg("avg_customer_rating").alias("avg_rating"),
    )
    .withColumn("avg_gmv", spark_round(col("avg_gmv"), 2))
    .withColumn("avg_rating", spark_round(col("avg_rating"), 2))
    .orderBy(desc("count"))
)

display(rest_type_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 7. Customer Segmentation & Cohorts

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.1 RFM Segment Distribution

# COMMAND ----------

rfm_distribution = (
    dim_customers
    .groupBy("rfm_segment")
    .agg(
        count("customer_id").alias("customer_count"),
        avg("lifetime_revenue").alias("avg_ltv"),
        avg("total_orders").alias("avg_orders"),
        avg("days_since_last_order").alias("avg_recency_days"),
    )
    .withColumn("avg_ltv", spark_round(col("avg_ltv"), 2))
    .withColumn("avg_orders", spark_round(col("avg_orders"), 1))
    .withColumn("avg_recency_days", spark_round(col("avg_recency_days"), 0))
    .withColumn("pct_of_total", spark_round(col("customer_count") / lit(total_customers) * 100, 2))
    .orderBy(desc("avg_ltv"))
)

display(rfm_distribution)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.2 Customer Segment by Subscription Tier

# COMMAND ----------

segment_tier = (
    dim_customers
    .groupBy("customer_segment", "subscription_tier")
    .agg(
        count("customer_id").alias("count"),
        avg("lifetime_revenue").alias("avg_revenue"),
    )
    .withColumn("avg_revenue", spark_round(col("avg_revenue"), 2))
    .orderBy("customer_segment", "subscription_tier")
)

display(segment_tier)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 7.3 Monthly Cohort Retention

# COMMAND ----------

display(
    agg_cohorts
    .orderBy("cohort_month", "activity_month")
    .select("cohort_month", "activity_month", "active_customers", "total_orders", "cohort_revenue")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 8. Delivery SLA Monitoring

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.1 Overall SLA Compliance

# COMMAND ----------

sla_overview = (
    agg_delivery_sla
    .groupBy("delivery_sla_status")
    .agg(
        spark_sum("delivery_count").alias("total_deliveries"),
        avg("avg_delivery_mins").alias("avg_time_mins"),
        avg("avg_delivery_rating").alias("avg_rating"),
    )
    .withColumn("avg_time_mins", spark_round(col("avg_time_mins"), 1))
    .withColumn("avg_rating", spark_round(col("avg_rating"), 2))
)

display(sla_overview)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.2 SLA by City

# COMMAND ----------

sla_by_city = (
    agg_delivery_sla
    .groupBy("city")
    .agg(
        spark_sum("delivery_count").alias("total_deliveries"),
        avg("avg_delivery_mins").alias("avg_delivery_mins"),
        avg("avg_distance_km").alias("avg_distance"),
        avg("avg_delivery_rating").alias("avg_rating"),
    )
    .withColumn("avg_delivery_mins", spark_round(col("avg_delivery_mins"), 1))
    .withColumn("avg_distance", spark_round(col("avg_distance"), 2))
    .withColumn("avg_rating", spark_round(col("avg_rating"), 2))
    .orderBy(desc("total_deliveries"))
)

display(sla_by_city)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 8.3 SLA by Vehicle Type

# COMMAND ----------

sla_by_vehicle = (
    agg_delivery_sla
    .groupBy("vehicle_type")
    .agg(
        spark_sum("delivery_count").alias("total_deliveries"),
        avg("avg_delivery_mins").alias("avg_time"),
        avg("avg_distance_km").alias("avg_distance"),
    )
    .withColumn("avg_time", spark_round(col("avg_time"), 1))
    .withColumn("avg_distance", spark_round(col("avg_distance"), 2))
    .orderBy("vehicle_type")
)

display(sla_by_vehicle)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 9. Cuisine & Category Insights

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.1 Top Cuisines by Revenue

# COMMAND ----------

cuisine_revenue = (
    fact_orders
    .filter(col("is_delivered"))
    .withColumn("cuisine", explode(split(col("cuisines_list"), ",")))
    .withColumn("cuisine", spark_trim(col("cuisine")))
    .groupBy("cuisine")
    .agg(
        count("order_id").alias("order_count"),
        spark_sum("total_amount").alias("total_revenue"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("total_revenue", spark_round(col("total_revenue"), 2))
    .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
    .orderBy(desc("total_revenue"))
    .limit(20)
)

display(cuisine_revenue)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9.2 Order Time Slot Analysis

# COMMAND ----------

time_slot_analysis = (
    fact_orders
    .groupBy("order_time_slot")
    .agg(
        count("order_id").alias("order_count"),
        spark_sum("total_amount").alias("total_gmv"),
        avg("total_amount").alias("avg_order_value"),
        countDistinct("customer_id").alias("unique_customers"),
    )
    .withColumn("total_gmv", spark_round(col("total_gmv"), 2))
    .withColumn("avg_order_value", spark_round(col("avg_order_value"), 2))
    .orderBy(desc("order_count"))
)

display(time_slot_analysis)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 10. Operational Alerts & Anomalies

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.1 Restaurants with High Cancel Rates (> 15%)

# COMMAND ----------

high_cancel_restaurants = (
    dim_restaurants
    .filter(col("total_orders_received") >= 20)
    .withColumn("cancel_rate",
                spark_round(
                    coalesce(col("total_cancelled"), lit(0)) / col("total_orders_received") * 100,
                    2,
                ))
    .filter(col("cancel_rate") > 15)
    .select("restaurant_id", "name", "city", "total_orders_received", "cancel_rate", "restaurant_health_score")
    .orderBy(desc("cancel_rate"))
)

alert_count = high_cancel_restaurants.count()
print(f"⚠️  {alert_count} restaurants with cancel rate > 15%")
display(high_cancel_restaurants)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 10.2 Customers at Risk of Churning

# COMMAND ----------

churn_risk = (
    dim_customers
    .filter(col("rfm_segment").isin("At Risk", "Churned"))
    .filter(col("lifetime_revenue") > 1000)
    .select(
        "customer_id", "full_name", "city", "customer_segment",
        "rfm_segment", "total_orders", "lifetime_revenue",
        "days_since_last_order", "cancel_rate_pct",
    )
    .orderBy(desc("lifetime_revenue"))
    .limit(50)
)

churn_count = dim_customers.filter(col("rfm_segment").isin("At Risk", "Churned")).count()
print(f"⚠️  {churn_count} customers at risk of churning")
display(churn_risk)

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## 11. Dashboard Generated At

# COMMAND ----------

print(f"\n{'='*70}")
print(f"  Dashboard generated at : {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"  Report date            : {REPORT_DATE}")
print(f"  Data source            : {GOLD_FQN}")
print(f"  Catalog                : {CATALOG}")
print(f"{'='*70}")
print("🟢 Dashboard rendering complete.")
print(f"   All data sourced from: {GOLD_FQN}.* (isolated catalog)")
