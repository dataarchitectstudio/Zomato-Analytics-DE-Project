# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Table Creation (DDL)
# MAGIC
# MAGIC **Purpose**: Create the dedicated Unity Catalog, schemas, and all Delta tables
# MAGIC for the Medallion Architecture. This notebook is **idempotent** — safe to run
# MAGIC multiple times without affecting existing data.
# MAGIC
# MAGIC **Creates**:
# MAGIC - Catalog: `zomato_analytics`
# MAGIC - Schemas: `bronze`, `silver`, `gold`
# MAGIC - All Bronze, Silver, and Gold tables with full schema definitions
# MAGIC
# MAGIC **Run this ONCE** before the first pipeline execution, or after a fresh workspace setup.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
CATALOG = dbutils.widgets.get("catalog_name")

print(f"Catalog: {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Catalog & Schemas

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"""
    COMMENT ON CATALOG {CATALOG} IS
    'Zomato Analytics DE Project — Isolated catalog for medallion architecture. Safe to DROP without affecting other workspace objects.'
""")

for schema in ["bronze", "silver", "gold"]:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{schema}")

spark.sql(f"COMMENT ON SCHEMA {CATALOG}.bronze IS 'Raw ingested data with audit columns'")
spark.sql(f"COMMENT ON SCHEMA {CATALOG}.silver IS 'Cleansed, deduplicated, and conformed data'")
spark.sql(f"COMMENT ON SCHEMA {CATALOG}.gold IS 'Business aggregations, dimensions, and fact tables'")

print(f"✓ Catalog '{CATALOG}' and schemas created")
spark.sql(f"SHOW SCHEMAS IN {CATALOG}").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Bronze Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.customers (
    customer_id         STRING      NOT NULL,
    first_name          STRING,
    last_name           STRING,
    email               STRING,
    phone               STRING,
    city                STRING,
    locality            STRING,
    pincode             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    signup_date         STRING,
    subscription_tier   STRING,
    is_active           BOOLEAN,
    preferred_payment   STRING,
    preferred_cuisine   STRING,
    total_orders_lifetime INT,
    avg_order_value     DOUBLE,
    _ingested_at        STRING,
    _source_system      STRING,
    _bronze_loaded_at   TIMESTAMP,
    _source_file        STRING,
    _row_hash           STRING
)
USING DELTA
COMMENT 'Raw customer data from app backend'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.restaurants (
    restaurant_id       STRING      NOT NULL,
    name                STRING,
    city                STRING,
    locality            STRING,
    address             STRING,
    pincode             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    cuisines            ARRAY<STRING>,
    restaurant_type     STRING,
    avg_cost_for_two    INT,
    rating              FLOAT,
    total_reviews       INT,
    is_delivering_now   BOOLEAN,
    has_online_delivery BOOLEAN,
    has_table_booking   BOOLEAN,
    is_premium_partner  BOOLEAN,
    onboarded_date      STRING,
    owner_name          STRING,
    owner_phone         STRING,
    gst_number          STRING,
    _ingested_at        STRING,
    _source_system      STRING,
    _bronze_loaded_at   TIMESTAMP,
    _source_file        STRING,
    _row_hash           STRING
)
USING DELTA
COMMENT 'Raw restaurant onboarding data'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.orders (
    order_id            STRING      NOT NULL,
    customer_id         STRING,
    restaurant_id       STRING,
    order_placed_at     STRING,
    order_status        STRING,
    payment_method      STRING,
    payment_status      STRING,
    platform            STRING,
    subtotal            DOUBLE,
    discount_pct        DOUBLE,
    discount_amount     DOUBLE,
    coupon_code         STRING,
    delivery_fee        DOUBLE,
    tax_amount          DOUBLE,
    total_amount        DOUBLE,
    num_items           INT,
    special_instructions STRING,
    _ingested_at        STRING,
    _source_system      STRING,
    _bronze_loaded_at   TIMESTAMP,
    _source_file        STRING,
    _row_hash           STRING
)
USING DELTA
COMMENT 'Raw order transactions from order management service'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.deliveries (
    delivery_id         STRING      NOT NULL,
    order_id            STRING,
    delivery_partner_id STRING,
    delivery_partner_name STRING,
    partner_phone       STRING,
    vehicle_type        STRING,
    pickup_timestamp    STRING,
    delivered_timestamp STRING,
    delivery_time_mins  INT,
    delivery_status     STRING,
    delivery_distance_km DOUBLE,
    delivery_rating     INT,
    _ingested_at        STRING,
    _source_system      STRING,
    _bronze_loaded_at   TIMESTAMP,
    _source_file        STRING,
    _row_hash           STRING
)
USING DELTA
COMMENT 'Raw delivery tracking events from logistics service'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.bronze.reviews (
    review_id           STRING      NOT NULL,
    order_id            STRING,
    customer_id         STRING,
    restaurant_id       STRING,
    rating              INT,
    review_text         STRING,
    review_timestamp    STRING,
    sentiment_label     STRING,
    is_verified_order   BOOLEAN,
    upvotes             INT,
    has_photo           BOOLEAN,
    _ingested_at        STRING,
    _source_system      STRING,
    _bronze_loaded_at   TIMESTAMP,
    _source_file        STRING,
    _row_hash           STRING
)
USING DELTA
COMMENT 'Raw customer reviews and ratings'
""")

print("✓ All Bronze tables created")
spark.sql(f"SHOW TABLES IN {CATALOG}.bronze").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver Tables

# COMMAND ----------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.customers (
    customer_id         STRING      NOT NULL,
    first_name          STRING,
    last_name           STRING,
    full_name           STRING,
    email               STRING,
    phone               STRING,
    phone_cleaned       STRING,
    city                STRING,
    locality            STRING,
    pincode             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    signup_date         TIMESTAMP,
    signup_year         INT,
    signup_month        INT,
    subscription_tier   STRING,
    is_active           BOOLEAN,
    preferred_payment   STRING,
    preferred_cuisine   STRING,
    total_orders_lifetime INT,
    avg_order_value     DOUBLE,
    customer_segment    STRING,
    _silver_loaded_at   TIMESTAMP,
    _data_quality_score DOUBLE
)
USING DELTA
COMMENT 'Cleansed customer data with segmentation'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.restaurants (
    restaurant_id       STRING      NOT NULL,
    name                STRING,
    city                STRING,
    locality            STRING,
    address             STRING,
    pincode             STRING,
    latitude            DOUBLE,
    longitude           DOUBLE,
    cuisines            ARRAY<STRING>,
    cuisines_list       STRING,
    num_cuisines        INT,
    restaurant_type     STRING,
    avg_cost_for_two    INT,
    rating              FLOAT,
    total_reviews       INT,
    is_delivering_now   BOOLEAN,
    has_online_delivery BOOLEAN,
    has_table_booking   BOOLEAN,
    is_premium_partner  BOOLEAN,
    onboarded_date      TIMESTAMP,
    owner_name          STRING,
    owner_phone         STRING,
    gst_number          STRING,
    price_tier          STRING,
    rating_tier         STRING,
    _silver_loaded_at   TIMESTAMP
)
USING DELTA
COMMENT 'Cleansed restaurant data with price and rating tiers'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.orders (
    order_id            STRING      NOT NULL,
    customer_id         STRING,
    restaurant_id       STRING,
    order_placed_at     TIMESTAMP,
    order_date          DATE,
    order_year          INT,
    order_month         INT,
    order_day_of_week   INT,
    order_hour          INT,
    order_day_name      STRING,
    order_time_slot     STRING,
    order_status        STRING,
    payment_method      STRING,
    payment_status      STRING,
    platform            STRING,
    subtotal            DOUBLE,
    discount_pct        DOUBLE,
    discount_amount     DOUBLE,
    coupon_code         STRING,
    delivery_fee        DOUBLE,
    tax_amount          DOUBLE,
    total_amount        DOUBLE,
    num_items           INT,
    special_instructions STRING,
    is_delivered        BOOLEAN,
    is_cancelled        BOOLEAN,
    has_discount        BOOLEAN,
    _silver_loaded_at   TIMESTAMP
)
USING DELTA
COMMENT 'Cleansed order data with time slots and status flags'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.deliveries (
    delivery_id         STRING      NOT NULL,
    order_id            STRING,
    delivery_partner_id STRING,
    delivery_partner_name STRING,
    partner_phone       STRING,
    vehicle_type        STRING,
    pickup_timestamp    TIMESTAMP,
    delivered_timestamp TIMESTAMP,
    delivery_time_mins  INT,
    delivery_status     STRING,
    delivery_distance_km DOUBLE,
    delivery_rating     INT,
    delivery_sla_status STRING,
    distance_bucket     STRING,
    _silver_loaded_at   TIMESTAMP
)
USING DELTA
COMMENT 'Cleansed delivery data with SLA classification'
""")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.silver.reviews (
    review_id           STRING      NOT NULL,
    order_id            STRING,
    customer_id         STRING,
    restaurant_id       STRING,
    rating              INT,
    review_text         STRING,
    review_text_length  INT,
    review_timestamp    TIMESTAMP,
    review_date         DATE,
    sentiment_label     STRING,
    is_verified_order   BOOLEAN,
    upvotes             INT,
    has_photo           BOOLEAN,
    _silver_loaded_at   TIMESTAMP
)
USING DELTA
COMMENT 'Cleansed review data with text length and sentiment'
""")

print("✓ All Silver tables created")
spark.sql(f"SHOW TABLES IN {CATALOG}.silver").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold Tables

# COMMAND ----------

# Gold tables are created dynamically by the Gold notebook (saveAsTable with overwriteSchema).
# Creating empty stubs here so the catalog browser shows the full structure.

gold_tables_ddl = {
    "dim_customers": """
        customer_id STRING, first_name STRING, last_name STRING, full_name STRING,
        email STRING, city STRING, subscription_tier STRING, customer_segment STRING,
        total_orders INT, lifetime_revenue DOUBLE, avg_order_value DOUBLE,
        days_since_last_order INT, cancel_rate_pct DOUBLE,
        recency_score INT, frequency_score INT, monetary_score INT,
        rfm_score INT, rfm_segment STRING,
        _gold_loaded_at TIMESTAMP
    """,
    "dim_restaurants": """
        restaurant_id STRING, name STRING, city STRING, restaurant_type STRING,
        price_tier STRING, rating_tier STRING,
        total_orders_received INT, total_gmv DOUBLE, avg_order_value DOUBLE,
        unique_customers INT, avg_customer_rating DOUBLE, avg_delivery_time_mins DOUBLE,
        fulfillment_rate_pct DOUBLE, sla_compliance_pct DOUBLE,
        restaurant_health_score DOUBLE,
        _gold_loaded_at TIMESTAMP
    """,
    "fact_orders": """
        order_id STRING, customer_id STRING, restaurant_id STRING,
        order_placed_at TIMESTAMP, order_date DATE, total_amount DOUBLE,
        is_delivered BOOLEAN, is_cancelled BOOLEAN,
        customer_city STRING, restaurant_name STRING,
        delivery_time_mins INT, delivery_sla_status STRING,
        _gold_loaded_at TIMESTAMP
    """,
    "agg_daily_city_metrics": """
        customer_city STRING, order_date DATE, order_year INT, order_month INT,
        total_orders INT, unique_customers INT, total_gmv DOUBLE,
        avg_order_value DOUBLE, cancel_rate_pct DOUBLE,
        _gold_loaded_at TIMESTAMP
    """,
    "agg_restaurant_performance": """
        restaurant_id STRING, name STRING, city STRING,
        restaurant_health_score DOUBLE, overall_rank INT, city_rank INT,
        performance_tier STRING,
        _gold_loaded_at TIMESTAMP
    """,
    "agg_customer_cohorts": """
        cohort_month STRING, activity_month STRING,
        active_customers INT, total_orders INT, cohort_revenue DOUBLE,
        _gold_loaded_at TIMESTAMP
    """,
    "agg_delivery_sla_report": """
        city STRING, vehicle_type STRING, delivery_sla_status STRING,
        delivery_count INT, avg_delivery_mins DOUBLE, avg_distance_km DOUBLE,
        avg_delivery_rating DOUBLE,
        _gold_loaded_at TIMESTAMP
    """,
    "agg_revenue_summary": """
        order_year INT, order_month INT, total_orders INT,
        total_gmv DOUBLE, avg_order_value DOUBLE, net_revenue DOUBLE,
        gmv_growth_pct DOUBLE,
        _gold_loaded_at TIMESTAMP
    """,
}

for table_name, columns in gold_tables_ddl.items():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.gold.{table_name} ({columns})
        USING DELTA
        COMMENT 'Gold layer — {table_name.replace("_", " ")}'
    """)

print("✓ All Gold tables created")
spark.sql(f"SHOW TABLES IN {CATALOG}.gold").show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

for schema in ["bronze", "silver", "gold"]:
    count = spark.sql(f"SHOW TABLES IN {CATALOG}.{schema}").count()
    print(f"  {CATALOG}.{schema}: {count} tables")

print(f"\n🟢 All tables created successfully under catalog '{CATALOG}'")
print(f"   To clean up: DROP CATALOG IF EXISTS {CATALOG} CASCADE")
