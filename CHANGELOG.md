# Changelog

All notable changes to the Zomato Analytics Data Engineering Project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [1.0.0] - 2026-03-22

### Added

- **Medallion Architecture** — Full Bronze → Silver → Gold data pipeline
- **Bronze Layer** (`notebooks/bronze/01_bronze_ingestion.py`)
  - Raw data ingestion with explicit schema enforcement
  - SHA-256 row hashing for change data capture
  - Incremental merge (upsert) support via Delta Lake
  - Audit columns: `_bronze_loaded_at`, `_source_file`, `_row_hash`
- **Silver Layer** (`notebooks/silver/02_silver_transformation.py`)
  - Deduplication with windowed row_number
  - Data type casting and null handling
  - Business rule application (customer segmentation, price tiers, rating tiers)
  - Referential integrity checks across entities
  - Data quality metrics tracking per table
- **Gold Layer** (`notebooks/gold/03_gold_aggregation.py`)
  - `dim_customers` — Customer dimension with RFM scoring (Recency, Frequency, Monetary)
  - `dim_restaurants` — Restaurant dimension with composite health score
  - `fact_orders` — Denormalized order fact table
  - `agg_daily_city_metrics` — City-level daily KPIs
  - `agg_restaurant_performance` — Restaurant scorecard with ranking
  - `agg_customer_cohorts` — Monthly cohort retention analysis
  - `agg_delivery_sla_report` — SLA compliance by city and vehicle type
  - `agg_revenue_summary` — Monthly revenue with MoM growth metrics
- **Executive Dashboard** (`notebooks/dashboard/04_analytics_dashboard.py`)
  - Platform overview KPIs
  - Revenue & GMV trends by month, payment method, and platform
  - City-level performance analysis
  - Restaurant leaderboard by health score
  - Customer RFM segmentation distribution
  - Delivery SLA monitoring
  - Cuisine & time-slot analysis
  - Operational alerts (high cancel rate restaurants, churn-risk customers)
- **Data Generator** (`data_generator/`)
  - Faker-based synthetic data generation for 7 entities
  - Configurable volume (default: 10K customers, 2.5K restaurants, 150K orders)
  - Supports Parquet, CSV, and JSON output formats
  - Deterministic ID generation with MD5 hashing
  - India-localized fake data (names, addresses, phone numbers)
- **CI/CD Pipeline**
  - GitHub Actions CI: lint (Black, isort, Flake8, MyPy), test (pytest + coverage), notebook validation, secret scanning, artifact packaging
  - GitHub Actions CD: automated deployment to Databricks Community Edition via CLI
  - Post-deployment smoke test
  - Databricks workflow job configuration (`deploy/databricks_job_config.json`)
- **Configuration**
  - Environment-aware pipeline config (dev/staging/prod)
  - Data quality thresholds
  - Pipeline scheduling definitions
- **Testing**
  - Unit tests for all data generators
  - Validates record counts, field presence, uniqueness, and value ranges
- **Documentation**
  - Comprehensive README with architecture diagram, data model, and setup guide
  - Environment variable template (`.env.example`)

---

## [Unreleased]

### Planned

- Streaming ingestion with Auto Loader
- Great Expectations data quality framework integration
- dbt transformation layer
- Alerting via Slack/PagerDuty on pipeline failures
- ML feature store integration
- Unity Catalog adoption
