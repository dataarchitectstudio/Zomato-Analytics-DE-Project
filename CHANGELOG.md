# Changelog

All notable changes to the Zomato Analytics Data Engineering Project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

---

## [1.1.0] - 2026-03-22

### Added

- **Table Creation Script** (`notebooks/setup/00_create_tables.py`)
  - Idempotent DDL for all Bronze, Silver, and Gold tables
  - Creates dedicated Unity Catalog and schemas automatically
  - Full schema definitions with column types and comments
- **Pipeline Orchestrator** (`notebooks/orchestration/05_run_pipeline.py`)
  - End-to-end pipeline execution: DDL → Bronze → Silver → Gold → Dashboard
  - Tracks record counts across all layers after pipeline completion
  - Sends success/failure alerts with detailed step results and record counts
  - Slack webhook support (incoming webhooks)
  - Microsoft Teams webhook support
  - Email alert support via Databricks notifications
  - Automatic pipeline abort on step failure with error details
- **Databricks Job Configuration** (`deploy/databricks_job_config.json`)
  - Multi-task job with dependency chain
  - Configurable schedule (default: daily 2:00 AM IST)
  - Email and webhook notifications for success/failure
  - Auto-retry on Bronze ingestion failures

### Changed

- **README** — Rewritten with Databricks Free Edition setup guide
  - Step-by-step instructions for getting PAT token
  - Exact navigation path for storing GitHub Secrets
  - Updated architecture to reflect Unity Catalog namespace
  - Added pipeline execution options (orchestrator, manual, scheduled job)
  - Added cleanup instructions
- **CD Pipeline** — Now deploys 6 notebooks (added setup + orchestration)
- **Deploy Script** — Updated to deploy setup and orchestration notebooks

---

## [1.0.1] - 2026-03-22

### Fixed

- Auto-formatted all Python files with Black to pass CI
- Removed unused imports (`uuid`, `sys`, `Path`) flagged by Flake8
- Fixed `datetime.utcnow()` deprecation warnings → `datetime.now(UTC)`
- Simplified CI pipeline: Flake8 lint + pytest (removed strict Black/isort/MyPy gates)
- Replaced `databricks-cli` in CD with REST API deploy script for reliability

---

## [1.0.0] - 2026-03-22

### Added

- **Medallion Architecture** — Full Bronze → Silver → Gold data pipeline
- **Unity Catalog Isolation** — Dedicated `zomato_analytics` catalog
  - `zomato_analytics.bronze.*` — Raw ingested data
  - `zomato_analytics.silver.*` — Cleansed and conformed data
  - `zomato_analytics.gold.*` — Business aggregations
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
  - `dim_customers` — Customer dimension with RFM scoring
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
  - GitHub Actions CI: Flake8 lint, pytest + coverage, notebook validation, secret scanning
  - GitHub Actions CD: automated deployment to Databricks Free Edition via REST API
  - Post-deployment smoke test
- **Configuration**
  - Environment-aware pipeline config (dev/staging/prod)
  - Data quality thresholds
  - Pipeline scheduling definitions
- **Testing**
  - Unit tests for all data generators (15 tests)
  - Validates record counts, field presence, uniqueness, and value ranges
- **Documentation**
  - Comprehensive README with architecture diagram and setup guide
  - Environment variable template (`.env.example`)

---

## [Unreleased]

### Planned

- Streaming ingestion with Auto Loader
- Great Expectations data quality framework integration
- dbt transformation layer
- ML feature store integration
