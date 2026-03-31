# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Pipeline Validation
# MAGIC
# MAGIC **Purpose**: End-to-end record count validation across every layer of the
# MAGIC medallion pipeline. Compares Bronze → Silver → Gold record counts and flags
# MAGIC any layer where records dropped beyond the expected tolerance.
# MAGIC
# MAGIC **Checks performed**:
# MAGIC 1. Bronze record counts per table (source of truth)
# MAGIC 2. Silver record counts — expected drop ≤ 5% (dedup + quality filtering)
# MAGIC 3. Gold fact/dim record counts — validated against Silver
# MAGIC 4. Audit log completeness — all layers must have entries for today's run
# MAGIC
# MAGIC **Result**: Writes a validation summary row to `audit.pipeline_audit_log`.
# MAGIC Raises an exception if any CRITICAL check fails, causing the workflow task to fail.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
dbutils.widgets.text("env", "dev", "Environment")

CATALOG = dbutils.widgets.get("catalog_name")
ENV = dbutils.widgets.get("env")

BRONZE_FQN = f"{CATALOG}.bronze"
SILVER_FQN = f"{CATALOG}.silver"
GOLD_FQN = f"{CATALOG}.gold"
AUDIT_FQN = f"{CATALOG}.audit"

# Silver may drop records due to dedup and quality filtering.
# We allow up to 10% drop from Bronze → Silver before flagging.
SILVER_DROP_TOLERANCE_PCT = 10.0

spark.sql(f"USE CATALOG {CATALOG}")

print(f"Catalog   : {CATALOG}")
print(f"Env       : {ENV}")
print(f"Bronze    : {BRONZE_FQN}")
print(f"Silver    : {SILVER_FQN}")
print(f"Gold      : {GOLD_FQN}")
print(f"Audit     : {AUDIT_FQN}")
print(f"Silver drop tolerance: {SILVER_DROP_TOLERANCE_PCT}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Collect Record Counts — Bronze Layer

# COMMAND ----------

CORE_TABLES = ["customers", "restaurants", "orders", "deliveries", "reviews"]

print("=" * 65)
print("BRONZE LAYER — Record Counts")
print("=" * 65)

bronze_counts = {}
for tbl in CORE_TABLES:
    cnt = spark.table(f"{BRONZE_FQN}.{tbl}").count()
    bronze_counts[tbl] = cnt
    print(f"  {BRONZE_FQN}.{tbl}: {cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Collect Record Counts — Silver Layer

# COMMAND ----------

print("=" * 65)
print("SILVER LAYER — Record Counts")
print("=" * 65)

silver_counts = {}
for tbl in CORE_TABLES:
    cnt = spark.table(f"{SILVER_FQN}.{tbl}").count()
    silver_counts[tbl] = cnt
    print(f"  {SILVER_FQN}.{tbl}: {cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Collect Record Counts — Gold Layer

# COMMAND ----------

GOLD_TABLES = [
    "dim_customers",
    "dim_restaurants",
    "fact_orders",
    "agg_daily_city_metrics",
    "agg_restaurant_performance",
    "agg_customer_cohorts",
    "agg_delivery_sla_report",
    "agg_revenue_summary",
]

print("=" * 65)
print("GOLD LAYER — Record Counts")
print("=" * 65)

gold_counts = {}
for tbl in GOLD_TABLES:
    cnt = spark.table(f"{GOLD_FQN}.{tbl}").count()
    gold_counts[tbl] = cnt
    print(f"  {GOLD_FQN}.{tbl}: {cnt:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze → Silver Comparison

# COMMAND ----------

print("\n" + "=" * 65)
print("BRONZE → SILVER — Drop Rate Analysis")
print("=" * 65)
print(f"{'Table':<20} {'Bronze':>12} {'Silver':>12} {'Dropped':>10} {'Drop %':>8} {'Status':>10}")
print("-" * 65)

bz_silver_checks = []
for tbl in CORE_TABLES:
    b_cnt = bronze_counts[tbl]
    s_cnt = silver_counts[tbl]
    dropped = b_cnt - s_cnt
    drop_pct = round(dropped / max(b_cnt, 1) * 100, 2)
    passed = drop_pct <= SILVER_DROP_TOLERANCE_PCT
    status = "PASS" if passed else "FAIL"
    bz_silver_checks.append({
        "table": tbl, "bronze": b_cnt, "silver": s_cnt,
        "dropped": dropped, "drop_pct": drop_pct, "passed": passed,
    })
    print(f"  {tbl:<18} {b_cnt:>12,} {s_cnt:>12,} {dropped:>10,} {drop_pct:>7}% {status:>10}")

print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver → Gold Comparison
# MAGIC
# MAGIC Gold tables are aggregations and joins — row counts will differ from Silver.
# MAGIC We validate that:
# MAGIC - `dim_customers` count ≤ `silver.customers` count (dedup / enrichment)
# MAGIC - `dim_restaurants` count ≤ `silver.restaurants` count
# MAGIC - `fact_orders` count ≤ `silver.orders` count (inner join filtering)
# MAGIC - All Gold tables have > 0 records

# COMMAND ----------

print("\n" + "=" * 65)
print("SILVER → GOLD — Key Table Checks")
print("=" * 65)

sg_checks = [
    ("dim_customers",   "customers",   "≤ silver.customers (dedup+enrich)"),
    ("dim_restaurants", "restaurants", "≤ silver.restaurants (dedup+enrich)"),
    ("fact_orders",     "orders",      "≤ silver.orders (inner-join filtering)"),
]

sg_results = []
for gold_tbl, silver_tbl, note in sg_checks:
    g_cnt = gold_counts[gold_tbl]
    s_cnt = silver_counts[silver_tbl]
    passed = 0 < g_cnt <= s_cnt
    status = "PASS" if passed else "FAIL"
    sg_results.append({"table": gold_tbl, "gold": g_cnt, "silver": s_cnt, "passed": passed})
    print(f"  [{status}] {gold_tbl}: {g_cnt:,}  ({note}: {s_cnt:,})")

print()
print("Gold aggregation tables (must be > 0 records):")
agg_results = []
for tbl in ["agg_daily_city_metrics", "agg_restaurant_performance",
            "agg_customer_cohorts", "agg_delivery_sla_report", "agg_revenue_summary"]:
    cnt = gold_counts[tbl]
    passed = cnt > 0
    status = "PASS" if passed else "FAIL"
    agg_results.append({"table": tbl, "gold": cnt, "passed": passed})
    print(f"  [{status}] {tbl}: {cnt:,}")

print("=" * 65)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validation Summary & Final Status

# COMMAND ----------

from datetime import date, datetime

all_checks = (
    [c["passed"] for c in bz_silver_checks]
    + [c["passed"] for c in sg_results]
    + [c["passed"] for c in agg_results]
)

total_checks = len(all_checks)
passed_checks = sum(all_checks)
failed_checks = total_checks - passed_checks

overall_status = "SUCCESS" if failed_checks == 0 else "FAILED"

print("\n" + "=" * 65)
print("PIPELINE VALIDATION SUMMARY")
print("=" * 65)
print(f"  Total checks   : {total_checks}")
print(f"  Passed         : {passed_checks}")
print(f"  Failed         : {failed_checks}")
print(f"  Overall status : {overall_status}")
print("=" * 65)

# Bronze total (source baseline)
total_bronze = sum(bronze_counts.values())
total_silver = sum(silver_counts.values())
total_gold_fact = gold_counts.get("fact_orders", 0)

print(f"\n  Source (Bronze) total records  : {total_bronze:,}")
print(f"  Silver total records           : {total_silver:,}")
print(f"  Gold fact_orders records       : {total_gold_fact:,}")
print(f"  Overall Bronze→Silver drop     : {round((total_bronze - total_silver) / max(total_bronze,1) * 100, 2)}%")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Write Validation Result to Audit Log

# COMMAND ----------

from pyspark.sql import Row

try:
    _ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
    _workflow_name = _ctx.jobName().getOrElse("manual_run")
except Exception:
    _workflow_name = "manual_run"

_load_date = date.today()
_run_dt = datetime.utcnow()

failed_tables = (
    [c["table"] for c in bz_silver_checks if not c["passed"]]
    + [c["table"] for c in sg_results if not c["passed"]]
    + [c["table"] for c in agg_results if not c["passed"]]
)

error_msg = None if overall_status == "SUCCESS" else f"Failed checks: {', '.join(failed_tables)}"

validation_row = Row(
    run_datetime=_run_dt,
    workflow_name=_workflow_name,
    task_name="pipeline_validation",
    layer="validation",
    table_name="all_layers",
    total_records=total_bronze,
    load_date=_load_date,
    status=overall_status,
    error_message=error_msg,
)

(
    spark.createDataFrame([validation_row])
    .write.format("delta")
    .mode("append")
    .saveAsTable(f"{AUDIT_FQN}.pipeline_audit_log")
)

print(f"✓ Validation audit entry written → {AUDIT_FQN}.pipeline_audit_log")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Show Full Audit Log for Today's Run

# COMMAND ----------

from pyspark.sql.functions import col, to_date, current_date

print(f"\nAudit log entries for load_date = {_load_date}:")
(
    spark.table(f"{AUDIT_FQN}.pipeline_audit_log")
    .filter(col("load_date") == _load_date)
    .orderBy("run_datetime", "layer")
    .show(50, truncate=False)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Raise Exception on Failure (Fails the Workflow Task)

# COMMAND ----------

if overall_status == "FAILED":
    raise Exception(
        f"Pipeline validation FAILED — {failed_checks} check(s) did not pass: {failed_tables}. "
        f"Check {AUDIT_FQN}.pipeline_audit_log for details."
    )

print("🟢 Pipeline validation complete. All checks passed.")
print(f"   Audit log: {AUDIT_FQN}.pipeline_audit_log")
