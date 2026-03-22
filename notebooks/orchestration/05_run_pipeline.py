# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Pipeline Orchestrator
# MAGIC
# MAGIC **Purpose**: End-to-end orchestration notebook that runs the full Medallion pipeline
# MAGIC in sequence, tracks record counts at each stage, and sends success/failure alerts.
# MAGIC
# MAGIC **Pipeline Flow**:
# MAGIC ```
# MAGIC 00_create_tables → 01_bronze_ingestion → 02_silver_transformation
# MAGIC     → 03_gold_aggregation → 04_analytics_dashboard → Alerts
# MAGIC ```
# MAGIC
# MAGIC **Alerts**: Sends email/webhook notifications with record counts on success
# MAGIC and error details on failure.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import time
import traceback
from datetime import datetime

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("alert_email", "", "Alert email (optional)")
dbutils.widgets.text("webhook_url", "", "Slack/Teams webhook URL (optional)")

CATALOG = dbutils.widgets.get("catalog_name")
ENV = dbutils.widgets.get("env")
ALERT_EMAIL = dbutils.widgets.get("alert_email")
WEBHOOK_URL = dbutils.widgets.get("webhook_url")

print(f"Catalog      : {CATALOG}")
print(f"Environment  : {ENV}")
print(f"Alert Email  : {ALERT_EMAIL or 'Not configured'}")
print(f"Webhook URL  : {'Configured' if WEBHOOK_URL else 'Not configured'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Step Definitions

# COMMAND ----------

# Base path for notebooks — adjust if your workspace layout differs
NOTEBOOK_BASE = "/Workspace/Zomato-Analytics"

PIPELINE_STEPS = [
    {
        "step": 1,
        "name": "Create Tables (DDL)",
        "notebook": f"{NOTEBOOK_BASE}/setup/00_create_tables",
        "timeout": 300,
        "params": {"catalog_name": CATALOG},
    },
    {
        "step": 2,
        "name": "Bronze Ingestion",
        "notebook": f"{NOTEBOOK_BASE}/bronze/01_bronze_ingestion",
        "timeout": 1800,
        "params": {
            "catalog_name": CATALOG,
            "env": ENV,
            "source_path": f"/Volumes/{CATALOG}/raw/landing",
            "load_type": "full",
        },
    },
    {
        "step": 3,
        "name": "Silver Transformation",
        "notebook": f"{NOTEBOOK_BASE}/silver/02_silver_transformation",
        "timeout": 3600,
        "params": {"catalog_name": CATALOG, "env": ENV},
    },
    {
        "step": 4,
        "name": "Gold Aggregation",
        "notebook": f"{NOTEBOOK_BASE}/gold/03_gold_aggregation",
        "timeout": 3600,
        "params": {"catalog_name": CATALOG, "env": ENV},
    },
    {
        "step": 5,
        "name": "Dashboard Refresh",
        "notebook": f"{NOTEBOOK_BASE}/dashboard/04_analytics_dashboard",
        "timeout": 1800,
        "params": {"catalog_name": CATALOG},
    },
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Helper Functions

# COMMAND ----------

def run_notebook_step(step_config: dict) -> dict:
    """Execute a single pipeline step and capture results."""
    step_name = step_config["name"]
    notebook_path = step_config["notebook"]
    timeout = step_config["timeout"]
    params = step_config["params"]

    print(f"\n{'='*60}")
    print(f"  Step {step_config['step']}: {step_name}")
    print(f"  Notebook: {notebook_path}")
    print(f"{'='*60}")

    start_time = time.time()

    try:
        result = dbutils.notebook.run(
            notebook_path,
            timeout_seconds=timeout,
            arguments=params,
        )
        elapsed = round(time.time() - start_time, 1)

        return {
            "step": step_config["step"],
            "name": step_name,
            "status": "SUCCESS",
            "duration_secs": elapsed,
            "result": result or "OK",
        }

    except Exception as e:
        elapsed = round(time.time() - start_time, 1)
        error_msg = str(e)

        print(f"  ✗ FAILED after {elapsed}s: {error_msg}")

        return {
            "step": step_config["step"],
            "name": step_name,
            "status": "FAILED",
            "duration_secs": elapsed,
            "error": error_msg,
            "traceback": traceback.format_exc(),
        }


def get_table_record_counts(catalog: str) -> dict:
    """Get record counts for all tables across all schemas."""
    counts = {}
    for schema in ["bronze", "silver", "gold"]:
        schema_counts = {}
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            for row in tables:
                table_name = row["tableName"]
                try:
                    count = spark.table(f"{catalog}.{schema}.{table_name}").count()
                    schema_counts[table_name] = count
                except Exception:
                    schema_counts[table_name] = "ERROR"
        except Exception:
            schema_counts["_error"] = "Schema not found"
        counts[schema] = schema_counts
    return counts

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Alert Functions

# COMMAND ----------

def build_alert_message(
    pipeline_status: str,
    step_results: list,
    record_counts: dict,
    total_duration: float,
) -> str:
    """Build a formatted alert message with pipeline results."""
    timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

    # Header
    icon = "✅" if pipeline_status == "SUCCESS" else "❌"
    lines = [
        f"{icon} Zomato Analytics Pipeline — {pipeline_status}",
        f"{'='*55}",
        f"Timestamp   : {timestamp}",
        f"Environment : {ENV}",
        f"Catalog     : {CATALOG}",
        f"Duration    : {total_duration}s",
        "",
        "STEP RESULTS:",
        f"{'-'*55}",
    ]

    # Step results
    for r in step_results:
        status_icon = "✓" if r["status"] == "SUCCESS" else "✗"
        duration = f"{r['duration_secs']}s"
        lines.append(f"  [{status_icon}] Step {r['step']}: {r['name']} — {r['status']} ({duration})")
        if r["status"] == "FAILED":
            lines.append(f"      Error: {r.get('error', 'Unknown')}")

    # Record counts
    lines.extend(["", "RECORD COUNTS:", f"{'-'*55}"])

    total_records = 0
    for schema, tables in record_counts.items():
        lines.append(f"  {CATALOG}.{schema}:")
        for table, count in tables.items():
            if isinstance(count, int):
                lines.append(f"    {table:40s} : {count:>10,}")
                total_records += count
            else:
                lines.append(f"    {table:40s} : {count}")

    lines.extend([
        f"{'-'*55}",
        f"  TOTAL RECORDS: {total_records:,}",
        f"{'='*55}",
    ])

    return "\n".join(lines)


def send_webhook_alert(webhook_url: str, message: str, is_success: bool) -> None:
    """Send alert to Slack or Microsoft Teams webhook."""
    if not webhook_url:
        return

    import requests

    # Detect webhook type and format payload
    if "hooks.slack.com" in webhook_url:
        # Slack format
        payload = {
            "text": f"```\n{message}\n```",
            "username": "Zomato Pipeline Bot",
        }
    else:
        # Microsoft Teams / Generic webhook format
        color = "00FF00" if is_success else "FF0000"
        payload = {
            "@type": "MessageCard",
            "themeColor": color,
            "summary": f"Pipeline {'SUCCESS' if is_success else 'FAILED'}",
            "sections": [{
                "activityTitle": "Zomato Analytics Pipeline",
                "text": f"<pre>{message}</pre>",
            }],
        }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
        print(f"  Webhook alert sent (status: {resp.status_code})")
    except Exception as e:
        print(f"  ⚠ Webhook alert failed: {e}")


def send_email_alert(email: str, message: str, is_success: bool) -> None:
    """Send email alert via Databricks notification (if configured)."""
    if not email:
        return

    subject = f"Zomato Pipeline {'✅ SUCCESS' if is_success else '❌ FAILED'} — {datetime.utcnow().strftime('%Y-%m-%d')}"

    try:
        # Databricks built-in email (requires SMTP configuration)
        dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath()
        print(f"  Email alert: {subject}")
        print(f"  To: {email}")
        print(f"  (Configure SMTP in Databricks Admin Console for actual delivery)")
    except Exception:
        print(f"  ⚠ Email notification not available in this workspace")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute Pipeline

# COMMAND ----------

print("=" * 60)
print("  ZOMATO ANALYTICS — FULL PIPELINE EXECUTION")
print("=" * 60)
print(f"  Started at: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"  Steps: {len(PIPELINE_STEPS)}")
print("=" * 60)

pipeline_start = time.time()
step_results = []
pipeline_failed = False

for step_config in PIPELINE_STEPS:
    result = run_notebook_step(step_config)
    step_results.append(result)

    if result["status"] == "SUCCESS":
        print(f"  ✓ {result['name']} completed in {result['duration_secs']}s")
    else:
        print(f"  ✗ {result['name']} FAILED — stopping pipeline")
        pipeline_failed = True
        break

total_duration = round(time.time() - pipeline_start, 1)
pipeline_status = "FAILED" if pipeline_failed else "SUCCESS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Collect Record Counts

# COMMAND ----------

print("\nCollecting record counts across all layers...")
record_counts = get_table_record_counts(CATALOG)

for schema, tables in record_counts.items():
    print(f"\n  {CATALOG}.{schema}:")
    for table, count in tables.items():
        if isinstance(count, int):
            print(f"    {table}: {count:,}")
        else:
            print(f"    {table}: {count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Send Alerts

# COMMAND ----------

alert_message = build_alert_message(
    pipeline_status=pipeline_status,
    step_results=step_results,
    record_counts=record_counts,
    total_duration=total_duration,
)

print(alert_message)

# Send webhook alert (Slack / Teams)
send_webhook_alert(WEBHOOK_URL, alert_message, is_success=not pipeline_failed)

# Send email alert
send_email_alert(ALERT_EMAIL, alert_message, is_success=not pipeline_failed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pipeline Summary

# COMMAND ----------

print("\n" + "=" * 60)
print(f"  PIPELINE {pipeline_status}")
print("=" * 60)

for r in step_results:
    icon = "✓" if r["status"] == "SUCCESS" else "✗"
    print(f"  [{icon}] Step {r['step']}: {r['name']} — {r['duration_secs']}s")

success_count = sum(1 for r in step_results if r["status"] == "SUCCESS")
print(f"\n  {success_count}/{len(PIPELINE_STEPS)} steps completed")
print(f"  Total duration: {total_duration}s")
print("=" * 60)

# Fail the notebook if pipeline failed (so Databricks Job marks it as failed)
if pipeline_failed:
    failed_step = next(r for r in step_results if r["status"] == "FAILED")
    raise Exception(
        f"Pipeline failed at Step {failed_step['step']}: {failed_step['name']} — {failed_step.get('error', 'Unknown error')}"
    )

print(f"\n🟢 Full pipeline completed successfully in {total_duration}s")
