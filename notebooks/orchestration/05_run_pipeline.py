# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Pipeline Orchestrator
# MAGIC
# MAGIC **Purpose**: End-to-end orchestration notebook that runs the full Medallion pipeline
# MAGIC in sequence, tracks record counts at each stage, and sends email alerts on
# MAGIC success or failure.
# MAGIC
# MAGIC **Pipeline Flow**:
# MAGIC ```
# MAGIC 00_create_tables → 01_generate_data → 01_bronze_ingestion
# MAGIC     → 02_silver_transformation → 03_gold_aggregation
# MAGIC     → 04_analytics_dashboard → Email Alert (with dashboard link)
# MAGIC ```
# MAGIC
# MAGIC **Email Alerts**: Sends to `dataarchitectstudio@gmail.com` with:
# MAGIC - Clean subject: `SUCCESS` or `FAILED` with pipeline name
# MAGIC - Body: step results, record counts, duration, exception details on failure

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

import json
import smtplib
import time
import traceback
from datetime import datetime, UTC
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
dbutils.widgets.text("env", "dev", "Environment")
dbutils.widgets.text("smtp_host", "smtp.gmail.com", "SMTP host")
dbutils.widgets.text("smtp_port", "587", "SMTP port")
dbutils.widgets.text("smtp_user", "", "SMTP username (sender email)")
dbutils.widgets.text("smtp_password", "", "SMTP app password")
dbutils.widgets.text("webhook_url", "", "Slack/Teams webhook URL (optional)")

CATALOG = dbutils.widgets.get("catalog_name")
ENV = dbutils.widgets.get("env")
SMTP_HOST = dbutils.widgets.get("smtp_host")
SMTP_PORT = int(dbutils.widgets.get("smtp_port"))
SMTP_USER = dbutils.widgets.get("smtp_user")
SMTP_PASSWORD = dbutils.widgets.get("smtp_password")
WEBHOOK_URL = dbutils.widgets.get("webhook_url")

# ─── Alert Recipients ───────────────────────────────────────
ALERT_EMAIL_TO = "dataarchitectstudio@gmail.com"
PIPELINE_NAME = "Zomato Analytics — Medallion Pipeline"
# ─────────────────────────────────────────────────────────────

# ─── Dashboard Link ─────────────────────────────────────────
# Derive workspace URL from Spark config for the dashboard link in emails
try:
    WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl", "")
    if WORKSPACE_URL and not WORKSPACE_URL.startswith("http"):
        WORKSPACE_URL = f"https://{WORKSPACE_URL}"
except Exception:
    WORKSPACE_URL = ""

DASHBOARD_NOTEBOOK = f"{NOTEBOOK_BASE}/dashboard/04_analytics_dashboard"
if WORKSPACE_URL:
    DASHBOARD_LINK = f"{WORKSPACE_URL}/#notebook{DASHBOARD_NOTEBOOK}"
else:
    DASHBOARD_LINK = ""
# ─────────────────────────────────────────────────────────────

print(f"Catalog      : {CATALOG}")
print(f"Environment  : {ENV}")
print(f"Alert Email  : {ALERT_EMAIL_TO}")
print(f"Dashboard    : {DASHBOARD_LINK or 'Link will be generated at runtime'}")
print(f"SMTP         : {'Configured' if SMTP_USER else 'Not configured (will print to console)'}")
print(f"Webhook      : {'Configured' if WEBHOOK_URL else 'Not configured'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Pipeline Step Definitions

# COMMAND ----------

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
        "name": "Generate Data",
        "notebook": f"{NOTEBOOK_BASE}/setup/01_generate_data",
        "timeout": 1800,
        "params": {"catalog_name": CATALOG},
    },
    {
        "step": 3,
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
        "step": 4,
        "name": "Silver Transformation",
        "notebook": f"{NOTEBOOK_BASE}/silver/02_silver_transformation",
        "timeout": 3600,
        "params": {"catalog_name": CATALOG, "env": ENV},
    },
    {
        "step": 5,
        "name": "Gold Aggregation",
        "notebook": f"{NOTEBOOK_BASE}/gold/03_gold_aggregation",
        "timeout": 3600,
        "params": {"catalog_name": CATALOG, "env": ENV},
    },
    {
        "step": 6,
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
    total = 0
    for schema in ["bronze", "silver", "gold"]:
        schema_counts = {}
        try:
            tables = spark.sql(f"SHOW TABLES IN {catalog}.{schema}").collect()
            for row in tables:
                table_name = row["tableName"]
                try:
                    count = spark.table(f"{catalog}.{schema}.{table_name}").count()
                    schema_counts[table_name] = count
                    total += count
                except Exception:
                    schema_counts[table_name] = -1
        except Exception:
            schema_counts["_error"] = -1
        counts[schema] = schema_counts
    return counts, total

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Email Alert Function

# COMMAND ----------

def _format_duration(seconds: float) -> str:
    """Convert seconds to human-readable duration."""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    minutes = int(seconds // 60)
    secs = int(seconds % 60)
    if minutes < 60:
        return f"{minutes}m {secs}s"
    hours = int(minutes // 60)
    mins = minutes % 60
    return f"{hours}h {mins}m {secs}s"


def build_email_html(
    pipeline_status: str,
    step_results: list,
    record_counts: dict,
    total_records: int,
    total_duration: float,
    failed_step: dict = None,
    dashboard_link: str = "",
) -> str:
    """Build an HTML email body with pipeline results and dashboard link."""
    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    duration_str = _format_duration(total_duration)

    # Status colors
    if pipeline_status == "SUCCESS":
        status_color = "#28a745"
        status_bg = "#d4edda"
        status_icon = "&#10004;"
    else:
        status_color = "#dc3545"
        status_bg = "#f8d7da"
        status_icon = "&#10008;"

    # Build step results rows
    step_rows = ""
    for r in step_results:
        if r["status"] == "SUCCESS":
            row_color = "#d4edda"
            row_icon = "&#10004;"
        else:
            row_color = "#f8d7da"
            row_icon = "&#10008;"
        step_rows += f"""
        <tr style="background-color: {row_color};">
            <td style="padding: 8px; border: 1px solid #dee2e6;">{row_icon} Step {r['step']}</td>
            <td style="padding: 8px; border: 1px solid #dee2e6;">{r['name']}</td>
            <td style="padding: 8px; border: 1px solid #dee2e6; text-align: center;">{r['status']}</td>
            <td style="padding: 8px; border: 1px solid #dee2e6; text-align: right;">{_format_duration(r['duration_secs'])}</td>
        </tr>"""

    # Build record count rows
    record_rows = ""
    for schema, tables in record_counts.items():
        for table, count in tables.items():
            count_display = f"{count:,}" if isinstance(count, int) and count >= 0 else "ERROR"
            record_rows += f"""
        <tr>
            <td style="padding: 6px 8px; border: 1px solid #dee2e6; font-family: monospace;">{CATALOG}.{schema}</td>
            <td style="padding: 6px 8px; border: 1px solid #dee2e6;">{table}</td>
            <td style="padding: 6px 8px; border: 1px solid #dee2e6; text-align: right;">{count_display}</td>
        </tr>"""

    # Build failure section
    failure_section = ""
    if failed_step:
        failure_section = f"""
    <div style="background-color: #f8d7da; border: 1px solid #f5c6cb; border-radius: 4px; padding: 15px; margin: 15px 0;">
        <h3 style="color: #721c24; margin-top: 0;">Failure Details</h3>
        <p><strong>Failed Step:</strong> Step {failed_step['step']} — {failed_step['name']}</p>
        <p><strong>Error:</strong></p>
        <pre style="background-color: #fff; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 12px; color: #721c24;">{failed_step.get('error', 'Unknown error')}</pre>
        <p><strong>Stack Trace:</strong></p>
        <pre style="background-color: #fff; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 11px; color: #555;">{failed_step.get('traceback', 'Not available')}</pre>
    </div>"""

    html = f"""
    <html>
    <body style="font-family: Arial, sans-serif; color: #333; max-width: 800px; margin: 0 auto;">

        <!-- Header -->
        <div style="background-color: {status_color}; color: white; padding: 20px; border-radius: 8px 8px 0 0;">
            <h1 style="margin: 0; font-size: 22px;">{status_icon} {PIPELINE_NAME}</h1>
            <h2 style="margin: 5px 0 0 0; font-size: 18px; font-weight: normal;">Pipeline Status: {pipeline_status}</h2>
        </div>

        <!-- Summary -->
        <div style="background-color: {status_bg}; padding: 15px; border-bottom: 1px solid #dee2e6;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="padding: 4px 0;"><strong>Timestamp:</strong></td>
                    <td>{timestamp}</td>
                </tr>
                <tr>
                    <td style="padding: 4px 0;"><strong>Environment:</strong></td>
                    <td>{ENV}</td>
                </tr>
                <tr>
                    <td style="padding: 4px 0;"><strong>Catalog:</strong></td>
                    <td>{CATALOG}</td>
                </tr>
                <tr>
                    <td style="padding: 4px 0;"><strong>Total Duration:</strong></td>
                    <td>{duration_str}</td>
                </tr>
                <tr>
                    <td style="padding: 4px 0;"><strong>Total Records Processed:</strong></td>
                    <td><strong>{total_records:,}</strong></td>
                </tr>
            </table>
        </div>

        {failure_section}

        <!-- Step Results -->
        <div style="padding: 15px 0;">
            <h3 style="margin-bottom: 10px;">Pipeline Steps</h3>
            <table style="width: 100%; border-collapse: collapse; border: 1px solid #dee2e6;">
                <thead>
                    <tr style="background-color: #343a40; color: white;">
                        <th style="padding: 10px; text-align: left;">Step</th>
                        <th style="padding: 10px; text-align: left;">Name</th>
                        <th style="padding: 10px; text-align: center;">Status</th>
                        <th style="padding: 10px; text-align: right;">Duration</th>
                    </tr>
                </thead>
                <tbody>
                    {step_rows}
                </tbody>
            </table>
        </div>

        <!-- Record Counts -->
        <div style="padding: 15px 0;">
            <h3 style="margin-bottom: 10px;">Record Counts by Table</h3>
            <table style="width: 100%; border-collapse: collapse; border: 1px solid #dee2e6;">
                <thead>
                    <tr style="background-color: #343a40; color: white;">
                        <th style="padding: 8px; text-align: left;">Schema</th>
                        <th style="padding: 8px; text-align: left;">Table</th>
                        <th style="padding: 8px; text-align: right;">Records</th>
                    </tr>
                </thead>
                <tbody>
                    {record_rows}
                    <tr style="background-color: #e9ecef; font-weight: bold;">
                        <td style="padding: 8px; border: 1px solid #dee2e6;" colspan="2">TOTAL</td>
                        <td style="padding: 8px; border: 1px solid #dee2e6; text-align: right;">{total_records:,}</td>
                    </tr>
                </tbody>
            </table>
        </div>

        <!-- Dashboard Link -->
        {f'''
        <div style="padding: 15px; text-align: center;">
            <a href="{dashboard_link}"
               style="display: inline-block; background-color: #007bff; color: white; padding: 12px 30px;
                      border-radius: 6px; text-decoration: none; font-size: 16px; font-weight: bold;">
                Open Executive Dashboard
            </a>
            <p style="margin: 8px 0 0 0; font-size: 12px; color: #6c757d;">Click to view the latest analytics in Databricks</p>
        </div>
        ''' if dashboard_link else ''}

        <!-- Footer -->
        <div style="background-color: #f8f9fa; padding: 12px; border-radius: 0 0 8px 8px; border-top: 1px solid #dee2e6; font-size: 12px; color: #6c757d;">
            <p style="margin: 0;">Zomato Analytics Data Engineering Project | Medallion Architecture</p>
            <p style="margin: 4px 0 0 0;">Automated alert from pipeline orchestrator</p>
        </div>

    </body>
    </html>
    """
    return html


def send_email(
    pipeline_status: str,
    step_results: list,
    record_counts: dict,
    total_records: int,
    total_duration: float,
    failed_step: dict = None,
) -> None:
    """Send pipeline alert email to dataarchitectstudio@gmail.com."""

    # ── Build subject ──
    date_str = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    if pipeline_status == "SUCCESS":
        subject = f"SUCCESS | {PIPELINE_NAME} | {total_records:,} records | {date_str}"
    else:
        failed_name = failed_step['name'] if failed_step else "Unknown"
        subject = f"FAILED | {PIPELINE_NAME} | Step: {failed_name} | {date_str}"

    # ── Build HTML body ──
    html_body = build_email_html(
        pipeline_status=pipeline_status,
        step_results=step_results,
        record_counts=record_counts,
        total_records=total_records,
        total_duration=total_duration,
        failed_step=failed_step,
        dashboard_link=DASHBOARD_LINK,
    )

    # ── Send via SMTP ──
    if SMTP_USER and SMTP_PASSWORD:
        try:
            msg = MIMEMultipart("alternative")
            msg["Subject"] = subject
            msg["From"] = SMTP_USER
            msg["To"] = ALERT_EMAIL_TO
            msg.attach(MIMEText(html_body, "html"))

            with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
                server.starttls()
                server.login(SMTP_USER, SMTP_PASSWORD)
                server.sendmail(SMTP_USER, ALERT_EMAIL_TO, msg.as_string())

            print(f"  ✓ Email sent to {ALERT_EMAIL_TO}")
            print(f"    Subject: {subject}")
        except Exception as e:
            print(f"  ✗ Email send failed: {e}")
            print(f"    Subject would have been: {subject}")
    else:
        # SMTP not configured — print the email content to console
        print(f"\n{'='*60}")
        print(f"  EMAIL ALERT (SMTP not configured — printing to console)")
        print(f"{'='*60}")
        print(f"  To      : {ALERT_EMAIL_TO}")
        print(f"  Subject : {subject}")
        print(f"{'='*60}")
        # Print plain text summary instead of HTML
        for r in step_results:
            icon = "✓" if r["status"] == "SUCCESS" else "✗"
            print(f"  [{icon}] Step {r['step']}: {r['name']} — {r['status']} ({_format_duration(r['duration_secs'])})")
            if r["status"] == "FAILED" and r.get("error"):
                print(f"      Error: {r['error']}")
        print(f"\n  Total Records: {total_records:,}")
        print(f"  Total Duration: {_format_duration(total_duration)}")
        print(f"{'='*60}")


def send_webhook_alert(webhook_url: str, pipeline_status: str, step_results: list,
                       total_records: int, total_duration: float, failed_step: dict = None) -> None:
    """Send alert to Slack or Microsoft Teams webhook."""
    if not webhook_url:
        return

    import requests

    timestamp = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")
    duration_str = _format_duration(total_duration)

    # Build compact text summary
    lines = [f"{'✅' if pipeline_status == 'SUCCESS' else '❌'} *{PIPELINE_NAME}* — {pipeline_status}"]
    lines.append(f"Environment: {ENV} | Duration: {duration_str} | Records: {total_records:,}")
    lines.append("")
    for r in step_results:
        icon = "✓" if r["status"] == "SUCCESS" else "✗"
        lines.append(f"  {icon} Step {r['step']}: {r['name']} ({_format_duration(r['duration_secs'])})")
    if failed_step:
        lines.append(f"\n*Error:* `{failed_step.get('error', 'Unknown')}`")

    text = "\n".join(lines)

    if "hooks.slack.com" in webhook_url:
        payload = {"text": text, "username": "Zomato Pipeline Bot"}
    else:
        color = "00FF00" if pipeline_status == "SUCCESS" else "FF0000"
        payload = {
            "@type": "MessageCard",
            "themeColor": color,
            "summary": f"Pipeline {pipeline_status}",
            "sections": [{"activityTitle": PIPELINE_NAME, "text": f"<pre>{text}</pre>"}],
        }

    try:
        resp = requests.post(webhook_url, json=payload, timeout=15)
        print(f"  ✓ Webhook alert sent (status: {resp.status_code})")
    except Exception as e:
        print(f"  ✗ Webhook alert failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Execute Pipeline

# COMMAND ----------

print("=" * 60)
print(f"  {PIPELINE_NAME}")
print("=" * 60)
print(f"  Started at : {datetime.now(UTC).strftime('%Y-%m-%d %H:%M:%S')} UTC")
print(f"  Catalog    : {CATALOG}")
print(f"  Steps      : {len(PIPELINE_STEPS)}")
print(f"  Alert to   : {ALERT_EMAIL_TO}")
print("=" * 60)

pipeline_start = time.time()
step_results = []
pipeline_failed = False
failed_step_detail = None

for step_config in PIPELINE_STEPS:
    result = run_notebook_step(step_config)
    step_results.append(result)

    if result["status"] == "SUCCESS":
        print(f"  ✓ {result['name']} completed in {_format_duration(result['duration_secs'])}")
    else:
        print(f"  ✗ {result['name']} FAILED — stopping pipeline")
        pipeline_failed = True
        failed_step_detail = result
        break

total_duration = round(time.time() - pipeline_start, 1)
pipeline_status = "FAILED" if pipeline_failed else "SUCCESS"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Collect Record Counts

# COMMAND ----------

print("\nCollecting record counts across all layers...")
record_counts, total_records = get_table_record_counts(CATALOG)

for schema, tables in record_counts.items():
    print(f"\n  {CATALOG}.{schema}:")
    for table, count in tables.items():
        if isinstance(count, int) and count >= 0:
            print(f"    {table}: {count:,}")
        else:
            print(f"    {table}: ERROR")

print(f"\n  TOTAL RECORDS: {total_records:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Send Alerts

# COMMAND ----------

# ── Send Email Alert ──
print("\nSending email alert...")
send_email(
    pipeline_status=pipeline_status,
    step_results=step_results,
    record_counts=record_counts,
    total_records=total_records,
    total_duration=total_duration,
    failed_step=failed_step_detail,
)

# ── Send Webhook Alert (Slack / Teams) ──
send_webhook_alert(
    webhook_url=WEBHOOK_URL,
    pipeline_status=pipeline_status,
    step_results=step_results,
    total_records=total_records,
    total_duration=total_duration,
    failed_step=failed_step_detail,
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Pipeline Summary

# COMMAND ----------

print("\n" + "=" * 60)
print(f"  PIPELINE {pipeline_status}")
print("=" * 60)

for r in step_results:
    icon = "✓" if r["status"] == "SUCCESS" else "✗"
    print(f"  [{icon}] Step {r['step']}: {r['name']} — {_format_duration(r['duration_secs'])}")

success_count = sum(1 for r in step_results if r["status"] == "SUCCESS")
print(f"\n  Steps completed  : {success_count}/{len(PIPELINE_STEPS)}")
print(f"  Total records    : {total_records:,}")
print(f"  Total duration   : {_format_duration(total_duration)}")
print(f"  Alert sent to    : {ALERT_EMAIL_TO}")
print("=" * 60)

# Fail the notebook if pipeline failed (so Databricks Job marks it as failed)
if pipeline_failed:
    raise Exception(
        f"Pipeline FAILED at Step {failed_step_detail['step']}: "
        f"{failed_step_detail['name']} — {failed_step_detail.get('error', 'Unknown error')}"
    )

print(f"\n🟢 Full pipeline completed successfully in {_format_duration(total_duration)}")
