"""
Databricks Job Creation Script
Creates or updates the Zomato Analytics pipeline job via REST API.
Works with Databricks Free Edition.

Usage:
    python scripts/create_databricks_job.py \
        --host https://your-workspace.cloud.databricks.com \
        --token dapi... \
        --workspace-path /Workspace/Zomato-Analytics \
        --alert-email dataarchitectstudio@gmail.com
"""

import argparse
import json
import sys

import requests


JOB_NAME = "Zomato Analytics — Full Medallion Pipeline"


def api_request(host, token, method, endpoint, payload=None):
    """Make authenticated request to Databricks REST API."""
    url = f"{host.rstrip('/')}/api/2.1{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = requests.request(method, url, headers=headers, json=payload, timeout=30)
    if resp.status_code in (200, 201):
        return resp.json() if resp.text else {}
    print(f"  API Error ({resp.status_code}): {resp.text}")
    return {"error": resp.text, "status_code": resp.status_code}


def find_existing_job(host, token):
    """Check if the job already exists by name."""
    result = api_request(host, token, "GET", f"/jobs/list?name={JOB_NAME}")
    if "error" in result:
        return None
    jobs = result.get("jobs", [])
    for job in jobs:
        if job.get("settings", {}).get("name") == JOB_NAME:
            return job.get("job_id")
    return None


def build_job_config(workspace_path, alert_email):
    """Build the Databricks job configuration."""
    wp = workspace_path.rstrip("/")

    tasks = [
        {
            "task_key": "create_tables",
            "description": "Create Unity Catalog, schemas, volumes, and all Delta tables",
            "notebook_task": {
                "notebook_path": f"{wp}/setup/00_create_tables",
                "base_parameters": {"catalog_name": "zomato_analytics"},
            },
            "timeout_seconds": 300,
            "max_retries": 1,
        },
        {
            "task_key": "generate_data",
            "description": "Generate synthetic data and write Parquet to landing volume",
            "depends_on": [{"task_key": "create_tables"}],
            "notebook_task": {
                "notebook_path": f"{wp}/setup/01_generate_data",
                "base_parameters": {"catalog_name": "zomato_analytics"},
            },
            "timeout_seconds": 1800,
            "max_retries": 1,
        },
        {
            "task_key": "bronze_ingestion",
            "description": "Ingest raw data into Bronze layer",
            "depends_on": [{"task_key": "generate_data"}],
            "notebook_task": {
                "notebook_path": f"{wp}/bronze/01_bronze_ingestion",
                "base_parameters": {
                    "catalog_name": "zomato_analytics",
                    "env": "dev",
                    "source_path": "/Volumes/zomato_analytics/raw/landing",
                    "load_type": "full",
                },
            },
            "timeout_seconds": 1800,
            "max_retries": 2,
            "min_retry_interval_millis": 60000,
        },
        {
            "task_key": "silver_transformation",
            "description": "Cleanse and transform Bronze data into Silver layer",
            "depends_on": [{"task_key": "bronze_ingestion"}],
            "notebook_task": {
                "notebook_path": f"{wp}/silver/02_silver_transformation",
                "base_parameters": {"catalog_name": "zomato_analytics", "env": "dev"},
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
        },
        {
            "task_key": "gold_aggregation",
            "description": "Build business aggregations in Gold layer",
            "depends_on": [{"task_key": "silver_transformation"}],
            "notebook_task": {
                "notebook_path": f"{wp}/gold/03_gold_aggregation",
                "base_parameters": {"catalog_name": "zomato_analytics", "env": "dev"},
            },
            "timeout_seconds": 3600,
            "max_retries": 1,
        },
        {
            "task_key": "dashboard_refresh",
            "description": "Refresh executive analytics dashboard",
            "depends_on": [{"task_key": "gold_aggregation"}],
            "notebook_task": {
                "notebook_path": f"{wp}/dashboard/04_analytics_dashboard",
                "base_parameters": {"catalog_name": "zomato_analytics"},
            },
            "timeout_seconds": 1800,
            "max_retries": 1,
        },
    ]

    job_config = {
        "name": JOB_NAME,
        "description": "End-to-end pipeline: DDL → Data Gen → Bronze → Silver → Gold → Dashboard",
        "tags": {
            "project": "zomato-analytics",
            "team": "data-engineering",
            "architecture": "medallion",
        },
        "tasks": tasks,
        "schedule": {
            "quartz_cron_expression": "0 0 2 * * ?",
            "timezone_id": "Asia/Kolkata",
            "pause_status": "PAUSED",
        },
        "email_notifications": {
            "on_success": [alert_email] if alert_email else [],
            "on_failure": [alert_email] if alert_email else [],
            "no_alert_for_skipped_runs": True,
        },
        "max_concurrent_runs": 1,
        "format": "MULTI_TASK",
    }

    return job_config


def main():
    parser = argparse.ArgumentParser(description="Create/update Databricks Job")
    parser.add_argument("--host", required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks PAT")
    parser.add_argument("--workspace-path", required=True, help="Notebook workspace path")
    parser.add_argument("--alert-email", default="dataarchitectstudio@gmail.com", help="Alert email")
    args = parser.parse_args()

    host = args.host.rstrip("/")
    job_config = build_job_config(args.workspace_path, args.alert_email)

    print("=" * 60)
    print("  Databricks Job Configuration")
    print("=" * 60)
    print(f"  Host       : {host}")
    print(f"  Job Name   : {JOB_NAME}")
    print(f"  Tasks      : {len(job_config['tasks'])}")
    print(f"  Alert Email: {args.alert_email}")
    print("-" * 60)

    # Check if job already exists
    existing_job_id = find_existing_job(host, args.token)

    if existing_job_id:
        # Update existing job
        print(f"\n  Existing job found (ID: {existing_job_id}) — updating...")
        payload = {"job_id": existing_job_id, "new_settings": job_config}
        result = api_request(host, args.token, "POST", "/jobs/reset", payload)
        if "error" in result:
            print(f"  ✗ Failed to update job: {result['error']}")
            sys.exit(1)
        print(f"  ✓ Job updated successfully (ID: {existing_job_id})")
        job_id = existing_job_id
    else:
        # Create new job
        print("\n  No existing job found — creating new...")
        result = api_request(host, args.token, "POST", "/jobs/create", job_config)
        if "error" in result:
            print(f"  ✗ Failed to create job: {result['error']}")
            sys.exit(1)
        job_id = result.get("job_id")
        print(f"  ✓ Job created successfully (ID: {job_id})")

    # Print job URL
    job_url = f"{host}/#job/{job_id}"
    print(f"\n  Job URL: {job_url}")

    print("\n" + "=" * 60)
    print("  Task Chain:")
    for i, task in enumerate(job_config["tasks"]):
        arrow = "  →  " if i > 0 else "     "
        print(f"  {arrow}{task['task_key']}")
    print("=" * 60)


if __name__ == "__main__":
    main()
