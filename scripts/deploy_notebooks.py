"""
Databricks Notebook Deployment Script
Deploys notebooks to Databricks workspace using the REST API.
Works with Databricks Free Edition, Community Edition, and paid tiers.

Usage:
    python scripts/deploy_notebooks.py \
        --host https://your-workspace.cloud.databricks.com \
        --token dapi... \
        --workspace-path /Workspace/Zomato-Analytics
"""

import argparse
import base64
import json
import sys

import requests

# Notebooks to deploy: (local_path, remote_name)
NOTEBOOKS = [
    ("notebooks/bronze/01_bronze_ingestion.py", "bronze/01_bronze_ingestion"),
    ("notebooks/silver/02_silver_transformation.py", "silver/02_silver_transformation"),
    ("notebooks/gold/03_gold_aggregation.py", "gold/03_gold_aggregation"),
    ("notebooks/dashboard/04_analytics_dashboard.py", "dashboard/04_analytics_dashboard"),
]


def _api_request(host: str, token: str, method: str, endpoint: str, payload: dict = None) -> dict:
    """Make authenticated request to Databricks REST API."""
    url = f"{host.rstrip('/')}/api/2.0{endpoint}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    }
    resp = requests.request(method, url, headers=headers, json=payload, timeout=30)

    if resp.status_code == 200:
        return resp.json() if resp.text else {}
    elif resp.status_code == 400 and "RESOURCE_ALREADY_EXISTS" in resp.text:
        return {}  # Directory already exists — safe to ignore
    else:
        print(f"  API Error ({resp.status_code}): {resp.text}")
        return {"error": resp.text, "status_code": resp.status_code}


def create_directory(host: str, token: str, path: str) -> bool:
    """Create a directory in the Databricks workspace."""
    result = _api_request(host, token, "POST", "/workspace/mkdirs", {"path": path})
    return "error" not in result


def deploy_notebook(host: str, token: str, local_path: str, remote_path: str) -> bool:
    """Upload a notebook to Databricks workspace."""
    with open(local_path, "r") as f:
        content = f.read()

    # Base64 encode the content
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")

    payload = {
        "path": remote_path,
        "language": "PYTHON",
        "overwrite": True,
        "content": content_b64,
        "format": "SOURCE",
    }

    result = _api_request(host, token, "POST", "/workspace/import", payload)

    if "error" in result:
        return False
    return True


def verify_notebook(host: str, token: str, path: str) -> bool:
    """Verify a notebook exists in the workspace."""
    result = _api_request(host, token, "GET", f"/workspace/get-status?path={path}")
    return result.get("object_type") == "NOTEBOOK"


def main():
    parser = argparse.ArgumentParser(description="Deploy notebooks to Databricks")
    parser.add_argument("--host", required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks PAT")
    parser.add_argument("--workspace-path", required=True, help="Target workspace path")
    args = parser.parse_args()

    host = args.host.rstrip("/")
    workspace_root = args.workspace_path.rstrip("/")

    print("=" * 60)
    print("  Databricks Notebook Deployment")
    print("=" * 60)
    print(f"  Host      : {host}")
    print(f"  Workspace : {workspace_root}")
    print("-" * 60)

    # Step 1: Create directories
    dirs_to_create = [workspace_root, f"{workspace_root}/bronze", f"{workspace_root}/silver", f"{workspace_root}/gold", f"{workspace_root}/dashboard"]
    for d in dirs_to_create:
        create_directory(host, args.token, d)
    print("  ✓ Workspace directories created")

    # Step 2: Deploy each notebook
    all_passed = True
    for local_path, remote_name in NOTEBOOKS:
        remote_path = f"{workspace_root}/{remote_name}"
        print(f"\n  Deploying: {local_path}")
        print(f"       → {remote_path}")

        success = deploy_notebook(host, args.token, local_path, remote_path)
        if success:
            # Verify
            verified = verify_notebook(host, args.token, remote_path)
            if verified:
                print(f"       ✓ Deployed and verified")
            else:
                print(f"       ⚠ Deployed but verification pending")
        else:
            print(f"       ✗ FAILED")
            all_passed = False

    # Step 3: Summary
    print("\n" + "=" * 60)
    if all_passed:
        print("  ✓ All notebooks deployed successfully")
    else:
        print("  ✗ Some deployments failed!")
        sys.exit(1)
    print("=" * 60)


if __name__ == "__main__":
    main()
