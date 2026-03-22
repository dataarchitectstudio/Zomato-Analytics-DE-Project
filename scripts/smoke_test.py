"""
Post-deployment smoke test for Databricks workspace.
Verifies that all expected notebooks exist after deployment.
"""

import argparse
import sys

import requests


def verify_notebook_exists(host: str, token: str, path: str) -> bool:
    """Check if a notebook exists in the Databricks workspace."""
    url = f"{host.rstrip('/')}/api/2.0/workspace/get-status"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(url, headers=headers, json={"path": path}, timeout=30)

    if response.status_code == 200:
        data = response.json()
        return data.get("object_type") == "NOTEBOOK"
    return False


def main():
    parser = argparse.ArgumentParser(description="Databricks deployment smoke test")
    parser.add_argument("--host", required=True, help="Databricks workspace URL")
    parser.add_argument("--token", required=True, help="Databricks API token")
    parser.add_argument("--workspace-path", required=True, help="Base workspace path")
    args = parser.parse_args()

    expected_notebooks = [
        f"{args.workspace_path}/bronze/01_bronze_ingestion",
        f"{args.workspace_path}/silver/02_silver_transformation",
        f"{args.workspace_path}/gold/03_gold_aggregation",
        f"{args.workspace_path}/dashboard/04_analytics_dashboard",
    ]

    print("=" * 50)
    print("  Databricks Deployment Smoke Test")
    print("=" * 50)

    all_passed = True
    for notebook_path in expected_notebooks:
        exists = verify_notebook_exists(args.host, args.token, notebook_path)
        status = "PASS" if exists else "FAIL"
        icon = "✓" if exists else "✗"
        print(f"  [{icon}] {notebook_path}: {status}")
        if not exists:
            all_passed = False

    print("=" * 50)
    if all_passed:
        print("🟢 All smoke tests passed.")
        sys.exit(0)
    else:
        print("🔴 Some smoke tests failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
