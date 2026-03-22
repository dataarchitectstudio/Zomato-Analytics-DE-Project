"""
Zomato Analytics — Pipeline Configuration
Centralized configuration for the Medallion Architecture pipeline.

Uses Unity Catalog with a DEDICATED catalog (`zomato_analytics`) to ensure
complete isolation from existing workspace objects. All tables live under:

    zomato_analytics.bronze.*
    zomato_analytics.silver.*
    zomato_analytics.gold.*

No existing catalogs, schemas, or tables are modified.
"""

import os
from dataclasses import dataclass
from typing import Dict

# ---------------------------------------------------------------------------
# Unity Catalog — Dedicated catalog name (change ONLY this to rename)
# ---------------------------------------------------------------------------
UNITY_CATALOG_NAME = "zomato_analytics"


@dataclass
class EnvironmentConfig:
    """Environment-specific pipeline settings."""

    env_name: str
    catalog: str
    bronze_schema: str
    silver_schema: str
    gold_schema: str
    raw_data_path: str
    checkpoint_path: str
    log_level: str = "INFO"

    @property
    def bronze_fqn(self) -> str:
        """Fully qualified namespace: catalog.schema"""
        return f"{self.catalog}.{self.bronze_schema}"

    @property
    def silver_fqn(self) -> str:
        return f"{self.catalog}.{self.silver_schema}"

    @property
    def gold_fqn(self) -> str:
        return f"{self.catalog}.{self.gold_schema}"


# Environment configurations
ENVIRONMENTS: Dict[str, EnvironmentConfig] = {
    "dev": EnvironmentConfig(
        env_name="dev",
        catalog=UNITY_CATALOG_NAME,
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        raw_data_path=f"/Volumes/{UNITY_CATALOG_NAME}/raw/landing",
        checkpoint_path=f"/Volumes/{UNITY_CATALOG_NAME}/raw/checkpoints",
        log_level="DEBUG",
    ),
    "staging": EnvironmentConfig(
        env_name="staging",
        catalog=f"{UNITY_CATALOG_NAME}_staging",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        raw_data_path=f"/Volumes/{UNITY_CATALOG_NAME}_staging/raw/landing",
        checkpoint_path=f"/Volumes/{UNITY_CATALOG_NAME}_staging/raw/checkpoints",
        log_level="INFO",
    ),
    "prod": EnvironmentConfig(
        env_name="prod",
        catalog=f"{UNITY_CATALOG_NAME}_prod",
        bronze_schema="bronze",
        silver_schema="silver",
        gold_schema="gold",
        raw_data_path=f"/Volumes/{UNITY_CATALOG_NAME}_prod/raw/landing",
        checkpoint_path=f"/Volumes/{UNITY_CATALOG_NAME}_prod/raw/checkpoints",
        log_level="WARNING",
    ),
}


def get_config(env: str = None) -> EnvironmentConfig:
    """Get configuration for the specified environment."""
    env = env or os.getenv("PIPELINE_ENV", "dev")
    if env not in ENVIRONMENTS:
        raise ValueError(f"Unknown environment: {env}. Valid: {list(ENVIRONMENTS.keys())}")
    return ENVIRONMENTS[env]


# Data quality thresholds
DQ_THRESHOLDS = {
    "min_pass_rate_pct": 95.0,
    "max_null_pct": 5.0,
    "max_duplicate_pct": 1.0,
    "min_row_count": {
        "customers": 1000,
        "restaurants": 100,
        "orders": 10000,
        "deliveries": 5000,
        "reviews": 5000,
    },
}

# Pipeline scheduling
PIPELINE_SCHEDULE = {
    "bronze_ingestion": "0 */15 * * *",
    "silver_transformation": "5 */1 * * *",
    "gold_aggregation": "30 */2 * * *",
    "dashboard_refresh": "0 6,12,18 * * *",
}
