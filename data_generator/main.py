"""
Zomato Analytics — Data Generation Entrypoint
Orchestrates generation of all synthetic datasets and writes them to disk.

Usage:
    python -m data_generator.main [--format parquet|csv|json] [--output-dir ./data/raw]
"""

import argparse
import logging
import os
import time

import pandas as pd

from .config import OUTPUT_DIR, OUTPUT_FORMAT, DataVolumeConfig, ZomatoDataConfig
from .generators import (
    generate_customers,
    generate_deliveries,
    generate_menu_items,
    generate_orders,
    generate_restaurants,
    generate_reviews,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("zomato_data_generator")


def _write_dataset(df: pd.DataFrame, name: str, output_dir: str, fmt: str) -> str:
    """Write a DataFrame to disk in the specified format. Returns the output path."""
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{name}.{fmt}")

    if fmt == "parquet":
        df.to_parquet(path, index=False, engine="pyarrow")
    elif fmt == "csv":
        df.to_csv(path, index=False)
    elif fmt == "json":
        df.to_json(path, orient="records", lines=True)
    else:
        raise ValueError(f"Unsupported format: {fmt}")

    return path


def run(output_dir: str = OUTPUT_DIR, fmt: str = OUTPUT_FORMAT) -> None:
    """Execute the full data generation pipeline."""
    vol = DataVolumeConfig()
    cfg = ZomatoDataConfig()

    logger.info("=" * 60)
    logger.info("Zomato Analytics — Synthetic Data Generator")
    logger.info("=" * 60)
    logger.info("Volume config: %s", vol)
    logger.info("Output dir   : %s", output_dir)
    logger.info("Output format: %s", fmt)
    logger.info("-" * 60)

    overall_start = time.time()

    # --- Customers ---
    logger.info("[1/6] Generating customers …")
    t0 = time.time()
    customers = generate_customers(vol, cfg)
    df_customers = pd.DataFrame(customers)
    path = _write_dataset(df_customers, "customers", output_dir, fmt)
    logger.info("  ✓ %d customers → %s (%.1fs)", len(df_customers), path, time.time() - t0)

    # --- Restaurants ---
    logger.info("[2/6] Generating restaurants …")
    t0 = time.time()
    restaurants = generate_restaurants(vol, cfg)
    df_restaurants = pd.DataFrame(restaurants)
    path = _write_dataset(df_restaurants, "restaurants", output_dir, fmt)
    logger.info("  ✓ %d restaurants → %s (%.1fs)", len(df_restaurants), path, time.time() - t0)

    # --- Menu Items ---
    logger.info("[3/6] Generating menu items …")
    t0 = time.time()
    menu_items = generate_menu_items(restaurants, vol, cfg)
    df_menu_items = pd.DataFrame(menu_items)
    path = _write_dataset(df_menu_items, "menu_items", output_dir, fmt)
    logger.info("  ✓ %d menu items → %s (%.1fs)", len(df_menu_items), path, time.time() - t0)

    # --- Orders & Order Items ---
    logger.info("[4/6] Generating orders & line items …")
    t0 = time.time()
    orders, order_items = generate_orders(customers, restaurants, menu_items, vol, cfg)
    df_orders = pd.DataFrame(orders)
    df_order_items = pd.DataFrame(order_items)
    path = _write_dataset(df_orders, "orders", output_dir, fmt)
    logger.info("  ✓ %d orders → %s", len(df_orders), path)
    path = _write_dataset(df_order_items, "order_items", output_dir, fmt)
    logger.info("  ✓ %d order items → %s (%.1fs)", len(df_order_items), path, time.time() - t0)

    # --- Deliveries ---
    logger.info("[5/6] Generating deliveries …")
    t0 = time.time()
    deliveries = generate_deliveries(orders, cfg)
    df_deliveries = pd.DataFrame(deliveries)
    path = _write_dataset(df_deliveries, "deliveries", output_dir, fmt)
    logger.info("  ✓ %d deliveries → %s (%.1fs)", len(df_deliveries), path, time.time() - t0)

    # --- Reviews ---
    logger.info("[6/6] Generating reviews …")
    t0 = time.time()
    reviews = generate_reviews(orders, vol, cfg)
    df_reviews = pd.DataFrame(reviews)
    path = _write_dataset(df_reviews, "reviews", output_dir, fmt)
    logger.info("  ✓ %d reviews → %s (%.1fs)", len(df_reviews), path, time.time() - t0)

    elapsed = time.time() - overall_start
    logger.info("-" * 60)
    logger.info("Data generation complete in %.1fs", elapsed)
    logger.info(
        "Total records generated: %d",
        sum(
            [
                len(df_customers),
                len(df_restaurants),
                len(df_menu_items),
                len(df_orders),
                len(df_order_items),
                len(df_deliveries),
                len(df_reviews),
            ]
        ),
    )
    logger.info("=" * 60)


def main() -> None:
    parser = argparse.ArgumentParser(description="Zomato Analytics — Synthetic Data Generator")
    parser.add_argument("--format", choices=["parquet", "csv", "json"], default=OUTPUT_FORMAT)
    parser.add_argument("--output-dir", default=OUTPUT_DIR)
    args = parser.parse_args()
    run(output_dir=args.output_dir, fmt=args.format)


if __name__ == "__main__":
    main()
