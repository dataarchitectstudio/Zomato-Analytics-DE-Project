"""Shared fixtures for the Zomato Analytics test suite."""

import pytest

from data_generator.config import DataVolumeConfig, ZomatoDataConfig
from data_generator.generators import (
    generate_customers,
    generate_deliveries,
    generate_menu_items,
    generate_orders,
    generate_restaurants,
    generate_reviews,
)


@pytest.fixture(scope="session")
def spark():
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("zomato-unit-tests")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    return session


@pytest.fixture(scope="session")
def small_volume():
    return DataVolumeConfig(
        num_customers=100,
        num_restaurants=20,
        num_orders=500,
        num_deliveries=400,
        num_reviews=300,
        num_menu_items_per_restaurant=15,
    )


@pytest.fixture(scope="session")
def config():
    return ZomatoDataConfig()


@pytest.fixture(scope="session")
def customers(small_volume, config):
    return generate_customers(small_volume, config)


@pytest.fixture(scope="session")
def restaurants(small_volume, config):
    return generate_restaurants(small_volume, config)


@pytest.fixture(scope="session")
def menu_items(restaurants, small_volume, config):
    return generate_menu_items(restaurants, small_volume, config)


@pytest.fixture(scope="session")
def orders_and_items(customers, restaurants, menu_items, small_volume, config):
    return generate_orders(customers, restaurants, menu_items, small_volume, config)


@pytest.fixture(scope="session")
def orders(orders_and_items):
    return orders_and_items[0]


@pytest.fixture(scope="session")
def order_items(orders_and_items):
    return orders_and_items[1]


@pytest.fixture(scope="session")
def deliveries(orders, config):
    return generate_deliveries(orders, config)


@pytest.fixture(scope="session")
def reviews(orders, small_volume, config):
    return generate_reviews(orders, small_volume, config)
