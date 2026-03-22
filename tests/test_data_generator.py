"""
Unit tests for the Zomato Analytics data generators.
"""

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


@pytest.fixture
def small_volume():
    """Small volume config for fast unit tests."""
    return DataVolumeConfig(
        num_customers=50,
        num_restaurants=10,
        num_orders=200,
        num_deliveries=180,
        num_reviews=150,
        num_menu_items_per_restaurant=10,
    )


@pytest.fixture
def config():
    return ZomatoDataConfig()


@pytest.fixture
def customers(small_volume, config):
    return generate_customers(small_volume, config)


@pytest.fixture
def restaurants(small_volume, config):
    return generate_restaurants(small_volume, config)


@pytest.fixture
def menu_items(restaurants, small_volume, config):
    return generate_menu_items(restaurants, small_volume, config)


class TestCustomerGenerator:
    def test_generates_correct_count(self, customers, small_volume):
        assert len(customers) == small_volume.num_customers

    def test_customer_has_required_fields(self, customers):
        required = {"customer_id", "first_name", "last_name", "email", "phone", "city"}
        for c in customers[:5]:
            assert required.issubset(c.keys())

    def test_customer_ids_are_unique(self, customers):
        ids = [c["customer_id"] for c in customers]
        assert len(ids) == len(set(ids))

    def test_customer_has_audit_fields(self, customers):
        for c in customers[:5]:
            assert "_ingested_at" in c
            assert "_source_system" in c


class TestRestaurantGenerator:
    def test_generates_correct_count(self, restaurants, small_volume):
        assert len(restaurants) == small_volume.num_restaurants

    def test_restaurant_has_cuisines_list(self, restaurants):
        for r in restaurants[:5]:
            assert isinstance(r["cuisines"], list)
            assert len(r["cuisines"]) >= 1

    def test_rating_in_valid_range(self, restaurants):
        for r in restaurants:
            assert 2.5 <= r["rating"] <= 5.0


class TestMenuItemGenerator:
    def test_generates_items_for_all_restaurants(self, menu_items, restaurants):
        restaurant_ids = {item["restaurant_id"] for item in menu_items}
        expected_ids = {r["restaurant_id"] for r in restaurants}
        assert restaurant_ids == expected_ids

    def test_prices_in_valid_range(self, menu_items, config):
        for item in menu_items[:20]:
            assert config.min_item_price <= item["price"] <= config.max_item_price


class TestOrderGenerator:
    def test_generates_orders_with_items(self, customers, restaurants, menu_items, small_volume, config):
        orders, order_items = generate_orders(customers, restaurants, menu_items, small_volume, config)
        assert len(orders) > 0
        assert len(order_items) > 0

    def test_order_totals_are_positive(self, customers, restaurants, menu_items, small_volume, config):
        orders, _ = generate_orders(customers, restaurants, menu_items, small_volume, config)
        for o in orders[:20]:
            assert o["total_amount"] >= 0


class TestDeliveryGenerator:
    def test_generates_deliveries_for_delivered_orders(self, customers, restaurants, menu_items, small_volume, config):
        orders, _ = generate_orders(customers, restaurants, menu_items, small_volume, config)
        deliveries = generate_deliveries(orders, config)
        assert len(deliveries) > 0

    def test_delivery_time_is_positive(self, customers, restaurants, menu_items, small_volume, config):
        orders, _ = generate_orders(customers, restaurants, menu_items, small_volume, config)
        deliveries = generate_deliveries(orders, config)
        for d in deliveries[:20]:
            assert d["delivery_time_mins"] > 0


class TestReviewGenerator:
    def test_generates_reviews(self, customers, restaurants, menu_items, small_volume, config):
        orders, _ = generate_orders(customers, restaurants, menu_items, small_volume, config)
        reviews = generate_reviews(orders, small_volume, config)
        assert len(reviews) > 0

    def test_ratings_in_valid_range(self, customers, restaurants, menu_items, small_volume, config):
        orders, _ = generate_orders(customers, restaurants, menu_items, small_volume, config)
        reviews = generate_reviews(orders, small_volume, config)
        for r in reviews[:20]:
            assert 1 <= r["rating"] <= 5
