"""
Unit tests for the Zomato Analytics data generators.
Validates correctness, referential integrity, domain constraints, and audit fields
for all six generated entities: customers, restaurants, menu items, orders,
deliveries, and reviews.
"""

from datetime import datetime


class TestCustomerGenerator:
    def test_generates_correct_count(self, customers, small_volume):
        assert len(customers) == small_volume.num_customers

    def test_customer_has_required_fields(self, customers):
        required = {
            "customer_id", "first_name", "last_name", "email", "phone",
            "city", "subscription_tier", "is_active", "preferred_payment",
            "preferred_cuisine", "_ingested_at", "_source_system",
        }
        for c in customers[:5]:
            assert required.issubset(c.keys())

    def test_customer_ids_are_unique(self, customers):
        ids = [c["customer_id"] for c in customers]
        assert len(ids) == len(set(ids))

    def test_customer_id_prefix(self, customers):
        for c in customers[:10]:
            assert c["customer_id"].startswith("CUST_")

    def test_customer_has_audit_fields(self, customers):
        for c in customers[:5]:
            assert "_ingested_at" in c
            assert "_source_system" in c
            assert c["_source_system"] == "zomato_app_backend"

    def test_email_is_lowercase(self, customers):
        for c in customers[:20]:
            assert c["email"] == c["email"].lower()

    def test_email_contains_at_symbol(self, customers):
        for c in customers[:20]:
            assert "@" in c["email"]

    def test_subscription_tier_valid(self, customers, config):
        valid_tiers = set(config.subscription_tiers)
        for c in customers:
            assert c["subscription_tier"] in valid_tiers

    def test_city_in_known_cities(self, customers, config):
        valid_cities = set(config.cities)
        for c in customers:
            assert c["city"] in valid_cities

    def test_preferred_payment_valid(self, customers, config):
        valid_payments = set(config.payment_methods)
        for c in customers:
            assert c["preferred_payment"] in valid_payments

    def test_preferred_cuisine_valid(self, customers, config):
        valid_cuisines = set(config.cuisines)
        for c in customers:
            assert c["preferred_cuisine"] in valid_cuisines

    def test_signup_date_is_valid_iso(self, customers):
        for c in customers[:10]:
            dt = datetime.fromisoformat(c["signup_date"])
            assert dt is not None

    def test_avg_order_value_is_positive(self, customers):
        for c in customers:
            assert c["avg_order_value"] >= 0

    def test_total_orders_lifetime_non_negative(self, customers):
        for c in customers:
            assert c["total_orders_lifetime"] >= 0

    def test_is_active_is_boolean(self, customers):
        for c in customers[:20]:
            assert isinstance(c["is_active"], bool)

    def test_coordinates_in_valid_range(self, customers):
        for c in customers[:10]:
            assert -90 <= c["latitude"] <= 90
            assert -180 <= c["longitude"] <= 180


class TestRestaurantGenerator:
    def test_generates_correct_count(self, restaurants, small_volume):
        assert len(restaurants) == small_volume.num_restaurants

    def test_restaurant_id_prefix(self, restaurants):
        for r in restaurants[:10]:
            assert r["restaurant_id"].startswith("REST_")

    def test_restaurant_ids_are_unique(self, restaurants):
        ids = [r["restaurant_id"] for r in restaurants]
        assert len(ids) == len(set(ids))

    def test_restaurant_has_cuisines_list(self, restaurants):
        for r in restaurants[:5]:
            assert isinstance(r["cuisines"], list)
            assert len(r["cuisines"]) >= 1

    def test_cuisines_are_valid(self, restaurants, config):
        valid_cuisines = set(config.cuisines)
        for r in restaurants:
            for cuisine in r["cuisines"]:
                assert cuisine in valid_cuisines

    def test_rating_in_valid_range(self, restaurants):
        for r in restaurants:
            assert 2.5 <= r["rating"] <= 5.0

    def test_rating_has_one_decimal_precision(self, restaurants):
        for r in restaurants:
            assert round(r["rating"], 1) == r["rating"]

    def test_restaurant_type_valid(self, restaurants, config):
        valid_types = set(config.restaurant_types)
        for r in restaurants:
            assert r["restaurant_type"] in valid_types

    def test_city_in_known_cities(self, restaurants, config):
        valid_cities = set(config.cities)
        for r in restaurants:
            assert r["city"] in valid_cities

    def test_source_system(self, restaurants):
        for r in restaurants[:5]:
            assert r["_source_system"] == "restaurant_onboarding_svc"

    def test_total_reviews_positive(self, restaurants):
        for r in restaurants:
            assert r["total_reviews"] >= 10

    def test_boolean_flags(self, restaurants):
        for r in restaurants[:10]:
            assert isinstance(r["is_delivering_now"], bool)
            assert isinstance(r["has_online_delivery"], bool)
            assert isinstance(r["has_table_booking"], bool)
            assert isinstance(r["is_premium_partner"], bool)

    def test_onboarded_date_is_valid_iso(self, restaurants):
        for r in restaurants[:10]:
            dt = datetime.fromisoformat(r["onboarded_date"])
            assert dt is not None


class TestMenuItemGenerator:
    def test_generates_items_for_all_restaurants(self, menu_items, restaurants):
        restaurant_ids_in_items = {item["restaurant_id"] for item in menu_items}
        expected_ids = {r["restaurant_id"] for r in restaurants}
        assert restaurant_ids_in_items == expected_ids

    def test_prices_in_valid_range(self, menu_items, config):
        for item in menu_items[:20]:
            assert config.min_item_price <= item["price"] <= config.max_item_price

    def test_item_id_not_null(self, menu_items):
        for item in menu_items[:20]:
            assert item["item_id"] is not None
            assert item["item_id"] != ""

    def test_veg_nonveg_valid(self, menu_items):
        valid = {"Veg", "Non-Veg", "Egg"}
        for item in menu_items[:20]:
            assert item["veg_nonveg"] in valid

    def test_is_available_is_boolean(self, menu_items):
        for item in menu_items[:20]:
            assert isinstance(item["is_available"], bool)

    def test_is_bestseller_is_boolean(self, menu_items):
        for item in menu_items[:20]:
            assert isinstance(item["is_bestseller"], bool)

    def test_preparation_time_valid(self, menu_items):
        valid_times = {10, 15, 20, 25, 30, 40, 45}
        for item in menu_items[:20]:
            assert item["preparation_time_mins"] in valid_times


class TestOrderGenerator:
    def test_generates_orders_with_items(self, orders, order_items):
        assert len(orders) > 0
        assert len(order_items) > 0

    def test_order_id_prefix(self, orders):
        for o in orders[:10]:
            assert o["order_id"].startswith("ORD_")

    def test_order_ids_are_unique(self, orders):
        ids = [o["order_id"] for o in orders]
        assert len(ids) == len(set(ids))

    def test_order_totals_are_positive(self, orders):
        for o in orders[:20]:
            assert o["total_amount"] >= 0

    def test_order_status_valid(self, orders, config):
        valid_statuses = set(config.order_statuses)
        for o in orders:
            assert o["order_status"] in valid_statuses

    def test_payment_method_valid(self, orders, config):
        valid_methods = set(config.payment_methods)
        for o in orders[:20]:
            assert o["payment_method"] in valid_methods

    def test_platform_valid(self, orders, config):
        valid_platforms = set(config.order_platforms)
        for o in orders[:20]:
            assert o["platform"] in valid_platforms

    def test_referential_integrity_customer(self, orders, customers):
        customer_ids = {c["customer_id"] for c in customers}
        for o in orders[:20]:
            assert o["customer_id"] in customer_ids

    def test_referential_integrity_restaurant(self, orders, restaurants):
        restaurant_ids = {r["restaurant_id"] for r in restaurants}
        for o in orders[:20]:
            assert o["restaurant_id"] in restaurant_ids

    def test_order_items_link_to_orders(self, orders, order_items):
        order_ids = {o["order_id"] for o in orders}
        for item in order_items[:20]:
            assert item["order_id"] in order_ids

    def test_subtotal_matches_line_items(self, orders, order_items):
        order_line_totals: dict = {}
        for item in order_items:
            order_line_totals.setdefault(item["order_id"], 0.0)
            order_line_totals[item["order_id"]] += item["line_total"]

        for o in orders[:5]:
            if o["order_id"] in order_line_totals:
                expected = round(order_line_totals[o["order_id"]], 2)
                assert abs(o["subtotal"] - expected) < 0.02

    def test_discount_amount_non_negative(self, orders):
        for o in orders[:20]:
            assert o["discount_amount"] >= 0

    def test_tax_amount_non_negative(self, orders):
        for o in orders[:20]:
            assert o["tax_amount"] >= 0

    def test_delivered_orders_have_completed_payment(self, orders):
        for o in orders:
            if o["order_status"] == "Delivered":
                assert o["payment_status"] == "Completed"

    def test_source_system(self, orders):
        for o in orders[:5]:
            assert o["_source_system"] == "order_management_svc"


class TestDeliveryGenerator:
    def test_generates_deliveries_for_delivered_orders(self, deliveries):
        assert len(deliveries) > 0

    def test_delivery_id_prefix(self, deliveries):
        for d in deliveries[:10]:
            assert d["delivery_id"].startswith("DLV_")

    def test_delivery_ids_are_unique(self, deliveries):
        ids = [d["delivery_id"] for d in deliveries]
        assert len(ids) == len(set(ids))

    def test_delivery_time_is_positive(self, deliveries):
        for d in deliveries[:20]:
            assert d["delivery_time_mins"] > 0

    def test_delivery_time_in_expected_range(self, deliveries):
        for d in deliveries[:20]:
            assert 15 <= d["delivery_time_mins"] <= 60

    def test_vehicle_type_valid(self, deliveries):
        valid = {"Bike", "Scooter", "Bicycle", "Car"}
        for d in deliveries[:20]:
            assert d["vehicle_type"] in valid

    def test_delivery_status_valid(self, deliveries, config):
        valid_statuses = set(config.delivery_statuses)
        for d in deliveries[:20]:
            assert d["delivery_status"] in valid_statuses

    def test_delivery_links_to_order(self, deliveries, orders):
        order_ids = {o["order_id"] for o in orders}
        for d in deliveries[:20]:
            assert d["order_id"] in order_ids

    def test_only_for_deliverable_orders(self, orders, deliveries):
        deliverable_statuses = {"Out for Delivery", "Delivered"}
        delivery_order_ids = {d["order_id"] for d in deliveries}
        for o in orders:
            if o["order_id"] in delivery_order_ids:
                assert o["order_status"] in deliverable_statuses

    def test_delivery_distance_positive(self, deliveries):
        for d in deliveries[:20]:
            assert d["delivery_distance_km"] > 0

    def test_source_system(self, deliveries):
        for d in deliveries[:5]:
            assert d["_source_system"] == "logistics_svc"

    def test_delivery_rating_valid_or_null(self, deliveries):
        for d in deliveries[:20]:
            if d["delivery_rating"] is not None:
                assert 1 <= d["delivery_rating"] <= 5


class TestReviewGenerator:
    def test_generates_reviews(self, reviews):
        assert len(reviews) > 0

    def test_review_id_prefix(self, reviews):
        for r in reviews[:10]:
            assert r["review_id"].startswith("REV_")

    def test_ratings_in_valid_range(self, reviews):
        for r in reviews[:20]:
            assert 1 <= r["rating"] <= 5

    def test_ratings_are_integers(self, reviews):
        for r in reviews[:20]:
            assert isinstance(r["rating"], int)

    def test_sentiment_label_correct(self, reviews):
        for r in reviews[:20]:
            if r["rating"] >= 4:
                assert r["sentiment_label"] == "Positive"
            elif r["rating"] <= 2:
                assert r["sentiment_label"] == "Negative"
            else:
                assert r["sentiment_label"] == "Neutral"

    def test_is_verified_order_always_true(self, reviews):
        for r in reviews:
            assert r["is_verified_order"] is True

    def test_review_only_from_delivered_orders(self, orders, reviews):
        delivered_ids = {o["order_id"] for o in orders if o["order_status"] == "Delivered"}
        for r in reviews:
            assert r["order_id"] in delivered_ids

    def test_upvotes_non_negative(self, reviews):
        for r in reviews[:20]:
            assert r["upvotes"] >= 0

    def test_has_photo_is_boolean(self, reviews):
        for r in reviews[:20]:
            assert isinstance(r["has_photo"], bool)

    def test_source_system(self, reviews):
        for r in reviews[:5]:
            assert r["_source_system"] == "review_svc"

    def test_review_text_not_empty(self, reviews):
        for r in reviews[:20]:
            assert r["review_text"] is not None
            assert len(r["review_text"].strip()) > 0
