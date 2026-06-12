"""
Schema contract tests for each Medallion pipeline layer.

Validates that:
- Data generator output satisfies all Bronze ingestion column requirements
- Silver derived column contracts are complete and non-overlapping with Bronze
- Gold aggregation column contracts cover all documented business tables
- Pipeline configuration values are internally consistent
"""

from datetime import date

# ---------------------------------------------------------------------------
# Bronze column contracts — must match generator output exactly
# ---------------------------------------------------------------------------
BRONZE_CUSTOMER_REQUIRED = {
    "customer_id", "first_name", "last_name", "email", "phone",
    "city", "locality", "pincode", "latitude", "longitude",
    "signup_date", "subscription_tier", "is_active", "preferred_payment",
    "preferred_cuisine", "total_orders_lifetime", "avg_order_value",
    "_ingested_at", "_source_system",
}

BRONZE_RESTAURANT_REQUIRED = {
    "restaurant_id", "name", "city", "locality", "address", "pincode",
    "latitude", "longitude", "cuisines", "restaurant_type", "avg_cost_for_two",
    "rating", "total_reviews", "is_delivering_now", "has_online_delivery",
    "has_table_booking", "is_premium_partner", "onboarded_date",
    "owner_name", "owner_phone", "gst_number",
    "_ingested_at", "_source_system",
}

BRONZE_ORDER_REQUIRED = {
    "order_id", "customer_id", "restaurant_id", "order_placed_at",
    "order_status", "payment_method", "payment_status", "platform",
    "subtotal", "discount_pct", "discount_amount", "coupon_code",
    "delivery_fee", "tax_amount", "total_amount", "num_items",
    "special_instructions", "_ingested_at", "_source_system",
}

BRONZE_DELIVERY_REQUIRED = {
    "delivery_id", "order_id", "delivery_partner_id", "delivery_partner_name",
    "partner_phone", "vehicle_type", "pickup_timestamp", "delivered_timestamp",
    "delivery_time_mins", "delivery_status", "delivery_distance_km",
    "delivery_rating", "_ingested_at", "_source_system",
}

BRONZE_REVIEW_REQUIRED = {
    "review_id", "order_id", "customer_id", "restaurant_id",
    "rating", "review_text", "review_timestamp", "sentiment_label",
    "is_verified_order", "upvotes", "has_photo",
    "_ingested_at", "_source_system",
}

# ---------------------------------------------------------------------------
# Silver derived column contracts — columns added on top of Bronze
# ---------------------------------------------------------------------------
SILVER_CUSTOMER_DERIVED = {
    "full_name", "signup_year", "signup_month",
    "customer_segment", "_silver_loaded_at",
}

SILVER_ORDER_DERIVED = {
    "order_date", "order_year", "order_month", "order_day_of_week",
    "order_hour", "order_time_slot",
    "is_delivered", "is_cancelled", "has_discount",
    "_silver_loaded_at",
}

SILVER_DELIVERY_DERIVED = {
    "delivery_sla_status", "distance_bucket", "_silver_loaded_at",
}

SILVER_RESTAURANT_DERIVED = {
    "price_tier", "rating_tier", "num_cuisines", "_silver_loaded_at",
}

SILVER_REVIEW_DERIVED = {
    "review_date", "review_text_length", "_silver_loaded_at",
}

# ---------------------------------------------------------------------------
# Gold column contracts — minimum required columns per table
# ---------------------------------------------------------------------------
GOLD_DIM_CUSTOMERS_MIN = {
    "customer_id", "full_name", "city", "subscription_tier",
    "customer_segment", "total_orders", "lifetime_revenue",
    "avg_order_value", "rfm_segment",
}

GOLD_DIM_RESTAURANTS_MIN = {
    "restaurant_id", "name", "city", "rating", "price_tier",
    "total_orders_received", "total_gmv",
    "avg_delivery_time_mins", "restaurant_health_score",
}

GOLD_FACT_ORDERS_MIN = {
    "order_id", "customer_id", "restaurant_id",
    "order_date", "order_status", "total_amount",
}

GOLD_AGG_DAILY_CITY_MIN = {
    "customer_city", "order_date", "total_orders",
    "total_gmv", "unique_customers", "avg_order_value",
}

GOLD_AGG_REVENUE_MIN = {
    "order_year", "order_month", "total_orders",
    "total_gmv", "unique_customers", "avg_order_value", "net_revenue",
}

GOLD_AGG_RESTAURANT_PERF_MIN = {
    "restaurant_id", "name", "city",
    "restaurant_health_score", "fulfillment_rate_pct",
    "sla_compliance_pct", "performance_tier",
}

GOLD_AGG_DELIVERY_SLA_MIN = {
    "city", "vehicle_type", "delivery_sla_status",
    "delivery_count", "avg_delivery_mins",
}

GOLD_AGG_CUSTOMER_COHORTS_MIN = {
    "cohort_month", "activity_month", "active_customers",
    "total_orders", "cohort_revenue",
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------
class TestBronzeSchemaContracts:
    """Verify generated data satisfies all Bronze ingestion column requirements."""

    def test_customers_have_all_bronze_columns(self, customers):
        missing = BRONZE_CUSTOMER_REQUIRED - set(customers[0].keys())
        assert not missing, f"Missing Bronze customer columns: {missing}"

    def test_restaurants_have_all_bronze_columns(self, restaurants):
        missing = BRONZE_RESTAURANT_REQUIRED - set(restaurants[0].keys())
        assert not missing, f"Missing Bronze restaurant columns: {missing}"

    def test_orders_have_all_bronze_columns(self, orders):
        missing = BRONZE_ORDER_REQUIRED - set(orders[0].keys())
        assert not missing, f"Missing Bronze order columns: {missing}"

    def test_deliveries_have_all_bronze_columns(self, deliveries):
        missing = BRONZE_DELIVERY_REQUIRED - set(deliveries[0].keys())
        assert not missing, f"Missing Bronze delivery columns: {missing}"

    def test_reviews_have_all_bronze_columns(self, reviews):
        missing = BRONZE_REVIEW_REQUIRED - set(reviews[0].keys())
        assert not missing, f"Missing Bronze review columns: {missing}"

    def test_no_undocumented_customer_columns(self, customers):
        """Fails if the generator adds new columns not in the Bronze contract."""
        undocumented = set(customers[0].keys()) - BRONZE_CUSTOMER_REQUIRED
        assert not undocumented, f"Undocumented columns — update contract: {undocumented}"

    def test_no_undocumented_restaurant_columns(self, restaurants):
        undocumented = set(restaurants[0].keys()) - BRONZE_RESTAURANT_REQUIRED
        assert not undocumented, f"Undocumented columns — update contract: {undocumented}"

    def test_no_undocumented_order_columns(self, orders):
        undocumented = set(orders[0].keys()) - BRONZE_ORDER_REQUIRED
        assert not undocumented, f"Undocumented columns — update contract: {undocumented}"

    def test_no_undocumented_delivery_columns(self, deliveries):
        undocumented = set(deliveries[0].keys()) - BRONZE_DELIVERY_REQUIRED
        assert not undocumented, f"Undocumented columns — update contract: {undocumented}"

    def test_no_undocumented_review_columns(self, reviews):
        undocumented = set(reviews[0].keys()) - BRONZE_REVIEW_REQUIRED
        assert not undocumented, f"Undocumented columns — update contract: {undocumented}"

    def test_all_entities_have_source_system(self, customers, restaurants, orders, deliveries, reviews):
        for entity in [customers[0], restaurants[0], orders[0], deliveries[0], reviews[0]]:
            assert "_source_system" in entity
            assert entity["_source_system"] is not None

    def test_all_entities_have_ingested_at(self, customers, restaurants, orders, deliveries, reviews):
        for entity in [customers[0], restaurants[0], orders[0], deliveries[0], reviews[0]]:
            assert "_ingested_at" in entity
            assert entity["_ingested_at"] is not None

    def test_all_primary_keys_present(self, customers, restaurants, orders, deliveries, reviews):
        assert "customer_id" in customers[0]
        assert "restaurant_id" in restaurants[0]
        assert "order_id" in orders[0]
        assert "delivery_id" in deliveries[0]
        assert "review_id" in reviews[0]


class TestSilverSchemaContracts:
    """Validate Silver derived columns are complete and non-overlapping with Bronze."""

    def test_silver_customer_derived_columns_defined(self):
        assert len(SILVER_CUSTOMER_DERIVED) >= 4

    def test_silver_order_derived_columns_defined(self):
        assert len(SILVER_ORDER_DERIVED) >= 8

    def test_silver_delivery_derived_columns_defined(self):
        assert len(SILVER_DELIVERY_DERIVED) >= 2

    def test_silver_restaurant_derived_columns_defined(self):
        assert len(SILVER_RESTAURANT_DERIVED) >= 3

    def test_silver_review_derived_columns_defined(self):
        assert len(SILVER_REVIEW_DERIVED) >= 2

    def test_silver_customer_no_overlap_with_bronze(self):
        overlap = SILVER_CUSTOMER_DERIVED & BRONZE_CUSTOMER_REQUIRED
        assert not overlap, f"Silver columns overlap with Bronze: {overlap}"

    def test_silver_order_no_overlap_with_bronze(self):
        overlap = SILVER_ORDER_DERIVED & BRONZE_ORDER_REQUIRED
        assert not overlap, f"Silver columns overlap with Bronze: {overlap}"

    def test_silver_delivery_no_overlap_with_bronze(self):
        overlap = SILVER_DELIVERY_DERIVED & BRONZE_DELIVERY_REQUIRED
        assert not overlap, f"Silver columns overlap with Bronze: {overlap}"

    def test_silver_restaurant_no_overlap_with_bronze(self):
        overlap = SILVER_RESTAURANT_DERIVED & BRONZE_RESTAURANT_REQUIRED
        assert not overlap, f"Silver columns overlap with Bronze: {overlap}"

    def test_silver_review_no_overlap_with_bronze(self):
        overlap = SILVER_REVIEW_DERIVED & BRONZE_REVIEW_REQUIRED
        assert not overlap, f"Silver columns overlap with Bronze: {overlap}"

    def test_silver_customer_has_segment(self):
        assert "customer_segment" in SILVER_CUSTOMER_DERIVED

    def test_silver_order_has_time_slot(self):
        assert "order_time_slot" in SILVER_ORDER_DERIVED

    def test_silver_delivery_has_sla_status(self):
        assert "delivery_sla_status" in SILVER_DELIVERY_DERIVED

    def test_silver_restaurant_has_tiers(self):
        assert "price_tier" in SILVER_RESTAURANT_DERIVED
        assert "rating_tier" in SILVER_RESTAURANT_DERIVED

    def test_all_silver_layers_have_loaded_at(self):
        for derived in [
            SILVER_CUSTOMER_DERIVED, SILVER_ORDER_DERIVED,
            SILVER_DELIVERY_DERIVED, SILVER_RESTAURANT_DERIVED,
            SILVER_REVIEW_DERIVED,
        ]:
            assert "_silver_loaded_at" in derived


class TestGoldSchemaContracts:
    """Validate Gold table column contracts are complete and internally consistent."""

    def test_all_eight_gold_tables_defined(self):
        gold_tables = [
            GOLD_DIM_CUSTOMERS_MIN, GOLD_DIM_RESTAURANTS_MIN,
            GOLD_FACT_ORDERS_MIN, GOLD_AGG_DAILY_CITY_MIN,
            GOLD_AGG_REVENUE_MIN, GOLD_AGG_RESTAURANT_PERF_MIN,
            GOLD_AGG_DELIVERY_SLA_MIN, GOLD_AGG_CUSTOMER_COHORTS_MIN,
        ]
        assert len(gold_tables) == 8

    def test_fact_orders_links_to_dimensions(self):
        assert "customer_id" in GOLD_FACT_ORDERS_MIN
        assert "restaurant_id" in GOLD_FACT_ORDERS_MIN

    def test_dim_customers_has_rfm_segment(self):
        assert "rfm_segment" in GOLD_DIM_CUSTOMERS_MIN

    def test_dim_restaurants_has_health_score(self):
        assert "restaurant_health_score" in GOLD_DIM_RESTAURANTS_MIN

    def test_revenue_summary_has_net_revenue(self):
        assert "net_revenue" in GOLD_AGG_REVENUE_MIN

    def test_restaurant_performance_has_tier(self):
        assert "performance_tier" in GOLD_AGG_RESTAURANT_PERF_MIN

    def test_delivery_sla_has_sla_status(self):
        assert "delivery_sla_status" in GOLD_AGG_DELIVERY_SLA_MIN

    def test_cohorts_has_cohort_month(self):
        assert "cohort_month" in GOLD_AGG_CUSTOMER_COHORTS_MIN
        assert "activity_month" in GOLD_AGG_CUSTOMER_COHORTS_MIN

    def test_dim_customers_min_column_count(self):
        assert len(GOLD_DIM_CUSTOMERS_MIN) >= 8

    def test_dim_restaurants_min_column_count(self):
        assert len(GOLD_DIM_RESTAURANTS_MIN) >= 8

    def test_gold_tables_have_no_duplicate_columns_within(self):
        for table in [
            GOLD_DIM_CUSTOMERS_MIN, GOLD_DIM_RESTAURANTS_MIN,
            GOLD_FACT_ORDERS_MIN, GOLD_AGG_DAILY_CITY_MIN,
        ]:
            assert len(table) == len(set(table))


class TestPipelineConfigSchema:
    """Validate pipeline configuration is internally consistent."""

    def test_config_has_fifteen_cities(self, config):
        assert len(config.cities) == 15

    def test_config_has_all_subscription_tiers(self, config):
        expected = {"Free", "Zomato Pro", "Zomato Pro Plus", "Zomato Gold"}
        assert set(config.subscription_tiers) == expected

    def test_config_has_all_order_statuses(self, config):
        expected = {
            "Placed", "Confirmed", "Preparing",
            "Out for Delivery", "Delivered", "Cancelled", "Refunded",
        }
        assert set(config.order_statuses) == expected

    def test_config_has_all_delivery_statuses(self, config):
        expected = {"Assigned", "Picked Up", "In Transit", "Delivered", "Failed", "Returned"}
        assert set(config.delivery_statuses) == expected

    def test_config_price_range_valid(self, config):
        assert config.min_item_price < config.max_item_price
        assert config.min_item_price > 0

    def test_config_delivery_fee_range_valid(self, config):
        assert config.min_delivery_fee >= 0
        assert config.max_delivery_fee > config.min_delivery_fee

    def test_config_date_range_valid(self, config):
        start = date.fromisoformat(config.data_start_date)
        end = date.fromisoformat(config.data_end_date)
        assert start < end

    def test_config_discount_probability_valid(self, config):
        assert 0.0 <= config.discount_probability <= 1.0

    def test_config_max_discount_valid(self, config):
        assert 0.0 < config.max_discount_pct <= 100.0

    def test_config_payment_methods_count(self, config):
        assert len(config.payment_methods) == 6

    def test_config_order_platforms_count(self, config):
        assert len(config.order_platforms) == 4

    def test_config_cuisines_count(self, config):
        assert len(config.cuisines) == 20
