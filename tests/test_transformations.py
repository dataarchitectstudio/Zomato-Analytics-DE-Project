"""
Unit tests for Silver and Gold layer transformation logic.

Each helper function below mirrors the exact PySpark logic used in the
Silver transformation notebook. Tests verify correctness of business rules,
boundary values, deduplication, referential integrity filtering, and
amount clamping — all using a local SparkSession (no Databricks required).
"""

from pyspark.sql import Row
from pyspark.sql.functions import (
    col, coalesce, hour, initcap, lit, lower, row_number,
    to_timestamp, trim, when,
)
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Helper functions — mirror Silver notebook transformation logic
# ---------------------------------------------------------------------------

def deduplicate(df, key_col: str, order_col: str):
    """Keep the latest record per key, ordered by order_col descending."""
    w = Window.partitionBy(key_col).orderBy(col(order_col).desc())
    return df.withColumn("_rn", row_number().over(w)).filter(col("_rn") == 1).drop("_rn")


def apply_time_slot(df, ts_col: str = "order_placed_at"):
    """Derive order time slot from the hour of the timestamp."""
    h = hour(to_timestamp(col(ts_col)))
    return df.withColumn(
        "order_time_slot",
        when((h >= 6) & (h < 11), "Breakfast")
        .when((h >= 11) & (h < 15), "Lunch")
        .when((h >= 15) & (h < 18), "Snacks")
        .when((h >= 18) & (h < 23), "Dinner")
        .otherwise("Late Night"),
    )


def apply_delivery_sla(df, mins_col: str = "delivery_time_mins"):
    """Classify delivery SLA status based on delivery time in minutes."""
    return df.withColumn(
        "delivery_sla_status",
        when(col(mins_col) <= 30, "Within SLA")
        .when(col(mins_col) <= 45, "Near SLA")
        .otherwise("SLA Breached"),
    )


def apply_price_tier(df, cost_col: str = "avg_cost_for_two"):
    """Derive restaurant price tier from average cost for two."""
    return df.withColumn(
        "price_tier",
        when(col(cost_col) < 300, "Budget")
        .when(col(cost_col) < 700, "Mid-Range")
        .when(col(cost_col) < 1500, "Premium")
        .otherwise("Luxury"),
    )


def apply_rating_tier(df, rating_col: str = "rating"):
    """Derive restaurant rating tier from numeric rating."""
    return df.withColumn(
        "rating_tier",
        when(col(rating_col) >= 4.5, "Excellent")
        .when(col(rating_col) >= 4.0, "Very Good")
        .when(col(rating_col) >= 3.5, "Good")
        .when(col(rating_col) >= 3.0, "Average")
        .otherwise("Below Average"),
    )


def apply_customer_segment(df, orders_col: str = "total_orders_lifetime"):
    """Segment customers by lifetime order count."""
    return df.withColumn(
        "customer_segment",
        when(col(orders_col) >= 50, "Power User")
        .when(col(orders_col) >= 15, "Regular")
        .when(col(orders_col) >= 5, "Occasional")
        .otherwise("New"),
    )


def clamp_negative_amounts(df, amount_col: str = "total_amount"):
    """Replace negative monetary values with 0."""
    return df.withColumn(
        amount_col,
        when(col(amount_col) < 0, lit(0.0)).otherwise(col(amount_col)),
    )


def apply_order_flags(df):
    """Derive boolean flags from order_status."""
    return (
        df.withColumn("is_delivered", col("order_status") == "Delivered")
        .withColumn("is_cancelled", col("order_status") == "Cancelled")
        .withColumn("has_discount", col("discount_amount") > 0)
    )


def apply_coalesce_default(df, column: str, default):
    """Replace nulls in column with a default value."""
    return df.withColumn(column, coalesce(col(column), lit(default)))


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDeduplication:
    def test_removes_duplicate_keys(self, spark):
        data = [
            Row(id="A", ts="2024-01-02", value=10),
            Row(id="A", ts="2024-01-01", value=5),
            Row(id="B", ts="2024-01-01", value=20),
        ]
        result = deduplicate(spark.createDataFrame(data), "id", "ts")
        assert result.count() == 2

    def test_keeps_latest_record(self, spark):
        data = [
            Row(id="A", ts="2024-01-02", value=10),
            Row(id="A", ts="2024-01-01", value=5),
        ]
        result = deduplicate(spark.createDataFrame(data), "id", "ts")
        assert result.collect()[0]["value"] == 10

    def test_no_duplicates_unchanged(self, spark):
        data = [Row(id="A", ts="2024-01-01"), Row(id="B", ts="2024-01-02")]
        result = deduplicate(spark.createDataFrame(data), "id", "ts")
        assert result.count() == 2

    def test_dedup_column_removed_after(self, spark):
        data = [Row(id="A", ts="2024-01-01"), Row(id="A", ts="2024-01-02")]
        result = deduplicate(spark.createDataFrame(data), "id", "ts")
        assert "_rn" not in result.columns

    def test_three_duplicates_keeps_one(self, spark):
        data = [
            Row(id="X", ts="2024-03-01"),
            Row(id="X", ts="2024-01-01"),
            Row(id="X", ts="2024-02-01"),
        ]
        result = deduplicate(spark.createDataFrame(data), "id", "ts")
        assert result.count() == 1
        assert result.collect()[0]["ts"] == "2024-03-01"


class TestTextStandardization:
    def test_initcap_applies_proper_case(self, spark):
        df = spark.createDataFrame([Row(name="john doe")])
        result = df.withColumn("name", initcap(trim(col("name"))))
        assert result.collect()[0]["name"] == "John Doe"

    def test_trim_removes_leading_trailing_spaces(self, spark):
        df = spark.createDataFrame([Row(val="  hello  ")])
        result = df.withColumn("val", trim(col("val")))
        assert result.collect()[0]["val"] == "hello"

    def test_lowercase_email(self, spark):
        df = spark.createDataFrame([Row(email="USER@EXAMPLE.COM")])
        result = df.withColumn("email", lower(col("email")))
        assert result.collect()[0]["email"] == "user@example.com"

    def test_trim_and_lowercase_combined(self, spark):
        df = spark.createDataFrame([Row(email="  USER@EXAMPLE.COM  ")])
        result = df.withColumn("email", lower(trim(col("email"))))
        assert result.collect()[0]["email"] == "user@example.com"

    def test_initcap_mixed_case_input(self, spark):
        df = spark.createDataFrame([Row(city="mUmBAI")])
        result = df.withColumn("city", initcap(col("city")))
        assert result.collect()[0]["city"] == "Mumbai"


class TestOrderTimeSlot:
    def test_breakfast_at_7am(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 07:30:00")]))
        assert df.collect()[0]["order_time_slot"] == "Breakfast"

    def test_lunch_at_noon(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 12:00:00")]))
        assert df.collect()[0]["order_time_slot"] == "Lunch"

    def test_snacks_at_4pm(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 16:00:00")]))
        assert df.collect()[0]["order_time_slot"] == "Snacks"

    def test_dinner_at_8pm(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 20:00:00")]))
        assert df.collect()[0]["order_time_slot"] == "Dinner"

    def test_late_night_at_1am(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 01:00:00")]))
        assert df.collect()[0]["order_time_slot"] == "Late Night"

    def test_breakfast_boundary_at_6am(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 06:00:00")]))
        assert df.collect()[0]["order_time_slot"] == "Breakfast"

    def test_dinner_boundary_at_11pm(self, spark):
        df = apply_time_slot(spark.createDataFrame([Row(order_placed_at="2024-06-15 22:59:00")]))
        assert df.collect()[0]["order_time_slot"] == "Dinner"

    def test_all_five_slots_produced(self, spark):
        rows = [
            Row(order_placed_at="2024-06-15 07:00:00"),
            Row(order_placed_at="2024-06-15 12:00:00"),
            Row(order_placed_at="2024-06-15 16:00:00"),
            Row(order_placed_at="2024-06-15 20:00:00"),
            Row(order_placed_at="2024-06-15 02:00:00"),
        ]
        df = apply_time_slot(spark.createDataFrame(rows))
        slots = {r["order_time_slot"] for r in df.collect()}
        assert slots == {"Breakfast", "Lunch", "Snacks", "Dinner", "Late Night"}


class TestDeliverySLAStatus:
    def test_within_sla_at_25_mins(self, spark):
        df = apply_delivery_sla(spark.createDataFrame([Row(delivery_time_mins=25)]))
        assert df.collect()[0]["delivery_sla_status"] == "Within SLA"

    def test_near_sla_at_38_mins(self, spark):
        df = apply_delivery_sla(spark.createDataFrame([Row(delivery_time_mins=38)]))
        assert df.collect()[0]["delivery_sla_status"] == "Near SLA"

    def test_sla_breached_at_55_mins(self, spark):
        df = apply_delivery_sla(spark.createDataFrame([Row(delivery_time_mins=55)]))
        assert df.collect()[0]["delivery_sla_status"] == "SLA Breached"

    def test_boundary_exactly_30_mins(self, spark):
        df = apply_delivery_sla(spark.createDataFrame([Row(delivery_time_mins=30)]))
        assert df.collect()[0]["delivery_sla_status"] == "Within SLA"

    def test_boundary_exactly_45_mins(self, spark):
        df = apply_delivery_sla(spark.createDataFrame([Row(delivery_time_mins=45)]))
        assert df.collect()[0]["delivery_sla_status"] == "Near SLA"

    def test_all_three_statuses_produced(self, spark):
        rows = [
            Row(delivery_time_mins=20),
            Row(delivery_time_mins=40),
            Row(delivery_time_mins=60),
        ]
        df = apply_delivery_sla(spark.createDataFrame(rows))
        statuses = {r["delivery_sla_status"] for r in df.collect()}
        assert statuses == {"Within SLA", "Near SLA", "SLA Breached"}


class TestPriceTier:
    def test_budget_below_300(self, spark):
        df = apply_price_tier(spark.createDataFrame([Row(avg_cost_for_two=200)]))
        assert df.collect()[0]["price_tier"] == "Budget"

    def test_mid_range_at_500(self, spark):
        df = apply_price_tier(spark.createDataFrame([Row(avg_cost_for_two=500)]))
        assert df.collect()[0]["price_tier"] == "Mid-Range"

    def test_premium_at_1000(self, spark):
        df = apply_price_tier(spark.createDataFrame([Row(avg_cost_for_two=1000)]))
        assert df.collect()[0]["price_tier"] == "Premium"

    def test_luxury_at_2500(self, spark):
        df = apply_price_tier(spark.createDataFrame([Row(avg_cost_for_two=2500)]))
        assert df.collect()[0]["price_tier"] == "Luxury"

    def test_all_four_tiers_produced(self, spark):
        rows = [
            Row(avg_cost_for_two=200),
            Row(avg_cost_for_two=500),
            Row(avg_cost_for_two=1000),
            Row(avg_cost_for_two=2000),
        ]
        df = apply_price_tier(spark.createDataFrame(rows))
        tiers = {r["price_tier"] for r in df.collect()}
        assert tiers == {"Budget", "Mid-Range", "Premium", "Luxury"}


class TestRatingTier:
    def test_excellent_at_4_8(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=4.8)]))
        assert df.collect()[0]["rating_tier"] == "Excellent"

    def test_very_good_at_4_2(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=4.2)]))
        assert df.collect()[0]["rating_tier"] == "Very Good"

    def test_good_at_3_7(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=3.7)]))
        assert df.collect()[0]["rating_tier"] == "Good"

    def test_average_at_3_1(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=3.1)]))
        assert df.collect()[0]["rating_tier"] == "Average"

    def test_below_average_at_2_5(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=2.5)]))
        assert df.collect()[0]["rating_tier"] == "Below Average"

    def test_boundary_exactly_4_5(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=4.5)]))
        assert df.collect()[0]["rating_tier"] == "Excellent"

    def test_boundary_exactly_4_0(self, spark):
        df = apply_rating_tier(spark.createDataFrame([Row(rating=4.0)]))
        assert df.collect()[0]["rating_tier"] == "Very Good"


class TestCustomerSegment:
    def test_power_user_at_100_orders(self, spark):
        df = apply_customer_segment(spark.createDataFrame([Row(total_orders_lifetime=100)]))
        assert df.collect()[0]["customer_segment"] == "Power User"

    def test_regular_at_20_orders(self, spark):
        df = apply_customer_segment(spark.createDataFrame([Row(total_orders_lifetime=20)]))
        assert df.collect()[0]["customer_segment"] == "Regular"

    def test_occasional_at_8_orders(self, spark):
        df = apply_customer_segment(spark.createDataFrame([Row(total_orders_lifetime=8)]))
        assert df.collect()[0]["customer_segment"] == "Occasional"

    def test_new_at_2_orders(self, spark):
        df = apply_customer_segment(spark.createDataFrame([Row(total_orders_lifetime=2)]))
        assert df.collect()[0]["customer_segment"] == "New"

    def test_new_at_zero_orders(self, spark):
        df = apply_customer_segment(spark.createDataFrame([Row(total_orders_lifetime=0)]))
        assert df.collect()[0]["customer_segment"] == "New"

    def test_boundary_at_50_orders(self, spark):
        df = apply_customer_segment(spark.createDataFrame([Row(total_orders_lifetime=50)]))
        assert df.collect()[0]["customer_segment"] == "Power User"

    def test_all_four_segments_produced(self, spark):
        rows = [
            Row(total_orders_lifetime=100),
            Row(total_orders_lifetime=20),
            Row(total_orders_lifetime=8),
            Row(total_orders_lifetime=1),
        ]
        df = apply_customer_segment(spark.createDataFrame(rows))
        segments = {r["customer_segment"] for r in df.collect()}
        assert segments == {"Power User", "Regular", "Occasional", "New"}


class TestAmountClamping:
    def test_negative_amount_clamped_to_zero(self, spark):
        df = clamp_negative_amounts(spark.createDataFrame([Row(total_amount=-50.0)]))
        assert df.collect()[0]["total_amount"] == 0.0

    def test_positive_amount_unchanged(self, spark):
        df = clamp_negative_amounts(spark.createDataFrame([Row(total_amount=250.0)]))
        assert df.collect()[0]["total_amount"] == 250.0

    def test_zero_amount_unchanged(self, spark):
        df = clamp_negative_amounts(spark.createDataFrame([Row(total_amount=0.0)]))
        assert df.collect()[0]["total_amount"] == 0.0

    def test_large_negative_clamped(self, spark):
        df = clamp_negative_amounts(spark.createDataFrame([Row(total_amount=-9999.99)]))
        assert df.collect()[0]["total_amount"] == 0.0


class TestOrderFlags:
    def test_is_delivered_true_for_delivered(self, spark):
        df = apply_order_flags(spark.createDataFrame([
            Row(order_status="Delivered", discount_amount=0.0)
        ]))
        assert df.collect()[0]["is_delivered"] is True

    def test_is_delivered_false_for_cancelled(self, spark):
        df = apply_order_flags(spark.createDataFrame([
            Row(order_status="Cancelled", discount_amount=0.0)
        ]))
        assert df.collect()[0]["is_delivered"] is False

    def test_is_cancelled_true_for_cancelled(self, spark):
        df = apply_order_flags(spark.createDataFrame([
            Row(order_status="Cancelled", discount_amount=0.0)
        ]))
        assert df.collect()[0]["is_cancelled"] is True

    def test_has_discount_true_when_amount_positive(self, spark):
        df = apply_order_flags(spark.createDataFrame([
            Row(order_status="Delivered", discount_amount=50.0)
        ]))
        assert df.collect()[0]["has_discount"] is True

    def test_has_discount_false_when_no_discount(self, spark):
        df = apply_order_flags(spark.createDataFrame([
            Row(order_status="Delivered", discount_amount=0.0)
        ]))
        assert df.collect()[0]["has_discount"] is False


class TestReferentialIntegrity:
    def test_unmatched_orders_filtered_out(self, spark):
        orders = spark.createDataFrame([
            Row(order_id="O1", customer_id="C1"),
            Row(order_id="O2", customer_id="C99"),
        ])
        customers = spark.createDataFrame([Row(customer_id="C1")])
        valid = orders.join(customers, "customer_id", "inner")
        assert valid.count() == 1

    def test_matched_orders_all_preserved(self, spark):
        orders = spark.createDataFrame([
            Row(order_id="O1", customer_id="C1"),
            Row(order_id="O2", customer_id="C2"),
        ])
        customers = spark.createDataFrame([
            Row(customer_id="C1"),
            Row(customer_id="C2"),
        ])
        valid = orders.join(customers, "customer_id", "inner")
        assert valid.count() == 2

    def test_all_unmatched_filtered_out(self, spark):
        orders = spark.createDataFrame([
            Row(order_id="O1", customer_id="C99"),
            Row(order_id="O2", customer_id="C88"),
        ])
        customers = spark.createDataFrame([Row(customer_id="C1")])
        valid = orders.join(customers, "customer_id", "inner")
        assert valid.count() == 0

    def test_deliveries_filtered_by_valid_orders(self, spark):
        deliveries = spark.createDataFrame([
            Row(delivery_id="D1", order_id="O1"),
            Row(delivery_id="D2", order_id="O99"),
        ])
        orders = spark.createDataFrame([Row(order_id="O1")])
        valid = deliveries.join(orders, "order_id", "inner")
        assert valid.count() == 1


class TestNullSafeCoalesce:
    def test_null_replaced_with_default_false(self, spark):
        df = spark.createDataFrame([Row(is_active=None)])
        result = apply_coalesce_default(df, "is_active", False)
        assert result.collect()[0]["is_active"] is False

    def test_null_replaced_with_default_zero(self, spark):
        df = spark.createDataFrame([Row(tip_amount=None)])
        result = apply_coalesce_default(df, "tip_amount", 0.0)
        assert result.collect()[0]["tip_amount"] == 0.0

    def test_non_null_value_preserved(self, spark):
        df = spark.createDataFrame([Row(tip_amount=25.0)])
        result = apply_coalesce_default(df, "tip_amount", 0.0)
        assert result.collect()[0]["tip_amount"] == 25.0
