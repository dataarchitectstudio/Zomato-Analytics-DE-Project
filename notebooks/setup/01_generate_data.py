# Databricks notebook source
# MAGIC %md
# MAGIC # Zomato Analytics — Synthetic Data Generator
# MAGIC
# MAGIC **Purpose**: Generate realistic synthetic data using Faker and write Parquet files
# MAGIC to the Unity Catalog Volume for Bronze layer ingestion.
# MAGIC
# MAGIC **Output**: Parquet files at `/Volumes/{catalog}/raw/landing/{entity}/`
# MAGIC
# MAGIC **Entities Generated**:
# MAGIC - customers (10,000)
# MAGIC - restaurants (2,500)
# MAGIC - orders (150,000)
# MAGIC - deliveries (~110,000)
# MAGIC - reviews (120,000)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Install Dependencies

# COMMAND ----------

# MAGIC %pip install faker
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Configuration

# COMMAND ----------

import hashlib
import random
from datetime import datetime, timedelta, timezone

from faker import Faker
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
    IntegerType, FloatType, ArrayType,
)

# COMMAND ----------

dbutils.widgets.text("catalog_name", "zomato_analytics", "Unity Catalog name")
dbutils.widgets.text("num_customers", "10000", "Number of customers")
dbutils.widgets.text("num_restaurants", "2500", "Number of restaurants")
dbutils.widgets.text("num_orders", "150000", "Number of orders")
dbutils.widgets.text("num_reviews", "120000", "Number of reviews")

CATALOG = dbutils.widgets.get("catalog_name")
NUM_CUSTOMERS = int(dbutils.widgets.get("num_customers"))
NUM_RESTAURANTS = int(dbutils.widgets.get("num_restaurants"))
NUM_ORDERS = int(dbutils.widgets.get("num_orders"))
NUM_REVIEWS = int(dbutils.widgets.get("num_reviews"))

LANDING_PATH = f"/Volumes/{CATALOG}/raw/landing"

print(f"Catalog      : {CATALOG}")
print(f"Landing Path : {LANDING_PATH}")
print(f"Customers    : {NUM_CUSTOMERS:,}")
print(f"Restaurants  : {NUM_RESTAURANTS:,}")
print(f"Orders       : {NUM_ORDERS:,}")
print(f"Reviews      : {NUM_REVIEWS:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Domain Configuration

# COMMAND ----------

CITIES = [
    "Mumbai", "Delhi", "Bangalore", "Hyderabad", "Chennai",
    "Kolkata", "Pune", "Ahmedabad", "Jaipur", "Lucknow",
    "Chandigarh", "Goa", "Kochi", "Indore", "Nagpur",
]

CUISINES = [
    "North Indian", "South Indian", "Chinese", "Italian", "Continental",
    "Mughlai", "Street Food", "Biryani", "Desserts", "Beverages",
    "Fast Food", "Thai", "Japanese", "Mexican", "Mediterranean",
    "Korean", "Bengali", "Rajasthani", "Gujarati", "Kerala",
]

RESTAURANT_TYPES = [
    "Quick Bites", "Casual Dining", "Fine Dining", "Cafe", "Bakery",
    "Cloud Kitchen", "Food Court", "Dhaba", "Lounge", "Microbrewery",
]

PAYMENT_METHODS = ["UPI", "Credit Card", "Debit Card", "Cash on Delivery", "Wallet", "Net Banking"]
ORDER_STATUSES = ["Placed", "Confirmed", "Preparing", "Out for Delivery", "Delivered", "Cancelled", "Refunded"]
DELIVERY_STATUSES = ["Assigned", "Picked Up", "In Transit", "Delivered", "Failed", "Returned"]
PLATFORMS = ["Android App", "iOS App", "Web", "Partner API"]
SUBSCRIPTION_TIERS = ["Free", "Zomato Pro", "Zomato Pro Plus", "Zomato Gold"]

DATA_START = "2024-01-01"
DATA_END = "2025-12-31"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Generator Functions

# COMMAND ----------

fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)
UTC = timezone.utc


def _gen_id(prefix, index):
    raw = f"{prefix}_{index}"
    return f"{prefix.upper()}_{hashlib.md5(raw.encode()).hexdigest()[:8]}"


def _rand_ts(start, end):
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    return s + timedelta(seconds=random.random() * (e - s).total_seconds())


# COMMAND ----------

def generate_customers():
    print("  Generating customers...")
    rows = []
    for i in range(NUM_CUSTOMERS):
        signup = _rand_ts(DATA_START, DATA_END)
        rows.append({
            "customer_id": _gen_id("cust", i),
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": fake.email(),
            "phone": fake.phone_number(),
            "city": random.choice(CITIES),
            "locality": fake.street_name(),
            "pincode": fake.postcode(),
            "latitude": round(fake.latitude(), 6),
            "longitude": round(fake.longitude(), 6),
            "signup_date": signup.isoformat(),
            "subscription_tier": random.choice(SUBSCRIPTION_TIERS),
            "is_active": random.choices([True, False], weights=[0.85, 0.15])[0],
            "preferred_payment": random.choice(PAYMENT_METHODS),
            "preferred_cuisine": random.choice(CUISINES),
            "total_orders_lifetime": random.randint(0, 350),
            "avg_order_value": round(random.uniform(150, 900), 2),
            "_ingested_at": datetime.now(UTC).isoformat(),
            "_source_system": "zomato_app_backend",
        })
    return rows

# COMMAND ----------

def generate_restaurants():
    print("  Generating restaurants...")
    rows = []
    for i in range(NUM_RESTAURANTS):
        cuisines = random.sample(CUISINES, random.randint(1, 4))
        onboarded = _rand_ts("2020-01-01", DATA_END)
        rows.append({
            "restaurant_id": _gen_id("rest", i),
            "name": f"{fake.company()} {random.choice(['Kitchen', 'Bistro', 'Dhaba', 'Cafe', 'Restaurant', 'Eatery', 'House'])}",
            "city": random.choice(CITIES),
            "locality": fake.street_name(),
            "address": fake.address().replace("\n", ", "),
            "pincode": fake.postcode(),
            "latitude": round(fake.latitude(), 6),
            "longitude": round(fake.longitude(), 6),
            "cuisines": cuisines,
            "restaurant_type": random.choice(RESTAURANT_TYPES),
            "avg_cost_for_two": random.choice([200, 300, 500, 700, 1000, 1500, 2000, 2500]),
            "rating": round(random.uniform(2.5, 5.0), 1),
            "total_reviews": random.randint(10, 5000),
            "is_delivering_now": random.choices([True, False], weights=[0.8, 0.2])[0],
            "has_online_delivery": random.choices([True, False], weights=[0.9, 0.1])[0],
            "has_table_booking": random.choices([True, False], weights=[0.3, 0.7])[0],
            "is_premium_partner": random.choices([True, False], weights=[0.15, 0.85])[0],
            "onboarded_date": onboarded.isoformat(),
            "owner_name": fake.name(),
            "owner_phone": fake.phone_number(),
            "gst_number": f"{''.join(random.choices('0123456789', k=2))}{fake.bothify('?????').upper()}{''.join(random.choices('0123456789', k=4))}{fake.bothify('?').upper()}{''.join(random.choices('0123456789AZ', k=2))}",
            "_ingested_at": datetime.now(UTC).isoformat(),
            "_source_system": "restaurant_onboarding_svc",
        })
    return rows

# COMMAND ----------

def generate_orders(customers, restaurants):
    print("  Generating orders...")
    orders = []
    for i in range(NUM_ORDERS):
        customer = random.choice(customers)
        restaurant = random.choice(restaurants)
        order_ts = _rand_ts(DATA_START, DATA_END)
        status = random.choices(ORDER_STATUSES, weights=[0.05, 0.05, 0.05, 0.05, 0.70, 0.07, 0.03])[0]

        num_items = random.randint(1, 5)
        subtotal = round(sum(random.uniform(49, 1499) for _ in range(num_items)), 2)

        discount_pct = 0.0
        coupon_code = None
        if random.random() < 0.35:
            discount_pct = round(random.uniform(5, 50), 1)
            coupon_code = fake.bothify("ZOMATO##??").upper()

        discount_amount = round(subtotal * discount_pct / 100, 2)
        delivery_fee = round(random.uniform(0, 80), 2)
        tax_amount = round((subtotal - discount_amount) * 0.05, 2)
        total_amount = round(subtotal - discount_amount + delivery_fee + tax_amount, 2)

        orders.append({
            "order_id": _gen_id("ord", i),
            "customer_id": customer["customer_id"],
            "restaurant_id": restaurant["restaurant_id"],
            "order_placed_at": order_ts.isoformat(),
            "order_status": status,
            "payment_method": random.choice(PAYMENT_METHODS),
            "payment_status": "Completed" if status == "Delivered" else random.choice(["Pending", "Completed", "Failed"]),
            "platform": random.choice(PLATFORMS),
            "subtotal": subtotal,
            "discount_pct": discount_pct,
            "discount_amount": discount_amount,
            "coupon_code": coupon_code,
            "delivery_fee": delivery_fee,
            "tax_amount": tax_amount,
            "total_amount": total_amount,
            "num_items": num_items,
            "special_instructions": fake.sentence(nb_words=5) if random.random() < 0.2 else None,
            "_ingested_at": datetime.now(UTC).isoformat(),
            "_source_system": "order_management_svc",
        })
    return orders

# COMMAND ----------

def generate_deliveries(orders):
    print("  Generating deliveries...")
    deliverable = [o for o in orders if o["order_status"] in ("Out for Delivery", "Delivered")]
    deliveries = []
    for i, order in enumerate(deliverable):
        pickup_ts = datetime.fromisoformat(order["order_placed_at"]) + timedelta(minutes=random.randint(5, 25))
        delivery_time = random.randint(15, 60)
        delivered_ts = pickup_ts + timedelta(minutes=delivery_time)

        deliveries.append({
            "delivery_id": _gen_id("dlv", i),
            "order_id": order["order_id"],
            "delivery_partner_id": _gen_id("dp", random.randint(0, 5000)),
            "delivery_partner_name": fake.name(),
            "partner_phone": fake.phone_number(),
            "vehicle_type": random.choice(["Bike", "Scooter", "Bicycle", "Car"]),
            "pickup_timestamp": pickup_ts.isoformat(),
            "delivered_timestamp": delivered_ts.isoformat(),
            "delivery_time_mins": delivery_time,
            "delivery_status": random.choices(DELIVERY_STATUSES, weights=[0.03, 0.05, 0.05, 0.80, 0.05, 0.02])[0],
            "delivery_distance_km": round(random.uniform(0.5, 15.0), 2),
            "delivery_rating": random.choices([1, 2, 3, 4, 5, None], weights=[0.02, 0.05, 0.10, 0.35, 0.40, 0.08])[0],
            "_ingested_at": datetime.now(UTC).isoformat(),
            "_source_system": "logistics_svc",
        })
    return deliveries

# COMMAND ----------

def generate_reviews(orders):
    print("  Generating reviews...")
    delivered = [o for o in orders if o["order_status"] == "Delivered"]
    sentiments = [
        "Amazing food, will order again!", "Decent taste but took too long.",
        "Cold food received, very disappointed.", "Best biryani in the city!",
        "Average experience, nothing special.", "Packaging was excellent, food was fresh.",
        "Portion size too small for the price.", "Loved the variety of options.",
        "Quick delivery, hot and fresh food!", "Great value for money.",
        "Consistent quality every time I order.", "Would recommend to friends and family.",
    ]
    reviews = []
    for i in range(min(NUM_REVIEWS, len(delivered))):
        order = delivered[i]
        review_ts = datetime.fromisoformat(order["order_placed_at"]) + timedelta(hours=random.randint(1, 48))
        rating = random.choices([1, 2, 3, 4, 5], weights=[0.05, 0.08, 0.15, 0.35, 0.37])[0]
        reviews.append({
            "review_id": _gen_id("rev", i),
            "order_id": order["order_id"],
            "customer_id": order["customer_id"],
            "restaurant_id": order["restaurant_id"],
            "rating": rating,
            "review_text": random.choice(sentiments) if random.random() < 0.7 else fake.paragraph(nb_sentences=2),
            "review_timestamp": review_ts.isoformat(),
            "sentiment_label": "Positive" if rating >= 4 else ("Negative" if rating <= 2 else "Neutral"),
            "is_verified_order": True,
            "upvotes": random.randint(0, 50),
            "has_photo": random.choices([True, False], weights=[0.2, 0.8])[0],
            "_ingested_at": datetime.now(UTC).isoformat(),
            "_source_system": "review_svc",
        })
    return reviews

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate All Data

# COMMAND ----------

import time

overall_start = time.time()

print("=" * 60)
print("  Zomato Analytics — Data Generation")
print("=" * 60)

customers = generate_customers()
print(f"    ✓ Customers: {len(customers):,}")

restaurants = generate_restaurants()
print(f"    ✓ Restaurants: {len(restaurants):,}")

orders = generate_orders(customers, restaurants)
print(f"    ✓ Orders: {len(orders):,}")

deliveries = generate_deliveries(orders)
print(f"    ✓ Deliveries: {len(deliveries):,}")

reviews = generate_reviews(orders)
print(f"    ✓ Reviews: {len(reviews):,}")

gen_time = round(time.time() - overall_start, 1)
print(f"\n  Data generation completed in {gen_time}s")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Parquet to Landing Volume

# COMMAND ----------

def write_to_landing(data, entity_name, schema):
    """Convert list of dicts to Spark DataFrame and write as Parquet to Volume."""
    path = f"{LANDING_PATH}/{entity_name}"
    df = spark.createDataFrame(data, schema=schema)
    df.write.mode("overwrite").parquet(path)
    count = df.count()
    print(f"  ✓ {entity_name}: {count:,} records → {path}")
    return count

# COMMAND ----------

from pyspark.sql.types import LongType

LANDING_SCHEMAS = {
    "customers": StructType([
        StructField("customer_id", StringType(), False),
        StructField("first_name", StringType()), StructField("last_name", StringType()),
        StructField("email", StringType()), StructField("phone", StringType()),
        StructField("city", StringType()), StructField("locality", StringType()),
        StructField("pincode", StringType()),
        StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
        StructField("signup_date", StringType()),
        StructField("subscription_tier", StringType()),
        StructField("is_active", BooleanType()),
        StructField("preferred_payment", StringType()),
        StructField("preferred_cuisine", StringType()),
        StructField("total_orders_lifetime", IntegerType()),
        StructField("avg_order_value", DoubleType()),
        StructField("_ingested_at", StringType()),
        StructField("_source_system", StringType()),
    ]),
    "restaurants": StructType([
        StructField("restaurant_id", StringType(), False),
        StructField("name", StringType()), StructField("city", StringType()),
        StructField("locality", StringType()), StructField("address", StringType()),
        StructField("pincode", StringType()),
        StructField("latitude", DoubleType()), StructField("longitude", DoubleType()),
        StructField("cuisines", ArrayType(StringType())),
        StructField("restaurant_type", StringType()),
        StructField("avg_cost_for_two", IntegerType()),
        StructField("rating", FloatType()),
        StructField("total_reviews", IntegerType()),
        StructField("is_delivering_now", BooleanType()),
        StructField("has_online_delivery", BooleanType()),
        StructField("has_table_booking", BooleanType()),
        StructField("is_premium_partner", BooleanType()),
        StructField("onboarded_date", StringType()),
        StructField("owner_name", StringType()), StructField("owner_phone", StringType()),
        StructField("gst_number", StringType()),
        StructField("_ingested_at", StringType()),
        StructField("_source_system", StringType()),
    ]),
    "orders": StructType([
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType()), StructField("restaurant_id", StringType()),
        StructField("order_placed_at", StringType()),
        StructField("order_status", StringType()),
        StructField("payment_method", StringType()), StructField("payment_status", StringType()),
        StructField("platform", StringType()),
        StructField("subtotal", DoubleType()), StructField("discount_pct", DoubleType()),
        StructField("discount_amount", DoubleType()), StructField("coupon_code", StringType()),
        StructField("delivery_fee", DoubleType()), StructField("tax_amount", DoubleType()),
        StructField("total_amount", DoubleType()),
        StructField("num_items", IntegerType()),
        StructField("special_instructions", StringType()),
        StructField("_ingested_at", StringType()),
        StructField("_source_system", StringType()),
    ]),
    "deliveries": StructType([
        StructField("delivery_id", StringType(), False),
        StructField("order_id", StringType()),
        StructField("delivery_partner_id", StringType()),
        StructField("delivery_partner_name", StringType()),
        StructField("partner_phone", StringType()),
        StructField("vehicle_type", StringType()),
        StructField("pickup_timestamp", StringType()),
        StructField("delivered_timestamp", StringType()),
        StructField("delivery_time_mins", IntegerType()),
        StructField("delivery_status", StringType()),
        StructField("delivery_distance_km", DoubleType()),
        StructField("delivery_rating", IntegerType()),
        StructField("_ingested_at", StringType()),
        StructField("_source_system", StringType()),
    ]),
    "reviews": StructType([
        StructField("review_id", StringType(), False),
        StructField("order_id", StringType()),
        StructField("customer_id", StringType()),
        StructField("restaurant_id", StringType()),
        StructField("rating", IntegerType()),
        StructField("review_text", StringType()),
        StructField("review_timestamp", StringType()),
        StructField("sentiment_label", StringType()),
        StructField("is_verified_order", BooleanType()),
        StructField("upvotes", IntegerType()),
        StructField("has_photo", BooleanType()),
        StructField("_ingested_at", StringType()),
        StructField("_source_system", StringType()),
    ]),
}

# COMMAND ----------

print("\n" + "=" * 60)
print("  Writing Parquet to Landing Volume")
print("=" * 60)

# Convert Decimal values to float for customers and restaurants
for cust in customers:
    cust['latitude'] = float(cust['latitude'])
    cust['longitude'] = float(cust['longitude'])
    cust['avg_order_value'] = float(cust['avg_order_value'])

for rest in restaurants:
    rest['latitude'] = float(rest['latitude'])
    rest['longitude'] = float(rest['longitude'])

total_written = 0
total_written += write_to_landing(customers, "customers", LANDING_SCHEMAS["customers"])
total_written += write_to_landing(restaurants, "restaurants", LANDING_SCHEMAS["restaurants"])
total_written += write_to_landing(orders, "orders", LANDING_SCHEMAS["orders"])
total_written += write_to_landing(deliveries, "deliveries", LANDING_SCHEMAS["deliveries"])
total_written += write_to_landing(reviews, "reviews", LANDING_SCHEMAS["reviews"])

total_time = round(time.time() - overall_start, 1)

print(f"\n{'='*60}")
print(f"  ✓ Total records written : {total_written:,}")
print(f"  ✓ Landing path          : {LANDING_PATH}")
print(f"  ✓ Total time            : {total_time}s")
print(f"{'='*60}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify Landing Data

# COMMAND ----------

print("Verifying landing data...")
for entity in ["customers", "restaurants", "orders", "deliveries", "reviews"]:
    path = f"{LANDING_PATH}/{entity}"
    df = spark.read.parquet(path)
    print(f"  ✓ {entity}: {df.count():,} records")

print("\n🟢 Data generation and landing complete.")
