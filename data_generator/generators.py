"""
Zomato Analytics — Synthetic Data Generators
Produces realistic datasets for customers, restaurants, menus, orders,
deliveries, and reviews using the Faker library.
"""

import hashlib
import random
from datetime import UTC, datetime, timedelta
from typing import Any, Dict, List, Tuple

from faker import Faker

from .config import DataVolumeConfig, ZomatoDataConfig

fake = Faker("en_IN")
Faker.seed(42)
random.seed(42)


def _generate_id(prefix: str, index: int) -> str:
    """Generate a deterministic, enterprise-style entity ID."""
    raw = f"{prefix}_{index}"
    hash_suffix = hashlib.md5(raw.encode()).hexdigest()[:8]
    return f"{prefix.upper()}_{hash_suffix}"


def _random_timestamp(start: str, end: str) -> datetime:
    """Return a random datetime between start and end ISO strings."""
    s = datetime.fromisoformat(start)
    e = datetime.fromisoformat(end)
    delta = (e - s).total_seconds()
    offset = random.random() * delta
    return s + timedelta(seconds=offset)


# ---------------------------------------------------------------------------
# Customer Generator
# ---------------------------------------------------------------------------
def generate_customers(
    volume: DataVolumeConfig,
    cfg: ZomatoDataConfig,
) -> List[Dict[str, Any]]:
    """Generate synthetic customer records."""
    customers = []
    for i in range(volume.num_customers):
        customer_id = _generate_id("cust", i)
        signup_date = _random_timestamp(cfg.data_start_date, cfg.data_end_date)
        city = random.choice(cfg.cities)

        customers.append(
            {
                "customer_id": customer_id,
                "first_name": fake.first_name(),
                "last_name": fake.last_name(),
                "email": fake.email(),
                "phone": fake.phone_number(),
                "city": city,
                "locality": fake.street_name(),
                "pincode": fake.postcode(),
                "latitude": round(fake.latitude(), 6),
                "longitude": round(fake.longitude(), 6),
                "signup_date": signup_date.isoformat(),
                "subscription_tier": random.choice(cfg.subscription_tiers),
                "is_active": random.choices([True, False], weights=[0.85, 0.15])[0],
                "preferred_payment": random.choice(cfg.payment_methods),
                "preferred_cuisine": random.choice(cfg.cuisines),
                "total_orders_lifetime": random.randint(0, 350),
                "avg_order_value": round(random.uniform(150, 900), 2),
                "_ingested_at": datetime.now(UTC).isoformat(),
                "_source_system": "zomato_app_backend",
            }
        )
    return customers


# ---------------------------------------------------------------------------
# Restaurant Generator
# ---------------------------------------------------------------------------
def generate_restaurants(
    volume: DataVolumeConfig,
    cfg: ZomatoDataConfig,
) -> List[Dict[str, Any]]:
    """Generate synthetic restaurant records."""
    restaurants = []
    for i in range(volume.num_restaurants):
        restaurant_id = _generate_id("rest", i)
        city = random.choice(cfg.cities)
        num_cuisines = random.randint(1, 4)
        cuisines = random.sample(cfg.cuisines, num_cuisines)
        onboarded_date = _random_timestamp("2020-01-01", cfg.data_end_date)

        restaurants.append(
            {
                "restaurant_id": restaurant_id,
                "name": f"{fake.company()} {random.choice(['Kitchen', 'Bistro', 'Dhaba', 'Cafe', 'Restaurant', 'Eatery', 'House'])}",
                "city": city,
                "locality": fake.street_name(),
                "address": fake.address().replace("\n", ", "),
                "pincode": fake.postcode(),
                "latitude": round(fake.latitude(), 6),
                "longitude": round(fake.longitude(), 6),
                "cuisines": cuisines,
                "restaurant_type": random.choice(cfg.restaurant_types),
                "avg_cost_for_two": random.choice([200, 300, 500, 700, 1000, 1500, 2000, 2500]),
                "rating": round(random.uniform(2.5, 5.0), 1),
                "total_reviews": random.randint(10, 5000),
                "is_delivering_now": random.choices([True, False], weights=[0.8, 0.2])[0],
                "has_online_delivery": random.choices([True, False], weights=[0.9, 0.1])[0],
                "has_table_booking": random.choices([True, False], weights=[0.3, 0.7])[0],
                "is_premium_partner": random.choices([True, False], weights=[0.15, 0.85])[0],
                "onboarded_date": onboarded_date.isoformat(),
                "owner_name": fake.name(),
                "owner_phone": fake.phone_number(),
                "gst_number": f"{''.join(random.choices('0123456789', k=2))}{fake.bothify('?????').upper()}{''.join(random.choices('0123456789', k=4))}{fake.bothify('?').upper()}{''.join(random.choices('0123456789AZ', k=2))}",
                "_ingested_at": datetime.now(UTC).isoformat(),
                "_source_system": "restaurant_onboarding_svc",
            }
        )
    return restaurants


# ---------------------------------------------------------------------------
# Menu Items Generator
# ---------------------------------------------------------------------------
def generate_menu_items(
    restaurants: List[Dict[str, Any]],
    volume: DataVolumeConfig,
    cfg: ZomatoDataConfig,
) -> List[Dict[str, Any]]:
    """Generate synthetic menu items for each restaurant."""
    item_categories = [
        "Starters",
        "Main Course",
        "Breads",
        "Rice & Biryani",
        "Desserts",
        "Beverages",
        "Combos",
        "Sides",
        "Soups",
        "Salads",
    ]
    veg_options = ["Veg", "Non-Veg", "Egg"]
    menu_items = []

    for rest in restaurants:
        num_items = random.randint(10, volume.num_menu_items_per_restaurant)
        for j in range(num_items):
            item_id = _generate_id(f"item_{rest['restaurant_id']}", j)
            menu_items.append(
                {
                    "item_id": item_id,
                    "restaurant_id": rest["restaurant_id"],
                    "item_name": fake.sentence(nb_words=3).rstrip("."),
                    "category": random.choice(item_categories),
                    "description": fake.sentence(nb_words=10),
                    "price": round(random.uniform(cfg.min_item_price, cfg.max_item_price), 2),
                    "veg_nonveg": random.choices(veg_options, weights=[0.5, 0.4, 0.1])[0],
                    "is_available": random.choices([True, False], weights=[0.9, 0.1])[0],
                    "is_bestseller": random.choices([True, False], weights=[0.1, 0.9])[0],
                    "preparation_time_mins": random.choice([10, 15, 20, 25, 30, 40, 45]),
                    "_ingested_at": datetime.now(UTC).isoformat(),
                    "_source_system": "menu_catalog_svc",
                }
            )
    return menu_items


# ---------------------------------------------------------------------------
# Orders Generator
# ---------------------------------------------------------------------------
def generate_orders(
    customers: List[Dict[str, Any]],
    restaurants: List[Dict[str, Any]],
    menu_items: List[Dict[str, Any]],
    volume: DataVolumeConfig,
    cfg: ZomatoDataConfig,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Generate synthetic orders and their line items."""
    # Build restaurant → menu lookup
    rest_menu_map: Dict[str, List[Dict[str, Any]]] = {}
    for item in menu_items:
        rest_menu_map.setdefault(item["restaurant_id"], []).append(item)

    orders = []
    order_items = []

    for i in range(volume.num_orders):
        order_id = _generate_id("ord", i)
        customer = random.choice(customers)
        restaurant = random.choice(restaurants)
        order_ts = _random_timestamp(cfg.data_start_date, cfg.data_end_date)
        status = random.choices(
            cfg.order_statuses,
            weights=[0.05, 0.05, 0.05, 0.05, 0.70, 0.07, 0.03],
        )[0]

        # Pick random items from this restaurant's menu
        available_items = rest_menu_map.get(restaurant["restaurant_id"], [])
        if not available_items:
            continue
        num_line_items = random.randint(1, 5)
        selected_items = random.choices(available_items, k=num_line_items)

        subtotal = 0.0
        for idx, sel_item in enumerate(selected_items):
            qty = random.randint(1, 3)
            line_total = round(sel_item["price"] * qty, 2)
            subtotal += line_total
            order_items.append(
                {
                    "order_item_id": f"{order_id}_ITEM_{idx}",
                    "order_id": order_id,
                    "item_id": sel_item["item_id"],
                    "item_name": sel_item["item_name"],
                    "quantity": qty,
                    "unit_price": sel_item["price"],
                    "line_total": line_total,
                    "_ingested_at": datetime.now(UTC).isoformat(),
                }
            )

        # Apply discount
        discount_pct = 0.0
        coupon_code = None
        if random.random() < cfg.discount_probability:
            discount_pct = round(random.uniform(5, cfg.max_discount_pct), 1)
            coupon_code = fake.bothify("ZOMATO##??").upper()

        discount_amount = round(subtotal * discount_pct / 100, 2)
        delivery_fee = round(random.uniform(cfg.min_delivery_fee, cfg.max_delivery_fee), 2)
        tax_amount = round((subtotal - discount_amount) * 0.05, 2)  # 5% GST
        total_amount = round(subtotal - discount_amount + delivery_fee + tax_amount, 2)

        orders.append(
            {
                "order_id": order_id,
                "customer_id": customer["customer_id"],
                "restaurant_id": restaurant["restaurant_id"],
                "order_placed_at": order_ts.isoformat(),
                "order_status": status,
                "payment_method": random.choice(cfg.payment_methods),
                "payment_status": (
                    "Completed" if status in ("Delivered",) else random.choice(["Pending", "Completed", "Failed"])
                ),
                "platform": random.choice(cfg.order_platforms),
                "subtotal": round(subtotal, 2),
                "discount_pct": discount_pct,
                "discount_amount": discount_amount,
                "coupon_code": coupon_code,
                "delivery_fee": delivery_fee,
                "tax_amount": tax_amount,
                "total_amount": total_amount,
                "num_items": num_line_items,
                "special_instructions": fake.sentence(nb_words=5) if random.random() < 0.2 else None,
                "_ingested_at": datetime.now(UTC).isoformat(),
                "_source_system": "order_management_svc",
            }
        )

    return orders, order_items


# ---------------------------------------------------------------------------
# Deliveries Generator
# ---------------------------------------------------------------------------
def generate_deliveries(
    orders: List[Dict[str, Any]],
    cfg: ZomatoDataConfig,
) -> List[Dict[str, Any]]:
    """Generate synthetic delivery records linked to orders."""
    deliverable_orders = [o for o in orders if o["order_status"] in ("Out for Delivery", "Delivered")]
    deliveries = []

    for i, order in enumerate(deliverable_orders):
        delivery_id = _generate_id("dlv", i)
        pickup_ts = datetime.fromisoformat(order["order_placed_at"]) + timedelta(minutes=random.randint(5, 25))
        delivery_time_mins = random.randint(15, 60)
        delivered_ts = pickup_ts + timedelta(minutes=delivery_time_mins)

        deliveries.append(
            {
                "delivery_id": delivery_id,
                "order_id": order["order_id"],
                "delivery_partner_id": _generate_id("dp", random.randint(0, 5000)),
                "delivery_partner_name": fake.name(),
                "partner_phone": fake.phone_number(),
                "vehicle_type": random.choice(["Bike", "Scooter", "Bicycle", "Car"]),
                "pickup_timestamp": pickup_ts.isoformat(),
                "delivered_timestamp": delivered_ts.isoformat(),
                "delivery_time_mins": delivery_time_mins,
                "delivery_status": random.choices(
                    cfg.delivery_statuses,
                    weights=[0.03, 0.05, 0.05, 0.80, 0.05, 0.02],
                )[0],
                "delivery_distance_km": round(random.uniform(0.5, 15.0), 2),
                "delivery_rating": random.choices(
                    [1, 2, 3, 4, 5, None],
                    weights=[0.02, 0.05, 0.10, 0.35, 0.40, 0.08],
                )[0],
                "_ingested_at": datetime.now(UTC).isoformat(),
                "_source_system": "logistics_svc",
            }
        )

    return deliveries


# ---------------------------------------------------------------------------
# Reviews Generator
# ---------------------------------------------------------------------------
def generate_reviews(
    orders: List[Dict[str, Any]],
    volume: DataVolumeConfig,
    cfg: ZomatoDataConfig,
) -> List[Dict[str, Any]]:
    """Generate synthetic customer review records."""
    delivered_orders = [o for o in orders if o["order_status"] == "Delivered"]
    reviews = []

    review_sentiments = [
        "Amazing food, will order again!",
        "Decent taste but took too long.",
        "Cold food received, very disappointed.",
        "Best biryani in the city!",
        "Average experience, nothing special.",
        "Packaging was excellent, food was fresh.",
        "Portion size too small for the price.",
        "Loved the variety of options.",
        "Worst experience ever, food was stale.",
        "Quick delivery, hot and fresh food!",
        "Great value for money.",
        "Too spicy for my taste, but quality was good.",
        "The restaurant messed up my order.",
        "Consistent quality every time I order.",
        "Would recommend to friends and family.",
    ]

    for i in range(min(volume.num_reviews, len(delivered_orders))):
        order = delivered_orders[i]
        review_id = _generate_id("rev", i)
        review_ts = datetime.fromisoformat(order["order_placed_at"]) + timedelta(hours=random.randint(1, 48))
        rating = random.choices(
            [1, 2, 3, 4, 5],
            weights=[0.05, 0.08, 0.15, 0.35, 0.37],
        )[0]

        reviews.append(
            {
                "review_id": review_id,
                "order_id": order["order_id"],
                "customer_id": order["customer_id"],
                "restaurant_id": order["restaurant_id"],
                "rating": rating,
                "review_text": (
                    random.choice(review_sentiments) if random.random() < 0.7 else fake.paragraph(nb_sentences=2)
                ),
                "review_timestamp": review_ts.isoformat(),
                "sentiment_label": "Positive" if rating >= 4 else ("Negative" if rating <= 2 else "Neutral"),
                "is_verified_order": True,
                "upvotes": random.randint(0, 50),
                "has_photo": random.choices([True, False], weights=[0.2, 0.8])[0],
                "_ingested_at": datetime.now(UTC).isoformat(),
                "_source_system": "review_svc",
            }
        )

    return reviews
