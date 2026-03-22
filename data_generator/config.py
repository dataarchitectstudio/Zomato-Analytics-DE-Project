"""
Data Generator Configuration
Centralized configuration for fake data generation parameters.
"""

import os
from dataclasses import dataclass, field
from typing import List


@dataclass
class DataVolumeConfig:
    """Controls the volume of generated data for each entity."""

    num_customers: int = 10_000
    num_restaurants: int = 2_500
    num_orders: int = 150_000
    num_deliveries: int = 145_000
    num_reviews: int = 120_000
    num_menu_items_per_restaurant: int = 25


@dataclass
class ZomatoDataConfig:
    """Domain-specific configuration for Zomato data generation."""

    # --- Geography ---
    cities: List[str] = field(
        default_factory=lambda: [
            "Mumbai",
            "Delhi",
            "Bangalore",
            "Hyderabad",
            "Chennai",
            "Kolkata",
            "Pune",
            "Ahmedabad",
            "Jaipur",
            "Lucknow",
            "Chandigarh",
            "Goa",
            "Kochi",
            "Indore",
            "Nagpur",
        ]
    )

    # --- Cuisine Types ---
    cuisines: List[str] = field(
        default_factory=lambda: [
            "North Indian",
            "South Indian",
            "Chinese",
            "Italian",
            "Continental",
            "Mughlai",
            "Street Food",
            "Biryani",
            "Desserts",
            "Beverages",
            "Fast Food",
            "Thai",
            "Japanese",
            "Mexican",
            "Mediterranean",
            "Korean",
            "Bengali",
            "Rajasthani",
            "Gujarati",
            "Kerala",
        ]
    )

    # --- Restaurant Types ---
    restaurant_types: List[str] = field(
        default_factory=lambda: [
            "Quick Bites",
            "Casual Dining",
            "Fine Dining",
            "Cafe",
            "Bakery",
            "Cloud Kitchen",
            "Food Court",
            "Dhaba",
            "Lounge",
            "Microbrewery",
        ]
    )

    # --- Payment Methods ---
    payment_methods: List[str] = field(
        default_factory=lambda: [
            "UPI",
            "Credit Card",
            "Debit Card",
            "Cash on Delivery",
            "Wallet",
            "Net Banking",
        ]
    )

    # --- Order Statuses ---
    order_statuses: List[str] = field(
        default_factory=lambda: [
            "Placed",
            "Confirmed",
            "Preparing",
            "Out for Delivery",
            "Delivered",
            "Cancelled",
            "Refunded",
        ]
    )

    # --- Delivery Statuses ---
    delivery_statuses: List[str] = field(
        default_factory=lambda: [
            "Assigned",
            "Picked Up",
            "In Transit",
            "Delivered",
            "Failed",
            "Returned",
        ]
    )

    # --- Platform Channels ---
    order_platforms: List[str] = field(
        default_factory=lambda: [
            "Android App",
            "iOS App",
            "Web",
            "Partner API",
        ]
    )

    # --- Subscription Tiers ---
    subscription_tiers: List[str] = field(
        default_factory=lambda: [
            "Free",
            "Zomato Pro",
            "Zomato Pro Plus",
            "Zomato Gold",
        ]
    )

    # --- Rating Range ---
    min_rating: float = 1.0
    max_rating: float = 5.0

    # --- Price Range (INR) ---
    min_item_price: float = 49.0
    max_item_price: float = 1_499.0
    min_delivery_fee: float = 0.0
    max_delivery_fee: float = 80.0

    # --- Discount Configuration ---
    discount_probability: float = 0.35
    max_discount_pct: float = 50.0

    # --- Temporal ---
    data_start_date: str = "2024-01-01"
    data_end_date: str = "2025-12-31"


# --- Output Paths ---
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "raw")
OUTPUT_FORMAT = "parquet"  # parquet | csv | json
