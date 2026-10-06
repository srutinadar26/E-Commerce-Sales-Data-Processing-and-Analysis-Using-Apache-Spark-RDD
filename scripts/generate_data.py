"""
scripts/generate_data.py
------------------------
Generates a realistic e-commerce sales CSV dataset (~15 000 records) and
saves it to  data/raw/ecommerce_sales.csv.

The generator deliberately introduces:
  • ~2 % missing values (None / empty strings)
  • ~1 % exact duplicate rows
  • ~1.5 % invalid values (negative prices, impossible ratings, etc.)

Run
---
    python scripts/generate_data.py
"""

import os
import sys
import random
import csv
from datetime import date, timedelta

# -- Allow imports from the project root --------------------------------------
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.logger import get_logger, load_config

logger = get_logger(__name__)

# -----------------------------------------------------------------------------
# Reference data
# -----------------------------------------------------------------------------
CATEGORIES = {
    "Electronics": [
        "Laptop Pro 15", "Gaming Keyboard", "Wireless Mouse", "4K Monitor",
        "USB-C Hub", "Noise Cancelling Headphones", "Smart Watch Series 5",
        "Tablet Ultra", "Portable SSD 1TB", "Webcam HD 1080p",
        "Mechanical Keyboard RGB", "Gaming Headset 7.1", "LED Desk Lamp",
        "Phone Stand Adjustable", "Bluetooth Speaker Portable",
    ],
    "Clothing": [
        "Men's Slim Fit Jeans", "Women's Floral Dress", "Cotton T-Shirt Pack",
        "Winter Jacket Parka", "Sports Leggings Pro", "Casual Hoodie",
        "Running Shoes Air", "Formal Shirt White", "Yoga Pants Flex",
        "Rain Coat Waterproof",
    ],
    "Books": [
        "Python Crash Course", "Clean Code", "Data Science Handbook",
        "The Pragmatic Programmer", "Design Patterns", "Machine Learning Basics",
        "Deep Learning with PyTorch", "SQL for Beginners", "Linux Command Line",
        "Algorithms Unlocked",
    ],
    "Home & Kitchen": [
        "Non-stick Cookware Set", "Air Fryer XL", "Coffee Maker Drip",
        "Blender Pro 1000W", "Instant Pot Duo", "Knife Set Premium",
        "Cutting Board Bamboo", "Dish Rack Stainless", "Food Processor 700W",
        "Electric Kettle Glass",
    ],
    "Sports": [
        "Yoga Mat Non-slip", "Resistance Bands Set", "Dumbbell Set 20kg",
        "Jump Rope Speed", "Foam Roller Massage", "Running Belt Phone",
        "Gym Gloves Padded", "Pull-up Bar Doorway", "Protein Shaker Bottle",
        "Exercise Bike Foldable",
    ],
    "Beauty": [
        "Vitamin C Serum", "Moisturising Face Cream", "Mascara Waterproof",
        "Foundation Natural 30ml", "Lip Gloss Set 12pc", "Eye Shadow Palette",
        "Shampoo Argan Oil", "Conditioner Deep Repair", "Nail Polish Kit",
        "Sunscreen SPF 50",
    ],
    "Toys": [
        "LEGO City Set 500pc", "RC Car Off-Road", "Wooden Puzzle 100pc",
        "Board Game Strategy", "Stuffed Animal Bear", "Action Figure Pack",
        "Slime Making Kit", "Magnetic Drawing Board", "Card Game Uno",
        "Building Blocks 200pc",
    ],
    "Automotive": [
        "Car Phone Mount", "Dash Cam Full HD", "Seat Cover Universal",
        "Car Vacuum Cleaner", "Tyre Inflator Portable", "Jump Starter 1000A",
        "Car Air Freshener", "Steering Wheel Cover", "OBD2 Scanner",
        "Windshield Sun Shade",
    ],
}

PRODUCT_PRICES = {
    # Electronics
    "Laptop Pro 15": 1299.99, "Gaming Keyboard": 89.99, "Wireless Mouse": 45.99,
    "4K Monitor": 399.99, "USB-C Hub": 39.99, "Noise Cancelling Headphones": 249.99,
    "Smart Watch Series 5": 299.99, "Tablet Ultra": 549.99, "Portable SSD 1TB": 109.99,
    "Webcam HD 1080p": 79.99, "Mechanical Keyboard RGB": 129.99,
    "Gaming Headset 7.1": 99.99, "LED Desk Lamp": 29.99,
    "Phone Stand Adjustable": 19.99, "Bluetooth Speaker Portable": 59.99,
    # Clothing
    "Men's Slim Fit Jeans": 49.99, "Women's Floral Dress": 59.99,
    "Cotton T-Shirt Pack": 24.99, "Winter Jacket Parka": 149.99,
    "Sports Leggings Pro": 44.99, "Casual Hoodie": 39.99,
    "Running Shoes Air": 89.99, "Formal Shirt White": 34.99,
    "Yoga Pants Flex": 39.99, "Rain Coat Waterproof": 79.99,
    # Books
    "Python Crash Course": 29.99, "Clean Code": 34.99,
    "Data Science Handbook": 49.99, "The Pragmatic Programmer": 39.99,
    "Design Patterns": 44.99, "Machine Learning Basics": 39.99,
    "Deep Learning with PyTorch": 54.99, "SQL for Beginners": 24.99,
    "Linux Command Line": 29.99, "Algorithms Unlocked": 34.99,
    # Home & Kitchen
    "Non-stick Cookware Set": 129.99, "Air Fryer XL": 99.99,
    "Coffee Maker Drip": 79.99, "Blender Pro 1000W": 89.99,
    "Instant Pot Duo": 119.99, "Knife Set Premium": 59.99,
    "Cutting Board Bamboo": 19.99, "Dish Rack Stainless": 29.99,
    "Food Processor 700W": 74.99, "Electric Kettle Glass": 39.99,
    # Sports
    "Yoga Mat Non-slip": 34.99, "Resistance Bands Set": 24.99,
    "Dumbbell Set 20kg": 89.99, "Jump Rope Speed": 14.99,
    "Foam Roller Massage": 29.99, "Running Belt Phone": 19.99,
    "Gym Gloves Padded": 19.99, "Pull-up Bar Doorway": 34.99,
    "Protein Shaker Bottle": 12.99, "Exercise Bike Foldable": 249.99,
    # Beauty
    "Vitamin C Serum": 24.99, "Moisturising Face Cream": 29.99,
    "Mascara Waterproof": 14.99, "Foundation Natural 30ml": 34.99,
    "Lip Gloss Set 12pc": 19.99, "Eye Shadow Palette": 29.99,
    "Shampoo Argan Oil": 17.99, "Conditioner Deep Repair": 16.99,
    "Nail Polish Kit": 22.99, "Sunscreen SPF 50": 19.99,
    # Toys
    "LEGO City Set 500pc": 59.99, "RC Car Off-Road": 49.99,
    "Wooden Puzzle 100pc": 19.99, "Board Game Strategy": 34.99,
    "Stuffed Animal Bear": 24.99, "Action Figure Pack": 29.99,
    "Slime Making Kit": 14.99, "Magnetic Drawing Board": 19.99,
    "Card Game Uno": 9.99, "Building Blocks 200pc": 29.99,
    # Automotive
    "Car Phone Mount": 19.99, "Dash Cam Full HD": 79.99,
    "Seat Cover Universal": 49.99, "Car Vacuum Cleaner": 39.99,
    "Tyre Inflator Portable": 44.99, "Jump Starter 1000A": 89.99,
    "Car Air Freshener": 9.99, "Steering Wheel Cover": 19.99,
    "OBD2 Scanner": 39.99, "Windshield Sun Shade": 14.99,
}

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "UPI", "Net Banking", "Cash on Delivery"]

ORDER_STATUSES = ["Delivered", "Shipped", "Processing", "Cancelled", "Returned"]
STATUS_WEIGHTS = [0.55, 0.20, 0.10, 0.10, 0.05]

CITY_STATE = [
    ("New York", "New York"), ("Los Angeles", "California"),
    ("Chicago", "Illinois"), ("Houston", "Texas"), ("Phoenix", "Arizona"),
    ("Philadelphia", "Pennsylvania"), ("San Antonio", "Texas"),
    ("San Diego", "California"), ("Dallas", "Texas"), ("San Jose", "California"),
    ("Austin", "Texas"), ("Jacksonville", "Florida"), ("Fort Worth", "Texas"),
    ("Columbus", "Ohio"), ("Charlotte", "North Carolina"),
    ("San Francisco", "California"), ("Indianapolis", "Indiana"),
    ("Seattle", "Washington"), ("Denver", "Colorado"), ("Nashville", "Tennessee"),
    ("Oklahoma City", "Oklahoma"), ("El Paso", "Texas"), ("Boston", "Massachusetts"),
    ("Portland", "Oregon"), ("Las Vegas", "Nevada"), ("Memphis", "Tennessee"),
    ("Louisville", "Kentucky"), ("Baltimore", "Maryland"), ("Milwaukee", "Wisconsin"),
    ("Albuquerque", "New Mexico"),
]

DISCOUNTS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.30]
DISCOUNT_WEIGHTS = [0.35, 0.20, 0.20, 0.10, 0.08, 0.04, 0.03]


# -----------------------------------------------------------------------------
# Helper utilities
# -----------------------------------------------------------------------------

def random_date(start: date, end: date) -> date:
    delta = (end - start).days
    return start + timedelta(days=random.randint(0, delta))


def weighted_choice(choices, weights):
    total = sum(weights)
    r = random.uniform(0, total)
    cumulative = 0
    for c, w in zip(choices, weights):
        cumulative += w
        if r <= cumulative:
            return c
    return choices[-1]


def generate_product_id(product_name: str) -> str:
    slug = product_name.upper().replace(" ", "_")[:8]
    return f"PROD_{slug}_{random.randint(1000, 9999)}"


# -----------------------------------------------------------------------------
# Core record generator
# -----------------------------------------------------------------------------

def generate_clean_record(txn_id: int) -> dict:
    """Generate a single valid e-commerce transaction record."""
    category = random.choice(list(CATEGORIES.keys()))
    product_name = random.choice(CATEGORIES[category])
    unit_price = PRODUCT_PRICES[product_name]
    quantity = random.randint(1, 5)
    discount = weighted_choice(DISCOUNTS, DISCOUNT_WEIGHTS)
    city, state = random.choice(CITY_STATE)
    txn_date = random_date(date(2023, 1, 1), date(2024, 12, 31))

    return {
        "transaction_id": f"TXN{txn_id:07d}",
        "date": txn_date.strftime("%Y-%m-%d"),
        "customer_id": f"CUST{random.randint(1, 3000):05d}",
        "product_id": f"PROD{hash(product_name) % 90000 + 10000:05d}",
        "product_name": product_name,
        "category": category,
        "quantity": quantity,
        "unit_price": unit_price,
        "discount": discount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "city": city,
        "state": state,
        "order_status": weighted_choice(ORDER_STATUSES, STATUS_WEIGHTS),
        "rating": round(random.uniform(1.0, 5.0), 1),
    }


def introduce_dirty_data(records: list, cfg: dict) -> list:
    """
    Introduce controlled noise into the dataset:
      • Missing values  (~missing_rate)
      • Exact duplicates (~duplicate_rate)
      • Invalid values  (~invalid_rate)
    """
    n = len(records)
    missing_rate = cfg["data_generation"]["missing_rate"]
    duplicate_rate = cfg["data_generation"]["duplicate_rate"]
    invalid_rate = cfg["data_generation"]["invalid_rate"]

    nullable_fields = ["rating", "payment_method", "city", "state", "discount"]

    # --- Missing values -------------------------------------------------------
    num_missing = int(n * missing_rate)
    for _ in range(num_missing):
        idx = random.randint(0, n - 1)
        field = random.choice(nullable_fields)
        records[idx][field] = ""

    # --- Invalid values -------------------------------------------------------
    num_invalid = int(n * invalid_rate)
    for _ in range(num_invalid):
        idx = random.randint(0, n - 1)
        choice = random.randint(0, 3)
        if choice == 0:
            records[idx]["unit_price"] = -abs(records[idx]["unit_price"])  # negative price
        elif choice == 1:
            records[idx]["rating"] = random.choice([0.0, 6.5, -1.0])       # out-of-range rating
        elif choice == 2:
            records[idx]["quantity"] = random.choice([-1, 0])               # invalid qty
        else:
            records[idx]["discount"] = random.choice([-0.1, 1.5])           # invalid discount

    # --- Duplicates -----------------------------------------------------------
    num_duplicates = int(n * duplicate_rate)
    duplicates = [
        dict(records[random.randint(0, n - 1)]) for _ in range(num_duplicates)
    ]
    records.extend(duplicates)
    random.shuffle(records)

    return records


# -----------------------------------------------------------------------------
# Main entry point
# -----------------------------------------------------------------------------

def main():
    cfg = load_config()
    num_records = cfg["data_generation"]["num_records"]

    # Resolve output path relative to project root
    base = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    output_path = os.path.join(base, cfg["paths"]["raw_data"])
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    logger.info("Generating %d base records …", num_records)
    records = [generate_clean_record(i + 1) for i in range(num_records)]

    logger.info("Introducing dirty data …")
    records = introduce_dirty_data(records, cfg)

    fieldnames = [
        "transaction_id", "date", "customer_id", "product_id", "product_name",
        "category", "quantity", "unit_price", "discount", "payment_method",
        "city", "state", "order_status", "rating",
    ]

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(records)

    logger.info("Dataset saved --> %s  (%d rows)", output_path, len(records))
    return output_path


if __name__ == "__main__":
    main()
