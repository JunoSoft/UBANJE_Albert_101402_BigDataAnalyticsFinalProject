"""
E-commerce synthetic dataset generator for AUCA Big Data Analytics Final Project.

Produces:
  users.json, products.json, categories.json, transactions.json, sessions_*.json

Usage (from project root):
  python 01_data_generation/generate_dataset.py
  python 01_data_generation/generate_dataset.py --mode FULL
"""
from __future__ import annotations

import argparse
import datetime
import json
import random
import sys
import threading
import uuid
from pathlib import Path

import numpy as np
from faker import Faker

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config


def generate_session_id() -> str:
    return f"sess_{uuid.uuid4().hex[:10]}"


def generate_transaction_id() -> str:
    return f"txn_{uuid.uuid4().hex[:12]}"


class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()

    def update_stock(self, product_id, quantity):
        with self.lock:
            product = self.products.get(product_id)
            if not product:
                return False
            if product["current_stock"] >= quantity:
                product["current_stock"] -= quantity
                return True
            return False

    def get_product(self, product_id):
        with self.lock:
            return self.products.get(product_id)


def determine_page_type(position, previous_pages):
    if position == 0:
        return random.choice(["home", "search", "category_listing"])
    if not previous_pages:
        return "home"

    prev_page = previous_pages[-1]["page_type"]
    transitions = {
        "home": (["category_listing", "search", "product_detail"], [0.5, 0.3, 0.2]),
        "category_listing": (
            ["product_detail", "category_listing", "search", "home"],
            [0.7, 0.1, 0.1, 0.1],
        ),
        "search": (
            ["product_detail", "search", "category_listing", "home"],
            [0.6, 0.2, 0.1, 0.1],
        ),
        "product_detail": (
            ["product_detail", "cart", "category_listing", "search", "home"],
            [0.3, 0.3, 0.2, 0.1, 0.1],
        ),
        "cart": (
            ["checkout", "product_detail", "category_listing", "home"],
            [0.6, 0.2, 0.1, 0.1],
        ),
        "checkout": (["confirmation", "cart", "home"], [0.8, 0.1, 0.1]),
        "confirmation": (
            ["home", "product_detail", "category_listing"],
            [0.6, 0.2, 0.2],
        ),
    }
    options, weights = transitions.get(prev_page, (["home"], [1.0]))
    return random.choices(options, weights=weights)[0]


def get_page_content(page_type, products_list, categories_list):
    if page_type == "product_detail":
        for _ in range(10):
            product = random.choice(products_list)
            if product["is_active"] and product["current_stock"] > 0:
                category = next(
                    (c for c in categories_list if c["category_id"] == product["category_id"]),
                    None,
                )
                return product, category
        product = random.choice(products_list)
        category = next(
            (c for c in categories_list if c["category_id"] == product["category_id"]),
            None,
        )
        return product, category

    if page_type == "category_listing":
        return None, random.choice(categories_list)

    return None, None


def json_serializer(obj):
    if isinstance(obj, (datetime.datetime, datetime.date)):
        return obj.isoformat()
    raise TypeError(f"Type {type(obj)} not serializable")


def build_dataset(profile: dict, out_dir: Path):
    fake = Faker()
    np.random.seed(config.RANDOM_SEED)
    random.seed(config.RANDOM_SEED)
    Faker.seed(config.RANDOM_SEED)

    num_users = profile["NUM_USERS"]
    num_products = profile["NUM_PRODUCTS"]
    num_categories = profile["NUM_CATEGORIES"]
    num_transactions = profile["NUM_TRANSACTIONS"]
    num_sessions = profile["NUM_SESSIONS"]
    timespan_days = profile["TIMESPAN_DAYS"]
    max_iterations = (num_sessions + num_transactions) * 2

    print("Initializing dataset generation...")
    print(f"Mode profile: users={num_users}, products={num_products}, "
          f"sessions={num_sessions}, transactions={num_transactions}")

    categories = []
    for cat_id in range(num_categories):
        category = {
            "category_id": f"cat_{cat_id:03d}",
            "name": fake.company(),
            "subcategories": [],
        }
        for sub_id in range(random.randint(3, 5)):
            category["subcategories"].append(
                {
                    "subcategory_id": f"sub_{cat_id:03d}_{sub_id:02d}",
                    "name": fake.bs().title(),
                    "profit_margin": round(random.uniform(0.1, 0.4), 2),
                }
            )
        categories.append(category)
    print(f"Generated {len(categories)} categories")

    products = []
    product_creation_start = datetime.datetime.now() - datetime.timedelta(days=timespan_days * 2)
    for prod_id in range(num_products):
        category = random.choice(categories)
        subcategory = random.choice(category["subcategories"])
        base_price = round(random.uniform(5, 500), 2)
        price_history = []
        initial_date = fake.date_time_between(
            start_date=product_creation_start,
            end_date=product_creation_start + datetime.timedelta(days=timespan_days // 3),
        )
        price_history.append({"price": base_price, "date": initial_date.isoformat()})
        for _ in range(random.randint(0, 2)):
            price_change_date = fake.date_time_between(start_date=initial_date, end_date="now")
            new_price = round(base_price * random.uniform(0.8, 1.2), 2)
            price_history.append({"price": new_price, "date": price_change_date.isoformat()})
            initial_date = price_change_date
        price_history.sort(key=lambda x: x["date"])
        current_price = price_history[-1]["price"]
        products.append(
            {
                "product_id": f"prod_{prod_id:05d}",
                "name": fake.catch_phrase().title(),
                "category_id": category["category_id"],
                "subcategory_id": subcategory["subcategory_id"],
                "base_price": current_price,
                "current_stock": random.randint(10, 1000),
                "is_active": random.choices([True, False], weights=[0.95, 0.05])[0],
                "price_history": price_history,
                "creation_date": price_history[0]["date"],
            }
        )
    print(f"Generated {len(products)} products")

    users = []
    for user_id in range(num_users):
        reg_date = fake.date_time_between(
            start_date=f"-{timespan_days * 3}d",
            end_date=f"-{timespan_days}d",
        )
        users.append(
            {
                "user_id": f"user_{user_id:06d}",
                "geo_data": {
                    "city": fake.city(),
                    "state": fake.state_abbr(),
                    "country": fake.country_code(),
                },
                "registration_date": reg_date.isoformat(),
                "last_active": fake.date_time_between(
                    start_date=reg_date, end_date="now"
                ).isoformat(),
            }
        )
    print(f"Generated {len(users)} users")

    inventory = InventoryManager(products)
    sessions = []
    transactions = []
    transaction_counter = 0
    session_counter = 0
    iteration = 0

    print("Generating sessions and transactions...")
    while (
        session_counter < num_sessions or transaction_counter < num_transactions
    ) and iteration < max_iterations:
        iteration += 1

        if session_counter < num_sessions:
            user = random.choice(users)
            session_id = generate_session_id()
            session_start = fake.date_time_between(
                start_date=f"-{timespan_days}d", end_date="now"
            )
            session_duration = random.randint(30, 3600)
            page_views = []
            viewed_products = set()
            cart_contents = {}
            time_slots = sorted(
                [0]
                + [
                    random.randint(1, session_duration - 1)
                    for _ in range(random.randint(3, 15))
                ]
                + [session_duration]
            )

            for i in range(len(time_slots) - 1):
                view_duration = time_slots[i + 1] - time_slots[i]
                page_type = determine_page_type(i, page_views)
                product, category = get_page_content(page_type, products, categories)

                if page_type == "product_detail" and product:
                    product_id = product["product_id"]
                    viewed_products.add(product_id)
                    if random.random() < 0.3:
                        if product_id not in cart_contents:
                            cart_contents[product_id] = {
                                "quantity": 0,
                                "price": product["base_price"],
                            }
                        inv_product = inventory.get_product(product_id)
                        max_possible = min(
                            3,
                            inv_product["current_stock"]
                            - cart_contents[product_id]["quantity"],
                        )
                        if max_possible > 0:
                            add_qty = random.randint(1, max_possible)
                            cart_contents[product_id]["quantity"] += add_qty

                page_views.append(
                    {
                        "timestamp": (
                            session_start + datetime.timedelta(seconds=time_slots[i])
                        ).isoformat(),
                        "page_type": page_type,
                        "product_id": product["product_id"] if product else None,
                        "category_id": category["category_id"] if category else None,
                        "view_duration": view_duration,
                    }
                )

            converted = False
            if cart_contents and any(
                p["page_type"] in ["checkout", "confirmation"] for p in page_views
            ):
                converted = random.random() < 0.7

            session_geo = user["geo_data"].copy()
            session_geo["ip_address"] = fake.ipv4()
            sessions.append(
                {
                    "session_id": session_id,
                    "user_id": user["user_id"],
                    "start_time": session_start.isoformat(),
                    "end_time": (
                        session_start + datetime.timedelta(seconds=session_duration)
                    ).isoformat(),
                    "duration_seconds": session_duration,
                    "geo_data": session_geo,
                    "device_profile": {
                        "type": random.choice(["mobile", "desktop", "tablet"]),
                        "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
                        "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
                    },
                    "viewed_products": list(viewed_products),
                    "page_views": page_views,
                    "cart_contents": {
                        k: v for k, v in cart_contents.items() if v["quantity"] > 0
                    },
                    "conversion_status": (
                        "converted"
                        if converted
                        else "abandoned"
                        if cart_contents
                        else "browsed"
                    ),
                    "referrer": random.choice(
                        ["direct", "email", "social", "search_engine", "affiliate"]
                    ),
                }
            )
            session_counter += 1

            if converted and transaction_counter < num_transactions:
                transaction_items = []
                valid = True
                for prod_id, details in cart_contents.items():
                    quantity = details["quantity"]
                    if quantity > 0:
                        if inventory.update_stock(prod_id, quantity):
                            transaction_items.append(
                                {
                                    "product_id": prod_id,
                                    "quantity": quantity,
                                    "unit_price": details["price"],
                                    "subtotal": round(quantity * details["price"], 2),
                                }
                            )
                        else:
                            valid = False
                            break

                if valid and transaction_items:
                    subtotal = sum(item["subtotal"] for item in transaction_items)
                    discount = 0
                    if random.random() < 0.2:
                        discount = round(
                            subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2
                        )
                    total = round(subtotal - discount, 2)
                    transactions.append(
                        {
                            "transaction_id": generate_transaction_id(),
                            "session_id": session_id,
                            "user_id": user["user_id"],
                            "timestamp": (
                                session_start
                                + datetime.timedelta(seconds=session_duration)
                            ).isoformat(),
                            "items": transaction_items,
                            "subtotal": subtotal,
                            "discount": discount,
                            "total": total,
                            "payment_method": random.choice(
                                ["credit_card", "paypal", "apple_pay", "crypto"]
                            ),
                            "status": "completed",
                        }
                    )
                    transaction_counter += 1

        if transaction_counter < num_transactions and random.random() < 0.2:
            user = random.choice(users)
            products_in_txn = random.sample(products, k=min(3, len(products)))
            transaction_items = []
            for product in products_in_txn:
                if product["is_active"]:
                    quantity = random.randint(1, 3)
                    if inventory.update_stock(product["product_id"], quantity):
                        transaction_items.append(
                            {
                                "product_id": product["product_id"],
                                "quantity": quantity,
                                "unit_price": product["base_price"],
                                "subtotal": round(
                                    quantity * product["base_price"], 2
                                ),
                            }
                        )
            if transaction_items:
                subtotal = sum(item["subtotal"] for item in transaction_items)
                discount = 0
                if random.random() < 0.2:
                    discount = round(
                        subtotal * random.choice([0.05, 0.1, 0.15, 0.2]), 2
                    )
                total = round(subtotal - discount, 2)
                transactions.append(
                    {
                        "transaction_id": generate_transaction_id(),
                        "session_id": None,
                        "user_id": user["user_id"],
                        "timestamp": fake.date_time_between(
                            start_date=f"-{timespan_days}d", end_date="now"
                        ).isoformat(),
                        "items": transaction_items,
                        "subtotal": subtotal,
                        "discount": discount,
                        "total": total,
                        "payment_method": random.choice(
                            ["credit_card", "paypal", "bank_transfer", "gift_card"]
                        ),
                        "status": random.choice(
                            ["completed", "processing", "shipped", "delivered"]
                        ),
                    }
                )
                transaction_counter += 1

        if iteration % 10000 == 0:
            print(
                f"Progress: {session_counter:,}/{num_sessions:,} sessions, "
                f"{transaction_counter:,}/{num_transactions:,} transactions "
                f"(iteration {iteration:,})"
            )

    out_dir.mkdir(parents=True, exist_ok=True)
    print("Saving datasets...")

    with open(out_dir / "users.json", "w", encoding="utf-8") as f:
        json.dump(users, f, default=json_serializer)
    with open(out_dir / "products.json", "w", encoding="utf-8") as f:
        json.dump(list(inventory.products.values()), f, default=json_serializer)
    with open(out_dir / "categories.json", "w", encoding="utf-8") as f:
        json.dump(categories, f, default=json_serializer)
    with open(out_dir / "transactions.json", "w", encoding="utf-8") as f:
        json.dump(transactions, f, default=json_serializer)

    chunk_size = 100000
    for i in range(0, len(sessions), chunk_size):
        chunk = sessions[i : i + chunk_size]
        with open(out_dir / f"sessions_{i // chunk_size}.json", "w", encoding="utf-8") as f:
            json.dump(chunk, f, default=json_serializer)

    meta = {
        "mode": config.DATASET_MODE,
        "profile": profile,
        "counts": {
            "users": len(users),
            "products": len(products),
            "categories": len(categories),
            "sessions": len(sessions),
            "transactions": len(transactions),
        },
        "generated_at": datetime.datetime.now().isoformat(),
    }
    with open(out_dir / "dataset_meta.json", "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=2)

    print(
        f"""
Dataset generation complete!
- Users: {len(users):,}
- Products: {len(products):,}
- Categories: {len(categories):,}
- Sessions: {len(sessions):,} (target: {num_sessions:,})
- Transactions: {len(transactions):,} (target: {num_transactions:,})
- Output: {out_dir}
"""
    )


def main():
    parser = argparse.ArgumentParser(description="Generate e-commerce analytics dataset")
    parser.add_argument(
        "--mode",
        choices=["DEMO", "FULL"],
        default=None,
        help="Override config.DATASET_MODE",
    )
    args = parser.parse_args()
    if args.mode:
        config.DATASET_MODE = args.mode

    config.ensure_dirs()
    profile = config.get_dataset_profile()
    build_dataset(profile, config.DATA_DIR)


if __name__ == "__main__":
    main()
