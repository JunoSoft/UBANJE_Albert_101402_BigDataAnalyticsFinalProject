"""
Load generated JSON into MongoDB and enrich user documents with purchase summaries.

Usage (from project root, with MongoDB running):
  python 02_mongodb/load_mongodb.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

from pymongo import ASCENDING, MongoClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config


def load_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def load_sessions(limit: int | None):
    files = sorted(config.DATA_DIR.glob("sessions_*.json"))
    sessions = []
    for path in files:
        chunk = load_json(path)
        sessions.extend(chunk)
        if limit is not None and len(sessions) >= limit:
            return sessions[:limit]
    return sessions


def build_purchase_summaries(transactions):
    summary = defaultdict(lambda: {
        "order_count": 0,
        "lifetime_spend": 0.0,
        "last_purchase": None,
    })
    for txn in transactions:
        uid = txn["user_id"]
        summary[uid]["order_count"] += 1
        summary[uid]["lifetime_spend"] += float(txn.get("total") or 0)
        ts = txn.get("timestamp")
        if ts and (
            summary[uid]["last_purchase"] is None or ts > summary[uid]["last_purchase"]
        ):
            summary[uid]["last_purchase"] = ts
    for uid, stats in summary.items():
        stats["lifetime_spend"] = round(stats["lifetime_spend"], 2)
        stats["avg_order_value"] = (
            round(stats["lifetime_spend"] / stats["order_count"], 2)
            if stats["order_count"]
            else 0.0
        )
    return summary


def maybe_limit(docs, limit):
    if limit is None:
        return docs
    return docs[:limit]


def main():
    config.ensure_dirs()
    required = ["users.json", "products.json", "categories.json", "transactions.json"]
    for name in required:
        if not (config.DATA_DIR / name).exists():
            raise FileNotFoundError(
                f"Missing {name}. Run: python 01_data_generation/generate_dataset.py"
            )

    print(f"Connecting to MongoDB at {config.MONGO_URI} ...")
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    db = client[config.MONGO_DB]

    users = load_json(config.DATA_DIR / "users.json")
    products = load_json(config.DATA_DIR / "products.json")
    categories = load_json(config.DATA_DIR / "categories.json")
    transactions = maybe_limit(
        load_json(config.DATA_DIR / "transactions.json"),
        config.MONGO_LOAD_LIMITS["transactions"],
    )
    sessions = load_sessions(config.MONGO_LOAD_LIMITS["sessions"])

    summaries = build_purchase_summaries(transactions)
    for user in users:
        stats = summaries.get(
            user["user_id"],
            {
                "order_count": 0,
                "lifetime_spend": 0.0,
                "avg_order_value": 0.0,
                "last_purchase": None,
            },
        )
        user["purchase_summary"] = stats

    users = maybe_limit(users, config.MONGO_LOAD_LIMITS["users"])
    products = maybe_limit(products, config.MONGO_LOAD_LIMITS["products"])
    categories = maybe_limit(categories, config.MONGO_LOAD_LIMITS["categories"])

    print("Dropping and recreating collections...")
    for name in ("users", "products", "categories", "transactions", "sessions"):
        db[name].drop()

    if categories:
        db.categories.insert_many(categories)
    if products:
        db.products.insert_many(products)
    if users:
        db.users.insert_many(users)
    if transactions:
        db.transactions.insert_many(transactions)
    if sessions:
        db.sessions.insert_many(sessions)

    db.categories.create_index([("category_id", ASCENDING)], unique=True)
    db.products.create_index([("product_id", ASCENDING)], unique=True)
    db.products.create_index([("category_id", ASCENDING)])
    db.products.create_index([("is_active", ASCENDING)])
    db.users.create_index([("user_id", ASCENDING)], unique=True)
    db.users.create_index([("purchase_summary.order_count", ASCENDING)])
    db.transactions.create_index([("transaction_id", ASCENDING)], unique=True)
    db.transactions.create_index([("user_id", ASCENDING)])
    db.transactions.create_index([("timestamp", ASCENDING)])
    db.transactions.create_index([("items.product_id", ASCENDING)])
    if sessions:
        db.sessions.create_index([("session_id", ASCENDING)], unique=True)
        db.sessions.create_index([("user_id", ASCENDING)])

    counts = {name: db[name].count_documents({}) for name in
              ("users", "products", "categories", "transactions", "sessions")}
    print("MongoDB load complete:")
    for k, v in counts.items():
        print(f"  {k}: {v:,}")
    client.close()


if __name__ == "__main__":
    main()
