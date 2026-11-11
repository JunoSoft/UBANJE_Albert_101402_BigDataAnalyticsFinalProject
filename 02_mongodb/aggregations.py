"""
Non-trivial MongoDB aggregation pipelines (Part 1).

1) Revenue analytics by category
2) User segmentation by purchase frequency
3) Top-selling products

Usage:
  python 02_mongodb/aggregations.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pymongo import MongoClient

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config


def save_result(name: str, rows):
    config.ensure_dirs()
    path = config.RESULTS_DIR / f"mongo_{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)
    print(f"Saved {path}")
    return path


def revenue_by_category(db):
    """Unwind line items, join products, aggregate revenue by category."""
    pipeline = [
        {"$unwind": "$items"},
        {
            "$lookup": {
                "from": "products",
                "localField": "items.product_id",
                "foreignField": "product_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$lookup": {
                "from": "categories",
                "localField": "product.category_id",
                "foreignField": "category_id",
                "as": "category",
            }
        },
        {"$unwind": {"path": "$category", "preserveNullAndEmptyArrays": True}},
        {
            "$group": {
                "_id": {
                    "category_id": "$product.category_id",
                    "category_name": "$category.name",
                },
                "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"},
                "orders": {"$addToSet": "$transaction_id"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "category_id": "$_id.category_id",
                "category_name": "$_id.category_name",
                "units_sold": 1,
                "revenue": {"$round": ["$revenue", 2]},
                "order_count": {"$size": "$orders"},
            }
        },
        {"$sort": {"revenue": -1}},
    ]
    return list(db.transactions.aggregate(pipeline, allowDiskUse=True))


def user_segmentation(db):
    """Segment users by purchase frequency using denormalized purchase_summary."""
    pipeline = [
        {
            "$addFields": {
                "segment": {
                    "$switch": {
                        "branches": [
                            {
                                "case": {
                                    "$eq": ["$purchase_summary.order_count", 0]
                                },
                                "then": "non_buyer",
                            },
                            {
                                "case": {
                                    "$lte": ["$purchase_summary.order_count", 2]
                                },
                                "then": "occasional",
                            },
                            {
                                "case": {
                                    "$lte": ["$purchase_summary.order_count", 5]
                                },
                                "then": "regular",
                            },
                        ],
                        "default": "power_buyer",
                    }
                }
            }
        },
        {
            "$group": {
                "_id": "$segment",
                "users": {"$sum": 1},
                "avg_lifetime_spend": {"$avg": "$purchase_summary.lifetime_spend"},
                "avg_orders": {"$avg": "$purchase_summary.order_count"},
            }
        },
        {
            "$project": {
                "_id": 0,
                "segment": "$_id",
                "users": 1,
                "avg_lifetime_spend": {"$round": ["$avg_lifetime_spend", 2]},
                "avg_orders": {"$round": ["$avg_orders", 2]},
            }
        },
        {"$sort": {"users": -1}},
    ]
    return list(db.users.aggregate(pipeline))


def top_selling_products(db, limit=15):
    pipeline = [
        {"$unwind": "$items"},
        {
            "$group": {
                "_id": "$items.product_id",
                "units_sold": {"$sum": "$items.quantity"},
                "revenue": {"$sum": "$items.subtotal"},
            }
        },
        {"$sort": {"units_sold": -1}},
        {"$limit": limit},
        {
            "$lookup": {
                "from": "products",
                "localField": "_id",
                "foreignField": "product_id",
                "as": "product",
            }
        },
        {"$unwind": "$product"},
        {
            "$project": {
                "_id": 0,
                "product_id": "$_id",
                "product_name": "$product.name",
                "category_id": "$product.category_id",
                "units_sold": 1,
                "revenue": {"$round": ["$revenue", 2]},
            }
        },
    ]
    return list(db.transactions.aggregate(pipeline, allowDiskUse=True))


def main():
    client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=8000)
    db = client[config.MONGO_DB]

    print("\n=== Aggregation 1: Revenue by category ===")
    revenue = revenue_by_category(db)
    for row in revenue[:8]:
        print(row)
    save_result("revenue_by_category", revenue)

    print("\n=== Aggregation 2: User segmentation ===")
    segments = user_segmentation(db)
    for row in segments:
        print(row)
    save_result("user_segmentation", segments)

    print("\n=== Aggregation 3: Top-selling products ===")
    top = top_selling_products(db)
    for row in top[:10]:
        print(row)
    save_result("top_selling_products", top)

    client.close()
    print("\nMongoDB aggregations finished.")


if __name__ == "__main__":
    main()
