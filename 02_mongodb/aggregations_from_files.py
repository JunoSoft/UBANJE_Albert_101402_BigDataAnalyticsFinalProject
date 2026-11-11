"""
Offline fallback that reproduces the same Mongo aggregation *outputs*
from JSON files (useful if Docker/Mongo is temporarily unavailable).

For the graded demo you should still run load_mongodb.py + aggregations.py
against a live MongoDB. This script only mirrors result files for charts.

Usage:
  python 02_mongodb/aggregations_from_files.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config


def load(name):
    with open(config.DATA_DIR / name, encoding="utf-8") as f:
        return json.load(f)


def save(name, rows):
    config.ensure_dirs()
    path = config.RESULTS_DIR / f"mongo_{name}.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2)
    print(f"Saved {path}")


def main():
    products = {p["product_id"]: p for p in load("products.json")}
    categories = {c["category_id"]: c for c in load("categories.json")}
    transactions = load("transactions.json")
    users = load("users.json")

    # Revenue by category
    cat_stats = defaultdict(lambda: {"units_sold": 0, "revenue": 0.0, "orders": set()})
    product_stats = defaultdict(lambda: {"units_sold": 0, "revenue": 0.0})
    user_orders = defaultdict(lambda: {"order_count": 0, "lifetime_spend": 0.0})

    for txn in transactions:
        user_orders[txn["user_id"]]["order_count"] += 1
        user_orders[txn["user_id"]]["lifetime_spend"] += float(txn.get("total") or 0)
        for item in txn.get("items", []):
            pid = item["product_id"]
            prod = products.get(pid, {})
            cid = prod.get("category_id", "unknown")
            cat_stats[cid]["units_sold"] += int(item.get("quantity") or 0)
            cat_stats[cid]["revenue"] += float(item.get("subtotal") or 0)
            cat_stats[cid]["orders"].add(txn["transaction_id"])
            product_stats[pid]["units_sold"] += int(item.get("quantity") or 0)
            product_stats[pid]["revenue"] += float(item.get("subtotal") or 0)

    revenue_rows = []
    for cid, stats in cat_stats.items():
        revenue_rows.append(
            {
                "category_id": cid,
                "category_name": categories.get(cid, {}).get("name"),
                "units_sold": stats["units_sold"],
                "revenue": round(stats["revenue"], 2),
                "order_count": len(stats["orders"]),
            }
        )
    revenue_rows.sort(key=lambda r: r["revenue"], reverse=True)
    save("revenue_by_category", revenue_rows)

    # User segmentation
    segment_stats = defaultdict(lambda: {"users": 0, "spend": 0.0, "orders": 0})
    for user in users:
        stats = user_orders.get(
            user["user_id"], {"order_count": 0, "lifetime_spend": 0.0}
        )
        oc = stats["order_count"]
        if oc == 0:
            seg = "non_buyer"
        elif oc <= 2:
            seg = "occasional"
        elif oc <= 5:
            seg = "regular"
        else:
            seg = "power_buyer"
        segment_stats[seg]["users"] += 1
        segment_stats[seg]["spend"] += stats["lifetime_spend"]
        segment_stats[seg]["orders"] += oc

    segments = []
    for seg, s in segment_stats.items():
        segments.append(
            {
                "segment": seg,
                "users": s["users"],
                "avg_lifetime_spend": round(s["spend"] / s["users"], 2) if s["users"] else 0,
                "avg_orders": round(s["orders"] / s["users"], 2) if s["users"] else 0,
            }
        )
    segments.sort(key=lambda r: r["users"], reverse=True)
    save("user_segmentation", segments)

    top = []
    for pid, stats in product_stats.items():
        top.append(
            {
                "product_id": pid,
                "product_name": products.get(pid, {}).get("name"),
                "category_id": products.get(pid, {}).get("category_id"),
                "units_sold": stats["units_sold"],
                "revenue": round(stats["revenue"], 2),
            }
        )
    top.sort(key=lambda r: r["units_sold"], reverse=True)
    save("top_selling_products", top[:15])
    print("Offline Mongo-equivalent aggregations complete.")


if __name__ == "__main__":
    main()
