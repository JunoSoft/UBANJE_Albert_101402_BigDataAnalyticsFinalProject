"""
Spark batch analytics: product co-occurrence recommendations (Part 2).

Implements "users who bought X also bought Y" and "viewed together"
using Spark SQL self-joins (no Python UDFs — more stable on Windows).

Usage:
  python 04_spark/recommendations.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0]
for path in (ROOT, HERE):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import config
from spark_common import clean_transactions, get_spark, read_json


def also_bought(transactions):
    items = (
        transactions.select("transaction_id", F.explode("items").alias("item"))
        .select("transaction_id", F.col("item.product_id").alias("product_id"))
        .dropna()
        .dropDuplicates(["transaction_id", "product_id"])
    )
    a = items.alias("a")
    b = items.alias("b")
    return (
        a.join(b, "transaction_id")
        .where(F.col("a.product_id") < F.col("b.product_id"))
        .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
        .agg(F.count("*").alias("co_purchase_count"))
        .filter(F.col("co_purchase_count") >= 2)
        .orderBy(F.col("co_purchase_count").desc())
    )


def viewed_together(sessions):
    viewed = (
        sessions.select("session_id", F.explode("viewed_products").alias("product_id"))
        .dropna()
        .dropDuplicates(["session_id", "product_id"])
    )
    a = viewed.alias("a")
    b = viewed.alias("b")
    return (
        a.join(b, "session_id")
        .where(F.col("a.product_id") < F.col("b.product_id"))
        .groupBy(F.col("a.product_id").alias("product_a"), F.col("b.product_id").alias("product_b"))
        .agg(F.count("*").alias("co_view_count"))
        .filter(F.col("co_view_count") >= 3)
        .orderBy(F.col("co_view_count").desc())
    )


def main():
    config.ensure_dirs()
    spark = get_spark("AUCA_Recommendations")
    spark.sparkContext.setLogLevel("WARN")

    transactions = clean_transactions(read_json(spark, "transactions.json"))
    pairs = also_bought(transactions)
    top_pairs = [row.asDict() for row in pairs.limit(50).collect()]
    out_path = config.RESULTS_DIR / "spark_also_bought.json"
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(top_pairs, f, indent=2)

    print("Top co-purchased product pairs:")
    for row in top_pairs[:15]:
        print(row)
    print(f"Saved {out_path}")

    sessions = read_json(spark, "sessions_*.json")
    view_pairs = viewed_together(sessions)
    top_view_pairs = [row.asDict() for row in view_pairs.limit(50).collect()]
    view_path = config.RESULTS_DIR / "spark_viewed_together.json"
    with open(view_path, "w", encoding="utf-8") as f:
        json.dump(top_view_pairs, f, indent=2)

    print("Top co-viewed product pairs:")
    for row in top_view_pairs[:10]:
        print(row)
    print(f"Saved {view_path}")

    spark.stop()


if __name__ == "__main__":
    main()
