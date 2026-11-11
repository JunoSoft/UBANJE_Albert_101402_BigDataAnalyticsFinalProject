"""
Spark batch job: clean & normalize raw JSON datasets (Part 2).

Writes cleaned Parquet under outputs/results/cleaned/.

Usage:
  python 04_spark/batch_cleaning.py
"""
from __future__ import annotations

import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0]
for path in (ROOT, HERE):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import config
from spark_common import (
    clean_products,
    clean_sessions,
    clean_transactions,
    clean_users,
    get_spark,
    load_core_frames,
)


def main():
    config.ensure_dirs()
    out_dir = config.RESULTS_DIR / "cleaned"
    out_dir.mkdir(parents=True, exist_ok=True)

    spark = get_spark("AUCA_BatchCleaning")
    spark.sparkContext.setLogLevel("WARN")

    users, products, categories, transactions, sessions = load_core_frames(spark)

    users_c = clean_users(users)
    products_c = clean_products(products)
    transactions_c = clean_transactions(transactions)
    sessions_c = clean_sessions(sessions)

    # Normalize category names and explode subcategory for joins later
    categories_c = categories.select(
        "category_id",
        "name",
        "subcategories",
    ).withColumnRenamed("name", "category_name")

    print("Cleaning summary (row counts):")
    for label, df in [
        ("users", users_c),
        ("products", products_c),
        ("categories", categories_c),
        ("transactions", transactions_c),
        ("sessions", sessions_c),
    ]:
        count = df.count()
        print(f"  {label}: {count:,}")
        df.write.mode("overwrite").parquet(str(out_dir / label))

    # Quick data-quality report
    null_txn_users = transactions_c.filter("user_id is null").count()
    inactive = products_c.filter("is_active = false").count()
    report = {
        "null_transaction_user_ids": null_txn_users,
        "inactive_products": inactive,
        "session_status_dist": [
            {**row.asDict()}
            for row in sessions_c.groupBy("conversion_status").count().collect()
        ],
    }
    import json

    with open(config.RESULTS_DIR / "spark_cleaning_report.json", "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)

    print(f"Cleaned parquet written to {out_dir}")
    spark.stop()


if __name__ == "__main__":
    main()
