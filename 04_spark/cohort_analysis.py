"""
Optional alternate Part 2 analysis: cohort spending by registration month.

Usage:
  python 04_spark/cohort_analysis.py
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
from spark_common import clean_transactions, clean_users, get_spark, read_json


def main():
    config.ensure_dirs()
    spark = get_spark("AUCA_CohortAnalysis")
    spark.sparkContext.setLogLevel("WARN")

    users = clean_users(read_json(spark, "users.json")).withColumn(
        "cohort_month", F.date_format("registration_date", "yyyy-MM")
    )
    txns = clean_transactions(read_json(spark, "transactions.json")).withColumn(
        "purchase_month", F.date_format("timestamp", "yyyy-MM")
    )

    cohort = (
        users.join(txns, "user_id", "left")
        .groupBy("cohort_month", "purchase_month")
        .agg(
            F.countDistinct("user_id").alias("active_buyers"),
            F.round(F.sum("total"), 2).alias("revenue"),
            F.round(F.avg("total"), 2).alias("avg_order_value"),
        )
        .orderBy("cohort_month", "purchase_month")
    )

    rows = [r.asDict() for r in cohort.collect()]
    out = config.RESULTS_DIR / "spark_cohort_analysis.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(rows, f, indent=2, default=str)

    print("Sample cohort rows:")
    for row in rows[:20]:
        print(row)
    print(f"Saved {out}")
    spark.stop()


if __name__ == "__main__":
    main()
