"""
Spark SQL analytics (Part 2).

Demonstrates complex SQL over DataFrames that notionally represent
MongoDB (users/products/transactions) and HBase-style session metrics.

Usage:
  python 04_spark/spark_sql_analytics.py
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
    spark = get_spark("AUCA_SparkSQL")
    spark.sparkContext.setLogLevel("WARN")

    users, products, categories, transactions, sessions = load_core_frames(spark)
    users = clean_users(users)
    products = clean_products(products)
    transactions = clean_transactions(transactions)
    sessions = clean_sessions(sessions)

    # Notionally: MongoDB entities
    users.createOrReplaceTempView("mongo_users")
    products.createOrReplaceTempView("mongo_products")
    transactions.createOrReplaceTempView("mongo_transactions")
    categories.createOrReplaceTempView("mongo_categories")

    # Notionally: HBase session activity
    sessions.createOrReplaceTempView("hbase_sessions")

    # Q1: Revenue + order volume by payment method and month
    q1 = spark.sql(
        """
        SELECT
          date_format(timestamp, 'yyyy-MM') AS month,
          payment_method,
          COUNT(*) AS orders,
          ROUND(SUM(total), 2) AS revenue,
          ROUND(AVG(total), 2) AS avg_order_value
        FROM mongo_transactions
        WHERE status IN ('completed', 'shipped', 'delivered', 'processing')
        GROUP BY 1, 2
        ORDER BY month, revenue DESC
        """
    )
    q1_rows = [r.asDict() for r in q1.collect()]

    # Q2: Join "Mongo" users with "HBase" sessions for engagement vs spend
    q2 = spark.sql(
        """
        WITH session_stats AS (
          SELECT
            user_id,
            COUNT(*) AS session_count,
            ROUND(AVG(duration_seconds), 1) AS avg_duration,
            SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS converted_sessions
          FROM hbase_sessions
          GROUP BY user_id
        ),
        spend AS (
          SELECT user_id, ROUND(SUM(total), 2) AS lifetime_spend, COUNT(*) AS orders
          FROM mongo_transactions
          GROUP BY user_id
        )
        SELECT
          u.user_id,
          u.geo_data.country AS country,
          COALESCE(s.session_count, 0) AS session_count,
          COALESCE(s.avg_duration, 0) AS avg_duration,
          COALESCE(s.converted_sessions, 0) AS converted_sessions,
          COALESCE(sp.orders, 0) AS orders,
          COALESCE(sp.lifetime_spend, 0) AS lifetime_spend
        FROM mongo_users u
        LEFT JOIN session_stats s ON u.user_id = s.user_id
        LEFT JOIN spend sp ON u.user_id = sp.user_id
        ORDER BY lifetime_spend DESC
        LIMIT 50
        """
    )
    q2_rows = [r.asDict() for r in q2.collect()]

    # Q3: Top categories by revenue (SQL join path)
    items = transactions.select(
        "transaction_id",
        F.explode("items").alias("item"),
    ).select(
        "transaction_id",
        F.col("item.product_id").alias("product_id"),
        F.col("item.quantity").alias("quantity"),
        F.col("item.subtotal").alias("subtotal"),
    )
    items.createOrReplaceTempView("txn_items")

    q3 = spark.sql(
        """
        SELECT
          p.category_id,
          c.name AS category_name,
          SUM(i.quantity) AS units_sold,
          ROUND(SUM(i.subtotal), 2) AS revenue
        FROM txn_items i
        JOIN mongo_products p ON i.product_id = p.product_id
        LEFT JOIN mongo_categories c ON p.category_id = c.category_id
        GROUP BY p.category_id, c.name
        ORDER BY revenue DESC
        """
    )
    q3_rows = [r.asDict() for r in q3.collect()]

    results = {
        "revenue_by_payment_month": q1_rows,
        "engagement_vs_spend_top50": q2_rows,
        "revenue_by_category": q3_rows,
    }
    out = config.RESULTS_DIR / "spark_sql_results.json"
    with open(out, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, default=str)

    print("Spark SQL Q1 sample:")
    for row in q1_rows[:8]:
        print(row)
    print("\nSpark SQL Q3 sample:")
    for row in q3_rows[:8]:
        print(row)
    print(f"\nSaved {out}")
    spark.stop()


if __name__ == "__main__":
    main()
