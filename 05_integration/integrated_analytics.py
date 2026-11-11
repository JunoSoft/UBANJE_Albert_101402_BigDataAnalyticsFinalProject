"""
Part 3 — Integrated analytics across MongoDB-style + HBase-style + Spark.

Implements two business workflows:
  1) Funnel conversion analysis (sessions → cart → purchase)
  2) Customer Lifetime Value (CLV) estimation using profile + spend + engagement

Usage:
  python 05_integration/integrated_analytics.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import functions as F

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0]
SPARK_DIR = ROOT / "04_spark"
for path in (ROOT, HERE, SPARK_DIR):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import config
from spark_common import (
    clean_sessions,
    clean_transactions,
    clean_users,
    get_spark,
    load_core_frames,
)


def funnel_analysis(sessions, transactions):
    """
    Business question:
      Where do users drop off between browse → product view → cart → purchase?

    Sources:
      - HBase-notionally: session page_views / conversion_status
      - MongoDB-notionally: transactions linked by session_id
      - Spark: explode, aggregate, join
    """
    page_events = sessions.select(
        "session_id",
        "user_id",
        "conversion_status",
        "referrer",
        F.explode("page_views").alias("pv"),
    ).select(
        "session_id",
        "user_id",
        "conversion_status",
        "referrer",
        F.col("pv.page_type").alias("page_type"),
    )

    stage_flags = page_events.groupBy("session_id", "user_id", "conversion_status", "referrer").agg(
        F.max(F.when(F.col("page_type").isin("home", "search", "category_listing"), 1).otherwise(0)).alias("stage_browse"),
        F.max(F.when(F.col("page_type") == "product_detail", 1).otherwise(0)).alias("stage_product"),
        F.max(F.when(F.col("page_type") == "cart", 1).otherwise(0)).alias("stage_cart"),
        F.max(F.when(F.col("page_type").isin("checkout", "confirmation"), 1).otherwise(0)).alias("stage_checkout"),
    )

    purchased_sessions = (
        transactions.filter(F.col("session_id").isNotNull())
        .select(F.col("session_id").alias("purchased_session"))
        .dropDuplicates()
    )

    funnel = (
        stage_flags.join(
            purchased_sessions,
            stage_flags.session_id == purchased_sessions.purchased_session,
            "left",
        )
        .withColumn(
            "stage_purchase",
            F.when(F.col("purchased_session").isNotNull(), 1).otherwise(0),
        )
    )

    summary = funnel.agg(
        F.count("*").alias("sessions"),
        F.sum("stage_browse").alias("browse"),
        F.sum("stage_product").alias("product_view"),
        F.sum("stage_cart").alias("cart"),
        F.sum("stage_checkout").alias("checkout"),
        F.sum("stage_purchase").alias("purchase"),
    ).collect()[0].asDict()

    # Conversion rates relative to previous stage
    stages = ["browse", "product_view", "cart", "checkout", "purchase"]
    rates = {}
    for i, stage in enumerate(stages):
        if i == 0:
            rates[stage] = 1.0
        else:
            prev = summary[stages[i - 1]] or 1
            rates[stage] = round((summary[stage] or 0) / prev, 4)

    by_referrer = (
        funnel.groupBy("referrer")
        .agg(
            F.count("*").alias("sessions"),
            F.sum("stage_purchase").alias("purchases"),
        )
        .withColumn(
            "conversion_rate",
            F.round(F.col("purchases") / F.col("sessions"), 4),
        )
        .orderBy(F.col("conversion_rate").desc())
    )

    return {
        "business_question": "Where do users drop off in the purchase funnel, and which referrers convert best?",
        "data_sources": {
            "sessions_page_views": "HBase user_sessions / events",
            "transactions": "MongoDB transactions",
            "join_engine": "Apache Spark",
        },
        "funnel_counts": summary,
        "stage_conversion_rates": rates,
        "conversion_by_referrer": [r.asDict() for r in by_referrer.collect()],
    }


def clv_estimation(users, sessions, transactions):
    """
    Business question:
      Who are high-value customers when we combine spend + engagement?

    Sources:
      - MongoDB users + transactions
      - HBase-derived session engagement
      - Spark window scoring
    """
    spend = transactions.groupBy("user_id").agg(
        F.count("*").alias("orders"),
        F.round(F.sum("total"), 2).alias("lifetime_spend"),
        F.round(F.avg("total"), 2).alias("avg_order_value"),
        F.max("timestamp").alias("last_purchase"),
    )

    engagement = sessions.groupBy("user_id").agg(
        F.count("*").alias("sessions"),
        F.round(F.avg("duration_seconds"), 1).alias("avg_session_sec"),
        F.sum(
            F.when(F.col("conversion_status") == "converted", 1).otherwise(0)
        ).alias("converted_sessions"),
    )

    features = (
        users.select("user_id", "registration_date", F.col("geo_data.country").alias("country"))
        .join(spend, "user_id", "left")
        .join(engagement, "user_id", "left")
        .fillna(
            {
                "orders": 0,
                "lifetime_spend": 0.0,
                "avg_order_value": 0.0,
                "sessions": 0,
                "avg_session_sec": 0.0,
                "converted_sessions": 0,
            }
        )
    )

    # Simple explainable CLV score: spend + engagement bonus
    scored = features.withColumn(
        "clv_score",
        F.round(
            F.col("lifetime_spend")
            + (F.col("sessions") * 0.5)
            + (F.col("converted_sessions") * 5.0),
            2,
        ),
    )

    quantiles = scored.approxQuantile("clv_score", [0.4, 0.7, 0.9], 0.01)
    if len(quantiles) == 3:
        p40, p70, p90 = quantiles
    else:
        p40 = p70 = p90 = 0.0

    scored = scored.withColumn(
        "value_tier",
        F.when(F.col("clv_score") >= F.lit(float(p90)), "platinum")
        .when(F.col("clv_score") >= F.lit(float(p70)), "gold")
        .when(F.col("clv_score") >= F.lit(float(p40)), "silver")
        .otherwise("bronze"),
    )

    tier_summary = (
        scored.groupBy("value_tier")
        .agg(
            F.count("*").alias("users"),
            F.round(F.avg("lifetime_spend"), 2).alias("avg_spend"),
            F.round(F.avg("sessions"), 2).alias("avg_sessions"),
            F.round(F.avg("orders"), 2).alias("avg_orders"),
        )
        .orderBy(F.col("avg_spend").desc())
    )

    top_customers = (
        scored.orderBy(F.col("clv_score").desc()).limit(25)
    )

    return {
        "business_question": "Which customers have the highest estimated lifetime value when combining purchases and engagement?",
        "data_sources": {
            "user_profiles": "MongoDB users",
            "transaction_history": "MongoDB transactions",
            "engagement_metrics": "HBase user_sessions aggregates",
            "scoring_engine": "Apache Spark",
        },
        "thresholds": {"p40": float(p40), "p70": float(p70), "p90": float(p90)},
        "tier_summary": [r.asDict() for r in tier_summary.collect()],
        "top_customers": [r.asDict() for r in top_customers.collect()],
    }


def main():
    config.ensure_dirs()
    spark = get_spark("AUCA_IntegratedAnalytics")
    spark.sparkContext.setLogLevel("WARN")

    users, products, categories, transactions, sessions = load_core_frames(spark)
    users = clean_users(users)
    transactions = clean_transactions(transactions)
    sessions = clean_sessions(sessions)

    funnel = funnel_analysis(sessions, transactions)
    clv = clv_estimation(users, sessions, transactions)

    out = {
        "funnel_conversion_analysis": funnel,
        "customer_lifetime_value": clv,
    }
    path = config.RESULTS_DIR / "integrated_analytics.json"
    with open(path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2, default=str)

    print("=== Funnel counts ===")
    print(funnel["funnel_counts"])
    print("=== CLV tiers ===")
    for row in clv["tier_summary"]:
        print(row)
    print(f"Saved {path}")
    spark.stop()


if __name__ == "__main__":
    main()
