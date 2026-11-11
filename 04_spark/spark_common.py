"""Shared Spark helpers for Part 2 / Part 3."""
from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config


def get_spark(app_name: str | None = None) -> SparkSession:
    import os

    # Stabilize PySpark workers on Windows
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    return (
        SparkSession.builder.appName(app_name or config.SPARK_APP_NAME)
        .master(config.SPARK_MASTER)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.driver.memory", "4g")
        .config("spark.python.worker.reuse", "true")
        .getOrCreate()
    )


def read_json(spark: SparkSession, relative_or_glob: str):
    path = str(config.DATA_DIR / relative_or_glob)
    return spark.read.option("multiLine", True).json(path)


def load_core_frames(spark: SparkSession):
    users = read_json(spark, "users.json")
    products = read_json(spark, "products.json")
    categories = read_json(spark, "categories.json")
    transactions = read_json(spark, "transactions.json")
    sessions = read_json(spark, "sessions_*.json")
    return users, products, categories, transactions, sessions


def clean_transactions(transactions):
    return (
        transactions.filter(F.col("transaction_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .withColumn("timestamp", F.to_timestamp("timestamp"))
        .withColumn("total", F.col("total").cast(DoubleType()))
        .withColumn("subtotal", F.col("subtotal").cast(DoubleType()))
        .withColumn("discount", F.col("discount").cast(DoubleType()))
        .withColumn(
            "status",
            F.lower(F.coalesce(F.col("status"), F.lit("unknown"))),
        )
        .dropDuplicates(["transaction_id"])
    )


def clean_users(users):
    return (
        users.filter(F.col("user_id").isNotNull())
        .withColumn("registration_date", F.to_timestamp("registration_date"))
        .withColumn("last_active", F.to_timestamp("last_active"))
        .dropDuplicates(["user_id"])
    )


def clean_products(products):
    return (
        products.filter(F.col("product_id").isNotNull())
        .withColumn("base_price", F.col("base_price").cast(DoubleType()))
        .withColumn("current_stock", F.col("current_stock").cast(IntegerType()))
        .withColumn("creation_date", F.to_timestamp("creation_date"))
        .dropDuplicates(["product_id"])
    )


def clean_sessions(sessions):
    return (
        sessions.filter(F.col("session_id").isNotNull())
        .withColumn("start_time", F.to_timestamp("start_time"))
        .withColumn("end_time", F.to_timestamp("end_time"))
        .withColumn("duration_seconds", F.col("duration_seconds").cast(IntegerType()))
        .withColumn(
            "conversion_status",
            F.lower(F.coalesce(F.col("conversion_status"), F.lit("unknown"))),
        )
        .dropDuplicates(["session_id"])
    )
