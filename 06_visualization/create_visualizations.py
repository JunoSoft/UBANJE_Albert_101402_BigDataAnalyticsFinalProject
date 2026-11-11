"""
Part 4 — Create 4 static visualizations for the technical report / presentation.

Usage:
  python 06_visualization/create_visualizations.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0]
for path in (ROOT, HERE):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import config

sns.set_theme(style="whitegrid", context="talk")


def load_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def fig1_revenue_by_category():
    path = config.RESULTS_DIR / "mongo_revenue_by_category.json"
    alt = config.RESULTS_DIR / "spark_sql_results.json"
    if path.exists():
        df = pd.DataFrame(load_json(path))
    elif alt.exists():
        df = pd.DataFrame(load_json(alt)["revenue_by_category"])
        df = df.rename(columns={"category_name": "category_name"})
    else:
        raise FileNotFoundError("Run Mongo aggregations or Spark SQL first.")

    if "category_name" not in df.columns:
        df["category_name"] = df.get("category_id", "unknown")
    df = df.sort_values("revenue", ascending=False).head(10)

    plt.figure(figsize=(12, 6))
    ax = sns.barplot(data=df, x="revenue", y="category_name", color="#1f4e79")
    ax.set_title("Top Categories by Revenue")
    ax.set_xlabel("Revenue")
    ax.set_ylabel("Category")
    plt.tight_layout()
    out = config.FIGURES_DIR / "01_revenue_by_category.png"
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"Wrote {out}")


def fig2_user_segments():
    path = config.RESULTS_DIR / "mongo_user_segmentation.json"
    if not path.exists():
        raise FileNotFoundError("Run Mongo aggregations first.")
    df = pd.DataFrame(load_json(path))

    plt.figure(figsize=(9, 6))
    ax = sns.barplot(data=df, x="segment", y="users", color="#2e7d32")
    ax.set_title("Customer Segments by Purchase Frequency")
    ax.set_xlabel("Segment")
    ax.set_ylabel("Users")
    for i, row in df.iterrows():
        ax.text(i, row["users"], f"avg spend {row['avg_lifetime_spend']}", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    out = config.FIGURES_DIR / "02_user_segments.png"
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"Wrote {out}")


def fig3_funnel():
    path = config.RESULTS_DIR / "integrated_analytics.json"
    if not path.exists():
        raise FileNotFoundError("Run integrated analytics first.")
    funnel = load_json(path)["funnel_conversion_analysis"]["funnel_counts"]
    stages = ["browse", "product_view", "cart", "checkout", "purchase"]
    labels = ["Browse", "Product View", "Cart", "Checkout", "Purchase"]
    values = [funnel.get(s, 0) for s in stages]
    df = pd.DataFrame({"stage": labels, "sessions": values})

    plt.figure(figsize=(10, 6))
    ax = sns.barplot(data=df, x="stage", y="sessions", color="#b36b00")
    ax.set_title("Purchase Funnel Conversion")
    ax.set_xlabel("Stage")
    ax.set_ylabel("Sessions reaching stage")
    for i, v in enumerate(values):
        ax.text(i, v, f"{v:,}", ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    out = config.FIGURES_DIR / "03_conversion_funnel.png"
    plt.savefig(out, dpi=160)
    plt.close()
    print(f"Wrote {out}")


def fig4_clv_tiers_or_also_bought():
    integ = config.RESULTS_DIR / "integrated_analytics.json"
    also = config.RESULTS_DIR / "spark_also_bought.json"

    if integ.exists():
        tiers = pd.DataFrame(load_json(integ)["customer_lifetime_value"]["tier_summary"])
        plt.figure(figsize=(9, 6))
        ax = sns.barplot(data=tiers, x="value_tier", y="avg_spend", color="#6a1b9a")
        ax.set_title("Average Spend by CLV Tier")
        ax.set_xlabel("Value Tier")
        ax.set_ylabel("Average Lifetime Spend")
        plt.tight_layout()
        out = config.FIGURES_DIR / "04_clv_tiers.png"
        plt.savefig(out, dpi=160)
        plt.close()
        print(f"Wrote {out}")
        return

    if also.exists():
        df = pd.DataFrame(load_json(also)).head(12)
        df["pair"] = df["product_a"] + " + " + df["product_b"]
        plt.figure(figsize=(12, 6))
        ax = sns.barplot(data=df, x="co_purchase_count", y="pair", color="#00695c")
        ax.set_title("Products Frequently Bought Together")
        plt.tight_layout()
        out = config.FIGURES_DIR / "04_also_bought.png"
        plt.savefig(out, dpi=160)
        plt.close()
        print(f"Wrote {out}")
        return

    raise FileNotFoundError("Run integrated analytics or recommendations first.")


def main():
    config.ensure_dirs()
    fig1_revenue_by_category()
    fig2_user_segments()
    fig3_funnel()
    fig4_clv_tiers_or_also_bought()
    print("All visualizations created in outputs/figures/")


if __name__ == "__main__":
    main()
