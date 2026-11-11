"""
Orchestrate the full analytics pipeline in the correct order.

Usage (from project root):
  python scripts/run_pipeline.py --all
  python scripts/run_pipeline.py --skip-docker-checks --steps generate,spark,viz
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def run_step(title: str, command: list[str]):
    print("\n" + "=" * 72)
    print(title)
    print("=" * 72)
    print(">", " ".join(command))
    started = time.time()
    result = subprocess.run(command, cwd=ROOT)
    elapsed = time.time() - started
    if result.returncode != 0:
        raise SystemExit(f"Step failed ({title}) after {elapsed:.1f}s with code {result.returncode}")
    print(f"OK in {elapsed:.1f}s")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--all", action="store_true", help="Run every step that does not need Docker waits beyond Mongo/HBase")
    parser.add_argument(
        "--steps",
        default="",
        help="Comma list: generate,mongo_load,mongo_agg,hbase_tables,hbase_load,hbase_query,spark_clean,spark_rec,spark_sql,spark_cohort,integrate,viz",
    )
    parser.add_argument("--mode", choices=["DEMO", "FULL"], default=None)
    args = parser.parse_args()

    selected = set(s.strip() for s in args.steps.split(",") if s.strip())
    if args.all:
        selected = {
            "generate",
            "mongo_load",
            "mongo_agg",
            "hbase_tables",
            "hbase_load",
            "hbase_query",
            "spark_clean",
            "spark_rec",
            "spark_sql",
            "integrate",
            "viz",
        }

    if not selected:
        parser.error("Provide --all or --steps")

    py = sys.executable

    if "generate" in selected:
        cmd = [py, "01_data_generation/generate_dataset.py"]
        if args.mode:
            cmd += ["--mode", args.mode]
        run_step("1) Generate dataset", cmd)

    if "mongo_load" in selected:
        run_step("2) Load MongoDB", [py, "02_mongodb/load_mongodb.py"])
    if "mongo_agg" in selected:
        run_step("3) MongoDB aggregations", [py, "02_mongodb/aggregations.py"])

    if "hbase_tables" in selected:
        run_step("4) Create HBase tables", [py, "03_hbase/create_tables.py"])
    if "hbase_load" in selected:
        run_step("5) Load HBase", [py, "03_hbase/load_hbase.py"])
    if "hbase_query" in selected:
        run_step("6) Query HBase", [py, "03_hbase/queries.py"])

    if "spark_clean" in selected:
        run_step("7) Spark cleaning", [py, "04_spark/batch_cleaning.py"])
    if "spark_rec" in selected:
        run_step("8) Spark recommendations", [py, "04_spark/recommendations.py"])
    if "spark_sql" in selected:
        run_step("9) Spark SQL analytics", [py, "04_spark/spark_sql_analytics.py"])
    if "spark_cohort" in selected:
        run_step("10) Spark cohort analysis", [py, "04_spark/cohort_analysis.py"])

    if "integrate" in selected:
        run_step("11) Integrated analytics", [py, "05_integration/integrated_analytics.py"])
    if "viz" in selected:
        run_step("12) Visualizations", [py, "06_visualization/create_visualizations.py"])

    print("\nPipeline finished. See outputs/results and outputs/figures.")


if __name__ == "__main__":
    main()
