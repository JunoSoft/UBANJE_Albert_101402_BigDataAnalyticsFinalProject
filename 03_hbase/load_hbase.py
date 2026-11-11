"""
Load a representative subset of session / product-metric data into HBase.

Usage:
  python 03_hbase/load_hbase.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0]
for path in (ROOT, HERE):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import config
from hbase_utils import (
    product_metric_rowkey,
    user_session_rowkey,
    wait_for_thrift,
)


def load_json(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def iter_sessions(limit: int | None):
    count = 0
    for path in sorted(config.DATA_DIR.glob("sessions_*.json")):
        for session in load_json(path):
            yield session
            count += 1
            if limit is not None and count >= limit:
                return


def day_of(iso_ts: str) -> str:
    return iso_ts[:10]


def main():
    config.ensure_dirs()
    if not list(config.DATA_DIR.glob("sessions_*.json")):
        raise FileNotFoundError("No session files. Generate the dataset first.")

    conn = wait_for_thrift(config.HBASE_HOST, config.HBASE_PORT)
    tables = {t.decode() if isinstance(t, bytes) else t for t in conn.tables()}
    if "user_sessions" not in tables or "product_metrics" not in tables:
        raise RuntimeError("Missing tables. Run: python 03_hbase/create_tables.py")

    user_tbl = conn.table("user_sessions")
    metrics_tbl = conn.table("product_metrics")

    # Aggregate product daily metrics in memory, then write
    product_daily = defaultdict(lambda: {"views": 0, "cart_adds": 0, "purchases": 0})

    # purchases from transactions (subset aligned with loaded window is fine)
    txn_path = config.DATA_DIR / "transactions.json"
    if txn_path.exists():
        for txn in load_json(txn_path):
            day = day_of(txn["timestamp"])
            for item in txn.get("items", []):
                key = (item["product_id"], day)
                product_daily[key]["purchases"] += int(item.get("quantity") or 0)

    loaded = 0
    batch = []
    limit = config.HBASE_LOAD_LIMITS["sessions"]

    print(f"Loading up to {limit} sessions into HBase...")
    for session in iter_sessions(limit):
        rowkey = user_session_rowkey(
            session["user_id"], session["start_time"], session["session_id"]
        )
        data = {
            b"info:session_id": session["session_id"].encode(),
            b"info:start_time": session["start_time"].encode(),
            b"info:end_time": session["end_time"].encode(),
            b"info:duration_seconds": str(session["duration_seconds"]).encode(),
            b"info:conversion_status": session["conversion_status"].encode(),
            b"info:referrer": session.get("referrer", "").encode(),
            b"info:device_type": session.get("device_profile", {}).get("type", "").encode(),
            b"info:os": session.get("device_profile", {}).get("os", "").encode(),
            b"info:browser": session.get("device_profile", {}).get("browser", "").encode(),
            b"geo:city": session.get("geo_data", {}).get("city", "").encode(),
            b"geo:state": session.get("geo_data", {}).get("state", "").encode(),
            b"geo:country": session.get("geo_data", {}).get("country", "").encode(),
            b"geo:ip": session.get("geo_data", {}).get("ip_address", "").encode(),
        }

        for idx, pv in enumerate(session.get("page_views", [])):
            data[f"events:pv_{idx:03d}".encode()] = json.dumps(pv).encode()
            if pv.get("page_type") == "product_detail" and pv.get("product_id"):
                key = (pv["product_id"], day_of(pv["timestamp"]))
                product_daily[key]["views"] += 1

        for prod_id, details in (session.get("cart_contents") or {}).items():
            key = (prod_id, day_of(session["start_time"]))
            product_daily[key]["cart_adds"] += int(details.get("quantity") or 0)

        batch.append((rowkey.encode(), data))
        loaded += 1
        if len(batch) >= 500:
            with user_tbl.batch(batch_size=500) as b:
                for rk, cols in batch:
                    b.put(rk, cols)
            batch.clear()
            if loaded % 2000 == 0:
                print(f"  sessions loaded: {loaded:,}")

    if batch:
        with user_tbl.batch(batch_size=500) as b:
            for rk, cols in batch:
                b.put(rk, cols)

    print(f"Writing {len(product_daily):,} product-day metric rows...")
    with metrics_tbl.batch(batch_size=1000) as b:
        for (product_id, day), stats in product_daily.items():
            rk = product_metric_rowkey(product_id, day).encode()
            b.put(
                rk,
                {
                    b"stats:views": str(stats["views"]).encode(),
                    b"stats:cart_adds": str(stats["cart_adds"]).encode(),
                    b"stats:purchases": str(stats["purchases"]).encode(),
                },
            )

    print(f"HBase load complete. Sessions written: {loaded:,}")
    conn.close()


if __name__ == "__main__":
    main()
