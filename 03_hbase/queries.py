"""
Sample HBase queries (Part 1).

1) Retrieve recent sessions for a specific user (prefix scan)
2) Retrieve product daily metrics for a product (prefix scan)

Usage:
  python 03_hbase/queries.py
  python 03_hbase/queries.py --user user_000001 --product prod_00010
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[0]
for path in (ROOT, HERE):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))

import config
from hbase_utils import wait_for_thrift


def decode_map(data: dict) -> dict:
    out = {}
    for k, v in data.items():
        key = k.decode() if isinstance(k, bytes) else k
        val = v.decode() if isinstance(v, bytes) else v
        out[key] = val
    return out


def get_user_sessions(conn, user_id: str, limit: int = 10):
    table = conn.table("user_sessions")
    prefix = f"{user_id}#".encode()
    rows = []
    for key, data in table.scan(row_prefix=prefix, limit=limit):
        decoded = decode_map(data)
        events = {
            k: json.loads(v)
            for k, v in decoded.items()
            if k.startswith("events:pv_")
        }
        rows.append(
            {
                "rowkey": key.decode(),
                "session_id": decoded.get("info:session_id"),
                "start_time": decoded.get("info:start_time"),
                "conversion_status": decoded.get("info:conversion_status"),
                "device_type": decoded.get("info:device_type"),
                "referrer": decoded.get("info:referrer"),
                "page_view_count": len(events),
            }
        )
    return rows


def get_product_metrics(conn, product_id: str, limit: int = 30):
    table = conn.table("product_metrics")
    prefix = f"{product_id}#".encode()
    rows = []
    for key, data in table.scan(row_prefix=prefix, limit=limit):
        decoded = decode_map(data)
        rows.append(
            {
                "rowkey": key.decode(),
                "day": key.decode().split("#", 1)[1],
                "views": int(decoded.get("stats:views", 0)),
                "cart_adds": int(decoded.get("stats:cart_adds", 0)),
                "purchases": int(decoded.get("stats:purchases", 0)),
            }
        )
    rows.sort(key=lambda r: r["day"])
    return rows


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--user", default=None)
    parser.add_argument("--product", default=None)
    args = parser.parse_args()

    # Pick defaults from generated data if present
    user_id = args.user
    product_id = args.product
    users_path = config.DATA_DIR / "users.json"
    products_path = config.DATA_DIR / "products.json"
    if user_id is None and users_path.exists():
        with open(users_path, encoding="utf-8") as f:
            user_id = json.load(f)[0]["user_id"]
    if product_id is None and products_path.exists():
        with open(products_path, encoding="utf-8") as f:
            product_id = json.load(f)[0]["product_id"]

    conn = wait_for_thrift(config.HBASE_HOST, config.HBASE_PORT)

    print(f"\n=== Sessions for {user_id} ===")
    sessions = get_user_sessions(conn, user_id, limit=8)
    for row in sessions:
        print(row)
    config.ensure_dirs()
    with open(config.RESULTS_DIR / "hbase_user_sessions.json", "w", encoding="utf-8") as f:
        json.dump(sessions, f, indent=2)

    print(f"\n=== Product metrics for {product_id} ===")
    metrics = get_product_metrics(conn, product_id, limit=40)
    for row in metrics[:10]:
        print(row)
    with open(config.RESULTS_DIR / "hbase_product_metrics.json", "w", encoding="utf-8") as f:
        json.dump(metrics, f, indent=2)

    # Equivalent HBase shell examples for the report
    shell_examples = f"""
# HBase shell equivalents

# 1) Scan recent sessions for a user (prefix)
scan 'user_sessions', {{ROWPREFIXFILTER => '{user_id}#', LIMIT => 10}}

# 2) Scan product daily metrics
scan 'product_metrics', {{ROWPREFIXFILTER => '{product_id}#', LIMIT => 30}}

# 3) Get one row
get 'user_sessions', '<rowkey_from_scan>'
"""
    print(shell_examples)
    with open(config.RESULTS_DIR / "hbase_shell_examples.txt", "w", encoding="utf-8") as f:
        f.write(shell_examples)

    conn.close()
    print("HBase queries finished.")


if __name__ == "__main__":
    main()
