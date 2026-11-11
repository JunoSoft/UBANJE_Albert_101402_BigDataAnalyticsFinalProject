"""
Create HBase tables for the project.

Usage:
  python 03_hbase/create_tables.py
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
from hbase_utils import wait_for_thrift


def ensure_table(conn, name: str, families: dict):
    existing = {t.decode() if isinstance(t, bytes) else t for t in conn.tables()}
    if name in existing:
        print(f"Table already exists: {name}")
        return
    conn.create_table(name, families)
    print(f"Created table: {name} families={list(families)}")


def main():
    conn = wait_for_thrift(config.HBASE_HOST, config.HBASE_PORT)
    ensure_table(
        conn,
        "user_sessions",
        {
            "info": dict(max_versions=1),
            "geo": dict(max_versions=1),
            "events": dict(max_versions=1),
        },
    )
    ensure_table(
        conn,
        "product_metrics",
        {
            "stats": dict(max_versions=1),
        },
    )
    print("Current tables:", conn.tables())
    conn.close()


if __name__ == "__main__":
    main()
