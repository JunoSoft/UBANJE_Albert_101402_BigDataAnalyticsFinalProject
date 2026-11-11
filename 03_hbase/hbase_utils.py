"""Shared HBase helpers (HappyBase / Thrift)."""
from __future__ import annotations

import time
from datetime import datetime

import happybase


def connect(host: str, port: int, timeout_ms: int = 20000):
    return happybase.Connection(host=host, port=port, timeout=timeout_ms)


def reverse_ts(iso_ts: str) -> str:
    """Lexicographic newest-first helper for row keys."""
    try:
        dt = datetime.fromisoformat(iso_ts)
    except ValueError:
        dt = datetime.strptime(iso_ts[:19], "%Y-%m-%dT%H:%M:%S")
    unix = int(dt.timestamp())
    return f"{10_000_000_000 - unix:010d}"


def user_session_rowkey(user_id: str, start_time: str, session_id: str) -> str:
    return f"{user_id}#{reverse_ts(start_time)}#{session_id}"


def product_metric_rowkey(product_id: str, day: str) -> str:
    return f"{product_id}#{day}"


def wait_for_thrift(host: str, port: int, attempts: int = 30, delay_sec: float = 4.0):
    last_err = None
    for i in range(1, attempts + 1):
        try:
            conn = connect(host, port)
            conn.tables()
            print(f"HBase Thrift ready at {host}:{port} (attempt {i})")
            return conn
        except Exception as exc:  # noqa: BLE001
            last_err = exc
            print(f"Waiting for HBase Thrift ({i}/{attempts}): {exc}")
            time.sleep(delay_sec)
    raise RuntimeError(f"HBase Thrift not reachable: {last_err}")
