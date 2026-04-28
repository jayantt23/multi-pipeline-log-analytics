"""Reporter — pretty-print a run's metadata + Q1/Q2/Q3 tables from Postgres."""
from __future__ import annotations

from typing import Any

import psycopg2
from tabulate import tabulate


def _fetch_run(cur, run_id: str) -> dict[str, Any] | None:
    cur.execute(
        """
        SELECT run_id, pipeline, status, started_at, finished_at, runtime_ms,
               batch_size, num_batches, avg_batch_size,
               total_records, malformed_count
        FROM runs WHERE run_id = %s
        """,
        (run_id,),
    )
    row = cur.fetchone()
    if not row:
        return None
    cols = [d.name for d in cur.description]
    return dict(zip(cols, row))


def _fetch_q1(cur, run_id: str) -> tuple[list[str], list[tuple]]:
    cur.execute(
        """
        SELECT log_date, status_code, request_count, total_bytes
        FROM q1_daily_traffic WHERE run_id = %s
        ORDER BY log_date, status_code
        """,
        (run_id,),
    )
    headers = ["log_date", "status_code", "request_count", "total_bytes"]
    return headers, list(cur.fetchall())


def _fetch_q2(cur, run_id: str) -> tuple[list[str], list[tuple]]:
    cur.execute(
        """
        SELECT rank, resource_path, request_count, total_bytes, distinct_host_count
        FROM q2_top_resources WHERE run_id = %s
        ORDER BY rank
        """,
        (run_id,),
    )
    headers = ["rank", "resource_path", "request_count", "total_bytes",
               "distinct_host_count"]
    return headers, list(cur.fetchall())


def _fetch_q3(cur, run_id: str) -> tuple[list[str], list[tuple]]:
    cur.execute(
        """
        SELECT log_date, log_hour, error_request_count, total_request_count,
               error_rate, distinct_error_hosts
        FROM q3_hourly_errors WHERE run_id = %s
        ORDER BY log_date, log_hour
        """,
        (run_id,),
    )
    headers = ["log_date", "log_hour", "error_request_count",
               "total_request_count", "error_rate", "distinct_error_hosts"]
    return headers, list(cur.fetchall())


def report(run_id: str, pg_dsn: str) -> str:
    """Return the formatted report string. Caller decides whether to print or pipe."""
    pg_conn = psycopg2.connect(pg_dsn)
    try:
        with pg_conn, pg_conn.cursor() as cur:
            meta = _fetch_run(cur, run_id)
            if meta is None:
                return f"No run found with run_id={run_id}"
            q1_h, q1_r = _fetch_q1(cur, run_id)
            q2_h, q2_r = _fetch_q2(cur, run_id)
            q3_h, q3_r = _fetch_q3(cur, run_id)
    finally:
        pg_conn.close()

    parts: list[str] = []
    parts.append("=" * 78)
    parts.append("Execution metadata")
    parts.append("=" * 78)
    meta_rows = [
        ("run_id", meta["run_id"]),
        ("pipeline", meta["pipeline"]),
        ("status", meta["status"]),
        ("started_at", meta["started_at"]),
        ("finished_at", meta["finished_at"]),
        ("runtime_ms", meta["runtime_ms"]),
        ("batch_size", meta["batch_size"]),
        ("num_batches", meta["num_batches"]),
        ("avg_batch_size", meta["avg_batch_size"]),
        ("total_records", meta["total_records"]),
        ("malformed_count", meta["malformed_count"]),
    ]
    parts.append(tabulate(meta_rows, headers=["field", "value"], tablefmt="github"))

    parts.append("\n" + "=" * 78)
    parts.append(f"Q1 — Daily Traffic Summary  ({len(q1_r)} rows)")
    parts.append("=" * 78)
    parts.append(tabulate(q1_r, headers=q1_h, tablefmt="github"))

    parts.append("\n" + "=" * 78)
    parts.append(f"Q2 — Top {len(q2_r)} Requested Resources")
    parts.append("=" * 78)
    parts.append(tabulate(q2_r, headers=q2_h, tablefmt="github"))

    parts.append("\n" + "=" * 78)
    parts.append(f"Q3 — Hourly Error Analysis  ({len(q3_r)} rows)")
    parts.append("=" * 78)
    parts.append(tabulate(q3_r, headers=q3_h, tablefmt="github"))

    return "\n".join(parts)
