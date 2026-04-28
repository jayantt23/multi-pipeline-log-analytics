
from __future__ import annotations

import logging
from typing import Any

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values
from pymongo.database import Database

from .pipelines.mongodb.aggregations import coll

log = logging.getLogger(__name__)

INSERT_CHUNK = 1_000


def _to_date(v: Any):
    """Mongo returns datetime; Postgres DATE wants date (or datetime works too)."""
    if v is None:
        return None
    if hasattr(v, "date"):
        return v.date()
    return v


def load(run_id: str, mongo_db: Database, pg_conn: PgConnection) -> None:
    """Copy q1/q2/q3 from Mongo into Postgres, scoped by `run_id`.

    `runs` is written by the orchestrator (it owns timing); this function only
    handles the three query result tables.
    """
    with pg_conn.cursor() as cur:
        # Q1 ----------------------------------------------------------------
        q1_docs = list(mongo_db[coll("q1_final", run_id)].find({}, {"_id": 0}))
        rows = [
            (run_id, _to_date(d["log_date"]), int(d["status_code"]),
             int(d["request_count"]), int(d["total_bytes"]))
            for d in q1_docs
        ]
        if rows:
            execute_values(
                cur,
                "INSERT INTO q1_daily_traffic "
                "(run_id, log_date, status_code, request_count, total_bytes) "
                "VALUES %s",
                rows, page_size=INSERT_CHUNK,
            )
        log.info("loaded q1_daily_traffic: %d rows", len(rows))

        # Q2 ----------------------------------------------------------------
        q2_docs = list(mongo_db[coll("q2_final", run_id)].find({}, {"_id": 0}))
        rows = [
            (run_id, int(d["rank"]), d["resource_path"],
             int(d["request_count"]), int(d["total_bytes"]),
             int(d["distinct_host_count"]))
            for d in q2_docs
        ]
        if rows:
            execute_values(
                cur,
                "INSERT INTO q2_top_resources "
                "(run_id, rank, resource_path, request_count, total_bytes, distinct_host_count) "
                "VALUES %s",
                rows, page_size=INSERT_CHUNK,
            )
        log.info("loaded q2_top_resources: %d rows", len(rows))

        # Q3 ----------------------------------------------------------------
        q3_docs = list(mongo_db[coll("q3_final", run_id)].find({}, {"_id": 0}))
        rows = [
            (run_id, _to_date(d["log_date"]), int(d["log_hour"]),
             int(d["error_request_count"]), int(d["total_request_count"]),
             float(d["error_rate"]), int(d["distinct_error_hosts"]))
            for d in q3_docs
        ]
        if rows:
            execute_values(
                cur,
                "INSERT INTO q3_hourly_errors "
                "(run_id, log_date, log_hour, error_request_count, total_request_count, "
                "error_rate, distinct_error_hosts) VALUES %s",
                rows, page_size=INSERT_CHUNK,
            )
        log.info("loaded q3_hourly_errors: %d rows", len(rows))
