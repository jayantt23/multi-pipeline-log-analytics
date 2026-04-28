"""End-to-end aggregation test on 15 hand-crafted records.

Skipped if a real MongoDB is not reachable on $MONGO_URI (default localhost:27017).
This test does NOT touch Postgres — it verifies the Mongo aggregation outputs
(`q*_final_<run_id>` collections) directly.

Hand-crafted dataset (see top of file): 13 valid + 2 malformed.
"""
from __future__ import annotations

import datetime as dt
import os
import uuid

import pytest

pymongo = pytest.importorskip("pymongo")
from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from src.pipelines.mongodb.aggregations import coll
from src.pipelines.mongodb.etl_pipeline import MongoPipeline

MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")


def _mongo_available() -> bool:
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=500)
        client.admin.command("ping")
        client.close()
        return True
    except (ServerSelectionTimeoutError, Exception):
        return False


pytestmark = pytest.mark.skipif(
    not _mongo_available(),
    reason="MongoDB not reachable at " + MONGO_URI,
)


# 15 lines: 13 valid + 2 malformed.
RAW_LINES = [
    'host1 - - [01/Jul/1995:00:00:01 -0400] "GET /a HTTP/1.0" 200 100',
    'host2 - - [01/Jul/1995:00:00:02 -0400] "GET /a HTTP/1.0" 200 100',
    'host1 - - [01/Jul/1995:01:00:00 -0400] "GET /b HTTP/1.0" 404 50',
    'host3 - - [01/Jul/1995:01:00:00 -0400] "GET /b HTTP/1.0" 200 75',
    'host3 - - [01/Jul/1995:02:00:00 -0400] "GET /a HTTP/1.0" 200 100',
    'host4 - - [02/Jul/1995:00:00:00 -0400] "GET /c HTTP/1.0" 500 200',
    'host5 - - [02/Jul/1995:00:00:00 -0400] "GET /c HTTP/1.0" 500 200',
    'host5 - - [02/Jul/1995:00:00:00 -0400] "GET /c HTTP/1.0" 200 200',
    'host5 - - [02/Jul/1995:01:00:00 -0400] "GET /a HTTP/1.0" 200 100',
    'host5 - - [02/Jul/1995:01:00:00 -0400] "GET /b HTTP/1.0" 200 50',
    'host1 - - [02/Jul/1995:01:00:00 -0400] "GET /a HTTP/1.0" 200 100',
    'host6 - - [02/Jul/1995:02:00:00 -0400] "GET /d HTTP/1.0" 200 -',
    'this is a malformed line',
    'host7 - - [02/Jul/1995:02:00:00 -0400] "GET /e HTTP/1.0" - 50',
    'host8 - - [02/Jul/1995:03:00:00 -0400] "GET /a HTTP/1.0" 404 25',
]


@pytest.fixture(scope="module")
def run_artifacts():
    """Ingest, aggregate, final_merge once; yield (pipeline, run_id, db). Cleanup after."""
    db_name = f"etl_test_{uuid.uuid4().hex[:8]}"
    pipeline = MongoPipeline(MONGO_URI, db_name)
    run_id = uuid.uuid4().hex

    raw = pipeline.db[coll("raw_logs", run_id)]
    raw.insert_many(
        [{"_id": i + 1, "raw": line} for i, line in enumerate(RAW_LINES)],
        ordered=True,
    )

    # batch_size = 10 so we get 2 batches (ords 1..10, 11..15).
    pipeline.aggregate(run_id, batch_size=10)
    pipeline.final_merge(run_id)

    yield pipeline, run_id

    # Drop the whole test database to clean up.
    pipeline.client.drop_database(db_name)
    pipeline.close()


def _as_date(v):
    return v.date() if hasattr(v, "date") else v


def test_run_stats(run_artifacts):
    pipeline, run_id = run_artifacts
    stats = pipeline.collect_run_stats(run_id)
    assert stats.total_records == 13
    assert stats.malformed_count == 2
    assert stats.num_batches == 2
    assert stats.avg_batch_size == pytest.approx(13 / 2)


def test_q1_daily_traffic(run_artifacts):
    pipeline, run_id = run_artifacts
    docs = list(pipeline.db[coll("q1_final", run_id)].find({}, {"_id": 0}))
    by_key = {(_as_date(d["log_date"]), d["status_code"]):
              (d["request_count"], d["total_bytes"]) for d in docs}
    d1 = dt.date(1995, 7, 1)
    d2 = dt.date(1995, 7, 2)
    assert by_key[(d1, 200)] == (4, 375)
    assert by_key[(d1, 404)] == (1, 50)
    assert by_key[(d2, 200)] == (5, 450)
    assert by_key[(d2, 404)] == (1, 25)
    assert by_key[(d2, 500)] == (2, 400)
    assert len(docs) == 5


def test_q2_top_resources_ranking_and_distinct(run_artifacts):
    pipeline, run_id = run_artifacts
    docs = sorted(
        pipeline.db[coll("q2_final", run_id)].find({}, {"_id": 0}),
        key=lambda d: d["rank"],
    )
    # 4 unique paths in test data
    assert len(docs) == 4
    # rank 1: /a, 6 reqs, 525 bytes, 5 distinct hosts
    assert docs[0] == {
        "rank": 1, "resource_path": "/a",
        "request_count": 6, "total_bytes": 525, "distinct_host_count": 5,
    }
    # rank 2/3 tie on request_count=3; tie-break by path ASC → /b before /c
    assert docs[1]["rank"] == 2 and docs[1]["resource_path"] == "/b"
    assert docs[1]["request_count"] == 3
    assert docs[1]["total_bytes"] == 175
    assert docs[1]["distinct_host_count"] == 3

    assert docs[2]["rank"] == 3 and docs[2]["resource_path"] == "/c"
    assert docs[2]["request_count"] == 3
    assert docs[2]["total_bytes"] == 600
    assert docs[2]["distinct_host_count"] == 2

    assert docs[3]["rank"] == 4 and docs[3]["resource_path"] == "/d"
    assert docs[3]["request_count"] == 1
    assert docs[3]["total_bytes"] == 0  # bytes='-' → 0
    assert docs[3]["distinct_host_count"] == 1


def test_q3_hourly_errors(run_artifacts):
    pipeline, run_id = run_artifacts
    docs = sorted(
        pipeline.db[coll("q3_final", run_id)].find({}, {"_id": 0}),
        key=lambda d: (_as_date(d["log_date"]), d["log_hour"]),
    )
    d1 = dt.date(1995, 7, 1)
    d2 = dt.date(1995, 7, 2)

    # 3 (date, hour) groups have at least one error
    assert len(docs) == 3

    # 1995-07-01, hour 1: 1 error (404 by host1) of 2 total → rate 0.5, 1 distinct host
    assert _as_date(docs[0]["log_date"]) == d1 and docs[0]["log_hour"] == 1
    assert docs[0]["error_request_count"] == 1
    assert docs[0]["total_request_count"] == 2
    assert float(docs[0]["error_rate"]) == pytest.approx(0.5, abs=1e-6)
    assert docs[0]["distinct_error_hosts"] == 1

    # 1995-07-02, hour 0: 2 errors (host4, host5) of 3 → rate 0.666667, 2 distinct
    assert _as_date(docs[1]["log_date"]) == d2 and docs[1]["log_hour"] == 0
    assert docs[1]["error_request_count"] == 2
    assert docs[1]["total_request_count"] == 3
    assert float(docs[1]["error_rate"]) == pytest.approx(2 / 3, abs=1e-6)
    assert docs[1]["distinct_error_hosts"] == 2

    # 1995-07-02, hour 3: 1 error of 1 → rate 1.0, 1 distinct
    assert _as_date(docs[2]["log_date"]) == d2 and docs[2]["log_hour"] == 3
    assert docs[2]["error_request_count"] == 1
    assert docs[2]["total_request_count"] == 1
    assert float(docs[2]["error_rate"]) == pytest.approx(1.0, abs=1e-6)
    assert docs[2]["distinct_error_hosts"] == 1
