"""Pure-data builders for the MongoDB aggregation pipelines.

Every function here returns a list of pipeline stages (no I/O). The Mongo backend
in `etl_pipeline.py` stitches these into actual `db.<coll>.aggregate(...)` calls.

Per-run collection naming convention:
    raw_logs_<run_id>           — ingested raw text + monotonic _id
    parsed_<run_id>             — parsed valid records, tagged with batch_id
    q1_partial_<run_id>         — per-batch (batch_id, log_date, status_code) sums
    q2_partial_<run_id>         — per-batch (batch_id, resource_path) sums
    q2_pairs_<run_id>           — distinct (resource_path, host)
    q3_partial_<run_id>         — per-batch (batch_id, log_date, log_hour) error+total
    q3_pairs_<run_id>           — distinct (log_date, log_hour, host) for error rows
    q1_final_<run_id> / q2_final_<run_id> / q3_final_<run_id>
"""
from __future__ import annotations

# Master regex from PARSING_SPEC.md §1. Identical across all engines.
MASTER_REGEX = r'^(\S+) \S+ \S+ \[([^\]]+)\] "([^"]*)" (\d{3}|-) (\d+|-)$'


def coll(name: str, run_id: str) -> str:
    return f"{name}_{run_id}"


# ---------------------------------------------------------------------------
# Stage 1: parse + batch-tag — raw_logs → parsed
# ---------------------------------------------------------------------------

_MONTH_BRANCHES = [
    {"case": {"$eq": ["$month_str", m]}, "then": i}
    for i, m in enumerate(
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
         "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], start=1)
]


def parse_pipeline(run_id: str, batch_size: int | None) -> list[dict]:
    """raw_logs → parsed. Adds batch_id, runs $regexFind, derives log_date/log_hour."""
    if batch_size is None:
        batch_id_expr = {"$ifNull": ["$file_batch", 1]}
    else:
        batch_id_expr = {"$add": [
            {"$floor": {"$divide": [{"$subtract": ["$_id", 1]}, batch_size]}},
            1,
        ]}
    return [
        {"$addFields": {
            "batch_id": batch_id_expr,
            "m": {"$regexFind": {"input": "$raw", "regex": MASTER_REGEX}},
        }},
        # PARSING_SPEC §6: malformed iff regex fails OR status == "-"
        {"$match": {
            "m": {"$ne": None},
            "$expr": {"$ne": [{"$arrayElemAt": ["$m.captures", 3]}, "-"]},
        }},
        {"$project": {
            "_id": 1,
            "batch_id": 1,
            "host": {"$arrayElemAt": ["$m.captures", 0]},
            "timestamp_raw": {"$arrayElemAt": ["$m.captures", 1]},
            "request_raw": {"$arrayElemAt": ["$m.captures", 2]},
            "status_code": {"$toInt": {"$arrayElemAt": ["$m.captures", 3]}},
            "bytes_transferred": {"$cond": [
                {"$eq": [{"$arrayElemAt": ["$m.captures", 4]}, "-"]},
                0,
                {"$convert": {
                    "input": {"$arrayElemAt": ["$m.captures", 4]},
                    "to": "long", "onError": 0, "onNull": 0,
                }},
            ]},
        }},
        # Decompose timestamp ("01/Jul/1995:00:00:01 -0400") in its own offset (PARSING_SPEC §2).
        {"$addFields": {
            "day": {"$toInt": {"$substrBytes": ["$timestamp_raw", 0, 2]}},
            "month_str": {"$substrBytes": ["$timestamp_raw", 3, 3]},
            "year": {"$toInt": {"$substrBytes": ["$timestamp_raw", 7, 4]}},
            "log_hour": {"$toInt": {"$substrBytes": ["$timestamp_raw", 12, 2]}},
            # $filter strips empty tokens so whitespace-only request lines
            # (e.g. "   ") collapse to [] → all method/path/protocol fields NULL,
            # per PARSING_SPEC §3.
            "req_tokens": {"$filter": {
                "input": {"$split": ["$request_raw", " "]},
                "cond": {"$ne": ["$$this", ""]},
            }},
        }},
        {"$addFields": {
            "month_num": {"$switch": {"branches": _MONTH_BRANCHES, "default": 0}},
            "http_method": {"$cond": [
                {"$gte": [{"$size": "$req_tokens"}, 1]},
                {"$arrayElemAt": ["$req_tokens", 0]}, None]},
            "resource_path": {"$cond": [
                {"$gte": [{"$size": "$req_tokens"}, 2]},
                {"$arrayElemAt": ["$req_tokens", 1]}, None]},
            "protocol_version": {"$cond": [
                {"$gte": [{"$size": "$req_tokens"}, 3]},
                {"$arrayElemAt": ["$req_tokens", 2]}, None]},
        }},
        {"$addFields": {
            "log_date": {"$dateFromParts": {
                "year": "$year", "month": "$month_num", "day": "$day"
            }},
        }},
        {"$project": {
            "timestamp_raw": 0, "request_raw": 0,
            "day": 0, "month_str": 0, "month_num": 0, "year": 0, "req_tokens": 0,
        }},
        {"$out": coll("parsed", run_id)},
    ]


def malformed_count_pipeline(run_id: str) -> list[dict]:
    """Count records that fail the master regex OR have status=='-'."""
    return [
        {"$addFields": {"m": {"$regexFind": {"input": "$raw", "regex": MASTER_REGEX}}}},
        {"$match": {"$or": [
            {"m": None},
            {"$expr": {"$eq": [{"$arrayElemAt": ["$m.captures", 3]}, "-"]}},
        ]}},
        {"$count": "malformed_count"},
    ]


# ---------------------------------------------------------------------------
# Stage 2: per-batch partials (read parsed_<run_id>)
# ---------------------------------------------------------------------------

def q1_partial_pipeline(run_id: str) -> list[dict]:
    """(batch_id, log_date, status_code) → request_count, total_bytes."""
    return [
        {"$group": {
            "_id": {
                "batch_id": "$batch_id",
                "log_date": "$log_date",
                "status_code": "$status_code",
            },
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes_transferred"},
        }},
        {"$out": coll("q1_partial", run_id)},
    ]


def q2_partial_pipeline(run_id: str) -> list[dict]:
    """(batch_id, resource_path) → request_count, total_bytes. Skips NULL paths."""
    return [
        {"$match": {"resource_path": {"$ne": None}}},
        {"$group": {
            "_id": {"batch_id": "$batch_id", "resource_path": "$resource_path"},
            "request_count": {"$sum": 1},
            "total_bytes": {"$sum": "$bytes_transferred"},
        }},
        {"$out": coll("q2_partial", run_id)},
    ]


def q2_pairs_pipeline(run_id: str) -> list[dict]:
    """Global distinct (resource_path, host) — one row per unique pair."""
    return [
        {"$match": {"resource_path": {"$ne": None}}},
        {"$group": {
            "_id": {"resource_path": "$resource_path", "host": "$host"},
        }},
        {"$out": coll("q2_pairs", run_id)},
    ]


def q3_partial_pipeline(run_id: str) -> list[dict]:
    """(batch_id, log_date, log_hour) → error_count + total_count.

    error_count counts rows with 400 ≤ status ≤ 599.
    total_count is unconditional (denominator for error_rate).
    """
    return [
        {"$group": {
            "_id": {
                "batch_id": "$batch_id",
                "log_date": "$log_date",
                "log_hour": "$log_hour",
            },
            "error_request_count": {"$sum": {"$cond": [
                {"$and": [
                    {"$gte": ["$status_code", 400]},
                    {"$lte": ["$status_code", 599]},
                ]},
                1, 0,
            ]}},
            "total_request_count": {"$sum": 1},
        }},
        {"$out": coll("q3_partial", run_id)},
    ]


def q3_pairs_pipeline(run_id: str) -> list[dict]:
    """Global distinct (log_date, log_hour, host) for error rows only."""
    return [
        {"$match": {"status_code": {"$gte": 400, "$lte": 599}}},
        {"$group": {
            "_id": {
                "log_date": "$log_date",
                "log_hour": "$log_hour",
                "host": "$host",
            },
        }},
        {"$out": coll("q3_pairs", run_id)},
    ]


# ---------------------------------------------------------------------------
# Stage 3: final merge across batches → q*_final
# ---------------------------------------------------------------------------

def q1_final_pipeline(run_id: str) -> list[dict]:
    return [
        {"$group": {
            "_id": {"log_date": "$_id.log_date", "status_code": "$_id.status_code"},
            "request_count": {"$sum": "$request_count"},
            "total_bytes": {"$sum": "$total_bytes"},
        }},
        {"$project": {
            "_id": 0,
            "log_date": "$_id.log_date",
            "status_code": "$_id.status_code",
            "request_count": 1,
            "total_bytes": 1,
        }},
        {"$out": coll("q1_final", run_id)},
    ]


def q2_final_pipeline(run_id: str) -> list[dict]:
    """Sum across batches, sort+limit top 20, look up distinct host counts, assign rank.

    Rank assignment uses $push + $unwind(includeArrayIndex) instead of
    $setWindowFields($documentNumber): Mongo requires $documentNumber to have a
    single-key sortBy, which can't express our (request_count DESC, resource_path
    ASC) tie-break. Since the prior $sort + $limit(20) already establishes the
    canonical order, $push preserves it and the array index becomes rank-1.
    """
    pairs_coll = coll("q2_pairs", run_id)
    return [
        {"$group": {
            "_id": "$_id.resource_path",
            "request_count": {"$sum": "$request_count"},
            "total_bytes": {"$sum": "$total_bytes"},
        }},
        # PARSING_SPEC §10 tie-break.
        {"$sort": {"request_count": -1, "_id": 1}},
        {"$limit": 20},
        {"$lookup": {
            "from": pairs_coll,
            "localField": "_id",
            "foreignField": "_id.resource_path",
            "as": "_pairs",
        }},
        {"$addFields": {"distinct_host_count": {"$size": "$_pairs"}}},
        # Drop _pairs — for popular resources this array can be huge (one entry
        # per distinct host) and would push us past Mongo's 100MB $push limit.
        {"$project": {"_pairs": 0}},
        {"$group": {"_id": None, "docs": {"$push": "$$ROOT"}}},
        {"$unwind": {"path": "$docs", "includeArrayIndex": "rank0"}},
        {"$replaceRoot": {"newRoot": {"$mergeObjects": [
            "$docs", {"rank": {"$add": ["$rank0", 1]}},
        ]}}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "resource_path": "$_id",
            "request_count": 1,
            "total_bytes": 1,
            "distinct_host_count": 1,
        }},
        {"$out": coll("q2_final", run_id)},
    ]


def q3_final_pipeline(run_id: str) -> list[dict]:
    """Sum across batches per (log_date, log_hour); compute error_rate (6 dp); count distinct error hosts.

    PARSING_SPEC §11: rows with zero errors are omitted (Q3 is keyed off the error set).
    """
    pairs_coll = coll("q3_pairs", run_id)
    return [
        {"$group": {
            "_id": {"log_date": "$_id.log_date", "log_hour": "$_id.log_hour"},
            "error_request_count": {"$sum": "$error_request_count"},
            "total_request_count": {"$sum": "$total_request_count"},
        }},
        {"$match": {"error_request_count": {"$gt": 0}}},
        {"$lookup": {
            "from": pairs_coll,
            "let": {"ld": "$_id.log_date", "lh": "$_id.log_hour"},
            "pipeline": [
                {"$match": {"$expr": {"$and": [
                    {"$eq": ["$_id.log_date", "$$ld"]},
                    {"$eq": ["$_id.log_hour", "$$lh"]},
                ]}}},
                {"$count": "n"},
            ],
            "as": "_dh",
        }},
        {"$addFields": {
            "distinct_error_hosts": {
                "$ifNull": [{"$arrayElemAt": ["$_dh.n", 0]}, 0]
            },
            "error_rate": {"$round": [
                {"$divide": ["$error_request_count", "$total_request_count"]},
                6,
            ]},
        }},
        {"$project": {
            "_id": 0,
            "log_date": "$_id.log_date",
            "log_hour": "$_id.log_hour",
            "error_request_count": 1,
            "total_request_count": 1,
            "error_rate": 1,
            "distinct_error_hosts": 1,
        }},
        {"$out": coll("q3_final", run_id)},
    ]
