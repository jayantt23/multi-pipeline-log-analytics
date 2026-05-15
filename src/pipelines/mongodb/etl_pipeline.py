"""MongoDB backend — Phase 1.

Native ingestion: streams raw .gz / plain-text files line-by-line into
`raw_logs_<run_id>` with explicit `_id = 1..N`. Python NEVER tokenises lines.

Native batching: `batch_id = floor((_id-1)/N) + 1` is computed inside Mongo via
`$addFields` (see `aggregations.parse_pipeline`).

All regex parsing happens via Mongo's `$regexFind`. Python only orchestrates.
"""
from __future__ import annotations

import gzip
import logging
from pathlib import Path

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

from ..base import Pipeline, RunStats
from . import aggregations as A

log = logging.getLogger(__name__)

INSERT_CHUNK = 10_000  # network-level chunking for insert_many; unrelated to batch_id


class MongoPipeline(Pipeline):
    name = "mongodb"

    def __init__(self, mongo_uri: str, mongo_db: str = "etl") -> None:
        self.client = MongoClient(mongo_uri)
        self.db: Database = self.client[mongo_db]

    # ------------------------------------------------------------------ ingest

    def ingest(self, input_paths: list[Path], run_id: str, batch_size: int | None) -> None:
        """Stream raw lines from `input_paths` into raw_logs_<run_id> with monotonic _id."""
        raw: Collection = self.db[A.coll("raw_logs", run_id)]
        # Drop any prior staging for this run_id so re-runs are clean.
        raw.drop()

        files = sorted(input_paths, key=lambda p: p.name)
        log.info("ingesting %d file(s) into %s", len(files), raw.name)

        ordinal = 0
        buf: list[dict] = []
        for file_index, path in enumerate(files, start=1):
            opener = gzip.open if path.suffix == ".gz" else open
            log.info("  reading %s", path)
            with opener(path, "rt", encoding="latin-1", errors="replace") as fh:
                for line in fh:
                    ordinal += 1
                    buf.append({
                        "_id": ordinal,
                        "raw": line.rstrip("\n"),
                        "file_batch": file_index,
                    })
                    if len(buf) >= INSERT_CHUNK:
                        raw.insert_many(buf, ordered=False)
                        buf.clear()
        if buf:
            raw.insert_many(buf, ordered=False)
        log.info("ingested %d records", ordinal)

    # ----------------------------------------------------------------- aggregate

    def aggregate(self, run_id: str, batch_size: int | None) -> None:
        """Run parse + per-batch aggregations. Produces parsed, q*_partial, q*_pairs."""
        raw = self.db[A.coll("raw_logs", run_id)]

        log.info("parse + tag → %s", A.coll("parsed", run_id))
        # `aggregate` returns a cursor; with $out the cursor is exhausted server-side.
        list(raw.aggregate(A.parse_pipeline(run_id, batch_size), allowDiskUse=True))

        # Malformed count: small client-side roundtrip.
        m_cursor = raw.aggregate(A.malformed_count_pipeline(run_id), allowDiskUse=True)
        m_doc = next(m_cursor, None)
        malformed = int(m_doc["malformed_count"]) if m_doc else 0
        # Stash on a small doc for cleanup / reporting.
        self.db[A.coll("malformed_counter", run_id)].replace_one(
            {"_id": "n"}, {"_id": "n", "count": malformed}, upsert=True
        )
        log.info("malformed_count = %d", malformed)

        parsed = self.db[A.coll("parsed", run_id)]

        log.info("q1_partial")
        list(parsed.aggregate(A.q1_partial_pipeline(run_id), allowDiskUse=True))
        log.info("q2_partial")
        list(parsed.aggregate(A.q2_partial_pipeline(run_id), allowDiskUse=True))
        log.info("q2_pairs")
        list(parsed.aggregate(A.q2_pairs_pipeline(run_id), allowDiskUse=True))
        log.info("q3_partial")
        list(parsed.aggregate(A.q3_partial_pipeline(run_id), allowDiskUse=True))
        log.info("q3_pairs")
        list(parsed.aggregate(A.q3_pairs_pipeline(run_id), allowDiskUse=True))

    # --------------------------------------------------------------- final_merge

    def final_merge(self, run_id: str) -> None:
        log.info("final merge → q1_final")
        list(self.db[A.coll("q1_partial", run_id)].aggregate(
            A.q1_final_pipeline(run_id), allowDiskUse=True))
        log.info("final merge → q2_final (top-20 with ranking)")
        list(self.db[A.coll("q2_partial", run_id)].aggregate(
            A.q2_final_pipeline(run_id), allowDiskUse=True))
        log.info("final merge → q3_final")
        list(self.db[A.coll("q3_partial", run_id)].aggregate(
            A.q3_final_pipeline(run_id), allowDiskUse=True))

    # ---------------------------------------------------------- collect_run_stats

    def collect_run_stats(self, run_id: str) -> RunStats:
        parsed = self.db[A.coll("parsed", run_id)]
        total_records = parsed.count_documents({})
        # num_batches = max(batch_id) over parsed (PARSING_SPEC §8).
        max_doc = next(
            parsed.aggregate([{"$group": {"_id": None, "n": {"$max": "$batch_id"}}}]),
            None,
        )
        num_batches = int(max_doc["n"]) if max_doc and max_doc.get("n") is not None else 0
        avg_batch_size = (total_records / num_batches) if num_batches else 0.0

        m_doc = self.db[A.coll("malformed_counter", run_id)].find_one({"_id": "n"})
        malformed = int(m_doc["count"]) if m_doc else 0

        return RunStats(
            total_records=total_records,
            num_batches=num_batches,
            avg_batch_size=avg_batch_size,
            malformed_count=malformed,
        )

    # ------------------------------------------------------------------ cleanup

    _STAGING_PREFIXES = (
        "raw_logs", "parsed",
        "q1_partial", "q2_partial", "q2_pairs",
        "q3_partial", "q3_pairs",
        "malformed_counter",
        # q*_final are dropped too: their rows now live in Postgres.
        "q1_final", "q2_final", "q3_final",
    )

    def cleanup(self, run_id: str, keep_staging: bool = False) -> None:
        if keep_staging:
            log.info("keep-staging: skipping drop for run %s", run_id)
            return
        for prefix in self._STAGING_PREFIXES:
            self.db.drop_collection(A.coll(prefix, run_id))
        log.info("staging dropped for run %s", run_id)

    def close(self) -> None:
        self.client.close()
