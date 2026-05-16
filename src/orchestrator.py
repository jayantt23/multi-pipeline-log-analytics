from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import psycopg2

from .loader import load
from .pipelines.base import Pipeline, RunStats
from .pipelines.mongodb.etl_pipeline import MongoPipeline
from .pipelines.hive.etl_pipeline import HivePipeline
from .pipelines.pig.etl_pipeline import PigPipeline
from .pipelines.mr.etl_pipeline import MapReducePipeline

log = logging.getLogger(__name__)


def _resolve_inputs(input_arg: Path) -> list[Path]:
    """Accept a directory (use all NASA_access_log_* files inside) or a single file."""
    if input_arg.is_dir():
        files = sorted(
            p for p in input_arg.iterdir()
            if p.is_file() and p.name.startswith("NASA_access_log_")
        )
        if not files:
            raise FileNotFoundError(
                f"No NASA_access_log_* files found in {input_arg}"
            )
        return files
    if input_arg.is_file():
        return [input_arg]
    raise FileNotFoundError(f"--input does not exist: {input_arg}")


def _make_pipeline(name: str, mongo_uri: str, mongo_db: str) -> Pipeline:
    if name == "mongodb":
        return MongoPipeline(mongo_uri, mongo_db)
    if name == "pig":
        return PigPipeline()
    if name == "hive":
        return HivePipeline()
    if name == "mr":
        return MapReducePipeline()
    raise ValueError(f"unknown pipeline: {name}")


def run(
    *,
    pipeline_name: str,
    input_path: Path,
    batch_size: int | None,
    mongo_uri: str,
    mongo_db: str,
    pg_dsn: str,
    keep_staging: bool = False,
) -> str:
    """Execute one full ETL run end-to-end. Returns the new run_id."""
    run_id = uuid.uuid4().hex
    if batch_size is None:
        log.info("run_id=%s pipeline=%s batch_mode=file", run_id, pipeline_name)
    else:
        log.info("run_id=%s pipeline=%s batch_size=%d", run_id, pipeline_name, batch_size)

    inputs = _resolve_inputs(input_path)
    log.info("inputs: %s", [str(p) for p in inputs])

    pipeline = _make_pipeline(pipeline_name, mongo_uri, mongo_db)

    started_at = datetime.now(timezone.utc)
    t0 = time.monotonic()
    try:
        pipeline.ingest(inputs, run_id, batch_size)
        pipeline.aggregate(run_id, batch_size)
        pipeline.final_merge(run_id)
        stats: RunStats = pipeline.collect_run_stats(run_id)

        # Empty-input guard: zero parsed records is almost certainly a
        # mis-configured input path; fail loudly instead of writing a
        # meaningless runs row with all-zero stats.
        if stats.total_records == 0:
            raise RuntimeError(
                f"No parsed records for run {run_id} "
                f"(malformed={stats.malformed_count}). "
                "Check --input path and contents."
            )

        # `with psycopg2.connect()` only manages the transaction, not the
        # connection itself — close explicitly to avoid leaking backends.
        pg_conn = psycopg2.connect(pg_dsn)
        try:
            with pg_conn:
                # 1. Placeholder runs row (status='running') so q*_final inserts
                #    satisfy the FK. If we crash before the UPDATE, the row
                #    stays as 'running' — distinguishable from a real result.
                _insert_runs_placeholder(
                    pg_conn, run_id=run_id, pipeline_name=pipeline_name,
                    started_at=started_at,
                    batch_size=(batch_size if batch_size is not None else 0),
                    stats=stats,
                )
                loader = getattr(pipeline, "load_results", None)
                if callable(loader):
                    loader(run_id, pg_conn)
                elif pipeline_name == "mongodb":
                    load(run_id, pipeline.db, pg_conn)
                else:
                    raise NotImplementedError(
                        f"Pipeline '{pipeline_name}' must implement load_results()"
                    )
                # 3. Stop the clock; final results are now persisted.
                t1 = time.monotonic()
                finished_at = datetime.now(timezone.utc)
                runtime_ms = int((t1 - t0) * 1000)
                # 4. Finalise runs row with real timings + status='completed'
                #    (commits at `with` exit).
                _finalize_runs_row(pg_conn, run_id, finished_at, runtime_ms)
        finally:
            pg_conn.close()
    finally:
        pipeline.cleanup(run_id, keep_staging=keep_staging)
        if hasattr(pipeline, "close"):
            pipeline.close()

    log.info("run %s done in %d ms", run_id, runtime_ms)
    return run_id


def _insert_runs_placeholder(
    pg_conn,
    *,
    run_id: str,
    pipeline_name: str,
    started_at: datetime,
    batch_size: int,
    stats: RunStats,
) -> None:
    """Insert runs with provisional finished_at/runtime_ms; refined after load()."""
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO runs (
                run_id, pipeline, status, started_at, finished_at, runtime_ms,
                batch_size, num_batches, avg_batch_size,
                total_records, malformed_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                run_id, pipeline_name, "running", started_at, started_at, 0,
                batch_size, stats.num_batches, stats.avg_batch_size,
                stats.total_records, stats.malformed_count,
            ),
        )


def _finalize_runs_row(pg_conn, run_id: str, finished_at: datetime, runtime_ms: int) -> None:
    with pg_conn.cursor() as cur:
        cur.execute(
            "UPDATE runs "
            "SET finished_at = %s, runtime_ms = %s, status = 'completed' "
            "WHERE run_id = %s",
            (finished_at, runtime_ms, run_id),
        )
