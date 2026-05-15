"""CLI — `etl run` and `etl report`.

Examples:
  python -m src.cli run --pipeline mongodb --input ../dataset/ --batch-size 100000
  python -m src.cli report --run-id <id>
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

from . import orchestrator
from .reporting.report import report

PIPELINES = ["mongodb", "pig", "hive", "mr"]


def _env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="etl", description="Multi-pipeline ETL framework")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser("run", help="Execute one ETL run")
    p_run.add_argument("--pipeline", required=True, choices=PIPELINES)
    p_run.add_argument("--input", required=True, type=Path,
                       help="Directory of NASA_access_log_* files, or a single file")
    p_run.add_argument("--batch-size", required=False, type=int,
                       help="Records per batch. Omit to use file-based batching (Jul/Aug).")
    p_run.add_argument("--keep-staging", action="store_true",
                       help="Skip drop of per-run Mongo staging collections")
    p_run.add_argument("--mongo-uri", default=_env("MONGO_URI", "mongodb://localhost:27017"))
    p_run.add_argument("--mongo-db", default=_env("MONGO_DB", "etl"))
    p_run.add_argument("--pg-dsn", default=_env(
        "PG_DSN", "postgresql://etl:etl@localhost:5432/etl"))

    p_rep = sub.add_parser("report", help="Pretty-print results for a run_id")
    p_rep.add_argument("--run-id", required=True)
    p_rep.add_argument("--pg-dsn", default=_env(
        "PG_DSN", "postgresql://etl:etl@localhost:5432/etl"))

    return parser


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s :: %(message)s",
    )
    args = _build_parser().parse_args(argv)

    if args.cmd == "run":
        if args.batch_size is not None and args.batch_size <= 0:
            print("--batch-size must be > 0", file=sys.stderr)
            return 2
        run_id = orchestrator.run(
            pipeline_name=args.pipeline,
            input_path=args.input,
            batch_size=args.batch_size,
            mongo_uri=args.mongo_uri,
            mongo_db=args.mongo_db,
            pg_dsn=args.pg_dsn,
            keep_staging=args.keep_staging,
        )
        print(f"\nRUN COMPLETE — run_id = {run_id}")
        print(f"Inspect results:  python -m src.cli report --run-id {run_id}")
        return 0

    if args.cmd == "report":
        out = report(args.run_id, args.pg_dsn)
        print(out)
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
