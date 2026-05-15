"""Hadoop MapReduce backend — Phase 2 stub."""
from __future__ import annotations

from pathlib import Path

from ..base import Pipeline, RunStats


class MapReducePipeline(Pipeline):
    name = "mr"

    def __init__(self, *args, **kwargs) -> None:
        pass

    def ingest(self, input_paths: list[Path], run_id: str, batch_size: int | None) -> None:
        raise NotImplementedError("Phase 2: MapReduce backend not yet implemented")

    def aggregate(self, run_id: str, batch_size: int | None) -> None:
        raise NotImplementedError("Phase 2: MapReduce backend not yet implemented")

    def final_merge(self, run_id: str) -> None:
        raise NotImplementedError("Phase 2: MapReduce backend not yet implemented")

    def collect_run_stats(self, run_id: str) -> RunStats:
        raise NotImplementedError("Phase 2: MapReduce backend not yet implemented")

    def cleanup(self, run_id: str, keep_staging: bool = False) -> None:
        raise NotImplementedError("Phase 2: MapReduce backend not yet implemented")
