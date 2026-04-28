"""Pipeline ABC — every backend (Mongo Phase 1, Pig/Hive/MR Phase 2) implements this."""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path


@dataclass
class RunStats:
    total_records: int
    num_batches: int
    avg_batch_size: float
    malformed_count: int


class Pipeline(ABC):
    """Contract every execution backend must satisfy.

    Lifecycle (driven by orchestrator):
      pipeline.ingest(input_paths, run_id, batch_size)
      pipeline.aggregate(run_id, batch_size)
      pipeline.final_merge(run_id)
      stats = pipeline.collect_run_stats(run_id)
      # ... loader copies *_final into Postgres ...
      pipeline.cleanup(run_id, keep_staging=False)
    """

    name: str

    @abstractmethod
    def ingest(self, input_paths: list[Path], run_id: str, batch_size: int) -> None:
        """Stream raw input lines into engine-native staging with stable ordinals."""

    @abstractmethod
    def aggregate(self, run_id: str, batch_size: int) -> None:
        """Parse, tag with batch_id, and produce per-batch partials + distinct-pair stages."""

    @abstractmethod
    def final_merge(self, run_id: str) -> None:
        """Combine partials across batches into q1_final / q2_final / q3_final."""

    @abstractmethod
    def collect_run_stats(self, run_id: str) -> RunStats:
        """Read total_records, num_batches, avg_batch_size, malformed_count from staging."""

    @abstractmethod
    def cleanup(self, run_id: str, keep_staging: bool = False) -> None:
        """Drop per-run staging unless keep_staging."""
