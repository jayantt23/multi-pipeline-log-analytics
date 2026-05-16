"""Apache Pig backend — Phase 2 (file-based batching for Jul/Aug)."""
from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Iterable

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from ..base import Pipeline, RunStats


class PigPipeline(Pipeline):
    name = "pig"

    def __init__(self, *args, **kwargs) -> None:
        self.pig_cmd = os.environ.get("PIG_CMD", "pig")
        self.exec_mode = os.environ.get("PIG_EXEC_MODE", "local")
        self.parallel = int(os.environ.get("PIG_PARALLEL", "4"))
        self.work_dir = Path(os.environ.get("PIG_WORK_DIR", ".pig_runs"))
        self._run_inputs: dict[str, list[Path]] = {}
        self._run_dirs: dict[str, Path] = {}

    def ingest(self, input_paths: list[Path], run_id: str, batch_size: int | None) -> None:
        if not input_paths:
            raise ValueError("No input files provided")
        ordered = sorted(
            input_paths,
            key=lambda p: (0 if "Jul" in p.name else 1, p.name),
        )
        self.work_dir.mkdir(parents=True, exist_ok=True)
        run_dir = self.work_dir / run_id
        run_dir.mkdir(parents=True, exist_ok=True)
        self._run_inputs[run_id] = [p.resolve() for p in ordered]
        self._run_dirs[run_id] = run_dir

    def aggregate(self, run_id: str, batch_size: int | None, query: str = "all") -> None:
        inputs = self._run_inputs.get(run_id)
        if not inputs:
            raise RuntimeError(f"No inputs registered for run {run_id}")
        out_dir = self._run_dirs[run_id]

        script = self._build_pig_script(inputs, out_dir, self.parallel, query)
        script_path = out_dir / "pig_etl.pig"
        script_path.write_text(script)

        env = os.environ.copy()
        pig_cp = env.get("PIG_CLASSPATH", "")
        extra_cp = "/opt/homebrew/Cellar/pig/0.18.0/libexec/lib/hadoop3-runtime/*"
        if pig_cp:
            env["PIG_CLASSPATH"] = f"{pig_cp}:{extra_cp}"
        else:
            env["PIG_CLASSPATH"] = extra_cp

        cmd = [self.pig_cmd, "-x", self.exec_mode, "-f", str(script_path)]
        res = subprocess.run(cmd, capture_output=True, text=True, env=env)
        if res.returncode != 0:
            raise RuntimeError(
                "Pig execution failed:\n"
                f"STDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
            )

    def final_merge(self, run_id: str) -> None:
        return None

    def collect_run_stats(self, run_id: str) -> RunStats:
        run_dir = self._run_dirs.get(run_id)
        if not run_dir:
            raise RuntimeError(f"Missing run directory for {run_id}")
        stats_rows = self._read_pig_output(run_dir / "stats")
        if not stats_rows:
            raise RuntimeError("Pig stats output is missing")
        total_records, malformed_count, num_batches = stats_rows[0]
        total_records_i = int(total_records)
        malformed_i = int(malformed_count)
        num_batches_i = int(num_batches)
        avg_batch_size = (total_records_i / num_batches_i) if num_batches_i else 0.0
        return RunStats(
            total_records=total_records_i,
            num_batches=num_batches_i,
            avg_batch_size=avg_batch_size,
            malformed_count=malformed_i,
        )

    def cleanup(self, run_id: str, keep_staging: bool = False) -> None:
        if keep_staging:
            return
        run_dir = self._run_dirs.get(run_id)
        if run_dir and run_dir.exists():
            shutil.rmtree(run_dir)
        self._run_dirs.pop(run_id, None)
        self._run_inputs.pop(run_id, None)

    def load_results(self, run_id: str, pg_conn: PgConnection, query: str = "all") -> None:
        run_dir = self._run_dirs.get(run_id)
        if not run_dir:
            raise RuntimeError(f"Missing run directory for {run_id}")

        with pg_conn.cursor() as cur:
            if query in ["1", "all"]:
                q1_rows = self._read_pig_output(run_dir / "q1_final")
                if q1_rows:
                    execute_values(
                        cur,
                        "INSERT INTO q1_daily_traffic "
                        "(run_id, log_date, status_code, request_count, total_bytes) "
                        "VALUES %s",
                        [
                            (
                                run_id,
                                row[0],
                                int(row[1]),
                                int(row[2]),
                                int(row[3]),
                            )
                            for row in q1_rows
                        ],
                    )

            if query in ["2", "all"]:
                q2_rows = self._read_pig_output(run_dir / "q2_final")
                if q2_rows:
                    execute_values(
                        cur,
                        "INSERT INTO q2_top_resources "
                        "(run_id, rank, resource_path, request_count, total_bytes, "
                        "distinct_host_count) VALUES %s",
                        [
                            (
                                run_id,
                                int(row[0]),
                                row[1] if row[1] else "",
                                int(row[2]),
                                int(row[3]),
                                int(row[4]),
                            )
                            for row in q2_rows
                        ],
                    )

            if query in ["3", "all"]:
                q3_rows = self._read_pig_output(run_dir / "q3_final")
                if q3_rows:
                    execute_values(
                        cur,
                        "INSERT INTO q3_hourly_errors "
                        "(run_id, log_date, log_hour, error_request_count, "
                        "total_request_count, error_rate, distinct_error_hosts) "
                        "VALUES %s",
                        [
                            (
                                run_id,
                                row[0],
                                int(row[1]),
                                int(row[2]),
                                int(row[3]),
                                round(float(row[4]), 6),
                                int(row[5]),
                            )
                            for row in q3_rows
                        ],
                    )

    def _build_pig_script(self, input_paths: list[Path], out_dir: Path, parallel: int, query: str = "all") -> str:
        load_lines: list[str] = []
        union_aliases: list[str] = []
        for idx, path in enumerate(input_paths, start=1):
            alias = f"raw_{idx}"
            tag_alias = f"{alias}_tag"
            load_lines.append(
                f"{alias} = LOAD '{path}' USING PigStorage() AS (raw:chararray);"
            )
            load_lines.append(
                f"{tag_alias} = FOREACH {alias} GENERATE raw, {idx} AS batch_id;"
            )
            union_aliases.append(tag_alias)

        raw_union = ", ".join(union_aliases)
        regex = r'^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] "([^"]*)" (\\d{3}|-) (\\d+|-)$'
        
        script = f"""SET default_parallel {parallel};
{chr(10).join(load_lines)}
raw_all = UNION {raw_union};

parsed_raw = FOREACH raw_all GENERATE
    raw,
    batch_id,
    REGEX_EXTRACT_ALL(raw, '{regex}')
        AS extracted:tuple(host:chararray, timestamp_raw:chararray, request_raw:chararray, status_raw:chararray, bytes_raw:chararray);

parsed_fields = FOREACH parsed_raw GENERATE
    batch_id,
    extracted.host AS host,
    extracted.timestamp_raw AS timestamp_raw,
    extracted.request_raw AS request_raw,
    extracted.status_raw AS status_raw,
    extracted.bytes_raw AS bytes_raw;

malformed = FILTER parsed_fields BY host IS NULL OR status_raw == '-';
valid = FILTER parsed_fields BY host IS NOT NULL AND status_raw != '-';

req_parts = FOREACH valid GENERATE
    batch_id,
    host,
    timestamp_raw,
    STRSPLIT(request_raw, '\\\\s+', 3) AS req_tokens,
    status_raw,
    bytes_raw;

parsed = FOREACH req_parts GENERATE
    batch_id,
    host,
    ToDate(timestamp_raw, 'dd/MMM/yyyy:HH:mm:ss Z') AS ts,
    (chararray)req_tokens.$0 AS http_method,
    (req_tokens.$1 IS NULL ? '' : (chararray)req_tokens.$1) AS resource_path,
    (chararray)req_tokens.$2 AS protocol_version,
    (int)status_raw AS status_code,
    (bytes_raw == '-' ? 0L : (long)bytes_raw) AS bytes_transferred;

    
parsed_enriched = FOREACH parsed GENERATE
    batch_id,
    host,
    ToString(ts, 'yyyy-MM-dd') AS log_date,
    GetHour(ts) AS log_hour,
    http_method,
    resource_path,
    protocol_version,
    status_code,
    bytes_transferred;

total_group = GROUP parsed_enriched ALL PARALLEL {parallel};
total_stats = FOREACH total_group GENERATE COUNT_STAR(parsed_enriched) AS total_records;

malformed_group = GROUP malformed ALL PARALLEL {parallel};
malformed_stats = FOREACH malformed_group GENERATE COUNT_STAR(malformed) AS malformed_count;

batch_ids = DISTINCT (FOREACH parsed_enriched GENERATE batch_id);
batch_group = GROUP batch_ids ALL PARALLEL {parallel};
batch_stats = FOREACH batch_group GENERATE COUNT_STAR(batch_ids) AS num_batches;

stats = CROSS total_stats, malformed_stats, batch_stats;
STORE stats INTO '{str(out_dir).replace(os.sep, '/')}/stats' USING PigStorage('\\t');
"""
        if query in ["1", "all"]:
            script += f"""
q1_group = GROUP parsed_enriched BY (log_date, status_code) PARALLEL {parallel};
q1_out = FOREACH q1_group GENERATE
    group.log_date AS log_date,
    group.status_code AS status_code,
    COUNT_STAR(parsed_enriched) AS request_count,
    SUM(parsed_enriched.bytes_transferred) AS total_bytes;
q1_sorted = ORDER q1_out BY log_date ASC, status_code ASC;
STORE q1_sorted INTO '{str(out_dir).replace(os.sep, '/')}/q1_final' USING PigStorage('\\t');
"""

        if query in ["2", "all"]:
            script += f"""
q2_group = GROUP parsed_enriched BY resource_path PARALLEL {parallel};
q2_metrics = FOREACH q2_group GENERATE
    group AS resource_path,
    COUNT_STAR(parsed_enriched) AS request_count,
    SUM(parsed_enriched.bytes_transferred) AS total_bytes;

q2_pairs = DISTINCT (FOREACH parsed_enriched GENERATE resource_path, host);
q2_pairs_group = GROUP q2_pairs BY resource_path PARALLEL {parallel};
q2_hosts = FOREACH q2_pairs_group GENERATE
    group AS resource_path,
    COUNT_STAR(q2_pairs) AS distinct_host_count;

q2_join = JOIN q2_metrics BY resource_path, q2_hosts BY resource_path;
q2_proj = FOREACH q2_join GENERATE
    q2_metrics::resource_path AS resource_path,
    q2_metrics::request_count AS request_count,
    q2_metrics::total_bytes AS total_bytes,
    q2_hosts::distinct_host_count AS distinct_host_count;

q2_sorted = ORDER q2_proj BY request_count DESC, resource_path ASC;
q2_ranked = RANK q2_sorted BY request_count DESC, resource_path ASC DENSE;
q2_top = FILTER q2_ranked BY rank_q2_sorted <= 20;
q2_out = FOREACH q2_top GENERATE
    (int)rank_q2_sorted AS rank,
    resource_path,
    request_count,
    total_bytes,
    distinct_host_count;
STORE q2_out INTO '{str(out_dir).replace(os.sep, '/')}/q2_final' USING PigStorage('\\t');
"""

        if query in ["3", "all"]:
            script += f"""
errors = FILTER parsed_enriched BY status_code >= 400 AND status_code <= 599;
err_group = GROUP errors BY (log_date, log_hour) PARALLEL {parallel};
err_metrics = FOREACH err_group GENERATE
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT_STAR(errors) AS error_request_count;

err_pairs = DISTINCT (FOREACH errors GENERATE log_date, log_hour, host);
err_pairs_group = GROUP err_pairs BY (log_date, log_hour) PARALLEL {parallel};
err_hosts = FOREACH err_pairs_group GENERATE
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT_STAR(err_pairs) AS distinct_error_hosts;

total_by_hour = GROUP parsed_enriched BY (log_date, log_hour) PARALLEL {parallel};
total_metrics = FOREACH total_by_hour GENERATE
    group.log_date AS log_date,
    group.log_hour AS log_hour,
    COUNT_STAR(parsed_enriched) AS total_request_count;

err_join = JOIN err_metrics BY (log_date, log_hour), err_hosts BY (log_date, log_hour);
err_total = JOIN err_join BY (err_metrics::log_date, err_metrics::log_hour), total_metrics BY (log_date, log_hour);

q3_out = FOREACH err_total GENERATE
    err_join::err_metrics::log_date AS log_date,
    err_join::err_metrics::log_hour AS log_hour,
    err_join::err_metrics::error_request_count AS error_request_count,
    total_metrics::total_request_count AS total_request_count,
    (double)err_join::err_metrics::error_request_count / (double)total_metrics::total_request_count AS error_rate,
    err_join::err_hosts::distinct_error_hosts AS distinct_error_hosts;

q3_sorted = ORDER q3_out BY log_date ASC, log_hour ASC;
STORE q3_sorted INTO '{str(out_dir).replace(os.sep, '/')}/q3_final' USING PigStorage('\\t');
"""
        return script

    def _read_pig_output(self, path: Path) -> list[list[str]]:
        if not path.exists():
            return []
        files: Iterable[Path]
        if path.is_dir():
            files = sorted(p for p in path.iterdir() if p.name.startswith("part-"))
        else:
            files = [path]
        rows: list[list[str]] = []
        for file in files:
            for line in file.read_text().splitlines():
                if not line.strip():
                    continue
                rows.append([col for col in line.split("\t")])
        return rows
