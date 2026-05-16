from __future__ import annotations

import os
import shutil
import subprocess
import glob
import json
from pathlib import Path
from typing import Iterable

from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

from ..base import Pipeline, RunStats


class MapReducePipeline(Pipeline):
    name = "mr"

    def __init__(self, *args, **kwargs) -> None:
        self.hadoop_cmd = os.environ.get("HADOOP_CMD", "hadoop")
        self.streaming_jar = os.environ.get("HADOOP_STREAMING_JAR")
        self.work_dir = Path(os.environ.get("MR_WORK_DIR", ".mr_runs"))
        self._run_inputs: dict[str, list[Path]] = {}
        self._run_dirs: dict[str, Path] = {}

    def _find_streaming_jar(self) -> str:
        if self.streaming_jar and os.path.exists(self.streaming_jar):
            return self.streaming_jar
            
        # Try to find it in common locations
        common_paths = [
            "/opt/homebrew/Cellar/hadoop/*/libexec/share/hadoop/tools/lib/hadoop-streaming-*.jar",
            "/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-*.jar",
            "/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"
        ]
        
        # Try asking hadoop classpath
        try:
            res = subprocess.run([self.hadoop_cmd, "classpath"], capture_output=True, text=True)
            if res.returncode == 0:
                for p in res.stdout.split(':'):
                    if 'hadoop-streaming' in p and p.endswith('.jar'):
                        return p
        except Exception:
            pass
            
        for pattern in common_paths:
            matches = glob.glob(pattern)
            if matches:
                return matches[0]
                
        # If we really can't find it, we'll try to rely on a pure-python fallback in aggregate
        return ""

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
        
        mapper_path = Path(__file__).parent / "mapper.py"
        reducer_path = Path(__file__).parent / "reducer.py"

        # Create batch mapping for file-based batching
        batch_mapping = {}
        for idx, p in enumerate(inputs, start=1):
            batch_mapping[p.name] = idx
            
        env = os.environ.copy()
        env["BATCH_MAPPING"] = json.dumps(batch_mapping)
        env["MR_QUERY"] = query
        
        jar_path = self._find_streaming_jar()
        mr_out_dir = out_dir / "mr_output"
        
        if jar_path:
            input_args = []
            for p in inputs:
                input_args.extend(["-input", f"file://{p}"])
                
            cmd = [
                self.hadoop_cmd, "jar", jar_path,
                "-D", "mapreduce.job.reduces=1",
                "-D", "mapred.job.name=nasa_logs_etl",
                "-cmdenv", f"BATCH_MAPPING={env['BATCH_MAPPING']}",
                "-cmdenv", f"MR_QUERY={query}"
            ]
            cmd.extend(input_args)
            cmd.extend([
                "-output", f"file://{mr_out_dir}",
                "-mapper", f"python3 {mapper_path}",
                "-reducer", f"python3 {reducer_path}",
                "-file", str(mapper_path),
                "-file", str(reducer_path)
            ])
            res = subprocess.run(cmd, capture_output=True, text=True, env=env)
            if res.returncode != 0:
                raise RuntimeError(
                    f"Hadoop Streaming failed:\nSTDOUT:\n{res.stdout}\nSTDERR:\n{res.stderr}"
                )
        else:
            # Fallback to local python streaming
            mr_out_dir.mkdir(parents=True, exist_ok=True)
            output_file = mr_out_dir / "part-00000"
            
            mapper_out = []
            
            # Run mapper for each file to set mapreduce_map_input_file
            for p in inputs:
                env_copy = env.copy()
                env_copy["mapreduce_map_input_file"] = str(p)
                with open(p, 'r', encoding='utf-8', errors='replace') as f:
                    map_proc = subprocess.run(
                        [sys.executable, str(mapper_path)], 
                        stdin=f, 
                        capture_output=True, 
                        text=True, 
                        env=env_copy
                    )
                    if map_proc.returncode != 0:
                        raise RuntimeError(f"Mapper failed: {map_proc.stderr}")
                    mapper_out.extend(line for line in map_proc.stdout.splitlines() if line)
            
            # Sort the mapped output
            mapper_out.sort()
            
            # Run reducer
            sorted_str = '\n'.join(mapper_out) + '\n'
            with open(output_file, 'w', encoding='utf-8') as out_f:
                red_proc = subprocess.run(
                    [sys.executable, str(reducer_path)],
                    input=sorted_str,
                    text=True,
                    stdout=out_f,
                    env=env
                )
                if red_proc.returncode != 0:
                    raise RuntimeError("Reducer failed")

    def final_merge(self, run_id: str) -> None:
        out_dir = self._run_dirs[run_id]
        mr_out_dir = out_dir / "mr_output"
        
        # Read the reducer output
        stats_data = {"total": 0, "malformed": 0, "batches": set()}
        q1_data = []
        q2_metrics = {}
        q2_hosts = {}
        q3_totals = {}
        q3_errors = {}
        q3_hosts = {}
        
        files = glob.glob(str(mr_out_dir / "part-*"))
        for fpath in files:
            with open(fpath, "r") as f:
                for line in f:
                    line = line.rstrip('\n')
                    if not line:
                        continue
                    parts = line.split('\t')
                    key_parts = parts[0].split('|')
                    prefix = key_parts[0]
                    
                    if prefix == "stats":
                        if key_parts[1] == "total":
                            stats_data["total"] += int(parts[1])
                        elif key_parts[1] == "malformed":
                            stats_data["malformed"] += int(parts[1])
                        elif key_parts[1] == "batch":
                            stats_data["batches"].add(int(key_parts[2]))
                    elif prefix == "q1":
                        q1_data.append([key_parts[1], int(key_parts[2]), int(parts[1]), int(parts[2])])
                    elif prefix == "q2_metric":
                        resource_path = key_parts[1]
                        q2_metrics[resource_path] = (int(parts[1]), int(parts[2]))
                    elif prefix == "q2_host":
                        resource_path = key_parts[1]
                        q2_hosts[resource_path] = int(parts[1])
                    elif prefix == "q3_total":
                        q3_totals[(key_parts[1], int(key_parts[2]))] = int(parts[1])
                    elif prefix == "q3_error":
                        q3_errors[(key_parts[1], int(key_parts[2]))] = int(parts[1])
                    elif prefix == "q3_host":
                        q3_hosts[(key_parts[1], int(key_parts[2]))] = int(parts[1])

        # Write stats
        num_batches = len(stats_data["batches"])
        with open(out_dir / "stats", "w") as f:
            f.write(f"{stats_data['total']}\t{stats_data['malformed']}\t{num_batches}\n")
            
        # Write q1
        if q1_data:
            # Sort by log_date ASC, status_code ASC
            q1_data.sort(key=lambda x: (x[0], x[1]))
            with open(out_dir / "q1_final", "w") as f:
                for row in q1_data:
                    f.write(f"{row[0]}\t{row[1]}\t{row[2]}\t{row[3]}\n")
                    
        # Write q2
        if q2_metrics:
            q2_list = []
            for path, (req_count, bytes_transferred) in q2_metrics.items():
                distinct_hosts = q2_hosts.get(path, 0)
                q2_list.append({
                    "resource_path": path,
                    "request_count": req_count,
                    "total_bytes": bytes_transferred,
                    "distinct_host_count": distinct_hosts
                })
            # Sort by request_count DESC, resource_path ASC
            q2_list.sort(key=lambda x: (-x["request_count"], x["resource_path"]))
            
            # Apply dense rank and top 20
            q2_top = []
            current_rank = 1
            for i, row in enumerate(q2_list):
                if current_rank > 20:
                    break
                row["rank"] = current_rank
                q2_top.append(row)
                if i + 1 < len(q2_list) and q2_list[i+1]["request_count"] != row["request_count"]:
                    current_rank += 1
                elif i + 1 < len(q2_list) and q2_list[i+1]["request_count"] == row["request_count"] and q2_list[i+1]["resource_path"] != row["resource_path"]:
                     current_rank += 1
            
            with open(out_dir / "q2_final", "w") as f:
                for row in q2_top:
                    f.write(f"{row['rank']}\t{row['resource_path']}\t{row['request_count']}\t{row['total_bytes']}\t{row['distinct_host_count']}\n")

        # Write q3
        if q3_errors:
            q3_list = []
            for key, err_count in q3_errors.items():
                if err_count > 0:
                    total_count = q3_totals.get(key, 0)
                    distinct_hosts = q3_hosts.get(key, 0)
                    error_rate = err_count / total_count if total_count > 0 else 0.0
                    q3_list.append({
                        "log_date": key[0],
                        "log_hour": key[1],
                        "error_request_count": err_count,
                        "total_request_count": total_count,
                        "error_rate": error_rate,
                        "distinct_error_hosts": distinct_hosts
                    })
            
            q3_list.sort(key=lambda x: (x["log_date"], x["log_hour"]))
            
            with open(out_dir / "q3_final", "w") as f:
                for row in q3_list:
                    f.write(f"{row['log_date']}\t{row['log_hour']}\t{row['error_request_count']}\t{row['total_request_count']}\t{row['error_rate']:.6f}\t{row['distinct_error_hosts']}\n")

    def collect_run_stats(self, run_id: str) -> RunStats:
        out_dir = self._run_dirs.get(run_id)
        if not out_dir:
            raise RuntimeError(f"Missing run directory for {run_id}")
        
        stats_file = out_dir / "stats"
        if not stats_file.exists():
            raise RuntimeError("MR stats output is missing")
            
        with open(stats_file, "r") as f:
            line = f.read().strip()
            
        parts = line.split('\t')
        total_records_i = int(parts[0])
        malformed_i = int(parts[1])
        num_batches_i = int(parts[2])
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
        out_dir = self._run_dirs.get(run_id)
        if not out_dir:
            raise RuntimeError(f"Missing run directory for {run_id}")

        with pg_conn.cursor() as cur:
            if query in ["1", "all"]:
                q1_file = out_dir / "q1_final"
                if q1_file.exists():
                    rows = []
                    with open(q1_file, "r") as f:
                        for line in f:
                            p = line.rstrip('\n').split('\t')
                            if len(p) >= 4:
                                rows.append((run_id, p[0], int(p[1]), int(p[2]), int(p[3])))
                    if rows:
                        execute_values(
                            cur,
                            "INSERT INTO q1_daily_traffic "
                            "(run_id, log_date, status_code, request_count, total_bytes) "
                            "VALUES %s",
                            rows
                        )

            if query in ["2", "all"]:
                q2_file = out_dir / "q2_final"
                if q2_file.exists():
                    rows = []
                    with open(q2_file, "r") as f:
                        for line in f:
                            p = line.rstrip('\n').split('\t')
                            if len(p) >= 5:
                                rows.append((run_id, int(p[0]), p[1] if p[1] else "", int(p[2]), int(p[3]), int(p[4])))
                    if rows:
                        execute_values(
                            cur,
                            "INSERT INTO q2_top_resources "
                            "(run_id, rank, resource_path, request_count, total_bytes, "
                            "distinct_host_count) VALUES %s",
                            rows
                        )

            if query in ["3", "all"]:
                q3_file = out_dir / "q3_final"
                if q3_file.exists():
                    rows = []
                    with open(q3_file, "r") as f:
                        for line in f:
                            p = line.rstrip('\n').split('\t')
                            if len(p) >= 6:
                                rows.append((run_id, p[0], int(p[1]), int(p[2]), int(p[3]), round(float(p[4]), 6), int(p[5])))
                    if rows:
                        execute_values(
                            cur,
                            "INSERT INTO q3_hourly_errors "
                            "(run_id, log_date, log_hour, error_request_count, "
                            "total_request_count, error_rate, distinct_error_hosts) "
                            "VALUES %s",
                            rows
                        )
