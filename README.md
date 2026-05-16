# Multi-Pipeline ETL & Reporting Framework — NASA HTTP Logs

DAS 839 — NoSQL Systems End Semester Project, Phase 1 & 2.

End-to-end ETL over the NASA Kennedy Space Center HTTP access logs (Jul95 + Aug95) with pluggable execution backends. 
- Phase 1 ships the **MongoDB** backend.
- Phase 2 ships the **Pig** backend (fully implemented), with Hive/MapReduce stubbed for future expansion.

## Layout

```
NoSQL_Project_ETL_Analysis_Pipeline/
├── docker-compose.yml          # mongo:7 + postgres:16
├── pyproject.toml              # python deps
├── PARSING_SPEC.md             # frozen parsing rules (regex, batch_id, tie-breaks)
├── sql/schema.sql              # postgres DDL (runs, q1, q2, q3)
├── src/
│   ├── cli.py                  # argparse: `run`, `report`
│   ├── orchestrator.py         # generic batching, dispatch, timer, runs row, cleanup
│   ├── loader.py               # mongo *_final → postgres
│   ├── pipelines/
│   │   ├── base.py             # Pipeline ABC + RunStats
│   │   ├── mongodb/            # phase 1 backend
│   │   ├── pig/                # phase 2 backend (completed)
│   │   ├── hive/ mr/           # phase 2 stubs
│   └── reporting/report.py     # tabulate output
└── tests/                      # parsing + batch_id + small e2e
```

## Setup

### Option A — Local Python + Docker stores (recommended)

```bash
cd NoSQL_Project_ETL_Analysis_Pipeline

# 1. start mongo + postgres (postgres auto-loads sql/schema.sql on first boot)
docker compose up -d

# 2. python venv with all libs
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 3. env config
cp .env.example .env

# 4. (optional) re-apply schema if you skipped Docker
psql "postgresql://etl:etl@localhost:5432/etl" -f sql/schema.sql
```

### Option B — Fully local (no Docker)

Install MongoDB 7 and PostgreSQL 16 locally, create db/user `etl/etl`, then run `psql -f sql/schema.sql` manually. Same `pip install -e ".[dev]"` step. Ensure Apache Hadoop and Pig are installed locally for the Pig backend to execute.

## Usage & Features

The CLI interface strictly follows the project guidelines, providing a single coherent entry point to orchestrate all backends, manage generic batching, and filter query execution.

### Basic Execution
Execute the pipeline on the dataset. The orchestrator tracks metadata and saves the final results directly to the PostgreSQL database.

```bash
# Execute using the MongoDB pipeline
python -m src.cli run --pipeline mongodb --input dataset/

# Execute using the Pig pipeline
python -m src.cli run --pipeline pig --input dataset/
```

### 1. Batching Strategy (`--batch-size`)
By default, if you omit `--batch-size`, the orchestrator uses **file-based batching**, treating the `NASA_access_log_Jul95` file as Batch 1 and `NASA_access_log_Aug95` as Batch 2.

If you specify `--batch-size N`, the orchestrator implements **generic line-based batching**. It reads the raw log files and physically chunks them into strict `N`-line batches in a temporary staging directory *before* handing the paths to the pipeline. This ensures:
1. All pipelines (Pig, Mongo, etc.) receive the exact same batch boundaries.
2. We comply with the rule that Python should not parse/clean the data (the pipelines still load raw string data).

```bash
# Run the Pig pipeline, breaking the input into 500,000 line batches
python -m src.cli run --pipeline pig --input dataset/ --batch-size 500000
```

### 2. Query Selection (`--query`)
To comply with the ability to selectively run the workload, the `--query` flag allows you to pick which query to process and save.
- **Choices:** `1`, `2`, `3`, or `all`
- **Default:** `all`

This significantly reduces execution time when isolating specific insights by dynamically disabling the unused aggregations in the underlying execution engine.

```bash
# Run only Query 1 (Daily Traffic Summary) with a 2-million record batch size
python -m src.cli run --pipeline pig --input dataset/ --query 1 --batch-size 2000000
```

### 3. Reporting Results (`report`)
Once a run completes (and saves the data to PostgreSQL), the CLI will print a `run_id`. You must use the `report` command to fetch and format the results exactly as required by the evaluation guidelines.

```bash
python -m src.cli report --run-id <RUN_ID>
```
The report generates:
1. **Execution Metadata:** Shows pipeline name, runtime, batch counts, average batch size, and malformed records.
2. **Query Results:** Formats Q1, Q2, and Q3 outputs sequentially if they were included in the query selection for that run.

## Tests

```bash
pytest -q
```

Smoke-level coverage: regex parsing edge cases, batch-id formula, tiny in-memory aggregation roundtrip.
