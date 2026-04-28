# Multi-Pipeline ETL & Reporting Framework — NASA HTTP Logs

DAS 839 — NoSQL Systems End Semester Project, Phase 1.

End-to-end ETL over the NASA Kennedy Space Center HTTP access logs (Jul95 + Aug95) with pluggable execution backends. Phase 1 ships the **MongoDB** backend; Pig / Hive / MapReduce backends are stubbed for Phase 2.

## Layout

```
NoSQL_Project_ETL_Analysis_Pipeline/
├── docker-compose.yml          # mongo:7 + postgres:16
├── pyproject.toml              # python deps
├── PARSING_SPEC.md             # frozen parsing rules (regex, batch_id, tie-breaks)
├── sql/schema.sql              # postgres DDL (runs, q1, q2, q3)
├── src/
│   ├── cli.py                  # argparse: `run`, `report`
│   ├── orchestrator.py         # dispatch, timer, runs row, cleanup
│   ├── loader.py               # mongo *_final → postgres
│   ├── pipelines/
│   │   ├── base.py             # Pipeline ABC + RunStats
│   │   ├── mongodb/            # phase 1 backend
│   │   ├── pig/ hive/ mr/      # phase 2 stubs
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

Install MongoDB 7 and PostgreSQL 16 locally, create db/user `etl/etl`, then run `psql -f sql/schema.sql` manually. Same `pip install -e ".[dev]"` step.

## Demo

```bash
# end-to-end run on the real dataset (uses ../dataset/)
python -m src.cli run --pipeline mongodb \
    --input ../dataset/ --batch-size 100000

# the run prints a run_id; pretty-print results + metadata:
python -m src.cli report --run-id <RUN_ID>
```

For the comparative demo, repeat `run` with `--batch-size 50000` and `--batch-size 200000`. `num_batches` should change but the Q1/Q2/Q3 result rows must be identical.

## Tests

```bash
pytest -q
```

Smoke-level coverage: regex parsing edge cases, batch-id formula, tiny in-memory aggregation roundtrip.

## Phase 2 (later)

Pig, Hive, MapReduce backends wired into the same `Pipeline` ABC contract; cross-engine equivalence diff for Q1/Q2/Q3; comparative report.
