# GitHub Engineering Observatory

A Databricks-based lakehouse and forecasting system that measures the public
GitHub engineering ecosystem across production, flow, rework, contribution,
engagement, sustainability, and network structure — built on
[GH Archive](https://www.gharchive.org/) hourly event files, with
Bronze/Silver/Gold modeling and MLflow-managed forecasting.

**Status: all spec §21 steps (1–10) complete and verified in-workspace
2026-07-30.** Notebooks 01–08 ran green on serverless; the scheduled job
`github-observatory-hourly-refresh` (hourly at :20 UTC) now keeps the
lakehouse current end to end. Current state: **13.2M events** across a
contiguous 72h backfill (2026-07-27→29) plus live hourly ingestion,
perfect Bronze↔Silver parity on all files (0 data-quality violations),
full Gold surface (hourly production metrics + velocity, flow,
contribution, engagement, retention, network) and the seasonal-naive
forecast slice with MLflow tracking. Early honest result: with only 3
days of history `naive_1h` beats `seasonal_24h` (MASE 1.0 vs 1.28,
sMAPE ≈ 2%) — revisit once the scheduler accumulates ≥2 weeks (OQ-9,
OQ-12).

**Deep history (2026-07-31):** `gold.ecosystem_hourly` now spans
**2016→present** — 85,983 historical hours imported from the BigQuery
public dataset (census columns only, `source='bigquery'`, $0 within the
free query tier; see `docs/metric_definitions.md` v3). Decade headline:
bot share of public events rose from **0% (2016) to 26.6% (2025)**.
The 2025→2026 discontinuity (push share 65%→94%, bot share 27%→9%) is
the strongest evidence yet for OQ-1: the 2026 feed is a filtered
regime — never compare across `source` without caveats.
The empirical findings materially change the downstream
design — read
[`docs/schema_validation_report.md`](docs/schema_validation_report.md) before
touching Silver/Gold code. Highlights from 499,742 profiled events
(3 sampled hours, 2026-07):

* PushEvent is **95.5%** of the 2026 stream; non-push classes look
  under-sampled at the source (open question OQ-1).
* PushEvent payloads carry **no commit data** (no `commits`, `size`,
  `distinct_size`) — pushes are the production unit.
* `pull_request.merged`/`state` are **gone**; merges are now
  `payload.action = 'merged'`.
* Event `id` is a safe primary key; `push_id` is **not** unique.
* One `public: false` event with empty `repo` appeared — quarantine rules
  are required, not theoretical.

## Layout

```
src/github_observatory/
  common/config.py        Unity Catalog names, source URL template
  ingestion/download_gharchive.py   idempotent hourly downloader + audit log
  ingestion/bronze_ingest.py        raw files → bronze.events_raw/quarantine/audit
  ingestion/bq_history.py           BigQuery decade aggregates → bronze.bq_ecosystem_hourly
                                    → gold (source='bigquery', stream wins)
  schema/profile_schema.py          stream schema profiler + Delta writer
  silver/transforms.py              Bronze → silver.events + lifecycle tables
                                    (MERGE SQL builders — the single implementation)
  gold/metrics.py                   silver.events → gold.ecosystem_hourly
                                    + ecosystem_velocity view
  gold/behavior.py                  flow / contribution / engagement /
                                    data-quality daily tables
  gold/sustainability.py            actor retention + network structure
  forecasting/seasonal_naive.py     gap-aware baselines + eval (pure stdlib)
notebooks/
  01_download_and_profile.py        run download + profiling on Databricks
  02_bronze_ingest.py               create + load the Bronze tables
  03_silver_build.py                build Silver + serverless spot-checks
  04_gold_build.py                  build Gold hourly metrics + invariants
  05_backfill.py                    contiguous 72h window: download→Bronze→Gold
  06_behavior_build.py              behavior/sustainability/DQ build + checks
  07_forecast.py                    forecast eval + MLflow + next-hour
  08_hourly_pipeline.py             schedulable end-to-end hourly refresh
  09_bq_history_import.py           land + merge the 2016–2025 deep history
  exploration/                      original catalog/volume setup notebooks
scripts/
  databricks_run.py                 sync repo into workspace, run notebooks
                                    on serverless via REST (stdlib only)
  bq_export_hourly.py               decade census export from the BigQuery
                                    public dataset (COUNT DISTINCT id; free tier)
artifacts/bq_import/                committed per-year CSVs + provenance manifest
docs/
  current_state.md                  repo/infra audit (Task 1)
  schema_validation_report.md       empirical schema findings (Task 3)
  metric_definitions.md             versioned metric contract (v2; OQ-7 whitelist)
  open_questions.md                 tracked unknowns + resolutions
artifacts/schema_profile/           committed profile CSV + summary JSON
tests/unit/                         fast tests — no network, no Spark
tests/integration/                  the real MERGE SQL on local OSS
                                    Spark + Delta (dev extras; ANSI on)
```

## Environment

```
Catalog:  github_observatory   Schemas: bronze / silver / gold
Volume:   /Volumes/github_observatory/bronze/raw_files/
Compute:  serverless
```

## Quickstart (local — no Spark needed)

Core modules are stdlib-only; Python ≥ 3.10. The `dev` extras add
pytest plus pyspark/delta-spark (needs a JVM) for the integration tier,
which executes the production MERGE SQL against local Delta tables —
`GITHUB_OBSERVATORY_CATALOG=spark_catalog` retargets the three-part
table names at the local session catalog.

```bash
# 1. Run tests
pip install -e ".[dev]"
python -m pytest tests/unit -q          # fast tier (<1s)
python -m pytest tests/integration -q   # real SQL on local Delta (~80s)

# 2. Download sample hours (idempotent; writes JSONL audit records)
PYTHONPATH=src python -m github_observatory.ingestion.download_gharchive \
    2026-07-29-15 2026-07-29-3 2026-07-26-15 --dest ./raw_files

# 3. Profile the stream schema
PYTHONPATH=src python -m github_observatory.schema.profile_schema \
    ./raw_files/*.json.gz --out-dir artifacts/schema_profile

# 4. Dry-run the Bronze parse path (no Spark, nothing written)
PYTHONPATH=src python -m github_observatory.ingestion.bronze_ingest \
    ./raw_files/*.json.gz
```

Note: the GH Archive hour segment is unpadded (`…-3.json.gz`, not `…-03`),
and requests need a browser-like User-Agent — both handled by the downloader.

## Quickstart (Databricks)

With `DATABRICKS_HOST`/`DATABRICKS_TOKEN` exported, sync and run everything
from any dev box (stdlib only, no CLI install):

```bash
python scripts/databricks_run.py check
python scripts/databricks_run.py sync-repo --branch claude/github-observatory-spec-lfr2c4
python scripts/databricks_run.py run-notebook \
    "/Repos/<me>/github-engineering-observatory/notebooks/01_download_and_profile"
python scripts/databricks_run.py run-notebook \
    "/Repos/<me>/github-engineering-observatory/notebooks/02_bronze_ingest"
```

Notebook 01 verifies the catalog/schemas/volume, downloads the sample hours
into the raw Volume, profiles them, and writes
`github_observatory.bronze.schema_profile`. Notebook 02 creates
`bronze.events_raw` / `events_quarantine` / `ingestion_audit`, ingests every
hour file in the Volume idempotently (MERGE on `event_id`), and reconciles
counts against the schema profile. If workspace egress to
`data.gharchive.org` is blocked, download locally and use
`scripts/databricks_run.py upload raw_files/*.json.gz --dest
/Volumes/github_observatory/bronze/raw_files`.

## Roadmap (spec §21)

1. ~~Repository and infrastructure audit~~
2. ~~Sample download~~
3. ~~Schema profiling~~
4. ~~Schema validation report~~
5. ~~Bronze ingestion (`events_raw`, quarantine, audit)~~ — verified
   in-workspace 2026-07-30
6. ~~Silver normalization and lifecycle tables~~ — verified in-workspace
   2026-07-30
7. ~~Gold production metrics; velocity/acceleration~~ — verified 2026-07-30
8. ~~Flow, contribution, engagement metrics; data-quality monitoring~~ —
   verified 2026-07-30
9. ~~Seasonal-naive forecast + statistical baselines, MLflow tracking~~ —
   verified 2026-07-30
10. ~~Retraining workflow, sustainability and network metrics~~ —
    scheduled job live 2026-07-30

All roadmap steps are complete; ongoing work is operating the hourly
pipeline, deepening backfill, and resolving the tracked open questions
(`docs/open_questions.md`).
