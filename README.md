# GitHub Engineering Observatory

A Databricks-based lakehouse and forecasting system that measures the public
GitHub engineering ecosystem across production, flow, rework, contribution,
engagement, sustainability, and network structure — built on
[GH Archive](https://www.gharchive.org/) hourly event files, with
Bronze/Silver/Gold modeling and MLflow-managed forecasting.

**Status:** schema validation complete (spec Tasks 1–3). The empirical
findings materially change the downstream design — read
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
  schema/profile_schema.py          stream schema profiler + Delta writer
notebooks/
  01_download_and_profile.py        run download + profiling on Databricks
  exploration/                      original catalog/volume setup notebooks
docs/
  current_state.md                  repo/infra audit (Task 1)
  schema_validation_report.md       empirical schema findings (Task 3)
  open_questions.md                 tracked unknowns + resolutions
artifacts/schema_profile/           committed profile CSV + summary JSON
tests/unit/                         downloader + profiler tests (no network)
```

## Environment

```
Catalog:  github_observatory   Schemas: bronze / silver / gold
Volume:   /Volumes/github_observatory/bronze/raw_files/
Compute:  serverless
```

## Quickstart (local — no Spark needed)

Core modules are stdlib-only; Python ≥ 3.10.

```bash
# 1. Run tests
pip install pytest && python -m pytest tests/unit -q

# 2. Download sample hours (idempotent; writes JSONL audit records)
PYTHONPATH=src python -m github_observatory.ingestion.download_gharchive \
    2026-07-29-15 2026-07-29-3 2026-07-26-15 --dest ./raw_files

# 3. Profile the stream schema
PYTHONPATH=src python -m github_observatory.schema.profile_schema \
    ./raw_files/*.json.gz --out-dir artifacts/schema_profile
```

Note: the GH Archive hour segment is unpadded (`…-3.json.gz`, not `…-03`),
and requests need a browser-like User-Agent — both handled by the downloader.

## Quickstart (Databricks)

Sync the repo into the workspace and run
`notebooks/01_download_and_profile.py` on serverless compute. It verifies the
catalog/schemas/volume, downloads the sample hours into the raw Volume,
profiles them, and writes `github_observatory.bronze.schema_profile`.

## Roadmap (spec §21)

1. ~~Repository and infrastructure audit~~
2. ~~Sample download~~
3. ~~Schema profiling~~
4. ~~Schema validation report~~
5. Bronze ingestion (`events_raw`, quarantine, audit) ← next
6. Silver normalization and lifecycle tables
7. Gold production metrics; velocity/acceleration
8. Flow, contribution, engagement metrics; data-quality monitoring
9. Seasonal-naive forecast + statistical baselines, MLflow tracking
10. Retraining workflow, sustainability and network metrics
