# Current State Report

**Date:** 2026-07-30
**Scope:** Spec §7 Task 1 — repository and infrastructure audit, performed before any pipeline work.
**Produced by:** Claude agent working on branch `claude/github-observatory-spec-lfr2c4`.

---

## 1. Repository state as found

At audit time the repository contained a single commit (`9b5567f`, 2026-07-29,
"Add catalog setup and bronze layer ingestion notebooks") with two
Databricks-notebook-format Python files at the repository root and nothing else
— no package code, tests, docs, CI, `pyproject.toml`, or Databricks Asset
Bundle configuration.

| File | Purpose | Notes |
| --- | --- | --- |
| `create_catalog_and_schema.py` | SQL cells creating catalog `github_observatory` and schemas `bronze`, `silver`, `gold` | Working; idempotent (`IF NOT EXISTS`) |
| `01_bronze_ingest.py` | Exploratory cells: show catalogs/schemas, create volume `github_observatory.bronze.raw_files`, list volume, read a JSON file | Scratch quality: `SHOW CATALOGS;)` has a stray `)`; the read cell references an undefined `local_path` variable. Kept as-is (exploration history), relocated to `notebooks/exploration/` |

Branch situation: `main` and `claude/github-observatory-spec-lfr2c4` both
pointed at `9b5567f`. The remote feature branch did not exist yet; it is
created by this increment's first push.

## 2. Databricks infrastructure

Per spec §6 (and corroborated by the SQL in the two notebooks, which created
these objects):

```
Catalog:  github_observatory
Schemas:  github_observatory.bronze / .silver / .gold
Volume:   github_observatory.bronze.raw_files
Path:     /Volumes/github_observatory/bronze/raw_files/
Compute:  serverless (validated per spec)
```

**Verification limitation:** this development session runs in an isolated
container without Databricks workspace credentials (no `databricks` CLI, no
`DATABRICKS_HOST`/`DATABRICKS_TOKEN`). Catalog/schema/volume existence is
therefore taken from the spec and the committed notebooks, not re-verified
here. `notebooks/01_download_and_profile.py` (added in this increment) begins
with cells that assert the catalog, schemas, and volume exist, so the first
in-workspace run re-verifies the environment before doing any work.

Constraints honored in all new code:

* No reliance on `file:/tmp` or `/dbfs/tmp`; raw files go to the Unity
  Catalog Volume, written via plain Python file APIs (FUSE).
* Core modules are stdlib-only so they run identically on serverless
  and locally; Spark/Delta interaction is isolated behind functions that
  are only invoked on Databricks.

## 3. Source verification (performed live from this session)

Facts established empirically against `data.gharchive.org` on 2026-07-30:

* **Availability:** hourly archives exist through at least 2026-07-29;
  files appear ~65 minutes after the hour closes (Last-Modified ≈ HH+1:05).
* **URL hour segment is NOT zero-padded.** `2026-07-29-3.json.gz` → HTTP 200;
  `2026-07-29-03.json.gz` → HTTP 404. The spec's `YYYY-MM-DD-HH` pattern is
  corrected in code (`download_gharchive.build_url`); zero-padded user input
  is tolerated and normalized.
* **User-Agent requirement confirmed:** default `urllib` UA → HTTP 403;
  `Mozilla/5.0` → HTTP 200. Matches spec §6.
* **Scale:** ~20 MB gzipped per hour (~160k–174k events/hour across the three
  sampled hours, i.e. roughly 4M events/day ecosystem-wide).

Samples downloaded for schema validation (Task 2), chosen to cover weekday
peak, weekday trough, and weekend:

| File | Hour (UTC) | Bytes | Events | sha256 (prefix) |
| --- | --- | --- | --- | --- |
| `2026-07-29-15.json.gz` | Wed 15:00 | 20,476,592 | 160,280 | `e57cfacc` |
| `2026-07-29-3.json.gz` | Wed 03:00 | 20,627,642 | 165,882 | `4b1d3f0d` |
| `2026-07-26-15.json.gz` | Sun 15:00 | 21,448,642 | 173,580 | `4663f485` |

(Raw `.json.gz` files are git-ignored; on Databricks they live in the raw
Volume. Download audit records are written as JSONL beside the files.)

Note: event volume is *higher* in the weekday-trough and weekend hours than
the weekday-peak hour in this small sample — evidence that naive
hour-of-day intuitions must not be hard-coded; seasonality gets estimated
from data (spec §12).

## 4. Gap analysis vs recommended structure (spec §15)

| Component | Status at audit | After this increment |
| --- | --- | --- |
| `README.md` | missing | added (quickstart + layout) |
| `pyproject.toml` | missing | added (stdlib core, pytest dev extra) |
| `src/github_observatory/ingestion/download_gharchive.py` | missing | **implemented** (Task 2) |
| `src/github_observatory/schema/profile_schema.py` | missing | **implemented** (Task 3) |
| `src/github_observatory/common/config.py` | missing | added (UC names centralized) |
| `docs/` (current_state, schema_validation_report, open_questions) | missing | added |
| `tests/unit/` | missing | added for downloader + profiler |
| Bronze/Silver/Gold pipeline modules | missing | not yet — blocked on schema validation, by design |
| `databricks.yml` + `resources/` (Asset Bundles) | missing | deferred until first vertical slice works (per spec §15) |
| Forecasting/MLflow modules | missing | Phase 2; not started |

## 5. Risks and constraints noted

* **Free Edition capacity:** at ~20 MB/hour gzipped (~0.5 GB/day raw), months
  of history are storable, but Silver/Gold table growth and serverless DBU
  limits will bound backfill depth. Tracked in `docs/open_questions.md`.
* **Post-2025 payload drift:** the spec warns commit-level summaries may be
  unreliable; resolved empirically in `docs/schema_validation_report.md`.
* **No workspace credentials from this container:** everything Databricks-side
  ships as runnable notebooks/modules; in-workspace execution is the
  verification step.

## 6. Next steps (spec §21 work order)

1. ~~Repository and infrastructure audit~~ — this document.
2. ~~Sample download~~ — downloader + three hours fetched.
3. ~~Schema profiling~~ / 4. ~~Schema validation report~~ — see
   `docs/schema_validation_report.md`.
5. Bronze ingestion (`events_raw`, quarantine, audit) — next increment.
6. Silver normalization, then Gold metrics, then the seasonal-naive
   forecast vertical slice (spec §16).
