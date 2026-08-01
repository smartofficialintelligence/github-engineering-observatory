# GCP Lifecycle Backfill

Payload-derived hourly aggregates for the full GH Archive decade (2016–2025),
produced by running Dataproc Serverless Spark against
`githubarchive.year.YYYY` and writing partitioned Parquet to GCS.

Full spec: [`docs/lifecycle_metrics_spec.md`](../../docs/lifecycle_metrics_spec.md).

## Prerequisites (one-time)

Verify with `gcloud auth list` and `gcloud config get project`:

* GCP project (`sematryx-481510` for this repo)
* `gcloud auth login` completed on the executing machine
* Dataproc Serverless API enabled: `gcloud services enable dataproc.googleapis.com`
* GCS bucket in the same region as Dataproc (e.g. `us-central1`)
  * Used for pyfile staging + Parquet output
* BQ scratch dataset in the project for the Spark-BigQuery connector's
  temp views (default name `observatory_scratch`; create empty)
* Compute default service account has:
  * `roles/dataproc.worker`
  * `roles/storage.objectAdmin` on the bucket
  * `roles/bigquery.dataViewer` on `githubarchive.*`
  * `roles/bigquery.jobUser` in the project

## Bench (spec §Motivation step 2)

One month, to measure cost and verify correctness before committing to the
full backfill.

```bash
python pipelines/gcp_lifecycle_backfill/submit.py bench \
    --project sematryx-481510 \
    --region us-central1 \
    --bucket <your-bucket> \
    --year 2023 --month 6
```

Output goes to `gs://<bucket>/lifecycle_out/year=2023/month=06/`.
Batch summary (DCU-seconds, elapsed time) is appended to
`gs://<bucket>/lifecycle_out/_manifest.jsonl` and printed to stdout.

## Validate the bench (spec §Validation contract, check #6)

Runs the equivalent aggregation in BigQuery SQL and diffs against the
Spark output row-by-row, column-by-column. Zero diff is the acceptance
criterion.

```bash
python pipelines/gcp_lifecycle_backfill/validate.py \
    --project sematryx-481510 \
    --year 2023 --month 6 \
    --spark-gs gs://<your-bucket>/lifecycle_out/year=2023/month=06
```

Exit 0 on `PASS`, non-zero on any per-column diff or hour-coverage gap.
The BQ validation query bills ~$1.60 per month of `githubarchive.year.*`
scanned (payload column dominant).

## Backfill (spec §Phase C — only after bench passes)

One batch per year (parallel-safe; failures re-runnable per-year):

```bash
python pipelines/gcp_lifecycle_backfill/submit.py backfill \
    --project sematryx-481510 \
    --region us-central1 \
    --bucket <your-bucket> \
    --years 2016-2025
```

## Cost expectations (revise from bench)

Placeholder — fill in from actual bench run. Draft assumptions until then:

| Component | Free tier | List price (2026-08) | Notes |
| --- | --- | --- | --- |
| BigQuery Storage Read API | 300 TiB/month | $1.10/TiB above | ~30 TB decade read fits in free tier |
| Dataproc Serverless standard | none | $0.06/DCU-hour | primary cost driver |
| GCS storage (Parquet output) | 5 GB/month | $0.020/GB/month | output is ~100 MB/decade — negligible |
| GCS egress to Databricks | 1 GB/month | $0.02–$0.12/GB | output ~100 MB — negligible |

Bench extrapolation: multiply the bench month's DCU-hours by 120 (10 years).
Add BQ validation cost (~$20 if run per-year during bench, negligible if
bench is one-month-only).

## Troubleshooting

* **`BATCH FAILED` with connector error** — bump `BQ_CONNECTOR_JAR` in
  `submit.py` to the latest release listed at
  https://github.com/GoogleCloudDataproc/spark-bigquery-connector/releases
  and re-submit.
* **`Access denied` on githubarchive** — the compute service account needs
  `bigquery.dataViewer` on the public dataset (it is public but still
  requires that role).
* **Materialization dataset errors** — the Spark-BigQuery connector needs
  a scratch BQ dataset in your project for view materialization; create
  `observatory_scratch` (or pass `--materialization-dataset=<name>`) with
  a short table expiration (`bq mk --dataset --default_table_expiration=86400 <project>:observatory_scratch`).
* **Hour count differs from 24 × days-in-month** — an underlying archive
  hour is missing. Check `open_questions.md` OQ-1 update — decade coverage
  was 100% at last check.

## Landing pass-2 output in Databricks (after bench passes)

Mirror pass-1's local-artifacts-then-upload pattern (see
`notebooks/09_bq_history_import.py`):

```bash
# 1. Download the year(s) you care about from GCS to local artifacts
mkdir -p artifacts/bq_lifecycle
gcloud storage cp -r 'gs://<your-bucket>/lifecycle_out/year=*' artifacts/bq_lifecycle/

# 2. Upload artifacts/bq_lifecycle/ to the Databricks raw Volume
python scripts/databricks_run.py upload artifacts/bq_lifecycle \
    --dest /Volumes/github_observatory/bronze/raw_files/bq_lifecycle

# 3. Run notebook 10 on serverless to ingest into bronze.bq_lifecycle_hourly
python scripts/databricks_run.py run-notebook \
    /Repos/<you>/github-engineering-observatory/notebooks/10_bq_lifecycle_import
```

Notebook 10 asserts the spec's row-level invariants (whitelist sum and
closure-taxonomy sum) as hard failures — it will not complete if any hour
in the ingested Parquet fails.

Gold-side propagation is intentionally deferred: `gold.ecosystem_hourly`
currently carries 9 of the 34 payload-derived columns. The 15 new columns
need to be added to Gold's DDL and to the stream Silver→Gold pipeline
before backfilled rows can be merged into Gold cleanly (spec §Follow-up).
Until then, query `bronze.bq_lifecycle_hourly` directly for the full
34-column view.

## Local dry-run (no GCP)

The Spark job accepts `--local --local-json <path>` to read a JSONL sample
instead of BigQuery. Used for CI-style logic tests against the three
committed sample hours in `raw_files/`.

Integration tests in `tests/integration/test_bq_lifecycle_history_sql.py`
run the ingestion module against a bench Parquet output using local OSS
Spark + Delta (see `LIFECYCLE_BENCH_PARQUET` env var); they skip cleanly
when no bench artifact is on disk.
