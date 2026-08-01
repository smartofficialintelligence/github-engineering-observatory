"""Integration tests: pass-2 lifecycle history ingestion on local Delta.

Loads the real 2023-06 bench Parquet (see
``pipelines/gcp_lifecycle_backfill/README.md``) if present locally,
runs it through ``bq_lifecycle_history.ingest_bronze_from_parquet``,
and asserts the invariants that matter for MERGE correctness:

* every hourly row lands in bronze.bq_lifecycle_hourly
* re-ingest is a no-op on values (idempotent MERGE)
* provenance columns are populated from the extract + at import time
* the closure-taxonomy sum invariant holds row-by-row
* the OQ-7 whitelist sum invariant holds row-by-row

The bench Parquet is not committed (it lives in the session scratchpad).
Tests skip cleanly when it isn't present, so CI without the artifact
still passes.
"""

from __future__ import annotations

import os

import pytest

from conftest import config
import github_observatory.ingestion.bq_lifecycle_history as blh

BENCH_PARQUET_DIR = os.environ.get(
    "LIFECYCLE_BENCH_PARQUET",
    "/tmp/claude-1000/-home-workspace-github-engineering-observatory/"
    "22f7d954-e8cf-46ea-af65-5f758b103f5b/scratchpad/bench_2023_06",
)


@pytest.fixture(scope="module")
def bench_ingested(spark):
    if not os.path.isdir(BENCH_PARQUET_DIR):
        pytest.skip(f"bench parquet not available at {BENCH_PARQUET_DIR}")
    blh.create_bq_lifecycle_tables(spark)
    metrics = blh.ingest_bronze_from_parquet(spark, BENCH_PARQUET_DIR)
    return spark, metrics


def one(spark, sql):
    rows = spark.sql(sql).collect()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {sql}"
    return rows[0].asDict()


def test_row_count_matches_bench(bench_ingested):
    spark, _ = bench_ingested
    row = one(spark, f"SELECT COUNT(*) n FROM {config.BQ_LIFECYCLE_HOURLY_TABLE}")
    # 715 hours = 720 (24 × 30 June days) minus 5 real GH Archive gaps
    assert row["n"] == 715


def test_provenance_populated(bench_ingested):
    spark, _ = bench_ingested
    row = one(
        spark,
        f"SELECT source_year, metric_spec_version, extract_job_id IS NOT NULL AS eid_set,"
        f" extracted_at IS NOT NULL AS eat_set, import_run_id IS NOT NULL AS irid_set,"
        f" imported_at IS NOT NULL AS iat_set"
        f" FROM {config.BQ_LIFECYCLE_HOURLY_TABLE} LIMIT 1",
    )
    assert row["source_year"] == 2023
    assert row["metric_spec_version"] == blh.METRIC_SPEC_VERSION
    assert row["eid_set"] and row["eat_set"] and row["irid_set"] and row["iat_set"]


def test_closure_taxonomy_sum_invariant(bench_ingested):
    """Spec §Validation check #4: split columns sum to issues_closed."""
    spark, _ = bench_ingested
    bad = one(
        spark,
        f"SELECT COUNT(*) n FROM {config.BQ_LIFECYCLE_HOURLY_TABLE}"
        f" WHERE issues_closed_completed + issues_closed_not_planned"
        f"       + issues_closed_duplicate + issues_closed_unknown"
        f"       != issues_closed",
    )
    assert bad["n"] == 0


def test_whitelist_sum_invariant(bench_ingested):
    """Spec §Validation check #3: production_events = pushes + PR-lifecycle
    + issue-lifecycle + releases."""
    spark, _ = bench_ingested
    bad = one(
        spark,
        f"SELECT COUNT(*) n FROM {config.BQ_LIFECYCLE_HOURLY_TABLE}"
        f" WHERE production_events !="
        f"       push_events"
        f"       + pr_opened + pr_merged + pr_closed_no_merge + pr_reopened"
        f"       + issues_opened + issues_closed + issues_reopened"
        f"       + releases_published",
    )
    assert bad["n"] == 0


def test_reingest_is_idempotent(bench_ingested):
    """Re-ingest must not change value columns; row count identical."""
    spark, _ = bench_ingested
    before = one(
        spark,
        f"SELECT COUNT(*) n, SUM(total_events) s, SUM(pr_merged) p,"
        f" SUM(issues_closed_completed) ic"
        f" FROM {config.BQ_LIFECYCLE_HOURLY_TABLE}",
    )
    blh.ingest_bronze_from_parquet(spark, BENCH_PARQUET_DIR)
    after = one(
        spark,
        f"SELECT COUNT(*) n, SUM(total_events) s, SUM(pr_merged) p,"
        f" SUM(issues_closed_completed) ic"
        f" FROM {config.BQ_LIFECYCLE_HOURLY_TABLE}",
    )
    assert after == before


def test_no_duplicate_event_hours(bench_ingested):
    spark, _ = bench_ingested
    dup = one(
        spark,
        f"SELECT COUNT(*) n FROM ("
        f" SELECT event_hour FROM {config.BQ_LIFECYCLE_HOURLY_TABLE}"
        f" GROUP BY event_hour HAVING COUNT(*) > 1)",
    )
    assert dup["n"] == 0
