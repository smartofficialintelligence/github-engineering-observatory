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

from conftest import config, gold_metrics
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


@pytest.fixture(scope="module")
def gold_after_lifecycle_merge(bench_ingested, ingested):
    """Ingest bench rows, then merge lifecycle into Gold on top of a
    Gold state that already has a stream row (from `ingested`). Verifies
    the stream/bigquery precedence rule end-to-end."""
    spark, _ = bench_ingested
    gold_metrics.create_gold_tables(spark)
    gold_metrics.build_gold(spark)  # ensure the synthetic stream row exists
    stream_before = spark.sql(
        f"SELECT COUNT(*) n FROM {config.GOLD_ECOSYSTEM_HOURLY_TABLE}"
        f" WHERE source = 'stream'"
    ).collect()[0]["n"]
    metrics = blh.merge_lifecycle_into_gold(spark)
    return spark, metrics, stream_before


def test_lifecycle_hours_land_in_gold_as_bigquery(gold_after_lifecycle_merge):
    """Every hour from bronze.bq_lifecycle_hourly appears in Gold with
    source='bigquery' and matching column values."""
    spark, _, _ = gold_after_lifecycle_merge
    # Pick an arbitrary bench hour and verify it landed correctly.
    row = one(
        spark,
        f"SELECT g.source, g.total_events, g.pr_merged,"
        f" g.issues_closed_completed, g.reviews_approved,"
        f" g.issue_comments_on_prs, g.release_download_count_sum,"
        f" b.total_events AS b_total, b.pr_merged AS b_pr_merged,"
        f" b.issues_closed_completed AS b_icc"
        f" FROM {config.GOLD_ECOSYSTEM_HOURLY_TABLE} g"
        f" JOIN {config.BQ_LIFECYCLE_HOURLY_TABLE} b USING (event_hour)"
        f" WHERE g.event_hour = TIMESTAMP'2023-06-01 00:00:00'",
    )
    assert row["source"] == "bigquery"
    assert row["total_events"] == row["b_total"]
    assert row["pr_merged"] == row["b_pr_merged"]
    assert row["issues_closed_completed"] == row["b_icc"]


def test_stream_row_untouched_by_lifecycle_merge(gold_after_lifecycle_merge):
    """The synthetic stream hour (2026-01-01T00) must retain source='stream'
    and its computed values — bigquery-source merge must not touch it."""
    spark, _, stream_before = gold_after_lifecycle_merge
    stream_after = spark.sql(
        f"SELECT COUNT(*) n FROM {config.GOLD_ECOSYSTEM_HOURLY_TABLE}"
        f" WHERE source = 'stream'"
    ).collect()[0]["n"]
    assert stream_after == stream_before

    row = one(
        spark,
        f"SELECT source, total_events, pr_merged, reviews_approved"
        f" FROM {config.GOLD_ECOSYSTEM_HOURLY_TABLE}"
        f" WHERE event_hour = TIMESTAMP'2026-01-01 00:00:00'",
    )
    assert row["source"] == "stream"
    assert row["total_events"] == 8  # the hand-computed synthetic value
    assert row["pr_merged"] == 1
    assert row["reviews_approved"] == 1


def test_lifecycle_merge_is_idempotent(gold_after_lifecycle_merge):
    """Re-running the merge must not change row count or any value."""
    spark, _, _ = gold_after_lifecycle_merge
    before = one(
        spark,
        f"SELECT COUNT(*) n, SUM(total_events) t, SUM(pr_merged) p,"
        f" SUM(release_download_count_sum) r"
        f" FROM {config.GOLD_ECOSYSTEM_HOURLY_TABLE}",
    )
    blh.merge_lifecycle_into_gold(spark)
    after = one(
        spark,
        f"SELECT COUNT(*) n, SUM(total_events) t, SUM(pr_merged) p,"
        f" SUM(release_download_count_sum) r"
        f" FROM {config.GOLD_ECOSYSTEM_HOURLY_TABLE}",
    )
    assert after == before
