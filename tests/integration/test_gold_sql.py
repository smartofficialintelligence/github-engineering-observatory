"""Integration tests: gold.ecosystem_hourly + velocity view on local Delta.

The synthetic hour has a fully hand-computed expected Gold row; the real
hour is checked for wiring consistency against direct Silver
aggregation (same definitions, independently written SQL).
"""

from __future__ import annotations

import pytest

from conftest import gold_metrics


@pytest.fixture(scope="session")
def gold_built(ingested):
    spark, _, _ = ingested
    gold_metrics.create_gold_tables(spark)
    metrics = gold_metrics.build_gold(spark)
    return spark, metrics


def one(spark, sql):
    rows = spark.sql(sql).collect()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {sql}"
    return rows[0].asDict()


def test_synthetic_hour_fully_hand_computed(gold_built):
    """Every column of the synthetic hour's Gold row, derived by hand.

    Clean events (S6 fork excluded): pushes 1, 2, 42 (one by a bot),
    PR merged, issue comment, review, release published, BrandNewEvent.
    """
    spark, _ = gold_built
    row = one(
        spark,
        "SELECT * FROM spark_catalog.gold.ecosystem_hourly "
        "WHERE event_hour = TIMESTAMP'2026-01-01 00:00:00'",
    )
    assert row["total_events"] == 8  # 9 rows minus the flagged S6 fork
    assert row["total_events_human"] == 7  # minus github-actions[bot]
    assert row["push_events"] == 3  # ids 1, 2, 42
    assert row["push_events_human"] == 2  # id 2 is the bot push
    # production: 3 pushes + PR merged + release published; comment,
    # review, and BrandNewEvent are excluded by the whitelist.
    assert row["production_events"] == 5
    assert row["pr_opened"] == 0
    assert row["pr_merged"] == 1
    assert row["pr_closed_no_merge"] == 0
    assert row["pr_reopened"] == 0
    assert row["issues_opened"] == 0
    assert row["issues_closed"] == 0
    assert row["issues_reopened"] == 0
    assert row["releases_published"] == 1
    assert row["bot_events"] == 1
    assert row["distinct_actors"] == 2  # alice (id 1), bot (id 9)
    assert row["distinct_actors_human"] == 1
    assert row["distinct_repos"] == 1  # everything on alice/repo (id 2)
    assert row["distinct_push_repos"] == 1
    # v4 columns (all hand-derived from the synthetic events)
    assert row["pr_events_total"] == 1  # event 3
    assert row["issues_events_total"] == 0
    assert row["issues_closed_completed"] == 0
    assert row["issues_closed_not_planned"] == 0
    assert row["issues_closed_duplicate"] == 0
    assert row["issues_closed_unknown"] == 0
    assert row["reviews_approved"] == 1  # event 5
    assert row["reviews_changes_requested"] == 0
    assert row["reviews_commented"] == 0
    assert row["reviews_dismissed"] == 0
    assert row["issue_comments_true"] == 0
    # event 4 is an IssueCommentEvent with issue.pull_request set
    assert row["issue_comments_on_prs"] == 1
    assert row["pr_review_comments"] == 0
    assert row["commit_comments"] == 0
    # event 6 has assets with download_count 5, 7, and NULL → sum 12
    assert row["release_download_count_sum"] == 12
    assert row["metric_version"] == gold_metrics.METRIC_DEFINITIONS_VERSION
    assert row["metric_version"] == 5  # explicit anchor: this test is a v5 test
    # v5 comparability context. The synthetic hour is 2026-01-01, which
    # falls in merge_restored and in no outage window.
    assert row["era"] == "merge_restored"
    assert row["era_ordinal"] == 3
    assert row["in_outage"] is False


def test_gold_upsert_is_idempotent(gold_built):
    spark, _ = gold_built
    before = one(
        spark,
        "SELECT COUNT(*) AS n, SUM(total_events) AS s FROM spark_catalog.gold.ecosystem_hourly",
    )
    metrics = gold_metrics.build_gold(spark)
    after = one(
        spark,
        "SELECT COUNT(*) AS n, SUM(total_events) AS s FROM spark_catalog.gold.ecosystem_hourly",
    )
    assert after == before
    # matched hours are recomputed (updated), none inserted
    assert metrics.get("num_inserted_rows", 0) == 0


def test_gold_where_predicate_restricts_scan(gold_built):
    spark, _ = gold_built
    metrics = gold_metrics.build_gold(spark, where="source_date = DATE'1999-01-01'")
    assert metrics.get("num_inserted_rows", 0) == 0
    assert metrics.get("num_updated_rows", 0) == 0


def test_velocity_view_gaps_yield_null(gold_built):
    """No adjacent/24h/168h neighbour hours exist in the test data —
    every acceleration column must be NULL, never a wrong-lag value."""
    spark, _ = gold_built
    row = one(
        spark,
        "SELECT * FROM spark_catalog.gold.ecosystem_velocity "
        "WHERE event_hour = TIMESTAMP'2026-01-01 00:00:00'",
    )
    assert row["total_events"] == 8  # levels pass through
    for col, value in row.items():
        if "_delta_" in col or "_pct_" in col:
            assert value is None, f"{col} should be NULL across a gap, got {value}"


def test_real_hour_matches_direct_silver_aggregation(real_hour, gold_built):
    """Gold row for the real hour == independently written aggregation
    over Silver (wiring check on 160k rows)."""
    spark, _ = real_hour
    gold_metrics.build_gold(spark)  # ensure the real hour is (re)built

    gold = one(
        spark,
        "SELECT total_events, push_events, pr_merged, bot_events, "
        "distinct_actors, distinct_repos "
        "FROM spark_catalog.gold.ecosystem_hourly "
        "WHERE event_hour = TIMESTAMP'2026-07-29 15:00:00'",
    )
    direct = one(
        spark,
        """
        SELECT COUNT(*) AS total_events,
               SUM(CASE WHEN event_type = 'PushEvent' THEN 1 ELSE 0 END) AS push_events,
               SUM(CASE WHEN event_type = 'PullRequestEvent'
                         AND payload_action = 'merged' THEN 1 ELSE 0 END) AS pr_merged,
               SUM(CASE WHEN actor_is_bot THEN 1 ELSE 0 END) AS bot_events,
               COUNT(DISTINCT actor_id) AS distinct_actors,
               COUNT(DISTINCT repo_id) AS distinct_repos
        FROM spark_catalog.silver.events
        WHERE quality_flag IS NULL AND created_at IS NOT NULL
          AND date_trunc('HOUR', created_at) = TIMESTAMP'2026-07-29 15:00:00'
        """,
    )
    assert gold == direct
    assert gold["total_events"] > 150_000  # sanity: the real hour, not synthetic
