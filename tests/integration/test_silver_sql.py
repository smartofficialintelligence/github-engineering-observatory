"""Integration tests: the production SQL path end to end on local Delta.

Bronze ``ingest_file`` and Silver ``build_silver`` run exactly as they
do on Databricks — same MERGE statements, same three-part table names
(retargeted at ``spark_catalog`` by conftest), ANSI mode on. Assertions
are outcome-based: synthetic events with hand-written expected rows.
"""

from __future__ import annotations

import gzip
import json

from conftest import (
    SYNTHETIC_EVENTS,
    SYNTHETIC_HOUR_FILE,
    bronze_ingest,
    make_event,
    transforms,
)


def one(spark, sql):
    rows = spark.sql(sql).collect()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {sql}"
    return rows[0].asDict()


# -- Bronze through the real MERGE --------------------------------------------


def test_bronze_stats(ingested):
    _, stats, _ = ingested
    assert stats.lines_read == 11
    assert stats.quarantined == 1
    assert stats.within_file_duplicate_ids == 1
    assert stats.rows_staged == 9
    assert stats.rows_inserted == 9  # MERGE metrics surfaced correctly


def test_bronze_quarantine_row(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.bronze.events_quarantine")
    assert row["reason"] == "malformed_json"
    assert row["source_line"] == 4
    assert row["raw_line"].startswith("this is not json")


def test_bronze_integer_id_normalized(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT event_id FROM spark_catalog.bronze.events_raw WHERE event_id = '42'")
    assert row["event_id"] == "42"


# -- Silver: silver.events -----------------------------------------------------


def test_events_row_parity_and_uniqueness(ingested):
    spark, _, metrics = ingested
    counts = one(
        spark,
        "SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS d "
        f"FROM spark_catalog.silver.events WHERE source_file = '{SYNTHETIC_HOUR_FILE}'",
    )
    assert counts["n"] == 9  # every Bronze row, S6 included
    assert counts["d"] == 9
    assert metrics["spark_catalog.silver.events"]["num_inserted_rows"] == 9


def test_events_typed_extraction(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.events WHERE event_id = '1'")
    assert row["actor_id"] == 1
    assert row["actor_login"] == "alice"
    assert row["actor_is_bot"] is False
    assert row["repo_id"] == 2
    assert row["repo_name"] == "alice/repo"
    assert row["payload_ref"] == "refs/heads/main"
    assert row["push_id"] == 99
    assert row["quality_flag"] is None
    assert row["created_at"].isoformat() == "2026-01-01T00:00:01"  # UTC session tz


def test_events_bot_and_org(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.events WHERE event_id = '2'")
    assert row["actor_is_bot"] is True
    assert row["org_id"] == 77
    assert row["org_login"] == "acme"


def test_events_s6_flagged_not_dropped(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.events WHERE event_id = '7'")
    assert row["public"] is False
    assert row["quality_flag"] == "non_public"
    assert row["repo_id"] is None


def test_events_unknown_type_ingests(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT event_type FROM spark_catalog.silver.events WHERE event_id = '8'")
    assert row["event_type"] == "BrandNewEvent"


# -- Silver: lifecycle tables --------------------------------------------------


def test_pr_events_covers_all_pr_carrying_types(ingested):
    spark, _, _ = ingested
    rows = spark.sql(
        "SELECT event_id, event_type FROM spark_catalog.silver.pr_events "
        f"WHERE source_date = DATE'2026-01-01'"
    ).collect()
    # The PullRequestEvent AND the review event both carry pull_request.
    assert {(r["event_id"], r["event_type"]) for r in rows} == {
        ("3", "PullRequestEvent"),
        ("5", "PullRequestReviewEvent"),
    }


def test_pr_events_merged_action(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.pr_events WHERE event_id = '3'")
    assert row["action"] == "merged"
    assert row["pr_number"] == 7
    assert row["pr_id"] == 555
    assert row["base_ref"] == "main"
    assert row["head_sha"] == "d" * 40


def test_issue_events_pr_partition_flag(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.issue_events WHERE event_id = '4'")
    assert row["issue_number"] == 7
    assert row["is_pull_request"] is True
    assert row["comment_count"] == 4
    assert row["pr_merged_at"].isoformat() == "2026-07-02T10:00:00"


def test_review_events(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.review_events WHERE event_id = '5'")
    assert row["review_id"] == 42
    assert row["review_state"] == "approved"
    assert row["pr_number"] == 7
    assert row["review_commit_sha"] == "e" * 40


def test_release_events_asset_aggregation(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.release_events WHERE event_id = '6'")
    assert row["release_id"] == 10
    assert row["tag_name"] == "v1.0"
    assert row["assets_count"] == 3
    assert row["assets_download_count"] == 12
    assert row["immutable"] is True
    assert row["prerelease"] is False


# -- idempotency and predicates ------------------------------------------------


def test_rebuild_inserts_nothing(ingested):
    spark, _, _ = ingested
    metrics = transforms.build_silver(spark)
    for table, m in metrics.items():
        assert m.get("num_inserted_rows", 0) == 0, f"{table} not idempotent"


def test_where_predicate_restricts_scan(ingested):
    spark, _, _ = ingested
    metrics = transforms.build_silver(spark, where="source_date = DATE'1999-01-01'")
    for table, m in metrics.items():
        assert m.get("num_inserted_rows", 0) == 0


def test_bronze_reingest_idempotent(ingested, tmp_path_factory):
    spark, _, _ = ingested
    # Re-ingest the same events under the same file name.
    raw_dir = tmp_path_factory.mktemp("raw2")
    path = str(raw_dir / SYNTHETIC_HOUR_FILE)
    lines = [json.dumps(e).encode() for e in SYNTHETIC_EVENTS]
    with open(path, "wb") as fh:
        fh.write(gzip.compress(b"\n".join(lines) + b"\n"))
    stats2 = bronze_ingest.ingest_file(spark, path)
    assert stats2.rows_inserted == 0
    assert stats2.duplicates_skipped == stats2.rows_staged


# -- one real GH Archive hour --------------------------------------------------


def test_real_hour_end_to_end(real_hour):
    """160,280 real events through the actual Bronze + Silver SQL."""
    spark, stats = real_hour
    assert stats.lines_read == 160_280
    assert stats.quarantined == 0

    counts = one(
        spark,
        "SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS d, "
        "SUM(CASE WHEN created_at IS NULL AND quality_flag IS NULL THEN 1 ELSE 0 END) AS bad_ts "
        "FROM spark_catalog.silver.events WHERE source_file = '2026-07-29-15.json.gz'",
    )
    assert counts["n"] == 160_280
    assert counts["d"] == 160_280
    assert counts["bad_ts"] == 0

    actions = {
        r["action"]
        for r in spark.sql(
            "SELECT DISTINCT action FROM spark_catalog.silver.pr_events "
            "WHERE source_date = DATE'2026-07-29'"
        ).collect()
    }
    known = {
        "opened", "merged", "closed", "labeled", "unlabeled",
        "assigned", "unassigned", "reopened", "created", "updated", "edited",
        "synchronize", "review_requested", "review_request_removed",
        "ready_for_review", "converted_to_draft", "enqueued", "dequeued",
        "auto_merge_enabled", "auto_merge_disabled", "milestoned", "demilestoned",
    }
    assert actions <= known, f"unexpected PR actions: {actions - known}"
