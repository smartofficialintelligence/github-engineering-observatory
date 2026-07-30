"""Integration tests: the production SQL path end to end on local Delta.

Bronze ``ingest_file`` and Silver ``build_silver`` run exactly as they
do on Databricks — same MERGE statements, same three-part table names
(retargeted at ``spark_catalog`` by conftest), ANSI mode on. Assertions
are outcome-based: synthetic events with hand-written expected rows.
"""

from __future__ import annotations

import gzip
import json
import os

import pytest

# conftest.py has already set GITHUB_OBSERVATORY_CATALOG and reloaded
# these modules; sys.modules hands back the retargeted versions.
import github_observatory.ingestion.bronze_ingest as bronze_ingest
import github_observatory.silver.transforms as transforms


def make_event(event_id, event_type="PushEvent", **overrides):
    event = {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 2, "name": "alice/repo"},
        "payload": {"push_id": 99, "ref": "refs/heads/main"},
        "public": True,
        "created_at": "2026-07-29T15:00:01Z",
    }
    event.update(overrides)
    return event


SYNTHETIC_EVENTS = [
    make_event("1"),  # plain push
    make_event(
        "2",
        actor={"id": 9, "login": "github-actions[bot]"},
        org={"id": 77, "login": "acme"},
    ),  # bot actor + org
    make_event(
        "3",
        event_type="PullRequestEvent",
        payload={
            "action": "merged",
            "number": 7,
            "pull_request": {
                "id": 555,
                "number": 7,
                "base": {"ref": "main", "sha": "c" * 40},
                "head": {"ref": "feat", "sha": "d" * 40},
            },
        },
    ),
    make_event(
        "4",
        event_type="IssueCommentEvent",
        payload={
            "action": "created",
            "issue": {
                "id": 900,
                "number": 7,  # same number space as the PR — flag must separate
                "state": "open",
                "comments": 4,
                "pull_request": {"merged_at": "2026-07-02T10:00:00Z"},
            },
        },
    ),
    make_event(
        "5",
        event_type="PullRequestReviewEvent",
        payload={
            "action": "created",
            "pull_request": {"id": 555, "number": 7},
            "review": {
                "id": 42,
                "state": "approved",
                "submitted_at": "2026-07-29T15:00:05Z",
                "commit_id": "e" * 40,
            },
        },
    ),
    make_event(
        "6",
        event_type="ReleaseEvent",
        payload={
            "action": "published",
            "release": {
                "id": 10,
                "tag_name": "v1.0",
                "prerelease": False,
                "draft": False,
                "immutable": True,
                "published_at": "2026-07-29T15:00:00Z",
                "assets": [
                    {"download_count": 5},
                    {"download_count": 7},
                    {"other": True},
                ],
            },
        },
    ),
    make_event("7", public=False, repo={}, event_type="ForkEvent"),  # Finding S6
    make_event("8", event_type="BrandNewEvent"),  # unknown type must ingest
    make_event(42),  # integer id, normalized to STRING
]


@pytest.fixture(scope="session")
def ingested(bronze_tables, silver_tables, tmp_path_factory):
    """Write the synthetic hour file, ingest to Bronze, build Silver."""
    spark = bronze_tables
    raw_dir = tmp_path_factory.mktemp("raw")
    path = str(raw_dir / "2026-01-01-0.json.gz")
    lines = [json.dumps(e).encode() for e in SYNTHETIC_EVENTS]
    lines.insert(3, b"this is not json {")  # malformed -> quarantine
    lines.append(json.dumps(make_event("1")).encode())  # in-file duplicate
    with open(path, "wb") as fh:
        fh.write(gzip.compress(b"\n".join(lines) + b"\n"))

    stats = bronze_ingest.ingest_file(spark, path)
    metrics = transforms.build_silver(spark)
    return spark, stats, metrics


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
        "FROM spark_catalog.silver.events",
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
    assert row["created_at"].isoformat() == "2026-07-29T15:00:01"  # UTC session tz


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
    rows = spark.sql("SELECT event_id, event_type FROM spark_catalog.silver.pr_events").collect()
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
    row = one(spark, "SELECT * FROM spark_catalog.silver.issue_events")
    assert row["event_id"] == "4"
    assert row["issue_number"] == 7
    assert row["is_pull_request"] is True
    assert row["comment_count"] == 4
    assert row["pr_merged_at"].isoformat() == "2026-07-02T10:00:00"


def test_review_events(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.review_events")
    assert row["review_id"] == 42
    assert row["review_state"] == "approved"
    assert row["pr_number"] == 7
    assert row["review_commit_sha"] == "e" * 40


def test_release_events_asset_aggregation(ingested):
    spark, _, _ = ingested
    row = one(spark, "SELECT * FROM spark_catalog.silver.release_events")
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
    counts = one(spark, "SELECT COUNT(*) AS n FROM spark_catalog.silver.events")
    assert counts["n"] == 9


def test_where_predicate_restricts_scan(ingested):
    spark, _, _ = ingested
    metrics = transforms.build_silver(spark, where="source_date = DATE'1999-01-01'")
    for table, m in metrics.items():
        assert m.get("num_inserted_rows", 0) == 0


def test_bronze_reingest_idempotent(ingested, tmp_path_factory):
    spark, stats, _ = ingested
    # Re-ingest the same file content under the same name.
    raw_dir = tmp_path_factory.mktemp("raw2")
    path = str(raw_dir / "2026-01-01-0.json.gz")
    lines = [json.dumps(e).encode() for e in SYNTHETIC_EVENTS]
    with open(path, "wb") as fh:
        fh.write(gzip.compress(b"\n".join(lines) + b"\n"))
    stats2 = bronze_ingest.ingest_file(spark, path)
    assert stats2.rows_inserted == 0
    assert stats2.duplicates_skipped == stats2.rows_staged


# -- optional: one real GH Archive hour ---------------------------------------

REAL_HOUR = os.path.join(
    os.path.dirname(__file__), "..", "..", "raw_files", "2026-07-29-15.json.gz"
)


@pytest.mark.skipif(not os.path.exists(REAL_HOUR), reason="sample hour not downloaded")
def test_real_hour_end_to_end(bronze_tables, silver_tables):
    """160,280 real events through the actual Bronze + Silver SQL."""
    spark = bronze_tables
    stats = bronze_ingest.ingest_file(spark, os.path.abspath(REAL_HOUR))
    assert stats.lines_read == 160_280
    assert stats.quarantined == 0
    transforms.build_silver(spark)

    counts = one(
        spark,
        "SELECT COUNT(*) AS n, COUNT(DISTINCT event_id) AS d, "
        "SUM(CASE WHEN quality_flag IS NOT NULL THEN 1 ELSE 0 END) AS flagged, "
        "SUM(CASE WHEN created_at IS NULL AND quality_flag IS NULL THEN 1 ELSE 0 END) AS bad_ts "
        "FROM spark_catalog.silver.events WHERE source_file = '2026-07-29-15.json.gz'",
    )
    assert counts["n"] == 160_280
    assert counts["d"] == 160_280
    assert counts["bad_ts"] == 0

    actions = {
        r["action"]
        for r in spark.sql(
            "SELECT DISTINCT action FROM spark_catalog.silver.pr_events"
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
