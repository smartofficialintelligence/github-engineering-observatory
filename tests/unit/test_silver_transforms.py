"""Unit tests for Silver transforms: the pure-python reference
extractors plus structural checks that keep the SQL builders and the
reference implementation aligned. No Spark, no network."""

from __future__ import annotations

import json
import re

import pytest

from github_observatory.silver import transforms as tr


def bronze_row(**overrides):
    """A Bronze events_raw row as bronze_ingest emits it."""
    row = {
        "event_id": "12345678901",
        "event_type": "PushEvent",
        "public": True,
        "created_at": "2026-07-29T15:00:01Z",
        "actor_json": json.dumps({"id": 1, "login": "alice"}),
        "repo_json": json.dumps({"id": 2, "name": "alice/repo"}),
        "org_json": None,
        "payload_json": json.dumps(
            {"push_id": 99, "ref": "refs/heads/main", "before": "a" * 40, "head": "b" * 40}
        ),
        "source_file": "2026-07-29-15.json.gz",
        "source_line": 1,
        "source_hour": "2026-07-29T15:00:00+00:00",
    }
    row.update(overrides)
    return row


# -- silver_event_row ----------------------------------------------------------


def test_event_row_basic_extraction():
    row = tr.silver_event_row(bronze_row())
    assert row["event_id"] == "12345678901"
    assert row["event_type"] == "PushEvent"
    assert row["actor_id"] == 1
    assert row["actor_login"] == "alice"
    assert row["actor_is_bot"] is False
    assert row["repo_id"] == 2
    assert row["repo_name"] == "alice/repo"
    assert row["org_id"] is None
    assert row["payload_action"] is None  # pushes carry no action
    assert row["payload_ref"] == "refs/heads/main"
    assert row["push_id"] == 99
    assert row["quality_flag"] is None


def test_event_row_bot_suffix():
    actor = json.dumps({"id": 3, "login": "github-actions[bot]"})
    row = tr.silver_event_row(bronze_row(actor_json=actor))
    assert row["actor_is_bot"] is True


def test_event_row_bot_suffix_must_be_terminal():
    actor = json.dumps({"id": 3, "login": "not[bot]really"})
    assert tr.silver_event_row(bronze_row(actor_json=actor))["actor_is_bot"] is False


def test_event_row_org_extraction():
    org = json.dumps({"id": 77, "login": "acme"})
    row = tr.silver_event_row(bronze_row(org_json=org))
    assert row["org_id"] == 77
    assert row["org_login"] == "acme"


def test_event_row_action_extraction():
    payload = json.dumps({"action": "opened"})
    row = tr.silver_event_row(bronze_row(event_type="IssuesEvent", payload_json=payload))
    assert row["payload_action"] == "opened"
    assert row["push_id"] is None


def test_quality_flag_non_public_wins():
    """Finding S6 shape: public=false AND empty repo — non_public wins."""
    row = tr.silver_event_row(bronze_row(public=False, repo_json="{}"))
    assert row["quality_flag"] == tr.FLAG_NON_PUBLIC


def test_quality_flag_missing_repo_id():
    row = tr.silver_event_row(bronze_row(repo_json="{}"))
    assert row["quality_flag"] == tr.FLAG_MISSING_REPO_ID
    assert row["repo_id"] is None


def test_quality_flag_invalid_created_at():
    row = tr.silver_event_row(bronze_row(created_at=None))
    assert row["quality_flag"] == tr.FLAG_INVALID_CREATED_AT


def test_quality_flag_clean_is_none():
    assert tr.silver_event_row(bronze_row())["quality_flag"] is None


def test_event_row_malformed_json_columns_null_not_raise():
    row = tr.silver_event_row(
        bronze_row(actor_json="not json", repo_json=None, payload_json="[1,2]")
    )
    assert row["actor_id"] is None
    assert row["actor_is_bot"] is False
    assert row["repo_id"] is None
    assert row["payload_action"] is None
    assert row["quality_flag"] == tr.FLAG_MISSING_REPO_ID


# -- lifecycle extractors ------------------------------------------------------


def pr_payload(**overrides):
    payload = {
        "action": "opened",
        "number": 7,
        "pull_request": {
            "id": 555,
            "number": 7,
            "base": {"ref": "main", "sha": "c" * 40, "repo": {"id": 2}},
            "head": {"ref": "feat", "sha": "d" * 40, "repo": {"id": 2}},
        },
    }
    payload.update(overrides)
    return json.dumps(payload)


def test_pr_event_row():
    row = tr.silver_pr_event_row(
        bronze_row(event_type="PullRequestEvent", payload_json=pr_payload())
    )
    assert row["pr_number"] == 7
    assert row["pr_id"] == 555
    assert row["action"] == "opened"
    assert row["base_ref"] == "main"
    assert row["head_ref"] == "feat"
    assert row["actor_login"] == "alice"


def test_pr_event_row_merged_action():
    """Finding S2: 'merged' is the primary merge signal."""
    row = tr.silver_pr_event_row(
        bronze_row(event_type="PullRequestEvent", payload_json=pr_payload(action="merged"))
    )
    assert row["action"] == "merged"


@pytest.mark.parametrize("etype", tr.PR_EVENT_TYPES)
def test_pr_event_row_accepts_all_pr_types(etype):
    assert tr.silver_pr_event_row(bronze_row(event_type=etype, payload_json=pr_payload()))


def test_pr_event_row_rejects_non_pr_types():
    assert tr.silver_pr_event_row(bronze_row(event_type="PushEvent")) is None


def test_pr_event_row_requires_number():
    payload = json.dumps({"action": "opened", "pull_request": {"id": 555}})
    assert (
        tr.silver_pr_event_row(bronze_row(event_type="PullRequestEvent", payload_json=payload))
        is None
    )


def issue_payload(**issue_overrides):
    issue = {
        "id": 900,
        "number": 3,
        "state": "open",
        "state_reason": None,
        "created_at": "2026-07-01T00:00:00Z",
        "closed_at": None,
        "comments": 4,
    }
    issue.update(issue_overrides)
    return json.dumps({"action": "opened", "issue": issue})


def test_issue_event_row():
    row = tr.silver_issue_event_row(
        bronze_row(event_type="IssuesEvent", payload_json=issue_payload())
    )
    assert row["issue_number"] == 3
    assert row["issue_id"] == 900
    assert row["issue_state"] == "open"
    assert row["is_pull_request"] is False
    assert row["comment_count"] == 4
    assert row["pr_merged_at"] is None


def test_issue_event_row_pr_partition_flag():
    """Issues and PRs share a number space — the flag must gate joins."""
    row = tr.silver_issue_event_row(
        bronze_row(
            event_type="IssueCommentEvent",
            payload_json=issue_payload(pull_request={"merged_at": "2026-07-02T10:00:00Z"}),
        )
    )
    assert row["is_pull_request"] is True
    assert row["pr_merged_at"] == "2026-07-02T10:00:00Z"


def test_issue_event_row_state_reason():
    row = tr.silver_issue_event_row(
        bronze_row(
            event_type="IssuesEvent",
            payload_json=issue_payload(state="closed", state_reason="not_planned"),
        )
    )
    assert row["issue_state_reason"] == "not_planned"


def test_review_event_row():
    payload = json.dumps(
        {
            "action": "created",
            "pull_request": {"id": 555, "number": 7},
            "review": {
                "id": 42,
                "state": "approved",
                "submitted_at": "2026-07-29T15:00:05Z",
                "commit_id": "e" * 40,
            },
        }
    )
    row = tr.silver_review_event_row(
        bronze_row(event_type="PullRequestReviewEvent", payload_json=payload)
    )
    assert row["review_id"] == 42
    assert row["review_state"] == "approved"
    assert row["pr_number"] == 7
    assert row["review_commit_sha"] == "e" * 40


def test_release_event_row_asset_aggregation():
    payload = json.dumps(
        {
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
                    {"no_count": True},
                ],
            },
        }
    )
    row = tr.silver_release_event_row(
        bronze_row(event_type="ReleaseEvent", payload_json=payload)
    )
    assert row["release_id"] == 10
    assert row["tag_name"] == "v1.0"
    assert row["assets_count"] == 3
    assert row["assets_download_count"] == 12
    assert row["immutable"] is True


def test_release_event_row_no_assets():
    payload = json.dumps({"action": "published", "release": {"id": 10}})
    row = tr.silver_release_event_row(
        bronze_row(event_type="ReleaseEvent", payload_json=payload)
    )
    assert row["assets_count"] == 0
    assert row["assets_download_count"] == 0


# -- SQL/reference alignment ---------------------------------------------------


def _ddl_columns(ddl: str) -> list[str]:
    return [chunk.strip().split()[0] for chunk in ddl.split(",")]


def _select_aliases(sql: str) -> set[str]:
    """Output column names of the USING(...) SELECT in a merge statement."""
    select = sql.split("USING (", 1)[1].split("\nFROM ", 1)[0]
    aliases = set(re.findall(r"AS (\w+)", select))
    # bare columns selected without alias (may share a line with aliased ones)
    for line in select.splitlines():
        for part in line.strip().split(", "):
            bare = part.strip().rstrip(",")
            if re.fullmatch(r"[a-z_]+", bare) and bare not in ("", "s", "t"):
                aliases.add(bare)
    return aliases


@pytest.mark.parametrize(
    "ddl,sql_builder",
    [
        (tr.EVENTS_DDL, tr.events_merge_sql),
        (tr.PR_EVENTS_DDL, tr.pr_events_merge_sql),
        (tr.ISSUE_EVENTS_DDL, tr.issue_events_merge_sql),
        (tr.REVIEW_EVENTS_DDL, tr.review_events_merge_sql),
        (tr.RELEASE_EVENTS_DDL, tr.release_events_merge_sql),
    ],
)
def test_merge_select_produces_every_ddl_column(ddl, sql_builder):
    sql = sql_builder("testrun")
    aliases = _select_aliases(sql)
    for column in _ddl_columns(ddl):
        assert column in aliases, f"{column} missing from merge SELECT"


@pytest.mark.parametrize(
    "ddl,extractor,row_builder",
    [
        (
            tr.PR_EVENTS_DDL,
            tr.silver_pr_event_row,
            lambda: bronze_row(event_type="PullRequestEvent", payload_json=pr_payload()),
        ),
        (
            tr.ISSUE_EVENTS_DDL,
            tr.silver_issue_event_row,
            lambda: bronze_row(event_type="IssuesEvent", payload_json=issue_payload()),
        ),
    ],
)
def test_reference_rows_only_contain_ddl_columns(ddl, extractor, row_builder):
    """Reference rows are a subset of the DDL (run/lineage columns are
    added SQL-side only)."""
    row = extractor(row_builder())
    assert set(row) <= set(_ddl_columns(ddl))


def test_events_reference_row_only_contains_ddl_columns():
    row = tr.silver_event_row(bronze_row())
    assert set(row) <= set(_ddl_columns(tr.EVENTS_DDL))


def test_merge_sql_uses_event_id_key_and_insert_only():
    for builder in (
        tr.events_merge_sql,
        tr.pr_events_merge_sql,
        tr.issue_events_merge_sql,
        tr.review_events_merge_sql,
        tr.release_events_merge_sql,
    ):
        sql = builder("r1", where="source_date = DATE'2026-07-29'")
        assert "ON t.event_id = s.event_id" in sql
        assert "WHEN NOT MATCHED THEN INSERT *" in sql
        assert "WHEN MATCHED" not in sql
        assert "source_date = DATE'2026-07-29'" in sql
        assert "'r1' AS silver_run_id" in sql


# -- reference_counts ----------------------------------------------------------


def test_reference_counts_aggregation():
    rows = [
        bronze_row(event_id="1"),
        bronze_row(
            event_id="2",
            event_type="PullRequestEvent",
            payload_json=pr_payload(action="merged"),
            actor_json=json.dumps({"id": 9, "login": "dep[bot]"}),
        ),
        bronze_row(event_id="3", event_type="IssuesEvent", payload_json=issue_payload()),
        bronze_row(event_id="4", public=False, repo_json="{}"),
    ]
    counts = tr.reference_counts(rows)
    assert counts["events"] == 4
    assert counts["bot_events"] == 1
    assert counts["quality_flags"] == {tr.FLAG_NON_PUBLIC: 1}
    assert counts["pr_events"] == 1
    assert counts["issue_events"] == 1
    assert counts["review_events"] == 0
    assert counts["release_events"] == 0
    assert counts["actions"]["PullRequestEvent:merged"] == 1
