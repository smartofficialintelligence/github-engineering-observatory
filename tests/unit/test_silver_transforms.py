"""Unit tests for Silver transforms: structural checks on the SQL
builders — fast, no Spark. Semantic correctness of the SQL is covered by
``tests/integration``, which executes the real MERGEs on local Delta."""

from __future__ import annotations

import re

import pytest

from github_observatory.silver import transforms as tr


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


ALL_BUILDERS = [
    (tr.EVENTS_DDL, tr.events_merge_sql),
    (tr.PR_EVENTS_DDL, tr.pr_events_merge_sql),
    (tr.ISSUE_EVENTS_DDL, tr.issue_events_merge_sql),
    (tr.REVIEW_EVENTS_DDL, tr.review_events_merge_sql),
    (tr.RELEASE_EVENTS_DDL, tr.release_events_merge_sql),
]


@pytest.mark.parametrize("ddl,sql_builder", ALL_BUILDERS)
def test_merge_select_produces_every_ddl_column(ddl, sql_builder):
    sql = sql_builder("testrun")
    aliases = _select_aliases(sql)
    for column in _ddl_columns(ddl):
        assert column in aliases, f"{column} missing from merge SELECT"


@pytest.mark.parametrize("ddl,sql_builder", ALL_BUILDERS)
def test_merge_sql_uses_event_id_key_and_insert_only(ddl, sql_builder):
    sql = sql_builder("r1", where="source_date = DATE'2026-07-29'")
    assert "ON t.event_id = s.event_id" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "WHEN MATCHED" not in sql
    assert "source_date = DATE'2026-07-29'" in sql
    assert "'r1' AS silver_run_id" in sql


@pytest.mark.parametrize("ddl,sql_builder", ALL_BUILDERS)
def test_merge_sql_uses_try_cast_only(ddl, sql_builder):
    """ANSI mode is on in serverless; a plain CAST of a malformed value
    would fail the whole MERGE instead of yielding NULL."""
    sql = sql_builder("r1")
    assert not re.search(r"(?<!TRY_)CAST\(get_json_object", sql), (
        "extractions must use TRY_CAST"
    )
    assert not re.search(r"(?<!TRY_)CAST\(created_at", sql)


def test_lifecycle_builders_filter_event_types():
    sql = tr.pr_events_merge_sql("r")
    for etype in tr.PR_EVENT_TYPES:
        assert f"'{etype}'" in sql
    sql = tr.issue_events_merge_sql("r")
    for etype in tr.ISSUE_EVENT_TYPES:
        assert f"'{etype}'" in sql
    assert "'PullRequestReviewEvent'" in tr.review_events_merge_sql("r")
    assert "'ReleaseEvent'" in tr.release_events_merge_sql("r")


def test_quality_flag_priority_order_in_sql():
    """non_public must be checked before missing_repo_id (Finding S6 rows
    have both conditions true)."""
    sql = tr.events_merge_sql("r")
    assert sql.index(tr.FLAG_NON_PUBLIC) < sql.index(tr.FLAG_MISSING_REPO_ID)
    assert sql.index(tr.FLAG_MISSING_REPO_ID) < sql.index(tr.FLAG_INVALID_CREATED_AT)


def test_bot_suffix_constant_used_in_sql():
    assert tr.BOT_LOGIN_SUFFIX == "[bot]"
    assert "[bot]" in tr.events_merge_sql("r")
