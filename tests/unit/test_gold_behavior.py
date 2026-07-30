"""Unit tests: structural checks on behavior + sustainability SQL
builders. Semantics run in ``tests/integration``."""

from __future__ import annotations

import re

import pytest

from github_observatory.gold import behavior as gb
from github_observatory.gold import sustainability as gs


def _ddl_columns(ddl: str) -> list[str]:
    return [chunk.strip().split()[0] for chunk in ddl.split(",")]


def _final_select_aliases(sql: str) -> set[str]:
    """Column names produced by the last top-level SELECT in the USING
    subquery (builders end with `SELECT ... FROM ...` after CTEs)."""
    using = sql.split("USING (", 1)[1].rsplit(") AS s", 1)[0]
    final = using.rsplit("SELECT", 1)[1].split("\nFROM ", 1)[0]
    aliases = set(re.findall(r"AS (\w+)", final))
    for line in final.splitlines():
        for part in line.strip().split(", "):
            bare = part.strip().rstrip(",")
            bare = bare.split(".")[-1]  # d.event_date -> event_date
            if re.fullmatch(r"[a-z_]+", bare) and bare not in ("", "s", "t"):
                aliases.add(bare)
    return aliases


ALL_BUILDERS = [
    (gb.FLOW_DAILY_DDL, gb.flow_daily_merge_sql, "event_date"),
    (gb.CONTRIBUTION_DAILY_DDL, gb.contribution_daily_merge_sql, "event_date"),
    (gb.ENGAGEMENT_DAILY_DDL, gb.engagement_daily_merge_sql, "event_date"),
    (gb.DATA_QUALITY_DDL, gb.data_quality_merge_sql, "source_file"),
    (gs.RETENTION_DAILY_DDL, gs.retention_daily_merge_sql, "event_date"),
    (gs.NETWORK_DAILY_DDL, gs.network_daily_merge_sql, "event_date"),
]


@pytest.mark.parametrize("ddl,builder,key", ALL_BUILDERS)
def test_merge_select_produces_every_ddl_column(ddl, builder, key):
    aliases = _final_select_aliases(builder("testrun"))
    for column in _ddl_columns(ddl):
        assert column in aliases, f"{column} missing from merge SELECT"


@pytest.mark.parametrize("ddl,builder,key", ALL_BUILDERS)
def test_merge_upserts_on_key(ddl, builder, key):
    sql = builder("r1")
    assert f"ON t.{key} = s.{key}" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_actor_first_seen_only_moves_earlier():
    sql = gb.actor_first_seen_merge_sql("r1")
    assert "WHEN MATCHED AND s.first_seen_at < t.first_seen_at THEN UPDATE SET *" in sql


@pytest.mark.parametrize(
    "builder",
    [gb.contribution_daily_merge_sql, gb.engagement_daily_merge_sql,
     gs.network_daily_merge_sql],
)
def test_silver_readers_apply_quality_filter(builder):
    sql = builder("r1")
    assert "quality_flag IS NULL" in sql
    assert "created_at IS NOT NULL" in sql


def test_retention_baselines_span_full_history():
    """The daily_actors CTE must NOT be where-restricted — baseline days
    can lie outside the build window; only output rows are filtered."""
    sql = gs.retention_daily_merge_sql("r1", where="event_date = DATE'2026-07-29'")
    cte = sql.split("daily_actors AS (", 1)[1].split(")", 1)[0]
    assert "2026-07-29" not in cte
    assert sql.rstrip().endswith(
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )
    assert "WHERE event_date = DATE'2026-07-29'" in sql


def test_flow_reads_ecosystem_hourly_not_silver():
    sql = gb.flow_daily_merge_sql("r1")
    assert "ecosystem_hourly" in sql
    assert "silver" not in sql.lower().replace("silver_run", "")


def test_gap_safe_division_everywhere():
    for _, builder, _ in ALL_BUILDERS:
        sql = builder("r1")
        assert "/ 0" not in sql
        if "_share" in sql or "_cv" in sql or "retention" in sql:
            assert "try_divide" in sql, builder.__name__
