"""Unit tests for Gold metrics: structural checks on the SQL builders —
fast, no Spark. Semantic correctness runs in ``tests/integration``."""

from __future__ import annotations

import re

from github_observatory.gold import metrics as gm


def _ddl_columns(ddl: str) -> list[str]:
    return [chunk.strip().split()[0] for chunk in ddl.split(",")]


def test_merge_select_produces_every_ddl_column():
    sql = gm.ecosystem_hourly_merge_sql("testrun")
    select = sql.split("USING (", 1)[1].split("\nFROM ", 1)[0]
    aliases = set(re.findall(r"AS (\w+)", select))
    for column in _ddl_columns(gm.ECOSYSTEM_HOURLY_DDL):
        assert column in aliases, f"{column} missing from merge SELECT"


def test_merge_upserts_on_event_hour():
    sql = gm.ecosystem_hourly_merge_sql("r1")
    assert "ON t.event_hour = s.event_hour" in sql
    assert "WHEN MATCHED THEN UPDATE SET *" in sql
    assert "WHEN NOT MATCHED THEN INSERT *" in sql


def test_merge_applies_quality_filter_and_where():
    sql = gm.ecosystem_hourly_merge_sql("r1", where="source_date = DATE'2026-07-29'")
    assert "quality_flag IS NULL" in sql
    assert "created_at IS NOT NULL" in sql
    assert "source_date = DATE'2026-07-29'" in sql
    assert "'r1' AS gold_run_id" in sql
    assert f"{gm.METRIC_DEFINITIONS_VERSION} AS metric_version" in sql


def test_production_whitelist_encoded_in_sql():
    expr = gm.production_event_sql()
    assert "event_type = 'PushEvent'" in expr
    assert "'PullRequestEvent' AND payload_action IN ('opened', 'merged', 'closed', 'reopened')" in expr
    assert "'IssuesEvent' AND payload_action IN ('opened', 'closed', 'reopened')" in expr
    assert "'ReleaseEvent' AND payload_action IN ('published')" in expr
    # excluded classes must not slip into the whitelist
    for excluded in ("IssueCommentEvent", "WatchEvent", "ForkEvent",
                     "CreateEvent", "labeled", "assigned"):
        assert excluded not in expr


def test_velocity_view_offsets_and_gap_safety():
    sql = gm.ecosystem_velocity_view_sql()
    assert sql.startswith(f"CREATE OR REPLACE VIEW {gm.GOLD_ECOSYSTEM_VELOCITY_VIEW}")
    for hours, label in ((1, "1h"), (24, "24h"), (168, "168h")):
        # exact-offset joins, not window lag: gaps become NULL
        assert f"INTERVAL {hours} HOURS" in sql
        for metric in ("total_events", "push_events", "production_events"):
            assert f"{metric}_delta_{label}" in sql
            assert f"{metric}_pct_{label}" in sql
    assert "try_divide" in sql  # no divide-by-zero failures under ANSI
    assert "lag(" not in sql.lower()
