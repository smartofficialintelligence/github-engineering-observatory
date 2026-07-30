"""Gold production metrics (spec §21 step 7).

Builds ``gold.ecosystem_hourly`` — one row per UTC hour of clean
Silver events — and the ``gold.ecosystem_velocity`` view (hourly
levels plus acceleration against 1h/24h/168h baselines).

Every definition here is bound by ``docs/metric_definitions.md``
(version 1); change that document before changing this SQL. Key rules:
event-time grain, ``quality_flag IS NULL`` only, bots included except
in ``*_human`` columns, and the OQ-7 v1 production whitelist encoded in
``PRODUCTION_EVENTS``.

The hourly build is a MERGE-upsert keyed on ``event_hour``: matched
hours are recomputed and overwritten (late-arriving data for an hour
changes its aggregates), unmatched hours insert, hours outside the
``where`` window are untouched. The velocity view derives acceleration
by exact-offset self-joins so hour gaps produce NULL, never a
wrong-lag comparison.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from github_observatory.common.config import (
    GOLD_ECOSYSTEM_HOURLY_TABLE,
    GOLD_ECOSYSTEM_VELOCITY_VIEW,
    SILVER_EVENTS_TABLE,
)

logger = logging.getLogger("github_observatory.gold")

# OQ-7 v1 production whitelist (docs/metric_definitions.md §Production).
# Event types mapping to None count unconditionally; otherwise only the
# listed payload_action values count.
PRODUCTION_EVENTS: dict[str, tuple[str, ...] | None] = {
    "PushEvent": None,
    "PullRequestEvent": ("opened", "merged", "closed", "reopened"),
    "IssuesEvent": ("opened", "closed", "reopened"),
    "ReleaseEvent": ("published",),
}

METRIC_DEFINITIONS_VERSION = 1

ECOSYSTEM_HOURLY_DDL = (
    "event_hour TIMESTAMP, "
    "total_events BIGINT, total_events_human BIGINT, "
    "push_events BIGINT, push_events_human BIGINT, "
    "production_events BIGINT, "
    "pr_opened BIGINT, pr_merged BIGINT, pr_closed_no_merge BIGINT, "
    "pr_reopened BIGINT, "
    "issues_opened BIGINT, issues_closed BIGINT, issues_reopened BIGINT, "
    "releases_published BIGINT, "
    "bot_events BIGINT, "
    "distinct_actors BIGINT, distinct_actors_human BIGINT, "
    "distinct_repos BIGINT, distinct_push_repos BIGINT, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)


def production_event_sql() -> str:
    """Boolean SQL expression: is this silver.events row a production event?"""
    clauses = []
    for event_type, actions in PRODUCTION_EVENTS.items():
        if actions is None:
            clauses.append(f"event_type = '{event_type}'")
        else:
            action_list = ", ".join(f"'{a}'" for a in actions)
            clauses.append(
                f"(event_type = '{event_type}' AND payload_action IN ({action_list}))"
            )
    return "(" + " OR ".join(clauses) + ")"


def _count_if(condition: str) -> str:
    return f"COUNT_IF({condition})"


def ecosystem_hourly_merge_sql(run_id: str, where: str = "TRUE") -> str:
    prod = production_event_sql()
    select = f"""
SELECT
    date_trunc('HOUR', created_at) AS event_hour,
    COUNT(*) AS total_events,
    {_count_if("NOT actor_is_bot")} AS total_events_human,
    {_count_if("event_type = 'PushEvent'")} AS push_events,
    {_count_if("event_type = 'PushEvent' AND NOT actor_is_bot")} AS push_events_human,
    {_count_if(prod)} AS production_events,
    {_count_if("event_type = 'PullRequestEvent' AND payload_action = 'opened'")} AS pr_opened,
    {_count_if("event_type = 'PullRequestEvent' AND payload_action = 'merged'")} AS pr_merged,
    {_count_if("event_type = 'PullRequestEvent' AND payload_action = 'closed'")} AS pr_closed_no_merge,
    {_count_if("event_type = 'PullRequestEvent' AND payload_action = 'reopened'")} AS pr_reopened,
    {_count_if("event_type = 'IssuesEvent' AND payload_action = 'opened'")} AS issues_opened,
    {_count_if("event_type = 'IssuesEvent' AND payload_action = 'closed'")} AS issues_closed,
    {_count_if("event_type = 'IssuesEvent' AND payload_action = 'reopened'")} AS issues_reopened,
    {_count_if("event_type = 'ReleaseEvent' AND payload_action = 'published'")} AS releases_published,
    {_count_if("actor_is_bot")} AS bot_events,
    COUNT(DISTINCT actor_id) AS distinct_actors,
    COUNT(DISTINCT CASE WHEN NOT actor_is_bot THEN actor_id END) AS distinct_actors_human,
    COUNT(DISTINCT repo_id) AS distinct_repos,
    COUNT(DISTINCT CASE WHEN event_type = 'PushEvent' THEN repo_id END) AS distinct_push_repos,
    {METRIC_DEFINITIONS_VERSION} AS metric_version,
    '{run_id}' AS gold_run_id,
    current_timestamp() AS gold_built_at
FROM {SILVER_EVENTS_TABLE}
WHERE quality_flag IS NULL
  AND created_at IS NOT NULL
  AND ({where})
GROUP BY date_trunc('HOUR', created_at)"""
    return (
        f"MERGE INTO {GOLD_ECOSYSTEM_HOURLY_TABLE} AS t\n"
        f"USING (\n{select}\n) AS s\n"
        "ON t.event_hour = s.event_hour\n"
        "WHEN MATCHED THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


_VELOCITY_METRICS = ("total_events", "push_events", "production_events")
_VELOCITY_OFFSETS = ((1, "1h"), (24, "24h"), (168, "168h"))


def ecosystem_velocity_view_sql() -> str:
    cols = ["c.event_hour"]
    cols += [f"c.{m}" for m in _VELOCITY_METRICS]
    joins = []
    for hours, label in _VELOCITY_OFFSETS:
        alias = f"p{label}"
        joins.append(
            f"LEFT JOIN {GOLD_ECOSYSTEM_HOURLY_TABLE} {alias} "
            f"ON {alias}.event_hour = c.event_hour - INTERVAL {hours} HOURS"
        )
        for metric in _VELOCITY_METRICS:
            cols.append(
                f"c.{metric} - {alias}.{metric} AS {metric}_delta_{label}"
            )
            cols.append(
                f"try_divide(CAST(c.{metric} - {alias}.{metric} AS DOUBLE), "
                f"{alias}.{metric}) AS {metric}_pct_{label}"
            )
    columns = ",\n    ".join(cols)
    join_sql = "\n".join(joins)
    return (
        f"CREATE OR REPLACE VIEW {GOLD_ECOSYSTEM_VELOCITY_VIEW} AS\n"
        f"SELECT\n    {columns}\n"
        f"FROM {GOLD_ECOSYSTEM_HOURLY_TABLE} c\n"
        f"{join_sql}"
    )


def create_gold_tables(spark: Any) -> None:
    """Create the Gold table and (re)create the velocity view."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_ECOSYSTEM_HOURLY_TABLE} "
        f"({ECOSYSTEM_HOURLY_DDL}) USING DELTA"
    )
    spark.sql(ecosystem_velocity_view_sql())


def build_gold(spark: Any, *, where: str = "TRUE") -> dict[str, int]:
    """Upsert gold.ecosystem_hourly (idempotent per window). ``where``
    restricts the Silver scan, e.g. ``"source_date = DATE'2026-07-29'"``
    for incremental rebuilds. Returns MERGE metrics."""
    run_id = uuid.uuid4().hex
    row = spark.sql(ecosystem_hourly_merge_sql(run_id, where)).collect()[0].asDict()
    metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
    logger.info("gold merge %s: %s", GOLD_ECOSYSTEM_HOURLY_TABLE, metrics)
    return metrics
