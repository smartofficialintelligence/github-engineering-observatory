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
    SILVER_ISSUE_EVENTS_TABLE,
    SILVER_RELEASE_EVENTS_TABLE,
    SILVER_REVIEW_EVENTS_TABLE,
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

METRIC_DEFINITIONS_VERSION = 4

# Columns added in v4 (docs/lifecycle_metrics_spec.md v1 + metric_definitions v4):
# denominators, issue closure taxonomy, review states, three comment classes,
# release download sum. Historical rows carry NULL for these until pass-2
# (bronze.bq_lifecycle_hourly) is merged in.
V4_LIFECYCLE_COLUMNS = (
    "pr_events_total", "issues_events_total",
    "issues_closed_completed", "issues_closed_not_planned",
    "issues_closed_duplicate", "issues_closed_unknown",
    "reviews_approved", "reviews_changes_requested",
    "reviews_commented", "reviews_dismissed",
    "issue_comments_true", "issue_comments_on_prs",
    "pr_review_comments", "commit_comments",
    "release_download_count_sum",
)

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
    + ", ".join(f"{c} BIGINT" for c in V4_LIFECYCLE_COLUMNS) + ", "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP, "
    "source STRING"
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
    """Aggregate silver.events (base) and LEFT JOIN per-hour counts from
    the specialized silver tables (issue_events, review_events,
    release_events) to compute the v4 columns.

    The specialized tables don't carry quality_flag today — Silver's own
    filter (event_type + key-field presence) drops the same set as
    silver.events for the payload-derived columns we care about. Filter
    on ``created_at IS NOT NULL`` mirrors the base filter modulo
    quality_flag; Finding-S6 non-public events do not appear in the
    specialized tables (they're ForkEvents, not issue/review/release).
    """
    prod = production_event_sql()
    base = f"""
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
    -- v4 counts computable from silver.events alone
    {_count_if("event_type = 'PullRequestEvent'")} AS pr_events_total,
    {_count_if("event_type = 'IssuesEvent'")} AS issues_events_total,
    {_count_if("event_type = 'PullRequestReviewCommentEvent'")} AS pr_review_comments,
    {_count_if("event_type = 'CommitCommentEvent'")} AS commit_comments
FROM {SILVER_EVENTS_TABLE}
WHERE quality_flag IS NULL
  AND created_at IS NOT NULL
  AND ({where})
GROUP BY date_trunc('HOUR', created_at)"""

    issue_extras = f"""
SELECT
    date_trunc('HOUR', created_at) AS event_hour,
    {_count_if("event_type = 'IssuesEvent' AND action = 'closed'"
               " AND issue_state_reason = 'completed'")} AS issues_closed_completed,
    {_count_if("event_type = 'IssuesEvent' AND action = 'closed'"
               " AND issue_state_reason = 'not_planned'")} AS issues_closed_not_planned,
    {_count_if("event_type = 'IssuesEvent' AND action = 'closed'"
               " AND issue_state_reason = 'duplicate'")} AS issues_closed_duplicate,
    {_count_if("event_type = 'IssuesEvent' AND action = 'closed'"
               " AND (issue_state_reason IS NULL"
               " OR issue_state_reason NOT IN ('completed', 'not_planned', 'duplicate'))")}
        AS issues_closed_unknown,
    {_count_if("event_type = 'IssueCommentEvent' AND NOT is_pull_request")} AS issue_comments_true,
    {_count_if("event_type = 'IssueCommentEvent' AND is_pull_request")} AS issue_comments_on_prs
FROM {SILVER_ISSUE_EVENTS_TABLE}
WHERE created_at IS NOT NULL AND ({where})
GROUP BY date_trunc('HOUR', created_at)"""

    review_extras = f"""
SELECT
    date_trunc('HOUR', created_at) AS event_hour,
    {_count_if("review_state = 'approved'")} AS reviews_approved,
    {_count_if("review_state = 'changes_requested'")} AS reviews_changes_requested,
    {_count_if("review_state = 'commented'")} AS reviews_commented,
    {_count_if("review_state = 'dismissed'")} AS reviews_dismissed
FROM {SILVER_REVIEW_EVENTS_TABLE}
WHERE created_at IS NOT NULL AND ({where})
GROUP BY date_trunc('HOUR', created_at)"""

    release_extras = f"""
SELECT
    date_trunc('HOUR', created_at) AS event_hour,
    SUM(assets_download_count) AS release_download_count_sum
FROM {SILVER_RELEASE_EVENTS_TABLE}
WHERE created_at IS NOT NULL AND ({where})
GROUP BY date_trunc('HOUR', created_at)"""

    select = f"""
WITH base AS ({base}),
     issue_extras AS ({issue_extras}),
     review_extras AS ({review_extras}),
     release_extras AS ({release_extras})
SELECT
    b.event_hour,
    b.total_events, b.total_events_human,
    b.push_events, b.push_events_human,
    b.production_events,
    b.pr_opened, b.pr_merged, b.pr_closed_no_merge, b.pr_reopened,
    b.issues_opened, b.issues_closed, b.issues_reopened,
    b.releases_published,
    b.bot_events,
    b.distinct_actors, b.distinct_actors_human,
    b.distinct_repos, b.distinct_push_repos,
    b.pr_events_total, b.issues_events_total,
    IFNULL(ie.issues_closed_completed, 0) AS issues_closed_completed,
    IFNULL(ie.issues_closed_not_planned, 0) AS issues_closed_not_planned,
    IFNULL(ie.issues_closed_duplicate, 0) AS issues_closed_duplicate,
    IFNULL(ie.issues_closed_unknown, 0) AS issues_closed_unknown,
    IFNULL(rv.reviews_approved, 0) AS reviews_approved,
    IFNULL(rv.reviews_changes_requested, 0) AS reviews_changes_requested,
    IFNULL(rv.reviews_commented, 0) AS reviews_commented,
    IFNULL(rv.reviews_dismissed, 0) AS reviews_dismissed,
    IFNULL(ie.issue_comments_true, 0) AS issue_comments_true,
    IFNULL(ie.issue_comments_on_prs, 0) AS issue_comments_on_prs,
    b.pr_review_comments,
    b.commit_comments,
    IFNULL(rl.release_download_count_sum, 0) AS release_download_count_sum,
    {METRIC_DEFINITIONS_VERSION} AS metric_version,
    '{run_id}' AS gold_run_id,
    current_timestamp() AS gold_built_at,
    'stream' AS source
FROM base b
LEFT JOIN issue_extras ie USING (event_hour)
LEFT JOIN review_extras rv USING (event_hour)
LEFT JOIN release_extras rl USING (event_hour)"""

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
    """Create the Gold table and (re)create the velocity view.

    Idempotent forward migrations:
      * v3: adds ``source`` column, backfills existing rows as 'stream'
      * v4: adds the 15 lifecycle columns (denominators, closure
        taxonomy, review states, comment classes, release-download sum);
        existing rows carry NULL until the next Gold rebuild (stream) or
        lifecycle history merge (bigquery source).
    """
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_ECOSYSTEM_HOURLY_TABLE} "
        f"({ECOSYSTEM_HOURLY_DDL}) USING DELTA"
    )
    columns = {f.name for f in spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE).schema.fields}
    if "source" not in columns:
        spark.sql(f"ALTER TABLE {GOLD_ECOSYSTEM_HOURLY_TABLE} ADD COLUMNS (source STRING)")
        logger.info("added source column to %s", GOLD_ECOSYSTEM_HOURLY_TABLE)
    spark.sql(
        f"UPDATE {GOLD_ECOSYSTEM_HOURLY_TABLE} SET source = 'stream' WHERE source IS NULL"
    )
    # v4: add lifecycle columns missing on pre-v4 tables. NULL for existing
    # rows is intentional — next rebuild fills stream rows; historical rows
    # get filled by ingestion.bq_lifecycle_history.merge_lifecycle_into_gold.
    missing_v4 = [c for c in V4_LIFECYCLE_COLUMNS if c not in columns]
    if missing_v4:
        cols_sql = ", ".join(f"{c} BIGINT" for c in missing_v4)
        spark.sql(f"ALTER TABLE {GOLD_ECOSYSTEM_HOURLY_TABLE} ADD COLUMNS ({cols_sql})")
        logger.info("added v4 lifecycle columns to %s: %s",
                    GOLD_ECOSYSTEM_HOURLY_TABLE, missing_v4)
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
