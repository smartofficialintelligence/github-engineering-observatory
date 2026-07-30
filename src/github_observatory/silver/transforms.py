"""Silver normalization: Bronze rows → typed events + lifecycle tables
(spec §21 step 6).

Tables built from ``bronze.events_raw``:

* ``silver.events`` — one row per event with typed columns: ``created_at``
  cast to TIMESTAMP, actor/repo/org scalars extracted from the Bronze JSON
  strings, ``payload_action`` promoted (the 2026 stream's lifecycle signal
  — ``pull_request.state``/``merged`` no longer exist, Finding S2), push
  ``ref``/``ref_type``/``push_id`` promoted, and ``actor_is_bot`` from the
  ``[bot]`` login suffix (a lower bound; heuristics beyond the suffix are
  OQ-8 and must be versioned when they land).
* ``silver.pr_events`` — one row per PR-carrying event
  (PullRequestEvent / PullRequestReviewEvent / PullRequestReviewCommentEvent)
  keyed for lifecycle joins on ``(repo_id, pr_number)``, verified
  collision-free in-sample.
* ``silver.issue_events`` — IssuesEvent / IssueCommentEvent rows with the
  rich issue object's ``state``/``state_reason``/``created_at``/``closed_at``
  and the critical ``is_pull_request`` partition flag: issues and PRs share
  one number space per repo, so this flag must gate any
  ``(repo_id, number)`` join. ``pr_merged_at`` captures the merge timestamp
  leaked via ``issue.pull_request.merged_at`` on PR comments.
* ``silver.review_events`` — PullRequestReviewEvent with review state
  (all four states observed in-sample) and submission metadata.
* ``silver.release_events`` — ReleaseEvent with tag/prerelease/draft and
  aggregated asset download counts (the one genuine adoption signal
  in-stream, OQ-10).

Quality policy (Finding S6): rows with ``public = false`` or an
unextractable ``repo.id`` are **flagged, not dropped** — ``quality_flag``
carries the first matching reason (``non_public`` > ``missing_repo_id`` >
``invalid_created_at``) and NULL means clean. Gold metrics filter on it
explicitly, so nothing disappears silently.

The SQL builders (``*_merge_sql``) are the single implementation of the
mapping — set-based over Delta so they scale to backfill, all MERGE on
``event_id`` so re-runs are no-ops. They are tested by executing them for
real: ``tests/integration`` runs Bronze ingest + ``build_silver`` against
local OSS Spark/Delta with ANSI mode on (see the dev extras), and the
workspace notebook 03 re-asserts the same invariants on serverless — the
authoritative runtime — after every build.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from github_observatory.common.config import (
    EVENTS_RAW_TABLE,
    SILVER_EVENTS_TABLE,
    SILVER_ISSUE_EVENTS_TABLE,
    SILVER_PR_EVENTS_TABLE,
    SILVER_RELEASE_EVENTS_TABLE,
    SILVER_REVIEW_EVENTS_TABLE,
)

logger = logging.getLogger("github_observatory.silver")

BOT_LOGIN_SUFFIX = "[bot]"

FLAG_NON_PUBLIC = "non_public"
FLAG_MISSING_REPO_ID = "missing_repo_id"
FLAG_INVALID_CREATED_AT = "invalid_created_at"

PR_EVENT_TYPES = (
    "PullRequestEvent",
    "PullRequestReviewEvent",
    "PullRequestReviewCommentEvent",
)
ISSUE_EVENT_TYPES = ("IssuesEvent", "IssueCommentEvent")

# -- table DDLs ----------------------------------------------------------------

EVENTS_DDL = (
    "event_id STRING, event_type STRING, created_at TIMESTAMP, public BOOLEAN, "
    "actor_id BIGINT, actor_login STRING, actor_is_bot BOOLEAN, "
    "repo_id BIGINT, repo_name STRING, org_id BIGINT, org_login STRING, "
    "payload_action STRING, payload_ref STRING, payload_ref_type STRING, "
    "push_id BIGINT, quality_flag STRING, "
    "source_file STRING, source_hour TIMESTAMP, source_date DATE, "
    "silver_run_id STRING, silver_built_at TIMESTAMP"
)

PR_EVENTS_DDL = (
    "event_id STRING, event_type STRING, created_at TIMESTAMP, "
    "repo_id BIGINT, repo_name STRING, "
    "actor_id BIGINT, actor_login STRING, actor_is_bot BOOLEAN, "
    "pr_number BIGINT, pr_id BIGINT, action STRING, "
    "base_ref STRING, base_sha STRING, head_ref STRING, head_sha STRING, "
    "source_date DATE, silver_run_id STRING, silver_built_at TIMESTAMP"
)

ISSUE_EVENTS_DDL = (
    "event_id STRING, event_type STRING, created_at TIMESTAMP, "
    "repo_id BIGINT, repo_name STRING, "
    "actor_id BIGINT, actor_login STRING, actor_is_bot BOOLEAN, "
    "issue_number BIGINT, issue_id BIGINT, action STRING, "
    "is_pull_request BOOLEAN, issue_state STRING, issue_state_reason STRING, "
    "issue_created_at TIMESTAMP, issue_closed_at TIMESTAMP, "
    "comment_count BIGINT, pr_merged_at TIMESTAMP, "
    "source_date DATE, silver_run_id STRING, silver_built_at TIMESTAMP"
)

REVIEW_EVENTS_DDL = (
    "event_id STRING, created_at TIMESTAMP, repo_id BIGINT, repo_name STRING, "
    "actor_id BIGINT, actor_login STRING, actor_is_bot BOOLEAN, "
    "pr_number BIGINT, review_id BIGINT, action STRING, review_state STRING, "
    "review_submitted_at TIMESTAMP, review_commit_sha STRING, "
    "source_date DATE, silver_run_id STRING, silver_built_at TIMESTAMP"
)

RELEASE_EVENTS_DDL = (
    "event_id STRING, created_at TIMESTAMP, repo_id BIGINT, repo_name STRING, "
    "actor_id BIGINT, actor_login STRING, actor_is_bot BOOLEAN, "
    "release_id BIGINT, tag_name STRING, action STRING, "
    "prerelease BOOLEAN, draft BOOLEAN, immutable BOOLEAN, "
    "published_at TIMESTAMP, assets_count INT, assets_download_count BIGINT, "
    "source_date DATE, silver_run_id STRING, silver_built_at TIMESTAMP"
)


def create_silver_tables(spark: Any) -> None:
    """Create the five Silver tables if they do not exist."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {SILVER_EVENTS_TABLE} ({EVENTS_DDL}) "
        "USING DELTA PARTITIONED BY (source_date)"
    )
    for table, ddl in (
        (SILVER_PR_EVENTS_TABLE, PR_EVENTS_DDL),
        (SILVER_ISSUE_EVENTS_TABLE, ISSUE_EVENTS_DDL),
        (SILVER_REVIEW_EVENTS_TABLE, REVIEW_EVENTS_DDL),
        (SILVER_RELEASE_EVENTS_TABLE, RELEASE_EVENTS_DDL),
    ):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA")


# -- SQL builders (production path) --------------------------------------------

# Shared extraction fragments over bronze.events_raw columns.
_ACTOR_SQL = (
    "TRY_CAST(get_json_object(actor_json, '$.id') AS BIGINT) AS actor_id, "
    "get_json_object(actor_json, '$.login') AS actor_login, "
    f"COALESCE(endswith(get_json_object(actor_json, '$.login'), '{BOT_LOGIN_SUFFIX}'), FALSE) AS actor_is_bot"
)
_REPO_SQL = (
    "TRY_CAST(get_json_object(repo_json, '$.id') AS BIGINT) AS repo_id, "
    "get_json_object(repo_json, '$.name') AS repo_name"
)
_CREATED_TS_SQL = "TRY_CAST(created_at AS TIMESTAMP)"
_RUN_COLS_SQL = (
    "source_date, '{run_id}' AS silver_run_id, current_timestamp() AS silver_built_at"
)


def _merge_sql(target: str, select_sql: str) -> str:
    return (
        f"MERGE INTO {target} AS t\n"
        f"USING (\n{select_sql}\n) AS s\n"
        "ON t.event_id = s.event_id\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


def events_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
SELECT
    event_id,
    event_type,
    {_CREATED_TS_SQL} AS created_at,
    public,
    {_ACTOR_SQL},
    {_REPO_SQL},
    TRY_CAST(get_json_object(org_json, '$.id') AS BIGINT) AS org_id,
    get_json_object(org_json, '$.login') AS org_login,
    get_json_object(payload_json, '$.action') AS payload_action,
    get_json_object(payload_json, '$.ref') AS payload_ref,
    get_json_object(payload_json, '$.ref_type') AS payload_ref_type,
    TRY_CAST(get_json_object(payload_json, '$.push_id') AS BIGINT) AS push_id,
    CASE
        WHEN public = FALSE THEN '{FLAG_NON_PUBLIC}'
        WHEN get_json_object(repo_json, '$.id') IS NULL THEN '{FLAG_MISSING_REPO_ID}'
        WHEN {_CREATED_TS_SQL} IS NULL THEN '{FLAG_INVALID_CREATED_AT}'
        ELSE NULL
    END AS quality_flag,
    source_file,
    source_hour,
    {_RUN_COLS_SQL.format(run_id=run_id)}
FROM {EVENTS_RAW_TABLE}
WHERE {where}"""
    return _merge_sql(SILVER_EVENTS_TABLE, select)


def pr_events_merge_sql(run_id: str, where: str = "TRUE") -> str:
    types = ", ".join(f"'{t}'" for t in PR_EVENT_TYPES)
    select = f"""
SELECT
    event_id,
    event_type,
    {_CREATED_TS_SQL} AS created_at,
    {_REPO_SQL},
    {_ACTOR_SQL},
    TRY_CAST(get_json_object(payload_json, '$.pull_request.number') AS BIGINT) AS pr_number,
    TRY_CAST(get_json_object(payload_json, '$.pull_request.id') AS BIGINT) AS pr_id,
    get_json_object(payload_json, '$.action') AS action,
    get_json_object(payload_json, '$.pull_request.base.ref') AS base_ref,
    get_json_object(payload_json, '$.pull_request.base.sha') AS base_sha,
    get_json_object(payload_json, '$.pull_request.head.ref') AS head_ref,
    get_json_object(payload_json, '$.pull_request.head.sha') AS head_sha,
    {_RUN_COLS_SQL.format(run_id=run_id)}
FROM {EVENTS_RAW_TABLE}
WHERE event_type IN ({types})
  AND get_json_object(payload_json, '$.pull_request.number') IS NOT NULL
  AND ({where})"""
    return _merge_sql(SILVER_PR_EVENTS_TABLE, select)


def issue_events_merge_sql(run_id: str, where: str = "TRUE") -> str:
    types = ", ".join(f"'{t}'" for t in ISSUE_EVENT_TYPES)
    select = f"""
SELECT
    event_id,
    event_type,
    {_CREATED_TS_SQL} AS created_at,
    {_REPO_SQL},
    {_ACTOR_SQL},
    TRY_CAST(get_json_object(payload_json, '$.issue.number') AS BIGINT) AS issue_number,
    TRY_CAST(get_json_object(payload_json, '$.issue.id') AS BIGINT) AS issue_id,
    get_json_object(payload_json, '$.action') AS action,
    get_json_object(payload_json, '$.issue.pull_request') IS NOT NULL AS is_pull_request,
    get_json_object(payload_json, '$.issue.state') AS issue_state,
    get_json_object(payload_json, '$.issue.state_reason') AS issue_state_reason,
    TRY_CAST(get_json_object(payload_json, '$.issue.created_at') AS TIMESTAMP) AS issue_created_at,
    TRY_CAST(get_json_object(payload_json, '$.issue.closed_at') AS TIMESTAMP) AS issue_closed_at,
    TRY_CAST(get_json_object(payload_json, '$.issue.comments') AS BIGINT) AS comment_count,
    TRY_CAST(get_json_object(payload_json, '$.issue.pull_request.merged_at') AS TIMESTAMP) AS pr_merged_at,
    {_RUN_COLS_SQL.format(run_id=run_id)}
FROM {EVENTS_RAW_TABLE}
WHERE event_type IN ({types})
  AND get_json_object(payload_json, '$.issue.number') IS NOT NULL
  AND ({where})"""
    return _merge_sql(SILVER_ISSUE_EVENTS_TABLE, select)


def review_events_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
SELECT
    event_id,
    {_CREATED_TS_SQL} AS created_at,
    {_REPO_SQL},
    {_ACTOR_SQL},
    TRY_CAST(get_json_object(payload_json, '$.pull_request.number') AS BIGINT) AS pr_number,
    TRY_CAST(get_json_object(payload_json, '$.review.id') AS BIGINT) AS review_id,
    get_json_object(payload_json, '$.action') AS action,
    get_json_object(payload_json, '$.review.state') AS review_state,
    TRY_CAST(get_json_object(payload_json, '$.review.submitted_at') AS TIMESTAMP) AS review_submitted_at,
    get_json_object(payload_json, '$.review.commit_id') AS review_commit_sha,
    {_RUN_COLS_SQL.format(run_id=run_id)}
FROM {EVENTS_RAW_TABLE}
WHERE event_type = 'PullRequestReviewEvent'
  AND get_json_object(payload_json, '$.review.id') IS NOT NULL
  AND ({where})"""
    return _merge_sql(SILVER_REVIEW_EVENTS_TABLE, select)


def release_events_merge_sql(run_id: str, where: str = "TRUE") -> str:
    assets = (
        "COALESCE(from_json(get_json_object(payload_json, '$.release.assets'), "
        "'ARRAY<STRUCT<download_count: BIGINT>>'), array())"
    )
    select = f"""
SELECT
    event_id,
    {_CREATED_TS_SQL} AS created_at,
    {_REPO_SQL},
    {_ACTOR_SQL},
    TRY_CAST(get_json_object(payload_json, '$.release.id') AS BIGINT) AS release_id,
    get_json_object(payload_json, '$.release.tag_name') AS tag_name,
    get_json_object(payload_json, '$.action') AS action,
    TRY_CAST(get_json_object(payload_json, '$.release.prerelease') AS BOOLEAN) AS prerelease,
    TRY_CAST(get_json_object(payload_json, '$.release.draft') AS BOOLEAN) AS draft,
    TRY_CAST(get_json_object(payload_json, '$.release.immutable') AS BOOLEAN) AS immutable,
    TRY_CAST(get_json_object(payload_json, '$.release.published_at') AS TIMESTAMP) AS published_at,
    CAST(size({assets}) AS INT) AS assets_count,
    aggregate({assets}, 0L, (acc, a) -> acc + COALESCE(a.download_count, 0L)) AS assets_download_count,
    {_RUN_COLS_SQL.format(run_id=run_id)}
FROM {EVENTS_RAW_TABLE}
WHERE event_type = 'ReleaseEvent'
  AND get_json_object(payload_json, '$.release.id') IS NOT NULL
  AND ({where})"""
    return _merge_sql(SILVER_RELEASE_EVENTS_TABLE, select)


def build_silver(spark: Any, *, where: str = "TRUE") -> dict[str, dict[str, int]]:
    """Run all five MERGEs (idempotent). ``where`` restricts the Bronze
    scan, e.g. ``"source_date = DATE'2026-07-29'"`` for incremental runs.

    Returns per-table MERGE metrics.
    """
    run_id = uuid.uuid4().hex
    results: dict[str, dict[str, int]] = {}
    for table, sql in (
        (SILVER_EVENTS_TABLE, events_merge_sql(run_id, where)),
        (SILVER_PR_EVENTS_TABLE, pr_events_merge_sql(run_id, where)),
        (SILVER_ISSUE_EVENTS_TABLE, issue_events_merge_sql(run_id, where)),
        (SILVER_REVIEW_EVENTS_TABLE, review_events_merge_sql(run_id, where)),
        (SILVER_RELEASE_EVENTS_TABLE, release_events_merge_sql(run_id, where)),
    ):
        row = spark.sql(sql).collect()[0].asDict()
        metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
        results[table] = metrics
        logger.info("silver merge %s: %s", table, metrics)
    return results
