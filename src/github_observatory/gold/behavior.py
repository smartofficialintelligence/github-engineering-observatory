"""Gold behavior metrics: flow, contribution, engagement, data quality
(spec §21 step 8).

Tables (all bound by ``docs/metric_definitions.md`` v2):

* ``gold.flow_daily`` — dispersion/regularity candidates over each day's
  hourly production series: CV, Fano factor, burstiness, hourly entropy.
  ``hours_observed`` marks partial days; OQ-9 (which candidate becomes
  the primary Flow metric) stays open until ≥2 weeks of history exists.
* ``gold.actor_first_seen`` — insert-only dimension: first event time
  per actor, feeding new-actor counts. First-seen is relative to
  ingested history and can only move earlier as backfill deepens.
* ``gold.contribution_daily`` — actor-centric daily distribution:
  distinct actors, bot share, top-N actor concentration, per-actor
  percentiles, new actors.
* ``gold.engagement_daily`` — stream-observed stars/forks with the OQ-1
  coverage caveat: these are **not** census counts.
* ``gold.data_quality`` — per source_file conservation checks:
  Bronze/Silver row parity, quarantine/flag/null counts, duplicate ids.

All builds are MERGE-upserts (recompute-and-overwrite within the build
window, untouched outside it) following ``gold/metrics.py``.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from github_observatory.common.config import (
    EVENTS_QUARANTINE_TABLE,
    EVENTS_RAW_TABLE,
    GOLD_ACTOR_FIRST_SEEN_TABLE,
    GOLD_CONTRIBUTION_DAILY_TABLE,
    GOLD_DATA_QUALITY_TABLE,
    GOLD_ECOSYSTEM_HOURLY_TABLE,
    GOLD_ENGAGEMENT_DAILY_TABLE,
    GOLD_FLOW_DAILY_TABLE,
    SILVER_EVENTS_TABLE,
)
from github_observatory.gold.metrics import METRIC_DEFINITIONS_VERSION

logger = logging.getLogger("github_observatory.gold.behavior")

FLOW_DAILY_DDL = (
    "event_date DATE, hours_observed INT, "
    "production_total BIGINT, production_mean DOUBLE, production_std DOUBLE, "
    "production_cv DOUBLE, production_fano DOUBLE, production_burstiness DOUBLE, "
    "production_hourly_entropy DOUBLE, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)

ACTOR_FIRST_SEEN_DDL = (
    "actor_id BIGINT, actor_login STRING, actor_is_bot BOOLEAN, "
    "first_seen_at TIMESTAMP, first_seen_date DATE"
)

CONTRIBUTION_DAILY_DDL = (
    "event_date DATE, events BIGINT, actors BIGINT, actors_human BIGINT, "
    "bot_event_share DOUBLE, "
    "top1_actor_share DOUBLE, top10_actor_share DOUBLE, top100_actor_share DOUBLE, "
    "events_per_actor_p50 DOUBLE, events_per_actor_p90 DOUBLE, "
    "events_per_actor_p99 DOUBLE, new_actors BIGINT, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)

ENGAGEMENT_DAILY_DDL = (
    "event_date DATE, stars BIGINT, forks BIGINT, "
    "distinct_starring_actors BIGINT, distinct_starred_repos BIGINT, "
    "distinct_forked_repos BIGINT, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)

DATA_QUALITY_DDL = (
    "source_file STRING, source_hour TIMESTAMP, "
    "bronze_rows BIGINT, silver_rows BIGINT, parity_ok BOOLEAN, "
    "quarantined BIGINT, flagged BIGINT, null_created_at BIGINT, "
    "duplicate_event_ids BIGINT, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)


def create_behavior_tables(spark: Any) -> None:
    for table, ddl in (
        (GOLD_FLOW_DAILY_TABLE, FLOW_DAILY_DDL),
        (GOLD_ACTOR_FIRST_SEEN_TABLE, ACTOR_FIRST_SEEN_DDL),
        (GOLD_CONTRIBUTION_DAILY_TABLE, CONTRIBUTION_DAILY_DDL),
        (GOLD_ENGAGEMENT_DAILY_TABLE, ENGAGEMENT_DAILY_DDL),
        (GOLD_DATA_QUALITY_TABLE, DATA_QUALITY_DDL),
    ):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA")


_RUN_COLS = (
    "{version} AS metric_version, '{run_id}' AS gold_run_id, "
    "current_timestamp() AS gold_built_at"
)


def _run_cols(run_id: str) -> str:
    return _RUN_COLS.format(version=METRIC_DEFINITIONS_VERSION, run_id=run_id)


def flow_daily_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
WITH hourly AS (
    SELECT CAST(event_hour AS DATE) AS event_date,
           production_events
    FROM {GOLD_ECOSYSTEM_HOURLY_TABLE}
    WHERE {where}
),
daily AS (
    SELECT event_date,
           COUNT(*) AS hours_observed,
           SUM(production_events) AS production_total,
           AVG(production_events) AS production_mean,
           COALESCE(STDDEV_POP(production_events), 0.0) AS production_std
    FROM hourly
    GROUP BY event_date
),
entropy AS (
    SELECT h.event_date,
           -SUM(
               CASE WHEN h.production_events > 0 AND d.production_total > 0
                    THEN (h.production_events / d.production_total)
                         * ln(h.production_events / d.production_total)
                    ELSE 0.0 END
           ) AS production_hourly_entropy
    FROM hourly h JOIN daily d USING (event_date)
    GROUP BY h.event_date
)
SELECT d.event_date,
       CAST(d.hours_observed AS INT) AS hours_observed,
       d.production_total,
       d.production_mean,
       d.production_std,
       try_divide(d.production_std, d.production_mean) AS production_cv,
       try_divide(d.production_std * d.production_std, d.production_mean) AS production_fano,
       try_divide(d.production_std - d.production_mean,
                  d.production_std + d.production_mean) AS production_burstiness,
       e.production_hourly_entropy,
       {_run_cols(run_id)}
FROM daily d JOIN entropy e USING (event_date)"""
    return (
        f"MERGE INTO {GOLD_FLOW_DAILY_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.event_date = s.event_date\n"
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )


def actor_first_seen_merge_sql(run_id: str, where: str = "TRUE") -> str:
    # Insert-only-or-move-earlier: backfill can reveal an earlier first
    # event, never a later one.
    select = f"""
SELECT actor_id,
       MIN_BY(actor_login, created_at) AS actor_login,
       MIN_BY(actor_is_bot, created_at) AS actor_is_bot,
       MIN(created_at) AS first_seen_at,
       CAST(MIN(created_at) AS DATE) AS first_seen_date
FROM {SILVER_EVENTS_TABLE}
WHERE quality_flag IS NULL AND created_at IS NOT NULL
  AND actor_id IS NOT NULL AND ({where})
GROUP BY actor_id"""
    return (
        f"MERGE INTO {GOLD_ACTOR_FIRST_SEEN_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.actor_id = s.actor_id\n"
        "WHEN MATCHED AND s.first_seen_at < t.first_seen_at THEN UPDATE SET *\n"
        "WHEN NOT MATCHED THEN INSERT *"
    )


def contribution_daily_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
WITH clean AS (
    SELECT CAST(created_at AS DATE) AS event_date, actor_id, actor_is_bot
    FROM {SILVER_EVENTS_TABLE}
    WHERE quality_flag IS NULL AND created_at IS NOT NULL
      AND actor_id IS NOT NULL AND ({where})
),
per_actor AS (
    SELECT event_date, actor_id, COUNT(*) AS n
    FROM clean GROUP BY event_date, actor_id
),
ranked AS (
    SELECT event_date, actor_id, n,
           ROW_NUMBER() OVER (PARTITION BY event_date ORDER BY n DESC, actor_id) AS rnk
    FROM per_actor
),
daily AS (
    SELECT event_date,
           SUM(n) AS events,
           COUNT(*) AS actors,
           SUM(CASE WHEN rnk <= 1 THEN n ELSE 0 END) AS top1,
           SUM(CASE WHEN rnk <= 10 THEN n ELSE 0 END) AS top10,
           SUM(CASE WHEN rnk <= 100 THEN n ELSE 0 END) AS top100,
           percentile_approx(n, 0.5) AS p50,
           percentile_approx(n, 0.9) AS p90,
           percentile_approx(n, 0.99) AS p99
    FROM ranked GROUP BY event_date
),
human AS (
    SELECT event_date,
           COUNT(DISTINCT CASE WHEN NOT actor_is_bot THEN actor_id END) AS actors_human,
           AVG(CASE WHEN actor_is_bot THEN 1.0 ELSE 0.0 END) AS bot_event_share
    FROM clean GROUP BY event_date
),
fresh AS (
    SELECT first_seen_date AS event_date, COUNT(*) AS new_actors
    FROM {GOLD_ACTOR_FIRST_SEEN_TABLE} GROUP BY first_seen_date
)
SELECT d.event_date, d.events, d.actors, h.actors_human, h.bot_event_share,
       try_divide(CAST(d.top1 AS DOUBLE), d.events) AS top1_actor_share,
       try_divide(CAST(d.top10 AS DOUBLE), d.events) AS top10_actor_share,
       try_divide(CAST(d.top100 AS DOUBLE), d.events) AS top100_actor_share,
       CAST(d.p50 AS DOUBLE) AS events_per_actor_p50,
       CAST(d.p90 AS DOUBLE) AS events_per_actor_p90,
       CAST(d.p99 AS DOUBLE) AS events_per_actor_p99,
       COALESCE(f.new_actors, 0) AS new_actors,
       {_run_cols(run_id)}
FROM daily d
JOIN human h USING (event_date)
LEFT JOIN fresh f USING (event_date)"""
    return (
        f"MERGE INTO {GOLD_CONTRIBUTION_DAILY_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.event_date = s.event_date\n"
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )


def engagement_daily_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
SELECT CAST(created_at AS DATE) AS event_date,
       COUNT_IF(event_type = 'WatchEvent') AS stars,
       COUNT_IF(event_type = 'ForkEvent') AS forks,
       COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent' THEN actor_id END) AS distinct_starring_actors,
       COUNT(DISTINCT CASE WHEN event_type = 'WatchEvent' THEN repo_id END) AS distinct_starred_repos,
       COUNT(DISTINCT CASE WHEN event_type = 'ForkEvent' THEN repo_id END) AS distinct_forked_repos,
       {_run_cols(run_id)}
FROM {SILVER_EVENTS_TABLE}
WHERE quality_flag IS NULL AND created_at IS NOT NULL
  AND event_type IN ('WatchEvent', 'ForkEvent') AND ({where})
GROUP BY CAST(created_at AS DATE)"""
    return (
        f"MERGE INTO {GOLD_ENGAGEMENT_DAILY_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.event_date = s.event_date\n"
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )


def data_quality_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
WITH bronze AS (
    SELECT source_file, MAX(source_hour) AS source_hour, COUNT(*) AS bronze_rows
    FROM {EVENTS_RAW_TABLE} WHERE {where} GROUP BY source_file
),
silver AS (
    SELECT source_file,
           COUNT(*) AS silver_rows,
           COUNT_IF(quality_flag IS NOT NULL) AS flagged,
           COUNT_IF(created_at IS NULL) AS null_created_at,
           COUNT(*) - COUNT(DISTINCT event_id) AS duplicate_event_ids
    FROM {SILVER_EVENTS_TABLE} WHERE {where} GROUP BY source_file
),
quarantine AS (
    SELECT source_file, COUNT(*) AS quarantined
    FROM {EVENTS_QUARANTINE_TABLE} GROUP BY source_file
)
SELECT b.source_file, b.source_hour,
       b.bronze_rows,
       COALESCE(s.silver_rows, 0) AS silver_rows,
       b.bronze_rows = COALESCE(s.silver_rows, 0) AS parity_ok,
       COALESCE(q.quarantined, 0) AS quarantined,
       COALESCE(s.flagged, 0) AS flagged,
       COALESCE(s.null_created_at, 0) AS null_created_at,
       COALESCE(s.duplicate_event_ids, 0) AS duplicate_event_ids,
       {_run_cols(run_id)}
FROM bronze b
LEFT JOIN silver s USING (source_file)
LEFT JOIN quarantine q USING (source_file)"""
    return (
        f"MERGE INTO {GOLD_DATA_QUALITY_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.source_file = s.source_file\n"
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )


def build_behavior(spark: Any, *, where: str = "TRUE") -> dict[str, dict[str, int]]:
    """Run all behavior builds (idempotent). ``where`` restricts source
    scans (silver predicates for contribution/engagement/first-seen and
    data-quality; an ecosystem_hourly predicate for flow)."""
    run_id = uuid.uuid4().hex
    results: dict[str, dict[str, int]] = {}
    # actor_first_seen must precede contribution (new_actors reads it).
    for table, sql in (
        (GOLD_ACTOR_FIRST_SEEN_TABLE, actor_first_seen_merge_sql(run_id, where)),
        (GOLD_FLOW_DAILY_TABLE, flow_daily_merge_sql(run_id)),
        (GOLD_CONTRIBUTION_DAILY_TABLE, contribution_daily_merge_sql(run_id, where)),
        (GOLD_ENGAGEMENT_DAILY_TABLE, engagement_daily_merge_sql(run_id, where)),
        (GOLD_DATA_QUALITY_TABLE, data_quality_merge_sql(run_id, where)),
    ):
        row = spark.sql(sql).collect()[0].asDict()
        metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
        results[table] = metrics
        logger.info("behavior merge %s: %s", table, metrics)
    return results
