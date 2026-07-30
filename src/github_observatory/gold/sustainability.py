"""Gold sustainability and network-structure metrics (spec §21 step 10).

* ``gold.actor_retention_daily`` — per day: active actors, how many were
  also active 1 day / 7 days earlier, and the retention ratios. Computed
  by exact-offset self-joins on daily actor sets, so calendar gaps yield
  NULL baselines rather than wrong comparisons (same policy as the
  velocity view).
* ``gold.network_daily`` — v1 actor↔repo bipartite structure proxies:
  repos-per-actor and actors-per-repo distributions, the share of actors
  touching multiple repos, and the share of repos touched by a single
  actor. Full graph metrics (components, centrality) are deferred until
  the measurement pipeline has real history.

Definitions bound by ``docs/metric_definitions.md`` v2. Both metrics
carry the OQ-8 caveat: bot filtering uses the ``[bot]``-suffix lower
bound, and retention of *human* actors is the headline series.
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from github_observatory.common.config import (
    GOLD_NETWORK_DAILY_TABLE,
    GOLD_RETENTION_DAILY_TABLE,
    SILVER_EVENTS_TABLE,
)
from github_observatory.gold.metrics import METRIC_DEFINITIONS_VERSION

logger = logging.getLogger("github_observatory.gold.sustainability")

RETENTION_DAILY_DDL = (
    "event_date DATE, active_actors BIGINT, active_actors_human BIGINT, "
    "retained_from_1d BIGINT, retention_1d DOUBLE, "
    "retained_from_7d BIGINT, retention_7d DOUBLE, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)

NETWORK_DAILY_DDL = (
    "event_date DATE, actors BIGINT, repos BIGINT, edges BIGINT, "
    "multi_repo_actors BIGINT, multi_repo_actor_share DOUBLE, "
    "repos_per_actor_avg DOUBLE, repos_per_actor_p95 DOUBLE, "
    "actors_per_repo_avg DOUBLE, actors_per_repo_p95 DOUBLE, "
    "single_actor_repo_share DOUBLE, "
    "metric_version INT, gold_run_id STRING, gold_built_at TIMESTAMP"
)


def create_sustainability_tables(spark: Any) -> None:
    for table, ddl in (
        (GOLD_RETENTION_DAILY_TABLE, RETENTION_DAILY_DDL),
        (GOLD_NETWORK_DAILY_TABLE, NETWORK_DAILY_DDL),
    ):
        spark.sql(f"CREATE TABLE IF NOT EXISTS {table} ({ddl}) USING DELTA")


def _run_cols(run_id: str) -> str:
    return (
        f"{METRIC_DEFINITIONS_VERSION} AS metric_version, "
        f"'{run_id}' AS gold_run_id, current_timestamp() AS gold_built_at"
    )


def retention_daily_merge_sql(run_id: str, where: str = "TRUE") -> str:
    # daily_actors spans ALL ingested history (baselines may lie outside
    # the build window); `where` narrows which days get (re)computed.
    select = f"""
WITH daily_actors AS (
    SELECT DISTINCT CAST(created_at AS DATE) AS event_date, actor_id, actor_is_bot
    FROM {SILVER_EVENTS_TABLE}
    WHERE quality_flag IS NULL AND created_at IS NOT NULL AND actor_id IS NOT NULL
),
days AS (
    SELECT event_date,
           COUNT(*) AS active_actors,
           COUNT_IF(NOT actor_is_bot) AS active_actors_human
    FROM daily_actors GROUP BY event_date
),
lag1 AS (
    SELECT c.event_date, COUNT(*) AS retained
    FROM daily_actors c JOIN daily_actors p
      ON p.actor_id = c.actor_id AND p.event_date = c.event_date - INTERVAL 1 DAY
    GROUP BY c.event_date
),
lag7 AS (
    SELECT c.event_date, COUNT(*) AS retained
    FROM daily_actors c JOIN daily_actors p
      ON p.actor_id = c.actor_id AND p.event_date = c.event_date - INTERVAL 7 DAYS
    GROUP BY c.event_date
),
base1 AS (SELECT event_date + INTERVAL 1 DAY AS event_date, active_actors AS prev FROM days),
base7 AS (SELECT event_date + INTERVAL 7 DAYS AS event_date, active_actors AS prev FROM days)
SELECT d.event_date, d.active_actors, d.active_actors_human,
       l1.retained AS retained_from_1d,
       try_divide(CAST(l1.retained AS DOUBLE), b1.prev) AS retention_1d,
       l7.retained AS retained_from_7d,
       try_divide(CAST(l7.retained AS DOUBLE), b7.prev) AS retention_7d,
       {_run_cols(run_id)}
FROM days d
LEFT JOIN lag1 l1 USING (event_date)
LEFT JOIN base1 b1 USING (event_date)
LEFT JOIN lag7 l7 USING (event_date)
LEFT JOIN base7 b7 USING (event_date)
WHERE {where}"""
    return (
        f"MERGE INTO {GOLD_RETENTION_DAILY_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.event_date = s.event_date\n"
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )


def network_daily_merge_sql(run_id: str, where: str = "TRUE") -> str:
    select = f"""
WITH edges AS (
    SELECT DISTINCT CAST(created_at AS DATE) AS event_date, actor_id, repo_id
    FROM {SILVER_EVENTS_TABLE}
    WHERE quality_flag IS NULL AND created_at IS NOT NULL
      AND actor_id IS NOT NULL AND repo_id IS NOT NULL AND ({where})
),
per_actor AS (
    SELECT event_date, actor_id, COUNT(*) AS repos FROM edges
    GROUP BY event_date, actor_id
),
per_repo AS (
    SELECT event_date, repo_id, COUNT(*) AS actors FROM edges
    GROUP BY event_date, repo_id
),
actor_stats AS (
    SELECT event_date,
           COUNT(*) AS actors,
           SUM(repos) AS edges,
           COUNT_IF(repos > 1) AS multi_repo_actors,
           AVG(repos) AS repos_per_actor_avg,
           percentile_approx(repos, 0.95) AS repos_per_actor_p95
    FROM per_actor GROUP BY event_date
),
repo_stats AS (
    SELECT event_date,
           COUNT(*) AS repos,
           AVG(actors) AS actors_per_repo_avg,
           percentile_approx(actors, 0.95) AS actors_per_repo_p95,
           AVG(CASE WHEN actors = 1 THEN 1.0 ELSE 0.0 END) AS single_actor_repo_share
    FROM per_repo GROUP BY event_date
)
SELECT a.event_date, a.actors, r.repos, a.edges,
       a.multi_repo_actors,
       try_divide(CAST(a.multi_repo_actors AS DOUBLE), a.actors) AS multi_repo_actor_share,
       a.repos_per_actor_avg,
       CAST(a.repos_per_actor_p95 AS DOUBLE) AS repos_per_actor_p95,
       r.actors_per_repo_avg,
       CAST(r.actors_per_repo_p95 AS DOUBLE) AS actors_per_repo_p95,
       r.single_actor_repo_share,
       {_run_cols(run_id)}
FROM actor_stats a JOIN repo_stats r USING (event_date)"""
    return (
        f"MERGE INTO {GOLD_NETWORK_DAILY_TABLE} AS t\nUSING (\n{select}\n) AS s\n"
        "ON t.event_date = s.event_date\n"
        "WHEN MATCHED THEN UPDATE SET *\nWHEN NOT MATCHED THEN INSERT *"
    )


def build_sustainability(spark: Any, *, where: str = "TRUE") -> dict[str, dict[str, int]]:
    run_id = uuid.uuid4().hex
    results: dict[str, dict[str, int]] = {}
    for table, sql in (
        (GOLD_RETENTION_DAILY_TABLE, retention_daily_merge_sql(run_id, where)),
        (GOLD_NETWORK_DAILY_TABLE, network_daily_merge_sql(run_id, where)),
    ):
        row = spark.sql(sql).collect()[0].asDict()
        metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
        results[table] = metrics
        logger.info("sustainability merge %s: %s", table, metrics)
    return results
