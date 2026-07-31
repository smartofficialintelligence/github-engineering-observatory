#!/usr/bin/env python3
"""Generate the AI/BI (Lakeview) dashboard definitions — dashboards as code.

Writes ``dashboards/*.lvdash.json`` from compact widget specs below.
Deploy with ``scripts/databricks_run.py deploy-dashboard <file>`` (creates
or updates by display name, then publishes with embedded credentials so
the hourly job keeps every tile current).

Grid: 6 columns wide; positions are (x, y, w, h).
"""

from __future__ import annotations

import json
import os
import sys

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboards")

G = "github_observatory.gold"


def dataset(name: str, query: str) -> dict:
    return {"name": name, "displayName": name, "queryLines": [query]}


def widget(name, dataset_name, wtype, title, encodings, pos, fields=None):
    query = {
        "datasetName": dataset_name,
        "disaggregated": False,
        "fields": fields or [],
    }
    return {
        "widget": {
            "name": name,
            "queries": [{"name": "main_query", "query": query}],
            "spec": {
                "version": 3,
                "widgetType": wtype,
                "frame": {"title": title, "showTitle": True},
                "encodings": encodings,
            },
        },
        "position": {"x": pos[0], "y": pos[1], "width": pos[2], "height": pos[3]},
    }


def field(name):
    return {"name": name, "expression": f"`{name}`"}


def line_enc(x, y, color=None, y_display=None):
    enc = {
        "x": {"fieldName": x, "scale": {"type": "temporal"}, "displayName": x},
        "y": {"fieldName": y, "scale": {"type": "quantitative"},
              "displayName": y_display or y},
    }
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"},
                        "displayName": color}
    return enc


def table_enc(columns):
    return {
        "columns": [
            {"fieldName": c, "displayName": c, "booleanValues": ["false", "true"]}
            for c in columns
        ]
    }


def counter_enc(value):
    return {"value": {"fieldName": value, "displayName": value}}


def text_widget(name, markdown, pos):
    return {
        "widget": {"name": name, "textbox_spec": markdown},
        "position": {"x": pos[0], "y": pos[1], "width": pos[2], "height": pos[3]},
    }


# ---------------------------------------------------------------- observatory

REGIME_NOTE = """\
## How to read this observatory

Everything here derives from GitHub's **public events feed** (via GH Archive
and its BigQuery mirror). For a decade that feed was a **census** of public
activity. Since mid-2025 it is not.

**The regime change (OQ-1).** Starting **June 2025** (volume −26% in one
month) with a second step in **October 2025**, most non-push event classes
were progressively dropped or sampled upstream — by 2026 stars run at ~44/hr
and PR events ~189/hr *for all of public GitHub*, which is implausibly low,
while pushes kept flowing at realistic rates (push share rose 64% → 94%).
Archive coverage was complete throughout, ruling out collection gaps: the
*feed itself* was filtered. Who filters (GitHub's /events API vs archive
collection) and by what rule is unresolved — tracked as **OQ-1**.

**Reading rules**

* **Full feed (thru May 2025)** — levels and trends are ecosystem facts.
* **Filtered feed (Jun 2025 on)** — only **push-based** metrics are
  trustworthy in absolute terms; star/fork/PR/issue numbers are a sample of
  unknown coverage. Fine for movement *within* the regime; never compare
  levels across the color boundary.
* The bot-share "drop" at the boundary (~27% → ~9%) is the filter changing
  the *measured mix*, not bots leaving — bots disproportionately emit the
  filtered event types.

**Provenance.** Rows are tagged by `source`: `bigquery` = decade import from
the public dataset (census columns; deduped on event id); `stream` = our own
hourly ingestion (2026-07 onward, full event detail). Stream rows always win
on overlap. Jan–Jun 2026 is a known gap.
"""


OBSERVATORY = {
    "datasets": [
        # ---- provenance & coverage -----------------------------------------
        dataset("coverage_monthly", f"""
            SELECT date_trunc('MONTH', event_hour) AS month, source,
                   COUNT(*) AS hours_observed
            FROM {G}.ecosystem_hourly GROUP BY 1, 2"""),
        dataset("regime_monthly", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   ROUND(SUM(push_events) / SUM(total_events), 4) AS push_share,
                   ROUND(SUM(bot_events) / SUM(total_events), 4) AS bot_share,
                   CASE WHEN date_trunc('MONTH', event_hour) < DATE'2025-06-01'
                        THEN 'full feed (thru May 2025)'
                        ELSE 'filtered feed (Jun 2025 on, OQ-1)' END AS regime
            FROM {G}.ecosystem_hourly GROUP BY 1, 4"""),
        # ---- production ----------------------------------------------------
        dataset("production_decade", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   CAST(SUM(push_events) / COUNT(*) AS BIGINT) AS pushes_per_hour,
                   CASE WHEN date_trunc('MONTH', event_hour) < DATE'2025-06-01'
                        THEN 'full feed (thru May 2025)'
                        ELSE 'filtered feed (Jun 2025 on, OQ-1)' END AS regime
            FROM {G}.ecosystem_hourly GROUP BY 1, 3"""),
        dataset("production_daily", f"""
            SELECT CAST(event_hour AS DATE) AS day,
                   SUM(production_events) AS production_events,
                   SUM(pr_opened) AS pr_opened, SUM(pr_merged) AS pr_merged,
                   SUM(issues_opened) AS issues_opened,
                   SUM(issues_closed) AS issues_closed,
                   SUM(releases_published) AS releases_published
            FROM {G}.ecosystem_hourly WHERE source = 'stream' GROUP BY 1"""),
        dataset("velocity_recent", f"""
            SELECT event_hour, production_events,
                   ROUND(production_events_pct_24h, 4) AS production_pct_24h
            FROM {G}.ecosystem_velocity
            WHERE event_hour >= current_timestamp() - INTERVAL 14 DAYS"""),
        # ---- flow ----------------------------------------------------------
        dataset("flow", f"""
            SELECT event_date, hours_observed,
                   ROUND(production_cv, 4) AS cv,
                   ROUND(production_fano, 2) AS fano,
                   ROUND(production_burstiness, 4) AS burstiness,
                   ROUND(production_hourly_entropy, 4) AS entropy
            FROM {G}.flow_daily WHERE hours_observed = 24"""),
        # ---- rework --------------------------------------------------------
        dataset("rework_daily", f"""
            SELECT CAST(event_hour AS DATE) AS day,
                   ROUND(try_divide(SUM(pr_reopened), SUM(pr_opened)), 4)
                       AS pr_reopen_rate,
                   ROUND(try_divide(SUM(issues_reopened), SUM(issues_opened)), 4)
                       AS issue_reopen_rate,
                   ROUND(try_divide(SUM(pr_closed_no_merge),
                                    SUM(pr_closed_no_merge) + SUM(pr_merged)), 4)
                       AS pr_closed_without_merge_rate
            FROM {G}.ecosystem_hourly WHERE source = 'stream' GROUP BY 1"""),
        dataset("review_outcomes", """
            SELECT CAST(created_at AS DATE) AS day, review_state,
                   COUNT(*) AS reviews
            FROM github_observatory.silver.review_events GROUP BY 1, 2"""),
        # ---- contribution --------------------------------------------------
        dataset("contribution", f"""
            SELECT event_date, actors, actors_human, new_actors,
                   ROUND(top100_actor_share, 4) AS top100_actor_share,
                   ROUND(bot_event_share, 4) AS bot_event_share,
                   events_per_actor_p50, events_per_actor_p90, events_per_actor_p99
            FROM {G}.contribution_daily"""),
        dataset("bot_share_decade", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   ROUND(SUM(bot_events) / SUM(total_events), 4) AS bot_share,
                   CASE WHEN date_trunc('MONTH', event_hour) < DATE'2025-06-01'
                        THEN 'full feed (thru May 2025)'
                        ELSE 'filtered feed (Jun 2025 on, OQ-1)' END AS regime
            FROM {G}.ecosystem_hourly GROUP BY 1, 3"""),
        # ---- engagement ----------------------------------------------------
        dataset("engagement", f"""
            SELECT event_date, stars, forks, distinct_starred_repos,
                   distinct_forked_repos
            FROM {G}.engagement_daily"""),
        # ---- sustainability & network --------------------------------------
        dataset("retention", f"""
            SELECT event_date, active_actors, active_actors_human,
                   ROUND(retention_1d, 4) AS retention_1d,
                   ROUND(retention_7d, 4) AS retention_7d
            FROM {G}.actor_retention_daily"""),
        dataset("network", f"""
            SELECT event_date,
                   ROUND(multi_repo_actor_share, 4) AS multi_repo_actor_share,
                   ROUND(single_actor_repo_share, 4) AS single_actor_repo_share,
                   ROUND(repos_per_actor_avg, 3) AS repos_per_actor_avg,
                   ROUND(actors_per_repo_avg, 3) AS actors_per_repo_avg
            FROM {G}.network_daily"""),
        # ---- forecasting ---------------------------------------------------
        dataset("forecast_eval", f"""
            SELECT target, method, n_predictions, ROUND(mae, 1) AS mae,
                   ROUND(smape, 4) AS smape, ROUND(mase, 3) AS mase
            FROM {G}.forecast_eval ORDER BY target, mase"""),
        dataset("forecast_fit", f"""
            SELECT event_hour, actual, prediction
            FROM {G}.forecast_predictions
            WHERE target = 'total_events' AND method = 'seasonal_24h'"""),
    ],
    "pages": [
        {
            "name": "provenance",
            "displayName": "Read Me First — Data & Regimes",
            "layout": [
                text_widget("regime_note", REGIME_NOTE, (0, 0, 6, 8)),
                widget("push_share", "regime_monthly", "line",
                       "Push share — the feed-filtering signature (OQ-1: steps Jun + Oct 2025)",
                       line_enc("month", "push_share", color="regime"),
                       (0, 8, 3, 7),
                       fields=[field("month"), field("push_share"), field("regime")]),
                widget("coverage", "coverage_monthly", "bar",
                       "Hours observed per month, by source (gaps are archive outages)",
                       line_enc("month", "hours_observed", color="source"),
                       (3, 8, 3, 7),
                       fields=[field("month"), field("hours_observed"), field("source")]),
            ],
        },
        {
            "name": "production",
            "displayName": "Production",
            "layout": [
                widget("pushes_decade", "production_decade", "line",
                       "Pushes per hour, monthly avg — the production unit, 2016-present",
                       line_enc("month", "pushes_per_hour", color="regime"),
                       (0, 0, 6, 7),
                       fields=[field("month"), field("pushes_per_hour"), field("regime")]),
                widget("prod_daily", "production_daily", "bar",
                       "Daily production events (stream; OQ-7 whitelist)",
                       line_enc("day", "production_events"),
                       (0, 7, 3, 6),
                       fields=[field("day"), field("production_events")]),
                widget("prod_accel", "velocity_recent", "line",
                       "Production acceleration (% vs same hour yesterday)",
                       line_enc("event_hour", "production_pct_24h"),
                       (3, 7, 3, 6),
                       fields=[field("event_hour"), field("production_pct_24h")]),
                widget("lifecycle_daily", "production_daily", "line",
                       "PR merges and releases per day (stream)",
                       line_enc("day", "pr_merged"),
                       (0, 13, 6, 6),
                       fields=[field("day"), field("pr_merged"),
                               field("releases_published")]),
            ],
        },
        {
            "name": "flow",
            "displayName": "Flow",
            "layout": [
                widget("flow_burstiness", "flow", "line",
                       "Burstiness of hourly production, full days (OQ-9 candidate)",
                       line_enc("event_date", "burstiness"),
                       (0, 0, 3, 6),
                       fields=[field("event_date"), field("burstiness")]),
                widget("flow_entropy", "flow", "line",
                       "Hourly entropy, nats; ln(24) = 3.18 is uniform (OQ-9 candidate)",
                       line_enc("event_date", "entropy"),
                       (3, 0, 3, 6),
                       fields=[field("event_date"), field("entropy")]),
                widget("flow_cv", "flow", "line",
                       "Coefficient of variation (OQ-9 candidate; needs weeks of data)",
                       line_enc("event_date", "cv"),
                       (0, 6, 6, 6),
                       fields=[field("event_date"), field("cv")]),
            ],
        },
        {
            "name": "rework",
            "displayName": "Rework",
            "layout": [
                widget("reopen_rates", "rework_daily", "line",
                       "Reopen rates: PRs and issues (stream; thin-sample caveat OQ-1)",
                       line_enc("day", "pr_reopen_rate"),
                       (0, 0, 3, 6),
                       fields=[field("day"), field("pr_reopen_rate"),
                               field("issue_reopen_rate")]),
                widget("close_no_merge", "rework_daily", "line",
                       "PRs closed without merge / all closures (OQ-2 v1 definition)",
                       line_enc("day", "pr_closed_without_merge_rate"),
                       (3, 0, 3, 6),
                       fields=[field("day"), field("pr_closed_without_merge_rate")]),
                widget("review_outcomes", "review_outcomes", "bar",
                       "Review outcomes per day (dismissed = rework proxy)",
                       line_enc("day", "reviews", color="review_state"),
                       (0, 6, 6, 6),
                       fields=[field("day"), field("reviews"), field("review_state")]),
            ],
        },
        {
            "name": "contribution",
            "displayName": "Contribution",
            "layout": [
                widget("bot_curve", "bot_share_decade", "line",
                       "Bot share of all events — the automation curve, 2016-present",
                       line_enc("month", "bot_share", color="regime"),
                       (0, 0, 6, 7),
                       fields=[field("month"), field("bot_share"), field("regime")]),
                widget("actors_daily", "contribution", "line",
                       "Daily distinct actors (all vs human)",
                       line_enc("event_date", "actors"),
                       (0, 7, 3, 6),
                       fields=[field("event_date"), field("actors"),
                               field("actors_human")]),
                widget("concentration", "contribution", "line",
                       "Top-100 actor share of daily events",
                       line_enc("event_date", "top100_actor_share"),
                       (3, 7, 3, 6),
                       fields=[field("event_date"), field("top100_actor_share")]),
                widget("per_actor", "contribution", "line",
                       "Events per actor: p50 / p90 / p99",
                       line_enc("event_date", "events_per_actor_p90"),
                       (0, 13, 3, 6),
                       fields=[field("event_date"), field("events_per_actor_p50"),
                               field("events_per_actor_p90"),
                               field("events_per_actor_p99")]),
                widget("new_actors", "contribution", "bar",
                       "New actors per day (relative to ingested history)",
                       line_enc("event_date", "new_actors"),
                       (3, 13, 3, 6),
                       fields=[field("event_date"), field("new_actors")]),
            ],
        },
        {
            "name": "engagement",
            "displayName": "Engagement (coverage-caveated)",
            "layout": [
                widget("stars_forks", "engagement", "line",
                       "Stars and forks per day — stream-observed, NOT census (OQ-1)",
                       line_enc("event_date", "stars"),
                       (0, 0, 3, 6),
                       fields=[field("event_date"), field("stars"), field("forks")]),
                widget("engaged_repos", "engagement", "line",
                       "Distinct repos starred / forked per day",
                       line_enc("event_date", "distinct_starred_repos"),
                       (3, 0, 3, 6),
                       fields=[field("event_date"), field("distinct_starred_repos"),
                               field("distinct_forked_repos")]),
            ],
        },
        {
            "name": "sustainability",
            "displayName": "Sustainability & Network",
            "layout": [
                widget("retention_trend", "retention", "line",
                       "Actor retention: 1-day and 7-day (gap-safe NULLs)",
                       line_enc("event_date", "retention_1d"),
                       (0, 0, 3, 6),
                       fields=[field("event_date"), field("retention_1d"),
                               field("retention_7d")]),
                widget("active_actors", "retention", "line",
                       "Daily active actors (all vs human)",
                       line_enc("event_date", "active_actors"),
                       (3, 0, 3, 6),
                       fields=[field("event_date"), field("active_actors"),
                               field("active_actors_human")]),
                widget("connectivity", "network", "line",
                       "Multi-repo actor share (cross-repo connectivity)",
                       line_enc("event_date", "multi_repo_actor_share"),
                       (0, 6, 3, 6),
                       fields=[field("event_date"), field("multi_repo_actor_share")]),
                widget("bus_factor", "network", "line",
                       "Single-actor repo share (bus-factor-1 proxy)",
                       line_enc("event_date", "single_actor_repo_share"),
                       (3, 6, 3, 6),
                       fields=[field("event_date"), field("single_actor_repo_share")]),
            ],
        },
        {
            "name": "forecast",
            "displayName": "Forecast",
            "layout": [
                widget("forecast_table", "forecast_eval", "table",
                       "Method leaderboard (MASE < 1 beats naive)",
                       table_enc(["target", "method", "n_predictions", "mae",
                                  "smape", "mase"]),
                       (0, 0, 3, 7),
                       fields=[field("target"), field("method"),
                               field("n_predictions"), field("mae"),
                               field("smape"), field("mase")]),
                widget("forecast_fit", "forecast_fit", "line",
                       "seasonal_24h: prediction vs actual (total events)",
                       line_enc("event_hour", "actual"),
                       (3, 0, 3, 7),
                       fields=[field("event_hour"), field("actual"),
                               field("prediction")]),
            ],
        },
    ],
}

# ------------------------------------------------------------ pipeline health

HEALTH = {
    "datasets": [
        dataset("freshness", f"""
            SELECT MAX(event_hour) AS latest_stream_hour,
                   CAST((unix_timestamp(current_timestamp())
                         - unix_timestamp(MAX(event_hour))) / 3600.0 AS DECIMAL(6,1))
                       AS hours_behind_now
            FROM {G}.ecosystem_hourly WHERE source = 'stream'"""),
        dataset("layer_counts", f"""
            SELECT 'bronze.events_raw' AS layer, COUNT(*) AS rows
            FROM github_observatory.bronze.events_raw
            UNION ALL SELECT 'silver.events', COUNT(*) FROM github_observatory.silver.events
            UNION ALL SELECT 'gold.ecosystem_hourly (stream)', COUNT(*)
            FROM {G}.ecosystem_hourly WHERE source = 'stream'
            UNION ALL SELECT 'gold.ecosystem_hourly (bigquery)', COUNT(*)
            FROM {G}.ecosystem_hourly WHERE source = 'bigquery'
            UNION ALL SELECT 'bronze.events_quarantine', COUNT(*)
            FROM github_observatory.bronze.events_quarantine"""),
        dataset("dq", f"""
            SELECT source_hour, source_file, bronze_rows, silver_rows, parity_ok,
                   quarantined, flagged, duplicate_event_ids
            FROM {G}.data_quality ORDER BY source_hour DESC LIMIT 200"""),
        dataset("dq_violations", f"""
            SELECT COUNT(*) AS violations FROM {G}.data_quality
            WHERE NOT parity_ok OR duplicate_event_ids > 0"""),
        dataset("ingest_trend", f"""
            SELECT CAST(source_hour AS DATE) AS day,
                   COUNT(*) AS files, SUM(bronze_rows) AS events
            FROM {G}.data_quality GROUP BY 1"""),
    ],
    "pages": [
        {
            "name": "health",
            "displayName": "Pipeline Health",
            "layout": [
                widget("violations", "dq_violations", "counter",
                       "Data-quality violations (must be 0)",
                       counter_enc("violations"), (0, 0, 2, 4),
                       fields=[field("violations")]),
                widget("behind", "freshness", "counter",
                       "Hours behind now (publication lag ≈ 2h is normal)",
                       counter_enc("hours_behind_now"), (2, 0, 2, 4),
                       fields=[field("latest_stream_hour"), field("hours_behind_now")]),
                widget("layers", "layer_counts", "table",
                       "Row counts by layer",
                       table_enc(["layer", "rows"]), (4, 0, 2, 4),
                       fields=[field("layer"), field("rows")]),
                widget("ingest_volume", "ingest_trend", "bar",
                       "Events ingested per day",
                       line_enc("day", "events"), (0, 4, 6, 6),
                       fields=[field("day"), field("events")]),
                widget("dq_table", "dq", "table",
                       "Per-file conservation checks (latest 200)",
                       table_enc(["source_hour", "source_file", "bronze_rows",
                                  "silver_rows", "parity_ok", "quarantined",
                                  "flagged", "duplicate_event_ids"]),
                       (0, 10, 6, 10),
                       fields=[field("source_hour"), field("source_file"),
                               field("bronze_rows"), field("silver_rows"),
                               field("parity_ok"), field("quarantined"),
                               field("flagged"), field("duplicate_event_ids")]),
            ],
        },
    ],
}

DASHBOARDS = {
    "github_observatory.lvdash.json": OBSERVATORY,
    "pipeline_health.lvdash.json": HEALTH,
}


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    for filename, definition in DASHBOARDS.items():
        path = os.path.join(OUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(definition, fh, indent=2)
        n_widgets = sum(len(p["layout"]) for p in definition["pages"])
        print(f"wrote {path}: {len(definition['datasets'])} datasets, "
              f"{len(definition['pages'])} pages, {n_widgets} widgets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
