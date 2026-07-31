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


# ---------------------------------------------------------------- observatory

OBSERVATORY = {
    "datasets": [
        dataset("monthly_decade", f"""
            SELECT date_trunc('MONTH', event_hour) AS month, source,
                   SUM(total_events) AS events,
                   SUM(push_events) AS pushes,
                   ROUND(SUM(bot_events) / SUM(total_events), 4) AS bot_share,
                   ROUND(SUM(push_events) / SUM(total_events), 4) AS push_share,
                   CAST(AVG(distinct_actors) AS BIGINT) AS avg_hourly_actors
            FROM {G}.ecosystem_hourly GROUP BY 1, 2"""),
        dataset("hourly_recent", f"""
            SELECT event_hour, total_events, push_events, production_events,
                   bot_events, distinct_actors
            FROM {G}.ecosystem_hourly
            WHERE source = 'stream'
              AND event_hour >= current_timestamp() - INTERVAL 14 DAYS"""),
        dataset("velocity_recent", f"""
            SELECT event_hour, total_events, total_events_delta_24h,
                   ROUND(total_events_pct_24h, 4) AS total_events_pct_24h
            FROM {G}.ecosystem_velocity
            WHERE event_hour >= current_timestamp() - INTERVAL 14 DAYS"""),
        dataset("forecast_eval", f"""
            SELECT target, method, n_predictions, ROUND(mae, 1) AS mae,
                   ROUND(smape, 4) AS smape, ROUND(mase, 3) AS mase,
                   eval_start, eval_end
            FROM {G}.forecast_eval ORDER BY target, mase"""),
        dataset("forecast_fit", f"""
            SELECT event_hour, actual, prediction
            FROM {G}.forecast_predictions
            WHERE target = 'total_events' AND method = 'seasonal_24h'"""),
        dataset("contribution", f"""
            SELECT event_date, actors, new_actors,
                   ROUND(top100_actor_share, 4) AS top100_actor_share,
                   ROUND(bot_event_share, 4) AS bot_event_share
            FROM {G}.contribution_daily"""),
        dataset("retention", f"""
            SELECT event_date, active_actors, ROUND(retention_1d, 4) AS retention_1d,
                   ROUND(retention_7d, 4) AS retention_7d
            FROM {G}.actor_retention_daily"""),
        dataset("network", f"""
            SELECT event_date, ROUND(multi_repo_actor_share, 4) AS multi_repo_actor_share,
                   ROUND(single_actor_repo_share, 4) AS single_actor_repo_share
            FROM {G}.network_daily"""),
    ],
    "pages": [
        {
            "name": "decade",
            "displayName": "The Decade (2016–present)",
            "layout": [
                widget("bot_share_trend", "monthly_decade", "line",
                       "Bot share of public GitHub events — the automation curve",
                       line_enc("month", "bot_share", color="source"),
                       (0, 0, 6, 8), fields=[field("month"), field("bot_share"), field("source")]),
                widget("volume_trend", "monthly_decade", "area",
                       "Monthly event volume by source",
                       line_enc("month", "events", color="source"),
                       (0, 8, 3, 7), fields=[field("month"), field("events"), field("source")]),
                widget("push_share_trend", "monthly_decade", "line",
                       "Push share (2026 jump = OQ-1 feed filtering, not behavior)",
                       line_enc("month", "push_share", color="source"),
                       (3, 8, 3, 7), fields=[field("month"), field("push_share"), field("source")]),
                widget("actors_trend", "monthly_decade", "line",
                       "Avg distinct actors per hour",
                       line_enc("month", "avg_hourly_actors", color="source"),
                       (0, 15, 6, 6),
                       fields=[field("month"), field("avg_hourly_actors"), field("source")]),
            ],
        },
        {
            "name": "live",
            "displayName": "Live Stream & Forecast",
            "layout": [
                widget("hourly_events", "hourly_recent", "line",
                       "Hourly events (stream, last 14 days)",
                       line_enc("event_hour", "total_events"),
                       (0, 0, 6, 7),
                       fields=[field("event_hour"), field("total_events")]),
                widget("hourly_mix", "hourly_recent", "line",
                       "Pushes vs production events",
                       line_enc("event_hour", "push_events"),
                       (0, 7, 3, 6),
                       fields=[field("event_hour"), field("push_events")]),
                widget("day_over_day", "velocity_recent", "line",
                       "24h acceleration (% vs same hour yesterday)",
                       line_enc("event_hour", "total_events_pct_24h"),
                       (3, 7, 3, 6),
                       fields=[field("event_hour"), field("total_events_pct_24h")]),
                widget("forecast_table", "forecast_eval", "table",
                       "Forecast leaderboard (MASE < 1 beats naive)",
                       table_enc(["target", "method", "n_predictions", "mae",
                                  "smape", "mase"]),
                       (0, 13, 3, 7),
                       fields=[field("target"), field("method"), field("n_predictions"),
                               field("mae"), field("smape"), field("mase")]),
                widget("forecast_fit", "forecast_fit", "line",
                       "seasonal_24h: prediction vs actual (total events)",
                       line_enc("event_hour", "actual"),
                       (3, 13, 3, 7),
                       fields=[field("event_hour"), field("actual"), field("prediction")]),
            ],
        },
        {
            "name": "people",
            "displayName": "Contribution & Structure",
            "layout": [
                widget("actors_daily", "contribution", "line",
                       "Daily distinct actors (and new actors)",
                       line_enc("event_date", "actors"),
                       (0, 0, 3, 6),
                       fields=[field("event_date"), field("actors")]),
                widget("new_actors", "contribution", "bar",
                       "New actors per day (relative to ingested history)",
                       line_enc("event_date", "new_actors"),
                       (3, 0, 3, 6),
                       fields=[field("event_date"), field("new_actors")]),
                widget("concentration", "contribution", "line",
                       "Top-100 actor share of daily events",
                       line_enc("event_date", "top100_actor_share"),
                       (0, 6, 3, 6),
                       fields=[field("event_date"), field("top100_actor_share")]),
                widget("retention_trend", "retention", "line",
                       "Actor retention (1-day)",
                       line_enc("event_date", "retention_1d"),
                       (3, 6, 3, 6),
                       fields=[field("event_date"), field("retention_1d")]),
                widget("network_trend", "network", "line",
                       "Multi-repo actor share (cross-repo connectivity)",
                       line_enc("event_date", "multi_repo_actor_share"),
                       (0, 12, 6, 6),
                       fields=[field("event_date"), field("multi_repo_actor_share")]),
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
