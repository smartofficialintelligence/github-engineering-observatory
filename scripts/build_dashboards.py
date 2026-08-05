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

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from github_observatory.schema import eras  # noqa: E402

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "dashboards")

G = "github_observatory.gold"


# The comparability control.
#
# A parameter, not a column. Two earlier attempts failed differently and
# both lessons are encoded here:
#
#   * A bare string parameter has no value list, so its widget degrades to
#     a free-text box — a control nobody can discover. Fixed by sourcing
#     the options from a dataset (COMPARABILITY_MODES) and wiring the
#     filter's field encoding to it, per the query-based-dropdown pattern.
#   * A column filter renders a dropdown but always offers "All", which
#     with a two-mode union meant plotting both modes at once.
#
# Databricks sets the parameter to NULL when "All" is chosen, so every
# comparison below reads it through IFNULL and treats NULL as Standardized.
# "All" therefore degrades to the safe view rather than to nonsense, and
# the data defaults to standardized even if the widget's own default is
# ever lost.
STANDARDIZE_PARAM = "standardize"
MODE_STD = "Standardized (comparable across eras)"
MODE_RAW = "Raw (as collected)"

# Resolved mode: NULL ("All") and anything unrecognised mean Standardized.
_MODE = f"IFNULL(:{STANDARDIZE_PARAM}, '{MODE_STD}')"

STANDARDIZE_LABEL = "Comparability"

STANDARDIZE_NOTE = """\
**Comparability control.** Defaults to *Standardized*, which applies the
constraints of the most constrained era to the whole series: collection-outage
days are dropped (they are floors, not measurements), and metrics read NULL
where the signal was structurally absent rather than 0 — a zero is
indistinguishable from "it stopped happening"; and levels from 2025-06-01 on are
**scaled** onto the historical basis, which closes the artificial step where the
feed began publishing ~30% fewer events. The adjustment is applied to the recent
segment rather than to history, so the ~113 measured months stay measured and
only the ~11 affected ones become estimates. Switch to *Raw* to see the record as collected,
step and all.

Re-basing covers the 2025-06-01 step only, where the cut was measured and
uniform. It is **not** applied after 2025-10-08: the feed flaps week to week
through 2026 and offers no stable basis to index against, so lifecycle series
there are unavailable rather than adjusted. See `docs/normalization_basis.md`."""

# Static option list for the dropdown. A dataset is required — the filter
# widget takes its selectable values from a field, not from a literal.
COMPARABILITY_MODES_DATASET = "comparability_modes"


def modes_dataset() -> dict:
    return dataset(
        COMPARABILITY_MODES_DATASET,
        f"SELECT * FROM (VALUES ('{MODE_STD}'), ('{MODE_RAW}')) AS t(mode)",
    )


def dataset(name: str, query: str, *, standardizable: bool = False) -> dict:
    """A Lakeview dataset. ``standardizable`` declares the parameter so the
    query may reference :standardize."""
    d = {"name": name, "displayName": name, "queryLines": [query]}
    if standardizable:
        d["parameters"] = [{
            "displayName": STANDARDIZE_PARAM,
            "keyword": STANDARDIZE_PARAM,
            "dataType": "STRING",
            "defaultSelection": {
                "values": {"dataType": "STRING",
                           "values": [{"value": MODE_STD}]}
            },
        }]
    return d


def regime_case(ts: str = "event_hour") -> str:
    """Label each month by whether its value is measured or adjusted.

    The adjusted segment is the recent one, not the historical one. An
    earlier version scaled 113 measured months down onto a 5-month
    anchor, which reads wrong because it is wrong: it converts the
    best-established part of the record into estimates on the strength
    of the least. Adjusting the short recent segment up instead leaves
    the measured history alone.

    The third segment is split out because the post-October drift means
    a single factor is on weaker ground there — pushes rise ~53% while
    total events stay flat, so the adjustment is directionally right but
    its size is less certain.
    """
    boundary = f"DATE'{eras.SPLICE_BOUNDARY}'"
    before = f"date_trunc('MONTH', {ts}) < {boundary}"
    anchor = (f"date_trunc('MONTH', {ts}) >= {boundary} "
              f"AND CAST({ts} AS DATE) <= DATE'2025-10-08'")
    return (
        f"CASE WHEN {before} "
        f"THEN 'to May 2025 — measured' "
        f"WHEN {anchor} AND {_MODE} = '{MODE_STD}' "
        f"THEN 'Jun-Oct 2025 — scaled to the historical basis' "
        f"WHEN {anchor} "
        f"THEN 'Jun-Oct 2025 — as collected (~30% fewer published)' "
        f"WHEN {_MODE} = '{MODE_STD}' "
        f"THEN 'Oct 2025 on — scaled, but basis drifts (+53% pushes)' "
        f"ELSE 'Oct 2025 on — as collected' END"
    )


def std_rebased(metric: str, ts: str = "event_hour") -> str:
    """Metric re-based onto today's measurement footing under Standardized.

    This is what closes the visible step at 2025-06-01: without it the
    series drops ~30% at the boundary and reads as a collapse in developer
    activity, when it is the feed that changed. Raw leaves it alone so the
    break stays visible for anyone who wants the record as collected.

    Pre-boundary values become estimates. The page note says so.
    """
    rebased = eras.rebase_sql(metric, ts)
    if rebased == metric:
        return metric  # no measured factor — nothing to re-base
    return (f"CASE WHEN {_MODE} = '{MODE_STD}' THEN ({rebased}) "
            f"ELSE {metric} END")


def std_metric(metric: str, ts: str = "event_hour") -> str:
    """Blank a metric out where it is structurally absent, under
    Standardized only.

    A structural zero is worse than a gap: `pr_merged` reads 0 through
    merge_blind, which is indistinguishable from "no merges happened"
    when it means "the signal was removed". NULL draws the hole.
    """
    guarded = eras.null_outside_valid_eras_sql(metric, ts)
    if guarded == metric:
        return metric  # valid in every era, nothing to gate
    return f"CASE WHEN {_MODE} = '{MODE_STD}' THEN ({guarded}) ELSE {metric} END"


def outage_where(ts: str | None = None) -> str:
    """WHERE fragment dropping collection outages under Standardized.

    ``ts=None`` uses Gold's stored in_outage column; passing a timestamp
    derives the same predicate for sources that lack it.
    """
    col = "in_outage" if ts is None else eras.in_outage_sql(ts)
    return f"({_MODE} = '{MODE_RAW}' OR NOT {col})"


def filter_widget(name, title, dataset_names, pos):
    """Dropdown whose options come from COMPARABILITY_MODES_DATASET and
    which sets the :standardize parameter on each dataset listed."""
    queries = [
        {"name": "options",
         "query": {"datasetName": COMPARABILITY_MODES_DATASET,
                   "fields": [{"name": "mode", "expression": "`mode`"}],
                   "disaggregated": False}}
    ]
    fields = [{"fieldName": "mode", "displayName": "mode",
               "queryName": "options"}]
    for i, ds in enumerate(dataset_names):
        qn = f"param_{i}"
        queries.append({
            "name": qn,
            "query": {"datasetName": ds,
                      "parameters": [{"name": STANDARDIZE_PARAM,
                                      "keyword": STANDARDIZE_PARAM}],
                      "disaggregated": False},
        })
        fields.append({"parameterName": STANDARDIZE_PARAM, "queryName": qn})

    return {
        "widget": {
            "name": name,
            "queries": queries,
            "spec": {
                "version": 2,
                "widgetType": "filter-single-select",
                "frame": {"title": title, "showTitle": True},
                "encodings": {"fields": fields},
                "selection": {
                    "defaultSelection": {
                        "values": {"dataType": "STRING",
                                   "values": [{"value": MODE_STD}]}
                    }
                },
            },
        },
        "position": {"x": pos[0], "y": pos[1], "width": pos[2], "height": pos[3]},
    }


def widget(name, dataset_name, wtype, title, encodings, pos, fields=None):
    # Tables and counters render raw rows; aggregate mode leaves them
    # with "no fields selected".
    query = {
        "datasetName": dataset_name,
        "disaggregated": wtype in ("table", "counter"),
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
while pushes were cut less deeply (−28% in June 2025; push share rose
64% → 94%). The BigQuery mirror is faithful — an archive hour file and
its BigQuery rows match to the event (verified 2025-07-15T15) — so the
steps are in the feed itself, not the collection.
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


PAGE_NOTES = {
    "production_stream": (
        "**Stream-only tiles below (hourly grain):** `production_events`, PR and "
        "release detail are payload-derived — historical values await the "
        "**pass-2 payload backfill**. Until then these tiles cover only the "
        "stream era (2026-07→) and validate plumbing more than they measure "
        "the ecosystem."
    ),
    "flow": (
        "Flow is computed here from **census push counts**, so the full decade "
        "is available now (full-coverage days only, monthly averages of daily "
        "statistics). The gold `flow_daily` table (production-based) will "
        "extend historically after the pass-2 payload backfill. OQ-9: which "
        "candidate becomes the primary Flow metric is still open."
    ),
    "rework": (
        "**Entire page is stream-only (hourly grain):** rework signals are "
        "payload-derived — history awaits the **pass-2 payload backfill**. "
        "Counts are also thinned by the filtered feed (OQ-1): treat as "
        "plumbing validation, not ecosystem signal, until backfilled."
    ),
    "contribution_stream": (
        "**Stream-only tiles below:** concentration, per-actor percentiles, and "
        "new actors need **actor-grain history**, which the census import does "
        "not carry — unlocking them requires an actor-daily export from "
        "BigQuery (planned, not yet run). These are daily by construction "
        "(actor sets don't aggregate hourly)."
    ),
    "engagement": (
        "**Stream-only (hourly grain):** stars and forks are census-countable "
        "and their decade history is importable with a small **pass-1b export "
        "extension** — not yet run. Current data is the filtered feed (OQ-1): "
        "stream-observed counts, NOT ecosystem totals."
    ),
    "sustainability": (
        "**Stream-only:** retention and network structure need **actor-grain "
        "history** (same unlock as Contribution: an actor-daily BigQuery "
        "export). Both are inherently daily — retention is a day-over-day "
        "set comparison; with ~days of stream data these are early readings."
    ),
}


OBSERVATORY = {
    "datasets": [
        modes_dataset(),
        # ---- provenance & coverage -----------------------------------------
        dataset("coverage_monthly", f"""
            SELECT date_trunc('MONTH', event_hour) AS month, source,
                   COUNT(*) AS hours_observed
            FROM {G}.ecosystem_hourly
            WHERE {outage_where()}
            GROUP BY 1, 2""", standardizable=True),
        dataset("regime_monthly", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   ROUND(SUM({std_rebased('push_events')})
                         / SUM({std_rebased('total_events')}), 4) AS push_share,
                   {regime_case()} AS regime
            FROM {G}.ecosystem_hourly
            WHERE {outage_where()}
            GROUP BY 1, 3""", standardizable=True),
        # ---- production ----------------------------------------------------
        dataset("production_decade", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   CAST(SUM({std_rebased('push_events')}) / COUNT(*) AS BIGINT)
                       AS pushes_per_hour,
                   {regime_case()} AS regime
            FROM {G}.ecosystem_hourly
            WHERE {outage_where()}
            GROUP BY 1, 3""", standardizable=True),
        dataset("production_yoy", f"""
            WITH m AS (
                SELECT date_trunc('MONTH', event_hour) AS month,
                       SUM({std_rebased('push_events')}) / COUNT(*) AS pushes_per_hour,
                       COUNT(*) AS hours_observed
                FROM {G}.ecosystem_hourly
                WHERE {outage_where()}
                GROUP BY 1
            )
            SELECT c.month,
                   ROUND((c.pushes_per_hour - p.pushes_per_hour)
                         / p.pushes_per_hour, 4) AS yoy_growth,
                   CASE WHEN c.month < DATE'2025-06-01'
                        THEN 'full feed (thru May 2025)'
                        ELSE 'filtered feed (Jun 2025 on, OQ-1)' END AS regime
            FROM m c
            JOIN m p ON p.month = c.month - INTERVAL 12 MONTHS
            WHERE NOT (c.month >= DATE'2025-06-01' AND p.month < DATE'2025-06-01')
              -- partial months bias the comparison (day-of-week mix):
              -- require near-full coverage on both sides
              AND c.hours_observed >= 600 AND p.hours_observed >= 600""", standardizable=True),
        dataset("production_hourly_stream", f"""
            SELECT event_hour, production_events,
                   {std_metric('pr_merged')} AS pr_merged,
                   releases_published
            FROM {G}.ecosystem_hourly
            WHERE source = 'stream' AND {outage_where()}""", standardizable=True),
        # ---- flow (census-based, full decade) ------------------------------
        dataset("flow_decade", f"""
            WITH daily AS (
                SELECT CAST(event_hour AS DATE) AS day,
                       COUNT(*) AS hrs,
                       AVG(push_events) AS mean,
                       STDDEV_POP(push_events) AS std,
                       SUM(push_events) AS tot
                FROM {G}.ecosystem_hourly
                WHERE {outage_where()}
                GROUP BY 1
            ),
            entropy AS (
                SELECT CAST(e.event_hour AS DATE) AS day,
                       -SUM(CASE WHEN e.push_events > 0 AND d.tot > 0
                                 THEN (e.push_events / d.tot)
                                      * ln(e.push_events / d.tot)
                                 ELSE 0.0 END) AS ent
                FROM {G}.ecosystem_hourly e
                JOIN daily d ON CAST(e.event_hour AS DATE) = d.day
                GROUP BY 1
            )
            SELECT date_trunc('MONTH', d.day) AS month,
                   ROUND(AVG(try_divide(d.std, d.mean)), 4) AS cv,
                   ROUND(AVG(try_divide(d.std - d.mean, d.std + d.mean)), 4)
                       AS burstiness,
                   ROUND(AVG(e.ent), 4) AS entropy,
                   CASE WHEN date_trunc('MONTH', d.day) < DATE'2025-06-01'
                        THEN 'full feed (thru May 2025)'
                        ELSE 'filtered feed (Jun 2025 on, OQ-1)' END AS regime
            FROM daily d JOIN entropy e USING (day)
            WHERE d.hrs = 24
            GROUP BY 1, 5""", standardizable=True),
        # ---- rework (stream-only, hourly) ----------------------------------
        dataset("rework_hourly", f"""
            SELECT event_hour, pr_reopened, issues_reopened,
                   {std_metric('pr_closed_no_merge')} AS pr_closed_no_merge,
                   {std_metric('pr_merged')} AS pr_merged
            FROM {G}.ecosystem_hourly
            WHERE source = 'stream' AND {outage_where()}""", standardizable=True),
        dataset("review_hourly", f"""
            SELECT date_trunc('HOUR', created_at) AS event_hour, review_state,
                   COUNT(*) AS reviews
            FROM github_observatory.silver.review_events
            WHERE {outage_where('created_at')}
            GROUP BY 1, 2""", standardizable=True),
        # ---- contribution --------------------------------------------------
        dataset("bot_share_decade", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   ROUND(SUM({std_rebased('bot_events')})
                         / SUM({std_rebased('total_events')}), 4) AS bot_share,
                   {regime_case()} AS regime
            FROM {G}.ecosystem_hourly
            WHERE {outage_where()}
            GROUP BY 1, 3""", standardizable=True),
        dataset("actors_decade", f"""
            SELECT date_trunc('MONTH', event_hour) AS month,
                   CAST(AVG({std_rebased('distinct_actors')}) AS BIGINT)
                       AS avg_hourly_actors,
                   CAST(AVG({std_rebased('distinct_actors_human')}) AS BIGINT)
                       AS avg_hourly_actors_human,
                   {regime_case()} AS regime
            FROM {G}.ecosystem_hourly
            WHERE {outage_where()}
            GROUP BY 1, 4""", standardizable=True),
        dataset("contribution", f"""
            SELECT event_date, new_actors,
                   ROUND(top100_actor_share, 4) AS top100_actor_share,
                   events_per_actor_p50, events_per_actor_p90, events_per_actor_p99
            FROM {G}.contribution_daily
            WHERE {outage_where('event_date')}""", standardizable=True),
        # ---- engagement (stream-only, hourly) ------------------------------
        dataset("engagement_hourly", f"""
            SELECT date_trunc('HOUR', created_at) AS event_hour,
                   COUNT_IF(event_type = 'WatchEvent') AS stars,
                   COUNT_IF(event_type = 'ForkEvent') AS forks
            FROM github_observatory.silver.events
            WHERE quality_flag IS NULL AND created_at IS NOT NULL
              AND event_type IN ('WatchEvent', 'ForkEvent')
              AND {outage_where('created_at')}
            GROUP BY 1""", standardizable=True),
        # ---- sustainability & network --------------------------------------
        dataset("retention", f"""
            SELECT event_date, active_actors, active_actors_human,
                   ROUND(retention_1d, 4) AS retention_1d,
                   ROUND(retention_7d, 4) AS retention_7d
            FROM {G}.actor_retention_daily
            WHERE {outage_where('event_date')}""", standardizable=True),
        dataset("network", f"""
            SELECT event_date,
                   ROUND(multi_repo_actor_share, 4) AS multi_repo_actor_share,
                   ROUND(single_actor_repo_share, 4) AS single_actor_repo_share
            FROM {G}.network_daily
            WHERE {outage_where('event_date')}""", standardizable=True),
        # ---- forecasting ---------------------------------------------------
        dataset("forecast_eval", f"""
            SELECT target, method, n_predictions, ROUND(mae, 1) AS mae,
                   ROUND(smape, 4) AS smape, ROUND(mase, 3) AS mase
            FROM {G}.forecast_eval ORDER BY target, mase"""),
        dataset("forecast_fit", f"""
            SELECT event_hour, actual, prediction
            FROM {G}.forecast_predictions
            WHERE target = 'total_events' AND method = 'seasonal_24h'
              AND event_hour >= current_timestamp() - INTERVAL 30 DAYS
              AND {outage_where('event_hour')}""", standardizable=True),
    ],
    "pages": [
        {
            "name": "provenance",
            "displayName": "Read Me First — Data & Regimes",
            "layout": [
                text_widget("regime_note", REGIME_NOTE, (0, 0, 6, 8)),
                widget("push_share", "regime_monthly", "line",
                       "Push share — the feed-filtering signature (OQ-1)",
                       line_enc("month", "push_share", color="regime"),
                       (0, 8, 3, 7),
                       fields=[field("month"), field("push_share"), field("regime")]),
                filter_widget("std_provenance", STANDARDIZE_LABEL,
                              ["coverage_monthly", "regime_monthly"],
                              (0, 15, 3, 2)),
                widget("coverage", "coverage_monthly", "bar",
                       "Hours observed per month, by source (gaps = archive outages)",
                       line_enc("month", "hours_observed", color="source"),
                       (3, 8, 3, 7),
                       fields=[field("month"), field("hours_observed"), field("source")]),
            ],
        },
        {
            "name": "production",
            "displayName": "Production",
            "layout": [
                filter_widget("std_production", STANDARDIZE_LABEL,
                              ["production_decade", "production_yoy",
                               "production_hourly_stream"], (0, 0, 3, 2)),
                text_widget("std_production_note", STANDARDIZE_NOTE, (3, 0, 3, 2)),
                widget("pushes_decade", "production_decade", "line",
                       "Pushes per hour, monthly avg — the production unit, 2020-present",
                       line_enc("month", "pushes_per_hour", color="regime"),
                       (0, 0, 3, 7),
                       fields=[field("month"), field("pushes_per_hour"), field("regime")]),
                widget("prod_accel", "production_yoy", "bar",
                       "Acceleration: YoY growth of pushes/hour (cross-regime excluded)",
                       line_enc("month", "yoy_growth", color="regime"),
                       (3, 0, 3, 7),
                       fields=[field("month"), field("yoy_growth"), field("regime")]),
                text_widget("prod_note", PAGE_NOTES["production_stream"], (0, 7, 6, 2)),
                widget("prod_hourly", "production_hourly_stream", "line",
                       "Production events per hour (stream era)",
                       line_enc("event_hour", "production_events"),
                       (0, 9, 3, 6),
                       fields=[field("event_hour"), field("production_events")]),
                widget("lifecycle_hourly", "production_hourly_stream", "line",
                       "PR merges per hour (stream era)",
                       line_enc("event_hour", "pr_merged"),
                       (3, 9, 3, 6),
                       fields=[field("event_hour"), field("pr_merged")]),
            ],
        },
        {
            "name": "flow",
            "displayName": "Flow",
            "layout": [
                filter_widget("std_flow", STANDARDIZE_LABEL,
                              ["flow_decade"], (0, 0, 3, 2)),
                text_widget("flow_note", PAGE_NOTES["flow"], (0, 0, 6, 2)),
                widget("flow_burstiness", "flow_decade", "line",
                       "Burstiness of hourly pushes — monthly avg of daily values",
                       line_enc("month", "burstiness", color="regime"),
                       (0, 2, 3, 7),
                       fields=[field("month"), field("burstiness"), field("regime")]),
                widget("flow_entropy", "flow_decade", "line",
                       "Hourly entropy, nats (ln 24 = 3.18 is uniform)",
                       line_enc("month", "entropy", color="regime"),
                       (3, 2, 3, 7),
                       fields=[field("month"), field("entropy"), field("regime")]),
                widget("flow_cv", "flow_decade", "line",
                       "Coefficient of variation of hourly pushes",
                       line_enc("month", "cv", color="regime"),
                       (0, 9, 6, 6),
                       fields=[field("month"), field("cv"), field("regime")]),
            ],
        },
        {
            "name": "rework",
            "displayName": "Rework",
            "layout": [
                filter_widget("std_rework", STANDARDIZE_LABEL,
                              ["rework_hourly", "review_hourly"], (0, 0, 3, 2)),
                text_widget("rework_note", PAGE_NOTES["rework"], (0, 0, 6, 2)),
                widget("reopens_hourly", "rework_hourly", "line",
                       "Reopened PRs and issues per hour",
                       line_enc("event_hour", "pr_reopened"),
                       (0, 2, 3, 6),
                       fields=[field("event_hour"), field("pr_reopened"),
                               field("issues_reopened")]),
                widget("closures_hourly", "rework_hourly", "line",
                       "PR closures per hour: merged vs closed-without-merge (OQ-2)",
                       line_enc("event_hour", "pr_merged"),
                       (3, 2, 3, 6),
                       fields=[field("event_hour"), field("pr_merged"),
                               field("pr_closed_no_merge")]),
                widget("review_outcomes", "review_hourly", "bar",
                       "Review outcomes per hour (dismissed = rework proxy)",
                       line_enc("event_hour", "reviews", color="review_state"),
                       (0, 8, 6, 6),
                       fields=[field("event_hour"), field("reviews"),
                               field("review_state")]),
            ],
        },
        {
            "name": "contribution",
            "displayName": "Contribution",
            "layout": [
                filter_widget("std_contribution", STANDARDIZE_LABEL,
                              ["bot_share_decade", "actors_decade",
                               "contribution"], (0, 0, 3, 2)),
                text_widget("std_contribution_note", STANDARDIZE_NOTE, (3, 0, 3, 2)),
                widget("bot_curve", "bot_share_decade", "line",
                       "Bot share of all events — the automation curve, 2020-present",
                       line_enc("month", "bot_share", color="regime"),
                       (0, 0, 3, 7),
                       fields=[field("month"), field("bot_share"), field("regime")]),
                widget("actors_curve", "actors_decade", "line",
                       "Avg distinct actors per hour, 2020-present (all vs human)",
                       line_enc("month", "avg_hourly_actors", color="regime"),
                       (3, 0, 3, 7),
                       fields=[field("month"), field("avg_hourly_actors"),
                               field("avg_hourly_actors_human"), field("regime")]),
                text_widget("contrib_note", PAGE_NOTES["contribution_stream"], (0, 7, 6, 2)),
                widget("concentration", "contribution", "line",
                       "Top-100 actor share of daily events (stream era)",
                       line_enc("event_date", "top100_actor_share"),
                       (0, 9, 3, 6),
                       fields=[field("event_date"), field("top100_actor_share")]),
                widget("new_actors", "contribution", "bar",
                       "New actors per day, relative to ingested history (stream era)",
                       line_enc("event_date", "new_actors"),
                       (3, 9, 3, 6),
                       fields=[field("event_date"), field("new_actors")]),
            ],
        },
        {
            "name": "engagement",
            "displayName": "Engagement (coverage-caveated)",
            "layout": [
                filter_widget("std_engagement", STANDARDIZE_LABEL,
                              ["engagement_hourly"], (0, 0, 3, 2)),
                text_widget("engage_note", PAGE_NOTES["engagement"], (0, 0, 6, 2)),
                widget("stars_hourly", "engagement_hourly", "line",
                       "Stars per hour (stream-observed, NOT census)",
                       line_enc("event_hour", "stars"),
                       (0, 2, 3, 6),
                       fields=[field("event_hour"), field("stars")]),
                widget("forks_hourly", "engagement_hourly", "line",
                       "Forks per hour (stream-observed, NOT census)",
                       line_enc("event_hour", "forks"),
                       (3, 2, 3, 6),
                       fields=[field("event_hour"), field("forks")]),
            ],
        },
        {
            "name": "sustainability",
            "displayName": "Sustainability & Network",
            "layout": [
                filter_widget("std_sustain", STANDARDIZE_LABEL,
                              ["retention", "network"], (0, 0, 3, 2)),
                text_widget("sustain_note", PAGE_NOTES["sustainability"], (0, 0, 6, 2)),
                widget("retention_trend", "retention", "line",
                       "Actor retention: 1-day and 7-day (stream era)",
                       line_enc("event_date", "retention_1d"),
                       (0, 2, 3, 6),
                       fields=[field("event_date"), field("retention_1d"),
                               field("retention_7d")]),
                widget("connectivity", "network", "line",
                       "Multi-repo actor share / single-actor repo share (stream era)",
                       line_enc("event_date", "multi_repo_actor_share"),
                       (3, 2, 3, 6),
                       fields=[field("event_date"), field("multi_repo_actor_share"),
                               field("single_actor_repo_share")]),
            ],
        },
        {
            "name": "forecast",
            "displayName": "Forecast",
            "layout": [
                filter_widget("std_forecast", STANDARDIZE_LABEL,
                              ["forecast_fit"], (0, 0, 3, 2)),
                widget("forecast_table", "forecast_eval", "table",
                       "Method leaderboard (MASE < 1 beats naive)",
                       table_enc(["target", "method", "n_predictions", "mae",
                                  "smape", "mase"]),
                       (0, 0, 3, 7),
                       fields=[field("target"), field("method"),
                               field("n_predictions"), field("mae"),
                               field("smape"), field("mase")]),
                widget("forecast_fit", "forecast_fit", "line",
                       "seasonal_24h: prediction vs actual, hourly (total events)",
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
            SELECT source_hour AS event_hour,
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
                       "Events ingested per hour",
                       line_enc("event_hour", "events"), (0, 4, 6, 6),
                       fields=[field("event_hour"), field("events")]),
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


def reserve_header_band(definition: dict) -> None:
    """Move the standardize control to the top of its page and push the
    page's own tiles down to make room.

    Done as a post-pass so page layouts stay written in their natural
    coordinates; adding a control never requires renumbering tiles by
    hand, which is how the first attempt produced overlaps.
    """
    for page in definition["pages"]:
        band = [w for w in page["layout"]
                if w["widget"]["name"].startswith("std_")]
        if not band:
            continue
        height = max(w["position"]["height"] for w in band)
        for w in page["layout"]:
            if w in band:
                w["position"]["y"] = 0
            else:
                w["position"]["y"] += height


def assert_no_overlaps(definition: dict, filename: str) -> None:
    """Fail the build on overlapping tiles — Lakeview renders them stacked
    and the damage is easy to miss in a screenshot."""
    def hits(a, b):
        return not (a["x"] + a["width"] <= b["x"] or b["x"] + b["width"] <= a["x"]
                    or a["y"] + a["height"] <= b["y"] or b["y"] + b["height"] <= a["y"])
    for page in definition["pages"]:
        items = [(w["widget"]["name"], w["position"]) for w in page["layout"]]
        for i in range(len(items)):
            for j in range(i + 1, len(items)):
                if hits(items[i][1], items[j][1]):
                    raise SystemExit(
                        f"{filename} page '{page['displayName']}': "
                        f"{items[i][0]} overlaps {items[j][0]}"
                    )


def assert_control_wiring(definition: dict, filename: str) -> None:
    """Check the two ways this control has already broken.

    A dataset referencing :standardize but not declaring it fails only at
    render time with an unbound-parameter error. A filter with no options
    query renders as a free-text box — present in the JSON, undiscoverable
    in the UI, which is exactly how the first version shipped.
    """
    for ds in definition["datasets"]:
        sql = " ".join(ds["queryLines"])
        uses = f":{STANDARDIZE_PARAM}" in sql
        declares = any(p["keyword"] == STANDARDIZE_PARAM
                       for p in ds.get("parameters", []))
        if uses != declares:
            raise SystemExit(
                f"{filename}: dataset '{ds['name']}' uses={uses} "
                f"declares={declares} for :{STANDARDIZE_PARAM}"
            )

    names = {ds["name"] for ds in definition["datasets"]}
    for page in definition["pages"]:
        for w in page["layout"]:
            spec = w["widget"].get("spec", {})
            if spec.get("widgetType") != "filter-single-select":
                continue
            fields = spec["encodings"]["fields"]
            if not any("fieldName" in f for f in fields):
                raise SystemExit(
                    f"{filename}: filter '{w['widget']['name']}' has no "
                    f"options field — it will render as a text box"
                )
            if COMPARABILITY_MODES_DATASET not in names:
                raise SystemExit(
                    f"{filename}: '{COMPARABILITY_MODES_DATASET}' dataset "
                    f"is missing; the dropdown has no values to offer"
                )
            if not any("parameterName" in f for f in fields):
                raise SystemExit(
                    f"{filename}: filter '{w['widget']['name']}' sets no "
                    f"parameter — it would filter nothing"
                )


def main() -> int:
    os.makedirs(OUT_DIR, exist_ok=True)
    for filename, definition in DASHBOARDS.items():
        reserve_header_band(definition)
        assert_no_overlaps(definition, filename)
        assert_control_wiring(definition, filename)
        path = os.path.join(OUT_DIR, filename)
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(definition, fh, indent=2)
        n_widgets = sum(len(p["layout"]) for p in definition["pages"])
        print(f"wrote {path}: {len(definition['datasets'])} datasets, "
              f"{len(definition['pages'])} pages, {n_widgets} widgets")
    return 0


if __name__ == "__main__":
    sys.exit(main())
