# Metric Definitions

**Version: 2** (2026-07-30). Any change to a definition below requires a
version bump here and a note in the changelog table at the bottom;
metrics computed under different versions must not be compared silently.

These definitions bind the Gold layer. They resolve OQ-7 (production
event whitelist) and adopt the v1 assumption for OQ-2; both remain
tracked in `docs/open_questions.md`.

## Global rules

1. **Grain is event time**: metrics bucket on `date_trunc('HOUR',
   silver.events.created_at)` (UTC), not on `source_hour`. The two
   agreed on all 499,742 sampled events (0 mismatches), but event time
   is the semantically correct axis.
2. **Quality filter**: rows with `quality_flag IS NOT NULL` or
   `created_at IS NULL` are excluded from all Gold aggregates (1 of
   499,742 in the sample). The excluded population stays queryable in
   `silver.events`; nothing is deleted.
3. **Bots are included** in all counts unless a metric name carries the
   `_human` suffix, which excludes `actor_is_bot = true` rows. The bot
   flag is the `[bot]`-suffix lower bound (OQ-8); when a better
   classifier lands, its version must be recorded here.
4. **Coverage caveat (OQ-1)**: non-push event classes appear severely
   under-sampled at the source in 2026. PR/issue/release absolute
   levels are reported as *stream-observed* counts, never as ecosystem
   census numbers. Push-based metrics are the only ones treated as
   near-census.

## Production event whitelist (resolves OQ-7, v1)

An event is a **production event** iff:

| Event type | Condition | Rationale |
| --- | --- | --- |
| `PushEvent` | always | the unit of shipped work in the 2026 stream (S3) |
| `PullRequestEvent` | `payload_action IN ('opened','merged','closed','reopened')` | lifecycle transitions only |
| `IssuesEvent` | `payload_action IN ('opened','closed','reopened')` | lifecycle transitions only |
| `ReleaseEvent` | `payload_action = 'published'` | shipped artifacts |

**Excluded, deliberately**: `labeled`/`unlabeled`/`assigned`/
`unassigned`/`milestoned` and similar state-churn actions (they inflate
raw counts — labeled alone was 145 vs 223 opened in-sample); comments
and reviews (flow/collaboration, not production — step 8); Watch/Fork
(engagement); Create/Delete (branch/tag plumbing); everything else.

## `gold.ecosystem_hourly` (one row per UTC hour)

| Column | Definition |
| --- | --- |
| `event_hour` | `date_trunc('HOUR', created_at)` — primary key |
| `total_events` | all clean events in the hour (every type, bots included) |
| `total_events_human` | same, `actor_is_bot = false` |
| `push_events` | `event_type = 'PushEvent'` |
| `push_events_human` | same, `actor_is_bot = false` |
| `production_events` | whitelist above |
| `pr_opened` | `PullRequestEvent` + action `opened` |
| `pr_merged` | `PullRequestEvent` + action `merged` (Finding S2: this **is** the merge signal; `pull_request.merged` no longer exists) |
| `pr_closed_no_merge` | `PullRequestEvent` + action `closed` — **v1 assumption (OQ-2)**: `closed` and `merged` are disjoint in the 2026 stream |
| `pr_reopened` | `PullRequestEvent` + action `reopened` |
| `issues_opened` / `issues_closed` / `issues_reopened` | `IssuesEvent` + the matching action |
| `releases_published` | `ReleaseEvent` + action `published` |
| `bot_events` | clean events with `actor_is_bot = true` |
| `distinct_actors` | `COUNT(DISTINCT actor_id)` |
| `distinct_actors_human` | same, excluding bots |
| `distinct_repos` | `COUNT(DISTINCT repo_id)` |
| `distinct_push_repos` | `COUNT(DISTINCT repo_id)` over pushes only |

Rebuilds are MERGE-upserts keyed on `event_hour`: re-running over a
window recomputes and overwrites those hours (necessary because late
data for an hour changes its aggregates), and never touches hours
outside the window.

## `gold.ecosystem_velocity` (view)

Velocity is the hourly level itself; acceleration is its change against
three seasonality-aware baselines, each computed by exact-offset
self-join so **gaps yield NULL rather than wrong-lag comparisons**:

| Column | Definition |
| --- | --- |
| `*_delta_1h`, `*_pct_1h` | vs the immediately preceding hour |
| `*_delta_24h`, `*_pct_24h` | vs the same hour one day earlier |
| `*_delta_168h`, `*_pct_168h` | vs the same hour one week earlier |

applied to `total_events`, `push_events`, and `production_events`.
Percentages use `try_divide` (NULL when the baseline is 0). The 168h
comparison is the preferred acceleration signal once backfill provides
history (weekly seasonality dominates; spec §12 requires estimating
seasonality from data, not assuming it).

## `gold.flow_daily` (one row per UTC day)

Dispersion/regularity candidates over the day's hourly
`production_events` series (source: `gold.ecosystem_hourly`).
`hours_observed < 24` marks partial days — consumers must filter or
caveat them. OQ-9 (which candidate becomes the primary Flow metric)
stays open until ≥2 weeks of history exists.

| Column | Definition |
| --- | --- |
| `production_mean` / `production_std` | population mean / stddev of hourly production |
| `production_cv` | std / mean |
| `production_fano` | variance / mean |
| `production_burstiness` | (std − mean) / (std + mean), in [−1, 1] |
| `production_hourly_entropy` | Shannon entropy (nats) of hourly shares; max `ln(24)` ≈ 3.178 for a uniform day |

## `gold.contribution_daily` (one row per UTC day)

Actor-centric distribution over clean events with non-null `actor_id`.

| Column | Definition |
| --- | --- |
| `actors` / `actors_human` | distinct actors (all / non-bot) |
| `bot_event_share` | share of events by `[bot]`-suffix actors (OQ-8 lower bound) |
| `topN_actor_share` (N ∈ 1, 10, 100) | share of the day's events produced by the N most active actors |
| `events_per_actor_p50/p90/p99` | `percentile_approx` over per-actor daily event counts |
| `new_actors` | actors whose `gold.actor_first_seen.first_seen_date` is that day — **relative to ingested history**; deepening backfill reclassifies |

`gold.actor_first_seen` is an insert-or-move-earlier dimension (MERGE
updates only when an earlier event is found).

## `gold.engagement_daily` (one row per UTC day)

Stars (`WatchEvent`) and forks (`ForkEvent`) plus distinct
actor/repo breadth. **OQ-1 caveat applies with full force**: these
classes look severely under-sampled in the 2026 stream; values are
stream-observed, never census.

## `gold.data_quality` (one row per source_file)

Conservation checks across the pipeline: `bronze_rows`, `silver_rows`,
`parity_ok` (must be true — Silver flags, never drops), `quarantined`,
`flagged`, `null_created_at`, `duplicate_event_ids` (must be 0).
A false `parity_ok` or nonzero duplicates is a pipeline bug, not a data
property.

## `gold.actor_retention_daily` (one row per UTC day)

`retention_1d` = |actors active on D ∩ active on D−1| / |active on D−1|
(and the 7-day analogue). Exact-offset joins: a missing baseline day
yields NULL, never a wrong-window ratio. Human-actor retention is the
headline series (bots are near-perfectly retained by construction).

## `gold.network_daily` (one row per UTC day)

v1 actor↔repo bipartite proxies over distinct daily (actor, repo)
edges: `multi_repo_actor_share` (cross-repo connectivity),
`repos_per_actor_avg/p95`, `actors_per_repo_avg/p95`,
`single_actor_repo_share` (bus-factor-1 proxy). Full graph metrics
(components, centrality) deferred until real history exists.

## Forecasting (`gold.forecast_eval`, `gold.forecast_predictions`)

Targets: next-hour `total_events` and `push_events` (spec §16).
Methods: `naive_1h`, `seasonal_24h`, `seasonal_168h`, `moving_avg_24h` —
all gap-aware (a prediction exists only when the exact lag hour exists;
`moving_avg_24h` requires 24 contiguous trailing hours).
Metrics: MAE; sMAPE (`2|p−a|/(|p|+|a|)` averaged); MASE = MAE scaled by
`naive_1h`'s MAE over the same evaluable hours (MASE < 1 beats naive).
Evaluation is in-sample over ingested history until enough data exists
for a holdout split; `forecast_eval` rows are overwritten per
(target, method) with each run.

## Forecast targets (OQ-12)

`total_events` and `push_events` per hour are the spec §16 targets;
both are columns of `gold.ecosystem_hourly`. They are ~95% collinear in
the 2026 stream — the decision on a second, non-push target remains
open in OQ-12.

## Changelog

| Version | Date | Change |
| --- | --- | --- |
| 1 | 2026-07-30 | Initial definitions; OQ-7 whitelist v1; OQ-2 v1 assumption (`closed` = closed-without-merge) |
| 2 | 2026-07-30 | Added flow, contribution, engagement, data-quality, retention, network, and forecasting definitions (steps 8–10) |
