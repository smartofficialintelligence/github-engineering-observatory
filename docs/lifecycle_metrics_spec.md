# Lifecycle Metrics — Pass-2 Backfill Spec

**Version 2** (2026-08-02). Adds `pr_merge_signal_stripped` batch-detected
data-quality flag (see §Era-awareness). v1 was 2026-08-01 initial spec. Governs the decade-scale (2016–2025) payload-derived
hourly aggregates produced by `pipelines/gcp_lifecycle_backfill/`. Every column
here binds a `github_observatory.gold.ecosystem_hourly` column of the same name;
stream-computed rows (produced from `silver.events` — see
[`src/github_observatory/gold/metrics.py`](../src/github_observatory/gold/metrics.py))
and backfilled rows (produced by the Spark job below) must be numerically
comparable hour-for-hour where both eras overlap, so the two derivations use
matching block-out logic (below) and matching column semantics.

Pairs with `docs/metric_definitions.md` (v4): that document is the metric
contract, this document is the *implementation* of that contract for one
source (`githubarchive.year.*`). Any change to the definitions there requires
a corresponding change here and a version bump in both.

## Motivation

Pass-1 (`bq_export_hourly.py`, committed) imported the *census columns* —
everything derivable from top-level fields — as SQL against
`githubarchive.year.YYYY`. It cost ~$30 total because the top-level columns
are small.

The *payload-derived columns* (PR/issues/releases lifecycle, plus the
Rework/Engagement metrics below) require reading the JSON `payload` column,
which is ~30 TB across the decade. A pure BigQuery SQL scan would bill ~$190
one-shot; a sampled version (1 day/week) would cost ~$27 but permanently
introduce a coverage regime alongside the already-tricky OQ-1 boundary.

**Decision:** extract via **Dataproc Serverless Spark** reading BigQuery
through the **Storage Read API** — the 300 TiB/month per-billing-account free
tier absorbs the read, so only Dataproc DCU-hours are billed. This produces
*exact* hourly aggregates for every payload-derived metric we might want,
in one scan, without a permanent sampling caveat. Runs are one-off backfills
(the 2026-forward stream keeps producing these columns natively).

See `pipelines/gcp_lifecycle_backfill/README.md` for the runbook.

## Block-out logic — must match stream Silver→Gold exactly

The stream pipeline flags rows in Silver and excludes flagged rows in Gold.
Pass-2 must reproduce **the same set** of surviving events.

Stream (`silver.transforms.events_merge_sql`):

```text
quality_flag =
    'non_public'          if public = FALSE
    'missing_repo_id'     if repo.id IS NULL
    'invalid_created_at'  if TRY_CAST(created_at AS TIMESTAMP) IS NULL
    NULL                  otherwise (survives)
```

Then `gold.metrics` selects `WHERE quality_flag IS NULL AND created_at IS NOT NULL`.

Pass-2 equivalent — **applied inside the Spark job** on the BigQuery source
after Storage Read:

```sql
WHERE public
  AND repo.id IS NOT NULL
  AND created_at IS NOT NULL
```

The pass-1 CSV export used the same WHERE clause; it produced counts that
match stream Gold to the event across 72 overlap hours (validated 2026-07-27→29,
per `metric_definitions.md` §Sources).

## Dedup rule — must match

`githubarchive` contains duplicate events; stream Gold uses
`COUNT(*)` over Silver, which is de-duplicated on `event_id` (Bronze MERGE).
Pass-2 counterpart: **every count is `COUNT(DISTINCT id)`**, matching the
pass-1 export convention.

Verified during pass-1: hour 2026-07-27T20 had 168,265 raw rows / 168,233
distinct ids in `githubarchive` vs 168,233 rows in stream Silver. Dedupe
alignment is non-negotiable.

## Grain — event-time, not source-hour

Bucket on `TIMESTAMP_TRUNC(created_at, HOUR)`. Matches stream Gold's
`date_trunc('HOUR', silver.events.created_at)`.

## Era-awareness

Two payload-shape changes cross the backfill window; the extractions handle
both eras in one expression:

**PR merged signal (OQ-5).** Three eras, one expression:

* **Pre-June-2025**: `merged` was `action='closed' AND pull_request.merged=true`.
* **June-2025 → ~2026 restoration**: **archive strips BOTH signals**. The
  `pull_request` object on `PullRequestEvent` is slimmed to 5 fields (`id`,
  `number`, `url`, `base`, `head`) with no `merged`/`merged_at`/`merged_by`
  and no `action='merged'` events. Confirmed 2026-08-02 via BQ audit of
  `githubarchive.day.20251115`: 211,311 PR events, 0 with `merged`
  action, 0 with `pull_request.merged` field present.
* **2026 stream (2026-07 onward)**: `action='merged'` restored as a
  dedicated action value; `pull_request.merged` still absent.

The era-aware expression
`(action='merged' OR (action='closed' AND merged=true))` handles the
first and third eras correctly but returns **zero** during the
signal-stripped middle era — because both branches are dead in that data,
not because no merges happened. `pr_closed_no_merge` correspondingly
**over-counts** during that era (all `action='closed'` fall into it,
including former merges).

Rather than hardcode a date boundary, the Spark job **probes each batch**
and emits a per-row boolean `pr_merge_signal_stripped`:

* Batch total `pr_events_total > 10,000` AND batch total `pr_merged = 0`
  → `pr_merge_signal_stripped = TRUE` for every row in the batch.
* Otherwise → `pr_merge_signal_stripped = FALSE`.

Consumer contract: filter `WHERE NOT pr_merge_signal_stripped` before
using `pr_merged` or `pr_closed_no_merge`. All other columns remain
valid in the stripped era (issue lifecycle, review states, comments,
releases, census counts are unaffected). See `metric_definitions.md`
`pr_merged` / `pr_closed_no_merge` rows for the contract; OQ-2
caveats the disjointness assumption for eras 1 and 3.

**Issue closure reason.** `payload.issue.state_reason` was introduced by
GitHub in **September 2022** — pre-2022 issue closures will all have
`state_reason IS NULL` and land in `issues_closed_unknown`. This is a real
data property, not a bug. Consumers should treat `issues_closed_unknown` as
"unclassified pre-Sep-2022 + explicit-null post" and compare shares within
the state_reason-available era for taxonomy analysis.

Coverage note: the OQ-1 filter (June/October 2025 progressive steps) affects
all payload-derived counts for post-June-2025 hours in the same way it
affects push counts today. Historical era (pre-June-2025) is trustworthy;
compare across the color boundary only with the coverage caveat that
`metric_definitions.md` §Coverage documents.

## Output columns

**Census columns** — already produced by pass-1; re-emitted here so that a
one-shot pass-2 run can also refresh them if desired (skip via job flag
`--census=false` for pure lifecycle re-runs).

Same 10 columns as pass-1 (`total_events`, `total_events_human`, `push_events`,
`push_events_human`, `bot_events`, `distinct_actors`, `distinct_actors_human`,
`distinct_repos`, `distinct_push_repos`, plus grain). Definitions unchanged.

**Payload-derived columns** — the reason for this backfill. Every column is
an aggregate over hourly-grain rows filtered by the block-out logic above.

Actor bot flag is defined as `IFNULL(ENDS_WITH(actor.login, '[bot]'), FALSE)`
in all `_human` variants — matches Silver's `endswith(login, '[bot]')`
convention.

### Lifecycle transitions (contract with stream Gold — direct match)

| Column | Expression | Notes |
| --- | --- | --- |
| `production_events` | `id` where OQ-7 whitelist matches (era-aware for PR merged) | see whitelist below |
| `pr_opened` | `type='PullRequestEvent' AND action='opened'` | |
| `pr_merged` | `type='PullRequestEvent' AND (action='merged' OR (action='closed' AND payload.pull_request.merged='true'))` | era-aware |
| `pr_closed_no_merge` | `type='PullRequestEvent' AND action='closed' AND IFNULL(payload.pull_request.merged,'false') != 'true'` | era-aware |
| `pr_reopened` | `type='PullRequestEvent' AND action='reopened'` | |
| `issues_opened` | `type='IssuesEvent' AND action='opened'` | |
| `issues_closed` | `type='IssuesEvent' AND action='closed'` | |
| `issues_reopened` | `type='IssuesEvent' AND action='reopened'` | |
| `releases_published` | `type='ReleaseEvent' AND action='published'` | |

### Lifecycle denominators (new — needed to compute state-churn ratios)

| Column | Expression | Notes |
| --- | --- | --- |
| `pr_events_total` | `type='PullRequestEvent'` (all actions) | state_churn = total − (opened+merged+closed_no_merge+reopened) |
| `issues_events_total` | `type='IssuesEvent'` (all actions) | same shape |

### Issue closure taxonomy (new — Rework signal)

For rows where `type='IssuesEvent' AND action='closed'`, split by
`payload.issue.state_reason`:

| Column | state_reason value | Notes |
| --- | --- | --- |
| `issues_closed_completed` | `'completed'` | intended resolution |
| `issues_closed_not_planned` | `'not_planned'` | won't-fix |
| `issues_closed_duplicate` | `'duplicate'` | consolidation |
| `issues_closed_unknown` | NULL or any other value | includes all pre-Sep-2022 closures |

Sum equals `issues_closed`; check as a validation assertion.

### Review states (new — Rework signal, per user brief "the real signal")

For rows where `type='PullRequestReviewEvent'`, split by `payload.review.state`:

| Column | state |
| --- | --- |
| `reviews_approved` | `'approved'` |
| `reviews_changes_requested` | `'changes_requested'` |
| `reviews_commented` | `'commented'` |
| `reviews_dismissed` | `'dismissed'` |

Sum equals total `PullRequestReviewEvent` count. Historical presence: review
state has been in the payload since PullRequestReviewEvent shipped (Aug 2016).

### Comment activity (new — collaboration flow)

Three distinct classes, kept separate because they measure different things:

| Column | Expression | Notes |
| --- | --- | --- |
| `issue_comments_true` | `type='IssueCommentEvent' AND payload.issue.pull_request IS NULL` | comment on a real issue |
| `issue_comments_on_prs` | `type='IssueCommentEvent' AND payload.issue.pull_request IS NOT NULL` | discussion comment on a PR (per §4 of schema report, 67.6% of IssueCommentEvent in-sample) |
| `pr_review_comments` | `type='PullRequestReviewCommentEvent'` | inline code-review comment |
| `commit_comments` | `type='CommitCommentEvent'` | commit-attached comment |

### Release adoption (new — the one genuine in-stream adoption signal)

| Column | Expression | Notes |
| --- | --- | --- |
| `release_download_count_sum` | `SUM(<aggregated assets download_count>)` over `type='ReleaseEvent'` rows | assets shape: `ARRAY<STRUCT<download_count: BIGINT, ...>>`; sum across the array per event, then across the hour. Uses `from_json` with a partial schema (matches `silver.release_events.assets_download_count`) |

Coverage caveat: many older releases carry `download_count=0` at
`published` time; the metric is most useful as a Q-over-Q or Y-over-Y trend
within recent eras, not as an absolute count. Track OQ-10.

### OQ-7 production whitelist — era-aware form

```sql
(type = 'PushEvent')
OR (type = 'PullRequestEvent'
    AND (action IN ('opened', 'reopened')
         OR (action = 'merged')                               -- 2026+ merge
         OR (action = 'closed'                                -- pre-2026 merge
             AND payload.pull_request.merged = 'true')
         OR (action = 'closed'                                -- close (either era)
             AND IFNULL(payload.pull_request.merged, 'false') != 'true')))
OR (type = 'IssuesEvent'  AND action IN ('opened', 'closed', 'reopened'))
OR (type = 'ReleaseEvent' AND action = 'published')
```

Note the four PR clauses collapse to
`action IN ('opened','reopened','merged','closed')` for the 2026 stream —
the pre-2026 branches add zero rows there. This form matches
`metric_definitions.md` §Production Event Whitelist under both eras.

## Column set summary

- Grain: `event_hour` (1 col)
- Census (already in pass-1): 9 cols
- Lifecycle transitions (contract with stream): 9 cols
- Lifecycle denominators (new): 2 cols
- Issue closure taxonomy (new): 4 cols
- Review states (new): 4 cols
- Comment activity (new): 4 cols
- Release adoption (new): 1 col

**Total: 34 columns per hourly row** (plus one data-quality flag:
`pr_merge_signal_stripped BOOLEAN`, and provenance columns: `source_year`,
`ingested_at`, `job_id`, `metric_spec_version`).

## Follow-up: stream pipeline alignment

The stream Silver→Gold pipeline in
[`src/github_observatory/silver/transforms.py`](../src/github_observatory/silver/transforms.py)
and [`src/github_observatory/gold/metrics.py`](../src/github_observatory/gold/metrics.py)
today produces only the first 18 columns above (census + lifecycle
transitions). The 15 new columns (denominators, closure taxonomy, review
states, comments, downloads) must be added to the stream Gold aggregation
before backfilled rows can be merged into the same table without leaving
stream rows NULL for those columns.

This spec commits to that plan; the stream update is scoped as
**Phase C step 4** (below). Until then, the pass-2 output lives in a new
table `bronze.bq_lifecycle_hourly` mirroring pass-1's
`bronze.bq_ecosystem_hourly`, and is only merged into `gold.ecosystem_hourly`
after the schema catches up. This avoids a partial-column gold row.

## Validation contract

The pass-2 output for a bench month must satisfy:

1. **Row count.** Number of hourly rows == 24 × days-in-month
   (2016-02 has 24×29; 2020-02 has 24×29; etc.). Missing hours are a bug
   in the extract, not a data property (the archive has 100% hour coverage
   per `open_questions.md` OQ-1 update).
2. **Census self-consistency.** `total_events == pushes + non_pushes` where
   non_pushes is derived from the same source and hour; validated against
   pass-1's existing `bronze.bq_ecosystem_hourly` rows for the same year
   — all census columns must match to the event.
3. **Whitelist consistency.**
   `production_events == pushes + (pr_opened+pr_merged+pr_closed_no_merge+pr_reopened) + (issues_opened+issues_closed+issues_reopened) + releases_published`.
4. **Closure taxonomy sum.** `issues_closed_completed + _not_planned + _duplicate + _unknown == issues_closed` for every hour.
5. **Review states sum.** `reviews_approved + _changes_requested + _commented + _dismissed == pr_review_events_total` (implied — spec allows adding this denominator if simpler).
6. **BigQuery SQL agreement.** Run the same aggregations in BigQuery SQL
   over the same month (see `validate.py`); every column, every hour, must
   agree exactly. This is the acceptance criterion for the whole pipeline.

Any check failing on the bench month blocks the full backfill.

## Changelog

| Version | Date | Change |
| --- | --- | --- |
| 1 | 2026-08-01 | Initial spec; 34-column output; block-out logic mirrors stream Silver/Gold; era-aware PR merge; issue state_reason; review states; three comment classes; release download sum |
| 2 | 2026-08-02 | Added `pr_merge_signal_stripped` BOOLEAN per-row flag. Middle era (June-2025 → 2026 restoration) confirmed by BQ audit to strip ALL PR merge signals from the archive, making `pr_merged`/`pr_closed_no_merge` structurally unrecoverable for that window. Batch-level probe emits the flag; consumers filter `WHERE NOT pr_merge_signal_stripped` for those two columns. `METRIC_SPEC_VERSION = 2`. |
