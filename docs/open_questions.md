# Open Questions

Unresolved assumptions and empirical questions, tracked per spec §19 rule 8.
Each entry states what is unknown, why it matters, and how it will be
resolved. Resolved items move to the bottom with their answer.

---

## OQ-1 — Is the 2026 events stream filtered or sampled upstream?

Non-push event classes are implausibly rare in the sampled hours (stars
~44/hour, PRs ~189/hour globally, versus thousands/hour historically), while
pushes run at ~160k/hour from 71k+ distinct actors. Either public GitHub
activity collapsed for those classes (implausible) or GitHub's public events
feed / GH Archive collection changed after 2025.

*Why it matters:* Engagement, Contribution, Rework, and Sustainability metrics
would measure a **sample of unknown coverage**, not the ecosystem. Absolute
levels and any cross-class ratios would be biased.

*Resolution path:* pick ~20 high-traffic repos; compare WatchEvent/ForkEvent
counts over a fixed window against stargazer/fork deltas from the GitHub REST
API; review GitHub changelog / GH Archive issue tracker for feed changes.
Until resolved, publish non-push metrics with a coverage caveat.

*Dating evidence (2026-07-31, from the BigQuery decade import):* the
filtering rolled out progressively through 2025, not at the 2026
boundary — total volume stepped down ~26% in **June 2025** (231k→150k
events/hr from the March peak) and again in **October 2025** (121k),
while push share climbed 64% → 70% → 94% (2026 stream). Monthly archive
coverage was complete throughout, ruling out collection gaps. Pushes
were not spared: pushes/hour fell ~28% in the June 2025 step
(129k → 93k), then measured 156k/hr in the 2026 stream — the filter's
composition evolved over time, so even within-filtered-era comparisons
need care. The BigQuery mirror is faithful to the archive: hour
2025-07-15T15 matches to the event (164,140 events / 98,194 pushes in
both), so the steps are upstream of all collection.

*Payload stripping in the filtered era (2026-08-02, from the pass-2
lifecycle bench and follow-up BQ audit of `githubarchive.day.20251115`):*
the filter strips more than event counts — it slims the surviving
PullRequestEvent payload to five fields (`id`, `number`, `url`, `base`,
`head`) with no `merged`, `merged_at`, `merged_by`, `state`, `closed_at`,
or `draft`, AND drops `action='merged'` events entirely (0 of 211,311
PR events on 2025-11-15). The pre-2026 merge signal
(`action='closed' AND pull_request.merged=true`) is therefore
unrecoverable for the June-2025-through-2026-restoration window: both
branches of our era-aware detector are dead. `MergeGroupEvent` is not
present in the archive to compensate. Pass-2 flags affected batches
with `pr_merge_signal_stripped=TRUE` (spec §Era-awareness); consumers
must filter that flag before trusting `pr_merged`/`pr_closed_no_merge`
for those hours. Related enrichment paths (GitHub REST API for a
sampled repo panel; PushEvent SHA correlation) are open — tracked in
OQ-10 as adoption-signal enrichment.

*Restoration date unknown:* the 2026 stream samples (2026-07-26, -29)
show `action='merged'` present again (172 in three hours), so the
filter's merge stripping was reversed between 2025-11-15 and 2026-07-26.
The exact restoration date matters for narrowing the NULL era; a small
BQ probe on `githubarchive.day.202603XX` / `.202604XX` (<$0.02 each)
can pin it down.

*Boundary dates pinned (2026-08-02, via
`pipelines/gcp_lifecycle_backfill/pin_signal_boundaries.py` binary
search over `githubarchive.day.*`, total spend $0.86, log at
`artifacts/signal_probes.jsonl`):*

* **Onset**: signal was present on **2025-10-08** (81,376 merges
  detectable via `pull_request.merged=true`) and absent on
  **2025-10-09** (0 detectable). Sharp overnight switch.
* **Restoration**: signal was absent on **2025-12-01** (0 detectable via
  either signal) and present on **2025-12-02** (11,871 merges via the
  new synthetic `action='merged'`). Sharp overnight switch.

**Actual stripped window: 2025-10-09 through 2025-12-01 = 54 days**,
not the "since June 2025" our earlier bench had implied. The volume
step in October 2025 (documented earlier in this OQ entry) is likely
the same upstream event as this signal-stripping — filter composition
was upgraded on Oct 9, then reverted on Dec 2 with a new synthetic
merge action tag replacing the historical `pull_request.merged=true`.
Post-restoration, `action='merged'` is the sole merge signal
(`pull_request.merged` field remains stripped from the slimmed payload).

The pass-2 pipeline's `pr_merge_signal_stripped` flag will therefore
fire only for batches spanning 2025-10-09 → 2025-12-01. Consumers can
now use the exact 54-day window directly rather than treating
"post-June-2025" as suspect.

*Decade schema map (2026-08-02, from `pipelines/schema_drift/`):* the
2025-10-09 boundary is far larger than a PR-merge event. Monthly schema
sampling across 139 hours (2015-01 → 2026-07) shows **831 fields removed
in that single event** — 292 from `PullRequestReviewEvent`, 269 from
`PullRequestEvent`, 258 from `PullRequestReviewCommentEvent`, 11 from
`PushEvent` — and that accounts for *every* field removal in the decade.
The preceding ten years only added fields (182 in total). `payload.commits`
went with it and, unlike the merge signal, never returned: commit-level
metrics are available 2015-01-01 → 2025-10-08 and permanently unavailable
after. Full evidence in `docs/schema_drift_timeline.md`; the comparability
rules that follow from it in `docs/normalization_basis.md`.

*Collection outages are a third, separate distortion (2026-08-02):* the
archive itself failed seven times for 42 days total, independently of any
filtering. Longest is **2021-10-06 → 10-29** (24 days, worst day 1.7% of
normal, three day-tables absent), which means any 2021 annual aggregate is
materially short — 2021 carries four separate incidents. The 2025-10-09 →
10-14 outage coincides exactly with the payload collapse, suggesting one
upstream incident caused both. Full list in `docs/normalization_basis.md`;
regenerate with `pipelines/schema_drift/find_outages.py` (free). This also
retracts a method used earlier in this document's investigation: hourly
probing cannot establish a boundary that falls inside an outage, because a
near-empty file reads as "field removed". Only full-day aggregates have the
denominator to support such a verdict.

## OQ-2 — Does PullRequestEvent `action='closed'` now mean closed-without-merge?

2026 introduces `action='merged'` (172 in sample) alongside `closed` (12);
`pull_request.merged` is gone. The clean interpretation is merged/closed
are disjoint, but 3 hours is thin evidence.

*Resolution path:* profile more hours; for a sample of `closed` PRs, check
`issue.pull_request.merged_at` on later IssueCommentEvents or spot-check via
API. `docs/metric_definitions.md` v1 adopts the disjoint interpretation
(`pr_closed_no_merge` counts action `closed`); confirming or refuting it
requires a metric-definitions version bump.

## OQ-3 — Why do `push_id` values repeat? (28 repeats in 477,433 pushes)

Distinct event `id`s share a `push_id`. Same push delivered twice, or a
retried delivery? Inspect the pairs: identical `repo`/`ref`/`before`/`head`?

*Why it matters:* dedup correctness (event `id` remains the dedupe key) and
whether push counts should collapse by `push_id` instead of events.

## OQ-4 — How frequent are non-public / redacted events?

One `public: false` ForkEvent with empty `repo: {}` appeared in 500k events.
Measure the rate over longer windows; confirm the Silver rule (flag/quarantine
rows with `public = false` or null `repo_id`) doesn't silently drop a
meaningful class.

## OQ-5 — What schema do pre-2026 archive hours have?

Backfill will cross GitHub's payload-slimming boundary (PushEvent commits,
PR objects). Old hours likely have the rich historical schema, so metrics
like `prs_merged` need era-specific derivations (`merged` flag vs `merged`
action) and Bronze needs a real `schema_version` policy.

*Resolution path:* profile one hour each from 2024 and 2025 with the same
profiler before any backfill decision.

## OQ-6 — How much history fits in Free Edition?

Raw gz is ~0.5 GB/day; Bronze parsed Delta will be larger. Establish
per-day Bronze/Silver storage after the first full ingested day, then set a
retention/backfill budget.

## OQ-8 — Bot classification beyond the `[bot]` suffix

Suffix marks 6.13% of events; no `actor.type` exists in-stream. High-volume
human-named accounts (e.g. 900+ pushes/3h) look automated. Candidate
approaches: known-bot lists, login patterns, behavioral thresholds — each
changes Contribution metrics materially. Decide and version the classifier.

## OQ-9 — Which Flow metric is most stable and interpretable?

Needs ≥2 weeks of `gold.ecosystem_hourly` before CV/Fano/burstiness/entropy
can be compared for stability. Deferred until measurement pipeline runs.

## OQ-10 — Enrichment sources for true adoption and dependency structure

In-stream, `release.assets[].download_count` is a genuine adoption signal
(present on 68.6% of sampled releases). For package-level adoption and
dependency graphs, evaluate deps.dev, ecosyste.ms, and registry APIs for
Free-Edition-friendly access. Decision needed before Phase "Dependency
Structure" naming upgrade (spec §3.7).

## OQ-11 — Do published hour files ever change after first publication?

Files appear ~65 min after the hour. If GH Archive re-uploads corrected
files, size/sha drift matters (idempotent skip would keep the stale copy).
The downloader already records sha256; add a periodic re-verify job later.

*Data point (2026-07-30):* all three sample hours re-downloaded from a
different machine/network hours after the first fetch produced byte-identical
files (same sha256) — no drift observed within a same-day window.

## OQ-12 — Forecast target confirmation

`total_events_next_hour` and `push_events_next_hour` (spec §16) are
supported by the stream. Given PushEvent is 95.5% of volume the two targets
are nearly collinear — decide whether the second slice target should instead
be a non-push class (e.g. PR activity) despite coverage caveats.

---

## Resolved

| # | Question | Answer (evidence) |
| --- | --- | --- |
| R-1 | Is the GH Archive URL hour zero-padded? | **No.** `...-3.json.gz` → 200, `...-03.json.gz` → 404 (2026-07-30). Spec's `HH` corrected in `build_url`. |
| R-2 | Is a browser User-Agent required? | **Yes.** Default urllib UA → HTTP 403; `Mozilla/5.0` → 200 (verified 2026-07-30). |
| R-3 | Are commit counts in PushEvent payloads? | **No.** 0 of 477,433 pushes carry `commits`/`size`/`distinct_size`. |
| R-4 | Is `pull_request.merged` present? | **No.** Merge signal is `payload.action = 'merged'` (see OQ-2 for the closed-action corollary). |
| R-5 | Are review states available? | **Yes.** `review.state` 100% on PullRequestReviewEvent; all four states observed. |
| R-6 | Can issue/PR lifecycles be linked? | **Yes.** `(repo_id, number)` collision-free in sample; partition issues vs PRs via `issue.pull_request` marker. |
| R-7 | Is event `id` a safe primary key? | **Yes.** 499,742/499,742 distinct, none missing. `push_id` is NOT unique (OQ-3). |
| OQ-7 | Which event set defines "production events"? | **Resolved v1 (2026-07-30):** whitelist fixed in `docs/metric_definitions.md` — pushes always; PR opened/merged/closed/reopened; issues opened/closed/reopened; releases published. State-churn actions (labeled 145 vs opened 223 in-sample), comments, reviews, watch/fork, create/delete excluded. |
