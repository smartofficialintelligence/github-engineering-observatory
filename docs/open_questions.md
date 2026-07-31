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
coverage was complete throughout, ruling out collection gaps.

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
