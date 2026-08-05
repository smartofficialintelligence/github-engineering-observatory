# Normalization Basis — What Is Comparable Across the Decade

**Version 1** (2026-08-02). Companion to `docs/schema_drift_timeline.md`
(the raw evidence) and `src/github_observatory/schema/eras.py` (the same
rules as code). Consult this before drawing any series that crosses
2025-06-01.

## The problem

The GH Archive feed is not a stable measuring instrument. Treating it as
one produces confident, wrong conclusions — "PR merges collapsed 100% in
October 2025" is the failure mode, and it is false: the merges happened,
the signal did not survive.

Three independent distortions, which need different defences.

### 1. Schema distortion — a field exists in one period, not another

A metric computed over a missing field reads zero, and zero is
indistinguishable from "the thing stopped happening" unless you know the
field is gone.

**Correctable** by restricting a cross-era metric to fields present in
every era being compared.

### 2. Sampling distortion — events dropped upstream

In the current feed, stars run at roughly 44/hour for all of public
GitHub. That is not a schema problem: `WatchEvent` still has all its
fields, there are simply almost none of them. Field intersection cannot
help.

Two separate events produce this, and an earlier version of this document
merged them into one — worth stating plainly, because the merged version
got the mechanism backwards.

**June 2025 — a roughly uniform cut.** Measured over 8 weeks either side,
pushes retained 69.6% and non-push 74.5%; push share moved 64.0% → 62.5%.
Non-push held up marginally *better*, and the event mix was largely
preserved. Human events retained 71.9% against bots' 69.8%, a 2.1-point
spread that is noise. Event counts cluster near 70% while distinct-entity
counts sit near 80% — the arithmetic signature of uniform subsampling,
since an actor disappears only when every one of its events is dropped.
Consistent with uniform sampling; not proof of it, since we still have no
direct evidence of mechanism (OQ-1).

**2026 — a progressive non-push collapse.** This is what actually drove
push share to ~92%. PR share fell 7.1% (2025-09) → 3.5% (2026-03) → 0.1%
(2026-07), issues and stars to ~0%. Monotonic rather than stepped, and
still in motion at last measurement. Shape and exact onset are being
characterised; until that lands, treat lifecycle metrics after roughly
2026-Q1 as unavailable rather than as measurements.

**Not correctable.** The defence is to prefer quantities invariant to
proportional subsampling — shares, ratios, concentration, distribution
shape — over absolute levels. That defence holds for the June 2025 step,
where sampling looks uniform. It does **not** rescue the 2026 collapse:
when a class falls to 0.1% of the feed, no ratio recovers it.

### 3. Collection outages — the archive itself failed

Distinct from both: the events happened and GitHub emitted them, but the
collector missed them. Counts inside an outage are floors, not
measurements, and any rate spanning one is wrong.

**Correctable** by excluding the affected days, provided you know where
they are. See the table below.

## Eras

| Era | Range | Character |
| --- | --- | --- |
| `census` | 2015-01-01 → 2025-05-31 | Full feed, rich payloads. Levels are ecosystem facts. |
| `filtered` | 2025-06-01 → 2025-10-08 | Volume −27%; non-push classes cut hardest. Payloads still rich. |
| `merge_blind` | 2025-10-09 → 2025-12-01 | Payloads stripped. No merge signal in any encoding. 54 days. |
| `merge_restored` | 2025-12-02 → ongoing | Merge signal back via a *new* encoding. Payloads stay slim. |

Boundary dates carry a confidence marker in `eras.py`. `2025-10-09` and
`2025-12-02` are **pinned** to the day; `2025-06-01` is **inferred** at
month resolution from the volume step and should not be quoted as exact.

## The 2025-10-09 collapse

One event removed **831 fields** — every field removal in the decade
happened that day:

| Event type | Fields removed |
| --- | ---: |
| `PullRequestReviewEvent` | 292 |
| `PullRequestEvent` | 269 |
| `PullRequestReviewCommentEvent` | 258 |
| `PushEvent` | 11 |

For contrast, the preceding ten years *added* 182 fields in small
increments (60 at once in October 2021, mostly reaction counts and
`node_id`s). The schema grew steadily, then collapsed in a day.

Evidence: on 2025-10-08, 1,714,628 of 1,718,234 pushes carried
`payload.commits` (99.8%) and 215,537 of 215,846 PR events carried
`payload.pull_request.merged` (99.9%). On 2025-10-09 both were zero, and
healthy days either side (2025-10-15, 2025-10-20, millions of rows each)
confirm.

Two consequences dominate everything downstream:

* **Merge signal.** `pull_request.merged` never returned. From
  2025-12-02 a synthetic `action='merged'` provides the signal instead,
  so merge detection must handle both encodings by era. Between
  2025-10-09 and 2025-12-01 neither exists and `pr_merged` is
  unrecoverable — it reads 0, and `pr_closed_no_merge` is correspondingly
  inflated because former merges fall into it.
* **Commit data.** PushEvent lost `payload.commits`, `payload.size` and
  `payload.distinct_size` together — all at 100% presence the month
  before — and none returned. `size` is the costly one: it carried the
  commit count as a plain integer. Commit metrics are therefore
  census-quality for 2015-01-01 → 2025-10-08 and unavailable in-stream
  after. This is the sharpest normalization trade in the project:
  normalizing everything down to the current era would discard ten years
  of commit-level history to gain comparability with the last few months.

  Not entirely lost, though. `before` and `head` survived the collapse,
  so each push still identifies its exact commit range, and GitHub's
  compare API (`/repos/{owner}/{repo}/compare/{before}...{head}`) returns
  `total_commits` for it. At 5,000 authenticated requests/hour against
  ~150k pushes/hour that covers ~3% of the stream — useless as a census,
  but sufficient for a sampled repo panel, which is the defensible way to
  measure commit volume anyway. Tracked as an enrichment option rather
  than a plan.

  Verified that this is a source loss, not a distribution artifact: the
  BigQuery mirror is built from the same hourly files and shows the same
  five-field payload. Its extra `other` column carries only an actor
  overflow (`{"actor":{"display_login":...}}`), not payload data.

## Collection outages

Seven incidents, 42 days of window. Found by comparing each day against
the median of the **same weekday** 4–9 weeks away — a same-weekday
baseline is required because activity drops ~40% at weekends, and a
distant baseline is required because a long outage otherwise depresses
its own reference.

| Window | Days | Worst | Notes |
| --- | ---: | ---: | --- |
| 2019-09-12 | 1 | 28.3% | |
| 2020-08-21 .. 23 | 3 | 19.5% | 1 table missing |
| 2021-03-09 .. 10 | 2 | 31.1% | |
| 2021-05-08 .. 11 | 4 | 0% | 3 of 4 tables missing |
| 2021-08-26 .. 27 | 2 | 2.7% | 1 table missing |
| **2021-10-06 .. 29** | **24** | **1.7%** | longest; 3 tables missing |
| 2025-10-09 .. 14 | 6 | 0.4% | coincides with the collapse |

**2021 carries four separate outages** — any 2021 annual aggregate is
materially short. The October 2021 incident ran in two phases: a crash to
357k on the 6th, a sustained ~⅓-of-normal plateau through the 21st, then
near-total loss 22nd–29th.

Separately, the 2011–2015 archive is missing **every 31 December table**
— a source artifact rather than a collection failure, but a backfill will
still find the hole.

## Rules for comparing

`eras.check_comparison(metric, start, end)` returns the warnings that
apply to a given series; it is the function dashboards and notebooks
should consult before drawing a line. Metrics fall into three classes:

* **`DECADE`** — levels comparable across every era.
* **`SHAPE_ONLY`** — ratios and shares comparable; absolute levels are
  not. Most census columns land here, because the field survives but the
  volume was filtered.
* **`ERA_BOUND`** — meaningful only inside specific eras. `pr_merged` and
  `pr_closed_no_merge` name `merge_blind` as invalid; commit metrics name
  both post-collapse eras.

Metrics not yet assessed default to `ERA_BOUND` — the checker fails
closed rather than assuming a series is safe.

Two rules of thumb:

1. **Never compare absolute levels across 2025-06-01.** Use shares or
   restrict to one era.
2. **Never present a zero from `merge_blind` or a commit metric after
   2025-10-08 as a measurement.** It is signal loss.

## For backfill operations

* Expect a hole at every outage window above; a backfill cannot recover
  what was never collected.
* Pass 2 (`pipelines/gcp_lifecycle_backfill/`) tags affected rows with
  `pr_merge_signal_stripped`, so consumers can filter rather than
  silently averaging a structural zero into a trend.
* Commit-derived columns should be backfilled for 2015-01-01 →
  2025-10-08 and left NULL after, rather than written as 0.
* Before adding a new payload-derived metric, check
  `docs/schema_drift_timeline.md` for the field's availability window.
  `pipelines/schema_drift/pin_boundary.py` pins an exact date for free if
  the monthly sample only brackets it.

## Reproducing

```bash
# monthly schema samples across the decade (free, ~1h)
python pipelines/schema_drift/scan_decade.py --years 2015-2026 \
    --output artifacts/schema_drift/hourly_profiles.jsonl

# the drift timeline
python pipelines/schema_drift/build_timeline.py \
    --input artifacts/schema_drift/hourly_profiles.jsonl \
    --output docs/schema_drift_timeline.md

# outage map (free metadata query)
python pipelines/schema_drift/find_outages.py --project <p> \
    --output artifacts/schema_drift/outages.json

# pin any single field's boundary to the day (free)
python pipelines/schema_drift/pin_boundary.py --feature push_commits \
    --lo 2025-09-15 --hi 2025-11-15
```

The raw per-hour profiles are gitignored (~230 MB); the derived timeline
and outage map are committed.

## Changelog

| Version | Date | Change |
| --- | --- | --- |
| 1 | 2026-08-02 | Initial basis: three distortion types, four eras, seven outages, the 831-field collapse, and the metric comparability classes. |
