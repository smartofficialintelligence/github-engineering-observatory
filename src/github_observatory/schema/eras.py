"""Normalization basis: what is comparable across the GH Archive decade.

The public events feed is not a stable measuring instrument. Over the
decade it changed in two independent ways, and conflating them produces
false conclusions:

**Schema distortion** — a field exists in one period and not another.
``payload.pull_request.merged`` vanished on 2025-10-09; ``payload.commits``
left PushEvent at some point in the slimming; ``payload.issue.state_reason``
only exists from late 2022. A metric derived from a missing field reads as
zero, which is indistinguishable from "the thing stopped happening" unless
you know the field is gone. This distortion *is* correctable: compute the
metric only from fields present in every era being compared.

**Sampling distortion** — whole events were dropped upstream. In the 2026
stream stars run at ~44/hour for all of public GitHub, which is
implausible; those WatchEvents are not in the feed at all. No amount of
field-intersection fixes this, because the field is present — there are
simply too few rows. Worse, the filtering was not uniform across event
types (pushes fell ~28% in the June 2025 step while non-push classes fell
far harder), so the event *mix* is distorted too.

The consequence for analysis:

* Against schema distortion, restrict to intersection fields.
* Against sampling distortion, prefer quantities invariant to proportional
  subsampling — shares, ratios, concentration, distribution shape — over
  absolute levels. "Pushes per hour" across the boundary misleads;
  "bot share of pushes" or "top-100 actor share" largely survives.

Every boundary below is marked ``PINNED`` (established empirically, with
the evidence named) or ``INFERRED`` (believed but not yet verified to the
day). Never present an INFERRED boundary as exact. Pin one with
``pipelines/schema_drift/pin_boundary.py`` — it costs nothing.
"""

from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field
from enum import Enum

# --- provenance ---------------------------------------------------------------


class Confidence(Enum):
    """How well a boundary date is established."""

    PINNED = "pinned"      # binary-searched to a 1-day gap, evidence recorded
    INFERRED = "inferred"  # believed from monthly sampling or documentation
    UNKNOWN = "unknown"    # boundary exists but has not been located


@dataclass(frozen=True)
class Boundary:
    """A date on which some observable property of the feed changed."""

    date: dt.date
    what: str
    confidence: Confidence
    evidence: str

    def __str__(self) -> str:
        return f"{self.date} {self.what} [{self.confidence.value}]"


# Boundaries established so far. Add one line per newly pinned boundary.
BOUNDARIES: tuple[Boundary, ...] = (
    Boundary(
        dt.date(2025, 6, 1),
        "volume step: total events/hour fell ~26% (231k → 150k vs March peak)",
        Confidence.INFERRED,
        "docs/open_questions.md OQ-1, from the BigQuery decade census import; "
        "monthly resolution only — the within-month date is not established",
    ),
    Boundary(
        dt.date(2025, 10, 9),
        "comprehensive payload stripping: PushEvent loses payload.commits "
        "AND PullRequestEvent loses payload.pull_request.merged, same day",
        Confidence.PINNED,
        "full-day counts from githubarchive.day.*: 2025-10-08 had "
        "1,714,628/1,718,234 pushes carrying commits (99.8%) and "
        "215,537/215,846 PR events carrying the merged field (99.9%); "
        "2025-10-09 had 0/12,228 and 0/1,456 respectively. Healthy days "
        "either side confirm — 2025-10-15 (3.47M rows) and 2025-10-20 "
        "(3.50M rows) both show 0 of millions. NOTE: hourly-file probing "
        "cannot establish this date, because 2025-10-09..14 is an archive "
        "outage (see OUTAGES); only full-day aggregates have the "
        "denominator to support the verdict.",
    ),
    Boundary(
        dt.date(2025, 12, 2),
        "PR merge signal restored in a new encoding: action='merged' appears",
        Confidence.PINNED,
        "binary search over githubarchive.day.*: 2025-12-01 ABSENT (0 merges "
        "detectable by either encoding), 2025-12-02 PRESENT (11,871 via the "
        "new synthetic action). artifacts/signal_probes.jsonl",
    ),
)


# --- collection outages -------------------------------------------------------


@dataclass(frozen=True)
class Outage:
    """A window in which GH Archive collected far less than normal.

    Distinct from filtering: during an outage the *collector* failed, so
    the missing events are missing for everyone. Any metric over these
    days is a floor, not a measurement, and any rate computed across them
    is wrong. Backfills leave a visible hole here.
    """

    start: dt.date
    end: dt.date
    typical_rows_per_day: int
    observed_rows_per_day: str
    evidence: str

    def contains(self, when: dt.date) -> bool:
        return self.start <= when <= self.end

    @property
    def days(self) -> int:
        return (self.end - self.start).days + 1


# Found by pipelines/schema_drift/find_outages.py over the full decade of
# githubarchive.day.__TABLES__ row counts (a free metadata query), each day
# compared against the median of the same weekday 4-9 weeks away. The
# same-weekday baseline matters: GitHub activity drops ~40% at weekends, so
# a naive comparison flags every Saturday. The 4-9 week offset matters too —
# the October 2021 outage ran 20 days and is invisible to any baseline drawn
# from its own neighbourhood.
#
# 37 days across the decade. Regenerate with:
#   python pipelines/schema_drift/find_outages.py --project <p> \
#       --output artifacts/schema_drift/outages.json
OUTAGES: tuple[Outage, ...] = (
    Outage(
        start=dt.date(2019, 9, 12),
        end=dt.date(2019, 9, 12),
        typical_rows_per_day=1_971_698,
        observed_rows_per_day="557,535 (28.3% of baseline)",
        evidence="find_outages.py; single-day collection failure",
    ),
    Outage(
        start=dt.date(2020, 8, 21),
        end=dt.date(2020, 8, 23),
        typical_rows_per_day=2_679_986,
        observed_rows_per_day="522k–790k (19.5%–29% of baseline); "
                              "2020-08-22 table missing entirely",
        evidence="find_outages.py; 3 days, 1 missing table",
    ),
    Outage(
        start=dt.date(2021, 3, 9),
        end=dt.date(2021, 3, 10),
        typical_rows_per_day=3_171_538,
        observed_rows_per_day="985k–1.01M (31%–33% of baseline)",
        evidence="find_outages.py; 2 days degraded",
    ),
    Outage(
        start=dt.date(2021, 5, 8),
        end=dt.date(2021, 5, 11),
        typical_rows_per_day=2_106_033,
        observed_rows_per_day="0 — tables for 05-08, 05-10, 05-11 absent",
        evidence="find_outages.py; 3 of 4 day tables missing outright",
    ),
    Outage(
        start=dt.date(2021, 8, 26),
        end=dt.date(2021, 8, 27),
        typical_rows_per_day=2_927_332,
        observed_rows_per_day="80,254 (2.7%); 2021-08-26 table missing",
        evidence="find_outages.py; near-total loss over 2 days",
    ),
    Outage(
        start=dt.date(2021, 10, 6),
        end=dt.date(2021, 10, 29),
        typical_rows_per_day=3_363_568,
        observed_rows_per_day="57,932–1.4M (1.7%–41%); tables for 10-26, "
                              "10-27, 10-28 absent",
        evidence=(
            "find_outages.py — the longest outage in the archive's history. "
            "Two phases: 10-06 crashes to 357k, then 10-07..10-21 runs at a "
            "sustained ~1.0-1.4M (roughly a third of normal), then 10-22..29 "
            "collapses to 58k-542k with three days missing entirely. "
            "Recovery on 10-30. Any 2021 annual aggregate is materially "
            "short and any October 2021 rate is meaningless."
        ),
    ),
    Outage(
        start=dt.date(2025, 10, 9),
        end=dt.date(2025, 10, 14),
        typical_rows_per_day=3_677_646,
        observed_rows_per_day="14.6k–425k (0.4%–12% of baseline)",
        evidence=(
            "find_outages.py, corroborated day by day: Oct 7 3,875,261; "
            "Oct 8 2,769,429 (partial, last event 23:50:40); Oct 9 18,906; "
            "Oct 10 18,864; Oct 11 14,606; Oct 12 15,534; Oct 13 18,241; "
            "Oct 14 424,570 (recovering); Oct 15 3,465,925 (recovered). "
            "Coincides exactly with the payload-stripping boundary, which "
            "suggests one upstream incident caused both."
        ),
    ),
)


# The 2011-2015 archive is missing every 31 December day table. This is a
# source artifact rather than a collection failure, but a backfill will
# still find a hole there.
MISSING_YEAR_END_TABLES: tuple[dt.date, ...] = tuple(
    dt.date(y, 12, 31) for y in range(2011, 2016)
)


def outage_for(when: dt.date | dt.datetime) -> Outage | None:
    day = when.date() if isinstance(when, dt.datetime) else when
    for outage in OUTAGES:
        if outage.contains(day):
            return outage
    return None


def outages_overlapping(start: dt.date, end: dt.date) -> tuple[Outage, ...]:
    return tuple(o for o in OUTAGES if o.start <= end and o.end >= start)


# --- eras ---------------------------------------------------------------------


@dataclass(frozen=True)
class Era:
    """A contiguous period over which the feed's observable properties are
    stable enough to compare within."""

    name: str
    start: dt.date
    end: dt.date | None  # None = ongoing
    summary: str
    caveats: tuple[str, ...] = field(default_factory=tuple)

    def contains(self, when: dt.date) -> bool:
        if when < self.start:
            return False
        return self.end is None or when <= self.end


# Ordered, non-overlapping, contiguous.
ERAS: tuple[Era, ...] = (
    Era(
        name="census",
        start=dt.date(2015, 1, 1),
        end=dt.date(2025, 5, 31),
        summary=(
            "Full public events feed with rich payloads. Treated as a census "
            "of public GitHub activity: levels and trends are ecosystem facts."
        ),
        caveats=(
            "Payload richness is not constant across this whole span — "
            "issue.state_reason arrives late 2022, and the PushEvent/PR "
            "slimming boundary inside this era is not yet pinned. Verified "
            "present through 2022-01 by monthly sampling.",
        ),
    ),
    Era(
        name="filtered",
        start=dt.date(2025, 6, 1),
        end=dt.date(2025, 10, 8),
        summary=(
            "Upstream filtering begins. Total volume down ~26%; non-push "
            "classes cut far harder than pushes. Payload merge signal still "
            "present."
        ),
        caveats=(
            "Absolute levels for non-push classes are a sample of unknown "
            "coverage — do not publish as census counts.",
            "The 2025-06-01 start is month-resolution only (INFERRED).",
        ),
    ),
    Era(
        name="merge_blind",
        start=dt.date(2025, 10, 9),
        end=dt.date(2025, 12, 1),
        summary=(
            "Payloads stripped and no PR merge signal in any encoding. "
            "54 days in which pr_merged is structurally unrecoverable."
        ),
        caveats=(
            "pr_merged reads 0 — signal loss, not an absence of merges. "
            "pr_closed_no_merge is correspondingly inflated, since former "
            "merges fall into it.",
            "PushEvent lost payload.commits at this era's start and never "
            "regained it — commit-level metrics end here permanently.",
            "The first six days overlap an archive collection outage "
            "(see OUTAGES): 2025-10-09..14 carry ~0.5% of normal volume, "
            "so counts there are floors, not measurements.",
        ),
    ),
    Era(
        name="merge_restored",
        start=dt.date(2025, 12, 2),
        end=None,
        summary=(
            "Merge signal returns in a new encoding — a synthetic "
            "action='merged' rather than the historical "
            "pull_request.merged=true. Only the merge signal came back: "
            "payloads stay slim and commits stay gone."
        ),
        caveats=(
            "pull_request.merged remains absent, so merge detection must "
            "handle both encodings depending on era.",
            "payload.commits is still absent — the loss at 2025-10-09 was "
            "not reversed. Commit counts remain unobservable.",
            "Filtering from the earlier eras persists; non-push absolute "
            "levels are still not census.",
        ),
    ),
)


def era_for(when: dt.date | dt.datetime) -> Era | None:
    """Which era does this moment fall in? None if before coverage."""
    day = when.date() if isinstance(when, dt.datetime) else when
    for era in ERAS:
        if era.contains(day):
            return era
    return None


def eras_spanned(start: dt.date, end: dt.date) -> tuple[Era, ...]:
    """Every era touched by [start, end] — more than one means any
    cross-era comparison needs the rules below applied."""
    return tuple(
        e for e in ERAS
        if e.start <= end and (e.end is None or e.end >= start)
    )


# --- metric comparability ------------------------------------------------------


class Comparability(Enum):
    """How a metric may honestly be compared across eras."""

    DECADE = "decade"          # levels comparable across every era
    SHAPE_ONLY = "shape_only"  # ratios/shares comparable; levels are not
    ERA_BOUND = "era_bound"    # meaningful only inside specific eras


@dataclass(frozen=True)
class MetricRule:
    comparability: Comparability
    rationale: str
    invalid_eras: tuple[str, ...] = field(default_factory=tuple)


# Rules for the columns of gold.ecosystem_hourly. A metric absent here has
# not been assessed — treat it as ERA_BOUND until it is.
METRIC_RULES: dict[str, MetricRule] = {
    # -- census columns: fields present in every era, but volumes filtered --
    "total_events": MetricRule(
        Comparability.SHAPE_ONLY,
        "Field always available, but the filter removed events "
        "non-uniformly. Absolute levels are not comparable across the "
        "2025-06-01 step; composition and within-era trend are.",
        invalid_eras=(),
    ),
    "push_events": MetricRule(
        Comparability.SHAPE_ONLY,
        "Pushes were cut ~28% at the June 2025 step — less than other "
        "classes but not spared. Levels break across the boundary.",
    ),
    "distinct_actors": MetricRule(
        Comparability.SHAPE_ONLY,
        "Actor counts scale with the number of surviving events; the "
        "filter suppresses them independently of real activity.",
    ),
    "distinct_repos": MetricRule(
        Comparability.SHAPE_ONLY,
        "Same reasoning as distinct_actors.",
    ),
    "bot_events": MetricRule(
        Comparability.SHAPE_ONLY,
        "Bots disproportionately emit the filtered event types, so the "
        "measured bot share moves with the filter, not with automation. "
        "The apparent 27% → 9% drop at the boundary is an artefact.",
    ),

    # -- PR lifecycle: era-bound on the merge-blind window --
    "pr_merged": MetricRule(
        Comparability.ERA_BOUND,
        "No merge signal exists in any encoding during merge_blind; the "
        "column reads 0 there for reasons of signal loss, not behaviour. "
        "Detection must also handle two encodings across the other eras.",
        invalid_eras=("merge_blind",),
    ),
    "pr_closed_no_merge": MetricRule(
        Comparability.ERA_BOUND,
        "Inflated during merge_blind because merges could not be "
        "distinguished from plain closures and fall into this bucket.",
        invalid_eras=("merge_blind",),
    ),
    "pr_opened": MetricRule(
        Comparability.SHAPE_ONLY,
        "Action is always present, so the signal survives; but PR events "
        "were heavily filtered, so levels are not comparable.",
    ),

    # -- issue lifecycle --
    "issues_opened": MetricRule(
        Comparability.SHAPE_ONLY,
        "Signal always present; volumes filtered.",
    ),
    "issues_closed": MetricRule(
        Comparability.SHAPE_ONLY,
        "Signal always present; volumes filtered.",
    ),
    "issues_closed_completed": MetricRule(
        Comparability.ERA_BOUND,
        "Depends on payload.issue.state_reason, which GitHub introduced in "
        "late 2022. Before that every closure lands in _unknown. The exact "
        "arrival date is not yet pinned.",
    ),
    "issues_closed_not_planned": MetricRule(
        Comparability.ERA_BOUND,
        "Same dependency on state_reason as issues_closed_completed.",
    ),

    # -- commit-derived: available for 10 of 11 years, then gone for good --
    "commits": MetricRule(
        Comparability.ERA_BOUND,
        "payload.commits was present on ~99.8% of pushes through "
        "2025-10-08 and absent from 2025-10-09 onward, never restored. "
        "Commit-level metrics are therefore computable for the census and "
        "filtered eras but end permanently at 2025-10-08.",
        invalid_eras=("merge_blind", "merge_restored"),
    ),
    "commits_per_push": MetricRule(
        Comparability.ERA_BOUND,
        "Same dependency on payload.commits; ends at 2025-10-08.",
        invalid_eras=("merge_blind", "merge_restored"),
    ),
}


# Metrics whose underlying field is available in every era — the honest
# intersection. Anything outside this set needs either an era restriction
# or a shape-only reading.
def decade_comparable_metrics() -> tuple[str, ...]:
    return tuple(sorted(
        m for m, r in METRIC_RULES.items()
        if r.comparability is Comparability.DECADE
    ))


def shape_only_metrics() -> tuple[str, ...]:
    return tuple(sorted(
        m for m, r in METRIC_RULES.items()
        if r.comparability is Comparability.SHAPE_ONLY
    ))


def era_bound_metrics() -> tuple[str, ...]:
    return tuple(sorted(
        m for m, r in METRIC_RULES.items()
        if r.comparability is Comparability.ERA_BOUND
    ))


def valid_range_for(metric: str) -> tuple[dt.date, dt.date | None] | None:
    """Widest date range over which ``metric`` is meaningful, or None if
    it is valid in no era. Useful for clamping a query window."""
    rule = rule_for(metric)
    usable = [e for e in ERAS if e.name not in rule.invalid_eras]
    if not usable:
        return None
    return usable[0].start, usable[-1].end


def rule_for(metric: str) -> MetricRule:
    """Comparability rule for a metric; unassessed metrics are ERA_BOUND."""
    return METRIC_RULES.get(
        metric,
        MetricRule(
            Comparability.ERA_BOUND,
            "Not yet assessed against the normalization basis. Treated as "
            "era-bound until someone establishes otherwise.",
        ),
    )


def check_comparison(metric: str, start: dt.date, end: dt.date) -> list[str]:
    """Warnings for comparing ``metric`` over [start, end].

    Empty list means the comparison is sound. Anything returned should be
    surfaced to whoever is reading the number — these are the ways the
    measuring instrument changed underneath the series.
    """
    warnings: list[str] = []

    # Outages apply regardless of era count: a single-era range that
    # contains one still yields wrong totals and rates.
    for outage in outages_overlapping(start, end):
        warnings.append(
            f"range covers a {outage.days}-day GH Archive collection "
            f"outage ({outage.start} to {outage.end}, "
            f"{outage.observed_rows_per_day} vs ~"
            f"{outage.typical_rows_per_day:,}/day typical). Counts there "
            f"are floors, not measurements; exclude those days or expect "
            f"a spurious trough."
        )

    spanned = eras_spanned(start, end)
    if len(spanned) <= 1:
        return warnings

    rule = rule_for(metric)
    names = [e.name for e in spanned]

    invalid = [n for n in names if n in rule.invalid_eras]
    if invalid:
        warnings.append(
            f"{metric} is not meaningful in era(s) {', '.join(invalid)}: "
            f"{rule.rationale}"
        )

    if rule.comparability is Comparability.SHAPE_ONLY:
        warnings.append(
            f"{metric} spans {len(spanned)} eras ({', '.join(names)}); "
            f"absolute levels are not comparable across them. Compare "
            f"shares, ratios or within-era trend instead. {rule.rationale}"
        )
    elif rule.comparability is Comparability.ERA_BOUND and not invalid:
        warnings.append(
            f"{metric} is era-bound and this range spans "
            f"{len(spanned)} eras ({', '.join(names)}). {rule.rationale}"
        )

    return warnings
