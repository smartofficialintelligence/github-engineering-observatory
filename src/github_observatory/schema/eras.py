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
        "PR merge signal stripped: payload.pull_request.merged disappears",
        Confidence.PINNED,
        "binary search, two independent methods agreeing: BigQuery day-table "
        "counts (pin_signal_boundaries.py) and local hourly-file scan "
        "(pin_boundary.py). 2025-10-08 PRESENT, 2025-10-09 ABSENT. "
        "artifacts/signal_probes.jsonl",
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
            "54-day window in which the feed carries no PR merge signal in "
            "any encoding. pr_merged is structurally unrecoverable here."
        ),
        caveats=(
            "pr_merged reads 0 — this is signal loss, not an absence of "
            "merges. pr_closed_no_merge is correspondingly inflated, since "
            "former merges fall into it.",
            "Both boundaries of this era are PINNED to the day.",
        ),
    ),
    Era(
        name="restored",
        start=dt.date(2025, 12, 2),
        end=None,
        summary=(
            "Merge signal returns in a new encoding: a synthetic "
            "action='merged' rather than the historical "
            "pull_request.merged=true. Payloads remain slim."
        ),
        caveats=(
            "pull_request.merged remains absent — only the action encoding "
            "works here, so merge detection must handle both forms.",
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
}


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
    spanned = eras_spanned(start, end)
    if len(spanned) <= 1:
        return []

    rule = rule_for(metric)
    names = [e.name for e in spanned]
    warnings: list[str] = []

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
