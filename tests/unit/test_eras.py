"""Unit tests for the normalization basis (schema/eras.py).

These guard the properties that make the module safe to build on:
eras tile the timeline without gaps or overlaps, every pinned boundary
lines up with an era edge, and the comparison checker actually fires on
the cases that would otherwise produce false conclusions.
"""

from __future__ import annotations

import datetime as dt

from github_observatory.schema import eras


# -- structural invariants -----------------------------------------------------


def test_eras_are_contiguous_and_non_overlapping():
    """Adjacent eras must meet exactly — a gap would silently drop hours,
    an overlap would make era_for ambiguous."""
    for earlier, later in zip(eras.ERAS, eras.ERAS[1:]):
        assert earlier.end is not None, f"{earlier.name} must be closed"
        assert earlier.end + dt.timedelta(days=1) == later.start, (
            f"{earlier.name} ends {earlier.end} but {later.name} "
            f"starts {later.start}"
        )


def test_only_the_final_era_is_open_ended():
    assert eras.ERAS[-1].end is None
    for era in eras.ERAS[:-1]:
        assert era.end is not None


def test_era_for_covers_every_era():
    for era in eras.ERAS:
        assert eras.era_for(era.start) is era
        if era.end is not None:
            assert eras.era_for(era.end) is era


def test_era_for_returns_none_before_coverage():
    assert eras.era_for(dt.date(2010, 1, 1)) is None


def test_era_for_accepts_datetime():
    got = eras.era_for(dt.datetime(2025, 11, 15, 13, 0))
    assert got is not None and got.name == "merge_blind"


# -- the merge-blind window ----------------------------------------------------


def test_merge_blind_window_matches_pinned_boundaries():
    """The 54-day window is the headline empirical result — if these
    dates drift, downstream NULLing silently changes meaning."""
    blind = next(e for e in eras.ERAS if e.name == "merge_blind")
    assert blind.start == dt.date(2025, 10, 9)
    assert blind.end == dt.date(2025, 12, 1)
    assert (blind.end - blind.start).days + 1 == 54


def test_pinned_boundaries_align_with_era_edges():
    era_starts = {e.start for e in eras.ERAS}
    for boundary in eras.BOUNDARIES:
        if boundary.confidence is eras.Confidence.PINNED:
            assert boundary.date in era_starts, (
                f"pinned boundary {boundary} does not start an era"
            )


def test_every_boundary_records_its_evidence():
    for boundary in eras.BOUNDARIES:
        assert boundary.evidence.strip(), f"{boundary} has no evidence"
        if boundary.confidence is eras.Confidence.PINNED:
            # A pinned claim must name how it was established.
            assert "binary search" in boundary.evidence.lower()


# -- eras_spanned --------------------------------------------------------------


def test_eras_spanned_within_one_era():
    spanned = eras.eras_spanned(dt.date(2019, 1, 1), dt.date(2020, 1, 1))
    assert [e.name for e in spanned] == ["census"]


def test_eras_spanned_across_the_decade():
    spanned = eras.eras_spanned(dt.date(2015, 1, 1), dt.date(2026, 1, 1))
    assert [e.name for e in spanned] == [
        "census", "filtered", "merge_blind", "restored",
    ]


def test_eras_spanned_is_inclusive_at_edges():
    """A range ending exactly on an era's first day must include it."""
    spanned = eras.eras_spanned(dt.date(2025, 6, 1), dt.date(2025, 10, 9))
    assert [e.name for e in spanned] == ["filtered", "merge_blind"]


# -- comparability rules -------------------------------------------------------


def test_within_era_comparison_is_clean():
    assert eras.check_comparison(
        "push_events", dt.date(2019, 1, 1), dt.date(2020, 1, 1)
    ) == []


def test_pr_merged_flags_the_merge_blind_window():
    warnings = eras.check_comparison(
        "pr_merged", dt.date(2025, 6, 1), dt.date(2026, 1, 1)
    )
    assert warnings, "must warn — the range covers the signal-loss window"
    assert any("merge_blind" in w for w in warnings)


def test_pr_merged_is_clean_when_the_range_avoids_merge_blind():
    assert eras.check_comparison(
        "pr_merged", dt.date(2019, 1, 1), dt.date(2020, 1, 1)
    ) == []


def test_shape_only_metric_warns_across_the_filter_step():
    warnings = eras.check_comparison(
        "push_events", dt.date(2025, 1, 1), dt.date(2025, 9, 1)
    )
    assert any("levels are not comparable" in w for w in warnings)


def test_bot_events_is_shape_only():
    """The 27% → 9% bot-share drop at the boundary is a filter artefact,
    so levels must never be presented as comparable."""
    rule = eras.rule_for("bot_events")
    assert rule.comparability is eras.Comparability.SHAPE_ONLY
    assert "artefact" in rule.rationale


def test_unassessed_metric_defaults_to_era_bound():
    """Fail closed: an unknown metric must not be assumed comparable."""
    rule = eras.rule_for("some_metric_nobody_has_assessed")
    assert rule.comparability is eras.Comparability.ERA_BOUND


def test_unassessed_metric_warns_on_cross_era_comparison():
    warnings = eras.check_comparison(
        "some_metric_nobody_has_assessed",
        dt.date(2015, 1, 1), dt.date(2026, 1, 1),
    )
    assert warnings


def test_merge_dependent_metrics_name_merge_blind_as_invalid():
    for metric in ("pr_merged", "pr_closed_no_merge"):
        assert "merge_blind" in eras.rule_for(metric).invalid_eras, metric


def test_state_reason_metrics_are_era_bound():
    """These depend on a field GitHub only introduced in late 2022."""
    for metric in ("issues_closed_completed", "issues_closed_not_planned"):
        assert eras.rule_for(metric).comparability is eras.Comparability.ERA_BOUND
