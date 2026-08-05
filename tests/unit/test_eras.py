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
    """Rows the archive has but the project does not claim."""
    assert eras.era_for(dt.date(2010, 1, 1)) is None
    assert eras.era_for(dt.date(2019, 6, 1)) is None
    assert not eras.in_scope(dt.date(2019, 12, 31))
    assert eras.in_scope(eras.PROJECT_START)


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
    """A pinned date is a strong claim; it must name the method that
    established it so a reader can re-derive or challenge it."""
    methods = ("binary search", "full-day counts", "day-table counts")
    for boundary in eras.BOUNDARIES:
        assert boundary.evidence.strip(), f"{boundary} has no evidence"
        if boundary.confidence is eras.Confidence.PINNED:
            assert any(m in boundary.evidence.lower() for m in methods), (
                f"{boundary} is PINNED but names no method"
            )


# -- collection outages --------------------------------------------------------


def test_october_2025_outage_is_registered():
    """Ranges covering this window produce spurious troughs unless the
    outage is known — 3.5M rows/day drops to ~15k."""
    outage = eras.outage_for(dt.date(2025, 10, 11))
    assert outage is not None
    assert outage.start == dt.date(2025, 10, 9)
    assert outage.end == dt.date(2025, 10, 14)
    assert outage.days == 6


def test_outage_boundaries_are_exclusive_of_healthy_days():
    assert eras.outage_for(dt.date(2025, 10, 8)) is None   # 2.77M rows
    assert eras.outage_for(dt.date(2025, 10, 15)) is None  # 3.47M rows


def test_outages_overlapping_detects_partial_overlap():
    assert eras.outages_overlapping(
        dt.date(2025, 10, 1), dt.date(2025, 10, 10)
    )
    assert not eras.outages_overlapping(
        dt.date(2025, 1, 1), dt.date(2025, 6, 1)
    )


def test_comparison_warns_about_outage_even_within_one_era():
    """The outage sits inside merge_blind, so era-count alone would not
    trigger a warning — the outage check must fire independently."""
    warnings = eras.check_comparison(
        "total_events", dt.date(2025, 10, 9), dt.date(2025, 11, 1)
    )
    assert any("outage" in w for w in warnings)


# -- commit availability -------------------------------------------------------


def test_commit_metrics_end_at_the_stripping_boundary():
    """payload.commits was present for 10 of 11 years then removed for
    good — the widest valid range must stop before merge_blind."""
    rule = eras.rule_for("commits")
    assert "merge_blind" in rule.invalid_eras
    assert "merge_restored" in rule.invalid_eras
    start, end = eras.valid_range_for("commits")
    assert start == eras.PROJECT_START
    assert end == dt.date(2025, 10, 8)


def test_valid_range_for_unrestricted_metric_is_open_ended():
    assert eras.valid_range_for("push_events") == (eras.PROJECT_START, None)


def test_metric_classification_helpers_partition_the_rules():
    total = (
        len(eras.decade_comparable_metrics())
        + len(eras.shape_only_metrics())
        + len(eras.era_bound_metrics())
    )
    assert total == len(eras.METRIC_RULES)


# -- eras_spanned --------------------------------------------------------------


def test_eras_spanned_within_one_era():
    spanned = eras.eras_spanned(dt.date(2019, 1, 1), dt.date(2020, 1, 1))
    assert [e.name for e in spanned] == ["census"]


def test_eras_spanned_across_the_decade():
    spanned = eras.eras_spanned(eras.PROJECT_START, dt.date(2026, 1, 1))
    assert [e.name for e in spanned] == [
        "census", "filtered", "merge_blind", "merge_restored",
    ]


def test_eras_spanned_is_inclusive_at_edges():
    """A range ending exactly on an era's first day must include it."""
    spanned = eras.eras_spanned(dt.date(2025, 6, 1), dt.date(2025, 10, 9))
    assert [e.name for e in spanned] == ["filtered", "merge_blind"]


# -- comparability rules -------------------------------------------------------


def test_within_era_comparison_is_clean():
    """2023 sits wholly inside census and carries no registered outage."""
    assert eras.check_comparison(
        "push_events", dt.date(2023, 1, 1), dt.date(2023, 12, 1)
    ) == []


def test_pr_merged_flags_the_merge_blind_window():
    warnings = eras.check_comparison(
        "pr_merged", dt.date(2025, 6, 1), dt.date(2026, 1, 1)
    )
    assert warnings, "must warn — the range covers the signal-loss window"
    assert any("merge_blind" in w for w in warnings)


def test_pr_merged_is_clean_when_the_range_avoids_merge_blind():
    assert eras.check_comparison(
        "pr_merged", dt.date(2023, 1, 1), dt.date(2023, 12, 1)
    ) == []


def test_a_clean_looking_2019_range_still_warns_about_its_outage():
    """2019 looks like a quiet census year but contains a one-day
    collection failure — the checker must not let that pass silently."""
    warnings = eras.check_comparison(
        "push_events", dt.date(2019, 1, 1), dt.date(2020, 1, 1)
    )
    assert any("outage" in w for w in warnings)


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
        eras.PROJECT_START, dt.date(2026, 1, 1),
    )
    assert warnings


def test_merge_dependent_metrics_name_merge_blind_as_invalid():
    for metric in ("pr_merged", "pr_closed_no_merge"):
        assert "merge_blind" in eras.rule_for(metric).invalid_eras, metric


def test_state_reason_metrics_are_era_bound():
    """These depend on a field GitHub only introduced in late 2022."""
    for metric in ("issues_closed_completed", "issues_closed_not_planned"):
        assert eras.rule_for(metric).comparability is eras.Comparability.ERA_BOUND


def test_all_seven_decade_outages_are_registered():
    """find_outages.py over the full decade found 7 collection outages
    totalling 37 days; all must be known to check_comparison."""
    assert len(eras.OUTAGES) == 7
    # Outage.days is the calendar span of each window. find_outages.py
    # reports 37 *flagged* days; the windows total 42 because a few
    # healthy days sit inside the long October 2021 window.
    assert sum(o.days for o in eras.OUTAGES) == 42


def test_october_2021_is_the_longest_outage():
    """20 flagged days over a 24-day span — invisible to any baseline
    drawn from its own neighbourhood, which is why it went unnoticed."""
    longest = max(eras.OUTAGES, key=lambda o: o.days)
    assert longest.start == dt.date(2021, 10, 6)
    assert longest.end == dt.date(2021, 10, 29)
    assert longest.days == 24


def test_outages_are_ordered_and_disjoint():
    for a, b in zip(eras.OUTAGES, eras.OUTAGES[1:]):
        assert a.end < b.start, f"{a.start} and {b.start} overlap or misorder"


def test_2021_annual_aggregates_are_flagged():
    """2021 carries four separate outages — an annual total is short."""
    hits = eras.outages_overlapping(dt.date(2021, 1, 1), dt.date(2021, 12, 31))
    assert len(hits) == 4


def test_missing_year_end_tables_are_recorded():
    assert dt.date(2013, 12, 31) in eras.MISSING_YEAR_END_TABLES
    assert len(eras.MISSING_YEAR_END_TABLES) == 5
