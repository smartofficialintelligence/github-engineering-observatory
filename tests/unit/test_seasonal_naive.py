"""Unit tests for the seasonal-naive forecaster — pure python, exact
hand-computed expectations."""

from __future__ import annotations

import datetime as dt

import pytest

from github_observatory.forecasting import seasonal_naive as sn

T0 = dt.datetime(2026, 7, 1, 0, 0, tzinfo=dt.timezone.utc)


def hours(values, start=T0, step_hours=1):
    return [(start + dt.timedelta(hours=i * step_hours), v) for i, v in enumerate(values)]


def by_method(evals):
    return {e.method: e for e in evals}


def test_naive_1h_exact():
    evals = by_method(sn.evaluate_methods(hours([10, 12, 11, 15]), "total_events"))
    naive = evals["naive_1h"]
    # predictions for hours 1..3: 10, 12, 11 vs actuals 12, 11, 15
    assert naive.n_predictions == 3
    assert naive.mae == pytest.approx((2 + 1 + 4) / 3)
    assert naive.mase == pytest.approx(1.0)  # naive scaled by itself


def test_seasonal_24h_beats_naive_on_daily_pattern():
    # Two identical days: seasonal_24h is perfect, naive_1h is not.
    day = [100, 50, 80, 120] * 6  # 24 hourly values
    evals = by_method(sn.evaluate_methods(hours(day * 2), "total_events"))
    seasonal = evals["seasonal_24h"]
    assert seasonal.n_predictions == 24  # second day fully predictable
    assert seasonal.mae == 0.0
    assert seasonal.mase == 0.0
    assert evals["naive_1h"].mae > 0


def test_seasonal_168h_needs_a_week():
    evals = by_method(sn.evaluate_methods(hours([1.0] * 100), "x"))
    assert evals["seasonal_168h"].n_predictions == 0
    assert evals["seasonal_168h"].mae is None
    week = by_method(sn.evaluate_methods(hours([1.0] * 200), "x"))
    assert week["seasonal_168h"].n_predictions == 200 - 168


def test_gaps_disqualify_rather_than_mislag():
    # Three isolated hours (like the sampled data): nothing predictable.
    series = [
        (dt.datetime(2026, 7, 26, 15, tzinfo=dt.timezone.utc), 10.0),
        (dt.datetime(2026, 7, 29, 3, tzinfo=dt.timezone.utc), 20.0),
        (dt.datetime(2026, 7, 29, 15, tzinfo=dt.timezone.utc), 30.0),
    ]
    for e in sn.evaluate_methods(series, "x"):
        assert e.n_predictions == 0, e.method


def test_moving_avg_requires_contiguous_window():
    values = hours([10.0] * 30)
    evals = by_method(sn.evaluate_methods(values, "x"))
    ma = evals["moving_avg_24h"]
    assert ma.n_predictions == 6  # hours 24..29 have full trailing windows
    assert ma.mae == 0.0
    # knock out one hour inside every window
    gappy = [(h, v) for h, v in values if h.hour != 5]
    for e in sn.evaluate_methods(gappy, "x"):
        if e.method == "moving_avg_24h":
            assert e.n_predictions == 0


def test_smape_hand_computed():
    evals = by_method(sn.evaluate_methods(hours([10, 20]), "x"))
    naive = evals["naive_1h"]  # one prediction: 10 vs 20
    assert naive.smape == pytest.approx(2 * 10 / 30)


def test_duplicate_hours_rejected():
    series = hours([1, 2]) + hours([3])
    with pytest.raises(ValueError, match="duplicate"):
        sn.evaluate_methods(series, "x")


def test_next_hour_forecast():
    day = list(range(24))
    hour, prediction = sn.next_hour_forecast(hours([float(v) for v in day]))
    assert hour == T0 + dt.timedelta(hours=24)
    assert prediction == 0.0  # seasonal_24h: same hour yesterday
    hour, prediction = sn.next_hour_forecast(hours([5.0, 7.0]), method="naive_1h")
    assert prediction == 7.0
    _, missing = sn.next_hour_forecast(hours([5.0, 7.0]), method="seasonal_168h")
    assert missing is None


def test_mase_uses_same_evaluable_hours():
    """MASE for a method is scaled by naive MAE over that method's own
    prediction hours, so partial coverage doesn't skew the comparison."""
    day = [100.0, 50.0] * 12
    two_days = hours(day * 2)
    evals = by_method(sn.evaluate_methods(two_days, "x"))
    seasonal = evals["seasonal_24h"]
    assert seasonal.mase == 0.0
    naive = evals["naive_1h"]
    assert naive.mase == pytest.approx(1.0)
