"""Seasonal-naive forecast + statistical baselines (spec §21 step 9, §16).

The vertical slice: forecast next-hour ``total_events`` and
``push_events`` from ``gold.ecosystem_hourly`` with transparent
statistical baselines, evaluate them honestly, and persist both the
per-hour predictions and the summary evaluation. MLflow logging happens
notebook-side (the workspace owns the tracking server); this module
stays pure stdlib and fully unit-testable.

Methods (all gap-aware — a prediction exists only when the exact lag
hour exists, mirroring the velocity view's no-wrong-lag policy):

* ``naive_1h`` — last hour's value.
* ``seasonal_24h`` — same hour yesterday.
* ``seasonal_168h`` — same hour last week (needs >1 week of history).
* ``moving_avg_24h`` — mean of the trailing 24 contiguous hours.

Evaluation: MAE, sMAPE, and MASE (MAE scaled by ``naive_1h``'s MAE over
the same evaluable hours, so methods are compared on identical ground;
MASE < 1 beats naive). Hours with no baseline are skipped, not imputed.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import logging
import uuid
from typing import Any, Callable, Sequence

from github_observatory.common.config import (
    GOLD_FORECAST_EVAL_TABLE,
    GOLD_FORECAST_PREDICTIONS_TABLE,
)

logger = logging.getLogger("github_observatory.forecasting")

HOUR = dt.timedelta(hours=1)

FORECAST_EVAL_DDL = (
    "target STRING, method STRING, eval_start TIMESTAMP, eval_end TIMESTAMP, "
    "hours_available BIGINT, n_predictions BIGINT, "
    "mae DOUBLE, smape DOUBLE, mase DOUBLE, "
    "forecast_run_id STRING, evaluated_at TIMESTAMP"
)

FORECAST_PREDICTIONS_DDL = (
    "event_hour TIMESTAMP, target STRING, method STRING, "
    "prediction DOUBLE, actual DOUBLE, abs_error DOUBLE, "
    "forecast_run_id STRING, evaluated_at TIMESTAMP"
)


@dataclasses.dataclass
class MethodEval:
    target: str
    method: str
    n_predictions: int
    mae: float | None
    smape: float | None
    mase: float | None
    predictions: list[tuple[dt.datetime, float, float]]  # (hour, prediction, actual)


def _predictor_factory(series: dict[dt.datetime, float]) -> dict[str, Callable]:
    def lag(hours: int) -> Callable[[dt.datetime], float | None]:
        def predict(hour: dt.datetime) -> float | None:
            return series.get(hour - hours * HOUR)
        return predict

    def moving_avg_24h(hour: dt.datetime) -> float | None:
        window = [series.get(hour - h * HOUR) for h in range(1, 25)]
        if any(v is None for v in window):
            return None  # contiguity required; gaps disqualify the window
        return sum(window) / 24.0

    return {
        "naive_1h": lag(1),
        "seasonal_24h": lag(24),
        "seasonal_168h": lag(168),
        "moving_avg_24h": moving_avg_24h,
    }


def evaluate_methods(
    hours: Sequence[tuple[dt.datetime, float]],
    target: str,
) -> list[MethodEval]:
    """Evaluate every method over an hourly series.

    ``hours``: (hour, value) pairs, any order, hours unique. For fair
    comparison, MASE for every method is scaled by naive_1h's MAE over
    that method's own evaluable hours.
    """
    series = {hour: float(value) for hour, value in hours}
    if len(series) != len(hours):
        raise ValueError("duplicate hours in series")
    predictors = _predictor_factory(series)
    naive = predictors["naive_1h"]

    evals: list[MethodEval] = []
    for method, predict in predictors.items():
        rows: list[tuple[dt.datetime, float, float]] = []
        naive_errors: list[float] = []
        for hour in sorted(series):
            prediction = predict(hour)
            if prediction is None:
                continue
            rows.append((hour, prediction, series[hour]))
            naive_prediction = naive(hour)
            if naive_prediction is not None:
                naive_errors.append(abs(naive_prediction - series[hour]))
        if rows:
            errors = [abs(p - a) for _, p, a in rows]
            mae = sum(errors) / len(rows)
            smape_terms = [
                2.0 * abs(p - a) / (abs(p) + abs(a))
                for _, p, a in rows
                if (abs(p) + abs(a)) > 0
            ]
            smape = sum(smape_terms) / len(smape_terms) if smape_terms else None
            naive_mae = sum(naive_errors) / len(naive_errors) if naive_errors else None
            mase = mae / naive_mae if naive_mae else None
        else:
            mae = smape = mase = None
        evals.append(
            MethodEval(
                target=target, method=method, n_predictions=len(rows),
                mae=mae, smape=smape, mase=mase, predictions=rows,
            )
        )
    return evals


def next_hour_forecast(
    hours: Sequence[tuple[dt.datetime, float]],
    method: str = "seasonal_24h",
) -> tuple[dt.datetime, float | None]:
    """Forecast the hour after the latest observed hour."""
    series = {hour: float(value) for hour, value in hours}
    target_hour = max(series) + HOUR
    prediction = _predictor_factory(series)[method](target_hour)
    return target_hour, prediction


# -- Delta persistence (call from Databricks) ---------------------------------


def read_hourly_series(spark: Any, table: str, column: str) -> list[tuple[dt.datetime, float]]:
    rows = spark.sql(f"SELECT event_hour, {column} FROM {table}").collect()
    return [(r["event_hour"], float(r[column])) for r in rows]


def write_evaluation(
    spark: Any,
    evals: Sequence[MethodEval],
    *,
    run_id: str | None = None,
) -> str:
    """Persist evaluation summaries and per-hour predictions (idempotent:
    summaries MERGE on (target, method); predictions on hour+target+method)."""
    run_id = run_id or uuid.uuid4().hex

    eval_rows = []
    prediction_rows = []
    for e in evals:
        if not e.predictions:
            continue
        hours = [h for h, _, _ in e.predictions]
        eval_rows.append((
            e.target, e.method, min(hours).isoformat(), max(hours).isoformat(),
            len(e.predictions), e.n_predictions, e.mae, e.smape, e.mase, run_id,
        ))
        for hour, prediction, actual in e.predictions:
            prediction_rows.append((
                hour.isoformat(), e.target, e.method,
                prediction, actual, abs(prediction - actual), run_id,
            ))

    if eval_rows:
        df = spark.createDataFrame(
            eval_rows,
            schema=(
                "target STRING, method STRING, eval_start STRING, eval_end STRING, "
                "hours_available BIGINT, n_predictions BIGINT, "
                "mae DOUBLE, smape DOUBLE, mase DOUBLE, forecast_run_id STRING"
            ),
        )
        df.createOrReplaceTempView("_forecast_eval_stage")
        spark.sql(f"""
            MERGE INTO {GOLD_FORECAST_EVAL_TABLE} AS t
            USING (
                SELECT target, method,
                       CAST(eval_start AS TIMESTAMP) AS eval_start,
                       CAST(eval_end AS TIMESTAMP) AS eval_end,
                       hours_available, n_predictions, mae, smape, mase,
                       forecast_run_id, current_timestamp() AS evaluated_at
                FROM _forecast_eval_stage
            ) AS s
            ON t.target = s.target AND t.method = s.method
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        spark.catalog.dropTempView("_forecast_eval_stage")

    if prediction_rows:
        df = spark.createDataFrame(
            prediction_rows,
            schema=(
                "event_hour STRING, target STRING, method STRING, "
                "prediction DOUBLE, actual DOUBLE, abs_error DOUBLE, "
                "forecast_run_id STRING"
            ),
        )
        df.createOrReplaceTempView("_forecast_pred_stage")
        spark.sql(f"""
            MERGE INTO {GOLD_FORECAST_PREDICTIONS_TABLE} AS t
            USING (
                SELECT CAST(event_hour AS TIMESTAMP) AS event_hour, target, method,
                       prediction, actual, abs_error, forecast_run_id,
                       current_timestamp() AS evaluated_at
                FROM _forecast_pred_stage
            ) AS s
            ON t.event_hour = s.event_hour AND t.target = s.target AND t.method = s.method
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
        spark.catalog.dropTempView("_forecast_pred_stage")

    logger.info(
        "forecast eval written: %d summaries, %d predictions (run %s)",
        len(eval_rows), len(prediction_rows), run_id,
    )
    return run_id


def create_forecast_tables(spark: Any) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_FORECAST_EVAL_TABLE} "
        f"({FORECAST_EVAL_DDL}) USING DELTA"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {GOLD_FORECAST_PREDICTIONS_TABLE} "
        f"({FORECAST_PREDICTIONS_DDL}) USING DELTA"
    )
