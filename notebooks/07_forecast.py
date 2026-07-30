# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Seasonal-naive forecast slice + MLflow
# MAGIC
# MAGIC Spec §21 step 9 / §16 vertical slice. Evaluates the statistical
# MAGIC baselines (`naive_1h`, `seasonal_24h`, `seasonal_168h`,
# MAGIC `moving_avg_24h`) on next-hour `total_events` and `push_events` over
# MAGIC all ingested hourly history, persists `gold.forecast_eval` +
# MAGIC `gold.forecast_predictions`, logs runs to MLflow, and emits the
# MAGIC next-hour forecast.
# MAGIC
# MAGIC Needs contiguous history (run `05_backfill` first): with 72 hours,
# MAGIC `seasonal_24h` gets ~48 evaluable predictions; `seasonal_168h`
# MAGIC activates only after >1 week of backfill.

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

spark.sql("USE CATALOG github_observatory")

# COMMAND ----------

# DBTITLE 1,Evaluate all methods on both targets
from github_observatory.common.config import GOLD_ECOSYSTEM_HOURLY_TABLE
from github_observatory.forecasting.seasonal_naive import (
    create_forecast_tables,
    evaluate_methods,
    next_hour_forecast,
    read_hourly_series,
    write_evaluation,
)

create_forecast_tables(spark)

TARGETS = ("total_events", "push_events")
all_evals = {}
for target in TARGETS:
    series = read_hourly_series(spark, GOLD_ECOSYSTEM_HOURLY_TABLE, target)
    evals = evaluate_methods(series, target)
    all_evals[target] = (series, evals)
    run_id = write_evaluation(spark, evals)
    for e in evals:
        print(
            f"{target:14s} {e.method:16s} n={e.n_predictions:>4} "
            f"mae={e.mae if e.mae is None else round(e.mae, 1)} "
            f"smape={e.smape if e.smape is None else round(e.smape, 4)} "
            f"mase={e.mase if e.mase is None else round(e.mase, 3)}"
        )

# COMMAND ----------

# DBTITLE 1,Log to MLflow
import mlflow

# Serverless Spark Connect does not expose spark.mlflow.* confs; set the
# URIs explicitly so mlflow never falls back to reading them.
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Shared/github-observatory-forecast")
for target, (series, evals) in all_evals.items():
    for e in evals:
        if e.mae is None:
            continue
        with mlflow.start_run(run_name=f"{target}-{e.method}"):
            mlflow.log_params({
                "target": target,
                "method": e.method,
                "hours_available": len(series),
                "metric_definitions_version": 2,
            })
            mlflow.log_metrics({
                "n_predictions": e.n_predictions,
                "mae": e.mae,
                "smape": e.smape if e.smape is not None else float("nan"),
                "mase": e.mase if e.mase is not None else float("nan"),
            })
print("logged to MLflow experiment /Shared/github-observatory-forecast")

# COMMAND ----------

# DBTITLE 1,Next-hour forecast
for target, (series, _) in all_evals.items():
    for method in ("seasonal_24h", "naive_1h"):
        hour, prediction = next_hour_forecast(series, method=method)
        print(f"{target:14s} {method:14s} {hour} -> "
              f"{'no baseline (gap)' if prediction is None else f'{prediction:,.0f}'}")

# COMMAND ----------

# DBTITLE 1,Persisted evaluation
display(spark.table("gold.forecast_eval").orderBy("target", "mase"))
display(
    spark.table("gold.forecast_predictions")
    .orderBy("target", "method", "event_hour")
    .limit(50)
)
