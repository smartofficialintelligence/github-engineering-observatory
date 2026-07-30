# Databricks notebook source
# MAGIC %md
# MAGIC # 08 — Hourly refresh pipeline (schedulable)
# MAGIC
# MAGIC Spec §21 step 10, the retraining/refresh workflow. One idempotent
# MAGIC pass, designed to run on a schedule (see
# MAGIC `scripts/databricks_run.py create-job`):
# MAGIC
# MAGIC 1. download the latest published GH Archive hour (published ~65 min
# MAGIC    after the hour closes — target is now − 2h, walking back over any
# MAGIC    hours missed since the last run, up to 24);
# MAGIC 2. Bronze → Silver → Gold hourly over the affected dates;
# MAGIC 3. behavior/sustainability/data-quality refresh;
# MAGIC 4. forecast re-evaluation + MLflow logging.
# MAGIC
# MAGIC Every step MERGEs, so overlapping runs and re-runs are safe.

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import datetime as dt
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

spark.sql("USE CATALOG github_observatory")

# COMMAND ----------

# DBTITLE 1,Determine hours to fetch (latest published, plus catch-up)
from github_observatory.common.config import RAW_VOLUME_PATH
from github_observatory.ingestion.download_gharchive import archive_filename

MAX_CATCHUP_HOURS = 24

now = dt.datetime.now(dt.timezone.utc)
latest_published = now.replace(minute=0, second=0, microsecond=0) - dt.timedelta(hours=2)
candidates = [latest_published - dt.timedelta(hours=i) for i in range(MAX_CATCHUP_HOURS)]
missing = [
    h for h in candidates
    if not os.path.exists(os.path.join(RAW_VOLUME_PATH, archive_filename(h)))
]
print(f"latest published hour: {latest_published}")
print(f"missing from volume (last {MAX_CATCHUP_HOURS}h): {len(missing)}")

# COMMAND ----------

# DBTITLE 1,Download + Bronze
from github_observatory.ingestion.bronze_ingest import create_bronze_tables, ingest_files
from github_observatory.ingestion.download_gharchive import download_hours

new_paths = []
if missing:
    results = download_hours(sorted(missing), RAW_VOLUME_PATH)
    for r in results:
        print(f"{r.status:15s} {r.source_file} {r.error_message or ''}")
    # 404s are expected at the publication edge; ingest what arrived.
    new_paths = [r.dest_path for r in results if r.status == "downloaded"]

create_bronze_tables(spark)
if new_paths:
    stats = ingest_files(spark, new_paths)
    for s in stats:
        print(f"ingested {s.source_file}: +{s.rows_inserted:,} rows")
else:
    print("volume already current — nothing to ingest")

# COMMAND ----------

# DBTITLE 1,Silver + Gold over the affected window
from github_observatory.gold.metrics import build_gold, create_gold_tables
from github_observatory.silver.transforms import build_silver, create_silver_tables

window_start = (latest_published - dt.timedelta(hours=MAX_CATCHUP_HOURS)).date()
where = f"source_date >= DATE'{window_start}'"

create_silver_tables(spark)
create_gold_tables(spark)
print("silver:", {t: m.get("num_inserted_rows", 0) for t, m in build_silver(spark, where=where).items()})
print("gold:", build_gold(spark, where=where))

# COMMAND ----------

# DBTITLE 1,Behavior, sustainability, data quality
from github_observatory.gold.behavior import build_behavior, create_behavior_tables
from github_observatory.gold.sustainability import (
    build_sustainability,
    create_sustainability_tables,
)

create_behavior_tables(spark)
create_sustainability_tables(spark)
build_behavior(spark)
build_sustainability(spark)

from pyspark.sql import functions as F

dq = spark.table("github_observatory.gold.data_quality")
bad = dq.filter(~F.col("parity_ok") | (F.col("duplicate_event_ids") > 0)).count()
assert bad == 0, f"{bad} data-quality violations"
print("data quality green")

# COMMAND ----------

# DBTITLE 1,Forecast refresh + MLflow
from github_observatory.common.config import GOLD_ECOSYSTEM_HOURLY_TABLE
from github_observatory.forecasting.seasonal_naive import (
    create_forecast_tables,
    evaluate_methods,
    next_hour_forecast,
    read_hourly_series,
    write_evaluation,
)

create_forecast_tables(spark)

import mlflow

# Serverless Spark Connect does not expose spark.mlflow.* confs; set the
# URIs explicitly so mlflow never falls back to reading them.
mlflow.set_tracking_uri("databricks")
mlflow.set_registry_uri("databricks-uc")
mlflow.set_experiment("/Shared/github-observatory-forecast")
for target in ("total_events", "push_events"):
    series = read_hourly_series(spark, GOLD_ECOSYSTEM_HOURLY_TABLE, target)
    evals = evaluate_methods(series, target)
    write_evaluation(spark, evals)
    best = min(
        (e for e in evals if e.mase is not None),
        key=lambda e: e.mase,
        default=None,
    )
    hour, prediction = next_hour_forecast(series, method="seasonal_24h")
    with mlflow.start_run(run_name=f"hourly-{target}"):
        mlflow.log_params({"target": target, "hours_available": len(series)})
        if best:
            mlflow.log_metrics({"best_mase": best.mase, "best_mae": best.mae})
            mlflow.log_param("best_method", best.method)
    print(
        f"{target}: best={best.method if best else 'n/a'} "
        f"next {hour} -> {'gap' if prediction is None else f'{prediction:,.0f}'}"
    )
print("pipeline complete")
