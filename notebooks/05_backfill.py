# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Backfill a contiguous hour window
# MAGIC
# MAGIC Downloads a contiguous range of GH Archive hours into the raw Volume
# MAGIC and runs Bronze → Silver → Gold over it. Contiguity is what the
# MAGIC velocity view, flow metrics, retention, and forecast baselines need —
# MAGIC the three original sample hours are isolated points.
# MAGIC
# MAGIC Default window: 72 hours, 2026-07-27 00:00 – 2026-07-29 23:00 UTC
# MAGIC (~1.5 GB raw; well inside the OQ-6 budget). Every step is idempotent —
# MAGIC re-running skips existing files and MERGEs insert nothing new.

# COMMAND ----------

# DBTITLE 1,Parameters
import datetime as dt

BACKFILL_START = dt.datetime(2026, 7, 27, 0, tzinfo=dt.timezone.utc)
BACKFILL_HOURS = 72

hours = [BACKFILL_START + dt.timedelta(hours=i) for i in range(BACKFILL_HOURS)]
print(f"window: {hours[0]} .. {hours[-1]} ({len(hours)} hours)")

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from github_observatory.common.config import RAW_VOLUME_PATH

# COMMAND ----------

# DBTITLE 1,Download (idempotent; ~20 MB/hour)
from github_observatory.ingestion.download_gharchive import download_hours

results = download_hours(hours, RAW_VOLUME_PATH)
failed = [r for r in results if r.status == "failed"]
downloaded = sum(1 for r in results if r.status == "downloaded")
skipped = sum(1 for r in results if r.status == "skipped_exists")
print(f"downloaded {downloaded}, skipped {skipped}, failed {len(failed)}")
for r in failed:
    print("FAILED", r.source_file, r.error_message)
assert not failed, f"{len(failed)} downloads failed"

# COMMAND ----------

# DBTITLE 1,Bronze ingest (idempotent MERGE per file)
from github_observatory.ingestion.bronze_ingest import create_bronze_tables, ingest_files
from github_observatory.ingestion.download_gharchive import archive_filename

create_bronze_tables(spark)
paths = [os.path.join(RAW_VOLUME_PATH, archive_filename(h)) for h in hours]
stats_list = ingest_files(spark, paths)
total_inserted = sum(s.rows_inserted for s in stats_list)
total_quarantined = sum(s.quarantined for s in stats_list)
print(f"files {len(stats_list)}, inserted {total_inserted:,}, quarantined {total_quarantined}")
assert len(stats_list) == len(paths), "some files failed — check bronze.ingestion_audit"

# COMMAND ----------

# DBTITLE 1,Silver + Gold hourly over the window
from github_observatory.silver.transforms import build_silver, create_silver_tables
from github_observatory.gold.metrics import build_gold, create_gold_tables

create_silver_tables(spark)
where = (
    f"source_date BETWEEN DATE'{hours[0].date()}' AND DATE'{hours[-1].date()}'"
)
silver_metrics = build_silver(spark, where=where)
for table, m in silver_metrics.items():
    print(f"{table:45s} inserted={m.get('num_inserted_rows', 0):>10,}")

create_gold_tables(spark)
gold_metrics = build_gold(spark, where=where)
print("gold:", gold_metrics)

# COMMAND ----------

# DBTITLE 1,Verify contiguity of the hourly series
from pyspark.sql import functions as F

hourly = (
    spark.table("github_observatory.gold.ecosystem_hourly")
    .filter(
        (F.col("event_hour") >= hours[0].replace(tzinfo=None))
        & (F.col("event_hour") <= hours[-1].replace(tzinfo=None))
    )
)
n = hourly.count()
print(f"hourly rows in window: {n}/{len(hours)}")
assert n == len(hours), "window not contiguous — check bronze.ingestion_audit for gaps"
display(hourly.orderBy("event_hour").select(
    "event_hour", "total_events", "push_events", "production_events", "pr_merged"
))
