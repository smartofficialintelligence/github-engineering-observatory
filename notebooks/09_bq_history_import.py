# Databricks notebook source
# MAGIC %md
# MAGIC # 09 — BigQuery deep-history import (2016–2025, pass 1)
# MAGIC
# MAGIC Imports the decade of hourly census aggregates exported from the
# MAGIC BigQuery public `githubarchive` dataset (see
# MAGIC `scripts/bq_export_hourly.py`; artifacts + manifest uploaded to the
# MAGIC raw Volume under `bq_import/`). Steps:
# MAGIC
# MAGIC 1. migrate `gold.ecosystem_hourly` to the v3 schema (`source` column);
# MAGIC 2. land artifacts as `bronze.bq_ecosystem_hourly` with provenance;
# MAGIC 3. merge into Gold with `source = 'bigquery'` — **stream rows are
# MAGIC    never modified**; payload-derived columns stay NULL (pass 2);
# MAGIC 4. verify precedence, continuity, and totals.

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

spark.sql("USE CATALOG github_observatory")

from github_observatory.common.config import RAW_VOLUME_PATH

ARTIFACT_DIR = os.path.join(RAW_VOLUME_PATH, "bq_import")
assert os.path.exists(os.path.join(ARTIFACT_DIR, "manifest.json")), (
    f"no manifest at {ARTIFACT_DIR} — upload artifacts/bq_import/* first "
    "(scripts/databricks_run.py upload ... --dest .../raw_files/bq_import)"
)

# COMMAND ----------

# DBTITLE 1,Migrate Gold schema (adds source column, backfills 'stream')
from pyspark.sql import functions as F

from github_observatory.gold.metrics import create_gold_tables

stream_before = spark.table("gold.ecosystem_hourly").count()
create_gold_tables(spark)
migrated = (
    spark.table("gold.ecosystem_hourly").filter(F.col("source") == "stream").count()
)
assert migrated == stream_before, "migration must mark every existing row as stream"
print(f"schema v3 OK: {migrated} stream rows")

# COMMAND ----------

# DBTITLE 1,Land bronze.bq_ecosystem_hourly
from github_observatory.ingestion.bq_history import (
    create_bq_history_tables,
    ingest_bronze,
    merge_history_into_gold,
    read_artifacts,
)

create_bq_history_tables(spark)
rows = read_artifacts(ARTIFACT_DIR)
n = ingest_bronze(spark, rows)
print(f"landed {n:,} hourly rows in bronze.bq_ecosystem_hourly")

# COMMAND ----------

# DBTITLE 1,Merge history into Gold (stream wins)
metrics = merge_history_into_gold(spark)
print("gold history merge:", metrics)

# COMMAND ----------

# DBTITLE 1,Verify: precedence, counts, and continuity
gold = spark.table("gold.ecosystem_hourly")

stream_after = gold.filter(F.col("source") == "stream").count()
assert stream_after == stream_before, (
    f"stream rows changed: {stream_before} -> {stream_after}"
)

by_source = {r["source"]: r["n"] for r in
             gold.groupBy("source").agg(F.count("*").alias("n")).collect()}
print("rows by source:", by_source)

dup_hours = gold.groupBy("event_hour").count().filter(F.col("count") > 1).count()
assert dup_hours == 0, f"{dup_hours} duplicate event_hours"

null_payload_on_stream = gold.filter(
    (F.col("source") == "stream") & F.col("pr_merged").isNull()
).count()
assert null_payload_on_stream == 0, "stream rows must keep payload columns"

display(
    gold.groupBy(F.year("event_hour").alias("year"), "source")
    .agg(
        F.count("*").alias("hours"),
        F.sum("total_events").alias("total_events"),
        F.avg(F.col("bot_events") / F.col("total_events")).alias("avg_bot_share"),
    )
    .orderBy("year")
)

# COMMAND ----------

# DBTITLE 1,The decade at a glance (bot share = the AI-era signal)
display(
    gold.groupBy(F.date_trunc("month", "event_hour").alias("month"))
    .agg(
        F.sum("total_events").alias("events"),
        F.sum("push_events").alias("pushes"),
        (F.sum("bot_events") / F.sum("total_events")).alias("bot_share"),
        F.avg("distinct_actors").alias("avg_hourly_actors"),
    )
    .orderBy("month")
)
