# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Download GH Archive samples and profile the stream schema
# MAGIC
# MAGIC Spec §7 Tasks 2–3, workspace side. This notebook:
# MAGIC 1. verifies the Unity Catalog environment;
# MAGIC 2. downloads sample hours into the raw Volume (idempotent);
# MAGIC 3. profiles every event type's fields, types, and presence rates;
# MAGIC 4. writes `github_observatory.bronze.schema_profile`.
# MAGIC
# MAGIC Runs on serverless compute; the package modules are pure stdlib.

# COMMAND ----------

# DBTITLE 1,Verify catalog, schemas, and raw volume exist
spark.sql("USE CATALOG github_observatory")
schemas = {row[0] for row in spark.sql("SHOW SCHEMAS").collect()}
missing = {"bronze", "silver", "gold"} - schemas
assert not missing, f"missing schemas: {missing} — run notebooks/exploration/create_catalog_and_schema.py"

volumes = {row.volume_name for row in spark.sql("SHOW VOLUMES IN github_observatory.bronze").collect()}
assert "raw_files" in volumes, "missing volume github_observatory.bronze.raw_files"
print("environment OK:", sorted(schemas), sorted(volumes))

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())  # notebooks/ -> repo root
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from github_observatory.common.config import RAW_VOLUME_PATH, SCHEMA_PROFILE_TABLE

print("raw volume:", RAW_VOLUME_PATH)

# COMMAND ----------

# DBTITLE 1,Download sample hours into the raw volume (idempotent)
from github_observatory.ingestion.download_gharchive import download_hours

# Weekday peak, weekday trough, weekend — same hours profiled in
# docs/schema_validation_report.md. Extend as needed.
SAMPLE_HOURS = ["2026-07-29-15", "2026-07-29-3", "2026-07-26-15"]

results = download_hours(SAMPLE_HOURS, RAW_VOLUME_PATH)
for r in results:
    print(f"{r.status:15s} {r.source_file:24s} http={r.http_status} "
          f"bytes={r.bytes_downloaded:,} lines={r.line_count} {r.error_message or ''}")
failed = [r for r in results if r.status == "failed"]
assert not failed, f"{len(failed)} download(s) failed"

# COMMAND ----------

# DBTITLE 1,Profile the stream schema
from github_observatory.schema.profile_schema import (
    StreamProfiler,
    infer_source_hour,
    write_profile_delta,
)

profiler = StreamProfiler()
for r in results:
    path = os.path.join(RAW_VOLUME_PATH, r.source_file)
    profiler.profile_file(path, source_hour=infer_source_hour(path))

summary = profiler.summary()
print(f"events: {summary['total_events']:,}  "
      f"malformed: {summary['malformed_lines']}  "
      f"duplicate ids: {summary['keys']['event_id']['duplicates']}  "
      f"hour mismatches: {summary['event_time_vs_source_hour']['mismatches']}")
print("event types:", summary["event_type_counts"])

# COMMAND ----------

# DBTITLE 1,Write github_observatory.bronze.schema_profile
rows = profiler.profile_rows()
write_profile_delta(rows, spark, table=SCHEMA_PROFILE_TABLE, mode="overwrite")
print(f"wrote {len(rows):,} rows to {SCHEMA_PROFILE_TABLE}")

# COMMAND ----------

# DBTITLE 1,Inspect: contract fields and anomalies
from pyspark.sql import functions as F

profile = spark.table(SCHEMA_PROFILE_TABLE)
display(
    profile.filter(F.col("notes").contains("contract field"))
    .orderBy("field_path", "event_type")
)

# COMMAND ----------

# DBTITLE 1,Persist run summary beside the raw files
import json

summary_path = os.path.join(RAW_VOLUME_PATH, "_profile_summary.json")
with open(summary_path, "w", encoding="utf-8") as fh:
    json.dump(summary, fh, indent=2, ensure_ascii=False, default=str)
print("summary written to", summary_path)
