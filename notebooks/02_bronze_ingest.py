# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Bronze ingestion: raw hour files → events_raw
# MAGIC
# MAGIC Spec §21 step 5, workspace side. This notebook:
# MAGIC 1. verifies the Unity Catalog environment and that raw hour files exist
# MAGIC    (run `01_download_and_profile` first if not);
# MAGIC 2. creates `bronze.events_raw`, `bronze.events_quarantine`, and
# MAGIC    `bronze.ingestion_audit` if missing;
# MAGIC 3. ingests every raw hour file in the Volume (idempotent — MERGE on
# MAGIC    `event_id`; re-runs insert nothing);
# MAGIC 4. loads download audit records from `_download_audit.jsonl` into
# MAGIC    `bronze.ingestion_audit`;
# MAGIC 5. reconciles Bronze row counts against the schema-profile numbers.
# MAGIC
# MAGIC Design (from `docs/schema_validation_report.md` §8): the eight
# MAGIC top-level fields are kept as-is — scalars for `id`/`type`/`public`/
# MAGIC `created_at`, raw JSON strings for `actor`/`repo`/`org`/`payload`.
# MAGIC `public = false` / empty `repo` rows (Finding S6) ingest normally;
# MAGIC only unparseable lines and rows without a usable `id` quarantine.

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

from github_observatory.common.config import (
    EVENTS_QUARANTINE_TABLE,
    EVENTS_RAW_TABLE,
    INGESTION_AUDIT_TABLE,
    RAW_VOLUME_PATH,
    SCHEMA_PROFILE_TABLE,
)

print("raw volume:", RAW_VOLUME_PATH)

# COMMAND ----------

# DBTITLE 1,Discover raw hour files in the volume
import re

raw_files = sorted(
    os.path.join(RAW_VOLUME_PATH, name)
    for name in os.listdir(RAW_VOLUME_PATH)
    if re.match(r"^\d{4}-\d{2}-\d{2}-\d{1,2}\.json\.gz$", name)
)
assert raw_files, (
    f"no raw hour files in {RAW_VOLUME_PATH} — run notebooks/01_download_and_profile.py first"
)
for path in raw_files:
    print(f"{os.path.getsize(path):>12,}  {os.path.basename(path)}")

# COMMAND ----------

# DBTITLE 1,Create Bronze tables if missing
from github_observatory.ingestion.bronze_ingest import create_bronze_tables

create_bronze_tables(spark)
for table in (EVENTS_RAW_TABLE, EVENTS_QUARANTINE_TABLE, INGESTION_AUDIT_TABLE):
    print(table)
    spark.sql(f"DESCRIBE TABLE {table}").show(50, truncate=False)

# COMMAND ----------

# DBTITLE 1,Ingest every raw hour file (idempotent)
from github_observatory.ingestion.bronze_ingest import ingest_files

stats_list = ingest_files(spark, raw_files)
for s in stats_list:
    print(
        f"{s.source_file:24s} read={s.lines_read:>8,} staged={s.rows_staged:>8,} "
        f"inserted={s.rows_inserted:>8,} dup_skipped={s.duplicates_skipped:>7,} "
        f"quarantined={s.quarantined}"
    )
assert len(stats_list) == len(raw_files), "some files failed — check bronze.ingestion_audit"

# COMMAND ----------

# DBTITLE 1,Load download audit records (JSONL beside the raw files)
import json

from github_observatory.ingestion.bronze_ingest import (
    download_result_audit_row,
    write_audit_rows,
)

import glob

audit_files = sorted(glob.glob(os.path.join(RAW_VOLUME_PATH, "_download_audit*.jsonl")))
download_rows = []
for audit_jsonl in audit_files:
    with open(audit_jsonl, encoding="utf-8") as fh:
        download_rows += [download_result_audit_row(json.loads(line)) for line in fh if line.strip()]
if download_rows:
    write_audit_rows(spark, download_rows)
    print(f"loaded {len(download_rows)} download audit records from {len(audit_files)} file(s)")
else:
    print("no _download_audit*.jsonl found — skipping download-phase audit load")

# COMMAND ----------

# DBTITLE 1,Sanity: table counts and per-file reconciliation
from pyspark.sql import functions as F

events = spark.table(EVENTS_RAW_TABLE)
print("events_raw rows:      ", events.count())
print("distinct event_ids:   ", events.select("event_id").distinct().count())
print("quarantine rows:      ", spark.table(EVENTS_QUARANTINE_TABLE).count())

display(
    events.groupBy("source_file")
    .agg(
        F.count("*").alias("bronze_rows"),
        F.countDistinct("event_id").alias("distinct_ids"),
        F.min("created_at").alias("min_created_at"),
        F.max("created_at").alias("max_created_at"),
    )
    .orderBy("source_file")
)

# COMMAND ----------

# DBTITLE 1,Sanity: event-type composition vs schema profile
# The schema profile counted events per type from the same files; Bronze
# totals must match it exactly (profile event_count is per event type).
profile_counts = {
    row["event_type"]: row["event_count"]
    for row in spark.table(SCHEMA_PROFILE_TABLE)
    .select("event_type", "event_count")
    .distinct()
    .collect()
}
bronze_counts = {
    row["event_type"]: row["n"]
    for row in events.groupBy("event_type").agg(F.count("*").alias("n")).collect()
}
mismatches = {
    etype: (profile_counts.get(etype), bronze_counts.get(etype))
    for etype in set(profile_counts) | set(bronze_counts)
    if profile_counts.get(etype) != bronze_counts.get(etype)
}
print("event types (profile vs bronze):", len(profile_counts), "vs", len(bronze_counts))
if mismatches:
    print("MISMATCHES:", mismatches)
    print("(expected only if Bronze has ingested more hours than the profile sampled)")
else:
    print("event-type composition matches the schema profile exactly")

# COMMAND ----------

# DBTITLE 1,Sanity: duplicate ids and Finding S6 rows
dup_ids = (
    events.groupBy("event_id").count().filter(F.col("count") > 1).count()
)
assert dup_ids == 0, f"{dup_ids} duplicate event_ids in events_raw — MERGE key broken"
print("duplicate event_ids: 0")

s6 = events.filter((F.col("public") == False) | F.col("repo_json").isin("{}", "null") | F.col("repo_json").isNull())  # noqa: E712
print("Finding-S6-like rows (public=false / empty repo):", s6.count())
display(s6.select("event_id", "event_type", "public", "repo_json", "source_file").limit(10))

# COMMAND ----------

# DBTITLE 1,Audit trail
display(
    spark.table(INGESTION_AUDIT_TABLE)
    .orderBy(F.col("started_at").desc())
    .limit(20)
)
