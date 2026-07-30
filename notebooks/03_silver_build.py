# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Silver build: typed events + lifecycle tables
# MAGIC
# MAGIC Spec §21 step 6, workspace side. This notebook:
# MAGIC 1. verifies the environment and that `bronze.events_raw` is populated
# MAGIC    (run `02_bronze_ingest` first if not);
# MAGIC 2. creates the five Silver tables if missing;
# MAGIC 3. runs the idempotent MERGE transforms (`silver.events`, `pr_events`,
# MAGIC    `issue_events`, `review_events`, `release_events`);
# MAGIC 4. reconciles the SQL build against the pure-python reference
# MAGIC    implementation on a row sample and against known sample-level counts.
# MAGIC
# MAGIC Design decisions (from `docs/schema_validation_report.md` §8): no
# MAGIC `pr_state`/`pr_merged` columns (gone from the 2026 stream) — lifecycle
# MAGIC comes from `payload_action`; S6 rows are flagged via `quality_flag`,
# MAGIC never dropped; `actor_is_bot` is the `[bot]`-suffix lower bound (OQ-8);
# MAGIC `issue_events.is_pull_request` must gate `(repo_id, number)` joins.

# COMMAND ----------

# DBTITLE 1,Verify environment and Bronze population
spark.sql("USE CATALOG github_observatory")
bronze_count = spark.sql("SELECT COUNT(*) AS n FROM bronze.events_raw").collect()[0]["n"]
assert bronze_count > 0, "bronze.events_raw is empty — run notebooks/02_bronze_ingest.py first"
print(f"bronze.events_raw: {bronze_count:,} rows")

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())  # notebooks/ -> repo root
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from github_observatory.common.config import (
    SILVER_EVENTS_TABLE,
    SILVER_ISSUE_EVENTS_TABLE,
    SILVER_PR_EVENTS_TABLE,
    SILVER_RELEASE_EVENTS_TABLE,
    SILVER_REVIEW_EVENTS_TABLE,
)

# COMMAND ----------

# DBTITLE 1,Create Silver tables if missing
from github_observatory.silver.transforms import create_silver_tables

create_silver_tables(spark)
print("silver tables ready")

# COMMAND ----------

# DBTITLE 1,Run the MERGE transforms (idempotent)
from github_observatory.silver.transforms import build_silver

metrics = build_silver(spark)
for table, m in metrics.items():
    print(f"{table:45s} inserted={m.get('num_inserted_rows', 0):>9,}")

# COMMAND ----------

# DBTITLE 1,Reconcile: silver.events vs bronze.events_raw
from pyspark.sql import functions as F

silver_events = spark.table(SILVER_EVENTS_TABLE)
silver_count = silver_events.count()
assert silver_count == bronze_count, (
    f"silver.events {silver_count:,} != bronze.events_raw {bronze_count:,} — "
    "Silver must carry every Bronze row (flagged, never dropped)"
)
dup_ids = silver_events.groupBy("event_id").count().filter(F.col("count") > 1).count()
assert dup_ids == 0, f"{dup_ids} duplicate event_ids in silver.events"
null_created = silver_events.filter(
    F.col("created_at").isNull() & F.col("quality_flag").isNull()
).count()
assert null_created == 0, "unflagged rows with null created_at"
print(f"silver.events: {silver_count:,} rows, 0 duplicate ids")

print("quality flags:")
display(silver_events.groupBy("quality_flag").count())

# COMMAND ----------

# DBTITLE 1,Reconcile: SQL build vs python reference implementation
# Re-derive silver rows for a sample of Bronze rows with the stdlib
# reference extractors and compare scalar-for-scalar against what the
# SQL MERGE wrote. The two implementations must never drift.
import datetime as dt

from github_observatory.silver.transforms import silver_event_row

SAMPLE_N = 2000
bronze_sample = (
    spark.table("bronze.events_raw")
    .orderBy(F.crc32(F.col("event_id")))  # deterministic pseudo-random sample
    .limit(SAMPLE_N)
    .collect()
)
sampled_ids = [r["event_id"] for r in bronze_sample]
silver_rows = {
    r["event_id"]: r.asDict()
    for r in silver_events.filter(F.col("event_id").isin(sampled_ids)).collect()
}

COMPARE_COLS = [
    "event_type", "public", "actor_id", "actor_login", "actor_is_bot",
    "repo_id", "repo_name", "org_id", "org_login",
    "payload_action", "payload_ref", "payload_ref_type", "push_id", "quality_flag",
]
mismatches = []
for bronze_row in bronze_sample:
    ref = silver_event_row(bronze_row.asDict())
    got = silver_rows.get(bronze_row["event_id"])
    if got is None:
        mismatches.append((bronze_row["event_id"], "MISSING_IN_SILVER", None, None))
        continue
    for col in COMPARE_COLS:
        if ref[col] != got[col]:
            mismatches.append((bronze_row["event_id"], col, ref[col], got[col]))
    # timestamp column: compare instants
    ref_ts = bronze_row["created_at"]
    if ref_ts and got["created_at"]:
        expected = dt.datetime.fromisoformat(ref_ts.replace("Z", "+00:00"))
        actual = got["created_at"].replace(tzinfo=dt.timezone.utc)
        if expected != actual:
            mismatches.append((bronze_row["event_id"], "created_at", expected, actual))

assert not mismatches, f"{len(mismatches)} mismatches, first 5: {mismatches[:5]}"
print(f"reference reconciliation: {len(bronze_sample):,} rows, {len(COMPARE_COLS) + 1} columns, 0 mismatches")

# COMMAND ----------

# DBTITLE 1,Sanity: lifecycle tables and known distributions
for table in (
    SILVER_PR_EVENTS_TABLE,
    SILVER_ISSUE_EVENTS_TABLE,
    SILVER_REVIEW_EVENTS_TABLE,
    SILVER_RELEASE_EVENTS_TABLE,
):
    print(f"{table:45s} {spark.table(table).count():>9,} rows")

print("\nPR actions (expect 'merged' as the primary merge signal, Finding S2):")
display(
    spark.table(SILVER_PR_EVENTS_TABLE)
    .filter(F.col("event_type") == "PullRequestEvent")
    .groupBy("action").count().orderBy(F.desc("count"))
)

print("bot share of events (expect ~6.1% on the sampled hours, OQ-8 lower bound):")
display(silver_events.groupBy("actor_is_bot").count())

print("issue_events is_pull_request partition (PR comments share the number space):")
display(spark.table(SILVER_ISSUE_EVENTS_TABLE).groupBy("event_type", "is_pull_request").count())

# COMMAND ----------

# DBTITLE 1,Sanity: review states and release adoption signal
display(spark.table(SILVER_REVIEW_EVENTS_TABLE).groupBy("review_state").count())
display(
    spark.table(SILVER_RELEASE_EVENTS_TABLE)
    .select("tag_name", "prerelease", "assets_count", "assets_download_count", "published_at")
    .orderBy(F.desc("assets_download_count"))
    .limit(10)
)
