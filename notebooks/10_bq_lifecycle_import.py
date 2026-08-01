# Databricks notebook source
# MAGIC %md
# MAGIC # 10 — BigQuery deep-history import (pass 2, payload-derived)
# MAGIC
# MAGIC Lands the pass-2 lifecycle Parquet exported from Dataproc Serverless
# MAGIC (see `pipelines/gcp_lifecycle_backfill/`) into
# MAGIC `bronze.bq_lifecycle_hourly`. Steps:
# MAGIC
# MAGIC 1. verify the pass-2 artifacts are on the raw Volume under `bq_lifecycle/`;
# MAGIC 2. create/migrate `bronze.bq_lifecycle_hourly`;
# MAGIC 3. MERGE every year's Parquet into Bronze (idempotent);
# MAGIC 4. assert the closure-taxonomy and OQ-7 whitelist invariants row-by-row
# MAGIC    (spec §Validation checks #3, #4 — hard failures);
# MAGIC 5. migrate `gold.ecosystem_hourly` to v4 (15 new columns) and MERGE
# MAGIC    the pass-2 rows into Gold as `source = 'bigquery'` — stream rows
# MAGIC    are never modified (mirror of notebook 09's precedence rule);
# MAGIC 6. print a by-year summary and cross-check parity with pass-1.

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

spark.sql("USE CATALOG github_observatory")

from github_observatory.common.config import (
    BQ_LIFECYCLE_HOURLY_TABLE,
    RAW_VOLUME_PATH,
)

ARTIFACT_DIR = os.path.join(RAW_VOLUME_PATH, "bq_lifecycle")
assert os.path.exists(ARTIFACT_DIR), (
    f"no artifacts at {ARTIFACT_DIR} — upload pass-2 output first: "
    "gcloud storage cp -r gs://<bucket>/lifecycle_out/year=* artifacts/bq_lifecycle/ "
    "&& python scripts/databricks_run.py upload artifacts/bq_lifecycle "
    f"--dest {ARTIFACT_DIR}"
)

# COMMAND ----------

# DBTITLE 1,Create bronze.bq_lifecycle_hourly
from github_observatory.ingestion.bq_lifecycle_history import (
    ALL_METRIC_COLUMNS,
    METRIC_SPEC_VERSION,
    create_bq_lifecycle_tables,
    ingest_bronze_from_parquet,
)

create_bq_lifecycle_tables(spark)
print(f"target: {BQ_LIFECYCLE_HOURLY_TABLE} — spec v{METRIC_SPEC_VERSION}, "
      f"{len(ALL_METRIC_COLUMNS)} metric columns")

# COMMAND ----------

# DBTITLE 1,Ingest each year's Parquet from the Volume
year_dirs = sorted(
    os.path.join(ARTIFACT_DIR, d) for d in os.listdir(ARTIFACT_DIR)
    if d.startswith("year=")
    and os.path.isdir(os.path.join(ARTIFACT_DIR, d))
)
assert year_dirs, f"no year=YYYY subdirectories under {ARTIFACT_DIR}"

total = 0
for year_dir in year_dirs:
    year = os.path.basename(year_dir).split("=", 1)[1]
    metrics = ingest_bronze_from_parquet(spark, year_dir)
    total += 1
    print(f"{year}: {metrics}")

print(f"\ningested {len(year_dirs)} year(s) into {BQ_LIFECYCLE_HOURLY_TABLE}")

# COMMAND ----------

# DBTITLE 1,Spec §Validation check #3 — OQ-7 whitelist sum invariant
whitelist_violations = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {BQ_LIFECYCLE_HOURLY_TABLE}
    WHERE production_events !=
          push_events
          + pr_opened + pr_merged + pr_closed_no_merge + pr_reopened
          + issues_opened + issues_closed + issues_reopened
          + releases_published
""").collect()[0]["n"]
assert whitelist_violations == 0, (
    f"OQ-7 whitelist sum invariant violated on {whitelist_violations} hours"
)
print(f"OQ-7 whitelist invariant: OK (0 violations)")

# COMMAND ----------

# DBTITLE 1,Spec §Validation check #4 — closure-taxonomy sum invariant
taxonomy_violations = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {BQ_LIFECYCLE_HOURLY_TABLE}
    WHERE issues_closed_completed + issues_closed_not_planned
          + issues_closed_duplicate + issues_closed_unknown
          != issues_closed
""").collect()[0]["n"]
assert taxonomy_violations == 0, (
    f"closure-taxonomy sum invariant violated on {taxonomy_violations} hours"
)
print(f"closure-taxonomy invariant: OK (0 violations)")

# COMMAND ----------

# DBTITLE 1,Migrate Gold to v4 (adds 15 lifecycle columns if missing)
from github_observatory.gold.metrics import (
    METRIC_DEFINITIONS_VERSION,
    V4_LIFECYCLE_COLUMNS,
    create_gold_tables,
)

gold_columns_before = {
    f.name for f in spark.table("gold.ecosystem_hourly").schema.fields
}
create_gold_tables(spark)
gold_columns_after = {
    f.name for f in spark.table("gold.ecosystem_hourly").schema.fields
}
added = gold_columns_after - gold_columns_before
missing_v4 = set(V4_LIFECYCLE_COLUMNS) - gold_columns_after
assert not missing_v4, f"v4 columns still missing after migration: {missing_v4}"
print(f"Gold at v{METRIC_DEFINITIONS_VERSION}; added this run: {sorted(added) or 'none'}")

# COMMAND ----------

# DBTITLE 1,Merge lifecycle history into Gold (stream wins)
from github_observatory.ingestion.bq_lifecycle_history import merge_lifecycle_into_gold

from pyspark.sql import functions as F

stream_before = spark.table("gold.ecosystem_hourly").filter(F.col("source") == "stream").count()
merge_metrics = merge_lifecycle_into_gold(spark)
print("gold lifecycle merge:", merge_metrics)

stream_after = spark.table("gold.ecosystem_hourly").filter(F.col("source") == "stream").count()
assert stream_after == stream_before, (
    f"stream rows changed by lifecycle merge: {stream_before} -> {stream_after}"
)

# COMMAND ----------

# DBTITLE 1,By-year summary + census parity with pass-1
summary = (
    spark.table(BQ_LIFECYCLE_HOURLY_TABLE)
    .groupBy(F.year("event_hour").alias("year"))
    .agg(
        F.count("*").alias("hours"),
        F.sum("total_events").alias("total_events"),
        F.sum("pr_merged").alias("pr_merged"),
        F.sum("issues_closed").alias("issues_closed"),
        F.sum("reviews_approved").alias("reviews_approved"),
        F.sum("issue_comments_on_prs").alias("issue_comments_on_prs"),
        F.sum("releases_published").alias("releases_published"),
    )
    .orderBy("year")
)
display(summary)

# Cross-check: pass-2 census columns must match pass-1 for overlap hours.
# Spec §Validation check #2 — any mismatch is a bug in one of the pipelines.
parity_gaps = spark.sql("""
    SELECT COUNT(*) AS n
    FROM github_observatory.bronze.bq_lifecycle_hourly l
    JOIN github_observatory.bronze.bq_ecosystem_hourly e USING (event_hour)
    WHERE l.total_events != e.total_events
       OR l.push_events != e.push_events
       OR l.bot_events != e.bot_events
""").collect()[0]["n"]
assert parity_gaps == 0, f"pass-1 vs pass-2 census parity violated on {parity_gaps} hours"
print("pass-1/pass-2 census parity: OK (0 divergences)")
