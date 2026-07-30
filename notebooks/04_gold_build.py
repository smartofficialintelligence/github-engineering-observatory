# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Gold build: ecosystem_hourly + velocity
# MAGIC
# MAGIC Spec §21 step 7, workspace side. This notebook:
# MAGIC 1. verifies `silver.events` is populated (run `03_silver_build` first);
# MAGIC 2. creates `gold.ecosystem_hourly` and the `gold.ecosystem_velocity`
# MAGIC    view if missing;
# MAGIC 3. runs the MERGE-upsert build (re-runs recompute matched hours);
# MAGIC 4. asserts invariants and displays the hourly metrics.
# MAGIC
# MAGIC Definitions are bound by `docs/metric_definitions.md` **v1**
# MAGIC (event-time grain, quality-flagged rows excluded, OQ-7 production
# MAGIC whitelist, bots included except `_human` columns). The same MERGE
# MAGIC SQL is exercised pre-push by `tests/integration`; this notebook is
# MAGIC the authoritative check on serverless.

# COMMAND ----------

# DBTITLE 1,Verify environment and Silver population
spark.sql("USE CATALOG github_observatory")
silver_clean = spark.sql(
    "SELECT COUNT(*) AS n FROM silver.events "
    "WHERE quality_flag IS NULL AND created_at IS NOT NULL"
).collect()[0]["n"]
assert silver_clean > 0, "silver.events is empty — run notebooks/03_silver_build.py first"
print(f"silver.events clean rows: {silver_clean:,}")

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())  # notebooks/ -> repo root
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

from github_observatory.common.config import (
    GOLD_ECOSYSTEM_HOURLY_TABLE,
    GOLD_ECOSYSTEM_VELOCITY_VIEW,
)

# COMMAND ----------

# DBTITLE 1,Create Gold objects and run the build
from github_observatory.gold.metrics import build_gold, create_gold_tables

create_gold_tables(spark)
metrics = build_gold(spark)
print("merge metrics:", metrics)

# COMMAND ----------

# DBTITLE 1,Invariants
from pyspark.sql import functions as F

hourly = spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE)

total = hourly.agg(F.sum("total_events")).collect()[0][0]
assert total == silver_clean, (
    f"sum(total_events) {total:,} != clean silver rows {silver_clean:,}"
)

dup_hours = hourly.groupBy("event_hour").count().filter(F.col("count") > 1).count()
assert dup_hours == 0, f"{dup_hours} duplicate event_hour rows"

negative = hourly.filter(
    (F.col("total_events") < F.col("push_events"))
    | (F.col("total_events") < F.col("production_events"))
    | (F.col("total_events_human") > F.col("total_events"))
).count()
assert negative == 0, "column hierarchy violated (human > total or part > whole)"

pr_merged_gold = hourly.agg(F.sum("pr_merged")).collect()[0][0]
pr_merged_silver = spark.sql(
    "SELECT COUNT(*) AS n FROM silver.events "
    "WHERE quality_flag IS NULL AND event_type = 'PullRequestEvent' "
    "AND payload_action = 'merged'"
).collect()[0]["n"]
assert pr_merged_gold == pr_merged_silver, "pr_merged mismatch vs silver"

print(f"invariants OK: {hourly.count()} hours, {total:,} events, "
      f"pr_merged {pr_merged_gold}")

# COMMAND ----------

# DBTITLE 1,Hourly metrics
display(hourly.orderBy("event_hour"))

# COMMAND ----------

# DBTITLE 1,Velocity view (deltas NULL until backfill provides neighbour hours)
display(spark.table(GOLD_ECOSYSTEM_VELOCITY_VIEW).orderBy("event_hour"))

# COMMAND ----------

# DBTITLE 1,Composition summary
display(
    hourly.select(
        "event_hour", "total_events", "push_events", "production_events",
        "pr_opened", "pr_merged", "issues_opened", "issues_closed",
        "releases_published", "bot_events", "distinct_actors", "distinct_repos",
    ).orderBy("event_hour")
)
