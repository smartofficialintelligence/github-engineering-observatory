# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — Behavior, sustainability, and data-quality build
# MAGIC
# MAGIC Spec §21 steps 8 and 10 (metrics), workspace side. Builds, from
# MAGIC Silver and `gold.ecosystem_hourly`:
# MAGIC
# MAGIC * `gold.flow_daily` — CV / Fano / burstiness / entropy candidates (OQ-9)
# MAGIC * `gold.actor_first_seen` + `gold.contribution_daily`
# MAGIC * `gold.engagement_daily` — stream-observed stars/forks (OQ-1 caveat)
# MAGIC * `gold.data_quality` — per-file conservation checks
# MAGIC * `gold.actor_retention_daily` + `gold.network_daily`
# MAGIC
# MAGIC All definitions bound by `docs/metric_definitions.md` v2. Everything
# MAGIC is a MERGE-upsert; re-runs recompute without duplicating.

# COMMAND ----------

# DBTITLE 1,Verify environment
spark.sql("USE CATALOG github_observatory")
silver_clean = spark.sql(
    "SELECT COUNT(*) AS n FROM silver.events "
    "WHERE quality_flag IS NULL AND created_at IS NOT NULL"
).collect()[0]["n"]
assert silver_clean > 0, "silver.events is empty — run notebook 03 first"
print(f"silver clean rows: {silver_clean:,}")

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

# COMMAND ----------

# DBTITLE 1,Build behavior + sustainability tables
from github_observatory.gold.behavior import build_behavior, create_behavior_tables
from github_observatory.gold.sustainability import (
    build_sustainability,
    create_sustainability_tables,
)

create_behavior_tables(spark)
create_sustainability_tables(spark)
for table, m in {**build_behavior(spark), **build_sustainability(spark)}.items():
    print(f"{table:50s} {m}")

# COMMAND ----------

# DBTITLE 1,Data quality must be green
from pyspark.sql import functions as F

dq = spark.table("github_observatory.gold.data_quality")
bad = dq.filter(~F.col("parity_ok") | (F.col("duplicate_event_ids") > 0))
assert bad.count() == 0, "data-quality violations — pipeline bug"
display(dq.orderBy("source_hour"))

# COMMAND ----------

# DBTITLE 1,Contribution and concentration
display(
    spark.table("github_observatory.gold.contribution_daily").orderBy("event_date")
)

# COMMAND ----------

# DBTITLE 1,Flow candidates (partial days marked by hours_observed)
display(spark.table("github_observatory.gold.flow_daily").orderBy("event_date"))

# COMMAND ----------

# DBTITLE 1,Retention and network structure
display(
    spark.table("github_observatory.gold.actor_retention_daily").orderBy("event_date")
)
display(spark.table("github_observatory.gold.network_daily").orderBy("event_date"))

# COMMAND ----------

# DBTITLE 1,Engagement (stream-observed; OQ-1 coverage caveat)
display(spark.table("github_observatory.gold.engagement_daily").orderBy("event_date"))
