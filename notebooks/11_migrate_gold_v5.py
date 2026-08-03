# Databricks notebook source
# MAGIC %md
# MAGIC # 11 — Migrate gold.ecosystem_hourly to v5 (comparability columns)
# MAGIC
# MAGIC Adds `era`, `era_ordinal` and `in_outage` and backfills them for every
# MAGIC existing row. All three derive from `event_hour` via
# MAGIC `src/github_observatory/schema/eras.py`, so this notebook holds no
# MAGIC dates of its own — re-running it after a boundary is pinned or an
# MAGIC outage is found reclassifies history to match.
# MAGIC
# MAGIC Additive and idempotent: `ALTER TABLE ADD COLUMNS` for whatever is
# MAGIC missing, then an `UPDATE` touching only rows whose classification is
# MAGIC NULL. No metric value is read or written.
# MAGIC
# MAGIC **Run this before deploying the dashboards** — their standardizable
# MAGIC queries reference `in_outage`, and every such tile errors on an
# MAGIC unknown column until this has run.

# COMMAND ----------

# DBTITLE 1,Make the repo package importable
import os
import sys

repo_root = os.path.dirname(os.getcwd())
src_path = os.path.join(repo_root, "src")
if src_path not in sys.path:
    sys.path.insert(0, src_path)

spark.sql("USE CATALOG github_observatory")

from github_observatory.common.config import GOLD_ECOSYSTEM_HOURLY_TABLE
from github_observatory.gold.metrics import (
    METRIC_DEFINITIONS_VERSION,
    V5_COMPARABILITY_COLUMNS,
    create_gold_tables,
)
from github_observatory.schema import eras

before = {f.name for f in spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE).schema.fields}
rows_before = spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE).count()
print(f"{GOLD_ECOSYSTEM_HOURLY_TABLE}: {rows_before:,} rows, {len(before)} columns")
print(f"v5 columns already present: "
      f"{sorted(set(V5_COMPARABILITY_COLUMNS) & before) or 'none'}")

# COMMAND ----------

# DBTITLE 1,Apply the migration
create_gold_tables(spark)

after = {f.name for f in spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE).schema.fields}
missing = sorted(set(V5_COMPARABILITY_COLUMNS) - after)
assert not missing, f"v5 columns still missing after migration: {missing}"
print(f"added: {sorted(after - before) or 'none (already migrated)'}")
print(f"schema now at metric_definitions v{METRIC_DEFINITIONS_VERSION}")

# COMMAND ----------

# DBTITLE 1,No row lost, every row classified
rows_after = spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE).count()
assert rows_after == rows_before, (
    f"row count changed during an additive migration: "
    f"{rows_before:,} -> {rows_after:,}"
)

unclassified = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {GOLD_ECOSYSTEM_HOURLY_TABLE}
    WHERE era IS NULL OR era_ordinal IS NULL OR in_outage IS NULL
""").collect()[0]["n"]
assert unclassified == 0, f"{unclassified:,} rows left unclassified"
print(f"{rows_after:,} rows, all classified")

# COMMAND ----------

# DBTITLE 1,Stored classification agrees with eras.py
# Recompute independently and require agreement, so a stale backfill from an
# older set of boundary dates cannot pass silently.
drift = spark.sql(f"""
    SELECT COUNT(*) AS n FROM {GOLD_ECOSYSTEM_HOURLY_TABLE}
    WHERE era          != ({eras.era_case_sql('event_hour')})
       OR era_ordinal  != ({eras.era_ordinal_sql('event_hour')})
       OR in_outage    != ({eras.in_outage_sql('event_hour')})
""").collect()[0]["n"]
assert drift == 0, (
    f"{drift:,} rows disagree with the current eras.py definitions — "
    f"re-run create_gold_tables after changing boundaries"
)
print("stored classification agrees with eras.py")

# COMMAND ----------

# DBTITLE 1,Coverage by era, and what standardizing removes
from pyspark.sql import functions as F

display(
    spark.table(GOLD_ECOSYSTEM_HOURLY_TABLE)
    .groupBy("era_ordinal", "era", "in_outage")
    .agg(
        F.count("*").alias("hours"),
        F.min("event_hour").alias("first_hour"),
        F.max("event_hour").alias("last_hour"),
    )
    .orderBy("era_ordinal", "in_outage")
)

# COMMAND ----------

# DBTITLE 1,Ready to deploy
outage_hours = spark.sql(
    f"SELECT COUNT(*) AS n FROM {GOLD_ECOSYSTEM_HOURLY_TABLE} WHERE in_outage"
).collect()[0]["n"]
pct = 100.0 * outage_hours / rows_after if rows_after else 0.0
print(f"{outage_hours:,} of {rows_after:,} hours ({pct:.2f}%) fall inside a "
      f"collection outage and drop out when standardizing.\n")
print("Deploy the dashboards now:")
print("  python scripts/databricks_run.py deploy-dashboard "
      "dashboards/github_observatory.lvdash.json")
