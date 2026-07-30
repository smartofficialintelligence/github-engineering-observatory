# Databricks notebook source
# DBTITLE 1,Show catalogs
# MAGIC %sql
# MAGIC SHOW CATALOGS;)

# COMMAND ----------

# DBTITLE 1,Use catalog and show schemas
# MAGIC %sql
# MAGIC USE CATALOG github_observatory;
# MAGIC
# MAGIC SHOW SCHEMAS;

# COMMAND ----------

# DBTITLE 1,Create volume
# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS github_observatory.bronze.raw_files;

# COMMAND ----------

# DBTITLE 1,Read JSON and show schema
df = spark.read.json(f"file:{local_path}")

print(f"Rows: {df.count():,}")

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Show volumes
# MAGIC %sql
# MAGIC SHOW VOLUMES IN github_observatory.bronze;

# COMMAND ----------

# DBTITLE 1,List root filesystem
display(dbutils.fs.ls("/"))

# COMMAND ----------

# DBTITLE 1,List volume contents
display(
    dbutils.fs.ls(
        "dbfs:/Volumes/github_observatory/bronze/raw_files/"
    )
)

# COMMAND ----------

