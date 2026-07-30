# Databricks notebook source
# DBTITLE 1,Create catalog and schemas
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS github_observatory
# MAGIC COMMENT 'GitHub engineering activity forecasting project';
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS github_observatory.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS github_observatory.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS github_observatory.gold;

# COMMAND ----------

