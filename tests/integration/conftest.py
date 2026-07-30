"""Integration-test fixtures: real Spark + Delta, real MERGE SQL.

The suite retargets the three-part table names at local OSS Spark by
setting ``GITHUB_OBSERVATORY_CATALOG=spark_catalog`` *before* the
package modules are (re)loaded — ``spark_catalog`` is the built-in
session catalog, so ``spark_catalog.bronze.events_raw`` resolves
locally while production keeps ``github_observatory.…``.

Run separately from the fast unit tier:

    python -m pytest tests/integration -q

Requires the ``dev`` extras (pyspark + delta-spark) and a JVM; the
whole suite skips cleanly when pyspark is not installed.
"""

from __future__ import annotations

import importlib
import os

import pytest

pyspark = pytest.importorskip("pyspark", reason="integration tier needs the dev extras")

os.environ["GITHUB_OBSERVATORY_CATALOG"] = "spark_catalog"

# Re-derive every module-level table name from the overridden catalog
# (unit tests in the same pytest process may have imported them first).
import github_observatory.common.config as config  # noqa: E402

importlib.reload(config)
import github_observatory.ingestion.bronze_ingest as bronze_ingest  # noqa: E402
import github_observatory.silver.transforms as transforms  # noqa: E402

importlib.reload(bronze_ingest)
importlib.reload(transforms)

assert config.EVENTS_RAW_TABLE == "spark_catalog.bronze.events_raw"


@pytest.fixture(scope="session")
def spark(tmp_path_factory):
    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    warehouse = tmp_path_factory.mktemp("warehouse")
    derby = tmp_path_factory.mktemp("derby")
    builder = (
        SparkSession.builder.master("local[2]")
        .appName("observatory-integration")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.driver.extraJavaOptions", f"-Dderby.system.home={derby}")
        # Match Databricks serverless semantics.
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    for schema in ("bronze", "silver", "gold"):
        session.sql(f"CREATE SCHEMA IF NOT EXISTS spark_catalog.{schema}")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def bronze_tables(spark):
    bronze_ingest.create_bronze_tables(spark)
    return spark


@pytest.fixture(scope="session")
def silver_tables(spark):
    transforms.create_silver_tables(spark)
    return spark
