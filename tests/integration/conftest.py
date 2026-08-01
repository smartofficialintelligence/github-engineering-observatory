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

import gzip
import importlib
import json
import os

import pytest

pyspark = pytest.importorskip("pyspark", reason="integration tier needs the dev extras")

os.environ["GITHUB_OBSERVATORY_CATALOG"] = "spark_catalog"

# Re-derive every module-level table name from the overridden catalog
# (unit tests in the same pytest process may have imported them first).
import github_observatory.common.config as config  # noqa: E402

importlib.reload(config)
import github_observatory.forecasting.seasonal_naive as seasonal_naive  # noqa: E402
import github_observatory.gold.behavior as gold_behavior  # noqa: E402
import github_observatory.gold.metrics as gold_metrics  # noqa: E402
import github_observatory.gold.sustainability as gold_sustainability  # noqa: E402
import github_observatory.ingestion.bq_history as bq_history  # noqa: E402
import github_observatory.ingestion.bq_lifecycle_history as bq_lifecycle_history  # noqa: E402
import github_observatory.ingestion.bronze_ingest as bronze_ingest  # noqa: E402
import github_observatory.silver.transforms as transforms  # noqa: E402

importlib.reload(bronze_ingest)
importlib.reload(transforms)
importlib.reload(gold_metrics)
importlib.reload(gold_behavior)
importlib.reload(gold_sustainability)
importlib.reload(seasonal_naive)
importlib.reload(bq_history)
importlib.reload(bq_lifecycle_history)

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


# -- shared synthetic data -----------------------------------------------------
#
# The synthetic hour (2026-01-01 00:00) is deliberately distinct from the
# real sample hour so per-hour and per-file assertions never overlap.

SYNTHETIC_HOUR_FILE = "2026-01-01-0.json.gz"
SYNTHETIC_HOUR = "2026-01-01T00"


def make_event(event_id, event_type="PushEvent", **overrides):
    event = {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 2, "name": "alice/repo"},
        "payload": {"push_id": 99, "ref": "refs/heads/main"},
        "public": True,
        "created_at": "2026-01-01T00:00:01Z",
    }
    event.update(overrides)
    return event


SYNTHETIC_EVENTS = [
    make_event("1"),  # plain push
    make_event(
        "2",
        actor={"id": 9, "login": "github-actions[bot]"},
        org={"id": 77, "login": "acme"},
    ),  # bot actor + org
    make_event(
        "3",
        event_type="PullRequestEvent",
        payload={
            "action": "merged",
            "number": 7,
            "pull_request": {
                "id": 555,
                "number": 7,
                "base": {"ref": "main", "sha": "c" * 40},
                "head": {"ref": "feat", "sha": "d" * 40},
            },
        },
    ),
    make_event(
        "4",
        event_type="IssueCommentEvent",
        payload={
            "action": "created",
            "issue": {
                "id": 900,
                "number": 7,  # same number space as the PR — flag must separate
                "state": "open",
                "comments": 4,
                "pull_request": {"merged_at": "2026-07-02T10:00:00Z"},
            },
        },
    ),
    make_event(
        "5",
        event_type="PullRequestReviewEvent",
        payload={
            "action": "created",
            "pull_request": {"id": 555, "number": 7},
            "review": {
                "id": 42,
                "state": "approved",
                "submitted_at": "2026-01-01T00:00:05Z",
                "commit_id": "e" * 40,
            },
        },
    ),
    make_event(
        "6",
        event_type="ReleaseEvent",
        payload={
            "action": "published",
            "release": {
                "id": 10,
                "tag_name": "v1.0",
                "prerelease": False,
                "draft": False,
                "immutable": True,
                "published_at": "2026-01-01T00:00:00Z",
                "assets": [
                    {"download_count": 5},
                    {"download_count": 7},
                    {"other": True},
                ],
            },
        },
    ),
    make_event("7", public=False, repo={}, event_type="ForkEvent"),  # Finding S6
    make_event("8", event_type="BrandNewEvent"),  # unknown type must ingest
    make_event(42),  # integer id, normalized to STRING
]


@pytest.fixture(scope="session")
def ingested(bronze_tables, silver_tables, tmp_path_factory):
    """Write the synthetic hour file, ingest to Bronze, build Silver."""
    spark = bronze_tables
    raw_dir = tmp_path_factory.mktemp("raw")
    path = str(raw_dir / SYNTHETIC_HOUR_FILE)
    lines = [json.dumps(e).encode() for e in SYNTHETIC_EVENTS]
    lines.insert(3, b"this is not json {")  # malformed -> quarantine
    lines.append(json.dumps(make_event("1")).encode())  # in-file duplicate
    with open(path, "wb") as fh:
        fh.write(gzip.compress(b"\n".join(lines) + b"\n"))

    stats = bronze_ingest.ingest_file(spark, path)
    metrics = transforms.build_silver(spark)
    return spark, stats, metrics


REAL_HOUR_FILE = "2026-07-29-15.json.gz"
REAL_HOUR_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "raw_files", REAL_HOUR_FILE
)


@pytest.fixture(scope="session")
def real_hour(bronze_tables, silver_tables):
    """Ingest the cached real GH Archive hour through Bronze + Silver.

    Session-scoped so silver- and gold-tier tests share one ingestion
    regardless of execution order.
    """
    if not os.path.exists(REAL_HOUR_PATH):
        pytest.skip("sample hour not downloaded")
    spark = bronze_tables
    stats = bronze_ingest.ingest_file(spark, os.path.abspath(REAL_HOUR_PATH))
    transforms.build_silver(spark)
    return spark, stats
