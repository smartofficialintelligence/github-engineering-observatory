"""Deep-history payload-derived import (pass 2) from Dataproc Serverless.

Pipeline: ``pipelines/gcp_lifecycle_backfill/spark_job.py`` on Dataproc
Serverless reads ``githubarchive.year.YYYY`` via the Storage Read API,
extracts 34 hourly aggregates per the spec, and writes partitioned
Parquet to GCS. This module lands those Parquet files as
``bronze.bq_lifecycle_hourly`` — raw-as-received for this source with
provenance, mirror-shape of ``bronze.bq_ecosystem_hourly``.

Merging into ``gold.ecosystem_hourly`` is deferred (spec §Follow-up):
the current Gold schema carries 9 of the 34 payload-derived columns.
Extending Gold + the stream Silver→Gold pipeline to produce the 15 new
columns is a separate scoped increment. Until then, the pass-2 output
lives in Bronze only; consumers who want the full lifecycle history
query ``bronze.bq_lifecycle_hourly`` directly.

Contract: ``docs/lifecycle_metrics_spec.md`` v1. Every column definition
is bound there; validated end-to-end against BigQuery SQL for the
2023-06 bench (715 hours × 33 columns exact-match).
"""

from __future__ import annotations

import logging
import uuid
from typing import Any

from github_observatory.schema import eras
from github_observatory.common.config import (
    BQ_LIFECYCLE_HOURLY_TABLE,
    GOLD_ECOSYSTEM_HOURLY_TABLE,
)

logger = logging.getLogger("github_observatory.bq_lifecycle_history")

METRIC_SPEC_VERSION = 2  # v2: adds pr_merge_signal_stripped column

# 34 aggregate columns produced by pipelines/gcp_lifecycle_backfill/spark_job.py.
# Order matters only for the DDL — MERGE stages by name.
CENSUS_COLUMNS = (
    "total_events", "total_events_human", "push_events", "push_events_human",
    "bot_events", "distinct_actors", "distinct_actors_human",
    "distinct_repos", "distinct_push_repos",
)
LIFECYCLE_COLUMNS = (
    "production_events", "pr_opened", "pr_merged", "pr_closed_no_merge",
    "pr_reopened", "issues_opened", "issues_closed", "issues_reopened",
    "releases_published",
)
DENOMINATOR_COLUMNS = ("pr_events_total", "issues_events_total")
CLOSURE_TAXONOMY_COLUMNS = (
    "issues_closed_completed", "issues_closed_not_planned",
    "issues_closed_duplicate", "issues_closed_unknown",
)
REVIEW_STATE_COLUMNS = (
    "reviews_approved", "reviews_changes_requested",
    "reviews_commented", "reviews_dismissed",
)
COMMENT_COLUMNS = (
    "issue_comments_true", "issue_comments_on_prs",
    "pr_review_comments", "commit_comments",
)
RELEASE_ADOPTION_COLUMNS = ("release_download_count_sum",)

ALL_METRIC_COLUMNS = (
    CENSUS_COLUMNS + LIFECYCLE_COLUMNS + DENOMINATOR_COLUMNS
    + CLOSURE_TAXONOMY_COLUMNS + REVIEW_STATE_COLUMNS
    + COMMENT_COLUMNS + RELEASE_ADOPTION_COLUMNS
)
assert len(ALL_METRIC_COLUMNS) == 33, f"expected 33 metric columns, got {len(ALL_METRIC_COLUMNS)}"

# Provenance columns: three from the extract, two added at ingest.
PROVENANCE_FROM_EXTRACT = ("source_year", "metric_spec_version", "job_id")

BQ_LIFECYCLE_HOURLY_DDL = (
    "event_hour TIMESTAMP, "
    + ", ".join(f"{c} BIGINT" for c in ALL_METRIC_COLUMNS) + ", "
    # v2 data-quality flag: TRUE when the batch containing this hour had
    # its PR merge signal stripped by the OQ-1 upstream filter. When TRUE,
    # pr_merged (always 0) and pr_closed_no_merge (over-counted) are
    # unreliable — filter with WHERE NOT pr_merge_signal_stripped when
    # querying those two columns.
    "pr_merge_signal_stripped BOOLEAN, "
    "source_year INT, metric_spec_version INT, "
    "extract_job_id STRING, extracted_at TIMESTAMP, "
    "import_run_id STRING, imported_at TIMESTAMP"
)


def create_bq_lifecycle_tables(spark: Any) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {BQ_LIFECYCLE_HOURLY_TABLE} "
        f"({BQ_LIFECYCLE_HOURLY_DDL}) USING DELTA"
    )


def _validate_input_schema(df: Any) -> None:
    """Sanity-check the Parquet schema before MERGE."""
    required = set(
        ALL_METRIC_COLUMNS + ("event_hour", "pr_merge_signal_stripped")
        + PROVENANCE_FROM_EXTRACT
    )
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Parquet input missing required columns: {sorted(missing)}"
        )
    if "ingested_at" not in df.columns:
        raise ValueError("Parquet input missing 'ingested_at' provenance column")


def ingest_bronze_from_parquet(
    spark: Any, parquet_path: str, *, run_id: str | None = None
) -> dict[str, int]:
    """Read Parquet from ``parquet_path`` (Volume or GCS URI supported by
    Spark's reader) and MERGE into ``bronze.bq_lifecycle_hourly``.

    Re-ingesting the same rows is safe: MERGE keyed on event_hour with
    UPDATE SET * refreshes provenance while preserving column values
    (bit-identical if the extract is deterministic).
    """
    run_id = run_id or uuid.uuid4().hex
    df = spark.read.parquet(parquet_path)
    _validate_input_schema(df)

    stage_view = f"_lifecycle_stage_{uuid.uuid4().hex}"
    # Normalize event_hour: Spark's Parquet reader gives us a TIMESTAMP;
    # cast explicitly so this same code path works for any input source.
    (
        df.selectExpr(
            "CAST(event_hour AS TIMESTAMP) AS event_hour",
            *ALL_METRIC_COLUMNS,
            "CAST(pr_merge_signal_stripped AS BOOLEAN) AS pr_merge_signal_stripped",
            "CAST(source_year AS INT) AS source_year",
            "CAST(metric_spec_version AS INT) AS metric_spec_version",
            "CAST(job_id AS STRING) AS extract_job_id",
            "CAST(ingested_at AS TIMESTAMP) AS extracted_at",
        ).createOrReplaceTempView(stage_view)
    )

    metric_cols_csv = ", ".join(ALL_METRIC_COLUMNS)
    try:
        row = spark.sql(f"""
            MERGE INTO {BQ_LIFECYCLE_HOURLY_TABLE} AS t
            USING (
                SELECT event_hour, {metric_cols_csv},
                       pr_merge_signal_stripped,
                       source_year, metric_spec_version, extract_job_id, extracted_at,
                       '{run_id}' AS import_run_id,
                       current_timestamp() AS imported_at
                FROM {stage_view}
            ) AS s
            ON t.event_hour = s.event_hour
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """).collect()[0].asDict()
    finally:
        spark.catalog.dropTempView(stage_view)

    metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
    logger.info("lifecycle bronze merge: %s", metrics)
    return metrics


SOURCE_BIGQUERY = "bigquery"


def gold_merge_sql(run_id: str) -> str:
    """MERGE bronze.bq_lifecycle_hourly into gold.ecosystem_hourly.

    Fills the 24 payload-derived columns (9 pre-v4 lifecycle + 15 v4)
    plus the 9 census columns from pass-2 output. Stream rows are never
    touched — the guard is ``WHEN MATCHED AND t.source = 'bigquery'``.

    Pass-1 (``bronze.bq_ecosystem_hourly``) may already have inserted a
    bigquery-source row for the same hour with payload cols NULL; this
    MERGE refreshes those rows with the pass-2 values. Hours pass-1
    missed (or hours never census-imported) are INSERTed fresh.
    """
    from github_observatory.gold.metrics import METRIC_DEFINITIONS_VERSION

    metric_cols_csv = ", ".join(ALL_METRIC_COLUMNS)
    return f"""
MERGE INTO {GOLD_ECOSYSTEM_HOURLY_TABLE} AS t
USING (
    SELECT event_hour, {metric_cols_csv},
           {METRIC_DEFINITIONS_VERSION} AS metric_version,
           '{run_id}' AS gold_run_id,
           current_timestamp() AS gold_built_at,
           '{SOURCE_BIGQUERY}' AS source,
           {eras.era_case_sql('event_hour')} AS era,
           {eras.era_ordinal_sql('event_hour')} AS era_ordinal,
           {eras.in_outage_sql('event_hour')} AS in_outage
    FROM {BQ_LIFECYCLE_HOURLY_TABLE}
) AS s
ON t.event_hour = s.event_hour
WHEN MATCHED AND t.source = '{SOURCE_BIGQUERY}' THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *"""


def merge_lifecycle_into_gold(spark: Any) -> dict[str, int]:
    """MERGE pass-2 lifecycle rows into Gold; never touch stream rows.

    Requires Gold at v4 (24 payload columns). Caller must first invoke
    ``gold.metrics.create_gold_tables(spark)`` to ensure the v4
    migration has run.
    """
    run_id = uuid.uuid4().hex
    row = spark.sql(gold_merge_sql(run_id)).collect()[0].asDict()
    metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
    logger.info("gold lifecycle merge: %s", metrics)
    return metrics
