"""Deep-history import from the BigQuery public GH Archive dataset.

Pass 1 (census columns): ``scripts/bq_export_hourly.py`` produces
per-year CSV artifacts + a manifest; this module lands them as
``bronze.bq_ecosystem_hourly`` (raw-as-received for this source, with
full provenance) and merges them into ``gold.ecosystem_hourly`` with
``source = 'bigquery'``.

Precedence rule (metric definitions v3): stream rows always win. The
Gold merge only inserts missing hours or refreshes rows it itself
created — a row with ``source = 'stream'`` is never touched. Historical
rows carry NULL for payload-derived columns (pr_*, issues_*, releases_*,
production_events): pass 1 deliberately avoids the payload column, and
NULL is honest about that.

The public dataset contains duplicate events; the export queries count
``DISTINCT id`` throughout (verified against our own Gold over 72 hours:
72/72 exact match after dedup).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import uuid
from typing import Any, Iterable

from github_observatory.common.config import (
    BQ_ECOSYSTEM_HOURLY_TABLE,
    GOLD_ECOSYSTEM_HOURLY_TABLE,
)
from github_observatory.gold.metrics import METRIC_DEFINITIONS_VERSION

logger = logging.getLogger("github_observatory.bq_history")

SOURCE_STREAM = "stream"
SOURCE_BIGQUERY = "bigquery"

CENSUS_COLUMNS = (
    "total_events", "total_events_human", "push_events", "push_events_human",
    "bot_events", "distinct_actors", "distinct_actors_human",
    "distinct_repos", "distinct_push_repos",
)

# Payload-derived gold columns unavailable from pass 1 (NULL for history).
PAYLOAD_COLUMNS = (
    "production_events", "pr_opened", "pr_merged", "pr_closed_no_merge",
    "pr_reopened", "issues_opened", "issues_closed", "issues_reopened",
    "releases_published",
)

BQ_ECOSYSTEM_HOURLY_DDL = (
    "event_hour TIMESTAMP, "
    + ", ".join(f"{c} BIGINT" for c in CENSUS_COLUMNS) + ", "
    "source_year INT, source_table STRING, bq_job_id STRING, "
    "query_sha256 STRING, artifact_file STRING, exported_at TIMESTAMP, "
    "import_run_id STRING, imported_at TIMESTAMP"
)

_STAGE_DDL = (
    "event_hour STRING, "
    + ", ".join(f"{c} BIGINT" for c in CENSUS_COLUMNS) + ", "
    "source_year INT, source_table STRING, bq_job_id STRING, "
    "query_sha256 STRING, artifact_file STRING, exported_at STRING"
)


def create_bq_history_tables(spark: Any) -> None:
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {BQ_ECOSYSTEM_HOURLY_TABLE} "
        f"({BQ_ECOSYSTEM_HOURLY_DDL}) USING DELTA"
    )


def ensure_gold_source_column(spark: Any) -> None:
    """Migrate gold.ecosystem_hourly to the v3 schema (delegates to the
    canonical guard in create_gold_tables)."""
    from github_observatory.gold.metrics import create_gold_tables

    create_gold_tables(spark)


def read_artifacts(artifact_dir: str) -> list[dict[str, Any]]:
    """Read manifest + per-year CSVs into stage rows (stdlib only)."""
    manifest_path = os.path.join(artifact_dir, "manifest.json")
    with open(manifest_path, encoding="utf-8") as fh:
        manifest = json.load(fh)
    rows: list[dict[str, Any]] = []
    for entry in manifest:
        path = os.path.join(artifact_dir, entry["artifact"])
        with open(path, encoding="utf-8", newline="") as fh:
            for record in csv.DictReader(fh):
                rows.append(
                    {
                        "event_hour": record["event_hour"],
                        **{c: int(record[c]) for c in CENSUS_COLUMNS},
                        "source_year": entry["year"],
                        "source_table": entry["source_table"],
                        "bq_job_id": entry["job_id"],
                        "query_sha256": entry["query_sha256"],
                        "artifact_file": entry["artifact"],
                        "exported_at": entry["exported_at"],
                    }
                )
    rows = _dedupe_boundary_hours(rows)
    logger.info("read %d hourly rows from %d artifacts", len(rows), len(manifest))
    return rows


def _dedupe_boundary_hours(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Year tables contain stray events whose created_at falls outside
    the nominal year, producing partial aggregate rows for boundary
    hours in two artifacts. Keep the row from the year that owns the
    hour; a stray-only row survives only when no owning-year row exists.
    """
    by_hour: dict[str, dict[str, Any]] = {}
    for row in rows:
        hour = row["event_hour"]
        owns = row["source_year"] == int(hour[:4])
        current = by_hour.get(hour)
        if current is None or (owns and current["source_year"] != int(hour[:4])):
            by_hour[hour] = row
    return sorted(by_hour.values(), key=lambda r: r["event_hour"])


_STAGE_FIELDS = tuple(
    ["event_hour", *CENSUS_COLUMNS, "source_year", "source_table", "bq_job_id",
     "query_sha256", "artifact_file", "exported_at"]
)


def ingest_bronze(spark: Any, rows: Iterable[dict[str, Any]], *, run_id: str | None = None) -> int:
    """MERGE artifact rows into bronze.bq_ecosystem_hourly (re-imports
    refresh in place)."""
    run_id = run_id or uuid.uuid4().hex
    data = [tuple(row[f] for f in _STAGE_FIELDS) for row in rows]
    if not data:
        return 0
    df = spark.createDataFrame(data, schema=_STAGE_DDL)
    view = f"_bq_stage_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            MERGE INTO {BQ_ECOSYSTEM_HOURLY_TABLE} AS t
            USING (
                SELECT CAST(event_hour AS TIMESTAMP) AS event_hour,
                       {", ".join(CENSUS_COLUMNS)},
                       source_year, source_table, bq_job_id, query_sha256,
                       artifact_file, CAST(exported_at AS TIMESTAMP) AS exported_at,
                       '{run_id}' AS import_run_id,
                       current_timestamp() AS imported_at
                FROM {view}
            ) AS s
            ON t.event_hour = s.event_hour
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT *
        """)
    finally:
        spark.catalog.dropTempView(view)
    return len(data)


def gold_merge_sql(run_id: str) -> str:
    null_cols = ",\n           ".join(
        f"CAST(NULL AS BIGINT) AS {c}" for c in PAYLOAD_COLUMNS
    )
    return f"""
MERGE INTO {GOLD_ECOSYSTEM_HOURLY_TABLE} AS t
USING (
    SELECT event_hour,
           {", ".join(CENSUS_COLUMNS)},
           {null_cols},
           {METRIC_DEFINITIONS_VERSION} AS metric_version,
           '{run_id}' AS gold_run_id,
           current_timestamp() AS gold_built_at,
           '{SOURCE_BIGQUERY}' AS source
    FROM {BQ_ECOSYSTEM_HOURLY_TABLE}
) AS s
ON t.event_hour = s.event_hour
WHEN MATCHED AND t.source = '{SOURCE_BIGQUERY}' THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *"""


def merge_history_into_gold(spark: Any) -> dict[str, int]:
    """Insert historical hours into Gold; never touch stream rows."""
    run_id = uuid.uuid4().hex
    ensure_gold_source_column(spark)
    row = spark.sql(gold_merge_sql(run_id)).collect()[0].asDict()
    metrics = {k: int(v) for k, v in row.items() if isinstance(v, (int, float))}
    logger.info("gold history merge: %s", metrics)
    return metrics
