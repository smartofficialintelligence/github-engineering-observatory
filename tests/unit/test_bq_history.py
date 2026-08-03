"""Unit tests for the BigQuery deep-history import — artifact parsing
and SQL structure. Semantics run in ``tests/integration``."""

from __future__ import annotations

import json
import re

from github_observatory.ingestion import bq_history as bh


def write_artifacts(tmp_path, years=(2016,)):
    manifest = []
    for year in years:
        name = f"ecosystem_hourly_{year}.csv"
        header = "event_hour," + ",".join(bh.CENSUS_COLUMNS)
        row = f"{year}-07-15T15:00:00Z," + ",".join(
            str(100 + i) for i in range(len(bh.CENSUS_COLUMNS))
        )
        (tmp_path / name).write_text(header + "\n" + row + "\n")
        manifest.append({
            "year": year, "job_id": f"job_{year}", "query_sha256": "beef" * 16,
            "rows": 1, "bytes_billed": 5, "artifact": name,
            "exported_at": "2026-07-30T22:00:00+00:00",
            "source_table": f"githubarchive.year.{year}",
        })
    (tmp_path / "manifest.json").write_text(json.dumps(manifest))
    return tmp_path


def test_read_artifacts_joins_manifest_provenance(tmp_path):
    rows = bh.read_artifacts(str(write_artifacts(tmp_path, years=(2016, 2017))))
    assert len(rows) == 2
    row = next(r for r in rows if r["source_year"] == 2016)
    assert row["event_hour"] == "2016-07-15T15:00:00Z"
    assert row["total_events"] == 100
    assert row["bq_job_id"] == "job_2016"
    assert row["artifact_file"] == "ecosystem_hourly_2016.csv"
    assert set(bh._STAGE_FIELDS) <= set(row)


def test_stage_fields_match_stage_ddl_order():
    ddl_names = [c.strip().split()[0] for c in bh._STAGE_DDL.split(",")]
    assert ddl_names == list(bh._STAGE_FIELDS)


def test_census_and_payload_columns_partition_gold_metrics():
    """Every gold metric column is exactly one of census or payload."""
    from github_observatory.gold.metrics import ECOSYSTEM_HOURLY_DDL

    gold_cols = {c.strip().split()[0] for c in ECOSYSTEM_HOURLY_DDL.split(",")}
    metric_cols = gold_cols - {
        "event_hour", "metric_version", "gold_run_id", "gold_built_at", "source",
        # v5 comparability context — derived from event_hour, not measured
        "era", "era_ordinal", "in_outage",
    }
    assert metric_cols == set(bh.CENSUS_COLUMNS) | set(bh.PAYLOAD_COLUMNS)
    assert not set(bh.CENSUS_COLUMNS) & set(bh.PAYLOAD_COLUMNS)


def test_gold_merge_never_touches_stream_rows():
    sql = bh.gold_merge_sql("r1")
    assert "WHEN MATCHED AND t.source = 'bigquery' THEN UPDATE SET *" in sql
    assert re.search(r"WHEN MATCHED THEN", sql) is None  # no unconditional update
    assert "WHEN NOT MATCHED THEN INSERT *" in sql
    assert "'bigquery' AS source" in sql


def test_gold_merge_nulls_every_payload_column():
    sql = bh.gold_merge_sql("r1")
    for column in bh.PAYLOAD_COLUMNS:
        assert f"CAST(NULL AS BIGINT) AS {column}" in sql
    for column in bh.CENSUS_COLUMNS:
        assert column in sql


def test_stream_merge_stamps_stream_source():
    from github_observatory.gold.metrics import ecosystem_hourly_merge_sql

    assert "'stream' AS source" in ecosystem_hourly_merge_sql("r1")


def test_boundary_hours_prefer_owning_year():
    """A 2018-01-01 hour aggregated from strays in the 2017 table must
    lose to the true row from the 2018 table, regardless of order."""
    def row(hour, year, total):
        return {
            "event_hour": hour, "source_year": year, "total_events": total,
            **{c: 0 for c in bh.CENSUS_COLUMNS if c != "total_events"},
            "source_table": f"githubarchive.year.{year}", "bq_job_id": "j",
            "query_sha256": "s", "artifact_file": "a", "exported_at": "t",
        }
    rows = bh._dedupe_boundary_hours([
        row("2018-01-01T00:00:00Z", 2017, 3),      # stray fragment
        row("2018-01-01T00:00:00Z", 2018, 170000),  # owning year
        row("2015-12-31T23:00:00Z", 2016, 7),       # stray with no owner: kept
    ])
    by_hour = {r["event_hour"]: r for r in rows}
    assert len(rows) == 2
    assert by_hour["2018-01-01T00:00:00Z"]["total_events"] == 170000
    assert by_hour["2015-12-31T23:00:00Z"]["total_events"] == 7
