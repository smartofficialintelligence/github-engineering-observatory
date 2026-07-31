"""Integration tests: BigQuery deep-history import on local Delta —
bronze landing, gold merge precedence, idempotence."""

from __future__ import annotations

import json

import pytest

from conftest import gold_metrics
import github_observatory.ingestion.bq_history as bh


@pytest.fixture(scope="session")
def history_imported(ingested, tmp_path_factory):
    """Land two artifact years: one purely historical hour and one hour
    that collides with an existing stream row (the synthetic hour)."""
    spark, _, _ = ingested
    gold_metrics.create_gold_tables(spark)
    gold_metrics.build_gold(spark)  # ensure stream rows exist (source='stream')

    art_dir = tmp_path_factory.mktemp("bq_artifacts")
    header = "event_hour," + ",".join(bh.CENSUS_COLUMNS)

    def csv_row(hour, base):
        values = [str(base + i) for i in range(len(bh.CENSUS_COLUMNS))]
        return f"{hour}," + ",".join(values)

    (art_dir / "ecosystem_hourly_2019.csv").write_text(
        header + "\n" + csv_row("2019-07-15T15:00:00Z", 1000) + "\n"
    )
    # collides with the synthetic stream hour 2026-01-01T00 (stream must win)
    (art_dir / "ecosystem_hourly_2026.csv").write_text(
        header + "\n" + csv_row("2026-01-01T00:00:00Z", 999999) + "\n"
    )
    manifest = [
        {"year": 2019, "job_id": "job19", "query_sha256": "aa", "rows": 1,
         "bytes_billed": 1, "artifact": "ecosystem_hourly_2019.csv",
         "exported_at": "2026-07-30T22:00:00+00:00",
         "source_table": "githubarchive.year.2019"},
        {"year": 2026, "job_id": "job26", "query_sha256": "bb", "rows": 1,
         "bytes_billed": 1, "artifact": "ecosystem_hourly_2026.csv",
         "exported_at": "2026-07-30T22:00:00+00:00",
         "source_table": "githubarchive.year.2026"},
    ]
    (art_dir / "manifest.json").write_text(json.dumps(manifest))

    bh.create_bq_history_tables(spark)
    n = bh.ingest_bronze(spark, bh.read_artifacts(str(art_dir)))
    metrics = bh.merge_history_into_gold(spark)
    return spark, n, metrics


def one(spark, sql):
    rows = spark.sql(sql).collect()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {sql}"
    return rows[0].asDict()


def test_bronze_landing_with_provenance(history_imported):
    spark, n, _ = history_imported
    assert n == 2
    row = one(
        spark,
        "SELECT * FROM spark_catalog.bronze.bq_ecosystem_hourly "
        "WHERE event_hour = TIMESTAMP'2019-07-15 15:00:00'",
    )
    assert row["total_events"] == 1000
    assert row["bq_job_id"] == "job19"
    assert row["query_sha256"] == "aa"
    assert row["source_year"] == 2019


def test_historical_hour_inserted_with_null_payload_columns(history_imported):
    spark, _, _ = history_imported
    row = one(
        spark,
        "SELECT * FROM spark_catalog.gold.ecosystem_hourly "
        "WHERE event_hour = TIMESTAMP'2019-07-15 15:00:00'",
    )
    assert row["source"] == "bigquery"
    assert row["total_events"] == 1000
    for column in bh.PAYLOAD_COLUMNS:
        assert row[column] is None, column
    for i, column in enumerate(bh.CENSUS_COLUMNS):
        assert row[column] == 1000 + i


def test_stream_row_never_overwritten(history_imported):
    spark, _, _ = history_imported
    row = one(
        spark,
        "SELECT source, total_events, pr_merged FROM spark_catalog.gold.ecosystem_hourly "
        "WHERE event_hour = TIMESTAMP'2026-01-01 00:00:00'",
    )
    assert row["source"] == "stream"
    assert row["total_events"] == 8  # the hand-computed synthetic value, not 999999
    assert row["pr_merged"] == 1


def test_reimport_idempotent(history_imported):
    spark, _, _ = history_imported
    before = one(
        spark,
        "SELECT COUNT(*) n, SUM(total_events) s FROM spark_catalog.gold.ecosystem_hourly",
    )
    bh.merge_history_into_gold(spark)
    after = one(
        spark,
        "SELECT COUNT(*) n, SUM(total_events) s FROM spark_catalog.gold.ecosystem_hourly",
    )
    assert after == before


def test_velocity_view_serves_history_rows(history_imported):
    spark, _, _ = history_imported
    row = one(
        spark,
        "SELECT total_events, total_events_delta_24h FROM spark_catalog.gold.ecosystem_velocity "
        "WHERE event_hour = TIMESTAMP'2019-07-15 15:00:00'",
    )
    assert row["total_events"] == 1000
    assert row["total_events_delta_24h"] is None  # isolated hour -> gap-safe NULL
