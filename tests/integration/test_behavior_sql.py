"""Integration tests: behavior, sustainability, and forecast persistence
on local Delta — multi-day synthetic data with hand-computed expectations.

Timeline (all clean, non-bot, distinct from other fixtures' hours):

  Day 2026-02-01: A(101) pushes R1(201) @01:00 and @02:00;
                  B(102) pushes R2(202) @01:00, stars R1 @01:00, forks R1 @01:00
  Day 2026-02-02: A pushes R1 and R2 @00:00; C(103) pushes R2 @00:00
  Day 2026-02-08: B pushes R1 @00:00; C pushes R2 @00:00
"""

from __future__ import annotations

import datetime as dt
import gzip
import json
import math

import pytest

from conftest import bronze_ingest, gold_metrics, make_event, transforms
from github_observatory.forecasting import seasonal_naive as sn
from github_observatory.gold import behavior as gb
from github_observatory.gold import sustainability as gs

A, B, C = ({"id": 101, "login": "alice2"}, {"id": 102, "login": "bob"},
           {"id": 103, "login": "carol"})
R1 = {"id": 201, "name": "org/r1"}
R2 = {"id": 202, "name": "org/r2"}


def ev(eid, actor, repo, created, etype="PushEvent"):
    payload = {"push_id": int(eid), "ref": "refs/heads/main"}
    if etype == "WatchEvent":
        payload = {"action": "started"}
    if etype == "ForkEvent":
        payload = {"action": "forked", "forkee": {"id": 999}}
    return make_event(
        eid, event_type=etype, actor=actor, repo=repo,
        created_at=created, payload=payload,
    )


TIMELINE = [
    ev("9001", A, R1, "2026-02-01T01:10:00Z"),
    ev("9002", A, R1, "2026-02-01T02:10:00Z"),
    ev("9003", B, R2, "2026-02-01T01:20:00Z"),
    ev("9004", B, R1, "2026-02-01T01:30:00Z", etype="WatchEvent"),
    ev("9005", B, R1, "2026-02-01T01:40:00Z", etype="ForkEvent"),
    ev("9006", A, R1, "2026-02-02T00:10:00Z"),
    ev("9007", A, R2, "2026-02-02T00:20:00Z"),
    ev("9008", C, R2, "2026-02-02T00:30:00Z"),
    ev("9009", B, R1, "2026-02-08T00:10:00Z"),
    ev("9010", C, R2, "2026-02-08T00:20:00Z"),
]


@pytest.fixture(scope="session")
def behavior_built(ingested, tmp_path_factory):
    # depends on `ingested` so the S6-bearing synthetic file is in
    # Bronze/Silver before data_quality is built.
    spark, _, _ = ingested
    raw_dir = tmp_path_factory.mktemp("raw_behavior")
    path = str(raw_dir / "2026-02-01-1.json.gz")
    lines = [json.dumps(e).encode() for e in TIMELINE]
    with open(path, "wb") as fh:
        fh.write(gzip.compress(b"\n".join(lines) + b"\n"))
    bronze_ingest.ingest_file(spark, path)
    transforms.build_silver(spark)
    gold_metrics.create_gold_tables(spark)
    gold_metrics.build_gold(spark)
    gb.create_behavior_tables(spark)
    gb.build_behavior(spark)
    gs.create_sustainability_tables(spark)
    gs.build_sustainability(spark)
    return spark


def one(spark, sql):
    rows = spark.sql(sql).collect()
    assert len(rows) == 1, f"expected 1 row, got {len(rows)}: {sql}"
    return rows[0].asDict()


def test_flow_daily_hand_computed(behavior_built):
    row = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.flow_daily WHERE event_date = DATE'2026-02-01'",
    )
    # production per hour: 01:00 -> 2 pushes, 02:00 -> 1 push
    assert row["hours_observed"] == 2
    assert row["production_total"] == 3
    assert row["production_mean"] == pytest.approx(1.5)
    assert row["production_std"] == pytest.approx(0.5)
    assert row["production_cv"] == pytest.approx(1 / 3)
    assert row["production_fano"] == pytest.approx(0.25 / 1.5)
    assert row["production_burstiness"] == pytest.approx(-0.5)
    expected_entropy = -(2 / 3 * math.log(2 / 3) + 1 / 3 * math.log(1 / 3))
    assert row["production_hourly_entropy"] == pytest.approx(expected_entropy)


def test_actor_first_seen(behavior_built):
    rows = {
        r["actor_id"]: r["first_seen_date"]
        for r in behavior_built.sql(
            "SELECT actor_id, first_seen_date FROM spark_catalog.gold.actor_first_seen "
            "WHERE actor_id IN (101, 102, 103)"
        ).collect()
    }
    assert rows[101] == dt.date(2026, 2, 1)
    assert rows[102] == dt.date(2026, 2, 1)
    assert rows[103] == dt.date(2026, 2, 2)


def test_contribution_daily_hand_computed(behavior_built):
    row = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.contribution_daily "
        "WHERE event_date = DATE'2026-02-01'",
    )
    assert row["events"] == 5
    assert row["actors"] == 2
    assert row["actors_human"] == 2
    assert row["bot_event_share"] == pytest.approx(0.0)
    assert row["top1_actor_share"] == pytest.approx(3 / 5)  # B has 3 of 5
    assert row["top10_actor_share"] == pytest.approx(1.0)
    assert row["new_actors"] == 2  # A and B first seen this day
    day2 = one(
        behavior_built,
        "SELECT new_actors FROM spark_catalog.gold.contribution_daily "
        "WHERE event_date = DATE'2026-02-02'",
    )
    assert day2["new_actors"] == 1  # C


def test_engagement_daily_hand_computed(behavior_built):
    row = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.engagement_daily "
        "WHERE event_date = DATE'2026-02-01'",
    )
    assert row["stars"] == 1
    assert row["forks"] == 1
    assert row["distinct_starring_actors"] == 1
    assert row["distinct_starred_repos"] == 1
    assert row["distinct_forked_repos"] == 1


def test_data_quality_parity(behavior_built):
    row = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.data_quality "
        "WHERE source_file = '2026-02-01-1.json.gz'",
    )
    assert row["bronze_rows"] == 10
    assert row["silver_rows"] == 10
    assert row["parity_ok"] is True
    assert row["quarantined"] == 0
    assert row["flagged"] == 0
    assert row["duplicate_event_ids"] == 0
    # the S6-bearing synthetic file must show its flag, still parity-ok
    s6 = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.data_quality "
        "WHERE source_file = '2026-01-01-0.json.gz'",
    )
    assert s6["parity_ok"] is True
    assert s6["flagged"] == 1
    assert s6["quarantined"] == 1


def test_retention_hand_computed(behavior_built):
    day2 = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.actor_retention_daily "
        "WHERE event_date = DATE'2026-02-02'",
    )
    assert day2["active_actors"] == 2  # A, C
    assert day2["retained_from_1d"] == 1  # A
    assert day2["retention_1d"] == pytest.approx(0.5)  # of {A, B}
    assert day2["retention_7d"] is None  # no 2026-01-26 baseline

    day8 = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.actor_retention_daily "
        "WHERE event_date = DATE'2026-02-08'",
    )
    assert day8["retention_1d"] is None  # 2026-02-07 has no data — gap, not 0
    assert day8["retained_from_7d"] == 1  # B from {A, B}
    assert day8["retention_7d"] == pytest.approx(0.5)


def test_network_daily_hand_computed(behavior_built):
    row = one(
        behavior_built,
        "SELECT * FROM spark_catalog.gold.network_daily "
        "WHERE event_date = DATE'2026-02-01'",
    )
    # distinct edges: A-R1, B-R2, B-R1
    assert row["actors"] == 2
    assert row["repos"] == 2
    assert row["edges"] == 3
    assert row["multi_repo_actors"] == 1  # B
    assert row["multi_repo_actor_share"] == pytest.approx(0.5)
    assert row["actors_per_repo_avg"] == pytest.approx(1.5)  # R1:2, R2:1
    assert row["single_actor_repo_share"] == pytest.approx(0.5)


def test_behavior_rebuild_idempotent(behavior_built):
    before = one(
        behavior_built,
        "SELECT COUNT(*) AS n, SUM(events) AS s FROM spark_catalog.gold.contribution_daily",
    )
    gb.build_behavior(behavior_built)
    gs.build_sustainability(behavior_built)
    after = one(
        behavior_built,
        "SELECT COUNT(*) AS n, SUM(events) AS s FROM spark_catalog.gold.contribution_daily",
    )
    assert after == before


# -- forecast persistence ------------------------------------------------------


def test_forecast_evaluation_persists_and_upserts(behavior_built):
    spark = behavior_built
    sn.create_forecast_tables(spark)
    t0 = dt.datetime(2026, 2, 1, 0, tzinfo=dt.timezone.utc)
    series = [(t0 + dt.timedelta(hours=i), float(100 + (i % 24))) for i in range(48)]
    evals = sn.evaluate_methods(series, "total_events")
    sn.write_evaluation(spark, evals)

    summary = {
        r["method"]: r
        for r in spark.sql(
            "SELECT * FROM spark_catalog.gold.forecast_eval WHERE target = 'total_events'"
        ).collect()
    }
    assert summary["seasonal_24h"]["mae"] == pytest.approx(0.0)
    assert summary["naive_1h"]["mase"] == pytest.approx(1.0)
    assert "seasonal_168h" not in summary  # no predictions -> no row

    n_before = spark.sql(
        "SELECT COUNT(*) AS n FROM spark_catalog.gold.forecast_predictions"
    ).collect()[0]["n"]
    sn.write_evaluation(spark, evals)  # re-run: upsert, no duplication
    n_after = spark.sql(
        "SELECT COUNT(*) AS n FROM spark_catalog.gold.forecast_predictions"
    ).collect()[0]["n"]
    assert n_after == n_before
