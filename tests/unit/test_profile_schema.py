"""Unit tests for the stream schema profiler, on synthetic events."""

from __future__ import annotations

import csv
import datetime as dt
import gzip
import json

from github_observatory.schema import profile_schema as ps


def push_event(event_id="1001", size=3, n_commits=3, created_at="2026-07-29T15:04:05Z"):
    return {
        "id": event_id,
        "type": "PushEvent",
        "actor": {"id": 11, "login": "alice"},
        "repo": {"id": 501, "name": "alice/widgets"},
        "public": True,
        "created_at": created_at,
        "payload": {
            "push_id": int(event_id) * 7,
            "ref": "refs/heads/main",
            "before": "aaa111",
            "head": "bbb222",
            "size": size,
            "distinct_size": size,
            "commits": [
                {"sha": f"c{i}", "message": f"m{i}", "author": {"name": "Alice", "email": "a@x"}}
                for i in range(n_commits)
            ],
        },
    }


def pr_event(event_id, repo_id, number, pr_id, merged=None, action="opened"):
    pr = {"id": pr_id, "number": number, "state": "open"}
    if merged is not None:
        pr["merged"] = merged
    return {
        "id": event_id,
        "type": "PullRequestEvent",
        "actor": {"id": 12, "login": "bob"},
        "repo": {"id": repo_id, "name": "org/repo"},
        "org": {"id": 900, "login": "org"},
        "public": True,
        "created_at": "2026-07-29T15:30:00Z",
        "payload": {"action": action, "number": number, "pull_request": pr},
    }


# --------------------------------------------------------------------------
# flatten_event


def test_flatten_nested_paths_and_types():
    obs = ps.flatten_event(push_event())
    assert obs["id"].types == {"STRING"}
    assert obs["actor.id"].types == {"BIGINT"}
    assert obs["public"].types == {"BOOLEAN"}  # bool not misread as BIGINT
    assert obs["payload"].types == {"STRUCT"}
    assert obs["payload.commits"].types == {"ARRAY<STRUCT>"}
    assert obs["payload.commits[].sha"].types == {"STRING"}
    assert obs["payload.commits[].author.name"].types == {"STRING"}


def test_flatten_null_and_scalar_arrays():
    obs = ps.flatten_event({"a": None, "b": [1, 2, 3], "c": [], "d": [1, "x"]})
    assert obs["a"].types == {"NULL"} and obs["a"].saw_null
    assert obs["b"].types == {"ARRAY<BIGINT>"}
    assert obs["b"].sample == [1, 2, 3]
    assert obs["c"].types == {"ARRAY<EMPTY>"}
    assert obs["d"].types == {"ARRAY<BIGINT|STRING>"}


def test_flatten_counts_each_array_subfield_once_per_event():
    obs = ps.flatten_event(push_event(n_commits=5))
    # one observation object per path regardless of 5 elements
    assert obs["payload.commits[].sha"].sample == "c0"


# --------------------------------------------------------------------------
# StreamProfiler aggregation


def make_profiler_with_events(events):
    profiler = ps.StreamProfiler()
    for event in events:
        profiler.add_event(event)
    return profiler


def test_presence_rates_and_event_counts():
    events = [
        pr_event("1", 500, 7, 9000),
        pr_event("2", 500, 8, 9001, merged=True, action="closed"),
    ]
    profiler = make_profiler_with_events(events)
    rows = {
        (r["event_type"], r["field_path"]): r for r in profiler.profile_rows()
    }
    merged_row = rows[("PullRequestEvent", "payload.pull_request.merged")]
    assert merged_row["presence_count"] == 1
    assert merged_row["event_count"] == 2
    assert merged_row["presence_rate"] == 0.5
    assert merged_row["spark_type"] == "BOOLEAN"
    assert "contract field" in merged_row["notes"]


def test_inconsistent_types_flagged():
    profiler = make_profiler_with_events(
        [
            {"id": "1", "type": "XEvent", "payload": {"v": 1}},
            {"id": "2", "type": "XEvent", "payload": {"v": "one"}},
        ]
    )
    row = next(
        r for r in profiler.profile_rows()
        if r["event_type"] == "XEvent" and r["field_path"] == "payload.v"
    )
    assert row["spark_type"] == "BIGINT|STRING"
    assert "inconsistent types" in row["notes"]


def test_nullable_marks_explicit_null_only():
    profiler = make_profiler_with_events(
        [
            {"id": "1", "type": "XEvent", "payload": {"v": None}},
            {"id": "2", "type": "XEvent", "payload": {}},  # absent ≠ null
        ]
    )
    row = next(r for r in profiler.profile_rows() if r["field_path"] == "payload.v")
    assert row["nullable"] is True
    assert row["presence_count"] == 1  # explicit null counts as present


def test_duplicate_and_missing_event_ids_detected():
    profiler = make_profiler_with_events(
        [
            pr_event("1", 500, 7, 9000),
            pr_event("1", 500, 7, 9000),
            {"type": "XEvent", "payload": {}},
        ]
    )
    keys = profiler.summary()["keys"]["event_id"]
    assert keys["duplicates"] == 1
    assert keys["missing"] == 1
    assert keys["distinct"] == 1


def test_pr_key_collision_detected():
    profiler = make_profiler_with_events(
        [
            pr_event("1", 500, 7, 9000),
            pr_event("2", 500, 7, 9000),   # same key, same id: fine
            pr_event("3", 500, 7, 9999),   # same key, DIFFERENT id: collision
            pr_event("4", 501, 7, 9002),   # different repo: fine
        ]
    )
    stats = profiler.summary()["keys"]["repo_id_pr_number_to_pr_id"]
    assert stats["distinct_keys"] == 2
    assert stats["keys_with_multiple_values"] == 1


def test_push_commit_stats_capture_truncation():
    profiler = make_profiler_with_events(
        [
            push_event("1", size=3, n_commits=3),
            push_event("2", size=40, n_commits=20),  # API caps array at 20
        ]
    )
    stats = profiler.summary()["push_commit_stats"]
    assert stats["push_events"] == 2
    assert stats["with_commits_array"] == 2
    assert stats["total_commit_elements"] == 23
    assert stats["total_size_reported"] == 43
    assert stats["events_size_gt_commits_array"] == 1
    keys = profiler.summary()["keys"]["push_id"]
    assert keys["events_with_push_id"] == 2 and keys["distinct"] == 2


def test_action_crosstab():
    profiler = make_profiler_with_events(
        [
            pr_event("1", 500, 7, 9000, action="opened"),
            pr_event("2", 500, 8, 9001, action="closed"),
            pr_event("3", 500, 9, 9002, action="closed"),
        ]
    )
    actions = profiler.summary()["actions_by_event_type"]
    assert actions["PullRequestEvent:closed"] == 2
    assert actions["PullRequestEvent:opened"] == 1


def test_contract_report_marks_absent_fields():
    profiler = make_profiler_with_events([push_event()])
    contract = profiler.summary()["contract_fields"]
    assert contract["payload.push_id"]["verdict"] == "PRESENT"
    assert contract["payload.review.id"]["verdict"] == "ABSENT_IN_SAMPLE"
    assert (
        contract["payload.ref"]["present_in_event_types"]["PushEvent"]["presence_rate"] == 1.0
    )


# --------------------------------------------------------------------------
# file streaming, malformed lines, hour mismatches


def test_profile_file_handles_malformed_lines_and_hour_mismatch(tmp_path):
    lines = [
        json.dumps(push_event("1", created_at="2026-07-29T15:10:00Z")),
        "{this is not json",
        json.dumps(push_event("2", created_at="2026-07-29T16:00:01Z")),  # outside hour
    ]
    path = tmp_path / "2026-07-29-15.json.gz"
    path.write_bytes(gzip.compress(("\n".join(lines) + "\n").encode()))

    profiler = ps.StreamProfiler()
    profiler.profile_file(
        str(path), source_hour=dt.datetime(2026, 7, 29, 15, tzinfo=dt.timezone.utc)
    )
    summary = profiler.summary()

    assert summary["total_events"] == 2
    assert summary["malformed_lines"] == 1
    assert summary["malformed_samples"][0]["line"] == 2
    assert summary["event_time_vs_source_hour"]["mismatches"] == 1
    file_stats = summary["files"]["2026-07-29-15.json.gz"]
    assert file_stats["events"] == 2 and file_stats["malformed"] == 1
    assert file_stats["min_created_at"] == "2026-07-29T15:10:00Z"


def testinfer_source_hour_from_filename():
    inferred = ps.infer_source_hour("/data/2026-07-29-3.json.gz")
    assert inferred == dt.datetime(2026, 7, 29, 3, tzinfo=dt.timezone.utc)
    assert ps.infer_source_hour("notes.txt") is None


# --------------------------------------------------------------------------
# artifacts


def test_csv_writer_emits_spec_columns(tmp_path):
    profiler = make_profiler_with_events([push_event()])
    out = tmp_path / "profile.csv"
    ps.write_profile_csv(profiler.profile_rows(), str(out))
    with open(out, newline="") as fh:
        reader = csv.DictReader(fh)
        assert tuple(reader.fieldnames) == ps.PROFILE_COLUMNS
        rows = list(reader)
    assert any(r["field_path"] == "payload.push_id" for r in rows)


def test_summary_json_roundtrips(tmp_path):
    profiler = make_profiler_with_events([push_event()])
    out = tmp_path / "summary.json"
    ps.write_summary_json(profiler.summary(), str(out))
    loaded = json.loads(out.read_text())
    assert loaded["total_events"] == 1
