"""Unit tests for Bronze ingestion parsing. No Spark, no network:
everything runs against synthetic gzip files, mirroring the downloader
and profiler test style."""

from __future__ import annotations

import gzip
import json

import pytest

from github_observatory.ingestion import bronze_ingest as bi


def make_event(event_id="12345678901", event_type="PushEvent", **overrides):
    event = {
        "id": event_id,
        "type": event_type,
        "actor": {"id": 1, "login": "alice"},
        "repo": {"id": 2, "name": "alice/repo", "url": "https://api.github.com/repos/alice/repo"},
        "payload": {"push_id": 99, "ref": "refs/heads/main"},
        "public": True,
        "created_at": "2026-07-29T15:00:01Z",
    }
    event.update(overrides)
    return event


def write_gz(tmp_path, lines, name="2026-07-29-15.json.gz"):
    path = tmp_path / name
    raw = b"\n".join(lines) + b"\n"
    path.write_bytes(gzip.compress(raw))
    return str(path)


def event_line(**kwargs) -> bytes:
    return json.dumps(make_event(**kwargs)).encode()


# -- parse_raw_line ------------------------------------------------------------


def test_parse_valid_event_produces_row():
    parsed = bi.parse_raw_line(event_line(), 1, "f.json.gz", "2026-07-29T15:00:00+00:00")
    assert parsed.quarantine is None
    row = parsed.row
    assert row["event_id"] == "12345678901"
    assert row["event_type"] == "PushEvent"
    assert row["public"] is True
    assert row["created_at"] == "2026-07-29T15:00:01Z"
    assert row["source_file"] == "f.json.gz"
    assert row["source_line"] == 1
    assert row["source_hour"] == "2026-07-29T15:00:00+00:00"
    # struct columns are compact JSON strings that round-trip
    assert json.loads(row["actor_json"]) == {"id": 1, "login": "alice"}
    assert json.loads(row["payload_json"])["push_id"] == 99
    assert row["org_json"] is None  # absent org stays NULL


def test_parse_row_columns_match_stage_ddl():
    """Every emitted row key must be a stage column and vice versa, so
    tuple conversion in _stage_and_merge cannot silently misalign."""
    parsed = bi.parse_raw_line(event_line(), 1, "f.json.gz", None)
    assert set(parsed.row) == set(bi._EVENTS_RAW_STAGE_COLUMNS)


def test_quarantine_columns_match_stage_ddl():
    parsed = bi.parse_raw_line(b"not json", 7, "f.json.gz", None)
    assert set(parsed.quarantine) == set(bi._QUARANTINE_STAGE_COLUMNS)


def test_malformed_json_quarantined():
    parsed = bi.parse_raw_line(b'{"id": "1", broken', 3, "f.json.gz", None)
    q = parsed.quarantine
    assert q["reason"] == bi.REASON_MALFORMED_JSON
    assert q["source_line"] == 3
    assert q["raw_line"].startswith('{"id": "1"')
    assert q["raw_length"] == len('{"id": "1", broken')


def test_non_object_json_quarantined():
    parsed = bi.parse_raw_line(b'[1, 2, 3]', 1, "f.json.gz", None)
    assert parsed.quarantine["reason"] == bi.REASON_NOT_OBJECT


@pytest.mark.parametrize("bad_id", [None, "", "   ", True, 1.5, {"x": 1}])
def test_unusable_id_quarantined(bad_id):
    line = json.dumps(make_event(event_id=bad_id)).encode()
    parsed = bi.parse_raw_line(line, 1, "f.json.gz", None)
    assert parsed.quarantine["reason"] == bi.REASON_MISSING_ID


def test_missing_id_key_quarantined():
    event = make_event()
    del event["id"]
    parsed = bi.parse_raw_line(json.dumps(event).encode(), 1, "f.json.gz", None)
    assert parsed.quarantine["reason"] == bi.REASON_MISSING_ID


def test_integer_id_normalized_to_string():
    parsed = bi.parse_raw_line(event_line(event_id=42), 1, "f.json.gz", None)
    assert parsed.row["event_id"] == "42"


def test_finding_s6_event_ingests_not_quarantined():
    """public=false with empty repo (Finding S6) must reach events_raw."""
    line = json.dumps(make_event(public=False, repo={})).encode()
    parsed = bi.parse_raw_line(line, 1, "f.json.gz", None)
    assert parsed.quarantine is None
    assert parsed.row["public"] is False
    assert parsed.row["repo_json"] == "{}"


def test_unknown_event_type_ingests():
    parsed = bi.parse_raw_line(event_line(event_type="BrandNewEvent"), 1, "f", None)
    assert parsed.quarantine is None
    assert parsed.row["event_type"] == "BrandNewEvent"


def test_missing_optional_scalars_become_null():
    event = make_event()
    del event["type"], event["public"], event["created_at"], event["payload"]
    parsed = bi.parse_raw_line(json.dumps(event).encode(), 1, "f", None)
    row = parsed.row
    assert row["event_type"] is None
    assert row["public"] is None
    assert row["created_at"] is None
    assert row["payload_json"] is None


def test_quarantine_raw_line_truncated_but_length_kept():
    huge = json.dumps({"junk": "x" * (bi.QUARANTINE_RAW_LINE_MAX_CHARS + 100)})
    parsed = bi.parse_raw_line(huge.encode(), 1, "f", None)  # no id -> quarantine
    q = parsed.quarantine
    assert len(q["raw_line"]) == bi.QUARANTINE_RAW_LINE_MAX_CHARS
    assert q["raw_length"] == len(huge)


# -- parse_file ----------------------------------------------------------------


def test_parse_file_counts_and_dedupe(tmp_path):
    lines = [
        event_line(event_id="1"),
        event_line(event_id="2"),
        event_line(event_id="1"),  # within-file duplicate
        b"garbage {",
        event_line(event_id="3"),
    ]
    path = write_gz(tmp_path, lines)
    rows, quarantine, stats = bi.parse_file(path)

    assert stats.lines_read == 5
    assert stats.events_parsed == 4
    assert stats.quarantined == 1
    assert stats.within_file_duplicate_ids == 1
    assert stats.rows_staged == 3
    assert [r["event_id"] for r in rows] == ["1", "2", "3"]
    assert quarantine[0]["source_line"] == 4
    # invariant checked by the CLI too
    assert stats.lines_read == stats.events_parsed + stats.quarantined


def test_parse_file_infers_source_hour_from_name(tmp_path):
    path = write_gz(tmp_path, [event_line()], name="2026-07-29-3.json.gz")
    rows, _, stats = bi.parse_file(path)
    assert stats.source_hour == "2026-07-29T03:00:00+00:00"
    assert rows[0]["source_hour"] == "2026-07-29T03:00:00+00:00"
    assert rows[0]["source_file"] == "2026-07-29-3.json.gz"


def test_parse_file_blank_lines_keep_physical_line_numbers(tmp_path):
    lines = [event_line(event_id="1"), b"", b"not json"]
    path = write_gz(tmp_path, lines)
    _, quarantine, stats = bi.parse_file(path)
    assert stats.lines_read == 2  # blank line skipped
    assert quarantine[0]["source_line"] == 3  # physical position preserved


def test_parse_file_plain_json_supported(tmp_path):
    path = tmp_path / "events.json"
    path.write_bytes(event_line(event_id="9") + b"\n")
    rows, _, stats = bi.parse_file(str(path))
    assert stats.rows_staged == 1
    assert rows[0]["source_hour"] is None  # name gives no hour


# -- chunking and audit mapping --------------------------------------------------


def test_chunks_cover_all_rows_without_overlap():
    rows = [{"i": i} for i in range(10)]
    chunks = list(bi._chunks(rows, 3))
    assert [len(c) for c in chunks] == [3, 3, 3, 1]
    assert [r["i"] for chunk in chunks for r in chunk] == list(range(10))


def test_download_result_audit_row_field_alignment():
    """DownloadResult dicts must map onto audit columns without loss."""
    from github_observatory.ingestion.download_gharchive import DownloadResult

    result = DownloadResult(
        run_id="r1",
        source_url="https://data.gharchive.org/2026-07-29-15.json.gz",
        source_file="2026-07-29-15.json.gz",
        source_hour="2026-07-29T15:00:00+00:00",
        download_started_at="2026-07-30T00:00:00+00:00",
        download_completed_at="2026-07-30T00:00:30+00:00",
        http_status=200,
        content_length=100,
        bytes_downloaded=100,
        uncompressed_bytes=1000,
        line_count=3,
        sha256="abc",
        gzip_valid=True,
        status="downloaded",
        dest_path="/Volumes/x/y/z/2026-07-29-15.json.gz",
    )
    row = bi.download_result_audit_row(result)
    assert row["phase"] == bi.PHASE_DOWNLOAD
    assert row["run_id"] == "r1"
    assert row["started_at"] == "2026-07-30T00:00:00+00:00"
    assert row["status"] == "downloaded"
    assert row["sha256"] == "abc"
    # every produced key is a real audit column
    assert set(row) <= set(bi._AUDIT_COLUMNS)


def test_audit_columns_match_stage_ddl_order():
    ddl_names = [
        chunk.strip().split()[0]
        for chunk in bi._AUDIT_STAGE_DDL.split(",")
    ]
    assert ddl_names == list(bi._AUDIT_COLUMNS)


def test_events_raw_stage_ddl_matches_columns():
    ddl_names = [c.strip().split()[0] for c in bi._EVENTS_RAW_STAGE_DDL.split(",")]
    assert ddl_names == list(bi._EVENTS_RAW_STAGE_COLUMNS)


def test_quarantine_stage_ddl_matches_columns():
    ddl_names = [c.strip().split()[0] for c in bi._QUARANTINE_STAGE_DDL.split(",")]
    assert ddl_names == list(bi._QUARANTINE_STAGE_COLUMNS)


# -- CLI -------------------------------------------------------------------------


def test_cli_dry_run(tmp_path, capsys):
    path = write_gz(tmp_path, [event_line(event_id="1"), b"broken {"])
    assert bi.main([path]) == 0
    out = capsys.readouterr().out
    assert '"lines_read": 2' in out
    assert '"quarantined": 1' in out
    assert "quarantine sample: line 2" in out
