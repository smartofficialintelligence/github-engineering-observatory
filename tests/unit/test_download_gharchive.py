"""Unit tests for the GH Archive downloader. No network access:
urllib.request.urlopen is monkeypatched with fakes."""

from __future__ import annotations

import datetime as dt
import gzip
import io
import json
import os
import urllib.error

import pytest

from github_observatory.ingestion import download_gharchive as dg


class FakeResponse:
    def __init__(self, body: bytes, status: int = 200, content_length: int | None = None):
        self._buf = io.BytesIO(body)
        self.status = status
        self.headers = {}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def read(self, n: int = -1) -> bytes:
        return self._buf.read(n)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class HeadersDict(dict):
    def get(self, key, default=None):  # urllib headers use .get
        return super().get(key, default)


def gz_payload(lines: list[bytes], trailing_newline: bool = True) -> bytes:
    raw = b"\n".join(lines) + (b"\n" if trailing_newline else b"")
    return gzip.compress(raw)


def make_urlopen(responses):
    """responses: list of callables or objects returned per call, in order."""
    calls = []

    def fake_urlopen(request, timeout=None):
        calls.append(request)
        item = responses[min(len(calls) - 1, len(responses) - 1)]
        if callable(item):
            item = item()
        if isinstance(item, Exception):
            raise item
        return item

    fake_urlopen.calls = calls
    return fake_urlopen


# --------------------------------------------------------------------------
# URL / spec parsing


def test_parse_hour_spec_accepts_padded_and_unpadded():
    assert dg.parse_hour_spec("2026-07-29-3") == dt.datetime(
        2026, 7, 29, 3, tzinfo=dt.timezone.utc
    )
    assert dg.parse_hour_spec("2026-07-29-03").hour == 3
    assert dg.parse_hour_spec("2026-07-29-15").hour == 15


@pytest.mark.parametrize("bad", ["2026-07-29", "2026-07-29-24", "2026-7-29-3", "garbage"])
def test_parse_hour_spec_rejects_invalid(bad):
    with pytest.raises(ValueError):
        dg.parse_hour_spec(bad)


def test_build_url_hour_is_unpadded():
    hour = dt.datetime(2026, 7, 29, 3, tzinfo=dt.timezone.utc)
    assert dg.build_url(hour) == "https://data.gharchive.org/2026-07-29-3.json.gz"
    assert dg.archive_filename(hour) == "2026-07-29-3.json.gz"


# --------------------------------------------------------------------------
# gzip validation


def test_validate_gzip_counts_lines(tmp_path):
    path = tmp_path / "x.json.gz"
    path.write_bytes(gz_payload([b'{"a":1}', b'{"a":2}']))
    valid, uncompressed, lines, err = dg.validate_gzip(str(path))
    assert valid and err is None
    assert lines == 2
    assert uncompressed == len(b'{"a":1}\n{"a":2}\n')


def test_validate_gzip_counts_final_unterminated_line(tmp_path):
    path = tmp_path / "x.json.gz"
    path.write_bytes(gz_payload([b'{"a":1}', b'{"a":2}'], trailing_newline=False))
    valid, _, lines, _ = dg.validate_gzip(str(path))
    assert valid and lines == 2


def test_validate_gzip_detects_corruption(tmp_path):
    path = tmp_path / "x.json.gz"
    body = gz_payload([b'{"a":1}' * 100])
    path.write_bytes(body[: len(body) // 2])  # truncated stream
    valid, _, _, err = dg.validate_gzip(str(path))
    assert not valid and err


# --------------------------------------------------------------------------
# download_hour behavior


def test_download_success_writes_file_and_audit_fields(tmp_path, monkeypatch):
    body = gz_payload([b'{"id":"1"}', b'{"id":"2"}'])
    fake = make_urlopen([lambda: FakeResponse(body, content_length=len(body))])
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    result = dg.download_hour("2026-07-29-15", str(tmp_path), retries=0)

    assert result.status == dg.STATUS_DOWNLOADED
    assert result.http_status == 200
    assert result.bytes_downloaded == len(body)
    assert result.content_length == len(body)
    assert result.line_count == 2
    assert result.gzip_valid is True
    assert result.sha256 and len(result.sha256) == 64
    assert result.download_completed_at is not None
    final = tmp_path / "2026-07-29-15.json.gz"
    assert final.read_bytes() == body
    assert not (tmp_path / "2026-07-29-15.json.gz.part").exists()


def test_partial_download_fails_clearly_and_removes_part(tmp_path, monkeypatch):
    body = gz_payload([b'{"id":"1"}'])
    fake = make_urlopen(
        [lambda: FakeResponse(body, content_length=len(body) + 999)]
    )
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    result = dg.download_hour("2026-07-29-15", str(tmp_path), retries=0)

    assert result.status == dg.STATUS_FAILED
    assert "partial download" in (result.error_message or "")
    assert not (tmp_path / "2026-07-29-15.json.gz").exists()
    assert not (tmp_path / "2026-07-29-15.json.gz.part").exists()


def test_corrupt_gzip_fails_and_removes_part(tmp_path, monkeypatch):
    good = gz_payload([b'{"id":"1"}' * 50])
    corrupt = good[: len(good) // 2]
    fake = make_urlopen([lambda: FakeResponse(corrupt, content_length=len(corrupt))])
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    result = dg.download_hour("2026-07-29-15", str(tmp_path), retries=0)

    assert result.status == dg.STATUS_FAILED
    assert "gzip integrity" in (result.error_message or "")
    assert not (tmp_path / "2026-07-29-15.json.gz").exists()


def test_existing_file_with_matching_size_is_skipped(tmp_path, monkeypatch):
    body = gz_payload([b'{"id":"1"}'])
    (tmp_path / "2026-07-29-15.json.gz").write_bytes(body)
    # Only a HEAD should happen; a GET returning different content would
    # corrupt the assertion below.
    fake = make_urlopen([lambda: FakeResponse(b"", content_length=len(body))])
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    result = dg.download_hour("2026-07-29-15", str(tmp_path))

    assert result.status == dg.STATUS_SKIPPED
    assert result.bytes_downloaded == 0
    assert (tmp_path / "2026-07-29-15.json.gz").read_bytes() == body
    assert len(fake.calls) == 1
    assert fake.calls[0].get_method() == "HEAD"


def test_existing_file_with_size_mismatch_is_redownloaded(tmp_path, monkeypatch):
    stale = gz_payload([b'{"id":"stale"}'])
    fresh = gz_payload([b'{"id":"fresh"}', b'{"id":"2"}'])
    (tmp_path / "2026-07-29-15.json.gz").write_bytes(stale)
    fake = make_urlopen(
        [
            lambda: FakeResponse(b"", content_length=len(fresh)),  # HEAD
            lambda: FakeResponse(fresh, content_length=len(fresh)),  # GET
        ]
    )
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    result = dg.download_hour("2026-07-29-15", str(tmp_path))

    assert result.status == dg.STATUS_DOWNLOADED
    assert (tmp_path / "2026-07-29-15.json.gz").read_bytes() == fresh


def test_http_404_fails_without_retries(tmp_path, monkeypatch):
    def raise_404():
        raise urllib.error.HTTPError(
            "https://data.gharchive.org/x", 404, "Not Found", HeadersDict(), None
        )

    fake = make_urlopen([raise_404, raise_404, raise_404])
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    result = dg.download_hour("2026-07-29-15", str(tmp_path), retries=3)

    assert result.status == dg.STATUS_FAILED
    assert result.http_status == 404
    assert len(fake.calls) == 1  # no retries on 404


def test_transient_error_is_retried(tmp_path, monkeypatch):
    body = gz_payload([b'{"id":"1"}'])

    def raise_urlerror():
        raise urllib.error.URLError("connection reset")

    fake = make_urlopen([raise_urlerror, lambda: FakeResponse(body, content_length=len(body))])
    monkeypatch.setattr(urllib.request, "urlopen", fake)
    monkeypatch.setattr(dg.time, "sleep", lambda s: None)

    result = dg.download_hour("2026-07-29-15", str(tmp_path), retries=1)

    assert result.status == dg.STATUS_DOWNLOADED
    assert len(fake.calls) == 2


def test_download_hours_appends_jsonl_audit(tmp_path, monkeypatch):
    body = gz_payload([b'{"id":"1"}'])
    fake = make_urlopen([lambda: FakeResponse(body, content_length=len(body))])
    monkeypatch.setattr(urllib.request, "urlopen", fake)

    results = dg.download_hours(["2026-07-29-15"], str(tmp_path))

    audit_path = tmp_path / dg.AUDIT_FILENAME
    assert audit_path.exists()
    records = [json.loads(line) for line in audit_path.read_text().splitlines()]
    assert len(records) == len(results) == 1
    rec = records[0]
    assert rec["status"] == dg.STATUS_DOWNLOADED
    assert rec["source_url"].endswith("2026-07-29-15.json.gz")
    assert rec["run_id"] == results[0].run_id
    assert rec["source_hour"] == "2026-07-29T15:00:00+00:00"
