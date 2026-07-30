"""Download GH Archive hourly event files (spec §7 Task 2).

Fetches https://data.gharchive.org/YYYY-MM-DD-H.json.gz files into a
destination directory — locally any path, on Databricks the Unity
Catalog Volume /Volumes/github_observatory/bronze/raw_files (FUSE
paths work with plain Python file APIs on serverless compute).

Guarantees:

* Idempotent: an existing file whose size matches the remote
  Content-Length is skipped; a size mismatch is treated as a suspect
  partial file and re-downloaded.
* Partial downloads fail clearly: data streams to ``<name>.part`` and
  is only moved into place after the byte count matches Content-Length
  and the gzip stream decompresses end to end.
* Every attempt emits a structured audit record (JSONL) with source
  URL, HTTP status, byte counts, sha256, timestamps, and outcome —
  field names align with github_observatory.bronze.ingestion_audit.
* Sends a browser-like User-Agent: GH Archive returns HTTP 403 to the
  default urllib User-Agent.

The hour segment of GH Archive URLs is NOT zero-padded (verified
2026-07-30: ``2026-07-29-3.json.gz`` exists, ``2026-07-29-03.json.gz``
returns 404). Zero-padded input is accepted and normalized.

Usage:

    python -m github_observatory.ingestion.download_gharchive \
        2026-07-29-15 2026-07-29-3 --dest /path/to/raw_files
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import gzip
import hashlib
import json
import logging
import os
import re
import shutil
import sys
import time
import urllib.error
import urllib.request
import uuid
from typing import Iterable, Sequence

from github_observatory.common.config import (
    GHARCHIVE_URL_TEMPLATE,
    HTTP_USER_AGENT,
    RAW_VOLUME_PATH,
)

logger = logging.getLogger("github_observatory.download")

CHUNK_SIZE = 1 << 20  # 1 MiB
AUDIT_FILENAME = "_download_audit.jsonl"
_HOUR_SPEC_RE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})-(\d{1,2})$")

# Statuses a DownloadResult can carry.
STATUS_DOWNLOADED = "downloaded"
STATUS_SKIPPED = "skipped_exists"
STATUS_FAILED = "failed"


@dataclasses.dataclass
class DownloadResult:
    """Audit record for one hour-file download attempt."""

    run_id: str
    source_url: str
    source_file: str
    source_hour: str  # ISO-8601 UTC hour, e.g. 2026-07-29T15:00:00+00:00
    download_started_at: str
    download_completed_at: str | None = None
    http_status: int | None = None
    content_length: int | None = None
    bytes_downloaded: int = 0
    uncompressed_bytes: int | None = None
    line_count: int | None = None
    sha256: str | None = None
    gzip_valid: bool | None = None
    status: str = STATUS_FAILED
    error_message: str | None = None
    dest_path: str | None = None

    def to_json(self) -> str:
        return json.dumps(dataclasses.asdict(self), sort_keys=True)


def parse_hour_spec(spec: str) -> dt.datetime:
    """Parse ``YYYY-MM-DD-H`` (zero-padded hour tolerated) to a UTC datetime."""
    m = _HOUR_SPEC_RE.match(spec.strip())
    if not m:
        raise ValueError(
            f"invalid hour spec {spec!r}: expected YYYY-MM-DD-H, e.g. 2026-07-29-15"
        )
    year, month, day, hour = (int(g) for g in m.groups())
    if not 0 <= hour <= 23:
        raise ValueError(f"invalid hour {hour} in spec {spec!r}: must be 0-23")
    return dt.datetime(year, month, day, hour, tzinfo=dt.timezone.utc)


def build_url(hour_dt: dt.datetime) -> str:
    """GH Archive URL for an hour. The hour segment is unpadded."""
    return GHARCHIVE_URL_TEMPLATE.format(
        date=hour_dt.strftime("%Y-%m-%d"), hour=hour_dt.hour
    )


def archive_filename(hour_dt: dt.datetime) -> str:
    """Canonical local filename, identical to the GH Archive basename."""
    return f"{hour_dt.strftime('%Y-%m-%d')}-{hour_dt.hour}.json.gz"


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _request(url: str, method: str = "GET") -> urllib.request.Request:
    return urllib.request.Request(url, method=method, headers={"User-Agent": HTTP_USER_AGENT})


def remote_content_length(url: str, timeout: float) -> tuple[int | None, int]:
    """HEAD the URL; return (content_length, http_status)."""
    with urllib.request.urlopen(_request(url, "HEAD"), timeout=timeout) as resp:
        length = resp.headers.get("Content-Length")
        return (int(length) if length is not None else None), resp.status


def validate_gzip(path: str) -> tuple[bool, int, int, str | None]:
    """Decompress the whole file to prove integrity.

    Returns (valid, uncompressed_bytes, line_count, error_message).
    line_count counts newline-terminated records plus a final
    unterminated record if present.
    """
    total = 0
    newlines = 0
    ends_with_newline = True
    try:
        with gzip.open(path, "rb") as fh:
            while True:
                chunk = fh.read(CHUNK_SIZE)
                if not chunk:
                    break
                total += len(chunk)
                newlines += chunk.count(b"\n")
                ends_with_newline = chunk.endswith(b"\n")
    except (OSError, EOFError, gzip.BadGzipFile) as exc:
        return False, total, newlines, f"{type(exc).__name__}: {exc}"
    lines = newlines if ends_with_newline else newlines + 1
    return True, total, (lines if total else 0), None


def _sha256_of_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(CHUNK_SIZE)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _move_into_place(part_path: str, final_path: str) -> None:
    try:
        os.replace(part_path, final_path)
    except OSError:
        # Some FUSE mounts reject rename; fall back to copy + delete.
        shutil.copyfile(part_path, final_path)
        os.remove(part_path)


def download_hour(
    hour: dt.datetime | str,
    dest_dir: str = RAW_VOLUME_PATH,
    *,
    run_id: str | None = None,
    timeout: float = 60.0,
    retries: int = 3,
    force: bool = False,
    validate: bool = True,
) -> DownloadResult:
    """Download one GH Archive hour into dest_dir. Never raises for a
    failed download — inspect ``result.status`` (callers doing batch
    work need the other hours to proceed)."""
    hour_dt = parse_hour_spec(hour) if isinstance(hour, str) else hour
    if hour_dt.tzinfo is None:
        hour_dt = hour_dt.replace(tzinfo=dt.timezone.utc)
    url = build_url(hour_dt)
    filename = archive_filename(hour_dt)
    final_path = os.path.join(dest_dir, filename)
    part_path = final_path + ".part"

    result = DownloadResult(
        run_id=run_id or uuid.uuid4().hex,
        source_url=url,
        source_file=filename,
        source_hour=hour_dt.isoformat(),
        download_started_at=_utcnow_iso(),
        dest_path=final_path,
    )

    os.makedirs(dest_dir, exist_ok=True)

    # Idempotency: skip when the existing file matches the remote size.
    if os.path.exists(final_path) and not force:
        local_size = os.path.getsize(final_path)
        try:
            remote_size, head_status = remote_content_length(url, timeout)
        except (urllib.error.URLError, OSError) as exc:
            remote_size, head_status = None, None
            logger.warning("HEAD %s failed (%s); validating local gzip instead", url, exc)
        if remote_size is not None and local_size == remote_size:
            result.status = STATUS_SKIPPED
            result.http_status = head_status
            result.content_length = remote_size
            result.bytes_downloaded = 0
            result.gzip_valid = None
            result.download_completed_at = _utcnow_iso()
            result.error_message = None
            logger.info("skip %s: exists with matching size %d bytes", filename, local_size)
            return result
        if remote_size is None:
            valid, _, _, err = validate_gzip(final_path)
            if valid:
                result.status = STATUS_SKIPPED
                result.gzip_valid = True
                result.bytes_downloaded = 0
                result.download_completed_at = _utcnow_iso()
                logger.info("skip %s: exists and gzip-valid (size unverified)", filename)
                return result
            logger.warning("existing %s failed gzip check (%s); re-downloading", filename, err)
        else:
            logger.warning(
                "existing %s size %d != remote %d; re-downloading",
                filename, local_size, remote_size,
            )

    last_error: str | None = None
    for attempt in range(retries + 1):
        if attempt:
            backoff = 2 ** attempt
            logger.info("retry %d/%d for %s in %ds", attempt, retries, url, backoff)
            time.sleep(backoff)
        try:
            outcome = _attempt_download(url, part_path, final_path, timeout, result, validate)
            if outcome:
                return result
            last_error = result.error_message
        except urllib.error.HTTPError as exc:
            result.http_status = exc.code
            last_error = f"HTTP {exc.code} {exc.reason} for {url}"
            if exc.code in (403, 404):
                # Not transient: 404 = hour not published (or bad spec);
                # 403 = blocked client. Retrying cannot help.
                break
        except (urllib.error.URLError, OSError, EOFError) as exc:
            last_error = f"{type(exc).__name__}: {exc}"
    if os.path.exists(part_path):
        os.remove(part_path)
    result.status = STATUS_FAILED
    result.error_message = last_error
    result.download_completed_at = _utcnow_iso()
    logger.error("FAILED %s: %s", url, last_error)
    return result


def _attempt_download(
    url: str,
    part_path: str,
    final_path: str,
    timeout: float,
    result: DownloadResult,
    validate: bool,
) -> bool:
    """One download attempt. True on success (result mutated); False on
    a clean failure recorded in result; raises on transport errors."""
    digest = hashlib.sha256()
    bytes_downloaded = 0
    with urllib.request.urlopen(_request(url), timeout=timeout) as resp:
        result.http_status = resp.status
        length = resp.headers.get("Content-Length")
        result.content_length = int(length) if length is not None else None
        with open(part_path, "wb") as out:
            while True:
                chunk = resp.read(CHUNK_SIZE)
                if not chunk:
                    break
                out.write(chunk)
                digest.update(chunk)
                bytes_downloaded += len(chunk)
    result.bytes_downloaded = bytes_downloaded

    if result.content_length is not None and bytes_downloaded != result.content_length:
        os.remove(part_path)
        result.error_message = (
            f"partial download: got {bytes_downloaded} of {result.content_length} bytes"
        )
        logger.warning("%s for %s", result.error_message, url)
        return False

    if validate:
        valid, uncompressed, lines, err = validate_gzip(part_path)
        result.gzip_valid = valid
        result.uncompressed_bytes = uncompressed
        result.line_count = lines
        if not valid:
            os.remove(part_path)
            result.error_message = f"gzip integrity check failed: {err}"
            logger.warning("%s for %s", result.error_message, url)
            return False

    result.sha256 = digest.hexdigest()
    _move_into_place(part_path, final_path)
    result.status = STATUS_DOWNLOADED
    result.error_message = None
    result.download_completed_at = _utcnow_iso()
    logger.info(
        "downloaded %s: HTTP %s, %d bytes, %s lines, sha256=%s",
        result.source_file, result.http_status, bytes_downloaded,
        result.line_count, result.sha256,
    )
    return True


def append_audit(results: Iterable[DownloadResult], dest_dir: str) -> str:
    """Write audit records as JSONL next to the data files.

    Appends to ``_download_audit.jsonl`` where the filesystem allows it.
    Unity Catalog Volume FUSE mounts reject appending to a non-empty
    file (``OSError: Illegal seek`` — append must seek to end), so on
    that error the records go to a fresh per-run file instead
    (``_download_audit.<run_id>.jsonl``, sequential write only).
    Consumers must glob ``_download_audit*.jsonl``.
    """
    results = list(results)
    audit_path = os.path.join(dest_dir, AUDIT_FILENAME)
    try:
        with open(audit_path, "a", encoding="utf-8") as fh:
            for result in results:
                fh.write(result.to_json() + "\n")
        return audit_path
    except OSError as exc:
        if exc.errno != 29:  # ESPIPE: illegal seek (FUSE append limitation)
            raise
    run_id = results[0].run_id if results else uuid.uuid4().hex
    stem, ext = os.path.splitext(AUDIT_FILENAME)
    per_run_path = os.path.join(dest_dir, f"{stem}.{run_id}{ext}")
    with open(per_run_path, "w", encoding="utf-8") as fh:
        for result in results:
            fh.write(result.to_json() + "\n")
    logger.info("audit append unsupported on %s; wrote %s", audit_path, per_run_path)
    return per_run_path


def download_hours(
    hours: Sequence[dt.datetime | str],
    dest_dir: str = RAW_VOLUME_PATH,
    *,
    timeout: float = 60.0,
    retries: int = 3,
    force: bool = False,
    validate: bool = True,
    write_audit: bool = True,
) -> list[DownloadResult]:
    """Download several hours under one run_id, appending audit records."""
    run_id = uuid.uuid4().hex
    results = [
        download_hour(
            hour, dest_dir, run_id=run_id, timeout=timeout,
            retries=retries, force=force, validate=validate,
        )
        for hour in hours
    ]
    if write_audit:
        append_audit(results, dest_dir)
    return results


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Download GH Archive hourly files with audit logging."
    )
    parser.add_argument(
        "hours", nargs="+",
        help="hour specs like 2026-07-29-15 (hour 0-23, zero-padding tolerated)",
    )
    parser.add_argument("--dest", default=RAW_VOLUME_PATH, help="destination directory")
    parser.add_argument("--timeout", type=float, default=60.0)
    parser.add_argument("--retries", type=int, default=3)
    parser.add_argument("--force", action="store_true", help="re-download existing files")
    parser.add_argument(
        "--no-validate", dest="validate", action="store_false",
        help="skip gzip integrity validation",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s"
    )
    try:
        hour_dts = [parse_hour_spec(spec) for spec in args.hours]
    except ValueError as exc:
        parser.error(str(exc))

    results = download_hours(
        hour_dts, args.dest, timeout=args.timeout, retries=args.retries,
        force=args.force, validate=args.validate,
    )
    failed = [r for r in results if r.status == STATUS_FAILED]
    for r in results:
        print(
            f"{r.status:15s} {r.source_file:22s} http={r.http_status} "
            f"bytes={r.bytes_downloaded} lines={r.line_count} url={r.source_url}"
        )
    if failed:
        print(f"{len(failed)} of {len(results)} downloads failed", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
