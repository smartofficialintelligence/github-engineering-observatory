"""Bronze ingestion: raw hour files → ``bronze.events_raw`` (spec §21 step 5).

Parses GH Archive hour files from the raw Volume into the Bronze layer:

* ``bronze.events_raw`` — one row per event, the eight top-level fields
  kept as-is: scalar columns for ``id``/``type``/``public``/``created_at``
  (``created_at`` stays a STRING; Silver casts to TIMESTAMP) and raw JSON
  strings for ``actor``/``repo``/``org``/``payload``. Nothing beyond a
  usable ``id`` is required — rows with ``public = false`` or an empty
  ``repo`` (Finding S6) ingest normally and are handled in Silver.
* ``bronze.events_quarantine`` — lines that cannot become events_raw rows
  (malformed JSON, non-object JSON, missing/unusable ``id``), with the
  raw line preserved for replay after a parser fix.
* ``bronze.ingestion_audit`` — one row per (run, file) with parse counts;
  the same table receives download-phase rows whose field names align
  with ``download_gharchive.DownloadResult``.

Idempotency: re-ingesting a file is safe everywhere. events_raw is
MERGEd on ``event_id`` (the empirically confirmed primary key — dedupe
never uses ``push_id``, which repeats; Finding S5). Quarantine is MERGEd
on ``(source_file, source_line)``. The audit table is append-only by
design: every attempt leaves a row.

Unknown event types must not fail ingestion (forward compatibility) —
``event_type`` is stored verbatim and never validated against a
whitelist at Bronze.

Parsing is pure standard library and runs identically on a laptop and on
Databricks serverless; Spark/Delta touch-points are isolated in the
``*_delta``/``ingest_*`` functions that receive a ``SparkSession``.

Local dry run (no Spark; writes nothing):

    python -m github_observatory.ingestion.bronze_ingest \
        raw_files/2026-07-29-15.json.gz --stats-only
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as dt
import gzip
import json
import logging
import os
import sys
import uuid
from typing import Any, Iterator, Sequence

from github_observatory.common.config import (
    EVENTS_QUARANTINE_TABLE,
    EVENTS_RAW_TABLE,
    INGESTION_AUDIT_TABLE,
)
from github_observatory.schema.profile_schema import infer_source_hour

logger = logging.getLogger("github_observatory.bronze_ingest")

# Quarantined raw lines are capped so one pathological line cannot bloat
# the table; the original length is always recorded.
QUARANTINE_RAW_LINE_MAX_CHARS = 65_536

# Rows staged per MERGE. Bounds driver memory: one hour file is ~170k
# events / ~1 GB of JSON text, which comfortably exceeds what a single
# createDataFrame call should hold.
MERGE_CHUNK_ROWS = 25_000

REASON_MALFORMED_JSON = "malformed_json"
REASON_NOT_OBJECT = "not_json_object"
REASON_MISSING_ID = "missing_event_id"

EVENTS_RAW_DDL = (
    "event_id STRING, event_type STRING, public BOOLEAN, created_at STRING, "
    "actor_json STRING, repo_json STRING, org_json STRING, payload_json STRING, "
    "source_file STRING, source_line BIGINT, source_hour TIMESTAMP, "
    "source_date DATE, ingest_run_id STRING, ingested_at TIMESTAMP"
)

QUARANTINE_DDL = (
    "source_file STRING, source_line BIGINT, reason STRING, event_id STRING, "
    "raw_line STRING, raw_length BIGINT, source_hour TIMESTAMP, "
    "source_date DATE, ingest_run_id STRING, ingested_at TIMESTAMP"
)

INGESTION_AUDIT_DDL = (
    "run_id STRING, phase STRING, source_file STRING, source_hour TIMESTAMP, "
    "started_at TIMESTAMP, completed_at TIMESTAMP, status STRING, "
    "error_message STRING, "
    # download-phase fields (DownloadResult names)
    "source_url STRING, http_status INT, content_length BIGINT, "
    "bytes_downloaded BIGINT, uncompressed_bytes BIGINT, line_count BIGINT, "
    "sha256 STRING, gzip_valid BOOLEAN, dest_path STRING, "
    # bronze-ingest-phase fields
    "lines_read BIGINT, events_parsed BIGINT, rows_staged BIGINT, "
    "rows_inserted BIGINT, duplicates_skipped BIGINT, quarantined BIGINT, "
    "within_file_duplicate_ids BIGINT"
)

PHASE_DOWNLOAD = "download"
PHASE_BRONZE = "bronze_ingest"

STATUS_SUCCEEDED = "succeeded"
STATUS_FAILED = "failed"


@dataclasses.dataclass
class ParsedLine:
    """Outcome of parsing one raw line: an events_raw row or a quarantine row.

    Exactly one of ``row``/``quarantine`` is set.
    """

    line_no: int
    row: dict[str, Any] | None = None
    quarantine: dict[str, Any] | None = None


@dataclasses.dataclass
class BronzeFileStats:
    """Parse/ingest counters for one raw file."""

    source_file: str
    source_hour: str | None
    lines_read: int = 0
    events_parsed: int = 0  # parsed to a valid events_raw candidate
    rows_staged: int = 0  # candidates handed to MERGE (after in-file dedupe)
    rows_inserted: int = 0  # actually new in events_raw (MERGE metric)
    duplicates_skipped: int = 0  # rows_staged - rows_inserted (cross-run/cross-file)
    quarantined: int = 0
    within_file_duplicate_ids: int = 0

    def as_dict(self) -> dict[str, Any]:
        return dataclasses.asdict(self)


def _utcnow_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")


def _dump_json(value: Any) -> str | None:
    """Re-serialize one top-level object compactly; None stays None.

    CPython preserves key order, so this is byte-stable for a given
    input modulo insignificant whitespace.
    """
    if value is None:
        return None
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def parse_raw_line(
    line: bytes,
    line_no: int,
    source_file: str,
    source_hour_iso: str | None,
) -> ParsedLine:
    """Parse one raw JSONL line into an events_raw row or a quarantine row.

    The Bronze contract is deliberately minimal: the line must be a JSON
    object carrying a non-empty ``id``. Everything else — unknown event
    types, ``public: false``, empty ``repo`` (Finding S6), missing
    ``created_at`` — ingests as-is for Silver to judge.
    """

    def quarantine_row(reason: str, event_id: str | None = None) -> ParsedLine:
        text = line.decode("utf-8", "replace")
        return ParsedLine(
            line_no=line_no,
            quarantine={
                "source_file": source_file,
                "source_line": line_no,
                "reason": reason,
                "event_id": event_id,
                "raw_line": text[:QUARANTINE_RAW_LINE_MAX_CHARS],
                "raw_length": len(text),
                "source_hour": source_hour_iso,
            },
        )

    try:
        event = json.loads(line)
    except (ValueError, UnicodeDecodeError):
        return quarantine_row(REASON_MALFORMED_JSON)
    if not isinstance(event, dict):
        return quarantine_row(REASON_NOT_OBJECT)

    event_id = event.get("id")
    # GH event ids are numeric strings; tolerate a bare number but store STRING.
    if isinstance(event_id, int) and not isinstance(event_id, bool):
        event_id = str(event_id)
    if not isinstance(event_id, str) or not event_id.strip():
        return quarantine_row(REASON_MISSING_ID)

    event_type = event.get("type")
    public = event.get("public")
    created_at = event.get("created_at")
    return ParsedLine(
        line_no=line_no,
        row={
            "event_id": event_id,
            "event_type": event_type if isinstance(event_type, str) else None,
            "public": public if isinstance(public, bool) else None,
            "created_at": created_at if isinstance(created_at, str) else None,
            "actor_json": _dump_json(event.get("actor")),
            "repo_json": _dump_json(event.get("repo")),
            "org_json": _dump_json(event.get("org")),
            "payload_json": _dump_json(event.get("payload")),
            "source_file": source_file,
            "source_line": line_no,
            "source_hour": source_hour_iso,
        },
    )


def iter_parsed_lines(path: str, source_hour: dt.datetime | None = None) -> Iterator[ParsedLine]:
    """Stream a raw hour file (gzip or plain JSONL) as ParsedLine objects.

    Blank lines are skipped without a line-number gap (numbers stay
    physical so quarantine rows point at the real file line).
    """
    source_file = os.path.basename(path)
    if source_hour is None:
        source_hour = infer_source_hour(path)
    source_hour_iso = source_hour.isoformat() if source_hour else None

    opener = gzip.open if path.endswith(".gz") else open
    with opener(path, "rb") as fh:
        for line_no, line in enumerate(fh, start=1):
            line = line.strip()
            if not line:
                continue
            yield parse_raw_line(line, line_no, source_file, source_hour_iso)


def parse_file(
    path: str,
    source_hour: dt.datetime | None = None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], BronzeFileStats]:
    """Parse a whole file: (events_raw rows, quarantine rows, stats).

    Rows are deduped on ``event_id`` within the file (first occurrence
    wins); cross-file/cross-run duplicates are left to the Delta MERGE.
    """
    if source_hour is None:
        source_hour = infer_source_hour(path)
    stats = BronzeFileStats(
        source_file=os.path.basename(path),
        source_hour=source_hour.isoformat() if source_hour else None,
    )
    rows: list[dict[str, Any]] = []
    quarantine: list[dict[str, Any]] = []
    seen_ids: set[str] = set()
    for parsed in iter_parsed_lines(path, source_hour):
        stats.lines_read += 1
        if parsed.quarantine is not None:
            stats.quarantined += 1
            quarantine.append(parsed.quarantine)
            continue
        assert parsed.row is not None
        stats.events_parsed += 1
        event_id = parsed.row["event_id"]
        if event_id in seen_ids:
            stats.within_file_duplicate_ids += 1
            continue
        seen_ids.add(event_id)
        rows.append(parsed.row)
    stats.rows_staged = len(rows)
    return rows, quarantine, stats


def _chunks(rows: Sequence[dict[str, Any]], size: int) -> Iterator[Sequence[dict[str, Any]]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


# -- Delta persistence (call from Databricks) ---------------------------------

# Column orders for tuple conversion; must match the DDLs above minus the
# columns computed in SQL (source_date, ingested_at, ingest_run_id are
# appended at stage time).
_EVENTS_RAW_STAGE_COLUMNS = (
    "event_id", "event_type", "public", "created_at",
    "actor_json", "repo_json", "org_json", "payload_json",
    "source_file", "source_line", "source_hour",
)

_QUARANTINE_STAGE_COLUMNS = (
    "source_file", "source_line", "reason", "event_id",
    "raw_line", "raw_length", "source_hour",
)

_EVENTS_RAW_STAGE_DDL = (
    "event_id STRING, event_type STRING, public BOOLEAN, created_at STRING, "
    "actor_json STRING, repo_json STRING, org_json STRING, payload_json STRING, "
    "source_file STRING, source_line BIGINT, source_hour STRING"
)

_QUARANTINE_STAGE_DDL = (
    "source_file STRING, source_line BIGINT, reason STRING, event_id STRING, "
    "raw_line STRING, raw_length BIGINT, source_hour STRING"
)


def create_bronze_tables(spark: Any) -> None:
    """Create the three Bronze tables if they do not exist."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {EVENTS_RAW_TABLE} ({EVENTS_RAW_DDL}) "
        "USING DELTA PARTITIONED BY (source_date)"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {EVENTS_QUARANTINE_TABLE} ({QUARANTINE_DDL}) "
        "USING DELTA"
    )
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {INGESTION_AUDIT_TABLE} ({INGESTION_AUDIT_DDL}) "
        "USING DELTA"
    )


def _merge_metrics(merge_result: Any) -> dict[str, int]:
    """Extract row-count metrics from the DataFrame a MERGE returns."""
    try:
        row = merge_result.collect()[0].asDict()
    except Exception:  # pragma: no cover - metric extraction is best-effort
        return {}
    return {k: int(v) for k, v in row.items() if isinstance(v, (int, float)) and v is not None}


def _stage_and_merge(
    spark: Any,
    chunk: Sequence[dict[str, Any]],
    stage_columns: Sequence[str],
    stage_ddl: str,
    run_id: str,
    merge_sql_template: str,
) -> dict[str, int]:
    """Stage one chunk as a temp view and run the given MERGE statement."""
    data = [tuple(row[col] for col in stage_columns) for row in chunk]
    df = spark.createDataFrame(data, schema=stage_ddl)
    view = f"_bronze_stage_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    try:
        merge_sql = merge_sql_template.format(view=view, run_id=run_id)
        return _merge_metrics(spark.sql(merge_sql))
    finally:
        spark.catalog.dropTempView(view)


_EVENTS_RAW_MERGE_SQL = f"""
MERGE INTO {EVENTS_RAW_TABLE} AS t
USING (
    SELECT event_id, event_type, public, created_at,
           actor_json, repo_json, org_json, payload_json,
           source_file, source_line,
           CAST(source_hour AS TIMESTAMP) AS source_hour,
           CAST(CAST(source_hour AS TIMESTAMP) AS DATE) AS source_date,
           '{{run_id}}' AS ingest_run_id,
           current_timestamp() AS ingested_at
    FROM {{view}}
) AS s
ON t.event_id = s.event_id
WHEN NOT MATCHED THEN INSERT *
"""

_QUARANTINE_MERGE_SQL = f"""
MERGE INTO {EVENTS_QUARANTINE_TABLE} AS t
USING (
    SELECT source_file, source_line, reason, event_id, raw_line, raw_length,
           CAST(source_hour AS TIMESTAMP) AS source_hour,
           CAST(CAST(source_hour AS TIMESTAMP) AS DATE) AS source_date,
           '{{run_id}}' AS ingest_run_id,
           current_timestamp() AS ingested_at
    FROM {{view}}
) AS s
ON t.source_file = s.source_file AND t.source_line = s.source_line
WHEN NOT MATCHED THEN INSERT *
"""


def ingest_file(
    spark: Any,
    path: str,
    *,
    run_id: str | None = None,
    source_hour: dt.datetime | None = None,
    chunk_rows: int = MERGE_CHUNK_ROWS,
) -> BronzeFileStats:
    """Ingest one raw hour file into the Bronze tables (idempotent)."""
    run_id = run_id or uuid.uuid4().hex
    rows, quarantine, stats = parse_file(path, source_hour)

    for chunk in _chunks(rows, chunk_rows):
        metrics = _stage_and_merge(
            spark, chunk, _EVENTS_RAW_STAGE_COLUMNS, _EVENTS_RAW_STAGE_DDL,
            run_id, _EVENTS_RAW_MERGE_SQL,
        )
        stats.rows_inserted += metrics.get("num_inserted_rows", 0)
    stats.duplicates_skipped = stats.rows_staged - stats.rows_inserted

    for chunk in _chunks(quarantine, chunk_rows):
        _stage_and_merge(
            spark, chunk, _QUARANTINE_STAGE_COLUMNS, _QUARANTINE_STAGE_DDL,
            run_id, _QUARANTINE_MERGE_SQL,
        )

    logger.info(
        "bronze ingest %s: read=%d parsed=%d staged=%d inserted=%d "
        "dup_skipped=%d quarantined=%d",
        stats.source_file, stats.lines_read, stats.events_parsed,
        stats.rows_staged, stats.rows_inserted, stats.duplicates_skipped,
        stats.quarantined,
    )
    return stats


def write_audit_rows(spark: Any, rows: Sequence[dict[str, Any]]) -> None:
    """Append rows to bronze.ingestion_audit (append-only by design).

    Accepts dicts with any subset of the audit columns — download-phase
    rows (``DownloadResult`` dicts plus ``phase``) and bronze-phase rows
    both fit; missing columns become NULL.
    """
    if not rows:
        return
    normalized = [{col: row.get(col) for col in _AUDIT_COLUMNS} for row in rows]
    data = [tuple(row[col] for col in _AUDIT_COLUMNS) for row in normalized]
    df = spark.createDataFrame(data, schema=_AUDIT_STAGE_DDL)
    view = f"_audit_stage_{uuid.uuid4().hex}"
    df.createOrReplaceTempView(view)
    try:
        spark.sql(f"""
            INSERT INTO {INGESTION_AUDIT_TABLE}
            SELECT run_id, phase, source_file,
                   CAST(source_hour AS TIMESTAMP),
                   CAST(started_at AS TIMESTAMP),
                   CAST(completed_at AS TIMESTAMP),
                   status, error_message,
                   source_url, http_status, content_length,
                   bytes_downloaded, uncompressed_bytes, line_count,
                   sha256, gzip_valid, dest_path,
                   lines_read, events_parsed, rows_staged,
                   rows_inserted, duplicates_skipped, quarantined,
                   within_file_duplicate_ids
            FROM {view}
        """)
    finally:
        spark.catalog.dropTempView(view)


_AUDIT_COLUMNS = (
    "run_id", "phase", "source_file", "source_hour", "started_at",
    "completed_at", "status", "error_message", "source_url", "http_status",
    "content_length", "bytes_downloaded", "uncompressed_bytes", "line_count",
    "sha256", "gzip_valid", "dest_path", "lines_read", "events_parsed",
    "rows_staged", "rows_inserted", "duplicates_skipped", "quarantined",
    "within_file_duplicate_ids",
)

_AUDIT_STAGE_DDL = (
    "run_id STRING, phase STRING, source_file STRING, source_hour STRING, "
    "started_at STRING, completed_at STRING, status STRING, "
    "error_message STRING, source_url STRING, http_status INT, "
    "content_length BIGINT, bytes_downloaded BIGINT, uncompressed_bytes BIGINT, "
    "line_count BIGINT, sha256 STRING, gzip_valid BOOLEAN, dest_path STRING, "
    "lines_read BIGINT, events_parsed BIGINT, rows_staged BIGINT, "
    "rows_inserted BIGINT, duplicates_skipped BIGINT, quarantined BIGINT, "
    "within_file_duplicate_ids BIGINT"
)


def download_result_audit_row(result: Any) -> dict[str, Any]:
    """Map a ``DownloadResult`` (object or dict) to an audit row."""
    src = result if isinstance(result, dict) else dataclasses.asdict(result)
    return {
        "run_id": src.get("run_id"),
        "phase": PHASE_DOWNLOAD,
        "source_file": src.get("source_file"),
        "source_hour": src.get("source_hour"),
        "started_at": src.get("download_started_at"),
        "completed_at": src.get("download_completed_at"),
        "status": src.get("status"),
        "error_message": src.get("error_message"),
        "source_url": src.get("source_url"),
        "http_status": src.get("http_status"),
        "content_length": src.get("content_length"),
        "bytes_downloaded": src.get("bytes_downloaded"),
        "uncompressed_bytes": src.get("uncompressed_bytes"),
        "line_count": src.get("line_count"),
        "sha256": src.get("sha256"),
        "gzip_valid": src.get("gzip_valid"),
        "dest_path": src.get("dest_path"),
    }


def ingest_files(
    spark: Any,
    paths: Sequence[str],
    *,
    chunk_rows: int = MERGE_CHUNK_ROWS,
) -> list[BronzeFileStats]:
    """Ingest several files under one run_id, auditing every attempt.

    A file that raises is recorded as failed in the audit table and does
    not stop the remaining files.
    """
    run_id = uuid.uuid4().hex
    all_stats: list[BronzeFileStats] = []
    audit_rows: list[dict[str, Any]] = []
    for path in paths:
        started_at = _utcnow_iso()
        base = {
            "run_id": run_id,
            "phase": PHASE_BRONZE,
            "source_file": os.path.basename(path),
            "started_at": started_at,
        }
        try:
            stats = ingest_file(spark, path, run_id=run_id, chunk_rows=chunk_rows)
        except Exception as exc:
            logger.exception("bronze ingest failed for %s", path)
            audit_rows.append(
                base | {
                    "completed_at": _utcnow_iso(),
                    "status": STATUS_FAILED,
                    "error_message": f"{type(exc).__name__}: {exc}"[:1000],
                }
            )
            continue
        all_stats.append(stats)
        audit_rows.append(
            base | stats.as_dict() | {
                "source_hour": stats.source_hour,
                "completed_at": _utcnow_iso(),
                "status": STATUS_SUCCEEDED,
            }
        )
    write_audit_rows(spark, audit_rows)
    return all_stats


# -- local dry run -------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Parse raw GH Archive files through the Bronze contract "
        "(local dry run; no Spark, nothing written)."
    )
    parser.add_argument("files", nargs="+", help="raw .json.gz (or .json) files")
    parser.add_argument(
        "--stats-only", action="store_true",
        help="print per-file stats as JSON (default behavior; flag kept for clarity)",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    exit_code = 0
    for path in args.files:
        rows, quarantine, stats = parse_file(path)
        print(json.dumps(stats.as_dict(), indent=2))
        for q in quarantine[:5]:
            print(f"  quarantine sample: line {q['source_line']} "
                  f"reason={q['reason']} len={q['raw_length']}")
        if stats.lines_read != stats.events_parsed + stats.quarantined:
            print(f"  COUNT MISMATCH in {path}", file=sys.stderr)
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    sys.exit(main())
