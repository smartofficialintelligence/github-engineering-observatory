"""GH Archive stream schema profiler (spec §7 Task 3).

Streams raw hourly ``.json.gz`` files and produces, per event type:

* row counts;
* every observed field path, top-level and nested (array element
  fields use ``[]`` notation, e.g. ``payload.commits[].sha``);
* inferred Spark-style types, with inconsistent types surfaced;
* presence counts/rates and explicit-null observations;
* one truncated sample value per path;
* contract-field verdicts for the fields the spec requires us to test;
* candidate primary/linkage key diagnostics (event id uniqueness,
  push_id, repo+number → issue/PR id mappings, review/release ids);
* per-event-type ``payload.action`` distributions;
* PushEvent commit-payload statistics (are commit counts available?);
* malformed-line and event-time-outside-source-hour counts.

Column semantics of the emitted profile rows (spec-required schema):

* ``presence_rate`` — fraction of events of that type in which the key
  is present at all (an explicit JSON ``null`` counts as present).
* ``nullable`` — true when an explicit JSON ``null`` was observed for
  the path. Optionality-by-absence is visible as ``presence_rate < 1``.

The profiler is pure standard library, so it runs identically on a
laptop and on Databricks serverless. ``write_profile_delta`` persists
rows to ``github_observatory.bronze.schema_profile`` when a
SparkSession is available.

Usage:

    python -m github_observatory.schema.profile_schema RAW_FILE... \
        --out-dir artifacts/schema_profile
"""

from __future__ import annotations

import argparse
import collections
import csv
import datetime as dt
import gzip
import io
import json
import logging
import os
import re
import sys
from typing import Any, Iterable, Iterator, Sequence

from github_observatory.common.config import SCHEMA_PROFILE_TABLE

logger = logging.getLogger("github_observatory.schema_profile")

# Fields the spec (§7 Task 3) requires explicit verdicts for.
CONTRACT_FIELDS: tuple[str, ...] = (
    "id",
    "type",
    "actor.id",
    "actor.login",
    "repo.id",
    "repo.name",
    "org.id",
    "org.login",
    "public",
    "created_at",
    "payload.action",
    "payload.ref",
    "payload.ref_type",
    "payload.before",
    "payload.head",
    "payload.push_id",
    "payload.issue.id",
    "payload.issue.number",
    "payload.pull_request.id",
    "payload.pull_request.number",
    "payload.pull_request.state",
    "payload.pull_request.merged",
    "payload.review.id",
    "payload.review.state",
    "payload.release.id",
    "payload.release.tag_name",
    "payload.release.prerelease",
    "payload.forkee.id",
    "payload.forkee.full_name",
)

PROFILE_COLUMNS = (
    "event_type",
    "field_path",
    "spark_type",
    "presence_count",
    "event_count",
    "presence_rate",
    "sample_value",
    "nullable",
    "notes",
)

_SCALAR_TYPES = {bool: "BOOLEAN", int: "BIGINT", float: "DOUBLE", str: "STRING"}
_SAMPLE_MAX_CHARS = 100
_CREATED_AT_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2})")


def _classify_scalar(value: Any) -> str:
    # bool is a subclass of int: test it first via exact type lookup.
    return _SCALAR_TYPES.get(type(value), "UNKNOWN")


def _array_type(value: list) -> str:
    if not value:
        return "ARRAY<EMPTY>"
    kinds = set()
    for elem in value:
        if isinstance(elem, dict):
            kinds.add("STRUCT")
        elif isinstance(elem, list):
            kinds.add("ARRAY")
        elif elem is None:
            kinds.add("NULL")
        else:
            kinds.add(_classify_scalar(elem))
    if len(kinds) == 1:
        return f"ARRAY<{kinds.pop()}>"
    return f"ARRAY<{'|'.join(sorted(kinds))}>"


def _truncate(text: str, limit: int = _SAMPLE_MAX_CHARS) -> str:
    return text if len(text) <= limit else text[: limit - 1] + "…"


class _PathObs:
    """Per-event observation of one field path."""

    __slots__ = ("types", "sample", "saw_null")

    def __init__(self) -> None:
        self.types: set[str] = set()
        self.sample: Any = None
        self.saw_null = False


def flatten_event(
    event: dict,
    max_depth: int = 8,
    max_array_elems: int = 20,
) -> dict[str, _PathObs]:
    """Map every field path in the event to its type/sample observation.

    Each path appears once per event regardless of how many array
    elements carry it, so aggregating observations counts *events*, not
    occurrences.
    """
    out: dict[str, _PathObs] = {}

    def note(path: str, type_name: str, sample: Any, is_null: bool) -> None:
        obs = out.get(path)
        if obs is None:
            obs = out[path] = _PathObs()
        obs.types.add(type_name)
        if obs.sample is None and sample is not None:
            obs.sample = sample
        obs.saw_null |= is_null

    def walk(obj: Any, prefix: str, depth: int) -> None:
        if isinstance(obj, dict):
            if prefix:
                note(prefix, "STRUCT", None, False)
            if depth >= max_depth:
                return
            for key, value in obj.items():
                child = f"{prefix}.{key}" if prefix else key
                walk(value, child, depth + 1)
        elif isinstance(obj, list):
            sample_scalars = [e for e in obj[:3] if not isinstance(e, (dict, list))]
            note(prefix, _array_type(obj), sample_scalars or None, False)
            if depth >= max_depth:
                return
            for elem in obj[:max_array_elems]:
                if isinstance(elem, dict):
                    walk(elem, prefix + "[]", depth + 1)
        elif obj is None:
            note(prefix, "NULL", None, True)
        else:
            note(prefix, _classify_scalar(obj), obj, False)

    walk(event, "", 0)
    return out


_DISTINCT_VALUES_CAP = 64  # per-field cap on distinct-value set size


class _FieldStats:
    __slots__ = (
        "presence_count", "null_events", "type_counter",
        "sample_value", "distinct_values",
    )

    def __init__(self) -> None:
        self.presence_count = 0
        self.null_events = 0
        self.type_counter: collections.Counter[str] = collections.Counter()
        self.sample_value: str | None = None
        # Bounded set for enum-drift detection. Only captures scalars
        # (strings/bools/small ints) — big samples fall through to
        # sample_value only.
        self.distinct_values: set = set()

    def update(self, obs: _PathObs) -> None:
        self.presence_count += 1
        if obs.saw_null:
            self.null_events += 1
        self.type_counter.update(obs.types)
        if self.sample_value is None and obs.sample is not None:
            try:
                rendered = json.dumps(obs.sample, ensure_ascii=False, default=str)
            except (TypeError, ValueError):
                rendered = repr(obs.sample)
            self.sample_value = _truncate(rendered)
        # Enum tracking: record scalar values up to a cap, so downstream
        # can diff distinct-value sets across dates (e.g. spot when
        # action='merged' or state_reason='not_planned' first appears).
        # Skip long strings — they're not enum-like.
        v = obs.sample
        if (
            v is not None
            and isinstance(v, (str, bool, int))
            and not (isinstance(v, str) and len(v) > 80)
            and len(self.distinct_values) < _DISTINCT_VALUES_CAP
        ):
            self.distinct_values.add(v)

    def spark_type(self) -> str:
        names = sorted(self.type_counter.keys() - {"NULL"})
        if not names:
            return "NULL"
        return "|".join(names)


class _CappedSetMap:
    """key -> small set of values, tracking collisions without unbounded memory."""

    __slots__ = ("data", "collisions", "cap")

    def __init__(self, cap: int = 3) -> None:
        self.data: dict[Any, set] = {}
        self.collisions = 0
        self.cap = cap

    def add(self, key: Any, value: Any) -> None:
        bucket = self.data.get(key)
        if bucket is None:
            self.data[key] = {value}
            return
        if value not in bucket:
            if len(bucket) == 1:
                self.collisions += 1  # key just became ambiguous
            if len(bucket) < self.cap:
                bucket.add(value)

    def stats(self) -> dict[str, int]:
        multi = sum(1 for bucket in self.data.values() if len(bucket) > 1)
        return {
            "distinct_keys": len(self.data),
            "keys_with_multiple_values": multi,
        }


class StreamProfiler:
    """Aggregates schema observations across one or more raw hour files."""

    def __init__(self, max_depth: int = 8, max_array_elems: int = 20) -> None:
        self.max_depth = max_depth
        self.max_array_elems = max_array_elems

        self.event_counts: collections.Counter[str] = collections.Counter()
        self.fields: dict[str, dict[str, _FieldStats]] = {}
        self.files: dict[str, dict[str, Any]] = {}
        self.malformed_lines = 0
        self.malformed_samples: list[dict[str, Any]] = []

        # Key / linkage diagnostics
        self.event_ids: set = set()
        self.duplicate_event_ids = 0
        self.missing_event_ids = 0
        self.push_ids: set = set()
        self.push_id_events = 0
        self.pr_key_to_id = _CappedSetMap()
        self.issue_key_to_id = _CappedSetMap()
        self.review_ids: set = set()
        self.release_ids: set = set()
        self.actions: collections.Counter[tuple[str, str]] = collections.Counter()

        # PushEvent commit availability
        self.push_events = 0
        self.push_with_commits_array = 0
        self.push_commit_elems = 0
        self.push_commit_elems_max = 0
        self.push_size_sum = 0
        self.push_with_size = 0
        self.push_size_gt_commits = 0

        # Event-time vs source-hour agreement
        self.hour_mismatches = 0
        self.hour_mismatch_samples: list[dict[str, Any]] = []

    # -- ingestion ---------------------------------------------------------

    def profile_file(
        self,
        path: str,
        source_hour: dt.datetime | None = None,
        max_events: int | None = None,
    ) -> None:
        """Stream one raw file (gzip or plain JSONL) into the profile."""
        name = os.path.basename(path)
        expected_prefix = source_hour.strftime("%Y-%m-%dT%H") if source_hour else None
        file_stats = {"events": 0, "malformed": 0, "min_created_at": None, "max_created_at": None}
        self.files[name] = file_stats

        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rb") as fh:
            for line_no, line in enumerate(fh, start=1):
                if max_events is not None and file_stats["events"] >= max_events:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        raise ValueError(f"top-level JSON is {type(event).__name__}, not object")
                except (ValueError, UnicodeDecodeError) as exc:
                    self.malformed_lines += 1
                    file_stats["malformed"] += 1
                    if len(self.malformed_samples) < 5:
                        self.malformed_samples.append(
                            {
                                "file": name,
                                "line": line_no,
                                "error": str(exc)[:200],
                                "snippet": line[:200].decode("utf-8", "replace"),
                            }
                        )
                    continue
                file_stats["events"] += 1
                self.add_event(event, expected_hour_prefix=expected_prefix, file_stats=file_stats, file_name=name)
        logger.info(
            "profiled %s: %d events, %d malformed", name, file_stats["events"], file_stats["malformed"]
        )

    def add_event(
        self,
        event: dict,
        expected_hour_prefix: str | None = None,
        file_stats: dict | None = None,
        file_name: str | None = None,
    ) -> None:
        event_type = event.get("type") or "UNKNOWN"
        self.event_counts[event_type] += 1
        per_type = self.fields.setdefault(event_type, {})
        for path, obs in flatten_event(event, self.max_depth, self.max_array_elems).items():
            stats = per_type.get(path)
            if stats is None:
                stats = per_type[path] = _FieldStats()
            stats.update(obs)
        self._track_keys(event, event_type)
        self._track_time(event, expected_hour_prefix, file_stats, file_name)

    # -- key/linkage diagnostics --------------------------------------------

    def _track_keys(self, event: dict, event_type: str) -> None:
        event_id = event.get("id")
        if event_id is None:
            self.missing_event_ids += 1
        elif event_id in self.event_ids:
            self.duplicate_event_ids += 1
        else:
            self.event_ids.add(event_id)

        payload = event.get("payload")
        if not isinstance(payload, dict):
            return
        repo = event.get("repo") or {}
        repo_id = repo.get("id") if isinstance(repo, dict) else None

        action = payload.get("action")
        if isinstance(action, str):
            self.actions[(event_type, action)] += 1

        if event_type == "PushEvent":
            self.push_events += 1
            push_id = payload.get("push_id")
            if push_id is not None:
                self.push_id_events += 1
                self.push_ids.add(push_id)
            commits = payload.get("commits")
            size = payload.get("size")
            if isinstance(size, int):
                self.push_with_size += 1
                self.push_size_sum += size
            if isinstance(commits, list):
                self.push_with_commits_array += 1
                n = len(commits)
                self.push_commit_elems += n
                self.push_commit_elems_max = max(self.push_commit_elems_max, n)
                if isinstance(size, int) and size > n:
                    self.push_size_gt_commits += 1

        pull_request = payload.get("pull_request")
        if isinstance(pull_request, dict) and repo_id is not None:
            number = pull_request.get("number")
            pr_id = pull_request.get("id")
            if number is not None and pr_id is not None:
                self.pr_key_to_id.add((repo_id, number), pr_id)

        issue = payload.get("issue")
        if isinstance(issue, dict) and repo_id is not None:
            number = issue.get("number")
            issue_id = issue.get("id")
            if number is not None and issue_id is not None:
                self.issue_key_to_id.add((repo_id, number), issue_id)

        review = payload.get("review")
        if isinstance(review, dict) and review.get("id") is not None:
            self.review_ids.add(review["id"])

        release = payload.get("release")
        if isinstance(release, dict) and release.get("id") is not None:
            self.release_ids.add(release["id"])

    def _track_time(
        self,
        event: dict,
        expected_hour_prefix: str | None,
        file_stats: dict | None,
        file_name: str | None,
    ) -> None:
        created_at = event.get("created_at")
        if not isinstance(created_at, str):
            return
        if file_stats is not None:
            if file_stats["min_created_at"] is None or created_at < file_stats["min_created_at"]:
                file_stats["min_created_at"] = created_at
            if file_stats["max_created_at"] is None or created_at > file_stats["max_created_at"]:
                file_stats["max_created_at"] = created_at
        if expected_hour_prefix is not None:
            match = _CREATED_AT_RE.match(created_at)
            if match and match.group(1) != expected_hour_prefix:
                self.hour_mismatches += 1
                if len(self.hour_mismatch_samples) < 3:
                    self.hour_mismatch_samples.append(
                        {"file": file_name, "created_at": created_at, "expected_hour": expected_hour_prefix}
                    )

    # -- outputs -------------------------------------------------------------

    def profile_rows(self) -> list[dict[str, Any]]:
        """Rows in the spec-required profile schema, sorted for determinism."""
        contract = set(CONTRACT_FIELDS)
        rows: list[dict[str, Any]] = []
        for event_type in sorted(self.fields):
            event_count = self.event_counts[event_type]
            for path in sorted(self.fields[event_type]):
                stats = self.fields[event_type][path]
                notes: list[str] = []
                if path in contract:
                    notes.append("contract field")
                non_null_types = stats.type_counter.keys() - {"NULL"}
                if len(non_null_types) > 1:
                    notes.append("inconsistent types")
                if stats.null_events:
                    notes.append(
                        f"explicit null in {stats.null_events}/{stats.presence_count} present events"
                    )
                rows.append(
                    {
                        "event_type": event_type,
                        "field_path": path,
                        "spark_type": stats.spark_type(),
                        "presence_count": stats.presence_count,
                        "event_count": event_count,
                        "presence_rate": round(stats.presence_count / event_count, 6),
                        "sample_value": stats.sample_value or "",
                        "nullable": stats.null_events > 0,
                        "notes": "; ".join(notes),
                    }
                )
        return rows

    def contract_report(self) -> dict[str, dict[str, Any]]:
        """Per contract field: where it appears and at what rate."""
        report: dict[str, dict[str, Any]] = {}
        for field in CONTRACT_FIELDS:
            per_type = {}
            for event_type, paths in self.fields.items():
                stats = paths.get(field)
                if stats is not None:
                    per_type[event_type] = {
                        "presence_count": stats.presence_count,
                        "event_count": self.event_counts[event_type],
                        "presence_rate": round(
                            stats.presence_count / self.event_counts[event_type], 6
                        ),
                        "spark_type": stats.spark_type(),
                        "null_events": stats.null_events,
                    }
            report[field] = {
                "present_in_event_types": dict(sorted(per_type.items())),
                "verdict": "ABSENT_IN_SAMPLE" if not per_type else "PRESENT",
            }
        return report

    def payload_top_level_fields(self) -> dict[str, dict[str, float]]:
        """Per event type: top-level payload.* fields and presence rates."""
        result: dict[str, dict[str, float]] = {}
        for event_type, paths in self.fields.items():
            entries = {}
            for path, stats in paths.items():
                if path.startswith("payload.") and path.count(".") == 1 and "[]" not in path:
                    entries[path] = round(
                        stats.presence_count / self.event_counts[event_type], 6
                    )
            result[event_type] = dict(sorted(entries.items()))
        return result

    def summary(self) -> dict[str, Any]:
        total_events = sum(self.event_counts.values())
        return {
            "total_events": total_events,
            "event_type_counts": dict(self.event_counts.most_common()),
            "files": self.files,
            "malformed_lines": self.malformed_lines,
            "malformed_samples": self.malformed_samples,
            "keys": {
                "event_id": {
                    "distinct": len(self.event_ids),
                    "duplicates": self.duplicate_event_ids,
                    "missing": self.missing_event_ids,
                },
                "push_id": {
                    "events_with_push_id": self.push_id_events,
                    "distinct": len(self.push_ids),
                    "push_events": self.push_events,
                },
                "repo_id_pr_number_to_pr_id": self.pr_key_to_id.stats(),
                "repo_id_issue_number_to_issue_id": self.issue_key_to_id.stats(),
                "review_id_distinct": len(self.review_ids),
                "release_id_distinct": len(self.release_ids),
            },
            "push_commit_stats": {
                "push_events": self.push_events,
                "with_commits_array": self.push_with_commits_array,
                "with_size_field": self.push_with_size,
                "total_commit_elements": self.push_commit_elems,
                "max_commit_elements": self.push_commit_elems_max,
                "total_size_reported": self.push_size_sum,
                "events_size_gt_commits_array": self.push_size_gt_commits,
            },
            "event_time_vs_source_hour": {
                "mismatches": self.hour_mismatches,
                "samples": self.hour_mismatch_samples,
            },
            "actions_by_event_type": {
                f"{etype}:{action}": count
                for (etype, action), count in sorted(self.actions.items())
            },
            "payload_top_level_fields": self.payload_top_level_fields(),
            "contract_fields": self.contract_report(),
        }


# -- artifact writers ---------------------------------------------------------


def write_profile_csv(rows: Iterable[dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(PROFILE_COLUMNS))
        writer.writeheader()
        writer.writerows(rows)


def write_summary_json(summary: dict[str, Any], path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(summary, fh, indent=2, ensure_ascii=False, default=str)
        fh.write("\n")


PROFILE_TABLE_DDL = (
    "event_type STRING, field_path STRING, spark_type STRING, "
    "presence_count BIGINT, event_count BIGINT, presence_rate DOUBLE, "
    "sample_value STRING, nullable BOOLEAN, notes STRING"
)


def write_profile_delta(
    rows: Sequence[dict[str, Any]],
    spark: Any,
    table: str = SCHEMA_PROFILE_TABLE,
    mode: str = "overwrite",
) -> None:
    """Persist profile rows to Unity Catalog (call from Databricks)."""
    data = [tuple(row[col] for col in PROFILE_COLUMNS) for row in rows]
    df = spark.createDataFrame(data, schema=PROFILE_TABLE_DDL)
    df.write.format("delta").mode(mode).option("overwriteSchema", "true").saveAsTable(table)


def infer_source_hour(path: str) -> dt.datetime | None:
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})-(\d{1,2})\.json(\.gz)?$", os.path.basename(path))
    if not m:
        return None
    year, month, day, hour = int(m[1]), int(m[2]), int(m[3]), int(m[4])
    return dt.datetime(year, month, day, hour, tzinfo=dt.timezone.utc)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Profile GH Archive raw files.")
    parser.add_argument("files", nargs="+", help="raw .json.gz (or .json) files")
    parser.add_argument("--out-dir", default="artifacts/schema_profile")
    parser.add_argument("--max-events", type=int, default=None, help="cap events per file (smoke runs)")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    profiler = StreamProfiler()
    for path in args.files:
        profiler.profile_file(path, source_hour=infer_source_hour(path), max_events=args.max_events)

    rows = profiler.profile_rows()
    summary = profiler.summary()
    csv_path = os.path.join(args.out_dir, "schema_profile.csv")
    json_path = os.path.join(args.out_dir, "profile_summary.json")
    write_profile_csv(rows, csv_path)
    write_summary_json(summary, json_path)

    print(f"events profiled:   {summary['total_events']:,}")
    print(f"event types:       {len(summary['event_type_counts'])}")
    print(f"profile rows:      {len(rows):,}")
    print(f"malformed lines:   {summary['malformed_lines']}")
    print(f"duplicate ids:     {summary['keys']['event_id']['duplicates']}")
    print(f"hour mismatches:   {summary['event_time_vs_source_hour']['mismatches']}")
    print(f"wrote {csv_path} and {json_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
