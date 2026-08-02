#!/usr/bin/env python3
"""Turn per-hour schema profiles into a decade drift timeline.

Consumes the JSONL written by ``scan_decade.py`` and emits a markdown
report answering:

  * When did each (event_type, field_path) first appear and last appear?
  * Which fields vanished mid-decade (and when)?
  * Which enum values (action, state, state_reason, review.state)
    appeared or disappeared, and on which sample?
  * How did per-event-type volume shares shift?

Sampling caveat: one hour per month means a field present in <1 event
per ~100k could be missed in a given month. Treat "first seen" as
"first sampled", not "introduced on".

Usage:
    python build_timeline.py \
        --input artifacts/schema_drift/hourly_profiles.jsonl \
        --output docs/schema_drift_timeline.md
"""

from __future__ import annotations

import argparse
import collections
import json
import sys
from pathlib import Path

# Fields whose value sets are enum-like and worth diffing explicitly.
ENUM_FIELDS = [
    "payload.action",
    "payload.review.state",
    "payload.issue.state",
    "payload.issue.state_reason",
    "payload.ref_type",
    "payload.pull_request.state",
    "payload.pull_request.merged",
    "payload.release.prerelease",
    "payload.release.draft",
]

# Contract-relevant paths we always report presence for, even if stable.
WATCH_PATHS = [
    "payload.pull_request.merged",
    "payload.pull_request.merged_at",
    "payload.pull_request.state",
    "payload.pull_request.title",
    "payload.pull_request.body",
    "payload.pull_request.additions",
    "payload.pull_request.deletions",
    "payload.pull_request.changed_files",
    "payload.commits",
    "payload.size",
    "payload.distinct_size",
    "payload.issue.state_reason",
    "payload.release.assets",
    "payload.forkee.stargazers_count",
]


def load(path: Path) -> list[dict]:
    """Read JSONL, keep only ok rows, sort chronologically, dedupe by hour."""
    by_hour: dict[str, dict] = {}
    for line in path.open():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        if r.get("status") != "ok":
            continue
        by_hour[r["hour"]] = r  # later wins on re-run
    return [by_hour[h] for h in sorted(by_hour)]


def _hour_sort_key(hour: str) -> tuple:
    """'2016-03-15-12' → (2016, 3, 15, 12) for correct chronological order."""
    parts = hour.split("-")
    return tuple(int(p) for p in parts)


def field_presence_timeline(records: list[dict]) -> dict:
    """(event_type, path) → {first_hour, last_hour, hours_present, gaps}."""
    seen: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
    for r in records:
        for et, paths in r.get("fields", {}).items():
            for path in paths:
                seen[(et, path)].append(r["hour"])
    out = {}
    for key, hours in seen.items():
        hours_sorted = sorted(hours, key=_hour_sort_key)
        out[key] = {
            "first": hours_sorted[0],
            "last": hours_sorted[-1],
            "count": len(hours_sorted),
            "hours": hours_sorted,
        }
    return out


def enum_timeline(records: list[dict]) -> dict:
    """(event_type, path, value) → sorted list of hours where seen."""
    out: dict[tuple[str, str, str], list[str]] = collections.defaultdict(list)
    for r in records:
        for et, paths in r.get("fields", {}).items():
            for path, stats in paths.items():
                if path not in ENUM_FIELDS:
                    continue
                for v in stats.get("distinct_values", []):
                    out[(et, path, str(v))].append(r["hour"])
    return {k: sorted(v, key=_hour_sort_key) for k, v in out.items()}


def format_report(records: list[dict]) -> str:
    if not records:
        return "# Schema Drift Timeline\n\nNo profiled hours found.\n"

    all_hours = [r["hour"] for r in records]
    lines: list[str] = []
    A = lines.append

    A("# GH Archive Schema Drift Timeline")
    A("")
    A(f"**Generated from {len(records)} sampled hours** "
      f"({all_hours[0]} → {all_hours[-1]}), one hour per month.")
    A("")
    A("Produced by `pipelines/schema_drift/scan_decade.py` + "
      "`build_timeline.py`. Source: raw hourly files from "
      "data.gharchive.org (free); profiling via the same `StreamProfiler` "
      "that produced `docs/schema_validation_report.md`.")
    A("")
    A("**Sampling caveat**: one hour per month. A field present in fewer "
      "than ~1 in 100k events may be missed in a given month, so \"first "
      "seen\" means \"first sampled\", not \"introduced on\". Boundary "
      "dates for the PR merge signal were pinned exactly by binary search "
      "— see `docs/open_questions.md` OQ-1.")
    A("")

    # ---- volume + event type mix ------------------------------------------
    A("## Event volume and type mix")
    A("")
    A("| Sample hour | Events | Types | Top type | Top share |")
    A("| --- | ---: | ---: | --- | ---: |")
    for r in records:
        counts = r.get("event_types", {})
        total = r.get("events_profiled", 0)
        if not counts:
            continue
        top_type, top_n = max(counts.items(), key=lambda kv: kv[1])
        share = 100.0 * top_n / total if total else 0
        A(f"| {r['hour']} | {total:,} | {len(counts)} | {top_type} | {share:.1f}% |")
    A("")

    # ---- event types appearing / disappearing ------------------------------
    A("## Event types: first and last sampled")
    A("")
    type_hours: dict[str, list[str]] = collections.defaultdict(list)
    for r in records:
        for et in r.get("event_types", {}):
            type_hours[et].append(r["hour"])
    A("| Event type | First sampled | Last sampled | Hours seen |")
    A("| --- | --- | --- | ---: |")
    for et in sorted(type_hours):
        hs = sorted(type_hours[et], key=_hour_sort_key)
        A(f"| `{et}` | {hs[0]} | {hs[-1]} | {len(hs)}/{len(records)} |")
    A("")

    # ---- watched contract fields -------------------------------------------
    presence = field_presence_timeline(records)
    A("## Contract-relevant field availability")
    A("")
    A("Fields the pipeline depends on (or once depended on). "
      "`Hours seen` counts sampled hours where the field appeared at least once.")
    A("")
    A("| Event type | Field | First | Last | Hours seen |")
    A("| --- | --- | --- | --- | ---: |")
    watch_rows = [
        (et, path, info)
        for (et, path), info in presence.items()
        if path in WATCH_PATHS
    ]
    for et, path, info in sorted(watch_rows):
        A(f"| `{et}` | `{path}` | {info['first']} | {info['last']} "
          f"| {info['count']}/{len(records)} |")
    A("")

    # ---- disappeared fields -------------------------------------------------
    last_hour = all_hours[-1]
    first_hour = all_hours[0]
    A("## Fields that disappeared")
    A("")
    A("Present in an early sample, absent from the most recent sample. "
      "Sorted by event type then path. Only shows fields seen in ≥3 hours "
      "(filters sampling noise).")
    A("")
    A("| Event type | Field | First | Last seen | Hours seen |")
    A("| --- | --- | --- | --- | ---: |")
    gone = [
        (et, path, info) for (et, path), info in presence.items()
        if info["last"] != last_hour and info["count"] >= 3
    ]
    for et, path, info in sorted(gone)[:120]:
        A(f"| `{et}` | `{path}` | {info['first']} | {info['last']} "
          f"| {info['count']} |")
    if len(gone) > 120:
        A(f"| … | *{len(gone) - 120} more rows truncated* | | | |")
    A("")

    # ---- new fields ---------------------------------------------------------
    A("## Fields that appeared mid-decade")
    A("")
    A("Absent from the earliest sample, present in the most recent. "
      "Only shows fields seen in ≥3 hours.")
    A("")
    A("| Event type | Field | First seen | Hours seen |")
    A("| --- | --- | --- | ---: |")
    new = [
        (et, path, info) for (et, path), info in presence.items()
        if info["first"] != first_hour and info["last"] == last_hour
        and info["count"] >= 3
    ]
    for et, path, info in sorted(new)[:120]:
        A(f"| `{et}` | `{path}` | {info['first']} | {info['count']} |")
    if len(new) > 120:
        A(f"| … | *{len(new) - 120} more rows truncated* | | |")
    A("")

    # ---- enum drift ---------------------------------------------------------
    A("## Enum value drift")
    A("")
    A("Distinct values observed for enum-like fields, with the sampled "
      "window in which each value appeared. This is where action-value "
      "changes (e.g. `merged`) and closure-reason additions show up.")
    A("")
    enums = enum_timeline(records)
    by_field: dict[tuple[str, str], list[tuple[str, list[str]]]] = collections.defaultdict(list)
    for (et, path, value), hours in enums.items():
        by_field[(et, path)].append((value, hours))
    for (et, path) in sorted(by_field):
        values = sorted(by_field[(et, path)])
        A(f"### `{et}` → `{path}`")
        A("")
        A("| Value | First | Last | Hours seen |")
        A("| --- | --- | --- | ---: |")
        for value, hours in values:
            A(f"| `{value}` | {hours[0]} | {hours[-1]} | {len(hours)} |")
        A("")

    return "\n".join(lines) + "\n"


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--input", required=True, help="JSONL from scan_decade.py")
    p.add_argument("--output", required=True, help="markdown report path")
    args = p.parse_args(argv)

    records = load(Path(args.input))
    report = format_report(records)
    out = Path(args.output)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(report)
    print(f"wrote {out} ({len(report):,} chars) from {len(records)} sampled hours")
    return 0


if __name__ == "__main__":
    sys.exit(main())
