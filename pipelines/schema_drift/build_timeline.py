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




# An hour must carry at least this many events of a type before its
# fields are judged. Below it, a field's absence says nothing — rare
# types like DiscussionEvent simply may not occur in a sampled hour.
MIN_EVENTS_FOR_VERDICT = 30

# A field counts as "carried" by an event type when it appears on at
# least this share of that type's events. Optional fields (org, which
# only exists for org-owned repos) hover well below; structural fields
# sit near 100%.
CARRIED_THRESHOLD_PCT = 50.0

# A field must have been carried in at least this share of the judged
# hours before it vanished (or after it arrived) to count as a schema
# change rather than drift. Optional fields such as `org` — present only
# for org-owned repos — cross the carried threshold by chance as that
# population shifts, and without this filter they dominate the results.
STABILITY = 0.80


def field_presence_timeline(records: list[dict]) -> dict:
    """(event_type, path) → per-hour presence, judged only where the
    event type had enough volume for the answer to mean anything.

    Records both hours where the field was carried and hours where the
    type was well-sampled but the field was not, so a caller can tell
    "removed" from "not sampled".
    """
    carried: dict[tuple[str, str], list[str]] = collections.defaultdict(list)
    judged: dict[tuple[str, str], list[str]] = collections.defaultdict(list)

    # Every path ever seen for a type, so absence can be evaluated.
    paths_by_type: dict[str, set[str]] = collections.defaultdict(set)
    for r in records:
        for et, paths in r.get("fields", {}).items():
            paths_by_type[et].update(paths)

    for r in records:
        hour = r["hour"]
        counts = r.get("event_types", {})
        fields = r.get("fields", {})
        for et, known_paths in paths_by_type.items():
            if counts.get(et, 0) < MIN_EVENTS_FOR_VERDICT:
                continue  # too thin to judge
            seen_here = fields.get(et, {})
            for path in known_paths:
                judged[(et, path)].append(hour)
                stats = seen_here.get(path)
                if stats and stats.get("presence_pct", 0) >= CARRIED_THRESHOLD_PCT:
                    carried[(et, path)].append(hour)

    out = {}
    for key, judged_hours in judged.items():
        c = sorted(carried.get(key, []), key=_hour_sort_key)
        j = sorted(judged_hours, key=_hour_sort_key)
        out[key] = {
            "first": c[0] if c else None,
            "last": c[-1] if c else None,
            "count": len(c),
            "judged_first": j[0],
            "judged_last": j[-1],
            "judged_count": len(j),
            "hours": c,
            "judged_hours": j,
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
    A("## Fields that were removed")
    A("")
    A(f"A field counts as *carried* by an event type when it appears on "
      f"≥{CARRIED_THRESHOLD_PCT:.0f}% of that type's events in an hour, and an "
      f"hour is only judged when the type had ≥{MIN_EVENTS_FOR_VERDICT} events. "
      f"Both filters matter: without the volume floor a rare type like "
      f"DiscussionEvent looks removed whenever it misses a sample, and "
      f"without the rate threshold optional fields like `org` (present only "
      f"for org-owned repos) look intermittent.")
    A("")
    A("Removed = carried in ≥3 judged hours, then absent from every judged "
      "hour since. `Last carried` is the final sample in which it appeared; "
      "the true removal date lies between that and the next judged hour.")
    A("")
    A("| Event type | Field | First carried | Last carried | Hours carried |")
    A("| --- | --- | --- | --- | ---: |")
    gone = []
    for (et, path), info in presence.items():
        if info["count"] < 3 or not info["last"]:
            continue
        # Judged hours strictly after the last carried hour — if any exist
        # and none carried it, the field is genuinely gone.
        last_key = _hour_sort_key(info["last"])
        after = [h for h in info["judged_hours"] if _hour_sort_key(h) > last_key]
        if len(after) < 2:
            continue
        judged_before = [h for h in info["judged_hours"]
                         if _hour_sort_key(h) <= last_key]
        if not judged_before:
            continue
        if info["count"] / len(judged_before) < STABILITY:
            continue  # intermittent, not removed
        gone.append((et, path, info, len(after)))
    for et, path, info, _ in sorted(gone)[:80]:
        A(f"| `{et}` | `{path}` | {info['first']} | {info['last']} "
          f"| {info['count']} |")
    if not gone:
        A("| *(none)* | | | | |")
    if len(gone) > 80:
        A(f"| … | *{len(gone) - 80} more truncated* | | | |")
    A("")

    # ---- new fields ---------------------------------------------------------
    A("## Fields that appeared")
    A("")
    A("Absent from ≥2 judged hours at the start, then carried through to "
      "the most recent judged hour. The true introduction date lies between "
      "the last judged hour without it and `First carried`.")
    A("")
    A("| Event type | Field | First carried | Hours carried |")
    A("| --- | --- | --- | ---: |")
    new = []
    for (et, path), info in presence.items():
        if info["count"] < 3 or not info["first"]:
            continue
        first_key = _hour_sort_key(info["first"])
        before = [h for h in info["judged_hours"] if _hour_sort_key(h) < first_key]
        judged_after = [h for h in info["judged_hours"]
                        if _hour_sort_key(h) >= first_key]
        still_current = info["last"] == info["judged_last"]
        if len(before) < 2 or not still_current or not judged_after:
            continue
        if info["count"] / len(judged_after) < STABILITY:
            continue  # intermittent, not introduced
        new.append((et, path, info))
    for et, path, info in sorted(new)[:80]:
        A(f"| `{et}` | `{path}` | {info['first']} | {info['count']} |")
    if not new:
        A("| *(none)* | | | |")
    if len(new) > 80:
        A(f"| … | *{len(new) - 80} more truncated* | | |")
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
