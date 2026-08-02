#!/usr/bin/env python3
"""Sample-profile one hour per month across a date range and write per-hour
schema summaries — the input for the drift timeline.

Uses the existing GH Archive downloader and StreamProfiler, so schema
observations here match the local schema profile we already validated.
Zero-cost (downloads are free from data.gharchive.org). Roughly
20 MB × N hours to download; per-hour profile takes ~30-120s.

Usage:

    python scan_decade.py --years 2016-2025 --dest <scratch_dir> \
        --output artifacts/schema_drift/hourly_profiles.jsonl

Downloads to ``--dest`` (default: scratchpad). Deletes downloaded files
after successful profiling unless ``--keep-downloads`` is set. Skips
hours already present in ``--output`` so re-runs resume cheaply.

Companion: ``build_timeline.py`` consumes the JSONL and emits a
human-readable drift map.
"""

from __future__ import annotations

import argparse
import collections
import datetime as dt
import json
import os
import sys
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from github_observatory.ingestion.download_gharchive import download_hour  # noqa: E402
from github_observatory.schema.profile_schema import StreamProfiler, infer_source_hour  # noqa: E402


# Cap the number of distinct values we record per field. Sufficient to
# spot enum drift (action, state, review.state, state_reason) without
# blowing up JSON size for high-cardinality fields like URLs / SHAs.
MAX_VALUES_PER_FIELD = 40
MAX_VALUE_SAMPLE_LEN = 60


def sample_hours(years: list[int], day_of_month: int, hour: int) -> list[str]:
    """One hour per month for each year. Format: 'YYYY-MM-DD-H' (unpadded hour)."""
    now = dt.datetime.now(dt.UTC)
    out: list[str] = []
    for y in years:
        for m in range(1, 13):
            if y == now.year and m > now.month:
                continue
            out.append(f"{y:04d}-{m:02d}-{day_of_month:02d}-{hour}")
    return out


def profile_hour(hour_spec: str, raw_dir: str) -> dict:
    """Download + profile one hour. Returns a compact per-hour summary."""
    result = download_hour(hour_spec, raw_dir)
    if result.status == "failed":
        return {"hour": hour_spec, "status": "download_failed",
                "error": result.error_message}

    path = os.path.join(raw_dir, result.source_file)
    profiler = StreamProfiler()
    try:
        profiler.profile_file(path, source_hour=infer_source_hour(path))
    except Exception as exc:  # noqa: BLE001 — record any profiling issue
        return {"hour": hour_spec, "status": "profile_failed",
                "error": str(exc)[:500], "trace": traceback.format_exc()[-500:]}

    summary = _extract_summary(hour_spec, profiler)
    return summary


def _extract_summary(hour_spec: str, profiler: StreamProfiler) -> dict:
    """Reduce StreamProfiler state to the fields relevant to drift analysis."""
    event_counts = dict(profiler.event_counts)
    fields_summary: dict[str, dict[str, dict]] = {}
    for event_type, path_map in profiler.fields.items():
        et_total = event_counts.get(event_type, 0)
        fields_summary[event_type] = {}
        for path, stats in path_map.items():
            distinct_values: list = []
            # StreamProfiler caps values via _PathObs / _FieldStats; use its
            # captured samples if available, else empty.
            if hasattr(stats, "distinct_values"):
                for v in list(stats.distinct_values)[:MAX_VALUES_PER_FIELD]:
                    s = str(v)
                    if len(s) > MAX_VALUE_SAMPLE_LEN:
                        s = s[:MAX_VALUE_SAMPLE_LEN] + "…"
                    distinct_values.append(s)
            fields_summary[event_type][path] = {
                "presence_count": stats.presence_count,
                "presence_pct": round(
                    100.0 * stats.presence_count / et_total, 2
                ) if et_total else 0.0,
                "types": sorted(stats.type_counter.keys()),
                "null_events": stats.null_events,
                "distinct_values": distinct_values,
            }
    return {
        "hour": hour_spec,
        "status": "ok",
        "events_profiled": sum(event_counts.values()),
        "event_types": event_counts,
        "fields": fields_summary,
    }


def load_existing(output_path: Path) -> set[str]:
    """Which hour specs already have a written summary?"""
    if not output_path.exists():
        return set()
    seen = set()
    for line in output_path.open():
        try:
            r = json.loads(line)
        except json.JSONDecodeError:
            continue
        h = r.get("hour")
        if h and r.get("status") == "ok":
            seen.add(h)
    return seen


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--years", default="2016-2025",
                   help="e.g. 2016-2025 or 2019 or 2019,2020,2021")
    p.add_argument("--day", type=int, default=15, help="day of month to sample (default 15)")
    p.add_argument("--hour", type=int, default=12, help="hour of day UTC (default 12)")
    p.add_argument("--dest", default=None, help="local dir for raw downloads")
    p.add_argument("--output", required=True, help="JSONL to append per-hour summaries to")
    p.add_argument("--keep-downloads", action="store_true",
                   help="don't delete raw files after profiling")
    p.add_argument("--parallel", type=int, default=4,
                   help="concurrent downloads+profiles (default 4)")
    args = p.parse_args(argv)

    # Parse --years
    if "-" in args.years:
        start, end = args.years.split("-")
        years = list(range(int(start), int(end) + 1))
    else:
        years = [int(y) for y in args.years.split(",")]

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    already = load_existing(output_path)
    print(f"resuming: {len(already)} hours already profiled in {output_path}")

    raw_dir = args.dest or f"/tmp/schema_drift_raw_{os.getpid()}"
    os.makedirs(raw_dir, exist_ok=True)

    todo = [h for h in sample_hours(years, args.day, args.hour) if h not in already]
    print(f"planned: {len(todo)} hours ({args.parallel} parallel)")
    if not todo:
        print("nothing to do")
        return 0

    ok = fail = 0
    with output_path.open("a") as out_fh, \
         ThreadPoolExecutor(max_workers=args.parallel) as pool:
        futures = {pool.submit(profile_hour, h, raw_dir): h for h in todo}
        for i, fut in enumerate(as_completed(futures), 1):
            h = futures[fut]
            try:
                summary = fut.result()
            except Exception as exc:  # noqa: BLE001
                summary = {"hour": h, "status": "raised",
                           "error": str(exc)[:500]}
            out_fh.write(json.dumps(summary) + "\n")
            out_fh.flush()
            status = summary.get("status")
            if status == "ok":
                ok += 1
                print(f"[{i}/{len(todo)}] {h} ok "
                      f"(events={summary['events_profiled']:,})", flush=True)
            else:
                fail += 1
                print(f"[{i}/{len(todo)}] {h} {status}: "
                      f"{summary.get('error','')[:100]}", flush=True)

            if not args.keep_downloads:
                path = os.path.join(raw_dir, f"{h}.json.gz")
                if os.path.exists(path):
                    os.unlink(path)

    print(f"\ndone: {ok} ok, {fail} failed → {output_path}")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
