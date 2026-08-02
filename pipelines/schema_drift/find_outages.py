#!/usr/bin/env python3
"""Find GH Archive collection outages across the whole decade — free.

An outage is a period where the *collector* failed, so events that
happened were never captured. This is categorically different from the
OQ-1 filtering (where GitHub stopped emitting certain events) and from
genuine low activity (weekends, Christmas). Counts inside an outage are
floors, not measurements, and any rate computed across one is wrong.

Method: ``githubarchive.day.__TABLES__`` exposes a row count per day
table and metadata queries are free, so the entire decade is one query.
Each day is compared against the median of the **same weekday** 4-9
weeks away in both directions:

  * same-weekday removes the ~40% weekend cycle, which otherwise floods
    the results with false positives (consecutive Saturdays look like a
    recurring outage);
  * 4-9 weeks away keeps a multi-week outage from depressing its own
    baseline — the October 2021 incident ran 20 days and is invisible to
    a +/-15-day local median for exactly that reason.

Days below ``--threshold`` of that baseline, or missing a table
entirely, are grouped into incidents allowing up to a 3-day healthy gap.

Usage:
    python find_outages.py --project <gcp-project> \
        --output artifacts/schema_drift/outages.json
"""

from __future__ import annotations

import argparse
import csv
import datetime as dt
import io
import json
import statistics
import subprocess
import sys
from pathlib import Path

QUERY = (
    "SELECT table_id, row_count "
    "FROM `githubarchive.day.__TABLES__` ORDER BY table_id"
)


def fetch_row_counts(project: str, cache: Path | None) -> dict[dt.date, int]:
    """Day -> row count. Metadata only, so this costs nothing."""
    if cache and cache.exists():
        text = cache.read_text()
    else:
        result = subprocess.run(
            ["bq", f"--project_id={project}", "query",
             "--use_legacy_sql=false", "--format=csv", "--max_rows=10000",
             QUERY],
            capture_output=True, text=True, check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(f"bq failed: {result.stderr[-500:]}")
        text = result.stdout
        if cache:
            cache.parent.mkdir(parents=True, exist_ok=True)
            cache.write_text(text)

    rows: dict[dt.date, int] = {}
    for rec in csv.DictReader(io.StringIO(text)):
        tid = (rec.get("table_id") or "").strip()
        if len(tid) != 8 or not tid.isdigit():
            continue  # skip non-day tables such as "yesterday"
        try:
            day = dt.date(int(tid[:4]), int(tid[4:6]), int(tid[6:]))
        except ValueError:
            continue
        rows[day] = int(rec["row_count"])
    return rows


def weekday_baseline(rows: dict[dt.date, int], day: dt.date,
                     lo_weeks: int, hi_weeks: int) -> float | None:
    """Median of the same weekday, lo_weeks..hi_weeks away both sides."""
    vals = [
        v for wk in range(lo_weeks, hi_weeks + 1)
        for sign in (-1, 1)
        if (v := rows.get(day + dt.timedelta(weeks=sign * wk))) is not None
    ]
    return statistics.median(vals) if vals else None


def find_incidents(rows: dict[dt.date, int], threshold: float,
                   lo_weeks: int, hi_weeks: int,
                   join_gap_days: int) -> list[dict]:
    days = sorted(rows)
    if not days:
        return []
    flagged: list[tuple[dt.date, int | None, int]] = []
    for i in range((days[-1] - days[0]).days + 1):
        day = days[0] + dt.timedelta(days=i)
        base = weekday_baseline(rows, day, lo_weeks, hi_weeks)
        if not base:
            continue
        count = rows.get(day)
        if count is None or count < threshold * base:
            flagged.append((day, count, int(base)))

    incidents: list[dict] = []
    group: list[tuple[dt.date, int | None, int]] = []
    for rec in flagged:
        if group and (rec[0] - group[-1][0]).days <= join_gap_days:
            group.append(rec)
        else:
            if group:
                incidents.append(_summarize(group))
            group = [rec]
    if group:
        incidents.append(_summarize(group))
    return incidents


def _summarize(group: list[tuple[dt.date, int | None, int]]) -> dict:
    start, end = group[0][0], group[-1][0]
    present = [c for _, c, _ in group if c is not None]
    baseline = group[0][2]
    worst = min(present) if present else 0
    # The 2011-2015 archive is missing every 31 December table; that is a
    # known artifact of the source, not a collection failure.
    year_end_quirk = (
        len(group) == 1 and start.month == 12 and start.day == 31
        and present == []
    )
    return {
        "start": start.isoformat(),
        "end": end.isoformat(),
        "days_flagged": len(group),
        "span_days": (end - start).days + 1,
        "missing_tables": sum(1 for _, c, _ in group if c is None),
        "worst_row_count": worst,
        "baseline_row_count": baseline,
        "worst_pct_of_baseline": round(100.0 * worst / baseline, 2) if baseline else None,
        "classification": "year_end_missing_table" if year_end_quirk else "outage",
    }


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--project", required=True, help="GCP project for bq")
    p.add_argument("--output", help="write incidents as JSON here")
    p.add_argument("--cache", default="artifacts/schema_drift/day_row_counts.csv",
                   help="cache the free metadata query here")
    p.add_argument("--threshold", type=float, default=0.40,
                   help="flag days below this fraction of the same-weekday "
                        "baseline (default 0.40, i.e. a >60%% drop)")
    p.add_argument("--baseline-weeks", default="4-9",
                   help="weeks away used for the baseline (default 4-9)")
    p.add_argument("--join-gap-days", type=int, default=3,
                   help="healthy days tolerated inside one incident")
    args = p.parse_args(argv)

    lo_weeks, hi_weeks = (int(x) for x in args.baseline_weeks.split("-"))
    rows = fetch_row_counts(args.project, Path(args.cache) if args.cache else None)
    print(f"{len(rows):,} day tables, "
          f"{min(rows)} → {max(rows)}", flush=True)

    incidents = find_incidents(rows, args.threshold, lo_weeks, hi_weeks,
                               args.join_gap_days)
    outages = [i for i in incidents if i["classification"] == "outage"]
    quirks = [i for i in incidents if i["classification"] != "outage"]

    print(f"\n{len(outages)} collection outages "
          f"({len(quirks)} year-end missing-table quirks excluded)\n")
    header = f"{'window':26s} {'days':>5s} {'miss':>5s} {'worst':>10s} {'expected':>11s} {'worst%':>8s}"
    print(header)
    for i in outages:
        window = f"{i['start']} .. {i['end']}"
        print(f"{window:26s} {i['days_flagged']:>5d} {i['missing_tables']:>5d} "
              f"{i['worst_row_count']:>10,} {i['baseline_row_count']:>11,} "
              f"{i['worst_pct_of_baseline']:>7.1f}%")

    total_days = sum(i["days_flagged"] for i in outages)
    print(f"\n{total_days} days affected by collection outages across the decade")

    if args.output:
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(
            {"outages": outages, "year_end_quirks": quirks,
             "params": {"threshold": args.threshold,
                        "baseline_weeks": args.baseline_weeks,
                        "join_gap_days": args.join_gap_days}},
            indent=2))
        print(f"wrote {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
