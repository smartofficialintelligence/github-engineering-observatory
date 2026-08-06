#!/usr/bin/env python3
"""Export decade hourly census aggregates from the BigQuery public
GH Archive dataset (pass 1 of the deep-history import).

Runs one query per year against ``githubarchive.year.YYYY``, computing
the census-column subset of ``gold.ecosystem_hourly`` (everything that
does not require the payload column — see docs/metric_definitions.md
v3). All row counts use COUNT(DISTINCT id): the public dataset contains
duplicate events (verified 2026-07-30: hour 2026-07-27T20 had 168,265
rows / 168,233 distinct ids) and dedupe-on-id is the pipeline-wide rule.

Writes per-year CSV artifacts plus a manifest with job ids, query
sha256, and bytes billed to ``artifacts/bq_import/``. The artifacts are
the raw material for ``bronze.bq_ecosystem_hourly`` — commit them.

Requires the ``bq`` CLI authenticated with a billing project:

    python scripts/bq_export_hourly.py --project sematryx-481510 \
        --years 2016-2025
"""

from __future__ import annotations

import argparse
import datetime as dt
import hashlib
import json
import os
import subprocess
import sys

OUT_DIR = os.path.join(os.path.dirname(__file__), "..", "artifacts", "bq_import")

QUERY_TEMPLATE = """\
SELECT FORMAT_TIMESTAMP('%Y-%m-%dT%H:00:00Z', TIMESTAMP_TRUNC(created_at, HOUR)) AS event_hour,
       COUNT(DISTINCT id) AS total_events,
       COUNT(DISTINCT IF(NOT IFNULL(ENDS_WITH(actor.login, '[bot]'), FALSE), id, NULL)) AS total_events_human,
       COUNT(DISTINCT IF(type = 'PushEvent', id, NULL)) AS push_events,
       COUNT(DISTINCT IF(type = 'PushEvent'
             AND NOT IFNULL(ENDS_WITH(actor.login, '[bot]'), FALSE), id, NULL)) AS push_events_human,
       COUNT(DISTINCT IF(IFNULL(ENDS_WITH(actor.login, '[bot]'), FALSE), id, NULL)) AS bot_events,
       COUNT(DISTINCT actor.id) AS distinct_actors,
       COUNT(DISTINCT IF(NOT IFNULL(ENDS_WITH(actor.login, '[bot]'), FALSE), actor.id, NULL)) AS distinct_actors_human,
       COUNT(DISTINCT repo.id) AS distinct_repos,
       COUNT(DISTINCT IF(type = 'PushEvent', repo.id, NULL)) AS distinct_push_repos
FROM `githubarchive.year.{year}`
WHERE public AND repo.id IS NOT NULL AND created_at IS NOT NULL
GROUP BY 1
ORDER BY 1
"""


def run_period(period: str, project: str) -> dict:
    """Export one period. ``period`` is 'YYYY' (year table) or 'YYYYMM'
    (month table).

    Month tables matter for the current year: githubarchive.year.YYYY is
    only built once a year completes, so a partial year is reachable only
    through githubarchive.month.YYYYMM. Census columns are cheap either
    way — the whole of 2026 to date dry-runs at 50 GB, about $0.28,
    because the query never touches the payload column.
    """
    is_month = len(period) == 6
    table = (f"githubarchive.month.{period}" if is_month
             else f"githubarchive.year.{period}")
    year = int(period[:4])
    query = QUERY_TEMPLATE.replace(
        "`githubarchive.year.{year}`", f"`{table}`").format(year=year)
    query_sha = hashlib.sha256(query.encode()).hexdigest()
    job_id = f"observatory_hourly_{period}_{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}"
    out_path = os.path.join(OUT_DIR, f"ecosystem_hourly_{period}.csv")

    print(f"[{period}] running job {job_id} ...", flush=True)
    result = subprocess.run(
        ["bq", f"--project_id={project}", "--job_id", job_id,
         "query", "--use_legacy_sql=false", "--format=csv",
         "--max_rows=20000", query],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{period} failed: {result.stderr[-800:]}")

    lines = [l for l in result.stdout.splitlines()
             if l.startswith(("event_hour", "20", "19"))]
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    show = subprocess.run(
        ["bq", f"--project_id={project}", "show", "--format=json", "-j", job_id],
        capture_output=True, text=True,
    )
    stats = json.loads(show.stdout).get("statistics", {}) if show.returncode == 0 else {}
    bytes_billed = int(stats.get("query", {}).get("totalBytesBilled", 0))
    row_count = len(lines) - 1
    print(f"[{period}] {row_count} hourly rows, {bytes_billed/1e9:.1f} GB billed "
          f"-> {out_path}", flush=True)
    return {
        "year": year, "period": period, "job_id": job_id,
        "query_sha256": query_sha, "rows": row_count,
        "bytes_billed": bytes_billed,
        "artifact": os.path.basename(out_path),
        "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "source_table": table,
    }


def run_year(year: int, project: str) -> dict:
    query = QUERY_TEMPLATE.format(year=year)
    query_sha = hashlib.sha256(query.encode()).hexdigest()
    job_id = f"observatory_hourly_{year}_{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}"
    out_path = os.path.join(OUT_DIR, f"ecosystem_hourly_{year}.csv")

    print(f"[{year}] running job {job_id} ...", flush=True)
    result = subprocess.run(
        [
            "bq", f"--project_id={project}", "--job_id", job_id,
            "query", "--use_legacy_sql=false", "--format=csv",
            "--max_rows=20000", query,
        ],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"{year} failed: {result.stderr[-800:]}")

    # Strip bq's status-line noise; keep header + data rows.
    lines = [l for l in result.stdout.splitlines() if l.startswith(("event_hour", "20", "19"))]
    with open(out_path, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    show = subprocess.run(
        ["bq", f"--project_id={project}", "show", "--format=json", "-j", job_id],
        capture_output=True, text=True,
    )
    stats = json.loads(show.stdout).get("statistics", {}) if show.returncode == 0 else {}
    bytes_billed = int(stats.get("query", {}).get("totalBytesBilled", 0))
    row_count = len(lines) - 1
    print(f"[{year}] {row_count} hourly rows, {bytes_billed/1e9:.0f} GB billed -> {out_path}", flush=True)
    return {
        "year": year, "job_id": job_id, "query_sha256": query_sha,
        "rows": row_count, "bytes_billed": bytes_billed,
        "artifact": os.path.basename(out_path),
        "exported_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "source_table": f"githubarchive.year.{year}",
    }


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--project", required=True, help="GCP billing project id")
    parser.add_argument("--years", default="2020-2025", help="e.g. 2020-2025 or 2019")
    parser.add_argument("--months", default=None,
                        help="month periods instead of years, e.g. "
                             "202601-202607. Needed for the current year, "
                             "whose year table is not built until it ends.")
    args = parser.parse_args(argv)

    if args.months:
        if "-" in args.months:
            a, b = args.months.split("-")
            periods, cur = [], int(a)
            while cur <= int(b):
                periods.append(f"{cur}")
                y, m = divmod(cur, 100)
                cur = (y + 1) * 100 + 1 if m == 12 else cur + 1
        else:
            periods = args.months.split(",")
    elif "-" in args.years:
        start, end = args.years.split("-")
        periods = [str(y) for y in range(int(start), int(end) + 1)]
    else:
        periods = [args.years]

    os.makedirs(OUT_DIR, exist_ok=True)
    manifest_path = os.path.join(OUT_DIR, "manifest.json")
    manifest = []
    if os.path.exists(manifest_path):
        with open(manifest_path, encoding="utf-8") as fh:
            manifest = json.load(fh)

    for period in periods:
        entry = run_period(period, args.project)
        manifest = [m for m in manifest
                    if m.get("period", str(m["year"])) != period] + [entry]
        manifest.sort(key=lambda m: m.get("period", str(m["year"])))
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(manifest, fh, indent=2)

    total_gb = sum(m["bytes_billed"] for m in manifest) / 1e9
    total_rows = sum(m["rows"] for m in manifest)
    print(f"\nmanifest: {len(manifest)} years, {total_rows:,} hourly rows, "
          f"{total_gb:.0f} GB billed total")
    return 0


if __name__ == "__main__":
    sys.exit(main())
