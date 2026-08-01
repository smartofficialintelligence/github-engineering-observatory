#!/usr/bin/env python3
"""Validate the Spark bench output against equivalent BigQuery SQL.

Runs the 34-column aggregation in BigQuery SQL for one month (using the same
block-out and era-aware derivations as the Spark job), fetches the Spark
output from GCS, and diffs the two hour-by-hour. Zero diff is the acceptance
criterion for the pipeline (spec §Validation contract, check #6).

Uses ``bq`` and ``gcloud storage`` CLIs (matches ``scripts/bq_export_hourly.py``).
Requires ``bq`` auth + a billing project.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import tempfile
from pathlib import Path

# --- SQL: BigQuery translation of pipelines/spark_job.py aggregate ---------
# Kept in lock-step with spark_job.py. Notable differences:
#   * TIMESTAMP_TRUNC(x, HOUR)     vs Spark  date_trunc('HOUR', x)
#   * COUNT(DISTINCT IF(c, id, NULL)) vs Spark COUNT(DISTINCT CASE WHEN c THEN id END)
#   * ENDS_WITH(x, y)              vs Spark  endswith(x, y)
#   * (SELECT SUM(...) FROM UNNEST(...)) vs Spark aggregate() over array

PR_MERGED = (
    "(JSON_EXTRACT_SCALAR(payload, '$.action') = 'merged'"
    " OR (JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'"
    "     AND JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged') = 'true'))"
)
PR_CLOSED_NM = (
    "(JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'"
    " AND IFNULL(JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged'), 'false') != 'true')"
)
PRODUCTION = (
    "(type = 'PushEvent'"
    " OR (type = 'PullRequestEvent'"
    "     AND (JSON_EXTRACT_SCALAR(payload, '$.action') IN ('opened', 'reopened')"
    f"         OR {PR_MERGED} OR {PR_CLOSED_NM}))"
    " OR (type = 'IssuesEvent'"
    "     AND JSON_EXTRACT_SCALAR(payload, '$.action') IN ('opened', 'closed', 'reopened'))"
    " OR (type = 'ReleaseEvent'"
    "     AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'published'))"
)
BOT = "IFNULL(ENDS_WITH(actor.login, '[bot]'), FALSE)"

RELEASE_DL = (
    "IFNULL((SELECT SUM(IFNULL(CAST(JSON_EXTRACT_SCALAR(a, '$.download_count') AS INT64), 0))"
    " FROM UNNEST(JSON_EXTRACT_ARRAY(payload, '$.release.assets')) a), 0)"
)


def _cd(cond: str) -> str:
    return f"COUNT(DISTINCT IF({cond}, id, NULL))"


def _cs(cond: str, expr: str) -> str:
    return f"SUM(IF({cond}, {expr}, 0))"


def build_bq_query(year: int, month: int) -> str:
    """Aggregate SQL for one month of githubarchive.year.YYYY."""
    return f"""
SELECT
    FORMAT_TIMESTAMP('%Y-%m-%dT%H:00:00Z',
                     TIMESTAMP_TRUNC(created_at, HOUR)) AS event_hour,
    COUNT(DISTINCT id) AS total_events,
    {_cd(f"NOT {BOT}")} AS total_events_human,
    {_cd("type = 'PushEvent'")} AS push_events,
    {_cd(f"type = 'PushEvent' AND NOT {BOT}")} AS push_events_human,
    {_cd(BOT)} AS bot_events,
    COUNT(DISTINCT actor.id) AS distinct_actors,
    COUNT(DISTINCT IF(NOT {BOT}, actor.id, NULL)) AS distinct_actors_human,
    COUNT(DISTINCT repo.id) AS distinct_repos,
    COUNT(DISTINCT IF(type = 'PushEvent', repo.id, NULL)) AS distinct_push_repos,
    {_cd(PRODUCTION)} AS production_events,
    {_cd("type = 'PullRequestEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'opened'")} AS pr_opened,
    {_cd(f"type = 'PullRequestEvent' AND {PR_MERGED}")} AS pr_merged,
    {_cd(f"type = 'PullRequestEvent' AND {PR_CLOSED_NM}")} AS pr_closed_no_merge,
    {_cd("type = 'PullRequestEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'reopened'")} AS pr_reopened,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'opened'")} AS issues_opened,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'")} AS issues_closed,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'reopened'")} AS issues_reopened,
    {_cd("type = 'ReleaseEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'published'")} AS releases_published,
    {_cd("type = 'PullRequestEvent'")} AS pr_events_total,
    {_cd("type = 'IssuesEvent'")} AS issues_events_total,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.issue.state_reason') = 'completed'")} AS issues_closed_completed,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.issue.state_reason') = 'not_planned'")} AS issues_closed_not_planned,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.issue.state_reason') = 'duplicate'")} AS issues_closed_duplicate,
    {_cd("type = 'IssuesEvent' AND JSON_EXTRACT_SCALAR(payload, '$.action') = 'closed'"
         " AND (JSON_EXTRACT_SCALAR(payload, '$.issue.state_reason') IS NULL"
         " OR JSON_EXTRACT_SCALAR(payload, '$.issue.state_reason')"
         " NOT IN ('completed', 'not_planned', 'duplicate'))")} AS issues_closed_unknown,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.review.state') = 'approved'")} AS reviews_approved,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.review.state') = 'changes_requested'")} AS reviews_changes_requested,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.review.state') = 'commented'")} AS reviews_commented,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND JSON_EXTRACT_SCALAR(payload, '$.review.state') = 'dismissed'")} AS reviews_dismissed,
    -- issue.pull_request is an OBJECT, so JSON_EXTRACT_SCALAR always returns
    -- NULL (only scalars come back non-null). Use JSON_EXTRACT for
    -- object-existence checks — this is what Spark's get_json_object does.
    {_cd("type = 'IssueCommentEvent'"
         " AND JSON_EXTRACT(payload, '$.issue.pull_request') IS NULL")} AS issue_comments_true,
    {_cd("type = 'IssueCommentEvent'"
         " AND JSON_EXTRACT(payload, '$.issue.pull_request') IS NOT NULL")} AS issue_comments_on_prs,
    {_cd("type = 'PullRequestReviewCommentEvent'")} AS pr_review_comments,
    {_cd("type = 'CommitCommentEvent'")} AS commit_comments,
    {_cs("type = 'ReleaseEvent'", RELEASE_DL)} AS release_download_count_sum
-- githubarchive.month.YYYYMM is 1/12 the bytes of year.YYYY — the year
-- table is unpartitioned and a WHERE on EXTRACT(MONTH) still scans the
-- whole thing (~$30 vs ~$2.50 for one month of validation).
FROM `githubarchive.month.{year}{month:02d}`
WHERE public AND repo.id IS NOT NULL AND created_at IS NOT NULL
GROUP BY 1
ORDER BY 1
"""


COMPARE_COLUMNS = [
    "total_events", "total_events_human", "push_events", "push_events_human",
    "bot_events", "distinct_actors", "distinct_actors_human", "distinct_repos",
    "distinct_push_repos", "production_events", "pr_opened", "pr_merged",
    "pr_closed_no_merge", "pr_reopened", "issues_opened", "issues_closed",
    "issues_reopened", "releases_published", "pr_events_total",
    "issues_events_total", "issues_closed_completed", "issues_closed_not_planned",
    "issues_closed_duplicate", "issues_closed_unknown", "reviews_approved",
    "reviews_changes_requested", "reviews_commented", "reviews_dismissed",
    "issue_comments_true", "issue_comments_on_prs", "pr_review_comments",
    "commit_comments", "release_download_count_sum",
]


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd[:5])} ...", flush=True)
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def run_bq(query: str, project: str, out_csv: Path) -> dict:
    """Execute the query via bq; write CSV output and return job stats."""
    job_id = f"lifecycle_validate_{dt.datetime.now(dt.timezone.utc):%Y%m%d%H%M%S}"
    r = _run([
        "bq", f"--project_id={project}", "--job_id", job_id,
        "query", "--use_legacy_sql=false", "--format=csv",
        "--max_rows=1000",
        query,
    ])
    # bq stdout has a status line + CSV; keep header + data.
    lines = [l for l in r.stdout.splitlines() if l.startswith(("event_hour", "20", "19"))]
    out_csv.write_text("\n".join(lines) + "\n")

    show = _run(
        ["bq", f"--project_id={project}", "show", "--format=json", "-j", job_id],
        check=False,
    )
    stats = json.loads(show.stdout).get("statistics", {}) if show.returncode == 0 else {}
    return {
        "job_id": job_id,
        "bytes_billed": int(stats.get("query", {}).get("totalBytesBilled", 0)),
        "rows": len(lines) - 1,
    }


def fetch_spark_output(gs_path: str, project: str, dest: Path) -> None:
    """Copy the Spark Parquet output from GCS to a local directory."""
    dest.mkdir(parents=True, exist_ok=True)
    _run([
        "gcloud", "storage", "cp", "-r", f"{gs_path}/*", str(dest) + "/",
        f"--project={project}",
    ])


def compare(bq_csv: Path, spark_dir: Path) -> dict:
    """Diff BQ CSV vs Spark Parquet. Returns per-column deltas + verdict."""
    import pandas as pd

    bq = pd.read_csv(bq_csv, parse_dates=["event_hour"])
    spark_files = list(spark_dir.glob("**/*.parquet"))
    if not spark_files:
        raise RuntimeError(f"no parquet files under {spark_dir}")
    sp = pd.concat([pd.read_parquet(f) for f in spark_files], ignore_index=True)

    # Normalize event_hour to a comparable key (tz-naive UTC datetime).
    bq["event_hour"] = pd.to_datetime(bq["event_hour"], utc=True).dt.tz_convert(None)
    sp["event_hour"] = pd.to_datetime(sp["event_hour"], utc=True).dt.tz_convert(None)

    merged = bq.merge(sp, on="event_hour", suffixes=("_bq", "_sp"), how="outer",
                      indicator=True)
    diffs = {}
    for col in COMPARE_COLUMNS:
        bq_col, sp_col = f"{col}_bq", f"{col}_sp"
        if bq_col not in merged or sp_col not in merged:
            diffs[col] = {"status": "missing_column"}
            continue
        # Cast both sides to float so NaN handling works uniformly.
        d = (merged[bq_col].astype(float) - merged[sp_col].astype(float))
        n_mismatch = int((d.abs() > 0).sum())
        diffs[col] = {
            "status": "ok" if n_mismatch == 0 else "mismatch",
            "hours_with_diff": n_mismatch,
            "max_abs_diff": float(d.abs().max()) if len(d) else 0.0,
            "sum_bq": int(merged[bq_col].fillna(0).sum()),
            "sum_sp": int(merged[sp_col].fillna(0).sum()),
        }
    row_coverage = {
        "hours_in_bq_only": int((merged["_merge"] == "left_only").sum()),
        "hours_in_sp_only": int((merged["_merge"] == "right_only").sum()),
        "hours_in_both": int((merged["_merge"] == "both").sum()),
    }
    verdict = (
        "PASS"
        if all(d["status"] == "ok" for d in diffs.values())
        and row_coverage["hours_in_bq_only"] == 0
        and row_coverage["hours_in_sp_only"] == 0
        else "FAIL"
    )
    return {"verdict": verdict, "coverage": row_coverage, "columns": diffs}


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, required=True)
    p.add_argument("--project", required=True, help="GCP billing project for bq")
    p.add_argument("--spark-gs", required=True,
                   help="gs:// path to the Spark output for this year+month")
    p.add_argument("--workdir", default=None,
                   help="local scratch dir (default: tempdir)")
    args = p.parse_args(argv)

    workdir = Path(args.workdir or tempfile.mkdtemp(prefix="lifecycle_validate_"))
    workdir.mkdir(parents=True, exist_ok=True)
    bq_csv = workdir / f"bq_{args.year}_{args.month:02d}.csv"
    spark_local = workdir / "spark"

    query = build_bq_query(args.year, args.month)
    bq_stats = run_bq(query, args.project, bq_csv)
    print(f"bq: {bq_stats['rows']} rows, "
          f"{bq_stats['bytes_billed']/1e9:.1f} GB billed", flush=True)

    fetch_spark_output(args.spark_gs, args.project, spark_local)
    result = compare(bq_csv, spark_local)
    result["bq_stats"] = bq_stats
    result["params"] = {"year": args.year, "month": args.month,
                        "spark_gs": args.spark_gs}
    print(json.dumps(result, indent=2, default=str))
    return 0 if result["verdict"] == "PASS" else 1


if __name__ == "__main__":
    sys.exit(main())
