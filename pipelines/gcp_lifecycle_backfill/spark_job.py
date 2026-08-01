"""GCP-side Spark job — decade-scale hourly aggregate extract from GH Archive.

Runs on Dataproc Serverless. Reads ``githubarchive.year.YYYY`` via the
BigQuery Spark connector (Storage Read API under the hood — projected column
reads are billed against the 300 TiB/month free tier), applies the block-out
logic from ``docs/lifecycle_metrics_spec.md`` §Block-out, and writes hourly
Parquet partitioned by year+month to GCS.

Every column and its derivation is defined in ``docs/lifecycle_metrics_spec.md``.
This file is that spec compiled to Spark SQL — do not edit the aggregate
without a matching spec change.

Submit via ``pipelines/gcp_lifecycle_backfill/submit.py``. Local dry-run:
``python spark_job.py --year 2023 --month 6 --output /tmp/out --local``.
"""

from __future__ import annotations

import argparse
import json
import sys

METRIC_SPEC_VERSION = 1

# Projected columns pulled from the BigQuery table via Storage Read API.
# Nested fields addressed via struct notation; the connector pushes down.
PROJECTED_COLUMNS = [
    "id",
    "type",
    "public",
    "created_at",
    "actor.id AS actor_id",
    "actor.login AS actor_login",
    "repo.id AS repo_id",
    "payload",
]

# Block-out predicate — matches Silver's quality_flag IS NULL policy.
BLOCK_OUT_WHERE = "public AND repo_id IS NOT NULL AND created_at IS NOT NULL"

# Bot suffix rule — matches silver.transforms BOT_LOGIN_SUFFIX.
BOT_EXPR = "IFNULL(endswith(actor_login, '[bot]'), FALSE)"
NON_BOT_EXPR = f"NOT ({BOT_EXPR})"

# Era-aware PR merged / closed-without-merge (spec §Era-awareness).
PR_MERGED = (
    "(get_json_object(payload, '$.action') = 'merged'"
    " OR (get_json_object(payload, '$.action') = 'closed'"
    "     AND get_json_object(payload, '$.pull_request.merged') = 'true'))"
)
PR_CLOSED_NM = (
    "(get_json_object(payload, '$.action') = 'closed'"
    " AND IFNULL(get_json_object(payload, '$.pull_request.merged'), 'false') != 'true')"
)

# OQ-7 production whitelist, era-aware form.
PRODUCTION = (
    "(type = 'PushEvent'"
    " OR (type = 'PullRequestEvent'"
    "     AND (get_json_object(payload, '$.action') IN ('opened', 'reopened')"
    f"         OR {PR_MERGED} OR {PR_CLOSED_NM}))"
    " OR (type = 'IssuesEvent'"
    "     AND get_json_object(payload, '$.action') IN ('opened', 'closed', 'reopened'))"
    " OR (type = 'ReleaseEvent'"
    "     AND get_json_object(payload, '$.action') = 'published'))"
)

# Sum of asset download_counts across the release's assets array. Silver
# uses the same expression (silver.transforms.release_events_merge_sql).
RELEASE_DL_SUM = (
    "aggregate("
    "COALESCE(from_json(get_json_object(payload, '$.release.assets'),"
    " 'ARRAY<STRUCT<download_count: BIGINT>>'), array()),"
    " 0L, (acc, a) -> acc + COALESCE(a.download_count, 0L))"
)


def _cd(cond: str) -> str:
    """COUNT(DISTINCT id WHERE cond) — dedup rule per spec §Dedup."""
    return f"COUNT(DISTINCT CASE WHEN {cond} THEN id END)"


def _cs(cond: str, expr: str = "1") -> str:
    """SUM(expr WHERE cond) — for numeric aggregates (e.g. download counts)."""
    return f"SUM(CASE WHEN {cond} THEN {expr} ELSE 0L END)"


def aggregate_sql(view: str) -> str:
    """Build the 34-column hourly aggregate SQL over a view of already-loaded
    events (columns: id, type, public, created_at, actor_id, actor_login,
    repo_id, payload). One row per UTC hour of clean events."""
    return f"""
SELECT
    date_trunc('HOUR', created_at) AS event_hour,
    -- census (10 columns; match pass-1 bq_export_hourly.py output)
    COUNT(DISTINCT id) AS total_events,
    {_cd(NON_BOT_EXPR)} AS total_events_human,
    {_cd("type = 'PushEvent'")} AS push_events,
    {_cd(f"type = 'PushEvent' AND {NON_BOT_EXPR}")} AS push_events_human,
    {_cd(BOT_EXPR)} AS bot_events,
    COUNT(DISTINCT actor_id) AS distinct_actors,
    COUNT(DISTINCT CASE WHEN {NON_BOT_EXPR} THEN actor_id END) AS distinct_actors_human,
    COUNT(DISTINCT repo_id) AS distinct_repos,
    COUNT(DISTINCT CASE WHEN type = 'PushEvent' THEN repo_id END) AS distinct_push_repos,
    -- production (era-aware whitelist)
    {_cd(PRODUCTION)} AS production_events,
    -- PR lifecycle (matches stream gold.ecosystem_hourly)
    {_cd("type = 'PullRequestEvent' AND get_json_object(payload, '$.action') = 'opened'")} AS pr_opened,
    {_cd(f"type = 'PullRequestEvent' AND {PR_MERGED}")} AS pr_merged,
    {_cd(f"type = 'PullRequestEvent' AND {PR_CLOSED_NM}")} AS pr_closed_no_merge,
    {_cd("type = 'PullRequestEvent' AND get_json_object(payload, '$.action') = 'reopened'")} AS pr_reopened,
    -- Issue lifecycle (matches stream)
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'opened'")} AS issues_opened,
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'closed'")} AS issues_closed,
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'reopened'")} AS issues_reopened,
    -- Release lifecycle (matches stream)
    {_cd("type = 'ReleaseEvent' AND get_json_object(payload, '$.action') = 'published'")} AS releases_published,
    -- Denominators (new, needed for state-churn derivation)
    {_cd("type = 'PullRequestEvent'")} AS pr_events_total,
    {_cd("type = 'IssuesEvent'")} AS issues_events_total,
    -- Issue closure taxonomy (new; state_reason added by GitHub Sep 2022)
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'closed'"
         " AND get_json_object(payload, '$.issue.state_reason') = 'completed'")} AS issues_closed_completed,
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'closed'"
         " AND get_json_object(payload, '$.issue.state_reason') = 'not_planned'")} AS issues_closed_not_planned,
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'closed'"
         " AND get_json_object(payload, '$.issue.state_reason') = 'duplicate'")} AS issues_closed_duplicate,
    {_cd("type = 'IssuesEvent' AND get_json_object(payload, '$.action') = 'closed'"
         " AND (get_json_object(payload, '$.issue.state_reason') IS NULL"
         " OR get_json_object(payload, '$.issue.state_reason')"
         " NOT IN ('completed', 'not_planned', 'duplicate'))")} AS issues_closed_unknown,
    -- Review states (new; the Rework signal)
    {_cd("type = 'PullRequestReviewEvent'"
         " AND get_json_object(payload, '$.review.state') = 'approved'")} AS reviews_approved,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND get_json_object(payload, '$.review.state') = 'changes_requested'")} AS reviews_changes_requested,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND get_json_object(payload, '$.review.state') = 'commented'")} AS reviews_commented,
    {_cd("type = 'PullRequestReviewEvent'"
         " AND get_json_object(payload, '$.review.state') = 'dismissed'")} AS reviews_dismissed,
    -- Comment activity (new)
    {_cd("type = 'IssueCommentEvent'"
         " AND get_json_object(payload, '$.issue.pull_request') IS NULL")} AS issue_comments_true,
    {_cd("type = 'IssueCommentEvent'"
         " AND get_json_object(payload, '$.issue.pull_request') IS NOT NULL")} AS issue_comments_on_prs,
    {_cd("type = 'PullRequestReviewCommentEvent'")} AS pr_review_comments,
    {_cd("type = 'CommitCommentEvent'")} AS commit_comments,
    -- Release adoption (new)
    {_cs("type = 'ReleaseEvent'", RELEASE_DL_SUM)} AS release_download_count_sum
FROM {view}
WHERE {BLOCK_OUT_WHERE}
GROUP BY event_hour
"""


def build_spark(app_name: str, local: bool):
    """Create a SparkSession. On Dataproc Serverless the connector jar is
    provided via --properties; locally the caller supplies it via --jars."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName(app_name)
    if local:
        # local dry-run: caller adds --jars pointing at a spark-bigquery jar,
        # or reads from a local JSON dump instead.
        builder = builder.master("local[*]")
    return builder.getOrCreate()


def read_year(spark, year: int, materialization_project: str, materialization_dataset: str):
    """Read one year's githubarchive table with projected columns.

    Projection is critical — Storage Read API bills only the columns you
    request, and ~90% of billed bytes are avoided vs reading the whole row.
    """
    projected = ", ".join(PROJECTED_COLUMNS)
    reader = (
        spark.read.format("bigquery")
        .option("table", f"githubarchive.year.{year}")
        .option("materializationProject", materialization_project)
        .option("materializationDataset", materialization_dataset)
        .option("viewsEnabled", "true")
        # projection pushdown to Storage Read API — reduces bytes read
        .option("selectedFields", "id,type,public,created_at,actor,repo,payload")
    )
    df = reader.load().selectExpr(*PROJECTED_COLUMNS)
    return df


def run(args: argparse.Namespace) -> int:
    spark = build_spark(f"lifecycle-backfill-{args.year}", args.local)
    try:
        if args.local_json:
            # Local dry-run: read a JSONL sample instead of BigQuery.
            df = spark.read.json(args.local_json).selectExpr(
                "id",
                "type",
                "public",
                "CAST(created_at AS TIMESTAMP) AS created_at",
                "actor.id AS actor_id",
                "actor.login AS actor_login",
                "repo.id AS repo_id",
                # payload arrives as a struct from JSON inference; re-serialize
                # to string so get_json_object works as it does in BigQuery.
                "to_json(payload) AS payload",
            )
        else:
            df = read_year(spark, args.year, args.materialization_project,
                          args.materialization_dataset)

        if args.month:
            df = df.where(
                f"year(created_at) = {args.year} AND month(created_at) = {args.month}"
            )

        df.createOrReplaceTempView("events")
        agg = spark.sql(aggregate_sql("events"))

        # Provenance columns
        from pyspark.sql import functions as F

        agg = (
            agg.withColumn("source_year", F.lit(args.year))
            .withColumn("metric_spec_version", F.lit(METRIC_SPEC_VERSION))
            .withColumn("ingested_at", F.current_timestamp())
            .withColumn("job_id", F.lit(args.job_id or "local"))
        )

        # Isolate each run's output under its own year (and optional month)
        # path so `overwrite` never wipes sibling year/month folders. Both
        # shapes remain readable by downstream ingestion as one dataset.
        if args.month:
            out_path = f"{args.output}/year={args.year}/month={args.month:02d}"
        else:
            out_path = f"{args.output}/year={args.year}"
        agg.write.mode("overwrite").parquet(out_path)

        n = agg.count()
        print(json.dumps({
            "status": "ok",
            "year": args.year,
            "month": args.month,
            "hourly_rows": n,
            "output": out_path,
            "metric_spec_version": METRIC_SPEC_VERSION,
        }))
        return 0
    finally:
        spark.stop()


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--year", type=int, required=True)
    p.add_argument("--month", type=int, default=None,
                   help="if set, restrict to one month (bench mode)")
    p.add_argument("--output", required=True,
                   help="output GCS or local path (partitioned Parquet)")
    p.add_argument("--materialization-project", default=None,
                   help="GCP project for BigQuery Spark connector materialization")
    p.add_argument("--materialization-dataset", default=None,
                   help="BQ dataset name in materialization-project (temp views)")
    p.add_argument("--job-id", default=None, help="propagated into provenance")
    p.add_argument("--local", action="store_true",
                   help="use local[*] SparkSession (for dry-run only)")
    p.add_argument("--local-json", default=None,
                   help="local JSONL sample to read instead of BigQuery")
    return p


if __name__ == "__main__":
    sys.exit(run(build_argparser().parse_args()))
