#!/usr/bin/env python3
"""Submit the lifecycle backfill Spark job to Dataproc Serverless.

Two modes:
    bench     one month of one year (~1/12 of a year's data). Use to
              measure DCU-hours + bytes-read before committing to the
              full backfill (spec §Motivation).
    backfill  one or more years, one batch each (batches parallelize
              cleanly at the year grain; separate batches keep failures
              per-year re-runnable).

Uses gcloud CLI (matches ``scripts/bq_export_hourly.py`` and
``scripts/databricks_run.py``). Requires ``gcloud auth login`` and a
project with Dataproc Serverless enabled.

Runs the aggregation defined in ``pipelines/gcp_lifecycle_backfill/spark_job.py``
against ``githubarchive.year.YYYY`` and writes hourly Parquet to
``gs://<bucket>/lifecycle_out/year=YYYY[/month=MM]/``. Manifest updated
per batch at ``gs://<bucket>/lifecycle_out/_manifest.jsonl``.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).parent
SPARK_JOB_PATH = HERE / "spark_job.py"

# Dataproc Serverless runtime 2.2 (Spark 3.5 + Scala 2.13 + Java 17)
# ships with the Spark-BigQuery connector preinstalled — do NOT add it via
# --jars, that path collides with the built-in and triggers a Scala-version
# properties-file error at connector init. Ref:
# https://cloud.google.com/dataproc-serverless/docs/concepts/versions/spark-runtime-2.2
DATAPROC_RUNTIME_VERSION = "2.2"

# Dataproc Serverless standard-tier list price (2026-08). Update if Google
# changes rates. Cost is DCU-seconds billed regardless of parallelism.
DCU_PRICE_PER_HOUR_USD = 0.06

# Bench cost model: 2023-06 consumed 26.9 DCU-hours (~$1.61). Actual per-
# month cost varies with data volume — 2016-2020 are much smaller, 2021-2024
# grow, 2025+ dropped under the OQ-1 filter. This is a rough upper bound
# for the sanity check; the real cost is reported after each batch.
COST_MODEL_DCU_HOURS_PER_MONTH = {
    # year: approx DCU-hours per month (upper bound based on ecosystem volume)
    2016: 4, 2017: 6, 2018: 8, 2019: 10, 2020: 15,
    2021: 20, 2022: 22, 2023: 27, 2024: 30,
    2025: 15,   # OQ-1 filter reduced volume mid-2025
}
DEFAULT_DCU_HOURS_PER_MONTH = 27  # fallback for years not in the model


def estimate_batch_cost_usd(year: int, months: int = 12) -> float:
    """Rough cost estimate for a Dataproc batch covering ``months`` of ``year``."""
    per_month = COST_MODEL_DCU_HOURS_PER_MONTH.get(year, DEFAULT_DCU_HOURS_PER_MONTH)
    return per_month * months * DCU_PRICE_PER_HOUR_USD


def _run(cmd: list[str], check: bool = True) -> subprocess.CompletedProcess:
    print(f"$ {' '.join(cmd)}", flush=True)
    return subprocess.run(cmd, capture_output=True, text=True, check=check)


def upload_pyfile(bucket: str, project: str) -> str:
    """Upload spark_job.py to gs://<bucket>/pyfiles/. Returns the gs:// URI."""
    dest = f"gs://{bucket}/pyfiles/spark_job.py"
    _run(["gcloud", "storage", "cp", str(SPARK_JOB_PATH), dest,
          f"--project={project}"])
    return dest


def submit_batch(
    *,
    project: str,
    region: str,
    batch_id: str,
    pyfile_gs: str,
    deps_bucket: str,
    job_args: list[str],
    service_account: str | None,
    subnet: str | None,
    ttl: str,
    max_executors: int,
) -> dict:
    """Submit one PySpark batch. ``gcloud batches submit`` blocks until
    the batch reaches a terminal state, so this call is synchronous.
    Returns the parsed batch resource.
    """
    # Dataproc Serverless has a default TTL of 4h — insufficient for
    # year-length batches. Set explicitly. Cost billed on DCU-seconds
    # regardless of TTL, so no downside to a generous cap.
    #
    # spark.dynamicAllocation.maxExecutors: without this, autoscaling caps
    # around ~15-30 executors and long jobs hit TTL. Cost is DCU-seconds
    # billed regardless of parallelism — higher max just finishes faster.
    args = [
        "gcloud", "dataproc", "batches", "submit", "pyspark",
        pyfile_gs,
        f"--project={project}",
        f"--region={region}",
        f"--batch={batch_id}",
        f"--version={DATAPROC_RUNTIME_VERSION}",
        f"--deps-bucket=gs://{deps_bucket}",
        f"--ttl={ttl}",
        f"--properties=spark.dynamicAllocation.maxExecutors={max_executors}",
    ]
    if service_account:
        args.append(f"--service-account={service_account}")
    if subnet:
        args.append(f"--subnet={subnet}")
    args.append("--")
    args.extend(job_args)

    # Submit blocks until terminal state; its stdout mixes Spark driver
    # output with any gcloud json, so we rely on exit code and then
    # describe the batch for authoritative state + runtime telemetry.
    submit_rc = _run(args, check=False).returncode

    describe = _run([
        "gcloud", "dataproc", "batches", "describe", batch_id,
        f"--project={project}", f"--region={region}", "--format=json",
    ], check=False)
    try:
        batch = json.loads(describe.stdout)
    except json.JSONDecodeError:
        print("describe stdout:", describe.stdout, file=sys.stderr)
        print("describe stderr:", describe.stderr, file=sys.stderr)
        raise RuntimeError(f"batch {batch_id}: describe failed after submit rc={submit_rc}")

    state = batch.get("state")
    if state != "SUCCEEDED":
        raise RuntimeError(f"batch {batch_id} ended in state {state} (submit rc={submit_rc})")
    return batch


def batch_summary(batch: dict) -> dict:
    """Extract cost-relevant fields from the batch resource."""
    runtime = batch.get("runtimeInfo", {})
    approx = runtime.get("approximateUsage", {})
    return {
        "batch_id": batch.get("name", "").rsplit("/", 1)[-1],
        "state": batch.get("state"),
        "create_time": batch.get("createTime"),
        "state_time": batch.get("stateTime"),
        "milli_dcu_seconds": int(approx.get("milliDcuSeconds", 0)),
        "shuffle_storage_gb_seconds": int(approx.get("shuffleStorageGbSeconds", 0)),
        # milli-DCU-seconds → DCU-hours: /1000 /3600. Serverless list price is
        # $0.06/DCU-hour standard tier as of 2026; verify before extrapolating.
        "dcu_hours": round(int(approx.get("milliDcuSeconds", 0)) / 1000 / 3600, 3),
    }


def append_manifest(bucket: str, project: str, entry: dict) -> None:
    """Append one JSON line to gs://<bucket>/lifecycle_out/_manifest.jsonl."""
    manifest_uri = f"gs://{bucket}/lifecycle_out/_manifest.jsonl"
    line = json.dumps(entry) + "\n"
    # gsutil compose is fussy; use gcloud storage's append via a temp file.
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".jsonl", delete=False) as fh:
        fh.write(line)
        tmp = fh.name
    try:
        # Try append via 'gcloud storage objects compose' if the manifest
        # exists, else just upload.
        exists = subprocess.run(
            ["gcloud", "storage", "ls", manifest_uri, f"--project={project}"],
            capture_output=True, text=True,
        ).returncode == 0
        if exists:
            # Fetch existing, append, upload back — simpler than compose.
            fetched = tempfile.NamedTemporaryFile("wb", delete=False).name
            _run(["gcloud", "storage", "cp", manifest_uri, fetched,
                  f"--project={project}"])
            with open(fetched, "a") as fh:
                fh.write(line)
            _run(["gcloud", "storage", "cp", fetched, manifest_uri,
                  f"--project={project}"])
        else:
            _run(["gcloud", "storage", "cp", tmp, manifest_uri,
                  f"--project={project}"])
    finally:
        Path(tmp).unlink(missing_ok=True)


def build_job_args(*, output_gs: str, year: int, month: int | None,
                   materialization_project: str,
                   materialization_dataset: str, batch_id: str) -> list[str]:
    args = [
        f"--year={year}",
        f"--output={output_gs}",
        f"--materialization-project={materialization_project}",
        f"--materialization-dataset={materialization_dataset}",
        f"--job-id={batch_id}",
    ]
    if month is not None:
        args.append(f"--month={month}")
    return args


def run_one(*, year: int, month: int | None, args: argparse.Namespace,
            pyfile_gs: str) -> dict:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    mode = f"m{month:02d}" if month else "full"
    batch_id = f"lifecycle-{year}-{mode}-{ts}"
    output_gs = f"gs://{args.bucket}/lifecycle_out"

    job_args = build_job_args(
        output_gs=output_gs, year=year, month=month,
        materialization_project=args.project,
        materialization_dataset=args.materialization_dataset,
        batch_id=batch_id,
    )
    batch = submit_batch(
        project=args.project, region=args.region, batch_id=batch_id,
        pyfile_gs=pyfile_gs, deps_bucket=args.bucket, job_args=job_args,
        service_account=args.service_account, subnet=args.subnet,
        ttl=args.ttl, max_executors=args.max_executors,
    )
    summary = batch_summary(batch)
    summary.update({
        "year": year, "month": month, "output": output_gs,
        "recorded_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
    })
    append_manifest(args.bucket, args.project, summary)
    print(json.dumps(summary, indent=2))
    return summary


def _gate_on_cost(estimated_usd: float, args: argparse.Namespace, label: str) -> None:
    """Print estimated cost. Abort unless --yes or under --max-cost-usd."""
    print(f"COST ESTIMATE: {label} ≈ ${estimated_usd:.2f}"
          f" (list price; actual reported after each batch)", flush=True)
    if estimated_usd > args.max_cost_usd and not args.yes:
        raise SystemExit(
            f"aborted: estimated ${estimated_usd:.2f} exceeds "
            f"--max-cost-usd={args.max_cost_usd:.2f}. Re-run with --yes to override."
        )


def cmd_bench(args: argparse.Namespace) -> int:
    est = estimate_batch_cost_usd(args.year, months=1)
    _gate_on_cost(est, args, f"bench {args.year}-{args.month:02d}")
    pyfile_gs = upload_pyfile(args.bucket, args.project)
    run_one(year=args.year, month=args.month, args=args, pyfile_gs=pyfile_gs)
    return 0


def _parse_years(spec: str) -> list[int]:
    if "-" in spec:
        start, end = spec.split("-")
        return list(range(int(start), int(end) + 1))
    return [int(y) for y in spec.split(",")]


def cmd_backfill(args: argparse.Namespace) -> int:
    """Backfill one or more years. Decomposes each year into 12 monthly
    batches by default — each batch is small (<1h), safe under any TTL,
    and independently re-runnable if it fails. Use --yearly to opt back
    into the old one-batch-per-year behavior (only recommended after a
    successful monthly run establishes actual per-year DCU cost).
    """
    years = _parse_years(args.years)
    unit = "year" if args.yearly else "month"
    n_units = len(years) if args.yearly else len(years) * 12
    total_est = sum(
        estimate_batch_cost_usd(y, months=12 if args.yearly else 1)
        * (1 if args.yearly else 12)
        for y in years
    )
    _gate_on_cost(
        total_est, args,
        f"{n_units} {unit} batch(es) across years {years[0]}-{years[-1]}",
    )

    pyfile_gs = upload_pyfile(args.bucket, args.project)
    summaries = []
    plan = [(y, None) for y in years] if args.yearly else [
        (y, m) for y in years for m in range(1, 13)
    ]
    for i, (year, month) in enumerate(plan, 1):
        print(f"\n[{i}/{len(plan)}] year={year} month={month or 'full'}", flush=True)
        summaries.append(run_one(year=year, month=month, args=args,
                                 pyfile_gs=pyfile_gs))
    total_dcu = sum(s["dcu_hours"] for s in summaries)
    total_usd = total_dcu * DCU_PRICE_PER_HOUR_USD
    print(f"\nbackfill complete: {len(summaries)} batch(es), "
          f"{total_dcu:.1f} DCU-hours total (~${total_usd:.2f})", flush=True)
    return 0


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    common = argparse.ArgumentParser(add_help=False)
    common.add_argument("--project", required=True,
                        help="GCP project (also the BQ materialization project)")
    common.add_argument("--region", default="us-central1",
                        help="Dataproc region (must match GCS bucket region)")
    common.add_argument("--bucket", required=True,
                        help="GCS bucket for pyfile staging + Parquet output")
    common.add_argument("--materialization-dataset", default="observatory_scratch",
                        help="BQ dataset in --project for connector temp views")
    common.add_argument("--service-account", default=None,
                        help="service account for the batch (else compute default)")
    common.add_argument("--subnet", default=None,
                        help="VPC subnet URI if project requires it")
    common.add_argument("--ttl", default="12h",
                        help="Dataproc batch TTL (default 12h; bench month ran"
                        " in <1h so this is generous). Cost is DCU-seconds"
                        " billed regardless of TTL — set high enough to finish.")
    common.add_argument("--max-executors", type=int, default=100,
                        help="spark.dynamicAllocation.maxExecutors ceiling"
                        " (default 100). Higher = faster wall-clock, same"
                        " DCU-second cost. Prevents TTL cancellation on large"
                        " batches.")
    common.add_argument("--max-cost-usd", type=float, default=5.0,
                        help="abort if the estimated cost for this invocation"
                        " exceeds this (default $5). Override with --yes.")
    common.add_argument("--yes", action="store_true",
                        help="proceed even if estimated cost exceeds"
                        " --max-cost-usd")

    subs = p.add_subparsers(dest="cmd", required=True)

    b = subs.add_parser("bench", parents=[common],
                        help="one-month bench (spec §Motivation)")
    b.add_argument("--year", type=int, required=True)
    b.add_argument("--month", type=int, required=True)
    b.set_defaults(fn=cmd_bench)

    bf = subs.add_parser("backfill", parents=[common],
                         help="year-range backfill (default: 12 monthly batches per year)")
    bf.add_argument("--years", default="2016-2025",
                    help="e.g. 2016-2025 or 2019 or 2019,2020,2021")
    bf.add_argument("--yearly", action="store_true",
                    help="opt out of monthly decomposition — one batch per year."
                    " Riskier: needs a longer TTL and any single failure loses"
                    " the whole year's progress. Use only after monthly runs"
                    " have established the per-year cost/runtime empirically.")
    bf.set_defaults(fn=cmd_backfill)

    return p


def main(argv=None) -> int:
    args = build_argparser().parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
