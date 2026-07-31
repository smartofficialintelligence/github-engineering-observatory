#!/usr/bin/env python3
"""Drive the Databricks workspace from a dev box — stdlib only.

Bridges the gap until Asset Bundles land (deferred per spec §15): syncs
this repo into the workspace as a Git folder and executes the numbered
notebooks on serverless compute via the Jobs API, so the in-workspace
verification steps are runnable from any machine that has credentials.

Credentials come from the environment:

    export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com
    export DATABRICKS_TOKEN=<personal access token>

Subcommands:

    check                         verify connectivity and identity
    sync-repo [--branch BRANCH]   create/update the workspace Git folder
    run-notebook PATH             submit a one-shot serverless notebook run
                                  (workspace path, e.g. the path sync-repo
                                  prints + /notebooks/01_download_and_profile)
    upload FILE... --dest DIR     PUT local files into a UC Volume directory
                                  (fallback if workspace-side egress to
                                  data.gharchive.org is blocked)

Typical first run:

    python scripts/databricks_run.py check
    python scripts/databricks_run.py sync-repo --branch claude/github-observatory-spec-lfr2c4
    python scripts/databricks_run.py run-notebook \
        /Repos/<me>/github-engineering-observatory/notebooks/01_download_and_profile
    python scripts/databricks_run.py run-notebook \
        /Repos/<me>/github-engineering-observatory/notebooks/02_bronze_ingest
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

REPO_URL = "https://github.com/smartofficialintelligence/github-engineering-observatory"
REPO_NAME = "github-engineering-observatory"
DEFAULT_BRANCH = "claude/github-observatory-spec-lfr2c4"
POLL_SECONDS = 20
TERMINAL_STATES = {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


class ApiError(RuntimeError):
    def __init__(self, status: int, body: str, url: str):
        super().__init__(f"HTTP {status} from {url}: {body[:500]}")
        self.status = status


def _credentials() -> tuple[str, str]:
    host = os.environ.get("DATABRICKS_HOST", "").rstrip("/")
    token = os.environ.get("DATABRICKS_TOKEN", "")
    if not host or not token:
        sys.exit(
            "DATABRICKS_HOST and DATABRICKS_TOKEN must be set, e.g.\n"
            "  export DATABRICKS_HOST=https://<workspace>.cloud.databricks.com\n"
            "  export DATABRICKS_TOKEN=<personal access token>"
        )
    if not host.startswith("http"):
        host = "https://" + host
    return host, token


def api(
    method: str,
    path: str,
    payload: dict | None = None,
    *,
    raw_body: bytes | None = None,
    content_type: str = "application/json",
    query: dict | None = None,
) -> dict:
    host, token = _credentials()
    url = host + path
    if query:
        url += "?" + urllib.parse.urlencode(query)
    body = raw_body if raw_body is not None else (
        json.dumps(payload).encode() if payload is not None else None
    )
    request = urllib.request.Request(
        url,
        data=body,
        method=method,
        headers={
            "Authorization": f"Bearer {token}",
            "Content-Type": content_type,
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=120) as resp:
            text = resp.read().decode("utf-8", "replace")
    except urllib.error.HTTPError as exc:
        raise ApiError(exc.code, exc.read().decode("utf-8", "replace"), url) from None
    return json.loads(text) if text.strip() else {}


def whoami() -> str:
    me = api("GET", "/api/2.0/preview/scim/v2/Me")
    return me.get("userName") or me.get("displayName") or "<unknown>"


# -- subcommands ---------------------------------------------------------------


def cmd_check(_: argparse.Namespace) -> int:
    host, _token = _credentials()
    try:
        user = whoami()
    except ApiError as exc:
        if exc.status in (401, 403):
            print(f"AUTH FAILED against {host}: {exc}", file=sys.stderr)
            return 1
        raise
    print(f"connected to {host} as {user}")
    catalogs = api("GET", "/api/2.1/unity-catalog/catalogs").get("catalogs", [])
    names = sorted(c["name"] for c in catalogs)
    print("catalogs:", ", ".join(names) or "<none visible>")
    if "github_observatory" not in names:
        print("WARNING: catalog github_observatory not visible to this token")
    return 0


def _find_repo(user: str) -> dict | None:
    prefix = f"/Repos/{user}"
    resp = api("GET", "/api/2.0/repos", query={"path_prefix": prefix})
    for repo in resp.get("repos", []):
        if repo.get("path", "").rstrip("/").endswith("/" + REPO_NAME):
            return repo
    # Git folders in newer workspaces don't appear in the repos list and
    # get-status reports them as DIRECTORY — but the object_id works as
    # the repo id for GET/PATCH /api/2.0/repos/{id}.
    path = f"{prefix}/{REPO_NAME}"
    try:
        status = api("GET", "/api/2.0/workspace/get-status", query={"path": path})
    except ApiError as exc:
        if exc.status == 404:
            return None
        raise
    object_id = status.get("object_id")
    if object_id is None:
        return None
    try:
        return api("GET", f"/api/2.0/repos/{object_id}")
    except ApiError as exc:
        if exc.status in (400, 404):  # exists but is not a Git folder
            sys.exit(f"{path} exists but is not a Git folder — remove or rename it")
        raise


def cmd_sync_repo(args: argparse.Namespace) -> int:
    user = whoami()
    repo = _find_repo(user)
    if repo is None:
        repo = api(
            "POST", "/api/2.0/repos",
            {
                "url": REPO_URL,
                "provider": "gitHub",
                "path": f"/Repos/{user}/{REPO_NAME}",
            },
        )
        print(f"created workspace repo at {repo['path']}")
    updated = api("PATCH", f"/api/2.0/repos/{repo['id']}", {"branch": args.branch})
    print(
        f"workspace repo {updated.get('path', repo['path'])} on branch "
        f"{updated.get('branch')} @ {updated.get('head_commit_id', '?')[:12]}"
    )
    print(f"notebook base path: {repo['path']}/notebooks")
    return 0


def cmd_run_notebook(args: argparse.Namespace) -> int:
    submission = api(
        "POST", "/api/2.2/jobs/runs/submit",
        {
            "run_name": f"observatory {os.path.basename(args.notebook_path)}",
            "tasks": [
                {
                    "task_key": "run",
                    "notebook_task": {
                        "notebook_path": args.notebook_path,
                        "source": "WORKSPACE",
                    },
                }
            ],
        },
    )
    run_id = submission["run_id"]
    print(f"submitted run {run_id}")
    while True:
        run = api("GET", "/api/2.2/jobs/runs/get", query={"run_id": run_id})
        state = run.get("state", {})
        life = state.get("life_cycle_state", "?")
        result = state.get("result_state", "")
        print(f"  {life} {result} {state.get('state_message', '')}".rstrip())
        if life in TERMINAL_STATES:
            break
        time.sleep(POLL_SECONDS)
    print("run page:", run.get("run_page_url", "<none>"))
    if result != "SUCCESS":
        for task in run.get("tasks", []):
            task_run = api(
                "GET", "/api/2.2/jobs/runs/get-output",
                query={"run_id": task["run_id"]},
            )
            error = task_run.get("error")
            if error:
                print(f"task {task['task_key']} error: {error}", file=sys.stderr)
        return 1
    return 0


def cmd_deploy_dashboard(args: argparse.Namespace) -> int:
    """Create or update an AI/BI dashboard from a .lvdash.json file,
    then publish it with embedded credentials."""
    with open(args.file, encoding="utf-8") as fh:
        serialized = fh.read()
    display_name = (
        os.path.basename(args.file)
        .removesuffix(".lvdash.json")
        .replace("_", " ")
        .title()
    )
    warehouse = api("GET", "/api/2.0/sql/warehouses")["warehouses"][0]

    existing = None
    resp = api("GET", "/api/2.0/lakeview/dashboards", query={"page_size": 100})
    for d in resp.get("dashboards", []):
        if d.get("display_name") == display_name:
            existing = d
            break

    body = {
        "display_name": display_name,
        "serialized_dashboard": serialized,
        "warehouse_id": warehouse["id"],
    }
    if existing:
        dash = api(
            "PATCH", f"/api/2.0/lakeview/dashboards/{existing['dashboard_id']}", body
        )
        print(f"updated dashboard {display_name} ({dash['dashboard_id']})")
    else:
        user = whoami()
        body["parent_path"] = f"/Workspace/Users/{user}"
        dash = api("POST", "/api/2.0/lakeview/dashboards", body)
        print(f"created dashboard {display_name} ({dash['dashboard_id']})")

    api(
        "POST",
        f"/api/2.0/lakeview/dashboards/{dash['dashboard_id']}/published",
        {"embed_credentials": True, "warehouse_id": warehouse["id"]},
    )
    host, _ = _credentials()
    print(f"published: {host}/sql/dashboardsv3/{dash['dashboard_id']}/published")
    return 0


PIPELINE_JOB_NAME = "github-observatory-hourly-refresh"


def cmd_create_job(args: argparse.Namespace) -> int:
    """Create or update the scheduled hourly-refresh job (notebook 08)."""
    existing = api(
        "GET", "/api/2.2/jobs/list", query={"name": PIPELINE_JOB_NAME}
    ).get("jobs", [])
    settings = {
        "name": PIPELINE_JOB_NAME,
        "tasks": [
            {
                "task_key": "hourly_refresh",
                "notebook_task": {
                    "notebook_path": args.notebook_path,
                    "source": "WORKSPACE",
                },
            }
        ],
        "schedule": {
            # :20 past every hour — the archive publishes ~:05 for the
            # previous-previous hour; :20 leaves slack.
            "quartz_cron_expression": "0 20 * * * ?",
            "timezone_id": "UTC",
            "pause_status": "PAUSED" if args.paused else "UNPAUSED",
        },
        "max_concurrent_runs": 1,
    }
    try:
        if existing:
            job_id = existing[0]["job_id"]
            api("POST", "/api/2.2/jobs/reset", {"job_id": job_id, "new_settings": settings})
            print(f"updated job {job_id} ({PIPELINE_JOB_NAME})")
        else:
            job_id = api("POST", "/api/2.2/jobs/create", settings)["job_id"]
            print(f"created job {job_id} ({PIPELINE_JOB_NAME})")
    except ApiError as exc:
        if "schedule" in str(exc).lower() or exc.status == 400:
            print(
                "job with schedule rejected (plan restriction?) — retrying unscheduled",
                file=sys.stderr,
            )
            settings.pop("schedule")
            job_id = api("POST", "/api/2.2/jobs/create", settings)["job_id"]
            print(f"created UNSCHEDULED job {job_id}; trigger manually or via "
                  f"POST /api/2.2/jobs/run-now")
        else:
            raise
    print(f"schedule: hourly at :20 UTC ({'paused' if args.paused else 'active'})")
    return 0


def cmd_upload(args: argparse.Namespace) -> int:
    dest = args.dest.rstrip("/")
    if not dest.startswith("/Volumes/"):
        sys.exit(f"--dest must be a /Volumes/... path, got {dest}")
    for local in args.files:
        name = os.path.basename(local)
        with open(local, "rb") as fh:
            data = fh.read()
        encoded = urllib.parse.quote(f"{dest}/{name}")
        api(
            "PUT", f"/api/2.0/fs/files{encoded}",
            raw_body=data,
            content_type="application/octet-stream",
            query={"overwrite": "true"},
        )
        print(f"uploaded {local} -> {dest}/{name} ({len(data):,} bytes)")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("check", help="verify connectivity and identity")

    p_sync = sub.add_parser("sync-repo", help="create/update the workspace Git folder")
    p_sync.add_argument("--branch", default=DEFAULT_BRANCH)

    p_run = sub.add_parser("run-notebook", help="one-shot serverless notebook run")
    p_run.add_argument("notebook_path", help="workspace notebook path (no extension)")

    p_up = sub.add_parser("upload", help="upload local files to a UC Volume")
    p_up.add_argument("files", nargs="+")
    p_up.add_argument("--dest", required=True, help="/Volumes/... directory")

    p_job = sub.add_parser(
        "create-job", help="create/update the scheduled hourly-refresh job"
    )
    p_job.add_argument(
        "notebook_path", help="workspace path of notebooks/08_hourly_pipeline"
    )
    p_job.add_argument(
        "--paused", action="store_true",
        help="register the schedule paused (activate later in the UI)",
    )

    p_dash = sub.add_parser(
        "deploy-dashboard", help="create/update + publish an AI/BI dashboard"
    )
    p_dash.add_argument("file", help="path to a .lvdash.json definition")

    args = parser.parse_args(argv)
    handler = {
        "check": cmd_check,
        "sync-repo": cmd_sync_repo,
        "run-notebook": cmd_run_notebook,
        "upload": cmd_upload,
        "create-job": cmd_create_job,
        "deploy-dashboard": cmd_deploy_dashboard,
    }[args.command]
    try:
        return handler(args)
    except ApiError as exc:
        print(str(exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
