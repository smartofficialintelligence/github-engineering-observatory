#!/usr/bin/env python3
"""Binary-search the exact date a schema feature appeared or disappeared —
free, using GH Archive hourly files instead of BigQuery.

``pipelines/gcp_lifecycle_backfill/pin_signal_boundaries.py`` does the same
search against BigQuery at ~$0.017/probe. This module downloads the hour
file from data.gharchive.org instead (~20 MB, free) and evaluates the
predicate locally, so pinning a boundary costs $0 and ~10 s/probe. Use
this for every boundary the decade drift map surfaces; use the BigQuery
version only when a full-day count (not a single hour) is required.

A *feature* is a named predicate over one event, e.g. "this
PullRequestEvent carries payload.pull_request.merged". A feature is
"present" in an hour if at least ``--min-hits`` events satisfy it.

Built-in features (``--list-features``) cover the fields that matter for
backfill correctness. ``--json-path`` defines an ad-hoc one without code
changes.

Usage:

    # when did pull_request.merged disappear?
    python pin_boundary.py --feature pr_merged_field \
        --lo 2025-06-15 --hi 2026-01-15

    # ad-hoc: when did issue.state_reason appear?
    python pin_boundary.py --json-path payload.issue.state_reason \
        --event-type IssuesEvent --lo 2021-01-15 --hi 2023-06-15
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Callable, Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from github_observatory.ingestion.download_gharchive import download_hour  # noqa: E402


def _get_path(obj: dict, path: str):
    """Walk a dotted path. Returns None if any segment is missing."""
    cur = obj
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


def make_path_predicate(json_path: str, event_type: Optional[str] = None,
                        equals: Optional[str] = None) -> Callable[[dict], bool]:
    """Predicate: event has ``json_path`` present (optionally == ``equals``)."""
    def pred(event: dict) -> bool:
        if event_type and event.get("type") != event_type:
            return False
        value = _get_path(event, json_path)
        if value is None:
            return False
        if equals is not None:
            return str(value).lower() == equals.lower()
        return True
    return pred


# Named features covering the fields that govern backfill correctness.
# Each maps to (json_path, event_type, equals-or-None).
FEATURES: dict[str, tuple[str, Optional[str], Optional[str]]] = {
    # PR merge signal — the two era-specific encodings
    "pr_merged_field": ("payload.pull_request.merged", "PullRequestEvent", None),
    "pr_merged_true": ("payload.pull_request.merged", "PullRequestEvent", "true"),
    "pr_merged_at": ("payload.pull_request.merged_at", "PullRequestEvent", None),
    "pr_merged_action": ("payload.action", "PullRequestEvent", "merged"),
    "pr_state_field": ("payload.pull_request.state", "PullRequestEvent", None),
    # PR payload richness
    "pr_title": ("payload.pull_request.title", "PullRequestEvent", None),
    "pr_body": ("payload.pull_request.body", "PullRequestEvent", None),
    "pr_additions": ("payload.pull_request.additions", "PullRequestEvent", None),
    "pr_changed_files": ("payload.pull_request.changed_files", "PullRequestEvent", None),
    # Push payload richness — the commit-count question
    "push_commits": ("payload.commits", "PushEvent", None),
    "push_size": ("payload.size", "PushEvent", None),
    "push_distinct_size": ("payload.distinct_size", "PushEvent", None),
    # Issue closure taxonomy
    "issue_state_reason": ("payload.issue.state_reason", "IssuesEvent", None),
    "issue_state_reason_not_planned": (
        "payload.issue.state_reason", "IssuesEvent", "not_planned"),
    "issue_state_reason_duplicate": (
        "payload.issue.state_reason", "IssuesEvent", "duplicate"),
    # Review + release
    "review_state": ("payload.review.state", "PullRequestReviewEvent", None),
    "release_assets": ("payload.release.assets", "ReleaseEvent", None),
    "release_immutable": ("payload.release.immutable", "ReleaseEvent", None),
    # Fork / repo detail
    "forkee_stargazers": ("payload.forkee.stargazers_count", "ForkEvent", None),
}


def probe_hour(day: dt.date, hour: int, pred: Callable[[dict], bool],
               raw_dir: str, min_hits: int, keep: bool) -> Optional[int]:
    """Download one hour, count events satisfying pred. None if unavailable."""
    spec = f"{day:%Y-%m-%d}-{hour}"
    result = download_hour(spec, raw_dir)
    if result.status == "failed":
        print(f"  [{spec}] download failed: {result.error_message}", flush=True)
        return None
    path = os.path.join(raw_dir, result.source_file)
    hits = 0
    scanned = 0
    try:
        with gzip.open(path, "rb") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    event = json.loads(line)
                except (ValueError, UnicodeDecodeError):
                    continue
                scanned += 1
                if pred(event):
                    hits += 1
                    if hits >= min_hits:
                        break  # early exit once threshold is met
    finally:
        if not keep and os.path.exists(path):
            os.unlink(path)
    if hits >= min_hits:
        # Early exit means `scanned` is a partial count — don't report it
        # as if the whole hour were read.
        print(f"  [{spec}] hits={hits}+ (stopped early) → PRESENT", flush=True)
    else:
        print(f"  [{spec}] hits={hits} of {scanned:,} events → ABSENT", flush=True)
    return hits


def binary_search(lo: dt.date, hi: dt.date, hour: int,
                  pred: Callable[[dict], bool], raw_dir: str,
                  min_hits: int, keep: bool) -> Optional[tuple]:
    """Find the day the predicate's presence flips between lo and hi."""
    def present(day: dt.date) -> Optional[bool]:
        hits = probe_hour(day, hour, pred, raw_dir, min_hits, keep)
        return None if hits is None else hits >= min_hits

    lo_state = present(lo)
    hi_state = present(hi)
    if lo_state is None or hi_state is None:
        print("anchor probe unavailable — widen the window", flush=True)
        return None
    if lo_state == hi_state:
        print(f"anchors agree (both {'PRESENT' if lo_state else 'ABSENT'}) — "
              f"no transition inside [{lo}, {hi}]", flush=True)
        return None

    probes = 2
    while (hi - lo).days > 1:
        mid = lo + dt.timedelta(days=(hi - lo).days // 2)
        state = present(mid)
        probes += 1
        if state is None:
            # Missing hour: nudge to a neighbouring day.
            for delta in (1, -1, 2, -2, 3, -3):
                cand = mid + dt.timedelta(days=delta)
                if lo < cand < hi:
                    state = present(cand)
                    probes += 1
                    if state is not None:
                        mid = cand
                        break
            if state is None:
                print(f"cannot probe near {mid} — aborting", flush=True)
                return None
        if state == lo_state:
            lo = mid
        else:
            hi = mid
    return lo, hi, lo_state, hi_state, probes


def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--feature", choices=sorted(FEATURES),
                   help="named feature to search for")
    p.add_argument("--json-path", help="ad-hoc dotted path, e.g. payload.issue.state_reason")
    p.add_argument("--event-type", help="restrict ad-hoc predicate to this event type")
    p.add_argument("--equals", help="require the path's value to equal this")
    p.add_argument("--lo", required=True, help="known-state anchor date YYYY-MM-DD")
    p.add_argument("--hi", required=True, help="other-state anchor date YYYY-MM-DD")
    p.add_argument("--hour", type=int, default=12, help="hour of day UTC (default 12)")
    p.add_argument("--min-hits", type=int, default=1,
                   help="events needed to call the feature present (default 1)")
    p.add_argument("--dest", default=None, help="download dir (default: temp)")
    p.add_argument("--keep-downloads", action="store_true")
    p.add_argument("--list-features", action="store_true")
    args = p.parse_args(argv)

    if args.list_features:
        print("named features:")
        for name, (path, et, eq) in sorted(FEATURES.items()):
            suffix = f" == {eq}" if eq else ""
            scope = f" [{et}]" if et else ""
            print(f"  {name:32s} {path}{suffix}{scope}")
        return 0

    if args.feature:
        json_path, event_type, equals = FEATURES[args.feature]
        label = args.feature
    elif args.json_path:
        json_path, event_type, equals = args.json_path, args.event_type, args.equals
        label = args.json_path
    else:
        p.error("pass --feature or --json-path (or --list-features)")

    pred = make_path_predicate(json_path, event_type, equals)
    lo = dt.date.fromisoformat(args.lo)
    hi = dt.date.fromisoformat(args.hi)
    raw_dir = args.dest or tempfile.mkdtemp(prefix="pin_boundary_")
    os.makedirs(raw_dir, exist_ok=True)

    print(f"searching for `{label}` transition in [{lo}, {hi}] "
          f"at {args.hour:02d}:00 UTC, min_hits={args.min_hits}")
    result = binary_search(lo, hi, args.hour, pred, raw_dir,
                           args.min_hits, args.keep_downloads)
    if result is None:
        return 1
    lo_d, hi_d, lo_state, hi_state, probes = result
    print(f"\n=== `{label}` ===")
    print(f"{lo_d}: {'PRESENT' if lo_state else 'ABSENT'}")
    print(f"{hi_d}: {'PRESENT' if hi_state else 'ABSENT'}")
    print(f"transition pinned to a 1-day gap in {probes} probes ($0.00)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
