#!/usr/bin/env python3
"""Binary-search the two dates where PR merge signal presence flips.

Two boundaries need pinning:
  * ONSET       — last day pre-June-2025 when `action='merged'` still fires
  * RESTORATION — first day post-stripping when `action='merged'` returns

Algorithm: for each boundary, binary search over the date window using
one BQ probe per iteration. Each probe queries `githubarchive.day.YYYYMMDD`
for the count of PullRequestEvents with `action='merged'`. Signal present
iff count > 0.

  * Onset window: 2023-06-15 → 2025-11-15 (~890 days) → 10 probes
  * Restoration window: 2025-11-15 → 2026-07-26 (~253 days) → 8 probes
  * Total: ~18 probes × ~$0.017/probe = ~$0.31

Guardrails: every probe dry-runs first, prints the bytes+cost estimate,
aborts if either individual probe cost or the running total exceeds
`--max-cost-usd` (default $0.50). Each probe passes
`--maximum_bytes_billed` as a hard cap so a runaway can't overshoot.

The two searches run concurrently (independent windows). All probes and
the final boundaries are appended to `--log-jsonl` for reuse if we later
want to check other signals (state_reason, download_count, etc.) — the
same day-table rows will already be cached in BQ's short-term result
cache, potentially free.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import subprocess
import sys
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Callable, Optional

# BQ on-demand pricing (2026-08).
BQ_PRICE_PER_TIB_USD = 6.25
_TIB = 1024 ** 4

# Anchor dates — known-signal-state days that bracket each unknown boundary.
# ONSET: pre-stripping side is 2023-06-15 (verified via 2023-06 bench),
# post-stripping side is 2025-11-15 (verified via BQ audit 2026-08-02).
# RESTORATION: pre-restoration side is 2025-11-15 (same audit),
# post-restoration side is 2026-07-26 (schema validation report bench).
ANCHOR_ONSET_LO = dt.date(2023, 6, 15)   # signal present
ANCHOR_ONSET_HI = dt.date(2025, 11, 15)  # signal absent
ANCHOR_RESTORE_LO = dt.date(2025, 11, 15)  # signal absent
ANCHOR_RESTORE_HI = dt.date(2026, 7, 26)   # signal present


# ---- BQ query construction --------------------------------------------------

def probe_query(day: dt.date) -> str:
    """SQL that returns the count of PR merges detectable via EITHER signal.

    Covers both era styles: `action='merged'` (post-Dec-2-2025 synthetic
    action) and `pull_request.merged='true'` (pre-June-2025 historical
    signal). Composite = "signal recoverable" — the property that
    actually matters for pr_merged accuracy.
    """
    table = f"githubarchive.day.{day:%Y%m%d}"
    return (
        f"SELECT COUNTIF("
        f"JSON_EXTRACT_SCALAR(payload, '$.action') = 'merged'"
        f" OR JSON_EXTRACT_SCALAR(payload, '$.pull_request.merged') = 'true'"
        f") AS merged_count FROM `{table}` WHERE type = 'PullRequestEvent'"
    )


def _run(cmd: list[str]) -> subprocess.CompletedProcess:
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def dry_run_bytes(query: str, project: str) -> int:
    """Returns bytes BQ would scan without executing."""
    r = _run([
        "bq", f"--project_id={project}", "query", "--use_legacy_sql=false",
        "--format=json", "--dry_run", query,
    ])
    if r.returncode != 0:
        raise RuntimeError(f"dry-run failed: {r.stderr[-500:]}")
    return int(json.loads(r.stdout)["statistics"]["query"]["totalBytesProcessed"])


def bytes_to_usd(n: int) -> float:
    return n / _TIB * BQ_PRICE_PER_TIB_USD


# ---- Budget bookkeeping (shared across threads) -----------------------------

class Budget:
    def __init__(self, cap_usd: float):
        self.cap = cap_usd
        self.spent = 0.0
        self.lock = threading.Lock()

    def try_charge(self, cost: float, label: str) -> None:
        with self.lock:
            if self.spent + cost > self.cap:
                raise RuntimeError(
                    f"budget cap ${self.cap:.2f} would be exceeded by {label} "
                    f"(${cost:.3f}); running total ${self.spent:.3f}"
                )
            self.spent += cost


# ---- Probe with guardrails --------------------------------------------------

def probe_day(day: dt.date, project: str, budget: Budget,
              log_path: Optional[Path]) -> Optional[bool]:
    """Returns True if signal present, False if absent, None if day
    table missing / query failed. Dry-runs first, budgets, then runs."""
    q = probe_query(day)
    try:
        bytes_scanned = dry_run_bytes(q, project)
    except RuntimeError as e:
        # Table probably doesn't exist (missing day).
        print(f"  [{day}] dry-run failed (missing day table?): {e}", flush=True)
        _write_log(log_path, day, None, 0, str(e))
        return None

    cost = bytes_to_usd(bytes_scanned)
    budget.try_charge(cost, f"probe {day}")

    # Cap = 10% headroom over dry-run.
    max_bytes = int(bytes_scanned * 1.1)
    r = _run([
        "bq", f"--project_id={project}", "query", "--use_legacy_sql=false",
        "--format=csv", f"--maximum_bytes_billed={max_bytes}",
        "--max_rows=1", q,
    ])
    if r.returncode != 0:
        print(f"  [{day}] query failed: {r.stderr[-300:]}", flush=True)
        _write_log(log_path, day, None, cost, r.stderr[-300:])
        return None

    # Parse: strip status lines, keep the single data row after 'merged_count'.
    lines = [l for l in r.stdout.splitlines() if l and not l.startswith("Waiting")]
    if len(lines) < 2:
        print(f"  [{day}] unexpected output: {r.stdout[:200]}", flush=True)
        _write_log(log_path, day, None, cost, "no data row")
        return None
    try:
        merged_count = int(lines[-1])
    except ValueError:
        print(f"  [{day}] parse error: {lines[-1]!r}", flush=True)
        _write_log(log_path, day, None, cost, "parse error")
        return None

    present = merged_count > 0
    print(f"  [{day}] merged_count={merged_count:>7} → "
          f"{'PRESENT' if present else 'ABSENT '} (${cost:.3f})", flush=True)
    _write_log(log_path, day, present, cost, None)
    return present


def _write_log(log_path: Optional[Path], day: dt.date,
               present: Optional[bool], cost: float, err: Optional[str]) -> None:
    if log_path is None:
        return
    entry = {"day": day.isoformat(), "signal_present": present,
             "est_cost_usd": round(cost, 4), "error": err}
    with log_path.open("a") as fh:
        fh.write(json.dumps(entry) + "\n")


# ---- Binary search ----------------------------------------------------------

def binary_search_transition(
    lo: dt.date, hi: dt.date, probe: Callable[[dt.date], Optional[bool]],
    name: str,
) -> Optional[tuple[dt.date, dt.date, bool, bool]]:
    """Search [lo, hi] for the day the boolean flips. Returns
    (last_lo_state_day, first_hi_state_day, lo_state, hi_state).
    Skips missing days (probe returns None) by nudging inward."""
    lo_state = probe(lo)
    hi_state = probe(hi)
    if lo_state is None or hi_state is None:
        print(f"[{name}] anchor probe failed, aborting", flush=True)
        return None
    if lo_state == hi_state:
        print(f"[{name}] anchors have same state ({lo_state}) — "
              f"transition not in [{lo}, {hi}]", flush=True)
        return None

    while (hi - lo).days > 1:
        mid = lo + dt.timedelta(days=(hi - lo).days // 2)
        state = probe(mid)
        if state is None:
            # Missing day: try adjacent days moving toward the smaller side.
            candidates = [mid + dt.timedelta(days=d) for d in (1, -1, 2, -2, 3, -3)]
            state = None
            for c in candidates:
                if lo < c < hi:
                    state = probe(c)
                    if state is not None:
                        mid = c
                        break
            if state is None:
                print(f"[{name}] cannot probe around {mid}, aborting", flush=True)
                return None
        if state == lo_state:
            lo = mid
        else:
            hi = mid
    return (lo, hi, lo_state, hi_state)


# ---- Main -------------------------------------------------------------------

def main(argv=None) -> int:
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--project", required=True, help="GCP billing project for bq")
    p.add_argument("--max-cost-usd", type=float, default=0.50,
                   help="hard cap on total spend (default $0.50)")
    p.add_argument("--log-jsonl", default=None,
                   help="append per-probe results to this JSONL file")
    p.add_argument("--boundary", choices=("onset", "restoration", "both"),
                   default="both")
    args = p.parse_args(argv)

    log_path = Path(args.log_jsonl) if args.log_jsonl else None
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)

    budget = Budget(args.max_cost_usd)

    def make_probe():
        return lambda day: probe_day(day, args.project, budget, log_path)

    tasks: dict[str, tuple[dt.date, dt.date]] = {}
    if args.boundary in ("onset", "both"):
        tasks["ONSET"] = (ANCHOR_ONSET_LO, ANCHOR_ONSET_HI)
    if args.boundary in ("restoration", "both"):
        tasks["RESTORATION"] = (ANCHOR_RESTORE_LO, ANCHOR_RESTORE_HI)

    results: dict[str, Optional[tuple]] = {}
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        futures = {
            name: pool.submit(binary_search_transition, lo, hi, make_probe(), name)
            for name, (lo, hi) in tasks.items()
        }
        for name, fut in futures.items():
            try:
                results[name] = fut.result()
            except Exception as e:
                print(f"[{name}] FAILED: {e}", flush=True)
                results[name] = None

    print("\n=== boundaries ===")
    for name, r in results.items():
        if r is None:
            print(f"{name}: could not resolve")
            continue
        lo, hi, lo_state, hi_state = r
        print(f"{name}: transitioned between {lo} ({lo_state}) "
              f"and {hi} ({hi_state}) — 1-day gap")
    print(f"total spend: ${budget.spent:.3f} of ${budget.cap:.2f} cap")
    return 0


if __name__ == "__main__":
    sys.exit(main())
