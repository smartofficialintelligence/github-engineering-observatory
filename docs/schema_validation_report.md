# GH Archive Stream Schema Validation Report

**Date:** 2026-07-30
**Deliverable:** spec §7 Task 3. The archived payload is the source of truth;
everything below is measured from real files, not GitHub documentation.

## Provenance and reproduction

| Sample | Window (UTC) | Bytes (gz) | Events | sha256 (prefix) |
| --- | --- | --- | --- | --- |
| `2026-07-29-15.json.gz` | Wed 15:00–15:59 | 20,476,592 | 160,280 | `e57cfacc` |
| `2026-07-29-3.json.gz` | Wed 03:00–03:59 | 20,627,642 | 165,882 | `4b1d3f0d` |
| `2026-07-26-15.json.gz` | Sun 15:00–15:59 | 21,448,642 | 173,580 | `4663f485` |

**Total: 499,742 events, 16 event types, 1,513 distinct (event_type, field_path) pairs.**

Reproduce with:

```bash
python -m github_observatory.ingestion.download_gharchive \
    2026-07-29-15 2026-07-29-3 2026-07-26-15 --dest <raw_dir>
python -m github_observatory.schema.profile_schema \
    <raw_dir>/2026-07-29-15.json.gz <raw_dir>/2026-07-29-3.json.gz \
    <raw_dir>/2026-07-26-15.json.gz --out-dir artifacts/schema_profile
```

Full machine-readable results: `artifacts/schema_profile/schema_profile.csv`
(spec-required 9-column profile) and `artifacts/schema_profile/profile_summary.json`.
On Databricks, `notebooks/01_download_and_profile.py` writes the same rows to
`github_observatory.bronze.schema_profile`.

---

## 1. Event-type composition (and a major coverage anomaly)

| Event type | Events | Share |
| --- | ---: | ---: |
| PushEvent | 477,433 | 95.54% |
| CreateEvent | 14,576 | 2.92% |
| DeleteEvent | 6,195 | 1.24% |
| PullRequestEvent | 567 | 0.11% |
| IssueCommentEvent | 278 | 0.06% |
| IssuesEvent | 225 | 0.05% |
| WatchEvent | 132 | 0.03% |
| PullRequestReviewCommentEvent | 130 | 0.03% |
| PullRequestReviewEvent | 124 | 0.02% |
| ReleaseEvent | 35 | 0.01% |
| ForkEvent | 32 | 0.01% |
| CommitCommentEvent | 6 | — |
| MemberEvent | 6 | — |
| DiscussionEvent / GollumEvent / PublicEvent | 1 each | — |

**Finding S1 — the 2026 stream is overwhelmingly pushes.** Historical GH Archive
years had PushEvent at roughly half of events with WatchEvent/ForkEvent/PR/issue
events in the thousands per hour. In these samples, stars run at ~44/hour and
PRs at ~189/hour globally, which is far below any plausible real-world rate for
public GitHub. The push volume itself is broad-based, not one bot flooding:
477,433 pushes come from **71,423 distinct actors** across **94,071 distinct
repos**; the top actor (`github-actions[bot]`) is 4.58% and the top-100 actors
together only 8.30%.

Consequence: non-push event classes appear **severely under-sampled at the
source**. Production metrics built on pushes are well supported. Engagement
(stars/forks), PR/issue, and review metrics remain computable but must be
labeled as thin samples of unknown coverage until cross-checked against the
GitHub API (tracked as OQ-1 in `docs/open_questions.md`). Absolute
"ecosystem totals" for those classes must not be published as census counts.

Missing event types (documented upstream, zero in sample):
`ForkApplyEvent`, `SponsorshipEvent`, `TeamAddEvent`, `DownloadEvent`, and —
notably — `GistEvent`. `DiscussionEvent` (not in the classic gharchive docs)
**is** present.

## 2. Top-level schema (identical across all 16 event types)

| Field | Type | Presence | Notes |
| --- | --- | ---: | --- |
| `id` | STRING (numeric) | 100% | globally unique in sample (499,742 distinct, 0 duplicates, 0 missing) |
| `type` | STRING | 100% | 16 values observed |
| `actor` | STRUCT | 100% | `id` BIGINT, `login`, `display_login`, `gravatar_id`, `url`, `avatar_url` always present |
| `repo` | STRUCT | 100% | `id` BIGINT, `name`, `url` — **except one event with empty `repo: {}`** (see Finding S6) |
| `org` | STRUCT | 3.0%–64.5% by type | only present when the repo belongs to an org; never null when present |
| `payload` | STRUCT | 100% | event-type-specific; see §4 |
| `public` | BOOLEAN | 100% | **not always `true`** — see Finding S6 |
| `created_at` | STRING ISO-8601 `...Z` | 100% | cast to TIMESTAMP in Silver |

All three files contain events only within their nominal hour (0 mismatches),
with first/last events at :00:00–:59:59 — hour files are clean partitions.
0 malformed JSON lines in 499,742.

## 3. Contract-field verdicts (spec §7 explicit list)

| Contract field | Verdict | Observed type | Where present (rate) |
| --- | --- | --- | --- |
| `id` | ✅ PRESENT | STRING | all 16 types (100%) |
| `type` | ✅ PRESENT | STRING | all (100%) |
| `actor.id` | ✅ PRESENT | BIGINT | all (100%) |
| `actor.login` | ✅ PRESENT | STRING | all (100%) |
| `repo.id` | ⚠️ PRESENT | BIGINT | 100% except ForkEvent 31/32 (Finding S6) |
| `repo.name` | ⚠️ PRESENT | STRING | same as `repo.id` |
| `org.id` | ✅ PRESENT (optional) | BIGINT | 14 types, 3.0%–64.5% (org-owned repos only) |
| `org.login` | ✅ PRESENT (optional) | STRING | same as `org.id` |
| `public` | ✅ PRESENT | BOOLEAN | all (100%); value `false` occurs (S6) |
| `created_at` | ✅ PRESENT | STRING | all (100%) |
| `payload.action` | ✅ PRESENT | STRING | 11 types (100% within each); **absent** on Push/Create/Delete/Gollum/Public |
| `payload.ref` | ✅ PRESENT | STRING | Push/Create/Delete (100%) |
| `payload.ref_type` | ✅ PRESENT | STRING | Create/Delete (100%) |
| `payload.before` | ✅ PRESENT | STRING sha | PushEvent (100%) |
| `payload.head` | ✅ PRESENT | STRING sha | PushEvent (100%) |
| `payload.push_id` | ✅ PRESENT | BIGINT | PushEvent (100%); **not unique** (S5) |
| `payload.issue.id` | ✅ PRESENT | BIGINT | Issues/IssueComment (100%) |
| `payload.issue.number` | ✅ PRESENT | BIGINT | Issues/IssueComment (100%) |
| `payload.pull_request.id` | ✅ PRESENT | BIGINT | PR/PRReview/PRReviewComment (100%) |
| `payload.pull_request.number` | ✅ PRESENT | BIGINT | same (100%); PullRequestEvent also has top-level `payload.number` |
| `payload.pull_request.state` | ❌ **ABSENT** | — | nowhere in 821 PR-carrying events |
| `payload.pull_request.merged` | ❌ **ABSENT** | — | nowhere; replaced by `action = "merged"` (Finding S2) |
| `payload.review.id` | ✅ PRESENT | BIGINT | PullRequestReviewEvent (100%) |
| `payload.review.state` | ✅ PRESENT | STRING | PullRequestReviewEvent (100%): `commented` 85, `approved` 31, `changes_requested` 7, `dismissed` 1 |
| `payload.release.id` | ✅ PRESENT | BIGINT | ReleaseEvent (100%) |
| `payload.release.tag_name` | ✅ PRESENT | STRING | ReleaseEvent (100%) |
| `payload.release.prerelease` | ✅ PRESENT | BOOLEAN | ReleaseEvent (100%) |
| `payload.forkee.id` | ✅ PRESENT | BIGINT | ForkEvent (100%) |
| `payload.forkee.full_name` | ✅ PRESENT | STRING | ForkEvent (100%) |

## 4. Payload shapes per event type

**Finding S2 — PullRequestEvent payloads are radically slimmed, but the merge
signal moved into `action`.** The 2026 `payload.pull_request` object contains
**only** `id`, `number`, `url`, `base{ref, sha, repo{id, name, url}}`,
`head{ref, sha, repo{id, name, url}}`. Gone relative to historical payloads:
`state`, `merged`, `merged_at`, `title`, `body`, `user`, `created_at`,
`closed_at`, `additions`, `deletions`, `changed_files`, and everything else.
In exchange, `payload.action` now has a **dedicated `merged` value**
(historically merges were `closed` + `merged=true`):

```
PullRequestEvent actions: opened 223, merged 172, labeled 145, closed 12,
                          unlabeled 12, assigned 2, reopened 1
```

`prs_merged` therefore = count(action `merged`); `closed` appears to mean
closed-without-merge (needs confirmation on more data; the old
closed+merged encoding is absent here). `labeled`/`unlabeled`/`assigned`
actions in the public stream are new and matter for lifecycle logic — an
event per (repo, PR number) is no longer mostly open/close transitions.

**Finding S3 — PushEvent carries no commit information whatsoever.** The
entire 2026 push payload is `push_id`, `ref`, `before`, `head`,
`repository_id`. Across 477,433 pushes: 0 have `commits`, 0 have `size` or
`distinct_size`. The spec's warning is confirmed in the strongest form:
**pushes are the only dependable production unit; commit counts and lines of
code are unobservable in-stream** (spec §3.1 stands). `payload.repository_id`
equals `repo.id` on all 477,433 pushes (verified) — treat as redundant.

**Full/rich payloads (near-historical shape) still exist for:**

* `IssuesEvent` / `IssueCommentEvent` — complete issue object: `state`,
  `state_reason` (`completed`/`not_planned`/`duplicate`/`reopened`),
  `created_at`, `closed_at`, `comments`, `body`, labels, reactions, plus new
  2026 fields (`issue_dependencies_summary`, `issue_field_values`,
  `parent_issue_url`, `type`). On IssueCommentEvent, 67.6% of issues carry
  `issue.pull_request{merged_at,...}` — i.e. most "issue comments" are PR
  comments; the flag distinguishes them, and `merged_at` leaks merge
  timestamps for commented PRs.
* `PullRequestReviewEvent` — full `review` object: `state`, `submitted_at`,
  `commit_id`, `body`. Actions: `created` 110, `updated` 14.
* `PullRequestReviewCommentEvent` — full `comment` object incl. `diff_hunk`,
  `path`, `in_reply_to_id` (26.9%).
* `ReleaseEvent` — full `release` object: `tag_name`, `prerelease`, `draft`,
  `published_at`, assets with `download_count` (a real adoption signal),
  plus new `immutable` field.
* `ForkEvent` — full `forkee` repo object: `stargazers_count`, `language`,
  `license`, `topics`, `created_at`, `pushed_at`, etc.
* `CreateEvent`/`DeleteEvent` — `ref`, `ref_type`, `pusher_type`
  (+ `master_branch`, `description` on Create) and new `full_ref`.
* `WatchEvent` — `{action: "started"}` only (as always).
* `MemberEvent`, `GollumEvent`, `CommitCommentEvent`, `DiscussionEvent`
  (full discussion object incl. `state`, `category`), `PublicEvent`
  (empty payload).

## 5. Documented-but-absent vs present-but-undocumented

**Documented (in spec/GitHub docs) but absent in the 2026 stream:**

| Field | Impact |
| --- | --- |
| `payload.pull_request.state` / `.merged` | merge/closure state now derived from `payload.action` |
| `payload.size` / `.distinct_size` / `.commits[]` (PushEvent) | no commit counts, shas, messages, or author emails in-stream |
| PR metadata (`title`, `user`, `created_at`, `closed_at`, diff stats) | PR latency/size metrics need enrichment or issue-object joins |
| `payload.pull_request.merged_by`, review `dismissed` action | review-dismissal rework proxy limited to `review.state='dismissed'` |

**Present but undocumented (relative to spec §7 list and classic event docs):**

| Field | Observation |
| --- | --- |
| `payload.action = "merged"` (PullRequestEvent) | new action value; primary merge signal |
| `payload.action = "labeled"/"unlabeled"/"assigned"/"unassigned"` on PR/Issues | new in public stream; inflates raw PR/issue event counts if not filtered |
| `payload.action = "forked"` (ForkEvent) | ForkEvent historically had no action |
| `payload.full_ref` (Create/Delete) | e.g. `refs/heads/main` alongside `ref` |
| `payload.repository_id` (PushEvent) | BIGINT, == `repo.id` in all sampled events |
| `payload.issue.issue_dependencies_summary` (+ `sub_issues_summary`) | new issue-graph structs |
| `payload.issue.state_reason` | closure taxonomy — valuable for Rework |
| `payload.release.immutable`, `assets[].digest` | new release provenance fields |
| `payload.forkee.pull_request_creation_policy` | new repo policy field |
| `*.user_view_type` on user objects | new field on actor-like structs |

## 6. Keys, linkage, and integrity

| Candidate key | Result |
| --- | --- |
| `id` (event) | **Primary key confirmed**: 499,742 distinct / 0 duplicates / 0 missing across 3 files |
| `payload.push_id` | ⚠️ Finding S5: **not unique** — 477,433 push events but 477,405 distinct push_ids (28 repeats). Dedupe on `id`, never `push_id` |
| `repo.id` + `payload.pull_request.number` | 803 distinct keys, **0 keys mapping to >1 `pull_request.id`** — safe PR lifecycle key |
| `repo.id` + `payload.issue.number` | 502 distinct keys, **0 collisions** against `issue.id` — safe issue lifecycle key |
| `payload.review.id` | 123 distinct in 124 events (unique per review event incl. 1 updated) |
| `payload.release.id` | 35 distinct in 35 events |

Issues and PRs share one number space per repo (PR comments arrive as
IssueCommentEvent with `issue.pull_request` set), so issue-lifecycle logic
must partition on that flag before joining `(repo_id, number)`.

**Finding S6 — one non-public event leaked into the sample:** a ForkEvent with
`"public": false`, an **empty `repo: {}`**, and `forkee.private: true`
(a private fork of a repo). Bronze must tolerate missing `repo.id`; Silver
should route rows with `public = false` or null `repo_id` to quarantine/flag
rather than assume the invariant.

## 7. Type consistency

* No scalar field shows conflicting types across events — all BIGINT/STRING/
  BOOLEAN assignments in §3 are stable across all three hours.
* The only type unions are benign array cases: fields like `assignees`,
  `release.assets`, `topics` oscillate between `ARRAY<EMPTY>` and
  `ARRAY<STRUCT>` (element type unknowable when empty).
* Fields observed **only** as JSON null in-sample (type undecidable):
  `forkee.mirror_url`, `pages[].summary`, `comment.line/path/position`
  (CommitCommentEvent), several `discussion.answer_*` fields. Store as
  STRING in Silver until a non-null observation says otherwise.
* IDs: all `*.id` fields are BIGINT except the top-level event `id`
  (numeric STRING) — keep event_id as STRING in Bronze/Silver (values exceed
  2^32 and GitHub does not guarantee numeric stability).

## 8. Consequences adopted for downstream design

1. Bronze `events_raw` keeps the eight top-level columns as-is
   (`payload` as raw JSON string), dedupes on `id`, and never requires
   `repo.id`/`public = true` (S6).
2. Silver drops planned columns `pr_state`, `pr_merged` (absent), adds
   `payload_action`-derived lifecycle status; `prs_merged` = action `merged`
   (S2). `issue_state`, `issue_state_reason`, `review_state` are available
   and typed STRING.
3. Production metrics center on pushes + PR/issue open/close/merge counts;
   no commit-count metrics (S3).
4. Engagement metrics carry an explicit coverage caveat; forecast targets
   start with `total_events` and `push_events`, which the stream supports
   robustly (S1).
5. Actor classification: `[bot]`-suffix logins mark 6.13% of events
   (30,612/499,742) — a lower bound; heuristics beyond the suffix needed
   later (`actor.type` does not exist in-stream).
6. `DiscussionEvent` added to the known-type whitelist; unknown types must
   not fail ingestion (forward compatibility).

## 9. Spec §20 questions answered by this sample

| Question | Answer |
| --- | --- |
| Which nested payload fields remain available in 2026? | See §4; slim PR/Push payloads, rich issue/review/release/fork payloads |
| Are commit counts present in any PushEvent payloads? | **No — 0 of 477,433** |
| Is `pull_request.merged` consistently present? | **No — never**; use `action='merged'` |
| Are review states available and stable? | Yes: all four states observed, 100% presence |
| Can issue and PR lifecycle events be linked reliably? | Yes: `(repo_id, number)` collision-free; partition issues vs PRs via `issue.pull_request` |
| How much bot classification from the stream alone? | `[bot]` suffix only (6.13% of events); no `actor.type` field |

Remaining §20 questions are tracked in `docs/open_questions.md`.
