# GH Archive Schema Drift Timeline

**Generated from 139 sampled hours** (2015-01-15-12 → 2026-07-15-12), one hour per month.

Produced by `pipelines/schema_drift/scan_decade.py` + `build_timeline.py`. Source: raw hourly files from data.gharchive.org (free); profiling via the same `StreamProfiler` that produced `docs/schema_validation_report.md`.

**Sampling caveat**: one hour per month. A field present in fewer than ~1 in 100k events may be missed in a given month, so "first seen" means "first sampled", not "introduced on". Boundary dates for the PR merge signal were pinned exactly by binary search — see `docs/open_questions.md` OQ-1.

## Event volume and type mix

| Sample hour | Events | Types | Top type | Top share |
| --- | ---: | ---: | --- | ---: |
| 2015-01-15-12 | 21,062 | 14 | PushEvent | 50.3% |
| 2015-02-15-12 | 13,885 | 14 | PushEvent | 53.4% |
| 2015-03-15-12 | 17,367 | 14 | PushEvent | 48.7% |
| 2015-04-15-12 | 27,861 | 14 | PushEvent | 48.0% |
| 2015-05-15-12 | 25,104 | 14 | PushEvent | 49.8% |
| 2015-06-15-12 | 28,235 | 14 | PushEvent | 46.9% |
| 2015-07-15-12 | 28,074 | 14 | PushEvent | 46.9% |
| 2015-08-15-12 | 17,476 | 14 | PushEvent | 42.0% |
| 2015-09-15-12 | 32,981 | 14 | PushEvent | 47.2% |
| 2015-10-15-12 | 33,312 | 14 | PushEvent | 47.5% |
| 2015-11-15-12 | 19,729 | 14 | PushEvent | 53.3% |
| 2015-12-15-12 | 34,479 | 14 | PushEvent | 50.4% |
| 2016-01-15-12 | 31,736 | 14 | PushEvent | 51.4% |
| 2016-02-15-12 | 38,627 | 14 | PushEvent | 46.8% |
| 2016-03-15-12 | 43,676 | 14 | PushEvent | 51.1% |
| 2016-04-15-12 | 43,592 | 14 | PushEvent | 49.2% |
| 2016-05-15-12 | 27,952 | 14 | PushEvent | 56.0% |
| 2016-06-15-12 | 43,566 | 14 | PushEvent | 50.2% |
| 2016-07-15-12 | 38,705 | 14 | PushEvent | 49.3% |
| 2016-08-15-12 | 36,439 | 14 | PushEvent | 49.2% |
| 2016-09-15-12 | 42,667 | 14 | PushEvent | 48.4% |
| 2016-10-15-12 | 28,021 | 14 | PushEvent | 54.0% |
| 2016-11-15-12 | 46,838 | 14 | PushEvent | 50.8% |
| 2016-12-15-12 | 45,157 | 14 | PushEvent | 52.5% |
| 2017-01-15-12 | 30,302 | 14 | PushEvent | 52.6% |
| 2017-02-15-12 | 53,115 | 14 | PushEvent | 51.8% |
| 2017-03-15-12 | 56,310 | 14 | PushEvent | 50.4% |
| 2017-04-15-12 | 31,189 | 14 | PushEvent | 53.1% |
| 2017-05-15-12 | 58,115 | 14 | PushEvent | 50.1% |
| 2017-06-15-12 | 53,828 | 14 | PushEvent | 51.9% |
| 2017-07-15-12 | 31,043 | 14 | PushEvent | 56.1% |
| 2017-08-15-12 | 52,088 | 14 | PushEvent | 52.3% |
| 2017-09-15-12 | 56,035 | 14 | PushEvent | 52.0% |
| 2017-10-15-12 | 40,732 | 14 | PushEvent | 54.4% |
| 2017-11-15-12 | 61,253 | 14 | PushEvent | 51.9% |
| 2017-12-15-12 | 57,472 | 14 | PushEvent | 52.3% |
| 2018-01-15-12 | 63,463 | 14 | PushEvent | 49.0% |
| 2018-02-15-12 | 58,752 | 14 | PushEvent | 52.0% |
| 2018-03-15-12 | 76,330 | 14 | PushEvent | 53.0% |
| 2018-04-15-12 | 43,895 | 14 | PushEvent | 57.5% |
| 2018-05-15-12 | 69,672 | 14 | PushEvent | 52.3% |
| 2018-06-15-12 | 63,119 | 14 | PushEvent | 52.1% |
| 2018-07-15-12 | 36,212 | 14 | PushEvent | 57.0% |
| 2018-08-15-12 | 58,616 | 14 | PushEvent | 51.7% |
| 2018-09-15-12 | 35,980 | 14 | PushEvent | 56.5% |
| 2018-10-15-12 | 72,805 | 14 | PushEvent | 49.6% |
| 2018-11-15-12 | 68,678 | 14 | PushEvent | 52.6% |
| 2018-12-15-12 | 40,091 | 14 | PushEvent | 56.1% |
| 2019-01-15-12 | 67,145 | 14 | PushEvent | 52.1% |
| 2019-02-15-12 | 70,462 | 14 | PushEvent | 50.4% |
| 2019-03-15-12 | 80,480 | 14 | PushEvent | 50.9% |
| 2019-04-15-12 | 91,017 | 14 | PushEvent | 49.9% |
| 2019-05-15-12 | 90,919 | 14 | PushEvent | 50.5% |
| 2019-06-15-12 | 46,408 | 14 | PushEvent | 55.8% |
| 2019-07-15-12 | 90,203 | 14 | PushEvent | 47.9% |
| 2019-08-15-12 | 76,303 | 14 | PushEvent | 50.1% |
| 2019-09-15-12 | 53,882 | 14 | PushEvent | 53.8% |
| 2019-10-15-12 | 100,538 | 14 | PushEvent | 48.3% |
| 2019-11-15-12 | 84,154 | 14 | PushEvent | 49.7% |
| 2019-12-15-12 | 57,324 | 14 | PushEvent | 54.5% |
| 2020-01-15-12 | 90,940 | 14 | PushEvent | 48.5% |
| 2020-02-15-12 | 63,454 | 14 | PushEvent | 54.6% |
| 2020-03-15-12 | 96,236 | 14 | PushEvent | 42.0% |
| 2020-04-15-12 | 127,641 | 14 | PushEvent | 51.5% |
| 2020-05-15-12 | 116,015 | 14 | PushEvent | 50.8% |
| 2020-06-15-12 | 121,300 | 14 | PushEvent | 47.2% |
| 2020-07-15-12 | 111,604 | 14 | PushEvent | 50.1% |
| 2020-08-15-12 | 71,824 | 14 | PushEvent | 56.6% |
| 2020-09-15-12 | 131,774 | 15 | PushEvent | 48.4% |
| 2020-10-15-12 | 146,298 | 15 | PushEvent | 52.3% |
| 2020-11-15-12 | 88,072 | 15 | PushEvent | 58.0% |
| 2020-12-15-12 | 135,096 | 15 | PushEvent | 49.6% |
| 2021-01-15-12 | 117,780 | 15 | PushEvent | 49.5% |
| 2021-02-15-12 | 128,508 | 15 | PushEvent | 48.0% |
| 2021-03-15-12 | 149,704 | 15 | PushEvent | 49.5% |
| 2021-04-15-12 | 154,260 | 15 | PushEvent | 52.9% |
| 2021-05-15-12 | 99,494 | 15 | PushEvent | 55.7% |
| 2021-06-15-12 | 150,065 | 15 | PushEvent | 51.3% |
| 2021-07-15-12 | 149,210 | 15 | PushEvent | 50.4% |
| 2021-08-15-12 | 105,158 | 15 | PushEvent | 57.5% |
| 2021-09-15-12 | 161,201 | 15 | PushEvent | 52.1% |
| 2021-10-15-12 | 45,897 | 15 | PushEvent | 57.4% |
| 2021-11-15-12 | 172,444 | 15 | PushEvent | 51.0% |
| 2021-12-15-12 | 165,842 | 15 | PushEvent | 53.8% |
| 2022-01-15-12 | 115,999 | 15 | PushEvent | 56.5% |
| 2022-02-15-12 | 170,199 | 15 | PushEvent | 51.4% |
| 2022-03-15-12 | 168,608 | 15 | PushEvent | 53.5% |
| 2022-04-15-12 | 156,079 | 15 | PushEvent | 57.3% |
| 2022-05-15-12 | 127,909 | 15 | PushEvent | 63.1% |
| 2022-06-15-12 | 158,716 | 15 | PushEvent | 55.2% |
| 2022-07-15-12 | 163,375 | 15 | PushEvent | 57.7% |
| 2022-08-15-12 | 186,842 | 15 | PushEvent | 53.6% |
| 2022-09-15-12 | 206,070 | 15 | PushEvent | 59.0% |
| 2022-10-15-12 | 162,324 | 15 | PushEvent | 61.9% |
| 2022-11-15-12 | 204,296 | 15 | PushEvent | 57.6% |
| 2022-12-15-12 | 209,412 | 15 | PushEvent | 60.7% |
| 2023-01-15-12 | 178,015 | 15 | PushEvent | 70.6% |
| 2023-02-15-12 | 225,052 | 15 | PushEvent | 58.1% |
| 2023-03-15-12 | 234,682 | 15 | PushEvent | 61.1% |
| 2023-04-15-12 | 193,529 | 15 | PushEvent | 72.3% |
| 2023-05-15-12 | 205,134 | 15 | PushEvent | 60.5% |
| 2023-06-15-12 | 198,897 | 15 | PushEvent | 61.9% |
| 2023-07-15-12 | 176,989 | 15 | PushEvent | 70.0% |
| 2023-08-15-12 | 227,279 | 15 | PushEvent | 67.8% |
| 2023-09-15-12 | 239,056 | 15 | PushEvent | 65.2% |
| 2023-10-15-12 | 206,163 | 15 | PushEvent | 73.7% |
| 2023-11-15-12 | 234,908 | 15 | PushEvent | 64.5% |
| 2023-12-15-12 | 228,690 | 15 | PushEvent | 67.2% |
| 2024-01-15-12 | 286,864 | 15 | PushEvent | 69.8% |
| 2024-02-15-12 | 284,638 | 15 | PushEvent | 71.0% |
| 2024-03-15-12 | 284,264 | 15 | PushEvent | 64.9% |
| 2024-04-15-12 | 297,288 | 15 | PushEvent | 64.0% |
| 2024-05-15-12 | 277,664 | 15 | PushEvent | 66.0% |
| 2024-06-15-12 | 233,373 | 15 | PushEvent | 74.2% |
| 2024-07-15-12 | 272,658 | 15 | PushEvent | 64.3% |
| 2024-08-15-12 | 257,182 | 15 | PushEvent | 69.1% |
| 2024-09-15-12 | 221,263 | 15 | PushEvent | 75.9% |
| 2024-10-15-12 | 279,315 | 15 | PushEvent | 63.0% |
| 2024-11-15-12 | 260,345 | 15 | PushEvent | 67.3% |
| 2024-12-15-12 | 229,669 | 15 | PushEvent | 75.7% |
| 2025-01-15-12 | 270,553 | 15 | PushEvent | 67.0% |
| 2025-02-15-12 | 253,922 | 15 | PushEvent | 76.1% |
| 2025-03-15-12 | 265,409 | 15 | PushEvent | 76.5% |
| 2025-04-15-12 | 276,552 | 15 | PushEvent | 64.9% |
| 2025-05-15-12 | 260,758 | 15 | PushEvent | 65.5% |
| 2025-06-15-12 | 167,710 | 15 | PushEvent | 73.3% |
| 2025-07-15-12 | 170,233 | 15 | PushEvent | 65.1% |
| 2025-08-15-12 | 168,873 | 15 | PushEvent | 65.7% |
| 2025-09-15-12 | 174,933 | 15 | PushEvent | 66.7% |
| 2025-10-15-12 | 156,052 | 16 | PushEvent | 74.7% |
| 2025-11-15-12 | 157,410 | 16 | PushEvent | 77.2% |
| 2025-12-15-12 | 156,113 | 16 | PushEvent | 72.1% |
| 2026-01-15-12 | 153,766 | 16 | PushEvent | 75.6% |
| 2026-02-15-12 | 153,474 | 16 | PushEvent | 74.6% |
| 2026-03-15-12 | 164,927 | 16 | PushEvent | 79.7% |
| 2026-04-15-12 | 156,006 | 16 | PushEvent | 81.4% |
| 2026-05-15-12 | 87,307 | 16 | PushEvent | 83.4% |
| 2026-06-15-12 | 162,822 | 15 | PushEvent | 84.9% |
| 2026-07-15-12 | 169,372 | 15 | PushEvent | 91.8% |

## Event types: first and last sampled

| Event type | First sampled | Last sampled | Hours seen |
| --- | --- | --- | ---: |
| `CommitCommentEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `CreateEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `DeleteEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `DiscussionEvent` | 2025-10-15-12 | 2026-05-15-12 | 8/139 |
| `ForkEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `GollumEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `IssueCommentEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `IssuesEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `MemberEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `PublicEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `PullRequestEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `PullRequestReviewCommentEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `PullRequestReviewEvent` | 2020-09-15-12 | 2026-07-15-12 | 71/139 |
| `PushEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `ReleaseEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |
| `WatchEvent` | 2015-01-15-12 | 2026-07-15-12 | 139/139 |

## Contract-relevant field availability

Fields the pipeline depends on (or once depended on). `Hours seen` counts sampled hours where the field appeared at least once.

| Event type | Field | First | Last | Hours seen |
| --- | --- | --- | --- | ---: |
| `ForkEvent` | `payload.forkee.stargazers_count` | 2015-01-15-12 | 2026-05-15-12 | 136/139 |
| `IssueCommentEvent` | `payload.issue.state_reason` | 2022-06-15-12 | 2026-07-15-12 | 50/139 |
| `IssuesEvent` | `payload.issue.state_reason` | 2022-06-15-12 | 2026-07-15-12 | 50/139 |
| `PullRequestEvent` | `payload.pull_request.additions` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.body` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.changed_files` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.deletions` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.merged` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.merged_at` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.state` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestEvent` | `payload.pull_request.title` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestReviewCommentEvent` | `payload.pull_request.body` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestReviewCommentEvent` | `payload.pull_request.merged_at` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestReviewCommentEvent` | `payload.pull_request.state` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestReviewCommentEvent` | `payload.pull_request.title` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PullRequestReviewEvent` | `payload.pull_request.body` | 2020-09-15-12 | 2025-09-15-12 | 61/139 |
| `PullRequestReviewEvent` | `payload.pull_request.merged_at` | 2020-09-15-12 | 2025-09-15-12 | 61/139 |
| `PullRequestReviewEvent` | `payload.pull_request.state` | 2020-09-15-12 | 2025-09-15-12 | 61/139 |
| `PullRequestReviewEvent` | `payload.pull_request.title` | 2020-09-15-12 | 2025-09-15-12 | 61/139 |
| `PushEvent` | `payload.commits` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PushEvent` | `payload.distinct_size` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `PushEvent` | `payload.size` | 2015-01-15-12 | 2025-09-15-12 | 129/139 |
| `ReleaseEvent` | `payload.release.assets` | 2015-01-15-12 | 2026-05-15-12 | 137/139 |

## Fields that were removed

A field counts as *carried* by an event type when it appears on ≥50% of that type's events in an hour, and an hour is only judged when the type had ≥30 events. Both filters matter: without the volume floor a rare type like DiscussionEvent looks removed whenever it misses a sample, and without the rate threshold optional fields like `org` (present only for org-owned repos) look intermittent.

Removed = carried in ≥3 judged hours, then absent from every judged hour since. `Last carried` is the final sample in which it appeared; the true removal date lies between that and the next judged hour.

| Event type | Field | First carried | Last carried | Hours carried |
| --- | --- | --- | --- | ---: |
| `ForkEvent` | `payload.forkee.public` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.comments` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.comments.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.commits` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.commits.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.html` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.html.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.issue` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.issue.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.review_comment` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.review_comment.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.review_comments` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.review_comments.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.self` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.self.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.statuses` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request._links.statuses.href` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request.additions` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request.assignee` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request.assignees` | 2016-07-15-12 | 2025-09-15-12 | 111 |
| `PullRequestEvent` | `payload.pull_request.base.label` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request.base.repo.archive_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.assignees_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.blobs_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.branches_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.clone_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.collaborators_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.comments_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.commits_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.compare_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.contents_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.contributors_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.created_at` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.default_branch` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.deployments_url` | 2016-02-15-12 | 2025-09-15-12 | 115 |
| `PullRequestEvent` | `payload.pull_request.base.repo.description` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.downloads_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.events_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.fork` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.forks` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.forks_count` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.forks_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.full_name` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `PullRequestEvent` | `payload.pull_request.base.repo.git_commits_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.git_refs_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.git_tags_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.git_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.has_downloads` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.has_issues` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.has_pages` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.has_wiki` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.homepage` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.hooks_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.html_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.issue_comment_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.issue_events_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.issues_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.keys_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.labels_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.language` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.languages_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.merges_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.milestones_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.mirror_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.notifications_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.open_issues` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.open_issues_count` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.avatar_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.events_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.followers_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.following_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.gists_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.gravatar_id` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.html_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.id` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.login` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.organizations_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| `PullRequestEvent` | `payload.pull_request.base.repo.owner.received_events_url` | 2015-01-15-12 | 2025-09-15-12 | 128 |
| … | *751 more truncated* | | | |

## Fields that appeared

Absent from ≥2 judged hours at the start, then carried through to the most recent judged hour. The true introduction date lies between the last judged hour without it and `First carried`.

| Event type | Field | First carried | Hours carried |
| --- | --- | --- | ---: |
| `CommitCommentEvent` | `actor.display_login` | 2016-06-15-12 | 118 |
| `CommitCommentEvent` | `payload.action` | 2025-10-15-12 | 6 |
| `CommitCommentEvent` | `payload.comment.node_id` | 2018-06-15-12 | 94 |
| `CommitCommentEvent` | `payload.comment.reactions` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.+1` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.-1` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.confused` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.eyes` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.heart` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.hooray` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.laugh` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.rocket` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.total_count` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.reactions.url` | 2021-10-15-12 | 54 |
| `CommitCommentEvent` | `payload.comment.user.node_id` | 2018-06-15-12 | 94 |
| `CommitCommentEvent` | `payload.comment.user.user_view_type` | 2024-11-15-12 | 17 |
| `CreateEvent` | `actor.display_login` | 2016-06-15-12 | 122 |
| `CreateEvent` | `payload.full_ref` | 2025-10-15-12 | 10 |
| `DeleteEvent` | `actor.display_login` | 2016-06-15-12 | 122 |
| `DeleteEvent` | `payload.full_ref` | 2025-10-15-12 | 10 |
| `ForkEvent` | `actor.display_login` | 2016-06-15-12 | 120 |
| `ForkEvent` | `payload.action` | 2025-10-15-12 | 8 |
| `ForkEvent` | `payload.forkee.allow_forking` | 2021-09-15-12 | 56 |
| `ForkEvent` | `payload.forkee.archived` | 2017-11-15-12 | 102 |
| `ForkEvent` | `payload.forkee.deployments_url` | 2016-02-15-12 | 123 |
| `ForkEvent` | `payload.forkee.disabled` | 2019-04-15-12 | 85 |
| `ForkEvent` | `payload.forkee.has_discussions` | 2022-11-15-12 | 42 |
| `ForkEvent` | `payload.forkee.has_projects` | 2017-04-15-12 | 109 |
| `ForkEvent` | `payload.forkee.has_pull_requests` | 2026-02-15-12 | 4 |
| `ForkEvent` | `payload.forkee.is_template` | 2021-10-15-12 | 55 |
| `ForkEvent` | `payload.forkee.license` | 2017-12-15-12 | 101 |
| `ForkEvent` | `payload.forkee.node_id` | 2018-06-15-12 | 96 |
| `ForkEvent` | `payload.forkee.owner.node_id` | 2018-06-15-12 | 95 |
| `ForkEvent` | `payload.forkee.owner.user_view_type` | 2024-11-15-12 | 19 |
| `ForkEvent` | `payload.forkee.pull_request_creation_policy` | 2026-02-15-12 | 4 |
| `ForkEvent` | `payload.forkee.topics` | 2021-10-15-12 | 55 |
| `ForkEvent` | `payload.forkee.visibility` | 2021-10-15-12 | 55 |
| `ForkEvent` | `payload.forkee.web_commit_signoff_required` | 2022-07-15-12 | 46 |
| `GollumEvent` | `actor.display_login` | 2016-06-15-12 | 118 |
| `IssueCommentEvent` | `actor.display_login` | 2016-06-15-12 | 122 |
| `IssueCommentEvent` | `payload.comment.node_id` | 2018-06-15-12 | 98 |
| `IssueCommentEvent` | `payload.comment.performed_via_github_app` | 2020-08-15-12 | 72 |
| `IssueCommentEvent` | `payload.comment.reactions` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.+1` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.-1` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.confused` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.eyes` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.heart` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.hooray` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.laugh` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.rocket` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.total_count` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.reactions.url` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.comment.user.node_id` | 2018-06-15-12 | 98 |
| `IssueCommentEvent` | `payload.comment.user.user_view_type` | 2024-11-15-12 | 21 |
| `IssueCommentEvent` | `payload.issue.active_lock_reason` | 2020-06-15-12 | 74 |
| `IssueCommentEvent` | `payload.issue.assignees` | 2016-07-15-12 | 121 |
| `IssueCommentEvent` | `payload.issue.draft` | 2021-11-15-12 | 53 |
| `IssueCommentEvent` | `payload.issue.node_id` | 2018-06-15-12 | 98 |
| `IssueCommentEvent` | `payload.issue.performed_via_github_app` | 2020-08-15-12 | 72 |
| `IssueCommentEvent` | `payload.issue.pull_request` | 2021-01-15-12 | 61 |
| `IssueCommentEvent` | `payload.issue.pull_request.diff_url` | 2021-01-15-12 | 61 |
| `IssueCommentEvent` | `payload.issue.pull_request.html_url` | 2021-01-15-12 | 61 |
| `IssueCommentEvent` | `payload.issue.pull_request.merged_at` | 2021-11-15-12 | 53 |
| `IssueCommentEvent` | `payload.issue.pull_request.patch_url` | 2021-01-15-12 | 61 |
| `IssueCommentEvent` | `payload.issue.pull_request.url` | 2021-01-15-12 | 61 |
| `IssueCommentEvent` | `payload.issue.reactions` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.+1` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.-1` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.confused` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.eyes` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.heart` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.hooray` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.laugh` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.rocket` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.total_count` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.reactions.url` | 2021-10-15-12 | 58 |
| `IssueCommentEvent` | `payload.issue.repository_url` | 2016-02-15-12 | 126 |
| `IssueCommentEvent` | `payload.issue.state_reason` | 2022-06-15-12 | 50 |
| `IssueCommentEvent` | `payload.issue.timeline_url` | 2021-10-15-12 | 58 |
| … | *102 more truncated* | | |

## Enum value drift

Distinct values observed for enum-like fields, with the sampled window in which each value appeared. This is where action-value changes (e.g. `merged`) and closure-reason additions show up.

### `CommitCommentEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `created` | 2025-10-15-12 | 2026-07-15-12 | 10 |

### `CreateEvent` → `payload.ref_type`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `branch` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `repository` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `tag` | 2015-01-15-12 | 2025-09-15-12 | 129 |

### `DeleteEvent` → `payload.ref_type`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `branch` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `tag` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `DiscussionEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `created` | 2025-10-15-12 | 2026-05-15-12 | 8 |

### `ForkEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `forked` | 2025-10-15-12 | 2026-07-15-12 | 10 |

### `IssueCommentEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `created` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `IssueCommentEvent` → `payload.issue.state`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `closed` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `open` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `IssueCommentEvent` → `payload.issue.state_reason`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `completed` | 2022-06-15-12 | 2026-07-15-12 | 50 |
| `duplicate` | 2024-12-15-12 | 2026-04-15-12 | 17 |
| `not_planned` | 2022-06-15-12 | 2026-07-15-12 | 50 |
| `reopened` | 2022-06-15-12 | 2026-07-15-12 | 50 |

### `IssuesEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `assigned` | 2025-10-15-12 | 2026-07-15-12 | 10 |
| `closed` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `labeled` | 2025-10-15-12 | 2026-07-15-12 | 10 |
| `opened` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `reopened` | 2015-01-15-12 | 2026-05-15-12 | 137 |
| `unassigned` | 2025-10-15-12 | 2026-06-15-12 | 8 |
| `unlabeled` | 2025-10-15-12 | 2026-07-15-12 | 10 |

### `IssuesEvent` → `payload.issue.state`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `closed` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `open` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `IssuesEvent` → `payload.issue.state_reason`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `completed` | 2022-06-15-12 | 2026-07-15-12 | 50 |
| `duplicate` | 2024-12-15-12 | 2026-06-15-12 | 19 |
| `not_planned` | 2022-06-15-12 | 2026-07-15-12 | 50 |
| `reopened` | 2022-06-15-12 | 2026-06-15-12 | 49 |

### `MemberEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `added` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `PullRequestEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `assigned` | 2025-10-15-12 | 2026-07-15-12 | 10 |
| `closed` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `labeled` | 2025-10-15-12 | 2026-07-15-12 | 10 |
| `merged` | 2025-12-15-12 | 2026-07-15-12 | 8 |
| `opened` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `reopened` | 2015-01-15-12 | 2026-06-15-12 | 138 |
| `unassigned` | 2025-10-15-12 | 2026-01-15-12 | 3 |
| `unlabeled` | 2025-10-15-12 | 2026-07-15-12 | 10 |

### `PullRequestEvent` → `payload.pull_request.merged`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `False` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `True` | 2015-01-15-12 | 2025-09-15-12 | 129 |

### `PullRequestEvent` → `payload.pull_request.state`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `closed` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `open` | 2015-01-15-12 | 2025-09-15-12 | 129 |

### `PullRequestReviewCommentEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `created` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `PullRequestReviewCommentEvent` → `payload.pull_request.state`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `closed` | 2015-01-15-12 | 2025-09-15-12 | 129 |
| `open` | 2015-01-15-12 | 2025-09-15-12 | 129 |

### `PullRequestReviewEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `created` | 2020-09-15-12 | 2026-07-15-12 | 71 |
| `dismissed` | 2025-10-15-12 | 2026-07-15-12 | 10 |
| `updated` | 2025-10-15-12 | 2026-07-15-12 | 10 |

### `PullRequestReviewEvent` → `payload.pull_request.state`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `closed` | 2020-09-15-12 | 2025-09-15-12 | 61 |
| `open` | 2020-09-15-12 | 2025-09-15-12 | 61 |

### `PullRequestReviewEvent` → `payload.review.state`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `approved` | 2020-09-15-12 | 2026-07-15-12 | 71 |
| `changes_requested` | 2020-09-15-12 | 2026-07-15-12 | 71 |
| `commented` | 2020-09-15-12 | 2026-07-15-12 | 71 |
| `dismissed` | 2020-09-15-12 | 2026-07-15-12 | 71 |

### `ReleaseEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `published` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `ReleaseEvent` → `payload.release.draft`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `False` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `True` | 2020-09-15-12 | 2026-03-15-12 | 39 |

### `ReleaseEvent` → `payload.release.prerelease`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `False` | 2015-01-15-12 | 2026-07-15-12 | 139 |
| `True` | 2015-01-15-12 | 2026-07-15-12 | 139 |

### `WatchEvent` → `payload.action`

| Value | First | Last | Hours seen |
| --- | --- | --- | ---: |
| `started` | 2015-01-15-12 | 2026-07-15-12 | 139 |

