# ADR-49: Read-side git children run under one per-request budget

> Decision record for issue #313 (bound the memory and work of read-side
> git subprocesses on deep histories). Context: the `git:read` endpoints
> answer from `git` subprocesses over a cache repository. Anyone who can push
> shapes that history. At 300,000 linear commits, one `git log -1 -- <file
> from the root commit>` peaks at 184 MB of heap. Before this decision, no
> read had a time, concurrency, or output bound: `http.Server` has no
> timeouts, children were killed only when the client disconnected, and
> `runGit` buffered all stdout.

**Every read-side git child goes through one runner, `gitread.Run`.** The
caller names the child's class:

- **Light:** the work is bounded by one object (ref resolution, one commit).
- **Heavy:** the work grows with history, tree, blob, or output size.

For every child, the runner:

- sets the class's heap cap before git starts;
- counts buffered stdout against the request's allowance;
- keeps only the tail of stderr;
- always reaps git before it returns.

A request's limits live in a `gitread.Budget` carried in the context. A
middleware installs it on every `git:read` route that starts git, including
merge/preview. Without a Budget (write endpoints), `Run` applies no limit,
so their behavior is unchanged. The limits are:

| Limit | Value | Failure (before or instead of the work) |
|---|---|---|
| Admission | 8 slots per process (`file` and `archive`: a separate 8), at most 2 of each per repository (token issuer and repo claim), taken at the request's first git child of either class and held until the response is delivered; 1 s wait | 503 + `Retry-After: 1`; git not started |
| Time | 30 s of git time per request from its first child, shared by all children (archive: 10 min) | 503, no `Retry-After`; git killed and reaped |
| Heap | `RLIMIT_DATA` 256 MiB Heavy / 32 MiB Light, set by `/bin/sh -c 'ulimit -d …; exec git'`; an inherited stricter limit is kept | 422 |
| Output | 64 MiB buffered stdout per request | 422; git killed at once |
| Blob size | 100 MiB, checked from `ls-tree` sizes | 422 before any response byte |
| Delivery | 30 s per 64 KiB written, and 2 min for the whole response from its first byte (`file`, `archive`: 10 min), as HTTP write deadlines | connection closed; handler returns and releases slot, payload and temp files |

Limit errors never wrap `*exec.ExitError`. Callers that map git exit
statuses to "not found" or "no match" check `IsBudgetError` first.
`internalErr` is the single HTTP mapping point.

**History walks use the same runner.** A walk that reads its output
incrementally, such as `rev-list --topo-order` for a rank, passes a
streaming `Cmd.Stdout` and returns `gitread.ErrStop` from `Write` when it
has seen enough. It runs as a Heavy child inside the request's budget. A
future walk (#292) therefore adds no bounding mechanism of its own.

**Non-Linux policy.** `RLIMIT_DATA` is enforced only on Linux; macOS
accepts the setting but does not enforce it. On other platforms `Run`
drops the heap cap and runs git under the admission, time, output, and
blob-size limits only. An endpoint never fails closed because the heap cap
is unavailable. Existing endpoints keep working on every released platform
(Linux and macOS). The consequence is that a git child's memory is
unbounded on macOS. The docs therefore recommend Linux wherever untrusted
users can push. The production image is Linux.

## Rejected alternatives

- **A skip cap or a history-size preflight** (`rev-list --count`,
  `--max-count`). `--max-count` limits output, not what git allocates to
  prepare the walk. A count preflight is itself a full walk.
- **`RLIMIT_AS`.** It counts address space, including pack mmaps and
  thread stack reservations, so honest reads would fail unpredictably.
- **Per-child slots, or releasing the slot when the response starts.** A
  slot bounds everything a request retains, not just its git processes: a
  files/metadata page keeps output between hundreds of children, and a
  finished blame keeps its payload while a slow client reads it. Admission
  therefore lasts until the response is delivered, and write deadlines
  (idle and absolute) bound how long a slow reader can hold it. Light-only
  requests (a multi-megabyte commit object) take a slot too.
- **Per-child deadlines.** A 30 s deadline per child would give a
  1000-entry page 8 hours.
- **Failing closed on macOS.** That would disable the whole read API on a
  released platform.

## Consequences

- The measured worst class at 300,000 commits (184 MiB glibc, 190 MiB musl)
  fits the cap. At 1,000,000 commits, per-file walks, blame of an old file,
  and `merge-base` to the root fail with 422 at the cap. Deep `commits`
  pages still fit. A files/metadata page whose per-file walks need more than
  30 s gets 503; smaller pages work.
- `git grep` is pinned to one thread. `RLIMIT_DATA` counts every thread's
  stack and malloc arena, and one thread per CPU (792 MB of VmData on 32
  CPUs) cannot start under the cap.
- `archive` spools each blob (at most 100 MiB) to a temporary file before
  writing it, so a client that stops reading keeps no git process alive.
  An archive error after its response started still aborts the connection.
- Slow readers of one repository hold at most 2 slots of a pool, so they
  cannot starve other repositories. Starving a whole pool takes slow
  readers on 4 repositories, and the write deadlines release every slot
  within the delivery limit. No finite pool guarantees availability under
  arbitrary load; the per-IP rate limiter is the lever for that.
- Hydration, `upload-pack`, and write endpoints are outside this budget.
  #322 tracks whether commit-graph writes join it.
