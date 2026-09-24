# ADR-47: Cache repos get split commit-graphs, written by full hydrates

> Decision record for issue #307. Context: nothing wrote a commit-graph
> into cache repositories, so every read-side history walk (`git log -1 --
> <path>` per `files/metadata` entry, the `--topo-order` rank walk from
> #292) parsed commit objects from packs.

**A full hydrate refreshes a split commit-graph with changed-path Bloom
filters before it releases the hydrate lock.** After a full sync that
applied every ref (`refsErr == nil` and no push-held ref skipped) and
published its generation marker, `Hydrate` runs

```
git commit-graph write --stdin-commits --split --no-progress [--changed-paths]
```

in the cache repo, still holding `codestorage.hydrate.lock`. Its stdin is
the set of distinct commit tips that sync just applied, collected while it
streamed the store refs, so the write never enumerates the live ref
namespace.
`--changed-paths` is passed only when the repo has no graph yet. Later
writes inherit the choice: Git keeps writing Bloom filters if and only if
the existing graph has them.

## When the graph is written

- Only on the full-sync path. The generation-marker fast path still forks
  no subprocess (issue #93).
- Nothing runs after receive-pack. A push bumps the store generation, so
  the next request on that node takes the full path, and that hydrate adds
  the pushed commits to the graph. API writes and pushes that land on other
  nodes are picked up the same way. This refresh is deferred maintenance:
  until the next full hydrate, the new commits are simply outside the
  graph.
- A marker-TTL refresh or a default-branch change also runs a write. With
  nothing new to add, the write costs about 4 ms (20k commits, one ref);
  in an exploratory run, 10,000 distinct tips cost about 50 ms.
- Two guards can skip the write; a skipped write leaves any existing graph
  in place, which stays correct and only gets staler:
  - **Tip cap.** Ref fanout is attacker-scaled: a push can mint many refs
    cheaply, and every later ref move forces a full hydrate. A
    `--reachable` write resolves every ref (Codex's security review
    measured 23.6 s for 500,000 refs), and a concurrent receive-pack can
    add refs after the sync, so counting refs before a `--reachable` walk
    would not bound it. Instead the sync collects its distinct tips (the
    set is dropped once it exceeds the cap, so memory stays bounded) and
    the write walks only those. Same-tip fanout collapses to one tip. The
    refresh is skipped when the sync saw more than `commitGraphMaxTips`
    (10,000) distinct tips.
  - **Process-wide slots.** At most `cap(commitGraphSlots)` (2) refreshes
    run at once per process, across all repos. A refresh that finds both
    slots busy is skipped, not queued; the repo's next full hydrate tries
    again. This bounds commit-graph CPU per node to two cores however
    many repos an attacker drives.

## Why split graphs and Bloom filters

Committed benchmarks, 20k-commit history, each commit changing 3 of
about 1,000 files in 50 directories. Git 2.55 on a shared AMD EPYC 9454P
host; ranges over 3–5 runs.

`BenchmarkLogPathCommitGraph` (`internal/gitread`), `git log -1 -- <path>`
as `files/metadata` runs it per entry:

| path | no graph | plain graph | graph with Bloom filters |
|---|---|---|---|
| file changed by the tip commit | 3.1–5.6 ms | 3.3–3.7 ms | 3.4–3.6 ms |
| file only in the root commit (worst case) | 126–148 ms | 69–73 ms | 22–24 ms |

`BenchmarkCommitGraphRefresh` (`internal/githttp`), cost charged to one
full hydrate:

| write | cost |
|---|---|
| first write, plain | 89–99 ms |
| first write, with Bloom filters | 2.1–2.4 s |
| after a 10-commit push (production writer, Bloom inherited) | 14–18 ms |
| after a 100-commit push | 44–46 ms |
| nothing new | 3.8–4.5 ms |

The write cost is charged to the full hydrate after a push, not to the
push itself. The `--topo-order` walk from #292 gains too: issue #307 reports
71 ms to 3.7 ms for the tip rank, measured with `BenchmarkTopoRanks` from
PR #306 (not yet on main when this was written).

Split graphs keep the amortized write cost proportional to the new
commits. Git merges layers geometrically (`--size-multiple=2`), so an
occasional write that merges layers costs more, and every write resolves
every ref. A non-split rewrite of the whole graph cost 110–170 ms per
write in an exploratory run. The default Bloom filter version (1) is
kept: Git documents version 2 as possibly incompatible with older Git,
and cache directories outlive image upgrades.

## Budgets and the no-Bloom fallback

Computing Bloom filters for a whole history can be expensive. In an
exploratory run, a repacked 100k-commit fixture took 74 s, against about
2 s for the 20k-commit benchmark. That fixture does not prove a general
scaling law, but it rules out an unbounded write under the hydrate lock.
`--max-new-filters` does not bound it usefully: Git chose which commits got
filters, the tip commits got none in our test, and with `--split` the
skipped commits are revisited only when their layer is merged. So the
refresh is bounded by time instead:

1. The whole refresh shares one deadline, `commitGraphDeadline` (5 s),
   derived from the request context.
2. The first attempt, in the inherited mode, gets at most
   `commitGraphFirstBudget` (3 s) of it. That covers the 20k-commit
   benchmark's first write with Bloom filters on an idle host. On a busy
   host the same write can overrun and fall back.
3. If that attempt runs out of its own budget while the request is still
   live and the shared deadline has not passed, one retry runs with
   `--no-changed-paths` for the rest of the shared deadline. A graph
   without Bloom filters costs about 5 µs per commit (0.46 s for 100k
   commits in an exploratory run).
4. A timed-out attempt gets SIGTERM, then SIGKILL after
   `commitGraphKillDelay` (1 s), and is reaped before the retry starts.
   So the refresh holds the lock for at most about 7 s.
5. If the request is cancelled, nothing is retried. Any other failure is
   logged and not retried.

These limits are chosen latency caps, not performance guarantees. A
timeout does not prove that Bloom filters were the bottleneck: ref
enumeration or a layer merge can also be slow. A repo that keeps timing
out pays up to about 7 s on each full hydrate.

A fallback layer carries no filters, and later writes inherit that from
the tip layer. Older layers keep their filters until a merge folds them
into a filter-free layer. Coverage then degrades from partial to none on
that node. The repo keeps the generation-number gains. If the first
attempt publishes its layer just as its budget expires, the fallback finds
nothing new and writes nothing, so the Bloom mode survives.

Result: small and medium histories get Bloom filters. Very large ones, and
repos that import a very large history in one push, end up with a plain
graph on that node. A later background backfill could restore Bloom
filters for those; it is out of scope here.

## Failure handling

A graph write never fails a request. Every error is logged with the repo
path and dropped. The marker is already published, and the refs are
already synced. A missing, stale, or partial graph is only slower:

- Git looks up graph entries by commit ID. Commits outside the graph are
  parsed from their objects. A commit's parents, tree, date, and
  first-parent diff never change, so a graph entry cannot go stale.
  Nothing in the cache deletes objects: the repack fold writes object
  unions, so a graph cannot point at a commit that is gone.
- Bloom filters give only false positives. A "maybe" falls back to a real
  tree diff.
- A reader that loses a split layer to a concurrent merge prints `warning:
  unable to find all commit-graph files` and uses the layers it loaded.
  This was tested with Git 2.55 by deleting a layer by hand: the output
  was unchanged and the exit code was 0. `gitread` shows stderr only on failure.
- If a budget expires, Git gets SIGTERM, which runs its lockfile cleanup.
  SIGKILL follows after `commitGraphKillDelay`. Git computes the graph in
  memory and takes `commit-graph-chain.lock` only to write the result. A
  SIGKILL during the computation left no lock behind in our tests.
- A pre-existing lock is never removed. Its owner cannot be proven: a dead
  server can leave a live child, and a foreign Git need not honor our
  flock. Git fails with "Unable to create ... lock: File exists". The
  failure is logged as `outcome=lock_held` and the next hydrate retries.
  An operator removes a stale lock by hand, following the
  [stale-lock runbook](../how-to/deploy-production.md#commit-graph-refresh).
  Issue #322 considered automatic cleanup and declined it for the same
  reason.
- Every refresh ends in one outcome from a fixed set (`written`,
  `written_plain`, `skipped_tip_cap`, `skipped_busy`, `lock_held`,
  `timed_out`, `canceled`, `failed`). Every outcome except `written` logs
  one line, `commit-graph refresh outcome=<outcome> repo=<path>: <detail>`.
  This is a documented log contract
  ([outcomes](../how-to/deploy-production.md#commit-graph-refresh)).

## Concurrency

- **Writers.** All of our writers run under the per-repo hydrate flock,
  so they serialize within and across processes on a node. Git itself does
  not write graphs in cache repos: `gc.auto=0`, and with Git 2.55
  receive-pack spawned no maintenance child (checked with trace2). Upstream
  sync fetches into separate mirror directories.
- **Readers.** `upload-pack`, `receive-pack`, `gitread`, and the API
  scratch repos (which reach the cache through alternates) read the graph
  lock-free. Git replaces the chain file and layers by atomic rename.
- **Concurrent receive-pack.** Receive-pack takes no hydrate lock, and
  this change does not add one. The write walks only the tips the sync
  applied; those commits and their history are already in the object
  store, and nothing deletes objects. Refs a concurrent push adds are not
  walked; the next full hydrate (which that push's generation bump
  forces) adds them. The write touches no refs, so push holds are
  unaffected.
- **Repack.** The fold materializes packs and writes a new pack holding
  the union of their objects. Object IDs are unchanged, so the graph stays
  valid and needs no integration.
- **Lock hold time.** Other full hydrates of the same repo wait up to the
  shared deadline plus the kill delays. Fast-path readers do not wait,
  because the marker is published before the write starts.

## Subprocess isolation

The child gets the same isolation as the API and sync Git subprocesses:
every inherited `GIT_*` variable is dropped, and `GIT_CONFIG_GLOBAL` and
`GIT_CONFIG_SYSTEM` point at `/dev/null` (plus `GIT_CONFIG_NOSYSTEM=1`).
The repo path may be relative (a relative `--cache-dir` is accepted); the
child runs with that path as its working directory. An inherited
`GIT_DIR`, `GIT_OBJECT_DIRECTORY`, or `GIT_CONFIG_COUNT/KEY/VALUE` could
otherwise redirect the write or disable it, and host config could change the graph format. Hydrate's other
subprocesses are unchanged.
