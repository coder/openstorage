# ADR-42: LFS reclamation is mark-and-sweep over reachable pointer files

> Decision record for issue #73.

Registered LFS objects were permanent: the sweeper's `HasLFSObject` guard
treats every registered row as live (ADR-34/#51), so a `git:write` holder
direct-PUTting ~100,000 tiny objects nobody references wedges the per-repo
object-count cap forever — every later new object answers `507` while staying
far under the byte quota. Reclaiming a registered object safely requires
proving no reachable pointer file references its OID: reachability over git
content, a garbage collector, never an admission-path patch (#51's bounds
exist precisely to keep such walks off request paths).

The collector is a background job shaped like the sweeper and the purger:
paged over a rotating cursor, budgeted per run, and resumable — a repository
whose walk exceeds the run's remainder is retried first next run, and nothing
is ever marked or retired from a partial walk, because an incomplete
reachable set proves nothing. Reachability is computed per namespace group
(ADR-35: `+ephemeral`/`+import` share the base repo's object space) from the
hydrated cache after `Materialize` synced it to the DB ref authority, by
streaming an unfiltered `git rev-list --objects` through `cat-file`
and decoding pointer-sized blobs with a decoder deliberately MORE lenient
than git-lfs: a false positive spares an object, a false negative could
retire live data.

The verdict is mark-and-sweep with a persistent grace mark rather than
delete-on-sight. A scan stamps `lfs_objects.unreferenced_since` when it first
observes a row unreferenced and clears it when the OID reappears; retirement
requires the stamp to have survived a full grace window (default 24h) — "was
unreferenced continuously", not "is unreferenced right now". Every liveness
signal clears the stamp transactionally: re-registration's idempotent path,
and the batch/PUT zero-growth "already stored" answers, which touch the row
FIRST and fall back to a retryable error when it vanished — a git-lfs client
always batches before pushing pointers, so learning "stored" buys a full
grace window to push. Retirement itself runs in the GuardedLFSDelete guard
class (#61): under the base-ledger lock it re-checks the purge fence, the
namespace group's pinned ref generations (`FOR UPDATE`, so a concurrent
push's deferred generation bump serializes against the verdict), and the
stamp's age, then deletes the row. The count and byte refund is DEFERRED
(Codex round 33 on PR #132): a fresh completion watermark only certifies a
sweep that listed blobs BEFORE this deletion — that sweep saw the registry
row and spared the blob — so refunding in the verdict would hand back
capacity the bytes still occupy. The verdict instead parks the refund in a
durable per-epoch bucket (`job_scan_state`), keyed by the monotone
destructive-sweep epoch it pinned under the same lock (every destructive
pass increments the epoch at START, before its blob listing), and a later
reclamation pass settles it — decrementing `lfs_object_counts`, refunding
`pack_usage`, and bumping the adoption generation atomically, the ADR-41
twin-refund discipline that unwedges a `507`'d repository — only once a
destructive sweep with a STRICTLY greater epoch (one that provably began
after the deletion committed) completes cleanly. Legacy-spelled parts
settle only under a legacy-armed completion, which keeps its own scope so
a later canonical-only pass cannot erase the proof; bases that vanish from
the work queue (every row retired) settle through a dedicated durable
rotation over the bucket keyspace, and a repository purge drops its
buckets with the rest of the ledger. The
accepted residual: on Postgres, a raw-git pointer push (no batch handshake)
whose commit began after the generation recheck locked the rows can land
after the verdict commits; the window is microseconds, requires referencing
an object already grace-expired-unreferenced, and the next batch upload
re-registers the bytes through the verified path. SQLite's single writer has
no such window.

Retirement deletes rows, never bytes. The now-rowless blobs — canonical and
legacy spellings alike — become ordinary orphans for the EXISTING sweep
machinery and its gates (`OPENSTORAGE_SWEEP_DELETE_LFS`, ADR-41's legacy
transition gate); #104's download-time restore is registered-row-gated, so a
retired row is not restorable through download paths, while never-migrated
legacy bytes simply return to the pre-registry read-compat class they came
from. That split makes the coupling load-bearing (Codex security round 13):
the refunded cap slot is the only bound on the `lfs/<repo>/` prefix that
quota floors and sweep listings walk, so retirement without a running reap
would let upload → grace → retire → re-upload cycles grow the prefix without
bound. Startup therefore rejects `OPENSTORAGE_LFS_GC_DELETE_ENABLED` on a
live sweep schedule unless the LFS reap is armed and the grace is at least
the reap cadence (max of the sweep age threshold, the sweep interval, and
the quarantine retention floor — every reap strands one quarantine backup
for at least an hour — so slots refund no faster than blobs AND their
backups become purgeable). The coupling is additionally OBSERVED at
runtime, not merely configured (issue #120): every destructive LFS sweep
pass that finishes its keyspace with zero recorded errors persists a
fleet-wide watermark (`job_scan_state`), the retirement transaction
re-reads it before refunding, and both the pass and the per-row verdict
demote to report-only whenever no clean destructive pass completed within
the grace window — covering armed-but-failing sweeps, passes that outlast
the reclamation cycle, and the versioning guard demoting a run mid-flight.
The sweep emergency stop (interval 0) is the planned instance of the same
demotion: the watermark stales and reclamation reports instead of
crash-looping the fleet. The watermark's payload also records whether the
certified sweep had the LEGACY delete class armed
(`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`): legacy-spelled registry rows
retire only under a legacy-armed watermark AND a replica whose own config
arms the flag — an unarmed sweep deliberately spares legacy orphan blobs,
so refunding their slots would let upload/retire cycles grow storage past
the object cap (deferred distinctly as `rows_deferred_legacy_unarmed`).
The job ships dark (interval 0) with report-only marking as the second
gate (`OPENSTORAGE_LFS_GC_DELETE_ENABLED`), mirroring the sweep flags'
staged rollout; `OPENSTORAGE_LFS_MAX_OBJECTS_PER_REPO` is the orthogonal
operator escape hatch that unwedges a repository immediately. An actively
abusive writer can re-upload junk to reset its grace clocks — reclamation
guarantees convergence after abuse stops, not defense against a live token,
which is revocation's job.
