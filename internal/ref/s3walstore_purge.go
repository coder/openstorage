// S3WAL repository-purge, LFS-reclamation, and absence-scan seams (issues
// #52/#73/#101). Purge phases are each ONE manifest CAS in the SQL
// pipeline's order; the fence is a field of the same object every write
// CASes, so the fence-first total order is inherent. The manifest object
// is NEVER deleted — it persists as the burned-name tombstone.
package ref

import (
	"context"
	"sort"
	"time"

	"github.com/coder/code-storage/internal/ref/s3wal"
)

// FenceRepoPurge durably marks the BASE group purge-fenced (idempotent,
// permanent): every later row-creating write for the group rejects, so the
// purge's zero-row verification cannot be raced by a settling writer.
func (s *S3WAL) FenceRepoPurge(ctx context.Context, base string) error {
	assertf(base != "", "FenceRepoPurge: empty base repo")
	assertf(baseRepoOf(base) == base, "FenceRepoPurge: %q is not a base repo", base)
	return s.mutate(ctx, base, s3walMutation{
		op:    "fence repo purge",
		grows: true,
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			if m.PurgedAt != nil {
				return false, nil // idempotent
			}
			stamp := clk
			m.PurgedAt = &stamp
			return true, nil
		},
	})
}

// IsRepoPurged reports whether the BASE repo is purge-fenced.
func (s *S3WAL) IsRepoPurged(ctx context.Context, base string) (bool, error) {
	assertf(base != "", "IsRepoPurged: empty base repo")
	m, err := s.viewManifest(ctx, base)
	if err != nil {
		return false, err
	}
	return m.PurgedAt != nil, nil
}

// DropRepoRefs deletes every ref row of the group's repos (refs die FIRST)
// and returns the rows deleted. Member generations bump per dropped row,
// like the SQL triggers; the generation state itself falls to
// PurgeRepoLedger. Idempotent.
func (s *S3WAL) DropRepoRefs(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var dropped int64
	err := s.mutate(ctx, baseRepoOf(group[0]), s3walMutation{
		op: "drop repo refs",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			dropped = 0
			for _, repo := range group {
				mem := m.Member(repo)
				if mem == nil || len(mem.Refs) == 0 {
					continue
				}
				n := int64(len(mem.Refs))
				dropped += n
				mem.Gen += n
				mem.Refs = nil
			}
			return dropped > 0, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return dropped, nil
}

// RetireRepoPacks tombstones every registered pack of the group's repos:
// each packs row becomes a retained row (bytes 0 — classification
// tombstones, not accounting; the usage counter stays credited until
// PurgeRepoLedger) and is deleted. An EXISTING retained row (a real fold's,
// carrying real bytes) is kept as-is so its eventual reclaim still debits
// correctly. Returns the pack rows retired. Idempotent.
func (s *S3WAL) RetireRepoPacks(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var retired int64
	err := s.mutate(ctx, baseRepoOf(group[0]), s3walMutation{
		op: "retire repo packs",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			retired = 0
			for _, repo := range group {
				mem := m.Member(repo)
				if mem == nil || len(mem.Packs) == 0 {
					continue
				}
				if mem.Retained == nil {
					mem.Retained = make(map[string]s3wal.RetainedPack)
				}
				for _, p := range mem.Packs {
					if _, ok := mem.Retained[p.Key]; !ok {
						mem.Retained[p.Key] = s3wal.RetainedPack{Bytes: 0, SupersededAt: clk}
					}
					retired++
					mem.Gen++
				}
				mem.Packs = nil
			}
			return retired > 0, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return retired, nil
}

// DropRepoLFSObjects deletes the group's LFS registry rows and the
// maintained count (LFS rows key by the BASE repo). Returns the object
// rows deleted. Idempotent.
func (s *S3WAL) DropRepoLFSObjects(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var dropped int64
	err := s.mutate(ctx, baseRepoOf(group[0]), s3walMutation{
		op: "drop repo lfs objects",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			dropped = int64(len(m.LFS.Objects))
			if dropped == 0 && m.LFS.Count == 0 {
				return false, nil
			}
			m.LFS.Objects = nil
			m.LFS.Count = 0
			return true, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return dropped, nil
}

// CountRepoRows counts the group's refs + packs + LFS rows — the purge's
// pre-completion verification read.
func (s *S3WAL) CountRepoRows(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	m, err := s.viewManifest(ctx, baseRepoOf(group[0]))
	if err != nil {
		return 0, err
	}
	var n int64
	for _, repo := range group {
		if mem := m.Member(repo); mem != nil {
			n += int64(len(mem.Refs)) + int64(len(mem.Packs))
		}
	}
	return n + int64(len(m.LFS.Objects)), nil
}

// PurgeRepoLedger removes the group's remaining accounting and state rows
// (reservations incl. claim leases, retained tombstones, usage counter,
// LFS counts/mismatches/adoption gens, member generations), returning the
// rows deleted — the SQL row-per-row counting mapped onto manifest
// entries/fields. Runs strictly AFTER the blob pass; idempotent (rerun
// returns 0). The manifest itself persists as the fence + tombstone.
func (s *S3WAL) PurgeRepoLedger(ctx context.Context, group []string) (int64, error) {
	validatePurgeGroup(group)
	var total int64
	err := s.mutate(ctx, baseRepoOf(group[0]), s3walMutation{
		op: "purge repo ledger",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			total = 0
			total += int64(len(m.Reservations))
			m.Reservations = nil
			for _, repo := range group {
				mem := m.Member(repo)
				if mem == nil {
					continue
				}
				total += int64(len(mem.Retained))
				mem.Retained = nil
				if mem.Gen > 0 { // the repo_generations row
					total++
					mem.Gen = 0
				}
				if len(mem.Refs) == 0 && len(mem.Packs) == 0 {
					delete(m.Members, repo)
				}
			}
			if m.UsageBytes != 0 { // the pack_usage rows
				total++
				m.UsageBytes = 0
			}
			total += int64(len(m.LFS.Mismatches))
			m.LFS.Mismatches = nil
			if m.LFS.AdoptionGen != 0 { // the lfs_adoption_gens row
				total++
				m.LFS.AdoptionGen = 0
			}
			if m.LFS.Count != 0 || len(m.LFS.Objects) != 0 { // the counts row
				total++
				m.LFS.Count = 0
				m.LFS.Objects = nil
			}
			return total > 0, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return total, nil
}

// ---------------------------------------------------------------------------
// LFS reclamation rows (issue #73).

// LFSReposWithObjects pages base repos holding at least one registered LFS
// object (keyset on repo ascending): a fleet prefix walk reading each
// group's maintained count (§2.9 — bucket listing order IS base byte
// order).
func (s *S3WAL) LFSReposWithObjects(ctx context.Context, after string, limit int) ([]string, error) {
	assertf(limit > 0 && limit <= lfsReclaimPageCap, "LFSReposWithObjects: bad page limit %d", limit)
	var repos []string
	err := s.s3walEachBase(ctx, after, func(base string) error {
		if len(repos) == limit {
			return errS3WALWalkDone
		}
		m, err := s.viewManifest(ctx, base)
		if err != nil {
			return err
		}
		if m.LFS.Count > 0 {
			repos = append(repos, base)
		}
		return nil
	})
	if err != nil && err != errS3WALWalkDone {
		return nil, err
	}
	return repos, nil
}

// errS3WALWalkDone terminates a fleet walk early once a page is full.
var errS3WALWalkDone = walkDoneError{}

type walkDoneError struct{}

func (walkDoneError) Error() string { return "s3wal: fleet walk page complete" }

// CountRefs prices repo's ref fanout, capped at limit (the count is one
// in-hand map, so the cap only bounds the REPORTED value).
func (s *S3WAL) CountRefs(ctx context.Context, repo string, limit int64) (int64, error) {
	assertf(repo != "", "CountRefs: empty repo")
	assertf(limit > 0, "CountRefs: non-positive limit %d", limit)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return 0, err
	}
	var n int64
	if mem := m.Member(repo); mem != nil {
		n = int64(len(mem.Refs))
	}
	return min(n, limit), nil
}

// ListRepoLFSObjects pages repo's registered LFS rows (keyset on key,
// ascending byte order).
func (s *S3WAL) ListRepoLFSObjects(ctx context.Context, repo, after string, limit int) ([]LFSObjectRow, error) {
	validateLFSReclaimPage(repo, limit)
	m, err := s.viewManifest(ctx, baseRepoOf(repo))
	if err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(m.LFS.Objects))
	for key := range m.LFS.Objects {
		if key > after {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	if len(keys) > limit {
		keys = keys[:limit]
	}
	rows := make([]LFSObjectRow, 0, len(keys))
	for _, key := range keys {
		obj := m.LFS.Objects[key]
		row := LFSObjectRow{Key: key, Bytes: obj.Bytes}
		if obj.UnreferencedSince != nil {
			row.UnreferencedSince = *obj.UnreferencedSince
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// MarkLFSObjectsUnreferenced stamps the reclamation mark on unmarked rows,
// preserving existing stamps (the grace window measures CONTINUOUS
// unreferenced observation). Returns the rows stamped.
func (s *S3WAL) MarkLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error) {
	validateLFSReclaimPage(repo, len(keys))
	var marked int64
	err := s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "mark lfs unreferenced",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			marked = 0
			for _, key := range keys {
				row := m.LFS.Objects[key]
				if row == nil || row.UnreferencedSince != nil {
					continue
				}
				stamp := clk
				row.UnreferencedSince = &stamp
				marked++
			}
			return marked > 0, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return marked, nil
}

// ClearLFSObjectsUnreferenced removes the reclamation mark from the given
// rows, returning the rows cleared.
func (s *S3WAL) ClearLFSObjectsUnreferenced(ctx context.Context, repo string, keys []string) (int64, error) {
	validateLFSReclaimPage(repo, len(keys))
	var cleared int64
	err := s.mutate(ctx, baseRepoOf(repo), s3walMutation{
		op: "clear lfs unreferenced",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			cleared = 0
			for _, key := range keys {
				row := m.LFS.Objects[key]
				if row == nil || row.UnreferencedSince == nil {
					continue
				}
				row.UnreferencedSince = nil
				cleared++
			}
			return cleared > 0, nil
		},
	})
	if err != nil {
		return 0, err
	}
	return cleared, nil
}

// RetireUnreferencedLFSObject deletes one grace-expired marked row,
// refunding count and bytes and bumping the adoption generation — all one
// CAS with the fences evaluated on the same snapshot: purge fence, pinned
// member generations (a pointer push mid-scan lives in a bumped generation
// by definition), and the live adoption-claim spare. Recovery class PRED:
// re-applying on a fresh snapshot re-evaluates every fence and finds a
// vanished row spared.
func (s *S3WAL) RetireUnreferencedLFSObject(ctx context.Context, repo, key string, pinnedGens map[string]int64, grace time.Duration) (bool, error) {
	assertf(repo != "" && key != "", "RetireUnreferencedLFSObject: empty repo/key")
	assertf(len(pinnedGens) > 0, "RetireUnreferencedLFSObject: no pinned generations")
	assertf(grace >= 0, "RetireUnreferencedLFSObject: negative grace %v", grace)
	base := baseRepoOf(repo)
	members := make([]string, 0, len(pinnedGens))
	for member := range pinnedGens {
		members = append(members, member)
	}
	sort.Strings(members)
	retired := false
	err := s.mutate(ctx, base, s3walMutation{
		op: "retire lfs object",
		fn: func(m *s3wal.Manifest, clk time.Time) (bool, error) {
			retired = false
			if m.PurgedAt != nil {
				return false, nil // the purge pipeline owns fenced rows
			}
			for _, member := range members {
				var gen int64
				if mem := m.Member(member); mem != nil {
					gen = mem.Gen
				}
				if gen != pinnedGens[member] {
					return false, nil // refs moved since the scan: stale
				}
			}
			// A live per-key adoption claim spares the row (Codex round
			// 11): an active canonical materialization owns its slot.
			if r := m.Reservations[lfsClaimTwin(repo, key)+AdoptionClaimSuffix]; r != nil &&
				s.s3walReservationLive(m, r.CreatedAt, AdoptionClaimTTL) {
				return false, nil
			}
			row := m.LFS.Objects[key]
			if row == nil || row.UnreferencedSince == nil {
				return false, nil // touched, re-registered, or retired
			}
			if !s.s3walExpiryAllowed(base, m, *row.UnreferencedSince, grace, "lfs unreferenced grace") {
				return false, nil // mark younger than grace (or skew demote)
			}
			delete(m.LFS.Objects, key)
			m.LFS.Count = max(m.LFS.Count-1, 0)
			m.UsageBytes = max(m.UsageBytes-row.Bytes, 0)
			m.LFS.AdoptionGen++
			retired = true
			return true, nil
		},
	})
	if err != nil {
		return false, err
	}
	return retired, nil
}

// ---------------------------------------------------------------------------
// Absence-scan fleet pagination (issue #101, plan §2.9).

// ListFleetLFSObjects pages registered LFS rows fleet-wide in (repo, key)
// byte order. LFS rows key by the base repo, so each group contributes one
// member; cursor resume strips the member suffix, re-reads that group, and
// continues the listing after it.
func (s *S3WAL) ListFleetLFSObjects(ctx context.Context, afterRepo, afterKey string, limit int) ([]AbsenceLFSRow, error) {
	validateAbsencePage(limit)
	var rows []AbsenceLFSRow
	emit := func(base string) error {
		if len(rows) == limit {
			return errS3WALWalkDone
		}
		m, err := s.viewManifest(ctx, base)
		if err != nil {
			return err
		}
		keys := make([]string, 0, len(m.LFS.Objects))
		for key := range m.LFS.Objects {
			if base > afterRepo || (base == afterRepo && key > afterKey) {
				keys = append(keys, key)
			}
		}
		sort.Strings(keys)
		for _, key := range keys {
			if len(rows) == limit {
				return errS3WALWalkDone
			}
			rows = append(rows, AbsenceLFSRow{Repo: base, Key: key, Bytes: m.LFS.Objects[key].Bytes})
		}
		return nil
	}
	err := s.s3walFleetResume(ctx, afterRepo, emit)
	if err != nil && err != errS3WALWalkDone {
		return nil, err
	}
	return rows, nil
}

// ListFleetPacks pages registered pack rows fleet-wide in (repo, id) byte
// order across every group member (§2.9: a base's member keys are
// contiguous in byte order — the key-build alphabet assert makes the lemma
// checked, not assumed).
func (s *S3WAL) ListFleetPacks(ctx context.Context, afterRepo, afterID string, limit int) ([]AbsencePackRow, error) {
	validateAbsencePage(limit)
	var rows []AbsencePackRow
	emit := func(base string) error {
		if len(rows) == limit {
			return errS3WALWalkDone
		}
		m, err := s.viewManifest(ctx, base)
		if err != nil {
			return err
		}
		members := make([]string, 0, len(m.Members))
		for member := range m.Members {
			assertf(member == base || (len(member) > len(base) && member[:len(base)] == base && member[len(base)] == '+'),
				"s3wal manifest %s holds foreign member %q", base, member)
			members = append(members, member)
		}
		sort.Strings(members)
		for _, member := range members {
			mem := m.Members[member]
			recs := append([]s3wal.PackRecord(nil), mem.Packs...)
			sort.Slice(recs, func(i, j int) bool { return recs[i].ID < recs[j].ID })
			for _, p := range recs {
				if member < afterRepo || (member == afterRepo && p.ID <= afterID) {
					continue
				}
				if len(rows) == limit {
					return errS3WALWalkDone
				}
				rows = append(rows, AbsencePackRow{Repo: member, ID: p.ID, Key: p.Key, Bytes: p.Bytes})
			}
		}
		return nil
	}
	err := s.s3walFleetResume(ctx, afterRepo, emit)
	if err != nil && err != errS3WALWalkDone {
		return nil, err
	}
	return rows, nil
}

// s3walFleetResume implements the §2.9 cursor-resume procedure: derive the
// cursor row's base group (strip the +suffix), re-emit that group's rows
// past the cursor, then continue the prefix listing strictly after it.
func (s *S3WAL) s3walFleetResume(ctx context.Context, afterRepo string, emit func(base string) error) error {
	startBase := ""
	if afterRepo != "" {
		startBase = baseRepoOf(afterRepo)
		if err := emit(startBase); err != nil {
			return err
		}
	}
	return s.s3walEachBase(ctx, startBase, emit)
}
