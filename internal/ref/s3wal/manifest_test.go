package s3wal

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestManifestCodecRoundTrip pins the v1 schema: every field survives an
// encode/decode cycle and EncodeAdvance advances seq/clk and mints a fresh
// writer nonce.
func TestManifestCodecRoundTrip(t *testing.T) {
	now := time.Now().UTC()
	stamp := now.Add(-time.Minute)
	purged := now.Add(-time.Hour)
	m := NewManifest()
	m.PurgedAt = &purged
	m.UsageBytes = 1234
	mem := m.EnsureMember("repo-a")
	mem.Gen = 7
	mem.Refs = map[string]RefRecord{"refs/heads/main": {SHA: strings.Repeat("ab", 20), CreatedAt: stamp}}
	mem.Packs = []PackRecord{{ID: "p1", Key: "packs/repo-a/pack-p1.pack", Bytes: 42, CreatedAt: stamp}}
	mem.Retained = map[string]RetainedPack{"packs/repo-a/pack-old.pack": {Bytes: 9, SupersededAt: stamp}}
	m.Reservations = map[string]*Reservation{
		"packs/repo-a/pack-in.pack": {Owner: "o1", Bytes: 5, Attempts: 2, Gen: "g1", CreatedAt: stamp, OpID: "op-1"},
	}
	m.DeleteLocks = map[string]*DeleteLock{
		"lfs/repo-a/o2/ffff": {Owner: "d1", OpID: "op-2", CreatedAt: stamp},
	}
	m.LFS = LFSState{
		Count:       1,
		Objects:     map[string]*LFSObject{"lfs/repo-a/o2/aaaa": {Bytes: 3, CreatedAt: stamp, UnreferencedSince: &stamp}},
		Mismatches:  map[string]string{"lfs/repo-a/aaaa": "fp-1"},
		AdoptionGen: 4,
	}
	m.SyncClaim = &SyncClaim{Holder: "h-1", CreatedAt: stamp}
	m.RecordOp("op-1")

	body, err := m.EncodeAdvance(ClkNext(now, m.Clk))
	if err != nil {
		t.Fatalf("EncodeAdvance: %v", err)
	}
	if m.Seq != 1 || m.Writer == "" {
		t.Fatalf("EncodeAdvance left seq=%d writer=%q", m.Seq, m.Writer)
	}

	got, err := DecodeManifest(body)
	if err != nil {
		t.Fatalf("DecodeManifest: %v", err)
	}
	if got.Seq != 1 || got.Writer != m.Writer || !got.Clk.Equal(m.Clk) {
		t.Fatalf("decoded header = (%d, %q, %v), want (1, %q, %v)", got.Seq, got.Writer, got.Clk, m.Writer, m.Clk)
	}
	if got.PurgedAt == nil || !got.PurgedAt.Equal(purged) {
		t.Fatalf("purged_at = %v, want %v", got.PurgedAt, purged)
	}
	gm := got.Member("repo-a")
	if gm == nil || gm.Gen != 7 || len(gm.Refs) != 1 || len(gm.Packs) != 1 || len(gm.Retained) != 1 {
		t.Fatalf("member round-trip lost state: %+v", gm)
	}
	if got.UsageBytes != 1234 || got.LFS.Count != 1 || got.LFS.AdoptionGen != 4 {
		t.Fatalf("counters round-trip: usage=%d lfs=%+v", got.UsageBytes, got.LFS)
	}
	r := got.Reservations["packs/repo-a/pack-in.pack"]
	if r == nil || r.Attempts != 2 || r.Gen != "g1" || r.OpID != "op-1" {
		t.Fatalf("reservation round-trip: %+v", r)
	}
	if got.DeleteLocks["lfs/repo-a/o2/ffff"] == nil || got.SyncClaim == nil || got.SyncClaim.Holder != "h-1" {
		t.Fatalf("delete lock / sync claim lost: %+v %+v", got.DeleteLocks, got.SyncClaim)
	}
	o := got.LFS.Objects["lfs/repo-a/o2/aaaa"]
	if o == nil || o.UnreferencedSince == nil || !o.UnreferencedSince.Equal(stamp) {
		t.Fatalf("lfs object round-trip: %+v", o)
	}
	if !got.OpApplied("op-1") {
		t.Fatal("ops ring lost op-1")
	}
	// Decoded manifests must accept another advance (seq 2).
	if _, err := got.EncodeAdvance(ClkNext(now.Add(time.Second), got.Clk)); err != nil {
		t.Fatalf("second EncodeAdvance: %v", err)
	}
	if got.Seq != 2 {
		t.Fatalf("seq after second advance = %d, want 2", got.Seq)
	}
}

// TestManifestDistinctBytesPerWrite pins the §5-R4 ETag-ABA kill: encoding
// the SAME logical state twice produces distinct bytes (fresh writer nonce
// + bumped seq), so content-derived ETags can never ABA.
func TestManifestDistinctBytesPerWrite(t *testing.T) {
	now := time.Now().UTC()
	a := NewManifest()
	b := NewManifest()
	bodyA, err := a.EncodeAdvance(ClkNext(now, a.Clk))
	if err != nil {
		t.Fatal(err)
	}
	bodyB, err := b.EncodeAdvance(ClkNext(now, b.Clk))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(bodyA, bodyB) {
		t.Fatal("identical logical state encoded to identical bytes — ETag ABA is live")
	}
	if memETag(bodyA) == memETag(bodyB) {
		t.Fatal("content ETags collide for distinct nonces")
	}
}

// TestManifestDecodeRejectsCorruption pins crash-and-burn decoding: wrong
// version, non-positive seq, missing writer/clock, and malformed JSON are
// hard errors, never silent empty state.
func TestManifestDecodeRejectsCorruption(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{"malformed", `{`},
		{"wrong version", `{"v":2,"seq":1,"writer":"w","clk":"2026-01-01T00:00:00Z"}`},
		{"zero seq", `{"v":1,"seq":0,"writer":"w","clk":"2026-01-01T00:00:00Z"}`},
		{"no writer", `{"v":1,"seq":1,"clk":"2026-01-01T00:00:00Z"}`},
		{"no clock", `{"v":1,"seq":1,"writer":"w"}`},
	} {
		if _, err := DecodeManifest([]byte(tc.body)); err == nil {
			t.Fatalf("%s: DecodeManifest accepted %q", tc.name, tc.body)
		}
	}
}

// TestOpsRingEviction pins the K=64 bound and oldest-first eviction: op-IDs
// beyond the ring survive only via per-entry records.
func TestOpsRingEviction(t *testing.T) {
	m := NewManifest()
	for i := 0; i < OpsRingSize+8; i++ {
		m.RecordOp(NewOpID())
	}
	if len(m.Ops) != OpsRingSize {
		t.Fatalf("ring size = %d, want %d", len(m.Ops), OpsRingSize)
	}
	evicted := NewOpID()
	m2 := NewManifest()
	m2.RecordOp(evicted)
	for i := 0; i < OpsRingSize; i++ {
		m2.RecordOp(NewOpID())
	}
	if m2.OpApplied(evicted) {
		t.Fatal("evicted op-id still reported applied via the ring")
	}
	// The per-entry record keeps testifying after eviction.
	m2.Reservations = map[string]*Reservation{"k": {Owner: "o", Bytes: 1, OpID: evicted}}
	if !m2.OpApplied(evicted) {
		t.Fatal("per-entry op-id record must outlive ring eviction")
	}
}

// TestClkNextMonotonic pins §2.4 rule 1: the clock never regresses, even
// across backward local-clock jumps, and always advances ≥ 1ms per write.
func TestClkNextMonotonic(t *testing.T) {
	now := time.Now().UTC()
	prev := now.Add(time.Hour) // manifest clock ahead: local clock jumped back
	next := ClkNext(now, prev)
	if want := prev.Add(time.Millisecond); !next.Equal(want) {
		t.Fatalf("backward jump: ClkNext = %v, want prev+1ms %v", next, want)
	}
	// Normal case: wall clock ahead of the manifest clock.
	if next := ClkNext(now, now.Add(-time.Minute)); !next.Equal(now) {
		t.Fatalf("normal advance: ClkNext = %v, want %v", next, now)
	}
}

// TestElapsedClockRules pins §2.4 rule 2: a backward local jump charges the
// smaller local reading (destruction delayed, never accelerated).
func TestElapsedClockRules(t *testing.T) {
	now := time.Now().UTC()
	stamp := now.Add(-10 * time.Minute)
	if got := Elapsed(now, now.Add(-time.Minute), stamp); got != 10*time.Minute {
		t.Fatalf("normal elapsed = %v, want 10m", got)
	}
	// Manifest clock 30m ahead (our clock jumped back): elapsed still
	// charges only localNow − stamp.
	if got := Elapsed(now, now.Add(30*time.Minute), stamp); got != 10*time.Minute {
		t.Fatalf("backward-jump elapsed = %v, want 10m", got)
	}
}

// TestSkewGuard pins the §2.4 fail-closed guard: a manifest clock ahead of
// the local clock past min(15min, ttl/4) demotes destruction, as does a
// wall-vs-monotonic drift past the same bound (a local-clock jump during
// this process's lifetime); an old manifest clock (quiet repo) is normal.
func TestSkewGuard(t *testing.T) {
	now := time.Now().UTC()
	if !SkewOK(now, now.Add(-24*time.Hour), 0, time.Hour) {
		t.Fatal("a quiet manifest (old clk) must not trip the skew guard")
	}
	if !SkewOK(now, now.Add(10*time.Second), 0, time.Minute) {
		t.Fatal("clk 10s ahead within ttl/4=15s must pass")
	}
	if SkewOK(now, now.Add(20*time.Second), 0, time.Minute) {
		t.Fatal("clk 20s ahead of a 1m ttl (bound 15s) must trip the guard")
	}
	if SkewOK(now, now.Add(16*time.Minute), 0, 24*time.Hour) {
		t.Fatal("the guard must cap at 15m regardless of ttl")
	}
	if !SkewOK(now, now, 10*time.Second, time.Minute) {
		t.Fatal("wall drift within ttl/4=15s must pass")
	}
	if SkewOK(now, now, 20*time.Second, time.Minute) {
		t.Fatal("a 20s forward jump against a 1m ttl (bound 15s) must trip the guard")
	}
	if SkewOK(now, now, -20*time.Second, time.Minute) {
		t.Fatal("the drift arm is symmetric: a backward jump must trip it too")
	}
	if SkewOK(now, now.Add(-24*time.Hour), 16*time.Minute, 24*time.Hour) {
		t.Fatal("drift past the 15m cap must trip the guard even on a quiet manifest")
	}
	if DriftOK(20*time.Second, time.Minute) {
		t.Fatal("DriftOK alone must trip past ttl/4")
	}
	if !DriftOK(time.Hour, time.Nanosecond) {
		t.Fatal("a degenerate sub-4ns window disables the drift arm (noise-indistinguishable)")
	}
}

// TestManifestKeyAlphabet pins the §2.9 member-contiguity lemma for the key
// shapes conformance tests actually use (test-derived names with slashes,
// underscores, uppercase) plus the prefix families the plan calls out:
// with extension-free keys and every base byte > '+', a base's member keys
// sort contiguously.
func TestManifestKeyAlphabet(t *testing.T) {
	for _, ok := range []string{"abc", "abc-1", "r2", "TestFoo/s3wal", "A_b.c~x"} {
		if !ValidBaseRepoKey(ok) {
			t.Fatalf("ValidBaseRepoKey(%q) = false, want true", ok)
		}
		if got, valid := BaseFromManifestKey(ManifestKey(ok)); !valid || got != ok {
			t.Fatalf("BaseFromManifestKey(ManifestKey(%q)) = (%q, %v)", ok, got, valid)
		}
	}
	for _, bad := range []string{"", "a+b", "a b", "a#b", "a\nb", "a*", "\x7fa", "ümlaut"} {
		if ValidBaseRepoKey(bad) {
			t.Fatalf("ValidBaseRepoKey(%q) = true, want false", bad)
		}
	}
	// Contiguity proof for a prefix family: every member of "abc" sorts
	// before the next base "abc-1" (and "abc-1"'s members before "abcd"),
	// because '+' sorts below every allowed base byte.
	family := []string{
		"abc", "abc+ephemeral", "abc+import",
		"abc-1", "abc-1+ephemeral",
		"abcd",
	}
	for i := 1; i < len(family); i++ {
		if !(family[i-1] < family[i]) {
			t.Fatalf("contiguity ordering broken: %q !< %q", family[i-1], family[i])
		}
	}
	// The extension-free invariant: a ".json" suffix WOULD corrupt the
	// order between prefix-related bases (regression guard for §2.1).
	if !("abc-1.json" < "abc.json") {
		t.Fatal("sanity: extension suffixes reorder prefix-related ids")
	}
}

// TestDecodeSeqHook pins the casobj watermark hook on real manifest bytes.
func TestDecodeSeqHook(t *testing.T) {
	m := NewManifest()
	body, err := m.EncodeAdvance(ClkNext(time.Now().UTC(), m.Clk))
	if err != nil {
		t.Fatal(err)
	}
	if seq, ok := DecodeSeq(body); !ok || seq != 1 {
		t.Fatalf("DecodeSeq = (%d, %v), want (1, true)", seq, ok)
	}
	if _, ok := DecodeSeq([]byte(`{"seq":3}`)); ok {
		t.Fatal("DecodeSeq accepted a body without the v1 marker")
	}
	if _, ok := DecodeSeq([]byte("not json")); ok {
		t.Fatal("DecodeSeq accepted garbage")
	}
}

// TestMemCASConditionalSemantics pins the fake's store fidelity: duplicate
// creates 412, stale advances 412, matched advances chain, byte-ordered
// List, and the stale-read hook serving the previous version.
func TestMemCASConditionalSemantics(t *testing.T) {
	ctx := context.Background()
	mc := NewMemCAS()
	etag1, err := mc.Create(ctx, "_wal/repo/a", []byte("v1"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := mc.Create(ctx, "_wal/repo/a", []byte("v1-dup")); err == nil || !isErr(err, ErrExists) {
		t.Fatalf("duplicate create = %v, want ErrExists", err)
	}
	etag2, err := mc.Advance(ctx, "_wal/repo/a", []byte("v2"), etag1)
	if err != nil || etag2 == etag1 {
		t.Fatalf("advance = (%q, %v), want new etag", etag2, err)
	}
	if _, err := mc.Advance(ctx, "_wal/repo/a", []byte("v3"), etag1); err == nil || !isErr(err, ErrLostRace) {
		t.Fatalf("stale advance = %v, want ErrLostRace", err)
	}
	if _, err := mc.Get(ctx, "_wal/repo/missing"); err == nil || !isErr(err, ErrNotExist) {
		t.Fatalf("missing get = %v, want ErrNotExist", err)
	}
	if _, err := mc.GetIfChanged(ctx, "_wal/repo/a", etag2); err == nil || !isErr(err, ErrNotModified) {
		t.Fatalf("unchanged conditional get = %v, want ErrNotModified", err)
	}

	// Byte-ordered listing with exclusive cursor.
	for _, k := range []string{"_wal/repo/b", "_wal/repo/a-1", "_wal/repo/a+e"} {
		if _, err := mc.Create(ctx, k, []byte("x")); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := mc.List(ctx, "_wal/repo/", "", 10)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"_wal/repo/a", "_wal/repo/a+e", "_wal/repo/a-1", "_wal/repo/b"}
	if len(keys) != len(want) {
		t.Fatalf("list = %v, want %v", keys, want)
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Fatalf("list[%d] = %q, want %q (byte order)", i, keys[i], want[i])
		}
	}
	after, err := mc.List(ctx, "_wal/repo/", "_wal/repo/a+e", 10)
	if err != nil || len(after) != 2 || after[0] != "_wal/repo/a-1" {
		t.Fatalf("list after cursor = (%v, %v)", after, err)
	}

	// Stale-read injection serves the previous version once requested.
	stale := true
	mc.Hooks.StaleGet = func(key string) bool { return key == "_wal/repo/a" && stale }
	obj, err := mc.Get(ctx, "_wal/repo/a")
	if err != nil || string(obj.Body) != "v1" || obj.ETag != etag1 {
		t.Fatalf("stale get = (%q, %q, %v), want previous version v1", obj.Body, obj.ETag, err)
	}
	stale = false
	obj, err = mc.Get(ctx, "_wal/repo/a")
	if err != nil || string(obj.Body) != "v2" {
		t.Fatalf("fresh get = (%q, %v), want v2", obj.Body, err)
	}
}

// TestMemCASFaultInjection pins the §2.3 fault classes: ambiguous-applied
// commits the write, ambiguous-dropped does not, and both surface
// *AmbiguousError; not-executed and contention surface their sentinels
// without applying.
func TestMemCASFaultInjection(t *testing.T) {
	ctx := context.Background()
	mc := NewMemCAS()
	etag, err := mc.Create(ctx, "k", []byte("v1"))
	if err != nil {
		t.Fatal(err)
	}

	fault := FaultAmbiguousApplied
	mc.Hooks.PutFault = func(op, key string) PutFault { return fault }
	_, err = mc.Advance(ctx, "k", []byte("v2"), etag)
	var amb *AmbiguousError
	if !asErr(err, &amb) {
		t.Fatalf("ambiguous-applied advance = %v, want *AmbiguousError", err)
	}
	mc.Hooks.PutFault = nil
	obj, err := mc.Get(ctx, "k")
	if err != nil || string(obj.Body) != "v2" {
		t.Fatalf("ambiguous-applied write must land: get = (%q, %v)", obj.Body, err)
	}

	fault = FaultAmbiguousDropped
	mc.Hooks.PutFault = func(op, key string) PutFault { return fault }
	_, err = mc.Advance(ctx, "k", []byte("v3"), obj.ETag)
	if !asErr(err, &amb) {
		t.Fatalf("ambiguous-dropped advance = %v, want *AmbiguousError", err)
	}
	mc.Hooks.PutFault = nil
	if got, _ := mc.Get(ctx, "k"); string(got.Body) != "v2" {
		t.Fatalf("ambiguous-dropped write must NOT land: got %q", got.Body)
	}

	for _, tc := range []struct {
		fault PutFault
		want  error
	}{{FaultNotExecuted, ErrNotExecuted}, {FaultContention, ErrCASContention}} {
		mc.Hooks.PutFault = func(op, key string) PutFault { return tc.fault }
		if _, err := mc.Advance(ctx, "k", []byte("v4"), obj.ETag); err == nil || !isErr(err, tc.want) {
			t.Fatalf("fault %d: advance = %v, want %v", tc.fault, err, tc.want)
		}
		mc.Hooks.PutFault = nil
		if got, _ := mc.Get(ctx, "k"); string(got.Body) != "v2" {
			t.Fatalf("fault %d must not apply: got %q", tc.fault, got.Body)
		}
	}
}

func isErr(err, target error) bool { return err != nil && errors.Is(err, target) }

func asErr[T error](err error, target *T) bool { return err != nil && errors.As(err, target) }
