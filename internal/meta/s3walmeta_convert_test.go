package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// v2OnlyKeys and v3OnlyKeys are the JSON names ADR-51 adds in v2 and v3.
var (
	v2OnlyKeys = []string{"create_state", "push_attempt", "push_due", "sync_due"}
	v3OnlyKeys = []string{"default_branch_omitted"}
)

// storedS3WALMeta decodes the stored document at any known version.
func storedS3WALMeta(t *testing.T, cas s3wal.ConditionalStore) (*s3walMetaDoc, []byte) {
	t.Helper()
	obj, err := cas.Get(context.Background(), s3walMetaKey)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := decodeS3WALMetaDocVersion(obj.Body, s3walConvertVersions...)
	if err != nil {
		t.Fatal(err)
	}
	return doc, obj.Body
}

// putS3WALMeta overwrites the stored document with doc as version v.
func putS3WALMeta(t *testing.T, cas s3wal.ConditionalStore, doc *s3walMetaDoc, v int) {
	t.Helper()
	ctx := context.Background()
	obj, err := cas.Get(ctx, s3walMetaKey)
	if err != nil {
		t.Fatal(err)
	}
	doc.V = v
	body, err := doc.encodeNext()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Advance(ctx, s3walMetaKey, body, obj.ETag); err != nil {
		t.Fatal(err)
	}
}

// expectedS3WALMeta restates the ADR-51 rule on a JSON copy of src. It is
// close to convertS3WALMetaDoc on purpose: the tests check the stored
// bytes and the round trip against it, not a second algorithm.
func expectedS3WALMeta(t *testing.T, src *s3walMetaDoc, to int) *s3walMetaDoc {
	t.Helper()
	raw, err := json.Marshal(src)
	if err != nil {
		t.Fatal(err)
	}
	var want s3walMetaDoc
	if err := json.Unmarshal(raw, &want); err != nil {
		t.Fatal(err)
	}
	want.V = to
	if to < s3walMetaV3 {
		for _, u := range want.Upstreams {
			u.DefaultBranchOmitted = false
		}
	}
	if to == s3walMetaV1 {
		for id, r := range want.Repos {
			// An unfinished sync create keeps its owed first sync.
			if u := want.Upstreams[id]; u != nil && !u.Detaching && r.DeletedAt == 0 && CreateState(r.CreateState) == CreateStatePending {
				u.SyncPending = true
			}
			r.CreateState = 0
		}
		for _, u := range want.Upstreams {
			u.PushAttempt, u.PushDue, u.SyncDue = 0, 0, 0
			u.SyncPending = u.SyncPending || u.Syncing
		}
	}
	return &want
}

// assertConverted checks the stored document against the rule, apart
// from seq and writer, and checks a body names no field of a later version.
func assertConverted(t *testing.T, cas s3wal.ConditionalStore, src *s3walMetaDoc, to int) {
	t.Helper()
	got, body := storedS3WALMeta(t, cas)
	want := expectedS3WALMeta(t, src, to)
	want.Seq, want.Writer = got.Seq, got.Writer
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored document:\n%+v\nwant:\n%+v", got, want)
	}
	for _, k := range v3OnlyKeys {
		if to < s3walMetaV3 && strings.Contains(string(body), `"`+k+`"`) {
			t.Fatalf("v%d body names v3 field %q: %s", to, k, body)
		}
	}
	for _, k := range v2OnlyKeys {
		if to == s3walMetaV1 && strings.Contains(string(body), `"`+k+`"`) {
			t.Fatalf("v1 body names v2 field %q: %s", k, body)
		}
	}
}

// seedS3WALMetaClaim seeds every table and leaves the pull lane claimed,
// as a process that stopped while it held the claim leaves it. It stores
// the result as v1, as a v1 release wrote it (no v2 field is set).
func seedS3WALMetaClaim(t *testing.T, cas *s3wal.MemCAS) (string, *s3walMetaDoc) {
	t.Helper()
	ctx := context.Background()
	st := NewS3WAL(cas)
	t.Cleanup(st.Close)
	repoID := seedS3WALUpstream(t, st)
	if _, err := st.CreateWebhook(ctx, "org", "https://hooks.example.com/x", "s3cret"); err != nil {
		t.Fatal(err)
	}
	if err := st.RequestSync(ctx, repoID); err != nil {
		t.Fatal(err)
	}
	if _, ok, _, err := st.TryBeginSync(ctx, repoID, "dead-holder", time.Hour); err != nil || !ok {
		t.Fatalf("TryBeginSync = %v, %v", ok, err)
	}
	doc, _ := storedS3WALMeta(t, cas)
	if u := doc.Upstreams[repoID]; !u.Syncing || u.SyncPending {
		t.Fatalf("seeded lane = %+v, want a claim that consumed sync_pending", u)
	}
	putS3WALMeta(t, cas, doc, 1)
	doc, _ = storedS3WALMeta(t, cas)
	return repoID, doc
}

func TestS3WALMetaConvert(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	repoID, v1 := seedS3WALMetaClaim(t, cas)

	res, err := ConvertS3WALMeta(ctx, cas, 3, S3WALConvertOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing || res.From != 1 || res.To != 3 || res.SeqAfter != res.SeqBefore+1 || res.Before != res.After || res.Before.Repos != 1 || res.Before.Webhooks != 1 {
		t.Fatalf("convert to 3 = %+v", res)
	}
	assertConverted(t, cas, v1, 3)

	again, err := ConvertS3WALMeta(ctx, cas, 3, S3WALConvertOptions{})
	if err != nil || again.From != 3 || again.SeqAfter != res.SeqAfter {
		t.Fatalf("re-run = %+v, %v; want no write at seq %d", again, err, res.SeqAfter)
	}

	// Give the v3 document values in every v2 and v3 field, then go back.
	v3, _ := storedS3WALMeta(t, cas)
	for _, r := range v3.Repos {
		r.CreateState = int(CreateStateComplete)
	}
	v3.Upstreams[repoID].PushAttempt, v3.Upstreams[repoID].PushDue, v3.Upstreams[repoID].SyncDue = 3, 42, 43
	v3.Upstreams[repoID].DefaultBranchOmitted = true
	putS3WALMeta(t, cas, v3, 3)
	v3, _ = storedS3WALMeta(t, cas)
	if _, err := ConvertS3WALMeta(ctx, cas, 1, S3WALConvertOptions{}); err != nil {
		t.Fatal(err)
	}
	assertConverted(t, cas, v3, 1)

	// The normal (v3) path refuses the v1 document and writes nothing.
	st := NewS3WAL(cas)
	defer st.Close()
	v1Seq := res.SeqAfter + 2
	if _, err := st.UpsertOrgWithKeys(ctx, "new", []string{"pem"}); err == nil || !strings.Contains(err.Error(), "convert --to 3") {
		t.Fatalf("v3 write on v1 = %v, want the conversion hint", err)
	}
	if got, _ := storedS3WALMeta(t, cas); got.V != 1 || got.Seq != v1Seq {
		t.Fatalf("refused write left v%d seq %d, want v1 seq %d", got.V, got.Seq, v1Seq)
	}
	// Back to v3: the claimed lane is still claimed and pending, so the
	// sweep runs it once the claim expires.
	if _, err := ConvertS3WALMeta(ctx, cas, 3, S3WALConvertOptions{}); err != nil {
		t.Fatal(err)
	}
	if up, err := st.GetRepoUpstream(ctx, repoID); err != nil || !up.Syncing || !up.SyncPending {
		t.Fatalf("lane after --to 3 = %+v, %v; want claimed and pending", up, err)
	}
}

// --to 1 records the first sync an unfinished sync create is owed, and
// refuses while a create without an upstream link is unfinished: on v1 it
// would read as a finished, possibly empty repository (#452 item 5,
// Option B).
func TestS3WALMetaConvertPendingCreates(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := NewS3WAL(cas)
	defer st.Close()
	if _, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	up := RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}
	syncCreate, err := st.CreateLinkedRepo(ctx, "org", "sync-pending", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	detached, err := st.CreateLinkedRepo(ctx, "org", "sync-detached", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.DeleteRepoUpstream(ctx, detached.RepoID); err != nil {
		t.Fatal(err)
	}
	fork, err := st.CreateRepoWithState(ctx, "org", "fork-pending", "main", CreateStatePending)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateRepoWithState(ctx, "org", "fork-deleted", "main", CreateStatePending); err != nil {
		t.Fatal(err)
	}
	if _, err := st.SoftDeleteRepo(ctx, "org", "fork-deleted"); err != nil {
		t.Fatal(err)
	}
	if _, err := st.CreateRepoWithState(ctx, "org", "done", "main", CreateStateComplete); err != nil {
		t.Fatal(err)
	}
	before, _ := storedS3WALMeta(t, cas)

	// Refused: both unfinished creates without a live link are named, and
	// nothing is written. A deleted pending repo is not named.
	_, err = ConvertS3WALMeta(ctx, cas, 1, S3WALConvertOptions{})
	pe, ok := errors.AsType[*S3WALPendingCreatesError](err)
	want := []S3WALPendingCreate{{fork.RepoID, "fork-pending"}, {detached.RepoID, "sync-detached"}}
	slices.SortFunc(want, func(a, b S3WALPendingCreate) int { return strings.Compare(a.RepoID, b.RepoID) })
	if !ok || !slices.Equal(pe.Creates, want) || !strings.Contains(err.Error(), "DELETE /repos/delete") || !strings.Contains(err.Error(), "--force") {
		t.Fatalf("--to 1 with unfinished forks = %v, want a refusal naming %v", err, want)
	}
	if got, _ := storedS3WALMeta(t, cas); got.V != 3 || got.Seq != before.Seq {
		t.Fatalf("refusal wrote v%d seq %d, want v3 seq %d", got.V, got.Seq, before.Seq)
	}

	// Forced: converted, the sync create keeps its owed first sync, and
	// the result names what it repaired and what it forced.
	res, err := ConvertS3WALMeta(ctx, cas, 1, S3WALConvertOptions{Force: true})
	if err != nil || !slices.Equal(res.RepairedSyncCreates, []string{syncCreate.RepoID}) || !slices.Equal(res.ForcedCreates, want) {
		t.Fatalf("forced --to 1 = %+v, %v", res, err)
	}
	got, _ := storedS3WALMeta(t, cas)
	if got.V != 1 || !got.Upstreams[syncCreate.RepoID].SyncPending {
		t.Fatalf("forced --to 1 left v%d, sync create lane %+v; want v1 with the first sync pending", got.V, got.Upstreams[syncCreate.RepoID])
	}
	assertConverted(t, cas, before, 1)
}

// With only unfinished sync creates, --to 1 needs no force.
func TestS3WALMetaConvertRepairsSyncCreates(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := NewS3WAL(cas)
	defer st.Close()
	if _, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	r, err := st.CreateLinkedRepo(ctx, "org", "sync-pending", "main",
		RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := ConvertS3WALMeta(ctx, cas, 1, S3WALConvertOptions{})
	if err != nil || !slices.Equal(res.RepairedSyncCreates, []string{r.RepoID}) || len(res.ForcedCreates) != 0 {
		t.Fatalf("--to 1 = %+v, %v; want the sync create repaired", res, err)
	}
	if got, _ := storedS3WALMeta(t, cas); got.V != 1 || !got.Upstreams[r.RepoID].SyncPending || got.Repos[r.RepoID].CreateState != 0 {
		t.Fatalf("stored v%d lane %+v repo %+v; want v1, first sync pending, legacy state", got.V, got.Upstreams[r.RepoID], got.Repos[r.RepoID])
	}
}

func TestS3WALMetaConvertEdges(t *testing.T) {
	ctx := context.Background()
	if res, err := ConvertS3WALMeta(ctx, s3wal.NewMemCAS(), 3, S3WALConvertOptions{}); err != nil || !res.Missing {
		t.Fatalf("missing document = %+v, %v", res, err)
	}
	// Empty tables, which the encoder omits, still verify.
	empty := s3wal.NewMemCAS()
	if _, err := empty.Create(ctx, s3walMetaKey, []byte(`{"v":1,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"created_at":1,"public_keys":[],"signing_keys":[]}},"repos":{}}`)); err != nil {
		t.Fatal(err)
	}
	if res, err := ConvertS3WALMeta(ctx, empty, 3, S3WALConvertOptions{}); err != nil || res.SeqAfter != 2 || res.After.Orgs != 1 {
		t.Fatalf("empty-table document = %+v, %v", res, err)
	}
	cas := s3wal.NewMemCAS()
	_, src := seedS3WALMetaClaim(t, cas)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ConvertS3WALMeta(cctx, cas, 3, S3WALConvertOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled convert = %v", err)
	}
	if got, _ := storedS3WALMeta(t, cas); got.V != 1 || got.Seq != src.Seq {
		t.Fatalf("cancelled convert wrote seq %d v%d", got.Seq, got.V)
	}
}

func TestS3WALMetaConvertAmbiguous(t *testing.T) {
	for name, f := range map[string]s3wal.PutFault{"applied": s3wal.FaultAmbiguousApplied, "dropped": s3wal.FaultAmbiguousDropped} {
		t.Run(name, func(t *testing.T) {
			cas := s3wal.NewMemCAS()
			_, src := seedS3WALMetaClaim(t, cas)
			armAmbiguousPut(t, cas, f, func() {})
			res, err := ConvertS3WALMeta(context.Background(), cas, 3, S3WALConvertOptions{})
			if err != nil || res.SeqAfter != src.Seq+1 {
				t.Fatalf("convert = %+v, %v; want exactly one write", res, err)
			}
			assertConverted(t, cas, src, 3)
		})
	}
}

// cancelOnGetCAS cancels once, during the first Get of the meta key: the
// fence is lost while the conversion reads.
type cancelOnGetCAS struct {
	*s3wal.MemCAS
	cancel func()
}

func (c *cancelOnGetCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	obj, err := c.MemCAS.Get(ctx, key)
	if f := c.cancel; f != nil && key == s3walMetaKey {
		c.cancel = nil
		f()
	}
	return obj, err
}

func TestS3WALMetaConvertCancelDuringRead(t *testing.T) {
	mem := s3wal.NewMemCAS()
	_, src := seedS3WALMetaClaim(t, mem)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if _, err := ConvertS3WALMeta(ctx, &cancelOnGetCAS{MemCAS: mem, cancel: cancel}, 3, S3WALConvertOptions{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("convert = %v, want context.Canceled", err)
	}
	if got, _ := storedS3WALMeta(t, mem); got.V != src.V || got.Seq != src.Seq {
		t.Fatalf("cancel during the read wrote v%d seq %d", got.V, got.Seq)
	}
}

// raceCAS runs before once, ahead of the first Advance of the meta key.
type raceCAS struct {
	*s3wal.MemCAS
	before func()
}

func (r *raceCAS) Advance(ctx context.Context, key string, body []byte, etag string) (string, error) {
	if f := r.before; f != nil && key == s3walMetaKey {
		r.before = nil
		f()
	}
	return r.MemCAS.Advance(ctx, key, body, etag)
}

// A writer that the fence missed: its write between the conversion's read
// and write survives, the conversion applies to it, and its next CAS, made
// on the ETag it read, loses without changing the converted document.
func TestS3WALMetaConvertRace(t *testing.T) {
	for _, tc := range []struct{ from, to int }{{1, 3}, {2, 3}, {3, 2}, {3, 1}} {
		from, to := tc.from, tc.to
		t.Run(fmt.Sprintf("v%dtov%d", from, to), func(t *testing.T) {
			ctx := context.Background()
			mem := s3wal.NewMemCAS()
			repoID, _ := seedS3WALMetaClaim(t, mem)
			if from != 1 {
				if _, err := ConvertS3WALMeta(ctx, mem, from, S3WALConvertOptions{}); err != nil {
					t.Fatal(err)
				}
			}
			var racedETag string
			cas := &raceCAS{MemCAS: mem, before: func() {
				doc, _ := storedS3WALMeta(t, mem)
				doc.NextOrgID++ // mint like a real writer: decode rejects ids above the counter (#478)
				doc.Orgs["raced"] = &s3walMetaOrg{ID: doc.NextOrgID, PublicKeys: []string{"pem"}}
				putS3WALMeta(t, mem, doc, from)
				obj, err := mem.Get(ctx, s3walMetaKey)
				if err != nil {
					t.Error(err)
				}
				racedETag = obj.ETag
			}}
			if _, err := ConvertS3WALMeta(ctx, cas, to, S3WALConvertOptions{}); err != nil {
				t.Fatal(err)
			}
			got, body := storedS3WALMeta(t, mem)
			// Only --to 1 re-marks the claimed pull as pending.
			if got.V != to || got.Orgs["raced"] == nil || got.Upstreams[repoID].SyncPending != (to == 1) {
				t.Fatalf("converted document v%d orgs %v lane %+v, want v%d with the raced org (pull pending: %v)", got.V, got.Orgs, got.Upstreams[repoID], to, to == 1)
			}
			if _, err := mem.Advance(ctx, s3walMetaKey, body, racedETag); !errors.Is(err, s3wal.ErrLostRace) {
				t.Fatalf("late CAS = %v, want a lost race", err)
			}
		})
	}
}

// Zero-value rule property: for random v3 documents d, to3(to2(to1(d))) is d
// with the v2 and v3 fields cleared and sync_pending set on claimed pull
// lanes and on unfinished sync creates, and to1(d) equals to1(to2(d)).
func TestS3WALMetaConvertProperty(t *testing.T) {
	rng := mrand.New(mrand.NewPCG(51, 51))
	for i := range 300 {
		d := &s3walMetaDoc{V: 3, Seq: 1, Writer: "w", Repos: map[string]*s3walMetaRepo{}, Upstreams: map[string]*s3walMetaUpstream{}}
		for j := range rng.IntN(4) {
			id := fmt.Sprintf("repo_%012x", j) // the minted id shape decode requires (issue #492)
			d.Repos[id] = &s3walMetaRepo{OrgID: 1, URL: id, DefaultBranch: "main", CreateState: rng.IntN(3), DeletedAt: rng.Int64N(2)}
			u := &s3walMetaUpstream{
				Owner: "o", Name: "n", UpstreamHost: "h",
				Syncing: rng.IntN(2) == 0, SyncPending: rng.IntN(2) == 0, PushSyncing: rng.IntN(2) == 0,
				PushPending: rng.IntN(3), PushAttempt: rng.IntN(6), PushDue: rng.Int64N(3), SyncDue: rng.Int64N(3),
				DefaultBranchOmitted: rng.IntN(2) == 0,
			}
			// A held claim has its holder, as every writer leaves it
			// (decode rejects holderless claims, issue #481).
			if u.Syncing {
				u.SyncClaimHolder = "h"
			}
			if u.PushSyncing {
				u.PushClaimHolder = "p"
			}
			d.Upstreams[id] = u
		}
		body, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		one, _, err := convertS3WALMetaDoc(body, 1)
		if err != nil {
			t.Fatal(err)
		}
		oneBody, err := json.Marshal(one)
		if err != nil {
			t.Fatal(err)
		}
		two, _, err := convertS3WALMetaDoc(oneBody, 2)
		if err != nil {
			t.Fatal(err)
		}
		twoBody, err := json.Marshal(two)
		if err != nil {
			t.Fatal(err)
		}
		three, _, err := convertS3WALMetaDoc(twoBody, 3)
		if err != nil {
			t.Fatal(err)
		}
		want := expectedS3WALMeta(t, d, 1)
		want.V = 3
		if !reflect.DeepEqual(three, want) {
			t.Fatalf("case %d: to3(to2(to1(d))) =\n%+v\nwant\n%+v", i, three, want)
		}
		// Dropping the v3 field first changes nothing about the v1 result.
		viaTwo, _, err := convertS3WALMetaDoc(mustMarshalS3WALMeta(t, expectedS3WALMeta(t, d, 2)), 1)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(viaTwo, one) {
			t.Fatalf("case %d: to1(to2(d)) =\n%+v\nwant to1(d) =\n%+v", i, viaTwo, one)
		}
	}
}

func mustMarshalS3WALMeta(t *testing.T, doc *s3walMetaDoc) []byte {
	t.Helper()
	body, err := json.Marshal(doc)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// seedV3Flags returns a stored document at version v with two upstreams,
// one of them with the v3 default_branch_omitted flag, so each conversion
// direction has a field to keep or drop.
func seedV3Flags(t *testing.T, v int) (*s3wal.MemCAS, string, string) {
	t.Helper()
	cas := s3wal.NewMemCAS()
	st := NewS3WAL(cas)
	t.Cleanup(st.Close)
	if _, err := st.UpsertOrgWithKeys(context.Background(), "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	up := RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com"}
	flagged, err := st.CreateLinkedRepo(context.Background(), "org", "flagged", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := st.CreateLinkedRepo(context.Background(), "org", "plain", "main", up, nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, _ := storedS3WALMeta(t, cas)
	doc.Upstreams[flagged.RepoID].DefaultBranchOmitted = v == s3walMetaV3
	putS3WALMeta(t, cas, doc, v)
	return cas, flagged.RepoID, plain.RepoID
}

// TestS3WALMetaConvertV3ZeroValue pins the ADR-51 zero-value rule for the
// v3 field default_branch_omitted (issue #603): going up to v3 changes
// only the version, and going down drops the field alone, so a repository
// that still waits for adoption keeps its branch.
func TestS3WALMetaConvertV3ZeroValue(t *testing.T) {
	ctx := context.Background()
	// The decoded flag reaches the interface type, and it survives a
	// document round trip, so a v3 writer finds what it stored.
	cas, flagged, plain := seedV3Flags(t, 3)
	st := NewS3WAL(cas)
	defer st.Close()
	for id, want := range map[string]bool{flagged: true, plain: false} {
		if up, err := st.GetRepoUpstream(ctx, id); err != nil || up.DefaultBranchOmitted != want {
			t.Fatalf("upstream %s: DefaultBranchOmitted = %v, %v; want %v", id, up.DefaultBranchOmitted, err, want)
		}
	}
	if _, err := st.UpsertOrgWithKeys(ctx, "other", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	if doc, _ := storedS3WALMeta(t, cas); !doc.Upstreams[flagged].DefaultBranchOmitted {
		t.Fatal("a v3 write dropped default_branch_omitted")
	}

	for _, tc := range []struct{ from, to int }{{3, 2}, {3, 1}, {2, 3}, {1, 3}, {1, 2}, {2, 1}} {
		t.Run(fmt.Sprintf("v%dtov%d", tc.from, tc.to), func(t *testing.T) {
			cas, _, _ := seedV3Flags(t, tc.from)
			src, _ := storedS3WALMeta(t, cas)
			if _, err := ConvertS3WALMeta(ctx, cas, tc.to, S3WALConvertOptions{}); err != nil {
				t.Fatal(err)
			}
			assertConverted(t, cas, src, tc.to)
		})
	}

	// The target check names every version.
	for _, to := range []int{0, 4} {
		if _, err := ConvertS3WALMeta(ctx, s3wal.NewMemCAS(), to, S3WALConvertOptions{}); err == nil || !strings.Contains(err.Error(), "want 1, 2 or 3") {
			t.Fatalf("--to %d = %v, want a refusal naming 1, 2 or 3", to, err)
		}
	}
}

// TestS3WALUpdateRepoDefaultBranchClearsFlag (issue #603): an explicit
// default-branch change ends the wait for adoption in the same write, as on
// PostgreSQL and SQLite. A lost compare-and-swap and another repository's
// change leave the flag alone.
func TestS3WALUpdateRepoDefaultBranchClearsFlag(t *testing.T) {
	ctx := context.Background()
	cas, flagged, plain := seedV3Flags(t, 3)
	st := NewS3WAL(cas)
	defer st.Close()
	omitted := func(repoID string) bool {
		t.Helper()
		up, err := st.GetRepoUpstream(ctx, repoID)
		if err != nil {
			t.Fatal(err)
		}
		return up.DefaultBranchOmitted
	}
	patch := func(url, from, to string, wantWon bool) {
		t.Helper()
		if won, err := st.UpdateRepoDefaultBranch(ctx, "org", url, from, to); err != nil || won != wantWon {
			t.Fatalf("PATCH %s %s -> %s = %v, %v; want won=%v", url, from, to, won, err, wantWon)
		}
	}
	patch("flagged", "stale", "dev", false)
	patch("plain", "main", "dev", true)
	if !omitted(flagged) {
		t.Fatal("a lost compare-and-swap or another repository's change cleared the flag")
	}
	patch("flagged", "main", "main", true) // the value does not change, the wait still ends
	if omitted(flagged) || omitted(plain) {
		t.Fatalf("after the PATCH: flagged %v, plain %v; want both clear", omitted(flagged), omitted(plain))
	}
}
