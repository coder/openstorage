package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	mrand "math/rand/v2"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// v2OnlyKeys are the JSON names ADR-51 adds in v2.
var v2OnlyKeys = []string{"create_state", "push_attempt", "push_due", "sync_due"}

// storedS3WALMeta decodes the stored document at any known version.
func storedS3WALMeta(t *testing.T, cas s3wal.ConditionalStore) (*s3walMetaDoc, []byte) {
	t.Helper()
	obj, err := cas.Get(context.Background(), s3walMetaKey)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := decodeS3WALMetaDocVersion(obj.Body, s3walMetaV1, s3walMetaV2)
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

// expectedS3WALMeta is the conversion written a second way: from the
// ADR-51 rule, not from convertS3WALMetaDoc.
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
	if to == s3walMetaV1 {
		for _, r := range want.Repos {
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
// from seq and writer, and checks a v1 body names no v2 field.
func assertConverted(t *testing.T, cas s3wal.ConditionalStore, src *s3walMetaDoc, to int) {
	t.Helper()
	got, body := storedS3WALMeta(t, cas)
	want := expectedS3WALMeta(t, src, to)
	want.Seq, want.Writer = got.Seq, got.Writer
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("stored document:\n%+v\nwant:\n%+v", got, want)
	}
	if to == s3walMetaV1 {
		for _, k := range v2OnlyKeys {
			if strings.Contains(string(body), `"`+k+`"`) {
				t.Fatalf("v1 body names v2 field %q: %s", k, body)
			}
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

	res, err := ConvertS3WALMeta(ctx, cas, 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Missing || res.From != 1 || res.To != 2 || res.SeqAfter != res.SeqBefore+1 || res.Before != res.After || res.Before.Repos != 1 || res.Before.Webhooks != 1 {
		t.Fatalf("convert to 2 = %+v", res)
	}
	assertConverted(t, cas, v1, 2)

	again, err := ConvertS3WALMeta(ctx, cas, 2)
	if err != nil || again.From != 2 || again.SeqAfter != res.SeqAfter {
		t.Fatalf("re-run = %+v, %v; want no write at seq %d", again, err, res.SeqAfter)
	}

	// Give the v2 document values in every v2 field, then go back.
	v2, _ := storedS3WALMeta(t, cas)
	for _, r := range v2.Repos {
		r.CreateState = int(CreateStateComplete)
	}
	v2.Upstreams[repoID].PushAttempt, v2.Upstreams[repoID].PushDue, v2.Upstreams[repoID].SyncDue = 3, 42, 43
	putS3WALMeta(t, cas, v2, 2)
	v2, _ = storedS3WALMeta(t, cas)
	if _, err := ConvertS3WALMeta(ctx, cas, 1); err != nil {
		t.Fatal(err)
	}
	assertConverted(t, cas, v2, 1)

	// The normal (v2) path refuses the v1 document and writes nothing.
	st := NewS3WAL(cas)
	defer st.Close()
	v1Seq := res.SeqAfter + 2
	if _, err := st.UpsertOrgWithKeys(ctx, "new", []string{"pem"}); err == nil || !strings.Contains(err.Error(), "convert --to 2") {
		t.Fatalf("v2 write on v1 = %v, want the conversion hint", err)
	}
	if got, _ := storedS3WALMeta(t, cas); got.V != 1 || got.Seq != v1Seq {
		t.Fatalf("refused write left v%d seq %d, want v1 seq %d", got.V, got.Seq, v1Seq)
	}
	// Back to v2: the claimed lane is pending, and v2 boot lists it.
	if _, err := ConvertS3WALMeta(ctx, cas, 2); err != nil {
		t.Fatal(err)
	}
	ids, err := st.PendingSyncRepoIDs(ctx)
	if err != nil || len(ids) != 1 || ids[0] != repoID {
		t.Fatalf("PendingSyncRepoIDs = %v, %v", ids, err)
	}
}

func TestS3WALMetaConvertEdges(t *testing.T) {
	ctx := context.Background()
	if res, err := ConvertS3WALMeta(ctx, s3wal.NewMemCAS(), 2); err != nil || !res.Missing {
		t.Fatalf("missing document = %+v, %v", res, err)
	}
	// Empty tables, which the encoder omits, still verify.
	empty := s3wal.NewMemCAS()
	if _, err := empty.Create(ctx, s3walMetaKey, []byte(`{"v":1,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"created_at":1,"public_keys":[],"signing_keys":[]}},"repos":{}}`)); err != nil {
		t.Fatal(err)
	}
	if res, err := ConvertS3WALMeta(ctx, empty, 2); err != nil || res.SeqAfter != 2 || res.After.Orgs != 1 {
		t.Fatalf("empty-table document = %+v, %v", res, err)
	}
	cas := s3wal.NewMemCAS()
	_, src := seedS3WALMetaClaim(t, cas)
	cctx, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := ConvertS3WALMeta(cctx, cas, 2); !errors.Is(err, context.Canceled) {
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
			res, err := ConvertS3WALMeta(context.Background(), cas, 2)
			if err != nil || res.SeqAfter != src.Seq+1 {
				t.Fatalf("convert = %+v, %v; want exactly one write", res, err)
			}
			assertConverted(t, cas, src, 2)
		})
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

// A v1 writer that the fence missed: its write between the conversion's
// read and write survives, and its next CAS, made on the v1 ETag it read,
// loses without changing the v2 document.
func TestS3WALMetaConvertRace(t *testing.T) {
	ctx := context.Background()
	mem := s3wal.NewMemCAS()
	seedS3WALMetaClaim(t, mem)
	var v1ETag string
	cas := &raceCAS{MemCAS: mem, before: func() {
		doc, _ := storedS3WALMeta(t, mem)
		doc.Orgs["raced"] = &s3walMetaOrg{ID: 9, PublicKeys: []string{"pem"}}
		putS3WALMeta(t, mem, doc, 1)
		obj, err := mem.Get(ctx, s3walMetaKey)
		if err != nil {
			t.Error(err)
		}
		v1ETag = obj.ETag
	}}
	if _, err := ConvertS3WALMeta(ctx, cas, 2); err != nil {
		t.Fatal(err)
	}
	got, body := storedS3WALMeta(t, mem)
	if got.V != 2 || got.Orgs["raced"] == nil {
		t.Fatalf("converted document v%d orgs %v, want v2 with the raced org", got.V, got.Orgs)
	}
	if _, err := mem.Advance(ctx, s3walMetaKey, body, v1ETag); !errors.Is(err, s3wal.ErrLostRace) {
		t.Fatalf("late v1 CAS = %v, want a lost race", err)
	}
}

// Zero-value rule property: for random v2 documents d, to2(to1(d)) is d
// with the v2 fields cleared and sync_pending set on claimed pull lanes.
func TestS3WALMetaConvertProperty(t *testing.T) {
	rng := mrand.New(mrand.NewPCG(51, 51))
	for i := range 300 {
		d := &s3walMetaDoc{V: 2, Seq: 1, Writer: "w", Repos: map[string]*s3walMetaRepo{}, Upstreams: map[string]*s3walMetaUpstream{}}
		for j := range rng.IntN(4) {
			id := fmt.Sprintf("r%d", j)
			d.Repos[id] = &s3walMetaRepo{OrgID: 1, URL: id, CreateState: rng.IntN(3), DeletedAt: rng.Int64N(2)}
			d.Upstreams[id] = &s3walMetaUpstream{
				Syncing: rng.IntN(2) == 0, SyncPending: rng.IntN(2) == 0, PushSyncing: rng.IntN(2) == 0,
				PushPending: rng.IntN(3), PushAttempt: rng.IntN(6), PushDue: rng.Int64N(3), SyncDue: rng.Int64N(3),
			}
		}
		body, err := json.Marshal(d)
		if err != nil {
			t.Fatal(err)
		}
		one, err := convertS3WALMetaDoc(body, 1)
		if err != nil {
			t.Fatal(err)
		}
		oneBody, err := json.Marshal(one)
		if err != nil {
			t.Fatal(err)
		}
		two, err := convertS3WALMetaDoc(oneBody, 2)
		if err != nil {
			t.Fatal(err)
		}
		want := expectedS3WALMeta(t, d, 1)
		want.V = 2
		if !reflect.DeepEqual(two, want) {
			t.Fatalf("case %d: to2(to1(d)) =\n%+v\nwant\n%+v", i, two, want)
		}
	}
}
