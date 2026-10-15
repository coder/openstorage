package meta

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// TestS3WALMetaDecodeLaneShapes (issues #481, #487): document contents that
// no writer produces, and that a later operation used to assert on, are a
// typed corrupt-document error at decode, never a panic.
func TestS3WALMetaDecodeLaneShapes(t *testing.T) {
	const head = `{"v":2,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"created_at":1,"public_keys":[]}},`
	up := func(fields string) string {
		return head + `"seq":1,"upstreams":{"repo_00000000000a":{"provider":"github","owner":"a","name":"b","upstream_host":"h","created_at":1` + fields + `}}}`
	}
	for _, tc := range []struct{ name, body, wantErr string }{
		{"pull claim without holder", up(`,"syncing":true`), `upstream "repo_00000000000a": pull claim has no holder`},
		{"legacy pull holder", up(`,"syncing":true,"sync_claim_holder":"!legacy-grace"`), `pull claim held by "!legacy-grace"`},
		{"push claim without holder", up(`,"push_syncing":true`), `upstream "repo_00000000000a": push claim has no holder`},
		{"negative push attempt", up(`,"push_attempt":-1`), "negative schedule (push_attempt -1, push_due 0, sync_due 0)"},
		{"negative sync due", up(`,"sync_due":-5`), "negative schedule (push_attempt 0, push_due 0, sync_due -5)"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := decodeS3WALMetaDoc([]byte(tc.body))
			var cd *CorruptDocumentError
			if !errors.As(err, &cd) || !strings.Contains(err.Error(), tc.wantErr) || doc != nil {
				t.Fatalf("decode = %+v, %v; want a *CorruptDocumentError with %q", doc, err, tc.wantErr)
			}
		})
	}
	// Controls: counters at their maximum, and a held claim, decode (only
	// the write that would wrap them is refused).
	for _, body := range []string{
		`{"v":2,"seq":9223372036854775807,"writer":"w","next_org_id":9223372036854775807,"next_key_id":9223372036854775807}`,
		up(`,"syncing":true,"sync_claim_holder":"h1","push_syncing":true,"push_claim_holder":"h2","push_attempt":5,"push_due":7`),
	} {
		if _, err := decodeS3WALMetaDoc([]byte(body)); err != nil {
			t.Fatalf("valid control %s: %v", body, err)
		}
	}
}

// putRawS3WALMeta replaces the stored document with body.
func putRawS3WALMeta(t *testing.T, cas *s3wal.MemCAS, body []byte) {
	t.Helper()
	obj, err := cas.Get(context.Background(), s3walMetaKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := cas.Advance(context.Background(), s3walMetaKey, body, obj.ETag); err != nil {
		t.Fatal(err)
	}
}

// A holderless pull claim used to panic TryBeginSync (assertNoLegacyClaim,
// issue #481). Now the claim is an error, and nothing is written.
func TestS3WALMetaHolderlessClaimNoPanic(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := NewS3WAL(cas)
	t.Cleanup(st.Close)
	repoID := seedS3WALUpstream(t, st)
	doc, _ := storedS3WALMeta(t, cas)
	doc.Upstreams[repoID].Syncing = true // claimed, but no holder
	body, err := doc.encodeAdvance()
	if err != nil {
		t.Fatal(err)
	}
	putRawS3WALMeta(t, cas, body)
	before, _ := cas.Get(ctx, s3walMetaKey)
	var cd *CorruptDocumentError
	if _, _, _, err := st.TryBeginSync(ctx, repoID, "h1", time.Minute); !errors.As(err, &cd) || !strings.Contains(err.Error(), "pull claim has no holder") {
		t.Fatalf("TryBeginSync = %v, want the holderless-claim *CorruptDocumentError", err)
	}
	if after, _ := cas.Get(ctx, s3walMetaKey); after.ETag != before.ETag {
		t.Fatal("TryBeginSync wrote through the corrupt document")
	}
}

// unmarkOnClearSink clears the detaching mark out of band during the
// detach's fence revocation (phase 2), as an operator edit would.
type unmarkOnClearSink struct {
	SyncClaimSink
	t   *testing.T
	cas *s3wal.MemCAS
}

func (u unmarkOnClearSink) ClearSyncClaim(ctx context.Context, repoID, holder string) error {
	if err := u.SyncClaimSink.ClearSyncClaim(ctx, repoID, holder); err != nil {
		return err
	}
	doc, _ := storedS3WALMeta(u.t, u.cas)
	doc.Upstreams[repoID].Detaching = false
	body, err := doc.encodeAdvance()
	if err != nil {
		return err
	}
	putRawS3WALMeta(u.t, u.cas, body)
	return nil
}

// A detaching mark that vanishes mid-detach used to panic the delete phase
// (issue #481). Now the detach is an error, and the row stays.
func TestS3WALMetaVanishedDetachMarkNoPanic(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	refStore := ref.NewS3WAL(cas, nil)
	st := NewS3WAL(cas, WithSyncClaimSink(unmarkOnClearSink{SyncClaimSink: refStore, t: t, cas: cas}))
	t.Cleanup(func() { st.Close(); refStore.Close() })
	repoID := seedS3WALUpstream(t, st)
	if _, ok, _, err := st.TryBeginSync(ctx, repoID, "h1", time.Minute); err != nil || !ok {
		t.Fatalf("TryBeginSync = %v, %v", ok, err)
	}
	var cd *CorruptDocumentError
	if err := st.DeleteRepoUpstream(ctx, repoID); !errors.As(err, &cd) || !strings.Contains(err.Error(), "detaching mark vanished") {
		t.Fatalf("DeleteRepoUpstream = %v, want the vanished-mark *CorruptDocumentError", err)
	}
	if doc, _ := storedS3WALMeta(t, cas); doc.Upstreams[repoID] == nil {
		t.Fatal("the detach deleted the row after its mark vanished")
	}
}

// Our writer nonce at another seq after an ambiguous write used to panic
// mutate (issue #481). Now the write is an error.
func TestS3WALMetaForeignSeqNonceNoPanic(t *testing.T) {
	ctx := context.Background()
	cas, a, _ := newS3WALMetaPair(t)
	if _, err := a.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	armAmbiguousPut(t, cas, s3wal.FaultAmbiguousApplied, func() {
		// The write landed. Keep its nonce, move its seq.
		landed, _ := storedS3WALMeta(t, cas)
		landed.Seq += 5
		body, err := json.Marshal(landed)
		if err != nil {
			t.Error(err)
			return
		}
		putRawS3WALMeta(t, cas, body)
	})
	var cd *CorruptDocumentError
	if _, err := a.CreateRepo(ctx, "org", "https://example.com/nonce.git", "main"); !errors.As(err, &cd) || !strings.Contains(err.Error(), "carries this process's writer nonce") {
		t.Fatalf("CreateRepo = %v, want the foreign-seq-nonce *CorruptDocumentError", err)
	}
	// The recovery read cached the edited body. It stays quarantined: no
	// later read or write goes through it (Codex P2 on #509).
	edited, _ := cas.Get(ctx, s3walMetaKey)
	if _, err := a.CreateRepo(ctx, "org", "https://example.com/again.git", "main"); !errors.As(err, &cd) {
		t.Fatalf("second CreateRepo = %v, want the quarantined *CorruptDocumentError", err)
	}
	if _, err := a.ListRepos(ctx, "org", ListParams{Limit: 10}); !errors.As(err, &cd) {
		t.Fatalf("ListRepos = %v, want the quarantined *CorruptDocumentError", err)
	}
	if got, _ := cas.Get(ctx, s3walMetaKey); got.ETag != edited.ETag {
		t.Fatal("a later write went through the quarantined document")
	}
}

// Seq and id counters at MaxInt64 (an out-of-band edit, issue #487): the
// document stays readable, and only the write or mint that would wrap is
// refused, with nothing written. A write at MaxInt64-1 commits a readable
// MaxInt64 document (Codex P2 on #509).
func TestS3WALMetaCountersAtMaximum(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name  string
		field string
		value int64
		write func(st *S3WAL) error
		want  string
	}{
		{"seq", "seq", math.MaxInt64, func(st *S3WAL) error {
			_, err := st.CreateRepo(ctx, "org", "https://example.com/max.git", "main")
			return err
		}, "seq 9223372036854775807 is at its maximum"},
		{"org counter", "next_org_id", math.MaxInt64, func(st *S3WAL) error {
			_, err := st.UpsertOrgWithKeys(ctx, "new-org", []string{"pem"})
			return err
		}, "next_org_id 9223372036854775807 is at its maximum"},
		{"key counter", "next_key_id", math.MaxInt64, func(st *S3WAL) error {
			_, err := st.AddSigningKey(ctx, "org", "ssh-ed25519 AAAAtest key")
			return err
		}, "next_key_id 9223372036854775807 is at its maximum"},
		{"seq one below", "seq", math.MaxInt64 - 1, func(st *S3WAL) error {
			_, err := st.CreateRepo(ctx, "org", "https://example.com/last.git", "main")
			return err
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cas := s3wal.NewMemCAS()
			seed := NewS3WAL(cas)
			t.Cleanup(seed.Close)
			if _, err := seed.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
				t.Fatal(err)
			}
			doc, _ := storedS3WALMeta(t, cas)
			switch tc.field {
			case "seq":
				doc.Seq = tc.value
			case "next_org_id":
				doc.NextOrgID = tc.value
			case "next_key_id":
				doc.NextKeyID = tc.value
			}
			body, err := json.Marshal(doc)
			if err != nil {
				t.Fatal(err)
			}
			putRawS3WALMeta(t, cas, body)
			before, _ := cas.Get(ctx, s3walMetaKey)
			st := NewS3WAL(cas)
			t.Cleanup(st.Close)
			if _, err := st.ListRepos(ctx, "org", ListParams{Limit: 10}); err != nil {
				t.Fatalf("read at %s=%d: %v", tc.field, tc.value, err)
			}
			err = tc.write(st)
			if tc.want == "" {
				if err != nil {
					t.Fatal(err)
				}
				if _, err := NewS3WAL(cas).ListRepos(ctx, "org", ListParams{Limit: 10}); err != nil {
					t.Fatalf("the committed MaxInt64 document does not read back: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("write at %s=%d = %v, want %q", tc.field, tc.value, err, tc.want)
			}
			if got, _ := cas.Get(ctx, s3walMetaKey); got.ETag != before.ETag {
				t.Fatal("the refused write changed the document")
			}
		})
	}
}
