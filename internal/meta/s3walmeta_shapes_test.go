package meta

import (
	"context"
	"encoding/json"
	"errors"
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
		return head + `"seq":1,"upstreams":{"r":{"provider":"github","owner":"a","name":"b","upstream_host":"h","created_at":1` + fields + `}}}`
	}
	for _, tc := range []struct{ name, body, wantErr string }{
		{"seq at max", `{"v":2,"seq":9223372036854775807,"writer":"w","next_org_id":0,"next_key_id":0}`, "seq or an id counter (0, 0) is at its maximum"},
		{"org counter at max", `{"v":2,"seq":1,"writer":"w","next_org_id":9223372036854775807,"next_key_id":0}`, "is at its maximum"},
		{"key counter at max", `{"v":2,"seq":1,"writer":"w","next_org_id":0,"next_key_id":9223372036854775807}`, "is at its maximum"},
		{"pull claim without holder", up(`,"syncing":true`), `upstream "r": pull claim has no holder`},
		{"legacy pull holder", up(`,"syncing":true,"sync_claim_holder":"!legacy-grace"`), `pull claim held by "!legacy-grace"`},
		{"push claim without holder", up(`,"push_syncing":true`), `upstream "r": push claim has no holder`},
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
	// Controls: the largest values a writer can still bump, and a held
	// claim, decode.
	for _, body := range []string{
		`{"v":2,"seq":9223372036854775806,"writer":"w","next_org_id":9223372036854775806,"next_key_id":9223372036854775806}`,
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
}
