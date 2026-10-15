package meta

import (
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/ref/s3wal"
)

// s3walRowSeed is one meta.json content from the issue #492 table.
type s3walRowSeed struct{ name, body, wantErr string }

// s3walCorruptRowSeeds holds every decodable-but-corrupt content in the
// issue #492 table. Each one used to reach an assertion, a nil dereference
// or a panicking helper in serve's background loops or the hook. Decode
// rejects each with *CorruptDocumentError. They also seed
// FuzzS3WALMetaDocument.
func s3walCorruptRowSeeds() []s3walRowSeed {
	const (
		head = `{"v":2,"seq":3,"writer":"0123456789abcdef0123456789abcdef","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"created_at":1,"public_keys":["pem"]}}`
		repo = `"repo_0123456789ab":{"org_id":1,"url":"https://example.com/r.git","default_branch":"main","created_at":1}`
		up   = `"repo_0123456789ab":{"provider":"github","owner":"a","name":"b","upstream_host":"github.com","created_at":1}`
	)
	return []s3walRowSeed{
		{"empty org name", `{"v":2,"seq":1,"writer":"0","next_org_id":1,"orgs":{"":{"id":1}}}`, "an org has an empty name"},
		{"empty webhook id", `{"v":2,"seq":1,"writer":"0","webhooks":{"":{"url":"0","secret":"0"}}}`, "a webhook has an empty id"},
		{"empty credential id", `{"v":2,"seq":1,"writer":"0","credentials":{"":{"repo_id":"r","password":"p"}}}`, "a credential has an empty id"},
		{"null repo row", head + `,"repos":{"repo_0123456789ab":null}}`, `repo "repo_0123456789ab" has no row`},
		{"null credential row", head + `,"repos":{` + repo + `},"credentials":{"c":null}}`, `credential "c" has no row`},
		{"null upstream row", head + `,"repos":{` + repo + `},"upstreams":{"repo_0123456789ab":null}}`, `upstream "repo_0123456789ab" has no row`},
		{"null webhook row", head + `,"webhooks":{"w":null}}`, `webhook "w" has no row`},
		{"empty webhook url", head + `,"webhooks":{"w":{"org_id":1,"url":"","secret":"s","created_at":1}}}`, `webhook "w" has an empty url or secret`},
		{"empty webhook secret", head + `,"webhooks":{"w":{"org_id":1,"url":"https://hooks.example.com","secret":"","created_at":1}}}`, `webhook "w" has an empty url or secret`},
		{"empty repo url", head + `,"repos":{"repo_0123456789ab":{"org_id":1,"url":"","default_branch":"main","created_at":1}}}`, "has an empty url or default branch"},
		{"empty default branch", head + `,"repos":{"repo_0123456789ab":{"org_id":1,"url":"u","default_branch":"","created_at":1}}}`, "has an empty url or default branch"},
		{"empty upstream owner", head + `,"repos":{` + repo + `},"upstreams":{"repo_0123456789ab":{"provider":"github","owner":"","name":"b","upstream_host":"h","created_at":1}}}`, "has an empty owner, name or host"},
		{"empty upstream name", head + `,"repos":{` + repo + `},"upstreams":{"repo_0123456789ab":{"provider":"github","owner":"a","name":"","upstream_host":"h","created_at":1}}}`, "has an empty owner, name or host"},
		{"empty upstream host", head + `,"repos":{` + repo + `},"upstreams":{"repo_0123456789ab":{"provider":"github","owner":"a","name":"b","upstream_host":"","created_at":1}}}`, "has an empty owner, name or host"},
		{"empty repo id", head + `,"repos":{"":{"org_id":1,"url":"u","default_branch":"main","created_at":1}}}`, `repo id "" is not a minted repo id`},
		{"plus repo id", head + `,"repos":{"+x":{"org_id":1,"url":"u","default_branch":"main","created_at":1}}}`, `repo id "+x" is not a minted repo id`},
		{"ephemeral repo id", head + `,"repos":{"+ephemeral":{"org_id":1,"url":"u","default_branch":"main","created_at":1}}}`, `repo id "+ephemeral" is not a minted repo id`},
		{"import repo id", head + `,"repos":{"+import":{"org_id":1,"url":"u","default_branch":"main","created_at":1}}}`, `repo id "+import" is not a minted repo id`},
		{"upstream key not a repo id", head + `,"repos":{` + repo + `},"upstreams":{"+x":{"provider":"github","owner":"a","name":"b","upstream_host":"h","created_at":1}}}`, `upstream key "+x" is not a minted repo id`},
		{"valid control", head + `,"repos":{` + repo + `},"upstreams":{` + up + `},"webhooks":{"w":{"org_id":1,"url":"https://hooks.example.com","secret":"s","created_at":1}},"credentials":{"c":{"repo_id":"repo_0123456789ab","username":"","password":"p","created_at":1}}}`, ""},
	}
}

// TestS3WALMetaDecodeRows (issue #492): every content in the table is a
// typed corrupt-document error at decode, and the valid control decodes.
func TestS3WALMetaDecodeRows(t *testing.T) {
	for _, tc := range s3walCorruptRowSeeds() {
		t.Run(tc.name, func(t *testing.T) {
			doc, err := decodeS3WALMetaDoc([]byte(tc.body))
			if tc.wantErr == "" {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			var cd *CorruptDocumentError
			if !errors.As(err, &cd) || !strings.Contains(err.Error(), tc.wantErr) || doc != nil {
				t.Fatalf("decode = %+v, %v; want a *CorruptDocumentError with %q", doc, err, tc.wantErr)
			}
		})
	}
}

// writerS3WALMetaDoc returns a document that real writers produced: an
// org, a linked repo with a credential, a webhook, and a soft-deleted repo
// past its purge window.
func writerS3WALMetaDoc(t testing.TB) []byte {
	t.Helper()
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := NewS3WAL(cas)
	defer st.Close()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	_, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"})
	must(err)
	cred := &GitCredential{Password: "pw"}
	linked, err := st.CreateLinkedRepo(ctx, "org", "https://example.com/linked.git", "main",
		RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com", ForwardOnPush: true}, cred)
	must(err)
	must(st.RequestSync(ctx, linked.RepoID))
	_, err = st.CreateWebhook(ctx, "org", "https://hooks.example.com/x", "s3cret")
	must(err)
	_, err = st.CreateRepo(ctx, "org", "https://example.com/gone.git", "main")
	must(err)
	_, err = st.SoftDeleteRepo(ctx, "org", "https://example.com/gone.git")
	must(err)
	obj, err := cas.Get(ctx, s3walMetaKey)
	must(err)
	return obj.Body
}

// sweepAndPurgeS3WALMeta runs, over a store holding body, the reads and
// writes of serve's background loops: the org-key refresher's read, the
// sync sweep (list, claim, finish), webhook and credential lookups, and one
// purge pass. Errors are fine; a panic fails the caller. It returns the
// store so a caller can inspect what the pass wrote.
func sweepAndPurgeS3WALMeta(t testing.TB, body []byte) *s3wal.MemCAS {
	t.Helper()
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	if _, err := cas.Create(ctx, s3walMetaKey, body); err != nil {
		t.Fatal(err)
	}
	st := NewS3WAL(cas)
	defer st.Close()
	doc, err := st.viewDoc(ctx)
	if err != nil {
		return cas
	}
	orgs := slices.Sorted(maps.Keys(doc.Orgs))
	repos := slices.Sorted(maps.Keys(doc.Repos))
	hooks := slices.Sorted(maps.Keys(doc.Webhooks))
	sched := s3walLocalSchedule{st}
	for _, org := range orgs {
		_, _ = st.ListWebhooks(ctx, org)
		_, _ = st.ListRepos(ctx, org, ListParams{Limit: 100})
		_, _ = st.PublicKeysForOrg(ctx, org)
		work, _ := sched.ListLocalWork(ctx, org, "", 100)
		for _, w := range work {
			if w.Pull {
				if c, err := sched.ClaimDueSync(ctx, w.RepoID, "fuzz-pull", time.Minute); err == nil && c.Claimed {
					_, _ = sched.FinishSyncOutcome(ctx, w.RepoID, "fuzz-pull", SyncOutcome{Kind: OutcomeDone, Success: true})
				}
			}
			if w.Push {
				if c, err := sched.ClaimDuePush(ctx, w.RepoID, "fuzz-push", time.Minute); err == nil && c.Claimed {
					_, _ = sched.FinishPushOutcome(ctx, w.RepoID, "fuzz-push", PushOutcome{Kind: OutcomeRetry, Delay: time.Second})
				}
			}
		}
	}
	for _, id := range hooks {
		_, _ = st.GetWebhook(ctx, id)
	}
	for _, id := range repos {
		_, _ = st.GitCredentialForRepo(ctx, id)
		_, _, _ = st.RepoPurgeState(ctx, id)
	}
	cands, _ := st.PurgeableRepos(ctx, time.Unix(1<<32, 0), PurgeCandidate{}, 100) // year 2106: every deletion, without overflowing UnixNano
	for _, c := range cands {
		_ = st.PurgeRepoMeta(ctx, c.RepoID)
	}
	return cas
}

// TestS3WALMetaSweepAndPurgeWriterDoc: the pass itself works on a document
// that real writers produced (it claims, finishes and purges).
func TestS3WALMetaSweepAndPurgeWriterDoc(t *testing.T) {
	cas := sweepAndPurgeS3WALMeta(t, writerS3WALMetaDoc(t))
	doc, _ := storedS3WALMeta(t, cas)
	var ran, purged int
	for id, r := range doc.Repos {
		if r.PurgedAt != 0 {
			purged++
		}
		if u := doc.Upstreams[id]; u != nil && u.RunCount == 1 {
			ran++
		}
	}
	if ran != 1 || purged != 1 {
		t.Fatalf("the pass ran %d pull lanes and purged %d repos, want 1 and 1: %+v", ran, purged, doc)
	}
}

// FuzzS3WALMetaDocument (issue #492): no meta.json content panics decode,
// and no content that decodes panics the background-loop pass above. The
// seeds are every row of the issue #492 table, the earlier corrupt
// documents (#466, #478, #481, #487), and a writer-produced document.
func FuzzS3WALMetaDocument(f *testing.F) {
	f.Add(writerS3WALMetaDoc(f))
	for _, s := range s3walCorruptRowSeeds() {
		f.Add([]byte(s.body))
	}
	for _, s := range []string{
		`{`,
		`{"v":1,"seq":1,"writer":"w","next_org_id":0,"next_key_id":0}`,
		`{"v":2,"seq":1,"writer":"w","next_org_id":-1,"next_key_id":0}`,
		`{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":null}}`,
		`{"v":2,"seq":9223372036854775807,"writer":"w","next_org_id":9223372036854775807,"next_key_id":9223372036854775807}`,
		`{"v":2,"seq":1,"writer":"w","next_org_id":0,"next_key_id":0,"upstreams":{"repo_0123456789ab":{"owner":"a","name":"b","upstream_host":"h","syncing":true,"push_attempt":9}}}`,
	} {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		if _, err := decodeS3WALMetaDoc(body); err != nil {
			return
		}
		sweepAndPurgeS3WALMeta(t, body)
	})
}
