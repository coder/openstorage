package sync_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/blob"
	"github.com/coder/openstorage/internal/events"
	"github.com/coder/openstorage/internal/meta"
	"github.com/coder/openstorage/internal/ref"
	"github.com/coder/openstorage/internal/ref/s3wal"
	"github.com/coder/openstorage/internal/secrets"
	upsync "github.com/coder/openstorage/internal/sync"
)

// consumerFuzzCap bounds the rows one input drives (orgs, repos, local
// work, webhook rows, purge candidates), so an input with many rows
// costs the fuzzer no more than a few runs.
const consumerFuzzCap = 4

// newConsumerStore opens the S3-WAL store over cas, with a fixed secrets
// codec when encrypted (serve with OPENSTORAGE_SECRETS_KEY), so encrypted
// inputs replay.
func newConsumerStore(t testing.TB, cas *s3wal.MemCAS, encrypted bool) *meta.S3WAL {
	if !encrypted {
		return meta.NewS3WAL(cas)
	}
	codec, err := secrets.ParseKeys(base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{7}, secrets.KeySize)))
	if err != nil {
		t.Fatal(err)
	}
	return meta.NewS3WAL(cas, meta.WithSecretsCodec(codec))
}

// capWebhookRows keeps the first consumerFuzzCap webhook rows (by id) of
// a JSON object document and drops the rest, before the store reads it:
// with a codec, ListWebhooks decrypts every row and rewrites each
// plaintext secret, so a large table would multiply every delivery
// fan-out. FuzzS3WALMetaDocument covers large tables in internal/meta.
// Other bytes pass through unchanged.
func capWebhookRows(body []byte) []byte {
	var top map[string]json.RawMessage
	var hooks map[string]json.RawMessage
	if json.Unmarshal(body, &top) != nil || json.Unmarshal(top["webhooks"], &hooks) != nil || len(hooks) <= consumerFuzzCap {
		return body
	}
	for _, id := range slices.Sorted(maps.Keys(hooks))[consumerFuzzCap:] {
		delete(hooks, id)
	}
	trimmed, err := json.Marshal(hooks)
	if err != nil {
		return body
	}
	top["webhooks"] = trimmed
	out, err := json.Marshal(top)
	if err != nil {
		return body
	}
	return out
}

// okTransport answers every webhook POST with 200 and never dials.
type okTransport struct{}

func (okTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
}

// deliverSink is the sync worker's event sink: like serve's, it fans an
// event out to the org's webhooks, and here it runs each attempt at once.
type deliverSink struct {
	hooks events.MetaWebhookStore
	d     *events.Deliverer
	n     *consumerCounts
}

func (s deliverSink) Emit(ctx context.Context, org, eventType string, payload []byte) {
	// Count each run family on its own: one lane must not hide the other.
	emitted, delivered := &s.n.pullEvents, &s.n.pullDeliveries
	if strings.HasPrefix(eventType, "repo.push_upstream.") {
		emitted, delivered = &s.n.pushRunEvents, &s.n.pushRunDeliveries
	}
	*emitted++
	hooks, _ := s.hooks.ListWebhooks(ctx, org)
	for _, h := range hooks {
		if s.d.Attempt(ctx, events.Delivery{WebhookID: h.ID, Event: eventType, Payload: payload}) == nil {
			*delivered++
		}
	}
}

// consumerCounts records what one pass reached, so a test can prove that
// the pass drives each consumer on a real document.
type consumerCounts struct {
	resolvedByID, resolvedByURL, refUpdates, pushDeliveries      int
	queuedDeliveries, purgeGroups                                int
	pullEvents, pullDeliveries, pushRunEvents, pushRunDeliveries int
}

// consumerSeed is a writer-produced document with an org, a linked repo
// that has pull and push work, a credential, a webhook, and a deleted repo.
// With encrypted, its secrets are encrypted.
func consumerSeed(f testing.TB, encrypted bool) []byte {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := newConsumerStore(f, cas, encrypted)
	defer st.Close()
	must := func(err error) {
		if err != nil {
			f.Fatal(err)
		}
	}
	_, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"})
	must(err)
	linked, err := st.CreateLinkedRepo(ctx, "org", "https://example.com/linked.git", "main",
		meta.RepoUpstream{Provider: "github", Owner: "acme", Name: "demo", UpstreamHost: "github.com", ForwardOnPush: true}, &meta.GitCredential{Password: "pw"})
	must(err)
	must(st.RequestSync(ctx, linked.RepoID))
	must(st.RequestPush(ctx, linked.RepoID, false))
	_, err = st.CreateWebhook(ctx, "org", "https://hooks.example.com/x", "s3cret")
	must(err)
	_, err = st.CreateRepo(ctx, "org", "https://example.com/gone.git", "main")
	must(err)
	_, err = st.SoftDeleteRepo(ctx, "org", "https://example.com/gone.git")
	must(err)
	obj, err := cas.Get(ctx, "_wal/meta.json")
	must(err)
	return obj.Body
}

// FuzzS3WALMetaConsumers (issue #519): no meta.json content that the
// S3-WAL store accepts panics a consumer outside internal/meta. For each
// org and repo of an accepted document, it drives what serve and the
// hook do with those rows: the git surface's repo resolve and quota group,
// the hook's ref update with its push event and webhook deliveries, the
// sync worker's pull and push runs from the local work list (the upstream
// clone URL, the claim, the run events), and the purge pass's quota group.
// Git transfers fail at once: the worker allows no git protocol. Each
// input runs without or with a secrets codec (encrypted).
func FuzzS3WALMetaConsumers(f *testing.F) {
	f.Add(consumerSeed(f, false), false)
	f.Add(consumerSeed(f, true), true)
	for _, s := range []string{
		`{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"public_keys":["k"]}},"repos":{"repo_0123456789ab":{"org_id":1,"url":"u","default_branch":"main","created_at":1}},"upstreams":{"repo_0123456789ab":{"provider":"github","owner":"a","name":"b","upstream_host":"h","sync_pending":true}}}`,
		`{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"public_keys":["k"]}},"webhooks":{"wh_1":{"org_id":2,"url":"https://h/x","secret":"s","created_at":1}},"credentials":{"cred_1":{"repo_id":"repo_ffffffffffff","password":"p","created_at":1}}}`,
	} {
		f.Add([]byte(s), false)
		f.Add([]byte(s), true)
	}
	f.Fuzz(func(t *testing.T, body []byte, encrypted bool) { driveMetaConsumers(t, body, encrypted) })
}

// TestS3WALMetaConsumersWriterDoc: on a writer-produced document, with
// and without a codec, the pass reaches every consumer (resolve by id and
// by url, ref update, push-event delivery, queued delivery by id, the pull run's and the push run's events
// and their deliveries, purge group), so the fuzz target tests something.
func TestS3WALMetaConsumersWriterDoc(t *testing.T) {
	for _, encrypted := range []bool{false, true} {
		n := driveMetaConsumers(t, consumerSeed(t, encrypted), encrypted)
		if n.resolvedByID == 0 || n.resolvedByURL == 0 || n.refUpdates == 0 || n.pushDeliveries == 0 || n.queuedDeliveries == 0 || n.purgeGroups == 0 ||
			n.pullEvents < 2 || n.pullDeliveries < 2 || n.pushRunEvents < 2 || n.pushRunDeliveries < 2 {
			t.Fatalf("encrypted=%v: the pass missed a consumer: %+v", encrypted, n)
		}
	}
}

// TestCapWebhookRows: a writer document with more webhooks than the cap
// keeps exactly consumerFuzzCap of them, and the store still accepts it.
func TestCapWebhookRows(t *testing.T) {
	ctx := context.Background()
	cas := s3wal.NewMemCAS()
	st := newConsumerStore(t, cas, true)
	defer st.Close()
	if _, err := st.UpsertOrgWithKeys(ctx, "org", []string{"pem"}); err != nil {
		t.Fatal(err)
	}
	for range consumerFuzzCap + 2 {
		if _, err := st.CreateWebhook(ctx, "org", "https://hooks.example.com/x", "s3cret"); err != nil {
			t.Fatal(err)
		}
	}
	obj, err := cas.Get(ctx, "_wal/meta.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Webhooks map[string]json.RawMessage `json:"webhooks"`
	}
	capped := capWebhookRows(obj.Body)
	if err := json.Unmarshal(capped, &doc); err != nil || len(doc.Webhooks) != consumerFuzzCap {
		t.Fatalf("capped document has %d webhooks (%v), want %d", len(doc.Webhooks), err, consumerFuzzCap)
	}
	other := s3wal.NewMemCAS()
	if _, err := other.Create(ctx, "_wal/meta.json", capped); err != nil {
		t.Fatal(err)
	}
	if err := meta.CheckFormat(ctx, newConsumerStore(t, other, true)); err != nil {
		t.Fatalf("the store refuses the capped document: %v", err)
	}
}

// TestS3WALMetaConsumersResolveCountsThePath: a URL that equals another
// repo's repo_id resolves by id, so it must not count as a URL resolve.
func TestS3WALMetaConsumersResolveCountsThePath(t *testing.T) {
	body := `{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"public_keys":["k"]}},"repos":{` +
		`"repo_aaaaaaaaaaaa":{"org_id":1,"url":"repo_bbbbbbbbbbbb","default_branch":"main","created_at":1},` +
		`"repo_bbbbbbbbbbbb":{"org_id":1,"url":"repo_aaaaaaaaaaaa","default_branch":"main","created_at":1}}}`
	if n := driveMetaConsumers(t, []byte(body), false); n.resolvedByID != 4 || n.resolvedByURL != 0 {
		t.Fatalf("crossed urls: %+v, want 4 resolves by id and none by url", n)
	}
}

// TestS3WALMetaConsumersDanglingWebhook: a webhook whose org_id names no
// org is never listed, but a queued delivery still reloads and posts it.
func TestS3WALMetaConsumersDanglingWebhook(t *testing.T) {
	body := `{"v":2,"seq":1,"writer":"w","next_org_id":1,"next_key_id":0,"orgs":{"o":{"id":1,"public_keys":["k"]}},"webhooks":{"wh_1":{"org_id":2,"url":"https://h/x","secret":"s","created_at":1}}}`
	if n := driveMetaConsumers(t, []byte(body), false); n.queuedDeliveries != 1 || n.pushDeliveries != 0 {
		t.Fatalf("dangling webhook: %+v, want one queued delivery and no push delivery", n)
	}
}

// driveMetaConsumers runs the pass of FuzzS3WALMetaConsumers over body.
// Errors are fine; a panic fails the caller.
func driveMetaConsumers(t testing.TB, body []byte, encrypted bool) (n consumerCounts) {
	ctx := context.Background()
	body = capWebhookRows(body)
	cas := s3wal.NewMemCAS()
	if _, err := cas.Create(ctx, "_wal/meta.json", body); err != nil {
		t.Fatal(err)
	}
	st := newConsumerStore(t, cas, encrypted)
	defer st.Close()
	if meta.CheckFormat(ctx, st) != nil {
		return n
	}
	var doc struct {
		Orgs  map[string]json.RawMessage `json:"orgs"`
		Repos map[string]struct {
			URL string `json:"url"`
		} `json:"repos"`
		Webhooks map[string]json.RawMessage `json:"webhooks"`
	}
	if err := json.Unmarshal(body, &doc); err != nil {
		t.Fatalf("the store accepted a document that encoding/json rejects: %v", err)
	}
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	hooks := events.MetaWebhookStore{Store: st}
	deliverer := &events.Deliverer{Webhooks: hooks, Client: &http.Client{Transport: okTransport{}}, MaxAttempts: 1, Logger: quiet}
	refs := ref.NewS3WAL(s3wal.NewMemCAS(), nil)
	defer refs.Close()
	creds, ups := upsync.MetaStores(st)
	lanes, ok := upsync.MetaLocalLanes(st)
	if !ok {
		t.Fatal("the S3-WAL store has no local lanes")
	}
	worker := &upsync.Worker{
		Upstreams: ups, Credentials: creds, Local: lanes, Logger: quiet, Timeout: 5 * time.Second,
		Repos: func(ctx context.Context, org, repoID string) (upsync.RepoInfo, error) { // serve's adapter
			r, err := st.GetRepoByID(ctx, org, repoID)
			if err != nil {
				return upsync.RepoInfo{}, upsync.ErrNotFound
			}
			return upsync.RepoInfo{RepoID: r.RepoID, URL: r.URL}, nil
		},
		Events:        deliverSink{hooks: hooks, d: deliverer, n: &n},
		Loopback:      upsync.Loopback{BaseURL: "file:///nonexistent", Token: func(context.Context, string, upsync.RepoInfo, time.Duration, string) (string, error) { return "", nil }},
		AllowProtocol: "x-none", // no transfer leaves the process
	}
	orgs := slices.Sorted(maps.Keys(doc.Orgs))
	for _, org := range orgs[:min(len(orgs), consumerFuzzCap)] {
		if org == "" {
			continue // no request carries an empty org (JWT iss)
		}
		delivering := events.NewRefStore(refs, events.RefStoreOptions{
			Org: org, Repos: st, Webhooks: hooks, Logger: quiet,
			EnqueueDelivery: func(ctx context.Context, d events.Delivery) error {
				err := deliverer.Attempt(ctx, d)
				if err == nil {
					n.pushDeliveries++
				}
				return err
			},
		})
		repoIDs := slices.Sorted(maps.Keys(doc.Repos))
		for _, id := range repoIDs[:min(len(repoIDs), consumerFuzzCap)] {
			// The git surface resolves the URL path element, then
			// keys refs and quota by the resolved repo_id.
			for _, key := range []string{id, doc.Repos[id].URL} {
				repo, err := meta.ResolveRepoKey(ctx, st, org, key)
				if err != nil {
					continue
				}
				// ResolveRepoKey tries the key as a repo_id first, so count the
				// lookup that answered, not which key it was.
				if repo.RepoID == key {
					n.resolvedByID++
				} else {
					n.resolvedByURL++
				}
				_ = blob.QuotaRepoGroup(repo.RepoID)
				if won, _ := delivering.CASRef(ctx, repo.RepoID, "refs/heads/main", ref.ZeroSHA, strings.Repeat("a", 40)); won {
					n.refUpdates++
				}
			}
		}
		work, _ := lanes.ListLocalWork(ctx, org, "", consumerFuzzCap)
		for _, w := range work {
			if w.Pull {
				_ = worker.Run(ctx, upsync.Job{Org: org, RepoID: w.RepoID})
			}
			if w.Push {
				_ = worker.RunPush(ctx, upsync.Job{Org: org, RepoID: w.RepoID})
			}
		}
	}
	// A queued delivery reloads its webhook by id, whatever its org_id
	// (#527): drive one for every (capped) webhook row.
	for _, id := range slices.Sorted(maps.Keys(doc.Webhooks)) {
		if deliverer.Attempt(ctx, events.Delivery{WebhookID: id, Event: events.EventPush, Payload: []byte(`{}`)}) == nil {
			n.queuedDeliveries++
		}
	}
	cands, _ := st.PurgeableRepos(ctx, time.Unix(1<<32, 0), meta.PurgeCandidate{}, consumerFuzzCap)
	for _, c := range cands {
		_ = blob.QuotaRepoGroup(c.RepoID) // the purge job's ledger group
		n.purgeGroups++
	}
	return n
}
