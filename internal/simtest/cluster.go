package simtest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coder/code-storage/internal/blob"
	"github.com/coder/code-storage/internal/events"
	"github.com/coder/code-storage/internal/meta"
	"github.com/coder/code-storage/internal/ref"
)

// simNode is one simulated stateless server node: its own SQLite and
// fileblob handles onto the SHARED database file and blob directory (like
// separate processes on one host), plus a private cache dir. Handles are
// wrapped in the fault-injecting decorators; CAS writes additionally flow
// through the production events.RefStore → ref.IdempotentCAS chain, exactly
// like the serve wiring.
type simNode struct {
	id       int
	cacheDir string

	raw      ref.Store      // lifecycle handle (Close on crash)
	rawBlob  blob.BlobStore // lifecycle handle
	simRef   *SimRefStore
	simBlob  *SimBlobStore
	casStore ref.RefStore // events.RefStore(IdempotentCAS(simRef)) — the write seam
	// metaStore is the node's own handle onto the shared meta DB (the
	// upstream-sync claim/steal/finish surface, issue #88). It carries the
	// simulation's logical clock (meta.WithNowFunc), so sync-claim lease
	// expiry is a pure function of the seed, never wall time.
	metaStore *meta.SQLite

	// believedTips is the node's client-side view: repoKey -> ref name ->
	// sha (ZeroSHA when believed absent). Stale beliefs across nodes are
	// what produce genuine CAS conflicts under the serialized scheduler.
	believedTips map[string]map[string]string
}

// pushTransition is one committed or delivered ref transition, keyed for
// webhook soundness/completeness checks.
type pushTransition struct {
	Repo, Ref, Before, After string
}

// deliveryRecorder captures webhook deliveries synchronously (it replaces
// the jobs queue, so emission order is deterministic).
type deliveryRecorder struct {
	mu   sync.Mutex
	sent []pushTransition
}

func (r *deliveryRecorder) enqueue(_ context.Context, d events.Delivery) error {
	var p events.PushPayload
	if err := json.Unmarshal(d.Payload, &p); err != nil {
		return fmt.Errorf("recorder: bad push payload: %w", err)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.sent = append(r.sent, pushTransition{
		Repo: p.Repository.ID, Ref: p.Ref, Before: p.Before, After: p.After,
	})
	return nil
}

func (r *deliveryRecorder) all() []pushTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]pushTransition, len(r.sent))
	copy(out, r.sent)
	return out
}

// simRepoSource resolves every RepoID to a synthetic live repo row (the
// simulation bypasses the meta store; payload construction only needs
// RepoID + URL).
type simRepoSource struct{}

func (simRepoSource) GetRepoByID(_ context.Context, _, repoID string) (meta.Repo, error) {
	assertf(repoID != "", "simRepoSource: empty repoID")
	return meta.Repo{RepoID: repoID, URL: "https://dst.invalid/" + repoID}, nil
}

// simWebhooks registers exactly one webhook so every won CAS emits one
// delivery.
type simWebhooks struct{}

func (simWebhooks) ListWebhooks(context.Context, string) ([]events.Webhook, error) {
	return []events.Webhook{{ID: "wh_dst", URL: "https://dst.invalid/hook", Secret: "s"}}, nil
}

const simOrg = "dst-org"

// cluster owns the shared backing state plus the oracle's clean handles.
type cluster struct {
	dir      string // run root: db, blobs, caches, scratch repos
	dbURL    string
	blobURL  string
	inj      *Injector
	rec      *deliveryRecorder
	now      func() time.Time
	metaURL  string
	nodes    []*simNode
	oracle   ref.Store      // clean, never fault-injected: ground truth
	oracleBl blob.BlobStore // clean blob handle for invariant (c)
	// oracleMeta is the clean meta handle the sync-claim oracle reads
	// repo_upstreams ground truth through (issue #88).
	oracleMeta *meta.SQLite
}

// newCluster boots the shared SQLite file + fileblob dir, the oracle
// handles, and n nodes.
func newCluster(dir string, n int, inj *Injector, now func() time.Time) (*cluster, error) {
	assertf(n >= 1, "newCluster: need at least one node, got %d", n)
	ctx := context.Background()
	c := &cluster{
		dir:     dir,
		dbURL:   "sqlite://" + filepath.Join(dir, "refs.sqlite"),
		blobURL: "file://" + filepath.Join(dir, "blobs"),
		metaURL: "sqlite://" + filepath.Join(dir, "meta.sqlite"),
		inj:     inj,
		rec:     &deliveryRecorder{},
		now:     now,
	}
	oracle, err := ref.OpenSQLite(ctx, c.dbURL)
	if err != nil {
		return nil, fmt.Errorf("open oracle ref store: %w", err)
	}
	if err := oracle.InitSchema(ctx); err != nil {
		oracle.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}
	c.oracle = oracle
	oracleBl, err := blob.OpenFile(c.blobURL)
	if err != nil {
		c.close()
		return nil, fmt.Errorf("open oracle blob store: %w", err)
	}
	c.oracleBl = oracleBl
	oracleMeta, err := meta.OpenSQLite(ctx, c.metaURL, meta.WithNowFunc(c.now))
	if err != nil {
		c.close()
		return nil, fmt.Errorf("open oracle meta store: %w", err)
	}
	c.oracleMeta = oracleMeta
	if err := oracleMeta.InitSchema(ctx); err != nil {
		c.close()
		return nil, fmt.Errorf("init meta schema: %w", err)
	}
	for i := 0; i < n; i++ {
		node := &simNode{
			id:           i,
			cacheDir:     filepath.Join(dir, fmt.Sprintf("cache-node%d", i)),
			believedTips: make(map[string]map[string]string),
		}
		if err := c.openNodeHandles(node); err != nil {
			c.close()
			return nil, err
		}
		c.nodes = append(c.nodes, node)
	}
	return c, nil
}

// openNodeHandles (re)opens a node's store handles and decorator chain,
// used at boot and after a simulated crash.
func (c *cluster) openNodeHandles(n *simNode) error {
	ctx := context.Background()
	raw, err := ref.OpenSQLite(ctx, c.dbURL)
	if err != nil {
		return fmt.Errorf("node %d: open ref store: %w", n.id, err)
	}
	rawBlob, err := blob.OpenFile(c.blobURL)
	if err != nil {
		raw.Close()
		return fmt.Errorf("node %d: open blob store: %w", n.id, err)
	}
	metaStore, err := meta.OpenSQLite(ctx, c.metaURL, meta.WithNowFunc(c.now))
	if err != nil {
		raw.Close()
		_ = rawBlob.Close()
		return fmt.Errorf("node %d: open meta store: %w", n.id, err)
	}
	n.raw, n.rawBlob, n.metaStore = raw, rawBlob, metaStore
	n.simRef = NewSimRefStore(raw, c.inj)
	n.simBlob = NewSimBlobStore(rawBlob, c.inj)
	// The production write chain: events emission over idempotent CAS
	// recovery over the (fault-injected) store — same shape as serve.
	n.casStore = events.NewRefStore(n.simRef, events.RefStoreOptions{
		Org:             simOrg,
		Repos:           simRepoSource{},
		Webhooks:        simWebhooks{},
		EnqueueDelivery: c.rec.enqueue,
		Now:             c.now,
		Logger:          slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	return nil
}

// crashNode drops a node's handles (process death). cold additionally wipes
// its cache dir (fresh machine). Beliefs survive only warm crashes: a cold
// node has no local state and must re-learn tips.
func (c *cluster) crashNode(n *simNode, cold bool) error {
	n.raw.Close()
	if err := n.rawBlob.Close(); err != nil {
		return fmt.Errorf("node %d: close blob store: %w", n.id, err)
	}
	n.metaStore.Close()
	n.raw, n.rawBlob, n.simRef, n.simBlob, n.casStore, n.metaStore = nil, nil, nil, nil, nil, nil
	if cold {
		if err := os.RemoveAll(n.cacheDir); err != nil {
			return fmt.Errorf("node %d: wipe cache: %w", n.id, err)
		}
		n.believedTips = make(map[string]map[string]string)
	}
	return c.openNodeHandles(n)
}

// close tears down every open handle; errors are ignored (teardown).
func (c *cluster) close() {
	for _, n := range c.nodes {
		if n.raw != nil {
			n.raw.Close()
		}
		if n.rawBlob != nil {
			_ = n.rawBlob.Close()
		}
		if n.metaStore != nil {
			n.metaStore.Close()
		}
	}
	if c.oracleBl != nil {
		_ = c.oracleBl.Close()
	}
	if c.oracle != nil {
		c.oracle.Close()
	}
	if c.oracleMeta != nil {
		c.oracleMeta.Close()
	}
}

// believedTip returns a node's belief for repo/refName (ZeroSHA = absent).
func (n *simNode) believedTip(repo, refName string) string {
	if m := n.believedTips[repo]; m != nil {
		if sha, ok := m[refName]; ok {
			return sha
		}
	}
	return ref.ZeroSHA
}

// setBelief records a node's view of repo/refName.
func (n *simNode) setBelief(repo, refName, sha string) {
	m := n.believedTips[repo]
	if m == nil {
		m = make(map[string]string)
		n.believedTips[repo] = m
	}
	m[refName] = sha
}
