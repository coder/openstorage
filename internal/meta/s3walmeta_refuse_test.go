package meta

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/openstorage/internal/metrics"
	"github.com/coder/openstorage/internal/ref/s3wal"
)

// refuseCAS counts document reads and fails them on demand. With newETag,
// it answers every conditional read in full under a new ETag, as S3 does
// for a restored copy of the same bytes (MemCAS ETags derive from content).
type refuseCAS struct {
	*s3wal.MemCAS
	reads   atomic.Int32
	down    atomic.Bool
	newETag atomic.Bool
}

func (c *refuseCAS) Get(ctx context.Context, key string) (s3wal.Object, error) {
	c.reads.Add(1)
	if c.down.Load() {
		return s3wal.Object{}, errStoreDown
	}
	return c.MemCAS.Get(ctx, key)
}

func (c *refuseCAS) GetIfChanged(ctx context.Context, key, etag string) (s3wal.Object, error) {
	c.reads.Add(1)
	if c.down.Load() {
		return s3wal.Object{}, errStoreDown
	}
	if c.newETag.Load() {
		obj, err := c.MemCAS.Get(ctx, key)
		obj.ETag += "-restored"
		return obj, err
	}
	return c.MemCAS.GetIfChanged(ctx, key, etag)
}

// refuseFixture: a store with a 1 ms org-key refresher over a trusted
// document (org "org" with key "pem", a repo with an upstream), or over
// cold bytes it never adopted. prev is the document one seq before
// trusted.
type refuseFixture struct {
	t              *testing.T
	mem            *s3wal.MemCAS
	cas            *refuseCAS
	st             *S3WAL
	repoID         string
	prev, trusted  s3wal.Object
	mu             sync.Mutex
	fault          s3wal.PutFault
	interleave     func()
	mark           refuseMark
	landed         []byte // the stale-recovery row's landed write
	metricsAtStart float64
}

// refuseMark is the refusal state at one instant.
type refuseMark struct {
	body       []byte
	etag       string
	seq        int64
	view       *s3walMetaDoc
	rolledBack bool
	quarantine bool
}

func newRefuseFixture(t *testing.T, cold []byte) *refuseFixture {
	ctx := t.Context()
	f := &refuseFixture{t: t, mem: s3wal.NewMemCAS(), metricsAtStart: refusedTotal()}
	f.mem.Hooks.PutFault = func(_, key string) s3wal.PutFault {
		f.mu.Lock()
		defer f.mu.Unlock()
		fault := f.fault
		f.fault = s3wal.FaultNone
		return fault
	}
	f.mem.Hooks.AfterPut = func(_, _ string) {
		f.mu.Lock()
		run := f.interleave
		f.interleave = nil
		f.mu.Unlock()
		if run != nil {
			run()
		}
	}
	f.cas = &refuseCAS{MemCAS: f.mem}
	o := resolveOptions([]Option{withKeyRefreshInterval(time.Millisecond)})
	if cold != nil {
		if _, err := f.mem.Create(ctx, s3walMetaKey, cold); err != nil {
			t.Fatal(err)
		}
	}
	f.st = newS3WALStore(f.cas, nil, nil, o)
	t.Cleanup(f.st.Close)
	if cold == nil {
		f.repoID = seedS3WALUpstream(t, f.st)
		f.prev, _ = f.mem.Get(ctx, s3walMetaKey)
		if err := f.st.RequestSync(ctx, f.repoID); err != nil {
			t.Fatal(err)
		}
		f.trusted, _ = f.mem.Get(ctx, s3walMetaKey)
	}
	f.snap()
	return f
}

func refusedTotal() float64 {
	var sum float64
	for _, c := range []string{"decode", "stale", "vanished"} {
		sum += metrics.Value("openstorage_s3wal_meta_refused_total", map[string]string{"class": c})
	}
	return sum
}

// snap records the state that a refusal must keep or drop.
func (f *refuseFixture) snap() {
	_, _ = f.st.viewDoc(f.t.Context()) // populate the shared view
	f.mark = f.state()
}

func (f *refuseFixture) state() refuseMark {
	f.st.mu.Lock()
	defer f.st.mu.Unlock()
	return refuseMark{f.st.cacheBody, f.st.cacheETag, f.st.lastSeq, f.st.cacheView, f.st.rolledBack, f.st.quarantined != nil}
}

// put replaces the stored document with body (or deletes it for nil).
func (f *refuseFixture) put(body []byte) {
	f.t.Helper()
	ctx := f.t.Context()
	cur, err := f.mem.Get(ctx, s3walMetaKey)
	switch {
	case errors.Is(err, s3wal.ErrNotExist) && body != nil:
		_, err = f.mem.Create(ctx, s3walMetaKey, body)
	case err == nil && body == nil:
		err = f.mem.Delete(ctx, s3walMetaKey)
	case err == nil:
		_, err = f.mem.Advance(ctx, s3walMetaKey, body, cur.ETag)
	}
	if err != nil {
		f.t.Fatal(err)
	}
}

// edit returns trusted with JSON fields replaced.
func (f *refuseFixture) edit(fields map[string]any) []byte {
	var doc map[string]any
	if err := json.Unmarshal(f.trusted.Body, &doc); err != nil {
		f.t.Fatal(err)
	}
	for k, v := range fields {
		doc[k] = v
	}
	out, err := json.Marshal(doc)
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

func (f *refuseFixture) armPut(fault s3wal.PutFault, interleave func()) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fault, f.interleave = fault, interleave
}

func isType[E error](err error) bool { var e E; return errors.As(err, &e) }

// TestS3WALMetaRefusalMatrix is the issue #516 test matrix: every
// before-adoption refusal class and three controls, each checked for its
// error, the state it keeps, auth, the refresher, the metric, a later
// write, and how it ends.
func TestS3WALMetaRefusalMatrix(t *testing.T) {
	ctx := context.Background()
	garbage := []byte(`{"v":2,"seq":`)
	for _, row := range []struct {
		name  string
		cold  []byte
		arm   func(f *refuseFixture) error
		isErr func(error) bool
		latch bool
		end   func(f *refuseFixture)
		heals bool
	}{
		{"decode failure, cold", garbage, func(f *refuseFixture) error { return CheckFormat(ctx, f.st) },
			isType[*CorruptDocumentError], false, func(f *refuseFixture) { f.put([]byte(`{"v":2,"seq":1,"writer":"w"}`)) }, true},
		{"decode failure, cached", nil, func(f *refuseFixture) error { f.put(garbage); return CheckFormat(ctx, f.st) },
			isType[*CorruptDocumentError], true, func(f *refuseFixture) { f.put(f.trusted.Body) }, true},
		{"version mismatch, cached", nil, func(f *refuseFixture) error { f.put(f.edit(map[string]any{"v": 1})); return CheckFormat(ctx, f.st) },
			isType[*FormatVersionError], true, func(f *refuseFixture) { f.put(f.trusted.Body) }, true},
		{"older seq", nil, func(f *refuseFixture) error { f.put(f.prev.Body); return CheckFormat(ctx, f.st) },
			isType[*StaleDocumentError], true, func(f *refuseFixture) { f.put(f.trusted.Body); f.cas.newETag.Store(true) }, true}, // heals on the bytes, not a 304
		{"same seq, other bytes", nil, func(f *refuseFixture) error {
			f.put(f.edit(map[string]any{"next_key_id": 7}))
			return CheckFormat(ctx, f.st)
		},
			isType[*StaleDocumentError], true, func(f *refuseFixture) { f.put(f.trusted.Body) }, true},
		{"vanished object", nil, func(f *refuseFixture) error { f.put(nil); return CheckFormat(ctx, f.st) },
			func(err error) bool { return err != nil && strings.Contains(err.Error(), "vanished after seq") }, true,
			func(f *refuseFixture) { f.put(f.trusted.Body) }, true},
		{"stale recovery read", nil, func(f *refuseFixture) error {
			f.armPut(s3wal.FaultAmbiguousApplied, func() { o, _ := f.mem.Get(ctx, s3walMetaKey); f.landed = o.Body; f.put(f.prev.Body) })
			_, err := f.st.CreateRepo(ctx, "org", "https://example.com/amb.git", "main")
			return err
		}, func(err error) bool {
			return err != nil && strings.Contains(err.Error(), "does not continue the document")
		}, true,
			func(f *refuseFixture) { f.put(f.landed) }, false}, // our own landed write is past the cache: restart only
		{"garbled reply, then a valid successor", nil, func(f *refuseFixture) error {
			f.put(garbage)
			_ = CheckFormat(ctx, f.st)
			f.put(f.edit(map[string]any{"seq": f.mark.seq + 1, "writer": "fedcba9876543210fedcba9876543210"}))
			return CheckFormat(ctx, f.st)
		}, isType[*StaleDocumentError], true, func(f *refuseFixture) { f.put(f.trusted.Body) }, true},
		{"counter at maximum (control)", nil, func(f *refuseFixture) error {
			f.put(f.edit(map[string]any{"seq": f.mark.seq + 1, "next_key_id": math.MaxInt64}))
			f.snap()
			_, err := f.st.AddSigningKey(ctx, "org", "ssh-ed25519 AAAAtest k")
			return err
		}, func(err error) bool { return err != nil && strings.Contains(err.Error(), "is at its maximum") }, false, func(*refuseFixture) {}, true},
		{"store error (control)", nil, func(f *refuseFixture) error {
			f.cas.down.Store(true)
			defer f.cas.down.Store(false)
			return CheckFormat(ctx, f.st)
		}, func(err error) bool { return errors.Is(err, errStoreDown) }, false, func(*refuseFixture) {}, true},
		{"secret decrypt failure (control)", nil, func(f *refuseFixture) error {
			enc := NewS3WAL(f.mem, WithSecretsCodec(newCodec(f.t, newTestKey(f.t))))
			defer enc.Close()
			wh, err := enc.CreateWebhook(ctx, "org", "https://hooks.example.com/x", "s")
			if err != nil {
				f.t.Fatal(err)
			}
			f.snap()
			_, err = f.st.GetWebhook(ctx, wh.ID)
			return err
		}, func(err error) bool { return err != nil && strings.Contains(err.Error(), "is encrypted") }, false, func(*refuseFixture) {}, true},
	} {
		t.Run(row.name, func(t *testing.T) {
			f := newRefuseFixture(t, row.cold)
			if err := row.arm(f); !row.isErr(err) {
				t.Fatalf("refusal error = %v", err)
			}
			if got, want := f.state(), row.latch; got.rolledBack != want || got.quarantine ||
				!bytes.Equal(got.body, f.mark.body) || got.etag != f.mark.etag || got.seq != f.mark.seq || got.view != f.mark.view {
				t.Fatalf("state = %+v, want the marked state with latch %v", got, want)
			}
			keys, kerr := f.st.PublicKeysForOrg(ctx, "org")
			if row.cold != nil && kerr == nil {
				t.Fatalf("PublicKeysForOrg on a cold refusal = %v; want an error", keys)
			} else if row.cold == nil && (kerr != nil || !slices.Equal(keys, []string{"pem"})) {
				t.Fatalf("PublicKeysForOrg = %v, %v; want the trusted keys", keys, kerr)
			}
			// The refresher survives: it keeps reading.
			deadline := time.Now().Add(10 * time.Second)
			for start := f.cas.reads.Load(); f.cas.reads.Load() < start+3; time.Sleep(time.Millisecond) {
				if time.Now().After(deadline) {
					t.Fatal("the org-key refresher stopped reading")
				}
			}
			if want, d := map[bool]float64{true: 1, false: 0}[row.latch], refusedTotal()-f.metricsAtStart; d != want {
				t.Fatalf("refusals counted = %v, want %v (once per entry)", d, want)
			}
			before, _ := f.mem.Get(ctx, s3walMetaKey)
			_, werr := f.st.CreateRepo(ctx, "org", "https://example.com/later.git", "main")
			if refused := row.latch || row.cold != nil; refused != (werr != nil) {
				t.Fatalf("later write = %v, want refused %v", werr, refused)
			} else if after, _ := f.mem.Get(ctx, s3walMetaKey); refused && after.ETag != before.ETag {
				t.Fatal("a refused process wrote meta.json")
			}
			row.end(f)
			if err := CheckFormat(ctx, f.st); (err == nil) != row.heals {
				t.Fatalf("after the end action: %v, want healed %v", err, row.heals)
			}
			if row.heals {
				f.cas.newETag.Store(false)
				if _, err := f.st.UpsertOrgWithKeys(ctx, "healed", []string{"pem2"}); err != nil || f.state().rolledBack {
					t.Fatalf("write after healing = %v (latched %v), want success", err, f.state().rolledBack)
				}
			}
		})
	}
}

// TestS3WALMetaRefuseIsTheOnlyWriter guards the choke point: only refuse
// and heal may assign the latch.
func TestS3WALMetaRefuseIsTheOnlyWriter(t *testing.T) {
	allowed := map[string][]string{
		"rolledBack": {"refuse", "heal"},
	}
	files, err := filepath.Glob("s3walmeta*.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v %v", files, err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range file.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok || fn.Body == nil {
				continue
			}
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				as, ok := n.(*ast.AssignStmt)
				if !ok {
					return true
				}
				for _, lhs := range as.Lhs {
					if sel, ok := lhs.(*ast.SelectorExpr); ok {
						if fns, guarded := allowed[sel.Sel.Name]; guarded && !slices.Contains(fns, fn.Name.Name) {
							t.Errorf("%s: %s assigns %s outside %v", fset.Position(as.Pos()), fn.Name.Name, sel.Sel.Name, fns)
						}
					}
				}
				return true
			})
		}
	}
}
