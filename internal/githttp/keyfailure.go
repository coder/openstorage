package githttp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// keyFailureLogEvery is the shortest time between two log lines for one kind
// of key failure.
const keyFailureLogEvery = time.Minute

// keyFailureLog logs a failure of the key store, or of a key that an org
// registered, at most once for each kind in keyFailureLogEvery. A line says
// how many failures of its kind came before it and were not logged. Any
// client can make a request that reaches the key store, so one line for
// each request would let a client fill the log. The zero value works.
type keyFailureLog struct {
	mu   sync.Mutex
	last map[string]time.Time
	held map[string]int
	// now is the clock. Nil means time.Now.
	now func() time.Time
}

func (l *keyFailureLog) report(kind, org string, err error) {
	now := time.Now
	if l.now != nil {
		now = l.now
	}
	l.mu.Lock()
	t := now()
	if last, ok := l.last[kind]; ok && t.Sub(last) < keyFailureLogEvery {
		l.held[kind]++
		l.mu.Unlock()
		return
	}
	if l.last == nil {
		l.last, l.held = map[string]time.Time{}, map[string]int{}
	}
	l.last[kind] = t
	held := l.held[kind]
	l.held[kind] = 0
	l.mu.Unlock()
	line := fmt.Sprintf("verify token: %s, issuer %.80q: %v", kind, org, err)
	if held > 0 {
		line += fmt.Sprintf(" (%d earlier failures of this kind not logged)", held)
	}
	log.Print(line)
}

// verifyFailure returns the error that a client of a git or LFS route reads
// for a failed Verify, and logs the failures that the server must fix. The
// text of the errors that Verify returns is fixed: a store's error and a
// key parser's error stay behind Unwrap, and this logs them. An unknown
// issuer and a request that ended while the keys were read are not logged,
// because a client causes both.
func (s *Server) verifyFailure(ctx context.Context, err error) error {
	var keys *auth.KeySourceError
	if errors.As(err, &keys) {
		if errors.Is(err, meta.ErrNotFound) {
			return fmt.Errorf("issuer %q is not registered", keys.Org)
		}
		if ctx.Err() == nil {
			s.keyFailures.report("key store lookup failed", keys.Org, keys.Err)
		}
		return err
	}
	var stored *auth.StoredKeyError
	if errors.As(err, &stored) {
		s.keyFailures.report("a registered key cannot be used", stored.Org, stored.Err)
	}
	return err
}
