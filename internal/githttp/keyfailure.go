package githttp

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/coder/openstorage/internal/auth"
	"github.com/coder/openstorage/internal/meta"
)

// keyFailureLogEvery is the shortest time between two log lines for one kind
// of key failure.
const keyFailureLogEvery = time.Minute

// keyFailureLogCause is the most runes of a failure's text that a log line
// holds.
const keyFailureLogCause = 300

// causeText returns the text of err for a log line. A store repeats the
// issuer in its own error text, and a client chose the issuer, so a long one
// would fill the line and push the cause out. The quoted issuer in the text
// becomes "<issuer>". A text over keyFailureLogCause runes keeps its first
// third and its last two thirds, so both the context and the root cause stay.
func causeText(org string, err error) string {
	text := strings.ReplaceAll(err.Error(), strconv.Quote(org), `"<issuer>"`)
	r := []rune(text)
	if len(r) <= keyFailureLogCause {
		return text
	}
	head := keyFailureLogCause / 3
	return string(r[:head]) + "..." + string(r[len(r)-(keyFailureLogCause-head):])
}

// KeyFailureLog logs a failure of the key store, or of a key that an org
// registered, at most once for each kind in keyFailureLogEvery. A line says
// how many failures of its kind came before it and were not logged. Any
// client can make a request that reaches the key store, so one line for
// each request would let a client fill the log. The zero value works.
type KeyFailureLog struct {
	mu   sync.Mutex
	last map[string]time.Time
	held map[string]int
	// Now is the clock. Nil means time.Now.
	Now func() time.Time
}

// Report logs err, an error that Verify returned, when the server must fix
// its cause: the key store failed, or a key that the org registered does not
// parse. An unknown issuer and a request that ended while the keys were read
// are not logged, because a client causes both. Any other error is not
// logged.
func (l *KeyFailureLog) Report(ctx context.Context, err error) {
	var keys *auth.KeySourceError
	if errors.As(err, &keys) {
		if !errors.Is(err, meta.ErrNotFound) && ctx.Err() == nil {
			l.report("key store lookup failed", keys.Org, keys.Err)
		}
		return
	}
	var stored *auth.StoredKeyError
	if errors.As(err, &stored) {
		l.report("a registered key cannot be used", stored.Org, stored.Err)
	}
}

func (l *KeyFailureLog) report(kind, org string, err error) {
	now := time.Now
	if l.Now != nil {
		now = l.Now
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
	line := fmt.Sprintf("verify token: %s, issuer %.80q: %q", kind, org, causeText(org, err))
	if held > 0 {
		line += fmt.Sprintf(" (%d earlier failures of this kind not logged)", held)
	}
	log.Print(line)
}

// verifyFailure returns the error that a client of a git or LFS route reads
// for a failed Verify, and logs the failures that the server must fix (see
// KeyFailureLog.Report). The text of the two key failures that Verify returns
// does not hold the cause: a store's error and a key parser's error stay
// behind Unwrap. An issuer that the store does not know gets its own text.
func (s *Server) verifyFailure(ctx context.Context, err error) error {
	s.keyFailures.Report(ctx, err)
	var keys *auth.KeySourceError
	if errors.As(err, &keys) && errors.Is(err, meta.ErrNotFound) {
		return fmt.Errorf("issuer %q is not registered", keys.Org)
	}
	return err
}
