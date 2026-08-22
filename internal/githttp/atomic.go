package githttp

// Atomic-push detection (issue #28). githooks(5) exposes no atomic
// indicator to server-side hooks, but the smart-HTTP layer sees the raw
// receive-pack request: a client that negotiated `git push --atomic`
// requests the `atomic` capability on its first command pkt-line
// (gitprotocol-pack, update-requests grammar). handleServiceRPC sniffs
// that capability and threads it to the pre-receive hook via EnvAtomic,
// which then evaluates the token's ref policy across ALL commands and
// denies the push BEFORE publication when any command is denied: under
// --atomic a single update-hook denial makes receive-pack abort every ref
// update anyway, so the early denial changes no ref outcome — it only
// keeps the quarantined pack from becoming a registered, unreachable,
// quota-consuming blob (registered packs are not sweeper orphans), and
// keeps an allowed command's already-committed PG CAS from durably
// surviving a push git reports as wholly failed.

import (
	"bytes"
	"io"
	"strconv"
	"strings"
)

// sniffPeekBudget bounds how many request bytes sniffAtomicPush buffers
// while looking for the first command line, which may be preceded by an
// unbounded `shallow <oid>` prefix (gitprotocol-pack update-requests;
// pushes from shallow clones). ~20k shallow lines fit. A stream that
// exhausts the budget is treated as ATOMIC — failing closed, because the
// prefix is client-controlled: treating it as non-atomic would let a
// pusher pad shallow lines to bypass the gate (Codex round 1), re-opening
// both the pack leak and the durable-CAS atomicity violation. The
// over-restriction is small and safe: a wholly-allowed push is unaffected
// by the atomic gate, and a mixed-policy push from a >1 MiB-shallow clone
// is denied with the split-the-push remedy instead of leaking.
const sniffPeekBudget = 1 << 20

// maxPktLen is the protocol's pkt-line length ceiling (gitprotocol-common:
// 65520 = 4 length bytes + 65516 payload).
const maxPktLen = 65520

// sniffAtomicPush reports whether the receive-pack request stream in r
// requests the `atomic` capability, returning a reader that replays the
// consumed bytes followed by the rest of r. The input is untrusted wire
// data: a framing surprise returns (false, replay) — never a panic —
// which is safe because git parses the identical bytes with the same
// grammar and dies on them before pre-receive can publish anything. The
// one place our scan can give up where git's would not — a shallow prefix
// exhausting sniffPeekBudget — fails closed (atomic) instead, see the
// budget comment. False positives on the capability itself are impossible
// relative to receive-pack's behavior: the sniffed token is the very
// request that makes receive-pack atomic.
func sniffAtomicPush(r io.Reader) (bool, io.Reader) {
	var peeked bytes.Buffer
	tee := io.TeeReader(r, &peeked)
	replay := func() io.Reader {
		return io.MultiReader(bytes.NewReader(peeked.Bytes()), r)
	}
	hdr := make([]byte, 4)
	for peeked.Len() < sniffPeekBudget {
		if _, err := io.ReadFull(tee, hdr); err != nil {
			return false, replay()
		}
		n, err := strconv.ParseUint(string(hdr), 16, 32)
		if err != nil || n > maxPktLen {
			return false, replay()
		}
		if n < 4 {
			// 0000 flush ends the command list with no command line seen
			// (a client with nothing to push); 0001–0003 are not valid in
			// update-requests. Either way there are no capabilities.
			return false, replay()
		}
		payload := make([]byte, n-4)
		if _, err := io.ReadFull(tee, payload); err != nil {
			return false, replay()
		}
		if bytes.HasPrefix(payload, []byte("shallow ")) {
			continue
		}
		// First command line (or `push-cert`): capabilities follow the
		// NUL, space-separated. Absent NUL means no capabilities.
		nul := bytes.IndexByte(payload, 0)
		if nul < 0 {
			return false, replay()
		}
		for _, tok := range strings.Fields(string(payload[nul+1:])) {
			if tok == "atomic" {
				return true, replay()
			}
		}
		return false, replay()
	}
	// Peek budget exhausted inside the (client-controlled) shallow prefix:
	// fail closed. See sniffPeekBudget.
	return true, replay()
}
