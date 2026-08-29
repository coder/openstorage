package sync

import "strings"

// Bounds on parsed per-ref rejections (issue #90). The porcelain output is
// produced by our own git subprocess, but "remote rejected" reasons quote
// text the UPSTREAM sent — treat it as untrusted: cap the entry count and
// reason length so a hostile upstream cannot bloat event payloads.
const (
	maxRejectedRefs      = 100
	maxRejectedReasonLen = 200
)

// parsePushRejections extracts upstream-rejected refs from `git push
// --porcelain` stdout. Porcelain ref lines are TAB-separated —
// "<flag>\t<from>:<to>\t<summary>" — with flag '!' marking a ref the
// upstream refused (e.g. "[rejected] (non-fast-forward)" or
// "[remote rejected] (<server reason>)"). The reason is the LAST
// parenthesized group of the summary when present, else the summary
// verbatim. Unparseable lines are skipped, never fatal: the caller already
// knows the push failed from the exit code; this only enriches the event.
func parsePushRejections(porcelain string) []RejectedRef {
	var rejected []RejectedRef
	for line := range strings.SplitSeq(porcelain, "\n") {
		if !strings.HasPrefix(line, "!\t") {
			continue
		}
		fields := strings.SplitN(line[2:], "\t", 2)
		if len(fields) != 2 {
			continue
		}
		refspec, summary := fields[0], strings.TrimSpace(fields[1])
		// The destination ref names what the upstream refused. A refspec
		// without ':' (git never emits one, but stay defensive) is used
		// verbatim.
		ref := refspec
		if _, dst, ok := strings.Cut(refspec, ":"); ok {
			ref = dst
		}
		if ref == "" || summary == "" {
			continue
		}
		reason := summary
		if open := strings.LastIndex(summary, "("); open >= 0 && strings.HasSuffix(summary, ")") {
			if inner := strings.TrimSpace(summary[open+1 : len(summary)-1]); inner != "" {
				reason = inner
			}
		}
		if len(reason) > maxRejectedReasonLen {
			reason = reason[:maxRejectedReasonLen]
		}
		rejected = append(rejected, RejectedRef{Ref: ref, Reason: reason})
		if len(rejected) == maxRejectedRefs {
			break
		}
	}
	return rejected
}
