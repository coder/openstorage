package sync

import "testing"

// docs/archive/phases/14-phase5-implementation-notes.md deferred #1: the openapi provider enum value "sourcehut" maps to
// git.sr.ht exactly like the pre-existing "sr.ht" spelling.
func TestProviderHostSourcehut(t *testing.T) {
	for _, provider := range []string{"sourcehut", "sr.ht"} {
		host, known := ProviderHost(provider)
		if !known || host != "git.sr.ht" {
			t.Errorf("ProviderHost(%q) = (%q, %v), want (git.sr.ht, true)", provider, host, known)
		}
	}
	if _, known := ProviderHost("code.storage"); known {
		t.Error(`ProviderHost("code.storage") = known, want unknown (fork-only provider)`)
	}
}
