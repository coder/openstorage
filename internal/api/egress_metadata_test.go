package api

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/coder/openstorage/internal/events"
)

// TestRegistrationDeniesCloudMetadataWithPrivateAllowed: with private
// targets admitted (OPENSTORAGE_SYNC_ALLOW_PRIVATE and
// OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE), a sync create and a webhook
// registration still refuse a cloud metadata endpoint, such as Alibaba
// Cloud's 100.100.100.200 inside RFC 6598 shared space. Nothing is stored.
// (events.TestTargetGuardMetadataNeighborsKeepTheirClass shows that the
// addresses around an endpoint stay admitted.)
func TestRegistrationDeniesCloudMetadataWithPrivateAllowed(t *testing.T) {
	runBackends(t, func(t *testing.T, e *testEnv) {
		s := newPendingSyncStack(t, e, true) // its guard admits private targets
		for _, host := range []string{"100.100.100.200", "169.254.170.2", "[fd20:ce::254]"} {
			name := "meta-" + randomHex(t, 4)
			code, _, msg := s.create(t, name, `{"upstream":{"provider":"gitea","owner":"o","name":"n","host":"http://`+host+`"}}`)
			if code != http.StatusBadRequest || !strings.HasSuffix(msg, "denied: cloud metadata address") {
				t.Fatalf("sync create to %s = %d %q, want 400 cloud metadata address", host, code, msg)
			}
			if _, err := e.store.GetRepoByURL(t.Context(), s.org, name); err == nil {
				t.Fatalf("sync create to %s stored a repository", host)
			}
		}

		srv, store := newWebhooksServerWithGuard(t, e, events.TargetGuard{AllowPrivate: true})
		token := orgToken(t, e, e.newOrg(t))
		for _, rawURL := range []string{"http://100.100.100.200/hook", "http://169.254.170.23/hook", "http://[fd00:ec2::23]/hook"} {
			body, err := json.Marshal(createWebhookRequest{URL: rawURL})
			if err != nil {
				t.Fatal(err)
			}
			var envlp errorEnvelope
			if status := whRequest(t, http.MethodPost, srv.URL+"/api/v1/webhooks", token, body, &envlp); status != http.StatusBadRequest ||
				!strings.HasSuffix(envlp.Error, "denied: cloud metadata address") {
				t.Fatalf("register %s = %d %q, want 400 cloud metadata address", rawURL, status, envlp.Error)
			}
		}
		if got := len(store.hooks); got != 0 {
			t.Fatalf("stored %d webhooks, want 0", got)
		}
	})
}
