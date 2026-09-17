package seaweedcapture_test

import (
	"context"
	"encoding/json/v2"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/coder/openstorage/internal/ref/s3wal/s3waltest"
	"github.com/coder/openstorage/internal/testutil/seaweedcapture"
)

// These opt-in checks use the caller's artifact directory so failure evidence
// survives the test. Run under the same lifecycle lock as the other fixtures.
func TestRealFixtureCapture(t *testing.T) {
	dir := os.Getenv("OPENSTORAGE_TEST_ARTIFACT_DIR")
	if dir == "" {
		t.Skip("requires private capture directory and Docker")
	}
	if os.Getenv("TESTCONTAINERS_SESSION_ID") == "" {
		t.Fatal("explicit session required")
	}
	for _, scenario := range []string{"shared-constructor", "startup-failure"} {
		t.Run(scenario, func(t *testing.T) {
			before, err := filepath.Glob(filepath.Join(dir, "seaweed-*", "status.json"))
			if err != nil {
				t.Fatal(err)
			}
			if scenario == "shared-constructor" {
				_, terminate, err := s3waltest.StartSeaweedFSContainer(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				terminate()
			} else {
				native := errors.New("deliberate startup failure after server event")
				_, err := seaweedcapture.Start(t.Context(), testcontainers.GenericContainerRequest{
					Image:        s3waltest.SeaweedFSImage,
					Cmd:          []string{"server", "-s3", "-dir=/data", "-ip.bind=0.0.0.0"},
					ExposedPorts: []string{"8333/tcp"},
					WaitingFor:   wait.ForLog("Start Seaweed S3 API Server").WithStartupTimeout(2 * time.Minute),
					Started:      true,
					LifecycleHooks: []testcontainers.ContainerLifecycleHooks{{PostReadies: []testcontainers.ContainerHook{
						func(context.Context, testcontainers.Container) error { return native },
					}}},
				})
				if !errors.Is(err, native) {
					t.Fatalf("native startup error not preserved: %v", err)
				}
			}
			after, err := filepath.Glob(filepath.Join(dir, "seaweed-*", "status.json"))
			if err != nil || len(after) != len(before)+1 {
				t.Fatalf("expected one new receipt: before=%d after=%d error=%v", len(before), len(after), err)
			}
			for _, path := range after {
				if slices.Contains(before, path) {
					continue
				}
				data, err := os.ReadFile(path)
				if err != nil {
					t.Fatal(err)
				}
				var status struct {
					Phase   string   `json:"phase"`
					Session string   `json:"session"`
					Errors  []string `json:"errors"`
				}
				if err := json.Unmarshal(data, &status); err != nil {
					t.Fatal(err)
				}
				if status.Phase != "finalized" || status.Session != os.Getenv("TESTCONTAINERS_SESSION_ID") {
					t.Fatal("capture not finalized/attributed")
				}
				wantError := scenario == "startup-failure"
				if wantError && (len(status.Errors) != 1 || status.Errors[0] != "startup-error") || !wantError && len(status.Errors) != 0 {
					t.Fatalf("unexpected capture errors: %v", status.Errors)
				}
				data, err = os.ReadFile(filepath.Join(filepath.Dir(path), "stderr.log"))
				if err != nil || !strings.Contains(string(data), "Start Seaweed S3 API Server") {
					t.Fatal("server startup event not retained")
				}
			}
		})
	}
}
