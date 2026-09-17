package githttp

import (
	"encoding/json/v2"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

var seaweedCaptureFail = flag.Bool("seaweed-capture-fail", false, "deliberately fail the selected SeaweedFS capture diagnostic after acknowledgment")

// TestSeaweedCaptureDiagnostic deliberately uses the existing TestMain stack,
// not a nested test binary or a standalone container. Its opt-in failure lets
// an external caller check retained logs AFTER TestMain's automatic teardown.
func TestSeaweedCaptureDiagnostic(t *testing.T) {
	if !*seaweedCaptureFail {
		t.Skip("manual diagnostic: select this full test name with -seaweed-capture-fail")
	}
	dir, session := os.Getenv("OPENSTORAGE_TEST_ARTIFACT_DIR"), os.Getenv("TESTCONTAINERS_SESSION_ID")
	if dir == "" || session == "" || testBackend != backendPG || os.Getenv("OPENSTORAGE_TEST_S3_ENDPOINT") != "" {
		t.Fatal("diagnostic requires private capture, explicit session, and the managed pg lane")
	}
	paths, err := filepath.Glob(filepath.Join(dir, "seaweed-"+session+"-*", "status.json"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected exactly one current-run capture receipt, got %d (error %v)", len(paths), err)
	}
	data, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal("cannot read capture receipt")
	}
	var status struct {
		Session   string `json:"session"`
		Container string `json:"container"`
		Phase     string `json:"phase"`
	}
	if err := json.Unmarshal(data, &status); err != nil {
		t.Fatal("invalid capture receipt")
	}
	if status.Session != session || status.Phase != "pending" || len(status.Container) != 64 {
		t.Fatal("unexpected capture attribution/state")
	}
	// This is an actual SeaweedFS startup event, not test-authored log data.
	const event = "Start Seaweed S3 API Server"
	deadline := time.Now().Add(10 * time.Second)
	for {
		data, err := os.ReadFile(filepath.Join(filepath.Dir(paths[0]), "stderr.log"))
		if err != nil {
			t.Fatal("cannot read retained server stderr")
		}
		if strings.Contains(string(data), event) {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server event not acknowledged by capture")
		}
		time.Sleep(20 * time.Millisecond)
	}
	fmt.Printf("SEAWEED_CAPTURE_ACK session=%s container=%s event=s3-start\n", session, status.Container)
	t.Fatal("SEAWEED_CAPTURE_DIAGNOSTIC deliberate failure after capture acknowledgment")
}
