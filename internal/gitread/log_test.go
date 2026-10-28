package gitread

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLog(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	commits, err := Log(ctx, f.dir, f.c2, LogOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(commits) != 2 {
		t.Fatalf("got %d commits, want 2", len(commits))
	}
	// Newest first (invariant 9).
	if commits[0].SHA != f.c2 || commits[1].SHA != f.c1 {
		t.Fatalf("order = [%s %s], want [%s %s]", commits[0].SHA, commits[1].SHA, f.c2, f.c1)
	}
	head := commits[0]
	if head.AuthorName != "Alice Author" || head.AuthorEmail != "alice@example.com" {
		t.Errorf("author = %q <%q>", head.AuthorName, head.AuthorEmail)
	}
	if head.CommitterName != "Bob Committer" || head.CommitterEmail != "bob@example.com" {
		t.Errorf("committer = %q <%q>", head.CommitterName, head.CommitterEmail)
	}
	wantAuthor := time.Date(2024, 1, 2, 3, 4, 5, 0, time.FixedZone("", 2*3600))
	if !head.AuthorDate.Equal(wantAuthor) {
		t.Errorf("author date = %v, want %v", head.AuthorDate, wantAuthor)
	}
	wantCommitter := time.Date(2024, 1, 2, 4, 5, 6, 0, time.FixedZone("", 2*3600))
	if !head.CommitterDate.Equal(wantCommitter) {
		t.Errorf("committer date = %v, want %v", head.CommitterDate, wantCommitter)
	}
	if head.Message != "second commit\n" {
		t.Errorf("message = %q", head.Message)
	}
	if len(head.ParentSHAs) != 1 || head.ParentSHAs[0] != f.c1 {
		t.Errorf("parents = %v, want [%s]", head.ParentSHAs, f.c1)
	}

	// Root commit: parent_shas MUST be empty and non-nil (invariant 2).
	root := commits[1]
	if root.ParentSHAs == nil || len(root.ParentSHAs) != 0 {
		t.Errorf("root parents = %#v, want empty non-nil", root.ParentSHAs)
	}
	if root.Message != "root commit\n\nbody of root\n" {
		t.Errorf("root message = %q", root.Message)
	}
	if root.TreeSHA == "" {
		t.Error("root tree sha empty")
	}
}

// SubjectOnly fills Subject with %s and leaves Message empty, so a caller
// that reads Message by mistake gets "" instead of a silent subject.
func TestLogSubjectOnly(t *testing.T) {
	f := newFixture(t)
	commits, err := Log(context.Background(), f.dir, f.c2, LogOptions{SubjectOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	if len(commits) != 2 {
		t.Fatalf("got %d commits, want 2", len(commits))
	}
	for i, want := range []string{"second commit", "root commit"} {
		if c := commits[i]; c.Subject != want || c.Message != "" {
			t.Errorf("commits[%d] subject, message = %q, %q, want %q, \"\"", i, c.Subject, c.Message, want)
		}
	}
}

func TestLogSkipMaxCountAndPathFilter(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()

	one, err := Log(ctx, f.dir, f.c2, LogOptions{MaxCount: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(one) != 1 || one[0].SHA != f.c2 {
		t.Fatalf("MaxCount=1 -> %v", one)
	}

	skipped, err := Log(ctx, f.dir, f.c2, LogOptions{Skip: 1, MaxCount: 5})
	if err != nil {
		t.Fatal(err)
	}
	if len(skipped) != 1 || skipped[0].SHA != f.c1 {
		t.Fatalf("Skip=1 -> %v", skipped)
	}

	// script.sh only changed in c1.
	byPath, err := Log(ctx, f.dir, f.c2, LogOptions{Path: "script.sh"})
	if err != nil {
		t.Fatal(err)
	}
	if len(byPath) != 1 || byPath[0].SHA != f.c1 {
		t.Fatalf("path filter -> %v", byPath)
	}
}

func TestGetCommitUnsigned(t *testing.T) {
	f := newFixture(t)
	detail, err := GetCommit(context.Background(), f.dir, f.c1)
	if err != nil {
		t.Fatal(err)
	}
	if detail.SHA != f.c1 || detail.Message != "root commit\n\nbody of root\n" {
		t.Errorf("detail = %+v", detail.Commit)
	}
	if detail.Signature != "" || detail.Payload != "" {
		t.Errorf("unsigned commit carries signature %q / payload %q", detail.Signature, detail.Payload)
	}
}

// TestGetCommitSigned proves the gpgsig extraction is byte-exact: the
// (payload, signature) pair must pass `ssh-keygen -Y verify` against the
// signing key — the same check git itself performs.
func TestGetCommitSigned(t *testing.T) {
	if _, err := exec.LookPath("ssh-keygen"); err != nil {
		t.Skip("ssh-keygen not installed")
	}
	f := newFixture(t)
	ctx := context.Background()

	keyDir := t.TempDir()
	keyPath := filepath.Join(keyDir, "id_ed25519")
	if out, err := exec.Command("ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", keyPath).CombinedOutput(); err != nil {
		t.Fatalf("ssh-keygen: %v\n%s", err, out)
	}

	writeFile(t, f.dir, "signed.txt", "signed content\n", 0o644)
	mustGit(t, f.dir, "add", "signed.txt")
	mustGit(t, f.dir,
		"-c", "gpg.format=ssh",
		"-c", "user.signingkey="+keyPath,
		"commit", "-q", "-S", "-m", "signed commit")
	signed := strings.TrimSpace(mustGit(t, f.dir, "rev-parse", "HEAD"))

	detail, err := GetCommit(ctx, f.dir, signed)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(detail.Signature, "BEGIN SSH SIGNATURE") {
		t.Fatalf("signature = %q", detail.Signature)
	}
	if detail.Payload == "" || strings.Contains(detail.Payload, "gpgsig") {
		t.Fatalf("payload = %q", detail.Payload)
	}

	// Byte-exact verification of the extracted pair.
	pub, err := os.ReadFile(keyPath + ".pub")
	if err != nil {
		t.Fatal(err)
	}
	signers := filepath.Join(keyDir, "allowed_signers")
	if err := os.WriteFile(signers, []byte(`signer namespaces="git" `+string(pub)), 0o600); err != nil {
		t.Fatal(err)
	}
	sigFile := filepath.Join(keyDir, "commit.sig")
	if err := os.WriteFile(sigFile, []byte(detail.Signature+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("ssh-keygen", "-Y", "verify",
		"-f", signers, "-I", "signer", "-n", "git", "-s", sigFile)
	cmd.Stdin = strings.NewReader(detail.Payload)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("extracted (payload, signature) failed verification: %v\n%s", err, out)
	}
}
