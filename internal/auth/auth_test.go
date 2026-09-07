package auth

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"testing"
)

// pemPublicKey encodes pub as one PKIX "PUBLIC KEY" PEM block.
// Keys are generated at runtime; nothing is committed.
func pemPublicKey(t *testing.T, pub any) []byte {
	t.Helper()
	der, err := x509.MarshalPKIXPublicKey(pub)
	if err != nil {
		t.Fatal(err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der})
}

func TestParsePublicKeysPEM_AllKeyFamilies(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	edPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}

	// One file, three concatenated blocks (the OPENSTORAGE_ORG_PUBKEYS shape).
	var data []byte
	data = append(data, pemPublicKey(t, &ecKey.PublicKey)...)
	data = append(data, pemPublicKey(t, &rsaKey.PublicKey)...)
	data = append(data, pemPublicKey(t, edPub)...)

	keys, err := ParsePublicKeysPEM(data)
	if err != nil {
		t.Fatalf("ParsePublicKeysPEM: %v", err)
	}
	if len(keys) != 3 {
		t.Fatalf("got %d keys, want 3", len(keys))
	}
	if _, ok := keys[0].(*ecdsa.PublicKey); !ok {
		t.Fatalf("keys[0] = %T, want *ecdsa.PublicKey", keys[0])
	}
	if _, ok := keys[1].(*rsa.PublicKey); !ok {
		t.Fatalf("keys[1] = %T, want *rsa.PublicKey", keys[1])
	}
	if _, ok := keys[2].(ed25519.PublicKey); !ok {
		t.Fatalf("keys[2] = %T, want ed25519.PublicKey", keys[2])
	}
}

func TestParsePublicKeysPEM_Rejections(t *testing.T) {
	ecKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privDER, err := x509.MarshalPKCS8PrivateKey(ecKey)
	if err != nil {
		t.Fatal(err)
	}
	privPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privDER})

	for name, data := range map[string][]byte{
		"empty input":       nil,
		"garbage":           []byte("not pem at all"),
		"private key block": privPEM,
	} {
		if _, err := ParsePublicKeysPEM(data); err == nil {
			t.Errorf("%s: expected error, got nil", name)
		}
	}
}

func TestClaimsContextRoundTrip(t *testing.T) {
	if _, ok := ClaimsFromContext(context.Background()); ok {
		t.Fatal("empty context unexpectedly carries claims")
	}
	want := &Claims{Iss: "selfhost", Sub: SubjectLiteral, Repo: "r1", Scopes: []string{ScopeGitRead}}
	ctx := WithClaims(context.Background(), want)
	got, ok := ClaimsFromContext(ctx)
	if !ok || got != want {
		t.Fatalf("ClaimsFromContext = (%v, %v), want (%v, true)", got, ok, want)
	}
}

func TestClaimsHasScope(t *testing.T) {
	c := &Claims{Scopes: []string{ScopeGitRead, ScopeRepoWrite}}
	if !c.HasScope(ScopeGitRead) || !c.HasScope(ScopeRepoWrite) {
		t.Fatal("granted scopes not found")
	}
	// Exact match only: no implication between scopes.
	if c.HasScope(ScopeGitWrite) || c.HasScope(ScopeOrgRead) || c.HasScope("git") {
		t.Fatal("HasScope matched a scope that was not granted")
	}
}
