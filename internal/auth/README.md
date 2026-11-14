# `internal/auth`

## Overview
JWT auth contract for the HTTP API and the git surface. It defines claim types, scope and algorithm constants, a `Verifier` that checks compact JWS tokens against per-org PEM public keys, helpers to generate keys and mint tokens, and ref-policy parsing and matching used on the git receive path. The package does no I/O of its own: key lookup goes through an injected `KeySource`.

## Interface
- `SubjectLiteral`, `RepoClaimOrg`, `ScopeGitRead`, `ScopeGitWrite`, `ScopeRepoWrite`, `ScopeOrgRead` — fixed `sub` value, org sentinel `repo` value, and scope strings [auth.go:SubjectLiteral]
- `AllowedAlgorithms` — JWS allow-list: ES256, RS256, EdDSA, PS256, ES384, ES512 [auth.go:AllowedAlgorithms]
- `Claims`, `Claims.HasScope` — verified claim set (iss, sub, repo, scopes, iat, exp, refs, ops, sync_claim) and exact scope match [auth.go:Claims]
- `Verifier` — interface `Verify(ctx, token) (*Claims, error)` [auth.go:Verifier]
- `WithClaims`, `ClaimsFromContext` — store and read verified claims on a context [auth.go:WithClaims]
- `ParsePublicKeysPEM` — parses PKIX `PUBLIC KEY` blocks into ECDSA, RSA (>= 2048 bit), or Ed25519 keys [auth.go:ParsePublicKeysPEM]
- `GenerateKey`, `DefaultAlgorithm`, `MintToken` — create a signer, map a key to its alg, sign claims into a compact JWT [mint.go:MintToken]
- `MarshalPrivateKeyPEM`, `MarshalPublicKeyPEM`, `ParsePrivateKeyPEM` — PKCS#8 and PKIX PEM encode/decode; SEC1 also accepted on parse [mint.go:ParsePrivateKeyPEM]
- `RefPolicy` (`MarshalJSON`/`UnmarshalJSON` as a `[pattern, ops]` tuple), `OpNoPush`, `OpNoForcePush`, `OpVerifySig` [policy.go:RefPolicy]
- `EphemeralNamespacePrefix` (`refs/namespaces/ephemeral/`), `NormalizeRefName`, `EphemeralRefName` [policy.go:EphemeralNamespacePrefix]
- `ParseRefPolicies`, `MatchPolicy`, `FoldOps` — parse the refs claim, first-match lookup over exact, `*`, or trailing `/*` patterns, fold the deprecated ops claim into refs [policy.go:MatchPolicy]
- `MalformedError` — a token or claim that does not decode: its text is fixed (`malformed jwt`, `malformed jwt claims`, `malformed refs claim`) so a client can read it, and `Unwrap` has the decoder's error for a log [malformed.go:MalformedError]
- `KeySource`, `OrgKeyVerifier`, `NewVerifier`, `UnionKeySource` — `Verifier` backed by per-org keys, and an overlay of in-memory PEM keys for one org [verifier.go:OrgKeyVerifier]

## Invariants
- `WithClaims` panics on nil claims [auth.go:WithClaims]
- `ParsePublicKeysPEM` rejects non-`PUBLIC KEY` blocks, RSA under 2048 bits, unsupported key types, and an empty key set [auth.go:ParsePublicKeysPEM]
- `MintToken` rejects an alg outside the signature map or one that does not fit the key; `GenerateKey` rejects unknown algs [mint.go:MintToken]
- `RefPolicy.UnmarshalJSON` requires exactly a 2-element tuple [policy.go:UnmarshalJSON]
- `ParseRefPolicies` returns only a `*MalformedError` as its error [policy.go:ParseRefPolicies]
- `MatchPolicy` returns no match for an empty ref [policy.go:MatchPolicy]
- `Verify` rejects tokens without exactly two dots, non-canonical base64url segments, and JSON-serialized JWS with other than one signature [verifier.go:Verify]
- `Verify` accepts only algs in `signatureAlgorithms` and checks the key family matches the alg [verifier.go:keySupportsAlg]
- `validateClaims` requires `exp` > now and a non-empty `sub`; `iat` is not checked [verifier.go:validateClaims]
- `init` panics if `signatureAlgorithms` and `AllowedAlgorithms` disagree [verifier.go:init]
- `UnionKeySource` panics on a nil inner source, empty org, no PEMs, or unparseable PEM [verifier.go:UnionKeySource]
- `Verify` fails closed for an issuer with no keys [verifier.go:Verify]

## Side Effects
- `GenerateKey` reads `crypto/rand.Reader` [mint.go:GenerateKey]
- `OrgKeyVerifier.Verify` calls the injected `KeySource.PublicKeysForOrg` with the unverified `iss` claim [verifier.go:Verify]
- `FoldOps` and `UnionKeySource.PublicKeysForOrg` do not mutate their inputs. `FoldOps` returns the caller's `refs` slice itself when `ops` is empty and a fresh slice otherwise [policy.go:FoldOps]
- `UnionKeySource.PublicKeysForOrg` copies only for the union org. For every other org it returns the inner source's slice unchanged [verifier.go:PublicKeysForOrg]
- `OrgKeyVerifier.Verify` logs one `slog` warning (the default logger) when the signature of a token verifies and its claims do not decode into the claim set, for example a claim of the wrong JSON type. It logs nothing for a token that no key signed, and nothing for a payload that is not a JSON object, because the unverified `iss` is read first [verifier.go:Verify]
- No database, network, filesystem, or subprocess use [auth.go:Claims]

## Verification
- Run: `go test ./internal/auth/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
- Nightly fuzz lane (`.github/workflows/fuzz-nightly.yml`), one target per run, for `FuzzVerify` and `FuzzParseRefPolicies`. Local form from `docs/reference/testing.md`: `go test ./internal/auth -run '^$' -fuzz '^FuzzVerify$' -fuzztime 60s -parallel 8`
