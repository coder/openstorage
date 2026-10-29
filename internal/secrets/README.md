# `internal/secrets`

## Overview
AES-256-GCM envelope encryption for stored secret strings. `ParseKeys` builds a `Codec` from base64 keys, `Encrypt` seals under the first key, and `Decrypt` opens with any listed key and flags reads that used a non-active key as stale. The package is in-memory computation only.

## Interface
- `Marker`, `ReservedPrefix`, `KeySize` — envelope prefix `enc:v1:`, reserved namespace `enc:`, and the 32-byte key length [secrets.go:Marker]
- `ErrUnknownKey`, `ErrDecrypt` — sentinel errors for an unlisted key ID and for malformed or failed envelopes [secrets.go:ErrUnknownKey]
- `ParseKeys` — parses comma-separated base64 keys into a `*Codec`; empty input returns nil, nil [secrets.go:ParseKeys]
- `Codec` (with exported `DecryptOnly` flag), `Codec.KeyCount`, `Codec.ActiveKeyID` — key set, count, and the 8-hex fingerprint of the encrypt key [secrets.go:Codec]
- `IsEncrypted` — reports whether a stored string has the v1 marker [secrets.go:IsEncrypted]
- `Codec.Encrypt`, `Codec.Decrypt` — seal plaintext; open an envelope and return plaintext, a stale flag, and an error [secrets.go:Encrypt]

## Invariants
- `ParseKeys` rejects empty entries, invalid base64, keys that are not 32 bytes, and duplicates, and names only entry positions in errors [secrets.go:ParseKeys]
- `Encrypt` panics on a nil or empty codec, on a `DecryptOnly` codec, and on plaintext that already has the marker [secrets.go:Encrypt]
- `Encrypt` decrypts its own output before returning and panics if the round trip fails [secrets.go:Encrypt]
- `Decrypt` panics on a nil or empty codec or on input without the marker; malformed envelopes return `ErrDecrypt`. A malformed key-ID field is never echoed. A well-formed key ID with a malformed base64 payload is echoed as `malformed envelope (key id <id>)` [secrets.go:Decrypt]
- `Decrypt` returns `ErrUnknownKey` when no key matches, and sets stale only when a key at index > 0 opened the envelope [secrets.go:Decrypt]
- `validKeyID` accepts exactly 8 lowercase hex characters [secrets.go:validKeyID]

## Side Effects
- `Encrypt` reads `crypto/rand` for a fresh nonce and panics if the read fails [secrets.go:Encrypt]
- Failed assertions panic with the prefix `secrets assertion failed` [secrets.go:assertf]

## Verification
- Run: `go test ./internal/secrets/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
- Nightly fuzz lane (`.github/workflows/fuzz-nightly.yml`), one target per run, for `FuzzDecrypt` and `FuzzParseKeys`: `go test ./internal/secrets -run '^$' -fuzz '^FuzzDecrypt$' -fuzztime 4m -parallel 8 -timeout 30m`
