# `internal/events`

## Overview
Outbound webhooks. The package signs and delivers webhook POSTs through a dial-time SSRF guard, and wraps `ref.RefStore` so each won non-ephemeral CAS emits a `push` event and a sync poke. Storage and queueing plug in through narrow interfaces.

## Interface
- `HeaderEvent`, `HeaderSignature`, `UserAgent` — `X-Pierre-Event`, `X-Pierre-Signature`, `Pierre-Webhook/1.0` [events.go:HeaderEvent]
- `EventPush`, `EventSyncStarted`, `EventSyncSucceeded`, `EventSyncFailed`, `EventPushUpstreamStarted`, `EventPushUpstreamSucceeded`, `EventPushUpstreamFailed` — event names [events.go:EventPush]
- `Webhook`, `WebhookLookup`, `WebhookLister`, `WebhookStore` — endpoint row and storage contracts [events.go:WebhookStore]
- `MetaWebhookStore` — adapts `meta.EventingStore` to `WebhookStore` [metastore.go:MetaWebhookStore]
- `Delivery`, `Deliverer` (`Attempt`, `Deliver`), `ErrWebhookGone` — queued payload, one POST attempt, retry loop with backoff [deliver.go:Deliverer]
- `TargetGuard` (`CheckRegistration`, `CheckHost`, `NewClient`, `DialContext`), `ErrTargetDenied` — SSRF guard and its HTTP client and dialer [guard.go:TargetGuard]
- `RepoSource`, `RefStoreOptions`, `RefStore`, `NewRefStore`, `CASRef`, `CASRefGen` — ref-store decorator that emits push events [refstore.go:NewRefStore]
- `EphemeralUpdate`, `BaseRepoID` — ephemeral-namespace check and base repo ID [refstore.go:EphemeralUpdate]
- `RepositoryRef`, `PushPayload`, `NewPushPayload` — push event body [refstore.go:NewPushPayload]
- `Sign` — `t=<ts>,sha256=<hex>` HMAC-SHA256 over `<ts>.<body>` [sign.go:Sign]

## Invariants
- `Deliverer.validate` asserts a non-nil store and non-empty webhook ID, event, and payload [deliver.go:validate]
- `Attempt` maps `meta.ErrNotFound` to `ErrWebhookGone`, fails on an empty URL or secret, and treats non-2xx as an error [deliver.go:Attempt]
- `Deliver` stops on `ErrWebhookGone` or cancel and gives up after `maxAttempts` (default 10) [deliver.go:Deliver]
- `defaultBackoff` asserts attempt >= 1 and caps the sleep at 64 s [deliver.go:defaultBackoff]
- `deniedReason` always denies metadata, unspecified, multicast, and broadcast addresses, and denies private classes unless `AllowPrivate` [guard.go:deniedReason]
- `vetHost` rejects the whole resolved set when one address is denied [guard.go:vetHost]
- `CheckRegistration` and `CheckHost` fail open on resolution errors; the dial-time check stays authoritative [guard.go:CheckRegistration]
- `DialContext` accepts only tcp, tcp4, tcp6 and dials only vetted IP literals [guard.go:DialContext]
- `NewClient` refuses redirects, sets a 30 s timeout, and disables keep-alives [guard.go:NewClient]
- `NewRefStore` asserts its options [refstore.go:NewRefStore]; `Sign` asserts a non-empty secret and positive timestamp [sign.go:Sign]

## Side Effects
- POSTs to the webhook URL with signed headers; re-reads the webhook row on each attempt; drains up to 4096 response bytes [deliver.go:Attempt]
- Sleeps between retries and logs drops, retries, and discards through slog [deliver.go:Deliver]
- DNS lookups (2 s for registration checks) and TCP dials (10 s) with a Control hook that rechecks the address [guard.go:dialVetted]
- On a won CAS, enqueues one delivery per webhook and a sync for non-ephemeral refs, logging failures instead of returning them [refstore.go:emitPush]
- Emission uses `context.WithoutCancel`, so a client hang-up does not drop the event [refstore.go:CASRef]

## Verification
- Run: `go test ./internal/events/... -tags=openstorage_viewcheck -count=1 -race -timeout=30m`
