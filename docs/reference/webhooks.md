# Outbound Webhooks

The backend delivers HTTP POST notifications to customer-configured endpoints. (Endpoint
registration/management is not exposed via the public SDKs — likely dashboard/control-plane;
an OSS backend needs its own config surface for this.)

## Delivery headers

```
X-Pierre-Event: <event_type>                     # push | repo.sync.started | repo.sync.succeeded | repo.sync.failed
X-Pierre-Signature: t=<unix_seconds>,sha256=<hex_hmac>
Content-Type: application/json
User-Agent: Pierre-Webhook/1.0
```

## Signature (verified identically by all three SDKs)

```
signed_data = "<timestamp>" + "." + <raw request body bytes>
signature   = hex(HMAC_SHA256(secret, signed_data))
header      = "t=" + timestamp + ",sha256=" + signature
```

Receiver validation the backend must be compatible with:
- timestamp age ≤ 300s by default (clients reject older), and not more than 60s in the
  future → the backend must sign with the *send-time* timestamp and deliver promptly,
  re-signing on retries.
- Constant-time comparison of hex digests.

## Event payloads

### `push` (fired on any persistent-namespace ref update: API commits, merges, git push)

```json
{
  "repository": {"id": "<repo_id>", "url": "<repo url>"},
  "ref": "refs/heads/main",
  "before": "<40-hex, all zeros for ref creation>",
  "after": "<40-hex, all zeros for ref deletion>",
  "customer_id": "<org/customer id>",
  "pushed_at": "2026-01-20T10:30:00Z"
}
```

All seven fields are required — the Go SDK rejects payloads with any empty field
("invalid push payload").

### `repo.sync.started` / `repo.sync.succeeded` / `repo.sync.failed`

From official docs (guides/webhooks.md): payloads carry `type`, repo identity and sync
metadata; `repo.sync.failed` additionally has `error`, one of:
`"authentication failed"`, `"failed to clone repository"`, `"failed to push to storage"`,
`"repository configuration error"`, `"upstream repository unreachable"`,
`"internal error"`, `"session unavailable"`, `"sync failed"`.
See [`../upstream/guides/webhooks.md`](../upstream/guides/webhooks.md) for the exact field tables.

Outbound push-upstream runs (our extension — see the
[HTTP API](http-api.md) "Upstream sync") reuse these three event types with a
`direction: "push"` discriminator; inbound pull payloads are unchanged on the
wire (no `direction` field), so absence means pull. Push-direction payloads may
additionally carry `forced: true` (the run used force refspecs) and, on
`repo.sync.failed` with `error: "push rejected by upstream"`, a
`rejected_refs: [{ref, reason}]` array naming each ref the upstream refused
(e.g. `non-fast-forward`). Push-direction `error` values reuse the strings
above plus `"failed to push to upstream"` (generic outbound push failure) and
`"push rejected by upstream"`. `run_count`/`is_first_sync` count outbound runs
independently of inbound ones.

### Unknown events

SDKs pass through unrecognized `X-Pierre-Event` values as `{type, raw}` — safe to add new
event types.

## Implementation guidance

- Emit events after the ref update commits atomically (queue + at-least-once delivery;
  receivers use timestamp checks, not idempotency keys — keep retry windows short or
  re-sign).
- Ephemeral-namespace updates do not emit `push` events (see the [domain model](../explanation/domain-model.md), invariant 7).
