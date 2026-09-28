# Outbound Webhooks

The backend sends HTTP POST notifications to customer-configured endpoints. This page
covers endpoint management, delivery headers, signatures, and event payloads. Delivery
egress rules: [`OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE`](configuration.md#openstorage_webhook_allow_private).

## Endpoint management

The public SDKs do not manage endpoints; upstream uses its dashboard/control plane.
open.storage adds its own org-level routes. All require `repo:write` with `repo: "org"`
and return the plain `{"error"}` envelope on failure.

| Method & path | Body → Response |
|---|---|
| `POST /api/v1/webhooks` | `{url, secret?}` → 200 `{id, url, secret, created_at}` |
| `GET /api/v1/webhooks` | → 200 `{webhooks: [{id, url, created_at}]}` |
| `DELETE /api/v1/webhooks/{id}` | → 204; 404 |

The signing secret is returned exactly once, from `POST`; the server generates one when
the body omits it. `GET` never echoes secrets.

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

Receivers (the SDKs) validate:

- **Timestamp window:** age ≤ 300s by default (older is rejected) and no more than 60s in
  the future. The backend therefore signs with the *send-time* timestamp, delivers
  promptly, and re-signs on retries.
- **Digest:** constant-time comparison of hex digests.

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

All seven fields are required: the Go SDK rejects a payload with any empty field
("invalid push payload"). Upstream's schema is `GitPushEvent` in the vendored
[OpenAPI spec](../upstream/openapi.yaml) (top-level `webhooks`).

### `repo.sync.started` / `repo.sync.succeeded` / `repo.sync.failed`

Payloads carry `type`, repo identity, and sync metadata. Exact field tables:
[`../upstream/guides/webhooks.md`](../upstream/guides/webhooks.md). Upstream's schemas are
`GitSyncStartedEvent`, `GitSyncSucceededEvent` and `GitSyncFailedEvent` in the vendored
[OpenAPI spec](../upstream/openapi.yaml). open.storage does not send their optional
`lifecycle_id` ([#354](https://github.com/coder/openstorage/issues/354)).

`repo.sync.failed` adds `error`, one of:

- `"authentication failed"`
- `"failed to clone repository"`
- `"failed to push to storage"`
- `"repository configuration error"`
- `"upstream repository unreachable"`
- `"internal error"`
- `"session unavailable"`
- `"sync failed"`

**Push direction** (open.storage extension; see
[Upstream sync](http-api.md#upstream-sync)). Outbound push-upstream runs reuse the same
three event types:

| Field | Meaning |
|---|---|
| `direction: "push"` | Present on push runs only. Inbound pull payloads are unchanged on the wire (no `direction` field), so absence means pull |
| `forced: true` | Optional; the run used force refspecs |
| `rejected_refs: [{ref, reason}]` | On `repo.sync.failed` with `error: "push rejected by upstream"`: each ref the upstream refused (e.g. `non-fast-forward`). The forward is atomic — one refused ref fails the whole batch and leaves the upstream untouched — so the array also lists innocent refs with reason `atomic push failed` |
| `error` | The strings above, plus `"failed to push to upstream"` (generic outbound push failure) and `"push rejected by upstream"` |
| `run_count` / `is_first_sync` | Count outbound runs independently of inbound ones |

Upstream's sync schemas set `additionalProperties: false`, so the `direction`, `forced` and
`rejected_refs` fields are a
[recorded deviation](../explanation/wire-compatibility.md#known-deviations-from-the-vendored-spec).

### Unknown events

SDKs pass unrecognized `X-Pierre-Event` values through as `{type, raw}`, so new event
types are safe to add.

## Implementation guidance

- Emit events after the ref update commits atomically, via a queue with at-least-once
  delivery.
- Receivers use timestamp checks, not idempotency keys: keep retry windows short or
  re-sign.
- Ephemeral-namespace updates do not emit `push` events
  ([domain model](../explanation/domain-model.md), invariant 7).
