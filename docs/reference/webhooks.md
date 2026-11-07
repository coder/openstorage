# Outbound Webhooks

The backend sends HTTP POST notifications to customer-configured endpoints. This page
covers endpoint management, delivery headers, signatures, and event payloads. Delivery
egress rules: [`OPENSTORAGE_WEBHOOK_ALLOW_PRIVATE`](configuration.md#openstorage_webhook_allow_private).

## Endpoint management

The public SDKs do not manage endpoints; upstream uses its dashboard/control plane.
open.storage adds its own org-level routes. All require `repo:write` with `repo: "org"` (or
no `repo` claim) and return the plain `{"error"}` envelope on failure.

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
                                                 # open.storage extension: repo.push_upstream.started | .succeeded | .failed
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
  "type": "push",
  "repository": {"id": "<repo_id>", "url": "<repo url>", "repo_name": "<repo url>"},
  "ref": "refs/heads/main",
  "before": "<40-hex, all zeros for ref creation>",
  "after": "<40-hex, all zeros for ref deletion>",
  "customer_id": "<org name>",
  "org": "<org name>",
  "pushed_at": "2026-01-20T10:30:00Z"
}
```

Every field is always sent and never empty: the Go SDK rejects a payload with an empty
field ("invalid push payload"). Upstream's schema is `GitPushEvent` in the vendored
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

These three events cover inbound (pull) runs only, and their bodies carry only the fields
of upstream's schemas.

### `repo.push_upstream.started` / `repo.push_upstream.succeeded` / `repo.push_upstream.failed`

open.storage extension for outbound push-upstream runs (see
[Upstream sync](http-api.md#upstream-sync)). Upstream has no outbound-run event. A run
sends `repo.push_upstream.started` once its loopback fetch has the snapshot it forwards,
before it pushes to the upstream. A run that fails before or at that fetch sends
`started` right before `failed`, with these exceptions. A run that a write in progress
holds back sends no event. A run that `DELETE /repos/base` or a
server shutdown cancels, or whose claim another worker took over, ends with no
`succeeded` or `failed`, even when it sent `started`. "Events and cancellation" under
[Upstream sync](http-api.md#upstream-sync) has more on held-back and cancelled runs. The body
has the `repo.sync.*` fields, with `type` set to the event name, plus:

| Field | Meaning |
|---|---|
| `direction: "push"` | Always present. It predates the separate event names and is kept so receivers that check it keep working |
| `forced: true` | Optional; the run used force refspecs |
| `rejected_refs: [{ref, reason}]` | On `repo.push_upstream.failed` with `error: "push rejected by upstream"`: each ref the upstream refused (e.g. `non-fast-forward`). The forward is atomic — one refused ref fails the whole batch and leaves the upstream untouched — so the array also lists innocent refs with reason `atomic push failed` |
| `error` | The strings above, plus `"failed to push to upstream"` (generic outbound push failure) and `"push rejected by upstream"` |
| `run_count` / `is_first_sync` | Count outbound runs independently of inbound ones |

> [!IMPORTANT]
> **Migration ([#354](https://github.com/coder/openstorage/issues/354)):** earlier releases
> sent outbound runs as `repo.sync.*` with `direction: "push"`, which upstream's schemas
> reject (`additionalProperties: false`). Receivers that watched `repo.sync.*` for
> `direction: "push"` must handle `repo.push_upstream.*` instead. The body is unchanged apart
> from `type`. During a rolling upgrade, replicas that have not upgraded yet still send the
> old names. Deliveries queued before the upgrade keep the event name and body they were
> queued with.

### Fields on every event: `org` and `repository.repo_name`

Every event body (`push`, `repo.sync.*` and `repo.push_upstream.*`) carries:

| Field | Value |
|---|---|
| `org` | The served org name (`OPENSTORAGE_ORG`). Upstream defines `org` as the `iss` claim of the org's JWTs, and open.storage accepts no other `iss` |
| `repository.repo_name` | The same value as `repository.url`, for example `team/project` |

Upstream deprecates `repository.url` ("use repo_name") and the push event's `customer_id`
("use org"). open.storage still sends both, unchanged: `customer_id` carries the org name, like
`org`. Deliveries queued before an upgrade keep the body they were queued with, and replicas
that have not upgraded yet send the old bodies, so a delivery can lack `org`, `repo_name` and
the push `type`. Upstream warns about the same gap on older deliveries.

### Unknown events

SDKs pass unrecognized `X-Pierre-Event` values through as `{type, raw}`, so new event
types are safe to add. Receivers should answer 2xx for event types they ignore. A non-2xx
answer makes the backend retry the delivery and then discard it.

## Implementation guidance

- Emit events after the ref update commits atomically, via a queue with at-least-once
  delivery.
- Receivers use timestamp checks, not idempotency keys: keep retry windows short or
  re-sign.
- Ephemeral-namespace updates do not emit `push` events
  ([domain model](../explanation/domain-model.md), invariant 7).
