# ADR-29: Webhook endpoint management is a self-defined CRUD surface

> Extracted verbatim from [`14-phase5-implementation-notes.md`](../archive/phases/14-phase5-implementation-notes.md)
> (“Phase-5 Implementation Notes & ADRs (eventing, upstream sync, credentials, fork)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

docs/06 specifies delivery (headers, HMAC signing, retries) but upstream manages webhook
*registration* in its closed dashboard/control-plane — there is no SDK surface to be
compatible with. Decision: **the OSS backend defines its own org-scoped CRUD routes**
(`POST/GET /api/v1/webhooks`, `DELETE /api/v1/webhooks/{id}`), following the
git-credentials precedent: org-wide tokens with `repo:write`. The signing secret is
returned exactly once — from `POST`, server-generated when the body omits it — and `GET`
never echoes secrets.
