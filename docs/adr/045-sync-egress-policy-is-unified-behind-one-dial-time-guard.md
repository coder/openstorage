# ADR-45: Sync egress policy is unified behind one dial-time guard

> Decision record for issue #139 (unify sync egress policy: extend the
> dial-time destination guard to the inbound pull lane). Context:
> [ADR-44](044-outbound-push-forwarding-rides-a-separate-push-claim-lane.md)
> gave the NEW outbound push lane a dial-time egress guard after a Codex
> security finding; the released inbound pull lane still fetched from
> `upstream_host` with no destination policy — the same SSRF class, weaker
> primitive (server-side reads plus stored-credential echo to the linked
> host, vs. publishing attacker refs with force).

**Both directions dial through the same vetting proxy.** Every http(s)
sync transfer — inbound pull fetches (`POST /repos/pull-upstream`, the
first sync at `POST /repos`, freshness pokes) and outbound push forwards —
runs behind the short-lived loopback proxy from ADR-44, whose dials go
through the webhook `TargetGuard.DialContext` (issue #53): deny
loopback/private/link-local/ULA/metadata/special-purpose destinations,
re-resolving and vetting every DNS answer at connect time (no rebinding
TOCTOU). The proxy's recorded verdicts outrank git's stderr phrasing: a
policy denial classifies as permanent configuration
(`repository configuration error` on pull, `invalid sync configuration` on
push), a non-policy dial failure as the transient
`upstream repository unreachable`.

**One knob, tri-state, compatibility-first.** `CODESTORAGE_SYNC_ALLOW_PRIVATE`
governs BOTH directions: `true` admits the private receiver classes
(internal forges), `false` is strict, and **unset is the transitional
default** — pushes stay strict while pulls ALLOW private upstreams with a
boot warning announcing the future flip of that default to deny. Syncing
from internal RFC 1918 gitea/forgejo/GitLab forges is a mainstream released
0.6.x deployment shape; default-denying it is a breaking change that gets a
deprecation window, not a surprise. `CODESTORAGE_SYNC_PUSH_ALLOW_PRIVATE`
becomes the deprecated push-only spelling: still honored (its historical
meaning, push only) when the unified knob is unset, ignored with a boot
notice when it is set. One knob was chosen over per-direction flags because
the threat and the deployment shape are symmetric — an internal-forge
deployment wants both directions private, a hardened deployment wants
neither — and a second per-direction flag would double the migration story
for no expressible policy anyone asked for. Malformed booleans in either
spelling are boot errors.

**Ambient proxies no longer apply to sync transfers.** The pull fetch
previously inherited `HTTP_PROXY`/`HTTPS_PROXY` from the environment; the
vetting proxy now replaces them (validating a proxy socket would not
validate the destination the proxy later connects to — the same reasoning
as webhook delivery and ADR-44's push lane). Operators that require proxied
egress must enforce an equivalent destination policy at that proxy. This is
the one deliberate pull-lane behavior change shipped without a transition:
chaining the guard through an operator proxy would reopen the exact
policy-bypass hole the guard exists to close.

**Registration-time courtesy check.** `POST /repos` with `base_repo`
rejects an `upstream_host` whose literal or currently-resolved addresses
the effective pull policy denies (400, before any rows are created),
mirroring webhook `CheckRegistration`: fast feedback at link time, fail-open
on resolution errors, never a substitute for dial-time enforcement. Under
the transitional default this already rejects metadata/special-purpose
targets while admitting private forges.

**Migration plan (release-notes language).** This release: pull fetches
gain the dial-time guard with private targets still allowed by default;
`serve` warns at boot until `CODESTORAGE_SYNC_ALLOW_PRIVATE` is set
explicitly. Set `true` to keep internal-forge sync across the future flip,
`false` to adopt strict mode early. A future minor release flips the unset
pull default to deny; the deprecated push-only spelling is removed no
earlier than that flip.
