# ADR-3: Namespace mapping via separate ref stores

> Extracted verbatim from [`08-spike-implementation-notes.md`](../archive/phases/08-spike-implementation-notes.md)
> (“Spike Implementation Notes & ADRs”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.
>
> **Amended by [#804](https://github.com/coder/openstorage/issues/804):** the `+import`
> remote no longer has its own storage key. It is push-only, and an import push writes the
> base repository's refs ([import remote](../explanation/git-protocol-and-namespaces.md#import-remote)).
> Data that 0.20.0 and earlier stored under `{repo}+import` stays in storage.

**Context.** docs/05 requires that `+ephemeral` remotes see/update only the ephemeral ref
space and persistent remotes never see ephemeral refs, and explicitly allows either git
namespaces or separate ref stores.

**Decision.** The full namespaced name (`{repo}+ephemeral`, `{repo}+import`) is the storage
key *everywhere*: PG ref rows, S3 pack prefix, and cache directory. Authentication strips
the suffix — the JWT `repo` claim carries the base repo id and is valid for its namespaces.

**Consequences.** Isolation comes for free: there is no refspec-mapping layer and no way
for ephemeral refs to leak into a persistent advertisement. The trade-off is that
namespaces do not share objects, so ephemeral→persistent promotion is a copy, not O(1).
This is a spike-local choice; the roadmap's open item (ref-prefix namespace for O(1)
promotion) remains open for the real backend.
