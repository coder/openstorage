# ADR-41: Registered LFS objects live at a canonical key legacy presigned URLs cannot address

> Decision record for issue #51, PR #61 (Codex security round 78).

`blob.LFSKey(repoID, oid) = lfs/<repoID>/o2/<oid>` is the **canonical** LFS
object key: only the server's digest-verified paths write it (verified proxy
uploads, adoption copies, registered-object restores), and only registered
objects are ever served from it. `blob.LFSLegacyKey(repoID, oid) =
lfs/<repoID>/<oid>` is the v0.1.x spelling — the only one old presigned
upload URLs can address. It is never registered: it exists as the adoption
*source* (rowless pre-#51 blobs live there until adopted, after which the
source is deleted) and as the read-compat location for not-yet-adopted
rowless objects.

The forcing scenario is a rolling upgrade: v0.1.x issued presigned PUT URLs
for the shared single-key spelling with a 15-minute expiry, and a presigned
URL cannot be revoked. With one shared mutable key, a still-valid URL could
overwrite a *registered* object **after** adoption verified and registered
it — readers would then be served forged bytes under a trusted OID, with
quota still charged at the old size and the sweeper treating the row as
live. No registration-time check can defend a key that stays writable after
the check, and "wait for old URLs to expire" is unimplementable because URL
issuance times on old replicas are unobservable. Moving verified content to
a spelling those URLs cannot reach closes the window by construction.

Adoption therefore *copies* the legacy bytes to the canonical key —
server-side, under a freshly renewed fleet adoption lease, with a
stale-floor cover reservation for the transient double-count — then
verifies and registers the copy. Rowless canonical bytes (a mid-adoption,
not-yet-verified copy) are never served: both download surfaces gate the
canonical key on the registry row and fall back to the legacy spelling for
rowless objects, preserving exactly the read surface v0.1.x offered. A
registered object whose canonical bytes went missing is repaired only by a
digest-verified re-upload (the zero-growth path), never by re-adopting a
legacy source that remains forgeable. ADR-35 (base-RepoID keying, shared
namespace object space) is unchanged; both spellings stay under
`lfs/<repoID>/`, so usage floors and the sweeper cover them uniformly.

v0.2.0 shipped before this split, registering rows at the legacy
spelling. Registration migrates such a row in place — transactionally,
zero-growth — on the object's first write-scoped touch under the upgraded
binary, so neither the usage counter nor the object-count cap
double-charges v0.2.0-era objects; until migrated they read from the
legacy blob exactly as v0.2.0 served them. Rolling-upgrade races that
strand a legacy row alongside the canonical one are reconciled by
registration and by the sweeper's janitor, refunding the double-charge.
Adopted legacy sources are retained (live presigned GETs and mid-roll old
replicas may still read them); `OPENSTORAGE_LFS_V020_WRITE_COMPAT`
additionally dual-publishes fresh uploads to the legacy spelling so
not-yet-upgraded replicas keep serving LFS reads until the fleet finishes
rolling. Reclaiming legacy-spelled keys is a separate post-roll operator
action (`OPENSTORAGE_SWEEP_DELETE_LFS_LEGACY`, Codex round 81): the
transition gate dominates every sweep flag, so no mode deletes a legacy
key while old replicas may still be reading it. Adoption registration is
likewise bound to a digest re-verification of the CANONICAL destination
bytes — never to source fingerprints, which content-derived ETags make
ABA-blind under a still-valid legacy PUT.
