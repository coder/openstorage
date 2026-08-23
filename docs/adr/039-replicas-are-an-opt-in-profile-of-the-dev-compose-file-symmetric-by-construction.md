# ADR-39: Replicas are an opt-in profile of the dev compose file, symmetric by construction

> Extracted verbatim from [`16-phase7-implementation-notes.md`](../archive/phases/16-phase7-implementation-notes.md)
> (“Phase-7 Implementation Notes & ADRs (deployment packaging + p6 burn-down)”). Historical decision record — later issues and PRs may have
> superseded details; the git history of the source file is authoritative.

Decision: **the two-node deployment lives in the existing `docker-compose.yml` as a
`replicas` profile — `node-a`/`node-b` defined via YAML anchors, differing only in host
port mapping and the `CODESTORAGE_BASE_URL` that embeds it.**

- **One compose file, not a second deployment file.** The nodes reuse the same
  `postgres`/`minio` service definitions the dev quickstart boots; a parallel file
  would drift. `profiles: ["replicas"]` keeps the default `docker compose up`
  behavior unchanged (backing services only).
- **Symmetry is load-bearing.** An earlier draft serialized `node-b` behind
  `node-a` (`depends_on: service_healthy`) to dodge a fresh-boot migration race.
  That workaround was dropped and the race fixed at the root (ADR-40): the profile
  exists to *prove* multi-node statelessness, and an asymmetric boot order would
  mask exactly the class of races it is supposed to demonstrate absent.
- **Healthcheck is a TCP probe** (`nc -z`, present in the alpine runtime): the server
  has no unauthenticated HTTP endpoint, and inventing one just for compose would widen
  the surface. A listener probe is sufficient for `up --wait`.
- **Keys stay on the host.** The org keypair is generated on the host
  (`codestorage keygen --out ./compose-keys/cs-key`) and the public key is mounted
  read-only into the nodes; `/compose-keys/` is gitignored. Runtime keys are never
  baked into the image or committed — the standing key policy from the quickstarts.
