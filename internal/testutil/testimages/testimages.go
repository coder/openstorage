// Package testimages pins the container images that tests and fixtures pull.
//
// Each pin is an OCI image index digest (all platforms), so every host runs
// the same bytes even after upstream moves the tag. The tag stays in front
// of the digest for readability only: Docker resolves by digest and ignores
// the tag. To bump a pin, resolve the new index digest with
// `docker buildx imagetools inspect <image>:<tag>` and update
// docker-compose.yml, README.md and CONTRIBUTING.md to the same digest.
// TestDigestCopiesMatch fails until every copy matches.
package testimages

const (
	// Postgres is postgres:17-alpine, which was PostgreSQL 17.11 when pinned.
	Postgres = "postgres:17-alpine@sha256:eee6c6e1b0894fa4fe0075eb054af9fd1cf39a35321c2d4110626cfe506411fb"
	// SeaweedFS is chrislusf/seaweedfs:4.53, the self-hosted store of record.
	SeaweedFS = "chrislusf/seaweedfs:4.53@sha256:ccad23e18639c8b98cee17e7100b95830a06077b56d8e5b006d6dcf8ec5e94a0"
)
