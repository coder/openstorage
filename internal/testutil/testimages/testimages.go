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
	Postgres = "postgres:17-alpine@sha256:b0f9560a2de083e2cc7382e75f808c7381a32852a7ec49117deedb300e552b24"
	// SeaweedFS is chrislusf/seaweedfs:4.48, the self-hosted store of record.
	SeaweedFS = "chrislusf/seaweedfs:4.48@sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d"
)
