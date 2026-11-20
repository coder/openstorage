// Package testimages pins the container images that tests, fixtures and CI
// lanes pull.
//
// Each pin is an OCI image index digest (all platforms), so every host runs
// the same bytes even after upstream moves the tag. The tag stays in front
// of the digest for readability only: Docker resolves by digest and ignores
// the tag. To bump a pin, resolve the new index digest with
// `docker buildx imagetools inspect <image>:<tag>` and update every copy
// that the pins table in testimages_test.go names to the same digest.
// TestDigestCopiesMatch fails until every copy matches.
package testimages

const (
	// Postgres is postgres:17-alpine, which was PostgreSQL 17.12 when pinned.
	Postgres = "postgres:17-alpine@sha256:d3244e8ce3f165ee3c93d9c4a4b1d0fe48b3a89e2287b717fdcbbde92d405065"
	// SeaweedFS is chrislusf/seaweedfs:4.54, the self-hosted store of record.
	SeaweedFS = "chrislusf/seaweedfs:4.54@sha256:4b42c212543a0acd4c0e675215ecdcaf62a186ef98def76c7e12701871222d15"
	// Floci is floci/floci:2.5.0, the second S3 implementation that the
	// Floci nightlies and TestQualifyFloci run against.
	Floci = "floci/floci:2.5.0@sha256:a0fbf944d7f338cc0856f1ef83831ea749ff2be71956eb7a7fce08fc21458e19"
	// AmazonLinux is amazonlinux:2023, release 2023.12.20261117.0 when
	// pinned. Only the AWS bootstrap Dockerfile.test builds on it.
	AmazonLinux = "amazonlinux:2023@sha256:9cf762f5099b606bcd851391317af7cd75b11363c4816ebcb5e91292dd5736a7"
)
