# Floci E2E spike (issue #201) — THROWAWAY branch

Investigation artifacts for extending Floci coverage beyond the nightly
blob-endpoint lane. Nothing here is a CI change; the full findings live in
the issue #201 comment. Contents:

- `floci-githttp-lane.sh` — prototype of the highest-value viable lane:
  full PG-mode githttp E2E with the blob store on Floci, bootstrapped via
  IAM CreateAccessKey → STS AssumeRole session credentials.
- `../internal/ref/s3wal/floci_spike_test.go` — spike-only probes:
  external-endpoint contention detector, torn-read classifier,
  DeleteObject+If-Match matrix probes (external/SeaweedFS/legacy MinIO).
- `../internal/githttp/e2e_test.go` — spike-only
  `CODESTORAGE_TEST_S3_ENDPOINT` override for the PG lane's S3 endpoint.

## Repro commands (Floci 1.7.0; `nightly-08282026` behaves identically)

```sh
# Boot + IAM key (mirrors floci-nightly.yml)
docker run -d --name floci -p 14566:4566 \
  -e FLOCI_SERVICES_S3_ENFORCE_AUTH=true floci/floci:1.7.0
export AWS_ACCESS_KEY_ID=root AWS_SECRET_ACCESS_KEY=root AWS_DEFAULT_REGION=us-east-1
aws --endpoint-url http://127.0.0.1:14566 iam create-user --user-name spike
aws --endpoint-url http://127.0.0.1:14566 iam create-access-key --user-name spike
# export the returned key pair, then:
aws --endpoint-url http://127.0.0.1:14566 s3api create-bucket --bucket s3wal-qualify

# 1. s3wal qualification suite (22 surfaces, require mode): ALL PASS
CODESTORAGE_TEST_S3WAL_URL='s3://s3wal-qualify?endpoint=http://127.0.0.1:14566&region=us-east-1&s3ForcePathStyle=true' \
  go test ./internal/ref/s3wal -run TestQualifyExternalEndpoint -count=1 -v

# 2. Concurrent-writer CAS detector: zero lost updates, but GETs racing
#    overwrites fail SDK CRC32 validation (torn responses)
CODESTORAGE_TEST_S3WAL_URL=... go test ./internal/ref/s3wal \
  -run TestSpikeContentionExternalEndpoint -count=1 -v

# 3. Torn-read classifier: verdict=torn-body + HTTP framing corruption
#    ("Unsolicited response received on idle HTTP channel")
CODESTORAGE_TEST_S3WAL_URL=... go test ./internal/ref/s3wal \
  -run TestSpikeTornReadClassifier -count=1 -v

# 4. DeleteObject+If-Match: Floci ignores it (deletes on mismatch);
#    SeaweedFS 4.42 is AWS-compliant; legacy MinIO ignores it
CODESTORAGE_TEST_S3WAL_URL=... go test ./internal/ref/s3wal \
  -run 'TestSpikeDeleteIfMatch.*' -count=1 -v

# 5. Full PG-mode githttp E2E on Floci (see floci-githttp-lane.sh for the
#    STS-credential variant): PASS, ~9.5 min wall with -race
CODESTORAGE_TEST_S3_ENDPOINT=http://127.0.0.1:14566 \
  go test ./internal/githttp -count=1 -race -timeout=25m

# 6. Gap re-checks (both still open on 1.7.0 and nightly-08282026):
#    IAM Deny on s3:GetObject is not evaluated; wrong
#    --expected-bucket-owner is accepted; header-auth SigV4 signature is
#    not validated (wrong secret accepted; unknown key IDs are rejected;
#    presigned-URL signatures ARE validated)
aws --endpoint-url http://127.0.0.1:14566 s3api head-object \
  --bucket s3wal-qualify --key some-key --expected-bucket-owner 111111111111  # succeeds; AWS would 403
```
