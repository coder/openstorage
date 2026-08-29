#!/usr/bin/env bash
# Prototype harness for issue #201 (THROWAWAY spike branch — not a CI change):
# the highest-value viable Floci lane found by the spike — the full PG-mode
# githttp E2E suite with its blob store pointed at Floci, bootstrapped
# through a REAL credential flow (IAM CreateAccessKey → STS AssumeRole →
# session credentials), which no current lane covers.
#
# Requires: docker, aws CLI, jq, go. Runs entirely on localhost.
# Usage: ./spike/floci-githttp-lane.sh [floci-image-tag]   (default 1.7.0)
set -euo pipefail

TAG="${1:-1.7.0}"
PORT=34566
NAME=floci-githttp-lane

cleanup() { docker rm -f "$NAME" >/dev/null 2>&1 || true; }
trap cleanup EXIT
cleanup

echo "=== boot floci/floci:$TAG (S3 auth enforcement on) ==="
t0=$(date +%s)
docker run -d --name "$NAME" -p "$PORT:4566" \
  -e FLOCI_SERVICES_S3_ENFORCE_AUTH=true "floci/floci:$TAG" >/dev/null

export AWS_DEFAULT_REGION=us-east-1
ep="http://127.0.0.1:$PORT"

echo "=== IAM: user + access key + role; STS: assume-role ==="
# IAM control plane accepts any credentials; only the S3 data plane
# enforces auth, and it honors only keys minted through IAM/STS.
export AWS_ACCESS_KEY_ID=root AWS_SECRET_ACCESS_KEY=root
until aws --endpoint-url "$ep" iam create-user --user-name lane >/dev/null 2>&1; do sleep 0.2; done
key_json=$(aws --endpoint-url "$ep" iam create-access-key --user-name lane --output json)
ak=$(jq -r '.AccessKey.AccessKeyId // empty' <<<"$key_json")
sk=$(jq -r '.AccessKey.SecretAccessKey // empty' <<<"$key_json")
# The image tag is mutable: full-string-validate before exporting (same
# defense as floci-nightly.yml).
[[ "$ak" =~ ^[A-Z0-9]{16,32}$ ]] && [[ "$sk" =~ ^[A-Za-z0-9/+=]{20,64}$ ]] \
  || { echo "malformed IAM credentials" >&2; exit 1; }
aws --endpoint-url "$ep" iam create-role --role-name lane-role \
  --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"AWS":"arn:aws:iam::000000000000:user/lane"},"Action":"sts:AssumeRole"}]}' >/dev/null
sts_json=$(AWS_ACCESS_KEY_ID="$ak" AWS_SECRET_ACCESS_KEY="$sk" \
  aws --endpoint-url "$ep" sts assume-role \
  --role-arn arn:aws:iam::000000000000:role/lane-role \
  --role-session-name githttp-e2e --output json)
export AWS_ACCESS_KEY_ID=$(jq -r .Credentials.AccessKeyId <<<"$sts_json")
export AWS_SECRET_ACCESS_KEY=$(jq -r .Credentials.SecretAccessKey <<<"$sts_json")
export AWS_SESSION_TOKEN=$(jq -r .Credentials.SessionToken <<<"$sts_json")
t1=$(date +%s)
echo "bootstrap (boot + IAM + STS): $((t1-t0))s; session creds: $AWS_ACCESS_KEY_ID"

echo "=== githttp E2E (PG mode, blob store = floci, session creds) ==="
export AWS_REGION=us-east-1
export CODESTORAGE_TEST_S3_ENDPOINT="$ep"
t2=$(date +%s)
rc=0
go test ./internal/githttp -count=1 -race -timeout=25m || rc=$?
t3=$(date +%s)
echo "suite: $((t3-t2))s rc=$rc"
docker stats --no-stream --format 'floci footprint after suite: mem={{.MemUsage}}' "$NAME"
exit $rc
