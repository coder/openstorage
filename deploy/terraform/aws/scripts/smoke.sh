#!/usr/bin/env bash
# Adapter only: the acceptance runner owns scenarios and result evidence.
set -euo pipefail
[[ ${OPENSTORAGE_AWS_SMOKE:-} == 1 ]] || {
  echo 'Set OPENSTORAGE_AWS_SMOKE=1 to test an already-approved, deployed reference.' >&2
  exit 2
}
[[ $# -ge 3 ]] || { echo 'usage: smoke.sh <local-inputs.json> <new-results-dir> <runner> [runner-args...]' >&2; exit 2; }
inputs=$(realpath "$1")
output=$(realpath "$2")
shift 2
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
umask 077
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
# output performs no apply/provisioning. Use the exact user-facing deployment.
terraform -chdir="$root" output -json deployment > "$work/deployment.json"
python3 - "$inputs" "$work" <<'PY'
import json
from pathlib import Path
import ssl
import sys

inputs = json.loads(Path(sys.argv[1]).read_text())
work = Path(sys.argv[2])
deployment = json.loads((work / "deployment.json").read_text())
if set(inputs) != {"candidate_commit", "binary", "binary_sha256", "private_key_file"}:
    sys.exit("Local inputs must contain only candidate_commit, binary, binary_sha256, private_key_file.")
if deployment.get("schema_version") != 1 or deployment.get("topology") != "postgres-s3-single-node":
    sys.exit("Unsupported Terraform deployment schema/topology.")
roots = ssl.get_default_verify_paths().cafile
if not roots:
    sys.exit("Install your OS CA certificate bundle before running smoke tests.")
# Keep public AWS trust for presigned S3 transfers, alongside the private ALB CA.
ca = work / "ca.pem"
ca.write_text(Path(roots).read_text() + "\n" + deployment["tls_ca_certificate_pem"])
for field in ("binary", "private_key_file"):
    inputs[field] = str(Path(inputs[field]).resolve())
config = dict(inputs, schema_version=1, topology="pg-s3-single-node", replica_urls=[],
              endpoint=deployment["service_url"], org=deployment["org"], ca_file=str(ca),
              aws={"region": deployment["region"], "bucket": deployment["bucket"],
                   "expected_bucket_owner": deployment["account_id"]})
(work / "config.json").write_text(json.dumps(config))
PY
echo 'Requires a compatible #222 runner (pg-s3-single-node + ca_file); do not spoof local or duplicate replicas.' >&2
"$@" --config "$work/config.json" --output "$output"
