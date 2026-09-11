#!/usr/bin/env bash
# Adapter only: the acceptance runner owns scenarios and result evidence.
set -euo pipefail
[[ ${OPENSTORAGE_AWS_SMOKE:-} == 1 ]] || {
  echo 'Set OPENSTORAGE_AWS_SMOKE=1 to test an already-approved, deployed reference.' >&2
  exit 2
}
[[ $# -ge 2 ]] || { echo 'usage: smoke.sh <local-signing-key.pem> <runner> [runner-args...]' >&2; exit 2; }
key=$(realpath "$1")
[[ -f "$key" && -r "$key" ]] || { echo 'Signing key is not readable.' >&2; exit 2; }
shift
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)
umask 077
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
# output performs no apply/provisioning. Use the exact user-facing deployment.
terraform -chdir="$root" output -json deployment > "$work/deployment.json"
# The runner must retain its non-secret result manifest outside this temporary directory.
"$@" --deployment "$work/deployment.json" --signing-key "$key"
