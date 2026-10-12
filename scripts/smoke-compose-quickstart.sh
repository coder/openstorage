#!/usr/bin/env bash
# Verification of the unchanged reference config, never a registry push or pull.
set +x
set -Eeuo pipefail
umask 077
: "${OPENSTORAGE_IMAGE:?Export a local image ID or an image reference already present locally}"
for tool in docker python3 curl git; do command -v "$tool" >/dev/null; done
# The image must already be local, so Compose's pull_policy: missing never pulls.
image_id=$(docker image inspect --format '{{.Id}}' "$OPENSTORAGE_IMAGE")
[[ $image_id =~ ^sha256:[a-f0-9]{64}$ ]]
git lfs version
root=$(cd "$(dirname "$0")/.." && pwd)
work=$(mktemp -d)
project="openstorage-smoke-$(date +%s)-$$"
main_project=$project
bad_project="$project-invalid"
# Ignore host Git configuration and tracing; credentials never enter remote URLs.
for var in ${!GIT_@}; do unset "$var"; done
export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 GIT_TERMINAL_PROMPT=0
export TOKEN_FILE="$work/token" GIT_ASKPASS="$work/askpass"
export OPENSTORAGE_PORT=${OPENSTORAGE_PORT:-$(python3 - <<'PY'
import socket
with socket.socket() as s:
    s.bind(("127.0.0.1", 0))
    print(s.getsockname()[1])
PY
)}
base="http://127.0.0.1:$OPENSTORAGE_PORT"
compose() { docker compose -f "$root/compose.quickstart.yml" -p "$project" "$@"; }
assert_deleted() {
    local resources
    resources=$(docker ps -aq --filter "label=com.docker.compose.project=$project") || return 1
    [[ -z $resources ]] || return 1
    resources=$(docker volume ls -q --filter "label=com.docker.compose.project=$project") || return 1
    [[ -z $resources ]] || return 1
    resources=$(docker network ls -q --filter "label=com.docker.compose.project=$project") || return 1
    [[ -z $resources ]]
}
destroy() {
    compose --profile tools down --volumes --remove-orphans >>"$work/compose.log" 2>&1 || return 1
    assert_deleted
}
cleanup() {
    rc=$?
    trap - EXIT
    for project in "$main_project" "$bad_project"; do
        destroy || rc=1
    done
    rm -rf "$work"
    echo "Quickstart smoke cleanup complete; exit=$rc"
    exit "$rc"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'echo "Quickstart smoke failed at line $LINENO (credentials and raw responses withheld)" >&2' ERR

# Test the contract before starting containers, including the absent-image guard.
if OPENSTORAGE_IMAGE='' compose config -q >"$work/missing-image.log" 2>&1; then
    echo 'Missing OPENSTORAGE_IMAGE was accepted' >&2
    exit 1
fi
compose --profile tools config --format json >"$work/config.json"
python3 - "$work/config.json" "$OPENSTORAGE_IMAGE" "$OPENSTORAGE_PORT" <<'PY'
import json, sys
c = json.load(open(sys.argv[1]))
s = c["services"]
assert set(s) == {"init", "server", "cli"}
assert set(c["volumes"]) == {"private", "public", "data"}
assert all(v["image"] == sys.argv[2] for v in s.values())
assert all(v["pull_policy"] == "missing" for v in s.values())
assert s["server"]["user"] == "openstorage"
assert s["server"]["depends_on"]["init"]["condition"] == "service_completed_successfully"
p, = s["server"]["ports"]
assert (p["host_ip"], p["published"], p["target"]) == ("127.0.0.1", sys.argv[3], 8080)
assert s["server"]["environment"]["OPENSTORAGE_BASE_URL"] == "http://127.0.0.1:" + sys.argv[3]
assert {v["source"]: v.get("read_only", False) for v in s["server"]["volumes"]} == {"public": True, "data": False}
assert {v["source"]: v.get("read_only", False) for v in s["cli"]["volumes"]} == {"private": True}
assert s["cli"]["profiles"] == ["tools"]
assert all(v.get("restart", "no") == "no" for v in s.values())
PY

init_shell() { compose run --rm --no-deps -T --entrypoint /bin/sh init -ec "$1"; }
snapshot() {
    init_shell 'find /private /public -mindepth 1 -maxdepth 1 -exec stat -c "%n %F %a %u %g %s %y" {} \; ; find /private /public -type f -exec sha256sum {} \;' | sort
}
up() { compose up -d --wait --wait-timeout 60 >>"$work/compose.log" 2>&1; }
up
server=$(compose ps -q server)
docker inspect "$server" >"$work/server.json"
python3 - "$work/server.json" "$project" "$image_id" <<'PY'
import json, sys
s, = json.load(open(sys.argv[1]))
assert s["Image"] == sys.argv[3]
assert s["Config"]["User"] == "openstorage"
assert s["State"]["Health"]["Status"] == "healthy"
assert {m["Name"]: m["RW"] for m in s["Mounts"]} == {sys.argv[2] + "_public": False, sys.argv[2] + "_data": True}
PY
[[ $(compose ps --status running --services) == server ]]
# Expand these checks inside the containers, not in the host shell.
# shellcheck disable=SC2016
compose exec -T server sh -ec 'test "$(id -u)" != 0; test ! -e /private; test -w /data; test -r /public/org.pub.pem'
{
    # shellcheck disable=SC2016
    init_shell 'test "$(stat -c %a /private)" = 700; test "$(stat -c %a /private/org.pem)" = 600; test "$(stat -c %a /public/org.pub.pem)" = 644; cmp /private/org.pub.pem /public/org.pub.pem; test "$(ls -A /public)" = org.pub.pem'
    snapshot >"$work/keys.before"
    compose run --rm --no-deps -T init
    snapshot >"$work/keys.repeat"
} >>"$work/compose.log" 2>&1
cmp "$work/keys.before" "$work/keys.repeat"
echo 'PASS fresh init, nonroot isolation, restrictive modes, byte/mtime-stable repeat'

compose run --rm --no-deps -T cli mint --key /private/org.pem --iss selfhost \
    --repo smoke --scopes repo:write,git:read,git:write --ttl 1h >"$TOKEN_FILE" 2>>"$work/compose.log"
printf 'Authorization: Bearer %s\n' "$(<"$TOKEN_FILE")" >"$work/auth"
cat >"$GIT_ASKPASS" <<'SH'
#!/bin/sh
case "$1" in
    *Username*) printf '%s\n' t ;;
    *Password*) cat "$TOKEN_FILE" ;;
    *) exit 1 ;;
esac
SH
chmod 700 "$GIT_ASKPASS"
api() { curl -q --silent --show-error --fail --max-time 30 -H "@$work/auth" "$@"; }
[[ $(curl -q -sS --max-time 30 -o /dev/null -w '%{http_code}' "$base/api/v1/repo") == 401 ]]
[[ $(curl -q -sS --max-time 30 -o /dev/null -w '%{http_code}' "$base/smoke.git/info/refs?service=git-upload-pack") == 401 ]]
api -X POST "$base/api/v1/repos" -H 'Content-Type: application/json' \
    -d '{"default_branch":"main"}' >"$work/create.json"
clone_url=$(python3 - "$work/create.json" "$base" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
assert r["repo_id"] and r["repo_name"] == "smoke"
assert r["http_url"] == sys.argv[2] + "/" + r["repo_id"] + ".git"
print(r["http_url"])
PY
)
api "$base/api/v1/repo" >"$work/repo.before"
git init -q -b main "$work/source"
git -C "$work/source" config credential.helper ''
git -C "$work/source" lfs install --local >"$work/git.log" 2>&1
git -C "$work/source" lfs track '*.bin' >>"$work/git.log" 2>&1
printf 'ordinary Git bytes\n' >"$work/source/hello.txt"
python3 - "$work/source/payload.bin" <<'PY'
import pathlib, sys
pathlib.Path(sys.argv[1]).write_bytes(bytes(range(256)) * 4096)
PY
oid=$(sha256sum "$work/source/payload.bin" | cut -d ' ' -f 1)
# Batch responses echo Authorization. Parse saved responses, never print them.
batch() {
    api -X POST "$base/smoke.git/info/lfs/objects/batch" -H 'Content-Type: application/vnd.git-lfs+json' \
        -d "{\"operation\":\"$1\",\"transfers\":[\"basic\"],\"objects\":[{\"oid\":\"$oid\",\"size\":1048576}]}" >"$work/batch.json"
    python3 - "$work/batch.json" "$base" "$oid" "$1" <<'PY'
import json, sys
r = json.load(open(sys.argv[1]))
o, = r["objects"]
assert "error" not in o
assert o["actions"][sys.argv[4]]["href"] == sys.argv[2] + "/smoke.git/info/lfs/objects/" + sys.argv[3]
PY
}
batch upload
git -C "$work/source" add .
git -C "$work/source" -c user.name=Smoke -c user.email=smoke@example.com commit -qm 'Git and LFS smoke'
git -C "$work/source" remote add origin "$clone_url"
git -C "$work/source" push -u origin main >>"$work/git.log" 2>&1
clone_and_compare() {
    GIT_LFS_SKIP_SMUDGE=1 git clone --config credential.helper= --config core.askPass="$GIT_ASKPASS" \
        "$clone_url" "$work/$1"
    git -C "$work/$1" lfs install --local
    git -C "$work/$1" lfs pull
    [[ $(git -C "$work/source" rev-parse HEAD) == "$(git -C "$work/$1" rev-parse HEAD)" ]]
    cmp "$work/source/hello.txt" "$work/$1/hello.txt"
    cmp "$work/source/payload.bin" "$work/$1/payload.bin"
} >>"$work/git.log" 2>&1
batch download
clone_and_compare before
compose down >>"$work/compose.log" 2>&1
up
compose up -d --wait --wait-timeout 60 --force-recreate >>"$work/compose.log" 2>&1
snapshot >"$work/keys.after" 2>>"$work/compose.log"
cmp "$work/keys.before" "$work/keys.after"
api "$base/api/v1/repo" >"$work/repo.after"
cmp "$work/repo.before" "$work/repo.after"
batch download
clone_and_compare after
echo 'PASS API, unauthorized rejection, host Git/LFS URLs and bytes, old token after down/up and recreation'
destroy

# Each fault gets fresh owned volumes; the real init must block the real server.
project=$bad_project
for fault in private-missing private-corrupt private-permissions pair-mismatch public-empty public-mismatch public-symlink public-directory published-only; do
    compose run --rm --no-deps -T init >>"$work/compose.log" 2>&1
    case "$fault" in
        private-missing) change='rm /private/org.pem' ;;
        private-corrupt) change='printf broken > /private/org.pem' ;;
        private-permissions) change='chmod 644 /private/org.pem' ;;
        pair-mismatch) change='openstorage keygen --out /tmp/other >/dev/null; cp /tmp/other.pub.pem /private/org.pub.pem' ;;
        public-empty) change=': > /public/org.pub.pem' ;;
        public-mismatch) change='openstorage keygen --out /tmp/other >/dev/null; cp /tmp/other.pub.pem /public/org.pub.pem' ;;
        public-symlink) change='rm /public/org.pub.pem; ln -s /private/org.pub.pem /public/org.pub.pem' ;;
        public-directory) change='rm /public/org.pub.pem; mkdir /public/org.pub.pem' ;;
        published-only) change='rm /private/org.pem /private/org.pub.pem' ;;
    esac
    init_shell "$change" >>"$work/compose.log" 2>&1
    snapshot >"$work/invalid.before" 2>>"$work/compose.log"
    if up; then echo "Invalid init accepted: $fault" >&2; exit 1; fi
    [[ -z $(compose ps --status running -q server) ]]
    [[ $(docker inspect --format '{{.State.ExitCode}}' "$(compose ps -a -q init)") != 0 ]]
    snapshot >"$work/invalid.after" 2>>"$work/compose.log"
    cmp "$work/invalid.before" "$work/invalid.after"
    destroy
    echo "PASS fail-closed init: $fault; state preserved and resources deleted"
done
echo 'PASS exact Compose quickstart development smoke'
