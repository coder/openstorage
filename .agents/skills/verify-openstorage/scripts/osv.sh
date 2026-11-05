#!/usr/bin/env bash
# osv.sh: build, launch, drive, and tear down one disposable open.storage node
# (local mode: SQLite + file blobs), or two nodes on PostgreSQL + SeaweedFS
# containers (--fleet), for verification. See ../SKILL.md.
#
#   osv.sh launch  <run> [--fleet]               build, keygen, start serve (and containers), run doctor
#   osv.sh doctor  <run>                         read-only health check of this run's nodes
#   osv.sh env     <run>                         print OSV_* exports: eval "$(osv.sh env <run>)"
#   osv.sh token   <run> <name> <mint flags...>  mint a token and store it under <name>
#   osv.sh token   <run> <name> --raw <value>    store a literal (bad) token under <name>
#   osv.sh api     <run> <label> [--node a|b] [--token <name>|none] [--expect <code>] <METHOD> <path> [curl args...]
#   osv.sh git     <run> <label> [--token <name>|none] [--expect <exit>] -- <git args...>
#   osv.sh eq      <run> <label> <expected> <actual>
#   osv.sh json    <file> <dotted.path>          print one JSON field; * maps a list (python3)
#   osv.sh packs   <run> <repo id>               list the repo's stored packs (read-only)
#   osv.sh stop    <run> a|b                     stop one node (fleet: check the other keeps serving)
#   osv.sh cleanup <run>                         stop serve and containers, delete <run>/work, keep <run>/evidence
#
# <run> is a caller-chosen directory outside the checkout. Scratch state lives in
# <run>/work (deleted by cleanup); proof lives in <run>/evidence (never deleted).
set -Eeuo pipefail
umask 077

ORG=selfhost
# Fleet mode: every container carries this label with the run dir as its value,
# so cleanup removes only this run's containers. The PostgreSQL password and the
# S3 secret key are random per run (work/secrets/), so another local user who
# reaches the loopback ports cannot use the stores. serve creates the bucket.
LABEL=dev.openstorage.osv.run
S3_ID=osv
BUCKET=openstorage
SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
REPO=$(cd "$SCRIPT_DIR/../../../.." && pwd -P)

die() { printf 'osv: %s\n' "$*" >&2; exit 1; }
[[ -f $REPO/go.mod && -d $REPO/cmd/openstorage ]] || die "cannot find the repo root from $SCRIPT_DIR"

now() { date -u +%Y-%m-%dT%H:%M:%SZ; }

# private_dir <dir>: refuse a directory that another local user could change.
# It must be owned by the current user and not writable by group or others,
# because env and every drive command source <run>/work/state.env.
private_dir() {
	local om uid mode
	om=$(stat -c '%u %a' "$1" 2>/dev/null || stat -f '%u %Lp' "$1" 2>/dev/null) || die "cannot stat $1"
	uid=${om%% *} mode=${om##* }
	[[ $uid == "$(id -u)" ]] || die "$1 is owned by uid $uid, not by you: refusing to use it"
	((8#$mode & 8#022)) && die "$1 is writable by group or others (mode $mode): refusing to use it"
	return 0
}

# safe_parents <dir>: refuse a path where another local user could rename
# <dir> or one of its parents and put their own state.env in its place. Every
# parent must be owned by root or by you, and not writable by group or others
# unless it has the sticky bit (as /tmp has: there only an entry's owner can
# rename it). <dir> must be a physical path (pwd -P).
safe_parents() {
	local d=$1 om uid mode
	while [[ $d != / ]]; do
		d=$(dirname "$d")
		om=$(stat -c '%u %a' "$d" 2>/dev/null || stat -f '%u %p' "$d" 2>/dev/null) || die "cannot stat $d"
		uid=${om%% *} mode=${om##* }
		[[ $uid == 0 || $uid == "$(id -u)" ]] || die "$d is owned by uid $uid: that user could swap the run dir: pick a run dir under directories that you or root own"
		if ((8#$mode & 8#022)) && ! ((8#$mode & 8#1000)); then
			die "$d is writable by group or others without the sticky bit (mode $mode): another user could swap the run dir"
		fi
	done
}

# require_git: git 2.32 or newer, the first release that honors GIT_CONFIG_GLOBAL.
# Older git reads the caller's ~/.gitconfig despite cmd_git's isolation.
require_git() {
	local v
	v=$(git --version) || die "git --version failed"
	[[ $v =~ ([0-9]+)\.([0-9]+) ]] || die "cannot parse '$v'"
	((BASH_REMATCH[1] > 2 || (BASH_REMATCH[1] == 2 && BASH_REMATCH[2] >= 32))) || die "$v is older than 2.32: it ignores GIT_CONFIG_GLOBAL"
}

# run_paths <run>: resolve RUN/WORK/EVID for an existing run dir.
run_paths() {
	[[ $# -ge 1 && -n $1 ]] || die "missing <run> directory argument"
	RUN=$(cd "$1" 2>/dev/null && pwd -P) || die "run dir $1 does not exist"
	WORK=$RUN/work
	EVID=$RUN/evidence
	BIN=$WORK/bin/openstorage
	safe_parents "$RUN"
	private_dir "$RUN"
	[[ ! -e $WORK ]] || private_dir "$WORK"
	[[ -f $RUN/.osv-run ]] || die "$RUN has no .osv-run marker: it was not created by 'osv.sh launch'"
}

# load_state: read the address and PID that launch recorded.
load_state() {
	[[ -f $WORK/state.env ]] || die "no $WORK/state.env: launch did not finish (or cleanup already ran)"
	# shellcheck disable=SC1091
	. "$WORK/state.env"
	[[ -n ${OSV_ADDR-} && -n ${OSV_PID-} ]] || die "state.env is missing OSV_ADDR or OSV_PID"
	OSV_URL=http://$OSV_ADDR
	OSV_URL_B='' OSV_MODE=${OSV_MODE:-local}
	if [[ $OSV_MODE == fleet ]]; then
		[[ -n ${OSV_ADDR_B-} && -n ${OSV_PID_B-} ]] || die "state.env is missing OSV_ADDR_B or OSV_PID_B"
		OSV_URL_B=http://$OSV_ADDR_B
	fi
}

check_label() {
	[[ $1 =~ ^[A-Za-z0-9._-]+$ ]] || die "label '$1' must match [A-Za-z0-9._-]+ (it names evidence files)"
	if [[ -s $EVID/log.tsv ]] && cut -f2 "$EVID/log.tsv" | grep -qxF -- "$1"; then
		die "label '$1' is already used in $EVID/log.tsv: give every api, git, and eq call its own label"
	fi
}

# record <label> <kind> <what> <token> <result> <expect> <verdict>: one row in evidence/log.tsv.
record() {
	[[ -s $EVID/log.tsv ]] || printf 'time\tlabel\tkind\twhat\ttoken\tresult\texpect\tverdict\n' >"$EVID/log.tsv"
	printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(now)" "$@" >>"$EVID/log.tsv"
}

token_desc() {
	if [[ $1 == none ]]; then echo "none"; else echo "$1 ($(<"$WORK/tokens/$1.desc"))"; fi
}

require_token() {
	[[ $1 == none || -f $WORK/tokens/$1 ]] || die "no token '$1': mint it with 'osv.sh token <run> $1 --repo <repo> --scopes <scopes>'"
}

# is_our_serve <pid>: the PID is alive and runs this run's binary.
is_our_serve() {
	local pid=$1 exe
	kill -0 "$pid" 2>/dev/null || return 1
	if [[ -r /proc/$pid/exe || -L /proc/$pid/exe ]]; then
		exe=$(readlink "/proc/$pid/exe" 2>/dev/null) || return 1
		[[ $exe == "$BIN" ]]
	else
		exe=$(ps -p "$pid" -o command= 2>/dev/null) || return 1
		[[ $exe == "$BIN serve "* ]]
	fi
}

# descendants_of <pid>: every live descendant PID (portable ps columns).
descendants_of() {
	ps -A -o pid= -o ppid= | awk -v root="$1" '
		{ parent[$1] = $2 }
		END {
			keep[root] = 1
			do {
				changed = 0
				for (p in parent) if (!(p in keep) && (parent[p] in keep)) { keep[p] = 1; changed = 1 }
			} while (changed)
			for (p in keep) if (p != root) print p
		}'
}

# SECRET_SHIM runs inside a clean env, right before exec. With OSV_SECRET_DIR
# set, it reads the store secrets from their files into AWS_SECRET_ACCESS_KEY
# and into the @PGSECRET@ placeholder of OPENSTORAGE_PG_URL. So the secrets
# reach the child's environment and never any command line.
# shellcheck disable=SC2016 # expands in the inner bash
SECRET_SHIM='if [[ -n ${OSV_SECRET_DIR-} ]]; then
	export AWS_SECRET_ACCESS_KEY=$(<"$OSV_SECRET_DIR/s3")
	[[ -z ${OPENSTORAGE_PG_URL-} ]] || export OPENSTORAGE_PG_URL=${OPENSTORAGE_PG_URL/@PGSECRET@/"$(<"$OSV_SECRET_DIR/pg")"}
	unset OSV_SECRET_DIR
fi'

# clean_argv [VAR=value...]: set CLEAN to an argv prefix that runs a command
# with PATH, a private HOME and TMPDIR, the given variables, and the store
# secrets (see SECRET_SHIM). The shim bash adds only PWD and SHLVL. A caller's HOME would hand the child the caller's
# git and AWS config, and an inherited OPENSTORAGE_* URL would replace the
# stores. Run a command as "${CLEAN[@]}" <command...>.
clean_argv() {
	CLEAN=(env -i PATH="$PATH" HOME="$WORK/home" TMPDIR="$WORK/tmp" "$@" bash -c "$SECRET_SHIM
exec \"\$@\"" osv-exec)
}

# start_serve <file prefix> [VAR=value...] -- <serve command...>: start one
# serve in its own session with a clean env (see clean_argv), wait for its
# address, and set SERVE_ADDR and SERVE_PID. The prefix names <prefix>.pid,
# <prefix>.out, and <prefix>.err.
start_serve() {
	local p=$1 vars=()
	shift
	while [[ $# -gt 0 && $1 != -- ]]; do vars+=("$1"); shift; done
	[[ ${1-} == -- ]] || die "start_serve: missing --"
	shift
	clean_argv OPENSTORAGE_ORG="$ORG" OPENSTORAGE_ORG_PUBKEYS="$WORK/keys/org.pub.pem" ${vars[@]+"${vars[@]}"}
	local setsid=()
	command -v setsid >/dev/null && setsid=(setsid)
	# The inner bash closes every inherited descriptor above stderr. Otherwise
	# serve keeps the caller's open files, such as a flock that the caller holds
	# around launch, and a cleanup under the same lock waits for serve forever.
	# Never redirect the loop's stderr: bash saves fd 2 above 9 for the loop,
	# the loop closes that copy, and serve then starts with stderr on /dev/null.
	# shellcheck disable=SC2016 # $$, $f, and $1 expand in the inner bash
	${setsid[@]+"${setsid[@]}"} bash -c 'echo $$ >"$1"; shift
		for f in /proc/$$/fd/* /dev/fd/*; do
			f=${f##*/}
			[[ $f =~ ^[0-9]+$ ]] && ((f > 2)) && eval "exec $f>&-"
		done
		exec "$@"' osv-serve "$p.pid" "${CLEAN[@]}" "$@" >"$p.out" 2>"$p.err" </dev/null &

	local pid='' addr='' deadline=$((SECONDS + 60))
	while ((SECONDS < deadline)); do
		[[ -z $pid && -s $p.pid ]] && pid=$(<"$p.pid")
		addr=$(sed -n 's/^listening on //p' "$p.out" | head -n 1)
		[[ -n $addr ]] && break
		if [[ -n $pid ]] && ! kill -0 "$pid" 2>/dev/null; then
			tail -n 20 "$p.err" >&2
			die "serve ($(basename "$p")) exited before it announced its address (stderr above; run 'osv.sh cleanup $RUN')"
		fi
		sleep 0.2
	done
	[[ -n $addr && -n $pid ]] || die "serve ($(basename "$p")) did not print 'listening on' within 60 s (run 'osv.sh cleanup $RUN')"
	SERVE_ADDR=$addr SERVE_PID=$pid
}

# compose_image <name>: the pinned image reference for <name> in the repo's
# docker-compose.yml, so fleet mode runs the same images as the dev stack.
compose_image() {
	local ref
	ref=$(sed -n "s|^ *image: \\($1:[^ ]*@sha256:[0-9a-f]\\{64\\}\\)\$|\\1|p" "$REPO/docker-compose.yml" | head -n 1)
	[[ -n $ref ]] || die "no digest-pinned '$1' image in $REPO/docker-compose.yml"
	echo "$ref"
}

# s3 <curl args...>: one SigV4-signed request to this run's SeaweedFS gateway.
# The credential goes to curl on stdin, never on its command line.
s3() {
	printf 'user = "%s:%s"\n' "$S3_ID" "$(<"$WORK/secrets/s3")" |
		curl -q -sS --noproxy '*' --max-time 30 --aws-sigv4 aws:amz:us-east-1:s3 -K - "$@"
}

# new_secret <name>: write a random 32-hex-digit secret to work/secrets/<name>.
new_secret() {
	mkdir -p "$WORK/secrets"
	python3 -c 'import secrets; print(secrets.token_hex(16), end="")' >"$WORK/secrets/$1"
}

# run_containers: IDs of every container that carries this run's label.
run_containers() {
	docker ps -aq --no-trunc --filter "label=$LABEL=$RUN"
}

# start_backends: start PostgreSQL and SeaweedFS for a fleet run, wait until
# each accepts real work, and set PG_PORT and S3_PORT.
start_backends() {
	local pg_img s3_img pg_id s3_id
	new_secret pg
	new_secret s3
	pg_img=$(compose_image postgres)
	s3_img=$(compose_image chrislusf/seaweedfs)
	printf 'postgres image: %s\nseaweedfs image: %s\n' "$pg_img" "$s3_img" >>"$EVID/launch.txt"

	echo "osv: starting postgres and seaweedfs containers (label $LABEL=$RUN)" >&2
	# Ports bind to loopback only; docker picks the host port.
	# "-e NAME" without a value copies NAME from the docker client's env, so
	# the password stays off the command line.
	pg_id=$(POSTGRES_PASSWORD=$(<"$WORK/secrets/pg") docker create --label "$LABEL=$RUN" -e POSTGRES_PASSWORD -p 127.0.0.1::5432 "$pg_img") ||
		die "docker create postgres failed (run 'osv.sh cleanup $RUN')"
	docker start "$pg_id" >/dev/null || die "docker start postgres failed (run 'osv.sh cleanup $RUN')"
	printf '{"identities":[{"name":"%s","credentials":[{"accessKey":"%s","secretKey":"%s"}],"actions":["Admin","Read","List","Tagging","Write"]}]}\n' \
		"$S3_ID" "$S3_ID" "$(<"$WORK/secrets/s3")" >"$WORK/s3.json"
	s3_id=$(docker create --label "$LABEL=$RUN" -p 127.0.0.1::8333 "$s3_img" \
		server -s3 -dir=/data -ip.bind=0.0.0.0 -s3.config=/osv-s3.json) ||
		die "docker create seaweedfs failed (run 'osv.sh cleanup $RUN')"
	docker cp "$WORK/s3.json" "$s3_id:/osv-s3.json" >/dev/null || die "docker cp of the S3 config failed (run 'osv.sh cleanup $RUN')"
	docker start "$s3_id" >/dev/null || die "docker start seaweedfs failed (run 'osv.sh cleanup $RUN')"
	printf 'postgres container: %s\nseaweedfs container: %s\n' "$pg_id" "$s3_id" >>"$EVID/launch.txt"
	PG_PORT=$(docker port "$pg_id" 5432/tcp | sed -n 's/^127\.0\.0\.1://p' | head -n 1)
	S3_PORT=$(docker port "$s3_id" 8333/tcp | sed -n 's/^127\.0\.0\.1://p' | head -n 1)
	[[ -n $PG_PORT && -n $S3_PORT ]] || die "cannot read the published container ports (run 'osv.sh cleanup $RUN')"

	# Ready means usable. The postgres image runs its init scripts on a
	# socket-only server first, so wait for a TCP query on the final server.
	local deadline=$((SECONDS + 90)) ok=''
	while ((SECONDS < deadline)); do
		if [[ $(PGPASSWORD=$(<"$WORK/secrets/pg") docker exec -e PGPASSWORD "$pg_id" psql -h 127.0.0.1 -U postgres -tAc 'select 1' 2>/dev/null) == 1 ]]; then
			ok=1
			break
		fi
		sleep 0.5
	done
	[[ -n $ok ]] || die "postgres did not answer a TCP query within 90 s (run 'osv.sh cleanup $RUN')"
	# A fresh SeaweedFS can hold the first object PUT for 10-15 s and refuse
	# writes before a volume is writable: wait for a real object write, in a
	# probe bucket that serve never reads.
	deadline=$((SECONDS + 120)) ok=''
	while ((SECONDS < deadline)); do
		s3 -o /dev/null -X PUT "http://127.0.0.1:$S3_PORT/osv-ready" 2>/dev/null || true
		if [[ $(s3 -o /dev/null -w '%{http_code}' -X PUT --data-binary ready "http://127.0.0.1:$S3_PORT/osv-ready/probe" 2>/dev/null) == 200 ]]; then
			ok=1
			break
		fi
		sleep 1
	done
	[[ -n $ok ]] || die "seaweedfs did not accept an object write within 120 s (run 'osv.sh cleanup $RUN')"
}

cmd_launch() {
	local fleet=''
	if [[ ${2-} == --fleet ]]; then
		[[ $# -eq 2 ]] || die "usage: osv.sh launch <run> [--fleet]"
		fleet=1
	else
		[[ $# -eq 1 ]] || die "usage: osv.sh launch <run> [--fleet]"
	fi
	[[ -n $1 ]] || die "usage: osv.sh launch <run> [--fleet]"
	# Resolve the path before creating anything, so a refused path leaves no trace.
	local parent
	parent=$(cd "$(dirname "$1")" 2>/dev/null && pwd -P) || die "the parent of $1 does not exist"
	RUN=$parent/$(basename "$1")
	[[ -d $RUN ]] && RUN=$(cd "$RUN" && pwd -P)
	case $RUN/ in "$REPO"/*) die "run dir $RUN is inside the checkout $REPO: pick one outside it so git status stays clean" ;; esac
	[[ ! -e $RUN || -z $(ls -A "$RUN") ]] || die "run dir $RUN is not empty: use a fresh directory for every run"
	for tool in go git curl python3 ${fleet:+docker}; do command -v "$tool" >/dev/null || die "$tool is not on PATH"; done
	require_git
	safe_parents "$RUN"
	if [[ -n $fleet ]]; then
		docker info >/dev/null 2>&1 || die "'docker info' failed: the docker daemon is not reachable"
		# Containers from an unfinished cleanup of an earlier run at this path
		# carry the same label, and doctor and cleanup would count them too.
		local old
		old=$(run_containers) || die "docker ps failed"
		[[ -z $old ]] || die "containers with label $LABEL=$RUN still exist from an earlier run at this path: check them, remove them with 'docker rm -f -v $(tr '\n' ' ' <<<"$old")', or use another run dir"
	fi
	mkdir -p "$RUN"
	chmod 700 "$RUN" || die "cannot chmod 700 $RUN"
	private_dir "$RUN"
	[[ -z $(ls -A "$RUN") ]] || die "run dir $RUN changed while launch prepared it: use a fresh directory"
	WORK=$RUN/work EVID=$RUN/evidence BIN=$RUN/work/bin/openstorage
	# The marker outlives cleanup's delete of work/, so a repeated cleanup still
	# knows to look for this run's containers.
	if [[ -n $fleet ]]; then echo mode=fleet; else echo mode=local; fi >"$RUN/.osv-run"
	mkdir -p "$WORK/bin" "$WORK/keys" "$WORK/tokens" "$WORK/home" "$WORK/tmp" "$EVID"

	{
		echo "launched: $(now)"
		echo "checkout: $REPO"
		echo "checkout HEAD: $(git -C "$REPO" rev-parse HEAD)"
		echo "checkout changes: $(git -C "$REPO" status --porcelain | wc -l | tr -d ' ') paths"
		echo "go: $(go version)"
		echo "git: $(git --version)"
	} >"$EVID/launch.txt"

	echo "osv: building $BIN from $REPO" >&2
	(cd "$REPO" && go build -o "$BIN" ./cmd/openstorage) >"$EVID/build.txt" 2>&1 || {
		cat "$EVID/build.txt" >&2
		die "go build failed (log: $EVID/build.txt)"
	}
	echo "binary: $("$BIN" version)" >>"$EVID/launch.txt"
	env -i PATH="$PATH" "$BIN" keygen --out "$WORK/keys/org" >/dev/null

	if [[ -z $fleet ]]; then
		start_serve "$WORK/serve" -- "$BIN" serve --listen 127.0.0.1:0 --data-dir "$WORK/data"
		printf 'OSV_MODE=local\nOSV_ADDR=%s\nOSV_PID=%s\n' "$SERVE_ADDR" "$SERVE_PID" >"$WORK/state.env"
		printf 'serve pid: %s\nlisten: http://%s\ndata dir: %s\n' "$SERVE_PID" "$SERVE_ADDR" "$WORK/data" >>"$EVID/launch.txt"
		echo "osv: serve pid $SERVE_PID listening on http://$SERVE_ADDR" >&2
	else
		start_backends
		local pg_url s3_url node pfx addr_a pid_a
		# @PGSECRET@ is replaced inside the clean env (SECRET_SHIM).
		pg_url="postgres://postgres:@PGSECRET@@127.0.0.1:$PG_PORT/postgres?sslmode=disable"
		s3_url="s3://$BUCKET?endpoint=http://127.0.0.1:$S3_PORT&s3ForcePathStyle=true&region=us-east-1"
		# Node a starts alone, so it runs the migrations and creates the bucket
		# before node b boots. Each node has its own cache: only the database
		# and the bucket are shared.
		for node in a b; do
			pfx=$WORK/serve
			[[ $node == a ]] || pfx=$WORK/serve-$node
			start_serve "$pfx" \
				OPENSTORAGE_PG_URL="$pg_url" OPENSTORAGE_S3_URL="$s3_url" \
				AWS_ACCESS_KEY_ID="$S3_ID" OSV_SECRET_DIR="$WORK/secrets" -- \
				"$BIN" serve --listen 127.0.0.1:0 --cache-dir "$WORK/cache-$node"
			printf 'node %s: serve pid %s, listen http://%s, cache %s\n' "$node" "$SERVE_PID" "$SERVE_ADDR" "$WORK/cache-$node" >>"$EVID/launch.txt"
			echo "osv: node $node: serve pid $SERVE_PID listening on http://$SERVE_ADDR" >&2
			[[ $node == b ]] || addr_a=$SERVE_ADDR pid_a=$SERVE_PID
		done
		printf 'database: postgres://postgres:<secret:pg>@127.0.0.1:%s/postgres\nbucket: %s\n' "$PG_PORT" "$s3_url" >>"$EVID/launch.txt"
		printf 'OSV_MODE=fleet\nOSV_ADDR=%s\nOSV_PID=%s\nOSV_ADDR_B=%s\nOSV_PID_B=%s\nOSV_S3_URL=%q\nOSV_S3_PORT=%s\n' \
			"$addr_a" "$pid_a" "$SERVE_ADDR" "$SERVE_PID" "$s3_url" "$S3_PORT" >"$WORK/state.env"
	fi
	cmd_doctor "$RUN"
}

cmd_doctor() {
	run_paths "$@"
	load_state
	local fails=0 out
	out=$(
		fails=0
		say() { printf '%-5s %-14s %s\n' "$1" "$2" "$3"; [[ $1 != FAIL ]] || fails=$((fails + 1)); }
		# node_checks <suffix> <addr> <pid>: process, port, healthz, and auth+db for one serve.
		node_checks() {
			local sfx=$1 addr=$2 pid=$3 url=http://$2 port=${2##*:} body auth tok code
			if is_our_serve "$pid"; then say PASS "process$sfx" "pid $pid runs $BIN"; else say FAIL "process$sfx" "pid $pid is not alive or is not $BIN"; fi

			if command -v ss >/dev/null; then
				if ss -Hltnp "sport = :$port" 2>/dev/null | grep -q "pid=$pid,"; then say PASS "port$sfx" "$addr is bound by pid $pid"; else say FAIL "port$sfx" "$addr is not bound by pid $pid (ss)"; fi
			elif command -v lsof >/dev/null; then
				if lsof -nP -a -p "$pid" -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then say PASS "port$sfx" "$addr is bound by pid $pid"; else say FAIL "port$sfx" "$addr is not bound by pid $pid (lsof)"; fi
			else
				say SKIP "port$sfx" "neither ss nor lsof is installed"
			fi

			body=$(curl -q -sS --noproxy '*' --max-time 10 "$url/healthz" 2>&1) || true
			if [[ $body == '{"status":"ok"}' ]]; then say PASS "healthz$sfx" "$url/healthz answers $body"; else say FAIL "healthz$sfx" "$url/healthz answered: ${body:0:200}"; fi

			auth=$WORK/tmp/doctor.auth
			if tok=$(env -i PATH="$PATH" "$BIN" mint --key "$WORK/keys/org.pem" --iss "$ORG" --repo org --scopes org:read --ttl 5m 2>&1); then
				printf 'Authorization: Bearer %s\n' "$tok" >"$auth"
				code=$(curl -q -s --noproxy '*' --max-time 10 -o /dev/null -w '%{http_code}' -H "@$auth" "$url/api/v1/repos") || true
				rm -f "$auth"
				if [[ $code == 200 ]]; then say PASS "auth+db$sfx" "org:read token lists repos (GET /api/v1/repos 200)"; else say FAIL "auth+db$sfx" "GET /api/v1/repos answered $code with an org:read token"; fi
			else
				say FAIL "auth+db$sfx" "mint failed: $tok"
			fi
		}
		if [[ $OSV_MODE == fleet ]]; then
			node_checks :a "$OSV_ADDR" "$OSV_PID"
			node_checks :b "$OSV_ADDR_B" "$OSV_PID_B"
		else
			node_checks '' "$OSV_ADDR" "$OSV_PID"
		fi

		head_sha=$(git -C "$REPO" rev-parse HEAD)
		built=$("$BIN" version 2>&1) || true
		if [[ $built == *"commit $head_sha"* ]]; then say PASS build "$built"; else say FAIL build "binary is '$built' but the checkout HEAD is $head_sha: start a new run"; fi
		# Go records uncommitted changes in the build setting vcs.modified=true.
		if go version -m "$BIN" 2>/dev/null | grep -q 'vcs\.modified=true'; then
			say WARN build "vcs.modified=true: built from a checkout with uncommitted changes"
		fi

		if [[ $OSV_MODE == fleet ]]; then
			local ids id running=0 total=0
			ids=$(run_containers 2>/dev/null) || ids=''
			for id in $ids; do
				total=$((total + 1))
				[[ $(docker inspect -f '{{.State.Running}}' "$id" 2>/dev/null) == true ]] && running=$((running + 1))
			done
			if ((total == 2 && running == 2)); then say PASS containers "postgres and seaweedfs run with label $LABEL=$RUN"; else say FAIL containers "$running of $total labeled containers run (want 2 of 2)"; fi
			code=$(s3 -o /dev/null -w '%{http_code}' -I "http://127.0.0.1:$OSV_S3_PORT/$BUCKET" 2>/dev/null) || true
			if [[ $code == 200 ]]; then say PASS bucket "s3 bucket $BUCKET exists on 127.0.0.1:$OSV_S3_PORT"; else say FAIL bucket "HEAD /$BUCKET on 127.0.0.1:$OSV_S3_PORT answered ${code:-nothing}"; fi
		elif [[ -f $WORK/data/codestorage.db ]]; then say PASS data-dir "$WORK/data (sqlite + file blobs)"; else say FAIL data-dir "$WORK/data/codestorage.db is missing"; fi
		echo "doctor: $fails failure(s)"
		exit $((fails > 0))
	) || fails=1
	printf '%s\n' "$out"
	printf '== doctor %s\n%s\n' "$(now)" "$out" >>"$EVID/doctor.txt"
	return "$fails"
}

cmd_env() {
	run_paths "$@"
	load_state
	printf 'export OSV_MODE=%q OSV_RUN=%q OSV_URL=%q OSV_BIN=%q OSV_WORK=%q OSV_EVIDENCE=%q OSV_ORG=%q\n' \
		"$OSV_MODE" "$RUN" "$OSV_URL" "$BIN" "$WORK" "$EVID" "$ORG"
	[[ $OSV_MODE == fleet ]] || return 0
	printf 'export OSV_URL_B=%q\n' "$OSV_URL_B"
}

# cmd_packs: list the repo's stored packs, from the data dir in local mode and
# from the shared bucket in fleet mode. Read-only.
cmd_packs() {
	run_paths "${1-}"
	[[ $# -eq 2 && -n $2 ]] || die "usage: osv.sh packs <run> <repo id>"
	load_state
	if [[ $OSV_MODE == fleet ]]; then
		clean_argv OPENSTORAGE_S3_URL="$OSV_S3_URL" AWS_ACCESS_KEY_ID="$S3_ID" OSV_SECRET_DIR="$WORK/secrets"
		"${CLEAN[@]}" "$BIN" list-packs --repo "$2"
	else
		clean_argv
		"${CLEAN[@]}" "$BIN" list-packs --repo "$2" --data-dir "$WORK/data"
	fi
}

# cmd_stop: stop one node of this run (SIGTERM, then SIGKILL after 30 s), for
# recipes that check what happens when a replica goes away.
cmd_stop() {
	run_paths "${1-}"
	[[ $# -eq 2 && ($2 == a || $2 == b) ]] || die "usage: osv.sh stop <run> a|b"
	load_state
	[[ $2 == a || $OSV_MODE == fleet ]] || die "node b exists only in a run launched with --fleet"
	local pidfile=$WORK/serve.pid
	[[ $2 == a ]] || pidfile=$WORK/serve-b.pid
	LEFT=''
	stop_serve "$pidfile"
	[[ -z $LEFT ]] || die "node $2: processes still alive:$LEFT"
	echo "== stop node $2 $(now): no process of node $2 is alive" | tee -a "$EVID/doctor.txt"
}

cmd_token() {
	run_paths "${1-}"
	shift
	local name=${1-}
	[[ $name =~ ^[A-Za-z0-9_-]+$ ]] || die "usage: osv.sh token <run> <name> <mint flags...> | --raw <value>"
	[[ $name != none ]] || die "'none' is reserved: it means 'send no credentials'"
	shift
	local tok desc
	if [[ ${1-} == --raw ]]; then
		[[ $# -eq 2 ]] || die "usage: osv.sh token <run> <name> --raw <value>"
		tok=$2 desc="raw literal"
	else
		[[ $# -gt 0 ]] || die "usage: osv.sh token <run> <name> --repo <repo|org> --scopes <list> [--ttl ..] [--refs ..]"
		tok=$(env -i PATH="$PATH" "$BIN" mint --key "$WORK/keys/org.pem" --iss "$ORG" "$@") || die "openstorage mint failed"
		desc="mint $*"
	fi
	printf '%s' "$tok" >"$WORK/tokens/$name"
	printf '%s' "$desc" >"$WORK/tokens/$name.desc"
	echo "osv: token '$name': $desc" >&2
}

parse_common() {
	TOKEN=none EXPECT='' NODE=''
	NSHIFT=0
	while [[ $# -gt 0 ]]; do
		case $1 in
			--node) NODE=${2?--node needs a value}; shift 2; NSHIFT=$((NSHIFT + 2)) ;;
			--token) TOKEN=${2?--token needs a value}; shift 2; NSHIFT=$((NSHIFT + 2)) ;;
			--expect) EXPECT=${2?--expect needs a value}; shift 2; NSHIFT=$((NSHIFT + 2)) ;;
			*) break ;;
		esac
	done
	require_token "$TOKEN"
}

cmd_api() {
	run_paths "${1-}"
	shift
	load_state
	local label=${1-}
	check_label "$label"
	shift
	parse_common "$@"
	shift "$NSHIFT"
	[[ $# -ge 2 ]] || die "usage: osv.sh api <run> <label> [--node a|b] [--token <name>|none] [--expect <code>] <METHOD> <path> [curl args...]"
	local where=''
	case ${NODE:-a} in
		a) [[ $OSV_MODE == local ]] || where=" (node a)" ;;
		b)
			[[ $OSV_MODE == fleet ]] || die "--node b needs a run launched with --fleet"
			OSV_URL=$OSV_URL_B where=" (node b)"
			;;
		*) die "--node must be a or b, not '$NODE'" ;;
	esac
	local method=$1 path=$2
	shift 2
	[[ $path == /* ]] || die "path '$path' must start with / (it is appended to $OSV_URL)"
	[[ ! -e $EVID/$label.request.txt ]] || die "label '$label' is already used in $EVID"
	local hdr=() auth=$WORK/tmp/auth.$$
	if [[ $TOKEN != none ]]; then
		# /api/* takes a Bearer token; the Git smart-HTTP surface takes Basic
		# auth with the literal username "t" and the token as the password.
		case $path in
			/api/*) printf 'Authorization: Bearer %s\n' "$(<"$WORK/tokens/$TOKEN")" >"$auth" ;;
			*) printf 'Authorization: Basic %s\n' "$(printf 't:%s' "$(<"$WORK/tokens/$TOKEN")" | base64 | tr -d '\n')" >"$auth" ;;
		esac
		hdr=(-H "@$auth")
	fi
	printf '%s %s%s\ntoken: %s\ncurl args:' "$method" "$OSV_URL" "$path" "$(token_desc "$TOKEN")" >"$EVID/$label.request.txt"
	printf ' %q' "$@" >>"$EVID/$label.request.txt"
	echo >>"$EVID/$label.request.txt"
	local code rc=0
	code=$(curl -q -sS --noproxy '*' --max-time 120 -X "$method" ${hdr[@]+"${hdr[@]}"} \
		-o "$EVID/$label.body" -D "$EVID/$label.headers" -w '%{http_code}' "$OSV_URL$path" "$@" 2>"$WORK/tmp/curl.err") || rc=$?
	rm -f "$auth"
	if ((rc != 0)); then
		code=curl-exit-$rc
		cat "$WORK/tmp/curl.err" >>"$EVID/$label.request.txt"
	fi
	local verdict=-
	[[ -z $EXPECT ]] || { [[ $code == "$EXPECT" ]] && verdict=PASS || verdict=FAIL; }
	record "$label" api "$method $path$where" "$TOKEN" "$code" "${EXPECT:--}" "$verdict"
	echo "$code"
	if [[ $verdict == FAIL ]]; then
		printf 'osv: %s: %s %s answered %s, expected %s. Body (first 400 bytes):\n' "$label" "$method" "$path" "$code" "$EXPECT" >&2
		head -c 400 "$EVID/$label.body" >&2 2>/dev/null || true
		echo >&2
		return 1
	fi
	[[ $code != curl-exit-* ]] || { cat "$WORK/tmp/curl.err" >&2; return 1; }
}

cmd_git() {
	run_paths "${1-}"
	shift
	load_state
	local label=${1-}
	check_label "$label"
	shift
	parse_common "$@"
	shift "$NSHIFT"
	[[ -z $NODE ]] || die "osv.sh git takes no --node: put the node's URL (\$OSV_URL or \$OSV_URL_B) in the git arguments"
	[[ ${1-} == -- ]] || die "usage: osv.sh git <run> <label> [--token <name>|none] [--expect <exit>] -- <git args...>"
	shift
	[[ $# -gt 0 ]] || die "no git arguments after --"
	require_git
	[[ ! -e $EVID/$label.git.txt ]] || die "label '$label' is already used in $EVID"
	local tokfile=''
	[[ $TOKEN == none ]] || tokfile=$WORK/tokens/$TOKEN
	local rc=0
	{
		printf '$ git'
		printf ' %q' "$@"
		printf '\n(cwd %s, token: %s)\n' "$WORK" "$(token_desc "$TOKEN")"
	} >"$EVID/$label.git.txt"
	(
		# Hermetic git: no inherited GIT_* variables, no global/system config,
		# pinned identity, no credential helper, no background maintenance.
		while IFS= read -r v; do unset "$v"; done < <(compgen -e | grep '^GIT_' || true)
		export GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_SYSTEM=/dev/null GIT_CONFIG_NOSYSTEM=1
		export GIT_TERMINAL_PROMPT=0 GIT_ASKPASS="$SCRIPT_DIR/askpass.sh" OSV_GIT_TOKEN_FILE="$tokfile"
		export GIT_AUTHOR_NAME=osv GIT_AUTHOR_EMAIL=osv@example.invalid
		export GIT_COMMITTER_NAME=osv GIT_COMMITTER_EMAIL=osv@example.invalid
		export GIT_CONFIG_COUNT=5
		export GIT_CONFIG_KEY_0=init.defaultBranch GIT_CONFIG_VALUE_0=main
		export GIT_CONFIG_KEY_1=maintenance.auto GIT_CONFIG_VALUE_1=false
		export GIT_CONFIG_KEY_2=gc.auto GIT_CONFIG_VALUE_2=0
		export GIT_CONFIG_KEY_3=credential.helper GIT_CONFIG_VALUE_3=
		export GIT_CONFIG_KEY_4=advice.detachedHead GIT_CONFIG_VALUE_4=false
		export NO_PROXY=127.0.0.1,localhost no_proxy=127.0.0.1,localhost
		# askpass.sh matches git's English "Username"/"Password" prompts, and
		# the recipes grep git's English messages.
		export LC_ALL=C
		cd "$WORK"
		exec git "$@"
	) >"$WORK/tmp/git.out" 2>&1 || rc=$?
	tee -a "$EVID/$label.git.txt" <"$WORK/tmp/git.out"
	local verdict=- want=${EXPECT:-0}
	[[ $rc == "$want" ]] && verdict=PASS || verdict=FAIL
	echo "exit: $rc (expect $want) $verdict" >>"$EVID/$label.git.txt"
	record "$label" git "git $*" "$TOKEN" "exit $rc" "exit $want" "$verdict"
	if [[ $verdict == FAIL ]]; then
		echo "osv: $label: git exited $rc, expected $want" >&2
		return 1
	fi
}

cmd_eq() {
	run_paths "${1-}"
	[[ $# -eq 4 ]] || die "usage: osv.sh eq <run> <label> <expected> <actual>"
	local label=$2 want=$3 got=$4 verdict=FAIL
	check_label "$label"
	[[ -n $want && $want == "$got" ]] && verdict=PASS
	printf 'expected: %s\nactual:   %s\n%s\n' "$want" "$got" "$verdict" >"$EVID/$label.eq.txt"
	record "$label" eq "equal" - "$got" "$want" "$verdict"
	echo "$verdict $label: expected '$want', actual '$got'"
	[[ $verdict == PASS ]]
}

cmd_json() {
	[[ $# -eq 2 ]] || die "usage: osv.sh json <file> <dotted.path>  (commits.0.sha; * maps a list: commits.*.sha)"
	python3 - "$1" "$2" <<'PYEOF'
import json, sys
def get(v, parts):
    if not parts:
        return v
    head, rest = parts[0], parts[1:]
    if head == "*":
        return [get(x, rest) for x in v]
    return get(v[int(head)] if isinstance(v, list) else v[head], rest)
try:
    v = get(json.load(open(sys.argv[1])), sys.argv[2].split("."))
except (OSError, ValueError, KeyError, IndexError, TypeError) as e:
    sys.exit("osv: json: cannot read %r from %s: %s: %s" % (sys.argv[2], sys.argv[1], type(e).__name__, e))
print(v if isinstance(v, str) else json.dumps(v))
PYEOF
}

save_redacted_evidence() {
	[[ -d $WORK ]] || return 0
	[[ -f $WORK/serve.out ]] && cp "$WORK/serve.out" "$EVID/serve.stdout.log"
	[[ -f $WORK/serve.err ]] && cp "$WORK/serve.err" "$EVID/serve.stderr.log"
	[[ -f $WORK/serve-b.out ]] && cp "$WORK/serve-b.out" "$EVID/serve-b.stdout.log"
	[[ -f $WORK/serve-b.err ]] && cp "$WORK/serve-b.err" "$EVID/serve-b.stderr.log"
	[[ -f $WORK/state.env ]] && cp "$WORK/state.env" "$EVID/state.env"
	local t
	for t in "$WORK"/tokens/*.desc; do
		[[ -e $t ]] || continue
		printf '%s: %s\n' "$(basename "$t" .desc)" "$(<"$t")"
	done >"$EVID/tokens.txt"
	# Replace every stored token value with <token:name> in the evidence, and
	# its Basic-auth form base64("t:<token>"), which curl -v prints in a
	# failed call's stderr that cmd_api keeps.
	# Fleet runs also replace their store secrets with <secret:name>.
	python3 - "$WORK/tokens" "$EVID" "$WORK/secrets" <<'PYEOF'
import base64, os, stat, sys
def fail(e):
    raise e
tokdir, evid, secdir = sys.argv[1], sys.argv[2], sys.argv[3]
toks = {}
for n in os.listdir(tokdir):
    if not n.endswith(".desc"):
        v = open(os.path.join(tokdir, n), "rb").read()
        if len(v) >= 8:
            toks[base64.b64encode(b"t:" + v)] = b"<basic:" + n.encode() + b">"
            toks[v] = b"<token:" + n.encode() + b">"
if os.path.isdir(secdir):
    for n in os.listdir(secdir):
        v = open(os.path.join(secdir, n), "rb").read()
        if len(v) >= 8:
            toks[v] = b"<secret:" + n.encode() + b">"
for d, _, names in os.walk(evid, onerror=fail):
    for n in names:
        p = os.path.join(d, n)
        if not stat.S_ISREG(os.lstat(p).st_mode):
            continue
        b = open(p, "rb").read()
        r = b
        for v, s in toks.items():
            r = r.replace(v, s)
        if r != b:
            open(p, "wb").write(r)
PYEOF
}

# stop_serve <pid file>: stop one serve of this run and add the PIDs that
# survive to LEFT.
stop_serve() {
	local pid='' kids='' p k deadline
	[[ -s $1 ]] && pid=$(<"$1")
	if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null && ! is_our_serve "$pid"; then
		# Our serve exited and the PID was reused: never signal that process.
		echo "osv: pid $pid from $1 no longer runs $BIN: treating that serve as exited" >&2
		return 0
	fi
	if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
		kids=$(descendants_of "$pid" | tr '\n' ' ')
		kill -TERM "$pid"
		deadline=$((SECONDS + 30))
		while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do sleep 0.2; done
		if kill -0 "$pid" 2>/dev/null; then
			echo "osv: serve pid $pid ignored SIGTERM for 30 s; sending SIGKILL" >&2
			kill -KILL "$pid" 2>/dev/null || true
		fi
		for k in $kids; do kill -0 "$k" 2>/dev/null && kill -KILL "$k" 2>/dev/null || true; done
		sleep 0.2
	fi
	for p in $pid $kids; do kill -0 "$p" 2>/dev/null && LEFT="$LEFT $p"; done
	return 0
}

# remove_containers: save the logs of each container with this run's label,
# remove it and its anonymous volumes, and add any that remain to LEFT.
remove_containers() {
	command -v docker >/dev/null || die "docker is not on PATH: cannot remove this run's containers (label $LABEL=$RUN)"
	local ids id name
	ids=$(run_containers) || die "docker ps failed: cannot list this run's containers (label $LABEL=$RUN)"
	for id in $ids; do
		# postgres or seaweedfs, from the image reference.
		name=$(docker inspect -f '{{.Config.Image}}' "$id" 2>/dev/null | sed 's|^.*/||; s|[:@].*||') || true
		docker logs --tail 5000 "$id" >"$EVID/container-${name:-unknown}-${id:0:12}.log" 2>&1 || true
		docker rm -f -v "$id" >/dev/null || true
	done
	ids=$(run_containers) || die "docker ps failed: cannot list this run's containers (label $LABEL=$RUN)"
	for id in $ids; do LEFT="$LEFT container:${id:0:12}"; done
	return 0
}

cmd_cleanup() {
	run_paths "$@"
	case $WORK in "$RUN"/work) ;; *) die "refusing to delete unexpected path $WORK" ;; esac
	trap save_redacted_evidence EXIT
	LEFT=''
	stop_serve "$WORK/serve-b.pid"
	stop_serve "$WORK/serve.pid"
	# The serves are down, so the stores can go. The marker is outside work/,
	# so a repeated cleanup still removes containers after work/ is gone.
	grep -qx mode=fleet "$RUN/.osv-run" && remove_containers
	if [[ -d /proc/self && -d $WORK ]]; then
		local d c
		for d in /proc/[0-9]*; do
			c=$(readlink "$d/cwd" 2>/dev/null || true)
			[[ $c == "$WORK" || $c == "$WORK"/* ]] && LEFT="$LEFT ${d#/proc/}(cwd)"
		done
	fi
	echo "== cleanup $(now)" >>"$EVID/doctor.txt"
	if [[ -n $LEFT ]]; then
		# Keep $WORK: a rerun must still see these processes by their cwd.
		echo "LEFTOVER processes:$LEFT" | tee -a "$EVID/doctor.txt" >&2
		echo "osv: kept $WORK. Check each PID; a shell whose cwd is under $WORK is listed too. Kill only what you started, then run cleanup again" >&2
		return 1
	fi

	trap - EXIT
	save_redacted_evidence
	rm -rf "$WORK"
	echo "no process or container from this run is alive; deleted $WORK" | tee -a "$EVID/doctor.txt"
	echo "evidence: $EVID ($(find "$EVID" -type f | wc -l | tr -d ' ') files)"
	if [[ -f $EVID/log.tsv ]]; then
		awk -F'\t' 'NR > 1 { n[$8]++ } END { printf "checks: %d PASS, %d FAIL, %d unchecked\n", n["PASS"], n["FAIL"], n["-"] }' "$EVID/log.tsv"
	fi
}

sub=${1-}
[[ -n $sub ]] || { sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2; }
shift
case $sub in
	launch | doctor | env | token | api | git | eq | json | packs | stop | cleanup) "cmd_$sub" "$@" ;;
	-h | --help | help) sed -n '2,20p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
	*) die "unknown subcommand '$sub' (try: osv.sh help)" ;;
esac
