#!/usr/bin/env bash
# osv.sh: build, launch, drive, and tear down one disposable open.storage node
# (local mode: SQLite + file blobs) for verification. See ../SKILL.md.
#
#   osv.sh launch  <run>                         build, keygen, start serve, run doctor
#   osv.sh doctor  <run>                         read-only health check of this run's node
#   osv.sh env     <run>                         print OSV_* exports: eval "$(osv.sh env <run>)"
#   osv.sh token   <run> <name> <mint flags...>  mint a token and store it under <name>
#   osv.sh token   <run> <name> --raw <value>    store a literal (bad) token under <name>
#   osv.sh api     <run> <label> [--token <name>|none] [--expect <code>] <METHOD> <path> [curl args...]
#   osv.sh git     <run> <label> [--token <name>|none] [--expect <exit>] -- <git args...>
#   osv.sh eq      <run> <label> <expected> <actual>
#   osv.sh json    <file> <dotted.path>          print one JSON field; * maps a list (python3)
#   osv.sh cleanup <run>                         stop serve, delete <run>/work, keep <run>/evidence
#
# <run> is a caller-chosen directory outside the checkout. Scratch state lives in
# <run>/work (deleted by cleanup); proof lives in <run>/evidence (never deleted).
set -Eeuo pipefail
umask 077

ORG=selfhost
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

cmd_launch() {
	[[ $# -eq 1 && -n $1 ]] || die "usage: osv.sh launch <run>"
	# Resolve the path before creating anything, so a refused path leaves no trace.
	local parent
	parent=$(cd "$(dirname "$1")" 2>/dev/null && pwd -P) || die "the parent of $1 does not exist"
	RUN=$parent/$(basename "$1")
	[[ -d $RUN ]] && RUN=$(cd "$RUN" && pwd -P)
	case $RUN/ in "$REPO"/*) die "run dir $RUN is inside the checkout $REPO: pick one outside it so git status stays clean" ;; esac
	[[ ! -e $RUN || -z $(ls -A "$RUN") ]] || die "run dir $RUN is not empty: use a fresh directory for every run"
	for tool in go git curl python3; do command -v "$tool" >/dev/null || die "$tool is not on PATH"; done
	require_git
	safe_parents "$RUN"
	mkdir -p "$RUN"
	chmod 700 "$RUN" || die "cannot chmod 700 $RUN"
	private_dir "$RUN"
	[[ -z $(ls -A "$RUN") ]] || die "run dir $RUN changed while launch prepared it: use a fresh directory"
	WORK=$RUN/work EVID=$RUN/evidence BIN=$RUN/work/bin/openstorage
	: >"$RUN/.osv-run"
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

	# Minimal env: an inherited OPENSTORAGE_* URL would silently override
	# --data-dir, and the caller's HOME would hand serve's git children the
	# caller's global git config.
	local setsid=()
	command -v setsid >/dev/null && setsid=(setsid)
	# shellcheck disable=SC2016 # $$ and $1 expand in the inner bash
	${setsid[@]+"${setsid[@]}"} bash -c 'echo $$ >"$1"; shift; exec "$@"' osv-serve "$WORK/serve.pid" \
		env -i PATH="$PATH" HOME="$WORK/home" TMPDIR="$WORK/tmp" \
		OPENSTORAGE_ORG="$ORG" OPENSTORAGE_ORG_PUBKEYS="$WORK/keys/org.pub.pem" \
		"$BIN" serve --listen 127.0.0.1:0 --data-dir "$WORK/data" \
		>"$WORK/serve.out" 2>"$WORK/serve.err" </dev/null &

	local pid='' addr='' deadline=$((SECONDS + 60))
	while ((SECONDS < deadline)); do
		[[ -z $pid && -s $WORK/serve.pid ]] && pid=$(<"$WORK/serve.pid")
		addr=$(sed -n 's/^listening on //p' "$WORK/serve.out" | head -n 1)
		[[ -n $addr ]] && break
		if [[ -n $pid ]] && ! kill -0 "$pid" 2>/dev/null; then
			tail -n 20 "$WORK/serve.err" >&2
			die "serve exited before it announced its address (stderr above; run 'osv.sh cleanup $RUN')"
		fi
		sleep 0.2
	done
	[[ -n $addr && -n $pid ]] || die "serve did not print 'listening on' within 60 s (run 'osv.sh cleanup $RUN')"
	printf 'OSV_ADDR=%s\nOSV_PID=%s\n' "$addr" "$pid" >"$WORK/state.env"
	printf 'serve pid: %s\nlisten: http://%s\ndata dir: %s\n' "$pid" "$addr" "$WORK/data" >>"$EVID/launch.txt"
	echo "osv: serve pid $pid listening on http://$addr" >&2
	cmd_doctor "$RUN"
}

cmd_doctor() {
	run_paths "$@"
	load_state
	local fails=0 out
	out=$(
		fails=0
		say() { printf '%-5s %-14s %s\n' "$1" "$2" "$3"; [[ $1 != FAIL ]] || fails=$((fails + 1)); }
		port=${OSV_ADDR##*:}

		if is_our_serve "$OSV_PID"; then say PASS process "pid $OSV_PID runs $BIN"; else say FAIL process "pid $OSV_PID is not alive or is not $BIN"; fi

		if command -v ss >/dev/null; then
			if ss -Hltnp "sport = :$port" 2>/dev/null | grep -q "pid=$OSV_PID,"; then say PASS port "$OSV_ADDR is bound by pid $OSV_PID"; else say FAIL port "$OSV_ADDR is not bound by pid $OSV_PID (ss)"; fi
		elif command -v lsof >/dev/null; then
			if lsof -nP -a -p "$OSV_PID" -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then say PASS port "$OSV_ADDR is bound by pid $OSV_PID"; else say FAIL port "$OSV_ADDR is not bound by pid $OSV_PID (lsof)"; fi
		else
			say SKIP port "neither ss nor lsof is installed"
		fi

		body=$(curl -q -sS --noproxy '*' --max-time 10 "$OSV_URL/healthz" 2>&1) || true
		if [[ $body == '{"status":"ok"}' ]]; then say PASS healthz "$OSV_URL/healthz answers $body"; else say FAIL healthz "$OSV_URL/healthz answered: ${body:0:200}"; fi

		auth=$WORK/tmp/doctor.auth
		if tok=$(env -i PATH="$PATH" "$BIN" mint --key "$WORK/keys/org.pem" --iss "$ORG" --repo org --scopes org:read --ttl 5m 2>&1); then
			printf 'Authorization: Bearer %s\n' "$tok" >"$auth"
			code=$(curl -q -s --noproxy '*' --max-time 10 -o /dev/null -w '%{http_code}' -H "@$auth" "$OSV_URL/api/v1/repos") || true
			rm -f "$auth"
			if [[ $code == 200 ]]; then say PASS auth+db "org:read token lists repos (GET /api/v1/repos 200)"; else say FAIL auth+db "GET /api/v1/repos answered $code with an org:read token"; fi
		else
			say FAIL auth+db "mint failed: $tok"
		fi

		head_sha=$(git -C "$REPO" rev-parse HEAD)
		built=$("$BIN" version 2>&1) || true
		if [[ $built == *"commit $head_sha"* ]]; then say PASS build "$built"; else say FAIL build "binary is '$built' but the checkout HEAD is $head_sha: start a new run"; fi
		# Go records uncommitted changes in the build setting vcs.modified=true.
		if go version -m "$BIN" 2>/dev/null | grep -q 'vcs\.modified=true'; then
			say WARN build "vcs.modified=true: built from a checkout with uncommitted changes"
		fi

		if [[ -f $WORK/data/codestorage.db ]]; then say PASS data-dir "$WORK/data (sqlite + file blobs)"; else say FAIL data-dir "$WORK/data/codestorage.db is missing"; fi
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
	printf 'export OSV_RUN=%q OSV_URL=%q OSV_BIN=%q OSV_WORK=%q OSV_EVIDENCE=%q OSV_ORG=%q\n' \
		"$RUN" "$OSV_URL" "$BIN" "$WORK" "$EVID" "$ORG"
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
	TOKEN=none EXPECT=''
	NSHIFT=0
	while [[ $# -gt 0 ]]; do
		case $1 in
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
	[[ $# -ge 2 ]] || die "usage: osv.sh api <run> <label> [--token <name>|none] [--expect <code>] <METHOD> <path> [curl args...]"
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
	record "$label" api "$method $path" "$TOKEN" "$code" "${EXPECT:--}" "$verdict"
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
	[[ -f $WORK/state.env ]] && cp "$WORK/state.env" "$EVID/state.env"
	local t
	for t in "$WORK"/tokens/*.desc; do
		[[ -e $t ]] || continue
		printf '%s: %s\n' "$(basename "$t" .desc)" "$(<"$t")"
	done >"$EVID/tokens.txt"
	# Replace every stored token value with <token:name> in the evidence, and
	# its Basic-auth form base64("t:<token>"), which curl -v prints in a
	# failed call's stderr that cmd_api keeps.
	python3 - "$WORK/tokens" "$EVID" <<'PYEOF'
import base64, os, stat, sys
tokdir, evid = sys.argv[1], sys.argv[2]
toks = {}
for n in os.listdir(tokdir):
    if not n.endswith(".desc"):
        v = open(os.path.join(tokdir, n), "rb").read()
        if len(v) >= 8:
            toks[base64.b64encode(b"t:" + v)] = b"<basic:" + n.encode() + b">"
            toks[v] = b"<token:" + n.encode() + b">"
for d, _, names in os.walk(evid):
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

cmd_cleanup() {
	run_paths "$@"
	case $WORK in "$RUN"/work) ;; *) die "refusing to delete unexpected path $WORK" ;; esac
	local pid='' kids='' left=''
	[[ -s $WORK/serve.pid ]] && pid=$(<"$WORK/serve.pid")
	if [[ -n $pid ]] && kill -0 "$pid" 2>/dev/null; then
		is_our_serve "$pid" || { save_redacted_evidence; die "pid $pid is alive but does not run $BIN: refusing to signal it"; }
		kids=$(descendants_of "$pid" | tr '\n' ' ')
		kill -TERM "$pid"
		local deadline=$((SECONDS + 30))
		while kill -0 "$pid" 2>/dev/null && ((SECONDS < deadline)); do sleep 0.2; done
		if kill -0 "$pid" 2>/dev/null; then
			echo "osv: serve pid $pid ignored SIGTERM for 30 s; sending SIGKILL" >&2
			kill -KILL "$pid" 2>/dev/null || true
		fi
		local k
		for k in $kids; do kill -0 "$k" 2>/dev/null && kill -KILL "$k" 2>/dev/null || true; done
		sleep 0.2
	fi
	local p
	for p in $pid $kids; do kill -0 "$p" 2>/dev/null && left="$left $p"; done
	if [[ -d /proc/self && -d $WORK ]]; then
		local d c
		for d in /proc/[0-9]*; do
			c=$(readlink "$d/cwd" 2>/dev/null || true)
			[[ $c == "$WORK" || $c == "$WORK"/* ]] && left="$left ${d#/proc/}(cwd)"
		done
	fi
	echo "== cleanup $(now)" >>"$EVID/doctor.txt"
	save_redacted_evidence
	if [[ -n $left ]]; then
		# Keep $WORK: a rerun must still see these processes by their cwd.
		echo "LEFTOVER processes:$left" | tee -a "$EVID/doctor.txt" >&2
		echo "osv: kept $WORK. Check each PID; a shell whose cwd is under $WORK is listed too. Kill only what you started, then run cleanup again" >&2
		return 1
	fi

	rm -rf "$WORK"
	echo "no process from this run is alive; deleted $WORK" | tee -a "$EVID/doctor.txt"
	echo "evidence: $EVID ($(find "$EVID" -type f | wc -l | tr -d ' ') files)"
	if [[ -f $EVID/log.tsv ]]; then
		awk -F'\t' 'NR > 1 { n[$8]++ } END { printf "checks: %d PASS, %d FAIL, %d unchecked\n", n["PASS"], n["FAIL"], n["-"] }' "$EVID/log.tsv"
	fi
}

sub=${1-}
[[ -n $sub ]] || { sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2; }
shift
case $sub in
	launch | doctor | env | token | api | git | eq | json | cleanup) "cmd_$sub" "$@" ;;
	-h | --help | help) sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' ;;
	*) die "unknown subcommand '$sub' (try: osv.sh help)" ;;
esac
