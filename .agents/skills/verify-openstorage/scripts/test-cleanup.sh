#!/usr/bin/env bash
# test-cleanup.sh: check what `osv.sh cleanup` (and the commands that append to
# the evidence) do with planted symlinks, unreadable paths, and container logs.
# It needs no build, server, or Docker: each case makes a run directory by
# hand, and a fake `docker` stands in for the real one.
#
#   .agents/skills/verify-openstorage/scripts/test-cleanup.sh
#   OSV=/path/to/other/osv.sh .agents/skills/verify-openstorage/scripts/test-cleanup.sh
#
# Prints one line per case and ends with a count line. Exit 0 when no case
# failed. Cases that need an unreadable file are skipped when the user is root,
# who reads everything. The fake token is made up. The script prints only
# counts of matches, never file contents.
set -uo pipefail
umask 077

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
OSV=${OSV:-$here/osv.sh}
[[ -f $OSV ]] || { echo "test-cleanup: no $OSV" >&2; exit 2; }
T=$(mktemp -d "${TMPDIR:-/tmp}/osv-test-XXXXXX") || exit 2
T=$(cd "$T" && pwd -P)
trap 'chmod -R u+rwx "$T" 2>/dev/null; rm -rf "$T"' EXIT

TOK=osvtest0123456789abcdef
TOKB64=$(printf 't:%s' "$TOK" | base64 | tr -d '\n')
SECV=secretvalue-s3-0123456789
FAKEID=aaaaaaaaaaaabbbbbbbbbbbbccccccccccccddddddddddddeeeeeeeeeeeeffff
VICTIM_TEXT=VICTIM-UNTOUCHED
npass=0 nfail=0 nskip=0 CASE='' WHY=''

mkdir "$T/bin"
cat >"$T/bin/docker" <<'DOCKEREOF'
#!/usr/bin/env bash
# fake docker: one postgres container per run until `rm` is called
case $1 in
	ps) [[ -e $FAKE_DOCKER_STATE/removed ]] || echo "$FAKE_DOCKER_ID" ;;
	inspect) echo 'docker.io/library/postgres:17@sha256:0000' ;;
	logs) echo "postgres log: connection from token $FAKE_DOCKER_TOKEN" ;;
	rm) : >"$FAKE_DOCKER_STATE/removed" ;;
esac
DOCKEREOF
chmod +x "$T/bin/docker"

# mkrun <name> [fleet]: a run directory as `launch` leaves it, holding the fake
# token everywhere cleanup must redact it. Sets RUN and EVID.
mkrun() {
	RUN=$T/$1 EVID=$T/$1/evidence
	mkdir -m 700 "$RUN" && mkdir -p "$RUN/work/tokens" "$RUN/work/tmp" "$RUN/work/secrets" "$EVID" "$RUN/docker" || exit 2
	echo "mode=${2:-local}" >"$RUN/.osv-run"
	printf '%s' "$TOK" >"$RUN/work/tokens/tk"
	printf 'mint --repo r' >"$RUN/work/tokens/tk.desc"
	printf '%s' "$SECV" >"$RUN/work/secrets/s3"
	local f
	for f in serve.out serve.err serve-b.out serve-b.err; do
		printf '%s: token %s basic %s secret %s\n' "$f" "$TOK" "$TOKB64" "$SECV" >"$RUN/work/$f"
	done
	printf 'OSV_MODE=local\nOSV_ADDR=127.0.0.1:1\nOSV_PID=999999\n' >"$RUN/work/state.env"
}

# osv <args...>: run osv.sh with the fake docker first on PATH; output in $T/out, status in RC.
osv() {
	local state=$RUN/docker
	timeout 60 env PATH="$T/bin:$PATH" FAKE_DOCKER_STATE="$state" FAKE_DOCKER_ID=$FAKEID FAKE_DOCKER_TOKEN=$TOK bash "$OSV" "$@" >"$T/out" 2>&1
	RC=$?
}

case_begin() { CASE=$1 WHY=''; }
case_end() {
	if [[ -n $WHY ]]; then printf 'FAIL %s: %s\n' "$CASE" "$WHY"; nfail=$((nfail + 1)); else printf 'PASS %s\n' "$CASE"; npass=$((npass + 1)); fi
}
# want <what> <command...>: record <what> in WHY when the command fails.
want() { local what=$1; shift; "$@" >/dev/null 2>&1 || WHY+="${WHY:+; }$what"; }
rc_is() { [[ $RC == "$1" ]]; }
out_has() { grep -q -- "$1" "$T/out"; }
# raw_count <dir>: files under <dir> that hold the fake token or its Basic form or the secret.
raw_count() { grep -rlsF -e "$TOK" -e "$TOKB64" -e "$SECV" "$1" | wc -l | tr -d ' '; }
redacted() { grep -qF '<token:tk>' "$1"; }
unreadable_works() { [[ $(id -u) != 0 ]]; }
skip() { printf 'SKIP %s: %s\n' "$1" "$2"; nskip=$((nskip + 1)); }

victim_ok() { [[ $(<"$1") == "$VICTIM_TEXT" ]]; }
new_victim() { printf '%s\n' "$VICTIM_TEXT" >"$1"; }

# --- control: a plain cleanup copies and redacts, so the refusals below mean something
case_begin "cleanup copies and redacts when nothing is planted"
mkrun control
osv cleanup "$RUN"
want "exit 0" rc_is 0
want "work/ deleted" test ! -e "$RUN/work"
for f in serve.stdout.log serve.stderr.log serve-b.stdout.log serve-b.stderr.log; do
	want "$f redacted" redacted "$EVID/$f"
done
want "tokens.txt lists tk" grep -qxF 'tk: mint --repo r' "$EVID/tokens.txt"
want "no raw value left" test "$(raw_count "$EVID")" = 0
case_end

# --- a symlink at each fixed evidence name is refused and not written through
targets="serve.stdout.log serve.stderr.log serve-b.stdout.log serve-b.stderr.log state.env tokens.txt doctor.txt"
for kind in symlink dangling; do
	mkrun "plant-$kind"
	for t in $targets; do
		if [[ $kind == symlink ]]; then new_victim "$T/victim-$kind-$t"; fi
		ln -s "$T/victim-$kind-$t" "$EVID/$t"
	done
	osv cleanup "$RUN"
	rc=$RC
	for t in $targets; do
		case_begin "$kind at evidence/$t is refused"
		want "cleanup exit 1" test "$rc" = 1
		want "message names $t" out_has "evidence $EVID/$t "
		if [[ $kind == symlink ]]; then want "victim unchanged" victim_ok "$T/victim-$kind-$t"; else want "no file created at the link target" test ! -e "$T/victim-$kind-$t"; fi
		want "work/ kept" test -d "$RUN/work"
		case_end
	done
	for t in $targets; do rm "$EVID/$t"; done
	osv cleanup "$RUN"
	case_begin "$kind: rerun after removing the links finishes"
	want "exit 0" rc_is 0
	want "work/ deleted" test ! -e "$RUN/work"
	want "no raw value left" test "$(raw_count "$EVID")" = 0
	want "serve.stdout.log redacted" redacted "$EVID/serve.stdout.log"
	case_end
done

case_begin "one refused copy does not stop the other copies or the redaction"
mkrun partial
new_victim "$T/victim-partial"
ln -s "$T/victim-partial" "$EVID/serve.stdout.log"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "victim unchanged" victim_ok "$T/victim-partial"
for f in serve.stderr.log serve-b.stdout.log serve-b.stderr.log; do
	want "$f copied and redacted" redacted "$EVID/$f"
done
want "tokens.txt written" grep -qxF 'tk: mint --repo r' "$EVID/tokens.txt"
want "no raw value left" test "$(raw_count "$EVID")" = 0
case_end

# --- other refusals: hard link, directory, FIFO, symlinked source
case_begin "hard link at evidence/serve.stderr.log is refused"
mkrun hardlink
new_victim "$T/victim-hl"
ln "$T/victim-hl" "$EVID/serve.stderr.log"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "victim unchanged" victim_ok "$T/victim-hl"
case_end

case_begin "directory at evidence/state.env is refused"
mkrun dirtarget
mkdir "$EVID/state.env"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "work/ kept" test -d "$RUN/work"
case_end

case_begin "FIFO at evidence/tokens.txt is refused without a hang"
mkrun fifo
mkfifo "$EVID/tokens.txt"
osv cleanup "$RUN"
want "exit 1 (124 means a hang)" rc_is 1
case_end

case_begin "symlink at work/serve.err (a copy source) is refused"
mkrun srcsym
new_victim "$T/victim-src"
rm "$RUN/work/serve.err"
ln -s "$T/victim-src" "$RUN/work/serve.err"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "nothing copied from the link" test ! -e "$EVID/serve.stderr.log"
case_end

case_begin "symlinked evidence directory is refused"
mkrun evsym
mkdir "$T/elsewhere"
rmdir "$EVID" && ln -s "$T/elsewhere" "$EVID"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "nothing written through the link" test -z "$(ls -A "$T/elsewhere")"
case_end

# --- commands that append to the evidence refuse a symlink too
case_begin "eq refuses a symlink at evidence/log.tsv"
mkrun logsym
new_victim "$T/victim-log"
ln -s "$T/victim-log" "$EVID/log.tsv"
osv eq "$RUN" lbl a a
want "non-zero exit" test "$RC" != 0
want "victim unchanged" victim_ok "$T/victim-log"
case_end

case_begin "stop refuses a symlink at evidence/doctor.txt"
mkrun docsym
new_victim "$T/victim-doc"
ln -s "$T/victim-doc" "$EVID/doctor.txt"
osv stop "$RUN" a
want "non-zero exit" test "$RC" != 0
want "victim unchanged" victim_ok "$T/victim-doc"
case_end

# label files: api, git, and eq create them empty first and refuse a planted link
for cmd in api git eq; do
	for kind in symlink dangling; do
		case_begin "$cmd refuses a $kind at its label file"
		mkrun "label-$cmd-$kind"
		case $cmd in
			api) planted=lbl.headers args=(api "$RUN" lbl GET /api/v1/repos) ;;
			git) planted=lbl.git.txt args=(git "$RUN" lbl -- status) ;;
			eq) planted=lbl.eq.txt args=(eq "$RUN" lbl a a) ;;
		esac
		victim=$T/victim-$cmd-$kind
		[[ $kind == symlink ]] && new_victim "$victim"
		ln -s "$victim" "$EVID/$planted"
		osv "${args[@]}"
		want "non-zero exit" test "$RC" != 0
		if [[ $kind == symlink ]]; then want "victim unchanged" victim_ok "$victim"; else want "no file created at the link target" test ! -e "$victim"; fi
		want "no other label file left behind" test "$(find "$EVID" -type f | wc -l | tr -d ' ')" = 0
		case_end
	done
done

# --- fleet: container logs are staged, copied, redacted, and survive a refusal
case_begin "container log is copied and redacted, and the container is removed"
mkrun fleet fleet
osv cleanup "$RUN"
want "exit 0" rc_is 0
want "container removed" test -e "$RUN/docker/removed"
want "log redacted" redacted "$EVID/container-postgres-${FAKEID:0:12}.log"
want "no raw value left" test "$(raw_count "$EVID")" = 0
case_end

case_begin "symlink at the container log is refused, and the rerun saves the log"
mkrun fleetsym fleet
new_victim "$T/victim-ct"
ln -s "$T/victim-ct" "$EVID/container-postgres-${FAKEID:0:12}.log"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "victim unchanged" victim_ok "$T/victim-ct"
want "container still removed" test -e "$RUN/docker/removed"
want "staged log kept in work/" test -s "$RUN/work/tmp/container-postgres-${FAKEID:0:12}.log"
rm "$EVID/container-postgres-${FAKEID:0:12}.log"
osv cleanup "$RUN"
want "rerun exit 0" rc_is 0
want "rerun saves the log redacted" redacted "$EVID/container-postgres-${FAKEID:0:12}.log"
case_end

# --- the redaction walk redacts every readable file, then fails on the rest
if unreadable_works; then
	case_begin "unreadable evidence paths do not stop the walk"
	mkrun walk
	for i in 1 2 3 4 5 6 7 8 9 10 11 12; do
		mkdir -p "$EVID/r$i"
		printf 'file %s\n' "$TOK" >"$EVID/r$i/f.txt"
		printf 'file %s\n' "$TOKB64" >"$EVID/top$i.txt"
	done
	for i in 1 2 3 4; do
		printf 'file %s\n' "$TOK" >"$EVID/locked$i.txt"
		chmod 000 "$EVID/locked$i.txt"
	done
	mkdir "$EVID/lockeddir"
	printf 'file %s\n' "$TOK" >"$EVID/lockeddir/f.txt"
	chmod 000 "$EVID/lockeddir"
	osv cleanup "$RUN"
	want "exit 1" rc_is 1
	want "message counts 5 paths" out_has '5 evidence path(s) could not be redacted'
	want "work/ kept" test -d "$RUN/work"
	want "every readable file redacted" test "$(raw_count "$EVID")" = 0
	chmod 600 "$EVID"/locked?.txt
	chmod 700 "$EVID/lockeddir"
	osv cleanup "$RUN"
	want "rerun exit 0" rc_is 0
	want "rerun redacts the formerly locked files" test "$(raw_count "$EVID")" = 0
	case_end

	case_begin "unreadable map source does not stop the others"
	mkrun mapsrc
	printf 'extra %s\n' "$SECV" >"$EVID/extra.txt"
	printf '%s' pgvalue-0123456789 >"$RUN/work/secrets/pg"
	chmod 000 "$RUN/work/secrets/pg"
	osv cleanup "$RUN"
	want "exit 1" rc_is 1
	want "message names work/secrets/pg" out_has "work/secrets/pg"
	want "token and s3 secret redacted anyway" test "$(raw_count "$EVID")" = 0
	want "work/ kept" test -d "$RUN/work"
	chmod 600 "$RUN/work/secrets/pg"
	osv cleanup "$RUN"
	want "rerun exit 0" rc_is 0
	case_end
else
	skip "unreadable evidence paths do not stop the walk" "running as root reads every file"
	skip "unreadable map source does not stop the others" "running as root reads every file"
fi

case_begin "symlink at work/secrets/pg is refused and not used as a redaction key"
mkrun mapsym
new_victim "$T/victim-key"
ln -s "$T/victim-key" "$RUN/work/secrets/pg"
printf 'extra %s\n' "$VICTIM_TEXT" >"$EVID/extra.txt"
osv cleanup "$RUN"
want "exit 1" rc_is 1
want "link target text not replaced" grep -qF "$VICTIM_TEXT" "$EVID/extra.txt"
want "victim unchanged" victim_ok "$T/victim-key"
case_end

printf 'test-cleanup: %d passed, %d failed, %d skipped\n' "$npass" "$nfail" "$nskip"
[[ $nfail == 0 ]]
