#!/usr/bin/env bash
# mutate.sh: break each code fact once in the real Go source and confirm
# that check.sh fails with the property SKILL.md's Validation table names.
#
#   .agents/skills/verify-lane-schedule/mutate.sh [--model]
#
# Each mutation edits one covered file, checks that its package still
# compiles, runs `check.sh --only <config>`, and restores the file with
# `git checkout`. It prints one line per mutation and a COUNT line, and
# exits 0 only when every mutation failed as expected, and 3 when check.sh
# cannot run at all (Go, Java, TLC download). Each config it uses
# must first pass unmodified. The covered files must have no uncommitted
# changes; an interrupted run restores the file it broke. Outputs stay in
# one private temporary directory.
#
# --model edits no code: it sets each fact FALSE in the model only
# (`check.sh --fact <fact>=FALSE`) and expects the same property to fail.
#
# Needs what check.sh needs (Go, Java 11+, curl for the first TLC download),
# plus git, bash 4 or newer, and python3 (not for --model). Takes about 2 to
# 5 minutes, most of it the unmodified baseline runs.
set -uo pipefail
[ "${BASH_VERSINFO[0]}" -ge 4 ] || { echo "mutate.sh: needs bash 4 or newer" >&2; exit 3; }
model=0
case ${1-} in
  "") ;;
  --model) model=1 ;;
  *) echo "mutate.sh: unknown argument $1 (takes only --model)" >&2; exit 3 ;;
esac
if [ "$model" = 0 ] && ! python3 -c '' >/dev/null 2>&1; then
  echo "mutate.sh: needs a working python3 to edit the Go source" >&2; exit 3
fi
skill_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo=$(git -C "$skill_dir" rev-parse --show-toplevel) || exit 3
cd "$repo" || exit 3
GO=${GO:-go}
pass=0 total=0
work=$(mktemp -d "${TMPDIR:-/tmp}/lane-mutate.XXXXXX") || exit 3
active=""
# An interrupted run (Ctrl-C, a CI timeout) must not leave a broken file.
restore() { if [ -n "$active" ]; then git checkout -q -- "$active"; active=""; fi; }
trap 'restore; exit 130' INT TERM
trap 'restore' EXIT

# Each config must pass unmodified, so a failure below is the mutation's.
declare -A baseline=()
base_ok() { # config
  local cfg=$1
  if [ -z "${baseline[$cfg]:-}" ]; then
    local d="$work/base-${cfg%.cfg}"
    local rc=0; "$skill_dir/check.sh" --out "$d/tlc" --only "$cfg" >"$d.txt" 2>&1 || rc=$?
    if [ "$rc" = 3 ]; then # a tool problem, not the model: nothing below can count
      local why; why=$(grep -m1 -E '^(check\.sh: |Error: )' "$d.txt" || tail -n 1 "$d.txt")
      echo "mutate.sh: check.sh could not run on unmodified $cfg (exit 3, $d.txt): $why" >&2
      exit 3
    fi
    if grep -qE "^   $cfg +PASS \(" "$d.txt"; then baseline[$cfg]=ok; else baseline[$cfg]=bad; fi
    [ "${baseline[$cfg]}" = ok ] || echo "BASELINE $cfg does not pass unmodified ($d.txt): its mutations cannot count"
  fi
  [ "${baseline[$cfg]}" = ok ]
}

mut() { # name file old new config expected
  local name=$1 file=$2 old=$3 new=$4 cfg=$5 want=$6
  total=$((total + 1))
  [ -n "${baseline[$cfg]:-}" ] || { echo "mutate.sh: $cfg is not in the baseline list below; add it" >&2; exit 3; }
  if [ "$model" = 1 ]; then
    base_ok "$cfg" || { echo "$name: ERROR baseline $cfg fails"; return; }
    local d="$work/$name"; mkdir -p "$d"
    local rc=0; "$skill_dir/check.sh" --out "$d/tlc" --only "$cfg" --fact "$name=FALSE" >"$d/check.txt" 2>&1 || rc=$?
    judge "$name" "$rc" "$want" "$d/check.txt"; return
  fi
  if ! git diff --quiet HEAD -- "$file"; then echo "$name: ERROR $file has uncommitted changes"; return; fi
  base_ok "$cfg" || { echo "$name: ERROR baseline $cfg fails"; return; }
  local d="$work/$name"; mkdir -p "$d"
  active=$file
  if ! OLD="$old" NEW="$new" python3 -c 'import os,sys; p=sys.argv[1]; s=open(p).read(); o=os.environ["OLD"]; assert s.count(o)==1, s.count(o); open(p,"w").write(s.replace(o,os.environ["NEW"]))' "$file"; then
    echo "$name: ERROR the mutation text no longer matches $file exactly once (update mutate.sh)"; restore; return
  fi
  if ! "$GO" build "./${file%/*}" >"$d/build.txt" 2>&1; then
    echo "$name: ERROR the broken $file does not compile ($d/build.txt)"; restore; return
  fi
  local rc=0; "$skill_dir/check.sh" --out "$d/tlc" --only "$cfg" >"$d/check.txt" 2>&1 || rc=$?
  restore
  judge "$name" "$rc" "$want" "$d/check.txt"
}

judge() { # name rc expected output: exactly this fact FALSE, and the property failed
  local fact; fact=$(grep -E "^   [A-Za-z]+ +FALSE" "$4" | awk '{print $1}' | paste -sd, -)
  if [ "$2" = 1 ] && [ "$fact" = "$1" ] && grep -q "$3" "$4"; then
    pass=$((pass + 1)); echo "$1: OK ($3)"
  else
    echo "$1: MISMATCH rc=$2 false_facts=[$fact] output=$4"
  fi
}

# Every config the table uses, run unmodified before any break, so a tool
# error (exit 3) stops the run before it edits code.
for cfg in Liveness.cfg LaneSchedule.cfg OtherWriters.cfg; do base_ok "$cfg" || true; done

mut ClaimConsumesPending internal/meta/sqlite_eventing.go 'SET push_syncing = 1, push_pending = 0, push_claim_holder' 'SET push_syncing = 1, push_claim_holder' Liveness.cfg 'Temporal properties were violated'
mut ClaimMarksDue internal/meta/s3walmeta_eventing.go 'attempt, u.PushDue = u.PushAttempt, nowNanos' 'attempt = u.PushAttempt' LaneSchedule.cfg 'Invariant BackoffKept is violated'
mut DoneKeepsPending internal/meta/sqlite_schedule.go 'push_attempt = 0, push_due = 0, sync_pending' 'push_attempt = 0, push_due = 0, push_pending = 0, sync_pending' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut RetryRemarks internal/meta/s3walmeta_schedule.go '				u.PushPending = max(u.PushPending, level)
' '' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut FinishKeepsExplicit internal/meta/sqlite_schedule.go 'push_attempt = push_attempt + 1, push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END' 'push_attempt = push_attempt + 1, push_due = ?' LaneSchedule.cfg 'Invariant ExplicitNotDelayed is violated'
mut StealInheritsForce internal/meta/s3walmeta_eventing.go 'force = u.PushPending == 2 || (u.PushSyncing && u.PushClaimForce)' 'force = u.PushPending == 2' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut RunPassesForce internal/sync/worker_push.go 'meta.PushOutcome{Kind: meta.OutcomeRestore, Force: force, Delay: shutdownRestoreDelay}' 'meta.PushOutcome{Kind: meta.OutcomeRestore, Delay: shutdownRestoreDelay}' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut WaitBeforeSnapshot internal/sync/worker_push.go '	if err := w.waitPublication(ctx, job.RepoID); err != nil {' '	if err := error(nil); err != nil {' LaneSchedule.cfg 'Invariant NoLostForward is violated'
mut ShutdownRestores internal/sync/worker_push.go '	if runErr != nil && shutdownAborted(ctx) {' '	if runErr != nil && errors.Is(runErr, ErrShutdown) {' LaneSchedule.cfg 'Invariant NoLostForward is violated'
mut MarkBeforeCas internal/events/refstore.go '	if err := s.runBeforeCAS(ctx, repo, name); err != nil {
		return false, 0, err
	}
	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, new)
	if err != nil || !won {
		return won, gen, err
	}
' '	won, gen, err := s.RefStore.CASRefGen(ctx, repo, name, old, new)
	if err != nil || !won {
		return won, gen, err
	}
	if err := s.runBeforeCAS(ctx, repo, name); err != nil {
		return false, 0, err
	}
' LaneSchedule.cfg 'Invariant NoLostForward is violated'
mut StrictSnapshot internal/githttp/hydrate.go 'if refsErr == nil && skippedHeld && strictHeld {' 'if refsErr == nil && skippedHeld && strictHeld && false {' OtherWriters.cfg 'Invariant NoLostForward is violated'
echo "COUNT: $pass/$total mutations failed check.sh as expected (outputs in $work)"
[ "$pass" = "$total" ]
