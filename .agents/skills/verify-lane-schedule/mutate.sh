#!/usr/bin/env bash
# mutate.sh: break each code fact once in the real Go source and confirm
# that check.sh fails with the property SKILL.md's Validation table names.
#
#   .agents/skills/verify-lane-schedule/mutate.sh
#
# Each mutation edits one covered file, checks that its package still
# compiles, runs `check.sh --only <config>`, and restores the file with
# `git checkout`. It prints one line per mutation and a COUNT line, and
# exits 0 only when every mutation failed as expected. Each config it uses
# must first pass unmodified. The covered files must have no uncommitted
# changes; an interrupted run restores the file it broke. Outputs stay in
# one private temporary directory. Needs what check.sh needs (Go, Java).
set -uo pipefail
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
    "$skill_dir/check.sh" --out "$d/tlc" --only "$cfg" >"$d.txt" 2>&1
    if grep -qE "^   $cfg +PASS \(" "$d.txt"; then baseline[$cfg]=ok; else baseline[$cfg]=bad; fi
    [ "${baseline[$cfg]}" = ok ] || echo "BASELINE $cfg does not pass unmodified ($d.txt): its mutations cannot count"
  fi
  [ "${baseline[$cfg]}" = ok ]
}

mut() { # name file old new config expected
  local name=$1 file=$2 old=$3 new=$4 cfg=$5 want=$6
  total=$((total + 1))
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
  local fact; fact=$(grep -E "^   [A-Za-z]+ +FALSE" "$d/check.txt" | awk '{print $1}' | paste -sd, -)
  if [ "$rc" = 1 ] && [ "$fact" = "$name" ] && grep -q "$want" "$d/check.txt"; then
    pass=$((pass + 1)); echo "$name: OK ($want)"
  else
    echo "$name: MISMATCH rc=$rc false_facts=[$fact] output=$d/check.txt"
  fi
}

mut ClaimConsumesPending internal/meta/sqlite_eventing.go 'SET push_syncing = 1, push_pending = 0, push_claim_holder' 'SET push_syncing = 1, push_claim_holder' Liveness.cfg 'Temporal properties were violated'
mut ClaimMarksDue internal/meta/s3walmeta_eventing.go 'attempt, u.PushDue = u.PushAttempt, nowNanos' 'attempt = u.PushAttempt' LaneSchedule.cfg 'Invariant BackoffKept is violated'
mut DoneKeepsPending internal/meta/sqlite_schedule.go 'push_attempt = 0, push_due = 0, sync_pending' 'push_attempt = 0, push_due = 0, push_pending = 0, sync_pending' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut RetryRemarks internal/meta/s3walmeta_schedule.go '				u.PushPending = max(u.PushPending, level)
' '' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut FinishKeepsExplicit internal/meta/sqlite_schedule.go 'push_attempt = push_attempt + 1, push_due = CASE WHEN push_due = 0 THEN 0 ELSE ? END' 'push_attempt = push_attempt + 1, push_due = ?' LaneSchedule.cfg 'Invariant ExplicitNotDelayed is violated'
mut StealInheritsForce internal/meta/s3walmeta_eventing.go 'force = u.PushPending == 2 || (u.PushSyncing && u.PushClaimForce)' 'force = u.PushPending == 2' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut RunPassesForce internal/sync/worker_push.go 'meta.PushOutcome{Kind: meta.OutcomeRestore, Force: force, Delay: shutdownRestoreDelay}' 'meta.PushOutcome{Kind: meta.OutcomeRestore, Delay: shutdownRestoreDelay}' LaneSchedule.cfg 'Invariant ForceKept is violated'
mut WaitBeforeSnapshot internal/sync/worker_push.go '		if err := w.waitPublication(ctx, job.RepoID); err != nil {' '		if err := error(nil); err != nil {' LaneSchedule.cfg 'Invariant NoLostForward is violated'
mut ShutdownRestores internal/sync/worker_push.go '	if runErr != nil && shutdownAborted(ctx) {' '	if runErr != nil && errors.Is(runErr, ErrShutdown) {' LaneSchedule.cfg 'Invariant NoLostForward is violated'
echo "COUNT: $pass/$total mutations failed check.sh as expected (outputs in $work)"
[ "$pass" = "$total" ]
