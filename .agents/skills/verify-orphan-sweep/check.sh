#!/usr/bin/env bash
# check.sh: model-check the orphan-pack sweeper against pack writers, and
# the repository purger against writers, a second purger and the sweeper.
#
#   .agents/skills/verify-orphan-sweep/check.sh [--out DIR] [--only CFG]... [--record]
#   .agents/skills/verify-orphan-sweep/check.sh --fingerprints-only
#
# 1. Reads the code facts and fingerprints from the Go source (facts/facts.go).
# 2. Runs TLC on each config in model/ against its module (PackSweep.tla or
#    RepoPurge.tla), the module's facts written into the config.
# 3. Compares the fingerprints with model/fingerprints.txt.
#
# Exit codes:
#   0  every property holds, every probe fails as expected, no covered code
#      changed since the sync commit
#   1  a property failed: NoLostPack (PackSweep.cfg), NoWaitForHeal
#      (NoTiming.cfg), BytesComeBack (Liveness.cfg), or a RepoPurge property
#      (Purge*.cfg). Read the trace in --out.
#   2  STALE: the properties hold, but covered code changed since the sync
#      commit, or a probe stopped failing. Review the model against the code.
#   3  the check could not run (Go, Java, TLC download, parse error)
#
# --only CFG runs only that config from model/ (repeatable). A partial run
# proves nothing about the other configs: finish with a full run.
#
# --fingerprints-only skips TLC and Java: it checks the facts and the
# fingerprints only, and exits 0, 2 or 3. CI runs it. A pass says the covered
# code is the code the model was checked against, not that the model holds.
#
# --record rewrites model/fingerprints.txt and the sync commit in SKILL.md
# to the current code. Use it only after you confirmed that the model
# describes the code at HEAD. It refuses unless the properties hold, every
# probe fails, and the covered files have no uncommitted changes. The
# sync commit is the merge base with origin/main (OPENSTORAGE_MAIN_REF)
# when the covered files match it, so a squash merge does not orphan it.
#
# Environment: JAVA (default java, needs 11+), GO (default go), OPENSTORAGE_MAIN_REF,
# TLC_WORKERS (default 4), TLC_HEAP (default 4g),
# OPENSTORAGE_TLA_CACHE (default ${XDG_CACHE_HOME:-$HOME/.cache}/openstorage-tla).
set -uo pipefail
export LC_ALL=C

TLC_VERSION=1.7.4
TLC_SHA256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
TLC_URL="https://github.com/tlaplus/tlaplus/releases/download/v${TLC_VERSION}/tla2tools.jar"

skill_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)
repo=$(git -C "$skill_dir" rev-parse --show-toplevel 2>/dev/null) || {
  echo "check.sh: not inside a git checkout" >&2; exit 3; }
model="$skill_dir/model"

# Configs in run order: name, module, kind (pass: invariants must hold; live:
# temporal properties must hold; probe: the named invariant must fail
# without a timing assumption; reach: the named invariant must fail because
# it negates a state the model must reach).
configs="PackSweep.cfg PackSweep pass -
NoTiming.cfg PackSweep pass -
Liveness.cfg PackSweep live -
NoThresholdAssumption.cfg PackSweep probe NoLostPack
SlowHealEntry.cfg PackSweep probe NoLostPack
PurgeControl.cfg RepoPurge pass -
PurgeSafety.cfg RepoPurge pass -
PurgeLiveness.cfg RepoPurge live -
PurgeQuarantine.cfg RepoPurge live -
PurgeReach.cfg RepoPurge reach RaceUnreachable
PurgeCliReach.cfg RepoPurge reach CliUnreachable"
# The code facts each module declares as constants.
facts_PackSweep="WriterVerifies VerifyOnRegisterError CopyBeforeDelete RecheckAfterDelete HealEverySweep HealRestores PurgeAgeGated PurgeFloorCoversHandshake PurgeAgeCoversThreshold"
facts_RepoPurge="WriterVerifies VerifyOnRegisterError CopyBeforeDelete RecheckAfterDelete HealRestores PurgeAgeGated PurgeFloorCoversHandshake PurgeGuarded SweepGuardStopsPurgeMode RefusesLive RefusesYoung CutoffCoversRetention CursorWraps FenceFirst RefsDieFirst RegistrationsFenced RefWritesFenced FoldsFenced SweepReapsFenced HealPurgesFenced"
cfg_names=$(awk '{print $1}' <<<"$configs")

out="" record=0 only=" " fp_only=0
while [ $# -gt 0 ]; do
  case $1 in
    --out) [ $# -ge 2 ] || { echo "check.sh: --out needs a directory" >&2; exit 3; }
           out=$2; shift 2 ;;
    --record) record=1; shift ;;
    --fingerprints-only) fp_only=1; shift ;;
    --only) [ $# -ge 2 ] || { echo "check.sh: --only needs a config name" >&2; exit 3; }
            grep -qxF -- "$2" <<<"$cfg_names" || {
              echo "check.sh: --only takes one of: $(tr '\n' ' ' <<<"$cfg_names")" >&2; exit 3; }
            only="$only$2 "; shift 2 ;;
    -h|--help) awk 'NR > 1 && /^set -uo pipefail/ { exit } NR > 1' "$0"; exit 0 ;;
    *) echo "check.sh: unknown argument $1" >&2; exit 3 ;;
  esac
done
if [ "$fp_only" = 1 ] && { [ "$record" = 1 ] || [ "$only" != " " ]; }; then
  echo "check.sh: --fingerprints-only runs no TLC, so it takes no --record or --only" >&2; exit 3
fi
tmp_out=""
if [ -z "$out" ]; then
  out=$(mktemp -d "${TMPDIR:-/tmp}/orphan-sweep-check.XXXXXX") || exit 3
  [ "$fp_only" = 1 ] && tmp_out=$out
else
  mkdir -p "$out" || exit 3
  # A reused directory would keep an old config's trace next to a new PASS.
  [ -z "$(ls -A "$out")" ] || { echo "check.sh: --out $out is not empty: use a new directory for every run" >&2; exit 3; }
fi
out=$(cd "$out" && pwd -P)

work=$(mktemp -d "${TMPDIR:-/tmp}/orphan-sweep-work.XXXXXX") || exit 3
trap 'rm -rf "$work" ${tmp_out:+"$tmp_out"}' EXIT

die() { echo "check.sh: $*" >&2; exit 3; }

# --- 1. Facts and fingerprints from the code --------------------------------
GO=${GO:-go}
command -v "$GO" >/dev/null || die "needs Go (set GO=/path/to/go)"
if ! (cd "$repo" && "$GO" run "$skill_dir/facts/facts.go" -root "$repo") >"$out/facts.txt" 2>"$out/facts.err"; then
  cat "$out/facts.err" >&2
  die "facts.go failed (a covered function moved or was renamed?)"
fi
# Every fact facts.go prints belongs to some module, and every module's
# facts are printed.
want=$(tr ' ' '\n' <<<"$facts_PackSweep $facts_RepoPurge" | sort -u)
got=$(awk '$1 == "FACT" {print $2}' "$out/facts.txt" | sort)
[ "$want" = "$got" ] || die "facts.go printed facts [$(tr '\n' ' ' <<<"$got")], expected [$(tr '\n' ' ' <<<"$want")]"
for module in PackSweep RepoPurge; do
  names="facts_$module"
  {
    echo "\\* Code facts from facts/facts.go at $(git -C "$repo" rev-parse --short HEAD)"
    echo "CONSTANTS"
    for f in ${!names}; do
      awk -v f="$f" '$1 == "FACT" && $2 == f { printf "    %s = %s\n", $2, $3 }' "$out/facts.txt"
    done
  } >"$work/facts-$module.cfg"
done
echo "== Code facts (HEAD $(git -C "$repo" rev-parse --short HEAD))"
false_facts=0
while read -r _ name value evidence; do
  if [ "$value" = TRUE ]; then
    printf '   %-26s TRUE\n' "$name"
  else
    printf '   %-26s FALSE  <- %s\n' "$name" "$evidence"
    false_facts=$((false_facts + 1))
  fi
done < <(grep '^FACT ' "$out/facts.txt")

# --- 2. TLC -----------------------------------------------------------------
if [ "$fp_only" = 0 ]; then
  JAVA=${JAVA:-java}
  command -v "$JAVA" >/dev/null || die "needs Java 11+ (set JAVA=/path/to/java)"
  jv=$("$JAVA" -version 2>&1 | sed -n 's/.*version "\([0-9][0-9]*\)[."].*/\1/p' | head -n 1)
  [ -n "$jv" ] && [ "$jv" -ge 11 ] || die "needs Java 11+, found: $("$JAVA" -version 2>&1 | head -n 1)"

  cache=${OPENSTORAGE_TLA_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/openstorage-tla}
  jar="$cache/tla2tools-$TLC_VERSION.jar"
  sha256() { if command -v sha256sum >/dev/null; then sha256sum "$1"; else shasum -a 256 "$1"; fi | awk '{print $1}'; }
  if [ ! -f "$jar" ]; then
    mkdir -p "$cache" || die "cannot create $cache"
    echo "== Downloading TLC $TLC_VERSION to $cache"
    curl -fsSL -o "$jar.tmp.$$" "$TLC_URL" || die "download failed: $TLC_URL"
    mv "$jar.tmp.$$" "$jar"
  fi
  [ "$(sha256 "$jar")" = "$TLC_SHA256" ] || die "$jar does not match sha256 $TLC_SHA256; delete it and rerun"
fi

# run_tlc CFG MODULE WORKERS EXTRA_ARGS... -> exit code of TLC; output in $out/CFG.out
run_tlc() {
  local cfg=$1 module=$2 workers=$3; shift 3
  local d="$work/${cfg%.cfg}"
  mkdir -p "$d/tmp"
  cp "$model/$module.tla" "$d/"
  cat "$model/$cfg" "$work/facts-$module.cfg" >"$d/run.cfg"
  cp "$d/run.cfg" "$out/${cfg%.cfg}.run.cfg"
  local rc=0
  (cd "$d" && "$JAVA" -Djava.io.tmpdir="$d/tmp" -Xmx"${TLC_HEAP:-4g}" -XX:+UseParallelGC \
     -cp "$jar" tlc2.TLC -workers "$workers" -cleanup -metadir "$d/meta" "$@" \
     -config run.cfg "$module.tla") </dev/null >"$out/${cfg%.cfg}.out" 2>&1 || rc=$?
  return "$rc"
}

# Print a TLC trace as "step: action" plus the variables that changed.
summarize() {
  awk '
    /^Back to state [0-9]+:/ { split($0, h, " line "); back = h[1] ">"; next }
    /^State [0-9]+:/ { n++; split($0, h, " line "); act[n] = h[1] (h[2] != "" ? ">" : ""); cur = ""; next }
    n && /^\/\\ / { sub(/^\/\\ /, ""); cur = $1; val[n, cur] = substr($0, index($0, "=") + 2); names[cur] = 1; next }
    n && cur != "" && /^   / { gsub(/^ +/, " "); val[n, cur] = val[n, cur] $0; next }
    END {
      for (i = 1; i <= n; i++) {
        print act[i]
        for (v in names) if (i == 1 || val[i, v] != val[i - 1, v]) printf "      %s = %s\n", v, val[i, v]
      }
      if (back != "") print back "  (the behavior loops from here forever)"
    }' "$1"
}

status=0
selected() { [ "$fp_only" = 0 ] || return 1; [ "$only" = " " ] || case $only in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
verdict() { printf '   %-27s %s\n' "$1" "$2"; }
if [ "$fp_only" = 1 ]; then echo "== TLC skipped (--fingerprints-only)"; else echo "== TLC $TLC_VERSION (outputs in $out)"; fi
while read -r cfg module kind _; do
  case $kind in probe|reach) continue ;; esac
  selected "$cfg" || continue
  extra=()
  [ "$kind" = live ] && extra=(-lncheck final)
  rc=0; run_tlc "$cfg" "$module" "${TLC_WORKERS:-4}" ${extra[@]+"${extra[@]}"} || rc=$?
  states=$(grep -o '[0-9,]* distinct states found, 0 states left' "$out/${cfg%.cfg}.out" | tail -n 1 | cut -d' ' -f1)
  case $rc in
    0) verdict "$cfg" "PASS ($states distinct states)" ;;
    12|13)
      verdict "$cfg" "FAIL: $(grep -m1 -E 'Invariant .* is violated|Temporal properties were violated' "$out/${cfg%.cfg}.out")"
      summarize "$out/${cfg%.cfg}.out" >"$out/${cfg%.cfg}.trace.txt"
      echo "      trace: $out/${cfg%.cfg}.trace.txt"
      status=1 ;;
    *) verdict "$cfg" "ERROR: TLC exit $rc"; sed -n '1,40p' "$out/${cfg%.cfg}.out" >&2; exit 3 ;;
  esac
done <<<"$configs"

# Probes drop one timing assumption each, and reach probes negate a state
# the model must reach; either way the named invariant must fail. One worker
# keeps the trace the shortest one.
stale=0 probe_passed=0
[ "$false_facts" -eq 0 ] || stale=1
while read -r cfg module kind expect; do
  case $kind in probe|reach) ;; *) continue ;; esac
  selected "$cfg" || continue
  rc=0; run_tlc "$cfg" "$module" 1 || rc=$?
  case $rc in
    12)
      if grep -q "Invariant $expect is violated" "$out/${cfg%.cfg}.out"; then
        summarize "$out/${cfg%.cfg}.out" >"$out/${cfg%.cfg}.trace.txt"
        verdict "$cfg" "fails as expected (trace: $out/${cfg%.cfg}.trace.txt)"
      else
        verdict "$cfg" "UNEXPECTED: $(grep -m1 'is violated' "$out/${cfg%.cfg}.out")"; status=1
      fi ;;
    0)
      if [ "$kind" = reach ]; then
        verdict "$cfg" "PASSES: the model no longer reaches this state, so the other $module configs may check nothing. Review the model"
      else
        verdict "$cfg" "PASSES: this timing assumption no longer matters. Review the model and SKILL.md"
      fi
      stale=1; probe_passed=1 ;;
    *) verdict "$cfg" "ERROR: TLC exit $rc"; sed -n '1,40p' "$out/${cfg%.cfg}.out" >&2; exit 3 ;;
  esac
done <<<"$configs"

# --- 3. Fingerprints --------------------------------------------------------
# shellcheck disable=SC2016 # the backticks are literal Markdown
sync=$(sed -n 's/^Sync commit: `\([0-9a-f]\{7,40\}\)`.*/\1/p' "$skill_dir/SKILL.md" 2>/dev/null | head -n 1)
grep '^FP ' "$out/facts.txt" | awk '{print $2, $3}' | sort >"$out/fingerprints.now"
grep -v '^#' "$model/fingerprints.txt" | sort >"$work/fingerprints.rec"
echo "== Coverage (sync commit ${sync:-unknown})"
changed=$(join -a1 -a2 -e MISSING -o 0,1.2,2.2 "$work/fingerprints.rec" "$out/fingerprints.now" | awk '$2 != $3 {print $1}')
if [ -n "$changed" ]; then
  stale=1
  echo "   STALE: these covered functions changed since the sync commit:"
  while read -r f; do echo "     $f"; done <<<"$changed"
  echo "   Read their diff (git diff ${sync:-<sync>} -- <file>) and update the model if it no longer describes them."
  echo "   Then run .agents/skills/verify-orphan-sweep/check.sh --record (needs Java) and commit"
  echo "   model/fingerprints.txt and SKILL.md. See \"Bring the model in line\" in SKILL.md."
else
  echo "   every covered function matches model/fingerprints.txt"
fi

if [ "$record" = 1 ]; then
  files=$(cut -d: -f1 "$out/fingerprints.now" | sort -u)
  # shellcheck disable=SC2086 # $files holds one path per word
  if [ "$status" -ne 0 ] || [ "$false_facts" -ne 0 ] || [ "$only" != " " ] || [ "$probe_passed" -ne 0 ]; then
    echo "check.sh: --record refused: a property failed, a fact is FALSE, a probe passed, or --only was given" >&2; exit 3
  elif ! git -C "$repo" diff --quiet HEAD -- $files; then
    echo "check.sh: --record refused: commit the covered files first (the fingerprints must match a commit)" >&2; exit 3
  fi
  # shellcheck disable=SC2016 # the backticks are literal Markdown
  [ "$(grep -c '^Sync commit: `[0-9a-f]*`' "$skill_dir/SKILL.md")" = 1 ] ||
    die "--record refused: SKILL.md must have exactly one line that starts with Sync commit: \`<sha>\`"
  # The sync commit must survive a squash merge: use the merge base with
  # main when the covered files are the same there, else HEAD with a warning.
  head=$(git -C "$repo" rev-parse HEAD)
  main=${OPENSTORAGE_MAIN_REF:-origin/main}
  # shellcheck disable=SC2086 # $files holds one path per word
  if mb=$(git -C "$repo" merge-base HEAD "$main" 2>/dev/null) &&
     git -C "$repo" diff --quiet "$mb" HEAD -- $files; then
    head=$mb
  else
    echo "check.sh: covered code differs from $main; recording HEAD. Re-run --record after the merge." >&2
  fi
  { echo "# Token fingerprints of the code model/PackSweep.tla and model/RepoPurge.tla describe (facts/facts.go)."
    echo "# check.sh --record rewrites this file; SKILL.md holds the sync commit."
    cat "$out/fingerprints.now"; } >"$model/fingerprints.txt"
  sed -i.bak "s/^Sync commit: \`[0-9a-f]*\`/Sync commit: \`$head\`/" "$skill_dir/SKILL.md" && rm -f "$skill_dir/SKILL.md.bak"
  grep -q "^Sync commit: \`$head\`" "$skill_dir/SKILL.md" || die "could not write the sync commit into SKILL.md"
  echo "== Recorded fingerprints and sync commit $head"
  stale=0
fi

if [ "$status" -ne 0 ]; then
  echo "RESULT: FAIL (a property broke; read the trace before you touch the model)"; exit 1
elif [ "$stale" -ne 0 ]; then
  if [ "$fp_only" = 1 ]; then
    echo "RESULT: STALE (TLC not run; run .agents/skills/verify-orphan-sweep/check.sh without --fingerprints-only)"; exit 2
  fi
  echo "RESULT: STALE (properties hold on the model; the model may not describe the code)"; exit 2
fi
if [ "$fp_only" = 1 ]; then
  echo "RESULT: PASS for facts and fingerprints only (TLC not run)"; exit 0
fi
if [ "$only" != " " ]; then
  echo "RESULT: PASS for the selected configs only:$only"; exit 0
fi
echo "RESULT: PASS"
