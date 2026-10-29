#!/usr/bin/env bash
# check.sh: model-check the orphan-pack sweeper against pack writers.
#
#   .agents/skills/verify-orphan-sweep/check.sh [--out DIR] [--only CFG]... [--record]
#
# 1. Reads the code facts and fingerprints from the Go source (facts/facts.go).
# 2. Runs TLC on model/PackSweep.tla with each config in model/, the facts
#    written into the config.
# 3. Compares the fingerprints with model/fingerprints.txt.
#
# Exit codes:
#   0  every property holds, both probes fail as expected, no covered code
#      changed since the sync commit
#   1  a property failed: NoLostPack (PackSweep.cfg), NoWaitForHeal
#      (NoTiming.cfg) or BytesComeBack (Liveness.cfg). Read the trace in --out.
#   2  STALE: the properties hold, but covered code changed since the sync
#      commit, or a probe stopped failing. Review the model against the code.
#   3  the check could not run (Go, Java, TLC download, parse error)
#
# --only CFG runs only that config from model/ (repeatable). A partial run
# proves nothing about the other configs: finish with a full run.
#
# --record rewrites model/fingerprints.txt and the sync commit in SKILL.md
# to the current code. Use it only after you confirmed that the model
# describes the code at HEAD. It refuses unless the properties hold, both
# probes fail, and the covered files have no uncommitted changes. The
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

out="" record=0 only=" "
while [ $# -gt 0 ]; do
  case $1 in
    --out) [ $# -ge 2 ] || { echo "check.sh: --out needs a directory" >&2; exit 3; }
           out=$2; shift 2 ;;
    --record) record=1; shift ;;
    --only) [ $# -ge 2 ] || { echo "check.sh: --only needs a config name" >&2; exit 3; }
            case $2 in
              PackSweep.cfg|NoTiming.cfg|Liveness.cfg|NoThresholdAssumption.cfg|SlowHealEntry.cfg) ;;
              *) echo "check.sh: --only takes PackSweep.cfg, NoTiming.cfg, Liveness.cfg, NoThresholdAssumption.cfg or SlowHealEntry.cfg" >&2; exit 3 ;;
            esac
            only="$only$2 "; shift 2 ;;
    -h|--help) awk 'NR > 1 && /^set -uo pipefail/ { exit } NR > 1' "$0"; exit 0 ;;
    *) echo "check.sh: unknown argument $1" >&2; exit 3 ;;
  esac
done
if [ -z "$out" ]; then
  out=$(mktemp -d "${TMPDIR:-/tmp}/orphan-sweep-check.XXXXXX") || exit 3
else
  mkdir -p "$out" || exit 3
fi
out=$(cd "$out" && pwd -P)

work=$(mktemp -d "${TMPDIR:-/tmp}/orphan-sweep-work.XXXXXX") || exit 3
trap 'rm -rf "$work"' EXIT

die() { echo "check.sh: $*" >&2; exit 3; }

# --- 1. Facts and fingerprints from the code --------------------------------
GO=${GO:-go}
command -v "$GO" >/dev/null || die "needs Go (set GO=/path/to/go)"
if ! (cd "$repo" && "$GO" run "$skill_dir/facts/facts.go" -root "$repo") >"$out/facts.txt" 2>"$out/facts.err"; then
  cat "$out/facts.err" >&2
  die "facts.go failed (a covered function moved or was renamed?)"
fi
facts_cfg="$work/facts.cfg"
{
  echo "\\* Code facts from facts/facts.go at $(git -C "$repo" rev-parse --short HEAD)"
  echo "CONSTANTS"
  awk '$1 == "FACT" { printf "    %s = %s\n", $2, $3 }' "$out/facts.txt"
} >"$facts_cfg"
nfacts=$(grep -c '^FACT ' "$out/facts.txt")
[ "$nfacts" -eq 9 ] || die "expected 9 facts from facts.go, got $nfacts"
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

# run_tlc CFG WORKERS EXTRA_ARGS... -> exit code of TLC; output in $out/CFG.out
run_tlc() {
  local cfg=$1 workers=$2; shift 2
  local d="$work/${cfg%.cfg}"
  mkdir -p "$d/tmp"
  cp "$model/PackSweep.tla" "$d/"
  cat "$model/$cfg" "$facts_cfg" >"$d/run.cfg"
  cp "$d/run.cfg" "$out/${cfg%.cfg}.run.cfg"
  local rc=0
  (cd "$d" && "$JAVA" -Djava.io.tmpdir="$d/tmp" -Xmx"${TLC_HEAP:-4g}" -XX:+UseParallelGC \
     -cp "$jar" tlc2.TLC -workers "$workers" -cleanup -metadir "$d/meta" "$@" \
     -config run.cfg PackSweep.tla) >"$out/${cfg%.cfg}.out" 2>&1 || rc=$?
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
selected() { [ "$only" = " " ] || case $only in *" $1 "*) return 0 ;; *) return 1 ;; esac; }
verdict() { printf '   %-27s %s\n' "$1" "$2"; }
echo "== TLC $TLC_VERSION (outputs in $out)"
for cfg in PackSweep.cfg NoTiming.cfg Liveness.cfg; do
  selected "$cfg" || continue
  extra=()
  [ "$cfg" = Liveness.cfg ] && extra=(-lncheck final)
  rc=0; run_tlc "$cfg" "${TLC_WORKERS:-4}" ${extra[@]+"${extra[@]}"} || rc=$?
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
done

# Probes drop one timing assumption each; NoLostPack must then fail. One
# worker keeps the trace the shortest one.
stale=0 probe_passed=0
[ "$false_facts" -eq 0 ] || stale=1
for cfg in NoThresholdAssumption.cfg SlowHealEntry.cfg; do
  selected "$cfg" || continue
  rc=0; run_tlc "$cfg" 1 || rc=$?
  case $rc in
    12)
      if grep -q 'Invariant NoLostPack is violated' "$out/${cfg%.cfg}.out"; then
        summarize "$out/${cfg%.cfg}.out" >"$out/${cfg%.cfg}.trace.txt"
        verdict "$cfg" "fails as expected (trace: $out/${cfg%.cfg}.trace.txt)"
      else
        verdict "$cfg" "UNEXPECTED: $(grep -m1 'is violated' "$out/${cfg%.cfg}.out")"; status=1
      fi ;;
    0) verdict "$cfg" "PASSES: this timing assumption no longer matters. Review the model and SKILL.md"; stale=1; probe_passed=1 ;;
    *) verdict "$cfg" "ERROR: TLC exit $rc"; sed -n '1,40p' "$out/${cfg%.cfg}.out" >&2; exit 3 ;;
  esac
done

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
  { echo "# Token fingerprints of the code model/PackSweep.tla describes (facts/facts.go)."
    echo "# check.sh --record rewrites this file; SKILL.md holds the sync commit."
    cat "$out/fingerprints.now"; } >"$model/fingerprints.txt"
  sed -i.bak "s/^Sync commit: \`[0-9a-f]*\`/Sync commit: \`$head\`/" "$skill_dir/SKILL.md" && rm -f "$skill_dir/SKILL.md.bak"
  echo "== Recorded fingerprints and sync commit $head"
  stale=0
fi

if [ "$status" -ne 0 ]; then
  echo "RESULT: FAIL (a property broke; read the trace before you touch the model)"; exit 1
elif [ "$stale" -ne 0 ]; then
  echo "RESULT: STALE (properties hold on the model; the model may not describe the code)"; exit 2
fi
if [ "$only" != " " ]; then
  echo "RESULT: PASS for the selected configs only:$only"; exit 0
fi
echo "RESULT: PASS"
