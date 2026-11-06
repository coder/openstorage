#!/bin/sh
# Usage: check-apply-messages.sh <cases-dir>
# Runs each patch of <cases-dir>/cases.tsv through the `git` on PATH and checks
# the exit status and that a line starts with the words internal/api reads (git
# apply's stderr for `cached` cases, its stdout for `summary` cases). One line
# per case, then a count line. internal/api/testdata/applymsg holds the cases.
set -u
dir=$(cd "${1:?usage: check-apply-messages.sh <cases-dir>}" && pwd) || exit 2
work=$(mktemp -d) || exit 2
trap 'rm -rf "$work"' EXIT
for var in $(env | sed -n 's/^\(GIT_[A-Za-z0-9_]*\)=.*/\1/p'); do unset "$var"; done
export HOME="$work" GIT_CONFIG_GLOBAL=/dev/null GIT_CONFIG_NOSYSTEM=1 LC_ALL=C
git --version
git init -q -b main "$work/repo" && cd "$work/repo" || exit 2
printf 'one\n' >a.txt
mkdir d
printf 'x\n' >d/f
git add -A
git -c user.name=t -c user.email=t@example.com commit -q -m base || exit 2
tab=$(printf '\t')
checked=0
failed=0
while IFS=$tab read -r name command want_exit _status prefix; do
	[ -n "$name" ] || continue
	cp .git/index "$work/index"
	case $command in
	cached)
		GIT_INDEX_FILE="$work/index" git apply --cached --binary --whitespace=nowarn <"$dir/$name.patch" >"$work/out" 2>"$work/err"
		code=$?
		seen="$work/err"
		;;
	summary)
		GIT_INDEX_FILE="$work/index" git apply --summary <"$dir/$name.patch" >"$work/out" 2>"$work/err"
		code=$?
		seen="$work/out"
		;;
	*)
		echo "FAIL $name: unknown command $command"
		failed=$((failed + 1))
		continue
		;;
	esac
	found=0
	while IFS= read -r line; do
		case $line in
		"$prefix"*) found=1 ;;
		esac
	done <"$seen"
	checked=$((checked + 1))
	if [ "$code" = "$want_exit" ] && [ "$found" = 1 ]; then
		echo "ok   $name: exit $code, a line starts with '$prefix'"
	else
		failed=$((failed + 1))
		echo "FAIL $name: exit $code (want $want_exit), line found $found, first lines: $(head -3 "$seen" | tr '\n' '|')"
	fi
done <"$dir/cases.tsv"
echo "CHECKED $checked FAILED $failed"
[ "$checked" -gt 0 ] && [ "$failed" -eq 0 ]
