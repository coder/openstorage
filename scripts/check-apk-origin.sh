#!/bin/sh
# Usage: check-apk-origin.sh
# Runs inside the runtime image, with network access: apk reads the repository
# indexes. Fails unless the image's own `main` repository for its Alpine branch
# (from /etc/alpine-release and /etc/apk/repositories) offers the installed
# build of every package. A package that only `community`, another
# repository or no repository offers fails, and so does a virtual package.
# Prints one line per package that does not pass or passes with a warning, then
# a count line.
set -u
work=$(mktemp -d) || exit 2
trap 'rm -rf "$work"' EXIT
release=$(cat /etc/alpine-release) || exit 2
branch="v${release%.*}"
main=$(awk -v s="/$branch/main" '$0 ~ /^https?:\/\// && substr($0, length($0) - length(s) + 1) == s' /etc/apk/repositories)
if [ "$(printf '%s\n' "$main" | grep -c .)" != 1 ]; then
	echo "FAIL /etc/apk/repositories holds no single $branch main repository: $(tr '\n' ' ' </etc/apk/repositories)"
	exit 1
fi
echo "main repository: $main"
apk info >"$work/names" 2>"$work/err" || {
	cat "$work/err"
	exit 2
}
# One argument per installed package name.
# shellcheck disable=SC2046
apk policy $(cat "$work/names") >"$work/policy" 2>"$work/err" || {
	cat "$work/err"
	exit 2
}
# apk warns and goes on when it cannot read an index.
if grep -F "fetching $main/" "$work/err"; then
	echo "FAIL apk could not read the index of $main"
	exit 1
fi
# One row per package: name, installed version, the repositories that offer the
# installed package, and the versions that main offers. apk policy lists one
# group per package build (a version line, then its sources), so a build with
# main's name and version but other contents is a group of its own.
awk -v main="$main" '
function group() {
	if (g_installed) { inst = g_version; inst_repos = g_repos }
	if (g_main) offered = offered " " g_version
	g_version = ""; g_installed = 0; g_main = 0; g_repos = ""
}
function flush() {
	group()
	if (name != "") print name "|" inst "|" inst_repos "|" offered
	name = ""; inst = ""; inst_repos = ""; offered = ""
}
/^[^ ].* policy:$/ { flush(); name = $1; next }
/^  [^ ]/ { group(); g_version = $1; sub(/:$/, "", g_version); next }
/^    [^ ]/ {
	if ($1 == "lib/apk/db/installed") g_installed = 1
	else {
		g_repos = g_repos " " $1
		if ($1 == main) g_main = 1
	}
}
END { flush() }
' "$work/policy" >"$work/rows"
checked=0
failed=0
while IFS='|' read -r name inst repos offered; do
	checked=$((checked + 1))
	case "$repos " in
	*" $main "*) continue ;;
	esac
	if [ -n "$repos" ]; then
		failed=$((failed + 1))
		echo "FAIL $name $inst: offered by$repos, not by $main"
		continue
	fi
	# Alpine keeps only the newest build in an index. When main replaced the
	# package between the build and this check, no repository offers the
	# installed version any more, and main offers a newer one.
	newer=""
	for v in $offered; do
		[ "$(apk version -t "$inst" "$v" </dev/null)" = "<" ] && newer=$v
	done
	if [ -n "$newer" ]; then
		echo "::warning::$name $inst: no repository offers it any more, and $main offers the newer $newer"
	else
		failed=$((failed + 1))
		echo "FAIL $name $inst: no repository offers it"
	fi
done <"$work/rows"
want=$(grep -c . "$work/names")
if [ "$checked" != "$want" ]; then
	failed=$((failed + 1))
	echo "FAIL apk policy described $checked of $want installed packages"
fi
echo "CHECKED $checked FAILED $failed"
[ "$checked" -gt 0 ] && [ "$failed" -eq 0 ]
