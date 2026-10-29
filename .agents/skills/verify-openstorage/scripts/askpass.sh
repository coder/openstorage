#!/bin/sh
# GIT_ASKPASS helper for osv.sh git: username "t", password = the token in
# $OSV_GIT_TOKEN_FILE. No token file means no credentials (git then fails).
[ -n "${OSV_GIT_TOKEN_FILE:-}" ] || exit 1
case "$1" in
*Username*) echo t ;;
*Password*) cat "$OSV_GIT_TOKEN_FILE" && echo ;;
*) exit 1 ;;
esac
