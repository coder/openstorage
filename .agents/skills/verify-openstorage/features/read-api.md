# Read a repo through the REST API

A user with a `git:read` token reads a repository without cloning it: the repo record, file trees, raw file bytes, branches, commits, one commit, diffs, and tags. Every answer matches what git shows for the same commit.

## Sub-features

- `read-repo` returns the repo record (`GET /api/v1/repo`).
- `read-tree` lists files recursively by default, and one level with `recursive=false`.
- `read-file` streams raw file bytes with `X-Blob-Sha` and `X-Last-Commit-Sha` headers.
- `read-branches` lists branches and reads one branch; an absent branch is `404`.
- `read-commits` lists commits newest first, pages with `limit` and `cursor`, and filters by `path`.
- `read-commit` reads one commit with its parents.
- `read-diff` shows a single-commit diff and a diff against `base_ref`.
- `read-tags` lists tags and reads one tag that git pushed.
- `read-alias` serves the same reads under `/api/repos/{repo_name}/...`.
- `read-empty` answers reads of a repo without commits.

## How to get to it (user POV)

- `GET /api/v1/repo`, `GET /api/v1/repos/{files,file,branches,branch,commits,commit,diff,tags,tag}` with a `git:read` token. The token's `repo` claim selects the repo.
- The path-style aliases `GET /api/repos/{repo_name}` and `GET /api/repos/{repo_name}/<same suffix>`.
- The SDKs call the same endpoints.

## Driving it with osv.sh

Preconditions:

- The baseline from [`README.md`](README.md#baseline-preconditions) ran in this shell.
- doctor passes.

- **Add history with git.** Commit `docs/guide.md` and `hello.txt` in `seed`, tag the commit `v1`, and push both. Mint a read-only token. git exits `0`.

```sh
mkdir -p "$OSV_WORK/seed/docs"
printf 'Hello from git\n' >"$OSV_WORK/seed/hello.txt"
printf '# Guide\n' >"$OSV_WORK/seed/docs/guide.md"
"$OSV" git "$RUN" read-add -- -C seed add hello.txt docs/guide.md
"$OSV" git "$RUN" read-commit-local -- -C seed commit -q -m 'Add greeting and guide'
"$OSV" git "$RUN" read-tag-local -- -C seed tag v1
TIP=$("$OSV" git "$RUN" read-tip-sha -- -C seed rev-parse HEAD)
"$OSV" git "$RUN" read-push --token rw -- -C seed push "$OSV_URL/demo.git" HEAD:refs/heads/main refs/tags/v1
"$OSV" token "$RUN" ro --repo demo --scopes git:read
```

- **Repo record.** Read the repo. `repo_name` is `demo`, `repo_id` is `REPO_ID`, and `default_branch` is `main`.

```sh
"$OSV" api "$RUN" read-repo --token ro --expect 200 GET /api/v1/repo
"$OSV" eq "$RUN" read-repo-id "$REPO_ID" "$("$OSV" json "$OSV_EVIDENCE/read-repo.body" repo_id)"
"$OSV" eq "$RUN" read-repo-default main "$("$OSV" json "$OSV_EVIDENCE/read-repo.body" default_branch)"
```

- **File tree.** List `main` recursively, then one level. The recursive `paths` are `README.md`, `docs/guide.md`, and `hello.txt`. The one-level listing shows `docs` as a `tree` entry.

```sh
"$OSV" api "$RUN" read-tree --token ro --expect 200 GET '/api/v1/repos/files?ref=main'
"$OSV" eq "$RUN" read-tree-paths '["README.md", "docs/guide.md", "hello.txt"]' "$("$OSV" json "$OSV_EVIDENCE/read-tree.body" paths)"
"$OSV" api "$RUN" read-tree-top --token ro --expect 200 GET '/api/v1/repos/files?ref=main&recursive=false'
"$OSV" eq "$RUN" read-tree-top-docs tree "$(python3 -c 'import json,sys; print({e["path"]: e["type"] for e in json.load(open(sys.argv[1]))["entries"]}["docs"])' "$OSV_EVIDENCE/read-tree-top.body")"
```

- **Raw file.** Read `hello.txt` at `main`. The body is `Hello from git`, `X-Blob-Sha` equals `git rev-parse TIP:hello.txt`, and `X-Last-Commit-Sha` is `TIP`.

```sh
"$OSV" api "$RUN" read-file --token ro --expect 200 \
  GET /api/v1/repos/file -G --data-urlencode path=hello.txt --data-urlencode ref=main
"$OSV" eq "$RUN" read-file-bytes 'Hello from git' "$(cat "$OSV_EVIDENCE/read-file.body")"
"$OSV" eq "$RUN" read-file-blob "$("$OSV" git "$RUN" read-blob-sha -- -C seed rev-parse "$TIP:hello.txt")" \
  "$(sed -n 's/^X-Blob-Sha: //p' "$OSV_EVIDENCE/read-file.headers" | tr -d '\r')"
"$OSV" eq "$RUN" read-file-commit "$TIP" "$(sed -n 's/^X-Last-Commit-Sha: //p' "$OSV_EVIDENCE/read-file.headers" | tr -d '\r')"
```

- **Branches.** List branches and read `main`, then read an absent branch. The list holds only `main` at `TIP`. The absent branch answers `404`.

```sh
"$OSV" api "$RUN" read-branches --token ro --expect 200 GET /api/v1/repos/branches
"$OSV" eq "$RUN" read-branches-main "main $TIP" "$("$OSV" json "$OSV_EVIDENCE/read-branches.body" branches.0.name) $("$OSV" json "$OSV_EVIDENCE/read-branches.body" branches.0.head_sha)"
"$OSV" api "$RUN" read-branch --token ro --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" read-branch-tip "$TIP" "$("$OSV" json "$OSV_EVIDENCE/read-branch.body" branch.head_sha)"
"$OSV" api "$RUN" read-branch-absent --token ro --expect 404 GET '/api/v1/repos/branch?name=nope'
```

- **Commits.** List `main`, then page with `limit=1`, then filter by path. The list is `TIP`, then `SEED`. The first page has `has_more` true and a `next_cursor`. The second page starts at `SEED`. The `README.md` filter returns only `SEED`.

```sh
"$OSV" api "$RUN" read-commits --token ro --expect 200 GET '/api/v1/repos/commits?ref=main'
"$OSV" eq "$RUN" read-commits-order "$TIP $SEED" "$("$OSV" json "$OSV_EVIDENCE/read-commits.body" commits.0.sha) $("$OSV" json "$OSV_EVIDENCE/read-commits.body" commits.1.sha)"
"$OSV" api "$RUN" read-commits-p1 --token ro --expect 200 GET '/api/v1/repos/commits?ref=main&limit=1'
"$OSV" eq "$RUN" read-commits-p1-more true "$("$OSV" json "$OSV_EVIDENCE/read-commits-p1.body" has_more)"
CURSOR=$("$OSV" json "$OSV_EVIDENCE/read-commits-p1.body" next_cursor)
"$OSV" api "$RUN" read-commits-p2 --token ro --expect 200 \
  GET /api/v1/repos/commits -G --data-urlencode ref=main --data-urlencode limit=1 --data-urlencode "cursor=$CURSOR"
"$OSV" eq "$RUN" read-commits-p2-sha "$SEED" "$("$OSV" json "$OSV_EVIDENCE/read-commits-p2.body" commits.0.sha)"
"$OSV" api "$RUN" read-commits-path --token ro --expect 200 GET '/api/v1/repos/commits?ref=main&path=README.md'
"$OSV" eq "$RUN" read-commits-path-only "[\"$SEED\"]" "$("$OSV" json "$OSV_EVIDENCE/read-commits-path.body" 'commits.*.sha')"
```

- **One commit.** Read `TIP`. `commit.sha` is `TIP`, `parent_shas` is `[SEED]`, and `message` is `Add greeting and guide`.

```sh
"$OSV" api "$RUN" read-commit --token ro --expect 200 GET "/api/v1/repos/commit?ref=$TIP"
"$OSV" eq "$RUN" read-commit-parent "[\"$SEED\"]" "$("$OSV" json "$OSV_EVIDENCE/read-commit.body" commit.parent_shas)"
"$OSV" eq "$RUN" read-commit-message 'Add greeting and guide' "$("$OSV" json "$OSV_EVIDENCE/read-commit.body" commit.message)"
```

- **Diffs.** Read the diff of `main`, then the diff of `main` against `SEED`. Both list two added files (`stats.files` is `2`), and the second has `base_sha` `SEED`.

```sh
"$OSV" api "$RUN" read-diff --token ro --expect 200 GET '/api/v1/repos/diff?ref=main'
"$OSV" eq "$RUN" read-diff-files 2 "$("$OSV" json "$OSV_EVIDENCE/read-diff.body" stats.files)"
"$OSV" api "$RUN" read-diff-base --token ro --expect 200 GET "/api/v1/repos/diff?ref=main&base_ref=$SEED"
"$OSV" eq "$RUN" read-diff-base-sha "$SEED" "$("$OSV" json "$OSV_EVIDENCE/read-diff-base.body" base_sha)"
"$OSV" eq "$RUN" read-diff-base-state A "$("$OSV" json "$OSV_EVIDENCE/read-diff-base.body" files.0.state)"
```

- **Tags.** List tags and read `v1`. The list holds `v1` at `TIP`.

```sh
"$OSV" api "$RUN" read-tags --token ro --expect 200 GET /api/v1/repos/tags
"$OSV" eq "$RUN" read-tags-v1 "v1 $TIP" "$("$OSV" json "$OSV_EVIDENCE/read-tags.body" tags.0.name) $("$OSV" json "$OSV_EVIDENCE/read-tags.body" tags.0.sha)"
"$OSV" api "$RUN" read-tag --token ro --expect 200 GET '/api/v1/repos/tag?name=v1'
"$OSV" eq "$RUN" read-tag-sha "$TIP" "$("$OSV" json "$OSV_EVIDENCE/read-tag.body" tag.sha)"
```

- **Path-style alias.** Read the branches and the repo through `/api/repos/demo`. The answers match the `/api/v1` answers.

```sh
"$OSV" api "$RUN" read-alias-branches --token ro --expect 200 GET /api/repos/demo/branches
"$OSV" eq "$RUN" read-alias-same "$(cat "$OSV_EVIDENCE/read-branches.body")" "$(cat "$OSV_EVIDENCE/read-alias-branches.body")"
"$OSV" api "$RUN" read-alias-repo --token ro --expect 200 GET /api/repos/demo
"$OSV" eq "$RUN" read-alias-repo-id "$REPO_ID" "$("$OSV" json "$OSV_EVIDENCE/read-alias-repo.body" repo_id)"
```

- **Empty repo.** Create `emptyrepo` and read it. The branch list is empty (`200`). `files` answers `404` `ref_not_found`, and `commits` answers `404` `branch_not_found`.

```sh
"$OSV" token "$RUN" empty --repo emptyrepo --scopes repo:write,git:read
"$OSV" api "$RUN" read-empty-create --token empty --expect 201 POST /api/v1/repos -d '{}'
"$OSV" api "$RUN" read-empty-branches --token empty --expect 200 GET /api/v1/repos/branches
"$OSV" eq "$RUN" read-empty-no-branches '[]' "$("$OSV" json "$OSV_EVIDENCE/read-empty-branches.body" branches)"
"$OSV" api "$RUN" read-empty-files --token empty --expect 404 GET /api/v1/repos/files
"$OSV" eq "$RUN" read-empty-files-code ref_not_found "$("$OSV" json "$OSV_EVIDENCE/read-empty-files.body" code)"
"$OSV" api "$RUN" read-empty-commits --token empty --expect 404 GET /api/v1/repos/commits
"$OSV" eq "$RUN" read-empty-commits-code branch_not_found "$("$OSV" json "$OSV_EVIDENCE/read-empty-commits.body" code)"
```

- **Proof.** `log.tsv` has a `PASS` row for every call above. Each `read-*.body` is the REST answer, and the `read-*.eq.txt` files tie it to the SHAs that git produced.

## Gotchas

- `files` without `recursive` lists every file under `path` and no `tree` entries. Only `recursive=false` shows folders, as `tree` entries.
- `files` and `files/metadata` without `cursor` and `limit` return the whole tree in one answer. Other list endpoints default to 20 entries per page.
- `file` returns raw bytes, not JSON. Compare bytes or blob SHAs, not a parsed body.
- A commit's `message` in `commits` and `commit` is the subject line only.
- Send query values that hold special characters with `-G --data-urlencode`. A cursor is base64url text, but encode it anyway.
- The `osv.sh json` output of a list is Python-style JSON with a space after each comma, for example `["README.md", "docs/guide.md"]`.
- The token's `repo` claim picks the repo for `/api/v1/...`. There is no repo in the path. To read another repo, mint another token.
