# Manage repositories

A user creates, inspects, lists, and deletes repositories, and changes a repository's default branch, through the REST API. A deleted repository disappears from every read and from Git, and its name cannot be used again.

## Sub-features

- `repo-create` answers `201` for a new name and `200` (`already exists`) for the same request again.
- `repo-get` returns the record for the token's repo.
- `repo-list` lists the organization's repos with an `org:read` token, filters with `q` and `prefix`, and pages with `limit`.
- `repo-url` resolves a `repo_id` to its name (`GET /api/repo-urls/{repo_id}`).
- `org-meta` returns the organization metadata (`GET /api/meta`).
- `repo-default-branch` changes the default branch with `PATCH /api/v1/repo`.
- `repo-delete` soft-deletes a repo: then `GET` answers `404`, a second delete answers `409`, a create with the name answers `404`, git answers `not found`, and the list drops it.

## How to get to it (user POV)

- `POST /api/v1/repos`, `PATCH /api/v1/repo`, and `DELETE /api/v1/repos/delete` with a `repo:write` token for the repo.
- `GET /api/v1/repo` with a `git:read` token for the repo.
- `GET /api/v1/repos`, `GET /api/repo-urls/{repo_id}`, and `GET /api/meta` with an `org:read` token whose `repo` claim is `org`.
- The path-style aliases `GET`, `PATCH`, and `DELETE /api/repos/{repo_name}`.

## Driving it with osv.sh

Preconditions:

- The baseline from [`README.md`](README.md#baseline-preconditions) ran in this shell.
- doctor passes.

- **Repeat a create.** Send the baseline's create request again. The status is `200`, `message` is `Repository 'demo' already exists`, and `repo_id` is `REPO_ID`.

```sh
"$OSV" api "$RUN" repo-create-again --token create --expect 200 \
  POST /api/v1/repos -H 'Content-Type: application/json' -d '{"default_branch":"main"}'
"$OSV" eq "$RUN" repo-create-again-id "$REPO_ID" "$("$OSV" json "$OSV_EVIDENCE/repo-create-again.body" repo_id)"
```

- **Create two more repos.** Create `scratch` and `tools`. Both answer `201`.

```sh
"$OSV" token "$RUN" scratch --repo scratch --scopes repo:write,git:read
"$OSV" token "$RUN" tools --repo tools --scopes repo:write,git:read
"$OSV" api "$RUN" repo-create-scratch --token scratch --expect 201 POST /api/v1/repos -d '{}'
"$OSV" api "$RUN" repo-create-tools --token tools --expect 201 POST /api/v1/repos -d '{}'
SCRATCH_ID=$("$OSV" json "$OSV_EVIDENCE/repo-create-scratch.body" repo_id)
```

- **Get one repo.** Read `scratch` with its own token. `repo_name` is `scratch` and `default_branch` is `main`.

```sh
"$OSV" api "$RUN" repo-get --token scratch --expect 200 GET /api/v1/repo
"$OSV" eq "$RUN" repo-get-default main "$("$OSV" json "$OSV_EVIDENCE/repo-get.body" default_branch)"
```

- **List, filter, and page.** List with an org token, filter with `q=too`, and page with `limit=1`. The list has `demo`, `scratch`, and `tools`. The filter returns only `tools`. The first page has one repo and `has_more` true.

```sh
"$OSV" token "$RUN" org --repo org --scopes org:read
"$OSV" api "$RUN" repo-list --token org --expect 200 GET /api/v1/repos
"$OSV" eq "$RUN" repo-list-names '["demo", "scratch", "tools"]' "$("$OSV" json "$OSV_EVIDENCE/repo-list.body" 'repos.*.repo_name' | python3 -c 'import json,sys; print(json.dumps(sorted(json.load(sys.stdin))))')"
"$OSV" api "$RUN" repo-list-q --token org --expect 200 GET '/api/v1/repos?q=too'
"$OSV" eq "$RUN" repo-list-q-only '["tools"]' "$("$OSV" json "$OSV_EVIDENCE/repo-list-q.body" 'repos.*.repo_name')"
"$OSV" api "$RUN" repo-list-page --token org --expect 200 GET '/api/v1/repos?limit=1'
"$OSV" eq "$RUN" repo-list-page-more true "$("$OSV" json "$OSV_EVIDENCE/repo-list-page.body" has_more)"
```

- **Filter by prefix.** `prefix=T` matches the start of the name without case, so it returns only `tools`. With `prefix` set, `q` matches only the rest of the name after the prefix: `prefix=t&q=t` returns no repo, because `ools` has no `t`.

```sh
"$OSV" api "$RUN" repo-list-prefix --token org --expect 200 GET '/api/v1/repos?prefix=T'
"$OSV" eq "$RUN" repo-list-prefix-only '["tools"]' "$("$OSV" json "$OSV_EVIDENCE/repo-list-prefix.body" 'repos.*.repo_name')"
"$OSV" api "$RUN" repo-list-prefix-q --token org --expect 200 GET '/api/v1/repos?prefix=t&q=t'
"$OSV" eq "$RUN" repo-list-prefix-q-none '[]' "$("$OSV" json "$OSV_EVIDENCE/repo-list-prefix-q.body" 'repos.*.repo_name')"
```

- **Resolve an ID and read the org metadata.** Resolve `SCRATCH_ID`, then read `/api/meta`. The ID resolves to `scratch`. The metadata is `{"ips":{"api":[],"git":[]},"provider":{"name":"","region":""}}`.

```sh
"$OSV" api "$RUN" repo-url --token org --expect 200 GET "/api/repo-urls/$SCRATCH_ID"
"$OSV" eq "$RUN" repo-url-name scratch "$("$OSV" json "$OSV_EVIDENCE/repo-url.body" repo_name)"
"$OSV" api "$RUN" org-meta --token org --expect 200 GET /api/meta
"$OSV" eq "$RUN" org-meta-body '{"ips":{"api":[],"git":[]},"provider":{"name":"","region":""}}' "$(cat "$OSV_EVIDENCE/org-meta.body")"
```

- **Change the default branch.** Set `demo`'s default branch to `trunk`, which does not exist yet. The status is `200` and `GET /api/v1/repo` shows `trunk`.

```sh
"$OSV" api "$RUN" repo-patch --token create --expect 200 \
  PATCH /api/v1/repo -H 'Content-Type: application/json' -d '{"default_branch":"trunk"}'
"$OSV" api "$RUN" repo-patch-read --token rw --expect 200 GET /api/v1/repo
"$OSV" eq "$RUN" repo-patch-default trunk "$("$OSV" json "$OSV_EVIDENCE/repo-patch-read.body" default_branch)"
```

- **Delete a repo.** Delete `scratch`, then try to read it, delete it again, create it again, and clone it. The answers are `200` `repository deleted`, `404` `repository_not_found`, `409` `repository_deleted`, `404`, and git exit `128` with `not found`. The list no longer has `scratch`.

```sh
"$OSV" api "$RUN" repo-delete --token scratch --expect 200 DELETE /api/v1/repos/delete
"$OSV" api "$RUN" repo-delete-get --token scratch --expect 404 GET /api/v1/repo
"$OSV" eq "$RUN" repo-delete-get-code repository_not_found "$("$OSV" json "$OSV_EVIDENCE/repo-delete-get.body" code)"
"$OSV" api "$RUN" repo-delete-again --token scratch --expect 409 DELETE /api/v1/repos/delete
"$OSV" eq "$RUN" repo-delete-again-code repository_deleted "$("$OSV" json "$OSV_EVIDENCE/repo-delete-again.body" code)"
"$OSV" api "$RUN" repo-delete-recreate --token scratch --expect 404 POST /api/v1/repos -d '{}'
"$OSV" git "$RUN" repo-delete-clone --token scratch --expect 128 -- clone -q "$OSV_URL/scratch.git" scratch
"$OSV" api "$RUN" repo-list-after --token org --expect 200 GET /api/v1/repos
"$OSV" eq "$RUN" repo-list-after-names '["demo", "tools"]' "$("$OSV" json "$OSV_EVIDENCE/repo-list-after.body" 'repos.*.repo_name' | python3 -c 'import json,sys; print(json.dumps(sorted(json.load(sys.stdin))))')"
```

- **Proof.** `log.tsv` has a `PASS` row for every call above, and `repo-list.body` and `repo-list-after.body` show the list before and after the delete.

## Gotchas

- Organization endpoints need a token minted with `--repo org --scopes org:read`. A repo token gets `403` `insufficient_scope`.
- `GET /api/meta` and `GET /api/repo-urls/{repo_id}` have no `/v1` in the path.
- A deleted name stays taken: create answers `404` with `was deleted, and its name cannot be reused`. Use a new name, or a new run, to repeat a delete check.
- `PATCH /api/v1/repo` does not create the branch. Clones see a dangling `HEAD` until a push creates it.
- Delete is a soft delete. `openstorage purge-repo` reclaims storage only for deletions older than one hour, so this map does not drive it.
- `q` matches a substring of the repo `url`, which is the repo name in local mode.
- The list is newest first. Repos created in the same second come back in `repo_id` order, which is random, so sort names before you compare them.
