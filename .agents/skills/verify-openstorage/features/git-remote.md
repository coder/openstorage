# Use a repo as a Git remote

A user creates a repository through the REST API and then uses it as an ordinary Git remote over smart HTTP: push, list refs, clone by name or by repo ID, and fetch commits that another writer pushed. The token travels as the Basic-auth password with the user name `t`.

## Sub-features

- `remote-create` creates the repo and returns `http_url` = `<base>/<repo_id>.git`.
- `remote-push` pushes a new `main` branch from a local repository.
- `remote-ls` lists the pushed refs with `git ls-remote`.
- `remote-clone-name` clones through `/<repo_name>.git`.
- `remote-clone-id` clones through the `http_url` from create (`/<repo_id>.git`).
- `remote-fetch` fetches a commit that a second clone pushed.
- `remote-rest-view` shows the pushed tip through `GET /api/v1/repos/branch`.
- `remote-empty` clones a repository that has no commits.

## How to get to it (user POV)

- Create the repo: `POST /api/v1/repos` with a `repo:write` token whose `repo` claim is the new name.
- Point git at `http://<host>/<repo_name>.git` or at the `http_url` from create, and give the token as the password for user `t`.
- The README quickstart (`README.md`, "Try locally: SQLite + files") follows the same path with `curl` and `git`.

## Driving it with osv.sh

Preconditions:

- A fresh run is launched, doctor passes, and `eval "$("$OSV" env "$RUN")"` has run.
- Repo `demo` does not exist yet. This recipe replaces the baseline: do not run the baseline block first.

- **Create the repo.** Mint the create and read-write tokens and create `demo`. The status is `201`, `repo_name` is `demo`, and `http_url` is `$OSV_URL/<repo_id>.git`.

```sh
"$OSV" token "$RUN" create --repo demo --scopes repo:write
"$OSV" token "$RUN" rw --repo demo --scopes git:read,git:write
"$OSV" api "$RUN" remote-create --token create --expect 201 \
  POST /api/v1/repos -H 'Content-Type: application/json' -d '{"default_branch":"main"}'
REPO_ID=$("$OSV" json "$OSV_EVIDENCE/remote-create.body" repo_id)
"$OSV" eq "$RUN" remote-http-url "$OSV_URL/$REPO_ID.git" "$("$OSV" json "$OSV_EVIDENCE/remote-create.body" http_url)"
```

- **Push a branch.** Commit `README.md` in a new local repository and push it as `main`. git prints `* [new branch]      HEAD -> main` and exits `0`.

```sh
"$OSV" git "$RUN" remote-init -- init -q src
printf 'hello from git\n' >"$OSV_WORK/src/README.md"
"$OSV" git "$RUN" remote-add -- -C src add README.md
"$OSV" git "$RUN" remote-commit -- -C src commit -q -m 'first commit'
SRC=$("$OSV" git "$RUN" remote-src-sha -- -C src rev-parse HEAD)
"$OSV" git "$RUN" remote-push --token rw -- -C src push "$OSV_URL/demo.git" HEAD:refs/heads/main
```

- **List refs.** Run `git ls-remote`. stdout has `<SRC>	HEAD` and `<SRC>	refs/heads/main`.

```sh
"$OSV" git "$RUN" remote-ls --token rw -- ls-remote "$OSV_URL/demo.git"
"$OSV" eq "$RUN" remote-ls-main "$SRC" "$(awk '$2 == "refs/heads/main" { print $1 }' "$OSV_EVIDENCE/remote-ls.git.txt")"
```

- **Clone by name and by ID.** Clone through both URL forms. Both clones check out `SRC`, and `README.md` holds `hello from git`.

```sh
"$OSV" git "$RUN" remote-clone-name --token rw -- clone -q "$OSV_URL/demo.git" by-name
"$OSV" git "$RUN" remote-clone-id --token rw -- clone -q "$OSV_URL/$REPO_ID.git" by-id
"$OSV" eq "$RUN" remote-name-head "$SRC" "$("$OSV" git "$RUN" remote-name-sha -- -C by-name rev-parse HEAD)"
"$OSV" eq "$RUN" remote-id-head "$SRC" "$("$OSV" git "$RUN" remote-id-sha -- -C by-id rev-parse HEAD)"
"$OSV" eq "$RUN" remote-readme "hello from git" "$(cat "$OSV_WORK/by-id/README.md")"
```

- **Fetch another writer's commit.** Push a second commit from the `by-id` clone, then fetch it into `src`. `origin/main` in `src` equals the second clone's `HEAD`, and its parent is `SRC`.

```sh
printf 'second line\n' >>"$OSV_WORK/by-id/README.md"
"$OSV" git "$RUN" remote-commit2 -- -C by-id commit -q -am 'second commit'
SECOND=$("$OSV" git "$RUN" remote-second-sha -- -C by-id rev-parse HEAD)
"$OSV" git "$RUN" remote-push2 --token rw -- -C by-id push origin HEAD:main
"$OSV" git "$RUN" remote-fetch --token rw -- -C src fetch -q "$OSV_URL/demo.git" main:refs/remotes/origin/main
"$OSV" eq "$RUN" remote-fetched "$SECOND" "$("$OSV" git "$RUN" remote-fetched-sha -- -C src rev-parse origin/main)"
"$OSV" eq "$RUN" remote-parent "$SRC" "$("$OSV" git "$RUN" remote-parent-sha -- -C src rev-parse 'origin/main^')"
```

- **See the push through REST.** Read the branch. `branch.head_sha` is `SECOND`.

```sh
"$OSV" api "$RUN" remote-branch --token rw --expect 200 GET '/api/v1/repos/branch?name=main'
"$OSV" eq "$RUN" remote-rest-tip "$SECOND" "$("$OSV" json "$OSV_EVIDENCE/remote-branch.body" branch.head_sha)"
```

- **Clone an empty repo.** Create `emptyrepo` and clone it. git exits `0` and prints `warning: You appear to have cloned an empty repository.`

```sh
"$OSV" token "$RUN" empty --repo emptyrepo --scopes repo:write,git:read
"$OSV" api "$RUN" remote-create-empty --token empty --expect 201 POST /api/v1/repos -d '{}'
"$OSV" git "$RUN" remote-clone-empty --token empty -- clone "$OSV_URL/emptyrepo.git" empty
"$OSV" eq "$RUN" remote-empty-warning yes \
  "$(grep -q 'cloned an empty repository' "$OSV_EVIDENCE/remote-clone-empty.git.txt" && echo yes || echo no)"
```

- **Proof.** `$OSV_EVIDENCE/log.tsv` has a `PASS` row for every call above, and the `remote-*.eq.txt` files hold the compared SHAs.

## Gotchas

- Create answers `201` once. The same create again answers `200` with `Repository 'demo' already exists` and the same `repo_id`.
- The Git surface takes Basic auth only. A Bearer header on `/demo.git/...` gets `401` with `basic auth with username 't' required`.
- serve does not log Git requests. The proof is git's exit code and output, plus a second view.
- git 2.54 and later can start a detached `git maintenance run --auto` after a commit. `osv.sh git` turns `maintenance.auto` off. Plain `git` outside the helper can leave a background process that cleanup then reports.
- `git -C src fetch origin` fails here because `src` has no `origin` remote. The recipe fetches by URL.
- Pushing to a repo that was never created, or that was deleted, fails with `fatal: repository '<url>/' not found`.
