# open.storage verification map

This directory is the maintained source for verifying the user-facing behavior of open.storage. Read the index before you drive the node. Then use the matching feature file as the recipe.

## Baseline preconditions

- Launch a fresh run with `"$OSV" launch "$RUN"` (see [`../SKILL.md`](../SKILL.md#launch)). `$RUN` is an empty directory outside the checkout. A feature that needs two replicas says so and needs `"$OSV" launch "$RUN" --fleet` instead. In a fleet run the baseline and the other features run on node a.
- Load the run's variables with `eval "$("$OSV" env "$RUN")"`.
- Require `doctor: 0 failure(s)` from `"$OSV" doctor "$RUN"`.
- Never drive a node that this run did not launch.
- Unless a feature says otherwise, start from the baseline below: repo `demo` with default branch `main`, and one pushed commit `Seed commit` that holds `README.md`. The local clone that made it is `$OSV_WORK/seed`.

```sh
"$OSV" token "$RUN" create --repo demo --scopes repo:write
"$OSV" token "$RUN" rw --repo demo --scopes git:read,git:write
"$OSV" api "$RUN" base-create --token create --expect 201 \
  POST /api/v1/repos -H 'Content-Type: application/json' -d '{"default_branch":"main"}'
REPO_ID=$("$OSV" json "$OSV_EVIDENCE/base-create.body" repo_id)
"$OSV" git "$RUN" base-init -- init -q seed
printf 'hello from git\n' >"$OSV_WORK/seed/README.md"
"$OSV" git "$RUN" base-add -- -C seed add README.md
"$OSV" git "$RUN" base-commit -- -C seed commit -q -m 'Seed commit'
"$OSV" git "$RUN" base-push --token rw -- -C seed push "$OSV_URL/demo.git" HEAD:refs/heads/main
SEED=$("$OSV" git "$RUN" base-seed-sha -- -C seed rev-parse HEAD)
```

The baseline needs no product shortcuts: it uses `POST /api/v1/repos` and a real `git push`. Feature files reuse `$REPO_ID` and `$SEED`.

## Driving conventions

- Run each recipe in one shell, from the repository root, with `OSV` and `RUN` set as in [`../SKILL.md`](../SKILL.md#launch).
- Start every recipe from the baseline state unless its preconditions say otherwise. A recipe that creates repos, branches, or tags leaves them in place. Launch a new run instead of undoing them.
- Send every request through `"$OSV" api` and every git command through `"$OSV" git`, so each one lands in the evidence.
- Give each call its own label. The recipes prefix labels with the feature's short name.
- Put `--expect` on every call whose status or exit code is part of the proof.
- Treat every command as literal. Keep quoted names, paths, and flags unchanged.
- Write request bodies into `$OSV_EVIDENCE` with a quoted heredoc (`<<'NDJSON'`) and send them with `--data-binary @file`.
- Drive the calls one at a time. The recipes are not load or concurrency tests.

## Proof and skip reporting

- Capture the user action and the resulting state through a second view: a REST write with a git clone, a git push with REST reads.
- Compare commit SHAs, blob SHAs, and file bytes with `"$OSV" eq`, so the verdict is in `log.tsv`.
- Denial proof is the status code (`--expect 401` or `--expect 403`) plus the error body, and a read that shows that nothing changed.
- Record the feature file and the sub-feature IDs that you drove in your report, next to the evidence directory.
- Report an unreachable path with the attempted command, its label, and the unmet precondition.
- Do not report a skipped entry point as verified through a different entry point.

## Feature entry contract

Each feature file starts with an H1 title and one paragraph that describes the user-visible behavior. It then uses exactly four H2 sections in this order.

1. `Sub-features` lists short IDs with one line for each behavior.
2. `How to get to it (user POV)` lists every user entry point.
3. `Driving it with osv.sh` starts with `Preconditions:` and uses labeled bullets that pair each user action with an exact command and an observable result.
4. `Gotchas` lists traps that can waste or invalidate a verification run.

Keep implementation details out of the map. Name only user paths, stable handles, required state, commands, and observable proof.

## Features

- [Use a repo as a Git remote](./git-remote.md) covers repo creation for git, push, ls-remote, clone by name and by ID, fetch of another writer's commit, and an empty clone.
- [Commit through the REST API](./commit-pack.md) covers commit-pack writes, read-back through REST and a fresh clone, stored packs, new branches and deletes, the first commit of an empty repo, and the no-change and stale-guard refusals.
- [Read a repo through the REST API](./read-api.md) covers the repo record, file trees, raw files, branches, commits with paging, one commit, diffs, tags, path-style aliases, and reads of an empty repo.
- [Manage repositories](./repo-lifecycle.md) covers create, get, list and filter, repo-URL lookup, organization metadata, default-branch changes, and soft delete with its follow-up answers.
- [Token auth and scopes](./token-scopes.md) covers missing, malformed, wrong-key, and expired tokens, missing scopes, repo-claim mismatches, and the matching git failures.
- [Two replicas share one store](./replicas.md) needs a fleet run (`launch --fleet`). It covers a clone, REST reads, and the repo list on node b after writes on node a, a REST commit on node b fetched through node a, a stale guard across nodes, and node a serving alone after node b stops.

Not mapped yet: Git LFS, webhooks, upstream sync and forks, merges and branch and tag writes through REST, notes, blame, grep, archives, ref policies (`--refs`), and the S3WAL mode.
