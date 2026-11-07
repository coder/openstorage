# Cut a release

Ship a new open.storage version: a semver tag, a GitHub release with CHANGELOG
notes, and verified cross-platform binaries. You never compute versions by hand.

[release-please](https://github.com/googleapis/release-please)
([#65](https://github.com/coder/openstorage/issues/65)) and the binaries workflow
([#66](https://github.com/coder/openstorage/issues/66)) drive the flow:

```
conventional commit lands on main
      │  .github/workflows/release-please.yml (every push to main)
      ▼
rolling release PR  ("chore(main): release X.Y.Z" — CHANGELOG.md + manifest bump)
      │  maintainer merges it
      ▼
tag vX.Y.Z + GitHub release created
      │  same run chains .github/workflows/release.yml via workflow_call
      ▼
binaries built (CGO off), archives verified on linux + macOS, assets uploaded
```

## 1. Land conventional commits

release-please reads only [conventional-commit](https://www.conventionalcommits.org/)
**squash titles** on main. The older `<scope>: <subject>` style (`api: ...`,
`githttp: ...`) is *invisible*: it neither bumps the version nor appears in the
CHANGELOG. The `pr-title` CI check ([#68](https://github.com/coder/openstorage/issues/68))
rejects non-conventional PR titles, including after title edits.

Title releasable PRs like this:

| Squash title | Effect (pre-1.0, `bump-minor-pre-major`) |
|---|---|
| `fix: ...`, `fix(scope): ...` | patch bump, CHANGELOG "Bug Fixes" |
| `feat: ...`, `feat(scope): ...` | minor bump, CHANGELOG "Features" |
| `feat!: ...` / `BREAKING CHANGE:` footer | minor bump (major once we pass 1.0.0) |
| `perf: ...` | patch bump, CHANGELOG "Performance Improvements" |
| `docs:`, `chore:`, `ci:`, `build:`, `test:`, `refactor:` | no bump, hidden from CHANGELOG |
| anything else (`api: ...`) | ignored entirely |

## 2. Get the release PR green

After the next push to main, release-please opens or updates the rolling release PR.

`github-actions[bot]` creates that PR, so GitHub holds its `pull_request` CI run at
**"action required"**. Nothing runs until a maintainer approves it. Branch
protection requires `lint` / `test` / `test-local` / `test-s3wal` from the `ci`
workflow and `pr-title` from the `pr-title` workflow.

Each push to the branch holds two runs: one `ci` and one `pr-title`. Approve both,
with the **Approve and run** button on each Actions run, or:

```sh
for wf in ci pr-title; do
  gh run list -R coder/openstorage --branch release-please--branches--main--components--openstorage \
    --workflow "$wf" --status action_required --limit 1
done
gh api --method POST repos/coder/openstorage/actions/runs/<run-id>/approve  # once per run
```

> [!IMPORTANT]
> Approve both runs. If either stays held, its required checks never report and the
> release PR cannot merge.

- Every push to main while the release PR is open refreshes the branch and creates
  two new held runs. Approve each one.
- A fine-grained PAT or GitHub App token in the action's `token` input would remove
  this step. Until one exists, approve-and-run is the documented path. Do **not**
  loosen branch protection instead.

## 3. Merge the release PR

Release PRs go through the merge queue like every other PR. Enqueue it as-is with
**Merge when ready**, or `gh pr merge <number> -R coder/openstorage --squash`. Do
not edit the title or the body: keep the `chore(main): release X.Y.Z` title so it
does not trigger another bump. The queue runs `ci` and `pr-title` again on the
queued commit, then squash-merges it. The next `release-please.yml` run then:

1. Creates tag `vX.Y.Z` and the GitHub release, using the CHANGELOG section as notes.
2. Chains the `release` workflow
   ([`scripts/release-archives.sh`](../../scripts/release-archives.sh)), which:
   - builds `./cmd/openstorage` for linux/amd64, linux/arm64, darwin/amd64, and
     darwin/arm64 with `CGO_ENABLED=0`;
   - stamps tag/commit/date into [`openstorage version`](../reference/cli.md#openstorage-version);
   - verifies archive contents and the bundled license against the root
     [`LICENSE`](../../LICENSE);
   - verifies checksums and runs the native binary on ubuntu and macOS runners;
   - uploads `openstorage_<version>_<os>_<arch>.tar.gz` and `checksums.txt`.

Each archive has `openstorage` and `LICENSE` at its root, so recipients get the
license and `mise`/`ubi`-style installers still find the binary.

**Watch the run:** `gh run list --workflow release-please -R coder/openstorage`.

**If the binaries job fails** after the release exists (flaky runner, infra), fix
the cause and run `gh workflow run release -R coder/openstorage -f tag=vX.Y.Z`. It
rebuilds from the tag and replaces the assets (`--clobber`).

## 4. Verify and publish the container image

A release does **not** build or publish an image. The
[`image`](../../.github/workflows/image.yml) workflow runs only when a maintainer
dispatches it for an existing release tag
([#227](https://github.com/coder/openstorage/issues/227)). It builds the tag's own
Dockerfile, so it refuses tags cut before that Dockerfile stamped the version
(v0.16.0 and earlier).

1. Stage and check, without a version tag (the default, `publish=false`):

   ```sh
   gh workflow run image -R coder/openstorage --ref main -f tag=vX.Y.Z
   ```

   The `stage` jobs build the tag natively on linux/amd64 and linux/arm64 runners, one
   build per platform, and push each result to `ghcr.io/coder/openstorage` **by digest**,
   with no tag. Every run therefore leaves untagged package versions behind (at least one
   index per platform), and a run with `publish=false` leaves no version tag. The `verify` jobs pull those digests with a
   read-only token. Each job checks the platform, the `openstorage --version` stamp, and
   the runtime tools. It then runs
   [`scripts/check-apply-messages.sh`](../../scripts/check-apply-messages.sh) inside the
   image, because the image's Git is Alpine's and the server reads its patch refusals by
   their words (a tag cut before that script landed skips this check with a notice).
   If it fails, Alpine's Git words a refusal differently from the words that `internal/api`
   reads. A new dispatch builds again and passes only if Alpine has since served a Git whose
   words match. Otherwise teach `internal/api` the new words, update the cases in
   `internal/api/testdata/applymsg`, and cut a new release.
   Last, it runs the [Compose quickstart smoke](../../scripts/smoke-compose-quickstart.sh).

2. After that run is green, request the push:

   ```sh
   gh workflow run image -R coder/openstorage --ref main -f tag=vX.Y.Z -f publish=true
   ```

   This run stages and checks again, so its digests are the ones that ship. The `publish`
   job uses the `ghcr-publish` environment. It waits until a required reviewer approves
   it in the run's **Review deployments** dialog, and the environment accepts deployments
   only from `main`. With the `stage` jobs, it is one of the two jobs with
   `packages: write`.

   After approval it does not build. It gives the two checked digests one multi-platform
   tag, `ghcr.io/coder/openstorage:X.Y.Z`, with index annotations (source, revision,
   version, description). The OCI labels, the SBOM and the provenance come with the
   digests. It refuses to run when that tag already exists, so version tags are never
   replaced. It also refuses to create the tag unless the new index would hold exactly the
   manifests of the two staged digests, and it checks the pushed index once more. The run
   summary records the index digest. The `verify-published` jobs then pull that digest on
   both platforms and repeat the checks and the smoke.

   The `digest-*` artifacts hold the two staged digests. They are kept for 35 days, and an
   approval can wait up to 30 days. If the `publish` job fails because they are gone, or
   for any reason before it creates the tag, dispatch the run again. If it fails after the
   tag exists, the tag is spent: do not update the quickstart pin (step 3), and fix forward
   with a new release (see "If a published image is bad" below).

   To tell which case you are in, start from the failed step. The step "Check that the
   index holds exactly the checked digests" runs only after the tag exists. The step "Give
   the checked digests the version tag" can fail on either side of the push, so look at
   the tag itself (after `docker login ghcr.io`):

   ```sh
   docker buildx imagetools inspect ghcr.io/coder/openstorage:X.Y.Z
   ```

   It prints the index when the tag exists and fails with `not found` when it does not.
   Any other error, such as `denied`, tells you nothing about the tag: fix the login and
   run it again. A tag that a package admin deleted also shows `not found`, so never
   dispatch a version whose tag was ever created.

   Another dispatch cannot replace a tag that exists, because the `publish` job stops at
   "Refuse to replace an existing version tag" unless the registry answers 404 for the
   tag. That step cannot know that an admin deleted the tag, so the rule above stands: do
   not dispatch a version whose tag was ever created. If the step fails with HTTP 200, the
   tag exists and the version is spent: fix forward with a new release. Any other status
   means the step could not tell, so inspect the tag as above.

3. After the `verify-published` jobs pass, update the quickstart pin. In
   [Option A of the Docker quickstart](quickstart-docker.md#option-a-use-a-published-image),
   change both places that name the image:

   - the sentence "Version `X.Y.Z` is published for ..." (version and platforms)
   - the `export OPENSTORAGE_IMAGE=ghcr.io/coder/openstorage:X.Y.Z@sha256:<index digest>` line

   Copy the `Pin` line from the publish run summary, or read the index digest
   (after `docker login ghcr.io`):

   ```sh
   docker buildx imagetools inspect ghcr.io/coder/openstorage:X.Y.Z \
     --format '{{json .Manifest}}' | jq -r .digest
   ```

   Keep the Docker-only row in the [README](../../README.md) and step 2 of the
   [migration guide](migrate-to-openstorage.md) consistent with that pin, as
   [#417](https://github.com/coder/openstorage/pull/417) did. Neither names a
   version or digest. To confirm no other copy remains, run:

   ```sh
   git grep -nE 'openstorage:[0-9]|is published for' -- README.md docs ':!docs/how-to/release.md'
   ```

   It must list only the two Option A lines, both with the new version.

> [!IMPORTANT]
> GitHub creates a new package as **private**. By default it inherits this
> repository's access: people who can read the repository can pull it after
> `docker login ghcr.io`. Public access is a separate package-settings change by an
> organization owner.

**If a published image is bad,** do not move or reuse its tag. Fix forward with a new
release. A package admin can delete the bad version in the package settings.

**Before deleting an untagged package version,** check that no tag needs it. The untagged
versions that a run leaves are the staged indexes, and a registry may list the image and
attestation manifests under them as untagged versions too. A tagged multi-platform image
points at those same manifests, so deleting one of them breaks the tag. List what a tag
needs, and delete only a version whose digest is not in that list for any tag you keep:

```sh
docker buildx imagetools inspect ghcr.io/coder/openstorage:X.Y.Z --format '{{json .Manifest}}' | jq -r '.manifests[].digest'
```

## Notes

- **Runtime dependencies:** the binary does not bundle `git` (always required) or
  `gpg` / `ssh-keygen` (required for `verify-sig` ref policies, which shell out and
  fail closed; [#50](https://github.com/coder/openstorage/issues/50)). Unlike the
  Docker image, the bare binary bundles none of these; install them on the host.
- **First release bootstrap:** in [`release-please-config.json`](../../release-please-config.json),
  `initial-version` pins the first release to `0.1.0`. Without it, release-please
  proposes `1.0.0` and ignores the manifest's `0.0.0`. `bootstrap-sha` pins where
  changelog collection starts. Both affect only the first release; the manifest
  maintains itself afterwards.
- **Fixing a bad squash title after merge:** edit the *merged PR's body* and add
  release-please's commit-override block. Because we squash-merge, the next run
  parses it instead of the commit message:

  ```
  BEGIN_COMMIT_OVERRIDE
  fix(githttp): the corrected conventional subject
  END_COMMIT_OVERRIDE
  ```

  Never put these markers in an unmerged PR body unless you mean it: they replace
  the squash title for release purposes.
- **Tag namespace:** leave `v*` tags to release-please. A manually pushed tag
  desyncs its notion of the last release.
