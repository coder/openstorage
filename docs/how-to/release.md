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
**"action required"**. Nothing runs until a maintainer approves it, and branch
protection requires `lint` / `test` / `test-local` / `test-s3wal`.

Approve with the **Approve and run** button on the PR's Actions run, or:

```sh
gh run list -R coder/openstorage --branch release-please--branches--main \
  --workflow ci --status action_required --limit 1
gh api --method POST repos/coder/openstorage/actions/runs/<run-id>/approve
```

> [!IMPORTANT]
> Keep `--workflow ci`. The branch also gets a held run from the optional `pr-title`
> workflow. Without the filter, `--limit 1` may return that run, and approving it
> leaves the required checks blocked.

- Every push to main while the release PR is open refreshes the branch and creates
  a new held run. Approve each one.
- Approving the `pr-title` run is optional. It is deliberately **not** a required
  check, so an unapproved run cannot wedge the release PR.
- A fine-grained PAT or GitHub App token in the action's `token` input would remove
  this step. Until one exists, approve-and-run is the documented path. Do **not**
  loosen branch protection instead.

## 3. Merge the release PR

Squash-merge it as-is. Keep the `chore(main): release X.Y.Z` title so it does not
trigger another bump. The next `release-please.yml` run then:

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

## Notes

- **Runtime dependencies:** the binary does not bundle `git` (always required) or
  `gpg` / `ssh-keygen` (required for `verify-sig` ref policies, which shell out and
  fail closed; [#50](https://github.com/coder/openstorage/issues/50)). Unlike the
  Docker image, install them on the host.
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
