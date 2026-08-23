# Cut a release

**Goal:** ship a new codestorage version — semver tag, GitHub release with CHANGELOG
notes, and verified cross-platform binaries — with no manual version arithmetic.

Releases are driven by [release-please](https://github.com/googleapis/release-please)
([#65](https://github.com/coder/code-storage/issues/65)) and the binaries workflow
([#66](https://github.com/coder/code-storage/issues/66)):

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

release-please only reacts to [conventional-commit](https://www.conventionalcommits.org/)
**squash titles** on main. This repo's historical `<scope>: <subject>` style (`api: ...`,
`githttp: ...`) is *invisible* to it — such commits neither bump the version nor appear
in the CHANGELOG. The `pr-title` CI check ([#68](https://github.com/coder/code-storage/issues/68))
rejects non-conventional titles on every PR and re-checks on title edits. For releasable
work, title the PR:

| Squash title | Effect (pre-1.0, `bump-minor-pre-major`) |
|---|---|
| `fix: ...`, `fix(scope): ...` | patch bump, CHANGELOG "Bug Fixes" |
| `feat: ...`, `feat(scope): ...` | minor bump, CHANGELOG "Features" |
| `feat!: ...` / `BREAKING CHANGE:` footer | minor bump (major once we pass 1.0.0) |
| `perf: ...` | patch bump, CHANGELOG "Performance Improvements" |
| `docs:`, `chore:`, `ci:`, `build:`, `test:`, `refactor:` | no bump, hidden from CHANGELOG |
| anything else (`api: ...`) | ignored entirely |

## 2. Get the release PR green

After the next push to main, release-please opens (or updates) the rolling release PR.

**Known caveat** (verified empirically on the first release PR, #74): the PR is created
by `github-actions[bot]`, so GitHub creates the `pull_request` CI run but holds it at
**"action required"** — nothing executes until a maintainer approves it, while branch
protection requires `lint` / `test` / `test-local`. Approve via the **Approve and run**
button on the PR's Actions run, or:

```sh
gh run list -R coder/code-storage --branch release-please--branches--main --limit 1
gh api --method POST repos/coder/code-storage/actions/runs/<run-id>/approve
```

Each release-please refresh of the branch (any push to main while the release PR is
open) creates a fresh held run needing the same approval. The separate `pr-title`
workflow run is held the same way, but it is deliberately **not** in the required-check
set precisely so an unapproved run cannot wedge the release PR — approving it is
optional (the `chore(main): release X.Y.Z` title passes). A fine-grained PAT or GitHub
App token in the action's `token` input would remove the ritual; until one is
provisioned, approve-and-run is the documented path. Do **not** loosen branch
protection instead.

## 3. Merge the release PR

Squash-merge as-is (keep the `chore(main): release X.Y.Z` title — it must not itself
trigger a bump). The next `release-please.yml` run on that push:

1. creates tag `vX.Y.Z` and the GitHub release with the CHANGELOG section as notes, and
2. chains the `release` workflow, which builds `./cmd/codestorage` for linux/amd64,
   linux/arm64, darwin/amd64, darwin/arm64 with `CGO_ENABLED=0`
   ([`scripts/release-archives.sh`](../../scripts/release-archives.sh)), stamps
   tag/commit/date into [`codestorage version`](../reference/cli.md#codestorage-version),
   verifies checksums plus the native binary on both ubuntu and macOS runners, and
   uploads `codestorage_<version>_<os>_<arch>.tar.gz` + `checksums.txt` as release
   assets (binary at the archive root, so `mise`/`ubi`-style installers can infer it).

Watch the run: `gh run list --workflow release-please -R coder/code-storage`.

If the binaries job fails after the release exists (flaky runner, infra), fix the cause
and re-run: `gh workflow run release -R coder/code-storage -f tag=vX.Y.Z` — it rebuilds
from the tag and replaces the assets (`--clobber`).

## Notes

- **Runtime dependencies:** the released binary does not bundle `git` (always required),
  or `gpg` / `ssh-keygen` (required for `verify-sig` ref policies, which shell out and
  fail closed — [#50](https://github.com/coder/code-storage/issues/50)). Unlike the
  Docker image, a bare binary brings none of these; install them on the host.
- **First release bootstrap:** `initial-version` in
  [`release-please-config.json`](../../release-please-config.json) pins the first
  release to `0.1.0` (without it, release-please's first-release path proposes `1.0.0`
  and ignores the manifest's `0.0.0`), and `bootstrap-sha` pins where changelog
  collection starts. Both only affect the first release; the manifest is
  self-maintaining afterwards.
- **Retroactively fixing a bad squash title:** if a releasable commit already landed
  on main with a non-conventional title, edit the *merged PR's body* to add
  release-please's commit-override block — the next run parses it instead of the
  commit message (works because we squash-merge):

  ```
  BEGIN_COMMIT_OVERRIDE
  fix(githttp): the corrected conventional subject
  END_COMMIT_OVERRIDE
  ```

  Never put these markers in an unmerged PR body unless you mean it — they replace
  the squash title for release purposes.
- **Tag namespace:** leave `v*` tags to release-please. A manually pushed tag would
  desync its notion of the last release.
