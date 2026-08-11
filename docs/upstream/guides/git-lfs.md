> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Git LFS

> Track large files with Git LFS over the same JWT-authenticated remote you already use. No separate LFS server, no extra configuration.

Git LFS is supported out of the box on native Code Storage repositories and
[GitHub App sync](/docs/guides/integrations#github-app-sync) repositories. The LFS server lives at the
same hostname as your Git remote, uses the same JWT for authentication, and stores objects in
managed object storage. There is nothing to enable or configure.

LFS is not supported on public GitHub sync or generic HTTPS Git sync repositories.

## Prerequisites

Install the [`git-lfs`](https://git-lfs.com) client locally and run the one-time hook installation:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git lfs install
```

This registers the LFS smudge/clean filters in your global Git config. You only need to do it once
per machine.

## Quick start

Clone the repository, declare which paths LFS should track, and push.

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
# 1. Clone with a JWT-authenticated remote (mint via SDK or dashboard)
git clone https://t:JWT@your-org.code.storage/repo-id.git
cd repo-id

# 2. Tell LFS which paths to track
git lfs track "*.psd"
git lfs track "assets/video/**"

# 3. Commit .gitattributes FIRST
git add .gitattributes
git commit -m "track psd and video assets with LFS"

# 4. Now add and push the large files
git add design.psd assets/video/intro.mp4
git commit -m "add design assets"
git push origin main
```

The pointer files land in Git. The object bytes stream to Code Storage's LFS backend over the same
HTTPS connection.

<Warning>
  **`.gitattributes` must be committed before the files it tracks.** `git lfs track` only writes a rule into `.gitattributes`. It does not retroactively convert anything. If you commit a large file first and then commit the `.gitattributes` rule, the file is already stored as a regular Git blob and will not be moved to LFS. Always commit `.gitattributes` in its own commit (or at minimum, in the same commit *before* the tracked files are staged).

  To recover, use
  [`git lfs migrate import`](https://github.com/git-lfs/git-lfs/blob/main/docs/man/git-lfs-migrate.adoc)
  to rewrite history and convert the existing blobs to LFS pointers.
</Warning>

## How it works

Every repository exposes a Git LFS Batch API at the same host as the Git remote. The client
discovers it automatically, so you do not configure it. The endpoint it posts to is:

```
https://your-org.code.storage/repo-id.git/info/lfs/objects/batch
```

When you `git push`, the LFS client:

1. Reads `.gitattributes` to determine which paths are tracked
2. Replaces tracked file contents in the commit with pointer files (size + SHA-256 OID)
3. Posts to the LFS Batch endpoint to negotiate object upload URLs
4. Uploads the actual object bytes
5. Pushes the Git refs as usual

The same JWT that authenticates `git push` authenticates the LFS upload. There is no separate token
or credential. Required scopes match Git operations: `git:read` for `git lfs pull`/`fetch`,
`git:write` for `git lfs push`.

## Verifying tracked files

After committing, confirm what LFS is actually managing:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
# List tracking rules
git lfs track

# List files currently stored as LFS pointers
git lfs ls-files
```

If `git lfs ls-files` does not show a file you expected, it was committed as a regular blob. Check
that `.gitattributes` was committed first and that the path glob matches.

## Migrating existing large files

If you already committed large files as regular blobs, rewrite history to move them to LFS:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
# Convert all *.psd in history to LFS pointers
git lfs migrate import --include="*.psd"
git push --force origin main
```

This rewrites commits, so coordinate with collaborators before force-pushing.

## Forking and LFS objects

Server-side [forks](/docs/guides/forking) copy Git history but not LFS objects. The fork inherits pointer
files. The bytes stay under the source repo. `git lfs pull` on the fork will fail.

To carry LFS objects across, round-trip locally:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
# Clone source with all LFS objects
git clone https://t:JWT@your-org.code.storage/source-repo.git
cd source-repo
git lfs fetch --all

# Create an empty destination repo (do not use baseRepo)

# Push refs + LFS objects to the new repo
git remote set-url origin https://t:JWT@your-org.code.storage/dest-repo.git
git push origin --all
git push origin --tags
git lfs push origin --all
```

## Caveats

* **Pointer files in diffs**: Tools that don't understand LFS will show the pointer file (`version`,
  `oid`, `size` lines) instead of the real content. Install `git-lfs` everywhere the repo is checked
  out.
* **Object lifecycle**: LFS objects are scoped to the repository. Deleting the repository deletes
  its LFS objects.
* **File locking API not supported**: `git lfs lock`, `git lfs unlock`, and `git lfs locks` will
  fail. There is no server-side lock registry.
* **Maximum single file size is 5 GiB**: Enforced by the `git-lfs` client itself, which uploads each
  object in a single PUT.
