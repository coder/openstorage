> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Ephemeral Branches

> Spin up disposable branches for previews, CI artifacts, and experiments—then promote them when ready.

Ephemeral branches give you a disposable ref space for preview environments, CI artifacts, and other
short-lived workflows. They never sync to GitHub and stay fully isolated from your normal branches,
but you can promote them whenever you’re ready. Under the hood they share the same Git objects as
your other branches, keeping storage efficient and fetches fast.

## Setup

Create a dedicated remote by inserting `+ephemeral` before `.git` in the repository URL:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git remote add ephemeral https://git.example.com/org/repo+ephemeral.git
```

This remote behaves like any other Git remote—no special tooling required.

## Quick start

Push and pull ephemeral branches with standard Git commands:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git push ephemeral feature-branch
git pull ephemeral feature-branch
```

## Promotion (ephemeral → default)

Promote an ephemeral branch to your main remote when it’s ready for production:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git fetch ephemeral feature:feature
git push origin feature
```

## Important notes

* **Base**: An ephemeral branch always starts from a base you name—`baseBranch`/`baseRef` through
  the API, or a clone of the normal remote when you work over git. The ephemeral remote has no
  default branch of its own.
* **Isolation**: Ephemeral branches never overlap with your default branches until you promote them.
* **GitHub separation**: They remain inside Code Storage—they are not mirrored to GitHub. On a
  [synced repository](/docs/guides/integrations), a push to a normal branch is forwarded to the upstream
  host, but a push to the ephemeral remote is not, so this is where machine-generated state belongs.
* **Shared objects**: Ephemeral branches reuse existing Git objects for efficient storage.
* **REST + Git**: Git operations work as usual, and REST endpoints that accept `ephemeral` or
  `ephemeral_base` can read/write ephemeral refs (for example list/get files, branch diffs, and
  branch promotion).

## SDK Workflows

### Create an ephemeral branch

Use `ephemeral=True` when creating commits, reading files, or listing files in the Python SDK (and
`ephemeral: true` in TypeScript).

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Create an ephemeral branch off of "main"
  const preview = await repo
    .createCommit({
      targetBranch: 'preview/pr-123',
      baseBranch: 'main',
      ephemeral: true, // Keep the branch in the ephemeral namespace
      commitMessage: 'Preview environment for PR 123',
      author: { name: 'CI Bot', email: 'ci@example.com' },
    })
    .addFileFromString('index.html', '<h1>Preview</h1>')
    .send();

  // Access content from the ephemeral branch
  const response = await repo.getFileStream({
    path: 'index.html',
    ref: 'preview/pr-123',
    ephemeral: true, // Read from the ephemeral namespace
  });
  const html = await response.text();
  console.log(html);

  // List files that live under the ephemeral branch
  const files = await repo.listFiles({
    ref: 'preview/pr-123',
    ephemeral: true,
  });
  console.log(files.paths);
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  # Create an ephemeral branch off of 'main'
  result = await (
      repo.create_commit(
          target_branch="preview/pr-123",
          base_branch="main",
          ephemeral=True,  # Mark the target branch as ephemeral
          commit_message="Preview environment for PR 123",
          author={"name": "CI Bot", "email": "ci@example.com"},
      )
      .add_file_from_string("index.html", "<h1>Preview</h1>")
      .send()
  )

  # Access files from the ephemeral branch
  response = await repo.get_file_stream(
      path="index.html",
      ref="preview/pr-123",
      ephemeral=True,  # Read from ephemeral namespace
  )
  content = await response.aread()

  # List files in the ephemeral branch
  files = await repo.list_files(
      ref="preview/pr-123",
      ephemeral=True,
  )
  print(files["paths"])
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Create an ephemeral branch off of "main"
  builder, err := repo.CreateCommit(storage.CommitOptions{
  	TargetBranch:  "preview/pr-123",
  	BaseBranch:    "main",
  	Ephemeral:     true,
  	CommitMessage: "Preview environment for PR 123",
  	Author:        storage.CommitSignature{Name: "CI Bot", Email: "ci@example.com"},
  })
  _, err = builder.AddFileFromString("index.html", "<h1>Preview</h1>", nil).Send(context.Background())

  // Access files from the ephemeral branch
  ephemeral := true
  resp, err := repo.FileStream(context.Background(), storage.GetFileOptions{
  	Path:      "index.html",
  	Ref:       "preview/pr-123",
  	Ephemeral: &ephemeral,
  })
  defer resp.Body.Close()

  // List files in the ephemeral branch
  files, err := repo.ListFiles(context.Background(), storage.ListFilesOptions{
  	Ref:       "preview/pr-123",
  	Ephemeral: &ephemeral,
  })
  fmt.Println(files.Paths)
  ```
</CodeGroup>

### Branch from an ephemeral base

Use `ephemeral_base=True` when branching off another ephemeral branch (requires `base_branch`).

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const variant = await repo
    .createCommit({
      targetBranch: 'preview/pr-123-variant',
      baseBranch: 'preview/pr-123',
      ephemeral: true,
      ephemeralBase: true, // Seed from another ephemeral branch
      commitMessage: 'Variant of preview environment',
      author: { name: 'CI Bot', email: 'ci@example.com' },
    })
    .addFileFromString('variant.txt', 'This is a variant\n')
    .send();
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  # Create an ephemeral branch off another ephemeral branch
  result = await (
      repo.create_commit(
          target_branch="preview/pr-123-variant",
          base_branch="preview/pr-123",
          ephemeral=True,
          ephemeral_base=True,  # Indicates the base branch is also ephemeral
          commit_message="Variant of preview environment",
          author={"name": "CI Bot", "email": "ci@example.com"},
      )
      .add_file_from_string("variant.txt", "This is a variant\n")
      .send()
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Create an ephemeral branch off another ephemeral branch
  builder, err := repo.CreateCommit(storage.CommitOptions{
  	TargetBranch:  "preview/pr-123-variant",
  	BaseBranch:    "preview/pr-123",
  	Ephemeral:     true,
  	EphemeralBase: true,
  	CommitMessage: "Variant of preview environment",
  	Author:        storage.CommitSignature{Name: "CI Bot", Email: "ci@example.com"},
  })
  _, err = builder.AddFileFromString("variant.txt", "This is a variant\n", nil).
  	Send(context.Background())
  ```
</CodeGroup>

### Promote an ephemeral branch

Promote an ephemeral branch with `promote_ephemeral_branch()` / `promoteEphemeralBranch()`; omit
`target_branch` to keep the same name.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Promote an ephemeral branch in place
  await repo.promoteEphemeralBranch({
    baseBranch: 'preview/pr-123',
  });

  // Or promote it to a brand-new target name
  const result = await repo.promoteEphemeralBranch({
    baseBranch: 'preview/pr-123',
    targetBranch: 'feature/awesome-change',
  });
  console.log(result.targetBranch);
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  # Promote an ephemeral branch to a persistent branch (keeping the same name)
  result = await repo.promote_ephemeral_branch(base_branch="preview/pr-123")

  # Or provide a new target name
  result = await repo.promote_ephemeral_branch(
      base_branch="preview/pr-123",
      target_branch="feature/awesome-change",
  )

  print(result["target_branch"])  # "feature/awesome-change"
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  // Promote an ephemeral branch in place
  result, err := repo.PromoteEphemeralBranch(context.Background(), storage.PromoteEphemeralBranchOptions{
  	BaseBranch: "preview/pr-123",
  	// TargetBranch: "feature/awesome-change", // optional, defaults to BaseBranch
  })
  fmt.Println(result.TargetBranch)

  // Or promote it to a brand-new target name
  renamed, err := repo.PromoteEphemeralBranch(context.Background(), storage.PromoteEphemeralBranchOptions{
  	BaseBranch:   "preview/pr-123",
  	TargetBranch: "feature/awesome-change",
  })
  fmt.Println(renamed.TargetBranch)
  ```
</CodeGroup>
