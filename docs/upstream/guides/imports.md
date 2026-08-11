> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Imports

> Push large repositories into Code Storage with immediate cold archival—keep disk usage low during bulk ingestion.

The `+import` endpoint is a git push namespace designed for bulk repository ingestion. When you push
to a repository using the `+import` suffix, Code Storage fans out the pack to all storage nodes as
usual, then immediately enqueues a cold-storage job. The repository is archived to object storage
before the next access rather than sitting on disk indefinitely.

Use `+import` when:

* You are migrating a large number of repositories at once
* You want to ingest repos you won't access immediately
* Disk usage during import is a concern

<Note>
  Cold archival is only enqueued on write (push). Reads—clone, fetch, pull—go through the normal
  thaw flow if the repository has been archived.
</Note>

## Setup

Add a dedicated remote by inserting `+import` before `.git` in the repository URL:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git remote add import https://t:JWT@your-org.code.storage/repo-id+import.git
```

This remote behaves like any other HTTPS remote. No special tooling required.

## Quick start

Create the repository first, then push to the import remote:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
# 1. Create the target repository
pierre repo create my-imported-repo

# 2. Add the import remote (replace org and JWT)
git remote add import https://t:eyJhbGciOiJSUzI1NiI...@your-org.code.storage/my-imported-repo+import.git

# 3. Push — cold archival is enqueued automatically after the push completes
git push import main
```

After the push succeeds, Code Storage schedules the repository for cold storage. The data is
distributed across all storage nodes first, then pushed to object storage.

## SDK workflow

Generate a JWT-authenticated import URL with the SDK, then use it with standard git commands:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { GitStorage } from '@pierre/storage';

  const store = new GitStorage({
    name: 'your-org',
    key: process.env.PIERRE_PRIVATE_KEY,
  });

  // Create the repository
  const repo = await store.createRepo({ id: 'my-imported-repo' });

  // Generate a write-capable URL for the import remote
  const importUrl = await repo.getImportRemoteURL({
    permissions: ['git:read', 'git:write'],
    ttl: 3600,
  });

  // Use in a shell command or CI step
  console.log(`git remote add import ${importUrl}`);
  console.log(`git push import main`);
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import os
  from pierre_storage import GitStorage

  storage = GitStorage({
      "name": "your-org",
      "key": os.environ["PIERRE_PRIVATE_KEY"],
  })

  # Create the repository
  repo = await storage.create_repo(id="my-imported-repo")

  # Generate a write-capable URL for the import remote
  import_url = await repo.get_import_remote_url(
      permissions=["git:read", "git:write"],
      ttl=3600,
  )

  print(f"git remote add import {import_url}")
  print("git push import main")
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  ctx := context.Background()

  // Create the repository
  repo, err := client.CreateRepo(ctx, storage.CreateRepoOptions{
  	ID: "my-imported-repo",
  })

  // Generate a write-capable URL for the import remote
  importURL, err := repo.ImportRemoteURL(ctx, storage.RemoteURLOptions{
  	Permissions: []storage.Permission{
  		storage.PermissionGitRead,
  		storage.PermissionGitWrite,
  	},
  	TTL: time.Hour,
  })

  fmt.Printf("git remote add import %s\n", importURL)
  fmt.Println("git push import main")
  ```
</CodeGroup>

## Bulk import

When importing many repositories, iterate over your source list and push each one through the import
remote:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { execSync } from 'child_process';
  import { GitStorage } from '@pierre/storage';

  const store = new GitStorage({
    name: 'your-org',
    key: process.env.PIERRE_PRIVATE_KEY,
  });

  async function importRepo(localPath: string, repoId: string) {
    const repo = await store.createRepo({ id: repoId });

    const importUrl = await repo.getImportRemoteURL({
      permissions: ['git:read', 'git:write'],
      ttl: 3600,
    });

    execSync(`git -C ${localPath} remote add cs-import ${importUrl}`);
    execSync(`git -C ${localPath} push cs-import --all`);
    execSync(`git -C ${localPath} push cs-import --tags`);
  }

  const repos = [
    { path: '/repos/service-a', id: 'service-a' },
    { path: '/repos/service-b', id: 'service-b' },
    { path: '/repos/service-c', id: 'service-c' },
  ];

  for (const r of repos) {
    await importRepo(r.path, r.id);
    console.log(`imported ${r.id}`);
  }
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import os
  import subprocess
  from pierre_storage import GitStorage

  storage = GitStorage({
      "name": "your-org",
      "key": os.environ["PIERRE_PRIVATE_KEY"],
  })

  async def import_repo(local_path: str, repo_id: str):
      repo = await storage.create_repo(id=repo_id)

      import_url = await repo.get_import_remote_url(
          permissions=["git:read", "git:write"],
          ttl=3600,
      )

      subprocess.run(["git", "-C", local_path, "remote", "add", "cs-import", import_url], check=True)
      subprocess.run(["git", "-C", local_path, "push", "cs-import", "--all"], check=True)
      subprocess.run(["git", "-C", local_path, "push", "cs-import", "--tags"], check=True)

  repos = [
      {"path": "/repos/service-a", "id": "service-a"},
      {"path": "/repos/service-b", "id": "service-b"},
      {"path": "/repos/service-c", "id": "service-c"},
  ]

  for r in repos:
      await import_repo(r["path"], r["id"])
      print(f"imported {r['id']}")
  ```
</CodeGroup>

## How it works

When you push to a `+import` remote:

1. The pack is received and unpacked at the gateway
2. Objects fan out to all three storage nodes (same as a normal push)
3. A cold-storage job is enqueued immediately—no waiting for the normal inactivity timeout
4. The repository is pushed to object storage (S3) and removed from hot disk

When you later clone or fetch from the repository:

* Code Storage detects the repository is cold and thaws it automatically
* Thaw time depends on repository size; the client receives a message to retry while thaw completes

## URL format

The import remote URL follows the same pattern as other namespaced remotes:

```
https://t:{jwt}@{org}.code.storage/{repo-id}+import.git
```

## Caveats

* **Still fans out first**: The push replicates to all storage nodes before cold archival. There is
  no way to skip replication.
* **Reads trigger thaw**: Subsequent clones or fetches will thaw the repository. This adds latency
  on first access.
* **Write only**: The `+import` namespace only affects push behavior. Clone and fetch routes ignore
  it and use the standard remote path.
