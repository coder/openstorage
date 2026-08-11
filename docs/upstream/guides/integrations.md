> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Integrations

> Connect Code Storage repositories to GitHub or other external Git providers through a single API surface.

Code Storage can keep a repository in sync with an external Git host while still exposing the Code
Storage remote to your tools, automations, and users.

Use Git Sync when you want to:

* mirror a repository from GitHub, GitLab, Bitbucket, Gitea, Forgejo, Codeberg, or SourceHut
* push to Code Storage and have those writes forwarded to the upstream provider
* keep using Code Storage APIs, JWT-backed remotes, ephemeral branches, and webhooks on the mirrored
  repo

## Sync modes

Code Storage currently supports three Git Sync modes:

| Mode              | Best for                                                                  | Authentication                    |
| ----------------- | ------------------------------------------------------------------------- | --------------------------------- |
| GitHub App        | Private GitHub repositories and webhook-driven sync                       | GitHub App installation token     |
| Public GitHub     | Public GitHub repositories you want to pull from without credentials      | No GitHub credentials             |
| Generic HTTPS Git | GitLab, Bitbucket, Gitea, Forgejo, Codeberg, SourceHut, and similar hosts | Stored username/password or token |

## GitHub App sync

GitHub has the most direct SDK flow today. Create the repository with a GitHub base, then call
`pullUpstream()` whenever you want to force a refresh.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const repo = await store.createRepo({
    id: 'my-synced-repo',
    baseRepo: {
      owner: 'your-github-org',
      name: 'repository-name',
      defaultBranch: 'main',
    },
    defaultBranch: 'main',
  });

  await repo.pullUpstream();
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo = await storage.create_repo(
      id="my-synced-repo",
      base_repo={
          "owner": "your-github-org",
          "name": "repository-name",
          "default_branch": "main",
      },
      default_branch="main",
  )

  await repo.pull_upstream()
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo, err := client.CreateRepo(context.Background(), storage.CreateRepoOptions{
  	ID: "my-synced-repo",
  	BaseRepo: storage.GitHubBaseRepo{
  		Owner:         "your-github-org",
  		Name:          "repository-name",
  		DefaultBranch: "main",
  	},
  	DefaultBranch: "main",
  })

  err = repo.PullUpstream(context.Background(), storage.PullUpstreamOptions{})
  ```
</CodeGroup>

**How it works:**

* when you create a repo with [`baseRepo`](/docs/reference/sdk/create-repo), Code Storage links it to the
  specified GitHub repository
* `pullUpstream()` fetches the latest changes from GitHub
* you can then use all [SDK features](/docs/reference/sdk/create-repo) (diffs, commits, file access) on
  the synced content
* the provider is automatically set to `"github"` when using `baseRepo` with `owner` and `name`

Code Storage treats GitHub as the source of truth for synced repositories. All pushes to Code
Storage are proxied straight through to GitHub and all changes are mirrored back to Code Storage.

While an initial sync is in progress, API calls against the repository return a `409 Conflict`:

```
ApiError: repository sync in progress. please retry shortly
  status: 409,
  statusText: 'Conflict',
  method: 'GET',
  body: { error: 'repository sync in progress. please retry shortly' }
```

Build retry logic around this response when automating repository creation. Push webhook events are
emitted once the initial sync completes.

After the initial sync, reads do not wait for later upstream syncs and may temporarily be stale.
Wait for the [`repo.sync.succeeded` webhook](/docs/guides/webhooks#reposyncsucceeded) when the latest
upstream content is required.

### Git LFS on GitHub App sync

GitHub App sync repositories support [Git LFS](/docs/guides/git-lfs) over the same Code Storage remote:

* **Downloads** serve from Code Storage when the object is already mirrored. On a miss, the client
  pulls from GitHub immediately and Code Storage mirrors the object in the background for next time.
* **Uploads** are passthrough to GitHub. The client uploads object bytes directly to GitHub; Code
  Storage does not store the upload.

Public GitHub sync and generic HTTPS Git sync do not support LFS.

## Public GitHub mode

If the upstream repository is public, you can skip GitHub App auth and create a synced repository in
public mode.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const repo = await store.createRepo({
    id: 'public-upstream-repo',
    baseRepo: {
      owner: 'octocat',
      name: 'hello-world',
      defaultBranch: 'main',
      auth: {
        authType: 'public',
      },
    },
  });

  await repo.pullUpstream();
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo = await storage.create_repo(
      id="public-upstream-repo",
      base_repo={
          "owner": "octocat",
          "name": "hello-world",
          "default_branch": "main",
          "auth": {
              "auth_type": "public",
          },
      },
  )

  await repo.pull_upstream()
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo, err := client.CreateRepo(context.Background(), storage.CreateRepoOptions{
  	ID: "public-upstream-repo",
  	BaseRepo: types.GitHubBaseRepo{
  		Owner:         "octocat",
  		Name:          "hello-world",
  		DefaultBranch: "main",
  		Auth: types.GitHubBaseRepoAuth{
  			AuthType: types.GitHubBaseRepoAuthTypePublic,
  		},
  	},
  })

  err = repo.PullUpstream(context.Background(), storage.PullUpstreamOptions{})
  ```
</CodeGroup>

Public mode is a **one-time sync/import**, not continuous mirroring. It is useful for templates and
open-source repositories, but it is intentionally limited:

* does not subscribe to GitHub webhooks
* does not maintain ongoing bidirectional sync with GitHub
* changes in GitHub are not automatically mirrored into Code Storage
* changes in Code Storage are not automatically pushed back to GitHub
* the GitHub repository must be publicly accessible -- if it becomes private later, sync operations
  fail until you switch to authenticated mode

Use GitHub App mode when you need ongoing bidirectional behavior with GitHub.

### Public mode vs GitHub App sync

| Capability                     | Public mode (`authType: public`)        | GitHub App mode                                     |
| ------------------------------ | --------------------------------------- | --------------------------------------------------- |
| Sync model                     | One-time sync/import                    | Continuous sync                                     |
| Webhooks                       | Not configured                          | Configured and used for ongoing updates             |
| GitHub to Code Storage updates | One-time pull only                      | Automatically synced via webhook-driven flow        |
| Code Storage to GitHub updates | Not automatically mirrored              | Forwarded to GitHub                                 |
| Git LFS                        | Not supported                           | Supported (download mirror + upload passthrough)    |
| Access requirements            | Public repo only, no GitHub credentials | GitHub App installation with repository permissions |

## Setting up a GitHub App

To enable GitHub App sync, you need to create a GitHub App and configure it for Code Storage.

<Steps>
  <Step title="Create the app">
    Go to **Settings -> Developer settings -> GitHub Apps** and create a new GitHub App.
  </Step>

  <Step title="Set permissions">
    Repository permissions:

    * Metadata: **Read** (required)
    * Contents: **Read** for one-way sync from GitHub, or **Read and write** for bidirectional sync
    * Workflows: **Read and write** (only if pushes will change files under `.github/workflows/`)

    <Note>
      Code Storage does not request a specific permission set when it mints an installation token, so
      connecting a repository succeeds with whatever the installation grants. A missing permission fails
      when a push runs, not at setup.
    </Note>

    Webhook events:

    * Push
    * Create
    * Pull Request (optional, if you want PR sync)

    Webhook callback URL -- either let Code Storage handle callbacks for you:

    ```
    https://[your-organization].code.storage/webhooks/github
    ```

    Or [use your own handler](#handle-webhooks-yourself).
  </Step>

  <Step title="Record credentials">
    Save these values -- you will need them when configuring Code Storage:

    * GitHub App ID
    * Private Key
    * Webhook Secret
  </Step>
</Steps>

## Automatic sync with webhooks

Your GitHub App can emit webhook events to automatically sync changes. You can either handle them
yourself or let Code Storage manage them.

<div id="handle-webhooks-yourself" />

### Option A: handle webhooks yourself

If you already have a webhook handler or want more control, set the webhook callback URL to point to
your handler. Your handler should process GitHub events and call Code Storage as needed.

```js theme={"theme":{"light":"github-light","dark":"min-dark"}}
// Your webhook handler
app.post('/github-webhook', async (req, res) => {
  // Verify GitHub webhook signature
  const signature = req.headers['x-hub-signature-256'];
  if (!verifyGitHubSignature(req.body, signature, webhookSecret)) {
    return res.status(401).send('Invalid signature');
  }

  // When you receive a push event, trigger Code Storage sync
  if (req.headers['x-github-event'] === 'push') {
    const repo = await store.findOne({
      id: mapGitHubRepoToStorageId(req.body.repository.full_name),
    });

    // Manually trigger a pull from GitHub
    await repo.pullUpstream();
  }
  res.status(200).send('OK');
});
```

Use [`repo.pullUpstream()`](/docs/reference/sdk/pull-upstream) to trigger a sync from GitHub when
handling events manually. For more on webhook verification and validation, see
[Webhooks](/docs/guides/webhooks).

### Option B: let Code Storage handle webhooks

If you don't want to run your own webhook handler:

1. In your GitHub App settings, set the webhook URL to
   `https://[your-organization].code.storage/webhooks/github`
2. Generate a webhook secret and save it.
3. In the Code Storage dashboard, go to the **Integrations** tab, enter your webhook secret, and
   save.

Code Storage will handle incoming GitHub events and trigger syncs automatically.

## Generic HTTPS Git sync

Generic Git Sync covers named providers that authenticate over HTTPS using a username/password pair
or a token:

* `gitlab`
* `bitbucket`
* `gitea`
* `forgejo`
* `codeberg`
* `sr.ht` or `sourcehut`

For some providers, Code Storage can derive the public upstream host automatically:

* `gitlab` -> `gitlab.com`
* `bitbucket` -> `bitbucket.org`
* `codeberg` -> `codeberg.org`
* `sr.ht` -> `git.sr.ht`

For self-hosted providers, and for providers without a fixed public host like `gitea` and `forgejo`,
pass `upstream_host` when you create the repo.

## Generic Git setup

Generic provider setup happens in three steps:

1. create the Code Storage repository with a generic Git base
2. store the HTTPS credential for that repo
3. trigger an initial pull

### 1. Create the synced repository

This example creates a repository backed by GitLab. For self-hosted instances, include
`upstreamHost` / `upstream_host`.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const repo = await store.createRepo({
    id: 'my-gitlab-repo',
    baseRepo: {
      provider: 'gitlab',
      owner: 'your-group',
      name: 'your-repo',
      defaultBranch: 'main',
    },
    defaultBranch: 'main',
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo = await storage.create_repo(
      id="my-gitlab-repo",
      base_repo={
          "provider": "gitlab",
          "owner": "your-group",
          "name": "your-repo",
          "default_branch": "main",
      },
      default_branch="main",
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo, err := client.CreateRepo(context.Background(), storage.CreateRepoOptions{
  	ID: "my-gitlab-repo",
  	BaseRepo: storage.GenericGitBaseRepo{
  		Provider:      "gitlab",
  		Owner:         "your-group",
  		Name:          "your-repo",
  		DefaultBranch: "main",
  	},
  	DefaultBranch: "main",
  })
  ```

  ```bash HTTP theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  curl "$CODE_STORAGE_BASE_URL/repos" \
    -X POST \
    -H "Authorization: Bearer $CODE_STORAGE_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "default_branch": "main",
      "base_repo": {
        "provider": "gitlab",
        "owner": "your-group",
        "name": "your-repo",
        "default_branch": "main"
      }
    }'
  ```
</CodeGroup>

For a self-hosted provider, add `upstreamHost` / `upstream_host`:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const repo = await store.createRepo({
    id: 'my-gitea-repo',
    baseRepo: {
      provider: 'gitea',
      owner: 'your-org',
      name: 'your-repo',
      defaultBranch: 'main',
      upstreamHost: 'git.example.com',
    },
    defaultBranch: 'main',
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo = await storage.create_repo(
      id="my-gitea-repo",
      base_repo={
          "provider": "gitea",
          "owner": "your-org",
          "name": "your-repo",
          "default_branch": "main",
          "upstream_host": "git.example.com",
      },
      default_branch="main",
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  repo, err := client.CreateRepo(context.Background(), storage.CreateRepoOptions{
  	ID: "my-gitea-repo",
  	BaseRepo: storage.GenericGitBaseRepo{
  		Provider:      "gitea",
  		Owner:         "your-org",
  		Name:          "your-repo",
  		DefaultBranch: "main",
  		UpstreamHost:  "git.example.com",
  	},
  	DefaultBranch: "main",
  })
  ```

  ```bash HTTP theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  curl "$CODE_STORAGE_BASE_URL/repos" \
    -X POST \
    -H "Authorization: Bearer $CODE_STORAGE_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "default_branch": "main",
      "base_repo": {
        "provider": "gitea",
        "owner": "your-org",
        "name": "your-repo",
        "default_branch": "main",
        "upstream_host": "git.example.com"
      }
    }'
  ```
</CodeGroup>

The response includes the internal `repo_id`. Use that value in the next step.

### 2. Store the Git credential

Create a credential record for the repository. `username` is optional for providers that accept a
token on its own.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  await store.createGitCredential({
    repoId: repo.id,
    username: 'john_doe',
    password: 'YOUR_ACCESS_TOKEN_OR_PASSWORD',
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  await storage.create_git_credential(
      repo_id=repo.id,
      username="john_doe",
      password="YOUR_ACCESS_TOKEN_OR_PASSWORD",
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  _, err = client.CreateGitCredential(context.Background(), storage.CreateGitCredentialRequest{
  	RepoID:   repo.ID,
  	Username: "john_doe",
  	Password: "YOUR_ACCESS_TOKEN_OR_PASSWORD",
  })
  ```

  ```bash HTTP theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  curl "$CODE_STORAGE_BASE_URL/repos/git-credentials" \
    -X POST \
    -H "Authorization: Bearer $CODE_STORAGE_TOKEN" \
    -H "Content-Type: application/json" \
    -d '{
      "repo_id": "REPO_ID_FROM_CREATE_REPO",
      "username": "john_doe",
      "password": "YOUR_ACCESS_TOKEN_OR_PASSWORD"
    }'
  ```
</CodeGroup>

### 3. Trigger the initial pull

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  await repo.pullUpstream();
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  await repo.pull_upstream()
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  err = repo.PullUpstream(context.Background(), storage.PullUpstreamOptions{})
  ```

  ```bash HTTP theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  curl "$CODE_STORAGE_BASE_URL/repos/pull-upstream" \
    -X POST \
    -H "Authorization: Bearer $CODE_STORAGE_TOKEN" \
    -H "Content-Type: application/json"
  ```
</CodeGroup>

## How Git Sync behaves

Once a repository is configured for Git Sync:

* `git clone`, `git fetch`, and `git pull` read from Code Storage
* `repo.pullUpstream()` and `POST /api/repos/{repo_name}/pull-upstream` trigger an async refresh
  from the configured provider
* `git push` to the default remote is forwarded to the external Git host
* successful pushes trigger background sync so Code Storage storage nodes stay current

This lets you keep Code Storage as the stable endpoint for your app while still mirroring changes to
and from the external host.

Only the normal remote forwards. Pushes to the [`+ephemeral` remote](/docs/guides/ephemeral-branches)
stay in Code Storage, and sync in the other direction transfers only `refs/heads/*` and
`refs/tags/*`. Put machine-generated state—agent snapshots, preview builds, scratch commits—on
ephemeral branches to keep it out of the upstream repository.

[Git LFS](/docs/guides/git-lfs) is the exception on GitHub App sync repositories: LFS uploads pass
through to GitHub from every ref, ephemeral refs included.

## Git Sync vs Forking vs Imports

| Workflow | Source                     | Ongoing connection | Best for                               |
| -------- | -------------------------- | ------------------ | -------------------------------------- |
| Git Sync | External Git provider      | Yes                | Repositories that should stay mirrored |
| Forking  | Code Storage repository    | No                 | Templates, snapshots, isolated copies  |
| Imports  | External or local Git repo | No                 | One-time ingestion and migration       |

Use **Git Sync** when the source repository should stay connected. Use
**[Forking](/docs/guides/forking)** when you want an independent copy. Use **[Imports](/docs/guides/imports)**
when you want a one-time push into Code Storage.

## Related reference pages

* [Create repository](/docs/reference/api/repositories/create-repo)
* [Git credentials](/docs/reference/api/repositories/create-generic-git-credential)
* [Pull from upstream](/docs/reference/api/repositories/pull-upstream)
* [createRepo()](/docs/reference/sdk/create-repo)
* [pullUpstream()](/docs/reference/sdk/pull-upstream)
* [Git LFS](/docs/guides/git-lfs)

## Support

Need help integrating? Reach out directly to [jacob@pierre.co](mailto:jacob@pierre.co).
