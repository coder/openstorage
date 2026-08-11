> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Forking Repositories

> Create lightweight copies of existing repositories for experimentation, templates, and isolated development.

Forking creates a new repository from an existing Code Storage repository at a specific point in
time. Unlike other integrations (which maintain a live connection), forks are independent
copies—changes to the original don't affect the fork, and vice versa.

## When to use forking

* **Templates**: Create new projects from a starter template
* **Experimentation**: Test changes without affecting the original repository
* **Snapshots**: Capture a repository's state at a specific commit or branch
* **Isolation**: Separate work across a trust or tenant boundary, or give each user an independent
  project of their own

For per-agent and per-task isolation inside a single repository, use
[ephemeral branches](/docs/guides/ephemeral-branches) instead—they share Git objects, and you can promote
the work that graduates.

## Quick start

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { GitStorage } from '@pierre/storage';

  const store = new GitStorage({
    name: 'your-org',
    key: process.env.PIERRE_PRIVATE_KEY,
  });

  // Fork at the latest commit (HEAD)
  const fork = await store.createRepo({
    id: 'my-fork',
    baseRepo: {
      id: 'template-repo', // Source repository ID
    },
  });

  // Fork at a specific branch
  const branchFork = await store.createRepo({
    id: 'feature-fork',
    baseRepo: {
      id: 'template-repo',
      ref: 'develop', // Fork from tip of 'develop' branch
    },
  });

  // Fork at a specific commit
  const commitFork = await store.createRepo({
    id: 'snapshot-fork',
    baseRepo: {
      id: 'template-repo',
      sha: 'abc123def456...', // Fork at exact commit
    },
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import os
  from pierre_storage import GitStorage

  storage = GitStorage({
      "name": "your-org",
      "key": os.environ["PIERRE_PRIVATE_KEY"],
  })

  # Fork at the latest commit (HEAD)
  fork = await storage.create_repo(
      id="my-fork",
      base_repo={
          "id": "template-repo",  # Source repository ID
      },
  )

  # Fork at a specific branch
  branch_fork = await storage.create_repo(
      id="feature-fork",
      base_repo={
          "id": "template-repo",
          "ref": "develop",  # Fork from tip of 'develop' branch
      },
  )

  # Fork at a specific commit
  commit_fork = await storage.create_repo(
      id="snapshot-fork",
      base_repo={
          "id": "template-repo",
          "sha": "abc123def456...",  # Fork at exact commit
      },
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  ctx := context.Background()

  // Fork at the latest commit (HEAD)
  fork, err := client.CreateRepo(ctx, storage.CreateRepoOptions{
  	ID: "my-fork",
  	BaseRepo: storage.ForkBaseRepo{
  		ID: "template-repo",
  	},
  })

  // Fork at a specific branch
  branchFork, err := client.CreateRepo(ctx, storage.CreateRepoOptions{
  	ID: "feature-fork",
  	BaseRepo: storage.ForkBaseRepo{
  		ID:  "template-repo",
  		Ref: "develop",
  	},
  })

  // Fork at a specific commit
  commitFork, err := client.CreateRepo(ctx, storage.CreateRepoOptions{
  	ID: "snapshot-fork",
  	BaseRepo: storage.ForkBaseRepo{
  		ID:  "template-repo",
  		SHA: "abc123def456...",
  	},
  })
  ```
</CodeGroup>

`baseRepo` takes `id`, `ref`, and `sha`. For the full parameter list, the order in which `sha`,
`ref`, and HEAD resolve, and the default branch the fork inherits, see
[`createRepo()`](/docs/reference/sdk/create-repo).

## Use cases

### Project templates

Create new projects from a starter template:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  async function createProjectFromTemplate(templateId: string, projectName: string, userId: string) {
    const project = await store.createRepo({
      id: `${userId}/${projectName}`,
      baseRepo: { id: templateId },
    });

    // Customize the new project
    await project
      .createCommit({
        targetBranch: 'main',
        commitMessage: 'Initialize project',
        author: { name: 'System', email: 'system@example.com' },
      })
      .addFileFromString('README.md', `# ${projectName}\n\nCreated by ${userId}`)
      .send();

    return project;
  }

  // Usage
  const project = await createProjectFromTemplate(
    'templates/react-starter',
    'my-new-app',
    'user-123',
  );
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  async def create_project_from_template(
      template_id: str,
      project_name: str,
      user_id: str
  ):
      project = await storage.create_repo(
          id=f"{user_id}/{project_name}",
          base_repo={"id": template_id},
      )

      # Customize the new project
      commit = project.create_commit(
          target_branch="main",
          commit_message="Initialize project",
          author={"name": "System", "email": "system@example.com"},
      )
      commit.add_file_from_string(
          "README.md",
          f"# {project_name}\n\nCreated by {user_id}"
      )
      await commit.send()

      return project

  # Usage
  project = await create_project_from_template(
      "templates/react-starter",
      "my-new-app",
      "user-123"
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  func createProjectFromTemplate(ctx context.Context, templateID, projectName, userID string) (*storage.Repo, error) {
  	// Fork the template repo
  	project, err := client.CreateRepo(ctx, storage.CreateRepoOptions{
  		ID:       userID + "/" + projectName,
  		BaseRepo: storage.ForkBaseRepo{ID: templateID},
  	})
  	if err != nil {
  		return nil, err
  	}

  	// Customize the new project
  	builder, err := project.CreateCommit(storage.CommitOptions{
  		TargetBranch:  "main",
  		CommitMessage: "Initialize project",
  		Author:        storage.CommitSignature{Name: "System", Email: "system@example.com"},
  	})
  	if err != nil {
  		return nil, err
  	}

  	_, err = builder.AddFileFromString(
  		"README.md",
  		"# "+projectName+"\n\nCreated by "+userID,
  		nil,
  	).Send(ctx)
  	if err != nil {
  		return nil, err
  	}

  	return project, nil
  }

  // Usage
  project, err := createProjectFromTemplate(context.Background(), "templates/react-starter", "my-new-app", "user-123")
  ```
</CodeGroup>

### Point-in-time snapshots

Capture a repository's state before making risky changes:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  async function createSnapshot(repoId: string, label: string) {
    const source = await store.findOne({ id: repoId });
    const commits = await source.listCommits({ limit: 1 });
    const headSha = commits.commits[0].sha;

    const snapshot = await store.createRepo({
      id: `snapshots/${repoId}/${label}`,
      baseRepo: {
        id: repoId,
        sha: headSha,
      },
    });

    return snapshot;
  }

  // Before a major refactor
  const backup = await createSnapshot('my-project', 'pre-refactor-2024-01');
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  async def create_snapshot(repo_id: str, label: str):
      source = await storage.find_one(id=repo_id)
      commits = await source.list_commits(limit=1)
      head_sha = commits["commits"][0]["sha"]

      snapshot = await storage.create_repo(
          id=f"snapshots/{repo_id}/{label}",
          base_repo={
              "id": repo_id,
              "sha": head_sha,
          },
      )

      return snapshot

  # Before a major refactor
  backup = await create_snapshot("my-project", "pre-refactor-2024-01")
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  func createSnapshot(ctx context.Context, repoID, label string) (*storage.Repo, error) {
  	// Capture the current HEAD SHA
  	source, err := client.FindOne(ctx, storage.FindOneOptions{ID: repoID})
  	if err != nil {
  		return nil, err
  	}
  	commits, err := source.ListCommits(ctx, storage.ListCommitsOptions{Limit: 1})
  	if err != nil {
  		return nil, err
  	}
  	headSHA := commits.Commits[0].SHA

  	// Fork at the captured commit
  	return client.CreateRepo(ctx, storage.CreateRepoOptions{
  		ID: "snapshots/" + repoID + "/" + label,
  		BaseRepo: storage.ForkBaseRepo{
  			ID:  repoID,
  			SHA: headSHA,
  		},
  	})
  }

  // Before a major refactor
  backup, err := createSnapshot(context.Background(), "my-project", "pre-refactor-2024-01")
  ```
</CodeGroup>

## Forking vs Syncing

| Feature    | Forking                         | Syncing                      |
| ---------- | ------------------------------- | ---------------------------- |
| Source     | Code Storage repositories       | External Git providers       |
| Connection | One-time copy                   | Continuous sync              |
| Updates    | Independent after fork          | Automatically synced         |
| Use case   | Templates, snapshots, isolation | Mirror external repositories |

Use **forking** when you want an independent copy. Use **[syncing](/docs/guides/integrations)** when you
want to maintain a live connection to an external repository.

## Limitations

* **Same organization**: You can only fork repositories within your own organization
* **No relationship tracking**: Forks are independent—there's no "parent" reference after creation
* **Full copy**: Forks include all branches and history up to the fork point
