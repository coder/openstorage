> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Git Operations

> Use standard Git commands with Code Storage over HTTPS. Clone, push, pull, and fetch with JWT-authenticated remotes.

Code Storage supports standard Git operations over HTTPS. You can use any Git client with
JWT-authenticated remote URLs.

## Authentication format

Git commands use HTTP Basic Auth with a JWT-backed remote. The format is consistent across every
repository:

* **Username**: Always `t` (for "token")
* **Password**: Your JWT token
* **Repository**: Must match the `repo` claim in your JWT

Example:

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git clone https://t:eyJhbGciOiJSUzI1NiI...@your-name.code.storage/repo-id.git
```

The SDK and dashboard mint these URLs for you, but you can also construct them manually once you
have a JWT. If authentication fails, confirm that:

* The JWT `repo` claim matches the repository you are cloning or pushing.
* The token includes the scopes required for the command (`git:read`, `git:write`, or `repo:write`).
* The token has not expired (`exp` claim)

## Supported commands

Every command uses the same remote syntax and JWT authentication scheme.

### Read operations (`git:read`)

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git clone https://t:JWT@your-name.code.storage/repo-id.git
git fetch origin
git pull origin main
```

### Write operations (`git:write`)

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
git push origin main
git push --tags
git push --force
```

The SDK's [`repo.getRemoteURL()`](/docs/reference/sdk/get-remote-url) helper generates the JWT-backed URL
for you. You can also mint JWTs manually through the authentication flow described in
[Authentication & Security](/docs/getting-started/authentication#manual-jwt-generation). Once you have
the URL, Git behaves exactly the way it does against any other HTTPS remote.

## Integration patterns

Use scoped JWTs to differentiate between automation, developers, and product-level access. Short
TTLs keep CI credentials disposable, while longer-lived tokens can be issued to developer machines
or [preview environments](/docs/guides/ephemeral-branches).

### CI/CD pipeline

```js theme={"theme":{"light":"github-light","dark":"min-dark"}}
// Generate short-lived token for CI
const token = await repo.getRemoteURL({
  permissions: ['git:read', 'git:write'],
  ttl: 3600, // 1 hour
});

// Use in CI script
await exec(`git clone ${token}`);
await exec('pnpm test');
await exec('git push origin main');
```

### Development environment

```js theme={"theme":{"light":"github-light","dark":"min-dark"}}
// Generate long-lived token for development
const devUrl = await repo.getRemoteURL({
  permissions: ['git:read', 'git:write'],
  ttl: 2592000, // 30 days
});

console.log(`Add to .git/config: ${devUrl}`);
```

When in doubt, mint tokens via the [SDK](/docs/reference/sdk)—its helpers manage scope validation and URL
formatting for you.
