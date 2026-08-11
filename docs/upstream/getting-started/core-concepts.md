> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Core Concepts

> Learn how Code Storage works under the hood. Understand repository IDs, JWT-based authentication, permission scopes, and how customer-signed tokens give you complete control over access.

### Repository management

Code Storage repositories are identified by unique IDs. You can either let the system generate an ID
or provide your own:

```js theme={"theme":{"light":"github-light","dark":"min-dark"}}
// Auto-generated ID
const repo = await store.createRepo();
console.log(repo.id); // e.g., '123e4567-e89b-12d3-a456-426614174000'

// Custom ID with namespacing
const customRepo = await store.createRepo({ id: 'team/project-alpha' });
console.log(customRepo.id); // 'team/project-alpha'
```

Repository IDs can include `/` for organizing repositories by team, project, or user.

### Authentication & Security

All access to Code Storage requires JWT tokens signed by your organization. Each token:

* Grants access to a **single repository** (except `org:read` tokens, which are org-wide)
* Contains **explicit permission scopes**
* Has a **configurable time-to-live (TTL)**
* Is **customer-signed** for full control

The SDK helps simplify and automate the management of these tokens.

### Token structure

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "iss": "your-org", // Your organization identifier
  "sub": "ci-pipeline-prod", // Agent identity (for logging)
  "repo": "team/project-alpha", // Repository access
  "scopes": ["git:read", "git:write"], // Permissions
  "iat": 1723453189, // Issued at (Unix timestamp)
  "exp": 1723456789 // Expiration (Unix timestamp)
}
```

JWT headers must include:

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "alg": "ES256", // Algorithm (ES256 or RS256 are supported)
  "typ": "JWT" // Type
}
```

### Permission scopes

| Scope        | Description                         | Operations                                                 |
| ------------ | ----------------------------------- | ---------------------------------------------------------- |
| `git:read`   | Read repository contents            | clone, fetch, pull                                         |
| `git:write`  | Modify repository                   | push                                                       |
| `repo:write` | Create, update, delete repositories | [POST /api/repos](/docs/reference/api/repositories/create-repo) |
| `org:read`   | List repositories                   | [GET /api/repos](/docs/reference/api/repositories/list-repos)   |

Scopes match exactly, and none of them implies another. `git:write` covers push and nothing else, so
a token that clones or fetches before it pushes needs `git:read` as well.

### Key management

You manage your own signing keys in the dashboard—nothing here needs a request to us. **Create key**
on the **API Keys** page at `https://code.storage/<your-org>/keys` generates an ECDSA P-256 keypair
in your browser, registers the public half, and shows you the private key once. We keep only the
public half and look it up by the `iss` claim when verifying a token. Deleting a key from that same
page revokes it, and every token signed with it stops verifying.

For detailed authentication setup, key management, manual JWT generation, and advanced token
configuration, see [Authentication](/docs/getting-started/authentication).
