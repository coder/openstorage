> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Authentication & Security

> Every request uses JWT tokens you sign and control. Generate short-lived tokens for CI/CD, long-lived tokens for development, and manage permissions with fine-grained scopes—no third-party authentication required.

All access to Code Storage requires JWT tokens signed by your organization. Each token:

* Grants access to a **single repository** (except `org:read` tokens, which are org-wide)
* Contains **explicit permission scopes**
* Has a **configurable time-to-live (TTL)**
* Is **customer-signed** for full control

The SDK helps simplify and automate the management of these tokens.

## Token structure

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "iss": "your-org", // Your organization identifier
  "sub": "ci-pipeline-prod", // Agent identity (for logging)
  "repo": "team/project-alpha", // Repository access (omit for org-wide tokens)
  "scopes": ["git:read", "git:write"], // Permissions
  "iat": 1723453189, // Issued at (Unix timestamp)
  "exp": 1723456789 // Expiration (Unix timestamp)
}
```

> SDK note: the client normalizes Git status codes in `state` to descriptive values and provides the
> original status under `rawState` alongside camelCase property names.

JWT headers must include:

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "alg": "ES256", // Algorithm (ES256 or RS256 are supported)
  "typ": "JWT" // Type
}
```

## Permission scopes

| Scope        | Description                         | Operations                                                 |
| ------------ | ----------------------------------- | ---------------------------------------------------------- |
| `git:read`   | Read repository contents            | clone, fetch, pull                                         |
| `git:write`  | Modify repository                   | push                                                       |
| `repo:write` | Create, update, delete repositories | [POST /api/repos](/docs/reference/api/repositories/create-repo) |
| `org:read`   | List repositories                   | [GET /api/repos](/docs/reference/api/repositories/list-repos)   |

Scopes are matched exactly, and none of them implies another. In particular `git:write` does not
grant read: `git clone`, `git fetch`, and `git pull` all run `git-upload-pack`, which requires
`git:read`. A token that clones or fetches before it pushes needs both, which is why every example
on this page lists both.

## Key management

You manage your own signing keys in the dashboard—nothing here needs a request to us. Open **API
Keys** at `https://code.storage/<your-org>/keys` and choose **Create key**. Your browser generates
an ECDSA P-256 keypair locally, registers the public half with Code Storage, and shows you the
private key once. Copy it then—it is never stored on our side and cannot be shown again. The same
step is built into the onboarding flow when you first set up an organization.

The private key is PKCS8 PEM, which is exactly what the SDK clients and the signing snippets below
expect. We keep only the public half and look it up by your organization identifier (the `iss`
claim) when verifying a token.

Deleting a key from that same page revokes it immediately: every token signed by it stops verifying
on the next request. That is the emergency stop, and it is all-or-nothing for that key—it
invalidates every token signed with it, not one agent's.

## Manual JWT generation

For advanced scenarios—such as integrating with custom Git tooling or provisioning tokens outside
the SDK clients—you can generate JWTs directly using your preferred JWT library (TypeScript) or the
Python helper. They accept your PEM-encoded private key, repository URL (name), and desired scopes,
then return a ready-to-use token:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  import { promises as fs } from 'node:fs';
  import { importPKCS8, SignJWT } from 'jose';

  // Load your private key from disk (PKCS8 PEM)
  const keyPem = await fs.readFile('path/to/key.pem', 'utf8');
  const key = await importPKCS8(keyPem, 'ES256');

  const now = Math.floor(Date.now() / 1000);
  const token = await new SignJWT({
    iss: 'your-name', // e.g., 'v0'
    sub: 'ci-pipeline-prod',
    repo: 'team/project-alpha',
    scopes: ['git:write', 'git:read'],
    iat: now,
    exp: now + 3600, // 1 hour
  })
    .setProtectedHeader({ alg: 'ES256', typ: 'JWT' })
    .sign(key);

  const gitUrl = `https://t:${token}@your-name.code.storage/team/project-alpha.git`;
  console.log(`git clone ${gitUrl}`);
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  from pathlib import Path
  from pierre_storage import generate_jwt

  # Read your private key (PKCS8 PEM)
  private_key = Path("path/to/key.pem").read_text()

  # Generate a JWT token
  token = generate_jwt(
      key_pem=private_key,
      issuer="your-name",  # e.g., 'v0'
      repo_id="team/project-alpha",
      scopes=["git:write", "git:read"],  # optional, defaults to git:write/git:read
      ttl=3600,  # optional, defaults to 1 year (seconds)
  )

  git_url = f"https://t:{token}@your-name.code.storage/team/project-alpha.git"
  print(f"git clone {git_url}")
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  package main

  import (
  	"crypto/ecdsa"
  	"crypto/x509"
  	"encoding/pem"
  	"fmt"
  	"log"
  	"os"
  	"time"

  	"github.com/golang-jwt/jwt/v5"
  )

  func main() {
  	keyPem, err := os.ReadFile("path/to/key.pem")
  	if err != nil {
  		log.Fatalf("failed to read private key: %v", err)
  	}

  	privateKey, err := parseECPrivateKey(keyPem)
  	if err != nil {
  		log.Fatalf("failed to parse private key: %v", err)
  	}

  	now := time.Now()
  	claims := jwt.MapClaims{
  		"iss":    "your-name",
  		"sub":    "ci-pipeline-prod",
  		"repo":   "team/project-alpha",
  		"scopes": []string{"git:write", "git:read"},
  		"iat":    now.Unix(),
  		"exp":    now.Add(time.Hour).Unix(),
  	}

  	token := jwt.NewWithClaims(jwt.SigningMethodES256, claims)
  	signed, err := token.SignedString(privateKey)
  	if err != nil {
  		log.Fatalf("failed to sign the token: %v", err)
  	}

  	cloneURL := fmt.Sprintf("https://t:%s@your-name.code.storage/team/project-alpha.git", signed)
  	fmt.Println("git clone", cloneURL)
  }

  func parseECPrivateKey(pemBytes []byte) (*ecdsa.PrivateKey, error) {
  	block, _ := pem.Decode(pemBytes)
  	if block == nil {
  		return nil, fmt.Errorf("failed to decode private key PEM")
  	}

  	if key, err := x509.ParsePKCS8PrivateKey(block.Bytes); err == nil {
  		if ecKey, ok := key.(*ecdsa.PrivateKey); ok {
  			return ecKey, nil
  		}
  		return nil, fmt.Errorf("private key is not ECDSA")
  	}

  	if ecKey, err := x509.ParseECPrivateKey(block.Bytes); err == nil {
  		return ecKey, nil
  	}

  	return nil, fmt.Errorf("unsupported private key format")
  }
  ```
</CodeGroup>

**Parameters**

* `keyPem` / `key_pem` (required): Private key in PKCS8 PEM format. RSA and EC (P-256/384/521) keys
  are supported.
* `issuer` (required): Customer identifier (for example `your-name` in HTTPS remotes).
* `repoId` / `repo_id` (required): Repository ID (name) the token will access.
* `scopes` (optional): Explicit permissions. Defaults to `["git:write", "git:read"]`. Available
  scopes: `git:read`, `git:write`, `repo:write`.
* `ttl` (optional): Token lifetime in seconds. Defaults to `31536000` (1 year).

The helper auto-detects the key type and signs with `RS256` or `ES256` as appropriate.
