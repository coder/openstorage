> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Branch Protection

> Embed write policies in the JWT used for Git authentication to control what kinds of writes are allowed and which refs they may touch.

Branch protection is a set of policies you can embed in the JWT that authenticates a Git remote URL
or a REST endpoint. Each generated JWT carries its own rules, so you can hand out a protected JWT to
one client and an unrestricted one to another against the same repository.

Policies live as an ordered list of per-ref rules attached to the JWT. It is accepted by the
URL-generating methods (`getRemoteURL`, `getEphemeralRemoteURL`, `getImportRemoteURL`) and by every
per-operation REST call (`createBranch`, `merge`, `createCommit`, notes, tags, and so on).

## Available policies

| Policy          | Constant                                                      | Effect                                                                                                                                                      |
| --------------- | ------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `no-force-push` | `OP_NO_FORCE_PUSH` (TypeScript/Python) / `OpNoForcePush` (Go) | Rejects any non-fast-forward ref update. Fast-forward pushes still succeed, including `git push --force` when the new commit is a descendant.               |
| `no-push`       | `OP_NO_PUSH` (TypeScript/Python) / `OpNoPush` (Go)            | Rejects any update (push, create, delete, tag, merge, commit) targeting the matched ref.                                                                    |
| `verify-sig`    | `OP_VERIFY_SIG` (TypeScript/Python) / `OpVerifySig` (Go)      | Rejects the push unless every commit it introduces carries a valid OpenPGP or SSH signature from a [registered signing key](#commit-signing-verifications). |

## Per-ref rules

The `refPolicies` option is an ordered list of `(pattern, ops)` entries, and the first match wins.

### Pattern syntax

A pattern is either an exact ref name or a prefix glob: a single trailing `*` that follows a `/`, or
the standalone `*`. There is no segment-by-segment matching. `refs/heads/feature/*` matches
everything under `refs/heads/feature/`, including nested paths like `refs/heads/feature/a/b`.

Branch shorthand is supported: a pattern that does not start with `refs/` (and is not exactly `*` or
`HEAD`) is normalized to `refs/heads/<pattern>` before matching. So `main` becomes
`refs/heads/main`, and `agents/*` becomes `refs/heads/agents/*`. Use the fully qualified form when
you want to match tags (`refs/tags/*`) or any other non-branch ref.

| Pattern        | After normalization   | Matches                      |
| -------------- | --------------------- | ---------------------------- |
| `main`         | `refs/heads/main`     | the `main` branch exactly    |
| `agents/*`     | `refs/heads/agents/*` | every branch under `agents/` |
| `refs/heads/*` | `refs/heads/*`        | every branch                 |
| `refs/tags/*`  | `refs/tags/*`         | every tag                    |
| `*`            | `*`                   | every ref (catch-all)        |

An entry with no `ops` (or an empty `ops` list) is an **explicit passthrough**: matching refs are
permitted with no policies enforced. An entry with `ops: ["no-push"]` is a **deny** for matching
refs.

### Put the `*` catch-all last

`refPolicies` is first-match-wins, so a rule with pattern `*` (or any other broad glob) **must be
the last entry** for the more specific rules above it to matter. If you put `*` first, every other
rule below it is dead code.

```ts theme={"theme":{"light":"github-light","dark":"min-dark"}}
// Wrong: catch-all swallows everything.
refPolicies: [
  { pattern: '*', ops: ['no-push'] }, // matches first, always
  { pattern: 'refs/heads/agents/*' }, // never reached
];

// Right: specific allow rules first, catch-all deny last.
refPolicies: [
  { pattern: 'refs/heads/agents/*' }, // explicit allow
  { pattern: '*', ops: ['no-push'] }, // everything else is denied
];
```

If the JWT tries to write a ref and **no** entry matches, the write is allowed. The catch-all is
what turns an allowlist into a deny-by-default policy.

## Examples

### Disable force pushes everywhere

Apply `no-force-push` to every branch with a single catch-all rule.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const url = await repo.getRemoteURL({
    permissions: ['git:read', 'git:write'],
    refPolicies: [{ pattern: 'refs/heads/*', ops: ['no-force-push'] }],
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  url = await repo.get_remote_url(
      permissions=["git:read", "git:write"],
      ref_policies=[{"pattern": "refs/heads/*", "ops": ["no-force-push"]}],
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  url, err := repo.RemoteURL(ctx, storage.RemoteURLOptions{
      Permissions: []storage.Permission{storage.PermissionGitRead, storage.PermissionGitWrite},
      RefPolicies: storage.RefPolicyList{
          {Pattern: "refs/heads/*", Ops: storage.Ops{storage.OpNoForcePush}},
      },
  })
  ```
</CodeGroup>

### Allow agents to push only to `agents/*`

Hand an autonomous agent a token that can push to `agents/*` (branch shorthand for
`refs/heads/agents/*`) and nothing else.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const agentPolicies = [
    { pattern: 'agents/*' }, // explicit allow (branch shorthand)
    { pattern: '*', ops: ['no-push'] }, // deny everything else
  ];

  const url = await repo.getRemoteURL({
    permissions: ['git:read', 'git:write'],
    refPolicies: agentPolicies,
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  agent_policies = [
      {"pattern": "agents/*"},
      {"pattern": "*", "ops": ["no-push"]},
  ]

  url = await repo.get_remote_url(
      permissions=["git:read", "git:write"],
      ref_policies=agent_policies,
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  agentPolicies := storage.RefPolicyList{
      {Pattern: "agents/*"},
      {Pattern: "*", Ops: storage.Ops{storage.OpNoPush}},
  }

  url, err := repo.RemoteURL(ctx, storage.RemoteURLOptions{
      Permissions: []storage.Permission{storage.PermissionGitRead, storage.PermissionGitWrite},
      RefPolicies: agentPolicies,
  })
  ```
</CodeGroup>

### Lock down `main`, allow everything else

A more permissive policy: `main` is read-only, but any other branch or tag is writable. Branch
shorthand expands `main` to `refs/heads/main`.

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const refPolicies = [
    { pattern: 'main', ops: ['no-push'] },
    // no catch-all, so everything else is allowed by default
  ];
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  ref_policies = [
      {"pattern": "main", "ops": ["no-push"]},
  ]
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  refPolicies := storage.RefPolicyList{
      {Pattern: "main", Ops: storage.Ops{storage.OpNoPush}},
  }
  ```
</CodeGroup>

### Apply the same policy to a REST call

`refPolicies` is accepted by every ref-mutating REST method (`createBranch`, `deleteBranch`,
`createTag`, `deleteTag`, `merge`, `createCommit`, `createCommitFromDiff`, `restoreCommit`,
`pullUpstream`, `createNote` / `appendNote` / `deleteNote`). For note writes, the policy is
evaluated against the selected notes ref. Define the policy once and reuse it:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  await repo.createBranch({
    baseRef: headSha,
    targetBranch: 'agents/research-bot',
    refPolicies: agentPolicies,
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  await repo.create_branch(
      base_ref=head_sha,
      target_branch="agents/research-bot",
      ref_policies=agent_policies,
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  _, err := repo.CreateBranch(ctx, storage.CreateBranchOptions{
      BaseRef:      headSHA,
      TargetBranch: "agents/research-bot",
      RefPolicies:  agentPolicies,
  })
  ```
</CodeGroup>

## Commit signing verifications

The `verify-sig` op rejects a push unless **every commit it introduces** is signed by a key you have
registered ahead of time. "Introduced" means commits reachable from the new ref tip but not from
that protected ref's own pre-push tip, so a fast-forward that adds three commits has all three
checked, while re-advancing the ref to commits it already contained verifies nothing.

The comparison is against the protected ref's prior tip, **not** against other refs in the repo. A
commit that already exists elsewhere is still checked the first time you bring it onto the protected
ref, so you cannot bypass the policy by pushing an unsigned commit to an unprotected ref and then
advancing the protected ref to that same commit.

Both OpenPGP and SSH signing formats Git supports are accepted matching `git config gpg.format`.

To inspect a signature yourself, [`getCommit()`](/docs/reference/sdk/get-commit) returns the commit's
armored `signature` along with the exact `payload` the signature was computed over, so you can
verify it independently of the push-time check.

### Register signing keys

`verify-sig` checks each signature against the set of public keys you have registered. Manage them
in the dashboard under **Signing Keys**. The keyring is read fresh on every push, so deleting a key
takes effect immediately on the next push.

### What gets rejected

When a `verify-sig`-covered push fails, Git reports `signature verification failed` and the storage
logs name the offending commit and ref (`verify-sig denied refs/heads/main at <sha>: <reason>`). A
commit is rejected when:

* the commit is **not signed**,
* it is signed but **no registered key matches** the signature, or
* there are **no registered signing keys** at all.

### Example: Require signed commits on `main`

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const url = await repo.getRemoteURL({
    permissions: ['git:read', 'git:write'],
    refPolicies: [{ pattern: 'refs/heads/main', ops: ['verify-sig'] }],
  });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  url = await repo.get_remote_url(
      permissions=["git:read", "git:write"],
      ref_policies=[{"pattern": "refs/heads/main", "ops": ["verify-sig"]}],
  )
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  url, err := repo.RemoteURL(ctx, storage.RemoteURLOptions{
      Permissions: []storage.Permission{storage.PermissionGitRead, storage.PermissionGitWrite},
      RefPolicies: storage.RefPolicyList{
          {Pattern: "refs/heads/main", Ops: storage.Ops{storage.OpVerifySig}},
      },
  })
  ```
</CodeGroup>

Ops combine on a single rule, so you can require signatures *and* forbid force pushes on the same
ref: `ops: ["verify-sig", "no-force-push"]`.

<Warning>
  `verify-sig` only validates signatures on commits arriving over `git push`. Commits created
  server-side through REST methods (`createCommit`, `createCommitFromDiff`, `merge`) are unsigned,
  so applying `verify-sig` to a ref those methods target will reject them. Reserve `verify-sig` for
  refs written by signed Git pushes.
</Warning>

## Where policies apply

Policies are evaluated on all ref-updating paths, including:

* `git push` over HTTPS
* Ref-mutating REST methods

## Ephemeral namespace quirk

Writes that go through the ephemeral namespace reach storage under a rewritten ref name. This
applies to pushes to `getEphemeralRemoteURL()` and to REST calls with `ephemeral: true` /
`targetIsEphemeral: true`. `refs/heads/main` becomes `refs/namespaces/ephemeral/refs/heads/main`.
Patterns are matched against that rewritten name, so `pattern: "main"` or `pattern: "refs/heads/*"`
does **not** cover the ephemeral copy of a branch.

To lock down a specific ephemeral ref, spell the rewritten path:

```ts theme={"theme":{"light":"github-light","dark":"min-dark"}}
refPolicies: [
  { pattern: 'refs/heads/main', ops: ['no-push'] }, // persistent main
  { pattern: 'refs/namespaces/ephemeral/refs/heads/main', ops: ['no-push'] }, // ephemeral main
];
```

Or match the whole ephemeral namespace with a prefix glob:

```ts theme={"theme":{"light":"github-light","dark":"min-dark"}}
refPolicies: [
  { pattern: 'refs/heads/main', ops: ['no-push'] }, // persistent main
  { pattern: 'refs/namespaces/ephemeral/*', ops: ['no-push'] }, // every ref written through the ephemeral namespace
];
```

Or use the catch-all `*` (which matches refs in any namespace) when you want one rule to cover both:

```ts theme={"theme":{"light":"github-light","dark":"min-dark"}}
refPolicies: [
  { pattern: 'refs/heads/main', ops: ['no-push'] }, // persistent main
  { pattern: '*', ops: ['no-push'] }, // everything else, including every ephemeral ref
];
```

## Legacy: `ops` on URL methods

The URL-generating methods (`getRemoteURL`, `getEphemeralRemoteURL`, `getImportRemoteURL`) also
accept a top-level `ops` array. It predates `refPolicies` and is preserved for compatibility. On
verify, the gateway folds it into the catch-all `*` rule, merging into an existing `*` entry in
`refPolicies` if one is present, or appending a new trailing `*` rule otherwise. A more specific
`refPolicies` match still wins.

Prefer `refPolicies` for new code. It covers every method that takes a policy and makes the
catch-all behavior explicit. If a token carries both, only the first match counts on a given ref.
The two are not additive.
