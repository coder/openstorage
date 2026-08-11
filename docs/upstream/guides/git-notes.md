> ## Documentation Index
> Fetch the complete documentation index at: https://code.storage/docs/llms.txt
> Use this file to discover all available pages before exploring further.

# Git Notes

> Attach metadata to commits without changing commit history, and keep different note streams isolated by ref.

Git notes attach text metadata to Git objects without changing the object or the commit history
around it. They are useful for build status, review state, an agent's verdict on a change, a reward
graded on one commit, and other context that should travel with a commit but should not be committed
into the repository tree.

Code Storage supports the default Git notes ref and custom notes refs. The default is
`refs/notes/commits`, which matches Git's own default for `git notes`.

## Choose a notes ref

Use the default ref when all notes belong to one stream. Use a custom notes ref when different
systems need independent note namespaces. For example:

* `ci` for build and deploy status
* `reviews` for review summaries
* `agents/verdicts` for an agent's judgment of one commit

Pass `ref` to the SDK note methods to target a custom notes ref:

<CodeGroup>
  ```typescript TypeScript theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  const ref = 'reviews';

  await repo.createNote({
    sha: 'abc123def456...',
    note: 'Reviewed. Ready for production.',
    ref,
  });

  const note = await repo.getNote({ sha: 'abc123def456...', ref });
  ```

  ```python Python theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  ref = "reviews"

  await repo.create_note(
      sha="abc123def456...",
      note="Reviewed. Ready for production.",
      ref=ref,
  )

  note = await repo.get_note(sha="abc123def456...", ref=ref)
  ```

  ```go Go theme={null} theme={"theme":{"light":"github-light","dark":"min-dark"}}
  ref := "reviews"

  _, err := repo.CreateNote(context.Background(), storage.CreateNoteOptions{
  	SHA:  "abc123def456",
  	Note: "Reviewed. Ready for production.",
  	Ref:  ref,
  })

  note, err := repo.GetNote(context.Background(), storage.GetNoteOptions{
  	SHA: "abc123def456",
  	Ref: ref,
  })
  ```
</CodeGroup>

The REST API uses the same name: pass `ref` in the write/delete request body, or as a query
parameter when reading a note.

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
curl "$CODE_STORAGE_BASE_URL/repos/my-repo/notes" \
  -H "Authorization: Bearer $CODE_STORAGE_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "sha": "abc123def456",
    "action": "add",
    "note": "Reviewed. Ready for production.",
    "ref": "reviews"
  }'
```

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
curl "$CODE_STORAGE_BASE_URL/repos/my-repo/notes?sha=abc123def456&ref=reviews" \
  -H "Authorization: Bearer $CODE_STORAGE_TOKEN"
```

## Ref rules

Empty or omitted `ref` values use `refs/notes/commits`.

Custom notes refs are canonicalized like Git's `git notes --ref` option:

| Input                | Canonical ref        |
| -------------------- | -------------------- |
| `reviews`            | `refs/notes/reviews` |
| `notes/reviews`      | `refs/notes/reviews` |
| `refs/notes/reviews` | `refs/notes/reviews` |

If the input is already fully qualified with a `refs/` prefix, it must be a safe `refs/notes/*` ref.
Short inputs are resolved inside `refs/notes/` before the same validation runs.

The server rejects refs outside the notes namespace and unsafe ref paths, including parent
traversal, empty path segments, trailing slashes, trailing dots, and `.lock` suffixes. Invalid notes
refs return `400`.

## List notes refs

Use the notes refs list API to discover custom notes namespaces before reading individual notes. It
returns notes refs under a `refs/notes/` prefix in lexical order.

```bash theme={"theme":{"light":"github-light","dark":"min-dark"}}
curl "$CODE_STORAGE_BASE_URL/repos/my-repo/notes/refs?prefix=reviews/&limit=20" \
  -H "Authorization: Bearer $CODE_STORAGE_TOKEN"
```

```json theme={"theme":{"light":"github-light","dark":"min-dark"}}
{
  "refs": [
    {
      "cursor": "opaque-cursor",
      "ref": "refs/notes/reviews/session-a",
      "sha": "a1b2c3d4e5f6..."
    }
  ],
  "next_cursor": "opaque-cursor",
  "has_more": true,
  "prefix": "refs/notes/reviews/"
}
```

The `prefix` is canonicalized like the note `ref` above: short inputs resolve inside `refs/notes/`,
and a fully qualified `refs/notes/*` prefix also works. A trailing slash restricts the listing to
descendants of that ref. Omit `prefix` to list everything under `refs/notes/`.

| Prefix               | Canonical prefix      | Matches                                 |
| -------------------- | --------------------- | --------------------------------------- |
| *(omitted)*          | `refs/notes/`         | All notes refs                          |
| `reviews`            | `refs/notes/reviews`  | `refs/notes/reviews` and descendants    |
| `reviews/`           | `refs/notes/reviews/` | Descendants under `refs/notes/reviews/` |
| `refs/notes/reviews` | `refs/notes/reviews`  | `refs/notes/reviews` and descendants    |

Use `next_cursor` as the `cursor` query parameter to read the next page. `limit` defaults to `20`
and is capped at `100`. Responses echo the normalized full prefix and full notes ref names. Invalid
prefixes and cursors return `400`.

## Write behavior

`createNote()` creates a new note and fails if the target already has a note on the selected notes
ref. To replace a note, delete it first with `deleteNote()`, then create the new note.

`appendNote()` appends text to the note on the selected ref. If no note exists, the server creates
one.

`deleteNote()` removes the note from the selected ref.

`targetRef` in write responses is always the canonical full ref, such as `refs/notes/reviews`.
`refSha` in read responses is the current SHA of the notes ref that was read.

## Concurrency and policies

`expectedRefSha` protects the selected notes ref with optimistic concurrency. When `ref` is omitted,
the guard applies to `refs/notes/commits`. When `ref` is set, it applies to that custom notes ref.

`refPolicies` are evaluated against the selected notes ref for `createNote()`, `appendNote()`, and
`deleteNote()`. See the [Branch Protection guide](/docs/guides/branch-protection).
