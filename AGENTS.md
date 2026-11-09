# AGENTS.md

Rules for agents and reviewers in this repository. [CONTRIBUTING.md](CONTRIBUTING.md) has the build, test, lint and pull request conventions.

## Code Review Rules

- Replacement objects (`refs/replace/*`): every git child that the server starts runs with `GIT_NO_REPLACE_OBJECTS=1`, so git reads each object as itself. gitlimit's registry sets it, and a runner outside the registry adds the variable to its environment. Do not handle replacements per call instead, such as `--no-replace-objects` on one command or a replacement-aware case in one handler. Refusing a push to `refs/replace/*` is not per-call handling.
- Git's stderr: do not add or widen a text pattern whose job is to catch a forged line in git's stderr. Repository text, such as a path with a newline, can spell any line, so such a pattern never covers every case. Keep the repository's text out of git's stderr instead (for example, `GIT_ATTR_SOURCE` set to the empty tree), or decide by object state. Updating a classifier when git changes its own message is fine, and so is replacing a classifier with an object-state check.
