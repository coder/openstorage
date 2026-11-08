# AGENTS.md

Rules for agents and reviewers in this repository. [CONTRIBUTING.md](CONTRIBUTING.md) has the build, test, lint and pull request conventions.

## Code Review Rules

- Replacement objects (`refs/replace/*`): keep `GIT_NO_REPLACE_OBJECTS=1` set for every git child, in gitlimit's registry (`registry.start`) and in the few runners outside it (`gitlimit.NoReplaceObjects`, the sync worker's `hermeticGitEnv`); do not add per-call handling, such as `--no-replace-objects` on one command or a replacement-aware case in one handler.
- Git's stderr: keep the existing classifiers of git's stderr (blame's range errors, grep's unreadable-object line, git apply's lines) as they are, and keep repository content out of git's stderr (for example `GIT_ATTR_SOURCE` set to the empty tree), or decide by git object state; do not add another text pattern that recognizes or rejects a forged stderr line.
