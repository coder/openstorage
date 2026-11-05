# scripts

## Overview

Standalone maintenance scripts for docs, releases, and the Compose quickstart. None of them is a Go package, and `go test ./...` does not run them.

- `check-doc-links.py`: an offline, standard-library-only checker for relative inline Markdown links and anchors in every `README.md` that git tracks or that is untracked and not ignored, the root `CONTRIBUTING.md` and `CHANGELOG.md`, and `docs/`. It also checks each `[path:symbol]` code citation in a `README.md`. A link target or cited file must be tracked by git, and the symbol must be a name that the file declares, or a whole-token phrase in the code of a shell script. It needs a git checkout. CI runs it in the required `lint` job of `.github/workflows/ci.yml`.
- `release-archives.sh`: cross-compiles `./cmd/openstorage` for four platforms, packages each binary with `LICENSE`, and writes `checksums.txt`. `.github/workflows/release.yml` runs it.
- `smoke-compose-quickstart.sh`: an end-to-end smoke test of `compose.quickstart.yml` against a local image. It covers key init, the HTTP API, Git and Git LFS push and clone, restart persistence, and fail-closed init faults. CI runs it in `.github/workflows/ci.yml` and `.github/workflows/image.yml`.

`test_check_doc_links.py` holds the regression tests for `check-doc-links.py`. It is a test file, not a script.

## Interface

### `check-doc-links.py`

- `[check-doc-links.py:__main__]` CLI: `python3 scripts/check-doc-links.py`. It takes no arguments. It exits 1 when `check()` returns a non-zero count, 2 when `doc_files()` cannot list the files, else 0.
- `[check-doc-links.py:check]` Checks the links of every `doc_files()` entry that `links_apply` selects, and the citations of every `README.md` among them. Prints one line per failure, then `broken links: N`. Returns N. Link failures: `BROKEN FILE`, `BROKEN FILE (escapes repo)`, `BROKEN ANCHOR`, `UNSUPPORTED REF-STYLE LINK DEF`, `UNSUPPORTED ANGLE-BRACKET DEST`, `UNSUPPORTED UNPARSED LINK`. Citation failures: `EMPTY CITATION`, `LINE NUMBER CITATION`, `UNSUPPORTED CITATION`, `MISSING CITED FILE`, `UNRESOLVED CITATION`.
- `[check-doc-links.py:doc_files]` The sorted `.md` paths that git tracks or that are untracked and not ignored, minus `vendor/` and files deleted from the working tree.
- `[check-doc-links.py:git_files]` The set of paths that `git ls-files -z` prints for the given options. Exits 2 when git fails.
- `[check-doc-links.py:tracked]` Every path in git's index and every directory that holds one, plus `.`. Cached.
- `[check-doc-links.py:links_apply]` True for every `README.md`, the root `CONTRIBUTING.md` and `CHANGELOG.md`, and every file under `docs/`.
- `[check-doc-links.py:is_readme]` True for a file named `README.md`, the only files whose citations `check` reads.
- `[check-doc-links.py:link_failures]` Failure lines for the inline links on one line, with inline code masked.
- `[check-doc-links.py:anchors]` The heading slugs and explicit anchors of one Markdown file. Cached per file.
- `[check-doc-links.py:citation_failures]` Failure lines for the citations on one raw line.
- `[check-doc-links.py:citation_problem]` The failure kind and hint for one citation, or `None` when it resolves.
- `[check-doc-links.py:LANGUAGES]` One reader per citable file kind, keyed by extension. A reader takes the file text and returns the names a citation may use.
- `[check-doc-links.py:language]` The `LANGUAGES` reader for a path, or `None`.
- `[check-doc-links.py:citable]` The names of one cited file, from its reader. Cached per file.
- `[check-doc-links.py:masked]` Blanks the comments and string literals that a lexer matches. Every line and column stays in place. A quoted literal keeps its delimiters.
- `[check-doc-links.py:go_names]` Go: functions, methods (`T.M` and `M`), types, struct fields and interface methods (`T.F` and `F`), constants, variables, and flags (`flag -name`) that the `flag` package or a `flag.FlagSet` defines.
- `[check-doc-links.py:tf_names]` Terraform: `T` and `T.N` for resources, `data.T.N`, `var.N` and `N`, `output.N` and `N`, `provider.N`, `module.N`, `local.K`, `terraform`, and `ADDRESS.B` for a block `B` nested in a resource, data, or ephemeral block, where `data.T.N` and `ephemeral.T.N` are the data and ephemeral addresses.
- `[check-doc-links.py:py_names]` Python: `def` and `class` names (`C.m` and `m`), module-level assignments and calls (`os.chdir`), and `__main__` when the file has a main guard.
- `[check-doc-links.py:js_names]` JavaScript: function, class, `const`, `let`, and `var` names that start a statement. A line after one that ends in an operator or an opening bracket (`JS_CONTINUES`) continues an expression, so a named function or class expression there is not a declaration. Object keys are not citable.
- `[check-doc-links.py:ShellMentions]` Shell: any phrase that appears as whole tokens in the script's code, with comments removed (`destroy`, `set -euo pipefail`, `config.json`). A `#` inside a quote that closes on the same line is text, not a comment. Strings and heredoc bodies count, because shell contracts cite the literal values in them.
- `[check-doc-links.py:unit_names]` systemd units: section names (`Service`). A line that continues the line before it (after a trailing `\`, with comment lines skipped) is not a header.
- `[check-doc-links.py:gh_anchor]` Converts heading text to a GitHub-style anchor slug.
- `[check-doc-links.py:ExplicitAnchors]` `HTMLParser` subclass. Collects `<a name>` and `<a id>` values into `.anchors`.
- `[check-doc-links.py:scan_blocks]` Yields the non-code Markdown blocks of one file as numbered lines.
- `[check-doc-links.py:markdown_blocks]` Splits numbered lines into blocks. Tracks fences, HTML comments, block quotes, and list items.
- `[check-doc-links.py:scan_lines]` Yields `(lineno, raw, visible)` tuples, where `visible` has inline code masked.
- `[check-doc-links.py:without_inline_code]` Masks code spans in one block and keeps line and column positions.
- `[check-doc-links.py:drop_columns]` Removes container indentation columns from a line (tabs use four-column stops).
- `[check-doc-links.py:starts_block]` True when a line starts a fence, heading, quote, list item, thematic break, or HTML comment.
- Module-level compiled regular expressions:
  - `[check-doc-links.py:CITATION]` One citation: `[`, a path whose last element has a letter-led extension or starts with `Dockerfile`, an optional `:symbol`, then `]` not followed by `(` or `[`.
  - `[check-doc-links.py:LINE_NUMBER]` A symbol that is a line number or a line range (`8`, `4-7`).
  - `[check-doc-links.py:GO_LEXER]` `[check-doc-links.py:TF_LEXER]` `[check-doc-links.py:PY_LEXER]` `[check-doc-links.py:JS_LEXER]` The comments and string literals that `masked` blanks: Go comments, strings, raw strings, and runes. Terraform `#`, `//`, and `/* */` comments, strings (with `${...}` interpolation), and heredocs. Python comments, strings, and docstrings. JavaScript comments, strings, and template literals.
  - `[check-doc-links.py:JS_CONTINUES]` The last character of a line that leaves its expression open: an operator or an opening bracket. A string keeps its quotes in the masked text, so a line that ends in a string is not an open expression.
  - `[check-doc-links.py:SHELL_LEXER]` One quoted string or backslash escape (kept), or one comment (dropped), on one line of a shell script.
  - `[check-doc-links.py:LINK]` One inline link: `](`, a destination with at most one level of balanced parentheses, an optional `#fragment`, and an optional quoted title, then `)`.
  - `[check-doc-links.py:REFDEF]` A reference-style link definition line (`[label]:` after at most three spaces).
  - `[check-doc-links.py:ANGLE]` An angle-bracket destination (`](` then optional space, then `<`).
  - `[check-doc-links.py:FENCE]` A code fence opener of three or more backticks or tildes after at most three spaces.
  - `[check-doc-links.py:HEADING]` An ATX heading: one to six `#`, at least one space, then the heading text.
  - `[check-doc-links.py:BACKTICKS]` One run of backticks (a code span delimiter).
  - `[check-doc-links.py:HTML]` An HTML comment, or an HTML start or end tag with optional attributes.
  - `[check-doc-links.py:THEMATIC]` A thematic break of three or more `*`, `-`, or `_`.
  - `[check-doc-links.py:SETEXT]` A setext heading underline of `=` or `-` characters.
  - `[check-doc-links.py:QUOTE]` A block quote marker (`>` after at most three spaces).
  - `[check-doc-links.py:ITEM]` A list item marker: `-`, `+`, `*`, or one to nine digits followed by `.` or `)`.

### `release-archives.sh`

- `[release-archives.sh:tag]` Required release tag, for example `v0.1.0`. It is stamped into `openstorage --version`.
- `[release-archives.sh:outdir]` Required output directory. The script creates it.
- `[release-archives.sh:ldflags]` Stamps `internal/version.version`, `internal/version.commit` (from `git rev-parse HEAD`), and `internal/version.date` (UTC).
- `[release-archives.sh:archive]` Output names: `openstorage_<version>_<os>_<arch>.tar.gz`, where `<version>` is the tag without the leading `v`. Plus `checksums.txt` in GNU `sha256sum` format.

### `smoke-compose-quickstart.sh`

- `[smoke-compose-quickstart.sh:OPENSTORAGE_IMAGE]` Required environment variable. A local image ID or an image reference that already exists in the local Docker daemon.
- `[smoke-compose-quickstart.sh:OPENSTORAGE_PORT]` Optional environment variable. Defaults to a free `127.0.0.1` port that a Python socket bind picks.
- `[smoke-compose-quickstart.sh:compose]` Wrapper for `docker compose -f "$root/compose.quickstart.yml" -p "$project"`.
- `[smoke-compose-quickstart.sh:up]` `compose up -d --wait --wait-timeout 60`.
- `[smoke-compose-quickstart.sh:init_shell]` Runs a `/bin/sh -ec` command in the `init` service (`--rm --no-deps -T`).
- `[smoke-compose-quickstart.sh:snapshot]` Sorted listing of metadata and `sha256sum` of every file in `/private` and `/public`.
- `[smoke-compose-quickstart.sh:api]` `curl -q --silent --show-error --fail --max-time 30` with the bearer header file `$work/auth`.
- `[smoke-compose-quickstart.sh:batch]` Sends a Git LFS batch request (`upload` or `download`) and checks the returned object `href`.
- `[smoke-compose-quickstart.sh:clone_and_compare]` Clones with `GIT_LFS_SKIP_SMUDGE=1`, runs `git lfs pull`, and compares `HEAD`, `hello.txt`, and `payload.bin` with the source.
- `[smoke-compose-quickstart.sh:destroy]` `compose --profile tools down --volumes --remove-orphans`, then `assert_deleted`.
- `[smoke-compose-quickstart.sh:assert_deleted]` Succeeds only when no container, volume, or network has the label `com.docker.compose.project=$project`.
- `[smoke-compose-quickstart.sh:cleanup]` `EXIT` trap. Destroys the main and the `-invalid` project, removes `$work`, and exits with the saved code.

No child directories.

## Invariants

### `check-doc-links.py`

- `[check-doc-links.py:doc_files]` Lists Markdown that git tracks and Markdown that is untracked and not ignored, so a new README is checked before `git add`. Git-ignored files and files under `vendor/` are never listed. A tracked file deleted from the working tree is skipped. A clean CI checkout has no untracked files, so CI reads the same set as before.
- `[check-doc-links.py:tracked]` `[check-doc-links.py:link_failures]` A link target must exist and be in git's index (or be a directory that holds an indexed file), else `BROKEN FILE (not tracked by git)`. A fragment-only link names its own file and needs no index entry. Stage a new target (`git add`, or `git add -N`) before you link to it, because a clean checkout holds only tracked files.
- `[check-doc-links.py:doc_files]` When `git ls-files` fails (outside a checkout, or without `git`), it prints the reason to stderr and exits 2 without checking anything.
- `[check-doc-links.py:links_apply]` Checks the links in every `README.md` (this one included), the root `CONTRIBUTING.md` and `CHANGELOG.md`, and every `.md` file under `docs/`. Other Markdown files, such as `.agents/skills/*/SKILL.md`, are not scanned.
- `[check-doc-links.py:os.chdir]` Resolves all paths from the repository root (the parent of `scripts/`), whatever the caller's working directory is.
- `[check-doc-links.py:link_failures]` Skips targets that match `^[a-z]+:` (URLs) or start with `/`.
- `[check-doc-links.py:link_failures]` Reports a relative target that resolves outside the repository as `BROKEN FILE (escapes repo)`.
- `[check-doc-links.py:link_failures]` Checks anchors only for `.md` targets. `[check-doc-links.py:anchors]` computes them on demand for any such file, scanned or not. Anchors of `docs/upstream/` targets are not checked, because those files are verbatim upstream copies.
- `[check-doc-links.py:REFDEF]` `[check-doc-links.py:ANGLE]` `[check-doc-links.py:LINK]` Fails closed: reference-style link definitions, angle-bracket destinations, and any `](` that `LINK` cannot consume on one line are reported as failures.
- `[check-doc-links.py:gh_anchor]` Anchor slugs: lowercase, strip backticks and asterisks, drop characters other than word characters, `-`, and space, then turn spaces into hyphens. `anchors` adds `-1`, `-2` suffixes to duplicate headings.
- `[check-doc-links.py:ExplicitAnchors]` Explicit `<a name>` and `<a id>` values keep their exact text.
- `[check-doc-links.py:markdown_blocks]` `[check-doc-links.py:without_inline_code]` Fenced, indented, and inline code create no anchors and no link destinations.
- `[check-doc-links.py:is_readme]` Checks citations only in `README.md` files.
- `[check-doc-links.py:check]` Reads citations from the raw text of non-code blocks. Citations inside code spans count. Citations inside fenced or indented code do not.
- `[check-doc-links.py:CITATION]` Recognizes a citation without looking at `LANGUAGES`. So a citation with a symbol in a file kind that has no reader fails as `UNSUPPORTED CITATION`, and a bare `[path]` of any kind needs only the file.
- `[check-doc-links.py:citation_problem]` Checks the symbol's form before the file. An empty symbol (`[path:]`) fails as `EMPTY CITATION`. A line number or range fails as `LINE NUMBER CITATION`. Cite a name instead.
- `[check-doc-links.py:citation_problem]` A citation path is relative to the directory of its `README.md`. After symlinks are resolved, it must name a file inside the repository that git's index tracks, else `MISSING CITED FILE`.
- `[check-doc-links.py:go_names]` `[check-doc-links.py:tf_names]` `[check-doc-links.py:py_names]` `[check-doc-links.py:js_names]` A citation of a Go, Terraform, Python, or JavaScript file resolves only to a declaration. `masked` runs first, so a declaration-shaped line inside a comment, a string, a raw string, a heredoc, or a docstring does not count. A call that mentions the name does not count either, except that `py_names` accepts a module-level call such as `os.chdir`. `go_names` relies on `gofmt` layout.
- `[check-doc-links.py:js_names]` A function or class expression on a line after one that ends in an operator or an opening bracket (`=`, `(`, `,`, `||`, `=>`, `?`) is not a declaration.
- `[check-doc-links.py:ShellMentions]` A citation of a shell script resolves to whole tokens in code. A `#` at the start of a line, after whitespace, or after `;`, `&`, `|`, `(`, or `)` starts a comment, unless a quote that opened earlier on the same line is still open. A quote that spans lines is not tracked, so a `#` on a later line of a multi-line string still starts a comment (fail closed), and one stray apostrophe cannot hide the comments below it. A name that appears only in a comment does not count, and `config` does not match inside `config.json`.

### `release-archives.sh`

- `[release-archives.sh:set -euo pipefail]` Any failed command, unset variable, or pipe failure aborts the script.
- `[release-archives.sh:tag]` The tag must match `^v[0-9]+\.[0-9]+\.[0-9]+`, else exit 1. The pattern has no end anchor, so suffixed tags such as `v1.2.3-rc1` pass.
- `[release-archives.sh:CGO_ENABLED]` `export CGO_ENABLED=0` asserts a pure-Go build. A transitive cgo requirement fails the darwin cross-compiles.
- `[release-archives.sh:go build]` Builds with `-trimpath` and `-ldflags "-s -w ..."`.
- `[release-archives.sh:platform]` Targets exactly `linux/amd64`, `linux/arm64`, `darwin/amd64`, `darwin/arm64`.
- `[release-archives.sh:entries]` Each archive must list exactly `openstorage` then `LICENSE` at its root, else exit 1.
- `[release-archives.sh:cmp]` The archived `LICENSE` must be byte-identical to the repository `LICENSE`.
- `[release-archives.sh:COPYFILE_DISABLE]` `COPYFILE_DISABLE=1` keeps macOS AppleDouble entries out of the archives.

### `smoke-compose-quickstart.sh`

- `[smoke-compose-quickstart.sh:set -Eeuo pipefail]` Any failure aborts. `-E` makes functions inherit the `ERR` trap. `set +x` keeps tracing off.
- `[smoke-compose-quickstart.sh:umask]` `umask 077` for every file the script creates.
- `[smoke-compose-quickstart.sh:tool]` Requires `docker`, `python3`, `curl`, `git`, and `git lfs`.
- `[smoke-compose-quickstart.sh:image_id]` `OPENSTORAGE_IMAGE` must already be local: `docker image inspect` must return an ID that matches `^sha256:[a-f0-9]{64}$`. The script never pulls or pushes.
- `[smoke-compose-quickstart.sh:GIT_CONFIG_GLOBAL]` Ignores host Git configuration: unsets every `GIT_*` variable and sets `GIT_CONFIG_GLOBAL=/dev/null`, `GIT_CONFIG_NOSYSTEM=1`, `GIT_TERMINAL_PROMPT=0`. Credentials come from `GIT_ASKPASS`, never from remote URLs.
- `[smoke-compose-quickstart.sh:missing-image.log]` `compose config -q` with an empty `OPENSTORAGE_IMAGE` must fail.
- `[smoke-compose-quickstart.sh:config.json]` The rendered Compose config must have services `init`, `server`, `cli`, volumes `private`, `public`, `data`, `pull_policy: missing` and the test image on every service, server user `openstorage`, server port `127.0.0.1:$OPENSTORAGE_PORT` to `8080`, `OPENSTORAGE_BASE_URL` set to match, server mounts `public` read-only and `data` read-write, cli mounts `private` read-only in profile `tools`, and no restart policy.
- `[smoke-compose-quickstart.sh:server.json]` The running server must use the exact image ID, run as `openstorage`, be `healthy`, and mount `<project>_public` read-only and `<project>_data` read-write.
- `[smoke-compose-quickstart.sh:compose exec]` Inside the server: non-root UID, no `/private`, `/data` writable, `/public/org.pub.pem` readable.
- `[smoke-compose-quickstart.sh:init_shell]` After init: `/private` mode 700, `/private/org.pem` 600, `/public/org.pub.pem` 644, public keys identical, and `/public` holds only `org.pub.pem`.
- `[smoke-compose-quickstart.sh:keys.repeat]` A repeat `init` run leaves the key volumes byte- and mtime-identical. Down, up, and `--force-recreate` also leave them unchanged (`keys.after`).
- `[smoke-compose-quickstart.sh:curl]` Unauthenticated `GET /api/v1/repo` and `GET /smoke.git/info/refs?service=git-upload-pack` must return HTTP 401.
- `[smoke-compose-quickstart.sh:create.json]` `POST /api/v1/repos` must return `repo_name == "smoke"` and `http_url == "$base/<repo_id>.git"`.
- `[smoke-compose-quickstart.sh:repo.after]` `GET /api/v1/repo` output and the old token must stay valid across down/up and container recreation.
- `[smoke-compose-quickstart.sh:fault]` For each init fault (`private-missing`, `private-corrupt`, `private-permissions`, `pair-mismatch`, `public-empty`, `public-mismatch`, `public-symlink`, `public-directory`, `published-only`), `up` must fail, no server may run, `init` must exit non-zero, and the key volumes must stay unchanged.
- `[smoke-compose-quickstart.sh:cleanup]` If `destroy` fails for either project, the exit code becomes 1.

## Side Effects

### `check-doc-links.py`

- `[check-doc-links.py:os.chdir]` Changes the process working directory to the repository root.
- `[check-doc-links.py:git_files]` Runs `git ls-files` (read-only).
- `[check-doc-links.py:check]` Reads Markdown files, the files they link to, and the files they cite. Writes nothing to disk. Prints results to stdout.

### `release-archives.sh`

- `[release-archives.sh:mkdir]` Creates `<outdir>`.
- `[release-archives.sh:tar]` Writes one `.tar.gz` per platform into `<outdir>`.
- `[release-archives.sh:sha256sum]` Writes `<outdir>/checksums.txt`.
- `[release-archives.sh:workdir]` Creates one `mktemp -d` directory per platform for the binary. It removes it with `rm -rf` only on success. There is no trap, so a failed build leaves the directory behind.
- `[release-archives.sh:commit]` Runs `git rev-parse HEAD` (read-only).

### `smoke-compose-quickstart.sh`

- `[smoke-compose-quickstart.sh:project]` Creates Docker containers, volumes, and networks under Compose project `openstorage-smoke-<unix-time>-<pid>` and `<project>-invalid`.
- `[smoke-compose-quickstart.sh:destroy]` Deletes those containers, volumes, and networks with `down --volumes --remove-orphans`.
- `[smoke-compose-quickstart.sh:init_shell]` Fault injection deletes, rewrites, chmods, and symlinks key files inside the test project's `/private` and `/public` volumes.
- `[smoke-compose-quickstart.sh:work]` Creates a `mktemp -d` work directory. It holds `token`, `auth`, `askpass` (mode 700), Compose and Git logs, a Git repository `source` with a 1 MiB `payload.bin`, and clones `before` and `after`. `cleanup` removes it.
- `[smoke-compose-quickstart.sh:TOKEN_FILE]` Mints a bearer token with `cli mint --iss selfhost --repo smoke --scopes repo:write,git:read,git:write --ttl 1h`.
- `[smoke-compose-quickstart.sh:api]` Sends HTTP requests to `http://127.0.0.1:$OPENSTORAGE_PORT`: `/api/v1/repo`, `/api/v1/repos`, `/smoke.git/info/refs`, and `/smoke.git/info/lfs/objects/batch`.
- `[smoke-compose-quickstart.sh:git]` Commits locally and runs `git push -u origin main` and `git clone` against the local server.
- `[smoke-compose-quickstart.sh:trap]` Traps: `EXIT` runs `cleanup`, `INT` exits 130, `TERM` exits 143, `ERR` prints the failing line without credentials or raw responses.

## Verification

- Doc links and citations: `python3 scripts/check-doc-links.py` (from the `lint` job in `.github/workflows/ci.yml`).
- Checker tests: `python3 -B -m unittest discover -s scripts -p 'test_*.py' -v` (from the `lint` job in `.github/workflows/ci.yml`). `-B` keeps `scripts/__pycache__/` out of the checkout.
- Smoke syntax: `bash -n scripts/smoke-compose-quickstart.sh` (from `.github/workflows/ci.yml`).
- Smoke run (needs Docker and a local image): `OPENSTORAGE_IMAGE=<local-image> bash scripts/smoke-compose-quickstart.sh` (from `.github/workflows/ci.yml` and `.github/workflows/image.yml`).
- Release archives (needs Go): `./scripts/release-archives.sh <tag> <outdir>` (from `.github/workflows/release.yml`). Use an `<outdir>` outside the checkout. Untracked files in the checkout make Go stamp `vcs.modified=true` (see the comment in `.github/workflows/release.yml`).
