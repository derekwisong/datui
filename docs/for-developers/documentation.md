# Documentation

Datui uses [mdBook][mdbook] to build static documentation web pages from markdown files. The published docs site (GitHub Pages) contains **tagged releases only**; docs are built and deployed when you push a version tag (see [Release workflow](#release-workflow)).

> The documentation markdown files live in the [docs](https://github.com/derekwisong/datui/tree/main/docs) subdirectory.

## Prerequisites

- **mdbook** — required for all doc builds:
  ```bash
  cargo install mdbook
  ```
  If you used the [Setup Script](contributing.md#setup-script), mdbook may already be installed. The build scripts look for it in `PATH` or `~/.cargo/bin/`.

- **Python 3 + scripts/requirements.txt** — required only when building docs for the current branch (e.g. `main`) or when generating command-line options. Tag builds use the committed `command-line-options.md` for that tag.

## Build documentation locally

### Build all tagged versions (matches production)

This builds docs for every `v*` tag and rebuilds the index. It **skips** any tag whose `book/<tag>/` was already built for the same git SHA, so re-running only rebuilds new or changed tags.

```bash
python3 scripts/docs/build_all_docs_local.py
```

Output goes to `book/`. At the end you can start a local HTTP server to browse, or open `book/index.html` in a browser.

To **force a full rebuild** (e.g. after changing the build script or cleaning up):

```bash
rm -rf book && python3 scripts/docs/build_all_docs_local.py
```

### Build a single version

Useful for quick iteration on one tag or to preview the current branch:

```bash
# Build one tag (e.g. v0.2.22)
python3 scripts/docs/build_single_version_docs.py v0.2.22

# Build current branch (e.g. main) — output in book/main/
python3 scripts/docs/build_single_version_docs.py main
```

A single-version build does **not** update the index. The index only lists tagged versions, so `book/main/` will not appear there; open `book/main/index.html` directly to preview.

## Generated reference: command-line options

`docs/reference/command-line-options.md` is **generated** from the application’s Clap definitions. Do not edit it manually.

The doc build copies `docs/` into a temp directory, generates the CLI options into that copy (for non-tag builds), then runs mdbook from the temp tree. Tag builds use the committed file for that tag.

To generate the options file on demand:

```bash
python3 scripts/docs/generate_command_line_options.py              # print to terminal
python3 scripts/docs/generate_command_line_options.py -o path.md   # write to path
```

The release workflow and `gen_docs` use this script when building docs.

## Check documentation links

Use [lychee](https://github.com/lycheeverse/lychee) to check for broken links:

```bash
cargo install lychee
./scripts/docs/check_doc_links.sh [--build] [--online] [PATH]
```

- **`--build`** — Build docs for `main` first (`build_single_version_docs.py main`), then check that tree.
- **`--online`** — Check external URLs as well (default is offline, internal links only).
- **`PATH`** — Directory to check (default: `book/main`). Use a tag directory after building all docs, e.g. `book/v0.2.22`.

Examples:

```bash
./scripts/docs/check_doc_links.sh --build              # build main, then check book/main
python3 scripts/docs/build_all_docs_local.py && \
  ./scripts/docs/check_doc_links.sh book/v0.2.22         # check a tag after full build
```

The script exits with a non-zero code if any broken links are found.

## Release workflow

Docs are built and deployed only when a version tag (`v*`) is pushed. The release workflow:

1. **Computes a cache key** from the set of all `v*` tags and their current SHAs. The key changes when a tag is added or when a tag’s SHA changes (e.g. force-move).
2. **Restores** the `book/` directory from cache (if any) so previous tag builds can be reused.
3. **For each tag**, builds docs only if `book/<tag>/.built_sha` is missing or does not match the tag’s current SHA. Otherwise the cached build for that tag is skipped.
4. Runs **rebuild_index.py** to regenerate the index from the tag directories.
5. **Prepares the Pages artifact** by copying `book` to a deploy tree and removing cache metadata (`.built_sha`), then uploads that tree to GitHub Pages.

So the first run (or after cache eviction) builds all tags; later runs only build new or changed tags. This keeps release job time down as the number of tags grows.

Scripts involved:

- **build_single_version_docs.py** — Builds one version (tag or branch). Used by CI and by `build_all_docs_local.py`.
- **build_all_docs_local.py** — Builds all tags locally with the same skip-if-built logic for fast re-runs.
- **rebuild_index.py** — Scans `book/` for `v*` version dirs and generates `book/index.html` from `index.html.j2`.

---

[mdbook]: https://rust-lang.github.io/mdBook
