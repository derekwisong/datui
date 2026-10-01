# Documentation

```bash
python3 scripts/docs/build_single_version_docs.py
python3 scripts/docs/rebuild_index.py
python3 -m http.server 8000 --directory book
```

Open `http://localhost:8000` to preview the landing page and the book for your
current checkout. A branch named `docs/revision` builds to
`book/docs/revision/`. Install the prerequisites first:

```bash
cargo install mdbook --version 0.5.2 --locked
python3 -m pip install -r scripts/requirements.txt
```

Use the project's virtual environment for the Python commands if needed.
The scripts find mdBook on `PATH` or in `~/.cargo/bin/`.

## Where to edit

| File | Purpose |
|---|---|
| `README.md` | GitHub introduction, install commands and links into the manual |
| `scripts/docs/index.html.j2` | Landing page at the site root |
| `docs/introduction.md` | The manual's task index |
| `docs/SUMMARY.md` | Sidebar order and chapter titles |
| `docs/user-guide/` | Task instructions and explanations |
| `docs/reference/` | Keys, query grammar and generated CLI options |
| `docs/night-market.css` | mdBook colors and layout |

Write the command or key first. Use a table for choices. Explain the result
and any limit that changes how to use it. Verify behavior against the code;
avoid speed claims that have not been measured.

Keep manual links relative and use `.md` extensions in Markdown. The landing
page also uses relative links, so the same output works at `/datui/` on
GitHub Pages or at the root of a future custom domain. Registering a domain,
DNS and Pages configuration are separate deployment steps.

## Build choices

| Command | Output and behavior |
|---|---|
| `python3 scripts/docs/build_single_version_docs.py` | Builds the current checkout, under its branch name |
| `python3 scripts/docs/build_single_version_docs.py preview` | Builds the current checkout to `book/preview/`; a branch argument names the output, it does not check out that branch |
| `python3 scripts/docs/build_single_version_docs.py vX.Y.Z` | Checks out the tag, builds it, then restores the checkout; use a clean worktree |
| `python3 scripts/docs/build_all_docs_local.py` | Builds all tags in temporary worktrees, updates `latest/`, then the landing page |
| `python3 scripts/docs/rebuild_index.py` | Renders only the landing page from existing built books |

Single-version builds do not rebuild the landing page. The index lists built
release and development books, including branch names with slashes. It ignores
asset directories and incomplete builds.

The primary documentation link uses `latest/` when that book exists,
otherwise the newest built release, then a development book. With no books,
it links to the source docs on GitHub. A missing demo is omitted. The site
root stays a landing page; it does not redirect into a manual.

## Generated CLI reference

Do not edit `docs/reference/command-line-options.md` by hand. Change the Clap
help text in `crates/datui-cli`, then regenerate:

```bash
python3 scripts/docs/generate_command_line_options.py -o docs/reference/command-line-options.md
```

Branch doc builds generate the reference in a temporary copy. Tag builds use
the committed reference from that tag. Building the generator needs Rust.

## Check the result

```bash
python3 -m unittest discover -s scripts/docs -p 'test_*.py'
./scripts/docs/check_doc_links.sh book/preview
```

The numbers quoted from the public datasets have their own check; see
[Check the examples](examples.md).

The link check needs [lychee](https://github.com/lycheeverse/lychee):
`cargo install lychee`. It checks local targets and fragments by default;
`--online` adds external URLs. `--build` builds the current checkout into
`book/main/` before checking.

Also inspect the landing page in light and dark mode, at phone and desktop
widths, with keyboard navigation and with JavaScript disabled. Install and
documentation links must work without scripts. The GIF starts only when the
visitor chooses Play and can be stopped.

## Publishing

The release workflow builds tagged documentation when a `v*` tag is pushed.
**Build and publish docs** can also be run manually. Both publish:

| Path | Contents |
|---|---|
| `/` | Landing page from the workflow checkout's template |
| `/latest/` | Copy of the newest tagged book |
| `/vX.Y.Z/` | Book built from that tag's files |

Merging a documentation PR does not replace the books for existing tags.
The revised manual reaches `/latest/` with the next release. A manual workflow
run can update the landing page without making a release.

Tag builds are cached by tag SHA in `book/<tag>/.built_sha`; deployment removes
those marker files. To rebuild one cached tag locally, remove its marker and
run the all-version builder again. Do not publish local development previews
as a release book.
