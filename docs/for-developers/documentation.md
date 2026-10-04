# Build documentation

The book is mdBook, built from `docs/`. Parts of it are generated from the
code, and every code block in it is checked.

```bash,repo
python3 scripts/docs/build_single_version_docs.py preview
python3 scripts/docs/rebuild_index.py
python3 -m http.server 8000 --directory book
```

Open `http://localhost:8000` for the landing page and the book. Install the
prerequisites first:

```bash,repo
cargo install mdbook --version 0.5.2 --locked
python3 -m pip install -r scripts/requirements.txt
```

The scripts find mdBook on `PATH` or in `~/.cargo/bin/`.

## Where to edit

| File | Purpose |
|---|---|
| `docs/SUMMARY.md` | Sidebar order and page titles |
| `docs/getting-started/` | Start: install, quick start |
| `docs/user-guide/` | Use datui: one page per task |
| `docs/formats/` | Formats: the overview, then one page per family |
| `docs/reference/` | Reference: options, keys, settings, syntax, Python API |
| `docs/for-developers/` | Contribute |
| `book.toml` | mdBook settings, and a redirect for every moved page and heading |
| `README.md`, `python/README.md` | The GitHub and PyPI pages |
| `scripts/docs/index.html.j2` | The landing page at the site root |
| `docs/night-market.css` | Colors and layout |

## Style

| Rule | |
|---|---|
| Title | The H1 is the page's title in `SUMMARY.md`, in sentence case |
| Lead | One sentence, then the command or the key, then a table |
| Prose | Only for what a table cannot say. No design rationale in user pages |
| One home per fact | Sampling, what gets read, light and dark, cloud logins: one page says it, the others link to it |
| Length | About 250 lines for a guide page; a reference page of tables may run longer |
| Words | The [glossary](../reference/glossary.md); `tests/wording_test.rs` fails on a retired word |
| Claims | Verified against the code. No speed claim that was not measured |
| Links | Relative, with `.md`. Never to `plans/` or another unpublished path |

A moved page or heading gets a redirect in `book.toml`, and the README and
landing page links move in the same change.

## Generated pages

Do not edit these by hand. Change the code they come from, then write them:

```bash,repo
cargo run -p datui-cli --bin gen_docs -- write
```

| Page or region | From |
|---|---|
| `reference/command-line-options.md` | The clap `Args` and `examples.toml` in `crates/datui-cli` |
| `reference/settings.md` | The option registry, `SETTINGS` in `crates/datui-cli/src/settings.rs` |
| `reference/environment.md` | `ENVIRONMENT` in the same file |
| `reference/keyboard-shortcuts.md`, region `keys` | The help strings, `crates/datui-lib/src/help-strings/*.txt`, read by `crates/datui-cli/src/keys.rs` |
| `formats/index.md`, region `formats` | The format descriptors in `crates/datui-cli/src/formats.rs` |
| Region `format-count` in `formats/index.md`, `introduction.md` and `README.md` | The same: how many formats, and their names |
| `reference/python-api.md`, region `options` | The registry's Python keywords |

`GENERATED` in `crates/datui-cli/src/docgen.rs` lists them. A region sits
between two comments, which mdBook and GitHub hide; the text around it is
written by hand:

```text
<!-- generated: keys -->
<!-- end generated: keys -->
```

`the_generated_docs_are_current` (`scripts/dev/test.sh cli`) fails while a
committed copy differs. `gen_docs` with no argument prints the command-line
reference; with `settings`, `environment` or `keys`, that page.

## Code blocks

Every fenced block in `docs/`, the READMEs and the next release's notes is one
of three kinds, named in its info string:

| Kind | Info string | Checked |
|---|---|---|
| Runnable | `bash`, `toml`, `python`, `sql`, `q`, ... | Run, as a reader would paste it on a fresh install |
| Shape to fill in | `bash,template`, `toml,template` | Not run. Placeholders are `<UPPER_CASE>`, and the sentence before the block says what to replace. Its flags must exist; TOML must parse once filled in |
| Output | `text`, `console` | Not run: what a command prints, or a screen |

Code shown from the source (`rust`, `yaml`, `json`) is not run. More
attributes go after the language, comma-separated:

| Attribute | Means |
|---|---|
| `network` | Reads public data; runs in the Nightly job |
| `interactive` | Its producer never ends; stopped once the first rows show |
| `continue` | Runs in the directory the page's previous block ran in |
| `expect=rows`, `screen` or `exit` | What its datui command must do: show rows (the default with a path), stay up (the home screen, the hex view), or print and exit |
| `spec` | A TOML format spec, checked with `datui formats check` |
| `dataset=NAME` | A `sql` or `q` block's data, from `scripts/docs/doc_datasets.toml` |
| `rows=N` | The rows a `sql` or `q` block returns |
| `repo` | Run from a checkout of this repository; its `scripts/` paths must exist, and it is not run |
| `install` | Installs datui; `test-install.yml` covers it |
| `file=NAME` | A file the page's next runnable block uses by name; written into its directory before it runs, not run itself |

A runnable block stands alone: it uses the built-in catalog's public data,
real commands (`seq`, `printf`, `journalctl`), or the file blocks above it.
Files an example needs are titled file blocks, never heredocs; sample-data
generators are readable scripts. Each file is its own block, its name in bold
on the line above, and the command block runs it by name:

````markdown
**`make_day_l2.py`**

```python,file=make_day_l2.py
...
```

```bash
python3 make_day_l2.py
datui day.l2
```
````

The lint fails a heredoc or a `python -c` in a shell block, and a file block
the next runnable block does not name. A binary generator lays out its records
with `ctypes.LittleEndianStructure` (`_pack_ = 1`, `_layout_ = "ms"`), a field
per field of the format. A `bash` or `toml` block never starts a line
with a `# comment`; say it in the text, or at the end of a command.

The examples `datui --help`, the manpage and the command-line reference show
are `crates/datui-cli/examples.toml`: each entry's `command`, `description`,
`test` (`run`, `network` or `interactive`) and an optional `expect`.

### Run the checks

| Command | Checks |
|---|---|
| `.venv/bin/python scripts/docs/doc_examples.py --lint` | Every block's label, placeholders and flags. Needs no binary |
| `.venv/bin/python scripts/docs/doc_examples.py --bin target/debug/datui` | Runs the runnable shell and TOML blocks, and `examples.toml`'s entries, that need no network |
| `... --bin target/debug/datui --network` | The network ones instead |
| `.venv/bin/python scripts/docs/doc_examples.py --python` | The `python` blocks, with the wheel installed ([Build Python bindings](python-bindings.md)) |
| `... -k quick-start` | Only the blocks whose `file:line` or text holds the word |
| `scripts/dev/test.sh unit doc_queries` | Every `q` block parses; `sql` and `q` blocks on data that ships with the docs run |
| `python3 scripts/docs/lint_docs.py` | H1s against `SUMMARY.md`, headings in sentence case, links, redirects |
| `./scripts/docs/check_doc_links.sh book/preview` | Every link in the built book, with [lychee](https://github.com/lycheeverse/lychee); `--online` adds external URLs |

A shell block runs in an empty directory with its own `DATUI_CONFIG_DIR` and
`DATUI_CACHE_DIR`. `scripts/docs/datui_shim.py` stands in for the reader: it
runs datui on a pseudo-terminal, passes once the table shows rows, answers a
download question, and otherwise fails with the screen's last text. A failure
names the file and line.

The queries on public data run once the datasets are downloaded:

```bash,repo
.venv/bin/python scripts/docs/doc_examples.py --fetch-datasets ~/tmp/doc-data
DATUI_DOC_DATA=~/tmp/doc-data cargo test -p datui-lib --lib doc_queries -- --ignored
```

CI runs the lints and the local blocks on every pull request. Nightly runs the
network blocks, the network Python blocks and the queries on public data.
[Check the examples](examples.md) covers the numbers the guides quote.

## Build choices

| Command | Output |
|---|---|
| `python3 scripts/docs/build_single_version_docs.py` | The current checkout, under its branch name |
| `python3 scripts/docs/build_single_version_docs.py preview` | The current checkout, to `book/preview/`; the argument only names the output |
| `python3 scripts/docs/build_single_version_docs.py vX.Y.Z` | Checks out the tag, builds it, restores the checkout; use a clean worktree |
| `python3 scripts/docs/build_all_docs_local.py` | Every tag in temporary worktrees, then `latest/` and the landing page |
| `python3 scripts/docs/rebuild_index.py` | Only the landing page, from the books already built |
| `python3 -m unittest discover -s scripts/docs -p 'test_*.py'` | The build scripts' tests |

A branch build writes the command-line reference into a temporary copy; a tag
build uses the one committed with the tag. The landing page lists release and
development books. It stays a landing page; it does not redirect into a book.
Check it in light and dark, at phone and desktop widths, with the keyboard and
with JavaScript off.

## Publishing

| Path | Contents | Built |
|---|---|---|
| `/` | Landing page, from `scripts/docs/index.html.j2` | Every deploy |
| `/latest/` | A copy of the newest tagged book | On a `v*` tag (`release.yml`) |
| `/vX.Y.Z/` | That tag's book | On its tag, then from cache by the tag's SHA |
| `/dev/` | `main`'s book | On every push to `main` that touches the docs (`build-and-publish-docs.yml`), and on a release |

A deploy replaces the whole site, so both workflows build every tag (from
cache), `/latest/` and `/dev/`. A merged docs change shows at `/dev/` within
minutes and reaches `/latest/` with the next release. **Build and publish
docs** can also be run by hand. A tag's cached book is marked by
`book/<tag>/.built_sha`; remove the marker to rebuild it locally.
