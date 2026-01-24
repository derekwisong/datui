# Documentation

Datui uses [mdBook][mdbook] to build static documentation web pages from markdown files.

> The documentation markdown files can be found in the
> [docs](https://github.com/derekwisong/datui/tree/main/docs) subdirectory.

## Build Documentation

### Install mdBook

> If you used the [Setup Script](contributing.md#setup-script), mdBook is already installed.

Building the documentation requires `mdbook` to be available in your terminal.

I recommend using `cargo` to install it. It will be available into your `~/.cargo/bin/`, 
where the documentation build script will look for it. You may also add that
location to your `PATH` if you like.

```bash
cargo install mdbook
```

### Build

To build the entire documentation site:
```bash
scripts/docs/build_all_docs_local.sh
```

This will populate the `book` directory with the site's files.

At the end it will ask you if you would like a server to view the docs, or you
can simply open the `index.html` with your web browser.

```
To view locally, you can:
  1. Open book/index.html in your browser
  2. Or use a simple HTTP server:
     python3 -m http.server 8000 --directory book
     Then visit: http://localhost:8000

Start a local HTTP server to view the docs? (y/n)
```

### Generated reference: command-line options

`docs/reference/command-line-options.md` is **generated automatically** from the application's Clap definitions. Do not edit it manually.

The docs build process (`build_single_version_docs.sh`, `build_all_docs_local.sh`) copies `docs/` to a temp directory, generates CLI options into that copy, then runs mdbook from the temp tree.

To emit the options markdown on demand:

```bash
python3 scripts/docs/generate_command_line_options.py              # print to terminal
python3 scripts/docs/generate_command_line_options.py -o path.md   # write to path
```

### Check documentation links

Use [lychee](https://github.com/lycheeverse/lychee) to check built documentation for broken links:

```bash
cargo install lychee
./scripts/docs/check_doc_links.sh [--build] [--online] [PATH]
```

- **`--build`**: Build docs for `main` first (`build_single_version_docs.sh main`), then check.
- **`--online`**: Also check external URLs (default: offline, internal links only).
- **`PATH`**: Directory to check (default: `book/main`). Relative paths are resolved from the repo root.

By default the script checks internal links only (`--offline`), so it runs quickly and does not require network access. Use `--online` to verify external URLs. The script exits with a non-zero code if any broken links are found.

---

[mdbook]: https://rust-lang.github.io/mdBook