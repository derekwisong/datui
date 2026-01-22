# Documentation

Datui uses [mdBook](mdbook) to build static documentation web pages from markdown files.

> The documentation markdown files can be found in the
> [docs](https://github.com/derekwisong/datui/tree/main/docs) subdirectory.

## Build Documentation

### Install mdBook

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

---

[mdbook]: https://rust-lang.github.io/mdBook