//! Binary that emits command-line options markdown to stdout.
//!
//! Used by the docs build process (Python script) to replace
//! `docs/reference/command-line-options.md` before mdbook runs.

fn main() {
    print!("{}", datui_cli::render_options_markdown());
}
