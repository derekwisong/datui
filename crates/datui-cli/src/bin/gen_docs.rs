//! Emits generated reference pages to stdout: the command-line options by default,
//! or with `settings`, the settings reference.
//!
//! Used by the docs build (Python scripts) to write `docs/reference/*.md`.

fn main() {
    match std::env::args().nth(1).as_deref() {
        Some("settings") => print!("{}", datui_cli::settings::render_settings_markdown()),
        _ => print!("{}", datui_cli::render_options_markdown()),
    }
}
