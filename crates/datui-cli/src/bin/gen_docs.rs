//! Writes the generated parts of the docs (`write`), or prints one page to stdout:
//! the command-line options by default, `settings`, `environment` or `keys`.
//!
//! `scripts/docs/generate_command_line_options.py` runs it for the docs build.

fn main() {
    let root = datui_cli::docgen::repo_root();
    match std::env::args().nth(1).as_deref() {
        Some("write") => match datui_cli::docgen::write_all(&root) {
            Ok(changed) => {
                for path in changed {
                    println!("wrote {}", path.display());
                }
            }
            Err(e) => {
                eprintln!("gen_docs: {e}");
                std::process::exit(1);
            }
        },
        Some("settings") => print!("{}", datui_cli::settings::render_settings_markdown()),
        Some("environment") => print!("{}", datui_cli::settings::render_environment_markdown()),
        Some("keys") => print!(
            "{}",
            datui_cli::keys::render_markdown(&|name| datui_cli::docgen::read_help(&root, name))
        ),
        _ => print!("{}", datui_cli::render_options_markdown()),
    }
}
