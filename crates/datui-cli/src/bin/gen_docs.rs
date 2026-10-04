//! Writes the generated parts of the docs and the manpages (`write`), stages the
//! pages and shell completions for a package or archive (`dist DIR`), or prints one
//! page to stdout: the command-line options by default, `settings`, `environment` or
//! `keys`.
//!
//! `scripts/docs/generate_command_line_options.py` runs it for the docs build, and
//! `scripts/packaging/build_package.py` and the release workflows for `dist`.

use std::path::Path;

fn main() {
    let root = datui_cli::docgen::repo_root();
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
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
        Some("dist") => {
            let Some(dir) = args.next() else {
                eprintln!("gen_docs: dist DIR");
                std::process::exit(2);
            };
            if let Err(e) = dist(Path::new(&dir)) {
                eprintln!("gen_docs: {dir}: {e}");
                std::process::exit(1);
            }
        }
        Some("settings") => print!("{}", datui_cli::settings::render_settings_markdown()),
        Some("environment") => print!("{}", datui_cli::settings::render_environment_markdown()),
        Some("keys") => print!("{}", datui_cli::keys::render_markdown()),
        _ => print!("{}", datui_cli::render_options_markdown()),
    }
}

/// `DIR/man/manN/PAGE.N` for every page, as committed, and `DIR/completions/` with
/// each shell's script under the name its shell looks for.
fn dist(dir: &Path) -> std::io::Result<()> {
    for page in datui_cli::man::PAGES {
        let section = dir.join("man").join(format!("man{}", page.section));
        std::fs::create_dir_all(&section)?;
        std::fs::write(section.join(page.file_name()), page.roff())?;
    }
    let completions = dir.join("completions");
    std::fs::create_dir_all(&completions)?;
    for (shell, file) in datui_cli::COMPLETION_FILES {
        std::fs::write(completions.join(file), datui_cli::completions(*shell))?;
    }
    Ok(())
}
