use clap::CommandFactory;
use clap_mangen::Man;
use std::env;
use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

/// Text for one roff line: backslashes and hyphens escaped, and a leading `.` or `'`
/// kept from being read as a request.
fn roff_text(text: &str) -> String {
    let escaped = text.replace('\\', "\\e").replace('-', "\\-");
    if escaped.starts_with('.') || escaped.starts_with('\'') {
        format!("\\&{escaped}")
    } else {
        escaped
    }
}

/// An EXAMPLES section in place of clap_mangen's EXTRA, which prints `examples.txt`
/// as one block of text under a heading no manpage uses.
fn render_examples(w: &mut dyn Write) -> io::Result<()> {
    writeln!(w, ".SH EXAMPLES")?;
    for example in datui_cli::examples() {
        // Unfilled, so a command is never broken or hyphenated: a URL split across
        // lines no longer pastes. A `.TP` tag wider than the page, as the penguins
        // URL is at 80 columns, also made troff warn on every `man datui`.
        writeln!(w, ".PP")?;
        writeln!(w, ".nf")?;
        writeln!(w, "\\fB{}\\fR", roff_text(&example.command))?;
        writeln!(w, ".fi")?;
        writeln!(w, ".RS")?;
        writeln!(w, "{}", roff_text(&example.description))?;
        writeln!(w, ".RE")?;
    }
    writeln!(w, ".PP")?;
    writeln!(
        w,
        "{}",
        roff_text("Documentation: https://derekwisong.github.io/datui/")
    )
}

fn main() -> io::Result<()> {
    let cmd = datui_cli::Args::command();
    let man = Man::new(cmd);
    let mut buffer: Vec<u8> = Default::default();
    man.render_title(&mut buffer)?;
    man.render_name_section(&mut buffer)?;
    man.render_synopsis_section(&mut buffer)?;
    man.render_description_section(&mut buffer)?;
    man.render_options_section(&mut buffer)?;
    render_examples(&mut buffer)?;
    man.render_version_section(&mut buffer)?;

    let out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());

    let dest_path = out_dir.join("datui.1");
    fs::write(&dest_path, &buffer)?;

    if env::var("PROFILE").unwrap_or_default() == "release"
        && let Some(release_dir) = out_dir.ancestors().nth(3)
    {
        let release_manpage = release_dir.join("datui.1");
        fs::write(&release_manpage, &buffer)?;
    }

    Ok(())
}
