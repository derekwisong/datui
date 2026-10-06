//! The landing page, the README's install section and the package descriptions:
//! the parts written from `scripts/docs/install.toml` and the format descriptors.

use super::{format_page, short_title};
use crate::formats::FileFormat;

/// What datui is, in the words every page and package leads with.
pub const TAGLINE: &str = "Explore tabular data in your terminal";

/// What datui reads, as the README and the docs introduction say it. The full
/// list and its count are on the Formats pages.
pub const PITCH: &str = "Parquet, CSV, JSON, Arrow, Excel, SQLite, logs, audio, model files and more, \
     including binary formats you describe in a format spec.";

/// The one-line description: crates.io, PyPI, the AUR, deb and rpm, Homebrew.
pub fn summary() -> String {
    format!("{TAGLINE}: Parquet, CSV, JSON and more")
}

/// The longer description: the deb's extended description, WinGet's `Description`.
pub fn description() -> String {
    format!(
        "{PITCH} On disk or in S3, GCS, Azure and HTTP(S). \
         Query with SQL or q, sort and filter, chart, analyze and export."
    )
}

/// `text` as a quoted TOML (and Ruby) string.
pub fn toml_string(text: &str) -> String {
    format!("\"{}\"", text.replace('\\', "\\\\").replace('"', "\\\""))
}

fn html(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

/// One way to install datui, from `install.toml`.
struct Channel {
    id: String,
    label: String,
    link: Option<String>,
    os: Vec<String>,
    lang: String,
    command: Option<String>,
    anchor: Option<String>,
    text: Option<String>,
    html: Option<String>,
    table: bool,
}

fn channels(read: &dyn Fn(&str) -> String) -> Vec<Channel> {
    let doc: toml::Table = read("scripts/docs/install.toml")
        .parse()
        .unwrap_or_else(|e| panic!("scripts/docs/install.toml: {e}"));
    let list = doc
        .get("channel")
        .and_then(|c| c.as_array())
        .expect("install.toml: [[channel]]");
    list.iter()
        .map(|c| {
            let s = |key: &str| c.get(key).and_then(|v| v.as_str()).map(str::to_string);
            let channel = Channel {
                id: s("id").expect("install.toml: a channel without `id`"),
                label: s("label").expect("install.toml: a channel without `label`"),
                link: s("link"),
                os: c
                    .get("os")
                    .and_then(|v| v.as_array())
                    .map(|a| {
                        a.iter()
                            .filter_map(|o| o.as_str().map(str::to_string))
                            .collect()
                    })
                    .unwrap_or_default(),
                lang: s("lang").unwrap_or_else(|| "bash".into()),
                command: s("command"),
                anchor: s("anchor"),
                text: s("text"),
                html: s("html"),
                table: c.get("table").and_then(|v| v.as_bool()).unwrap_or(true),
            };
            assert!(
                channel.command.is_some() || (channel.text.is_some() && channel.html.is_some()),
                "install.toml: `{}` needs a command, or both text and html",
                channel.id
            );
            channel
        })
        .collect()
}

/// The fenced block of the channel that heads the tables (`table = false`).
fn headline(read: &dyn Fn(&str) -> String) -> String {
    channels(read)
        .into_iter()
        .filter(|c| !c.table)
        .filter_map(|c| {
            c.command
                .map(|cmd| format!("```{},install\n{}\n```\n", c.lang, cmd))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// The table of the other channels. `install_page` is where installation.md is,
/// as the table's file links it.
fn table(read: &dyn Fn(&str) -> String, install_page: &str) -> String {
    let mut out = String::from("| Platform | Command |\n|---|---|\n");
    for c in channels(read).into_iter().filter(|c| c.table) {
        let label = match &c.link {
            Some(link) => format!("[{}]({link})", c.label),
            None => c.label.clone(),
        };
        let cell = match (&c.command, &c.anchor) {
            (Some(cmd), _) if !cmd.contains('\n') => format!("`{cmd}`"),
            (Some(_), Some(anchor)) => {
                let name = anchor.replace('-', " ");
                let mut letters = name.chars();
                let first = letters.next().map(|l| l.to_ascii_uppercase());
                format!(
                    "[{}{}]({install_page}#{anchor})",
                    first.unwrap_or_default(),
                    letters.as_str()
                )
            }
            (Some(_), None) => panic!("install.toml: `{}` has lines but no anchor", c.id),
            (None, _) => c.text.clone().unwrap_or_default(),
        };
        out.push_str(&format!("| {label} | {} |\n", cell.replace('|', "\\|")));
    }
    out
}

/// README.md's install section: the one-line script, then the table.
pub fn readme_install(read: &dyn Fn(&str) -> String) -> String {
    format!(
        "{}\n{}",
        headline(read),
        table(
            read,
            "https://derekwisong.github.io/datui/latest/getting-started/installation.html"
        )
    )
}

/// One channel's command, as a fenced block: installation.md's apt section.
pub fn channel_block(read: &dyn Fn(&str) -> String, id: &str) -> String {
    let c = channels(read)
        .into_iter()
        .find(|c| c.id == id)
        .unwrap_or_else(|| panic!("install.toml: no channel `{id}`"));
    let cmd = c
        .command
        .unwrap_or_else(|| panic!("install.toml: `{id}` has no command"));
    format!("```{},install\n{}\n```\n", c.lang, cmd)
}

/// installation.md's one-line script.
pub fn docs_install_script(read: &dyn Fn(&str) -> String) -> String {
    headline(read)
}

/// installation.md's table of package managers.
pub fn docs_install_table(read: &dyn Fn(&str) -> String) -> String {
    table(read, "")
}

/// The landing page's install panels: one per channel, all shown without
/// JavaScript; the page's script makes them tabs and picks one for the visitor's
/// system.
pub fn landing_install(read: &dyn Fn(&str) -> String) -> String {
    let mut out = String::from("<div class=\"install-panels\" id=\"install-panels\">\n");
    for c in channels(read) {
        out.push_str(&format!(
            "  <section class=\"install-panel\" id=\"install-{}\" data-label=\"{}\" data-os=\"{}\">\n    <h3>{}</h3>\n",
            html(&c.id),
            html(&c.label),
            html(&c.os.join(" ")),
            html(&c.label),
        ));
        match &c.command {
            Some(cmd) => out.push_str(&format!(
                "    <pre data-example=\"{},install\"><code>{}</code></pre>\n",
                html(&c.lang),
                html(cmd)
            )),
            None => out.push_str(&format!(
                "    <p>{}</p>\n",
                c.html.as_deref().unwrap_or_default()
            )),
        }
        out.push_str("  </section>\n");
    }
    out.push_str("</div>");
    out
}

/// A family page's title, as its H1 says it.
fn family_title(page: &str) -> &'static str {
    match page {
        "columnar-and-json.md" => "Columnar and JSON",
        "delimited-text.md" => "Delimited text",
        "databases-and-arrays.md" => "Databases and arrays",
        "model-files.md" => "Model files",
        "signals-and-logs.md" => "Signals and logs",
        _ => panic!("no family title for {page}"),
    }
}

/// The landing page's formats strip: each family and its formats, from the
/// descriptors, then format specs.
pub fn landing_formats() -> String {
    let mut families: Vec<(&str, Vec<&str>)> = Vec::new();
    for format in FileFormat::ALL {
        let page = format_page(format).split('#').next().unwrap_or_default();
        match families.iter_mut().find(|(p, _)| *p == page) {
            Some((_, names)) => names.push(short_title(format)),
            None => families.push((page, vec![short_title(format)])),
        }
    }
    let mut out = String::from("<dl class=\"format-families\">\n");
    for (page, names) in families {
        out.push_str(&format!(
            "  <div><dt><a href=\"{{{{ guide('formats/{}') }}}}\">{}</a></dt><dd>{}</dd></div>\n",
            page.trim_end_matches(".md"),
            family_title(page),
            html(&names.join(" · ")),
        ));
    }
    out.push_str(
        "  <div><dt><a href=\"{{ guide('formats/format-specs') }}\">Format specs</a></dt><dd>Your own binary formats, described in TOML</dd></div>\n</dl>",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_channel_reads_and_one_heads_the_tables() {
        let root = super::super::repo_root();
        let all = channels(&|p| super::super::read_file(&root, p));
        assert_eq!(all.iter().filter(|c| !c.table).count(), 1);
        for os in ["linux", "macos", "windows"] {
            assert!(
                all.iter().any(|c| c.os.iter().any(|o| o == os)),
                "no channel shown first on {os}"
            );
        }
    }

    #[test]
    fn every_format_is_in_the_strip() {
        let strip = landing_formats();
        for format in FileFormat::ALL {
            assert!(strip.contains(short_title(format)), "{format:?}");
        }
    }

    #[test]
    fn the_summary_fits_every_package() {
        // Homebrew's audit and the AUR's pkgdesc want one short line.
        assert!(summary().len() <= 80, "{}", summary());
        assert!(!summary().ends_with('.'));
    }
}
