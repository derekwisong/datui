//! The parts of the docs written from the code, and the one place that writes them.
//!
//! Each [`Generated`] is a whole page or a marked region of one. `gen_docs write`
//! writes them all; `the_generated_docs_are_current` fails while a committed copy
//! differs. The renderers are plain functions over the registries (options, settings,
//! environment, formats, keys), so a manpage can render the same sources as roff.
//!
//! A region sits between two comments, which mdBook and GitHub hide:
//!
//! ```text
//! <!-- generated: NAME -->
//! ...
//! <!-- end generated: NAME -->
//! ```
//!
//! In a Jinja template the comments are `{# generated: NAME #}`, and in TOML, Ruby
//! and desktop files `# generated: NAME`, so they stay out of what the file renders.

use std::path::{Path, PathBuf};

use crate::formats::{FileFormat, RemoteRead, Stored};
use crate::settings;

mod site;

/// What renders a generated part. Its argument reads a repository file by its path
/// from the root, line endings as LF.
type Render = fn(&dyn Fn(&str) -> String) -> String;

/// A page, or a region of one, written from the code.
pub struct Generated {
    /// From the repository's root.
    pub file: &'static str,
    /// The region's name, or `None` for the whole file.
    pub region: Option<&'static str>,
    pub render: Render,
}

/// Every generated part of the docs.
pub const GENERATED: &[Generated] = &[
    Generated {
        file: "docs/reference/command-line-options.md",
        region: None,
        render: |_| crate::render_options_markdown(),
    },
    Generated {
        file: "docs/reference/settings.md",
        region: None,
        render: |_| settings::render_settings_markdown(),
    },
    Generated {
        file: "docs/reference/environment.md",
        region: None,
        render: |_| settings::render_environment_markdown(),
    },
    Generated {
        file: "docs/reference/keyboard-shortcuts.md",
        region: Some("keys"),
        render: |_| crate::keys::render_markdown(),
    },
    Generated {
        file: "docs/formats/index.md",
        region: Some("formats"),
        render: |_| render_formats_markdown(),
    },
    Generated {
        file: "docs/formats/index.md",
        region: Some("format-count"),
        render: |_| format_count_sentence(),
    },
    Generated {
        file: "docs/introduction.md",
        region: Some("pitch"),
        render: |_| site::PITCH.to_string(),
    },
    Generated {
        file: "README.md",
        region: Some("pitch"),
        render: |_| site::PITCH.to_string(),
    },
    Generated {
        file: "docs/reference/python-api.md",
        region: Some("options"),
        render: |_| render_python_options_markdown(),
    },
    Generated {
        file: "docs/reference/catalogs.md",
        region: Some("public-catalog"),
        render: |read| render_public_catalog(&read("crates/datui-lib/src/public_catalog.toml")),
    },
    Generated {
        file: "docs/reference/manual-pages.md",
        region: Some("pages"),
        render: |_| crate::man::render_markdown_index(),
    },
    Generated {
        file: "README.md",
        region: Some("install"),
        render: site::readme_install,
    },
    Generated {
        file: "docs/getting-started/installation.md",
        region: Some("install-script"),
        render: site::docs_install_script,
    },
    Generated {
        file: "docs/getting-started/installation.md",
        region: Some("install-table"),
        render: site::docs_install_table,
    },
    Generated {
        file: "docs/getting-started/installation.md",
        region: Some("install-apt"),
        render: |read| site::channel_block(read, "apt"),
    },
    Generated {
        file: "docs/getting-started/installation.md",
        region: Some("install-dnf"),
        render: |read| site::channel_block(read, "dnf"),
    },
    Generated {
        file: "scripts/docs/index.html.j2",
        region: Some("formats"),
        render: |_| site::landing_formats(),
    },
    Generated {
        file: "scripts/docs/index.html.j2",
        region: Some("install"),
        render: site::landing_install,
    },
    Generated {
        file: "Cargo.toml",
        region: Some("description"),
        render: |_| format!("description = {}", site::toml_string(&site::summary())),
    },
    Generated {
        file: "Cargo.toml",
        region: Some("deb-description"),
        render: |_| {
            format!(
                "extended-description = {}",
                site::toml_string(&site::description())
            )
        },
    },
    Generated {
        file: "python/pyproject.toml",
        region: Some("description"),
        render: |_| format!("description = {}", site::toml_string(&site::summary())),
    },
    Generated {
        file: "scripts/packaging/homebrew-formula.rb.template",
        region: Some("desc"),
        render: |_| format!("  desc {}", site::toml_string(&site::summary())),
    },
    Generated {
        file: "scripts/packaging/datui.desktop",
        region: Some("comment"),
        render: |_| format!("Comment={}", site::TAGLINE),
    },
];

/// The opening and closing comments of a region in `file`.
fn markers(file: &str, name: &str) -> (String, String) {
    if file.ends_with(".j2") {
        (
            format!("{{# generated: {name} #}}"),
            format!("{{# end generated: {name} #}}"),
        )
    } else if file.ends_with(".md") {
        (
            format!("<!-- generated: {name} -->"),
            format!("<!-- end generated: {name} -->"),
        )
    } else {
        (
            format!("# generated: {name}"),
            format!("# end generated: {name}"),
        )
    }
}

/// `text`, the contents of `file`, with region `name` replaced by `content`. An
/// error when the region's markers are missing.
pub fn splice(file: &str, text: &str, name: &str, content: &str) -> Result<String, String> {
    let (open, close) = markers(file, name);
    let start = text.find(&open).ok_or_else(|| format!("no `{open}`"))? + open.len();
    let end = text[start..]
        .find(&close)
        .ok_or_else(|| format!("no `{close}`"))?
        + start;
    Ok(format!(
        "{}\n{}\n{}",
        &text[..start],
        content.trim_matches('\n'),
        &text[end..]
    ))
}

/// The repository's root, from this crate's manifest directory.
pub fn repo_root() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    root.canonicalize().unwrap_or(root)
}

/// A repository file's text, by its path from the root, line endings as LF.
pub fn read_file(root: &Path, path: &str) -> String {
    let full = root.join(path);
    std::fs::read_to_string(&full)
        .unwrap_or_else(|e| panic!("{}: {e}", full.display()))
        .replace("\r\n", "\n")
}

/// What each generated file should hold: the file and its text, every region filled.
/// The manpages are whole files, after the docs.
pub fn render_all(root: &Path) -> Result<Vec<(PathBuf, String)>, String> {
    let read = |path: &str| read_file(root, path);
    let mut out: Vec<(PathBuf, String)> = Vec::new();
    for part in GENERATED {
        let path = root.join(part.file);
        let content = (part.render)(&read);
        let current = match out.iter().position(|(p, _)| *p == path) {
            Some(i) => out.remove(i).1,
            None if part.region.is_none() => String::new(),
            None => std::fs::read_to_string(&path)
                .map_err(|e| format!("{}: {e}", part.file))?
                // A Windows checkout may turn line endings into CRLF.
                .replace("\r\n", "\n"),
        };
        let text = match part.region {
            None => content,
            Some(name) => splice(part.file, &current, name, &content)
                .map_err(|e| format!("{}: {e}", part.file))?,
        };
        out.push((path, text));
    }
    for page in crate::man::PAGES {
        out.push((root.join(page.path()), page.render(&read)));
    }
    Ok(out)
}

/// Write every generated part. Returns the files that changed.
pub fn write_all(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut changed = Vec::new();
    for (path, text) in render_all(root)? {
        let old = std::fs::read_to_string(&path).unwrap_or_default();
        if old.replace("\r\n", "\n") != text {
            std::fs::write(&path, &text).map_err(|e| format!("{}: {e}", path.display()))?;
            changed.push(path);
        }
    }
    Ok(changed)
}

/// The bundled catalog for the docs: its header comment as a quote, the rest as a
/// block. A block's comment lines read as headings in some outlines, so the header's
/// rules are prose here.
pub fn render_public_catalog(text: &str) -> String {
    let lines: Vec<&str> = text.lines().collect();
    let header = lines.iter().take_while(|l| l.starts_with('#')).count();
    let mut out = String::new();
    for line in &lines[..header] {
        let said = line.trim_start_matches('#').trim();
        if said.is_empty() {
            out.push_str(">\n");
        } else {
            out.push_str(&format!("> {said}\n"));
        }
    }
    let body = lines[header..].join("\n");
    out.push_str(&format!(
        "\n```toml,output\n{}\n```",
        body.trim_matches('\n')
    ));
    out
}

/// The family page and heading that describe a format, from `docs/formats/`.
pub fn format_page(format: FileFormat) -> &'static str {
    use FileFormat::*;
    match format {
        Parquet => "columnar-and-json.md#parquet",
        Csv | Tsv | Psv => "delimited-text.md#csv-tsv-and-psv",
        Text => "delimited-text.md#text-and-logs",
        Json | Jsonl => "columnar-and-json.md#json-and-ndjson",
        Arrow => "columnar-and-json.md#arrow-ipc",
        Avro | Orc => "columnar-and-json.md#avro-and-orc",
        Excel => "columnar-and-json.md#excel",
        Sqlite => "databases-and-arrays.md#sqlite",
        Numpy => "databases-and-arrays.md#numpy",
        Safetensors | Gguf => "model-files.md",
        Audio => "signals-and-logs.md#audio",
        Midi => "signals-and-logs.md#midi",
        Vcd => "signals-and-logs.md#vcd",
        Nmea | Gpx => "signals-and-logs.md#gps-logs",
        Ulog | Dataflash => "signals-and-logs.md#flight-logs",
        Candump => "signals-and-logs.md#can-logs",
        Fix => "signals-and-logs.md#fix-logs",
        Sdf => "signals-and-logs.md#sdf",
        Elf => "signals-and-logs.md#elf",
        Journal => "signals-and-logs.md#systemd-journal",
    }
}

/// A format's name in the docs: its title, or the containers for audio.
pub fn doc_title(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Audio => "WAV, BWF, RF64, AIFF",
        FileFormat::Text => "Text",
        _ => format.title(),
    }
}

/// A format's name in a list of them: its title, or a plain name for audio and text.
pub fn short_title(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Audio => "WAV/AIFF audio",
        FileFormat::Text => "plain text",
        _ => format.title(),
    }
}

/// "datui reads 27 formats: Parquet, CSV, ... and binary formats you describe in a
/// format spec." Counted from the descriptors, so the number cannot go stale.
pub fn format_count_sentence() -> String {
    let titles: Vec<&str> = FileFormat::ALL.into_iter().map(short_title).collect();
    format!(
        "datui reads {} formats: {}, and binary formats you describe in a format spec.",
        titles.len(),
        titles.join(", ")
    )
}

/// The formats overview's table: how a file of each format is read, from its
/// descriptor. One row per format, then an Arrow IPC stream and a format spec.
pub fn render_formats_markdown() -> String {
    let mut out = String::from(
        "| Format | `--format` | Extensions | Read | Compressed | HTTP(S) | In a bucket | Bucket prefix |\n\
         |---|---|---|---|---|---|---|---|\n",
    );
    let said = |mode: Option<crate::formats::ReadMode>| mode.map_or("no", |m| m.label());
    for format in FileFormat::ALL {
        let d = format.descriptor();
        let mut names: Vec<String> = d.extensions.iter().map(|e| format!("`.{e}`")).collect();
        names.extend(d.name_endings.iter().map(|n| format!("`{n}`")));
        let extensions = if names.is_empty() {
            "none: by content".to_string()
        } else {
            names.join(", ")
        };
        out.push_str(&format!(
            "| [{}]({}) | `{}` | {} | {} | {} | {} | {} | {} |\n",
            doc_title(format),
            format_page(format),
            format.name(),
            extensions,
            said(format.read_mode(Stored::Plain)),
            said(format.read_mode(Stored::Compressed { in_memory: false })),
            format.http_file().label(),
            format.bucket_object(Stored::Plain).label(),
            format
                .bucket_prefix(Stored::Plain)
                .map_or("no", RemoteRead::label),
        ));
    }
    let arrow = FileFormat::Arrow;
    out.push_str(&format!(
        "| [Arrow IPC stream](columnar-and-json.md#arrow-ipc) | `arrow` | as Arrow IPC | {} | {} | {} | {} | {} |\n",
        said(arrow.read_mode(Stored::Stream)),
        said(arrow.read_mode(Stored::Compressed { in_memory: false })),
        arrow.http_file().label(),
        arrow.bucket_object(Stored::Stream).label(),
        arrow.bucket_prefix(Stored::Stream).map_or("no", RemoteRead::label),
    ));
    let spec = crate::FormatChoice::Spec("a.spec".into());
    out.push_str(&format!(
        "| [Format spec](format-specs.md) | its name | its `match` | {} | {} | {} | {} | {} |\n",
        said(spec.read_mode(Stored::Plain)),
        said(spec.read_mode(Stored::Compressed { in_memory: false })),
        spec.http_file().label(),
        spec.bucket_object(Stored::Plain).label(),
        spec.bucket_prefix(Stored::Plain)
            .map_or("no", RemoteRead::label),
    ));
    out
}

/// The keywords `datui.view()` and `DatuiOptions` take, from the option registry: the
/// open's own options, then the config keys', then `config`.
pub fn render_python_options_markdown() -> String {
    use clap::CommandFactory;
    let cmd = crate::Args::command();
    let flag_help = |flag: &str| -> String {
        cmd.get_arguments()
            .find(|a| a.get_long() == Some(flag))
            .and_then(|a| a.get_help())
            .map(|h| h.to_string())
            .unwrap_or_default()
    };
    let cell = |s: &str| s.replace('|', "\\|").replace('\n', " ");
    let mut out =
        String::from("| Keyword | Takes | Command line | What it does |\n|---|---|---|---|\n");
    for open in settings::OPEN {
        out.push_str(&format!(
            "| `{}` | {} | `--{}` | {} |\n",
            open.kwarg,
            open.kind.describe(),
            open.flag,
            cell(&flag_help(open.flag)),
        ));
    }
    for setting in settings::SETTINGS.iter().filter(|s| s.kwarg.is_some()) {
        let line = match setting.flag {
            Some(flag) => format!("`--{flag}`"),
            None => format!("`-c {}=...`", setting.key),
        };
        out.push_str(&format!(
            "| `{}` | {} | {} | {} |\n",
            setting.kwarg.unwrap_or_default(),
            setting.kind.describe(),
            line,
            cell(setting.doc),
        ));
    }
    out.push_str(
        "| `config` | dict | `-c KEY=VALUE` | Any config key to its value, as `-c` sets it: `config={\"display.row_numbers\": True}` |\n",
    );
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The committed docs are what the code renders. Run `gen_docs write` (or
    /// `.venv/bin/python scripts/docs/generate_command_line_options.py --all`) after
    /// changing a flag, a setting, a format, a key or an environment variable.
    #[test]
    fn the_generated_docs_are_current() {
        let root = repo_root();
        let stale: Vec<String> = render_all(&root)
            .expect("every generated region has its markers")
            .into_iter()
            .filter(|(path, text)| {
                std::fs::read_to_string(path)
                    .map(|t| t.replace("\r\n", "\n"))
                    .ok()
                    .as_deref()
                    != Some(text.as_str())
            })
            .map(|(path, _)| path.display().to_string())
            .collect();
        assert!(
            stale.is_empty(),
            "stale: {}. Run `cargo run -p datui-cli --bin gen_docs -- write`",
            stale.join(", ")
        );
    }

    #[test]
    fn a_region_is_replaced_between_its_markers() {
        let text = "a\n<!-- generated: x -->\nold\n<!-- end generated: x -->\nb\n";
        assert_eq!(
            splice("a.md", text, "x", "new").unwrap(),
            "a\n<!-- generated: x -->\nnew\n<!-- end generated: x -->\nb\n"
        );
        assert!(splice("a.md", text, "y", "new").is_err());
        let toml = "a\n# generated: x\nold\n# end generated: x\nb\n";
        assert_eq!(
            splice("a.toml", toml, "x", "new").unwrap(),
            "a\n# generated: x\nnew\n# end generated: x\nb\n"
        );
    }

    #[test]
    fn the_format_count_counts_every_format() {
        let sentence = format_count_sentence();
        assert!(sentence.starts_with(&format!("datui reads {} formats", FileFormat::ALL.len())));
        assert_eq!(sentence.matches(", ").count(), FileFormat::ALL.len());
    }
}
