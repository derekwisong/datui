//! The manpages, rendered from the sources the docs render from: the clap
//! definitions, the option, environment and key registries, the format descriptors,
//! `examples.toml` and the query and format-spec references.
//!
//! The pages are rendered by `gen_docs write` and committed under
//! `crates/datui-cli/man/`; `the_generated_docs_are_current` fails while a committed
//! page differs. Committed, they need nothing at build time: the
//! references live outside this crate, which a build from crates.io could not read,
//! and every package, tarball and `datui man` takes the same files. The date is the
//! release's (`release-date.txt`, set by `scripts/bump_version.py`), never the build's,
//! so a page is the same however and whenever it is built.

mod plain;
pub mod roff;

use clap::CommandFactory;
use roff::{Headings, Markdown, arg, bold, example_block, inline, italic, line, literal, text};

use crate::settings::{self, DefaultValue, EnvGroup};
use crate::{exit, keys};

/// What a page renders from, beyond this crate: a repository file's text, by its path
/// from the root.
pub type Read<'a> = &'a dyn Fn(&str) -> String;

/// One manpage.
pub struct Page {
    /// `datui-config`.
    pub name: &'static str,
    pub section: u8,
    /// The committed page, as `datui man` prints it.
    pub text: &'static str,
    render: fn(&Page, Read) -> String,
}

macro_rules! page {
    ($name:literal, $section:literal, $render:expr) => {
        Page {
            name: $name,
            section: $section,
            text: include_str!(concat!("../../man/", $name, ".", $section)),
            render: $render,
        }
    };
}

/// Every page, in the order `datui man --list` and the docs list them.
pub const PAGES: &[Page] = &[
    page!("datui", 1, |p, read| render_datui(p, read)),
    page!("datui-config", 1, |p, read| render_command(
        p, read, "config"
    )),
    page!("datui-catalog", 1, |p, read| render_command(
        p, read, "catalog"
    )),
    page!("datui-cache", 1, |p, read| render_command(p, read, "cache")),
    page!("datui-views", 1, |p, read| render_command(p, read, "views")),
    page!("datui-formats", 1, |p, read| render_command(
        p, read, "formats"
    )),
    page!("datui-completions", 1, |p, read| render_command(
        p,
        read,
        "completions"
    )),
    page!("datui-man", 1, |p, read| render_command(p, read, "man")),
    page!("datui-config", 5, |p, read| render_config_file(p, read)),
    page!("datui-keys", 7, |p, read| render_keys(p, read)),
    page!("datui-query", 7, |p, read| render_query(p, read)),
    page!("datui-formats", 7, |p, read| render_formats(p, read)),
];

/// The date every page carries: the release's, from `release-date.txt`.
pub const RELEASE_DATE: &str = include_str!("../../release-date.txt");

const BUGS: &str = "https://github.com/derekwisong/datui/issues";
const DOCS: &str = "https://derekwisong.github.io/datui/";

impl Page {
    /// `datui-config.5`: the file name, and how examples name the page.
    pub fn file_name(&self) -> String {
        format!("{}.{}", self.name, self.section)
    }

    /// `datui-config(5)`.
    pub fn title(&self) -> String {
        format!("{}({})", self.name, self.section)
    }

    /// Where the page is committed, from the repository's root.
    pub fn path(&self) -> String {
        format!("crates/datui-cli/man/{}", self.file_name())
    }

    /// The page as `gen_docs` renders it.
    pub fn render(&self, read: Read) -> String {
        roff::tidy(&(self.render)(self, read))
    }

    /// The committed page's text with a Windows checkout's line endings undone.
    pub fn roff(&self) -> String {
        self.text.replace("\r\n", "\n")
    }

    /// The page as plain text filled to `width` columns, for a system without `man`.
    pub fn plain(&self, width: usize) -> String {
        plain::render(&self.roff(), width)
    }

    /// The NAME line's description, as `whatis` and `datui man --list` show it.
    pub fn summary(&self) -> String {
        match (self.name, self.section) {
            ("datui", 1) => lower_first(
                &crate::Args::command()
                    .get_about()
                    .map(|a| a.to_string())
                    .unwrap_or_default(),
            ),
            ("datui-config", 5) => "the datui configuration file".into(),
            ("datui-keys", 7) => "the keys of every datui screen".into(),
            ("datui-query", 7) => "the q syntax of the datui query prompt".into(),
            ("datui-formats", 7) => "the formats datui reads, and format specs".into(),
            (name, _) => {
                let command = name.trim_start_matches("datui-");
                let about = subcommand(command)
                    .get_about()
                    .map(|a| a.to_string())
                    .unwrap_or_default();
                // The first clause: the NAME line is one line.
                let first = about.split([':', ';']).next().unwrap_or(&about);
                lower_first(first.trim())
            }
        }
    }
}

/// The page `query` names: `datui-config`, `config`, `config.5`, `keys`. A name that
/// two sections share (`config`) is the command's, as `man` picks section 1 first.
pub fn find(query: &str) -> Option<&'static Page> {
    let query = query.trim();
    let (name, section) = match query.rsplit_once('.') {
        Some((name, s)) if s.parse::<u8>().is_ok() => (name, s.parse::<u8>().ok()),
        _ => (query, None),
    };
    let name = name.strip_prefix("datui-").unwrap_or(name);
    PAGES.iter().find(|p| {
        let short = p.name.strip_prefix("datui-").unwrap_or(p.name);
        (short == name || p.name == name) && section.is_none_or(|s| s == p.section)
    })
}

fn lower_first(s: &str) -> String {
    let mut chars = s.chars();
    match chars.next() {
        // An initialism (`SQL`) keeps its case.
        Some(c) if chars.clone().next().is_some_and(|n| n.is_lowercase()) => {
            c.to_lowercase().chain(chars).collect()
        }
        Some(c) => std::iter::once(c).chain(chars).collect(),
        None => String::new(),
    }
}

fn subcommand(name: &str) -> clap::Command {
    let mut cmd = crate::Args::command();
    cmd.build();
    cmd.find_subcommand(name)
        .unwrap_or_else(|| panic!("no subcommand {name}"))
        .clone()
}

/// The manual a section belongs to, as `.TH` names it.
fn manual(section: u8) -> &'static str {
    match section {
        1 => "User Commands",
        5 => "File Formats",
        _ => "Miscellaneous",
    }
}

/// `.TH` and NAME.
fn head(page: &Page) -> String {
    let mut out = String::from(".\\\" Generated by gen_docs from crates/datui-cli. Do not edit.\n");
    out.push_str(&format!(
        ".TH {} {} {} {} {}\n",
        literal(&page.name.to_uppercase()),
        page.section,
        RELEASE_DATE.trim(),
        arg(&format!("datui {}", env!("CARGO_PKG_VERSION"))),
        arg(manual(page.section)),
    ));
    // No hyphenation and a ragged right, through the registers groff's man macros
    // reset each paragraph from: an identifier (`DATUI_GCP_PROJECT`, a dotted key)
    // broken at a hyphen reads as another word.
    out.push_str(".nr HY 0\n.ds AD l\n");
    out.push_str(&format!(
        ".SH NAME\n{} \\- {}\n",
        literal(page.name),
        text(&page.summary())
    ));
    out
}

/// A paragraph of Markdown.
fn para(out: &mut String, md: &str) {
    out.push_str(".PP\n");
    out.push_str(&line(inline(md)));
    out.push('\n');
}

/// A tagged paragraph: the tag is roff already, the body Markdown.
fn item(out: &mut String, tag: &str, md: &str) {
    out.push_str(".TP\n");
    out.push_str(&line(tag.to_string()));
    out.push('\n');
    out.push_str(&line(inline(md)));
    out.push('\n');
}

/// An option's tag: `-F, --format FMT`, `--infer-types[=COLS|off]`, `PATH...`.
fn option_tag(a: &clap::Arg) -> String {
    let values: Vec<String> = a
        .get_value_names()
        .map(|names| names.iter().map(|n| n.to_string()).collect())
        .unwrap_or_default();
    if a.is_positional() {
        let names = values
            .iter()
            .map(|n| italic(n))
            .collect::<Vec<_>>()
            .join(" ");
        return if a.get_num_args().is_some_and(|n| n.max_values() > 1) {
            format!("{names} ...")
        } else {
            names
        };
    }
    let mut names = Vec::new();
    if let Some(s) = a.get_short() {
        names.push(bold(&format!("-{s}")));
    }
    if let Some(l) = a.get_long() {
        names.push(bold(&format!("--{l}")));
    }
    let mut tag = names.join(", ");
    if a.get_action().takes_values() && !values.is_empty() {
        let value = values
            .iter()
            .map(|n| italic(n))
            .collect::<Vec<_>>()
            .join(" ");
        if a.get_num_args().is_some_and(|n| n.min_values() == 0) {
            tag.push_str(&format!("[={value}]"));
        } else {
            tag.push(' ');
            tag.push_str(&value);
        }
    }
    tag
}

fn help_of(a: &clap::Arg) -> String {
    let mut help = a
        .get_long_help()
        .or(a.get_help())
        .map(|h| h.to_string())
        .unwrap_or_default();
    let shown: Vec<String> = a
        .get_possible_values()
        .iter()
        .filter(|v| !v.is_hide_set())
        .map(|v| format!("`{}`", v.get_name()))
        .collect();
    if !shown.is_empty() && !a.is_hide_possible_values_set() && a.get_action().takes_values() {
        help.push_str(&format!(". One of {}", shown.join(", ")));
    }
    if !help.is_empty() && !help.ends_with('.') {
        help.push('.');
    }
    help
}

fn options(out: &mut String, args: &[&clap::Arg]) {
    for a in args {
        item(out, &option_tag(a), &help_of(a));
    }
}

fn shown_args(cmd: &clap::Command) -> Vec<&clap::Arg> {
    cmd.get_arguments()
        .filter(|a| !a.is_hide_set())
        .filter(|a| !matches!(a.get_id().as_str(), "help" | "version"))
        .collect()
}

fn exit_status(out: &mut String, session: bool) {
    out.push_str(".SH \"EXIT STATUS\"\n");
    for s in exit::STATUSES.iter().filter(|s| session || !s.session_only) {
        item(out, &bold(&s.code.to_string()), s.means);
    }
}

fn environment(out: &mut String, names: &[&str]) {
    out.push_str(".SH ENVIRONMENT\n");
    for var in settings::ENVIRONMENT
        .iter()
        .filter(|v| names.is_empty() || v.names.iter().any(|n| names.contains(n)))
    {
        environment_entry(out, var);
    }
}

fn environment_entry(out: &mut String, var: &settings::EnvVar) {
    let tag = var
        .names
        .iter()
        .map(|n| bold(n))
        .collect::<Vec<_>>()
        .join(", ");
    item(out, &tag, &format!("{}.", var.doc));
}

/// Where the config and cache directories are, then the files in them.
fn files(out: &mut String, which: Files) {
    out.push_str(".SH FILES\n");
    para(
        out,
        "*CONFIG* is `$DATUI_CONFIG_DIR` when it is set, else `$XDG_CONFIG_HOME/datui` (`~/.config/datui`) on Linux, `~/Library/Application Support/datui` on macOS and `%APPDATA%\\datui` on Windows.",
    );
    if which.cache {
        para(
            out,
            "*CACHE* is `$DATUI_CACHE_DIR` when it is set, else `$XDG_CACHE_HOME/datui` (`~/.cache/datui`) on Linux, `~/Library/Caches/datui` on macOS and `%LOCALAPPDATA%\\datui` on Windows.",
        );
    }
    let config: &[(&str, &str)] = &[
        (
            "CONFIG/config.toml",
            "The config file: see datui-config(5). `datui config path` prints the files read",
        ),
        (
            "CONFIG/catalog.toml",
            "Your catalog, which Ctrl+D on the home screen adds to: see datui-catalog(1)",
        ),
        (
            "CONFIG/catalogs/",
            "More catalogs, one *.toml each, named by its file; public.toml replaces the bundled one",
        ),
        ("CONFIG/views/", "Saved views: see datui-views(1)"),
        (
            "CONFIG/formats/",
            "Format specs and dictionaries, searched first: see datui-formats(7)",
        ),
    ];
    let cache: &[(&str, &str)] = &[
        (
            "CACHE/recents_history.txt",
            "The recent datasets the home screen lists. `datui cache clear --recents` forgets them",
        ),
        ("CACHE/*_history.txt", "The prompts' history"),
        (
            "CACHE/shapes/, CACHE/facts/, CACHE/cloud_listings/",
            "What the home screen has measured of datasets and listed of cloud sources, so it can show rows, columns and sizes without reading them again",
        ),
        (
            "CACHE/datui.log",
            "The log, unless `log.file` names another",
        ),
    ];
    let mut entries: Vec<&(&str, &str)> = Vec::new();
    if which.config {
        entries.extend(config);
    }
    if which.cache {
        entries.extend(cache);
    }
    for (path, what) in entries {
        let tag = path
            .split(", ")
            .map(|p| {
                let (dir, rest) = p.split_once('/').unwrap_or((p, ""));
                format!("\\fI{}\\fR/{}", literal(dir), literal(rest))
            })
            .collect::<Vec<_>>()
            .join(", ");
        item(out, &tag, &format!("{what}."));
    }
}

#[derive(Clone, Copy)]
struct Files {
    config: bool,
    cache: bool,
}

fn examples(out: &mut String, page: &Page) {
    let examples = crate::examples_of(&page.file_name());
    if examples.is_empty() {
        return;
    }
    out.push_str(".SH EXAMPLES\n");
    for example in examples {
        para(out, &format!("{}.", example.description));
        // Each file it reads under its name, then the command.
        for file in &example.files {
            out.push_str(&format!(".PP\n{}\n", line(bold(&file.name))));
            example_block(out, &file.text);
        }
        example_block(out, &example.command);
    }
}

fn see_also(out: &mut String, page: &Page, others: &[&str]) {
    out.push_str(".SH \"SEE ALSO\"\n");
    let mut refs: Vec<String> = PAGES
        .iter()
        .filter(|p| p.file_name() != page.file_name())
        .filter(|p| {
            page.name == "datui" || p.name == "datui" || others.contains(&p.file_name().as_str())
        })
        .map(|p| format!("{}({})", bold(p.name), p.section))
        .collect();
    let external: &[(&str, u8)] = match page.name {
        "datui" => &[("jq", 1), ("less", 1), ("journalctl", 1), ("vd", 1)],
        "datui-man" => &[("man", 1)],
        _ => &[],
    };
    refs.extend(external.iter().map(|(n, s)| format!("{}({s})", bold(n))));
    out.push_str(&line(refs.join(", ")));
    out.push('\n');
    para(out, &format!("The datui documentation: <{DOCS}>"));
}

/// BUGS, AUTHORS and COPYRIGHT, the year from the license.
fn tail(out: &mut String, read: Read) {
    out.push_str(".SH BUGS\n");
    para(out, &format!("Report bugs at <{BUGS}>."));
    out.push_str(".SH AUTHORS\n");
    para(out, "Derek Wisong and the datui contributors.");
    let license = read("LICENSE");
    let copyright = license
        .lines()
        .find(|l| l.starts_with("Copyright"))
        .unwrap_or("Copyright (c) Derek Wisong");
    out.push_str(".SH COPYRIGHT\n.PP\n");
    out.push_str(&line(text(copyright).replace("(c)", "\\(co")));
    out.push('\n');
    para(out, "datui is free software under the MIT License.");
}

fn render_datui(page: &Page, read: Read) -> String {
    let mut cmd = crate::Args::command();
    cmd.build();
    let mut out = head(page);

    out.push_str(".SH SYNOPSIS\n.nf\n");
    out.push_str(&format!(
        "{} [{}]... [{}]...\n",
        bold("datui"),
        italic("OPTION"),
        italic("PATH")
    ));
    out.push_str(&format!(
        "{} | {} [{}]... [{}]\n",
        italic("command"),
        bold("datui"),
        italic("OPTION"),
        bold("-")
    ));
    out.push_str(&format!(
        "{} {} [{}]...\n",
        bold("datui"),
        italic("COMMAND"),
        italic("ARG")
    ));
    out.push_str(".fi\n");

    out.push_str(".SH DESCRIPTION\n");
    for paragraph in read("crates/datui-cli/long_about.txt").split("\n\n") {
        para(
            &mut out,
            &paragraph.split_whitespace().collect::<Vec<_>>().join(" "),
        );
    }
    para(
        &mut out,
        "Each *PATH* is a file, a directory, a glob, or an `http://`, `https://`, `s3://`, `gs://` or `az://` (`abfss://`) URL. Files of one shape are read as one table. `-` reads standard input, as does no *PATH* when data is piped in; with no *PATH* and nothing piped in, datui starts at its home screen.",
    );
    para(
        &mut out,
        "A file is scanned where it is wherever its format allows, and only the rows on screen are read; sorting, queries and analysis read what they need. datui-formats(7) says how each format is read. On any screen, `?` shows its keys; datui-keys(7) lists them all.",
    );

    out.push_str(".SH OPTIONS\n");
    let mut groups: Vec<Option<String>> = vec![None];
    for a in cmd.get_arguments() {
        let heading = a.get_help_heading().map(str::to_string);
        if !groups.contains(&heading) {
            groups.push(heading);
        }
    }
    for group in groups {
        let args: Vec<&clap::Arg> = shown_args(&cmd)
            .into_iter()
            .filter(|a| a.get_help_heading().map(str::to_string) == group)
            .collect();
        if args.is_empty() {
            continue;
        }
        out.push_str(&format!(
            ".SS {}\n",
            arg(&text(group.as_deref().unwrap_or("Arguments")))
        ));
        options(&mut out, &args);
    }
    out.push_str(".SS Help\n");
    item(
        &mut out,
        &format!("{}, {}", bold("-h"), bold("--help")),
        "Print help: a summary with `-h`, more with `--help`.",
    );
    item(
        &mut out,
        &format!("{}, {}", bold("-V"), bold("--version")),
        "Print the version.",
    );

    out.push_str(".SH COMMANDS\n");
    for sub in cmd.get_subcommands().filter(|c| c.get_name() != "help") {
        let about = sub.get_about().map(|a| a.to_string()).unwrap_or_default();
        item(
            &mut out,
            &bold(&format!("datui {}", sub.get_name())),
            &format!("{about}. See datui-{}(1).", sub.get_name()),
        );
    }

    exit_status(&mut out, true);

    out.push_str(".SH ENVIRONMENT\n");
    for group in [
        EnvGroup::Datui,
        EnvGroup::Terminal,
        EnvGroup::Programs,
        EnvGroup::Cloud,
    ] {
        out.push_str(&format!(".SS {}\n", arg(&text(group.title()))));
        if group == EnvGroup::Cloud {
            para(
                &mut out,
                "Read as each provider's own tools read them; a variable set but empty counts as unset. `[cloud] env_files` can read them from `.env` files.",
            );
        }
        for var in settings::ENVIRONMENT.iter().filter(|v| v.group == group) {
            environment_entry(&mut out, var);
        }
    }

    files(
        &mut out,
        Files {
            config: true,
            cache: true,
        },
    );
    examples(&mut out, page);
    see_also(&mut out, page, &[]);
    tail(&mut out, read);
    out
}

/// The subcommands' pages, git-style: `datui-config(1)` for `datui config`.
fn render_command(page: &Page, read: Read, name: &str) -> String {
    let cmd = subcommand(name);
    let mut out = head(page);
    let actions: Vec<&clap::Command> = cmd
        .get_subcommands()
        .filter(|c| c.get_name() != "help")
        .collect();
    let synopsis = |words: &str, c: &clap::Command| -> String {
        let mut s = bold(words);
        for a in shown_args(c) {
            if a.get_id() == "config" {
                continue;
            }
            let tag = option_tag(a);
            if a.is_positional() && a.is_required_set() {
                s.push_str(&format!(" {tag}"));
            } else {
                s.push_str(&format!(" [{tag}]"));
            }
        }
        s
    };

    out.push_str(".SH SYNOPSIS\n.nf\n");
    if actions.is_empty() || !cmd.is_subcommand_required_set() {
        out.push_str(&synopsis(&format!("datui {name}"), &cmd));
        out.push('\n');
    }
    for action in &actions {
        out.push_str(&synopsis(
            &format!("datui {name} {}", action.get_name()),
            action,
        ));
        out.push('\n');
    }
    out.push_str(".fi\n");

    out.push_str(".SH DESCRIPTION\n");
    let about = cmd
        .get_long_about()
        .or(cmd.get_about())
        .map(|a| a.to_string())
        .unwrap_or_default();
    para(&mut out, &format!("{about}."));
    let extra = match name {
        "config" => "The file's keys are in datui-config(5).",
        "catalog" => {
            "A catalog is one TOML file of named datasets, local or remote, that the home screen lists as a section under its label. *CONFIG*/catalog.toml is yours, and Ctrl+D on a home row adds to it; every *CONFIG*/catalogs/*.toml is a catalog, named by its file, and `catalogs` in the config lists files elsewhere; public ships with datui, and a catalogs/public.toml replaces it. A catalog's top level holds `label` and `description`; every other table is a dataset, keyed by a short id of lowercase letters, digits and `-`. A dataset's keys: `name` (its row), `path` or `url`, `auth` (`auto` or `anonymous`) or `connection` (a `[[cloud.connections]]` name), `description`, `publisher`, `license`, `homepage`, `documentation` (an https link), `size` (a web file's bytes, shown until measured), `columns.NAME = { description, unit, values = { CODE = \"meaning\" } }` and `bookmarks.\"Name\" = \"path/\"`. A long legend is a `[id.columns.NAME.values]` table, with the column's other keys written as dotted keys. `datui catalog show public` prints a worked example."
        }
        "cache" => {
            "The cache holds nothing datui cannot rebuild; clearing it loses the recents' order and the prompts' history."
        }
        "views" => {
            "A view is saved from the views list (`v`) at the table, and applied with `--view NAME` or from that list."
        }
        "formats" => {
            "Format specs are TOML files that describe a binary format, or a family of delimited text files; datui-formats(7) describes them. The search path is *CONFIG*/formats, then `$DATUI_FORMATS_PATH`, then `[formats] path` in the config."
        }
        "completions" => {
            "The script completes datui's options, commands and their values. Print it into the directory your shell loads completions from."
        }
        "man" => {
            "With no option, prints the page, or shows it with man(1) when standard output is a terminal; where man(1) is missing, as plain text through a pager: `$PAGER`, else less(1) or more(1). `--dir` writes every page, so `man datui` finds them; a package or the release archive installs them already. *PAGE* is a page's name with or without `datui-`, and `.5` or `.7` for the file and topic pages when a command shares the name."
        }
        _ => "",
    };
    if !extra.is_empty() {
        para(&mut out, extra);
    }

    if !actions.is_empty() {
        out.push_str(".SH COMMANDS\n");
        for action in &actions {
            let about = action
                .get_about()
                .map(|a| a.to_string())
                .unwrap_or_default();
            item(
                &mut out,
                &bold(&format!("datui {name} {}", action.get_name())),
                &format!("{about}."),
            );
            let args: Vec<&clap::Arg> = shown_args(action)
                .into_iter()
                .filter(|a| a.get_id() != "config")
                .collect();
            if !args.is_empty() {
                out.push_str(".RS\n");
                options(&mut out, &args);
                out.push_str(".RE\n");
            }
        }
    }

    out.push_str(".SH OPTIONS\n");
    let own: Vec<&clap::Arg> = shown_args(&cmd)
        .into_iter()
        .filter(|a| a.get_id() != "config")
        .collect();
    options(&mut out, &own);
    let global = crate::Args::command();
    if let Some(config) = global.get_arguments().find(|a| a.get_id() == "config") {
        options(&mut out, &[config]);
    }
    item(
        &mut out,
        &format!("{}, {}", bold("-h"), bold("--help")),
        "Print help.",
    );

    exit_status(&mut out, false);
    let (env, which, related): (&[&str], Option<Files>, &[&str]) = match name {
        "config" => (
            &["DATUI_CONFIG_DIR", "DATUI_LOG"],
            Some(Files {
                config: true,
                cache: false,
            }),
            &["datui-config.5"],
        ),
        "catalog" => (
            &["DATUI_CONFIG_DIR"],
            Some(Files {
                config: true,
                cache: false,
            }),
            &["datui-config.5"],
        ),
        "cache" => (
            &["DATUI_CACHE_DIR"],
            Some(Files {
                config: false,
                cache: true,
            }),
            &[],
        ),
        "views" => (
            &["DATUI_CONFIG_DIR"],
            Some(Files {
                config: true,
                cache: false,
            }),
            &[],
        ),
        "formats" => (
            &["DATUI_CONFIG_DIR", "DATUI_FORMATS_PATH"],
            Some(Files {
                config: true,
                cache: false,
            }),
            &["datui-formats.7"],
        ),
        _ => (&[], None, &[]),
    };
    if !env.is_empty() {
        environment(&mut out, env);
    }
    if let Some(which) = which {
        files(&mut out, which);
    }
    examples(&mut out, page);
    see_also(&mut out, page, related);
    tail(&mut out, read);
    out
}

fn render_config_file(page: &Page, read: Read) -> String {
    let mut out = head(page);
    out.push_str(".SH SYNOPSIS\n.nf\n");
    out.push_str(&format!("\\fICONFIG\\fR/{}\n", literal("config.toml")));
    out.push_str(&format!(
        "{} {} {}\n",
        bold("datui"),
        bold("-c"),
        italic("KEY=VALUE")
    ));
    out.push_str(".fi\n");
    out.push_str(".SH DESCRIPTION\n");
    para(
        &mut out,
        "datui's settings are TOML: a table per section, `[display]`, and a key per setting. A key is written `section.name` here and with `-c`.",
    );
    para(
        &mut out,
        "Values are taken, lowest first, from the defaults, the files listed in `import` (in order), the config file, `-c KEY=VALUE` (repeatable), and a key's own flag. `datui config init` writes the file with every key commented out at its default, `datui config path` prints the files read, and `datui config keys` lists every key with its value in effect and what set it. A key datui does not know, or a value a key does not take, is an error that names it.",
    );
    out.push_str(".SS Types\n");
    for (kind, written) in settings::TYPES {
        item(&mut out, &italic(kind), written);
    }
    item(&mut out, &italic("bool"), "`true` or `false`.");
    item(&mut out, &italic("path"), "A path; `~` and `$VAR` expand.");

    out.push_str(".SH SETTINGS\n");
    for section in settings::SECTIONS {
        let keys: Vec<&settings::Setting> = settings::in_section(section.name).collect();
        if keys.is_empty() {
            continue;
        }
        let title = if section.name.is_empty() {
            "Top level".to_string()
        } else {
            format!("[{}]", section.name)
        };
        out.push_str(&format!(".SS {}\n", arg(&literal(&title))));
        if !section.intro.is_empty() {
            para(&mut out, section.intro);
        }
        for setting in keys {
            let kind = setting.kind.describe().replace("\\|", "|");
            let tag = format!("{} ({})", bold(setting.key), italic(&kind));
            let mut body = setting.doc.to_string();
            if !body.ends_with('.') {
                body.push('.');
            }
            match setting.default {
                DefaultValue::Value(v) => body.push_str(&format!(" Default: `{v}`.")),
                DefaultValue::Unset(_) => body.push_str(" Unset by default."),
                DefaultValue::Color { dark, light } => {
                    body.push_str(&format!(" Default: `{dark}` dark, `{light}` light."))
                }
            }
            if let Some(flag) = setting.flag {
                body.push_str(&format!(" Flag: `--{flag}`."));
            }
            // A doc's own mention of a flag or key reads as Markdown would show it.
            item(&mut out, &tag, &body.replace('|', "\\|"));
        }
    }

    environment(&mut out, &["DATUI_CONFIG_DIR", "DATUI_LOG"]);
    files(
        &mut out,
        Files {
            config: true,
            cache: false,
        },
    );
    examples(&mut out, page);
    see_also(&mut out, page, &["datui-config.1"]);
    tail(&mut out, read);
    out
}

/// One group of keys: its name, then a tagged paragraph per key.
fn key_group(out: &mut String, name: Option<&str>, keys: &[keys::Key]) {
    if let Some(name) = name {
        out.push_str(".PP\n");
        out.push_str(&line(format!("\\fI{}\\fR", text(name))));
        out.push('\n');
    }
    for key in keys {
        out.push_str(".TP\n");
        out.push_str(&line(bold(key.keys)));
        out.push('\n');
        out.push_str(&line(text(key.long())));
        out.push('\n');
    }
}

fn render_keys(page: &Page, read: Read) -> String {
    let mut out = head(page);
    out.push_str(".SH DESCRIPTION\n");
    para(
        &mut out,
        "`?` or `F1` on any screen shows that screen's keys, grouped by task: `/` narrows them to those whose text matches, and Enter closes the help and presses the key on the line. This page lists every screen's keys, with the longer descriptions.",
    );
    out.push_str(".SH KEYS\n");
    out.push_str(&format!(".SS {}\n", arg(&text("Every screen"))));
    key_group(&mut out, None, keys::GLOBAL.keys);
    out.push_str(&format!(".SS {}\n", arg(&text("Help"))));
    key_group(&mut out, None, keys::HELP.keys);
    for screen in keys::SCREENS {
        out.push_str(&format!(".SS {}\n", arg(&text(screen.title))));
        para(&mut out, screen.reached);
        for group in screen.groups {
            key_group(&mut out, Some(group.name), group.keys);
        }
    }
    examples(&mut out, page);
    see_also(&mut out, page, &[]);
    tail(&mut out, read);
    out
}

/// What a query example runs on: the datasets of `scripts/docs/doc_datasets.toml`.
fn datasets(read: Read) -> toml::Table {
    read("scripts/docs/doc_datasets.toml")
        .parse()
        .expect("doc_datasets.toml is TOML")
}

fn render_query(page: &Page, read: Read) -> String {
    let mut out = head(page);
    out.push_str(".SH SYNOPSIS\n.nf\n");
    out.push_str(&format!(
        "{} [{}] [{} {}] [{} {}]\n",
        bold("select"),
        italic("columns"),
        bold("by"),
        italic("groups"),
        bold("where"),
        italic("conditions")
    ));
    out.push_str(".fi\n");
    out.push_str(".SH DESCRIPTION\n");
    // The summary the query prompt's help shows.
    out.push_str(".SS In brief\n");
    for (example, meaning) in keys::Q_SUMMARY {
        out.push_str(".TP\n");
        out.push_str(&line(literal(example)));
        out.push('\n');
        out.push_str(&line(text(meaning)));
        out.push('\n');
    }
    let doc = read("docs/reference/query-syntax.md");
    let table = datasets(read);
    let label = |name: &str| -> String { format!("On the {} dataset (see DATASETS):", bold(name)) };
    let md = Markdown {
        headings: Headings::Sections,
        dataset_label: Some(&label),
    };
    out.push_str(&md.render(&doc));

    // Every dataset an example names, and how to open it.
    let used: Vec<String> = doc
        .lines()
        .filter_map(|l| l.trim().strip_prefix("```"))
        .filter_map(|info| {
            info.split(',')
                .find_map(|a| a.trim().strip_prefix("dataset="))
        })
        .map(str::to_string)
        .fold(Vec::new(), |mut v, n| {
            if !v.contains(&n) {
                v.push(n);
            }
            v
        });
    out.push_str(".SH DATASETS\n");
    para(
        &mut out,
        "The examples run on public datasets from the built-in catalog (Public datasets on the home screen). Open one, press `/`, and type the query.",
    );
    for name in used {
        let entry = table.get(&name).and_then(|e| e.as_table());
        let open = entry
            .and_then(|e| e.get("url").or_else(|| e.get("open")))
            .and_then(|v| v.as_str())
            .unwrap_or_default();
        out.push_str(&format!(".TP\n{}\n", line(bold(&name))));
        out.push_str(".EX\n");
        out.push_str(&line(literal(&format!("datui {open}"))));
        out.push_str("\n.EE\n");
    }
    examples(&mut out, page);
    see_also(&mut out, page, &[]);
    tail(&mut out, read);
    out
}

fn render_formats(page: &Page, read: Read) -> String {
    let mut out = head(page);
    out.push_str(".SH DESCRIPTION\n");
    para(&mut out, &crate::docgen::format_count_sentence());
    let sections = Markdown {
        headings: Headings::Sections,
        dataset_label: None,
    };
    let inside = Markdown {
        headings: Headings::Subsections,
        dataset_label: None,
    };
    // The overview's count sentence is above already.
    let index = read("docs/formats/index.md");
    let index = match crate::docgen::splice("docs/formats/index.md", &index, "format-count", "") {
        Ok(text) => text,
        Err(_) => index,
    };
    out.push_str(&sections.render(&index));
    out.push_str(".SH \"FORMAT SPECS\"\n");
    out.push_str(&inside.render(&read("docs/formats/format-specs.md")));
    out.push_str(".SH \"FORMAT SPEC REFERENCE\"\n");
    out.push_str(&inside.render(&read("docs/reference/format-specs.md")));
    examples(&mut out, page);
    see_also(&mut out, page, &["datui-formats.1"]);
    tail(&mut out, read);
    out
}

/// `docs/reference/manual-pages.md`'s list: each page, linked to its HTML.
pub fn render_markdown_index() -> String {
    let mut out = String::from("| Page | What it covers |\n|---|---|\n");
    for page in PAGES {
        out.push_str(&format!(
            "| [{}](man/{}.html) | {} |\n",
            page.title(),
            page.file_name(),
            page.summary()
        ));
    }
    out
}

#[cfg(test)]
mod tests;
