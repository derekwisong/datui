//! The pages' conventions, and that every flag, command, key, variable and status in
//! the sources appears in a page. `the_generated_docs_are_current` checks that the
//! committed pages are what renders.

use super::*;

/// The committed page, by file name.
fn page(file: &str) -> String {
    PAGES
        .iter()
        .find(|p| p.file_name() == file)
        .unwrap_or_else(|| panic!("no page {file}"))
        .roff()
}

/// The order the pages keep: man-pages(7)'s, as the issue (#705) gives it. A page
/// has a subset, with its own sections between.
const ORDER: &[&str] = &[
    "NAME",
    "SYNOPSIS",
    "DESCRIPTION",
    "OPTIONS",
    "EXIT STATUS",
    "ENVIRONMENT",
    "FILES",
    "EXAMPLES",
    "SEE ALSO",
    "BUGS",
    "AUTHORS",
    "COPYRIGHT",
];

fn headings(roff: &str) -> Vec<String> {
    roff.lines()
        .filter_map(|l| l.strip_prefix(".SH "))
        .map(|h| h.trim_matches('"').to_string())
        .collect()
}

#[test]
fn every_page_has_its_title_line_and_name() {
    let date = RELEASE_DATE.trim();
    assert!(
        date.len() == 10 && date.as_bytes()[4] == b'-' && date.as_bytes()[7] == b'-',
        "release-date.txt is YYYY-MM-DD: {date}"
    );
    for p in PAGES {
        let roff = p.roff();
        let th = roff
            .lines()
            .find(|l| l.starts_with(".TH "))
            .unwrap_or_else(|| panic!("{}: no .TH", p.file_name()));
        assert_eq!(
            th,
            format!(
                ".TH {} {} {date} \"datui {}\" \"{}\"",
                literal(&p.name.to_uppercase()),
                p.section,
                env!("CARGO_PKG_VERSION"),
                manual(p.section)
            ),
            "{}",
            p.file_name()
        );
        // whatis and apropos read the line after `.SH NAME`: `name \- what`.
        let lines: Vec<&str> = roff.lines().collect();
        let at = lines.iter().position(|l| *l == ".SH NAME").expect("NAME");
        let name = lines[at + 1];
        assert!(
            name.starts_with(&format!("{} \\- ", literal(p.name))),
            "{}: {name}",
            p.file_name()
        );
        assert!(
            !name.contains("\\f"),
            "{}: a plain NAME line",
            p.file_name()
        );
    }
}

#[test]
fn sections_come_in_order() {
    for p in PAGES {
        let found: Vec<String> = headings(&p.roff())
            .into_iter()
            .filter(|h| ORDER.contains(&h.as_str()))
            .collect();
        let mut expected = found.clone();
        expected.sort_by_key(|h| ORDER.iter().position(|o| o == h));
        assert_eq!(found, expected, "{}", p.file_name());
        for required in [
            "NAME",
            "DESCRIPTION",
            "SEE ALSO",
            "BUGS",
            "AUTHORS",
            "COPYRIGHT",
        ] {
            assert!(
                found.iter().any(|h| h == required),
                "{}: no {required}",
                p.file_name()
            );
        }
        if p.section == 1 {
            for required in ["SYNOPSIS", "OPTIONS", "EXIT STATUS", "EXAMPLES"] {
                assert!(
                    found.iter().any(|h| h == required),
                    "{}: no {required}",
                    p.file_name()
                );
            }
        }
    }
}

/// No raw Unicode: every character outside ASCII is an escape, so a page reads the
/// same without UTF-8.
#[test]
fn pages_are_ascii() {
    for p in PAGES {
        for (n, l) in p.roff().lines().enumerate() {
            assert!(l.is_ascii(), "{}:{}: {l}", p.file_name(), n + 1);
        }
    }
}

/// Whether `roff` shows `what`, as an option or as prose writes it.
fn mentions(roff: &str, what: &str) -> bool {
    roff.contains(&literal(what)) || roff.contains(&text(what))
}

#[test]
fn every_flag_and_command_has_its_page() {
    let mut cmd = crate::Args::command();
    cmd.build();
    let datui = page("datui.1");
    for a in cmd.get_arguments().filter(|a| !a.is_hide_set()) {
        if let Some(long) = a.get_long() {
            assert!(mentions(&datui, &format!("--{long}")), "datui.1: --{long}");
        }
        if let Some(short) = a.get_short() {
            assert!(mentions(&datui, &format!("-{short}")), "datui.1: -{short}");
        }
    }
    for sub in cmd.get_subcommands().filter(|c| c.get_name() != "help") {
        let file = format!("datui-{}.1", sub.get_name());
        let roff = page(&file);
        assert!(mentions(&datui, &format!("datui {}", sub.get_name())));
        let mut all = vec![sub];
        all.extend(sub.get_subcommands().filter(|c| c.get_name() != "help"));
        for c in all {
            assert!(
                roff.contains(&literal(c.get_name())),
                "{file}: {}",
                c.get_name()
            );
            for a in c.get_arguments().filter(|a| !a.is_hide_set()) {
                if let Some(long) = a.get_long() {
                    assert!(mentions(&roff, &format!("--{long}")), "{file}: --{long}");
                }
                let values = a.get_action().takes_values().then(|| a.get_value_names());
                for name in values.flatten().into_iter().flatten() {
                    assert!(roff.contains(&literal(name)), "{file}: {name}");
                }
            }
        }
    }
}

#[test]
fn every_setting_and_variable_is_documented() {
    let config = page("datui-config.5");
    for setting in settings::SETTINGS {
        assert!(
            config.contains(&literal(setting.key)),
            "datui-config.5: {}",
            setting.key
        );
    }
    let datui = page("datui.1");
    for var in settings::ENVIRONMENT {
        for name in var.names {
            assert!(datui.contains(&literal(name)), "datui.1: {name}");
        }
    }
}

#[test]
fn every_key_and_format_is_documented() {
    let root = crate::docgen::repo_root();
    let keys_page = page("datui-keys.7");
    for screen in keys::SCREENS {
        let help = keys::parse(&crate::docgen::read_help(&root, screen.help));
        for section in help.sections.iter().filter(|s| s.is_keys()) {
            for row in &section.rows {
                assert!(
                    mentions(&keys_page, &row.label),
                    "datui-keys.7: {} in {}",
                    row.label,
                    screen.help
                );
            }
        }
    }
    let formats = page("datui-formats.7");
    for format in crate::FileFormat::ALL {
        assert!(
            formats.contains(&literal(format.name())),
            "datui-formats.7: {}",
            format.name()
        );
    }
}

#[test]
fn every_exit_status_is_documented() {
    for p in PAGES.iter().filter(|p| p.section == 1) {
        let roff = p.roff();
        for status in exit::STATUSES {
            let shown = roff.contains(&format!(".TP\n\\fB{}\\fR\n", status.code));
            assert_eq!(
                shown,
                p.name == "datui" || !status.session_only,
                "{}: {}",
                p.file_name(),
                status.code
            );
        }
    }
}

/// Every example names a page there is, and every command's page has one.
#[test]
fn examples_name_pages_that_exist() {
    for example in crate::examples() {
        for name in &example.pages {
            assert!(
                PAGES.iter().any(|p| &p.file_name() == name),
                "{}: no page {name}",
                example.command
            );
        }
    }
    for p in PAGES.iter().filter(|p| p.section == 1) {
        assert!(
            !crate::examples_of(&p.file_name()).is_empty(),
            "{} has no examples",
            p.file_name()
        );
    }
}

#[test]
fn a_page_is_found_by_its_short_name() {
    let found = |q: &str| find(q).map(Page::file_name);
    assert_eq!(found("datui").as_deref(), Some("datui.1"));
    assert_eq!(found("config").as_deref(), Some("datui-config.1"));
    assert_eq!(found("config.5").as_deref(), Some("datui-config.5"));
    assert_eq!(found("datui-config.5").as_deref(), Some("datui-config.5"));
    assert_eq!(found("keys").as_deref(), Some("datui-keys.7"));
    assert_eq!(found("formats.7").as_deref(), Some("datui-formats.7"));
    assert_eq!(found("nope"), None);
}
