//! The keys of every screen, read from the help strings `?` shows.
//!
//! `docs/reference/keyboard-shortcuts.md` is rendered from here, so the reference says
//! what the help says and cannot fall behind it. The help strings live in `datui-lib`
//! (`src/help-strings/*.txt`); callers hand their text in, read at run time, so this
//! crate needs none of them to build. [`parse`] is the shared reading: the markdown
//! reference uses it, and so can a manpage.
//!
//! A help file is blocks separated by blank lines. A line at the margin that ends in
//! `:` names a section. A keyed row is indented, its label, `:`, two or more spaces
//! and its description, which runs on over the lines indented past the label. A
//! section's rows are keys when one of its labels is a key; a section of terms (the
//! export formats, the home screen's sections) is prose to the reference and left
//! out of it, as is the rest of the prose.

/// One screen's help, in the order the reference lists them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Screen {
    /// The help file, without `.txt`.
    pub help: &'static str,
    /// Its heading in the reference.
    pub title: &'static str,
    /// How the screen is reached, as markdown.
    pub reached: &'static str,
}

/// Every help file, as the reference lists them. A test fails on a help file missing
/// here.
pub const SCREENS: &[Screen] = &[
    Screen {
        help: "main_view",
        title: "Table",
        reached: "Where a dataset opens.",
    },
    Screen {
        help: "home",
        title: "Home screen",
        reached: "`datui` with no path, or <kbd>Ctrl</kbd>+<kbd>O</kbd> from anywhere.",
    },
    Screen {
        help: "query",
        title: "Query prompt",
        reached: "<kbd>/</kbd> at the table.",
    },
    Screen {
        help: "find",
        title: "Find",
        reached: "<kbd>f</kbd> at the table.",
    },
    Screen {
        help: "go_to_line",
        title: "Go to row",
        reached: "<kbd>:</kbd> at the table.",
    },
    Screen {
        help: "go_to_column",
        title: "Go to column",
        reached: "<kbd>g</kbd> at the table.",
    },
    Screen {
        help: "inspector",
        title: "Inspector",
        reached: "<kbd>Space</kbd> at the table.",
    },
    Screen {
        help: "info_panel",
        title: "Info panel",
        reached: "<kbd>i</kbd> at the table.",
    },
    Screen {
        help: "value_counts",
        title: "Value counts",
        reached: "<kbd>F</kbd> at the table.",
    },
    Screen {
        help: "sort_filter",
        title: "Sort and filter",
        reached: "<kbd>s</kbd> at the table.",
    },
    Screen {
        help: "pivot_melt",
        title: "Pivot and melt",
        reached: "<kbd>p</kbd> at the table.",
    },
    Screen {
        help: "chart",
        title: "Chart",
        reached: "<kbd>c</kbd> at the table.",
    },
    Screen {
        help: "analysis_describe",
        title: "Analysis: Describe",
        reached: "<kbd>a</kbd> at the table.",
    },
    Screen {
        help: "analysis_distribution",
        title: "Analysis: Distribution",
        reached: "Distribution in the Analysis sidebar.",
    },
    Screen {
        help: "analysis_distribution_detail",
        title: "Analysis: Distribution detail",
        reached: "<kbd>Enter</kbd> on a column in Distribution.",
    },
    Screen {
        help: "analysis_correlation_matrix",
        title: "Analysis: Correlation",
        reached: "Correlation Matrix in the Analysis sidebar.",
    },
    Screen {
        help: "analysis_correlation_detail",
        title: "Analysis: Correlation detail",
        reached: "<kbd>Enter</kbd> on a pair in the correlation matrix.",
    },
    Screen {
        help: "analysis_data_quality",
        title: "Analysis: Data Quality",
        reached: "Data Quality in the Analysis sidebar.",
    },
    Screen {
        help: "export",
        title: "Export",
        reached: "<kbd>e</kbd> at the table.",
    },
    Screen {
        help: "copy",
        title: "Copy",
        reached: "<kbd>y</kbd> at the table.",
    },
    Screen {
        help: "views",
        title: "Views",
        reached: "<kbd>v</kbd> at the table.",
    },
    Screen {
        help: "format_picker",
        title: "Format picker",
        reached: "<kbd>b</kbd> at a table read through a format spec.",
    },
    Screen {
        help: "hex_view",
        title: "Hex view",
        reached: "`datui --hex FILE`, <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen, or <kbd>x</kbd> in the Info panel.",
    },
];

/// A help file, read.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Help {
    /// The prose before the first section or row, a string per paragraph.
    pub intro: Vec<String>,
    pub sections: Vec<Section>,
}

/// A run of keyed rows, under a heading or none.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Section {
    pub title: Option<String>,
    pub rows: Vec<Row>,
}

/// One keyed row: `Enter:  Run the query`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    /// What is typed, as the help writes it: `↑ / ↓ (j/k)`, `Ctrl+O`. Or, in a row
    /// that is not a key, the term it explains: `Sample size`.
    pub label: String,
    /// The description, its lines joined.
    pub text: String,
}

impl Section {
    /// Whether this is a section of keys: one of its labels is a key.
    pub fn is_keys(&self) -> bool {
        self.rows.iter().any(|r| is_key(&r.label))
    }
}

/// A keyed row's label and description: the label ends at a `:` followed by two or
/// more spaces.
fn keyed(line: &str) -> Option<(String, String)> {
    let indent = line.len() - line.trim_start_matches(' ').len();
    if indent == 0 {
        return None;
    }
    let body = &line[indent..];
    // The label ends at a `:` followed by two spaces or more; `::` is the `:` key.
    let mut search = 0;
    while let Some(at) = body[search..].find(':') {
        let colon = search + at;
        let rest = &body[colon + 1..];
        let gap = rest.len() - rest.trim_start_matches(' ').len();
        if colon > 0 && gap >= 2 && gap < rest.len() {
            let label = body[..colon].trim_end().to_string();
            let text = rest.trim().to_string();
            return Some((label, text));
        }
        search = colon + 1;
    }
    None
}

/// A section heading: text at the margin ending in `:`.
fn heading(line: &str) -> Option<String> {
    let trimmed = line.trim_end();
    (!line.starts_with(' ') && trimmed.ends_with(':') && !trimmed.contains(": "))
        .then(|| trimmed.trim_end_matches(':').to_string())
}

/// Read a help file.
pub fn parse(text: &str) -> Help {
    let mut help = Help::default();
    let mut current: Option<Section> = None;
    let mut seen_structure = false;
    for block in text.split("\n\n").map(|b| b.trim_matches('\n')) {
        if block.trim().is_empty() {
            continue;
        }
        let lines: Vec<&str> = block.lines().collect();
        let mut start = 0;
        if let Some(title) = lines.first().and_then(|l| heading(l)) {
            if let Some(done) = current.take() {
                help.sections.push(done);
            }
            current = Some(Section {
                title: Some(title),
                rows: Vec::new(),
            });
            seen_structure = true;
            start = 1;
        }
        let mut rows: Vec<Row> = Vec::new();
        // The last row's indent: a line indented past it continues its description.
        let mut row_indent: Option<usize> = None;
        let mut prose = false;
        for line in &lines[start..] {
            let indent = line.len() - line.trim_start_matches(' ').len();
            if let Some(&at) = row_indent.as_ref()
                && indent > at
                && let Some(row) = rows.last_mut()
            {
                row.text.push(' ');
                row.text.push_str(line.trim());
                continue;
            }
            if let Some((label, text)) = keyed(line) {
                rows.push(Row { label, text });
                row_indent = Some(indent);
                continue;
            }
            prose = true;
            row_indent = None;
        }
        if rows.is_empty() {
            if prose && !seen_structure && current.is_none() {
                help.intro.push(
                    lines[start..]
                        .iter()
                        .map(|l| l.trim())
                        .collect::<Vec<_>>()
                        .join(" "),
                );
            }
            continue;
        }
        seen_structure = true;
        match current.as_mut() {
            Some(section) => section.rows.extend(rows),
            None => current = Some(Section { title: None, rows }),
        }
    }
    if let Some(done) = current.take() {
        help.sections.push(done);
    }
    help.sections.retain(|s| !s.rows.is_empty());
    help
}

/// Named keys, as the help writes them.
const NAMED: &[&str] = &[
    "Enter",
    "Esc",
    "Tab",
    "Space",
    "Backspace",
    "Delete",
    "Del",
    "PgUp",
    "PgDn",
    "Home",
    "End",
    "Insert",
    "PageUp",
    "PageDown",
    "Click",
    "Wheel",
    "type",
    "typing",
    "(type)",
    "(text)",
    "(digits)",
    "↑↓",
    "←→",
];

/// Whether `label` is something typed: `Enter`, `↑ / ↓ (j/k)`, `Ctrl+U / Ctrl+K`,
/// `1-9`, `e, w, y, o`. A word that is not a key (`Sample size`, `Recent`) is not.
pub fn is_key(label: &str) -> bool {
    // A key that is itself punctuation: `,`, `(`.
    if label.trim().chars().count() == 1 {
        return true;
    }
    let cleaned = label.replace(['(', ')'], " ");
    let tokens = cleaned
        .split(|c: char| c.is_whitespace() || c == ',')
        .flat_map(|t| {
            // `PgUp/PgDn`, `h/l`; a lone `/` is the slash key.
            if t.len() > 1 && t.contains('/') {
                t.split('/').filter(|p| !p.is_empty()).collect::<Vec<_>>()
            } else {
                vec![t]
            }
        })
        .filter(|t| !t.is_empty() && !matches!(*t, "or" | "and" | "too"));
    let mut any = false;
    for token in tokens {
        any = true;
        let key = token
            .rsplit_once('+')
            .filter(|(m, k)| !m.is_empty() && !k.is_empty())
            .map_or(token, |(_, k)| k);
        let modifiers_ok = token
            .rsplit_once('+')
            .filter(|(m, k)| !m.is_empty() && !k.is_empty())
            .is_none_or(|(m, _)| m.split('+').all(|m| matches!(m, "Ctrl" | "Shift" | "Alt")));
        let single = key.chars().count() == 1;
        let function = key.starts_with('F') && key[1..].parse::<u8>().is_ok_and(|n| n <= 12);
        let range = key
            .split_once('-')
            .is_some_and(|(a, b)| a.parse::<u8>().is_ok() && b.parse::<u8>().is_ok());
        let named = NAMED.contains(&key) || NAMED.contains(&token);
        if !(modifiers_ok && (single || function || range || named || key == "Ctrl+Enter")) {
            return false;
        }
    }
    any
}

/// Escape text for a markdown table cell.
fn cell(text: &str) -> String {
    text.replace('|', "\\|")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

/// A key label as markdown: code, so `[` and `*` read as themselves.
fn key_cell(label: &str) -> String {
    if label.contains('`') {
        format!("`` {} ``", label.replace('|', "\\|"))
    } else {
        format!("`{}`", label.replace('|', "\\|"))
    }
}

/// The keyboard reference: every screen's keys, from its help. `read` returns a help
/// file's text by name (`main_view`).
pub fn render_markdown(read: &dyn Fn(&str) -> String) -> String {
    let mut out = String::new();
    for screen in SCREENS {
        let help = parse(&read(screen.help));
        out.push_str(&format!("\n## {}\n\n{}\n", screen.title, screen.reached));
        for section in help.sections.iter().filter(|s| s.is_keys()) {
            if let Some(title) = &section.title {
                out.push_str(&format!("\n### {} · {}\n", screen.title, title));
            }
            out.push_str("\n| Key | Action |\n|---|---|\n");
            for row in &section.rows {
                let label = if is_key(&row.label) {
                    key_cell(&row.label)
                } else {
                    cell(&row.label)
                };
                out.push_str(&format!("| {label} | {} |\n", cell(&row.text)));
            }
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_sections_and_continuations_are_read() {
        let help = parse(
            "Intro line one\nline two.\n\nNavigation:\n  ↑ / ↓ (j/k):      Move\n  ::                Go to a row\n                    by number\n\nTerms:\n  Sample size:      Rows read\n",
        );
        assert_eq!(help.intro, ["Intro line one line two."]);
        assert_eq!(help.sections.len(), 2);
        let nav = &help.sections[0];
        assert_eq!(nav.title.as_deref(), Some("Navigation"));
        assert_eq!(nav.rows[1].label, ":");
        assert_eq!(nav.rows[1].text, "Go to a row by number");
        assert!(nav.is_keys());
        assert!(!help.sections[1].is_keys());
    }

    #[test]
    fn keys_are_told_from_terms() {
        for key in [
            "↑ / ↓ (j/k)",
            "Ctrl+U / Ctrl+K",
            "PgUp/PgDn",
            "1-9",
            "e, w, y, o",
            "? / F1",
            ":",
            "Tab / Shift+Tab",
            "Space or typing",
            "(type)",
            "Esc / i",
            "< / > (, / .)",
            "+ / - (= / _)",
        ] {
            assert!(is_key(key), "{key}");
        }
        for term in ["Sample size", "Recent", "Python (Polars)", "CSV", "Bar"] {
            assert!(!is_key(term), "{term}");
        }
    }
}
