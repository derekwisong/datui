//! Retired words stay retired.
//!
//! `docs/reference/glossary.md` gives one name to each concept. This test fails
//! when a word the glossary retired, or a claim datui cannot keep ("opens
//! anything", #687), appears anywhere a user reads: the UI's strings, the help
//! strings, the docs, the READMEs, the next release's notes, `--help` (which the
//! shell completions are built from) and the generated manpage. A real use goes in `ALLOWED`, with the
//! file and the text that makes it real; an entry nothing matches fails too, so the
//! list never outlives what it excuses.

use clap::CommandFactory;
use std::path::{Path, PathBuf};

/// A retired phrase and the word to use instead. Matched without regard to case,
/// as whole words: `-`, `_`, `.` and `/` count as part of a word, so `--template`
/// and `views/` are not `template` or `views`, and `value counts` is not `value count`.
const RETIRED: &[(&str, &str)] = &[
    ("template", "view"),
    ("templates", "views"),
    ("dataset info", "Info"),
    ("statistical analysis", "Analysis"),
    ("row inspector", "inspector"),
    ("row detail", "inspector"),
    ("value count", "value counts"),
    ("count values", "value counts"),
    ("drill into", "drill down"),
    ("drills into", "drills down"),
    ("drilled into", "drilled down"),
    ("drilling into", "drilling down"),
    ("cloud browser", "cloud source"),
    ("browse files", "home screen"),
    ("binary spec", "format spec"),
    ("binary specs", "format specs"),
    ("dbc file", "dictionary"),
    ("dbc files", "dictionaries"),
    ("fix dictionary", "dictionary"),
    ("fix dictionaries", "dictionaries"),
    ("sheet", "table"),
    ("sheets", "tables"),
    ("row limit", "sample"),
    ("q-style", "q"),
    ("search mode", "Text mode"),
    ("sql, search", "SQL, Text"),
    ("fuzzy search", "Text query"),
    ("locate", "find"),
    ("go to line", "go to row"),
    // #687: never claim datui opens everything.
    ("opens anything", "the formats it reads"),
    ("open anything", "the formats it reads"),
    ("reads anything", "the formats it reads"),
    ("read anything", "the formats it reads"),
    ("opens any file", "the formats it reads"),
    ("open any file", "the formats it reads"),
    ("any data file", "the formats it reads"),
    ("any file format", "the formats it reads"),
    ("any format", "the formats it reads"),
    ("every format", "the formats it reads"),
    ("all formats", "the formats it reads"),
];

/// Real uses: a path (or path prefix) and text on the same line that makes the
/// use real.
const ALLOWED: &[(&str, &str)] = &[
    // The glossary names what each word replaces.
    ("docs/reference/glossary.md", "| "),
    // Omarchy's own word for the theme file its tooling fills in.
    (
        "docs/user-guide/configuration.md",
        "renders per-app theme files from templates",
    ),
    (
        "docs/user-guide/configuration.md",
        "Install the template from the repository",
    ),
    (
        "docs/user-guide/configuration.md",
        "The template sets `theme.mode`",
    ),
    (
        "docs/user-guide/configuration.md",
        "before rendering templates",
    ),
    // The Excel reader: the workbook XML's own tag, and Excel's kinds of sheet that
    // are not worksheets (chart sheet, dialog sheet, macro sheet).
    ("crates/datui-lib/src/excel.rs", "sheet"),
    // The check describing itself.
    ("docs/for-developers/tests.md", "\"opens anything\" claims"),
    // GGUF's own key.
    ("docs/", "chat template"),
    ("release-notes/", "chat template"),
    // The 0.4 release notes name what was renamed.
    ("release-notes/", "| `[templates]`"),
    ("release-notes/", "| `[query] default_mode = \"search\"`"),
    ("release-notes/", "Templates are now views"),
    ("release-notes/", "- The `template` module is `view`"),
];

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn files_under(dir: &Path, ext: &str, out: &mut Vec<PathBuf>) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path.is_dir() {
            files_under(&path, ext, out);
        } else if path.extension().is_some_and(|e| e == ext) {
            out.push(path);
        }
    }
}

fn word_char(c: char) -> bool {
    c.is_alphanumeric() || matches!(c, '-' | '_' | '.' | '/')
}

/// Whether `phrase` occurs in `line` as whole words.
fn occurrences(line: &str, phrase: &str) -> bool {
    let lower = line.to_lowercase();
    let mut from = 0;
    while let Some(at) = lower[from..].find(phrase) {
        let start = from + at;
        let end = start + phrase.len();
        let before = lower[..start].chars().next_back();
        let after = lower[end..].chars().next();
        // A sentence's full stop is not part of a word.
        let after_is_word = match after {
            Some('.') => lower[end + 1..]
                .chars()
                .next()
                .is_some_and(|c| c.is_alphanumeric()),
            Some(c) => word_char(c),
            None => false,
        };
        let before_is_word = before.is_some_and(word_char);
        if !before_is_word && !after_is_word {
            return true;
        }
        from = end;
    }
    false
}

/// One user-facing text: where it came from, and its lines.
struct Text {
    origin: String,
    lines: Vec<(usize, String)>,
}

fn whole_file(path: &Path) -> Text {
    let body = std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{} should be readable: {e}", path.display()));
    Text {
        origin: relative(path),
        lines: body
            .lines()
            .enumerate()
            .map(|(i, l)| (i + 1, l.to_string()))
            .collect(),
    }
}

fn relative(path: &Path) -> String {
    path.strip_prefix(root())
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// The string literals of a Rust file outside its tests: what the UI says.
fn rust_strings(path: &Path) -> Text {
    let body = std::fs::read_to_string(path).unwrap();
    let chars: Vec<char> = body.chars().collect();
    let mut lines = Vec::new();
    let (mut i, mut line) = (0, 1);
    let mut current = String::new();
    // Brace depth, and the depth a `#[cfg(test)]` module opened at: its strings are
    // the tests', not the UI's.
    let mut depth = 0usize;
    let mut in_tests: Option<usize> = None;
    let mut tests_ahead = false;
    let starts = |at: usize, text: &str| {
        text.chars()
            .enumerate()
            .all(|(k, t)| chars.get(at + k) == Some(&t))
    };
    while i < chars.len() {
        let c = chars[i];
        if c == '#' && in_tests.is_none() && starts(i, "#[cfg(test)]") {
            let mut j = i + "#[cfg(test)]".len();
            while chars.get(j).is_some_and(|c| c.is_whitespace()) {
                j += 1;
            }
            tests_ahead = ["mod ", "pub mod ", "pub(crate) mod "]
                .iter()
                .any(|m| starts(j, m));
            i = j;
        } else if c == '{' {
            if tests_ahead {
                in_tests = Some(depth);
                tests_ahead = false;
            }
            depth += 1;
            i += 1;
        } else if c == '}' {
            depth = depth.saturating_sub(1);
            if in_tests == Some(depth) {
                in_tests = None;
            }
            i += 1;
        } else if c == ';' {
            // `#[cfg(test)] mod name;` declares a file of tests, which is skipped whole.
            tests_ahead = false;
            i += 1;
        } else if c == '\n' {
            line += 1;
            i += 1;
        } else if c == '/' && chars.get(i + 1) == Some(&'/') {
            while i < chars.len() && chars[i] != '\n' {
                i += 1;
            }
        } else if c == '/' && chars.get(i + 1) == Some(&'*') {
            i += 2;
            while i + 1 < chars.len() && !(chars[i] == '*' && chars[i + 1] == '/') {
                line += usize::from(chars[i] == '\n');
                i += 1;
            }
            i += 2;
        } else if c == '\'' {
            // A char literal ('"', '\''), or a lifetime.
            if chars.get(i + 1) == Some(&'\\') {
                i += 2;
                while i < chars.len() && chars[i] != '\'' {
                    i += 1;
                }
                i += 1;
            } else if chars.get(i + 2) == Some(&'\'') {
                i += 3;
            } else {
                i += 1;
            }
        } else if c == 'r'
            && (chars.get(i + 1) == Some(&'"') || chars.get(i + 1) == Some(&'#'))
            && (i == 0 || !(chars[i - 1].is_alphanumeric() || chars[i - 1] == '_'))
        {
            let mut j = i + 1;
            let mut hashes = 0;
            while chars.get(j) == Some(&'#') {
                hashes += 1;
                j += 1;
            }
            if chars.get(j) != Some(&'"') {
                i += 1;
                continue;
            }
            j += 1;
            let start_line = line;
            current.clear();
            while j < chars.len() {
                if chars[j] == '"' && (0..hashes).all(|h| chars.get(j + 1 + h) == Some(&'#')) {
                    break;
                }
                line += usize::from(chars[j] == '\n');
                current.push(chars[j]);
                j += 1;
            }
            if in_tests.is_none() {
                lines.push((start_line, current.replace('\n', " ")));
            }
            i = j + 1 + hashes;
        } else if c == '"' {
            let start_line = line;
            current.clear();
            let mut j = i + 1;
            while j < chars.len() && chars[j] != '"' {
                if chars[j] == '\\' {
                    // A line continuation joins the next line's text with one space.
                    if chars.get(j + 1) == Some(&'\n') {
                        line += 1;
                        j += 2;
                        while chars.get(j).is_some_and(|c| c.is_whitespace()) {
                            line += usize::from(chars[j] == '\n');
                            j += 1;
                        }
                        current.push(' ');
                        continue;
                    }
                    j += 2;
                    current.push(' ');
                    continue;
                }
                line += usize::from(chars[j] == '\n');
                current.push(chars[j]);
                j += 1;
            }
            if in_tests.is_none() {
                lines.push((start_line, current.replace('\n', " ")));
            }
            i = j + 1;
        } else {
            i += 1;
        }
    }
    Text {
        origin: relative(path),
        lines,
    }
}

fn generated(origin: &str, body: &str) -> Text {
    Text {
        origin: origin.to_string(),
        lines: body
            .lines()
            .enumerate()
            .map(|(i, l)| (i + 1, l.to_string()))
            .collect(),
    }
}

/// Every text this test reads.
fn texts() -> Vec<Text> {
    let root = root();
    let mut paths = Vec::new();
    files_under(&root.join("docs"), "md", &mut paths);
    files_under(
        &root.join("crates/datui-lib/src/help-strings"),
        "txt",
        &mut paths,
    );
    for file in [
        "README.md",
        "python/README.md",
        "python/pyproject.toml",
        "crates/datui-cli/README.md",
        "crates/datui-cli/long_about.txt",
        "crates/datui-cli/examples.txt",
        "crates/datui-lib/README.md",
        "scripts/docs/index.html.j2",
    ] {
        let path = root.join(file);
        if path.exists() {
            paths.push(path);
        }
    }
    let version = env!("CARGO_PKG_VERSION").trim_end_matches("-dev");
    let notes = root.join(format!("release-notes/v{version}.md"));
    if notes.exists() {
        paths.push(notes);
    }
    let mut texts: Vec<Text> = paths.iter().map(|p| whole_file(p)).collect();

    let mut sources = Vec::new();
    files_under(&root.join("crates/datui-lib/src"), "rs", &mut sources);
    files_under(&root.join("crates/datui-cli/src"), "rs", &mut sources);
    files_under(&root.join("python/datui"), "py", &mut sources);
    for path in sources {
        let rel = relative(&path);
        if rel.contains("/src/tests/")
            || rel.ends_with("/tests.rs")
            || rel.ends_with("text_input_flows.rs")
        {
            continue;
        }
        if rel.ends_with(".py") {
            texts.push(whole_file(&path));
        } else {
            texts.push(rust_strings(&path));
        }
    }

    let mut cmd = datui_cli::Args::command();
    texts.push(generated(
        "datui --help",
        &cmd.render_long_help().to_string(),
    ));
    for sub in cmd.get_subcommands_mut() {
        let name = format!("datui {} --help", sub.get_name());
        texts.push(generated(&name, &sub.render_long_help().to_string()));
    }
    texts.push(generated(
        "datui.1",
        include_str!(concat!(env!("OUT_DIR"), "/datui.1")),
    ));
    texts
}

fn allowed(origin: &str, line: &str, used: &mut [bool]) -> bool {
    let mut hit = false;
    for (i, (prefix, text)) in ALLOWED.iter().enumerate() {
        if origin.starts_with(prefix) && line.contains(text) {
            used[i] = true;
            hit = true;
        }
    }
    hit
}

#[test]
fn retired_words_stay_out_of_what_users_read() {
    let mut found = Vec::new();
    let mut used = vec![false; ALLOWED.len()];
    for text in texts() {
        for (n, line) in &text.lines {
            for (phrase, instead) in RETIRED {
                if occurrences(line, phrase) && !allowed(&text.origin, line, &mut used) {
                    found.push(format!(
                        "{}:{n}: \"{phrase}\" (say \"{instead}\"): {}",
                        text.origin,
                        line.trim()
                    ));
                }
            }
        }
    }
    assert!(
        found.is_empty(),
        "retired wording; see docs/reference/glossary.md, or add a real use to ALLOWED in \
         tests/wording_test.rs:\n{}",
        found.join("\n")
    );
    let stale: Vec<_> = ALLOWED
        .iter()
        .zip(&used)
        .filter(|(_, used)| !**used)
        .map(|(entry, _)| format!("{entry:?}"))
        .collect();
    assert!(
        stale.is_empty(),
        "ALLOWED entries nothing matches any more; remove them:\n{}",
        stale.join("\n")
    );
}

#[test]
fn the_matcher_reads_whole_words() {
    assert!(occurrences("Save a template.", "template"));
    assert!(occurrences("Dataset Info tab", "dataset info"));
    assert!(!occurrences("datui --dict a.dbc", "dict"));
    assert!(!occurrences("a .dbc file", "dbc file"));
    assert!(!occurrences("the views/ directory", "views"));
    assert!(!occurrences("Value Counts", "value count"));
    assert!(!occurrences("templated", "template"));
    assert!(!occurrences("spreadsheet", "sheet"));
    assert!(occurrences("a DBC file.", "dbc file"));
}
