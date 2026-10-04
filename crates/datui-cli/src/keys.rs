//! The key registry: every key of every screen, in one table.
//!
//! Each [`Key`] is what is typed, a short label (for hints), a one-line description
//! (the in-app help), an optional longer description (the manpage and the docs),
//! and the key the help's Enter presses for it. Keys sit in task [`Group`]s on a
//! [`Screen`]; [`GLOBAL`] holds the keys every screen takes.
//!
//! What shows keys reads them from here: the help overlay `?` opens in `datui-lib`,
//! `docs/reference/keyboard-shortcuts.md` ([`render_markdown`]) and `datui-keys(7)`.
//! A key added to the app is an entry here; a test in `datui-lib` presses each
//! entry's key in its screen and fails when nothing takes it.

/// Where the keys are typed: one in-app help screen each.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Context {
    Table,
    Home,
    Query,
    Find,
    GoToRow,
    GoToColumn,
    Inspector,
    Info,
    ValueCounts,
    SortFilter,
    PivotMelt,
    Chart,
    Describe,
    Distribution,
    DistributionDetail,
    Correlation,
    CorrelationDetail,
    DataQuality,
    Export,
    Copy,
    Views,
    FormatPicker,
    Hex,
}

/// One screen's keys.
#[derive(Debug, Clone, Copy)]
pub struct Screen {
    pub context: Context,
    /// Its heading in the help, the reference and the manpage: `Table`.
    pub title: &'static str,
    /// How the screen is reached, as markdown.
    pub reached: &'static str,
    /// Its keys, by task, in the order the help shows them.
    pub groups: &'static [Group],
}

/// Keys that do one kind of task: `Explore`, `Shape`, `Analyze`, `Output`, or a
/// screen's own (`Fields`, `Picker`).
#[derive(Debug, Clone, Copy)]
pub struct Group {
    pub name: &'static str,
    pub keys: &'static [Key],
}

/// One key, or a few that do one thing in turn (`n / N`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Key {
    /// What is typed, as the help writes it: `↑ / ↓ (j/k)`, `Ctrl+O`, `(type)`.
    pub keys: &'static str,
    /// A word or two for a hint beside the key: `Filter`, `Next match`.
    pub label: &'static str,
    /// One line: the in-app help, and the man page and docs without [`Key::more`].
    pub line: &'static str,
    /// The whole description, where it says more than the line: the manpage and the
    /// docs print it in place of the line.
    pub more: Option<&'static str>,
    pub run: Run,
}

/// What Enter on a key's help line presses, in the screen help was opened from.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Run {
    /// The first key of [`Key::keys`].
    First,
    /// This key, as [`chord`] reads it.
    Key(&'static str),
    /// Nothing: typing, the mouse, a range of digits.
    Never,
}

/// A key entry: `keys`, a hint `label` and the help's `line`.
pub const fn k(keys: &'static str, label: &'static str, line: &'static str) -> Key {
    Key {
        keys,
        label,
        line,
        more: None,
        run: Run::First,
    }
}

impl Key {
    /// With the whole description for the manpage and the docs.
    pub const fn more(self, more: &'static str) -> Self {
        Key {
            more: Some(more),
            ..self
        }
    }

    /// Enter in the help presses `spec` rather than the first key.
    pub const fn run(self, spec: &'static str) -> Self {
        Key {
            run: Run::Key(spec),
            ..self
        }
    }

    /// Enter in the help presses nothing for this entry.
    pub const fn no_run(self) -> Self {
        Key {
            run: Run::Never,
            ..self
        }
    }

    /// The description the manpage and the docs print.
    pub fn long(&self) -> &'static str {
        self.more.unwrap_or(self.line)
    }

    /// The key Enter on this line presses, if any.
    pub fn action(&self) -> Option<Chord> {
        match self.run {
            Run::First => chord(first_key(self.keys)),
            Run::Key(spec) => chord(spec),
            Run::Never => None,
        }
    }
}

/// A key as the app receives it, without the terminal library: `datui-lib` turns it
/// into a key event.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Chord {
    pub code: Code,
    pub ctrl: bool,
    pub alt: bool,
    pub shift: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    Char(char),
    Enter,
    Esc,
    Tab,
    BackTab,
    Backspace,
    Delete,
    Insert,
    Up,
    Down,
    Left,
    Right,
    PageUp,
    PageDown,
    Home,
    End,
    F(u8),
}

/// Every key a `keys` text names, in order: `↑ / ↓ (j/k)` is ↑, ↓, j and k. Text that
/// is not a key (`(type)`, `Click`) names none.
pub fn chords(keys: &str) -> Vec<Chord> {
    let spaced = keys.replace(['(', ')'], " / ");
    spaced
        .split(" / ")
        .flat_map(|part| part.split(", "))
        .flat_map(|part| part.split(" or "))
        .map(str::trim)
        .flat_map(|token| {
            // `PgUp/PgDn`, `h/l`; a lone `/` is the slash key.
            if token.len() > 1 && token.contains('/') {
                token
                    .split('/')
                    .filter(|t| !t.is_empty())
                    .collect::<Vec<_>>()
            } else {
                vec![token]
            }
        })
        .filter_map(chord)
        .collect()
}

/// The first key a `keys` text names: `↑` of `↑ / ↓ (j/k)`, `Ctrl+D` of
/// `Ctrl+D/Ctrl+U`.
pub fn first_key(keys: &str) -> &str {
    let mut end = keys.len();
    for sep in [" / ", ", ", " (", " or ", " "] {
        if let Some(at) = keys.find(sep)
            && at > 0
        {
            end = end.min(at);
        }
    }
    let first = &keys[..end];
    // `PgUp/PgDn`, `Ctrl+D/Ctrl+U`; a lone `/` is the slash key.
    match first.find('/') {
        Some(at) if at > 0 && first.len() > 1 => &first[..at],
        _ => first,
    }
}

/// Read one key: `j`, `G`, `Enter`, `Ctrl+O`, `Shift+Tab`, `F1`, `↑`. `None` for text
/// that is not one key.
pub fn chord(spec: &str) -> Option<Chord> {
    let mut chord = Chord {
        code: Code::Esc,
        ctrl: false,
        alt: false,
        shift: false,
    };
    let mut rest = spec;
    loop {
        if let Some(r) = rest.strip_prefix("Ctrl+").filter(|r| !r.is_empty()) {
            chord.ctrl = true;
            rest = r;
        } else if let Some(r) = rest.strip_prefix("Alt+").filter(|r| !r.is_empty()) {
            chord.alt = true;
            rest = r;
        } else if let Some(r) = rest.strip_prefix("Shift+").filter(|r| !r.is_empty()) {
            chord.shift = true;
            rest = r;
        } else {
            break;
        }
    }
    chord.code = match rest {
        "Enter" => Code::Enter,
        "Esc" => Code::Esc,
        "Tab" if chord.shift => Code::BackTab,
        "Tab" => Code::Tab,
        "Space" => Code::Char(' '),
        "Backspace" => Code::Backspace,
        "Delete" | "Del" => Code::Delete,
        "Insert" => Code::Insert,
        "PgUp" | "PageUp" => Code::PageUp,
        "PgDn" | "PageDown" => Code::PageDown,
        "Home" => Code::Home,
        "End" => Code::End,
        "↑" | "↑↓" => Code::Up,
        "↓" => Code::Down,
        "←" | "←→" => Code::Left,
        "→" => Code::Right,
        _ => {
            let mut chars = rest.chars();
            match (chars.next(), chars.next()) {
                // Ctrl+O is the letter o with Ctrl; a capital alone comes with Shift,
                // as a terminal reports it.
                (Some(c), None) if c.is_ascii_alphabetic() && chord.ctrl => {
                    Code::Char(c.to_ascii_lowercase())
                }
                (Some(c), None) if !c.is_whitespace() => {
                    if c.is_ascii_uppercase() {
                        chord.shift = true;
                    }
                    Code::Char(c)
                }
                (Some('F'), Some(_)) => Code::F(rest[1..].parse().ok().filter(|n| *n <= 12)?),
                _ => return None,
            }
        }
    };
    Some(chord)
}

/// The keys every screen takes. Every screen's help lists them last.
pub const GLOBAL: Group = Group {
    name: "Everywhere",
    keys: &[
        k("? / F1", "Help", "This screen's keys (F1 in text fields)")
            .more("This screen's keys. In a text field ? types, and F1 opens the help")
            .run("F1"),
        k("Ctrl+O", "Home", "The home screen; abandons a load"),
        k(
            "Ctrl+Q / Ctrl+C",
            "Quit",
            "Quit (Ctrl+C in a text field too)",
        )
        .more("Quit from anywhere; Ctrl+C quits from a text field too, where Alt+W copies"),
    ],
};

/// The keys of the help overlay itself, for its footer and the reference.
pub const HELP: Group = Group {
    name: "Help",
    keys: &[
        k("↑ / ↓ (j/k)", "Move", "Move between the keys"),
        k("Enter", "Run", "Close the help and press the key")
            .more("Close the help and press the key on the line, where help was opened"),
        k("/", "Filter", "Narrow to the keys whose text matches"),
        k("Esc", "Close", "Clear the filter, then close"),
    ],
};

/// Where the whole reference is, the help's last line.
pub const REFERENCE_URL: &str =
    "https://derekwisong.github.io/datui/reference/keyboard-shortcuts.html";

/// The q language in a few lines, for the query screen's help and `datui-query(7)`:
/// an example and what it shows.
pub const Q_SUMMARY: &[(&str, &str)] = &[
    (
        "select [cols] [by groups] [where conds]",
        "Each part optional",
    ),
    (
        "select a, b where a > 10, b < 5",
        "In where, , is and; | is or",
    ),
    (
        "select avg price, n:count id by region",
        "Aggregate by group",
    ),
    (
        "total:a+b  col[\"first name\"]",
        "Name a column; a name with spaces",
    ),
    ("1/c+a is 1/(c+a)", "Right to left: 100 < (a+b)*2"),
    (
        "date.year  2024.01.31  city.contains[\"York\"]",
        "Accessors and dates",
    ),
    (
        "name in [\"Ann\", \"Bo\"]  item like \"*ham*\"",
        "Membership and patterns",
    ),
];

/// The analysis tools' sample keys, the same on each.
const SAMPLE_KEYS: &[Key] = &[
    k("s", "Sample", "Choose the sample every tool reads").more(
        "Choose the sample every tool reads: which rows, how they are picked, how many, the seed",
    ),
    k(
        "v",
        "Sample rows",
        "View the sample's rows as a table; Esc comes back",
    ),
    k(
        "r",
        "Resample",
        "Draw another sample (when the result is a sample)",
    ),
    k("a", "All rows", "Read every row instead, after confirming"),
    k("t", "New rows", "Read again with rows that arrived since").more(
        "While following a file, read again with the rows that arrived since the results were read",
    ),
];

/// Every screen, as the reference lists them.
pub const SCREENS: &[Screen] = &[
    Screen {
        context: Context::Table,
        title: "Table",
        reached: "Where a dataset opens.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("↑ / ↓ (j/k)", "Move", "Move the row cursor"),
                    k("← / → (h/l)", "Column", "Move the column cursor")
                        .more("Move the column cursor, frozen columns included; the columns scroll only when it would leave the screen"),
                    k("[ / ]", "Page columns", "A page of columns left or right")
                        .more("A page of columns left or right, the cursor on the page's first column (Shift+←/→ too)"),
                    k("{ / }", "First, last", "First column, last column"),
                    k("PgUp / PgDn", "Page", "A page up or down (Ctrl+B / Ctrl+F too)"),
                    k("Ctrl+D / Ctrl+U", "Half page", "Half a page down or up"),
                    k("Home / End", "Top, end", "First or last row (G = End)"),
                    k(":", "Go to row", "Go to a row number (:0 Enter for the top)"),
                    k("g", "Go to column", "Go to a column by name"),
                    k("f", "Find", "Find text or a regex in the view")
                        .more("Find text or a regex in the view; the cursor, column cursor and all, goes to the first match at or after its row"),
                    k("n / N", "Next, previous", "Next or previous match")
                        .more("Next / previous match from the cursor's cell, wrapping round the view"),
                    k("Enter", "Drill", "Drill down to a group's rows, or inspect")
                        .more("On a row of a by query or a SQL GROUP BY, drill down to its rows (Esc comes back); elsewhere, inspect the row"),
                    k("Space", "Inspect", "Inspect the row: every field, whole")
                        .more("Inspect the row: every field, each value whole and exact (Esc or Space closes). The bar's first chip says what Enter does: Inspect, or Drill"),
                ],
            },
            Group {
                name: "Shape",
                keys: &[
                    k("/", "Query", "Query: SQL, Text or q"),
                    k("s", "Sort & Filter", "Sort & Filter sidebar, on the cursor's column")
                        .more("Open the Sort & Filter sidebar (tabs: Columns, Filters), on the cursor's column"),
                    k("+ / -", "Filter", "Keep (+) or drop (-) rows with this cell's value")
                        .more("Filter on the cursor's cell: + keeps the rows with its value, - drops them (a null cell: the nulls). Each adds a row to the Filters tab, joined with \"and\"; the value is the cell's exactly as stored"),
                    k("r", "Reverse", "Reverse the sort, or the row order")
                        .more("Reverse sort order (sorted columns carry a direction mark in the header); with no sort, reverse the row order"),
                    k("H / L", "Move column", "Move the cursor's column left or right")
                        .more("Move the cursor's column one place left or right, the cursor with it; a frozen column moves among the frozen ones. R puts the order back"),
                    k("p", "Pivot & Melt", "Pivot or melt"),
                    k("R", "Reset", "Clear query, filters, sort, layout, view")
                        .more("Reset table: clear the query, filters, sort, column order, hidden columns and widths, frozen columns, pivot/melt, drill-down and the applied view"),
                ],
            },
            Group {
                name: "Analyze",
                keys: &[
                    k("F", "Counts", "Value counts of the cursor's column")
                        .more("Value counts of the cursor's column: each value's rows, percent and a bar, with a summary"),
                    k("a", "Analysis", "Describe, distributions, correlation, quality")
                        .more("Open Analysis. In a Data Quality evidence drill a is disabled; Esc returns to the observation"),
                    k("c", "Chart", "Chart the view"),
                    k("i", "Info", "Schema, resources, partitions, notes")
                        .more("Open the Info panel (tabs: Schema, Resources, Partitions, Notes). H on its Schema tab reads a CSV's first row as data, or as column names"),
                ],
            },
            Group {
                name: "Output",
                keys: &[
                    k("e", "Export", "Export the view to a file"),
                    k("y", "Copy", "Copy a cell, row, view or table")
                        .more("Copy to the clipboard (cell, row, view or table); a cell is the cursor's"),
                    k("v", "Views", "The saved views list"),
                    k("V", "Apply view", "Apply the best-matching view")
                        .more("Apply the best-matching view (or open the list)"),
                ],
            },
            Group {
                name: "Display",
                keys: &[
                    k("#", "Row numbers", "Row numbers on or off"),
                    k(",", "Grouping", "Number formatting (digit grouping) on or off"),
                    k("D", "Types", "The type row under the headers on or off"),
                    k("< / >", "Width", "The cursor's column 4 cells narrower or wider"),
                    k("= / w", "Fit, auto", "Fit the column to the rows on screen; automatic")
                        .more("Fit the cursor's column to the rows on screen; w puts it back to automatic width"),
                    k("b", "Format spec", "Read a binary file with another format spec")
                        .more("A binary file read through a format spec: read it again with another spec. Clears the query, filters and sort"),
                    k("t", "Follow", "Follow the file as it grows")
                        .more("Follow the file as it grows (CSV, TSV, PSV, NDJSON). Reads it again, so the query, filters and sort are cleared; while following, t pauses and resumes, and Esc stops"),
                ],
            },
            Group {
                name: "Go",
                keys: &[
                    k("q", "Back", "Home when opened from there, else quit")
                        .more("Back to the home screen when the dataset was opened from it; otherwise quit (the control bar says which)"),
                    k("Q", "Quit", "Quit"),
                    k("Esc", "Back", "Leave a drill-down, stop a find or a follow"),
                ],
            },
            Group {
                name: "Mouse",
                keys: &[
                    k("Click", "Point", "The cursor to the cell; on a header, its column")
                        .no_run(),
                    k("Double-click", "Enter", "Enter on the row").no_run(),
                    k("Wheel", "Scroll", "↑ / ↓, three rows a notch")
                        .more("↑ / ↓, three rows a notch; the same in help, the inspector and the sidebars")
                        .no_run(),
                    k("Shift+wheel", "Across", "← / →, the column cursor")
                        .more("← / →, the column cursor (a sideways wheel too)")
                        .no_run(),
                    k("Click a chip", "Press", "Press its key").no_run(),
                ],
            },
        ],
    },
    Screen {
        context: Context::Home,
        title: "Home screen",
        reached: "`datui` with no path, or <kbd>Ctrl</kbd>+<kbd>O</kbd> from anywhere.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("↑ / ↓", "Move", "Move the selection (Ctrl+P / Ctrl+N too)"),
                    k("Ctrl+↑ / Ctrl+↓", "Section", "Previous or next section"),
                    k("PgUp / PgDn", "Page", "A screenful, stopping at the first and last"),
                    k("Home / End", "First, last", "The first or last row"),
                    k("← / →", "Fold", "Fold or unfold; → goes inside")
                        .more("Fold or unfold a section; → on a directory or a file of tables goes inside it"),
                    k("Space", "Fold", "Fold the section (types once filtering)")
                        .more("While the filter is empty: fold or unfold the section header under the cursor. With a filter typed, it types"),
                    k("Tab", "Sort", "Cycle the sort")
                        .more("Cycle the sort; the control bar names the order in effect when it has room"),
                ],
            },
            Group {
                name: "Go",
                keys: &[
                    k("Enter", "Open", "What the control bar names: Open, Inside, Look")
                        .more("What the control bar says on this row: \"Open all\" reads a whole directory as one table, \"Inside\" steps into it, \"Open\" loads a file, \"Look\" finds out first. A place a collection suggests, indented under its dataset, opens whole. On a section header, fold or unfold it; on the More row, show the rest; on the hidden-files row, show them"),
                    k("Backspace", "Up", "Delete a filter character, or up a level")
                        .more("Delete a filter character; on an empty filter, up a level (from a bucket, back to its cloud source; from the top of a collection's remote dataset, back here)"),
                    k("Esc", "Back", "Path prompt, filter, directory, then the table")
                        .more("Back out one layer: the path prompt, the filter, the directory (back to the row it was entered from), then to the open table, which the control bar's chip names"),
                ],
            },
            Group {
                name: "Find",
                keys: &[
                    k("(type)", "Filter", "Narrow by name or column, fuzzy")
                        .more("Narrow by name or column; fuzzy, so \"sal\" finds \"sales\". What you open often ranks first. Typing also searches below the directory you are inside and the listed bucket names; matches appear under \"Found\"")
                        .no_run(),
                    k("~", "Path", "Type a path or URL (on an empty filter)")
                        .more("While the filter is empty: type a path or URL by hand. The list shows the directory being typed, narrowed by the name after the last /. The prompt is a plain editor: characters, Backspace, Ctrl+U clears, Tab completes the one name left or what the names share, ↑ / ↓ pick a name, Enter opens a file or goes inside a directory, Esc closes. s3://, gs:// and az:// complete from buckets and prefixes already known. With a filter typed, ~ types into it"),
                    k("Ctrl+U", "Clear", "Clear the filter or path input"),
                    k("Ctrl+R", "Refresh", "List again what is on screen"),
                ],
            },
            Group {
                name: "Manage",
                keys: &[
                    k("Ctrl+A", "All files", "Show or hide files datui cannot read")
                        .more("Show or hide files datui cannot read; inside a SQLite database, its internal tables"),
                    k("Ctrl+X", "Hex", "The file under the cursor as bytes")
                        .more("Show the local file under the cursor as bytes, in the hex view, whatever datui would read it as"),
                    k("Ctrl+D", "Remember", "Remember the directory; again to forget")
                        .more("Remember the directory under the cursor, so it stays listed; again to forget it. A file stands for the directory it is in, a heading for the one it lists"),
                    k("Delete", "Forget", "Forget a recent or a place; hide a cloud source")
                        .more("Forget the highlighted recent entry, or a whole place after confirming, or a remembered place on its heading, or hide a cloud source"),
                    k("Shift+Delete", "Forget all", "Forget every recent entry, after confirming"),
                ],
            },
            Group {
                name: "Mouse",
                keys: &[
                    k("Click", "Select", "Select the row; double-click is Enter").no_run(),
                    k("Wheel", "Scroll", "Move the selection three rows")
                        .more("Move the selection three rows, stopping at the ends")
                        .no_run(),
                ],
            },
        ],
    },
    Screen {
        context: Context::Query,
        title: "Query prompt",
        reached: "<kbd>/</kbd> at the table.",
        groups: &[
            Group {
                name: "Run",
                keys: &[
                    k("Enter", "Run", "Run the query")
                        .more("Run the query (reopening / restores the last query, selected: typing replaces it, arrows edit it). On the tab bar, Enter returns to the input"),
                    k("Ctrl+J", "Run", "Run, the same as Enter"),
                    k("Ctrl+T", "Mode", "Next mode: SQL, Text, q")
                        .more("Next mode: SQL, Text, q (from the input too)"),
                    k("Shift+Tab", "Tab bar", "Input to the tab bar and back"),
                    k("← / → (h/l)", "Mode", "On the tab bar: switch mode"),
                    k("Esc", "Close", "Close"),
                ],
            },
            Group {
                name: "Edit",
                keys: &[
                    k("Tab", "Complete", "SQL: complete a column name or df")
                        .more("SQL: complete a column name or df; again for the next match. Text, q: to the tab bar"),
                    k("Alt+Enter", "New line", "SQL: start a new line"),
                    k("↑ / ↓", "History", "Earlier and later queries (Ctrl+P / Ctrl+N)")
                        .more("Earlier and later queries from the history (Ctrl+P / Ctrl+N too; each mode keeps its own). In SQL over several lines, they move between lines first"),
                    k("Ctrl+U / Ctrl+K", "Delete", "Delete to the start or end of the line"),
                    k("Ctrl+Z / Ctrl+R", "Undo", "Undo, redo"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Find,
        title: "Find",
        reached: "<kbd>f</kbd> at the table.",
        groups: &[
            Group {
                name: "Find",
                keys: &[
                    k("(text)", "Pattern", "What to find: text, or a regex with Ctrl+R")
                        .no_run(),
                    k("Enter", "Find", "Go to the first match at or after the row")
                        .more("Find: the cursor, column cursor and all, goes to the first match at or after its row. On an empty field, clear the find"),
                    k("Ctrl+R", "Regex", "Regex on or off"),
                    k("Ctrl+L", "Column", "Only the cursor's column, or every column")
                        .more("Only the column cursor's column, or every column shown"),
                    k("↑ / ↓", "History", "Earlier patterns (Ctrl+P / Ctrl+N too)"),
                    k("Esc", "Cancel", "Cancel"),
                ],
            },
            Group {
                // Keys of the table, not the prompt: Enter here would type them.
                name: "At the table",
                keys: &[
                    k("n / N", "Next, previous", "Next or previous match")
                        .more("Next / previous match from the cursor's cell. Past the last match the find comes round to the first, and the bar says so. Each one typed while a find reads runs in turn; Esc stops them all").no_run(),
                    k("f", "Find again", "Find again, the last pattern ready to edit").no_run(),
                    k("Esc", "Clear", "Clear the find (or stop one still reading)").no_run(),
                ],
            },
        ],
    },
    Screen {
        context: Context::GoToRow,
        title: "Go to row",
        reached: "<kbd>:</kbd> at the table.",
        groups: &[Group {
            name: "Go",
            keys: &[
                k("(digits)", "Row", "The row to go to (:0 Enter is the top)").no_run(),
                k("Enter", "Go", "Go"),
                k("Backspace", "Delete", "Delete a digit"),
                k("Esc", "Cancel", "Cancel"),
            ],
        }],
    },
    Screen {
        context: Context::GoToColumn,
        title: "Go to column",
        reached: "<kbd>g</kbd> at the table.",
        groups: &[Group {
            name: "Go",
            keys: &[
                k("(type)", "Narrow", "Narrow to the names that contain it").no_run(),
                k("↑ / ↓", "Move", "Move"),
                k("Enter", "Go", "Move the column cursor to it")
                    .more("Go. The column cursor moves to it. A column already whole on screen stays where it is; another becomes the first after the frozen ones, or lands on the last page when it is there"),
                k("Backspace", "Delete", "Delete a character (Ctrl+W a word, Ctrl+U all)"),
                k("Esc", "Close", "Close without moving"),
            ],
        }],
    },
    Screen {
        context: Context::Inspector,
        title: "Inspector",
        reached: "<kbd>Space</kbd> at the table.",
        groups: &[
            Group {
                name: "Fields",
                keys: &[
                    k("↑ / ↓ (j/k)", "Move", "Move between fields"),
                    k("Home / End", "First, last", "First and last field"),
                    k("PgUp / PgDn", "Page", "A page of fields"),
                    k("← / → (h/l)", "Row", "Previous and next row")
                        .more("Previous and next row; the table's cursor moves with it"),
                    k("Tab", "Value", "Into the value, to scroll and find in it"),
                    k("Enter", "Open", "Open a struct, list or JSON; read a field")
                        .more("On a group's row, its rows, as at the table. Else open a struct, a list, or text holding a JSON object or array; or read a field the table's rows do not hold (hidden and binary columns)"),
                    k("r", "Read", "On a group's row, read a field the rows lack")
                        .more("On a group's row, read a field the rows do not hold"),
                    k("/", "Find", "Find a field by name, then by value")
                        .more("Find a field by name, then by value: type to narrow, Enter or ↓ keeps the list narrowed, Esc clears it"),
                    k("f", "Nulls", "Nulls shown or hidden (comparing: only diffs)")
                        .more("Nulls: shown or hidden (null and empty fields); comparing, only the fields that differ"),
                    k("s", "Order", "Order: the table's, A-Z, or nulls last"),
                    k("c", "Compare", "Compare with the next row, or the pinned one")
                        .more("Compare: a column for the next row, or the pinned one"),
                    k("m", "Pin", "Pin this row to compare others with; again to unpin"),
                    k("Esc / Space", "Close", "Close; Esc clears a find first"),
                ],
            },
            Group {
                name: "Output",
                keys: &[
                    k("y", "Copy", "Copy the value as its view shows it"),
                    k("Y", "Copy row", "Copy the whole row as one JSON object"),
                    k("e", "View", "The value's next view, where it has more than one"),
                    k("w", "Wrap", "Word wrap or hard wrap for long text"),
                    k("o", "Open", "Open the value in another program"),
                ],
            },
            Group {
                name: "Value",
                keys: &[
                    k("↑ / ↓ (j/k)", "Scroll", "Scroll a line"),
                    k("PgUp / PgDn", "Page", "Scroll a page"),
                    k("Home / End", "Top, end", "Top and end, however long the value")
                        .more("Top and end, at once however long the value"),
                    k("/", "Find", "Find in the value; n and N go on")
                        .more("Find in the value; n and N go to the next and last place"),
                    k("e, w, y, o", "As fields", "As in the fields"),
                    k("Esc / Tab", "Fields", "Back to the fields; Esc clears a find first"),
                ],
            },
            Group {
                name: "Nested",
                keys: &[
                    k("Enter / → (l)", "Open", "Open the focused item"),
                    k("Esc / ← (h)", "Up", "Up a level; at the row, Esc closes"),
                    k("y", "Copy", "Copy the item: text, or JSON indented")
                        .more("Copy the focused item: text as itself, a JSON object or array indented"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Info,
        title: "Info panel",
        reached: "<kbd>i</kbd> at the table.",
        groups: &[Group {
            name: "Panel",
            keys: &[
                k("← / → (h/l)", "Tab", "Switch tabs")
                    .more("Switch tabs. Afterward focus rests on the tab bar, so Tab returns to the body before ↑/↓ scroll"),
                k("Tab / Shift+Tab", "Focus", "Schema tab: tab bar and table")
                    .more("On Schema tab: move focus (tab bar ↔ schema table). On other tabs: focus stays on tab bar"),
                k("↑ / ↓ (j/k)", "Move", "Move the cursor, or scroll the list")
                    .more("Schema tab (when focused) or Notes tab: move the cursor. Model, Audio, MIDI, Metadata and format tabs: scroll the list"),
                k("PgUp / PgDn", "Page", "Scroll the list a page")
                    .more("Model, Audio, MIDI, Metadata and format tabs: scroll the list a page"),
                k("Home / End", "Top, end", "The top or the end of the list")
                    .more("Model, Audio, MIDI, Metadata and format tabs: the top or the end of the list"),
                k("Enter", "Take", "Notes tab: take the note's offer")
                    .more("Notes tab: take the offer on the note, where it has one"),
                k("H", "Header", "Schema tab, CSV: first row as data or names")
                    .more("Schema tab, CSV, TSV, PSV: read the first row as data, under column_1, column_2, …; again to read it as column names. Reads the file again, so the query, filters and sort are cleared, and the panel closes"),
                k("x", "Hex", "Show the file's bytes in the hex view"),
                k("Esc / i", "Close", "Close the panel"),
            ],
        }],
    },
    Screen {
        context: Context::ValueCounts,
        title: "Value counts",
        reached: "<kbd>F</kbd> at the table.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("↑ / ↓ (j/k)", "Move", "Move"),
                    k("PgUp / PgDn", "Page", "A page"),
                    k("Home / End", "First, last", "First and last row"),
                    k("← / → (h/l)", "Column", "The previous or next column")
                        .more("The previous or next column; the table's column cursor moves with it"),
                    k("Enter", "Drill", "The rows holding the value")
                        .more("The rows holding the value, as a drill-down; Esc there comes back here"),
                    k("s", "Sort", "Sort by count or by value"),
                    k("a", "All rows", "Count every row, when the counts are of a sample"),
                    k("t", "New rows", "Count again with the rows that arrived")
                        .more("While following a file, count again with the rows that arrived since; the bar says how many"),
                ],
            },
            Group {
                name: "Output",
                keys: &[
                    k("y", "Copy", "Copy the counts as TSV")
                        .more("Copy the counts as TSV: every value, its count, percent and cumulative percent"),
                    k("e", "Export", "Export the counts to a file"),
                    k("Esc", "Back", "Back to the table")
                        .more("Back to the table; while every row is being counted, stop that and keep the sample"),
                ],
            },
        ],
    },
    Screen {
        context: Context::SortFilter,
        title: "Sort and filter",
        reached: "<kbd>s</kbd> at the table.",
        groups: &[
            Group {
                name: "Sidebar",
                keys: &[
                    k("Tab / Shift+Tab", "Focus", "Between the tab bar and the body"),
                    k("← / →", "Tab", "Switch Columns / Filters (h/l on the tab bar)"),
                    k("Enter", "Apply", "Apply and close (Filters tab: edit the row)")
                        .more("Apply everything staged and close (on the Filters tab, Enter adds or edits instead)"),
                    k("a", "Apply", "Filters tab, outside the editor: apply and close")
                        .more("On the Filters tab, outside the row editor: apply and close"),
                    k("Ctrl+J", "Apply", "Apply from anywhere, mid-edit too")
                        .more("Apply from anywhere, including mid-edit (the row in progress is saved). Ctrl+Enter does the same, on a terminal that tells it from Enter"),
                    k("Esc", "Cancel", "Close; staged changes are discarded")
                        .more("Cancel and close; staged changes are discarded, and reopening shows what is applied"),
                ],
            },
            Group {
                name: "Columns",
                keys: &[
                    k("(type)", "Narrow", "Narrow the list, with the find field focused")
                        .more("Narrow the column list, when the find field is focused")
                        .no_run(),
                    k("Space", "Sort", "Cycle the sort: none, ascending, descending")
                        .more("Cycle the column's sort: none → ascending → descending (each column carries its own direction)"),
                    k("1-9", "Sort place", "Put the column at that place in the sort")
                        .more("Put the column at that place in the sort order; 0 removes it (a digit past the end of the order says so)")
                        .run("1"),
                    k("Del", "Unsort", "Remove the column from the sort"),
                    k("[ / ]", "Sort order", "Earlier or later in the sort order"),
                    k("+ / - (= / _)", "Position", "Move the column's display position"),
                    k("L", "Freeze", "Freeze columns up to this one at the left")
                        .more("Freeze this column and every column up to it at the left edge; on a column already frozen, pull the boundary back"),
                    k("v", "Hide", "Show or hide the column")
                        .more("Show or hide the column (it keeps its place, dimmed)"),
                    k("< / > (, / .)", "Width", "4 cells narrower or wider")
                        .more("Make the column 4 cells narrower or wider. A number column is never narrower than its numbers"),
                    k("f", "Fit", "Fit to the rows on screen")
                        .more("Fit the column to the rows on screen, its name up to the automatic limit"),
                    k("w", "Auto width", "Back to the automatic width"),
                    k("C", "Clear", "Clear staged sort, order, locks, hidden, widths")
                        .more("Clear the staged sort, order, locks, hidden columns and widths"),
                ],
            },
            Group {
                name: "Filters",
                keys: &[
                    k("Enter", "Edit", "Edit the row, or add one on the last row")
                        .more("Edit the row under the cursor, or add one on the last row. Editing walks three steps on the row: pick the column (type to narrow, Enter chooses), pick the operator the same way, then type the value and Enter saves the row. Tab, → and Space also choose at the column and operator steps; Shift+Tab steps back. ↑↓ (j/k) move in both lists, and ↓ jumps from the find field into the list. Esc backs out of the edit and only the edit"),
                    k("Space", "And/or", "Toggle and/or on the row"),
                    k("d / Del", "Delete", "Delete the row"),
                    k("C", "Clear", "Clear every staged filter"),
                ],
            },
        ],
    },
    Screen {
        context: Context::PivotMelt,
        title: "Pivot and melt",
        reached: "<kbd>p</kbd> at the table.",
        groups: &[
            Group {
                name: "Form",
                keys: &[
                    k("Tab / Shift+Tab", "Next", "Move between the rows (↑ / ↓ too)")
                        .more("Move between the rows (↑/↓ too); in a picker: choose and move to the next or previous row"),
                    k("← / →", "Mode", "Switch Pivot and Melt")
                        .more("Switch Pivot and Melt (h/l too, outside text fields); in a text field ←/→ move the cursor"),
                    k("Space", "Pick", "Open the row's picker; typing narrows it")
                        .more("Open the row's picker, narrowed by what you type (typing opens it too)"),
                    k("Enter", "Apply", "Apply the spec echoed above the footer"),
                    k("Esc", "Close", "Close without applying; stop a pivot")
                        .more("Close without applying; while a pivot is computed, stop it and keep the form"),
                ],
            },
            Group {
                name: "Picker",
                keys: &[
                    k("↑ / ↓", "Move", "Move; typing narrows"),
                    k("Enter", "Choose", "Choose; on a several-choice row, done"),
                    k("Space", "Toggle", "Choose; toggle a column on a several-choice row"),
                    k("Esc", "Back", "Back out of the picker, keeping toggles"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Chart,
        title: "Chart",
        reached: "<kbd>c</kbd> at the table.",
        groups: &[
            Group {
                name: "Options",
                keys: &[
                    k("1-6", "Type", "Chart type: XY, Histogram, Box, KDE, Heatmap, Bar")
                        .more("Switch chart type directly: XY, Histogram, Box Plot, KDE, Heatmap, Bar ([ / ] cycle)")
                        .run("1"),
                    k("[ / ]", "Type", "Previous or next chart type"),
                    k("Tab / Shift+Tab", "Next", "Move between the option rows (↑ / ↓ too)"),
                    k("Enter / Space", "Pick", "Open a picker, toggle, or cycle the row")
                        .more("Open a column row's picker, toggle an option, or cycle the plot style, range or order"),
                    k("← / → (h/l)", "Adjust", "Cycle a choice; adjust a number (+ / - too)")
                        .more("Cycle the plot style, range or order; adjust bins, bandwidth, or Sample size (+ / - too)"),
                    k("PgUp / PgDn", "Step", "Adjust Sample size in bigger steps"),
                    k("g", "Grid", "Grid on or off")
                        .more("Grid on or off at the labeled ticks: XY, Histogram, Box Plot and KDE. [analysis] chart_grid sets where it starts"),
                    k("Esc", "Back", "Back to the table"),
                ],
            },
            Group {
                name: "Plot",
                keys: &[
                    k("x", "Crosshair", "XY: a crosshair reads out the values")
                        .more("XY: the plot takes the keys, and a crosshair reads out x and every series' value under the plot. ← / → (h/l) step to the next point or column, Home/End go to the ends; x, Tab or Esc hand the keys back to the option rows. A click on the plot puts the crosshair there"),
                    k("e", "Export", "Export the chart to PNG or EPS")
                        .more("Export the chart to PNG or EPS. Needs the chart's required columns picked first"),
                    k("t", "New rows", "Draw again with the rows that arrived")
                        .more("While following a file, draw again with the rows that arrived since; the bar says how many"),
                ],
            },
            Group {
                name: "Picker",
                keys: &[
                    k("↑ / ↓", "Move", "Move; typing narrows"),
                    k("Enter / Space", "Choose", "Choose (Y series: Space toggles one)")
                        .more("Choose; on the Y series row Space toggles a series in or out"),
                    k("Tab / Shift+Tab", "Next", "Choose and move to the next or previous row"),
                    k("Esc", "Back", "Back out of the picker alone"),
                ],
            },
            Group {
                name: "Export dialog",
                keys: &[
                    k("Tab / Shift+Tab", "Next", "Format, Path, Title, Width, Height"),
                    k("↑ / ↓ (j/k)", "Format", "Change the format"),
                    k("Enter", "Export", "Export, from anywhere in the dialog")
                        .more("Export, from anywhere in the dialog. An existing file asks Overwrite / No, starting on No; ←/→ (h/l) or Tab pick, Enter confirms, and declining returns to the filled dialog"),
                    k("Esc", "Back", "Back to the chart"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Describe,
        title: "Analysis: Describe",
        reached: "<kbd>a</kbd> at the table.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("Tab", "Focus", "Between the results and the sidebar"),
                    k("↑ / ↓ (j/k)", "Move", "Rows, or the sidebar's tools"),
                    k("← / → (h/l)", "Scroll", "Scroll the statistics")
                        .more("Scroll the statistics; the header counts those out of view"),
                    k("Home / End", "First, last", "First or last row"),
                    k("PgUp / PgDn", "Page", "A page"),
                    k("Enter", "Tool", "Run the sidebar's tool")
                        .more("Select tool from sidebar (when sidebar focused). The first run on a dataset starts in the Sample form, where Enter runs it; later tools reuse that sample"),
                    k("Esc", "Close", "Cancel a run; otherwise close")
                        .more("Cancel a run in progress; otherwise close the analysis view"),
                ],
            },
            Group {
                name: "Sample",
                keys: SAMPLE_KEYS,
            },
        ],
    },
    Screen {
        context: Context::Distribution,
        title: "Analysis: Distribution",
        reached: "Distribution in the Analysis sidebar.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("↑ / ↓ (j/k)", "Move", "Rows, or the sidebar's tools"),
                    k("← / → (h/l)", "Scroll", "Scroll the statistics")
                        .more("Scroll the statistics; the header counts those out of view"),
                    k("Home / End", "First, last", "First or last row"),
                    k("PgUp / PgDn", "Page", "A page"),
                    k("Tab", "Focus", "Between the results and the sidebar"),
                    k("Enter", "Detail", "Q-Q plot and histogram for the column")
                        .more("Open detail view for selected column (shows Q-Q plot and histogram); with the sidebar focused, select a tool"),
                    k("Esc", "Close", "Cancel a run; otherwise close")
                        .more("Cancel a run in progress; otherwise close the analysis view"),
                ],
            },
            Group {
                name: "Sample",
                keys: SAMPLE_KEYS,
            },
        ],
    },
    Screen {
        context: Context::DistributionDetail,
        title: "Analysis: Distribution detail",
        reached: "<kbd>Enter</kbd> on a column in Distribution.",
        groups: &[Group {
            name: "Detail",
            keys: &[
                k("↑ / ↓ (j/k)", "Family", "Compare the values with another family"),
                k("s", "Scale", "Histogram scale: linear or log"),
                k("Esc", "Back", "Back to the distribution table"),
            ],
        }],
    },
    Screen {
        context: Context::Correlation,
        title: "Analysis: Correlation",
        reached: "Correlation Matrix in the Analysis sidebar.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("Tab", "Focus", "Between the matrix and the sidebar"),
                    k("↑ / ↓ (j/k)", "Move", "Matrix rows, or the sidebar's tools"),
                    k("← / → (h/l)", "Column", "Matrix columns"),
                    k("Home / End", "Corners", "The first or last corner cell")
                        .more("Jump to the first/last corner cell (the column resets too)"),
                    k("PgUp / PgDn", "Page", "A page"),
                    k("Enter", "Detail", "The pair's detail (not on the diagonal)")
                        .more("Open pair detail view (on a cell) or select tool (sidebar); does nothing on a diagonal cell"),
                    k("m", "Method", "Pearson or Spearman, named in the title")
                        .more("Method: Pearson r or Spearman ρ, named in the title (both come from the one run, so it reads nothing)"),
                    k("Esc", "Close", "Cancel a run; otherwise close")
                        .more("Cancel a run in progress; otherwise close the analysis view"),
                ],
            },
            Group {
                name: "Sample",
                keys: SAMPLE_KEYS,
            },
        ],
    },
    Screen {
        context: Context::CorrelationDetail,
        title: "Analysis: Correlation detail",
        reached: "<kbd>Enter</kbd> on a pair in the correlation matrix.",
        groups: &[Group {
            name: "Detail",
            keys: &[
                k("m", "Method", "Pearson or Spearman"),
                k("Esc", "Back", "Back to the correlation matrix")
                    .more("Return to the correlation matrix. Resampling (r) works from the matrix, not from inside this detail"),
            ],
        }],
    },
    Screen {
        context: Context::DataQuality,
        title: "Analysis: Data Quality",
        reached: "Data Quality in the Analysis sidebar.",
        groups: &[
            Group {
                name: "Setup",
                keys: &[
                    k("↑ / ↓, Tab", "Move", "Move between rows"),
                    k("← / →", "Choose", "Change a short list's choice"),
                    k("Space", "Open", "Open the row's form or list")
                        .more("Open the row: the Sample form, a list (type to narrow), Time roles, Intervals, Column intent or Expected"),
                    k("s", "Sample", "The Sample form")
                        .more("The Sample form; its Enter applies to Setup and returns there"),
                    k("p", "Plan", "The access plan: what a run reads"),
                    k("d", "Release", "Release kept rows and the local copy")
                        .more("Release the rows runs kept and a full scan's local copy, named on the Read rule; the next run that would reuse them reads again"),
                    k("Enter", "Run", "Run, from any row")
                        .more("Run, from any row. A full read asks first; the report on screen or in the session cache is shown, not read again"),
                    k("Esc", "Discard", "Discard every staged edit"),
                ],
            },
            Group {
                name: "Setup lists",
                keys: &[
                    k("↑ / ↓", "Move", "Choose the role, pair, column or row")
                        .more("Time roles: choose the role. Intervals: choose the pair. Column intent: choose the column. Expected: Windows, From, Before (Tab too)"),
                    k("← / →", "Choose", "Time roles: its column; Expected: windows")
                        .more("Time roles: choose the role's column. Expected: choose the windows. Intervals: measure the pair or not"),
                    k("Space", "Toggle", "Intervals: measure or not; intent: its form")
                        .more("Intervals: measure the pair or not. Column intent: declare the column's intent in a form"),
                    k("Enter", "Done", "Done"),
                    k("Esc", "Undo", "Put the list back as it was"),
                ],
            },
            Group {
                name: "Report",
                keys: &[
                    k("← / → (h/l)", "Page", "Previous or next page"),
                    k("1 - 5", "Page", "Overview, Columns, Segments, Trends, Intervals")
                        .run("1"),
                    k("↑ / ↓ (j/k)", "Move", "Move, or scroll a tall finding"),
                    k("PgUp / PgDn", "Page", "A page; Home / End the ends")
                        .more("Page; Home/End jump to either end"),
                    k("Enter", "Open", "Open a finding, then its rows")
                        .more("Open a finding, then its rows. On an empty page, open the setting it needs"),
                    k("c / t", "Only", "Overview: one column's or type's findings")
                        .more("Overview: only one column's or one type's findings"),
                    k("o", "Order", "Overview: ranked, by rows, by rate"),
                    k("e", "Setup", "Setup"),
                    k("s", "Sample", "Setup, with the Sample form open"),
                    k("v", "Sample rows", "View the sample's rows; Esc returns"),
                    k("p", "Plan", "The access plan: what a run reads"),
                    k("r", "Resample", "On a sample, run again with a new seed"),
                    k("x", "Export", "Export the report to JSON or Markdown")
                        .more("Export the report to JSON or Markdown; nothing is read"),
                    k("Tab", "Focus", "Between the result and the tools"),
                    k("Esc", "Back", "Back one level: all findings, then close")
                        .more("Back one level: all findings again, then close"),
                ],
            },
            Group {
                name: "Segments",
                keys: &[
                    k("Enter", "Compare", "The segment's columns beside its comparison")
                        .more("A segment's columns beside the one it is compared with, largest change first"),
                    k("o", "Order", "Largest change first, or back in order"),
                    k("b", "Base", "Compare with the highlighted segment"),
                ],
            },
            Group {
                name: "Trends",
                keys: &[
                    k("Enter", "Bars", "A line's bars, with rate and interval")
                        .more("A line's bars: span, segments, rows sampled of counted, rate, 95% interval, and the bar before. ↑ / ↓ there: the next bar"),
                    k("m", "Measure", "Next measure"),
                    k("w", "Coarser", "Stage a coarser window in Setup")
                        .more("Stage a coarser window in Setup, for segments the sample reached thinly or not at all"),
                    k("g", "Gaps", "The expected windows with no rows")
                        .more("The expected windows with no rows: empty by exact count, not sampled, or out of scope"),
                    k("Esc", "Back", "Back to Trends"),
                ],
            },
            Group {
                name: "Intervals",
                keys: &[
                    k("Enter", "Detail", "An interval's detail; again, its rows")
                        .more("An interval's detail: its ends, the rows with both, missing and unread ends, negative and zero durations, percentiles and breaches. Enter there shows the rows behind the count under the cursor: a sample's from the rows the run kept"),
                    k("Esc", "Back", "Back to the list"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Export,
        title: "Export",
        reached: "<kbd>e</kbd> at the table.",
        groups: &[
            Group {
                name: "Form",
                keys: &[
                    k("Tab / Shift+Tab", "Next", "Move between fields"),
                    k("↑ / ↓ (j/k)", "Format", "Change the format or compression")
                        .more("In the format list: change format. On Compression: change compression. In Path and Delimiter: nothing (no history there, and j/k are characters)"),
                    k("← / → (h/l)", "Cursor", "Move the text cursor; change compression")
                        .more("Move the cursor in text fields. On Compression: change compression. On Include header and Source file: move focus"),
                    k("Space", "Toggle", "Toggle a checkbox")
                        .more("Toggle a checkbox (Include header, Source file)"),
                    k("Enter", "Export", "Export, from anywhere in the form")
                        .more("Export, from anywhere in the form. On a blank path the form says \"Enter a file path.\" instead of exporting"),
                    k("Esc", "Close", "Close without exporting"),
                ],
            },
            Group {
                name: "File exists",
                keys: &[
                    k("← / → (h/l)", "Pick", "Pick Overwrite or No (Tab too)"),
                    k("Enter", "Confirm", "Confirm the one picked"),
                    k("Esc", "No", "Decline: back to the filled form"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Copy,
        title: "Copy",
        reached: "<kbd>y</kbd> at the table.",
        groups: &[
            Group {
                name: "Form",
                keys: &[
                    k("Tab / Shift+Tab", "Next", "Move between rows"),
                    k("↑ / ↓", "Move", "Move between rows"),
                    k("Space", "Pick", "Open the row's picker; on Header: toggle"),
                    k("Enter", "Copy", "Copy, from anywhere in the form")
                        .more("Copy, from anywhere in the form. On the Cell scope with no column picked, Enter re-accents the spec line instead of copying"),
                    k("Esc", "Close", "Close a picker, then the dialog")
                        .more("Close a picker, then the dialog, without copying"),
                ],
            },
            Group {
                name: "Picker",
                keys: &[
                    k("(type)", "Narrow", "Narrow the list").no_run(),
                    k("↑ / ↓", "Move", "Move the cursor (j/k narrow)")
                        .more("Move the cursor (j/k narrow the picker; only ↑/↓ move there)"),
                    k("Enter / Space", "Choose", "Choose"),
                    k("Tab / Shift+Tab", "Next", "Choose and move on"),
                ],
            },
        ],
    },
    Screen {
        context: Context::Views,
        title: "Views",
        reached: "<kbd>v</kbd> at the table.",
        groups: &[
            Group {
                name: "List",
                keys: &[
                    k("↑ / ↓ (j/k)", "Move", "Move in the list"),
                    k("Enter", "Apply", "Apply the selected view"),
                    k("s", "Save", "Save the current state as a view")
                        .more("Save the current state as a view (an untouched table has nothing to save, and s says so)"),
                    k("e", "Edit", "Edit the selected view"),
                    k("d", "Delete", "Delete the selected view, after confirming"),
                    k("i", "Score", "How the view's score was computed")
                        .more("Show how the selected view's score was computed (Esc closes the score view)"),
                    k("Esc", "Close", "Close"),
                ],
            },
            Group {
                name: "Save and edit",
                keys: &[
                    k("Tab / Shift+Tab", "Next", "Move between rows (↑ / ↓ outside the description)"),
                    k("Enter", "Save", "Save (in the description, Enter types)")
                        .more("Save. In the description Enter types: Tab out of it, then Enter"),
                    k("Ctrl+J", "Save", "Save from anywhere, the description too")
                        .more("Save from anywhere, the description included; so does Ctrl+Enter, on a terminal that tells it from Enter"),
                    k("PgUp / PgDn", "Lines", "Five lines in the description"),
                    k("Space", "Toggle", "Expand Matching; toggle schema match")
                        .more("Expand or collapse Matching; toggle schema match"),
                    k("Esc", "Back", "Back to the list, discarding edits"),
                ],
            },
            Group {
                name: "Delete",
                keys: &[
                    k("Enter (d / D)", "Delete", "Delete"),
                    k("Esc", "Cancel", "Cancel"),
                ],
            },
        ],
    },
    Screen {
        context: Context::FormatPicker,
        title: "Format picker",
        reached: "<kbd>b</kbd> at a table read through a format spec.",
        groups: &[Group {
            name: "Pick",
            keys: &[
                k("(type)", "Narrow", "Narrow to the names that contain it").no_run(),
                k("↑ / ↓", "Move", "Move"),
                k("Enter", "Read", "Read the file again with the spec")
                    .more("Read the file again with the spec chosen. The query, filters and sort are cleared"),
                k("Backspace", "Delete", "Delete a character (Ctrl+W a word, Ctrl+U all)"),
                k("Esc", "Close", "Close and keep the format"),
            ],
        }],
    },
    Screen {
        context: Context::Hex,
        title: "Hex view",
        reached: "`datui --hex FILE`, <kbd>Ctrl</kbd>+<kbd>X</kbd> on the home screen, or <kbd>x</kbd> in the Info panel.",
        groups: &[
            Group {
                name: "Explore",
                keys: &[
                    k("← / → (h/l)", "Byte", "A byte"),
                    k("↑ / ↓ (j/k)", "Row", "A row"),
                    k("w / b", "Group", "The next group of four, or back one"),
                    k("0 / $", "Row ends", "The start or end of the row"),
                    k("g / G", "File ends", "Start or end of the file (Home / End too)")
                        .more("The start or end of the file (Home and End too)"),
                    k("PgUp / PgDn", "Page", "A page (Ctrl+B / F; Ctrl+U / D half)")
                        .more("A page (Ctrl+B/F too; Ctrl+U/D half a page)"),
                    k(":", "Go to", "Go to an offset: 4096, 0x1000, +16, e-8")
                        .more("Go to an offset: 4096 or 0x1000; +16 or -16 from the cursor; e-8 for the eighth byte from the end"),
                ],
            },
            Group {
                name: "Find",
                keys: &[
                    k("f", "Find", "Find text or bytes (de ad, 0x1acf, ?? any)")
                        .more("Find bytes. Text is found as its UTF-8 bytes (Ctrl+U in the prompt: as UTF-16 little-endian). 0x1acffc1d, or two or more hex pairs (de ad be ef), is a byte pattern, where ?? matches any byte. Text in double quotes is text, even when it looks like hex. A match may span rows"),
                    k("n / N", "Next, previous", "Next or previous match, round the end")
                        .more("The next or previous match, round the end of the file"),
                    k("R", "Stride", "Bytes per row from the matches' spacing")
                        .more("When the matches after the one found are all the same distance apart, make that the bytes per row"),
                    k("Esc", "Stop", "Stop a find that is reading"),
                ],
            },
            Group {
                name: "Display",
                keys: &[
                    k("r", "Row size", "Bytes per row; empty: as many as fit")
                        .more("Bytes per row, so that records line up; empty goes back to as many as fit. --hex-width N sets it on the command line"),
                    k("#", "Offsets", "Offsets in decimal or hex"),
                    k("i / Enter", "Inspector", "Show or hide the byte inspector")
                        .more("Show or hide the byte inspector. Beside the bytes when there is room for it and 16 bytes a row, over them when there is not"),
                    k("v", "Mark", "Mark a range from the cursor; again to unmark")
                        .more("Mark from the cursor; move to mark a range, and the status line counts it. v again, or Esc, unmarks"),
                ],
            },
            Group {
                name: "Go",
                keys: &[
                    k("B", "Format spec", "Read the file with a format spec instead"),
                    k("Esc", "Back", "Back to the Info panel or home")
                        .more("Back to the table, when opened from the Info panel; back home, when opened from there"),
                    k("q", "Back", "Home, when opened from there; else quit"),
                ],
            },
        ],
    },
];

/// The screen for `context`.
pub fn screen(context: Context) -> &'static Screen {
    SCREENS
        .iter()
        .find(|s| s.context == context)
        .expect("every context has a screen")
}

/// Every key entry with its screen and group, in reference order; [`GLOBAL`] first.
pub fn entries() -> impl Iterator<Item = (Option<&'static Screen>, &'static Group, &'static Key)> {
    GLOBAL
        .keys
        .iter()
        .map(|key| (None, &GLOBAL, key))
        .chain(SCREENS.iter().flat_map(|screen| {
            screen
                .groups
                .iter()
                .flat_map(move |group| group.keys.iter().map(move |key| (Some(screen), group, key)))
        }))
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

fn markdown_table(out: &mut String, keys: &[Key]) {
    out.push_str("\n| Key | Action |\n|---|---|\n");
    for key in keys {
        out.push_str(&format!(
            "| {} | {} |\n",
            key_cell(key.keys),
            cell(key.long())
        ));
    }
}

/// The keyboard reference: the keys every screen takes, then each screen's.
pub fn render_markdown() -> String {
    let mut out = String::new();
    out.push_str("\n## Every screen\n\nThese keys work on every screen.\n");
    markdown_table(&mut out, GLOBAL.keys);
    out.push_str("\n## Help\n\n<kbd>?</kbd> or <kbd>F1</kbd>.\n");
    markdown_table(&mut out, HELP.keys);
    for screen in SCREENS {
        out.push_str(&format!("\n## {}\n\n{}\n", screen.title, screen.reached));
        for group in screen.groups {
            out.push_str(&format!("\n### {} · {}\n", screen.title, group.name));
            markdown_table(&mut out, group.keys);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_keys_are_read() {
        for (keys, first) in [
            ("↑ / ↓ (j/k)", "↑"),
            ("Ctrl+D / Ctrl+U", "Ctrl+D"),
            ("PgUp/PgDn", "PgUp"),
            ("/", "/"),
            ("+ / - (= / _)", "+"),
            ("e, w, y, o", "e"),
            ("Enter (d / D)", "Enter"),
            ("↑ / ↓, Tab", "↑"),
            ("Click a chip", "Click"),
        ] {
            assert_eq!(first_key(keys), first, "{keys}");
        }
    }

    #[test]
    fn chords_are_read() {
        let c = chord("Ctrl+O").unwrap();
        assert!(c.ctrl && !c.shift && c.code == Code::Char('o'));
        assert_eq!(chord("G").unwrap().code, Code::Char('G'));
        assert!(chord("G").unwrap().shift);
        assert_eq!(chord("Shift+Tab").unwrap().code, Code::BackTab);
        assert_eq!(chord("F1").unwrap().code, Code::F(1));
        assert_eq!(chord("↓").unwrap().code, Code::Down);
        assert_eq!(chord("Space").unwrap().code, Code::Char(' '));
        assert_eq!(chord("+").unwrap().code, Code::Char('+'));
        assert!(chord("Click").is_none());
        assert!(chord("(type)").is_none());
        assert!(chord("1-9").is_none());
        let codes: Vec<Code> = chords("↑ / ↓ (j/k)").iter().map(|c| c.code).collect();
        assert_eq!(
            codes,
            [Code::Up, Code::Down, Code::Char('j'), Code::Char('k')]
        );
        assert_eq!(chords("PgUp/PgDn").len(), 2);
        assert_eq!(chords("/")[0].code, Code::Char('/'));
        assert!(chords("(type)").is_empty());
    }

    /// Every entry's Enter presses a key, unless it says it presses none; every line
    /// is one line, short enough for half the help; every label is three words at most.
    #[test]
    fn every_entry_is_whole() {
        let mut bad = Vec::new();
        for (screen, group, key) in entries() {
            let at = format!(
                "{} · {} · {}",
                screen.map_or("Global", |s| s.title),
                group.name,
                key.keys
            );
            if key.run != Run::Never && key.action().is_none() {
                bad.push(format!("{at}: Enter presses nothing; .run() or .no_run()"));
            }
            if key.line.contains('\n') || key.line.chars().count() > 52 {
                bad.push(format!("{at}: line longer than 52: {}", key.line));
            }
            if key.label.is_empty() || key.label.split(' ').count() > 3 {
                bad.push(format!("{at}: label {:?}", key.label));
            }
            if key.line.ends_with('.') || key.more.is_some_and(|m| m.ends_with('.')) {
                bad.push(format!("{at}: ends with a period"));
            }
            if key.more == Some(key.line) {
                bad.push(format!("{at}: more repeats the line"));
            }
        }
        assert!(bad.is_empty(), "{}", bad.join("\n"));
    }

    #[test]
    fn every_context_has_one_screen() {
        use std::collections::HashSet;
        let contexts: HashSet<_> = SCREENS.iter().map(|s| s.context).collect();
        assert_eq!(contexts.len(), SCREENS.len(), "a context is listed twice");
        for screen in SCREENS {
            assert!(!screen.groups.is_empty(), "{} has no keys", screen.title);
            let mut names = HashSet::new();
            for group in screen.groups {
                assert!(
                    names.insert(group.name),
                    "{}: {} twice",
                    screen.title,
                    group.name
                );
                assert!(!group.keys.is_empty());
            }
        }
    }
}
