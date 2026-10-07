//! Help writing SQL in the query prompt: the word being typed, the columns it
//! could name, and Tab completing it.
//!
//! Everything here works on text and a schema already in hand; nothing reads
//! data.

use polars::prelude::DataType;

/// The table name SQL runs against.
pub const TABLE: &str = "df";

/// Words that must be quoted to name a column, however they are cased: those
/// Polars SQL will not read as a bare column name somewhere in a statement.
const RESERVED: &[&str] = &[
    "&[",
    "&str]",
    "=",
    "all",
    "and",
    "as",
    "asc",
    "between",
    "by",
    "case",
    "cross",
    "cube",
    "current_date",
    "current_time",
    "current_timestamp",
    "current_user",
    "desc",
    "distinct",
    "else",
    "end",
    "except",
    "exclude",
    "exists",
    "false",
    "fetch",
    "from",
    "full",
    "group",
    "having",
    "in",
    "inner",
    "intersect",
    "interval",
    "into",
    "is",
    "join",
    "lateral",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "minus",
    "natural",
    "not",
    "null",
    "offset",
    "on",
    "or",
    "order",
    "outer",
    "returning",
    "right",
    "rollup",
    "select",
    "session_user",
    "struct",
    "then",
    "top",
    "true",
    "union",
    "user",
    "using",
    "values",
    "when",
    "where",
    "with",
];

/// The word before the cursor that a completion would replace.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Word {
    /// What to match names against: the word without an opening quote.
    pub text: String,
    /// Characters before the cursor that a completion replaces, the opening
    /// quote of a quoted name included.
    pub span: usize,
}

/// The word being typed at `col` (a character index) on `line`. `None` inside
/// a string literal, where a column name would be the wrong thing to offer.
pub fn word_before(line: &str, col: usize) -> Option<Word> {
    let before: Vec<char> = line.chars().take(col).collect();
    // Each kind of quote is literal inside the other: `"O'Brien"`, `'say "hi"'`.
    let mut in_string = false;
    let mut open_name = None;
    for (i, &c) in before.iter().enumerate() {
        match c {
            '\'' if open_name.is_none() => in_string = !in_string,
            '"' if !in_string => open_name = if open_name.is_some() { None } else { Some(i) },
            _ => {}
        }
    }
    if in_string {
        return None;
    }
    if let Some(open) = open_name {
        return Some(Word {
            text: before[open + 1..].iter().collect(),
            span: before.len() - open,
        });
    }
    let start = before
        .iter()
        .rposition(|&c| !is_name_char(c))
        .map_or(0, |i| i + 1);
    Some(Word {
        text: before[start..].iter().collect(),
        span: before.len() - start,
    })
}

fn is_name_char(c: char) -> bool {
    c.is_alphanumeric() || c == '_'
}

/// How SQL spells a column: bare when it can be, else in double quotes.
pub fn sql_name(name: &str) -> String {
    let mut chars = name.chars();
    let plain = chars.next().is_some_and(|c| c.is_alphabetic() || c == '_')
        && chars.all(is_name_char)
        && !RESERVED.contains(&name.to_ascii_lowercase().as_str());
    if plain {
        name.to_string()
    } else {
        format!("\"{}\"", name.replace('"', "\"\""))
    }
}

/// The columns `word` could name, in schema order: those starting with it,
/// or when none do, those containing it. Case is ignored. An empty word
/// matches every column.
pub fn matching<'a>(columns: &'a [(String, DataType)], word: &str) -> Vec<&'a (String, DataType)> {
    let word = word.to_lowercase();
    let starts: Vec<_> = columns
        .iter()
        .filter(|(name, _)| name.to_lowercase().starts_with(&word))
        .collect();
    if !starts.is_empty() || word.is_empty() {
        return starts;
    }
    columns
        .iter()
        .filter(|(name, _)| name.to_lowercase().contains(&word))
        .collect()
}

/// What Tab can put in place of `word`: the matching columns, spelled as SQL
/// wants them, and the table name.
pub fn completions(columns: &[(String, DataType)], word: &str) -> Vec<String> {
    let mut out: Vec<String> = matching(columns, word)
        .into_iter()
        .map(|(name, _)| sql_name(name))
        .collect();
    if TABLE.starts_with(&word.to_lowercase()) && !out.iter().any(|c| c == TABLE) {
        out.push(TABLE.to_string());
    }
    out
}

/// A completion in progress: repeated Tabs step through the candidates while
/// nothing else has changed the text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cycle {
    candidates: Vec<String>,
    index: usize,
    /// The value and cursor the last step left, so any other edit ends the
    /// cycle.
    value: String,
    cursor: usize,
}

/// What one Tab does to the text: replace `span` characters before the
/// cursor with `insert`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Step {
    pub span: usize,
    pub insert: String,
}

/// The step one Tab takes, given the text around the cursor and the cycle the
/// last Tab left. `None` when there is nothing to complete.
pub fn tab(
    columns: &[(String, DataType)],
    line: &str,
    col: usize,
    value: &str,
    cursor: usize,
    cycle: &mut Option<Cycle>,
) -> Option<Step> {
    let word = || word_before(line, col);
    let candidates = |word: &str| completions(columns, word);
    tab_with(word, candidates, value, cursor, cycle)
}

/// The q word being typed at `col` (a character index) on `line`. `None` inside a
/// string: in q, `"` opens one, and a name with spaces is spelled `col["..."]`.
pub fn q_word_before(line: &str, col: usize) -> Option<Word> {
    let before: Vec<char> = line.chars().take(col).collect();
    if before.iter().filter(|&&c| c == '"').count() % 2 == 1 {
        return None;
    }
    let start = before
        .iter()
        .rposition(|&c| !is_name_char(c))
        .map_or(0, |i| i + 1);
    Some(Word {
        text: before[start..].iter().collect(),
        span: before.len() - start,
    })
}

/// [`tab`] for q: the columns spelled as q reads them, and no table name.
pub fn q_tab(
    columns: &[(String, DataType)],
    line: &str,
    col: usize,
    value: &str,
    cursor: usize,
    cycle: &mut Option<Cycle>,
) -> Option<Step> {
    let word = || q_word_before(line, col);
    let candidates = |word: &str| {
        matching(columns, word)
            .into_iter()
            .map(|(name, _)| crate::query::q_name(name))
            .collect()
    };
    tab_with(word, candidates, value, cursor, cycle)
}

fn tab_with(
    word: impl FnOnce() -> Option<Word>,
    completions: impl FnOnce(&str) -> Vec<String>,
    value: &str,
    cursor: usize,
    cycle: &mut Option<Cycle>,
) -> Option<Step> {
    if let Some(c) = cycle.as_mut()
        && c.value == value
        && c.cursor == cursor
    {
        let span = c.candidates[c.index].chars().count();
        c.index = (c.index + 1) % c.candidates.len();
        return Some(Step {
            span,
            insert: c.candidates[c.index].clone(),
        });
    }
    *cycle = None;
    let word = word()?;
    if word.span == 0 {
        return None;
    }
    let candidates = completions(&word.text);
    let first = candidates.first()?.clone();
    if candidates.len() == 1 {
        return Some(Step {
            span: word.span,
            insert: first,
        });
    }
    let common = common_prefix(&candidates);
    if common.chars().count() > word.span {
        return Some(Step {
            span: word.span,
            insert: common,
        });
    }
    *cycle = Some(Cycle {
        candidates,
        index: 0,
        value: String::new(),
        cursor: 0,
    });
    Some(Step {
        span: word.span,
        insert: first,
    })
}

/// Record where a step left the text, so the next Tab continues the cycle.
pub fn landed(cycle: &mut Option<Cycle>, value: &str, cursor: usize) {
    if let Some(c) = cycle.as_mut() {
        c.value = value.to_string();
        c.cursor = cursor;
    }
}

fn common_prefix(candidates: &[String]) -> String {
    let mut prefix: Vec<char> = candidates[0].chars().collect();
    for c in &candidates[1..] {
        let n = prefix
            .iter()
            .zip(c.chars())
            .take_while(|(a, b)| *a == b)
            .count();
        prefix.truncate(n);
    }
    prefix.into_iter().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cols(names: &[&str]) -> Vec<(String, DataType)> {
        names
            .iter()
            .map(|n| (n.to_string(), DataType::String))
            .collect()
    }

    #[test]
    fn the_word_is_the_name_before_the_cursor() {
        let w = word_before("SELECT tpep_pi", 14).unwrap();
        assert_eq!((w.text.as_str(), w.span), ("tpep_pi", 7));
        let w = word_before("SELECT a, ", 10).unwrap();
        assert_eq!((w.text.as_str(), w.span), ("", 0));
        // A quoted name being typed includes its spaces and its quote.
        let w = word_before("SELECT \"Team ", 13).unwrap();
        assert_eq!((w.text.as_str(), w.span), ("Team ", 6));
        // Inside a string literal nothing is a column.
        assert_eq!(word_before("WHERE carrier = 'A", 18), None);
        // A quote of the other kind is part of the name or the string.
        let line = "WHERE \"O'Brien\" = 'say \"x' AND na";
        let w = word_before(line, line.chars().count()).unwrap();
        assert_eq!((w.text.as_str(), w.span), ("na", 2));
        let w = word_before("SELECT \"O'Br", 12).unwrap();
        assert_eq!((w.text.as_str(), w.span), ("O'Br", 5));
    }

    #[test]
    fn names_are_quoted_only_when_sql_needs_it() {
        assert_eq!(sql_name("dep_delay"), "dep_delay");
        assert_eq!(sql_name("Team 1"), "\"Team 1\"");
        assert_eq!(sql_name("order"), "\"order\"");
        assert_eq!(sql_name("User"), "\"User\"");
        assert_eq!(sql_name("values"), "\"values\"");
        assert_eq!(sql_name("1st"), "\"1st\"");
        assert_eq!(sql_name("say \"hi\""), "\"say \"\"hi\"\"\"");
    }

    #[test]
    fn prefix_matches_win_over_substring_matches() {
        let c = cols(&["dep_delay", "arr_delay", "dest"]);
        let names = |w| {
            matching(&c, w)
                .into_iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>()
        };
        assert_eq!(names("de"), ["dep_delay", "dest"]);
        assert_eq!(names("DELAY"), ["dep_delay", "arr_delay"]);
        assert_eq!(names(""), ["dep_delay", "arr_delay", "dest"]);
    }

    #[test]
    fn tab_completes_extends_then_cycles() {
        let c = cols(&["Round", "Date", "Team 1", "Team 2"]);
        let mut cycle = None;
        // One candidate: completed outright.
        let step = tab(&c, "SELECT Ro", 9, "SELECT Ro", 9, &mut cycle).unwrap();
        assert_eq!(step.insert, "Round");
        assert_eq!(step.span, 2);
        // Two: extended to what they share, quote and all.
        let step = tab(&c, "SELECT te", 9, "SELECT te", 9, &mut cycle).unwrap();
        assert_eq!(step.insert, "\"Team ");
        // Nothing more in common: the candidates in turn.
        let line = "SELECT \"Team ";
        let step = tab(&c, line, 13, line, 13, &mut cycle).unwrap();
        assert_eq!((step.span, step.insert.as_str()), (6, "\"Team 1\""));
        landed(&mut cycle, "SELECT \"Team 1\"", 15);
        let step = tab(&c, "", 0, "SELECT \"Team 1\"", 15, &mut cycle).unwrap();
        assert_eq!((step.span, step.insert.as_str()), (8, "\"Team 2\""));
        // The table name completes too, after the columns.
        let mut cycle = None;
        let step = tab(&c, "FROM d", 6, "FROM d", 6, &mut cycle).unwrap();
        assert_eq!(step.insert, "Date");
        landed(&mut cycle, "FROM Date", 9);
        let step = tab(&c, "", 0, "FROM Date", 9, &mut cycle).unwrap();
        assert_eq!((step.span, step.insert.as_str()), (4, "df"));
        // Any other edit ends the cycle.
        let step = tab(&c, "FROM Dat", 8, "FROM Dat", 8, &mut cycle).unwrap();
        assert_eq!(step.insert, "Date");
        // Nothing typed, nothing to do.
        assert_eq!(tab(&c, "SELECT ", 7, "SELECT ", 7, &mut None), None);
    }
}
