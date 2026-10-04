//! Roff for the manpages: text escaped as man(7) wants it, and the Markdown of the
//! docs turned into man macros, so a page and its web page share one source.
//!
//! Prose is one line per paragraph and left to the formatter to fill. Commands and
//! code are unfilled (`.EX`), so they are never broken or hyphenated and paste as
//! written. Every character outside ASCII is a roff escape, so a page reads the same
//! on a terminal that has no UTF-8.

/// The escape for a character outside ASCII: its glyph name where roff has one that
/// groff and mandoc both know, else `\[uXXXX]`.
fn special(c: char) -> String {
    let name = match c {
        '\u{2192}' => "->",
        '\u{2190}' => "<-",
        '\u{2191}' => "ua",
        '\u{2193}' => "da",
        '\u{2014}' => "em",
        '\u{2013}' => "en",
        '\u{00d7}' => "mu",
        '\u{2264}' => "<=",
        '\u{2265}' => ">=",
        '\u{2260}' => "!=",
        '\u{2212}' => "mi",
        '\u{00b7}' => "pc",
        '\u{2018}' => "oq",
        '\u{2019}' => "cq",
        '\u{201c}' => "lq",
        '\u{201d}' => "rq",
        '\u{03bc}' | '\u{00b5}' => "*m",
        '\u{03c1}' => "*r",
        '\u{00b0}' => "de",
        _ => return format!("\\[u{:04X}]", c as u32),
    };
    if name.len() == 2 {
        format!("\\({name}")
    } else {
        format!("\\[{name}]")
    }
}

/// Where a long word may break: written as `\:`, which adds nothing to the text.
const BREAK: char = '\u{E000}';

/// `s` with a break allowed after each `.`, `/` and `,` of a word longer than fits a
/// narrow terminal's line beside an indent: a long path, URL or timestamp.
fn with_breaks(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for (n, word) in s.split(' ').enumerate() {
        if n > 0 {
            out.push(' ');
        }
        if word.chars().count() <= 24 {
            out.push_str(word);
            continue;
        }
        let chars: Vec<char> = word.chars().collect();
        for (i, &c) in chars.iter().enumerate() {
            out.push(c);
            if matches!(c, '.' | '/' | ',') && i + 1 < chars.len() {
                out.push(BREAK);
            }
        }
    }
    out
}

/// Escape one character; `literal` writes every `-` as the minus `\-`, which pastes
/// as the hyphen-minus a shell needs.
fn push_char(out: &mut String, c: char, literal: bool, option_like: bool) {
    match c {
        '\\' => out.push_str("\\e"),
        BREAK => out.push_str("\\:"),
        '-' if literal || option_like => out.push_str("\\-"),
        c if c.is_ascii() => out.push(c),
        c => out.push_str(&special(c)),
    }
}

/// Keep a line from being read as a request: a leading `.` or `'`.
fn guard(line: String) -> String {
    if line.starts_with('.') || line.starts_with('\'') {
        format!("\\&{line}")
    } else {
        line
    }
}

/// Prose, escaped. A `-` that starts a word (`-c`, `--hive`, a lone `-`) is an option
/// and becomes `\-`; a hyphen inside a word stays one.
pub fn text(s: &str) -> String {
    let mut out = String::new();
    let mut prev: Option<char> = None;
    for c in with_breaks(s).chars() {
        let option_like = c == '-'
            && prev.is_none_or(|p| {
                p.is_whitespace() || matches!(p, '(' | '[' | '"' | '\'' | '-' | '=' | '/' | ',')
            });
        push_char(&mut out, c, false, option_like);
        prev = Some(c);
    }
    out
}

/// Something typed: a command, an option, a value. Every `-` is `\-`.
pub fn literal(s: &str) -> String {
    let mut out = String::new();
    for c in s.chars() {
        push_char(&mut out, c, true, false);
    }
    out
}

/// A text line: escaped already, and guarded against reading as a request.
pub fn line(escaped: String) -> String {
    guard(escaped)
}

/// A macro's argument, quoted: `"` inside is `\(dq`.
pub fn arg(escaped: &str) -> String {
    format!("\"{}\"", escaped.replace('"', "\\(dq"))
}

/// Literal text that may break after a `.`, `/` or `,` when it is long, so a long
/// path or timestamp in prose still fits a narrow terminal. `\:` adds nothing to the
/// text, so it copies as written.
fn breakable(code: &str) -> String {
    literal(&with_breaks(code))
}

/// Bold, for literal input.
pub fn bold(s: &str) -> String {
    format!("\\fB{}\\fR", literal(s))
}

/// Italic, for something to replace.
pub fn italic(s: &str) -> String {
    format!("\\fI{}\\fR", literal(s))
}

/// A command shown unfilled and indented: one per line, never broken.
pub fn example_block(out: &mut String, code: &str) {
    out.push_str(".PP\n.RS 4\n.EX\n");
    for l in code.trim_end_matches('\n').lines() {
        out.push_str(&guard(literal(l)));
        out.push('\n');
    }
    out.push_str(".EE\n.RE\n");
}

/// Drop the paragraph breaks a heading already makes: `.PP` right after `.SH` or
/// `.SS`, which mandoc warns about.
pub fn tidy(roff: &str) -> String {
    let mut out = String::with_capacity(roff.len());
    let mut after_heading = false;
    for l in roff.lines() {
        if l == ".PP" && after_heading {
            continue;
        }
        after_heading = l.starts_with(".SH") || l.starts_with(".SS");
        out.push_str(l);
        out.push('\n');
    }
    out
}

/// Markdown inline text as roff: `code` and `<kbd>` bold, `**bold**`, `*italic*`,
/// links as their text (and an absolute link's URL after it), entities and `\|`
/// unescaped.
pub fn inline(md: &str) -> String {
    let md = md
        .replace("<kbd>", "\u{1}")
        .replace("</kbd>", "\u{1}")
        .replace("&lt;", "<")
        .replace("&gt;", ">")
        .replace("&amp;", "&")
        .replace("<br>", " ");
    let chars: Vec<char> = md.chars().collect();
    let mut out = String::new();
    let mut plain = String::new();
    let flush = |plain: &mut String, out: &mut String| {
        if !plain.is_empty() {
            // A word's start is judged within the run of plain text; a run that
            // follows a font change starts a word only if it starts with a space.
            out.push_str(&text(plain));
            plain.clear();
        }
    };
    let mut i = 0;
    while i < chars.len() {
        let c = chars[i];
        match c {
            '\\' if i + 1 < chars.len() && chars[i + 1].is_ascii_punctuation() => {
                plain.push(chars[i + 1]);
                i += 2;
            }
            '`' => {
                // A code span: as many backticks close it as open it.
                let ticks = chars[i..].iter().take_while(|&&c| c == '`').count();
                let start = i + ticks;
                let mut j = start;
                let mut end = None;
                while j < chars.len() {
                    if chars[j] == '`' {
                        let run = chars[j..].iter().take_while(|&&c| c == '`').count();
                        if run == ticks {
                            end = Some(j);
                            break;
                        }
                        j += run;
                    } else {
                        j += 1;
                    }
                }
                match end {
                    Some(end) => {
                        flush(&mut plain, &mut out);
                        let code: String = chars[start..end].iter().collect();
                        let code = code.trim().replace("\\|", "|");
                        out.push_str(&format!("\\fB{}\\fR", breakable(&code)));
                        i = end + ticks;
                    }
                    None => {
                        plain.push(c);
                        i += 1;
                    }
                }
            }
            '\u{1}' => {
                let end = chars[i + 1..].iter().position(|&c| c == '\u{1}');
                match end {
                    Some(n) => {
                        flush(&mut plain, &mut out);
                        let key: String = chars[i + 1..i + 1 + n].iter().collect();
                        out.push_str(&bold(&key));
                        i += n + 2;
                    }
                    None => i += 1,
                }
            }
            '*' | '_' if c == '*' || (i == 0 || !chars[i - 1].is_alphanumeric()) => {
                let strong = chars.get(i + 1) == Some(&c);
                let width = if strong { 2 } else { 1 };
                let start = i + width;
                let close: Vec<char> = std::iter::repeat_n(c, width).collect();
                let end = (start..chars.len().saturating_sub(width - 1)).find(|&j| {
                    chars[j..j + width] == close[..]
                        && j > start
                        && !chars[j - 1].is_whitespace()
                        && (c == '*' || chars.get(j + width).is_none_or(|n| !n.is_alphanumeric()))
                });
                match end {
                    Some(end) if !chars[start].is_whitespace() => {
                        flush(&mut plain, &mut out);
                        let inner: String = chars[start..end].iter().collect();
                        let font = if strong { "B" } else { "I" };
                        out.push_str(&format!("\\f{font}{}\\fR", inline(&inner)));
                        i = end + width;
                    }
                    _ => {
                        plain.push(c);
                        i += 1;
                    }
                }
            }
            '[' => {
                // [text](target)
                let close = chars[i..].iter().position(|&c| c == ']').map(|n| i + n);
                let link = close
                    .filter(|&j| chars.get(j + 1) == Some(&'('))
                    .and_then(|j| {
                        chars[j + 2..]
                            .iter()
                            .position(|&c| c == ')')
                            .map(|n| (j, j + 2 + n))
                    });
                match link {
                    Some((j, k)) => {
                        flush(&mut plain, &mut out);
                        let label: String = chars[i + 1..j].iter().collect();
                        let target: String = chars[j + 2..k].iter().collect();
                        out.push_str(&inline(&label));
                        if target.starts_with("http://") || target.starts_with("https://") {
                            out.push_str(&format!(" <\\fI{}\\fR>", literal(&target)));
                        }
                        i = k + 1;
                    }
                    None => {
                        plain.push(c);
                        i += 1;
                    }
                }
            }
            _ => {
                plain.push(c);
                i += 1;
            }
        }
    }
    flush(&mut plain, &mut out);
    out
}

/// Markdown inline text with its formatting dropped: for a heading.
pub fn plain(md: &str) -> String {
    let md = md.replace(['`', '*'], "").replace("\\|", "|");
    let mut out = String::new();
    let mut rest = md.as_str();
    // Links keep their text.
    while let Some(open) = rest.find('[') {
        out.push_str(&rest[..open]);
        let after = &rest[open + 1..];
        match after
            .find("](")
            .and_then(|c| after[c..].find(')').map(|e| (c, c + e)))
        {
            Some((c, e)) => {
                out.push_str(&after[..c]);
                rest = &after[e + 1..];
            }
            None => {
                out.push('[');
                rest = after;
            }
        }
    }
    out.push_str(rest);
    out
}

/// How a page's Markdown headings map to man's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Headings {
    /// `##` is a section (`.SH`, upper case), `###` a subsection.
    Sections,
    /// `##` is a subsection, `###` a bold line: the Markdown sits inside a section.
    Subsections,
}

/// Turns a Markdown page into roff.
pub struct Markdown<'a> {
    pub headings: Headings,
    /// What a `dataset=NAME` code block runs on: a line saying so goes before it.
    pub dataset_label: Option<&'a dyn Fn(&str) -> String>,
}

/// A table row's cells, `\|` kept as part of a cell.
fn cells(row: &str) -> Vec<String> {
    let row = row.trim().trim_start_matches('|');
    let row = row.strip_suffix('|').unwrap_or(row);
    let mut out = vec![String::new()];
    let mut chars = row.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '\\' if chars.peek() == Some(&'|') => {
                out.last_mut().unwrap().push_str("\\|");
                chars.next();
            }
            '|' => out.push(String::new()),
            c => out.last_mut().unwrap().push(c),
        }
    }
    out.into_iter().map(|c| c.trim().to_string()).collect()
}

/// A header that says the column is the row's description.
fn describes(header: &str) -> bool {
    matches!(
        plain(header).as_str(),
        "Description"
            | "Means"
            | "What it means"
            | "What it does"
            | "What it says"
            | "Role"
            | "Does"
            | "Read as"
            | "Action"
    )
}

impl Markdown<'_> {
    /// The whole page; its `#` title and generated-region comments left out.
    pub fn render(&self, md: &str) -> String {
        let md = md.replace("\r\n", "\n");
        let lines: Vec<&str> = md.lines().collect();
        let mut out = String::new();
        let mut i = 0;
        while i < lines.len() {
            let l = lines[i];
            let t = l.trim();
            if t.is_empty() || t.starts_with("<!--") || (t.starts_with("# ") && !l.starts_with(' '))
            {
                i += 1;
                continue;
            }
            if let Some(info) = t.strip_prefix("```") {
                let fence: String = t.chars().take_while(|&c| c == '`').collect();
                let mut body = Vec::new();
                i += 1;
                while i < lines.len() && !lines[i].trim().starts_with(fence.as_str()) {
                    body.push(lines[i]);
                    i += 1;
                }
                i += 1;
                let dataset = info
                    .split(',')
                    .find_map(|a| a.trim().strip_prefix("dataset="));
                if let (Some(name), Some(label)) = (dataset, self.dataset_label) {
                    out.push_str(&format!(".PP\n{}\n", line(label(name))));
                }
                let indent = body
                    .iter()
                    .filter(|b| !b.trim().is_empty())
                    .map(|b| b.len() - b.trim_start().len())
                    .min()
                    .unwrap_or(0);
                let code: Vec<&str> = body.iter().map(|b| b.get(indent..).unwrap_or("")).collect();
                example_block(&mut out, &code.join("\n"));
                continue;
            }
            if let Some(heading) = t.strip_prefix("## ") {
                match self.headings {
                    Headings::Sections => out.push_str(&format!(
                        ".SH {}\n",
                        arg(&text(&plain(heading).to_uppercase()))
                    )),
                    Headings::Subsections => {
                        out.push_str(&format!(".SS {}\n", arg(&text(&plain(heading)))))
                    }
                }
                i += 1;
                continue;
            }
            if let Some(heading) = t.strip_prefix("### ").or_else(|| t.strip_prefix("#### ")) {
                match self.headings {
                    Headings::Sections => {
                        out.push_str(&format!(".SS {}\n", arg(&text(&plain(heading)))))
                    }
                    Headings::Subsections => out.push_str(&format!(
                        ".PP\n{}\n",
                        line(format!("\\fB{}\\fR", text(&plain(heading))))
                    )),
                }
                i += 1;
                continue;
            }
            if t.starts_with('|') {
                let mut rows = Vec::new();
                while i < lines.len() && lines[i].trim().starts_with('|') {
                    rows.push(cells(lines[i]));
                    i += 1;
                }
                self.table(&mut out, &rows);
                continue;
            }
            let item = t
                .strip_prefix("- ")
                .or_else(|| t.strip_prefix("* "))
                .map(|rest| (None, rest))
                .or_else(|| {
                    let (n, rest) = t.split_once(". ")?;
                    n.parse::<u32>().ok().map(|n| (Some(n), rest))
                });
            if let Some((number, first)) = item {
                let mut body = vec![first.to_string()];
                i += 1;
                while i < lines.len()
                    && !lines[i].trim().is_empty()
                    && lines[i].starts_with(' ')
                    && !lines[i].trim().starts_with("- ")
                {
                    body.push(lines[i].trim().to_string());
                    i += 1;
                }
                let tag = number.map_or("\\(bu".to_string(), |n| format!("{n}."));
                out.push_str(&format!(".IP {tag} 4\n{}\n", line(inline(&body.join(" ")))));
                continue;
            }
            // A paragraph: every line up to a blank one or a block.
            let mut body = Vec::new();
            while i < lines.len() {
                let t = lines[i].trim();
                if t.is_empty()
                    || t.starts_with("```")
                    || t.starts_with('#')
                    || t.starts_with('|')
                    || t.starts_with("- ")
                    || t.starts_with("<!--")
                {
                    break;
                }
                body.push(t);
                i += 1;
            }
            out.push_str(&format!(".PP\n{}\n", line(inline(&body.join(" ")))));
        }
        out
    }

    /// A table as a list: the first cell is the tag, the description column the
    /// text, and any other column a labeled line under it.
    fn table(&self, out: &mut String, rows: &[Vec<String>]) {
        let Some(header) = rows.first() else {
            return;
        };
        let body = rows
            .iter()
            .skip(1)
            .filter(|r| !r.iter().all(|c| c.chars().all(|c| matches!(c, '-' | ':'))));
        let description = if header.len() == 2 {
            Some(1)
        } else {
            header.iter().position(|h| describes(h))
        };
        for row in body {
            let tag = row.first().map(String::as_str).unwrap_or("");
            out.push_str(&format!(".TP\n{}\n", line(inline(tag))));
            let mut first = true;
            if let Some(d) = description
                && let Some(cell) = row.get(d)
                && !cell.is_empty()
            {
                out.push_str(&line(inline(cell)));
                out.push('\n');
                first = false;
            }
            for (n, cell) in row.iter().enumerate().skip(1) {
                if Some(n) == description || cell.is_empty() || cell == "\u{2014}" {
                    continue;
                }
                if !first {
                    out.push_str(".br\n");
                }
                let label = header.get(n).map(|h| plain(h)).unwrap_or_default();
                out.push_str(&line(format!("{}: {}", text(&label), inline(cell))));
                out.push('\n');
                first = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn options_are_minus_signs_and_hyphens_stay() {
        assert_eq!(
            text("--hive and -c, comma-separated"),
            "\\-\\-hive and \\-c, comma-separated"
        );
        assert_eq!(literal("a-b"), "a\\-b");
    }

    #[test]
    fn unicode_is_escaped() {
        assert_eq!(text("↑ → é"), "\\(ua \\(-> \\[u00E9]");
        assert!(inline("a — b").is_ascii());
    }

    #[test]
    fn inline_markdown() {
        assert_eq!(
            inline("`--format` and **bold**"),
            "\\fB\\-\\-format\\fR and \\fBbold\\fR"
        );
        assert_eq!(inline("see [Query](query.md)"), "see Query");
        assert_eq!(
            inline("<kbd>Enter</kbd> or *X*"),
            "\\fBEnter\\fR or \\fIX\\fR"
        );
        assert_eq!(inline("a \\| b, `x \\| y`"), "a | b, \\fBx | y\\fR");
        assert_eq!(inline("snake_case_name"), "snake_case_name");
    }

    #[test]
    fn a_line_never_starts_a_request() {
        assert_eq!(line(".hidden".into()), "\\&.hidden");
    }

    #[test]
    fn blocks() {
        let md = Markdown {
            headings: Headings::Sections,
            dataset_label: None,
        };
        let roff = md.render(
            "# Title\n\nPara one\nline two.\n\n## A `b` c\n\n```bash\ndatui -x\n```\n\n| K | V |\n|---|---|\n| `a` | b |\n\n- item\n  more\n",
        );
        assert_eq!(
            roff,
            ".PP\nPara one line two.\n.SH \"A B C\"\n.PP\n.RS 4\n.EX\ndatui \\-x\n.EE\n.RE\n.TP\n\\fBa\\fR\nb\n.IP \\(bu 4\nitem more\n"
        );
    }
}
