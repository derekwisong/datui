//! A manpage as plain text, for `datui man` where there is no `man` to show it
//! (Windows, a minimal container). Reads the subset of man(7) the pages are written
//! in ([`super::roff`]): headings, paragraphs, tagged paragraphs, bullets, indents and
//! unfilled examples.

/// The text of roff `escaped`: fonts dropped, glyph escapes as their characters.
fn unescape(escaped: &str) -> String {
    let mut out = String::with_capacity(escaped.len());
    let mut chars = escaped.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            // \fB, \fI, \fR, \fP: a font change.
            Some('f') => {
                chars.next();
            }
            Some('e') => out.push('\\'),
            Some('-') => out.push('-'),
            Some(':' | '&') => {}
            Some('(') => {
                let name: String = chars.by_ref().take(2).collect();
                out.push_str(glyph(&name));
            }
            Some('[') => {
                let name: String = chars.by_ref().take_while(|&c| c != ']').collect();
                match name
                    .strip_prefix('u')
                    .and_then(|hex| u32::from_str_radix(hex, 16).ok())
                    .and_then(char::from_u32)
                {
                    Some(c) => out.push(c),
                    None => out.push_str(glyph(&name)),
                }
            }
            Some(other) => out.push(other),
            None => {}
        }
    }
    out
}

/// The character a roff glyph name stands for.
fn glyph(name: &str) -> &'static str {
    match name {
        "->" => "→",
        "<-" => "←",
        "ua" => "↑",
        "da" => "↓",
        "em" => "—",
        "en" => "–",
        "mu" => "×",
        "<=" => "≤",
        ">=" => "≥",
        "!=" => "≠",
        "mi" => "-",
        "pc" => "·",
        "oq" | "cq" => "'",
        "lq" | "rq" | "dq" => "\"",
        "*m" => "µ",
        "*r" => "ρ",
        "de" => "°",
        "bu" => "•",
        "co" => "©",
        _ => "?",
    }
}

/// A macro's arguments: words, or quoted strings.
fn args(rest: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut chars = rest.trim().chars().peekable();
    while let Some(&c) = chars.peek() {
        if c == ' ' {
            chars.next();
            continue;
        }
        let mut word = String::new();
        if c == '"' {
            chars.next();
            for c in chars.by_ref() {
                if c == '"' {
                    break;
                }
                word.push(c);
            }
        } else {
            while let Some(&c) = chars.peek() {
                if c == ' ' {
                    break;
                }
                word.push(c);
                chars.next();
            }
        }
        out.push(word);
    }
    out
}

/// Lays out filled text at an indent.
struct Writer {
    out: String,
    width: usize,
    /// The words of the paragraph being filled.
    words: Vec<String>,
    /// Where filled lines start, and where the paragraph's first line starts.
    indent: usize,
    first: Option<usize>,
}

impl Writer {
    fn flush(&mut self) {
        if self.words.is_empty() {
            return;
        }
        let mut col = self.first.take().unwrap_or(self.indent);
        let mut line = " ".repeat(col);
        let mut fresh = true;
        for word in self.words.drain(..) {
            let len = word.chars().count();
            if !fresh && col + 1 + len > self.width {
                self.out.push_str(line.trim_end());
                self.out.push('\n');
                line = " ".repeat(self.indent);
                col = self.indent;
                fresh = true;
            }
            if !fresh {
                line.push(' ');
                col += 1;
            }
            line.push_str(&word);
            col += len;
            fresh = false;
        }
        self.out.push_str(line.trim_end());
        self.out.push('\n');
    }

    fn blank(&mut self) {
        self.flush();
        if !self.out.is_empty() && !self.out.ends_with("\n\n") {
            self.out.push('\n');
        }
    }
}

/// `roff`, one of the committed pages, as text filled to `width` columns.
pub fn render(roff: &str, width: usize) -> String {
    const BODY: usize = 7;
    let mut w = Writer {
        out: String::new(),
        width: width.max(40),
        words: Vec::new(),
        indent: BODY,
        first: None,
    };
    // `.RS` steps, outermost first, on top of the body's indent.
    let mut shifts: Vec<usize> = Vec::new();
    let base = |shifts: &[usize]| BODY + shifts.iter().sum::<usize>();
    let mut fill = true;
    // A `.TP`'s next line is its tag.
    let mut tag_next = false;
    for raw in roff.lines() {
        if let Some(request) = raw.strip_prefix('.') {
            let (name, rest) = request.split_once(' ').unwrap_or((request, ""));
            match name {
                "TH" => {
                    let a = args(rest);
                    if let (Some(title), Some(section)) = (a.first(), a.get(1)) {
                        w.out
                            .push_str(&format!("{}({})\n", unescape(title), unescape(section)));
                    }
                }
                "SH" => {
                    w.blank();
                    shifts.clear();
                    w.out.push_str(&unescape(&args(rest).join(" ")));
                    w.out.push('\n');
                    w.indent = BODY;
                }
                "SS" => {
                    w.blank();
                    shifts.clear();
                    w.out.push_str("   ");
                    w.out.push_str(&unescape(&args(rest).join(" ")));
                    w.out.push('\n');
                    w.indent = BODY;
                }
                "PP" | "LP" | "P" => {
                    w.blank();
                    w.indent = base(&shifts);
                }
                "TP" => {
                    w.blank();
                    w.indent = base(&shifts);
                    tag_next = true;
                }
                "IP" => {
                    w.blank();
                    let a = args(rest);
                    let step: usize = a.get(1).and_then(|n| n.parse().ok()).unwrap_or(4);
                    let at = base(&shifts);
                    w.indent = at + step;
                    if let Some(mark) = a.first().filter(|m| !m.is_empty()) {
                        let mark = unescape(mark);
                        let pad = step.saturating_sub(mark.chars().count()).max(1);
                        w.first = Some(at);
                        w.words.push(format!("{mark}{}", " ".repeat(pad - 1)));
                    }
                }
                "RS" => {
                    w.flush();
                    shifts.push(rest.trim().parse().unwrap_or(4));
                    w.indent = base(&shifts);
                }
                "RE" => {
                    w.flush();
                    shifts.pop();
                    w.indent = base(&shifts);
                }
                "nf" | "EX" => {
                    w.flush();
                    fill = false;
                }
                "fi" | "EE" => fill = true,
                "br" => w.flush(),
                // Comments, registers, strings, blank requests.
                _ => {}
            }
            continue;
        }
        let text = unescape(raw);
        if tag_next {
            tag_next = false;
            w.out.push_str(&" ".repeat(w.indent));
            w.out.push_str(&text);
            w.out.push('\n');
            w.indent += 4;
            continue;
        }
        if !fill {
            w.out.push_str(&" ".repeat(w.indent));
            w.out.push_str(&text);
            w.out.push('\n');
            continue;
        }
        w.words.extend(text.split_whitespace().map(str::to_string));
    }
    w.flush();
    w.out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_page_reads_as_text() {
        let roff = ".\\\" comment\n.TH DATUI\\-KEYS 7 2026-01-01 \"datui\" \"Misc\"\n.nr HY 0\n.SH NAME\ndatui\\-keys \\- the keys\n.SH KEYS\n.SS \"Every screen\"\n.TP\n\\fBCtrl+O\\fR\nThe home screen \\(em from anywhere, \\(ua and \\(da too.\n.IP \\(bu 4\nA bullet\n.PP\n.RS 4\n.EX\ndatui \\-\\-hex FILE\n.EE\n.RE\n";
        let text = render(roff, 80);
        assert_eq!(
            text,
            "DATUI-KEYS(7)\n\nNAME\n       datui-keys - the keys\n\nKEYS\n\n   Every screen\n\n       Ctrl+O\n           The home screen — from anywhere, ↑ and ↓ too.\n\n       •   A bullet\n\n           datui --hex FILE\n"
        );
    }

    #[test]
    fn long_paragraphs_fill_to_the_width() {
        let roff =
            ".SH DESCRIPTION\none two three four five six seven eight nine ten eleven twelve\n";
        let text = render(roff, 40);
        assert!(text.lines().all(|l| l.chars().count() <= 40), "{text}");
        assert!(text.contains("       one two"), "{text}");
    }

    /// Every committed page renders with nothing left of its markup.
    #[test]
    fn every_page_is_clean_text() {
        for page in super::super::PAGES {
            let text = page.plain(80);
            for marker in ["\\f", "\\(", ".TP", ".SH", ".RS", "\\-"] {
                assert!(!text.contains(marker), "{}: {marker}", page.file_name());
            }
            assert!(
                text.starts_with(&page.name.to_uppercase()),
                "{}",
                page.file_name()
            );
        }
    }
}
