//! Terminal glyphs, with an ASCII fallback for terminals not doing UTF-8 (SSH, a C
//! locale), where Unicode would render as replacement boxes.
//!
//! No Nerd Font glyphs. Every Unicode character passes the font-coverage audit
//! (`scripts/code/audit_glyphs.py`): present in JetBrainsMono Nerd Font, and never an
//! `Emoji=Yes, Emoji_Presentation=No` codepoint missing from Liberation Mono or Noto Sans
//! Mono (those fall back to the color-emoji font and draw blank or clipped). Nerd Font
//! icons appear only in the Omarchy menu and in users' own `[glyphs]` overrides.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::symbols::{Marker, line};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::sync::OnceLock;
use unicode_width::UnicodeWidthStr;

/// Symbols used by the UI, in whichever alphabet the terminal can render.
#[derive(Debug, Clone, Copy)]
pub struct Glyphs {
    /// Whether this is the Unicode set: the sets are consts, so no address identifies them.
    pub unicode: bool,
    /// Marks the selected row — the one thing that must be findable instantly.
    pub selector: &'static str,
    /// Non-selected row indent; must be the same display width as `selector`.
    pub selector_blank: &'static str,
    /// Text cursor in an input.
    pub cursor: &'static str,
    /// Prompt marker.
    pub prompt: &'static str,
    /// Vertical rule between panes.
    pub rule: &'static str,
    /// The frozen-columns separator while too narrow for every frozen column (the rest
    /// scroll after it). One column wide, like `rule`, and visibly different.
    pub rule_broken: &'static str,
    /// Truncation marker.
    pub ellipsis: &'static str,
    /// The home screen's row up a level.
    pub up: &'static str,
    /// Between row and column counts: `2.4M × 18`.
    pub times: &'static str,
    /// Section collapse markers; both must be the same display width.
    pub collapsed: &'static str,
    pub expanded: &'static str,
    /// Keycap names for the footer. Named keys are spelled out there, matching
    /// the rest of datui, so only the arrows need a fallback.
    pub updown: &'static str,
    /// Left/right pair, for the fold hint.
    pub updown_lr: &'static str,
    /// Ctrl plus the up/down pair, for the section-jump chip.
    pub ctrl_updown: &'static str,
    /// Separator between facts in a status line: `listing · nfs`.
    pub middot: &'static str,
    /// A fact that is not there: a size no footer stated, a format nothing named.
    /// Also joins a note's summary to its scope.
    pub dash: &'static str,
    /// The coefficient of determination, in the regression fit line.
    pub r_squared: &'static str,
    /// Spearman's rank correlation, in the correlation matrix.
    pub rho: &'static str,
    /// Spinner frames, cycled while something is loading. Every frame must be the
    /// same display width, or the text beside it jitters.
    pub spinner: &'static [&'static str],
    /// Where a row's data lives, beside its name (on an ultrawide the details pane is too
    /// far from the row). All five must share a display width, or names shift.
    pub here: &'static str,
    pub in_memory: &'static str,
    pub over_network: &'static str,
    pub in_object_store: &'static str,
    pub place_unknown: &'static str,
    /// A null cell. Blank is what a null used to be, and blank is also what an empty
    /// string is, so the two were indistinguishable.
    pub null: &'static str,
    /// A cell whose file was written without this column (not a null in the data). One
    /// column wide, like `null`.
    pub absent: &'static str,
    /// A cell whose file stores the column in a type the column cannot hold, so it was
    /// not read from that file. A value is there; it is not this type.
    pub conflict: &'static str,
    /// After a column's name in the header: this column is not in every file, or the
    /// files disagree on its type. A footnote mark, and the Info panel is the note.
    pub drift_mark: &'static str,
    /// After a sorted column's name in the header, giving the direction. One column wide in
    /// both sets (header arithmetic counts it like `drift_mark`); triangles, since arrows
    /// mean off-screen columns in that row.
    pub sort_asc: &'static str,
    pub sort_desc: &'static str,
    /// The rail down the left edge of the row the cursor is on.
    pub rail: &'static str,
    /// Rule drawn beside a section title: resting, and under the cursor.
    pub rule_h: &'static str,
    pub rule_h_focused: &'static str,
    /// Arrows for the off-screen column hints in the table header.
    pub arrow_left: &'static str,
    pub arrow_right: &'static str,
    /// Between the steps of a location trail: `cloud › Azure › datui-test`.
    pub trail: &'static str,
    /// Either side of a choice shown alone because its values do not fit on its
    /// row: `‹ TSV ›`, which ←/→ step. One column wide in both sets.
    pub choice_prev: &'static str,
    pub choice_next: &'static str,
    /// After a column's name in a column list: hidden from the table. One column
    /// wide in both sets, like the header marks.
    pub hidden_mark: &'static str,
    /// After a field in the inspector's Compare column: the two rows' values
    /// differ. One column wide in both sets.
    pub diff_mark: &'static str,
    /// Checkbox states, for toggle lists.
    pub checkbox_on: &'static str,
    pub checkbox_off: &'static str,
    /// Radio button states, for pick-one lists.
    pub radio_on: &'static str,
    pub radio_off: &'static str,
    /// Single-cell state dots: all, some, none. One column wide in both sets.
    pub dot_full: &'static str,
    pub dot_half: &'static str,
    pub dot_empty: &'static str,
    /// Five ascending levels for a score shown as one character.
    pub score_marks: &'static [&'static str; 5],
    /// A confirmation mark.
    pub check: &'static str,
    /// A caution mark.
    pub warning: &'static str,
    /// Scrollbar thumb, drawn down the right edge of an overlay.
    pub scroll_thumb: &'static str,
    /// The scrollbar's track, above and below the thumb: a lighter shade of it.
    pub scroll_track: &'static str,
    /// Stands in for a value that is bytes, not text.
    pub binary_stub: &'static str,
    /// Marks for a line break, tab or other control character in a one-line preview (a cell
    /// cannot draw them); one column wide in both sets. The inspector shows them as is.
    pub newline_mark: &'static str,
    pub tab_mark: &'static str,
    pub control_mark: &'static str,
    /// A byte in the hex view's ASCII gutter that is not printable. One column wide
    /// in both sets.
    pub hex_dot: &'static str,
    /// Eight compact levels for inline charts (lowest to highest).
    pub mini_bars: &'static [&'static str; 8],
    /// In a line of `mini_bars`, a place with nothing measured: segments a sample
    /// drew no row from. One column wide in both sets, and unlike every bar level.
    pub unsampled: &'static str,
    /// Under a line of bars, the one selected. One column wide in both sets.
    pub pointer: &'static str,
    /// A horizontal bar's end, one to eight eighths of a cell filled from the left;
    /// the last is a whole cell, the bar's body. One column wide in both sets.
    pub bar_eighths: &'static [&'static str; 8],
    /// The home-screen wordmark, three rows of box drawing. `None` when the terminal
    /// cannot draw it, and the one-line title bar is used instead.
    pub wordmark: Option<&'static [&'static str]>,
    /// The one border every Surface draws. Not a `[glyphs]` override slot:
    /// its eight pieces must agree with each other, and ratatui draws them.
    pub border: ratatui::symbols::border::Set<'static>,
    /// What the plots draw with. Not a `[glyphs]` override slot: ratatui draws
    /// these, and the Unicode marks are whole blocks of braille and eighths.
    pub plot: PlotMarks,
}

/// The marks ratatui's `Chart`, `Canvas` and `BarChart` draw, and their axis and legend
/// lines; ratatui ignores the locale, so each set names its own.
#[derive(Debug, Clone, Copy)]
pub struct PlotMarks {
    /// A line: an XY line, a density curve, a fit drawn over bars, a dense Q-Q plot.
    pub line: Marker,
    /// One mark per point: a scatter, a box plot's strokes, a sparse Q-Q plot.
    pub point: Marker,
    /// A column from zero up to each point: the XY bar style and the histogram.
    pub bar: Marker,
    /// A vertical bar's top, one to eight eighths of a cell filled from the bottom;
    /// the last is a whole cell, the bar's body.
    pub column_eighths: &'static [&'static str; 8],
    /// The axes and the legend frame.
    pub axis: line::Set<'static>,
    /// A tick on the x axis line and on the y axis line, pointing at its label.
    pub tick_x: &'static str,
    pub tick_y: &'static str,
    /// The grid: a dotted line across the plot at a y tick, and down it at an x tick.
    pub grid_across: &'static str,
    pub grid_down: &'static str,
}

impl PlotMarks {
    /// The vertical bars a `BarChart` draws with.
    pub fn column_set(&self) -> ratatui::symbols::bar::Set<'static> {
        let e = self.column_eighths;
        ratatui::symbols::bar::Set {
            full: e[7],
            seven_eighths: e[6],
            three_quarters: e[5],
            five_eighths: e[4],
            half: e[3],
            three_eighths: e[2],
            one_quarter: e[1],
            one_eighth: e[0],
            empty: " ",
        }
    }

    /// Whether a canvas drawing with `marker` put this symbol in its cell, rather
    /// than an axis or a label. Braille's blank pattern counts: the grid draws it.
    pub fn is_mark(marker: Marker, symbol: &str) -> bool {
        let mut chars = symbol.chars();
        let (Some(c), None) = (chars.next(), chars.next()) else {
            return false;
        };
        match marker {
            Marker::Braille => ('\u{2800}'..='\u{28ff}').contains(&c),
            Marker::Dot => symbol == ratatui::symbols::DOT,
            Marker::Custom(mark) => c == mark,
            _ => false,
        }
    }

    /// Redraw ratatui `Chart` axes and legend frame (always `line::NORMAL`) in this set's
    /// `axis` lines, found by shape: the axes are the `└` with `│` above and an unclosed
    /// `─` run to its right; the legend is a closed box. Labels with those characters stay.
    /// A chart too small for both axes changes every line cell. A no-op under Unicode.
    pub fn redraw_axes(&self, area: Rect, buf: &mut Buffer) {
        let (from, to) = (line::NORMAL, self.axis);
        if from == to {
            return;
        }
        let area = area.intersection(buf.area);
        let at = |x: u16, y: u16| buf[(x, y)].symbol();
        // How many cells in a row hold `symbol`, stepping from (x, y) by (dx, dy).
        let run = |x: u16, y: u16, (dx, dy): (i32, i32), symbol: &str| {
            let mut n = 0;
            let (mut cx, mut cy) = (i32::from(x) + dx, i32::from(y) + dy);
            while (i32::from(area.left())..i32::from(area.right())).contains(&cx)
                && (i32::from(area.top())..i32::from(area.bottom())).contains(&cy)
                && at(cx as u16, cy as u16) == symbol
            {
                n += 1;
                cx += dx;
                cy += dy;
            }
            n
        };
        let (up, down, right) = ((0, -1), (0, 1), (1, 0));
        let mut frame = Vec::new();
        let mut found_axes = false;
        for y in area.top()..area.bottom() {
            for x in area.left()..area.right() {
                let symbol = at(x, y);
                if symbol == from.bottom_left {
                    let (high, wide) = (
                        run(x, y, up, from.vertical),
                        run(x, y, right, from.horizontal),
                    );
                    let end = x + wide + 1;
                    let boxed = end < area.right() && at(end, y) == from.bottom_right;
                    if high > 0 && wide > 0 && !boxed {
                        found_axes = true;
                        frame.extend((y - high..=y).map(|y| (x, y)));
                        frame.extend((x + 1..end).map(|x| (x, y)));
                    }
                } else if symbol == from.top_left {
                    let wide = run(x, y, right, from.horizontal);
                    let high = run(x, y, down, from.vertical);
                    let (r, b) = (x + wide + 1, y + high + 1);
                    let closed = r < area.right()
                        && b < area.bottom()
                        && at(r, y) == from.top_right
                        && at(x, b) == from.bottom_left
                        && at(r, b) == from.bottom_right
                        && run(r, y, down, from.vertical) == high
                        && run(x, b, right, from.horizontal) == wide;
                    if closed {
                        for i in x..=r {
                            frame.extend([(i, y), (i, b)]);
                        }
                        for j in y + 1..b {
                            frame.extend([(x, j), (r, j)]);
                        }
                    }
                }
            }
        }
        if !found_axes {
            frame = (area.top()..area.bottom())
                .flat_map(|y| (area.left()..area.right()).map(move |x| (x, y)))
                .collect();
        }
        let pairs = [
            (from.vertical, to.vertical),
            (from.horizontal, to.horizontal),
            (from.top_left, to.top_left),
            (from.top_right, to.top_right),
            (from.bottom_left, to.bottom_left),
            (from.bottom_right, to.bottom_right),
        ];
        for (x, y) in frame {
            let cell = &mut buf[(x, y)];
            if let Some((_, twin)) = pairs.iter().find(|(line, _)| cell.symbol() == *line) {
                cell.set_symbol(twin);
            }
        }
    }
}

const UNICODE: Glyphs = Glyphs {
    unicode: true,
    selector: "▎ ",
    selector_blank: "  ",
    cursor: "▏",
    prompt: "› ",
    rule: "│",
    rule_broken: "┆",
    ellipsis: "…",
    up: "..",
    times: "×",
    collapsed: "▸ ",
    expanded: "▾ ",
    updown: "↑↓",
    updown_lr: "←→",
    ctrl_updown: "^↑↓",
    middot: "·",
    dash: "—",
    r_squared: "R²",
    rho: "ρ",
    spinner: &["⣷", "⣯", "⣟", "⡿", "⢿", "⣻", "⣽", "⣾"],
    // Plain Unicode from blocks the common coding fonts cover, checked per codepoint
    // (`fc-list "<font>:charset=<hex>"`) against JetBrainsMono Nerd Font, Liberation Mono
    // and Noto Sans Mono. Emoji-class codepoints (☁, ⇅, ☑/☐, ◐/◑) were retired: they fall
    // back to the color-emoji font.
    here: "◦",
    in_memory: "▪",
    over_network: "↕",
    in_object_store: "≈",
    place_unknown: "◌",
    null: "∅",
    absent: "·",
    conflict: "≠",
    drift_mark: "*",
    sort_asc: "▲",
    sort_desc: "▼",
    rail: "▎",
    rule_h: "─",
    rule_h_focused: "━",
    arrow_left: "←",
    arrow_right: "→",
    trail: "›",
    choice_prev: "‹",
    choice_next: "›",
    hidden_mark: "⊘",
    diff_mark: "Δ",
    checkbox_on: "■",
    checkbox_off: "□",
    radio_on: "●",
    radio_off: "○",
    dot_full: "●",
    dot_half: "◔",
    dot_empty: "○",
    score_marks: &["○", "◔", "◕", "◉", "●"],
    check: "✓",
    // Not ⚠ U+26A0 (emoji-class, missing from Liberation and Noto): the triangle's shape
    // from a codepoint all three carry.
    warning: "▲",
    scroll_thumb: "█",
    scroll_track: "░",
    binary_stub: "‹binary›",
    // Latin-1, which every floor font carries; the arrows and control pictures
    // (↵ ⇥ ␊) are missing from Liberation Mono and Noto Sans Mono.
    newline_mark: "¶",
    tab_mark: "»",
    control_mark: "¤",
    hex_dot: "·",
    mini_bars: &["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"],
    unsampled: "·",
    pointer: "▲",
    bar_eighths: &["▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"],
    wordmark: Some(&[
        "┌──╮ ╭──╮ ╶─┬─╴ ╷  ╷ ╶┬╴",
        "│  │ ├──┤   │   │  │  │ ",
        "└──╯ ╵  ╵   ╵   ╰──╯ ╶┴╴",
    ]),
    border: ratatui::symbols::border::ROUNDED,
    plot: PlotMarks {
        line: Marker::Braille,
        point: Marker::Dot,
        bar: Marker::HalfBlock,
        column_eighths: &["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"],
        axis: line::NORMAL,
        tick_x: "┬",
        tick_y: "┤",
        grid_across: "·",
        grid_down: "┊",
    },
};

const ASCII: Glyphs = Glyphs {
    unicode: false,
    selector: "> ",
    selector_blank: "  ",
    cursor: "_",
    prompt: "> ",
    rule: "|",
    rule_broken: ":",
    ellipsis: "...",
    up: "..",
    times: "x",
    collapsed: "+ ",
    expanded: "- ",
    updown: "Up/Dn",
    updown_lr: "Lt/Rt",
    ctrl_updown: "^Up/Dn",
    middot: "-",
    dash: "-",
    r_squared: "R^2",
    rho: "rho",
    spinner: &["|", "/", "-", "\\"],
    here: ".",
    in_memory: "*",
    over_network: "~",
    in_object_store: "@",
    place_unknown: "?",
    null: "~",
    absent: ".",
    conflict: "!",
    drift_mark: "*",
    sort_asc: "^",
    sort_desc: "v",
    rail: ">",
    rule_h: "-",
    rule_h_focused: "=",
    arrow_left: "<",
    arrow_right: ">",
    trail: ">",
    choice_prev: "<",
    choice_next: ">",
    hidden_mark: "x",
    diff_mark: "*",
    checkbox_on: "[x]",
    checkbox_off: "[ ]",
    radio_on: "(*)",
    radio_off: "( )",
    dot_full: "#",
    dot_half: "+",
    dot_empty: ".",
    score_marks: &[".", "-", "+", "*", "#"],
    check: "+",
    warning: "!",
    scroll_thumb: "#",
    scroll_track: "|",
    binary_stub: "<binary>",
    // vim's `list` marks: `$` ends a line, `>` is a tab.
    newline_mark: "$",
    tab_mark: ">",
    control_mark: "?",
    hex_dot: ".",
    mini_bars: &[".", ":", "-", "=", "+", "*", "#", "@"],
    unsampled: "?",
    pointer: "^",
    // Under half a cell is still a mark, so a small value never reads as zero.
    bar_eighths: &["-", "-", "-", "=", "=", "=", "=", "#"],
    wordmark: None,
    border: ratatui::symbols::border::Set {
        top_left: "+",
        top_right: "+",
        bottom_left: "+",
        bottom_right: "+",
        vertical_left: "|",
        vertical_right: "|",
        horizontal_top: "-",
        horizontal_bottom: "-",
    },
    plot: PlotMarks {
        line: Marker::Custom('*'),
        point: Marker::Custom('o'),
        bar: Marker::Custom('#'),
        // Where the bar's top edge sits in its last cell: low, halfway, full.
        column_eighths: &["_", "_", "-", "-", "-", "#", "#", "#"],
        axis: line::Set {
            vertical: "|",
            horizontal: "-",
            top_right: "+",
            top_left: "+",
            bottom_right: "+",
            bottom_left: "+",
            vertical_left: "+",
            vertical_right: "+",
            horizontal_down: "+",
            horizontal_up: "+",
            cross: "+",
        },
        tick_x: "+",
        tick_y: "+",
        grid_across: ".",
        grid_down: ":",
    },
};

/// What the user asked for, from `[display] unicode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnicodeMode {
    /// Detect from the environment: see [`environment_is_utf8`].
    #[default]
    Auto,
    Always,
    Never,
}

/// One `[glyphs]` override from the config: a single glyph, or a list for the
/// slots that hold one (`spinner`, `score_marks`, `mini_bars`, `bar_eighths`).
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum SlotOverride {
    One(String),
    Many(Vec<String>),
}

/// The overridable single-string slots, via a callback macro so names, defaults and
/// assignment cannot drift. The wordmark is excluded: it is the brand.
macro_rules! with_string_slots {
    ($callback:ident) => {
        $callback!(
            selector,
            selector_blank,
            cursor,
            prompt,
            rule,
            rule_broken,
            ellipsis,
            up,
            times,
            collapsed,
            expanded,
            updown,
            updown_lr,
            ctrl_updown,
            middot,
            dash,
            r_squared,
            rho,
            here,
            in_memory,
            over_network,
            in_object_store,
            place_unknown,
            null,
            absent,
            conflict,
            drift_mark,
            sort_asc,
            sort_desc,
            rail,
            rule_h,
            rule_h_focused,
            arrow_left,
            arrow_right,
            trail,
            choice_prev,
            choice_next,
            hidden_mark,
            diff_mark,
            checkbox_on,
            checkbox_off,
            radio_on,
            radio_off,
            dot_full,
            dot_half,
            dot_empty,
            check,
            warning,
            scroll_thumb,
            scroll_track,
            binary_stub,
            newline_mark,
            tab_mark,
            control_mark,
            hex_dot,
            unsampled,
            pointer
        )
    };
}

/// Instructional text mapped to ASCII for the help overlay on a non-UTF-8 terminal, at
/// the render boundary only (user data is never transliterated). Coverage is checked by
/// `every_help_screen_is_ascii_clean`.
pub fn asciify_instructions(text: &str) -> std::borrow::Cow<'_, str> {
    if get().unicode || text.is_ascii() {
        return std::borrow::Cow::Borrowed(text);
    }
    std::borrow::Cow::Owned(instructions_in_ascii(text))
}

/// `text` with every instructional character replaced by its (wider) ASCII twin. Help
/// rows (key, two+ spaces, description) are re-padded per section, so descriptions
/// stay aligned and a section shifts right together when a key outgrows its column.
pub fn instructions_in_ascii(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + text.len() / 8);
    let mut section: Vec<&str> = Vec::new();
    for line in text.split('\n') {
        if line.trim().is_empty() {
            push_section(&section, &mut out);
            section.clear();
            push_ascii(line, &mut out);
            out.push('\n');
        } else {
            section.push(line);
        }
    }
    push_section(&section, &mut out);
    // Every line was pushed with a newline; the last one never had it.
    out.pop();
    out
}

/// One section's lines, each followed by a newline. Continuation lines,
/// indented to one of the section's description columns, move with it.
fn push_section(lines: &[&str], out: &mut String) {
    // (key end, description start, description column) per keyed row.
    let rows: Vec<Option<(usize, usize, usize)>> = lines
        .iter()
        .map(|line| key_gap(line).map(|(key, desc)| (key, desc, display_width(&line[..desc]))))
        .collect();
    let shift = lines
        .iter()
        .zip(&rows)
        .filter_map(|(line, row)| {
            row.map(|(key, _, column)| (ascii_width(&line[..key]) + 2).saturating_sub(column))
        })
        .max()
        .unwrap_or(0);
    for (line, row) in lines.iter().zip(&rows) {
        match *row {
            Some((key, desc, column)) => {
                push_ascii(&line[..key], out);
                let pad = column + shift - ascii_width(&line[..key]);
                out.extend(std::iter::repeat_n(' ', pad));
                push_ascii(&line[desc..], out);
            }
            None => {
                let indent = line.len() - line.trim_start_matches(' ').len();
                if shift > 0 && rows.iter().flatten().any(|&(_, _, c)| c == indent) {
                    out.extend(std::iter::repeat_n(' ', shift));
                }
                push_ascii(line, out);
            }
        }
        out.push('\n');
    }
}

/// Columns `text` takes once its instructional characters are ASCII.
fn ascii_width(text: &str) -> usize {
    let mut ascii = String::new();
    push_ascii(text, &mut ascii);
    ascii.len()
}

/// Where a help row's key ends and its description starts: the first run of two or
/// more spaces after the indent, with text after. `None` for prose, headings or blanks
/// (help prose uses single spaces).
pub(crate) fn key_gap(line: &str) -> Option<(usize, usize)> {
    let lead = line.len() - line.trim_start_matches(' ').len();
    let key_end = lead + line[lead..].find("  ")?;
    let desc_start = line.len() - line[key_end..].trim_start_matches(' ').len();
    (desc_start < line.len()).then_some((key_end, desc_start))
}

fn push_ascii(text: &str, out: &mut String) {
    let mut chars = text.chars().peekable();
    while let Some(c) = chars.next() {
        // A paired arrow is one key, spelled as the glyph set spells it;
        // twin by twin it would read "UpDn".
        let pair = match (c, chars.peek()) {
            ('↑', Some('↓')) => Some(ASCII.updown),
            ('←', Some('→')) => Some(ASCII.updown_lr),
            _ => None,
        };
        if let Some(pair) = pair {
            chars.next();
            out.push_str(pair);
            continue;
        }
        match ascii_twin(c) {
            Some(twin) => out.push_str(twin),
            None if c.is_ascii() => out.push(c),
            // An unknown character is marked, never sent raw; the audit test keeps this
            // unreachable from help files.
            None => out.push('?'),
        }
    }
}

/// Display columns `text` occupies; scalar counts get CJK and combining marks wrong.
pub fn display_width(text: &str) -> usize {
    UnicodeWidthStr::width(text)
}

/// The longest prefix of `text` that fits `width` display columns, never
/// splitting a wide character.
pub fn take_columns(text: &str, width: usize) -> &str {
    let mut used = 0usize;
    for (i, c) in text.char_indices() {
        let w = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > width {
            return &text[..i];
        }
        used += w;
    }
    text
}

/// The longest suffix of `text` that fits `width` display columns, never
/// splitting a wide character.
pub fn take_columns_end(text: &str, width: usize) -> &str {
    let mut used = 0usize;
    let mut start = text.len();
    for (i, c) in text.char_indices().rev() {
        let w = unicode_width::UnicodeWidthChar::width(c).unwrap_or(0);
        if used + w > width {
            break;
        }
        used += w;
        start = i;
    }
    &text[start..]
}

/// Printable ASCII only: one byte, one cell, one grapheme, so the fast paths
/// below need no segmentation.
fn plain_ascii(text: &str) -> bool {
    text.bytes().all(|b| (0x20..0x7f).contains(&b))
}

/// The graphemes of `text` as ratatui draws them: its own segmentation, with the
/// clusters holding a control character dropped as its `Span` drops them.
fn drawn_graphemes<'a>(span: &'a ratatui::text::Span<'a>) -> impl Iterator<Item = &'a str> {
    span.styled_graphemes(ratatui::style::Style::default())
        .map(|g| g.symbol)
}

/// Cells `text` takes in a table cell, grapheme by grapheme at ratatui's widths: unlike
/// [`display_width`], a joined emoji sequence counts once and control characters zero.
pub fn cell_width(text: &str) -> usize {
    use ratatui::buffer::CellWidth;
    if plain_ascii(text) {
        return text.len();
    }
    let span = ratatui::text::Span::raw(text);
    drawn_graphemes(&span)
        .map(|g| usize::from(g.cell_width()))
        .sum()
}

/// `text` fitted to `width` cells: whole if it fits, else cut at a grapheme boundary
/// and closed with `marker` (never half a wide character). Control characters are
/// dropped, as ratatui drops them. If `marker` itself does not fit, as much as does.
/// Reads `text` only as far as `width` cells and one more, however long it is.
pub fn fit_cells<'a>(text: &'a str, width: usize, marker: &str) -> Cow<'a, str> {
    use ratatui::buffer::CellWidth;
    let marker_width = cell_width(marker);
    let head_len = text.len().min(width.saturating_add(1));
    let head = text.get(..head_len).unwrap_or(text);
    if head.len() == head_len && plain_ascii(head) {
        if text.len() <= width {
            return Cow::Borrowed(text);
        }
        if width <= marker_width {
            return Cow::Owned(take_columns(marker, width).to_string());
        }
        return Cow::Owned(format!("{}{marker}", &text[..width - marker_width]));
    }
    let span = ratatui::text::Span::raw(text);
    // Whether it fits, measured no further than past `width`.
    let mut total = 0usize;
    let mut drawn_bytes = 0usize;
    for g in drawn_graphemes(&span) {
        total += usize::from(g.cell_width());
        if total > width {
            break;
        }
        drawn_bytes += g.len();
    }
    if total <= width {
        return if drawn_bytes == text.len() {
            Cow::Borrowed(text)
        } else {
            Cow::Owned(drawn_graphemes(&span).collect())
        };
    }
    if width <= marker_width {
        return Cow::Owned(take_columns(marker, width).to_string());
    }
    let budget = width - marker_width;
    let mut used = 0usize;
    let mut out = String::with_capacity(budget + marker.len());
    for g in drawn_graphemes(&span) {
        let w = usize::from(g.cell_width());
        if used + w > budget {
            break;
        }
        used += w;
        out.push_str(g);
    }
    out.push_str(marker);
    Cow::Owned(out)
}

/// The ASCII twin of one instructional character, `None` for plain ASCII or
/// a character no help file may use.
fn ascii_twin(c: char) -> Option<&'static str> {
    match c {
        '↑' => Some("Up"),
        '↓' => Some("Dn"),
        '←' => Some("Lt"),
        '→' => Some("Rt"),
        '↔' => Some("Lt/Rt"),
        '—' => Some("-"),
        '…' => Some("..."),
        '×' => Some("x"),
        '≤' => Some("<="),
        '≥' => Some(">="),
        '∅' => Some("~"),
        '·' => Some("."),
        '≠' => Some("!"),
        'ρ' => Some("rho"),
        '│' => Some("|"),
        '┆' => Some(":"),
        '¶' => Some("$"),
        '»' => Some(">"),
        '¤' => Some("?"),
        'Δ' => Some("*"),
        _ => None,
    }
}

/// The Unicode default for a single-string slot, or `None` for a list slot or
/// an unknown name.
fn unicode_default(slot: &str) -> Option<&'static str> {
    macro_rules! lookup {
        ($($name:ident),*) => {
            match slot {
                $(stringify!($name) => Some(UNICODE.$name),)*
                _ => None,
            }
        };
    }
    with_string_slots!(lookup)
}

/// Check a `[glyphs]` override map at load time, naming the bad slot. Each override
/// must keep its glyph's display width, so every layout width invariant holds.
pub fn validate_overrides(overrides: &BTreeMap<String, SlotOverride>) -> Result<(), String> {
    let same_width = |slot: &str, text: &str, default: &str| -> Result<(), String> {
        if text.width() == default.width() {
            Ok(())
        } else {
            Err(format!(
                "glyph override for `{slot}` is {} columns wide; {default:?} is {} — \
                 an override must keep the width of the glyph it replaces",
                text.width(),
                default.width(),
            ))
        }
    };
    for (slot, value) in overrides {
        match (unicode_default(slot), value) {
            (Some(default), SlotOverride::One(text)) => same_width(slot, text, default)?,
            (Some(_), SlotOverride::Many(_)) => {
                return Err(format!(
                    "glyph slot `{slot}` takes a single string, not a list"
                ));
            }
            (None, _) => {
                let (len, default) = match slot.as_str() {
                    "spinner" => (None, UNICODE.spinner),
                    "score_marks" => (Some(5), &UNICODE.score_marks[..]),
                    "mini_bars" => (Some(8), &UNICODE.mini_bars[..]),
                    "bar_eighths" => (Some(8), &UNICODE.bar_eighths[..]),
                    _ => return Err(format!("unknown glyph slot `{slot}`")),
                };
                let SlotOverride::Many(entries) = value else {
                    return Err(format!("glyph slot `{slot}` takes a list of strings"));
                };
                match len {
                    Some(len) if entries.len() != len => {
                        return Err(format!(
                            "glyph slot `{slot}` takes exactly {len} entries, got {}",
                            entries.len()
                        ));
                    }
                    None if entries.is_empty() => {
                        return Err(format!("glyph slot `{slot}` takes at least one entry"));
                    }
                    _ => {}
                }
                for entry in entries {
                    same_width(slot, entry, default[0])?;
                }
            }
        }
    }
    Ok(())
}

/// A config string lives as long as the run does; the set holds `&'static str`.
fn leak(text: &str) -> &'static str {
    Box::leak(text.to_string().into_boxed_str())
}

/// Lay a validated override map over a set. Called once at startup.
fn apply_overrides(set: &mut Glyphs, overrides: &BTreeMap<String, SlotOverride>) {
    for (slot, value) in overrides {
        match (slot.as_str(), value) {
            ("spinner", SlotOverride::Many(frames)) => {
                set.spinner = Box::leak(
                    frames
                        .iter()
                        .map(|f| leak(f))
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                );
            }
            ("score_marks", SlotOverride::Many(marks)) if marks.len() == 5 => {
                set.score_marks = Box::leak(Box::new(std::array::from_fn(|i| leak(&marks[i]))));
            }
            ("mini_bars", SlotOverride::Many(bars)) if bars.len() == 8 => {
                set.mini_bars = Box::leak(Box::new(std::array::from_fn(|i| leak(&bars[i]))));
            }
            ("bar_eighths", SlotOverride::Many(bars)) if bars.len() == 8 => {
                set.bar_eighths = Box::leak(Box::new(std::array::from_fn(|i| leak(&bars[i]))));
            }
            (name, SlotOverride::One(text)) => {
                let text = leak(text);
                macro_rules! assign {
                    ($($slot:ident),*) => {
                        match name {
                            $(stringify!($slot) => set.$slot = text,)*
                            // Validated at config load; an unknown name that
                            // still got here changes nothing.
                            _ => {}
                        }
                    };
                }
                with_string_slots!(assign)
            }
            _ => {}
        }
    }
}

static GLYPHS: OnceLock<Glyphs> = OnceLock::new();

/// The signals the glyph choice reads, gathered so the rule can be tested on
/// any OS.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct Environment {
    /// The first non-empty of `LC_ALL`, `LC_CTYPE`, `LANG`.
    locale: Option<String>,
    /// Running on Windows, which sets no locale variable.
    windows: bool,
}

impl Environment {
    fn current() -> Self {
        Self {
            locale: ["LC_ALL", "LC_CTYPE", "LANG"]
                .into_iter()
                .find_map(|key| std::env::var(key).ok().filter(|v| !v.is_empty())),
            windows: cfg!(windows),
        }
    }

    /// The rule: a set locale variable decides on every OS (`LANG=C` is ASCII; MSYS2 as on
    /// Unix). Windows sets none and its console takes Unicode regardless of code page;
    /// `WT_SESSION` and the code page are unusable signals there.
    fn is_utf8(&self) -> bool {
        match &self.locale {
            Some(value) => {
                let lower = value.to_ascii_lowercase();
                lower.contains("utf-8") || lower.contains("utf8")
            }
            None => self.windows,
        }
    }
}

/// Whether the terminal can be trusted with UTF-8: `LC_ALL` over `LC_CTYPE` over `LANG`,
/// as POSIX; with none set, only Windows counts. The locale, not terminal capability,
/// decides whether multi-byte characters render.
pub fn environment_is_utf8() -> bool {
    Environment::current().is_utf8()
}

/// Choose the glyph set for this run, with `[glyphs]` overrides over the Unicode set.
/// Never over the ASCII set: it is the tested floor, where rich-font overrides would
/// garble. Later calls are ignored.
pub fn init_with_overrides(mode: UnicodeMode, overrides: &BTreeMap<String, SlotOverride>) {
    let mut chosen = match mode {
        UnicodeMode::Always => UNICODE,
        UnicodeMode::Never => ASCII,
        UnicodeMode::Auto => {
            if environment_is_utf8() {
                UNICODE
            } else {
                ASCII
            }
        }
    };
    if chosen.unicode && !overrides.is_empty() {
        apply_overrides(&mut chosen, overrides);
    }
    let _ = GLYPHS.set(chosen);
}

/// The active glyph set. Falls back to detection when [`init_with_overrides`] was never
/// called, so library users and tests get sensible symbols without ceremony.
pub fn get() -> &'static Glyphs {
    GLYPHS.get_or_init(|| {
        if environment_is_utf8() {
            UNICODE
        } else {
            ASCII
        }
    })
}

/// Whether the active set is Unicode (copies of consts have no identifying address).
pub fn active_is_unicode() -> bool {
    get().unicode
}

/// The Unicode set, for tests and for callers that know their output is UTF-8.
pub fn unicode() -> &'static Glyphs {
    &UNICODE
}

/// Each bordered box in `rows` as the (column, row) of its bottom-left corner, for
/// tests counting frames (ASCII corners are all `+`, so the corner is found by its
/// left side above and bottom edge after).
#[cfg(test)]
pub(crate) fn frame_corners(rows: &[String]) -> Vec<(usize, usize)> {
    let b = get().border;
    let grid: Vec<Vec<String>> = rows
        .iter()
        .map(|r| r.chars().map(String::from).collect())
        .collect();
    let at = |x: usize, y: usize| grid.get(y).and_then(|r| r.get(x)).map(String::as_str);
    // Where each corner has a glyph of its own, every one on screen is a box's,
    // whatever its shape: count them all, and every box opened must close.
    let glyphs = [b.top_left, b.top_right, b.bottom_left, b.bottom_right];
    if (1..4).all(|i| !glyphs[..i].contains(&glyphs[i])) {
        let opened: usize = rows.iter().map(|r| r.matches(b.top_left).count()).sum();
        let closed: Vec<(usize, usize)> = grid
            .iter()
            .enumerate()
            .flat_map(|(y, row)| {
                row.iter()
                    .enumerate()
                    .filter(|(_, cell)| cell.as_str() == b.bottom_left)
                    .map(move |(x, _)| (x, y))
            })
            .collect();
        assert_eq!(opened, closed.len(), "every frame closes: {rows:#?}");
        return closed;
    }
    let mut corners = Vec::new();
    for (y, row) in grid.iter().enumerate().skip(1) {
        for x in 0..row.len() {
            if at(x, y) == Some(b.bottom_left)
                && at(x, y - 1) == Some(b.vertical_left)
                && at(x + 1, y) == Some(b.horizontal_bottom)
                && (x == 0 || at(x - 1, y) != Some(b.horizontal_bottom))
            {
                corners.push((x, y));
            }
        }
    }
    corners
}

/// The ASCII set.
pub fn ascii() -> &'static Glyphs {
    &ASCII
}

/// `text` in `width` columns, cut at its end and marked with the ellipsis.
pub fn fit(text: &str, width: usize) -> String {
    fit_cells(text, width, get().ellipsis).into_owned()
}

/// `text` in `width` columns, cut at its start: a path keeps its leaf. The ellipsis is
/// measured, since it is three columns on an ASCII terminal.
pub fn fit_start(text: &str, width: usize) -> String {
    if display_width(text) <= width {
        return text.to_string();
    }
    let ellipsis = get().ellipsis;
    let ellipsis_width = display_width(ellipsis);
    if width <= ellipsis_width {
        return take_columns_end(text, width).to_string();
    }
    format!(
        "{ellipsis}{}",
        take_columns_end(text, width - ellipsis_width)
    )
}

/// `text` in `width` columns, cut from the middle: `weather/…/daily`.
pub fn fit_middle(text: &str, width: usize) -> String {
    if display_width(text) <= width {
        return text.to_string();
    }
    let mark = get().ellipsis;
    let mark_w = display_width(mark);
    if width <= mark_w {
        return take_columns(mark, width).to_string();
    }
    let room = width - mark_w;
    let head = take_columns(text, room.div_ceil(2));
    let tail = take_columns_end(text, room - display_width(head));
    format!("{head}{mark}{tail}")
}

/// Text written with `·` between its parts, in the glyph set's middot: `-` on an ASCII
/// terminal. A `·` in a UI string is this template, drawn through here.
pub fn dotted(text: &str) -> String {
    text.replace('·', get().middot)
}

#[cfg(test)]
mod tests;
