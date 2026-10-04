//! Terminal glyphs, with an ASCII fallback.
//!
//! datui runs on a modern desktop terminal *and* over SSH on a plain server with a
//! bitmap font and a C locale. Neither should be the one that suffers: on a capable
//! terminal the box-drawing and arrow characters carry real meaning, and on a
//! limited one they turn into replacement boxes that make the UI harder to read
//! rather than prettier.
//!
//! Nothing here is a Nerd Font glyph. Every Unicode character has passed the
//! font-coverage audit (`scripts/code/audit_glyphs.py`): present in JetBrainsMono
//! Nerd Font, and never an `Emoji=Yes, Emoji_Presentation=No` codepoint that
//! Liberation Mono and Noto Sans Mono don't also carry, because a terminal whose
//! font lacks one of those falls back to the *color emoji* font and renders a
//! blank cell or a clipped blob (#325). Nerd Font icons appear only in the Omarchy
//! menu definition, where the font is guaranteed — and in a user's own `[glyphs]`
//! overrides, where the risk is theirs. The ASCII fallback exists for terminals
//! that are not doing UTF-8 at all.

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
    /// Whether this is the Unicode set. `UNICODE` and `ASCII` are consts, so
    /// every use instantiates fresh promoted statics — neither the set's address
    /// nor its fields' can identify it. The set says itself.
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
    /// The frozen-columns separator while the window is too narrow for every frozen
    /// column: the rest scroll after it until there is room. One column wide, like
    /// `rule`, and visibly not it.
    pub rule_broken: &'static str,
    /// Truncation marker.
    pub ellipsis: &'static str,
    /// Between row and column counts: `2.4M × 18`.
    pub times: &'static str,
    /// Section collapse markers; both must be the same display width.
    pub collapsed: &'static str,
    pub expanded: &'static str,
    /// Keycap names for the control bar. Named keys are spelled out there, matching
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
    /// Where a row's data lives, shown beside its name.
    ///
    /// Beside the name on purpose. The detail pane has said this for a long time, but
    /// on a full-screen ultrawide the pane is a foot away from the row the cursor is
    /// on, and "is this one in the cloud?" is a question you ask about the row you are
    /// looking at. All five must be the same display width or every name after them
    /// shifts by a column.
    pub here: &'static str,
    pub in_memory: &'static str,
    pub over_network: &'static str,
    pub in_object_store: &'static str,
    pub place_unknown: &'static str,
    /// A null cell. Blank is what a null used to be, and blank is also what an empty
    /// string is, so the two were indistinguishable.
    pub null: &'static str,
    /// A cell whose file has no such column: not a null the data holds, but a column
    /// that file was written without. One character wide, like `null`, so a column of
    /// them lines up.
    pub absent: &'static str,
    /// A cell whose file stores the column in a type the column cannot hold, so it was
    /// not read from that file. A value is there; it is not this type.
    pub conflict: &'static str,
    /// After a column's name in the header: this column is not in every file, or the
    /// files disagree on its type. A footnote mark, and the Info panel is the note.
    pub drift_mark: &'static str,
    /// After a column's name in the header: the view is sorted by this column, and
    /// which way. The header width arithmetic counts these like `drift_mark`, so both
    /// must be one column wide in both sets. Triangles, not `arrow_left`/`arrow_right`,
    /// which already mean "columns off-screen" in the same header row.
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
    /// Stands in for a value that is bytes, not text.
    pub binary_stub: &'static str,
    /// In a one-line preview of text, where the value has a line break, a tab, or
    /// another control character, which a terminal cell cannot draw. One column
    /// wide in both sets. The inspector shows the characters themselves.
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

/// The marks ratatui's `Chart`, `Canvas` and `BarChart` put on a plot, and the lines
/// of its axes and legend frame. ratatui picks none of these from the locale, so each
/// set names its own.
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

    /// ratatui's `Chart` draws its axes and legend frame from `line::NORMAL`
    /// whatever the set, and exposes neither; this finds them by shape and redraws
    /// them from the set's own `axis` lines. The axes are the `└` with `│` above it
    /// and a `─` run to its right that no `┘` closes; the legend is a closed box.
    /// A label, title or name holding the same characters is left alone. A chart
    /// too small for both axes has no corner to find them by, so there every line
    /// cell changes. Nothing changes under the Unicode set.
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
    // Plain Unicode from blocks the common coding fonts actually cover — checked
    // against JetBrainsMono Nerd Font, Liberation Mono and Noto Sans Mono per
    // codepoint (`fc-list "<font>:charset=<hex>"`). A slot the terminal font lacks
    // is worse than absent: an `Emoji=Yes, Emoji_Presentation=No` codepoint (☁, ☑)
    // falls back to the *color emoji* font and renders a blank cell or a clipped
    // blob, and no font the user picks fixes that. That audit retired ☁ U+2601,
    // ⇅ U+21C5, ☑/☐ U+2611/U+2610 and ◐/◑ U+25D0/U+25D1 from this set (#325).
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
    // Not ⚠ U+26A0: emoji-class, and absent from Liberation Mono and Noto Sans
    // Mono, so those setups hit the color-emoji fallback. The caution triangle's
    // shape, from a codepoint all three floor fonts carry.
    warning: "▲",
    scroll_thumb: "█",
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

/// The overridable single-string slots, passed to a callback macro so the name
/// list, the default lookup and the assignment cannot drift apart. The wordmark
/// is deliberately absent: it is the brand, and it already yields to the
/// one-line title wherever it cannot be drawn.
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

/// Instructional text with its Unicode characters mapped to ASCII, for the
/// help overlay on a terminal that is not doing UTF-8. Applied at the render
/// boundary only — user data is never transliterated. The pairs cover what
/// the help files actually contain; the audit that counts them is
/// `every_help_screen_is_ascii_clean`.
pub fn asciify_instructions(text: &str) -> std::borrow::Cow<'_, str> {
    if get().unicode || text.is_ascii() {
        return std::borrow::Cow::Borrowed(text);
    }
    std::borrow::Cow::Owned(instructions_in_ascii(text))
}

/// `text` with every instructional character replaced by its ASCII twin,
/// whatever the terminal. The twins are wider (`↑` is `Up`), so help rows
/// laid out as key, two or more spaces, description are re-padded one
/// section (a run of non-blank lines) at a time: descriptions stay at the
/// columns they were authored at, and when an ASCII key no longer fits, the
/// whole section moves right together rather than that one row.
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

/// Where a help row's key ends and its description starts: the first run of
/// two or more spaces after the indent, with text after it. `None` for a
/// line without one (prose, a heading, a blank line). Help prose takes one
/// space between sentences, so the first double space is always a key gap.
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
            // A character the map does not know is marked rather than
            // shipped to a terminal that cannot draw it; the audit test
            // keeps this case from ever being reachable from a help file.
            None => out.push('?'),
        }
    }
}

/// Display columns `text` will occupy, as the terminal draws it. Scalar
/// counts undercount CJK and overcount combining marks; layout math that
/// budgets cells must use this.
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

/// Cells `text` takes when a table cell draws it, grapheme by grapheme at
/// ratatui's own widths. Unlike [`display_width`], an emoji sequence joined
/// into one grapheme counts once, and control characters count nothing,
/// because ratatui draws nothing for them.
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

/// `text` as it fits in `width` cells: whole when it fits, otherwise cut at a
/// grapheme boundary and closed with `marker`, so a clipped value never passes
/// for a whole one. A wide character that would straddle the edge goes, never
/// half of it. Control characters are dropped, as ratatui would drop them, so
/// the result is exactly what is drawn. When even `marker` does not fit, as much
/// of it as fits.
pub fn fit_cells<'a>(text: &'a str, width: usize, marker: &str) -> Cow<'a, str> {
    use ratatui::buffer::CellWidth;
    let marker_width = cell_width(marker);
    if plain_ascii(text) {
        if text.len() <= width {
            return Cow::Borrowed(text);
        }
        if width <= marker_width {
            return Cow::Owned(take_columns(marker, width).to_string());
        }
        return Cow::Owned(format!("{}{marker}", &text[..width - marker_width]));
    }
    let span = ratatui::text::Span::raw(text);
    let total: usize = drawn_graphemes(&span)
        .map(|g| usize::from(g.cell_width()))
        .sum();
    if total <= width {
        let whole = drawn_graphemes(&span).map(str::len).sum::<usize>() == text.len();
        return if whole {
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

/// Check a `[glyphs]` override map without touching the active set, so a bad
/// config fails at load time with the slot named, not mid-draw.
///
/// An override must keep the display width of the glyph it replaces: every
/// width invariant in the layout arithmetic — the locality markers, the header
/// marks, the equal-width spinner frames — holds automatically that way.
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
    /// A terminal that draws these glyphs whatever the code page: Windows
    /// Terminal (`WT_SESSION`) or VS Code's (`TERM_PROGRAM=vscode`). Always false
    /// off Windows.
    unicode_terminal: bool,
    /// The console output code page. Always `None` off Windows.
    console_code_page: Option<u32>,
}

/// UTF-8, as a Windows code page.
const CP_UTF8: u32 = 65001;

impl Environment {
    fn current() -> Self {
        let locale = ["LC_ALL", "LC_CTYPE", "LANG"]
            .into_iter()
            .find_map(|key| std::env::var(key).ok().filter(|v| !v.is_empty()));
        #[cfg(windows)]
        {
            // SAFETY: takes no arguments and only reads console state; 0 means
            // there is no console.
            let page = unsafe { windows_sys::Win32::System::Console::GetConsoleOutputCP() };
            Self {
                locale,
                unicode_terminal: std::env::var_os("WT_SESSION").is_some()
                    || std::env::var_os("TERM_PROGRAM").is_some_and(|t| t == "vscode"),
                console_code_page: (page != 0).then_some(page),
            }
        }
        #[cfg(not(windows))]
        Self {
            locale,
            ..Self::default()
        }
    }

    /// The rule. A locale variable decides when one is set, on every OS, so
    /// `LANG=C` means ASCII everywhere and MSYS2 shells on Windows count as
    /// they do on Unix. Windows itself sets none: there, Windows Terminal or
    /// VS Code (whose consoles default to an OEM code page, but which draw
    /// these glyphs) or a console switched to UTF-8 (`chcp 65001`, or the
    /// system "Use Unicode UTF-8" option) picks Unicode.
    fn is_utf8(&self) -> bool {
        match &self.locale {
            Some(value) => {
                let lower = value.to_ascii_lowercase();
                lower.contains("utf-8") || lower.contains("utf8")
            }
            None => self.unicode_terminal || self.console_code_page == Some(CP_UTF8),
        }
    }
}

/// Whether the terminal can be trusted with UTF-8.
///
/// `LC_ALL` beats `LC_CTYPE` beats `LANG`, as in POSIX. With none of them set,
/// Windows counts as UTF-8 under Windows Terminal, VS Code's terminal or a
/// UTF-8 console code page. A terminal that is not doing UTF-8 renders
/// multi-byte characters as replacement boxes, so this is the signal that
/// matters, not terminal capability, which says nothing about the font.
pub fn environment_is_utf8() -> bool {
    Environment::current().is_utf8()
}

/// Choose the glyph set for this run. Later calls are ignored, so this is safe to
/// call once from startup and never think about again.
pub fn init(mode: UnicodeMode) {
    init_with_overrides(mode, &BTreeMap::new());
}

/// [`init`], with the config's `[glyphs]` overrides laid over the Unicode set.
/// The ASCII set is never touched: it is the tested floor a C locale falls back
/// to, and an override written for a rich font would garble exactly there.
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

/// The active glyph set. Falls back to detection when [`init`] was never
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

/// Whether the active set is the Unicode one. `get` hands out a copy of a
/// const, so no address — the set's nor a field's — can identify it; the flag
/// on the set can.
pub fn active_is_unicode() -> bool {
    get().unicode
}

/// The Unicode set, for tests and for callers that know their output is UTF-8.
pub fn unicode() -> &'static Glyphs {
    &UNICODE
}

/// Each bordered box drawn in `rows` (one string per screen row), as the
/// (column, row) of its bottom-left corner, in the active set.
///
/// For tests that count frames. The ASCII set draws every corner as `+`, so
/// counting the corner glyph finds four per box there; a bottom-left corner
/// is the one with the left side above it and the bottom edge after it.
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

#[cfg(test)]
mod tests {
    use super::*;
    use unicode_width::UnicodeWidthStr;

    /// Every character the key registry uses outside ASCII has a twin in
    /// `ascii_twin`, so the ASCII floor never sees a `?` where an
    /// instruction was.
    #[test]
    fn every_help_screen_is_ascii_clean() {
        use datui_cli::keys;
        let mut texts: Vec<&str> = Vec::new();
        for (screen, group, key) in keys::entries() {
            texts.extend([group.name, key.keys, key.label, key.line, key.long()]);
            if let Some(screen) = screen {
                texts.push(screen.title);
            }
        }
        for (example, meaning) in keys::Q_SUMMARY {
            texts.extend([*example, *meaning]);
        }
        for text in &texts {
            for c in text.chars().filter(|c| !c.is_ascii()) {
                assert!(
                    ascii_twin(c).is_some(),
                    "{text:?} uses {c:?}, which has no ASCII twin"
                );
            }
        }
        let checked = texts.len();
        assert!(checked > 10, "the help files were found");
        // And the border set's twin is pure ASCII by construction.
        let b = ASCII.border;
        for piece in [
            b.top_left,
            b.top_right,
            b.bottom_left,
            b.bottom_right,
            b.vertical_left,
            b.vertical_right,
            b.horizontal_top,
            b.horizontal_bottom,
        ] {
            assert!(piece.is_ascii(), "{piece:?}");
        }
    }

    /// A key whose arrows become words re-pads, so its description stays in
    /// line with the rows around it.
    #[test]
    fn an_ascii_key_keeps_its_description_column() {
        let text = "Keys:\n  ↑ / ↓:      Move\n  Enter:      Open, → on a folder\n";
        assert_eq!(
            instructions_in_ascii(text),
            "Keys:\n  Up / Dn:    Move\n  Enter:      Open, Rt on a folder\n"
        );
    }

    /// Paired arrows read as the glyph set's pair, not as two words run
    /// together, and the row still keeps its column.
    #[test]
    fn paired_arrows_read_as_one_key() {
        let text = "  ↑↓ / j/k:      Rows\n  ←→ / h/l:      Columns\n  Home/End:      Ends";
        assert_eq!(
            instructions_in_ascii(text),
            "  Up/Dn / j/k:   Rows\n  Lt/Rt / h/l:   Columns\n  Home/End:      Ends"
        );
    }

    /// A key that no longer fits its column moves its whole section right,
    /// continuation lines included; prose and other sections stay put.
    #[test]
    fn an_overlong_ascii_key_moves_its_section_together() {
        let text = [
            "Keys:",
            "  ← / → (h/l):  Page",
            "  e:            Plan, and",
            "                more",
            "  Prose → here.",
            "",
            "  q:  Quit",
        ]
        .join("\n");
        let expected = [
            "Keys:",
            "  Lt / Rt (h/l):  Page",
            "  e:              Plan, and",
            "                  more",
            "  Prose Rt here.",
            "",
            "  q:  Quit",
        ]
        .join("\n");
        assert_eq!(instructions_in_ascii(&text), expected);
    }

    /// Every locality marker has to be the same display width in a given set, or the
    /// name beside it starts one column further along on some rows than on others and
    /// the whole list looks broken.
    #[test]
    fn locality_markers_are_all_one_column() {
        for set in [unicode(), ascii()] {
            for marker in [
                set.here,
                set.in_memory,
                set.over_network,
                set.in_object_store,
                set.place_unknown,
            ] {
                assert_eq!(
                    UnicodeWidthStr::width(marker),
                    1,
                    "{marker:?} is not one column wide"
                );
            }
        }
    }

    /// The sort marks sit inside the header's column-width arithmetic, so each must be
    /// exactly one column in both sets or a sorted column drifts out of line.
    #[test]
    fn sort_marks_are_one_column() {
        for set in [unicode(), ascii()] {
            for mark in [set.sort_asc, set.sort_desc] {
                assert_eq!(
                    UnicodeWidthStr::width(mark),
                    1,
                    "{mark:?} is not one column wide"
                );
            }
        }
    }

    /// The two sets must agree column for column, since the layout arithmetic around
    /// them is written once and used for both.
    #[test]
    fn the_two_sets_have_the_same_shape() {
        let (u, a) = (unicode(), ascii());
        for (left, right) in [
            (u.here, a.here),
            (u.in_memory, a.in_memory),
            (u.over_network, a.over_network),
            (u.in_object_store, a.in_object_store),
            (u.place_unknown, a.place_unknown),
            (u.selector, a.selector),
            (u.selector_blank, a.selector_blank),
            (u.collapsed, a.collapsed),
            (u.expanded, a.expanded),
            (u.sort_asc, a.sort_asc),
            (u.sort_desc, a.sort_desc),
        ]
        .into_iter()
        .chain(
            u.bar_eighths
                .iter()
                .copied()
                .zip(a.bar_eighths.iter().copied()),
        ) {
            assert_eq!(
                UnicodeWidthStr::width(left),
                UnicodeWidthStr::width(right),
                "{left:?} and {right:?} are different widths"
            );
        }
    }

    /// ratatui draws the plot marks, so the audit script cannot see the ASCII
    /// set's marker characters; this checks them, and that each column eighth is
    /// one cell in both sets.
    #[test]
    fn the_ascii_plot_marks_are_ascii() {
        let p = ascii().plot;
        for marker in [p.line, p.point, p.bar] {
            let Marker::Custom(c) = marker else {
                panic!("{marker:?} is drawn by ratatui from its own Unicode set");
            };
            assert!(c.is_ascii_graphic(), "{c:?}");
        }
        let a = p.axis;
        for piece in [
            a.vertical,
            a.horizontal,
            a.top_right,
            a.top_left,
            a.bottom_right,
            a.bottom_left,
            a.vertical_left,
            a.vertical_right,
            a.horizontal_down,
            a.horizontal_up,
            a.cross,
        ] {
            assert!(piece.is_ascii(), "{piece:?}");
        }
        for set in [unicode(), ascii()] {
            for eighth in set.plot.column_eighths {
                assert_eq!(UnicodeWidthStr::width(*eighth), 1, "{eighth:?}");
            }
        }
    }

    fn chart_buffer(width: u16, height: u16, x_title: &str, name: &str) -> Buffer {
        use ratatui::widgets::{Axis, Chart, Dataset, LegendPosition, Widget};
        let labels = || vec!["0", "5", "10"];
        let chart = Chart::new(vec![
            Dataset::default()
                .name(name)
                .marker(Marker::Custom('o'))
                .data(&[(5.0, 5.0)]),
        ])
        .x_axis(
            Axis::default()
                .title(x_title)
                .bounds([0.0, 10.0])
                .labels(labels()),
        )
        .y_axis(Axis::default().bounds([0.0, 10.0]).labels(labels()))
        .legend_position(Some(LegendPosition::TopRight))
        .hidden_legend_constraints((
            ratatui::layout::Constraint::Percentage(100),
            ratatui::layout::Constraint::Percentage(100),
        ));
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        chart.render(area, &mut buf);
        buf
    }

    fn buffer_text(buf: &Buffer) -> String {
        let a = buf.area;
        (a.top()..a.bottom())
            .map(|y| {
                (a.left()..a.right())
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The swap finds the axes and the legend frame by shape: a title or a legend
    /// name holding the same characters keeps them, and Unicode changes nothing.
    #[test]
    fn redraw_axes_changes_only_the_frame() {
        let before = chart_buffer(40, 12, "a│b└─c", "x─│y");
        let mut buf = before.clone();
        unicode().plot.redraw_axes(buf.area, &mut buf);
        assert_eq!(buf, before);

        ascii().plot.redraw_axes(buf.area, &mut buf);
        let text = buffer_text(&buf);
        let rows: Vec<&str> = text.lines().collect();
        assert!(rows[0].ends_with("+----+"), "the legend frame:\n{text}");
        assert!(rows[1].ends_with("|x─│y|"), "the legend name:\n{text}");
        assert!(rows[2].ends_with("+----+"), "the legend frame:\n{text}");
        assert!(text.contains("a│b└─c"), "the axis title:\n{text}");
        assert!(rows[10].contains("+-------"), "the axis corner:\n{text}");
        let kept: String = text.chars().filter(|c| !c.is_ascii()).collect();
        assert_eq!(kept, "─││└─", "only the name and the title:\n{text}");
    }

    /// Under three rows the chart has no x axis, so no corner to find the y axis by;
    /// every line cell changes then, and the plot is still ASCII.
    #[test]
    fn redraw_axes_in_a_chart_too_small_for_both_axes() {
        let mut buf = chart_buffer(20, 2, "", "");
        assert!(!buffer_text(&buf).is_ascii());
        ascii().plot.redraw_axes(buf.area, &mut buf);
        let text = buffer_text(&buf);
        assert!(text.is_ascii() && text.contains('|'), "{text}");
    }

    /// `get()` stores a copy of a const, so no address can identify the active
    /// set — the `unicode` flag on the set is what `active_is_unicode` reads.
    #[test]
    fn active_is_unicode_matches_the_chosen_set() {
        let expected = get().spinner.len() == unicode().spinner.len();
        assert_eq!(active_is_unicode(), expected);
        assert!(unicode().unicode);
        assert!(!ascii().unicode);
    }

    /// A locale variable decides on every OS; Windows signals count only when
    /// none is set (#541).
    #[test]
    fn utf8_rule_reads_the_locale_then_the_windows_console() {
        let env = |locale: Option<&str>, unicode_terminal: bool, page: Option<u32>| Environment {
            locale: locale.map(String::from),
            unicode_terminal,
            console_code_page: page,
        };
        // Unix: the locale alone.
        assert!(env(Some("en_US.UTF-8"), false, None).is_utf8());
        assert!(env(Some("C.utf8"), false, None).is_utf8());
        assert!(!env(Some("C"), false, None).is_utf8());
        assert!(!env(None, false, None).is_utf8());
        // Windows sets no locale: Windows Terminal, VS Code or a UTF-8 code page.
        assert!(env(None, true, Some(437)).is_utf8());
        assert!(env(None, false, Some(CP_UTF8)).is_utf8());
        assert!(!env(None, false, Some(437)).is_utf8());
        // An explicit locale still wins there, as LANG=C does on Unix.
        assert!(!env(Some("C"), true, Some(CP_UTF8)).is_utf8());
        assert!(env(Some("en_US.UTF-8"), false, Some(437)).is_utf8());
    }

    /// Off Windows the console signals are never read, so Unix behavior is the
    /// locale's alone.
    #[cfg(not(windows))]
    #[test]
    fn unix_reads_no_windows_signals() {
        let current = Environment::current();
        assert!(!current.unicode_terminal);
        assert_eq!(current.console_code_page, None);
    }

    /// A bad `[glyphs]` line must fail at config load with the slot named.
    #[test]
    fn overrides_validate_names_arity_and_width() {
        let one =
            |k: &str, v: &str| BTreeMap::from([(k.to_string(), SlotOverride::One(v.to_string()))]);
        assert!(validate_overrides(&one("in_object_store", "☁")).is_ok());
        assert!(
            validate_overrides(&one("no_such_slot", "x"))
                .is_err_and(|e| e.contains("no_such_slot"))
        );
        // ‹binary› is eight columns; a one-column override moves every layout after it.
        assert!(
            validate_overrides(&one("binary_stub", "b")).is_err_and(|e| e.contains("binary_stub"))
        );
        assert!(validate_overrides(&one("checkbox_on", "")).is_err());
        // The wordmark is not a slot.
        assert!(validate_overrides(&one("wordmark", "datui")).is_err());

        let many = |k: &str, v: &[&str]| {
            BTreeMap::from([(
                k.to_string(),
                SlotOverride::Many(v.iter().map(|s| s.to_string()).collect()),
            )])
        };
        assert!(validate_overrides(&many("spinner", &["◐", "◓", "◑", "◒"])).is_ok());
        assert!(validate_overrides(&many("spinner", &[])).is_err());
        assert!(
            validate_overrides(&many("score_marks", &["a", "b"]))
                .is_err_and(|e| e.contains("exactly 5"))
        );
        assert!(
            validate_overrides(&many("times", &["×"])).is_err_and(|e| e.contains("single string"))
        );
        assert!(validate_overrides(&one("spinner", "◐")).is_err_and(|e| e.contains("list")));
    }

    /// Overrides land on the set they name and leave every other slot alone.
    #[test]
    fn overrides_apply_over_the_unicode_set() {
        let mut set = UNICODE;
        let overrides = BTreeMap::from([
            (
                "in_object_store".to_string(),
                SlotOverride::One("☁".to_string()),
            ),
            (
                "spinner".to_string(),
                SlotOverride::Many(vec!["◐".to_string(), "◑".to_string()]),
            ),
        ]);
        validate_overrides(&overrides).expect("a valid override map");
        apply_overrides(&mut set, &overrides);
        assert_eq!(set.in_object_store, "☁");
        assert_eq!(set.spinner, &["◐", "◑"]);
        assert_eq!(set.checkbox_on, UNICODE.checkbox_on);
    }

    /// A marker that is also a letter or a space would read as part of the name.
    #[test]
    fn no_marker_could_be_mistaken_for_text() {
        for set in [unicode(), ascii()] {
            for marker in [
                set.here,
                set.in_memory,
                set.over_network,
                set.in_object_store,
                set.place_unknown,
            ] {
                let c = marker.chars().next().expect("a marker");
                assert!(
                    !c.is_alphanumeric() && !c.is_whitespace(),
                    "{marker:?} would read as part of a filename"
                );
            }
        }
    }

    /// Cells, not characters and not bytes: wide characters count two, a combining
    /// mark and a joined emoji sequence count with the character they join, and a
    /// control character counts nothing, since ratatui draws nothing for it.
    #[test]
    fn cell_width_counts_what_is_drawn() {
        assert_eq!(cell_width("tail"), 4);
        assert_eq!(cell_width("東京大阪"), 8);
        assert_eq!(cell_width("e\u{301}e\u{301}"), 2);
        assert_eq!(cell_width("line1\nline2"), 10);
        assert_eq!(cell_width("tab\tseparated"), 12);
        assert_eq!(cell_width("👩\u{200d}👩\u{200d}👧"), 2);
    }

    /// Whole when it fits, borrowed; otherwise cut at a grapheme and marked, never
    /// wider than asked, in both marker widths.
    #[test]
    fn fit_cells_clips_at_graphemes_and_marks_the_cut() {
        assert!(matches!(fit_cells("tail", 4, "…"), Cow::Borrowed("tail")));
        assert_eq!(fit_cells("abcdef", 5, "…"), "abcd…");
        assert_eq!(fit_cells("abcdef", 5, "..."), "ab...");
        assert_eq!(fit_cells("abcdef", 2, "..."), "..");
        assert_eq!(fit_cells("abcdef", 0, "…"), "");
        assert!(matches!(
            fit_cells("東京大阪", 8, "…"),
            Cow::Borrowed("東京大阪")
        ));
        assert_eq!(fit_cells("東京大阪", 7, "…"), "東京大…");
        // 京 would straddle the edge: it goes whole, and its cell stays blank.
        assert_eq!(fit_cells("東京大阪", 4, "…"), "東…");
        assert_eq!(fit_cells("東京大阪", 4, "..."), "...");
        assert_eq!(fit_cells("e\u{301}e\u{301}e\u{301}", 2, "…"), "e\u{301}…");
        assert_eq!(fit_cells("line1\nline2", 10, "…"), "line1line2");
        assert_eq!(fit_cells("line1\nline2", 6, "…"), "line1…");
    }

    /// Whatever the text and the width, the result is a run of the text's own
    /// graphemes plus the marker, within the width.
    #[test]
    fn fit_cells_never_splits_a_grapheme() {
        let samples = [
            "plain ascii text",
            "東京大阪 京都横浜",
            "e\u{301}a\u{308}o\u{302}u\u{30a}",
            "👩\u{200d}👩\u{200d}👧 family",
            "🇯🇵🇺🇸 flags",
            "mixed 名古屋 e\u{301} 👍🏽 end",
        ];
        for marker in ["…", "..."] {
            for text in samples {
                let span = ratatui::text::Span::raw(text);
                let graphemes: Vec<&str> = drawn_graphemes(&span).collect();
                for width in 0..=cell_width(text) + 1 {
                    let fitted = fit_cells(text, width, marker);
                    assert!(
                        cell_width(&fitted) <= width,
                        "{text:?} at {width}: {fitted:?} is too wide"
                    );
                    if fitted == text {
                        assert!(cell_width(text) <= width);
                        continue;
                    }
                    let kept = fitted.strip_suffix(marker).unwrap_or("");
                    let mut rebuilt = String::new();
                    for g in &graphemes {
                        if rebuilt.len() >= kept.len() {
                            break;
                        }
                        rebuilt.push_str(g);
                    }
                    assert_eq!(
                        rebuilt, kept,
                        "{text:?} at {width}: {fitted:?} cuts a grapheme"
                    );
                }
            }
        }
    }
}
