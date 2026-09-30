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
    /// Eight compact levels for inline charts (lowest to highest).
    pub mini_bars: &'static [&'static str; 8],
    /// A horizontal bar's end, one to eight eighths of a cell filled from the left;
    /// the last is a whole cell, the bar's body. One column wide in both sets.
    pub bar_eighths: &'static [&'static str; 8],
    /// The home-screen wordmark, three rows of box drawing. `None` when the terminal
    /// cannot draw it, and the one-line title bar is used instead.
    pub wordmark: Option<&'static [&'static str]>,
    /// The one border every Surface draws. Not a `[glyphs]` override slot:
    /// its eight pieces must agree with each other, and ratatui draws them.
    pub border: ratatui::symbols::border::Set<'static>,
}

const UNICODE: Glyphs = Glyphs {
    unicode: true,
    selector: "▎ ",
    selector_blank: "  ",
    cursor: "▏",
    prompt: "› ",
    rule: "│",
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
    mini_bars: &["▁", "▂", "▃", "▄", "▅", "▆", "▇", "█"],
    bar_eighths: &["▏", "▎", "▍", "▌", "▋", "▊", "▉", "█"],
    wordmark: Some(&[
        "┌──╮ ╭──╮ ╶─┬─╴ ╷  ╷ ╶┬╴",
        "│  │ ├──┤   │   │  │  │ ",
        "└──╯ ╵  ╵   ╵   ╰──╯ ╶┴╴",
    ]),
    border: ratatui::symbols::border::ROUNDED,
};

const ASCII: Glyphs = Glyphs {
    unicode: false,
    selector: "> ",
    selector_blank: "  ",
    cursor: "_",
    prompt: "> ",
    rule: "|",
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
    mini_bars: &[".", ":", "-", "=", "+", "*", "#", "@"],
    // Under half a cell draws nothing; the value beside the bar says the rest.
    bar_eighths: &[" ", " ", " ", "=", "=", "=", "=", "#"],
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
};

/// What the user asked for, from `[display] unicode`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UnicodeMode {
    /// Detect from the locale.
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
            binary_stub
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
    let mut out = String::with_capacity(text.len());
    for c in text.chars() {
        match ascii_twin(c) {
            Some(twin) => out.push_str(twin),
            None if c.is_ascii() => out.push(c),
            // A character the map does not know is marked rather than
            // shipped to a terminal that cannot draw it; the audit test
            // keeps this case from ever being reachable from a help file.
            None => out.push('?'),
        }
    }
    std::borrow::Cow::Owned(out)
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

/// Whether the environment claims a UTF-8 locale.
///
/// `LC_ALL` beats `LC_CTYPE` beats `LANG`, as in POSIX. A terminal that is not doing
/// UTF-8 renders multi-byte characters as replacement boxes, so this is the signal
/// that matters — not terminal capability, which says nothing about the font.
pub fn locale_is_utf8() -> bool {
    for key in ["LC_ALL", "LC_CTYPE", "LANG"] {
        if let Ok(value) = std::env::var(key) {
            if value.is_empty() {
                continue;
            }
            let lower = value.to_ascii_lowercase();
            return lower.contains("utf-8") || lower.contains("utf8");
        }
    }
    false
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
            if locale_is_utf8() {
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

/// The active glyph set. Falls back to locale detection when [`init`] was never
/// called, so library users and tests get sensible symbols without ceremony.
pub fn get() -> &'static Glyphs {
    GLYPHS.get_or_init(|| if locale_is_utf8() { UNICODE } else { ASCII })
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

/// The ASCII set.
pub fn ascii() -> &'static Glyphs {
    &ASCII
}

#[cfg(test)]
mod tests {
    use super::*;
    use unicode_width::UnicodeWidthStr;

    /// Every character a help file uses outside ASCII has a twin in
    /// `ascii_twin`, so the ASCII floor never sees a `?` where an
    /// instruction was.
    #[test]
    fn every_help_screen_is_ascii_clean() {
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src/help-strings");
        let mut checked = 0;
        for entry in std::fs::read_dir(dir).expect("help-strings dir") {
            let path = entry.expect("dir entry").path();
            let text = std::fs::read_to_string(&path).expect("help file");
            for c in text.chars().filter(|c| !c.is_ascii()) {
                assert!(
                    ascii_twin(c).is_some(),
                    "{path:?} uses {c:?}, which has no ASCII twin"
                );
            }
            checked += 1;
        }
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

    /// `get()` stores a copy of a const, so no address can identify the active
    /// set — the `unicode` flag on the set is what `active_is_unicode` reads.
    #[test]
    fn active_is_unicode_matches_the_chosen_set() {
        let expected = get().spinner.len() == unicode().spinner.len();
        assert_eq!(active_is_unicode(), expected);
        assert!(unicode().unicode);
        assert!(!ascii().unicode);
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
}
