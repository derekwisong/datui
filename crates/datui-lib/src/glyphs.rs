//! Terminal glyphs, with an ASCII fallback.
//!
//! datui runs on a modern desktop terminal *and* over SSH on a plain server with a
//! bitmap font and a C locale. Neither should be the one that suffers: on a capable
//! terminal the box-drawing and arrow characters carry real meaning, and on a
//! limited one they turn into replacement boxes that make the UI harder to read
//! rather than prettier.
//!
//! Nothing here is a Nerd Font glyph. These are plain Unicode from blocks that any
//! UTF-8 font covers; Nerd Font icons appear only in the Omarchy menu definition,
//! where the font is guaranteed. The fallback exists for terminals that are not
//! doing UTF-8 at all.

use std::sync::OnceLock;

/// Symbols used by the UI, in whichever alphabet the terminal can render.
#[derive(Debug, Clone, Copy)]
pub struct Glyphs {
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
    /// Keycap names for the control bar.
    pub enter: &'static str,
    pub backspace: &'static str,
    pub updown: &'static str,
    /// Left/right pair, for the fold hint.
    pub updown_lr: &'static str,
    /// Tab key, for the sort hint.
    pub tab: &'static str,
}

const UNICODE: Glyphs = Glyphs {
    selector: "▸ ",
    selector_blank: "  ",
    cursor: "▏",
    prompt: "› ",
    rule: "│",
    ellipsis: "…",
    times: "×",
    collapsed: "▸ ",
    expanded: "▾ ",
    enter: "⏎",
    backspace: "⌫",
    updown: "↑↓",
    updown_lr: "←→",
    tab: "⇥",
};

const ASCII: Glyphs = Glyphs {
    selector: "> ",
    selector_blank: "  ",
    cursor: "_",
    prompt: "> ",
    rule: "|",
    ellipsis: "...",
    times: "x",
    collapsed: "+ ",
    expanded: "- ",
    enter: "Enter",
    backspace: "Bksp",
    updown: "Up/Dn",
    updown_lr: "Lt/Rt",
    tab: "Tab",
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
    let chosen = match mode {
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
    let _ = GLYPHS.set(chosen);
}

/// The active glyph set. Falls back to locale detection when [`init`] was never
/// called, so library users and tests get sensible symbols without ceremony.
pub fn get() -> &'static Glyphs {
    GLYPHS.get_or_init(|| if locale_is_utf8() { UNICODE } else { ASCII })
}

/// The Unicode set, for tests and for callers that know their output is UTF-8.
pub fn unicode() -> &'static Glyphs {
    &UNICODE
}

/// The ASCII set.
pub fn ascii() -> &'static Glyphs {
    &ASCII
}
