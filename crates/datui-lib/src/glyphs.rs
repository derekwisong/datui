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
    /// Keycap names for the control bar. Named keys are spelled out there, matching
    /// the rest of datui, so only the arrows need a fallback.
    pub updown: &'static str,
    /// Left/right pair, for the fold hint.
    pub updown_lr: &'static str,
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
    updown: "↑↓",
    updown_lr: "←→",
    spinner: &["⣷", "⣯", "⣟", "⡿", "⢿", "⣻", "⣽", "⣾"],
    // Plain Unicode from blocks any UTF-8 font covers, and each one Neutral width
    // rather than Ambiguous, so an East Asian locale does not render them double-wide
    // and knock every name out of line.
    here: "◦",
    in_memory: "▪",
    over_network: "⇅",
    in_object_store: "☁",
    place_unknown: "◌",
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
    updown: "Up/Dn",
    updown_lr: "Lt/Rt",
    spinner: &["|", "/", "-", "\\"],
    here: ".",
    in_memory: "*",
    over_network: "~",
    in_object_store: "@",
    place_unknown: "?",
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

#[cfg(test)]
mod tests {
    use super::*;
    use unicode_width::UnicodeWidthStr;

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
        ] {
            assert_eq!(
                UnicodeWidthStr::width(left),
                UnicodeWidthStr::width(right),
                "{left:?} and {right:?} are different widths"
            );
        }
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
