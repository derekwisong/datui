//! Keeping untrusted text (cells, column names, filenames, parser errors) from becoming
//! terminal commands: `\x1b]52;c;...\x07` in a cell writes the clipboard, `\x1b[2J` wipes
//! the screen. ratatui's [`Buffer::set_stringn`] drops control graphemes, but `Span`
//! and `Line` rendering append zero-width graphemes (ESC included) to the previous cell,
//! and crossterm prints cells unfiltered. Rather than guard every `Span` site, one sweep
//! runs at the end of `App::render` over the finished buffer.
//!
//! [`Buffer::set_stringn`]: ratatui::buffer::Buffer::set_stringn

use ratatui::buffer::Buffer;

/// What a stripped control character becomes: a visible marker, since deleting it
/// would let a hostile value pose as a plausible one (`1<?>0` vs `10`).
const REPLACEMENT: char = '\u{fffd}';

/// Characters that must never reach the terminal from untrusted text: C0 controls, DEL
/// and C1 (some terminals take `\u{009b}` as CSI). Tab is layout and allowed.
#[inline]
fn is_forbidden(c: char) -> bool {
    let n = c as u32;
    (n < 0x20 && c != '\t') || n == 0x7f || (0x80..=0x9f).contains(&n)
}

/// A display-safe copy of `s`, or `None` if already safe (the usual case, sparing an
/// allocation).
pub fn sanitized(s: &str) -> Option<String> {
    if !s.chars().any(is_forbidden) {
        return None;
    }
    Some(
        s.chars()
            .map(|c| if is_forbidden(c) { REPLACEMENT } else { c })
            .collect(),
    )
}

/// Replace control characters in every cell of a finished buffer: once, after all
/// rendering, before the backend, the last point datui controls the output.
pub fn sanitize_buffer(buf: &mut Buffer) {
    for cell in buf.content.iter_mut() {
        if let Some(clean) = sanitized(cell.symbol()) {
            cell.set_symbol(&clean);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::layout::Rect;

    #[test]
    fn leaves_ordinary_text_alone() {
        assert_eq!(sanitized("hello"), None);
        assert_eq!(sanitized(""), None);
        // Tab is layout, not control.
        assert_eq!(sanitized("a\tb"), None);
        // Non-ASCII text must survive untouched.
        assert_eq!(sanitized("héllo · 日本語 · 🦀"), None);
    }

    #[test]
    fn replaces_c0_controls() {
        assert_eq!(sanitized("\x1b[2J").unwrap(), "\u{fffd}[2J");
        assert_eq!(sanitized("a\x07b").unwrap(), "a\u{fffd}b");
        assert_eq!(sanitized("a\rb").unwrap(), "a\u{fffd}b");
        assert_eq!(sanitized("a\nb").unwrap(), "a\u{fffd}b");
        assert_eq!(sanitized("a\x00b").unwrap(), "a\u{fffd}b");
    }

    #[test]
    fn replaces_del_and_c1() {
        assert_eq!(sanitized("a\x7fb").unwrap(), "a\u{fffd}b");
        // Single-byte CSI, accepted by some terminals.
        assert_eq!(sanitized("\u{009b}31m").unwrap(), "\u{fffd}31m");
        assert_eq!(sanitized("\u{0080}").unwrap(), "\u{fffd}");
        assert_eq!(sanitized("\u{009f}").unwrap(), "\u{fffd}");
    }

    #[test]
    fn keeps_the_character_after_the_c1_range() {
        // U+00A0 is a no-break space, not a control. Off-by-one guard.
        assert_eq!(sanitized("\u{00a0}"), None);
    }

    #[test]
    fn sweeps_a_whole_buffer() {
        let mut buf = Buffer::empty(Rect::new(0, 0, 4, 1));
        buf[(0, 0)].set_symbol("a");
        buf[(1, 0)].set_symbol("\x1b");
        // A cell symbol can hold several graphemes: Span rendering appends
        // zero-width ones to the preceding cell, which is how an escape gets
        // in alongside a visible character in the first place.
        buf[(2, 0)].set_symbol("b\x1b]52;c;x\x07");
        buf[(3, 0)].set_symbol("c");

        sanitize_buffer(&mut buf);

        assert_eq!(buf[(0, 0)].symbol(), "a");
        assert_eq!(buf[(1, 0)].symbol(), "\u{fffd}");
        assert_eq!(buf[(2, 0)].symbol(), "b\u{fffd}]52;c;x\u{fffd}");
        assert_eq!(buf[(3, 0)].symbol(), "c");
    }
}
