//! Keeping untrusted text from becoming terminal commands.
//!
//! datui displays text it did not write: cell values, column names, filenames,
//! and parser error messages all originate in whatever file the user opened.
//! A terminal does not distinguish text from commands, so a cell containing
//! `\x1b]52;c;...\x07` is a clipboard write, and one containing `\x1b[2J` wipes
//! the screen. That is the standard vulnerability class for any program that
//! renders foreign text.
//!
//! ratatui defends against this in [`Buffer::set_stringn`], which drops
//! graphemes containing control characters. It does **not** defend against it
//! in `Span` and `Line` rendering, which is what almost everything actually
//! uses: `Span::render_ref` appends zero-width graphemes to the preceding cell,
//! and an ESC is zero-width. The crossterm backend then writes each cell symbol
//! out with `Print`, unfiltered, and the escape reaches the terminal intact.
//!
//! Sanitising at the roughly two hundred places that build a `Span` would work
//! until someone adds the two hundred and first. So the sweep happens once, at
//! the end of `App::render`, over the finished buffer. Every path into the
//! screen has converged by then, including paths added later and paths nobody
//! remembered to audit.
//!
//! [`Buffer::set_stringn`]: ratatui::buffer::Buffer::set_stringn

use ratatui::buffer::Buffer;

/// What a stripped control character is replaced with.
///
/// A visible marker rather than deletion: silently dropping bytes would let a
/// hostile value disguise itself as a different, plausible value. Seeing
/// `total: 1<?>0` is a hint that something is wrong with the data, where
/// `total: 10` is a lie.
const REPLACEMENT: char = '\u{fffd}';

/// True for characters that must never reach the terminal from untrusted text.
///
/// The C0 controls, DEL, and the C1 range, which some terminals accept as
/// single-byte equivalents of the two-byte escape sequences (`\u{009b}` for
/// CSI, for instance). Tab is allowed through: it is layout rather than
/// control, and ratatui may place one legitimately.
#[inline]
fn is_forbidden(c: char) -> bool {
    let n = c as u32;
    (n < 0x20 && c != '\t') || n == 0x7f || (0x80..=0x9f).contains(&n)
}

/// Returns a display-safe copy of `s`, or `None` if it was already safe.
///
/// The `None` case is the common one by a wide margin, and returning it avoids
/// allocating for the overwhelming majority of cells.
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

/// Replaces control characters in every cell of a finished buffer.
///
/// Call this once, after all rendering, and before the buffer is handed to a
/// backend. It is the last point at which datui controls what the terminal
/// receives.
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
