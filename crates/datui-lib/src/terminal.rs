//! The terminal while the TUI holds it: setting it up, handing it back, and letting
//! it go quietly once it has gone.

/// Undo `run`'s terminal setup: let go of the mouse, pop the keyboard flags (a
/// no-op where they were never pushed; a terminal that ignored the push ignores the
/// pop too), then hand back the screen.
///
/// Says so on stderr if that fails, but never panics: after a hangup the terminal is
/// gone, and `ratatui::restore`'s `eprintln!` would panic on it, then panic again in
/// the panic hook and abort.
pub(crate) fn restore_terminal() {
    use std::io::Write;
    let_go(&mut std::io::stdout());
    if let Err(e) = ratatui::try_restore() {
        let _ = writeln!(std::io::stderr(), "Failed to restore terminal: {e}");
    }
}

/// Turn off what the session may have asked of the terminal, whether or not it did:
/// a terminal ignores turning off what is not on.
fn let_go(out: &mut impl std::io::Write) {
    let _ = crossterm::execute!(out, crossterm::event::DisableMouseCapture);
    // Focus reports, asked for under `theme.mode = "auto"`.
    let _ = crossterm::execute!(out, crossterm::event::DisableFocusChange);
    let _ = crossterm::execute!(out, crossterm::event::PopKeyboardEnhancementFlags);
}

/// Ask the terminal to tell Ctrl+Enter from Enter.
///
/// Without the kitty keyboard protocol the two are byte-identical. Disambiguation
/// alone fixes that — plain Enter, Tab and Backspace keep their legacy encodings — and
/// the terminal keeps a separate flag stack for the alternate screen, so leaving it on
/// exit or panic restores the shell's keyboard either way.
///
/// Pushed without asking first. Asking means waiting for an answer, and a terminal
/// that never answers held the first frame for Crossterm's two-second timeout; nor can
/// the answer be read off to one side, because Crossterm's one parser consumes it. A
/// terminal without the protocol ignores the push, as it ignores the pop that every
/// exit has always sent: both are private-marker CSI sequences, which terminals that
/// do not know them discard. Keys then arrive in the legacy encoding, which Crossterm
/// reads either way.
pub(crate) fn push_keyboard_flags() {
    let _ = crossterm::execute!(
        std::io::stdout(),
        crossterm::event::PushKeyboardEnhancementFlags(
            crossterm::event::KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
        )
    );
}

/// Ask the terminal to report focus (`CSI ? 1004 h`), so the palette can follow a
/// scheme changed while datui was in the background: on focus the background is asked
/// again. A terminal without focus reports ignores it; [`restore_terminal`] turns it
/// off on every way out.
pub(crate) fn follow_focus(out: &mut impl std::io::Write) {
    let _ = crossterm::execute!(out, crossterm::event::EnableFocusChange);
}

/// Ratatui's terminal, let go without its `Drop` when the terminal has gone. That
/// `Drop` shows the cursor and `eprintln!`s a failure, which after a hangup panics,
/// panics again in the panic hook, and aborts.
pub(crate) struct QuietTerminal(pub(crate) Option<ratatui::DefaultTerminal>);

impl QuietTerminal {
    pub(crate) fn get(&mut self) -> &mut ratatui::DefaultTerminal {
        self.0.as_mut().expect("held until dropped")
    }
}

impl Drop for QuietTerminal {
    fn drop(&mut self) {
        if let Some(mut terminal) = self.0.take()
            && terminal.show_cursor().is_err()
        {
            std::mem::forget(terminal);
        }
    }
}

/// The terminal while the TUI owns it. Dropped without `run::conclude` — an error
/// returned with `?` — it hands the screen back; a panic is the session's to handle,
/// and restoring twice would pop the shell's keyboard flags.
pub(crate) struct TakenTerminal {
    pub(crate) restored: bool,
}

impl TakenTerminal {
    pub(crate) fn restore(&mut self) {
        if !std::mem::replace(&mut self.restored, true) {
            restore_terminal();
        }
    }
}

impl Drop for TakenTerminal {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.restore();
        }
    }
}

// Unix only: on a Windows console Crossterm sends these through the console API, not
// as bytes.
#[cfg(all(test, unix))]
mod tests {
    use super::*;

    /// Handing the terminal back turns off focus reports, which `auto` turns on.
    #[test]
    fn letting_go_turns_off_focus_reports() {
        let mut out = Vec::new();
        let_go(&mut out);
        let out = String::from_utf8(out).unwrap();
        assert!(out.contains("\x1b[?1004l"), "{out:?}");
        assert!(out.contains("\x1b[<1u"), "{out:?}");

        let mut on = Vec::new();
        follow_focus(&mut on);
        assert_eq!(on, b"\x1b[?1004h");
    }
}
