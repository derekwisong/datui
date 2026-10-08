//! The terminal while the TUI holds it: setting it up, handing it back, and letting
//! it go quietly once it has gone.

use super::draw::Drawer;

/// Undo `run`'s terminal setup: release the mouse, pop keyboard flags (harmless if never
/// pushed or ignored), restore the screen. Reports failures on stderr without panicking:
/// after a hangup `ratatui::restore`'s `eprintln!` would panic twice and abort.
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

/// Ask the terminal to tell Ctrl+Enter from Enter (identical bytes without the kitty
/// protocol). Disambiguation alone keeps Enter, Tab and Backspace legacy-encoded, and
/// the alternate screen has its own flag stack, so exit restores the shell either way.
/// Pushed without querying: a query waits (two seconds on a silent terminal) and its
/// answer would be eaten by Crossterm's parser. Terminals without the protocol discard
/// the private-marker CSI, and Crossterm reads legacy keys anyway.
pub(crate) fn push_keyboard_flags() {
    let _ = crossterm::execute!(
        std::io::stdout(),
        crossterm::event::PushKeyboardEnhancementFlags(
            crossterm::event::KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES
        )
    );
}

/// Ask for focus reports (`CSI ? 1004 h`) so the background is asked again on focus.
/// Ignored where unsupported; [`restore_terminal`] turns it off on every exit.
pub(crate) fn follow_focus(out: &mut impl std::io::Write) {
    let _ = crossterm::execute!(out, crossterm::event::EnableFocusChange);
}

/// Ratatui's terminal, let go without its `Drop` when the terminal has gone. That
/// `Drop` shows the cursor and `eprintln!`s a failure, which after a hangup panics,
/// panics again in the panic hook, and aborts. Frames go through its [`Drawer`].
pub(crate) struct QuietTerminal(pub(crate) Option<ratatui::DefaultTerminal>, Drawer);

impl QuietTerminal {
    pub(crate) fn new(terminal: ratatui::DefaultTerminal) -> Self {
        // Off until the settings say otherwise.
        Self(Some(terminal), Drawer::new(false))
    }

    pub(crate) fn draw(&mut self, render: impl FnOnce(&mut ratatui::Frame)) -> std::io::Result<()> {
        let terminal = self.0.as_mut().expect("held until dropped");
        self.1.draw(terminal, render)
    }

    /// Clear the screen, after another program had it; the next frame is drawn whole.
    pub(crate) fn clear(&mut self) -> std::io::Result<()> {
        let terminal = self.0.as_mut().expect("held until dropped");
        self.1.clear(terminal)
    }

    /// `display.scroll_region`.
    pub(crate) fn scroll_with_region(&mut self, on: bool) {
        self.1.set_scroll(on);
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
