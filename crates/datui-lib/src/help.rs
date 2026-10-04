//! The help overlay: one screen's keys from the key registry
//! ([`datui_cli::keys`]), grouped by task, the keys of every screen last.
//!
//! `/` narrows the lines to those whose key, label or description holds what is
//! typed. Enter closes the help and presses the key on the selected line, as the
//! next key the app takes: [`Help::key`] answers [`HelpKey::Press`], and the App
//! hands that key back to the event loop as its follow-up `AppEvent::Key`, so it
//! takes the path a typed key takes (held while busy, replayed in order) rather
//! than a parallel set of actions.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui_cli::keys::{self, Chord, Code, Context, Key};

/// What a line of the help holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Line {
    /// A key, which Enter can press.
    Key(&'static Key),
    /// An example and what it means: the q summary on the query screen.
    Note(&'static str, &'static str),
}

/// One group's lines, under its name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Block {
    pub name: &'static str,
    pub lines: Vec<Line>,
}

/// The overlay's state: open over a context, or closed.
#[derive(Debug, Default)]
pub struct Help {
    context: Option<Context>,
    /// What `/` narrowed the lines to.
    pub(crate) filter: String,
    /// Whether typed characters go to the filter.
    pub(crate) filtering: bool,
    /// The selected key, among the keys shown.
    pub(crate) selected: usize,
    /// The first line drawn; the render keeps the selection in view and writes it back.
    pub(crate) scroll: usize,
}

/// What a key typed at the help did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HelpKey {
    /// Handled inside the help.
    Stay,
    /// The help closed.
    Closed,
    /// The help closed; press this key where it was opened.
    Press(KeyEvent),
}

impl Help {
    pub fn is_open(&self) -> bool {
        self.context.is_some()
    }

    /// The screen whose keys are shown.
    pub fn context(&self) -> Option<Context> {
        self.context
    }

    pub fn open(&mut self, context: Context) {
        *self = Help {
            context: Some(context),
            ..Help::default()
        };
    }

    pub fn close(&mut self) {
        *self = Help::default();
    }

    /// The overlay's title: `Table Help`.
    pub fn title(&self) -> String {
        self.context
            .map(|c| format!("{} Help", keys::screen(c).title))
            .unwrap_or_default()
    }

    /// The blocks shown: the screen's groups, the q summary on the query screen, then
    /// the keys of every screen; each narrowed by the filter, empty ones left out.
    pub fn blocks(&self) -> Vec<Block> {
        let Some(context) = self.context else {
            return Vec::new();
        };
        let needle = self.filter.to_lowercase();
        let keeps = |text: String| needle.is_empty() || text.to_lowercase().contains(&needle);
        let mut blocks: Vec<Block> = Vec::new();
        let mut push = |name: &'static str, lines: Vec<Line>| {
            if !lines.is_empty() {
                blocks.push(Block { name, lines });
            }
        };
        let key_lines = |group: &'static keys::Group| -> Vec<Line> {
            group
                .keys
                .iter()
                .filter(|k| keeps(format!("{} {} {}", k.keys, k.label, k.line)))
                .map(Line::Key)
                .collect()
        };
        for group in keys::screen(context).groups {
            push(group.name, key_lines(group));
        }
        if context == Context::Query {
            let notes = keys::Q_SUMMARY
                .iter()
                .filter(|(example, meaning)| keeps(format!("{example} {meaning}")))
                .map(|&(example, meaning)| Line::Note(example, meaning))
                .collect();
            push("q syntax", notes);
        }
        push(keys::GLOBAL.name, key_lines(&keys::GLOBAL));
        blocks
    }

    /// The keys shown, in order: what ↑↓ walk.
    pub fn shown_keys(&self) -> Vec<&'static Key> {
        self.blocks()
            .into_iter()
            .flat_map(|b| b.lines)
            .filter_map(|line| match line {
                Line::Key(key) => Some(key),
                Line::Note(..) => None,
            })
            .collect()
    }

    /// The selected key, if any is shown.
    pub fn selected_key(&self) -> Option<&'static Key> {
        let shown = self.shown_keys();
        shown
            .get(self.selected.min(shown.len().saturating_sub(1)))
            .copied()
    }

    fn step(&mut self, by: isize) {
        let count = self.shown_keys().len();
        if count == 0 {
            self.selected = 0;
            return;
        }
        self.selected = self
            .selected
            .min(count - 1)
            .saturating_add_signed(by)
            .min(count - 1);
    }

    /// Enter: close, and press the selected line's key if it has one.
    fn run(&mut self) -> HelpKey {
        let Some(chord) = self.selected_key().and_then(Key::action) else {
            return HelpKey::Stay;
        };
        self.close();
        HelpKey::Press(key_event(chord))
    }

    /// A key typed while the help is open.
    pub fn key(&mut self, event: &KeyEvent) -> HelpKey {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        match event.code {
            KeyCode::Enter => return self.run(),
            KeyCode::Up => self.step(-1),
            KeyCode::Down => self.step(1),
            KeyCode::PageUp => self.step(-10),
            KeyCode::PageDown => self.step(10),
            KeyCode::Char('p') if ctrl => self.step(-1),
            KeyCode::Char('n') if ctrl => self.step(1),
            KeyCode::Esc => {
                if self.filtering || !self.filter.is_empty() {
                    self.filter.clear();
                    self.filtering = false;
                    self.selected = 0;
                } else {
                    self.close();
                    return HelpKey::Closed;
                }
            }
            KeyCode::F(1) => {
                self.close();
                return HelpKey::Closed;
            }
            _ if self.filtering => match event.code {
                KeyCode::Backspace => {
                    self.filter.pop();
                    self.selected = 0;
                }
                KeyCode::Char('u') if ctrl => {
                    self.filter.clear();
                    self.selected = 0;
                }
                KeyCode::Char(c)
                    if !event
                        .modifiers
                        .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT) =>
                {
                    self.filter.push(c);
                    self.selected = 0;
                }
                _ => {}
            },
            KeyCode::Char('/') => self.filtering = true,
            KeyCode::Char('?') => {
                self.close();
                return HelpKey::Closed;
            }
            KeyCode::Char('k') => self.step(-1),
            KeyCode::Char('j') => self.step(1),
            KeyCode::Home | KeyCode::Char('g') => self.selected = 0,
            KeyCode::End | KeyCode::Char('G') => self.step(isize::MAX),
            _ => {}
        }
        HelpKey::Stay
    }
}

/// The key event a chord stands for, as a terminal reports it.
pub fn key_event(chord: Chord) -> KeyEvent {
    let code = match chord.code {
        Code::Char(c) => KeyCode::Char(c),
        Code::Enter => KeyCode::Enter,
        Code::Esc => KeyCode::Esc,
        Code::Tab => KeyCode::Tab,
        Code::BackTab => KeyCode::BackTab,
        Code::Backspace => KeyCode::Backspace,
        Code::Delete => KeyCode::Delete,
        Code::Insert => KeyCode::Insert,
        Code::Up => KeyCode::Up,
        Code::Down => KeyCode::Down,
        Code::Left => KeyCode::Left,
        Code::Right => KeyCode::Right,
        Code::PageUp => KeyCode::PageUp,
        Code::PageDown => KeyCode::PageDown,
        Code::Home => KeyCode::Home,
        Code::End => KeyCode::End,
        Code::F(n) => KeyCode::F(n),
    };
    let mut modifiers = KeyModifiers::NONE;
    if chord.ctrl {
        modifiers |= KeyModifiers::CONTROL;
    }
    if chord.alt {
        modifiers |= KeyModifiers::ALT;
    }
    if chord.shift {
        modifiers |= KeyModifiers::SHIFT;
    }
    KeyEvent::new(code, modifiers)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn press(help: &mut Help, code: KeyCode) -> HelpKey {
        help.key(&KeyEvent::new(code, KeyModifiers::NONE))
    }

    fn type_text(help: &mut Help, text: &str) {
        for c in text.chars() {
            press(help, KeyCode::Char(c));
        }
    }

    #[test]
    fn every_screen_ends_with_the_keys_of_every_screen() {
        for screen in keys::SCREENS {
            let mut help = Help::default();
            help.open(screen.context);
            let blocks = help.blocks();
            assert_eq!(blocks.last().map(|b| b.name), Some(keys::GLOBAL.name));
            assert!(blocks.len() > 1, "{}", screen.title);
        }
    }

    #[test]
    fn the_query_screen_carries_the_q_summary() {
        let mut help = Help::default();
        help.open(Context::Query);
        assert!(help.blocks().iter().any(|b| b.name == "q syntax"));
        help.open(Context::Table);
        assert!(!help.blocks().iter().any(|b| b.name == "q syntax"));
    }

    #[test]
    fn slash_narrows_by_key_and_description() {
        let mut help = Help::default();
        help.open(Context::Table);
        let all = help.shown_keys().len();
        press(&mut help, KeyCode::Char('/'));
        type_text(&mut help, "value counts");
        let shown = help.shown_keys();
        assert_eq!(shown.len(), 1, "{shown:?}");
        assert_eq!(shown[0].keys, "F");
        // Backspace takes back a character.
        press(&mut help, KeyCode::Backspace);
        type_text(&mut help, "s");
        assert_eq!(help.filter, "value counts");
        // Esc clears the filter, then closes.
        assert_eq!(press(&mut help, KeyCode::Esc), HelpKey::Stay);
        assert_eq!(help.shown_keys().len(), all);
        assert!(help.is_open());
        assert_eq!(press(&mut help, KeyCode::Esc), HelpKey::Closed);
        assert!(!help.is_open());
    }

    #[test]
    fn a_filter_that_matches_nothing_shows_nothing() {
        let mut help = Help::default();
        help.open(Context::Table);
        press(&mut help, KeyCode::Char('/'));
        type_text(&mut help, "zzzzzz");
        assert!(help.blocks().is_empty());
        assert_eq!(press(&mut help, KeyCode::Enter), HelpKey::Stay);
        assert!(help.is_open());
    }

    #[test]
    fn enter_closes_and_presses_the_selected_key() {
        let mut help = Help::default();
        help.open(Context::Table);
        press(&mut help, KeyCode::Char('/'));
        type_text(&mut help, "value counts");
        let pressed = press(&mut help, KeyCode::Enter);
        assert_eq!(
            pressed,
            HelpKey::Press(KeyEvent::new(KeyCode::Char('F'), KeyModifiers::SHIFT))
        );
        assert!(!help.is_open());
    }

    #[test]
    fn arrows_walk_the_keys_and_stop_at_the_ends() {
        let mut help = Help::default();
        help.open(Context::GoToRow);
        let count = help.shown_keys().len();
        press(&mut help, KeyCode::Up);
        assert_eq!(help.selected, 0);
        press(&mut help, KeyCode::Char('j'));
        assert_eq!(help.selected, 1);
        press(&mut help, KeyCode::End);
        assert_eq!(help.selected, count - 1);
        press(&mut help, KeyCode::Down);
        assert_eq!(help.selected, count - 1);
        press(&mut help, KeyCode::Home);
        assert_eq!(help.selected, 0);
    }

    /// A line with nothing to press (typing, the mouse) keeps the help open.
    #[test]
    fn enter_on_a_line_with_no_key_stays() {
        let mut help = Help::default();
        help.open(Context::GoToRow);
        assert_eq!(help.selected_key().map(|k| k.keys), Some("(digits)"));
        assert_eq!(press(&mut help, KeyCode::Enter), HelpKey::Stay);
        assert!(help.is_open());
    }
}
