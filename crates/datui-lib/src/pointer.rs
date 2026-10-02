//! The mouse: what a click or a turn of the wheel means where it lands.
//!
//! Keys stay the interface and the mouse is a shortcut to them. The wheel presses
//! the arrows of whatever has the keys, a click on a control-bar chip presses its key,
//! and a click on the table or the home list moves the cursor there, a double click
//! then pressing Enter. Nothing here changes what a key does.
//!
//! A pointer is aimed at what is on screen when it is used, so mouse input is never
//! held for later the way typed keys are: where a typed key would wait, a mouse event
//! is dropped (see [`crate::event_pump::EventPump::terminal_mouse`]).

use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};

use crate::widgets::datatable::CellHit;
use crate::{App, InputMode};

/// Rows a notch of the wheel moves.
pub const WHEEL_ROWS: usize = 3;

/// Two clicks on one cell this close together are a double click.
pub const DOUBLE_CLICK: Duration = Duration::from_millis(500);

/// The mouse events the app acts on: a left press and the wheel. Motion, drags,
/// releases and the other buttons are left alone.
pub fn wanted(mouse: &MouseEvent) -> bool {
    matches!(
        mouse.kind,
        MouseEventKind::Down(MouseButton::Left)
            | MouseEventKind::ScrollDown
            | MouseEventKind::ScrollUp
            | MouseEventKind::ScrollLeft
            | MouseEventKind::ScrollRight
    )
}

/// Ask the terminal for presses and the wheel, SGR-encoded so a wide screen's far
/// columns report right. Not crossterm's `EnableMouseCapture`, which also asks for
/// every motion: a stream of events the app would only throw away. Undone by
/// `DisableMouseCapture`, which turns off every mode.
pub struct EnableMouse;

impl crossterm::Command for EnableMouse {
    fn write_ansi(&self, f: &mut impl std::fmt::Write) -> std::fmt::Result {
        f.write_str("\x1b[?1000h\x1b[?1006h")
    }

    #[cfg(windows)]
    fn execute_winapi(&self) -> std::io::Result<()> {
        crossterm::Command::execute_winapi(&crossterm::event::EnableMouseCapture)
    }

    #[cfg(windows)]
    fn is_ansi_code_supported(&self) -> bool {
        crossterm::Command::is_ansi_code_supported(&crossterm::event::EnableMouseCapture)
    }
}

/// What a mouse event comes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pointer {
    Nothing,
    /// Press these keys in order, as if typed, as far as typed keys would act now.
    Keys(Vec<KeyEvent>),
    /// Move to what was clicked, then press the key if there is one (a double
    /// click's Enter).
    Point(Target, Option<KeyEvent>),
}

/// Where a click or the wheel moves the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A cell of the table as drawn.
    Table(CellHit),
    /// A row of the home list, by its place among those listed.
    HomeRow(usize),
    /// The home list's selection, moved this many rows and stopped at the ends: the
    /// arrows there go round, which a wheel must not.
    HomeStep(isize),
}

/// What the last frame drew where, and the last click.
#[derive(Debug, Default)]
pub struct Pointing {
    /// The control bar's chips and the key each presses.
    chips: Vec<(Rect, KeyEvent)>,
    /// The home list: its area and the row on each of its lines.
    home_list: Option<(Rect, Vec<Option<usize>>)>,
    last_click: Option<(u16, u16, Instant)>,
}

impl Pointing {
    /// A frame begins: what it does not draw cannot be clicked.
    pub fn forget_drawn(&mut self) {
        self.chips.clear();
        self.home_list = None;
    }

    /// The control bar's chips as drawn. A chip whose key is not one key (`↑↓`)
    /// presses nothing.
    pub fn chips_drawn(&mut self, chips: Vec<(Rect, &str)>) {
        self.chips = chips
            .into_iter()
            .filter_map(|(rect, key)| chip_key(key).map(|key| (rect, key)))
            .collect();
    }

    /// The home list as drawn: `lines[i]` is the row on the list's line `i`.
    pub fn home_list_drawn(&mut self, area: Rect, lines: Vec<Option<usize>>) {
        self.home_list = Some((area, lines));
    }

    fn chip_at(&self, at: Position) -> Option<KeyEvent> {
        self.chips
            .iter()
            .find(|(rect, _)| rect.contains(at))
            .map(|(_, key)| *key)
    }

    fn home_row_at(&self, at: Position) -> Option<usize> {
        let (area, lines) = self.home_list.as_ref()?;
        if !area.contains(at) {
            return None;
        }
        lines.get(usize::from(at.y - area.y)).copied().flatten()
    }

    /// Whether a click at `at` is the second of a double click. A third click starts
    /// over, so a triple click is a double click and then a click.
    fn second_click(&mut self, at: Position, now: Instant) -> bool {
        let double = self.last_click.is_some_and(|(x, y, then)| {
            (x, y) == (at.x, at.y) && now.saturating_duration_since(then) <= DOUBLE_CLICK
        });
        self.last_click = if double {
            None
        } else {
            Some((at.x, at.y, now))
        };
        double
    }
}

/// The key a chip's label names, when it names one key: `Enter`, `^O`, `q`, `F1`.
pub fn chip_key(label: &str) -> Option<KeyEvent> {
    let named = |code| Some(KeyEvent::new(code, KeyModifiers::NONE));
    match label {
        "Enter" => return named(KeyCode::Enter),
        "Esc" => return named(KeyCode::Esc),
        "Tab" => return named(KeyCode::Tab),
        "Space" => return named(KeyCode::Char(' ')),
        "F1" => return named(KeyCode::F(1)),
        _ => {}
    }
    let mut chars = label.chars();
    match (chars.next(), chars.next(), chars.next()) {
        (Some('^'), Some(c), None) if c.is_ascii_alphabetic() => Some(KeyEvent::new(
            KeyCode::Char(c.to_ascii_lowercase()),
            KeyModifiers::CONTROL,
        )),
        // As a terminal reports it: an upper-case letter comes with Shift.
        (Some(c), None, None) if c.is_ascii_uppercase() => {
            Some(KeyEvent::new(KeyCode::Char(c), KeyModifiers::SHIFT))
        }
        (Some(c), None, None) if c.is_ascii_graphic() => named(KeyCode::Char(c)),
        _ => None,
    }
}

impl App {
    /// What a mouse event means on the screen last drawn.
    pub fn pointer(&mut self, mouse: &MouseEvent, now: Instant) -> Pointer {
        let shift = mouse.modifiers.contains(KeyModifiers::SHIFT);
        match mouse.kind {
            MouseEventKind::ScrollDown if shift => self.wheel_across(true),
            MouseEventKind::ScrollUp if shift => self.wheel_across(false),
            MouseEventKind::ScrollRight => self.wheel_across(true),
            MouseEventKind::ScrollLeft => self.wheel_across(false),
            MouseEventKind::ScrollDown => self.wheel(true),
            MouseEventKind::ScrollUp => self.wheel(false),
            MouseEventKind::Down(MouseButton::Left) => self.click(
                Position {
                    x: mouse.column,
                    y: mouse.row,
                },
                now,
            ),
            _ => Pointer::Nothing,
        }
    }

    /// Forget the last click: it was dropped, so the next is not its second.
    pub fn forget_click(&mut self) {
        self.pointer.last_click = None;
    }

    /// Move the cursor to what a click or the wheel landed on.
    pub fn point(&mut self, target: &Target) {
        // As a key does: the line about the last action is stale now.
        self.flash = None;
        match target {
            Target::Table(hit) => {
                if let Some(state) = self.data_table_state.as_mut() {
                    state.point_at(hit);
                }
            }
            Target::HomeRow(row) => {
                self.home.status = None;
                self.home.select(*row);
            }
            Target::HomeStep(rows) => {
                self.home.status = None;
                self.home.page_selection(*rows);
            }
        }
    }

    /// The home screen is up with nothing over it, so its keys go to it.
    fn home_has_the_keys(&self) -> bool {
        self.input_mode == InputMode::Home
            && !self.show_help
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// The wheel: the arrows of whatever has the keys. Not where ↑↓ walk a text
    /// field's history, and not in the prompt for a path on the home screen.
    fn wheel(&self, down: bool) -> Pointer {
        if self.home_has_the_keys() {
            if self.home.path_input_active {
                return Pointer::Nothing;
            }
            let rows = WHEEL_ROWS as isize;
            return Pointer::Point(Target::HomeStep(if down { rows } else { -rows }), None);
        }
        if self.text_field_focused() {
            return Pointer::Nothing;
        }
        let code = if down { KeyCode::Down } else { KeyCode::Up };
        Pointer::Keys(vec![KeyEvent::new(code, KeyModifiers::NONE); WHEEL_ROWS])
    }

    /// The wheel across, or Shift with it: the column cursor, at the table only.
    /// Elsewhere ←→ switch tabs or step rows, which a sideways scroll should not.
    fn wheel_across(&self, right: bool) -> Pointer {
        if !self.in_normal_table_view() || self.data_table_state.is_none() {
            return Pointer::Nothing;
        }
        let code = if right { KeyCode::Right } else { KeyCode::Left };
        Pointer::Keys(vec![KeyEvent::new(code, KeyModifiers::NONE)])
    }

    fn click(&mut self, at: Position, now: Instant) -> Pointer {
        if let Some(key) = self.pointer.chip_at(at) {
            self.pointer.last_click = None;
            return Pointer::Keys(vec![key]);
        }
        let double = self.pointer.second_click(at, now);
        let enter = double.then(|| KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
        if self.home_has_the_keys() {
            return match self.pointer.home_row_at(at) {
                Some(row) if !self.home.path_input_active => {
                    Pointer::Point(Target::HomeRow(row), enter)
                }
                _ => Pointer::Nothing,
            };
        }
        if self.in_normal_table_view()
            && let Some(hit) = self
                .data_table_state
                .as_ref()
                .and_then(|state| state.drawn_cell(at.x, at.y))
        {
            // Enter acts on a row; a double click on the header only picks the column.
            let enter = enter.filter(|_| hit.row.is_some());
            return Pointer::Point(Target::Table(hit), enter);
        }
        Pointer::Nothing
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_chip_presses_the_key_it_names() {
        let key = |code, modifiers| Some(KeyEvent::new(code, modifiers));
        assert_eq!(chip_key("Enter"), key(KeyCode::Enter, KeyModifiers::NONE));
        assert_eq!(
            chip_key("^O"),
            key(KeyCode::Char('o'), KeyModifiers::CONTROL)
        );
        assert_eq!(chip_key("q"), key(KeyCode::Char('q'), KeyModifiers::NONE));
        assert_eq!(chip_key("Q"), key(KeyCode::Char('Q'), KeyModifiers::SHIFT));
        assert_eq!(chip_key("/"), key(KeyCode::Char('/'), KeyModifiers::NONE));
        assert_eq!(chip_key("F1"), key(KeyCode::F(1), KeyModifiers::NONE));
        // Two keys, or a glyph standing for a pair: nothing to press.
        assert_eq!(chip_key("↑↓"), None);
        assert_eq!(chip_key("h/l"), None);
        assert_eq!(chip_key("PgUp"), None);
    }

    #[test]
    fn a_second_click_on_the_same_cell_soon_after_is_a_double_click() {
        let mut p = Pointing::default();
        let at = Position { x: 4, y: 7 };
        let t = Instant::now();
        assert!(!p.second_click(at, t));
        assert!(p.second_click(at, t + Duration::from_millis(200)));
        // A third starts over.
        assert!(!p.second_click(at, t + Duration::from_millis(300)));
        // Too late, or somewhere else.
        assert!(!p.second_click(at, t + Duration::from_secs(2)));
        assert!(!p.second_click(Position { x: 5, y: 7 }, t + Duration::from_secs(2)));
    }

    #[test]
    fn only_a_left_press_and_the_wheel_are_wanted() {
        let mouse = |kind| MouseEvent {
            kind,
            column: 0,
            row: 0,
            modifiers: KeyModifiers::NONE,
        };
        assert!(wanted(&mouse(MouseEventKind::Down(MouseButton::Left))));
        assert!(wanted(&mouse(MouseEventKind::ScrollUp)));
        assert!(wanted(&mouse(MouseEventKind::ScrollRight)));
        assert!(!wanted(&mouse(MouseEventKind::Down(MouseButton::Right))));
        assert!(!wanted(&mouse(MouseEventKind::Up(MouseButton::Left))));
        assert!(!wanted(&mouse(MouseEventKind::Drag(MouseButton::Left))));
        assert!(!wanted(&mouse(MouseEventKind::Moved)));
    }
}
