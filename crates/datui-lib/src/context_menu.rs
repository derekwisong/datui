//! The menu a right click on a table cell opens: the keys that act on a cell, each
//! line naming its key. A line chosen closes the menu and presses its key, so the
//! menu does exactly what typing the key does. A menu is not a dialog: a click
//! anywhere else closes it.

use crossterm::event::{KeyCode, KeyEvent};
use ratatui::buffer::Buffer;
use ratatui::layout::{Position, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

use crate::pointer::{self, Hit};
use crate::render::context::RenderContext;
use crate::widgets::ui::Surface;

/// One line: the key it presses, as the key registry spells it, and what it does
/// to the cell.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MenuItem {
    pub key: &'static str,
    pub label: &'static str,
}

/// The cell's keys, in the order the menu lists them.
pub const ITEMS: &[MenuItem] = &[
    MenuItem {
        key: "+",
        label: "Filter to this value",
    },
    MenuItem {
        key: "-",
        label: "Filter out this value",
    },
    MenuItem {
        key: "F",
        label: "Value counts",
    },
    MenuItem {
        key: "[",
        label: "Sort ascending",
    },
    MenuItem {
        key: "]",
        label: "Sort descending",
    },
    MenuItem {
        key: "y",
        label: "Copy",
    },
    MenuItem {
        key: "Space",
        label: "Inspect row",
    },
];

impl MenuItem {
    /// The key this line presses, as typed.
    pub fn key_event(&self) -> KeyEvent {
        let chord = datui_cli::keys::chord(self.key).expect("a menu key is one key");
        crate::help::key_event(chord)
    }
}

/// An open menu: where it was asked for and the line the cursor is on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContextMenu {
    pub at: Position,
    pub selected: usize,
}

/// What a key does in the open menu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MenuKey {
    /// Moved the cursor.
    Moved,
    /// Close the menu and press this key.
    Run(KeyEvent),
    /// Esc: close it.
    Close,
    /// Not the menu's: close it, and the key acts as it would have.
    Other,
}

impl ContextMenu {
    pub fn new(at: Position) -> Self {
        Self { at, selected: 0 }
    }

    /// ↑ / ↓ (k / j) move, wrapping; Enter runs the line; Esc closes. Any other key,
    /// a line's own included, leaves the menu and acts as typed.
    pub fn key(&mut self, event: &KeyEvent) -> MenuKey {
        let n = ITEMS.len();
        if !event.modifiers.is_empty() {
            return MenuKey::Other;
        }
        match event.code {
            KeyCode::Up | KeyCode::Char('k') => {
                self.selected = (self.selected + n - 1) % n;
                MenuKey::Moved
            }
            KeyCode::Down | KeyCode::Char('j') => {
                self.selected = (self.selected + 1) % n;
                MenuKey::Moved
            }
            KeyCode::Enter => MenuKey::Run(ITEMS[self.selected].key_event()),
            KeyCode::Esc => MenuKey::Close,
            _ => MenuKey::Other,
        }
    }

    /// Where the menu draws on `screen`: just below and right of the point, moved
    /// left or up as far as it takes to stay on screen.
    pub fn area(&self, screen: Rect) -> Rect {
        let key_w = key_width();
        let label_w = ITEMS
            .iter()
            .map(|item| crate::glyphs::display_width(item.label))
            .max()
            .unwrap_or(0);
        // Border and gutter each side, the rail, the key, two spaces, the label.
        let width = (4 + 1 + key_w + 2 + label_w) as u16;
        let height = ITEMS.len() as u16 + 2;
        let width = width.min(screen.width);
        let height = height.min(screen.height);
        let x = self
            .at
            .x
            .saturating_add(1)
            .min(screen.right().saturating_sub(width))
            .max(screen.x);
        let below = self.at.y.saturating_add(1);
        let y = if below.saturating_add(height) <= screen.bottom() {
            below
        } else {
            screen.bottom().saturating_sub(height).max(screen.y)
        };
        Rect::new(x, y, width, height)
    }

    /// Draw the menu over `screen` and record its lines for a click.
    pub fn render(&self, screen: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        let area = self.area(screen);
        pointer::record(area, Hit::Menu);
        let content = Surface::new("").render(area, buf, ctx);
        let g = crate::glyphs::get();
        let key_w = key_width();
        for (i, item) in ITEMS.iter().enumerate().take(content.height as usize) {
            let row = Rect {
                y: content.y + i as u16,
                height: 1,
                ..content
            };
            let selected = i == self.selected;
            let key = format!(
                "{}{}",
                item.key,
                " ".repeat(key_w.saturating_sub(crate::glyphs::display_width(item.key)))
            );
            let line = Line::from(vec![
                Span::styled(
                    if selected { g.rail } else { " " },
                    Style::default().fg(ctx.accent),
                ),
                Span::styled(key, Style::default().fg(ctx.keybind_hints)),
                Span::raw("  "),
                Span::styled(item.label, Style::default().fg(ctx.text_primary)),
            ]);
            let mut paragraph = Paragraph::new(line);
            if selected {
                paragraph = paragraph.style(ctx.highlight_style());
            }
            paragraph.render(row, buf);
            pointer::record(row, Hit::MenuItem(i));
        }
    }
}

fn key_width() -> usize {
    ITEMS
        .iter()
        .map(|item| crate::glyphs::display_width(item.key))
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crossterm::event::KeyModifiers;

    /// Every line's key is a key of the table's in the key registry, so the menu
    /// cannot offer a key the table does not take.
    #[test]
    fn every_line_presses_a_table_key_from_the_registry() {
        let table = datui_cli::keys::screen(datui_cli::keys::Context::Table);
        let table_keys: Vec<_> = table
            .groups
            .iter()
            .flat_map(|g| g.keys.iter())
            .flat_map(|k| datui_cli::keys::chords(k.keys))
            .collect();
        for item in ITEMS {
            let chord = datui_cli::keys::chord(item.key).expect("one key");
            assert!(
                table_keys.contains(&chord),
                "{} is not a table key in the registry",
                item.key
            );
        }
    }

    #[test]
    fn the_arrows_move_round_and_enter_runs_the_line() {
        let mut menu = ContextMenu::new(Position { x: 0, y: 0 });
        let press = |code| KeyEvent::new(code, KeyModifiers::NONE);
        assert_eq!(menu.key(&press(KeyCode::Up)), MenuKey::Moved);
        assert_eq!(menu.selected, ITEMS.len() - 1, "↑ from the top wraps");
        assert_eq!(
            menu.key(&press(KeyCode::Enter)),
            MenuKey::Run(press(KeyCode::Char(' ')))
        );
        menu.key(&press(KeyCode::Char('j')));
        assert_eq!(menu.selected, 0);
        assert_eq!(
            menu.key(&press(KeyCode::Enter)),
            MenuKey::Run(press(KeyCode::Char('+')))
        );
        assert_eq!(menu.key(&press(KeyCode::Esc)), MenuKey::Close);
        assert_eq!(menu.key(&press(KeyCode::Char('x'))), MenuKey::Other);
        let shifted = KeyEvent::new(KeyCode::Down, KeyModifiers::SHIFT);
        assert_eq!(menu.key(&shifted), MenuKey::Other, "Shift+↓ is the table's");
    }

    #[test]
    fn the_menu_opens_at_the_point_and_stays_on_screen() {
        let screen = Rect::new(0, 0, 80, 24);
        let at = |x, y| ContextMenu::new(Position { x, y }).area(screen);
        let menu = at(10, 5);
        assert_eq!((menu.x, menu.y), (11, 6));
        assert_eq!(menu.height, ITEMS.len() as u16 + 2);
        // At the right and bottom edges it moves left and up, whole.
        let corner = at(79, 23);
        assert_eq!(corner.right(), 80);
        assert_eq!(corner.bottom(), 24);
        assert_eq!(corner.width, menu.width);
        // A screen smaller than the menu: it fits as much as there is.
        let tiny = ContextMenu::new(Position { x: 3, y: 3 }).area(Rect::new(0, 0, 12, 5));
        assert_eq!(tiny, Rect::new(0, 0, 12, 5));
    }

    #[test]
    fn each_line_shows_its_key_and_the_cursor_line_carries_the_rail() {
        let ctx = RenderContext::for_test();
        let screen = Rect::new(0, 0, 60, 20);
        let mut buf = Buffer::empty(screen);
        let menu = ContextMenu {
            at: Position { x: 2, y: 2 },
            selected: 2,
        };
        menu.render(screen, &mut buf, &ctx);
        let area = menu.area(screen);
        let lines: Vec<String> = (area.y..area.bottom())
            .map(|y| {
                (area.x..area.right())
                    .map(|x| buf[(x, y)].symbol())
                    .collect()
            })
            .collect();
        let g = crate::glyphs::get();
        assert!(
            lines[1].contains("+      Filter to this value"),
            "{lines:?}"
        );
        assert!(lines[3].contains(&format!("{}F", g.rail)), "{lines:?}");
        assert!(lines[7].contains("Space  Inspect row"), "{lines:?}");
    }
}
