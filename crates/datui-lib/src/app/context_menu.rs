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

use crate::app::pointer::{self, Hit};
use crate::render::context::RenderContext;
use crate::widgets::ui::Surface;

/// One line: the key it presses, as the key registry spells it, and what it does
/// to the cell. A line with no key (`""`) does its [`MenuAction`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MenuItem {
    pub key: &'static str,
    pub label: &'static str,
    pub action: Option<MenuAction>,
}

/// What a line with no key of its own does to the cursor's column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MenuAction {
    /// The column's type: the same names and formats a spec's `type` takes.
    ChangeType,
    /// A datetime made from a date, a time and an offset, as a spec's `as` makes one.
    CombineDatetime,
}

const fn keyed(key: &'static str, label: &'static str) -> MenuItem {
    MenuItem {
        key,
        label,
        action: None,
    }
}

/// The lines for the cursor's column that no key reaches: its type, and, on a text,
/// date or time column, a datetime made from it.
pub fn column_items(combine: bool) -> Vec<MenuItem> {
    let mut items = vec![MenuItem {
        key: "",
        label: "Change type...",
        action: Some(MenuAction::ChangeType),
    }];
    if combine {
        items.push(MenuItem {
            key: "",
            label: "Combine into datetime...",
            action: Some(MenuAction::CombineDatetime),
        });
    }
    items
}

/// The cell's keys, in the order the menu lists them.
pub const ITEMS: &[MenuItem] = &[
    keyed("+", "Filter to this value"),
    keyed("-", "Filter out this value"),
    keyed("F", "Value counts"),
    keyed("[", "Sort ascending"),
    keyed("]", "Sort descending"),
    keyed("y", "Copy"),
    keyed("Space", "Inspect row"),
];

impl MenuItem {
    /// The key this line presses, as typed.
    pub fn key_event(&self) -> KeyEvent {
        let chord = datui_cli::keys::chord(self.key).expect("a menu key is one key");
        crate::app::help::key_event(chord)
    }
}

/// An open menu: where it was asked for, its lines and the one the cursor is on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextMenu {
    pub at: Position,
    pub selected: usize,
    pub items: Vec<MenuItem>,
}

/// What a key does in the open menu.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MenuKey {
    /// Moved the cursor.
    Moved,
    /// Close the menu and press this key.
    Run(KeyEvent),
    /// Close the menu and do this.
    Do(MenuAction),
    /// Esc: close it.
    Close,
    /// Not the menu's: close it, and the key acts as it would have.
    Other,
}

impl ContextMenu {
    pub fn with(at: Position, extra: Vec<MenuItem>) -> Self {
        let mut items = ITEMS.to_vec();
        items.extend(extra);
        Self {
            at,
            selected: 0,
            items,
        }
    }

    /// What choosing line `i` does.
    pub fn chosen(&self, i: usize) -> Option<MenuKey> {
        let item = self.items.get(i)?;
        Some(match item.action {
            Some(action) => MenuKey::Do(action),
            None => MenuKey::Run(item.key_event()),
        })
    }

    /// ↑ / ↓ (k / j) move, wrapping; Enter runs the line; Esc closes. Any other key,
    /// a line's own included, leaves the menu and acts as typed.
    pub fn key(&mut self, event: &KeyEvent) -> MenuKey {
        let n = self.items.len();
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
            KeyCode::Enter => self.chosen(self.selected).unwrap_or(MenuKey::Close),
            KeyCode::Esc => MenuKey::Close,
            _ => MenuKey::Other,
        }
    }

    /// Where the menu draws on `screen`: just below and right of the point, moved
    /// left or up as far as it takes to stay on screen.
    pub fn area(&self, screen: Rect) -> Rect {
        let key_w = key_width(&self.items);
        let label_w = self
            .items
            .iter()
            .map(|item| crate::glyphs::display_width(item.label))
            .max()
            .unwrap_or(0);
        // Border and gutter each side, the rail, the key, two spaces, the label.
        let width = (4 + 1 + key_w + 2 + label_w) as u16;
        let height = self.items.len() as u16 + 2;
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
        let key_w = key_width(&self.items);
        for (i, item) in self.items.iter().enumerate().take(content.height as usize) {
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

fn key_width(items: &[MenuItem]) -> usize {
    items
        .iter()
        .map(|item| crate::glyphs::display_width(item.key))
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests;
