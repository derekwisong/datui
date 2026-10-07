//! The pick-one list: type to narrow, `↑↓` move, Enter chooses. A radio group
//! is a short Picker, not a grid.

use crate::app::pointer::Hit;
use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::widgets::{Paragraph, Widget};

/// The list and where the cursor is in it. Owns the narrowing; the caller owns
/// what choosing means.
#[derive(Debug, Clone, Default)]
pub struct PickerState {
    items: Vec<String>,
    pub filter: String,
    /// Index into the full item list, so narrowing never moves the cursor off
    /// the item it was on.
    selected: usize,
}

impl PickerState {
    pub fn new(items: Vec<String>) -> Self {
        Self {
            items,
            filter: String::new(),
            selected: 0,
        }
    }

    /// Every item, whatever the filter admits.
    pub fn items(&self) -> &[String] {
        &self.items
    }

    /// The items the filter admits, with their original indices: the one it names
    /// first, then those it starts, then the rest, each in list order.
    pub fn filtered(&self) -> Vec<(usize, &str)> {
        let mut ranked: Vec<(u8, usize, &str)> = self
            .items
            .iter()
            .enumerate()
            .filter_map(|(i, item)| {
                crate::home::fuzzy::substring_rank(&self.filter, item)
                    .map(|r| (r, i, item.as_str()))
            })
            .collect();
        ranked.sort_by_key(|(rank, i, _)| (*rank, *i));
        ranked.into_iter().map(|(_, i, item)| (i, item)).collect()
    }

    /// Where the cursor sits among the visible items; 0 when the item it was
    /// on has been filtered away.
    pub fn visible_selection(&self) -> usize {
        self.filtered()
            .iter()
            .position(|(i, _)| *i == self.selected)
            .unwrap_or(0)
    }

    /// The selected item's original index, or None when the filter admits nothing.
    pub fn selected_original(&self) -> Option<usize> {
        let filtered = self.filtered();
        filtered
            .get(self.visible_selection())
            .or_else(|| filtered.first())
            .map(|(i, _)| *i)
    }

    pub fn select_original(&mut self, index: usize) {
        if index < self.items.len() {
            self.selected = index;
        }
    }

    pub fn type_char(&mut self, c: char) {
        self.filter.push(c);
        self.settle();
    }

    pub fn backspace(&mut self) {
        self.filter.pop();
        self.settle();
    }

    pub fn clear_filter(&mut self) {
        self.filter.clear();
        self.settle();
    }

    /// One typed character, with its modifiers: plain characters narrow,
    /// Ctrl+W drops a word, Ctrl+U clears, and any other chord is a chord —
    /// never a letter typed into the filter.
    pub fn filter_key(&mut self, c: char, mods: crossterm::event::KeyModifiers) {
        use crossterm::event::KeyModifiers;
        let ctrl = mods.contains(KeyModifiers::CONTROL);
        if ctrl && c == 'w' {
            self.delete_word();
        } else if ctrl && c == 'u' {
            self.clear_filter();
        } else if !ctrl && !mods.contains(KeyModifiers::ALT) {
            self.type_char(c);
        }
    }

    /// Ctrl+W: drop the word before the cursor, readline-style. The filter
    /// is append-only, so the cursor is always the end.
    pub fn delete_word(&mut self) {
        while self.filter.ends_with(' ') {
            self.filter.pop();
        }
        while self.filter.chars().next_back().is_some_and(|c| c != ' ') {
            self.filter.pop();
        }
        self.settle();
    }

    pub fn move_up(&mut self) {
        self.step(-1);
    }

    pub fn move_down(&mut self) {
        self.step(1);
    }

    fn step(&mut self, delta: isize) {
        let filtered = self.filtered();
        if filtered.is_empty() {
            return;
        }
        let at = self.visible_selection() as isize;
        let n = filtered.len() as isize;
        let next = (at + delta).rem_euclid(n) as usize;
        self.selected = filtered[next].0;
    }

    /// After the filter changes, land the cursor on something visible: the item
    /// the filter names in full, else the one it was on, else the first.
    fn settle(&mut self) {
        let filtered = self.filtered();
        let named = filtered
            .first()
            .filter(|(_, item)| crate::home::fuzzy::substring_rank(&self.filter, item) == Some(0));
        if let Some((exact, _)) = named {
            self.selected = *exact;
        } else if filtered.iter().all(|(i, _)| *i != self.selected)
            && let Some((first, _)) = filtered.first()
        {
            self.selected = *first;
        }
    }
}

/// Draws a pick-one list: the selection carries the rail, and the tint when
/// the list is focused. Selected-but-unfocused stays visible. With marks it
/// is a toggle list: each item carries a checkbox, and choosing means Space.
pub struct Picker<'a> {
    items: Vec<&'a str>,
    selected: Option<usize>,
    focused: bool,
    marks: Option<Vec<bool>>,
    /// Beside each item, right-aligned and dimmed: a count.
    details: Option<Vec<String>>,
    clicks: Option<Clicks>,
}

/// What a click on a line does, recorded as the list is drawn.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Clicks {
    /// An open picker: the cursor goes to the line, which is chosen (toggled in a
    /// list of marks).
    Choose,
    /// The analysis tools list.
    Tool,
}

impl<'a> Picker<'a> {
    pub fn new(items: Vec<&'a str>, selected: Option<usize>, focused: bool) -> Self {
        Self {
            items,
            selected,
            focused,
            marks: None,
            details: None,
            clicks: None,
        }
    }

    /// What a click on a line does. A list from a [`PickerState`] is an open picker
    /// and chooses already.
    pub fn on_click(mut self, clicks: Clicks) -> Self {
        self.clicks = Some(clicks);
        self
    }

    pub fn from_state(state: &'a PickerState, focused: bool) -> Self {
        let items = state.filtered().into_iter().map(|(_, item)| item).collect();
        Self {
            items,
            selected: Some(state.visible_selection()),
            focused,
            marks: None,
            details: None,
            clicks: Some(Clicks::Choose),
        }
    }

    /// Checkbox states, one per visible item in order.
    pub fn marks(mut self, marks: Vec<bool>) -> Self {
        self.marks = Some(marks);
        self
    }

    /// A note per visible item in order, right-aligned beside it.
    pub fn details(mut self, details: Vec<String>) -> Self {
        self.details = Some(details);
        self
    }

    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        let g = crate::glyphs::get();
        let height = area.height as usize;
        // Scroll just enough to keep the selection in view, and count what ran
        // off the bottom rather than half-showing it.
        let selected = self.selected.unwrap_or(0);
        let offset = selected.saturating_sub(height.saturating_sub(1));
        let below = self.items.len().saturating_sub(offset + height);
        if self.clicks == Some(Clicks::Choose) {
            crate::app::pointer::record(area, Hit::Picker);
        }
        for row in 0..height.min(self.items.len().saturating_sub(offset)) {
            let i = offset + row;
            let is_selected = self.selected == Some(i);
            let row_area = Rect {
                y: area.y + row as u16,
                height: 1,
                ..area
            };
            if row + 1 == height && below > 0 && !is_selected {
                let more = format!("  {} {} more", g.ellipsis, below + 1);
                Paragraph::new(more)
                    .style(Style::default().fg(ctx.dimmed))
                    .render(row_area, buf);
                break;
            }
            // The rail marks focus, not selection: inside one Surface it is
            // the one "you are here", and Tab visibly moves it. The choice
            // stays marked without focus by a glyph, not by color alone —
            // under NO_COLOR an accent-only selection disappeared.
            let (marker, mut style) = if is_selected {
                (
                    if self.focused { g.rail } else { g.middot },
                    Style::default().fg(ctx.accent),
                )
            } else {
                (" ", Style::default().fg(ctx.text_primary))
            };
            if is_selected && self.focused {
                style = style.patch(ctx.highlight_style());
            }
            let mark = match &self.marks {
                Some(marks) => {
                    let on = marks.get(i).copied().unwrap_or(false);
                    format!("{} ", if on { g.checkbox_on } else { g.checkbox_off })
                }
                None => String::new(),
            };
            Paragraph::new(format!("{}{}{}", marker, mark, self.items[i]))
                .style(style)
                .render(row_area, buf);
            if let Some(detail) = self.details.as_ref().and_then(|d| d.get(i)) {
                let w = crate::glyphs::display_width(detail) as u16;
                if w + 2 < row_area.width {
                    let x = row_area.right() - w;
                    buf.set_string(x - 1, row_area.y, " ", style);
                    buf.set_string(x, row_area.y, detail, style.fg(ctx.dimmed));
                }
            }
            if let Some(hit) = self.click_on(i) {
                crate::app::pointer::record(row_area, hit);
            }
        }
    }
}

impl Picker<'_> {
    /// What a click on line `i` lands on.
    fn click_on(&self, i: usize) -> Option<Hit> {
        let selected = self.selected.unwrap_or(0);
        Some(match self.clicks.as_ref()? {
            Clicks::Choose => Hit::PickerItem {
                visible: i,
                selected,
                multi: self.marks.is_some(),
            },
            Clicks::Tool => Hit::Tool(i),
        })
    }
}

#[cfg(test)]
mod tests;
