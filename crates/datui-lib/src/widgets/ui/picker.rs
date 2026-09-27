//! The pick-one list: type to narrow, `↑↓` move, Enter chooses. A radio group
//! is a short Picker, not a grid.

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

    /// The items the filter admits, with their original indices.
    pub fn filtered(&self) -> Vec<(usize, &str)> {
        let needle = self.filter.to_lowercase();
        self.items
            .iter()
            .enumerate()
            .filter(|(_, item)| item.to_lowercase().contains(&needle))
            .map(|(i, item)| (i, item.as_str()))
            .collect()
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

    /// After the filter changes, land the cursor on something visible.
    fn settle(&mut self) {
        let filtered = self.filtered();
        if filtered.iter().all(|(i, _)| *i != self.selected)
            && let Some((first, _)) = filtered.first()
        {
            self.selected = *first;
        }
    }
}

/// Draws a pick-one list: the selection carries the rail, and the tint when
/// the list is focused. Selected-but-unfocused stays visible.
pub struct Picker<'a> {
    items: Vec<&'a str>,
    selected: Option<usize>,
    focused: bool,
}

impl<'a> Picker<'a> {
    pub fn new(items: Vec<&'a str>, selected: Option<usize>, focused: bool) -> Self {
        Self {
            items,
            selected,
            focused,
        }
    }

    pub fn from_state(state: &'a PickerState, focused: bool) -> Self {
        let items = state.filtered().into_iter().map(|(_, item)| item).collect();
        Self {
            items,
            selected: Some(state.visible_selection()),
            focused,
        }
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
            // stays visible without focus through the accent alone.
            let (marker, mut style) = if is_selected {
                (
                    if self.focused { g.rail } else { " " },
                    Style::default().fg(ctx.accent),
                )
            } else {
                (" ", Style::default().fg(ctx.text_primary))
            };
            if is_selected && self.focused {
                style = style.patch(ctx.highlight_style());
            }
            Paragraph::new(format!("{}{}", marker, self.items[i]))
                .style(style)
                .render(row_area, buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn state() -> PickerState {
        PickerState::new(
            ["CSV", "Parquet", "JSON", "NDJSON", "Arrow", "Avro"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
    }

    #[test]
    fn typing_narrows_and_backspace_widens() {
        let mut s = state();
        s.type_char('a');
        let names: Vec<&str> = s.filtered().iter().map(|(_, n)| *n).collect();
        assert_eq!(names, ["Parquet", "Arrow", "Avro"]);
        s.type_char('r');
        let names: Vec<&str> = s.filtered().iter().map(|(_, n)| *n).collect();
        assert_eq!(
            names,
            ["Parquet", "Arrow"],
            "matches anywhere, ignoring case"
        );
        s.backspace();
        s.backspace();
        assert_eq!(s.filtered().len(), 6);
    }

    #[test]
    fn narrowing_keeps_the_cursor_on_its_item_when_it_survives() {
        let mut s = state();
        s.select_original(4); // Arrow
        s.type_char('r');
        assert_eq!(
            s.selected_original(),
            Some(4),
            "Arrow matches 'r' and keeps the cursor"
        );
        s.type_char('q');
        assert_eq!(
            s.selected_original(),
            Some(1),
            "'rq' filters Arrow away, so the cursor lands on the first match"
        );
    }

    #[test]
    fn movement_walks_the_visible_items_and_wraps() {
        let mut s = state();
        s.type_char('a'); // Parquet, Arrow, Avro
        s.move_down();
        assert_eq!(s.selected_original(), Some(4));
        s.move_down();
        assert_eq!(s.selected_original(), Some(5));
        s.move_down();
        assert_eq!(s.selected_original(), Some(1), "wraps to the top");
        s.move_up();
        assert_eq!(s.selected_original(), Some(5), "and back around");
    }

    #[test]
    fn a_filter_that_admits_nothing_chooses_nothing_and_never_panics() {
        let mut s = state();
        for c in "zzz".chars() {
            s.type_char(c);
        }
        assert_eq!(s.selected_original(), None);
        s.move_down();
        s.move_up();
        assert_eq!(s.selected_original(), None);
    }

    fn render_rows(picker: &Picker, width: u16, height: u16) -> Vec<String> {
        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        picker.render(area, &mut buf, &ctx);
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    #[test]
    fn the_selection_carries_the_rail_and_only_the_selection() {
        let g = crate::glyphs::get();
        let picker = Picker::new(vec!["CSV", "Parquet", "JSON"], Some(1), true);
        let rows = render_rows(&picker, 20, 3);
        assert!(
            rows[1].starts_with(&format!("{}Parquet", g.rail)),
            "got {rows:?}"
        );
        assert!(rows[0].starts_with(" CSV"), "got {rows:?}");
        assert!(rows[2].starts_with(" JSON"), "got {rows:?}");
    }

    /// The rail leaves with focus — it is the form's one "you are here" —
    /// but the chosen item never stops being visible: it keeps the accent.
    #[test]
    fn an_unfocused_selection_stays_visible_without_the_rail() {
        let picker = Picker::new(vec!["CSV", "Parquet"], Some(0), false);
        let rows = render_rows(&picker, 20, 2);
        assert!(
            rows[0].starts_with(" CSV"),
            "no rail without focus: {rows:?}"
        );

        let ctx = RenderContext::for_test();
        let area = Rect::new(0, 0, 20, 2);
        let mut buf = Buffer::empty(area);
        picker.render(area, &mut buf, &ctx);
        assert_ne!(
            buf[(1, 0)].fg,
            buf[(1, 1)].fg,
            "the chosen item still reads apart from the rest"
        );
    }

    /// Items past the window are counted, not half-drawn.
    #[test]
    fn overflow_is_counted_on_the_last_row() {
        let picker = Picker::new(vec!["a", "b", "c", "d", "e"], Some(0), true);
        let rows = render_rows(&picker, 20, 3);
        assert!(rows[2].contains("3 more"), "got {rows:?}");
    }

    #[test]
    fn scrolling_keeps_the_selection_in_view() {
        let picker = Picker::new(vec!["a", "b", "c", "d", "e"], Some(4), true);
        let g = crate::glyphs::get();
        let rows = render_rows(&picker, 20, 3);
        assert!(
            rows[2].starts_with(&format!("{}e", g.rail)),
            "the selected last item is drawn, not the overflow count: {rows:?}"
        );
    }
}
