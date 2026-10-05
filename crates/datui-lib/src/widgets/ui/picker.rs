//! The pick-one list: type to narrow, `↑↓` move, Enter chooses. A radio group
//! is a short Picker, not a grid.

use crate::pointer::Hit;
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
    /// The values of a form's field, listed: the field steps to the line's value.
    Step(crate::pointer::FieldId),
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
            crate::pointer::record(area, Hit::Picker);
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
                crate::pointer::record(row_area, hit);
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
            Clicks::Step(field) => Hit::Option {
                field: Some(field.clone()),
                index: i,
                current: selected,
            },
            Clicks::Tool => Hit::Tool(i),
        })
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
    /// but the chosen item never stops being visible: it keeps the accent
    /// and a marker glyph, so it survives a terminal with no color at all.
    #[test]
    fn an_unfocused_selection_stays_visible_without_the_rail() {
        let g = crate::glyphs::get();
        let picker = Picker::new(vec!["CSV", "Parquet"], Some(0), false);
        let rows = render_rows(&picker, 20, 2);
        assert!(
            rows[0].starts_with(&format!("{}CSV", g.middot)),
            "the choice keeps a glyph without focus: {rows:?}"
        );
        assert!(
            !rows[0].starts_with(g.rail),
            "but never the rail, which means focus: {rows:?}"
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

    /// Marks turn the list into a toggle list: every item carries a checkbox,
    /// on or off, so what is already chosen never has to be remembered.
    #[test]
    fn marks_draw_a_checkbox_on_every_item() {
        let g = crate::glyphs::get();
        let picker = Picker::new(vec!["dept", "region"], Some(0), true).marks(vec![true, false]);
        let rows = render_rows(&picker, 20, 2);
        assert!(
            rows[0].contains(&format!("{} dept", g.checkbox_on)),
            "got {rows:?}"
        );
        assert!(
            rows[1].contains(&format!("{} region", g.checkbox_off)),
            "got {rows:?}"
        );
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
    /// A chord is a chord: Ctrl+W edits the filter, and no modified
    /// character ever lands in it as a letter.
    #[test]
    fn the_filter_keeps_readline_chords_out_of_the_text() {
        use crossterm::event::KeyModifiers;
        let mut p = PickerState::new(vec!["first_name".to_string(), "start date".to_string()]);
        for c in "start d".chars() {
            p.filter_key(c, KeyModifiers::NONE);
        }
        assert_eq!(p.filter, "start d");
        p.filter_key('w', KeyModifiers::CONTROL);
        assert_eq!(p.filter, "start ", "Ctrl+W drops the word, not types w");
        p.filter_key('u', KeyModifiers::CONTROL);
        assert_eq!(p.filter, "", "Ctrl+U clears, not types u");
        p.filter_key('x', KeyModifiers::ALT);
        assert_eq!(p.filter, "", "an Alt chord is not a letter");
    }
}
