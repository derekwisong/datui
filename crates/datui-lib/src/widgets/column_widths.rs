//! The width each column of the main table is drawn at, kept by column identity so
//! the layout holds still while the view pages, scrolls, reorders and resizes.
//!
//! An automatic width is learned from the first page a column is drawn on, from
//! values the renderer formats anyway: nothing is read for it. Text keeps that width
//! on later pages, bounded by [`text_cap`], and a longer value is clipped behind the
//! marker. A number, date or flag can't be clipped without reading as another value,
//! so its width only grows, to the widest value seen. Widths set by hand in the
//! Columns sidebar outrank both.
//!
//! A deliberate change to what the view shows (a query, reshape, drill, sort or
//! filter) learns every automatic width again, from the first rows the new view
//! reads. Paging and scrolling never do.

use polars::prelude::DataType;
use std::collections::HashMap;

/// How a column's width is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WidthChoice {
    /// Learned from the rows on screen, text and headings bounded by the cap.
    #[default]
    Auto,
    /// Set by hand. Text and headings get exactly this many cells; a number column
    /// is never narrower than its widest value on screen.
    Manual(u16),
    /// Fit to the rows on screen the next time the table is drawn, which makes it
    /// [`WidthChoice::Manual`].
    Fit,
}

/// The narrowest and widest a width set by hand may be.
pub const MIN_WIDTH: u16 = 4;
pub const MAX_WIDTH: u16 = 240;
/// Cells one narrower or wider press moves a width.
pub const WIDTH_STEP: u16 = 4;
/// Where narrower or wider starts on a column that has not been drawn yet.
pub const UNSEEN_WIDTH: u16 = 12;

impl WidthChoice {
    /// One step narrower, from the width set by hand or else from the width drawn.
    pub fn narrower(self, shown: Option<u16>) -> Self {
        Self::Manual(
            self.base(shown)
                .saturating_sub(WIDTH_STEP)
                .clamp(MIN_WIDTH, MAX_WIDTH),
        )
    }

    /// One step wider, from the width set by hand or else from the width drawn.
    pub fn wider(self, shown: Option<u16>) -> Self {
        Self::Manual(
            self.base(shown)
                .saturating_add(WIDTH_STEP)
                .clamp(MIN_WIDTH, MAX_WIDTH),
        )
    }

    fn base(self, shown: Option<u16>) -> u16 {
        match self {
            Self::Manual(width) => width,
            Self::Auto | Self::Fit => shown.unwrap_or(UNSEEN_WIDTH),
        }
    }
}

/// The most cells an automatic width gives text or a heading: two fifths of the
/// terminal (32 at 80 columns, 48 at 120), between 16 and 64. Taken from the
/// terminal rather than the table, so opening a sidebar moves no column; a resize
/// moves them, as it should.
pub fn text_cap(screen_width: u16) -> u16 {
    let fifths = u32::from(screen_width) * 2 / 5;
    u16::try_from(fifths).unwrap_or(u16::MAX).clamp(16, 64)
}

/// One column measured on the rows on screen, in cells, as the table draws it.
#[derive(Debug, Clone, Copy, Default)]
pub struct PageMeasure {
    /// The name and its marks.
    pub header: u16,
    /// The type row's label, or 0 without the type row.
    pub type_label: u16,
    /// The widest value or null glyph.
    pub values: u16,
    /// Whether any row on screen holds a value rather than a null.
    pub has_values: bool,
    /// Whether a value may be drawn clipped: text, not a number.
    pub clips: bool,
}

impl PageMeasure {
    /// The width that shows this page whole, heading bounded by `cap`.
    fn fitted(&self, cap: u16) -> u16 {
        self.values
            .max(self.type_label)
            .max(self.header.min(cap))
            .clamp(1, MAX_WIDTH)
    }
}

#[derive(Debug, Clone)]
struct Entry {
    dtype: DataType,
    /// The values' width: for text, of the first page with a value in it; for the
    /// rest, the widest page so far.
    learned: u16,
    /// Whether text has learned from a page with a value, rather than only nulls.
    settled: bool,
    choice: WidthChoice,
    /// The width the column was last drawn at.
    shown: Option<u16>,
    /// Whether `shown` was drawn since automatic widths were last relearned, so it
    /// is the width the column draws at now rather than in the view before.
    current: bool,
}

/// Display widths by column identity: a name and its type. A column whose type
/// changes, as a query or reading it as text can do, is a new column and starts
/// afresh; one that comes back with its type keeps its width. Separate from the
/// footer's byte estimate, which plans the buffer and says nothing about cells.
#[derive(Debug, Clone, Default)]
pub struct ColumnWidths {
    by_name: HashMap<String, Vec<Entry>>,
    /// Automatic widths are to be learned again from the next rows read. Waits for
    /// them: until they arrive the old rows are still drawn, and they would teach
    /// the new view their widths.
    relearn: bool,
}

impl ColumnWidths {
    fn entry(&self, name: &str, dtype: &DataType) -> Option<&Entry> {
        self.by_name.get(name)?.iter().find(|e| &e.dtype == dtype)
    }

    fn entry_mut(&mut self, name: &str, dtype: &DataType) -> &mut Entry {
        // Looked up before inserting: drawing calls this per column per frame, and
        // the name is allocated only the first time.
        if !self.by_name.contains_key(name) {
            self.by_name.insert(name.to_string(), Vec::new());
        }
        let entries = self.by_name.get_mut(name).expect("inserted above");
        let at = match entries.iter().position(|e| &e.dtype == dtype) {
            Some(at) => at,
            None => {
                entries.push(Entry {
                    dtype: dtype.clone(),
                    learned: 0,
                    settled: false,
                    choice: WidthChoice::Auto,
                    shown: None,
                    current: false,
                });
                entries.len() - 1
            }
        };
        &mut entries[at]
    }

    /// The width to draw the column at on this page, learning from the page as
    /// automatic widths do. `cap` bounds automatic text and headings.
    pub fn width(&mut self, name: &str, dtype: &DataType, page: PageMeasure, cap: u16) -> u16 {
        let entry = self.entry_mut(name, dtype);
        if page.clips {
            if !entry.settled {
                entry.learned = entry.learned.max(page.values);
                entry.settled = page.has_values;
            }
        } else {
            entry.learned = entry.learned.max(page.values);
        }
        let width = match entry.choice {
            WidthChoice::Manual(width) if page.clips => width,
            WidthChoice::Manual(width) => width.max(page.values),
            WidthChoice::Auto | WidthChoice::Fit => {
                let values = if page.clips {
                    entry.learned.min(cap)
                } else {
                    entry.learned
                };
                values.max(page.type_label).max(page.header.min(cap))
            }
        };
        entry.shown = Some(width);
        entry.current = true;
        width
    }

    /// How the column's width is chosen.
    pub fn choice(&self, name: &str, dtype: &DataType) -> WidthChoice {
        self.entry(name, dtype)
            .map_or(WidthChoice::Auto, |e| e.choice)
    }

    /// The width the column was last drawn at, if it has been.
    pub fn shown(&self, name: &str, dtype: &DataType) -> Option<u16> {
        self.entry(name, dtype).and_then(|e| e.shown)
    }

    /// The width the column was last drawn at, if it has been drawn since the
    /// widths were last relearned: what a sideways page can be planned with.
    pub fn drawn(&self, name: &str, dtype: &DataType) -> Option<u16> {
        self.entry(name, dtype)
            .filter(|e| e.current)
            .and_then(|e| e.shown)
    }

    pub fn set_choice(&mut self, name: &str, dtype: &DataType, choice: WidthChoice) {
        let choice = match choice {
            WidthChoice::Manual(width) => WidthChoice::Manual(width.clamp(MIN_WIDTH, MAX_WIDTH)),
            other => other,
        };
        self.entry_mut(name, dtype).choice = choice;
    }

    /// The columns waiting to be fitted to the rows on screen.
    pub fn fits_pending(&self) -> Vec<(String, DataType)> {
        self.by_name
            .iter()
            .flat_map(|(name, entries)| {
                entries
                    .iter()
                    .filter(|e| e.choice == WidthChoice::Fit)
                    .map(move |e| (name.clone(), e.dtype.clone()))
            })
            .collect()
    }

    /// Fit the column to `page`: its values and type whole, its heading up to `cap`.
    pub fn fit(&mut self, name: &str, dtype: &DataType, page: PageMeasure, cap: u16) {
        self.entry_mut(name, dtype).choice = WidthChoice::Manual(page.fitted(cap));
    }

    /// The view changed: learn every automatic width again from the next rows read
    /// (see [`Self::rows_arrived`]). Widths set by hand are kept.
    pub fn relearn(&mut self) {
        self.relearn = true;
    }

    /// The change asked to relearn never showed (it failed and the view was put
    /// back), so the widths learned for the view on screen stand.
    pub fn keep_learned(&mut self) {
        self.relearn = false;
    }

    /// Rows read for the view are about to replace the ones on screen. After
    /// [`Self::relearn`], every automatic width starts again from them: text from
    /// its first page with a value, and the rest from nothing, so they may narrow.
    pub fn rows_arrived(&mut self) {
        if std::mem::take(&mut self.relearn) {
            for entry in self.by_name.values_mut().flatten() {
                entry.learned = 0;
                entry.settled = false;
                entry.current = false;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn text(values: u16) -> PageMeasure {
        PageMeasure {
            header: 4,
            type_label: 3,
            values,
            has_values: true,
            clips: true,
        }
    }

    fn number(values: u16) -> PageMeasure {
        PageMeasure {
            clips: false,
            ..text(values)
        }
    }

    #[test]
    fn the_cap_is_two_fifths_of_the_terminal_within_bounds() {
        assert_eq!(text_cap(80), 32);
        assert_eq!(text_cap(120), 48);
        assert_eq!(text_cap(60), 24);
        assert_eq!(text_cap(20), 16);
        assert_eq!(text_cap(400), 64);
    }

    /// Text keeps the width of the first page it showed a value on; later pages
    /// neither widen nor narrow it.
    #[test]
    fn text_keeps_its_first_page_width() {
        let mut widths = ColumnWidths::default();
        let s = DataType::String;
        assert_eq!(widths.width("d", &s, text(10), 32), 10);
        assert_eq!(widths.width("d", &s, text(200), 32), 10);
        assert_eq!(widths.width("d", &s, text(2), 32), 10);
    }

    /// A first page of nulls teaches nothing: the first page with a value does.
    #[test]
    fn a_page_of_nulls_does_not_settle_text() {
        let mut widths = ColumnWidths::default();
        let s = DataType::String;
        let nulls = PageMeasure {
            values: 1,
            has_values: false,
            ..text(1)
        };
        assert_eq!(widths.width("d", &s, nulls, 32), 4);
        assert_eq!(widths.width("d", &s, text(12), 32), 12);
        assert_eq!(widths.width("d", &s, text(20), 32), 12);
    }

    /// Long text and long headings are bounded by the cap.
    #[test]
    fn automatic_text_and_headings_stop_at_the_cap() {
        let mut widths = ColumnWidths::default();
        let s = DataType::String;
        assert_eq!(widths.width("d", &s, text(215), 32), 32);
        let long_heading = PageMeasure {
            header: 105,
            ..number(3)
        };
        assert_eq!(widths.width("n", &DataType::Int64, long_heading, 32), 32);
    }

    /// A number never draws narrower than its widest value seen, and does not shrink
    /// back on a page of narrower ones.
    #[test]
    fn numbers_widen_and_stay_wide() {
        let mut widths = ColumnWidths::default();
        let i = DataType::Int64;
        assert_eq!(widths.width("n", &i, number(5), 32), 5);
        assert_eq!(widths.width("n", &i, number(7), 32), 7);
        assert_eq!(widths.width("n", &i, number(2), 32), 7);
    }

    /// A width set by hand is exact for text, and a floor for numbers.
    #[test]
    fn a_manual_width_is_exact_for_text_and_a_floor_for_numbers() {
        let mut widths = ColumnWidths::default();
        widths.set_choice("d", &DataType::String, WidthChoice::Manual(6));
        assert_eq!(widths.width("d", &DataType::String, text(30), 32), 6);
        widths.set_choice("n", &DataType::Int64, WidthChoice::Manual(6));
        assert_eq!(widths.width("n", &DataType::Int64, number(3), 32), 6);
        assert_eq!(widths.width("n", &DataType::Int64, number(9), 32), 9);
        widths.set_choice("d", &DataType::String, WidthChoice::Manual(1));
        assert_eq!(
            widths.choice("d", &DataType::String),
            WidthChoice::Manual(MIN_WIDTH)
        );
    }

    /// The same name with another type is another column: it starts afresh, and
    /// the first comes back with its own width.
    #[test]
    fn a_column_is_its_name_and_type() {
        let mut widths = ColumnWidths::default();
        widths.set_choice("x", &DataType::Int64, WidthChoice::Manual(20));
        assert_eq!(widths.width("x", &DataType::String, text(5), 32), 5);
        assert_eq!(widths.choice("x", &DataType::String), WidthChoice::Auto);
        assert_eq!(widths.width("x", &DataType::Int64, number(3), 32), 20);
    }

    #[test]
    fn fit_takes_the_page_and_automatic_returns_to_the_learned_width() {
        let mut widths = ColumnWidths::default();
        let s = DataType::String;
        assert_eq!(widths.width("d", &s, text(10), 32), 10);
        widths.set_choice("d", &s, WidthChoice::Fit);
        assert_eq!(widths.fits_pending(), vec![("d".to_string(), s.clone())]);
        widths.fit("d", &s, text(90), 32);
        assert!(widths.fits_pending().is_empty());
        assert_eq!(widths.choice("d", &s), WidthChoice::Manual(90));
        assert_eq!(widths.width("d", &s, text(3), 32), 90);
        widths.set_choice("d", &s, WidthChoice::Auto);
        assert_eq!(widths.width("d", &s, text(3), 32), 10);
    }

    /// A relearn waits for the new view's rows: the old ones drawn meanwhile teach
    /// nothing that lasts. Then text and numbers start again, and manual widths stay.
    #[test]
    fn a_relearn_starts_again_from_the_next_rows() {
        let mut widths = ColumnWidths::default();
        let (s, i) = (DataType::String, DataType::Int64);
        assert_eq!(widths.width("d", &s, text(10), 32), 10);
        assert_eq!(widths.width("n", &i, number(9), 32), 9);
        widths.set_choice("m", &s, WidthChoice::Manual(7));
        widths.relearn();
        assert_eq!(widths.width("d", &s, text(20), 32), 10);
        widths.rows_arrived();
        assert_eq!(widths.width("d", &s, text(20), 32), 20);
        assert_eq!(widths.width("d", &s, text(30), 32), 20);
        assert_eq!(widths.width("n", &i, number(3), 32), 4);
        assert_eq!(widths.width("m", &s, text(30), 32), 7);
        // Only once: later rows are paging.
        widths.rows_arrived();
        assert_eq!(widths.width("d", &s, text(5), 32), 20);
    }

    /// A width drawn before a relearn is not one to plan a page with until the
    /// column is drawn again; it is still the width the sidebar steps from.
    #[test]
    fn a_relearn_makes_drawn_widths_unknown_until_drawn_again() {
        let mut widths = ColumnWidths::default();
        let s = DataType::String;
        assert_eq!(widths.drawn("d", &s), None);
        assert_eq!(widths.width("d", &s, text(10), 32), 10);
        assert_eq!(widths.drawn("d", &s), Some(10));
        widths.relearn();
        widths.rows_arrived();
        assert_eq!(widths.drawn("d", &s), None);
        assert_eq!(widths.shown("d", &s), Some(10));
        assert_eq!(widths.width("d", &s, text(4), 32), 4);
        assert_eq!(widths.drawn("d", &s), Some(4));
    }

    /// A relearn whose view never showed is dropped.
    #[test]
    fn a_relearn_kept_back_changes_nothing() {
        let mut widths = ColumnWidths::default();
        let s = DataType::String;
        assert_eq!(widths.width("d", &s, text(10), 32), 10);
        widths.relearn();
        widths.keep_learned();
        widths.rows_arrived();
        assert_eq!(widths.width("d", &s, text(20), 32), 10);
    }

    #[test]
    fn narrower_and_wider_step_from_what_is_drawn() {
        assert_eq!(
            WidthChoice::Auto.wider(Some(10)),
            WidthChoice::Manual(10 + WIDTH_STEP)
        );
        assert_eq!(
            WidthChoice::Auto.narrower(Some(10)),
            WidthChoice::Manual(10 - WIDTH_STEP)
        );
        assert_eq!(
            WidthChoice::Manual(MIN_WIDTH).narrower(None),
            WidthChoice::Manual(MIN_WIDTH)
        );
        assert_eq!(
            WidthChoice::Manual(MAX_WIDTH).wider(None),
            WidthChoice::Manual(MAX_WIDTH)
        );
        assert_eq!(
            WidthChoice::Fit.wider(None),
            WidthChoice::Manual(UNSEEN_WIDTH + WIDTH_STEP)
        );
    }
}
