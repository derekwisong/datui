//! Each main-table column's drawn width, kept by identity so the layout holds through
//! paging, scrolling, reordering and resizing. Automatic widths are learned from the
//! first page drawn (values already formatted, nothing read): text keeps it, bounded by
//! [`text_cap`] and clipped beyond; numbers, dates and flags only grow (clipped they
//! would read wrong). Sidebar-set widths win. A query, reshape, drill, sort or filter
//! relearns automatic widths; paging and scrolling never do.

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
/// terminal (32 at 80 columns), between 16 and 64. From the terminal, not the table, so
/// a sidebar moves nothing; a resize does.
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
    /// The width it was last drawn at with the room to the table's right edge, as
    /// the last column drawn: what it showed, though not what it is planned with.
    filled: Option<u16>,
    /// Whether `shown` was drawn since automatic widths were last relearned, so it
    /// is the width the column draws at now rather than in the view before.
    current: bool,
}

/// Display widths by column identity (name and type): a retyped column starts afresh,
/// one returning with its type keeps its width. Unrelated to the byte estimate that
/// plans buffers.
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
                    filled: None,
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
        entry.filled = None;
        entry.current = true;
        width
    }

    /// The column, drawn last at `width`, was widened to `filled` to reach the table's
    /// right edge. Only an automatic width fills: one set by hand is drawn as set.
    pub fn fill(&mut self, name: &str, dtype: &DataType, filled: u16) {
        let entry = self.entry_mut(name, dtype);
        entry.filled = Some(filled);
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

    /// [`Self::shown`], with the room the column filled at the right edge: what a
    /// step narrower or wider starts from, so a wider column never draws narrower.
    pub fn on_screen(&self, name: &str, dtype: &DataType) -> Option<u16> {
        self.entry(name, dtype).and_then(|e| e.filled.or(e.shown))
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
mod tests;
