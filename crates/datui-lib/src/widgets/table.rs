//! The table widget: draws a [`DataTableState`]'s page, fitting the columns to the
//! width, and records what it drew for the clicks that follow.

use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, Cell, HighlightSpacing, Padding, Paragraph, Row, StatefulWidget, Table,
        TableState, Widget,
    },
};

use crate::error_display::user_message_from_polars;
use crate::formats::column_types::dtype_label;
use crate::numfmt::{self, CellFormatter, NumberFormatSettings};
use crate::table::{DataTableState, DrawnTable, visible_slice};
use crate::widgets::column_paging::{OnScreen, Room};
use crate::widgets::column_widths::{ColumnWidths, PageMeasure, WidthChoice};

pub struct DataTable {
    pub header_bg: Color,
    pub header_fg: Color,
    pub row_numbers_fg: Color,
    pub separator_fg: Color,
    pub table_cell_padding: u16,
    pub alternate_row_bg: Option<Color>,
    /// When true, colorize cells by column type using the optional colors below.
    pub column_colors: bool,
    pub str_col: Option<Color>,
    pub int_col: Option<Color>,
    pub float_col: Option<Color>,
    pub bool_col: Option<Color>,
    pub temporal_col: Option<Color>,
    /// Color for binary-column placeholder cells (the `‹binary›` stub). Applied with italic,
    /// independent of `column_colors`, so stubs always read as "placeholder, not data".
    pub binary_col: Option<Color>,
    /// Names of columns that are binary in the source schema. Their cells hold the `‹binary›`
    /// stub (see `binary_stub`) and are styled with `binary_col` + italic.
    pub binary_cols: std::collections::HashSet<String>,
    /// Display-time number formatting (digit grouping, separators, alignment).
    pub number_format: NumberFormatSettings,
    /// Draw a second header row naming each column's type.
    pub dtype_row: bool,
    /// Tint under the row the cursor is on. `None` falls back to reversed video.
    pub selected_bg: Option<Color>,
    /// The full selected-row style, from the theme's `highlight_style` helper.
    pub selection_style: Style,
    /// The rail beside the selected row and the off-screen column hints.
    pub accent: Color,
    /// Null cells and the type row.
    pub dimmed: Color,
    /// Per row on screen, its file's drift group. Empty when the dataset's files agree,
    /// or when the rows no longer stand for rows of a file.
    pub drift_rows: Vec<u32>,
    /// What each drift group is missing. Indexed by the values in `drift_rows`.
    pub drift_groups: Arc<Vec<crate::formats::schema_union::DriftGroup>>,
    /// Columns the view is sorted by; each carries a direction mark in the header.
    /// Filled from the state at render, so the marks always describe the frame drawn.
    pub sort_columns: Vec<String>,
    /// Which way each of them runs, per column, as it is applied.
    pub sort_descending: Vec<bool>,
    /// The column cursor's column: its header and cells are tinted.
    pub current_column: Option<String>,
    /// The column cursor's cells, from the theme's `column_cursor_style` helper.
    pub column_cursor_style: Style,
    /// The column cursor's header and the current cell, from the theme's
    /// `cell_cursor_style` helper.
    pub cell_cursor_style: Style,
    /// The glyph set the table draws with: the terminal's, unless a test asks for one.
    pub glyphs: &'static crate::glyphs::Glyphs,
    /// The terminal's width, which bounds automatic text widths (see
    /// [`crate::widgets::column_widths::text_cap`]) and how much of a value a cell keeps.
    /// 0 takes the table's own width.
    pub screen_width: u16,
    /// The cell a find landed on: its view row and column.
    pub find_cell: Option<(usize, String)>,
    /// How that cell is drawn, from the theme's `find_match_style`.
    pub find_style: Style,
    /// The found cell's column, while the cursor is on its row: set at render.
    pub(crate) find_column: Option<String>,
    /// The cells a find being typed matches, by view row and column, drawn as
    /// found.
    pub match_cells: Option<std::sync::Arc<crate::find::MatchCells>>,
    /// The view row the first row drawn is: set at render.
    pub(crate) drawn_from: usize,
    /// Each column's unit from a delimited spec's unit row, for the type row: set at
    /// render.
    pub(crate) units: Vec<(String, String)>,
    /// The columns the view gave a type: their type row is in the accent.
    pub(crate) retyped: Vec<String>,
}

impl Default for DataTable {
    fn default() -> Self {
        Self {
            header_bg: Color::Reset,
            header_fg: Color::Reset,
            row_numbers_fg: Color::Reset,
            separator_fg: Color::Reset,
            table_cell_padding: 1,
            alternate_row_bg: None,
            column_colors: false,
            str_col: None,
            int_col: None,
            float_col: None,
            bool_col: None,
            temporal_col: None,
            binary_col: None,
            binary_cols: std::collections::HashSet::new(),
            number_format: NumberFormatSettings::default(),
            dtype_row: false,
            selected_bg: None,
            selection_style: Style::default(),
            accent: Color::Reset,
            dimmed: Color::Reset,
            drift_rows: Vec::new(),
            drift_groups: Arc::new(Vec::new()),
            sort_columns: Vec::new(),
            sort_descending: Vec::new(),
            current_column: None,
            column_cursor_style: Style::default(),
            cell_cursor_style: Style::default(),
            glyphs: crate::glyphs::get(),
            screen_width: 0,
            find_cell: None,
            find_style: Style::default(),
            find_column: None,
            match_cells: None,
            drawn_from: 0,
            units: Vec::new(),
            retyped: Vec::new(),
        }
    }
}

/// Parameters for rendering the row numbers column.
struct RowNumbersParams {
    start_row: usize,
    visible_rows: usize,
    num_rows: usize,
    /// The number each row on screen shows: see [`DataTableState::row_numbers_from`].
    numbers: Vec<usize>,
    selected_row: Option<usize>,
}

/// One column of the rows on screen, formatted once: what the layout measures and
/// what the table draws.
struct ColumnSlice {
    name: String,
    drift_mark: &'static str,
    sort_mark: &'static str,
    /// Cells for the name and both marks.
    header_width: u16,
    type_label: Option<String>,
    type_width: u16,
    cells: Arc<Cells>,
    /// The width the column is laid out at: its stable width once sized (see
    /// [`ColumnWidths`]), until then what shows this page whole.
    width: u16,
    right_align: bool,
    /// Whether a value may be shown clipped beside other columns (see
    /// [`is_truncatable_dtype`]).
    clips: bool,
    cell_style: Option<Style>,
    /// The column's type colour, for its heading.
    colour: Option<Color>,
}

impl ColumnSlice {
    /// The width the column asks for: whole within it, or clipped behind the marker.
    fn natural_width(&self) -> u16 {
        self.width
    }

    fn measure(&self) -> PageMeasure {
        PageMeasure {
            header: self.header_width,
            type_label: self.type_width,
            values: self.cells.value_width,
            has_values: self.cells.has_values,
            clips: self.clips,
        }
    }
}

/// What sizes columns for one draw: the stable widths, the types that key them, and
/// the cap on automatic text.
struct Sizing<'a> {
    widths: &'a mut ColumnWidths,
    schema: &'a Schema,
    cap: u16,
    /// Where the rows drawn come from, when they come from the state: `offset` rows
    /// into the frame on hand, whose columns key the cells kept between frames.
    page: Option<(&'a mut PageCells, &'a DataFrame, usize)>,
}

/// A column's values on screen, formatted, and what they measure.
#[derive(Clone)]
pub(crate) struct Cells {
    cells: Vec<SliceCell>,
    /// Each cell's width, so a scroll measures the page without measuring again.
    widths: Vec<u16>,
    /// Cells for the widest value on screen.
    value_width: u16,
    /// Whether any row on screen holds a value rather than a null.
    has_values: bool,
}

impl Cells {
    fn new(cells: Vec<SliceCell>, widths: Vec<u16>) -> Self {
        Self {
            value_width: widths.iter().copied().max().unwrap_or(0),
            has_values: cells.iter().any(|c| matches!(c, SliceCell::Value(_))),
            cells,
            widths,
        }
    }
}

/// The cells the last frame drew, reused while a column is the same series,
/// formatting, cut, glyphs and null marks: whole when the page is unchanged (cursor
/// move, spinner), and for the rows still on screen after a scroll, so a scroll
/// formats only the rows it brings in.
#[derive(Default)]
pub(crate) struct PageCells {
    columns: std::collections::HashMap<String, KeptCells>,
}

struct KeptCells {
    /// Held so that its address names it: no other series can take the address while
    /// these cells are kept.
    series: Series,
    offset: usize,
    rows: usize,
    format: CellFormatter,
    cut: usize,
    glyphs: &'static crate::glyphs::Glyphs,
    nulls: Vec<&'static str>,
    drift: Vec<u32>,
    cells: Arc<Cells>,
    /// Drawn by the frame being drawn, or by the one before.
    used: bool,
}

impl PageCells {
    /// A new frame: the cells the frame before did not draw are let go.
    pub(crate) fn next_frame(&mut self) {
        self.columns
            .retain(|_, kept| std::mem::take(&mut kept.used));
    }
}

#[derive(Clone)]
enum SliceCell {
    /// A null, drawn as the glyph for its kind of empty.
    Null(&'static str),
    Value(String),
}

/// Where the scrolling columns are, for the off-screen hints over the header.
struct ScrollCue {
    area: Rect,
    more_left: bool,
    /// Columns not drawn at all, right of the last drawn.
    more_right: usize,
}

/// Columns laid out for one side of the table: the columns that fit, the width each
/// gets, and the rows they are drawn for.
struct FittedColumns {
    cols: Vec<ColumnSlice>,
    widths: Vec<u16>,
    rows: usize,
    /// The last column ends at the right edge with more after it: its heading yields its
    /// last cell to the off-screen hint.
    hint_cell: bool,
}

/// The least and most the scrolling side keeps beside frozen columns: a third of the
/// table between, never over half. Enough for any number or timestamp whole.
const MIN_SCROLL_RESERVE: u16 = 12;

const MAX_SCROLL_RESERVE: u16 = 40;

/// Narrower than this, a column is not drawn as a clipped sliver: the clip marker plus
/// at least one cell of what it marks.
fn min_partial_width(g: &crate::glyphs::Glyphs) -> u16 {
    let marker = u16::try_from(crate::glyphs::cell_width(g.ellipsis)).unwrap_or(u16::MAX);
    marker.saturating_add(1).max(3)
}

/// Which side of the frozen separator a layout is for. Both follow one sizing rule;
/// they differ in what they do with a column that does not fit whole.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    /// Only the first column may be clipped, and never as a numeric preview: any other
    /// column that does not fit whole scrolls instead, where it can be read whole.
    Frozen,
    /// The last column shown may be clipped, and a first column with no room for its
    /// values shows a marked preview rather than nothing.
    Scrolling,
}

/// A column's width with `remaining` cells left, or `None` to leave it for the next
/// scroll. Heading and values fit separately (a long heading clips, never hiding
/// values). Text may clip; a number or timestamp that does not fit waits for the scroll
/// (cut, it reads as another value), unless it is the first scrolling column and
/// nothing else would show: then it is clipped behind the marker.
fn fit_column(
    col: &ColumnSlice,
    remaining: u16,
    min_partial: u16,
    first: bool,
    side: Side,
) -> Option<u16> {
    if col.natural_width() <= remaining {
        return Some(col.natural_width());
    }
    if side == Side::Frozen && !first {
        return None;
    }
    if remaining >= min_partial && (col.cells.value_width <= remaining || col.clips) {
        return Some(remaining);
    }
    (side == Side::Scrolling && first && remaining > 0).then_some(remaining)
}

/// Fitted `spans` as one line of a `width`-cell column, right-aligned when `right`.
/// ratatui places spans by string width, which can differ from drawn cells (`لا`,
/// halfwidth marks), so padding is counted in drawn cells and disagreeing spans are
/// merged.
fn cell_line<'a>(mut spans: Vec<Span<'a>>, width: u16, right: bool) -> Line<'a> {
    use unicode_width::UnicodeWidthStr;
    let drawn = |s: &Span| crate::glyphs::cell_width(&s.content);
    if spans.len() > 1 && spans.iter().any(|s| drawn(s) != s.content.width()) {
        let style = spans[0].style;
        let joined: String = spans.iter().map(|s| s.content.as_ref()).collect();
        spans = vec![Span::styled(joined, style)];
    }
    let used: usize = spans.iter().map(drawn).sum();
    let pad = usize::from(width).saturating_sub(used);
    if right && pad > 0 {
        // A cell's padding borrows from one run of spaces rather than making its own.
        const SPACES: &str = "                                                                ";
        let blank = match SPACES.get(..pad) {
            Some(blank) => Span::raw(blank),
            None => Span::raw(" ".repeat(pad)),
        };
        spans.insert(0, blank);
    }
    Line::from(spans)
}

/// The on-screen rows of `df` (`len` from `offset`), as [`visible_slice`], or its
/// columns with no rows when none are on screen, so an empty table draws its header;
/// `None` past the end.
fn visible_or_header(df: &DataFrame, offset: usize, len: usize) -> Option<DataFrame> {
    if df.width() == 0 {
        return None;
    }
    Some(visible_slice(df, offset, len).unwrap_or_else(|| df.clear()))
}

/// Whether a column may be shown truncated: text, bytes, categorical labels and nested
/// previews (a clipped value still reads as its start; whole nested values would hold
/// a huge width). Not numbers, temporals or booleans: cut, they read as wrong values.
fn is_truncatable_dtype(dtype: &DataType) -> bool {
    match dtype {
        DataType::String | DataType::Binary => true,
        other => other.is_categorical() || other.is_enum() || other.is_nested(),
    }
}

impl DataTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_colors(
        mut self,
        header_bg: Color,
        header_fg: Color,
        row_numbers_fg: Color,
        separator_fg: Color,
    ) -> Self {
        self.header_bg = header_bg;
        self.header_fg = header_fg;
        self.row_numbers_fg = row_numbers_fg;
        self.separator_fg = separator_fg;
        self
    }

    pub fn with_cell_padding(mut self, padding: u16) -> Self {
        self.table_cell_padding = padding;
        self
    }

    /// The terminal's width, so automatic widths do not change when a sidebar opens.
    pub fn with_screen_width(mut self, width: u16) -> Self {
        self.screen_width = width;
        self
    }

    pub fn with_alternate_row_bg(mut self, color: Option<Color>) -> Self {
        self.alternate_row_bg = color;
        self
    }

    /// Enable column-type coloring and set colors for string, int, float, bool, and temporal columns.
    pub fn with_column_type_colors(
        mut self,
        str_col: Color,
        int_col: Color,
        float_col: Color,
        bool_col: Color,
        temporal_col: Color,
    ) -> Self {
        self.column_colors = true;
        self.str_col = Some(str_col);
        self.int_col = Some(int_col);
        self.float_col = Some(float_col);
        self.bool_col = Some(bool_col);
        self.temporal_col = Some(temporal_col);
        self
    }

    /// Set the color used for binary-column placeholder cells.
    pub fn with_binary_col(mut self, color: Color) -> Self {
        self.binary_col = Some(color);
        self
    }

    /// Set the names of binary columns, whose cells render the `‹binary›` stub.
    pub fn with_binary_columns(mut self, names: std::collections::HashSet<String>) -> Self {
        self.binary_cols = names;
        self
    }

    /// Set display-time number formatting (digit grouping and alignment).
    pub fn with_number_format(mut self, settings: NumberFormatSettings) -> Self {
        self.number_format = settings;
        self
    }

    /// Show or hide the second header row of column types.
    pub fn with_dtype_row(mut self, on: bool) -> Self {
        self.dtype_row = on;
        self
    }

    /// Tell the table which rows came from files missing which columns, so a cell the
    /// file never had draws differently from a null the data holds.
    pub fn with_drift(
        mut self,
        rows: Vec<u32>,
        groups: Arc<Vec<crate::formats::schema_union::DriftGroup>>,
    ) -> Self {
        self.drift_rows = rows;
        self.drift_groups = groups;
        self
    }

    /// The sort columns and directions for the header marks; the stateful render fills
    /// these itself, the builder is for direct `render_dataframe` callers like tests.
    #[cfg(test)]
    pub fn with_sort(mut self, columns: Vec<String>, descending: Vec<bool>) -> Self {
        debug_assert_eq!(columns.len(), descending.len());
        self.sort_columns = columns;
        self.sort_descending = descending;
        self
    }

    /// The direction mark after a sorted column's name; every column of a multi-sort gets
    /// its own (no position number). Empty when unsorted.
    fn sort_mark_for(&self, column: &str) -> &'static str {
        if let Some(i) = self.sort_columns.iter().position(|c| c == column) {
            let g = self.glyphs;
            if self.sort_descending.get(i).copied().unwrap_or(false) {
                g.sort_desc
            } else {
                g.sort_asc
            }
        } else {
            ""
        }
    }

    /// The footnote mark after a column's name, when it is not in every file or the
    /// files disagree on its type. Empty otherwise.
    fn drift_mark_for(&self, column: &str, drifting: &HashSet<&str>) -> &'static str {
        if drifting.contains(column) {
            self.glyphs.drift_mark
        } else {
            ""
        }
    }

    /// Every column some file lacks, gathered once a frame so most columns cost one hash
    /// lookup.
    fn drifting_columns(&self) -> HashSet<&str> {
        self.drift_groups
            .iter()
            .flat_map(|group| group.absent.iter().chain(group.unread.iter()))
            .map(|name| name.as_str())
            .collect()
    }

    /// What a null in `column` draws as per drift group: the null glyph, the absent glyph
    /// (files without the column), or the conflict glyph (files with another type). Empty
    /// when nothing drifts.
    fn null_glyphs_for(
        &self,
        column: &str,
        g: &'static crate::glyphs::Glyphs,
        drifting: &HashSet<&str>,
    ) -> Vec<&'static str> {
        if self.drift_rows.is_empty() || !drifting.contains(column) {
            return Vec::new();
        }
        self.drift_groups
            .iter()
            .map(|group| {
                if group.absent.iter().any(|c| c == column) {
                    g.absent
                } else if group.unread.iter().any(|c| c == column) {
                    g.conflict
                } else {
                    g.null
                }
            })
            .collect()
    }

    /// The selected row's style and tint, rail color, and dim null color, from the theme's
    /// `highlight_style`; no fallbacks of its own.
    pub fn with_selection_colors(
        mut self,
        selection_style: Style,
        selected_bg: Option<Color>,
        accent: Color,
        dimmed: Color,
    ) -> Self {
        self.selection_style = selection_style;
        self.selected_bg = selected_bg;
        self.accent = accent;
        self.dimmed = dimmed;
        self
    }

    /// Draw these cells (view row, column) as found: a find's matches as it is typed.
    pub fn with_match_cells(
        mut self,
        cells: Option<std::sync::Arc<crate::find::MatchCells>>,
    ) -> Self {
        self.match_cells = cells;
        self
    }

    /// Mark the cell a find landed on, drawn in `style` while the cursor is on it.
    pub fn with_find_cell(mut self, cell: Option<(usize, String)>, style: Style) -> Self {
        self.find_cell = cell;
        self.find_style = style;
        self
    }

    /// The column cursor's styles, from the theme's helpers: its cells, and its
    /// header and the current cell.
    pub fn with_cursor_styles(mut self, column: Style, cell: Style) -> Self {
        self.column_cursor_style = column;
        self.cell_cursor_style = cell;
        self
    }

    /// How many rows the header takes: the names, plus the type row when it is on.
    pub fn header_height(&self) -> u16 {
        if self.dtype_row { 2 } else { 1 }
    }

    /// Style of the highlighted row, from the theme's `highlight_style` helper.
    fn highlight_style(&self) -> Style {
        self.selection_style
    }

    /// Return the color for a column dtype when column_colors is enabled.
    fn column_type_color(&self, dtype: &DataType) -> Option<Color> {
        if !self.column_colors {
            return None;
        }
        match dtype {
            DataType::String => self.str_col,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => self.int_col,
            DataType::Float32 | DataType::Float64 => self.float_col,
            DataType::Boolean => self.bool_col,
            DataType::Date | DataType::Datetime(_, _) | DataType::Time | DataType::Duration(_) => {
                self.temporal_col
            }
            _ => None,
        }
    }

    /// Render `df` into `area` alone with widths learned from this page, returning the
    /// columns shown; for layout tests. `leading_gap` keeps the first column off the left
    /// edge (right of the frozen separator).
    #[cfg(test)]
    pub(crate) fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        leading_gap: bool,
    ) -> usize {
        let mut widths = ColumnWidths::default();
        let sizing = Sizing {
            widths: &mut widths,
            schema: df.schema(),
            cap: self.text_cap(area.width),
            page: None,
        };
        self.render_scrolling(df, area, buf, state, leading_gap, sizing)
            .0
    }

    /// The cap on automatic text widths, from the terminal's width when known.
    fn text_cap(&self, table_width: u16) -> u16 {
        let basis = if self.screen_width > 0 {
            self.screen_width
        } else {
            table_width
        };
        crate::widgets::column_widths::text_cap(basis)
    }

    /// Lay out and draw the scrolling columns, at their stable widths. Returns how
    /// many were drawn.
    fn render_scrolling(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        leading_gap: bool,
        mut sizing: Sizing,
    ) -> (usize, Vec<(u16, u16, String)>, usize) {
        let rows = df
            .height()
            .min((area.height as usize).saturating_sub(self.header_height() as usize));
        let lead = u16::from(leading_gap);
        let mut fitted = self.fit_columns(df, rows, area.width, lead, Side::Scrolling, &mut sizing);
        let shown = fitted.cols.len();
        let mut used = fitted
            .widths
            .iter()
            .fold(lead, |used, &w| used.saturating_add(w))
            + self
                .table_cell_padding
                .saturating_mul(u16::try_from(shown.saturating_sub(1)).unwrap_or(u16::MAX));
        if let Some(filled) =
            self.fill_last_column(&mut fitted, area.width.saturating_sub(used), &mut sizing)
        {
            used = used.saturating_add(filled);
        }
        fitted.hint_cell = shown > 0 && shown < df.width() && used >= area.width;
        let (columns, rows) = self.draw_columns(&fitted, area, buf, state, leading_gap);
        (shown, columns, rows)
    }

    /// Widen the last drawn column by the `room` left at the right edge so long text runs
    /// to the edge. Only an automatic-width, fully drawn, non-right-aligned column. Returns
    /// the cells taken.
    fn fill_last_column(
        &self,
        fitted: &mut FittedColumns,
        room: u16,
        sizing: &mut Sizing,
    ) -> Option<u16> {
        let (col, width) = fitted.cols.last().zip(fitted.widths.last_mut())?;
        if room == 0 || col.right_align || *width < col.natural_width() {
            return None;
        }
        let dtype = sizing.schema.get(col.name.as_str())?;
        if sizing.widths.choice(&col.name, dtype) != WidthChoice::Auto {
            return None;
        }
        *width = width.saturating_add(room);
        sizing.widths.fill(&col.name, dtype, *width);
        Some(room)
    }

    /// The frozen columns that fit beside a usable scrolling side, with widths (`width` is
    /// right of the row numbers). The scrolling side keeps a third of the table (within
    /// bounds); frozen columns that do not fit move to it. Only the first frozen column
    /// may clip to stay frozen, never a number.
    fn fit_frozen_columns(
        &self,
        locked: &DataFrame,
        rows: usize,
        width: u16,
        nothing_else_scrolls: bool,
        table_width: u16,
        sizing: &mut Sizing,
    ) -> FittedColumns {
        // The space before the separator, and the separator. The gap after it is the
        // scrolling side's.
        let room = width.saturating_sub(2);
        if nothing_else_scrolls {
            let fitted = self.fit_columns(locked, rows, room, 0, Side::Frozen, sizing);
            if fitted.cols.len() == locked.width() {
                return fitted;
            }
        }
        let reserve = (table_width / 3)
            .clamp(MIN_SCROLL_RESERVE, MAX_SCROLL_RESERVE)
            .min(table_width / 2);
        self.fit_columns(
            locked,
            rows,
            room.saturating_sub(reserve),
            0,
            Side::Frozen,
            sizing,
        )
    }

    /// Lay `df`'s columns left to right in `width` cells (`lead` first), formatting a
    /// column's first `rows` values only when reached. One rule for frozen and scrolling:
    /// each at its stable width, learned in cells from the rows on screen when first drawn.
    fn fit_columns(
        &self,
        df: &DataFrame,
        rows: usize,
        width: u16,
        lead: u16,
        side: Side,
        sizing: &mut Sizing,
    ) -> FittedColumns {
        let drifting = self.drifting_columns();
        let min_partial = min_partial_width(self.glyphs);
        // Reused across every cell so formatting allocates only the string each cell keeps.
        let mut scratch = String::new();
        let mut fitted = FittedColumns {
            cols: Vec::new(),
            widths: Vec::new(),
            rows,
            hint_cell: false,
        };
        let mut used = lead;
        for col_index in 0..df.width() {
            let remaining = width.saturating_sub(used);
            if remaining == 0 {
                break;
            }
            let source = sizing.page.as_mut().map(|(cells, frame, offset)| {
                let series = frame[col_index].as_materialized_series().clone();
                (&mut **cells, series, *offset)
            });
            let mut col = self.slice_column(df, col_index, rows, &drifting, &mut scratch, source);
            let dtype = sizing
                .schema
                .get(col.name.as_str())
                .unwrap_or_else(|| df[col_index].dtype());
            col.width = sizing
                .widths
                .width(&col.name, dtype, col.measure(), sizing.cap);
            let first = fitted.cols.is_empty();
            let Some(w) = fit_column(&col, remaining, min_partial, first, side) else {
                break;
            };
            let whole = w >= col.natural_width();
            used = used
                .saturating_add(w)
                .saturating_add(self.table_cell_padding);
            fitted.cols.push(col);
            fitted.widths.push(w);
            if !whole {
                // A clipped column took everything left; nothing after it can fit.
                break;
            }
        }
        fitted
    }

    /// The most cells a cell is cut to: past any width it can be drawn at, the
    /// terminal's or one set by hand, so the cut never shows.
    fn cell_cut(&self) -> usize {
        crate::exact::cell_cut(self.screen_width)
    }

    /// The first `rows` values of `col_data`, formatted for their cells and measured.
    /// With `kept`, the cells of a page `shift` rows earlier (the same series and
    /// formatting), each row still on screen is taken from it rather than formatted.
    fn format_cells(
        &self,
        col_data: &Column,
        rows: usize,
        col_fmt: &CellFormatter,
        null_glyph_by_group: &[&'static str],
        scratch: &mut String,
        kept: Option<(Cells, &[u32], isize)>,
    ) -> Cells {
        let mut cells = Vec::with_capacity(rows);
        let mut widths = Vec::with_capacity(rows);
        let (mut old, old_drift, shift) = match kept {
            Some((old, drift, shift)) => (Some(old), drift, shift),
            None => (None, &[][..], 0),
        };
        for row_index in 0..rows {
            let from = old.as_mut().and_then(|old| {
                let at = usize::try_from(row_index as isize + shift).ok()?;
                // A null's glyph follows its row's file, which the scroll moves too.
                (at < old.cells.len() && old_drift.get(at) == self.drift_rows.get(row_index)).then(
                    || {
                        let cell = std::mem::replace(&mut old.cells[at], SliceCell::Null(""));
                        (cell, old.widths[at])
                    },
                )
            });
            let (cell, width) = from.unwrap_or_else(|| {
                self.format_cell(col_data, row_index, col_fmt, null_glyph_by_group, scratch)
            });
            cells.push(cell);
            widths.push(width);
        }
        Cells::new(cells, widths)
    }

    /// Row `row_index` of `col_data`, formatted for its cell, and its width.
    fn format_cell(
        &self,
        col_data: &Column,
        row_index: usize,
        col_fmt: &CellFormatter,
        null_glyph_by_group: &[&'static str],
        scratch: &mut String,
    ) -> (SliceCell, u16) {
        #[cfg(test)]
        tests::FORMATTED.with(|n| n.set(n.get() + 1));
        let g = self.glyphs;
        let width = |text: &str| u16::try_from(crate::glyphs::cell_width(text)).unwrap_or(u16::MAX);
        let value = col_data.get(row_index).unwrap();
        if matches!(value, AnyValue::Null) {
            let glyph = self
                .drift_rows
                .get(row_index)
                .and_then(|group| null_glyph_by_group.get(*group as usize))
                .copied()
                .unwrap_or(g.null);
            return (SliceCell::Null(glyph), width(glyph));
        }
        // A list is previewed here, for the cells on screen only: the buffer keeps
        // it a list, as formatting a whole row group's lists stalled every scroll.
        let text = match &value {
            AnyValue::List(items) => Cow::Owned(crate::exact::list_preview(items)),
            value => numfmt::format_any_value(col_fmt, value, scratch),
        };
        // Breaks and tabs would vanish and run text together; only what a cell can
        // show is kept and measured, not a huge value whole.
        let text = crate::exact::cell_text(text, g, self.cell_cut());
        let w = width(&text);
        (SliceCell::Value(text), w)
    }

    /// One column's heading, type and first `rows` values, formatted and measured.
    fn slice_column(
        &self,
        df: &DataFrame,
        col_index: usize,
        rows: usize,
        drifting: &HashSet<&str>,
        scratch: &mut String,
        source: Option<(&mut PageCells, Series, usize)>,
    ) -> ColumnSlice {
        let g = self.glyphs;
        let col_data = &df[col_index];
        let name = col_data.name().as_str();
        let dtype = col_data.dtype();
        // Binary columns hold the `‹binary›` stub: style them with binary_col + italic so they
        // read as a placeholder rather than data, regardless of the column_colors setting.
        let is_binary = self.binary_cols.contains(name);
        let cell_style = if is_binary {
            let mut s = Style::default().add_modifier(Modifier::ITALIC);
            if let Some(c) = self.binary_col {
                s = s.fg(c);
            }
            Some(s)
        } else {
            self.column_type_color(dtype)
                .map(|c| Style::default().fg(c))
        };
        // Resolved once per column (dtype eligibility, include/exclude globs). Binary columns
        // hold the stub, so pass through.
        let col_fmt = if is_binary {
            CellFormatter::Passthrough
        } else {
            self.number_format.formatter_for(name, dtype)
        };
        // Numeric columns render flush-right so magnitudes line up; strings, booleans,
        // temporals and binary stubs stay left.
        let right_align = self.number_format.align_numeric_right
            && !is_binary
            && numfmt::is_right_aligned_dtype(dtype);
        // A null here may be the data's, a file without the column, or a file storing it in
        // another type: resolved once per column, by group.
        let null_glyph_by_group = self.null_glyphs_for(name, g, drifting);

        let rows = rows.min(col_data.len());
        let drift = &self.drift_rows[..rows.min(self.drift_rows.len())];
        let cut = self.cell_cut();
        let cells = match source {
            Some((page, series, offset)) => match page.columns.get_mut(name) {
                Some(kept)
                    if Arc::ptr_eq(&kept.series.0, &series.0)
                        && kept.format == col_fmt
                        && kept.cut == cut
                        && std::ptr::eq(kept.glyphs, g)
                        && kept.nulls == null_glyph_by_group =>
                {
                    kept.used = true;
                    if kept.offset != offset || kept.rows != rows || kept.drift != drift {
                        let old = std::mem::replace(
                            &mut kept.cells,
                            Arc::new(Cells::new(Vec::new(), Vec::new())),
                        );
                        // The frame before let go of its columns, so the cells move.
                        let old = Arc::try_unwrap(old).unwrap_or_else(|old| (*old).clone());
                        let shift = offset as isize - kept.offset as isize;
                        kept.cells = Arc::new(self.format_cells(
                            col_data,
                            rows,
                            &col_fmt,
                            &null_glyph_by_group,
                            scratch,
                            Some((old, &kept.drift, shift)),
                        ));
                        kept.offset = offset;
                        kept.rows = rows;
                        kept.drift.clear();
                        kept.drift.extend_from_slice(drift);
                    }
                    kept.cells.clone()
                }
                _ => {
                    let cells = Arc::new(self.format_cells(
                        col_data,
                        rows,
                        &col_fmt,
                        &null_glyph_by_group,
                        scratch,
                        None,
                    ));
                    let kept = KeptCells {
                        series,
                        offset,
                        rows,
                        format: col_fmt,
                        cut,
                        glyphs: g,
                        nulls: null_glyph_by_group,
                        drift: drift.to_vec(),
                        cells: cells.clone(),
                        used: true,
                    };
                    page.columns.insert(name.to_string(), kept);
                    cells
                }
            },
            None => Arc::new(self.format_cells(
                col_data,
                rows,
                &col_fmt,
                &null_glyph_by_group,
                scratch,
                None,
            )),
        };

        let drift_mark = self.drift_mark_for(name, drifting);
        let sort_mark = self.sort_mark_for(name);
        // Both header marks widen the column, or a sorted or drifting column's last
        // character would be pushed out of its cell.
        let header_width = crate::glyphs::cell_width(name)
            + crate::glyphs::cell_width(drift_mark)
            + crate::glyphs::cell_width(sort_mark);
        // The type row is part of the header, so a column is at least as wide as its type name
        // (or a spec unit beside it: `f64 · deg F`). Binary's type is the source's.
        let type_label = self.dtype_row.then(|| {
            let label = if is_binary {
                dtype_label(&DataType::Binary)
            } else {
                dtype_label(dtype)
            };
            match self.units.iter().find(|(column, _)| column == name) {
                Some((_, unit)) => format!("{label} {} {unit}", self.glyphs.middot),
                None => label,
            }
        });
        let type_width = type_label
            .as_deref()
            .map(crate::glyphs::cell_width)
            .unwrap_or(0);
        let cells_u16 = |w: usize| u16::try_from(w).unwrap_or(u16::MAX);
        let width = cells_u16(header_width.max(type_width)).max(cells.value_width);
        ColumnSlice {
            name: name.to_string(),
            drift_mark,
            sort_mark,
            header_width: cells_u16(header_width),
            type_label,
            type_width: cells_u16(type_width),
            cells,
            width,
            right_align,
            clips: is_binary || is_truncatable_dtype(dtype),
            cell_style,
            colour: if is_binary {
                self.binary_col
            } else {
                self.column_type_color(dtype)
            },
        }
    }

    /// Draw fitted columns into `area`. Every heading, type and value is pre-fitted at a
    /// grapheme boundary and marked where cut, so ratatui never truncates (it cuts
    /// right-aligned values from the left: `1234567` to `34567`).
    fn draw_columns<'f>(
        &self,
        fitted: &'f FittedColumns,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        leading_gap: bool,
    ) -> (Vec<(u16, u16, String)>, usize) {
        let g = self.glyphs;
        // Borrowed from the cells wherever the text fits whole, which is most cells.
        let fit = |text: &'f str, width: u16| -> Cow<'f, str> {
            crate::glyphs::fit_cells(text, usize::from(width), g.ellipsis)
        };
        // A null is drawn as a glyph in the dim colour, so it can never be mistaken
        // for an empty string or a zero that happens to be blank.
        let null_style = Style::default()
            .fg(self.dimmed)
            .add_modifier(Modifier::ITALIC);
        let columns = || fitted.cols.iter().zip(fitted.widths.iter().copied());

        let rows: Vec<Row> = (0..fitted.rows)
            .map(|row_index| {
                let cells: Vec<Cell> = columns()
                    .map(|(col, w)| {
                        let span = match col.cells.cells.get(row_index) {
                            Some(SliceCell::Null(glyph)) => Span::styled(fit(glyph, w), null_style),
                            Some(SliceCell::Value(text)) => {
                                let mut style = col.cell_style.unwrap_or_default();
                                if self.match_cells.as_ref().is_some_and(|cells| {
                                    cells.get(col.name.as_str()).is_some_and(|rows| {
                                        rows.contains(&(self.drawn_from + row_index))
                                    })
                                }) {
                                    style = style.patch(self.find_style);
                                }
                                Span::styled(fit(text, w), style)
                            }
                            None => return Cell::default(),
                        };
                        // Room for the padding a right-aligned cell puts before it.
                        let mut spans = Vec::with_capacity(1 + usize::from(col.right_align));
                        spans.push(span);
                        Cell::from(cell_line(spans, w, col.right_align))
                    })
                    .collect();
                // Striped by view row, so the stripes move with a scroll and the
                // terminal can move the lines (app/draw.rs).
                let row_style = if (self.drawn_from + row_index) % 2 == 1 {
                    self.alternate_row_bg
                        .map(|c| Style::default().bg(c))
                        .unwrap_or_default()
                } else {
                    Style::default()
                };
                Row::new(cells).style(row_style)
            })
            .collect();

        let header_row_style = if self.header_bg == Color::Reset {
            Style::default().fg(self.header_fg)
        } else {
            Style::default().bg(self.header_bg).fg(self.header_fg)
        };
        // The name takes its column's type color, bold; the type row repeats it plain. Headings
        // follow their column's alignment.
        let last = fitted.cols.len().saturating_sub(1);
        // The column cursor, when its column is among these.
        let cursor = self
            .current_column
            .as_deref()
            .and_then(|name| fitted.cols.iter().position(|c| c.name == name));
        // The current cell is drawn as found while a find's cell is the cursor's: a find
        // moves the cursor to the cell it lands on.
        let cell_style = if cursor.is_some() && self.find_column == self.current_column {
            self.find_style
        } else {
            self.cell_cursor_style
        };
        // Under a reversed row (`table_selected = "reversed"`), a reversed cell would
        // read as the rest of the row: the current cell is the one drawn upright.
        let cell_style = if self
            .highlight_style()
            .add_modifier
            .contains(Modifier::REVERSED)
        {
            cell_style.remove_modifier(Modifier::REVERSED)
        } else {
            cell_style
        };
        let headers: Vec<Cell> = columns()
            .enumerate()
            .map(|(i, (col, w))| {
                // The off-screen hint goes on the type row when there is one, else on
                // the name row: that line of the last column stops a cell short.
                let hint = u16::from(fitted.hint_cell && i == last && w > 1);
                let (name_w, type_w) = if col.type_label.is_some() {
                    (w, w - hint)
                } else {
                    (w - hint, w)
                };
                let name_style = match col.colour {
                    Some(c) => Style::default().fg(c).add_modifier(Modifier::BOLD),
                    None => Style::default().add_modifier(Modifier::BOLD),
                };
                // The marks are state, so a long name gives way to them: the name is
                // what gets clipped, never the sort direction or the drift footnote.
                let marks = crate::glyphs::cell_width(col.drift_mark)
                    + crate::glyphs::cell_width(col.sort_mark);
                let mut heading = Vec::with_capacity(3);
                match u16::try_from(marks).ok().filter(|&m| m < name_w) {
                    Some(marks) => {
                        heading.push(Span::styled(fit(&col.name, name_w - marks), name_style));
                        if !col.drift_mark.is_empty() {
                            heading.push(Span::styled(
                                col.drift_mark,
                                Style::default().fg(self.dimmed),
                            ));
                        }
                        // In the name's own style: the mark says how this column's
                        // values run, so it reads as part of the heading.
                        if !col.sort_mark.is_empty() {
                            heading.push(Span::styled(col.sort_mark, name_style));
                        }
                    }
                    None => heading.push(Span::styled(fit(&col.name, name_w), name_style)),
                }
                let mut lines = vec![cell_line(heading, name_w, col.right_align)];
                if let Some(label) = &col.type_label {
                    let type_style = match col.colour {
                        // A type the view gave, not the read: it shows.
                        _ if self.retyped.contains(&col.name) => Style::default().fg(self.accent),
                        Some(c) => Style::default().fg(c),
                        None => Style::default().fg(self.dimmed),
                    };
                    let label = Span::styled(fit(label, type_w), type_style);
                    lines.push(cell_line(vec![label], type_w, col.right_align));
                }
                let cell = Cell::from(Text::from(lines));
                if cursor == Some(i) {
                    cell.style(self.cell_cursor_style)
                } else {
                    cell
                }
            })
            .collect();

        let mut table = Table::new(rows, fitted.widths.clone())
            .column_spacing(self.table_cell_padding)
            .header(
                Row::new(headers)
                    .style(header_row_style)
                    .height(self.header_height()),
            )
            .row_highlight_style(self.highlight_style())
            .column_highlight_style(self.column_cursor_style)
            .cell_highlight_style(cell_style);
        if leading_gap {
            // A blank selection column on every row, so the gap stripes and highlights with the row.
            table = table
                .highlight_symbol(" ")
                .highlight_spacing(HighlightSpacing::Always);
        }
        // The frozen and scrolling sides share the row selection; the column is each
        // side's own, so it is set for this draw only.
        state.select_column(cursor);
        StatefulWidget::render(table, area, buf, state);
        state.select_column(None);
        // Where the Table put each column: past the gap's selection column, laid out
        // as it lays them out, so a click finds the column it drew.
        let lead = u16::from(leading_gap).min(area.width);
        let columns_area = Rect {
            x: area.x + lead,
            width: area.width - lead,
            ..area
        };
        let spans = ratatui::layout::Layout::horizontal(
            fitted
                .widths
                .iter()
                .map(|&w| ratatui::layout::Constraint::Length(w)),
        )
        .flex(ratatui::layout::Flex::Start)
        .spacing(self.table_cell_padding)
        .split(columns_area);
        let columns = spans
            .iter()
            .zip(&fitted.cols)
            .map(|(span, col)| (span.x, span.right(), col.name.clone()))
            .collect();
        (
            columns,
            fitted.rows.min(usize::from(
                area.height.saturating_sub(self.header_height()),
            )),
        )
    }

    /// A scrolling column's drawn width: as last drawn in this view, or measured from the
    /// held rows on screen as drawing would learn it. Sideways pages are planned with it.
    fn measure_column(
        &self,
        state: &mut DataTableState,
        name: &str,
        offset: usize,
        rows: usize,
        cap: u16,
    ) -> u16 {
        if let Some(width) = state.drawn_width(name) {
            return width;
        }
        let Some(page) = state.page_column(name, offset, rows) else {
            return crate::widgets::column_widths::UNSEEN_WIDTH;
        };
        let col = self.slice_column(
            &page,
            0,
            page.height(),
            &self.drifting_columns(),
            &mut String::new(),
            None,
        );
        let dtype = state.width_dtype(name);
        state.widths.width(name, &dtype, col.measure(), cap)
    }

    /// Fit each column waiting for it to the rows on screen, from the buffer already
    /// held, so a column scrolled out of view is fitted to this page too.
    fn fit_pending(&self, state: &mut DataTableState, offset: usize, rows: usize, cap: u16) {
        let pending = state.widths.fits_pending();
        if pending.is_empty() || !state.buffer_on_hand() {
            return;
        }
        let drifting = self.drifting_columns();
        let mut scratch = String::new();
        for (name, dtype) in pending {
            let Some(page) = state.page_column(&name, offset, rows) else {
                continue;
            };
            let col = self.slice_column(&page, 0, page.height(), &drifting, &mut scratch, None);
            state.widths.fit(&name, &dtype, col.measure(), cap);
        }
    }

    fn render_row_numbers(&self, area: Rect, buf: &mut Buffer, params: RowNumbersParams) {
        // Header row: same style as the rest of the column headers (fill full width so color matches)
        let header_style = if self.header_bg == Color::Reset {
            Style::default().fg(self.header_fg)
        } else {
            Style::default().bg(self.header_bg).fg(self.header_fg)
        };
        let header_h = self.header_height().min(area.height);
        let header_fill = " ".repeat(area.width as usize);
        for dy in 0..header_h {
            Paragraph::new(header_fill.clone())
                .style(header_style)
                .render(
                    Rect {
                        x: area.x,
                        y: area.y + dy,
                        width: area.width,
                        height: 1,
                    },
                    buf,
                );
        }

        // Only render up to the actual number of rows in the data
        let rows_to_render = params
            .visible_rows
            .min(params.num_rows.saturating_sub(params.start_row));

        if rows_to_render == 0 {
            return;
        }

        let number = |row_idx: usize| params.numbers.get(row_idx).copied().unwrap_or_default();
        let max_row_num = (0..rows_to_render).map(number).max().unwrap_or_default();
        let max_width = max_row_num.checked_ilog10().unwrap_or(0) as usize + 1;
        // One line's text, written again for each row.
        let mut padded_text = String::with_capacity(max_width);

        for row_idx in 0..rows_to_render.min(area.height.saturating_sub(header_h) as usize) {
            use std::fmt::Write as _;
            padded_text.clear();
            let _ = write!(padded_text, "{:>max_width$}", number(row_idx));

            // Match the table background (alternate rows striped); the selected row carries the
            // table's highlight tint.
            let is_selected = params.selected_row == Some(row_idx);
            let striped = (self.drawn_from + row_idx) % 2 == 1;
            let (fg, bg) = if is_selected {
                (
                    Color::Reset,
                    self.selected_bg
                        .or(self.alternate_row_bg.filter(|_| striped)),
                )
            } else {
                (
                    self.row_numbers_fg,
                    self.alternate_row_bg.filter(|_| striped),
                )
            };
            let row_num_style = match bg {
                Some(bg_color) => Style::default().fg(fg).bg(bg_color),
                None => Style::default().fg(fg),
            };

            let y = area.y + row_idx as u16 + header_h;
            if y < area.y + area.height {
                let line = Rect {
                    x: area.x,
                    y,
                    width: area.width,
                    height: 1,
                };
                buf.set_style(line, row_num_style);
                buf.set_stringn(
                    line.x,
                    y,
                    &padded_text,
                    usize::from(line.width),
                    Style::default(),
                );
            }
        }
    }
}

impl StatefulWidget for DataTable {
    type State = DataTableState;

    fn render(mut self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        state.page_cells.next_frame();
        if self.screen_width == 0 {
            self.screen_width = area.width;
        }
        // The view's own sort, not the grouped original's: it is what ordered the
        // rows being drawn, so the header marks can never disagree with them.
        (self.sort_columns, self.sort_descending) = state.header_sort();
        self.current_column = state.current_column().map(str::to_string);
        self.units = state.units();
        self.retyped = state.retyped_columns();
        // The leftmost column is the rail: accented on the cursor's row, blank elsewhere, and
        // holding the "columns off to the left" hint in the header.
        let cap = self.text_cap(area.width);
        let whole = area;
        state.drawn = None;
        let rail_area = Rect {
            x: area.x,
            y: area.y,
            width: 1.min(area.width),
            height: area.height,
        };
        let area = Rect {
            x: area.x.saturating_add(1),
            y: area.y,
            width: area.width.saturating_sub(1),
            height: area.height,
        };
        let header_h = self.header_height();
        state.visible_termcols = area.width as usize;
        let new_visible_rows = (area.height as usize).saturating_sub(header_h as usize);
        let visible_rows_changed = new_visible_rows != state.visible_rows;
        state.visible_rows = new_visible_rows;

        // Fewer rows (the footer grew a line): the page starts that much later, so the
        // row the cursor is on stays the row it is on.
        if let Some(selected) = state.table_state.selected()
            && selected >= state.visible_rows
            && state.visible_rows > 0
        {
            let overflow = selected - (state.visible_rows - 1);
            state.view.start_row += overflow;
            state.table_state.select(Some(state.visible_rows - 1));
        }

        // Only a page the rows on hand do not cover needs a read: the footer growing
        // and shrinking a line must not re-read the buffer each time.
        if visible_rows_changed && !state.page_on_hand(state.view.start_row) {
            // The App event loop checks this flag after each render and triggers an
            // async collect.
            state.needs_recollect = true;
        }

        // Only show errors in main view if not suppressed (e.g., when query input is active)
        // Query errors should only be shown in the query input frame
        if let Some(error) = state.error()
            && !state.suppress_error_display
        {
            Paragraph::new(format!("Error: {}", user_message_from_polars(error)))
                .centered()
                .block(
                    Block::default()
                        .borders(Borders::NONE)
                        .padding(Padding::top(area.height / 2)),
                )
                .wrap(ratatui::widgets::Wrap { trim: true })
                .render(area, buf);
            return;
        }
        // If suppress_error_display is true, continue rendering the table normally

        let start_row = state.start_to_draw();
        self.drawn_from = start_row;
        state.on_screen = None;
        // Only on the cursor's row (and, when drawn, its column): the cursor is what a
        // find moves, and a mark left behind would read as a second match.
        let selected = state.table_state.selected();
        self.find_column = self.find_cell.take().and_then(|(row, name)| {
            (row.checked_sub(start_row) == selected && selected.is_some()).then_some(name)
        });

        // Where the scrolling columns are, for the cue drawn over the header after them.
        let mut scroll_indicator: Option<ScrollCue> = None;

        // The numbers `#` shows, and the column wide enough for the widest.
        let numbers = if state.row_numbers() {
            state.row_numbers_from(start_row, state.visible_rows)
        } else {
            Vec::new()
        };
        let row_num_width = if state.row_numbers() {
            let widest = numbers.iter().max().copied().unwrap_or(1);
            widest.to_string().len().max(1) as u16 + 1 // +1 for spacing
        } else {
            0
        };
        let row_num_width = row_num_width.min(area.width);
        let data_area = Rect {
            x: area.x + row_num_width,
            width: area.width - row_num_width,
            ..area
        };
        let row_num_area = Rect {
            width: row_num_width,
            ..area
        };
        let visible_rows = state.visible_rows;
        let row_numbers = |start_row, num_rows, numbers, selected_row| RowNumbersParams {
            start_row,
            visible_rows,
            num_rows,
            numbers,
            selected_row,
        };
        let row_number_params = row_numbers(
            start_row,
            state.num_rows(),
            numbers,
            state.table_state.selected(),
        );

        // Both sides are cut to the same rows on screen, so the frozen columns are
        // measured on what they show, not on the head of the buffer.
        let offset = start_row.saturating_sub(state.view.buffered_start_row);
        let rows_room = (area.height as usize).saturating_sub(header_h as usize);
        let locked_slice = state
            .view
            .locked_df
            .as_ref()
            .and_then(|df| visible_or_header(df, offset, state.visible_rows));
        self.fit_pending(state, offset, state.visible_rows.min(rows_room), cap);

        if state.view.df.is_some() || state.view.locked_df.is_some() {
            if state.row_numbers() {
                self.render_row_numbers(row_num_area, buf, row_number_params);
            }
            let mut drawn_columns = Vec::new();
            let mut drawn_rows = 0;
            let mut scroll_area = data_area;
            let mut leading_gap = false;
            if let Some(locked) = locked_slice {
                let asked = state.locked_columns_count();
                // The rule runs down the header and the rows on screen, and stops
                // under the last: below it is no table to divide.
                let rule_bottom = (area.y + header_h)
                    .saturating_add(locked.height().min(rows_room) as u16)
                    .min(area.bottom());
                let mut fitted = self.fit_frozen_columns(
                    &locked,
                    locked.height().min(rows_room),
                    data_area.width,
                    state.view.column_order.len() <= asked,
                    area.width,
                    &mut Sizing {
                        widths: &mut state.widths,
                        schema: &state.view.schema,
                        cap,
                        page: state
                            .view
                            .locked_df
                            .as_ref()
                            .map(|frame| (&mut state.page_cells, frame, offset)),
                    },
                );
                state.fit_frozen(fitted.cols.len());
                // The state may not take the fit while no buffer is on hand; it then
                // keeps its count, and only what fits of it is drawn.
                let shown = state.frozen_shown().min(fitted.cols.len());
                fitted.cols.truncate(shown);
                fitted.widths.truncate(shown);
                let mut separator_x = data_area.x;
                if shown > 0 {
                    let gaps = self.table_cell_padding.saturating_mul(shown as u16 - 1);
                    let columns_width = fitted.widths.iter().sum::<u16>().saturating_add(gaps);
                    // One cell more than the columns: the space before the separator,
                    // which takes the header fill and the row tints.
                    let frozen_area = Rect {
                        width: columns_width.saturating_add(1).min(data_area.width),
                        ..data_area
                    };
                    let (columns, rows) =
                        self.draw_columns(&fitted, frozen_area, buf, &mut state.table_state, false);
                    drawn_columns.extend(columns);
                    drawn_rows = drawn_rows.max(rows);
                    separator_x = frozen_area.right();
                }
                if separator_x < data_area.right() {
                    // A broken rule while some frozen columns had to scroll: the window
                    // holds fewer than were asked for, and they come back with room.
                    let rule = if shown < asked {
                        self.glyphs.rule_broken
                    } else {
                        self.glyphs.rule
                    };
                    for y in area.y..rule_bottom {
                        let cell = &mut buf[(separator_x, y)];
                        cell.set_symbol(rule);
                        cell.set_style(Style::default().fg(self.separator_fg));
                    }
                }
                let scroll_x = separator_x.saturating_add(1).min(data_area.right());
                scroll_area = Rect {
                    x: scroll_x,
                    width: data_area.right() - scroll_x,
                    ..data_area
                };
                leading_gap = true;
            }
            // A page asked for lands here, where the room it is planned in is known.
            let room = Room {
                width: scroll_area.width,
                lead: u16::from(leading_gap),
                padding: self.table_cell_padding,
            };
            let rows = state.visible_rows.min(rows_room);
            state.land_column_moves(room, |state, name| {
                self.measure_column(state, name, offset, rows, cap)
            });
            if let Some(sliced_df) = state
                .view
                .df
                .as_ref()
                .and_then(|df| visible_or_header(df, offset, state.visible_rows))
            {
                let total_cols = sliced_df.width();
                let (shown, columns, rows) = self.render_scrolling(
                    &sliced_df,
                    scroll_area,
                    buf,
                    &mut state.table_state,
                    leading_gap,
                    Sizing {
                        widths: &mut state.widths,
                        schema: &state.view.schema,
                        cap,
                        page: state
                            .view
                            .df
                            .as_ref()
                            .map(|frame| (&mut state.page_cells, frame, offset)),
                    },
                );
                drawn_columns.extend(columns);
                drawn_rows = drawn_rows.max(rows);
                let more_left = state.termcol_index > 0;
                let more_right = total_cols.saturating_sub(shown);
                let first = state.frozen_shown() + state.termcol_index + 1;
                let total = state.view.column_order.len();
                state.on_screen =
                    state
                        .cursor_index()
                        .filter(|_| total > 1)
                        .map(|cursor| OnScreen {
                            first,
                            last: first + shown.saturating_sub(1),
                            cursor: cursor + 1,
                            total,
                        });
                scroll_indicator = Some(ScrollCue {
                    area: scroll_area,
                    more_left,
                    more_right,
                });
            } else {
                // Every column frozen: all on screen, and the cursor walks them.
                let total = state.view.column_order.len();
                state.on_screen =
                    state
                        .cursor_index()
                        .filter(|_| total > 1)
                        .map(|cursor| OnScreen {
                            first: 1,
                            last: total,
                            cursor: cursor + 1,
                            total,
                        });
            }
            state.drawn = Some(DrawnTable {
                area: whole,
                header: header_h,
                start_row,
                rows: drawn_rows,
                columns: drawn_columns,
            });
        } else if !state.view.column_order.is_empty() {
            // No rows on hand, but a schema: the header alone, each column its own type.
            let empty_columns: Vec<_> = state
                .view
                .column_order
                .iter()
                .map(|name| {
                    let dtype = state
                        .view
                        .schema
                        .get(name.as_str())
                        .cloned()
                        .unwrap_or(DataType::String);
                    Series::new_empty(name.as_str().into(), &dtype).into()
                })
                .collect();
            match DataFrame::new_infer_height(empty_columns) {
                Ok(empty_df) => {
                    if state.row_numbers() {
                        self.render_row_numbers(
                            row_num_area,
                            buf,
                            row_numbers(0, 0, Vec::new(), None),
                        );
                    }
                    self.render_scrolling(
                        &empty_df,
                        data_area,
                        buf,
                        &mut state.table_state,
                        false,
                        Sizing {
                            widths: &mut state.widths,
                            schema: &state.view.schema,
                            cap,
                            page: None,
                        },
                    );
                }
                _ => {
                    Paragraph::new("No data").render(area, buf);
                }
            }
        } else {
            // Truly empty: no schema, not loaded, or blank file
            Paragraph::new("No data").render(area, buf);
        }

        // A table known to hold no rows says so under its header.
        let empty = state.view.num_rows_valid
            && state.num_rows() == 0
            && !state.view.column_order.is_empty();
        if empty && area.height > header_h && data_area.width > 0 {
            let line = Rect {
                y: area.y + header_h,
                height: 1,
                ..data_area
            };
            Paragraph::new("No rows")
                .style(Style::default().fg(self.dimmed))
                .render(line, buf);
        }

        // The rail: the header rows take the header fill so the bar runs edge to edge,
        // and the selected row gets the accent mark.
        if rail_area.width > 0 && rail_area.height > 0 {
            let g = self.glyphs;
            let header_style = if self.header_bg == Color::Reset {
                Style::default().fg(self.header_fg)
            } else {
                Style::default().bg(self.header_bg).fg(self.header_fg)
            };
            for dy in 0..header_h.min(rail_area.height) {
                let cell = &mut buf[(rail_area.x, rail_area.y + dy)];
                cell.set_char(' ');
                cell.set_style(header_style);
            }
            if state.view.df.is_some()
                && !empty
                && let Some(sel) = state.table_state.selected()
            {
                let y = rail_area.y + header_h + sel as u16;
                if y < rail_area.y + rail_area.height {
                    let cell = &mut buf[(rail_area.x, y)];
                    cell.set_symbol(g.rail.trim_end());
                    let mut style = Style::default()
                        .fg(self.accent)
                        .add_modifier(Modifier::BOLD);
                    if let Some(bg) = self.selected_bg {
                        style = style.bg(bg);
                    }
                    cell.set_style(style);
                }
            }
        }

        // Off-screen hints: the left in the rail; the right counts hidden columns, on the type
        // row if shown (short labels leave room), else the name row, after the last column.
        if let Some(cue) = scroll_indicator
            && cue.area.width > 0
            && cue.area.height > 0
        {
            let g = self.glyphs;
            let scroll_area = cue.area;
            let hidden = cue.more_right;
            let hint_style = if self.header_bg == Color::Reset {
                Style::default()
                    .fg(self.accent)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .bg(self.header_bg)
                    .fg(self.accent)
                    .add_modifier(Modifier::BOLD)
            };
            if cue.more_left && rail_area.width > 0 {
                let cell = &mut buf[(rail_area.x, rail_area.y)];
                cell.set_symbol(g.arrow_left);
                cell.set_style(hint_style);
            }
            if hidden > 0 {
                let y = if header_h > 1 {
                    scroll_area.y + 1
                } else {
                    scroll_area.y
                };
                // The count when the blank run at the end of the row holds it, the arrow
                // alone when not, so the count never covers a heading or a type.
                let right = scroll_area.x + scroll_area.width;
                let free = (scroll_area.x..right)
                    .rev()
                    .take_while(|&x| buf[(x, y)].symbol() == " ")
                    .count();
                let mut text = format!(" +{hidden} {}", g.arrow_right);
                if text.chars().count() > free {
                    text = g.arrow_right.to_string();
                }
                let w = text.chars().count() as u16;
                if scroll_area.width >= w {
                    let x0 = right - w;
                    for (i, ch) in text.chars().enumerate() {
                        let cell = &mut buf[(x0 + i as u16, y)];
                        cell.set_char(ch);
                        cell.set_style(hint_style);
                    }
                }
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod tests;
