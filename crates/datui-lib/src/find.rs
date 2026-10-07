//! Find in the table: `/` (or `f`) asks for a pattern, `n` and `N` move between the
//! cells that hold it.
//!
//! As the pattern is typed, the cells that match among the rows on hand light up and
//! are counted (`3 on screen`); nothing is read for that. Ctrl+G keeps only the rows
//! that match, as a filter the Sort & Filter sidebar lists.
//!
//! The view is searched as it stands — query, filters, sort and the columns shown —
//! and never changed: only the cursor moves. The search runs off the UI thread as a
//! job, reading the view a window at a time the way pages are read, starting from the
//! buffer the table already holds. It looks for the next match only: nothing counts
//! every match, so a find near the cursor reads little, and the count of matches is
//! known only as far as the finds so far have walked from the top.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use polars::prelude::*;

use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::jobs::{Answer, Job, Progress};
use crate::table::ViewRows;
use crate::widgets::text_input::{TextInput, TextInputEvent};
use crate::{App, AppEvent, InputMode, InputType};

/// The row index a window is read with.
const ROW: &str = "__datui_find_row";
/// The rows a window held.
const ROWS: &str = "__datui_find_rows";
/// Rows in the first window read past the buffer. Each window after it is twice the
/// one before: a view that cannot skip to a window (a filter, a CSV) reads up to it
/// every time, and doubling keeps that within twice the rows a single pass would
/// read, while a match a page past the buffer costs one small read. A view that sees
/// every row before its first (a sort) is read in one window instead, and so is the
/// range behind the cursor on a view that cannot skip: its first window back would
/// read all of it anyway. Forward on such a view, a window is never smaller than the
/// rows above it, which it reads too.
const FIRST_WINDOW: usize = 65_536;
/// The most rows one window reads: a slice's length is a `u32`.
const LARGEST_WINDOW: usize = u32::MAX as usize;

/// What a find looks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FindSpec {
    pub pattern: String,
    /// The pattern is a regular expression rather than plain text.
    pub regex: bool,
    /// The pattern's letters in order, anything between: `smth` finds `Smith`.
    /// Spaces in the pattern are ignored.
    pub fuzzy: bool,
    /// Only this column, when set; otherwise every column shown.
    pub column: Option<String>,
}

impl FindSpec {
    /// Smart case: a pattern with no capital letter ignores case. In a regex an
    /// escape such as `\S` is a class, not a capital.
    pub fn ignores_case(&self) -> bool {
        let mut chars = self.pattern.chars();
        while let Some(c) = chars.next() {
            if self.regex && c == '\\' {
                chars.next();
                continue;
            }
            if c.is_uppercase() {
                return false;
            }
        }
        true
    }

    /// The regex a cell's text is matched with, or `None` for a case-sensitive plain
    /// pattern, which is matched literally.
    pub(crate) fn regex_source(&self) -> Option<String> {
        let case = if self.ignores_case() { "(?i)" } else { "" };
        if self.fuzzy {
            let letters: Vec<String> = self
                .pattern
                .chars()
                .filter(|c| !c.is_whitespace())
                .map(|c| regex::escape(&c.to_string()))
                .collect();
            return Some(format!("{case}{}", letters.join(".*")));
        }
        match (self.regex, self.ignores_case()) {
            (false, false) => None,
            (false, true) => Some(format!("{case}{}", regex::escape(&self.pattern))),
            (true, _) => Some(format!("{case}{}", self.pattern)),
        }
    }

    /// Why the pattern cannot be searched, in one line.
    pub fn check(&self) -> Result<(), String> {
        let Some(source) = self.regex_source().filter(|_| self.regex && !self.fuzzy) else {
            return Ok(());
        };
        regex::Regex::new(&source).map(|_| ()).map_err(|e| {
            // The crate's message draws the pattern with a caret under it; the last
            // line is the reason.
            let text = e.to_string();
            let reason = text
                .lines()
                .rev()
                .find(|line| !line.trim().is_empty())
                .unwrap_or("invalid")
                .trim()
                .trim_start_matches("error: ")
                .to_string();
            format!("Not a regex: {reason}")
        })
    }

    /// Whether a cell's `text` matches; false for a null.
    fn matches(&self, text: Expr) -> Expr {
        let found = match self.regex_source() {
            None => text.str().contains_literal(lit(self.pattern.clone())),
            Some(source) => text.str().contains(lit(source), true),
        };
        found.fill_null(lit(false))
    }

    /// How the pattern reads on the footer: quoted plain text, or a regex
    /// between slashes, cut short when long.
    pub fn label(&self) -> String {
        const LONGEST: usize = 18;
        let g = crate::glyphs::get();
        let mut text: String = self.pattern.chars().take(LONGEST).collect();
        if self.pattern.chars().count() > LONGEST {
            text.push_str(g.ellipsis);
        }
        if self.fuzzy {
            format!("~{text}")
        } else if self.regex {
            format!("/{text}/")
        } else {
            format!("\"{text}\"")
        }
    }
}

/// Whether a cell of column `name` matches `spec`, or `None` for a column a find
/// skips: the one place a cell is matched, for a find and for the filter it keeps.
pub(crate) fn cell_matches(spec: &FindSpec, name: &str, dtype: &DataType) -> Option<Expr> {
    Some(spec.matches(text_of(name, dtype)?))
}

/// A column's values as text to match, or `None` for a column a find skips: bytes,
/// nested values and nulls have no text of their own.
fn text_of(name: &str, dtype: &DataType) -> Option<Expr> {
    if dtype.is_nested()
        || dtype.is_object()
        || matches!(
            dtype,
            DataType::Binary | DataType::BinaryOffset | DataType::Null
        )
    {
        return None;
    }
    let column = col(name);
    Some(if dtype.is_string() {
        column
    } else if let DataType::Duration(unit) = dtype {
        // Polars has no cast from a duration to text; written as the table shows it.
        duration_text(column, *unit)
    } else if crate::past_calendar::can_leave_calendar(dtype) {
        // A plain cast panics on a date past the calendar; this writes it as its
        // stored number, as the table does.
        crate::past_calendar::text_expr(column, polars::chunked_array::cast::CastOptions::NonStrict)
    } else {
        column.cast(DataType::String)
    })
}

/// A duration column as the text the table shows for it, such as `1d 2h`.
fn duration_text(column: Expr, unit: TimeUnit) -> Expr {
    column.map_with_fmt_str(
        move |c| {
            let text = c
                .as_materialized_series()
                .to_physical_repr()
                .i64()?
                .apply_into_string_amortized(|v, out| {
                    use std::fmt::Write;
                    let _ = write!(out, "{}", AnyValue::Duration(v, unit));
                });
            Ok(text.with_name(c.name().clone()).into_column())
        },
        |_: &Schema, field: &Field| Ok(Field::new(field.name().clone(), DataType::String)),
        "find_duration_text",
    )
}

/// The columns a find over `order` reads, in that order, and the match of each.
pub(crate) fn searched_columns(
    order: &[String],
    schema: &Schema,
    spec: &FindSpec,
) -> Vec<(String, Expr)> {
    order
        .iter()
        .filter(|name| spec.column.as_ref().is_none_or(|only| only == *name))
        .filter_map(|name| {
            let text = text_of(name, schema.get(name)?)?;
            Some((name.clone(), spec.matches(text)))
        })
        .collect()
}

/// Which way a find goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Next,
    Previous,
}

/// Where a find starts: a view row, and in it the cursor's place among the columns
/// searched. With no place the whole row is in reach: `f` finds the first match at or
/// after the cursor.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Start {
    pub(crate) row: usize,
    pub(crate) column: Option<At>,
}

/// The cursor's place in its row, as an index into the columns searched.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum At {
    /// On a searched column.
    On(usize),
    /// On a column not searched, just before this searched one (or past the last).
    Before(usize),
}

impl At {
    /// The first searched column past the cursor.
    fn ahead(self) -> usize {
        match self {
            At::On(c) => c + 1,
            At::Before(c) => c,
        }
    }

    /// The searched columns before this one are behind the cursor.
    fn behind(self) -> usize {
        match self {
            At::On(c) | At::Before(c) => c,
        }
    }
}

/// The cell a find landed on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Found {
    pub row: usize,
    pub column: String,
    /// It went past the end (or the start) and came round to reach it.
    pub wrapped: bool,
}

/// In row `row`, only the columns in `columns` are in reach: the cells on the far
/// side of the start are left to the other half of the search.
#[derive(Debug, Clone)]
struct Limit {
    row: usize,
    columns: Range<usize>,
}

/// Why a search stopped before it answered.
pub(crate) const CANCELLED: &str = "Find cancelled";

/// One search through a view, on a worker.
pub(crate) struct Search {
    rows: ViewRows,
    columns: Vec<String>,
    exprs: Vec<Expr>,
    stop: Arc<AtomicBool>,
    report: Box<dyn Fn(usize) + Send>,
    /// Rows read so far, for the progress line.
    read: usize,
    /// Rows in the next window read past the buffer.
    window: usize,
}

impl Search {
    pub(crate) fn new(
        rows: ViewRows,
        columns: Vec<(String, Expr)>,
        stop: Arc<AtomicBool>,
        report: impl Fn(usize) + Send + 'static,
    ) -> Self {
        let (names, exprs): (Vec<String>, Vec<Expr>) = columns
            .into_iter()
            .enumerate()
            .map(|(i, (name, expr))| (name, expr.alias(format!("m{i}"))))
            .unzip();
        // The buffer serves only while it holds every column searched: hiding a
        // column re-reads it narrower.
        let mut rows = rows;
        if rows
            .buffer
            .as_ref()
            .is_some_and(|(df, _)| names.iter().any(|n| df.column(n).is_err()))
        {
            rows.buffer = None;
        }
        // A sort costs a whole pass for any window, so it gets one.
        let window = if rows.whole {
            LARGEST_WINDOW
        } else {
            FIRST_WINDOW
        };
        Self {
            rows,
            columns: names,
            exprs,
            stop,
            report: Box::new(report),
            read: 0,
            window,
        }
    }

    /// Find the next match from `start` going `direction`, wrapping round the view
    /// once. `None` when nothing in the view matches.
    pub(crate) fn run(
        mut self,
        start: Start,
        direction: Direction,
    ) -> Result<Option<Found>, String> {
        let n = self.columns.len();
        let r = start.row;
        let found = |(row, column): (usize, usize), wrapped: bool, columns: &[String]| Found {
            row,
            column: columns[column].clone(),
            wrapped,
        };
        match direction {
            Direction::Next => {
                let ahead = start.column.map(|at| Limit {
                    row: r,
                    columns: at.ahead()..n,
                });
                if let Some(hit) = self.forward(r, None, ahead.as_ref(), false)? {
                    return Ok(Some(found(hit, false, &self.columns)));
                }
                // Round from the top: up to the start row, and in it the cells up to
                // the cursor's, which is a match of its own when it is the only one.
                let (end, behind) = match start.column {
                    Some(at) => (
                        r + 1,
                        Some(Limit {
                            row: r,
                            columns: 0..at.ahead(),
                        }),
                    ),
                    None => (r, None),
                };
                Ok(self
                    .forward(0, Some(end), behind.as_ref(), false)?
                    .map(|hit| found(hit, true, &self.columns)))
            }
            Direction::Previous => {
                let behind = start.column.map(|at| Limit {
                    row: r,
                    columns: 0..at.behind(),
                });
                if let Some(hit) = self.backward(0, r + 1, behind.as_ref())? {
                    return Ok(Some(found(hit, false, &self.columns)));
                }
                // Round from the bottom, down to the cursor's cell.
                let (from, ahead) = match start.column {
                    Some(at) => (
                        r,
                        Some(Limit {
                            row: r,
                            columns: at.behind()..n,
                        }),
                    ),
                    None => (r + 1, None),
                };
                let hit = match self.rows.num_rows {
                    Some(total) => self.backward(from, total, ahead.as_ref())?,
                    // With no count, the end is found by reading to it.
                    None => self.forward(from, None, ahead.as_ref(), true)?,
                };
                Ok(hit.map(|hit| found(hit, true, &self.columns)))
            }
        }
    }

    /// The first match in rows `[from, end)`, or the last with `last`. With no `end`
    /// the rows run to the view's end, found by reading to it.
    fn forward(
        &mut self,
        from: usize,
        end: Option<usize>,
        limit: Option<&Limit>,
        last: bool,
    ) -> Result<Option<(usize, usize)>, String> {
        let end = end.or(self.rows.num_rows);
        let mut at = from;
        let mut best = None;
        while end.is_none_or(|end| at < end) {
            self.check_stop()?;
            let (len, buffered) = self.plan_forward(at, end, last);
            if len == 0 {
                break;
            }
            let (rows, hit) = self.window_at(at, len, buffered, limit, last)?;
            if hit.is_some() {
                if !last {
                    return Ok(hit);
                }
                best = hit;
            }
            if rows < len {
                break;
            }
            at += len;
        }
        Ok(best)
    }

    /// The last match in rows `[from, end)`, read from the end down.
    fn backward(
        &mut self,
        from: usize,
        end: usize,
        limit: Option<&Limit>,
    ) -> Result<Option<(usize, usize)>, String> {
        let mut end = end;
        while end > from {
            self.check_stop()?;
            let (start, buffered) = self.plan_backward(from, end);
            let (_, hit) = self.window_at(start, end - start, buffered, limit, true)?;
            if hit.is_some() {
                return Ok(hit);
            }
            end = start;
        }
        Ok(None)
    }

    fn check_stop(&self) -> Result<(), String> {
        if self.stop.load(Ordering::Relaxed) {
            return Err(CANCELLED.to_string());
        }
        Ok(())
    }

    /// The buffer's rows, as `(start, end)`.
    fn buffered(&self) -> Option<(usize, usize)> {
        self.rows
            .buffer
            .as_ref()
            .map(|(df, start)| (*start, start + df.height()))
    }

    /// The next window from `at`: the rest of the buffer when `at` is in it, else
    /// rows read from the view, stopping short of the buffer. For the `last` match,
    /// every row to the end is read, so a view that cannot skip reads them at once.
    fn plan_forward(&mut self, at: usize, end: Option<usize>, last: bool) -> (usize, bool) {
        let end = end.unwrap_or(usize::MAX);
        if let Some((start, stop)) = self.buffered()
            && (start..stop).contains(&at)
        {
            return (stop.min(end) - at, true);
        }
        let mut window = self.window_for(last);
        // A view that cannot skip reads the rows above a window with it: a window no
        // smaller than them keeps a find from deep in the view from reading them
        // again for every doubling.
        if self.rows.reads_up_to {
            window = window.max(at);
        }
        let mut stop = at.saturating_add(window).min(end);
        if let Some((start, _)) = self.buffered()
            && at < start
        {
            stop = stop.min(start);
        }
        self.window = window.saturating_mul(2).min(LARGEST_WINDOW);
        (stop - at, false)
    }

    /// Rows in the next window read from the view. Read backwards, or to the end, a
    /// view that cannot skip to a window takes the whole range in one.
    fn window_for(&self, whole_range: bool) -> usize {
        if whole_range && self.rows.reads_up_to {
            LARGEST_WINDOW
        } else {
            self.window
        }
    }

    /// The window that ends at `end`, no lower than `from`: the buffer's rows when
    /// the row before `end` is in it, else rows read from the view.
    fn plan_backward(&mut self, from: usize, end: usize) -> (usize, bool) {
        if let Some((start, stop)) = self.buffered()
            && (start..stop).contains(&(end - 1))
        {
            return (start.max(from), true);
        }
        let mut start = end.saturating_sub(self.window_for(true)).max(from);
        if let Some((_, stop)) = self.buffered()
            && end > stop
        {
            start = start.max(stop);
        }
        self.window = self.window.saturating_mul(2).min(LARGEST_WINDOW);
        (start, false)
    }

    /// Read rows `[start, start + len)` and find the first match among them (the
    /// last with `last`). Returns the rows there were, and the match as its row and
    /// column. One aggregate per column: the window's matching cells are never
    /// collected, only where the first (or last) one is.
    fn window_at(
        &mut self,
        start: usize,
        len: usize,
        buffered: bool,
        limit: Option<&Limit>,
        last: bool,
    ) -> Result<(usize, Option<(usize, usize)>), String> {
        let message = |e: PolarsError| crate::error_display::user_message_from_polars(&e);
        let lf = match self.rows.buffer.as_ref().filter(|_| buffered) {
            Some((df, at)) => df
                .slice((start - at) as i64, len)
                .lazy()
                .select(self.exprs.clone()),
            None => self
                .rows
                .window(start, len, self.exprs.clone())
                .map_err(message)?,
        };
        let offset = IdxSize::try_from(start)
            .map_err(|_| "The view has too many rows to find in".to_string())?;
        let row = || col(ROW).cast(DataType::UInt64);
        let mut aggregates = vec![polars::prelude::len().cast(DataType::UInt64).alias(ROWS)];
        for i in 0..self.columns.len() {
            let mut cell = col(format!("m{i}"));
            if let Some(limit) = limit
                && !limit.columns.contains(&i)
            {
                cell = cell.and(row().neq(lit(limit.row as u64)));
            }
            let rows = row().filter(cell);
            let at = if last { rows.max() } else { rows.min() };
            aggregates.push(at.alias(format!("f{i}")));
        }
        let lf = lf.with_row_index(ROW, Some(offset)).select(aggregates);
        let df = crate::statistics::collect_lazy(lf, self.rows.streaming).map_err(message)?;
        let get = |name: &str| -> Option<u64> { df.column(name).ok()?.u64().ok()?.get(0) };
        let rows = get(ROWS).unwrap_or(0) as usize;
        let mut best: Option<(usize, usize)> = None;
        for i in 0..self.columns.len() {
            let Some(at) = get(&format!("f{i}")).map(|r| r as usize) else {
                continue;
            };
            // Reading order: the first row, and in it the first column; backwards,
            // the last row and the last column.
            let better = best.is_none_or(|(b, _)| if last { at >= b } else { at < b });
            if better {
                best = Some((at, i));
            }
        }
        self.read += rows;
        (self.report)(self.read);
        Ok((rows, best))
    }
}

/// The find prompt, and the find in effect.
pub struct Find {
    pub input: TextInput,
    /// The prompt's pattern is a regex.
    pub regex: bool,
    /// The prompt's pattern is letters in order.
    pub fuzzy: bool,
    /// The prompt limits the find to `column`.
    pub in_column: bool,
    /// The column the prompt opened on: the column cursor's.
    pub column: Option<String>,
    /// Why the pattern typed cannot be searched.
    pub error: Option<String>,
    /// The find `n` and `N` repeat.
    pub active: Option<ActiveFind>,
    /// The cells the prompt's pattern matches among the rows on hand.
    pub live: Option<LiveMatches>,
    /// The rows on hand `live` was worked out over: their first view row, how many,
    /// and the frame. Rows that arrive or a new frame make it stale.
    live_rows: Option<(usize, usize, u64)>,
    /// Rows the find reading has read, for the footer's progress line.
    pub read: Option<usize>,
}

/// The view rows a pattern being typed matches among the rows on hand, by column
/// name: looked up by the table as it draws, without a name cloned per cell.
pub type MatchCells = std::collections::HashMap<String, std::collections::HashSet<usize>>;

/// The cells a pattern being typed matches among the rows on hand. Worked out in
/// memory as the pattern or the rows on hand change; never read.
#[derive(Debug, Clone, Default)]
pub struct LiveMatches {
    pub cells: Arc<MatchCells>,
}

impl LiveMatches {
    /// The matches in view rows `rows`: the count the prompt shows.
    pub fn within(&self, rows: Range<usize>) -> usize {
        self.cells
            .values()
            .map(|hits| hits.iter().filter(|r| rows.contains(r)).count())
            .sum()
    }
}

/// The find in effect, and where it last landed.
#[derive(Debug, Clone)]
pub struct ActiveFind {
    pub spec: FindSpec,
    /// The dataset it was made for.
    pub dataset: u64,
    /// The frame (`len_generation`) its cell is a cell of.
    pub frame: u64,
    /// The cell it landed on: view row and column name.
    pub hit: Option<(usize, String)>,
    /// Which match that is, counting from the top, when the finds so far say.
    pub ordinal: Option<usize>,
}

impl Find {
    pub fn new(input: TextInput) -> Self {
        Self {
            input,
            regex: false,
            fuzzy: false,
            in_column: false,
            column: None,
            error: None,
            active: None,
            live: None,
            live_rows: None,
            read: None,
        }
    }

    /// The spec the prompt describes.
    pub(crate) fn prompt_spec(&self) -> FindSpec {
        FindSpec {
            pattern: self.input.value().to_string(),
            regex: self.regex,
            fuzzy: self.fuzzy,
            column: self.column.clone().filter(|_| self.in_column),
        }
    }
}

/// A find running on a worker: how to stop it, and what its answer is judged by.
#[derive(Debug, Clone)]
pub(crate) struct FindRun {
    pub(crate) stop: Arc<AtomicBool>,
    pub(crate) dataset: u64,
    pub(crate) frame: u64,
    pub(crate) direction: Direction,
    /// It started before every cell of the view, so its first match is match 1.
    pub(crate) from_top: bool,
    /// It started from the cell the last find landed on, with that match's number.
    pub(crate) from_hit: Option<Option<usize>>,
}

/// What the footer says while a find reads.
fn finding_status(spec: &FindSpec, read: Option<usize>) -> String {
    match read {
        // Abbreviated, as the row count beside it is: the line shares a narrow bar
        // with the way out and the count.
        Some(rows) if rows > 0 => format!(
            "Finding {}... {} rows",
            spec.label(),
            crate::discover::format_rows(rows)
        ),
        _ => format!("Finding {}...", spec.label()),
    }
}

impl App {
    /// `/` (or `f`) at the table: the find prompt, holding the last pattern, selected
    /// so that typing replaces it.
    pub(crate) fn open_find(&mut self) {
        if self.data_table_state.is_none() {
            return;
        }
        self.prompt.find.column = self.find_column();
        self.prompt.find.error = None;
        match self.prompt.find.active.as_ref() {
            Some(active) => {
                let pattern = active.spec.pattern.clone();
                self.prompt.find.input.set_value(pattern);
                self.prompt.find.input.select_all();
            }
            None => self.prompt.find.input.clear(),
        }
        self.prompt.find.input.set_focused(true);
        self.input_mode = InputMode::Editing;
        self.prompt.input_type = Some(InputType::Find);
        self.refresh_live_matches();
    }

    /// Light up the cells the prompt's pattern matches among the rows on screen and a
    /// page either side. Only rows already in memory are matched, so typing never waits
    /// on a read, and only those near the view, so a buffer of a whole row group is not
    /// matched on every key.
    pub(crate) fn refresh_live_matches(&mut self) {
        self.prompt.find.live = None;
        self.prompt.find.live_rows = self.rows_on_hand_key();
        let spec = self.prompt.find.prompt_spec();
        if spec.pattern.trim().is_empty() || spec.check().is_err() {
            return;
        }
        let Some((df, start)) = self.live_window() else {
            return;
        };
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let columns: Vec<(String, Expr)> =
            searched_columns(state.get_column_order(), state.schema(), &spec)
                .into_iter()
                .filter(|(name, _)| df.column(name).is_ok())
                .collect();
        if columns.is_empty() {
            self.prompt.find.live = Some(LiveMatches::default());
            return;
        }
        let exprs: Vec<Expr> = columns
            .iter()
            .enumerate()
            .map(|(i, (_, expr))| expr.clone().alias(format!("m{i}")))
            .collect();
        // In memory: the rows on hand are a frame already collected.
        let Ok(found) = df.lazy().select(exprs).collect() else {
            return;
        };
        let mut cells = MatchCells::new();
        for (i, (name, _)) in columns.iter().enumerate() {
            let Ok(hits) = found
                .column(&format!("m{i}"))
                .and_then(|c| c.bool().cloned())
            else {
                continue;
            };
            let rows: std::collections::HashSet<usize> = hits
                .iter()
                .enumerate()
                .filter(|(_, hit)| *hit == Some(true))
                .map(|(row, _)| start + row)
                .collect();
            if !rows.is_empty() {
                cells.insert(name.clone(), rows);
            }
        }
        self.prompt.find.live = Some(LiveMatches {
            cells: Arc::new(cells),
        });
    }

    /// The rows the live find matches: those on screen and a page either side, of
    /// the rows on hand, and the row the first is.
    fn live_window(&self) -> Option<(DataFrame, usize)> {
        let state = self.data_table_state.as_ref()?;
        let (df, start) = state.rows_on_hand()?;
        let page = state.visible_rows.max(1);
        let from = state.start_row().saturating_sub(page).max(start);
        let to = (state.start_row() + 2 * page).min(start + df.height());
        (to > from).then(|| (df.slice((from - start) as i64, to - from), from))
    }

    /// Which rows the live matches were worked out over, to tell when they need
    /// working out again.
    fn rows_on_hand_key(&self) -> Option<(usize, usize, u64)> {
        let state = self.data_table_state.as_ref()?;
        let (df, start) = self.live_window()?;
        Some((start, df.height(), state.len_generation()))
    }

    /// While the find prompt is open, work the matches out again when the rows on
    /// hand changed under it: a collect after the footer took a row, a follow's new
    /// rows.
    pub(crate) fn refresh_stale_live_matches(&mut self) {
        if self.prompt.input_type == Some(InputType::Find)
            && self.prompt.find.live_rows != self.rows_on_hand_key()
        {
            self.refresh_live_matches();
        }
    }

    /// The matches the prompt's pattern has among the rows on screen, while the
    /// prompt is open.
    pub fn live_on_screen(&self) -> Option<usize> {
        let live = self.prompt.find.live.as_ref()?;
        let state = self.data_table_state.as_ref()?;
        let start = state.start_row();
        Some(live.within(start..start + state.visible_rows))
    }

    /// The cells to light up: the prompt's matches while it is open.
    pub fn live_cells(&self) -> Option<Arc<MatchCells>> {
        (self.prompt.input_type == Some(InputType::Find))
            .then_some(self.prompt.find.live.as_ref())
            .flatten()
            .map(|live| live.cells.clone())
    }

    /// The column a find limited to one column searches: the column cursor's, which a
    /// find moves to the cell it lands on.
    pub(crate) fn find_column(&self) -> Option<String> {
        self.data_table_state
            .as_ref()?
            .current_column()
            .map(str::to_string)
    }

    fn close_find_prompt(&mut self) {
        self.prompt.find.input.set_focused(false);
        self.prompt.find.error = None;
        self.prompt.find.live = None;
        self.input_mode = InputMode::Normal;
        self.prompt.input_type = None;
    }

    /// A key in the find prompt. Ctrl+R switches regex, Ctrl+T letters in order,
    /// Ctrl+L the column limit, and Ctrl+G keeps the rows that match; the field keeps
    /// its readline keys and its history.
    pub(crate) fn find_prompt_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        if event.is_press() && ctrl {
            match event.code {
                KeyCode::Char('r') => {
                    self.prompt.find.regex = !self.prompt.find.regex;
                    self.prompt.find.fuzzy &= !self.prompt.find.regex;
                    self.prompt.find.error = None;
                    self.refresh_live_matches();
                    return None;
                }
                KeyCode::Char('t') => {
                    self.prompt.find.fuzzy = !self.prompt.find.fuzzy;
                    self.prompt.find.regex &= !self.prompt.find.fuzzy;
                    self.prompt.find.error = None;
                    self.refresh_live_matches();
                    return None;
                }
                KeyCode::Char('l') => {
                    self.prompt.find.in_column = !self.prompt.find.in_column;
                    self.refresh_live_matches();
                    return None;
                }
                KeyCode::Char('g') => return self.keep_matches(),
                _ => {}
            }
        }
        let before = self.prompt.find.input.value().to_string();
        match self.prompt.find.input.handle_key(event, Some(&self.cache)) {
            TextInputEvent::Submit => {
                let spec = self.prompt.find.prompt_spec();
                if spec.pattern.is_empty() {
                    // An emptied field is how a find is taken back (#644).
                    self.prompt.find.active = None;
                    self.close_find_prompt();
                    return None;
                }
                if let Err(reason) = spec.check() {
                    self.prompt.find.error = Some(reason);
                    return None;
                }
                let _ = self.prompt.find.input.save_to_history(&self.cache);
                self.close_find_prompt();
                self.start_find(spec, Direction::Next, true);
            }
            TextInputEvent::Cancel => self.close_find_prompt(),
            TextInputEvent::HistoryChanged | TextInputEvent::None => {
                if self.prompt.find.input.value() != before {
                    self.prompt.find.error = None;
                    self.refresh_live_matches();
                }
            }
        }
        None
    }

    /// Ctrl+G in the find prompt: keep only the rows that match, as a filter added to
    /// the sidebar's, and leave the find in effect for `n` and `N` among them.
    fn keep_matches(&mut self) -> Option<AppEvent> {
        let spec = self.prompt.find.prompt_spec();
        if spec.pattern.trim().is_empty() {
            return None;
        }
        if let Err(reason) = spec.check() {
            self.prompt.find.error = Some(reason);
            return None;
        }
        let state = self.data_table_state.as_ref()?;
        let operator = if spec.fuzzy {
            FilterOperator::HasFuzzy
        } else if spec.regex {
            FilterOperator::HasRegex
        } else {
            FilterOperator::Has
        };
        // Over every column, the ones shown now: what the find searched.
        let columns = if spec.column.is_none() {
            state.get_column_order().to_vec()
        } else {
            Vec::new()
        };
        let statement = FilterStatement {
            columns,
            column: spec
                .column
                .clone()
                .unwrap_or_else(|| crate::filter_modal::ANY_COLUMN.to_string()),
            operator,
            value: spec.pattern.clone(),
            logical_op: LogicalOperator::And,
        };
        let mut statements = state.view_filters().to_vec();
        let frame = state.len_generation();
        let _ = self.prompt.find.input.save_to_history(&self.cache);
        self.close_find_prompt();
        self.prompt.find.active = Some(ActiveFind {
            spec,
            dataset: self.dataset_generation,
            frame,
            hit: None,
            ordinal: None,
        });
        if statements.contains(&statement) {
            return None;
        }
        statements.push(statement);
        Some(AppEvent::Filter(statements))
    }

    /// `n` / `N` at the table: the find in effect again, from the cursor's cell.
    pub(crate) fn find_again(&mut self, direction: Direction) {
        match self.prompt.find.active.as_ref() {
            Some(active) if active.dataset == self.dataset_generation => {
                let spec = active.spec.clone();
                self.start_find(spec, direction, false);
            }
            _ => self.flash_note("Nothing to find yet: / finds".to_string()),
        }
    }

    /// The cell the find in effect landed on, while the view is the one it searched.
    pub fn find_hit(&self) -> Option<(usize, String)> {
        let active = self.prompt.find.active.as_ref()?;
        let state = self.data_table_state.as_ref()?;
        (active.dataset == self.dataset_generation && active.frame == state.len_generation())
            .then(|| active.hit.clone())
            .flatten()
    }

    /// What the footer says about the find in effect: the pattern, and which
    /// match the cursor is on when that is known.
    pub fn find_mark(&self) -> Option<String> {
        let active = self.prompt.find.active.as_ref()?;
        // While it reads, the busy line names the pattern and the bar needs the room
        // for the rows read; while the prompt is open, the prompt is the find.
        if active.dataset != self.dataset_generation
            || self.finding()
            || self.prompt.input_type == Some(InputType::Find)
        {
            return None;
        }
        let mut mark = format!("find {}", active.spec.label());
        if let Some(column) = &active.spec.column {
            mark.push_str(&format!(" in {column}"));
        }
        if let Some(k) = active.ordinal.filter(|_| self.find_hit().is_some()) {
            mark.push_str(&format!(
                " {} match {}",
                crate::glyphs::get().middot,
                crate::numfmt::group_chrome(k)
            ));
        }
        Some(mark)
    }

    /// Whether a find is in effect on this dataset, so Esc at the table clears it.
    pub(crate) fn find_shown(&self) -> bool {
        self.prompt
            .find
            .active
            .as_ref()
            .is_some_and(|active| active.dataset == self.dataset_generation)
    }

    /// Whether a find is reading.
    pub fn finding(&self) -> bool {
        self.jobs
            .current(|job| matches!(job, Job::Find(_) | Job::HexFind(_)))
            .is_some()
    }

    /// Stop the find that is reading: Esc while it runs. Its worker stops at its next
    /// window, and its answer, if it comes first, is dropped.
    pub(crate) fn cancel_find(&mut self) {
        if self.stop_find() {
            self.flash_note(CANCELLED.to_string());
        }
    }

    /// Stop the find that is reading, if one is. Returns whether one was.
    pub(crate) fn stop_find(&mut self) -> bool {
        if self.stop_hex_find() {
            return true;
        }
        let Some((_, Job::Find(run))) = self.jobs.current(|job| matches!(job, Job::Find(_))) else {
            return false;
        };
        run.stop.store(true, Ordering::Relaxed);
        self.jobs.cancel(|job| matches!(job, Job::Find(_)));
        self.status_message = None;
        self.prompt.find.read = None;
        true
    }

    /// Start a find for `spec` from the cursor. `n` and `N` start past the cursor's
    /// cell; `fresh` (`f`), the whole cursor row is in reach.
    fn start_find(&mut self, spec: FindSpec, direction: Direction, fresh: bool) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let columns = searched_columns(state.get_column_order(), state.schema(), &spec);
        if columns.is_empty() {
            self.flash_note(match &spec.column {
                Some(column) if !state.get_column_order().contains(column) => {
                    format!("Nothing to find in {column}: it is not shown")
                }
                Some(column) => format!("Nothing to find in {column}: it holds no text"),
                None => "No column to find in".to_string(),
            });
            return;
        }
        let frame = state.len_generation();
        let row = state.cursor_row();
        // `n` and `N` go on from the cursor's cell, as in vim; `f` reads its whole row.
        let at = state.current_column().filter(|_| !fresh).map(|name| {
            match columns.iter().position(|(n, _)| n == name) {
                Some(c) => At::On(c),
                None => {
                    let order = state.get_column_order();
                    let place = |n: &str| order.iter().position(|o| o == n);
                    let cursor = place(name);
                    At::Before(columns.iter().filter(|(n, _)| place(n) < cursor).count())
                }
            }
        });
        let previous =
            self.prompt.find.active.as_ref().filter(|a| {
                a.dataset == self.dataset_generation && a.frame == frame && a.spec == spec
            });
        // On the cell the last find landed on, the count of matches goes on from it.
        let on_hit = previous
            .and_then(|a| Some((a.hit.as_ref()?, a.ordinal)))
            .and_then(|((hit_row, name), ordinal)| {
                let c = columns.iter().position(|(n, _)| n == name)?;
                (*hit_row == row && at == Some(At::On(c))).then(|| (name.clone(), ordinal))
            });
        let start = Start { row, column: at };
        self.prompt.find.active = Some(ActiveFind {
            spec: spec.clone(),
            dataset: self.dataset_generation,
            frame,
            hit: on_hit.as_ref().map(|(name, _)| (row, name.clone())),
            ordinal: on_hit.as_ref().and_then(|(_, ordinal)| *ordinal),
        });
        let stop = Arc::new(AtomicBool::new(false));
        let run = FindRun {
            stop: stop.clone(),
            dataset: self.dataset_generation,
            frame,
            direction,
            from_top: row == 0 && at.is_none_or(|at| at.ahead() == 0),
            from_hit: on_hit.as_ref().map(|(_, ordinal)| *ordinal),
        };
        let rows = state.view_rows();
        let status = finding_status(&spec, None);
        self.prompt.find.read = None;
        self.spawn_job(Job::Find(run), Some(&status), move |worker| {
            let report = worker.reporter();
            let search = Search::new(rows, columns, stop, move |read| {
                report(Progress::Finding { rows: read })
            });
            Ok(Answer::Found(search.run(start, direction)?))
        });
    }

    /// A find's progress: the rows it has read.
    pub(crate) fn find_progress(&mut self, rows: usize) {
        self.prompt.find.read = Some(rows);
        if let Some(active) = self.prompt.find.active.as_ref() {
            self.status_message = Some(finding_status(&active.spec, Some(rows)));
        }
    }

    /// A find answered. The cursor goes to the cell found, the view as it was.
    pub(crate) fn find_answered(&mut self, run: FindRun, current: bool, found: Option<Found>) {
        if !current {
            return;
        }
        // The line was the find's progress, which the job's own line no longer is.
        self.status_message = None;
        self.prompt.find.read = None;
        let Some(active) = self.prompt.find.active.as_mut() else {
            return;
        };
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        if active.dataset != run.dataset
            || active.frame != run.frame
            || state.len_generation() != run.frame
        {
            return;
        }
        let Some(found) = found else {
            active.hit = None;
            active.ordinal = None;
            let message = format!("No match for {}", active.spec.label());
            self.flash_note(message);
            return;
        };
        let same_cell = run.from_hit.is_some()
            && active
                .hit
                .as_ref()
                .is_some_and(|(row, name)| *row == found.row && *name == found.column);
        let before = run.from_hit.flatten();
        active.ordinal = match run.direction {
            _ if same_cell => before,
            // Round from the top, it is the first match in the view.
            Direction::Next if found.wrapped => Some(1),
            Direction::Next if run.from_hit.is_some() => before.map(|k| k + 1),
            Direction::Next => run.from_top.then_some(1),
            Direction::Previous if found.wrapped => None,
            Direction::Previous => before.and_then(|k| k.checked_sub(1)).filter(|k| *k > 0),
        };
        active.hit = Some((found.row, found.column.clone()));
        let needs_rows = state.go_to_found_row(found.row);
        // The cursor takes the found cell's column, scrolling as little as it takes.
        state.set_current_column(&found.column);
        if found.wrapped {
            self.flash_note(match run.direction {
                Direction::Next => "Wrapped to the top".to_string(),
                Direction::Previous => "Wrapped to the bottom".to_string(),
            });
        }
        if needs_rows {
            self.spawn_async_collect(Self::LOADING_BUFFER);
        }
    }

    /// A find failed: the reason on the footer, and nothing moved.
    pub(crate) fn find_failed(&mut self, current: bool, message: &str) {
        if !current {
            return;
        }
        self.status_message = None;
        self.prompt.find.read = None;
        if message != CANCELLED {
            self.flash_note(format!("Find failed: {message}"));
        }
    }
}

#[cfg(test)]
mod tests;

/// The keys at the table, through the app.
#[cfg(test)]
mod app_tests;
