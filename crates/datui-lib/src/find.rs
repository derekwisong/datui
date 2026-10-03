//! Find in the table: `f` asks for a pattern, `n` and `N` move between the cells
//! that hold it.
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

use crate::jobs::{Answer, Job, Progress};
use crate::widgets::datatable::ViewRows;
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
    fn regex_source(&self) -> Option<String> {
        let case = if self.ignores_case() { "(?i)" } else { "" };
        match (self.regex, self.ignores_case()) {
            (false, false) => None,
            (false, true) => Some(format!("{case}{}", regex::escape(&self.pattern))),
            (true, _) => Some(format!("{case}{}", self.pattern)),
        }
    }

    /// Why the pattern cannot be searched, in one line.
    pub fn check(&self) -> Result<(), String> {
        let Some(source) = self.regex_source().filter(|_| self.regex) else {
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

    /// How the pattern reads on the control bar: quoted plain text, or a regex
    /// between slashes, cut short when long.
    pub fn label(&self) -> String {
        const LONGEST: usize = 18;
        let g = crate::glyphs::get();
        let mut text: String = self.pattern.chars().take(LONGEST).collect();
        if self.pattern.chars().count() > LONGEST {
            text.push_str(g.ellipsis);
        }
        if self.regex {
            format!("/{text}/")
        } else {
            format!("\"{text}\"")
        }
    }
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
            .map_err(|_| "The view has too many rows to search".to_string())?;
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
    /// The prompt limits the find to `column`.
    pub in_column: bool,
    /// The column the prompt opened on: the column cursor's.
    pub column: Option<String>,
    /// Why the pattern typed cannot be searched.
    pub error: Option<String>,
    /// The find `n` and `N` repeat.
    pub active: Option<ActiveFind>,
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
            in_column: false,
            column: None,
            error: None,
            active: None,
        }
    }

    /// The spec the prompt describes.
    fn prompt_spec(&self) -> FindSpec {
        FindSpec {
            pattern: self.input.value().to_string(),
            regex: self.regex,
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

/// What the control bar says while a find reads.
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
    /// `f` at the table: the find prompt, holding the last pattern, selected so that
    /// typing replaces it.
    pub(crate) fn open_find(&mut self) {
        if self.data_table_state.is_none() {
            return;
        }
        self.find.column = self.find_column();
        self.find.error = None;
        match self.find.active.as_ref() {
            Some(active) => {
                let pattern = active.spec.pattern.clone();
                self.find.input.set_value(pattern);
                self.find.input.select_all();
            }
            None => self.find.input.clear(),
        }
        self.find.input.set_focused(true);
        self.input_mode = InputMode::Editing;
        self.input_type = Some(InputType::Find);
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
        self.find.input.set_focused(false);
        self.find.error = None;
        self.input_mode = InputMode::Normal;
        self.input_type = None;
    }

    /// A key in the find prompt. Ctrl+R switches regex, Ctrl+L the column limit; the
    /// field keeps its readline keys and its history.
    pub(crate) fn find_prompt_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let ctrl = event.modifiers.contains(KeyModifiers::CONTROL);
        if event.is_press() && ctrl && event.code == KeyCode::Char('r') {
            self.find.regex = !self.find.regex;
            self.find.error = None;
            return None;
        }
        if event.is_press() && ctrl && event.code == KeyCode::Char('l') {
            self.find.in_column = !self.find.in_column;
            return None;
        }
        let before = self.find.input.value().to_string();
        match self.find.input.handle_key(event, Some(&self.cache)) {
            TextInputEvent::Submit => {
                let spec = self.find.prompt_spec();
                if spec.pattern.is_empty() {
                    // An emptied field is how a find is taken back (#644).
                    self.find.active = None;
                    self.close_find_prompt();
                    return None;
                }
                if let Err(reason) = spec.check() {
                    self.find.error = Some(reason);
                    return None;
                }
                let _ = self.find.input.save_to_history(&self.cache);
                self.close_find_prompt();
                self.start_find(spec, Direction::Next, true);
            }
            TextInputEvent::Cancel => self.close_find_prompt(),
            TextInputEvent::HistoryChanged | TextInputEvent::None => {
                if self.find.input.value() != before {
                    self.find.error = None;
                }
            }
        }
        None
    }

    /// `n` / `N` at the table: the find in effect again, from the cursor's cell.
    pub(crate) fn find_again(&mut self, direction: Direction) {
        match self.find.active.as_ref() {
            Some(active) if active.dataset == self.dataset_generation => {
                let spec = active.spec.clone();
                self.start_find(spec, direction, false);
            }
            _ => self.flash_note("Nothing to find yet: f finds".to_string()),
        }
    }

    /// The cell the find in effect landed on, while the view is the one it searched.
    pub fn find_hit(&self) -> Option<(usize, String)> {
        let active = self.find.active.as_ref()?;
        let state = self.data_table_state.as_ref()?;
        (active.dataset == self.dataset_generation && active.frame == state.len_generation())
            .then(|| active.hit.clone())
            .flatten()
    }

    /// What the control bar says about the find in effect: the pattern, and which
    /// match the cursor is on when that is known.
    pub fn find_mark(&self) -> Option<String> {
        let active = self.find.active.as_ref()?;
        // While it reads, the busy line names the pattern and the bar needs the room
        // for the rows read; while the prompt is open, the prompt is the find.
        if active.dataset != self.dataset_generation
            || self.finding()
            || self.input_type == Some(InputType::Find)
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
        self.find
            .active
            .as_ref()
            .is_some_and(|active| active.dataset == self.dataset_generation)
    }

    /// Whether a find is reading.
    pub fn finding(&self) -> bool {
        self.jobs
            .current(|job| matches!(job, Job::Find(_)))
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
        let Some((_, Job::Find(run))) = self.jobs.current(|job| matches!(job, Job::Find(_))) else {
            return false;
        };
        run.stop.store(true, Ordering::Relaxed);
        self.jobs.cancel(|job| matches!(job, Job::Find(_)));
        self.status_message = None;
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
            self.find.active.as_ref().filter(|a| {
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
        self.find.active = Some(ActiveFind {
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
        if let Some(active) = self.find.active.as_ref() {
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
        let Some(active) = self.find.active.as_mut() else {
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

    /// A find failed: the reason on the control bar, and nothing moved.
    pub(crate) fn find_failed(&mut self, current: bool, message: &str) {
        if !current {
            return;
        }
        self.status_message = None;
        if message != CANCELLED {
            self.flash_note(format!("Find failed: {message}"));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(pattern: &str, regex: bool) -> FindSpec {
        FindSpec {
            pattern: pattern.to_string(),
            regex,
            column: None,
        }
    }

    fn frame() -> DataFrame {
        df!(
            "name" => ["Alice", "bob", "Carol", "dave", "alice"],
            "city" => ["Oslo", "Lima", "oslo", "Rome", "Lima"],
            "n" => [1i64, 22, 3, 42, 5],
        )
        .unwrap()
    }

    /// Every match of `spec` in `df` reading forward from `start`, `steps` times.
    fn walk(
        df: &DataFrame,
        spec: &FindSpec,
        buffer: Option<(DataFrame, usize)>,
        start: Start,
        direction: Direction,
        steps: usize,
    ) -> Vec<(usize, String, bool)> {
        let order: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect();
        let names: Vec<String> = searched_columns(&order, df.schema(), spec)
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        let mut at = start;
        let mut out = Vec::new();
        for _ in 0..steps {
            let columns = searched_columns(&order, df.schema(), spec);
            let rows = ViewRows::of(df.clone().lazy(), buffer.clone());
            let search = Search::new(rows, columns, Arc::default(), |_| {});
            let Some(found) = search.run(at, direction).unwrap() else {
                break;
            };
            let column = names.iter().position(|n| *n == found.column).map(At::On);
            at = Start {
                row: found.row,
                column,
            };
            out.push((found.row, found.column, found.wrapped));
        }
        out
    }

    fn cells(found: &[(usize, String, bool)]) -> Vec<(usize, &str)> {
        found.iter().map(|(r, c, _)| (*r, c.as_str())).collect()
    }

    #[test]
    fn plain_text_ignores_case_until_a_capital_is_typed() {
        let df = frame();
        let start = Start {
            row: 0,
            column: None,
        };
        let lower = walk(&df, &spec("oslo", false), None, start, Direction::Next, 2);
        assert_eq!(cells(&lower), [(0, "city"), (2, "city")]);
        let upper = walk(&df, &spec("Oslo", false), None, start, Direction::Next, 2);
        assert_eq!(cells(&upper), [(0, "city"), (0, "city")], "{upper:?}");
        assert!(upper[1].2, "the one match wraps to itself");
    }

    #[test]
    fn plain_text_is_literal() {
        let df = df!("a" => ["a.c", "abc"]).unwrap();
        let start = Start {
            row: 0,
            column: None,
        };
        let found = walk(&df, &spec("a.c", false), None, start, Direction::Next, 2);
        assert_eq!(cells(&found), [(0, "a"), (0, "a")]);
    }

    #[test]
    fn a_regex_matches_and_keeps_smart_case() {
        let df = frame();
        let start = Start {
            row: 0,
            column: None,
        };
        let digits = walk(
            &df,
            &spec(r"^\d{2}$", true),
            None,
            start,
            Direction::Next,
            3,
        );
        assert_eq!(cells(&digits), [(1, "n"), (3, "n"), (1, "n")]);
        // `\S` is a class, not a capital: still case-blind.
        assert!(spec(r"^\Sl", true).ignores_case());
        let a = walk(&df, &spec(r"^a\S", true), None, start, Direction::Next, 2);
        assert_eq!(cells(&a), [(0, "name"), (4, "name")]);
        let capital = walk(&df, &spec("^A", true), None, start, Direction::Next, 2);
        assert_eq!(cells(&capital), [(0, "name"), (0, "name")]);
    }

    #[test]
    fn a_bad_regex_says_why() {
        let err = spec("(ab", true).check().unwrap_err();
        assert!(err.starts_with("Not a regex"), "{err}");
        assert!(!err.contains('\n'), "{err}");
        assert!(spec("(ab", false).check().is_ok(), "plain text is literal");
    }

    #[test]
    fn next_moves_cell_by_cell_and_wraps_to_the_top() {
        let df = frame();
        let start = Start {
            row: 0,
            column: None,
        };
        // Both "Lima" rows and "alice": every cell holding "li", row by row and left
        // to right within a row.
        let found = walk(&df, &spec("li", false), None, start, Direction::Next, 5);
        assert_eq!(
            cells(&found),
            [
                (0, "name"),
                (1, "city"),
                (4, "name"),
                (4, "city"),
                (0, "name")
            ]
        );
        assert!(!found[3].2);
        assert!(found[4].2, "past the last match comes round to the first");
    }

    #[test]
    fn previous_walks_back_and_wraps_to_the_bottom() {
        let df = frame();
        let start = Start {
            row: 1,
            column: Some(At::On(1)),
        };
        let found = walk(&df, &spec("li", false), None, start, Direction::Previous, 3);
        assert_eq!(cells(&found), [(0, "name"), (4, "city"), (4, "name")]);
        assert!(found[1].2, "before the first match comes round to the last");
    }

    /// On a column not searched, the cursor sits between the searched ones: the
    /// cells to its right are ahead of it, those to its left behind.
    #[test]
    fn from_a_column_not_searched_the_cells_either_side_split() {
        let df = frame();
        let only_city = FindSpec {
            column: Some("city".to_string()),
            ..spec("li", false)
        };
        let order: Vec<String> = ["name", "city", "n"].map(String::from).to_vec();
        let one = |at: At, direction: Direction| {
            let columns = searched_columns(&order, df.schema(), &only_city);
            let rows = ViewRows::of(df.clone().lazy(), None);
            let search = Search::new(rows, columns, Arc::default(), |_| {});
            let found = search
                .run(
                    Start {
                        row: 1,
                        column: Some(at),
                    },
                    direction,
                )
                .unwrap()
                .unwrap();
            (found.row, found.wrapped)
        };
        // On `name`, left of `city`: row 1's Lima is ahead, and behind is round.
        assert_eq!(one(At::Before(0), Direction::Next), (1, false));
        assert_eq!(one(At::Before(0), Direction::Previous), (4, true));
        // On `n`, right of it: the other way about.
        assert_eq!(one(At::Before(1), Direction::Next), (4, false));
        assert_eq!(one(At::Before(1), Direction::Previous), (1, false));
    }

    #[test]
    fn previous_wraps_with_the_row_count_known_too() {
        let df = frame();
        let columns = searched_columns(
            &["name".to_string(), "city".to_string()],
            df.schema(),
            &spec("li", false),
        );
        let mut rows = ViewRows::of(df.clone().lazy(), None);
        rows.num_rows = Some(df.height());
        let search = Search::new(rows, columns, Arc::default(), |_| {});
        let found = search
            .run(
                Start {
                    row: 0,
                    column: Some(At::On(0)),
                },
                Direction::Previous,
            )
            .unwrap()
            .unwrap();
        assert_eq!((found.row, found.column.as_str()), (4, "city"));
        assert!(found.wrapped);
    }

    #[test]
    fn f_finds_a_match_on_the_cursor_row_itself() {
        let df = frame();
        let start = Start {
            row: 2,
            column: None,
        };
        let found = walk(&df, &spec("oslo", false), None, start, Direction::Next, 1);
        assert_eq!(cells(&found), [(2, "city")]);
    }

    #[test]
    fn a_column_limit_skips_the_others() {
        let df = frame();
        let mut only = spec("li", false);
        only.column = Some("city".to_string());
        let start = Start {
            row: 0,
            column: None,
        };
        let found = walk(&df, &only, None, start, Direction::Next, 3);
        assert_eq!(cells(&found), [(1, "city"), (4, "city"), (1, "city")]);
    }

    #[test]
    fn nothing_found_is_none() {
        let df = frame();
        let start = Start {
            row: 3,
            column: None,
        };
        assert!(walk(&df, &spec("zzz", false), None, start, Direction::Next, 1).is_empty());
        assert!(
            walk(
                &df,
                &spec("zzz", false),
                None,
                start,
                Direction::Previous,
                1
            )
            .is_empty()
        );
    }

    /// A long view: the buffer holds rows 100..200, the match is far below it, read
    /// in growing windows from the view, and the rows read are reported.
    #[test]
    fn a_match_beyond_the_buffer_is_read_from_the_view() {
        let n = 300_000usize;
        let values: Vec<String> = (0..n)
            .map(|i| {
                if i == 250_123 {
                    "needle".to_string()
                } else {
                    format!("hay{i}")
                }
            })
            .collect();
        let df = df!("v" => values).unwrap();
        let buffer = df.slice(100, 100);
        let columns = searched_columns(&["v".to_string()], df.schema(), &spec("needle", false));
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = reads.clone();
        let rows = ViewRows::of(df.clone().lazy(), Some((buffer, 100)));
        let search = Search::new(rows, columns, Arc::default(), move |read| {
            seen.lock().unwrap().push(read)
        });
        let found = search
            .run(
                Start {
                    row: 150,
                    column: None,
                },
                Direction::Next,
            )
            .unwrap()
            .unwrap();
        assert_eq!((found.row, found.wrapped), (250_123, false));
        let reads = reads.lock().unwrap();
        // The buffer's rest first, then windows of 65,536, 131,072 and the rest of the
        // view, read from row 200 on: none of it twice.
        assert_eq!(
            reads.as_slice(),
            [50, 50 + 65_536, 50 + 65_536 + 131_072, n - 150]
        );
    }

    #[test]
    fn a_stopped_search_ends_before_reading() {
        let df = frame();
        let columns = searched_columns(&["name".to_string()], df.schema(), &spec("a", false));
        let stop = Arc::new(AtomicBool::new(true));
        let search = Search::new(ViewRows::of(df.lazy(), None), columns, stop, |_| {
            panic!("nothing is read once stopped")
        });
        let ended = search.run(
            Start {
                row: 0,
                column: None,
            },
            Direction::Next,
        );
        assert_eq!(ended.unwrap_err(), CANCELLED);
    }

    #[test]
    fn numbers_dates_and_categories_are_searched_as_text() {
        let df = df!(
            "d" => [chrono::NaiveDate::from_ymd_opt(2024, 5, 17).unwrap()],
            "x" => [12.5f64],
            "b" => [true],
        )
        .unwrap()
        .lazy()
        .with_column(
            lit("north")
                .cast(DataType::from_categories(Categories::global()))
                .alias("c"),
        )
        .collect()
        .unwrap();
        let start = Start {
            row: 0,
            column: None,
        };
        for (pattern, column) in [("05-17", "d"), ("2.5", "x"), ("true", "b"), ("nor", "c")] {
            let found = walk(&df, &spec(pattern, false), None, start, Direction::Next, 1);
            assert_eq!(cells(&found), [(0, column)], "{pattern}");
        }
    }

    /// A date past the calendar, on which a plain cast panics, is its stored number;
    /// durations and times are text too, so no type fails the whole find.
    #[test]
    fn a_date_past_the_calendar_and_durations_are_text() {
        let d = Series::new("d".into(), [19_860i32, i32::MAX])
            .cast(&DataType::Date)
            .unwrap();
        let t = Series::new("t".into(), [3_600_000_000_000i64, 0])
            .cast(&DataType::Time)
            .unwrap();
        let dur = Series::new("dur".into(), [86_400_000i64, 1])
            .cast(&DataType::Duration(TimeUnit::Milliseconds))
            .unwrap();
        let df = DataFrame::new_infer_height(vec![d.into(), t.into(), dur.into()]).unwrap();
        let start = Start {
            row: 0,
            column: None,
        };
        let past = walk(&df, &spec("since", false), None, start, Direction::Next, 1);
        assert_eq!(cells(&past), [(1, "d")]);
        let time = walk(&df, &spec("01:00", false), None, start, Direction::Next, 1);
        assert_eq!(cells(&time), [(0, "t")]);
        let dur = walk(&df, &spec("1d", false), None, start, Direction::Next, 1);
        assert_eq!(cells(&dur), [(0, "dur")]);
    }

    /// A sorted view costs a whole pass for any window, so it is read in one: the
    /// match far down is found with one read past the buffer, not a window per
    /// doubling.
    #[test]
    fn a_sorted_view_is_read_in_one_window() {
        let n = 300_000usize;
        let values: Vec<String> = (0..n)
            .map(|i| {
                if i == 7 {
                    "needle".to_string()
                } else {
                    format!("hay{i}")
                }
            })
            .collect();
        let df = df!("k" => (0..n as i64).collect::<Vec<_>>(), "v" => values).unwrap();
        // Descending: the needle (k = 7) is view row n - 8.
        let lf = df.lazy().sort(
            ["k"],
            SortMultipleOptions::default().with_order_descending(true),
        );
        let buffer = lf.clone().slice(0, 100).collect().unwrap();
        let columns = searched_columns(
            &["k".to_string(), "v".to_string()],
            &lf.clone().collect_schema().unwrap(),
            &spec("needle", false),
        );
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = reads.clone();
        let rows = ViewRows::of(lf, Some((buffer, 0)));
        assert!(rows.whole);
        let search = Search::new(rows, columns, Arc::default(), move |read| {
            seen.lock().unwrap().push(read)
        });
        let found = search
            .run(
                Start {
                    row: 0,
                    column: None,
                },
                Direction::Next,
            )
            .unwrap()
            .unwrap();
        assert_eq!((found.row, found.column.as_str()), (n - 8, "v"));
        assert_eq!(reads.lock().unwrap().as_slice(), [100, n]);
    }

    /// `N` on a view that cannot skip to a window reads the range behind the cursor
    /// in one pass, and so does its way round from the bottom.
    #[test]
    fn previous_on_a_filtered_view_reads_the_range_once() {
        let n = 300_000usize;
        let values: Vec<String> = (0..n)
            .map(|i| {
                if i == 4 {
                    "needle".to_string()
                } else {
                    format!("hay{i}")
                }
            })
            .collect();
        let df = df!("k" => (0..n as i64).collect::<Vec<_>>(), "v" => values).unwrap();
        // Even k only: the needle (k = 4) is view row 2, of 150,000.
        let lf = df.lazy().filter((col("k") % lit(2)).eq(lit(0)));
        let columns = || {
            searched_columns(
                &["k".to_string(), "v".to_string()],
                &lf.clone().collect_schema().unwrap(),
                &spec("needle", false),
            )
        };
        let previous = |row: usize, buffer: Option<(DataFrame, usize)>| {
            let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
            let seen = reads.clone();
            let rows = ViewRows::of(lf.clone(), buffer);
            assert!(rows.reads_up_to && !rows.whole);
            let search = Search::new(rows, columns(), Arc::default(), move |read| {
                seen.lock().unwrap().push(read)
            });
            let found = search
                .run(Start { row, column: None }, Direction::Previous)
                .unwrap()
                .unwrap();
            let reads = reads.lock().unwrap().clone();
            (found.row, found.wrapped, reads)
        };
        let buffer = lf.clone().slice(140_000, 100).collect().unwrap();
        assert_eq!(
            previous(140_050, Some((buffer, 140_000))),
            (2, false, vec![51, 140_051]),
            "the buffer, then every row before it at once"
        );
        assert_eq!(
            previous(1, None),
            (2, true, vec![2, 150_000]),
            "nothing behind; round from the bottom in one read too"
        );
    }

    /// `n` from deep in a view that cannot skip reads a first window as large as the
    /// rows above it, rather than doubling up from a small one and reading those
    /// rows again for each window.
    #[test]
    fn next_from_deep_in_a_filtered_view_reads_the_rows_above_once() {
        let n = 600_000usize;
        let values: Vec<String> = (0..n)
            .map(|i| {
                if i == 598_000 {
                    "needle".to_string()
                } else {
                    format!("hay{i}")
                }
            })
            .collect();
        let df = df!("k" => (0..n as i64).collect::<Vec<_>>(), "v" => values).unwrap();
        // Even k only: the needle (k = 598,000) is view row 299,000, of 300,000.
        let lf = df.lazy().filter((col("k") % lit(2)).eq(lit(0)));
        let columns = searched_columns(
            &["k".to_string(), "v".to_string()],
            &lf.clone().collect_schema().unwrap(),
            &spec("needle", false),
        );
        let buffer = lf.clone().slice(200_000, 100).collect().unwrap();
        let reads = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen = reads.clone();
        let rows = ViewRows::of(lf, Some((buffer, 200_000)));
        assert!(rows.reads_up_to && !rows.whole);
        let search = Search::new(rows, columns, Arc::default(), move |read| {
            seen.lock().unwrap().push(read)
        });
        let found = search
            .run(
                Start {
                    row: 200_050,
                    column: None,
                },
                Direction::Next,
            )
            .unwrap()
            .unwrap();
        assert_eq!((found.row, found.wrapped), (299_000, false));
        // The buffer's rest, then the rest of the view in one window: not 65,536
        // rows and then 131,072, each reading the 200,000 above it again.
        assert_eq!(reads.lock().unwrap().as_slice(), [50, 99_950]);
    }

    #[test]
    fn a_view_skips_to_a_window_only_unfiltered_over_parquet_or_ipc() {
        use crate::widgets::datatable::reads_up_to_a_window;
        let dir = tempfile::tempdir().unwrap();
        let mut df = df!("k" => [1i64, 2, 3]).unwrap();
        let csv = dir.path().join("t.csv");
        CsvWriter::new(std::fs::File::create(&csv).unwrap())
            .finish(&mut df)
            .unwrap();
        let parquet = dir.path().join("t.parquet");
        ParquetWriter::new(std::fs::File::create(&parquet).unwrap())
            .finish(&mut df)
            .unwrap();
        let csv = LazyCsvReader::new(PlRefPath::try_from_path(&csv).unwrap())
            .finish()
            .unwrap();
        let parquet = LazyFrame::scan_parquet(
            PlRefPath::try_from_path(&parquet).unwrap(),
            Default::default(),
        )
        .unwrap();
        assert!(reads_up_to_a_window(&csv));
        assert!(!reads_up_to_a_window(&parquet));
        assert!(reads_up_to_a_window(&parquet.filter(col("k").gt(lit(1)))));
    }

    #[test]
    fn bytes_and_nested_columns_are_skipped() {
        let schema = Schema::from_iter([
            Field::new("b".into(), DataType::Binary),
            Field::new("l".into(), DataType::List(Box::new(DataType::String))),
            Field::new("s".into(), DataType::String),
        ]);
        let order = ["b".to_string(), "l".to_string(), "s".to_string()];
        let searched: Vec<String> = searched_columns(&order, &schema, &spec("x", false))
            .into_iter()
            .map(|(n, _)| n)
            .collect();
        assert_eq!(searched, ["s"]);
    }

    #[test]
    fn the_label_quotes_text_and_slashes_a_regex() {
        assert_eq!(spec("abc", false).label(), "\"abc\"");
        assert_eq!(spec("a+", true).label(), "/a+/");
        let long = spec(&"x".repeat(40), false).label();
        assert!(long.chars().count() < 25, "{long}");
    }
}

/// The keys at the table, through the app.
#[cfg(test)]
mod app_tests {
    use super::*;
    use crate::widgets::datatable::DataTableState;
    use std::sync::mpsc::{self, Receiver};
    use std::time::{Duration, Instant};

    fn key(app: &mut App, code: KeyCode) {
        key_with(app, code, KeyModifiers::NONE);
    }

    fn key_with(app: &mut App, code: KeyCode, modifiers: KeyModifiers) {
        let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, modifiers)));
        while let Some(event) = next {
            next = app.event(&event);
        }
    }

    fn type_text(app: &mut App, text: &str) {
        for c in text.chars() {
            key(app, KeyCode::Char(c));
        }
    }

    /// Handle what the workers send until nothing is owed.
    fn settle(app: &mut App, rx: &Receiver<AppEvent>) {
        let deadline = Instant::now() + Duration::from_secs(120);
        loop {
            let event = match rx.try_recv() {
                Ok(event) => event,
                Err(_) if app.count_waits_for_a_frame() => AppEvent::FramePainted,
                Err(_) if !crate::tests::work_pending(app) => return,
                Err(_) => {
                    assert!(Instant::now() < deadline, "the find never answered");
                    match rx.recv_timeout(Duration::from_millis(50)) {
                        Ok(event) => event,
                        Err(_) => continue,
                    }
                }
            };
            let mut next = Some(event);
            while let Some(event) = next {
                next = app.event(&event);
            }
        }
    }

    /// An app over `df` with its first buffer read, ten rows on screen.
    fn app_over(df: DataFrame) -> (App, Receiver<AppEvent>) {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, crate::tests::test_runtime());
        let mut state =
            DataTableState::from_lazyframe(df.lazy(), &crate::OpenOptions::default()).unwrap();
        state.visible_rows = 10;
        app.data_table_state = Some(state);
        app.spawn_async_collect("Loading");
        settle(&mut app, &rx);
        (app, rx)
    }

    fn haystack(rows: usize, needles: &[usize]) -> DataFrame {
        let values: Vec<String> = (0..rows)
            .map(|i| {
                if needles.contains(&i) {
                    format!("needle {i}")
                } else {
                    format!("hay {i}")
                }
            })
            .collect();
        df!("id" => (0..rows as i64).collect::<Vec<_>>(), "v" => values).unwrap()
    }

    fn cursor(app: &App) -> usize {
        app.data_table_state.as_ref().unwrap().cursor_row()
    }

    fn find(app: &mut App, rx: &Receiver<AppEvent>, pattern: &str) {
        key(app, KeyCode::Char('f'));
        assert_eq!(app.input_type, Some(InputType::Find));
        type_text(app, pattern);
        key(app, KeyCode::Enter);
        settle(app, rx);
    }

    /// Enter on an emptied field takes the find back (#644): no mark, no hit, and `n`
    /// has nothing to repeat.
    #[test]
    fn an_emptied_find_clears_the_find() {
        let (mut app, rx) = app_over(haystack(1_000, &[5, 700]));
        find(&mut app, &rx, "needle");
        assert!(app.find_mark().is_some());

        key(&mut app, KeyCode::Char('f'));
        // The old pattern is selected, so Backspace empties the field.
        key(&mut app, KeyCode::Backspace);
        assert_eq!(app.find.input.value(), "");
        key(&mut app, KeyCode::Enter);
        assert_eq!(app.input_mode, InputMode::Normal);
        assert_eq!(app.find_mark(), None);
        assert_eq!(app.find_hit(), None);

        let at = cursor(&app);
        key(&mut app, KeyCode::Char('n'));
        settle(&mut app, &rx);
        assert_eq!(cursor(&app), at, "n does not move");
        assert_eq!(app.flash_message(), Some("Nothing to find yet: f finds"));
    }

    /// Esc at the table clears a find before it backs out of anything else (#644).
    #[test]
    fn esc_at_the_table_clears_the_find() {
        let (mut app, rx) = app_over(haystack(1_000, &[5, 700]));
        find(&mut app, &rx, "needle");
        assert!(app.find_mark().is_some());
        key(&mut app, KeyCode::Esc);
        assert_eq!(app.find_mark(), None);
        assert_eq!(app.find_hit(), None);
        assert_eq!(app.input_mode, InputMode::Normal);
    }

    #[test]
    fn f_then_n_and_capital_n_walk_the_matches_past_the_buffer() {
        let (mut app, rx) = app_over(haystack(300_000, &[5, 250_123]));
        let buffered_end = app.data_table_state.as_ref().unwrap().buffered_end();
        assert!(buffered_end < 250_123, "the far match is past the buffer");

        find(&mut app, &rx, "needle");
        assert_eq!(app.input_mode, InputMode::Normal);
        assert_eq!(cursor(&app), 5);
        assert_eq!(app.find_hit(), Some((5, "v".to_string())));
        assert_eq!(
            app.find_mark().as_deref(),
            Some("find \"needle\" · match 1")
        );

        key(&mut app, KeyCode::Char('n'));
        settle(&mut app, &rx);
        assert_eq!(cursor(&app), 250_123);
        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            (state.buffered_start()..state.buffered_end()).contains(&250_123),
            "the rows around the match were read"
        );
        assert_eq!(
            app.find_mark().as_deref(),
            Some("find \"needle\" · match 2")
        );

        key(&mut app, KeyCode::Char('n'));
        settle(&mut app, &rx);
        assert_eq!(cursor(&app), 5);
        assert_eq!(app.flash_message(), Some("Wrapped to the top"));
        assert_eq!(
            app.find_mark().as_deref(),
            Some("find \"needle\" · match 1")
        );

        key(&mut app, KeyCode::Char('N'));
        settle(&mut app, &rx);
        assert_eq!(cursor(&app), 250_123);
        assert_eq!(app.flash_message(), Some("Wrapped to the bottom"));
        assert_eq!(
            app.find_mark().as_deref(),
            Some("find \"needle\""),
            "round from the bottom, which match it is is not known"
        );
    }

    #[test]
    fn the_view_is_searched_and_left_as_it_was() {
        let (mut app, rx) = app_over(haystack(1_000, &[3, 700]));
        let state = app.data_table_state.as_mut().unwrap();
        state.sort(vec!["id".to_string()], false);
        settle(&mut app, &rx);
        let frame = app.data_table_state.as_ref().unwrap().len_generation();
        find(&mut app, &rx, "needle");
        let state = app.data_table_state.as_ref().unwrap();
        // Descending: row 299 of the view is id 700.
        assert_eq!(cursor(&app), 299);
        assert_eq!(state.len_generation(), frame, "the view is the same frame");
        assert_eq!(state.get_sort_columns(), ["id".to_string()]);
    }

    #[test]
    fn esc_stops_a_find_and_the_cursor_stays() {
        let (mut app, rx) = app_over(haystack(300_000, &[250_123]));
        key(&mut app, KeyCode::Char('f'));
        type_text(&mut app, "needle");
        key(&mut app, KeyCode::Enter);
        assert!(app.finding() && app.is_busy());
        assert!(app.hard_escape_while_busy(&KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE)));
        key(&mut app, KeyCode::Esc);
        assert!(!app.finding());
        assert!(!app.is_busy(), "the keys are the user's again");
        assert_eq!(app.flash_message(), Some(CANCELLED));
        settle(&mut app, &rx);
        assert_eq!(cursor(&app), 0, "a cancelled find moves nothing");
        assert_eq!(app.find_hit(), None);
    }

    #[test]
    fn ctrl_o_stops_a_find_on_the_way_home() {
        let (mut app, rx) = app_over(haystack(300_000, &[250_123]));
        find_started(&mut app);
        key_with(&mut app, KeyCode::Char('o'), KeyModifiers::CONTROL);
        assert!(!app.finding());
        assert_eq!(app.input_mode, InputMode::Home);
        settle(&mut app, &rx);
        assert_eq!(cursor(&app), 0);
    }

    fn find_started(app: &mut App) {
        key(app, KeyCode::Char('f'));
        type_text(app, "needle");
        key(app, KeyCode::Enter);
        assert!(app.finding());
    }

    #[test]
    fn the_prompt_toggles_regex_and_column_and_says_why_a_regex_is_bad() {
        let (mut app, rx) = app_over(haystack(20, &[]));
        key(&mut app, KeyCode::Char('f'));
        key_with(&mut app, KeyCode::Char('r'), KeyModifiers::CONTROL);
        key_with(&mut app, KeyCode::Char('l'), KeyModifiers::CONTROL);
        assert!(app.find.regex && app.find.in_column);
        assert_eq!(app.find.column.as_deref(), Some("id"));
        type_text(&mut app, "(1");
        key(&mut app, KeyCode::Enter);
        assert_eq!(app.input_type, Some(InputType::Find), "it stays open");
        assert!(
            app.find
                .error
                .as_deref()
                .unwrap()
                .starts_with("Not a regex")
        );
        key(&mut app, KeyCode::Backspace);
        key(&mut app, KeyCode::Backspace);
        assert!(app.find.error.is_none(), "an edit clears the reason");
        // Only in `id`: "hay 12" in `v` is not a match for `^12$`.
        type_text(&mut app, "^12$");
        key(&mut app, KeyCode::Enter);
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), Some((12, "id".to_string())));
        assert_eq!(
            app.find_mark().as_deref(),
            Some("find /^12$/ in id · match 1")
        );
    }

    /// Ctrl+L limits a find to the column cursor's column, and a match moves the
    /// column cursor to the found cell's column.
    #[test]
    fn the_column_cursor_is_the_find_column_and_a_match_moves_it() {
        let df = df!(
            "id" => (0..20i64).collect::<Vec<_>>(),
            "v" => (0..20).map(|i| format!("hay {i}")).collect::<Vec<_>>(),
            "w" => (0..20).map(|i| if i == 7 { "needle".to_string() } else { format!("w {i}") }).collect::<Vec<_>>(),
        )
        .unwrap();
        let (mut app, rx) = app_over(df);
        let current = |app: &App| {
            app.data_table_state
                .as_ref()
                .unwrap()
                .current_column()
                .map(str::to_string)
        };
        key(&mut app, KeyCode::Char('l'));
        assert_eq!(current(&app).as_deref(), Some("v"));
        key(&mut app, KeyCode::Char('f'));
        key_with(&mut app, KeyCode::Char('l'), KeyModifiers::CONTROL);
        assert_eq!(app.find.column.as_deref(), Some("v"));
        type_text(&mut app, "needle");
        key(&mut app, KeyCode::Enter);
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), None, "not in v");

        // Every column: the match is in `w`, and the column cursor goes there.
        key(&mut app, KeyCode::Char('f'));
        key_with(&mut app, KeyCode::Char('l'), KeyModifiers::CONTROL);
        key(&mut app, KeyCode::Enter);
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), Some((7, "w".to_string())));
        assert_eq!(cursor(&app), 7);
        assert_eq!(current(&app).as_deref(), Some("w"));
        // The next limited find opens on it.
        key(&mut app, KeyCode::Char('f'));
        assert_eq!(app.find.column.as_deref(), Some("w"));
    }

    /// `n` and `N` go on from the cursor's cell: moved along the found row, the next
    /// match is the one to the cursor's right, the previous the one to its left.
    #[test]
    fn n_and_capital_n_start_from_the_cursors_cell() {
        let cell = |hit: bool, i: usize| {
            if hit {
                "needle".to_string()
            } else {
                format!("hay {i}")
            }
        };
        let df = df!(
            "a" => (0..20).map(|i| cell(i == 2, i)).collect::<Vec<_>>(),
            "b" => (0..20).map(|i| cell(i == 2, i)).collect::<Vec<_>>(),
            "c" => (0..20).map(|i| cell(false, i)).collect::<Vec<_>>(),
            "d" => (0..20).map(|i| cell(i == 2 || i == 9, i)).collect::<Vec<_>>(),
        )
        .unwrap();
        let (mut app, rx) = app_over(df);
        find(&mut app, &rx, "needle");
        assert_eq!(app.find_hit(), Some((2, "a".to_string())));
        // To `c`, past the match in `b`.
        key(&mut app, KeyCode::Char('l'));
        key(&mut app, KeyCode::Char('l'));
        key(&mut app, KeyCode::Char('n'));
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), Some((2, "d".to_string())), "right of c");
        // Back to `c`: the previous match is `b`, left of it.
        key(&mut app, KeyCode::Char('h'));
        key(&mut app, KeyCode::Char('N'));
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), Some((2, "b".to_string())), "left of c");
        // Down the rows: from row 5, the next is row 9, not the rest of row 2.
        for _ in 0..3 {
            key(&mut app, KeyCode::Char('j'));
        }
        assert_eq!(cursor(&app), 5);
        key(&mut app, KeyCode::Char('n'));
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), Some((9, "d".to_string())));
    }

    #[test]
    fn no_match_says_so_and_nothing_moves() {
        let (mut app, rx) = app_over(haystack(50, &[]));
        key(&mut app, KeyCode::Char('j'));
        find(&mut app, &rx, "needle");
        assert_eq!(cursor(&app), 1);
        assert_eq!(app.flash_message(), Some("No match for \"needle\""));
    }

    #[test]
    fn n_before_any_find_says_how_to_start_one() {
        let (mut app, _rx) = app_over(haystack(5, &[]));
        key(&mut app, KeyCode::Char('N'));
        assert_eq!(app.flash_message(), Some("Nothing to find yet: f finds"));
        assert!(
            !app.data_table_state.as_ref().unwrap().row_numbers(),
            "N no longer toggles row numbers"
        );
    }

    #[test]
    fn hash_toggles_row_numbers() {
        let (mut app, _rx) = app_over(haystack(5, &[]));
        key(&mut app, KeyCode::Char('#'));
        assert!(app.data_table_state.as_ref().unwrap().row_numbers());
        key(&mut app, KeyCode::Char('#'));
        assert!(!app.data_table_state.as_ref().unwrap().row_numbers());
    }

    /// After the view changes under it, `n` starts from the cursor in the new view:
    /// the cell it landed on belongs to a frame that is gone.
    #[test]
    fn n_after_the_view_changes_starts_from_the_cursor_in_the_new_view() {
        let (mut app, rx) = app_over(haystack(1_000, &[5, 700]));
        find(&mut app, &rx, "needle");
        assert_eq!(app.find_hit(), Some((5, "v".to_string())));
        let state = app.data_table_state.as_mut().unwrap();
        state.sort(vec!["id".to_string()], false);
        settle(&mut app, &rx);
        assert_eq!(app.find_hit(), None, "the old cell is not this view's");
        assert_eq!(app.find_mark().as_deref(), Some("find \"needle\""));
        let from = cursor(&app);
        key(&mut app, KeyCode::Char('n'));
        settle(&mut app, &rx);
        // Descending: id 700 is view row 299, id 5 row 994.
        let expected = if from < 299 { 299 } else { 994 };
        assert_eq!(app.find_hit(), Some((expected, "v".to_string())));
        assert_eq!(cursor(&app), expected);
    }

    /// The found cell is drawn in the theme's find slot, on the cursor's row, and
    /// nowhere once the cursor moves off it; with row numbers shown too.
    #[test]
    fn the_found_cell_is_highlighted_on_the_cursor_row() {
        for row_numbers in [false, true] {
            found_cell_is_highlighted(row_numbers);
        }
    }

    fn found_cell_is_highlighted(row_numbers: bool) {
        use ratatui::{buffer::Buffer, layout::Rect, widgets::Widget};
        let (mut app, rx) = app_over(haystack(30, &[4]));
        if row_numbers {
            key(&mut app, KeyCode::Char('#'));
        }
        find(&mut app, &rx, "needle 4");
        let style = app.theme.find_match_style();
        let draw = |app: &mut App| {
            let area = Rect::new(0, 0, 80, 24);
            let mut buf = Buffer::empty(area);
            app.render(area, &mut buf);
            buf
        };
        let buf = draw(&mut app);
        let marked: Vec<(u16, u16)> = buf
            .content()
            .iter()
            .enumerate()
            .filter(|(_, cell)| cell.bg == style.bg.unwrap())
            .map(|(i, _)| ((i % 80) as u16, (i / 80) as u16))
            .collect();
        assert!(!marked.is_empty(), "the cell is marked");
        let y = marked[0].1;
        let row: String = (0..80).map(|x| buf[(x, y)].symbol().to_string()).collect();
        assert!(row.contains("needle 4"), "{row}");
        let text: String = marked
            .iter()
            .map(|&(x, y)| buf[(x, y)].symbol().to_string())
            .collect();
        assert!(
            text.contains("needle 4"),
            "the mark is on the value: {text:?}"
        );

        // The found cell is the current cell, drawn as found rather than as the cell
        // cursor; the header keeps the cell cursor's mark.
        // The cell cursor's look on this terminal: its tint, or reversed where the
        // tint cannot show.
        let cell_cursor = app.theme.cell_cursor_style();
        let is_cell_cursor = |cell: &ratatui::buffer::Cell| match cell_cursor.bg {
            Some(bg) => cell.bg == bg,
            None => cell.modifier.contains(ratatui::style::Modifier::REVERSED),
        };
        assert!(
            marked.iter().all(|&(x, y)| !is_cell_cursor(&buf[(x, y)])),
            "one style on the found cell"
        );
        assert!(
            (0..80).any(|x| is_cell_cursor(&buf[(x, 0)])),
            "the header carries the cursor"
        );

        // The column cursor off it: the cell is plain, and the new current cell is the
        // cell cursor's.
        key(&mut app, KeyCode::Char('h'));
        let buf = draw(&mut app);
        assert!(
            !buf.content()
                .iter()
                .any(|cell| cell.bg == style.bg.unwrap()),
            "off its column, the cell is plain"
        );
        assert!((0..80).any(|x| is_cell_cursor(&buf[(x, y)])));
        key(&mut app, KeyCode::Char('l'));
        assert!(
            draw(&mut app)
                .content()
                .iter()
                .any(|cell| cell.bg == style.bg.unwrap()),
            "back on it, marked again"
        );

        key(&mut app, KeyCode::Char('j'));
        let buf = draw(&mut app);
        assert!(
            !buf.content()
                .iter()
                .any(|cell| cell.bg == style.bg.unwrap()),
            "off its row, the cell is plain"
        );
    }
}
