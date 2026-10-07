//! The row buffer: planning which rows to read around the view, reading them (on a
//! worker, or here for tests), installing them, and the scroll that walks through them.

use super::*;

/// Parameters for a background buffer load. Produced by `prepare_async_collect()`.
pub struct CollectRequest {
    /// LazyFrame to collect (sliced to the buffer range, with column selection applied).
    pub lf: LazyFrame,
    /// Whether to use Polars streaming engine.
    pub polars_streaming: bool,
    /// Buffer start row in the full dataset.
    pub buffer_start: usize,
    /// Buffer end row in the full dataset.
    pub buffer_end: usize,
    /// How the worker fits the rows it reads to the buffer: [`FillPlan::fit`].
    pub plan: FillPlan,
}

/// What the fill-reading worker needs to make it the buffer: the adjoining rows on
/// hand, the view and the caps, as planned. A trim that would copy (see
/// `trim_rows`) runs here, off the UI thread; `apply_async_collect` installs the
/// result as is.
pub struct FillPlan {
    buffer_start: usize,
    buffer_end: usize,
    num_rows: usize,
    count_known: bool,
    /// Lines were still being indexed when the read was planned: a short read ends
    /// where the indexing had got to, not the file.
    indexing: bool,
    /// The rows on hand and their first row, when the fill is planned to be stitched
    /// on to them. Shared, not copied.
    held: Option<(DataFrame, usize)>,
    view_start: usize,
    view_len: usize,
    max_rows: usize,
    max_mb: usize,
}

impl FillPlan {
    /// Make the buffer from `df`, the planned range: stitched to adjoining rows on hand,
    /// then cut to the caps around the view.
    pub fn fit(mut self, df: DataFrame) -> CollectResult {
        let returned = df.height();
        let bytes_per_row = (returned > 0).then(|| (df.estimated_size() / returned).max(1));
        // A shape mismatch (the columns changed underneath) keeps the fetched rows alone.
        let (df, start, seam) = match self.held.take() {
            Some((mut held, held_start)) if held_start + held.height() == self.buffer_start => {
                let seam = held.height();
                match held.vstack_mut(&df) {
                    Ok(_) => (held, held_start, Some(seam)),
                    Err(_) => (df, self.buffer_start, None),
                }
            }
            Some((held, held_start))
                if returned > 0 && self.buffer_start + returned == held_start =>
            {
                match df.vstack(&held) {
                    Ok(joined) => (joined, self.buffer_start, Some(returned)),
                    Err(_) => (df, self.buffer_start, None),
                }
            }
            _ => (df, self.buffer_start, None),
        };
        let (df, start) = self.cut_to_caps(df, start, seam);
        CollectResult {
            df,
            start,
            returned,
            bytes_per_row,
            buffer_start: self.buffer_start,
            buffer_end: self.buffer_end,
            num_rows: self.num_rows,
            count_known: self.count_known,
            indexing: self.indexing,
        }
    }

    /// Cut `df` (rows `[start, start + df.height())`) to the row cap and byte budget,
    /// centered on the view (a head cut would drop a late jump's rows). Returns the kept
    /// rows and their first row. The budget bounds rows held between collects, not the
    /// collect.
    fn cut_to_caps(&self, df: DataFrame, start: usize, seam: Option<usize>) -> (DataFrame, usize) {
        let total = df.height();
        if total == 0 {
            return (df, start);
        }
        // The row cap as well: a row group stitched on to the rows on hand can run over it.
        let mut max_rows = total;
        if self.max_rows > 0 {
            max_rows = max_rows.min(self.max_rows);
        }
        if self.max_mb > 0 {
            let bytes_per_row = (df.estimated_size() / total).max(1);
            max_rows = max_rows.min(self.max_mb * 1024 * 1024 / bytes_per_row);
        }
        let max_rows = max_rows.max(1);
        if max_rows >= total {
            return (df, start);
        }
        let view_off = self.view_start.saturating_sub(start).min(total);
        let view_len = self.view_len.max(1).min(total);
        let view_center = view_off + view_len / 2;
        let mut keep_start = view_center.saturating_sub(max_rows / 2);
        if keep_start + max_rows > total {
            keep_start = total - max_rows;
        }
        let kept = max_rows.min(total - keep_start);
        (trim_rows(df, keep_start, kept, seam), start + keep_start)
    }
}

/// Result of a background buffer load, made by [`FillPlan::fit`] on the worker and
/// installed as it is by `apply_async_collect()`.
pub struct CollectResult {
    /// The buffer: the rows read, stitched and cut to the caps.
    pub(super) df: DataFrame,
    /// The first row of `df`.
    pub(super) start: usize,
    /// Rows the read returned, before the stitch and the cut.
    returned: usize,
    /// Bytes per row of the rows read, to plan the next fill by.
    bytes_per_row: Option<usize>,
    /// The range the read was planned for.
    buffer_start: usize,
    buffer_end: usize,
    /// Row count for the full (unsliced) dataset. Only meaningful when `count_known`.
    num_rows: usize,
    /// Whether `num_rows` is the true total. False for a first buffer read before the
    /// background `len()` count has resolved; `num_rows` is then provisional.
    count_known: bool,
    /// See `FillPlan::indexing`.
    indexing: bool,
}

impl CollectResult {
    /// The rows read, as the buffer will hold them.
    pub(crate) fn rows(&self) -> &DataFrame {
        &self.df
    }
}

/// A string's in-memory width when nothing says otherwise: the view plus a short value.
pub(super) const STRING_BYTES_GUESS: usize = 40;

/// Estimated in-memory bytes per row of `columns`: fixed widths, strings by the footer
/// average in `column_bytes` (or a guess) plus their view, nested by footer average or
/// guess. Binary columns are buffered as a stub (`binary_stub_exprs`).
pub(super) fn estimate_bytes_per_row(
    schema: &Schema,
    columns: &[String],
    column_bytes: &[(String, usize)],
) -> usize {
    let footer_width = |name: &String| {
        column_bytes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, w)| *w)
    };
    columns
        .iter()
        .map(|name| match schema.get(name.as_str()) {
            Some(DataType::String) => 16 + footer_width(name).unwrap_or(STRING_BYTES_GUESS - 16),
            Some(DataType::Binary) => 16 + binary_stub().len(),
            Some(DataType::Boolean) => 1,
            Some(DataType::Null) => 0,
            Some(dtype) if dtype.is_primitive_numeric() || dtype.is_temporal() => {
                match dtype.to_physical() {
                    DataType::Int8 | DataType::UInt8 => 1,
                    DataType::Int16 | DataType::UInt16 => 2,
                    DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
                    DataType::Int128 => 16,
                    _ => 8,
                }
            }
            Some(DataType::Decimal(..)) => 16,
            _ => footer_width(name).unwrap_or(64),
        })
        .sum::<usize>()
        .max(1)
}

/// Rows `[offset, offset + len)` of `df`, copied when a slice would keep much more
/// allocated (a single-chunk fill, a stitched union, shared string data), keeping a
/// `seam` as a chunk boundary (see [`compact_rows`]). A chunk that is itself a slice is
/// not seen through.
pub(super) fn trim_rows(
    df: DataFrame,
    offset: usize,
    len: usize,
    seam: Option<usize>,
) -> DataFrame {
    if backing_rows(&df, offset, len) > len + len / 4 {
        compact_rows(df, offset, len, seam)
    } else {
        df.slice(offset as i64, len)
    }
}

/// The most rows any column of `df` keeps allocated behind the slice `[offset, offset
/// + len)`: every chunk the slice touches, whole.
pub(super) fn backing_rows(df: &DataFrame, offset: usize, len: usize) -> usize {
    let end = offset + len;
    df.columns()
        .iter()
        .filter_map(Column::as_series)
        .map(|s| {
            let mut start = 0;
            let mut touched = 0;
            for chunk in s.chunks() {
                let chunk_end = start + chunk.len();
                if start < end && offset < chunk_end {
                    touched += chunk.len();
                }
                start = chunk_end;
            }
            touched
        })
        .max()
        .unwrap_or(len)
}

/// Rows `[offset, offset + len)` in their own storage: one chunk per column, two when
/// `seam` falls inside, so a later cut to one side lets the other go. Neither `rechunk`
/// nor `take` reliably releases the parent; builders with `ShareStrategy::Never` copy
/// everything. Constant columns stay one value. Each source column is released once
/// copied, bounding the extra memory to about one column.
pub(super) fn compact_rows(
    df: DataFrame,
    offset: usize,
    len: usize,
    seam: Option<usize>,
) -> DataFrame {
    use polars::series::builder::SeriesBuilder;
    use polars_arrow::array::builder::ShareStrategy;
    #[cfg(test)]
    tests::COMPACTIONS.with(|count| count.set(count.get() + 1));
    let len = len.min(df.height().saturating_sub(offset));
    let pieces = match seam.filter(|&seam| offset < seam && seam < offset + len) {
        Some(seam) => vec![(offset, seam - offset), (seam, offset + len - seam)],
        None => vec![(offset, len)],
    };
    let copy = |series: &Series, (offset, len): (usize, usize)| {
        let mut builder = SeriesBuilder::new(series.dtype().clone());
        builder.reserve(len);
        builder.subslice_extend(series, offset, len, ShareStrategy::Never);
        builder.freeze(series.name().clone())
    };
    let columns = df
        .into_columns()
        .into_iter()
        .map(|column| match column {
            Column::Scalar(constant) => {
                Column::new_scalar(constant.name().clone(), constant.scalar().clone(), len)
            }
            Column::Series(series) => {
                let mut kept = copy(&series, pieces[0]);
                for &piece in &pieces[1..] {
                    if kept.append_owned(copy(&series, piece)).is_err() {
                        kept = copy(&series, (offset, len));
                        break;
                    }
                }
                kept.into_column()
            }
        })
        .collect();
    // Cannot fail: the names are one frame's and every column was built to `len` rows.
    DataFrame::new(len, columns).unwrap_or_else(|_| DataFrame::empty_with_height(len))
}

/// Shrink `[buffer_start, buffer_end)` to at most `max_len` rows, kept around the view
/// `[view_start, view_end)` and inside `[floor, ceil)`.
pub(super) fn shrink_around_view(
    view_start: usize,
    view_end: usize,
    max_len: usize,
    floor: usize,
    ceil: usize,
    buffer_start: &mut usize,
    buffer_end: &mut usize,
) {
    if buffer_end.saturating_sub(*buffer_start) <= max_len {
        return;
    }
    let view_len = view_end.saturating_sub(view_start);
    if view_len >= max_len {
        *buffer_start = view_start;
        *buffer_end = (view_start + max_len).min(ceil);
        return;
    }
    let half = (max_len - view_len) / 2;
    *buffer_end = (view_end + half).min(ceil);
    *buffer_start = buffer_end.saturating_sub(max_len).max(floor);
    if *buffer_start > view_start {
        *buffer_start = view_start;
    }
    *buffer_end = (*buffer_start + max_len).min(ceil);
}

/// The most files one buffer read opens, beyond those the view itself spans.
pub(super) const MAX_FILES_PER_BUFFER: usize = 16;

/// Narrow `[start, end)` to at most `max_files` files, keeping every file the view
/// `[view_start, view_end)` lies in and adding the ones after it first.
pub(super) fn limit_files(
    offsets: &[usize],
    view_start: usize,
    view_end: usize,
    start: usize,
    end: usize,
    max_files: usize,
) -> (usize, usize) {
    let (Some((first, last)), Some((view_first, view_last))) = (
        files_holding(offsets, start, end.saturating_sub(start)),
        files_holding(
            offsets,
            view_start,
            view_end.saturating_sub(view_start).max(1),
        ),
    ) else {
        return (start, end);
    };
    // An empty file is not opened (see `window_of`), so it costs nothing to reach past.
    let opened = |from: usize, to: usize| (from..=to).filter(|&i| holds_rows(offsets, i)).count();
    if opened(first, last) <= max_files {
        return (start, end);
    }
    let (mut lo, mut hi) = (view_first.max(first), view_last.min(last));
    let mut files = opened(lo, hi);
    while files < max_files && (hi < last || lo > first) {
        if hi < last {
            hi += 1;
            files += usize::from(holds_rows(offsets, hi));
        }
        if files < max_files && lo > first {
            lo -= 1;
            files += usize::from(holds_rows(offsets, lo));
        }
    }
    (start.max(offsets[lo]), end.min(offsets[hi + 1]))
}

/// Whether file `i` has any rows, given where each file's rows start.
pub(super) fn holds_rows(offsets: &[usize], i: usize) -> bool {
    offsets[i + 1] > offsets[i]
}

/// The files from `first` to `last` that hold rows: a window reads these and passes
/// over the empty ones, which a dataset written a file a day can be mostly made of.
pub(super) fn files_with_rows(offsets: &[usize], first: usize, last: usize) -> Vec<usize> {
    (first..=last).filter(|&i| holds_rows(offsets, i)).collect()
}

/// The first and last files holding rows `[start, start + len)`, given where each file's
/// rows start (`offsets`, with the total last). `None` when the rows lie past the end.
pub(super) fn files_holding(offsets: &[usize], start: usize, len: usize) -> Option<(usize, usize)> {
    let files = offsets.len().checked_sub(1)?;
    let total = *offsets.last()?;
    if files == 0 || len == 0 || start >= total {
        return None;
    }
    let end = (start + len).min(total);
    // The file a row is in: the last one starting at or before it. Empty files start
    // where the next one does and are skipped over.
    let file_of = |row: usize| offsets.partition_point(|&o| o <= row).saturating_sub(1);
    Some((file_of(start), file_of(end - 1).min(files - 1)))
}

/// Rows `[start, start + len)` of `lf` as `all_columns`: with counted `files`, a scan of
/// only the files holding them; with `records`, read straight from the source.
pub(super) fn window_of(
    lf: &LazyFrame,
    files: Option<&RemoteFiles>,
    records: Option<&dyn crate::formats::pushdown::Windowed>,
    read_as_text: &[PlSmallStr],
    start: usize,
    len: usize,
    all_columns: Vec<Expr>,
) -> PolarsResult<LazyFrame> {
    // Polars gives an anonymous scan no row offset, so a slice deep in the view would
    // read every row before it; the source starts the window there instead.
    if let Some(records) = records {
        return Ok(records.window(start, len)?.select(all_columns));
    }
    if let Some((files, offsets)) = files.and_then(|f| f.offsets.as_ref().map(|o| (f, o)))
        && let Some((first, last)) = files_holding(offsets, start, len)
    {
        // The window's first file holds its first row, so leaving out the empty files
        // after it does not move the slice.
        let urls: Vec<String> = files_with_rows(offsets, first, last)
            .into_iter()
            .map(|i| files.urls[i].clone())
            .collect();
        let lf = (files.scan)(&urls, read_as_text)?;
        return Ok(lf
            .select(all_columns)
            .slice((start - offsets[first]) as i64, len as u32));
    }
    Ok(lf
        .clone()
        .select(all_columns)
        .slice(start as i64, len as u32))
}

/// The rows of a view, for a reader off the UI thread: read a window at a time as a
/// page is, or from the buffer the table already holds.
#[derive(Clone)]
pub(crate) struct ViewRows {
    lf: LazyFrame,
    files: Option<RemoteFiles>,
    /// See [`DataTableState::window_now`].
    records: Option<Arc<dyn crate::formats::pushdown::Windowed>>,
    read_as_text: Vec<PlSmallStr>,
    /// The buffer on hand and the view row it starts at.
    pub(crate) buffer: Option<(DataFrame, usize)>,
    /// The view's row count, when it is known.
    pub(crate) num_rows: Option<usize>,
    pub(crate) streaming: bool,
    /// Any window of the view reads all of it: see [`sees_every_row_first`].
    pub(crate) whole: bool,
    /// A window of the view reads every row before it: see [`reads_up_to_a_window`].
    pub(crate) reads_up_to: bool,
}

/// Whether `lf` has to see every row before it gives its first: a sort, a group by or a
/// pivot under it. Then a window of it costs as much as all of it.
pub(crate) fn sees_every_row_first(lf: &LazyFrame) -> bool {
    use polars::lazy::dsl::DslPlan;
    lf.logical_plan.into_iter().any(|node| {
        matches!(
            node,
            DslPlan::Sort { .. } | DslPlan::GroupBy { .. } | DslPlan::Pivot { .. }
        )
    })
}

/// Whether a window of `lf` reads every row before it: a filter (to find the first
/// match) or a scan with no row index to skip by (CSV). Parquet and IPC skip.
pub(crate) fn reads_up_to_a_window(lf: &LazyFrame) -> bool {
    use polars::lazy::dsl::{DslPlan, FileScanDsl};
    lf.logical_plan.into_iter().any(|node| match node {
        DslPlan::Filter { .. } => true,
        DslPlan::Scan { scan_type, .. } => !matches!(
            **scan_type,
            FileScanDsl::Parquet { .. } | FileScanDsl::Ipc { .. }
        ),
        _ => false,
    })
}

impl ViewRows {
    /// Rows `[start, start + len)` of the view as `exprs`.
    pub(crate) fn window(
        &self,
        start: usize,
        len: usize,
        exprs: Vec<Expr>,
    ) -> PolarsResult<LazyFrame> {
        window_of(
            &self.lf,
            self.files.as_ref(),
            self.records.as_deref(),
            &self.read_as_text,
            start,
            len,
            exprs,
        )
    }

    /// The view `lf`, with `buffer` on hand from row `buffer_start`.
    #[cfg(test)]
    pub(crate) fn of(lf: LazyFrame, buffer: Option<(DataFrame, usize)>) -> Self {
        Self {
            whole: sees_every_row_first(&lf),
            reads_up_to: reads_up_to_a_window(&lf),
            lf,
            files: None,
            records: None,
            read_as_text: Vec::new(),
            buffer,
            num_rows: None,
            streaming: false,
        }
    }
}

/// Snap `[start, end)` outward to whole row groups (`offsets`, total last). The groups
/// the view lies in are always whole (Polars fetches whole groups, so paging inside is
/// free); others the window reaches are added within `cap` rows (0: none), ahead of the
/// view first.
pub(super) fn align_to_row_groups(
    offsets: &[usize],
    view_start: usize,
    view_end: usize,
    start: usize,
    end: usize,
    cap: usize,
) -> (usize, usize) {
    let Some(groups) = offsets.len().checked_sub(1).filter(|n| *n > 0) else {
        return (start, end);
    };
    let group_of = |row: usize| {
        offsets
            .partition_point(|&o| o <= row)
            .saturating_sub(1)
            .min(groups - 1)
    };
    let last_row = |s: usize, e: usize| e.saturating_sub(1).max(s);
    let (mut lo, mut hi) = (
        group_of(view_start),
        group_of(last_row(view_start, view_end)),
    );
    let (want_lo, want_hi) = (group_of(start), group_of(last_row(start, end)));
    let fits = |lo: usize, hi: usize| cap == 0 || offsets[hi + 1] - offsets[lo] <= cap;
    loop {
        if hi < want_hi && fits(lo, hi + 1) {
            hi += 1;
        } else if lo > want_lo && fits(lo - 1, hi) {
            lo -= 1;
        } else {
            break;
        }
    }
    (offsets[lo], offsets[hi + 1])
}

impl DataTableState {
    /// Returns true if a scroll by `rows` would trigger a collect (view would leave the buffer).
    /// Used so the UI only shows the throbber when actual data loading will occur.
    pub fn scroll_would_trigger_collect(&self, rows: i64) -> bool {
        if rows < 0 && self.view.start_row == 0 {
            return false;
        }
        let new_start_row = if self.view.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.view.df.as_ref()
                && rows > 0
                && df.shape().0 <= self.visible_rows
            {
                return false;
            }
            let unclamped = (self.view.start_row as i64 + rows) as usize;
            if rows > 0 {
                unclamped.min(self.view.num_rows.saturating_sub(self.visible_rows))
            } else {
                unclamped
            }
        };
        if new_start_row == self.view.start_row {
            return false;
        }
        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.view.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.view.buffered_start_row
            && view_end <= self.view.buffered_end_row
            && self.view.buffered_end_row > 0;
        !within_buffer
    }

    /// Scroll by `rows`: within the buffer, re-slice the display; outside it, set the
    /// position and return true so the caller collects.
    pub fn slide_table(&mut self, rows: i64) -> bool {
        if rows < 0 && self.view.start_row == 0 {
            return false;
        }

        let new_start_row = if self.view.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.view.df.as_ref()
                && rows > 0
                && df.shape().0 <= self.visible_rows
            {
                return false;
            }
            let unclamped = (self.view.start_row as i64 + rows) as usize;
            if rows > 0 {
                // Keep a screen of data in view: otherwise held PageDown at the bottom pushes past
                // `num_rows`, repeatedly asking for no-op collects.
                unclamped.min(self.view.num_rows.saturating_sub(self.visible_rows))
            } else {
                unclamped
            }
        };

        if new_start_row == self.view.start_row {
            return false;
        }

        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.view.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.view.buffered_start_row
            && view_end <= self.view.buffered_end_row
            && self.view.buffered_end_row > 0;

        self.view.start_row = new_start_row;

        if within_buffer {
            if self.table_state.selected().is_none() {
                self.table_state.select(Some(0));
            }
            false
        } else {
            true // caller must collect
        }
    }

    /// Read the view's rows synchronously, as a job does for the app; for tests only.
    #[cfg(test)]
    pub fn collect(&mut self) {
        if self.defer_collect {
            return;
        }
        if !self.view.num_rows_valid {
            // A count that fails means the frame itself is broken: say so rather than
            // draw it as empty.
            match collect_lazy(row_count_lf(&self.view.lf), self.polars_streaming) {
                Ok(df) => {
                    self.error = None;
                    let n = match df.get(0).as_deref().and_then(|row| row.first()) {
                        Some(AnyValue::UInt64(len)) => *len as usize,
                        _ => 0,
                    };
                    self.set_num_rows(n);
                }
                Err(e) => {
                    self.error = Some(e);
                    self.set_num_rows(0);
                }
            }
        }
        let Some(request) = self.prepare_async_collect(None) else {
            return;
        };
        match collect_lazy(request.lf, request.polars_streaming) {
            Ok(df) => self.apply_async_collect(request.plan.fit(df)),
            Err(e) => self.error = Some(e),
        }
    }

    /// In the app a mutation asks for its rows: the event loop reads them on a job
    /// after the next frame. Under [`Self::deferred`] the caller reads them itself.
    #[cfg(not(test))]
    pub(super) fn collect(&mut self) {
        if !self.defer_collect {
            self.needs_recollect = true;
        }
    }

    /// Expressions for every column in `column_order`, binary columns replaced by a stub
    /// ([`binary_stub`]) so blobs are never read, for the display buffer and analysis
    /// (multi-GB blobs would exhaust memory). `lf` keeps the bytes for export.
    pub(crate) fn binary_stub_exprs(&self) -> Vec<Expr> {
        self.view
            .column_order
            .iter()
            .map(|name| {
                if matches!(self.view.schema.get(name.as_str()), Some(DataType::Binary)) {
                    lit(binary_stub()).alias(name.as_str())
                } else {
                    col(name.as_str())
                }
            })
            .collect()
    }

    /// Plan an async collect without blocking: clamp the start row, then a `CollectRequest`
    /// if the rows on screen need a new buffer, or `None` (display slices updated). Without
    /// `num_rows_override`, `Self::num_rows_bound` lets the first rows skip the count.
    pub fn prepare_async_collect(
        &mut self,
        num_rows_override: Option<usize>,
    ) -> Option<CollectRequest> {
        if self.visible_rows > 0 {
            self.proximity_threshold = self.proximity();
        }

        if let Some(n) = num_rows_override {
            self.view.num_rows = n;
            self.view.num_rows_valid = true;
        }

        // `bound` is the total, or `usize::MAX` while `len()` runs, planning a top-of-data
        // window without waiting. See `num_rows_bound`.
        let count_known = self.view.num_rows_valid;
        let bound = self.num_rows_bound();

        if count_known {
            if self.view.num_rows > 0 {
                let max_start = self.view.num_rows.saturating_sub(1);
                if self.view.start_row > max_start {
                    self.view.start_row = max_start;
                }
            } else {
                // Confirmed-empty dataset: clear everything.
                self.view.start_row = 0;
                self.drop_buffer();
                self.view.df = None;
                self.view.locked_df = None;
                return None;
            }
        }

        // No column shown: there are no rows to read, and a read of none would come
        // back empty and ask again.
        if self.view.column_order.is_empty() {
            self.drop_buffer();
            self.view.df = None;
            self.view.locked_df = None;
            return None;
        }

        let view_start = self.view.start_row;
        let view_end = self.view.start_row + self.visible_rows.min(bound - self.view.start_row);
        let within_buffer = view_start >= self.view.buffered_start_row
            && view_end <= self.view.buffered_end_row
            && self.view.buffered_end_row > 0;

        // Compute the buffer range using the same logic as collect().
        let (new_buffer_start, new_buffer_end) = if within_buffer {
            let dist_to_start = view_start.saturating_sub(self.view.buffered_start_row);
            let dist_to_end = self.view.buffered_end_row.saturating_sub(view_end);
            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.view.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.view.buffered_end_row < bound;

            if !needs_expansion_back && !needs_expansion_forward {
                // Buffer is fine, just re-slice display.
                (self.view.buffered_start_row, self.view.buffered_end_row)
            } else {
                let mut s = if needs_expansion_back {
                    view_start.saturating_sub(self.reach_rows(self.pages_lookback))
                } else {
                    self.view.buffered_start_row
                };
                let mut e = if needs_expansion_forward {
                    (view_end + self.reach_rows(self.pages_lookahead)).min(bound)
                } else {
                    self.view.buffered_end_row
                };
                self.fit_window(view_start, view_end, &mut s, &mut e);
                (s, e)
            }
        } else {
            let had_buffer = self.view.buffered_end_row > 0;
            let scrolled_past_end = had_buffer && view_start >= self.view.buffered_end_row;
            let scrolled_past_start = had_buffer && view_end <= self.view.buffered_start_row;
            let extend_forward_ok = scrolled_past_end
                && (view_start - self.view.buffered_end_row)
                    <= self.reach_rows(self.pages_lookahead);
            let extend_backward_ok = scrolled_past_start
                && (self.view.buffered_start_row - view_end)
                    <= self.reach_rows(self.pages_lookback);

            let mut s;
            let mut e;
            if extend_forward_ok {
                s = self.view.buffered_start_row;
                e = (view_end + self.reach_rows(self.pages_lookahead)).min(bound);
            } else if extend_backward_ok {
                s = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                e = self.view.buffered_end_row;
            } else {
                s = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                e = (view_end + self.reach_rows(self.pages_lookahead)).min(bound);
                let min_initial_len = self.min_buffer_len();
                let current_len = e.saturating_sub(s);
                if current_len < min_initial_len {
                    let need = min_initial_len.saturating_sub(current_len);
                    let can_extend_end = bound.saturating_sub(e);
                    let can_extend_start = s;
                    if can_extend_end >= need {
                        e = (e + need).min(bound);
                    } else if can_extend_start >= need {
                        s = s.saturating_sub(need);
                    } else {
                        e = (e + can_extend_end).min(bound);
                        s = s.saturating_sub(need.saturating_sub(can_extend_end));
                    }
                }
            }
            self.fit_window(view_start, view_end, &mut s, &mut e);
            (s, e)
        };

        let buffer_size = new_buffer_end.saturating_sub(new_buffer_start);
        if buffer_size == 0 {
            return None;
        }
        // Already held: the view fits, or fitting the expansion to whole row groups
        // gave back the group on hand.
        if self.holds_buffer(new_buffer_start, new_buffer_end) {
            self.slice_buffer_into_display();
            if self.table_state.selected().is_none() {
                self.table_state.select(Some(0));
            }
            return None;
        }

        let lf = match self.buffer_lf(new_buffer_start, buffer_size) {
            Ok(lf) => lf,
            Err(e) => {
                self.error = Some(e);
                return None;
            }
        };

        // Unknown count: `num_rows` is provisional (this buffer's end); the background `len()`
        // corrects it unless a short read reveals the end.
        let num_rows = if count_known {
            self.view.num_rows
        } else {
            new_buffer_end
        };
        Some(CollectRequest {
            lf,
            polars_streaming: self.polars_streaming,
            buffer_start: new_buffer_start,
            buffer_end: new_buffer_end,
            plan: self.fill_plan(new_buffer_start, new_buffer_end, num_rows, count_known),
        })
    }

    /// How a fill of `[buffer_start, buffer_end)` is to be made the buffer, from what
    /// is held and shown now. See [`FillPlan`].
    pub(super) fn fill_plan(
        &self,
        buffer_start: usize,
        buffer_end: usize,
        num_rows: usize,
        count_known: bool,
    ) -> FillPlan {
        let held = self
            .abuts_buffer(buffer_start, buffer_end.saturating_sub(buffer_start))
            .then(|| self.view.buffered_df.clone())
            .flatten()
            .map(|df| (df, self.view.buffered_start_row));
        FillPlan {
            buffer_start,
            buffer_end,
            num_rows,
            count_known,
            indexing: self.indexing().is_some(),
            held,
            view_start: self.view.start_row,
            view_len: self.visible_rows,
            max_rows: self.max_buffered_rows,
            max_mb: self.max_buffered_mb,
        }
    }

    /// Apply the result of a background buffer load. The worker has already stitched
    /// and cut it ([`FillPlan::fit`]): installing it copies nothing.
    pub fn apply_async_collect(&mut self, result: CollectResult) {
        let CollectResult {
            df,
            start,
            returned: returned_rows,
            bytes_per_row,
            buffer_start,
            buffer_end,
            num_rows,
            count_known,
            indexing,
        } = result;
        let requested_rows = buffer_end.saturating_sub(buffer_start);

        if count_known {
            self.view.num_rows = num_rows;
            self.view.num_rows_valid = true;
        } else if returned_rows < requested_rows
            && (buffer_start == 0 || returned_rows > 0)
            // Lines still being indexed end where the indexing has got to, not the file.
            && !indexing
            && self.indexing().is_none()
        {
            // A short read that began inside the data gives the exact total; an empty slice deep
            // in the frame may lie past the data, so only the count can say.
            self.view.num_rows = buffer_start + returned_rows;
            self.view.num_rows_valid = true;
        } else if !self.view.num_rows_valid {
            // A full buffer with the count pending: a provisional total (at least this buffer's
            // end), corrected by `count_landed()`.
            self.view.num_rows = self.view.num_rows.max(buffer_end);
        }
        // else: the background len() already resolved the exact count between this
        // buffer being requested and applied — keep it; don't downgrade to provisional.
        self.error = None;
        self.remember_pristine_count();

        if bytes_per_row.is_some() {
            self.view.observed_bytes_per_row = bytes_per_row;
        }
        // A fill without the view's first row was planned for replaced rows: keep what is held
        // and replan. One holding the first row but not the whole view (a resize) is kept and
        // the rest fetched. After a short read, a view past the end shows only rows up to it.
        let end = start + df.height();
        let view_end = self.view.start_row + self.visible_rows.max(1);
        let reaches_end = end >= buffer_start + returned_rows;
        let shows_view = start <= self.view.start_row
            && (self.view.start_row < end || (returned_rows < requested_rows && reaches_end));
        if !shows_view {
            self.needs_recollect = true;
            return;
        }
        self.release_display_buffer();
        self.view.buffered_start_row = start;
        self.view.buffered_end_row = end;
        self.view.buffered_df = Some(df);
        // Slice the buffered DataFrame into display DataFrames (locked + scroll columns).
        self.slice_buffer_into_display();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
        if view_end > end && end < self.view.num_rows {
            self.needs_recollect = true;
        }
    }

    /// True when `rows` rows fetched from `start` run on from the rows on hand or up to
    /// them, so a fill of them is planned to be stitched on (see [`FillPlan`]).
    fn abuts_buffer(&self, start: usize, rows: usize) -> bool {
        self.stitches_buffer()
            && (start == self.view.buffered_end_row || start + rows == self.view.buffered_start_row)
    }

    /// A view of `sample`, drawn from `source` into `rows`, with `schema`'s columns. It
    /// starts empty and grows via [`Self::sample_grew`]. `through`: drawn from the view's
    /// query or filters rather than the source.
    pub(crate) fn sampled_from(
        source: DataTableState,
        sample: crate::analysis::sampling::Sample,
        schema: &Schema,
        rows: Arc<crate::analysis::table_sample::SampleRows>,
        through: bool,
        path: Option<crate::analysis::table_sample::DrawPath>,
    ) -> Result<Self> {
        let mut view = source.sample_view(DataFrame::empty_with_schema(schema))?;
        let frame = scanned_frame(&view.original_lf)
            .ok_or_else(|| color_eyre::eyre::eyre!("a sample's frame has no rows to scan"))?;
        view.sampled = Some(Box::new(Sampled {
            source: Box::new(source),
            sample,
            rows,
            frame,
            through,
            drawn: None,
            path,
        }));
        Ok(view)
    }

    /// The view's sample, while it has one.
    pub fn sampled(&self) -> Option<&Sampled> {
        self.sampled.as_deref()
    }

    /// The view the sample was drawn from, or this one when it has none: where a new
    /// sample is drawn from.
    pub fn unsampled(&self) -> &DataTableState {
        self.sampled
            .as_ref()
            .map_or(self, |sampled| sampled.source.as_ref())
    }

    /// The view the sample was drawn from, putting the sample down; `self` when it
    /// has none.
    pub(crate) fn into_unsampled(mut self) -> DataTableState {
        match self.sampled.take() {
            Some(sampled) => *sampled.source,
            None => self,
        }
    }

    /// Take the chunks drawn since the last call into every frame (so query, filters and
    /// sort run over them). `None` if none; otherwise whether the rows on hand still stand
    /// (they do while nothing reorders, since new rows come after).
    pub(crate) fn sample_grew(&mut self) -> Option<bool> {
        let sampled = self.sampled.as_ref()?;
        let chunks = sampled.rows.take_new();
        if chunks.is_empty() {
            return None;
        }
        // On the same buffers: each column takes the chunks' arrays, nothing copied.
        let mut frame = (*sampled.frame).clone();
        for chunk in &chunks {
            frame.vstack_mut(chunk).ok()?;
        }
        Some(self.rebind_sample(Arc::new(frame), false))
    }

    /// The draw ended, having read what `drawn` says: the rows go into the order the
    /// source holds them, once.
    pub(crate) fn sample_drawn(&mut self, drawn: crate::analysis::table_sample::Drawn) {
        let Some(sampled) = self.sampled.as_mut() else {
            return;
        };
        // One chunk per column from here: the many the draw left would slow every
        // read, and the chunks are let go so the rows are held once.
        let ordered = sampled.rows.take_in_source_order().ok().flatten();
        // A seeded read of one file needs no path; it was not one, then.
        sampled.path = drawn.path;
        sampled.drawn = Some(drawn);
        if let Some(frame) = ordered {
            self.rebind_sample(Arc::new(frame), true);
        }
    }

    /// Every frame scans `frame` in place of the sample's last one. `reordered` when
    /// the rows already shown changed places. Returns whether the rows on hand stand.
    fn rebind_sample(&mut self, frame: Arc<DataFrame>, reordered: bool) -> bool {
        let Some(old) = self.sampled.as_ref().map(|sampled| sampled.frame.clone()) else {
            return false;
        };
        let rows_stand = !reordered
            && self.view.sort_columns.is_empty()
            && self.view.sort_ascending
            && self.scan_is_the_root();
        let rows = frame.height();
        self.each_frame(|lf| {
            crate::analysis::table_sample::rebind(&mut lf.logical_plan, &old, &frame)
        });
        if let Some(sampled) = self.sampled.as_mut() {
            sampled.frame = frame;
        }
        self.invalidate_num_rows();
        if self.is_pristine() {
            self.set_num_rows(rows);
        } else if self.scan_is_the_root() {
            self.pristine_rows = Some(rows);
        }
        if !rows_stand {
            self.drop_buffer();
        }
        self.needs_recollect = true;
        rows_stand
    }

    /// Bytes per row of a sample of this view: every column of the source (from source) or
    /// the view, shown or not.
    pub(crate) fn sample_row_bytes(&self, from_source: bool) -> usize {
        let schema = if from_source {
            &self.original_schema
        } else {
            &self.view.schema
        };
        let columns: Vec<String> = schema
            .iter_names()
            .filter(|name| name.as_str() != crate::formats::schema_union::DRIFT_COLUMN)
            .map(|name| name.to_string())
            .collect();
        // What the table measured, when it measured these columns.
        if !from_source && columns.len() == self.view.column_order.len() {
            return self.bytes_per_row();
        }
        estimate_bytes_per_row(schema, &columns, &self.column_bytes)
    }

    /// How many files a page at `start` would read: only for a windowed remote scan;
    /// `None` (not zero) when Polars reads the whole scan as it decides.
    pub fn files_a_page_reads(&self, start: usize, len: usize) -> Option<usize> {
        let offsets = self.files_window().and_then(|f| f.offsets.as_ref())?;
        let (first, last) = files_holding(offsets, start, len)?;
        Some(files_with_rows(offsets, first, last).len())
    }

    /// The frame for buffer rows `[start, start + len)`, columns in display order. For a
    /// remote dataset whose files are counted, a scan of only the files holding them.
    pub(super) fn buffer_lf(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let mut all_columns = self.binary_stub_exprs();
        if self.carries_source_rows() {
            all_columns.push(col(crate::formats::schema_union::DRIFT_COLUMN));
        }
        self.window_lf(start, len, all_columns)
    }

    /// Whether rows carry their source position for `#`: file-traced rows or lines while
    /// the frame is the scan's (query, reshape and group rows do not).
    pub(crate) fn carries_source_rows(&self) -> bool {
        self.view.drift_column_present
            || (self.scan_is_the_root() && (self.source_rows_at_open || self.view.view_numbered))
    }

    /// What `#` shows for `rows` rows from `start`: source positions where carried, else
    /// view positions from `row_start_index` (the same when pristine).
    pub fn row_numbers_from(&self, start: usize, rows: usize) -> Vec<usize> {
        let view = |i: usize| start + i + self.row_start_index;
        let places = self
            .view
            .buffered_df
            .as_ref()
            .filter(|_| self.carries_source_rows())
            .and_then(|df| df.column(crate::formats::schema_union::DRIFT_COLUMN).ok())
            .and_then(|column| {
                let offset = start.checked_sub(self.view.buffered_start_row)?;
                let len = rows.min(column.len().saturating_sub(offset));
                let slice = column.slice(offset as i64, len);
                let places = slice.u32().ok()?;
                // Several files' lines are numbered in their own file.
                let place = |p: usize| {
                    self.numbering
                        .as_ref()
                        .and_then(|lines| lines.line_in_file(p))
                        .unwrap_or(p)
                };
                Some(
                    places
                        .iter()
                        .map(|p| p.map(|p| place(p as usize) + self.row_start_index))
                        .collect::<Vec<_>>(),
                )
            });
        (0..rows)
            .map(|i| {
                places
                    .as_ref()
                    .and_then(|p| p.get(i).copied().flatten())
                    .unwrap_or_else(|| view(i))
            })
            .collect()
    }

    /// The frame for rows `[start, start + len)` of the view, as `all_columns`. For a
    /// remote dataset whose files are counted, a scan of only the files holding them.
    pub(super) fn window_lf(
        &self,
        start: usize,
        len: usize,
        all_columns: Vec<Expr>,
    ) -> PolarsResult<LazyFrame> {
        window_of(
            &self.view.lf,
            self.files_window(),
            self.window_now().as_deref(),
            &self.read_as_text,
            start,
            len,
            all_columns,
        )
    }

    /// The view's rows as a find reads them: a window at a time, the way a page is
    /// read, and the buffer already on hand.
    pub(crate) fn view_rows(&self) -> ViewRows {
        ViewRows {
            lf: self.view.lf.clone(),
            files: self.files_window().cloned(),
            // A find reads every row it can reach: lines still being indexed are read
            // through the frame, which waits for them, not the window of those so far.
            records: self.window_now().filter(|_| self.indexing().is_none()),
            read_as_text: self.read_as_text.clone(),
            buffer: self
                .view
                .buffered_df
                .as_ref()
                .filter(|_| self.buffer_on_hand())
                .map(|df| (df.clone(), self.view.buffered_start_row)),
            num_rows: self.view.num_rows_valid.then_some(self.view.num_rows),
            streaming: self.polars_streaming,
            whole: sees_every_row_first(&self.view.lf),
            reads_up_to: reads_up_to_a_window(&self.view.lf),
        }
    }

    /// Center the cursor on view row `row`, where a find matched; true if a collect is
    /// needed. A row past a provisional total extends it until the count lands.
    pub(crate) fn go_to_found_row(&mut self, row: usize) -> bool {
        if !self.view.num_rows_valid && self.view.num_rows <= row {
            self.view.num_rows = row + 1;
        }
        self.scroll_to_row_centered(row)
    }

    /// The view row the cursor is on.
    pub(crate) fn cursor_row(&self) -> usize {
        self.view.start_row + self.table_state.selected().unwrap_or(0)
    }

    /// Bytes a buffered row takes: measured on the last buffer collected, or until
    /// then estimated from the schema.
    pub(super) fn bytes_per_row(&self) -> usize {
        self.view.observed_bytes_per_row.unwrap_or_else(|| {
            estimate_bytes_per_row(
                &self.view.schema,
                &self.view.column_order,
                &self.column_bytes,
            )
        })
    }

    /// The best in-memory width estimate per row, shared by buffer planning and Data
    /// Quality's (approximate) preflight so they agree.
    pub fn estimated_row_bytes(&self) -> usize {
        self.bytes_per_row()
    }

    /// Number of source files known to participate in the pristine dataset scan.
    /// Returns `None` after a query or reshape has broken the row-to-file mapping.
    pub fn source_file_count(&self) -> Option<usize> {
        self.is_pristine().then(|| self.loaded_file_count())
    }

    /// Files the dataset was loaded from, whatever the view does with their rows.
    pub(crate) fn loaded_file_count(&self) -> usize {
        if !self.drift_files.is_empty() {
            self.drift_files.len()
        } else if let Some(remote) = &self.remote_files {
            remote.urls.len()
        } else {
            1
        }
    }

    /// Rows the `max_buffered_mb` budget allows, at least a screen; 0 for none. Planned to,
    /// so a wide window is never materialized only to be cut.
    pub(super) fn byte_cap_rows(&self) -> usize {
        if self.max_buffered_mb == 0 {
            return 0;
        }
        let max_bytes = self.max_buffered_mb * 1024 * 1024;
        (max_bytes / self.bytes_per_row()).max(self.visible_rows.max(1))
    }

    /// Whether the buffer is a remote window: a pristine object-store scan. With anything
    /// applied, `slice(0, N)` stops at N matches, so page windows cost a row group, not
    /// forty.
    pub(super) fn remote_window(&self) -> bool {
        self.remote_source && self.is_pristine()
    }

    /// The files a page reads by, while the frame is the scan as loaded: a filter or
    /// sort reads every file before its window, so it goes through the whole scan.
    fn files_window(&self) -> Option<&RemoteFiles> {
        self.remote_files.as_ref().filter(|_| self.is_pristine())
    }

    /// Rows the buffer reaches past the view one way: `pages` of it locally, for remote
    /// scans with something applied, or many-file datasets (where cost is files opened, so
    /// a few at a time); half the window for a pristine remote object (`fit_window` trims
    /// to the cap).
    fn reach_rows(&self, pages: usize) -> usize {
        if !self.remote_window() || self.remote_files.is_some() {
            return pages * self.visible_rows.max(1);
        }
        let window = if self.max_buffered_rows > 0 {
            self.max_buffered_rows
        } else {
            DEFAULT_MAX_BUFFERED_ROWS
        };
        window / 2
    }

    /// The smallest buffer worth filling: a page plus the reach either side.
    fn min_buffer_len(&self) -> usize {
        self.visible_rows.max(1)
            + self.reach_rows(self.pages_lookahead)
            + self.reach_rows(self.pages_lookback)
    }

    /// True when the view already shows the last page, so End has nothing to load.
    pub fn at_end(&self) -> bool {
        self.view.start_row == self.view.num_rows.saturating_sub(self.visible_rows)
    }

    /// Fit `[buffer_start, buffer_end)` to the caps around the view, then for a remote
    /// object with a known footer to the view's row groups, cut back to the caps inside.
    fn fit_window(
        &self,
        view_start: usize,
        view_end: usize,
        buffer_start: &mut usize,
        buffer_end: &mut usize,
    ) {
        let byte_cap = self.byte_cap_rows();
        let cap = match (self.max_buffered_rows, byte_cap) {
            (0, cap) | (cap, 0) => cap,
            (rows, bytes) => rows.min(bytes),
        };
        if cap > 0 {
            shrink_around_view(
                view_start,
                view_end,
                cap,
                0,
                self.num_rows_bound(),
                buffer_start,
                buffer_end,
            );
        }
        let Some(offsets) = self
            .row_group_offsets
            .as_deref()
            .filter(|_| self.remote_window())
        else {
            return;
        };
        (*buffer_start, *buffer_end) = align_to_row_groups(
            offsets,
            view_start,
            view_end,
            *buffer_start,
            *buffer_end,
            cap,
        );
        // Caps hold inside a group: an oversized group is read a window at a time, never
        // pulling the next group early.
        if cap > 0 {
            let (floor, ceil) = (*buffer_start, *buffer_end);
            shrink_around_view(
                view_start,
                view_end,
                cap,
                floor,
                ceil,
                buffer_start,
                buffer_end,
            );
        }
        // Over many files, at most a few of them, around the view's.
        if let Some(file_offsets) = self.remote_files.as_ref().and_then(|f| f.offsets.as_ref()) {
            (*buffer_start, *buffer_end) = limit_files(
                file_offsets,
                view_start,
                view_end,
                *buffer_start,
                *buffer_end,
                MAX_FILES_PER_BUFFER,
            );
        }
        // A view straddling two groups needs both, but one is on hand: fetch the other
        // alone and stitch it on (see `apply_async_collect`).
        if self.buffer_on_hand() {
            let (held_start, held_end) = (self.view.buffered_start_row, self.view.buffered_end_row);
            if held_start <= *buffer_start && *buffer_start < held_end && held_end < *buffer_end {
                *buffer_start = held_end;
            } else if *buffer_start < held_start
                && held_start < *buffer_end
                && *buffer_end <= held_end
            {
                *buffer_end = held_start;
            }
        }
    }

    /// Release the replaced buffer and its display frames (a stitch already took what it
    /// keeps); a requested relearn applies to the next rows.
    fn release_display_buffer(&mut self) {
        self.widths.rows_arrived();
        self.view.buffered_df = None;
        self.view.locked_df = None;
        self.view.df = None;
    }

    /// Recompute locked_df and df from the cached full buffer. Used when only termcol_index (or locked columns) changed.
    pub(super) fn slice_buffer_into_display(&mut self) {
        let full_df = match self.view.buffered_df.as_ref() {
            Some(df) => df,
            None => return,
        };

        if self.view.locked_columns_count > 0 {
            let locked_names: Vec<&str> = self
                .view
                .column_order
                .iter()
                .take(self.view.locked_columns_count)
                .map(|s| s.as_str())
                .collect();
            if let Ok(locked_df) = full_df.select(locked_names) {
                self.view.locked_df = Some(locked_df);
            }
        } else {
            self.view.locked_df = None;
        }

        let scroll_names: Vec<&str> = self
            .view
            .column_order
            .iter()
            .skip(self.frozen_shown() + self.termcol_index)
            .map(|s| s.as_str())
            .collect();
        if scroll_names.is_empty() {
            self.view.df = None;
        } else {
            if let Ok(scroll_df) = full_df.select(scroll_names) {
                self.view.df = Some(scroll_df);
            }
        }
    }

    /// Whether the view is inside the buffer within a page of an end with more data past
    /// it: where to grow the buffer ahead, before an in-buffer scroll leaves it blank.
    pub fn wants_to_load_ahead(&self) -> bool {
        if self.visible_rows == 0
            || self.view.buffered_df.is_none()
            || !self.page_on_hand(self.view.start_row)
        {
            return false;
        }
        let near = self.proximity();
        let view_end = self.view.start_row
            + self
                .visible_rows
                .min(self.num_rows_bound().saturating_sub(self.view.start_row));
        let behind = self.view.start_row - self.view.buffered_start_row <= near
            && self.view.buffered_start_row > 0;
        let ahead = self.view.buffered_end_row - view_end <= near
            && self.view.buffered_end_row < self.num_rows_bound();
        behind || ahead
    }

    /// How near an end the view comes before the buffer grows: half the reach, at least a
    /// page (a cloud fetch outlasts a PageDown).
    fn proximity(&self) -> usize {
        (self.reach_rows(self.pages_lookahead) / 2).max(self.visible_rows)
    }

    /// Where the view and the buffer are, to tell one load-ahead attempt from the next.
    pub fn buffer_position(&self) -> (u64, usize, usize, usize) {
        (
            self.len_generation(),
            self.view.start_row,
            self.view.buffered_start_row,
            self.view.buffered_end_row,
        )
    }

    /// Whether every row of the page starting at `start` is in the buffer.
    pub(crate) fn page_on_hand(&self, start: usize) -> bool {
        let bound = self.num_rows_bound();
        let end = start + self.visible_rows.min(bound.saturating_sub(start));
        self.view.buffered_df.is_some()
            && self.view.buffered_end_row > 0
            && start >= self.view.buffered_start_row
            && end <= self.view.buffered_end_row
    }

    /// The first row to draw: the view's own once its rows are on hand, else the last page
    /// drawn whole, so a pending fetch never shows half a page of nothing.
    pub(crate) fn start_to_draw(&mut self) -> usize {
        if self.page_on_hand(self.view.start_row) {
            self.view.drawn_start = self.view.start_row;
            self.view.start_row
        } else if self.page_on_hand(self.view.drawn_start) {
            self.view.drawn_start
        } else {
            self.view.start_row
        }
    }
}
