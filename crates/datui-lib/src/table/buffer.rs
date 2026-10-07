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
    /// Row count for the full (unsliced) dataset. Only meaningful when `count_known`.
    pub num_rows: usize,
    /// Whether `num_rows` is the true total. False for a first buffer rendered before
    /// the background `len()` count has resolved; in that case `num_rows` is provisional.
    pub count_known: bool,
    /// How the worker fits the rows it reads to the buffer: [`FillPlan::fit`].
    pub plan: FillPlan,
}

/// What the worker that reads a fill needs to make it the buffer: the rows on hand it
/// runs on from or up to, the view, and the caps, as they were when it was planned.
///
/// A trim copies the rows it keeps when a slice would keep the fill allocated behind
/// them (see [`trim_rows`]): up to the byte budget, too long for the UI thread (#483).
/// The worker does it, and `apply_async_collect` installs what it hands back as it is.
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
    /// Make the buffer of `df`, the rows read for the planned range: stitched on to
    /// the rows on hand when it runs on from them or up to them, then cut to the caps
    /// around the view.
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

    /// Cut `df`, spanning `[start, start + df.height())`, to the row cap and the byte
    /// budget. The rows kept are centered on the view rather than taken from the head:
    /// a jump near the end of the dataset would otherwise drop exactly the rows the
    /// view needs. Returns the rows kept and their first row.
    ///
    /// The budget bounds the rows held between collects, not the collect itself: the
    /// fill, the operators upstream of it and an eager source frame all take memory of
    /// their own.
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
    num_rows: usize,
    /// See `CollectRequest::count_known`.
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

/// Bytes a row of `columns` takes in memory, estimated from the schema: the width of
/// each fixed-size type; for a string the footer's average in `column_bytes` (or a
/// guess) plus its view; for a nested column the footer's average, else a guess.
/// Binary columns are buffered as a stub (see `binary_stub_exprs`).
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

/// The rows `[offset, offset + len)` of `df`, copied when a slice of them would keep
/// much more allocated than they are. A `seam` inside them, where a stitch joined two
/// fills, stays a chunk boundary (see [`compact_rows`]).
///
/// A slice keeps every chunk it touches. A fill read in many chunks (a Parquet or CSV
/// scan) lets the rest go with a slice alone; one read in a single chunk, a stitched
/// union or a string column sharing its parent's data would keep the whole fill. A
/// chunk that is itself a slice of more is not seen through.
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

/// The rows `[offset, offset + len)` of `df` in storage of their own: one chunk a
/// column, or two when `seam` falls inside them, so a later cut down to one side of a
/// stitch (`holds_buffer`) is a slice that lets the other side go.
///
/// A slice keeps the whole of its parent allocated, and neither `rechunk` (a lone chunk
/// is left as it is) nor `take` (a string column keeps its parent's data buffers) is
/// sure to let go of it. Polars' builders with `ShareStrategy::Never` copy every
/// physical type, nested children and string bytes included. A constant column stays
/// one value: built out, it would be a copy of the value per row.
///
/// Each column of `df` is let go of once it is copied, so the copy costs about one
/// column's kept rows over `df` rather than all of them. On the collect worker the
/// rows on screen are still held meanwhile (#483).
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

/// Rows `[start, start + len)` of `lf` as `all_columns`. With `files` counted, a scan
/// of only the files holding them, so a window deep in a remote dataset does not read
/// every file before it. With `records`, the rows read straight from the source.
pub(super) fn window_of(
    lf: &LazyFrame,
    files: Option<&RemoteFiles>,
    records: Option<&dyn crate::pushdown::Windowed>,
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
    records: Option<Arc<dyn crate::pushdown::Windowed>>,
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

/// Whether a window of `lf` reads every row before it: a filter, which has to test
/// them to know which row is the window's first, or a scan with no row index to skip
/// by, such as a CSV. Parquet and IPC skip to a window.
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

/// Snap `[start, end)` outward to the row groups it touches, given where each group
/// starts (`offsets`, with the total last).
///
/// Polars fetches a row group whole for any slice that touches it, so the groups the
/// view `[view_start, view_end)` lies in are always taken whole: paging inside them then
/// costs nothing. The other groups the window reaches into are added while the result
/// stays within `cap` rows (0 for no cap), the ones ahead of the view first.
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
        if rows < 0 && self.start_row == 0 {
            return false;
        }
        let new_start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.df.as_ref()
                && rows > 0
                && df.shape().0 <= self.visible_rows
            {
                return false;
            }
            let unclamped = (self.start_row as i64 + rows) as usize;
            if rows > 0 {
                unclamped.min(self.num_rows.saturating_sub(self.visible_rows))
            } else {
                unclamped
            }
        };
        if new_start_row == self.start_row {
            return false;
        }
        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;
        !within_buffer
    }

    /// Update scroll position. If the view is within the buffer, re-slices display.
    /// If outside the buffer, sets the position but the caller must trigger a collect
    /// (synchronous or async) to load the new buffer range.
    /// Returns true if a collect is needed (view is outside the current buffer).
    pub fn slide_table(&mut self, rows: i64) -> bool {
        if rows < 0 && self.start_row == 0 {
            return false;
        }

        let new_start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.df.as_ref()
                && rows > 0
                && df.shape().0 <= self.visible_rows
            {
                return false;
            }
            let unclamped = (self.start_row as i64 + rows) as usize;
            if rows > 0 {
                // Clamp forward scroll to keep at least visible_rows of data in view.
                // Without this, holding PageDown at the bottom pushes start_row past
                // num_rows, which makes scroll_would_trigger_collect fire repeatedly
                // and can leave busy stuck if the resulting collect is a no-op.
                unclamped.min(self.num_rows.saturating_sub(self.visible_rows))
            } else {
                unclamped
            }
        };

        if new_start_row == self.start_row {
            return false;
        }

        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        self.start_row = new_start_row;

        if within_buffer {
            if self.table_state.selected().is_none() {
                self.table_state.select(Some(0));
            }
            false
        } else {
            true // caller must collect
        }
    }

    pub fn collect(&mut self) {
        if self.defer_collect {
            return;
        }
        // Update proximity threshold based on visible rows
        if self.visible_rows > 0 {
            self.proximity_threshold = self.proximity();
        }

        // Run len() only when lf has changed (query, filter, sort, pivot, melt, reset, drill).
        if !self.num_rows_valid {
            self.num_rows = match collect_lazy(row_count_lf(&self.lf), self.polars_streaming) {
                Ok(df) => {
                    // The frame counts, so there is nothing wrong with it: retire a
                    // failure left by the frame this one replaced. `load_buffer` ends
                    // the same way, but the zero-row path below returns before it.
                    self.error = None;
                    match df.get(0) {
                        Some(col) => match col.first() {
                            Some(AnyValue::UInt64(len)) => *len as usize,
                            _ => 0,
                        },
                        _ => 0,
                    }
                }
                // A count that fails means the frame itself is broken — a sort or a
                // column order naming a column the query removed, say. Zero rows is the
                // wrong thing to report: it blanks the table and returns below, before
                // `load_buffer`, the only other place that records a failure. The caller
                // is then told nothing, so a broken frame reads as an empty one. Say what
                // went wrong instead.
                Err(e) => {
                    self.error = Some(e);
                    0
                }
            };
            self.num_rows_valid = true;
            self.remember_pristine_count();
        }

        if self.num_rows > 0 {
            let max_start = self.num_rows.saturating_sub(1);
            if self.start_row > max_start {
                self.start_row = max_start;
            }
        } else {
            self.start_row = 0;
            self.drop_buffer();
            self.df = None;
            self.locked_df = None;
            return;
        }

        // Proximity-based buffer logic
        let view_start = self.start_row;
        let view_end = self.start_row + self.visible_rows.min(self.num_rows - self.start_row);

        // Check if current view is within buffered range
        let within_buffer = view_start >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        // Buffer grows incrementally: initial load and each expansion add only a few pages (lookahead + lookback).
        // fit_window caps at max_buffered_rows and slides the window when at cap.

        if within_buffer {
            let dist_to_start = view_start.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);

            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.buffered_end_row < self.num_rows;

            if !needs_expansion_back && !needs_expansion_forward {
                // Column scroll only: reuse cached full buffer and re-slice into locked/scroll columns.
                let expected_len = self
                    .buffered_end_row
                    .saturating_sub(self.buffered_start_row);
                if self
                    .buffered_df
                    .as_ref()
                    .is_some_and(|b| b.height() == expected_len)
                {
                    self.slice_buffer_into_display();
                    if self.table_state.selected().is_none() {
                        self.table_state.select(Some(0));
                    }
                    return;
                }
                self.load_buffer(self.buffered_start_row, self.buffered_end_row);
                if self.table_state.selected().is_none() {
                    self.table_state.select(Some(0));
                }
                return;
            }

            let mut new_buffer_start = if needs_expansion_back {
                view_start.saturating_sub(self.reach_rows(self.pages_lookback))
            } else {
                self.buffered_start_row
            };

            let mut new_buffer_end = if needs_expansion_forward {
                (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows)
            } else {
                self.buffered_end_row
            };

            self.fit_window(
                view_start,
                view_end,
                &mut new_buffer_start,
                &mut new_buffer_end,
            );
            if self.holds_buffer(new_buffer_start, new_buffer_end) {
                // Fitting the expansion gave back the row group already held.
                self.slice_buffer_into_display();
                if self.table_state.selected().is_none() {
                    self.table_state.select(Some(0));
                }
                return;
            }
            self.load_buffer(new_buffer_start, new_buffer_end);
        } else {
            // Outside buffer: either extend the previous buffer (so it grows) or load a fresh small window.
            // Only extend when the view is "close" to the existing buffer (e.g. user paged down a bit).
            // A big jump (e.g. jump to end) should load just a window around the new view, not extend
            // the buffer across the whole dataset.
            let mut new_buffer_start;
            let mut new_buffer_end;

            let had_buffer = self.buffered_end_row > 0;
            let scrolled_past_end = had_buffer && view_start >= self.buffered_end_row;
            let scrolled_past_start = had_buffer && view_end <= self.buffered_start_row;

            let extend_forward_ok = scrolled_past_end
                && (view_start - self.buffered_end_row) <= self.reach_rows(self.pages_lookahead);
            let extend_backward_ok = scrolled_past_start
                && (self.buffered_start_row - view_end) <= self.reach_rows(self.pages_lookback);

            if extend_forward_ok {
                // View is just a few pages past buffer end; extend forward.
                new_buffer_start = self.buffered_start_row;
                new_buffer_end =
                    (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows);
            } else if extend_backward_ok {
                // View is just a few pages before buffer start; extend backward.
                new_buffer_start = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                new_buffer_end = self.buffered_end_row;
            } else if scrolled_past_end || scrolled_past_start {
                // Big jump (e.g. jump to end or jump to start): load a fresh window around the view.
                new_buffer_start = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                new_buffer_end =
                    (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows);
                let min_initial_len = self.min_buffer_len();
                let current_len = new_buffer_end.saturating_sub(new_buffer_start);
                if current_len < min_initial_len {
                    let need = min_initial_len.saturating_sub(current_len);
                    let can_extend_end = self.num_rows.saturating_sub(new_buffer_end);
                    let can_extend_start = new_buffer_start;
                    if can_extend_end >= need {
                        new_buffer_end = (new_buffer_end + need).min(self.num_rows);
                    } else if can_extend_start >= need {
                        new_buffer_start = new_buffer_start.saturating_sub(need);
                    } else {
                        new_buffer_end = (new_buffer_end + can_extend_end).min(self.num_rows);
                        new_buffer_start =
                            new_buffer_start.saturating_sub(need.saturating_sub(can_extend_end));
                    }
                }
            } else {
                // No buffer yet or big jump: load a fresh small window (view ± a few pages).
                new_buffer_start = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                new_buffer_end =
                    (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows);

                // Ensure at least (1 + lookahead + lookback) pages so buffer size is consistent (e.g. 364 at 52 visible).
                let min_initial_len = self.min_buffer_len();
                let current_len = new_buffer_end.saturating_sub(new_buffer_start);
                if current_len < min_initial_len {
                    let need = min_initial_len.saturating_sub(current_len);
                    let can_extend_end = self.num_rows.saturating_sub(new_buffer_end);
                    let can_extend_start = new_buffer_start;
                    if can_extend_end >= need {
                        new_buffer_end = (new_buffer_end + need).min(self.num_rows);
                    } else if can_extend_start >= need {
                        new_buffer_start = new_buffer_start.saturating_sub(need);
                    } else {
                        new_buffer_end = (new_buffer_end + can_extend_end).min(self.num_rows);
                        new_buffer_start =
                            new_buffer_start.saturating_sub(need.saturating_sub(can_extend_end));
                    }
                }
            }

            self.fit_window(
                view_start,
                view_end,
                &mut new_buffer_start,
                &mut new_buffer_end,
            );
            self.load_buffer(new_buffer_start, new_buffer_end);
        }

        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
    }

    /// Column expressions for every column in `column_order`, with binary columns replaced by a
    /// stub literal ([`binary_stub`]) so their blobs are never read. Used both for the display
    /// buffer (keeps scroll/jump collects fast) and for analysis (describe/distribution/
    /// correlation), where reading multi-GB blobs across partitions would otherwise exhaust
    /// memory and freeze the process. The full bytes stay available through `lf` for export.
    pub(crate) fn binary_stub_exprs(&self) -> Vec<Expr> {
        self.column_order
            .iter()
            .map(|name| {
                if matches!(self.schema.get(name.as_str()), Some(DataType::Binary)) {
                    lit(binary_stub()).alias(name.as_str())
                } else {
                    col(name.as_str())
                }
            })
            .collect()
    }

    /// Plan an async collect without blocking: clamp the start row, then a
    /// `CollectRequest` when the rows on screen need a new buffer load, or `None` when
    /// the buffer already holds them (the display slices are updated then).
    /// `num_rows_override`, when given, is the row count first; without a count the
    /// plan uses [`Self::num_rows_bound`], so the first rows need not wait for one.
    pub fn prepare_async_collect(
        &mut self,
        num_rows_override: Option<usize>,
    ) -> Option<CollectRequest> {
        if self.visible_rows > 0 {
            self.proximity_threshold = self.proximity();
        }

        if let Some(n) = num_rows_override {
            self.num_rows = n;
            self.num_rows_valid = true;
        }

        // `bound` is the exact total when known, or `usize::MAX` while the background
        // `len()` is still running. Using it instead of `self.num_rows` lets us plan a
        // top-of-data window for first paint without waiting for the count. See
        // `num_rows_bound`.
        let count_known = self.num_rows_valid;
        let bound = self.num_rows_bound();

        if count_known {
            if self.num_rows > 0 {
                let max_start = self.num_rows.saturating_sub(1);
                if self.start_row > max_start {
                    self.start_row = max_start;
                }
            } else {
                // Confirmed-empty dataset: clear everything.
                self.start_row = 0;
                self.drop_buffer();
                self.df = None;
                self.locked_df = None;
                return None;
            }
        }

        let view_start = self.start_row;
        let view_end = self.start_row + self.visible_rows.min(bound - self.start_row);
        let within_buffer = view_start >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        // Compute the buffer range using the same logic as collect().
        let (new_buffer_start, new_buffer_end) = if within_buffer {
            let dist_to_start = view_start.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);
            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.buffered_end_row < bound;

            if !needs_expansion_back && !needs_expansion_forward {
                // Buffer is fine, just re-slice display.
                (self.buffered_start_row, self.buffered_end_row)
            } else {
                let mut s = if needs_expansion_back {
                    view_start.saturating_sub(self.reach_rows(self.pages_lookback))
                } else {
                    self.buffered_start_row
                };
                let mut e = if needs_expansion_forward {
                    (view_end + self.reach_rows(self.pages_lookahead)).min(bound)
                } else {
                    self.buffered_end_row
                };
                self.fit_window(view_start, view_end, &mut s, &mut e);
                (s, e)
            }
        } else {
            let had_buffer = self.buffered_end_row > 0;
            let scrolled_past_end = had_buffer && view_start >= self.buffered_end_row;
            let scrolled_past_start = had_buffer && view_end <= self.buffered_start_row;
            let extend_forward_ok = scrolled_past_end
                && (view_start - self.buffered_end_row) <= self.reach_rows(self.pages_lookahead);
            let extend_backward_ok = scrolled_past_start
                && (self.buffered_start_row - view_end) <= self.reach_rows(self.pages_lookback);

            let mut s;
            let mut e;
            if extend_forward_ok {
                s = self.buffered_start_row;
                e = (view_end + self.reach_rows(self.pages_lookahead)).min(bound);
            } else if extend_backward_ok {
                s = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                e = self.buffered_end_row;
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

        // When the count isn't known yet, `num_rows` is provisional (the planned end of
        // this buffer). `apply_async_collect` keeps `num_rows_valid` false so the
        // background `len()` corrects it, unless the short read reveals the true end.
        let num_rows = if count_known {
            self.num_rows
        } else {
            new_buffer_end
        };
        Some(CollectRequest {
            lf,
            polars_streaming: self.polars_streaming,
            buffer_start: new_buffer_start,
            buffer_end: new_buffer_end,
            num_rows,
            count_known,
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
            .then(|| self.buffered_df.clone())
            .flatten()
            .map(|df| (df, self.buffered_start_row));
        FillPlan {
            buffer_start,
            buffer_end,
            num_rows,
            count_known,
            indexing: self.indexing().is_some(),
            held,
            view_start: self.start_row,
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
            self.num_rows = num_rows;
            self.num_rows_valid = true;
        } else if returned_rows < requested_rows
            && (buffer_start == 0 || returned_rows > 0)
            // Lines still being indexed end where the indexing has got to, not the file.
            && !indexing
            && self.indexing().is_none()
        {
            // Short read: the slice ran off the end, so we now know the exact total
            // without waiting for the background len() count. A slice deep in the
            // frame that found nothing may lie past the data entirely; only the count
            // can say where it ends.
            self.num_rows = buffer_start + returned_rows;
            self.num_rows_valid = true;
        } else if !self.num_rows_valid {
            // Full buffer with the count still unresolved: render with a provisional
            // total (at least this buffer's end) and leave num_rows_valid false so the
            // in-flight background len() corrects it via count_landed().
            self.num_rows = self.num_rows.max(buffer_end);
        }
        // else: the background len() already resolved the exact count between this
        // buffer being requested and applied — keep it; don't downgrade to provisional.
        self.error = None;
        self.remember_pristine_count();

        if bytes_per_row.is_some() {
            self.observed_bytes_per_row = bytes_per_row;
        }
        // A fill that does not hold the view's first row was planned for rows since
        // replaced (a synchronous collect re-planned while it was out, or the view
        // jumped past what the cut kept): installing it would draw rows under the wrong
        // numbers. Keep what is held and plan again. A fill that holds the first row
        // but not the whole view (the terminal grew while it was out) is kept, and the
        // rest fetched; a downloaded row group is too costly to throw away for a resize.
        // A read that came back short ends the data, so a view past it is shown by the
        // rows kept up to that end, and only by them: a cut may have dropped the end.
        let end = start + df.height();
        let view_end = self.start_row + self.visible_rows.max(1);
        let reaches_end = end >= buffer_start + returned_rows;
        let shows_view = start <= self.start_row
            && (self.start_row < end || (returned_rows < requested_rows && reaches_end));
        if !shows_view {
            self.needs_recollect = true;
            return;
        }
        self.release_display_buffer();
        self.buffered_start_row = start;
        self.buffered_end_row = end;
        self.buffered_df = Some(df);
        // Slice the buffered DataFrame into display DataFrames (locked + scroll columns).
        self.slice_buffer_into_display();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
        if view_end > end && end < self.num_rows {
            self.needs_recollect = true;
        }
    }

    /// True when `rows` rows fetched from `start` run on from the rows on hand or up to
    /// them, so a fill of them is planned to be stitched on (see [`FillPlan`]).
    fn abuts_buffer(&self, start: usize, rows: usize) -> bool {
        self.stitches_buffer()
            && (start == self.buffered_end_row || start + rows == self.buffered_start_row)
    }

    /// Invalidate num_rows cache when lf is mutated. Takes a fresh `len_generation` so any
    /// in-flight background count for the previous `lf` is recognized as stale. Also drops
    /// the cheap Parquet-footer count source: once `lf` carries a filter/query/group, the
    /// row count no longer equals the sum of file footers.
    ///
    /// A view of `sample`, drawn from `source` into `rows`, whose rows have the
    /// columns of `schema`. It starts empty and takes rows with
    /// [`Self::sample_grew`]. `through` when the sample was drawn from the view's
    /// query or filters, rather than the source under them.
    pub(crate) fn sampled_from(
        source: DataTableState,
        sample: crate::sampling::Sample,
        schema: &Schema,
        rows: Arc<crate::table_sample::SampleRows>,
        through: bool,
        path: Option<crate::table_sample::DrawPath>,
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

    /// Take the chunks the draw kept since the last call: every frame reads them, so
    /// the query, filters and sort run over them too. `None` when there were none;
    /// otherwise whether the rows on hand still stand. The view stays where it is,
    /// and the rows on hand stand while nothing reorders them, since the new rows
    /// come after them.
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
    pub(crate) fn sample_drawn(&mut self, drawn: crate::table_sample::Drawn) {
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
            && self.sort_columns.is_empty()
            && self.sort_ascending
            && self.scan_is_the_root();
        let rows = frame.height();
        self.each_frame(|lf| crate::table_sample::rebind(&mut lf.logical_plan, &old, &frame));
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

    /// Bytes a row of a sample of this view takes: of the source's columns when it
    /// is drawn from the source, of the view's when from the view, every column of
    /// either, shown or not.
    pub(crate) fn sample_row_bytes(&self, from_source: bool) -> usize {
        let schema = if from_source {
            &self.original_schema
        } else {
            &self.schema
        };
        let columns: Vec<String> = schema
            .iter_names()
            .filter(|name| name.as_str() != crate::schema_union::DRIFT_COLUMN)
            .map(|name| name.to_string())
            .collect();
        // What the table measured, when it measured these columns.
        if !from_source && columns.len() == self.column_order.len() {
            return self.bytes_per_row();
        }
        estimate_bytes_per_row(schema, &columns, &self.column_bytes)
    }

    /// The frame for buffer rows `[start, start + len)`, columns in display order. For a
    /// remote dataset whose files are counted, a scan of only the files holding them.
    /// How many of the dataset's files a page at `start` would read.
    ///
    /// A windowed remote scan reads only the files holding those rows; everything else
    /// hands the whole scan to Polars, which reads what it decides to and does not say.
    /// `None` is that second case — not zero, which would claim a page came from
    /// nowhere.
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
            all_columns.push(col(crate::schema_union::DRIFT_COLUMN));
        }
        self.window_lf(start, len, all_columns)
    }

    /// Whether the frame's rows carry their place in the source, for `#`: a dataset's
    /// rows that know their file, or lines, while the frame is still the scan's. A
    /// query's rows, a reshape's and a group's stand for no row of the source.
    pub(crate) fn carries_source_rows(&self) -> bool {
        self.drift_column_present
            || (self.scan_is_the_root() && (self.source_rows_at_open || self.view_numbered))
    }

    /// What `#` shows for `rows` rows from `start`: each row's place in the source
    /// where the rows carry it, else its place in the view, counted from
    /// `row_start_index`. A pristine view's places are the source's either way.
    pub fn row_numbers_from(&self, start: usize, rows: usize) -> Vec<usize> {
        let view = |i: usize| start + i + self.row_start_index;
        let places = self
            .buffered_df
            .as_ref()
            .filter(|_| self.carries_source_rows())
            .and_then(|df| df.column(crate::schema_union::DRIFT_COLUMN).ok())
            .and_then(|column| {
                let offset = start.checked_sub(self.buffered_start_row)?;
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
            &self.lf,
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
            lf: self.lf.clone(),
            files: self.files_window().cloned(),
            // A find reads every row it can reach: lines still being indexed are read
            // through the frame, which waits for them, not the window of those so far.
            records: self.window_now().filter(|_| self.indexing().is_none()),
            read_as_text: self.read_as_text.clone(),
            buffer: self
                .buffered_df
                .as_ref()
                .filter(|_| self.buffer_on_hand())
                .map(|df| (df.clone(), self.buffered_start_row)),
            num_rows: self.num_rows_valid.then_some(self.num_rows),
            streaming: self.polars_streaming,
            whole: sees_every_row_first(&self.lf),
            reads_up_to: reads_up_to_a_window(&self.lf),
        }
    }

    /// Put the cursor on view row `row`, centered, for a find that matched there.
    /// Returns true if a collect is needed. A row past a provisional total is one the
    /// find read, so the total reaches it until the count lands.
    pub(crate) fn go_to_found_row(&mut self, row: usize) -> bool {
        if !self.num_rows_valid && self.num_rows <= row {
            self.num_rows = row + 1;
        }
        self.scroll_to_row_centered(row)
    }

    /// The view row the cursor is on.
    pub(crate) fn cursor_row(&self) -> usize {
        self.start_row + self.table_state.selected().unwrap_or(0)
    }

    /// Bytes a buffered row takes: measured on the last buffer collected, or until
    /// then estimated from the schema.
    pub(super) fn bytes_per_row(&self) -> usize {
        self.observed_bytes_per_row.unwrap_or_else(|| {
            estimate_bytes_per_row(&self.schema, &self.column_order, &self.column_bytes)
        })
    }

    /// Best available in-memory width estimate for one logical row.
    ///
    /// Data Quality uses this only for a preflight estimate and labels the result as
    /// approximate. Buffer planning uses the same source so the two surfaces do not
    /// disagree about the shape of the current view.
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

    /// Rows the `max_buffered_mb` budget allows a buffer, never fewer than a screen;
    /// 0 for no budget. Planning to this, rather than trimming the collected frame to
    /// it, keeps a wide window from being materialized only to be cut down.
    pub(super) fn byte_cap_rows(&self) -> usize {
        if self.max_buffered_mb == 0 {
            return 0;
        }
        let max_bytes = self.max_buffered_mb * 1024 * 1024;
        (max_bytes / self.bytes_per_row()).max(self.visible_rows.max(1))
    }

    /// True while the buffer is planned as a remote window: a scan of an object store
    /// that nothing has been applied to. A query, filter, sort or reshape reads the
    /// object through a predicate, and `slice(0, N)` then stops at the first N matches,
    /// so the page-based window costs a row group where the remote one would read forty.
    pub(super) fn remote_window(&self) -> bool {
        self.remote_source && self.is_pristine()
    }

    /// The files a page reads by, while the frame is the scan as loaded: a filter or
    /// sort reads every file before its window, so it goes through the whole scan.
    fn files_window(&self) -> Option<&RemoteFiles> {
        self.remote_files.as_ref().filter(|_| self.is_pristine())
    }

    /// Rows the buffer reaches past the view in one direction: `pages` of it for a local
    /// file, a remote scan with something applied to it (see `remote_window`), or a
    /// remote dataset of many files; half the window for a pristine remote object
    /// (`fit_window` trims the two halves plus the view back to the cap).
    ///
    /// Many files are read a few at a time instead: what a read costs there is the
    /// files it opens, not its rows, and a wide window over a dataset of small files
    /// (a day of blocks in 2009 is a few rows) is hundreds of downloads.
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
        self.start_row == self.num_rows.saturating_sub(self.visible_rows)
    }

    /// Fit a planned buffer `[buffer_start, buffer_end)` to the caps: `max_buffered_rows`
    /// and the byte budget around the view, then for a remote object whose footer is
    /// known the row groups the view lies in, cut back to the caps inside them.
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
        // The caps hold inside a group too: a group over them is read one window at
        // a time, the window kept inside the group so it never pulls the next one
        // before the view reaches it.
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
            let (held_start, held_end) = (self.buffered_start_row, self.buffered_end_row);
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

    pub(super) fn load_buffer(&mut self, buffer_start: usize, buffer_end: usize) {
        let buffer_size = buffer_end.saturating_sub(buffer_start);
        if buffer_size == 0 {
            return;
        }

        let use_streaming = self.polars_streaming;
        let lf = match self.buffer_lf(buffer_start, buffer_size) {
            Ok(lf) => lf,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };
        let full_df = match collect_lazy(lf, use_streaming) {
            Ok(df) => df,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };

        // Stitched and cut as a background fill is, here on the spot, with the old
        // rows let go first: the plan has taken any it stitches on to.
        let plan = self.fill_plan(buffer_start, buffer_end, self.num_rows, self.num_rows_valid);
        self.release_display_buffer();
        let fitted = plan.fit(full_df);
        if fitted.bytes_per_row.is_some() {
            self.observed_bytes_per_row = fitted.bytes_per_row;
        }
        let full_df = fitted.df;
        let effective_buffer_start = fitted.start;
        let effective_buffer_end = fitted.start + full_df.height();

        if self.locked_columns_count > 0 {
            let locked_names: Vec<&str> = self
                .column_order
                .iter()
                .take(self.locked_columns_count)
                .map(|s| s.as_str())
                .collect();
            let locked_df = match full_df.select(locked_names) {
                Ok(df) => df,
                Err(e) => {
                    self.error = Some(e);
                    return;
                }
            };
            self.locked_df = Some(locked_df);
        } else {
            self.locked_df = None;
        }

        let scroll_names: Vec<&str> = self
            .column_order
            .iter()
            .skip(self.frozen_shown() + self.termcol_index)
            .map(|s| s.as_str())
            .collect();
        if scroll_names.is_empty() {
            self.df = None;
        } else {
            let scroll_df = match full_df.select(scroll_names) {
                Ok(df) => df,
                Err(e) => {
                    self.error = Some(e);
                    return;
                }
            };
            self.df = Some(scroll_df);
        }
        if self.error.is_some() {
            self.error = None;
        }
        self.buffered_start_row = effective_buffer_start;
        self.buffered_end_row = effective_buffer_end;
        self.buffered_df = Some(full_df);
    }

    /// Let go of the buffer being replaced and the display frames cut from it. A
    /// synchronous load does so before its cut, so the cut's copy is not made while
    /// the old rows are still held; a stitch has already taken the rows it keeps.
    /// The view's rows come next, so a relearn asked for takes effect.
    fn release_display_buffer(&mut self) {
        self.widths.rows_arrived();
        self.buffered_df = None;
        self.locked_df = None;
        self.df = None;
    }

    /// Recompute locked_df and df from the cached full buffer. Used when only termcol_index (or locked columns) changed.
    pub(super) fn slice_buffer_into_display(&mut self) {
        let full_df = match self.buffered_df.as_ref() {
            Some(df) => df,
            None => return,
        };

        if self.locked_columns_count > 0 {
            let locked_names: Vec<&str> = self
                .column_order
                .iter()
                .take(self.locked_columns_count)
                .map(|s| s.as_str())
                .collect();
            if let Ok(locked_df) = full_df.select(locked_names) {
                self.locked_df = Some(locked_df);
            }
        } else {
            self.locked_df = None;
        }

        let scroll_names: Vec<&str> = self
            .column_order
            .iter()
            .skip(self.frozen_shown() + self.termcol_index)
            .map(|s| s.as_str())
            .collect();
        if scroll_names.is_empty() {
            self.df = None;
        } else {
            if let Ok(scroll_df) = full_df.select(scroll_names) {
                self.df = Some(scroll_df);
            }
        }
    }

    /// Whether the view is inside the buffer and within a page of one of its ends, with
    /// more data past that end: where a collect would grow the buffer, if one ran. A
    /// scroll that stays inside the buffer runs none, so the growing waited until the
    /// view had left it — and the page was blank while it happened.
    pub fn wants_to_load_ahead(&self) -> bool {
        if self.visible_rows == 0
            || self.buffered_df.is_none()
            || !self.page_on_hand(self.start_row)
        {
            return false;
        }
        let near = self.proximity();
        let view_end = self.start_row
            + self
                .visible_rows
                .min(self.num_rows_bound().saturating_sub(self.start_row));
        let behind =
            self.start_row - self.buffered_start_row <= near && self.buffered_start_row > 0;
        let ahead = self.buffered_end_row - view_end <= near
            && self.buffered_end_row < self.num_rows_bound();
        behind || ahead
    }

    /// How close the view comes to an end of the buffer before the buffer grows past
    /// it: half the reach ahead, and never under a page. A page was the whole margin, and
    /// a cloud fetch takes longer than the next PageDown does to cross it.
    fn proximity(&self) -> usize {
        (self.reach_rows(self.pages_lookahead) / 2).max(self.visible_rows)
    }

    /// Where the view and the buffer are, to tell one load-ahead attempt from the next.
    pub fn buffer_position(&self) -> (u64, usize, usize, usize) {
        (
            self.len_generation(),
            self.start_row,
            self.buffered_start_row,
            self.buffered_end_row,
        )
    }

    /// Whether every row of the page starting at `start` is in the buffer.
    pub(crate) fn page_on_hand(&self, start: usize) -> bool {
        let bound = self.num_rows_bound();
        let end = start + self.visible_rows.min(bound.saturating_sub(start));
        self.buffered_df.is_some()
            && self.buffered_end_row > 0
            && start >= self.buffered_start_row
            && end <= self.buffered_end_row
    }

    /// The first row to draw: the view's own once its rows are on hand, and until then
    /// the last page that was drawn whole. The view moves the moment a key asks, before
    /// its rows are fetched, and drawn from there it was half a page of rows over half a
    /// page of nothing until the fetch landed.
    pub(crate) fn start_to_draw(&mut self) -> usize {
        if self.page_on_hand(self.start_row) {
            self.drawn_start = self.start_row;
            self.start_row
        } else if self.page_on_hand(self.drawn_start) {
            self.drawn_start
        } else {
            self.start_row
        }
    }
}
