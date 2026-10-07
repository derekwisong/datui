//! What is known of the dataset: its row count, footers, files, drift, notes and units,
//! and what of it a followed file has grown by.

use super::*;

/// Builds a scan of some of a dataset's files, as the full scan reads them, with the
/// columns named in the second argument read as text from every file rather than as
/// the type most rows have.
pub type FileScan = Arc<dyn Fn(&[String], &[PlSmallStr]) -> PolarsResult<LazyFrame> + Send + Sync>;

/// Counts the rows in each row group of every file of a dataset. Blocks.
pub type FileCounter = Arc<
    dyn Fn(&Arc<crate::schema_union::FooterProgress>) -> Result<Vec<Vec<usize>>, String>
        + Send
        + Sync,
>;

/// Reads every footer of a dataset that opened from a couple of them, and returns what
/// they say. `None` when they could not be read, in which case the dataset stays as it
/// opened. Blocks, and counts itself off against the progress it is given.
pub type FootersJoin =
    Arc<dyn Fn(&Arc<crate::schema_union::FooterProgress>) -> Option<FootersFound> + Send + Sync>;

/// What reading every footer turned up, and everything built from it that the dataset
/// has to be given together — the schema and the scans that read at that schema.
pub struct FootersFound {
    /// Every column every file has, and which files disagree about what.
    pub dataset: crate::schema_union::DatasetSchema,
    /// The scan that reads the dataset whole.
    pub lf: LazyFrame,
    /// Each file's rows, in scan order.
    pub file_rows: Vec<usize>,
    /// Every file listed, in scan order — including any whose footer would not read.
    /// The dataset's per-file findings index this, so it is the whole list.
    pub files: Vec<String>,
    /// Each file's row groups, or empty if a footer would not parse.
    pub row_groups: Vec<Vec<usize>>,
    /// How to read part of a remote dataset rather than all of it. `None` for one that
    /// does not read by file.
    pub remote: Option<RemoteRead>,
    /// The row count the footers read say, when they were a sample.
    pub estimate: Option<crate::schema_union::RowEstimate>,
}

/// How a remote dataset reads some of its files, as the pass behind an open found them.
///
/// The three travel together because they describe one list. The scan is built at a
/// schema — the one the dataset opened with has never heard of the columns this pass
/// found — and the counter answers one entry per file it was given, which has to be the
/// same list `urls` holds or the answer is dropped on a length check and the dataset
/// never learns its own size.
pub struct RemoteRead {
    /// The files that will open, which is not every file listed.
    pub urls: Vec<String>,
    pub scan: FileScan,
    pub count: FileCounter,
}

impl From<RemoteRead> for RemoteFiles {
    fn from(read: RemoteRead) -> Self {
        RemoteFiles {
            urls: Arc::new(read.urls),
            scan: read.scan,
            count: read.count,
            offsets: None,
        }
    }
}

/// A dataset of many files, and how to read only some of them: a remote one, or a
/// local Hive directory once every footer is known.
///
/// Polars reads a scan of many files in order: row 900,000 is reached by reading every
/// file before it, and a count is a read of all of them. Once each file's rows are
/// known, from its footer, a buffer is a scan of just the files holding its rows.
#[derive(Clone)]
pub struct RemoteFiles {
    /// Every file, in scan order.
    pub urls: Arc<Vec<String>>,
    pub scan: FileScan,
    pub count: FileCounter,
    /// Where each file's rows start, with the total last. Known once counted.
    pub offsets: Option<Vec<usize>>,
}

/// The `[start, end)` row ranges of the files flagged in `conflicts`, merged where
/// they touch.
///
/// `starts[i]` is where file `i`'s rows begin in the dataset and `total` is how many
/// rows the dataset has, so the last file's end is known without a start after it.
///
/// Runs, not files: a vendor who wrote a column as text for a month wrote a
/// contiguous stretch of files, and the predicate built from this is one term per run
/// however many files the stretch holds. Merging changes no row's fate — three
/// touching ranges keep out exactly what one joined range does — which is why it is
/// pinned here, where the runs themselves can be counted, rather than by a test of
/// what ends up on screen.
///
/// Post-conditions, for any `starts` ascending and `conflicts` of the same length:
/// - a row is in some run exactly when the file it belongs to is flagged;
/// - the runs are ascending and no two of them touch or overlap;
/// - a file of no rows produces no run of its own, and never splits one.
pub(super) fn conflicting_row_runs(
    starts: &[usize],
    total: usize,
    conflicts: &[bool],
) -> Vec<(usize, usize)> {
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for (file, start) in starts.iter().copied().enumerate() {
        if !conflicts.get(file).copied().unwrap_or(false) {
            continue;
        }
        let end = starts.get(file + 1).copied().unwrap_or(total);
        match runs.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => runs.push((start, end)),
        }
    }
    // A file of no rows leaves an empty range, which keeps no row out and would make
    // the "no two touch" post-condition depend on which files happen to be empty.
    runs.retain(|(start, end)| start < end);
    runs
}

impl DataTableState {
    /// Draws from the shared counter rather than incrementing, so a mutation here can
    /// never land on the value a later dataset is about to be seeded with.
    pub(crate) fn invalidate_num_rows(&mut self) {
        self.num_rows_valid = false;
        self.len_generation = next_len_generation();
    }

    /// True while `lf` is the data as loaded: no sidebar filter or sort, no query in
    /// any bar, no pivot or melt, no drill-down. Derived rather than kept, so clearing
    /// the filters or un-sorting makes the frame pristine again by itself.
    /// Whether the table shows other rows than its source holds: a filter, a query, a
    /// reshape or a drill. A sort alone reorders the same rows.
    pub(crate) fn changes_rows(&self) -> bool {
        !self.filters.is_empty()
            || !self.active_query.is_empty()
            || !self.active_sql_query.is_empty()
            || !self.active_fuzzy_query.is_empty()
            || self.reshaped_lf.is_some()
            || self.grouped.is_some()
            || self.drilled_down_group_index.is_some()
    }

    /// Whether the view may still take its rows straight from the scan: nothing
    /// that picks rows (a filter, a search, a reshape, a group, a drill). A query
    /// may only choose columns, so it may.
    pub(crate) fn may_keep_scan_rows(&self) -> bool {
        self.filters.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.reshaped_lf.is_none()
            && self.grouped.is_none()
            && self.drilled_down_group_index.is_none()
    }

    pub(super) fn is_pristine(&self) -> bool {
        self.column_changes.is_empty()
            && self.filters.is_empty()
            && self.sort_columns.is_empty()
            && self.sort_ascending
            && self.active_query.is_empty()
            && self.active_sql_query.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.reshaped_lf.is_none()
            && self.grouped.is_none()
            && self.drilled_down_group_index.is_none()
    }

    /// Whether the frame on screen still grows from the dataset's own scan.
    ///
    /// A filter and a sort do: `apply_transformations` rebuilds them over whatever the
    /// root is, so widening the root under them is exactly what should happen. A query,
    /// a SQL statement, a fuzzy search, a pivot, a melt and a drill-down do not — each
    /// makes its own result the root, with its own columns, and replacing the root
    /// underneath one leaves the view naming columns the frame no longer has.
    ///
    /// `grouped` and `drilled_down_group_index` are set together by a drill down and
    /// cleared together by a drill up, so asking both is belt and braces — kept because
    /// what they guard is the frame being rebuilt under a view of one group of it.
    /// The frame an analysis reads: the view as filtered and queried, without its
    /// order. No statistic depends on the order, and a sort is the one step that makes
    /// a sampled read of a huge table read all of it.
    pub fn analysis_lf(&self) -> LazyFrame {
        self.unsorted_lf.clone().unwrap_or_else(|| self.lf.clone())
    }

    /// What the Pivot & Melt builder previews a few rows of: the view as the user
    /// sees its columns, without its order. A sort would make the head of a large
    /// table a read of all of it.
    pub fn preview_lf(&self) -> LazyFrame {
        Self::without_drift(self.analysis_lf())
    }

    /// Whether the view has a sort, which [`Self::preview_lf`] leaves out.
    pub fn is_sorted(&self) -> bool {
        self.unsorted_lf.is_some()
    }

    pub fn scan_is_the_root(&self) -> bool {
        self.active_query.is_empty()
            && self.active_sql_query.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.reshaped_lf.is_none()
            && self.grouped.is_none()
            && self.drilled_down_group_index.is_none()
    }

    /// A pristine scan's count is its footer's: take it back, without a `len()`, when
    /// the frame is the scan as loaded again.
    pub(super) fn restore_footer_count(&mut self) {
        if !self.is_pristine() {
            return;
        }
        if let Some(total) = self.row_group_offsets.as_ref().and_then(|o| o.last()) {
            self.set_num_rows(*total);
        }
    }

    /// What finding and reading this dataset cost.
    pub fn measurements(&self) -> &Arc<crate::measurements::Meter> {
        &self.measurements
    }

    /// The directory whose Parquet footers can be summed for an exact row count, if the
    /// current `lf` still allows it. See `parquet_count_dir`.
    pub fn parquet_count_dir(&self) -> Option<PathBuf> {
        self.parquet_count_dir
            .clone()
            .filter(|_| self.is_pristine())
    }

    /// Current count generation. A background `len()` task captures this; its result is
    /// only applied if the generation still matches (i.e. the data hasn't changed since).
    pub fn len_generation(&self) -> u64 {
        self.len_generation
    }

    /// Returns the cached row count when valid (same value shown in the control bar). Use this to
    /// avoid an extra full scan for analysis/describe when the table has already been collected.
    pub fn num_rows_if_valid(&self) -> Option<usize> {
        if self.num_rows_valid {
            Some(self.num_rows)
        } else {
            None
        }
    }

    /// True when num_rows reflects the current `lf`. Used by App to decide whether
    /// to dispatch a background len() query before planning the buffer collect.
    pub fn is_num_rows_valid(&self) -> bool {
        self.num_rows_valid
    }

    /// Effective upper bound on row indices for buffer planning. When the exact count
    /// is known, that's `num_rows`; when it isn't yet (first paint before the background
    /// `len()` resolves), treat the dataset as unbounded so we plan a top-of-data window
    /// (`slice(0, N)`) instead of clamping everything to a stale/zero count.
    pub(super) fn num_rows_bound(&self) -> usize {
        if self.num_rows_valid {
            self.num_rows
        } else {
            usize::MAX
        }
    }

    /// Apply a row count computed in the background (so prepare_async_collect doesn't
    /// have to fall back to a blocking len() on the UI thread).
    pub(super) fn set_num_rows(&mut self, n: usize) {
        self.num_rows = n;
        self.num_rows_valid = true;
        self.remember_pristine_count();
        // A view past the end of a frame that turned out smaller comes back to it.
        if self.start_row > 0 && self.start_row >= n {
            self.start_row = n.saturating_sub(self.visible_rows);
            self.needs_recollect = true;
        }
    }

    /// Keep the pristine frame's count for the control bar's "417 of 1,000". Only a
    /// count already resolved for the data as loaded — never a reason to run one.
    pub(super) fn remember_pristine_count(&mut self) {
        if self.num_rows_valid && self.error.is_none() && self.is_pristine() {
            self.pristine_rows = Some(self.num_rows);
        }
    }

    /// The dataset's full row count for the control bar, when the rows on screen are a
    /// subset of it: a sidebar filter, a query in any bar or a drill-down is active and
    /// the count from before it was applied is known. A pivot or melt makes rows that
    /// are not the dataset's, so the comparison would mislead and none is offered.
    /// Cheap by construction: it only reads what a pristine collect already knew.
    pub fn total_rows_when_subset(&self) -> Option<usize> {
        let subsetting = !self.filters.is_empty()
            || !self.active_query.is_empty()
            || !self.active_sql_query.is_empty()
            || !self.active_fuzzy_query.is_empty()
            || self.drilled_down_group_index.is_some();
        if subsetting && self.reshaped_lf.is_none() {
            self.pristine_rows
        } else {
            None
        }
    }

    /// Clone of the LazyFrame for off-thread queries (e.g. background len()).
    pub fn lf_clone(&self) -> LazyFrame {
        self.lf.clone()
    }

    /// Whether the current LazyFrame should use Polars streaming engine.
    pub fn polars_streaming_enabled(&self) -> bool {
        self.polars_streaming
    }

    /// True when a fill that runs on from the rows on hand, or up to them, will be
    /// stitched on to them rather than replace them. See [`FillPlan`].
    pub(crate) fn stitches_buffer(&self) -> bool {
        self.remote_window() && self.buffer_on_hand()
    }

    /// The rows on hand and the view row the first of them is, when every row of
    /// the buffered range is: what a find lights up as it is typed, without a read.
    pub(crate) fn rows_on_hand(&self) -> Option<(&DataFrame, usize)> {
        self.buffered_df
            .as_ref()
            .filter(|_| self.buffer_on_hand())
            .map(|df| (df, self.buffered_start_row))
    }

    /// True when every row of the buffered range is on hand.
    pub(crate) fn buffer_on_hand(&self) -> bool {
        self.buffered_end_row > self.buffered_start_row
            && self
                .buffered_df
                .as_ref()
                .is_some_and(|b| b.height() == self.buffered_end_row - self.buffered_start_row)
    }

    /// True when the rows on hand include `[start, end)`. The buffer is then cut down
    /// to that range, so a row group stitched on to cross into it is let go once the
    /// view has left it, rather than fetched again when the view comes back.
    pub(super) fn holds_buffer(&mut self, start: usize, end: usize) -> bool {
        if !self.buffer_on_hand()
            || start < self.buffered_start_row
            || end > self.buffered_end_row
            || end <= start
        {
            return false;
        }
        if (start, end) != (self.buffered_start_row, self.buffered_end_row) {
            let offset = start - self.buffered_start_row;
            // Trimmed so the rows let go are freed rather than kept behind a slice; the
            // display frames alias the old buffer and go with it.
            self.locked_df = None;
            self.df = None;
            self.buffered_df = self
                .buffered_df
                .take()
                .map(|b| trim_rows(b, offset, end - start, None));
            self.buffered_start_row = start;
            self.buffered_end_row = end;
        }
        true
    }

    /// Start row of the currently buffered range.
    pub fn buffered_start(&self) -> usize {
        self.buffered_start_row
    }

    /// End row (exclusive) of the currently buffered range.
    pub fn buffered_end(&self) -> usize {
        self.buffered_end_row
    }

    /// True for a scan of an object store in place.
    ///
    /// Polars fetches a Parquet row group whole for any slice that touches it and keeps
    /// nothing between collects, so the small, proximity-driven refills that suit a
    /// local file each download the same row group again: paging through one row group
    /// cost a fetch of it every few pages. A remote buffer is planned as a single window
    /// of `max_buffered_rows` around the view instead. Scrolling inside it costs
    /// nothing; leaving it, or a jump, costs one fetch.
    pub fn is_remote_source(&self) -> bool {
        self.remote_source
    }

    /// The schema of the data as loaded, before any query or reshape: what a view's
    /// settings run on, and so what its schema rule records and matches.
    pub fn source_schema(&self) -> &Arc<Schema> {
        &self.original_schema
    }

    /// Record the row groups of a remote Parquet object, `rows` in each, so a buffer
    /// fill is planned as whole groups (see `align_to_row_groups`). Also the row count.
    pub(super) fn record_row_groups(&mut self, rows: &[usize]) {
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for n in rows {
            offsets.push(offsets.last().unwrap_or(&0) + n);
        }
        self.set_num_rows(*offsets.last().unwrap_or(&0));
        self.row_group_offsets = Some(offsets);
    }

    /// Whether a Data Quality run over `scope` reads every row and every byte-bearing
    /// column of the source: the case where a copy of the whole objects costs no more
    /// than one of its passes. A filter or a hidden column may let a pass read less
    /// than the objects, and a binary column is never read at all.
    pub(crate) fn quality_reads_whole_source(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> bool {
        use crate::data_quality::QualityScope;
        let columns = || {
            self.original_schema
                .iter()
                .filter(|(name, _)| name.as_str() != crate::schema_union::DRIFT_COLUMN)
        };
        if columns().any(|(_, dtype)| matches!(dtype, DataType::Binary)) {
            return false;
        }
        match scope {
            QualityScope::WholeSource => true,
            QualityScope::CurrentView => {
                let shown = self
                    .column_order
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>();
                !self.changes_rows() && columns().all(|(name, _)| shown.contains(name.as_str()))
            }
            _ => false,
        }
    }

    /// Each remote object the dataset reads, in scan order: every file of a remote
    /// dataset, or the one object. `None` in place of one the open did not size.
    pub(crate) fn each_remote_object(
        &self,
    ) -> Option<Box<dyn Iterator<Item = Option<&RemoteObject>> + '_>> {
        let objects = self.remote_objects.as_ref()?;
        Some(match &self.remote_files {
            Some(remote) => Box::new(remote.urls.iter().map(|url| objects.get(url))),
            None => Box::new(objects.values().map(Some)),
        })
    }

    /// The remote objects this dataset reads. `None` when any is unknown.
    pub(crate) fn remote_objects(&self) -> Option<Vec<RemoteObject>> {
        let objects = self
            .each_remote_object()?
            .map(|object| object.cloned())
            .collect::<Option<Vec<_>>>()?;
        (!objects.is_empty()).then_some(objects)
    }

    /// The bytes and count of [`Self::remote_objects`], without copying them out:
    /// Setup asks on every frame.
    pub(crate) fn remote_objects_size(&self) -> Option<(u64, usize)> {
        let (bytes, count) = self
            .each_remote_object()?
            .try_fold((0u64, 0usize), |(bytes, count), object| {
                object.map(|object| (bytes + object.size, count + 1))
            })?;
        (count > 0).then_some((bytes, count))
    }

    /// Record what the footers said about the dataset's columns. See `DatasetSchema`.
    /// `file_rows` is each file's row count, in scan order, and empty when they are not
    /// all known — the same condition under which the scan numbers its rows.
    pub(super) fn record_dataset_schema(
        &mut self,
        schema: crate::schema_union::DatasetSchema,
        file_rows: &[usize],
        files: &[String],
    ) {
        self.drift_files = files.to_vec();
        // The scan numbers rows exactly when the files differ and every one is counted.
        self.drift_column_present = schema.drifts() && file_rows.len() == schema.file_group.len();
        self.drift_groups = Arc::new(schema.groups.clone());
        self.drift_file_group = schema.file_group.clone();
        self.drift_file_starts = Vec::with_capacity(file_rows.len());
        let mut row = 0usize;
        for rows in file_rows {
            self.drift_file_starts.push(row);
            row += rows;
        }
        self.drift_dataset_rows = row;
        self.drift_at_open = self.drift_column_present;
        self.groups_at_open = self.drift_groups.clone();
        self.notes = Self::notes_datui_can_act_on(&schema, self.drift_column_present);
        self.notes_at_open = self.notes.clone();
        self.notes_seen = false;
        self.read_as_text = Vec::new();
        self.dataset_at_open = Some(schema.clone());
        self.dataset_schema = Some(schema);
    }

    /// The pass that is still reading this dataset's footers, if one is.
    pub fn footers_pending(&self) -> Option<FootersJoin> {
        self.footers_pending.clone()
    }

    /// Whether *this frame's* row count is already on its way.
    ///
    /// The frame matters: what the pass is bringing is the dataset's count, which is
    /// not the count of a query's result. Asking this about the wrong frame is how a
    /// count nobody else was going to take gets declined.
    ///
    /// A staged open is still reading every footer, and those footers hold the count.
    /// Asking for it separately would read all of them a second time, so the dataset
    /// says it will have one shortly and the caller does not start a count of its own.
    pub fn counts_itself_later(&self) -> bool {
        // Lines still being indexed: any frame's count is of the lines so far, and the
        // indexing is bringing the rest.
        if self.indexing().is_some() && !self.num_rows_valid {
            return true;
        }
        // Only while it does not have one, and only while the frame is the scan. What
        // the pass is bringing is the *dataset's* count; a query's result has a count
        // of its own that nobody else is going to take. Declining it there means the
        // row count spins for as long as the query is open and `End` says it is
        // counting while nothing is — and it costs nothing to take, because a frame
        // that is not the scan does not read footers for it either.
        self.footers_pending.is_some() && !self.num_rows_valid && self.is_pristine()
    }

    /// The lines being indexed behind the first rows, if they still are.
    pub fn indexing(&self) -> Option<&Arc<crate::lines::Lines>> {
        // Asked of the lines, so a dataset set aside while they finished (the quality
        // evidence view) does not wait for them for good.
        self.indexing.as_ref().filter(|lines| lines.indexing())
    }

    /// The lines this dataset opened from in part, until it has been told they are all
    /// in, though their indexing is paused: what an indexing thread works on.
    pub fn lines_to_index(&self) -> Option<&Arc<crate::lines::Lines>> {
        self.indexing.as_ref()
    }

    /// The dataset's row count from a sample of its footers, while the frame is the
    /// dataset as loaded and its count is not known. `pass` is the estimate of the
    /// footer pass still reading, which the dataset has not been given yet.
    pub fn row_estimate(
        &self,
        pass: Option<crate::schema_union::RowEstimate>,
    ) -> Option<crate::schema_union::RowEstimate> {
        if self.num_rows_valid || !self.is_pristine() {
            return None;
        }
        self.row_estimate
            .or_else(|| pass.filter(|_| self.footers_pending.is_some()))
    }

    /// Where each of the dataset's files starts in the view, with the total last: while
    /// the view keeps the dataset's rows and every file's rows are known.
    pub fn file_row_starts(&self) -> Option<Vec<usize>> {
        if self.changes_rows() {
            return None;
        }
        self.remote_files.as_ref()?.offsets.clone()
    }

    /// How many files a count of the dataset reads footers of, when it reads them.
    pub fn files_to_count(&self) -> Option<usize> {
        self.remote_files
            .as_ref()
            .filter(|f| f.offsets.is_none())
            .map(|f| f.urls.len())
    }

    /// Whether `#` is on for this dataset when the config leaves it to the format:
    /// text and logs, whose rows carry their place in the file.
    pub fn numbered_by_default(&self) -> bool {
        matches!(
            self.read_as,
            Some(crate::FileFormat::Text | crate::FileFormat::Journal)
        )
    }

    /// Whether `#` is on and numbers the rows by their place in the view, because
    /// the view's rows do not carry their place in the source: a sorted or filtered
    /// view of data in a store, of many files, or too large to number.
    pub fn row_numbers_count_the_view(&self) -> bool {
        self.row_numbers
            && !self.carries_source_rows()
            && self.scan_is_the_root()
            && (!self.filters.is_empty() || !self.sort_columns.is_empty() || !self.sort_ascending)
    }

    /// Every line is indexed, `rows` of them: the count of the lines in order, and the
    /// notes that say what the whole file holds. The frames already read every line
    /// (their height waits for the indexing), so nothing read through them is stale.
    /// Returns whether the dataset was waiting for them.
    pub(crate) fn lines_indexed(&mut self, rows: usize) -> bool {
        let Some(lines) = self.indexing.take() else {
            return false;
        };
        let notes = crate::lines::notes(&lines, self.indexing_guessed);
        let opened = std::mem::take(&mut self.indexing_notes);
        self.open_notes.retain(|n| !opened.contains(n));
        self.open_notes.extend(notes);
        // A file that shrank has no count to give: the lines so far are not all of it.
        if lines.shrank() {
            self.open_notes.push(crate::text_formats::note(
                crate::lines::SHRANK.to_string(),
                "the file".to_string(),
            ));
            return true;
        }
        // The "of" in `417 of 1,000` under a filter.
        self.pristine_rows = Some(rows);
        if self.is_pristine() {
            self.set_num_rows(rows);
        }
        true
    }

    /// Give up on the rest of the footers: the pass could not read them.
    ///
    /// The dataset stays as it opened — a working view of it, built from two footers —
    /// and stops waiting. That matters beyond the columns: while a pass is pending the
    /// dataset declines to count itself, because the pass was going to bring the count
    /// with it. One failed pass would otherwise cost it an exact row count, and its
    /// windowed reads, for the rest of the session.
    pub fn give_up_on_pending_footers(&mut self) {
        self.footers_pending = None;
    }

    /// Every footer's answer, joined to the dataset already on screen.
    ///
    /// The open painted from the first file and the newest; this is what the rest of
    /// them say. Columns only ever join: a name the opening schema did not have goes on
    /// the end, and every name already there keeps its place — including the places a
    /// user has since moved them to — so nothing moves under the cursor except to make
    /// room for what arrived.
    ///
    /// The frame is rebuilt rather than widened in place, because the scan itself
    /// differs: it now knows which files hold a column in a type the dataset cannot
    /// keep, and with every file's row count it can number the rows, which is what
    /// tells an absent cell from a null.
    ///
    /// Gives them back as `Err` rather than taking them, while the user is looking at
    /// something built on top of the scan instead of the scan itself — see
    /// [`Self::scan_is_the_root`]. Rebuilding the root under a query takes away the
    /// columns the query named; handing them back lets the caller keep them and offer
    /// them again when the view comes back to the data.
    pub fn join_dataset_schema(
        &mut self,
        mut found: FootersFound,
    ) -> std::result::Result<(), Box<FootersFound>> {
        if !self.scan_is_the_root() {
            // The columns must wait; what the footers said about the files need not.
            // `record_file_row_groups` keeps the offsets without touching the count of a
            // frame that is a query's result rather than the dataset — so letting the
            // query go gets the total back without going and fetching it.
            // Taken, not borrowed: this runs on every event for as long as the view
            // stays off the scan, and applying the same row groups on each keystroke is
            // a walk of every file in the dataset for nothing.
            let row_groups = std::mem::take(&mut found.row_groups);
            if !row_groups.is_empty() {
                self.record_file_row_groups(&row_groups);
            }
            // Boxed because what comes back is most of a dataset's worth of schema, and
            // an `Err` that size would be carried by every call that succeeds too.
            return Err(Box::new(found));
        }
        let FootersFound {
            dataset,
            lf,
            file_rows,
            files,
            row_groups,
            remote,
            estimate,
        } = found;
        self.row_estimate = if row_groups.is_empty() {
            estimate
        } else {
            None
        };
        let (file_rows, files) = (file_rows.as_slice(), files.as_slice());
        let known: std::collections::HashSet<&str> =
            self.column_order.iter().map(String::as_str).collect();
        let joining: Vec<String> = dataset
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .filter(|name| {
                name != crate::schema_union::DRIFT_COLUMN && !known.contains(name.as_str())
            })
            .collect();
        drop(known);
        self.column_order.extend(joining);
        // Columns only join — but a name can still go, if the footer that was the only
        // evidence for it would not parse this time round. Every read projects
        // `column_order`, so a name the new schema does not have is not a missing
        // column on screen, it is a scan that cannot run at all.
        self.column_order
            .retain(|name| dataset.schema.contains(name.as_str()));
        let schema = dataset.schema.clone();
        // The scan is built at a schema, and the one this dataset opened with has never
        // heard of the columns that just arrived. Left in place, the first windowed
        // page read asks it for a column it does not have and the table stops showing
        // rows at the moment it was supposed to show more of them.
        match (remote, self.remote_files.as_mut()) {
            (Some(found), Some(remote)) => {
                remote.urls = Arc::new(found.urls);
                remote.scan = found.scan;
                // The counter too, and for the same reason the scan is replaced: it
                // answers one entry per file it was given, and the one the dataset
                // opened with was given every file listed. Left beside a shorter `urls`
                // its answer is dropped on a length check without a word, and the
                // dataset spends the rest of the session re-counting itself and never
                // reaching an end to jump to.
                remote.count = found.count;
            }
            // A local directory reads by file only once every footer is known.
            (Some(found), None) => self.remote_files = Some(found.into()),
            (None, _) => {}
        }
        // Takes the notes, the drift groups and the row starts with it, and clears
        // `read_as_text` — sound only because the offer to read a column as text is
        // not made until the footers are all in, so there is nothing to clear.
        self.record_dataset_schema(dataset, file_rows, files);
        self.footers_pending = None;
        // The rows on screen were read through the old frame. Dropping the buffer has
        // the next collect read them through the new one, at the row the user is still
        // sitting on — `start_row` and the column scroll are left exactly as they are.
        self.replace_root(lf, schema);
        // The joined scan may hold rows the two-footer open never saw, so the count
        // remembered for the narrow root no longer describes the dataset.
        self.pristine_rows = None;
        // Measured on the frame that just went. A dataset that opened two columns wide
        // and gained thirty would plan its first page after the join from the two-column
        // width, which against a bucket is a read many times the budget the user set.
        self.observed_bytes_per_row = None;
        // Every file's row groups are known now, so this is the dataset's count. Set
        // before the rebuild so the count is in place the moment the frame is, rather
        // than for any ordering the lines below depend on.
        if !row_groups.is_empty() {
            self.record_file_row_groups(&row_groups);
        }
        // Rebuilt but not read. This runs on the thread drawing the screen, and
        // `apply_transformations` ends in a `collect` — against a dataset in a bucket
        // that is a page fetched, and with no count yet it is a `len()` over every file
        // in the dataset, which is the whole cost this staging exists to avoid. The
        // buffer is gone and the caller reads it back off the event loop. This line is
        // what keeps the collect off this thread; do not take it away.
        self.deferred(Self::apply_transformations);
        Ok(())
    }

    /// The frame as the user sees it: `lf` without the hidden row-index column.
    ///
    /// Everything that exports, reshapes, groups or analyses the data reads this. The
    /// buffer reads `lf` itself and keeps the column, which is how `display_drift`
    /// traces a row back to its file; it stays invisible because the display is only
    /// ever a projection of `column_order`, which never names it.
    pub fn visible_lf(&self) -> LazyFrame {
        Self::without_drift(self.lf.clone())
    }

    /// Whether the frame scans a temporary file this state holds (a decompressed
    /// archive). A view captured at exit must not reference it: the file is removed
    /// when the last state holding it drops, and the plan would scan a path that no
    /// longer exists.
    pub fn scans_a_temp_file(&self) -> bool {
        self.decompress_temp_file.is_some() || !self.converted.is_empty()
    }

    /// Whether the frame scans a downloaded remote file, removed when datui lets go of
    /// it; see [`Self::scans_a_temp_file`].
    pub fn scans_a_download(&self) -> bool {
        self.download.is_some()
    }

    /// How the open reads the data, when an open found it; `None` for a frame handed
    /// in whole, such as one from Python.
    pub fn read_mode(&self) -> Option<crate::ReadMode> {
        self.read_mode
    }

    /// The format the open read the data as. See [`OpenFacts::read_as`].
    pub fn read_as(&self) -> Option<crate::FileFormat> {
        self.read_as
    }

    /// Whether the data was downloaded from a remote source before it was read.
    pub fn fetched(&self) -> bool {
        self.fetched
    }

    /// The temporary files this state holds, which an error from reading it may name:
    /// a decompressed copy, a download.
    pub(crate) fn temp_files(&self) -> Vec<&Path> {
        let files = self.decompress_temp_file.iter().map(|file| file.path());
        let files = files.chain(self.download.iter().map(|download| download.path()));
        let files = files.chain(self.converted.iter().map(|file| file.path()));
        files.collect()
    }

    /// `lf` without the hidden drift column. A non-strict drop, so it is a no-op on a
    /// frame that never had one and no caller has to know which it holds.
    pub(super) fn without_drift(lf: LazyFrame) -> LazyFrame {
        lf.drop(by_name([crate::schema_union::DRIFT_COLUMN], false, false))
    }

    /// The frame a query, a SQL statement or a fuzzy search builds on. Never carries
    /// the drift column: a query's rows are its own, and its schema becomes the
    /// column order, so the column would otherwise become one of the data's.
    pub(super) fn query_source(&self) -> LazyFrame {
        Self::without_drift(self.original_lf.clone())
    }

    /// Whether rows still know which file they came from.
    pub fn drifts(&self) -> bool {
        self.drift_column_present
    }

    /// What each drift group is missing, for the renderer. Empty when nothing drifts.
    pub fn drift_groups(&self) -> Arc<Vec<crate::schema_union::DriftGroup>> {
        self.drift_groups.clone()
    }

    /// Whether an export can name each row's file: the frame has to still carry the
    /// scan's row index, and the dataset has to have files to name.
    pub fn can_name_source_files(&self) -> bool {
        self.drift_column_present
            && !self.drift_files.is_empty()
            && self.drift_files.len() == self.drift_file_starts.len()
    }

    /// The rows an export writes, planned and not run: the view, and when
    /// `name_files` asks and the dataset can, a column naming each row's file in
    /// place of the scan's hidden row index. Otherwise the index is dropped, so
    /// datui's own bookkeeping never lands in the user's file.
    pub fn export_frame(&self, name_files: bool) -> ExportFrame {
        if name_files && self.can_name_source_files() {
            ExportFrame {
                lf: self.lf.clone(),
                files: Some(SourceFiles {
                    names: Arc::new(self.drift_files.clone()),
                    starts: Arc::new(self.drift_file_starts.clone()),
                }),
            }
        } else {
            ExportFrame {
                lf: self.visible_lf(),
                files: None,
            }
        }
    }

    /// What datui noticed: about the dataset when it opened, then about the view the
    /// filter and sort have made of it. Empty when there is nothing to say.
    pub fn notes(&self) -> Vec<crate::notes::Note> {
        // What the read did first, because it is the frame everything below is about:
        // a directory read as CSV with a JSON file left out, or a lake table read as its
        // plain files, changes what every other note is a note about. `merged` rather
        // than a plain chain, because the open and the footer walk each count the
        // files a mixed directory's read passed over, and this is the one place both
        // tallies are in hand.
        let mut notes = crate::notes::merged(
            &self.open_notes,
            &self.notes,
            &self.view_notes,
            self.dataset_schema.as_ref(),
        );
        // What reading a table in place has found, which may grow after the open.
        if let Some(pushdown) = &self.pushdown {
            notes.extend(pushdown.notes());
        }
        notes.extend(self.unfit_notes.iter().flatten().cloned());
        notes.extend(self.changes_dropped.iter().cloned());
        if let Some((version, unfit)) = &self.changes_unfit
            && *version == self.changes_version
        {
            notes.extend(unfit.iter().cloned());
        }
        notes
    }

    /// The source a full quality run over `scope` reads every row of, for the checks
    /// that read a source whole (an audio file's signal): the records of the data as
    /// loaded, or a view with nothing applied.
    pub(crate) fn window_for_quality(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> Option<Arc<dyn crate::pushdown::Windowed>> {
        use crate::data_quality::QualityScope;
        matches!(scope, QualityScope::WholeSource | QualityScope::CurrentView)
            .then(|| {
                self.fixed_window
                    .clone()
                    .filter(|_| self.is_pristine() && self.indexing().is_none())
            })
            .flatten()
    }

    /// The lake format whose plain files this dataset is, if it is one.
    ///
    /// For the chip in the control bar. The note says the same at length; this is what
    /// keeps the row count from reading as the table's.
    pub fn not_the_table(&self) -> Option<&'static str> {
        self.not_the_table
    }

    /// The file's other tables, as `--table` names them; empty for a file of one.
    pub fn other_tables(&self) -> &[String] {
        &self.other_tables
    }

    /// What a read through a format spec found, when the dataset was read through one.
    pub fn format_read(&self) -> Option<&Arc<crate::formats::Read>> {
        self.format_read.as_ref()
    }

    /// The source a window of the view is read straight from, when there is one: the
    /// records or audio frames of the data as loaded, or the view a source runs itself.
    pub(super) fn window_now(&self) -> Option<Arc<dyn crate::pushdown::Windowed>> {
        if let Some(window) = self.follow_window() {
            return Some(Arc::new(window));
        }
        if let Some(records) = self.fixed_window.as_ref().filter(|_| self.is_pristine()) {
            return Some(records.clone());
        }
        self.pushed_view().map(|view| view.window)
    }

    /// The view as the source runs it, when it runs it: the data as loaded is the root
    /// (no query, reshape or drill), and the source can say the sidebar's filters and
    /// sort. Derived from them each time, so it never disagrees with them.
    pub(crate) fn pushed_view(&self) -> Option<crate::pushdown::PushedView> {
        let pushdown = self.pushdown.as_ref()?;
        if !self.scan_is_the_root() || self.drift_column_present {
            return None;
        }
        let sort: Vec<(String, bool)> = self
            .sort_columns
            .iter()
            .cloned()
            .zip(self.sort_descending.iter().copied())
            .collect();
        pushdown.view(&self.filters, &sort, !self.sort_ascending)
    }

    /// The view's own count, from a source that runs the view, or for a followed file
    /// whose view is known up to a row, that count and the rows after it.
    pub(crate) fn source_counter(&self) -> Option<crate::pushdown::Counter> {
        if let Some(counter) = self.follow_counter() {
            return Some(counter);
        }
        self.pushed_view().map(|view| view.counter)
    }

    /// The points where a followed view's rows are known, when they hold for the view
    /// on screen.
    fn follow_known(&self) -> Option<&[(usize, usize)]> {
        self.follow_known
            .as_ref()
            .filter(|(generation, _)| *generation == self.len_generation)
            .map(|(_, known)| known.as_slice())
    }

    /// A followed file's windows, read from the mark before each: the rows as they are,
    /// or filtered with no sort, read on from where the view's rows are known.
    fn follow_window(&self) -> Option<crate::follow::Window> {
        let follow = self.follow.as_ref()?;
        let known = if self.is_pristine() {
            None
        } else if self.scan_is_the_root() && self.sort_columns.is_empty() && self.sort_ascending {
            Some(self.follow_known()?.to_vec())
        } else {
            return None;
        };
        Some(crate::follow::Window {
            lf: self.lf.clone(),
            path: follow.path().to_path_buf(),
            marks: follow.marks().clone(),
            known,
        })
    }

    /// The count of a followed view known up to a file row: what was known, and the
    /// rows of the view among those after it, read from the mark before them.
    fn follow_counter(&self) -> Option<crate::pushdown::Counter> {
        let follow = self.follow.as_ref()?;
        let &(before, row) = self.follow_known()?.last()?;
        let rest = crate::follow::from_marks(&self.lf, follow.path(), follow.marks(), row, None)?;
        let streaming = self.polars_streaming;
        Some(Arc::new(move || {
            let df = crate::statistics::collect_lazy(row_count_lf(&rest), streaming)?;
            let after = match df.get(0).and_then(|row| row.first().cloned()) {
                Some(AnyValue::UInt64(n)) => n as usize,
                _ => 0,
            };
            Ok(before + after)
        }))
    }

    /// What a read through a delimited spec found, when the dataset was read through
    /// one.
    pub fn delimited_read(&self) -> Option<&Arc<crate::delimited_spec::DelimitedRead>> {
        self.delimited.as_ref()
    }

    /// The unit of the column named `column`, from a delimited spec's unit row or the
    /// file itself. A filter, sort, drill or query that keeps the loaded column, renamed
    /// or not, keeps its unit; a column a query computes has none, whatever it is called.
    pub fn unit_of(&self, column: &str) -> Option<&str> {
        if self.delimited.is_none() && self.file_units.is_empty() {
            return None;
        }
        let loaded = match &self.lineage {
            None => column,
            Some(lineage) => lineage
                .iter()
                .find(|(shown, _)| shown == column)
                .map(|(_, loaded)| loaded.as_str())?,
        };
        match &self.delimited {
            Some(read) => read.unit_of(loaded),
            None => self
                .file_units
                .iter()
                .find(|(name, _)| name == loaded)
                .map(|(_, unit)| unit.as_str()),
        }
    }

    /// Each column of the view that has a unit, with it.
    pub fn units(&self) -> Vec<(String, String)> {
        if self.delimited.is_none() && self.file_units.is_empty() {
            return Vec::new();
        }
        self.schema
            .iter_names()
            .filter_map(|name| Some((name.to_string(), self.unit_of(name)?.to_string())))
            .collect()
    }

    /// What the file said besides its rows: its Info panel tab.
    pub fn format_detail(&self) -> Option<&crate::text_formats::Detail> {
        self.detail.as_deref()
    }

    /// The Info panel tab of a followed pipe's journal, read again once it has ended
    /// and its rows are all on hand: the frame that reads every entry. `None` for any
    /// other dataset, or when it has been asked for already.
    pub(crate) fn ended_journal_to_describe(&mut self) -> Option<LazyFrame> {
        let follow = self.follow.as_mut()?;
        if follow.described
            || follow.live()
            || follow.behind()
            || follow.spool().is_none()
            || self.read_as != Some(crate::FileFormat::Journal)
        {
            return None;
        }
        follow.described = true;
        Some(self.original_lf.clone())
    }

    pub(crate) fn set_format_detail(&mut self, detail: crate::text_formats::Detail) {
        self.detail = Some(Arc::new(detail));
    }

    /// Whether datui noticed anything at all. Answers what `notes()` is usually asked
    /// — whether to offer the tab — without building the list to find out.
    pub fn has_notes(&self) -> bool {
        // A view note needs a column the files disagree on, and such a column always
        // draws a note of its own when the dataset opens. So the view half can never
        // be the only half, and the Notes tab does not appear and disappear as the
        // user sorts.
        //
        // The open's own notes count: a directory read as one format with another left
        // out may have nothing else worth saying, and that is exactly the dataset whose
        // reader the user most wants to know about.
        !self.notes.is_empty()
            || !self.open_notes.is_empty()
            || self.unfit_notes.as_ref().is_some_and(|n| !n.is_empty())
            || !self.changes_dropped.is_empty()
            || self
                .changes_unfit
                .as_ref()
                .is_some_and(|(v, n)| *v == self.changes_version && !n.is_empty())
            || self
                .pushdown
                .as_ref()
                .is_some_and(|p| !p.notes().is_empty())
    }

    /// Whether there is something to say that has not been offered yet.
    pub fn notes_unseen(&self) -> bool {
        self.has_notes() && !self.notes_seen
    }

    /// The rows a filter or sort on `column` has to leave out: every row of every file
    /// that holds the column in a type it is not read in.
    fn unread_row_runs(&self, column: &str) -> Vec<(usize, usize)> {
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let conflicts: Vec<bool> = (0..self.drift_file_starts.len())
            .map(|file| {
                dataset
                    .file_group
                    .get(file)
                    .and_then(|group| dataset.groups.get(*group as usize))
                    .is_some_and(|group| group.unread.iter().any(|name| name == column))
            })
            .collect();
        conflicting_row_runs(&self.drift_file_starts, self.drift_dataset_rows, &conflicts)
    }

    /// Columns the filter or sort names that some file holds in another type, in the
    /// dataset's own column order and each named once however many times the view
    /// mentions it.
    fn view_columns_with_conflicts(&self) -> Vec<crate::schema_union::ColumnDrift> {
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let named: HashSet<&str> = self
            .filters
            .iter()
            .map(|filter| filter.column.as_str())
            .chain(self.sort_columns.iter().map(String::as_str))
            .collect();
        dataset
            .columns
            .iter()
            .filter(|column| column.conflicting_files > 0 && named.contains(column.name.as_str()))
            .cloned()
            .collect()
    }

    /// Leave out the rows whose files do not hold a filtered or sorted column in the
    /// type it is read as, and say how many.
    ///
    /// Those rows read as null in that column, and a null is not a value the column
    /// can be compared or ordered by: a filter drops them already, and a sort would
    /// otherwise gather them at one end as though they belonged there. They are left
    /// out of both, and the note says how many so the smaller count is never a
    /// surprise.
    pub(super) fn view_exclusions(&self) -> Vec<(Vec<(usize, usize)>, crate::notes::Note)> {
        if !self.drift_column_present {
            return Vec::new();
        }
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for column in self.view_columns_with_conflicts() {
            let runs = self.unread_row_runs(&column.name);
            let rows: usize = runs.iter().map(|(start, end)| end - start).sum();
            if rows == 0 {
                continue;
            }
            let filtered = self
                .filters
                .iter()
                .any(|filter| filter.column.as_str() == column.name.as_str());
            let sorted = self
                .sort_columns
                .iter()
                .any(|sorted| sorted.as_str() == column.name.as_str());
            out.push((
                runs,
                crate::notes::left_out_note(&column, dataset, rows, filtered, sorted),
            ));
        }
        out
    }

    /// The notes for what the filter and sort on screen leave out, for a caller that
    /// is putting a frame back that already leaves those rows out rather than building
    /// one. Derived, never stored across a change of view: a note that outlives the
    /// sort that earned it is the fault this is shaped to avoid.
    pub(super) fn view_notes_only(&self) -> Vec<crate::notes::Note> {
        self.view_exclusions()
            .into_iter()
            .map(|(_, note)| note)
            .collect()
    }

    pub(super) fn leave_out_unread_rows(
        &self,
        mut lf: LazyFrame,
    ) -> (LazyFrame, Vec<crate::notes::Note>) {
        let mut notes = Vec::new();
        for (runs, note) in self.view_exclusions() {
            let keep = runs
                .iter()
                .map(|(start, end)| {
                    col(crate::schema_union::DRIFT_COLUMN)
                        .lt(lit(*start as u32))
                        .or(col(crate::schema_union::DRIFT_COLUMN).gt_eq(lit(*end as u32)))
                })
                .reduce(Expr::and);
            if let Some(keep) = keep {
                lf = lf.filter(keep);
            }
            notes.push(note);
        }
        (lf, notes)
    }

    /// The Info panel has been opened; the quiet accent has done its job.
    pub fn mark_notes_seen(&mut self) {
        self.notes_seen = true;
    }

    /// Read `column` as text from every file, so the values a type conflict hid can be
    /// seen.
    ///
    /// Rebuilds the scan rather than re-opening the dataset: everything it needs is
    /// already here. The file list, each file's row count and — since the footers were
    /// read — the type each file holds each conflicting column in are all on hand, so
    /// this costs no directory listing, no footer read and no request. The view goes
    /// with it: a filter and sort in force are re-applied to the new frame.
    ///
    /// Returns whether anything happened. `false` for a column that is not on offer,
    /// which is what the panel only ever asks about, and for one already read this way.
    /// A scan that cannot be built is kept as the error showing, and returned.
    pub fn read_column_as_text(&mut self, column: &str) -> PolarsResult<bool> {
        let name = PlSmallStr::from(column);
        let Some(dataset) = self.dataset_at_open.clone() else {
            return Ok(false);
        };
        if !self.drift_column_present || self.read_as_text.contains(&name) {
            return Ok(false);
        }
        if !dataset
            .columns
            .iter()
            .any(|drift| drift.name == name && drift.can_read_as_text())
        {
            return Ok(false);
        }

        let mut as_text = self.read_as_text.clone();
        as_text.push(name);

        // Built from the dataset as its footers found it, never from the view below.
        // The view has the column as text and nothing conflicting, so it no longer
        // holds the one thing the scan needs: the type each file actually wrote.
        let drift =
            crate::schema_union::ScanDrift::new(&self.drift_files, &dataset, &self.file_rows());
        let scanned = match self.remote_files.as_ref() {
            Some(remote) => (remote.scan)(&remote.urls, &as_text),
            None => crate::schema_union::lenient_scan(
                &self.drift_files,
                dataset.schema.clone(),
                None,
                drift.as_ref(),
                &as_text,
            ),
        };
        let lf = match scanned {
            Ok(lf) => lf,
            Err(e) => {
                // Shown where any failed read is; the view is as it was.
                self.error = Some(e.clone());
                return Err(e);
            }
        };
        // The remote scan hoists inside its own closure, as it does for the frame the
        // dataset opened with; only the local branch has it left to do.
        let lf = if self.remote_files.is_some() {
            lf
        } else {
            crate::hoist_partition_columns(
                lf,
                &dataset.schema,
                self.partition_columns.as_deref().unwrap_or(&[]),
                drift.is_some(),
            )
        };

        let view = dataset.reading_as_text(&as_text);
        self.read_as_text = as_text;
        // `text_schema` keeps the columns in their places, so the order the user
        // arranged still names every one of them and still means what it did.
        let schema = view.schema.clone();
        self.drift_groups = Arc::new(view.groups.clone());
        self.groups_at_open = self.drift_groups.clone();
        self.notes = Self::notes_datui_can_act_on(&view, self.drift_column_present);
        self.notes_at_open = self.notes.clone();
        // One note went and another arrived, and the new one is about how the column
        // now compares — which matters most to a user who has a filter on it.
        self.notes_seen = false;
        self.dataset_schema = Some(view);
        self.replace_root(lf, schema);
        // Re-applies the filter and sort over the new frame, and with them the note
        // about what they leave out — which is one note shorter now.
        self.apply_transformations();
        Ok(true)
    }

    /// The dataset's notes, with the offer to read a column as text left on only where
    /// taking it would work.
    ///
    /// A note is written from the footers' schema, which says whether a column *could*
    /// be shown as text. Whether it can be read that way is a second question: the scan
    /// has to know where each file's rows begin, and it does not for a dataset too large
    /// to read every footer, or one where a footer would not parse. Those are the same
    /// datasets that cannot draw the marks. An offer the panel shows and the action
    /// then declines is worse than no offer, so it is taken off here rather than
    /// refused later.
    fn notes_datui_can_act_on(
        dataset: &crate::schema_union::DatasetSchema,
        counted: bool,
    ) -> Vec<crate::notes::Note> {
        let mut notes = crate::notes::from_dataset(dataset);
        if !counted {
            for note in &mut notes {
                note.read_as_text = None;
            }
        }
        notes
    }

    /// Each file's row count, as the footers gave them. The starts are kept rather than
    /// the counts, so this is their differences with the dataset's total closing the
    /// last one.
    pub(super) fn file_rows(&self) -> Vec<usize> {
        self.drift_file_starts
            .iter()
            .enumerate()
            .map(|(file, start)| {
                self.drift_file_starts
                    .get(file + 1)
                    .copied()
                    .unwrap_or(self.drift_dataset_rows)
                    .saturating_sub(*start)
            })
            .collect()
    }

    /// The columns being read as text rather than as the type most rows have.
    pub fn read_as_text(&self) -> &[PlSmallStr] {
        &self.read_as_text
    }

    /// What the footers said about the dataset's columns, when it is many files.
    pub fn dataset_schema(&self) -> Option<&crate::schema_union::DatasetSchema> {
        self.dataset_schema.as_ref()
    }

    /// The counter for a remote dataset's files, while its count would be the data's:
    /// the frame is the scan as loaded, and the files have not been counted yet.
    ///
    /// Not while a pass is already reading every footer of this dataset. That pass
    /// brings the row groups back with it, and this counter reads the same footers a
    /// second time — for a prefix of 6,541 files, 6,541 ranged reads to learn what is
    /// already on its way. Staging the open to save round trips and then spending them
    /// here would be worse than not staging it at all.
    pub fn remote_files_counter(&self) -> Option<FileCounter> {
        if self.footers_pending.is_some() {
            return None;
        }
        self.remote_files
            .as_ref()
            .filter(|f| f.offsets.is_none() && self.is_pristine())
            .map(|f| f.count.clone())
    }

    /// Record the rows in each row group of each file of a remote dataset: the total,
    /// the row groups a buffer is planned in, and which files hold which rows.
    ///
    /// A local dataset has no files to window over, and takes the total and the groups.
    pub(super) fn record_file_row_groups(&mut self, groups: &[Vec<usize>]) {
        if let Some(files) = self.remote_files.as_mut() {
            if groups.len() != files.urls.len() {
                return;
            }
            let mut offsets = Vec::with_capacity(groups.len() + 1);
            offsets.push(0);
            for file in groups {
                offsets.push(offsets.last().unwrap_or(&0) + file.iter().sum::<usize>());
            }
            files.offsets = Some(offsets);
        }
        let flat: Vec<usize> = groups.iter().flatten().copied().collect();
        if self.is_pristine() {
            self.record_row_groups(&flat);
        } else {
            // Kept for when the frame is the scan again (`restore_footer_count`).
            let mut row_offsets = Vec::with_capacity(flat.len() + 1);
            row_offsets.push(0);
            for n in &flat {
                row_offsets.push(row_offsets.last().unwrap_or(&0) + n);
            }
            self.row_group_offsets = Some(row_offsets);
        }
    }
}
