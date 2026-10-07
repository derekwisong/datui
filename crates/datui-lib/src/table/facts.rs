//! What is known of the dataset: its row count, footers, files, drift, notes and units,
//! and what of it a followed file has grown by.

use super::*;

/// Builds a scan of some of a dataset's files as the full scan reads them, the second
/// argument's columns read as text from every file instead of the majority type.
pub type FileScan = Arc<dyn Fn(&[String], &[PlSmallStr]) -> PolarsResult<LazyFrame> + Send + Sync>;

/// Counts the rows in each row group of every file of a dataset. Blocks.
pub type FileCounter = Arc<
    dyn Fn(&Arc<crate::schema_union::FooterProgress>) -> Result<Vec<Vec<usize>>, String>
        + Send
        + Sync,
>;

/// Reads every footer of a dataset opened from a couple of them; `None` when they
/// could not be read (the dataset stays as opened). Blocks, reporting to the progress.
pub type FootersJoin =
    Arc<dyn Fn(&Arc<crate::schema_union::FooterProgress>) -> Option<FootersFound> + Send + Sync>;

/// What reading every footer found, and what the dataset must be given with it: the
/// schema and the scans built at that schema.
pub struct FootersFound {
    /// Every column every file has, and which files disagree about what.
    pub dataset: crate::schema_union::DatasetSchema,
    /// The scan that reads the dataset whole.
    pub lf: LazyFrame,
    /// Each file's rows, in scan order.
    pub file_rows: Vec<usize>,
    /// Every file listed in scan order, unreadable footers included: per-file findings
    /// index this list.
    pub files: Vec<String>,
    /// Each file's row groups, or empty if a footer would not parse.
    pub row_groups: Vec<Vec<usize>>,
    /// How to read part of a remote dataset; `None` for one that does not read by file.
    pub remote: Option<RemoteRead>,
    /// The row count the footers read say, when they were a sample.
    pub estimate: Option<crate::schema_union::RowEstimate>,
}

/// How a remote dataset reads some of its files, as the pass behind an open found
/// them. Together because they describe one list: the scan is built at the new
/// schema, and the counter answers one entry per file, which must match `urls` or
/// its answer is dropped.
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

/// A dataset of many files and how to read only some: remote, or a local Hive
/// directory once every footer is known. Polars reads files in order (row 900,000
/// means reading every file before it); with each file's rows known, a buffer scans
/// just the files holding its rows.
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
/// they touch (`starts[i]` is file `i`'s first row; `total` closes the last). Runs
/// keep the predicate one term per contiguous stretch. Post-conditions, for ascending
/// `starts` and equal-length `conflicts`:
/// - a row is in some run exactly when its file is flagged;
/// - runs are ascending and no two touch or overlap;
/// - a file of no rows makes no run and never splits one.
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
    // Empty files leave empty ranges, which keep out nothing.
    runs.retain(|(start, end)| start < end);
    runs
}

impl DataTableState {
    /// Draws from the shared counter rather than incrementing, so a later dataset's seed
    /// is never reused.
    pub(crate) fn invalidate_num_rows(&mut self) {
        self.view.num_rows_valid = false;
        self.view.len_generation = next_len_generation();
    }

    /// Whether the table shows other rows than its source holds: a filter, query, reshape
    /// or drill. A sort alone reorders the same rows.
    pub(crate) fn changes_rows(&self) -> bool {
        !self.view.filters.is_empty()
            || !self.view.active_query.is_empty()
            || !self.view.active_sql_query.is_empty()
            || !self.view.active_fuzzy_query.is_empty()
            || self.view.reshaped_lf.is_some()
            || self.view.grouped.is_some()
            || self.view.drilled_down_group_index.is_some()
    }

    /// Whether the view may take its rows straight from the scan: nothing picks rows
    /// (filter, search, reshape, group, drill); a query may only choose columns.
    pub(crate) fn may_keep_scan_rows(&self) -> bool {
        self.view.filters.is_empty()
            && self.view.active_fuzzy_query.is_empty()
            && self.view.reshaped_lf.is_none()
            && self.view.grouped.is_none()
            && self.view.drilled_down_group_index.is_none()
    }

    /// Whether `lf` is the data as loaded: no filter, sort, query, reshape or drill.
    /// Derived, so clearing them makes the frame pristine again.
    pub(super) fn is_pristine(&self) -> bool {
        self.view.column_changes.is_empty()
            && self.view.filters.is_empty()
            && self.view.sort_columns.is_empty()
            && self.view.sort_ascending
            && self.view.active_query.is_empty()
            && self.view.active_sql_query.is_empty()
            && self.view.active_fuzzy_query.is_empty()
            && self.view.reshaped_lf.is_none()
            && self.view.grouped.is_none()
            && self.view.drilled_down_group_index.is_none()
    }

    /// The frame an analysis reads: the view as filtered and queried, unsorted (no
    /// statistic needs order, and a sort makes a sampled read read everything).
    pub fn analysis_lf(&self) -> LazyFrame {
        self.view
            .unsorted_lf
            .clone()
            .unwrap_or_else(|| self.view.lf.clone())
    }

    /// What the Pivot & Melt builder previews: the view's columns, unsorted (a sort would
    /// make the head of a large table a full read).
    pub fn preview_lf(&self) -> LazyFrame {
        Self::without_drift(self.analysis_lf())
    }

    /// Whether the view has a sort, which [`Self::preview_lf`] leaves out.
    pub fn is_sorted(&self) -> bool {
        self.view.unsorted_lf.is_some()
    }

    /// Whether the frame on screen still grows from the dataset's scan. Filters and sorts
    /// are rebuilt over any root, so they do; a query, SQL, fuzzy search, pivot, melt or
    /// drill-down makes its own result the root, and widening the scan under it would
    /// leave the view naming missing columns.
    pub fn scan_is_the_root(&self) -> bool {
        self.view.active_query.is_empty()
            && self.view.active_sql_query.is_empty()
            && self.view.active_fuzzy_query.is_empty()
            && self.view.reshaped_lf.is_none()
            && self.view.grouped.is_none()
            && self.view.drilled_down_group_index.is_none()
    }

    /// A pristine scan's count is its footer's: restore it without a `len()` when the
    /// frame is the scan as loaded again.
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

    /// The directory whose Parquet footers sum to an exact row count, if `lf` still
    /// allows it.
    pub fn parquet_count_dir(&self) -> Option<PathBuf> {
        self.parquet_count_dir
            .clone()
            .filter(|_| self.is_pristine())
    }

    /// Current count generation; a background `len()` result applies only if it still
    /// matches.
    pub fn len_generation(&self) -> u64 {
        self.view.len_generation
    }

    /// The cached row count when valid (the footer's), sparing analysis a full scan.
    pub fn num_rows_if_valid(&self) -> Option<usize> {
        if self.view.num_rows_valid {
            Some(self.view.num_rows)
        } else {
            None
        }
    }

    /// Whether num_rows reflects the current `lf`, deciding whether a background `len()`
    /// goes before the buffer collect.
    pub fn is_num_rows_valid(&self) -> bool {
        self.view.num_rows_valid
    }

    /// Upper bound on row indices for buffer planning: `num_rows` when known; else
    /// unbounded, so the first paint plans `slice(0, N)` rather than clamping to zero.
    pub(super) fn num_rows_bound(&self) -> usize {
        if self.view.num_rows_valid {
            self.view.num_rows
        } else {
            usize::MAX
        }
    }

    /// Apply a background row count, so planning never blocks on `len()`.
    pub(super) fn set_num_rows(&mut self, n: usize) {
        self.view.num_rows = n;
        self.view.num_rows_valid = true;
        self.remember_pristine_count();
        // A view past the end of a frame that turned out smaller comes back to it.
        if self.view.start_row > 0 && self.view.start_row >= n {
            self.view.start_row = n.saturating_sub(self.visible_rows);
            self.needs_recollect = true;
        }
    }

    /// Keep the pristine frame's count for the footer's "417 of 1,000", only when already
    /// known; never a reason to count.
    pub(super) fn remember_pristine_count(&mut self) {
        if self.view.num_rows_valid && self.error.is_none() && self.is_pristine() {
            self.pristine_rows = Some(self.view.num_rows);
        }
    }

    /// Clone of the LazyFrame for off-thread queries (e.g. background len()).
    pub fn lf_clone(&self) -> LazyFrame {
        self.view.lf.clone()
    }

    /// Whether the current LazyFrame should use Polars streaming engine.
    pub fn polars_streaming_enabled(&self) -> bool {
        self.polars_streaming
    }

    /// Whether a fill adjoining the rows on hand is stitched to them rather than replacing
    /// them. See [`FillPlan`].
    pub(crate) fn stitches_buffer(&self) -> bool {
        self.remote_window() && self.buffer_on_hand()
    }

    /// The rows on hand and the view row of the first, when the whole buffered range is
    /// held: what a find highlights as typed, without a read.
    pub(crate) fn rows_on_hand(&self) -> Option<(&DataFrame, usize)> {
        self.view
            .buffered_df
            .as_ref()
            .filter(|_| self.buffer_on_hand())
            .map(|df| (df, self.view.buffered_start_row))
    }

    /// True when every row of the buffered range is on hand.
    pub(crate) fn buffer_on_hand(&self) -> bool {
        self.view.buffered_end_row > self.view.buffered_start_row
            && self.view.buffered_df.as_ref().is_some_and(|b| {
                b.height() == self.view.buffered_end_row - self.view.buffered_start_row
            })
    }

    /// Whether the rows on hand include `[start, end)`; the buffer is then cut to that
    /// range, so a stitched row group is released once the view leaves it.
    pub(super) fn holds_buffer(&mut self, start: usize, end: usize) -> bool {
        if !self.buffer_on_hand()
            || start < self.view.buffered_start_row
            || end > self.view.buffered_end_row
            || end <= start
        {
            return false;
        }
        if (start, end) != (self.view.buffered_start_row, self.view.buffered_end_row) {
            let offset = start - self.view.buffered_start_row;
            // Trimmed so released rows are freed; display frames alias the old buffer and go
            // with it.
            self.view.locked_df = None;
            self.view.df = None;
            self.view.buffered_df = self
                .view
                .buffered_df
                .take()
                .map(|b| trim_rows(b, offset, end - start, None));
            self.view.buffered_start_row = start;
            self.view.buffered_end_row = end;
        }
        true
    }

    /// Start row of the currently buffered range.
    pub fn buffered_start(&self) -> usize {
        self.view.buffered_start_row
    }

    /// End row (exclusive) of the currently buffered range.
    pub fn buffered_end(&self) -> usize {
        self.view.buffered_end_row
    }

    /// True for a scan of an object store in place. Polars fetches a whole row group for
    /// any slice and caches nothing, so a remote buffer is one window of
    /// `max_buffered_rows` around the view: scrolling inside is free, leaving it or a
    /// jump costs one fetch.
    pub fn is_remote_source(&self) -> bool {
        self.remote_source
    }

    /// The schema as loaded, before queries or reshapes: what a view's settings run on
    /// and its schema rule matches.
    pub fn source_schema(&self) -> &Arc<Schema> {
        &self.original_schema
    }

    /// Record a remote Parquet object's row groups (`rows` each) so fills are planned in
    /// whole groups (`align_to_row_groups`); also the row count.
    pub(super) fn record_row_groups(&mut self, rows: &[usize]) {
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for n in rows {
            offsets.push(offsets.last().unwrap_or(&0) + n);
        }
        self.set_num_rows(*offsets.last().unwrap_or(&0));
        self.row_group_offsets = Some(offsets);
    }

    /// Whether a Data Quality run over `scope` reads every row and byte-bearing column of
    /// the source, so a whole-object copy costs no more than a pass. Filters or hidden
    /// columns may read less; binary columns are never read.
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
                    .view
                    .column_order
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>();
                !self.changes_rows() && columns().all(|(name, _)| shown.contains(name.as_str()))
            }
            _ => false,
        }
    }

    /// Each remote object the dataset reads, in scan order; `None` for one the open did
    /// not size.
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

    /// [`Self::remote_objects`]' bytes and count without copying (asked every frame).
    pub(crate) fn remote_objects_size(&self) -> Option<(u64, usize)> {
        let (bytes, count) = self
            .each_remote_object()?
            .try_fold((0u64, 0usize), |(bytes, count), object| {
                object.map(|object| (bytes + object.size, count + 1))
            })?;
        (count > 0).then_some((bytes, count))
    }

    /// Record what the footers said of the columns (see `DatasetSchema`). `file_rows` is
    /// each file's row count in scan order, empty unless all are known (when the scan
    /// numbers its rows).
    pub(super) fn record_dataset_schema(
        &mut self,
        schema: crate::schema_union::DatasetSchema,
        file_rows: &[usize],
        files: &[String],
    ) {
        self.drift_files = files.to_vec();
        // The scan numbers rows exactly when the files differ and every one is counted.
        self.view.drift_column_present =
            schema.drifts() && file_rows.len() == schema.file_group.len();
        self.view.drift_groups = Arc::new(schema.groups.clone());
        self.drift_file_group = schema.file_group.clone();
        self.drift_file_starts = Vec::with_capacity(file_rows.len());
        let mut row = 0usize;
        for rows in file_rows {
            self.drift_file_starts.push(row);
            row += rows;
        }
        self.drift_dataset_rows = row;
        self.drift_at_open = self.view.drift_column_present;
        self.groups_at_open = self.view.drift_groups.clone();
        self.view.notes = Self::notes_datui_can_act_on(&schema, self.view.drift_column_present);
        self.notes_at_open = self.view.notes.clone();
        self.view.notes_seen = false;
        self.read_as_text = Vec::new();
        self.dataset_at_open = Some(schema.clone());
        self.dataset_schema = Some(schema);
    }

    /// The pass that is still reading this dataset's footers, if one is.
    pub fn footers_pending(&self) -> Option<FootersJoin> {
        self.footers_pending.clone()
    }

    /// Whether this frame's row count is already coming: a staged open still reading
    /// footers (which hold the count) or line indexing in progress. The frame matters: a
    /// query's result has its own count nobody else will take.
    pub fn counts_itself_later(&self) -> bool {
        // Lines still being indexed: any count is of the lines so far; indexing brings the
        // rest.
        if self.indexing().is_some() && !self.view.num_rows_valid {
            return true;
        }
        // Only without a count and while the frame is the scan: the pass brings the
        // dataset's count, not a query's.
        self.footers_pending.is_some() && !self.view.num_rows_valid && self.is_pristine()
    }

    /// The lines being indexed behind the first rows, if they still are.
    pub fn indexing(&self) -> Option<&Arc<crate::lines::Lines>> {
        // Asked of the lines, so a dataset set aside meanwhile does not wait forever.
        self.indexing.as_ref().filter(|lines| lines.indexing())
    }

    /// The lines this dataset opened from in part, until told all are in (even while
    /// paused): what an indexing thread works on.
    pub fn lines_to_index(&self) -> Option<&Arc<crate::lines::Lines>> {
        self.indexing.as_ref()
    }

    /// The row count estimated from sampled footers, while the frame is the dataset as
    /// loaded and uncounted. `pass` is the still-running footer pass's estimate.
    pub fn row_estimate(
        &self,
        pass: Option<crate::schema_union::RowEstimate>,
    ) -> Option<crate::schema_union::RowEstimate> {
        if self.view.num_rows_valid || !self.is_pristine() {
            return None;
        }
        self.row_estimate
            .or_else(|| pass.filter(|_| self.footers_pending.is_some()))
    }

    /// Where each file starts in the view, the total last: while the view keeps the
    /// dataset's rows and every file's rows are known.
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

    /// Whether `#` defaults on for this format: text and logs, whose rows have a place in
    /// the file.
    pub fn numbered_by_default(&self) -> bool {
        matches!(
            self.read_as,
            Some(crate::FileFormat::Text | crate::FileFormat::Journal)
        )
    }

    /// Whether `#` numbers rows by view position because they lack a source position: a
    /// sorted or filtered view of a store, many files, or too large to number.
    pub fn row_numbers_count_the_view(&self) -> bool {
        self.row_numbers
            && !self.carries_source_rows()
            && self.scan_is_the_root()
            && (!self.view.filters.is_empty()
                || !self.view.sort_columns.is_empty()
                || !self.view.sort_ascending)
    }

    /// All `rows` lines are indexed: the count, and notes on the whole file. Frames
    /// already read every line (their height waited), so nothing is stale. Returns
    /// whether the dataset was waiting.
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

    /// Give up on the remaining footers (the pass failed): the dataset stays as opened
    /// and stops waiting, so it can count itself rather than lose its exact count and
    /// windowed reads for the session.
    pub fn give_up_on_pending_footers(&mut self) {
        self.footers_pending = None;
    }

    /// Join every footer's answer to the dataset on screen. Columns only join: new names
    /// go on the end, existing ones keep their (user-arranged) places. The frame is
    /// rebuilt, since the scan now knows conflicting files and can number rows (telling
    /// absent from null). Returned as `Err` while the view is built on the scan (see
    /// [`Self::scan_is_the_root`]): rebuilding the root under a query takes its columns,
    /// so the caller holds them for later.
    pub fn join_dataset_schema(
        &mut self,
        mut found: FootersFound,
    ) -> std::result::Result<(), Box<FootersFound>> {
        if !self.scan_is_the_root() {
            // The columns must wait, but the files' row groups need not: they set offsets
            // without touching a query result's count. Taken, not borrowed: this runs every
            // event while the view is off the scan.
            let row_groups = std::mem::take(&mut found.row_groups);
            if !row_groups.is_empty() {
                self.record_file_row_groups(&row_groups);
            }
            // Boxed: an `Err` this large would burden every successful call.
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
            self.view.column_order.iter().map(String::as_str).collect();
        let joining: Vec<String> = dataset
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .filter(|name| {
                name != crate::schema_union::DRIFT_COLUMN && !known.contains(name.as_str())
            })
            .collect();
        drop(known);
        self.view.column_order.extend(joining);
        // A name can go if its only footer evidence failed to parse this time; every read
        // projects `column_order`, so a missing name would break the scan.
        self.view
            .column_order
            .retain(|name| dataset.schema.contains(name.as_str()));
        let schema = dataset.schema.clone();
        // The scan is built at a schema, and the opening one lacks the new columns; the
        // next page read would ask for a column it lacks.
        match (remote, self.remote_files.as_mut()) {
            (Some(found), Some(remote)) => {
                remote.urls = Arc::new(found.urls);
                remote.scan = found.scan;
                // The counter too: it answers one entry per file given, and the opening one had
                // every file listed; beside a shorter `urls` its answer would be dropped.
                remote.count = found.count;
            }
            // A local directory reads by file only once every footer is known.
            (Some(found), None) => self.remote_files = Some(found.into()),
            (None, _) => {}
        }
        // Also clears `read_as_text`, sound because reading as text is only offered once
        // all footers are in.
        self.record_dataset_schema(dataset, file_rows, files);
        self.footers_pending = None;
        // Drop the buffer so the next collect reads through the new frame, at the same row;
        // `start_row` and column scroll stay.
        self.replace_root(lf, schema);
        // The joined scan may hold rows the narrow open never saw.
        self.pristine_rows = None;
        // Measured on the old frame: a dataset that gained thirty columns would plan its
        // next page from two, overshooting the read budget.
        self.view.observed_bytes_per_row = None;
        // Every file's row groups are known: this is the dataset's count, set before the
        // rebuild.
        if !row_groups.is_empty() {
            self.record_file_row_groups(&row_groups);
        }
        // Rebuilt but not read: this is the drawing thread, and `apply_transformations` ends
        // in a `collect` (on a bucket, a page fetch or a full `len()`). The caller reads it
        // off the event loop; keep this deferred.
        self.deferred(Self::apply_transformations);
        Ok(())
    }

    /// The frame as the user sees it: `lf` without the hidden row-index column. Exports,
    /// reshapes, groups and analyses read this; the buffer keeps the column to trace
    /// rows to files (`display_drift`), invisible since display projects `column_order`.
    pub fn visible_lf(&self) -> LazyFrame {
        Self::without_drift(self.view.lf.clone())
    }

    /// Whether the frame scans a temp file this state holds (a decompressed archive),
    /// removed when the last holder drops; an exit capture must not reference it.
    pub fn scans_a_temp_file(&self) -> bool {
        self.decompress_temp_file.is_some() || !self.converted.is_empty()
    }

    /// Whether the frame scans a downloaded remote file, removed when released; see
    /// [`Self::scans_a_temp_file`].
    pub fn scans_a_download(&self) -> bool {
        self.download.is_some()
    }

    /// How the open reads the data; `None` for a frame handed in whole (from Python).
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

    /// The temp files this state holds, which a read error may name: a decompressed copy,
    /// a download.
    pub(crate) fn temp_files(&self) -> Vec<&Path> {
        let files = self.decompress_temp_file.iter().map(|file| file.path());
        let files = files.chain(self.download.iter().map(|download| download.path()));
        let files = files.chain(self.converted.iter().map(|file| file.path()));
        files.collect()
    }

    /// `lf` without the hidden drift column; a non-strict drop, a no-op when absent.
    pub(super) fn without_drift(lf: LazyFrame) -> LazyFrame {
        lf.drop(by_name([crate::schema_union::DRIFT_COLUMN], false, false))
    }

    /// The frame a query, SQL statement or fuzzy search builds on, without the drift
    /// column (its schema becomes the column order).
    pub(super) fn query_source(&self) -> LazyFrame {
        Self::without_drift(self.original_lf.clone())
    }

    /// The schema of [`Self::query_source`], as known since the open: nothing resolved.
    pub(super) fn query_source_schema(&self) -> Arc<Schema> {
        Self::without_source_rows(self.original_schema.clone()).0
    }

    /// Whether rows still know which file they came from.
    pub fn drifts(&self) -> bool {
        self.view.drift_column_present
    }

    /// What each drift group is missing, for the renderer. Empty when nothing drifts.
    pub fn drift_groups(&self) -> Arc<Vec<crate::schema_union::DriftGroup>> {
        self.view.drift_groups.clone()
    }

    /// Whether an export can name each row's file: the frame still has the row index and
    /// the dataset has files.
    pub fn can_name_source_files(&self) -> bool {
        self.view.drift_column_present
            && !self.drift_files.is_empty()
            && self.drift_files.len() == self.drift_file_starts.len()
    }

    /// The rows an export writes, planned: the view, with a column naming each row's file
    /// in place of the hidden row index when `name_files` asks and the dataset can;
    /// otherwise the index is dropped.
    pub fn export_frame(&self, name_files: bool) -> ExportFrame {
        if name_files && self.can_name_source_files() {
            ExportFrame {
                lf: self.view.lf.clone(),
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

    /// What datui noticed: about the dataset on open, then about the view. Empty when
    /// nothing.
    pub fn notes(&self) -> Vec<crate::notes::Note> {
        // What the read did comes first: it frames every other note. `merged`, since the
        // open and the footer walk each count files a mixed directory's read passed over.
        let mut notes = crate::notes::merged(
            &self.open_notes,
            &self.view.notes,
            &self.view.view_notes,
            self.dataset_schema.as_ref(),
        );
        // What reading a table in place has found, which may grow after the open.
        if let Some(pushdown) = &self.pushdown {
            notes.extend(pushdown.notes());
        }
        notes.extend(self.unfit_notes.iter().flatten().cloned());
        notes.extend(self.view.changes_dropped.iter().cloned());
        if let Some((version, unfit)) = &self.changes_unfit
            && *version == self.view.changes_version
        {
            notes.extend(unfit.iter().cloned());
        }
        notes
    }

    /// The source a full quality run over `scope` reads whole, for checks like an audio
    /// signal's: the data as loaded, or an unmodified view.
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

    /// The lake format whose plain files this dataset is, for the footer chip so the row
    /// count does not read as the table's.
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

    /// The source a view window is read straight from, if any: records or audio frames
    /// of the data as loaded, or a view the source runs itself.
    pub(super) fn window_now(&self) -> Option<Arc<dyn crate::pushdown::Windowed>> {
        if let Some(window) = self.follow_window() {
            return Some(Arc::new(window));
        }
        if let Some(records) = self.fixed_window.as_ref().filter(|_| self.is_pristine()) {
            return Some(records.clone());
        }
        self.pushed_view().map(|view| view.window)
    }

    /// The view as the source runs it, when it can: the root is the data as loaded and
    /// the source can express the sidebar's filters and sort. Derived each time.
    pub(crate) fn pushed_view(&self) -> Option<crate::pushdown::PushedView> {
        let pushdown = self.pushdown.as_ref()?;
        if !self.scan_is_the_root() || self.view.drift_column_present {
            return None;
        }
        let sort: Vec<(String, bool)> = self
            .view
            .sort_columns
            .iter()
            .cloned()
            .zip(self.view.sort_descending.iter().copied())
            .collect();
        pushdown.view(&self.view.filters, &sort, !self.view.sort_ascending)
    }

    /// The view's own count from a source that runs it, or, for a followed file known up
    /// to a row, that count plus the rows after.
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
            .filter(|(generation, _)| *generation == self.view.len_generation)
            .map(|(_, known)| known.as_slice())
    }

    /// A followed file's windows, read from the mark before each: rows as they are, or
    /// filtered unsorted, continuing from where the view's rows are known.
    fn follow_window(&self) -> Option<crate::follow::Window> {
        let follow = self.follow.as_ref()?;
        let known = if self.is_pristine() {
            None
        } else if self.scan_is_the_root()
            && self.view.sort_columns.is_empty()
            && self.view.sort_ascending
        {
            Some(self.follow_known()?.to_vec())
        } else {
            return None;
        };
        Some(crate::follow::Window {
            lf: self.view.lf.clone(),
            path: follow.path().to_path_buf(),
            marks: follow.marks().clone(),
            known,
        })
    }

    /// A followed view's count known up to a file row, plus the view's rows after it,
    /// read from the preceding mark.
    fn follow_counter(&self) -> Option<crate::pushdown::Counter> {
        let follow = self.follow.as_ref()?;
        let &(before, row) = self.follow_known()?.last()?;
        let rest =
            crate::follow::from_marks(&self.view.lf, follow.path(), follow.marks(), row, None)?;
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

    /// The unit of `column`, from a delimited spec's unit row or the file. Columns kept
    /// through filters, sorts, drills or queries (renamed or not) keep it; computed
    /// columns have none.
    pub fn unit_of(&self, column: &str) -> Option<&str> {
        if self.delimited.is_none() && self.file_units.is_empty() {
            return None;
        }
        let loaded = match &self.view.lineage {
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
        self.view
            .schema
            .iter_names()
            .filter_map(|name| Some((name.to_string(), self.unit_of(name)?.to_string())))
            .collect()
    }

    /// What the file said besides its rows: its Info panel tab.
    pub fn format_detail(&self) -> Option<&crate::text_formats::Detail> {
        self.detail.as_deref()
    }

    /// A followed journal's Info tab, reread once its pipe ended and all rows are in: the
    /// frame reading every entry. `None` otherwise, or once asked.
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

    /// Whether datui noticed anything, without building `notes()`.
    pub fn has_notes(&self) -> bool {
        // A view note needs a disagreeing column, which always has an open note, so the
        // Notes tab does not flicker with sorting. The open's own notes count too.
        !self.view.notes.is_empty()
            || !self.open_notes.is_empty()
            || self.unfit_notes.as_ref().is_some_and(|n| !n.is_empty())
            || !self.view.changes_dropped.is_empty()
            || self
                .changes_unfit
                .as_ref()
                .is_some_and(|(v, n)| *v == self.view.changes_version && !n.is_empty())
            || self
                .pushdown
                .as_ref()
                .is_some_and(|p| !p.notes().is_empty())
    }

    /// Whether there is something to say that has not been offered yet.
    pub fn notes_unseen(&self) -> bool {
        self.has_notes() && !self.view.notes_seen
    }

    /// The rows a filter or sort on `column` must leave out: every row of every file
    /// holding it in an unread type.
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

    /// Columns the filter or sort names that some file holds in another type, in dataset
    /// order, each once.
    fn view_columns_with_conflicts(&self) -> Vec<crate::schema_union::ColumnDrift> {
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let named: HashSet<&str> = self
            .view
            .filters
            .iter()
            .map(|filter| filter.column.as_str())
            .chain(self.view.sort_columns.iter().map(String::as_str))
            .collect();
        dataset
            .columns
            .iter()
            .filter(|column| column.conflicting_files > 0 && named.contains(column.name.as_str()))
            .cloned()
            .collect()
    }

    /// Leave out rows whose files hold a filtered or sorted column in an unread type, and
    /// say how many: those rows are null there, which a filter drops anyway and a sort
    /// would wrongly gather at one end.
    pub(super) fn view_exclusions(&self) -> Vec<(Vec<(usize, usize)>, crate::notes::Note)> {
        if !self.view.drift_column_present {
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
                .view
                .filters
                .iter()
                .any(|filter| filter.column.as_str() == column.name.as_str());
            let sorted = self
                .view
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

    /// The notes for what the filter and sort leave out, for a caller restoring a frame
    /// that already excludes those rows. Derived, never stored across a view change.
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
        self.view.notes_seen = true;
    }

    /// Read `column` as text from every file, showing values a type conflict hid.
    /// Rebuilds the scan from what is on hand (files, row counts, per-file types): no
    /// listing, footer read or request. Filters and sort are re-applied. Returns whether
    /// anything happened (`false` if not on offer or already read so); a failed scan is
    /// kept as the error and returned.
    pub fn read_column_as_text(&mut self, column: &str) -> PolarsResult<bool> {
        let name = PlSmallStr::from(column);
        let Some(dataset) = self.dataset_at_open.clone() else {
            return Ok(false);
        };
        if !self.view.drift_column_present || self.read_as_text.contains(&name) {
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

        // From the dataset as its footers found it, not the view (which no longer has the
        // per-file types).
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
        // The remote scan hoists partitions inside its own closure; only local scans do it
        // here.
        let lf = if self.remote_files.is_some() {
            lf
        } else {
            crate::open_scan::hoist_partition_columns(
                lf,
                &dataset.schema,
                self.partition_columns.as_deref().unwrap_or(&[]),
                drift.is_some(),
            )
        };

        let view = dataset.reading_as_text(&as_text);
        self.read_as_text = as_text;
        // `text_schema` keeps column places, so the user's order still holds.
        let schema = view.schema.clone();
        self.view.drift_groups = Arc::new(view.groups.clone());
        self.groups_at_open = self.view.drift_groups.clone();
        self.view.notes = Self::notes_datui_can_act_on(&view, self.view.drift_column_present);
        self.notes_at_open = self.view.notes.clone();
        // One note went and another came (how the column now compares), worth seeing.
        self.view.notes_seen = false;
        self.dataset_schema = Some(view);
        self.replace_root(lf, schema);
        // Re-applies the filter and sort, and their exclusion note, now one shorter.
        self.apply_transformations();
        Ok(true)
    }

    /// The dataset's notes, keeping the read-as-text offer only where it would work: it
    /// needs each file's row start, unknown when footers were sampled or failed. An offer
    /// the action then declines is worse than none.
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

    /// Each file's row count from the footers: differences of the kept starts, the total
    /// closing the last.
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

    /// The counter for a remote dataset's files while its count would be the data's (the
    /// frame is the scan as loaded, files uncounted). Not while a footer pass runs: it
    /// brings the row groups, and this would read every footer again.
    pub fn remote_files_counter(&self) -> Option<FileCounter> {
        if self.footers_pending.is_some() {
            return None;
        }
        self.remote_files
            .as_ref()
            .filter(|f| f.offsets.is_none() && self.is_pristine())
            .map(|f| f.count.clone())
    }

    /// Record each file's row-group sizes: the total, the groups buffers are planned in,
    /// and which files hold which rows. A local dataset takes only the total and groups.
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
