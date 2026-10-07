//! The view's settings, its checkpoint (`rollback_point`, `roll_back`,
//! `try_transition`) and a followed file's view.

use super::*;

/// The view as it stood before a query or view replaced it: a checkpoint. A query can
/// plan and still fail on its rows (a value that will not cast); then the table returns
/// to this, rows and all. Frames and buffers are shared, not copied. Taken by
/// [`DataTableState::rollback_point`] or [`DataTableState::try_transition`], restored by
/// [`DataTableState::roll_back`].
pub struct ViewRollback {
    /// The data as loaded when this was taken; see [`DataTableState::roll_back`].
    root_generation: u64,
    /// A count of this frame that came back after it was replaced, to return with it.
    counted: Option<CountedRows>,
    table_state: TableState,
    termcol_index: usize,
    view: View,
}

impl ViewRollback {
    /// A background count of frame `len_generation` came back while this checkpoint
    /// was waiting. Kept when the frame is the one this restores, so the rows and the
    /// count return together; returns whether it was.
    pub fn count_landed(
        &mut self,
        len_generation: u64,
        rows: usize,
        file_row_groups: Option<&[Vec<usize>]>,
    ) -> bool {
        let ours = len_generation == self.view.len_generation;
        if ours {
            self.counted = Some(CountedRows {
                rows,
                file_row_groups: file_row_groups.map(<[_]>::to_vec),
            });
        }
        ours
    }
}

/// A row count read in the background: the total, and for a remote dataset of many
/// files, the rows in each row group of each file.
pub(super) struct CountedRows {
    rows: usize,
    file_row_groups: Option<Vec<Vec<usize>>>,
}

impl DataTableState {
    // Getter methods for view creation
    /// Filters for a view: while drilled into a group these are the grouped view's,
    /// which is what a view reproduces (it cannot express a drill-down).
    pub fn get_filters(&self) -> &[FilterStatement] {
        match &self.view.grouped {
            Some(view) => &view.filters,
            None => &self.view.filters,
        }
    }

    pub fn get_sort_columns(&self) -> &[String] {
        match &self.view.grouped {
            Some(view) => &view.sort_columns,
            None => &self.view.sort_columns,
        }
    }

    pub fn get_sort_ascending(&self) -> bool {
        match &self.view.grouped {
            Some(view) => view.sort_ascending,
            None => self.view.sort_ascending,
        }
    }

    pub fn get_sort_descending(&self) -> &[bool] {
        match &self.view.grouped {
            Some(view) => &view.sort_descending,
            None => &self.view.sort_descending,
        }
    }

    /// Filters applied to the frame on screen (inside the group while drilled). This is
    /// what the Sort & Filter sidebar shows and edits.
    pub fn view_filters(&self) -> &[FilterStatement] {
        &self.view.filters
    }

    pub fn view_sort_columns(&self) -> &[String] {
        &self.view.sort_columns
    }

    pub fn view_sort_ascending(&self) -> bool {
        self.view.sort_ascending
    }

    pub fn view_sort_descending(&self) -> &[bool] {
        &self.view.sort_descending
    }

    /// The header's sort marks: the sidebar's sort, or else the ORDER BY of the SQL
    /// in effect, while its own rows are on screen (not a group drilled into).
    pub(crate) fn header_sort(&self) -> (Vec<String>, Vec<bool>) {
        if self.view.sort_columns.is_empty() && self.view.grouped.is_none() {
            self.view.query_order.iter().cloned().unzip()
        } else {
            (
                self.view.sort_columns.clone(),
                self.view.sort_descending.clone(),
            )
        }
    }

    /// The pivot/melt result in effect, for a snapshot that may need to put it back.
    #[cfg(test)]
    pub(crate) fn reshaped_lf_clone(&self) -> Option<LazyFrame> {
        self.view.reshaped_lf.clone()
    }

    pub fn get_column_order(&self) -> &[String] {
        &self.view.column_order
    }

    /// Whether the table shows its defaults (no query, filters, sort or reshape, file
    /// order, nothing locked): a view saved from here carries nothing and, matching by
    /// schema, would shadow real views.
    pub fn is_at_defaults(&self) -> bool {
        self.sampled.is_none()
            && self.view.column_changes.is_empty()
            && self.view.active_query.is_empty()
            && self.view.active_sql_query.is_empty()
            && self.view.active_fuzzy_query.is_empty()
            && self.view.filters.is_empty()
            && self.view.sort_columns.is_empty()
            && self.view.last_pivot_spec.is_none()
            && self.view.last_melt_spec.is_none()
            && self.locked_columns_count() == 0
            && self.view.column_order.iter().map(String::as_str).eq(self
                .view
                .schema
                .iter_names()
                .map(|s| s.as_str()))
    }

    pub fn get_active_query(&self) -> &str {
        &self.view.active_query
    }

    pub fn get_active_sql_query(&self) -> &str {
        &self.view.active_sql_query
    }

    /// Whether the rows on screen can be read at all: a sort, a filter or a column
    /// named in the layout that the frame does not have fails here. Resolves the plan
    /// and reads no rows.
    pub fn check_plan(&self) -> PolarsResult<()> {
        self.view
            .lf
            .clone()
            .select(self.binary_stub_exprs())
            .collect_schema()
            .map(|_| ())
    }

    /// The view as it is now, to go back to if a query fails while running.
    pub fn rollback_point(&self) -> ViewRollback {
        ViewRollback {
            root_generation: self.root_generation,
            counted: None,
            table_state: self.table_state,
            termcol_index: self.termcol_index,
            view: self.view.clone(),
        }
    }

    /// Restore the view `rollback_point` saved, with no error showing, with its row count,
    /// buffer, and any count that landed meanwhile ([`ViewRollback::count_landed`]). Reads
    /// nothing. A checkpoint over since-replaced data (a footer join, a column read as text)
    /// would mix roots, so the view returns to the data as loaded instead.
    pub fn roll_back(&mut self, saved: ViewRollback) {
        if saved.root_generation != self.root_generation {
            self.return_to_root();
            return;
        }
        self.widths.keep_learned();
        self.view = saved.view;
        self.table_state = saved.table_state;
        self.termcol_index = saved.termcol_index;
        self.clear_column_moves();
        self.reveal_cursor = true;
        self.error = None;
        // After the frame, so the count is taken as this frame's.
        if let Some(counted) = saved.counted {
            self.take_count(counted.rows, counted.file_row_groups.as_deref());
        }
    }

    /// Run `steps` as one view transition, planned but never read, with no error showing.
    /// A failing step restores the prior view and returns its error; on success the prior
    /// view comes back with the result, for [`Self::roll_back`] if the new rows fail.
    pub fn try_transition<T, E>(
        &mut self,
        steps: impl FnOnce(&mut Self) -> std::result::Result<T, E>,
    ) -> std::result::Result<(T, ViewRollback), E> {
        let saved = self.rollback_point();
        self.error = None;
        match self.deferred(steps) {
            Ok(value) => Ok((value, saved)),
            Err(e) => {
                self.roll_back(saved);
                Err(e)
            }
        }
    }

    /// Run `steps` with every collect they would make left to the caller, who reads
    /// the rows off the UI thread (`prepare_async_collect`).
    pub fn deferred<R>(&mut self, steps: impl FnOnce(&mut Self) -> R) -> R {
        let deferred = std::mem::replace(&mut self.defer_collect, true);
        let result = steps(self);
        self.defer_collect = deferred;
        result
    }

    /// A background count of frame `len_generation` came back. Taken when that frame is
    /// the one on screen; returns whether it was.
    pub fn count_landed(
        &mut self,
        len_generation: u64,
        rows: usize,
        file_row_groups: Option<&[Vec<usize>]>,
    ) -> bool {
        let current = len_generation == self.view.len_generation;
        if current {
            self.take_count(rows, file_row_groups);
        }
        current
    }

    /// What a staged open leaves: a row total from however far the buffer reached,
    /// with no count taken.
    #[cfg(test)]
    pub(crate) fn set_provisional_rows(&mut self, n: usize) {
        self.view.num_rows = n;
    }

    /// The count of the frame on screen: from the files' row groups when there are
    /// some, else the total.
    fn take_count(&mut self, rows: usize, file_row_groups: Option<&[Vec<usize>]>) {
        match file_row_groups {
            Some(groups) => self.record_file_row_groups(groups),
            None => self.set_num_rows(rows),
        }
    }

    /// The follow of the file this dataset reads, while it is followed.
    pub fn follow(&self) -> Option<&crate::follow::Follow> {
        self.follow.as_ref()
    }

    pub fn follow_mut(&mut self) -> Option<&mut crate::follow::Follow> {
        self.follow.as_mut()
    }

    /// Join `fields` that a followed NDJSON pipe brought after the open: the scan reads
    /// them, appended to the column order, as footer joins do. `Err` while the view is a
    /// query, reshape or group (the caller holds them); `Ok(false)` when nothing joins.
    pub(crate) fn join_followed_fields(
        &mut self,
        fields: &[Field],
    ) -> std::result::Result<bool, ()> {
        if !self.scan_is_the_root() {
            return Err(());
        }
        let (Some(follow), Some(format)) = (self.follow.as_ref(), self.read_as) else {
            return Ok(false);
        };
        let (path, rows) = (follow.path().to_path_buf(), follow.shown());
        let Some(mut lf) = crate::follow::widen(&self.original_lf, &path, format, fields, rows)
        else {
            return Ok(false);
        };
        let Ok(schema) = lf.collect_schema() else {
            return Ok(false);
        };
        let known: std::collections::HashSet<&str> =
            self.view.column_order.iter().map(String::as_str).collect();
        let joining: Vec<String> = schema
            .iter_names()
            .map(|name| name.to_string())
            .filter(|name| !known.contains(name.as_str()))
            .collect();
        drop(known);
        self.view.column_order.extend(joining);
        self.replace_root(lf, schema);
        if self.is_pristine() {
            // The same rows the watcher counted, with more columns.
            self.set_num_rows(rows);
        }
        // Rebuilt but not read, as a footer join is: the caller reads the rows on
        // screen off the event loop.
        self.deferred(Self::apply_transformations);
        Ok(true)
    }

    /// Follow the file this dataset reads with `follow`, whose watcher is running.
    pub fn start_following(&mut self, follow: crate::follow::Follow) {
        self.follow = Some(follow);
    }

    /// Stop following. The rows read so far stay.
    pub fn stop_following(&mut self) {
        if let Some(mut follow) = self.follow.take() {
            follow.end();
        }
    }

    /// Put the view on the last page, leaving the cursor where it is until the rows of
    /// that page are read: the next read is of that page alone.
    pub(crate) fn aim_at_end(&mut self) {
        if self.view.num_rows_valid && self.visible_rows > 0 {
            self.view.start_row = self.view.num_rows.saturating_sub(self.visible_rows);
        }
    }

    /// Whether the cursor is on the last row of a view whose length is known.
    pub fn on_last_row(&self) -> bool {
        self.view.num_rows_valid
            && (self.view.num_rows == 0
                || self.view.start_row + self.table_state.selected().unwrap_or(0) + 1
                    >= self.view.num_rows)
    }

    /// Every frame the view holds that carries the scan of the data as loaded.
    pub(super) fn each_frame(&mut self, mut f: impl FnMut(&mut LazyFrame)) {
        f(&mut self.original_lf);
        f(&mut self.view.base_lf);
        f(&mut self.view.lf);
        if let Some(lf) = self.view.unsorted_lf.as_mut() {
            f(lf);
        }
        if let Some(lf) = self.view.reshaped_lf.as_mut() {
            f(lf);
        }
        if let Some(source) = self.view.group_source.as_mut() {
            f(&mut source.rows);
        }
        if let Some(grouped) = self.view.grouped.as_mut() {
            f(&mut grouped.lf);
            f(&mut grouped.base_lf);
            if let Some(source) = grouped.group_source.as_mut() {
                f(&mut source.rows);
            }
        }
    }

    /// The followed file now holds `rows` complete rows: every frame reads that many.
    /// `restarted` when reread from the start. Returns whether the rows on hand still stand
    /// (a filter-only view keeps them; new rows come after).
    pub(crate) fn follow_to(&mut self, rows: usize, restarted: bool) -> bool {
        let Some(path) = self.follow.as_ref().map(|f| f.path().to_path_buf()) else {
            return true;
        };
        let rows_stand = !restarted
            && self.view.sort_columns.is_empty()
            && self.view.sort_ascending
            && self.scan_is_the_root();
        let known = self.known_before_follow(&path, restarted);
        self.each_frame(|lf| crate::follow::bound(lf, &path, rows));
        self.invalidate_num_rows();
        self.follow_known = known.map(|known| (self.view.len_generation, known));
        if self.is_pristine() {
            // The watcher counted them as the scan reads them: nothing to count again.
            self.set_num_rows(rows);
        } else if self.scan_is_the_root() {
            self.pristine_rows = Some(rows);
        }
        if restarted {
            self.view.start_row = 0;
            self.table_state.select(Some(0));
        }
        if !rows_stand {
            self.drop_buffer();
        }
        rows_stand
    }

    /// Where the view's rows are known before frames read more of the followed `path`:
    /// known points for the current count, plus the count when exact. Only for row-wise
    /// views (filters and a sort over file rows), so new rows are counted alone.
    fn known_before_follow(&mut self, path: &Path, restarted: bool) -> Option<Vec<(usize, usize)>> {
        if restarted || self.is_pristine() || !self.scan_is_the_root() {
            return None;
        }
        let mut known = self
            .follow_known
            .take()
            .filter(|(generation, _)| *generation == self.view.len_generation)
            .map(|(_, known)| known);
        if self.view.num_rows_valid
            && let Some(row) = crate::follow::bound_of(&self.view.lf, path)
        {
            let known = known.get_or_insert_with(Vec::new);
            // One point per stretch of marks is enough to read on from.
            if let [.., before, last] = known.as_slice()
                && last.1 - before.1 < crate::follow::MARK_ROWS as usize
            {
                known.pop();
            }
            if known.last().is_none_or(|&(_, at)| at < row) {
                known.push((self.view.num_rows, row));
            }
        }
        known
    }

    /// The followed file was deleted: every frame reads it through `file`, a handle
    /// held on it, which still reads what it held.
    pub(crate) fn read_followed_through(&mut self, file: &std::fs::File) {
        let Some(path) = self.follow.as_ref().map(|f| f.path().to_path_buf()) else {
            return;
        };
        self.each_frame(|lf| crate::follow::read_through(lf, &path, file));
    }

    /// The frame on screen: the root, then the query or reshape, the filters and the
    /// sort. Column order is applied when rows are read.
    pub fn lf(&self) -> &LazyFrame {
        &self.view.lf
    }

    /// The schema of the frame on screen.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.view.schema
    }

    /// The rows the frame holds: exact when [`Self::is_num_rows_valid`], else as far as
    /// the reads so far have reached.
    pub fn num_rows(&self) -> usize {
        self.view.num_rows
    }

    /// Why the last query, step or read failed, while it is still showing.
    pub fn error(&self) -> Option<&PolarsError> {
        self.error.as_ref()
    }

    /// Stop showing the last failure. The view is as it was; only the message goes.
    pub fn dismiss_error(&mut self) {
        self.error = None;
    }

    /// The first row of the page on screen.
    pub fn start_row(&self) -> usize {
        self.view.start_row
    }

    /// The hive partition columns the dataset was loaded with.
    pub fn partition_columns(&self) -> Option<&[String]> {
        self.partition_columns.as_deref()
    }

    /// Whether reads use Polars' streaming engine.
    pub fn polars_streaming(&self) -> bool {
        self.polars_streaming
    }
}
