//! Row counts, footer passes and line indexing behind a dataset's first rows, and
//! what waits on them: an End, a `:N`, the status line.

use crate::background::{LenCount, OwedCount};
use crate::jobs::{Answer, Job};
use crate::table::DataTableState;
use crate::{App, AppEvent, logging};
use std::sync::Arc;

/// The dataset's row count, footer pass and line indexing, and what waits on them.
#[derive(Default)]
pub struct Counting {
    /// The footer counter of the dataset on screen, reported to by its pass behind the
    /// open; handed over by the installing load. See [`Self::footer_progress`].
    pub(crate) footer_progress: Arc<crate::schema_union::FooterProgress>,
    /// The count when this frame began, or `None` with no pass running. Read once per
    /// frame so the loading body and footer, painted moments apart while the pass
    /// runs, print the same number.
    pub(crate) footers_this_frame: Option<(usize, usize)>,
    /// Where the last load-ahead was asked from. See [`App::load_ahead`].
    pub(crate) loaded_ahead_from: Option<(u64, usize, usize, usize)>,
    // `len_generation` of the background row count in flight, so a (possibly minutes-long)
    // count is not respawned on every scroll.
    pub(crate) len_count_inflight: Option<u64>,
    /// The `len_generation` of a promised count not yet started. A local full count
    /// competes with the first page for disk and Polars workers, and the page may make
    /// it unnecessary, so it starts after a paint. See [`App::frame_painted`].
    pub(crate) count_after_paint: Option<u64>,
    /// Counts started, so a test can say none began before the page was painted.
    #[cfg(test)]
    pub(crate) counts_spawned: std::cell::Cell<usize>,
    /// Times an installed dataset's own first rows were asked for, so a test can say a
    /// view applied on open read them instead.
    #[cfg(test)]
    pub(crate) first_rows_asked: usize,
    // `len_generation` whose background count failed: while current, the count shows
    // "?" rather than a provisional total.
    pub(crate) len_count_failed: Option<u64>,
    /// End pressed on a remote dataset before its count: jump when this generation's
    /// count arrives.
    pub(crate) end_after_count: Option<u64>,
    /// End pressed while a dataset still read its footers (which bring its end): jump
    /// when they land, for that `dataset_generation` only, so a directory left behind
    /// cannot move the next one's view.
    pub(crate) end_when_the_footers_land: Option<u64>,
    /// End pressed while a text file's lines were indexed: jump when done, for that
    /// dataset alone.
    pub(crate) end_when_indexed: Option<u64>,
    /// Stops the indexing thread of the dataset on screen's lines.
    pub(crate) indexing_stop: Arc<std::sync::atomic::AtomicBool>,
    /// The lines being indexed, until they all are.
    pub(crate) indexing_lines: Option<Arc<crate::lines::Lines>>,
    /// The indexing waits while home is up.
    pub(crate) indexing_paused: bool,
    /// `:N` past the lines indexed so far, for that dataset: gone to once they all are.
    pub(crate) goto_when_indexed: Option<(u64, usize)>,
    /// The last count started: what it has read of the footers, and its stop (Esc).
    pub(crate) count_progress: Arc<crate::schema_union::FooterProgress>,
    /// The `dataset_generation` an exact count was asked for (`c` in Info) despite
    /// having more files than are counted unasked.
    pub(crate) exact_count_asked: Option<u64>,
    /// `c` pressed while a stopped count wound down: count again when its answer for
    /// this `len_generation` arrives.
    pub(crate) count_after_stop: Option<u64>,
    /// Footers found while the user viewed a query, pivot or drill-down. Held, not
    /// applied: widening the scan under a query takes its columns away. Offered again
    /// once the view is back on the data.
    pub(crate) footers_held: Option<(u64, crate::table::FootersFound)>,
    /// Fields a followed NDJSON pipe brought after the open, held like footers.
    pub(crate) followed_fields_held: Option<(u64, Vec<polars::prelude::Field>)>,
    /// A re-read owed after a footer pass came back empty, held because its collect
    /// would bump the generation under running work. The dataset still needs the
    /// ordinary count; retried after every event until that work is done.
    pub(crate) reread_owed: Option<u64>,
    /// The objects a listing had found when this frame began, read once like
    /// `footers_this_frame`.
    pub(crate) listed_this_frame: Option<usize>,
}

impl Counting {
    /// A new dataset is on screen, its footers counted on `footers`: the last one's
    /// pass stops, and Ends or `:N`s waiting on its footers, count or lines are
    /// forgotten: a `len_generation` does not say which dataset, so a leftover would
    /// act on the next.
    pub(crate) fn reset_for_dataset(&mut self, footers: Arc<crate::schema_union::FooterProgress>) {
        self.footer_progress.cancel();
        self.footer_progress = footers;
        self.end_when_the_footers_land = None;
        self.end_after_count = None;
        self.end_when_indexed = None;
        self.goto_when_indexed = None;
    }

    /// The markers a running query keeps for the view it may roll back to.
    pub(crate) fn markers(&self) -> CountMarkers {
        CountMarkers {
            len_count_inflight: self.len_count_inflight,
            count_after_paint: self.count_after_paint,
            len_count_failed: self.len_count_failed,
        }
    }

    /// Put back the markers of a view a failed query rolled back to.
    pub(crate) fn restore(&mut self, markers: CountMarkers) {
        self.len_count_inflight = markers.len_count_inflight;
        self.count_after_paint = markers.count_after_paint;
        self.len_count_failed = markers.len_count_failed;
    }
}

/// A frame's count markers: in flight, held for a paint, and failed.
#[derive(Clone, Copy)]
pub(crate) struct CountMarkers {
    pub(crate) len_count_inflight: Option<u64>,
    pub(crate) count_after_paint: Option<u64>,
    pub(crate) len_count_failed: Option<u64>,
}

/// Bytes of a text file indexed per step; between steps the indexer checks it is
/// still wanted.
const INDEX_STEP: usize = 16 << 20;

impl App {
    /// Whether the footer's row count is on its way, so a spinner stands in. Asked by
    /// the bar and by the run loop that turns the spinner, so they agree.
    pub fn row_count_pending(&self) -> bool {
        // A load in flight: the number held is the outgoing dataset's. A dataset reading
        // its own footers: it declines the ordinary count, and the number held only
        // reaches as far as the buffer (`Rows: 70` for six thousand files).
        self.counting.len_count_inflight.is_some()
            || self.loading.awaiting_dataset()
            // A re-read owed after failed footers starts a count too; without this the bar
            // would print the buffer's partial number meanwhile.
            || self.counting.reread_owed.is_some()
            || self
                .data_table_state
                .as_ref()
                .is_some_and(|state| state.counts_itself_later())
    }

    pub(crate) fn dataset_is_still_reading_its_footers(&self) -> bool {
        self.data_table_state
            .as_ref()
            .is_some_and(|state| state.footers_pending().is_some())
    }

    /// The first rows of an open are on screen, or will not be read: its wait is over.
    pub(crate) fn first_rows_settled(&mut self) {
        self.loading.first_rows_settled();
    }

    /// Read the rows on screen again now that the frame they were read through was
    /// replaced by the join. The dataset's errand, not its open's: the join already
    /// dropped the buffer, so this must not be dropped.
    pub(crate) fn reread_after_the_footers_joined(&mut self) {
        // Any re-read satisfies an owed one.
        self.counting.reread_owed = None;
        // An End waiting on these footers. Taken either way: a flag from a gone dataset is
        // not this one's.
        if self.counting.end_when_the_footers_land.take() == Some(self.dataset_generation) {
            self.status_message = None;
            if let Some(next) = self.jump_key(crate::Scroll::End) {
                // The jump reads the page it lands on, so reading this one first would be wasted.
                let _ = self.events.send(next);
                return;
            }
            // Unless it asked for no read (already at the end, or waiting on the ordinary
            // count): the join dropped the buffer, so fall through and read.
        }
        self.spawn_async_collect(Self::LOADING_BUFFER);
    }

    /// Run a buffer collect asked for while other work held the generation; like
    /// `reread_when_the_work_allows`, retried after every event.
    pub(crate) fn collect_when_the_work_allows(&mut self) {
        let Some(&Job::OwedRows { dataset, .. }) = self.jobs.owed(Self::owed_rows) else {
            return;
        };
        if dataset != self.dataset_generation {
            // The dataset it was owed to is gone: put down the errand and its keys; the status
            // line belongs to the replacement.
            self.jobs.take_owed(Self::owed_rows);
            return;
        }
        if self.work_a_bump_would_strand() {
            return;
        }
        let Some(Job::OwedRows { status, .. }) = self.jobs.take_owed(Self::owed_rows) else {
            return;
        };
        if !self.spawn_async_collect(&status) {
            self.busy = false;
            self.status_message = None;
            // The owed collect may have been an open's first rows; else the bar would read
            // "Loading buffer... 70%" forever.
            self.first_rows_settled();
        }
    }

    /// Run the re-read a failed footer pass owes the dataset once it strands nothing
    /// (`work_the_join_would_cancel`), as held columns wait.
    pub(crate) fn reread_when_the_work_allows(&mut self) {
        let Some(generation) = self.counting.reread_owed else {
            return;
        };
        if generation != self.dataset_generation {
            // The dataset it was owed to is gone; so is the errand.
            self.counting.reread_owed = None;
            return;
        }
        if self.work_the_join_would_cancel() {
            return;
        }
        self.reread_after_the_footers_joined();
    }

    /// Retire an End waiting on a count that can no longer answer it: only the flag and
    /// its message; the jump is not re-issued (see `BackgroundLenReady`).
    fn retire_the_end_that_was_waiting(&mut self) {
        self.counting.end_after_count = None;
        self.take_down_the_counting_status();
    }

    /// Take down "Counting rows to find the end...", and only that: the line may now
    /// belong to a load or an export.
    fn take_down_the_counting_status(&mut self) {
        if matches!(
            self.status_message.as_deref(),
            Some(Self::COUNTING_FOR_END | Self::INDEXING_FOR_ROW)
        ) {
            self.status_message = None;
        }
    }

    /// Whether the bar is still keeping quiet about a fetch for the view.
    pub(crate) fn fetch_too_young_to_mention(&self) -> bool {
        self.status_message.as_deref() == Some(Self::LOADING_BUFFER)
            && self
                .rows_in_flight()
                .is_some_and(|inflight| inflight.began.elapsed() < Self::A_FETCH_WORTH_SAYING)
    }

    /// Work the re-read after a join would cancel: anything a bump would strand
    /// ([`Jobs::would_strand`]), plus a chart being prepared, since the join changes the
    /// frame (a fresh `len_generation`) under it.
    pub(crate) fn work_the_join_would_cancel(&self) -> bool {
        self.work_a_bump_would_strand() || self.chart_preparing()
    }

    /// Give the dataset what its footers found, if it can take it now. Not while a
    /// query, pivot, melt or drill-down is the root: widening the scan under it takes
    /// its columns away, so they are held and retried after every event. Returns
    /// whether the dataset took them, so the caller re-reads the rows on screen.
    pub(crate) fn join_held_footers(&mut self) -> bool {
        let Some((generation, _)) = self.counting.footers_held.as_ref() else {
            return false;
        };
        if *generation != self.dataset_generation {
            // The dataset they belong to is gone; so are they.
            self.counting.footers_held = None;
            return false;
        }
        if self.data_table_state.is_none() || self.work_the_join_would_cancel() {
            return false;
        }
        let Some((generation, found)) = self.counting.footers_held.take() else {
            return false;
        };
        let state = self
            .data_table_state
            .as_mut()
            .expect("checked just above, and nothing since takes it");
        // The frame on screen knows whether it still grows from the scan.
        match state.join_dataset_schema(found) {
            Ok(()) => true,
            Err(found) => {
                self.counting.footers_held = Some((generation, *found));
                false
            }
        }
    }

    /// Start the pass reading the rest of a staged open's footers. Not waited on: the
    /// dataset works meanwhile. Judged by `dataset_generation`, since collects bump the
    /// task generation many times during it.
    pub(crate) fn start_pending_footers(&mut self) {
        let Some(join) = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.footers_pending())
        else {
            return;
        };
        let dataset = self.dataset_generation;
        let progress = self.counting.footer_progress.clone();
        // Answered either way, even on failure or panic: a waiting dataset will not count
        // itself, since the pass was bringing its count.
        self.spawn_job(Job::FootersJoin { dataset }, None, move |_| {
            Ok(Answer::FootersJoined(join(&progress).map(Box::new)))
        });
    }

    /// Run the indexing of the screen's dataset if lines remain (new, or paused for
    /// home). A dataset no longer on screen stops for good; reads waiting on it give up.
    pub(crate) fn index_lines(&mut self) {
        use std::sync::atomic::Ordering;
        self.counting.indexing_stop.store(true, Ordering::Relaxed);
        self.counting.indexing_paused = false;
        let lines = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.lines_to_index().cloned());
        if let Some(old) = self.counting.indexing_lines.take()
            && lines.as_ref().is_none_or(|lines| !Arc::ptr_eq(lines, &old))
        {
            old.stop_indexing();
        }
        let Some(lines) = lines.filter(|lines| lines.resume_indexing()) else {
            return;
        };
        self.counting.indexing_lines = Some(lines.clone());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.counting.indexing_stop = stop.clone();
        let dataset = self.dataset_generation;
        // Not waited on; a read of every line waits on its own worker.
        self.spawn_job(Job::IndexLines { dataset }, None, move |_| {
            loop {
                // Paused or replaced: whoever stopped it decides the waiting reads' fate.
                if stop.load(Ordering::Relaxed) {
                    return Err("stopped".to_string());
                }
                // A panic stops it where it is: the rows so far are all there is.
                let done = logging::catch_panic(|| lines.index_more(INDEX_STEP)).unwrap_or(true);
                if done {
                    lines.stop_indexing();
                    return Ok(Answer::LinesIndexed(lines.rows()));
                }
            }
        });
    }

    /// Home is up: indexing and the reads waiting on it pause until the table is back
    /// ([`Self::begin_frame`]).
    pub(crate) fn pause_indexing(&mut self) {
        if self.counting.indexing_lines.is_some() {
            self.counting
                .indexing_stop
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.counting.indexing_paused = true;
        }
    }

    /// More lines are indexed: the frames take them; once all are, the count and any
    /// waiting End follow.
    pub(crate) fn lines_indexed(&mut self, generation: u64, rows: usize) {
        if generation != self.dataset_generation {
            return;
        }
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        self.counting.indexing_lines = None;
        if !state.lines_indexed(rows) {
            // Set aside during the quality evidence view: they land on the dataset that comes
            // back.
            if let Some(held) = self.quality.evidence_return.as_mut() {
                held.lines_indexed(rows);
            }
            return;
        }
        if let Some((goto, row)) = self.counting.goto_when_indexed.take()
            && goto == generation
        {
            self.take_down_the_counting_status();
            let _ = self.events.send(AppEvent::GoToLine(row));
        }
        if self.counting.end_when_indexed.take() == Some(generation) {
            self.take_down_the_counting_status();
            if let Some(next) = self.jump_key(crate::Scroll::End) {
                let _ = self.events.send(next);
                return;
            }
        }
        // The count indexing held back starts now, and rows past the first are read.
        if self.in_normal_table_view() && !self.loading.awaiting_dataset() {
            self.spawn_collect(None);
        }
    }

    /// Whether the dataset's count waits to be asked for: it has more files than
    /// `[read] exact_count_files` and an estimate to show meanwhile.
    pub(crate) fn count_held_at_estimate(&self, state: &DataTableState) -> bool {
        let limit = self.app_config.read.exact_count_files;
        limit > 0
            && state.files_to_count().is_some_and(|files| files > limit)
            && self.counting.exact_count_asked != Some(self.dataset_generation)
            && state.row_estimate(None).is_some()
    }

    /// The row count estimated from a sample of footers while uncounted: the dataset's
    /// own, or what its footer pass has said so far.
    pub(crate) fn row_estimate(&self) -> Option<crate::schema_union::RowEstimate> {
        self.data_table_state
            .as_ref()?
            .row_estimate(self.counting.footer_progress.estimate())
    }

    /// `(read, of)` when the running count reads footers it can report, so it can be
    /// stopped.
    pub(crate) fn footers_counted(&self) -> Option<(usize, usize)> {
        self.counting.len_count_inflight?;
        self.counting
            .count_progress
            .reading()
            .filter(|_| !self.counting.count_progress.is_cancelled())
    }

    /// `c` in Info: count every row exactly despite the file count.
    pub(crate) fn count_exactly(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        if state.is_num_rows_valid() {
            return;
        }
        let generation = state.len_generation();
        self.counting.exact_count_asked = Some(self.dataset_generation);
        // The footer pass still brings the count; the request holds until it lands.
        if state.counts_itself_later() {
            return;
        }
        // A count stopped before is asked again.
        if self.counting.len_count_failed == Some(generation) {
            self.counting.len_count_failed = None;
        }
        // One stopped and not yet wound down: again once it has.
        if self.counting.len_count_inflight == Some(generation)
            && self.counting.count_progress.is_cancelled()
        {
            self.counting.count_after_stop = Some(generation);
            return;
        }
        if self.counting.len_count_inflight != Some(generation) {
            self.counting.len_count_inflight = Some(generation);
            let job = LenCount::for_state(state);
            self.spawn_count(job);
        }
    }

    /// Esc while a count reads footers: stop it. What it read is kept for the next.
    pub(crate) fn stop_count(&mut self) {
        self.counting.count_progress.cancel();
    }

    /// What the footer pass found, joined if its dataset is still the one on screen
    /// (going home and back keeps it).
    pub(crate) fn footers_joined(
        &mut self,
        dataset: u64,
        found: Option<crate::table::FootersFound>,
    ) -> Option<AppEvent> {
        if dataset == self.dataset_generation {
            let Some(found) = found else {
                // The pass failed: the dataset stays as opened and stops waiting, so it counts
                // itself the ordinary way via the collect below.
                if let Some(state) = self.data_table_state.as_mut() {
                    state.give_up_on_pending_footers();
                }
                // The jump now waits on the ordinary count. Owed, not run: the collect bumps the
                // generation, which an export or analysis may be waiting on.
                self.counting.reread_owed = Some(dataset);
                self.reread_when_the_work_allows();
                return None;
            };
            self.counting.footers_held = Some((dataset, found));
            if self.join_held_footers() {
                self.reread_after_the_footers_joined();
            }
        }
        None
    }

    /// Count the rows off the UI thread; the answer comes back as `BackgroundLenReady`
    /// or `BackgroundLenFailed`.
    pub(crate) fn spawn_count(&mut self, job: LenCount) {
        #[cfg(test)]
        self.counting
            .counts_spawned
            .set(self.counting.counts_spawned.get() + 1);
        self.counting.count_progress = job.progress.clone();
        let count = OwedCount::new(job, self.events.clone());
        self.runtime
            .spawn_blocking(move || count.answer(LenCount::run));
    }

    /// Whether waited-on rows of the frame on screen (an open's, a query's, a scroll's
    /// page) are still being read. A load-ahead is nobody's wait; counts do not queue
    /// behind it.
    fn waited_on_rows_pending(&self, generation: u64) -> bool {
        self.loading.awaiting_dataset()
            || self.jobs.owed(Self::owed_rows).is_some()
            || (self.rows_waited_on()
                && self
                    .rows_in_flight()
                    .is_some_and(|inflight| inflight.dataset == generation))
    }

    /// Whether a paint now would start or retire the count waiting on one. A test
    /// harness, which paints nothing, asks this to know when to report a paint.
    pub fn count_waits_for_a_frame(&self) -> bool {
        self.counting
            .count_after_paint
            .is_some_and(|generation| !self.waited_on_rows_pending(generation))
    }

    /// A frame was painted: read the rows it needed and start the count waiting on it.
    pub fn frame_painted(&mut self) {
        self.pointer.painted();
        self.count_what_was_painted();
        // A resize sets the rows on screen while drawing, after the event pass; rematched
        // finds draw on the frame the wake brings.
        if self.refresh_stale_live_matches() {
            let _ = self.events.send(AppEvent::Wake);
        }
        if let Some(state) = &mut self.data_table_state
            && state.needs_recollect
        {
            state.needs_recollect = false;
            self.spawn_async_collect(App::LOADING_BUFFER);
        }
    }

    /// Start the count waiting on a paint unless its rows are still being read; retire
    /// it if its frame is gone or the rows already gave the count.
    fn count_what_was_painted(&mut self) {
        let Some(generation) = self.counting.count_after_paint else {
            return;
        };
        if self.waited_on_rows_pending(generation) {
            return;
        }
        self.counting.count_after_paint = None;
        let wanted = self
            .data_table_state
            .as_ref()
            .filter(|state| state.len_generation() == generation && !state.is_num_rows_valid());
        match wanted {
            Some(state) => {
                self.counting.len_count_inflight = Some(generation);
                self.spawn_count(LenCount::for_state(state));
            }
            None => {
                if self.counting.len_count_inflight == Some(generation) {
                    self.counting.len_count_inflight = None;
                }
            }
        }
    }

    /// The installed page may have given the row count, or belong to another frame:
    /// either way the waiting count is no longer owed.
    pub(crate) fn retire_a_count_the_rows_answered(&mut self) {
        let Some(generation) = self.counting.count_after_paint else {
            return;
        };
        let answered = self
            .data_table_state
            .as_ref()
            .is_none_or(|state| state.len_generation() != generation || state.is_num_rows_valid());
        if answered {
            self.counting.count_after_paint = None;
            if self.counting.len_count_inflight == Some(generation) {
                self.counting.len_count_inflight = None;
            }
        }
    }

    /// Answers from row counts, footer passes and line indexing.
    pub(crate) fn counting_event(&mut self, event: AppEvent) -> Option<AppEvent> {
        match event {
            AppEvent::BackgroundLenReady {
                len_generation,
                num_rows,
                file_row_groups,
            } => {
                if self.counting.len_count_inflight == Some(len_generation) {
                    self.counting.len_count_inflight = None;
                }
                if self.counting.len_count_failed == Some(len_generation) {
                    self.counting.len_count_failed = None;
                }
                // A count of the view a running query replaced goes back with it.
                if let Some(run) = self.prompt.query_running.as_mut() {
                    run.rollback
                        .count_landed(len_generation, num_rows, file_row_groups.as_deref());
                    if run.counts.len_count_inflight == Some(len_generation) {
                        run.counts.len_count_inflight = None;
                    }
                }
                // Apply the exact total only if the data is unchanged since the count spawned; the
                // buffer has usually painted, so this just corrects the scrollbar and total.
                if let Some(state) = self.data_table_state.as_mut()
                    && state.count_landed(len_generation, num_rows, file_row_groups.as_deref())
                {
                    // End was pressed before there was an end to go to.
                    if self.counting.end_after_count == Some(len_generation) {
                        self.counting.end_after_count = None;
                        self.status_message = None;
                        return self.jump_key(crate::Scroll::End);
                    }
                } else if self.counting.end_after_count == Some(len_generation) {
                    // The count End waited on answers a frame a join has replaced. Retire the flag
                    // and its status; do not re-ask: the current frame may be a different dataset's
                    // (`end_after_count` names only a `len_generation`), which would then jump to its
                    // end unasked.
                    self.retire_the_end_that_was_waiting();
                }
                self.remember_a_downloads_shape();
                None
            }
            AppEvent::FramePainted => {
                self.frame_painted();
                None
            }
            AppEvent::BackgroundLenFailed { len_generation } => {
                if self.counting.len_count_inflight == Some(len_generation) {
                    self.counting.len_count_inflight = None;
                }
                if self.counting.count_after_stop.take() == Some(len_generation) {
                    self.count_exactly();
                    return None;
                }
                if let Some(run) = self.prompt.query_running.as_mut()
                    && run.counts.len_count_inflight == Some(len_generation)
                {
                    run.counts.len_count_inflight = None;
                    run.counts.len_count_failed = Some(len_generation);
                }
                // Mark the count failed so the row count shows "?", only for the frame on screen:
                // counts for two frames can run at once (a join, query, filter or sort takes a
                // fresh `len_generation`), and an orphan's failure must not overwrite a live
                // frame's.
                if self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.len_generation() == len_generation)
                {
                    self.counting.len_count_failed = Some(len_generation);
                }
                // Only for the count End was waiting on, not another frame's failure.
                if self.counting.end_after_count == Some(len_generation) {
                    self.counting.end_after_count = None;
                    if self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.len_generation() == len_generation)
                    {
                        self.status_message =
                            Some("Could not count the rows to find the end".to_string());
                    } else {
                        // Its frame is gone, so its failure says nothing of the one on screen; retired
                        // quietly, as above.
                        self.take_down_the_counting_status();
                    }
                }
                None
            }
            _ => unreachable!("not an event for counting_event"),
        }
    }

    /// Count, behind the Info panel, the values the read's column types made null, for
    /// the Notes: one pass the first time the panel opens on a typed dataset.
    pub(crate) fn count_unfit(&mut self) {
        let dataset = self.dataset_generation;
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let read = state
            .unfit_to_count()
            .map(|(source, typed)| (source, typed, None));
        let view = state
            .changes_unfit_to_count()
            .map(|(source, typed, version)| (source, typed, Some(version)));
        let streaming = self.app_config.performance.streaming;
        for (source, typed, version) in [read, view].into_iter().flatten() {
            let running = self
                .jobs
                .current(|job| {
                    matches!(job, Job::UnfitCount { dataset: d, version: v }
                        if *d == dataset && *v == version)
                })
                .is_some();
            if running {
                continue;
            }
            self.spawn_job(Job::UnfitCount { dataset, version }, None, move |_| {
                let counted = crate::statistics::collect_lazy(
                    crate::column_types::unfit_frame(source, &typed),
                    streaming,
                )
                .map_err(|e| crate::error_display::user_message_from_polars(&e))?;
                Ok(Answer::UnfitCounted(crate::column_types::unfit_counts(
                    &counted, &typed,
                )))
            });
        }
    }

    /// Whether the values the read's column types made null are being counted.
    pub fn unfit_count_pending(&self) -> bool {
        self.jobs
            .current(|job| matches!(job, Job::UnfitCount { .. }))
            .is_some()
    }
}
