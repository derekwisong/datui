//! Row counts, footer passes and line indexing behind a dataset's first rows, and
//! what waits on them: an End, a `:N`, the status line.

use crate::background::{LenCount, OwedCount};
use crate::jobs::{Answer, Job};
use crate::table::DataTableState;
use crate::{App, AppEvent, logging};
use std::sync::Arc;

/// The dataset's row count, footer pass and line indexing, and what waits on them.
pub struct Counting {
    /// The footer counter of the dataset on screen, which its pass behind the open
    /// reports to. Handed over by the load that installed it; a load in flight counts on
    /// its own until then. See [`Self::footer_progress`].
    pub(crate) footer_progress: Arc<crate::schema_union::FooterProgress>,
    /// The count as it stood when this frame began, or `None` if no pass was running.
    ///
    /// Taken once because the pass is running on other threads while the frame is
    /// drawn. The loading body and the footer are painted a millisecond apart,
    /// and when each read the counter for itself they printed different numbers for
    /// one wait — and the bar could print a phase's flat percentage beside a count
    /// that had finished between the two reads.
    pub(crate) footers_this_frame: Option<(usize, usize)>,
    /// Where the last load-ahead was asked from. See [`App::load_ahead`].
    pub(crate) loaded_ahead_from: Option<(u64, usize, usize, usize)>,
    // `len_generation` of the in-flight background row-count, if any. Prevents re-spawning
    // the (potentially minutes-long) count on every scroll while it's still running.
    pub(crate) len_count_inflight: Option<u64>,
    /// The `len_generation` of a count `len_count_inflight` promises that has not
    /// started. A full count of a local frame competes with reading its first page for
    /// the disk and the Polars workers, and that page's rows can make it unnecessary,
    /// so it starts once a frame has painted them. See [`App::frame_painted`].
    pub(crate) count_after_paint: Option<u64>,
    /// Counts started, so a test can say none began before the page was painted.
    #[cfg(test)]
    pub(crate) counts_spawned: std::cell::Cell<usize>,
    /// Times an installed dataset's own first rows were asked for, so a test can say a
    /// view applied on open read them instead.
    #[cfg(test)]
    pub(crate) first_rows_asked: usize,
    // `len_generation` whose background row-count failed. While this matches the current
    // generation (and the count is still invalid) the row count is shown as "?" rather than a
    // misleading provisional total.
    pub(crate) len_count_failed: Option<u64>,
    /// End was pressed on a remote dataset before its rows were counted: go there when
    /// the count for this generation arrives, rather than to a guess.
    pub(crate) end_after_count: Option<u64>,
    /// End was pressed while a dataset was still reading its footers, which is where
    /// its end is coming from. Jump when they land — and only for that dataset, which
    /// is what the generation is for: a directory the user pressed End on and then walked
    /// away from must not move the view of the one they opened next. `end_after_count`
    /// alongside keys itself the same way, to `len_generation`.
    pub(crate) end_when_the_footers_land: Option<u64>,
    /// End was pressed while a text file's lines were still being indexed: jump when
    /// the last of them is, for that dataset alone.
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
    /// The dataset (`dataset_generation`) an exact count was asked for (`c` in the
    /// Info panel), of more files than the count reads unasked.
    pub(crate) exact_count_asked: Option<u64>,
    /// `c` was pressed while a stopped count was still winding down: count again when
    /// its answer, for this `len_generation`, comes in.
    pub(crate) count_after_stop: Option<u64>,
    /// What a dataset's footers found while the user was looking at a query, a pivot or
    /// a drill-down rather than at the data. Held rather than applied, because widening
    /// the scan under a query takes the query's own columns away, and offered again the
    /// moment the view comes back to the dataset itself.
    pub(crate) footers_held: Option<(u64, crate::table::FootersFound)>,
    /// Fields a followed pipe's NDJSON brought after the open, held as footers are
    /// until the view is back on the data.
    pub(crate) followed_fields_held: Option<(u64, Vec<polars::prelude::Field>)>,
    /// A re-read the dataset is owed by a footer pass that came back empty-handed, held
    /// back because the collect it goes through would bump the generation out from
    /// under work already running. The pass that failed brings no columns to hold, so
    /// `footers_held` has nothing to say about it, and the dataset still needs the
    /// ordinary count the pass was going to save it — hence an errand of its own, tried
    /// again after every event until the work it would cancel is done.
    pub(crate) reread_owed: Option<u64>,
    /// The objects a listing had found when this frame began, for the same reason.
    pub(crate) listed_this_frame: Option<usize>,
}

impl Counting {
    /// Forget what keys pressed at the dataset being replaced were waiting for: an End
    /// on its footers or its count. A `len_generation` says nothing about which dataset
    /// it belonged to, so a marker left here would act on the next one.
    pub(crate) fn reset_for_dataset(&mut self) {
        self.end_when_the_footers_land = None;
        self.end_after_count = None;
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

/// A frame's count markers: the count out for it, the one held for a paint, and the
/// one that failed.
#[derive(Clone, Copy)]
pub(crate) struct CountMarkers {
    pub(crate) len_count_inflight: Option<u64>,
    pub(crate) count_after_paint: Option<u64>,
    pub(crate) len_count_failed: Option<u64>,
}

/// Bytes of a text file indexed per step behind its first rows, between which the
/// indexing looks whether it is still wanted.
const INDEX_STEP: usize = 16 << 20;

impl App {
    /// Whether the dataset on screen is one that opened before its footers were read
    /// and is still waiting for them.
    ///
    /// Not the same question as whether a pass is running. The counter is shared with
    /// every open, and abandoning one does not stop it: without this, giving up on a
    /// large local directory and going back to the dataset you had would leave that
    /// dataset's footer counting footers belonging to the directory you left.
    /// Whether the row count on the footer is on its way, so a spinner stands in
    /// for it. Asked by the bar, and by the run loop, which turns the spinner: the
    /// two disagreed while a dataset read its own footers, and the spinner sat still.
    pub fn row_count_pending(&self) -> bool {
        // A load in flight counts as pending: the number `data_table_state` still holds
        // belongs to the dataset being replaced, and printing it beside the incoming
        // file's name would read as the new one's.
        // A dataset still reading its own footers counts too: it declines the ordinary
        // count because that pass is bringing one, so nothing is "in flight" — and the
        // number it holds meanwhile is only as far as the buffer reaches. Printed
        // plainly, a prefix of six thousand files reads `Rows: 70`.
        self.counting.len_count_inflight.is_some()
            || self.loading.awaiting_dataset()
            // A re-read owed to a dataset whose footers could not be read is a count
            // that is coming: the collect it is waiting to run is what starts one. The
            // dataset has already stopped saying it counts itself later (it gave up on
            // the pass the moment that pass failed), so without this the bar falls
            // through to printing the number it happens to hold — which is only as far
            // as the buffer reached. A prefix of six thousand files reads `Rows: 70`,
            // plainly, for as long as the work in front of the errand takes.
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

    /// Read the rows on screen again, now that the frame they were read through has
    /// been replaced.
    ///
    /// The dataset's own errand, not its open's: this happens long after the open has
    /// finished, and after a glance at the home screen just the same. The join has
    /// already dropped the buffer, so nothing dropping this leaves the table with no
    /// rows to show at the moment it was to show more of them.
    pub(crate) fn reread_after_the_footers_joined(&mut self) {
        // Any re-read satisfies one that was owed: this is the collect the errand was
        // waiting to run, whoever asked for it.
        self.counting.reread_owed = None;
        // End was pressed while the footers were still coming, and they are what the
        // end was waiting on. Taken either way: a flag left from a dataset that is gone
        // is not this one's to act on. The jump reads the page it lands on, so reading
        // the page here first would be one fetched to be thrown away.
        if self.counting.end_when_the_footers_land.take() == Some(self.dataset_generation) {
            self.status_message = None;
            if let Some(next) = self.jump_key(crate::Scroll::End) {
                // The jump reads the page it lands on, so reading this one first would
                // be a page fetched to be thrown away.
                let _ = self.events.send(next);
                return;
            }
            // Unless it asked for no read: the view was already at the end, or the pass
            // brought no count and the jump is waiting on the ordinary one. The join has
            // dropped the buffer either way, so falling through is the difference
            // between a table and an empty one.
        }
        self.spawn_async_collect(Self::LOADING_BUFFER);
    }

    /// Run a buffer collect that was asked for while other work was waiting on the
    /// generation.
    ///
    /// The same shape as `reread_when_the_work_allows` below, and for the same reason:
    /// the collect bumps `task_generation`, so it waits its turn and is tried again
    /// after every event.
    pub(crate) fn collect_when_the_work_allows(&mut self) {
        let Some(&Job::OwedRows { dataset, .. }) = self.jobs.owed(Self::owed_rows) else {
            return;
        };
        if dataset != self.dataset_generation {
            // The dataset it was owed to is gone, and so is the view it was filling.
            // Only the errand is put down, and the keys it held with it; the status line
            // belongs to whatever replaced the dataset.
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
            // The collect that was owed may have been an open's first rows. Left waiting
            // on them, the bar would read "Loading buffer... 70%" with the app idle for
            // the rest of the session.
            self.first_rows_settled();
        }
    }

    /// Run the re-read a failed footer pass owes the dataset, once it can be run
    /// without throwing another answer away.
    ///
    /// The re-read bumps `task_generation`, so it waits until no export or analysis
    /// is on the generation it would bump past (`work_the_join_would_cancel`), the way
    /// held columns do.
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

    /// Retire an End that was waiting on a count which can no longer answer it.
    ///
    /// Only the flag and the message it put up: the jump itself is not re-issued. See
    /// the caller in `BackgroundLenReady` for why asking again is the wrong repair.
    fn retire_the_end_that_was_waiting(&mut self) {
        self.counting.end_after_count = None;
        self.take_down_the_counting_status();
    }

    /// Take down "Counting rows to find the end...", and only that.
    ///
    /// Clearing the status outright would wipe whatever else is using the line — a
    /// load's phase, an export's progress — on behalf of a key pressed somewhere else.
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

    /// Work already running that the re-read after a join would cancel.
    ///
    /// The re-read goes through the ordinary collect, which bumps `task_generation`, so
    /// everything a bump would strand has to be done first — and that is
    /// [`Jobs::would_strand`]'s job now, rather than a list of the kinds of work that
    /// might be running.
    ///
    /// One thing more than a bump, though: a join takes a fresh `len_generation` too. A
    /// chart is prepared against the frame rather than the generation, so a bump cannot
    /// strand one but changing the frame under it can.
    pub(crate) fn work_the_join_would_cancel(&self) -> bool {
        self.work_a_bump_would_strand() || self.chart_preparing()
    }

    /// Give the dataset what its footers found, if it can take it now.
    ///
    /// It cannot while the user is looking at a query, a pivot, a melt or a drill-down:
    /// those make their own result the root, and widening the scan underneath one takes
    /// away the columns it is built from. So the columns wait — held, not dropped — and
    /// this is tried again after every event, which is the cheapest way to catch the
    /// moment the view comes back to the data.
    ///
    /// Returns whether the dataset took them, so the caller can re-read the rows on
    /// screen through the wider frame.
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
        // Whether this is the moment is the dataset's call, not this one's: it is the
        // frame on screen that knows whether it still grows from the scan.
        match state.join_dataset_schema(found) {
            Ok(()) => true,
            Err(found) => {
                self.counting.footers_held = Some((generation, *found));
                false
            }
        }
    }

    /// Start the pass that reads the rest of a staged open's footers.
    ///
    /// Not a job, which the user would wait on: the whole point of opening
    /// before every footer is read is that the dataset works while they are read. The
    /// generation is the dataset's rather than the task's, because a collect bumps the
    /// task's and this pass outlives several of them.
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
        // Answered either way: a pass that could not read them, or panicked, says so
        // too, or the dataset waits for it for good — and a waiting dataset will not
        // count itself, because the count was what the pass was bringing back.
        self.spawn_job(Job::FootersJoin { dataset }, None, move |_| {
            Ok(Answer::FootersJoined(join(&progress).map(Box::new)))
        });
    }

    /// Index the rest of a text file's lines behind its first rows, and say when they
    /// are all in ([`AppEvent::LinesIndexed`]). Not a job, which the user would wait on:
    /// the table works meanwhile, and a read of every line waits for them on its own
    /// worker. The last dataset's indexing, if it is still going, stops.
    pub(crate) fn start_indexing(&mut self) {
        self.counting.end_when_indexed = None;
        self.counting.goto_when_indexed = None;
        self.index_lines();
    }

    /// Run the indexing of the dataset on screen's lines, if they still have lines to
    /// index: a new dataset's, or one paused while home was up. Lines of a dataset no
    /// longer on screen stop for good, and the reads waiting on them give up.
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
        let generation = self.dataset_generation;
        let tx = self.events.clone();
        let waiting = lines.clone();
        let spawned = std::thread::Builder::new()
            .name("datui-index".to_string())
            .spawn(move || {
                loop {
                    // Paused or replaced: whoever stopped it says what becomes of the
                    // reads waiting on the lines.
                    if stop.load(Ordering::Relaxed) {
                        return;
                    }
                    // A panic stops it where it is: the rows so far are what there is,
                    // rather than a count that never comes.
                    let done =
                        logging::catch_panic(|| lines.index_more(INDEX_STEP)).unwrap_or(true);
                    if done {
                        lines.stop_indexing();
                        let rows = lines.rows();
                        let _ = tx.send(AppEvent::LinesIndexed { generation, rows });
                        return;
                    }
                }
            });
        // No thread to index them: the lines so far are what there is, and nothing
        // waits for more.
        if spawned.is_err() {
            waiting.stop_indexing();
            self.counting.indexing_lines = None;
            if let Some(state) = self.data_table_state.as_mut() {
                state.lines_indexed(waiting.rows());
            }
        }
    }

    /// Home is up: the indexing waits, the reads waiting on it with it, until the
    /// table is back ([`Self::begin_frame`]).
    pub(crate) fn pause_indexing(&mut self) {
        if self.counting.indexing_lines.is_some() {
            self.counting
                .indexing_stop
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.counting.indexing_paused = true;
        }
    }

    /// More of the dataset's lines are indexed: its frames take them, and once all are,
    /// its count and an End that waited for it.
    fn lines_indexed(&mut self, generation: u64, rows: usize) {
        if generation != self.dataset_generation {
            return;
        }
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        self.counting.indexing_lines = None;
        if !state.lines_indexed(rows) {
            // Set aside while the lines finished (the quality evidence view): they
            // land on the dataset that comes back.
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
        // The count the indexing held back starts now, and rows past the first ones
        // read are read.
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

    /// The dataset's row count from a sample of its footers, while it is not counted:
    /// the dataset's own, or the one its footer pass has said so far.
    pub(crate) fn row_estimate(&self) -> Option<crate::schema_union::RowEstimate> {
        self.data_table_state
            .as_ref()?
            .row_estimate(self.counting.footer_progress.estimate())
    }

    /// Whether the count running reads footers it can say it has read, and so can be
    /// stopped: `(read, of)`.
    pub(crate) fn footers_counted(&self) -> Option<(usize, usize)> {
        self.counting.len_count_inflight?;
        self.counting
            .count_progress
            .reading()
            .filter(|_| !self.counting.count_progress.is_cancelled())
    }

    /// `c` in the Info panel: count every row exactly, though the dataset has more
    /// files than the count reads unasked.
    pub(crate) fn count_exactly(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        if state.is_num_rows_valid() {
            return;
        }
        let generation = state.len_generation();
        self.counting.exact_count_asked = Some(self.dataset_generation);
        // The footer pass is still bringing the count; the request holds for when it
        // lands.
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

    /// What the footer pass found, for the dataset it was started for: joined when that
    /// is still the dataset on screen, which going home and back leaves it, rather than
    /// whether an open is in flight.
    pub(crate) fn footers_joined(
        &mut self,
        dataset: u64,
        found: Option<crate::table::FootersFound>,
    ) -> Option<AppEvent> {
        if dataset == self.dataset_generation {
            let Some(found) = found else {
                // The pass could not read them. The dataset stays as it opened
                // and stops waiting, so it can go and count itself the ordinary
                // way rather than never at all — which is what the collect
                // below sets going, since it is the counting the dataset was
                // declining while it waited.
                if let Some(state) = self.data_table_state.as_mut() {
                    state.give_up_on_pending_footers();
                }
                // The pass is not bringing a count after all, so the jump goes
                // back to waiting on the ordinary one the collect starts. Owed
                // rather than run: the collect bumps `task_generation`, and an
                // export or an analysis may be waiting on the one it would bump
                // past. `reread_when_the_work_allows` runs it the moment that
                // work is done.
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

    /// Whether the rows of the frame on screen that someone is waiting for are still
    /// being read: the page an open, a query or a scroll asked for. A load-ahead is
    /// nobody's wait, so a count does not queue behind one.
    fn waited_on_rows_pending(&self, generation: u64) -> bool {
        self.loading.awaiting_dataset()
            || self.jobs.owed(Self::owed_rows).is_some()
            || (self.rows_waited_on()
                && self
                    .rows_in_flight()
                    .is_some_and(|inflight| inflight.dataset == generation))
    }

    /// Whether a frame painted now would start, or retire, the count waiting on one.
    /// The run loop paints after every update; a test harness, which paints nothing,
    /// asks this to know when to say a frame was painted.
    pub fn count_waits_for_a_frame(&self) -> bool {
        self.counting
            .count_after_paint
            .is_some_and(|generation| !self.waited_on_rows_pending(generation))
    }

    /// A frame has been painted. Read the rows it found it needed (it set the rows on
    /// screen, or a change asked for them), and start the count waiting on it.
    pub fn frame_painted(&mut self) {
        self.pointer.painted();
        self.count_what_was_painted();
        // A resize sets the rows on screen as it draws, after the event pass looked;
        // matches worked out again are drawn on the frame the wake brings.
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

    /// Start the count that was waiting for a frame's rows to be on screen, unless they
    /// are still being read; retire it if the frame it was for has gone or its rows
    /// already said how many there are.
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

    /// The page just installed may have said how many rows there are, or belong to a
    /// frame other than the one a count is waiting on: either way that count is not
    /// owed any more.
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

    /// Row counts, footer passes and line indexing answering, and the frame that waited on them.
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
                // Apply the exact total only if the data hasn't changed since the count
                // was spawned. This runs independently of the buffer paint (which has
                // usually already rendered), so it just corrects the scrollbar/total —
                // no busy state, no re-collect.
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
                    // This is the count End was waiting on, and it answers a frame that
                    // is gone — a join landed underneath it and took a fresh
                    // `len_generation` past it. Left here the flag is stranded on a
                    // generation nothing will ever match: the next count to fail for any
                    // reason would speak in its name. So it is retired, and the status
                    // it put up comes down with it.
                    //
                    // Retired, not asked again of the frame that is here. That frame can
                    // belong to a dataset the user opened since — `end_after_count` names
                    // a `len_generation`, which says nothing about which dataset — and
                    // re-asking made the *new* dataset scroll itself to the end on the
                    // strength of a key pressed in the old one. A jump the frame change
                    // swallowed is a jump the user can make again; a jump that arrives on
                    // its own, in a directory they did not press it in, is not.
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
                // Mark this generation's count as failed so the row count renders as "?"
                // instead of a misleading provisional total. Before the End handling
                // below: this is about the count, not about who was waiting on it.
                //
                // Only for the frame on screen, because the slot holds one generation.
                // Counts for two frames run at once — a join, a query, a filter or a
                // sort takes a fresh `len_generation` without stopping the count already
                // running — so a failure arriving is not necessarily this frame's.
                // Written unconditionally, an orphan's failure overwrote a live frame's,
                // `count_unknown` went false, and the bar fell through from "?" to the
                // number the buffer happened to reach: a confident partial on a dataset
                // whose count failed. The orphan's own failure is worth nothing to
                // anybody — nothing will ever render against a generation that is gone.
                if self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.len_generation() == len_generation)
                {
                    self.counting.len_count_failed = Some(len_generation);
                }
                // Only for the count End was actually waiting on. Taken unconditionally,
                // a count that failed for one frame answered for an End pressed on
                // another — printing "Could not count the rows to find the end" about a
                // key the user pressed somewhere else entirely, and long since.
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
                        // The frame it was counting is gone, so its failure says nothing
                        // about the one on screen, and the End it belonged to cannot be
                        // answered by it. Retired quietly, as above.
                        self.take_down_the_counting_status();
                    }
                }
                None
            }
            AppEvent::LinesIndexed { generation, rows } => {
                self.lines_indexed(generation, rows);
                None
            }
            _ => unreachable!("not an event for counting_event"),
        }
    }

    /// Count, behind the Info panel, the values the read's column types made null, for
    /// the Notes: one pass over the frame before the types, the first time the panel
    /// opens on a dataset with typed columns.
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
