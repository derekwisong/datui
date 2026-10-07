//! Row counts, footer passes and line indexing behind a dataset's first rows, and
//! what waits on them: an End, a `:N`, the status line.

use crate::background::{LenCount, OwedCount};
use crate::jobs::{Answer, Job};
use crate::table::DataTableState;
use crate::{App, AppEvent, logging};
use std::sync::Arc;

/// Bytes of a text file indexed per step behind its first rows, between which the
/// indexing looks whether it is still wanted.
const INDEX_STEP: usize = 16 << 20;

/// What a pass behind a staged open reported, and which dataset it was reading for.
/// `None` where the footers are: a pass that could not read them says so, so the
/// dataset stops waiting.
pub(crate) type FootersReported = Option<(u64, Option<crate::table::FootersFound>)>;

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
        self.len_count_inflight.is_some()
            || self.loading.awaiting_dataset()
            // A re-read owed to a dataset whose footers could not be read is a count
            // that is coming: the collect it is waiting to run is what starts one. The
            // dataset has already stopped saying it counts itself later (it gave up on
            // the pass the moment that pass failed), so without this the bar falls
            // through to printing the number it happens to hold — which is only as far
            // as the buffer reached. A prefix of six thousand files reads `Rows: 70`,
            // plainly, for as long as the work in front of the errand takes.
            || self.reread_owed.is_some()
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
        self.reread_owed = None;
        // End was pressed while the footers were still coming, and they are what the
        // end was waiting on. Taken either way: a flag left from a dataset that is gone
        // is not this one's to act on. The jump reads the page it lands on, so reading
        // the page here first would be one fetched to be thrown away.
        if self.end_when_the_footers_land.take() == Some(self.dataset_generation) {
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
    /// The failure branch of `BackgroundFootersJoined` used to re-read on the spot,
    /// which bumped `task_generation` with no check at all — the one path into the
    /// collect that never asked `work_the_join_would_cancel`. An export in its collect
    /// phase then never wrote its file and said nothing about it. So the errand waits
    /// its turn, the way held columns already do.
    pub(crate) fn reread_when_the_work_allows(&mut self) {
        let Some(generation) = self.reread_owed else {
            return;
        };
        if generation != self.dataset_generation {
            // The dataset it was owed to is gone; so is the errand.
            self.reread_owed = None;
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
        self.end_after_count = None;
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
    /// chart is prepared against the frame rather than the generation
    /// (`BackgroundChartReady` carries no generation at all), so a bump cannot strand
    /// one but changing the frame under it can.
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
        let Some((generation, _)) = self.footers_held.as_ref() else {
            return false;
        };
        if *generation != self.dataset_generation {
            // The dataset they belong to is gone; so are they.
            self.footers_held = None;
            return false;
        }
        if self.data_table_state.is_none() || self.work_the_join_would_cancel() {
            return false;
        }
        let Some((generation, found)) = self.footers_held.take() else {
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
                self.footers_held = Some((generation, *found));
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
        let generation = self.dataset_generation;
        let slot = self.pending_footers_result.clone();
        let tx = self.events.clone();
        let progress = self.footer_progress.clone();
        self.runtime.spawn_blocking(move || {
            // Reported either way. A pass that could not read them has to say so, or
            // the dataset waits for it for the rest of the session — and a waiting
            // dataset is one that will not count itself, because the count was what
            // the pass was bringing back. A pass that panicked could not read them.
            let found = logging::catch_panic(|| join(&progress)).unwrap_or(None);
            if !Self::record_footers(&slot, generation, found) {
                return;
            }
            let _ = tx.send(AppEvent::BackgroundFootersJoined { generation });
        });
    }

    /// Index the rest of a text file's lines behind its first rows, and say when they
    /// are all in ([`AppEvent::LinesIndexed`]). Not a job, which the user would wait on:
    /// the table works meanwhile, and a read of every line waits for them on its own
    /// worker. The last dataset's indexing, if it is still going, stops.
    pub(crate) fn start_indexing(&mut self) {
        self.end_when_indexed = None;
        self.goto_when_indexed = None;
        self.index_lines();
    }

    /// Run the indexing of the dataset on screen's lines, if they still have lines to
    /// index: a new dataset's, or one paused while home was up. Lines of a dataset no
    /// longer on screen stop for good, and the reads waiting on them give up.
    pub(crate) fn index_lines(&mut self) {
        use std::sync::atomic::Ordering;
        self.indexing_stop.store(true, Ordering::Relaxed);
        self.indexing_paused = false;
        let lines = self
            .data_table_state
            .as_ref()
            .and_then(|state| state.lines_to_index().cloned());
        if let Some(old) = self.indexing_lines.take()
            && lines.as_ref().is_none_or(|lines| !Arc::ptr_eq(lines, &old))
        {
            old.stop_indexing();
        }
        let Some(lines) = lines.filter(|lines| lines.resume_indexing()) else {
            return;
        };
        self.indexing_lines = Some(lines.clone());
        let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
        self.indexing_stop = stop.clone();
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
            self.indexing_lines = None;
            if let Some(state) = self.data_table_state.as_mut() {
                state.lines_indexed(waiting.rows());
            }
        }
    }

    /// Home is up: the indexing waits, the reads waiting on it with it, until the
    /// table is back ([`Self::begin_frame`]).
    pub(crate) fn pause_indexing(&mut self) {
        if self.indexing_lines.is_some() {
            self.indexing_stop
                .store(true, std::sync::atomic::Ordering::Relaxed);
            self.indexing_paused = true;
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
        self.indexing_lines = None;
        if !state.lines_indexed(rows) {
            // Set aside while the lines finished (the quality evidence view): they
            // land on the dataset that comes back.
            if let Some(held) = self.quality_evidence_return.as_mut() {
                held.lines_indexed(rows);
            }
            return;
        }
        if let Some((goto, row)) = self.goto_when_indexed.take()
            && goto == generation
        {
            self.take_down_the_counting_status();
            let _ = self.events.send(AppEvent::GoToLine(row));
        }
        if self.end_when_indexed.take() == Some(generation) {
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
            && self.exact_count_asked != Some(self.dataset_generation)
            && state.row_estimate(None).is_some()
    }

    /// The dataset's row count from a sample of its footers, while it is not counted:
    /// the dataset's own, or the one its footer pass has said so far.
    pub(crate) fn row_estimate(&self) -> Option<crate::schema_union::RowEstimate> {
        self.data_table_state
            .as_ref()?
            .row_estimate(self.footer_progress.estimate())
    }

    /// Whether the count running reads footers it can say it has read, and so can be
    /// stopped: `(read, of)`.
    pub(crate) fn footers_counted(&self) -> Option<(usize, usize)> {
        self.len_count_inflight?;
        self.count_progress
            .reading()
            .filter(|_| !self.count_progress.is_cancelled())
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
        self.exact_count_asked = Some(self.dataset_generation);
        // The footer pass is still bringing the count; the request holds for when it
        // lands.
        if state.counts_itself_later() {
            return;
        }
        // A count stopped before is asked again.
        if self.len_count_failed == Some(generation) {
            self.len_count_failed = None;
        }
        // One stopped and not yet wound down: again once it has.
        if self.len_count_inflight == Some(generation) && self.count_progress.is_cancelled() {
            self.count_after_stop = Some(generation);
            return;
        }
        if self.len_count_inflight != Some(generation) {
            self.len_count_inflight = Some(generation);
            let job = LenCount::for_state(state);
            self.spawn_count(job);
        }
    }

    /// Esc while a count reads footers: stop it. What it read is kept for the next.
    pub(crate) fn stop_count(&mut self) {
        self.count_progress.cancel();
    }

    /// Put what a pass found in the slot, unless a later dataset's pass has answered
    /// first. Returns whether it went in, so a pass that lost does not also announce
    /// itself.
    ///
    /// Two passes can be in flight at once — opening a second large prefix does not
    /// stop the first one reading — and they finish in whatever order the network
    /// gives. Without this the slower, older one overwrites the newer entry, and the
    /// generation the event carries then disagrees with the generation in the slot,
    /// so both are discarded and the dataset on screen never gets its columns.
    pub(crate) fn record_footers(
        slot: &std::sync::Mutex<FootersReported>,
        generation: u64,
        found: Option<crate::table::FootersFound>,
    ) -> bool {
        let mut slot = slot.lock().unwrap_or_else(|e| e.into_inner());
        if slot.as_ref().is_some_and(|(held, _)| *held > generation) {
            return false;
        }
        *slot = Some((generation, found));
        true
    }

    /// Count the rows off the UI thread; the answer comes back as `BackgroundLenReady`
    /// or `BackgroundLenFailed`.
    pub(crate) fn spawn_count(&mut self, job: LenCount) {
        #[cfg(test)]
        self.counts_spawned.set(self.counts_spawned.get() + 1);
        self.count_progress = job.progress.clone();
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
        self.count_after_paint
            .is_some_and(|generation| !self.waited_on_rows_pending(generation))
    }

    /// A frame has been painted. Start the count that was waiting for its rows to be on
    /// screen, unless they are still being read; retire it if the frame it was for has
    /// gone or its rows already said how many there are.
    pub fn frame_painted(&mut self) {
        self.pointer.painted();
        let Some(generation) = self.count_after_paint else {
            return;
        };
        if self.waited_on_rows_pending(generation) {
            return;
        }
        self.count_after_paint = None;
        let wanted = self
            .data_table_state
            .as_ref()
            .filter(|state| state.len_generation() == generation && !state.is_num_rows_valid());
        match wanted {
            Some(state) => {
                self.len_count_inflight = Some(generation);
                self.spawn_count(LenCount::for_state(state));
            }
            None => {
                if self.len_count_inflight == Some(generation) {
                    self.len_count_inflight = None;
                }
            }
        }
    }

    /// The page just installed may have said how many rows there are, or belong to a
    /// frame other than the one a count is waiting on: either way that count is not
    /// owed any more.
    pub(crate) fn retire_a_count_the_rows_answered(&mut self) {
        let Some(generation) = self.count_after_paint else {
            return;
        };
        let answered = self
            .data_table_state
            .as_ref()
            .is_none_or(|state| state.len_generation() != generation || state.is_num_rows_valid());
        if answered {
            self.count_after_paint = None;
            if self.len_count_inflight == Some(generation) {
                self.len_count_inflight = None;
            }
        }
    }

    /// Row counts, footer passes and line indexing answering, and the frame that waited on them.
    pub(crate) fn counting_event(&mut self, event: &AppEvent) -> Option<AppEvent> {
        match event {
            AppEvent::BackgroundLenReady {
                len_generation,
                num_rows,
                file_row_groups,
            } => {
                if self.len_count_inflight == Some(*len_generation) {
                    self.len_count_inflight = None;
                }
                if self.len_count_failed == Some(*len_generation) {
                    self.len_count_failed = None;
                }
                // A count of the view a running query replaced goes back with it.
                if let Some(run) = self.query_running.as_mut() {
                    run.rollback.count_landed(
                        *len_generation,
                        *num_rows,
                        file_row_groups.as_deref(),
                    );
                    if run.len_count_inflight == Some(*len_generation) {
                        run.len_count_inflight = None;
                    }
                }
                // Apply the exact total only if the data hasn't changed since the count
                // was spawned. This runs independently of the buffer paint (which has
                // usually already rendered), so it just corrects the scrollbar/total —
                // no busy state, no re-collect.
                if let Some(state) = self.data_table_state.as_mut()
                    && state.count_landed(*len_generation, *num_rows, file_row_groups.as_deref())
                {
                    // End was pressed before there was an end to go to.
                    if self.end_after_count == Some(*len_generation) {
                        self.end_after_count = None;
                        self.status_message = None;
                        return self.jump_key(crate::Scroll::End);
                    }
                } else if self.end_after_count == Some(*len_generation) {
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
                if self.len_count_inflight == Some(*len_generation) {
                    self.len_count_inflight = None;
                }
                if self.count_after_stop.take() == Some(*len_generation) {
                    self.count_exactly();
                    return None;
                }
                if let Some(run) = self.query_running.as_mut()
                    && run.len_count_inflight == Some(*len_generation)
                {
                    run.len_count_inflight = None;
                    run.len_count_failed = Some(*len_generation);
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
                    .is_some_and(|state| state.len_generation() == *len_generation)
                {
                    self.len_count_failed = Some(*len_generation);
                }
                // Only for the count End was actually waiting on. Taken unconditionally,
                // a count that failed for one frame answered for an End pressed on
                // another — printing "Could not count the rows to find the end" about a
                // key the user pressed somewhere else entirely, and long since.
                if self.end_after_count == Some(*len_generation) {
                    self.end_after_count = None;
                    if self
                        .data_table_state
                        .as_ref()
                        .is_some_and(|state| state.len_generation() == *len_generation)
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
                self.lines_indexed(*generation, *rows);
                None
            }
            AppEvent::BackgroundFootersJoined { .. } => {
                // Taken whoever the event belongs to, and judged by what is *in* the
                // slot rather than by the event that woke us. Two passes can be running
                // at once, and the newer one may have overwritten the slot before the
                // older one's event is handled: judging by the event would throw the
                // newer answer away and leave the dataset on screen waiting for one
                // that has already been and gone. An entry is also worth draining
                // either way — it is a dataset's worth of schema and every file name.
                let taken = self
                    .pending_footers_result
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .take();
                // Whether this is still the dataset on screen. Not whether an open is in
                // flight: going home leaves the dataset up and puts any open down, and
                // coming straight back to it must not find it stranded on two footers
                // for the rest of the session.
                if let Some((slot_generation, found)) = taken
                    && slot_generation == self.dataset_generation
                {
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
                        self.reread_owed = Some(slot_generation);
                        self.reread_when_the_work_allows();
                        return None;
                    };
                    self.footers_held = Some((slot_generation, found));
                    if self.join_held_footers() {
                        self.reread_after_the_footers_joined();
                    }
                }
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
