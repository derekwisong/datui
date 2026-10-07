//! The hex view's find: asked again, run off the UI thread, and its answer.

use crate::app::hex_view::{Found, HexFindRun, HexHit};
use crate::app::jobs::{Answer, Job, Progress};
use crate::{App, AppEvent};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

impl App {
    /// `n` and `N`: the next or previous match of the last pattern, from the cursor.
    pub(crate) fn hex_find_again(&mut self, forward: bool) -> Option<AppEvent> {
        let view = self.hex.view.as_ref()?;
        let pattern = view.found.as_ref()?.pattern.clone();
        let len = view.len();
        if len == 0 {
            return None;
        }
        let from = if forward {
            if view.cursor + 1 >= len {
                0
            } else {
                view.cursor + 1
            }
        } else if view.cursor == 0 {
            len - 1
        } else {
            view.cursor - 1
        };
        self.start_hex_find(pattern, from, forward);
        None
    }

    /// Find `pattern` from `from` on a worker. The keys wait, and Esc stops it.
    pub(crate) fn start_hex_find(
        &mut self,
        pattern: crate::app::hex_view::Pattern,
        from: u64,
        forward: bool,
    ) {
        self.stop_hex_find();
        let Some(view) = self.hex.view.as_mut() else {
            return;
        };
        let stop = Arc::new(AtomicBool::new(false));
        let run = HexFindRun {
            stop: stop.clone(),
            view: view.serial,
            pattern: pattern.clone(),
        };
        let bytes = view.bytes.clone();
        let total = view.len();
        let status = format!("Finding {}...", pattern.label);
        self.spawn_job(Job::HexFind(run), Some(&status), move |worker| {
            let report = worker.reporter();
            let hay = bytes.as_slice();
            let mut hit = crate::app::hex_view::find(hay, &pattern, from, forward, &stop, |read| {
                report(Progress::HexFinding { read, total })
            })
            .map_err(|_| crate::find::CANCELLED.to_string())?;
            hit.stride = hit
                .at
                .and_then(|at| crate::app::hex_view::stride(hay, &pattern, at, &stop));
            Ok(Answer::HexFound(hit))
        });
    }

    /// A find's progress through the file.
    pub(crate) fn hex_find_progress(&mut self, read: u64, total: u64) {
        let Some(label) = self
            .jobs
            .current(|job| matches!(job, Job::HexFind(_)))
            .and_then(|(_, job)| match job {
                Job::HexFind(run) => Some(run.pattern.label.clone()),
                _ => None,
            })
        else {
            return;
        };
        let percent = (read.min(total) * 100).checked_div(total).unwrap_or(100);
        self.status_message = Some(format!("Finding {label}... {percent}%"));
    }

    /// A find answered: the cursor goes to the match.
    pub(crate) fn hex_found(&mut self, run: HexFindRun, hit: HexHit) {
        self.status_message = None;
        let Some(view) = self.hex.view.as_mut().filter(|v| v.serial == run.view) else {
            return;
        };
        view.found = Some(Found {
            pattern: run.pattern.clone(),
            hit: hit.at,
            stride: hit.stride,
        });
        match hit.at {
            Some(at) => {
                view.go(at);
                if hit.wrapped {
                    self.flash_note(format!("Found {}, round past the end", run.pattern.label));
                }
            }
            None => self.flash_note(format!("No match for {}", run.pattern.label)),
        }
    }

    /// Stop the hex view's find, if one is running. Returns whether one was.
    pub(crate) fn stop_hex_find(&mut self) -> bool {
        let Some((_, Job::HexFind(run))) = self.jobs.current(|job| matches!(job, Job::HexFind(_)))
        else {
            return false;
        };
        run.stop.store(true, Ordering::Relaxed);
        self.jobs.cancel(|job| matches!(job, Job::HexFind(_)));
        self.status_message = None;
        true
    }
}
