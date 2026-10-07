//! `DATUI_TRACE_FIRST_ROWS=PATH`: when the first frame showing a dataset's rows has been
//! drawn, write the wall-clock time to PATH as one line of Unix nanoseconds, once.
//!
//! For `scripts/bench/startup.py`, which times a launch from spawn to that line. Wall
//! clock rather than a monotonic one, because the reader is another process. Unset, it
//! costs one environment read per run.

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::App;
use crate::render::main_view::MainViewContent;

pub const ENV: &str = "DATUI_TRACE_FIRST_ROWS";

pub(crate) struct FirstRowsTrace {
    path: Option<PathBuf>,
}

impl FirstRowsTrace {
    pub(crate) fn from_env() -> Self {
        Self::to(
            std::env::var_os(ENV)
                .filter(|p| !p.is_empty())
                .map(PathBuf::from),
        )
    }

    fn to(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    /// Call after each frame is drawn.
    pub(crate) fn painted(&mut self, app: &App) {
        if self.path.is_some() {
            self.record_if(rows_on_screen(app));
        }
    }

    fn record_if(&mut self, shown: bool) {
        if !shown {
            return;
        }
        let Some(path) = self.path.take() else {
            return;
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        // A benchmark aid: a write that fails leaves the run untimed, nothing more.
        if let Err(e) = std::fs::write(&path, format!("{now}\n")) {
            log::warn!(target: "datui", "{ENV}: could not write {}: {e}", path.display());
        }
    }
}

/// The table is the view on screen and its buffer holds at least one row.
fn rows_on_screen(app: &App) -> bool {
    MainViewContent::current(app) == MainViewContent::Datatable
        && app
            .data_table_state
            .as_ref()
            .and_then(|state| state.display_df())
            .is_some_and(|df| df.height() > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_one_line_at_the_first_frame_with_rows_and_never_again() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("first-rows");
        let mut trace = FirstRowsTrace::to(Some(path.clone()));
        trace.record_if(false);
        assert!(!path.exists(), "a frame without rows is not recorded");
        trace.record_if(true);
        let first = std::fs::read_to_string(&path).unwrap();
        let ns: u128 = first.trim().parse().unwrap();
        assert!(ns > 0 && first.ends_with('\n') && first.lines().count() == 1);
        std::fs::remove_file(&path).unwrap();
        trace.record_if(true);
        assert!(!path.exists(), "only the first frame with rows is recorded");
    }

    #[test]
    fn unset_records_nothing() {
        let mut trace = FirstRowsTrace::to(None);
        trace.record_if(true);
        assert!(trace.path.is_none());
    }
}
