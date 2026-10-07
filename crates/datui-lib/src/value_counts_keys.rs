//! Value Counts: counting a column's values off the UI thread, its keys, and drilling,
//! copying or exporting from it.

use crate::form::ListMove;
use crate::jobs::{Answer, Job};
use crate::{App, AppEvent, Overlay, clipboard, copy_modal, value_counts, value_counts_modal};
use crossterm::event::{KeyCode, KeyEvent};
use std::sync::Arc;

impl App {
    /// `F` at the table: Value Counts for the column cursor's column.
    pub(crate) fn open_value_counts(&mut self) {
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let names = state.get_column_order().to_vec();
        let Some(at) = state
            .current_column()
            .and_then(|current| names.iter().position(|n| n == current))
        else {
            return;
        };
        self.value_counts.open(names, at, state.len_generation());
        self.open_overlay(Overlay::ValueCounts);
        self.count_values(false);
    }

    /// Whether the Value Counts screen is up: on its own, or under the export
    /// dialog writing its counts.
    pub(crate) fn value_counts_shown(&self) -> bool {
        self.overlay == Overlay::ValueCounts
            || (self.overlay == Overlay::Export && self.export_counts.is_some())
    }

    /// Whether a count for the Value Counts screen is being read while it is up.
    pub(crate) fn value_counts_computing(&self) -> bool {
        self.overlay == Overlay::ValueCounts && self.value_counts.computing.is_some()
    }

    /// Where the export dialog goes back to: Value Counts when it is writing them.
    pub(crate) fn export_returns_to(&self) -> Overlay {
        if self.export_counts.is_some() {
            Overlay::ValueCounts
        } else {
            Overlay::None
        }
    }

    /// Count the column on the Value Counts screen, unless its counts are already
    /// held: quickly, or every row when `exact`. Off the UI thread, without holding
    /// the keys, so stepping to another column or Esc stops it.
    fn count_values(&mut self, exact: bool) {
        let Some(column) = self.value_counts.column().map(str::to_string) else {
            return;
        };
        let held_sample = self.value_counts.current().map(|c| c.is_sample());
        if held_sample == Some(false) || (held_sample == Some(true) && !exact) {
            return;
        }
        if self
            .value_counts
            .computing
            .as_ref()
            .is_some_and(|c| c.column == column && (c.exact || !exact))
        {
            return;
        }
        self.stop_value_count();
        let Some(state) = self.data_table_state.as_ref() else {
            return;
        };
        let read = if exact {
            value_counts::Read::Exact
        } else {
            value_counts::Read::Quick {
                sample_rows: self.app_config.analysis.sample_rows,
                seed: self.analysis_modal.sample.seed,
                remote: state.is_remote_source(),
            }
        };
        let plan = value_counts::Plan {
            lf: state.analysis_lf(),
            column: column.clone(),
            read,
            known_total: state.num_rows_if_valid(),
            streaming: state.polars_streaming(),
        };
        let watch = crate::sampling::ReadWatch::default();
        self.value_counts.failed = None;
        self.value_counts.computing = Some(value_counts_modal::Computing {
            column,
            exact,
            watch: watch.clone(),
            file_starts: state.file_row_starts().map(Arc::new),
        });
        self.spawn_job(Job::ValueCounts, None, move |_| {
            plan.run(&watch)
                .map(|counts| Answer::ValueCounts(Box::new(counts)))
                .map_err(|e| crate::error_display::user_message_from_report(&e, None))
        });
    }

    /// Stop the count in flight, if one is: its read stops at its next batch and its
    /// answer is dropped.
    pub(crate) fn stop_value_count(&mut self) {
        if let Some(computing) = self.value_counts.computing.take() {
            computing.watch.stop();
            self.jobs.cancel(|job| matches!(job, Job::ValueCounts));
        }
    }

    /// Keys on the Value Counts screen.
    pub(crate) fn value_counts_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        // The histogram has no lines to move through or drill into.
        let listing = !self.value_counts.shows_histogram();
        if listing && let Some(step) = ListMove::from_key(event) {
            match step {
                ListMove::Home => self.value_counts.move_to_start(),
                ListMove::End => self.value_counts.move_to_end(),
                _ => self
                    .value_counts
                    .move_by(step.delta(self.value_counts_page())),
            }
            return None;
        }
        match event.code {
            // A count still reading stops; with nothing to show for the column, Esc
            // goes on back to the table.
            KeyCode::Esc => {
                let counting = self.value_counts.counting();
                self.stop_value_count();
                if counting && self.value_counts.current().is_some() {
                    self.flash_note("Count stopped".to_string());
                } else {
                    self.overlay = Overlay::None;
                }
            }
            KeyCode::Char('G') if listing => self.value_counts.move_to_end(),
            KeyCode::Left | KeyCode::Right | KeyCode::Char('h') | KeyCode::Char('l') => {
                let by = if matches!(event.code, KeyCode::Left | KeyCode::Char('h')) {
                    -1
                } else {
                    1
                };
                if self.value_counts.step(by) {
                    // The table follows, so Esc lands on the column last counted.
                    if let (Some(state), Some(column)) =
                        (self.data_table_state.as_mut(), self.value_counts.column())
                    {
                        state.set_current_column(column);
                    }
                    self.count_values(false);
                }
            }
            KeyCode::Char('s') if listing => self.value_counts.toggle_order(),
            KeyCode::Char('c') => self.value_counts.toggle_view(),
            KeyCode::Char('a') => {
                if self.value_counts.current().is_some_and(|c| c.is_sample()) {
                    self.count_values(true);
                }
            }
            KeyCode::Enter if listing => self.drill_into_counted_value(),
            KeyCode::Char('y') => self.copy_value_counts(),
            KeyCode::Char('e') => self.export_value_counts(),
            // The counts keep the rows they were read of; `t` counts the new ones too.
            KeyCode::Char('t') if self.follow_rows_waiting() => {
                self.stop_value_count();
                self.take_follow_rows(false);
                if let Some(state) = self.data_table_state.as_ref() {
                    let names = self.value_counts.columns.clone();
                    let at = self.value_counts.at;
                    self.value_counts.open(names, at, state.len_generation());
                }
                self.count_values(false);
            }
            _ => {}
        }
        None
    }

    /// Lines a page of the Value Counts listing moves: as many as the last frame drew.
    fn value_counts_page(&self) -> usize {
        self.value_counts.page.max(1)
    }

    /// Enter on Value Counts: the rows holding the value under the cursor, as a drill.
    fn drill_into_counted_value(&mut self) {
        let Some(kind) = self.value_counts.selected_kind() else {
            return;
        };
        let (Some(counts), Some(column)) = (
            self.value_counts.current().cloned(),
            self.value_counts.column().map(str::to_string),
        ) else {
            return;
        };
        let value = match kind {
            value_counts::LineKind::Value(at) => match counts.value(at) {
                Ok(value) => value,
                Err(_) => return,
            },
            value_counts::LineKind::Null => polars::prelude::AnyValue::Null,
            value_counts::LineKind::Other(_) => {
                self.flash_note("Other is many values: pick one to see its rows".to_string());
                return;
            }
        };
        let Some(state) = self.data_table_state.as_mut() else {
            return;
        };
        let nested = state.is_drilled_down();
        match state.deferred(|s| s.drill_into_value(&column, value)) {
            Ok(()) => {
                self.stop_value_count();
                // Inside a group already, Esc goes back past this view to the one
                // the group came from, so there are no counts to come back to.
                self.value_counts.drill_return = !nested;
                self.overlay = Overlay::None;
                self.sync_sort_filter_modal();
                self.spawn_async_collect(Self::LOADING_BUFFER);
            }
            Err(e) => self.flash_note(format!(
                "Could not drill in: {}",
                crate::error_display::user_message_from_report(&e, None)
            )),
        }
    }

    /// `y` on Value Counts: every value with its count and percentages, as TSV.
    fn copy_value_counts(&mut self) {
        let Some(counts) = self.value_counts.current().cloned() else {
            return;
        };
        let order = self.value_counts.order;
        let html = match self.copy_destination() {
            Ok(destination) => destination.accepts().html,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
            let table = counts
                .table(order)
                .map_err(|e| format!("Copy failed: {e}"))?;
            let payload =
                crate::clipboard::tabular_payload(&table, clipboard::CopyFormat::Tsv, true, html)
                    .map_err(|e| format!("Copy failed: {e}"))?;
            Ok(Answer::Copied {
                payload,
                message: format!(
                    "Copied {} values as TSV",
                    copy_modal::thousands(table.height())
                ),
            })
        });
    }

    /// `e` on Value Counts: the export dialog, writing the counts.
    fn export_value_counts(&mut self) {
        let Some(counts) = self.value_counts.current() else {
            return;
        };
        let table = match counts.table(self.value_counts.order) {
            Ok(table) => table,
            Err(e) => {
                self.error_modal
                    .show(format!("Cannot export the counts: {e}"));
                return;
            }
        };
        self.export_modal.open(
            self.source.original_file_format,
            self.display.history_limit,
            &self.theme,
            self.source.original_file_delimiter,
        );
        let stem = self.dataset_stem();
        self.export_modal.suggest_path(&format!("{stem}-counts"));
        self.export_modal.offer_source_file = false;
        self.export_modal.nested_columns = false;
        self.export_modal.avro_renames = table
            .columns()
            .iter()
            .any(|c| crate::avro_types::renames(c.name(), c.dtype()));
        self.export_counts = Some(table);
        self.open_overlay(Overlay::Export);
    }
}
