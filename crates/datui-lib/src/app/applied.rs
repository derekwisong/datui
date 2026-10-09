//! What a dialog or prompt asks of the app once it is answered: one event,
//! [`AppEvent::Applied`], carries every such result back into the loop, so the frame
//! that closes the dialog is drawn before the work starts.

use crate::app::jobs::{Answer, Job, Progress};
use crate::app::modals::filter_modal::FilterStatement;
use crate::app::modals::pivot_melt_modal::{MeltSpec, PivotSpec};
use crate::chart::chart_export::ChartExportRequest;
use crate::config::QueryMode;
use crate::export::ExportRequest;
use crate::{App, AppEvent, ExportProgress, Overlay, app};
use std::path::PathBuf;

/// A dialog's or prompt's answer, applied by [`App::apply`].
pub enum Applied {
    /// Jump to row `n`, waiting for line indexing past what is indexed so far.
    GoToLine(usize),
    QQuery(String),
    SqlQuery(String),
    Filter(Vec<FilterStatement>),
    /// Columns, and per column whether it runs descending.
    Sort(Vec<String>, Vec<bool>),
    /// Column order, and how many columns are locked.
    ColumnOrder(Vec<String>, usize),
    /// The sidebar's Apply as one change: column order, locked count, filters, and the
    /// sort's columns with whether each runs descending.
    ApplyView(
        Vec<String>,
        usize,
        Vec<FilterStatement>,
        Vec<String>,
        Vec<bool>,
    ),
    Pivot(PivotSpec),
    Melt(MeltSpec),
    /// Write the on-screen Data Quality report from memory; nothing is read.
    QualityReportExport(
        PathBuf,
        crate::analysis::quality_export::ReportFormat,
        crate::Overwrite,
    ),
    /// Show the export's progress, then run it as [`Applied::DoExport`].
    Export(ExportRequest),
    /// Run the export once the UI has drawn its progress.
    DoExport(ExportRequest),
    /// Collect and format the whole view off-thread for a table-scope copy.
    CopyTable {
        format: crate::clipboard::CopyFormat,
        header: bool,
    },
    /// Show the chart export's phase, then run it as [`Applied::DoChartExport`].
    ChartExport(ChartExportRequest),
    /// Run the chart export once its phase is drawn.
    DoChartExport(ChartExportRequest),
    /// A confirmed documentation link, checked by `app::link_open::checked_url`.
    OpenLink(String),
    /// Read the dataset on screen again, keeping its place. See
    /// [`App::reopen_in_place`].
    Reopen(Option<Box<crate::loading::open_options::KeptPlace>>),
}

impl From<Applied> for AppEvent {
    fn from(applied: Applied) -> Self {
        AppEvent::Applied(applied)
    }
}

impl App {
    /// Carry out what a dialog or prompt asked for.
    pub(crate) fn apply(&mut self, applied: Applied) -> Option<AppEvent> {
        match applied {
            Applied::GoToLine(n) => {
                // Past the lines indexed so far: gone to once they all are.
                if let Some(state) = self.data_table_state.as_ref()
                    && state.indexing().is_some()
                    && (n >= state.num_rows()
                        || state.changes_rows()
                        || !state.view_sort_columns().is_empty()
                        || !state.view_sort_ascending())
                {
                    self.counting.goto_when_indexed = Some((self.dataset_generation, n));
                    self.status_message = Some(Self::INDEXING_FOR_ROW.to_string());
                    self.busy = false;
                    return None;
                }
                self.handle_scroll(|s| s.scroll_to_row_centered(n))
            }
            Applied::QQuery(query) => {
                self.run_query(QueryMode::Q, &query, "Applying query...");
                None
            }
            Applied::SqlQuery(sql) => {
                self.run_query(QueryMode::Sql, &sql, "Applying SQL query...");
                None
            }
            Applied::Filter(statements) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| s.filter(statements.clone()));
                }
                self.spawn_async_collect("Filtering...");
                None
            }
            Applied::Sort(columns, descending) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| s.sort_by(columns.clone(), descending.clone()));
                }
                self.spawn_async_collect("Sorting...");
                None
            }
            Applied::ApplyView(order, locked, filters, columns, descending) => {
                if let Some(state) = &mut self.data_table_state {
                    let change = state
                        .deferred(|s| s.apply_view(order, locked, filters, columns, descending));
                    self.spawn_async_collect(match change {
                        crate::table::ViewChange { sort: true, .. } => "Sorting...",
                        crate::table::ViewChange { filters: true, .. } => "Filtering...",
                        _ => Self::LOADING_BUFFER,
                    });
                }
                None
            }
            Applied::ColumnOrder(order, locked_count) => {
                if let Some(state) = &mut self.data_table_state {
                    state.deferred(|s| {
                        s.set_column_order(order);
                        s.set_locked_columns(locked_count);
                    });
                    self.spawn_async_collect(Self::LOADING_BUFFER);
                }
                None
            }
            Applied::Pivot(spec) => {
                // The modal stays up until the result is in, so a failed pivot leaves the spec to
                // fix.
                let job = self.data_table_state.as_ref()?.plan_pivot(&spec);
                self.spawn_job(Job::Pivot, Some(Self::COMPUTING_PIVOT), move |_| {
                    let pivoted = job
                        .run()
                        .map_err(|e| crate::error_display::user_message_from_report(&e, None))?;
                    Ok(Answer::Pivoted { spec, pivoted })
                });
                None
            }
            Applied::Melt(spec) => {
                self.busy = true;
                if let Some(state) = &mut self.data_table_state {
                    let result = state.deferred(|s| s.melt(&spec));
                    match result {
                        Ok(()) => {
                            self.close_overlay();
                            self.spawn_async_collect("Computing melt...");
                            None
                        }
                        Err(e) => {
                            self.busy = false;
                            self.error_modal
                                .show(crate::error_display::user_message_from_report(&e, None));
                            None
                        }
                    }
                } else {
                    self.busy = false;
                    None
                }
            }
            Applied::QualityReportExport(path, format, overwrite) => {
                // The report and the plan it was measured with, cloned to the writer: built from
                // memory, nothing read.
                let results = self.analysis_modal.quality.results.clone()?;
                let plan = self.analysis_modal.quality_result_plan().clone();
                self.spawn_job(
                    Job::QualityReport,
                    Some("Writing the report..."),
                    move |_| {
                        crate::analysis::quality_export::write(
                            &path, &results, &plan, format, overwrite,
                        )
                        .map_err(|error| Self::format_export_error(&error))?;
                        Ok(Answer::QualityReportWritten(path))
                    },
                );
                None
            }
            Applied::Export(request) => {
                if self.data_table_state.is_some() {
                    self.busy = true;
                    self.export_progress =
                        Some(ExportProgress::new(&request.path, "Preparing export"));
                    // Drawn before the export starts.
                    Some(Applied::DoExport(request).into())
                } else {
                    None
                }
            }
            Applied::DoExport(request) => {
                let Some(state) = &self.data_table_state else {
                    self.export_progress = None;
                    self.busy = false;
                    return None;
                };
                // Cloned, not taken: a failed write reopens the dialog on the same counts.
                let frame = match self.export_modal.counts.clone() {
                    Some(counts) => {
                        crate::table::ExportFrame::of(polars::prelude::IntoLazy::lazy(counts))
                    }
                    None => state.export_frame(request.options.source_file),
                };
                let streaming = state.polars_streaming();
                // One job from plan to commit: it holds the generation throughout, and any rows
                // it collects die with it.
                let phase = match request.route(streaming) {
                    crate::export::Route::Streamed => Self::export_write_phase(&request),
                    crate::export::Route::Collected => "Collecting data",
                };
                self.export_progress = Some(ExportProgress::new(&request.path, phase));
                let writing = Self::export_write_phase(&request);
                self.spawn_job(Job::Export, Some("Exporting..."), move |worker| {
                    let report = worker.reporter();
                    let written = move |bytes| {
                        report(Progress::ExportWriting {
                            phase: writing,
                            bytes,
                        })
                    };
                    frame
                        .into_lazy()
                        .map_err(color_eyre::eyre::Report::from)
                        .and_then(|lf| crate::export::run(lf, &request, streaming, written))
                        .map_err(|e| Self::format_export_error(&e))?;
                    // Success is reported only once the file is committed.
                    Ok(Answer::Exported(request.path))
                });
                None
            }
            Applied::Reopen(asked) => self.reopen_in_place(asked),
            Applied::OpenLink(url) => {
                // Not waited on; a browser that will not start gets a flash, not an error.
                if app::link_open::open(&url).is_err() {
                    self.flash_note("Couldn't open the link; y copies it".to_string());
                }
                None
            }
            Applied::CopyTable { format, header } => {
                let accepts = match self.copy_destination() {
                    Ok(destination) => destination.accepts(),
                    Err(e) => {
                        self.busy = false;
                        self.error_modal.show(e);
                        return None;
                    }
                };
                if let Some(state) = &self.data_table_state {
                    let lf = state.visible_lf();
                    let streaming = state.polars_streaming();
                    self.spawn_job(Job::Copy, Some("Collecting data for copy..."), move |_| {
                        // A capped destination's copy is read in batches and abandoned at the cap; others
                        // are collected whole.
                        let (payload, rows) = match accepts.base64_limit {
                            Some(limit) => crate::clipboard::bounded_table_text(
                                lf, format, header, limit,
                            )
                            .map(|(text, rows)| (crate::clipboard::Payload::text(text), rows)),
                            None => crate::analysis::statistics::collect_lazy(lf, streaming)
                                .map_err(|e| crate::error_display::user_message_from_polars(&e))
                                .and_then(|df| {
                                    crate::clipboard::tabular_payload(
                                        &df,
                                        format,
                                        header,
                                        accepts.html,
                                    )
                                    .map(|payload| (payload, df.height()))
                                }),
                        }
                        .map_err(|message| format!("Copy failed: {message}"))?;
                        // Handed on whole, never copied.
                        Ok(Answer::Copied {
                            payload,
                            message: format!(
                                "Copied {} rows as {}",
                                app::modals::copy_modal::thousands(rows),
                                format.as_str()
                            ),
                        })
                    });
                } else {
                    self.busy = false;
                }
                None
            }
            Applied::ChartExport(request) => {
                self.busy = true;
                self.export_progress = Some(ExportProgress::new(&request.path, "Exporting chart"));
                Some(Applied::DoChartExport(request).into())
            }
            Applied::DoChartExport(request) => {
                // `ChartExport` arms `busy` and defers here to draw its phase; a Ctrl-O meanwhile left
                // the chart, so release rather than park an export nothing will prepare.
                if !self.overlay.shows(&Overlay::Chart) {
                    self.export_progress = None;
                    self.status_message = None;
                    self.busy = false;
                    return None;
                }
                self.start_chart_export(request);
                None
            }
        }
    }
}
