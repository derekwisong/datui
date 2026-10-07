//! The copy modal's keys: the shared form keys (`crate::form`), then what each row
//! does with them.

use crate::copy_modal::CopyFocus;
use crate::feedback::Confirm;
use crate::form::FormKey;
use crate::jobs::{Answer, Job};
use crate::loading::open_options::OpenOptions;
use crate::table::DataTableState;
use crate::{App, AppEvent, clipboard, cloud::source, copy_modal, export::python_script};
use color_eyre::Result;
use crossterm::event::{KeyCode, KeyEvent};
use std::path::Path;

impl App {
    /// Keys in the copy modal.
    pub(crate) fn copy_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if crate::form::picker_form_key(&mut self.copy_modal, event) {
            return None;
        }

        // Any key means the form is being edited again: the accented gap line returns to
        // plain (Enter re-arms it).
        self.copy_modal.attention = false;

        match crate::form::key(&mut self.copy_modal, event) {
            FormKey::Cancel => {
                self.close_overlay();
            }
            // Enter copies from anywhere; the spec line has echoed what it will do.
            FormKey::Submit => {
                if self.copy_modal.validation_error().is_some() {
                    self.copy_modal.attention = true;
                    return None;
                }
                return self.perform_copy();
            }
            FormKey::Step(CopyFocus::Scope, delta) => self.copy_modal.step_scope(delta),
            FormKey::Step(CopyFocus::Format, delta) => self.copy_modal.step_format(delta),
            FormKey::Step(CopyFocus::Column, delta) => self.copy_modal.step_column(delta),
            FormKey::Act(CopyFocus::Header) => self.copy_modal.toggle_header(),
            FormKey::Act(CopyFocus::Column) => self.copy_modal.open_picker(),
            FormKey::Other if event.code == KeyCode::Char('?') => self.open_help_overlay(),
            _ => {}
        }
        None
    }

    /// Enter in the copy dialog: buffer scopes copy and flash; the table scope checks
    /// size, then collects off-thread.
    pub(crate) fn perform_copy(&mut self) -> Option<AppEvent> {
        use copy_modal::{CopyScope, thousands};
        /// What Enter decided under the table borrow, acted on after: the clipboard needs
        /// the whole app.
        enum Planned {
            Copy(clipboard::Payload, String),
            Collect,
            /// The size, if known (not while the count is coming or a binary column's width is
            /// unknown).
            Confirm(Option<usize>),
        }
        let format = self.copy_modal.format;
        let header = self.copy_modal.header();
        let scope = self.copy_modal.scope;
        // The destination decides what is built: no HTML for one that cannot take it, and
        // nothing past its cap.
        let accepts = match self.copy_destination() {
            Ok(destination) => destination.accepts(),
            Err(e) => {
                self.close_overlay();
                self.error_modal.show(e);
                return None;
            }
        };
        let planned: Result<Planned, String> = match self.data_table_state.as_ref() {
            None => Err("Nothing to copy: no table is open".to_string()),
            Some(state) => match scope {
                CopyScope::Cell => {
                    let column = self.copy_modal.column.clone().unwrap_or_default();
                    match state.copy_cell_value(&column) {
                        Some(value) => {
                            let row = state.selected_display_row().unwrap_or(0);
                            Ok(Planned::Copy(
                                clipboard::Payload::text(value),
                                format!("Copied cell {column} of row {}", thousands(row)),
                            ))
                        }
                        None => Err("Nothing to copy: the current row is not buffered".to_string()),
                    }
                }
                CopyScope::Row => match state.copy_row_df() {
                    Some(df) => clipboard::tabular_payload(&df, format, header, accepts.html).map(
                        |payload| {
                            let row = state.selected_display_row().unwrap_or(0);
                            Planned::Copy(
                                payload,
                                format!("Copied row {} as {}", thousands(row), format.as_str()),
                            )
                        },
                    ),
                    None => Err("Nothing to copy: the current row is not buffered".to_string()),
                },
                CopyScope::View => match state.copy_view_df() {
                    Some(df) => clipboard::tabular_payload(&df, format, header, accepts.html).map(
                        |payload| {
                            Planned::Copy(
                                payload,
                                format!(
                                    "Copied {} rows as {}",
                                    thousands(df.height()),
                                    format.as_str()
                                ),
                            )
                        },
                    ),
                    None => Err("Nothing to copy: no rows are on screen".to_string()),
                },
                CopyScope::Python => Ok(Planned::Copy(
                    clipboard::Payload::text(self.python_script(state)),
                    "Copied the view as Python".to_string(),
                )),
                CopyScope::Table => {
                    // A capped destination's copy reads only to its cap: the smaller of the two.
                    let cap = accepts
                        .base64_limit
                        .map_or(usize::MAX, |limit| limit / 4 * 3);
                    match state.estimated_copy_bytes() {
                        Some(bytes) if bytes > Self::COPY_REFUSE_BYTES => Err(format!(
                            "The table is about {} — too much to hold on a clipboard. \
                             Export it to a file instead (e).",
                            crate::numfmt::bytes(bytes as u64)
                        )),
                        Some(bytes) if bytes.min(cap) > Self::COPY_CONFIRM_BYTES => {
                            Ok(Planned::Confirm(Some(bytes)))
                        }
                        Some(_) => Ok(Planned::Collect),
                        None if cap <= Self::COPY_CONFIRM_BYTES => Ok(Planned::Collect),
                        // Size unknown (count still coming, binary width unknown): ask before collecting.
                        None => Ok(Planned::Confirm(None)),
                    }
                }
            },
        };
        self.close_overlay();
        match planned {
            Ok(Planned::Copy(payload, message)) => {
                self.finish_copy(payload, message);
                None
            }
            Ok(Planned::Collect) => Some(AppEvent::CopyTable { format, header }),
            Ok(Planned::Confirm(bytes)) => {
                let counting = self
                    .data_table_state
                    .as_ref()
                    .is_some_and(|state| state.num_rows_if_valid().is_none());
                let message = match bytes {
                    Some(bytes) => format!(
                        "This copies about {} to the clipboard.\n\nCopy the whole table?",
                        crate::numfmt::bytes(bytes as u64)
                    ),
                    None if counting => "The table's size is not known yet — the row count \
                                         is still being read.\n\nCopy the whole table anyway?"
                        .to_string(),
                    None => "The size of the table's binary columns is not known.\n\n\
                             Copy the whole table anyway?"
                        .to_string(),
                };
                self.confirmation_modal
                    .show(message, Confirm::Copy(format, header));
                None
            }
            Err(message) => {
                self.error_modal.show(message);
                None
            }
        }
    }

    /// The view on screen as a Python Polars script: the open's reader, then every
    /// step that made the view. See [`python_script`].
    pub fn python_script(&self, state: &DataTableState) -> String {
        let (paths, options) = match &self.source.opened {
            Some((paths, options)) => (Some(paths.as_slice()), options.clone()),
            None => (None, OpenOptions::default()),
        };
        let cloud = options.effective_cloud(&self.app_config.cloud);
        let mut options = options;
        if options.read_python.is_empty() {
            // A decompressed file is read into its dataset directly, not through a scan.
            options.read_python = state.read_python().to_vec();
        }
        // The source an `s3://<id>@bucket` URL names has its own endpoint and region.
        let remote = paths
            .and_then(|paths| paths.first())
            .map(|p| p.to_string_lossy().into_owned())
            .filter(|p| source::is_remote_url(Path::new(p)));
        let source_of = remote
            .as_deref()
            .and_then(|url| source::split_source_id(url).0)
            .and_then(|id| cloud.connections.iter().find(|c| c.name == id));
        // What this session learned of the place: read unsigned, it is public.
        #[cfg(feature = "cloud")]
        let unsigned = remote
            .as_deref()
            .and_then(crate::cloud::cloud_sources::known_access)
            .unwrap_or(false);
        #[cfg(not(feature = "cloud"))]
        let unsigned = false;
        let record = python_script::OpenRecord {
            paths,
            options: &options,
            format: state.read_as().or(options.format),
            read_mode: state.read_mode(),
            schema: state.source_schema(),
            remote_objects: state
                .remote_objects()
                .unwrap_or_default()
                .into_iter()
                .map(|object| object.url)
                .collect(),
            s3_endpoint: source_of
                .and_then(|c| c.endpoint_url.clone())
                .or(cloud.s3_endpoint_url.clone())
                .filter(|s| !s.trim().is_empty()),
            s3_region: source_of
                .and_then(|c| c.region.clone())
                .or(cloud.s3_region.clone())
                .filter(|s| !s.trim().is_empty()),
            unsigned,
            read_as_text: state.read_as_text().iter().map(|c| c.to_string()).collect(),
            spec: state.format_read().map(|read| read.spec.name.clone()),
        };
        python_script::Script {
            source: python_script::source(&record),
            steps: state.python_steps(),
        }
        .render()
    }

    /// Hand a payload to the clipboard (built at the first copy), then flash or show
    /// the error: a silent no-op copy is worse than a loud failure.
    pub(crate) fn finish_copy(&mut self, payload: clipboard::Payload, message: String) {
        let written = self
            .copy_destination()
            .and_then(|destination| destination.write(payload));
        match written {
            Ok(()) => self.flash_note(message),
            Err(e) => self.error_modal.show(e),
        }
    }

    /// The clipboard destination, built at the first copy.
    pub(crate) fn copy_destination(&mut self) -> Result<&mut dyn clipboard::Destination, String> {
        if self.external.clipboard.is_none() {
            let choice = clipboard::BackendChoice::parse(&self.app_config.clipboard.backend)
                .unwrap_or_default();
            let limit = usize::try_from(self.app_config.clipboard.osc52_limit.bytes())
                .unwrap_or(usize::MAX);
            self.external.clipboard = Some(clipboard::destination(choice, limit)?);
        }
        Ok(self
            .external
            .clipboard
            .as_deref_mut()
            .expect("destination just built"))
    }

    /// Copy `text`, refused when it is over a capped destination's limit.
    pub(crate) fn copy_string(&mut self, text: String, message: String) {
        let limit = match self.copy_destination() {
            Ok(destination) => destination.accepts().base64_limit,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        if let Some(limit) = limit
            && text.len() > limit / 4 * 3
        {
            self.error_modal
                .show(clipboard::over_osc52_limit(None, limit));
            return;
        }
        self.finish_copy(clipboard::Payload::text(text), message);
    }

    /// Copy the one value of `column`, exact, and flash `message`.
    pub(crate) fn copy_value(&mut self, column: polars::prelude::Column, message: String) {
        // Destination first: a value over the terminal's cap is refused before formatting.
        let limit = match self.copy_destination() {
            Ok(destination) => destination.accepts().base64_limit,
            Err(e) => {
                self.error_modal.show(e);
                return;
            }
        };
        if let Some(limit) = limit {
            let fits = limit / 4 * 3;
            let over = column
                .get(0)
                .is_ok_and(|value| crate::exact::copy_len_floor(&value, fits) > fits);
            if over {
                self.error_modal
                    .show(clipboard::over_osc52_limit(None, limit));
                return;
            }
        }
        if column.as_materialized_series().estimated_size() <= Self::FIELD_COPY_INLINE_BYTES {
            match crate::exact::copy_text(&column) {
                Ok(text) => self.finish_copy(clipboard::Payload::text(text), message),
                Err(e) => self.error_modal.show(format!("Copy failed: {e}")),
            }
            return;
        }
        self.spawn_job(Job::Copy, Some("Copying..."), move |_| {
            let text = crate::exact::copy_text(&column).map_err(|e| format!("Copy failed: {e}"))?;
            Ok(Answer::Copied {
                payload: clipboard::Payload::text(text),
                message,
            })
        });
    }

    /// Replace the clipboard destination, so tests can watch copies without a display
    /// or terminal.
    pub fn set_clipboard_destination(&mut self, destination: Box<dyn clipboard::Destination>) {
        self.external.clipboard = Some(destination);
    }
}
