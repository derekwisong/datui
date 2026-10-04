//! The export modal's keys: the shared form keys (`crate::form`), then what each
//! field does with them.

use crate::export::{ExportOptions, ExportRequest};
use crate::export_modal::ExportFocus;
use crate::form::FormKey;
use crate::logging::LogFailure;
use crate::output_file::Overwrite;
use crate::{App, AppEvent, home};
use crossterm::event::KeyEvent;

impl App {
    /// Keys in the export modal.
    pub(crate) fn export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match crate::form::key(&mut self.export_modal, event) {
            FormKey::Cancel => {
                self.export_modal.close();
                self.input_mode = self.export_returns_to();
                self.export_counts = None;
            }
            FormKey::Submit => return self.submit_export(),
            FormKey::Step(ExportFocus::FormatSelector, delta) => {
                self.export_modal.step_format(delta);
            }
            FormKey::Step(ExportFocus::Compression, delta) => {
                self.export_modal.step_compression(delta);
            }
            FormKey::Act(ExportFocus::CsvIncludeHeader) => {
                self.export_modal.csv_include_header = !self.export_modal.csv_include_header;
            }
            FormKey::Act(ExportFocus::SourceFile) => {
                self.export_modal.source_file = !self.export_modal.source_file;
            }
            FormKey::Text(ExportFocus::PathInput) => self.export_path_key(event),
            FormKey::Text(ExportFocus::CsvDelimiter) => {
                self.export_modal
                    .csv_delimiter_input
                    .handle_key(event, None);
            }
            _ => {}
        }
        None
    }

    /// Enter, from any field: build the export from the state every row already
    /// echoes. A blank path cannot, and says so inline instead of doing nothing.
    fn submit_export(&mut self) -> Option<AppEvent> {
        let path_str = self.export_modal.path_input.value().trim().to_string();
        if path_str.is_empty() {
            self.export_modal.path_error = Some("Enter a file path.");
            crate::form::Form::focus(&mut self.export_modal, ExportFocus::PathInput);
            return None;
        }
        // `~` and `$VAR` expand as everywhere else a path is typed; unexpanded they
        // become a literal `~` directory or a NotFound from the writer.
        let path = home::expand_user_path(&path_str);
        let format = self.export_modal.selected_format;
        let compression = self.export_modal.compression();
        let path_with_ext = Self::ensure_file_extension(&path, format, compression);
        // The path field shows the extension the file gets.
        if path_with_ext != path {
            self.export_modal
                .path_input
                .set_value(path_with_ext.display().to_string());
        }
        self.export_modal
            .path_input
            .save_to_history(&self.cache)
            .or_log("save the export path");
        let delimiter = self
            .export_modal
            .csv_delimiter_input
            .value()
            .chars()
            .next()
            .unwrap_or(',') as u8;
        let request = ExportRequest {
            path: path_with_ext,
            format,
            options: ExportOptions {
                csv_delimiter: delimiter,
                csv_include_header: self.export_modal.csv_include_header,
                csv_compression: self.export_modal.csv_compression,
                json_compression: self.export_modal.json_compression,
                ndjson_compression: self.export_modal.ndjson_compression,
                source_file: self.export_modal.source_file,
            },
            overwrite: Overwrite::Forbid,
        };
        if request.path.exists() {
            let path_display = request.path.display().to_string();
            self.pending_export = Some(request);
            self.confirmation_modal.show_destructive(
                format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                "Overwrite",
            );
            // Suspended, not closed: declining returns to the filled form with the
            // typed path intact.
            self.export_modal.suspend();
            self.input_mode = self.export_returns_to();
            return None;
        }
        self.export_modal.close();
        self.input_mode = self.export_returns_to();
        Some(AppEvent::Export(request))
    }
}
