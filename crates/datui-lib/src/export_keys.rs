//! The export modal's keys.

use crate::export::{ExportOptions, ExportRequest};
use crate::export_modal::{ExportFocus, ExportFormat};
use crate::output_file::Overwrite;
use crate::{App, AppEvent, home};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the export modal.
    pub(crate) fn export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match event.code {
            KeyCode::Esc => {
                self.export_modal.close();
                self.input_mode = self.export_returns_to();
                self.export_counts = None;
            }
            KeyCode::Tab => self.export_modal.next_focus(),
            KeyCode::BackTab => self.export_modal.prev_focus(),
            KeyCode::Up | KeyCode::Char('k') => {
                match self.export_modal.focus {
                    ExportFocus::FormatSelector => {
                        // Cycle through formats
                        let current_idx = ExportFormat::ALL
                            .iter()
                            .position(|&f| f == self.export_modal.selected_format)
                            .unwrap_or(0);
                        let prev_idx = if current_idx == 0 {
                            ExportFormat::ALL.len() - 1
                        } else {
                            current_idx - 1
                        };
                        self.export_modal.selected_format = ExportFormat::ALL[prev_idx];
                        self.export_modal.sync_path_to_format();
                    }
                    ExportFocus::PathInput => {
                        // Pass to text input widget (for history navigation)
                        self.export_path_key(event);
                    }
                    ExportFocus::CsvDelimiter => {
                        // Pass to text input widget (for history navigation)
                        self.export_modal
                            .csv_delimiter_input
                            .handle_key(event, None);
                    }
                    ExportFocus::CsvCompression
                    | ExportFocus::JsonCompression
                    | ExportFocus::NdjsonCompression => {
                        // Left to move to previous compression option
                        self.export_modal.cycle_compression_backward();
                    }
                    _ => {
                        self.export_modal.prev_focus();
                    }
                }
            }
            KeyCode::Down | KeyCode::Char('j') => {
                match self.export_modal.focus {
                    ExportFocus::FormatSelector => {
                        // Cycle through formats
                        let current_idx = ExportFormat::ALL
                            .iter()
                            .position(|&f| f == self.export_modal.selected_format)
                            .unwrap_or(0);
                        let next_idx = (current_idx + 1) % ExportFormat::ALL.len();
                        self.export_modal.selected_format = ExportFormat::ALL[next_idx];
                        self.export_modal.sync_path_to_format();
                    }
                    ExportFocus::PathInput => {
                        // Pass to text input widget (for history navigation)
                        self.export_path_key(event);
                    }
                    ExportFocus::CsvDelimiter => {
                        // Pass to text input widget (for history navigation)
                        self.export_modal
                            .csv_delimiter_input
                            .handle_key(event, None);
                    }
                    ExportFocus::CsvCompression
                    | ExportFocus::JsonCompression
                    | ExportFocus::NdjsonCompression => {
                        // Right to move to next compression option
                        self.export_modal.cycle_compression();
                    }
                    _ => {
                        self.export_modal.next_focus();
                    }
                }
            }
            KeyCode::Left | KeyCode::Char('h') => {
                match self.export_modal.focus {
                    ExportFocus::PathInput => {
                        self.export_path_key(event);
                    }
                    ExportFocus::CsvDelimiter => {
                        self.export_modal
                            .csv_delimiter_input
                            .handle_key(event, None);
                    }
                    ExportFocus::FormatSelector => {
                        // Don't change focus in format selector
                    }
                    ExportFocus::CsvCompression
                    | ExportFocus::JsonCompression
                    | ExportFocus::NdjsonCompression => {
                        // Move to previous compression option
                        self.export_modal.cycle_compression_backward();
                    }
                    _ => self.export_modal.prev_focus(),
                }
            }
            KeyCode::Right | KeyCode::Char('l') => {
                match self.export_modal.focus {
                    ExportFocus::PathInput => {
                        self.export_path_key(event);
                    }
                    ExportFocus::CsvDelimiter => {
                        self.export_modal
                            .csv_delimiter_input
                            .handle_key(event, None);
                    }
                    ExportFocus::FormatSelector => {
                        // Don't change focus in format selector
                    }
                    ExportFocus::CsvCompression
                    | ExportFocus::JsonCompression
                    | ExportFocus::NdjsonCompression => {
                        // Move to next compression option
                        self.export_modal.cycle_compression();
                    }
                    _ => self.export_modal.next_focus(),
                }
            }
            KeyCode::Enter => {
                // Enter applies from anywhere in the form: build the export from
                // the state every row already echoes. A blank path cannot, and
                // says so inline instead of doing nothing.
                let path_str = self.export_modal.path_input.value().trim().to_string();
                if path_str.is_empty() {
                    self.export_modal.path_error = Some("Enter a file path.");
                    self.export_modal.focus = ExportFocus::PathInput;
                }
                if !path_str.is_empty() {
                    // `~` and `$VAR` expand as everywhere else a path is
                    // typed; unexpanded they become a literal `~` directory
                    // or a NotFound from the writer.
                    let mut path = home::expand_user_path(&path_str);
                    let format = self.export_modal.selected_format;
                    let compression = match format {
                        ExportFormat::Csv | ExportFormat::Tsv | ExportFormat::Psv => {
                            self.export_modal.csv_compression
                        }
                        ExportFormat::Json => self.export_modal.json_compression,
                        ExportFormat::Ndjson => self.export_modal.ndjson_compression,
                        ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => None,
                    };
                    // Ensure file extension is present (including compression extension if needed)
                    let path_with_ext = Self::ensure_file_extension(&path, format, compression);
                    // Update the path input to show the extension
                    if path_with_ext != path {
                        self.export_modal
                            .path_input
                            .set_value(path_with_ext.display().to_string());
                    }
                    path = path_with_ext;
                    let delimiter = self
                        .export_modal
                        .csv_delimiter_input
                        .value()
                        .chars()
                        .next()
                        .unwrap_or(',') as u8;
                    let request = ExportRequest {
                        path,
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
                    // Check if file exists and show confirmation
                    if request.path.exists() {
                        let path_display = request.path.display().to_string();
                        self.pending_export = Some(request);
                        self.confirmation_modal.show_destructive(
                            format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                            "Overwrite",
                        );
                        // Suspended, not closed: declining returns to the
                        // filled form with the typed path intact.
                        self.export_modal.suspend();
                        self.input_mode = self.export_returns_to();
                    } else {
                        // Start export with progress
                        self.export_modal.close();
                        self.input_mode = self.export_returns_to();
                        return Some(AppEvent::Export(request));
                    }
                }
            }
            KeyCode::Char(' ') => {
                // Space to toggle checkboxes, but pass to text inputs if they're focused
                match self.export_modal.focus {
                    ExportFocus::PathInput => {
                        // Pass spacebar to text input
                        self.export_path_key(event);
                    }
                    ExportFocus::CsvDelimiter => {
                        // Pass spacebar to text input
                        self.export_modal
                            .csv_delimiter_input
                            .handle_key(event, None);
                    }
                    ExportFocus::CsvIncludeHeader => {
                        // Toggle checkbox
                        self.export_modal.csv_include_header =
                            !self.export_modal.csv_include_header;
                    }
                    ExportFocus::SourceFile => {
                        self.export_modal.source_file = !self.export_modal.source_file;
                    }
                    _ => {}
                }
            }
            KeyCode::Char(_)
            | KeyCode::Backspace
            | KeyCode::Delete
            | KeyCode::Home
            | KeyCode::End => {
                match self.export_modal.focus {
                    ExportFocus::PathInput => {
                        self.export_path_key(event);
                    }
                    ExportFocus::CsvDelimiter => {
                        self.export_modal
                            .csv_delimiter_input
                            .handle_key(event, None);
                    }
                    ExportFocus::FormatSelector => {
                        // Don't input text in format selector
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        None
    }
}
