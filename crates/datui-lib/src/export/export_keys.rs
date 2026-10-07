//! The export modal's keys: the shared form keys (`crate::form`), then what each
//! field does with them.

use crate::cli::{CompressionFormat, FileFormat};
use crate::export::export_modal::ExportFocus;
use crate::export::export_modal::ExportFormat;
use crate::export::output_file::Overwrite;
use crate::export::{ExportOptions, ExportRequest};
use crate::feedback::Confirm;
use crate::form::FormKey;
use crate::logging::LogFailure;
use crate::{App, AppEvent, home};
use crossterm::event::KeyEvent;
use std::path::{Path, PathBuf};

impl App {
    /// Keys in the export modal.
    pub(crate) fn export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match crate::form::key(&mut self.export_modal, event) {
            FormKey::Cancel => {
                self.close_overlay();
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

    /// Enter from any field: build the export. A blank path says so inline.
    fn submit_export(&mut self) -> Option<AppEvent> {
        let path_str = self.export_modal.path_input.value().trim().to_string();
        if path_str.is_empty() {
            self.export_modal.path_error = Some("Enter a file path.".to_string());
            crate::form::Form::focus(&mut self.export_modal, ExportFocus::PathInput);
            return None;
        }
        // `~` and `$VAR` expand as in every typed path.
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
            self.confirmation_modal.show_destructive(
                format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                "Overwrite",
                Confirm::Export(Box::new(request)),
            );
            // Suspended, not closed: declining returns to the filled form.
            self.step_back();
            return None;
        }
        // Suspended while writing: a failed write brings the form back with the reason.
        self.step_back();
        Some(AppEvent::Export(request))
    }

    /// Give the path input a key; when the value changed, follow the typed extension
    /// with the format choice, so Parquet bytes never land in `out.csv`. Cursor-only
    /// keys re-pick nothing, so a format chosen after typing stands.
    fn export_path_key(&mut self, event: &KeyEvent) {
        let before = self.export_modal.path_input.value().to_string();
        self.export_modal
            .path_input
            .handle_key(event, Some(&self.cache));
        if self.export_modal.path_input.value() != before {
            self.export_modal.sync_format_to_path();
            // Typing is the correction the message asked for.
            self.export_modal.path_error = None;
        }
    }

    fn ensure_file_extension(
        path: &Path,
        format: ExportFormat,
        compression: Option<CompressionFormat>,
    ) -> PathBuf {
        let current_ext = path.extension().and_then(|e| e.to_str()).unwrap_or("");
        let mut new_path = path.to_path_buf();

        if current_ext.is_empty() {
            // No extension: use default for format (and add compression if selected)
            let desired_ext = if let Some(comp) = compression {
                format!("{}.{}", format.extension(), comp.extension())
            } else {
                format.extension().to_string()
            };
            new_path.set_extension(&desired_ext);
        } else {
            // User provided an extension: keep it. Only add compression suffix when compression is selected.
            let is_compression_only = matches!(
                current_ext.to_lowercase().as_str(),
                "gz" | "zst" | "bz2" | "xz"
            ) && ExportFormat::from_extension(current_ext).is_none();

            if is_compression_only {
                // Path has only compression ext (e.g. file.gz); stem may have format (file.csv.gz)
                let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                let stem_has_format = stem
                    .split('.')
                    .next_back()
                    .and_then(ExportFormat::from_extension)
                    .is_some();
                if stem_has_format {
                    if let Some(comp) = compression
                        && let Some(format_ext) = stem
                            .split('.')
                            .next_back()
                            .and_then(ExportFormat::from_extension)
                            .map(|f| f.extension())
                    {
                        new_path =
                            PathBuf::from(stem.rsplit_once('.').map(|x| x.0).unwrap_or(stem));
                        new_path.set_extension(format!("{}.{}", format_ext, comp.extension()));
                    }
                } else if let Some(comp) = compression {
                    new_path.set_extension(format!("{}.{}", format.extension(), comp.extension()));
                } else {
                    new_path.set_extension(format.extension());
                }
            } else if let Some(comp) = compression
                && format.supports_compression()
            {
                new_path.set_extension(format!("{}.{}", current_ext, comp.extension()));
            }
            // Otherwise the path keeps its extension (foo.feather stays foo.feather).
        }

        new_path
    }

    /// The default export format for a dataset opened from `path`: the format the open
    /// read (sniffed or `--format`), else the extension. A compressed CSV stays CSV:
    /// `sales.csv.gz`'s `.csv` is in the stem.
    pub(crate) fn export_format_for(
        path: &Path,
        format: Option<FileFormat>,
    ) -> Option<ExportFormat> {
        format
            .or_else(|| FileFormat::from_path(path))
            .or_else(|| {
                CompressionFormat::from_extension(path)
                    .and(path.file_stem())
                    .and_then(|stem| FileFormat::from_path(Path::new(stem)))
            })
            .and_then(crate::formats::readers::export_default)
    }

    /// What the status line says while an export writes its file.
    pub(crate) fn export_write_phase(request: &ExportRequest) -> &'static str {
        if request.options.compression(request.format).is_some() {
            "Writing and compressing file"
        } else {
            "Writing file"
        }
    }

    /// Why an export, report or chart did not write, for the dialog's status line under
    /// its path.
    pub(crate) fn format_export_error(error: &color_eyre::eyre::Report) -> String {
        use std::io::{self, ErrorKind};

        for cause in error.chain() {
            if let Some(io_err) = cause.downcast_ref::<io::Error>() {
                // Matched by type, not kind: encoder errors share kinds like InvalidInput with the
                // destination checks.
                let msg = match (
                    crate::export::output_file::Refused::of(io_err),
                    io_err.kind(),
                ) {
                    (Some(refused), _) => format!("{refused}."),
                    // A CSV open in a spreadsheet app, on Windows.
                    (None, _) if crate::error_display::held_by_another_program(io_err) => {
                        "it is open in another program; close it there and try again.".to_string()
                    }
                    (None, ErrorKind::PermissionDenied) => "permission denied.".to_string(),
                    (None, ErrorKind::IsADirectory) => "it is a directory.".to_string(),
                    (None, _) => crate::error_display::user_message_from_io(io_err, None),
                };
                return format!("Cannot write: {msg}");
            }
            if let Some(pe) = cause.downcast_ref::<polars::prelude::PolarsError>() {
                let msg = crate::error_display::user_message_from_polars(pe);
                return format!("Export failed: {}", msg);
            }
        }
        let error_str = error.to_string();
        let first_line = error_str.lines().next().unwrap_or("Unknown error").trim();
        format!("Export failed: {}", first_line)
    }
}
