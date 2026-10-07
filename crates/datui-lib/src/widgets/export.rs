//! Export modal rendering: a FormView, a row per field in one column, the
//! actions in the footer. Format is a Choice, so it is drawn as one: its values
//! side by side on its row, where ←/→ visibly step along them.

use crate::CompressionFormat;
use crate::app::pointer::FieldId;
use crate::export::export_modal::{COMPRESSION_OPTIONS, ExportFocus, ExportFormat, ExportModal};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormLine, FormValue, FormView, HintBar};
use ratatui::layout::Rect;
use ratatui::style::Style;

/// The value column's offset: past the longest labels, "Compression:" and
/// "Source file:", plus two cells of air, which the format row's tint uses.
const LABEL_WIDTH: u16 = 14;

/// The widest the dialog grows: room for every format on its row.
const MAX_WIDTH: u16 = 70;

/// Rows the dialog takes: the most fields a format shows (format, path,
/// delimiter, header, compression, source file), a blank and the status line,
/// then the blank, the footer and the frame.
const HEIGHT: u16 = 6 + 2 + 4;

/// Shown under the rows of a format that cannot hold lists or structs.
const NESTED_NOTE: &str = "Lists and structs written as JSON";

/// Shown under the rows of an Avro export whose view has a name Avro refuses.
const AVRO_NAMES_NOTE: &str = "Column names made valid for Avro";

/// Format names, in the order ←/→ step them.
const FORMAT_NAMES: [&str; ExportFormat::ALL.len()] = {
    let mut names = [""; ExportFormat::ALL.len()];
    let mut i = 0;
    while i < names.len() {
        names[i] = ExportFormat::ALL[i].as_str();
        i += 1;
    }
    names
};

const fn compression_name(compression: Option<CompressionFormat>) -> &'static str {
    match compression {
        None => "None",
        Some(CompressionFormat::Gzip) => "Gzip",
        Some(CompressionFormat::Zstd) => "Zstd",
        Some(CompressionFormat::Bzip2) => "Bzip2",
        Some(CompressionFormat::Xz) => "XZ",
    }
}

/// Compression names, in the order ←/→ step them: shown side by side, as the
/// formats are, so the choices are on screen rather than a bare `None`.
const COMPRESSION_NAMES: [&str; COMPRESSION_OPTIONS.len()] = {
    let mut names = [""; COMPRESSION_OPTIONS.len()];
    let mut i = 0;
    while i < names.len() {
        names[i] = compression_name(COMPRESSION_OPTIONS[i]);
        i += 1;
    }
    names
};

/// Where the dialog sits in `area`: centered and compact, a fixed height so
/// nothing moves as the format's fields come and go.
pub fn dialog_area(area: Rect) -> Rect {
    let width = area.width.saturating_sub(4).min(MAX_WIDTH);
    let height = HEIGHT.min(area.height);
    crate::render::layout::centered_rect(area, width, height)
}

pub fn render_export_modal(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    ctx: &RenderContext,
) {
    // Primary first, Esc last, and one chip for what the focused row itself
    // takes; when the dialog runs out of room, Tab yields first and the way
    // out goes last.
    let footer = HintBar::from_ctx(ctx)
        .screen(datui_cli::keys::Context::Export)
        .group("Form")
        .key("Enter")
        .weight(3);
    let footer = match modal.focus {
        ExportFocus::FormatSelector => footer.key_as("← / →", "Format").weight(2),
        ExportFocus::CsvIncludeHeader | ExportFocus::SourceFile => {
            footer.key_as("Space", "Toggle").weight(2)
        }
        ExportFocus::Compression => footer.key("← / →").weight(2),
        ExportFocus::PathInput | ExportFocus::CsvDelimiter => footer,
    };
    let footer = footer.key("Tab").weight(1).key("Esc").weight(4);
    crate::app::pointer::record(area, crate::app::pointer::Hit::Modal);

    modal
        .path_input
        .set_focused(modal.focus == ExportFocus::PathInput);
    modal
        .csv_delimiter_input
        .set_focused(modal.focus == ExportFocus::CsvDelimiter);

    // One row per field `fields` offers, in its order, so Tab walks what is on
    // screen. Format stays the first row: stepping it adds and drops the rows
    // below, never moves it.
    let selected = ExportFormat::ALL
        .iter()
        .position(|f| *f == modal.selected_format)
        .unwrap_or(0);
    let compression = COMPRESSION_OPTIONS
        .iter()
        .position(|c| *c == modal.compression())
        .unwrap_or(0);
    let rows = modal
        .focus_order()
        .into_iter()
        .map(|field| {
            let (label, value) = match field {
                ExportFocus::FormatSelector => (
                    "Format:",
                    FormValue::Options {
                        items: &FORMAT_NAMES,
                        selected,
                        clicks: Some(FieldId::of::<ExportModal>(field)),
                    },
                ),
                ExportFocus::PathInput => ("Path:", FormValue::Input(&modal.path_input)),
                ExportFocus::CsvDelimiter => {
                    ("Delimiter:", FormValue::Input(&modal.csv_delimiter_input))
                }
                ExportFocus::CsvIncludeHeader => {
                    ("Header:", FormValue::Toggle(modal.csv_include_header))
                }
                ExportFocus::Compression => (
                    "Compression:",
                    FormValue::Options {
                        items: &COMPRESSION_NAMES,
                        selected: compression,
                        clicks: Some(FieldId::of::<ExportModal>(field)),
                    },
                ),
                ExportFocus::SourceFile => ("Source file:", FormValue::Toggle(modal.source_file)),
            };
            FormLine::Field(field, label, value)
        })
        .collect();

    // The reason the form cannot export yet, inline on the last line: a warning
    // at most, never a modal. Otherwise the line says how a format without
    // nesting writes the view's list and struct columns, or that Avro renames.
    let status = match modal.path_error.as_deref() {
        Some(message) => (message, ctx.warning),
        None if modal.nested_columns && !modal.selected_format.holds_nesting() => {
            (NESTED_NOTE, ctx.dimmed)
        }
        None if modal.avro_renames && modal.selected_format == ExportFormat::Avro => {
            (AVRO_NAMES_NOTE, ctx.dimmed)
        }
        None => ("", ctx.dimmed),
    };
    FormView {
        title: "Export Data",
        screen: datui_cli::keys::Context::Export,
        footer: Some(footer),
        label_width: LABEL_WIDTH,
        rows,
        focused: Some(modal.focus),
        picker: None,
        status: Some((status.0.to_string(), Style::default().fg(status.1))),
    }
    .render::<ExportModal>(area, buf, ctx);
}

#[cfg(test)]
mod tests;
