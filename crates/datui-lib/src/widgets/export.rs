//! Export modal rendering: the reference migration to the `widgets::ui` kit.
//! One Surface, a Picker for the format, FormRows for the options, actions in
//! the footer.

use crate::CompressionFormat;
use crate::export_modal::{ExportFocus, ExportFormat, ExportModal};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, SectionRule, Surface};
use ratatui::layout::Rect;

/// The value column's offset inside the options half: past the longest label,
/// "Include header:", plus two cells of air.
const LABEL_WIDTH: u16 = 17;

/// Columns the format list needs: rail plus the longest name plus air.
const FORMAT_WIDTH: u16 = 12;

fn compression_name(compression: Option<CompressionFormat>) -> &'static str {
    match compression {
        None => "None",
        Some(CompressionFormat::Gzip) => "Gzip",
        Some(CompressionFormat::Zstd) => "Zstd",
        Some(CompressionFormat::Bzip2) => "Bzip2",
        Some(CompressionFormat::Xz) => "XZ",
    }
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
    let g = crate::glyphs::get();
    let mut footer = HintBar::from_ctx(ctx).hint_weighted("Enter", "Export", 3);
    match modal.focus {
        ExportFocus::FormatSelector => {
            footer = footer.hint_weighted(g.updown, "Format", 2);
        }
        ExportFocus::CsvIncludeHeader | ExportFocus::SourceFile => {
            footer = footer.hint_weighted("Space", "Toggle", 2);
        }
        ExportFocus::CsvCompression
        | ExportFocus::JsonCompression
        | ExportFocus::NdjsonCompression => {
            footer = footer.hint_weighted(g.updown, "Change", 2);
        }
        ExportFocus::PathInput | ExportFocus::CsvDelimiter => {}
    }
    let footer = footer
        .hint_weighted("Tab", "Next", 1)
        .hint_weighted("Esc", "Cancel", 4);
    let content = Surface::new("Export Data")
        .footer(&footer)
        .render(area, buf, ctx);
    if content.height < 2 || content.width < 4 {
        return;
    }

    // Left: the format picker under its section rule. Right: the path and the
    // chosen format's options, one FormRow each, values on one column.
    let format_focused = modal.focus == ExportFocus::FormatSelector;
    SectionRule {
        title: "Format",
        chip: None,
        focused: format_focused,
    }
    .render(
        Rect {
            width: FORMAT_WIDTH.min(content.width),
            height: 1,
            ..content
        },
        buf,
        ctx,
    );
    let list_area = Rect {
        y: content.y + 1,
        width: FORMAT_WIDTH.min(content.width),
        height: content.height - 1,
        ..content
    };
    let names: Vec<&str> = ExportFormat::ALL.iter().map(|f| f.as_str()).collect();
    let selected = ExportFormat::ALL
        .iter()
        .position(|f| *f == modal.selected_format);
    Picker::new(names, selected, format_focused).render(list_area, buf, ctx);

    modal
        .path_input
        .set_focused(modal.focus == ExportFocus::PathInput);
    modal
        .csv_delimiter_input
        .set_focused(modal.focus == ExportFocus::CsvDelimiter);

    // The rows mirror `focus_order`, so Tab walks what is on screen.
    let mut rows: Vec<(&str, FormValue, ExportFocus)> = vec![(
        "Path:",
        FormValue::Input(&modal.path_input),
        ExportFocus::PathInput,
    )];
    match modal.selected_format {
        ExportFormat::Csv => rows.extend([
            (
                "Delimiter:",
                FormValue::Input(&modal.csv_delimiter_input),
                ExportFocus::CsvDelimiter,
            ),
            (
                "Include header:",
                FormValue::Toggle(modal.csv_include_header),
                ExportFocus::CsvIncludeHeader,
            ),
            (
                "Compression:",
                FormValue::Choice(compression_name(modal.csv_compression)),
                ExportFocus::CsvCompression,
            ),
        ]),
        ExportFormat::Json => rows.push((
            "Compression:",
            FormValue::Choice(compression_name(modal.json_compression)),
            ExportFocus::JsonCompression,
        )),
        ExportFormat::Ndjson => rows.push((
            "Compression:",
            FormValue::Choice(compression_name(modal.ndjson_compression)),
            ExportFocus::NdjsonCompression,
        )),
        ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {}
    }
    if modal.offer_source_file {
        rows.push((
            "Source file:",
            FormValue::Toggle(modal.source_file),
            ExportFocus::SourceFile,
        ));
    }

    let options_x = content.x + FORMAT_WIDTH + 2;
    let options_width = (content.x + content.width).saturating_sub(options_x);
    if options_width == 0 {
        return;
    }
    for (i, (label, value, focus)) in rows.into_iter().enumerate() {
        // Aligned with the format items, one row below the section rule.
        let y = content.y + 1 + i as u16;
        if y >= content.y + content.height {
            break;
        }
        FormRow {
            label,
            value,
            focused: modal.focus == focus,
            label_width: LABEL_WIDTH,
        }
        .render(
            Rect {
                x: options_x,
                y,
                width: options_width,
                height: 1,
            },
            buf,
            ctx,
        );
    }
}
