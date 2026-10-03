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

/// Shown under the rows of a format that cannot hold lists or structs.
const NESTED_NOTE: &str = "Lists and structs are written as JSON.";

/// Shown under the rows of an Avro export whose view has a name Avro refuses.
const AVRO_NAMES_NOTE: &str = "Column names are made valid for Avro.";

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

    // At full width the format picker sits left under its section rule with
    // the option rows beside it. Narrow, the two-column split would leave the
    // path a few cells, so the format becomes the first row — ↑↓ still cycle
    // it — and every row runs the full width.
    let narrow = content.width < FORMAT_WIDTH + 2 + LABEL_WIDTH + 16;
    let format_focused = modal.focus == ExportFocus::FormatSelector;
    if !narrow {
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
    }

    modal
        .path_input
        .set_focused(modal.focus == ExportFocus::PathInput);
    modal
        .csv_delimiter_input
        .set_focused(modal.focus == ExportFocus::CsvDelimiter);

    // The rows mirror `focus_order`, so Tab walks what is on screen.
    let mut rows: Vec<(&str, FormValue, ExportFocus)> = Vec::new();
    if narrow {
        rows.push((
            "Format:",
            FormValue::Choice(modal.selected_format.as_str()),
            ExportFocus::FormatSelector,
        ));
    }
    rows.push((
        "Path:",
        FormValue::Input(&modal.path_input),
        ExportFocus::PathInput,
    ));
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
        ExportFormat::Tsv | ExportFormat::Psv => rows.extend([
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

    let (options_x, first_y) = if narrow {
        (content.x, content.y)
    } else {
        // Aligned with the format items, one row below the section rule.
        (content.x + FORMAT_WIDTH + 2, content.y + 1)
    };
    let options_width = (content.x + content.width).saturating_sub(options_x);
    if options_width == 0 {
        return;
    }
    let row_count = rows.len() as u16;
    for (i, (label, value, focus)) in rows.into_iter().enumerate() {
        let y = first_y + i as u16;
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

    // The reason the form cannot export yet, inline under the rows: a warning
    // at most, never a modal. Otherwise the line says how a format without
    // nesting writes the view's list and struct columns, or that Avro renames.
    let status = match modal.path_error {
        Some(message) => Some((message, ctx.warning)),
        None if modal.nested_columns && !modal.selected_format.holds_nesting() => {
            Some((NESTED_NOTE, ctx.dimmed))
        }
        None if modal.avro_renames && modal.selected_format == ExportFormat::Avro => {
            Some((AVRO_NAMES_NOTE, ctx.dimmed))
        }
        None => None,
    };
    if let Some((message, color)) = status {
        let y = first_y + row_count + 1;
        if y < content.y + content.height {
            ratatui::widgets::Widget::render(
                ratatui::widgets::Paragraph::new(message)
                    .style(ratatui::style::Style::default().fg(color)),
                Rect {
                    x: options_x,
                    y,
                    width: options_width,
                    height: 1,
                },
                buf,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::buffer::Buffer;

    fn painted(modal: &mut ExportModal, width: u16, height: u16) -> String {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render_export_modal(area, &mut buf, modal, &RenderContext::for_test());
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Narrow, the two-column split would leave the path a few cells: the
    /// format becomes the first row and every row runs the full width.
    #[test]
    fn a_narrow_export_dialog_stacks_instead_of_splitting() {
        let mut modal = ExportModal::new();
        modal.active = true;
        let out = painted(&mut modal, 44, 12);
        assert!(out.contains("Format:"), "format is a row: {out}");
        assert!(out.contains("CSV"), "the choice is echoed: {out}");
        assert!(out.contains("Path:"), "{out}");
        // Wide, the picker keeps its section rule.
        let out = painted(&mut modal, 70, 12);
        assert!(out.contains("Format"), "{out}");
        assert!(out.contains("Parquet"), "the list is visible: {out}");
    }

    /// The dialog's height lists every format, and a preset shows its header and
    /// compression rows without a delimiter.
    #[test]
    fn presets_are_listed_without_a_delimiter_row() {
        let mut modal = ExportModal::new();
        modal.active = true;
        let out = painted(&mut modal, 66, 13);
        for format in ExportFormat::ALL {
            assert!(out.contains(format.as_str()), "{format:?}: {out}");
        }
        assert!(out.contains("Delimiter:"), "{out}");
        modal.selected_format = ExportFormat::Tsv;
        let out = painted(&mut modal, 66, 13);
        assert!(!out.contains("Delimiter:"), "{out}");
        assert!(out.contains("Include header:"), "{out}");
        assert!(out.contains("Compression:"), "{out}");
    }

    /// A view with list or struct columns hears how CSV writes them; formats
    /// that keep the types, and views without such columns, say nothing.
    #[test]
    fn csv_says_how_nested_columns_are_written() {
        let mut modal = ExportModal::new();
        modal.active = true;
        for width in [44u16, 70] {
            let out = painted(&mut modal, width, 12);
            assert!(!out.contains(NESTED_NOTE), "no nested columns: {out}");
            modal.nested_columns = true;
            let out = painted(&mut modal, width, 12);
            assert!(out.contains(NESTED_NOTE), "missing at {width}: {out}");
            modal.selected_format = ExportFormat::Parquet;
            let out = painted(&mut modal, width, 12);
            assert!(!out.contains(NESTED_NOTE), "Parquet keeps them: {out}");
            modal.selected_format = ExportFormat::Csv;
            modal.nested_columns = false;
        }
    }

    /// A view with a name Avro refuses hears it is renamed, and only for Avro.
    #[test]
    fn avro_says_it_renames_columns() {
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.selected_format = ExportFormat::Avro;
        for width in [44u16, 70] {
            let out = painted(&mut modal, width, 12);
            assert!(!out.contains(AVRO_NAMES_NOTE), "valid names: {out}");
            modal.avro_renames = true;
            let out = painted(&mut modal, width, 12);
            assert!(out.contains(AVRO_NAMES_NOTE), "missing at {width}: {out}");
            modal.selected_format = ExportFormat::Parquet;
            let out = painted(&mut modal, width, 12);
            assert!(!out.contains(AVRO_NAMES_NOTE), "Parquet keeps them: {out}");
            modal.selected_format = ExportFormat::Avro;
            modal.avro_renames = false;
        }
    }

    /// An invalid form says why inline, and never with a modal.
    #[test]
    fn the_blank_path_message_renders_inline() {
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.path_error = Some("Enter a file path.");
        for width in [44u16, 70] {
            let out = painted(&mut modal, width, 12);
            assert!(
                out.contains("Enter a file path."),
                "missing at {width}: {out}"
            );
        }
    }
}
