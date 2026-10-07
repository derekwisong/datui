//! Export modal rendering: one Surface, a FormRow per field in one column, the
//! actions in the footer. Format is a Choice, so it is drawn as one: its values
//! side by side on its row, where ←/→ visibly step along them.

use crate::CompressionFormat;
use crate::export_modal::{COMPRESSION_OPTIONS, ExportFocus, ExportFormat, ExportModal};
use crate::pointer::FieldId;
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Surface};
use ratatui::layout::Rect;

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
    let g = crate::glyphs::get();
    let mut footer = HintBar::from_ctx(ctx).hint_weighted("Enter", "Export", 3);
    match modal.focus {
        ExportFocus::FormatSelector => {
            footer = footer.hint_weighted(g.updown_lr, "Format", 2);
        }
        ExportFocus::CsvIncludeHeader | ExportFocus::SourceFile => {
            footer = footer.hint_weighted("Space", "Toggle", 2);
        }
        ExportFocus::Compression => {
            footer = footer.hint_weighted(g.updown_lr, "Change", 2);
        }
        ExportFocus::PathInput | ExportFocus::CsvDelimiter => {}
    }
    let footer = footer
        .hint_weighted("Tab", "Next", 1)
        .hint_weighted("Esc", "Cancel", 4);
    crate::pointer::record(area, crate::pointer::Hit::Modal);
    let content = Surface::new("Export Data")
        .footer(&footer)
        .render(area, buf, ctx);
    if content.height < 1 || content.width < 4 {
        return;
    }

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
    let format_field = FieldId::of::<ExportModal>(ExportFocus::FormatSelector);
    let compression_field = FieldId::of::<ExportModal>(ExportFocus::Compression);
    let compression = COMPRESSION_OPTIONS
        .iter()
        .position(|c| *c == modal.compression())
        .unwrap_or(0);
    let fields = modal.focus_order();
    for (i, &field) in fields.iter().enumerate() {
        let y = content.y + i as u16;
        if y >= content.bottom() {
            break;
        }
        let (label, value) = match field {
            ExportFocus::FormatSelector => (
                "Format:",
                FormValue::Options {
                    items: &FORMAT_NAMES,
                    selected,
                    clicks: Some(format_field.clone()),
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
                    clicks: Some(compression_field.clone()),
                },
            ),
            ExportFocus::SourceFile => ("Source file:", FormValue::Toggle(modal.source_file)),
        };
        let row = Rect {
            y,
            height: 1,
            ..content
        };
        // The row first: the format's values, recorded as drawn, lie on top.
        crate::pointer::record_field::<ExportModal>(row, field);
        FormRow {
            label,
            value,
            focused: modal.focus == field,
            label_width: LABEL_WIDTH,
        }
        .render(row, buf, ctx);
    }

    // The reason the form cannot export yet, inline on the last line: a warning
    // at most, never a modal. Otherwise the line says how a format without
    // nesting writes the view's list and struct columns, or that Avro renames.
    // The last line, not under the rows, so it stays put as rows come and go.
    let status = match modal.path_error.as_deref() {
        Some(message) => Some((message, ctx.warning)),
        None if modal.nested_columns && !modal.selected_format.holds_nesting() => {
            Some((NESTED_NOTE, ctx.dimmed))
        }
        None if modal.avro_renames && modal.selected_format == ExportFormat::Avro => {
            Some((AVRO_NAMES_NOTE, ctx.dimmed))
        }
        None => None,
    };
    let Some((message, color)) = status else {
        return;
    };
    // A failed write's reason can run longer than the line: it wraps upward into
    // the rows the format leaves free, keeping a blank under the last field, and
    // is cut with an ellipsis past that.
    let width = content.width.saturating_sub(1) as usize;
    let first_free = content.y + fields.len() as u16 + 1;
    let room = content.bottom().saturating_sub(first_free).max(1) as usize;
    let mut lines = crate::widgets::info::wrap_to(message, width);
    if lines.len() > room {
        lines.truncate(room);
        if let Some(last) = lines.last_mut() {
            let cut = format!("{last} {}", crate::glyphs::get().ellipsis);
            *last = crate::glyphs::fit(&cut, width);
        }
    }
    let top = content.bottom() - lines.len() as u16;
    if top < content.y + fields.len() as u16 {
        return;
    }
    for (i, line) in lines.iter().enumerate() {
        // Under the labels, past the rail gutter.
        ratatui::widgets::Widget::render(
            ratatui::widgets::Paragraph::new(line.as_str())
                .style(ratatui::style::Style::default().fg(color)),
            Rect {
                x: content.x + 1,
                y: top + i as u16,
                width: content.width - 1,
                height: 1,
            },
            buf,
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pointer::Hit;
    use ratatui::buffer::Buffer;

    fn draw(modal: &mut ExportModal, width: u16, height: u16) -> Buffer {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        render_export_modal(area, &mut buf, modal, &RenderContext::for_test());
        buf
    }

    fn lines(buf: &Buffer) -> Vec<String> {
        (0..buf.area.height)
            .map(|y| {
                (0..buf.area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect()
    }

    fn painted(modal: &mut ExportModal, width: u16, height: u16) -> String {
        lines(&draw(modal, width, height)).join("\n")
    }

    /// The dialog's first content row, where Format always sits.
    const FORMAT_Y: u16 = 1;

    /// Format is one row of its values, the chosen one tinted as the table's
    /// current row is, the others plain: ←/→ step along what is drawn.
    #[test]
    fn format_is_one_row_with_the_chosen_value_tinted() {
        let ctx = RenderContext::for_test();
        let tint = ctx.highlight_style().bg.expect("the default theme tints");
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.selected_format = ExportFormat::Tsv;
        let buf = draw(&mut modal, MAX_WIDTH, HEIGHT);
        let rows = lines(&buf);
        let row = &rows[usize::from(FORMAT_Y)];
        assert!(row.contains("Format:"), "{row}");
        let mut at = 0;
        for name in FORMAT_NAMES {
            let found = row[at..].find(name).map(|i| i + at);
            assert!(found.is_some(), "{name} after {at} in {row:?}");
            at = found.unwrap() + name.len();
        }
        // Nothing else lists a format: no column of them below.
        for line in &rows[usize::from(FORMAT_Y) + 1..] {
            assert!(!line.contains("Parquet"), "a second list: {line:?}");
        }
        let x_of = |name: &str| row[..row.find(name).unwrap()].chars().count() as u16;
        let tsv = x_of(" TSV ");
        for x in tsv..tsv + 5 {
            assert_eq!(buf[(x, FORMAT_Y)].bg, tint, "TSV tinted at {x}");
        }
        let csv = x_of("CSV");
        assert_ne!(buf[(csv, FORMAT_Y)].bg, tint, "CSV is not chosen");
        // The chosen text lines up with the other rows' values.
        let path_row = &rows[usize::from(FORMAT_Y) + 1];
        assert!(path_row.contains("Path:"), "{path_row}");
        assert_eq!(
            x_of("TSV") - x_of("CSV"),
            5,
            "padded a cell each side: {row:?}"
        );
        assert_eq!(x_of("CSV"), 2 + 1 + LABEL_WIDTH, "{row:?}");
    }

    /// Compression shows its choices on its row, as Format does, the chosen one
    /// tinted: `None` reads as a choice among others, not a bare word.
    #[test]
    fn compression_shows_its_choices() {
        let ctx = RenderContext::for_test();
        let tint = ctx.highlight_style().bg.expect("the default theme tints");
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.selected_format = ExportFormat::Csv;
        modal.csv_compression = Some(CompressionFormat::Zstd);
        let buf = draw(&mut modal, MAX_WIDTH, HEIGHT);
        let rows = lines(&buf);
        let (y, row) = rows
            .iter()
            .enumerate()
            .find(|(_, r)| r.contains("Compression:"))
            .expect("the compression row");
        for name in COMPRESSION_NAMES {
            assert!(row.contains(&format!(" {name} ")), "{name} in {row:?}");
        }
        let x = row[..row.find(" Zstd ").unwrap()].chars().count() as u16;
        assert_eq!(buf[(x + 1, y as u16)].bg, tint, "Zstd chosen: {row:?}");
    }

    /// Too narrow for every value, the row shows the chosen one alone between
    /// its step marks, and still in the value column.
    #[test]
    fn a_narrow_dialog_shows_the_chosen_format_alone() {
        let g = crate::glyphs::get();
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.selected_format = ExportFormat::Parquet;
        let rows = lines(&draw(&mut modal, 50, HEIGHT));
        let row = &rows[usize::from(FORMAT_Y)];
        let compact = format!("{} Parquet {}", g.choice_prev, g.choice_next);
        assert!(row.contains(&compact), "{row:?}");
        assert!(!row.contains("CSV") && !row.contains("Avro"), "{row:?}");
        let at = row[..row.find("Parquet").unwrap()].chars().count() as u16;
        assert_eq!(at, 2 + 1 + LABEL_WIDTH, "{row:?}");
    }

    /// The format's fields come and go below a Format row that stays put; the
    /// rows are the form's fields, in its order.
    #[test]
    fn fields_follow_the_format_under_a_fixed_format_row() {
        let mut modal = ExportModal::new();
        modal.active = true;
        let shown = |modal: &mut ExportModal| {
            let rows = lines(&draw(modal, MAX_WIDTH, HEIGHT));
            assert!(rows[usize::from(FORMAT_Y)].contains("Format:"));
            ["Delimiter:", "Header:", "Compression:", "Source file:"]
                .into_iter()
                .filter(|label| rows.iter().any(|row| row.contains(label)))
                .collect::<Vec<_>>()
        };
        assert_eq!(shown(&mut modal), ["Delimiter:", "Header:", "Compression:"]);
        modal.selected_format = ExportFormat::Tsv;
        assert_eq!(shown(&mut modal), ["Header:", "Compression:"]);
        modal.selected_format = ExportFormat::Ndjson;
        assert_eq!(shown(&mut modal), ["Compression:"]);
        modal.selected_format = ExportFormat::Parquet;
        assert!(shown(&mut modal).is_empty());
        modal.offer_source_file = true;
        assert_eq!(shown(&mut modal), ["Source file:"]);
    }

    /// A click on a format's value steps the field to it: each value records
    /// where it was drawn, over the row's own record.
    #[test]
    fn each_format_value_is_a_click_target() {
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.selected_format = ExportFormat::Tsv;
        let hits = crate::pointer::recording(|| {
            draw(&mut modal, MAX_WIDTH, HEIGHT);
        });
        let field = Some(FieldId::of::<ExportModal>(ExportFocus::FormatSelector));
        let options: Vec<(u16, usize, usize)> = hits
            .iter()
            .filter_map(|(rect, hit)| match hit {
                Hit::Option {
                    field: f,
                    index,
                    current,
                } if *f == field => Some((rect.width, *index, *current)),
                _ => None,
            })
            .collect();
        let expected: Vec<(u16, usize, usize)> = FORMAT_NAMES
            .iter()
            .enumerate()
            .map(|(i, name)| (name.len() as u16 + 2, i, 1))
            .collect();
        assert_eq!(options, expected);
        let row = hits
            .iter()
            .position(|(_, hit)| {
                *hit == Hit::Field(FieldId::of::<ExportModal>(ExportFocus::FormatSelector))
            })
            .expect("the row is recorded");
        let first_option = hits
            .iter()
            .position(|(_, hit)| matches!(hit, Hit::Option { .. }))
            .unwrap();
        assert!(row < first_option, "the values lie on top of the row");

        // Compact, the step marks step by one.
        let hits = crate::pointer::recording(|| {
            draw(&mut modal, 50, HEIGHT);
        });
        let steps: Vec<(usize, usize)> = hits
            .iter()
            .filter_map(|(_, hit)| match hit {
                Hit::Option {
                    field: f,
                    index,
                    current,
                } if *f == field => Some((*index, *current)),
                _ => None,
            })
            .collect();
        assert_eq!(steps, [(0, 1), (1, 0)]);
    }

    /// A view with list or struct columns hears how CSV writes them; formats
    /// that keep the types, and views without such columns, say nothing.
    #[test]
    fn csv_says_how_nested_columns_are_written() {
        let mut modal = ExportModal::new();
        modal.active = true;
        for width in [50u16, MAX_WIDTH] {
            let out = painted(&mut modal, width, HEIGHT);
            assert!(!out.contains(NESTED_NOTE), "no nested columns: {out}");
            modal.nested_columns = true;
            let out = painted(&mut modal, width, HEIGHT);
            assert!(out.contains(NESTED_NOTE), "missing at {width}: {out}");
            modal.selected_format = ExportFormat::Parquet;
            let out = painted(&mut modal, width, HEIGHT);
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
        for width in [50u16, MAX_WIDTH] {
            let out = painted(&mut modal, width, HEIGHT);
            assert!(!out.contains(AVRO_NAMES_NOTE), "valid names: {out}");
            modal.avro_renames = true;
            let out = painted(&mut modal, width, HEIGHT);
            assert!(out.contains(AVRO_NAMES_NOTE), "missing at {width}: {out}");
            modal.selected_format = ExportFormat::Parquet;
            let out = painted(&mut modal, width, HEIGHT);
            assert!(!out.contains(AVRO_NAMES_NOTE), "Parquet keeps them: {out}");
            modal.selected_format = ExportFormat::Avro;
            modal.avro_renames = false;
        }
    }

    /// An invalid form says why inline, and never with a modal; the line holds
    /// its place whatever the format shows.
    #[test]
    fn the_blank_path_message_renders_inline() {
        let mut modal = ExportModal::new();
        modal.active = true;
        modal.path_error = Some("Enter a file path.".to_string());
        for format in [ExportFormat::Csv, ExportFormat::Parquet] {
            modal.selected_format = format;
            for width in [50u16, MAX_WIDTH] {
                let rows = lines(&draw(&mut modal, width, HEIGHT));
                let at = rows
                    .iter()
                    .position(|row| row.contains("Enter a file path."));
                assert_eq!(at, Some(usize::from(HEIGHT) - 4), "{format:?} at {width}");
            }
        }
    }

    /// At 80 columns the dialog lists every format; at 60 it falls back.
    #[test]
    fn the_dialog_fits_every_format_at_eighty_columns() {
        for (width, full) in [(80u16, true), (60, false)] {
            let screen = Rect::new(0, 0, width, 24);
            let dialog = dialog_area(screen);
            let mut modal = ExportModal::new();
            modal.active = true;
            let mut buf = Buffer::empty(screen);
            render_export_modal(dialog, &mut buf, &mut modal, &RenderContext::for_test());
            let text = lines(&buf).join("\n");
            assert_eq!(text.contains("Avro"), full, "at {width}: {text}");
        }
    }
}
