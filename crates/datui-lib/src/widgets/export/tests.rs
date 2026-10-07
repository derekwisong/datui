use super::*;
use crate::app::pointer::Hit;
use ratatui::buffer::Buffer;

fn draw(modal: &mut ExportModal, width: u16, height: u16) -> Buffer {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    render_export_modal(area, &mut buf, modal, &RenderContext::for_test());
    buf
}

fn painted(modal: &mut ExportModal, width: u16, height: u16) -> String {
    crate::tests::buffer_lines(&draw(modal, width, height)).join("\n")
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
    modal.selected_format = ExportFormat::Tsv;
    let buf = draw(&mut modal, MAX_WIDTH, HEIGHT);
    let rows = crate::tests::buffer_lines(&buf);
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
    modal.selected_format = ExportFormat::Csv;
    modal.csv_compression = Some(CompressionFormat::Zstd);
    let buf = draw(&mut modal, MAX_WIDTH, HEIGHT);
    let rows = crate::tests::buffer_lines(&buf);
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
    modal.selected_format = ExportFormat::Parquet;
    let rows = crate::tests::buffer_lines(&draw(&mut modal, 50, HEIGHT));
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
    let shown = |modal: &mut ExportModal| {
        let rows = crate::tests::buffer_lines(&draw(modal, MAX_WIDTH, HEIGHT));
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
    modal.selected_format = ExportFormat::Tsv;
    let hits = crate::app::pointer::recording(|| {
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
    let hits = crate::app::pointer::recording(|| {
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
    modal.path_error = Some("Enter a file path.".to_string());
    for format in [ExportFormat::Csv, ExportFormat::Parquet] {
        modal.selected_format = format;
        for width in [50u16, MAX_WIDTH] {
            let rows = crate::tests::buffer_lines(&draw(&mut modal, width, HEIGHT));
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
        let mut buf = Buffer::empty(screen);
        render_export_modal(dialog, &mut buf, &mut modal, &RenderContext::for_test());
        let text = crate::tests::buffer_lines(&buf).join("\n");
        assert_eq!(text.contains("Avro"), full, "at {width}: {text}");
    }
}
