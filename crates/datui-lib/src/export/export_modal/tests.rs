use super::*;

/// The suggested name follows the format as it steps, and stays a suggestion:
/// typing still replaces it whole.
#[test]
fn a_suggested_path_follows_the_format() {
    let mut modal = ExportModal::new();
    modal.selected_format = ExportFormat::Csv;
    modal.suggest_path("people-export");
    assert_eq!(modal.path_input.value(), "people-export.csv");
    modal.step_format(1);
    assert_eq!(
        modal.path_input.value(),
        format!("people-export.{}", modal.selected_format.extension())
    );
    assert!(modal.path_input.is_suggested());
}

#[test]
fn from_path_reads_the_extension_and_looks_through_compression() {
    assert_eq!(ExportFormat::from_path("out.csv"), Some(ExportFormat::Csv));
    assert_eq!(
        ExportFormat::from_path("a/b/out.PARQUET"),
        Some(ExportFormat::Parquet)
    );
    assert_eq!(
        ExportFormat::from_path("out.csv.gz"),
        Some(ExportFormat::Csv)
    );
    assert_eq!(
        ExportFormat::from_path("out.jsonl"),
        Some(ExportFormat::Ndjson)
    );
    assert_eq!(ExportFormat::from_path("out"), None);
    assert_eq!(ExportFormat::from_path("out.dat"), None);
    // A bare compression extension names no format either way.
    assert_eq!(ExportFormat::from_path("out.gz"), None);
}

/// TSV and PSV are CSV presets: their names pick them, their delimiter is set, and
/// the form offers the header and compression without a delimiter row.
#[test]
fn tsv_and_psv_are_presets_with_their_delimiter_set() {
    assert_eq!(ExportFormat::from_path("out.tsv"), Some(ExportFormat::Tsv));
    assert_eq!(
        ExportFormat::from_path("out.psv.zst"),
        Some(ExportFormat::Psv)
    );
    assert_eq!(ExportFormat::Tsv.preset_delimiter(), Some(b'\t'));
    assert_eq!(ExportFormat::Psv.preset_delimiter(), Some(b'|'));
    assert_eq!(ExportFormat::Csv.preset_delimiter(), None);
    let mut modal = ExportModal::new();
    for format in [ExportFormat::Tsv, ExportFormat::Psv] {
        modal.selected_format = format;
        let order = modal.focus_order();
        assert!(!order.contains(&ExportFocus::CsvDelimiter), "{format:?}");
        assert!(order.contains(&ExportFocus::CsvIncludeHeader), "{format:?}");
        assert!(order.contains(&ExportFocus::Compression), "{format:?}");
    }
    modal.selected_format = ExportFormat::Csv;
    modal.path_input.set_value("out.csv");
    modal.selected_format = ExportFormat::Tsv;
    modal.sync_path_to_format();
    assert_eq!(modal.path_input.value(), "out.tsv");
}

#[test]
fn sync_format_follows_a_known_extension_and_only_that() {
    let mut modal = ExportModal::new();
    modal.selected_format = ExportFormat::Parquet;
    modal.path_input.set_value("out.csv");
    modal.sync_format_to_path();
    assert_eq!(modal.selected_format, ExportFormat::Csv);

    modal.selected_format = ExportFormat::Parquet;
    modal.path_input.set_value("out.dat");
    modal.sync_format_to_path();
    assert_eq!(modal.selected_format, ExportFormat::Parquet);
}

#[test]
fn a_compression_suffix_in_the_path_sets_compression() {
    let mut modal = ExportModal::new();
    modal.path_input.set_value("out.csv.gz");
    modal.sync_format_to_path();
    assert_eq!(modal.selected_format, ExportFormat::Csv);
    assert_eq!(modal.csv_compression, Some(CompressionFormat::Gzip));

    let mut modal = ExportModal::new();
    modal.path_input.set_value("out.jsonl.zst");
    modal.sync_format_to_path();
    assert_eq!(modal.selected_format, ExportFormat::Ndjson);
    assert_eq!(modal.ndjson_compression, Some(CompressionFormat::Zstd));

    // A plain path leaves an explicitly chosen compression standing.
    let mut modal = ExportModal::new();
    modal.csv_compression = Some(CompressionFormat::Gzip);
    modal.path_input.set_value("out.csv");
    modal.sync_format_to_path();
    assert_eq!(modal.csv_compression, Some(CompressionFormat::Gzip));
}

/// A field the new format does not show never keeps focus: stepping the
/// format settles it on one that is shown.
#[test]
fn focus_stays_on_a_shown_field_as_the_format_steps() {
    let mut modal = ExportModal::new();
    for format in ExportFormat::ALL {
        modal.selected_format = format;
        for field in modal.focus_order() {
            modal.selected_format = format;
            crate::app::form::Form::set_focused(&mut modal, field);
            for delta in [1, -1, 1, 1] {
                modal.step_format(delta);
                assert!(
                    modal.focus_order().contains(&modal.focus),
                    "{field:?} from {format:?} lands on {:?} at {:?}",
                    modal.focus,
                    modal.selected_format
                );
            }
        }
    }
}

#[test]
fn picking_a_format_rewrites_the_typed_extension() {
    let mut modal = ExportModal::new();
    modal.path_input.set_value("out.csv");
    modal.selected_format = ExportFormat::Parquet;
    modal.sync_path_to_format();
    assert_eq!(modal.path_input.value(), "out.parquet");

    // The compression suffix goes when the new format cannot carry it...
    let mut modal = ExportModal::new();
    modal.path_input.set_value("data.v2.csv.gz");
    modal.selected_format = ExportFormat::Parquet;
    modal.sync_path_to_format();
    assert_eq!(modal.path_input.value(), "data.v2.parquet");

    // ...and stays when it can.
    let mut modal = ExportModal::new();
    modal.path_input.set_value("out.csv.gz");
    modal.selected_format = ExportFormat::Ndjson;
    modal.sync_path_to_format();
    assert_eq!(modal.path_input.value(), "out.jsonl.gz");

    // An extension naming no format is the user's to keep.
    let mut modal = ExportModal::new();
    modal.path_input.set_value("out.dat");
    modal.selected_format = ExportFormat::Parquet;
    modal.sync_path_to_format();
    assert_eq!(modal.path_input.value(), "out.dat");
}
