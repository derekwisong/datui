//! Tests for reading Excel workbooks.
//!
//! The Excel path goes through calamine rather than Polars, and it was the only
//! input format with no coverage at all. That gap surfaced during a calamine
//! upgrade: the whole suite passed without once opening a spreadsheet, so it
//! could not say whether the new version still read one correctly.
//!
//! These assert on actual cell values, not just that a load returned something,
//! because the failure mode of an Excel reader upgrade is silently wrong data:
//! shifted columns, dates read as serial numbers, booleans read as integers.

use datui::{App, OpenOptions};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

use common::pump_open_until_loaded;

fn open_excel(name: &str, options: OpenOptions) -> DataFrame {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data").join(name);
    assert!(path.exists(), "missing fixture: {}", path.display());

    pump_open_until_loaded(&mut app, &rx, vec![path], options);

    let datatable = app
        .data_table_state
        .as_ref()
        .unwrap_or_else(|| panic!("{} did not load", name));
    datatable.lf().clone().collect().expect("collect")
}

#[test]
fn reads_headers_and_row_count() {
    let df = open_excel("people.xlsx", OpenOptions::default());

    assert_eq!(df.height(), 1000, "people.xlsx has 1000 data rows");
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|s| s.as_str())
            .collect::<Vec<_>>(),
        [
            "id",
            "first_name",
            "last_name",
            "age",
            "city",
            "state",
            "department",
            "job_title",
            "salary",
            "start_date",
            "active",
        ],
        "the first sheet row is the header, not data"
    );
}

#[test]
fn reads_cell_values_from_the_first_row() {
    let df = open_excel("people.xlsx", OpenOptions::default());

    // Guards against an off-by-one that treats the header as data, and against
    // columns being shifted relative to their names.
    let first_str = |col: &str| {
        df.column(col)
            .unwrap_or_else(|_| panic!("no column {}", col))
            .get(0)
            .unwrap()
            .to_string()
            .trim_matches('"')
            .to_string()
    };

    assert_eq!(first_str("first_name"), "Person1");
    assert_eq!(first_str("last_name"), "Lastname1");
    assert_eq!(first_str("city"), "Bristol");
    assert_eq!(first_str("state"), "MI");
    assert_eq!(first_str("job_title"), "Junior");
}

#[test]
fn reads_numeric_columns_as_numbers() {
    let df = open_excel("people.xlsx", OpenOptions::default());

    // Not strings. A regression here shows up as sorting and charting silently
    // treating salaries as text.
    for col in ["id", "age", "salary"] {
        let dtype = df.column(col).unwrap().dtype();
        assert!(
            dtype.is_primitive_numeric(),
            "{} should be numeric, got {:?}",
            col,
            dtype
        );
    }

    let id = df.column("id").unwrap();
    assert_eq!(id.get(0).unwrap().to_string(), "1");
    assert_eq!(id.get(999).unwrap().to_string(), "1000");
}

#[test]
fn preserves_empty_cells_as_nulls() {
    let df = open_excel("people.xlsx", OpenOptions::default());

    // The department column has gaps in the fixture. An empty cell must stay
    // null rather than becoming an empty string or a zero.
    let department = df.column("department").unwrap();
    assert!(
        department.null_count() > 0,
        "expected empty department cells to read as null"
    );
    assert!(
        department.null_count() < df.height(),
        "department should not be entirely null"
    );
}

#[test]
fn reads_other_workbooks() {
    // Breadth over depth: these have different shapes and column types, so a
    // reader change that only breaks one layout still gets caught.
    for name in ["sales.xlsx", "mixed_types.xlsx", "single_row.xlsx"] {
        let df = open_excel(name, OpenOptions::default());
        assert!(df.width() > 0, "{} produced no columns", name);
        assert!(df.height() > 0, "{} produced no rows", name);
    }
}

#[test]
fn selects_a_sheet_by_index_and_by_name() {
    let indexed = open_excel(
        "people.xlsx",
        OpenOptions {
            excel_sheet: Some("0".to_string()),
            ..Default::default()
        },
    );
    let named = open_excel(
        "people.xlsx",
        OpenOptions {
            excel_sheet: Some("Sheet".to_string()),
            ..Default::default()
        },
    );

    assert_eq!(indexed.height(), named.height());
    assert_eq!(indexed.width(), named.width());
    assert_eq!(indexed.height(), 1000);
}

/// A `--sheet` that misses says what it missed among: the sheets the file has, by
/// index and name, so the remedy is in the message rather than in a second guess.
#[test]
fn a_bad_sheet_error_names_the_sheets_that_exist() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/people.xlsx");
    let from_excel = |sheet: &str| {
        datui::widgets::datatable::DataTableState::from_excel(
            &path,
            None,
            None,
            None,
            None,
            false,
            1,
            Some(sheet),
        )
    };

    let msg = match from_excel("99") {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("sheet 99 should not exist"),
    };
    assert!(msg.contains("99"), "names the index asked for: {msg}");
    assert!(msg.contains("0 'Sheet'"), "lists what exists: {msg}");

    let msg = match from_excel("Nope") {
        Err(e) => format!("{e}"),
        Ok(_) => panic!("sheet 'Nope' should not exist"),
    };
    assert!(msg.contains("'Nope'"), "names the sheet asked for: {msg}");
    assert!(msg.contains("0 'Sheet'"), "lists what exists: {msg}");
}
