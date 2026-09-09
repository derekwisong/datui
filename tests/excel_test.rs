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

use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc;

mod common;

/// Pumps the load event chain until the file is open, including background
/// task results delivered over the channel.
fn pump_open_until_loaded(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    loop {
        if let Some(ev) = next.take() {
            if matches!(ev, AppEvent::Crash(_)) {
                app.event(&ev);
                return;
            }
            next = app.event(&ev);
        } else {
            match rx.recv_timeout(std::time::Duration::from_millis(5000)) {
                Ok(ev) => next = Some(ev),
                Err(_) => return,
            }
        }
    }
}

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
    datatable.lf.clone().collect().expect("collect")
}

#[test]
fn reads_headers_and_row_count() {
    let df = open_excel("people.xlsx", OpenOptions::default());

    assert_eq!(df.height(), 1000, "people.xlsx has 1000 data rows");
    assert_eq!(
        df.get_column_names_str(),
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
