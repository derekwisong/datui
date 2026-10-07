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
            table: Some("0".to_string()),
            ..Default::default()
        },
    );
    let named = open_excel(
        "people.xlsx",
        OpenOptions {
            table: Some("Sheet".to_string()),
            ..Default::default()
        },
    );

    assert_eq!(indexed.height(), named.height());
    assert_eq!(indexed.width(), named.width());
    assert_eq!(indexed.height(), 1000);
}

/// A `--table` that misses says what it missed among: the sheets the file has, by
/// index and name, so the remedy is in the message rather than in a second guess.
#[test]
fn a_bad_sheet_error_names_the_sheets_that_exist() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/people.xlsx");
    let from_excel = |sheet: &str| {
        let options = OpenOptions {
            table: Some(sheet.to_string()),
            ..OpenOptions::default()
        };
        datui::table::DataTableState::from_excel(&path, &options)
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

fn sheets_xlsx() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/sheets.xlsx")
}

/// A sheet named like an index is found by its name; an index still picks one.
#[test]
fn a_sheet_name_comes_before_an_index() {
    let named = open_excel(
        "sheets.xlsx",
        OpenOptions {
            table: Some("2023".to_string()),
            ..Default::default()
        },
    );
    assert_eq!(named.height(), 12, "the sheet called 2023");
    let indexed = open_excel(
        "sheets.xlsx",
        OpenOptions {
            table: Some("1".to_string()),
            ..Default::default()
        },
    );
    assert_eq!(indexed.height(), 12, "the second sheet");
}

/// The Excel tab names every sheet with its range and size, from what the open read.
#[test]
fn the_excel_tab_lists_the_sheets() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![sheets_xlsx()], OpenOptions::default());
    let state = app.data_table_state.as_ref().expect("loaded");
    let detail = state.format_detail().expect("an Excel tab");
    assert_eq!(detail.tab, "Excel");
    assert!(
        detail.lines[0].starts_with("3 worksheets"),
        "{:?}",
        detail.lines
    );
    assert!(detail.lines[0].contains("1 hidden"), "{:?}", detail.lines);
    assert_eq!(detail.lines[1], "Opened: Orders");
    let said: Vec<(String, String)> = detail
        .list
        .iter()
        .map(|(k, v)| match v {
            datui::model_files::MetaValue::Text(t) => (k.clone(), t.clone()),
            other => panic!("{other:?}"),
        })
        .collect();
    let times = datui::glyphs::get().times;
    assert_eq!(
        said[0],
        ("Orders".into(), format!("A1:B4, 4 {times} 2, opened"))
    );
    assert_eq!(said[1], ("2023".into(), format!("A1:C13, 13 {times} 3")));
    assert_eq!(said[2].0, "Lookup");
    assert!(said[2].1.ends_with("hidden"), "{said:?}");
}

/// The home screen counts a workbook's sheets from its directory, Enter opens the
/// first and → lists them; a sheet's place opens that sheet.
#[test]
fn the_home_screen_lists_a_workbooks_sheets() {
    use datui::discover;
    let path = sheets_xlsx();
    let mut entry = discover::Entry::for_test(&path, "sheets.xlsx");
    discover::enrich(&mut entry);
    assert_eq!(
        entry.cost.tables,
        Some(2),
        "the hidden sheet is the workbook's own"
    );
    assert!(entry.cost.opens_one && !entry.enter_lists_tables());
    let rows = discover::database_rows(&path);
    let names: Vec<(&str, bool)> = rows
        .iter()
        .map(|r| (r.name.as_str(), r.hidden_by_default()))
        .collect();
    assert_eq!(
        names,
        [("Orders", false), ("2023", false), ("Lookup", true)]
    );

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![rows[1].path.clone()],
        OpenOptions::default(),
    );
    let df = app
        .data_table_state
        .as_ref()
        .expect("the sheet opened")
        .lf()
        .clone()
        .collect()
        .unwrap();
    assert_eq!(df.height(), 12, "2023, not the first sheet");
}
