use super::*;
#[cfg(feature = "sql")]
use crate::sql_plan::asks_per_row;
use crate::widgets::table::tests::{header_row_string, row_string};
use crate::widgets::table::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::Span;
use ratatui::widgets::StatefulWidget;

thread_local! {
    /// Calls of `compact_rows` on this thread: a test's own thread is the UI thread.
    pub(super) static COMPACTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

fn compactions() -> usize {
    COMPACTIONS.with(std::cell::Cell::get)
}

/// Rows a worker read for `[buffer_start, buffer_end)`, before they are fit.
struct Fill {
    df: DataFrame,
    buffer_start: usize,
    buffer_end: usize,
    num_rows: usize,
    count_known: bool,
}

impl DataTableState {
    /// `path` read as CSV, as an open reads one.
    fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_delimited(path, b',', options)
    }

    /// `path` read as delimited text split on `delimiter`, as an open reads one.
    fn from_delimited(path: &Path, delimiter: u8, options: &OpenOptions) -> Result<Self> {
        let read =
            crate::readers::csv::read_delimited(path, delimiter, options, &Default::default())?;
        Self::from_read(read, options)
    }

    /// Hand `fill` over as the collect worker does: fit as planned from what is
    /// held and shown now, then installed.
    fn land(&mut self, fill: Fill) {
        let plan = self.fill_plan(
            fill.buffer_start,
            fill.buffer_end,
            fill.num_rows,
            fill.count_known,
        );
        self.apply_async_collect(plan.fit(fill.df));
    }
}

/// Data Quality's evidence rows read the downloaded file the dataset scans, so
/// they hold it too: a view captured there is refused like the dataset's own.
#[test]
fn evidence_rows_hold_the_download_they_scan() {
    let dir = tempfile::tempdir().unwrap();
    let mut file =
        crate::download::TempDownload::create(Some(dir.path()), Some("csv")).expect("a temp file");
    std::io::Write::write_all(&mut file, b"id\n1\n2\n").unwrap();
    let download = crate::download::TempDownload::keep(file);
    let state = DataTableState::from_csv(download.path(), &Default::default())
        .unwrap()
        .with_open(OpenFacts {
            download: Some(download.clone()),
            ..Default::default()
        });
    let path = download.path().to_path_buf();
    drop(download);

    let view = state
        .quality_evidence_view(&crate::data_quality::QualityScope::WholeSource, lit(true))
        .unwrap();
    assert!(view.scans_a_download());
    drop(state);
    assert!(path.exists(), "the view still scans it");
    assert_eq!(
        collect_lazy(view.view.lf.clone(), false).unwrap().height(),
        2
    );
    drop(view);
    assert!(!path.exists());
}

/// The same for a compressed CSV: the evidence rows scan the decompressed copy,
/// so they hold it, and it goes with the last state that scans it.
#[test]
fn evidence_rows_hold_the_decompressed_file_they_scan() {
    let source = tempfile::tempdir().unwrap();
    let scratch = tempfile::tempdir().unwrap();
    let gz = source.path().join("rows.csv.gz");
    let mut encoder = flate2::write::GzEncoder::new(
        std::fs::File::create(&gz).unwrap(),
        flate2::Compression::default(),
    );
    std::io::Write::write_all(&mut encoder, b"id\n1\n2\n").unwrap();
    encoder.finish().unwrap();
    let options = crate::OpenOptions {
        temp_dir: Some(scratch.path().to_path_buf()),
        ..Default::default()
    };
    let state = DataTableState::from_csv(&gz, &options).unwrap();
    let path = state
        .decompress_temp_file
        .as_ref()
        .expect("decompressed to a temp file")
        .path()
        .to_path_buf();
    assert!(path.starts_with(scratch.path()));

    let view = state
        .quality_evidence_view(&crate::data_quality::QualityScope::WholeSource, lit(true))
        .unwrap();
    assert!(view.scans_a_temp_file());
    drop(state);
    assert!(path.exists(), "the view still scans it");
    assert_eq!(
        collect_lazy(view.view.lf.clone(), false).unwrap().height(),
        2
    );
    drop(view);
    assert!(!path.exists());
}

/// Scrolling sideways must not count the rows.
///
/// A staged open shows a screen before the count is known, and the count on a
/// cloud hive is a metadata read per object. Column scrolling runs on the thread
/// that draws and reads keys, so a count taken there is a freeze no keystroke can
/// interrupt.
/// The busy-key classifier admits Left/Right on the stated premise that column
/// scroll never collects, which is what this holds.
#[test]
fn scrolling_sideways_does_not_count_the_rows() {
    use polars::prelude::*;

    let frame = || {
        df!(
            "a" => &[1i64, 2, 3],
            "b" => &[4i64, 5, 6],
            "c" => &[7i64, 8, 9],
        )
        .unwrap()
        .lazy()
    };
    let mut lf = frame();
    let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state = DataTableState::from_schema_and_lazyframe(
        schema,
        frame(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    assert_eq!(
        state.num_rows_if_valid(),
        None,
        "a staged open starts without a count"
    );

    state.scroll_right();
    assert_eq!(
        state.num_rows_if_valid(),
        None,
        "scrolling right must leave the count to the background pass"
    );
    state.scroll_left();
    assert_eq!(
        state.num_rows_if_valid(),
        None,
        "and so must scrolling back"
    );
}

/// And it must still scroll. Not counting is only the right answer if the new
/// columns actually reach the screen, so this holds the other half: with a
/// buffer in hand, Right moves the window the display is sliced from.
#[test]
fn scrolling_sideways_still_moves_the_columns() {
    use polars::prelude::*;

    let frame = || {
        df!(
            "a" => &[1i64, 2, 3],
            "b" => &[4i64, 5, 6],
            "c" => &[7i64, 8, 9],
        )
        .unwrap()
        .lazy()
    };
    let mut lf = frame();
    let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state = DataTableState::from_schema_and_lazyframe(
        schema,
        frame(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    // As the app has it once a page has landed: a count and a buffer.
    state.set_num_rows(3);
    state.visible_rows = 3;
    state.visible_termcols = 1;
    state.collect();
    let first = state
        .display_df()
        .map(|df| {
            df.get_column_names()
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();

    state.scroll_right();
    let second = state
        .display_df()
        .map(|df| {
            df.get_column_names()
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    assert_ne!(first, second, "Right should show a different column window");
    assert!(!second.is_empty(), "and should show something");

    state.scroll_left();
    let back = state
        .display_df()
        .map(|df| {
            df.get_column_names()
                .iter()
                .map(|n| n.to_string())
                .collect::<Vec<_>>()
                .join(",")
        })
        .unwrap_or_default();
    assert_eq!(back, first, "Left should come back to where it started");
}

use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};

/// The join drops the rows it read through the frame it replaced.
///
/// The buffer holds rows read at the old schema, through the old scan. Kept, the
/// next paint draws them under the new column order — and `display_slice_df` offsets
/// into them from `buffered_start_row`, so where the user is deep in a dataset it is
/// the wrong rows under the right numbers.
#[test]
fn the_join_drops_the_rows_read_through_the_frame_it_replaced() {
    let narrow = || df!("id" => &[1i64, 2]).unwrap().lazy();
    let wider = || {
        df!("id" => &[1i64, 2], "oops" => &["a", "b"])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![2],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(narrow()).schema.clone(),
        narrow(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    state.visible_rows = 2;
    state.collect();
    assert!(
        state.view.buffered_df.is_some(),
        "there are rows on hand, read at the old schema"
    );

    assert!(
        state
            .join_dataset_schema(FootersFound {
                estimate: None,
                dataset: dataset_of(wider()),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
            .is_ok()
    );

    assert!(
        state.view.buffered_df.is_none(),
        "and they are let go, rather than drawn under the columns that replaced them"
    );
    assert_eq!(
        (state.view.buffered_start_row, state.view.buffered_end_row),
        (0, 0),
        "with nothing left saying which rows they were"
    );
}

/// The join throws away a width measured on the frame it replaced.
///
/// `bytes_per_row` prefers what was observed over what the schema estimates, and the
/// observation belongs to the frame that just went. A dataset that opened two
/// columns wide and gained thirty would plan its first page after the join from the
/// two-column width — against a bucket, a read many times the budget the user set.
/// `install_base` clears it for the same reason.
#[test]
fn the_join_does_not_keep_a_width_measured_on_the_frame_it_replaced() {
    let narrow = || df!("id" => &[1i64, 2]).unwrap().lazy();
    let wide = || {
        df!("id" => &[1i64, 2], "a" => &["x", "y"], "b" => &["x", "y"])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![2],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(narrow()).schema.clone(),
        narrow(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    state.view.observed_bytes_per_row = Some(8);
    let measured_narrow = state.bytes_per_row();

    assert!(
        state
            .join_dataset_schema(FootersFound {
                estimate: None,
                dataset: dataset_of(wide()),
                lf: wide(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
            .is_ok(),
        "nothing is built on the scan here, so the columns go straight in"
    );

    assert!(
        state.bytes_per_row() > measured_narrow,
        "a row of the widened dataset is not planned at the width of the old one: \
         {} vs {measured_narrow}",
        state.bytes_per_row()
    );
}

/// A dataset stops saying a count is coming once one has arrived.
///
/// While a pass is reading its footers the dataset declines to count itself, because
/// that pass is bringing the count — and the footer shows a spinner in place of
/// a number it would otherwise print as fact. When the pass lands on a view built on
/// the scan the columns have to wait, but the row groups describe the same files the
/// frame is already reading, so the number need not: without this the user watches
/// that spinner over a count the dataset is holding.
#[test]
fn a_count_that_has_arrived_is_not_held_back_with_the_columns() {
    let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
    let wider = || {
        df!("id" => (0..100i64).collect::<Vec<_>>(), "oops" => vec!["a"; 100])
            .unwrap()
            .lazy()
    };
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![100],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(rows()).schema.clone(),
        rows(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(vec!["one".to_string()]),
            scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
            count: Arc::new(|_| Ok(vec![vec![100]])),
            offsets: None,
        }),
        footers_pending: Some(Arc::new(|_| None)),
        ..Default::default()
    });
    assert!(
        state.counts_itself_later(),
        "the pass is bringing a count, so the dataset is not going to fetch one"
    );

    // The user is in a query when it lands, so the columns cannot go in.
    state.view.active_query = "select doubled: id * 2".to_string();
    let held = state.join_dataset_schema(FootersFound {
        estimate: None,
        dataset: dataset_of(wider()),
        lf: wider(),
        file_rows: vec![100],
        files: vec!["one".to_string()],
        row_groups: vec![vec![100]],
        remote: None,
    });
    assert!(held.is_err(), "the columns wait for the query to be let go");

    // What the footers said about the files stays, so letting the query go gets the
    // total back rather than sending anyone to fetch it again.
    state.view.active_query.clear();
    state.restore_footer_count();
    assert_eq!(
        state.num_rows_if_valid(),
        Some(100),
        "the count is there, without a read to find it"
    );
    assert!(
        !state.counts_itself_later(),
        "so the dataset stops saying one is coming, and the bar prints a number \
         rather than a spinner over one it is holding"
    );
}

/// A column the second pass could not see goes, rather than breaking every read.
///
/// Columns only join — but the pass reads footers again, and a footer that parsed
/// at the open can fail the second time: a transient error, or the object replaced
/// between the two reads. If that was the only file with a column, the name is left
/// naming nothing. Every page projects `column_order`, so a name the new schema
/// does not have is not a blank column on screen, it is a scan that will not plan.
#[test]
fn a_column_the_second_pass_could_not_see_leaves_the_order() {
    let at_open = || {
        df!("id" => &[1i64], "only_the_first_file_had_this" => &["x"])
            .unwrap()
            .lazy()
    };
    let second_time = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
    let dataset_of = |lf: LazyFrame| {
        let mut lf = lf;
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![1],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
    };

    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset_of(at_open()).schema.clone(),
        at_open(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    assert!(
        state
            .get_column_order()
            .iter()
            .any(|c| c == "only_the_first_file_had_this"),
        "the dataset opened with it"
    );

    assert!(
        state
            .join_dataset_schema(FootersFound {
                estimate: None,
                dataset: dataset_of(second_time()),
                lf: second_time(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            })
            .is_ok()
    );

    assert_eq!(
        state.get_column_order(),
        ["id", "oops"],
        "the name the pass can no longer account for is not left naming nothing"
    );
}

/// Three files of two rows each, and every way they can conflict.
///
/// Each case names the files that hold the column in a type it is not read in, and
/// the rows those files own. The last file is the one that has no start after it,
/// so a run ending there is the case an implementation is most likely to get wrong
/// — and the one that two files of the same length hide, since dropping to the end
/// of the dataset and dropping to the next file's start agree there.
#[test]
fn conflicting_files_become_the_runs_of_rows_they_own() {
    let starts = [0, 2, 4];
    let runs = |conflicts: [bool; 3]| conflicting_row_runs(&starts, 6, &conflicts);

    assert_eq!(runs([false, false, false]), vec![], "nothing conflicts");
    assert_eq!(runs([true, false, false]), vec![(0, 2)], "the first file");
    assert_eq!(
        runs([false, true, false]),
        vec![(2, 4)],
        "a file in the middle ends where the next one begins, not at the end of \
         the dataset"
    );
    assert_eq!(
        runs([false, false, true]),
        vec![(4, 6)],
        "and the last one ends at the end of the dataset"
    );
    assert_eq!(
        runs([true, true, false]),
        vec![(0, 4)],
        "files that touch are one run"
    );
    assert_eq!(runs([true, true, true]), vec![(0, 6)], "as are all of them");
    assert_eq!(
        runs([true, false, true]),
        vec![(0, 2), (4, 6)],
        "files that do not touch are not"
    );
}

/// A file of no rows owns no rows, so it neither makes a run nor breaks one.
///
/// Zero-row Parquet files are written by any pipeline that partitions on a key
/// with no data for some value, so this is a shape datui meets rather than one it
/// has to imagine.
#[test]
fn a_file_of_no_rows_neither_makes_a_run_nor_splits_one() {
    // Files of 2, 0 and 2 rows: the middle one begins and ends at row 2.
    let starts = [0, 2, 2];
    assert_eq!(
        conflicting_row_runs(&starts, 4, &[false, true, false]),
        vec![],
        "a conflicting file with no rows keeps no row out"
    );
    assert_eq!(
        conflicting_row_runs(&starts, 4, &[true, false, true]),
        vec![(0, 4)],
        "and an empty file between two that conflict does not part them"
    );
    assert_eq!(
        conflicting_row_runs(&starts, 4, &[true, true, true]),
        vec![(0, 4)],
        "however it is flagged itself"
    );
}

fn file_schema(
    columns: &[(&str, polars::prelude::DataType)],
    rows: usize,
) -> Option<crate::schema_union::FileFooter> {
    let mut schema = polars::prelude::Schema::with_capacity(columns.len());
    for (name, dtype) in columns {
        schema.with_column((*name).into(), dtype.clone());
    }
    Some(crate::schema_union::FileFooter {
        schema: Arc::new(schema),
        row_group_rows: vec![rows],
        file_bytes: 0,
        row_group_bytes: Vec::new(),
        column_bytes: Vec::new(),
    })
}

fn create_test_lf() -> LazyFrame {
    df! (
        "a" => &[1, 2, 3],
        "b" => &["x", "y", "z"]
    )
    .unwrap()
    .lazy()
}

fn create_large_test_lf() -> LazyFrame {
    df! (
        "a" => (0..100).collect::<Vec<i32>>(),
        "b" => (0..100).map(|i| format!("text_{}", i)).collect::<Vec<String>>(),
        "c" => (0..100).map(|i| i % 3).collect::<Vec<i32>>(),
        "d" => (0..100).map(|i| i % 5).collect::<Vec<i32>>()
    )
    .unwrap()
    .lazy()
}

#[test]
fn test_from_csv() {
    // Ensure sample data is generated before running test
    // Test uncompressed CSV loading
    let path = crate::tests::sample_data_dir().join("3-sfd-header.csv");
    let state = DataTableState::from_csv(&path, &Default::default()).unwrap(); // Uses default buffer params from options
    assert_eq!(state.view.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
}

#[test]
fn test_from_csv_gzipped() {
    // Ensure sample data is generated before running test
    // Test gzipped CSV loading
    let path = crate::tests::sample_data_dir().join("mixed_types.csv.gz");
    let state = DataTableState::from_csv(&path, &Default::default()).unwrap(); // Uses default buffer params from options
    assert_eq!(state.view.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
}

/// `--delimiter` reaches the in-memory readers of every compression, and the
/// one-row read that per-column null values are built from (#290), including
/// for a file that is only readable once decompressed.
#[test]
fn test_delimiter_reaches_every_csv_reader() {
    use std::io::Write;
    let dir = tempfile::tempdir().unwrap();
    let body = b"id|name\n1|NA\n2|x\n";
    let bz = dir.path().join("t.csv.bz2");
    let mut enc = bzip2::write::BzEncoder::new(
        std::fs::File::create(&bz).unwrap(),
        bzip2::Compression::best(),
    );
    enc.write_all(body).unwrap();
    enc.finish().unwrap();
    let xz = dir.path().join("t.csv.xz");
    let mut enc = xz2::write::XzEncoder::new(std::fs::File::create(&xz).unwrap(), 6);
    enc.write_all(body).unwrap();
    enc.finish().unwrap();
    let plain = dir.path().join("t.csv");
    std::fs::write(&plain, body).unwrap();

    let in_memory = OpenOptions {
        delimiter: Some(b'|'),
        decompress_in_memory: true,
        ..Default::default()
    };
    // A global and a per-column value together make the read ask for the schema;
    // the column's own value replaces the global one, so only `x` is null.
    let null_values = OpenOptions {
        delimiter: Some(b'|'),
        null_values: Some(vec!["NA".into(), "name=x".into()]),
        ..Default::default()
    };
    // The same in memory, where the column names have to come from the
    // decompressed bytes rather than the file on disk.
    let null_values_in_memory = OpenOptions {
        decompress_in_memory: true,
        ..null_values.clone()
    };
    for (what, path, opts) in [
        ("bzip2", &bz, &in_memory),
        ("xz", &xz, &in_memory),
        ("null values", &plain, &null_values),
        ("null values, bzip2", &bz, &null_values_in_memory),
        ("null values, xz", &xz, &null_values_in_memory),
    ] {
        let state = DataTableState::from_csv(path, opts).unwrap();
        let df = state.view.lf.clone().collect().unwrap();
        let names: Vec<_> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect();
        assert_eq!(names, ["id", "name"], "{what}");
        if opts.null_values.is_some() {
            assert_eq!(df.column("name").unwrap().null_count(), 1, "{what}");
        }
    }
}

#[test]
fn test_from_delimited_tsv_has_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_tsv_header.tsv");
    let content = "a\tb\tc\td\n1\t2\t3\t4\n5\t6\t7\t8\n";
    std::fs::write(&path, content).unwrap();
    let opts = OpenOptions {
        has_header: Some(true),
        ..Default::default()
    };
    let mut state = DataTableState::from_delimited(&path, b'\t', &opts).unwrap();
    state.collect();
    assert_eq!(state.view.schema.len(), 4);
    assert!(state.view.schema.contains("a"));
    assert!(state.view.schema.contains("b"));
    assert!(state.view.schema.contains("c"));
    assert!(state.view.schema.contains("d"));
    assert_eq!(state.view.num_rows, 2);
}

#[test]
fn test_from_delimited_tsv_no_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_tsv_no_header.tsv");
    let content = "a\tb\tc\td\n1\t2\t3\t4\n5\t6\t7\t8\n";
    std::fs::write(&path, content).unwrap();
    let opts = OpenOptions {
        has_header: Some(false),
        ..Default::default()
    };
    let mut state = DataTableState::from_delimited(&path, b'\t', &opts).unwrap();
    state.collect();
    assert_eq!(state.view.schema.len(), 4);
    assert!(state.view.schema.contains("column_1"));
    assert!(state.view.schema.contains("column_2"));
    assert!(state.view.schema.contains("column_3"));
    assert!(state.view.schema.contains("column_4"));
    assert_eq!(state.view.num_rows, 3);
}

#[test]
fn test_from_delimited_psv_no_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_psv_no_header.psv");
    let content = "x|y|z\n10|20|30\n40|50|60\n";
    std::fs::write(&path, content).unwrap();
    let opts = OpenOptions {
        has_header: Some(false),
        ..Default::default()
    };
    let mut state = DataTableState::from_delimited(&path, b'|', &opts).unwrap();
    state.collect();
    assert_eq!(state.view.schema.len(), 3);
    assert!(state.view.schema.contains("column_1"));
    assert!(state.view.schema.contains("column_2"));
    assert!(state.view.schema.contains("column_3"));
    assert_eq!(state.view.num_rows, 3);
}

#[test]
fn test_sort() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["a".to_string()], false);
    let df = state.view.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
}

#[test]
fn test_select_next_previous() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 10;
    state.table_state.select(Some(5));

    state.select_next();
    assert_eq!(state.table_state.selected(), Some(6));

    state.select_previous();
    assert_eq!(state.table_state.selected(), Some(5));
}

#[test]
fn test_page_up_down() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 20;
    state.collect();

    assert_eq!(state.view.start_row, 0);
    state.page_down();
    assert_eq!(state.view.start_row, 20);
    state.page_down();
    assert_eq!(state.view.start_row, 40);
    state.page_up();
    assert_eq!(state.view.start_row, 20);
    state.page_up();
    assert_eq!(state.view.start_row, 0);
}

#[test]
fn test_scroll_left_right() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    assert_eq!(state.termcol_index, 0);
    state.scroll_right();
    assert_eq!(state.termcol_index, 1);
    state.scroll_right();
    assert_eq!(state.termcol_index, 2);
    state.scroll_left();
    assert_eq!(state.termcol_index, 1);
    state.scroll_left();
    assert_eq!(state.termcol_index, 0);
}

#[test]
fn test_reverse() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["a".to_string()], true);
    assert_eq!(
        state
            .view
            .lf
            .clone()
            .collect()
            .unwrap()
            .column("a")
            .unwrap()
            .get(0)
            .unwrap(),
        AnyValue::Int32(1)
    );
    state.reverse();
    assert_eq!(
        state
            .view
            .lf
            .clone()
            .collect()
            .unwrap()
            .column("a")
            .unwrap()
            .get(0)
            .unwrap(),
        AnyValue::Int32(3)
    );
}

fn column_values(state: &DataTableState, name: &str) -> Vec<Option<i64>> {
    let df = state.view.lf.clone().collect().unwrap();
    df.column(name)
        .unwrap()
        .cast(&DataType::Int64)
        .unwrap()
        .i64()
        .unwrap()
        .iter()
        .collect()
}

#[test]
fn test_sort_puts_nulls_last_in_both_directions() {
    let lf = df!("a" => &[Some(2i64), None, Some(3), None, Some(1)])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

    state.sort(vec!["a".to_string()], true);
    assert_eq!(
        column_values(&state, "a"),
        [Some(1), Some(2), Some(3), None, None]
    );

    state.sort(vec!["a".to_string()], false);
    assert_eq!(
        column_values(&state, "a"),
        [Some(3), Some(2), Some(1), None, None]
    );

    state.reverse();
    assert_eq!(
        column_values(&state, "a"),
        [Some(1), Some(2), Some(3), None, None]
    );
}

#[test]
fn test_multi_column_sort_puts_nulls_last_in_every_column() {
    let lf = df!(
        "a" => &[Some(1i64), None, Some(1), Some(2), Some(1)],
        "b" => &[Some(5i64), Some(9), None, Some(7), Some(6)],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

    state.sort(vec!["a".to_string(), "b".to_string()], false);
    assert_eq!(
        column_values(&state, "a"),
        [Some(2), Some(1), Some(1), Some(1), None]
    );
    assert_eq!(
        column_values(&state, "b"),
        [Some(7), Some(6), Some(5), None, Some(9)]
    );
}

/// Minimal long-format data for pivot tests: id, date, key, value.
/// Includes duplicates for aggregation (e.g. (1,d1,A) appears twice).
fn create_pivot_long_lf() -> LazyFrame {
    let df = df!(
        "id" => &[1_i32, 1, 1, 2, 2, 2, 1, 2],
        "date" => &["d1", "d1", "d1", "d1", "d1", "d1", "d1", "d1"],
        "key" => &["A", "B", "C", "A", "B", "C", "A", "B"],
        "value" => &[10.0_f64, 20.0, 30.0, 40.0, 50.0, 60.0, 11.0, 51.0],
    )
    .unwrap();
    df.lazy()
}

/// Wide-format data for melt tests: id, date, c1, c2, c3.
fn create_melt_wide_lf() -> LazyFrame {
    let df = df!(
        "id" => &[1_i32, 2, 3],
        "date" => &["d1", "d2", "d3"],
        "c1" => &[10.0_f64, 20.0, 30.0],
        "c2" => &[11.0, 21.0, 31.0],
        "c3" => &[12.0, 22.0, 32.0],
    )
    .unwrap();
    df.lazy()
}

/// Dates past the calendar in the second row: a date, and datetimes in ms and
/// us with and without a zone. The first row is 1970-01-01.
fn past_calendar_lf() -> LazyFrame {
    let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
    let datetime = |name: &str, unit, zone: Option<TimeZone>| {
        Series::new(name.into(), [0, i64::MIN + 1])
            .cast(&DataType::Datetime(unit, zone))
            .unwrap()
            .into_column()
    };
    DataFrame::new_infer_height(vec![
        Column::new("id".into(), [1i32, 2]),
        Column::new("s".into(), ["a", "b"]),
        Series::new("d".into(), [0, i32::MAX])
            .cast(&DataType::Date)
            .unwrap()
            .into_column(),
        datetime("t_ms", TimeUnit::Milliseconds, None),
        datetime("t_us", TimeUnit::Microseconds, None),
        datetime("t_ms_tz", TimeUnit::Milliseconds, paris.clone()),
        datetime("t_us_tz", TimeUnit::Microseconds, paris),
    ])
    .unwrap()
    .lazy()
}

const PAST_CALENDAR: [&str; 5] = ["d", "t_ms", "t_us", "t_ms_tz", "t_us_tz"];

/// `column`'s two values as text: Polars' own for the first, and the stored
/// number the table shows for the one past the calendar.
fn past_calendar_text(column: &str) -> [String; 2] {
    let df = past_calendar_lf().collect().unwrap();
    let values = df.column(column).unwrap();
    let first = values
        .slice(0, 1)
        .cast(&DataType::String)
        .unwrap()
        .str()
        .unwrap()
        .get(0)
        .unwrap()
        .to_string();
    let past = crate::exact::past_calendar_text(&values.get(1).unwrap()).unwrap();
    [first, past]
}

/// The address range of every buffer behind `arr`: values, validity, offsets, string
/// views and data, and nested children. Panics on a type it cannot walk, so a test
/// cannot pass by skipping a column.
fn array_storage(arr: &dyn polars_arrow::array::Array, out: &mut Vec<(usize, usize)>) {
    use polars_arrow::array::{
        BinaryViewArray, BooleanArray, FixedSizeListArray, ListArray, NullArray, PrimitiveArray,
        StructArray, Utf8ViewArray,
    };
    fn push<T>(out: &mut Vec<(usize, usize)>, s: &[T]) {
        if !s.is_empty() {
            let start = s.as_ptr() as usize;
            out.push((start, start + std::mem::size_of_val(s)));
        }
    }
    let any = arr.as_any();
    // A null array's validity is Polars' shared zeroed bitmap, owned by no frame.
    if any.downcast_ref::<NullArray>().is_some() {
        return;
    }
    if let Some(validity) = arr.validity() {
        push(out, validity.as_slice().0);
    }
    macro_rules! primitive {
        ($($t:ty),*) => {$(
            if let Some(a) = any.downcast_ref::<PrimitiveArray<$t>>() {
                return push(out, a.values().as_slice());
            }
        )*};
    }
    primitive!(i8, i16, i32, i64, i128, u8, u16, u32, u64, f32, f64);
    if let Some(a) = any.downcast_ref::<BooleanArray>() {
        push(out, a.values().as_slice().0);
    } else if let Some(a) = any.downcast_ref::<Utf8ViewArray>() {
        push(out, a.views().as_slice());
        a.data_buffers()
            .iter()
            .for_each(|b| push(out, b.as_slice()));
    } else if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
        push(out, a.views().as_slice());
        a.data_buffers()
            .iter()
            .for_each(|b| push(out, b.as_slice()));
    } else if let Some(a) = any.downcast_ref::<ListArray<i64>>() {
        push(out, a.offsets().as_slice());
        array_storage(a.values().as_ref(), out);
    } else if let Some(a) = any.downcast_ref::<FixedSizeListArray>() {
        array_storage(a.values().as_ref(), out);
    } else if let Some(a) = any.downcast_ref::<StructArray>() {
        a.values()
            .iter()
            .for_each(|v| array_storage(v.as_ref(), out));
    } else {
        panic!("no storage walk for {:?}", arr.dtype());
    }
}

fn frame_storage(df: &DataFrame) -> Vec<(usize, usize)> {
    let mut out = Vec::new();
    for column in df.columns() {
        for chunk in column.as_materialized_series().chunks() {
            array_storage(chunk.as_ref(), &mut out);
        }
    }
    out
}

/// Whether any buffer behind `held` lies inside one behind `source`.
fn shares_storage(held: &DataFrame, source: &DataFrame) -> bool {
    let source = frame_storage(source);
    frame_storage(held)
        .iter()
        .any(|&(s, e)| source.iter().any(|&(ss, se)| s < se && ss < e))
}

/// Every kind of column a buffer holds: fixed width, booleans, short and long
/// strings, binary, nulls in each, a categorical, an enum, a datetime, a decimal, a
/// list of strings, a fixed-size array, a struct and an all-null column.
fn mixed_frame(start: usize, end: usize) -> DataFrame {
    let rows = start..end;
    let long = |i: usize| format!("{i:>8}-{}", "x".repeat(40));
    let mut df = df!(
        "id" => rows.clone().map(|i| i as i64).collect::<Vec<_>>(),
        "maybe" => rows.clone().map(|i| (i % 3 != 0).then_some(i as f64)).collect::<Vec<_>>(),
        "flag" => rows.clone().map(|i| (i % 5 != 0).then_some(i % 2 == 0)).collect::<Vec<_>>(),
        "short" => rows.clone().map(|i| format!("s{}", i % 100)).collect::<Vec<_>>(),
        "long" => rows.clone().map(|i| (i % 7 != 0).then(|| long(i))).collect::<Vec<_>>(),
    )
    .unwrap();
    let n = end - start;
    let cat = df
        .column("short")
        .unwrap()
        .cast(&DataType::from_categories(Categories::global()))
        .unwrap()
        .with_name("cat".into());
    let when = df
        .column("id")
        .unwrap()
        .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
        .unwrap()
        .with_name("when".into());
    let list: ListChunked = rows
        .clone()
        .map(|i| Some(Series::new("".into(), [long(i), format!("t{i}")])))
        .collect();
    let nested = StructChunked::from_columns(
        "nested".into(),
        n,
        &[
            df.column("id").unwrap().clone(),
            df.column("long").unwrap().clone(),
        ],
    )
    .unwrap();
    let labels: Vec<String> = (0..100).map(|i| format!("s{i}")).collect();
    let labels = FrozenCategories::new(labels.iter().map(String::as_str)).unwrap();
    let label = df
        .column("short")
        .unwrap()
        .cast(&DataType::from_frozen_categories(labels))
        .unwrap()
        .with_name("label".into());
    let bytes = df
        .column("long")
        .unwrap()
        .cast(&DataType::Binary)
        .unwrap()
        .with_name("bytes".into());
    let price = df
        .column("maybe")
        .unwrap()
        .cast(&DataType::Decimal(18, 2))
        .unwrap()
        .with_name("price".into());
    let tags = list.with_name("tags".into()).into_column();
    let pair = tags
        .cast(&DataType::Array(Box::new(DataType::String), 2))
        .unwrap()
        .with_name("pair".into());
    for column in [
        cat,
        label,
        bytes,
        when,
        price,
        tags,
        pair,
        nested.into_column(),
    ] {
        df.with_column(column).unwrap();
    }
    df.with_column(Series::new_null("nothing".into(), n).into_column())
        .unwrap();
    df
}

#[test]
fn compacted_rows_own_their_storage() {
    // A slice of a fill keeps the fill's buffers, rechunked or not; the compacted
    // rows share none of them and read the same.
    let source = mixed_frame(0, 20_000);
    let slice = source.slice(1_000, 1_000);
    assert!(shares_storage(&slice, &source), "the probe sees a slice");
    let mut rechunked = slice.clone();
    rechunked.rechunk_mut();
    assert!(
        shares_storage(&rechunked, &source),
        "a lone chunk rechunked is still the slice"
    );

    let kept = compact_rows(source.clone(), 1_000, 1_000, None);
    assert!(!shares_storage(&kept, &source));
    assert!(kept.equals_missing(&slice));
    assert_eq!(kept.schema(), source.schema());

    // Chunks a slice keeps whole are sliced; past a quarter more, the rows are copied.
    let mut chunked = mixed_frame(0, 1_000);
    for i in 1..4 {
        chunked
            .vstack_mut(&mixed_frame(i * 1_000, (i + 1) * 1_000))
            .unwrap();
    }
    let chunk = |i: usize| chunked.slice(i as i64 * 1_000, 1_000);
    let sliced = trim_rows(chunked.clone(), 1_000, 2_000, None);
    assert!(shares_storage(&sliced, &chunk(1)) && shares_storage(&sliced, &chunk(2)));
    assert!(!shares_storage(&sliced, &chunk(0)) && !shares_storage(&sliced, &chunk(3)));
    let copied = trim_rows(chunked.clone(), 1_500, 1_000, None);
    assert!((0..4).all(|i| !shares_storage(&copied, &chunk(i))));
    assert!(copied.equals_missing(&chunked.slice(1_500, 1_000)));

    // Two chunks: rows from one of them, and rows across both. Across a seam, the
    // rows either side of it are copied apart.
    let mut stitched = mixed_frame(0, 3_000);
    stitched.vstack_mut(&mixed_frame(3_000, 6_000)).unwrap();
    for (offset, len) in [(3_500, 1_000), (2_500, 1_000), (0, 6_000)] {
        for seam in [None, Some(3_000)] {
            let kept = compact_rows(stitched.clone(), offset, len, seam);
            assert!(!shares_storage(&kept, &stitched), "{offset}+{len}");
            assert!(kept.equals_missing(&stitched.slice(offset as i64, len)));
            assert_eq!(kept.schema(), stitched.schema());
            let across = seam.is_some() && offset < 3_000 && 3_000 < offset + len;
            let chunks = if across { 2 } else { 1 };
            assert!(
                kept.columns()
                    .iter()
                    .filter_map(Column::as_series)
                    .all(|s| s.n_chunks() == chunks),
                "{offset}+{len} {seam:?}"
            );
        }
    }

    // A constant column is cut to length, not written out a row at a time.
    let constant = "k".repeat(200);
    let with_constant = mixed_frame(0, 2_000)
        .lazy()
        .with_column(lit(constant.as_str()).alias("constant"))
        .collect()
        .unwrap();
    assert!(matches!(
        with_constant.column("constant").unwrap(),
        Column::Scalar(_)
    ));
    let kept = compact_rows(with_constant.clone(), 500, 1_000, None);
    let Column::Scalar(cut) = kept.column("constant").unwrap() else {
        panic!("the constant column was built out");
    };
    assert_eq!(cut.len(), 1_000);
    assert!(kept.equals_missing(&with_constant.slice(500, 1_000)));
}

/// A state with a 1 MB byte budget and the first column locked.
fn trimming_state(lf: LazyFrame) -> DataTableState {
    let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
    state.view.locked_columns_count = 1;
    state.visible_rows = 40;
    state
}

fn assert_view_rows(state: &DataTableState, source: &DataFrame) {
    let (start, end) = (state.buffered_start(), state.buffered_end());
    assert!(start <= state.view.start_row && state.view.start_row + 40 <= end);
    let held = state.view.buffered_df.as_ref().unwrap();
    assert_eq!(held.height(), end - start);
    assert!(held.equals_missing(&source.slice(start as i64, end - start)));
    let id = |df: &DataFrame, row: usize| df.column("id").unwrap().i64().unwrap().get(row);
    let locked = state.view.locked_df.as_ref().unwrap();
    assert_eq!(locked.get_column_names(), ["id"]);
    assert_eq!(
        id(locked, state.view.start_row - start),
        Some(state.view.start_row as i64)
    );
    let shown = state.view.df.as_ref().unwrap();
    assert_eq!(shown.height(), held.height());
    assert!(
        shown.column("id").is_err(),
        "the locked column is not repeated"
    );
    for frame in [held, locked, shown] {
        assert!(!shares_storage(frame, source), "trimmed rows are let go");
    }
}

#[test]
fn a_trimmed_fill_lets_go_of_the_rows_it_drops() {
    const N: usize = 10_000;
    let source = mixed_frame(0, N);
    let budget_rows = 1024 * 1024 / (source.estimated_size() / N);
    assert!(budget_rows < N * 3 / 4, "the budget trims the fill");

    // Asynchronous: a plain fill over the budget.
    let mut state = trimming_state(source.clone().lazy());
    state.view.num_rows = N;
    state.view.num_rows_valid = true;
    state.view.start_row = 7_500;
    // The worker makes the copy; the UI thread installs it as it is.
    let plan = state.fill_plan(0, N, N, true);
    let fill = source.clone();
    let result = std::thread::spawn(move || {
        let result = plan.fit(fill);
        assert_eq!(compactions(), 1, "the worker copies the rows it keeps");
        result
    })
    .join()
    .unwrap();
    let before = compactions();
    state.apply_async_collect(result);
    assert_eq!(compactions(), before, "the install copies nothing");
    assert!(state.buffered_end() - state.buffered_start() <= budget_rows);
    assert_view_rows(&state, &source);
}

#[test]
fn a_fill_cut_around_a_view_since_left_is_not_installed() {
    // The worker cuts around the view the fill was planned for. A view that jumped
    // past the rows kept, while it was out, keeps what is held and asks again. So
    // does a read that came back short, whose end the cut dropped.
    const N: usize = 10_000;
    let source = mixed_frame(0, N);
    for short in [false, true] {
        let mut state = trimming_state(source.clone().lazy());
        state.view.num_rows = N;
        state.view.num_rows_valid = !short;
        state.view.start_row = 300;
        let asked = if short { N + 5_000 } else { N };
        let result = state.fill_plan(0, asked, asked, !short).fit(source.clone());
        assert!(
            result.start + result.df.height() < 9_000,
            "the fill was cut"
        );
        state.view.start_row = 9_000;
        state.needs_recollect = false;
        state.apply_async_collect(result);
        assert!(
            state.view.buffered_df.is_none(),
            "nothing drawn under wrong numbers (short {short})"
        );
        assert!(state.needs_recollect);
        assert_eq!((state.view.num_rows, state.view.num_rows_valid), (N, true));
    }
}

#[test]
fn an_untrimmed_fill_is_kept_as_collected() {
    // Ordinary paging copies nothing: the buffer is the fill itself.
    let source = mixed_frame(0, 200);
    let mut state = trimming_state(source.clone().lazy());
    state.land(Fill {
        df: source.clone(),
        buffer_start: 0,
        buffer_end: 200,
        num_rows: 200,
        count_known: true,
    });
    let held = state.view.buffered_df.as_ref().unwrap();
    assert_eq!(frame_storage(held), frame_storage(&source));
}

#[test]
fn a_stitched_trim_lets_go_of_both_groups() {
    // Forward and back across a row group boundary: the union is trimmed to the
    // cap, and neither fetched group is held behind the kept rows. The copy is the
    // worker's; installing it copies nothing. Leaving the stitched rows cuts the
    // buffer down to one group and lets the other go, again without a copy.
    const G: usize = 1_000_000;
    const CAP: usize = DEFAULT_MAX_BUFFERED_ROWS;
    let rows = |start: usize, end: usize| {
        Fill {
        df: df!(
            "id" => (start as i64..end as i64).collect::<Vec<i64>>(),
            "name" => (start..end).map(|i| format!("{i:>8}-{}", "y".repeat(24))).collect::<Vec<_>>(),
        )
        .unwrap(),
        buffer_start: start,
        buffer_end: end,
        num_rows: 10 * G,
        count_known: true,
    }
    };
    let check = |state: &DataTableState, fetched: &[&DataFrame]| {
        let (start, end) = (state.buffered_start(), state.buffered_end());
        assert!(start <= state.view.start_row && state.view.start_row + 40 <= end);
        assert_eq!(end - start, CAP, "trimmed back to the cap");
        let held = state.view.buffered_df.as_ref().unwrap();
        let ids = held.column("id").unwrap().i64().unwrap();
        assert_eq!(ids.get(0), Some(start as i64));
        assert_eq!(ids.get(CAP - 1), Some(end as i64 - 1));
        for df in fetched {
            assert!(!shares_storage(held, df));
            assert!(!shares_storage(state.view.locked_df.as_ref().unwrap(), df));
            assert!(!shares_storage(state.view.df.as_ref().unwrap(), df));
        }
    };
    let lf = rows(0, 1).df.lazy();
    for forward in [true, false] {
        let mut state = DataTableState::new(lf.clone(), None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![G; 10]],
                ..Default::default()
            });
        state.view.locked_columns_count = 1;
        state.visible_rows = 40;
        let (first, view) = if forward {
            (G - 60, G - 20)
        } else {
            (G + 20, G - 20)
        };
        assert!(state.scroll_to(first));
        let request = state.prepare_async_collect(None).expect("one group");
        let first = rows(request.buffer_start, request.buffer_end);
        let first_df = first.df.clone();
        state.land(first);

        assert!(state.scroll_to(view));
        let request = state.prepare_async_collect(None).expect("the other group");
        assert_eq!(
            request.buffer_start == G,
            forward,
            "fetched alone, to stitch on"
        );
        let second = rows(request.buffer_start, request.buffer_end);
        let second_df = second.df.clone();
        // Fit on a worker, as the app does, against the rows held when planned.
        let plan = request.plan;
        let result = std::thread::spawn(move || {
            let result = plan.fit(second.df);
            assert!(compactions() > 0, "the worker copies the rows it keeps");
            result
        })
        .join()
        .unwrap();
        let before = compactions();
        state.apply_async_collect(result);
        assert_eq!(compactions(), before, "the install copies nothing");
        check(&state, &[&first_df, &second_df]);

        // A window inside one group: the rows of the other are let go.
        let stitched = state.view.buffered_df.clone().unwrap();
        let (start, end) = (state.buffered_start(), state.buffered_end());
        let (start, end, other) = if forward {
            (G, end, stitched.slice(0, G - start))
        } else {
            (start, G, stitched.slice((G - start) as i64, end - G))
        };
        assert!(state.scroll_to(if forward { G } else { G - 40 }));
        assert!(state.holds_buffer(start, end));
        assert_eq!(compactions(), before, "the cut falls on the seam: a slice");
        state.slice_buffer_into_display();
        assert_eq!((state.buffered_start(), state.buffered_end()), (start, end));
        let held = state.view.buffered_df.as_ref().unwrap();
        assert_eq!(held.height(), end - start);
        assert!(!shares_storage(held, &other));
        assert!(!shares_storage(state.view.df.as_ref().unwrap(), &other));
        let ids = held.column("id").unwrap().i64().unwrap();
        assert_eq!(ids.get(0), Some(state.buffered_start() as i64));
    }
}

#[test]
fn the_files_holding_a_range_of_rows() {
    let offsets = [0, 100, 100, 250, 400];
    assert_eq!(files_holding(&offsets, 0, 10), Some((0, 0)));
    assert_eq!(
        files_holding(&offsets, 95, 10),
        Some((0, 2)),
        "the empty file is skipped"
    );
    assert_eq!(files_holding(&offsets, 100, 10), Some((2, 2)));
    assert_eq!(files_holding(&offsets, 390, 50), Some((3, 3)));
    assert_eq!(files_holding(&offsets, 400, 10), None);
    assert_eq!(files_holding(&[0], 0, 10), None);
}

#[test]
fn a_buffer_over_many_small_files_opens_a_few() {
    // A thousand files of ten rows each.
    let offsets: Vec<usize> = (0..=1000).map(|i| i * 10).collect();
    // A window of 2,000 rows around row 5,000 spans 200 files; it keeps 16.
    let (start, end) = limit_files(&offsets, 5_000, 5_040, 4_000, 6_000, 16);
    assert_eq!(
        files_holding(&offsets, start, end - start).map(|(a, b)| b - a + 1),
        Some(16)
    );
    assert!(
        start <= 5_000 && 5_040 <= end,
        "the view stays: {start}..{end}"
    );
    // A view spanning more files than the limit keeps all of them.
    let (start, end) = limit_files(&offsets, 0, 400, 0, 400, 16);
    assert_eq!((start, end), (0, 400));
    // Few files: unchanged.
    assert_eq!(limit_files(&offsets, 0, 40, 0, 100, 16), (0, 100));
}

/// A dataset written a file a day is mostly empty files in its quiet years; a window
/// opens only the files with rows, and the limit counts only those (#659).
#[test]
fn a_window_passes_over_empty_files() {
    // Every other file empty: file 2i holds rows 10i..10i+10.
    let offsets: Vec<usize> = (0..=1000_usize).map(|i| i.div_ceil(2) * 10).collect();
    let (first, last) = files_holding(&offsets, 0, 40).unwrap();
    assert_eq!((first, last), (0, 6));
    assert_eq!(files_with_rows(&offsets, first, last), vec![0, 2, 4, 6]);
    let (start, end) = limit_files(&offsets, 0, 40, 0, 2_000, 16);
    let (first, last) = files_holding(&offsets, start, end - start).unwrap();
    assert_eq!(
        files_with_rows(&offsets, first, last).len(),
        16,
        "sixteen files with rows, not eight and the empty ones between"
    );
}

/// The local route to the values a type conflict hides: real Parquet files that
/// disagree, read through `lenient_scan` rather than through a dataset's own scan
/// closure. The remote route above shares none of this code.
#[test]
fn a_local_dataset_reads_the_values_a_type_conflict_hides() {
    use crate::data_quality::{DataQualityPlan, ObservationKind, QualityCompute, QualityScope};
    use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
    use polars::prelude::{DataType, ParquetWriter, df};

    let dir = tempfile::tempdir().unwrap();
    let write = |name: &str, mut frame: polars::prelude::DataFrame| -> String {
        let path = dir.path().join(name);
        let file = std::fs::File::create(&path).unwrap();
        ParquetWriter::new(file).finish(&mut frame).unwrap();
        path.to_string_lossy().to_string()
    };
    // `n` is an integer in the first file and text in the second, so the scan reads
    // it as Int64 and leaves the second file's values behind entirely.
    let files = vec![
        write(
            "a.parquet",
            df!("id" => &[0i64, 1, 2], "n" => &[10i64, 20, 30]).unwrap(),
        ),
        write(
            "b.parquet",
            df!("id" => &[3i64, 4], "n" => &["sixty", "seventy"]).unwrap(),
        ),
    ];
    let dataset: DatasetSchema = union_file_schemas(
        &[
            file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
            file_schema(&[("id", DataType::Int64), ("n", DataType::String)], 2),
        ],
        SchemaOrigin::AllFooters(2),
    );
    let file_rows = vec![3usize, 2];
    let drift = crate::schema_union::ScanDrift::new(&files, &dataset, &file_rows);
    let lf = crate::schema_union::lenient_scan(
        &files,
        dataset.schema.clone(),
        None,
        drift.as_ref(),
        &[],
    )
    .unwrap();
    let state = DataTableState::from_schema_and_lazyframe(
        dataset.schema.clone(),
        lf,
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(OpenFacts {
        dataset: Some(DatasetAtOpen {
            schema: dataset,
            file_rows,
            files,
        }),
        ..Default::default()
    });
    assert!(state.drifts(), "the two files disagree on `n`");
    assert_eq!(
        state.quality_conflict_reads(),
        1,
        "one column, in one file, before anything runs"
    );

    let (lf, source) = state.data_quality_source_scan();
    let mut source = source.expect("every file is counted");
    source.conflict_scan = state.quality_conflict_scan();
    let lf = crate::data_quality::prepare_source_quality_scan(lf, Some(&source)).unwrap();
    let plan = DataQualityPlan {
        scope: QualityScope::WholeSource,
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results =
        crate::data_quality::compute_data_quality(&lf, Some(5), &plan, Some(&source), false)
            .unwrap();
    let conflict = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::TypeConflict)
        .expect("`n` is text in the second file");
    let file = conflict.files.first().expect("the file that disagrees");
    assert_eq!(file.number, 2);
    assert_eq!(
        file.examples,
        vec!["sixty".to_string(), "seventy".to_string()],
        "read at the type that file wrote, not as the null the scan hands back"
    );
}

/// A remote dataset reads its column as text too, and its windowed reads with it.
///
/// The remote branch takes a different route: the scan closure it was opened with
/// is asked again with the column named, and `buffer_lf` passes the same names on
/// every later window. Miss that second half and a cloud dataset would read the
/// first screen as text and the next one as it was, disagreeing with its own
/// schema.
#[test]
fn a_remote_dataset_reads_a_conflicting_column_as_text_on_every_window() {
    use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
    use polars::prelude::{DataType, IntoLazy, df};

    let urls: Vec<String> = vec!["a".to_string(), "b".to_string()];
    // The scan the dataset was opened with, standing in for the cloud one: the
    // first file holds `n` as an integer and the second as text.
    let scan: FileScan = Arc::new(move |urls: &[String], as_text: &[PlSmallStr]| {
        let frames: Vec<LazyFrame> = urls
            .iter()
            .map(|url| {
                // The hidden row index the real scan stamps, numbered from where
                // the file's rows begin in the dataset so a window keeps its place.
                let frame = if url == "a" {
                    df!(
                        "id" => &[0i64, 1, 2],
                        "n" => &[10i64, 20, 30],
                        crate::schema_union::DRIFT_COLUMN => &[0u32, 1, 2],
                    )
                    .unwrap()
                } else {
                    df!(
                        "id" => &[3i64, 4],
                        "n" => &["sixty", "seventy"],
                        crate::schema_union::DRIFT_COLUMN => &[3u32, 4],
                    )
                    .unwrap()
                };
                let lf = frame.lazy();
                if as_text.contains(&PlSmallStr::from("n")) {
                    lf.with_column(col("n").cast(DataType::String))
                } else if url == "a" {
                    lf
                } else {
                    // Not read from this file at all, as the real scan leaves it.
                    lf.with_column(lit(NULL).cast(DataType::Int64).alias("n"))
                }
            })
            .collect();
        polars::prelude::concat(frames, Default::default())
    });

    let dataset: DatasetSchema = union_file_schemas(
        &[
            file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
            file_schema(&[("id", DataType::Int64), ("n", DataType::String)], 2),
        ],
        SchemaOrigin::AllFooters(2),
    );
    // The dataset's schema, which does not name the hidden row index the frame
    // carries — as the real open does it.
    let mut state = DataTableState::from_schema_and_lazyframe(
        dataset.schema.clone(),
        scan(&urls, &[]).unwrap(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap()
    .with_open(OpenFacts {
        remote_source: true,
        remote_files: Some(RemoteFiles {
            urls: Arc::new(urls.clone()),
            scan,
            count: Arc::new(|_| Ok(vec![vec![3], vec![2]])),
            offsets: None,
        }),
        dataset: Some(DatasetAtOpen {
            schema: dataset,
            file_rows: vec![3, 2],
            files: urls.clone(),
        }),
        ..Default::default()
    });
    let groups = (state.remote_files_counter().unwrap())(&Default::default()).unwrap();
    assert!(state.count_landed(state.len_generation(), 5, Some(&groups)));
    assert!(state.drifts(), "the two files disagree on `n`");

    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );
    assert_eq!(
        state.view.schema.get("n"),
        Some(&DataType::String),
        "the column is text now"
    );

    // Every window, not only the first: the second file's rows are on their own
    // page, and they are the ones the conflict was hiding.
    let text = |state: &DataTableState, start: usize, rows: usize| -> Vec<String> {
        collect_lazy(state.buffer_lf(start, rows).unwrap(), false)
            .unwrap()
            .column("n")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|value| value.unwrap_or("null").to_string())
            .collect()
    };
    assert_eq!(text(&state, 0, 3), ["10", "20", "30"]);
    assert_eq!(
        text(&state, 3, 2),
        ["sixty", "seventy"],
        "the page that needed the text read most"
    );
}

#[test]
fn a_counted_remote_dataset_reads_only_the_files_a_buffer_needs() {
    use polars::prelude::IntoLazy;
    let part = |from: i32| {
        polars::df!("n" => (from..from + 100).collect::<Vec<i32>>())
            .unwrap()
            .lazy()
    };
    let urls: Vec<String> = (0..5).map(|i| format!("file{i}")).collect();
    let asked = Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
    let scan: FileScan = {
        let asked = asked.clone();
        Arc::new(move |urls: &[String], _as_text: &[PlSmallStr]| {
            asked.lock().unwrap().push(urls.to_vec());
            let frames: Vec<LazyFrame> = urls
                .iter()
                .map(|u| part(u.trim_start_matches("file").parse::<i32>().unwrap() * 100))
                .collect();
            polars::prelude::concat(frames, Default::default())
        })
    };
    let full = scan(&urls, &[]).unwrap();
    asked.lock().unwrap().clear();
    let mut state = DataTableState::from_lazyframe(full, &crate::OpenOptions::default())
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(urls),
                scan,
                count: Arc::new(|_| Ok(vec![vec![50, 50]; 5])),
                offsets: None,
            }),
            ..Default::default()
        });
    let groups = (state.remote_files_counter().unwrap())(&Default::default()).unwrap();
    assert!(state.count_landed(state.len_generation(), 500, Some(&groups)));
    assert_eq!(state.num_rows_if_valid(), Some(500));
    assert!(state.remote_files_counter().is_none(), "counted once");

    let df = collect_lazy(state.buffer_lf(350, 20).unwrap(), false).unwrap();
    let values: Vec<i32> = df
        .column("n")
        .unwrap()
        .i32()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(values, (350..370).collect::<Vec<i32>>());
    assert_eq!(*asked.lock().unwrap(), vec![vec!["file3".to_string()]]);

    asked.lock().unwrap().clear();
    let df = collect_lazy(state.buffer_lf(190, 20).unwrap(), false).unwrap();
    assert_eq!(df.height(), 20);
    assert_eq!(
        *asked.lock().unwrap(),
        vec![vec!["file1".to_string(), "file2".to_string()]],
        "a range across a boundary reads both files"
    );
}

/// What an open finds arrives in one step, in the order it depends on: a many-file
/// dataset's row groups land after its files, so they count it and place each page
/// in its files; a single object's footer counts it.
#[test]
fn an_open_s_findings_arrive_together() {
    let lf = || df!("a" => (0..100i32).collect::<Vec<_>>()).unwrap().lazy();
    let many = DataTableState::from_lazyframe(lf(), &crate::OpenOptions::default())
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(vec!["one".to_string(), "two".to_string()]),
                scan: Arc::new(move |_: &[String], _: &[PlSmallStr]| Ok(lf())),
                count: Arc::new(|_| Err("counted at the open".to_string())),
                offsets: None,
            }),
            row_groups: vec![vec![30, 30], vec![40]],
            ..Default::default()
        });
    assert_eq!(many.num_rows_if_valid(), Some(100));
    assert_eq!(
        many.files_a_page_reads(50, 20),
        Some(2),
        "rows 50..70 span both"
    );
    assert!(
        many.remote_files_counter().is_none(),
        "nothing left to count"
    );

    let one = DataTableState::from_lazyframe(lf(), &crate::OpenOptions::default())
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![60, 40]],
            ..Default::default()
        });
    assert_eq!(one.num_rows_if_valid(), Some(100));
    assert!(one.is_remote_source());
}

#[test]
fn a_remote_source_buffers_one_window_and_pages_inside_it_for_free() {
    // Every buffer fill of an object-store scan downloads whole row groups, so the
    // buffer is one window of `max_buffered_rows` rather than a few pages: paging
    // inside it asks for nothing, and a jump asks once.
    let lf = df!("a" => &[0i32]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            ..Default::default()
        });
    state.view.num_rows = 1_000_000;
    state.view.num_rows_valid = true;
    state.visible_rows = 40;
    let window = |start: usize| Fill {
        df: df!("a" => (0..10_000).collect::<Vec<i32>>()).unwrap(),
        buffer_start: start,
        buffer_end: start + 10_000,
        num_rows: 1_000_000,
        count_known: true,
    };

    let request = state.prepare_async_collect(None).expect("first fill");
    assert_eq!((request.buffer_start, request.buffer_end), (0, 10_000));
    state.land(window(0));
    for _ in 0..20 {
        assert!(!state.page_down(), "a page inside the window needs no fill");
    }

    assert!(state.scroll_to_end());
    let request = state
        .prepare_async_collect(None)
        .expect("the jump fills once");
    assert_eq!(
        (request.buffer_start, request.buffer_end),
        (990_000, 1_000_000)
    );
    state.land(window(990_000));

    assert!(state.scroll_to_start(), "Home after End must fill again");
    let request = state
        .prepare_async_collect(None)
        .expect("one fill at the top");
    assert_eq!((request.buffer_start, request.buffer_end), (0, 10_000));
}

#[test]
fn align_to_row_groups_takes_the_view_groups_whole_and_lookahead_within_the_cap() {
    let offsets = [0, 1_000_000, 2_000_000, 3_000_000, 3_500_000];
    // A window inside one group is that group, whatever the row cap.
    assert_eq!(
        align_to_row_groups(&offsets, 50, 97, 0, 100_000, 100_000),
        (0, 1_000_000)
    );
    // A view straddling a boundary takes both groups, over the cap.
    assert_eq!(
        align_to_row_groups(&offsets, 999_980, 1_000_020, 950_000, 1_050_000, 100_000),
        (0, 2_000_000)
    );
    // Groups the window reaches into come along while they fit, the one ahead first.
    assert_eq!(
        align_to_row_groups(
            &offsets, 1_500_000, 1_500_047, 950_000, 2_050_000, 2_000_000
        ),
        (1_000_000, 3_000_000)
    );
    assert_eq!(
        align_to_row_groups(&offsets, 1_500_000, 1_500_047, 950_000, 2_050_000, 0),
        (0, 3_000_000)
    );
    // The last, short group; and a window past the data is clamped to it.
    assert_eq!(
        align_to_row_groups(
            &offsets, 3_400_000, 3_400_047, 3_350_000, 3_450_000, 100_000
        ),
        (3_000_000, 3_500_000)
    );
    // No groups known: the window is left alone.
    assert_eq!(align_to_row_groups(&[0], 5, 10, 0, 100, 50), (0, 100));
}

#[test]
fn a_remote_object_is_read_inside_its_row_group() {
    // Ten 1M-row groups with the default 100k-row cap: the window is planned inside
    // the group the view is in, so it never pulls the next group before the view
    // reaches it; crossing fetches rows of the next group alone, stitched on to the
    // ones on hand and trimmed back to the cap.
    const G: usize = 1_000_000;
    const CAP: usize = DEFAULT_MAX_BUFFERED_ROWS;
    let lf = df!("a" => &[0i32]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![G; 10]],
            ..Default::default()
        });
    assert_eq!(state.view.num_rows, 10 * G);
    state.visible_rows = 40;
    let rows = |start: usize, end: usize| Fill {
        df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
        buffer_start: start,
        buffer_end: end,
        num_rows: 10 * G,
        count_known: true,
    };

    let request = state.prepare_async_collect(None).expect("first fill");
    assert_eq!((request.buffer_start, request.buffer_end), (0, CAP));
    state.land(rows(0, CAP));
    for _ in 0..20 {
        assert!(!state.page_down(), "a page inside the window needs no fill");
    }

    // A jump to the end of group 0 is clipped to it: group 1 is not touched yet.
    assert!(state.scroll_to(G - 60));
    let request = state
        .prepare_async_collect(None)
        .expect("the end of group 0");
    assert_eq!((request.buffer_start, request.buffer_end), (G - CAP, G));
    state.land(rows(G - CAP, G));

    // A view straddling the boundary fetches rows of group 1 alone.
    assert!(state.scroll_to(G - 20));
    let request = state.prepare_async_collect(None).expect("into group 1");
    assert_eq!((request.buffer_start, request.buffer_end), (G, G + CAP / 2));
    state.land(rows(G, G + CAP / 2));
    let (held_start, held_end) = (state.buffered_start(), state.buffered_end());
    assert!(
        held_start <= G - 20 && G + 20 <= held_end,
        "the view is on hand"
    );
    assert_eq!(held_end - held_start, CAP, "trimmed back to the cap");
    let held = state.view.buffered_df.as_ref().unwrap();
    assert_eq!(held.height(), CAP);
    assert_eq!(
        held.column("a").unwrap().i32().unwrap().get(G - held_start),
        Some(G as i32),
        "stitched in order"
    );
    assert!(!state.page_down());

    // End and Home are one window each.
    assert!(state.scroll_to_end());
    let request = state.prepare_async_collect(None).expect("the last window");
    assert_eq!(
        (request.buffer_start, request.buffer_end),
        (10 * G - CAP, 10 * G)
    );
    state.land(rows(10 * G - CAP, 10 * G));
    assert!(state.scroll_to_start());
    let request = state.prepare_async_collect(None).expect("the first window");
    assert_eq!((request.buffer_start, request.buffer_end), (0, CAP));
}

#[test]
fn small_row_groups_are_fetched_whole() {
    // 40k-row groups under a 100k cap: a fill is whole groups, as many as fit,
    // and crossing into the next fetches exactly that group.
    const G: usize = 40_000;
    let lf = df!("a" => &[0i32]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![G; 25]],
            ..Default::default()
        });
    state.visible_rows = 40;
    let rows = |start: usize, end: usize| Fill {
        df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
        buffer_start: start,
        buffer_end: end,
        num_rows: 25 * G,
        count_known: true,
    };

    let request = state.prepare_async_collect(None).expect("first fill");
    assert_eq!((request.buffer_start, request.buffer_end), (0, 2 * G));
    state.land(rows(0, 2 * G));

    assert!(state.scroll_to(2 * G - 20));
    let request = state.prepare_async_collect(None).expect("the next group");
    assert_eq!((request.buffer_start, request.buffer_end), (2 * G, 3 * G));
    state.land(rows(2 * G, 3 * G));
    let (held_start, held_end) = (state.buffered_start(), state.buffered_end());
    assert!(held_start <= 2 * G - 20 && 2 * G + 20 <= held_end);
    assert!(held_end - held_start <= DEFAULT_MAX_BUFFERED_ROWS);
}

#[test]
fn a_wide_schema_is_budgeted_before_the_collect() {
    // 1,000 Float64 columns are 8,000 bytes a row: a 512 MB budget allows 67,108
    // rows, so the planned window is that and not the 100k row cap. The rows are
    // never materialized only to be trimmed after the collect.
    let columns: Vec<Column> = (0..1000)
        .map(|i| Series::new(format!("f{i}").into(), &[0.0f64]).into())
        .collect();
    let lf = DataFrame::new(1, columns).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![1_000_000; 3]],
            ..Default::default()
        });
    assert_eq!(
        estimate_bytes_per_row(&state.view.schema, &state.view.column_order, &[]),
        8_000
    );
    state.visible_rows = 40;

    let request = state.prepare_async_collect(None).expect("first fill");
    let planned = request.buffer_end - request.buffer_start;
    assert!(
        planned <= 512 * 1024 * 1024 / 8_000,
        "planned {planned} rows over the byte budget"
    );
    assert!(planned >= 40, "never below a screen");
    assert_eq!(
        request.buffer_start, 0,
        "a window at the top starts at the top"
    );

    // The screen is the floor, whatever the budget.
    let tiny = |bytes: usize| {
        let mut tiny = DataTableState::new(
            df!("a" => &["x".repeat(2_000)]).unwrap().lazy(),
            None,
            None,
            None,
            Some(1),
            true,
        )
        .unwrap()
        .with_open(OpenFacts {
            column_bytes: vec![("a".to_string(), bytes)],
            ..Default::default()
        });
        tiny.visible_rows = 40;
        tiny.byte_cap_rows()
    };
    assert_eq!(tiny(2_000), 1024 * 1024 / 2_016);
    assert_eq!(tiny(1 << 20), 40);
}

#[test]
fn a_collected_buffer_measures_the_next_plan() {
    // The schema guesses 40 bytes for a string; the first buffer shows the strings
    // are 2 KB, and the next window is planned on that.
    let big: Vec<String> = (0..100).map(|_| "z".repeat(2_000)).collect();
    let lf = df!("a" => &big).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
    state.view.num_rows = 1_000_000;
    state.view.num_rows_valid = true;
    state.visible_rows = 40;
    let guessed = state.byte_cap_rows();
    assert_eq!(guessed, 1024 * 1024 / STRING_BYTES_GUESS);
    state.land(Fill {
        df: df!("a" => &big).unwrap(),
        buffer_start: 0,
        buffer_end: 100,
        num_rows: 1_000_000,
        count_known: true,
    });
    let measured = state.byte_cap_rows();
    assert!(
        (400..=600).contains(&measured),
        "about 1 MB / 2 KB rows, got {measured}"
    );
}

#[test]
fn a_count_below_the_view_brings_the_view_back() {
    // A filter applied deep in the data: the frame turns out to have 100 rows and
    // the view was at 9,990. It comes back to the data and asks for a fill.
    let lf = df!("a" => (0..10_000i32).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 10;
    state.view.num_rows = 10_000;
    state.view.num_rows_valid = true;
    assert!(state.scroll_to_end());
    assert_eq!(state.view.start_row, 9_990);
    state.set_num_rows(100);
    assert_eq!(state.view.start_row, 90);
    assert!(state.needs_recollect);

    // A slice deep in the frame that found nothing is not the count.
    state.needs_recollect = false;
    state.view.num_rows_valid = false;
    state.view.start_row = 9_990;
    state.land(Fill {
        df: df!("a" => Vec::<i32>::new()).unwrap(),
        buffer_start: 9_990,
        buffer_end: 10_060,
        num_rows: 10_060,
        count_known: false,
    });
    assert!(
        !state.view.num_rows_valid,
        "only the count can say where it ends"
    );
}

#[test]
fn a_stale_stitch_keeps_the_rows_on_hand() {
    // A fill planned to run on from rows since replaced neither abuts what is held
    // nor shows the view: installing it would draw rows under the wrong numbers.
    const G: usize = 1_000_000;
    let lf = df!("a" => &[0i32]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![G; 10]],
            ..Default::default()
        });
    state.visible_rows = 40;
    let rows = |start: usize, end: usize| Fill {
        df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
        buffer_start: start,
        buffer_end: end,
        num_rows: 10 * G,
        count_known: true,
    };
    assert!(state.scroll_to(G - 60));
    let request = state
        .prepare_async_collect(None)
        .expect("the end of group 0");
    state.land(rows(request.buffer_start, request.buffer_end));
    assert!(state.scroll_to(G - 20));
    let stitch = state.prepare_async_collect(None).expect("into group 1");
    assert_eq!(stitch.buffer_start, G);

    // Meanwhile a synchronous collect moved the view and replaced the buffer.
    assert!(state.scroll_to(500_000));
    state.land(rows(450_000, 550_000));
    state.needs_recollect = false;

    // Fit as the worker did, against the rows on hand when it was planned.
    let fill = rows(stitch.buffer_start, stitch.buffer_end);
    state.apply_async_collect(stitch.plan.fit(fill.df));
    assert_eq!(
        (state.buffered_start(), state.buffered_end()),
        (450_000, 550_000),
        "the rows on hand stay"
    );
    assert!(state.needs_recollect, "and a fill is asked for");
}

#[test]
fn a_fill_that_holds_the_first_row_is_kept_when_the_view_grew() {
    // The terminal grew while a row group was downloading: the fill holds the view's
    // first row but not its last. It is installed, and the rest asked for, rather than
    // thrown away.
    const G: usize = 1_000_000;
    let lf = df!("a" => &[0i32]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![G; 10]],
            ..Default::default()
        });
    state.visible_rows = 40;
    assert!(state.scroll_to(G - 60));
    let request = state
        .prepare_async_collect(None)
        .expect("the end of group 0");
    assert!(request.buffer_end <= G);

    state.visible_rows = 120; // resized while the fetch was out
    state.needs_recollect = false;
    state.land(Fill {
        df: df!("a" => (request.buffer_start as i32..request.buffer_end as i32)
            .collect::<Vec<i32>>())
        .unwrap(),
        buffer_start: request.buffer_start,
        buffer_end: request.buffer_end,
        num_rows: 10 * G,
        count_known: true,
    });
    assert_eq!(
        (state.buffered_start(), state.buffered_end()),
        (request.buffer_start, request.buffer_end),
        "the downloaded rows are kept"
    );
    assert!(state.needs_recollect, "and the rest of the view is fetched");
}

#[test]
fn the_byte_cap_governs_the_alignment() {
    // Rows of a kilobyte under a 64 MB budget allow 66k rows, fewer than the 100k row
    // cap; with 40k-row groups the first fill must be group 0 exactly, not a window
    // cut through group 1 that a later refill downloads again.
    const G: usize = 40_000;
    let lf = df!("a" => &["x"]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, Some(64), true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![G; 5]],
            column_bytes: vec![("a".to_string(), 1_000)],
            ..Default::default()
        });
    state.visible_rows = 40;
    let cap = state.byte_cap_rows();
    assert!(
        (G..2 * G).contains(&cap),
        "cap {cap} between one and two groups"
    );
    let rows = |start: usize, end: usize| Fill {
        df: df!("a" => (start..end).map(|i| "x".repeat(8 + i % 3)).collect::<Vec<_>>()).unwrap(),
        buffer_start: start,
        buffer_end: end,
        num_rows: 5 * G,
        count_known: true,
    };

    let request = state.prepare_async_collect(None).expect("first fill");
    assert_eq!((request.buffer_start, request.buffer_end), (0, G));
    state.land(rows(0, G));

    assert!(state.scroll_to(G - 20));
    let request = state.prepare_async_collect(None).expect("into group 1");
    assert_eq!(request.buffer_start, G, "group 1 alone");
    assert!(request.buffer_end <= 2 * G);
}

#[test]
fn a_nested_column_takes_its_width_from_the_footer() {
    let schema = Schema::from_iter([Field::new(
        "l".into(),
        DataType::List(Box::new(DataType::Float64)),
    )]);
    let columns = vec!["l".to_string()];
    assert_eq!(estimate_bytes_per_row(&schema, &columns, &[]), 64);
    assert_eq!(
        estimate_bytes_per_row(&schema, &columns, &[("l".to_string(), 800)]),
        800
    );
}

#[test]
fn binary_columns_are_stubbed_in_display_buffer() {
    // Binary columns must not be materialized into the display buffer (their blobs can be huge
    // and reading them is what stalls jump-to-end); the buffer holds a stub instead.
    let a = Series::new("a".into(), &[1i32, 2, 3]);
    let blob = Series::new("blob".into(), &["aaaa", "bbbb", "cccc"])
        .cast(&DataType::Binary)
        .unwrap();
    let lf = DataFrame::new_infer_height(vec![a.into(), blob.into()])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 10;
    state.collect();

    let df = state.view.df.as_ref().expect("display df present");
    let col = df.column("blob").expect("blob column present in buffer");
    assert_eq!(
        col.dtype(),
        &DataType::String,
        "binary column should be stubbed (not read as binary)"
    );
    assert_eq!(col.str().unwrap().get(0).unwrap(), binary_stub());
    // A non-binary column is untouched.
    assert_eq!(df.column("a").unwrap().dtype(), &DataType::Int32);
}

#[test]
fn analysis_describe_stubs_binary_columns_without_reading_blobs() {
    // Describe (and the other analysis tools) route the frame through `binary_stub_exprs`
    // so binary blobs are never materialized — reading multi-GB blobs across partitions is
    // what froze the process. The binary column still appears, as a stub.
    let a = Series::new("a".into(), &[1i32, 2, 3]);
    let blob = Series::new("blob".into(), &["aaaa", "bbbb", "cccc"])
        .cast(&DataType::Binary)
        .unwrap();
    let lf = DataFrame::new_infer_height(vec![a.into(), blob.into()])
        .unwrap()
        .lazy();
    let state = DataTableState::new(lf, None, None, None, None, true).unwrap();

    let analysis_lf = state.view.lf.clone().select(state.binary_stub_exprs());
    let results = crate::statistics::compute_describe_from_lazy(
        &analysis_lf,
        Some(3),
        &crate::sampling::Sample {
            method: crate::sampling::SampleMethod::EveryRow,
            ..crate::sampling::Sample::default()
        },
        false,
    )
    .expect("describe should not fail on binary columns");

    let blob_stat = results
        .column_statistics
        .iter()
        .find(|c| c.name == "blob")
        .expect("binary column present in describe");
    // Stubbed to a constant string, so describe treats it as categorical and its min/max is
    // the stub — never the raw bytes.
    let cat = blob_stat
        .categorical_stats
        .as_ref()
        .expect("stubbed binary column has categorical stats");
    assert_eq!(cat.min.as_deref(), Some(binary_stub()));
    assert_eq!(cat.max.as_deref(), Some(binary_stub()));
    // The numeric column is still described normally.
    let a_stat = results
        .column_statistics
        .iter()
        .find(|c| c.name == "a")
        .expect("numeric column present in describe");
    assert!(a_stat.numeric_stats.is_some());
}

/// A state over `id` (frozen), `name`, `city`, `amount`, thirty rows, drawn once at
/// 80×24 so the room and widths are known.
fn cursor_fixture() -> (DataTableState, Rect) {
    let n = 30;
    let lf = df!(
        "id" => (0..n).collect::<Vec<i64>>(),
        "name" => (0..n).map(|i| format!("name {i}")).collect::<Vec<_>>(),
        "city" => (0..n).map(|i| format!("city {i}")).collect::<Vec<_>>(),
        "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 22;
    state.set_locked_columns(1);
    state.table_state.select(Some(0));
    let area = Rect::new(0, 0, 80, 24);
    DataTable::default().render(area, &mut Buffer::empty(area), &mut state);
    (state, area)
}

/// The x range of the column headed `name` on the header row.
fn column_span(buf: &Buffer, area: Rect, name: &str) -> std::ops::Range<u16> {
    let header = row_string(buf, area, 0);
    let at = header.find(name).expect("the column is drawn") as u16;
    at..at + name.len() as u16
}

fn list_state() -> DataTableState {
    let many: Vec<String> = (0..12).map(|i| format!("t{i}")).collect();
    let tags = Series::new(
        "tags".into(),
        &[
            Series::new("".into(), &["a", "b"]),
            Series::new("".into(), many),
        ],
    );
    let id = Series::new("id".into(), &[1i64, 2]);
    let more = Series::new(
        "more".into(),
        &[
            Series::new("".into(), &["x"]),
            Series::new("".into(), &["y"]),
        ],
    );
    let lf = DataFrame::new_infer_height(vec![id.into(), tags.into(), more.into()])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 2;
    state.collect();
    state
}

/// The display frames keep lists as lists, frozen or scrolling, through a
/// sideways scroll: a step re-cuts the buffer and formats nothing; only the
/// cells drawn are formatted.
#[test]
fn a_sideways_scroll_keeps_lists_in_the_display_frames() {
    let mut state = list_state();
    state.set_locked_columns(2);
    state.scroll_right();
    let is_list =
        |df: &DataFrame, name: &str| matches!(df.column(name).unwrap().dtype(), DataType::List(_));
    assert!(is_list(state.view.locked_df.as_ref().unwrap(), "tags"));
    assert!(is_list(state.view.df.as_ref().unwrap(), "more"));
}

/// A state over thirty text columns of mixed widths, drawn once at 50 wide.
fn paging_state() -> (DataTableState, impl Fn(&mut DataTableState)) {
    let columns: Vec<Column> = (0..30)
        .map(|i| {
            let value = "x".repeat(3 + (i * 7) % 11);
            Series::new(format!("col{i:02}").into(), vec![value; 3]).into()
        })
        .collect();
    let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.collect();
    let draw = |state: &mut DataTableState| {
        let area = Rect::new(0, 0, 50, 4);
        DataTable::default().render(area, &mut Buffer::empty(area), state);
    };
    draw(&mut state);
    (state, draw)
}

/// Keys typed faster than frames, over columns not drawn yet, land at the draw in
/// the order typed, exactly where they land with a frame after each, and keep
/// waiting while no rows are on hand to measure with.
#[test]
fn moves_typed_before_a_draw_land_in_order() {
    use ColumnMove::*;
    let keys = [PageRight, PageRight, StepRight, PageLeft, PageRight];
    let (mut paced, draw) = paging_state();
    for mv in keys {
        paced.scroll_columns(mv);
        draw(&mut paced);
    }
    assert!(paced.termcol_index > 0);

    let (mut typed, draw) = paging_state();
    typed.defer_collect = true;
    for mv in keys {
        typed.scroll_columns(mv);
    }
    draw(&mut typed);
    assert_eq!(typed.termcol_index, 0, "no rows to measure with: they wait");
    typed.defer_collect = false;
    draw(&mut typed);
    assert_eq!(typed.termcol_index, paced.termcol_index);
    assert!(typed.column_moves.is_empty());

    // First drops what waits; nothing is left to land later.
    let (mut first, draw) = paging_state();
    first.scroll_columns(Last);
    first.scroll_columns(StepRight);
    first.scroll_columns(ColumnMove::First);
    draw(&mut first);
    assert_eq!(first.termcol_index, 0);
}

/// `[` straight after `]` goes back to the page `]` left, even where the page
/// before, packed from the right, would start further left.
#[test]
fn page_left_after_page_right_retraces() {
    let (mut state, draw) = paging_state();
    let room = state.scroll_room.unwrap();
    let p = room.padding;
    let usable = room.width - room.lead;
    // Three of `a` fill the first page, two of `b` the second with a column too
    // wide for the room after them; packed from the right, the page ending before
    // that wide column would take in two of the `a` columns as well.
    let a = (usable - 2 * p) / 3;
    let b =
        (usable.saturating_sub(2 * a + 3 * p) / 2).max(crate::widgets::column_widths::MIN_WIDTH);
    let widths = [a, a, a, b, b, usable + 10];
    state.set_width_choices(
        widths
            .iter()
            .enumerate()
            .map(|(i, &w)| (format!("col{i:02}"), WidthChoice::Manual(w))),
    );
    draw(&mut state);
    state.scroll_columns(ColumnMove::PageRight);
    draw(&mut state);
    assert_eq!(state.termcol_index, 3);
    state.scroll_columns(ColumnMove::PageRight);
    draw(&mut state);
    assert_eq!(state.termcol_index, 5);
    let names = state.scrolling_names().to_vec();
    let packed =
        crate::widgets::column_paging::plan(ColumnMove::PageLeft, 5, names.len(), room, |i| {
            state.drawn_width(&names[i])
        });
    assert!(packed < Some(3), "the packed page differs: {packed:?}");
    state.scroll_columns(ColumnMove::PageLeft);
    assert_eq!(state.termcol_index, 3, "back to the page left");
    state.scroll_columns(ColumnMove::PageLeft);
    assert_eq!(state.termcol_index, 0);
    // Not straight after `]`: the page before, packed from the right.
    state.scroll_columns(ColumnMove::PageRight);
    state.scroll_columns(ColumnMove::PageRight);
    state.scroll_columns(ColumnMove::StepRight);
    state.scroll_columns(ColumnMove::StepLeft);
    state.scroll_columns(ColumnMove::PageLeft);
    assert_eq!(Some(state.termcol_index), packed);
}

fn glyph_sets() -> [&'static crate::glyphs::Glyphs; 2] {
    [crate::glyphs::unicode(), crate::glyphs::ascii()]
}

fn set_name(g: &crate::glyphs::Glyphs) -> &'static str {
    if g.unicode { "unicode" } else { "ascii" }
}

/// The cells of row `y` from `x0` on, as the terminal shows them: a wide
/// character once, without the blank its second cell holds.
fn drawn_from(buf: &Buffer, y: u16, x0: u16) -> String {
    use ratatui::buffer::CellWidth;
    let mut row = String::new();
    let mut x = x0;
    while x < buf.area.right() {
        let symbol = buf[(x, y)].symbol();
        row.push_str(symbol);
        x += symbol.cell_width().max(1);
    }
    row
}

/// Draw `state` at `width` × `height` with `table`, row by row as shown.
fn draw(table: DataTable, state: &mut DataTableState, width: u16, height: u16) -> Vec<String> {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    table.render(area, &mut buf, state);
    (0..height).map(|y| drawn_from(&buf, y, 0)).collect()
}

/// A state over `df` with its first page buffered for `visible_rows` rows.
fn state_of(df: &DataFrame, visible_rows: usize) -> DataTableState {
    let mut state = DataTableState::new(df.clone().lazy(), None, None, None, None, true).unwrap();
    state.visible_rows = visible_rows;
    state.collect();
    state
}

/// The column names, in order, for the frozen-prefix tests.
const PHONETIC: [&str; 6] = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];

fn phonetic_frame() -> DataFrame {
    let columns: Vec<Column> = PHONETIC
        .iter()
        .map(|n| {
            Series::new(
                (*n).into(),
                &[format!("{n}-value-one"), format!("{n}-value-two")],
            )
            .into()
        })
        .collect();
    DataFrame::new_infer_height(columns).unwrap()
}

/// Every column name some frame shows while scrolling right from the start.
fn names_reached(
    state: &mut DataTableState,
    g: &'static crate::glyphs::Glyphs,
    width: u16,
) -> Vec<&'static str> {
    let mut seen = Vec::new();
    for _ in 0..PHONETIC.len() + 2 {
        let table = DataTable {
            glyphs: g,
            ..DataTable::default()
        };
        let rows = draw(table, state, width, 3);
        for name in PHONETIC {
            if rows[0].contains(name) && !seen.contains(&name) {
                seen.push(name);
            }
        }
        state.scroll_right();
    }
    seen
}

/// The issue's long-URL frame: six columns over 80 rows, every description short
/// but row 24's.
fn long_url_frame() -> DataFrame {
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let n = 80usize;
    let start = chrono::NaiveDate::from_ymd_opt(2024, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    df!(
        "id" => (0..n as i64).collect::<Vec<_>>(),
        "description" => (0..n)
            .map(|i| if i == 24 { url.clone() } else { format!("item {i}") })
            .collect::<Vec<_>>(),
        "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
        "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
        "timestamp" => (0..n)
            .map(|i| start + chrono::Duration::hours(i as i64))
            .collect::<Vec<_>>(),
        "uuid" => (0..n)
            .map(|i| format!("00000000-0000-0000-0000-{:012x}", i * 7919 + 1))
            .collect::<Vec<_>>(),
    )
    .unwrap()
}

mod query;
mod render;
