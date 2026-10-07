use super::*;

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

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|name| name.to_string())
        .collect()
}

fn mixed_strings() -> LazyFrame {
    df!(
        "id" => &[1i64, 2, 3, 4],
        "amount" => &[" 10 ", "20", "", " 40"],
        "day" => &["2024-01-01", "2024-01-02", " ", "2024-01-04"],
        "at" => &["2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z", "", "2024-01-01T12:00:00Z"],
        "score" => &[0.5f64, 1.5, 2.5, 3.5],
        "word" => &["a", " b", "c ", ""],
        "empty" => &[None::<&str>, None, None, None],
    )
    .unwrap()
    .lazy()
}

/// String inference reads only the columns it types, and reads them exactly as
/// the whole-frame sample it replaced did: trimmed, blanks null, same rows.
#[test]
fn the_inference_sample_holds_only_its_targets() {
    let targets: Vec<String> = ["amount", "day", "at", "word", "empty"]
        .map(String::from)
        .to_vec();
    let sample = DataTableState::string_inference_sample(mixed_strings(), &targets, 3).unwrap();
    assert_eq!(names(&sample), ["amount", "day", "at", "word", "empty"]);
    assert_eq!(sample.height(), 3);

    // The sample as it was taken before: every column, the targets normalized.
    let blank = lit(PlSmallStr::from_static(""));
    let wide = mixed_strings()
        .limit(3)
        .with_columns(
            targets
                .iter()
                .map(|c| {
                    col(c.as_str())
                        .str()
                        .strip_chars(lit(PlSmallStr::from_static(" \t\n\r")))
                })
                .collect::<Vec<_>>(),
        )
        .with_columns(
            targets
                .iter()
                .map(|c| {
                    when(col(c.as_str()).eq(blank.clone()))
                        .then(Null {}.lit())
                        .otherwise(col(c.as_str()))
                        .alias(c.as_str())
                })
                .collect::<Vec<_>>(),
        )
        .collect()
        .unwrap();
    assert_eq!(wide.width(), 7);
    assert!(sample.equals_missing(&wide.select(targets.iter().map(String::as_str)).unwrap()));

    let one = DataTableState::string_inference_sample(mixed_strings(), &["day".to_string()], 1_000)
        .unwrap();
    assert_eq!(names(&one), ["day"]);
    assert_eq!(one.height(), 4);
}

/// The frame string inference returns keeps every column in its place, and types
/// the targets as before: numbers, dates, timestamps, text left as text, an
/// all-null column left alone.
#[test]
fn string_inference_keeps_the_whole_frame() {
    let utc = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
    let typed = |target: &ParseStringsTarget, types: StringTypes| {
        DataTableState::type_string_columns(
            mixed_strings(),
            target,
            1_000,
            types,
            &mut Vec::new(),
            &[],
            &mut Vec::new(),
        )
        .unwrap()
        .collect()
        .unwrap()
    };
    let all = StringTypes {
        dates: true,
        numbers: true,
    };

    let df = typed(&ParseStringsTarget::All, all);
    let schema = df.schema();
    let order = names(&df);
    assert_eq!(
        order,
        ["id", "amount", "day", "at", "score", "word", "empty"]
    );
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
    assert_eq!(schema.get("amount"), Some(&DataType::Int64));
    assert_eq!(schema.get("day"), Some(&DataType::Date));
    assert_eq!(schema.get("at"), Some(&utc));
    assert_eq!(schema.get("score"), Some(&DataType::Float64));
    assert_eq!(schema.get("word"), Some(&DataType::String));
    assert_eq!(schema.get("empty"), Some(&DataType::String));
    let amount: Vec<Option<i64>> = df.column("amount").unwrap().i64().unwrap().iter().collect();
    assert_eq!(amount, [Some(10), Some(20), None, Some(40)]);
    assert_eq!(df.column("day").unwrap().null_count(), 1);
    assert_eq!(df.column("at").unwrap().null_count(), 1);
    let word: Vec<Option<&str>> = df.column("word").unwrap().str().unwrap().iter().collect();
    assert_eq!(word, [Some("a"), Some("b"), Some("c"), Some("")]);
    let score: Vec<Option<f64>> = df.column("score").unwrap().f64().unwrap().iter().collect();
    assert_eq!(score, [Some(0.5), Some(1.5), Some(2.5), Some(3.5)]);

    // Named columns: only those that are text are typed; the rest are as read.
    let some = typed(
        &ParseStringsTarget::Columns(vec!["day".into(), "id".into(), "missing".into()]),
        all,
    );
    assert_eq!(names(&some), order);
    assert_eq!(some.schema().get("day"), Some(&DataType::Date));
    assert_eq!(some.schema().get("amount"), Some(&DataType::String));
    assert_eq!(
        some.column("amount").unwrap().str().unwrap().get(0),
        Some(" 10 ")
    );

    // Nothing to type: the frame comes back as it was.
    let none = typed(&ParseStringsTarget::Columns(vec!["id".into()]), all);
    assert!(none.equals_missing(&mixed_strings().collect().unwrap()));

    // JSON: dates only. Numbers in strings stay text, untrimmed.
    let json = DataTableState::apply_parse_dates_to_json_lazyframe(
        mixed_strings(),
        &crate::OpenOptions::default(),
        &mut Vec::new(),
    )
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(names(&json), order);
    assert_eq!(json.schema().get("day"), Some(&DataType::Date));
    assert_eq!(json.schema().get("at"), Some(&utc));
    assert_eq!(json.schema().get("amount"), Some(&DataType::String));
    assert_eq!(
        json.column("word").unwrap().str().unwrap().get(1),
        Some(" b")
    );
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
    assert_eq!(collect_lazy(view.lf.clone(), false).unwrap().height(), 2);
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
    assert_eq!(collect_lazy(view.lf.clone(), false).unwrap().height(), 2);
    drop(view);
    assert!(!path.exists());
}

/// Scrolling sideways must not count the rows.
///
/// A staged open shows a screen before the count is known, and the count on a
/// cloud hive is a metadata read per object. `scroll_right` runs on the thread
/// that draws and reads keys — `App::handle`'s `RIGHT_KEYS` arm calls it
/// directly — so a count taken there is a freeze no keystroke can interrupt.
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
        state.buffered_df.is_some(),
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
        state.buffered_df.is_none(),
        "and they are let go, rather than drawn under the columns that replaced them"
    );
    assert_eq!(
        (state.buffered_start_row, state.buffered_end_row),
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
    state.observed_bytes_per_row = Some(8);
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
/// that pass is bringing the count — and the control bar shows a spinner in place of
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
    state.active_query = "select doubled: id * 2".to_string();
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
    state.active_query.clear();
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

/// Every way of building on the scan holds the arriving columns off, not just one.
///
/// Each of these replaces the frame's root with a result of its own, whose columns
/// are not the dataset's. The doc on `scan_is_the_root` says so of all of them; the
/// query is the one the app-level test exercises, so this is the rest.
#[test]
fn anything_built_on_the_scan_holds_the_arriving_columns_off() {
    // A string column because a fuzzy search needs one to search.
    let narrow = || {
        df!("id" => &[1i64, 2], "v" => &[10i64, 20], "name" => &["one", "two"])
            .unwrap()
            .lazy()
    };
    let wider = || {
        df!(
            "id" => &[1i64, 2],
            "v" => &[10i64, 20],
            "name" => &["one", "two"],
            "oops" => &["a", "b"],
        )
        .unwrap()
        .lazy()
    };
    let found = || {
        let mut lf = wider();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        let footer = crate::schema_union::FileFooter {
            schema,
            row_group_rows: vec![2],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        };
        FootersFound {
            estimate: None,
            dataset: crate::schema_union::union_sampled(1, &[0], &[Some(footer)]),
            lf: wider(),
            file_rows: Vec::new(),
            files: Vec::new(),
            row_groups: Vec::new(),
            remote: None,
        }
    };
    let fresh = || {
        let mut lf = narrow();
        let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
        DataTableState::from_schema_and_lazyframe(
            schema,
            narrow(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
    };

    // A filter and a sort are not built on the scan, they are the scan with
    // something done to it, and `apply_transformations` puts them back over
    // whatever the root becomes. Those the columns may join under.
    let mut sorted = fresh();
    sorted.sort(vec!["id".to_string()], true);
    assert!(
        sorted.join_dataset_schema(found()).is_ok(),
        "a sort is rebuilt over the wider scan, so the columns go in under it"
    );

    let mut queried = fresh();
    queried.query("select doubled: v * 2".to_string());
    assert!(
        queried.join_dataset_schema(found()).is_err(),
        "a query's columns are its own"
    );

    #[cfg(feature = "sql")]
    {
        let mut sql = fresh();
        sql.sql_query("SELECT id FROM df".to_string());
        assert!(sql.error.is_none(), "the statement runs: {:?}", sql.error);
        assert!(
            sql.join_dataset_schema(found()).is_err(),
            "and a SQL statement's are too"
        );
    }

    let mut fuzzy = fresh();
    fuzzy.fuzzy_search("10".to_string());
    assert!(
        fuzzy.join_dataset_schema(found()).is_err(),
        "and what a fuzzy search matched is a result, not the dataset"
    );

    let mut melted = fresh();
    melted
        .melt(&MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["v".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        })
        .expect("the melt runs");
    assert!(
        melted.join_dataset_schema(found()).is_err(),
        "a melt's rows are not the dataset's rows"
    );

    let mut pivoted = fresh();
    pivoted
        .pivot(&PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "name".to_string(),
            value_column: "v".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: None,
        })
        .expect("the pivot runs");
    assert!(
        pivoted.join_dataset_schema(found()).is_err(),
        "and a pivot's columns are made from the data, not read from it"
    );

    let mut drilled = fresh();
    // The field rather than the drill itself, which needs a grouped frame to drill
    // into: what is being asked here is whether the clause is consulted.
    drilled.drilled_down_group_index = Some(0);
    assert!(
        drilled.join_dataset_schema(found()).is_err(),
        "and a drill-down is showing one group of it, not it"
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
    assert_eq!(state.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
}

#[test]
fn test_from_csv_gzipped() {
    // Ensure sample data is generated before running test
    // Test gzipped CSV loading
    let path = crate::tests::sample_data_dir().join("mixed_types.csv.gz");
    let state = DataTableState::from_csv(&path, &Default::default()).unwrap(); // Uses default buffer params from options
    assert_eq!(state.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
}

#[test]
fn test_from_parquet() {
    // Ensure sample data is generated before running test
    let path = crate::tests::sample_data_dir().join("people.parquet");
    let state = DataTableState::from_parquet(&path, &OpenOptions::default()).unwrap();
    assert!(!state.schema.is_empty());
}

#[test]
fn test_from_ipc() {
    use polars::prelude::IpcWriter;
    use std::io::BufWriter;
    let mut df = df!(
        "x" => &[1_i32, 2, 3],
        "y" => &["a", "b", "c"]
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_ipc.arrow");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = BufWriter::new(file);
    IpcWriter::new(&mut writer).finish(&mut df).unwrap();
    drop(writer);
    let state = DataTableState::from_ipc(&path, &OpenOptions::default()).unwrap();
    assert_eq!(state.schema.len(), 2);
    assert!(state.schema.contains("x"));
    assert!(state.schema.contains("y"));
}

#[test]
fn test_from_avro() {
    use polars::io::avro::AvroWriter;
    use std::io::BufWriter;
    let mut df = df!(
        "id" => &[1_i32, 2, 3],
        "name" => &["alice", "bob", "carol"]
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_avro.avro");
    let file = std::fs::File::create(&path).unwrap();
    let mut writer = BufWriter::new(file);
    AvroWriter::new(&mut writer).finish(&mut df).unwrap();
    drop(writer);
    let state = DataTableState::from_avro(&path, &OpenOptions::default()).unwrap();
    assert_eq!(state.schema.len(), 2);
    assert!(state.schema.contains("id"));
    assert!(state.schema.contains("name"));
}

#[test]
fn test_from_orc() {
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use orc_rust::ArrowWriterBuilder;
    use std::io::BufWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));
    let id_array = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec!["a", "b", "c"]));
    let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array]).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("datui_test_orc.orc");
    let file = std::fs::File::create(&path).unwrap();
    let writer = BufWriter::new(file);
    let mut orc_writer = ArrowWriterBuilder::new(writer, schema).try_build().unwrap();
    orc_writer.write(&batch).unwrap();
    orc_writer.close().unwrap();

    let state = DataTableState::from_orc(&path, &OpenOptions::default()).unwrap();
    assert_eq!(state.schema.len(), 2);
    assert!(state.schema.contains("id"));
    assert!(state.schema.contains("name"));
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
    let mut enc =
        bzip2::write::BzEncoder::new(File::create(&bz).unwrap(), bzip2::Compression::best());
    enc.write_all(body).unwrap();
    enc.finish().unwrap();
    let xz = dir.path().join("t.csv.xz");
    let mut enc = xz2::write::XzEncoder::new(File::create(&xz).unwrap(), 6);
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
        let df = state.lf.clone().collect().unwrap();
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
    assert_eq!(state.schema.len(), 4);
    assert!(state.schema.contains("a"));
    assert!(state.schema.contains("b"));
    assert!(state.schema.contains("c"));
    assert!(state.schema.contains("d"));
    assert_eq!(state.num_rows, 2);
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
    assert_eq!(state.schema.len(), 4);
    assert!(state.schema.contains("column_1"));
    assert!(state.schema.contains("column_2"));
    assert!(state.schema.contains("column_3"));
    assert!(state.schema.contains("column_4"));
    assert_eq!(state.num_rows, 3);
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
    assert_eq!(state.schema.len(), 3);
    assert!(state.schema.contains("column_1"));
    assert!(state.schema.contains("column_2"));
    assert!(state.schema.contains("column_3"));
    assert_eq!(state.num_rows, 3);
}

#[test]
fn test_filter() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let filters = vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Gt,
        value: "2".to_string(),
        logical_op: LogicalOperator::And,
    }];
    state.filter(filters);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape().0, 1);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
}

#[test]
fn test_sort() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["a".to_string()], false);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
}

#[test]
fn test_query() {
    let lf = create_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.query("select b where a = 2".to_string());
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape(), (1, 1));
    assert_eq!(
        df.column("b").unwrap().get(0).unwrap(),
        AnyValue::String("y")
    );
}

#[test]
fn test_query_date_accessors() {
    use chrono::NaiveDate;
    let df = df!(
        "event_date" => [
            NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
            NaiveDate::from_ymd_opt(2024, 6, 20).unwrap(),
            NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
        ],
        "name" => &["a", "b", "c"],
    )
    .unwrap();
    let lf = df.lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

    // Select with date accessors
    state.query("select name, year: event_date.year, month: event_date.month".to_string());
    assert!(
        state.error.is_none(),
        "query should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape(), (3, 3));
    assert_eq!(
        df.column("year").unwrap().get(0).unwrap(),
        AnyValue::Int32(2024)
    );
    assert_eq!(
        df.column("month").unwrap().get(0).unwrap(),
        AnyValue::Int8(1)
    );
    assert_eq!(
        df.column("month").unwrap().get(1).unwrap(),
        AnyValue::Int8(6)
    );

    // Filter with date accessor
    state.query("select name, event_date where event_date.month = 12".to_string());
    assert!(
        state.error.is_none(),
        "filter should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 1);
    assert_eq!(
        df.column("name").unwrap().get(0).unwrap(),
        AnyValue::String("c")
    );

    // Filter with YYYY.MM.DD date literal
    state.query("select name, event_date where event_date.date > 2024.06.15".to_string());
    assert!(
        state.error.is_none(),
        "date literal filter should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(
        df.height(),
        2,
        "2024-06-20 and 2024-12-31 are after 2024-06-15"
    );

    // String accessors: upper, lower, len, ends_with
    state.query(
        "select name, upper_name: name.upper, name_len: name.len where name.ends_with[\"c\"]"
            .to_string(),
    );
    assert!(
        state.error.is_none(),
        "string accessors should succeed: {:?}",
        state.error
    );
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 1, "only 'c' ends with 'c'");
    assert_eq!(
        df.column("upper_name").unwrap().get(0).unwrap(),
        AnyValue::String("C")
    );

    // Query that returns 0 rows: df and locked_df must be cleared for correct empty-table render
    state.query("select where event_date.date = 2020.01.01".to_string());
    assert!(state.error.is_none());
    assert_eq!(state.num_rows, 0);
    state.visible_rows = 10;
    state.collect();
    assert!(state.df.is_none(), "df must be cleared when num_rows is 0");
    assert!(
        state.locked_df.is_none(),
        "locked_df must be cleared when num_rows is 0"
    );
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

    assert_eq!(state.start_row, 0);
    state.page_down();
    assert_eq!(state.start_row, 20);
    state.page_down();
    assert_eq!(state.start_row, 40);
    state.page_up();
    assert_eq!(state.start_row, 20);
    state.page_up();
    assert_eq!(state.start_row, 0);
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
    let df = state.lf.clone().collect().unwrap();
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

/// Ties keep their order, so the page read at the top (a top-k to Polars) and
/// the page read below it agree on the rows they share.
#[test]
fn a_sort_with_ties_reads_the_same_rows_page_by_page() {
    let df = df!(
        "k" => (0..3000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..3000i64).collect::<Vec<_>>(),
    )
    .unwrap();
    let sorted = df
        .lazy()
        .sort_by_exprs([col("k")], sort_options(vec![false]));
    let top = sorted.clone().slice(0, 200).collect().unwrap();
    let below = sorted.clone().slice(100, 100).collect().unwrap();
    assert!(top.slice(100, 100).equals(&below));
    let one = sorted.slice(5, 1).collect().unwrap();
    assert!(top.slice(5, 1).equals(&one));
}

/// `IN (SELECT …)` reads the subquery's values once rather than once per row,
/// which on the streaming engine ran out of memory for a page of 100k rows
/// (#509), and still returns what polars-sql's own plan does, NULLs included.
/// The data is small, so that without the fix the test fails on the plan rather
/// than on memory. A user's own list question of the same shape is left alone.
#[cfg(feature = "sql")]
#[test]
fn a_sql_in_subquery_reads_its_values_once() {
    let mut df = df!(
        "k" => (0..3000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..3000i64).map(|i| (i % 7 != 0).then_some(i)).collect::<Vec<_>>(),
    )
    .unwrap();
    // A first value that is a NULL list: its length is NULL, not 1 as exploded.
    let lists: ListChunked = (0..3000i64)
        .map(|i| (i != 0).then(|| Series::new(PlSmallStr::EMPTY, [i])))
        .collect();
    df.with_column(lists.into_column().with_name("l".into()))
        .unwrap();
    let n = df.height();
    for (sql, rows) in [
        (
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 1)",
            None,
        ),
        // The values hold a NULL, so no row is NOT IN them.
        (
            "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 1)",
            Some(0),
        ),
        (
            "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 1 AND v IS NOT NULL)",
            None,
        ),
        // No values: every row, a NULL `v` too.
        (
            "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 5)",
            Some(n),
        ),
        (
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 5)",
            Some(0),
        ),
        (
            "SELECT * FROM df WHERE k = 2 OR v IN (SELECT v FROM df WHERE k = 1 LIMIT 100)",
            None,
        ),
        (
            "SELECT * FROM df WHERE v IN (SELECT MIN(v) FROM df GROUP BY v % 100)",
            None,
        ),
        (
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 0 AND v IS NOT NULL))",
            None,
        ),
        // Unknown, not false, for a value outside a set holding a NULL, and for a
        // NULL value.
        (
            "SELECT * FROM df WHERE (v NOT IN (SELECT v FROM df WHERE k = 1)) IS NULL",
            None,
        ),
        // Only NULLs: every row unknown.
        (
            "SELECT * FROM df WHERE (v IN (SELECT v FROM df WHERE v IS NULL)) IS NULL",
            Some(n),
        ),
        // No values: false, a NULL `v` too.
        (
            "SELECT * FROM df WHERE (v IN (SELECT v FROM df WHERE k = 5)) IS NULL",
            Some(0),
        ),
        (
            "SELECT * FROM df WHERE ARRAY_LENGTH(FIRST(l)) IS NULL AND v NOT IN (SELECT v FROM df WHERE k = 5)",
            Some(n),
        ),
    ] {
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());
        let raw = ctx.execute(sql).unwrap();
        // Else the check on the view's plan below would pass for nothing.
        assert!(
            (&raw.logical_plan).into_iter().any(asks_per_row),
            "{sql}: polars-sql's plan"
        );
        let expected = raw.collect().unwrap();
        if let Some(rows) = rows {
            assert_eq!(expected.height(), rows, "{sql}");
        }
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        assert!(
            !(&state.lf.logical_plan).into_iter().any(asks_per_row),
            "{sql}"
        );
        for streaming in [false, cfg!(feature = "streaming")] {
            let got = collect_lazy(state.lf.clone(), streaming).unwrap();
            assert!(
                got.equals_missing(&expected),
                "{sql}, streaming {streaming}"
            );
        }
    }
}

/// An `IN (SELECT …)` subquery returns exactly the statement's columns, after a
/// QUALIFY as after a WHERE: polars-sql leaves the column holding the values in a
/// QUALIFY's result (#519). The rows are polars-sql's own, and a user's column
/// named like polars-sql's is kept.
#[cfg(feature = "sql")]
#[test]
fn a_sql_in_subquery_returns_only_the_statements_columns() {
    const LOOKALIKE: &str = "_POLARS_TMP_999999999";
    let df = df!(
        "k" => (0..300i64).map(|i| i % 3).collect::<Vec<_>>(),
        "i" => (0..300i64).collect::<Vec<_>>(),
        "w" => (0..300i64).map(|i| (i % 7 != 0).then_some(i % 11)).collect::<Vec<_>>(),
        LOOKALIKE => (0..300i64).collect::<Vec<_>>(),
    )
    .unwrap();
    let all = ["k", "i", "w", LOOKALIKE];
    for (sql, columns) in [
        (
            "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)",
            &["k", "i", "r"][..],
        ),
        (
            "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r NOT IN (SELECT w FROM df WHERE w > 3 AND w IS NOT NULL) \
             ORDER BY i DESC LIMIT 50",
            &["k", "i", "r"][..],
        ),
        (
            "SELECT * FROM df \
             QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) IN (SELECT w FROM df)",
            &all[..],
        ),
        (
            "SELECT DISTINCT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r, _POLARS_TMP_999999999 FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)",
            &["k", "r", LOOKALIKE][..],
        ),
        (
            "SELECT * FROM (SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)) WHERE i > 1",
            &["k", "i", "r"][..],
        ),
        (
            "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3) AND i IN (SELECT w FROM df)",
            &["k", "i", "r"][..],
        ),
        (
            "WITH q AS (SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)) \
             SELECT * FROM q UNION ALL SELECT * FROM q",
            &["k", "i", "r"][..],
        ),
        // A join suffixes the right side's copy of both polars-sql's column and
        // the user's.
        (
            "WITH q AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
             QUALIFY r IN (SELECT w FROM df WHERE w < 3)) \
             SELECT * FROM q a JOIN q b ON a.i = b.i JOIN q c ON a.i = c.i ORDER BY a.i",
            &[
                "k",
                "i",
                "w",
                LOOKALIKE,
                "r",
                "k:b",
                "i:b",
                "w:b",
                "_POLARS_TMP_999999999:b",
                "r:b",
                "k:c",
                "i:c",
                "w:c",
                "_POLARS_TMP_999999999:c",
                "r:c",
            ][..],
        ),
        ("SELECT * FROM df WHERE i IN (SELECT w FROM df)", &all[..]),
        (
            "SELECT k, i FROM df WHERE i NOT IN (SELECT w FROM df WHERE w IS NOT NULL)",
            &["k", "i"][..],
        ),
    ] {
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());
        let raw = ctx.execute(sql).unwrap().collect().unwrap();
        assert!(raw.height() > 0, "{sql}");
        let expected = raw.select(columns.iter().copied()).unwrap();
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, columns, "{sql}");
        for streaming in [false, cfg!(feature = "streaming")] {
            let got = collect_lazy(state.lf.clone(), streaming).unwrap();
            assert!(
                got.equals_missing(&expected),
                "{sql}, streaming {streaming}: {got:?}"
            );
        }
    }
}

/// The ORDER BY of the SQL in effect is state, so it leaves its mark: the sort
/// mark on each column it orders by, as named in the result, until the sidebar
/// sorts or another query runs (#688, item 13).
#[cfg(feature = "sql")]
#[test]
fn a_sql_order_by_marks_the_header_until_the_sidebar_sorts() {
    let df = df!("k" => [1i64, 2, 3], "v" => [3i64, 2, 1]).unwrap();
    let marks = |sql: &str| {
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        state.header_sort()
    };
    let owned = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    assert_eq!(
        marks("SELECT v, k FROM df ORDER BY k DESC"),
        (owned(&["k"]), vec![true])
    );
    assert_eq!(
        marks("SELECT * FROM df ORDER BY k DESC, v LIMIT 2"),
        (owned(&["k", "v"]), vec![true, false])
    );
    assert_eq!(
        marks("SELECT v AS w, k FROM df ORDER BY w"),
        (owned(&["w"]), vec![false]),
        "named as in the result"
    );
    assert_eq!(
        marks("SELECT k, SUM(v) AS s FROM df GROUP BY k ORDER BY s DESC"),
        (owned(&["s"]), vec![true])
    );
    // An expression, or a column the result leaves out, leaves no mark.
    assert_eq!(marks("SELECT * FROM df ORDER BY k + 1"), (vec![], vec![]));
    assert_eq!(marks("SELECT v FROM df ORDER BY k"), (vec![], vec![]));
    assert_eq!(marks("SELECT * FROM df"), (vec![], vec![]));

    // On the header, and the sidebar's sort replaces it.
    let mut state =
        DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
    state.sql_query("SELECT * FROM df ORDER BY k DESC".to_string());
    state.collect();
    let area = Rect::new(0, 0, 30, 6);
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let header = row_string(&buf, area, 0);
    let g = crate::glyphs::get();
    assert!(header.contains(&format!("k{}", g.sort_desc)), "{header:?}");
    state.sort_by(vec!["v".to_string()], vec![false]);
    assert_eq!(state.header_sort(), (owned(&["v"]), vec![false]));
    // A new query names its own order, or none.
    state.sql_query("SELECT * FROM df".to_string());
    assert_eq!(state.header_sort(), (vec![], vec![]));
}

/// A SQL ORDER BY keeps tied rows in order, as the sidebar's sort does: the page
/// read at the top (a top-k to Polars) and the next page agree on the rows they
/// share, through a LIMIT and a subquery, and over a grouping, a join, a union or
/// a DISTINCT, whose rows would otherwise come in any order (#495). With either
/// engine.
#[cfg(feature = "sql")]
#[test]
fn a_sql_order_by_with_ties_reads_the_same_rows_page_by_page() {
    let df = df!(
        "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..5000i64).collect::<Vec<_>>(),
    )
    .unwrap();
    for sql in [
        "SELECT * FROM df ORDER BY k",
        "SELECT v, k FROM df ORDER BY k DESC",
        "SELECT * FROM df ORDER BY k LIMIT 4000",
        "SELECT * FROM (SELECT * FROM df ORDER BY k) WHERE v >= 0",
        "SELECT v % 1000 AS g, COUNT(*) AS n, MIN(v) AS v FROM df GROUP BY g ORDER BY n",
        "SELECT a.k, a.v FROM df a JOIN df b ON a.v = b.v ORDER BY a.k",
        "SELECT a.k, a.v FROM df a LEFT JOIN df b ON a.v = b.v + 1 ORDER BY a.k",
        "SELECT k, v FROM df UNION SELECT k, v FROM df ORDER BY k",
        "SELECT DISTINCT v % 1000 AS g, v % 1000 AS v FROM df ORDER BY g % 3",
    ] {
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        let unstable = (&state.lf.logical_plan).into_iter().any(|node| {
            matches!(
                node,
                polars::lazy::dsl::DslPlan::Sort { sort_options, .. }
                    if !sort_options.maintain_order
            )
        });
        assert!(!unstable, "{sql}");
        // The app pages with the streaming engine by default, which runs the top
        // page's top-k its own way.
        for streaming in [false, cfg!(feature = "streaming")] {
            let page =
                |offset, len| collect_lazy(state.lf.clone().slice(offset, len), streaming).unwrap();
            let top = page(0, 200);
            let next = page(100, 200);
            assert!(top.slice(100, 100).equals(&next.slice(0, 100)), "{sql}");
            let one = page(1500, 1);
            let around = page(1400, 200);
            assert!(around.slice(100, 1).equals(&one), "{sql}");
            // Ties keep the order they were read in.
            let v = top.column("v").unwrap().i64().unwrap();
            assert!(
                v.into_no_null_iter().is_sorted(),
                "{sql}, streaming {streaming}: {:?}",
                v.head(Some(10))
            );
        }
    }
}

/// A SQL result with no ORDER BY reads the same rows page by page, and a Sort &
/// Filter sort over it keeps its ties in that order: groupings, distincts, unions
/// and joins give their rows in one order, so every page agrees with a read of
/// the whole result, and a LIMIT keeps the same rows (#508). Both engines give the
/// same order. A grouping sorted by its keys leaves its groups' order to the sort.
#[cfg(feature = "sql")]
#[test]
fn a_sql_result_without_order_by_reads_the_same_rows_page_by_page() {
    let df = df!(
        "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..5000i64).collect::<Vec<_>>(),
    )
    .unwrap();
    for sql in [
        "SELECT a.k, a.v FROM df a JOIN df b ON a.v = b.v",
        "SELECT a.k, a.v, b.v AS w FROM df a LEFT JOIN df b ON a.v = b.v + 1",
        "SELECT k, v FROM df UNION ALL SELECT k, v FROM df",
        "SELECT k, v FROM df UNION SELECT k, v FROM df",
        "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g LIMIT 300",
        "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g",
        "SELECT g, COUNT(*) AS n FROM (SELECT v % 1000 AS g FROM df) GROUP BY g",
        "SELECT DISTINCT v % 1000 AS g FROM df",
        "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 1) LIMIT 1000",
    ] {
        for sort in [false, true] {
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            if sort {
                // By the first column, which most of these results repeat.
                let first = state.schema.get_at_index(0).unwrap().0.to_string();
                state.sort(vec![first], true);
                assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            }
            let mut fulls = Vec::new();
            for streaming in [false, cfg!(feature = "streaming")] {
                let read = |lf: LazyFrame| collect_lazy(lf, streaming).unwrap();
                let full = read(state.lf.clone());
                let middle = full.height() as i64 / 2;
                for offset in [0, 100, middle] {
                    let page = read(state.lf.clone().slice(offset, 200));
                    assert!(
                        page.equals_missing(&full.slice(offset, 200)),
                        "{sql}, sort {sort}, streaming {streaming}, offset {offset}"
                    );
                }
                let one = read(state.lf.clone().slice(middle + 7, 1));
                assert!(
                    one.equals_missing(&full.slice(middle + 7, 1)),
                    "{sql}, sort {sort}, streaming {streaming}"
                );
                fulls.push(full);
            }
            assert!(
                fulls[0].equals_missing(&fulls[1]),
                "{sql}, sort {sort}: the engines disagree"
            );
        }
    }
    // Keeping the groups' order as well would double a large grouping's time.
    let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
    state.sql_query("SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g".to_string());
    assert!((&state.lf.logical_plan).into_iter().any(|node| matches!(
        node,
        polars::lazy::dsl::DslPlan::GroupBy {
            maintain_order: false,
            ..
        }
    )));
}

/// A SQL grouping whose ORDER BY covers every key, by alias, ordinal or name,
/// leaves its groups' order to the sort (#523), and still reads the same rows page
/// by page, in the order polars-sql's own plan gives, with either engine, NULL and
/// NaN keys too. A sort that leaves any key out, sorts by an expression of one, or
/// reads the groups through a LIMIT, a filter or a computed column keeps the
/// groups' order.
#[cfg(feature = "sql")]
#[test]
fn a_sql_grouping_sorted_by_its_keys_leaves_the_order_to_the_sort() {
    use polars::lazy::dsl::DslPlan;
    // NaNs group as one and sort as one, as do 0.0 and -0.0.
    let floats = [0.0, -0.0, f64::NAN, -f64::NAN, 1.0, f64::INFINITY, -1.5];
    let df = df!(
        "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
        "v" => (0..5000i64).collect::<Vec<_>>(),
        "w" => (0..5000i64).map(|i| (i % 11 != 0).then_some(i % 700)).collect::<Vec<_>>(),
        "f" => (0..5000usize)
            .map(|i| (i % 9 != 0).then_some(floats[i % floats.len()]))
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let groups_ordered = |plan: &DslPlan| -> Vec<bool> {
        plan.into_iter()
            .filter_map(|node| match node {
                DslPlan::GroupBy { maintain_order, .. } => Some(*maintain_order),
                _ => None,
            })
            .collect()
    };
    for (sql, ordered) in [
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g ORDER BY g",
            false,
        ),
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY v % 1000 ORDER BY 1 DESC LIMIT 300",
            false,
        ),
        (
            "SELECT k AS kk, v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY k, g ORDER BY n, g, kk",
            false,
        ),
        (
            "SELECT k AS d, COUNT(*) AS n FROM df GROUP BY k ORDER BY k DESC",
            false,
        ),
        (
            "SELECT w, MIN(v) AS v FROM df GROUP BY w HAVING COUNT(*) > 1 ORDER BY ALL",
            false,
        ),
        (
            "SELECT w, COUNT(*) AS n FROM df GROUP BY w ORDER BY w DESC NULLS FIRST",
            false,
        ),
        (
            "SELECT k, f, COUNT(*) AS n FROM df GROUP BY k, f ORDER BY f DESC NULLS FIRST, k",
            false,
        ),
        (
            "SELECT w % 7 AS w, COUNT(*) AS n FROM df GROUP BY w % 7 ORDER BY w",
            false,
        ),
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g ORDER BY n",
            true,
        ),
        (
            "SELECT k, w, COUNT(*) AS n FROM df GROUP BY k, w ORDER BY k, w + 0",
            true,
        ),
        (
            "SELECT * FROM (SELECT w, COUNT(*) AS n FROM df GROUP BY w) WHERE n > 7 ORDER BY w",
            true,
        ),
        (
            "SELECT k, v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY k, g ORDER BY g",
            true,
        ),
        (
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY v % 1000 ORDER BY v % 1000",
            true,
        ),
        (
            "SELECT * FROM (SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g LIMIT 300) ORDER BY g",
            true,
        ),
        (
            "SELECT g, ROW_NUMBER() OVER () AS r FROM (SELECT v % 1000 AS g FROM df GROUP BY g) ORDER BY g",
            true,
        ),
    ] {
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query(sql.to_string());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        assert_eq!(groups_ordered(&state.lf.logical_plan), [ordered], "{sql}");
        let mut ctx = polars_sql::SQLContext::new();
        ctx.register("df", df.clone().lazy());
        let raw = ctx.execute(sql).unwrap();
        for streaming in [false, cfg!(feature = "streaming")] {
            let read = |lf: LazyFrame| collect_lazy(lf, streaming).unwrap();
            let full = read(state.lf.clone());
            if !ordered {
                // No ties, so polars-sql's unstable sort gives the one order too.
                assert!(
                    full.equals_missing(&read(raw.clone())),
                    "{sql}, streaming {streaming}"
                );
            }
            let middle = full.height() as i64 / 2;
            for offset in [0, 100, middle] {
                let page = read(state.lf.clone().slice(offset, 200));
                assert!(
                    page.equals_missing(&full.slice(offset, 200)),
                    "{sql}, streaming {streaming}, offset {offset}"
                );
            }
        }
    }
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

#[test]
fn test_by_query_puts_null_group_last() {
    let lf = df!(
        "g" => &[Some(2i64), None, Some(1), Some(2)],
        "v" => &[1i64, 2, 3, 4],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.query("select sum v by g".to_string());
    assert!(state.error.is_none(), "{:?}", state.error);
    assert_eq!(column_values(&state, "g"), [Some(1), Some(2), None]);
}

#[test]
fn test_filter_multiple() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let filters = vec![
        FilterStatement {
            columns: Vec::new(),
            column: "c".to_string(),
            operator: FilterOperator::Eq,
            value: "1".to_string(),
            logical_op: LogicalOperator::And,
        },
        FilterStatement {
            columns: Vec::new(),
            column: "d".to_string(),
            operator: FilterOperator::Eq,
            value: "2".to_string(),
            logical_op: LogicalOperator::And,
        },
    ];
    state.filter(filters);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.shape().0, 7);
}

#[test]
fn test_filter_and_sort() {
    let lf = create_large_test_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let filters = vec![FilterStatement {
        columns: Vec::new(),
        column: "c".to_string(),
        operator: FilterOperator::Eq,
        value: "1".to_string(),
        logical_op: LogicalOperator::And,
    }];
    state.filter(filters);
    state.sort(vec!["a".to_string()], false);
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
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

#[test]
fn test_pivot_basic() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
    assert!(names.contains(&"A"));
    assert!(names.contains(&"B"));
    assert!(names.contains(&"C"));
    assert_eq!(df.height(), 2);
}

#[test]
fn test_pivot_aggregation_last() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    let a_col = df.column("A").unwrap();
    let row0 = a_col.get(0).unwrap();
    let row1 = a_col.get(1).unwrap();
    assert_eq!(row0, AnyValue::Float64(11.0));
    assert_eq!(row1, AnyValue::Float64(40.0));
}

#[test]
fn test_pivot_aggregation_first() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::First,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    let a_col = df.column("A").unwrap();
    assert_eq!(a_col.get(0).unwrap(), AnyValue::Float64(10.0));
    assert_eq!(a_col.get(1).unwrap(), AnyValue::Float64(40.0));
}

#[test]
fn test_pivot_aggregation_min_max() {
    let lf = create_pivot_long_lf();
    let mut state_min = DataTableState::new(lf.clone(), None, None, None, None, true).unwrap();
    state_min
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Min,
            sort_columns: None,
        })
        .unwrap();
    let df_min = state_min.lf.clone().collect().unwrap();
    assert_eq!(
        df_min.column("A").unwrap().get(0).unwrap(),
        AnyValue::Float64(10.0)
    );

    let mut state_max = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state_max
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Max,
            sort_columns: None,
        })
        .unwrap();
    let df_max = state_max.lf.clone().collect().unwrap();
    assert_eq!(
        df_max.column("A").unwrap().get(0).unwrap(),
        AnyValue::Float64(11.0)
    );
}

#[test]
fn test_pivot_aggregation_avg_count() {
    let lf = create_pivot_long_lf();
    let mut state_avg = DataTableState::new(lf.clone(), None, None, None, None, true).unwrap();
    state_avg
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Avg,
            sort_columns: None,
        })
        .unwrap();
    let df_avg = state_avg.lf.clone().collect().unwrap();
    let a = df_avg.column("A").unwrap().get(0).unwrap();
    if let AnyValue::Float64(x) = a {
        assert!((x - 10.5).abs() < 1e-6);
    } else {
        panic!("expected float");
    }

    let mut state_count = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state_count
        .pivot(&PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Count,
            sort_columns: None,
        })
        .unwrap();
    let df_count = state_count.lf.clone().collect().unwrap();
    let a = df_count.column("A").unwrap().get(0).unwrap();
    assert_eq!(a, AnyValue::UInt32(2));
}

#[test]
fn test_pivot_string_first_last() {
    let df = df!(
        "id" => &[1_i32, 1, 2, 2],
        "key" => &["X", "Y", "X", "Y"],
        "value" => &["low", "mid", "high", "mid"],
    )
    .unwrap();
    let lf = df.lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let out = state.lf.clone().collect().unwrap();
    assert_eq!(
        out.column("X").unwrap().get(0).unwrap(),
        AnyValue::String("low")
    );
    assert_eq!(
        out.column("Y").unwrap().get(0).unwrap(),
        AnyValue::String("mid")
    );
}

#[test]
fn test_melt_basic() {
    let lf = create_melt_wide_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = MeltSpec {
        index: vec!["id".to_string(), "date".to_string()],
        value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        variable_name: "variable".to_string(),
        value_name: "value".to_string(),
    };
    state.melt(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 9);
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert!(names.contains(&"variable"));
    assert!(names.contains(&"value"));
    assert!(names.contains(&"id"));
    assert!(names.contains(&"date"));
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

/// Pivoted on a date past the calendar, its new column is named by the stored
/// number, the columns still in date order; Polars' own naming panicked (#506).
/// Without one, the columns are named as they always were.
#[test]
fn a_pivot_on_a_date_past_the_calendar_names_it_by_its_stored_number() {
    for on in PAST_CALENDAR {
        let [first, past] = past_calendar_text(on);
        for with_past in [true, false] {
            let lf = past_calendar_lf()
                .filter(col("id").eq(lit(1)).or(lit(with_past)))
                .select([col("id"), col(on), col("s")]);
            let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
            state
                .pivot(&PivotSpec {
                    index: vec!["id".to_string()],
                    pivot_column: on.to_string(),
                    value_column: "s".to_string(),
                    aggregation: PivotAggregation::First,
                    sort_columns: None,
                })
                .unwrap();
            let df = state.lf.clone().collect().unwrap();
            let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
            let dates = match (with_past, on) {
                (false, _) => vec![first.as_str()],
                // i32::MAX days is after 1970, i64::MIN + 1 before it.
                (true, "d") => vec![first.as_str(), past.as_str()],
                (true, _) => vec![past.as_str(), first.as_str()],
            };
            assert_eq!(names[1..], dates, "{on}");
            assert_eq!(
                df.column(&first).unwrap().str().unwrap().get(0),
                Some("a"),
                "{on}"
            );
            if with_past {
                assert_eq!(
                    df.column(&past).unwrap().str().unwrap().get(1),
                    Some("b"),
                    "{on}"
                );
            }
        }
    }
}

/// Melted with text, a date past the calendar is its stored number, where the
/// cast to text panicked (#506), and one in range Polars' own text. Melted with
/// dates only, the values stay dates.
#[test]
fn a_melt_of_dates_with_text_writes_a_date_past_the_calendar_as_its_number() {
    let melt = |columns: [&str; 2]| {
        let mut state =
            DataTableState::new(past_calendar_lf(), None, None, None, None, true).unwrap();
        state
            .melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: columns.map(String::from).to_vec(),
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .unwrap();
        state.lf.clone().collect().unwrap()
    };
    for column in PAST_CALENDAR {
        let [first, past] = past_calendar_text(column);
        let df = melt([column, "s"]);
        let values: Vec<Option<&str>> = df.column("value").unwrap().str().unwrap().iter().collect();
        assert_eq!(
            values,
            [
                Some(first.as_str()),
                Some(past.as_str()),
                Some("a"),
                Some("b")
            ],
            "{column}"
        );
    }
    let df = melt(["t_ms", "t_us"]);
    assert!(matches!(
        df.column("value").unwrap().dtype(),
        DataType::Datetime(..)
    ));
}

/// SQL's three plan rewrites compose: in a join filtered by an `IN` subquery, a
/// date past the calendar met with text is its stored number (#506), the join
/// keeps one row order (#508), and the subquery's values are read once (#509).
#[cfg(feature = "sql")]
#[test]
fn a_sql_join_with_an_in_subquery_and_a_date_past_the_calendar() {
    use polars::lazy::dsl::DslPlan;
    for c in PAST_CALENDAR {
        let [first, past] = past_calendar_text(c);
        let sql = format!(
            "SELECT a.id, COALESCE(a.{c}, b.s) AS x FROM df a JOIN df b ON a.id = b.id \
             WHERE a.id IN (SELECT t.id FROM df t WHERE t.s <> 'z')"
        );
        let mut state =
            DataTableState::new(past_calendar_lf(), None, None, None, None, true).unwrap();
        state.sql_query(sql.clone());
        assert!(state.error.is_none(), "{sql}: {:?}", state.error);
        let plan = &state.lf.logical_plan;
        assert!(!plan.into_iter().any(asks_per_row), "{sql}");
        let joins: Vec<_> = plan
            .into_iter()
            .filter_map(|node| match node {
                DslPlan::Join { options, .. } => Some(options.args.maintain_order),
                _ => None,
            })
            .collect();
        assert!(!joins.is_empty(), "{sql}");
        assert!(
            joins.iter().all(|order| *order != MaintainOrderJoin::None),
            "{sql}"
        );
        for streaming in [false, cfg!(feature = "streaming")] {
            let df = collect_lazy(state.lf.clone(), streaming).unwrap();
            let ids: Vec<Option<i32>> = df.column("id").unwrap().i32().unwrap().iter().collect();
            assert_eq!(ids, [Some(1), Some(2)], "{sql}, streaming {streaming}");
            let x: Vec<Option<&str>> = df.column("x").unwrap().str().unwrap().iter().collect();
            assert_eq!(
                x,
                [Some(first.as_str()), Some(past.as_str())],
                "{sql}, streaming {streaming}"
            );
            let page = collect_lazy(state.lf.clone().slice(1, 1), streaming).unwrap();
            assert!(page.equals_missing(&df.slice(1, 1)), "{sql}");
        }
    }
}

/// A q query forgets the melt it replaces; rolled back, the melt is
/// what SQL runs against again, not only what the table shows.
#[test]
fn a_rollback_brings_back_the_melt_a_query_forgot() {
    let lf = create_melt_wide_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = MeltSpec {
        index: vec!["id".to_string(), "date".to_string()],
        value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        variable_name: "variable".to_string(),
        value_name: "value".to_string(),
    };
    state.melt(&spec).unwrap();
    let saved = state.rollback_point();
    state.query("select id".to_string());
    assert!(state.last_melt_spec().is_none());
    state.roll_back(saved);
    assert!(state.last_melt_spec().is_some());
    assert_eq!(state.query_root().collect().unwrap().height(), 9);
}

#[test]
fn test_melt_all_except_index() {
    let lf = create_melt_wide_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    let spec = MeltSpec {
        index: vec!["id".to_string(), "date".to_string()],
        value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
        variable_name: "var".to_string(),
        value_name: "val".to_string(),
    };
    state.melt(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert!(df.column("var").is_ok());
    assert!(df.column("val").is_ok());
}

#[test]
fn test_pivot_on_current_view_after_filter() {
    let lf = create_pivot_long_lf();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "id".to_string(),
        operator: FilterOperator::Eq,
        value: "1".to_string(),
        logical_op: LogicalOperator::And,
    }]);
    let spec = PivotSpec {
        index: vec!["id".to_string(), "date".to_string()],
        pivot_column: "key".to_string(),
        value_column: "value".to_string(),
        aggregation: PivotAggregation::Last,
        sort_columns: None,
    };
    state.pivot(&spec).unwrap();
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 1);
    let id_col = df.column("id").unwrap();
    assert_eq!(id_col.get(0).unwrap(), AnyValue::Int32(1));
}

/// The one-pass pivot gives what the lazy pivot over the whole view gave, for every
/// aggregation, with nulls in the index, the pivot column and the values, pairs with
/// no rows, and more rows than one streaming morsel, so `first` and `last` are
/// checked for order across batches.
#[test]
fn a_pivot_in_one_pass_matches_the_lazy_pivot() {
    let n = 250_000usize;
    let view = df!(
        "g" => (0..n)
            .map(|i| (i % 13 != 0).then_some((i % 97) as i64))
            .collect::<Vec<_>>(),
        "key" => (0..n)
            .map(|i| (i % 17 != 0).then(|| format!("k{}", (i * 7) % 11)))
            .collect::<Vec<_>>(),
        "v" => (0..n)
            .map(|i| (i % 7 != 0).then_some((i % 1_000) as f64))
            .collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    // Pairs with no rows at all: `k3` never meets a `g` divisible by five.
    .filter(
        (col("g") % lit(5i64))
            .neq(lit(0i64))
            .or(col("key").neq(lit("k3")))
            .fill_null(lit(true)),
    );

    // The pivot as it ran before: the new columns read first, then the lazy pivot.
    let lazy_pivot = |spec: &PivotSpec| {
        let on = spec.pivot_column.as_str();
        let value = spec.value_column.as_str();
        let on_columns = view
            .clone()
            .select([col(on)])
            .unique(None, UniqueKeepStrategy::Any)
            .sort([on], SortMultipleOptions::default().with_nulls_last(true))
            .collect()
            .unwrap();
        let index = if spec.index.is_empty() {
            all() - by_name([on, value], true, false)
        } else {
            by_name(spec.index.iter().map(String::as_str), true, false)
        };
        view.clone()
            .pivot(
                by_name([on], true, false),
                Arc::new(on_columns),
                index,
                by_name([value], true, false),
                pivot_agg_expr(spec.aggregation, element()),
                true,
                PlSmallStr::from_static("_"),
                PivotColumnNaming::Auto,
            )
            .collect()
            .unwrap()
    };
    let close = |a: &DataFrame, b: &DataFrame| {
        a.get_column_names() == b.get_column_names()
            && a.height() == b.height()
            && a.columns().iter().zip(b.columns()).all(|(x, y)| {
                if x.dtype().is_float() {
                    let (x, y) = (x.f64().unwrap(), y.f64().unwrap());
                    x.iter().zip(y.iter()).all(|pair| match pair {
                        (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(1.0),
                        (x, y) => x.is_none() && y.is_none(),
                    })
                } else {
                    x.as_materialized_series()
                        .equals_missing(y.as_materialized_series())
                }
            })
    };

    for index in [vec!["g".to_string()], Vec::new()] {
        for aggregation in PivotAggregation::ALL {
            let spec = PivotSpec {
                index: index.clone(),
                pivot_column: "key".to_string(),
                value_column: "v".to_string(),
                aggregation,
                sort_columns: None,
            };
            let expected = lazy_pivot(&spec);
            for streaming in [false, true] {
                let pivoted = PivotJob {
                    view: view.clone(),
                    spec: spec.clone(),
                    streaming,
                }
                .run()
                .unwrap();
                assert!(
                    close(&pivoted, &expected),
                    "{aggregation:?}, index {index:?}, streaming {streaming}:\n\
                     {pivoted:?}\n{expected:?}"
                );
            }
        }
    }
}

#[test]
fn test_fuzzy_token_regex() {
    assert_eq!(fuzzy_token_regex("foo"), "(?i).*f.*o.*o.*");
    assert_eq!(fuzzy_token_regex("a"), "(?i).*a.*");
    // Regex-special characters are escaped
    let pat = fuzzy_token_regex("[");
    assert!(pat.contains("\\["));
}

#[test]
fn test_fuzzy_search() {
    // Filter logic is covered by test_fuzzy_search_regex_direct. This test runs the full
    // path through DataTableState; it requires sample data (CSV with string column).
    crate::tests::ensure_sample_data();
    let path = crate::tests::sample_data_dir().join("3-sfd-header.csv");
    let mut state = DataTableState::from_csv(&path, &Default::default()).unwrap();
    state.visible_rows = 10;
    state.collect();
    let before = state.num_rows;
    state.fuzzy_search("string".to_string());
    assert!(state.error.is_none(), "{:?}", state.error);
    assert!(state.num_rows <= before, "fuzzy search should filter rows");
    state.fuzzy_search("".to_string());
    state.collect();
    assert_eq!(state.num_rows, before, "empty fuzzy search should reset");
    assert!(state.get_active_fuzzy_query().is_empty());
}

#[test]
fn test_fuzzy_search_regex_direct() {
    // Sanity check: Polars str().contains with our regex matches "alice" for pattern ".*a.*l.*i.*"
    let lf = df!("name" => &["alice", "bob", "carol"]).unwrap().lazy();
    let pattern = fuzzy_token_regex("alice");
    let out = lf
        .filter(col("name").str().contains(lit(pattern.clone()), false))
        .collect()
        .unwrap();
    assert_eq!(out.height(), 1, "regex {:?} should match alice", pattern);

    // Two columns OR (as in fuzzy_search)
    let lf2 = df!(
        "id" => &[1i32, 2, 3],
        "name" => &["alice", "bob", "carol"],
        "city" => &["NYC", "LA", "Boston"]
    )
    .unwrap()
    .lazy();
    let pat = fuzzy_token_regex("alice");
    let expr = col("name")
        .str()
        .contains(lit(pat.clone()), false)
        .or(col("city").str().contains(lit(pat), false));
    let out2 = lf2.clone().filter(expr).collect().unwrap();
    assert_eq!(out2.height(), 1);

    // Replicate exact fuzzy_search logic: schema from original_lf, string_cols, then filter
    let schema = lf2.clone().collect_schema().unwrap();
    let string_cols: Vec<String> = schema
        .iter()
        .filter(|(_, dtype)| dtype.is_string())
        .map(|(name, _)| name.to_string())
        .collect();
    assert!(
        !string_cols.is_empty(),
        "df! string cols should be detected"
    );
    let pattern = fuzzy_token_regex("alice");
    let token_expr = string_cols
        .iter()
        .map(|c| col(c.as_str()).str().contains(lit(pattern.clone()), false))
        .reduce(|a, b| a.or(b))
        .unwrap();
    let out3 = lf2.filter(token_expr).collect().unwrap();
    assert_eq!(
        out3.height(),
        1,
        "fuzzy_search-style filter should match 1 row"
    );
}

/// A fuzzy search replaces the sort along with the rest of the pipeline: a descending
/// sort before it must not leave the result reversed with no sort column to show it.
#[test]
fn fuzzy_search_after_a_descending_sort_is_not_reversed() {
    let lf = df!(
        "id" => &[1i32, 2, 3],
        "name" => &["alice", "bob", "carol"]
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["id".to_string()], false);
    state.fuzzy_search("a".to_string());
    assert!(state.error.is_none(), "{:?}", state.error);
    assert!(state.view_sort_columns().is_empty());
    assert!(state.view_sort_ascending());

    // The sidebar re-applies the (empty) filters and sort over the result.
    state.filter(Vec::new());
    let df = state.lf.clone().collect().unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(df.column("id").unwrap().get(0).unwrap(), AnyValue::Int32(1));
}

/// A reshape likewise replaces the sort. It runs over the sorted view, so its rows
/// come out descending, and a sidebar action afterwards must leave them that way
/// rather than reverse a frame that shows no sort column.
#[test]
fn melt_after_a_descending_sort_is_not_reversed() {
    let lf = df!("id" => &[1i32, 2, 3], "c1" => &[10i32, 20, 30])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.sort(vec!["id".to_string()], false);
    state
        .melt(&MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["c1".to_string()],
            variable_name: "var".to_string(),
            value_name: "val".to_string(),
        })
        .unwrap();
    assert!(state.view_sort_columns().is_empty());
    assert!(state.view_sort_ascending());
    let melted = state.lf.clone().collect().unwrap();
    assert_eq!(
        melted.column("id").unwrap().get(0).unwrap(),
        AnyValue::Int32(3)
    );

    state.filter(Vec::new());
    assert!(state.lf.clone().collect().unwrap().equals(&melted));
}

#[test]
fn test_fuzzy_search_no_string_columns() {
    let lf = df!("a" => &[1i32, 2, 3], "b" => &[10i64, 20, 30])
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.fuzzy_search("x".to_string());
    assert!(state.error.is_some());
}

/// What the Search hint promises: every word's letters in order, in any text
/// column. Each word may match a different column; letters out of order do not.
#[test]
fn search_matches_every_words_letters_in_order_in_any_text_column() {
    let rows = |query: &str| {
        let lf = df!(
            "name" => &["Smith", "Marion", "Smith"],
            "city" => &["London", "London", "Paris"]
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.fuzzy_search(query.to_string());
        assert!(state.error.is_none(), "{:?}", state.error);
        state.lf.clone().collect().unwrap().height()
    };
    assert_eq!(rows("smth"), 2, "letters in order, not adjacent, any case");
    assert_eq!(rows("smth ldn"), 1, "every word, each in its own column");
    assert_eq!(rows("htims"), 0, "letters out of order");
}

/// By-queries must produce results sorted by the group columns (age_group, then team)
/// so that output order is deterministic and practical. Raw data is deliberately out of order.
#[test]
fn test_by_query_result_sorted_by_group_columns() {
    // Build a small table: age_group (1-5, out of order), team (Red/Blue/Green), score (0-100)
    let df = df!(
        "age_group" => &[3i64, 1, 5, 2, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
        "team" => &[
            "Red", "Blue", "Green", "Red", "Blue", "Green", "Green", "Red", "Blue",
            "Green", "Red", "Blue", "Red", "Blue", "Green",
        ],
        "score" => &[50.0f64, 10.0, 90.0, 20.0, 30.0, 40.0, 60.0, 70.0, 80.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0],
    )
    .unwrap();
    let lf = df.lazy();
    let options = crate::OpenOptions::default();
    let mut state = DataTableState::from_lazyframe(lf, &options).unwrap();
    state.query("select avg score by age_group, team".to_string());
    assert!(
        state.error.is_none(),
        "query should succeed: {:?}",
        state.error
    );
    let result = state.lf.collect().unwrap();
    // Result must be sorted by group columns (age_group, then team)
    let sorted = result
        .sort(
            ["age_group", "team"],
            SortMultipleOptions::default().with_order_descending(false),
        )
        .unwrap();
    assert_eq!(
        result, sorted,
        "by-query result must be sorted by (age_group, team)"
    );
}

/// Computed group keys (e.g. Fare: 1+floor Fare % 25) must be sorted by their result column
/// values, not by re-evaluating the expression on the result.
#[test]
fn test_by_query_computed_group_key_sorted_by_result_column() {
    let df = df!(
        "x" => &[7.0f64, 12.0, 3.0, 22.0, 17.0, 8.0],
        "v" => &[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0],
    )
    .unwrap();
    let lf = df.lazy();
    let options = crate::OpenOptions::default();
    let mut state = DataTableState::from_lazyframe(lf, &options).unwrap();
    // bucket: 1+floor(x)%3 -> values 1,2,3; raw x order 7,12,3,22,17,8 -> buckets 2,2,1,2,2,2
    state.query("select sum v by bucket: 1+floor x % 3".to_string());
    assert!(
        state.error.is_none(),
        "query should succeed: {:?}",
        state.error
    );
    let result = state.lf.collect().unwrap();
    let bucket = result.column("bucket").unwrap();
    // Must be sorted by bucket (1, 2, 3)
    for i in 1..result.height() {
        let prev: i64 = bucket.get(i - 1).unwrap().try_extract().unwrap_or(0);
        let curr: i64 = bucket.get(i).unwrap().try_extract().unwrap_or(0);
        assert!(
            curr >= prev,
            "bucket column must be sorted: {} then {}",
            prev,
            curr
        );
    }
}

// Read the header (top) row of a rendered buffer as a string.
fn header_row_string(buf: &Buffer, area: Rect) -> String {
    (area.x..area.x + area.width)
        .map(|x| buf[(x, area.y)].symbol().to_string())
        .collect()
}

// Read an arbitrary row of a rendered buffer as a string (y = 0 is the header).
fn row_string(buf: &Buffer, area: Rect, y: u16) -> String {
    (area.x..area.x + area.width)
        .map(|x| buf[(x, area.y + y)].symbol().to_string())
        .collect()
}

fn table_with_format(preset: &str, align: bool) -> DataTable {
    DataTable::default().with_number_format(NumberFormatSettings {
        format: crate::numfmt::NumberFormat::preset(preset).unwrap(),
        enabled: true,
        exclude: Vec::new(),
        align_numeric_right: align,
    })
}

#[test]
fn grouping_is_off_by_default() {
    // Upgrading must not change how anything renders.
    let table = DataTable::default();
    let df = df!("pos" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 30, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let row = row_string(&buf, area, 1);
    assert!(row.contains("1234567"), "got: {row:?}");
    assert!(!row.contains("1,234,567"), "got: {row:?}");
}

#[test]
fn thousands_separators_are_applied_to_integer_columns() {
    let table = table_with_format("thousands", false);
    // Genomic coordinates, the case from issue #51.
    let df = df!("chromStart" => &[248956422i64, 3088269832]).unwrap();
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert!(row_string(&buf, area, 1).contains("248,956,422"));
    assert!(row_string(&buf, area, 2).contains("3,088,269,832"));
}

#[test]
fn column_width_accounts_for_separators() {
    // The separators widen the column; the heading must not be clipped and
    // the value must render in full.
    let table = table_with_format("thousands", false);
    let df = df!("n" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 12, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(shown, 1);
    assert!(row_string(&buf, area, 1).contains("1,234,567"));
}

#[test]
fn strings_are_untouched_and_integers_group_uniformly() {
    let table = table_with_format("thousands", false);
    let df = df!(
        "chrom" => &["chr1"],
        "n" => &[2024i32],
    )
    .unwrap();
    let area = Rect::new(0, 0, 30, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let row = row_string(&buf, area, 1);
    assert!(row.contains("chr1"), "got: {row:?}");
    // No magnitude threshold: a column must not mix grouped and ungrouped
    // values, so four-digit numbers group like everything else.
    assert!(row.contains("2,024"), "got: {row:?}");
}

#[test]
fn excluding_a_column_is_how_identifier_columns_stay_plain() {
    // The replacement for a digit threshold: name the columns that hold
    // identifiers rather than quantities.
    let table = DataTable::default().with_number_format(NumberFormatSettings {
        format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
        enabled: true,
        exclude: vec![crate::numfmt::Glob::new("year")],
        align_numeric_right: false,
    });
    let df = df!(
        "year" => &[2024i32],
        "count" => &[2024i32],
    )
    .unwrap();
    let area = Rect::new(0, 0, 40, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let row = row_string(&buf, area, 1);
    assert!(row.contains("2024"), "excluded column stays plain: {row:?}");
    assert!(row.contains("2,024"), "other column groups: {row:?}");
}

#[test]
fn numeric_columns_and_their_headers_render_flush_right() {
    let table = table_with_format("none", true);
    // Header "value" is 5 wide; the values are shorter, so they must be
    // padded on the left, not the right.
    let df = df!("value" => &[7i64, 42]).unwrap();
    let area = Rect::new(0, 0, 5, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(row_string(&buf, area, 1), "    7");
    assert_eq!(row_string(&buf, area, 2), "   42");
    assert_eq!(header_row_string(&buf, area), "value");
}

#[test]
fn header_follows_its_column_alignment() {
    // A wide numeric column: the heading must sit flush right over the
    // digits rather than floating left.
    let table = table_with_format("none", true);
    let df = df!("n" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 7, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(header_row_string(&buf, area), "      n");
    assert_eq!(row_string(&buf, area, 1), "1234567");
}

#[test]
fn non_numeric_columns_stay_left_aligned() {
    let table = table_with_format("none", true);
    let df = df!("name" => &["ab"]).unwrap();
    let area = Rect::new(0, 0, 4, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(row_string(&buf, area, 1), "ab  ");
    assert_eq!(header_row_string(&buf, area), "name");
}

#[test]
fn alignment_can_be_turned_off() {
    let table = table_with_format("none", false);
    let df = df!("value" => &[7i64]).unwrap();
    let area = Rect::new(0, 0, 5, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(row_string(&buf, area, 1), "7    ");
}

#[test]
fn excluded_columns_are_not_grouped() {
    let table = DataTable::default().with_number_format(NumberFormatSettings {
        format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
        enabled: true,
        exclude: vec![crate::numfmt::Glob::new("*_id")],
        align_numeric_right: false,
    });
    let df = df!(
        "sample_id" => &[1234567i64],
        "count" => &[1234567i64],
    )
    .unwrap();
    let area = Rect::new(0, 0, 40, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let row = row_string(&buf, area, 1);
    assert!(row.contains("1234567"), "excluded column raw: {row:?}");
    assert!(row.contains("1,234,567"), "other column grouped: {row:?}");
}

#[test]
fn disabled_formatting_renders_raw_digits() {
    // What the `,` toggle does: same settings, enabled = false.
    let mut settings = NumberFormatSettings {
        format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
        enabled: true,
        exclude: Vec::new(),
        align_numeric_right: false,
    };
    settings.enabled = false;
    let table = DataTable::default().with_number_format(settings);
    let df = df!("n" => &[1234567i64]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert!(row_string(&buf, area, 1).contains("1234567"));
}

#[test]
fn binary_stub_columns_are_never_formatted_or_aligned() {
    // The stub is a placeholder, not data.
    let table = DataTable {
        binary_cols: std::collections::HashSet::from(["blob".to_string()]),
        ..table_with_format("thousands", true)
    };
    let df = df!("blob" => &[binary_stub()]).unwrap();
    let area = Rect::new(0, 0, 10, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert!(row_string(&buf, area, 1).starts_with(binary_stub()));
}

/// The buffer holds a binary column as stub text; the type row still says binary.
#[test]
fn a_binary_column_s_type_row_says_binary() {
    let table = DataTable {
        binary_cols: std::collections::HashSet::from(["blob".to_string()]),
        dtype_row: true,
        ..table_with_format("thousands", true)
    };
    let df = df!("blob" => &[binary_stub()], "s" => &["x"]).unwrap();
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let types = row_string(&buf, area, 1);
    assert!(types.contains("binary"), "{types:?}");
    assert_eq!(types.matches("str").count(), 1, "{types:?}");
}

/// A line break or a tab in a value is marked in the one-line cell; drawn as
/// is, ratatui drops it and `line1\nline2` reads `line1line2`.
#[test]
fn breaks_tabs_and_controls_are_marked_in_a_cell() {
    let table = DataTable::default();
    let df = df!("s" => ["line1\nline2", "tab\tseparated", "esc\u{1b}[0m"]).unwrap();
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let g = table.glyphs;
    assert!(
        row_string(&buf, area, 1).starts_with(&format!("line1{}line2", g.newline_mark)),
        "{:?}",
        row_string(&buf, area, 1)
    );
    assert!(row_string(&buf, area, 2).starts_with(&format!("tab{}separated", g.tab_mark)));
    assert!(row_string(&buf, area, 3).starts_with(&format!("esc{}[0m", g.control_mark)));
}

/// A direction control in a value is marked too: drawn, a terminal that lays
/// out bidirectional text would reverse the rest of the row.
#[test]
fn direction_controls_are_marked_in_a_cell() {
    let table = DataTable::default();
    let df = df!("s" => ["a\u{202e}evil\u{202c}z"]).unwrap();
    let area = Rect::new(0, 0, 30, 3);
    let mut buf = Buffer::empty(area);
    table.render_dataframe(&df, area, &mut buf, &mut TableState::default(), false);
    let m = table.glyphs.control_mark;
    let row = row_string(&buf, area, 1);
    assert!(row.starts_with(&format!("a{m}evil{m}z")), "{row:?}");
    assert!(
        !buf.content()
            .iter()
            .any(|c| c.symbol().contains('\u{202e}'))
    );
}

/// A cell measures only the start of a huge value, and says it goes on.
#[test]
fn a_huge_value_is_measured_by_its_start() {
    let table = DataTable::default();
    let huge = "x".repeat(crate::exact::CELL_PREVIEW_BYTES * 4);
    let df = df!("s" => [huge.as_str()]).unwrap();
    let mut scratch = String::new();
    let slice = table.slice_column(&df, 0, 1, &HashSet::new(), &mut scratch);
    let ellipsis = crate::glyphs::cell_width(table.glyphs.ellipsis);
    assert_eq!(
        usize::from(slice.value_width),
        crate::exact::CELL_PREVIEW_BYTES + ellipsis
    );
}

#[test]
fn trailing_overflow_string_column_is_truncated_not_dropped() {
    // A string trailing column that doesn't fully fit should still be shown truncated
    // (filling the remaining width) rather than dropped entirely (leaving blank space).
    let table = DataTable::default();
    let df = df!(
        "a" => &[1i32, 2, 3],
        "wide_text" => &["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"],
    )
    .unwrap();
    // "a" needs width 1; with padding that's used_width 2, leaving 6 for "wide_text",
    // whose full width (10) overflows -> it must be shown truncated to the remaining 6.
    let area = Rect::new(0, 0, 8, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(
        shown, 2,
        "the overflowing trailing string column should be kept (truncated)"
    );
    // Part of the heading should be visible so the user knows what the column is,
    // behind the clip marker (three cells in the ASCII set).
    assert!(
        header_row_string(&buf, area).contains("wid"),
        "truncated column heading should be visible: {:?}",
        header_row_string(&buf, area)
    );
}

#[test]
fn binary_stub_cells_are_styled_with_binary_color_and_italic() {
    // Binary columns render the `‹binary›` stub; those cells should be colored with
    // binary_col and italicized so they read as a placeholder, while ordinary columns
    // keep their normal (non-italic) styling.
    let table = DataTable {
        binary_col: Some(Color::DarkGray),
        binary_cols: std::collections::HashSet::from(["blob".to_string()]),
        ..DataTable::default()
    };
    let df = df!(
        "a" => &[1i32, 2],
        "blob" => &[binary_stub(), binary_stub()],
    )
    .unwrap();
    let area = Rect::new(0, 0, 20, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);

    // A data row (y = 1; y = 0 is the header). The stub cells should be dark gray + italic.
    let stub_styled = (area.x..area.x + area.width).any(|x| {
        let cell = &buf[(x, 1)];
        cell.fg == Color::DarkGray && cell.modifier.contains(Modifier::ITALIC)
    });
    assert!(
        stub_styled,
        "binary stub cells should be colored with binary_col and italicized"
    );
    // The non-binary "a" column must not be italicized.
    let any_italic_non_darkgray = (area.x..area.x + area.width).any(|x| {
        let cell = &buf[(x, 1)];
        cell.modifier.contains(Modifier::ITALIC) && cell.fg != Color::DarkGray
    });
    assert!(
        !any_italic_non_darkgray,
        "only binary columns should be italicized"
    );
}

#[test]
fn trailing_overflow_numeric_column_is_dropped_not_truncated() {
    // A numeric column must NOT be shown truncated: a partial number reads as a wrong value.
    let table = DataTable::default();
    let df = df!(
        "a" => &[1i32, 2, 3],
        "wide_number" => &[111_111_111i64, 222_222_222, 333_333_333],
    )
    .unwrap();
    let area = Rect::new(0, 0, 8, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(
        shown, 1,
        "an overflowing numeric column should be dropped, not truncated"
    );
}

#[test]
fn byte_clamp_keeps_view_near_end_of_buffer() {
    // Regression: jumping to the END of a large dataset used to go blank because the
    // max_buffered_mb byte-clamp trimmed the buffer's HEAD (slice(0, max_rows)), discarding
    // exactly the tail rows the view needed. The clamp must keep the view in range.
    let lf = df!("a" => &["seed"]).unwrap().lazy();
    // 1 MB byte budget.
    let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
    state.num_rows = 1000;
    state.num_rows_valid = true;
    state.visible_rows = 40;
    state.start_row = 960; // jump-to-end position (num_rows - visible_rows)

    // Buffer spans [900, 1000); the view [960, 1000) sits at its tail. ~2 MB of data forces
    // a trim to roughly half the rows.
    let buffer_start = 900;
    let big: Vec<String> = (0..100).map(|_| "z".repeat(20_000)).collect();
    let df = df!("a" => big).unwrap();

    let fitted = state.fill_plan(buffer_start, 1000, 1000, true).fit(df);
    let (sliced, eff_start) = (fitted.df, fitted.start);
    let eff_end = eff_start + sliced.height();
    assert!(
        sliced.height() < 100,
        "expected a trim below the byte budget"
    );
    assert!(
        eff_start <= state.start_row,
        "view start {} fell before kept buffer start {}",
        state.start_row,
        eff_start
    );
    assert!(
        eff_end >= state.start_row + state.visible_rows,
        "view end {} fell after kept buffer end {}",
        state.start_row + state.visible_rows,
        eff_end
    );
    assert_eq!(
        sliced.height(),
        eff_end - eff_start,
        "df height must match range"
    );
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
    state.locked_columns_count = 1;
    state.visible_rows = 40;
    state
}

fn assert_view_rows(state: &DataTableState, source: &DataFrame) {
    let (start, end) = (state.buffered_start(), state.buffered_end());
    assert!(start <= state.start_row && state.start_row + 40 <= end);
    let held = state.buffered_df.as_ref().unwrap();
    assert_eq!(held.height(), end - start);
    assert!(held.equals_missing(&source.slice(start as i64, end - start)));
    let id = |df: &DataFrame, row: usize| df.column("id").unwrap().i64().unwrap().get(row);
    let locked = state.locked_df.as_ref().unwrap();
    assert_eq!(locked.get_column_names(), ["id"]);
    assert_eq!(
        id(locked, state.start_row - start),
        Some(state.start_row as i64)
    );
    let shown = state.df.as_ref().unwrap();
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
    state.num_rows = N;
    state.num_rows_valid = true;
    state.start_row = 7_500;
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

    // Synchronous: the same fill collected on the spot from an eager source, which
    // stays held by the frame itself; the buffer's copy does not. A binary column
    // is drawn as a stub there, so it is left out.
    let source = source.drop("bytes").unwrap();
    let mut state = trimming_state(source.clone().lazy());
    state.num_rows = N;
    state.num_rows_valid = true;
    state.start_row = 300;
    state.load_buffer(0, N);
    assert!(state.error.is_none());
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
        state.num_rows = N;
        state.num_rows_valid = !short;
        state.start_row = 300;
        let asked = if short { N + 5_000 } else { N };
        let result = state.fill_plan(0, asked, asked, !short).fit(source.clone());
        assert!(
            result.start + result.df.height() < 9_000,
            "the fill was cut"
        );
        state.start_row = 9_000;
        state.needs_recollect = false;
        state.apply_async_collect(result);
        assert!(
            state.buffered_df.is_none(),
            "nothing drawn under wrong numbers (short {short})"
        );
        assert!(state.needs_recollect);
        assert_eq!((state.num_rows, state.num_rows_valid), (N, true));
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
    let held = state.buffered_df.as_ref().unwrap();
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
        assert!(start <= state.start_row && state.start_row + 40 <= end);
        assert_eq!(end - start, CAP, "trimmed back to the cap");
        let held = state.buffered_df.as_ref().unwrap();
        let ids = held.column("id").unwrap().i64().unwrap();
        assert_eq!(ids.get(0), Some(start as i64));
        assert_eq!(ids.get(CAP - 1), Some(end as i64 - 1));
        for df in fetched {
            assert!(!shares_storage(held, df));
            assert!(!shares_storage(state.locked_df.as_ref().unwrap(), df));
            assert!(!shares_storage(state.df.as_ref().unwrap(), df));
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
        state.locked_columns_count = 1;
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
        let stitched = state.buffered_df.clone().unwrap();
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
        let held = state.buffered_df.as_ref().unwrap();
        assert_eq!(held.height(), end - start);
        assert!(!shares_storage(held, &other));
        assert!(!shares_storage(state.df.as_ref().unwrap(), &other));
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

/// The two checks that read footers rather than values: a column a file never had,
/// and a column a file holds in a type the scan cannot read.
///
/// Both are invisible to every measurement over values — an absent cell arrives as
/// a null and a conflicting one is not read at all — so the only way to test them
/// is through a dataset whose files genuinely disagree.
#[test]
fn absent_columns_and_type_conflicts_are_measured_from_the_footers() {
    use crate::data_quality::{DataQualityPlan, ObservationKind, QualityCompute, QualityScope};
    use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
    use polars::prelude::{DataType, IntoLazy, df};

    let urls: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    // `a` agrees with the schema; `b` holds `n` as text, which the scan cannot read
    // as the Int64 the majority wrote; `c` has no `fee` column at all.
    let scan: FileScan = Arc::new(move |urls: &[String], as_text: &[PlSmallStr]| {
        let reading_text = as_text.contains(&PlSmallStr::from("n"));
        let frames: Vec<LazyFrame> = urls
            .iter()
            .map(|url| match url.as_str() {
                "a" => df!(
                    "id" => &[0i64, 1, 2],
                    "n" => &[10i64, 20, 30],
                    "fee" => &[1.5f64, 2.5, 3.5],
                    crate::schema_union::DRIFT_COLUMN => &[0u32, 1, 2],
                )
                .unwrap()
                .lazy()
                .with_column(col("n").cast(if reading_text {
                    DataType::String
                } else {
                    DataType::Int64
                })),
                "b" => {
                    let frame = df!(
                        "id" => &[3i64, 4],
                        "n" => &["sixty", "seventy"],
                        "fee" => &[4.5f64, 5.5],
                        crate::schema_union::DRIFT_COLUMN => &[3u32, 4],
                    )
                    .unwrap()
                    .lazy();
                    if reading_text {
                        frame
                    } else {
                        // Not read from this file at all, as the real scan leaves it.
                        frame.with_column(lit(NULL).cast(DataType::Int64).alias("n"))
                    }
                }
                _ => df!(
                    "id" => &[5i64, 6],
                    "n" => &[50i64, 60],
                    crate::schema_union::DRIFT_COLUMN => &[5u32, 6],
                )
                .unwrap()
                .lazy()
                // A column the file never had reads as null, which is exactly why
                // no measurement over values can tell it from one.
                .with_column(lit(NULL).cast(DataType::Float64).alias("fee"))
                .select([
                    col("id"),
                    col("n").cast(if reading_text {
                        DataType::String
                    } else {
                        DataType::Int64
                    }),
                    col("fee"),
                    col(crate::schema_union::DRIFT_COLUMN),
                ]),
            })
            .collect();
        polars::prelude::concat(frames, Default::default())
    });

    let dataset: DatasetSchema = union_file_schemas(
        &[
            file_schema(
                &[
                    ("id", DataType::Int64),
                    ("n", DataType::Int64),
                    ("fee", DataType::Float64),
                ],
                3,
            ),
            file_schema(
                &[
                    ("id", DataType::Int64),
                    ("n", DataType::String),
                    ("fee", DataType::Float64),
                ],
                2,
            ),
            file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 2),
        ],
        SchemaOrigin::AllFooters(3),
    );
    let state = DataTableState::from_schema_and_lazyframe(
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
            count: Arc::new(|_| Ok(vec![vec![3], vec![2], vec![2]])),
            offsets: None,
        }),
        dataset: Some(DatasetAtOpen {
            schema: dataset,
            file_rows: vec![3, 2, 2],
            files: urls.clone(),
        }),
        ..Default::default()
    });
    assert!(state.drifts(), "the three files do not agree");

    let (lf, source) = state.data_quality_source_scan();
    let mut source = source.expect("every file is counted, so rows map to files");
    source.conflict_scan = state.quality_conflict_scan();
    let lf = crate::data_quality::prepare_source_quality_scan(lf, Some(&source)).unwrap();
    let plan = DataQualityPlan {
        scope: QualityScope::WholeSource,
        compute: QualityCompute::Full,
        ..DataQualityPlan::default()
    };
    let results =
        crate::data_quality::compute_data_quality(&lf, Some(7), &plan, Some(&source), false)
            .unwrap();

    let absent = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::Absent)
        .expect("`fee` is absent from the third file");
    assert_eq!(absent.column, "fee");
    assert_eq!(
        (absent.affected_rows, absent.evaluated_rows),
        (2, 7),
        "the third file's two rows, out of the source's seven"
    );
    assert_eq!(
        absent
            .files
            .iter()
            .map(|file| file.number)
            .collect::<Vec<_>>(),
        vec![3],
        "named by the number the Scope page gives it"
    );
    assert_eq!(
        absent.fact, "1 of 3 files has no such column",
        "every footer was read, so the count is a total rather than a floor"
    );

    let conflict = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::TypeConflict)
        .expect("`n` is text in the second file");
    assert_eq!(conflict.column, "n");
    assert_eq!((conflict.affected_rows, conflict.evaluated_rows), (2, 7));
    let file = conflict.files.first().expect("the file that disagrees");
    assert_eq!(file.number, 2);
    assert_eq!(file.stored_type.as_deref(), Some("str"));
    assert_eq!(
        file.examples,
        vec!["sixty".to_string(), "seventy".to_string()],
        "the values the conflict hides, read at the type that file wrote"
    );

    // The same two checks at the budget that reads no values at all: the footers
    // were read when the dataset opened, so there is nothing left to pay for.
    let metadata = crate::data_quality::compute_data_quality(
        &lf,
        Some(7),
        &DataQualityPlan {
            scope: QualityScope::WholeSource,
            compute: QualityCompute::Metadata,
            ..DataQualityPlan::default()
        },
        Some(&source),
        false,
    )
    .unwrap();
    assert_eq!(metadata.evaluated_rows, 0, "no value was read");
    assert_eq!(
        metadata
            .observations
            .iter()
            .map(|observation| (observation.kind, observation.affected_rows))
            .collect::<Vec<_>>(),
        vec![
            (ObservationKind::Absent, 2),
            (ObservationKind::TypeConflict, 2),
        ],
        "both are reported without reading a value"
    );

    // The drill-in is the files themselves: an absent cell has no value to filter.
    let scope = absent.evidence_scope().expect("a scope, not a predicate");
    assert_eq!(scope, QualityScope::SourceFiles(vec![3]));
    assert!(absent.evidence_predicate().is_none());
    let evidence = state
        .quality_evidence_view(&scope, lit(true))
        .expect("the rows the third file contributed");
    let rows = collect_lazy(evidence.lf.clone(), false).unwrap();
    assert_eq!(
        rows.column("id")
            .unwrap()
            .i64()
            .unwrap()
            .into_no_null_iter()
            .collect::<Vec<_>>(),
        vec![5, 6],
        "the file that has no `fee`, and only that file"
    );
    assert!(
        rows.column(crate::schema_union::DRIFT_COLUMN).is_err(),
        "the hidden scan index is never handed back as user data"
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
        state.schema.get("n"),
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
    state.num_rows = 1_000_000;
    state.num_rows_valid = true;
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
    assert_eq!(state.num_rows, 10 * G);
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
    let held = state.buffered_df.as_ref().unwrap();
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
        estimate_bytes_per_row(&state.schema, &state.column_order, &[]),
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
    state.num_rows = 1_000_000;
    state.num_rows_valid = true;
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
fn a_filtered_remote_scan_falls_back_to_the_page_window() {
    // `filter(..).slice(0, N)` stops at the first N matches, so a window of a few
    // pages stops at the first row group with any; the 100k window read forty.
    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    let lf = df!("a" => (0..1_000i32).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![500, 500]],
            parquet_count_dir: Some(PathBuf::from("/hive")),
            ..Default::default()
        });
    state.visible_rows = 40;
    state.defer_collect = true;

    let request = state.prepare_async_collect(None).expect("first fill");
    assert_eq!(
        (request.buffer_start, request.buffer_end),
        (0, 1_000),
        "both groups fit the remote window"
    );

    state.filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "a".to_string(),
        operator: FilterOperator::Gt,
        value: "990".to_string(),
        logical_op: LogicalOperator::And,
    }]);
    let request = state.prepare_async_collect(None).expect("filtered fill");
    assert_eq!(
        (request.buffer_start, request.buffer_end),
        (0, 7 * 40),
        "a page plus three either side, not the remote window"
    );

    state.filter(Vec::new());
    assert!(
        state.remote_window(),
        "with the filters cleared the frame is the scan as loaded again"
    );
    assert_eq!(
        state.num_rows_if_valid(),
        Some(1_000),
        "and its footer answers the count"
    );

    // The local hive footer count is gated the same way.
    assert_eq!(state.parquet_count_dir(), Some(PathBuf::from("/hive")));
    state.sort(vec!["a".to_string()], true);
    assert!(state.parquet_count_dir().is_none());
    state.sort(Vec::new(), true);
    assert_eq!(state.parquet_count_dir(), Some(PathBuf::from("/hive")));
    state.reverse();
    assert!(!state.remote_window(), "reversed is not as loaded");
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
    state.num_rows = 10_000;
    state.num_rows_valid = true;
    assert!(state.scroll_to_end());
    assert_eq!(state.start_row, 9_990);
    state.set_num_rows(100);
    assert_eq!(state.start_row, 90);
    assert!(state.needs_recollect);

    // A slice deep in the frame that found nothing is not the count.
    state.needs_recollect = false;
    state.num_rows_valid = false;
    state.start_row = 9_990;
    state.land(Fill {
        df: df!("a" => Vec::<i32>::new()).unwrap(),
        buffer_start: 9_990,
        buffer_end: 10_060,
        num_rows: 10_060,
        count_known: false,
    });
    assert!(
        !state.num_rows_valid,
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
fn a_new_base_is_measured_afresh() {
    // The width measured on a buffer of the old frame does not plan the new one.
    let big: Vec<String> = (0..100).map(|_| "z".repeat(2_000)).collect();
    let lf = df!("a" => &big, "b" => (0..100i32).collect::<Vec<i32>>())
        .unwrap()
        .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 10;
    state.defer_collect = true;
    state.land(Fill {
        df: df!("a" => &big, "b" => (0..100i32).collect::<Vec<i32>>()).unwrap(),
        buffer_start: 0,
        buffer_end: 100,
        num_rows: 100,
        count_known: true,
    });
    assert!(state.observed_bytes_per_row.is_some());
    state.query("select b".to_string());
    assert!(state.observed_bytes_per_row.is_none());
    assert_eq!(
        state.bytes_per_row(),
        4,
        "the narrow frame, from its schema"
    );
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
fn a_reset_remote_scan_is_pristine_again() {
    // A query makes the frame a predicate over the object, so the row-group window
    // and footer count stand down; clearing it brings both back without a len().
    let lf = df!("a" => (0..100).collect::<Vec<i32>>()).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true)
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            row_groups: vec![vec![60, 40]],
            ..Default::default()
        });
    assert!(state.remote_window());
    state.query("select a where a > 50".to_string());
    assert!(!state.remote_window());
    state.query(String::new());
    assert!(state.remote_window());
    assert_eq!(state.num_rows_if_valid(), Some(100));
}

#[test]
fn quality_source_scope_ignores_current_query_and_evidence_matches_scope() {
    let lf = df!("a" => &[1i32, 2, 3, 4]).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.drift_files = vec!["first.parquet".into(), "second.parquet".into()];
    state.drift_file_starts = vec![0, 2];
    state.query("select a where a > 2".to_string());
    let (current, _) = state.data_quality_scan(false);
    let (source, context) = state.data_quality_source_scan();
    let source =
        crate::data_quality::prepare_source_quality_scan(source, context.as_ref()).unwrap();
    assert_eq!(current.collect().unwrap().height(), 2);
    assert_eq!(source.collect().unwrap().height(), 4);
    assert_eq!(state.quality_source_file_count(), 2);
    let (raw, mapping) = state.data_quality_source_scan();
    let indexed = crate::data_quality::prepare_source_quality_scan(raw, mapping.as_ref()).unwrap();
    let first_file = crate::data_quality::apply_quality_scope(
        indexed,
        &crate::data_quality::QualityScope::SourceFiles(vec![1]),
        mapping.as_ref(),
    )
    .unwrap()
    .collect()
    .unwrap();
    assert_eq!(first_file.height(), 2);
    assert_eq!(
        first_file.column("a").unwrap().i32().unwrap().get(0),
        Some(1)
    );

    let evidence = state
        .quality_evidence_view(
            &crate::data_quality::QualityScope::WholeSource,
            col("a").eq(lit(1)),
        )
        .unwrap();
    assert_eq!(evidence.visible_lf().collect().unwrap().height(), 1);
    let bounded = state
        .quality_evidence_view(
            &crate::data_quality::QualityScope::FirstRows(1),
            col("a").eq(lit(4)),
        )
        .unwrap();
    assert_eq!(bounded.visible_lf().collect().unwrap().height(), 0);
    let file_evidence = state
        .quality_evidence_view(
            &crate::data_quality::QualityScope::SourceFiles(vec![1]),
            col("a").eq(lit(1)),
        )
        .unwrap();
    assert_eq!(file_evidence.visible_lf().collect().unwrap().height(), 1);
}

#[test]
fn source_time_roles_can_use_columns_hidden_by_current_query() {
    let lf = df!("a" => &[1i32, 2], "event" => &[20_000i32, 20_001])
        .unwrap()
        .lazy()
        .with_columns([col("event").cast(DataType::Date)]);
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.query("select a".to_string());
    assert!(
        state
            .quality_temporal_columns(&crate::data_quality::QualityScope::CurrentView)
            .is_empty()
    );
    assert_eq!(
        state.quality_temporal_columns(&crate::data_quality::QualityScope::WholeSource),
        vec!["event"]
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

    let df = state.df.as_ref().expect("display df present");
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

    let analysis_lf = state.lf.clone().select(state.binary_stub_exprs());
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
    assert_eq!(blob_stat.dtype, DataType::String);
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

#[test]
fn trailing_overflow_binary_column_is_truncated() {
    // Binary columns render as text (e.g. b"...") and should be truncated like strings —
    // this is the EDGAR `txt_bytes` case where a wide Binary column was wrongly dropped.
    let table = DataTable::default();
    let a = Series::new("a".into(), &[1i32, 2, 3]);
    let bin = Series::new(
        "wide_bytes".into(),
        &["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"],
    )
    .cast(&DataType::Binary)
    .unwrap();
    let df = DataFrame::new_infer_height(vec![a.into(), bin.into()]).unwrap();
    let area = Rect::new(0, 0, 8, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(
        shown, 2,
        "an overflowing binary column should be shown truncated"
    );
}

#[test]
fn tiny_remaining_width_drops_overflow_string_column() {
    // Even a string column should not render a useless 1-2 char sliver.
    let table = DataTable::default();
    let df = df!(
        "abcd" => &[1i32, 2, 3],
        "next" => &["yyyy", "yyyy", "yyyy"],
    )
    .unwrap();
    // "abcd" is 4 wide; used_width becomes 5, leaving only 1 (< MIN) for "next".
    let area = Rect::new(0, 0, 5, 4);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    assert_eq!(shown, 1, "a sub-minimal sliver should not be shown");
}

#[test]
fn more_columns_indicator_appears_and_tracks_scroll() {
    // 4 columns that can't all fit: a right-edge marker should signal off-screen columns,
    // and after scrolling right a left-edge marker should appear too.
    let mut state =
        DataTableState::new(create_large_test_lf(), None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.collect();

    let area = Rect::new(0, 0, 6, 4); // narrow: not all 4 columns fit
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let header = header_row_string(&buf, area);
    // The arrows come from the glyph set for the locale, which is ASCII on Windows CI.
    let g = crate::glyphs::get();
    assert!(
        header.contains(g.arrow_right),
        "expected right indicator, header: {header:?}"
    );
    assert!(
        !header.contains(g.arrow_left),
        "should not show left indicator at offset 0: {header:?}"
    );

    // Scroll right: now columns exist both left and right of the viewport.
    state.scroll_right();
    let mut buf2 = Buffer::empty(area);
    DataTable::default().render(area, &mut buf2, &mut state);
    let header2 = header_row_string(&buf2, area);
    assert!(
        header2.contains(g.arrow_left),
        "expected left indicator after scroll: {header2:?}"
    );
}

#[test]
fn sorted_column_header_carries_the_direction_mark() {
    // A sorted view must not look identical to an unsorted one: the sorted
    // column's header says so, and only that column's.
    let g = crate::glyphs::get();
    let table = DataTable::default().with_sort(vec!["age".to_string()], vec![false]);
    let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("age{}", g.sort_asc)),
        "the sorted column is marked: {header:?}"
    );
    assert!(
        !header.contains(&format!("name{}", g.sort_asc)),
        "the unsorted column is not: {header:?}"
    );
    assert!(
        !header.contains(g.sort_desc),
        "an ascending sort never shows the descending mark: {header:?}"
    );
}

#[test]
fn the_direction_mark_flips_with_the_sort() {
    let g = crate::glyphs::get();
    let table = DataTable::default().with_sort(vec!["age".to_string()], vec![true]);
    let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("age{}", g.sort_desc)),
        "a descending sort points down: {header:?}"
    );
    assert!(!header.contains(g.sort_asc), "and never up: {header:?}");
}

#[test]
fn every_column_of_a_multi_sort_is_marked() {
    // Marks only, no position numbers: the columns all run the same way.
    let g = crate::glyphs::get();
    let table = DataTable::default().with_sort(
        vec!["name".to_string(), "age".to_string()],
        vec![false, false],
    );
    let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    for name in ["name", "age"] {
        assert!(
            header.contains(&format!("{name}{}", g.sort_asc)),
            "{name} carries the mark: {header:?}"
        );
    }
}

#[test]
fn the_sort_mark_composes_with_the_drift_mark() {
    // A column can be sorted and drifting at once; the header carries both
    // marks and the width arithmetic counts both, so nothing is clipped.
    let g = crate::glyphs::get();
    let table = DataTable::default()
        .with_sort(vec!["age".to_string()], vec![false])
        .with_drift(
            Vec::new(),
            Arc::new(vec![crate::schema_union::DriftGroup {
                absent: vec!["age".into()],
                unread: Vec::new(),
            }]),
        );
    let df = df!("age" => &[41i32]).unwrap();
    let area = Rect::new(0, 0, 20, 3);
    let mut buf = Buffer::empty(area);
    let mut ts = TableState::default();
    table.render_dataframe(&df, area, &mut buf, &mut ts, false);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("age{}{}", g.drift_mark, g.sort_asc)),
        "footnote first, direction after: {header:?}"
    );
    assert!(
        row_string(&buf, area, 1).contains("41"),
        "the widened header does not clip the value"
    );
}

#[test]
fn the_header_mark_follows_the_state_sort_and_its_reverse() {
    // Through the stateful render: the marks come from the state being drawn,
    // so applying a sort shows them and `reverse` flips them, with no caller
    // wiring in between.
    let g = crate::glyphs::get();
    let mut state = DataTableState::new(create_test_lf(), None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.sort(vec!["a".to_string()], true);

    let area = Rect::new(0, 0, 20, 5);
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let header = header_row_string(&buf, area);
    assert!(
        header.contains(&format!("a{}", g.sort_asc)),
        "sorted ascending: {header:?}"
    );

    state.reverse();
    let mut buf2 = Buffer::empty(area);
    DataTable::default().render(area, &mut buf2, &mut state);
    let header2 = header_row_string(&buf2, area);
    assert!(
        header2.contains(&format!("a{}", g.sort_desc)),
        "reversed: {header2:?}"
    );
    assert!(
        !header2.contains(g.sort_asc),
        "the old direction is gone: {header2:?}"
    );
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

/// A click finds the cell drawn under it, at 80×24 and on a wide screen: each
/// column where its heading is, frozen or scrolling, and each row where its cells
/// are, with row numbers on and the view scrolled down.
#[test]
fn a_click_finds_the_cell_drawn_under_it() {
    for (width, height) in [(80, 24), (200, 50)] {
        let n = 400;
        let mut columns = vec![Column::new("id".into(), (0..n).collect::<Vec<i64>>())];
        for c in 0..30 {
            columns.push(Column::new(
                format!("col_{c:02}").as_str().into(),
                (0..n).map(|i| format!("v{i}_{c}")).collect::<Vec<_>>(),
            ));
        }
        let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_locked_columns(1);
        state.toggle_row_numbers();
        let area = Rect::new(0, 0, width, height);
        let render = |state: &mut DataTableState| {
            let mut buf = Buffer::empty(area);
            DataTable::default().render(area, &mut buf, state);
            buf
        };
        // The first frame sets how many rows fit; the rows are read for it.
        render(&mut state);
        state.collect();
        for scrolled in [false, true] {
            if scrolled {
                state.page_down();
                state.collect();
            }
            let buf = render(&mut state);
            let drawn = state.drawn.clone().expect("the table was drawn");
            let header = row_string(&buf, area, 0);
            assert!(drawn.columns.len() > 3, "{width}x{height}: {header:?}");
            assert_eq!(drawn.columns[0].2, "id", "the frozen column first");
            for (_, _, name) in &drawn.columns {
                // In cells, not bytes: the frozen separator is a wide glyph.
                let at = header.find(name.as_str()).expect("heading drawn");
                let from = header[..at].chars().count() as u16;
                for x in [from, from + name.len() as u16 - 1] {
                    let hit = state.drawn_cell(x, 0).expect("on the table");
                    assert_eq!(hit.row, None, "the header is no row");
                    assert_eq!(hit.column.as_deref(), Some(name.as_str()), "at {x}");
                }
            }
            // The rail and the row numbers are no column, but are the row.
            let y = drawn.header + 5;
            assert_eq!(
                state.drawn_cell(0, y),
                Some(CellHit {
                    row: Some(5),
                    column: None
                })
            );
            // A cell: the row and column whose value is drawn there.
            let (from, to, name) = drawn.columns[2].clone();
            let c: usize = name["col_".len()..].parse().unwrap();
            let hit = state.drawn_cell(from, y).expect("a cell");
            let row = drawn.start_row + 5;
            let text: String = (from..to).map(|x| buf[(x, y)].symbol()).collect();
            assert_eq!(text.trim(), format!("v{row}_{c}"), "{width}x{height}");
            state.point_at(&hit);
            assert_eq!(state.table_state.selected(), Some(5));
            assert_eq!(state.current_column(), Some(name.as_str()));
            // Off the table's right or bottom edge is nothing.
            assert_eq!(state.drawn_cell(width, y), None);
            assert_eq!(state.drawn_cell(0, height), None);
        }
    }
}

/// The column cursor at 80×24: its header and cells take the column tint, the
/// current cell (the cursor's row and column) the cell tint, and the rest of the
/// current row keeps the row tint. Frozen columns take it the same way.
#[test]
fn the_column_cursor_tints_its_header_and_cells() {
    let (mut state, area) = cursor_fixture();
    let row_tint = Color::Rgb(0x28, 0x34, 0x57);
    let column_tint = Color::Rgb(0x29, 0x2e, 0x42);
    let cell_tint = Color::Rgb(0x3b, 0x42, 0x61);
    let table = || {
        DataTable {
            selection_style: Style::default().bg(row_tint),
            ..DataTable::default()
        }
        .with_cursor_styles(
            crate::config::column_cursor_style(Some(column_tint)),
            crate::config::cell_cursor_style(Some(cell_tint)),
        )
    };
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::Right);
    assert_eq!(state.current_column(), Some("city"));
    let mut buf = Buffer::empty(area);
    table().render(area, &mut buf, &mut state);
    let city = column_span(&buf, area, "city");
    let name = column_span(&buf, area, "name");
    for x in city.clone() {
        assert_eq!(buf[(x, 0)].bg, cell_tint, "the header, at {x}");
        assert!(buf[(x, 0)].modifier.contains(Modifier::BOLD));
        assert_eq!(buf[(x, 1)].bg, cell_tint, "the current cell, at {x}");
        for y in 2..area.height {
            assert_eq!(buf[(x, y)].bg, column_tint, "the column, at {x},{y}");
        }
    }
    for x in name.clone() {
        assert_eq!(buf[(x, 1)].bg, row_tint, "the rest of the row");
        assert_ne!(buf[(x, 0)].bg, cell_tint, "another header");
        assert_ne!(buf[(x, 2)].bg, column_tint, "another column");
    }

    // Frozen: the same marks, left of the separator.
    state.move_cursor(CursorMove::First);
    assert_eq!(state.current_column(), Some("id"));
    let mut buf = Buffer::empty(area);
    table().render(area, &mut buf, &mut state);
    let id = column_span(&buf, area, "id");
    for x in id {
        assert_eq!(buf[(x, 0)].bg, cell_tint);
        assert_eq!(buf[(x, 1)].bg, cell_tint);
        assert_eq!(buf[(x, 5)].bg, column_tint);
    }
    for x in city {
        assert_ne!(buf[(x, 0)].bg, cell_tint, "city lets go of it");
    }
}

/// Where the tints would not show (16 colors, `NO_COLOR`), the header and the
/// current cell are reversed, so the cursor is still on screen, and nothing else is.
#[test]
fn the_column_cursor_shows_without_its_tints() {
    let (mut state, area) = cursor_fixture();
    state.move_cursor(CursorMove::Right);
    for tint in [Color::Black, Color::White, Color::Reset] {
        let mut buf = Buffer::empty(area);
        DataTable::default()
            .with_cursor_styles(
                crate::config::column_cursor_style(Some(tint)),
                crate::config::cell_cursor_style(Some(tint)),
            )
            .render(area, &mut buf, &mut state);
        let name = column_span(&buf, area, "name");
        let reversed = |x, y| buf[(x, y)].modifier.contains(Modifier::REVERSED);
        for x in name {
            assert!(reversed(x, 0), "{tint:?}: the header");
            assert!(reversed(x, 1), "{tint:?}: the current cell");
            assert!(!reversed(x, 2), "{tint:?}: not the rest of the column");
        }
        let city = column_span(&buf, area, "city");
        assert!(!reversed(city.start, 0) && !reversed(city.start, 1));
    }
}

/// Under a reversed row, the current cell is drawn upright, tinted or not, so it
/// stands out from the row; the header keeps its own mark.
#[test]
fn the_current_cell_stands_out_of_a_reversed_row() {
    let (mut state, area) = cursor_fixture();
    state.move_cursor(CursorMove::Right);
    for tint in [Color::Rgb(0x3b, 0x42, 0x61), Color::Black, Color::Reset] {
        let mut buf = Buffer::empty(area);
        DataTable {
            selection_style: Style::default().add_modifier(Modifier::REVERSED),
            ..DataTable::default()
        }
        .with_cursor_styles(
            crate::config::column_cursor_style(Some(tint)),
            crate::config::cell_cursor_style(Some(tint)),
        )
        .render(area, &mut buf, &mut state);
        let reversed = |x, y| buf[(x, y)].modifier.contains(Modifier::REVERSED);
        for x in column_span(&buf, area, "name") {
            assert!(!reversed(x, 1), "{tint:?}: the current cell is upright");
            assert!(buf[(x, 1)].modifier.contains(Modifier::BOLD));
        }
        let city = column_span(&buf, area, "city");
        assert!(reversed(city.start, 1), "{tint:?}: the rest of the row");
    }
}

/// The cursor follows its column by name when the columns are reordered or
/// frozen, and a column hidden from under it hands it to the one in its place.
#[test]
fn the_column_cursor_follows_its_column_by_name() {
    let (mut state, area) = cursor_fixture();
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::Right);
    assert_eq!(state.current_column(), Some("city"));
    let order = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    state.set_column_order(order(&["city", "id", "name", "amount"]));
    assert_eq!(state.current_column(), Some("city"));
    assert_eq!(state.current_column_index(), Some(0));
    state.set_locked_columns(0);
    state.set_column_order(order(&["id", "name", "city", "amount"]));
    state.set_locked_columns(3);
    assert_eq!(state.current_column(), Some("city"), "frozen now");
    // Hidden: the column now in its place takes it.
    state.set_locked_columns(0);
    state.set_column_order(order(&["id", "name", "amount"]));
    assert_eq!(state.current_column(), Some("amount"));
    // Hidden at the end: the last column.
    state.set_column_order(order(&["id", "name"]));
    assert_eq!(state.current_column(), Some("name"));
    state.set_column_order(Vec::new());
    assert_eq!(state.current_column(), None);
    DataTable::default().render(area, &mut Buffer::empty(area), &mut state);
}

/// `h` `l` cross from the frozen columns to the scrolling ones and back in the
/// shown order; the view moves only when the cursor would leave it, and a page
/// puts the cursor on the new page's first column.
#[test]
fn the_column_cursor_scrolls_only_at_the_edges() {
    let n = 5;
    let names: Vec<String> = (0..40).map(|i| format!("column_{i:02}")).collect();
    let columns: Vec<Column> = names
        .iter()
        .map(|name| Column::new(name.as_str().into(), (0..n).collect::<Vec<i64>>()))
        .collect();
    let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 5;
    state.set_locked_columns(2);
    let area = Rect::new(0, 0, 80, 8);
    let draw = |state: &mut DataTableState| {
        DataTable::default().render(area, &mut Buffer::empty(area), state);
        state.columns_on_screen().unwrap()
    };
    let start = draw(&mut state);
    assert_eq!((start.first, start.cursor), (3, 1));
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::Right);
    let on = draw(&mut state);
    assert_eq!((on.first, on.cursor), (3, 3), "into the scrolling side");
    // Walk to the right edge: nothing scrolls until the cursor would leave (a
    // column cut at the edge counts as leaving), then just enough.
    let mut before = on;
    let past = loop {
        state.move_cursor(CursorMove::Right);
        let now = draw(&mut state);
        assert_eq!(now.cursor, before.cursor + 1);
        if now.first != before.first {
            break now;
        }
        before = now;
    };
    assert!(past.first > 3 && past.cursor <= past.last, "{past:?}");
    assert!(past.cursor >= start.last, "not before the edge: {past:?}");
    assert!(past.first <= before.last, "no column skipped: {past:?}");
    // Back to the left edge, and one more scrolls back a column.
    for _ in past.first..past.cursor {
        state.move_cursor(CursorMove::Left);
    }
    assert_eq!(draw(&mut state).first, past.first);
    state.move_cursor(CursorMove::Left);
    assert_eq!(draw(&mut state).first, past.first - 1);
    // Pages: the cursor starts the new page; on the last, it takes the last column.
    state.move_cursor(CursorMove::PageRight);
    let page = draw(&mut state);
    assert_eq!(page.cursor, page.first);
    state.move_cursor(CursorMove::Last);
    let last = draw(&mut state);
    assert_eq!((last.cursor, last.last), (40, 40));
    state.move_cursor(CursorMove::PageRight);
    assert_eq!(draw(&mut state).cursor, 40);
    // From a frozen column, the next is the first scrolling column: the view
    // goes back to it.
    state.go_to_column("column_01");
    assert_eq!(draw(&mut state).first, last.first, "frozen: on screen");
    state.move_cursor(CursorMove::Right);
    let back = draw(&mut state);
    assert_eq!((back.first, back.cursor), (3, 3));
    // `[` on the first page: its first column, then the first of all.
    state.move_cursor(CursorMove::Right);
    state.move_cursor(CursorMove::PageLeft);
    assert_eq!(draw(&mut state).cursor, 3);
    state.move_cursor(CursorMove::PageLeft);
    assert_eq!(draw(&mut state).cursor, 1);
    state.move_cursor(CursorMove::Left);
    assert_eq!(draw(&mut state).cursor, 1, "nothing left of the first");
}

/// Every cell right of the frozen separator sits one cell off it, as the cells
/// left of it do, and the gap takes its row's tint. A right-aligned number as
/// wide as its column, a negative one most often, used to touch the line:
/// `│-9.930889` (#386).
#[test]
fn the_frozen_separator_has_a_gap_on_both_sides() {
    let lf = df!(
        "carrier" => &["AA", "UA", "9E"],
        "delay" => &[-9.930889f64, 3.5, 12.25],
        "name" => &["American", "United", "Endeavor"],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.set_locked_columns(1);
    state.table_state.select(Some(0));

    let area = Rect::new(0, 0, 40, 6);
    let mut buf = Buffer::empty(area);
    let table = DataTable {
        header_bg: Color::Indexed(238),
        alternate_row_bg: Some(Color::Indexed(236)),
        selection_style: Style::default().bg(Color::Indexed(24)),
        ..DataTable::default()
    };
    table.render(area, &mut buf, &mut state);

    let rows: Vec<String> = (0..area.height)
        .map(|y| row_string(&buf, area, y))
        .collect();
    assert!(
        rows[1].contains(&format!("{} -9.930889", crate::glyphs::get().rule)),
        "{rows:#?}"
    );
    let rule = crate::glyphs::get().rule;
    let sep = (0..area.width)
        .find(|&x| buf[(x, 0)].symbol() == rule)
        .expect("a separator");
    for y in 0..area.height {
        let row = &rows[y as usize];
        assert_eq!(buf[(sep - 1, y)].symbol(), " ", "row {y}: {row:?}");
        assert_eq!(buf[(sep + 1, y)].symbol(), " ", "row {y}: {row:?}");
        assert_eq!(
            buf[(sep + 1, y)].bg,
            buf[(sep + 2, y)].bg,
            "row {y}'s gap takes the row's tint: {row:?}"
        );
    }
    // The header, the highlight and the stripe, not only unstyled rows.
    assert_eq!(buf[(sep + 1, 0)].bg, Color::Indexed(238));
    assert_eq!(buf[(sep + 1, 1)].bg, Color::Indexed(24));
    assert_eq!(buf[(sep + 1, 2)].bg, Color::Indexed(236));
}

/// The frozen separator runs down the header and the rows, and stops under the
/// last: a grouped view of seven rows had it running down the empty screen.
#[test]
fn the_frozen_separator_stops_at_the_last_row() {
    let lf = df!(
        "carrier" => &["AA", "UA", "9E"],
        "delay" => &[-9.9f64, 3.5, 12.25],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.set_locked_columns(1);
    state.table_state.select(Some(0));
    let area = Rect::new(0, 0, 30, 10);
    let mut buf = Buffer::empty(area);
    DataTable::default().render(area, &mut buf, &mut state);
    let rule = crate::glyphs::get().rule;
    let sep = (0..area.width)
        .find(|&x| buf[(x, 0)].symbol() == rule)
        .expect("a separator");
    let ruled: Vec<u16> = (0..area.height)
        .filter(|&y| buf[(sep, y)].symbol() == rule)
        .collect();
    let last = ruled.last().copied().unwrap();
    assert_eq!(
        ruled,
        (0..=last).collect::<Vec<_>>(),
        "unbroken to the last row"
    );
    let header = DataTable::default().header_height();
    assert_eq!(last, header + 2, "under the third row, no further");
}

/// A frozen column whose type is wider than its name and values still gets its
/// whole width and the gap before the separator. The width pass left the type row
/// out, so `id` over `i64` ran onto the line (`i64│`) and a one-letter string
/// column did not fit at all.
#[test]
fn a_frozen_column_is_as_wide_as_its_type() {
    let lf = df!(
        "id" => &[1i64, 2],
        "k" => &["x", "y"],
        "v" => &[-3.5f64, 4.25],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 2;
    state.set_locked_columns(2);
    let area = Rect::new(0, 0, 30, 4);
    let mut buf = Buffer::empty(area);
    DataTable {
        dtype_row: true,
        ..DataTable::default()
    }
    .render(area, &mut buf, &mut state);

    let rows: Vec<String> = (0..area.height)
        .map(|y| row_string(&buf, area, y))
        .collect();
    let rule = crate::glyphs::get().rule;
    assert!(rows[0].contains(&format!(" id k   {rule}")), "{rows:#?}");
    assert!(rows[1].contains(&format!("i64 str {rule}")), "{rows:#?}");
    assert!(rows[2].contains(&format!("  1 x   {rule}")), "{rows:#?}");
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

/// A list cell reads as it did when the buffer held lists as text, and the type
/// row names the list once the rows have landed, not `str`.
#[test]
fn a_list_column_draws_its_items_under_its_list_type() {
    let mut state = list_state();
    let area = Rect::new(0, 0, 80, 4);
    let mut buf = Buffer::empty(area);
    DataTable {
        dtype_row: true,
        ..DataTable::default()
    }
    .render(area, &mut buf, &mut state);
    let rows: Vec<String> = (0..area.height)
        .map(|y| row_string(&buf, area, y))
        .collect();
    assert!(rows[1].contains("list[str]"), "{rows:#?}");
    assert!(!rows[1].contains(" str "), "{rows:#?}");
    assert!(rows[2].contains("[a, b]"), "{rows:#?}");
    // The column's width cap cuts the rest; `exact` tests the whole preview.
    assert!(
        rows[3].contains("[t0, t1, t2, t3, t4, t5, t6, t7"),
        "{rows:#?}"
    );
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
    assert!(is_list(state.locked_df.as_ref().unwrap(), "tags"));
    assert!(is_list(state.df.as_ref().unwrap(), "more"));
}

/// A page over columns not drawn yet waits for the draw, which measures them from
/// the rows on hand and lands it: `Last` ends the page with the last column whole,
/// and the one before that would not have fitted. Nothing moves before then.
#[test]
fn a_page_over_columns_not_drawn_lands_at_the_draw() {
    let names: Vec<String> = (0..30).map(|i| format!("col{i:02}")).collect();
    let columns: Vec<Column> = names
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let value = "x".repeat(3 + i % 5);
            Series::new(name.as_str().into(), vec![value; 3]).into()
        })
        .collect();
    let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 3;
    state.collect();
    let area = Rect::new(0, 0, 50, 4);
    let draw = |state: &mut DataTableState| {
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, state);
        row_string(&buf, area, 0)
    };
    draw(&mut state);
    assert!(state.drawn_width("col29").is_none(), "not drawn yet");

    state.scroll_columns(ColumnMove::Last);
    assert_eq!(state.termcol_index, 0, "nothing moves before the draw");
    let header = draw(&mut state);
    assert!(header.trim_end().ends_with("col29"), "{header}");
    let start = state.termcol_index;
    assert!(start > 0);
    // The column before the page would not have fitted beside it.
    let room = state.scroll_room.unwrap();
    let used: u16 = names[start - 1..]
        .iter()
        .map(|n| state.shown_width(n).unwrap() + room.padding)
        .sum::<u16>()
        - room.padding;
    assert!(used > room.width, "{used} in {}", room.width);

    // Back, which lands at the draw again; then forward over columns drawn now,
    // which needs none.
    state.scroll_columns(ColumnMove::PageLeft);
    draw(&mut state);
    let back = state.termcol_index;
    assert!(back < start);
    state.scroll_columns(ColumnMove::PageRight);
    assert_eq!(state.termcol_index, start);
    state.scroll_columns(ColumnMove::First);
    assert_eq!(state.termcol_index, 0);
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

/// The hidden-columns count goes in the blank run after the last column, or not
/// at all: at no width does it cover the type under a heading. The separator's gap
/// took the one cell of slack that used to keep `+2 >` clear of `str` at 60
/// columns, and the count then wrote over the `r`.
#[test]
fn the_hidden_count_never_covers_a_type() {
    let lf = df!(
        "k" => &["x"],
        "origin" => &["JFK"],
        "dest" => &["LAX"],
        "tail" => &["N1"],
        "name" => &["Endeavor"],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 1;
    state.set_locked_columns(1);
    let table = || DataTable {
        dtype_row: true,
        ..DataTable::default()
    };
    assert_eq!(table().header_height(), 2, "the count goes on the type row");
    for width in 8..=40 {
        let area = Rect::new(0, 0, width, 3);
        let mut buf = Buffer::empty(area);
        table().render(area, &mut buf, &mut state);
        let names = row_string(&buf, area, 0);
        let types = row_string(&buf, area, 1);
        // Every heading shown whole has its whole type under it; these are all
        // strings, so each starts where its name does.
        for name in ["origin", "dest", "tail", "name"] {
            if let Some(at) = names.find(&format!(" {name}")) {
                let x = names[..at].chars().count() + 1;
                let under: String = types.chars().skip(x).take(3).collect();
                assert_eq!(under, "str", "width {width}:\n{names}\n{types}");
            }
        }
    }
}

/// The gap is paid for in the width budget: at every width, the columns right of
/// the separator are whole or absent. Left out, the last column that fits exactly
/// comes out one cell short, and a number cut short reads as a different number.
/// The one exception is a first column with no room for its value at all, which
/// shows a preview behind the clip marker rather than nothing.
#[test]
fn the_separator_gap_never_cuts_a_number_short() {
    let values = ["-987", "654", "-32", "10"];
    let lf = df!(
        "k" => &["x"],
        "a" => &[-987i64],
        "b" => &[654i64],
        "c" => &[-32i64],
        "d" => &[10i64],
    )
    .unwrap()
    .lazy();
    let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
    state.visible_rows = 1;
    state.set_locked_columns(1);
    let data_row = DataTable::default().header_height();
    for width in 8..=30 {
        let area = Rect::new(0, 0, width, data_row + 1);
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, &mut state);
        let row = row_string(&buf, area, data_row);
        let g = crate::glyphs::get();
        let (_, scrolled) = row.split_once(g.rule).expect("a separator");
        for (i, token) in scrolled.split_whitespace().enumerate() {
            let previewed = i == 0 && token.ends_with(g.ellipsis);
            assert!(
                values.contains(&token) || previewed,
                "width {width}: {token:?} is cut short in {row:?}"
            );
        }
    }
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

/// A heading wider than the table is clipped, marked, over values that fit whole:
/// it never leaves the table blank, at any width, in either glyph set, with or
/// without the type row. It used to drop the column, and with it everything
/// after, leaving the rail and an off-screen hint over nothing.
#[test]
fn a_long_header_never_blanks_the_table() {
    let name = format!("numeric_header_{}", "x".repeat(90));
    let df = DataFrame::new_infer_height(vec![
        Series::new(name.as_str().into(), &[1i64, 22, 333]).into(),
        Series::new("tail".into(), &["t1", "t2", "t3"]).into(),
    ])
    .unwrap();
    for g in glyph_sets() {
        for dtype_row in [false, true] {
            for width in [12u16, 20, 60, 80, 120] {
                let table = || DataTable {
                    glyphs: g,
                    dtype_row,
                    ..DataTable::default()
                };
                let header_h = usize::from(table().header_height());
                let mut state = state_of(&df, 3);
                let rows = draw(table(), &mut state, width, header_h as u16 + 3);
                let ctx = format!(
                    "{} glyphs, type row {dtype_row}, width {width}:\n{}",
                    set_name(g),
                    rows.join("\n")
                );
                assert!(rows[0].contains("num"), "{ctx}");
                for (i, value) in ["1", "22", "333"].iter().enumerate() {
                    assert!(
                        rows[header_h + i].split_whitespace().any(|t| t == *value),
                        "{value} is whole on its row: {ctx}"
                    );
                }
                if dtype_row && width >= 20 {
                    assert!(rows[1].contains("i64"), "{ctx}");
                }
                // Wider than any cap on automatic widths: always clipped, and from
                // 60 columns on, with the column after it beside it.
                assert!(rows[0].contains(g.ellipsis), "{ctx}");
                if width >= 60 {
                    assert!(rows[0].contains("tail"), "{ctx}");
                }
            }
        }
    }
}

/// A clipped heading gives way to its marks: the name is cut, never the sort
/// direction, which is state.
#[test]
fn a_clipped_heading_keeps_its_sort_mark() {
    let name = format!("numeric_header_{}", "x".repeat(90));
    let df = DataFrame::new_infer_height(vec![Series::new(name.as_str().into(), &[1i64]).into()])
        .unwrap();
    for g in glyph_sets() {
        let table = DataTable {
            glyphs: g,
            ..DataTable::default()
        }
        .with_sort(vec![name.clone()], vec![true]);
        let area = Rect::new(0, 0, 30, 2);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false);
        let header = header_row_string(&buf, area);
        assert!(
            header
                .trim_end()
                .ends_with(&format!("{}{}", g.ellipsis, g.sort_desc)),
            "{}: {header:?}",
            set_name(g)
        );
    }
}

/// A heading whose drawn width is not its string's width (`لا` draws two cells,
/// a halfwidth sound mark one) is drawn whole over right-aligned numbers, with its
/// sort mark after it. ratatui's own alignment pushed the last letter off the cell,
/// and the mark landed on it.
#[test]
fn a_heading_is_placed_by_the_cells_it_draws() {
    for name in ["الاسم", "ｶﾞｷﾞ"] {
        let df = DataFrame::new_infer_height(vec![
            Series::new(name.into(), &[1i64]).into(),
            Series::new("tail".into(), &["x"]).into(),
        ])
        .unwrap();
        let table = DataTable::default().with_sort(vec![name.to_string()], vec![false]);
        let mark = table.glyphs.sort_asc;
        let area = Rect::new(0, 0, 30, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false);
        let rows: Vec<String> = (0..3).map(|y| drawn_from(&buf, y, 0)).collect();
        assert!(rows[0].starts_with(&format!("{name}{mark}")), "{rows:#?}");
        let width = crate::glyphs::cell_width(name) + 1;
        assert!(
            rows[1].starts_with(&format!("{:>width$} x", "1")),
            "{rows:#?}"
        );
    }
}

/// A number too wide for the whole table shows its leading digits behind the clip
/// marker: something rather than nothing, and never its trailing digits passing
/// for a whole number, which is what ratatui's own cut of a right-aligned value
/// would show.
#[test]
fn a_number_wider_than_the_table_is_a_marked_preview() {
    let full = "-1234567890123456789";
    let df = df!("n" => &[-1234567890123456789i64]).unwrap();
    for g in glyph_sets() {
        for width in 3u16..=24 {
            let mut state = state_of(&df, 1);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 2);
            let shown: String = rows[1].chars().skip(1).collect::<String>();
            let shown = shown.trim();
            let ctx = format!("{} glyphs, width {width}: {rows:?}", set_name(g));
            if usize::from(width) > full.len() {
                assert_eq!(shown, full, "{ctx}");
            } else if g.ellipsis.starts_with(shown) {
                // Room for no more than the marker.
                assert!(!shown.is_empty(), "{ctx}");
            } else {
                let kept = shown
                    .strip_suffix(g.ellipsis)
                    .unwrap_or_else(|| panic!("a clipped number carries the marker: {ctx}"));
                assert!(full.starts_with(kept), "{ctx}");
            }
        }
    }
}

/// Wide characters are measured in cells: a column of four CJK characters is
/// eight cells wide, and shows whole beside the columns after it while there is
/// room. Counted in characters, it was given four and clipped with space to spare.
#[test]
fn wide_characters_are_measured_in_cells() {
    let values = ["東京大阪", "京都横浜", "名古屋市"];
    let df = df!(
        "a" => &values,
        "b" => &[1i64, 2, 3],
        "tail" => &["x", "y", "z"],
    )
    .unwrap();
    for g in glyph_sets() {
        for width in [16u16, 30, 80] {
            let mut state = state_of(&df, 3);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 4);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            assert!(rows[0].contains("tail"), "{ctx}");
            for (i, value) in values.iter().enumerate() {
                assert!(rows[1 + i].contains(value), "{ctx}");
            }
        }
    }
}

/// A value cut where it meets the edge keeps whole graphemes and gains the clip
/// marker: no half of a wide character, no accent split from its letter, no
/// joined emoji broken apart, at any width, in either glyph set.
#[test]
fn a_clipped_cell_keeps_whole_graphemes() {
    let values = [
        "東京大阪名古屋横浜",
        "e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}",
        "👩\u{200d}👩\u{200d}👧👍🏽🇯🇵 and more",
        "plain text that runs on",
    ];
    let df = df!("id" => &[1i64, 2, 3, 4], "text" => &values).unwrap();
    for g in glyph_sets() {
        for width in 4u16..=30 {
            let mut state = state_of(&df, 4);
            let area = Rect::new(0, 0, width, 5);
            let mut buf = Buffer::empty(area);
            DataTable {
                glyphs: g,
                ..DataTable::default()
            }
            .render(area, &mut buf, &mut state);
            // The rail, `id` two cells wide, and one cell of padding.
            let text_x = 4;
            if text_x >= width {
                continue;
            }
            for (i, value) in values.iter().enumerate() {
                let y = 1 + i as u16;
                let shown = drawn_from(&buf, y, text_x);
                let shown = shown.trim_end();
                let ctx = format!("{} glyphs, width {width}, row {i}: {shown:?}", set_name(g));
                if shown.is_empty() || shown == *value {
                    continue;
                }
                let kept = shown
                    .strip_suffix(g.ellipsis)
                    .unwrap_or_else(|| panic!("a clipped value is marked: {ctx}"));
                let span = Span::raw(*value);
                let mut whole = String::new();
                for grapheme in span.styled_graphemes(Style::default()) {
                    if whole.len() >= kept.len() {
                        break;
                    }
                    whole.push_str(grapheme.symbol);
                }
                assert_eq!(whole, kept, "{ctx}");
                // And each cell holds a whole grapheme of the value or the marker.
                for x in text_x..width {
                    let symbol = buf[(x, y)].symbol();
                    assert!(
                        symbol == " "
                            || g.ellipsis.contains(symbol)
                            || span
                                .styled_graphemes(Style::default())
                                .any(|gr| gr.symbol == symbol),
                        "cell {x} holds {symbol:?}: {ctx}"
                    );
                }
            }
        }
    }
}

/// A frozen text column wider than the window is clipped, marked and still
/// frozen, with the column after it beside it. It used to vanish, leaving only
/// the column after it.
#[test]
fn a_frozen_long_text_is_clipped_not_dropped() {
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let df = df!("url" => &[url.as_str(), "short"], "tail" => &[7i64, 8]).unwrap();
    for g in glyph_sets() {
        for width in [40u16, 60, 80, 120] {
            let mut state = state_of(&df, 2);
            state.set_locked_columns(1);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 3);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            let (frozen, scrolled) = rows[0].split_once(g.rule).expect(&ctx);
            assert!(frozen.contains("url"), "{ctx}");
            assert!(scrolled.contains("tail"), "{ctx}");
            let (frozen, scrolled) = rows[1].split_once(g.rule).expect(&ctx);
            assert!(frozen.contains("https://exa"), "{ctx}");
            assert!(frozen.trim_end().ends_with(g.ellipsis), "{ctx}");
            assert!(scrolled.split_whitespace().any(|t| t == "7"), "{ctx}");
            assert_eq!(state.frozen_shown(), 1, "{ctx}");
        }
    }
}

/// The frozen columns are measured on the rows on screen, like the rest. They
/// were measured on the head of the buffer, which is not the page on screen once
/// the view has moved into it, so a longer value on screen was cut short.
#[test]
fn frozen_columns_are_measured_on_the_rows_on_screen() {
    let long = "a much longer frozen value";
    let names: Vec<String> = (0..400)
        .map(|i| {
            if i == 302 {
                long.to_string()
            } else {
                format!("n{i}")
            }
        })
        .collect();
    let df = df!("name" => names, "v" => (0..400i64).collect::<Vec<_>>()).unwrap();
    let mut state = state_of(&df, 5);
    state.set_locked_columns(1);
    state.scroll_to(300);
    state.collect();
    assert!(
        state.buffered_start_row < state.start_row,
        "the page is not the head of the buffer: {} vs {}",
        state.buffered_start_row,
        state.start_row
    );
    let rows = draw(DataTable::default(), &mut state, 80, 6);
    assert!(
        rows.iter().any(|row| row.contains(long)),
        "the value on screen is whole: {rows:#?}"
    );
}

/// Frozen columns are spaced like scrolling ones: the configured padding on both
/// sides of the separator, at every setting. The frozen side used a hardcoded
/// single space.
#[test]
fn frozen_and_scrolling_columns_share_the_padding() {
    let df = df!(
        "id" => &[1i64, 2],
        "k" => &["x", "y"],
        "v" => &[3i64, 4],
        "w" => &[5i64, 6],
    )
    .unwrap();
    for padding in [0u16, 1, 2, 3] {
        let mut state = state_of(&df, 2);
        state.set_locked_columns(2);
        let table = DataTable {
            table_cell_padding: padding,
            ..DataTable::default()
        };
        let rows = draw(table, &mut state, 40, 3);
        let gap = " ".repeat(usize::from(padding));
        let rule = crate::glyphs::get().rule;
        assert!(
            rows[0].starts_with(&format!(" id{gap}k {rule} v{gap}w")),
            "padding {padding}: {rows:#?}"
        );
        assert!(
            rows[1].contains(&format!(" 1{gap}x {rule} 3{gap}5")),
            "padding {padding}: {rows:#?}"
        );
    }
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

/// Frozen columns that cannot all fit beside a usable scrolling column: as many
/// as fit stay frozen, the broken rule says some had to scroll, those lead the
/// scrolling side, every column stays reachable, and the request stands, so a
/// wider window freezes them all again.
#[test]
fn a_frozen_prefix_too_wide_scrolls_until_there_is_room() {
    let df = phonetic_frame();
    for g in glyph_sets() {
        for row_numbers in [false, true] {
            let mut state = state_of(&df, 2);
            state.row_numbers = row_numbers;
            state.set_locked_columns(4);
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table(), &mut state, 60, 3);
            let ctx = format!(
                "{} glyphs, row numbers {row_numbers}: {rows:#?}",
                set_name(g)
            );
            assert_eq!(state.locked_columns_count(), 4, "{ctx}");
            let shown = state.frozen_shown();
            assert!((1..4).contains(&shown), "{shown} frozen: {ctx}");
            let (frozen, scrolled) = rows[0].split_once(g.rule_broken).expect(&ctx);
            assert!(!rows[0].contains(g.rule), "{ctx}");
            assert!(frozen.contains(PHONETIC[0]), "{ctx}");
            assert!(
                scrolled.trim_start().starts_with(PHONETIC[shown]),
                "the first column left out leads the scrolling side: {ctx}"
            );

            let reached = names_reached(&mut state, g, 60);
            assert_eq!(reached.len(), PHONETIC.len(), "{reached:?}: {ctx}");

            let rows = draw(table(), &mut state, 160, 3);
            assert_eq!(state.frozen_shown(), 4, "{rows:#?}");
            let (frozen, _) = rows[0].split_once(g.rule).expect("the plain rule");
            for name in &PHONETIC[..4] {
                assert!(frozen.contains(name), "{rows:#?}");
            }
        }
    }
}

/// A rollback puts back the frozen fit its columns were sliced for: a wider
/// layout since then must re-slice them, or the columns that had to scroll show
/// twice, frozen and scrolling.
#[test]
fn a_rollback_keeps_the_frozen_fit_its_columns_were_sliced_for() {
    let df = phonetic_frame();
    let mut state = state_of(&df, 2);
    state.set_locked_columns(4);
    draw(DataTable::default(), &mut state, 60, 3);
    assert!(state.frozen_shown() < 4);
    let saved = state.rollback_point();
    draw(DataTable::default(), &mut state, 200, 3);
    assert_eq!(state.frozen_shown(), 4);
    state.roll_back(saved);
    let rows = draw(DataTable::default(), &mut state, 200, 3);
    for name in PHONETIC {
        assert_eq!(rows[0].matches(name).count(), 1, "{name}: {rows:#?}");
    }
}

/// With every column frozen there is nothing to scroll, until the window is too
/// narrow for them all: then the ones that do not fit scroll, and each is still
/// reachable.
#[test]
fn with_every_column_frozen_each_is_still_reachable() {
    let df = phonetic_frame();
    for g in glyph_sets() {
        let mut state = state_of(&df, 2);
        state.set_locked_columns(PHONETIC.len());
        let table = || DataTable {
            glyphs: g,
            ..DataTable::default()
        };
        let rows = draw(table(), &mut state, 200, 3);
        assert_eq!(state.frozen_shown(), PHONETIC.len(), "{rows:#?}");
        assert!(rows[0].contains(g.rule), "{rows:#?}");
        assert!(!rows[0].contains(g.arrow_right), "{rows:#?}");

        let rows = draw(table(), &mut state, 60, 3);
        assert!(state.frozen_shown() < PHONETIC.len(), "{rows:#?}");
        assert!(rows[0].contains(g.rule_broken), "{rows:#?}");
        let reached = names_reached(&mut state, g, 60);
        assert_eq!(reached.len(), PHONETIC.len(), "{reached:?}");
    }
}

/// The page from the issue's reproduction: one description runs to a long URL.
/// That page still shows its rows, with the description clipped and marked
/// rather than any column vanishing for it.
#[test]
fn a_page_with_one_long_value_is_not_blank() {
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let n = 80usize;
    let df = df!(
        "id" => (0..n as i64).collect::<Vec<_>>(),
        "description" => (0..n)
            .map(|i| if i == 24 { url.clone() } else { format!("item {i}") })
            .collect::<Vec<_>>(),
        "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
        "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
    )
    .unwrap();
    for g in glyph_sets() {
        for width in [60u16, 80, 120] {
            let mut state = state_of(&df, 20);
            state.page_down();
            state.collect();
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 21);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            assert!(rows[0].contains("id"), "{ctx}");
            assert!(rows[0].contains("desc"), "{ctx}");
            let long = rows
                .iter()
                .find(|row| row.contains("https://example.com/"))
                .expect(&ctx);
            assert!(long.contains(g.ellipsis), "{ctx}");
            for row in &rows[1..] {
                assert!(!row.trim().is_empty(), "{ctx}");
            }
        }
    }
}

/// The issue's long-URL frame: six columns over 80 rows, every description short
/// but row 24's.
fn long_url_frame() -> DataFrame {
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let n = 80usize;
    let start = NaiveDate::from_ymd_opt(2024, 1, 1)
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

/// Paging down past a page with one long value, and back, moves no column: the
/// heading row is the same on every page, at every width, in both glyph sets.
/// Measured per page, the long URL turned a six-column view into a two-column one.
/// On its page the URL is clipped and marked.
#[test]
fn widths_hold_still_across_pages() {
    let df = long_url_frame();
    for g in glyph_sets() {
        for width in [60u16, 80, 120] {
            let mut state = state_of(&df, 20);
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let first = draw(table(), &mut state, width, 21);
            let ctx = |rows: &[String]| format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            if width >= 80 {
                assert!(first[0].contains("timestamp"), "{}", ctx(&first));
            }
            let mut saw_url = false;
            for step in 0..6 {
                if step < 3 {
                    state.page_down();
                } else {
                    state.page_up();
                }
                state.collect();
                let rows = draw(table(), &mut state, width, 21);
                assert_eq!(rows[0], first[0], "page {step}: {}", ctx(&rows));
                if let Some(row) = rows.iter().find(|r| r.contains("https://")) {
                    saw_url = true;
                    let clipped = row.split_whitespace().nth(1).unwrap();
                    assert!(clipped.ends_with(g.ellipsis), "{}", ctx(&rows));
                }
            }
            assert!(saw_url, "the long URL's page was drawn");
        }
    }
}

/// A frozen column keeps its width across pages, so the number of columns that
/// stay frozen does not change with the page either.
#[test]
fn frozen_columns_hold_still_across_pages() {
    let df = long_url_frame();
    for g in glyph_sets() {
        for width in [60u16, 80, 120] {
            let mut state = state_of(&df, 20);
            state.set_locked_columns(3);
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let first = draw(table(), &mut state, width, 21);
            let frozen = state.frozen_shown();
            for _ in 0..3 {
                state.page_down();
                state.collect();
                let rows = draw(table(), &mut state, width, 21);
                let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                assert_eq!(state.frozen_shown(), frozen, "{ctx}");
                assert_eq!(rows[0], first[0], "{ctx}");
            }
        }
    }
}

/// A number's column widens for a wider number on a later page, so it is never
/// cut, and does not narrow again on a page of narrower ones.
#[test]
fn a_number_column_widens_and_stays_wide() {
    let values: Vec<i64> = (0..60)
        .map(|i| if i == 30 { 123_456_789 } else { i })
        .collect();
    let df =
        df!("n" => values, "t" => (0..60).map(|i| format!("t{i}")).collect::<Vec<_>>()).unwrap();
    let mut state = state_of(&df, 20);
    draw(DataTable::default(), &mut state, 60, 21);
    assert_eq!(state.shown_width("n"), Some(2));
    state.page_down();
    state.collect();
    let rows = draw(DataTable::default(), &mut state, 60, 21);
    assert!(rows.iter().any(|r| r.contains("123456789")), "{rows:#?}");
    state.page_down();
    state.collect();
    draw(DataTable::default(), &mut state, 60, 21);
    assert_eq!(state.shown_width("n"), Some(9));
}

/// A struct is a preview like text: one long value on a later page is clipped
/// at the cap, and the columns after it stay on screen on the pages after.
#[test]
fn a_long_struct_value_does_not_widen_its_column_for_good() {
    let n = 60usize;
    let y: Vec<String> = (0..n)
        .map(|i| {
            if i == 30 {
                "a very long struct value ".repeat(6)
            } else {
                "short".to_string()
            }
        })
        .collect();
    let s = StructChunked::from_series(
        "s".into(),
        n,
        [
            Series::new("x".into(), (0..n as i64).collect::<Vec<_>>()),
            Series::new("y".into(), y),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let df = DataFrame::new_infer_height(vec![
        Series::new("id".into(), (0..n as i64).collect::<Vec<_>>()).into(),
        s.into(),
        Series::new("tail".into(), (0..n as i64).collect::<Vec<_>>()).into(),
    ])
    .unwrap();
    let mut state = state_of(&df, 20);
    for page in 0..3 {
        let rows = draw(DataTable::default(), &mut state, 80, 21);
        assert!(rows[0].contains("tail"), "page {page}: {rows:#?}");
        assert!(
            state.shown_width("s").is_some_and(|w| w <= 32),
            "page {page}: {rows:#?}"
        );
        if page == 1 {
            let long = rows.iter().find(|r| r.contains("{30,")).unwrap();
            assert!(long.contains(crate::glyphs::get().ellipsis), "{rows:#?}");
        }
        state.page_down();
        state.collect();
    }
}

/// Automatic text and headings stop at two fifths of the terminal, marked where
/// cut: 32 cells at 80 columns, 48 at 120.
#[test]
fn automatic_text_stops_at_the_cap_and_is_marked() {
    let long = "x".repeat(200);
    let df = df!("text" => &[long.as_str()], "tail" => &[1i64]).unwrap();
    for g in glyph_sets() {
        for (width, cap) in [(80u16, 32u16), (120, 48)] {
            let mut state = state_of(&df, 1);
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, width, 2);
            let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
            assert_eq!(state.shown_width("text"), Some(cap), "{ctx}");
            let row: String = rows[1].chars().skip(1).collect();
            let value = row.split_whitespace().next().unwrap();
            assert!(value.ends_with(g.ellipsis), "{ctx}");
            assert_eq!(crate::glyphs::cell_width(value), usize::from(cap), "{ctx}");
            assert!(rows[0].contains("tail"), "{ctx}");
        }
    }
}

/// The last column drawn takes the room to the table's right edge: a long text on
/// the far right runs to the edge rather than stopping at the cap. A width set by
/// hand is drawn as set, and a number stays under its heading.
#[test]
fn the_last_column_runs_to_the_right_edge() {
    let long = "x".repeat(200);
    let df = df!("id" => &[1i64], "text" => &[long.as_str()]).unwrap();
    let ellipsis = crate::glyphs::get().ellipsis;
    let mut state = state_of(&df, 1);
    let rows = draw(DataTable::default(), &mut state, 120, 2);
    let value = rows[1].trim_end();
    assert!(value.ends_with(ellipsis), "{rows:#?}");
    assert_eq!(crate::glyphs::cell_width(value), 120, "{rows:#?}");
    assert_eq!(state.shown_width("text"), Some(48), "learned at the cap");
    assert!(state.on_screen_width("text").unwrap() > 48, "drawn past it");

    state.set_width_choices([("text".to_string(), WidthChoice::Manual(20))]);
    let rows = draw(DataTable::default(), &mut state, 120, 2);
    assert_eq!(state.shown_width("text"), Some(20), "{rows:#?}");
    assert!(
        crate::glyphs::cell_width(rows[1].trim_end()) < 40,
        "{rows:#?}"
    );

    let df = df!("text" => &["ab"], "n" => &[5i64]).unwrap();
    let mut state = state_of(&df, 1);
    let rows = draw(DataTable::default(), &mut state, 120, 2);
    assert!(
        crate::glyphs::cell_width(rows[1].trim_end()) < 20,
        "{rows:#?}"
    );
}

/// The cap follows the terminal, not the table: a sidebar narrowing the table
/// moves no column.
#[test]
fn a_sidebar_moves_no_column() {
    let df = long_url_frame();
    let mut state = state_of(&df, 20);
    state.page_down();
    state.collect();
    let table = || DataTable {
        screen_width: 120,
        ..DataTable::default()
    };
    let whole = draw(table(), &mut state, 120, 21);
    let widths: Vec<_> = ["id", "description", "amount"]
        .iter()
        .map(|c| state.shown_width(c))
        .collect();
    let beside = draw(table(), &mut state, 70, 21);
    for (c, before) in ["id", "description", "amount"].iter().zip(&widths) {
        assert_eq!(state.shown_width(c), *before, "{c}: {beside:#?}");
    }
    assert!(
        whole[0].starts_with(&beside[0][..40]),
        "{whole:#?} {beside:#?}"
    );
}

/// A width set by hand: exact for text, values clipped and marked; it survives
/// paging, scrolling, reordering, hiding and resizing; fit and reset change it.
#[test]
fn a_manual_width_survives_everything_but_fit_and_reset() {
    let df = long_url_frame();
    let mut state = state_of(&df, 20);
    state.set_width_choices([("description".to_string(), WidthChoice::Manual(6))]);
    let rows = draw(DataTable::default(), &mut state, 80, 21);
    assert_eq!(state.shown_width("description"), Some(6), "{rows:#?}");
    // Six cells, the ellipsis among them: `item …`, or `ite...` in ASCII.
    let ellipsis = crate::glyphs::get().ellipsis;
    let kept = 6 - crate::glyphs::display_width(ellipsis);
    let clipped = format!("{}{ellipsis}", &"item 10"[..kept]);
    assert!(rows.iter().any(|r| r.contains(&clipped)), "{rows:#?}");

    state.page_down();
    state.collect();
    state.scroll_right();
    draw(DataTable::default(), &mut state, 80, 21);
    state.scroll_left();
    // Moved to the end, then hidden and shown again.
    let mut order = state.headers();
    order.retain(|c| c != "description");
    order.push("description".to_string());
    state.set_column_order(order.clone());
    draw(DataTable::default(), &mut state, 200, 21);
    assert_eq!(state.shown_width("description"), Some(6));
    order.pop();
    state.set_column_order(order.clone());
    draw(DataTable::default(), &mut state, 40, 21);
    order.insert(1, "description".to_string());
    state.set_column_order(order);
    draw(DataTable::default(), &mut state, 120, 21);
    assert_eq!(state.width_choice("description"), WidthChoice::Manual(6));
    assert_eq!(state.shown_width("description"), Some(6));

    // Fit takes the rows on screen: page two holds the URL.
    state.set_width_choices([("description".to_string(), WidthChoice::Fit)]);
    draw(DataTable::default(), &mut state, 120, 21);
    let url_width = 20 + 13 * 15;
    assert_eq!(
        state.width_choice("description"),
        WidthChoice::Manual(url_width)
    );
    state.reset();
    assert_eq!(state.width_choice("description"), WidthChoice::Auto);
}

/// A column scrolled out of view is fitted to the rows on screen too.
#[test]
fn a_column_out_of_view_is_fitted_to_the_page() {
    let df = long_url_frame();
    let mut state = state_of(&df, 20);
    state.page_down();
    state.collect();
    for _ in 0..3 {
        state.scroll_right();
    }
    draw(DataTable::default(), &mut state, 80, 21);
    state.set_width_choices([("description".to_string(), WidthChoice::Fit)]);
    let rows = draw(DataTable::default(), &mut state, 80, 21);
    assert!(!rows[0].contains("description"), "{rows:#?}");
    assert_eq!(
        state.width_choice("description"),
        WidthChoice::Manual(20 + 13 * 15)
    );
}

/// A number column set narrower than its numbers still shows them whole.
#[test]
fn a_number_column_set_narrow_still_shows_whole_numbers() {
    let df = df!("n" => &[1_234_567i64, 2], "t" => &["a", "b"]).unwrap();
    let mut state = state_of(&df, 2);
    state.set_width_choices([("n".to_string(), WidthChoice::Manual(4))]);
    let rows = draw(DataTable::default(), &mut state, 40, 3);
    assert!(rows[1].contains("1234567"), "{rows:#?}");
}

/// The same name with another type, as a query can make it, is another column:
/// it starts from an automatic width, and the first gets its own back.
#[test]
fn a_column_whose_type_changes_starts_afresh() {
    let df = df!("a" => &["x", "y"], "n" => &[1i64, 2]).unwrap();
    let mut state = state_of(&df, 2);
    state.set_width_choices([("a".to_string(), WidthChoice::Manual(9))]);
    state.query("select a: n".to_string());
    assert_eq!(state.width_choice("a"), WidthChoice::Auto);
    state.query("select a, n".to_string());
    assert_eq!(state.width_choice("a"), WidthChoice::Manual(9));
}

/// A column ending at the table's edge with more after it leaves its heading's
/// last cell to the off-screen hint, so the hint covers neither a letter nor the
/// clip marker, and the values keep the full width: a number that fits is whole,
/// never a preview.
#[test]
fn the_offscreen_hint_takes_a_heading_cell_not_a_value_cell() {
    let df = df!(
        "aaaaaaaaaa" => &["aaaaaaaaaa"],
        "bbbbbbb" => &[1_234_567i64],
        "c" => &["x"],
    )
    .unwrap();
    for g in glyph_sets() {
        for dtype_row in [false, true] {
            let table = DataTable {
                glyphs: g,
                dtype_row,
                ..DataTable::default()
            };
            let header_h = usize::from(table.header_height());
            let mut state = state_of(&df, 1);
            // The rail, ten cells, a gap and seven: the second column ends at the edge.
            let rows = draw(table, &mut state, 19, header_h as u16 + 1);
            let ctx = format!("{} glyphs, type row {dtype_row}: {rows:#?}", set_name(g));
            assert!(rows[header_h].ends_with(" 1234567"), "{ctx}");
            let hint_row = &rows[header_h - 1];
            assert!(hint_row.ends_with(g.arrow_right), "{ctx}");
            let before = hint_row.strip_suffix(g.arrow_right).unwrap();
            if dtype_row {
                assert!(before.trim_end().ends_with("i64"), "{ctx}");
            } else {
                assert!(before.ends_with(g.ellipsis), "{ctx}");
            }
        }
    }
}

/// A followed file's page near its end, and a filtered view's count after rows
/// arrive, are read from the marks the watcher made, not from the file's start.
#[test]
fn a_followed_view_reads_and_counts_from_its_marks() {
    use std::io::Write as _;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("grow.csv");
    let mut text = String::from("t,n\n");
    for i in 0..20_000 {
        text.push_str(&format!("{i},{}\n", i % 7));
    }
    std::fs::write(&path, &text).unwrap();
    let scan = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
        .with_ignore_errors(true)
        .finish()
        .unwrap();
    let options = crate::OpenOptions::default();
    let (lf, tail) =
        crate::follow::bound_to_complete(scan, &path, crate::FileFormat::Csv, &options).unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    let mut state = DataTableState::new(lf, None, None, None, None, false).unwrap();
    let rows = tail.rows();
    state.start_following(crate::follow::Follow::start(
        tail,
        std::time::Duration::from_secs(3_600),
        tx,
        None,
    ));
    state.follow_to(rows, false);
    let from_marks = |lf: &LazyFrame| format!("{:?}", lf.logical_plan).contains("FOLLOWED");
    let page = state.buffer_lf(19_990, 10).unwrap();
    assert!(from_marks(&page));
    let t = |df: DataFrame| df.column("t").unwrap().i64().unwrap().to_vec();
    assert_eq!(t(page.collect().unwrap()).first(), Some(&Some(19_990)));

    state.defer_collect = true;
    state.filter(vec![FilterStatement {
        columns: Vec::new(),
        column: "n".to_string(),
        operator: crate::filter_modal::FilterOperator::Eq,
        value: "3".to_string(),
        logical_op: crate::filter_modal::LogicalOperator::And,
    }]);
    let matches = |n: usize| (0..n).filter(|i| i % 7 == 3).count();
    // The first count of the filter reads the whole file.
    assert!(state.source_counter().is_none());
    state.set_num_rows(matches(20_000));

    let mut out = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    let more: String = (20_000..20_050)
        .map(|i| format!("{i},{}\n", i % 7))
        .collect();
    out.write_all(more.as_bytes()).unwrap();
    let follow = state.follow_mut().unwrap();
    follow.check_now();
    // A watcher that hears changes may report before the check it was asked for.
    let (rows, restarted) = loop {
        let crate::AppEvent::Followed(news) =
            rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap()
        else {
            panic!("the watcher said something else");
        };
        follow.take(&news.change);
        if follow.waiting() == 50 {
            break follow.catch_up();
        }
    };
    assert_eq!(rows, 20_050);
    state.follow_to(rows, restarted);
    assert!(!state.is_num_rows_valid());
    let counter = state.source_counter().expect("counts the new rows alone");
    assert_eq!(counter().unwrap(), matches(20_050));
    state.set_num_rows(matches(20_050));
    let last = state.buffer_lf(matches(20_050) - 3, 3).unwrap();
    assert!(from_marks(&last));
    let expected: Vec<_> = (0..20_050i64).filter(|i| i % 7 == 3).map(Some).collect();
    assert_eq!(t(last.collect().unwrap()), expected[expected.len() - 3..]);
}
