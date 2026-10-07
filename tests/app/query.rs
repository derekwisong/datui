//! Queries, SQL, filters, sorts, search, drill-down, pivot and melt, parsing strings, retyping.

use super::*;

#[test]
fn test_full_workflow() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    // 1. Create test CSV file inline
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("large_test.csv");

    let mut df = df!(
        "a" => (0..100).collect::<Vec<i32>>(),
        "b" => (0..100).map(|i| format!("text_{}", i)).collect::<Vec<String>>(),
        "c" => (0..100).map(|i| i % 3).collect::<Vec<i32>>(),
        "d" => (0..100).map(|i| i % 5).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    // 2. Open the file (pump full load chain)
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.clone()],
        OpenOptions::default(),
    );

    assert!(app.data_table_state.is_some());
    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.num_rows(), 100);

    // 2. Filter the data (s = Sort & Filter, switch to Filter tab, configure, Apply)
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(AppEvent::Key(key_event));
    assert_eq!(app.overlay, Overlay::SortFilter);

    app.sort_filter_modal.switch_tab(); // Filter tab
    app.sort_filter_modal.filter.available_columns =
        app.data_table_state.as_ref().unwrap().headers();
    let column = app.sort_filter_modal.filter.available_columns[2].clone();
    app.sort_filter_modal
        .filter
        .statements
        .push(datui::filter_modal::FilterStatement {
            columns: Vec::new(),
            column,
            operator: datui::filter_modal::FilterOperator::Eq,
            value: "1".to_string(),
            logical_op: datui::filter_modal::LogicalOperator::And,
        });
    // On the Filters tab Enter means add/edit; Ctrl+Enter is the apply.
    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL);
    if let Some(next_event) = app.event(AppEvent::Key(key_event)) {
        app.event(next_event);
    }
    drain_events(&mut app, &rx);
    assert_ne!(app.overlay, Overlay::SortFilter);

    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.lf().clone().collect().unwrap().shape().0, 33);

    // 3. Sort the data (s = Sort & Filter, Sort tab, configure, Apply)
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(AppEvent::Key(key_event));
    assert_eq!(app.overlay, Overlay::SortFilter);

    app.sort_filter_modal.sort.columns = app
        .data_table_state
        .as_ref()
        .unwrap()
        .headers()
        .iter()
        .enumerate()
        .map(|(i, h)| datui::sort_modal::SortColumn {
            name: h.clone(),
            sort_order: None,
            sort_descending: false,
            display_order: i,
            is_locked: false,
            is_to_be_locked: false,
            is_visible: true,
            width: datui::widgets::column_widths::WidthChoice::Auto,
            shown_width: None,
        })
        .collect();
    app.sort_filter_modal.sort.table_state.select(Some(0));
    // Space cycles none -> ascending -> descending.
    app.sort_filter_modal.sort.cycle_sort();
    app.sort_filter_modal.sort.cycle_sort();
    app.sort_filter_modal.focus = datui::sort_filter_modal::SortFilterField::TabBar;

    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    if let Some(next_event) = app.event(AppEvent::Key(key_event)) {
        app.event(next_event);
    }
    drain_events(&mut app, &rx);
    assert_ne!(app.overlay, Overlay::SortFilter);

    let datatable = app.data_table_state.as_ref().unwrap();
    let df = datatable.lf().clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
}

/// The help over the `:` command line at 80x24 sits above the footer's prompt: its
/// frame is whole, the prompt drawn under it.
#[test]
fn test_help_clears_the_command_line_at_80_by_24() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("help_over_strip.csv");
    press(&mut app, KeyCode::Char(':'));
    press(&mut app, KeyCode::F(1));
    assert!(app.help_visible());
    let screen = draw_sized(&mut app, (80, 24));
    let rows: Vec<&str> = screen.lines().collect();
    let bottom = rows
        .iter()
        .rposition(|row| row.contains('╰'))
        .unwrap_or_else(|| panic!("the help's bottom border is drawn:\n{screen}"));
    let prompt = rows
        .iter()
        .position(|row| row.trim_start().starts_with("sql:") || row.trim_start().starts_with("q:"))
        .unwrap_or_else(|| panic!("the prompt is drawn:\n{screen}"));
    assert!(
        bottom < prompt,
        "the frame ends above the prompt:\n{screen}"
    );
}

/// An aggregate reads every row as one group-by, whatever the sample size, and says
/// so; a bar chart of the mean per category needs no query.
#[test]
fn aggregates_run_over_every_row() {
    use datui::chart_modal::{Aggregate, ChartFocus, Mark};
    let (mut app, rx, tx) = open_flights("chart_aggregate_test.parquet");
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Bar);
    app.chart.modal.row_limit = Some(10);
    app.chart.modal.focus = ChartFocus::Y;
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter); // delay
    app.chart.modal.focus = ChartFocus::Aggregate;
    press(&mut app, KeyCode::Right); // count -> distinct
    press(&mut app, KeyCode::Right); // -> sum
    press(&mut app, KeyCode::Right); // -> mean
    assert_eq!(app.chart.modal.aggregate(), Aggregate::Mean);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(text.contains("all 900 rows"), "{text}");
    assert!(!text.contains("sample of"), "{text}");
    // F9 is carrier 8 of 9: its delays are 8 and 9, half each, a mean of 8.5.
    assert!(text.contains("F9") && text.contains("8.50"), "{text}");
}

/// Sorting reorders rows across files, so a row's position no longer says which file
/// it came from. The scan's drift column rides along with the row, so the distinction
/// survives.
#[test]
fn test_absent_cells_still_read_as_absent_after_a_sort() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "note" => &[None::<&str>, None]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "note" => &["hi", "yo"], "extra" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "absent before the sort"
    );

    // Descending by id interleaves the two files: 4, 3, 2, 1.
    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], false);
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.error().is_none(), "the sort itself must succeed");

    let text = painted(&mut app, &rx, &tx, area);
    assert!(
        text.contains(g.absent),
        "the rows from the file without `extra` are still absent, not null"
    );
    assert!(text.contains(g.null), "and the real nulls are still nulls");
}

/// All three kinds of empty still read right after a filter.
///
/// The sort case above is the other half of the same criterion. A filter is the one
/// that rebuilds the frame rather than reordering it, and it is the one no test looked
/// at on screen: the row → file mapping has to survive a predicate, not just a reorder.
/// The conflicting column is here rather than in the sort fixture because a filter that
/// does not name it must keep its rows — leaving them out is for a filter that does.
#[test]
fn test_absent_null_and_conflicting_cells_still_differ_after_a_filter() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    // Three rows against two, so the majority type for `price` is the number and the
    // text file's rows are the ones left unread.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!(
            "id" => &[1i64, 4, 5],
            "note" => &[None::<&str>, None, None],
            "price" => &[10i64, 40, 50],
        )
        .unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!(
            "id" => &[2i64, 3],
            "note" => &["hi", "yo"],
            "extra" => &["x", "y"],
            "price" => &["cheap", "dear"],
        )
        .unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 120, 20);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        before.contains(g.absent) && before.contains(g.null) && before.contains(g.conflict),
        "all three before the filter: {before}"
    );

    // On `id`, which both files hold as the same type — so nothing is left out for
    // being unreadable, and rows from both files survive.
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt(
        "id",
        datui::filter_modal::FilterOperator::GtEq,
        "2",
    )]);
    assert!(state.error().is_none(), "the filter itself must succeed");
    // And a filter actually happened: without this the test passes when the predicate
    // is dropped on the floor, because an unfiltered screen carries all three glyphs
    // too. The criterion is about surviving a predicate, so there has to be one.
    assert_eq!(current_rows(&app), 4, "1 is gone; 2, 3, 4 and 5 are left");

    let after = painted(&mut app, &rx, &tx, area);
    assert!(
        after.contains(g.absent),
        "the rows of the file without `extra` still say absent: {after}"
    );
    assert!(
        after.contains(g.null),
        "and the real nulls are still nulls: {after}"
    );
    assert!(
        after.contains(g.conflict),
        "and the file that holds `price` as text still says so: {after}"
    );
}

/// A conflicting column has no value to order by, so a sort on it leaves those rows
/// out rather than gathering them at one end as though they belonged there — and says
/// how many went.
#[test]
fn test_a_sort_leaves_out_the_rows_its_column_is_not_read_from() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4, 5, 6, 7], "n" => &[30i64, 40, 50, 60, 70]).unwrap(),
    );
    // `n` as text here, so it is not read from this file: two rows of conflict.
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[8i64, 9], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 10, "every row is there to begin with");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["n".to_string()], true);
    assert!(state.error().is_none(), "the sort itself must succeed");

    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(
        ids.len(),
        8,
        "the two rows from the file that stores `n` as text are gone: {ids:?}"
    );
    assert!(
        !ids.contains(&8) && !ids.contains(&9),
        "and it is those two, not two others: {ids:?}"
    );
    assert!(
        ids.contains(&0) && ids.contains(&1) && ids.contains(&2),
        "the file with no `n` at all keeps its rows: its cells are absent, not a \
         value in another type: {ids:?}"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("left out of the"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(left_out.summary, "n: 2 rows in 1 file left out of the sort");
    assert_eq!(left_out.scope, "in all 3 footers");
    assert!(
        state.notes_unseen(),
        "and the `i` accent comes back for a note the user has not been offered"
    );
}

/// The filter half: the other two wordings the note has, and the row a filter's own
/// terms matched but its file cannot stand behind.
///
/// A sidebar filter of `id = 3 or n = 0` matches the row whose `id` is 3 — but that
/// row's file stores `n` as text, so its `n` was never read and the view cannot
/// answer either half of the question. It goes, and the note says why.
#[test]
fn test_a_filter_leaves_out_the_rows_its_column_is_not_read_from() {
    use datui::filter_modal::{FilterOperator, LogicalOperator};

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let mut or_id_3 = filter_stmt("id", FilterOperator::Eq, "3");
    or_id_3.logical_op = LogicalOperator::Or;
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt("n", FilterOperator::Eq, "0"), or_id_3]);
    assert!(state.error().is_none(), "the filter itself must succeed");

    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(
        ids,
        vec![0],
        "id 3 matched a term of its own, but its file's `n` was never read"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("left out of the"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n: 2 rows in 1 file left out of the filter"
    );

    // Sorting by the same column too: one note, naming both.
    state.sort(vec!["n".to_string()], true);
    let notes = state.notes();
    let both: Vec<&str> = notes
        .iter()
        .filter(|note| note.summary.contains("left out of the"))
        .map(|note| note.summary.as_str())
        .collect();
    assert_eq!(
        both,
        ["n: 2 rows in 1 file left out of the filter and sort"],
        "one note for the column, not one for each of the two things naming it"
    );
}

/// A column the feed started sending is named by where it starts, not only by a count.
///
/// "in 2 of 3 files" says a column is unusual; "none before date=2024-01-02" says when
/// it began, which for a field added to a feed is the whole question. Through a real
/// directory rather than a hand-built footer list, because the partition it names comes
/// from the file's own path.
#[test]
fn test_a_column_that_starts_partway_through_says_where_it_starts() {
    // And a column that belongs to one partition is named by that partition.
    for (dates, column, files, expected) in [
        (
            ["2024-01-01", "2024-01-02", "2024-01-03"],
            "fee",
            [false, true, true],
            "fee is in 2 of 3 files, none before date=2024-01-02; absent from the rest, \
             not null",
        ),
        (
            ["2024-03-01", "2024-03-02", "2024-03-03"],
            "oops",
            [false, true, false],
            "oops is in 1 of 3 files, only date=2024-03-02; absent from the rest, not null",
        ),
    ] {
        let dir = tempfile::tempdir().unwrap();
        for (i, (date, has)) in dates.iter().zip(files).enumerate() {
            let id = i as i64 + 1;
            let df = if has {
                df!("id" => &[id], column => &[id * 10]).unwrap()
            } else {
                df!("id" => &[id]).unwrap()
            };
            write_parquet(dir.path(), &format!("date={date}"), df);
        }
        let app = open_local_dataset(dir.path());
        let state = app.data_table_state.as_ref().unwrap();
        let notes = state.notes();
        let about = notes
            .iter()
            .find(|note| note.summary.starts_with(&format!("{column} is in")))
            .unwrap_or_else(|| panic!("no note about `{column}`: {notes:#?}"));
        assert_eq!(about.summary, expected);
    }
}

/// The offer is not made where datui could not honour it.
///
/// Reading a column as text needs to know where each file's rows begin, and datui does
/// not for a dataset whose footers could not all be read — the same datasets that
/// cannot draw the marks. The note is still worth saying; the offer on it is not.
#[test]
fn test_no_offer_to_read_as_text_where_the_files_were_not_all_counted() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "n" => &["sixty"]).unwrap(),
    );
    // A third file datui cannot read the footer of.
    let broken = dir.path().join("date=2024-01-03");
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(broken.join("data.parquet"), b"not a parquet file at all").unwrap();

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|note| note.summary.contains("not read there")),
        "the conflict is still worth saying: {notes:#?}"
    );
    assert!(
        notes.iter().all(|note| note.read_as_text.is_none()),
        "but datui cannot act on it, so it does not offer to: {notes:#?}"
    );
}

/// A filter on a column read as text compares text, and the panel says so.
///
/// `n > 5` was written for a number. Read as text it keeps `"sixty"` and drops `"10"`,
/// which is a different question with the same words — so the note that arrives in
/// place of the conflict note is the one thing standing between the user and a view
/// they would read wrongly.
#[test]
fn test_reading_a_filtered_column_as_text_says_the_comparison_changed() {
    use datui::filter_modal::FilterOperator;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[1i64, 10, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt("n", FilterOperator::Gt, "5")]);
    assert_eq!(current_rows(&app), 2, "10 and 20 are greater than 5");

    let state = app.data_table_state.as_mut().unwrap();
    state.mark_notes_seen();
    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|note| note.summary == "n read as text: filter and sort compare text"),
        "the filter means something else now, and the panel says so: {notes:#?}"
    );
    assert!(
        state.notes_unseen(),
        "and the `i` accent comes back, since the user has not been told yet"
    );
}

/// The accent is about the note being *new*: a sort that has something to say brings
/// it back after the panel has already been opened once.
#[test]
fn test_a_sort_that_leaves_rows_out_offers_its_note_afresh() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.mark_notes_seen();
    assert!(
        !state.notes_unseen(),
        "the dataset's own notes have been offered"
    );

    state.sort(vec!["n".to_string()], true);
    assert!(
        state.notes_unseen(),
        "the note about the rows the sort left out has not been"
    );

    state.mark_notes_seen();
    state.sort(vec!["n".to_string()], false);
    assert!(
        !state.notes_unseen(),
        "and sorting the same column the other way says nothing new, so the accent \
         stays away"
    );
}

/// A conflicting file that is not the last one, several of them, and two stretches
/// that do not touch.
///
/// The last file is where a run's end and the end of the dataset are the same number,
/// so a dataset whose only conflict is there cannot tell a right implementation from
/// one that drops everything from the first conflict onwards.
#[test]
fn test_the_rows_left_out_are_the_conflicting_files_own_wherever_they_sit() {
    let dir = tempfile::tempdir().unwrap();
    // Read as an integer: six of the ten rows hold it that way.
    let int = |ids: &[i64], ns: &[i64]| df!("id" => ids, "n" => ns).unwrap();
    let text = |ids: &[i64], ns: &[&str]| df!("id" => ids, "n" => ns).unwrap();
    write_parquet(dir.path(), "date=2024-01-01", int(&[0, 1], &[0, 1]));
    write_parquet(dir.path(), "date=2024-01-02", text(&[2, 3], &["a", "b"]));
    write_parquet(dir.path(), "date=2024-01-03", text(&[4], &["c"]));
    write_parquet(dir.path(), "date=2024-01-04", int(&[5, 6], &[5, 6]));
    write_parquet(dir.path(), "date=2024-01-05", text(&[7], &["d"]));
    write_parquet(dir.path(), "date=2024-01-06", int(&[8, 9], &[8, 9]));

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 10, "every row is there to begin with");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["n".to_string()], true);
    assert!(state.error().is_none(), "the sort itself must succeed");

    let mut ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![0, 1, 5, 6, 8, 9],
        "the two stretches that store `n` as text go, and nothing after them does"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("left out of the"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n: 4 rows in 3 files left out of the sort"
    );
    // Copy as Python cannot leave them out, so it says so and stops there.
    let script = app.python_script(app.data_table_state.as_ref().unwrap());
    assert!(
        script.contains(
            "    # n: 4 rows in 3 files left out of the sort\n    \
             # .sort("
        ),
        "{script}"
    );
}

/// Clearing the sort brings the rows back and takes the note with it, and a sort on a
/// column the files agree on never took any rows to begin with.
#[test]
fn test_only_the_conflicting_column_costs_rows_and_only_while_it_is_sorted() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], true);
    assert_eq!(
        current_rows(&app),
        5,
        "`id` is the same type everywhere, so a sort on it leaves nothing out"
    );
    let state = app.data_table_state.as_mut().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|n| n.summary.contains("left out of the")),
        "and says nothing about rows going"
    );

    state.sort(vec!["n".to_string()], true);
    assert_eq!(current_rows(&app), 3, "sorting by `n` leaves the two out");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(Vec::new(), true);
    assert_eq!(
        current_rows(&app),
        5,
        "and clearing the sort brings them back"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|n| n.summary.contains("left out of the")),
        "with nothing left saying they went: {:#?}",
        state.notes()
    );
}

/// A query builds its own rows, and its schema becomes the column order — so a query
/// root that still carried the hidden drift column turned it into one of the data's,
/// visible in the table and the sidebar.
#[cfg(feature = "sql")]
#[test]
fn test_a_query_never_turns_the_drift_column_into_a_real_one() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );
    let expected = ["date", "id", "extra"];

    for (what, run) in [
        ("a fuzzy search", 0),
        ("a DSL query", 1),
        ("a SQL query", 2),
        ("a reset", 3),
    ] {
        let (mut app, rx, _tx) = open_local_dataset_with_channel(dir.path());
        let state = app.data_table_state.as_mut().unwrap();
        match run {
            0 => state.fuzzy_search("x".to_string()),
            1 => state.query("select where id > 0".to_string()),
            2 => state.sql_query("select * from df".to_string()),
            _ => state.reset(),
        }
        assert!(state.error().is_none(), "{what}: {:?}", state.error());
        common::read_rows(&mut app, &rx);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(
            state.error().is_none(),
            "{what} collect: {:?}",
            state.error()
        );

        let order: Vec<&str> = state
            .get_column_order()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(order, expected, "column order after {what}");
        let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, expected, "schema after {what}");
    }
}

/// A query builds its own rows, so notes about the files behind the dataset no longer
/// describe what is on screen. They come back on a reset.
#[cfg(feature = "sql")]
#[test]
fn test_a_query_puts_the_notes_away_and_a_reset_brings_them_back() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, _tx) = open_local_dataset_with_channel(dir.path());
    let state = app.data_table_state.as_mut().unwrap();
    assert_eq!(state.notes().len(), 1, "the dataset has something to say");

    state.sql_query("select id from df".to_string());
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.error().is_none(), "the query: {:?}", state.error());
    assert!(
        state.notes().is_empty(),
        "a note about `extra` would describe a column the frame no longer has"
    );

    state.reset();
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.error().is_none(), "the reset: {:?}", state.error());
    assert_eq!(state.notes().len(), 1, "and the reset brings them back");
}

/// Asking for source files on a frame that no longer has them must not leak datui's
/// bookkeeping instead.
///
/// The option is on but the dataset cannot honor it, so the export plans the view
/// without the index at all.
#[cfg(feature = "sql")]
#[test]
fn test_asking_to_name_files_on_a_query_result_leaks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    // A query replaces the frame, so the rows no longer stand for rows of a file and
    // naming them is refused — but the export still runs.
    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("select * from df".to_string());
    common::read_rows(&mut app, &rx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(!state.can_name_source_files(), "nothing to name any more");

    let out = dir.path().join("refused.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let header = csv.lines().next().unwrap();
    assert!(
        !header.contains("__datui_row"),
        "datui's own bookkeeping must not reach the file: {header}"
    );
}

/// The Columns list is workable with keys alone from the moment it opens:
/// the render draws the cursor on the first row, so the first Space must sort
/// it. The state used to hold no selection while the rail showed one, and
/// Space, L and v silently did nothing until an arrow press.
#[test]
fn the_sort_list_cursor_is_real_on_open() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_cursor_open.csv");

    open_columns_list(&mut app);
    assert_eq!(
        app.sort_filter_modal.sort.table_state.selected(),
        Some(0),
        "the cursor the rail shows is the cursor the keys act on"
    );
    press(&mut app, KeyCode::Char(' '));
    let sorted: Vec<String> = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .filter(|c| c.sort_order.is_some())
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(sorted, vec!["a".to_string()], "the first Space sorts");
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_sort_columns(),
        &["a".to_string()],
        "Enter applies the staged sort"
    );
}

/// Esc retraces a browse back to the listing and stops there, however deep the user
/// went — it does not climb above the directory the browse began at.
#[test]
fn test_escape_stops_at_where_browsing_began() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let tmp = tempfile::tempdir().unwrap();
    let start = tmp.path().join("a");
    let deeper = start.join("b");
    std::fs::create_dir_all(&deeper).unwrap();
    let key = |app: &mut App, code| {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };

    app.home.path_input_active = true;
    app.home.path_input = start.display().to_string();
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.home.browsing.as_deref(), Some(start.as_path()));

    // As if Enter had descended into `b`.
    app.home.browsing = Some(deeper);
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing.as_deref(), Some(start.as_path()));
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing, None);
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A sidebar filter applies on top of the active DSL query rather than replacing it, and
/// clearing the filters returns to the query result. Reset still clears everything.
#[test]
fn test_sidebar_filter_applies_on_top_of_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("query_then_filter.csv");

    app.event(AppEvent::QQuery("select where a < 50".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);

    app.event(AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "1",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    // a in 0..50 with a % 3 == 1: 1, 4, ..., 49
    assert_eq!(
        current_rows(&app),
        17,
        "filter must apply to the query result"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where a < 50");
    assert_eq!(state.get_filters().len(), 1);

    app.event(AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        current_rows(&app),
        50,
        "clearing filters returns to the query result"
    );

    app.event(AppEvent::Reset);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 100);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_query()
            .is_empty()
    );
}

/// The q additions run through the app: `distinct`, the word operators and a
/// computed group key.
#[test]
fn test_q_style_distinct_like_mod_and_xbar() {
    let (mut app, rx, tx) = open_query_filter_fixture("query_q_style_additions.csv");

    app.event(AppEvent::QQuery("select distinct c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().error().is_none());
    assert_eq!(current_rows(&app), 3);

    // alpha_0 .. alpha_8, then 0 = (a mod 4) keeps 0, 4 and 8.
    app.event(AppEvent::QQuery(
        "select where name like \"alpha_?\", 0 = a mod 4".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 3);

    app.event(AppEvent::QQuery(
        "select n: count a by b: 10 xbar a".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("b").unwrap().get(9).unwrap(), AnyValue::Int64(90));
    assert_eq!(
        df.column("n").unwrap().get(9).unwrap(),
        AnyValue::UInt32(10)
    );
}

/// And for SQL.
#[cfg(feature = "sql")]
#[test]
fn test_sidebar_filter_keeps_sql_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("sql_then_filter.csv");

    app.event(AppEvent::SqlQuery(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);

    app.event(AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    // a in 0..30 with a % 3 == 0: 0, 3, ..., 27
    assert_eq!(current_rows(&app), 10);

    app.event(AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);
}

/// Footer rows dropped with `skip_tail_rows` stay dropped after a sidebar sort: the
/// load-time trimming is part of the pipeline's root, not just of the first view.
#[test]
fn test_skip_tail_rows_survives_a_sidebar_sort() {
    let mut csv = String::from("a,b\n");
    for i in 0..100 {
        csv.push_str(&format!("{i},{}\n", i * 2));
    }
    // Two summary rows at the end, the kind `skip_tail_rows` exists for.
    csv.push_str("9999,-1\n9998,-2\n");
    let options = OpenOptions {
        skip_tail_rows: Some(2),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open_csv_with("skip_tail_then_sort.csv", &csv, options);
    assert_eq!(current_rows(&app), 100);

    app.event(AppEvent::Sort(vec!["b".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    assert_eq!(df.height(), 100, "the footer rows must not come back");
    assert_eq!(
        df.column("b").unwrap().get(0).unwrap(),
        AnyValue::Int64(198)
    );
}

/// Numbers parsed out of padded strings with `parse_strings` are still numbers when a
/// sidebar filter compares them.
#[test]
fn test_parse_strings_survives_a_sidebar_filter() {
    use datui::ParseStringsTarget;
    use datui::filter_modal::FilterOperator;
    let mut csv = String::from("id,amount\n");
    for i in 0..100 {
        csv.push_str(&format!("{i},\" {} \"\n", i * 3));
    }
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open_csv_with("parse_strings_then_filter.csv", &csv, options);
    {
        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.schema().get("amount").unwrap().is_integer(),
            "parse_strings should have made amount numeric"
        );
    }

    app.event(AppEvent::Filter(vec![filter_stmt(
        "amount",
        FilterOperator::Gt,
        "150",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    // amount = 3 * id > 150 for id 51..100
    assert_eq!(current_rows(&app), 49);
}

/// SQL after a pivot sees the pivoted columns: the reshape is the root the query runs
/// against, so `SELECT` of a pivoted column works and the reshape stays in the view.
#[cfg(feature = "sql")]
#[test]
fn test_sql_after_pivot_sees_the_pivoted_columns() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    let (mut app, rx, tx) = open_csv_with("pivot_then_sql.csv", &csv, OpenOptions::default());

    app.event(AppEvent::Pivot(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::First,
    }));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 10);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema()
            .contains("k1")
    );

    app.event(AppEvent::SqlQuery(
        "SELECT id, k2 FROM df WHERE k1 > 4".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 5, "ids 5..9");
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert_eq!(names, vec!["id", "k2"]);
}

/// While drilled into a group, a sidebar filter or sort applies within the group and
/// leaves the drill-down in place; drilling back up restores the grouped view.
#[test]
fn test_sidebar_filter_and_sort_stay_inside_a_drill_down() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_down_filter.csv");

    app.event(AppEvent::QQuery("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 3, "one row per group");

    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_down_into_group(0)
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert_eq!(current_rows(&app), 34, "c == 0: 0, 3, ..., 99");

    app.event(AppEvent::Filter(vec![filter_stmt(
        "a",
        FilterOperator::Lt,
        "30",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    assert!(
        state.is_drilled_down(),
        "the filter must not undo the drill-down"
    );
    assert_eq!(current_rows(&app), 10);

    app.event(AppEvent::Sort(vec!["a".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(27));

    app.data_table_state.as_mut().unwrap().drill_up().unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert!(
        state.get_filters().is_empty(),
        "the group's filter stays with the group"
    );
    assert_eq!(current_rows(&app), 3);
}

/// A DSL query after a pivot shows the loaded columns again, so SQL afterwards must run
/// against the loaded data, not against a pivot the user no longer sees.
#[cfg(feature = "sql")]
#[test]
fn test_query_after_pivot_drops_the_reshape_for_sql() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    let (mut app, rx, tx) = open_csv_with("pivot_query_sql.csv", &csv, OpenOptions::default());

    app.event(AppEvent::Pivot(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::First,
    }));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema()
            .contains("k1")
    );

    app.event(AppEvent::QQuery("select id, key".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 20);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .last_pivot_spec()
            .is_none()
    );

    app.event(AppEvent::SqlQuery("SELECT * FROM df".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    let names: Vec<String> = state.schema().iter_names().map(|s| s.to_string()).collect();
    assert_eq!(names, vec!["id", "key", "val"], "the unpivoted columns");
    assert_eq!(current_rows(&app), 20);
}

/// Drilling into a group swaps the applied filters and sort for the group's; the Sort &
/// Filter sidebar must follow, or Apply would re-send the grouped view's filter against
/// a List column. Drilling back up brings the grouped view's settings back.
#[test]
fn test_drill_down_resyncs_the_sort_filter_sidebar() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sidebar.csv");

    app.event(AppEvent::QQuery("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let statement = filter_stmt("c", FilterOperator::Gt, "0");
    app.event(AppEvent::Filter(vec![statement.clone()]));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(AppEvent::Sort(vec!["c".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 2, "groups c = 1 and c = 2");
    // What Apply would have left in the sidebar.
    app.sort_filter_modal.filter.statements = vec![statement];
    app.sort_filter_modal.sort.columns = vec![datui::sort_modal::SortColumn {
        name: "c".to_string(),
        sort_order: Some(0),
        sort_descending: false,
        display_order: 0,
        is_locked: false,
        is_to_be_locked: false,
        is_visible: true,
        width: datui::widgets::column_widths::WidthChoice::Auto,
        shown_width: None,
    }];

    // Enter on the highlighted group row drills in.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert!(
        app.sort_filter_modal.filter.statements.is_empty(),
        "no filter applies inside the group yet"
    );
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.sort_order.is_none())
    );
    assert_eq!(
        app.sort_filter_modal.filter.available_columns,
        app.data_table_state.as_ref().unwrap().headers()
    );

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
    let statements = &app.sort_filter_modal.filter.statements;
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].column, "c");
    assert_eq!(statements[0].value, "0");
    let sorted: Vec<&str> = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .filter(|c| c.sort_order.is_some())
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(sorted, vec!["c"]);
    let c = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .find(|c| c.name == "c")
        .unwrap();
    assert!(c.sort_descending, "the applied direction arrives staged");
}

/// Esc from a group taller than the screen draws the grouped rows again, not the
/// group's: the group's buffer covered the view, so it used to be kept. The cursor
/// comes back to the group drilled into, and the key column is frozen again.
#[test]
fn test_esc_from_a_drill_down_shows_the_grouped_rows() {
    let mut csv = String::from("a,c\n");
    for i in 0..1200 {
        csv.push_str(&format!("{i},{}\n", i % 3));
    }
    let (mut app, rx, tx) = open_csv_with("drill_esc_rows.csv", &csv, OpenOptions::default());
    let area = Rect::new(0, 0, 100, 30);
    painted(&mut app, &rx, &tx, area);

    app.event(AppEvent::QQuery("select a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "c"), ["0", "1", "2"]);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        1
    );

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert!(on_screen(&app, "c").iter().all(|c| c == "2"));
    assert_eq!(on_screen(&app, "a")[..2], ["2", "5"]);

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(on_screen(&app, "c"), ["0", "1", "2"]);
    assert_eq!(
        state.table_state.selected(),
        Some(2),
        "on the group drilled into"
    );
    assert_eq!(state.locked_columns_count(), 1, "the key stays frozen");
}

/// Drilling from a grouped view taller than the screen into a small group draws the
/// group, not the grouped rows the buffer held.
#[test]
fn test_drill_into_a_small_group_shows_its_rows() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_small_group.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(AppEvent::QQuery("select name by a".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "a").len(), 26, "a screen of the 100 groups");

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "name"), ["beta_1"]);
}

/// Enter on an aggregated `by` result, which holds no rows of its groups, drills into
/// the source rows of the group, after the query's `where`; Esc brings the aggregate
/// back.
#[test]
fn test_enter_drills_from_an_aggregated_result() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_aggregate.csv");
    let area = Rect::new(0, 0, 100, 30);

    app.event(AppEvent::QQuery(
        "select n: count a, total: sum a by c where a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "n"), ["10", "10", "10"]);

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["1".to_string()])
    );
    assert_eq!(
        state.headers(),
        ["c", "a", "name"],
        "the source's columns, key first"
    );
    assert_eq!(
        on_screen(&app, "a"),
        ["1", "4", "7", "10", "13", "16", "19", "22", "25", "28"],
        "c = 1 and a < 30"
    );

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(on_screen(&app, "total"), ["135", "145", "155"]);
    assert_eq!(state.table_state.selected(), Some(1));
}

/// A computed, renamed key drills by the expression that computed it, and a null key
/// drills into the rows whose key is null.
#[test]
fn test_drill_from_an_aggregate_by_a_computed_key_and_a_null_key() {
    let csv = "k,v\nx,1\n,2\ny,3\n,4\nx,5\n,6\n";
    let (mut app, rx, tx) = open_csv_with("drill_null_key.csv", csv, OpenOptions::default());

    app.event(AppEvent::QQuery("select n: count v by key: k".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    // Nulls sort last.
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(2).unwrap();
    assert_eq!(state.headers(), ["k", "v"]);
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.column("k").unwrap().null_count(), 3);
    assert_eq!(df.column("v").unwrap().i64().unwrap().sum(), Some(12));
    state.drill_up().unwrap();

    app.event(AppEvent::QQuery(
        "select n: count v by big: v > 3".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(1).unwrap();
    assert_eq!(
        state
            .drilled_group_key()
            .map(|(columns, _)| columns.to_vec()),
        Some(vec!["big".to_string()])
    );
    let df = state.lf().clone().collect().unwrap();
    let v: Vec<i64> = df
        .column("v")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(v, [4, 5, 6], "big = true");
}

/// Every group of an aggregate drills into as many rows as it counted, whatever the
/// key's type: floats with NaN and signed zeros, dates, zoned datetimes, categoricals,
/// and nulls of each.
#[test]
fn test_drill_from_an_aggregate_by_typed_keys() {
    let dir = common::fixture_dir().join("drill_typed_keys");
    let tz = TimeZone::opt_try_new(Some("America/New_York")).unwrap();
    let df = df!(
        "f" => [Some(1.5), Some(1.5), Some(f64::NAN), Some(f64::NAN), None, Some(-0.0), Some(0.0), Some(0.1 + 0.2)],
        "d" => [Some(19000), Some(19000), Some(19001), None, None, Some(19001), Some(19002), Some(19000)],
        "t" => [Some(1_700_000_000_000_000i64), Some(1_700_000_000_000_000), None, Some(1_700_000_000_000_001), None, Some(1), Some(1), Some(1)],
        "s" => [Some("a"), Some("b"), Some("a"), None, Some("b"), Some("a"), None, Some("c")],
        "v" => [1i64, 2, 3, 4, 5, 6, 7, 8],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("d").cast(DataType::Date),
        col("t").cast(DataType::Datetime(TimeUnit::Microseconds, tz)),
        col("s").cast(DataType::from_categories(Categories::global())),
    ])
    .collect()
    .unwrap();
    write_parquet(&dir, "", df);
    let path = dir.join("data.parquet");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let schema = app.data_table_state.as_ref().unwrap().schema().clone();
    assert!(matches!(schema.get("s"), Some(DataType::Categorical(..))));
    assert!(matches!(
        schema.get("t"),
        Some(DataType::Datetime(_, Some(_)))
    ));

    for key in ["f", "d", "t", "s", "f, s", "day: d, late: t > 5"] {
        app.event(AppEvent::QQuery(format!("select n: count v by {key}")));
        pump_until_idle(&mut app, &rx, &tx);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.error().is_none(), "{key}: {:?}", state.error());
        let counts: Vec<u32> = state
            .lf()
            .clone()
            .collect()
            .unwrap()
            .column("n")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        for (group, counted) in counts.into_iter().enumerate() {
            state.drill_down_into_group(group).unwrap();
            let rows = state.lf().clone().collect().unwrap().height();
            assert_eq!(rows as u32, counted, "by {key}, group {group}");
            state.drill_up().unwrap();
        }
    }
}

/// Enter on an aggregate reads the row's keys from the rows on screen, so the drill
/// happens at once without computing the aggregate again. With a key column hidden
/// the row is read off the UI thread, and the drill lands when it comes back.
#[test]
fn test_enter_on_an_aggregate_drills_from_the_buffer_or_reads_the_row() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_from_buffer.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(AppEvent::QQuery(
        "select n: count a, total: sum a by c".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);

    press(&mut app, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        state.is_drilled_down(),
        "drilled before any event is pumped"
    );
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["1".to_string()])
    );
    pump_until_idle(&mut app, &rx, &tx);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);

    // Hide the key: the buffer no longer holds it.
    app.data_table_state
        .as_mut()
        .unwrap()
        .set_column_order(vec!["n".to_string(), "total".to_string()]);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert!(app.is_busy(), "reading the row");
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["1".to_string()])
    );
    assert!(on_screen(&app, "c").iter().all(|c| c == "1"));
}

/// A sort on the aggregate reorders its rows; Enter drills into the row on screen, not
/// the one that was there before the sort.
#[test]
fn test_drill_from_a_sorted_aggregate_takes_the_row_on_screen() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sorted_aggregate.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(AppEvent::QQuery("select n: count a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(AppEvent::Sort(vec!["c".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "c"), ["2", "1", "0"]);

    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["2".to_string()])
    );
    assert!(on_screen(&app, "c").iter().all(|c| c == "2"));
}

/// Columns hidden and frozen inside a drill belong to the drill: Esc puts back the
/// grouped view's own column order and frozen key.
#[test]
fn test_esc_restores_the_grouped_columns_changed_inside_the_drill() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_columns_restored.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(AppEvent::QQuery(
        "select n: count a, total: sum a by c".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);

    let state = app.data_table_state.as_mut().unwrap();
    state.set_column_order(vec!["name".to_string(), "a".to_string()]);
    state.set_locked_columns(2);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["c", "n", "total"]);
    assert_eq!(state.locked_columns_count(), 1);
    assert_eq!(on_screen(&app, "total"), ["1683", "1617", "1650"]);
}

/// A list-form group whose key is null drills into rows whose key is null, not the
/// text "null"; a list form that also aggregates names only its keys in the breadcrumb.
#[test]
fn test_drill_from_lists_keeps_a_null_key_and_names_only_keys() {
    let csv = "k,v\nx,1\n,2\ny,3\n,4\n";
    let (mut app, rx, tx) = open_csv_with("drill_list_null_key.csv", csv, OpenOptions::default());
    app.event(AppEvent::QQuery("select v, n: count v by k".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.is_grouped());
    // Nulls sort last.
    state.drill_down_into_group(2).unwrap();
    assert_eq!(
        state
            .drilled_group_key()
            .map(|(columns, _)| columns.to_vec()),
        Some(vec!["k".to_string()])
    );
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(df.column("k").unwrap().null_count(), 2);
    assert_eq!(df.column("k").unwrap().dtype(), &DataType::String);
}

/// Enter where there is nothing to drill into opens the row inspector, as Space does;
/// on a `by` view Enter drills and the footer says `Enter Drill`; inside the group,
/// where there is nothing further, it inspects again, and the footer offers the way
/// back. Esc closes.
#[test]
fn test_enter_inspects_where_there_is_nothing_to_drill_into() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_nothing.csv");
    let area = Rect::new(0, 0, 220, 30);
    let plain = painted(&mut app, &rx, &tx, area);
    assert!(!plain.contains("Enter Drill"), "{plain}");
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(app.overlay, Overlay::Inspect);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());
    assert_ne!(app.overlay, Overlay::Inspect);

    app.event(AppEvent::QQuery("select n: count a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let grouped = painted(&mut app, &rx, &tx, area);
    assert!(
        grouped.contains("Enter Drill"),
        "Enter drills here: {grouped}"
    );
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.at_table(), "Enter drilled");
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());

    let inside = painted(&mut app, &rx, &tx, area);
    assert!(inside.contains("Esc Back"), "{inside}");
    assert!(!inside.contains("Drill"), "{inside}");
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(app.overlay, Overlay::Inspect);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    assert!(app.at_table());
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
}

/// The acceptance case: Enter on a department of a SQL `GROUP BY` shows that
/// department's rows that passed the `WHERE`, key first, and Esc brings the grouped
/// rows back with the cursor on the department.
#[cfg(feature = "sql")]
#[test]
fn test_sql_group_by_drills_into_rows_after_where() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_where");
    let area = Rect::new(0, 0, 100, 30);
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, AVG(salary) AS avg_salary FROM df WHERE salary > 100000 GROUP BY dept",
    );
    painted(&mut app, &rx, &tx, area);
    let depts = on_screen(&app, "dept");
    assert_eq!(depts.len(), 4, "eng, ops, sales and null: {depts:?}");
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        1,
        "the key is frozen, as a `by` key is"
    );

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec![depts[1].clone()])
    );
    assert_eq!(state.headers(), ["dept", "id", "salary", "ts"]);
    let df = state.lf().clone().collect().unwrap();
    let salaries = df.column("salary").unwrap().i64().unwrap();
    assert!(salaries.into_no_null_iter().all(|s| s > 100_000));
    let expected = (0..40i64)
        .filter(|i| 60_000 + i * 5_000 > 100_000)
        .filter(|i| {
            ["eng", "ops", "sales"]
                .get((i % 4) as usize)
                .map_or("null", |d| d)
                == depts[1]
        })
        .count();
    assert_eq!(df.height(), expected);
    assert!(on_screen(&app, "dept").iter().all(|d| *d == depts[1]));

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(state.headers(), ["dept", "avg_salary"]);
    assert_eq!(on_screen(&app, "dept"), depts);
    assert_eq!(state.table_state.selected(), Some(1));
}

/// Every group of a SQL aggregate drills into as many rows as it counted, whatever the
/// key: a null, a renamed column, a computed key named by its alias, by its expression
/// or by ordinal, several keys, and with HAVING, ORDER BY and LIMIT on the result.
#[cfg(feature = "sql")]
#[test]
fn test_sql_group_by_drills_by_null_computed_and_aliased_keys() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_keys");
    for sql in [
        "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept",
        "SELECT COUNT(*) AS n, dept AS d FROM df GROUP BY dept",
        "SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM df GROUP BY h",
        "SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM df GROUP BY EXTRACT(HOUR FROM ts)",
        "SELECT salary > 150000 AS high, COUNT(*) AS n FROM df GROUP BY 1",
        "SELECT dept, id % 2 = 0 AS even, COUNT(*) AS n FROM df GROUP BY dept, even",
        "SELECT dept, COUNT(*) AS n FROM df WHERE id > 5 GROUP BY dept \
         HAVING COUNT(*) > 3 ORDER BY n DESC, dept LIMIT 3",
        "SELECT t.dept, COUNT(*) AS n FROM df AS t WHERE t.salary < 200000 GROUP BY t.dept",
    ] {
        run_sql(&mut app, &rx, &tx, sql);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.can_drill_down(), "{sql}");
        let counts: Vec<u32> = state
            .lf()
            .clone()
            .collect()
            .unwrap()
            .column("n")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(!counts.is_empty(), "{sql}");
        for (group, counted) in counts.into_iter().enumerate() {
            state.drill_down_into_group(group).unwrap();
            let rows = state.lf().clone().collect().unwrap();
            assert_eq!(rows.height() as u32, counted, "{sql}, group {group}");
            assert!(
                rows.get_column_names()
                    .iter()
                    .all(|c| !c.starts_with("__datui")),
                "{sql}: no scratch columns"
            );
            state.drill_up().unwrap();
        }
    }
}

/// A null key drills into the rows whose key is null, and a computed key into the
/// rows that compute it, with the source's own columns.
#[cfg(feature = "sql")]
#[test]
fn test_sql_group_by_null_and_computed_key_rows() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_null");
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept ORDER BY dept NULLS LAST",
    );
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(3).unwrap();
    assert_eq!(
        state
            .drilled_group_key()
            .map(|(columns, _)| columns.to_vec()),
        Some(vec!["dept".to_string()])
    );
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("dept").unwrap().null_count(), 10);
    state.drill_up().unwrap();

    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT EXTRACT(HOUR FROM ts) AS h, SUM(salary) AS total FROM df \
         GROUP BY h ORDER BY h",
    );
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(3).unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["3".to_string()])
    );
    assert_eq!(state.headers(), ["id", "dept", "salary", "ts"]);
    let df = state.lf().clone().collect().unwrap();
    let ids: Vec<i64> = df
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(ids, [3, 27], "hour 3");
}

/// `ARRAY_AGG` lists are values the statement computed, not the group's rows: a drill
/// still shows the source rows.
#[cfg(feature = "sql")]
#[test]
fn test_sql_group_by_with_lists_drills_into_source_rows() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_lists");
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, ARRAY_AGG(id) AS ids, MAX(salary) AS top FROM df \
         WHERE dept IS NOT NULL GROUP BY dept ORDER BY dept",
    );
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.is_grouped(), "a GROUP BY");
    assert!(!app.enter_inspects(), "Enter drills");
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(0).unwrap();
    assert_eq!(state.headers(), ["dept", "id", "salary", "ts"]);
    assert_eq!(state.lf().clone().collect().unwrap().height(), 10);
}

/// A statement whose rows cannot be traced back reliably does not drill: Enter
/// inspects the row instead.
#[cfg(feature = "sql")]
#[test]
fn test_sql_shapes_without_a_source_do_not_drill() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_unsupported");
    let area = Rect::new(0, 0, 100, 30);
    for sql in [
        "SELECT dept, salary FROM df",
        "SELECT dept, COUNT(*) AS n FROM df \
         WHERE dept IN (SELECT dept FROM df WHERE salary > 200000) GROUP BY dept",
        "SELECT a.dept, COUNT(*) AS n FROM df AS a JOIN df AS b ON a.id = b.id GROUP BY a.dept",
        "WITH t AS (SELECT * FROM df) SELECT dept, COUNT(*) AS n FROM t GROUP BY dept",
        "SELECT AVG(salary) AS avg FROM df GROUP BY dept",
    ] {
        run_sql(&mut app, &rx, &tx, sql);
        painted(&mut app, &rx, &tx, area);
        assert!(
            !app.data_table_state.as_ref().unwrap().can_drill_down(),
            "{sql}"
        );
        press_and_send(&mut app, &tx, KeyCode::Enter);
        assert_eq!(app.overlay, Overlay::Inspect, "{sql}");
        press_and_send(&mut app, &tx, KeyCode::Esc);
        assert!(app.at_table(), "{sql}");
        assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
    }
}

/// Enter on a grouped result with no rows says there is nothing to drill into rather
/// than doing nothing.
#[cfg(feature = "sql")]
#[test]
fn test_enter_on_an_empty_sql_group_by_flashes() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_empty");
    let area = Rect::new(0, 0, 100, 30);
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, COUNT(*) AS n FROM df WHERE salary < 0 GROUP BY dept",
    );
    painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 0);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.flash_message(), Some("No group to drill down into"));
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
}

/// Polars returns groups in any order. A grouping without ORDER BY comes back sorted by
/// its keys, as a `by` result does, so each read of it (a page, the count, Esc from a
/// drill) shows the same rows in the same places; ORDER BY is left as written.
#[cfg(feature = "sql")]
#[test]
fn test_sql_group_by_without_order_by_is_sorted_by_its_keys() {
    let (mut app, rx, tx) = open_salary_fixture("sql_group_order");
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT EXTRACT(HOUR FROM ts) AS h, dept, COUNT(*) AS n FROM df GROUP BY dept, h",
    );
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    let sorted = df
        .sort(
            ["h", "dept"],
            SortMultipleOptions::default().with_nulls_last(true),
        )
        .unwrap();
    assert!(df.equals_missing(&sorted), "{df}");
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        2,
        "both keys lead, so both are frozen"
    );

    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept ORDER BY dept DESC NULLS FIRST",
    );
    assert_eq!(
        current_rows(&app),
        4,
        "the order as written: {:?}",
        app.data_table_state
            .as_ref()
            .unwrap()
            .lf()
            .clone()
            .collect()
    );
    let first = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("dept")
        .unwrap()
        .get(0)
        .unwrap()
        .is_null();
    assert!(first, "ORDER BY is kept");
}

/// SQL inside a drill-down runs on the group, like the sidebar does, not on the whole
/// loaded table.
#[cfg(feature = "sql")]
#[test]
fn test_sql_inside_a_drill_down_stays_in_the_group() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sql.csv");

    app.event(AppEvent::QQuery("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_down_into_group(0)
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 34);

    app.event(AppEvent::SqlQuery(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    assert_eq!(
        current_rows(&app),
        10,
        "a in 0, 3, ..., 27: within the group"
    );
}

/// Phase 2 promised that aggregations show nulls: a column some files were written
/// without must read as null everywhere it is absent, not vanish from the aggregate and
/// not stop it. Describe is the aggregation a user reaches for first, so this asserts on
/// the panel it paints — the `Nulls` figure beside `extra` — rather than on the results
/// struct behind it. `extra` is in two of the three files, so it counts two values and
/// one null, and that one null is an absence: no file wrote a null into `extra`.
#[test]
fn test_an_aggregation_counts_an_absent_column_as_null() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "extra" => &["y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 120, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    // `a` opens the analysis modal; Enter on Describe, where the sidebar starts,
    // shows its Sample form, and Enter again runs it.
    if let Some(next) = app.event(key(KeyCode::Char('a'))) {
        let _ = tx.send(next);
    }
    app.event(key(KeyCode::Enter));
    if let Some(next) = app.event(key(KeyCode::Enter)) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = common::buffer_lines(&buf);

    // That the Describe table is the thing on screen, before reading figures off it.
    // Without this the fallback is the data table, whose header also begins with a
    // column name, and the failure would be about the wrong screen.
    assert!(
        rows.iter()
            .any(|line| line.contains("Count") && line.contains("Nulls")),
        "Describe should be on screen with its Count and Nulls columns; got:\n{}",
        rows.join("\n")
    );
    let extra = rows
        .iter()
        .find(|line| line.trim_start().starts_with("extra"))
        .unwrap_or_else(|| {
            panic!(
                "describe should list `extra`, the column two of the three files have; got:\n{}",
                rows.join("\n")
            )
        });
    // `skip(1)` steps over the column name, which this fixture keeps to a single token
    // on purpose: a name with a space in it would put its second half where Count is.
    // The row also runs into the sidebar at the right, which is harmless while only the
    // first two figures are read.
    let figures: Vec<&str> = extra.split_whitespace().skip(1).collect();
    assert_eq!(
        figures.first().copied(),
        Some("2"),
        "two files wrote `extra`, so it counts two values; row was {extra:?}"
    );
    assert_eq!(
        figures.get(1).copied(),
        Some("1"),
        "the third file was written without `extra`, and that absence counts as a null \
         in the aggregate; row was {extra:?}"
    );
}

/// Once a filter is being typed, `?` is an ordinary character again — a help
/// key that ate letters would break "searching for anything with a ? in it",
/// and, more importantly, the promise that typing always filters.
#[test]
fn home_question_mark_types_into_a_started_filter() {
    common::isolate_cache();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.filter = "sal".to_string();

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert!(!app.help_visible());
    assert_eq!(app.home.filter, "sal?");
}

/// F1 opens home help even while the filter has text, and closing it leaves
/// the filter as typed.
#[test]
fn home_f1_opens_help_mid_filter() {
    common::isolate_cache();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.filter = "sal".to_string();

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::F(1),
        KeyModifiers::NONE,
    )));
    assert!(app.help_visible(), "F1 opens help mid-filter");

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.help_visible());
    assert_eq!(app.home.filter, "sal", "the filter survives the overlay");
}

/// The Columns list is a list: PgUp/PgDn page it, Home/End reach its ends, ↓
/// stops at the last column, and the rows out of view are counted above and below.
#[test]
fn test_sort_filter_columns_list_pages_and_counts_both_ends() {
    use datui::sort_filter_modal::SortFilterField;

    let path = common::fixture_dir().join("columns_52.csv");
    let header: Vec<String> = (0..52).map(|i| format!("col_{i}")).collect();
    let row: Vec<String> = (0..52).map(|i| i.to_string()).collect();
    std::fs::write(&path, format!("{}\n{}\n", header.join(","), row.join(","))).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());

    open_columns_list(&mut app);
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Column(0));
    let g = datui::glyphs::get();
    let screen = rows_at(&mut app, 80, 24).join("\n");
    assert!(
        screen.contains(&format!("{} ", g.ellipsis)),
        "below counted: {screen}"
    );

    press(&mut app, KeyCode::End);
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Column(51));
    press(&mut app, KeyCode::Down);
    assert_eq!(
        app.sort_filter_modal.focus,
        SortFilterField::Column(51),
        "↓ stops at the last column"
    );
    let rows = rows_at(&mut app, 80, 24);
    let cursor = rows.iter().position(|r| r.contains("col_51")).unwrap();
    let above = rows.iter().position(|r| r.contains(" more")).unwrap();
    assert!(above < cursor, "the count above: {}", rows.join("\n"));

    press(&mut app, KeyCode::Home);
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Column(0));
    press(&mut app, KeyCode::PageDown);
    let SortFilterField::Column(paged) = app.sort_filter_modal.focus else {
        panic!("still in the list");
    };
    assert!(paged > 1, "a page: {paged}");
    press(&mut app, KeyCode::PageUp);
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Column(0));
}

/// An edit staged in the Sort & Filter modal and then canceled dies with the modal:
/// reopening `s` rebuilds it from the table's applied state, so nothing arrives
/// pre-staged and Apply commits nothing stale.
#[test]
fn test_sort_filter_esc_discards_staged_edits() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_filter_esc_discards.csv");
    let headers_before = app.data_table_state.as_ref().unwrap().headers();

    // Open the modal, walk to the column list, and hide the first column — staged only.
    open_columns_list(&mut app);
    assert_eq!(app.overlay, Overlay::SortFilter);
    press(&mut app, KeyCode::Down); // select the first column
    press(&mut app, KeyCode::Char('v'));
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .any(|c| !c.is_visible),
        "the toggle staged a hidden column"
    );

    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::SortFilter);

    // Reopen: the canceled hide is gone and nothing is staged as sorted.
    press(&mut app, KeyCode::Char('s'));
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.is_visible),
        "a canceled hide must not be staged on reopen"
    );
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.sort_order.is_none())
    );

    // And Enter straight after applies and commits nothing.
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.headers(),
        headers_before,
        "Apply after a canceled edit changes nothing"
    );
    assert!(state.view_sort_columns().is_empty());
}

/// The other half of the same contract: what IS applied comes back staged. A hide
/// that was applied shows as hidden on reopen, and an applied sort shows its order.
#[test]
fn test_sort_filter_reopen_reflects_applied_state() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_filter_reopen.csv");

    // Hide the first column and apply.
    open_columns_list(&mut app);
    // The list opens with the first column ("a") already under the cursor.
    press(&mut app, KeyCode::Char('v'));
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), vec!["c".to_string(), "name".to_string()]);

    // Sort by "c", applied through the event the modal would send.
    app.event(AppEvent::Sort(vec!["c".to_string()], vec![false]));
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('s'));
    let staged = &app.sort_filter_modal.sort.columns;
    let a = staged.iter().find(|c| c.name == "a").unwrap();
    assert!(!a.is_visible, "the applied hide arrives staged");
    let c = staged.iter().find(|c| c.name == "c").unwrap();
    assert!(c.is_visible);
    assert_eq!(c.sort_order, Some(1), "the applied sort arrives staged");
}

/// Sort & Filter: `v` is visibility only (#379). A column hidden and shown again
/// returns to its place, in the same session or after the hide was applied, and
/// hiding never reorders the list under the cursor.
#[test]
fn showing_a_hidden_column_puts_it_back_in_place() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_unhide_in_place.csv");
    let abc = |names: &[&str]| names.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };
    let open_list = |app: &mut App| {
        open_columns_list(app);
    };

    // Hide c and show it again before applying: nothing moves.
    open_list(&mut app);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["a", "c", "name"])
    );

    // Hide c and apply; reopened, it is still listed second and comes back there.
    // The sidebar opens on the column cursor's column, a.
    open_list(&mut app);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["a", "name"])
    );
    open_list(&mut app);
    let modal = &app.sort_filter_modal.sort;
    let under_cursor = modal.filtered_columns()[modal.table_state.selected().unwrap()]
        .1
        .name
        .clone();
    assert_eq!(under_cursor, "a", "the sidebar opens on the column cursor");
    assert_eq!(
        modal.filtered_columns()[1].1.name,
        "c",
        "the hidden column keeps its row"
    );
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["a", "c", "name"])
    );

    // Hiding a leaves the rows where they were, so Down v hides c.
    open_list(&mut app);
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["name"])
    );
}

/// Columns sidebar width controls (#462): a fit is staged, discarded by Esc,
/// applied by Enter to the page on screen without moving the view, and kept
/// through paging and a resize; `>` and `<` step from the width drawn, `w` returns
/// to automatic, and R puts every column back.
#[test]
fn column_widths_from_the_sidebar() {
    use datui::widgets::column_widths::{WIDTH_STEP, WidthChoice};
    let url = format!("https://example.com/{}", "long-segment/".repeat(15));
    let csv_path = common::fixture_dir().join("sidebar_column_widths.csv");
    let n = 80usize;
    let mut df = df!(
        "id" => (0..n as i64).collect::<Vec<_>>(),
        "description" => (0..n)
            .map(|i| if i == 24 { url.clone() } else { format!("item {i}") })
            .collect::<Vec<_>>(),
        "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
        "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
    )
    .unwrap();
    CsvWriter::new(&mut File::create(&csv_path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    // As the main loop draws: a frame that changes the rows on screen reads them,
    // and the next frame shows them.
    let draw = |app: &mut App, width: u16, height: u16| {
        app.event(AppEvent::Resize(width, height));
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let state = app.data_table_state.as_mut().unwrap();
        if std::mem::take(&mut state.needs_recollect) {
            app.spawn_async_collect("Loading buffer...");
            pump_until_idle(app, &rx, &tx);
            app.render(area, &mut buf);
        }
        common::buffer_text(&buf)
    };
    let on_column = |app: &mut App, name: &str| {
        open_columns_list(app);
        let sort = &mut app.sort_filter_modal.sort;
        let row = sort
            .filtered_columns()
            .iter()
            .position(|(_, c)| c.name == name)
            .unwrap();
        sort.table_state.select(Some(row));
    };
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };
    let choice = |app: &App, name: &str| app.data_table_state.as_ref().unwrap().width_choice(name);

    draw(&mut app, 100, 24);
    press_and_send(&mut app, &tx, KeyCode::PageDown);
    pump_until_idle(&mut app, &rx, &tx);
    let paged = draw(&mut app, 100, 24);
    // Cut to the cap, eleven cells with the ellipsis: `https://ex…`, or
    // `https://...` in ASCII.
    let ellipsis = datui::glyphs::get().ellipsis;
    let cut = format!(
        "{}{ellipsis}",
        &"https://ex"[..11 - datui::glyphs::display_width(ellipsis)]
    );
    assert!(paged.contains(&cut), "{paged}");
    let start = app.data_table_state.as_ref().unwrap().start_row();
    assert!(start > 0);

    // A narrow sidebar gives the names the room until a column has a width to show.
    let heading = |text: &str| -> String {
        text.lines()
            .find(|l| l.contains("Lock") && l.contains("Column"))
            .unwrap()
            .to_string()
    };
    on_column(&mut app, "description");
    let narrow = draw(&mut app, 60, 20);
    assert!(!heading(&narrow).contains("Width"), "{narrow}");
    assert!(narrow.contains("description "), "{narrow}");
    press(&mut app, KeyCode::Char('f'));
    let narrow = draw(&mut app, 60, 20);
    assert!(heading(&narrow).contains("Width"), "{narrow}");
    press(&mut app, KeyCode::Esc);

    // Staged, shown in the list, and gone with Esc.
    on_column(&mut app, "description");
    press(&mut app, KeyCode::Char('f'));
    assert!(app.sort_filter_modal.sort.has_unapplied_changes);
    let staged = draw(&mut app, 100, 24);
    // The list row ends in the staged width.
    let listed = staged.match_indices("description").any(|(at, _)| {
        let rest: String = staged[at..].chars().take(40).collect();
        rest.contains("fit")
    });
    assert!(listed, "{staged}");
    press(&mut app, KeyCode::Esc);
    assert_eq!(choice(&app, "description"), WidthChoice::Auto);

    // Applied: fitted to this page, which stays on screen.
    on_column(&mut app, "description");
    press(&mut app, KeyCode::Char('f'));
    apply(&mut app);
    let fitted = draw(&mut app, 100, 24);
    let url_width = u16::try_from(url.len()).unwrap();
    assert_eq!(choice(&app, "description"), WidthChoice::Manual(url_width));
    assert_eq!(app.data_table_state.as_ref().unwrap().start_row(), start);
    assert!(
        fitted.contains("https://example.com/long-segment/long-segment/"),
        "{fitted}"
    );

    // Kept through paging and a resize.
    press_and_send(&mut app, &tx, KeyCode::PageDown);
    pump_until_idle(&mut app, &rx, &tx);
    draw(&mut app, 60, 20);
    draw(&mut app, 100, 24);
    assert_eq!(choice(&app, "description"), WidthChoice::Manual(url_width));

    // w is automatic again; wider and narrower step from the width on screen, the
    // room the last column fills included.
    on_column(&mut app, "description");
    press(&mut app, KeyCode::Char('w'));
    apply(&mut app);
    assert_eq!(choice(&app, "description"), WidthChoice::Auto);
    draw(&mut app, 100, 24);
    let shown = app
        .data_table_state
        .as_ref()
        .unwrap()
        .on_screen_width("status")
        .unwrap();
    on_column(&mut app, "status");
    press(&mut app, KeyCode::Char('>'));
    press(&mut app, KeyCode::Char('.'));
    press(&mut app, KeyCode::Char('<'));
    apply(&mut app);
    assert_eq!(
        choice(&app, "status"),
        WidthChoice::Manual(shown + WIDTH_STEP)
    );
    let wider = draw(&mut app, 100, 24);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().shown_width("status"),
        Some(shown + WIDTH_STEP),
        "{wider}"
    );

    // R resets every column to automatic.
    press_and_send(&mut app, &tx, KeyCode::Char('R'));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(choice(&app, "status"), WidthChoice::Auto);
}

/// `cell_padding` names its densities: `"compact"` puts one cell between
/// columns, `"comfortable"` (the default) two, and a number that many.
#[test]
fn named_padding_spaces_the_table() {
    use datui::config::{AppConfig, ConfigLayer};
    for (setting, gap) in [("\"compact\"", 1), ("\"comfortable\"", 2), ("3", 3)] {
        let layer = ConfigLayer::parse(&format!("[display]\ncell_padding = {setting}\n")).unwrap();
        let config = AppConfig::from_layers([layer]).unwrap();
        let (mut app, _rx, _tx) =
            open_query_filter_fixture_with("named_padding_spaces_the_table.csv", config);
        let area = Rect::new(0, 0, 60, 10);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let types: String = (0..area.width).map(|x| buf[(x, 1)].symbol()).collect();
        let gap = " ".repeat(gap);
        assert!(
            types.contains(&format!("i64{gap}i64{gap}str")),
            "{setting}: {types:?}"
        );
    }
}

/// Sort & Filter (#379): a column hidden after the sidebar reordered the table
/// comes back after the column it followed there, not where the file has it; and
/// hiding the last frozen column keeps its lock for when it is shown again.
#[test]
fn a_hidden_column_returns_to_the_applied_order_and_lock() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_unhide_applied_order.csv");
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };
    let open_list = |app: &mut App| {
        open_columns_list(app);
    };
    let select = |app: &mut App, name: &str| {
        let sort = &mut app.sort_filter_modal.sort;
        let row = sort
            .filtered_columns()
            .iter()
            .position(|(_, c)| c.name == name)
            .unwrap();
        sort.table_state.select(Some(row));
    };

    // name moves to the front, name and a freeze, then a is hidden.
    open_list(&mut app);
    select(&mut app, "name");
    press(&mut app, KeyCode::Char('+'));
    press(&mut app, KeyCode::Char('+'));
    select(&mut app, "a");
    press(&mut app, KeyCode::Char('L'));
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["name", "c"]);
    assert_eq!(state.locked_columns_count(), 1);

    open_list(&mut app);
    assert_eq!(
        app.sort_filter_modal.sort.get_full_column_order(),
        ["name", "a", "c"],
        "a is listed after name, where it was applied"
    );
    select(&mut app, "a");
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["name", "a", "c"]);
    assert_eq!(state.locked_columns_count(), 2, "a is frozen again");
}

/// Each column of a multi-sort runs its own way: Space cycles one column
/// none → ascending → descending, Enter applies from the list, and the header
/// carries each column's own mark. `r` still reverses the whole view.
#[test]
fn test_sort_filter_per_column_directions_reach_the_table() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_per_column.csv");

    open_columns_list(&mut app);
    press(&mut app, KeyCode::Char(' ')); // ascending
    press(&mut app, KeyCode::Char(' ')); // descending
    press(&mut app, KeyCode::Down); // c
    press(&mut app, KeyCode::Char(' ')); // ascending
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::SortFilter, "Enter applies and closes");

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.view_sort_columns(),
        ["a".to_string(), "c".to_string()]
    );
    assert_eq!(state.view_sort_descending(), [true, false]);
    let df = state.lf().clone().collect().unwrap();
    let first = df.column("a").unwrap().get(0).unwrap();
    assert_eq!(first, AnyValue::Int64(99), "a runs descending");

    // The header says which way each column runs.
    let g = datui::glyphs::get();
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let header: String = (0..area.width)
        .map(|x| buf[(x, 0)].symbol().to_string())
        .collect();
    assert!(
        header.contains(&format!("a{}", g.sort_desc)),
        "got {header:?}"
    );
    assert!(
        header.contains(&format!("c{}", g.sort_asc)),
        "got {header:?}"
    );

    // `r` flips every direction at once.
    press(&mut app, KeyCode::Char('r'));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.view_sort_descending(), [false, true]);
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(0));
}

/// Filters are driven entirely from the keyboard: Space on the add row opens the
/// editor, typing narrows the column Picker, Enter walks the steps, Enter applies,
/// and Del deletes the statement under focus.
#[test]
fn test_filter_editor_keyboard_flow() {
    let (mut app, rx, tx) = open_query_filter_fixture("filter_editor_flow.csv");

    start_new_filter(&mut app);
    assert!(app.sort_filter_modal.filter.editor.is_some());
    for ch in "na".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows to "name"
    }
    press(&mut app, KeyCode::Enter); // choose the column
    for ch in "co".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows operators to "contains"
    }
    press(&mut app, KeyCode::Enter); // choose the operator
    for ch in "alpha".chars() {
        press(&mut app, KeyCode::Char(ch)); // the value
    }
    press(&mut app, KeyCode::Enter); // commit the statement
    assert!(app.sort_filter_modal.filter.editor.is_none());
    assert_eq!(app.sort_filter_modal.filter.statements.len(), 1);

    // Enter applies from any row.
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::SortFilter);
    assert_eq!(current_rows(&app), 50, "only the alpha_ rows remain");

    // Reopen: the statement is staged; Del deletes it; Ctrl+Enter clears the filter.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Down); // add sort -> the statement
    press(&mut app, KeyCode::Delete); // Del deletes like d
    assert!(app.sort_filter_modal.filter.statements.is_empty());
    let apply = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::CONTROL,
    )));
    if let Some(next) = apply {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 100);
}

/// The filter row under edit gives the rail to its open picker's line, so one
/// rail is on screen at every step; typing the value, with no picker, the row
/// has it back.
#[test]
fn test_filter_editor_keeps_one_rail() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filter_editor_one_rail.csv");
    let rail = datui::glyphs::get().rail;
    // Past the table's own rail, in its first column.
    let rails = |screen: &str| {
        screen
            .lines()
            .map(|line| {
                line.chars()
                    .skip(1)
                    .collect::<String>()
                    .matches(rail)
                    .count()
            })
            .sum::<usize>()
    };
    start_new_filter(&mut app);
    let column_step = draw_sized(&mut app, (80, 24));
    assert_eq!(rails(&column_step), 1, "{column_step}");
    type_text(&mut app, "na");
    press(&mut app, KeyCode::Enter);
    let operator_step = draw_sized(&mut app, (80, 24));
    assert_eq!(rails(&operator_step), 1, "{operator_step}");
    type_text(&mut app, "co");
    press(&mut app, KeyCode::Enter);
    let value_step = draw_sized(&mut app, (80, 24));
    assert_eq!(rails(&value_step), 1, "{value_step}");
}

/// Ctrl+J applies mid-edit on every terminal, committing the row in progress,
/// and the editor's footer names it.
#[test]
fn test_ctrl_j_applies_from_the_filter_editor() {
    let (mut app, rx, tx) = open_query_filter_fixture("filter_editor_ctrl_j.csv");

    start_new_filter(&mut app);
    for ch in "na".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Enter); // the column
    for ch in "co".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Enter); // the operator
    for ch in "alpha".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    let screen = screen_text(&mut app);
    assert!(screen.contains("^J") && screen.contains("Apply"));

    if let Some(next) = press_ctrl(&mut app, 'j') {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::SortFilter);
    assert_eq!(current_rows(&app), 50, "the row in progress was applied");
}

/// In the filter editor's column and operator steps, Space chooses like
/// Enter instead of typing into the narrow filter, where a space matches
/// nothing and blanks the list. The value field below keeps Space for typing.
#[test]
fn test_space_chooses_in_the_filter_editor_steps() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filter_editor_space.csv");

    start_new_filter(&mut app);
    for ch in "na".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows to "name"
    }
    press(&mut app, KeyCode::Char(' ')); // chooses the column, like Enter
    for ch in "co".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows operators to "contains"
    }
    press(&mut app, KeyCode::Char(' ')); // chooses the operator
    for ch in "al pha".chars() {
        press(&mut app, KeyCode::Char(ch)); // the value types spaces as text
    }
    press(&mut app, KeyCode::Enter); // commit the statement
    assert!(app.sort_filter_modal.filter.editor.is_none());
    let statement = &app.sort_filter_modal.filter.statements[0];
    assert_eq!(statement.column, "name");
    assert_eq!(statement.value, "al pha");
}

/// Del on the Columns list is the sort's delete: the column leaves the sort
/// outright, wherever in the Space cycle it stands, and the rest renumber.
#[test]
fn test_del_removes_a_column_from_the_sort() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("del_unsorts.csv");

    open_columns_list(&mut app);
    press(&mut app, KeyCode::Char(' ')); // 1, ascending
    press(&mut app, KeyCode::Down); // c
    press(&mut app, KeyCode::Char(' ')); // 2
    press(&mut app, KeyCode::Char(' ')); // descending
    press(&mut app, KeyCode::Up); // back to a
    press(&mut app, KeyCode::Delete);

    let (names, directions) = app.sort_filter_modal.sort.sorted_columns_and_directions();
    assert_eq!(names, ["c"], "a left the sort and c renumbered to 1");
    assert_eq!(directions, [true], "keeping its own direction");
}

/// The first filter is three keys away: s, ↓, Space. With nothing in effect the
/// sidebar opens on "add sort", and arrows move from the moment it opens.
#[test]
fn test_the_first_filter_is_s_down_space() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filters_tab_bar_enter.csv");
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(
        app.sort_filter_modal.focus,
        datui::sort_filter_modal::SortFilterField::AddSort
    );
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.overlay, Overlay::SortFilter, "the sidebar stays open");
    assert!(
        app.sort_filter_modal.filter.editor.is_some(),
        "and the editor is up, on the add row"
    );
}

/// Esc backs out one layer at a time: an open editor dies alone, the sidebar
/// survives it, and the next Esc discards the staged edit with the sidebar.
#[test]
fn test_filter_editor_esc_ends_the_edit_not_the_sidebar() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filter_editor_esc.csv");

    start_new_filter(&mut app);
    press(&mut app, KeyCode::Char('n'));
    assert!(app.sort_filter_modal.filter.editor.is_some());

    press(&mut app, KeyCode::Esc);
    assert!(
        app.sort_filter_modal.filter.editor.is_none(),
        "the edit dies"
    );
    assert_eq!(app.overlay, Overlay::SortFilter, "the sidebar does not");
    assert!(app.sort_filter_modal.filter.statements.is_empty());

    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::SortFilter);
}

/// The sidebar is one Surface: one border, no bordered buttons, the actions in
/// the footer, and no radio glyphs anywhere.
#[test]
fn test_sort_filter_sidebar_is_one_surface() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("sort_filter_surface.csv");
    press(&mut app, KeyCode::Char('s'));

    let area = Rect::new(0, 0, 100, 28);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = common::buffer_lines(&buf);

    assert!(rows.iter().any(|r| r.contains("Sort & Filter")));
    let frames = common::frame_bottoms(&rows);
    assert_eq!(
        frames.len(),
        1,
        "one border on the sidebar and none inside it: {rows:#?}"
    );
    let bottom = frames[0];
    for (key, label) in [("Enter", "Apply"), ("Esc", "Cancel")] {
        assert!(
            rows[bottom - 1].contains(key) && rows[bottom - 1].contains(label),
            "{key} {label} is a chip on the footer row: {:?}",
            rows[bottom - 1]
        );
    }
    for radio in [
        datui::glyphs::unicode().radio_on,
        datui::glyphs::unicode().radio_off,
    ] {
        assert!(
            rows.iter().all(|r| !r.contains(radio)),
            "a radio glyph survived: {radio:?}"
        );
    }
}

/// Esc backs out one layer: an open picker first, then the dialog.
#[test]
fn esc_closes_the_picker_then_the_dialog() {
    use datui::pivot_melt_modal::PivotMeltFocus;
    let (mut app, _rx, _tx) = open_query_filter_fixture("forms_esc_order.csv");
    press(&mut app, KeyCode::Char('p'));
    press(&mut app, KeyCode::Down);
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::PivotIndex);
    press(&mut app, KeyCode::Char(' '));
    assert!(
        app.pivot_melt_modal.picker.is_some(),
        "Space opens the picker"
    );
    press(&mut app, KeyCode::Esc);
    assert!(app.pivot_melt_modal.picker.is_none());
    assert_eq!(app.overlay, Overlay::PivotMelt, "the dialog survives");
    press(&mut app, KeyCode::Esc);
    assert_ne!(app.overlay, Overlay::PivotMelt);
}

/// A several-choice picker keeps its level: Enter keeps the toggles made in it, Esc
/// undoes them, as Esc discards the innermost level everywhere.
#[test]
fn esc_in_a_toggle_picker_undoes_its_toggles() {
    use datui::pivot_melt_modal::PivotMeltFocus;
    let (mut app, _rx, _tx) = open_query_filter_fixture("forms_picker_toggles.csv");
    press(&mut app, KeyCode::Char('p'));
    press(&mut app, KeyCode::Down);
    assert_eq!(app.pivot_melt_modal.focus, PivotMeltFocus::PivotIndex);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.pivot_melt_modal.index_columns.len(), 1, "toggled in");
    press(&mut app, KeyCode::Esc);
    assert!(app.pivot_melt_modal.picker.is_none());
    assert!(
        app.pivot_melt_modal.index_columns.is_empty(),
        "Esc undid the toggle"
    );
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    assert!(app.pivot_melt_modal.picker.is_none());
    assert_eq!(app.pivot_melt_modal.index_columns.len(), 1, "Enter kept it");
}

/// Enter submits from any field: on an incomplete pivot it re-accents the spec line
/// rather than leaving or raising a modal, from the tab bar and from a row alike.
#[test]
fn enter_on_an_incomplete_form_stays_and_says_why() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("forms_enter_incomplete.csv");
    press(&mut app, KeyCode::Char('p'));
    for _ in 0..2 {
        assert!(press(&mut app, KeyCode::Enter).is_none());
        assert_eq!(app.overlay, Overlay::PivotMelt);
        assert!(app.pivot_melt_modal.attention, "the gap line re-accents");
        assert!(!app.modal_showing());
        press(&mut app, KeyCode::Down);
    }
}

/// Sort & Filter lists what is in effect: a sort flips with Space, moves with
/// `[` / `]`, goes with `d`, and a new one is added from "add sort"; Enter applies
/// the list as it stands.
#[test]
fn sort_and_filter_edits_what_is_in_effect() {
    use datui::sort_filter_modal::SortFilterField;
    let (mut app, rx, tx) = open_query_filter_fixture("forms_in_effect.csv");
    app.event(AppEvent::Sort(
        vec!["a".to_string(), "c".to_string()],
        vec![false, false],
    ));
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('s'));
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Sort(0));
    press(&mut app, KeyCode::Char(' ')); // a: descending
    press(&mut app, KeyCode::Char(']')); // a after c
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Sort(1));
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Char('d')); // c goes
    assert_eq!(
        app.sort_filter_modal.sort.sorted_columns_and_directions(),
        (vec!["a".to_string()], vec![true])
    );
    press(&mut app, KeyCode::Down); // add sort
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::AddSort);
    press(&mut app, KeyCode::Char(' '));
    assert!(app.sort_filter_modal.sort_picker.is_some());
    for ch in "name".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Enter); // chooses, and the picker closes
    assert!(app.sort_filter_modal.sort_picker.is_none());
    assert_eq!(app.sort_filter_modal.focus, SortFilterField::Sort(1));
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_ne!(app.overlay, Overlay::SortFilter);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.view_sort_columns(),
        ["a".to_string(), "name".to_string()]
    );
    assert_eq!(state.view_sort_descending(), [true, false]);
}

/// A new user pressing `:` gets SQL; a build without SQL opens on q.
#[test]
fn the_query_prompt_opens_on_sql() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("prompt_default.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    #[cfg(feature = "sql")]
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    #[cfg(not(feature = "sql"))]
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Q));
}

/// `[query] default_mode` chooses where `:` opens, read from the config file.
#[test]
fn the_preferred_query_mode_is_where_the_prompt_opens() {
    let config: datui::AppConfig = toml::from_str("[query]\ndefault_mode = \"q\"\n").unwrap();
    let (mut app, _rx, _tx) = open_query_filter_fixture_with("prompt_preferred.csv", config);
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Q));

    // Typed there, a q query runs as one.
    for c in "select a where a > 10".chars() {
        press_key(&mut app, KeyCode::Char(c), KeyModifiers::NONE);
    }
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select a where a > 10");
    assert!(state.get_active_sql_query().is_empty());
}

/// Quoted text against a date column is a string, as in q: the prompt says so in q's
/// words, with the literal to write, and stays open to fix it.
#[test]
fn a_quoted_date_in_a_q_query_is_explained_in_the_prompt() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dated.csv");
    std::fs::write(&path, "id,d\n0,2024-01-01\n1,2024-01-02\n").unwrap();
    let config: datui::AppConfig = toml::from_str("[query]\ndefault_mode = \"q\"\n").unwrap();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().schema().get("d"),
        Some(&DataType::Date),
        "the repro needs d read as a date"
    );

    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Q));
    type_text(&mut app, "select where d = \"2024.01.01\"");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);

    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Q));
    assert!(!app.modal_showing(), "no modal over the prompt");
    assert_eq!(
        app.query_prompt_error().as_deref(),
        Some("d is a date; \"2024.01.01\" is a string. A date is 2024.01.01")
    );
    let screen = screen_at(&mut app, 100, 24);
    assert!(screen.contains("A date is 2024.01.01"), "{screen}");
    assert_eq!(current_rows(&app), 2, "the table is as it was");

    // Unquoted, it runs.
    press_key(&mut app, KeyCode::Char('u'), KeyModifiers::CONTROL);
    type_text(&mut app, "select where d = 2024.01.01");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None);
    assert_eq!(current_rows(&app), 1);
}

/// Editing an active query reopens its own mode, whatever the preference:
/// q text is never offered up as SQL, or the other way round.
#[test]
fn reopening_the_prompt_selects_the_active_query_mode() {
    let (mut app, rx, tx) = open_query_filter_fixture("prompt_reopen_mode.csv");

    run_and_settle(
        &mut app,
        AppEvent::QQuery("select a where a > 10".to_string()),
        &rx,
        &tx,
    );
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Q));
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), None);

    #[cfg(feature = "sql")]
    {
        run_and_settle(
            &mut app,
            AppEvent::SqlQuery("SELECT a FROM df WHERE a > 90".to_string()),
            &rx,
            &tx,
        );
        press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
        assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
        press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    }

    // Clearing the query returns `/` to the default.
    run_and_settle(&mut app, AppEvent::QQuery(String::new()), &rx, &tx);
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    assert_eq!(
        app.query_prompt_mode(),
        Some(datui::AppConfig::default().query.default_mode.resolve())
    );
}

/// Ctrl+T switches the language from inside the input and around, and what is
/// then typed runs in the language the prefix names.
#[test]
fn ctrl_t_switches_the_query_mode() {
    let (mut app, rx, tx) = open_query_filter_fixture("prompt_chord.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    let modes = QueryMode::available();
    assert_eq!(app.query_prompt_mode(), Some(modes[0]));
    for &mode in modes[1..].iter().chain(&modes[..1]) {
        press_key(&mut app, KeyCode::Char('t'), KeyModifiers::CONTROL);
        assert_eq!(app.query_prompt_mode(), Some(mode));
    }

    while app.query_prompt_mode() != Some(QueryMode::Q) {
        press_key(&mut app, KeyCode::Char('t'), KeyModifiers::CONTROL);
    }
    type_text(&mut app, "select where a < 50");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where a < 50");
    assert_eq!(current_rows(&app), 50);
}

/// Under a SQL statement the command line lists the columns of `df`, narrowed to
/// the word being typed, and Tab completes it: the one name that fits, then the
/// table name.
#[cfg(feature = "sql")]
#[test]
fn tab_completes_column_names_in_sql() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_complete.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    let footer = |app: &mut App| screen_at(app, 80, 24).lines().last().unwrap().to_string();
    let line = footer(&mut app);
    assert!(line.contains("a  c  name"), "{line}");

    type_text(&mut app, "SELECT na");
    let line = footer(&mut app);
    assert!(line.contains("name") && !line.contains("a  c"), "{line}");
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_text(), Some("SELECT name"));
    type_text(&mut app, " FROM d");
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_text(), Some("SELECT name FROM df"));
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None, "the statement ran");
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_sql_query(),
        "SELECT name FROM df"
    );
}

/// Tab completes a column name in q too, spelled as q reads it.
#[test]
fn tab_completes_column_names_in_q() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("q_complete.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    while app.query_prompt_mode() != Some(QueryMode::Q) {
        press_key(&mut app, KeyCode::Char('t'), KeyModifiers::CONTROL);
    }
    type_text(&mut app, "select na");
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_text(), Some("select name"));
}

/// Alt+Enter breaks the line; Enter runs the statement, line breaks and all.
#[cfg(feature = "sql")]
#[test]
fn alt_enter_breaks_a_sql_line_and_enter_runs_it() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_newline.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    type_text(&mut app, "SELECT a FROM df");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::ALT);
    type_text(&mut app, "WHERE a < 10");
    assert_eq!(
        app.query_prompt_text(),
        Some("SELECT a FROM df\nWHERE a < 10")
    );
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql), "not run yet");
    let screen = screen_at(&mut app, 80, 24);
    assert!(
        screen.contains("SELECT a FROM df") && screen.contains("WHERE a < 10"),
        "{screen}"
    );

    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None);
    assert_eq!(current_rows(&app), 10);
}

/// A statement that plans but fails on the data keeps the prompt open with the
/// reason under it, in datui's words; the table stays as it was, no modal
/// takes the keys, and the statement can be fixed where it is.
#[cfg(feature = "sql")]
#[test]
fn a_sql_statement_that_fails_while_running_stays_in_the_prompt() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_runtime_error.csv");
    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    type_text(&mut app, "SELECT CAST(name AS INT) AS n FROM df");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);

    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    assert!(!app.modal_showing(), "no modal over the prompt");
    let error = app.query_prompt_error().expect("the reason is shown");
    // Counted in the batch that failed, so "100 of 100" or, read in pieces, a floor.
    assert!(
        error.starts_with("name: 100 of 100 values are not") || error.starts_with("At least"),
        "{error}"
    );
    assert!(
        error.contains("in name are not whole numbers, such as \"alpha_0\"")
            || error.contains("values are not whole numbers, such as \"alpha_0\""),
        "{error}"
    );
    assert!(error.contains("TRY_CAST(name AS INT)"), "{error}");
    let screen = screen_at(&mut app, 80, 24);
    assert!(screen.contains("are not whole numbers"), "{screen}");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.get_active_sql_query().is_empty(), "nothing ran");
    assert_eq!(current_rows(&app), 100, "the table is as it was");

    // Fixed in place: the statement is still there to edit.
    press_key(&mut app, KeyCode::Char('a'), KeyModifiers::CONTROL);
    for _ in 0.."SELECT ".len() {
        press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    }
    type_text(&mut app, "TRY_");
    assert_eq!(
        app.query_prompt_text(),
        Some("SELECT TRY_CAST(name AS INT) AS n FROM df")
    );
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None);
    assert_eq!(app.query_prompt_error(), None);
    assert_eq!(current_rows(&app), 100);
}

/// #400: a query that plans but fails on its first rows is not applied. The
/// table, its schema and its row count stay those of the view before it, and a
/// sort afterwards works on that view instead of failing the same way again.
#[cfg(feature = "sql")]
#[test]
fn a_query_that_fails_when_collected_is_not_installed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dated.csv");
    let mut csv = String::from("id,ds,v\n");
    for i in 0..30 {
        csv.push_str(&format!("{i},2024-01-{:02},{}\n", i % 28 + 1, 30 - i));
    }
    std::fs::write(&path, csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let names = |app: &App| -> Vec<String> {
        let state = app.data_table_state.as_ref().unwrap();
        state.schema().iter_names().map(|n| n.to_string()).collect()
    };
    assert_eq!(names(&app), ["id", "ds", "v"]);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().schema().get("ds"),
        Some(&DataType::Date),
        "the repro needs ds read as a date"
    );

    press_key(&mut app, KeyCode::Char(':'), KeyModifiers::NONE);
    type_text(
        &mut app,
        "SELECT CAST(SUBSTR(ds, 1, 10) AS DATE) AS d, COUNT(*) FROM df GROUP BY d",
    );
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);

    // The prompt says why, with the statement still there to fix.
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    assert!(!app.modal_showing());
    let error = app.query_prompt_error().expect("the reason is shown");
    assert!(error.contains("String"), "{error}");
    // Nothing of the failed query is installed.
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(names(&app), ["id", "ds", "v"]);
    assert!(state.get_active_sql_query().is_empty());
    assert!(
        state.is_num_rows_valid(),
        "the row count is not left unknown"
    );
    assert_eq!(state.num_rows(), 30);

    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    let screen = screen_at(&mut app, 80, 24);
    assert!(!screen.contains("/ ?"), "{screen}");
    assert!(screen.contains("/ 30"), "{screen}");

    // A sort works on the data as it was.
    run_and_settle(
        &mut app,
        AppEvent::Sort(vec!["v".to_string()], vec![false]),
        &rx,
        &tx,
    );
    assert!(!app.modal_showing(), "the sort does not fail");
    let state = app.data_table_state.as_ref().unwrap();
    let sorted = state.lf().clone().collect().unwrap();
    assert_eq!(sorted.height(), 30);
    assert_eq!(
        sorted.column("v").unwrap().i64().unwrap().get(0),
        Some(1),
        "sorted ascending on v"
    );
}

/// Reopening `/` restores the last query selected, so typing states a new
/// question instead of appending to the tail of the old one.
#[test]
fn reopening_the_query_prompt_selects_the_old_query() {
    let (mut app, rx, tx) = open_query_filter_fixture_with("reopen_query.csv", q_style_config());

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char(':'),
        KeyModifiers::NONE,
    )));
    for c in "select a where a > 10".chars() {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Char(c),
            KeyModifiers::NONE,
        )));
    }
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next.take() {
        next = app.event(ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 89);

    // Reopen and type a fresh query: the first character replaces the old text.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char(':'),
        KeyModifiers::NONE,
    )));
    for c in "select a where a > 50".chars() {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Char(c),
            KeyModifiers::NONE,
        )));
    }
    let mut next = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next.take() {
        next = app.event(ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        current_rows(&app),
        49,
        "typing replaced the restored query rather than appending to it"
    );
}

/// Ctrl+U kills from the cursor back to the start of the line, keeping what
/// follows, and Ctrl+Z puts it back: readline's bindings, in the query prompt.
#[test]
fn test_query_prompt_ctrl_u_kills_to_line_start_and_ctrl_z_undoes() {
    let (mut app, rx, tx) =
        open_query_filter_fixture_with("ctrl_u_query_prompt.csv", q_style_config());

    press(&mut app, KeyCode::Char(':'));
    assert_eq!(app.input_mode, InputMode::Editing);
    for c in "select name where c = 1".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    for _ in 0.."where c = 1".len() {
        press(&mut app, KeyCode::Left);
    }

    press_ctrl(&mut app, 'u');
    let screen = screen_text(&mut app);
    assert!(
        screen.contains("where c = 1"),
        "the text after the cursor stays"
    );
    assert!(
        !screen.contains("select name"),
        "the text before it is gone"
    );

    press_ctrl(&mut app, 'z');
    assert!(screen_text(&mut app).contains("select name where c = 1"));

    // What runs is what the field holds after the undo.
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_active_query(),
        "select name where c = 1"
    );
    assert_eq!(current_rows(&app), 33);
}

/// The same bindings in a form field: the view save form's name.
#[test]
fn test_form_field_ctrl_u_kills_to_line_start_and_ctrl_z_undoes() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("ctrl_u_form_field.csv");
    // The save gate wants something to save.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["a".to_string()], vec![false]);

    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(
        app.view_modal.form_focus,
        datui::widgets::view_modal::FormFocus::Name
    );
    let suggested = app.view_modal.name_input.value().to_string();
    // The suggested name is selected; End keeps it so typing extends it.
    press(&mut app, KeyCode::End);
    for c in " by a".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    for _ in 0.." by a".len() {
        press(&mut app, KeyCode::Left);
    }

    press_ctrl(&mut app, 'u');
    assert_eq!(app.view_modal.name_input.value(), " by a");
    assert_eq!(app.view_modal.name_input.cursor(), 0);

    press_ctrl(&mut app, 'z');
    assert_eq!(
        app.view_modal.name_input.value(),
        format!("{suggested} by a")
    );

    // Esc discards the form, so the test saves nothing.
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.view_modal.mode,
        datui::widgets::view_modal::ViewModalMode::List
    );
}

/// `0` takes a column out of the sort and stages that as a change, so Apply
/// has something to apply; a digit past the end of the order says why it did
/// nothing, on the sidebar's own status line, until the next key.
#[test]
fn test_sort_digits_stage_zero_and_explain_out_of_range() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_digits.csv");

    // Sort by the first column through its digit, and apply.
    open_columns_list(&mut app);
    press(&mut app, KeyCode::Char('1'));
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().view_sort_columns(),
        vec!["a".to_string()]
    );

    // Reopen: the applied sort arrives staged and nothing is pending.
    open_columns_list(&mut app);
    assert!(!app.sort_filter_modal.sort.has_unapplied_changes);

    // A digit past the end of the order does nothing and says so.
    press(&mut app, KeyCode::Char('5'));
    assert_eq!(
        app.sort_filter_modal.sort.columns[0].sort_order,
        Some(1),
        "an out-of-range digit changes nothing"
    );
    assert!(!app.sort_filter_modal.sort.has_unapplied_changes);
    assert!(
        screen_text(&mut app).contains("Position 5 is past the end; use 1."),
        "the status line says why"
    );
    assert!(!app.modal_showing(), "validation is not a modal");

    // `0` removes it and stages the change.
    press(&mut app, KeyCode::Char('0'));
    assert!(
        app.sort_filter_modal.sort.status.is_none(),
        "the next key clears the status line"
    );
    assert!(!screen_text(&mut app).contains("past the end"));
    assert_eq!(app.sort_filter_modal.sort.columns[0].sort_order, None);
    assert!(
        app.sort_filter_modal.sort.has_unapplied_changes,
        "0 is a change to apply"
    );

    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .view_sort_columns()
            .is_empty(),
        "Apply took the column out of the sort"
    );
}

/// A query that leaves fewer columns, then one, then none shown: nothing to page,
/// no range, and the scroll does not outlive the columns it was over.
#[test]
fn wide_table_paging_after_a_query_with_one_and_no_columns() {
    let size = (80, 24);
    let (mut app, rx, tx) = open_wide_table("wide_nav_query.parquet", 40, size);
    press_and_draw(&mut app, KeyCode::Char('}'), size);
    assert_eq!(columns_shown(&app).unwrap().last, 40);
    app.event(AppEvent::QQuery("select id_000, price_001".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    draw_sized(&mut app, size);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["id_000", "price_001"]);
    assert_eq!(state.termcol_index, 0, "the new schema starts at the left");
    assert_eq!(range_shown(&app), Some((1, 2)), "both on screen");

    app.event(AppEvent::QQuery("select id_000".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    for key in ['{', '}', 'l', 'h'] {
        let screen = press_and_draw(&mut app, KeyCode::Char(key), size);
        assert!(header_line(&screen).contains("id_000"), "{key}: {screen}");
        assert_eq!(app.data_table_state.as_ref().unwrap().termcol_index, 0);
    }
    for arrow in [KeyCode::Left, KeyCode::Right] {
        let screen = page_and_draw(&mut app, arrow, size);
        assert!(
            header_line(&screen).contains("id_000"),
            "{arrow:?}: {screen}"
        );
        assert_eq!(app.data_table_state.as_ref().unwrap().termcol_index, 0);
    }

    // No column shown at all: the keys do nothing, and the picker has nothing to offer.
    run_and_settle(&mut app, AppEvent::ColumnOrder(Vec::new(), 0), &rx, &tx);
    for arrow in [KeyCode::Left, KeyCode::Right] {
        page_and_draw(&mut app, arrow, size);
        assert_eq!(app.data_table_state.as_ref().unwrap().termcol_index, 0);
    }
    for key in ['{', '}', 'g'] {
        press_and_draw(&mut app, KeyCode::Char(key), size);
        assert!(app.at_table(), "{key}");
        assert_eq!(app.data_table_state.as_ref().unwrap().termcol_index, 0);
        assert_eq!(columns_shown(&app), None);
    }
}

/// A query's text of a date past the calendar is its stored number, as the table
/// shows it, and its date parts are null, where Polars panicked and failed the
/// query for the whole column (#506). The first row is Polars' own text.
#[test]
fn out_of_range_dates_in_a_query_cast_to_their_stored_number() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_past_calendar(&dir);
    let some = |s: &str| Some(s.to_string());
    for (c, first, past) in PAST_CALENDAR {
        let word = |s: &str| some(s.split(' ').next().unwrap());
        for (query, expected) in [
            (format!("select x: {c}.str"), [some(first), some(past)]),
            (
                format!("select x: {c}.format[\"%Y\"]"),
                [some("1970"), some(past)],
            ),
            (
                format!("select x: {c}.part[\" \", 0]"),
                [word(first), word(past)],
            ),
            (
                format!("select x: {c}.slice[0, 4]"),
                [some(&first[..4]), some(&past[..4])],
            ),
            (
                format!("select x: {c}.replace[\"since\", \"after\"]"),
                [some(first), some(&past.replace("since", "after"))],
            ),
            (format!("select x: {c}.strip"), [some(first), some(past)]),
            // Coalesced with text, Polars casts the date to text itself.
            (
                format!("select x: {c} ^ \"none\""),
                [some(first), some(past)],
            ),
        ] {
            run_query(&mut app, &rx, &tx, &query);
            assert_eq!(view_text(&app, "x"), expected, "{query}");
        }
        run_query(
            &mut app,
            &rx,
            &tx,
            &format!("select s where {c} like \"*since*\""),
        );
        assert_eq!(view_text(&app, "s"), [some("b")], "{c} like");
        // Read back as a date, the stored number reads as none.
        for query in [
            format!("select x: {c}.to_date[\"%Y-%m-%d\"]"),
            format!("select x: {c}.to_datetime[\"%Y-%m-%d\"]"),
            format!("select x: {c}.date"),
            format!("select x: {c}.month_start"),
            format!("select x: {c}.month_end"),
            format!("select x: {c}.doy"),
        ] {
            run_query(&mut app, &rx, &tx, &query);
            assert_eq!(view_text(&app, "x")[1], None, "{query}");
        }
        run_query(&mut app, &rx, &tx, &format!("select x: {c}.date"));
        assert_eq!(view_text(&app, "x")[0], some("1970-01-01"), "{c}.date");
        // A key of the text drills into the row that holds it.
        run_query(
            &mut app,
            &rx,
            &tx,
            &format!("select n: count s by k: {c}.str"),
        );
        let keys = view_text(&app, "k");
        assert!(keys.contains(&some(past)), "{c}: {keys:?}");
        assert_drills_to_its_row(&mut app, &keys, past, c);
    }
    draw_wide(&mut app, "query");
}

/// SQL's casts to text, `||`, `CONCAT`, `STRFTIME`, and a `COALESCE`, `CASE` or
/// `UNION` of a date with text write a date past the calendar as its stored number,
/// where Polars panicked (#506), and a grouping by that text drills into its row.
#[cfg(feature = "sql")]
#[test]
fn out_of_range_dates_in_sql_cast_to_their_stored_number() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_past_calendar(&dir);
    let some = |s: &str| Some(s.to_string());
    for (c, first, past) in PAST_CALENDAR {
        for (sql, expected) in [
            (
                format!("SELECT CAST({c} AS VARCHAR) AS x FROM df"),
                [some(first), some(past)],
            ),
            (
                format!("SELECT STRFTIME({c}, '%Y') AS x FROM df"),
                [some("1970"), some(past)],
            ),
            (
                format!("SELECT {c} || '!' AS x FROM df"),
                [some(&format!("{first}!")), some(&format!("{past}!"))],
            ),
            (
                format!("SELECT CONCAT({c}, '!') AS x FROM df"),
                [some(&format!("{first}!")), some(&format!("{past}!"))],
            ),
            (
                format!(
                    "SELECT x FROM (SELECT s, CAST({c} AS VARCHAR) AS x FROM df) \
                     WHERE x IS NOT NULL ORDER BY s"
                ),
                [some(first), some(past)],
            ),
            // Met with text, Polars casts the date to text itself.
            (
                format!("SELECT COALESCE({c}, 'none') AS x FROM df"),
                [some(first), some(past)],
            ),
            (
                format!("SELECT CASE WHEN s = 'b' THEN {c} ELSE s END AS x FROM df"),
                [some("a"), some(past)],
            ),
        ] {
            run_sql(&mut app, &rx, &tx, &sql);
            assert_eq!(app.error_message(), None, "{sql}");
            assert_eq!(view_text(&app, "x"), expected, "{sql}");
        }
        let sql = format!("SELECT {c} AS x FROM df UNION ALL SELECT s FROM df");
        run_sql(&mut app, &rx, &tx, &sql);
        assert_eq!(app.error_message(), None, "{sql}");
        let mut stacked = view_text(&app, "x");
        stacked.sort();
        let mut expected = [some(first), some(past), some("a"), some("b")];
        expected.sort();
        assert_eq!(stacked, expected, "{sql}");

        let sql = format!("SELECT s FROM df WHERE CAST({c} AS VARCHAR) LIKE '%since%'");
        run_sql(&mut app, &rx, &tx, &sql);
        assert_eq!(view_text(&app, "s"), [some("b")], "{sql}");

        let sql = format!("SELECT CAST({c} AS VARCHAR) AS k, COUNT(*) AS n FROM df GROUP BY k");
        run_sql(&mut app, &rx, &tx, &sql);
        let keys = view_text(&app, "k");
        assert!(keys.contains(&some(past)), "{sql}");
        assert!(
            app.data_table_state.as_ref().unwrap().can_drill_down(),
            "{sql}"
        );
        assert_drills_to_its_row(&mut app, &keys, past, &sql);
    }
    draw_wide(&mut app, "sql");
}

/// Date math a query does on a nanosecond datetime at the ends of its range is null
/// where it would move the value past them, and so are the parts read in a zone's
/// local time there, where Polars overflowed and failed the query (#517).
#[test]
fn nanosecond_date_math_at_the_ends_of_the_range_is_null_in_a_query() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_ns_edges(&dir);
    for (query, expected) in [
        (
            "select x: n.month_start",
            [
                Some("1970-01-01 00:00:00"),
                Some("2262-04-01 23:47:16.854775807"),
                None,
            ],
        ),
        (
            "select x: n.month_end",
            [Some("1970-01-31 00:00:00"), None, None],
        ),
        (
            "select x: z.month_start",
            [Some("1970-01-01 01:00:00 CET"), None, None],
        ),
        ("select x: z.date", [Some("1970-01-01"), None, None]),
        ("select x: z.time", [Some("01:00:00"), None, None]),
        ("select x: z.doy", [Some("1"), None, None]),
    ] {
        run_query(&mut app, &rx, &tx, query);
        let expected = expected.map(|v| v.map(String::from));
        assert_eq!(view_text(&app, "x"), expected, "{query}");
    }
}

/// SQL's `INTERVAL` arithmetic and date parts on a nanosecond datetime at the ends
/// of its range are null where they would leave it, where Polars overflowed (#517).
#[cfg(feature = "sql")]
#[test]
fn nanosecond_date_math_at_the_ends_of_the_range_is_null_in_sql() {
    let dir = common::fixture_dir();
    let (mut app, rx, tx) = open_ns_edges(&dir);
    for (sql, expected) in [
        (
            "SELECT n + INTERVAL '1 day' AS x FROM df",
            [
                Some("1970-01-02 00:00:00"),
                None,
                Some("1677-09-22 00:12:43.145224193"),
            ],
        ),
        (
            "SELECT z + INTERVAL '1 month' AS x FROM df",
            [Some("1970-02-01 01:00:00 CET"), None, None],
        ),
        (
            "SELECT EXTRACT(DOY FROM z) AS x FROM df",
            [Some("1"), None, None],
        ),
    ] {
        run_sql(&mut app, &rx, &tx, sql);
        assert_eq!(app.error_message(), None, "{sql}");
        let expected = expected.map(|v| v.map(String::from));
        assert_eq!(view_text(&app, "x"), expected, "{sql}");
    }
}

/// `H` / `L` move the column cursor's column through the column order, the cursor
/// with it; a frozen column stays among the frozen ones; `R` puts the order back.
#[test]
fn h_and_l_move_the_cursors_column() {
    let (mut app, rx, tx) = open_quick_filter_table("move_columns.parquet");
    let state = |app: &App| {
        let s = app.data_table_state.as_ref().unwrap();
        (s.headers(), s.current_column().unwrap().to_string())
    };
    let original = state(&app).0;
    assert_eq!(original, ["name", "n", "x", "day", "tags"]);

    table_key(&mut app, &rx, &tx, 'L');
    assert_eq!(
        state(&app),
        (strings(&["n", "name", "x", "day", "tags"]), "name".into())
    );
    table_key(&mut app, &rx, &tx, 'L');
    assert_eq!(state(&app).0, ["n", "x", "name", "day", "tags"]);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .current_column_index(),
        Some(2),
        "the cursor went with it"
    );
    table_key(&mut app, &rx, &tx, 'H');
    assert_eq!(
        state(&app),
        (strings(&["n", "name", "x", "day", "tags"]), "name".into())
    );
    // At an end, nothing moves.
    table_key(&mut app, &rx, &tx, '{');
    assert!(press(&mut app, KeyCode::Char('H')).is_none());
    table_key(&mut app, &rx, &tx, '}');
    assert!(press(&mut app, KeyCode::Char('L')).is_none());
    // The sidebar shows the order H and L made.
    press(&mut app, KeyCode::Char('s'));
    let names: Vec<String> = app.sort_filter_modal.sort.get_column_order();
    assert_eq!(names, ["n", "name", "x", "day", "tags"]);
    press(&mut app, KeyCode::Esc);

    table_key(&mut app, &rx, &tx, 'R');
    assert_eq!(state(&app).0, original, "R resets the order");

    // Frozen: `name` alone; it cannot leave the frozen block, nor `n` enter it.
    run_and_settle(
        &mut app,
        AppEvent::ColumnOrder(original.clone(), 1),
        &rx,
        &tx,
    );
    draw_sized(&mut app, (100, 24));
    table_key(&mut app, &rx, &tx, '{');
    assert_eq!(state(&app).1, "name");
    assert!(press(&mut app, KeyCode::Char('L')).is_none());
    table_key(&mut app, &rx, &tx, 'l');
    assert_eq!(state(&app).1, "n");
    assert!(press(&mut app, KeyCode::Char('H')).is_none());
    table_key(&mut app, &rx, &tx, 'L');
    assert_eq!(state(&app).0, ["name", "x", "n", "day", "tags"]);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        1
    );
}

/// `+` keeps the rows with the cursor's cell's value and `-` drops them, each a
/// filter in the sidebar's list that joins the others with "and"; a null cell is a
/// null test, a float matches as drawn, a date by its text; `R` clears them all.
#[test]
fn plus_and_minus_filter_on_the_cursors_cell() {
    let (mut app, rx, tx) = open_quick_filter_table("quick_filter.parquet");

    table_key(&mut app, &rx, &tx, '+');
    assert_eq!(quick_view(&app), (2, strings(&["name = north"])));
    // In the Filters tab, where it can be edited.
    press(&mut app, KeyCode::Char('s'));
    let listed = &app.sort_filter_modal.filter.statements;
    assert_eq!(listed.len(), 1);
    assert_eq!(
        (listed[0].column.as_str(), listed[0].value.as_str()),
        ("name", "north")
    );
    press(&mut app, KeyCode::Esc);
    // Joined with "and": north rows with n other than 1.
    table_key(&mut app, &rx, &tx, 'l');
    table_key(&mut app, &rx, &tx, '-');
    assert_eq!(quick_view(&app), (1, strings(&["name = north", "n != 1"])));
    assert_eq!(
        app.data_table_state.as_ref().unwrap().view_filters()[1].logical_op,
        datui::filter_modal::LogicalOperator::And
    );
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);
    assert_eq!(quick_view(&app), (6, vec![]), "R clears them");

    // A float, exactly as stored: 0.1 + 0.2 is drawn 0.3 beside 0.3, and is not it.
    let to_x = |app: &mut App| {
        for c in ['{', 'l', 'l'] {
            table_key(app, &rx, &tx, c);
        }
        assert_eq!(
            app.data_table_state.as_ref().unwrap().current_column(),
            Some("x")
        );
    };
    to_x(&mut app);
    table_key(&mut app, &rx, &tx, '+');
    assert_eq!(quick_view(&app), (1, strings(&["x = 0.30000000000000004"])));
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);
    to_x(&mut app);
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, '+');
    assert_eq!(quick_view(&app), (1, strings(&["x = 0.3"])));
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);
    // A third twice: + keeps both, - drops exactly those (and the null).
    to_x(&mut app);
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, '+');
    assert_eq!(quick_view(&app).0, 2);
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);
    to_x(&mut app);
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, '-');
    assert_eq!(quick_view(&app), (3, strings(&["x != 0.3333333333333333"])));
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);

    // A null cell: is null, not null.
    table_key(&mut app, &rx, &tx, '{');
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, '+');
    assert_eq!(quick_view(&app), (1, strings(&["name is null"])));
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, 'j');
    table_key(&mut app, &rx, &tx, '-');
    assert_eq!(quick_view(&app), (5, strings(&["name not null"])));
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);

    // A date.
    table_key(&mut app, &rx, &tx, '{');
    for c in ['l', 'l', 'l'] {
        table_key(&mut app, &rx, &tx, c);
    }
    assert_eq!(
        app.data_table_state.as_ref().unwrap().current_column(),
        Some("day")
    );
    table_key(&mut app, &rx, &tx, '+');
    assert_eq!(quick_view(&app), (3, strings(&["day = 2024-01-01"])));
    table_key(&mut app, &rx, &tx, 'R');
    run_and_settle(&mut app, key(KeyCode::Home), &rx, &tx);

    // A list: a flash, and nothing filtered.
    table_key(&mut app, &rx, &tx, '}');
    assert_eq!(
        app.data_table_state.as_ref().unwrap().current_column(),
        Some("tags")
    );
    assert!(press(&mut app, KeyCode::Char('+')).is_none());
    assert_eq!(quick_view(&app), (6, vec![]));
    assert_eq!(
        app.flash_message(),
        Some("+ and - filter on plain values, not lists")
    );
}

/// A filter value its column cannot read stays in the sidebar, which says why, and
/// applies nothing.
#[test]
fn a_filter_value_its_column_cannot_read_is_refused_with_a_reason() {
    let (mut app, rx, tx) = open_quick_filter_table("quick_filter_refused.parquet");
    press(&mut app, KeyCode::Char('s'));
    app.sort_filter_modal
        .filter
        .statements
        .push(datui::filter_modal::FilterStatement {
            columns: Vec::new(),
            column: "day".into(),
            operator: datui::filter_modal::FilterOperator::Gt,
            value: "2024-13-01".into(),
            logical_op: datui::filter_modal::LogicalOperator::And,
        });
    run_and_settle(&mut app, key(KeyCode::Enter), &rx, &tx);
    assert_eq!(app.overlay, Overlay::SortFilter, "the sidebar stays");
    assert_eq!(
        app.sort_filter_modal.sort.status.as_deref(),
        Some("day: \"2024-13-01\" is not a date written YYYY-MM-DD")
    );
    assert_eq!(quick_view(&app), (6, vec![]), "nothing applied");
    // Fixed, it applies.
    app.sort_filter_modal.filter.statements[0].value = "2024-01-01".into();
    run_and_settle(&mut app, key(KeyCode::Enter), &rx, &tx);
    assert!(app.at_table());
    assert_eq!(quick_view(&app), (2, strings(&["day > 2024-01-01"])));
}

/// A pattern that names no file is still a glob.
#[test]
fn a_pattern_that_names_no_file_still_expands() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for name in ["one.csv", "two.csv"] {
        write_marker(&tmp.path().join(name), name);
    }
    for pattern in ["*.csv", "???.csv", "[ot][nw]*.csv"] {
        let (_, df) = open_and_collect(vec![tmp.path().join(pattern)], OpenOptions::default());
        assert_eq!(marker_values(&df), ["one.csv", "two.csv"], "{pattern}");
    }
}

/// A pattern that matches nothing says so, not Polars' expansion input. A missing
/// `x?.csv` reaches the scan as a pattern, as `*.csv` always did.
#[test]
fn a_pattern_that_matches_nothing_says_so() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for name in ["x?.csv", "d[1].parquet", "*.arrow"] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        tx.send(AppEvent::OpenNamed(
            vec![tmp.path().join(name)],
            OpenOptions::default(),
        ))
        .unwrap();
        drain_events(&mut app, &rx);
        let message = app.error_message().expect("an error");
        assert!(
            message.ends_with(": No files match this pattern."),
            "{message}"
        );
    }
}

/// While a query's first rows are read, the table area says what the footer
/// does, in place of the rows it replaces; once they are in, they show.
#[cfg(feature = "sql")]
#[test]
fn a_running_query_says_so_in_the_table() {
    let (mut app, rx, tx) = open_query_filter_fixture("running_query_in_place.csv");
    press(&mut app, KeyCode::Char(':'));
    for c in "SELECT name FROM df WHERE c = 1".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(event);
    }
    // Its rows are not in until their job's end is handled.
    assert!(app.is_busy(), "the query is running");
    let screen = screen_text(&mut app);
    // Above the footer's row, which says it too.
    let table: String = screen.chars().take(120 * 29).collect();
    assert!(table.contains("Applying SQL query..."), "{screen}");
    assert!(!table.contains("alpha_0"), "{screen}");

    pump_until_idle(&mut app, &rx, &tx);
    let screen = screen_text(&mut app);
    assert!(!screen.contains("Applying SQL query..."), "{screen}");
    assert!(screen.contains("beta_1"), "{screen}");
}

/// Under `theme.mode = "auto"` the terminal's answer about its background picks the
/// palette, whatever `COLORFGBG` guessed at startup, and the configured
/// `theme.colors` stay over it. Focus coming back asks again, once. An explicit
/// mode ignores both.
#[test]
fn test_terminal_background_switches_the_palette_under_auto() {
    use datui::config::{AppConfig, ColorConfig, ConfigLayer, Theme, ThemeMode};
    let hex = |s: &str| datui::ColorParser::new().parse(s).expect("color parses");
    let config = |text: &str| {
        AppConfig::from_layers([ConfigLayer::parse(text).expect("layer parses")]).expect("resolves")
    };
    let auto = config("[theme.colors]\naccent = \"#123456\"\n");
    let theme = Theme::from_config(&auto.theme).expect("theme builds");
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, common::test_runtime(), theme, auto);
    let area = Rect::new(0, 0, 80, 24);

    for (mode, stock) in [
        (ThemeMode::Light, ColorConfig::light()),
        (ThemeMode::Dark, ColorConfig::dark()),
        (ThemeMode::Light, ColorConfig::light()),
    ] {
        app.event(AppEvent::TerminalBackground(mode));
        assert_eq!(
            app.theme().table_header_bg(),
            hex(&stock.table_header_bg),
            "{mode:?}"
        );
        assert_eq!(app.theme().dimmed(), hex(&stock.dimmed), "{mode:?}");
        assert_eq!(app.theme().accent(), hex("#123456"), "{mode:?}");
        // Drawn with it.
        let mut buf = Buffer::empty(area);
        Widget::render(&mut app, area, &mut buf);
    }

    assert!(!app.take_background_query());
    app.event(AppEvent::TerminalFocused);
    assert!(app.take_background_query());
    assert!(!app.take_background_query(), "asked once");

    let light = config("[theme]\nmode = \"light\"\n");
    let theme = Theme::from_config(&light.theme).expect("theme builds");
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, common::test_runtime(), theme, light);
    app.event(AppEvent::TerminalBackground(ThemeMode::Dark));
    assert_eq!(
        app.theme().table_header_bg(),
        hex(&ColorConfig::light().table_header_bg)
    );
    app.event(AppEvent::TerminalFocused);
    assert!(!app.take_background_query());
}

/// A column retyped from the Info panel: the view reads it as the type at once, a
/// value that does not fit is null and counted in the Notes, the footer says so, and
/// `as read` takes the type away again.
#[test]
fn a_column_retyped_in_the_table() {
    let (mut app, rx, tx) = open_csv_with(
        "retype_codes.csv",
        "id,code,when\n1,10,03/04/2024\n2,x,04/04/2024\n3,30,05/04/2024\n",
        OpenOptions {
            parse_dates: false,
            ..OpenOptions::default()
        },
    );
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        view_frame(&app).column("code").unwrap().dtype(),
        &DataType::String
    );

    retype_from_schema(&mut app, "code");
    type_into(&mut app, "i64");
    app.event(key(KeyCode::Enter));
    assert_eq!(app.overlay, datui::Overlay::Info, "back to the panel");
    pump_until(&mut app, &rx, &tx, |app| {
        !app.is_busy() && !app.unfit_count_pending()
    });
    let df = view_frame(&app);
    let code: Vec<Option<i64>> = df.column("code").unwrap().i64().unwrap().iter().collect();
    assert_eq!(code, [Some(10), None, Some(30)]);
    let notes = summaries(&app);
    assert!(
        notes.contains(&"code: 1 value not i64, read as null".to_string()),
        "{notes:?}"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.retyped_columns(), ["code"]);
    let area = Rect::new(0, 0, 120, 30);
    let mut buf = Buffer::empty(area);
    app.event(key(KeyCode::Esc));
    app.render(area, &mut buf);
    let shown = common::buffer_text(&buf);
    assert!(shown.contains("typed code"), "the footer says so: {shown}");

    // A date with the format that reads the column's first value.
    retype_from_schema(&mut app, "when");
    type_into(&mut app, "date");
    app.event(key(KeyCode::Enter));
    app.event(key(KeyCode::Enter));
    pump_until_idle(&mut app, &rx, &tx);
    let df = view_frame(&app);
    assert_eq!(df.column("when").unwrap().dtype(), &DataType::Date);
    assert_eq!(
        df.column("when").unwrap().get(0).unwrap().to_string(),
        "2024-04-03"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.column_type_of("when").unwrap().format.as_deref(),
        Some("%d/%m/%Y")
    );

    // As read again.
    retype_from_schema(&mut app, "code");
    type_into(&mut app, "as read");
    app.event(key(KeyCode::Enter));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        view_frame(&app).column("code").unwrap().dtype(),
        &DataType::String
    );
    assert_eq!(
        app.data_table_state.as_ref().unwrap().retyped_columns(),
        ["when"]
    );
}
