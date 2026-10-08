//! Paging reads each row once: a fill reads only the rows past the ones on hand, and a
//! CSV window reads on from the mark before it.

use super::*;

/// Page down from the top `pages` times as the app does: each key slides the view, a
/// view past the buffer reads its rows, and a view near the buffer's end reads ahead.
/// Returns every range read.
fn page_down(state: &mut DataTableState, pages: usize) -> Vec<(usize, usize)> {
    let mut reads = Vec::new();
    let mut fill = |state: &mut DataTableState| {
        if let Some(request) = state.prepare_async_collect(None) {
            reads.push((request.buffer_start, request.buffer_end));
            let df = collect_lazy(request.lf, request.polars_streaming).unwrap();
            state.apply_async_collect(request.plan.fit(df));
        }
    };
    fill(state);
    for _ in 0..pages {
        if state.slide_table(state.visible_rows as i64) {
            fill(state);
        }
        if state.wants_to_load_ahead() {
            fill(state);
        }
    }
    reads
}

/// Paging forward through `state` reads no row twice, and reads at most the rows paged
/// through and what the reads ahead reach past them.
fn reads_each_row_once(mut state: DataTableState, pages: usize) {
    state.visible_rows = 40;
    let reads = page_down(&mut state, pages);
    for pair in reads.windows(2) {
        assert!(
            pair[1].0 >= pair[0].1,
            "a fill read rows on hand again: {reads:?}"
        );
    }
    let read: usize = reads.iter().map(|(start, end)| end - start).sum();
    let paged = state.start_row() + state.visible_rows;
    // A read ahead starts within half its reach of the buffer's end.
    let most = reads
        .iter()
        .map(|(start, end)| end - start)
        .max()
        .unwrap_or(0);
    assert!(
        read <= paged + 2 * most,
        "{read} rows read to page through {paged}: {reads:?}"
    );
    assert!(
        reads.len() > 2,
        "paging read ahead more than once: {reads:?}"
    );
}

#[test]
fn paging_a_csv_reads_each_row_once() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    let mut text = String::from("id,name\n");
    for i in 0..3_000 {
        text.push_str(&format!("{i},name {i}\n"));
    }
    std::fs::write(&path, text).unwrap();
    let options = OpenOptions::default();
    let read =
        crate::formats::readers::csv::read_delimited(&path, b',', &options, &Default::default())
            .unwrap();
    reads_each_row_once(DataTableState::from_read(read, &options).unwrap(), 60);
}

#[test]
fn paging_a_frame_reads_each_row_once() {
    let df = df!("id" => (0..3_000i64).collect::<Vec<_>>()).unwrap();
    let state = DataTableState::new(df.lazy(), None, None, None, None, true).unwrap();
    reads_each_row_once(state, 60);
}

/// A view that is not the scan as loaded reads its window whole again: rows a sort
/// ties may come back in another order, so two reads are not stitched.
#[test]
fn a_sorted_view_is_not_stitched() {
    let df = df!("id" => (0..3_000i64).map(|i| i % 7).collect::<Vec<_>>()).unwrap();
    let mut state = DataTableState::new(df.lazy(), None, None, None, None, true).unwrap();
    state.sort(vec!["id".into()], true);
    state.visible_rows = 40;
    let reads = page_down(&mut state, 10);
    assert!(
        reads.iter().skip(1).all(|(start, _)| *start == 0),
        "{reads:?}"
    );
}

/// `rows` rows of `width`-character text and an id, written as Parquet and scanned.
fn parquet_state(dir: &Path, rows: usize, width: usize) -> DataTableState {
    let path = dir.join(format!("t{width}.parquet"));
    let mut df = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "text" => (0..rows).map(|i| format!("{i:0width$}")).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(std::fs::File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let lf = LazyFrame::scan_parquet(PlRefPath::try_from_path(&path).unwrap(), Default::default())
        .unwrap();
    let mut state = DataTableState::new(lf, None, None, None, None, false).unwrap();
    state.visible_rows = 40;
    state
}

/// A local Parquet read decodes whole data pages: once the first read has measured the
/// rows, a read ahead takes thousands of them for about the cost of a page, by the
/// bytes they hold.
#[test]
fn a_parquet_view_reads_ahead_through_its_pages() {
    let dir = tempfile::tempdir().unwrap();
    let mut narrow = parquet_state(dir.path(), 30_000, 8);
    let reads = page_down(&mut narrow, 5);
    let len = |(start, end): &(usize, usize)| end - start;
    assert!(len(&reads[0]) < 1_000, "the first read is pages: {reads:?}");
    // Up to that many rows past the view, from the end of the rows on hand.
    let ahead = reads.iter().skip(1).map(len).max().unwrap_or(0);
    assert!(
        ahead > buffer::PAGES_DECODED_AHEAD / 2 && ahead <= buffer::PAGES_DECODED_AHEAD,
        "{reads:?}"
    );
    reads_each_row_once(parquet_state(dir.path(), 30_000, 8), 300);

    // Rows of 4 KB: the bytes read ahead bound the rows.
    let mut wide = parquet_state(dir.path(), 5_000, 4_000);
    let reads = page_down(&mut wide, 5);
    let most = reads.iter().skip(1).map(len).max().unwrap_or(0);
    assert!(most < 4_000, "{reads:?}");
}
