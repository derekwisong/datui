//! A view's sample as its rows land: the rows on hand stand while nothing reorders
//! them, and the frame ends as one chunk a column.

use crate::table::DataTableState;
use crate::table_sample::{Drawn, SampleRows};
use polars::prelude::*;
use std::sync::Arc;

fn chunk(from: i64) -> DataFrame {
    df!("id" => (from..from + 10).collect::<Vec<_>>()).unwrap()
}

#[test]
fn rows_on_hand_stand_until_a_sort_and_the_end_is_one_chunk() {
    let source =
        DataTableState::from_lazyframe(chunk(0).lazy(), &crate::OpenOptions::default()).unwrap();
    let rows = Arc::new(SampleRows::default());
    let schema = chunk(0).schema().clone();
    let mut view = DataTableState::sampled_from(
        source,
        crate::sampling::Sample::default(),
        &schema,
        Arc::clone(&rows),
        false,
        None,
    )
    .unwrap();
    assert_eq!(view.sample_grew(), None, "nothing came");
    rows.push(0, chunk(0));
    assert_eq!(
        view.sample_grew(),
        Some(true),
        "rows after the ones on hand"
    );
    view.sort_by(vec!["id".to_string()], vec![true]);
    rows.push(10, chunk(10));
    assert_eq!(
        view.sample_grew(),
        Some(false),
        "a sort puts new rows among the old: the rows on hand go"
    );
    for at in 2..6i64 {
        rows.push(at as u64 * 10, chunk(at * 10));
        view.sample_grew();
    }
    let sampled = view.sampled().unwrap();
    assert!(sampled.frame().first_col_n_chunks() > 1);
    view.sample_drawn(Drawn::default());
    let sampled = view.sampled().unwrap();
    assert_eq!(sampled.rows(), 60);
    assert_eq!(
        sampled.frame().first_col_n_chunks(),
        1,
        "one chunk a column"
    );
}
