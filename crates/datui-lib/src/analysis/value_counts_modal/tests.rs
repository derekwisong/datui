use super::*;
use polars::prelude::*;

/// A count of a dataset of files says how many the rows it has read reach.
#[test]
fn a_count_says_how_many_files_its_rows_reach() {
    let computing = Computing {
        column: "k".to_string(),
        exact: false,
        watch: ReadWatch::default(),
        file_starts: Some(std::sync::Arc::new(vec![0, 100, 250, 400])),
    };
    assert_eq!(computing.files_reached(), None, "nothing read yet");
    computing.watch.saw(0);
    assert_eq!(computing.files_reached(), Some((1, 3)));
    computing.watch.saw(150);
    assert_eq!(computing.files_reached(), Some((2, 3)));
    computing.watch.saw(1_000);
    assert_eq!(computing.files_reached(), Some((3, 3)));
}

fn counts(column: &str, values: &[i32]) -> ValueCounts {
    crate::analysis::value_counts::Plan {
        lf: DataFrame::new_infer_height(vec![Column::new(column.into(), values)])
            .unwrap()
            .lazy(),
        column: column.to_string(),
        read: crate::analysis::value_counts::Read::Exact,
        known_total: None,
        streaming: false,
    }
    .run(&ReadWatch::default())
    .unwrap()
}

#[test]
fn counts_are_held_per_column_and_dropped_with_the_frame() {
    let mut modal = ValueCountsModal::default();
    let columns = vec!["a".to_string(), "b".to_string()];
    modal.open(columns.clone(), 0, 1);
    modal.hold(counts("a", &[1, 1, 2]));
    assert!(modal.current().is_some());
    assert!(modal.step(1));
    assert!(modal.current().is_none(), "b is not counted yet");
    assert!(!modal.step(1), "no column past the last");
    assert!(modal.step(-1));
    assert_eq!(modal.current().unwrap().summary.rows, 3);

    modal.open(columns.clone(), 0, 1);
    assert!(modal.current().is_some(), "the same view keeps its counts");
    modal.open(columns, 0, 2);
    assert!(modal.current().is_none(), "another view's counts go");
}

/// A number column opens as its histogram; `c` turns to the listing and back,
/// and the next column starts from its own type again.
#[test]
fn numbers_open_as_a_histogram_and_c_toggles() {
    let mut modal = ValueCountsModal::default();
    modal.open(vec!["n".to_string(), "s".to_string()], 0, 1);
    modal.hold(counts("n", &[1, 1, 2, 3, 3, 3]));
    assert!(modal.shows_histogram());
    let histogram = modal.current().unwrap().histogram.clone().unwrap();
    assert_eq!(
        histogram.bins.iter().map(|b| b.count).collect::<Vec<_>>(),
        [2.0, 1.0, 3.0],
        "a bin per value of a short integer range"
    );
    modal.toggle_view();
    assert!(!modal.shows_histogram());
    modal.toggle_view();
    assert!(modal.shows_histogram());
    modal.toggle_view();
    assert!(modal.step(1));
    let text = crate::analysis::value_counts::Plan {
        lf: DataFrame::new_infer_height(vec![Column::new("s".into(), ["a", "b"])])
            .unwrap()
            .lazy(),
        column: "s".to_string(),
        read: crate::analysis::value_counts::Read::Exact,
        known_total: None,
        streaming: false,
    }
    .run(&ReadWatch::default())
    .unwrap();
    modal.hold(text);
    assert!(!modal.shows_histogram(), "text has no histogram");
    modal.toggle_view();
    assert!(!modal.shows_histogram());
    assert!(modal.step(-1));
    assert!(modal.shows_histogram(), "back to the number's default");
}

#[test]
fn the_cursor_stays_on_the_listing_and_in_view() {
    let mut modal = ValueCountsModal::default();
    modal.open(vec!["a".to_string()], 0, 1);
    modal.hold(counts("a", &[1, 2, 3, 4, 5, 6]));
    modal.move_by(10);
    assert_eq!(modal.selected, 5);
    modal.scroll_into_view(3);
    assert_eq!(modal.offset, 3);
    modal.move_by(-10);
    modal.scroll_into_view(3);
    assert_eq!((modal.selected, modal.offset), (0, 0));
    modal.toggle_order();
    assert_eq!(modal.order, Order::Value);
}
