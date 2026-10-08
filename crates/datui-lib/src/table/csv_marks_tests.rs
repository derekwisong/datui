use super::*;
use crate::OpenOptions;

/// `text` written as a CSV and read as an open reads it.
fn frame(text: &str, options: &OpenOptions) -> (tempfile::TempDir, LazyFrame) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    std::fs::write(&path, text).unwrap();
    let read =
        crate::formats::readers::csv::read_delimited(&path, b',', options, &Default::default())
            .unwrap();
    (dir, read.lf)
}

/// Every window of `lf` read from marks equals Polars' slice of it, in both engines,
/// read in an order that jumps back and forth.
fn windows_match(lf: &LazyFrame) {
    windows_of(&CsvMarks::of(lf).expect("one CSV scan"), lf);
}

fn windows_of(marks: &Arc<CsvMarks>, lf: &LazyFrame) {
    let rows = lf.clone().collect().unwrap().height();
    let mut starts: Vec<usize> = (0..=rows + 2).collect();
    starts.reverse();
    let half = starts.len() / 2;
    starts.rotate_left(half);
    for start in starts {
        for len in [1, 3, 17] {
            let window = marks.window(lf, start, len).expect("a window of the scan");
            let expected = lf.clone().slice(start as i64, len as IdxSize);
            for streaming in [false, true] {
                let got =
                    crate::analysis::statistics::collect_lazy(window.clone(), streaming).unwrap();
                let want =
                    crate::analysis::statistics::collect_lazy(expected.clone(), streaming).unwrap();
                assert!(
                    got.equals_missing(&want),
                    "rows {start}+{len} (streaming {streaming}):\n{got}\nwanted\n{want}"
                );
            }
        }
    }
}

fn many_rows(n: usize) -> String {
    let mut text = String::from("id,name,when,amount\n");
    for i in 0..n {
        text.push_str(&format!(
            "{i},name {i},2020-01-{:02},{}.5\n",
            i % 28 + 1,
            i * 3
        ));
    }
    text
}

#[test]
fn a_window_from_a_mark_is_polars_slice() {
    let (_dir, lf) = frame(&many_rows(120), &OpenOptions::default());
    windows_match(&lf);
}

#[test]
fn quoted_line_ends_and_commas_are_inside_a_row() {
    let mut text = String::from("id,note\n");
    for i in 0..60 {
        text.push_str(&format!("{i},\"line one\nline, two \"\"{i}\"\"\"\n"));
    }
    let (_dir, lf) = frame(&text, &OpenOptions::default());
    windows_match(&lf);
}

#[test]
fn comments_blank_lines_and_crlf_count_as_polars_counts_them() {
    let mut text = String::from("\u{feff}id,v\r\n");
    for i in 0..50 {
        if i % 7 == 0 {
            text.push_str("# a comment, \"unclosed\r\n");
        }
        text.push_str(&format!("{i},{}\r\n", i * 2));
    }
    // No line end after the last row.
    text.push_str("50,100");
    let options = OpenOptions {
        comment_char: Some("#".into()),
        ..OpenOptions::default()
    };
    let (_dir, lf) = frame(&text, &options);
    windows_match(&lf);
}

#[test]
fn skipped_lines_rows_and_no_header_start_where_polars_does() {
    let body: String = (0..40).map(|i| format!("{i},{}\n", i % 5)).collect();
    for (head, options) in [
        (
            "junk line\nmore junk\na,b\n",
            OpenOptions {
                skip_lines: Some(2),
                ..OpenOptions::default()
            },
        ),
        (
            "x,y\n9,9\n8,8\na,b\n",
            OpenOptions {
                skip_rows: Some(3),
                ..OpenOptions::default()
            },
        ),
        (
            "",
            OpenOptions {
                has_header: Some(false),
                ..OpenOptions::default()
            },
        ),
        (
            "a,b\nunit a,unit b\n",
            OpenOptions {
                header_rows: vec![0, 1],
                null_values: Some(vec!["3".into()]),
                ..OpenOptions::default()
            },
        ),
    ] {
        let (_dir, lf) = frame(&format!("{head}{body}"), &options);
        windows_match(&lf);
    }
}

/// A file padded with NULs is scanned from a buffer of its text: windows read from it.
#[test]
fn a_nul_padded_file_is_read_from_its_buffer() {
    let text = format!("{}\0\0\0\0", many_rows(80));
    let (_dir, lf) = frame(&text, &OpenOptions::default());
    windows_match(&lf);
}

/// Windows read one after another, as paging reads them, count each byte of the file
/// about once: each reads on from where the last ended.
#[test]
fn paging_counts_each_byte_about_once() {
    let text = many_rows(2_000);
    let (_dir, lf) = frame(&text, &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    let before = COUNTED.with(std::cell::Cell::get);
    let mut windows = 0usize;
    for start in (0..2_000).step_by(40) {
        // On the worker it runs on in the app; here on this thread.
        marks.read(start, 40).unwrap();
        windows += 1;
    }
    let counted = COUNTED.with(std::cell::Cell::get) - before;
    // A step past a window's last row is counted again a row at a time; from the first
    // row each time would be about `windows / 2` times the file.
    assert!(windows > 40);
    assert!(
        counted <= 2 * text.len(),
        "{counted} bytes counted for a file of {}",
        text.len()
    );
}

/// A window deep in the file leaves marks behind it, so the next one reads on from
/// near itself rather than from the first row.
#[test]
fn a_window_leaves_marks_for_the_next() {
    let (_dir, lf) = frame(&many_rows(2_000), &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    marks.window(&lf, 1_500, 10).unwrap().collect().unwrap();
    let known = marks.known.lock().unwrap();
    assert_eq!(known.at.first().map(|&(row, _)| row), Some(0));
    assert!(
        known.at.len() > 50,
        "a mark every chunk: {}",
        known.at.len()
    );
    assert!(
        known.at.iter().any(|&(row, _)| row == 1_510),
        "the window's end"
    );
    assert!(
        known
            .at
            .windows(2)
            .all(|w| w[0].0 < w[1].0 && w[0].1 < w[1].1)
    );
}

#[test]
fn marks_of_a_file_since_rewritten_are_dropped() {
    let (dir, lf) = frame(&many_rows(300), &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    marks.window(&lf, 250, 5).unwrap().collect().unwrap();
    // Longer rows, so every old mark is mid-row.
    let mut text = String::from("id,name,when,amount\n");
    for i in 0..300 {
        text.push_str(&format!(
            "{i},a longer name {i},2021-02-{:02},{i}.25\n",
            i % 28 + 1
        ));
    }
    std::fs::write(dir.path().join("t.csv"), text).unwrap();
    let got = marks.window(&lf, 250, 5).unwrap().collect().unwrap();
    let want = lf.clone().slice(250, 5).collect().unwrap();
    assert!(got.equals_missing(&want), "{got}\nwanted\n{want}");
}

/// Only a frame that keeps the scan's rows in place reads from marks: a filter or a
/// sort reads its window through Polars.
#[test]
fn a_frame_that_moves_rows_is_not_read_from_marks() {
    let (_dir, lf) = frame(&many_rows(50), &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    assert!(
        marks
            .window(&lf.clone().select([col("id")]), 3, 2)
            .is_some()
    );
    assert!(
        marks
            .window(
                &lf.clone()
                    .with_columns([(col("id") * lit(2)).alias("twice")]),
                3,
                2
            )
            .is_some()
    );
    let renamed = lf.clone().rename(["id"], ["ID"], true);
    assert!(marks.window(&renamed, 3, 2).is_some());
    windows_of(&marks, &renamed);
    assert!(
        marks
            .window(&lf.clone().filter(col("id").gt(lit(3))), 3, 2)
            .is_none()
    );
    assert!(
        marks
            .window(&lf.clone().with_row_index("i", None), 3, 2)
            .is_none()
    );
    assert!(
        marks
            .window(&lf.clone().sort(["id"], Default::default()), 3, 2)
            .is_none()
    );
    assert!(marks.window(&lf.clone().slice(5, 10), 3, 2).is_none());
    // Another scan of the same file is not the one marked.
    let (_other, other) = frame(&many_rows(50), &OpenOptions::default());
    assert!(marks.window(&other, 3, 2).is_none());
}

/// The row ends the scalar walk finds are the ones Polars' counter finds, for text of
/// quotes, separators, comment marks and line ends in every order.
#[test]
fn the_walk_ends_rows_where_polars_counter_does() {
    let alphabet = *b"a,\"\n#\r";
    let mut seed = 0x9e37_79b9_7f4a_7c15u64;
    for case in 0..400 {
        let len = 1 + case % 60;
        let mut bytes: Vec<u8> = (0..len)
            .map(|_| {
                seed ^= seed << 13;
                seed ^= seed >> 7;
                seed ^= seed << 17;
                alphabet[(seed % alphabet.len() as u64) as usize]
            })
            .collect();
        bytes.push(b'\n');
        for comment in [None, Some("#")] {
            let options =
                CsvReadOptions::default().map_parse_options(|p| p.with_comment_prefix(comment));
            let counter = Counter::of(&options);
            let (mut pos, mut rows) = (0, 0);
            while let Some(end) = counter.row_end(&bytes, pos) {
                pos = end;
                rows += 1;
                let (counted, used) = counter.lines.count_rows(&bytes[..pos], false);
                assert_eq!(
                    (counted, used),
                    (rows, pos),
                    "{:?} with comments {}",
                    String::from_utf8_lossy(&bytes),
                    comment.is_some()
                );
            }
        }
    }
}

/// A pristine view of a CSV reads its pages from marks; a sorted one through Polars.
#[test]
fn a_pristine_view_pages_from_marks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    std::fs::write(&path, many_rows(500)).unwrap();
    let options = OpenOptions::default();
    let read =
        crate::formats::readers::csv::read_delimited(&path, b',', &options, &Default::default())
            .unwrap();
    let mut state = super::super::DataTableState::from_read(read, &options).unwrap();
    let from_marks = |state: &super::super::DataTableState| {
        let lf = state.buffer_lf(400, 20).unwrap();
        let anonymous = (&lf.logical_plan).into_iter().any(|node| {
            matches!(node, DslPlan::Scan { scan_type, .. }
                if matches!(**scan_type, FileScanDsl::Anonymous { .. }))
        });
        (anonymous, lf.collect().unwrap())
    };
    let (anonymous, rows) = from_marks(&state);
    assert!(anonymous);
    let want = state.view.lf.clone().slice(400, 20).collect().unwrap();
    assert!(rows.equals_missing(&want), "{rows}\nwanted\n{want}");
    state.sort(vec!["amount".into()], false);
    assert!(!from_marks(&state).0);
}
