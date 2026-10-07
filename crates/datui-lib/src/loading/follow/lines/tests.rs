use super::*;

#[test]
fn only_whole_lines_are_complete_until_the_end() {
    assert_eq!(complete(b"", false), 0);
    assert_eq!(complete(b"{\"a\":", false), 0);
    assert_eq!(complete(b"{\"a\":1}\n{\"a\"", false), 8);
    assert_eq!(complete(b"{\"a\":1}\n", false), 8);
    assert_eq!(complete(b"{\"a\":1}\n{\"a\":2}", true), 15);
}

/// Short lines (blank, whitespace, not JSON) among the objects never fail a read:
/// every read gives the objects, and a line that is not JSON a row of nulls.
#[test]
fn short_and_bad_lines_read_as_the_watcher_counts_them() {
    let dir = tempfile::tempdir().unwrap();
    let cases: [(&str, Vec<Option<i64>>); 4] = [
        ("{\"a\":1}\n\n   \n{\"a\":2}\n", vec![Some(1), Some(2)]),
        (
            "{\"a\":1}\n{\"a\":2}\ngarbage\n{\"a\":3}\n",
            vec![Some(1), Some(2), None, Some(3)],
        ),
        (
            "{\"a\":1} \n{\"a\":2}\t\n{\"a\":3}\n",
            vec![Some(1), Some(2), Some(3)],
        ),
        ("\u{feff}{\"a\":1}\n{\"a\":2}\n", vec![Some(1), Some(2)]),
    ];
    for (text, ids) in cases {
        let path = dir.path().join("short.ndjson");
        std::fs::write(&path, text).unwrap();
        let lf = LinesScan::open(&path, None, true, None)
            .unwrap()
            .lazy()
            .unwrap();
        let all = lf.clone().collect().unwrap();
        let got: Vec<Option<i64>> = all.column("a").unwrap().i64().unwrap().to_vec();
        assert_eq!(got, ids, "{text:?}");
        let head = lf.clone().slice(0, 2).collect().unwrap();
        assert_eq!(head.height(), 2, "{text:?}");
        let kept = lf.clone().filter(col("a").gt(lit(1))).collect().unwrap();
        let above = ids.iter().filter(|v| v.is_some_and(|v| v > 1)).count();
        assert_eq!(kept.height(), above, "{text:?}");
        let count = lf.clone().select([len()]).collect().unwrap();
        assert_eq!(
            count
                .column("len")
                .unwrap()
                .get(0)
                .unwrap()
                .extract::<usize>(),
            Some(ids.len()),
            "{text:?}"
        );
    }
}

#[test]
fn a_run_ends_at_a_line_s_end() {
    let bytes = b"{\"a\":1}\n{\"a\":22}\n{\"a\":3}";
    assert_eq!(run_end(bytes, 0, 3), 8);
    assert_eq!(run_end(bytes, 8, 3), 17);
    assert_eq!(run_end(bytes, 17, 3), bytes.len());
    assert_eq!(run_end(bytes, 0, 100), bytes.len());
}

#[test]
fn a_read_of_some_rows_skips_blank_lines() {
    let bytes = b"{\"a\":1}\n\n{\"a\":2}\n  \n{\"a\":3}\n";
    assert_eq!(after_rows(bytes, 0), 0);
    assert_eq!(after_rows(bytes, 1), 8);
    assert_eq!(after_rows(bytes, 2), 17);
    assert_eq!(after_rows(bytes, 3), bytes.len());
    assert_eq!(after_rows(bytes, 9), bytes.len());
}

/// The scan reads what the file holds at each moment, through its last newline,
/// wherever the writer has got to: never an error for an object cut off.
#[test]
fn a_cut_off_last_object_is_never_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("growing.ndjson");
    let mut text = String::new();
    for i in 0..200 {
        text.push_str(&format!("{{\"id\": {i}, \"msg\": \"line {i}\"}}\n"));
    }
    let bytes = text.as_bytes();
    let first = bytes.iter().position(|&b| b == b'\n').unwrap() + 1;
    std::fs::write(&path, &bytes[..first]).unwrap();
    let lf = LinesScan::open(&path, None, true, None)
        .unwrap()
        .lazy()
        .unwrap();
    for cut in (first..=bytes.len()).step_by(7).chain([bytes.len()]) {
        std::fs::write(&path, &bytes[..cut]).unwrap();
        let whole = bytes[..cut].iter().filter(|&&b| b == b'\n').count();
        let df = lf.clone().collect().unwrap();
        assert_eq!(df.height(), whole, "cut at {cut}");
        let count = lf.clone().select([len()]).collect().unwrap();
        assert_eq!(
            count
                .column("len")
                .unwrap()
                .get(0)
                .unwrap()
                .extract::<usize>(),
            Some(whole),
            "count at {cut}"
        );
        let filtered = lf
            .clone()
            .filter(col("id").gt_eq(lit(0)))
            .select([col("msg")])
            .collect()
            .unwrap();
        assert_eq!(filtered.height(), whole, "filter at {cut}");
        let head = lf.clone().slice(0, 3).collect().unwrap();
        assert_eq!(head.height(), whole.min(3), "head at {cut}");
    }
}
