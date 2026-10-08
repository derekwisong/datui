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
        if i % 11 == 3 {
            text.push_str("\r\n");
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
        marks.read(start, 40, None).unwrap();
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

/// xorshift: the same cases on every run.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn chance(&mut self, percent: usize) -> bool {
        self.below(100) < percent
    }
}

/// How a random CSV is written.
struct Style {
    separator: u8,
    quote: u8,
    comment: Option<&'static str>,
    blank_lines: bool,
    crlf: bool,
    ragged: bool,
    stray_quote: bool,
}

fn random_field(r: &mut Rng, style: &Style) -> String {
    let q = style.quote as char;
    let sep = style.separator as char;
    match r.below(10) {
        0 => String::new(),
        1 => format!("{}", r.below(1000)),
        2 => format!("{q}a{sep}b\nc{q}"),
        3 => format!("{q}x{q}{q}y{q}{q}\r\nz{q}"),
        4 => format!("{q}{q}"),
        5 if style.stray_quote => format!("ab{q}c"),
        6 => format!("{q}#not a comment\n# still not{q}"),
        7 => "NA".into(),
        _ => format!("w{}", r.below(50)),
    }
}

/// A CSV of `rows` rows of `columns` fields in `style`: short and long rows, blank
/// lines, comments, quoted line ends, quotes inside fields, a BOM, CRLF.
fn random_csv(r: &mut Rng, style: &Style, columns: usize, rows: usize, header: bool) -> String {
    let eol = if style.crlf { "\r\n" } else { "\n" };
    let sep = (style.separator as char).to_string();
    let mut text = String::new();
    if r.chance(20) {
        text.push('\u{feff}');
    }
    if header {
        let names: Vec<String> = (0..columns).map(|i| format!("c{i}")).collect();
        text.push_str(&names.join(&sep));
        text.push_str(eol);
    }
    for i in 0..rows {
        if let Some(c) = style.comment
            && r.chance(10)
        {
            let q = style.quote as char;
            text.push_str(&format!("{c} comment {q}open{sep}{i}{eol}"));
        }
        if style.blank_lines && r.chance(8) {
            text.push_str(eol);
        }
        let n = if style.ragged && r.chance(10) {
            if r.chance(50) {
                columns + 1
            } else {
                columns.saturating_sub(1).max(1)
            }
        } else {
            columns
        };
        let mut fields: Vec<String> = (0..n).map(|_| random_field(r, style)).collect();
        fields[0] = format!("{i}");
        text.push_str(&fields.join(&sep));
        if i + 1 < rows || r.chance(70) {
            text.push_str(if style.crlf && r.chance(80) {
                "\r\n"
            } else {
                "\n"
            });
        }
    }
    text
}

/// Random windows of `lf` from marks, in both engines: each is the rows Polars' own
/// slice gives, or what the whole frame holds there; an error only where the slice
/// fails too.
fn random_windows_hold(
    lf: &LazyFrame,
    ignores_errors: bool,
    r: &mut Rng,
    label: &str,
) -> Vec<String> {
    let marks = CsvMarks::of(lf);
    if ignores_errors {
        // Polars' slice alone decides what such a file shows.
        return match marks {
            Some(_) => vec![format!("marks with errors ignored: {label}")],
            None => Vec::new(),
        };
    }
    let Some(marks) = marks else {
        return vec![format!("no marks: {label}")];
    };
    let mut wrong = Vec::new();
    for streaming in [false, true] {
        let Ok(whole) = crate::analysis::statistics::collect_lazy(lf.clone(), streaming) else {
            continue;
        };
        for _ in 0..8 {
            let start = r.below(whole.height() + 3);
            let len = 1 + r.below(12);
            let Some(window) = marks.window(lf, start, len) else {
                wrong.push(format!("no window: {label}"));
                continue;
            };
            let truth = whole.slice(start as i64, len);
            let old = crate::analysis::statistics::collect_lazy(
                lf.clone().slice(start as i64, len as IdxSize),
                streaming,
            );
            let got = crate::analysis::statistics::collect_lazy(window, streaming);
            let fine = match (&got, &old) {
                (Ok(got), Ok(old)) => got.equals_missing(old) || got.equals_missing(&truth),
                (Ok(got), Err(_)) => got.equals_missing(&truth),
                (Err(_), old) => old.is_err(),
            };
            if !fine {
                wrong.push(format!(
                    "{label}: rows {start}+{len} streaming {streaming}: got {got:?}, old {old:?}"
                ));
            }
        }
    }
    wrong
}

fn write_csv(text: &str) -> (tempfile::TempDir, std::path::PathBuf) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.csv");
    std::fs::write(&path, text).unwrap();
    (dir, path)
}

/// Random CSVs as datui opens them, with its options: no window from marks is a
/// wrong row or an error the slice would not give.
#[test]
fn random_csvs_read_from_marks_as_polars_reads_them() {
    let mut r = Rng(0x1234_5678_9abc_def1);
    let mut wrong = Vec::new();
    for case in 0..300 {
        let style = Style {
            separator: b',',
            quote: b'"',
            comment: [None, Some("#"), Some("//")][r.below(3)],
            blank_lines: r.chance(40),
            crlf: r.chance(30),
            ragged: false,
            stray_quote: r.chance(20),
        };
        let columns = 1 + r.below(4);
        let rows = r.below(60);
        let variant = r.below(7);
        let mut body = random_csv(&mut r, &style, columns, rows, variant != 2);
        let mut options = OpenOptions {
            comment_char: style.comment.map(String::from),
            ..OpenOptions::default()
        };
        match variant {
            1 => {
                body = format!("junk \"line\nmore, junk\n{body}");
                options.skip_lines = Some(2);
            }
            2 => options.has_header = Some(false),
            3 => options.skip_rows = Some(r.below(3)),
            4 => options.null_values = Some(vec!["NA".into(), "w3".into()]),
            5 => options.skip_initial_space = true,
            6 => options.ignore_errors = true,
            _ => {}
        }
        let (_dir, path) = write_csv(&body);
        let Ok(read) = crate::formats::readers::csv::read_delimited(
            &path,
            b',',
            &options,
            &Default::default(),
        ) else {
            continue;
        };
        wrong.extend(random_windows_hold(
            &read.lf,
            options.ignore_errors,
            &mut r,
            &format!("case {case} variant {variant}: {body:?}"),
        ));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Random CSVs scanned with Polars' own options: separators, quote characters,
/// ragged rows truncated or ignored, missing fields null or not.
#[test]
fn random_csvs_with_polars_options_read_from_marks() {
    let mut r = Rng(0xdead_beef_cafe_f00d);
    let mut wrong = Vec::new();
    for case in 0..300 {
        let style = Style {
            separator: b",;\t"[r.below(3)],
            quote: b"\"'"[r.below(2)],
            comment: [None, Some("#")][r.below(2)],
            blank_lines: r.chance(40),
            crlf: r.chance(30),
            ragged: r.chance(40),
            stray_quote: r.chance(20),
        };
        let columns = 1 + r.below(4);
        let rows = r.below(60);
        let body = random_csv(&mut r, &style, columns, rows, true);
        let (_dir, path) = write_csv(&body);
        let truncate = style.ragged && r.chance(50);
        let ignore = style.ragged && !truncate;
        let lf = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .with_glob(false)
            .with_separator(style.separator)
            .with_quote_char(Some(style.quote))
            .with_comment_prefix(style.comment.map(PlSmallStr::from))
            .with_truncate_ragged_lines(truncate)
            .with_ignore_errors(ignore)
            .with_missing_is_null(r.chance(50))
            .with_encoding(CsvEncoding::LossyUtf8)
            .finish()
            .unwrap();
        // The plan's conversion is cached at an open; here, by asking for the schema.
        let _ = lf.clone().collect_schema();
        wrong.extend(random_windows_hold(
            &lf,
            ignore,
            &mut r,
            &format!("case {case}: {body:?}"),
        ));
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Short rows, a blank line and trailing blank lines read as nulls, as Polars reads
/// them, wherever a window starts.
#[test]
fn short_rows_and_blank_lines_read_as_polars_reads_them() {
    for text in [
        "c0,c1,c2\n0,a,b\n1,a\n2,a,b\n3,a,b\n4,a,b\n",
        "c0,c1,c2\n0,a,b\n1\n2,a,b\n3,a,b\n4,a,b\n",
        "c0,c1,c2\n\n0,a,b\n1,a,b\n",
        "c0,c1\n0,a\n\n\n1,b\n2,c\n",
        "c0,c1\n0,a\n1,b\n\n\n",
        "c0,c1\r\n0,a\r\n\r\n1,b\r\n2,c\r\n",
    ] {
        let (_dir, lf) = frame(text, &OpenOptions::default());
        windows_match(&lf);
    }
}

/// A quote inside a field (`24" monitor`): Polars' parser reads past it, but its
/// chunker takes it to open a quoted run, so the two counts part there and the marks
/// break. Every window then reads as Polars' slice reads it, erring where it errs.
/// With `--ignore-errors` there are no marks at all.
#[test]
fn a_quote_inside_a_field_reads_as_polars_slice() {
    let mut text = String::from("id,desc,price\n");
    for i in 0..3_000 {
        if i % 500 == 17 {
            text.push_str(&format!("{i},24\" monitor,{i}\n"));
        } else {
            text.push_str(&format!("{i},item {i},{i}.5\n"));
        }
    }
    let (_dir, lf) = frame(&text, &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    for start in (0..3_000).step_by(97) {
        let got = marks.window(&lf, start, 40).unwrap().collect();
        let old = lf.clone().slice(start as i64, 40).collect();
        match (got, old) {
            (Ok(got), Ok(old)) => assert!(got.equals_missing(&old), "rows {start}+40"),
            // Before the quote, rows where Polars' slice fails reading on past them are
            // the file's lines, in place.
            (Ok(got), Err(_)) => {
                assert!(start < 17, "rows {start}+40 past the quote");
                let ids: Vec<Option<i64>> =
                    got.column("id").unwrap().i64().unwrap().iter().collect();
                let lines: Vec<Option<i64>> =
                    (start..start + ids.len()).map(|i| Some(i as i64)).collect();
                assert_eq!(ids, lines, "rows {start}+40");
            }
            (Err(e), old) => assert!(old.is_err(), "rows {start}+40 fail only from marks: {e}"),
        }
    }
    assert!(marks.known.lock().unwrap().broken, "the counts parted");

    let ignoring = OpenOptions {
        ignore_errors: true,
        ..OpenOptions::default()
    };
    let (_dir, lf) = frame(&text, &ignoring);
    assert!(CsvMarks::of(&lf).is_none(), "no marks with errors ignored");
}

/// The two counts agree on an unclosed quote at a field's start (both read on to the
/// next quote): the rows past it are Polars' own reading of those bytes, merged rows
/// and all, and match its slice wherever that reads them.
#[test]
fn an_unclosed_quote_reads_as_polars_reads_it() {
    let mut text = String::from("id,note\n");
    for i in 0..200 {
        let note = if i == 50 {
            "\"open".to_string()
        } else {
            format!("n{i}")
        };
        text.push_str(&format!("{i},{note}\n"));
    }
    let (_dir, lf) = frame(&text, &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    for start in [0, 40, 49, 50, 100, 150] {
        let got = marks.window(&lf, start, 20).unwrap().collect();
        let old = lf.clone().slice(start as i64, 20).collect();
        match (got, old) {
            (Ok(got), Ok(old)) => assert!(got.equals_missing(&old), "rows {start}+20"),
            (Ok(got), Err(_)) => {
                assert!(start + got.height() <= 50, "rows {start}+20 past the quote");
                let ids: Vec<Option<i64>> =
                    got.column("id").unwrap().i64().unwrap().iter().collect();
                let lines: Vec<Option<i64>> =
                    (start..start + ids.len()).map(|i| Some(i as i64)).collect();
                assert_eq!(ids, lines, "rows {start}+20");
            }
            (Err(_), old) => assert!(old.is_err(), "rows {start}+20 fail only from marks"),
        }
    }
}

/// A window that reads no columns (a count) parses one to check the rows against, and
/// does not break the marks.
#[test]
fn a_window_of_no_columns_counts_its_rows() {
    let (_dir, lf) = frame(&many_rows(200), &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    let rows = marks.read(150, 20, Some(&[])).unwrap();
    assert_eq!((rows.height(), rows.width()), (20, 0));
    assert!(!marks.known.lock().unwrap().broken);
}

/// A date parse with no format infers it from the values it sees: from a window's it
/// could infer another than the whole column's, so it is read through Polars. One with
/// a format reads from marks.
#[test]
fn a_date_parse_with_no_format_is_not_read_from_marks() {
    let mut text = String::from("id,d\n");
    for i in 0..400 {
        if i < 200 {
            text.push_str(&format!("{i},2020-01-{:02}\n", i % 28 + 1));
        } else {
            text.push_str(&format!("{i},{:02}/01/2020\n", i % 28 + 1));
        }
    }
    let options = OpenOptions {
        parse_dates: false,
        infer_schema_length: Some(0),
        ..OpenOptions::default()
    };
    let (_dir, lf) = frame(&text, &options);
    let marks = CsvMarks::of(&lf).unwrap();
    let parse = |format: Option<&str>| {
        let options = StrptimeOptions {
            format: format.map(Into::into),
            strict: false,
            exact: true,
            cache: true,
        };
        lf.clone()
            .with_columns([col("d").str().to_date(options).alias("parsed")])
    };
    assert!(marks.window(&parse(None), 250, 3).is_none());
    let explicit = parse(Some("%Y-%m-%d"));
    let got = marks.window(&explicit, 250, 3).unwrap().collect().unwrap();
    let old = explicit.slice(250, 3).collect().unwrap();
    assert!(got.equals_missing(&old));
}

/// A hidden column is not parsed from a mark, as Polars' scan skips it: a value it
/// cannot read fails nothing.
#[test]
fn a_hidden_column_is_not_parsed() {
    let mut text = String::from("a,b\n");
    for i in 0..300 {
        if i == 150 {
            text.push_str(&format!("{i},N/A\n"));
        } else {
            text.push_str(&format!("{i},{i}\n"));
        }
    }
    let options = OpenOptions {
        infer_schema_length: Some(100),
        ..OpenOptions::default()
    };
    let (_dir, lf) = frame(&text, &options);
    let marks = CsvMarks::of(&lf).unwrap();
    let shown = lf.clone().select([col("a")]);
    let old = shown.clone().slice(140, 20).collect().unwrap();
    let got = marks.window(&shown, 140, 20).unwrap().collect().unwrap();
    assert!(got.equals_missing(&old), "{got}\nwanted\n{old}");
}

/// Steps that read other rows than their own are not read from marks.
#[test]
fn only_per_row_expressions_read_from_marks() {
    let (_dir, lf) = frame(&many_rows(50), &OpenOptions::default());
    let marks = CsvMarks::of(&lf).unwrap();
    let per_row = [
        col("id") * lit(2),
        col("name").str().to_uppercase(),
        when(col("id").gt(lit(3))).then(lit(1)).otherwise(lit(0)),
        col("amount").cast(DataType::Int64),
    ];
    for expr in per_row {
        let shown = lf.clone().with_columns([expr.clone().alias("x")]);
        assert!(marks.window(&shown, 3, 2).is_some(), "{expr:?}");
    }
    let across_rows = [
        col("id").shift(lit(1)),
        col("id").reverse(),
        col("id").sum(),
        col("id").unique(),
        col("id").sum().over([col("name")]).unwrap(),
        col("id").sort(Default::default()),
    ];
    for expr in across_rows {
        let shown = lf.clone().with_columns([expr.clone().alias("x")]);
        assert!(marks.window(&shown, 3, 2).is_none(), "{expr:?}");
    }
}

/// The steps datui's own reads add to a CSV (trimmed names, spaces skipped, text
/// typed, dates parsed) keep the scan's rows in place: its windows read from marks.
#[test]
fn datuis_reads_of_a_csv_read_from_marks() {
    let text = "id, name ,when,amount,code\n1, a,2020-01-02,1.5,007\n2, b,2020-01-03,2.5,008\n";
    for (case, options) in [
        OpenOptions::default(),
        OpenOptions {
            skip_initial_space: true,
            ..OpenOptions::default()
        },
        OpenOptions {
            parse_strings: Some(crate::loading::open_options::ParseStringsTarget::All),
            ..OpenOptions::default()
        },
        OpenOptions {
            null_values: Some(vec!["a".into()]),
            parse_dates: true,
            ..OpenOptions::default()
        },
    ]
    .into_iter()
    .enumerate()
    {
        let (_dir, lf) = frame(text, &options);
        let marks = CsvMarks::of(&lf).expect("one CSV scan");
        let window = marks.window(&lf, 1, 1);
        assert!(window.is_some(), "case {case}: {:?}", lf.logical_plan);
        windows_of(&marks, &lf);
    }
}

/// Random files of quotes where Polars' parser and chunker part (`24" tv`, `O"Brien`,
/// `5'11"`), quoted separators and doubled quotes, rows over two lines, unclosed
/// quotes, CRLF. A window from marks shows Polars' slice, or, where the slice fails or
/// reads otherwise, the rows the file was written with, up to any unclosed quote.
/// With `--ignore-errors` there are no marks.
#[test]
fn where_polars_slice_fails_marks_show_only_the_files_rows() {
    let mut r = Rng(0x5151_7777_aaaa_0001);
    let mut wrong = Vec::new();
    for case in 0..300 {
        let rows = 5 + r.below(300);
        let unclosed_at = r.chance(30).then(|| r.below(rows));
        let eol = if r.chance(30) { "\r\n" } else { "\n" };
        let mut text = format!("id,desc,n{eol}");
        let mut written: Vec<String> = Vec::new();
        for i in 0..rows {
            let (raw, value) = if Some(i) == unclosed_at {
                ("\"oops".to_string(), None)
            } else {
                match r.below(8) {
                    0 => (format!("{}\" tv", r.below(90)), None),
                    1 => ("O\"Brien".to_string(), None),
                    2 => ("\"q, \"\"x\"\"\"".to_string(), Some("q, \"x\"".to_string())),
                    3 => (
                        format!("\"multi{eol}line\""),
                        Some(format!("multi{eol}line")),
                    ),
                    4 => ("5'11\"".to_string(), None),
                    _ => (format!("w{i}"), None),
                }
            };
            written.push(value.unwrap_or_else(|| raw.clone()));
            text.push_str(&format!("{i},{raw},{i}{eol}"));
        }
        let ignore_errors = r.chance(30);
        let options = OpenOptions {
            ignore_errors,
            ..OpenOptions::default()
        };
        let (_dir, path) = write_csv(&text);
        let Ok(read) = crate::formats::readers::csv::read_delimited(
            &path,
            b',',
            &options,
            &Default::default(),
        ) else {
            continue;
        };
        let lf = read.lf;
        let marks = CsvMarks::of(&lf);
        if ignore_errors {
            if marks.is_some() {
                wrong.push(format!("case {case}: marks with errors ignored"));
            }
            continue;
        }
        let Some(marks) = marks else {
            wrong.push(format!("case {case}: no marks"));
            continue;
        };
        for _ in 0..20 {
            let start = r.below(rows + 2);
            let len = 1 + r.below(30);
            let old = lf.clone().slice(start as i64, len as IdxSize).collect();
            let Ok(got) = marks.window(&lf, start, len).unwrap().collect() else {
                if old.is_ok() {
                    wrong.push(format!(
                        "case {case}: rows {start}+{len} fail only from marks"
                    ));
                }
                continue;
            };
            if old.as_ref().is_ok_and(|old| old.equals_missing(&got)) {
                continue;
            }
            let ids = got.column("id").unwrap().cast(&DataType::Int64).unwrap();
            let ids = ids.i64().unwrap();
            let desc = got.column("desc").unwrap().cast(&DataType::String).unwrap();
            let desc = desc.str().unwrap();
            for k in 0..got.height() {
                let row = start + k;
                if unclosed_at.is_some_and(|u| row >= u) {
                    break;
                }
                let right = row < written.len()
                    && ids.get(k) == Some(row as i64)
                    && desc.get(k).unwrap_or("") == written[row].trim_end_matches('\r');
                if !right {
                    wrong.push(format!(
                        "case {case}: row {row} shows id {:?} desc {:?}, written {:?}",
                        ids.get(k),
                        desc.get(k),
                        written.get(row)
                    ));
                    break;
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
