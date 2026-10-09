use super::*;

/// The Markdown writer as it was, holding every cell, with each value as
/// exact text: what the bounded one must write, byte for byte.
fn markdown_reference(df: &DataFrame) -> Result<String, String> {
    let column_names = df.get_column_names_owned();
    let escape = |s: &str| s.replace('|', "\\|").replace(['\n', '\r'], " ");
    let mut names: Vec<String> = Vec::with_capacity(column_names.len());
    let mut cells: Vec<Vec<String>> = Vec::with_capacity(column_names.len());
    let mut numeric: Vec<bool> = Vec::with_capacity(column_names.len());
    for name in &column_names {
        let column = df.column(name).map_err(|e| e.to_string())?;
        names.push(escape(name));
        numeric.push(column.dtype().is_primitive_numeric());
        let series = column.as_materialized_series();
        let mut body = Vec::with_capacity(df.height());
        for i in 0..df.height() {
            let value = series.get(i).map_err(|e| e.to_string())?;
            body.push(match value {
                AnyValue::Null => String::new(),
                v => escape(&crate::exact::value_text(&v)),
            });
        }
        cells.push(body);
    }
    let widths: Vec<usize> = names
        .iter()
        .zip(&cells)
        .map(|(name, body)| {
            body.iter()
                .map(|c| c.chars().count())
                .max()
                .unwrap_or(0)
                .max(name.chars().count())
                .max(3)
        })
        .collect();
    // A numeric column is padded to the right, so the raw text reads the way
    // the `---:` delimiter tells a renderer to draw it; the two agree.
    let pad = |s: &str, w: usize, right: bool| {
        let fill = " ".repeat(w - s.chars().count());
        if right {
            format!("{fill}{s}")
        } else {
            format!("{s}{fill}")
        }
    };
    let mut lines = Vec::with_capacity(df.height() + 2);
    lines.push(format!(
        "| {} |",
        names
            .iter()
            .zip(&widths)
            .zip(&numeric)
            .map(|((n, &w), &num)| pad(n, w, num))
            .collect::<Vec<_>>()
            .join(" | ")
    ));
    lines.push(format!(
        "|{}|",
        widths
            .iter()
            .zip(&numeric)
            .map(|(&w, &num)| {
                if num {
                    format!(" {}: ", "-".repeat(w.saturating_sub(1)))
                } else {
                    format!(" {} ", "-".repeat(w))
                }
            })
            .collect::<Vec<_>>()
            .join("|")
    ));
    for row in 0..df.height() {
        lines.push(format!(
            "| {} |",
            cells
                .iter()
                .zip(&widths)
                .zip(&numeric)
                .map(|((body, &w), &num)| pad(&body[row], w, num))
                .collect::<Vec<_>>()
                .join(" | ")
        ));
    }
    Ok(lines.join("\n"))
}

fn tricky() -> DataFrame {
    df!(
        "name" => ["plain", "tab\there", "pipe|and\nnewline", "\"quoted\""],
        "n" => [Some(1i64), Some(2), None, Some(4)],
    )
    .unwrap()
}

#[test]
fn tsv_quotes_only_what_a_paste_needs_quoted() {
    let text = delimited(&tricky(), b'\t', true).unwrap();
    let lines: Vec<&str> = text.split('\n').collect();
    assert_eq!(lines[0], "name\tn");
    assert_eq!(lines[1], "plain\t1");
    // The embedded tab and newline are quoted, so a spreadsheet reads one cell.
    assert_eq!(lines[2], "\"tab\there\"\t2");
    assert!(text.contains("\"pipe|and\nnewline\"\t"));
    // A null is an empty field, and nothing here is the UI's null glyph.
    assert!(!text.contains('∅'));
    assert!(text.ends_with("\"\"\"quoted\"\"\"\t4"), "{text:?}");
}

#[test]
fn header_toggle_is_honored() {
    let with = delimited(&tricky(), b',', true).unwrap();
    let without = delimited(&tricky(), b',', false).unwrap();
    assert!(with.starts_with("name,n"));
    assert!(without.starts_with("plain,1"));
}

#[test]
fn markdown_escapes_aligns_and_keeps_nulls_empty() {
    let text = markdown(&tricky()).unwrap();
    let lines: Vec<&str> = text.split('\n').collect();
    assert!(lines[0].starts_with("| name"));
    // The numeric column's delimiter declares right alignment, and the raw
    // text pads its header and cells the same way, so the two agree.
    assert!(lines[1].contains("-: |"), "{}", lines[1]);
    assert!(lines[0].ends_with("|   n |"), "{}", lines[0]);
    assert!(lines[2].ends_with("|   1 |"), "{}", lines[2]);
    assert!(text.contains("pipe\\|and newline"), "{text}");
    // Every row spans the same padded width.
    let width = lines[0].chars().count();
    assert!(lines.iter().all(|l| l.chars().count() == width), "{text}");
}

#[test]
fn html_flavor_escapes_and_rides_beside_tsv_only() {
    let payload = tabular_payload(&tricky(), CopyFormat::Tsv, true, true).unwrap();
    let html = payload.html.expect("tsv carries the html flavor");
    assert!(html.starts_with("<table><thead>"));
    assert!(html.contains("<td>\"quoted\"</td>"));
    let md = tabular_payload(&tricky(), CopyFormat::Markdown, true, true).unwrap();
    assert!(md.html.is_none(), "markdown is its own rich flavor");
    let text_only = tabular_payload(&tricky(), CopyFormat::Csv, true, false).unwrap();
    assert!(text_only.html.is_none(), "not built where it cannot go");
    assert_eq!(text_only.text, delimited(&tricky(), b',', true).unwrap());
}

/// Every format copies a float as stored, not as Polars' compact display
/// (`1.0000e6`) rounds it for the screen.
#[test]
fn every_format_copies_floats_exactly() {
    let df = df!("x" => [1000000.125f64, -0.0]).unwrap();
    for format in [CopyFormat::Tsv, CopyFormat::Csv, CopyFormat::Markdown] {
        let payload = tabular_payload(&df, format, false, true).unwrap();
        assert!(
            payload.text.contains("1000000.125"),
            "{format:?}: {payload:?}"
        );
        assert!(!payload.text.contains("e6"), "{format:?}: {payload:?}");
        if let Some(html) = payload.html {
            assert!(html.contains("<td>1000000.125</td>"), "{html}");
        }
    }
}

/// Every kind of cell a copy meets: quoting, nulls, wide characters, numbers
/// either side of zero, and a list written as JSON.
fn varied() -> DataFrame {
    let mut df = df!(
        "name" => ["plain", "tab\there", "pipe|and\nnewline", "\"quoted\"", "été", "日本語"],
        "n" => [Some(1i64), Some(-22), None, Some(4), Some(1_000_000), Some(0)],
        "x" => [Some(0.5f64), None, Some(-1.25), Some(3.0), Some(1e-9), Some(2.5)],
    )
    .unwrap();
    let tags: ListChunked = (0..6)
        .map(|i| (i % 2 == 0).then(|| Series::new("".into(), [format!("t{i}"), "a,b".into()])))
        .collect();
    df.with_column(tags.with_name("tags".into()).into_column())
        .unwrap();
    df
}

#[test]
fn markdown_writes_what_it_wrote_holding_every_cell() {
    let empty = varied().head(Some(0));
    for df in [tricky(), varied(), empty] {
        let df = crate::export::nested_json::frame_as_json(&df).unwrap();
        let text = markdown(&df).unwrap();
        assert_eq!(text, markdown_reference(&df).unwrap());
        let layout = {
            let mut layout = MarkdownLayout::new(&df);
            layout.measure(&df).unwrap();
            layout
        };
        assert_eq!(layout.len(df.height()), text.chars().count());
    }
}

#[test]
fn base64_is_sized_before_it_is_encoded() {
    use base64::Engine as _;
    for (bytes, encoded) in [
        (0, 0),
        (1, 4),
        (2, 4),
        (3, 4),
        (4, 8),
        (5, 8),
        (6, 8),
        (7, 12),
    ] {
        assert_eq!(base64_len(bytes), encoded, "{bytes}");
        let text = "a".repeat(bytes);
        let real = base64::engine::general_purpose::STANDARD
            .encode(&text)
            .len();
        assert_eq!(real, encoded);
    }
    // UTF-8 is sized by its bytes: "é" is two.
    assert!(osc52_sequence("ééé", 8).is_ok());
    assert!(osc52_sequence("éééé", 8).is_err());
    // Exactly at the cap goes; a byte more does not.
    assert_eq!(
        osc52_sequence("abcdef", 8).unwrap(),
        "\x1b]52;c;YWJjZGVm\x07"
    );
    let err = osc52_sequence("abcdefg", 8).unwrap_err();
    assert!(err.starts_with("the copy is 1 KB of base64"), "{err}");
}

#[test]
fn a_bounded_table_copy_writes_what_a_whole_one_would() {
    for format in CopyFormat::ALL {
        for header in [true, false] {
            for df in [tricky(), varied(), varied().head(Some(0))] {
                let whole = tabular_payload(&df, format, header, false).unwrap().text;
                let (text, rows) =
                    bounded_table_text(df.clone().lazy(), format, header, 1 << 20).unwrap();
                assert_eq!(text, whole, "{format:?} header {header}");
                assert_eq!(rows, df.height());
            }
        }
    }
}

/// Every copy format, whole and bounded, takes durations as the ISO 8601 a
/// CSV export writes: TSV, CSV, Markdown and the HTML flavor.
#[test]
fn durations_copy_as_a_csv_export_writes_them() {
    use crate::export::nested_json::tests::{duration_text, durations};
    let df = durations();
    let row = |i: usize, separator: &str| {
        duration_text()
            .iter()
            .map(|(_, text)| text[i].unwrap_or(""))
            .collect::<Vec<_>>()
            .join(separator)
    };
    for format in CopyFormat::ALL {
        let payload = tabular_payload(&df, format, true, true).unwrap();
        let (bounded, rows) = bounded_table_text(df.clone().lazy(), format, true, 1 << 20).unwrap();
        assert_eq!(bounded, payload.text, "{format:?}");
        assert_eq!(rows, df.height());
        let lines: Vec<&str> = payload.text.lines().collect();
        match format {
            CopyFormat::Tsv | CopyFormat::Csv => {
                let separator = if format == CopyFormat::Tsv { "\t" } else { "," };
                assert_eq!(lines[0], ["ms", "us", "ns"].join(separator));
                for (i, line) in lines[1..].iter().enumerate() {
                    assert_eq!(*line, row(i, separator), "{format:?} row {i}");
                }
                let html = payload.html.expect("html beside tsv and csv");
                assert!(html.contains("<td>-PT1.5S</td>"), "{html}");
                assert!(html.contains("<tr><td></td><td></td><td></td></tr>"));
            }
            CopyFormat::Markdown => {
                assert_eq!(lines.len(), 2 + df.height());
                for (i, line) in lines[2..].iter().enumerate() {
                    let cells: Vec<&str> =
                        line.trim_matches('|').split('|').map(str::trim).collect();
                    assert_eq!(cells.join(","), row(i, ","), "row {i}");
                }
            }
        }
    }
}

/// Dates and datetimes copy as they always did: TSV and CSV as the CSV writer
/// writes them, Markdown and the HTML flavor as exact text. One past the
/// calendar, on which the writer panics, is its stored number in each.
#[test]
fn dates_copy_as_each_format_wrote_them() {
    use crate::export::nested_json::tests::calendar;
    let df = calendar(false);
    for format in CopyFormat::ALL {
        let payload = tabular_payload(&df, format, true, true).unwrap();
        let (bounded, _) = bounded_table_text(df.clone().lazy(), format, true, 1 << 20).unwrap();
        assert_eq!(bounded, payload.text, "{format:?}");
        let separator = match format {
            CopyFormat::Tsv => b'\t',
            CopyFormat::Csv => b',',
            CopyFormat::Markdown => {
                assert_eq!(payload.text, markdown_reference(&df).unwrap());
                continue;
            }
        };
        let mut written = Vec::new();
        CsvWriter::new(&mut written)
            .with_separator(separator)
            .finish(&mut df.clone())
            .unwrap();
        let written = String::from_utf8(written).unwrap();
        assert_eq!(payload.text, written.trim_end_matches('\n'), "{format:?}");
        assert_eq!(payload.html, Some(html_table(&df, true).unwrap()));
    }
    assert!(
        markdown_reference(&df)
            .unwrap()
            .contains("| 1970-01-01 01:00:00.000000 +01:00 |")
    );

    let past = calendar(true);
    let stored = "-9223372036854775807 us since 1970-01-01 UTC";
    for format in CopyFormat::ALL {
        let payload = tabular_payload(&past, format, true, true).unwrap();
        let (bounded, _) = bounded_table_text(past.clone().lazy(), format, true, 1 << 20).unwrap();
        assert_eq!(bounded, payload.text, "{format:?}");
        assert!(payload.text.contains(stored), "{format:?}");
        if let Some(html) = payload.html {
            assert!(html.contains(&format!("<td>{stored}</td>")), "{html}");
        }
    }
}

/// A failed read says what the collected copy would: Polars' words, tidied.
#[test]
fn a_bounded_table_copy_fails_in_the_words_a_whole_one_does() {
    let lf = df!("s" => ["a"]).unwrap().lazy().select([col("missing")]);
    let whole = crate::analysis::statistics::collect_lazy(lf.clone(), true).unwrap_err();
    let err = bounded_table_text(lf, CopyFormat::Tsv, true, 1 << 20).unwrap_err();
    assert_eq!(
        err,
        crate::error_display::user_message_from_polars(&whole),
        "{whole}"
    );
}

#[test]
fn a_bounded_table_copy_stops_at_the_first_batch_over_the_cap() {
    // 100,000 rows of 16 bytes: 1.6 MB of TSV, against a 64 KB cap.
    let limit = 64 * 1024;
    let rows = 100_000;
    let df = df!("id" => (0..rows as i64).map(|i| i + 1_000_000_000).collect::<Vec<_>>(),
                     "k" => (0..rows as i64).map(|i| i % 10 + 10).collect::<Vec<_>>())
    .unwrap();
    for format in CopyFormat::ALL {
        let mut text = BoundedText::new(format, true, limit);
        let mut taken = 0;
        for offset in (0..rows).step_by(BOUNDED_BATCH_ROWS) {
            taken += 1;
            if text.take(df.slice(offset as i64, BOUNDED_BATCH_ROWS)) {
                break;
            }
        }
        // About 48 KB of text fits: the stop comes within a batch of it.
        let fits = limit / 4 * 3 / 16;
        assert!(
            taken * BOUNDED_BATCH_ROWS <= fits + 2 * BOUNDED_BATCH_ROWS,
            "{format:?}: {taken} batches"
        );
        assert!(text.text.len() <= limit / 4 * 3 + BOUNDED_BATCH_ROWS * 32);
        let err = text.finish().unwrap_err();
        assert!(err.starts_with("the copy is over 64 KB of base64"), "{err}");

        let err = bounded_table_text(df.clone().lazy(), format, true, limit).unwrap_err();
        assert!(err.contains("osc52_limit"), "{err}");
    }
}

#[test]
fn a_capped_destination_takes_text_only() {
    let osc = destination(BackendChoice::Osc52, 4096).unwrap();
    assert_eq!(
        osc.accepts(),
        Accepts {
            html: false,
            base64_limit: Some(4096)
        }
    );
    // Auto is whichever came up: the terminal path where there is no display.
    let auto = destination(BackendChoice::Auto, 4096).unwrap();
    assert_eq!(auto.accepts().html, auto.describe() == "clipboard");
    assert_eq!(
        auto.accepts().base64_limit.is_some(),
        auto.describe() == "terminal"
    );
}

/// Over SSH with no display forwarded, `auto` copies through the terminal without
/// asking a display server; a forwarded display (`ssh -X`) is tried first, as is a
/// local one.
#[test]
fn auto_goes_to_the_terminal_over_ssh_without_a_display() {
    let env = |vars: &'static [(&'static str, &'static str)]| {
        move |name: &str| {
            vars.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| v.to_string())
        }
    };
    assert!(no_display_here(env(&[(
        "SSH_CONNECTION",
        "10.0.0.2 5 10.0.0.1 22"
    )])));
    assert!(no_display_here(env(&[
        ("SSH_TTY", "/dev/pts/3"),
        ("DISPLAY", " ")
    ])));
    assert!(!no_display_here(env(&[
        ("SSH_TTY", "/dev/pts/3"),
        ("DISPLAY", "localhost:10.0")
    ])));
    assert!(!no_display_here(env(&[
        ("SSH_TTY", "/dev/pts/3"),
        ("WAYLAND_DISPLAY", "wayland-1")
    ])));
    assert!(!no_display_here(env(&[("DISPLAY", ":0")])));
    assert!(!no_display_here(env(&[])));
}

#[test]
fn html_escapes_markup_in_values() {
    let df = df!("x" => ["<b>&"]).unwrap();
    let html = html_table(&df, false).unwrap();
    assert!(html.contains("<td>&lt;b&gt;&amp;</td>"), "{html}");
}

#[test]
fn osc52_wraps_base64_and_the_cap_names_the_config() {
    let seq = osc52_sequence("hello", 1024).unwrap();
    assert_eq!(seq, "\x1b]52;c;aGVsbG8=\x07");
    let err = osc52_sequence("hello world, far too long", 8).unwrap_err();
    assert!(err.contains("osc52_limit"), "{err}");
}

#[test]
fn backend_choice_parses_the_config_words() {
    assert_eq!(BackendChoice::parse("auto"), Some(BackendChoice::Auto));
    assert_eq!(BackendChoice::parse("Native"), Some(BackendChoice::Native));
    assert_eq!(BackendChoice::parse("OSC52"), Some(BackendChoice::Osc52));
    assert_eq!(BackendChoice::parse("wayland"), None);
}
