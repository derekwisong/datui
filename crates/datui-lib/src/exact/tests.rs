use super::*;

#[test]
fn floats_read_back_to_the_same_bits() {
    for v in [
        1000000.125,
        0.1 + 0.2,
        -0.0,
        1e300,
        -2.5e-12,
        123456789012345.67,
        f64::MIN_POSITIVE,
        f64::MAX,
        1.0,
    ] {
        let text = f64_text(v);
        let back: f64 = text.parse().unwrap();
        assert_eq!(back.to_bits(), v.to_bits(), "{v} wrote {text}");
    }
    assert_eq!(f64_text(1000000.125), "1000000.125");
    assert_eq!(f64_text(0.1 + 0.2), "0.30000000000000004");
    assert_eq!(f64_text(1e300), "1e300");
    assert_eq!(f64_text(1.0), "1.0");
}

#[test]
fn negative_zero_nan_and_the_infinities_are_spelled_out() {
    assert_eq!(f64_text(-0.0), "-0.0");
    assert_eq!(f64_text(0.0), "0.0");
    assert_eq!(f64_text(f64::NAN), "NaN");
    assert_eq!(f64_text(f64::INFINITY), "inf");
    assert_eq!(f64_text(f64::NEG_INFINITY), "-inf");
    assert_eq!(f32_text(-0.0), "-0.0");
    assert_eq!(f32_text(f32::NAN), "NaN");
}

#[test]
fn an_f32_is_shortest_at_its_own_precision() {
    assert_eq!(f32_text(0.1), "0.1");
    assert_eq!(value_text(&AnyValue::Float32(16777217.0)), "16777216.0");
    let v = 3.4028235e38f32;
    assert_eq!(f32_text(v).parse::<f32>().unwrap(), v);
}

/// Polars' compact display rounds what the table previews; the exact text
/// is the stored value, with no grouping and no settings consulted.
#[test]
fn exact_is_not_the_compact_display() {
    let v = AnyValue::Float64(1000000.125);
    assert_eq!(v.str_value(), "1.0000e6");
    assert_eq!(value_text(&v), "1000000.125");
    assert_eq!(value_text(&AnyValue::Int64(-1234567)), "-1234567");
}

#[test]
fn datetimes_keep_every_digit_of_their_unit_and_the_offset() {
    // 2024-01-02 03:04:05.000120 UTC
    let us = 1_704_164_645_000_120i64;
    assert_eq!(
        value_text(&AnyValue::Datetime(us, TimeUnit::Microseconds, None)),
        "2024-01-02 03:04:05.000120"
    );
    assert_eq!(
        value_text(&AnyValue::Datetime(
            us * 1000 + 7,
            TimeUnit::Nanoseconds,
            None
        )),
        "2024-01-02 03:04:05.000120007"
    );
    let paris = TimeZone::opt_try_new(Some("Europe/Paris"))
        .unwrap()
        .unwrap();
    assert_eq!(
        value_text(&AnyValue::Datetime(
            us,
            TimeUnit::Microseconds,
            Some(&paris)
        )),
        "2024-01-02 04:04:05.000120 +01:00"
    );
    let ms = 1_704_164_645_000i64;
    assert_eq!(
        value_text(&AnyValue::Datetime(ms, TimeUnit::Milliseconds, None)),
        "2024-01-02 03:04:05.000"
    );
}

#[test]
fn dates_times_durations_and_decimals_are_whole() {
    assert_eq!(value_text(&AnyValue::Date(19724)), "2024-01-02");
    assert_eq!(
        value_text(&AnyValue::Time(3_723_000_000_123)),
        "01:02:03.000000123"
    );
    assert_eq!(
        value_text(&AnyValue::Duration(90_061_000_001, TimeUnit::Microseconds)),
        "PT90061.000001S"
    );
    assert_eq!(value_text(&AnyValue::Decimal(-123450, 10, 4)), "-12.3450");
}

#[test]
fn a_null_is_empty_and_bytes_are_base64() {
    assert_eq!(value_text(&AnyValue::Null), "");
    assert_eq!(value_text(&AnyValue::Binary(b"hi\x00")), "aGkA");
}

#[test]
fn the_escaped_view_tells_a_break_from_a_literal_backslash() {
    assert_eq!(escaped("line1\nline2"), r#""line1\nline2""#);
    assert_eq!(escaped(r"line1\nline2"), r#""line1\\nline2""#);
    assert_eq!(escaped("a\tb\r\n"), r#""a\tb\r\n""#);
    assert_eq!(escaped(""), r#""""#);
    assert_eq!(escaped("  pad "), r#""  pad ""#);
    assert_eq!(escaped("say \"hi\""), r#""say \"hi\"""#);
    assert_eq!(escaped("\u{1b}[0m"), r#""\u{1b}[0m""#);
    assert_eq!(
        escaped("no\u{a0}break\u{200b}"),
        r#""no\u{a0}break\u{200b}""#
    );
    assert_eq!(escaped("東京"), "\"東京\"");
}

#[test]
fn bytes_escape() {
    assert_eq!(escaped_bytes(b"ab\x00\xff\""), r#"b"ab\x00\xff\"""#);
}

#[test]
fn the_preview_marks_breaks_tabs_and_controls() {
    let g = crate::glyphs::unicode();
    assert_eq!(preview("line1\nline2", g), "line1¶line2");
    assert_eq!(preview("tab\tseparated", g), "tab»separated");
    assert_eq!(preview("crlf\r\nend", g), "crlf¶end");
    assert_eq!(preview("bell\u{7}", g), "bell¤");
    assert_eq!(preview("c1\u{85}", g), "c1¤");
    assert!(matches!(preview("plain é 東京", g), Cow::Borrowed(_)));
    let a = crate::glyphs::ascii();
    assert_eq!(preview("a\nb\tc\u{1b}", a), "a$b>c?");
}

#[test]
fn text_facts_count_what_the_screen_hides() {
    let f = text_facts("  two\nlines ");
    assert_eq!(
        f,
        TextFacts {
            chars: 12,
            lines: 2,
            leading_spaces: 2,
            trailing_spaces: 1
        }
    );
    assert_eq!(text_facts("").lines, 0);
    assert_eq!(text_facts("   ").trailing_spaces, 0);
    assert_eq!(text_facts("   ").leading_spaces, 3);
}

#[test]
fn prefix_cuts_at_a_character_boundary() {
    assert_eq!(prefix("東京", 4), "東");
    assert_eq!(prefix("abc", 10), "abc");
}

fn list(values: &[f64]) -> AnyValue<'static> {
    AnyValue::List(Series::new("".into(), values))
}

#[test]
fn nested_values_lay_out_one_item_per_line_with_exact_scalars() {
    let pretty = nested_pretty(&list(&[1000000.125, -0.0]), usize::MAX);
    assert_eq!(pretty.text, "[\n  1000000.125,\n  -0.0\n]");
    assert!(!pretty.cut);
    assert_eq!(value_text(&list(&[1.5, 2.0])), "[1.5, 2.0]");
    assert_eq!(nested_pretty(&list(&[]), 100).text, "[]");

    let df = df!("name" => ["a\"b"], "n" => [Some(1i64)]).unwrap();
    let s = df.into_struct("s".into()).into_series();
    let value = s.get(0).unwrap();
    assert_eq!(nested_len(&value), Some(2));
    assert_eq!(
        nested_pretty(&value, usize::MAX).text,
        "{\n  \"name\": \"a\\\"b\",\n  \"n\": 1\n}"
    );
}

#[test]
fn a_long_nested_value_stops_at_the_budget() {
    let values: Vec<f64> = (0..10_000).map(f64::from).collect();
    let pretty = nested_pretty(&list(&values), 200);
    assert!(pretty.cut);
    assert!(pretty.text.len() < 260, "{}", pretty.text.len());
}

/// Polars panics formatting these; the stored number stands in.
#[test]
fn a_date_past_the_calendar_is_its_stored_number() {
    let us = AnyValue::Datetime(i64::MIN + 1, TimeUnit::Microseconds, None);
    assert_eq!(
        value_text(&us),
        "-9223372036854775807 us since 1970-01-01 UTC"
    );
    let paris = TimeZone::opt_try_new(Some("Europe/Paris"))
        .unwrap()
        .unwrap();
    let ms = AnyValue::Datetime(i64::MAX, TimeUnit::Milliseconds, Some(&paris));
    assert!(value_text(&ms).starts_with("9223372036854775807 ms"));
    assert_eq!(
        value_text(&AnyValue::Date(i32::MAX)),
        "2147483647 days since 1970-01-01"
    );
    // Every nanosecond count is a date; the edges keep their digits.
    assert_eq!(
        value_text(&AnyValue::Datetime(i64::MAX, TimeUnit::Nanoseconds, None)),
        "2262-04-11 23:47:16.854775807"
    );
    assert_eq!(value_text(&AnyValue::Date(-800_000)), "-0221-09-04");
}

/// The table's text for a value is Polars' own, except where Polars panics:
/// a date, datetime or time past the calendar is its stored number, alone or
/// inside a list or struct. Durations never panic and keep Polars' text.
#[test]
fn str_value_never_panics_on_a_date_past_the_calendar() {
    let paris = TimeZone::opt_try_new(Some("Europe/Paris"))
        .unwrap()
        .unwrap();
    for (unit, name) in [
        (TimeUnit::Milliseconds, "ms"),
        (TimeUnit::Microseconds, "us"),
    ] {
        for zone in [None, Some(&paris)] {
            for v in [i64::MIN + 1, i64::MAX] {
                assert_eq!(
                    str_value(&AnyValue::Datetime(v, unit, zone)),
                    format!("{v} {name} since 1970-01-01 UTC"),
                    "{unit:?} {zone:?}"
                );
            }
            let epoch = AnyValue::Datetime(0, unit, zone);
            assert_eq!(str_value(&epoch), epoch.str_value());
        }
    }
    // Every nanosecond count is a date, with or without a zone.
    for zone in [None, Some(&paris)] {
        for v in [i64::MIN + 1, i64::MAX] {
            let value = AnyValue::Datetime(v, TimeUnit::Nanoseconds, zone);
            assert_eq!(str_value(&value), value.str_value());
        }
    }
    assert_eq!(
        str_value(&AnyValue::Date(i32::MAX)),
        "2147483647 days since 1970-01-01"
    );
    assert_eq!(
        str_value(&AnyValue::Date(i32::MIN)),
        "-2147483648 days since 1970-01-01"
    );
    assert_eq!(str_value(&AnyValue::Date(0)), "1970-01-01");
    assert_eq!(str_value(&AnyValue::Time(-1)), "-1 ns since midnight");
    assert_eq!(
        str_value(&AnyValue::Time(NANOS_PER_DAY)),
        "86400000000000 ns since midnight"
    );
    assert_eq!(
        str_value(&AnyValue::Time(NANOS_PER_DAY - 1)),
        "23:59:59.999999999"
    );
    for unit in [
        TimeUnit::Milliseconds,
        TimeUnit::Microseconds,
        TimeUnit::Nanoseconds,
    ] {
        for v in [i64::MIN, i64::MIN + 1, i64::MAX] {
            let value = AnyValue::Duration(v, unit);
            assert_eq!(str_value(&value), value.str_value());
        }
    }

    let stamps = |values: &[i64]| {
        Series::new("".into(), values)
            .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
            .unwrap()
    };
    let past = stamps(&[0, i64::MIN + 1]);
    assert_eq!(
        str_value(&AnyValue::List(past.clone())),
        r#"["1970-01-01 00:00:00.000000", "-9223372036854775807 us since 1970-01-01 UTC"]"#
    );
    let fine = AnyValue::List(stamps(&[0]));
    assert_eq!(str_value(&fine), fine.str_value());
    let nested = AnyValue::List(Series::new("".into(), [past.clone()]));
    assert!(str_value(&nested).contains("-9223372036854775807 us"));
    let row = StructChunked::from_series(
        "".into(),
        2,
        [
            Series::new("id".into(), [1i64, 2]),
            past.with_name("at".into()),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    assert_eq!(
        str_value(&row.get(1).unwrap()),
        r#"{"id": 2, "at": "-9223372036854775807 us since 1970-01-01 UTC"}"#
    );
    assert_eq!(
        str_value(&row.get(0).unwrap()),
        row.get(0).unwrap().str_value()
    );
}

/// Only the values past the calendar are taken out; a column with none is
/// left alone.
#[test]
fn values_past_the_calendar_become_null() {
    let dates = Series::new("d".into(), [Some(i32::MIN), Some(0), None, Some(i32::MAX)])
        .cast(&DataType::Date)
        .unwrap();
    let kept = calendar_without_out_of_range(&dates).unwrap().unwrap();
    assert_eq!(kept.dtype(), &DataType::Date);
    assert_eq!(kept.name().as_str(), "d");
    assert_eq!(kept.null_count(), 3);
    assert_eq!(kept.get(1).unwrap(), AnyValue::Date(0));
    let fine = dates.slice(1, 2);
    assert!(calendar_without_out_of_range(&fine).unwrap().is_none());
    let numbers = Series::new("n".into(), [i64::MIN, i64::MAX]);
    assert!(calendar_without_out_of_range(&numbers).unwrap().is_none());
}

#[test]
fn the_escaped_view_spells_out_every_invisible_character() {
    for c in [
        '\u{61c}',
        '\u{2000}',
        '\u{3000}',
        '\u{180e}',
        '\u{fe0f}',
        '\u{e0041}',
        '\u{9b}',
        '\u{2066}',
        '\u{2028}',
    ] {
        assert_eq!(
            escaped(&c.to_string()),
            format!("\"\\u{{{:x}}}\"", c as u32)
        );
    }
}

/// A direction control would turn the rest of a row around in a terminal
/// that lays out bidirectional text.
#[test]
fn the_preview_marks_direction_controls() {
    let g = crate::glyphs::unicode();
    assert_eq!(preview("a\u{202e}b\u{202c}c\u{61c}", g), "a¤b¤c¤");
    assert!(matches!(preview("שלום مرحبا — x", g), Cow::Borrowed(_)));
}

#[test]
fn a_cell_previews_only_the_start_of_a_huge_value() {
    let g = crate::glyphs::unicode();
    let huge = "x".repeat(CELL_PREVIEW_BYTES * 10);
    let cell = cell_preview(&huge, g);
    assert_eq!(cell.len(), CELL_PREVIEW_CELLS + 1 + g.ellipsis.len());
    assert!(cell.ends_with(g.ellipsis));
    assert_eq!(cell_preview("a\nb", g), "a¶b");
}

/// Cut past `cells`, a value draws and measures at any width up to `cells` as the
/// whole value does: wide characters, joined emoji, marks and combining accents
/// included. What a cut keeps follows the cells, not the bytes.
#[test]
fn a_cut_cell_draws_as_the_whole_value_at_any_width_it_can_have() {
    let cells = 40;
    let texts = [
        "plain words ".repeat(200),
        "東京の市場、報告。".repeat(100),
        "line one\nline two\ttab ".repeat(50),
        "👨‍👩‍👧‍👦 family ".repeat(60),
        "e\u{301}a\u{301}".repeat(300),
        "\u{200b}".repeat(30) + "end",
    ];
    for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
        for text in &texts {
            let whole = preview(text, g);
            let cut = cell_text(Cow::Borrowed(text), g, cells);
            assert!(cut.len() <= 4 * 4 * cells + 64, "{} bytes kept", cut.len());
            for width in 1..=cells {
                assert_eq!(
                    crate::glyphs::fit_cells(&cut, width, g.ellipsis),
                    crate::glyphs::fit_cells(&whole, width, g.ellipsis),
                    "{text:.20?} at {width}"
                );
            }
            if crate::glyphs::cell_width(&whole) > cells {
                assert!(crate::glyphs::cell_width(&cut) > cells, "{text:.20?}");
            }
        }
    }
    // Short or owned text that needs no mark is kept as it is.
    let owned = String::from("1,234");
    let at = owned.as_ptr();
    let kept = cell_text(Cow::Owned(owned), crate::glyphs::unicode(), cells);
    assert_eq!(kept.as_ptr(), at);
}

/// One huge string or a million items in a list stop at the budget too.
#[test]
fn a_nested_value_is_cut_inside_a_huge_item() {
    let huge = "y".repeat(1 << 20);
    let s = Series::new("".into(), ["a", huge.as_str()]);
    let pretty = nested_pretty(&AnyValue::List(s), 200);
    assert!(pretty.cut);
    assert!(pretty.text.len() <= 210, "{}", pretty.text.len());
    assert!(pretty.text.starts_with("[\n  \"a\",\n  \"yyy"));
    let bytes = Series::new("".into(), [vec![7u8; 1 << 20].as_slice()]);
    let compact = nested_compact(&AnyValue::List(bytes), 100);
    assert!(compact.cut && compact.text.len() <= 100, "{}", compact.text);
    let many = Series::new("".into(), (0..1_000_000i64).collect::<Vec<_>>());
    let compact = nested_compact(&AnyValue::List(many), 50);
    assert!(compact.cut && compact.text.len() < 60, "{}", compact.text);
}

/// The floor a capped copy is checked against is never more than what the
/// copy writes, and it stops counting past its stop.
#[test]
fn copy_len_floor_is_a_floor_and_stops_early() {
    let point = StructChunked::from_columns(
        "p".into(),
        1,
        &[
            Column::new("x".into(), [Some(1i64)]),
            Column::new("label".into(), [None::<&str>]),
        ],
    )
    .unwrap()
    .into_column();
    let columns = [
        Column::new("s".into(), ["tab\there \"quoted\" été"]),
        Column::new("b".into(), [b"Hi\x00".as_slice()]),
        Column::new("f".into(), [1000000.125f64]),
        Column::new("n".into(), [None::<i64>]),
        Column::new("l".into(), [list(&[1.5, 2.0])]),
        Column::new("t".into(), [Series::new("".into(), ["a\"b", "", "日本"])]),
        Column::new("e".into(), [Series::new_empty("".into(), &DataType::Int64)]),
        point,
    ];
    for column in columns {
        let value = column.get(0).unwrap();
        let text = copy_text(&column).unwrap();
        let floor = copy_len_floor(&value, usize::MAX);
        assert!(
            floor <= text.len(),
            "{}: {floor} over {text:?}",
            column.name()
        );
    }
    // Text and bytes are exact.
    assert_eq!(copy_len_floor(&AnyValue::String("abcdef"), 0), 6);
    assert_eq!(copy_len_floor(&AnyValue::Binary(b"abcd"), 0), 8);
    // A million items are past a 1 KB stop from the brackets and commas alone.
    let many = Series::new("".into(), (0..1_000_000i64).collect::<Vec<_>>());
    assert!(copy_len_floor(&AnyValue::List(many), 1024) > 1024);
}

#[test]
fn copy_text_is_exact_and_nested_is_json() {
    let floats = Column::new("f".into(), [1000000.125f64]);
    assert_eq!(copy_text(&floats).unwrap(), "1000000.125");
    let nulls = Column::new("s".into(), [None::<&str>]);
    assert_eq!(copy_text(&nulls).unwrap(), "");
    let lists = Column::new("l".into(), [list(&[1.5, 2.0])]);
    assert_eq!(copy_text(&lists).unwrap(), "[1.5,2.0]");
}

/// The table's list preview: ten items, then how many there were.
#[test]
fn a_list_previews_ten_items_and_counts_the_rest() {
    let few = Series::new("".into(), &["a", "b"]);
    assert_eq!(list_preview(&few), "[a, b]");
    let many = Series::new("".into(), (0..12).collect::<Vec<i32>>());
    assert_eq!(
        list_preview(&many),
        "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9...] (12 items)"
    );
    let ten = Series::new("".into(), (0..10).collect::<Vec<i32>>());
    assert_eq!(list_preview(&ten), "[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]");
    // Items of a megabyte each: no more than a cell could show is copied.
    let huge = "x".repeat(1 << 20);
    let articles = Series::new("".into(), vec![huge.as_str(); 10]);
    let text = list_preview(&articles);
    assert!(
        text.len() < 2 * CELL_PREVIEW_BYTES + 8,
        "{} bytes",
        text.len()
    );
    assert!(text.starts_with("[xxx") && text.ends_with("..."));
}
