use super::*;

#[test]
fn durations_keep_seconds_while_they_matter() {
    let secs = std::time::Duration::from_secs;
    assert_eq!(clock(secs(12)), "12s");
    assert_eq!(clock(secs(185)), "3m 05s");
    assert_eq!(clock(secs(90_000)), "25h 00m");
    assert_eq!(duration(12), "12s");
    assert_eq!(duration(5_400), "1.5h");
    assert_eq!(duration(-(10 * 86_400 + 3 * 3_600)), "-10.1d");
    assert_eq!(duration_or_dash(None), "-");
}

#[test]
fn percents_never_round_a_small_share_to_none() {
    assert_eq!(percent(0.4), "40.0%");
    assert_eq!(percent(0.005), "0.50%");
    assert_eq!(percent(0.00001), "<0.01%");
    assert_eq!(percent(0.0), "0.0%");
    assert_eq!(percent_of(1, 0), "-");
}

#[test]
fn bytes_are_binary_units_whole_from_100_up() {
    assert_eq!(bytes(0), "0 B");
    assert_eq!(bytes(512), "512 B");
    assert_eq!(bytes(1536), "1.5 KiB");
    assert_eq!(bytes(3 << 20), "3.0 MiB");
    assert_eq!(bytes(340 << 20), "340 MiB");
    assert_eq!(bytes(5 << 40), "5.0 TiB");
}

fn fmt_i64(nf: &NumberFormat, v: i64) -> String {
    let mut s = String::new();
    let w = nf.write_i64(v, &mut s);
    assert_eq!(w, s.chars().count(), "reported width disagrees with output");
    assert_eq!(w, nf.width_i64(v), "width_i64 disagrees with write_i64");
    s
}

fn thousands() -> NumberFormat {
    NumberFormat::preset("thousands").unwrap()
}

/// A date past the calendar is its stored number in a cell, formatted or
/// not, and measured as that: Polars panics formatting it.
#[test]
fn a_date_past_the_calendar_is_its_stored_number_in_a_cell() {
    let mut scratch = String::new();
    let thousands = NumberFormatSettings {
        format: NumberFormat::preset("thousands").unwrap(),
        ..NumberFormatSettings::default()
    };
    let value = AnyValue::Datetime(i64::MIN + 1, polars::prelude::TimeUnit::Microseconds, None);
    let dtype = DataType::Datetime(polars::prelude::TimeUnit::Microseconds, None);
    for fmt in [
        CellFormatter::Passthrough,
        thousands.formatter_for("t", &dtype),
        CellFormatter::Number(NumberFormat::preset("thousands").unwrap()),
    ] {
        let text = format_any_value(&fmt, &value, &mut scratch).into_owned();
        assert_eq!(text, "-9223372036854775807 us since 1970-01-01 UTC");
        assert_eq!(display_width(&fmt, &value, &mut scratch), text.len());
    }
    let date = AnyValue::Date(i32::MAX);
    assert_eq!(
        format_any_value(&CellFormatter::Passthrough, &date, &mut scratch),
        "2147483647 days since 1970-01-01"
    );
}
#[test]
fn digit_count_basics() {
    assert_eq!(digit_count(0), 1);
    assert_eq!(digit_count(9), 1);
    assert_eq!(digit_count(10), 2);
    assert_eq!(digit_count(999), 3);
    assert_eq!(digit_count(1000), 4);
    assert_eq!(digit_count(u64::MAX), 20);
}

#[test]
fn every_value_in_a_formatted_column_is_grouped() {
    // No magnitude threshold: a column must not mix "1000" and
    // "248,956,422". Columns holding identifiers are named in `exclude`
    // instead of being guessed at by size.
    let nf = thousands();
    assert_eq!(fmt_i64(&nf, 0), "0");
    assert_eq!(fmt_i64(&nf, 999), "999");
    assert_eq!(fmt_i64(&nf, 1000), "1,000");
    assert_eq!(fmt_i64(&nf, 2024), "2,024");
    assert_eq!(fmt_i64(&nf, 9999), "9,999");
    assert_eq!(fmt_i64(&nf, 10000), "10,000");
    assert_eq!(fmt_i64(&nf, 1234567), "1,234,567");
}

#[test]
fn negatives_and_extremes() {
    let nf = thousands();
    assert_eq!(fmt_i64(&nf, -1234567), "-1,234,567");
    assert_eq!(fmt_i64(&nf, -999), "-999");
    assert_eq!(fmt_i64(&nf, i64::MIN), "-9,223,372,036,854,775,808");
    assert_eq!(fmt_i64(&nf, i64::MAX), "9,223,372,036,854,775,807");

    let mut s = String::new();
    let w = nf.write_u64(u64::MAX, &mut s);
    assert_eq!(s, "18,446,744,073,709,551,615");
    assert_eq!(w, s.chars().count());
    assert_eq!(w, nf.width_u64(u64::MAX));
}

#[test]
fn bed_style_coordinates() {
    // The case from issue #51: genomic coordinates in the millions/billions.
    let nf = thousands();
    assert_eq!(fmt_i64(&nf, 248_956_422), "248,956,422");
    assert_eq!(fmt_i64(&nf, 3_088_269_832), "3,088,269,832");
}

#[test]
fn indian_grouping() {
    let nf = NumberFormat::preset("indian").unwrap();
    assert_eq!(fmt_i64(&nf, 100), "100");
    assert_eq!(fmt_i64(&nf, 1000), "1,000");
    assert_eq!(fmt_i64(&nf, 12345), "12,345");
    assert_eq!(fmt_i64(&nf, 123456), "1,23,456");
    assert_eq!(fmt_i64(&nf, 1234567), "12,34,567");
    assert_eq!(fmt_i64(&nf, 12345678), "1,23,45,678");
    assert_eq!(fmt_i64(&nf, -12345678), "-1,23,45,678");
}

#[test]
fn all_presets_render() {
    let cases = [
        ("none", "1234567"),
        ("thousands", "1,234,567"),
        ("european", "1.234.567"),
        ("si", "1\u{202f}234\u{202f}567"),
        ("swiss", "1'234'567"),
        ("indian", "12,34,567"),
        ("underscore", "1_234_567"),
    ];
    for (name, expected) in cases {
        let nf = NumberFormat::preset(name).unwrap();
        assert_eq!(fmt_i64(&nf, 1234567), expected, "preset {name}");
    }
    assert!(NumberFormat::preset("klingon").is_none());
    for name in NumberFormat::PRESET_NAMES {
        assert!(NumberFormat::preset(name).is_some(), "preset {name}");
    }
}

#[test]
fn width_matches_rendered_length_across_range() {
    for nf in NumberFormat::PRESET_NAMES
        .iter()
        .map(|n| NumberFormat::preset(n).unwrap())
    {
        let mut v: i64 = 1;
        for _ in 0..19 {
            for probe in [v, v - 1, -v, v * 3 / 2] {
                let mut s = String::new();
                let w = nf.write_i64(probe, &mut s);
                assert_eq!(w, s.chars().count(), "{:?} on {probe}", nf.grouping);
                assert_eq!(w, nf.width_i64(probe), "{:?} on {probe}", nf.grouping);
            }
            v = v.saturating_mul(10);
        }
    }
}

#[test]
fn floats_regroup_integer_part_only() {
    let nf = thousands();
    let mut s = String::new();
    let w = nf.regroup_decimal("1234567.891", &mut s);
    assert_eq!(s, "1,234,567.891");
    assert_eq!(w, s.chars().count());

    s.clear();
    nf.regroup_decimal("-1234.5", &mut s);
    assert_eq!(s, "-1,234.5");
}

#[test]
fn european_swaps_decimal_separator() {
    let nf = NumberFormat::preset("european").unwrap();
    let mut s = String::new();
    let w = nf.regroup_decimal("1234567.89", &mut s);
    assert_eq!(s, "1.234.567,89");
    assert_eq!(w, s.chars().count());
}

#[test]
fn non_decimal_strings_pass_through_untouched() {
    let nf = thousands();
    for src in ["NaN", "inf", "-inf", "1e300", "1.5e-8", ""] {
        let mut s = String::new();
        let w = nf.regroup_decimal(src, &mut s);
        assert_eq!(s, src, "{src} should pass through");
        assert_eq!(w, src.chars().count());
    }
}

#[test]
fn float_precision_is_applied() {
    let nf = NumberFormat {
        float_precision: Some(2),
        ..thousands()
    };
    let (mut scratch, mut out) = (String::new(), String::new());
    let w = nf.write_f64(1234.5678, &mut scratch, &mut out);
    assert_eq!(out, "1,234.57");
    assert_eq!(w, out.chars().count());

    out.clear();
    nf.write_f64(-0.5, &mut scratch, &mut out);
    assert_eq!(out, "-0.50");
}

#[test]
fn is_noop_detects_the_free_path() {
    assert!(NumberFormat::PLAIN.is_noop());
    assert!(!thousands().is_noop());
    assert!(
        !NumberFormat {
            float_precision: Some(2),
            ..NumberFormat::PLAIN
        }
        .is_noop()
    );
    assert!(
        !NumberFormat {
            decimal_sep: ',',
            ..NumberFormat::PLAIN
        }
        .is_noop()
    );
}

#[test]
fn chrome_grouping_is_unconditional() {
    // The app's own labels group regardless of the user's data settings,
    // and with no digit threshold: "Rows: 1,234" not "Rows: 1234".
    assert_eq!(group_chrome(0), "0");
    assert_eq!(group_chrome(999), "999");
    assert_eq!(group_chrome(1234), "1,234");
    assert_eq!(group_chrome(1_234_567), "1,234,567");
    assert_eq!(group_chrome(usize::MAX), "18,446,744,073,709,551,615");
}

#[test]
fn glob_matching() {
    assert!(Glob::new("year").matches("year"));
    assert!(!Glob::new("year").matches("years"));
    assert!(Glob::new("*_id").matches("sample_id"));
    assert!(Glob::new("*_id").matches("_id"));
    assert!(!Glob::new("*_id").matches("id_sample"));
    assert!(Glob::new("chrom*").matches("chromStart"));
    assert!(Glob::new("*").matches("anything"));
    assert!(Glob::new("c?rom").matches("chrom"));
    assert!(!Glob::new("c?rom").matches("chhrom"));
    assert!(Glob::new("a*b*c").matches("axxbyyc"));
    assert!(!Glob::new("a*b*c").matches("axxbyy"));

    // Wildcard characters appearing literally in the *name*. Found by the
    // `glob_match` fuzz target: the matcher compared for equality before testing
    // for `*`, so a `*` in the name consumed the pattern's wildcard and stopped it
    // wildcarding anything further.
    assert!(Glob::new("*").matches("*]"));
    assert!(Glob::new("*").matches("a*b"));
    assert!(Glob::new("*").matches("?"));
    assert!(Glob::new("a*c").matches("a*c"));
    assert!(Glob::new("a*c").matches("a*x*c"));
    assert!(Glob::new("?").matches("*"));
}

#[test]
fn settings_resolve_per_column() {
    let settings = NumberFormatSettings {
        format: thousands(),
        enabled: true,
        exclude: vec![Glob::new("*_id"), Glob::new("year")],
        align_numeric_right: true,
    };
    // Numeric column: formatted.
    assert!(
        !settings
            .formatter_for("chromStart", &DataType::Int64)
            .is_passthrough()
    );
    // Non-numeric: never formatted.
    assert!(
        settings
            .formatter_for("chrom", &DataType::String)
            .is_passthrough()
    );
    assert!(
        settings
            .formatter_for("when", &DataType::Date)
            .is_passthrough()
    );
    // Excluded by glob.
    assert!(
        settings
            .formatter_for("sample_id", &DataType::Int64)
            .is_passthrough()
    );
    assert!(
        settings
            .formatter_for("year", &DataType::Int32)
            .is_passthrough()
    );
}

#[test]
fn settings_disabled_is_all_passthrough() {
    let settings = NumberFormatSettings {
        format: thousands(),
        enabled: false,
        ..Default::default()
    };
    assert!(
        settings
            .formatter_for("chromStart", &DataType::Int64)
            .is_passthrough()
    );
}

#[test]
fn plain_format_is_always_passthrough() {
    let settings = NumberFormatSettings::default();
    assert!(
        settings
            .formatter_for("chromStart", &DataType::Int64)
            .is_passthrough()
    );
}

#[test]
fn floats_flag_disables_grouping_for_floats_only() {
    let settings = NumberFormatSettings {
        format: NumberFormat {
            floats: false,
            ..thousands()
        },
        ..Default::default()
    };
    assert!(
        !settings
            .formatter_for("count", &DataType::Int64)
            .is_passthrough()
    );
    assert!(
        settings
            .formatter_for("ratio", &DataType::Float64)
            .is_passthrough()
    );
}

#[test]
fn any_value_formatting_and_width_agree() {
    let fmt = CellFormatter::Number(thousands());
    let mut scratch = String::new();
    let cases: Vec<AnyValue> = vec![
        AnyValue::Int32(1234567),
        AnyValue::Int64(-9876543),
        AnyValue::UInt32(4000000),
        AnyValue::UInt64(u64::MAX),
        AnyValue::Int8(-12),
    ];
    for v in cases {
        let s = format_any_value(&fmt, &v, &mut scratch).into_owned();
        assert_eq!(
            display_width(&fmt, &v, &mut scratch),
            s.chars().count(),
            "width mismatch for {v:?} -> {s}"
        );
    }
    assert_eq!(
        format_any_value(&fmt, &AnyValue::Int32(1234567), &mut scratch),
        "1,234,567"
    );
}

#[test]
fn nulls_and_strings_are_untouched() {
    let fmt = CellFormatter::Number(thousands());
    let mut scratch = String::new();
    assert_eq!(format_any_value(&fmt, &AnyValue::Null, &mut scratch), "");
    // A string value in a "numeric" formatter still passes through.
    assert_eq!(
        format_any_value(&fmt, &AnyValue::String("chr1"), &mut scratch),
        "chr1"
    );
    assert_eq!(
        format_any_value(
            &CellFormatter::Passthrough,
            &AnyValue::Int64(1234567),
            &mut scratch
        ),
        "1234567"
    );
}

/// The CLI crate duplicates the preset list to give clap `--help` output and
/// completion, since it cannot depend on this crate. Catch drift here.
#[test]
fn number_format_values_match_presets() {
    let mut cli: Vec<&str> = datui_cli::NUMBER_FORMAT_VALUES.to_vec();
    let mut expected: Vec<&str> = NumberFormat::PRESET_NAMES.to_vec();
    // "system" is CLI/config-only: it resolves to a preset, it is not one.
    expected.push("system");
    cli.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        cli, expected,
        "datui_cli::NUMBER_FORMAT_VALUES is out of sync with NumberFormat::PRESET_NAMES"
    );
}

#[test]
fn locale_tags_map_to_presets() {
    assert_eq!(preset_for_locale_tag("en_US.UTF-8"), "thousands");
    assert_eq!(preset_for_locale_tag("de_DE.UTF-8"), "european");
    assert_eq!(preset_for_locale_tag("de_DE.UTF-8@euro"), "european");
    assert_eq!(preset_for_locale_tag("fr_FR"), "si");
    assert_eq!(preset_for_locale_tag("hi_IN"), "indian");
    assert_eq!(preset_for_locale_tag("de_CH"), "swiss");
    assert_eq!(preset_for_locale_tag("it-CH"), "swiss");
    assert_eq!(preset_for_locale_tag("ja_JP"), "thousands");
    // Unknown tags fall back rather than failing.
    assert_eq!(preset_for_locale_tag("xx_YY"), "thousands");
}
