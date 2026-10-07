use super::*;
use unicode_width::UnicodeWidthStr;

/// Every character the key registry uses outside ASCII has a twin in
/// `ascii_twin`, so the ASCII floor never sees a `?` where an
/// instruction was.
#[test]
fn every_help_screen_is_ascii_clean() {
    use datui_cli::keys;
    let mut texts: Vec<&str> = Vec::new();
    for (screen, group, key) in keys::entries() {
        texts.extend([group.name, key.keys, key.label, key.line, key.long()]);
        texts.extend(key.also);
        if let Some(screen) = screen {
            texts.push(screen.title);
        }
    }
    for (example, meaning) in keys::Q_SUMMARY {
        texts.extend([*example, *meaning]);
    }
    for text in &texts {
        for c in text.chars().filter(|c| !c.is_ascii()) {
            assert!(
                ascii_twin(c).is_some(),
                "{text:?} uses {c:?}, which has no ASCII twin"
            );
        }
    }
    let checked = texts.len();
    assert!(checked > 10, "the help files were found");
    // And the border set's twin is pure ASCII by construction.
    let b = ASCII.border;
    for piece in [
        b.top_left,
        b.top_right,
        b.bottom_left,
        b.bottom_right,
        b.vertical_left,
        b.vertical_right,
        b.horizontal_top,
        b.horizontal_bottom,
    ] {
        assert!(piece.is_ascii(), "{piece:?}");
    }
}

/// A key whose arrows become words re-pads, so its description stays in
/// line with the rows around it.
#[test]
fn an_ascii_key_keeps_its_description_column() {
    let text = "Keys:\n  ↑ / ↓:      Move\n  Enter:      Open, → on a folder\n";
    assert_eq!(
        instructions_in_ascii(text),
        "Keys:\n  Up / Dn:    Move\n  Enter:      Open, Rt on a folder\n"
    );
}

/// Paired arrows read as the glyph set's pair, not as two words run
/// together, and the row still keeps its column.
#[test]
fn paired_arrows_read_as_one_key() {
    let text = "  ↑↓ / j/k:      Rows\n  ←→ / h/l:      Columns\n  Home/End:      Ends";
    assert_eq!(
        instructions_in_ascii(text),
        "  Up/Dn / j/k:   Rows\n  Lt/Rt / h/l:   Columns\n  Home/End:      Ends"
    );
}

/// A key that no longer fits its column moves its whole section right,
/// continuation lines included; prose and other sections stay put.
#[test]
fn an_overlong_ascii_key_moves_its_section_together() {
    let text = [
        "Keys:",
        "  ← / → (h/l):  Page",
        "  e:            Plan, and",
        "                more",
        "  Prose → here.",
        "",
        "  q:  Quit",
    ]
    .join("\n");
    let expected = [
        "Keys:",
        "  Lt / Rt (h/l):  Page",
        "  e:              Plan, and",
        "                  more",
        "  Prose Rt here.",
        "",
        "  q:  Quit",
    ]
    .join("\n");
    assert_eq!(instructions_in_ascii(&text), expected);
}

/// Every locality marker has to be the same display width in a given set, or the
/// name beside it starts one column further along on some rows than on others and
/// the whole list looks broken.
#[test]
fn locality_markers_are_all_one_column() {
    for set in [unicode(), ascii()] {
        for marker in [
            set.here,
            set.in_memory,
            set.over_network,
            set.in_object_store,
            set.place_unknown,
        ] {
            assert_eq!(
                UnicodeWidthStr::width(marker),
                1,
                "{marker:?} is not one column wide"
            );
        }
    }
}

/// The sort marks sit inside the header's column-width arithmetic, so each must be
/// exactly one column in both sets or a sorted column drifts out of line.
#[test]
fn sort_marks_are_one_column() {
    for set in [unicode(), ascii()] {
        for mark in [set.sort_asc, set.sort_desc] {
            assert_eq!(
                UnicodeWidthStr::width(mark),
                1,
                "{mark:?} is not one column wide"
            );
        }
    }
}

/// The two sets must agree column for column, since the layout arithmetic around
/// them is written once and used for both.
#[test]
fn the_two_sets_have_the_same_shape() {
    let (u, a) = (unicode(), ascii());
    for (left, right) in [
        (u.here, a.here),
        (u.in_memory, a.in_memory),
        (u.over_network, a.over_network),
        (u.in_object_store, a.in_object_store),
        (u.place_unknown, a.place_unknown),
        (u.selector, a.selector),
        (u.selector_blank, a.selector_blank),
        (u.collapsed, a.collapsed),
        (u.expanded, a.expanded),
        (u.sort_asc, a.sort_asc),
        (u.sort_desc, a.sort_desc),
    ]
    .into_iter()
    .chain(
        u.bar_eighths
            .iter()
            .copied()
            .zip(a.bar_eighths.iter().copied()),
    ) {
        assert_eq!(
            UnicodeWidthStr::width(left),
            UnicodeWidthStr::width(right),
            "{left:?} and {right:?} are different widths"
        );
    }
}

/// ratatui draws the plot marks, so the audit script cannot see the ASCII
/// set's marker characters; this checks them, and that each column eighth is
/// one cell in both sets.
#[test]
fn the_ascii_plot_marks_are_ascii() {
    let p = ascii().plot;
    for marker in [p.line, p.point, p.bar] {
        let Marker::Custom(c) = marker else {
            panic!("{marker:?} is drawn by ratatui from its own Unicode set");
        };
        assert!(c.is_ascii_graphic(), "{c:?}");
    }
    let a = p.axis;
    for piece in [
        a.vertical,
        a.horizontal,
        a.top_right,
        a.top_left,
        a.bottom_right,
        a.bottom_left,
        a.vertical_left,
        a.vertical_right,
        a.horizontal_down,
        a.horizontal_up,
        a.cross,
    ] {
        assert!(piece.is_ascii(), "{piece:?}");
    }
    for set in [unicode(), ascii()] {
        for eighth in set.plot.column_eighths {
            assert_eq!(UnicodeWidthStr::width(*eighth), 1, "{eighth:?}");
        }
    }
}

fn chart_buffer(width: u16, height: u16, x_title: &str, name: &str) -> Buffer {
    use ratatui::widgets::{Axis, Chart, Dataset, LegendPosition, Widget};
    let labels = || vec!["0", "5", "10"];
    let chart = Chart::new(vec![
        Dataset::default()
            .name(name)
            .marker(Marker::Custom('o'))
            .data(&[(5.0, 5.0)]),
    ])
    .x_axis(
        Axis::default()
            .title(x_title)
            .bounds([0.0, 10.0])
            .labels(labels()),
    )
    .y_axis(Axis::default().bounds([0.0, 10.0]).labels(labels()))
    .legend_position(Some(LegendPosition::TopRight))
    .hidden_legend_constraints((
        ratatui::layout::Constraint::Percentage(100),
        ratatui::layout::Constraint::Percentage(100),
    ));
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    chart.render(area, &mut buf);
    buf
}

/// The swap finds the axes and the legend frame by shape: a title or a legend
/// name holding the same characters keeps them, and Unicode changes nothing.
#[test]
fn redraw_axes_changes_only_the_frame() {
    let before = chart_buffer(40, 12, "a│b└─c", "x─│y");
    let mut buf = before.clone();
    unicode().plot.redraw_axes(buf.area, &mut buf);
    assert_eq!(buf, before);

    ascii().plot.redraw_axes(buf.area, &mut buf);
    let text = crate::tests::buffer_text(&buf);
    let rows: Vec<&str> = text.lines().collect();
    assert!(rows[0].ends_with("+----+"), "the legend frame:\n{text}");
    assert!(rows[1].ends_with("|x─│y|"), "the legend name:\n{text}");
    assert!(rows[2].ends_with("+----+"), "the legend frame:\n{text}");
    assert!(text.contains("a│b└─c"), "the axis title:\n{text}");
    assert!(rows[10].contains("+-------"), "the axis corner:\n{text}");
    let kept: String = text.chars().filter(|c| !c.is_ascii()).collect();
    assert_eq!(kept, "─││└─", "only the name and the title:\n{text}");
}

/// Under three rows the chart has no x axis, so no corner to find the y axis by;
/// every line cell changes then, and the plot is still ASCII.
#[test]
fn redraw_axes_in_a_chart_too_small_for_both_axes() {
    let mut buf = chart_buffer(20, 2, "", "");
    assert!(!crate::tests::buffer_text(&buf).is_ascii());
    ascii().plot.redraw_axes(buf.area, &mut buf);
    let text = crate::tests::buffer_text(&buf);
    assert!(text.is_ascii() && text.contains('|'), "{text}");
}

/// `get()` stores a copy of a const, so no address can identify the active
/// set — the `unicode` flag on the set is what `active_is_unicode` reads.
#[test]
fn active_is_unicode_matches_the_chosen_set() {
    let expected = get().spinner.len() == unicode().spinner.len();
    assert_eq!(active_is_unicode(), expected);
    assert!(unicode().unicode);
    assert!(!ascii().unicode);
}

/// A locale variable decides on every OS; Windows counts as UTF-8 only when
/// none is set (#541). Windows Terminal opened as the default terminal sets
/// no `WT_SESSION`, so the rule reads neither it nor the code page.
#[test]
fn utf8_rule_reads_the_locale_then_windows() {
    let env = |locale: Option<&str>, windows: bool| Environment {
        locale: locale.map(String::from),
        windows,
    };
    // Unix: the locale alone.
    assert!(env(Some("en_US.UTF-8"), false).is_utf8());
    assert!(env(Some("C.utf8"), false).is_utf8());
    assert!(!env(Some("C"), false).is_utf8());
    assert!(!env(None, false).is_utf8());
    // Windows sets no locale: Unicode.
    assert!(env(None, true).is_utf8());
    // An explicit locale still wins there, as LANG=C does on Unix.
    assert!(!env(Some("C"), true).is_utf8());
    assert!(env(Some("en_US.UTF-8"), true).is_utf8());
}

#[test]
fn current_knows_its_os() {
    assert_eq!(Environment::current().windows, cfg!(windows));
}

/// A bad `[glyphs]` line must fail at config load with the slot named.
#[test]
fn overrides_validate_names_arity_and_width() {
    let one =
        |k: &str, v: &str| BTreeMap::from([(k.to_string(), SlotOverride::One(v.to_string()))]);
    assert!(validate_overrides(&one("in_object_store", "☁")).is_ok());
    assert!(
        validate_overrides(&one("no_such_slot", "x")).is_err_and(|e| e.contains("no_such_slot"))
    );
    // ‹binary› is eight columns; a one-column override moves every layout after it.
    assert!(validate_overrides(&one("binary_stub", "b")).is_err_and(|e| e.contains("binary_stub")));
    assert!(validate_overrides(&one("checkbox_on", "")).is_err());
    // The wordmark is not a slot.
    assert!(validate_overrides(&one("wordmark", "datui")).is_err());

    let many = |k: &str, v: &[&str]| {
        BTreeMap::from([(
            k.to_string(),
            SlotOverride::Many(v.iter().map(|s| s.to_string()).collect()),
        )])
    };
    assert!(validate_overrides(&many("spinner", &["◐", "◓", "◑", "◒"])).is_ok());
    assert!(validate_overrides(&many("spinner", &[])).is_err());
    assert!(
        validate_overrides(&many("score_marks", &["a", "b"]))
            .is_err_and(|e| e.contains("exactly 5"))
    );
    assert!(validate_overrides(&many("times", &["×"])).is_err_and(|e| e.contains("single string")));
    assert!(validate_overrides(&one("spinner", "◐")).is_err_and(|e| e.contains("list")));
}

/// Overrides land on the set they name and leave every other slot alone.
#[test]
fn overrides_apply_over_the_unicode_set() {
    let mut set = UNICODE;
    let overrides = BTreeMap::from([
        (
            "in_object_store".to_string(),
            SlotOverride::One("☁".to_string()),
        ),
        (
            "spinner".to_string(),
            SlotOverride::Many(vec!["◐".to_string(), "◑".to_string()]),
        ),
    ]);
    validate_overrides(&overrides).expect("a valid override map");
    apply_overrides(&mut set, &overrides);
    assert_eq!(set.in_object_store, "☁");
    assert_eq!(set.spinner, &["◐", "◑"]);
    assert_eq!(set.checkbox_on, UNICODE.checkbox_on);
}

/// A marker that is also a letter or a space would read as part of the name.
#[test]
fn no_marker_could_be_mistaken_for_text() {
    for set in [unicode(), ascii()] {
        for marker in [
            set.here,
            set.in_memory,
            set.over_network,
            set.in_object_store,
            set.place_unknown,
        ] {
            let c = marker.chars().next().expect("a marker");
            assert!(
                !c.is_alphanumeric() && !c.is_whitespace(),
                "{marker:?} would read as part of a filename"
            );
        }
    }
}

/// Cells, not characters and not bytes: wide characters count two, a combining
/// mark and a joined emoji sequence count with the character they join, and a
/// control character counts nothing, since ratatui draws nothing for it.
#[test]
fn cell_width_counts_what_is_drawn() {
    assert_eq!(cell_width("tail"), 4);
    assert_eq!(cell_width("東京大阪"), 8);
    assert_eq!(cell_width("e\u{301}e\u{301}"), 2);
    assert_eq!(cell_width("line1\nline2"), 10);
    assert_eq!(cell_width("tab\tseparated"), 12);
    assert_eq!(cell_width("👩\u{200d}👩\u{200d}👧"), 2);
}

/// Whole when it fits, borrowed; otherwise cut at a grapheme and marked, never
/// wider than asked, in both marker widths.
#[test]
fn fit_cells_clips_at_graphemes_and_marks_the_cut() {
    assert!(matches!(fit_cells("tail", 4, "…"), Cow::Borrowed("tail")));
    assert_eq!(fit_cells("abcdef", 5, "…"), "abcd…");
    assert_eq!(fit_cells("abcdef", 5, "..."), "ab...");
    assert_eq!(fit_cells("abcdef", 2, "..."), "..");
    assert_eq!(fit_cells("abcdef", 0, "…"), "");
    assert!(matches!(
        fit_cells("東京大阪", 8, "…"),
        Cow::Borrowed("東京大阪")
    ));
    assert_eq!(fit_cells("東京大阪", 7, "…"), "東京大…");
    // 京 would straddle the edge: it goes whole, and its cell stays blank.
    assert_eq!(fit_cells("東京大阪", 4, "…"), "東…");
    assert_eq!(fit_cells("東京大阪", 4, "..."), "...");
    assert_eq!(fit_cells("e\u{301}e\u{301}e\u{301}", 2, "…"), "e\u{301}…");
    assert_eq!(fit_cells("line1\nline2", 10, "…"), "line1line2");
    assert_eq!(fit_cells("line1\nline2", 6, "…"), "line1…");
}

/// Whatever the text and the width, the result is a run of the text's own
/// graphemes plus the marker, within the width.
#[test]
fn fit_cells_never_splits_a_grapheme() {
    let samples = [
        "plain ascii text",
        "東京大阪 京都横浜",
        "e\u{301}a\u{308}o\u{302}u\u{30a}",
        "👩\u{200d}👩\u{200d}👧 family",
        "🇯🇵🇺🇸 flags",
        "mixed 名古屋 e\u{301} 👍🏽 end",
    ];
    for marker in ["…", "..."] {
        for text in samples {
            let span = ratatui::text::Span::raw(text);
            let graphemes: Vec<&str> = drawn_graphemes(&span).collect();
            for width in 0..=cell_width(text) + 1 {
                let fitted = fit_cells(text, width, marker);
                assert!(
                    cell_width(&fitted) <= width,
                    "{text:?} at {width}: {fitted:?} is too wide"
                );
                if fitted == text {
                    assert!(cell_width(text) <= width);
                    continue;
                }
                let kept = fitted.strip_suffix(marker).unwrap_or("");
                let mut rebuilt = String::new();
                for g in &graphemes {
                    if rebuilt.len() >= kept.len() {
                        break;
                    }
                    rebuilt.push_str(g);
                }
                assert_eq!(
                    rebuilt, kept,
                    "{text:?} at {width}: {fitted:?} cuts a grapheme"
                );
            }
        }
    }
}
