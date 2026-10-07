use super::*;

/// The optional tabs: Partitions and Notes as asked, no others.
fn offer(partitions: bool, notes: bool) -> TabsOffered {
    TabsOffered {
        metadata: false,
        format: false,
        partitions,
        notes,
        documentation: false,
    }
}

/// What a file says about itself: a size and, for a format with a facts read, its
/// footer and tab; a directory has no size of its own to give; a file that is gone,
/// or whose footer is not one, is a reason rather than a blank.
#[test]
fn file_facts_read_what_each_source_has() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("rows.csv");
    std::fs::write(&csv, "a\n1\n").unwrap();
    let parquet = dir.path().join("rows.parquet");
    let mut df = df!("a" => &[1i64, 2, 3]).unwrap();
    ParquetWriter::new(std::fs::File::create(&parquet).unwrap())
        .finish(&mut df)
        .unwrap();
    let parquet_len = std::fs::metadata(&parquet).unwrap().len();
    let facts = crate::formats::readers::of(crate::FileFormat::Parquet).facts;

    assert!(matches!(
        FileFacts::read(&csv, None),
        Ok(FileFacts::Read {
            size: Some(4),
            footer: None,
            detail: None,
        })
    ));
    match FileFacts::read(&parquet, facts) {
        Ok(FileFacts::Read {
            size: Some(size),
            footer: Some(footer),
            detail: Some(detail),
        }) => {
            assert_eq!(size, parquet_len);
            assert_eq!(footer.num_rows, 3);
            assert_eq!(detail.tab, "Parquet");
        }
        other => panic!("a Parquet file's size, footer and tab: {other:?}"),
    }
    assert!(matches!(
        FileFacts::read(dir.path(), facts),
        Ok(FileFacts::Read {
            size: None,
            footer: None,
            detail: None,
        })
    ));
    // Reasons short enough for the panel's one line.
    assert_eq!(
        FileFacts::read(&dir.path().join("gone.parquet"), facts).unwrap_err(),
        "file not found"
    );
    assert_eq!(
        FileFacts::read(&csv, facts).expect_err("a CSV has no footer"),
        "unreadable footer"
    );
}

/// A dataset that has not been counted says so rather than showing how far it got.
///
/// Through a rendered panel, not the helper: the helper cannot tell whether its
/// caller passed `num_rows_if_valid()` or the raw field, and the raw field is what
/// the bug was.
#[test]
fn the_schema_tab_does_not_call_a_partial_the_total() {
    use crate::table::DataTableState;
    use polars::prelude::*;

    let rows = || df!("id" => (0..70i64).collect::<Vec<_>>()).unwrap().lazy();
    let mut lf = rows();
    let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
    let mut state = DataTableState::from_schema_and_lazyframe(
        schema,
        rows(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    // As a staged open leaves it: a provisional from however far the buffer reached,
    // with no count taken.
    state.set_provisional_rows(70);

    let theme = RenderContext::for_test();
    let painted = |state: &DataTableState| {
        let area = Rect::new(0, 0, 60, 12);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        let panel = DataTableInfo::new(
            state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        panel.render_schema_summary(area, &mut buf);
        crate::tests::buffer_text(&buf)
    };

    let uncounted = painted(&state);
    assert!(
        uncounted.contains("counting..."),
        "a count not taken is not a total: {uncounted}"
    );
    assert!(
        !uncounted.contains("70"),
        "and the buffer's height is not shown in its place: {uncounted}"
    );

    assert!(state.count_landed(state.len_generation(), 70, None));
    let counted = painted(&state);
    assert!(
        counted.contains("Rows (total): 70"),
        "and once it has been counted, that is what it says: {counted}"
    );
}

/// A schema taller than the panel says how many columns are out of view
/// before any scrolling, the selection carries the shared rail, and the
/// panel names its keys in a footer.
#[test]
fn a_tall_schema_counts_its_hidden_columns() {
    use crate::table::DataTableState;
    use polars::prelude::*;

    let wide = || {
        let base = df!("col_0" => &[1i64]).unwrap().lazy();
        let extra: Vec<Expr> = (1..24)
            .map(|i| lit(1i64).alias(format!("col_{i}")))
            .collect();
        base.with_columns(extra)
    };
    let mut lf = wide();
    let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
    let state = DataTableState::from_schema_and_lazyframe(
        schema,
        wide(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();

    let theme = RenderContext::for_test();
    let area = Rect::new(0, 0, 60, 16);
    let mut buf = Buffer::empty(area);
    let mut modal = InfoModal::default();
    let mut panel = DataTableInfo::new(
        &state,
        InfoContext {
            format: None,
            facts: None,
            facts_tab: None,
            footer_expected: false,
            declared_types: false,
        },
        &mut modal,
        &theme,
    );
    (&mut panel).render(area, &mut buf);
    let text = crate::tests::buffer_text(&buf);
    assert!(
        text.contains("below"),
        "the hidden columns are counted: {text}"
    );
    assert!(text.contains("Esc"), "the footer names the way out: {text}");
    assert!(text.contains("Tabs"), "and the tab keys: {text}");
    assert!(!text.contains(">>"), "the bespoke marker is gone: {text}");
}

/// The Schema tab's footer offers `H` for delimited text alone, and no other
/// tab does.
#[test]
fn the_schema_footer_offers_h_only_where_it_works() {
    use crate::table::DataTableState;
    use polars::prelude::*;

    let lf = || df!("a" => &[1i64], "b" => &[2i64]).unwrap().lazy();
    let schema = std::sync::Arc::new((*lf().collect_schema().unwrap()).clone());
    let state = DataTableState::from_schema_and_lazyframe(
        schema,
        lf(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    let theme = RenderContext::for_test();
    let area = Rect::new(0, 0, 80, 16);
    let footer = |header_toggle: bool, tab: InfoTab| {
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal {
            active_tab: tab,
            ..Default::default()
        };
        let mut panel = DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        panel.header_toggle = header_toggle;
        (&mut panel).render(area, &mut buf);
        crate::tests::buffer_text(&buf)
    };
    let shown = footer(true, InfoTab::Schema);
    assert!(shown.contains("Header"), "{shown}");
    assert!(!footer(false, InfoTab::Schema).contains("Header"));
    assert!(!footer(true, InfoTab::Resources).contains("Header"));
}

/// Times read in the unit the docs promise, on both sides of the switch.
///
/// The band just under a second is the whole point. Judging it in whole
/// milliseconds moves the switch to 999.5 ms, so half a millisecond's worth of
/// figures the page promises in milliseconds come back in seconds; not rounding at
/// all prints `1000.00 ms`, which beside the `1.00s` a tick later says the slower
/// open was the faster one. Neither shows up in a test that only uses round
/// numbers, which is why these are not round.
#[test]
fn a_time_reads_in_the_unit_the_page_promises() {
    use std::time::Duration;

    let cases = [
        (Duration::ZERO, "0.00 ms"),
        (Duration::from_nanos(1_000), "0.00 ms"),
        (Duration::from_nanos(5_000), "0.01 ms"),
        (Duration::from_micros(344), "0.34 ms"),
        (Duration::from_micros(999_500), "999.50 ms"),
        (Duration::from_nanos(999_994_999), "999.99 ms"),
        (Duration::from_nanos(999_995_000), "1.00s"),
        (Duration::from_secs(1), "1.00s"),
        (Duration::from_millis(3_880), "3.88s"),
    ];
    for (took, expected) in cases {
        assert_eq!(
            format_took(took),
            expected,
            "{took:?} should read as {expected}"
        );
    }
}

/// The Resources tab says how the open reads the data, and nothing for a frame no
/// open found.
#[test]
fn the_resources_tab_says_how_the_data_is_read() {
    use crate::table::{DataTableState, OpenFacts};
    let painted = |read_mode: Option<crate::ReadMode>| {
        let rows = || df!("id" => [1i64, 2]).unwrap().lazy();
        let schema = Arc::new((*rows().collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            read_mode,
            ..Default::default()
        });
        let theme = RenderContext::for_test();
        let area = Rect::new(0, 0, 60, 12);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        )
        .render_resources_tab(area, &mut buf);
        crate::tests::buffer_lines(&buf)
    };
    let lines = painted(Some(crate::ReadMode::InMemory));
    assert!(
        lines
            .iter()
            .any(|l| l.trim_end() == format!("{:<17}in memory", "Read:")),
        "{lines:#?}"
    );
    let converted = painted(Some(crate::ReadMode::Converted));
    assert!(converted.iter().any(|l| l.contains("converted to Arrow")));
    assert!(!painted(None).iter().any(|l| l.starts_with("Read:")));
}

/// The Resources tab shows what the open cost, and shows only what was measured.
///
/// Through the rendered tab rather than [`measurement_line`], because the bug worth
/// guarding is a row reaching the panel for a stretch of work that never ran — a
/// dataset opened before any of this existed would otherwise read as one whose
/// listing took no time at all.
#[test]
fn the_resources_tab_shows_what_was_measured_and_nothing_else() {
    use crate::loading::measurements::Meter;
    use crate::table::DataTableState;
    use polars::prelude::*;
    use std::time::Duration;

    // The meter rides on the dataset, so each case paints a dataset carrying the
    // meter under test rather than handing one to the panel beside it.
    let dataset_with = |meter: &std::sync::Arc<Meter>| {
        let rows = || df!("id" => (0..3i64).collect::<Vec<_>>()).unwrap().lazy();
        let mut lf = rows();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        DataTableState::from_schema_and_lazyframe(
            schema,
            rows(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(crate::table::OpenFacts {
            measurements: meter.clone(),
            ..Default::default()
        })
    };

    let theme = RenderContext::for_test();
    let painted = |meter: &std::sync::Arc<Meter>, height: u16| {
        let state = dataset_with(meter);
        let area = Rect::new(0, 0, 70, height);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        let panel = DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        panel.render_resources_tab(area, &mut buf);
        crate::tests::buffer_text(&buf)
    };

    // Nothing measured: no heading, and above all no row of zeroes standing in for
    // a measurement that was never taken.
    let unmeasured = painted(&std::sync::Arc::new(Meter::default()), 24);
    assert!(
        !unmeasured.contains("Measurements"),
        "a meter holding nothing has nothing to show: {unmeasured}"
    );

    // A local open: two stretches, neither of which made a request.
    let local = std::sync::Arc::new(Meter::default());
    local.listed(Duration::from_micros(344), Some(6541), false);
    local.read_footers(Duration::from_millis(3880), Some(6541), false);
    let shown = painted(&local, 24);
    assert!(
        shown.contains("Measurements"),
        "once there is something to say, the section appears: {shown}"
    );
    assert!(
        shown.contains("0.34 ms, 6,541 files"),
        "a listing that really took a third of a millisecond says so, rather than \
             rounding to a figure that reads as unmeasured: {shown}"
    );
    assert!(
        shown.contains("3.88s, 6,541 footers read"),
        "and a stretch over a second is in seconds, counting footers rather than files: {shown}"
    );
    assert!(
        shown.contains("Total:") && shown.contains("3.88s"),
        "the total is a time: {shown}"
    );
    // Every value starts in the one value column, the buffer's too.
    for label in [
        "File size:",
        "Buffer (Rows):",
        "Buffer (MB):",
        "Listing:",
        "Total:",
    ] {
        let line = shown
            .lines()
            .find(|l| l.starts_with(label))
            .unwrap_or_else(|| panic!("{label} in {shown}"));
        let cells: Vec<char> = line.chars().collect();
        assert!(
            cells[label.len()..17].iter().all(|c| *c == ' ') && cells[17] != ' ',
            "{label} value at column 17: {line:?}"
        );
    }
    assert!(
        shown
            .lines()
            .any(|l| l.starts_with("Measurements ") && l.contains(crate::glyphs::get().rule_h)),
        "the heading is a section rule: {shown}"
    );
    assert!(
        !shown.contains("13,082"),
        "and not the two file counts added together, which is not the size of \
             anything: {shown}"
    );
    assert!(
        !shown.contains("requests"),
        "a local open made none, and says nothing rather than saying zero: {shown}"
    );

    // One of a thing is one of a thing. A one-file directory and a single remote
    // object both reach this, and "1 files read" is what the counts are for.
    let just_one = std::sync::Arc::new(Meter::default());
    just_one.listed(Duration::from_millis(1), Some(1), false);
    just_one.footer_request(512);
    just_one.read_footers(Duration::from_millis(2), Some(1), true);
    let singular = painted(&just_one, 24);
    assert!(
        singular.contains("1 file,") || singular.contains("1 file "),
        "one file, not one files: {singular}"
    );
    assert!(
        singular.contains("1 footer read,"),
        "and one footer read, not one footers read: {singular}"
    );
    assert!(
        !singular.contains("1 files") && !singular.contains("1 footers"),
        "neither plural appears anywhere: {singular}"
    );

    // A glob: a listing with no file count and nothing over the wire, beside
    // footers that have both. The row must show a bare time — a `0 files` or a
    // `0 requests` here would each say datui looked and found none.
    let globbed = std::sync::Arc::new(Meter::default());
    globbed.listed(Duration::from_millis(1), None, false);
    // Two footers, two requests each: this route must ask an object's size before
    // it can ask for its tail.
    for _ in 0..4 {
        globbed.footer_request(250);
    }
    globbed.read_footers(Duration::from_millis(3), Some(2), true);
    let glob_shown = painted(&globbed, 24);
    let row = |label: &str| -> String {
        glob_shown
            .lines()
            .find(|l| l.trim_start().starts_with(label))
            .unwrap_or_else(|| panic!("{label} row is shown: {glob_shown}"))
            .to_string()
    };
    let listing_row = row("Listing:");
    assert_eq!(
        listing_row.trim_end(),
        "Listing:         1.00 ms",
        "the walk reports a time and nothing else: no file count it never learned, \
             and no request count no listing route can take"
    );
    let total_row = row("Total:");
    assert!(
        total_row.contains("4.00 ms") && total_row.contains("4 requests"),
        "and the total is both times with the footer reads' requests: {total_row:?}"
    );

    // A remote open: the footer pass counted its own requests and bytes.
    let remote = std::sync::Arc::new(Meter::default());
    remote.listed(Duration::from_millis(500), Some(3), false);
    remote.footer_request(49_152);
    remote.read_footers(Duration::from_millis(1500), Some(3), true);
    let over_wire = painted(&remote, 24);
    assert!(
        over_wire.contains("1.50s, 3 footers read, 1 request, 48.0 KiB"),
        "the footer row says what datui asked for and what came back: {over_wire}"
    );
    assert!(
        over_wire.contains("500.00 ms, 3 files") && !over_wire.contains("500.00 ms, 3 files, "),
        "while the listing, whose pages the store turns over itself, claims no \
             requests of its own: {over_wire}"
    );

    // Every height, down to one that fits nothing. A heading with no row under it
    // is the failure this checks for: it says a section was cut off where there may
    // have been nothing to cut, and there is exactly one height per tab layout at
    // which a guard that is short by one produces it.
    for height in 1..=24u16 {
        let short = painted(&local, height);
        if short.contains("Measurements") {
            assert!(
                short.contains("Listing:"),
                "at height {height} the heading is shown with no row under it: {short}"
            );
        }
    }
}

/// A file of several tables names the others under the schema's size; a file of
/// one adds no line.
#[test]
fn the_schema_tab_names_a_file_s_other_tables() {
    use crate::table::{DataTableState, OpenFacts};
    use polars::prelude::*;

    let theme = RenderContext::for_test();
    let paint = |other_tables: Vec<String>| {
        let mut lf = df!("id" => &[1i64, 2]).unwrap().lazy();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let state = DataTableState::from_schema_and_lazyframe(
            schema,
            lf,
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            other_tables,
            ..Default::default()
        });
        let area = Rect::new(0, 0, 70, 12);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        modal.open();
        let mut panel = DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        (&mut panel).render(area, &mut buf);
        crate::tests::buffer_lines(&buf)
    };
    let middot = crate::glyphs::get().middot;
    let text = paint(vec!["GSV 9".into(), "sentences".into()]);
    let line = format!("Other tables (--table): GSV 9 {middot} sentences");
    assert!(text.iter().any(|row| row.contains(&line)), "{text:#?}");
    let text = paint(Vec::new());
    assert!(
        !text.iter().any(|row| row.contains("Other tables")),
        "{text:#?}"
    );
}

/// The format tab is named for the format, shows its lines, then its list under a
/// rule with a count, and says how much of the list is out of view.
#[test]
fn the_format_tab_shows_its_lines_and_list() {
    use crate::formats::model_files::MetaValue;
    use crate::formats::text_formats::Detail;
    use crate::table::{DataTableState, OpenFacts};
    use polars::prelude::*;

    let theme = RenderContext::for_test();
    let mut lf = df!("time" => &[1i64]).unwrap().lazy();
    let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
    let list: Vec<(String, MetaValue)> = (0..30)
        .map(|i| {
            (
                format!("tb.sig{i}"),
                MetaValue::Text(format!("wire 1 bit id {i}")),
            )
        })
        .collect();
    let state =
        DataTableState::from_schema_and_lazyframe(schema, lf, &crate::OpenOptions::default(), None)
            .unwrap()
            .with_open(OpenFacts {
                detail: Some(std::sync::Arc::new(Detail {
                    tab: "VCD",
                    lines: vec!["VCD timescale 1ns".into(), "Version: Icarus".into()],
                    list_title: "Signals",
                    list,
                    first: true,
                    ..Default::default()
                })),
                ..Default::default()
            });
    let area = Rect::new(0, 0, 60, 20);
    let mut buf = Buffer::empty(area);
    let mut modal = InfoModal::default();
    modal.open_on(InfoTab::Format);
    let mut panel = DataTableInfo::new(
        &state,
        InfoContext {
            format: None,
            facts: None,
            facts_tab: None,
            footer_expected: false,
            declared_types: false,
        },
        &mut modal,
        &theme,
    );
    (&mut panel).render(area, &mut buf);
    let text: Vec<String> = crate::tests::buffer_lines(&buf);
    let has = |needle: &str| text.iter().any(|row| row.contains(needle));
    assert!(has("VCD") && !has("Format"), "{text:#?}");
    assert!(has("Version: Icarus"), "{text:#?}");
    assert!(has("Signals") && has("30"), "{text:#?}");
    assert!(has("tb.sig0") && has("wire 1 bit id 0"), "{text:#?}");
    assert!(has("below"), "the rest is counted: {text:#?}");
}

/// The body has the keys: the schema's rule is bright and the row carries the
/// rail, and the tab line, which never takes focus, has none. One frame, the
/// footer inside it.
#[test]
fn the_body_has_the_accent_and_the_tab_bar_none() {
    use crate::table::DataTableState;
    use polars::prelude::*;

    let rows = || {
        df!("id" => &[1i64, 2], "name" => &["a", "b"])
            .unwrap()
            .lazy()
    };
    let mut lf = rows();
    let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
    let state = DataTableState::from_schema_and_lazyframe(
        schema,
        rows(),
        &crate::OpenOptions::default(),
        None,
    )
    .unwrap();
    let theme = RenderContext::for_test();
    let g = crate::glyphs::get();

    let paint = || {
        let area = Rect::new(0, 0, 50, 16);
        let mut buf = Buffer::empty(area);
        let mut modal = InfoModal::default();
        modal.open();
        let mut panel = DataTableInfo::new(
            &state,
            InfoContext {
                format: None,
                facts: None,
                facts_tab: None,
                footer_expected: false,
                declared_types: false,
            },
            &mut modal,
            &theme,
        );
        (&mut panel).render(area, &mut buf);
        let text: Vec<String> = crate::tests::buffer_lines(&buf);
        (buf, text)
    };
    let find = |text: &[String], needle: &str| {
        let y = text
            .iter()
            .position(|row| row.contains(needle))
            .unwrap_or_else(|| panic!("{needle:?} not drawn: {text:#?}"));
        // Cells, not bytes: the frame and the rail are multibyte.
        let x = text[y][..text[y].find(needle).unwrap()].chars().count();
        (x as u16, y as u16)
    };

    let (buf, text) = paint();
    let (x, y) = find(&text, "Schema  types inferred");
    // The rule's title in the plain accent: the rail marks focus, not the rule.
    assert_eq!(buf[(x, y)].fg, theme.accent, "{text:#?}");
    let (_, id_row) = find(&text, " id ");
    assert!(text[id_row as usize].contains(g.rail), "{text:#?}");
    let (x, y) = find(&text, "Resources");
    assert!(!text[y as usize].contains(g.rail), "{text:#?}");
    assert_ne!(
        buf[(x, y)].fg,
        theme.accent,
        "an inactive tab is not accented"
    );

    // One frame: its corners on the first and last rows and nowhere else,
    // the footer on the last row inside it.
    for row in &text[1..text.len() - 1] {
        assert!(
            !row.contains(g.border.top_left) && !row.contains(g.border.bottom_left),
            "a second border inside the panel: {text:#?}"
        );
    }
    assert!(text[text.len() - 2].contains("Esc"), "{text:#?}");
}

#[test]
fn the_tabs_on_offer_depend_on_the_dataset() {
    assert_eq!(
        InfoTab::visible(offer(false, false)),
        [InfoTab::Schema, InfoTab::Resources]
    );
    assert_eq!(
        InfoTab::visible(offer(true, true)),
        [
            InfoTab::Schema,
            InfoTab::Resources,
            InfoTab::Partitions,
            InfoTab::Notes
        ]
    );
    assert_eq!(
        InfoTab::visible(offer(false, true)),
        [InfoTab::Schema, InfoTab::Resources, InfoTab::Notes],
        "notes without partitions still sit last"
    );
}

#[test]
fn the_format_tab_sits_beside_the_schema() {
    let offered = TabsOffered {
        format: true,
        ..offer(false, true)
    };
    assert_eq!(
        InfoTab::visible(offered),
        [
            InfoTab::Schema,
            InfoTab::Format,
            InfoTab::Resources,
            InfoTab::Notes
        ]
    );
    assert_eq!(InfoTab::Format.prev(offered), InfoTab::Schema);
    assert_eq!(InfoTab::Format.index(offer(false, false)), 0, "not offered");
}

#[test]
fn a_clock_shows_hours_only_when_there_are_some() {
    assert_eq!(clock(3.25), "0:03.250");
    assert_eq!(clock(62.0), "1:02.000");
    assert_eq!(clock(3723.0005), "1:02:03.001");
}

/// A value breaks between words; indentation stays, the spaces at a break go, and
/// only a word wider than the room is split.
#[test]
fn metadata_values_wrap_on_word_boundaries() {
    assert_eq!(
        wrap_words("Broadcast WAV coding history", 12),
        ["Broadcast", "WAV coding", "history"]
    );
    assert_eq!(
        wrap_words("    {% if x %}   y", 10),
        ["    {% if", "x %}   y"]
    );
    assert_eq!(
        wrap_words("a 0123456789abcdef", 6),
        ["a 0123", "456789", "abcdef"]
    );
    // Measured in columns: three double-width characters are six.
    assert_eq!(wrap_words("日本語 text", 7), ["日本語", "text"]);
    assert_eq!(wrap_words("", 5), [""]);
    // Spaces at a break or past the room leave no blank line.
    assert_eq!(wrap_words("abc   ", 4), ["abc"]);
    assert_eq!(wrap_words("          x", 5), ["x"]);
}

/// Each value is drawn whole: its own newlines kept, wrapped under the key, a short
/// array listed and a long one counted.
#[test]
fn metadata_values_wrap_whole_under_their_key() {
    use crate::formats::model_files::MetaValue;
    let meta = vec![
        (
            "a".to_string(),
            MetaValue::Text("line one\nsecond line that is long".to_string()),
        ),
        (
            "tokens".to_string(),
            MetaValue::List {
                of: "strings",
                len: 151_936,
                items: vec![],
            },
        ),
        (
            "tags".to_string(),
            MetaValue::List {
                of: "strings",
                len: 2,
                items: vec!["x".to_string(), "y".to_string()],
            },
        ),
    ];
    let lines = metadata_lines(&meta, 20);
    let key = |s: &str| format!("{s:<6}  ");
    let blank = " ".repeat(8);
    assert_eq!(
        lines,
        [
            (key("a"), "line one".to_string()),
            (blank.clone(), "second line".to_string()),
            (blank.clone(), "that is long".to_string()),
            (key("tokens"), "[151,936".to_string()),
            (blank.clone(), "strings]".to_string()),
            (key("tags"), "[\"x\", \"y\"]".to_string()),
        ]
    );
    // A value of megabytes is drawn to its first 64 KiB, and says what is left.
    let huge = vec![(
        "tokenizer.huggingface.json".to_string(),
        MetaValue::Text("x".repeat(VALUE_SHOWN_BYTES + 2048)),
    )];
    let lines = metadata_lines(&huge, 80);
    let last = &lines.last().unwrap().1;
    assert!(last.ends_with("2.0 KiB more"), "{last}");
    let drawn: usize = lines[..lines.len() - 1].iter().map(|(_, v)| v.len()).sum();
    assert_eq!(drawn, VALUE_SHOWN_BYTES);
    assert_eq!(short_count(8_030_261_248), "8.0B");
    assert_eq!(short_count(950), "950");
}

#[test]
fn tab_navigation_wraps_through_what_is_on_offer() {
    // Nothing optional: two tabs, back and forth.
    assert_eq!(
        InfoTab::Schema.next(offer(false, false)),
        InfoTab::Resources
    );
    assert_eq!(
        InfoTab::Resources.next(offer(false, false)),
        InfoTab::Schema
    );
    assert_eq!(
        InfoTab::Schema.prev(offer(false, false)),
        InfoTab::Resources
    );

    // Both optional tabs present.
    assert_eq!(
        InfoTab::Resources.next(offer(true, true)),
        InfoTab::Partitions
    );
    assert_eq!(InfoTab::Partitions.next(offer(true, true)), InfoTab::Notes);
    assert_eq!(InfoTab::Notes.next(offer(true, true)), InfoTab::Schema);
    assert_eq!(InfoTab::Schema.prev(offer(true, true)), InfoTab::Notes);

    // Notes only.
    assert_eq!(InfoTab::Resources.next(offer(false, true)), InfoTab::Notes);
    assert_eq!(InfoTab::Notes.prev(offer(false, true)), InfoTab::Resources);
}

/// A tab that is no longer on offer must not strand the cursor: it reads as the
/// first tab, so moving on from it goes somewhere real.
#[test]
fn a_tab_that_is_no_longer_offered_falls_back_to_the_first() {
    assert_eq!(InfoTab::Notes.index(offer(false, false)), 0);
    assert_eq!(InfoTab::Notes.next(offer(false, false)), InfoTab::Resources);
    assert_eq!(InfoTab::Partitions.index(offer(false, false)), 0);
    assert_eq!(
        InfoTab::Partitions.prev(offer(false, false)),
        InfoTab::Resources
    );
}

/// The window's three promises, checked over every shape that fits in a terminal.
///
/// Review after review found defects in this arithmetic while it lived inside the
/// render, and the test that was meant to guard it re-implemented the same
/// arithmetic — so the two drifted and it could never fail. This calls the real
/// function and asserts what the panel actually needs.
#[test]
fn the_notes_window_always_shows_the_selected_note_and_wastes_no_room() {
    let shapes: Vec<Vec<usize>> = vec![
        vec![2, 2, 2, 2, 2, 2],
        vec![2],
        vec![3, 2, 4, 2],
        vec![2, 9, 2],
        vec![5, 5, 5],
        vec![1, 1, 1, 1, 1, 1, 1, 1],
        vec![4, 2, 2, 7, 2],
    ];
    let span =
        |h: &[usize], a: usize, b: usize| h[a..b].iter().sum::<usize>() + (b - a).saturating_sub(1);
    for heights in &shapes {
        for show in 1..=30usize {
            for selected in 0..heights.len() {
                for stored in 0..heights.len() {
                    let (first, last) = notes_window(heights, selected, stored, show);
                    let at = format!(
                        "heights {heights:?}, show {show}, selected {selected}, stored {stored}"
                    );

                    assert!(first <= selected, "the cursor is above the window at {at}");
                    assert!(selected < last, "the cursor is below the window at {at}");

                    let used = span(heights, first, last);
                    if last - first > 1 {
                        assert!(used <= show, "{used} rows in {show} at {at}");
                    }

                    // Nothing more would fit below, and nothing more would fit above.
                    if last < heights.len() {
                        assert!(
                            span(heights, first, last + 1) > show,
                            "another note below would have fitted at {at}"
                        );
                    }
                    if first > 0 {
                        assert!(
                            span(heights, first - 1, last) > show,
                            "another note above would have fitted at {at}"
                        );
                    }
                }
            }
        }
    }
}

/// A note taller than the whole panel is still drawn, because leaving it out would
/// put it out of reach.
#[test]
fn a_note_taller_than_the_panel_is_still_the_window() {
    let (first, last) = notes_window(&[2, 9, 2], 1, 0, 4);
    assert_eq!((first, last), (1, 2), "just the note that does not fit");
}

/// The panel is the only thing that decides how many notes fit, and the cursor can
/// always reach the last of them.
#[test]
fn the_notes_cursor_reaches_every_note() {
    let mut modal = InfoModal::new();
    assert!(!modal.notes_move(1, 0), "nothing to move through");
    for expected in 1..5 {
        assert!(modal.notes_move(1, 5));
        assert_eq!(modal.notes_selected_index, expected);
    }
    assert!(!modal.notes_move(1, 5), "and stops at the last");
    for expected in (0..4).rev() {
        assert!(modal.notes_move(-1, 5));
        assert_eq!(modal.notes_selected_index, expected);
    }
    assert!(!modal.notes_move(-1, 5), "and at the first");
}

#[test]
fn wrapping_measures_columns_not_characters() {
    assert_eq!(wrap_to("one two three", 9), ["one two", "three"]);
    assert_eq!(wrap_to("", 10), [""], "an empty line is still a line");
    assert_eq!(
        wrap_to("supercalifragilistic", 5),
        ["supercalifragilistic"],
        "a word longer than the panel is left whole rather than broken"
    );
    // Double-width characters take two columns each, so four of them fill eight.
    assert_eq!(wrap_to("日本語表 x", 8), ["日本語表", "x"]);
}

#[test]
fn test_format_int() {
    assert_eq!(format_int(0), "0");
    assert_eq!(format_int(1234), "1,234");
    assert_eq!(format_int(1_234_567), "1,234,567");
}
