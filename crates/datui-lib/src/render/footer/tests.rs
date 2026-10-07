use super::*;

/// A hint says the registry's word for its key, or one of the other words the
/// entry lists for it: the footers and the help are written from one place.
#[test]
fn a_hint_says_the_registrys_words() {
    let arrows = registry_hint(Context::Inspector, "↑ / ↓");
    let entry = datui_cli::keys::lookup(Context::Inspector, None, "↑ / ↓ (j/k)").unwrap();
    assert_eq!(arrows.label, entry.label);
    assert_eq!(arrows.key, crate::glyphs::get().updown);
    let differ = registry_hint_as(Context::Inspector, None, "f", "Differ");
    assert_eq!(
        (differ.key.as_ref(), differ.label.as_ref()),
        ("f", "Differ")
    );
    assert_eq!(registry_hint(Context::Global, "Ctrl+O").key, "^O");
}

/// A word of the footer's own is refused, wherever the footer is drawn.
#[test]
#[should_panic(expected = "does not say")]
fn a_word_of_the_footers_own_is_refused() {
    registry_hint_as(Context::Inspector, None, "f", "Hidden");
}

fn line(footer: &Footer, width: u16) -> String {
    let area = Rect::new(0, 0, width, 1);
    let mut buf = Buffer::empty(area);
    footer.render_line(area, &mut buf, &RenderContext::for_test());
    (0..width)
        .map(|x| buf[(x, 0)].symbol().to_string())
        .collect::<String>()
        .trim_end()
        .to_string()
}

fn busy_footer() -> Footer {
    Footer {
        dataset: Some("weather/daily.parquet".to_string()),
        stages: vec![QUERY_STAGE.to_string()],
        view: ViewState {
            typed: Vec::new(),
            filters: vec!["prcp > 0".to_string()],
            sort: Some("date ▼".to_string()),
        },
        work: Some("Reading footers: 3 of 40...".to_string()),
        notes: Vec::new(),
        message: None,
        message_path: None,
        position: Some(Position {
            row: 41_208,
            total: Total::Known(1_204_331),
            column: None,
        }),
        hints: vec![
            registry_hint_in(Context::Find, Some("At the table"), "n / N"),
            registry_hint_in(Context::Find, Some("At the table"), "Esc"),
        ],
        help: Some("?"),
        query_key: None,
        view_key: None,
        spinner: "|",
    }
}

/// The `query` stage and the filters are keys to click where the footer says
/// so, cut at the edge; elsewhere they are only text.
#[test]
fn the_query_and_the_filters_are_clickable_where_given_keys() {
    let ctx = RenderContext::for_test();
    let drawn = |footer: &Footer, width: u16| {
        let area = Rect::new(0, 3, width, 1);
        let mut buf = Buffer::empty(area);
        let drawn = footer.render_line(area, &mut buf, &ctx);
        let text: String = (0..width).map(|x| buf[(x, 3)].symbol()).collect();
        (drawn, text)
    };
    let plain = busy_footer();
    let (keys, _) = drawn(&plain, 140);
    assert!(keys.iter().all(|(_, k)| k != ":" && k != "s"), "{keys:?}");

    let footer = Footer {
        query_key: Some(":"),
        view_key: Some("s"),
        ..busy_footer()
    };
    let (keys, text) = drawn(&footer, 140);
    let find = |key: &str| keys.iter().find(|(_, k)| k == key).map(|(r, _)| *r);
    let query = find(":").expect("the query is a key");
    let at = text.find(QUERY_STAGE).unwrap();
    assert_eq!(
        (query.x, query.y, query.width),
        (text[..at].chars().count() as u16, 3, 5)
    );
    let view = find("s").expect("the filters are a key");
    let from = text[..text.find("prcp").unwrap()].chars().count() as u16;
    let to = text[..text.find("date ▼").unwrap()].chars().count() as u16 + 6;
    assert_eq!((view.x, view.right()), (from, to), "{text}");
    assert!(find("?").is_some(), "help is still a key");
}

#[test]
fn at_rest_only_help_shows_at_the_right() {
    let footer = Footer {
        help: Some("?"),
        spinner: "|",
        ..Footer::default()
    };
    assert_eq!(line(&footer, 40).trim(), "? keys");
}

#[test]
fn the_line_reads_in_pipeline_order() {
    let text = line(&busy_footer(), 160);
    let at = |s: &str| text.find(s).unwrap_or_else(|| panic!("{s:?} in {text:?}"));
    assert!(at("weather/daily") < at("query"));
    assert!(at("query") < at("prcp > 0"));
    assert!(at("prcp > 0") < at("date"));
    assert!(at("date") < at("41,208 / 1,204,331"));
    assert!(at("41,208") < at("n/N Next"));
    assert!(text.ends_with("? keys"), "{text:?}");
}

/// Each width keeps the segments the priority order says it keeps.
#[test]
fn segments_yield_in_priority_order() {
    let footer = busy_footer();
    let wide = line(&footer, 160);
    assert!(
        wide.contains("weather/daily") && wide.contains("prcp > 0"),
        "{wide}"
    );

    let w120 = line(&footer, 120);
    assert!(
        w120.contains("prcp > 0") && w120.contains("41,208 / 1,204,331"),
        "the name is cut first: {w120}"
    );
    assert!(!w120.contains("weather/daily.parquet"), "{w120}");

    let w80 = line(&footer, 80);
    assert!(w80.contains("n/N Next") && w80.contains("? keys"), "{w80}");
    assert!(w80.contains("41,208"), "{w80}");
    assert!(w80.contains("query"), "{w80}");

    let w60 = line(&footer, 60);
    assert!(
        w60.contains("n/N Next") && w60.contains("Esc Clear"),
        "{w60}"
    );
    assert!(!w60.contains("weather"), "the name went first: {w60}");

    let w40 = line(&footer, 40);
    assert!(w40.contains("n/N Next"), "{w40}");
    assert!(w40.contains('?'), "{w40}");
    assert!(!w40.contains("prcp"), "{w40}");
    for text in [&wide, &w80, &w60, &w40] {
        assert!(!text.contains("Clea "), "nothing is cut mid-word: {text}");
    }
}

/// A flash that ends in a path keeps what it says and the file name when cut;
/// any other is cut at its end.
#[test]
fn a_path_flash_keeps_its_file_name() {
    let message = "Exported to /home/someone/projects/weather/daily/out.csv";
    let cut = super::cut_message(message, Some("Exported to ".len()), 32);
    let mark = crate::glyphs::get().ellipsis;
    assert!(cut.starts_with(&format!("Exported to {mark}")), "{cut}");
    assert!(cut.ends_with("/out.csv"), "{cut}");
    assert_eq!(crate::glyphs::display_width(&cut), 32);
    let plain = super::cut_message("Copied 3 rows to the clipboard", None, 12);
    assert!(
        plain.starts_with("Copied") && plain.ends_with(mark),
        "{plain}"
    );
    assert_eq!(super::cut_message(message, Some(12), 200), message);
}

#[test]
fn a_message_takes_the_dataset_s_room_first() {
    let mut footer = busy_footer();
    footer.message = Some("Copied 3 rows".to_string());
    let text = line(&footer, 100);
    assert!(text.contains("Copied 3 rows"), "{text}");
    assert!(!text.contains("weather"), "{text}");
    assert!(text.contains("? keys"), "{text}");
}

#[test]
fn filters_shrink_to_counts_before_they_go() {
    let mut footer = busy_footer();
    footer.dataset = None;
    footer.work = None;
    footer.view.filters = vec![
        "temperature > 30".to_string(),
        "station = \"USW00094728\"".to_string(),
    ];
    let text = line(&footer, 80);
    assert!(text.contains("2 filters"), "{text}");
    assert!(text.contains("sorted"), "{text}");
}

#[test]
fn a_long_name_is_cut_in_the_middle() {
    assert_eq!(crate::glyphs::fit_middle("abcdefghij", 10), "abcdefghij");
    let cut = crate::glyphs::fit_middle("weather/stations/daily", 12);
    assert_eq!(crate::glyphs::display_width(&cut), 12);
    assert!(cut.starts_with("weath") && cut.ends_with("daily"), "{cut}");
}

#[test]
fn the_position_shortens_before_it_goes() {
    let footer = Footer {
        position: Some(Position {
            row: 41_208,
            total: Total::Known(1_204_331),
            column: None,
        }),
        hints: vec![
            registry_hint(Context::Table, "+ / -"),
            registry_hint(Context::Table, "F"),
        ],
        help: Some("?"),
        spinner: "|",
        ..Footer::default()
    };
    let text = line(&footer, 48);
    assert!(text.contains("41,208 / 1.2M"), "{text}");
}

/// With no fill, the footer's colors sit on the terminal's background: in the dark
/// and the light palette, and once degraded to 256 colors, each part keeps a color
/// of its own, apart from the background.
#[test]
fn the_footer_reads_on_both_palettes_and_at_256_colors() {
    use crate::config::{ColorConfig, rgb_to_256_color};
    let rgb = |hex: &str| -> (u8, u8, u8) {
        let v = u32::from_str_radix(hex.trim_start_matches('#'), 16).expect(hex);
        ((v >> 16) as u8, (v >> 8) as u8, v as u8)
    };
    for (mode, colors) in [
        ("dark", ColorConfig::dark()),
        ("light", ColorConfig::light()),
    ] {
        // The background is the terminal's own; the text on a key chip matches it.
        let background = rgb(&colors.text_inverse);
        let parts = [
            ("rule", &colors.table_column_separator),
            ("keys", &colors.chip_key),
            ("labels", &colors.chip_label),
            ("secondary", &colors.text_secondary),
            ("separators", &colors.dimmed),
            ("filters", &colors.warning),
        ];
        for (part, hex) in parts {
            let c = rgb(hex);
            let distance = (c.0 as i32 - background.0 as i32).abs()
                + (c.1 as i32 - background.1 as i32).abs()
                + (c.2 as i32 - background.2 as i32).abs();
            assert!(
                distance >= 60,
                "{mode}: the {part} {hex} on {}",
                colors.text_inverse
            );
            assert_ne!(
                rgb_to_256_color(c.0, c.1, c.2),
                rgb_to_256_color(background.0, background.1, background.2),
                "{mode} at 256 colors: the {part} {hex} becomes the background"
            );
        }
        assert_ne!(
            rgb_to_256_color(
                rgb(&colors.chip_key).0,
                rgb(&colors.chip_key).1,
                rgb(&colors.chip_key).2
            ),
            rgb_to_256_color(
                rgb(&colors.chip_label).0,
                rgb(&colors.chip_label).1,
                rgb(&colors.chip_label).2
            ),
            "{mode} at 256 colors: a key and its label stay apart"
        );
    }
}

#[test]
fn a_progress_line_counts_draws_a_bar_and_offers_stop() {
    let progress = ProgressLine {
        counts: vec![
            ProgressCount::of("rows", 412_880_117, None),
            ProgressCount::of("files", 18_402, Some(126_033)),
        ],
        stoppable: true,
    };
    let area = Rect::new(0, 0, 100, 1);
    let mut buf = Buffer::empty(area);
    render_progress(&progress, area, &mut buf, &RenderContext::for_test());
    let text: String = (0..100).map(|x| buf[(x, 0)].symbol().to_string()).collect();
    assert!(text.contains("rows 412,880,117"), "{text}");
    assert!(text.contains("files 18,402 / 126,033"), "{text}");
    assert!(text.contains("15%"), "{text}");
    assert!(text.trim_end().ends_with("Esc Stop"), "{text}");
}
