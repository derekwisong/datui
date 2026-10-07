use super::*;
use crate::app::modals::pivot_melt_modal::{
    PivotAggregation, PivotSpec, PreviewInput, PreviewSpec, run_preview,
};
use polars::prelude::*;

fn modal_with_columns(columns: &[&str]) -> PivotMeltModal {
    let mut m = PivotMeltModal::new();
    m.available_columns = columns.iter().map(|s| s.to_string()).collect();
    let config = crate::config::AppConfig::default();
    let theme = crate::config::Theme::from_config(&config.theme).unwrap();
    m.open(1000, &theme);
    m
}

fn render_rows(modal: &mut PivotMeltModal, width: u16, height: u16) -> Vec<String> {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    render(area, &mut buf, modal, &ctx);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

fn long() -> DataFrame {
    df!(
        "dept" => ["a", "a", "b", "b"],
        "job" => ["x", "y", "x", "y"],
        "salary" => [1i64, 2, 3, 4],
    )
    .unwrap()
}

fn pivot_spec() -> PivotSpec {
    PivotSpec {
        index: vec!["dept".to_string()],
        pivot_column: "job".to_string(),
        value_column: "salary".to_string(),
        aggregation: PivotAggregation::Avg,
    }
}

/// A modal staged for `pivot_spec`, with the preview answered over `input`.
fn previewed(input: DataFrame, whole: bool, view_rows: Option<usize>) -> PivotMeltModal {
    let mut m = modal_with_columns(&["dept", "job", "salary"]);
    m.index_columns = vec!["dept".to_string()];
    m.pivot_column = Some("job".to_string());
    m.value_column = Some("salary".to_string());
    m.aggregation = PivotAggregation::Avg;
    let spec = PreviewSpec::Pivot(pivot_spec());
    let result = run_preview(&input, &spec);
    m.preview.input = Some(PreviewInput {
        rows: std::sync::Arc::new(input),
        whole,
    });
    m.preview.view_rows = view_rows;
    m.preview.wanted = Some(spec.clone());
    m.preview.shown = Some((spec, result));
    m
}

/// Wide: one border, the form on the left with each choice echoed at the
/// shared column, and the preview on the right with the shape and the rows.
#[test]
fn the_wide_builder_puts_the_preview_beside_the_form() {
    let g = crate::glyphs::get();
    let mut m = previewed(long(), true, Some(4));
    let rows = render_rows(&mut m, 120, 24);

    assert!(rows[0].contains("Pivot & Melt"), "title on the frame");
    for row in &rows[1..23] {
        assert!(
            !row.contains('╭') && !row.contains('╰'),
            "a second border inside the surface: {row:?}"
        );
    }
    assert!(rows[1].contains("Pivot") && rows[1].contains("Melt"));
    assert!(rows[3].contains("Index:") && rows[3].contains("dept"));
    assert!(rows[4].contains("Columns:") && rows[4].contains("job"));
    assert!(rows[5].contains("Values:") && rows[5].contains("salary"));
    assert!(rows[6].contains("Aggregate:") && rows[6].contains("avg"));
    let char_col = |s: &str, needle: &str| s.find(needle).map(|b| s[..b].chars().count());
    for (row, value) in [(3, "dept"), (4, "job"), (5, "salary"), (6, "avg")] {
        assert_eq!(
            char_col(&rows[row], value),
            Some(2 + 1 + LABEL_WIDTH as usize),
            "row {row} value out of column: {:?}",
            rows[row]
        );
    }
    let spec = format!("dept {} job {} avg(salary)", g.times, g.arrow_right);
    assert!(rows[22].contains(&spec), "the spec line: {:?}", rows[22]);

    // The preview starts past the form.
    let at = 2 + FORM_WIDTH as usize + GAP as usize;
    assert_eq!(char_col(&rows[1], "Preview"), Some(at), "{:?}", rows[1]);
    assert!(rows[2].contains("Input") && rows[2].contains("all 4 rows"));
    assert!(
        rows[3].contains(&format!("2 rows {} 3 columns", g.times)),
        "the exact shape: {:?}",
        rows[3]
    );
    // Names, types, then the rows.
    assert!(rows[6].contains("dept") && rows[6].contains('x') && rows[6].contains('y'));
    assert!(rows[7].contains("str") && rows[7].contains("f64"));
    assert!(rows[8].contains('a') && rows[8].contains("1.0") && rows[8].contains("2.0"));
    assert!(rows[9].contains('b') && rows[9].contains("3.0") && rows[9].contains("4.0"));
}

/// Narrow: the form on top, the preview under it, nothing cut off the side.
#[test]
fn a_narrow_builder_stacks_the_preview_under_the_form() {
    let g = crate::glyphs::get();
    let mut m = previewed(long(), true, Some(4));
    let rows = render_rows(&mut m, 70, 30);
    let find = |needle: &str| rows.iter().position(|r| r.contains(needle));
    let spec = find(&format!("dept {} job", g.times)).expect("spec line");
    let preview = find("Preview").expect("preview rule");
    assert!(preview > spec, "the preview is under the form: {rows:#?}");
    assert!(find("Aggregate:").unwrap() < spec);
    assert!(rows[preview + 2].contains(&format!("2 rows {} 3 columns", g.times)));
}

/// Only part of the view read: the rows are unknown and a pivot's columns
/// a floor; the input line says how much was read.
#[test]
fn a_head_says_what_it_cannot_know() {
    let g = crate::glyphs::get();
    let m = previewed(long(), false, Some(1_939_184));
    let (_, Ok(frame)) = m.preview.shown.as_ref().unwrap() else {
        panic!("the pivot runs");
    };
    assert_eq!(
        shape_line(&m.preview, frame),
        format!("? rows {} 3+ columns", g.times)
    );
    assert_eq!(input_line(&m.preview), "first 1,000 rows of 1,939,184");
    let mut m = m;
    m.preview.sorted = true;
    assert_eq!(
        input_line(&m.preview),
        "first 1,000 rows of 1,939,184, unsorted"
    );
}

/// Many new columns: the callout says so, before anything is applied.
#[test]
fn a_wide_pivot_is_called_out() {
    let n = 150;
    let input = df!(
        "dept" => vec!["a"; n],
        "job" => (0..n).map(|i| format!("j{i:03}")).collect::<Vec<_>>(),
        "salary" => (0..n as i64).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut m = previewed(input, false, None);
    let rows = render_rows(&mut m, 120, 24);
    let body = rows.join("\n");
    assert!(
        body.contains("150 new columns from 150 rows"),
        "the callout: {body}"
    );
    assert!(
        body.contains("+1"),
        "columns that do not fit are counted: {body}"
    );
}

/// A failed reshape says why in the preview, not in a modal.
#[test]
fn a_failed_preview_says_why_in_the_pane() {
    let mut m = previewed(long(), true, None);
    let spec = m.preview.wanted.clone().unwrap();
    m.preview.shown = Some((spec, Err("Something is wrong".to_string())));
    let body = render_rows(&mut m, 120, 24).join("\n");
    assert!(body.contains("Something is wrong"), "{body}");
}

/// Before anything is chosen the rows say "none", the spec line names the
/// first gap, and the preview shows no result.
#[test]
fn an_empty_form_shows_placeholders_and_the_gap() {
    let mut m = modal_with_columns(&["a", "b"]);
    let rows = render_rows(&mut m, 120, 24);
    assert!(rows[3].contains("none"), "empty index: {:?}", rows[3]);
    assert!(
        rows[22].contains("Select at least one index column."),
        "the gap is named: {:?}",
        rows[22]
    );
    let result = rows[3].find("Result").expect("the result line");
    assert!(
        rows[3][result + "Result".len()..]
            .trim_start()
            .starts_with('-'),
        "no result yet: {:?}",
        rows[3]
    );
}

/// The open Picker drops in below the rows, scoped to the focused row,
/// with a checkbox per item on a toggle row.
#[test]
fn the_index_picker_shows_toggles() {
    let g = crate::glyphs::get();
    let mut m = modal_with_columns(&["dept", "region", "salary"]);
    m.focus = PivotMeltFocus::PivotIndex;
    m.open_picker();
    m.picker_toggle(); // dept in
    let rows = render_rows(&mut m, 120, 24);
    let body = rows.join("\n");
    assert_eq!(
        body.matches(g.rail).count(),
        1,
        "the picker's line has the one rail, not its row too: {body}"
    );
    assert!(body.contains(&format!("{} dept", g.checkbox_on)), "{body}");
    assert!(
        body.contains(&format!("{} region", g.checkbox_off)),
        "{body}"
    );
    let keys: Vec<(String, String)> = hints(&m)
        .into_iter()
        .map(|h| (h.key.into_owned(), h.label.into_owned()))
        .collect();
    assert_eq!(
        keys[0],
        ("Space".to_string(), "Toggle".to_string()),
        "the footer says Space toggles: {keys:?}"
    );
}

/// An open Picker owns the clicks even on a terminal too short to draw it, so a
/// click cannot move focus off the row it belongs to.
#[test]
fn an_open_picker_with_no_room_still_takes_the_clicks() {
    let mut m = modal_with_columns(&["dept", "region", "salary"]);
    m.focus = PivotMeltFocus::PivotIndex;
    m.open_picker();
    let hits = crate::app::pointer::recording(|| {
        render_rows(&mut m, 120, 9);
    });
    assert!(hits.iter().any(|(_, h)| *h == Hit::Picker), "{hits:?}");
    assert!(
        !hits
            .iter()
            .any(|(_, h)| matches!(h, Hit::PickerItem { .. }))
    );
}

/// The melt form swaps its strategy row's dependents in place.
#[test]
fn the_melt_form_follows_the_strategy() {
    let mut m = modal_with_columns(&["id", "q1", "q2"]);
    m.switch_tab();
    m.melt_value_strategy = crate::app::modals::pivot_melt_modal::MeltValueStrategy::ByPattern;
    let rows = render_rows(&mut m, 120, 24);
    assert!(rows[5].contains("Pattern:"), "got {:?}", rows[5]);
    assert!(rows[6].contains("Variable name:"), "got {:?}", rows[6]);
    assert!(rows[7].contains("Value name:") && rows[7].contains("value"));
}

/// Every hint the footer shows has its label in the key registry, at every
/// kind of field and in either picker; never more than three.
#[test]
fn every_footer_hint_has_a_registry_label() {
    let mut m = modal_with_columns(&["id", "q1", "q2"]);
    let check = |m: &PivotMeltModal| {
        let keys = hints(m);
        assert!((2..=3).contains(&keys.len()), "{:?}", m.focus);
        for hint in keys {
            assert!(!hint.label.is_empty(), "{:?} {}", m.focus, hint.key);
        }
    };
    for focus in [
        PivotMeltFocus::TabBar,
        PivotMeltFocus::PivotIndex,
        PivotMeltFocus::PivotColumn,
        PivotMeltFocus::PivotAggregation,
    ] {
        m.focus = focus;
        check(&m);
        m.open_picker();
        check(&m);
        m.picker = None;
    }
    m.switch_tab();
    m.focus = PivotMeltFocus::MeltVariable;
    check(&m);
    assert!(question_types(&m), "a text field types ?");
}

#[test]
fn a_tiny_area_never_panics() {
    for (w, h) in [(0, 0), (3, 2), (10, 4), (20, 6), (50, 8), (60, 20), (97, 9)] {
        let mut m = previewed(long(), false, None);
        m.focus = PivotMeltFocus::PivotIndex;
        m.open_picker();
        let _ = render_rows(&mut m, w, h);
    }
}
