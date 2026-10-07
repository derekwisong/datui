use super::*;
use crate::view::{MatchCriteria, MatchReason, SavedView, ViewSettings};
use crate::widgets::view_modal::ViewRow;
use std::time::SystemTime;

fn a_view(name: &str, description: Option<&str>) -> SavedView {
    SavedView {
        id: name.to_string(),
        name: name.to_string(),
        description: description.map(str::to_string),
        created: SystemTime::now(),
        last_used: None,
        usage_count: 0,
        last_matched_file: None,
        match_criteria: MatchCriteria {
            exact_path: None,
            relative_path: None,
            path_pattern: None,
            filename_pattern: None,
            schema_columns: None,
            schema_types: None,
            table: None,
        },
        settings: ViewSettings {
            chart: None,
            sample: None,
            query: None,
            sql_query: None,
            fuzzy_query: None,
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_descending: Vec::new(),
            sort_ascending: true,
            column_order: Vec::new(),
            locked_columns_count: 0,
            pivot: None,
            melt: None,
            reshape_source: None,
            columns: Vec::new(),
        },
    }
}

fn list_modal() -> ViewModal {
    let mut modal = ViewModal::new();
    modal.rows = vec![
        ViewRow {
            view: a_view("salary review", Some("Sorted by salary")),
            score: 2000.0,
            reason: Some(MatchReason::SameFile),
        },
        ViewRow {
            view: a_view("quarterly report", None),
            score: 40.0,
            reason: None,
        },
    ];
    modal.table_state.select(Some(0));
    modal
}

fn render_to_rows(modal: &mut ViewModal, width: u16, height: u16) -> Vec<String> {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    render(area, &mut buf, modal, Some("salary review"), &ctx);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

/// With nothing saved the footer offers only what acts, and a status or
/// header too long for the sidebar is cut with a mark, never silently.
#[test]
fn the_list_offers_only_what_acts_and_cuts_with_a_mark() {
    let g = crate::glyphs::get();
    let mut modal = ViewModal::new();
    let rows = render_to_rows(&mut modal, 40, 12);
    let text = rows.join("\n");
    assert!(!text.contains("Apply"), "{text}");
    assert!(!text.contains("Delete"), "{text}");
    assert!(text.contains("Save") && text.contains("Close"), "{text}");

    let mut modal = list_modal();
    modal.status = Some(
        "Nothing to save yet: set a query, filter, sort, column layout, or pivot/melt first."
            .into(),
    );
    let rows = render_to_rows(&mut modal, 40, 12);
    let status = rows
        .iter()
        .find(|r| r.contains("Nothing to save"))
        .expect("the status line");
    assert!(status.contains(g.ellipsis), "{status:?}");
    let header = rows
        .iter()
        .find(|r| r.contains("Name"))
        .expect("the header");
    assert!(header.contains(g.ellipsis), "{header:?}");
    assert!(rows.iter().any(|r| r.contains("Apply")));
}

/// One border, the rows inside it, the reason annotation on the row whose
/// criteria fit, and the footer chips on the last inner row.
#[test]
fn the_list_is_one_surface_with_annotated_rows() {
    let mut modal = list_modal();
    let rows = render_to_rows(&mut modal, 80, 16);
    assert!(
        rows[0].contains("Views"),
        "title on the frame: {:?}",
        rows[0]
    );
    for row in &rows[1..15] {
        assert!(
            !row.contains('╭') && !row.contains('╰'),
            "a second border inside the surface: {row:?}"
        );
    }
    let g = crate::glyphs::get();
    let cursor_row = rows
        .iter()
        .find(|r| r.contains("salary review"))
        .expect("the view is listed");
    // Past the frame and its gutter, the row starts with the rail.
    assert_eq!(
        cursor_row.chars().nth(2).map(String::from).as_deref(),
        Some(g.rail),
        "the cursor row carries the rail: {cursor_row:?}"
    );
    assert!(
        cursor_row.contains("same file"),
        "the matching row says why: {cursor_row:?}"
    );
    assert!(cursor_row.contains(g.check), "the active view is checked");
    let other_row = rows
        .iter()
        .find(|r| r.contains("quarterly report"))
        .expect("the second view is listed");
    assert!(
        !other_row.contains("same file")
            && !other_row.contains("same columns")
            && !other_row.contains("glob"),
        "a view that fits nothing carries no annotation: {other_row:?}"
    );
    let footer = &rows[14];
    for chip in ["Apply", "Save", "Edit", "Delete", "Score", "Close"] {
        assert!(footer.contains(chip), "footer misses {chip}: {footer:?}");
    }
}

/// The form is name-first with the criteria folded away; expanding walks
/// them in, collapsed they are absent.
#[test]
fn the_form_collapses_the_matching_section() {
    let mut modal = list_modal();
    modal.mode = ViewModalMode::Create;
    modal.name_input.set_value("salaries");
    modal.schema_match_enabled = true;

    let rows = render_to_rows(&mut modal, 80, 20);
    assert!(rows[0].contains("Save View"), "got {:?}", rows[0]);
    let text = rows.join("\n");
    assert!(text.contains("Name:"));
    assert!(text.contains("Matching"));
    assert!(text.contains("1 rule"), "the chip counts the set criteria");
    assert!(
        !text.contains("Exact path:"),
        "collapsed criteria stay hidden"
    );

    modal.matching_expanded = true;
    let text = render_to_rows(&mut modal, 80, 20).join("\n");
    for label in [
        "Exact path:",
        "Relative path:",
        "Path pattern:",
        "Filename pattern:",
        "Schema match:",
    ] {
        assert!(text.contains(label), "expanded form misses {label}");
    }
    assert!(!text.contains("Table:"), "no table, no row");

    // A view of one table of a file echoes the table it is for.
    modal.table = Some("orders".to_string());
    let text = render_to_rows(&mut modal, 80, 20).join("\n");
    assert!(text.contains("2 rules"), "the table counts as a rule");
    let table_row = text
        .lines()
        .find(|line| line.contains("Table:"))
        .expect("the table row");
    assert!(table_row.contains("orders"), "{table_row:?}");
}

#[test]
fn a_tiny_area_never_panics() {
    for (w, h) in [(0, 0), (5, 3), (12, 4), (30, 6)] {
        let mut modal = list_modal();
        render_to_rows(&mut modal, w, h);
        modal.mode = ViewModalMode::Create;
        modal.matching_expanded = true;
        render_to_rows(&mut modal, w, h);
    }
}
