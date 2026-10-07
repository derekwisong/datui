use super::*;

fn render_row(row: &FormRow, width: u16) -> (String, Buffer) {
    let ctx = RenderContext::for_test();
    let area = Rect::new(0, 0, width, 1);
    let mut buf = Buffer::empty(area);
    row.render(area, &mut buf, &ctx);
    let text = (0..width)
        .map(|x| buf[(x, 0)].symbol().to_string())
        .collect();
    (text, buf)
}

#[test]
fn the_value_is_echoed_at_the_shared_column() {
    let row = FormRow {
        label: "Compression:",
        value: FormValue::Choice("Gzip"),
        focused: false,
        label_width: 17,
    };
    let (text, _) = render_row(&row, 40);
    // One rail-gutter column, then the label, then the value column.
    assert_eq!(text.find("Gzip"), Some(18), "got {text:?}");
}

#[test]
fn a_toggle_draws_the_checkbox_glyph() {
    let g = crate::glyphs::get();
    for (on, marker) in [(true, g.checkbox_on), (false, g.checkbox_off)] {
        let row = FormRow {
            label: "Include header:",
            value: FormValue::Toggle(on),
            focused: false,
            label_width: 17,
        };
        let (text, _) = render_row(&row, 40);
        assert!(text.contains(marker), "expected {marker:?} in {text:?}");
    }
}

/// With its picker open the row gives the rail to the picker's line: one
/// rail on screen. The label keeps the accent, naming what is being picked.
#[test]
fn an_open_picker_takes_the_rail() {
    let g = crate::glyphs::get();
    let ctx = RenderContext::for_test();
    let row = FormRow {
        label: "Index:",
        value: FormValue::Choice("dept"),
        focused: true,
        label_width: 10,
    };
    let area = Rect::new(0, 0, 30, 1);
    let mut buf = Buffer::empty(area);
    row.render_picking(area, &mut buf, &ctx, true);
    assert_ne!(buf[(0, 0)].symbol(), g.rail);
    assert_eq!(buf[(1, 0)].fg, ctx.accent, "the label keeps the accent");
    let (text, _) = render_row(&row, 30);
    assert!(
        text.starts_with(g.rail),
        "closed, the rail is back: {text:?}"
    );
}

/// Focus is the rail and the accent, never a layout change: the gutter is
/// reserved, so the label and value sit still while the rail arrives.
#[test]
fn focus_brings_the_rail_and_moves_nothing() {
    let g = crate::glyphs::get();
    let make = |focused| FormRow {
        label: "Path:",
        value: FormValue::Choice("out.csv"),
        focused,
        label_width: 17,
    };
    let (plain_text, plain) = render_row(&make(false), 40);
    let (focused_text, focused) = render_row(&make(true), 40);
    assert!(plain_text.starts_with(' '), "the gutter is reserved");
    assert!(
        focused_text.starts_with(g.rail),
        "the focused row is marked"
    );
    assert!(
        plain_text.chars().skip(1).eq(focused_text.chars().skip(1)),
        "past the rail, focus changes no text: {plain_text:?} vs {focused_text:?}"
    );
    let changed: Vec<u16> = (0..40)
        .filter(|&x| plain[(x, 0)].fg != focused[(x, 0)].fg)
        .collect();
    assert!(!changed.is_empty(), "focus is invisible");
    assert!(
        changed.iter().all(|&x| x < 18),
        "focus colored the value, not just the rail and label: {changed:?}"
    );
}

#[test]
fn an_options_row_never_panics_and_falls_back_when_narrow() {
    let g = crate::glyphs::get();
    let items = ["CSV", "TSV", "Parquet"];
    for width in 0..40 {
        for label_width in [0, 5, 9] {
            let row = FormRow {
                label: "Format:",
                value: FormValue::Options {
                    items: &items,
                    selected: 2,
                    clicks: None,
                },
                focused: true,
                label_width,
            };
            let (text, _) = render_row(&row, width);
            if width >= 30 && label_width == 9 {
                assert!(text.contains(" CSV  TSV  Parquet "), "{text:?}");
            } else if width == 20 && label_width == 9 {
                let compact = format!("{} Parquet {}", g.choice_prev, g.choice_next);
                assert!(text.contains(&compact), "{text:?}");
            }
        }
    }
}

#[test]
fn a_narrow_row_never_panics() {
    for width in 0..20 {
        let row = FormRow {
            label: "Include header:",
            value: FormValue::Toggle(true),
            focused: true,
            label_width: 17,
        };
        let _ = render_row(&row, width);
    }
}
