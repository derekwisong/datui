//! Pivot / Melt sidebar rendering: one Surface, a FormRow per field, the one
//! Picker below the rows for whichever row is being edited, and the staged
//! spec echoed live above the footer.

use crate::pivot_melt_modal::{PivotMeltFocus, PivotMeltModal, PivotMeltTab};
use crate::render::context::RenderContext;
use crate::widgets::ui::{FormRow, FormValue, HintBar, Picker, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// Where the value column starts, past the rail gutter: the longest label,
/// "Variable name:", plus two cells of air.
const LABEL_WIDTH: u16 = 16;

fn row_label(focus: PivotMeltFocus) -> &'static str {
    match focus {
        PivotMeltFocus::PivotIndex | PivotMeltFocus::MeltIndex => "Index:",
        PivotMeltFocus::PivotColumn | PivotMeltFocus::MeltColumns => "Columns:",
        PivotMeltFocus::PivotValue => "Values:",
        PivotMeltFocus::PivotAggregation => "Aggregate:",
        PivotMeltFocus::MeltStrategy => "Strategy:",
        PivotMeltFocus::MeltPattern => "Pattern:",
        PivotMeltFocus::MeltType => "Type:",
        PivotMeltFocus::MeltVariable => "Variable name:",
        PivotMeltFocus::MeltValue => "Value name:",
        PivotMeltFocus::TabBar => "",
    }
}

/// Render the Pivot & Melt sidebar into the given area.
pub fn render(area: Rect, buf: &mut Buffer, modal: &mut PivotMeltModal, ctx: &RenderContext) {
    // The footer names what the keys do right now: editing a row through the
    // Picker, or walking and applying the form.
    let footer = match &modal.picker {
        Some(_) if modal.is_multi_row(modal.focus) => HintBar::from_ctx(ctx)
            .hint_weighted("Space", "Toggle", 3)
            .hint_weighted("Enter", "Done", 2)
            .hint_weighted("type", "Narrow", 1)
            .hint_weighted("Esc", "Back", 4),
        Some(_) => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Choose", 3)
            .hint_weighted("type", "Narrow", 1)
            .hint_weighted("Esc", "Back", 4),
        None if modal.is_picker_row(modal.focus) => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Apply", 3)
            .hint_weighted("Space", "Edit", 2)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4),
        None => HintBar::from_ctx(ctx)
            .hint_weighted("Enter", "Apply", 3)
            .hint_weighted("Tab", "Next", 1)
            .hint_weighted("Esc", "Cancel", 4),
    };
    let content = Surface::new("Pivot & Melt")
        .footer(&footer)
        .render(area, buf, ctx);
    if content.height < 4 || content.width < 10 {
        return;
    }

    // Tab line: the active tab carries the accent, and the rail sits beside
    // its name while the tab bar holds focus — never beside a tab the
    // surface is not on. The slot is reserved either way, so nothing moves.
    let g = crate::glyphs::get();
    let pivot_tab = modal.active_tab == PivotMeltTab::Pivot;
    let on_tab_bar = modal.focus == PivotMeltFocus::TabBar;
    let mark = |active: bool| {
        if on_tab_bar && active { g.rail } else { " " }
    };
    let tab_style = |active: bool| {
        if active {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_secondary)
        }
    };
    let tab_line = Line::from(vec![
        Span::styled(mark(pivot_tab), Style::default().fg(ctx.accent)),
        Span::styled("Pivot", tab_style(pivot_tab)),
        Span::styled(format!(" {}", g.rule), Style::default().fg(ctx.dimmed)),
        Span::styled(mark(!pivot_tab), Style::default().fg(ctx.accent)),
        Span::styled("Melt", tab_style(!pivot_tab)),
    ]);
    Paragraph::new(tab_line).render(
        Rect {
            height: 1,
            ..content
        },
        buf,
    );

    // The spec line sits on the last content row, directly above the footer.
    let spec_y = content.y + content.height - 1;

    modal
        .melt_pattern_input
        .set_focused(modal.focus == PivotMeltFocus::MeltPattern);
    modal
        .melt_variable_input
        .set_focused(modal.focus == PivotMeltFocus::MeltVariable);
    modal
        .melt_value_input
        .set_focused(modal.focus == PivotMeltFocus::MeltValue);

    // One FormRow per field; the chosen value is always echoed on the row, so
    // nothing is ambiguous when focus is elsewhere.
    let rows = modal.row_order();
    let mut y = content.y + 2;
    for &row in rows {
        if y >= spec_y {
            break;
        }
        let index_echo;
        let value = match row {
            PivotMeltFocus::PivotIndex => {
                index_echo = modal.index_columns.join(", ");
                echo_or_placeholder(&index_echo, "none")
            }
            PivotMeltFocus::PivotColumn => {
                echo_or_placeholder(modal.pivot_column.as_deref().unwrap_or(""), "none")
            }
            PivotMeltFocus::PivotValue => {
                echo_or_placeholder(modal.value_column.as_deref().unwrap_or(""), "none")
            }
            PivotMeltFocus::PivotAggregation => FormValue::Choice(modal.aggregation.as_str()),
            PivotMeltFocus::MeltIndex => {
                index_echo = modal.melt_index_columns.join(", ");
                echo_or_placeholder(&index_echo, "none")
            }
            PivotMeltFocus::MeltStrategy => FormValue::Choice(modal.melt_value_strategy.as_str()),
            PivotMeltFocus::MeltPattern => FormValue::Input(&modal.melt_pattern_input),
            PivotMeltFocus::MeltType => FormValue::Choice(modal.melt_type_filter.as_str()),
            PivotMeltFocus::MeltColumns => {
                index_echo = modal.melt_explicit_list.join(", ");
                echo_or_placeholder(&index_echo, "none")
            }
            PivotMeltFocus::MeltVariable => FormValue::Input(&modal.melt_variable_input),
            PivotMeltFocus::MeltValue => FormValue::Input(&modal.melt_value_input),
            PivotMeltFocus::TabBar => continue,
        };
        FormRow {
            label: row_label(row),
            value,
            focused: modal.focus == row,
            label_width: LABEL_WIDTH,
        }
        .render(
            Rect {
                y,
                height: 1,
                ..content
            },
            buf,
            ctx,
        );
        y += 1;
    }

    // The focused row's Picker drops in below the rows and reaches down to
    // the spec line; the selection carries the rail while the list is up.
    if let Some(state) = &modal.picker {
        let picker_y = y + 1;
        if picker_y < spec_y {
            let picker_area = Rect {
                x: content.x + 2,
                y: picker_y,
                width: content.width.saturating_sub(2),
                height: spec_y - picker_y,
            };
            let mut picker = Picker::from_state(state, true);
            if modal.is_multi_row(modal.focus) {
                let marks = state
                    .filtered()
                    .into_iter()
                    .map(|(_, item)| modal.is_marked(item))
                    .collect();
                picker = picker.marks(marks);
            }
            picker.render(picker_area, buf, ctx);
        }
    }

    // The full spec, echoed live: a mis-aimed aggregation is catchable here,
    // before it runs. While the spec is incomplete, the line says what is
    // missing instead.
    let spec = match modal.active_tab {
        PivotMeltTab::Pivot => modal.pivot_spec_line(g),
        PivotMeltTab::Melt => modal.melt_spec_line(),
    };
    let (text, style) = match spec {
        Ok(line) => (line, Style::default().fg(ctx.text_primary)),
        // Enter on the incomplete form lit the line up; edits dim it again.
        Err(gap) if modal.attention => (gap, Style::default().fg(ctx.warning)),
        Err(gap) => (gap, Style::default().fg(ctx.dimmed)),
    };
    Paragraph::new(text).style(style).render(
        Rect {
            y: spec_y,
            height: 1,
            ..content
        },
        buf,
    );
}

fn echo_or_placeholder<'a>(value: &'a str, placeholder: &'a str) -> FormValue<'a> {
    if value.is_empty() {
        FormValue::Placeholder(placeholder)
    } else {
        FormValue::Choice(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pivot_melt_modal::PivotAggregation;

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

    /// One border, the tab line, a row per field with the value echoed at the
    /// shared column, and the spec line above the footer.
    #[test]
    fn the_pivot_form_echoes_every_choice_and_the_spec() {
        let g = crate::glyphs::get();
        let mut m = modal_with_columns(&["dept", "job", "salary"]);
        m.index_columns = vec!["dept".to_string()];
        m.pivot_column = Some("job".to_string());
        m.value_column = Some("salary".to_string());
        m.aggregation = PivotAggregation::Avg;
        let rows = render_rows(&mut m, 50, 20);

        assert!(rows[0].contains("Pivot & Melt"), "title on the frame");
        for row in &rows[1..19] {
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
        // Values align on one column: past the rail gutter and the label.
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
        assert!(
            rows[17].contains(&spec),
            "the spec line above the footer: {:?}",
            rows[17]
        );
        assert!(
            rows[18].contains("Enter") && rows[18].contains("Apply"),
            "the footer chips: {:?}",
            rows[18]
        );
    }

    /// Before anything is chosen the rows say "none" and the spec line names
    /// the first gap instead of echoing a spec.
    #[test]
    fn an_empty_form_shows_placeholders_and_the_gap() {
        let mut m = modal_with_columns(&["a", "b"]);
        let rows = render_rows(&mut m, 50, 20);
        assert!(rows[3].contains("none"), "empty index: {:?}", rows[3]);
        assert!(
            rows[17].contains("Select at least one index column."),
            "the gap is named: {:?}",
            rows[17]
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
        let rows = render_rows(&mut m, 50, 20);
        let body = rows.join("\n");
        assert!(
            body.contains(&format!("{} dept", g.checkbox_on)),
            "chosen item checked: {body}"
        );
        assert!(
            body.contains(&format!("{} region", g.checkbox_off)),
            "other items unchecked: {body}"
        );
        assert!(
            rows[18].contains("Space") && rows[18].contains("Toggle"),
            "the footer says Space toggles: {:?}",
            rows[18]
        );
    }

    /// The melt form swaps its strategy row's dependents in place.
    #[test]
    fn the_melt_form_follows_the_strategy() {
        let mut m = modal_with_columns(&["id", "q1", "q2"]);
        m.switch_tab();
        m.melt_value_strategy = crate::pivot_melt_modal::MeltValueStrategy::ByPattern;
        let rows = render_rows(&mut m, 50, 20);
        assert!(rows[5].contains("Pattern:"), "got {:?}", rows[5]);
        assert!(rows[6].contains("Variable name:"), "got {:?}", rows[6]);
        assert!(rows[7].contains("Value name:") && rows[7].contains("value"));
    }

    #[test]
    fn a_tiny_area_never_panics() {
        for (w, h) in [(0, 0), (3, 2), (10, 4), (20, 6), (50, 8), (60, 20)] {
            let mut m = modal_with_columns(&["a", "b"]);
            m.focus = PivotMeltFocus::PivotIndex;
            m.open_picker();
            let _ = render_rows(&mut m, w, h);
        }
    }
}
