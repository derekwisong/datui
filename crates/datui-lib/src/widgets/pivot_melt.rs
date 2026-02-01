//! Pivot / Melt modal rendering.
//!
//! Phase 4: Pivot tab UI. Phase 5: Melt tab UI.

use crate::pivot_melt_modal::{PivotMeltFocus, PivotMeltModal, PivotMeltTab};
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, Paragraph, Row, StatefulWidget, Table, Tabs, Widget,
};

/// Render the Pivot and Melt modal: tab bar, tab-specific body, footer.
/// Uses `border_color` for default borders and `active_color` for focused elements.
/// `text_primary` and `text_inverse` are used for text-input cursor (same as query prompt).
pub fn render_shell(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut PivotMeltModal,
    border_color: Color,
    active_color: Color,
    text_primary: Color,
    text_inverse: Color,
) {
    Clear.render(area, buf);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color));
    let inner = block.inner(area);
    block.render(area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(10),
            Constraint::Length(3),
        ])
        .split(inner);

    // Tab bar (Ratatui Tabs widget): no frame/title, no divider, line below
    let tab_line_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(chunks[0]);
    let selected = match modal.active_tab {
        PivotMeltTab::Pivot => 0,
        PivotMeltTab::Melt => 1,
    };
    let tabs = Tabs::new(vec!["Pivot", "Melt"])
        .style(Style::default().fg(border_color))
        .highlight_style(
            Style::default()
                .fg(active_color)
                .add_modifier(Modifier::REVERSED),
        )
        .select(selected);
    tabs.render(tab_line_chunks[0], buf);
    let line_style = if modal.focus == PivotMeltFocus::TabBar {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    Block::default()
        .borders(Borders::BOTTOM)
        .border_style(line_style)
        .render(tab_line_chunks[1], buf);

    // Body
    match modal.active_tab {
        PivotMeltTab::Pivot => render_pivot_body(
            chunks[1],
            buf,
            modal,
            border_color,
            active_color,
            text_primary,
            text_inverse,
        ),
        PivotMeltTab::Melt => render_melt_body(
            chunks[1],
            buf,
            modal,
            border_color,
            active_color,
            text_primary,
            text_inverse,
        ),
    }

    // Footer (expand to fill horizontal space, like filter/sort dialogs)
    let footer_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(34),
        ])
        .split(chunks[2]);

    let apply_style = if modal.focus == PivotMeltFocus::Apply {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let cancel_style = if modal.focus == PivotMeltFocus::Cancel {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let clear_style = if modal.focus == PivotMeltFocus::Clear {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Apply")
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(apply_style),
        )
        .centered()
        .render(footer_chunks[0], buf);
    Paragraph::new("Cancel")
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(cancel_style),
        )
        .centered()
        .render(footer_chunks[1], buf);
    Paragraph::new("Clear")
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(clear_style),
        )
        .centered()
        .render(footer_chunks[2], buf);
}

fn render_pivot_body(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut PivotMeltModal,
    border_color: Color,
    active_color: Color,
    _text_primary: Color,
    _text_inverse: Color,
) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(5),
            Constraint::Length(5),
            Constraint::Length(4),
            Constraint::Length(4),
        ])
        .split(area);

    // Filter (1 line, no placeholder)
    let filter_style = if modal.focus == PivotMeltFocus::PivotFilter {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let filter_block = Block::default()
        .borders(Borders::ALL)
        .title("Filter Index Columns")
        .border_style(filter_style);
    let filter_inner = filter_block.inner(chunks[0]);
    filter_block.render(chunks[0], buf);

    // Render filter input using TextInput widget
    let is_focused = modal.focus == PivotMeltFocus::PivotFilter;
    modal.pivot_filter_input.set_focused(is_focused);
    (&modal.pivot_filter_input).render(filter_inner, buf);

    // Index list
    let list_style = if modal.focus == PivotMeltFocus::PivotIndexList {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let list_block = Block::default()
        .borders(Borders::ALL)
        .title("Index Columns")
        .border_style(list_style);
    let list_inner = list_block.inner(chunks[1]);
    list_block.render(chunks[1], buf);

    let filtered = modal.pivot_filtered_columns();
    if !filtered.is_empty() && modal.pivot_index_table.selected().is_none() {
        modal.pivot_index_table.select(Some(0));
    }
    let rows: Vec<Row> = filtered
        .iter()
        .map(|c| {
            let check = if modal.index_columns.contains(c) {
                "[x]"
            } else {
                "[ ]"
            };
            Row::new(vec![Cell::from(check), Cell::from(c.as_str())])
        })
        .collect();
    let widths = [Constraint::Length(4), Constraint::Min(10)];
    let table = Table::new(rows, widths)
        .column_spacing(1)
        .header(
            Row::new(vec!["", "Column"])
                .style(Style::default().add_modifier(Modifier::BOLD))
                .bottom_margin(0),
        )
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    StatefulWidget::render(table, list_inner, buf, &mut modal.pivot_index_table);

    // Pivot / Value: small tables (single-select lists)
    let pivot_style = if modal.focus == PivotMeltFocus::PivotPivotCol {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let value_style = if modal.focus == PivotMeltFocus::PivotValueCol {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let row_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[2]);

    let pivot_pool = modal.pivot_pool();
    if !pivot_pool.is_empty() {
        let n = pivot_pool.len();
        let idx = modal.pivot_pool_idx.min(n.saturating_sub(1));
        if modal.pivot_pool_idx != idx {
            modal.pivot_pool_idx = idx;
            modal.pivot_column = pivot_pool.get(idx).cloned();
        }
        modal.pivot_pool_table.select(Some(idx));
    }
    let pivot_rows: Vec<Row> = pivot_pool
        .iter()
        .map(|c| Row::new(vec![Cell::from(c.as_str())]))
        .collect();
    let pivot_block = Block::default()
        .borders(Borders::ALL)
        .title("Pivot Column")
        .border_style(pivot_style);
    let pivot_inner = pivot_block.inner(row_chunks[0]);
    pivot_block.render(row_chunks[0], buf);
    if pivot_rows.is_empty() {
        Paragraph::new("(none)").render(pivot_inner, buf);
    } else {
        let pt = Table::new(pivot_rows, [Constraint::Min(5)])
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        StatefulWidget::render(pt, pivot_inner, buf, &mut modal.pivot_pool_table);
    }

    let value_pool = modal.pivot_value_pool();
    if !value_pool.is_empty() {
        let n = value_pool.len();
        let idx = modal.value_pool_idx.min(n.saturating_sub(1));
        if modal.value_pool_idx != idx {
            modal.value_pool_idx = idx;
            modal.value_column = value_pool.get(idx).cloned();
        }
        modal.value_pool_table.select(Some(idx));
    }
    let value_rows: Vec<Row> = value_pool
        .iter()
        .map(|c| Row::new(vec![Cell::from(c.as_str())]))
        .collect();
    let value_block = Block::default()
        .borders(Borders::ALL)
        .title("Value Column")
        .border_style(value_style);
    let value_inner = value_block.inner(row_chunks[1]);
    value_block.render(row_chunks[1], buf);
    if value_rows.is_empty() {
        Paragraph::new("(none)").render(value_inner, buf);
    } else {
        let vt = Table::new(value_rows, [Constraint::Min(5)])
            .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
        StatefulWidget::render(vt, value_inner, buf, &mut modal.value_pool_table);
    }

    // Aggregation
    let agg_style = if modal.focus == PivotMeltFocus::PivotAggregation {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let opts = modal.pivot_aggregation_options();
    let agg_label = opts
        .get(modal.aggregation_idx)
        .map(|a| a.as_str())
        .unwrap_or("last");
    let agg_block = Block::default()
        .borders(Borders::ALL)
        .title("Aggregation")
        .border_style(agg_style);
    let agg_inner = agg_block.inner(chunks[3]);
    agg_block.render(chunks[3], buf);
    Paragraph::new(agg_label).render(agg_inner, buf);

    // Sort toggle
    let sort_style = if modal.focus == PivotMeltFocus::PivotSortToggle {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let sort_check = if modal.sort_new_columns { "[x]" } else { "[ ]" };
    let sort_block = Block::default()
        .borders(Borders::ALL)
        .title("Sort New Columns")
        .border_style(sort_style);
    let sort_inner = sort_block.inner(chunks[4]);
    sort_block.render(chunks[4], buf);
    Paragraph::new(format!("{} Sort", sort_check)).render(sort_inner, buf);
}

fn render_melt_body(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut PivotMeltModal,
    border_color: Color,
    active_color: Color,
    text_primary: Color,
    text_inverse: Color,
) {
    use crate::pivot_melt_modal::{MeltValueStrategy, PivotMeltFocus};

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(6),
            Constraint::Length(4),
            Constraint::Length(5),
            Constraint::Length(4),
        ])
        .split(area);

    // Filter (1 line, no placeholder)
    let filter_style = if modal.focus == PivotMeltFocus::MeltFilter {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let filter_block = Block::default()
        .borders(Borders::ALL)
        .title("Filter Index Columns")
        .border_style(filter_style);
    let filter_inner = filter_block.inner(chunks[0]);
    filter_block.render(chunks[0], buf);

    // Render filter input using TextInput widget
    let is_focused = modal.focus == PivotMeltFocus::MeltFilter;
    modal.melt_filter_input.set_focused(is_focused);
    (&modal.melt_filter_input).render(filter_inner, buf);

    // Index list
    let list_style = if modal.focus == PivotMeltFocus::MeltIndexList {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let list_block = Block::default()
        .borders(Borders::ALL)
        .title("Index Columns")
        .border_style(list_style);
    let list_inner = list_block.inner(chunks[1]);
    list_block.render(chunks[1], buf);

    let filtered = modal.melt_filtered_columns();
    if !filtered.is_empty() && modal.melt_index_table.selected().is_none() {
        modal.melt_index_table.select(Some(0));
    }
    let rows: Vec<Row> = filtered
        .iter()
        .map(|c| {
            let check = if modal.melt_index_columns.contains(c) {
                "[x]"
            } else {
                "[ ]"
            };
            Row::new(vec![Cell::from(check), Cell::from(c.as_str())])
        })
        .collect();
    let widths = [Constraint::Length(4), Constraint::Min(10)];
    let table = Table::new(rows, widths)
        .column_spacing(1)
        .header(
            Row::new(vec!["", "Column"])
                .style(Style::default().add_modifier(Modifier::BOLD))
                .bottom_margin(0),
        )
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    StatefulWidget::render(table, list_inner, buf, &mut modal.melt_index_table);

    // Strategy row
    let strat_style = if modal.focus == PivotMeltFocus::MeltStrategy {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let strat_block = Block::default()
        .borders(Borders::ALL)
        .title("Strategy")
        .border_style(strat_style);
    let strat_inner = strat_block.inner(chunks[2]);
    strat_block.render(chunks[2], buf);
    Paragraph::new(modal.melt_value_strategy.as_str()).render(strat_inner, buf);

    // Pattern / Type / Explicit
    let opt_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[3]);

    match modal.melt_value_strategy {
        MeltValueStrategy::ByPattern => {
            let pat_style = if modal.focus == PivotMeltFocus::MeltPattern {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            let pat_block = Block::default()
                .borders(Borders::ALL)
                .title("Pattern (Regex)")
                .border_style(pat_style);
            let pat_inner = pat_block.inner(opt_chunks[0]);
            pat_block.render(opt_chunks[0], buf);
            let pt = modal.melt_pattern.as_str();
            let pc = modal.melt_pattern_cursor.min(pt.chars().count());
            let mut ch = pt.chars();
            let b: String = ch.by_ref().take(pc).collect();
            let a = ch
                .next()
                .map(|c| c.to_string())
                .unwrap_or_else(|| " ".to_string());
            let af: String = ch.collect();
            let mut pl = Line::default();
            pl.spans.push(Span::raw(b));
            if modal.focus == PivotMeltFocus::MeltPattern {
                pl.spans.push(Span::styled(
                    a,
                    Style::default().bg(text_inverse).fg(text_primary),
                ));
            } else {
                pl.spans.push(Span::raw(a));
            }
            if !af.is_empty() {
                pl.spans.push(Span::raw(af));
            }
            Paragraph::new(pl).render(pat_inner, buf);
        }
        MeltValueStrategy::ByType => {
            let ty_style = if modal.focus == PivotMeltFocus::MeltType {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            let ty_block = Block::default()
                .borders(Borders::ALL)
                .title("Type")
                .border_style(ty_style);
            let ty_inner = ty_block.inner(opt_chunks[0]);
            ty_block.render(opt_chunks[0], buf);
            Paragraph::new(modal.melt_type_filter.as_str()).render(ty_inner, buf);
        }
        MeltValueStrategy::ExplicitList => {
            let ex_style = if modal.focus == PivotMeltFocus::MeltExplicitList {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            let ex_block = Block::default()
                .borders(Borders::ALL)
                .title("Value Columns")
                .border_style(ex_style);
            let ex_inner = ex_block.inner(chunks[3]);
            ex_block.render(chunks[3], buf);
            let pool = modal.melt_explicit_pool();
            if !pool.is_empty() && modal.melt_explicit_table.selected().is_none() {
                modal.melt_explicit_table.select(Some(0));
            }
            let ex_rows: Vec<Row> = pool
                .iter()
                .map(|c| {
                    let check = if modal.melt_explicit_list.contains(c) {
                        "[x]"
                    } else {
                        "[ ]"
                    };
                    Row::new(vec![Cell::from(check), Cell::from(c.as_str())])
                })
                .collect();
            let ew = [Constraint::Length(4), Constraint::Min(10)];
            let ex_table = Table::new(ex_rows, ew)
                .column_spacing(1)
                .header(
                    Row::new(vec!["", "Column"])
                        .style(Style::default().add_modifier(Modifier::BOLD))
                        .bottom_margin(0),
                )
                .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));
            StatefulWidget::render(ex_table, ex_inner, buf, &mut modal.melt_explicit_table);
        }
        MeltValueStrategy::AllExceptIndex => {}
    }

    // Variable name / Value name with cursor
    let var_style = if modal.focus == PivotMeltFocus::MeltVarName {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let val_style = if modal.focus == PivotMeltFocus::MeltValName {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let vchunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(chunks[4]);
    let var_block = Block::default()
        .borders(Borders::ALL)
        .title("Variable Name")
        .border_style(var_style);
    let var_inner = var_block.inner(vchunks[0]);
    var_block.render(vchunks[0], buf);
    let vn = modal.melt_variable_name.as_str();
    let vc = modal.melt_variable_cursor.min(vn.chars().count());
    let mut ch = vn.chars();
    let vb: String = ch.by_ref().take(vc).collect();
    let va = ch
        .next()
        .map(|c| c.to_string())
        .unwrap_or_else(|| " ".to_string());
    let vaf: String = ch.collect();
    let mut vl = Line::default();
    vl.spans.push(Span::raw(vb));
    if modal.focus == PivotMeltFocus::MeltVarName {
        vl.spans.push(Span::styled(
            va,
            Style::default().bg(text_inverse).fg(text_primary),
        ));
    } else {
        vl.spans.push(Span::raw(va));
    }
    if !vaf.is_empty() {
        vl.spans.push(Span::raw(vaf));
    }
    Paragraph::new(vl).render(var_inner, buf);

    let val_block = Block::default()
        .borders(Borders::ALL)
        .title("Value Name")
        .border_style(val_style);
    let val_inner = val_block.inner(vchunks[1]);
    val_block.render(vchunks[1], buf);
    let wn = modal.melt_value_name.as_str();
    let wc = modal.melt_value_cursor.min(wn.chars().count());
    let mut ch = wn.chars();
    let wb: String = ch.by_ref().take(wc).collect();
    let wa = ch
        .next()
        .map(|c| c.to_string())
        .unwrap_or_else(|| " ".to_string());
    let waf: String = ch.collect();
    let mut wl = Line::default();
    wl.spans.push(Span::raw(wb));
    if modal.focus == PivotMeltFocus::MeltValName {
        wl.spans.push(Span::styled(
            wa,
            Style::default().bg(text_inverse).fg(text_primary),
        ));
    } else {
        wl.spans.push(Span::raw(wa));
    }
    if !waf.is_empty() {
        wl.spans.push(Span::raw(waf));
    }
    Paragraph::new(wl).render(val_inner, buf);
}
