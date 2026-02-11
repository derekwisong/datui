//! Sort & Filter sidebar rendering.

use crate::filter_modal::{FilterFocus, FilterOperator, LogicalOperator};
use crate::render::context::RenderContext;
use crate::sort_filter_modal::{SortFilterFocus, SortFilterModal, SortFilterTab};
use crate::sort_modal::SortFocus;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::{StatefulWidget, Widget};
use ratatui::style::{Modifier, Style};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, List, ListItem, Paragraph, Row, Table, Tabs,
};

/// Render the Sort & Filter sidebar into the given area.
pub fn render(area: Rect, buf: &mut Buffer, modal: &mut SortFilterModal, ctx: &RenderContext) {
    let border_c = ctx.modal_border;
    let active_c = ctx.modal_border_active;

    Clear.render(area, buf);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Sort & Filter");
    let inner_area = block.inner(area);
    block.render(area, buf);

    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Min(0),
            Constraint::Length(3),
        ])
        .split(inner_area);

    let tab_line_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(1), Constraint::Length(1)])
        .split(chunks[0]);
    let tab_selected = match modal.active_tab {
        SortFilterTab::Sort => 0,
        SortFilterTab::Filter => 1,
    };
    let tabs = Tabs::new(vec!["Sort", "Filter"])
        .style(Style::default().fg(border_c))
        .highlight_style(
            Style::default()
                .fg(active_c)
                .add_modifier(Modifier::REVERSED),
        )
        .select(tab_selected);
    tabs.render(tab_line_chunks[0], buf);
    let line_style = if modal.focus == SortFilterFocus::TabBar {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Block::default()
        .borders(Borders::BOTTOM)
        .border_type(BorderType::Rounded)
        .border_style(line_style)
        .render(tab_line_chunks[1], buf);

    if modal.active_tab == SortFilterTab::Filter {
        render_filter_tab(&mut modal.filter, chunks[1], buf, border_c, active_c, ctx);
    } else {
        render_sort_tab(modal, chunks[1], buf, border_c, active_c, ctx);
    }

    let footer_chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(34),
        ])
        .split(chunks[2]);

    let mut apply_text_style = Style::default();
    let mut apply_border_style = Style::default();
    if modal.focus == SortFilterFocus::Apply {
        apply_text_style = apply_text_style.fg(active_c);
        apply_border_style = apply_border_style.fg(active_c);
    } else {
        apply_text_style = apply_text_style.fg(border_c);
        apply_border_style = apply_border_style.fg(border_c);
    }
    if modal.active_tab == SortFilterTab::Sort && modal.sort.has_unapplied_changes {
        apply_text_style = apply_text_style.add_modifier(Modifier::BOLD);
    }

    Paragraph::new("Apply")
        .style(apply_text_style)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(apply_border_style),
        )
        .centered()
        .render(footer_chunks[0], buf);

    let cancel_style = if modal.focus == SortFilterFocus::Cancel {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new("Cancel")
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(cancel_style),
        )
        .centered()
        .render(footer_chunks[1], buf);

    let clear_style = if modal.focus == SortFilterFocus::Clear {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new("Clear")
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(clear_style),
        )
        .centered()
        .render(footer_chunks[2], buf);
}

fn render_filter_tab(
    filter: &mut crate::filter_modal::FilterModal,
    area: Rect,
    buf: &mut Buffer,
    border_c: ratatui::style::Color,
    active_c: ratatui::style::Color,
    _ctx: &RenderContext,
) {
    let fchunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Length(3),
            Constraint::Min(0),
        ])
        .split(area);

    let row_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(30),
            Constraint::Percentage(20),
            Constraint::Percentage(30),
            Constraint::Percentage(20),
        ])
        .split(fchunks[0]);

    let col_name = if filter.available_columns.is_empty() {
        ""
    } else {
        &filter.available_columns[filter.new_column_idx]
    };
    let col_style = if filter.focus == FilterFocus::Column {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new(col_name)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Col")
                .border_style(col_style),
        )
        .render(row_layout[0], buf);

    let op_name = FilterOperator::iterator()
        .nth(filter.new_operator_idx)
        .unwrap_or(FilterOperator::Eq)
        .as_str();
    let op_style = if filter.focus == FilterFocus::Operator {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new(op_name)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Op")
                .border_style(op_style),
        )
        .render(row_layout[1], buf);

    let val_style = if filter.focus == FilterFocus::Value {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new(filter.new_value.as_str())
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Val")
                .border_style(val_style),
        )
        .render(row_layout[2], buf);

    let log_name = LogicalOperator::iterator()
        .nth(filter.new_logical_idx)
        .unwrap_or(LogicalOperator::And)
        .as_str();
    let log_style = if filter.focus == FilterFocus::Logical {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new(log_name)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Logic")
                .border_style(log_style),
        )
        .render(row_layout[3], buf);

    let add_style = if filter.focus == FilterFocus::Add {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    Paragraph::new("Add Filter")
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(add_style),
        )
        .centered()
        .render(fchunks[1], buf);

    let items: Vec<ListItem> = filter
        .statements
        .iter()
        .enumerate()
        .map(|(i, s)| {
            let prefix = if i > 0 {
                format!("{} ", s.logical_op.as_str())
            } else {
                "".to_string()
            };
            ListItem::new(format!(
                "{}{}{}{}",
                prefix,
                s.column,
                s.operator.as_str(),
                s.value
            ))
        })
        .collect();
    let list_style = if filter.focus == FilterFocus::Statements {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    let list = List::new(items)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .title("Current Filters")
                .border_style(list_style),
        )
        .highlight_style(Style::default().add_modifier(Modifier::REVERSED));
    StatefulWidget::render(list, fchunks[2], buf, &mut filter.list_state);
}

fn render_sort_tab(
    modal: &mut SortFilterModal,
    area: Rect,
    buf: &mut Buffer,
    border_c: ratatui::style::Color,
    active_c: ratatui::style::Color,
    ctx: &RenderContext,
) {
    let schunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(0),
            Constraint::Length(2),
            Constraint::Length(3),
        ])
        .split(area);

    let filter_block_title = "Filter Columns";
    let mut filter_block_border_style = Style::default().fg(border_c);
    if modal.sort.focus == SortFocus::Filter {
        filter_block_border_style = filter_block_border_style.fg(active_c);
    }
    let filter_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title(filter_block_title)
        .border_style(filter_block_border_style);
    let filter_inner_area = filter_block.inner(schunks[0]);
    filter_block.render(schunks[0], buf);

    let is_focused = modal.sort.focus == SortFocus::Filter;
    modal.sort.filter_input.set_focused(is_focused);
    (&modal.sort.filter_input).render(filter_inner_area, buf);

    let filtered = modal.sort.filtered_columns();
    let rows: Vec<Row> = filtered
        .iter()
        .map(|(_, col)| {
            let lock_cell = if col.is_locked {
                "●"
            } else if col.is_to_be_locked {
                "◐"
            } else {
                " "
            };
            let lock_style = if col.is_locked {
                Style::default()
            } else if col.is_to_be_locked {
                Style::default().fg(ctx.dimmed)
            } else {
                Style::default()
            };
            let order_cell = if col.is_visible && col.display_order < 9999 {
                format!("{:2}", col.display_order + 1)
            } else {
                "  ".to_string()
            };
            let sort_cell = if let Some(order) = col.sort_order {
                format!("{:2}", order)
            } else {
                "  ".to_string()
            };
            let name_cell = Cell::from(col.name.clone());

            let row_style = if col.is_visible {
                Style::default()
            } else {
                Style::default().fg(ctx.dimmed)
            };

            Row::new(vec![
                Cell::from(lock_cell).style(lock_style),
                Cell::from(order_cell).style(row_style),
                Cell::from(sort_cell).style(row_style),
                name_cell.style(row_style),
            ])
        })
        .collect();

    let header = Row::new(vec![
        Cell::from("🔒").style(Style::default()),
        Cell::from("Order").style(Style::default()),
        Cell::from("Sort").style(Style::default()),
        Cell::from("Name").style(Style::default()),
    ])
    .style(Style::default().add_modifier(Modifier::UNDERLINED));

    let table_border_style = if modal.sort.focus == SortFocus::ColumnList {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };
    let table = Table::new(
        rows,
        [
            Constraint::Length(2),
            Constraint::Length(6),
            Constraint::Length(6),
            Constraint::Min(0),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title("Columns")
            .border_style(table_border_style),
    )
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    StatefulWidget::render(table, schunks[1], buf, &mut modal.sort.table_state);

    use ratatui::text::{Line, Span};
    let mut hint_line1 = Line::default();
    hint_line1.spans.push(Span::raw("Sort:    "));
    hint_line1.spans.push(Span::styled(
        "Space",
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    ));
    hint_line1.spans.push(Span::raw(" Toggle "));
    hint_line1.spans.push(Span::styled(
        "[]",
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    ));
    hint_line1.spans.push(Span::raw(" Reorder "));
    hint_line1.spans.push(Span::styled(
        "1-9",
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    ));
    hint_line1.spans.push(Span::raw(" Jump"));

    let mut hint_line2 = Line::default();
    hint_line2.spans.push(Span::raw("Display: "));
    hint_line2.spans.push(Span::styled(
        "L",
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    ));
    hint_line2.spans.push(Span::raw(" Lock "));
    hint_line2.spans.push(Span::styled(
        "+-",
        Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD),
    ));
    hint_line2.spans.push(Span::raw(" Reorder"));

    Paragraph::new(vec![hint_line1, hint_line2]).render(schunks[2], buf);

    let order_border_style = if modal.sort.focus == SortFocus::Order {
        Style::default().fg(active_c)
    } else {
        Style::default().fg(border_c)
    };

    let order_block = Block::default()
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .title("Order")
        .border_style(order_border_style);
    let order_inner = order_block.inner(schunks[3]);
    order_block.render(schunks[3], buf);

    let order_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(order_inner);

    let ascending_indicator = if modal.sort.ascending { "●" } else { "○" };
    let ascending_text = format!("{} Ascending", ascending_indicator);
    let ascending_style = if modal.sort.ascending {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Paragraph::new(ascending_text)
        .style(ascending_style)
        .centered()
        .render(order_layout[0], buf);

    let descending_indicator = if !modal.sort.ascending { "●" } else { "○" };
    let descending_text = format!("{} Descending", descending_indicator);
    let descending_style = if !modal.sort.ascending {
        Style::default().add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };
    Paragraph::new(descending_text)
        .style(descending_style)
        .centered()
        .render(order_layout[1], buf);
}
