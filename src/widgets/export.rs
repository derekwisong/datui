//! Export modal rendering.

use crate::export_modal::{ExportFocus, ExportFormat, ExportModal};
use crate::CompressionFormat;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::{Color, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Block, Borders, Clear, List, ListItem, Paragraph, Widget};

/// Render the export modal with format selector on left, options on right.
pub fn render_export_modal(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
    text_primary: Color,
    text_inverse: Color,
) {
    Clear.render(area, buf);
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title("Export Data");
    let inner = block.inner(area);
    block.render(area, buf);

    // Split into left (format list) and right (options)
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(20), // Format list width
            Constraint::Min(40),    // Options area
        ])
        .split(inner);

    // Left: Format selector (list)
    render_format_list(chunks[0], buf, modal, border_color, active_color);

    // Right: Path input and format-specific options
    let right_chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3), // Path input
            Constraint::Min(10),   // Format-specific options
            Constraint::Length(3), // Buttons
        ])
        .split(chunks[1]);

    // Path input
    render_path_input(
        right_chunks[0],
        buf,
        modal,
        border_color,
        active_color,
        text_primary,
        text_inverse,
    );

    // Format-specific options
    render_format_options(
        right_chunks[1],
        buf,
        modal,
        border_color,
        active_color,
        text_primary,
        text_inverse,
    );

    // Footer buttons
    render_footer(right_chunks[2], buf, modal, border_color, active_color);
}

fn render_format_list(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
) {
    let is_focused = modal.focus == ExportFocus::FormatSelector;
    let border_style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title("Format");
    let inner = block.inner(area);
    block.render(area, buf);

    let items: Vec<ListItem> = ExportFormat::ALL
        .iter()
        .map(|format| {
            let marker = if modal.selected_format == *format {
                "●"
            } else {
                "○"
            };
            let style = if modal.selected_format == *format {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };
            ListItem::new(Line::from(vec![Span::styled(
                format!("{} {}", marker, format.as_str()),
                style,
            )]))
        })
        .collect();

    let list = List::new(items).style(if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default()
    });
    list.render(inner, buf);
}

fn render_path_input(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
    _text_primary: Color,
    _text_inverse: Color,
) {
    let is_focused = modal.focus == ExportFocus::PathInput;
    let border_style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(border_style)
        .title("File Path");
    let inner = block.inner(area);
    block.render(area, buf);

    // Render input using TextInput widget
    modal.path_input.set_focused(is_focused);
    (&modal.path_input).render(inner, buf);
}

fn render_format_options(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
    text_primary: Color,
    text_inverse: Color,
) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(border_color))
        .title("Options");
    let inner = block.inner(area);
    block.render(area, buf);

    match modal.selected_format {
        ExportFormat::Csv => render_csv_options(
            inner,
            buf,
            modal,
            border_color,
            active_color,
            text_primary,
            text_inverse,
        ),
        ExportFormat::Json => render_json_options(inner, buf, modal, border_color, active_color),
        ExportFormat::Ndjson => {
            render_ndjson_options(inner, buf, modal, border_color, active_color)
        }
        ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => {
            render_no_format_options(inner, buf, modal, border_color, active_color)
        }
    }
}

fn render_csv_options(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
    _text_primary: Color,
    _text_inverse: Color,
) {
    // Vertical layout: 3 rows + compression grid
    // Row 1: Delimiter label + input
    // Row 2: Include Header label + checkbox
    // Row 3: Compression label (on its own line)
    // Row 4+: Compression grid (3 items wide)

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Delimiter row
            Constraint::Length(1), // Include Header row
            Constraint::Length(1), // Compression label row
            Constraint::Min(1),    // Compression grid (flexible)
        ])
        .split(area);

    // Row 1: Delimiter label + input
    // Use fixed width for label to align with other labels
    let delimiter_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(15), // Fixed width for label alignment ("Delimiter:     ")
            Constraint::Length(2),  // Padding between label and widget
            Constraint::Min(1),     // Input widget (fills remaining space)
        ])
        .split(rows[0]);

    let is_delimiter_focused = modal.focus == ExportFocus::CsvDelimiter;
    let delimiter_label_style = if is_delimiter_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Delimiter:")
        .style(delimiter_label_style)
        .render(delimiter_row[0], buf);

    // Render delimiter input using TextInput widget (no border to fit on one row)
    modal.csv_delimiter_input.set_focused(is_delimiter_focused);
    (&modal.csv_delimiter_input).render(delimiter_row[2], buf);

    // Row 2: Include Header label + checkbox
    // Use same label width as delimiter for alignment
    let header_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(15), // Fixed width for label alignment ("Include Header:")
            Constraint::Length(2),  // Padding between label and widget
            Constraint::Min(1),     // Checkbox
        ])
        .split(rows[1]);

    let is_header_focused = modal.focus == ExportFocus::CsvIncludeHeader;
    let header_label_style = if is_header_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Include Header:")
        .style(header_label_style)
        .render(header_row[0], buf);

    // Checkbox
    let marker = if modal.csv_include_header {
        "☑"
    } else {
        "☐"
    };
    let checkbox_style = if is_header_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new(Line::from(vec![Span::styled(marker, checkbox_style)]))
        .render(header_row[2], buf);

    // Row 3: Compression label (on its own line)
    // Use same label width for alignment
    let compression_label_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(15), // Fixed width for label alignment ("Compression:    ")
            Constraint::Min(1),     // Rest of row
        ])
        .split(rows[2]);

    let is_compression_focused = modal.focus == ExportFocus::CsvCompression;
    let compression_label_style = if is_compression_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Compression:")
        .style(compression_label_style)
        .render(compression_label_row[0], buf);

    // Row 4+: Compression grid (3 items wide)
    if rows.len() > 3 && rows[3].height > 0 {
        render_compression_grid(
            rows[3],
            buf,
            modal,
            ExportFocus::CsvCompression,
            modal.csv_compression,
            border_color,
            active_color,
        );
    }
}

fn render_json_options(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
) {
    // Vertical layout: Compression label on its own line, then grid
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Compression label row
            Constraint::Min(1),    // Compression grid (flexible)
        ])
        .split(area);

    // Compression label (on its own line) - use fixed width for alignment
    let compression_label_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(15), // Fixed width for label alignment
            Constraint::Min(1),     // Rest of row
        ])
        .split(rows[0]);

    let is_compression_focused = modal.focus == ExportFocus::JsonCompression;
    let compression_label_style = if is_compression_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Compression:")
        .style(compression_label_style)
        .render(compression_label_row[0], buf);

    // Compression grid (3 items wide)
    if rows.len() > 1 && rows[1].height > 0 {
        render_compression_grid(
            rows[1],
            buf,
            modal,
            ExportFocus::JsonCompression,
            modal.json_compression,
            border_color,
            active_color,
        );
    }
}

fn render_ndjson_options(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
) {
    // Vertical layout: Compression label on its own line, then grid
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Compression label row
            Constraint::Min(1),    // Compression grid (flexible)
        ])
        .split(area);

    // Compression label (on its own line) - use fixed width for alignment
    let compression_label_row = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(15), // Fixed width for label alignment
            Constraint::Min(1),     // Rest of row
        ])
        .split(rows[0]);

    let is_compression_focused = modal.focus == ExportFocus::NdjsonCompression;
    let compression_label_style = if is_compression_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Compression:")
        .style(compression_label_style)
        .render(compression_label_row[0], buf);

    // Compression grid (3 items wide)
    if rows.len() > 1 && rows[1].height > 0 {
        render_compression_grid(
            rows[1],
            buf,
            modal,
            ExportFocus::NdjsonCompression,
            modal.ndjson_compression,
            border_color,
            active_color,
        );
    }
}

fn render_no_format_options(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    _active_color: Color,
) {
    let msg = format!(
        "No additional options for {} format",
        modal.selected_format.as_str()
    );
    Paragraph::new(msg)
        .style(Style::default().fg(border_color))
        .centered()
        .render(area, buf);
}

/// Render compression options in a grid layout (3 items wide)
fn render_compression_grid(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    focus: ExportFocus,
    compression: Option<CompressionFormat>,
    border_color: Color,
    active_color: Color,
) {
    let is_focused = modal.focus == focus;

    let compression_options = [
        (None, "None"),
        (Some(CompressionFormat::Gzip), "Gzip"),
        (Some(CompressionFormat::Zstd), "Zstd"),
        (Some(CompressionFormat::Bzip2), "Bzip2"),
        (Some(CompressionFormat::Xz), "XZ"),
    ];

    // Grid: 3 items per row
    const ITEMS_PER_ROW: usize = 3;
    let num_rows = (compression_options.len() as u16).div_ceil(ITEMS_PER_ROW as u16);

    // Calculate item width (divide area width by 3)
    let item_width = area.width / ITEMS_PER_ROW as u16;
    let item_height = 1;

    let mut option_idx = 0;
    for row in 0..num_rows.min(area.height) {
        let y = area.y + row;
        if y >= area.bottom() {
            break;
        }

        for col in 0..ITEMS_PER_ROW {
            if option_idx >= compression_options.len() {
                break;
            }

            let x = area.x + (col as u16 * item_width);
            let item_area = Rect {
                x,
                y,
                width: item_width,
                height: item_height,
            };

            let (opt, label) = &compression_options[option_idx];
            let is_selected = *opt == compression;
            let is_option_focused = is_focused && option_idx == modal.compression_selection_idx;

            let marker = if is_selected { "●" } else { "○" };
            let style = if is_selected || is_option_focused {
                Style::default().fg(active_color)
            } else {
                Style::default().fg(border_color)
            };

            let text = format!("{} {}", marker, label);
            Paragraph::new(Line::from(vec![Span::styled(text, style)])).render(item_area, buf);

            option_idx += 1;
        }
    }
}

fn render_footer(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    modal: &mut ExportModal,
    border_color: Color,
    active_color: Color,
) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(area);

    // Export button
    let is_focused = modal.focus == ExportFocus::ExportButton;
    let text_style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let border_style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Export")
        .style(text_style)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .centered()
        .render(chunks[0], buf);

    // Cancel button
    let is_focused = modal.focus == ExportFocus::CancelButton;
    let text_style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };
    let border_style = if is_focused {
        Style::default().fg(active_color)
    } else {
        Style::default().fg(border_color)
    };

    Paragraph::new("Cancel")
        .style(text_style)
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(border_style),
        )
        .centered()
        .render(chunks[1], buf);
}
