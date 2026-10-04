//! The Sample form: which rows every analysis tool reads, and how they are picked.
//!
//! Only settings. A value names itself ("All rows (36.8M)", "Random", "100,000
//! rows"), and a kind of rows that needs telling what to type (partitions, files, a
//! time range) carries its context on the lines under it. The keys are on the
//! control bar, like everywhere else.

use crate::render::context::RenderContext;
use crate::sample_modal::{SampleField, SampleForm};
use crate::widgets::ui::{FormRow, FormValue, Surface};
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget, Wrap};

/// How many source files the list under the Files row shows at once.
pub const FILES_SHOWN: usize = 6;

/// Where values start, past the rail gutter and the longest label.
const LABEL_WIDTH: u16 = 15;

/// `focused` is whether the form has the cursor; a form waiting beside a focused
/// tool list is drawn without the rail, so one thing looks focused.
pub fn render(form: &SampleForm, focused: bool, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
    let dimmed = Style::default().fg(ctx.dimmed);
    let g = crate::glyphs::get();
    // The form, top to bottom: a row per setting, and the context lines a setting
    // brings with it, directly under it.
    enum Item {
        Row(SampleField),
        Context(Line<'static>),
    }
    let mut items = Vec::new();
    for field in form.fields() {
        items.push(Item::Row(field));
        match field {
            SampleField::PartitionValues => {
                let known = form.partition_values_known();
                if !known.is_empty() {
                    let shown = if known.len() > 4 {
                        format!(
                            "{}, {}, {} {}",
                            known[0],
                            known[1],
                            g.ellipsis,
                            known[known.len() - 1]
                        )
                    } else {
                        known.join(", ")
                    };
                    items.push(Item::Context(Line::styled(
                        format!("Holds {shown}"),
                        dimmed,
                    )));
                }
                // The forms a value can take, shown with values this column holds.
                let (list, range) = match known.as_slice() {
                    [first, .., last] if known.len() >= 3 => {
                        (format!("{first},{last}"), format!("{}..{last}", known[1]))
                    }
                    _ => ("2019,2021".to_string(), "2020..2022".to_string()),
                };
                items.push(Item::Context(Line::styled(
                    format!("One, a list {list}, or a range {range}"),
                    dimmed,
                )));
            }
            SampleField::Files => {
                let chosen = form.files_chosen();
                let files = &form.context.files;
                for (index, name) in files
                    .iter()
                    .enumerate()
                    .skip(form.file_offset)
                    .take(FILES_SHOWN)
                {
                    let mark = if chosen.contains(&(index + 1)) {
                        g.checkbox_on
                    } else {
                        g.checkbox_off
                    };
                    items.push(Item::Context(Line::from(vec![
                        Span::styled(format!("{mark} {:>3}  ", index + 1), dimmed),
                        Span::styled(short_path(name), Style::default().fg(ctx.text_primary)),
                    ])));
                }
                let rest = files.len().saturating_sub(form.file_offset + FILES_SHOWN);
                if rest > 0 {
                    items.push(Item::Context(Line::styled(
                        format!("{rest} more {}", g.ellipsis),
                        dimmed,
                    )));
                }
            }
            SampleField::RangeTo => {
                if let Some(rows) = form.context.view_rows {
                    items.push(Item::Context(Line::styled(
                        format!("Table: {} rows", crate::numfmt::group_chrome(rows)),
                        dimmed,
                    )));
                }
            }
            SampleField::TimeBefore => {
                items.push(Item::Context(Line::styled(
                    format!("Dates as 2024-01-31 {} Before excluded", g.middot),
                    dimmed,
                )));
            }
            _ => {}
        }
    }

    let error_height = u16::from(form.error.is_some()) * 3;
    let height = items.len() as u16 + error_height + 2;
    let width = area.width.saturating_sub(4).clamp(40, 72).min(area.width);
    let frame = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height: height.min(area.height),
    };
    let inner = Surface::new("Sample").render(frame, buf, ctx);
    let bottom = inner.y + inner.height;
    let mut y = inner.y;
    for item in &items {
        if y >= bottom {
            return;
        }
        let line = Rect {
            y,
            height: 1,
            ..inner
        };
        match item {
            Item::Row(field) => {
                let choice = form.choice(*field);
                let value = match form.input(*field) {
                    Some(input) => FormValue::Input(input),
                    None => FormValue::Choice(&choice),
                };
                FormRow {
                    label: field.label(),
                    value,
                    focused: focused && form.field == *field,
                    label_width: LABEL_WIDTH,
                }
                .render(line, buf, ctx);
                crate::pointer::record_field::<SampleForm>(line, *field);
            }
            Item::Context(text) => {
                // Under the value column, so it reads as belonging to the row above.
                let indent = LABEL_WIDTH + 1;
                Paragraph::new(text.clone()).render(
                    Rect {
                        x: line.x + indent,
                        width: line.width.saturating_sub(indent),
                        ..line
                    },
                    buf,
                );
            }
        }
        y += 1;
    }
    if let Some(error) = &form.error
        && y + 1 < bottom
    {
        Paragraph::new(error.as_str())
            .wrap(Wrap { trim: true })
            .style(Style::default().fg(ctx.warning))
            .render(
                Rect {
                    y: y + 1,
                    height: bottom - y - 1,
                    ..inner
                },
                buf,
            );
    }
}

/// A file's path from its last two parts: a hive file is told apart by its
/// partition directory, and the root every file shares says nothing.
fn short_path(path: &str) -> String {
    let parts: Vec<&str> = path.rsplit(['/', '\\']).take(2).collect();
    parts.into_iter().rev().collect::<Vec<_>>().join("/")
}
