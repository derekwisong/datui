//! `label  value` on one line inside a Surface. The focused row carries the
//! rail and its label in the accent; the chosen value is always echoed, so
//! nothing is ambiguous when focus is elsewhere.

use crate::app::pointer::{FieldId, Hit};
use crate::render::context::RenderContext;
use crate::widgets::text_input::TextInput;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// What sits after the label.
pub enum FormValue<'a> {
    /// A text field. The caller sets the input's focus before rendering; the
    /// input draws its own cursor.
    Input(&'a TextInput),
    /// A checkbox.
    Toggle(bool),
    /// A pick-one value, cycled or chosen through a Picker; the row echoes the
    /// current choice.
    Choice(&'a str),
    /// A choice not yet made: the row says so quietly instead of sitting blank.
    Placeholder(&'a str),
    /// A short Choice's values side by side, the chosen one tinted, so ←/→ move
    /// along what is drawn. When they do not fit, the chosen one alone between
    /// step marks: `‹ TSV ›`. With `clicks`, each value records a click that
    /// steps the field to it; record the row's field before rendering, so the
    /// values lie on top of it.
    Options {
        items: &'a [&'a str],
        selected: usize,
        clicks: Option<FieldId>,
    },
    /// Styled text, such as column names in their types' colors. `dimmed` reads the
    /// whole row, label too, as not in use.
    Spans { spans: Vec<Span<'a>>, dimmed: bool },
}

pub struct FormRow<'a> {
    pub label: &'a str,
    pub value: FormValue<'a>,
    pub focused: bool,
    /// Where the value column starts, past the rail gutter, shared by every
    /// row so values align.
    pub label_width: u16,
}

impl FormRow<'_> {
    pub fn render(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        self.render_picking(area, buf, ctx, false);
    }

    /// Draw the row, its picker open when `picking`: the picker's current line
    /// has the keys and the one rail, so the row keeps its accent label and
    /// gives up its rail.
    pub fn render_picking(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext, picking: bool) {
        if area.height == 0 || area.width == 0 {
            return;
        }
        // The rail gutter is always there, so focus arriving moves nothing —
        // Tab walks the rail down the rows, which is what says the rows are
        // walkable. Same mark as the table's current row and the Picker's
        // selection: one focus signal everywhere.
        let g = crate::glyphs::get();
        let rail = if self.focused && !picking {
            g.rail
        } else {
            " "
        };
        Paragraph::new(rail)
            .style(Style::default().fg(ctx.accent))
            .render(Rect { width: 1, ..area }, buf);
        self.render_body(
            Rect {
                x: area.x + 1,
                width: area.width - 1,
                ..area
            },
            buf,
            ctx,
        );
    }

    /// The label and the value, from `area.x`, for a list that draws its own rail.
    pub fn render_body(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        if area.width == 0 || area.height == 0 {
            return;
        }
        let g = crate::glyphs::get();
        let dimmed = matches!(self.value, FormValue::Spans { dimmed: true, .. });
        let label_style = if dimmed {
            Style::default().fg(ctx.dimmed)
        } else if self.focused {
            Style::default().fg(ctx.accent).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.label)
        };
        let label_w = self.label_width.min(area.width);
        // Cells of air between the label and the value column.
        let air = label_w.saturating_sub(crate::glyphs::display_width(self.label) as u16);
        Paragraph::new(self.label).style(label_style).render(
            Rect {
                width: label_w,
                ..area
            },
            buf,
        );

        let value_area = Rect {
            x: area.x + label_w,
            width: area.width.saturating_sub(label_w),
            ..area
        };
        if value_area.width == 0 {
            return;
        }
        match &self.value {
            FormValue::Input(input) => (*input).render(value_area, buf),
            FormValue::Toggle(on) => {
                let marker = if *on { g.checkbox_on } else { g.checkbox_off };
                Paragraph::new(marker)
                    .style(Style::default().fg(ctx.text_primary))
                    .render(value_area, buf);
            }
            FormValue::Choice(value) => {
                Paragraph::new(*value)
                    .style(Style::default().fg(ctx.text_primary))
                    .render(value_area, buf);
            }
            FormValue::Placeholder(value) => {
                Paragraph::new(*value)
                    .style(Style::default().fg(ctx.dimmed))
                    .render(value_area, buf);
            }
            FormValue::Spans { spans, dimmed } => {
                let spans: Vec<Span> = spans
                    .iter()
                    .map(|s| match dimmed {
                        true => Span::styled(s.content.clone(), Style::default().fg(ctx.dimmed)),
                        false => s.clone(),
                    })
                    .collect();
                Paragraph::new(Line::from(spans)).render(value_area, buf);
            }
            FormValue::Options {
                items,
                selected,
                clicks,
            } => OptionsRow {
                items,
                selected: *selected,
                clicks: clicks.as_ref(),
                focused: self.focused,
                air,
            }
            .render(value_area, buf, ctx),
        }
    }
}

/// A choice's values on one row, or the chosen one alone where they do not fit.
struct OptionsRow<'a> {
    items: &'a [&'a str],
    selected: usize,
    clicks: Option<&'a FieldId>,
    focused: bool,
    /// Cells of air between the label and the value column, which the row may
    /// draw into so its chosen text lines up with the other rows' values.
    air: u16,
}

impl OptionsRow<'_> {
    fn render(&self, value_area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
        // The chosen one keeps its tint wherever focus is, so the choice reads
        // from anywhere in the form; the others recede when focus is elsewhere.
        let chosen = Style::default()
            .fg(ctx.text_primary)
            .add_modifier(Modifier::BOLD)
            .patch(ctx.highlight_style());
        let other = Style::default().fg(if self.focused {
            ctx.text_secondary
        } else {
            ctx.dimmed
        });
        let hit = |index: usize, current: usize| {
            self.clicks.map(|field| Hit::Option {
                field: Some(field.clone()),
                index,
                current,
            })
        };
        // Each value padded a cell each side, for the tint; drawn a cell into the
        // air, the first value's text sits in the value column. Compact, the step
        // mark takes a second cell.
        let full: usize = self
            .items
            .iter()
            .map(|item| crate::glyphs::display_width(item) + 2)
            .sum();
        let compact = full > usize::from(value_area.width + self.air.min(1));
        let lead = self.air.min(if compact { 2 } else { 1 });
        let mut spans = Vec::new();
        let mut hits = Vec::new();
        if !compact {
            for (i, item) in self.items.iter().enumerate() {
                if let Some(hit) = hit(i, self.selected) {
                    hits.push((spans.len(), hit));
                }
                let style = if i == self.selected { chosen } else { other };
                spans.push(Span::styled(format!(" {item} "), style));
            }
        } else {
            // Too narrow: the chosen value alone between step marks, which a
            // click steps by one as ←/→ do; the name itself takes Space.
            let g = crate::glyphs::get();
            let name = self.items.get(self.selected).copied().unwrap_or("");
            let mark = Style::default().fg(ctx.text_secondary);
            spans.push(Span::styled(g.choice_prev, mark));
            spans.push(Span::styled(format!(" {name} "), chosen));
            spans.push(Span::styled(g.choice_next, mark));
            if let (Some(back), Some(on)) = (hit(0, 1), hit(1, 0)) {
                hits.push((0, back));
                hits.push((2, on));
            }
        }
        let area = Rect {
            x: value_area.x - lead,
            width: value_area.width + lead,
            ..value_area
        };
        let line = Line::from(spans);
        crate::app::pointer::record_spans(area, &line, hits);
        Paragraph::new(line).render(area, buf);
    }
}

#[cfg(test)]
mod tests;
