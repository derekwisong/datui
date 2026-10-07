//! Column intent in Data Quality Setup: the list of the scope's columns with what
//! each is declared to hold, and the form that declares it for one.

use crate::analysis::intent_modal::{IntentField, IntentForm};
use crate::glyphs;
use crate::numfmt;
use crate::render::layout::dialog_in;
use crate::widgets::data_quality::{DataQualityWidgetConfig, rule_line};
use crate::widgets::ui::{FormRow, FormValue, Surface};
use polars::prelude::DataType;
use ratatui::buffer::Buffer;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Paragraph, Row, StatefulWidget, Table, TableState, Widget};

/// The scope's columns Setup can declare intent on: the schema it reads, less
/// datui's own bookkeeping.
pub fn intent_columns(schema: &polars::prelude::Schema) -> Vec<(String, DataType)> {
    schema
        .iter()
        .filter(|(name, _)| name.as_str() != crate::formats::schema_union::DRIFT_COLUMN)
        .map(|(name, dtype)| (name.to_string(), dtype.clone()))
        .collect()
}

/// A column's type as the list names it: text read as time says how.
fn type_label(config: &DataQualityWidgetConfig<'_>, column: &str, dtype: &DataType) -> String {
    match config.plan.time_format(column) {
        Some(format) => format!("text as {}", format.kind.label()),
        None => crate::formats::column_types::dtype_label(dtype),
    }
}

/// Every column of the scope, each with its declared rules; the key named under it.
pub fn render_list(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let theme = config.theme;
    let plan = config.plan;
    let dimmed = Style::default().fg(theme.dimmed());
    let columns = intent_columns(config.state.quality_schema(&plan.scope));
    let [title, body, key] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Fill(1),
            Constraint::Length(2),
        ])
        .margin(1)
        .areas(area);
    let declared = plan.intent.declared_columns().len();
    Paragraph::new(rule_line(
        "Column intent",
        (declared > 0)
            .then(|| format!("{} declared", numfmt::group_chrome(declared)))
            .as_deref(),
        title.width,
        theme,
    ))
    .render(title, buf);
    if columns.is_empty() {
        Paragraph::new(Span::styled("No columns in this scope", dimmed)).render(body, buf);
        return;
    }
    let g = glyphs::get();
    let types = columns
        .iter()
        .map(|(column, dtype)| type_label(config, column, dtype))
        .collect::<Vec<_>>();
    let name_width = columns
        .iter()
        .map(|(column, _)| glyphs::display_width(column))
        .max()
        .unwrap_or(0)
        .clamp(6, 24) as u16
        + 2;
    let type_width = types
        .iter()
        .map(|dtype| glyphs::display_width(dtype))
        .max()
        .unwrap_or(0)
        .clamp(4, 18) as u16
        + 2;
    let rules_width = (body.width as usize)
        .saturating_sub(glyphs::display_width(g.selector) + (name_width + type_width) as usize);
    let rows = columns.iter().zip(&types).map(|((column, _), dtype)| {
        let mut rules = Vec::new();
        if plan.intent.key.iter().any(|name| name == column) {
            rules.push("key".to_string());
        }
        if let Some(intent) = plan.intent.column(column) {
            rules.extend(intent.rules());
        }
        let rules = if rules.is_empty() {
            Cell::from(Span::styled("any", dimmed))
        } else {
            Cell::from(crate::glyphs::fit(
                &rules.join(&format!(" {} ", g.middot)),
                rules_width,
            ))
        };
        Row::new(vec![
            Cell::from(crate::glyphs::fit(column, name_width as usize - 2)),
            Cell::from(Span::styled(
                crate::glyphs::fit(dtype, type_width as usize - 2),
                dimmed,
            )),
            rules,
        ])
    });
    table_state.select(Some(config.plan_field.min(columns.len().saturating_sub(1))));
    let table = Table::new(
        rows,
        [
            Constraint::Length(name_width),
            Constraint::Length(type_width),
            Constraint::Fill(1),
        ],
    )
    .header(Row::new(["Column", "Type", "Must hold"]).style(dimmed))
    .row_highlight_style(theme.highlight_style())
    .highlight_symbol(g.selector);
    StatefulWidget::render(table, body, buf, table_state);
    let key_line = if plan.intent.key.is_empty() {
        "Key: none declared".to_string()
    } else {
        format!("Key: {}", plan.intent.key.join(", "))
    };
    Paragraph::new(Line::styled(
        crate::glyphs::fit(&key_line, key.width as usize),
        dimmed,
    ))
    .render(
        Rect {
            y: key.y + 1,
            height: 1,
            ..key
        },
        buf,
    );
}

/// Where the form's values start, past the rail gutter and its longest label.
const LABEL_WIDTH: u16 = 11;

/// One column's declaration, over the list: a row per rule its type takes, and a
/// line saying what the focused row expects, or why Enter did not apply.
pub fn render_form(
    form: &IntentForm,
    config: &DataQualityWidgetConfig<'_>,
    area: Rect,
    buf: &mut Buffer,
) {
    let ctx = config.ctx;
    let fields = form.fields();
    let width = 64.min(area.width.saturating_sub(2));
    // The type, the rows, a blank and the status line, inside the frame.
    let height = fields.len() as u16 + 3 + 2;
    let popup = dialog_in(area, width, height);
    let title = format!("Intent: {}", form.column);
    let content = Surface::new(&title).render(popup, buf, ctx);
    if content.height < 2 || content.width < 8 {
        return;
    }
    let line = |index: u16| Rect {
        y: content.y + index,
        height: 1,
        ..content
    };
    FormRow {
        label: "Type:",
        value: FormValue::Choice(&crate::formats::column_types::dtype_label(&form.dtype)),
        focused: false,
        label_width: LABEL_WIDTH,
    }
    .render(line(0), buf, ctx);
    let reading = form.reading_label();
    for (index, field) in fields.iter().enumerate() {
        let row = index as u16 + 1;
        if row >= content.height.saturating_sub(1) {
            break;
        }
        let value = match field {
            IntentField::Key => FormValue::Toggle(form.in_key),
            IntentField::Required => FormValue::Toggle(form.required),
            IntentField::ReadAs => FormValue::Choice(&reading),
            text => match form.input(*text) {
                Some(input) => FormValue::Input(input),
                None => continue,
            },
        };
        FormRow {
            label: field.label(),
            value,
            focused: form.field == *field,
            label_width: LABEL_WIDTH,
        }
        .render(line(row), buf, ctx);
        crate::pointer::record_field::<crate::analysis::intent_modal::IntentForm>(
            line(row),
            *field,
        );
    }
    // What the focused row takes, or why Enter refused: the form's own line.
    let (status, warn) = match &form.error {
        Some(error) => (error.clone(), true),
        None => (
            match form.field {
                IntentField::Key => "Key column: together the key names one row".to_string(),
                IntentField::Required => "Every row has a value".to_string(),
                IntentField::ReadAs if form.time.is_some() => {
                    crate::glyphs::dotted("Read by Text as time · change in Setup")
                }
                IntentField::ReadAs => "Text that does not read is counted".to_string(),
                IntentField::Allowed => {
                    crate::glyphs::dotted("Comma-separated · \"a, b\" holds a comma")
                }
                IntentField::Minimum | IntentField::Maximum => crate::glyphs::dotted(&format!(
                    "{} · empty for no bound",
                    upper_first(form.value_kind().bound_hint())
                )),
            },
            false,
        ),
    };
    Paragraph::new(Line::styled(
        crate::glyphs::fit(&status, content.width as usize),
        Style::default().fg(if warn { ctx.warning } else { ctx.dimmed }),
    ))
    .render(line(content.height - 1), buf);
}

fn upper_first(text: &str) -> String {
    let mut chars = text.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests;
