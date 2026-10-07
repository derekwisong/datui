//! Column intent in Data Quality Setup: the list of the scope's columns with what
//! each is declared to hold, and the form that declares it for one.

use crate::glyphs;
use crate::intent_modal::{IntentField, IntentForm};
use crate::numfmt;
use crate::render::layout::centered_rect;
use crate::widgets::data_quality::{DataQualityWidgetConfig, fit, rule_line};
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
        .filter(|(name, _)| name.as_str() != crate::schema_union::DRIFT_COLUMN)
        .map(|(name, dtype)| (name.to_string(), dtype.clone()))
        .collect()
}

/// A column's type as the list names it: text read as time says how.
fn type_label(config: &DataQualityWidgetConfig<'_>, column: &str, dtype: &DataType) -> String {
    match config.plan.time_format(column) {
        Some(format) => format!("text as {}", format.kind.label()),
        None => crate::column_types::dtype_label(dtype),
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
    let dimmed = Style::default().fg(theme.get("dimmed"));
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
            Cell::from(fit(&rules.join(&format!(" {} ", g.middot)), rules_width))
        };
        Row::new(vec![
            Cell::from(fit(column, name_width as usize - 2)),
            Cell::from(Span::styled(fit(dtype, type_width as usize - 2), dimmed)),
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
    Paragraph::new(Line::styled(fit(&key_line, key.width as usize), dimmed)).render(
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
    let popup = centered_rect(
        area.inner(ratatui::layout::Margin::new(1, 1)),
        width,
        height,
    );
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
        value: FormValue::Choice(&crate::column_types::dtype_label(&form.dtype)),
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
        crate::pointer::record_field::<crate::intent_modal::IntentForm>(line(row), *field);
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
                IntentField::Allowed => "Comma-separated · \"a, b\" holds a comma".to_string(),
                IntentField::Minimum | IntentField::Maximum => crate::glyphs::dotted(&format!(
                    "{} · empty for no bound",
                    upper_first(form.value_kind().bound_hint())
                )),
            },
            false,
        ),
    };
    Paragraph::new(Line::styled(
        fit(&crate::glyphs::dotted(&status), content.width as usize),
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
mod tests {
    use super::*;
    use crate::analysis_modal::{AnalysisFocus, DetailScroll, SetupRow};
    use crate::config::Theme;
    use crate::data_quality::fixtures::measure;
    use crate::data_quality::{
        DataQualityPlan, DataQualityResults, QualityCompute, QualityMetric, QualityPage,
    };
    use crate::quality_export::ExportForm;
    use crate::quality_intent::{ColumnIntent, DeclaredIntent};
    use crate::render::context::RenderContext;
    use crate::table::DataTableState;
    use crate::widgets::data_quality::{SetupView, render};
    use polars::prelude::*;
    use std::sync::Arc;

    fn frame() -> LazyFrame {
        let rows = 2_000i64;
        df!(
            "id" => (0..rows).map(|row| row % 1_500).collect::<Vec<_>>(),
            "status" => (0..rows).map(|row| ["open", "closed", "void"][row as usize % 3]).collect::<Vec<_>>(),
            "amount" => (0..rows).map(|row| (row % 130) as f64 - 10.0).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy()
    }

    struct Screen {
        state: DataTableState,
        plan: DataQualityPlan,
        results: DataQualityResults,
        theme: Theme,
        ctx: RenderContext,
        findings: crate::quality_report::FindingsView,
    }

    /// What a draw puts over the page: the intent form or the export dialog.
    #[derive(Default)]
    struct Over<'a> {
        intent: Option<&'a IntentForm>,
        export: Option<&'a ExportForm>,
        access: bool,
    }

    impl Screen {
        fn new(compute: QualityCompute) -> Self {
            let lf = frame();
            let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
            let state = DataTableState::from_schema_and_lazyframe(
                schema,
                lf.clone(),
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap();
            let plan = DataQualityPlan {
                compute,
                dataset_rows: 500,
                intent: DeclaredIntent {
                    key: vec!["id".to_string()],
                    columns: vec![
                        ColumnIntent {
                            required: true,
                            allowed: vec!["open".to_string(), "closed".to_string()],
                            ..ColumnIntent::new("status")
                        },
                        ColumnIntent {
                            min: Some("0".to_string()),
                            max: Some("100".to_string()),
                            ..ColumnIntent::new("amount")
                        },
                    ],
                },
                ..DataQualityPlan::default()
            };
            let results = measure(&lf, Some(2_000), &plan);
            Self {
                state,
                plan,
                results,
                theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
                ctx: RenderContext::for_test(),
                findings: crate::quality_report::FindingsView::default(),
            }
        }

        fn draw(
            &self,
            page: QualityPage,
            field: usize,
            over: Over<'_>,
            size: (u16, u16),
        ) -> String {
            let config = DataQualityWidgetConfig {
                checks_expanded: false,
                state: &self.state,
                plan: &self.plan,
                measured: &self.plan,
                results: Some(&self.results),
                from_cache: false,
                metric: QualityMetric::NullRate,
                column_index: 0,
                segment_index: 0,
                interval_index: 0,
                trend_line: 0,
                expected_form: None,
                segments_by_change: false,
                page,
                setup: SetupView::default(),
                plan_field: field,
                show_access: over.access,
                observation_detail: false,
                focus: AnalysisFocus::Main,
                theme: &self.theme,
                ctx: &self.ctx,
                findings: &self.findings,
                rows_kept: true,
                evidence_read: None,
                intent_form: over.intent,
                export_form: over.export,
            };
            let (width, height) = size;
            let area = Rect::new(0, 0, width, height);
            let mut buf = Buffer::empty(area);
            let mut table = TableState::default();
            let mut sidebar = TableState::default();
            render(
                config,
                &mut table,
                &mut sidebar,
                &mut DetailScroll::default(),
                area,
                &mut buf,
            );
            (0..height)
                .map(|y| {
                    (0..width)
                        .map(|x| buf[(x, y)].symbol().to_string())
                        .collect::<String>()
                })
                .collect::<Vec<_>>()
                .join("\n")
        }
    }

    /// Every character outside ASCII is a glyph slot, which `LANG=C` swaps for its
    /// ASCII twin, or the frame.
    fn assert_glyph_slots(text: &str) {
        let g = glyphs::get();
        let slots = [
            g.rail,
            g.rule_h,
            g.rule_h_focused,
            g.middot,
            g.ellipsis,
            g.selector,
            g.checkbox_on,
            g.checkbox_off,
            g.warning,
            g.check,
            g.arrow_left,
            g.arrow_right,
        ]
        .concat();
        for c in text.chars().filter(|c| !c.is_ascii()) {
            assert!(
                slots.contains(c) || "╭╮╰╯│─".contains(c),
                "{c:?} is not a glyph slot:\n{text}"
            );
        }
    }

    const SIZES: [(u16, u16); 2] = [(80, 24), (60, 20)];

    /// The list names each column, its type and what it must hold, with the key
    /// under it; the form takes the rows the type takes. Both fit 80x24 and 60x20.
    #[test]
    fn the_intent_list_and_form_fit_80x24_and_60x20() {
        let screen = Screen::new(QualityCompute::Sample);
        let mut form = IntentForm::new(
            "amount",
            DataType::Float64,
            None,
            &screen.plan.intent,
            &screen.theme,
        );
        form.field = IntentField::Minimum;
        for size in SIZES {
            let text = screen.draw(QualityPage::Intent, 2, Over::default(), size);
            for expected in ["Column intent", "Must hold", "key", "0 to 100", "Key: id"] {
                assert!(text.contains(expected), "{expected} at {size:?}:\n{text}");
            }
            assert_glyph_slots(&text);

            let over = Over {
                intent: Some(&form),
                ..Over::default()
            };
            let text = screen.draw(QualityPage::Intent, 2, over, size);
            for expected in [
                "Intent: amount",
                "Key:",
                "Required:",
                "Minimum:",
                "Maximum:",
                "100",
                "A number · empty for no bound",
            ] {
                assert!(text.contains(expected), "{expected} at {size:?}:\n{text}");
            }
            assert!(!text.contains("Allowed:"), "a float takes no set:\n{text}");
            assert_glyph_slots(&text);
        }
        // A refused Enter says why on the form's own line.
        form.error = Some("Minimum is above maximum".to_string());
        let over = Over {
            intent: Some(&form),
            ..Over::default()
        };
        let text = screen.draw(QualityPage::Intent, 2, over, (80, 24));
        assert!(text.contains("Minimum is above maximum"), "{text}");
    }

    /// Setup names the declared intent on its row, and the Read section and the
    /// access plan say what it costs: nothing past the sample, and a key that speaks
    /// only for the sampled rows; on a full scan, the key's own pass.
    #[test]
    fn setup_discloses_what_intent_costs() {
        let screen = Screen::new(QualityCompute::Sample);
        let field = SetupRow::Intent.index();
        for size in SIZES {
            let text = screen.draw(QualityPage::Setup, field, Over::default(), size);
            assert!(text.contains("Column intent"), "{text}");
            assert!(text.contains("key id"), "{text}");
            assert_glyph_slots(&text);
        }
        let text = screen.draw(QualityPage::Setup, field, Over::default(), (120, 50));
        assert!(
            text.contains("Column intent: on the rows read · no extra read"),
            "{text}"
        );
        assert!(
            text.contains("Key: repeats among the 500 sampled rows only"),
            "{text}"
        );
        let over = Over {
            access: true,
            ..Over::default()
        };
        let text = screen.draw(QualityPage::Setup, field, over, (100, 30));
        assert!(text.contains("Column intent"), "{text}");
        assert!(text.contains("sampled rows only"), "{text}");

        let full = Screen::new(QualityCompute::Full);
        let text = full.draw(QualityPage::Setup, field, Over::default(), (120, 50));
        assert!(text.contains("key adds 1 pass"), "{text}");
    }

    /// The report lists the declared rules' violations as problems, and the export
    /// dialog fits over it at both sizes.
    #[test]
    fn the_report_and_export_dialog_fit_80x24_and_60x20() {
        let screen = Screen::new(QualityCompute::Full);
        let mut export = ExportForm::new("orders", &screen.theme);
        for size in SIZES {
            let text = screen.draw(QualityPage::Overview, 0, Over::default(), size);
            for title in ["Repeated key", "Not allowed", "Out of range"] {
                assert!(text.contains(title), "{title} at {size:?}:\n{text}");
            }
            assert_glyph_slots(&text);
            let over = Over {
                export: Some(&export),
                ..Over::default()
            };
            let text = screen.draw(QualityPage::Overview, 0, over, size);
            for expected in [
                "Export Report",
                "Path:",
                "orders-quality.json",
                "Format:",
                "JSON",
            ] {
                assert!(text.contains(expected), "{expected} at {size:?}:\n{text}");
            }
            assert_glyph_slots(&text);
        }
        export.error = Some("Type a path to write to".to_string());
        let over = Over {
            export: Some(&export),
            ..Over::default()
        };
        let text = screen.draw(QualityPage::Overview, 0, over, (80, 24));
        assert!(text.contains("Type a path to write to"), "{text}");
    }
}
