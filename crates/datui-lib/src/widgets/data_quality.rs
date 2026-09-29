use crate::analysis_modal::{AnalysisFocus, AnalysisTool};
use crate::config::Theme;
use crate::data_quality::{
    DataQualityPlan, DataQualityResults, MAX_RETAINED_SAMPLE_ROWS, ObservationKind,
    QualityComparison, QualityCompute, QualityGrain, QualityMetric, QualityPage, QualityPrecision,
    QualityScope, TemporalRole,
};
use crate::glyphs;
use crate::numfmt;
use crate::quality_report::{
    CHECKS_SHOWN, Check, Outcome, QualityReport, Severity, build_report, checks, coverage,
    describe, explain, verdict,
};
use crate::widgets::datatable::DataTableState;
use crate::widgets::text_input::TextInput;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, StatefulWidget, Table, TableState,
    Widget, Wrap,
};

pub struct DataQualityWidgetConfig<'a> {
    /// The clean entry's checks table shows every check, not the first few.
    pub checks_expanded: bool,
    pub state: &'a DataTableState,
    pub plan: &'a DataQualityPlan,
    pub results: Option<&'a DataQualityResults>,
    pub from_cache: bool,
    pub metric: QualityMetric,
    pub column_index: usize,
    pub page: QualityPage,
    pub editing: bool,
    pub plan_field: usize,
    pub scope_input: &'a TextInput,
    pub scope_error: Option<&'a str>,
    pub scope_file_offset: usize,
    pub show_access: bool,
    pub observation_detail: bool,
    pub confirm_run: bool,
    pub running: bool,
    pub focus: AnalysisFocus,
    pub theme: &'a Theme,
}

pub fn render(
    config: DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    sidebar_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let sidebar_width = if area.width >= 108 {
        30
    } else if area.width >= 76 {
        20
    } else {
        0
    };
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .split(area);

    render_breadcrumb(&config, vertical[0], buf);
    render_plan_strip(&config, vertical[1], buf);

    let body = if sidebar_width > 0 {
        let horizontal = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([Constraint::Fill(1), Constraint::Length(sidebar_width)])
            .split(vertical[2]);
        render_sidebar(&config, sidebar_state, horizontal[1], buf);
        horizontal[0]
    } else {
        vertical[2]
    };

    match config.page {
        QualityPage::Plan => render_plan(&config, table_state, body, buf),
        QualityPage::Scope => render_scope(&config, body, buf),
        QualityPage::TimeRoles => render_time_roles(&config, table_state, body, buf),
        QualityPage::Overview => render_overview(&config, table_state, body, buf),
        QualityPage::Columns => render_columns(&config, table_state, body, buf),
        QualityPage::Segments => render_segments(&config, table_state, body, buf),
        QualityPage::Trends => render_trends(&config, table_state, body, buf),
        QualityPage::Detail => render_detail(&config, table_state, body, buf),
    }

    if config.show_access {
        render_access_plan(&config, area, buf);
    } else if config.observation_detail {
        render_finding_detail(&config, table_state, area, buf);
    } else if config.confirm_run {
        render_run_confirmation(&config, area, buf);
    } else if config.running {
        render_running(&config, area, buf);
    } else if sidebar_width == 0 && config.focus == AnalysisFocus::Sidebar {
        render_narrow_tool_picker(&config, sidebar_state, area, buf);
    }
}

fn render_breadcrumb(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let page = match config.page {
        QualityPage::Plan => None,
        QualityPage::Scope => Some("Scope"),
        QualityPage::TimeRoles => Some("Time roles"),
        QualityPage::Overview => Some("Overview"),
        QualityPage::Columns => Some("Columns"),
        QualityPage::Segments => Some("Segments"),
        QualityPage::Trends => Some("Trends"),
        QualityPage::Detail => Some("Detail"),
    };
    let mut spans = vec![
        Span::raw("Analysis"),
        Span::styled(" / ", Style::default().fg(config.theme.get("dimmed"))),
        Span::raw("Data Quality"),
    ];
    if let Some(page) = page {
        spans.push(Span::styled(
            " / ",
            Style::default().fg(config.theme.get("dimmed")),
        ));
        spans.push(Span::styled(
            page,
            Style::default()
                .fg(config.theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        ));
    }
    if config.from_cache {
        spans.push(Span::styled(
            "  [session cache]",
            Style::default().fg(config.theme.get("dimmed")),
        ));
    }
    Paragraph::new(Line::from(spans))
        .style(Style::default().bg(config.theme.get("controls_bg")))
        .render(area, buf);
}

/// Whole leading segments that fit `width` columns: a strip cut mid-word
/// ("-> compa") reads as a different fact, so trailing facts yield whole.
fn fit_segments(segments: Vec<Vec<Span<'static>>>, width: u16) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut used = 0usize;
    for segment in segments {
        let w: usize = segment
            .iter()
            .map(|s| crate::glyphs::display_width(&s.content))
            .sum();
        if used + w > width as usize && !spans.is_empty() {
            break;
        }
        used += w;
        spans.extend(segment);
    }
    Line::from(spans)
}

/// Over a result, the strip says what the numbers were measured on, in words; the
/// plan's own vocabulary is for the Plan page, where it is being chosen.
fn render_coverage_strip(
    config: &DataQualityWidgetConfig<'_>,
    results: &DataQualityResults,
    area: Rect,
    buf: &mut Buffer,
) {
    let dimmed = Style::default().fg(config.theme.get("dimmed"));
    let plan = config.plan;
    let mut facts = Vec::new();
    if plan.grain != QualityGrain::Dataset {
        facts.push(format!("by {}", plan.grain.label()));
    }
    if plan.comparison != QualityComparison::None {
        facts.push(format!("compared with {}", plan.comparison_label()));
    }
    if plan.compute == QualityCompute::Sample && results.precision != QualityPrecision::Exact {
        facts.push(format!("seed {}", results.sample_seed));
    }
    if config.state.is_remote_source() {
        facts.push("remote source, read only".to_string());
    }
    let separator = format!(" {} ", glyphs::get().middot);
    Paragraph::new(vec![
        Line::styled(
            fit(&coverage(results, plan), area.width as usize),
            Style::default().fg(config.theme.get("text_primary")),
        ),
        Line::styled(facts.join(&separator), dimmed),
    ])
    .style(Style::default().bg(config.theme.get("table_header_bg")))
    .render(area, buf);
}

fn render_plan_strip(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    if let Some(results) = config.results
        && !matches!(
            config.page,
            QualityPage::Plan | QualityPage::Scope | QualityPage::TimeRoles
        )
    {
        render_coverage_strip(config, results, area, buf);
        return;
    }
    let remote = config.state.is_remote_source();
    let bytes = planned_read_label(config.state, config.plan);
    let source = if remote {
        "remote transfer"
    } else {
        "local read"
    };
    let source_style = if remote {
        Style::default().fg(config.theme.get("warning"))
    } else {
        Style::default().fg(config.theme.get("text_primary"))
    };
    let dimmed = Style::default().fg(config.theme.get("dimmed"));
    let accent = Style::default().fg(config.theme.get("accent"));
    let plan_line = fit_segments(
        vec![
            vec![
                Span::styled("scope ", dimmed),
                Span::styled(
                    config.plan.scope.label(),
                    accent.add_modifier(Modifier::BOLD),
                ),
            ],
            vec![
                Span::styled(" -> grain ", dimmed),
                Span::styled(config.plan.grain.label(), accent),
            ],
            vec![
                Span::styled(" -> compute ", dimmed),
                Span::styled(compute_label(config.plan), accent),
            ],
            vec![
                Span::styled(" -> compare ", dimmed),
                Span::styled(config.plan.comparison_label(), accent),
            ],
        ],
        area.width,
    );
    let cost_line = fit_segments(
        vec![
            vec![
                Span::styled(format!("{source} "), source_style),
                Span::styled(
                    if remote { "unknown".to_string() } else { bytes },
                    source_style.add_modifier(Modifier::BOLD),
                ),
            ],
            vec![
                Span::styled(" | ", dimmed),
                Span::styled(
                    if remote {
                        "requests unknown".to_string()
                    } else {
                        "no network requests".to_string()
                    },
                    dimmed,
                ),
            ],
            vec![
                Span::styled(" | ", dimmed),
                Span::styled(
                    "remote write 0 B",
                    Style::default().fg(config.theme.get("success")),
                ),
            ],
        ],
        area.width,
    );
    let lines = vec![plan_line, cost_line];
    Paragraph::new(lines)
        .style(Style::default().bg(config.theme.get("table_header_bg")))
        .render(area, buf);
}

fn render_plan(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    // The plan table is the page; when the terminal cannot hold everything, drop
    // the access summary — the plan strip and `p` both still carry it — rather
    // than let the solver shave a row off the plan and hide a field.
    const PLAN_ROWS: u16 = 9;
    const ACCESS_ROWS: u16 = 7;
    let compact = area.height.saturating_sub(2) < 2 + PLAN_ROWS + ACCESS_ROWS;
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints(if compact {
            vec![
                Constraint::Length(2),
                Constraint::Length(PLAN_ROWS),
                Constraint::Length(0),
                Constraint::Length(0),
                Constraint::Min(0),
            ]
        } else {
            vec![
                Constraint::Length(2),
                Constraint::Length(PLAN_ROWS),
                Constraint::Length(2),
                Constraint::Length(5),
                Constraint::Min(0),
            ]
        })
        .margin(1)
        .split(area);
    render_section_title("PROFILE PLAN", sections[0], config.theme, buf);

    let temporal = if config.plan.temporal_roles.is_empty() {
        "none".to_string()
    } else {
        config
            .plan
            .temporal_roles
            .iter()
            .map(|item| format!("{}={}", item.role.label(), item.column))
            .collect::<Vec<_>>()
            .join(" -> ")
    };
    let rows = vec![
        Row::new(vec![
            Cell::from("Scope"),
            Cell::from(config.plan.scope.label()),
        ]),
        Row::new(vec![
            Cell::from("Grain"),
            Cell::from(config.plan.grain.label()),
        ]),
        Row::new(vec![
            Cell::from("Compute"),
            Cell::from(compute_label(config.plan)),
        ]),
        Row::new(vec![
            Cell::from("Compare"),
            Cell::from(config.plan.comparison_label()),
        ]),
        Row::new(vec![Cell::from("Time roles"), Cell::from(temporal)]),
        Row::new(vec![
            Cell::from("Latency threshold"),
            Cell::from(
                config
                    .plan
                    .latency_threshold_seconds
                    .map(|seconds| duration_label(Some(seconds)))
                    .unwrap_or_else(|| "none".to_string()),
            ),
        ]),
        Row::new(vec![
            Cell::from("Sample rows"),
            Cell::from(format!(
                "{} per segment",
                numfmt::group_chrome(config.plan.sample_rows)
            )),
        ]),
    ];
    if config.editing {
        table_state.select(Some(config.plan_field));
    } else {
        table_state.select(None);
    }
    let table = Table::new(rows, [Constraint::Length(18), Constraint::Fill(1)])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_set(crate::glyphs::get().border)
                .border_style(Style::default().fg(config.theme.get("modal_border"))),
        )
        .row_highlight_style(config.theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);

    render_section_title("ACCESS", sections[2], config.theme, buf);
    let planned = planned_rows(config.state, config.plan);
    let planned_label = planned
        .map(numfmt::group_chrome)
        .unwrap_or_else(|| "unknown".to_string());
    let bytes_label = planned_read_label(config.state, config.plan);
    let access_rows = vec![
        Row::new(vec!["Rows evaluated", planned_label.as_str()]),
        Row::new(vec![
            if config.state.is_remote_source() {
                "Remote transfer"
            } else {
                "Local read"
            },
            bytes_label.as_str(),
        ]),
        Row::new(vec!["Remote writes", "none"]),
        Row::new(vec!["Session memory", "profile; size unknown"]),
    ];
    let access = Table::new(access_rows, [Constraint::Length(22), Constraint::Fill(1)])
        .block(Block::default().borders(Borders::NONE));
    Widget::render(access, sections[3], buf);
}

fn render_scope(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(7),
            Constraint::Length(3),
            Constraint::Fill(1),
        ])
        .margin(1)
        .split(area);
    render_section_title("ELIGIBLE ROWS", sections[0], config.theme, buf);
    Paragraph::new(vec![
        Line::raw("view  |  source  |  rows 100..200"),
        Line::raw("files 1,3  (source inventory order)"),
        Line::raw("partition column=value  (source)"),
        Line::raw("time column=2024-01-01..2024-02-01"),
        Line::raw("Time end is exclusive; dates use UTC midnight."),
    ])
    .style(Style::default().fg(config.theme.get("text_primary")))
    .render(sections[1], buf);
    Widget::render(config.scope_input, sections[2], buf);
    let bottom = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .split(sections[3]);
    if let Some(error) = config.scope_error {
        Paragraph::new(error)
            .style(Style::default().fg(config.theme.get("warning")))
            .render(bottom[0], buf);
    }
    let files = config.state.quality_source_file_names();
    if !files.is_empty() {
        render_section_title(
            &format!(
                "SOURCE FILES  {} / {}",
                config.scope_file_offset + 1,
                files.len()
            ),
            bottom[1],
            config.theme,
            buf,
        );
        let list_area = Rect {
            y: bottom[1].y.saturating_add(2),
            height: bottom[1].height.saturating_sub(2),
            ..bottom[1]
        };
        let visible = list_area.height as usize;
        let items = files
            .iter()
            .enumerate()
            .skip(config.scope_file_offset)
            .take(visible)
            .map(|(index, name)| ListItem::new(format!("{:>3}  {name}", index + 1)))
            .collect::<Vec<_>>();
        Widget::render(List::new(items), list_area, buf);
    }
}

fn render_overview(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let report = build_report(results);
    let notes = config.state.notes();
    let notes_height = if notes.is_empty() {
        0
    } else {
        (notes.len() as u16).min(3) + 2
    };
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(notes_height),
            Constraint::Fill(1),
        ])
        .horizontal_margin(1)
        .vertical_margin(1)
        .split(area);
    render_verdict(config, &report, sections[0], buf);
    if !notes.is_empty() {
        let mut lines = vec![rule_line(
            "Dataset notes",
            Some(&numfmt::group_chrome(notes.len())),
            sections[1].width,
            config.theme,
        )];
        lines.extend(notes.iter().take(3).map(|note| {
            Line::raw(format!(
                "{} {} {}",
                note.summary,
                glyphs::get().dash,
                note.scope
            ))
        }));
        Paragraph::new(lines).render(sections[1], buf);
    }

    let list = sections[2];
    if report.findings.is_empty() {
        let message = if report.metadata_only {
            "No values were read, so nothing about them is known. Press e and set Compute to sample or full to check the values."
        } else {
            "No columns to check."
        };
        Paragraph::new(message)
            .wrap(Wrap { trim: true })
            .style(Style::default().fg(config.theme.get("dimmed")))
            .render(list, buf);
        return;
    }
    normalize_selection(table_state, report.findings.len());
    if report.problems == 0 && report.notes == 0 && !report.metadata_only {
        // Nothing to fix: what was checked is the answer, so it is on the page
        // rather than behind the clean entry.
        let parts = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Fill(1)])
            .split(list);
        render_findings(config, &report, table_state, parts[0], buf);
        Paragraph::new(check_lines(
            &checks(results, &report),
            parts[1].width,
            None,
            config.theme,
        ))
        .render(parts[1], buf);
        return;
    }
    render_findings(config, &report, table_state, list, buf);
}

/// The answer before the evidence: a mark and the counts. What they were measured on
/// is the strip above.
fn render_verdict(
    config: &DataQualityWidgetConfig<'_>,
    report: &QualityReport,
    area: Rect,
    buf: &mut Buffer,
) {
    let g = glyphs::get();
    let theme = config.theme;
    let (mark, tone) = if report.problems > 0 {
        (g.warning, theme.get("warning"))
    } else if report.metadata_only {
        (g.middot, theme.get("dimmed"))
    } else {
        (g.check, theme.get("success"))
    };
    Paragraph::new(Line::from(vec![
        Span::styled(format!("{mark} "), Style::default().fg(tone)),
        Span::styled(
            verdict(report),
            Style::default().fg(tone).add_modifier(Modifier::BOLD),
        ),
    ]))
    .render(area, buf);
}

/// A title on a rule with a flat count chip, as `SectionRule` draws it, from the
/// theme this widget is handed.
fn rule_line(title: &str, chip: Option<&str>, width: u16, theme: &Theme) -> Line<'static> {
    let mut spans = vec![
        Span::styled(
            title.to_string(),
            Style::default()
                .fg(theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(" "),
    ];
    let mut used = glyphs::display_width(title) + 1;
    if let Some(chip) = chip {
        let chip = format!(" {chip} ");
        used += glyphs::display_width(&chip) + 1;
        spans.push(Span::styled(
            chip,
            Style::default()
                .bg(theme.get("controls_bg"))
                .fg(theme.get("text_primary")),
        ));
        spans.push(Span::raw(" "));
    }
    spans.push(Span::styled(
        glyphs::get()
            .rule_h
            .repeat((width as usize).saturating_sub(used)),
        Style::default().fg(theme.get("column_separator")),
    ));
    Line::from(spans)
}

fn severity_mark(severity: Severity, theme: &Theme) -> Span<'static> {
    let g = glyphs::get();
    match severity {
        Severity::Problem => Span::styled(g.warning, Style::default().fg(theme.get("warning"))),
        Severity::Note => Span::styled(g.middot, Style::default().fg(theme.get("text_primary"))),
        Severity::Clean => Span::styled(g.check, Style::default().fg(theme.get("success"))),
    }
}

/// Findings under one rule per severity. The selection lives in finding space, so
/// the keys never land on a rule; the scroll offset lives in line space.
fn render_findings(
    config: &DataQualityWidgetConfig<'_>,
    report: &QualityReport,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    enum Item {
        Rule(Severity, usize),
        Gap,
        Finding(usize),
    }
    let mut items = Vec::new();
    let mut current = None;
    for (index, finding) in report.findings.iter().enumerate() {
        if current != Some(finding.severity) {
            if current.is_some() {
                items.push(Item::Gap);
            }
            let count = report
                .findings
                .iter()
                .filter(|other| other.severity == finding.severity)
                .count();
            let count = match finding.severity {
                // The clean entry is one row naming many columns; count the columns.
                Severity::Clean => report.clean_columns,
                _ => count,
            };
            items.push(Item::Rule(finding.severity, count));
            current = Some(finding.severity);
        }
        items.push(Item::Finding(index));
    }

    let height = area.height as usize;
    if height == 0 {
        return;
    }
    let selected = table_state.selected().unwrap_or(0);
    let selected_line = items
        .iter()
        .position(|item| matches!(item, Item::Finding(index) if *index == selected))
        .unwrap_or(0);
    let mut offset = table_state.offset().min(items.len().saturating_sub(1));
    if selected_line < offset {
        // Bring the section's rule along when the selection is its first row.
        offset = if selected_line > 0 && matches!(items[selected_line - 1], Item::Rule(..)) {
            selected_line - 1
        } else {
            selected_line
        };
    } else if selected_line >= offset + height {
        offset = selected_line + 1 - height;
    }
    *table_state.offset_mut() = offset;

    // Mark, title, columns, summary. The title column fits the longest title; the
    // columns take a share of what is left, so the summary keeps the rest.
    const TITLE: usize = 17;
    let width = area.width as usize;
    let lead = 4; // rail + space + mark + space
    let rest = width.saturating_sub(lead + TITLE);
    let columns_width = (rest * 2 / 5).clamp(10.min(rest), 32);
    let summary_width = rest.saturating_sub(columns_width + 1);
    let g = glyphs::get();
    let theme = config.theme;
    let focused = config.focus == AnalysisFocus::Main;
    for (row, item) in items.iter().skip(offset).take(height).enumerate() {
        let line_area = Rect {
            y: area.y + row as u16,
            height: 1,
            ..area
        };
        let line = match item {
            Item::Gap => continue,
            Item::Rule(severity, count) => rule_line(
                severity.heading(),
                Some(&numfmt::group_chrome(*count)),
                area.width,
                theme,
            ),
            Item::Finding(index) => {
                let finding = &report.findings[*index];
                let is_selected = *index == selected;
                let columns = fit(&finding.columns_label(columns_width), columns_width);
                let summary = fit(&finding.summary, summary_width);
                let line = Line::from(vec![
                    Span::styled(
                        if is_selected { g.rail } else { " " },
                        Style::default().fg(theme.get("accent")),
                    ),
                    Span::raw(" "),
                    severity_mark(finding.severity, theme),
                    Span::raw(" "),
                    Span::styled(
                        format!("{:<TITLE$}", finding.title),
                        Style::default().fg(theme.get("text_primary")),
                    ),
                    Span::styled(
                        format!("{columns:<columns_width$} "),
                        Style::default().fg(theme.get("text_primary")),
                    ),
                    Span::styled(summary, Style::default().fg(theme.get("dimmed"))),
                ]);
                if is_selected {
                    let style = if focused {
                        theme.highlight_style()
                    } else {
                        Style::default()
                    };
                    buf.set_style(line_area, style);
                }
                line
            }
        };
        line.render(line_area, buf);
    }
}

/// The checks the run made, one per line: mark, name, reach, outcome, and what each
/// looks for when there is room. `limit` keeps the first few; the rest are counted.
fn check_lines(
    checks: &[Check],
    width: u16,
    limit: Option<usize>,
    theme: &Theme,
) -> Vec<Line<'static>> {
    const NAME: usize = 18;
    const REACH: usize = 24;
    const OUTCOME: usize = 33;
    let width = width as usize;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let g = glyphs::get();
    let shown = limit.unwrap_or(checks.len()).min(checks.len());
    let mut lines = vec![rule_line(
        "Checks",
        Some(&numfmt::group_chrome(checks.len())),
        width as u16,
        theme,
    )];
    for check in &checks[..shown] {
        let (mark, outcome, style) = match &check.outcome {
            Outcome::Passed => (
                Span::styled(g.check, Style::default().fg(theme.get("success"))),
                "passed".to_string(),
                Style::default().fg(theme.get("text_primary")),
            ),
            Outcome::Found { tier, detail } => (
                severity_mark(*tier, theme),
                format!("{detail} flagged"),
                Style::default().fg(theme.get("text_primary")),
            ),
            Outcome::NotRun(reason) => (
                Span::styled(g.dash, dimmed),
                format!("not run: {reason}"),
                dimmed,
            ),
        };
        let rest = width.saturating_sub(2 + NAME + REACH);
        let outcome_width = if rest > OUTCOME + 20 { OUTCOME } else { rest };
        let mut spans = vec![
            mark,
            Span::raw(" "),
            Span::styled(format!("{:<NAME$}", check.name), style),
            Span::styled(
                format!("{:<REACH$}", fit(&check.applies_to, REACH - 1)),
                dimmed,
            ),
            Span::styled(
                format!(
                    "{:<outcome_width$}",
                    fit(&outcome, outcome_width.saturating_sub(1))
                ),
                style,
            ),
        ];
        // What the check looks for, where the table has the width; the names say
        // most of it, and the user guide says the rest.
        let looks = rest.saturating_sub(outcome_width);
        if looks >= 16 {
            spans.push(Span::styled(fit(check.looks_for, looks), dimmed));
        }
        lines.push(Line::from(spans));
    }
    if shown < checks.len() {
        lines.push(Line::styled(
            format!(
                "  {} more {}",
                checks.len() - shown,
                if checks.len() - shown == 1 {
                    "check"
                } else {
                    "checks"
                }
            ),
            dimmed,
        ));
    }
    lines
}

/// `text` cut to `width` display columns, with the ellipsis glyph when anything was
/// cut, so a truncated count never reads as a smaller one.
fn fit(text: &str, width: usize) -> String {
    if glyphs::display_width(text) <= width {
        return text.to_string();
    }
    if width == 0 {
        return String::new();
    }
    let ellipsis = glyphs::get().ellipsis;
    let keep = width.saturating_sub(glyphs::display_width(ellipsis));
    format!("{}{ellipsis}", glyphs::take_columns(text, keep))
}

/// The finding itself: what it is in one sentence with its numbers, why it matters,
/// what to check, and the evidence, in a frame on top of the list.
fn render_finding_detail(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        return;
    };
    let report = build_report(results);
    let Some(finding) = table_state
        .selected()
        .and_then(|index| report.findings.get(index))
    else {
        return;
    };
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let label = Style::default().fg(theme.get("accent"));
    let explanation = explain(finding);
    let (headline, evidence) = describe(finding, results);
    // A reading surface: cap the measure on a wide terminal. The checks table on
    // the clean entry is a table, and may use more of the width.
    let width = area
        .width
        .saturating_sub(4)
        .min(if finding.kind.is_none() { 118 } else { 84 });
    let inner = width.saturating_sub(4).max(1);
    let mut lines = vec![
        Line::styled(
            finding.columns.join(", "),
            Style::default()
                .fg(theme.get("text_primary"))
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(headline),
    ];
    if !evidence.is_empty() {
        lines.push(Line::raw(""));
        lines.extend(
            evidence
                .into_iter()
                .map(|line| Line::raw(format!("  {line}"))),
        );
    }
    if !explanation.why.is_empty() {
        lines.push(Line::raw(""));
        lines.push(Line::from(vec![
            Span::styled("Why it matters: ", label),
            Span::raw(explanation.why),
        ]));
        lines.push(Line::from(vec![
            Span::styled("What to check: ", label),
            Span::raw(explanation.check),
        ]));
    }
    if finding.kind.is_none() {
        lines.push(Line::raw(""));
        lines.extend(check_lines(
            &checks(results, &report),
            inner,
            (!config.checks_expanded).then_some(CHECKS_SHOWN),
            theme,
        ));
    }
    lines.push(Line::raw(""));
    if let Some(kind) = finding.kind {
        lines.push(Line::styled(
            format!("Measured as: {}", kind.definition()),
            dimmed,
        ));
    }
    lines.push(Line::styled(coverage(results, config.plan), dimmed));
    if finding.kind.is_some() {
        let rows = if finding.can_open_rows(results) {
            match finding.kind {
                // The measurement counts rows beyond one per value; the rows that
                // share a value are always more.
                Some(ObservationKind::KeyLike) => {
                    "Enter shows every row that shares a repeated value.".to_string()
                }
                Some(ObservationKind::Absent | ObservationKind::TypeConflict) => {
                    let files = finding
                        .evidence_scope(results)
                        .map(|scope| match scope {
                            QualityScope::SourceFiles(files) => files.len(),
                            _ => 0,
                        })
                        .unwrap_or(0);
                    format!(
                        "Enter shows the rows of the {files} named {}.",
                        if files == 1 { "file" } else { "files" }
                    )
                }
                _ => "Enter shows the rows.".to_string(),
            }
        } else if results.precision == crate::data_quality::QualityPrecision::Sampled
            && finding.evidence_predicate(results).is_some()
        {
            "Measured on a sample: run a full profile (e, Compute) to show the exact rows."
                .to_string()
        } else {
            String::new()
        };
        if !rows.is_empty() {
            lines.push(Line::styled(rows, dimmed));
        }
    }
    // Grow with the text up to the screen rather than cut it.
    let inner = inner as usize;
    let needed = lines
        .iter()
        .map(|line| line.width().max(1).div_ceil(inner))
        .sum::<usize>() as u16
        + 2;
    let popup = centered_rect(width, needed.min(area.height.saturating_sub(2)), area);
    Clear.render(popup, buf);
    Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .block(
            Block::default()
                .title(finding.title)
                .borders(Borders::ALL)
                .border_set(crate::glyphs::get().border)
                .border_style(Style::default().fg(theme.get("modal_border_active")))
                .padding(ratatui::widgets::Padding::horizontal(1)),
        )
        .render(popup, buf);
}

fn render_time_roles(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Fill(1),
            Constraint::Length(3),
        ])
        .margin(1)
        .split(area);
    Paragraph::new(
        "Assign meaning explicitly. Datui never infers event, publication, receipt, or processing semantics from a column name.",
    )
    .style(Style::default().fg(config.theme.get("text_primary")))
    .render(sections[0], buf);

    let rows = TemporalRole::ALL.iter().map(|role| {
        let assignment = config
            .plan
            .temporal_roles
            .iter()
            .find(|assignment| assignment.role == *role);
        Row::new(vec![
            role.label().to_string(),
            assignment
                .map(|item| item.column.clone())
                .unwrap_or_else(|| "unassigned".to_string()),
            assignment
                .and_then(|item| item.timezone.clone())
                .unwrap_or_else(|| "source value".to_string()),
        ])
    });
    table_state.select(Some(config.plan_field.min(TemporalRole::ALL.len() - 1)));
    let table = Table::new(
        rows,
        [
            Constraint::Length(22),
            Constraint::Length(28),
            Constraint::Fill(1),
        ],
    )
    .header(
        Row::new(["Semantic role", "Accepted column", "Interpretation"])
            .style(Style::default().fg(config.theme.get("dimmed"))),
    )
    .row_highlight_style(config.theme.highlight_style())
    .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);
    Paragraph::new(format!(
        "{} cycles only date and datetime columns. Unassigned roles produce no lifecycle claims.",
        glyphs::get().updown_lr
    ))
    .style(Style::default().fg(config.theme.get("dimmed")))
    .render(sections[2], buf);
}

fn render_columns(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let report = build_report(results);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .split(area);
    let title = rule_line(
        "Columns",
        Some(&numfmt::group_chrome(results.columns.len())),
        sections[0].width,
        config.theme,
    );
    Paragraph::new(title).render(sections[0], buf);
    let layout = if sections[1].width >= 120 {
        2
    } else if sections[1].width >= 72 {
        1
    } else {
        0
    };
    let dimmed = Style::default().fg(config.theme.get("dimmed"));
    let rows = results.columns.iter().enumerate().map(|(index, profile)| {
        let severity = report
            .column_status
            .get(index)
            .copied()
            .unwrap_or(Severity::Clean);
        let mark = Cell::from(Line::from(severity_mark(severity, config.theme)));
        let findings = report
            .column_findings
            .get(index)
            .filter(|titles| !titles.is_empty())
            .map(|titles| Cell::from(titles.join(", ")))
            .unwrap_or_else(|| Cell::from(Span::styled("none", dimmed)));
        let most_common = profile
            .dominant_value
            .as_ref()
            .zip(profile.dominant_count)
            .map(|(value, count)| {
                format!(
                    "{value} ({})",
                    crate::quality_report::percent(count, profile.non_null_rows())
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let missing = if profile.null_count == 0 {
            Cell::from(Span::styled("0", dimmed))
        } else {
            Cell::from(format!(
                "{} ({})",
                numfmt::group_chrome(profile.null_count),
                crate::quality_report::percent(profile.null_count, profile.evaluated_rows)
            ))
        };
        let distinct = count_label(profile.distinct_count);
        Row::new(match layout {
            2 => vec![
                mark,
                Cell::from(profile.name.clone()),
                Cell::from(profile.dtype.to_string()),
                missing,
                Cell::from(distinct),
                Cell::from(most_common),
                findings,
            ],
            1 => vec![
                mark,
                Cell::from(profile.name.clone()),
                Cell::from(profile.dtype.to_string()),
                missing,
                findings,
            ],
            _ => vec![mark, Cell::from(profile.name.clone()), findings],
        })
    });
    normalize_selection(table_state, results.columns.len());
    let (headers, widths) = match layout {
        2 => (
            vec![
                "",
                "Column",
                "Type",
                "Missing",
                "Distinct",
                "Most common",
                "Findings",
            ],
            vec![
                Constraint::Length(1),
                Constraint::Length(22),
                Constraint::Length(10),
                Constraint::Length(16),
                Constraint::Length(10),
                Constraint::Length(22),
                Constraint::Fill(1),
            ],
        ),
        1 => (
            vec!["", "Column", "Type", "Missing", "Findings"],
            vec![
                Constraint::Length(1),
                Constraint::Length(20),
                Constraint::Length(8),
                Constraint::Length(16),
                Constraint::Fill(1),
            ],
        ),
        _ => (
            vec!["", "Column", "Findings"],
            vec![
                Constraint::Length(1),
                Constraint::Length(16),
                Constraint::Fill(1),
            ],
        ),
    };
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(dimmed))
        .row_highlight_style(config.theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);
}

fn render_segments(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .split(area);
    let column = results.columns.get(config.column_index);
    let mut heading = format!(
        "SEGMENTS  /  {}  /  {}",
        column.map(|profile| profile.name.as_str()).unwrap_or("-"),
        config.metric.label()
    );
    if config.plan.comparison == QualityComparison::Previous
        && matches!(
            config.plan.grain,
            QualityGrain::File | QualityGrain::Partition(_)
        )
    {
        heading.push_str("  /  PREVIOUS NEEDS ORDER");
    }
    render_section_title(&heading, sections[0], config.theme, buf);
    let layout = if sections[1].width >= 150 {
        3
    } else if sections[1].width >= 110 {
        2
    } else if sections[1].width >= 72 {
        1
    } else {
        0
    };
    let rows = results.segments.iter().map(|segment| {
        let value = segment_metric_value(segment, config.column_index, config.metric);
        let metric = metric_label(value);
        let compared = segment
            .compared_with
            .as_ref()
            .and_then(|label| results.segments.iter().find(|other| &other.label == label));
        let change =
            value
                .zip(compared.and_then(|other| {
                    segment_metric_value(other, config.column_index, config.metric)
                }))
                .map(|(current, prior)| format!("{:+.2} pp", (current - prior) * 100.0))
                .unwrap_or_else(|| "-".to_string());
        let largest = segment
            .largest_change
            .clone()
            .unwrap_or_else(|| "-".to_string());
        Row::new(match layout {
            3 => vec![
                segment.label.clone(),
                segment
                    .total_rows
                    .map(numfmt::group_chrome)
                    .unwrap_or_else(|| "unknown".to_string()),
                numfmt::group_chrome(segment.evaluated_rows),
                metric.clone(),
                segment
                    .compared_with
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                change,
                format!("{:.1}%", segment.null_rate * 100.0),
                largest,
            ],
            2 => vec![
                segment.label.clone(),
                numfmt::group_chrome(segment.evaluated_rows),
                metric.clone(),
                segment
                    .compared_with
                    .clone()
                    .unwrap_or_else(|| "-".to_string()),
                change,
                largest,
            ],
            1 => vec![segment.label.clone(), metric.clone(), change, largest],
            _ => vec![segment.label.clone(), metric, change],
        })
    });
    normalize_selection(table_state, results.segments.len());
    let (headers, widths) = match layout {
        3 => (
            vec![
                "Segment",
                "Total rows",
                "Evaluated",
                "Selected metric",
                "Compared with",
                "Delta",
                "All-null rate",
                "Largest change",
            ],
            vec![
                Constraint::Length(22),
                Constraint::Length(14),
                Constraint::Length(14),
                Constraint::Length(16),
                Constraint::Length(20),
                Constraint::Length(12),
                Constraint::Length(14),
                Constraint::Fill(1),
            ],
        ),
        2 => (
            vec![
                "Segment",
                "Evaluated",
                "Selected metric",
                "Compared with",
                "Delta",
                "Largest change",
            ],
            vec![
                Constraint::Length(22),
                Constraint::Length(14),
                Constraint::Length(16),
                Constraint::Length(20),
                Constraint::Length(12),
                Constraint::Fill(1),
            ],
        ),
        1 => (
            vec!["Segment", "Metric", "Delta", "Largest change"],
            vec![
                Constraint::Length(20),
                Constraint::Length(10),
                Constraint::Length(10),
                Constraint::Fill(1),
            ],
        ),
        _ => (
            vec!["Segment", "Metric", "Delta"],
            vec![
                Constraint::Fill(1),
                Constraint::Length(12),
                Constraint::Length(14),
            ],
        ),
    };
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(Style::default().fg(config.theme.get("dimmed"))))
        .row_highlight_style(config.theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);
}

fn segment_metric_value(
    segment: &crate::data_quality::SegmentQualityProfile,
    column_index: usize,
    metric: QualityMetric,
) -> Option<f64> {
    metric.value(segment.columns.get(column_index)?)
}

fn metric_label(value: Option<f64>) -> String {
    value
        .map(|value| format!("{:.1}%", value * 100.0))
        .unwrap_or_else(|| "-".to_string())
}

fn render_trends(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let ordered = matches!(
        config.plan.grain,
        QualityGrain::RowChunks(_) | QualityGrain::TimeWindows { .. }
    );
    let show_trend = ordered && results.segments.len() > 1;
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(if show_trend { 2 } else { 0 }),
            Constraint::Length(if show_trend { 4 } else { 0 }),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .margin(1)
        .split(area);
    if show_trend {
        let column = results
            .columns
            .get(config.column_index)
            .map(|profile| profile.name.as_str())
            .unwrap_or("-");
        render_section_title(
            &format!("{}  /  {} ACROSS SEGMENTS", column, config.metric.label()),
            sections[0],
            config.theme,
            buf,
        );
        render_metric_trend(results, config, sections[1], buf);
    }
    render_section_title("LIFECYCLE LATENCY", sections[2], config.theme, buf);
    if results.temporal.is_empty() {
        Paragraph::new("No lifecycle path. Assign Time roles to see latency measurements.")
            .alignment(Alignment::Center)
            .style(Style::default().fg(config.theme.get("text_primary")))
            .render(sections[3], buf);
        return;
    }
    let layout = if sections[3].width >= 152 {
        2
    } else if sections[3].width >= 72 {
        1
    } else {
        0
    };
    let rows = results.temporal.iter().map(|profile| {
        let interval = format!(
            "{} -> {}",
            profile.start_role.label(),
            profile.end_role.label()
        );
        Row::new(match layout {
            2 => vec![
                profile.segment.clone(),
                interval,
                numfmt::group_chrome(profile.evaluated_rows),
                format!("{} / {}", profile.missing_start, profile.missing_end),
                numfmt::group_chrome(profile.negative_count),
                duration_label(profile.p50_seconds),
                duration_label(profile.p90_seconds),
                duration_label(profile.p95_seconds),
                duration_label(profile.p99_seconds),
                duration_label(profile.max_seconds),
            ],
            1 => vec![
                profile.segment.clone(),
                interval,
                numfmt::group_chrome(profile.negative_count),
                duration_label(profile.p50_seconds),
                duration_label(profile.p95_seconds),
            ],
            _ => vec![
                interval,
                duration_label(profile.p50_seconds),
                numfmt::group_chrome(profile.negative_count),
            ],
        })
    });
    let (headers, widths) = match layout {
        2 => (
            vec![
                "Segment",
                "Interval",
                "Evaluated",
                "Missing s/e",
                "Negative",
                "p50",
                "p90",
                "p95",
                "p99",
                "Max",
            ],
            vec![
                Constraint::Length(20),
                Constraint::Length(28),
                Constraint::Length(12),
                Constraint::Length(13),
                Constraint::Length(10),
                Constraint::Length(11),
                Constraint::Length(11),
                Constraint::Length(11),
                Constraint::Length(11),
                Constraint::Fill(1),
            ],
        ),
        1 => (
            vec!["Segment", "Interval", "Negative", "p50", "p95"],
            vec![
                Constraint::Length(22),
                Constraint::Fill(1),
                Constraint::Length(9),
                Constraint::Length(9),
                Constraint::Length(9),
            ],
        ),
        _ => (
            vec!["Interval", "p50", "Negative"],
            vec![
                Constraint::Fill(1),
                Constraint::Length(9),
                Constraint::Length(9),
            ],
        ),
    };
    normalize_selection(table_state, results.temporal.len());
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(Style::default().fg(config.theme.get("dimmed"))))
        .row_highlight_style(config.theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[3], buf, table_state);
}

fn render_metric_trend(
    results: &DataQualityResults,
    config: &DataQualityWidgetConfig<'_>,
    area: Rect,
    buf: &mut Buffer,
) {
    let max_bars = area.width.saturating_sub(4) as usize;
    if max_bars == 0 {
        return;
    }
    let count = results.segments.len().min(max_bars);
    if !results
        .segments
        .iter()
        .take(count)
        .any(|segment| segment_metric_value(segment, config.column_index, config.metric).is_some())
    {
        Paragraph::new("No values for this column/measurement in the displayed segments.")
            .style(Style::default().fg(config.theme.get("dimmed")))
            .render(area, buf);
        return;
    }
    let max_rate = results
        .segments
        .iter()
        .take(count)
        .filter_map(|segment| segment_metric_value(segment, config.column_index, config.metric))
        .fold(0.01_f64, f64::max);
    let bars = results
        .segments
        .iter()
        .take(count)
        .map(|segment| {
            segment_metric_value(segment, config.column_index, config.metric)
                .map(|value| {
                    let level = (value / max_rate * 7.0).round().clamp(0.0, 7.0) as usize;
                    glyphs::get().mini_bars[level]
                })
                .unwrap_or(" ")
        })
        .collect::<String>();
    let first = &results.segments[0];
    let last = &results.segments[count - 1];
    Paragraph::new(vec![
        Line::styled(bars, Style::default().fg(config.theme.get("accent"))),
        Line::raw(format!(
            "{} {}  ->  {} {}",
            first.label,
            metric_label(segment_metric_value(
                first,
                config.column_index,
                config.metric
            )),
            last.label,
            metric_label(segment_metric_value(
                last,
                config.column_index,
                config.metric
            ))
        )),
        Line::styled(
            if count < results.segments.len() {
                format!(
                    "First {count} of {} ordered segments; scale 0..{:.1}%",
                    results.segments.len(),
                    max_rate * 100.0
                )
            } else {
                format!(
                    "{count} ordered segments; scale 0..{:.1}%",
                    max_rate * 100.0
                )
            },
            Style::default().fg(config.theme.get("dimmed")),
        ),
    ])
    .render(area, buf);
}

fn duration_label(seconds: Option<i64>) -> String {
    let Some(seconds) = seconds else {
        return "-".to_string();
    };
    let sign = if seconds < 0 { "-" } else { "" };
    let seconds = seconds.unsigned_abs();
    if seconds >= 86_400 {
        format!("{sign}{:.1}d", seconds as f64 / 86_400.0)
    } else if seconds >= 3_600 {
        format!("{sign}{:.1}h", seconds as f64 / 3_600.0)
    } else if seconds >= 60 {
        format!("{sign}{:.1}m", seconds as f64 / 60.0)
    } else {
        format!("{sign}{seconds}s")
    }
}

fn render_detail(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let index = table_state.selected().unwrap_or(0);
    let Some(profile) = results.columns.get(index) else {
        Paragraph::new("Select a column first.")
            .alignment(Alignment::Center)
            .render(area, buf);
        return;
    };
    // The column's findings first, in the report's own words; the measurements
    // under them are the evidence.
    let report = build_report(results);
    // The frame's title names the column.
    let mut text = vec![Line::styled(
        format!("Type: {}", profile.dtype),
        Style::default().fg(config.theme.get("dimmed")),
    )];
    let findings = report
        .findings
        .iter()
        .filter(|finding| finding.kind.is_some() && finding.columns.contains(&profile.name))
        .collect::<Vec<_>>();
    if findings.is_empty() {
        text.push(Line::from(vec![
            severity_mark(Severity::Clean, config.theme),
            Span::raw(" No findings"),
        ]));
    }
    for finding in findings {
        text.push(Line::from(vec![
            severity_mark(finding.severity, config.theme),
            Span::raw(format!(" {}: {}", finding.title, finding.summary)),
        ]));
    }
    text.push(Line::raw(""));
    text.extend([
        Line::raw(format!(
            "Evaluated: {} ({})",
            numfmt::group_chrome(profile.evaluated_rows),
            results.precision.label()
        )),
        Line::raw(format!(
            "Missing: {} / {} ({:.2}%)",
            numfmt::group_chrome(profile.null_count),
            numfmt::group_chrome(profile.evaluated_rows),
            profile.null_rate() * 100.0
        )),
        Line::raw(format!(
            "Distinct: {}",
            profile
                .distinct_count
                .map(numfmt::group_chrome)
                .unwrap_or_else(|| "-".to_string())
        )),
    ]);
    // A measurement that does not apply to this type is left out rather than
    // printed as a dash, so what is on screen was actually measured.
    if profile.min.is_some() || profile.max.is_some() {
        text.push(Line::raw(format!(
            "Range: {} .. {}",
            profile.min.as_deref().unwrap_or("-"),
            profile.max.as_deref().unwrap_or("-")
        )));
    }
    if let Some((value, count)) = profile.dominant_value.as_ref().zip(profile.dominant_count) {
        text.push(Line::raw(format!(
            "Most common: {value:?}, {} {}",
            numfmt::group_chrome(count),
            if count == 1 { "row" } else { "rows" }
        )));
    }
    if profile.min_length.is_some() || profile.max_length.is_some() {
        text.push(Line::raw(format!(
            "{} length: {} .. {}",
            if matches!(profile.dtype, polars::prelude::DataType::List(_)) {
                "List"
            } else {
                "Text"
            },
            count_label(profile.min_length),
            count_label(profile.max_length)
        )));
    }
    if profile.integer_parse_count.is_some()
        || profile.decimal_parse_count.is_some()
        || profile.date_parse_count.is_some()
        || profile.datetime_parse_count.is_some()
    {
        text.push(Line::raw(format!(
            "Text parses: integer {}  decimal {}  date {}  datetime {}",
            count_label(profile.integer_parse_count),
            count_label(profile.decimal_parse_count),
            count_label(profile.date_parse_count),
            count_label(profile.datetime_parse_count),
        )));
    }
    text.push(Line::raw(""));
    text.push(Line::styled(
        coverage(results, config.plan),
        Style::default().fg(config.theme.get("dimmed")),
    ));
    for group in results
        .category_variants
        .iter()
        .filter(|group| group.column == profile.name)
        .take(3)
    {
        text.push(Line::raw(format!(
            "Category {:?}: {}",
            group.normalized,
            group
                .variants
                .iter()
                .map(|(value, count)| format!("{value:?} ({count})"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    Paragraph::new(text)
        .block(
            Block::default()
                .title(profile.name.as_str())
                .borders(Borders::ALL)
                .border_set(crate::glyphs::get().border)
                .border_style(Style::default().fg(config.theme.get("modal_border_active"))),
        )
        .render(area, buf);
}

fn count_label(value: Option<usize>) -> String {
    value
        .map(numfmt::group_chrome)
        .unwrap_or_else(|| "-".to_string())
}

fn render_sidebar(
    config: &DataQualityWidgetConfig<'_>,
    sidebar_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1), Constraint::Length(7)])
        .split(area);
    let tools = [
        ("Describe", AnalysisTool::Describe),
        ("Distribution Analysis", AnalysisTool::DistributionAnalysis),
        ("Correlation Matrix", AnalysisTool::CorrelationMatrix),
        ("Data Quality", AnalysisTool::DataQuality),
    ];
    let items = tools.iter().enumerate().map(|(index, (name, tool))| {
        let focused =
            config.focus == AnalysisFocus::Sidebar && sidebar_state.selected() == Some(index);
        let selected = *tool == AnalysisTool::DataQuality;
        let g = crate::glyphs::get();
        // The middot marks the applied choice, as in every Picker; the rail
        // and tint stay with focus.
        let prefix = if selected {
            format!("{} ", g.middot)
        } else {
            "  ".to_string()
        };
        ListItem::new(format!("{prefix}{name}")).style(if focused {
            config.theme.highlight_style()
        } else {
            Style::default().fg(config.theme.get("text_primary"))
        })
    });
    Widget::render(
        List::new(items).block(
            Block::default()
                .title("Analysis Tools")
                .borders(Borders::ALL)
                .border_set(crate::glyphs::get().border)
                .border_style(Style::default().fg(config.theme.get("modal_border"))),
        ),
        parts[0],
        buf,
    );

    // The plan strip above already carries the planned access, so this panel
    // reports what the finished run actually did instead of repeating it.
    // The panel is 18 columns wide beside a medium terminal, so it says less
    // there rather than cutting a number in half.
    let roomy = parts[1].width >= 26;
    let written = if roomy {
        "0 B remote write"
    } else {
        "0 B written"
    };
    // A section heading on a rule, not another box: the sidebar already sits
    // inside the screen's chrome, and a nested border spent two columns the
    // 18-wide panel did not have.
    let heading = |title: &str| {
        let g = crate::glyphs::get();
        let width = parts[1].width as usize;
        let fill = width.saturating_sub(crate::glyphs::display_width(title) + 1);
        Line::from(vec![
            Span::styled(
                title.to_string(),
                Style::default().fg(config.theme.get("accent")),
            ),
            Span::styled(
                format!(" {}", g.rule_h.repeat(fill)),
                Style::default().fg(config.theme.get("modal_border")),
            ),
        ])
    };
    let lines = match config.results {
        // The verdict in counts, kept in view on the pages that do not lead with it.
        // The strip above says what was measured.
        Some(results) => {
            let report = build_report(results);
            let count = |severity: Severity, value: usize, one: &str, many: &str| {
                // None of a kind is good news, whatever the kind.
                let severity = if value == 0 {
                    Severity::Clean
                } else {
                    severity
                };
                Line::from(vec![
                    severity_mark(severity, config.theme),
                    Span::raw(format!(
                        " {} {}",
                        numfmt::group_chrome(value),
                        if value == 1 { one } else { many }
                    )),
                ])
            };
            let mut lines = vec![heading("Result")];
            if report.metadata_only {
                if report.problems > 0 {
                    lines.push(count(
                        Severity::Problem,
                        report.problems,
                        "problem",
                        "problems",
                    ));
                }
                lines.push(Line::raw("values not read"));
            } else {
                lines.push(count(
                    Severity::Problem,
                    report.problems,
                    "problem",
                    "problems",
                ));
                lines.push(count(Severity::Note, report.notes, "note", "notes"));
                lines.push(count(
                    Severity::Clean,
                    report.clean_columns,
                    "clean column",
                    if roomy { "clean columns" } else { "clean" },
                ));
            }
            lines
        }
        None => vec![
            heading("Planned"),
            Line::raw(format!(
                "{} rows",
                planned_rows(config.state, config.plan)
                    .map(numfmt::group_chrome)
                    .unwrap_or_else(|| "unknown".to_string())
            )),
            Line::raw(match (config.state.is_remote_source(), roomy) {
                (true, true) => "remote -> this machine",
                (true, false) => "remote read",
                (false, _) => "local read",
            }),
            Line::styled(written, Style::default().fg(config.theme.get("success"))),
        ],
    };
    Paragraph::new(lines).render(parts[1], buf);
}

fn render_narrow_tool_picker(
    config: &DataQualityWidgetConfig<'_>,
    sidebar_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let popup = centered_rect(32, 8, area);
    Clear.render(popup, buf);
    let tools = [
        "Describe",
        "Distribution Analysis",
        "Correlation Matrix",
        "Data Quality",
    ];
    let items = tools
        .iter()
        .enumerate()
        .map(|(index, label)| {
            ListItem::new(*label).style(if sidebar_state.selected() == Some(index) {
                config.theme.highlight_style()
            } else {
                Style::default().fg(config.theme.get("text_primary"))
            })
        })
        .collect::<Vec<_>>();
    Widget::render(
        List::new(items).block(
            Block::default()
                .title("Analysis Tools")
                .borders(Borders::ALL)
                .border_set(crate::glyphs::get().border)
                .border_style(Style::default().fg(config.theme.get("accent"))),
        ),
        popup,
        buf,
    );
}

fn render_access_plan(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let popup = centered_rect(72, 16, area);
    Clear.render(popup, buf);
    let rows = planned_rows(config.state, config.plan);
    let bytes = planned_read_label(config.state, config.plan);
    let rows_label = match rows {
        Some(rows) => numfmt::group_chrome(rows),
        None if config.plan.samples_each_segment() => format!(
            "unknown; {} per segment, refused over {}",
            numfmt::group_chrome(config.plan.sample_rows.min(50_000)),
            numfmt::group_chrome(MAX_RETAINED_SAMPLE_ROWS)
        ),
        None => "unknown".to_string(),
    };
    let source = if config.state.is_remote_source() {
        "remote source"
    } else {
        "local source"
    };
    let request_count = if config.state.is_remote_source() {
        "unknown".to_string()
    } else {
        "none".to_string()
    };
    let table_rows = vec![
        Row::new(vec![Cell::from("Source"), Cell::from(source)]),
        Row::new(vec![
            Cell::from("Scope"),
            Cell::from(config.plan.scope.label()),
        ]),
        Row::new(vec![
            Cell::from("Grain"),
            Cell::from(config.plan.grain.label()),
        ]),
        Row::new(vec![
            Cell::from("Compute"),
            Cell::from(compute_label(config.plan)),
        ]),
        Row::new(vec![Cell::from("Rows evaluated"), Cell::from(rows_label)]),
        Row::new(vec![
            Cell::from("Value reads"),
            Cell::from(if config.state.is_remote_source() {
                "unknown".to_string()
            } else {
                bytes
            }),
        ]),
        Row::new(vec![Cell::from("Requests"), Cell::from(request_count)]),
        Row::new(vec![
            Cell::from("Known source files"),
            Cell::from(
                (if config.plan.scope.uses_source() {
                    let count = config.state.quality_source_file_count();
                    if count > 0 {
                        Some(count)
                    } else {
                        config.state.source_file_count()
                    }
                } else {
                    config.state.source_file_count()
                })
                .map(numfmt::group_chrome)
                .unwrap_or_else(|| "unknown".to_string()),
            ),
        ]),
        Row::new(vec![
            Cell::from("Conflict values"),
            Cell::from(match config.state.quality_conflict_reads() {
                0 => "none: no file holds a column in an unreadable type".to_string(),
                reads if config.plan.compute == QualityCompute::Full => {
                    format!("{reads} extra one-column file reads")
                }
                reads => format!("not read; a full scan would add {reads} one-column file reads"),
            }),
        ]),
        Row::new(vec![Cell::from("Remote writes"), Cell::from("none")]),
        Row::new(vec![Cell::from("Local file writes"), Cell::from("none")]),
        Row::new(vec![
            Cell::from("Estimate basis"),
            Cell::from(if config.plan.samples_each_segment() {
                "full scope read; refused over 500,000 rows or 512 MiB retained"
            } else {
                "spread sample; at most one read of the scope"
            }),
        ]),
    ];
    let table = Table::new(table_rows, [Constraint::Length(20), Constraint::Fill(1)]).block(
        Block::default()
            .title("Access Plan")
            .borders(Borders::ALL)
            .border_set(crate::glyphs::get().border)
            .border_style(Style::default().fg(config.theme.get("accent"))),
    );
    Widget::render(table, popup, buf);
}

fn render_run_confirmation(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let popup = centered_rect(72, 10, area);
    Clear.render(popup, buf);
    Paragraph::new(vec![
        Line::styled(
            if config.plan.samples_each_segment() {
                "Full read for per-segment sampling"
            } else {
                "Full value scan"
            },
            Style::default()
                .fg(config.theme.get("warning"))
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(if config.plan.samples_each_segment() {
            "Every eligible row is read; the budget is kept per segment. If that \
             would total over 500,000 rows or 512 MiB the run is refused, not trimmed."
        } else {
            "This plan evaluates every eligible row and may read the full source."
        }),
        Line::raw("The source remains read-only; remote writes are 0 B."),
        Line::raw(""),
        Line::raw("Enter run    Esc cancel"),
    ])
    // The warning is the whole point of the dialog, so wrap it rather than cut it.
    .wrap(Wrap { trim: true })
    .block(
        Block::default()
            .title("Confirm Access")
            .borders(Borders::ALL)
            .border_set(crate::glyphs::get().border)
            .border_style(Style::default().fg(config.theme.get("warning"))),
    )
    .render(popup, buf);
}

fn render_running(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let popup = centered_rect(58, 7, area);
    Clear.render(popup, buf);
    Paragraph::new(vec![
        Line::styled(
            "Profiling data quality",
            Style::default()
                .fg(config.theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw("The declared plan is running off the UI thread."),
        Line::raw("Esc cancels installation of its result."),
    ])
    .block(
        Block::default()
            .title("Running")
            .borders(Borders::ALL)
            .border_set(crate::glyphs::get().border)
            .border_style(Style::default().fg(config.theme.get("accent"))),
    )
    .render(popup, buf);
}

fn render_section_title(title: &str, area: Rect, theme: &Theme, buf: &mut Buffer) {
    let line = format!(
        "{title} {}",
        glyphs::get().rule_h.repeat(area.width as usize)
    );
    Paragraph::new(line)
        .style(
            Style::default()
                .fg(theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        )
        .render(area, buf);
}

fn render_run_prompt(area: Rect, theme: &Theme, buf: &mut Buffer) {
    Paragraph::new("Return to Plan and run it to create a profile.")
        .alignment(Alignment::Center)
        .style(Style::default().fg(theme.get("text_primary")))
        .render(area, buf);
}

fn normalize_selection(state: &mut TableState, len: usize) {
    if len == 0 {
        state.select(None);
    } else if state.selected().is_none_or(|selected| selected >= len) {
        state.select(Some(0));
    }
}

fn planned_rows(state: &DataTableState, plan: &DataQualityPlan) -> Option<usize> {
    if plan.compute == QualityCompute::Metadata {
        return Some(0);
    }
    let total = match &plan.scope {
        QualityScope::CurrentView => state.num_rows_if_valid()?,
        QualityScope::FirstRows(rows) => state.num_rows_if_valid()?.min(*rows),
        QualityScope::ViewRows { start, end } => {
            let rows = state.num_rows_if_valid()?;
            rows.min(*end).saturating_sub(start.saturating_sub(1))
        }
        _ => return None,
    };
    match plan.compute {
        QualityCompute::Metadata => unreachable!(),
        QualityCompute::Sample if plan.samples_each_segment() => None,
        QualityCompute::Sample => Some(total.min(plan.sample_rows.min(50_000))),
        QualityCompute::Full => Some(total),
    }
}

fn planned_read_bytes(state: &DataTableState, plan: &DataQualityPlan) -> Option<usize> {
    if plan.scope.uses_source() || plan.samples_each_segment() {
        return None;
    }
    let rows = match plan.compute {
        QualityCompute::Metadata => 0,
        // A ceiling: the sample is spread across the whole scope, which one Parquet or
        // IPC file serves in a few dozen short runs and anything else in one stream.
        QualityCompute::Sample => planned_scope_rows(state, plan)?,
        QualityCompute::Full => return None,
    };
    Some(rows.saturating_mul(state.estimated_row_bytes()))
}

fn planned_scope_rows(state: &DataTableState, plan: &DataQualityPlan) -> Option<usize> {
    let rows = state.num_rows_if_valid()?;
    Some(match &plan.scope {
        QualityScope::FirstRows(limit) => rows.min(*limit),
        QualityScope::ViewRows { start, end } => {
            rows.min(*end).saturating_sub(start.saturating_sub(1))
        }
        QualityScope::CurrentView => rows,
        _ => return None,
    })
}

/// The planned read in words: a sample of a scope larger than itself reads at most
/// the scope, and how much less depends on the source.
fn planned_read_label(state: &DataTableState, plan: &DataQualityPlan) -> String {
    let bytes = planned_read_bytes(state, plan);
    let sampled = plan.compute == QualityCompute::Sample
        && planned_scope_rows(state, plan).is_some_and(|rows| rows > plan.sample_rows.min(50_000));
    match bytes {
        Some(bytes) if sampled => format!(
            "up to {}",
            approximate_bytes(bytes).trim_start_matches("about ")
        ),
        other => approximate_bytes_option(other),
    }
}

fn approximate_bytes_option(bytes: Option<usize>) -> String {
    bytes
        .map(approximate_bytes)
        .unwrap_or_else(|| "unknown".to_string())
}

fn compute_label(plan: &DataQualityPlan) -> String {
    match plan.compute {
        QualityCompute::Metadata => "metadata only".to_string(),
        QualityCompute::Sample => format!(
            "{} rows{} / seed {}",
            numfmt::group_chrome(plan.sample_rows.min(50_000)),
            if plan.samples_each_segment() {
                "/segment"
            } else {
                ""
            },
            plan.sample_seed
        ),
        QualityCompute::Full => "full scan".to_string(),
    }
}

fn approximate_bytes(bytes: usize) -> String {
    const KIB: f64 = 1024.0;
    const MIB: f64 = KIB * 1024.0;
    const GIB: f64 = MIB * 1024.0;
    let bytes = bytes as f64;
    if bytes >= GIB {
        format!("about {:.1} GiB", bytes / GIB)
    } else if bytes >= MIB {
        format!("about {:.1} MiB", bytes / MIB)
    } else if bytes >= KIB {
        format!("about {:.1} KiB", bytes / KIB)
    } else {
        format!("about {} B", bytes as usize)
    }
}

fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
    let width = width.min(area.width.saturating_sub(2)).max(1);
    let height = height.min(area.height.saturating_sub(2)).max(1);
    Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 2,
        width,
        height,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn byte_estimates_use_binary_units() {
        assert_eq!(approximate_bytes(512), "about 512 B");
        assert_eq!(approximate_bytes(2 * 1024), "about 2.0 KiB");
        assert_eq!(approximate_bytes(3 * 1024 * 1024), "about 3.0 MiB");
    }
}
