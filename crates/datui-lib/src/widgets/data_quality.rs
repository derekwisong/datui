use crate::analysis_modal::{AnalysisFocus, AnalysisTool, DetailScroll};
use crate::config::Theme;
use crate::data_quality::{
    DataQualityPlan, DataQualityResults, ObservationKind, QualityComparison, QualityCompute,
    QualityGrain, QualityMetric, QualityPage, QualityPrecision, QualityScope, TemporalRole,
};
use crate::glyphs;
use crate::numfmt;
use crate::quality_report::{
    CHECKS_SHOWN, Check, Outcome, QualityReport, Severity, advice, build_report, checks, describe,
    verdict,
};
use crate::widgets::datatable::DataTableState;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, Borders, Cell, Clear, List, ListItem, Paragraph, Row, StatefulWidget, Table, TableState,
    Tabs, Widget, Wrap,
};

pub struct DataQualityWidgetConfig<'a> {
    /// The Sample form is this pane until the first run: nothing of the plan or the
    /// report is drawn behind it, so there is one thing to look at.
    pub first_run: bool,
    /// The clean entry's checks table shows every check, not the first few.
    pub checks_expanded: bool,
    pub state: &'a DataTableState,
    pub plan: &'a DataQualityPlan,
    /// The plan the result on screen was measured with; the header says this one
    /// even while the Plan page edits another.
    pub measured: &'a DataQualityPlan,
    pub results: Option<&'a DataQualityResults>,
    pub from_cache: bool,
    pub metric: QualityMetric,
    pub column_index: usize,
    pub segment_index: usize,
    pub segments_by_change: bool,
    pub page: QualityPage,
    /// The plan has been edited since the result on screen was measured.
    pub pending: bool,
    pub plan_field: usize,
    pub show_access: bool,
    pub observation_detail: bool,
    pub confirm_run: bool,
    pub focus: AnalysisFocus,
    pub theme: &'a Theme,
}

/// The tool list's width: the width every analysis tool gives it, and none on a
/// terminal too narrow to hold it beside the result (a picker pops up instead).
fn data_quality_sidebar_width(width: u16) -> u16 {
    if width >= 76 {
        crate::widgets::analysis::sidebar_width(width)
    } else {
        0
    }
}

/// Where the result goes: under the header and the page tabs, left of the tool list.
/// The Sample form fills it before the first run.
pub(crate) fn main_pane(area: Rect) -> Rect {
    Rect {
        y: area.y + 2,
        height: area.height.saturating_sub(2),
        width: area
            .width
            .saturating_sub(data_quality_sidebar_width(area.width)),
        ..area
    }
}

pub fn render(
    config: DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    sidebar_state: &mut TableState,
    detail_scroll: &mut DetailScroll,
    area: Rect,
    buf: &mut Buffer,
) {
    let sidebar_width = data_quality_sidebar_width(area.width);
    let vertical = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .split(area);

    render_header(&config, vertical[0], buf);
    if !config.first_run {
        render_tabs(&config, vertical[1], buf);
    }

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

    if config.first_run {
        // The Sample form draws itself centered in `main_pane`; the pane stays empty.
        if sidebar_width == 0 && config.focus == AnalysisFocus::Sidebar {
            render_narrow_tool_picker(&config, sidebar_state, area, buf);
        }
        return;
    }
    match config.page {
        QualityPage::Plan => render_plan(&config, table_state, body, buf),
        QualityPage::TimeRoles => render_time_roles(&config, table_state, body, buf),
        QualityPage::Overview => render_overview(&config, table_state, body, buf),
        QualityPage::Columns => render_columns(&config, table_state, body, buf),
        QualityPage::Segments => render_segments(&config, table_state, body, buf),
        QualityPage::SegmentDetail => {
            render_segment_detail(&config, table_state, config.segment_index, body, buf)
        }
        QualityPage::Trends => render_trends(&config, table_state, body, buf),
        QualityPage::Detail => render_detail(&config, table_state, body, buf),
    }

    if config.show_access {
        render_access_plan(&config, area, buf);
    } else if config.observation_detail {
        render_finding_detail(&config, table_state, detail_scroll, area, buf);
    } else if config.confirm_run {
        render_run_confirmation(&config, area, buf);
    } else if sidebar_width == 0 && config.focus == AnalysisFocus::Sidebar {
        render_narrow_tool_picker(&config, sidebar_state, area, buf);
    }
}

/// One line, as every analysis tool heads its result: the tool, then what the
/// numbers were measured on.
fn render_header(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let middot = glyphs::get().middot;
    let mut text = "Data Quality".to_string();
    if let Some(results) = config.results.filter(|_| !config.first_run) {
        let plan = config.measured;
        text.push_str(&format!(" {middot} {}", measured_on(plan, results)));
        if plan.grain != QualityGrain::Dataset {
            text.push_str(&format!(" {middot} {}", plan.grain.label()));
        }
        if plan.comparison != QualityComparison::None {
            text.push_str(&format!(
                " {middot} compared with {}",
                plan.comparison_label()
            ));
        }
    }
    let mut spans = vec![Span::raw(text)];
    if config.from_cache {
        spans.push(Span::styled(
            "  [session cache]",
            Style::default().fg(config.theme.get("dimmed")),
        ));
    }
    Paragraph::new(Line::from(spans))
        .style(crate::widgets::analysis::header_style(
            config.theme,
            "controls_bg",
            "table_header",
        ))
        .render(area, buf);
}

/// The rows a result was measured on, in the words the other tools' headers use.
fn measured_on(plan: &DataQualityPlan, results: &DataQualityResults) -> String {
    let sample = plan.sample();
    let scope = if plan.scope == QualityScope::CurrentView {
        String::new()
    } else {
        format!(" {} {}", glyphs::get().middot, plan.scope.label())
    };
    match (results.precision, results.total_rows) {
        (QualityPrecision::Metadata, _) => format!("file metadata only, no values read{scope}"),
        (QualityPrecision::Exact, _) => sample.outcome(results.evaluated_rows, None, None),
        (QualityPrecision::Sampled | QualityPrecision::Estimated, Some(total)) => {
            sample.outcome(total, Some(results.evaluated_rows), results.per_value)
        }
        (QualityPrecision::Sampled | QualityPrecision::Estimated, None) => format!(
            "sample of {} rows{scope}",
            numfmt::group_chrome(results.evaluated_rows)
        ),
    }
}

/// The pages, with the one shown carrying the accent. `←→` walk them.
fn render_tabs(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let shown = config.page.tab();
    Tabs::new(QualityPage::TABS.iter().map(|page| page.title()))
        .style(Style::default().fg(config.theme.get("dimmed")))
        .highlight_style(
            Style::default()
                .fg(config.theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        )
        .select(QualityPage::TABS.iter().position(|page| *page == shown))
        .divider(" ")
        .render(area, buf);
}
fn render_plan(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let plan = config.plan;
    let has_time_columns = !config
        .state
        .quality_temporal_columns(&plan.scope)
        .is_empty();
    let temporal = if !has_time_columns {
        "none: no date or time columns".to_string()
    } else if plan.temporal_roles.is_empty() {
        "none".to_string()
    } else {
        format!(
            "{}: {}",
            plan.temporal_roles.len(),
            plan.temporal_roles
                .iter()
                .map(|item| format!("{}={}", item.role.label(), item.column))
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let comparison = match (plan.comparison, plan.baseline_segment.as_deref()) {
        (QualityComparison::Baseline, Some(segment)) => format!("baseline {segment}"),
        (comparison, _) => comparison.choice_label().to_string(),
    };
    let mut rows = vec![
        ("Sample", plan.sample().summary()),
        ("Grain", plan.grain.label()),
        (
            "Values",
            if plan.compute == QualityCompute::Metadata {
                "file metadata only".to_string()
            } else {
                "read".to_string()
            },
        ),
        ("Compare", comparison),
        ("Time roles", temporal),
    ];
    // An interval needs two roles; until then the threshold has nothing to apply to.
    if plan.temporal_roles.len() >= 2 {
        rows.push((
            "Latency threshold",
            plan.latency_threshold_seconds
                .map(|seconds| duration_label(Some(seconds)))
                .unwrap_or_else(|| "none".to_string()),
        ));
    }
    let plan_rows = rows.len() as u16;
    // The plan is the page; the access summary takes what is left, and `p` has it
    // in full on a terminal too short for both.
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(plan_rows),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .margin(1)
        .split(area);
    Paragraph::new(rule_line("Plan", None, sections[0].width, theme)).render(sections[0], buf);
    // The field under the cursor carries the rail while the page has the keys.
    if config.focus == AnalysisFocus::Main {
        table_state.select(Some(config.plan_field.min(rows.len() - 1)));
    } else {
        table_state.select(None);
    }
    let table = Table::new(
        rows.into_iter()
            .map(|(label, value)| Row::new(vec![Cell::from(label), Cell::from(value)])),
        [Constraint::Length(19), Constraint::Fill(1)],
    )
    .row_highlight_style(theme.highlight_style())
    .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);
    if config.pending {
        Paragraph::new(Line::styled(
            "  Changed since the last run: Enter runs it, Esc puts it back",
            Style::default().fg(theme.get("warning")),
        ))
        .render(
            Rect {
                y: sections[2].y + 1,
                height: 1,
                ..sections[2]
            },
            buf,
        );
    }

    let planned = planned_rows(config.state, plan)
        .map(numfmt::group_chrome)
        .unwrap_or_else(|| "unknown".to_string());
    let bytes_label = planned_read_label(config.state, plan);
    let access_rows = vec![
        Row::new(vec!["Rows evaluated".to_string(), planned]),
        Row::new(vec![
            if config.state.is_remote_source() {
                "Remote transfer".to_string()
            } else {
                "Local read".to_string()
            },
            bytes_label,
        ]),
        Row::new(vec!["Remote writes".to_string(), "none".to_string()]),
        Row::new(vec![
            "Session memory".to_string(),
            "profile; size unknown".to_string(),
        ]),
    ];
    let access_area = sections[3];
    Paragraph::new(rule_line("Access", None, access_area.width, theme)).render(access_area, buf);
    let access =
        Table::new(access_rows, [Constraint::Length(21), Constraint::Fill(1)]).style(dimmed);
    Widget::render(
        access,
        Rect {
            y: access_area.y + 2,
            height: access_area.height.saturating_sub(2),
            ..access_area
        },
        buf,
    );
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
            "No values were read, so nothing about them is known. Set the plan's Values to read (e) to check them."
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

    // Mark, title, columns, summary. The title column fits the longest title and a
    // gap; the columns take a share of what is left, so the summary keeps the rest.
    let title_width = report
        .findings
        .iter()
        .map(|finding| glyphs::display_width(finding.title))
        .max()
        .unwrap_or(0)
        + 2;
    let width = area.width as usize;
    let lead = 4; // rail + space + mark + space
    let rest = width.saturating_sub(lead + title_width);
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
                        format!("{:<title_width$}", finding.title),
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
    let width = width as usize;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let g = glyphs::get();
    let shown = limit.unwrap_or(checks.len()).min(checks.len());
    let outcome = |check: &Check| match &check.outcome {
        Outcome::Passed => "passed".to_string(),
        Outcome::Found { detail, .. } => format!("{detail} flagged"),
        Outcome::NotRun(reason) => format!("not run: {reason}"),
    };
    // Each column as wide as its widest entry across every check, so showing all
    // of them moves nothing; what a check looks for takes the rest, and wraps under
    // itself rather than being cut.
    let column = |text: &dyn Fn(&Check) -> String| {
        checks
            .iter()
            .map(|check| glyphs::display_width(&text(check)))
            .max()
            .unwrap_or(0)
            + 2
    };
    let name_width = column(&|check| check.name.to_string());
    let reach_width = column(&|check| check.applies_to.clone());
    let outcome_width = column(&outcome);
    let lead = 2 + name_width + reach_width + outcome_width;
    // Too narrow for a readable fourth column: it goes on the lines below instead.
    let (looks_indent, looks_width) = if width >= lead + 24 {
        (lead, width - lead)
    } else {
        (2, width.saturating_sub(2).max(1))
    };
    let mut lines = vec![rule_line(
        "Checks",
        Some(&numfmt::group_chrome(checks.len())),
        width as u16,
        theme,
    )];
    for check in &checks[..shown] {
        let (mark, style) = match &check.outcome {
            Outcome::Passed => (
                Span::styled(g.check, Style::default().fg(theme.get("success"))),
                Style::default().fg(theme.get("text_primary")),
            ),
            Outcome::Found { tier, .. } => (
                severity_mark(*tier, theme),
                Style::default().fg(theme.get("text_primary")),
            ),
            Outcome::NotRun(_) => (Span::styled(g.dash, dimmed), dimmed),
        };
        let beside = looks_indent == lead;
        let mut spans = vec![
            mark,
            Span::raw(" "),
            Span::styled(format!("{:<name_width$}", check.name), style),
            Span::styled(format!("{:<reach_width$}", check.applies_to), dimmed),
            // Padded only when something follows it on the line: trailing spaces
            // past the frame would wrap into a blank line.
            Span::styled(
                if beside {
                    format!("{:<outcome_width$}", outcome(check))
                } else {
                    outcome(check)
                },
                style,
            ),
        ];
        let mut looks = crate::widgets::info::wrap_to(check.looks_for, looks_width).into_iter();
        if beside {
            spans.extend(looks.next().map(|text| Span::styled(text, dimmed)));
        }
        lines.push(Line::from(spans));
        lines.extend(
            looks.map(|text| Line::styled(format!("{}{text}", " ".repeat(looks_indent)), dimmed)),
        );
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
    scroll: &mut DetailScroll,
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
    let (headline, evidence) = describe(finding, results);
    // A reading surface: cap the measure on a wide terminal. The checks table on
    // the clean entry is a table, and may use more of the width.
    let width = area
        .width
        .saturating_sub(4)
        .min(if finding.kind.is_none() { 118 } else { 84 });
    let inner = width.saturating_sub(4).max(1);
    // The name and the columns on the frame; the facts as a list under it.
    let title = if finding.kind.is_none() {
        finding.title.to_string()
    } else {
        let name = format!("{} {} ", finding.title, glyphs::get().middot);
        let room = (width as usize).saturating_sub(glyphs::display_width(&name) + 4);
        format!("{name}{}", finding.columns_label(room))
    };
    let bullet = |text: String| {
        Line::from(vec![
            Span::styled(format!("{} ", glyphs::get().middot), dimmed),
            Span::raw(text),
        ])
    };
    let mut lines = Vec::new();
    // The frame cut the list short: the whole of it, unless the evidence lists
    // every column with its rate already.
    if finding.kind.is_some()
        && finding.columns.len() > 1
        && !finding.varied()
        && finding.columns_label(usize::MAX) != finding.columns_label(inner as usize)
    {
        lines.push(bullet(finding.columns.join(", ")));
    }
    lines.push(bullet(headline));
    lines.extend(
        evidence
            .into_iter()
            .map(|line| Line::raw(format!("  {line}"))),
    );
    lines.extend(advice(finding).into_iter().map(bullet));
    if finding.kind.is_none() {
        lines.push(Line::raw(""));
        lines.extend(check_lines(
            &checks(results, &report),
            inner,
            (!config.checks_expanded).then_some(CHECKS_SHOWN),
            theme,
        ));
    }
    if finding.kind.is_some() {
        // The count is known when the rows are one observation's, or the same rows
        // in every column; "any of these columns" is a union nobody counted.
        let counted = finding.observations.len() == 1
            || finding.same_rows
            || finding.kind == Some(ObservationKind::CategoryVariants);
        let sampled = if finding.opens_sample(results) {
            "sampled "
        } else {
            ""
        };
        let rows = if finding.can_open_rows(results) {
            match finding.kind {
                // The measurement counts rows beyond one per value; the rows that
                // share a value are always more.
                Some(ObservationKind::KeyLike) => {
                    format!("Enter shows every {sampled}row that shares a repeated value.")
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
                _ if counted => format!(
                    "Enter shows the {} {sampled}{}.",
                    numfmt::group_chrome(finding.affected_rows),
                    if finding.affected_rows == 1 {
                        "row"
                    } else {
                        "rows"
                    }
                ),
                _ => format!("Enter shows the {sampled}rows."),
            }
        } else {
            String::new()
        };
        if !rows.is_empty() {
            lines.push(Line::raw(""));
            lines.push(Line::styled(rows, dimmed));
        }
    }
    // Grow with the text up to the screen, then scroll inside the frame; the
    // bottom edge counts what is below.
    let rows = lines
        .iter()
        .map(|line| crate::render::home_view::wrapped_rows(line, inner as usize))
        .sum::<usize>()
        .min(u16::MAX as usize) as u16;
    let height = (rows + 2).min(area.height.saturating_sub(2));
    scroll.max = rows.saturating_sub(height.saturating_sub(2));
    scroll.offset = scroll.offset.min(scroll.max);
    let below = scroll.max - scroll.offset;
    let popup = centered_rect(width, height, area);
    Clear.render(popup, buf);
    let mut block = Block::default()
        .title(title)
        .borders(Borders::ALL)
        .border_set(crate::glyphs::get().border)
        .border_style(Style::default().fg(theme.get("modal_border_active")))
        .padding(ratatui::widgets::Padding::horizontal(1));
    if below > 0 {
        block = block.title_bottom(
            Line::styled(
                format!(" {} {below} more ", glyphs::get().ellipsis),
                Style::default().fg(theme.get("dimmed")),
            )
            .right_aligned(),
        );
    }
    Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll.offset, 0))
        .block(block)
        .render(popup, buf);
}

fn render_time_roles(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let accent = Style::default().fg(theme.get("accent"));
    let columns = config.state.quality_temporal_columns(&config.plan.scope);
    let roles = TemporalRole::ALL.len() as u16;
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(roles + 1),
            Constraint::Length(1),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .margin(1)
        .split(area);
    Paragraph::new(rule_line("Time roles", None, sections[0].width, theme))
        .render(sections[0], buf);

    let focused_role = TemporalRole::ALL[config.plan_field.min(TemporalRole::ALL.len() - 1)];
    let assigned = |role: TemporalRole| {
        config
            .plan
            .temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .map(|assignment| assignment.column.clone())
    };
    let rows = TemporalRole::ALL.iter().map(|role| {
        Row::new(vec![
            Cell::from(role.label()),
            match assigned(*role) {
                Some(column) => Cell::from(column),
                None => Cell::from(Span::styled("unassigned", dimmed)),
            },
        ])
    });
    table_state.select(Some(config.plan_field.min(TemporalRole::ALL.len() - 1)));
    let table = Table::new(rows, [Constraint::Length(20), Constraint::Fill(1)])
        .header(Row::new(["Role", "Column"]).style(dimmed))
        .row_highlight_style(theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);

    // The candidates, with a few of their values from the rows already on screen,
    // so choosing which column is "received" is choosing among things seen.
    Paragraph::new(rule_line(
        "Date and time columns",
        Some(&numfmt::group_chrome(columns.len())),
        sections[3].width,
        theme,
    ))
    .render(sections[3], buf);
    if columns.is_empty() {
        Paragraph::new(Span::styled("None in this data", dimmed)).render(sections[4], buf);
        return;
    }
    let name_width = columns
        .iter()
        .map(|column| glyphs::display_width(column))
        .max()
        .unwrap_or(0)
        + 2;
    let types = columns
        .iter()
        .map(|column| {
            config
                .state
                .schema
                .get(column)
                .map(|dtype| dtype.to_string())
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let type_width = types
        .iter()
        .map(|dtype| glyphs::display_width(dtype))
        .max()
        .unwrap_or(0)
        + 2;
    let chosen = assigned(focused_role);
    let values_width = (sections[4].width as usize).saturating_sub(2 + name_width + type_width);
    let lines = columns
        .iter()
        .zip(&types)
        .map(|(column, dtype)| {
            let is_chosen = chosen.as_deref() == Some(column.as_str());
            let values = config.state.buffered_values(column, 3);
            let values = if values.is_empty() {
                "not in the rows on screen".to_string()
            } else {
                values.join("   ")
            };
            Line::from(vec![
                Span::styled(if is_chosen { glyphs::get().rail } else { " " }, accent),
                Span::raw(" "),
                Span::styled(
                    format!("{column:<name_width$}"),
                    if is_chosen {
                        accent
                    } else {
                        Style::default().fg(theme.get("text_primary"))
                    },
                ),
                Span::styled(format!("{dtype:<type_width$}"), dimmed),
                Span::raw(fit(&values, values_width)),
            ])
        })
        .collect::<Vec<_>>();
    Paragraph::new(lines).render(sections[4], buf);
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
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .split(area);
    if config.plan.grain == QualityGrain::Dataset {
        render_section_title("SEGMENTS", sections[0], theme, buf);
        Paragraph::new(
            "The rows are one segment. Set the plan's Grain to split them by file, \
             partition, row chunk or time window, and compare the parts.",
        )
        .wrap(Wrap { trim: true })
        .style(Style::default().fg(theme.get("text_primary")))
        .render(sections[1], buf);
        return;
    }
    Paragraph::new(rule_line(
        if config.segments_by_change {
            "Segments, largest change first"
        } else {
            "Segments"
        },
        Some(&numfmt::group_chrome(results.segments.len())),
        sections[0].width,
        theme,
    ))
    .render(sections[0], buf);
    // A thin sample per segment can only name large moves; say how thin.
    if results.precision == QualityPrecision::Sampled && !results.segments.is_empty() {
        let typical = {
            let mut rows = results
                .segments
                .iter()
                .map(|segment| segment.evaluated_rows)
                .collect::<Vec<_>>();
            rows.sort_unstable();
            rows[rows.len() / 2]
        };
        Paragraph::new(Line::styled(
            format!(
                "About {} sampled rows a segment: a change is named only when it is past \
                 sampling noise. Trends pools segments to show smaller ones",
                numfmt::group_chrome(typical)
            ),
            dimmed,
        ))
        .render(
            Rect {
                y: sections[0].y + 1,
                height: 1,
                ..sections[0]
            },
            buf,
        );
    }

    // What there is for every segment without choosing anything: its rows, how
    // much of it is empty, and the one change against its comparison that moved
    // most. Enter shows the rest.
    let rows_label = |segment: &crate::data_quality::SegmentQualityProfile| match segment.total_rows
    {
        Some(total) if total != segment.evaluated_rows => format!(
            "{} of {}",
            numfmt::group_chrome(segment.evaluated_rows),
            numfmt::group_chrome(total)
        ),
        _ => numfmt::group_chrome(segment.evaluated_rows),
    };
    let label_width = results
        .segments
        .iter()
        .map(|segment| glyphs::display_width(&segment.label))
        .max()
        .unwrap_or(0)
        .clamp(7, 40) as u16
        + 2;
    let rows_width = results
        .segments
        .iter()
        .map(|segment| glyphs::display_width(&rows_label(segment)))
        .max()
        .unwrap_or(0)
        .max(4) as u16
        + 2;
    let compared = config.plan.comparison != QualityComparison::None;
    let order = crate::data_quality::segment_order(results, config.segments_by_change);
    let rows = order
        .iter()
        .map(|&index| &results.segments[index])
        .map(|segment| {
            let mut cells = vec![
                Cell::from(segment.label.clone()),
                Cell::from(rows_label(segment)),
                Cell::from(format!("{:.1}%", segment.null_rate * 100.0)),
            ];
            if compared {
                cells.push(Cell::from(
                    segment.largest_change.clone().unwrap_or_default(),
                ));
            }
            Row::new(cells)
        });
    let mut headers = vec!["Segment", "Rows", "Null cells"];
    let mut widths = vec![
        Constraint::Length(label_width),
        Constraint::Length(rows_width),
        Constraint::Length(12),
    ];
    if compared {
        headers.push("Largest change");
        widths.push(Constraint::Fill(1));
    }
    normalize_selection(table_state, results.segments.len());
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(dimmed))
        .row_highlight_style(theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);
}

/// A rate as the report writes one: two places below 1% so a small share never
/// reads as none.
fn rate_label(value: f64) -> String {
    let percent = value * 100.0;
    if percent > 0.0 && percent < 1.0 {
        format!("{percent:.2}%")
    } else {
        format!("{percent:.1}%")
    }
}

/// One segment's columns, every measure beside the segment it is compared with,
/// the largest move first: the answer to "what changed here" without choosing a
/// column or a measure first.
fn render_segment_detail(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    segment_index: usize,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let Some(segment) = results.segments.get(segment_index) else {
        return;
    };
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let changes = crate::data_quality::segment_changes(results, segment_index);
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .split(area);
    let other = segment.compared_with.as_deref();
    let title = match other {
        Some(other) => format!("{} vs {other}", segment.label),
        None => segment.label.clone(),
    };
    Paragraph::new(rule_line(
        &title,
        Some(&numfmt::group_chrome(changes.len())),
        sections[0].width,
        theme,
    ))
    .render(sections[0], buf);
    if changes.is_empty() {
        Paragraph::new(Span::styled("Nothing measured is above zero here", dimmed))
            .render(sections[1], buf);
        return;
    }
    let name_width = changes
        .iter()
        .map(|change| glyphs::display_width(&change.column))
        .max()
        .unwrap_or(0)
        .clamp(6, 32) as u16
        + 2;
    let value_width = |label: &str| (glyphs::display_width(label).clamp(8, 24) + 2) as u16;
    let rows = changes.iter().map(|change| {
        let style = if change.clear || other.is_none() {
            Style::default()
        } else {
            dimmed
        };
        let mut cells = vec![
            Cell::from(Span::styled(change.column.clone(), style)),
            Cell::from(Span::styled(change.metric.short_label(), dimmed)),
        ];
        if other.is_some() {
            cells.push(Cell::from(
                change
                    .before
                    .map(rate_label)
                    .unwrap_or_else(|| "-".to_string()),
            ));
        }
        cells.push(Cell::from(rate_label(change.now)));
        if other.is_some() {
            cells.push(match change.change() {
                Some(points) => Cell::from(Span::styled(format!("{points:+.1} pp"), style)),
                None => Cell::from(""),
            });
        }
        Row::new(cells)
    });
    let mut headers = vec!["Column".to_string(), "Measure".to_string()];
    let mut widths = vec![Constraint::Length(name_width), Constraint::Length(10)];
    if let Some(other) = other {
        headers.push(other.to_string());
        widths.push(Constraint::Length(value_width(other)));
    }
    headers.push(segment.label.clone());
    widths.push(Constraint::Length(value_width(&segment.label)));
    if other.is_some() {
        headers.push("Change".to_string());
        widths.push(Constraint::Fill(1));
    }
    normalize_selection(table_state, changes.len());
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(dimmed))
        .row_highlight_style(theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);
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
    let show_trend = crate::data_quality::shows_trend(config.plan, results);
    let latency_height = if results.temporal.is_empty() {
        4
    } else {
        (results.temporal.len() as u16 + 4).min(12)
    };
    let parts = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1), Constraint::Length(latency_height)])
        .margin(1)
        .split(area);
    let text = Style::default().fg(config.theme.get("text_primary"));
    if show_trend {
        render_trend_table(config, results, table_state, parts[0], buf);
    } else {
        let [title, body] = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(2), Constraint::Fill(1)])
            .areas(parts[0]);
        render_section_title("ACROSS SEGMENTS", title, config.theme, buf);
        Paragraph::new(
            "Set the plan's Grain to a partition column, to days, weeks or months of a \
             date, or to chunks of rows, to follow each column from one to the next.",
        )
        .wrap(Wrap { trim: true })
        .style(text)
        .render(body, buf);
    }
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(0),
            Constraint::Length(0),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .split(parts[1]);
    render_section_title("TIME BETWEEN DATES", sections[2], config.theme, buf);
    if results.temporal.is_empty() {
        let message = if config
            .state
            .quality_temporal_columns(&config.plan.scope)
            .is_empty()
        {
            "No date or time columns, so no delays to measure."
        } else {
            "Assign the plan's Time roles, such as when a row happened and when it was \
             received, to measure the delay between them."
        };
        Paragraph::new(message)
            .wrap(Wrap { trim: true })
            .style(text)
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
    // The trend table above owns the cursor; this one is read, not walked.
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(Style::default().fg(config.theme.get("dimmed"))));
    Widget::render(table, sections[3], buf);
}

/// Each column's measure across the segments as a line of bars, the rows each
/// segment holds first. The whole range fits the width: a bar pools as many
/// consecutive segments as it takes.
fn render_trend_table(
    config: &DataQualityWidgetConfig<'_>,
    results: &DataQualityResults,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let name_width = results
        .columns
        .iter()
        .map(|profile| glyphs::display_width(&profile.name))
        .max()
        .unwrap_or(0)
        .clamp(12, 28) as u16
        + 2;
    const RANGE: u16 = 20;
    let bars = area.width.saturating_sub(name_width + RANGE + 3).max(8) as usize;
    let (rows, per_bar) = crate::data_quality::trend_rows(results, config.metric, bars);
    let first = results
        .segments
        .first()
        .map(|s| s.label.as_str())
        .unwrap_or("");
    let last = results
        .segments
        .last()
        .map(|s| s.label.as_str())
        .unwrap_or("");
    let unit = match &config.plan.grain {
        QualityGrain::TimeWindows { every, .. } => match every.as_str() {
            "1h" => "hours",
            "1d" => "days",
            "1w" => "weeks",
            _ => "months",
        },
        QualityGrain::Partition(_) => "partitions",
        _ => "chunks",
    };
    let [title, note, table_area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .areas(area);
    Paragraph::new(rule_line(
        &format!("{} over time", config.metric.label()),
        Some(&numfmt::group_chrome(
            rows.iter()
                .filter(|row| !row.rows)
                .map(|row| row.names.len())
                .sum::<usize>(),
        )),
        title.width,
        theme,
    ))
    .render(title, buf);
    let mut facts = vec![format!(
        "{first} to {last}, {} {unit}",
        numfmt::group_chrome(results.segments.len())
    )];
    if per_bar > 1 {
        facts.push(format!("each bar {per_bar} {unit}"));
    }
    if results.precision == QualityPrecision::Sampled {
        facts.push("a bar's rate pools its sampled rows".to_string());
    }
    Paragraph::new(Line::styled(
        facts.join(&format!(" {} ", glyphs::get().middot)),
        dimmed,
    ))
    .render(note, buf);
    let mini = glyphs::get().mini_bars;
    let lines = rows.iter().map(|row| {
        let spark = row
            .bars
            .iter()
            .map(|value| match value {
                Some(value) if row.high > 0.0 => {
                    mini[((value / row.high) * 7.0).round().clamp(0.0, 7.0) as usize]
                }
                Some(_) => mini[0],
                None => " ",
            })
            .collect::<String>();
        let range = if row.rows {
            format!(
                "{} to {}",
                numfmt::group_chrome(row.low.round() as usize),
                numfmt::group_chrome(row.high.round() as usize)
            )
        } else if (row.high - row.low).abs() < 1e-9 {
            rate_label(row.high)
        } else {
            format!("{} to {}", rate_label(row.low), rate_label(row.high))
        };
        let name = crate::quality_report::columns_label(&row.names, name_width as usize - 2);
        Row::new(vec![
            Cell::from(if row.rows {
                Span::styled(name, dimmed)
            } else {
                Span::raw(name)
            }),
            Cell::from(Span::styled(range, dimmed)),
            Cell::from(Span::styled(
                spark,
                Style::default().fg(theme.get("accent")),
            )),
        ])
    });
    normalize_selection(table_state, rows.len());
    let table = Table::new(
        lines,
        [
            Constraint::Length(name_width),
            Constraint::Length(RANGE),
            Constraint::Fill(1),
        ],
    )
    .header(Row::new(["Column", "Range", "Trend"]).style(dimmed))
    .row_highlight_style(theme.highlight_style())
    .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, table_area, buf, table_state);
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
    crate::widgets::analysis::render_sidebar(
        area,
        buf,
        sidebar_state,
        Some(AnalysisTool::DataQuality),
        config.focus,
        config.theme,
    );
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
            Cell::from("the shared sample; at most one read of the scope"),
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
            "Full value scan",
            Style::default()
                .fg(config.theme.get("warning"))
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw("This plan evaluates every eligible row and may read the full source."),
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
        QualityCompute::Sample => Some(total.min(dataset_rows(plan))),
        QualityCompute::Full => Some(total),
    }
}

fn planned_read_bytes(state: &DataTableState, plan: &DataQualityPlan) -> Option<usize> {
    if plan.scope.uses_source() {
        return None;
    }
    let rows = match plan.compute {
        QualityCompute::Metadata => 0,
        // The head reads what it keeps. Anything else is a ceiling: a spread sample
        // is one Parquet or IPC file's few dozen short runs or one stream of the
        // scope, and a per-partition one streams the scope.
        QualityCompute::Sample if plan.method == crate::sampling::SampleMethod::FirstRows => {
            planned_scope_rows(state, plan)?.min(dataset_rows(plan))
        }
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
        && plan.method != crate::sampling::SampleMethod::FirstRows
        && planned_scope_rows(state, plan).is_some_and(|rows| rows > dataset_rows(plan));
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

/// Rows a dataset-grain run keeps: the shared sample's size.
fn dataset_rows(plan: &DataQualityPlan) -> usize {
    plan.dataset_rows
}

fn compute_label(plan: &DataQualityPlan) -> String {
    match plan.compute {
        QualityCompute::Metadata => "metadata only".to_string(),
        QualityCompute::Sample => format!(
            "{} rows {} / seed {}",
            numfmt::group_chrome(dataset_rows(plan)),
            plan.method.label().to_lowercase(),
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
