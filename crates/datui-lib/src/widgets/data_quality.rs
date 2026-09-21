use crate::analysis_modal::{AnalysisFocus, AnalysisTool};
use crate::config::Theme;
use crate::data_quality::{
    DataQualityPlan, DataQualityResults, ObservationKind, QualityComparison, QualityCompute,
    QualityGrain, QualityMetric, QualityPage, QualityScope, TemporalRole,
};
use crate::glyphs;
use crate::numfmt;
use crate::widgets::datatable::DataTableState;
use crate::widgets::text_input::TextInput;
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, Clear, List, ListItem, Paragraph, Row, StatefulWidget, Table,
    TableState, Widget,
};

pub struct DataQualityWidgetConfig<'a> {
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
            Constraint::Length(2),
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
    render_controls(&config, vertical[3], buf);

    if config.show_access {
        render_access_plan(&config, area, buf);
    } else if config.observation_detail {
        render_observation_detail(&config, table_state, area, buf);
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

fn render_plan_strip(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let remote = config.state.is_remote_source();
    let bytes = planned_read_bytes(config.state, config.plan);
    let source = if remote {
        "REMOTE TRANSFER"
    } else {
        "LOCAL READ"
    };
    let source_style = if remote {
        Style::default().fg(config.theme.get("warning"))
    } else {
        Style::default().fg(config.theme.get("text_primary"))
    };
    let lines = vec![
        Line::from(vec![
            Span::styled("scope ", Style::default().fg(config.theme.get("dimmed"))),
            Span::styled(
                config.plan.scope.label(),
                Style::default()
                    .fg(config.theme.get("accent"))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::styled(
                " -> grain ",
                Style::default().fg(config.theme.get("dimmed")),
            ),
            Span::styled(
                config.plan.grain.label(),
                Style::default().fg(config.theme.get("accent")),
            ),
            Span::styled(
                " -> compute ",
                Style::default().fg(config.theme.get("dimmed")),
            ),
            Span::styled(
                compute_label(config.plan),
                Style::default().fg(config.theme.get("accent")),
            ),
            Span::styled(
                " -> compare ",
                Style::default().fg(config.theme.get("dimmed")),
            ),
            Span::styled(
                config.plan.comparison_label(),
                Style::default().fg(config.theme.get("accent")),
            ),
        ]),
        Line::from(vec![
            Span::styled(format!("{source} "), source_style),
            Span::styled(
                if remote {
                    "unknown".to_string()
                } else {
                    approximate_bytes_option(bytes)
                },
                source_style.add_modifier(Modifier::BOLD),
            ),
            Span::styled(" | ", Style::default().fg(config.theme.get("dimmed"))),
            Span::styled(
                if remote {
                    "requests unknown".to_string()
                } else {
                    "no network requests".to_string()
                },
                Style::default().fg(config.theme.get("dimmed")),
            ),
            Span::styled(" | ", Style::default().fg(config.theme.get("dimmed"))),
            Span::styled(
                "REMOTE WRITE 0 B",
                Style::default().fg(config.theme.get("success")),
            ),
        ]),
    ];
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
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(8),
            Constraint::Length(2),
            Constraint::Length(5),
            Constraint::Fill(1),
        ])
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
    ];
    if config.editing {
        table_state.select(Some(config.plan_field));
    } else {
        table_state.select(None);
    }
    let table = Table::new(rows, [Constraint::Length(16), Constraint::Fill(1)])
        .block(
            Block::default()
                .borders(Borders::ALL)
                .border_style(Style::default().fg(config.theme.get("modal_border"))),
        )
        .row_highlight_style(config.theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[1], buf, table_state);

    render_section_title("ACCESS", sections[2], config.theme, buf);
    let planned = planned_rows(config.state, config.plan);
    let bytes = planned_read_bytes(config.state, config.plan);
    let planned_label = planned
        .map(numfmt::group_chrome)
        .unwrap_or_else(|| "unknown".to_string());
    let bytes_label = approximate_bytes_option(bytes);
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

    Paragraph::new(if config.editing {
        "Editing: Up/Down field  Left/Right value  Enter details/apply  Esc cancel"
    } else {
        "The plan is inert until you run it. Press p for the exact access basis."
    })
    .style(Style::default().fg(config.theme.get("dimmed")))
    .render(sections[4], buf);
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
    let notes = config.state.notes();
    let notes_height = if notes.is_empty() {
        0
    } else {
        (notes.len() as u16).min(4) + 2
    };
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),
            Constraint::Length(notes_height),
            Constraint::Length(2),
            Constraint::Fill(1),
        ])
        .margin(1)
        .split(area);
    Paragraph::new(Line::from(vec![
        Span::styled(
            numfmt::group_chrome(results.evaluated_rows),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!(" {} rows  ", results.precision.label())),
        Span::styled(
            numfmt::group_chrome(results.columns.len()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(" columns  "),
        Span::styled(
            numfmt::group_chrome(results.observations.len()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(" observations"),
        Span::raw("  "),
        Span::styled(
            results
                .identity
                .as_ref()
                .map(|identity| numfmt::group_chrome(identity.extra_rows))
                .unwrap_or_else(|| "-".to_string()),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(" duplicate extras"),
    ]))
    .render(sections[0], buf);
    if !notes.is_empty() {
        let lines = notes
            .iter()
            .take(4)
            .map(|note| Line::raw(format!("{} — {}", note.summary, note.scope)))
            .collect::<Vec<_>>();
        Paragraph::new(lines)
            .block(
                Block::default()
                    .title(" Dataset notes ")
                    .borders(Borders::ALL)
                    .border_style(Style::default().fg(config.theme.get("modal_border"))),
            )
            .render(sections[1], buf);
    }
    render_section_title("OBSERVATIONS", sections[2], config.theme, buf);

    let rows = results.observations.iter().map(|item| {
        Row::new(vec![
            item.kind.label().to_string(),
            item.column.clone(),
            format!(
                "{} / {}",
                numfmt::group_chrome(item.affected_rows),
                numfmt::group_chrome(item.evaluated_rows)
            ),
            item.fact.clone(),
        ])
    });
    normalize_selection(table_state, results.observations.len());
    let table = Table::new(
        rows,
        [
            Constraint::Length(18),
            Constraint::Length(24),
            Constraint::Length(22),
            Constraint::Fill(1),
        ],
    )
    .header(
        Row::new(["Kind", "Column", "Affected", "Measured fact"])
            .style(Style::default().fg(config.theme.get("dimmed"))),
    )
    .row_highlight_style(config.theme.highlight_style())
    .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, sections[3], buf, table_state);
}

fn render_observation_detail(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some((results, observation)) = config.results.and_then(|results| {
        results
            .observations
            .get(table_state.selected()?)
            .map(|observation| (results, observation))
    }) else {
        return;
    };
    let definition = match observation.kind {
        ObservationKind::Nulls => "Null values / evaluated rows",
        ObservationKind::Empty => "Exact empty strings / evaluated rows",
        ObservationKind::Whitespace => "Nonempty strings that trim to empty / evaluated rows",
        ObservationKind::NonFinite => "NaN or positive/negative infinity / evaluated rows",
        ObservationKind::Constant => "One distinct non-null value in evaluated rows",
        ObservationKind::ParseableText => "Values parseable as a typed value, stored as text",
        ObservationKind::DuplicateRows => "Equal complete rows; extras = sum(group size - 1)",
        ObservationKind::CategoryVariants => "Distinct originals equal after trim and lowercase",
    };
    let mut lines = vec![
        Line::styled(
            format!("{}  /  {}", observation.kind.label(), observation.column),
            Style::default()
                .fg(config.theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        ),
        Line::raw(""),
        Line::raw(observation.fact.clone()),
        Line::raw(format!(
            "Affected: {} / {} evaluated rows",
            numfmt::group_chrome(observation.affected_rows),
            numfmt::group_chrome(observation.evaluated_rows)
        )),
        Line::raw(format!("Definition: {definition}")),
        Line::raw(""),
        Line::styled(
            format!(
                "{} precision; {} eligible rows; sample seed {}",
                results.precision.label(),
                count_label(results.total_rows),
                results.sample_seed
            ),
            Style::default().fg(config.theme.get("dimmed")),
        ),
    ];
    let can_open_rows = results.precision == crate::data_quality::QualityPrecision::Exact
        && observation.evidence_predicate().is_some();
    lines.push(Line::styled(
        if can_open_rows {
            "Enter opens matching rows (the source may be read again)."
        } else if results.precision == crate::data_quality::QualityPrecision::Sampled {
            "Sampled observation: run a full profile for exact matching rows."
        } else {
            "No deterministic row filter for this aggregate; use the measured fact above."
        },
        Style::default().fg(config.theme.get("dimmed")),
    ));
    if observation.kind == ObservationKind::CategoryVariants {
        for group in results
            .category_variants
            .iter()
            .filter(|group| {
                group.column == observation.column
                    && observation.normalized_category.as_ref() == Some(&group.normalized)
            })
            .take(3)
        {
            lines.push(Line::raw(format!(
                "{:?}: {}{}",
                group.normalized,
                group
                    .variants
                    .iter()
                    .take(3)
                    .map(|(value, count)| format!("{value:?} ({count})"))
                    .collect::<Vec<_>>()
                    .join(", "),
                if group.complete { "" } else { " (partial)" }
            )));
        }
    }
    let popup = centered_rect(78, 16, area);
    Clear.render(popup, buf);
    Paragraph::new(lines)
        .block(
            Block::default()
                .title(if can_open_rows {
                    " OBSERVATION — Enter Rows / Esc Back "
                } else {
                    " OBSERVATION — Enter/Esc Close "
                })
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(config.theme.get("modal_border_active"))),
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
    Paragraph::new(
        "Left/Right cycles only date and datetime columns. Unassigned roles produce no lifecycle claims.",
    )
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
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .split(area);
    render_section_title("COLUMN PROFILES", sections[0], config.theme, buf);
    let layout = if sections[1].width >= 148 {
        2
    } else if sections[1].width >= 72 {
        1
    } else {
        0
    };
    let rows = results.columns.iter().map(|profile| {
        let range = match (&profile.min, &profile.max) {
            (Some(min), Some(max)) => format!("{min} .. {max}"),
            _ => "-".to_string(),
        };
        let shape = match (profile.min_length, profile.max_length) {
            (Some(min), Some(max)) => format!("len {min}..{max}; {range}"),
            _ => range,
        };
        let dominant = profile
            .dominant_value
            .as_ref()
            .zip(profile.dominant_count)
            .map(|(value, count)| {
                format!(
                    "{}: {} ({:.1}%)",
                    value,
                    numfmt::group_chrome(count),
                    count as f64 / profile.non_null_rows().max(1) as f64 * 100.0
                )
            })
            .unwrap_or_else(|| "-".to_string());
        let null = format!(
            "{} ({:.1}%)",
            numfmt::group_chrome(profile.null_count),
            profile.null_rate() * 100.0
        );
        let distinct = count_label(profile.distinct_count);
        Row::new(match layout {
            2 => vec![
                profile.name.clone(),
                profile.dtype.to_string(),
                numfmt::group_chrome(profile.evaluated_rows),
                null,
                count_label(profile.empty_count),
                distinct,
                dominant,
                shape,
            ],
            1 => vec![
                profile.name.clone(),
                profile.dtype.to_string(),
                null,
                distinct,
                dominant,
            ],
            _ => vec![profile.name.clone(), null, distinct],
        })
    });
    normalize_selection(table_state, results.columns.len());
    let (headers, widths) = match layout {
        2 => (
            vec![
                "Column",
                "Type",
                "Evaluated",
                "Null",
                "Empty",
                "Distinct",
                "Dominant",
                "Shape / range",
            ],
            vec![
                Constraint::Length(22),
                Constraint::Length(14),
                Constraint::Length(14),
                Constraint::Length(18),
                Constraint::Length(12),
                Constraint::Length(14),
                Constraint::Length(24),
                Constraint::Fill(1),
            ],
        ),
        1 => (
            vec!["Column", "Type", "Null", "Distinct", "Dominant"],
            vec![
                Constraint::Length(22),
                Constraint::Length(12),
                Constraint::Length(16),
                Constraint::Length(12),
                Constraint::Fill(1),
            ],
        ),
        _ => (
            vec!["Column", "Null", "Distinct"],
            vec![
                Constraint::Fill(1),
                Constraint::Length(14),
                Constraint::Length(10),
            ],
        ),
    };
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(Style::default().fg(config.theme.get("dimmed"))))
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
    let layout = if sections[1].width >= 124 {
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
        Row::new(match layout {
            2 => vec![
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
            ],
            1 => vec![
                segment.label.clone(),
                numfmt::group_chrome(segment.evaluated_rows),
                metric.clone(),
                change,
            ],
            _ => vec![segment.label.clone(), metric, change],
        })
    });
    normalize_selection(table_state, results.segments.len());
    let (headers, widths) = match layout {
        2 => (
            vec![
                "Segment",
                "Total rows",
                "Evaluated",
                "Selected metric",
                "Compared with",
                "Delta",
                "All-null rate",
            ],
            vec![
                Constraint::Length(24),
                Constraint::Length(16),
                Constraint::Length(16),
                Constraint::Length(20),
                Constraint::Length(24),
                Constraint::Length(12),
                Constraint::Fill(1),
            ],
        ),
        1 => (
            vec!["Segment", "Evaluated", "Metric", "Delta"],
            vec![
                Constraint::Length(26),
                Constraint::Length(12),
                Constraint::Length(18),
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
    let mut text = vec![
        Line::from(vec![
            Span::styled(
                &profile.name,
                Style::default()
                    .fg(config.theme.get("accent"))
                    .add_modifier(Modifier::BOLD),
            ),
            Span::raw(format!("  {}", profile.dtype)),
        ]),
        Line::raw(format!(
            "Evaluated: {} ({})",
            numfmt::group_chrome(profile.evaluated_rows),
            results.precision.label()
        )),
        Line::raw(format!(
            "Null: {} / {} ({:.2}%)",
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
        Line::raw(format!(
            "Range: {} .. {}",
            profile.min.as_deref().unwrap_or("-"),
            profile.max.as_deref().unwrap_or("-")
        )),
        Line::raw(format!(
            "Dominant: {}",
            profile
                .dominant_value
                .as_ref()
                .zip(profile.dominant_count)
                .map(|(value, count)| format!("{value:?}, {count} rows"))
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::raw(format!(
            "{} length: {} .. {}",
            if matches!(profile.dtype, polars::prelude::DataType::List(_)) {
                "List"
            } else {
                "Text"
            },
            profile
                .min_length
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string()),
            profile
                .max_length
                .map(|value| value.to_string())
                .unwrap_or_else(|| "-".to_string())
        )),
        Line::raw(format!(
            "Text parses: integer {}  decimal {}  date {}  datetime {}",
            count_label(profile.integer_parse_count),
            count_label(profile.decimal_parse_count),
            count_label(profile.date_parse_count),
            count_label(profile.datetime_parse_count),
        )),
        Line::raw(""),
        Line::styled(
            format!(
                "Provenance: {} of {} eligible rows; {} precision; sample seed {}.",
                numfmt::group_chrome(profile.evaluated_rows),
                count_label(results.total_rows),
                results.precision.label(),
                results.sample_seed
            ),
            Style::default().fg(config.theme.get("dimmed")),
        ),
    ];
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
                .title(" Evidence ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
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
        let prefix = if selected { "> " } else { "  " };
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
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(config.theme.get("modal_border"))),
        ),
        parts[0],
        buf,
    );

    let bytes = planned_read_bytes(config.state, config.plan);
    Paragraph::new(vec![
        Line::styled("ACCESS", Style::default().fg(config.theme.get("accent"))),
        Line::raw(approximate_bytes_option(bytes)),
        Line::raw(if config.state.is_remote_source() {
            "remote -> this machine"
        } else {
            "local read"
        }),
        Line::styled(
            "0 B remote write",
            Style::default().fg(config.theme.get("success")),
        ),
    ])
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_style(Style::default().fg(config.theme.get("modal_border"))),
    )
    .render(parts[1], buf);
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
                .title(" Analysis tools ")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(Style::default().fg(config.theme.get("accent"))),
        ),
        popup,
        buf,
    );
}

fn render_controls(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let actions = if config.running {
        vec![("Esc", "Cancel run")]
    } else if config.show_access {
        vec![("Enter", "Close"), ("Esc", "Close")]
    } else if config.observation_detail {
        vec![("Enter", "Evidence"), ("Esc", "Back")]
    } else if config.page == QualityPage::Scope {
        vec![
            ("Enter", "Use scope"),
            ("PgUp/Dn", "Files"),
            ("Esc", "Back"),
        ]
    } else if config.page == QualityPage::TimeRoles {
        vec![
            (glyphs::get().updown, "Role"),
            (glyphs::get().arrow_left, "Column"),
            ("Enter", "Done"),
            ("Esc", "Back"),
        ]
    } else if config.editing {
        vec![
            (glyphs::get().updown, "Field"),
            (glyphs::get().arrow_left, "Value"),
            ("Enter", "Apply"),
            ("Esc", "Cancel"),
        ]
    } else if config.page == QualityPage::Plan {
        vec![
            ("Enter", "Run"),
            ("e", "Edit plan"),
            ("p", "Plan details"),
            ("Esc", "Back"),
        ]
    } else if config.page == QualityPage::Segments {
        vec![
            ("[ ]", "Column"),
            ("m", "Metric"),
            ("b", "Baseline"),
            ("1-4", "Page"),
        ]
    } else if config.page == QualityPage::Trends {
        vec![
            ("[ ]", "Column"),
            ("m", "Metric"),
            ("1-4", "Page"),
            ("p", "Access"),
        ]
    } else {
        vec![
            ("1", "Overview"),
            ("2", "Columns"),
            ("3", "Segments"),
            ("4", "Trends"),
            ("Enter", "Inspect"),
            ("p", "Access plan"),
        ]
    };
    let mut spans = Vec::new();
    for (key, label) in actions {
        spans.push(Span::styled(
            format!(" {key} "),
            Style::default()
                .fg(config.theme.get("controls_bg"))
                .bg(config.theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        ));
        spans.push(Span::styled(
            format!(" {label}  "),
            Style::default().fg(config.theme.get("dimmed")),
        ));
    }
    Paragraph::new(Line::from(spans))
        .style(Style::default().bg(config.theme.get("controls_bg")))
        .render(area, buf);
}

fn render_access_plan(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let popup = centered_rect(72, 16, area);
    Clear.render(popup, buf);
    let rows = planned_rows(config.state, config.plan);
    let bytes = planned_read_bytes(config.state, config.plan);
    let rows_label = rows
        .map(numfmt::group_chrome)
        .unwrap_or_else(|| "unknown".to_string());
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
                approximate_bytes_option(bytes)
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
        Row::new(vec![Cell::from("Remote writes"), Cell::from("none")]),
        Row::new(vec![Cell::from("Local file writes"), Cell::from("none")]),
        Row::new(vec![
            Cell::from("Estimate basis"),
            Cell::from("sample prefix row width; full scan unknown"),
        ]),
    ];
    let table = Table::new(table_rows, [Constraint::Length(20), Constraint::Fill(1)]).block(
        Block::default()
            .title(" ACCESS PLAN — Esc Close ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(Style::default().fg(config.theme.get("accent"))),
    );
    Widget::render(table, popup, buf);
}

fn render_run_confirmation(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let popup = centered_rect(64, 9, area);
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
    .block(
        Block::default()
            .title(" CONFIRM ACCESS ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
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
            .title(" RUNNING ")
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
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
        QualityCompute::Sample => Some(total.min(plan.sample_rows.min(50_000))),
        QualityCompute::Full => Some(total),
    }
}

fn planned_read_bytes(state: &DataTableState, plan: &DataQualityPlan) -> Option<usize> {
    if plan.scope.uses_source() {
        return None;
    }
    let rows = match plan.compute {
        QualityCompute::Metadata => 0,
        QualityCompute::Sample => {
            let multiplier = if plan.sample_rows <= 1_000 {
                5
            } else if plan.sample_rows <= 5_000 {
                3
            } else {
                2
            };
            let rows = state.num_rows_if_valid()?;
            let rows = match &plan.scope {
                QualityScope::FirstRows(limit) => rows.min(*limit),
                QualityScope::ViewRows { start, end } => {
                    rows.min(*end).saturating_sub(start.saturating_sub(1))
                }
                QualityScope::CurrentView => rows,
                _ => unreachable!(),
            };
            rows.min(plan.sample_rows.saturating_mul(multiplier).min(50_000))
        }
        QualityCompute::Full => return None,
    };
    Some(rows.saturating_mul(state.estimated_row_bytes()))
}

fn approximate_bytes_option(bytes: Option<usize>) -> String {
    bytes
        .map(approximate_bytes)
        .unwrap_or_else(|| "unknown".to_string())
}

fn compute_label(plan: &DataQualityPlan) -> String {
    match plan.compute {
        QualityCompute::Metadata => "metadata only".to_string(),
        QualityCompute::Sample => {
            format!(
                "{} rows / seed {}",
                numfmt::group_chrome(plan.sample_rows),
                plan.sample_seed
            )
        }
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
