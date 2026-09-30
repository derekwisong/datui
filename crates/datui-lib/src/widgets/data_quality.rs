use crate::analysis_modal::{AnalysisFocus, AnalysisTool, DetailScroll, SetupRow};
use crate::config::Theme;
use crate::data_quality::{
    ColumnQualityProfile, DataQualityPlan, DataQualityResults, ObservationKind, QualityComparison,
    QualityCompute, QualityGrain, QualityMetric, QualityPage, QualityPrecision, QualityScope,
    SegmentCount, TemporalRole, window_cadence,
};
use crate::glyphs;
use crate::numfmt;
use crate::quality_report::{
    CHECKS_SHOWN, Check, Outcome, QualityReport, Severity, advice, build_report, checks, describe,
    verdict,
};
use crate::render::context::RenderContext;
use crate::widgets::datatable::DataTableState;
use crate::widgets::ui::{FormValue, Picker, Surface};
use polars::prelude::{DataType, Schema};
use ratatui::buffer::Buffer;
use ratatui::layout::{Alignment, Constraint, Direction, Layout, Rect};
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Cell, Paragraph, Row, StatefulWidget, Table, TableState, Tabs, Widget, Wrap,
};

/// What Setup says about the draft beside its rows, all of it known without a read.
#[derive(Debug, Clone, Default)]
pub struct SetupView<'a> {
    /// The columns a time role can be given: date and time columns, then text.
    pub time_candidates: &'a [String],
    /// A run would start from the rows the last run read.
    pub reuses_sample: bool,
    /// A random sample reads seeded runs of one file instead of streaming it.
    pub reads_blocks: bool,
    /// Where a run gets exact segment totals: see [`SegmentCount`].
    pub segment_count: SegmentCount,
    /// The rows this draft's sample reads were read earlier and released to the
    /// memory budget.
    pub released: bool,
    /// The session cache holds this draft's report.
    pub cached: bool,
    /// The report on screen was measured with exactly this draft.
    pub unchanged: bool,
    /// Setup holds edits Esc would discard.
    pub edited: bool,
    /// Why Enter did not run.
    pub note: Option<&'a str>,
    /// When a cancelled read began winding down, while it still is.
    pub cancelling: Option<std::time::Instant>,
}

pub struct DataQualityWidgetConfig<'a> {
    /// The clean entry's checks table shows every check, not the first few.
    pub checks_expanded: bool,
    pub state: &'a DataTableState,
    pub plan: &'a DataQualityPlan,
    /// The plan the result on screen was measured with; the header says this one
    /// even while Setup edits another.
    pub measured: &'a DataQualityPlan,
    pub results: Option<&'a DataQualityResults>,
    pub from_cache: bool,
    pub metric: QualityMetric,
    pub column_index: usize,
    pub segment_index: usize,
    pub segments_by_change: bool,
    pub page: QualityPage,
    pub setup: SetupView<'a>,
    pub plan_field: usize,
    pub show_access: bool,
    pub observation_detail: bool,
    pub confirm_run: bool,
    pub focus: AnalysisFocus,
    pub theme: &'a Theme,
    /// The dialogs' Surfaces and the table's number formatting.
    pub ctx: &'a RenderContext,
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
    render_tabs(&config, vertical[1], buf);

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
        QualityPage::Setup => render_setup(&config, body, buf),
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
    if let Some(results) = config.results.filter(|_| !config.page.is_setup()) {
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
    if config.from_cache && !config.page.is_setup() {
        spans.push(Span::styled(
            "  [session cache]",
            Style::default().fg(config.theme.get("dimmed")),
        ));
    }
    // State, not a message: it holds until the worker exits, on every page.
    if let Some(since) = config.setup.cancelling {
        spans.push(Span::styled(
            format!(
                "  Cancellation requested; source read finishing {} {}",
                glyphs::get().middot,
                crate::render::analysis_view::elapsed(since.elapsed())
            ),
            Style::default().fg(config.theme.get("warning")),
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

/// The pages, with the one shown carrying the accent. `←→` walk them. Setup is not
/// one of them: it names itself alone, since the report's pages do not apply to it.
fn render_tabs(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    if config.page.is_setup() {
        Paragraph::new(Line::styled(
            "Setup",
            Style::default()
                .fg(config.theme.get("accent"))
                .add_modifier(Modifier::BOLD),
        ))
        .render(area, buf);
        return;
    }
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
/// One line of Setup, top to bottom.
enum SetupLine {
    Rule(&'static str, Option<String>),
    Row(SetupRow),
    Note(String, bool),
}

/// Data Quality Setup: every setting a run takes, in four sections, and what the
/// run will read. Nothing here reads: the schema, the rows on screen and what
/// earlier runs kept are all it knows. Staged edits wait for Enter.
fn render_setup(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let plan = config.plan;
    let view = &config.setup;
    let ctx = config.ctx;
    // A short terminal gives up the top margin before it gives up a row.
    let top = u16::from(area.height >= 20);
    let area = Rect {
        x: area.x + 1,
        y: area.y + top,
        width: area.width.saturating_sub(2),
        height: area.height.saturating_sub(top),
    };
    if area.height < 3 || area.width < 20 {
        return;
    }
    let width = area.width as usize;
    let schema = config.state.quality_schema(&plan.scope);

    let mut lines = vec![
        SetupLine::Rule("Rows & sample", None),
        SetupLine::Row(SetupRow::Sample),
    ];
    let texts = config.state.quality_text_columns(&plan.scope).len();
    let times = view.time_candidates.len().saturating_sub(texts);
    let counts = [(times, "date or time"), (texts, "text")]
        .into_iter()
        .filter(|(count, _)| *count > 0)
        .map(|(count, kind)| format!("{} {kind}", numfmt::group_chrome(count)))
        .collect::<Vec<_>>()
        .join(&format!(" {} ", glyphs::get().middot));
    lines.push(SetupLine::Rule(
        "Columns",
        (!counts.is_empty()).then_some(counts),
    ));
    lines.push(SetupLine::Row(SetupRow::TextAsTime));
    lines.push(SetupLine::Row(SetupRow::TimeRoles));
    for (note, warn) in column_notes(plan, schema) {
        for line in crate::widgets::info::wrap_to(&note, width.saturating_sub(2)) {
            lines.push(SetupLine::Note(line, warn));
        }
    }
    lines.push(SetupLine::Rule("Study", None));
    for row in [
        SetupRow::Grain,
        SetupRow::Compare,
        SetupRow::Values,
        SetupRow::Latency,
    ] {
        lines.push(SetupLine::Row(row));
    }
    lines.push(SetupLine::Rule("Read", None));
    let read_start = lines.len();
    for note in read_lines(config) {
        for line in crate::widgets::info::wrap_to(&note, width.saturating_sub(2)) {
            lines.push(SetupLine::Note(line, false));
        }
    }

    // The status line keeps the bottom row: why Run waits, or that it would
    // replace the report on screen.
    let status = setup_status(config);
    let body_height = area.height.saturating_sub(1) as usize;
    // Scrolled only as far as the focused row needs; the read summary, last, is
    // what gives way, and says how much of it is off screen.
    let focused = lines
        .iter()
        .position(
            |line| matches!(line, SetupLine::Row(row) if *row == SetupRow::at(config.plan_field)),
        )
        .unwrap_or(0);
    let (offset, room) = if lines.len() <= body_height {
        (0, body_height)
    } else {
        // One line counts what is cut, unless scrolling reaches the end anyway.
        let room = body_height.saturating_sub(1);
        let offset = (focused + 1).saturating_sub(room);
        if offset + body_height >= lines.len() {
            (lines.len() - body_height, body_height)
        } else {
            (offset, room)
        }
    };
    let end = (offset + room).min(lines.len());
    let cut = lines.len() - end;
    let dimmed = Style::default().fg(ctx.dimmed);
    let warning = Style::default().fg(ctx.warning);
    if cut > 0 {
        Paragraph::new(Line::styled(
            format!(
                "  {} {cut} more{}",
                glyphs::get().ellipsis,
                if end >= read_start { " (p)" } else { "" }
            ),
            dimmed,
        ))
        .render(
            Rect {
                y: area.y + room as u16,
                height: 1,
                ..area
            },
            buf,
        );
    }
    for (index, line) in lines.iter().enumerate().take(end).skip(offset) {
        let row_area = Rect {
            y: area.y + (index - offset) as u16,
            height: 1,
            ..area
        };
        match line {
            SetupLine::Rule(title, chip) => crate::widgets::ui::SectionRule {
                title,
                chip: chip.as_deref(),
                focused: false,
            }
            .render(row_area, buf, ctx),
            SetupLine::Row(row) => {
                let (value, placeholder) = setup_value(config, *row);
                let room = (row_area.width as usize).saturating_sub(1 + SETUP_LABEL_WIDTH as usize);
                let value = fit(&value, room);
                crate::widgets::ui::FormRow {
                    label: row.label(),
                    value: if placeholder {
                        FormValue::Placeholder(&value)
                    } else {
                        FormValue::Choice(&value)
                    },
                    focused: config.focus == AnalysisFocus::Main
                        && SetupRow::at(config.plan_field) == *row,
                    label_width: SETUP_LABEL_WIDTH,
                }
                .render(row_area, buf, ctx);
            }
            SetupLine::Note(text, warn) => {
                Paragraph::new(Line::styled(
                    format!("  {text}"),
                    if *warn { warning } else { dimmed },
                ))
                .render(row_area, buf);
            }
        }
    }
    if let Some((text, warn)) = status {
        Paragraph::new(Line::styled(text, if warn { warning } else { dimmed })).render(
            Rect {
                y: area.y + area.height - 1,
                height: 1,
                ..area
            },
            buf,
        );
    }
}

/// Where Setup's values start, past the rail gutter and the longest label.
const SETUP_LABEL_WIDTH: u16 = 15;

/// A Setup row's value, and whether it is a placeholder: a choice not made, or one
/// that has nothing to apply to yet.
fn setup_value(config: &DataQualityWidgetConfig<'_>, row: SetupRow) -> (String, bool) {
    let plan = config.plan;
    match row {
        SetupRow::Sample => (plan.sample().summary(), false),
        SetupRow::TextAsTime if plan.time_formats.is_empty() => {
            ("none: every text column is text".to_string(), true)
        }
        // The columns here, each one's format on its own line below.
        SetupRow::TextAsTime => (
            plan.time_formats
                .iter()
                .map(|format| format.column.as_str())
                .collect::<Vec<_>>()
                .join(", "),
            false,
        ),
        SetupRow::TimeRoles if config.setup.time_candidates.is_empty() => {
            ("none: no date, time or text columns".to_string(), true)
        }
        SetupRow::TimeRoles if plan.temporal_roles.is_empty() => ("none".to_string(), true),
        SetupRow::TimeRoles => (
            plan.temporal_roles
                .iter()
                .map(|role| format!("{} = {}", role.role.label(), role.column))
                .collect::<Vec<_>>()
                .join(", "),
            false,
        ),
        SetupRow::Grain => (plan.grain.label(), false),
        SetupRow::Compare => (
            match (plan.comparison, plan.baseline_segment.as_deref()) {
                (QualityComparison::Baseline, Some(segment)) => format!("baseline {segment}"),
                (comparison, _) => comparison.choice_label().to_string(),
            },
            false,
        ),
        SetupRow::Values if plan.compute == QualityCompute::Metadata => {
            ("file metadata only".to_string(), false)
        }
        SetupRow::Values => ("read".to_string(), false),
        SetupRow::Latency if plan.interval_pairs().is_empty() => {
            ("needs an interval to measure".to_string(), true)
        }
        SetupRow::Latency => (
            crate::analysis_modal::threshold_label(plan.latency_threshold_seconds).to_string(),
            plan.latency_threshold_seconds.is_none(),
        ),
    }
}

/// What the columns section says under its rows: text that needs a format before
/// it can be read as time, and which intervals the roles measure, or why none.
fn column_notes(plan: &DataQualityPlan, schema: &Schema) -> Vec<(String, bool)> {
    let mut notes = plan
        .time_formats
        .iter()
        .map(|format| {
            (
                format!("{} read as {}", format.column, format.label()),
                false,
            )
        })
        .collect::<Vec<_>>();
    let mut unread = Vec::new();
    let grain = match &plan.grain {
        QualityGrain::TimeWindows { column, .. } => Some(column.as_str()),
        _ => None,
    };
    for column in plan
        .temporal_roles
        .iter()
        .map(|role| role.column.as_str())
        .chain(grain)
    {
        if !plan.reads_as_time(column, schema) && !unread.contains(&column) {
            unread.push(column);
        }
    }
    for column in unread {
        notes.push((
            format!("{column} is text: choose its format under Text as time"),
            true,
        ));
    }
    let pairs = plan.interval_pairs();
    if !pairs.is_empty() {
        notes.push((
            format!(
                "Intervals: {}",
                pairs
                    .iter()
                    .map(|(start, end)| format!("{} to {}", start.label(), end.label()))
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            false,
        ));
    } else if !plan.temporal_roles.is_empty() {
        notes.push((
            format!(
                "No interval from {}. Measured: event to published, received or \
                 processed; period end to published; published to received; received \
                 to processed",
                plan.temporal_roles
                    .iter()
                    .map(|role| role.role.label())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            true,
        ));
    }
    notes
}

/// What Run will read, in the order it happens, before it happens.
fn read_lines(config: &DataQualityWidgetConfig<'_>) -> Vec<String> {
    let plan = config.plan;
    let view = &config.setup;
    let state = config.state;
    let mut lines = Vec::new();
    if view.unchanged {
        lines.push("The report on screen is this setup's: Run shows it, no read".to_string());
        return lines;
    }
    if view.cached {
        lines.push("This setup's report is in the session cache: no read".to_string());
        return lines;
    }
    let scope_rows = planned_scope_rows(state, plan);
    let n = numfmt::group_chrome(plan.dataset_rows);
    match plan.compute {
        QualityCompute::Metadata => {
            lines.push("File metadata only: no values read".to_string());
            return lines;
        }
        QualityCompute::Full => lines.push(format!(
            "Every eligible row, in up to {} passes over the scope: one per check",
            full_passes(config)
        )),
        QualityCompute::Sample if view.reuses_sample => {
            lines.push("Uses the rows a run already read: no source read".to_string());
        }
        QualityCompute::Sample => lines.push(match &plan.method {
            crate::sampling::SampleMethod::FirstRows => {
                format!("Reads the first {n} rows of the scope")
            }
            crate::sampling::SampleMethod::PerPartition { column } => {
                format!("One pass over every eligible row, keeping {n} per {column}")
            }
            _ if scope_rows.is_some_and(|rows| rows <= plan.dataset_rows) => {
                "Reads every row: the scope holds no more than the sample".to_string()
            }
            _ if view.reads_blocks => {
                format!("Seeded runs of the file, about {n} rows, not a pass over it")
            }
            _ => format!("One pass that streams every eligible row, keeping a seeded {n}"),
        }),
    }
    if plan.compute == QualityCompute::Sample && !view.reuses_sample && view.released {
        lines.push("Read before; released to free memory, so read again".to_string());
    }
    let exact = scope_rows.is_some_and(|rows| rows <= plan.dataset_rows);
    if plan.compute == QualityCompute::Sample && !exact {
        let column = match &plan.grain {
            QualityGrain::Partition(column) | QualityGrain::TimeWindows { column, .. } => {
                column.as_str()
            }
            _ => "",
        };
        match &view.segment_count {
            SegmentCount::CountPass => lines.push(format!(
                "Plus one count of {column} for exact segment totals, kept for later runs"
            )),
            SegmentCount::InSamplePass => lines.push(format!(
                "Counts every row by {column} in that pass: exact segment totals"
            )),
            SegmentCount::Retained => {
                lines.push("Segment totals from a count already read: no read".to_string())
            }
            SegmentCount::RolledUp(finer) => lines.push(format!(
                "Segment totals summed from the {} counts already read",
                window_cadence(finer)
            )),
            SegmentCount::TooMany => lines.push(format!(
                "Too many segments to count; choose a coarser grain than {}",
                plan.grain.label()
            )),
            SegmentCount::NotNeeded | SegmentCount::PerValue => {}
        }
    }
    if plan.compute == QualityCompute::Sample {
        lines.push("Then measured in memory: no further reads".to_string());
    }
    let rows = planned_rows(state, plan)
        .map(numfmt::group_chrome)
        .unwrap_or_else(|| "unknown".to_string());
    let sampled = plan.compute == QualityCompute::Sample;
    let read = if sampled && view.reuses_sample && view.segment_count.reads() && !exact {
        "the grain's column, for the count".to_string()
    } else if sampled && view.reuses_sample {
        "none".to_string()
    } else if state.is_remote_source() {
        "unknown".to_string()
    } else if sampled
        && view.reads_blocks
        && !exact
        && matches!(plan.method, crate::sampling::SampleMethod::Spread)
    {
        // A ceiling of the whole file would say more than the runs read.
        "the row groups the runs fall in".to_string()
    } else {
        planned_read_label(state, plan)
    };
    lines.push(format!(
        "Rows evaluated {rows} {} {} {read}",
        glyphs::get().middot,
        if state.is_remote_source() {
            "remote transfer"
        } else {
            "local read"
        }
    ));
    lines
}

/// Collects a full run makes over its scope: one per check, and a count first when
/// the scope's size is not known. How much each reads again depends on the
/// source; that there are this many is the plan's.
fn full_passes(config: &DataQualityWidgetConfig<'_>) -> usize {
    let plan = config.plan;
    let state = config.state;
    let texts = state
        .quality_schema(&plan.scope)
        .iter()
        .filter(|(_, dtype)| matches!(dtype, DataType::String | DataType::Categorical(..)))
        .count();
    // Profile, most common values, duplicates, and at most one for shared nulls.
    let mut passes = 4 + texts;
    if !matches!(plan.grain, QualityGrain::Dataset) {
        passes += 1;
    }
    if !plan.interval_pairs().is_empty() {
        passes += 1;
    }
    if planned_scope_rows(state, plan).is_none() {
        passes += 1;
    }
    passes + state.quality_conflict_reads()
}

/// Setup's bottom line: a cancelled read still running, why Enter did not run, or
/// that the draft differs from the report it would replace.
fn setup_status(config: &DataQualityWidgetConfig<'_>) -> Option<(String, bool)> {
    let view = &config.setup;
    if let Some(since) = view.cancelling {
        // Said as a reason once Enter has been refused for it, short enough to keep
        // its clock beside the tool list at 80 columns.
        return Some((
            format!(
                "{} {} {}",
                if view.note.is_some() {
                    "Run waits: source read finishing"
                } else {
                    "Cancellation requested; source read finishing"
                },
                glyphs::get().middot,
                crate::render::analysis_view::elapsed(since.elapsed())
            ),
            true,
        ));
    }
    if let Some(note) = view.note {
        return Some((note.to_string(), true));
    }
    if view.edited {
        return Some(("Edited: Enter runs it, Esc discards".to_string(), false));
    }
    None
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
            "No values were read, so nothing about them is known. Set Values to read in Setup (e) to check them."
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
    // Grow with the text up to the screen, then scroll inside the frame; the last
    // row counts what is below.
    let rows = lines
        .iter()
        .map(|line| crate::render::home_view::wrapped_rows(line, inner as usize))
        .sum::<usize>()
        .min(u16::MAX as usize) as u16;
    let height = (rows + 2).min(area.height.saturating_sub(2));
    let room = height.saturating_sub(2);
    scroll.max = rows.saturating_sub(room);
    scroll.offset = scroll.offset.min(scroll.max);
    // Short of the end, the last row counts what is below instead of showing text.
    let shown = if scroll.offset < scroll.max {
        room.saturating_sub(1)
    } else {
        room
    };
    let below = rows.saturating_sub(scroll.offset + shown);
    let popup = centered_rect(width, height, area);
    let content = Surface::new(&title)
        .border_style(Style::default().fg(config.ctx.modal_border_active))
        .render(popup, buf, config.ctx);
    Paragraph::new(lines)
        .wrap(Wrap { trim: false })
        .scroll((scroll.offset, 0))
        .render(
            Rect {
                height: shown.min(content.height),
                ..content
            },
            buf,
        );
    if below > 0 && content.height > shown {
        Paragraph::new(Line::styled(
            format!("{} {below} more", glyphs::get().ellipsis),
            dimmed,
        ))
        .alignment(Alignment::Right)
        .render(
            Rect {
                y: content.y + shown,
                height: 1,
                ..content
            },
            buf,
        );
    }
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
    let columns = config.setup.time_candidates;
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
        "Date, time and text columns",
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
            // Text says how it is read, so a role on it is not mistaken for a cast.
            match (
                config.plan.time_format(column),
                config.state.quality_schema(&config.plan.scope).get(column),
            ) {
                (Some(format), _) => format!("text as {}", format.kind.label()),
                (None, Some(DataType::String | DataType::Categorical(..))) => {
                    "text, no format".to_string()
                }
                (None, dtype) => dtype.map(|dtype| dtype.to_string()).unwrap_or_default(),
            }
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
        Paragraph::new(rule_line("Segments", Some("1"), sections[0].width, theme))
            .render(sections[0], buf);
        Paragraph::new(
            "The rows are one segment. Set Grain in Setup (e) to split them by file, \
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
    // The rule, a blank, and the note saying what fills it, wrapped.
    let latency_height = if results.temporal.is_empty() {
        5
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
        Paragraph::new(rule_line(
            "Across segments",
            None,
            title.width,
            config.theme,
        ))
        .render(title, buf);
        Paragraph::new(
            "Set Grain in Setup (e) to a partition column, to days, weeks or months of a \
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
    let intervals =
        (!results.temporal.is_empty()).then(|| numfmt::group_chrome(results.temporal.len()));
    Paragraph::new(rule_line(
        "Time between dates",
        intervals.as_deref(),
        sections[2].width,
        config.theme,
    ))
    .render(sections[2], buf);
    if results.temporal.is_empty() {
        let message = if config.setup.time_candidates.is_empty() {
            "No date, time or text columns, so no delays to measure."
        } else {
            "Assign Time roles in Setup (e), such as when a row happened and when it was \
             received, to measure the delay between them. Text is read as time through \
             a format chosen under Text as time."
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

/// One column's findings, then its measurements, as aligned label and value rows
/// on the page, the way Setup lays out its rows.
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
    let theme = config.theme;
    let [title, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    Paragraph::new(rule_line(&profile.name, None, title.width, theme)).render(title, buf);

    // The column's findings first, in the report's own words; the measurements
    // under them are the evidence.
    let report = build_report(results);
    let mut findings = report
        .findings
        .iter()
        .filter(|finding| finding.kind.is_some() && finding.columns.contains(&profile.name))
        .map(|finding| FieldRow {
            mark: Some(severity_mark(finding.severity, theme)),
            label: finding.title.to_string(),
            value: finding.summary.clone(),
        })
        .collect::<Vec<_>>();
    if findings.is_empty() {
        findings.push(FieldRow {
            mark: Some(severity_mark(Severity::Clean, theme)),
            label: "No findings".to_string(),
            value: String::new(),
        });
    }
    let measurements = detail_measurements(config.ctx, results, profile);
    let label_width = findings
        .iter()
        .chain(&measurements)
        .map(|row| glyphs::display_width(&row.label))
        .max()
        .unwrap_or(0)
        + 2;
    // A reading surface: values wrap at a comfortable measure on a wide terminal.
    let width = (body.width as usize).min(DETAIL_MEASURE);
    let mut lines = field_lines(&findings, label_width, width, true);
    lines.push(Line::raw(""));
    lines.extend(field_lines(&measurements, label_width, width, true));
    render_counted(lines, body, theme, buf);
}

/// The widest a reading surface's text runs, however wide the terminal.
const DETAIL_MEASURE: usize = 100;

/// The most of one value the Detail page shows: a range's end, the most common
/// value, a spelling.
const END_WIDTH: usize = 32;

/// A label and its value on one row, with an optional mark in the lead.
struct FieldRow {
    mark: Option<Span<'static>>,
    label: String,
    value: String,
}

/// Rows in two aligned columns: the label padded to `label_width`, and the value
/// in what is left, wrapped under itself; a value of several lines keeps each on
/// its own. With `marks`, a two-column lead holds each row's mark, or nothing.
fn field_lines(
    rows: &[FieldRow],
    label_width: usize,
    width: usize,
    marks: bool,
) -> Vec<Line<'static>> {
    let lead = if marks { 2 } else { 0 };
    let value_width = width.saturating_sub(lead + label_width).max(8);
    let mut lines = Vec::new();
    for row in rows {
        let mut values = row
            .value
            .lines()
            // A value that fits is kept as written: wrapping splits on whitespace
            // and would drop the leading, trailing or doubled spaces a value holds.
            .flat_map(|line| {
                if glyphs::display_width(line) <= value_width {
                    vec![line.to_string()]
                } else {
                    crate::widgets::info::wrap_to(line, value_width)
                }
            })
            .collect::<Vec<_>>()
            .into_iter();
        let mut spans = Vec::new();
        if marks {
            spans.push(row.mark.clone().unwrap_or_else(|| Span::raw(" ")));
            spans.push(Span::raw(" "));
        }
        spans.push(Span::raw(format!(
            "{}{}",
            row.label,
            " ".repeat(label_width.saturating_sub(glyphs::display_width(&row.label)))
        )));
        spans.extend(
            values
                .next()
                .map(|value| Span::raw(fit(&value, value_width))),
        );
        lines.push(Line::from(spans));
        lines.extend(values.map(|value| {
            Line::raw(format!(
                "{}{}",
                " ".repeat(lead + label_width),
                fit(&value, value_width)
            ))
        }));
    }
    lines
}

/// `lines` in `area`; when they run past it, the last row counts what is below
/// rather than showing half of it.
fn render_counted(mut lines: Vec<Line<'static>>, area: Rect, theme: &Theme, buf: &mut Buffer) {
    let height = area.height as usize;
    if lines.len() > height && height > 0 {
        let below = lines.len() - (height - 1);
        lines.truncate(height - 1);
        lines.push(Line::styled(
            format!("  {} {below} more", glyphs::get().ellipsis),
            Style::default().fg(theme.get("dimmed")),
        ));
    }
    Paragraph::new(lines).render(area, buf);
}

/// What was measured on a column, in the table's own formatting. A measurement
/// that does not apply to its type is left out rather than shown as a dash, and
/// what the header says (the rows checked, whether sampled) is not repeated.
fn detail_measurements(
    ctx: &RenderContext,
    results: &DataQualityResults,
    profile: &ColumnQualityProfile,
) -> Vec<FieldRow> {
    // Long text is cut, so both ends of a range and a count after a value stay
    // in view.
    let value = |text: &str| fit(&table_value(ctx, profile, text), END_WIDTH);
    let row = |label: &str, value: String| FieldRow {
        mark: None,
        label: label.to_string(),
        value,
    };
    let mut rows = vec![
        row("Type", profile.dtype.to_string()),
        row(
            "Missing",
            if profile.null_count == 0 {
                "0".to_string()
            } else {
                format!(
                    "{} ({})",
                    numfmt::group_chrome(profile.null_count),
                    crate::quality_report::percent(profile.null_count, profile.evaluated_rows)
                )
            },
        ),
    ];
    for (label, measured) in [
        ("Distinct", profile.distinct_count),
        ("Empty text", profile.empty_count),
        ("Blank text", profile.whitespace_count),
        ("NaN", profile.nan_count),
    ] {
        if let Some(measured) = measured {
            rows.push(row(label, numfmt::group_chrome(measured)));
        }
    }
    if profile.positive_infinity_count.is_some() || profile.negative_infinity_count.is_some() {
        rows.push(row(
            "Infinite",
            format!(
                "{} positive, {} negative",
                count_label(profile.positive_infinity_count),
                count_label(profile.negative_infinity_count)
            ),
        ));
    }
    if profile.min.is_some() || profile.max.is_some() {
        rows.push(row(
            "Range",
            format!(
                "{} to {}",
                profile.min.as_deref().map(value).unwrap_or_default(),
                profile.max.as_deref().map(value).unwrap_or_default()
            ),
        ));
    }
    if let Some((dominant, count)) = profile.dominant_value.as_ref().zip(profile.dominant_count) {
        rows.push(row(
            "Most common",
            format!(
                "{}, {} {}",
                value(dominant),
                numfmt::group_chrome(count),
                if count == 1 { "row" } else { "rows" }
            ),
        ));
    }
    if profile.min_length.is_some() || profile.max_length.is_some() {
        rows.push(row(
            if matches!(profile.dtype, polars::prelude::DataType::List(_)) {
                "List length"
            } else {
                "Text length"
            },
            format!(
                "{} to {}",
                count_label(profile.min_length),
                count_label(profile.max_length)
            ),
        ));
    }
    // Only the parsers that accepted something: four zeros say less than "none".
    let parses = [
        ("integer", profile.integer_parse_count),
        ("decimal", profile.decimal_parse_count),
        ("date", profile.date_parse_count),
        ("datetime", profile.datetime_parse_count),
    ];
    if parses.iter().any(|(_, count)| count.is_some()) {
        let found = parses
            .iter()
            .filter_map(|(parser, count)| {
                count
                    .filter(|count| *count > 0)
                    .map(|count| format!("{parser} {}", numfmt::group_chrome(count)))
            })
            .collect::<Vec<_>>();
        rows.push(row(
            "Parses as",
            if found.is_empty() {
                "none".to_string()
            } else {
                found.join(", ")
            },
        ));
    }
    for group in results
        .category_variants
        .iter()
        .filter(|group| group.column == profile.name)
        .take(3)
    {
        // Spellings differ by case and by spaces the table does not show, so they are
        // quoted as the finding quotes them: "West " and "West" read apart.
        rows.push(row(
            "Spellings",
            group
                .variants
                .iter()
                .map(|(variant, count)| {
                    format!(
                        "{} ({})",
                        crate::quality_report::quoted(variant, END_WIDTH),
                        numfmt::group_chrome(*count)
                    )
                })
                .collect::<Vec<_>>()
                .join("\n"),
        ));
    }
    rows
}

/// A value the profile holds as text, formatted as the table formats its column:
/// grouped, or to fixed places, when the number format says so; as-is otherwise.
fn table_value(ctx: &RenderContext, profile: &ColumnQualityProfile, text: &str) -> String {
    use polars::prelude::{AnyValue, DataType};
    let formatter = ctx
        .number_format
        .formatter_for(&profile.name, &profile.dtype);
    if formatter.is_passthrough() {
        return text.to_string();
    }
    let parsed = match profile.dtype {
        DataType::Float32 | DataType::Float64 => text.parse::<f64>().ok().map(AnyValue::Float64),
        DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
            text.parse::<u64>().ok().map(AnyValue::UInt64)
        }
        _ => text.parse::<i64>().ok().map(AnyValue::Int64),
    };
    parsed
        .map(|parsed| {
            numfmt::format_any_value(&formatter, &parsed, &mut String::new()).into_owned()
        })
        .unwrap_or_else(|| text.to_string())
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

/// The tool list, on a terminal too narrow to keep it beside the result.
fn render_narrow_tool_picker(
    config: &DataQualityWidgetConfig<'_>,
    sidebar_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let tools = vec![
        "Describe",
        "Distribution Analysis",
        "Correlation Matrix",
        "Data Quality",
    ];
    let popup = centered_rect(28, tools.len() as u16 + 2, area);
    let content = Surface::new("Analysis Tools").render(popup, buf, config.ctx);
    Picker::new(tools, sidebar_state.selected(), true).render(content, buf, config.ctx);
}

/// What a run of the plan will read and write, before it runs.
fn render_access_plan(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let state = config.state;
    let plan = config.plan;
    let remote = state.is_remote_source();
    let row = |label: &str, value: String| FieldRow {
        mark: None,
        label: label.to_string(),
        value,
    };
    let source_files = if plan.scope.uses_source() {
        Some(state.quality_source_file_count())
            .filter(|count| *count > 0)
            .or_else(|| state.source_file_count())
    } else {
        state.source_file_count()
    };
    let rows = [
        row(
            "Source",
            if remote { "remote" } else { "local" }.to_string(),
        ),
        row("Scope", plan.scope.label()),
        row("Grain", plan.grain.label()),
        row("Sample", compute_label(plan)),
        row(
            "Rows evaluated",
            planned_rows(state, plan)
                .map(numfmt::group_chrome)
                .unwrap_or_else(|| "unknown".to_string()),
        ),
        row(
            "Value reads",
            if remote {
                "unknown".to_string()
            } else {
                planned_read_label(state, plan)
            },
        ),
        row(
            "Requests",
            if remote { "unknown" } else { "none" }.to_string(),
        ),
        row(
            "Known source files",
            source_files
                .map(numfmt::group_chrome)
                .unwrap_or_else(|| "unknown".to_string()),
        ),
        row(
            "Conflict values",
            match state.quality_conflict_reads() {
                0 => "none: no file holds a column in an unreadable type".to_string(),
                reads if plan.compute == QualityCompute::Full => {
                    format!("{reads} extra one-column file reads")
                }
                reads => format!("not read; a full scan would add {reads} one-column file reads"),
            },
        ),
        row("Remote writes", "none".to_string()),
        row("Local file writes", "none".to_string()),
        row("Passes", passes_label(config)),
        row(
            "Estimate basis",
            match plan.compute {
                QualityCompute::Metadata => "file footers read when the data opened".to_string(),
                QualityCompute::Sample => {
                    "rows: the sample; bytes: a ceiling, the whole scope".to_string()
                }
                QualityCompute::Full => {
                    "rows: every eligible row; what each pass re-reads depends on the \
                     source and its caches"
                        .to_string()
                }
            },
        ),
    ];
    let width = 72.min(area.width.saturating_sub(2));
    let label_width = rows
        .iter()
        .map(|row| glyphs::display_width(&row.label))
        .max()
        .unwrap_or(0)
        + 2;
    // The frame and its gutters take four columns of the width.
    let lines = field_lines(&rows, label_width, width.saturating_sub(4) as usize, false);
    let popup = centered_rect(width, lines.len() as u16 + 2, area);
    let content = Surface::new("Access Plan").render(popup, buf, config.ctx);
    render_counted(lines, content, config.theme, buf);
}

/// The reads a run makes over its scope, counted from the plan: honest about the
/// passes even where their bytes are unknown.
fn passes_label(config: &DataQualityWidgetConfig<'_>) -> String {
    let plan = config.plan;
    let view = &config.setup;
    match plan.compute {
        _ if view.unchanged || view.cached => "none: the report is already here".to_string(),
        QualityCompute::Metadata => "none".to_string(),
        QualityCompute::Full => format!("up to {}, one per check", full_passes(config)),
        QualityCompute::Sample => {
            let sample = if view.reuses_sample {
                "none for the sample: rows already read"
            } else {
                "one sampling pass"
            };
            match view.segment_count {
                SegmentCount::CountPass => {
                    format!("{sample}, then one count of the grain's column")
                }
                SegmentCount::InSamplePass => format!("{sample}, counting the grain's rows"),
                _ => sample.to_string(),
            }
        }
    }
}

/// A full scan asks first: it may read the whole source.
fn render_run_confirmation(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let width = 60.min(area.width.saturating_sub(2));
    let lines = [
        "This setup reads every eligible row and may read the whole source.",
        "The source stays read-only; remote writes are 0 B.",
    ]
    .iter()
    .flat_map(|text| crate::widgets::info::wrap_to(text, width.saturating_sub(4) as usize))
    .collect::<Vec<_>>();
    let popup = centered_rect(width, lines.len() as u16 + 2, area);
    let content = Surface::new("Full Scan")
        .border_style(Style::default().fg(config.ctx.modal_border_active))
        .render(popup, buf, config.ctx);
    render_counted(
        lines.into_iter().map(Line::raw).collect(),
        content,
        config.theme,
        buf,
    );
}

fn render_run_prompt(area: Rect, theme: &Theme, buf: &mut Buffer) {
    Paragraph::new("No report yet: e opens Setup, and Enter there runs it.")
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

    /// A value that fits keeps its spaces; one that does not wraps under itself.
    #[test]
    fn field_values_keep_their_spaces_and_wrap_under_themselves() {
        let row = |value: &str| FieldRow {
            mark: None,
            label: "Range".to_string(),
            value: value.to_string(),
        };
        let text = |lines: Vec<Line<'static>>| {
            lines
                .iter()
                .map(|line| line.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            text(field_lines(&[row(" South to New  York")], 7, 40, false)),
            ["Range   South to New  York"]
        );
        assert_eq!(
            text(field_lines(&[row("one two three four")], 7, 17, false)),
            ["Range  one two", "       three four"]
        );
    }

    use crate::data_quality::compute_data_quality;
    use polars::prelude::*;

    fn fixture() -> LazyFrame {
        df!(
            "id" => &[1i64, 2, 3, 4, 5, 6, 7, 8],
            "region" => &[
                Some("West"), Some("west"), Some("West"), Some("West "),
                Some("East"), None, Some("North"), Some("West"),
            ],
            "amount" => &[1.5f64, 2.0, 3.25, 4.0, 5.0, 6.0, 7.0, 8.0],
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
    }

    impl Screen {
        fn new() -> Self {
            let lf = fixture();
            let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
            let state = DataTableState::from_schema_and_lazyframe(
                schema,
                lf.clone(),
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap();
            let plan = DataQualityPlan {
                compute: QualityCompute::Full,
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&lf, Some(8), &plan, None, false).unwrap();
            Self {
                state,
                plan,
                results,
                theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
                ctx: RenderContext::for_test(),
            }
        }

        fn config(&self, page: QualityPage) -> DataQualityWidgetConfig<'_> {
            DataQualityWidgetConfig {
                checks_expanded: false,
                state: &self.state,
                plan: &self.plan,
                measured: &self.plan,
                results: Some(&self.results),
                from_cache: false,
                metric: QualityMetric::NullRate,
                column_index: 0,
                segment_index: 0,
                segments_by_change: false,
                page,
                setup: SetupView::default(),
                plan_field: 0,
                show_access: false,
                observation_detail: false,
                confirm_run: false,
                focus: AnalysisFocus::Main,
                theme: &self.theme,
                ctx: &self.ctx,
            }
        }

        fn draw(
            &self,
            config: DataQualityWidgetConfig<'_>,
            selected: usize,
            width: u16,
            height: u16,
        ) -> Vec<String> {
            let area = Rect::new(0, 0, width, height);
            let mut buf = Buffer::empty(area);
            let mut table = TableState::default();
            table.select(Some(selected));
            let mut sidebar = TableState::default();
            sidebar.select(Some(3));
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
                .collect()
        }
    }

    /// The column Detail page: the column on a rule, its findings, then its
    /// measurements as label and value rows whose values start in one column.
    /// No frame, no Rust debug quotes, nothing the header already says.
    #[test]
    fn column_detail_is_aligned_rows_without_a_box() {
        let screen = Screen::new();
        let region = screen
            .results
            .columns
            .iter()
            .position(|column| column.name == "region")
            .unwrap();
        // Narrow enough that the tool list gives way; the page is the whole width.
        let rows = screen.draw(screen.config(QualityPage::Detail), region, 70, 24);
        let text = rows.join("\n");
        for corner in ['╭', '╮', '╰', '╯', '│'] {
            assert!(!text.contains(corner), "no box on the page:\n{text}");
        }
        assert!(
            rows.iter()
                .any(|row| row.trim_start().starts_with("region ─")),
            "the column is named on a rule:\n{text}"
        );
        assert!(
            text.contains("Mixed spellings"),
            "the column's findings are on the page:\n{text}"
        );
        let findings_end = rows
            .iter()
            .position(|row| row.contains("Mixed spellings"))
            .unwrap();
        let type_row = rows.iter().position(|row| row.contains("Type")).unwrap();
        assert!(findings_end < type_row, "findings first:\n{text}");

        // Every measurement's value starts where the first one's does.
        let value_column = |label: &str| {
            let row = rows
                .iter()
                .find(|row| row.trim_start().starts_with(label))
                .unwrap_or_else(|| panic!("{label} row:\n{text}"));
            let after = row.find(label).unwrap() + label.len();
            after + row[after..].len() - row[after..].trim_start().len()
        };
        let column = value_column("Type");
        for label in ["Missing", "Distinct", "Range", "Most common", "Spellings"] {
            assert_eq!(value_column(label), column, "{label} aligned:\n{text}");
        }

        let spellings = rows
            .iter()
            .position(|row| row.contains("Spellings"))
            .unwrap();
        let measurements = rows[type_row..spellings].join("\n");
        assert!(
            !measurements.contains('"'),
            "values as the table shows them, not debug quoted:\n{text}"
        );
        assert!(measurements.contains("West, 3 rows"), "{text}");
        // Spellings differ by what the table cannot show; quoted, they read apart.
        let spellings = rows[spellings..].join("\n");
        for spelling in ["\"West\" (3)", "\"West \" (1)", "\"west\" (1)"] {
            assert!(spellings.contains(spelling), "{spelling}:\n{text}");
        }
        assert!(
            !text.contains("Evaluated") && !text.contains("sampled"),
            "the header says what was evaluated:\n{text}"
        );
    }

    /// Section titles are words on a rule, never SCREAMING.
    #[test]
    fn no_page_has_an_uppercase_title() {
        let screen = Screen::new();
        for page in [
            QualityPage::Setup,
            QualityPage::Overview,
            QualityPage::Columns,
            QualityPage::Segments,
            QualityPage::Trends,
            QualityPage::Detail,
        ] {
            for (width, height) in [(120, 32), (80, 24), (60, 20)] {
                let rows = screen.draw(screen.config(page), 1, width, height);
                for row in &rows {
                    let shouting = row.split(|c: char| !c.is_alphabetic()).find(|word| {
                        word.chars().count() >= 4 && word.chars().all(|c| c.is_uppercase())
                    });
                    assert!(
                        shouting.is_none(),
                        "{page:?} at {width}x{height} shouts {shouting:?}: {row:?}"
                    );
                }
            }
        }
    }

    /// Each dialog is one Surface: one frame, its title on it, nothing boxed
    /// inside, at the smallest size datui supports.
    #[test]
    fn dialogs_are_one_surface() {
        let screen = Screen::new();
        for title in [
            "Access Plan",
            "Full Scan",
            "Mixed spellings",
            "Analysis Tools",
        ] {
            for (width, height) in [(80, 24), (60, 20)] {
                let mut config = screen.config(QualityPage::Setup);
                match title {
                    "Access Plan" => config.show_access = true,
                    "Full Scan" => config.confirm_run = true,
                    // The first finding on the Overview.
                    "Mixed spellings" => {
                        config.page = QualityPage::Overview;
                        config.observation_detail = true;
                    }
                    _ => config.focus = AnalysisFocus::Sidebar,
                }
                let rows = screen.draw(config, 0, width, height);
                let text = rows.join("\n");
                let corners = text.matches('╭').count();
                // At 80 columns the tool list keeps its own frame beside the page.
                let expected = if width >= 76 && title != "Analysis Tools" {
                    2
                } else {
                    1
                };
                assert_eq!(corners, expected, "{title} at {width}x{height}:\n{text}");
                let frame = rows
                    .iter()
                    .find(|row| row.contains(&format!("╭{title}")))
                    .unwrap_or_else(|| panic!("{title} on its frame:\n{text}"));
                assert!(frame.contains('╮'), "{title}:\n{text}");
            }
        }
    }

    /// Setup at the baseline size and the smallest: every row in its section, the
    /// focused one on screen with the rail whichever it is, and every character
    /// outside ASCII a glyph slot, which has an ASCII twin under `LANG=C`.
    #[test]
    fn setup_fits_80x24_and_60x20_in_glyph_slots() {
        let screen = Screen::new();
        let g = glyphs::get();
        let slots = [
            g.rail,
            g.rule_h,
            g.rule_h_focused,
            g.middot,
            g.ellipsis,
            g.selector,
        ]
        .concat();
        for (width, height) in [(80, 24), (60, 20)] {
            for row in SetupRow::ALL {
                let mut config = screen.config(QualityPage::Setup);
                config.plan_field = row.index();
                let rows = screen.draw(config, 0, width, height);
                let text = rows.join("\n");
                assert!(
                    rows.iter()
                        .any(|line| line.contains(&format!("{}{}", g.rail, row.label()))),
                    "{row:?} focused at {width}x{height}:\n{text}"
                );
                for other in SetupRow::ALL {
                    assert!(
                        text.contains(other.label()),
                        "{other:?} on screen at {width}x{height}:\n{text}"
                    );
                }
                for section in ["Rows & sample", "Columns", "Study", "Read"] {
                    assert!(
                        text.contains(section),
                        "{section} at {width}x{height}:\n{text}"
                    );
                }
                for c in text.chars().filter(|c| !c.is_ascii()) {
                    assert!(
                        slots.contains(c) || "╭╮╰╯│─".contains(c),
                        "{c:?} is not a glyph slot at {width}x{height}:\n{text}"
                    );
                }
            }
        }
    }

    /// Setup says what a run will read before it runs, and says it honestly: a full
    /// run counts its passes rather than promising one read, and Setup's own line
    /// says why Enter waits.
    #[test]
    fn setup_states_the_read_and_why_run_waits() {
        let screen = Screen::new();
        let mut config = screen.config(QualityPage::Setup);
        config.show_access = true;
        let text = screen.draw(config, 0, 100, 30).join("\n");
        assert!(!text.contains("at most one read"), "{text}");
        assert!(text.contains("one per check"), "{text}");

        let config = screen.config(QualityPage::Setup);
        let text = screen.draw(config, 0, 100, 30).join("\n");
        assert!(text.contains("passes over the scope"), "{text}");

        let mut config = screen.config(QualityPage::Setup);
        config.setup.cancelling = Some(std::time::Instant::now());
        config.setup.note = Some("Run waits: a cancelled read is still finishing");
        let rows = screen.draw(config, 0, 100, 30);
        let text = rows.join("\n");
        assert!(
            rows[0].contains("Cancellation requested; source read finishing"),
            "the header keeps the state: {text}"
        );
        assert!(
            text.contains("Run waits: source read finishing"),
            "Setup says why Enter did not run: {text}"
        );
    }
}
