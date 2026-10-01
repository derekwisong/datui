use crate::analysis_modal::{AnalysisFocus, AnalysisTool, DetailScroll, EvidenceRead, SetupRow};
use crate::config::Theme;
use crate::data_quality::{
    ColumnQualityProfile, DataQualityPlan, DataQualityResults, IntervalClock, IntervalFact,
    ObservationKind, QualityComparison, QualityCompute, QualityGrain, QualityMetric, QualityPage,
    QualityPrecision, QualityScope, SegmentCount, TemporalLatencyProfile, TemporalRole,
    interval_label, window_cadence,
};
use crate::glyphs;
use crate::numfmt;
use crate::quality_report::{
    CHECKS_SHOWN, Check, Coverage, EvidenceRows, FindingOrder, FindingsView, Outcome,
    QualityReport, Severity, advice, build_report, checks, coverage, describe, verdict,
};
use crate::quality_trends::{
    GapCheck, GapKind, Gaps, TrendBar, TrendMeasure, TrendRow, TrendView, trend_view,
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
    /// The draft differs from the report on screen only in the windows it expects,
    /// which Run checks against the counts the report holds.
    pub expectation_only: bool,
    /// Setup holds edits Esc would discard.
    pub edited: bool,
    /// Why Enter did not run.
    pub note: Option<&'a str>,
    /// A cancelled run still going, while the screen should say so.
    pub cancelling: Option<Cancelling>,
}

/// A cancelled run that has not exited yet.
#[derive(Debug, Clone, Copy)]
pub struct Cancelling {
    /// When the cancel came.
    pub since: std::time::Instant,
    /// The cancel came during a read of the source nothing can stop, which runs to
    /// its end; otherwise the run is stopping and has outlasted its batch.
    pub read_runs_out: bool,
}

impl Cancelling {
    /// What is still going, after "Cancellation requested; " or "Run waits: ".
    fn what(self) -> &'static str {
        if self.read_runs_out {
            "source read finishing"
        } else {
            "run stopping"
        }
    }
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
    /// The interval a detail shows.
    pub interval_index: usize,
    /// The Trends line a bar detail shows.
    pub trend_line: usize,
    /// The Expected editor, while it is open.
    pub expected_form: Option<&'a crate::analysis_modal::ExpectedForm>,
    pub segments_by_change: bool,
    pub page: QualityPage,
    pub setup: SetupView<'a>,
    pub plan_field: usize,
    pub show_access: bool,
    pub observation_detail: bool,
    pub confirm_run: bool,
    /// How Overview narrows and orders the findings.
    pub findings: &'a FindingsView,
    /// The rows the report measured are still in memory: a finding's open from them.
    pub rows_kept: bool,
    /// A read of a finding's rows, waiting for Enter.
    pub evidence_read: Option<&'a EvidenceRead>,
    pub focus: AnalysisFocus,
    pub theme: &'a Theme,
    /// The dialogs' Surfaces and the table's number formatting.
    pub ctx: &'a RenderContext,
    /// One column's declared intent, being edited over the Column intent list.
    pub intent_form: Option<&'a crate::intent_modal::IntentForm>,
    /// The dialog writing the report on screen to a file.
    pub export_form: Option<&'a crate::quality_export::ExportForm>,
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
        QualityPage::IntervalPairs => render_interval_pairs(&config, table_state, body, buf),
        QualityPage::Intent => {
            crate::widgets::quality_intent::render_list(&config, table_state, body, buf)
        }
        QualityPage::Overview => render_overview(&config, table_state, body, buf),
        QualityPage::Columns => render_columns(&config, table_state, body, buf),
        QualityPage::Segments => render_segments(&config, table_state, body, buf),
        QualityPage::SegmentDetail => {
            render_segment_detail(&config, table_state, config.segment_index, body, buf)
        }
        QualityPage::Trends => render_trends(&config, table_state, body, buf),
        QualityPage::TrendDetail => render_trend_detail(&config, table_state, body, buf),
        QualityPage::Gaps => render_gaps(&config, table_state, body, buf),
        QualityPage::ExpectedWindows => render_expected_windows(&config, body, buf),
        QualityPage::Intervals => render_intervals(&config, table_state, body, buf),
        QualityPage::IntervalDetail => render_interval_detail(&config, table_state, body, buf),
        QualityPage::Detail => render_detail(&config, table_state, body, buf),
    }

    if let Some(form) = config.intent_form {
        crate::widgets::quality_intent::render_form(form, &config, area, buf);
    } else if let Some(form) = config.export_form {
        crate::widgets::quality_export::render(form, &config, area, buf);
    } else if config.show_access {
        render_access_plan(&config, area, buf);
    } else if config.observation_detail {
        render_finding_detail(&config, table_state, detail_scroll, area, buf);
    } else if config.confirm_run {
        render_run_confirmation(&config, area, buf);
    } else if sidebar_width == 0 && config.focus == AnalysisFocus::Sidebar {
        render_narrow_tool_picker(&config, sidebar_state, area, buf);
    }
    // A staged read of rows sits over whatever asked for it: a finding or a count.
    if let Some(read) = config.evidence_read {
        render_evidence_read(&config, read, area, buf);
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
    if let Some(cancelling) = config.setup.cancelling {
        spans.push(Span::styled(
            format!(
                "  Cancellation requested; {} {} {}",
                cancelling.what(),
                glyphs::get().middot,
                crate::render::analysis_view::elapsed(cancelling.since.elapsed())
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
    lines.push(SetupLine::Row(SetupRow::Intervals));
    lines.push(SetupLine::Row(SetupRow::Intent));
    for (note, warn) in column_notes(plan, schema) {
        for line in crate::widgets::info::wrap_to(&note, width.saturating_sub(2)) {
            lines.push(SetupLine::Note(line, warn));
        }
    }
    lines.push(SetupLine::Rule("Study", None));
    for row in [
        SetupRow::Grain,
        SetupRow::Expected,
        SetupRow::Compare,
        SetupRow::Values,
        SetupRow::Latency,
        SetupRow::WindowBy,
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
        SetupRow::Intervals if plan.candidate_pairs().is_empty() => {
            ("needs two time roles".to_string(), true)
        }
        SetupRow::Intervals if plan.interval_pairs().is_empty() => ("none".to_string(), true),
        SetupRow::Intervals => (
            plan.interval_pairs()
                .into_iter()
                .map(interval_label)
                .collect::<Vec<_>>()
                .join(", "),
            false,
        ),
        SetupRow::Intent if plan.intent.is_empty() => ("none".to_string(), true),
        SetupRow::Intent => (plan.intent.summary(), false),
        SetupRow::Grain => (plan.grain.label(), false),
        SetupRow::Expected => match (&plan.grain, plan.expected.as_ref()) {
            (QualityGrain::TimeWindows { every, .. }, Some(expected)) => (
                format!(
                    "{}, {}",
                    expected.cadence_label(every),
                    expected.range_label()
                ),
                false,
            ),
            (QualityGrain::TimeWindows { .. }, None) => {
                ("none: no window is a gap".to_string(), true)
            }
            _ => ("needs a time-window grain".to_string(), true),
        },
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
        SetupRow::WindowBy => match (&plan.grain, plan.interval_clock) {
            (QualityGrain::TimeWindows { .. }, _) if plan.interval_pairs().is_empty() => {
                ("needs an interval".to_string(), true)
            }
            (QualityGrain::TimeWindows { column, .. }, IntervalClock::Grain) => {
                (format!("the grain's column, {column}"), false)
            }
            (QualityGrain::TimeWindows { .. }, clock) => (clock.label().to_string(), false),
            _ => ("needs a time-window grain".to_string(), true),
        },
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
    // A role that is in no interval measures nothing; say which, and where to fix it.
    let unpaired = plan.unpaired_roles();
    if plan.temporal_roles.len() == 1 {
        notes.push((
            "One role makes no interval: assign another under Time roles".to_string(),
            true,
        ));
    } else if !unpaired.is_empty() {
        notes.push((
            format!(
                "In no interval: {}. Choose a start and end under Intervals",
                unpaired
                    .iter()
                    .map(|role| role.label())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            true,
        ));
    }
    // An interval from a time with no zone to an instant reads the first as UTC.
    let mut naive = Vec::new();
    for (start, end) in plan.interval_pairs() {
        let (Some(start), Some(end)) = (plan.role_column(start), plan.role_column(end)) else {
            continue;
        };
        for (column, other) in [(start, end), (end, start)] {
            if plan.zoned(column, schema) == Some(false)
                && plan.zoned(other, schema) == Some(true)
                && !naive.contains(&column)
            {
                naive.push(column);
            }
        }
    }
    if !naive.is_empty() {
        notes.push((
            format!("No time zone, read as UTC: {}", naive.join(", ")),
            false,
        ));
    }
    // Intent on a column the scope does not have measures nothing.
    let absent = plan
        .intent
        .declared_columns()
        .into_iter()
        .filter(|column| schema.get(column).is_none())
        .collect::<Vec<_>>();
    if !absent.is_empty() {
        notes.push((
            format!("Not in this scope, so not checked: {}", absent.join(", ")),
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
    if view.expectation_only {
        lines.push(
            "Only Expected changed: Run checks the report on screen against it, no read"
                .to_string(),
        );
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
        QualityCompute::Full => {
            lines.push(format!(
                "Every eligible row, in up to {} passes over the scope: one per check",
                full_passes(config)
            ));
            // Each column an interval is windowed by is a grouping of its own.
            let clocks =
                crate::data_quality::interval_passes(plan, state.quality_schema(&plan.scope));
            if clocks > 1 {
                lines.push(format!(
                    "Window by {}: {clocks} of those passes for intervals, one per column",
                    plan.interval_clock.label()
                ));
            }
        }
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
                "Too many segments {} to count; choose a coarser grain",
                plan.grain.label()
            )),
            SegmentCount::NotNeeded | SegmentCount::PerValue => {}
        }
    }
    if plan.compute == QualityCompute::Sample {
        lines.push("Then measured in memory: no further reads".to_string());
    }
    lines.extend(intent_read_lines(plan, exact));
    // Gaps come from the segment counts the run already takes, never a read of
    // their own.
    if plan.expected_windows().is_some() && plan.compute != QualityCompute::Metadata {
        lines.push("Expected windows: checked against the segment counts, no read".to_string());
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

/// What the declared intent costs, said before Run: nothing past the rows read, but
/// a key on a sample speaks only for the sampled rows, and on a full scan it is a
/// grouping of its own.
fn intent_read_lines(plan: &DataQualityPlan, every_row: bool) -> Vec<String> {
    if plan.intent.is_empty() {
        return Vec::new();
    }
    let key = !plan.intent.key.is_empty();
    match plan.compute {
        QualityCompute::Metadata => vec!["Column intent needs values: not checked".to_string()],
        QualityCompute::Full if key => vec![
            "Column intent: counted in the profile pass; the key adds one pass over its columns"
                .to_string(),
        ],
        QualityCompute::Full => {
            vec!["Column intent: counted in the profile pass, no extra pass".to_string()]
        }
        QualityCompute::Sample => {
            let mut lines =
                vec!["Column intent: checked on the rows read, no extra read".to_string()];
            if key && !every_row {
                lines.push(format!(
                    "Key: finds repeats among the {} sampled rows only; Every row checks them all",
                    numfmt::group_chrome(plan.dataset_rows)
                ));
            }
            lines
        }
    }
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
    passes += crate::data_quality::interval_passes(plan, state.quality_schema(&plan.scope));
    // The declared key is a grouping of its columns: one pass of its own.
    passes += usize::from(!plan.intent.key.is_empty());
    if planned_scope_rows(state, plan).is_none() {
        passes += 1;
    }
    passes + state.quality_conflict_reads()
}

/// Setup's bottom line: a cancelled read still running, why Enter did not run, or
/// that the draft differs from the report it would replace.
fn setup_status(config: &DataQualityWidgetConfig<'_>) -> Option<(String, bool)> {
    let view = &config.setup;
    if let Some(cancelling) = view.cancelling {
        // Said as a reason once Enter has been refused for it, short enough to keep
        // its clock beside the tool list at 80 columns.
        return Some((
            format!(
                "{}{} {} {}",
                if view.note.is_some() {
                    "Run waits: "
                } else {
                    "Cancellation requested; "
                },
                cancelling.what(),
                glyphs::get().middot,
                crate::render::analysis_view::elapsed(cancelling.since.elapsed())
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
    let all_checks = checks(results, &report);
    let notes = config.state.notes();
    let notes_height = if notes.is_empty() {
        0
    } else {
        (notes.len() as u16).min(3) + 2
    };
    // Coverage sits under the verdict on every report, clean or not: four lines
    // where there is room, two on a short terminal.
    let coverage = coverage_lines(
        &coverage(results, &all_checks, config.measured),
        area.width.saturating_sub(2),
        if area.height >= 16 { 4 } else { 2 },
        config.theme,
    );
    let sections = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2 + coverage.len() as u16),
            Constraint::Length(notes_height),
            Constraint::Fill(1),
        ])
        .horizontal_margin(1)
        .vertical_margin(1)
        .split(area);
    render_verdict(config, &report, sections[0], buf);
    Paragraph::new(coverage).render(
        Rect {
            y: sections[0].y + 1,
            height: sections[0].height.saturating_sub(1),
            ..sections[0]
        },
        buf,
    );
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

    let mut list = sections[2];
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
    let shown = config.findings.shown(&report);
    // Narrowed or reordered, the list says so above itself, and what it holds.
    let facets = config.findings.describe();
    if !facets.is_empty() && list.height > 1 {
        let total = report
            .findings
            .iter()
            .filter(|finding| finding.kind.is_some())
            .count();
        let listed = shown
            .iter()
            .filter(|index| report.findings[**index].kind.is_some())
            .count();
        let middot = glyphs::get().middot;
        let mut text = if config.findings.narrowed() {
            format!("{listed} of {total} findings")
        } else {
            format!(
                "{total} {}",
                if total == 1 { "finding" } else { "findings" }
            )
        };
        for facet in &facets {
            text.push_str(&format!(" {middot} {facet}"));
        }
        Paragraph::new(Line::styled(
            fit(&text, list.width as usize),
            Style::default().fg(config.theme.get("dimmed")),
        ))
        .render(Rect { height: 1, ..list }, buf);
        list.y += 1;
        list.height -= 1;
    }
    normalize_selection(table_state, shown.len());
    if shown.is_empty() {
        Paragraph::new("No findings match. Esc shows them all.")
            .wrap(Wrap { trim: true })
            .style(Style::default().fg(config.theme.get("dimmed")))
            .render(list, buf);
        return;
    }
    if report.problems == 0 && report.notes == 0 && !report.metadata_only {
        // Nothing to fix: what was checked is the answer, so it is on the page
        // rather than behind the clean entry.
        let parts = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(3), Constraint::Fill(1)])
            .split(list);
        render_findings(config, &report, table_state, parts[0], buf);
        Paragraph::new(check_lines(&all_checks, parts[1].width, None, config.theme))
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

/// The label gutter of the coverage lines, as wide as its longest label and a gap.
const COVERAGE_LABEL: usize = 8;

/// The coverage under the verdict: checks, rows and limits, each a labeled line of
/// facts that wraps between facts. Within `max_lines` the rows give way first, since
/// the header states them too, and a section cut short counts what it left out.
fn coverage_lines(
    coverage: &Coverage,
    width: u16,
    max_lines: usize,
    theme: &Theme,
) -> Vec<Line<'static>> {
    let mut sections = [
        ("Checks", coverage.checks()),
        ("Rows", coverage.rows.clone()),
        ("Limits", coverage.limits()),
    ]
    .into_iter()
    .filter(|(_, facts)| !facts.is_empty())
    .collect::<Vec<_>>();
    if sections.len() > max_lines {
        sections.retain(|(label, _)| *label != "Rows");
    }
    sections.truncate(max_lines);
    let text_width = (width as usize).saturating_sub(COVERAGE_LABEL).max(1);
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let plain = Style::default().fg(theme.get("text_primary"));
    let mut lines = Vec::new();
    for (index, (label, facts)) in sections.iter().enumerate() {
        // Every later section keeps one line; this one may take the rest.
        let later = sections.len() - index - 1;
        let room = max_lines.saturating_sub(lines.len() + later).max(1);
        for (row, text) in pack_facts(facts, text_width, room).into_iter().enumerate() {
            let gutter = if row == 0 {
                format!("{label:<COVERAGE_LABEL$}")
            } else {
                " ".repeat(COVERAGE_LABEL)
            };
            lines.push(Line::from(vec![
                Span::styled(gutter, dimmed),
                Span::styled(text, plain),
            ]));
        }
    }
    lines
}

/// `facts` joined by middots into at most `room` lines of `width`, breaking between
/// facts. What does not fit is counted at the end of the last line: "+2 more".
fn pack_facts(facts: &[String], width: usize, room: usize) -> Vec<String> {
    let sep = format!(" {} ", glyphs::get().middot);
    let join = |line: &[String]| line.join(&sep);
    let mut lines: Vec<Vec<String>> = vec![Vec::new()];
    let mut placed = 0;
    for fact in facts {
        let fact = fit(fact, width);
        let current = lines.last_mut().expect("one line at least");
        let mut joined = current.clone();
        joined.push(fact.clone());
        if current.is_empty() || glyphs::display_width(&join(&joined)) <= width {
            *current = joined;
        } else if lines.len() < room {
            lines.push(vec![fact]);
        } else {
            break;
        }
        placed += 1;
    }
    let left = facts.len() - placed;
    let last = lines.last_mut().expect("one line at least");
    if left > 0 {
        // Make room for the count by giving up facts from the end of the line.
        let mut dropped = left;
        loop {
            let more = format!("+{dropped} more");
            let mut line = last.clone();
            line.push(more);
            if glyphs::display_width(&join(&line)) <= width || last.is_empty() {
                *last = line;
                break;
            }
            // A line's only fact is cut short rather than given up, so a line
            // never says only how much it left out.
            if let [only] = last.as_mut_slice() {
                let room =
                    width.saturating_sub(glyphs::display_width(&format!("{sep}+{dropped} more")));
                if room >= 8 {
                    *only = fit(only, room);
                    continue;
                }
            }
            last.pop();
            dropped += 1;
        }
    }
    lines.iter().map(|line| join(line)).collect()
}

/// A title on a rule with a flat count chip, as `SectionRule` draws it, from the
/// theme this widget is handed.
pub(crate) fn rule_line(
    title: &str,
    chip: Option<&str>,
    width: u16,
    theme: &Theme,
) -> Line<'static> {
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
    // Positions in the list as shown, which is what the selection counts; each
    // severity's rule counts what is shown under it.
    let shown = config.findings.shown(report);
    let mut items = Vec::new();
    let mut current = None;
    for (position, index) in shown.iter().enumerate() {
        let finding = &report.findings[*index];
        if current != Some(finding.severity) {
            if current.is_some() {
                items.push(Item::Gap);
            }
            let count = shown
                .iter()
                .filter(|other| report.findings[**other].severity == finding.severity)
                .count();
            let count = match finding.severity {
                // The clean entry is one row naming many columns; count the columns.
                Severity::Clean => report.clean_columns,
                _ => count,
            };
            items.push(Item::Rule(finding.severity, count));
            current = Some(finding.severity);
        }
        items.push(Item::Finding(position));
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
    // Ordered by a number, the number is on every row, right-aligned, so the order
    // can be read; the summary gives up the room.
    let ordered = config.findings.order != FindingOrder::Ranked;
    let numbers = |finding: &crate::quality_report::Finding| {
        if finding.kind.is_none() {
            return (String::new(), String::new());
        }
        (
            numfmt::group_chrome(finding.affected_rows),
            crate::quality_report::percent(finding.affected_rows, finding.evaluated_rows),
        )
    };
    let widest = |pick: fn((String, String)) -> String| {
        shown
            .iter()
            .map(|index| glyphs::display_width(&pick(numbers(&report.findings[*index]))))
            .max()
            .unwrap_or(0)
    };
    let (count_width, rate_width) = (widest(|(count, _)| count), widest(|(_, rate)| rate));
    // The number the list is ordered by leads, each in its own aligned column.
    let affected = |finding: &crate::quality_report::Finding| {
        let (count, rate) = numbers(finding);
        match config.findings.order {
            FindingOrder::Rate => format!("{rate:>rate_width$}  {count:>count_width$}"),
            _ => format!("{count:>count_width$}  {rate:>rate_width$}"),
        }
    };
    let affected_width = if ordered {
        count_width + rate_width + 3
    } else {
        0
    };
    let summary_width = rest.saturating_sub(columns_width + 1 + affected_width);
    // Too narrow to say anything, the summary gives its room to the numbers.
    let (summary_width, affected_width) = if ordered && summary_width < 12 {
        (
            0,
            rest.saturating_sub(columns_width + 1).max(affected_width),
        )
    } else {
        (summary_width, affected_width)
    };
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
            Item::Finding(position) => {
                let finding = &report.findings[shown[*position]];
                let is_selected = *position == selected;
                let columns = fit(&finding.columns_label(columns_width), columns_width);
                let summary = fit(&finding.summary, summary_width);
                let mut spans = vec![
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
                    Span::styled(
                        if ordered {
                            format!("{summary:<summary_width$}")
                        } else {
                            summary
                        },
                        Style::default().fg(theme.get("dimmed")),
                    ),
                ];
                if ordered {
                    spans.push(Span::styled(
                        format!("{:>affected_width$}", affected(finding)),
                        Style::default().fg(theme.get("text_primary")),
                    ));
                }
                let line = Line::from(spans);
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
        Outcome::Skipped(reason) => format!("skipped: {reason}"),
        Outcome::Unavailable(reason) => format!("unavailable: {reason}"),
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
            Outcome::Skipped(_) | Outcome::Unavailable(_) => (Span::styled(g.dash, dimmed), dimmed),
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
/// A segment's label as the terminal draws it: the `∅` naming rows with no value
/// is the null glyph, which `LANG=C` swaps for its ASCII twin. The label itself
/// stays as it is, since evidence finds a segment's rows by it.
fn segment_text(label: &str) -> String {
    label.replace('∅', glyphs::get().null)
}

pub(crate) fn fit(text: &str, width: usize) -> String {
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
        .and_then(|position| config.findings.selected(&report, position))
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
        && !finding.lists_columns()
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
        lines.push(Line::raw(""));
        lines.push(Line::styled(
            evidence_line(config, finding, results),
            dimmed,
        ));
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

/// What Enter does with a finding's rows, in one sentence: shows the ones the run
/// kept, reads ones it did not keep once asked, or says why there are none.
fn evidence_line(
    config: &DataQualityWidgetConfig<'_>,
    finding: &crate::quality_report::Finding,
    results: &DataQualityResults,
) -> String {
    let rows = match finding.evidence(results) {
        Ok(rows) => rows,
        Err(reason) => return format!("{reason}."),
    };
    if let EvidenceRows::Files(QualityScope::SourceFiles(files)) = &rows {
        return format!(
            "Enter asks before reading the rows of the {} named {}.",
            files.len(),
            if files.len() == 1 { "file" } else { "files" }
        );
    }
    let sampled = results.precision == QualityPrecision::Sampled;
    let noun = |count: usize| {
        format!(
            "{} {}{}",
            numfmt::group_chrome(count),
            if sampled { "sampled " } else { "" },
            if count == 1 { "row" } else { "rows" }
        )
    };
    let what = match (finding.kind, finding.evidence_count(results)) {
        // The measurement counts rows beyond one per value; the rows that share a
        // value are always more.
        (Some(ObservationKind::KeyLike), _) => format!(
            "every {}row that shares a repeated value",
            if sampled { "sampled " } else { "" }
        ),
        (Some(ObservationKind::DuplicateRows), Some(count)) => {
            format!("the {} that have a copy", noun(count))
        }
        (Some(ObservationKind::ParseableText), Some(count)) => {
            format!("the {} that do not parse", noun(count))
        }
        (_, Some(count)) => format!("the {}", noun(count)),
        // Grouped columns: a row missing in any of them; the table counts them.
        (_, None) => format!(
            "the {}rows with any of them",
            if sampled { "sampled " } else { "" }
        ),
    };
    if config.rows_kept {
        let together = if matches!(rows, EvidenceRows::Duplicates) {
            ", copies together"
        } else {
            ""
        };
        format!("Enter shows {what}{together}.")
    } else if sampled {
        format!("The sampled rows are no longer kept: Enter asks before reading {what} again.")
    } else if config.measured.compute == QualityCompute::Full {
        format!("A full scan keeps no rows: Enter asks before reading {what}.")
    } else {
        format!("The rows read are no longer kept: Enter asks before reading {what}.")
    }
}

/// A read of a finding's rows, before it reads: what, why, how much, from where.
fn render_evidence_read(
    config: &DataQualityWidgetConfig<'_>,
    read: &EvidenceRead,
    area: Rect,
    buf: &mut Buffer,
) {
    let rows = read
        .summary
        .iter()
        .map(|(label, value)| FieldRow {
            mark: None,
            label: label.to_string(),
            value: value.clone(),
        })
        .collect::<Vec<_>>();
    let width = 72.min(area.width.saturating_sub(2));
    let label_width = rows
        .iter()
        .map(|row| glyphs::display_width(&row.label))
        .max()
        .unwrap_or(0)
        + 2;
    let lines = field_lines(&rows, label_width, width.saturating_sub(4) as usize, false);
    let popup = centered_rect(width, lines.len() as u16 + 2, area);
    let content = Surface::new("Read Rows")
        .border_style(Style::default().fg(config.ctx.modal_border_active))
        .render(popup, buf, config.ctx);
    render_counted(lines, content, config.theme, buf);
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
        .map(|segment| glyphs::display_width(&segment_text(&segment.label)))
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
                Cell::from(segment_text(&segment.label)),
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
        Some(other) => format!(
            "{} vs {}",
            segment_text(&segment.label),
            segment_text(other)
        ),
        None => segment_text(&segment.label),
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
        headers.push(segment_text(other));
        widths.push(Constraint::Length(value_width(&segment_text(other))));
    }
    headers.push(segment_text(&segment.label));
    widths.push(Constraint::Length(value_width(&segment_text(
        &segment.label,
    ))));
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
    let [area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    if crate::data_quality::shows_trend(config.plan, results) {
        render_trend_table(config, results, table_state, area, buf);
        return;
    }
    let [title, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .areas(area);
    Paragraph::new(rule_line(
        "Across segments",
        None,
        title.width,
        config.theme,
    ))
    .render(title, buf);
    let mut text = vec![Line::raw(
        "Set Grain in Setup (e) to a partition column, to days, weeks or months of a \
         date, or to chunks of rows, to follow each column from one to the next.",
    )];
    // One window found is no trend, but the windows expected around it still are.
    if let Some(gaps) = crate::quality_trends::expected_gaps(config.plan, results) {
        text.extend([Line::raw(""), Line::raw(gaps_summary(config.plan, &gaps))]);
    }
    Paragraph::new(text)
        .wrap(Wrap { trim: true })
        .style(Style::default().fg(config.theme.get("text_primary")))
        .render(body, buf);
}

/// `count` of `of`, with its rate when there is one: `2 of 5 (40.0%)`.
fn share(count: usize, of: usize) -> String {
    if of == 0 || count == 0 {
        format!(
            "{} of {}",
            numfmt::group_chrome(count),
            numfmt::group_chrome(of)
        )
    } else {
        format!(
            "{} of {} ({})",
            numfmt::group_chrome(count),
            numfmt::group_chrome(of),
            rate_label(count as f64 / of as f64)
        )
    }
}

/// The rate of `count` in `of`, or a dash with nothing to take it over.
fn share_rate(count: usize, of: usize) -> String {
    // A dash as `duration_label` has it, beside which it sits.
    if of == 0 {
        "-".to_string()
    } else {
        rate_label(count as f64 / of as f64)
    }
}

/// Each interval in each segment, one row each: which, where, and the counts a
/// glance needs. Enter opens every count it took, so nothing is lost to a narrow
/// terminal.
fn render_intervals(
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
    let plan = config.plan;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let [title, note, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .margin(1)
        .areas(area);
    let profiles = &results.temporal;
    let chip = (!profiles.is_empty()).then(|| numfmt::group_chrome(profiles.len()));
    Paragraph::new(rule_line(
        "Time between dates",
        chip.as_deref(),
        title.width,
        theme,
    ))
    .render(title, buf);
    if profiles.is_empty() {
        let schema = config.state.quality_schema(&plan.scope);
        let message = if config.setup.time_candidates.is_empty() {
            "No date, time or text columns, so no time between dates to measure."
        } else if plan.compute == QualityCompute::Metadata && !plan.interval_pairs().is_empty() {
            "File metadata only reads no values, so no time between dates. Set Values \
             to Read in Setup (e)."
        } else if plan.interval_pairs().iter().any(|(start, end)| {
            [start, end].into_iter().any(|role| {
                plan.role_column(*role)
                    .is_some_and(|column| !plan.reads_as_time(column, schema))
            })
        }) {
            "An interval's column is text with no format. Choose one under Text as \
             time in Setup (e)."
        } else if !plan.interval_pairs().is_empty() {
            "No rows to measure the time between dates on."
        } else if !plan.candidate_pairs().is_empty() {
            "The time roles make no interval. Choose a start and an end under \
             Intervals in Setup (e)."
        } else {
            "Assign Time roles in Setup (e), such as when a row happened and when it \
             was received, to measure the time between them. Text is read as time \
             through a format chosen under Text as time."
        };
        Paragraph::new(message)
            .wrap(Wrap { trim: true })
            .style(Style::default().fg(theme.get("text_primary")))
            .render(
                Rect {
                    height: body.height + 1,
                    ..note
                },
                buf,
            );
        return;
    }
    // What the last column counts, and out of what, said once above it.
    let threshold = plan.latency_threshold_seconds;
    let over = match threshold {
        Some(_) => format!(
            "Over: duration > {}, of rows with both ends",
            crate::analysis_modal::threshold_label(threshold)
        ),
        None => "Negative: end before start, of rows with both ends".to_string(),
    };
    Paragraph::new(Line::styled(fit(&over, note.width as usize), dimmed)).render(note, buf);

    let segmented = profiles
        .iter()
        .any(|profile| profile.segment != profiles[0].segment);
    let segment_width = profiles
        .iter()
        .map(|profile| glyphs::display_width(&segment_text(&profile.segment)))
        .max()
        .unwrap_or(0)
        .clamp(8, 22) as u16;
    let last = |profile: &TemporalLatencyProfile, counted: bool| {
        let count = match profile.above_threshold_count {
            Some(count) if threshold.is_some() => count,
            _ => profile.negative_count,
        };
        let rate = share_rate(count, profile.paired_rows);
        if counted && profile.paired_rows > 0 {
            format!("{} ({rate})", numfmt::group_chrome(count))
        } else {
            rate
        }
    };
    let last_header = if threshold.is_some() {
        "Over"
    } else {
        "Negative"
    };
    let wide = body.width >= 92;
    let medium = !wide && body.width >= 44;
    let mut headers = vec!["Interval"];
    let mut fixed = vec![];
    if segmented && (wide || medium) {
        headers.push("Segment");
        fixed.push(segment_width);
    }
    if wide {
        headers.extend(["Both ends", "Missing s/e", "p50", "p95"]);
        fixed.extend([9, 12, 7, 7]);
    } else {
        headers.push("p50");
        fixed.push(7);
    }
    headers.push(last_header);
    fixed.push(if wide { 15 } else { 9 });
    // What the interval's name has left: a long one ends in an ellipsis rather
    // than losing its last letters unmarked, and the detail says it in full.
    let interval_width = (body.width as usize).saturating_sub(
        glyphs::display_width(glyphs::get().selector)
            + fixed.iter().map(|width| *width as usize + 1).sum::<usize>(),
    );
    let rows = profiles.iter().map(|profile| {
        let mut cells = vec![fit(&profile.label(), interval_width)];
        if segmented && (wide || medium) {
            cells.push(fit(&segment_text(&profile.segment), segment_width as usize));
        }
        if wide {
            cells.extend([
                numfmt::group_chrome(profile.paired_rows),
                format!(
                    "{} / {}",
                    numfmt::group_chrome(profile.missing_start),
                    numfmt::group_chrome(profile.missing_end)
                ),
                duration_label(profile.p50_seconds),
                duration_label(profile.p95_seconds),
            ]);
        } else {
            cells.push(duration_label(profile.p50_seconds));
        }
        cells.push(last(profile, wide));
        Row::new(cells)
    });
    let widths = std::iter::once(Constraint::Fill(1))
        .chain(fixed.into_iter().map(Constraint::Length))
        .collect::<Vec<_>>();
    normalize_selection(table_state, profiles.len());
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(dimmed))
        .row_highlight_style(theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, body, buf, table_state);
}

/// One interval in one segment: its endpoints and every count it took, each out
/// of what it is out of. Missing ends, text a format did not read, and negative
/// durations are separate rows, never folded together. The counts with rows
/// behind them take the cursor; Enter opens those rows. From the measurements the
/// report holds: nothing here reads.
fn render_interval_detail(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let Some(results) = config.results else {
        render_run_prompt(area, config.theme, buf);
        return;
    };
    let Some(profile) = results.temporal.get(config.interval_index) else {
        return;
    };
    let theme = config.theme;
    let plan = config.plan;
    let [title, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    Paragraph::new(rule_line(
        &profile.label(),
        Some(&segment_text(&profile.segment)),
        title.width,
        theme,
    ))
    .render(title, buf);

    let endpoint = |role: TemporalRole, column: &str| match plan.time_format(column) {
        Some(format) if format.zoned() => format!("{}: {column}, text with offset", role.label()),
        Some(format) => format!(
            "{}: {column}, text as {}",
            role.label(),
            format.kind.label()
        ),
        None => format!("{}: {column}", role.label()),
    };
    let grain = plan.interval_grain(&profile.start_column, &profile.end_column);
    let facts = IntervalFact::ALL
        .into_iter()
        .filter_map(|fact| profile.count(fact, plan).map(|count| (fact, count)))
        .collect::<Vec<_>>();
    let selected = facts
        .get(
            table_state
                .selected()
                .unwrap_or(0)
                .min(facts.len().saturating_sub(1)),
        )
        .map(|(fact, _)| *fact);
    let plain = |label: &str, value: String| {
        (
            FieldRow {
                mark: None,
                label: label.to_string(),
                value,
            },
            None,
        )
    };
    let fact_row = |fact: IntervalFact| {
        let (count, of) = profile.count(fact, plan)?;
        let value = match fact {
            IntervalFact::Negative | IntervalFact::Zero | IntervalFact::OverThreshold => {
                format!("{} with both ends", share(count, of))
            }
            _ => share(count, of),
        };
        let mark = (selected == Some(fact))
            .then(|| Span::styled(glyphs::get().rail, Style::default().fg(theme.get("accent"))));
        Some((
            FieldRow {
                mark,
                label: fact.label(profile),
                value,
            },
            Some(fact),
        ))
    };
    let mut rows = vec![
        plain("Start", endpoint(profile.start_role, &profile.start_column)),
        plain("End", endpoint(profile.end_role, &profile.end_column)),
    ];
    if grain != QualityGrain::Dataset {
        rows.push(plain(
            "Segment",
            format!("{}, {}", segment_text(&profile.segment), grain.label()),
        ));
    }
    rows.push(plain("Rows", numfmt::group_chrome(profile.evaluated_rows)));
    rows.push(plain(
        "Both ends",
        share(profile.paired_rows, profile.evaluated_rows),
    ));
    rows.extend(
        [
            IntervalFact::MissingStart,
            IntervalFact::MissingEnd,
            IntervalFact::UnparsedStart,
            IntervalFact::UnparsedEnd,
            IntervalFact::Negative,
            IntervalFact::Zero,
        ]
        .into_iter()
        .filter_map(fact_row),
    );
    let pair = |left: Option<i64>, right: Option<i64>| {
        format!("{}, {}", duration_label(left), duration_label(right))
    };
    rows.push(plain(
        "p50, p90",
        pair(profile.p50_seconds, profile.p90_seconds),
    ));
    rows.push(plain(
        "p95, p99",
        pair(profile.p95_seconds, profile.p99_seconds),
    ));
    rows.push(plain("Maximum", duration_label(profile.max_seconds)));
    rows.push(plain(
        "Threshold",
        match profile.threshold_seconds {
            Some(_) => format!(
                "duration > {}, strictly",
                crate::analysis_modal::threshold_label(profile.threshold_seconds)
            ),
            None => "none: set Latency over in Setup".to_string(),
        },
    ));
    rows.extend(fact_row(IntervalFact::OverThreshold));

    let label_width = rows
        .iter()
        .map(|(row, _)| glyphs::display_width(&row.label))
        .max()
        .unwrap_or(0)
        + 2;
    let width = (body.width as usize).min(DETAIL_MEASURE);
    // Each row's lines, and where the selected count's start, so a short terminal
    // scrolls to keep it in view.
    let mut lines = Vec::new();
    let mut focus = 0;
    // Enter opens nothing in a row chunk or a file, so say why under the segment
    // rather than leave it to be found out.
    let closed = (!profile.segment_opens(plan)).then(|| {
        let what = match grain {
            QualityGrain::File => "a file",
            _ => "a row chunk",
        };
        Line::styled(
            fit(
                &format!("  Rows do not open: {what} is not a value to filter on"),
                width,
            ),
            Style::default().fg(theme.get("dimmed")),
        )
    });
    for (row, fact) in &rows {
        if fact.is_some() && *fact == selected {
            focus = lines.len();
        }
        let mut row_lines = field_lines(std::slice::from_ref(row), label_width, width, true);
        if fact.is_some() && *fact == selected {
            for line in &mut row_lines {
                if let Some(label) = line.spans.get_mut(2) {
                    label.style = Style::default().fg(theme.get("accent"));
                }
            }
        }
        lines.extend(row_lines);
        if row.label == "Segment" {
            lines.extend(closed.clone());
        }
    }
    let height = body.height as usize;
    let offset = if lines.len() > height {
        // Room for the count of what is below, and for the focused row; never
        // past the last line, which would leave the bottom blank.
        (focus + 2)
            .saturating_sub(height.saturating_sub(1))
            .min(lines.len() - height)
    } else {
        0
    };
    render_counted(lines.split_off(offset), body, theme, buf);
}

/// Which starts and ends are measured, chosen from every pair the assigned roles
/// make, the suggested ones first. Space turns the pair under the cursor on or off.
fn render_interval_pairs(
    config: &DataQualityWidgetConfig<'_>,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let theme = config.theme;
    let plan = config.plan;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let [title, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(2), Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    let candidates = plan.candidate_pairs();
    let chosen = plan.interval_pairs();
    Paragraph::new(rule_line(
        "Intervals",
        Some(&format!(
            "{} of {}",
            numfmt::group_chrome(chosen.len()),
            numfmt::group_chrome(candidates.len())
        )),
        title.width,
        theme,
    ))
    .render(title, buf);
    if candidates.is_empty() {
        Paragraph::new(Span::styled(
            "Assign two time roles first: an interval runs from one to the other",
            dimmed,
        ))
        .wrap(Wrap { trim: true })
        .render(body, buf);
        return;
    }
    let g = glyphs::get();
    let label_width = candidates
        .iter()
        .map(|pair| glyphs::display_width(&interval_label(*pair)))
        .max()
        .unwrap_or(0) as u16
        + 2;
    // The selector, the box, the label and the gaps between them come first.
    let columns_width = (body.width as usize)
        .saturating_sub(glyphs::display_width(g.selector) + 1 + label_width as usize + 2);
    let rows = candidates.iter().map(|pair| {
        let on = chosen.contains(pair);
        let columns = format!(
            "{} to {}",
            plan.role_column(pair.0).unwrap_or_default(),
            plan.role_column(pair.1).unwrap_or_default()
        );
        Row::new(vec![
            Cell::from(if on { g.checkbox_on } else { g.checkbox_off }),
            Cell::from(interval_label(*pair)),
            Cell::from(Span::styled(fit(&columns, columns_width), dimmed)),
        ])
    });
    table_state.select(Some(
        config.plan_field.min(candidates.len().saturating_sub(1)),
    ));
    let table = Table::new(
        rows,
        [
            Constraint::Length(glyphs::display_width(g.checkbox_on).max(1) as u16),
            Constraint::Length(label_width),
            Constraint::Fill(1),
        ],
    )
    .row_highlight_style(theme.highlight_style())
    .highlight_symbol(g.selector);
    StatefulWidget::render(table, body, buf, table_state);
}

/// The width of the Trends table's range column.
const TREND_RANGE: u16 = 20;

/// The Trends table's name column and how many bars fit beside it and the range,
/// in `width`, past the selector and the gaps between columns. The table and a
/// bar's detail lay out the same, so a bar there is the bar the table drew.
fn trend_layout(results: &DataQualityResults, width: u16) -> (u16, usize) {
    let name_width = results
        .columns
        .iter()
        .map(|profile| glyphs::display_width(&profile.name))
        .max()
        .unwrap_or(0)
        .clamp(12, 28) as u16
        + 2;
    let bars = width.saturating_sub(trend_lead(name_width)).max(8) as usize;
    (name_width, bars)
}

/// Where a Trends line's first bar sits: past the selector, the name, the range
/// and the space after each.
fn trend_lead(name_width: u16) -> u16 {
    glyphs::display_width(glyphs::get().selector) as u16 + name_width + TREND_RANGE + 2
}

/// What a segment of `grain` is called, many of them.
fn trend_unit(grain: &QualityGrain) -> &'static str {
    match grain {
        QualityGrain::TimeWindows { every, .. } => match every.as_str() {
            "1h" => "hours",
            "1d" => "days",
            "1w" => "weeks",
            _ => "months",
        },
        QualityGrain::Partition(_) => "partitions",
        QualityGrain::File => "files",
        _ => "chunks",
    }
}

/// One of `unit`, or many.
fn counted_unit(count: usize, unit: &str) -> String {
    let unit = if count == 1 {
        unit.strip_suffix('s').unwrap_or(unit)
    } else {
        unit
    };
    format!("{} {unit}", numfmt::group_chrome(count))
}

/// A bar's mark in `line`: its level, the unsampled mark where the sample drew no
/// row (so a bar of missed segments is never a short bar or a blank), and a blank
/// where the measure has nothing to apply to. The exact rows line has a level for
/// every bar.
fn bar_mark(line: &TrendRow, bar: &TrendBar, index: usize, g: &glyphs::Glyphs) -> &'static str {
    if bar.evaluated == 0 && line.measure != TrendMeasure::Rows {
        return g.unsampled;
    }
    match line.bars.get(index).copied().flatten() {
        Some(value) if line.high > 0.0 => {
            g.mini_bars[((value / line.high) * 7.0).round().clamp(0.0, 7.0) as usize]
        }
        Some(_) => g.mini_bars[0],
        None => " ",
    }
}

/// A line's lowest and highest bar, as the Range column says them.
fn trend_range(line: &TrendRow) -> String {
    if line.rows() {
        format!(
            "{} to {}",
            numfmt::group_chrome(line.low.round() as usize),
            numfmt::group_chrome(line.high.round() as usize)
        )
    } else if (line.high - line.low).abs() < 1e-9 {
        rate_label(line.high)
    } else {
        format!("{} to {}", rate_label(line.low), rate_label(line.high))
    }
}

/// The coarser grain `w` stages, in words: "weekly".
fn coarser_label(grain: &QualityGrain) -> String {
    match grain {
        QualityGrain::TimeWindows { every, .. } => window_cadence(every).to_string(),
        other => other.label(),
    }
}

/// What the Trends page says under its title: the span, how much of it the sample
/// reached, and the expected windows with no rows. Each fact a line.
fn trend_notes(
    config: &DataQualityWidgetConfig<'_>,
    results: &DataQualityResults,
    view: &TrendView<'_>,
) -> Vec<String> {
    let plan = config.plan;
    let unit = trend_unit(&plan.grain);
    let middot = glyphs::get().middot;
    let first = view.slots.first().map(|slot| slot.label).unwrap_or("");
    let last = view.slots.last().map(|slot| slot.label).unwrap_or("");
    let mut facts = vec![format!(
        "{} to {}, {}",
        segment_text(first),
        segment_text(last),
        counted_unit(view.slots.len(), unit)
    )];
    if view.per_bar > 1 {
        facts.push(format!("each bar {}", counted_unit(view.per_bar, unit)));
    }
    if view.sampled {
        facts.push("a bar's rate pools its sampled rows".to_string());
    }
    let mut notes = vec![facts.join(&format!(" {middot} "))];
    let (unsampled, thin) = view.coverage();
    if view.sampled && unsampled + thin > 0 {
        let mut reach = Vec::new();
        if unsampled > 0 {
            reach.push(format!(
                "{} of {} not sampled ({})",
                numfmt::group_chrome(unsampled),
                counted_unit(view.slots.len(), unit),
                glyphs::get().unsampled
            ));
        }
        if thin > 0 {
            reach.push(format!(
                "{} under {} sampled rows",
                numfmt::group_chrome(thin),
                crate::quality_report::THIN_SEGMENT_ROWS
            ));
        }
        let mut note = reach.join(", ");
        if let Some(coarser) = plan.coarser_grain() {
            note.push_str(&format!("; w stages {}", coarser_label(&coarser)));
        }
        notes.push(note);
    }
    if let Some(gaps) = crate::quality_trends::expected_gaps(plan, results) {
        notes.push(gaps_summary(plan, &gaps));
    }
    notes
}

/// The expected windows in one line, for the Trends page.
fn gaps_summary(plan: &DataQualityPlan, gaps: &Gaps) -> String {
    let every = match &plan.grain {
        QualityGrain::TimeWindows { every, .. } => every.as_str(),
        _ => "",
    };
    let cadence = plan
        .expected
        .as_ref()
        .map(|expected| expected.cadence_label(every))
        .unwrap_or_default();
    let unit = trend_unit(&plan.grain);
    match gaps {
        Gaps::NoValues => format!("Expected {cadence}: file metadata only counts no windows"),
        Gaps::NoWindows => {
            format!("Expected {cadence}: no window found and no range stated")
        }
        Gaps::TooMany { windows } => format!(
            "Expected {cadence}: {} in range, over {}; narrow it in Setup (e)",
            counted_unit(*windows, unit),
            numfmt::group_chrome(crate::quality_trends::MAX_EXPECTED_WINDOWS)
        ),
        // From typed past the last window found, with Before blank.
        Gaps::Checked(check) if check.expected == 0 => {
            format!("Expected {cadence}: no window in range")
        }
        Gaps::Checked(check) if check.gaps() == 0 => format!(
            "Expected {cadence}: all {} have rows",
            counted_unit(check.expected, unit)
        ),
        Gaps::Checked(check) => format!(
            "Expected {cadence}, {}: {}; g lists them",
            counted_unit(check.expected, unit),
            gap_counts(check)
        ),
    }
}

/// "5 empty, 2 not sampled": each kind of gap there is, with its count.
fn gap_counts(check: &GapCheck) -> String {
    [
        (check.empty, GapKind::Empty),
        (check.unsampled, GapKind::Unsampled),
        (check.out_of_scope, GapKind::OutOfScope),
    ]
    .into_iter()
    .filter(|(count, _)| *count > 0)
    .map(|(count, kind)| format!("{} {}", numfmt::group_chrome(count), kind.label()))
    .collect::<Vec<_>>()
    .join(", ")
}

/// Each column's measure across the segments as a line of bars, the rows each
/// segment holds first. The whole range fits the width: a bar pools as many
/// consecutive segments as it takes. Segments the sample missed are bars of their
/// own mark, and the notes say how many, and what expected windows have no rows.
fn render_trend_table(
    config: &DataQualityWidgetConfig<'_>,
    results: &DataQualityResults,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let theme = config.theme;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let (name_width, bars) = trend_layout(results, area.width);
    let view = trend_view(results, config.metric, bars);
    let notes = trend_notes(config, results, &view)
        .iter()
        .flat_map(|note| crate::widgets::info::wrap_to(note, area.width as usize))
        .collect::<Vec<_>>();
    // A short terminal keeps the table: the notes give way first.
    let room = (area.height as usize).saturating_sub(5).max(1);
    let notes = notes.into_iter().take(room).collect::<Vec<_>>();
    let [title, note, table_area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(notes.len() as u16 + 1),
            Constraint::Fill(1),
        ])
        .areas(area);
    Paragraph::new(rule_line(
        &format!("{} over time", config.metric.label()),
        Some(&numfmt::group_chrome(
            view.lines
                .iter()
                .filter(|row| !row.rows())
                .map(|row| row.names.len())
                .sum::<usize>(),
        )),
        title.width,
        theme,
    ))
    .render(title, buf);
    Paragraph::new(
        notes
            .iter()
            .map(|text| Line::styled(fit(text, note.width as usize), dimmed))
            .collect::<Vec<_>>(),
    )
    .render(note, buf);
    let g = glyphs::get();
    let accent = Style::default().fg(theme.get("accent"));
    let lines = view.lines.iter().map(|row| {
        let spark = view
            .bars
            .iter()
            .enumerate()
            .map(|(index, bar)| {
                let mark = bar_mark(row, bar, index, g);
                Span::styled(mark, if mark == g.unsampled { dimmed } else { accent })
            })
            .collect::<Vec<_>>();
        let name = crate::quality_report::columns_label(&row.names, name_width as usize - 2);
        Row::new(vec![
            Cell::from(if row.rows() {
                Span::styled(name, dimmed)
            } else {
                Span::raw(name)
            }),
            Cell::from(Span::styled(trend_range(row), dimmed)),
            Cell::from(Line::from(spark)),
        ])
    });
    normalize_selection(table_state, view.lines.len());
    let table = Table::new(
        lines,
        [
            Constraint::Length(name_width),
            Constraint::Length(TREND_RANGE),
            Constraint::Fill(1),
        ],
    )
    .header(Row::new(["Column", "Range", "Trend"]).style(dimmed))
    .row_highlight_style(theme.highlight_style())
    .highlight_symbol(g.selector);
    StatefulWidget::render(table, table_area, buf, table_state);
}

/// One bar of one Trends line, the line drawn above it with a pointer under the
/// bar: what it spans, how many segments it pools and how much of them the run
/// read, its value with what it is out of, how sure a sample is of it, and how it
/// stands against the bar it is compared with. From the report's measurements:
/// nothing here reads. ↑↓ walk the bars.
fn render_trend_detail(
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
    let g = glyphs::get();
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let accent = Style::default().fg(theme.get("accent"));
    let [area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    let (name_width, bars) = trend_layout(results, area.width);
    let view = trend_view(results, config.metric, bars);
    let (Some(line), false) = (view.lines.get(config.trend_line), view.bars.is_empty()) else {
        Paragraph::new("No bars to show: Esc returns to Trends.")
            .style(Style::default().fg(theme.get("text_primary")))
            .render(area, buf);
        return;
    };
    // Held to the last bar: the keys only know an upper bound, the width decides.
    let index = table_state.selected().unwrap_or(0).min(view.bars.len() - 1);
    table_state.select(Some(index));
    let [title, spark, pointer, _, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Length(1),
            Constraint::Fill(1),
        ])
        .areas(area);
    let measure = match line.measure {
        TrendMeasure::Rows => "Rows per segment".to_string(),
        TrendMeasure::SampledRows => "Sampled rows per segment".to_string(),
        TrendMeasure::Column(_) => config.metric.label().to_string(),
    };
    Paragraph::new(rule_line(
        &measure,
        Some(&format!(
            "bar {} of {}",
            numfmt::group_chrome(index + 1),
            numfmt::group_chrome(view.bars.len())
        )),
        title.width,
        theme,
    ))
    .render(title, buf);

    // The line as Trends drew it, its selected bar pointed at from below.
    let name = crate::quality_report::columns_label(&line.names, name_width as usize - 2);
    let lead = |text: String, width: u16| {
        format!(
            "{text}{}",
            " ".repeat((width as usize).saturating_sub(glyphs::display_width(&text)))
        )
    };
    let mut spans = vec![
        Span::raw(g.selector_blank),
        Span::raw(lead(name, name_width + 1)),
        Span::styled(lead(trend_range(line), TREND_RANGE + 1), dimmed),
    ];
    spans.extend(view.bars.iter().enumerate().map(|(at, candidate)| {
        let mark = bar_mark(line, candidate, at, g);
        let style = if mark == g.unsampled { dimmed } else { accent };
        Span::styled(
            mark,
            if at == index {
                style.add_modifier(Modifier::BOLD)
            } else {
                style
            },
        )
    }));
    Paragraph::new(Line::from(spans)).render(spark, buf);
    Paragraph::new(Line::from(vec![
        Span::raw(" ".repeat(trend_lead(name_width) as usize + index)),
        Span::styled(g.pointer, accent),
    ]))
    .render(pointer, buf);

    let rows = trend_bar_fields(config, results, &view, line, index);
    let label_width = rows
        .iter()
        .map(|row| glyphs::display_width(&row.label))
        .max()
        .unwrap_or(0)
        + 2;
    let width = (body.width as usize).min(DETAIL_MEASURE);
    render_counted(
        field_lines(&rows, label_width, width, false),
        body,
        theme,
        buf,
    );
}

/// A bar's facts as label and value rows.
fn trend_bar_fields(
    config: &DataQualityWidgetConfig<'_>,
    results: &DataQualityResults,
    view: &TrendView<'_>,
    line: &TrendRow,
    index: usize,
) -> Vec<FieldRow> {
    let plan = config.plan;
    let bar = &view.bars[index];
    let unit = trend_unit(&plan.grain);
    let row = |label: &str, value: String| FieldRow {
        mark: None,
        label: label.to_string(),
        value,
    };
    let mut rows = vec![row(
        "Span",
        segment_text(&crate::quality_trends::bar_span(view, bar, &plan.grain)),
    )];
    // How much of the bar the run reached, before any number taken from it.
    let mut reach = vec![counted_unit(bar.segments(), unit)];
    if view.sampled {
        if bar.unsampled > 0 {
            reach.push(format!(
                "{} not sampled",
                numfmt::group_chrome(bar.unsampled)
            ));
        }
        if bar.thin > 0 {
            reach.push(format!(
                "{} under {} sampled rows",
                numfmt::group_chrome(bar.thin),
                crate::quality_report::THIN_SEGMENT_ROWS
            ));
        }
        if bar.unsampled + bar.thin == 0 {
            reach.push("each sampled".to_string());
        }
    }
    rows.push(row("Segments", reach.join(", ")));
    let every_row = !view.sampled || bar.eligible == Some(bar.evaluated);
    rows.push(row(
        "Rows",
        match (view.sampled, bar.eligible) {
            (false, Some(eligible)) => {
                format!("{}, every row read", numfmt::group_chrome(eligible))
            }
            (false, None) => format!("{}, every row read", numfmt::group_chrome(bar.evaluated)),
            (true, Some(eligible)) => format!(
                "{} sampled of {} ({})",
                numfmt::group_chrome(bar.evaluated),
                numfmt::group_chrome(eligible),
                share_rate(bar.evaluated, eligible)
            ),
            (true, None) => format!(
                "{} sampled, total not counted",
                numfmt::group_chrome(bar.evaluated)
            ),
        },
    ));
    let compared = crate::quality_trends::compared_bar(view, index, plan);
    let against = if plan.comparison == QualityComparison::Baseline {
        "Baseline bar"
    } else {
        "Previous bar"
    };
    if line.rows() {
        // Rows per segment: the mean, and the smallest and largest segment.
        let counts = view.slots[bar.slots.clone()]
            .iter()
            .map(|slot| match line.measure {
                TrendMeasure::Rows => slot.total.unwrap_or(0),
                _ => slot.evaluated,
            })
            .collect::<Vec<_>>();
        let (low, high) = (
            counts.iter().min().copied().unwrap_or(0),
            counts.iter().max().copied().unwrap_or(0),
        );
        let mean = line.bars[index].unwrap_or(0.0);
        rows.push(row(
            "Per segment",
            if low == high {
                numfmt::group_chrome(low)
            } else {
                format!(
                    "{} on average, {} to {}",
                    numfmt::group_chrome(mean.round() as usize),
                    numfmt::group_chrome(low),
                    numfmt::group_chrome(high)
                )
            },
        ));
        if let Some(other) = compared {
            let before = line.bars[other].unwrap_or(0.0);
            let moved = if before > 0.0 {
                format!(" ({:+.1}%)", (mean - before) / before * 100.0)
            } else {
                String::new()
            };
            rows.push(row(
                against,
                format!(
                    "{} to {} per segment{moved}",
                    numfmt::group_chrome(before.round() as usize),
                    numfmt::group_chrome(mean.round() as usize)
                ),
            ));
        }
        return rows;
    }
    let (count, of) = line.parts[index];
    let noun = match config.metric {
        QualityMetric::DistinctShare
        | QualityMetric::IntegerParseShare
        | QualityMetric::DecimalParseShare => "values",
        _ => "rows",
    };
    rows.push(row(
        config.metric.label(),
        if of > 0.0 {
            format!(
                "{} of {} {noun} ({})",
                numfmt::group_chrome(count.round() as usize),
                numfmt::group_chrome(of.round() as usize),
                rate_label(count / of)
            )
        } else if bar.evaluated == 0 {
            "none: no row sampled".to_string()
        } else {
            format!("none: no {noun} to measure")
        },
    ));
    // How sure a sample is of the rate: never a point, and never for a distinct
    // share, which does not stand for the whole.
    let interval = if of <= 0.0 {
        None
    } else if every_row {
        Some("none needed: every row read".to_string())
    } else if config.metric == QualityMetric::DistinctShare {
        Some("none: a distinct share does not stand for the whole".to_string())
    } else {
        crate::quality_trends::wilson_interval(count, of).map(|(low, high)| {
            let mut text = format!("{} to {}", rate_label(low), rate_label(high));
            if (of as usize) < crate::quality_report::THIN_SEGMENT_ROWS {
                text.push_str(&format!(
                    ", from under {} {noun}",
                    crate::quality_report::THIN_SEGMENT_ROWS
                ));
            }
            text
        })
    };
    if let Some(interval) = interval {
        rows.push(row("95% interval", interval));
    }
    if let Some(other) = compared {
        let other_bar = &view.bars[other];
        let exact = results.precision == QualityPrecision::Exact
            || (every_row && other_bar.eligible == Some(other_bar.evaluated));
        rows.push(row(
            against,
            match crate::quality_trends::bar_change(line, index, other, exact) {
                Some(change) => format!(
                    "{} to {}, {:+.1} points: {}",
                    rate_label(change.before),
                    rate_label(change.now),
                    change.points(),
                    // Segments never judge a distinct share: it falls as a segment
                    // grows, so bars of different sizes differ by it whatever the data.
                    if config.metric == QualityMetric::DistinctShare {
                        "not judged for a distinct share"
                    } else if change.clear {
                        "a clear change"
                    } else if change.points().abs() < crate::data_quality::MATERIAL_CHANGE_PP {
                        "under a point"
                    } else {
                        "within sampling noise"
                    }
                ),
                None => "nothing to compare: one side has no rate".to_string(),
            },
        ));
    }
    rows
}

/// The expected windows with no rows, a line per run of them: why, which windows,
/// and the rows a sample missed. Bounded: the runs past the cap are counted.
fn render_gaps(
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
    let plan = config.plan;
    let dimmed = Style::default().fg(theme.get("dimmed"));
    let [area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    let Some(gaps) = crate::quality_trends::expected_gaps(plan, results) else {
        Paragraph::new(
            "No expected windows: set Expected in Setup (e) to say which windows rows \
             belong in.",
        )
        .wrap(Wrap { trim: true })
        .style(Style::default().fg(theme.get("text_primary")))
        .render(area, buf);
        return;
    };
    let check = match &gaps {
        Gaps::Checked(check) if check.expected > 0 => check,
        _ => {
            Paragraph::new(gaps_summary(plan, &gaps))
                .wrap(Wrap { trim: true })
                .style(Style::default().fg(theme.get("text_primary")))
                .render(area, buf);
            return;
        }
    };
    let unit = trend_unit(&plan.grain);
    let every = check.every.as_str();
    let sampled = matches!(
        results.precision,
        QualityPrecision::Sampled | QualityPrecision::Estimated
    );
    let mut notes = vec![format!(
        "{}, {}: {} of {} {}",
        check.column,
        crate::quality_trends::calendar_span(
            check.from,
            crate::quality_trends::floor_window(
                check.before - chrono::Duration::microseconds(1),
                every
            ),
            every
        ),
        numfmt::group_chrome(check.with_rows),
        counted_unit(check.expected, unit),
        if sampled { "sampled" } else { "with rows" }
    )];
    if check.weekend > 0 {
        notes.push(format!(
            "{} on weekends, not expected",
            counted_unit(check.weekend, unit)
        ));
    }
    // What each kind means, for the kinds listed: empty only on an exact count.
    let scope = plan.scope.label();
    for (count, text) in [
        (
            check.empty,
            format!("Empty: no rows in {scope}, by exact count"),
        ),
        (
            check.unsampled,
            match missed_rows(check) {
                Some(rows) if check.counted => format!(
                    "Not sampled: {} rows there, none drawn",
                    numfmt::group_chrome(rows)
                ),
                None if check.counted => "Not sampled: rows there, none drawn".to_string(),
                _ => "Not sampled: none drawn, rows not counted".to_string(),
            },
        ),
        (
            check.out_of_scope,
            "Out of scope: outside the time range the scope reads".to_string(),
        ),
    ] {
        if count > 0 {
            notes.push(text);
        }
    }
    let notes = notes
        .iter()
        .flat_map(|note| crate::widgets::info::wrap_to(note, area.width as usize))
        .take((area.height as usize).saturating_sub(5).max(1))
        .collect::<Vec<_>>();
    let [title, note, body] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1),
            Constraint::Length(notes.len() as u16 + 1),
            Constraint::Fill(1),
        ])
        .areas(area);
    Paragraph::new(rule_line(
        &format!(
            "Expected {}",
            plan.expected
                .as_ref()
                .map(|expected| expected.cadence_label(every))
                .unwrap_or_default()
        ),
        Some(&format!(
            "{} {}",
            numfmt::group_chrome(check.gaps()),
            if check.gaps() == 1 { "gap" } else { "gaps" }
        )),
        title.width,
        theme,
    ))
    .render(title, buf);
    Paragraph::new(
        notes
            .iter()
            .map(|text| Line::styled(fit(text, note.width as usize), dimmed))
            .collect::<Vec<_>>(),
    )
    .render(note, buf);
    if check.runs.is_empty() {
        Paragraph::new("Every expected window has rows.")
            .style(Style::default().fg(theme.get("text_primary")))
            .render(body, buf);
        return;
    }
    // The windows first, which is what a gap is; the rows a sample missed where
    // there is room for them.
    let span_width = if every == "1h" { 35 } else { 24 };
    let wide = body.width as usize >= span_width + 2 + 12 + 10 + 14 + 4;
    let rows = check
        .runs
        .iter()
        .map(|run| {
            let mut cells = vec![
                Cell::from(crate::quality_trends::calendar_span(
                    run.first, run.last, every,
                )),
                Cell::from(run.kind.label()),
                Cell::from(Span::styled(counted_unit(run.windows, unit), dimmed)),
            ];
            if wide {
                cells.push(Cell::from(Span::styled(
                    run.rows
                        .map(|rows| format!("{} rows", numfmt::group_chrome(rows)))
                        .unwrap_or_default(),
                    dimmed,
                )));
            }
            Row::new(cells)
        })
        .chain((check.more_runs > 0).then(|| {
            Row::new(vec![Cell::from(Span::styled(
                format!(
                    "{} {} more",
                    glyphs::get().ellipsis,
                    numfmt::group_chrome(check.more_runs)
                ),
                dimmed,
            ))])
        }))
        .collect::<Vec<_>>();
    normalize_selection(table_state, check.runs.len());
    let mut widths = vec![
        Constraint::Length(span_width as u16),
        Constraint::Length(12),
        Constraint::Length(10),
    ];
    let mut headers = vec!["Windows", "Gap", "Length"];
    if wide {
        widths.push(Constraint::Length(14));
        headers.push("Rows");
    }
    let table = Table::new(rows, widths)
        .header(Row::new(headers).style(dimmed))
        .row_highlight_style(theme.highlight_style())
        .highlight_symbol(glyphs::get().selector);
    StatefulWidget::render(table, body, buf, table_state);
}

/// The rows in the windows a sample missed, when every one was counted.
fn missed_rows(check: &GapCheck) -> Option<usize> {
    check
        .runs
        .iter()
        .filter(|run| run.kind == GapKind::Unsampled)
        .map(|run| run.rows)
        .sum::<Option<usize>>()
        .filter(|_| check.more_runs == 0)
}

/// Setup's Expected editor: which windows rows are expected in, and from and before
/// when. Nothing here reads; Enter writes it into the draft.
fn render_expected_windows(config: &DataQualityWidgetConfig<'_>, area: Rect, buf: &mut Buffer) {
    let ctx = config.ctx;
    let theme = config.theme;
    let Some(form) = config.expected_form else {
        return;
    };
    let [area] = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Fill(1)])
        .margin(1)
        .areas(area);
    let every = match &config.plan.grain {
        QualityGrain::TimeWindows { every, .. } => every.as_str(),
        _ => "1d",
    };
    let notes = [
        format!("Grain: {}", config.plan.grain.label()),
        "From, Before: a date or a UTC timestamp, such as 2024-01-01 or \
         2024-01-01T09:00:00Z. Blank: the first or last window found."
            .to_string(),
        "A window in range with no rows is listed under Trends (g); a weekend only \
         when every day is expected."
            .to_string(),
    ];
    let width = (area.width as usize).min(DETAIL_MEASURE);
    let mut y = area.y;
    put_line(
        rule_line("Expected Windows", None, area.width, theme),
        area,
        &mut y,
        buf,
    );
    put_line(Line::raw(""), area, &mut y, buf);
    let cadence = form.cadence_label(every);
    for (field, label) in crate::analysis_modal::EXPECTED_ROWS.iter().enumerate() {
        let row_area = Rect {
            y,
            height: 1,
            ..area
        };
        if y < area.y + area.height {
            crate::widgets::ui::FormRow {
                label,
                value: match field {
                    0 => FormValue::Choice(&cadence),
                    1 => FormValue::Input(&form.from),
                    _ => FormValue::Input(&form.before),
                },
                focused: form.field == field,
                label_width: 10,
            }
            .render(row_area, buf, ctx);
        }
        y += 1;
    }
    put_line(Line::raw(""), area, &mut y, buf);
    let dimmed = Style::default().fg(theme.get("dimmed"));
    for note in &notes {
        for text in crate::widgets::info::wrap_to(note, width.saturating_sub(2)) {
            put_line(Line::styled(format!("  {text}"), dimmed), area, &mut y, buf);
        }
    }
    if let Some(error) = &form.error {
        put_line(Line::raw(""), area, &mut y, buf);
        put_line(
            Line::styled(fit(error, width), Style::default().fg(theme.get("warning"))),
            area,
            &mut y,
            buf,
        );
    }
}

/// `line` on row `y` of `area` when it is inside it, and the next row after.
fn put_line(line: Line<'static>, area: Rect, y: &mut u16, buf: &mut Buffer) {
    if *y < area.y + area.height {
        Paragraph::new(line).render(
            Rect {
                y: *y,
                height: 1,
                ..area
            },
            buf,
        );
    }
    *y += 1;
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
        row("Column intent", {
            let every_row =
                planned_scope_rows(state, plan).is_some_and(|rows| rows <= plan.dataset_rows);
            let lines = intent_read_lines(plan, every_row);
            if lines.is_empty() {
                "none declared".to_string()
            } else {
                // The row names it; the lines say what it costs.
                lines
                    .iter()
                    .map(|line| line.strip_prefix("Column intent: ").unwrap_or(line))
                    .collect::<Vec<_>>()
                    .join("; ")
            }
        }),
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
pub(crate) fn planned_read_label(state: &DataTableState, plan: &DataQualityPlan) -> String {
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

/// What reading every row of `plan`'s scope again costs, as far as it is known: a
/// ceiling from the rows and their width, and unknown for a remote or source scope.
pub(crate) fn scope_read_label(state: &DataTableState, plan: &DataQualityPlan) -> String {
    if state.is_remote_source() {
        return "unknown".to_string();
    }
    match planned_scope_rows(state, plan) {
        Some(rows) => format!(
            "up to {}",
            approximate_bytes(rows.saturating_mul(state.estimated_row_bytes()))
                .trim_start_matches("about ")
        ),
        None => "unknown".to_string(),
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

pub(crate) fn compute_label(plan: &DataQualityPlan) -> String {
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

pub(crate) fn centered_rect(width: u16, height: u16, area: Rect) -> Rect {
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
        findings: FindingsView,
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
                findings: FindingsView::default(),
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
                interval_index: 0,
                trend_line: 0,
                expected_form: None,
                segments_by_change: false,
                page,
                setup: SetupView::default(),
                plan_field: 0,
                show_access: false,
                observation_detail: false,
                confirm_run: false,
                findings: &self.findings,
                rows_kept: false,
                evidence_read: None,
                focus: AnalysisFocus::Main,
                theme: &self.theme,
                ctx: &self.ctx,
                intent_form: None,
                export_form: None,
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

    /// Coverage sits under the verdict whether the report found problems or none, at
    /// the baseline size and the smallest, with the findings still on screen below
    /// it and every character outside ASCII a glyph slot.
    #[test]
    fn coverage_accompanies_every_verdict_at_80x24_and_60x20() {
        let screen = Screen::new();
        let g = glyphs::get();
        let slots = [
            g.rail, g.rule_h, g.middot, g.ellipsis, g.warning, g.check, g.dash,
        ]
        .concat();
        // The same rows sampled, and found clean: a report with nothing to fix.
        let mut clean = screen.results.clone();
        clean.observations.clear();
        clean.precision = QualityPrecision::Sampled;
        clean.total_rows = Some(80);
        clean.reads = Some(crate::data_quality::ObservedReads {
            reads: 1,
            counted: 1,
            rows: 80,
        });
        for (results, verdict) in [(&screen.results, "problem"), (&clean, "No problems found")] {
            for (width, height) in [(80, 24), (60, 20)] {
                let mut config = screen.config(QualityPage::Overview);
                config.results = Some(results);
                let rows = screen.draw(config, 0, width, height);
                let text = rows.join("\n");
                let at = rows
                    .iter()
                    .position(|row| row.contains(verdict))
                    .unwrap_or_else(|| panic!("verdict at {width}x{height}:\n{text}"));
                assert!(
                    rows[at + 1].trim_start().starts_with("Checks"),
                    "coverage under the verdict at {width}x{height}:\n{text}"
                );
                assert!(
                    text.contains("Problems") || text.contains("Clean"),
                    "findings still on screen at {width}x{height}:\n{text}"
                );
                if results.precision == QualityPrecision::Exact {
                    assert!(
                        rows[at + 1].contains("exact") && text.contains("all 8 read, exact"),
                        "{text}"
                    );
                } else {
                    // Clean, and not everything could be looked at: it says so.
                    assert!(rows[at + 1].contains("1 unavailable"), "{text}");
                    assert!(text.contains("8 of 80 sampled (10.0%)"), "{text}");
                    assert!(
                        text.contains("Nearly unique: needs every row checked"),
                        "{text}"
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

    /// A finding says where its rows come from before Enter: the rows the run kept,
    /// a sample no longer kept, or a full scan that kept none. A staged read lists
    /// what it reads, at the baseline size and the smallest, in glyph slots.
    #[test]
    fn evidence_says_where_its_rows_come_from() {
        let screen = Screen::new();
        let mut sampled = screen.results.clone();
        sampled.precision = QualityPrecision::Sampled;
        let report = build_report(&sampled);
        let position = FindingsView::default()
            .shown(&report)
            .iter()
            .position(|index| report.findings[*index].title == "Missing values")
            .unwrap();
        let detail = |results: &DataQualityResults, kept: bool, width, height| {
            let mut config = screen.config(QualityPage::Overview);
            config.results = Some(results);
            config.observation_detail = true;
            config.rows_kept = kept;
            screen.draw(config, position, width, height).join("\n")
        };
        assert!(detail(&sampled, true, 80, 24).contains("Enter shows the 1 sampled row."));
        assert!(detail(&sampled, false, 80, 24).contains("no longer kept"));
        assert!(detail(&screen.results, false, 80, 24).contains("A full scan keeps no rows"));
        // A sample that held every row is exact, but it was no full scan.
        let whole_sample = DataQualityPlan {
            compute: QualityCompute::Sample,
            ..screen.plan.clone()
        };
        let mut config = screen.config(QualityPage::Overview);
        config.measured = &whole_sample;
        config.observation_detail = true;
        let text = screen.draw(config, position, 80, 24).join("\n");
        assert!(text.contains("The rows read are no longer kept"), "{text}");

        let read = EvidenceRead {
            rows: EvidenceRows::Matching(polars::prelude::lit(true)),
            label: String::new(),
            sample: None,
            scope: QualityScope::CurrentView,
            summary: vec![
                ("Rows", "Missing values · region".to_string()),
                ("Why", "a full scan keeps no rows".to_string()),
                (
                    "Reads",
                    "current view, as far as the table scrolls".to_string(),
                ),
                ("Shows", "1 row".to_string()),
                ("Source", "local, read only".to_string()),
            ],
        };
        let g = glyphs::get();
        let slots = [g.rail, g.rule_h, g.middot, g.ellipsis, g.warning, g.check].concat();
        for (width, height) in [(80, 24), (60, 20)] {
            let mut config = screen.config(QualityPage::Overview);
            config.observation_detail = true;
            config.evidence_read = Some(&read);
            let text = screen.draw(config, position, width, height).join("\n");
            for label in ["Read Rows", "Why", "Reads", "Shows", "read only"] {
                assert!(text.contains(label), "{label} at {width}x{height}:\n{text}");
            }
            for c in text.chars().filter(|c| !c.is_ascii()) {
                assert!(
                    slots.contains(c) || "╭╮╰╯│─·".contains(c),
                    "{c:?} at {width}x{height}:\n{text}"
                );
            }
        }
    }

    /// A line with room for one fact keeps it, cut short, beside the count of the
    /// rest, rather than saying only how much it left out.
    #[test]
    fn a_lone_fact_is_cut_rather_than_dropped() {
        let facts = [
            "Nearly unique: needs every row checked".to_string(),
            "key repeats among 10,000 sampled rows only".to_string(),
        ];
        let lines = pack_facts(&facts, 37, 1);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].starts_with("Nearly unique"), "{lines:?}");
        assert!(lines[0].ends_with("+1 more"), "{lines:?}");
        assert!(glyphs::display_width(&lines[0]) <= 37, "{lines:?}");
    }

    /// Facts wrap between facts, never inside one, and what does not fit is counted.
    #[test]
    fn coverage_facts_wrap_whole_and_count_the_rest() {
        let facts = ["one fact", "another fact", "a third fact", "a fourth"]
            .map(String::from)
            .to_vec();
        let dot = glyphs::get().middot;
        assert_eq!(
            pack_facts(&facts, 24, 2),
            [
                format!("one fact {dot} another fact"),
                format!("a third fact {dot} a fourth")
            ]
        );
        assert_eq!(
            pack_facts(&facts, 24, 1),
            [format!("one fact {dot} +3 more")]
        );
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
        config.setup.cancelling = Some(Cancelling {
            since: std::time::Instant::now(),
            read_runs_out: true,
        });
        config.setup.note = Some("Run waits: the cancelled run is still stopping");
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

        // A run that should have stopped at its next batch and has not says so,
        // without claiming a read it cannot stop.
        let mut config = screen.config(QualityPage::Setup);
        config.setup.cancelling = Some(Cancelling {
            since: std::time::Instant::now(),
            read_runs_out: false,
        });
        let rows = screen.draw(config, 0, 100, 30);
        assert!(
            rows[0].contains("Cancellation requested; run stopping"),
            "{}",
            rows.join("\n")
        );
        assert!(!rows.join("\n").contains("source read finishing"));
    }
}

/// Intervals on screen: the report's list and one interval's detail, and what
/// Setup says about roles and zones before a run.
#[cfg(test)]
mod interval_tests {
    use super::*;
    use crate::data_quality::{TemporalRoleAssignment, TimeInterpretation, TimeKind};
    use polars::prelude::*;

    const HOUR: i64 = 3_600_000_000;

    /// Two days of sends and receipts, with a receipt missing, one early and one
    /// over an hour; and validity periods, one open and one that ends first.
    fn frame() -> LazyFrame {
        let day = 1_704_067_200_000_000i64; // 2024-01-01
        let sent = (0..8)
            .map(|row| Some(day + (row / 4) * 24 * HOUR + row * HOUR))
            .collect::<Vec<_>>();
        let seen = sent
            .iter()
            .enumerate()
            .map(|(row, at)| match row {
                1 => None,
                2 => at.map(|at| at - 60_000_000),
                5 => at.map(|at| at + 2 * HOUR),
                _ => at.map(|at| at + 600_000_000),
            })
            .collect::<Vec<_>>();
        let datetimes = |name: &str, values: Vec<Option<i64>>| -> Column {
            Series::new(name.into(), values)
                .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
                .unwrap()
                .into()
        };
        let days = |name: &str, values: [Option<i32>; 8]| -> Column {
            Series::new(name.into(), values)
                .cast(&DataType::Date)
                .unwrap()
                .into()
        };
        DataFrame::new(
            8,
            vec![
                datetimes("sent", sent),
                datetimes("seen", seen),
                Column::new("stamp".into(), vec!["2024-01-01T00:10:00Z"; 8]),
                days("from", [Some(19_723); 8]),
                days(
                    "to",
                    [
                        Some(19_730),
                        None,
                        Some(19_720),
                        Some(19_730),
                        Some(19_730),
                        Some(19_730),
                        Some(19_730),
                        Some(19_723),
                    ],
                ),
            ],
        )
        .unwrap()
        .lazy()
    }

    fn role(role: TemporalRole, column: &str) -> TemporalRoleAssignment {
        TemporalRoleAssignment {
            role,
            column: column.to_string(),
            timezone: None,
        }
    }

    struct Screen {
        state: DataTableState,
        plan: DataQualityPlan,
        /// What Setup offers roles: the frame's date and time columns, and text.
        candidates: Vec<String>,
        results: DataQualityResults,
        theme: Theme,
        ctx: RenderContext,
    }

    impl Screen {
        fn new(plan: DataQualityPlan) -> Self {
            let lf = frame();
            let schema = Arc::new((*lf.clone().collect_schema().unwrap()).clone());
            let state = DataTableState::from_schema_and_lazyframe(
                schema,
                lf.clone(),
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap();
            let results =
                crate::data_quality::compute_data_quality(&lf, Some(8), &plan, None, false)
                    .unwrap();
            Self {
                state,
                plan,
                candidates: ["sent", "seen", "from", "to", "stamp"]
                    .map(String::from)
                    .to_vec(),
                results,
                theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
                ctx: RenderContext::for_test(),
            }
        }

        fn studied() -> Self {
            Self::new(DataQualityPlan {
                compute: QualityCompute::Full,
                temporal_roles: vec![
                    role(TemporalRole::Event, "sent"),
                    role(TemporalRole::Received, "seen"),
                    role(TemporalRole::ValidFrom, "from"),
                    role(TemporalRole::ValidTo, "to"),
                ],
                latency_threshold_seconds: Some(3_600),
                grain: QualityGrain::TimeWindows {
                    column: "sent".to_string(),
                    every: "1d".to_string(),
                },
                ..DataQualityPlan::default()
            })
        }

        fn draw(
            &self,
            page: QualityPage,
            interval: usize,
            selected: usize,
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
                interval_index: interval,
                trend_line: 0,
                expected_form: None,
                segments_by_change: false,
                page,
                setup: SetupView {
                    time_candidates: &self.candidates,
                    ..SetupView::default()
                },
                plan_field: selected,
                show_access: false,
                observation_detail: false,
                confirm_run: false,
                findings: &FindingsView {
                    column: None,
                    check: None,
                    order: FindingOrder::Ranked,
                },
                rows_kept: false,
                evidence_read: None,
                focus: AnalysisFocus::Main,
                theme: &self.theme,
                ctx: &self.ctx,
                intent_form: None,
                export_form: None,
            };
            let (width, height) = size;
            let area = Rect::new(0, 0, width, height);
            let mut buf = Buffer::empty(area);
            let mut table = TableState::default();
            table.select(Some(selected));
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
    /// ASCII twin.
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
        ]
        .concat();
        for c in text.chars().filter(|c| !c.is_ascii()) {
            assert!(
                slots.contains(c) || "╭╮╰╯│─".contains(c),
                "{c:?} is not a glyph slot:\n{text}"
            );
        }
    }

    /// The list says what its last column counts and out of what; each row names
    /// its interval and segment where there is room, and the selected row has the
    /// selector at every size.
    #[test]
    fn the_interval_list_fits_80x24_and_60x20() {
        let screen = Screen::studied();
        let intervals = &screen.results.temporal;
        assert_eq!(intervals.len(), 4, "two intervals over two days");
        for size in [(120, 32), (80, 24), (60, 20)] {
            for (selected, interval) in intervals.iter().enumerate() {
                let text = screen.draw(QualityPage::Intervals, 0, selected, size);
                assert!(text.contains("Time between dates"), "{text}");
                assert!(
                    text.contains("Over: duration > 1 hour, of rows with both ends"),
                    "{size:?}: {text}"
                );
                let row = text
                    .lines()
                    .find(|line| line.contains(glyphs::get().selector))
                    .unwrap_or_else(|| panic!("a selected row at {size:?}:\n{text}"));
                // In full, or cut with an ellipsis where the width runs out.
                let label = interval.label();
                assert!(
                    row.contains(&label)
                        || row.contains(&label[..12]) && row.contains(glyphs::get().ellipsis),
                    "{size:?}: {row}"
                );
                if size.0 >= 80 {
                    assert!(row.contains(&interval.segment), "{row}");
                }
                assert_glyph_slots(&text);
            }
        }
        // Wide enough, the list adds the denominator and each end's missing count.
        let wide = screen.draw(QualityPage::Intervals, 0, 0, (160, 32));
        assert!(
            wide.contains("Both ends") && wide.contains("Missing s/e"),
            "{wide}"
        );
    }

    /// An interval's detail at 80x24 holds every count at once; at 60x20 it scrolls
    /// to the count under the cursor. Missing ends, unread text and negative
    /// durations are rows of their own, and each count says what it is out of.
    #[test]
    fn an_interval_detail_keeps_every_count_at_80x24_and_60x20() {
        let screen = Screen::studied();
        let first = &screen.results.temporal[0];
        assert_eq!(first.label(), "event to received");
        assert_eq!(
            (first.evaluated_rows, first.paired_rows, first.missing_end),
            (4, 3, 1)
        );
        let labels = [
            "Start",
            "End",
            "Segment",
            "Rows",
            "Both ends",
            "Missing start",
            "Missing end",
            "Negative",
            "Zero",
            "p50, p90",
            "p95, p99",
            "Maximum",
            "Threshold",
            "Over 1 hour",
        ];
        let text = screen.draw(QualityPage::IntervalDetail, 0, 0, (80, 24));
        for label in labels {
            assert!(text.contains(label), "{label}:\n{text}");
        }
        assert!(
            text.contains("3 of 4 (75.0%)"),
            "both ends out of rows:\n{text}"
        );
        assert!(
            text.contains("1 of 3 (33.3%) with both ends"),
            "negative out of both ends:\n{text}"
        );
        assert!(text.contains("duration > 1 hour, strictly"), "{text}");
        assert!(!text.contains("more"), "nothing cut at 80x24:\n{text}");
        let facts = IntervalFact::ALL
            .into_iter()
            .filter(|fact| first.count(*fact, &screen.plan).is_some())
            .collect::<Vec<_>>();
        // 60x17 is what a 60x20 terminal leaves the page under its bars.
        for size in [(80, 24), (60, 20), (60, 17)] {
            for (selected, fact) in facts.iter().enumerate() {
                let text = screen.draw(QualityPage::IntervalDetail, 0, selected, size);
                let rail = format!("{} {}", glyphs::get().rail, fact.label(first));
                assert!(
                    text.contains(&rail),
                    "{fact:?} under the cursor at {size:?}:\n{text}"
                );
                assert_glyph_slots(&text);
                // Scrolled, the page ends on its last line or the count of what
                // is below, never on blank lines.
                if !text.contains("Start") {
                    let lines = text.lines().collect::<Vec<_>>();
                    let last = lines.iter().rposition(|line| !line.trim().is_empty());
                    assert_eq!(last, Some(lines.len() - 2), "{fact:?}:\n{text}");
                }
            }
        }
    }

    /// Where Enter cannot help, the page says why: a report of file metadata reads
    /// no times, and a row chunk's rows are not a value a count can open.
    #[test]
    fn intervals_say_why_nothing_opens() {
        let roles = vec![
            role(TemporalRole::Event, "sent"),
            role(TemporalRole::Received, "seen"),
        ];
        let screen = Screen::new(DataQualityPlan {
            compute: QualityCompute::Metadata,
            temporal_roles: roles.clone(),
            ..DataQualityPlan::default()
        });
        assert!(screen.results.temporal.is_empty());
        let text = screen.draw(QualityPage::Intervals, 0, 0, (80, 24));
        assert!(
            text.contains("File metadata only reads no values"),
            "{text}"
        );

        let screen = Screen::new(DataQualityPlan {
            compute: QualityCompute::Full,
            temporal_roles: roles,
            grain: QualityGrain::RowChunks(4),
            ..DataQualityPlan::default()
        });
        for size in [(80, 24), (60, 20)] {
            let text = screen.draw(QualityPage::IntervalDetail, 0, 0, size);
            assert!(text.contains("Rows do not open: a row chunk"), "{text}");
        }
    }

    /// Windowing intervals by their ends groups once per end column, and on a full
    /// scan Setup says how many of its passes that is before Run.
    #[test]
    fn setup_counts_the_passes_a_window_clock_takes() {
        let mut plan = DataQualityPlan {
            compute: QualityCompute::Full,
            temporal_roles: vec![
                role(TemporalRole::Event, "sent"),
                role(TemporalRole::Received, "seen"),
                role(TemporalRole::Processed, "to"),
            ],
            grain: QualityGrain::TimeWindows {
                column: "sent".to_string(),
                every: "1d".to_string(),
            },
            interval_clock: IntervalClock::End,
            ..DataQualityPlan::default()
        };
        let text = Screen::new(plan.clone()).draw(QualityPage::Setup, 0, 0, (120, 40));
        assert!(
            text.contains("Window by each interval's end: 2 of those passes for intervals"),
            "{text}"
        );
        plan.interval_clock = IntervalClock::Grain;
        let text = Screen::new(plan).draw(QualityPage::Setup, 0, 0, (120, 40));
        assert!(!text.contains("of those passes for intervals"), "{text}");
    }

    /// Valid from to valid to is a validity period: no end is open, an end before
    /// the start ends first.
    #[test]
    fn a_validity_period_reads_as_one() {
        let screen = Screen::studied();
        let index = screen
            .results
            .temporal
            .iter()
            .position(|interval| interval.is_validity())
            .unwrap();
        let text = screen.draw(QualityPage::IntervalDetail, index, 0, (80, 24));
        assert!(text.contains("Open, no end"), "{text}");
        assert!(text.contains("Ends first"), "{text}");
        assert!(!text.contains("Negative"), "{text}");
    }

    /// Before a run, Setup names roles no interval uses and says how a time with
    /// no zone meets one with an offset; the pairs editor lists every start and end.
    #[test]
    fn setup_names_unpaired_roles_and_how_zones_compare() {
        let screen = Screen::new(DataQualityPlan {
            temporal_roles: vec![
                role(TemporalRole::Event, "sent"),
                role(TemporalRole::Received, "stamp"),
                role(TemporalRole::Created, "from"),
            ],
            time_formats: vec![TimeInterpretation {
                column: "stamp".to_string(),
                kind: TimeKind::Datetime,
                format: "%Y-%m-%dT%H:%M:%S%.f%#z".to_string(),
            }],
            ..DataQualityPlan::default()
        });
        let text = screen.draw(QualityPage::Setup, 0, 0, (120, 32));
        assert!(
            text.contains("In no interval: created. Choose a start and end under Intervals"),
            "{text}"
        );
        assert!(text.contains("No time zone, read as UTC: sent"), "{text}");
        assert!(text.contains("event to received"), "{text}");

        let text = screen.draw(QualityPage::IntervalPairs, 0, 0, (80, 24));
        assert!(text.contains("Intervals  1 of 6"), "{text}");
        let g = glyphs::get();
        assert!(
            text.contains(&format!("{} event to received", g.checkbox_on)),
            "{text}"
        );
        assert!(
            text.contains(&format!("{} created to event", g.checkbox_off)),
            "{text}"
        );
        assert_glyph_slots(&text);
    }
}

#[cfg(test)]
mod trend_tests {
    use super::*;
    use crate::analysis_modal::ExpectedForm;
    use crate::data_quality::{ExpectedWindows, compute_data_quality};
    use polars::prelude::{DataType, IntoLazy, LazyFrame, col, df};
    use ratatui::style::Color;
    use std::sync::Arc;

    /// Weekday rows over eight weeks, forty a day, the second week missing and a
    /// third of the amounts null.
    fn frame() -> LazyFrame {
        let days = (0..56)
            .filter(|day| day % 7 < 5 && !(7..14).contains(day))
            .collect::<Vec<i32>>();
        let day = days
            .iter()
            .flat_map(|day| std::iter::repeat_n(19_723 + day, 40))
            .collect::<Vec<_>>();
        let rows = day.len();
        df!(
            "day" => day,
            "amount" => (0..rows).map(|row| (row % 3 != 0).then_some(row as f64)).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy()
        .with_column(col("day").cast(DataType::Date))
    }

    struct Screen {
        state: DataTableState,
        plan: DataQualityPlan,
        results: DataQualityResults,
        metric: QualityMetric,
        theme: Theme,
        ctx: RenderContext,
    }

    impl Screen {
        /// A daily study of a 30-row sample, weekdays expected through March 3.
        fn sampled() -> Self {
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
                dataset_rows: 30,
                sample_seed: 415,
                grain: QualityGrain::TimeWindows {
                    column: "day".to_string(),
                    every: "1d".to_string(),
                },
                expected: Some(ExpectedWindows {
                    weekdays: true,
                    from: Some("2024-01-01".to_string()),
                    before: Some("2024-03-04".to_string()),
                }),
                ..DataQualityPlan::default()
            };
            let results = compute_data_quality(&lf, Some(1_400), &plan, None, false).unwrap();
            assert!(!results.unsampled_segments.is_empty());
            Self {
                state,
                plan,
                results,
                metric: QualityMetric::NullRate,
                theme: Theme::from_config(&crate::config::ThemeConfig::default()).unwrap(),
                ctx: RenderContext::for_test(),
            }
        }

        fn draw_with(
            &self,
            theme: &Theme,
            page: QualityPage,
            line: usize,
            selected: usize,
            form: Option<&ExpectedForm>,
            (width, height): (u16, u16),
        ) -> Buffer {
            let config = DataQualityWidgetConfig {
                checks_expanded: false,
                state: &self.state,
                plan: &self.plan,
                measured: &self.plan,
                results: Some(&self.results),
                from_cache: false,
                metric: self.metric,
                column_index: 0,
                segment_index: 0,
                interval_index: 0,
                trend_line: line,
                expected_form: form,
                segments_by_change: false,
                page,
                setup: SetupView::default(),
                plan_field: SetupRow::Expected.index(),
                show_access: false,
                observation_detail: false,
                confirm_run: false,
                focus: AnalysisFocus::Main,
                theme,
                ctx: &self.ctx,
                findings: &FindingsView::default(),
                rows_kept: false,
                evidence_read: None,
                intent_form: None,
                export_form: None,
            };
            let area = Rect::new(0, 0, width, height);
            let mut buf = Buffer::empty(area);
            let mut table = TableState::default();
            table.select(Some(selected));
            render(
                config,
                &mut table,
                &mut TableState::default(),
                &mut DetailScroll::default(),
                area,
                &mut buf,
            );
            buf
        }

        fn draw(
            &self,
            page: QualityPage,
            line: usize,
            selected: usize,
            size: (u16, u16),
        ) -> String {
            text(&self.draw_with(&self.theme, page, line, selected, None, size))
        }

        /// The Trends line of the amount column.
        fn amount(&self) -> usize {
            trend_view(&self.results, self.metric, 1)
                .lines
                .iter()
                .position(|line| line.names == ["amount"])
                .unwrap()
        }
    }

    fn text(buf: &Buffer) -> String {
        let area = buf.area;
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// Every character outside ASCII is a glyph slot, which `LANG=C` swaps for its
    /// ASCII twin.
    fn assert_glyph_slots(text: &str) {
        let g = glyphs::get();
        let slots = [
            g.rail,
            g.rule_h,
            g.middot,
            g.ellipsis,
            g.selector,
            g.unsampled,
            g.pointer,
            g.updown,
        ]
        .concat()
            + &g.mini_bars.concat();
        for c in text.chars().filter(|c| !c.is_ascii()) {
            assert!(
                slots.contains(c) || "╭╮╰╯│─".contains(c),
                "{c:?} is not a glyph slot:\n{text}"
            );
        }
    }

    /// A bar the sample drew nothing from has a mark of its own in both glyph sets:
    /// not a bar level, and not the blank of a measure with nothing to apply to. So
    /// neither a C locale nor a 16-color terminal loses it.
    #[test]
    fn a_missed_bar_has_its_own_mark_in_both_glyph_sets() {
        let screen = Screen::sampled();
        let view = trend_view(&screen.results, QualityMetric::NullRate, 100);
        let missed = view
            .bars
            .iter()
            .position(|bar| bar.evaluated == 0)
            .expect("a day with no sampled row");
        let amount = &view.lines[screen.amount()];
        for g in [glyphs::unicode(), glyphs::ascii()] {
            let mark = bar_mark(amount, &view.bars[missed], missed, g);
            assert_eq!(mark, g.unsampled);
            assert!(!g.mini_bars.contains(&mark) && mark != " ", "{mark:?}");
            assert_ne!(g.pointer, " ");
            assert_eq!(glyphs::display_width(g.unsampled), 1);
            assert_eq!(glyphs::display_width(g.pointer), 1);
        }
        // The exact count still has every day's rows.
        let rows = &view.lines[0];
        assert_eq!(rows.names, ["rows"]);
        assert_eq!(rows.bars[missed], Some(40.0));
    }

    /// Trends says how much of the scope the sample reached, offers a coarser window,
    /// and sums up the expected windows, at 80x24 and 60x20.
    #[test]
    fn trends_say_their_coverage_and_gaps_at_80x24_and_60x20() {
        let screen = Screen::sampled();
        for size in [(80, 24), (60, 20)] {
            let text = screen.draw(QualityPage::Trends, 0, 0, size);
            assert!(text.contains("not sampled"), "{size:?}:\n{text}");
            assert!(text.contains("stages weekly"), "{size:?}:\n{text}");
            assert!(text.contains("Expected weekdays"), "{size:?}:\n{text}");
            assert!(text.contains("sampled rows"), "{size:?}:\n{text}");
            assert!(text.contains("amount"), "{size:?}:\n{text}");
            assert_glyph_slots(&text);
        }
    }

    /// A bar's detail holds every fact at 80x24 and 60x20, the pointer under the bar
    /// selected, and walks to the last bar without running past it.
    #[test]
    fn a_trend_bar_states_its_facts_at_80x24_and_60x20() {
        let screen = Screen::sampled();
        let amount = screen.amount();
        for size in [(80, 24), (60, 20)] {
            let first = screen.draw(QualityPage::TrendDetail, amount, 0, size);
            for label in ["Span", "Segments", "Rows", "Null rate", "95% interval"] {
                assert!(first.contains(label), "{label} at {size:?}:\n{first}");
            }
            assert!(first.contains("bar 1 of"), "{first}");
            assert!(!first.contains("Previous bar"), "nothing before the first");
            let second = screen.draw(QualityPage::TrendDetail, amount, 1, size);
            assert!(second.contains("Previous bar"), "{size:?}:\n{second}");
            // Past the end, the last bar; the pointer sits on the spark's last mark.
            let last = screen.draw(QualityPage::TrendDetail, amount, 10_000, size);
            let lines = last.lines().collect::<Vec<_>>();
            let spark = lines
                .iter()
                .position(|line| line.contains("amount"))
                .unwrap();
            let pointer = lines[spark + 1]
                .find(glyphs::get().pointer)
                .expect("a pointer");
            let marks =
                lines[spark][..lines[spark].find(" │").unwrap_or(lines[spark].len())].trim_end();
            assert_eq!(
                glyphs::display_width(marks) - 1,
                glyphs::display_width(&lines[spark + 1][..pointer]),
                "{last}"
            );
            assert_glyph_slots(&last);
        }
        // A rows line says rows per segment, not a rate.
        let rows = screen.draw(QualityPage::TrendDetail, 0, 0, (80, 24));
        assert!(rows.contains("Rows per segment"), "{rows}");
        assert!(rows.contains("Per segment"), "{rows}");
        assert!(!rows.contains("95% interval"), "{rows}");
    }

    /// Nothing on these pages is said by color alone: drawn with every color gone,
    /// and with the 16 a basic terminal has, every cell holds the same symbol.
    #[test]
    fn trends_and_gaps_read_the_same_without_color() {
        let screen = Screen::sampled();
        let mono = Theme {
            colors: screen
                .theme
                .colors
                .keys()
                .map(|key| (key.clone(), Color::Reset))
                .collect(),
        };
        let sixteen = Theme {
            colors: screen
                .theme
                .colors
                .iter()
                .map(|(key, color)| {
                    let color = match *color {
                        Color::Rgb(r, g, b) => crate::config::rgb_to_basic_ansi(r, g, b),
                        Color::Indexed(index) => Color::Indexed(index % 16),
                        other => other,
                    };
                    (key.clone(), color)
                })
                .collect(),
        };
        for (page, line) in [
            (QualityPage::Trends, 0),
            (QualityPage::TrendDetail, screen.amount()),
            (QualityPage::Gaps, 0),
        ] {
            let full = text(&screen.draw_with(&screen.theme, page, line, 1, None, (80, 24)));
            for theme in [&mono, &sixteen] {
                assert_eq!(
                    text(&screen.draw_with(theme, page, line, 1, None, (80, 24))),
                    full,
                    "{page:?}"
                );
            }
        }
    }

    /// Gaps list each run of windows by why it has no rows: empty by the exact
    /// count, not sampled, with the weekends not expected said apart.
    #[test]
    fn gaps_list_each_kind_at_80x24_and_60x20() {
        let screen = Screen::sampled();
        for size in [(80, 24), (60, 20)] {
            let text = screen.draw(QualityPage::Gaps, 0, 0, size);
            assert!(text.contains("Expected weekdays"), "{size:?}:\n{text}");
            assert!(text.contains("2024-01-08 to 2024-01-12 empty"), "{text}");
            assert!(text.contains("not sampled"), "{text}");
            assert!(text.contains("on weekends, not expected"), "{text}");
            assert!(text.contains("gaps"), "{text}");
            assert_glyph_slots(&text);
        }
        // Wide enough, each missed run says the rows the scope holds there.
        let wide = screen.draw(QualityPage::Gaps, 0, 0, (120, 30));
        assert!(wide.contains("40 rows"), "{wide}");
    }

    /// Setup's Expected row says what is stated, and its editor lays out at 60x20.
    #[test]
    fn setup_states_the_expected_windows() {
        let mut screen = Screen::sampled();
        let setup = screen.draw(QualityPage::Setup, 0, 0, (100, 30));
        assert!(
            setup.contains("Expected       weekdays, 2024-01-01 to before 2024-03-04"),
            "{setup}"
        );
        assert!(
            setup.contains("Expected windows: checked against the segment counts, no read"),
            "{setup}"
        );
        let form = ExpectedForm::new(&screen.plan, &screen.theme);
        let editor = text(&screen.draw_with(
            &screen.theme,
            QualityPage::ExpectedWindows,
            0,
            0,
            Some(&form),
            (60, 20),
        ));
        for text in [
            "Windows",
            "weekdays, Monday to Friday",
            "From",
            "2024-01-01",
            "Before",
        ] {
            assert!(editor.contains(text), "{text}:\n{editor}");
        }
        screen.plan.expected = None;
        let none = screen.draw(QualityPage::Setup, 0, 0, (100, 30));
        assert!(none.contains("none: no window is a gap"), "{none}");
        screen.plan.grain = QualityGrain::Dataset;
        let none = screen.draw(QualityPage::Setup, 0, 0, (100, 30));
        assert!(
            none.contains("Expected       needs a time-window grain"),
            "{none}"
        );
    }

    /// A distinct share is shown against the bar before but never judged, as
    /// Segments never judges one: it falls as a segment grows.
    #[test]
    fn a_distinct_share_is_not_judged_between_bars() {
        let mut screen = Screen::sampled();
        screen.metric = QualityMetric::DistinctShare;
        let second = screen.draw(QualityPage::TrendDetail, screen.amount(), 1, (100, 30));
        assert!(second.contains("Previous bar"), "{second}");
        assert!(second.contains("+0.0 points: not judged"), "{second}");
        assert!(second.contains("none: a distinct share"), "{second}");
    }

    /// One day found is no trend, but the week expected around it still has its
    /// gaps: Trends sums them up beside the way to a grain.
    #[test]
    fn one_window_found_still_sums_up_the_expected_ones() {
        let mut screen = Screen::sampled();
        screen.plan = DataQualityPlan {
            compute: crate::data_quality::QualityCompute::Full,
            expected: Some(ExpectedWindows {
                weekdays: false,
                from: Some("2024-01-01".to_string()),
                before: Some("2024-01-08".to_string()),
            }),
            ..screen.plan.clone()
        };
        let one_day = frame().filter(
            col("day")
                .cast(DataType::Int32)
                .eq(polars::prelude::lit(19_724)),
        );
        screen.results = compute_data_quality(&one_day, None, &screen.plan, None, false).unwrap();
        assert!(!crate::data_quality::shows_trend(
            &screen.plan,
            &screen.results
        ));
        let text = screen.draw(QualityPage::Trends, 0, 0, (80, 24));
        assert!(text.contains("Set Grain"), "{text}");
        assert!(
            text.contains("Expected every day, 7 days: 6 empty"),
            "{text}"
        );
    }

    /// From past the last window found, Before blank: no window is in range, and
    /// Trends says that rather than that every window has rows.
    #[test]
    fn a_range_with_no_window_says_so() {
        let mut screen = Screen::sampled();
        screen.plan.expected = Some(ExpectedWindows {
            weekdays: false,
            from: Some("2025-01-01".to_string()),
            before: None,
        });
        for page in [QualityPage::Trends, QualityPage::Gaps] {
            let text = screen.draw(page, 0, 0, (80, 24));
            assert!(
                text.contains("Expected every day: no window in range"),
                "{page:?}:\n{text}"
            );
        }
    }
}
