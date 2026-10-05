//! Analysis modal view rendering (progress overlay, AnalysisWidget, or no-data message).
//! Also provides help overlay title/text for analysis so the main render loop does not need analysis-specific layout.

use crate::analysis_modal;
use crate::render::context::RenderContext;
use crate::widgets::{analysis, data_quality};
use ratatui::layout::Rect;
use ratatui::style::Style;
use ratatui::text::{Line, Span};
use ratatui::widgets::{Clear, Paragraph, Widget};

/// Renders the analysis view when analysis_modal is active: progress overlay, main
/// widget, or "No data available", with the Sample form over it while it is open.
pub fn render(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    render_body(area, buf, app, ctx);
    if let Some(picker) = &app.analysis_modal.data_quality_picker {
        render_plan_picker(
            picker,
            crate::widgets::data_quality::main_pane(area),
            buf,
            ctx,
        );
    }
    if let Some(form) = &app.analysis_modal.sample_form {
        let quality =
            app.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality);
        // Before a first run the form is the tool's pane; after, it floats over it.
        let target = match (form.inline, quality) {
            (false, _) => area,
            (true, true) => crate::widgets::data_quality::main_pane(area),
            (true, false) => crate::widgets::analysis::main_pane(area),
        };
        let focused =
            !form.inline || app.analysis_modal.focus == analysis_modal::AnalysisFocus::Main;
        if !form.inline {
            // Floating, it owns the keys: what it covers takes no clicks.
            crate::pointer::record(target, crate::pointer::Hit::Modal);
        }
        crate::widgets::sample_form::render(form, focused, target, buf, ctx);
    }
}

/// A Setup row's choices, a short list over the Setup it sets.
fn render_plan_picker(
    picker: &analysis_modal::PlanPicker,
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
) {
    let title = picker.title.as_str();
    let items = picker.state.filtered();
    let widest = items
        .iter()
        .map(|(_, item)| crate::glyphs::display_width(item))
        .max()
        .unwrap_or(0) as u16;
    // A column's margin each side at most: an offset format is long, and its `z` is
    // the part that says it is one.
    let width = (widest + 8).clamp(28, area.width.saturating_sub(2).max(28));
    let height = (items.len() as u16 + 3)
        .min(area.height.saturating_sub(2))
        .max(5);
    let popup = Rect {
        x: area.x + area.width.saturating_sub(width) / 2,
        y: area.y + area.height.saturating_sub(height) / 3,
        width: width.min(area.width),
        height,
    };
    Clear.render(popup, buf);
    let inner = crate::widgets::ui::Surface::new(title).render(popup, buf, ctx);
    let filter = if picker.state.filter.is_empty() {
        Line::from(Span::styled(
            "type to narrow",
            Style::default().fg(ctx.dimmed),
        ))
    } else {
        Line::from(Span::raw(picker.state.filter.clone()))
    };
    Paragraph::new(filter).render(Rect { height: 1, ..inner }, buf);
    crate::widgets::ui::Picker::from_state(&picker.state, true).render(
        Rect {
            y: inner.y + 1,
            height: inner.height.saturating_sub(1),
            ..inner
        },
        buf,
        ctx,
    );
}

fn render_body(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    if let Some(ref progress) = app.analysis_modal.computing {
        // A run has no total to count against, so a gauge could only ever read 0%.
        // What moves is time and the stage: the spinner, the clock and the stage's
        // name say it is alive, rows are counted only where the read counts them,
        // and the control bar says Esc cancels. Every tool, Data Quality included,
        // runs behind this one view, whose lines never move as the stages go by.
        Clear.render(area, buf);
        let g = crate::glyphs::get();
        let spinner = g.spinner[app.throbber_frame as usize % g.spinner.len()];
        let quality =
            app.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality);
        let source = if quality {
            let rows = progress
                .read
                .as_ref()
                .and_then(crate::sampling::ReadWatch::rows_seen)
                .map(|rows| {
                    format!(
                        " {} {} rows so far",
                        g.middot,
                        crate::numfmt::group_chrome(rows)
                    )
                })
                .unwrap_or_default();
            match progress.reads_source {
                Some(true) => format!("   Reading the source{rows}"),
                Some(false) => "   No source read: rows already read".to_string(),
                None => progress
                    .reuse
                    .as_ref()
                    .map(|reuse| format!("   {reuse}"))
                    .unwrap_or_default(),
            }
        } else {
            String::new()
        };
        let lines = vec![
            Line::from(""),
            Line::from(vec![
                Span::styled(format!(" {spinner} "), Style::default().fg(ctx.accent)),
                Span::styled(
                    progress.phase.clone(),
                    Style::default().fg(ctx.text_primary),
                ),
                Span::styled(
                    format!("  {}", elapsed(progress.started.elapsed())),
                    Style::default().fg(ctx.dimmed),
                ),
            ]),
            // What decides how long this takes, stated rather than left to guess.
            Line::from(Span::styled(
                if quality
                    && app.analysis_modal.data_quality_plan.compute
                        == crate::data_quality::QualityCompute::Metadata
                {
                    "   Reads file metadata only, no values".to_string()
                } else {
                    format!("   Reads {}", app.analysis_modal.sample.summary())
                },
                Style::default().fg(ctx.dimmed),
            )),
            Line::from(Span::styled(source, Style::default().fg(ctx.dimmed))),
        ];
        Paragraph::new(lines).render(area, buf);
    } else if let Some(state) = &app.data_table_state {
        if app.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            // Borrowed field by field, never cloned. A per-file profile of a large
            // dataset owns a column profile per column per segment, and copying all of
            // it once per repaint made the dashboard slowest at the scale it is for.
            let candidates = app.quality_time_candidates();
            let note = app.analysis_modal.data_quality_setup_note.clone();
            let plan = &app.analysis_modal.data_quality_plan;
            let unchanged = app.analysis_modal.data_quality_results.is_some()
                && app.analysis_modal.data_quality_last_plan.as_ref() == Some(plan);
            let relabel_only = !unchanged
                && app.analysis_modal.data_quality_results.is_some()
                && app
                    .analysis_modal
                    .data_quality_last_plan
                    .as_ref()
                    .is_some_and(|last| last.same_measurement(plan));
            let setup = data_quality::SetupView {
                time_candidates: &candidates,
                reuses_sample: app.quality_kept_serves(plan),
                reads_blocks: app.quality_reads_blocks(plan),
                segment_count: app.quality_segment_count(plan),
                released: app.quality_released(plan),
                cached: app.quality_cached(plan),
                unchanged,
                relabel_only,
                edited: app.analysis_modal.setup_edited(),
                note: note.as_deref(),
                cancelling: app.cancelled_run_shown(),
                kept: app.quality_kept_rows(),
                copy: app.quality_copy_plan(plan),
                copy_released: app.quality_copy_released(),
            };
            let rows_kept = app.quality_rows_kept().is_some();
            let modal = &mut app.analysis_modal;
            let config = data_quality::DataQualityWidgetConfig {
                checks_expanded: modal.data_quality_checks_expanded,
                state,
                // Setup edits the draft; the result pages show the plan they were
                // measured with, whatever is being staged.
                plan: if modal.data_quality_page.is_setup() {
                    &modal.data_quality_plan
                } else {
                    modal
                        .data_quality_last_plan
                        .as_ref()
                        .unwrap_or(&modal.data_quality_plan)
                },
                measured: modal
                    .data_quality_last_plan
                    .as_ref()
                    .unwrap_or(&modal.data_quality_plan),
                results: modal.data_quality_results.as_ref(),
                from_cache: modal.data_quality_from_cache,
                metric: modal.data_quality_metric,
                column_index: modal.data_quality_column_index,
                segment_index: modal.data_quality_segment_index,
                interval_index: modal.data_quality_interval_index,
                trend_line: modal.data_quality_trend_line,
                expected_form: modal.data_quality_expected_form.as_ref(),
                segments_by_change: modal.data_quality_segments_by_change,
                page: modal.data_quality_page,
                setup,
                plan_field: modal.data_quality_plan_field,
                show_access: modal.data_quality_show_access,
                observation_detail: modal.data_quality_observation_detail,
                findings: &modal.data_quality_findings,
                rows_kept,
                evidence_read: modal.data_quality_evidence_read.as_ref(),
                focus: modal.focus,
                theme: &app.theme,
                ctx,
                intent_form: modal.data_quality_intent_form.as_ref(),
                export_form: modal.data_quality_export.as_ref(),
            };
            Clear.render(area, buf);
            data_quality::render(
                config,
                &mut modal.data_quality_table_state,
                &mut modal.sidebar_state,
                &mut modal.data_quality_detail_scroll,
                area,
                buf,
            );
            return;
        }
        let context = state.get_analysis_context();
        Clear.render(area, buf);

        let results_for_widget = app.analysis_modal.current_results().cloned();
        let config = analysis::AnalysisWidgetConfig {
            state,
            results: results_for_widget.as_ref(),
            context: &context,
            view: app.analysis_modal.view,
            selected_tool: app.analysis_modal.selected_tool,
            selected_correlation: app.analysis_modal.selected_correlation,
            correlation_method: app.analysis_modal.correlation_method,
            focus: app.analysis_modal.focus,
            selected_theoretical_distribution: app.analysis_modal.selected_theoretical_distribution,
            histogram_scale: app.analysis_modal.histogram_scale,
            theme: &app.theme,
            table_cell_padding: app.table_cell_padding,
            number_format: &ctx.number_format,
            sample: &app.analysis_modal.sample,
            ctx,
        };
        // Each table sets how far it scrolls as it draws.
        let mut unused = analysis_modal::ColumnScroll::default();
        let column_scroll = match app.analysis_modal.selected_tool {
            Some(analysis_modal::AnalysisTool::Describe) => {
                &mut app.analysis_modal.describe_columns
            }
            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                &mut app.analysis_modal.distribution_columns
            }
            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                &mut app.analysis_modal.correlation_columns
            }
            _ => &mut unused,
        };
        let widget = analysis::AnalysisWidget::new(
            config,
            &mut app.analysis_modal.table_state,
            &mut app.analysis_modal.distribution_table_state,
            &mut app.analysis_modal.correlation_table_state,
            &mut app.analysis_modal.sidebar_state,
            &mut app.analysis_modal.distribution_selector_state,
            column_scroll,
        );
        widget.render(area, buf);
    } else {
        Clear.render(area, buf);
        Paragraph::new("No data available for analysis")
            .centered()
            .style(ratatui::style::Style::default().fg(ctx.warning))
            .render(area, buf);
    }
}

/// `12s`, `3m 05s`, `1h 02m`: a clock for a wait, to the second while seconds matter.
pub(crate) fn elapsed(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    match secs {
        0..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m {:02}s", secs / 60, secs % 60),
        _ => format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60),
    }
}
