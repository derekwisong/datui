//! Analysis modal view rendering (progress overlay, AnalysisWidget, or no-data message).
//! Also provides help overlay title/text for analysis so the main render loop does not need analysis-specific layout.

use crate::analysis_modal::{self, AnalysisModal};
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
        crate::widgets::sample_form::render(form, focused, target, buf, ctx);
    }
}

/// A plan field's choices, a short list over the plan it sets.
fn render_plan_picker(
    picker: &analysis_modal::PlanPicker,
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    ctx: &RenderContext,
) {
    let title = match picker.field {
        1 => "Grain",
        2 => "Values",
        3 => "Compare",
        _ => "Latency threshold",
    };
    let items = picker.state.filtered();
    let widest = items
        .iter()
        .map(|(_, item)| crate::glyphs::display_width(item))
        .max()
        .unwrap_or(0) as u16;
    let width = (widest + 8).clamp(28, area.width.saturating_sub(4).max(28));
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
        // A run is one Polars query with no steps to count, so a gauge could only ever
        // read 0%. What moves is time: the spinner and the clock say it is alive, and
        // the control bar says Esc cancels. Every tool, Data Quality included, runs
        // behind this one view.
        Clear.render(area, buf);
        let g = crate::glyphs::get();
        let spinner = g.spinner[app.throbber_frame as usize % g.spinner.len()];
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
                if app.analysis_modal.selected_tool
                    == Some(analysis_modal::AnalysisTool::DataQuality)
                    && app.analysis_modal.data_quality_plan.compute
                        == crate::data_quality::QualityCompute::Metadata
                {
                    "   Reads file metadata only, no values".to_string()
                } else {
                    format!("   Reads {}", app.analysis_modal.sample.summary())
                },
                Style::default().fg(ctx.dimmed),
            )),
        ];
        Paragraph::new(lines).render(area, buf);
    } else if let Some(state) = &app.data_table_state {
        if app.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality) {
            // Borrowed field by field, never cloned. A per-file profile of a large
            // dataset owns a column profile per column per segment, and copying all of
            // it once per repaint made the dashboard slowest at the scale it is for.
            let modal = &mut app.analysis_modal;
            let config = data_quality::DataQualityWidgetConfig {
                first_run: modal.sample_form.as_ref().is_some_and(|form| form.inline),
                checks_expanded: modal.data_quality_checks_expanded,
                state,
                // The Plan page edits the working plan; the result pages show the
                // plan they were measured with, whatever is being edited.
                plan: if matches!(
                    modal.data_quality_page,
                    crate::data_quality::QualityPage::Plan
                        | crate::data_quality::QualityPage::TimeRoles
                ) {
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
                segments_by_change: modal.data_quality_segments_by_change,
                page: modal.data_quality_page,
                pending: modal.quality_plan_pending(),
                plan_field: modal.data_quality_plan_field,
                show_access: modal.data_quality_show_access,
                observation_detail: modal.data_quality_observation_detail,
                confirm_run: modal.data_quality_confirm_run,
                focus: modal.focus,
                theme: &app.theme,
                ctx,
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
        let column_offset = match app.analysis_modal.selected_tool {
            Some(analysis_modal::AnalysisTool::Describe) => {
                app.analysis_modal.describe_column_offset
            }
            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => {
                app.analysis_modal.distribution_column_offset
            }
            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => {
                app.analysis_modal.correlation_column_offset
            }
            Some(analysis_modal::AnalysisTool::DataQuality) => 0,
            None => 0,
        };

        let results_for_widget = app.analysis_modal.current_results().cloned();
        let config = analysis::AnalysisWidgetConfig {
            state,
            results: results_for_widget.as_ref(),
            context: &context,
            view: app.analysis_modal.view,
            selected_tool: app.analysis_modal.selected_tool,
            column_offset,
            selected_correlation: app.analysis_modal.selected_correlation,
            focus: app.analysis_modal.focus,
            selected_theoretical_distribution: app.analysis_modal.selected_theoretical_distribution,
            histogram_scale: app.analysis_modal.histogram_scale,
            theme: &app.theme,
            table_cell_padding: app.table_cell_padding,
            number_format: &ctx.number_format,
            sample: &app.analysis_modal.sample,
        };
        let widget = analysis::AnalysisWidget::new(
            config,
            &mut app.analysis_modal.table_state,
            &mut app.analysis_modal.distribution_table_state,
            &mut app.analysis_modal.correlation_table_state,
            &mut app.analysis_modal.sidebar_state,
            &mut app.analysis_modal.distribution_selector_state,
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

/// Returns (title, text) for the help overlay when analysis modal help is shown.
/// Keeps analysis-specific help content and layout in the analysis view module.
pub fn help_title_and_text(modal: &AnalysisModal) -> (String, String) {
    match modal.view {
        analysis_modal::AnalysisView::DistributionDetail => (
            "Distribution Detail Help".to_string(),
            crate::help_strings::analysis_distribution_detail().to_string(),
        ),
        analysis_modal::AnalysisView::CorrelationDetail => (
            "Correlation Detail Help".to_string(),
            crate::help_strings::analysis_correlation_detail().to_string(),
        ),
        analysis_modal::AnalysisView::Main => match modal.selected_tool {
            Some(analysis_modal::AnalysisTool::DistributionAnalysis) => (
                "Distribution Analysis Help".to_string(),
                crate::help_strings::analysis_distribution().to_string(),
            ),
            Some(analysis_modal::AnalysisTool::Describe) => (
                "Describe Tool Help".to_string(),
                crate::help_strings::analysis_describe().to_string(),
            ),
            Some(analysis_modal::AnalysisTool::CorrelationMatrix) => (
                "Correlation Matrix Help".to_string(),
                crate::help_strings::analysis_correlation_matrix().to_string(),
            ),
            Some(analysis_modal::AnalysisTool::DataQuality) => (
                "Data Quality Help".to_string(),
                crate::help_strings::analysis_data_quality().to_string(),
            ),
            None => (
                "Analysis Help".to_string(),
                "Select an analysis tool from the sidebar.".to_string(),
            ),
        },
    }
}

/// `12s`, `3m 05s`, `1h 02m`: a clock for a wait, to the second while seconds matter.
fn elapsed(d: std::time::Duration) -> String {
    let secs = d.as_secs();
    match secs {
        0..=59 => format!("{secs}s"),
        60..=3599 => format!("{}m {:02}s", secs / 60, secs % 60),
        _ => format!("{}h {:02}m", secs / 3600, (secs % 3600) / 60),
    }
}
