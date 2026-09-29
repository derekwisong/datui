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
    if let Some(form) = &app.analysis_modal.sample_form {
        let quality =
            app.analysis_modal.selected_tool == Some(analysis_modal::AnalysisTool::DataQuality);
        // Data Quality's row-by-row checks keep at most 50,000 rows a run.
        let cap = quality.then_some(50_000);
        // Before a first run the form is the tool's pane; after, it floats over it.
        let target = match (form.inline, quality) {
            (false, _) => area,
            (true, true) => crate::widgets::data_quality::main_pane(area),
            (true, false) => crate::widgets::analysis::main_pane(area),
        };
        let focused =
            !form.inline || app.analysis_modal.focus == analysis_modal::AnalysisFocus::Main;
        crate::widgets::sample_form::render(form, focused, cap, target, buf, ctx);
    }
}

fn render_body(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    if let Some(ref progress) = app.analysis_modal.computing
        && app.analysis_modal.selected_tool != Some(analysis_modal::AnalysisTool::DataQuality)
    {
        // A run is one Polars query with no steps to count, so a gauge could only ever
        // read 0%. What moves is time: the spinner and the clock say it is alive, and
        // the control bar says Esc cancels.
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
                format!("   Reads {}", app.analysis_modal.sample.summary()),
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
                checks_expanded: modal.data_quality_checks_expanded,
                state,
                plan: &modal.data_quality_plan,
                results: modal.data_quality_results.as_ref(),
                from_cache: modal.data_quality_from_cache,
                metric: modal.data_quality_metric,
                column_index: modal.data_quality_column_index,
                page: modal.data_quality_page,
                editing: modal.data_quality_editing,
                plan_field: modal.data_quality_plan_field,
                show_access: modal.data_quality_show_access,
                observation_detail: modal.data_quality_observation_detail,
                confirm_run: modal.data_quality_confirm_run,
                running: modal.computing.is_some(),
                focus: modal.focus,
                theme: &app.theme,
            };
            Clear.render(area, buf);
            data_quality::render(
                config,
                &mut modal.data_quality_table_state,
                &mut modal.sidebar_state,
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
