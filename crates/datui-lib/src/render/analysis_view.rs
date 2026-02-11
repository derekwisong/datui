//! Analysis modal view rendering (progress overlay, AnalysisWidget, or no-data message).
//! Also provides help overlay title/text for analysis so the main render loop does not need analysis-specific layout.

use crate::analysis_modal::{self, AnalysisModal};
use crate::render::context::RenderContext;
use crate::widgets::analysis;
use ratatui::layout::Rect;
use ratatui::widgets::{Block, BorderType, Borders, Clear, Gauge, Paragraph, Widget};

/// Renders the analysis view when analysis_modal is active: progress overlay, main widget, or "No data available".
pub fn render(
    area: Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &RenderContext,
) {
    if let Some(ref progress) = app.analysis_modal.computing {
        let percent = if progress.total > 0 {
            (progress.current as u16).saturating_mul(100) / progress.total as u16
        } else {
            0
        };
        Clear.render(area, buf);
        let block = Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .border_style(ratatui::style::Style::default().fg(ctx.modal_border))
            .title(" Analysis ");
        let inner = block.inner(area);
        block.render(area, buf);
        let text = format!(
            "{}: {} / {}",
            progress.phase, progress.current, progress.total
        );
        Paragraph::new(text)
            .style(ratatui::style::Style::default().fg(ctx.text_primary))
            .render(
                Rect {
                    x: inner.x,
                    y: inner.y,
                    width: inner.width,
                    height: 1,
                },
                buf,
            );
        Gauge::default()
            .gauge_style(ratatui::style::Style::default().fg(ctx.label))
            .ratio(percent as f64 / 100.0)
            .render(
                Rect {
                    x: inner.x,
                    y: inner.y + 1,
                    width: inner.width,
                    height: 1,
                },
                buf,
            );
    } else if let Some(state) = &app.data_table_state {
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
            None => (
                "Analysis Help".to_string(),
                "Select an analysis tool from the sidebar.".to_string(),
            ),
        },
    }
}
