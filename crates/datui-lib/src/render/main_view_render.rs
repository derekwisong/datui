//! Main view dispatcher: datatable, analysis, or chart.

use crate::render::main_view::MainViewContent;

/// Renders the main view based on content mode: datatable (table + sidebars), analysis, or chart.
pub fn render_main_view(
    area: ratatui::layout::Rect,
    main_area: ratatui::layout::Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &crate::render::context::RenderContext,
) {
    let content = MainViewContent::from_app_state(
        app.analysis_modal.active,
        app.input_mode == crate::InputMode::Chart,
    );
    match content {
        MainViewContent::Datatable => {
            crate::render::datatable_main::render(area, main_area, buf, app, ctx);
        }
        MainViewContent::Analysis => {
            crate::render::analysis_view::render(main_area, buf, app, ctx);
        }
        MainViewContent::Chart => {
            crate::render::chart_view::render(main_area, buf, app, ctx);
        }
    }
}
