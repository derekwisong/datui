//! Main view dispatcher: home, loading, datatable, analysis, or chart.

use crate::render::main_view::MainViewContent;

/// Renders whichever view [`MainViewContent::current`] says is showing.
pub fn render_main_view(
    area: ratatui::layout::Rect,
    main_area: ratatui::layout::Rect,
    buf: &mut ratatui::buffer::Buffer,
    app: &mut crate::App,
    ctx: &crate::render::context::RenderContext,
) {
    let content = MainViewContent::current(app);
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
        MainViewContent::Home => {
            crate::render::home_view::render(main_area, buf, app, ctx);
        }
        MainViewContent::Loading => {
            crate::render::loading_view::render(main_area, buf, app, ctx);
        }
    }
}
