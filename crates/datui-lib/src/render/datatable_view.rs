use ratatui::layout::Rect;

/// Which sidebar (right panel) is currently active, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSidebar {
    None,
    Info,
    SortFilter,
    Template,
    PivotMelt,
}

impl ActiveSidebar {
    /// Determine which sidebar is active from modal states.
    pub fn from_modals(
        info_active: bool,
        sort_filter_active: bool,
        template_active: bool,
        pivot_melt_active: bool,
    ) -> Self {
        if info_active {
            ActiveSidebar::Info
        } else if sort_filter_active {
            ActiveSidebar::SortFilter
        } else if template_active {
            ActiveSidebar::Template
        } else if pivot_melt_active {
            ActiveSidebar::PivotMelt
        } else {
            ActiveSidebar::None
        }
    }

    /// Get sidebar width for this sidebar type.
    /// When `config_override` is Some(w), use that for all sidebars; otherwise use built-in defaults.
    pub fn width(&self, config_override: Option<u16>) -> u16 {
        if let Some(w) = config_override {
            return w;
        }
        match self {
            ActiveSidebar::None => 0,
            ActiveSidebar::Info => 72,
            ActiveSidebar::SortFilter => 50,
            ActiveSidebar::Template => 80,
            ActiveSidebar::PivotMelt => 50,
        }
    }
}

/// Layout for datatable view internals.
/// This splits the main view into content area, optional sidebar, and optional input strip.
#[derive(Debug, Clone, Copy)]
pub struct DatatableLayout {
    /// Area for table content (and breadcrumb if drilled down).
    pub content_area: Rect,
    /// Area for sidebar (when active).
    pub sidebar_area: Option<Rect>,
    pub input_strip_area: Option<Rect>,
}

impl DatatableLayout {
    /// Splits main_view into content (and optional sidebar) plus optional input strip at bottom.
    pub fn compute(
        main_view: Rect,
        active_sidebar: ActiveSidebar,
        input_strip_visible: bool,
        input_strip_height: u16,
        sidebar_width_override: Option<u16>,
    ) -> Self {
        use ratatui::layout::{Constraint, Direction, Layout};

        let (content_region, input_strip_area) = if input_strip_visible && input_strip_height > 0 {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Min(0), Constraint::Length(input_strip_height)])
                .split(main_view);
            (layout[0], Some(layout[1]))
        } else {
            (main_view, None)
        };

        let (content_area, sidebar_area) = if active_sidebar != ActiveSidebar::None {
            let sidebar_width = active_sidebar.width(sidebar_width_override);
            let layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([Constraint::Min(0), Constraint::Length(sidebar_width)])
                .split(content_region);
            (layout[0], Some(layout[1]))
        } else {
            (content_region, None)
        };

        DatatableLayout {
            content_area,
            sidebar_area,
            input_strip_area,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_sidebar_from_modals_none() {
        assert_eq!(
            ActiveSidebar::from_modals(false, false, false, false),
            ActiveSidebar::None
        );
    }

    #[test]
    fn test_active_sidebar_from_modals_info() {
        assert_eq!(
            ActiveSidebar::from_modals(true, false, false, false),
            ActiveSidebar::Info
        );
    }

    #[test]
    fn test_active_sidebar_from_modals_priority() {
        assert_eq!(
            ActiveSidebar::from_modals(true, true, true, true),
            ActiveSidebar::Info
        );
        assert_eq!(
            ActiveSidebar::from_modals(false, true, true, true),
            ActiveSidebar::SortFilter
        );
        assert_eq!(
            ActiveSidebar::from_modals(false, false, true, true),
            ActiveSidebar::Template
        );
        assert_eq!(
            ActiveSidebar::from_modals(false, false, false, true),
            ActiveSidebar::PivotMelt
        );
    }

    #[test]
    fn test_active_sidebar_width() {
        assert_eq!(ActiveSidebar::None.width(None), 0);
        assert_eq!(ActiveSidebar::Info.width(None), 72);
        assert_eq!(ActiveSidebar::SortFilter.width(None), 50);
        assert_eq!(ActiveSidebar::Template.width(None), 80);
        assert_eq!(ActiveSidebar::PivotMelt.width(None), 50);
        assert_eq!(ActiveSidebar::Info.width(Some(70)), 70);
        assert_eq!(ActiveSidebar::SortFilter.width(Some(60)), 60);
    }

    #[test]
    fn test_datatable_layout_no_sidebar_no_input() {
        let main_view = Rect::new(0, 0, 100, 50);
        let layout = DatatableLayout::compute(main_view, ActiveSidebar::None, false, 0, None);

        assert_eq!(layout.content_area, main_view);
        assert_eq!(layout.sidebar_area, None);
        assert_eq!(layout.input_strip_area, None);
    }

    #[test]
    fn test_datatable_layout_with_sidebar() {
        let main_view = Rect::new(0, 0, 100, 50);
        let layout = DatatableLayout::compute(main_view, ActiveSidebar::Info, false, 0, None);

        assert_eq!(layout.content_area.width, 28);
        assert_eq!(layout.sidebar_area.unwrap().width, 72);
        assert_eq!(layout.input_strip_area, None);
    }

    #[test]
    fn test_datatable_layout_with_input_strip() {
        let main_view = Rect::new(0, 0, 100, 50);
        let layout = DatatableLayout::compute(main_view, ActiveSidebar::None, true, 5, None);

        assert_eq!(layout.content_area.height, 45);
        assert_eq!(layout.sidebar_area, None);
        let strip = layout.input_strip_area.unwrap();
        assert_eq!(strip.height, 5);
        assert_eq!(
            layout.content_area.y + layout.content_area.height,
            strip.y,
            "content and input_strip adjacent"
        );
    }

    #[test]
    fn test_datatable_layout_with_sidebar_and_input() {
        let main_view = Rect::new(0, 0, 100, 50);
        let layout = DatatableLayout::compute(main_view, ActiveSidebar::SortFilter, true, 3, None);

        assert_eq!(layout.content_area.width, 50);
        assert_eq!(layout.content_area.height, 47);
        assert_eq!(layout.sidebar_area.unwrap().width, 50);
        assert_eq!(layout.sidebar_area.unwrap().height, 47);
        let strip = layout.input_strip_area.unwrap();
        assert_eq!(strip.height, 3);
        assert_eq!(
            layout.content_area.y + layout.content_area.height,
            strip.y,
            "content and input_strip adjacent"
        );
        assert_eq!(
            strip.y + strip.height,
            main_view.y + main_view.height,
            "input_strip fills bottom of main_view"
        );
    }
}
