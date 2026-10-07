use ratatui::layout::Rect;

/// Which sidebar (right panel) is currently active, if any.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ActiveSidebar {
    None,
    Info,
    SortFilter,
    Views,
}

impl ActiveSidebar {
    /// Determine which sidebar is active from modal states.
    pub fn from_modals(info_active: bool, sort_filter_active: bool, view_active: bool) -> Self {
        if info_active {
            ActiveSidebar::Info
        } else if sort_filter_active {
            ActiveSidebar::SortFilter
        } else if view_active {
            ActiveSidebar::Views
        } else {
            ActiveSidebar::None
        }
    }

    /// This sidebar's width: `config_override` for all sidebars when set, else its own
    /// default.
    pub fn width(&self, config_override: Option<u16>) -> u16 {
        if let Some(w) = config_override {
            return w;
        }
        match self {
            ActiveSidebar::None => 0,
            ActiveSidebar::Info => 72,
            ActiveSidebar::SortFilter => 50,
            ActiveSidebar::Views => 80,
        }
    }
}

/// Layout for datatable view internals.
/// This splits the main view into the content area and an optional sidebar.
#[derive(Debug, Clone, Copy)]
pub struct DatatableLayout {
    /// Area for table content (and breadcrumb if drilled down).
    pub content_area: Rect,
    /// Area for sidebar (when active).
    pub sidebar_area: Option<Rect>,
}

impl DatatableLayout {
    /// Splits main_view into content and an optional sidebar.
    pub fn compute(
        main_view: Rect,
        active_sidebar: ActiveSidebar,
        sidebar_width_override: Option<u16>,
    ) -> Self {
        use ratatui::layout::{Constraint, Direction, Layout};

        let content_region = main_view;

        let (content_area, sidebar_area) = if active_sidebar != ActiveSidebar::None {
            // The data never yields to chrome: a sidebar takes its preferred
            // width — configured or built in — only out of what is left after
            // the table keeps a readable strip. On a terminal too narrow for
            // both, the two split evenly rather than the sidebar taking all.
            const TABLE_MIN: u16 = 30;
            const SIDEBAR_MIN: u16 = 20;
            let desired = active_sidebar.width(sidebar_width_override);
            let room = content_region.width.saturating_sub(TABLE_MIN);
            let sidebar_width = if room >= SIDEBAR_MIN {
                desired.min(room)
            } else {
                desired.min(content_region.width / 2)
            };
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_active_sidebar_from_modals_none() {
        assert_eq!(
            ActiveSidebar::from_modals(false, false, false),
            ActiveSidebar::None
        );
    }

    #[test]
    fn test_active_sidebar_from_modals_info() {
        assert_eq!(
            ActiveSidebar::from_modals(true, false, false),
            ActiveSidebar::Info
        );
    }

    #[test]
    fn test_active_sidebar_from_modals_priority() {
        assert_eq!(
            ActiveSidebar::from_modals(true, true, true),
            ActiveSidebar::Info
        );
        assert_eq!(
            ActiveSidebar::from_modals(false, true, true),
            ActiveSidebar::SortFilter
        );
        assert_eq!(
            ActiveSidebar::from_modals(false, false, true),
            ActiveSidebar::Views
        );
    }

    #[test]
    fn test_active_sidebar_width() {
        assert_eq!(ActiveSidebar::None.width(None), 0);
        assert_eq!(ActiveSidebar::Info.width(None), 72);
        assert_eq!(ActiveSidebar::SortFilter.width(None), 50);
        assert_eq!(ActiveSidebar::Views.width(None), 80);
        assert_eq!(ActiveSidebar::Info.width(Some(70)), 70);
        assert_eq!(ActiveSidebar::SortFilter.width(Some(60)), 60);
    }

    #[test]
    fn test_datatable_layout_no_sidebar_no_input() {
        let main_view = Rect::new(0, 0, 100, 50);
        let layout = DatatableLayout::compute(main_view, ActiveSidebar::None, None);

        assert_eq!(layout.content_area, main_view);
        assert_eq!(layout.sidebar_area, None);
    }

    #[test]
    fn test_datatable_layout_with_sidebar() {
        let main_view = Rect::new(0, 0, 110, 50);
        let layout = DatatableLayout::compute(main_view, ActiveSidebar::Info, None);

        assert_eq!(layout.content_area.width, 38);
        assert_eq!(layout.sidebar_area.unwrap().width, 72);
    }

    /// The data never yields to chrome: every sidebar leaves the table a
    /// readable strip at 80×24 and 60×20, configured widths included.
    #[test]
    fn a_sidebar_never_consumes_the_table() {
        for sidebar in [
            ActiveSidebar::Info,
            ActiveSidebar::SortFilter,
            ActiveSidebar::Views,
        ] {
            for (width, height) in [(60u16, 20u16), (80, 24), (160, 40)] {
                for config in [None, Some(100u16)] {
                    let main_view = Rect::new(0, 0, width, height);
                    let layout = DatatableLayout::compute(main_view, sidebar, config);
                    let bar = layout.sidebar_area.unwrap();
                    assert!(
                        layout.content_area.width >= 30,
                        "{sidebar:?} at {width}x{height} (config {config:?}) leaves \
                         {} columns of table",
                        layout.content_area.width
                    );
                    assert!(bar.width >= 20, "{sidebar:?} squeezed to {}", bar.width);
                    assert_eq!(layout.content_area.width + bar.width, width);
                }
            }
        }
        // Below the floor the two split evenly rather than the sidebar
        // taking the whole terminal.
        let tiny = Rect::new(0, 0, 44, 16);
        let layout = DatatableLayout::compute(tiny, ActiveSidebar::Views, None);
        assert_eq!(layout.sidebar_area.unwrap().width, 22);
        assert_eq!(layout.content_area.width, 22);
    }
}
