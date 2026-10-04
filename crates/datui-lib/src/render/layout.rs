use ratatui::layout::{Constraint, Direction, Layout, Rect};

/// Top-level layout: main view, control bar, optional debug row. Input strip and sidebars are split from main view by DatatableLayout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppLayout {
    pub main_view: Rect,
    pub control_bar: Rect,
    pub debug: Option<Rect>,
}

/// Top-level vertical layout: main view (fill), control bar (1 row), optional debug (1 row).
/// Input strip and sidebars are internal to the datatable view; they split main_view via DatatableLayout.
pub fn app_layout(area: Rect, debug_enabled: bool) -> AppLayout {
    let mut constraints = vec![Constraint::Fill(1), Constraint::Length(1)];

    if debug_enabled {
        constraints.push(Constraint::Length(1));
    }

    let layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints(constraints)
        .split(area);

    let main_view = layout[0];
    let control_bar_idx = layout.len() - if debug_enabled { 2 } else { 1 };
    let control_bar = layout[control_bar_idx];

    let debug = if debug_enabled {
        Some(layout[layout.len() - 1])
    } else {
        None
    };

    AppLayout {
        main_view,
        control_bar,
        debug,
    }
}

/// Centered rect with fixed width and height, clamped to fit inside `r`.
/// Use for modals that must not shrink (e.g. delete confirm) so content stays visible.
pub fn centered_rect_fixed(r: Rect, width: u16, height: u16) -> Rect {
    let w = width.min(r.width);
    let h = height.min(r.height);
    let x = r.x + r.width.saturating_sub(w) / 2;
    let y = r.y + r.height.saturating_sub(h) / 2;
    Rect {
        x,
        y,
        width: w,
        height: h,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_app_layout_minimal() {
        let area = Rect::new(0, 0, 100, 50);
        let layout = app_layout(area, false);

        assert_eq!(layout.main_view.height, 49);
        assert_eq!(layout.control_bar.height, 1);
        assert_eq!(layout.control_bar.y, 49);
        assert_eq!(layout.debug, None);
    }

    #[test]
    fn test_app_layout_with_debug() {
        let area = Rect::new(0, 0, 100, 50);
        let layout = app_layout(area, true);

        assert_eq!(layout.main_view.height, 48);
        assert_eq!(layout.control_bar.height, 1);
        assert_eq!(layout.control_bar.y, 48);
        assert!(layout.debug.is_some());
        assert_eq!(layout.debug.unwrap().height, 1);
        assert_eq!(layout.debug.unwrap().y, 49);
    }
}
