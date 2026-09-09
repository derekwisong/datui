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

/// Centered rect within `r` with given percentage width and height.
pub fn centered_rect(r: Rect, percent_x: u16, percent_y: u16) -> Rect {
    let popup_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Percentage((100 - percent_y) / 2),
            Constraint::Percentage(percent_y),
            Constraint::Percentage((100 - percent_y) / 2),
        ])
        .split(r);

    Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage((100 - percent_x) / 2),
            Constraint::Percentage(percent_x),
            Constraint::Percentage((100 - percent_x) / 2),
        ])
        .split(popup_layout[1])[1]
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

/// Like `centered_rect` but enforces minimum width and height.
pub fn centered_rect_with_min(
    r: Rect,
    percent_x: u16,
    percent_y: u16,
    min_width: u16,
    min_height: u16,
) -> Rect {
    let inner = centered_rect(r, percent_x, percent_y);

    // Growing to the minimum must not push the rect outside its parent. On a short
    // terminal the minimum can exceed the whole screen — a 12-row modal centred in 14
    // rows starts at row 5 and would end at row 17 — and drawing past the buffer is a
    // panic, not a clipped draw.
    let width = inner.width.max(min_width).min(r.width);
    let height = inner.height.max(min_height).min(r.height);
    let x = inner.x.min(r.x + r.width.saturating_sub(width));
    let y = inner.y.min(r.y + r.height.saturating_sub(height));

    Rect {
        x,
        y,
        width,
        height,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn centered_rect_with_min_never_leaves_its_parent() {
        // A modal whose minimum size exceeds the terminal must be clipped to it.
        // Drawing outside the buffer panics, so this is a crash, not a cosmetic bug —
        // and a 14-row terminal is an ordinary split pane, not an edge case.
        for (w, h) in [(100, 14), (40, 6), (20, 3), (10, 1), (1, 1)] {
            let parent = Rect::new(0, 0, w, h);
            let got = centered_rect_with_min(parent, 64, 26, 50, 12);
            assert!(
                got.x + got.width <= parent.x + parent.width,
                "{got:?} runs off the right of {parent:?}"
            );
            assert!(
                got.y + got.height <= parent.y + parent.height,
                "{got:?} runs off the bottom of {parent:?}"
            );
        }
    }

    #[test]
    fn centered_rect_with_min_still_grows_when_there_is_room() {
        let parent = Rect::new(0, 0, 200, 60);
        let got = centered_rect_with_min(parent, 10, 10, 50, 12);
        assert_eq!(got.width, 50, "should grow to the minimum width");
        assert_eq!(got.height, 12, "should grow to the minimum height");
    }

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

    #[test]
    fn test_centered_rect_50_50() {
        let area = Rect::new(0, 0, 100, 100);
        let centered = centered_rect(area, 50, 50);

        assert_eq!(centered.width, 50);
        assert_eq!(centered.height, 50);
        assert_eq!(centered.x, 25);
        assert_eq!(centered.y, 25);
    }

    #[test]
    fn test_centered_rect_full_coverage() {
        let area = Rect::new(0, 0, 100, 100);
        let centered = centered_rect(area, 100, 100);

        assert_eq!(centered.width, 100);
        assert_eq!(centered.height, 100);
        assert_eq!(centered.x, 0);
        assert_eq!(centered.y, 0);
    }

    #[test]
    fn test_centered_rect_small_area() {
        let area = Rect::new(10, 10, 20, 20);
        let centered = centered_rect(area, 50, 50);

        assert_eq!(centered.width, 10);
        assert_eq!(centered.height, 10);
        assert_eq!(centered.x, 15);
        assert_eq!(centered.y, 15);
    }

    #[test]
    fn test_centered_rect_with_min_enforcement() {
        let area = Rect::new(0, 0, 100, 100);
        let centered = centered_rect_with_min(area, 10, 10, 50, 50);

        assert!(centered.width >= 50);
        assert!(centered.height >= 50);
    }

    #[test]
    fn test_centered_rect_with_min_no_enforcement_needed() {
        let area = Rect::new(0, 0, 100, 100);
        let centered = centered_rect_with_min(area, 80, 80, 50, 50);

        assert_eq!(centered.width, 80);
        assert_eq!(centered.height, 80);
    }
}
