use ratatui::layout::Rect;

/// Top-level layout: the main view, the rule above the footer, the footer, and an
/// optional debug row. Sidebars are split from the main view by DatatableLayout.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppLayout {
    pub main_view: Rect,
    /// The thin rule between the view and the footer.
    pub rule: Rect,
    /// The footer's lines: the status line, then whatever it grew for.
    pub footer: Rect,
    pub debug: Option<Rect>,
}

/// Top-level vertical layout: the main view fills what the rule, the footer's
/// `footer_lines` and the debug row leave. The footer grows by taking rows from the
/// bottom of the main view, so the view's top stays put. Without `rule` (a framed
/// takeover, whose own border sets it off) the view keeps that row.
pub fn app_layout(area: Rect, debug_enabled: bool, footer_lines: u16, rule: bool) -> AppLayout {
    let debug_rows = u16::from(debug_enabled).min(area.height);
    let footer_lines = footer_lines.clamp(1, crate::render::footer::MAX_LINES);
    // The status line before the rule, and the rule before the view.
    let footer_rows = footer_lines.min(area.height - debug_rows);
    let rule_rows = u16::from(rule).min(area.height - debug_rows - footer_rows);
    let main_rows = area.height - debug_rows - footer_rows - rule_rows;
    let row = |y: u16, height: u16| Rect {
        x: area.x,
        y,
        width: area.width,
        height,
    };
    let main_view = row(area.y, main_rows);
    let rule = row(area.y + main_rows, rule_rows);
    let footer = row(rule.y + rule_rows, footer_rows);
    let debug = debug_enabled.then(|| row(footer.y + footer_rows, debug_rows));
    AppLayout {
        main_view,
        rule,
        footer,
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
        let layout = app_layout(area, false, 1, true);

        assert_eq!(layout.main_view.height, 48);
        assert_eq!(layout.rule.y, 48);
        assert_eq!(layout.footer.height, 1);
        assert_eq!(layout.footer.y, 49);
        assert_eq!(layout.debug, None);
    }

    #[test]
    fn test_app_layout_with_debug() {
        let area = Rect::new(0, 0, 100, 50);
        let layout = app_layout(area, true, 1, true);

        assert_eq!(layout.main_view.height, 47);
        assert_eq!(layout.footer.height, 1);
        assert_eq!(layout.footer.y, 48);
        assert!(layout.debug.is_some());
        assert_eq!(layout.debug.unwrap().height, 1);
        assert_eq!(layout.debug.unwrap().y, 49);
    }

    /// A footer that grows takes rows from the bottom of the view; the view's top
    /// stays put, and it never grows past three lines.
    #[test]
    fn the_footer_grows_upward() {
        let area = Rect::new(0, 0, 80, 24);
        let rest = app_layout(area, false, 1, true);
        let grown = app_layout(area, false, 3, true);
        assert_eq!(grown.main_view.y, rest.main_view.y);
        assert_eq!(grown.main_view.height, rest.main_view.height - 2);
        assert_eq!(grown.footer.height, 3);
        assert_eq!(app_layout(area, false, 9, true).footer.height, 3);
    }
}
