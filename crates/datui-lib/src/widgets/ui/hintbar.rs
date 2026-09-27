//! The chip row: keys and their labels, one renderer for the control bar and
//! every Surface footer.

use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// One key chip: the key on the accent, the label beside it.
#[derive(Debug, Clone, Copy)]
pub struct Hint<'a> {
    pub key: &'a str,
    pub label: &'a str,
    /// Give this label the accent — a quiet "look here", never extra text.
    pub accented: bool,
    /// What yields first when the row runs out of room: the lightest chip,
    /// wherever it sits. None weighs chips by position, leftmost heaviest —
    /// plain cut-from-the-right. The way out (Esc) should weigh the most.
    pub weight: Option<i32>,
}

/// A row of key chips. Primary action first, Esc last; chips that do not fit
/// are dropped whole from the right, never clipped mid-word.
#[derive(Debug, Clone)]
pub struct HintBar<'a> {
    hints: Vec<Hint<'a>>,
    key_style: Style,
    label_style: Style,
    accent_label_style: Style,
}

impl<'a> HintBar<'a> {
    /// A bar with explicit styles, for the control bar's background-filled row.
    pub fn with_styles(key_style: Style, label_style: Style, accent_label_style: Style) -> Self {
        Self {
            hints: Vec::new(),
            key_style,
            label_style,
            accent_label_style,
        }
    }

    /// A bar styled from the theme snapshot, for Surface footers.
    pub fn from_ctx(ctx: &crate::render::context::RenderContext) -> Self {
        Self::with_styles(
            Style::default()
                .bg(ctx.keybind_hints)
                .fg(ctx.text_inverse)
                .add_modifier(Modifier::BOLD),
            Style::default().fg(ctx.keybind_labels),
            Style::default().fg(ctx.keybind_hints),
        )
    }

    pub fn hint(mut self, key: &'a str, label: &'a str) -> Self {
        self.hints.push(Hint {
            key,
            label,
            accented: false,
            weight: None,
        });
        self
    }

    /// A chip with an explicit weight; see [`Hint::weight`].
    pub fn hint_weighted(mut self, key: &'a str, label: &'a str, weight: i32) -> Self {
        self.hints.push(Hint {
            key,
            label,
            accented: false,
            weight: Some(weight),
        });
        self
    }

    pub fn hints(mut self, pairs: &[(&'a str, &'a str)]) -> Self {
        for (key, label) in pairs {
            self.hints.push(Hint {
                key,
                label,
                accented: false,
                weight: None,
            });
        }
        self
    }

    /// Accent the label of the chip whose key is `key`.
    pub fn accent(mut self, key: &str) -> Self {
        for hint in &mut self.hints {
            if hint.key == key {
                hint.accented = true;
            }
        }
        self
    }

    /// A chip's cost in columns: the key padded one cell each side, a space,
    /// the label, then two cells before the next chip.
    fn chip_width(hint: &Hint) -> u16 {
        (hint.key.chars().count() as u16 + 2) + (hint.label.chars().count() as u16 + 3)
    }

    /// Which chips a row of `width` shows: chips are dropped whole, lightest
    /// first, until the rest fit. Display order never changes.
    fn kept(&self, width: u16) -> Vec<bool> {
        let n = self.hints.len();
        let mut keep = vec![true; n];
        let weight = |i: usize| self.hints[i].weight.unwrap_or((n - i) as i32);
        let mut used: u16 = self.hints.iter().map(Self::chip_width).sum();
        while used > width {
            // Lightest chip goes; on a tie, the rightmost.
            let Some(drop) = (0..n)
                .filter(|&i| keep[i])
                .min_by_key(|&i| (weight(i), std::cmp::Reverse(i)))
            else {
                break;
            };
            keep[drop] = false;
            used -= Self::chip_width(&self.hints[drop]);
        }
        keep
    }

    /// The columns the bar will actually use in a row of `width`.
    pub fn width_in(&self, width: u16) -> u16 {
        self.kept(width)
            .iter()
            .zip(&self.hints)
            .filter(|(keep, _)| **keep)
            .map(|(_, hint)| Self::chip_width(hint))
            .sum()
    }
}

impl Widget for &HintBar<'_> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        let kept = self.kept(area.width);
        let mut spans = Vec::new();
        for (hint, keep) in self.hints.iter().zip(kept) {
            if !keep {
                continue;
            }
            spans.push(Span::styled(format!(" {} ", hint.key), self.key_style));
            let style = if hint.accented {
                self.accent_label_style
            } else {
                self.label_style
            };
            spans.push(Span::styled(format!(" {}  ", hint.label), style));
        }
        Paragraph::new(Line::from(spans)).render(area, buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::context::RenderContext;

    fn render_to_string(bar: &HintBar, width: u16) -> String {
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        bar.render(area, &mut buf);
        (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect()
    }

    fn bar<'a>() -> HintBar<'a> {
        HintBar::from_ctx(&RenderContext::for_test())
            .hint("Enter", "Export")
            .hint("Tab", "Next")
            .hint("Esc", "Cancel")
    }

    #[test]
    fn chips_read_key_then_label_in_order() {
        let out = render_to_string(&bar(), 60);
        let positions: Vec<usize> = ["Enter", "Export", "Tab", "Next", "Esc", "Cancel"]
            .iter()
            .map(|word| out.find(word).unwrap_or_else(|| panic!("{word} missing")))
            .collect();
        assert!(
            positions.windows(2).all(|pair| pair[0] < pair[1]),
            "chips out of order: {out:?}"
        );
    }

    /// A chip that does not fit is dropped whole; the ones before it stay whole.
    #[test]
    fn a_tight_bar_drops_whole_chips_from_the_right() {
        let full = bar().width_in(u16::MAX);
        for width in 1..full {
            let out = render_to_string(&bar(), width);
            for (key, label) in [("Enter", "Export"), ("Tab", "Next"), ("Esc", "Cancel")] {
                // Either the whole chip is there or none of it.
                assert_eq!(
                    out.contains(key),
                    out.contains(label),
                    "chip {key}/{label} was clipped at width {width}: {out:?}"
                );
            }
            // Unweighted, the bar cuts from the right: a later chip on screen
            // means every earlier one is too.
            if out.contains("Cancel") {
                assert!(out.contains("Next") && out.contains("Export"), "{out:?}");
            }
            if out.contains("Next") {
                assert!(out.contains("Export"), "{out:?}");
            }
        }
        let out = render_to_string(&bar(), full);
        assert!(out.contains("Cancel"), "everything fits at {full}: {out:?}");
    }

    /// Weighted, the way out yields last: a tight footer keeps Enter and Esc
    /// and gives up Tab, whatever the order they are drawn in.
    #[test]
    fn the_escape_chip_outlives_lighter_chips() {
        let weighted = || {
            HintBar::from_ctx(&RenderContext::for_test())
                .hint_weighted("Enter", "Export", 2)
                .hint_weighted("Tab", "Next", 1)
                .hint_weighted("Esc", "Cancel", 3)
        };
        let full = weighted().width_in(u16::MAX);
        let out = render_to_string(&weighted(), full - 1);
        assert!(
            out.contains("Export") && out.contains("Cancel") && !out.contains("Next"),
            "Tab is the chip that yields: {out:?}"
        );
        // Tighter still, the primary action goes before the way out.
        let narrow = render_to_string(&weighted(), 16);
        assert!(
            narrow.contains("Cancel") && !narrow.contains("Export"),
            "Esc goes last: {narrow:?}"
        );
    }

    /// The key sits on the accent; the label does not.
    #[test]
    fn the_key_carries_the_chip_background_and_the_label_does_not() {
        let area = Rect::new(0, 0, 40, 1);
        let mut buf = Buffer::empty(area);
        let bar = bar();
        bar.render(area, &mut buf);
        let out = render_to_string(&bar, 40);
        let key_x = out.find("Enter").unwrap() as u16;
        let label_x = out.find("Export").unwrap() as u16;
        assert_ne!(
            buf[(key_x, 0)].bg,
            buf[(label_x, 0)].bg,
            "key and label share a background, so there is no chip"
        );
    }

    #[test]
    fn accent_marks_one_label_and_changes_no_text() {
        let plain = render_to_string(&bar(), 60);
        let accented_bar = bar().accent("Tab");
        assert_eq!(plain, render_to_string(&accented_bar, 60));

        let area = Rect::new(0, 0, 60, 1);
        let mut plain_buf = Buffer::empty(area);
        bar().render(area, &mut plain_buf);
        let mut accent_buf = Buffer::empty(area);
        accented_bar.render(area, &mut accent_buf);
        let changed: Vec<u16> = (0..60)
            .filter(|&x| plain_buf[(x, 0)].fg != accent_buf[(x, 0)].fg)
            .collect();
        assert!(!changed.is_empty(), "the accent did nothing");
        let label_at = plain.find("Next").unwrap() as u16;
        let chunk = (label_at - 1)..(label_at + "Next".len() as u16 + 2);
        for x in &changed {
            assert!(chunk.contains(x), "column {x} is outside the Next label");
        }
    }
}
