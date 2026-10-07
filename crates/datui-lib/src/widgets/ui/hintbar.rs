//! The chip row: keys and their labels, one renderer for the footer and
//! every Surface footer.

use crate::render::footer::{Hint, registry_hint_as, registry_hint_in};
use datui_cli::keys::Context;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// Blank cells after a chip's label, before the next chip.
const GAP: u16 = 2;

/// One key chip: the key on the accent, the label beside it, from the key registry.
#[derive(Debug, Clone)]
struct Chip {
    hint: Hint,
    /// What yields first when the row runs out of room: the lightest chip,
    /// wherever it sits. None weighs chips by position, leftmost heaviest —
    /// plain cut-from-the-right. The way out (Esc) should weigh the most.
    weight: Option<i32>,
}

/// A row of key chips. Primary action first, Esc last; chips that do not fit
/// are dropped whole from the right, never clipped mid-word.
///
/// Every chip is a key of one screen's registry entries ([`Self::screen`]), so a
/// dialog's footer says what its help says.
#[derive(Debug, Clone)]
pub struct HintBar {
    hints: Vec<Chip>,
    screen: Option<(Context, Option<&'static str>)>,
    key_style: Style,
    label_style: Style,
    accent_label_style: Style,
}

impl HintBar {
    /// A bar with explicit styles, for the footer's background-filled row.
    pub fn with_styles(key_style: Style, label_style: Style, accent_label_style: Style) -> Self {
        Self {
            hints: Vec::new(),
            screen: None,
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

    /// The screen whose registry entries the chips that follow are.
    pub fn screen(mut self, context: Context) -> Self {
        self.screen = Some((context, None));
        self
    }

    /// The group of the screen's entries the chips that follow are, where the screen
    /// lists a key twice.
    pub fn group(mut self, group: &'static str) -> Self {
        if let Some((_, in_group)) = self.screen.as_mut() {
            *in_group = Some(group);
        }
        self
    }

    /// A chip for `keys`, with the registry's word for them.
    pub fn key(self, keys: &str) -> Self {
        let (context, group) = self.screen.expect("a bar's screen before its keys");
        self.push(registry_hint_in(context, group, keys))
    }

    /// A chip for `keys` saying `label`, one of the registry's words for them.
    pub fn key_as(self, keys: &str, label: &'static str) -> Self {
        let (context, group) = self.screen.expect("a bar's screen before its keys");
        self.push(registry_hint_as(context, group, keys, label))
    }

    /// A chip built elsewhere from the registry.
    pub fn push(mut self, hint: Hint) -> Self {
        self.hints.push(Chip { hint, weight: None });
        self
    }

    /// The weight of the last chip; see `Chip::weight`.
    pub fn weight(mut self, weight: i32) -> Self {
        if let Some(chip) = self.hints.last_mut() {
            chip.weight = Some(weight);
        }
        self
    }

    /// Accent the label of the chip whose key is `key`.
    pub fn accent(mut self, key: &str) -> Self {
        for chip in &mut self.hints {
            if chip.hint.key == key {
                chip.hint.accented = true;
            }
        }
        self
    }

    /// A chip's cost in columns: the key padded one cell each side, a space,
    /// the label, then [`GAP`] cells before the next chip. Measured in display
    /// columns — a `[glyphs]` override may be wide.
    fn chip_width(chip: &Chip) -> u16 {
        (crate::glyphs::display_width(&chip.hint.key) as u16 + 2)
            + (crate::glyphs::display_width(&chip.hint.label) as u16 + 3)
    }

    /// Which chips a row of `width` shows: chips are dropped whole, lightest
    /// first, until the rest fit. Display order never changes. A bar that ends
    /// its row (`flush`) needs no gap after its last chip.
    fn kept(&self, width: u16, flush: bool) -> Vec<bool> {
        let n = self.hints.len();
        let mut keep = vec![true; n];
        let weight = |i: usize| self.hints[i].weight.unwrap_or((n - i) as i32);
        let budget = if flush {
            width.saturating_add(GAP)
        } else {
            width
        };
        let mut used: u16 = self.hints.iter().map(Self::chip_width).sum();
        while used > budget {
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

    fn used(&self, keep: &[bool]) -> u16 {
        keep.iter()
            .zip(&self.hints)
            .filter(|(keep, _)| **keep)
            .map(|(_, hint)| Self::chip_width(hint))
            .sum()
    }

    /// The columns the bar will actually use in a row of `width`.
    pub fn width_in(&self, width: u16) -> u16 {
        self.used(&self.kept(width, false))
    }

    /// [`Self::width_in`] for a bar drawn with [`Self::render_flush`]: the last
    /// chip's trailing gap is not counted.
    pub fn flush_width_in(&self, width: u16) -> u16 {
        self.used(&self.kept(width, true)).saturating_sub(GAP)
    }

    /// Draw a bar that nothing follows on its row, such as a Surface footer: a
    /// chip fits when its label does, without the gap a next chip would need.
    pub fn render_flush(&self, area: Rect, buf: &mut Buffer) {
        self.draw(area, buf, true);
    }

    /// Where each chip [`Widget::render`] draws in `area` lands, key and label without
    /// the gap after it, with its key: what a click on the bar presses.
    pub fn chips_in(&self, area: Rect) -> Vec<(Rect, &str)> {
        self.chips(area, false)
    }

    /// Where each kept chip lands in `area`, drawn flush or not.
    fn chips(&self, area: Rect, flush: bool) -> Vec<(Rect, &str)> {
        let mut x = area.x;
        let mut chips = Vec::new();
        for (hint, keep) in self.hints.iter().zip(self.kept(area.width, flush)) {
            if !keep {
                continue;
            }
            let width = Self::chip_width(hint);
            let shown = (width - GAP).min(area.right().saturating_sub(x));
            if shown > 0 {
                chips.push((
                    Rect::new(x, area.y, shown, area.height.min(1)),
                    hint.hint.key.as_ref(),
                ));
            }
            x = x.saturating_add(width);
        }
        chips
    }

    fn draw(&self, area: Rect, buf: &mut Buffer, flush: bool) {
        // Every chip that names one key is a click target that presses it, as the
        // status footer's are; drawn after what it sits on, it lies on top.
        for (rect, key) in self.chips(area, flush) {
            if let Some(key) = crate::pointer::chip_key(key) {
                crate::pointer::record(rect, crate::pointer::Hit::Chip(key));
            }
        }
        let kept = self.kept(area.width, flush);
        let mut spans = Vec::new();
        for (chip, keep) in self.hints.iter().zip(kept) {
            if !keep {
                continue;
            }
            spans.push(Span::styled(format!(" {} ", chip.hint.key), self.key_style));
            let style = if chip.hint.accented {
                self.accent_label_style
            } else {
                self.label_style
            };
            spans.push(Span::styled(format!(" {}  ", chip.hint.label), style));
        }
        Paragraph::new(Line::from(spans)).render(area, buf);
    }
}

impl Widget for &HintBar {
    fn render(self, area: Rect, buf: &mut Buffer) {
        self.draw(area, buf, false);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::render::context::RenderContext;

    /// Each chip's place is where its key is drawn, and ends with its label.
    #[test]
    fn chips_are_found_where_they_are_drawn() {
        let bar = HintBar::from_ctx(&RenderContext::for_test())
            .screen(Context::Export)
            .key("Enter")
            .push(crate::render::footer::registry_hint(
                Context::Global,
                "Ctrl+Q",
            ));
        let drawn = render_to_string(&bar, 40);
        let chips = bar.chips_in(Rect::new(0, 0, 40, 1));
        assert_eq!(chips.len(), 2);
        for ((rect, key), label) in chips.iter().zip(["Export", "Quit"]) {
            let text: String = drawn
                .chars()
                .skip(rect.x as usize)
                .take(rect.width as usize)
                .collect();
            assert_eq!(text, format!(" {key}  {label}"));
        }
        // A bar too narrow for the second chip offers only the first.
        assert_eq!(bar.chips_in(Rect::new(0, 0, 18, 1)).len(), 1);
    }

    fn render_to_string(bar: &HintBar, width: u16) -> String {
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        bar.render(area, &mut buf);
        (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect()
    }

    fn bar() -> HintBar {
        HintBar::from_ctx(&RenderContext::for_test())
            .screen(Context::Export)
            .key("Enter")
            .key("Tab")
            .key("Esc")
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
                .screen(Context::Export)
                .key("Enter")
                .weight(2)
                .key("Tab")
                .weight(1)
                .key("Esc")
                .weight(3)
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

    /// A bar that ends its row keeps a chip whose label reaches the edge: the
    /// gap after the last chip is for a next one, and there is none.
    #[test]
    fn a_flush_bar_keeps_a_chip_that_ends_at_the_edge() {
        let full = bar().width_in(u16::MAX);
        let tight = full - 2;
        assert!(!render_to_string(&bar(), tight).contains("Cancel"));
        let area = Rect::new(0, 0, tight, 1);
        let mut buf = Buffer::empty(area);
        bar().render_flush(area, &mut buf);
        let out: String = (0..tight).map(|x| buf[(x, 0)].symbol()).collect();
        assert!(out.ends_with("Cancel"), "{out:?}");
        assert_eq!(bar().flush_width_in(tight), tight);
        // One column less and the chip goes whole, as ever.
        let area = Rect::new(0, 0, tight - 1, 1);
        let mut buf = Buffer::empty(area);
        bar().render_flush(area, &mut buf);
        let out: String = (0..tight - 1).map(|x| buf[(x, 0)].symbol()).collect();
        assert!(!out.contains("Esc") && !out.contains("Cancel"), "{out:?}");
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
