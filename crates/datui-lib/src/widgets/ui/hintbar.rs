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
    #[cfg(test)]
    pub fn width_in(&self, width: u16) -> u16 {
        self.used(&self.kept(width, false))
    }

    /// The columns a bar drawn with [`Self::render_flush`] uses in a row of `width`:
    /// the last chip's trailing gap is not counted.
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
    #[cfg(test)]
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
            if let Some(key) = crate::app::pointer::chip_key(key) {
                crate::app::pointer::record(rect, crate::app::pointer::Hit::Chip(key));
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
mod tests;
