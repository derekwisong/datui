//! The status footer: a thin rule, then one line that says where you are and what
//! is in effect, with the keys of whatever mode is active at the right.
//!
//! The line reads in pipeline order — dataset › query › filters · sort — with the
//! position, the mode's keys and `? keys` at the right. When the line runs out of
//! room the segments yield in a fixed order (see [`Footer::fit`]); a message takes
//! room from the low ones and never adds a line.
//!
//! The footer grows, up to [`MAX_LINES`], only for something ongoing: a prompt being
//! typed (drawn by `input_strip`), or a job with progress ([`ProgressLine`]). It
//! grows upward, taking rows from the bottom of the view above.

use std::borrow::Cow;

use crate::render::context::RenderContext;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::style::{Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Paragraph, Widget};

/// The most lines the footer takes below its rule.
pub const MAX_LINES: u16 = 3;

/// One key of the active mode: `n/N Next`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Hint {
    pub key: Cow<'static, str>,
    pub label: Cow<'static, str>,
    /// The label in the accent: a quiet "look here" (unread notes).
    pub accented: bool,
}

impl Hint {
    pub fn new(key: impl Into<Cow<'static, str>>, label: impl Into<Cow<'static, str>>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            accented: false,
        }
    }

    fn width(&self) -> usize {
        crate::glyphs::display_width(&self.key) + 1 + crate::glyphs::display_width(&self.label)
    }
}

/// A key from the key registry, as a hint: the keys written compactly (`n / N` is
/// `n/N`) beside the entry's label. The registry is the one place a label is
/// spelled, so the footer and the help cannot disagree.
pub fn registry_hint(context: datui_cli::keys::Context, keys: &str) -> Hint {
    registry_hint_in(context, None, keys)
}

/// [`registry_hint`] for the entry in one group of the screen, where the screen
/// lists the keys twice (Find's `Esc`: Cancel in the prompt, Clear at the table).
pub fn registry_hint_in(
    context: datui_cli::keys::Context,
    group: Option<&str>,
    keys: &str,
) -> Hint {
    let entry = datui_cli::keys::lookup(context, group, keys);
    debug_assert!(
        entry.is_some(),
        "{keys:?} is not in the {context:?} registry"
    );
    let label = entry.map_or("", |k| k.label);
    // `^G` in a hint, as in the dialogs' footers; `Ctrl+G` in prose.
    Hint::new(keys.replace(" / ", "/").replace("Ctrl+", "^"), label)
}

/// What the footer says about a followed file.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct FollowMark {
    /// What `t` does on this screen: the footer's hint while following.
    pub key: Option<&'static str>,
    /// `following · 3s ago`, `paused`.
    pub chip: Option<String>,
    /// Rows that came in below the cursor, or that wait to be shown.
    pub note: Option<String>,
    /// In the warning color: rows that did not fit the schema.
    pub warning: Option<String>,
    /// A recording (`--tee`): `rec 12 MB · 1.2 MB/s`, then `saved ...`.
    pub rec: Option<String>,
    /// The recording stopped in an error: `rec` is said in the warning color.
    pub rec_stopped: bool,
}

impl FollowMark {
    /// The facts the footer's status line carries, each with whether it warns.
    pub fn notes(&self) -> Vec<(String, bool)> {
        let mut notes = Vec::new();
        notes.extend(self.chip.clone().map(|c| (c, false)));
        notes.extend(self.note.clone().map(|n| (n, false)));
        notes.extend(self.warning.clone().map(|w| (w, true)));
        notes.extend(self.rec.clone().map(|r| (r, self.rec_stopped)));
        notes
    }
}

/// The row count the position is out of.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Total {
    Known(usize),
    /// Still being counted: a spinner stands in for the number.
    Pending,
    /// The count could not be made.
    Unknown,
}

/// Where the cursor is: `41,208 / 1,204,331`, led by the column when the table is
/// wider than the screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Position {
    /// The cursor's row, counted as the row numbers count.
    pub row: usize,
    pub total: Total,
    pub column: Option<crate::widgets::column_paging::OnScreen>,
}

/// How much of the position a narrow line keeps.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum PositionForm {
    /// `col 3/40 · 41,208 / 1,204,331`
    Full,
    /// `41,208 / 1,204,331`
    Rows,
    /// `41,208 / 1.2M`
    Short,
    Dropped,
}

impl Position {
    fn text(&self, form: PositionForm, spinner: &str) -> Option<(String, String)> {
        let total = |short: bool| match self.total {
            Total::Known(n) if short => crate::discover::format_rows(n),
            Total::Known(n) => crate::numfmt::group_chrome(n),
            Total::Pending => spinner.to_string(),
            Total::Unknown => "?".to_string(),
        };
        let row = crate::numfmt::group_chrome(self.row);
        let dot = crate::glyphs::get().middot;
        match form {
            PositionForm::Full => {
                let lead = self
                    .column
                    .map(|on| format!("{} {dot} ", on.label(true)))
                    .unwrap_or_default();
                Some((format!("{lead}{row}"), format!(" / {}", total(false))))
            }
            PositionForm::Rows => Some((row, format!(" / {}", total(false)))),
            PositionForm::Short => Some((row, format!(" / {}", total(true)))),
            PositionForm::Dropped => None,
        }
    }
}

/// Filters and sort: in full (`prcp > 0 · date ▼`), or counted (`2 filters ·
/// sorted`) when the line is short.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ViewState {
    pub filters: Vec<String>,
    pub sort: Option<String>,
}

impl ViewState {
    fn full(&self) -> Vec<String> {
        let mut parts = self.filters.clone();
        parts.extend(self.sort.clone());
        parts
    }

    fn counted(&self) -> Vec<String> {
        let mut parts = Vec::new();
        match self.filters.len() {
            0 => {}
            1 => parts.push("1 filter".to_string()),
            n => parts.push(format!("{n} filters")),
        }
        if self.sort.is_some() {
            parts.push("sorted".to_string());
        }
        parts
    }
}

/// A job's progress: what it has done of how much, a bar when the total is known,
/// and whether Esc stops it. `rows 412,880,117   files 18,402 / 126,033  ███▍  14%`.
#[derive(Debug, Clone, PartialEq)]
pub struct ProgressLine {
    /// What is counted, in order: a noun, how many are done, and of how many.
    pub counts: Vec<ProgressCount>,
    /// Esc stops the job: the line says so.
    pub stoppable: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProgressCount {
    pub noun: &'static str,
    pub done: u64,
    pub total: Option<u64>,
}

impl ProgressLine {
    /// The share done, from the last count with a total.
    pub fn fraction(&self) -> Option<f64> {
        self.counts
            .iter()
            .rev()
            .find_map(|c| c.total.filter(|t| *t > 0).map(|t| c.done as f64 / t as f64))
            .map(|f| f.clamp(0.0, 1.0))
    }
}

/// Where a segment the user can click landed, and what it names: a hint's key.
pub type Drawn = Vec<(Rect, String)>;

/// Everything the status line says.
#[derive(Debug, Clone, Default)]
pub struct Footer {
    /// The dataset's name: `weather/daily`.
    pub dataset: Option<String>,
    /// What stands between the data and the filters: `query`, `pivoted`, `not the
    /// Delta table`, `3 formats match`. The first is drawn in the accent.
    pub stages: Vec<String>,
    pub view: ViewState,
    /// Background work: `Reading footers: 3 of 40...`. Shrinks to its spinner.
    pub work: Option<String>,
    /// Facts beside the work: a follow's state, a recording. Each is dropped
    /// whole; the bool draws it in the warning color.
    pub notes: Vec<(String, bool)>,
    /// A completion flash.
    pub message: Option<String>,
    pub position: Option<Position>,
    /// The active mode's keys.
    pub hints: Vec<Hint>,
    /// The key that opens help: `?`, or `F1` where `?` types.
    pub help: Option<&'static str>,
    /// The spinner frame, for pending counts and the shrunk work segment.
    pub spinner: &'static str,
}

/// What the line keeps at a width; see [`Footer::fit`].
#[derive(Debug, Clone, PartialEq, Eq)]
struct Fit {
    /// `None` dropped; `Some(w)` drawn in `w` columns, middle-elided when short.
    dataset: Option<usize>,
    stages: bool,
    view: ViewForm,
    work: WorkForm,
    notes: usize,
    message: Option<usize>,
    position: PositionForm,
    help_long: bool,
    help: bool,
    hints: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ViewForm {
    Full,
    Counted,
    Dropped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkForm {
    Full,
    Spinner,
    Dropped,
}

/// Columns between pipeline steps: ` › `.
const STEP: usize = 3;
/// Columns between the left and the right of the line, at least.
const GUTTER: usize = 2;
/// The fewest columns a middle-elided dataset name is drawn in.
const DATASET_MIN: usize = 8;
/// The fewest columns a cut message is drawn in.
const MESSAGE_MIN: usize = 8;

impl Footer {
    fn help_label(long: bool) -> &'static str {
        if long { " keys" } else { "" }
    }

    fn left_parts(&self, fit: &Fit) -> Vec<usize> {
        let mut widths = Vec::new();
        if let Some(w) = fit.dataset {
            widths.push(w);
        }
        if fit.stages {
            widths.extend(self.stages.iter().map(|s| crate::glyphs::display_width(s)));
        }
        let view = match fit.view {
            ViewForm::Full => self.view.full(),
            ViewForm::Counted => self.view.counted(),
            ViewForm::Dropped => Vec::new(),
        };
        if !view.is_empty() {
            // One step, its parts joined by ` · `.
            let joined: usize = view.iter().map(|p| crate::glyphs::display_width(p)).sum();
            widths.push(joined + STEP * (view.len() - 1));
        }
        match (fit.work, &self.work) {
            (WorkForm::Full, Some(work)) if !work.is_empty() => widths.push(
                crate::glyphs::display_width(self.spinner) + 1 + crate::glyphs::display_width(work),
            ),
            (WorkForm::Full | WorkForm::Spinner, Some(_)) => {
                widths.push(crate::glyphs::display_width(self.spinner))
            }
            _ => {}
        }
        widths.extend(
            self.notes
                .iter()
                .take(fit.notes)
                .map(|(n, _)| crate::glyphs::display_width(n)),
        );
        widths
    }

    fn left_width(&self, fit: &Fit) -> usize {
        let parts = self.left_parts(fit);
        let steps: usize = parts.iter().sum::<usize>() + STEP * parts.len().saturating_sub(1);
        let message = fit
            .message
            .map_or(0, |w| w + if steps > 0 { STEP } else { 0 });
        // A leading space.
        1 + steps + message
    }

    fn right_width(&self, fit: &Fit) -> usize {
        let mut parts = Vec::new();
        if let Some(position) = &self.position
            && let Some((a, b)) = position.text(fit.position, self.spinner)
        {
            parts.push(crate::glyphs::display_width(&a) + crate::glyphs::display_width(&b));
        }
        parts.extend(self.hints.iter().take(fit.hints).map(Hint::width));
        if fit.help
            && let Some(key) = self.help
        {
            parts.push(
                crate::glyphs::display_width(key)
                    + crate::glyphs::display_width(Self::help_label(fit.help_long)),
            );
        }
        // Two spaces between each, and one at the end.
        parts.iter().sum::<usize>() + 2 * parts.len().saturating_sub(1) + 1
    }

    fn width(&self, fit: &Fit) -> usize {
        self.left_width(fit) + GUTTER + self.right_width(fit)
    }

    fn message_width(&self) -> Option<usize> {
        self.message
            .as_ref()
            .map(|m| crate::glyphs::display_width(m))
    }

    /// What the line keeps at `width` columns. Highest priority first, what yields
    /// last: the mode's keys and help (`? keys` shortens to `?`); a message; the
    /// position (`41,208 / 1.2M`); the query and other stages; background work
    /// (shrinks to its spinner); filters and sort (counted, `2 filters · sorted`);
    /// the dataset's name (middle-elided, and the first to go for a message).
    fn fit(&self, width: usize) -> Fit {
        let dataset_full = self
            .dataset
            .as_ref()
            .map(|d| crate::glyphs::display_width(d));
        let mut fit = Fit {
            dataset: dataset_full,
            stages: true,
            view: ViewForm::Full,
            work: WorkForm::Full,
            notes: self.notes.len(),
            message: self.message_width(),
            position: PositionForm::Full,
            help_long: true,
            help: true,
            hints: self.hints.len(),
        };
        if self.width(&fit) <= width {
            return fit;
        }
        // A message takes the dataset's room before anything else's.
        if fit.message.is_some() {
            fit.dataset = None;
        }
        let steps: [fn(&Footer, &mut Fit, usize) -> bool; 15] = [
            // The name, middle-elided into whatever room is left.
            |footer, f, width| {
                let Some(full) = f.dataset else { return false };
                let without = Fit {
                    dataset: Some(0),
                    ..f.clone()
                };
                let room = width.saturating_sub(footer.width(&without));
                if room >= DATASET_MIN.min(full) {
                    f.dataset = Some(room.min(full));
                    return true;
                }
                false
            },
            |_, f, _| {
                f.view = match f.view {
                    ViewForm::Full => ViewForm::Counted,
                    other => other,
                };
                true
            },
            |_, f, _| {
                f.dataset = None;
                true
            },
            // The column leaves the position before the position yields anything.
            |_, f, _| {
                f.position = f.position.max(PositionForm::Rows);
                true
            },
            |_, f, _| {
                f.notes = 0;
                true
            },
            |_, f, _| {
                if f.work == WorkForm::Full {
                    f.work = WorkForm::Spinner;
                }
                true
            },
            |_, f, _| {
                f.view = ViewForm::Dropped;
                true
            },
            |_, f, _| {
                f.stages = false;
                true
            },
            |_, f, _| {
                f.position = f.position.max(PositionForm::Short);
                true
            },
            |_, f, _| {
                f.work = WorkForm::Dropped;
                true
            },
            |_, f, _| {
                f.position = PositionForm::Dropped;
                true
            },
            // The message is cut, not dropped, while it can still say something.
            |footer, f, width| {
                let Some(full) = f.message else { return false };
                let without = Fit {
                    message: Some(0),
                    ..f.clone()
                };
                let room = width.saturating_sub(footer.width(&without));
                if room >= MESSAGE_MIN.min(full) {
                    f.message = Some(room.min(full));
                    return true;
                }
                false
            },
            |_, f, _| {
                f.help_long = false;
                true
            },
            |_, f, _| {
                f.message = None;
                true
            },
            // Last, the mode's keys from the right; help stays, and the first key goes
            // only where not even it fits beside help.
            |footer, f, width| {
                while f.hints > 0 && footer.width(f) > width {
                    f.hints -= 1;
                }
                true
            },
        ];
        for step in steps {
            step(self, &mut fit, width);
            if self.width(&fit) <= width {
                break;
            }
        }
        fit
    }

    /// Draw the status line into `area` (one row). Returns where each hint landed,
    /// for a click.
    pub fn render_line(&self, area: Rect, buf: &mut Buffer, ctx: &RenderContext) -> Drawn {
        if area.width == 0 || area.height == 0 {
            return Vec::new();
        }
        let fit = self.fit(area.width as usize);
        let g = crate::glyphs::get();
        let dim = Style::default().fg(ctx.dimmed);
        let label = Style::default().fg(ctx.keybind_labels);
        let secondary = Style::default().fg(ctx.text_secondary);
        let accent = Style::default()
            .fg(ctx.keybind_hints)
            .add_modifier(Modifier::BOLD);
        let step = Span::styled(format!(" {} ", g.trail), dim);
        let dot = Span::styled(format!(" {} ", g.middot), dim);

        // Left: the pipeline.
        let mut steps: Vec<Vec<Span>> = Vec::new();
        if let (Some(w), Some(name)) = (fit.dataset, &self.dataset) {
            steps.push(vec![Span::styled(elide_middle(name, w), label)]);
        }
        if fit.stages {
            for (i, stage) in self.stages.iter().enumerate() {
                let style = if i == 0 && stage == QUERY_STAGE {
                    accent
                } else {
                    label
                };
                steps.push(vec![Span::styled(stage.clone(), style)]);
            }
        }
        let view = match fit.view {
            ViewForm::Full => self.view.full(),
            ViewForm::Counted => self.view.counted(),
            ViewForm::Dropped => Vec::new(),
        };
        if !view.is_empty() {
            let filters = match fit.view {
                ViewForm::Full => self.view.filters.len(),
                _ => usize::from(!self.view.filters.is_empty()),
            };
            let mut spans = Vec::new();
            for (i, part) in view.into_iter().enumerate() {
                if i > 0 {
                    spans.push(dot.clone());
                }
                let style = if i < filters {
                    Style::default().fg(ctx.warning)
                } else {
                    label
                };
                spans.push(Span::styled(part, style));
            }
            steps.push(spans);
        }
        let throbber = Style::default().fg(ctx.throbber);
        match (fit.work, &self.work) {
            (WorkForm::Full, Some(work)) if !work.is_empty() => steps.push(vec![
                Span::styled(self.spinner, throbber),
                Span::raw(" "),
                Span::styled(work.clone(), secondary),
            ]),
            (WorkForm::Full | WorkForm::Spinner, Some(_)) => {
                steps.push(vec![Span::styled(self.spinner, throbber)])
            }
            _ => {}
        }
        for (note, warning) in self.notes.iter().take(fit.notes) {
            let style = if *warning {
                Style::default().fg(ctx.warning)
            } else {
                secondary
            };
            steps.push(vec![Span::styled(note.clone(), style)]);
        }
        let mut left = vec![Span::raw(" ")];
        let any_step = !steps.is_empty();
        for (i, spans) in steps.into_iter().enumerate() {
            if i > 0 {
                left.push(step.clone());
            }
            left.extend(spans);
        }
        if let (Some(w), Some(message)) = (fit.message, &self.message) {
            if any_step {
                left.push(Span::raw("   "));
            }
            left.push(Span::styled(
                crate::glyphs::fit_cells(message, w, "...").into_owned(),
                Style::default().fg(ctx.text_primary),
            ));
        }
        Paragraph::new(Line::from(left)).render(area, buf);

        // Right: position, the mode's keys, help.
        let mut right: Vec<Span> = Vec::new();
        let mut keys: Vec<(usize, usize, String)> = Vec::new();
        let gap = |right: &mut Vec<Span>| {
            if !right.is_empty() {
                right.push(Span::raw("  "));
            }
        };
        if let Some(position) = &self.position
            && let Some((row, total)) = position.text(fit.position, self.spinner)
        {
            right.push(Span::styled(row, label));
            right.push(Span::styled(total, secondary));
        }
        let width_of = |spans: &[Span]| spans.iter().map(|s| s.width()).sum::<usize>();
        for hint in self.hints.iter().take(fit.hints) {
            gap(&mut right);
            let at = width_of(&right);
            right.push(Span::styled(hint.key.to_string(), accent));
            right.push(Span::raw(" "));
            let style = if hint.accented {
                Style::default().fg(ctx.keybind_hints)
            } else {
                label
            };
            right.push(Span::styled(hint.label.to_string(), style));
            // A blank slot holds its place and is nothing to click.
            if !hint.key.trim().is_empty() {
                keys.push((at, hint.width(), hint.key.to_string()));
            }
        }
        if fit.help
            && let Some(key) = self.help
        {
            gap(&mut right);
            let at = width_of(&right);
            right.push(Span::styled(key, accent));
            right.push(Span::styled(Self::help_label(fit.help_long), label));
            keys.push((
                at,
                crate::glyphs::display_width(key)
                    + crate::glyphs::display_width(Self::help_label(fit.help_long)),
                key.to_string(),
            ));
        }
        right.push(Span::raw(" "));
        let used = width_of(&right).min(area.width as usize) as u16;
        let x = area.right().saturating_sub(used);
        Paragraph::new(Line::from(right).right_aligned()).render(
            Rect {
                x,
                width: used,
                ..area
            },
            buf,
        );
        keys.into_iter()
            .filter_map(|(at, w, key)| {
                let x = x.checked_add(at as u16)?;
                (x < area.right()).then(|| {
                    (
                        Rect::new(x, area.y, (w as u16).min(area.right() - x), 1),
                        key,
                    )
                })
            })
            .collect()
    }
}

/// The stage a query in effect shows as.
pub const QUERY_STAGE: &str = "query";

/// `text` in `width` columns, cut from the middle: `weather/…/daily`.
pub fn elide_middle(text: &str, width: usize) -> String {
    let full = crate::glyphs::display_width(text);
    if full <= width {
        return text.to_string();
    }
    let mark = crate::glyphs::get().ellipsis;
    let mark_w = crate::glyphs::display_width(mark);
    if width <= mark_w {
        return crate::glyphs::take_columns(mark, width).to_string();
    }
    let room = width - mark_w;
    let head = crate::glyphs::take_columns(text, room.div_ceil(2));
    let tail = crate::glyphs::take_columns_end(text, room - crate::glyphs::display_width(head));
    format!("{head}{mark}{tail}")
}

/// Draw the rule above the footer: the table's column separator color, no fill.
pub fn render_rule(area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let g = crate::glyphs::get();
    Paragraph::new(g.rule_h.repeat(area.width as usize))
        .style(Style::default().fg(ctx.column_separator))
        .render(Rect { height: 1, ..area }, buf);
}

/// Draw a progress line: the counts, a bar when a total is known, and `Esc Stop`.
pub fn render_progress(line: &ProgressLine, area: Rect, buf: &mut Buffer, ctx: &RenderContext) {
    if area.width == 0 || area.height == 0 {
        return;
    }
    let dim = Style::default().fg(ctx.dimmed);
    let label = Style::default().fg(ctx.keybind_labels);
    let secondary = Style::default().fg(ctx.text_secondary);
    let accent = Style::default()
        .fg(ctx.keybind_hints)
        .add_modifier(Modifier::BOLD);
    let mut spans = vec![Span::raw(" ")];
    for (i, count) in line.counts.iter().enumerate() {
        if i > 0 {
            spans.push(Span::raw("   "));
        }
        spans.push(Span::styled(format!("{} ", count.noun), dim));
        spans.push(Span::styled(
            crate::numfmt::group_chrome(count.done as usize),
            label,
        ));
        if let Some(total) = count.total {
            spans.push(Span::styled(
                format!(" / {}", crate::numfmt::group_chrome(total as usize)),
                secondary,
            ));
        }
    }
    let stop = line.stoppable.then_some(("Esc", "Stop"));
    let stop_width = stop.map_or(0, |(k, l)| k.len() + 1 + l.len() + 1);
    let used: usize = spans.iter().map(|s| s.width()).sum();
    if let Some(fraction) = line.fraction() {
        // A bar in what is left, at most twenty cells, then the percentage.
        let percent = format!(" {:>3.0}%", fraction * 100.0);
        let room = (area.width as usize)
            .saturating_sub(used + 3 + percent.len() + stop_width + 2)
            .min(20);
        if room >= 4 {
            spans.push(Span::raw("   "));
            spans.push(Span::styled(bar(fraction, room), accent));
            spans.push(Span::styled(percent, secondary));
        }
    }
    Paragraph::new(Line::from(spans)).render(area, buf);
    if let Some((key, text)) = stop {
        let line = Line::from(vec![
            Span::styled(key, accent),
            Span::raw(" "),
            Span::styled(text, label),
            Span::raw(" "),
        ]);
        let w = (line.width() as u16).min(area.width);
        Paragraph::new(line).render(
            Rect {
                x: area.right() - w,
                width: w,
                ..area
            },
            buf,
        );
    }
}

/// A bar `width` cells wide, `fraction` of it filled in eighths, the rest dots.
fn bar(fraction: f64, width: usize) -> String {
    let g = crate::glyphs::get();
    let eighths = (fraction * width as f64 * 8.0).round() as usize;
    let full = eighths / 8;
    let part = eighths % 8;
    let mut out = g.bar_eighths[7].repeat(full.min(width));
    if full < width {
        if part > 0 {
            out.push_str(g.bar_eighths[part - 1]);
        } else {
            out.push_str(g.unsampled);
        }
        out.push_str(&g.unsampled.repeat(width - full - 1));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(footer: &Footer, width: u16) -> String {
        let area = Rect::new(0, 0, width, 1);
        let mut buf = Buffer::empty(area);
        footer.render_line(area, &mut buf, &RenderContext::for_test());
        (0..width)
            .map(|x| buf[(x, 0)].symbol().to_string())
            .collect::<String>()
            .trim_end()
            .to_string()
    }

    fn busy_footer() -> Footer {
        Footer {
            dataset: Some("weather/daily.parquet".to_string()),
            stages: vec![QUERY_STAGE.to_string()],
            view: ViewState {
                filters: vec!["prcp > 0".to_string()],
                sort: Some("date ▼".to_string()),
            },
            work: Some("Reading footers: 3 of 40...".to_string()),
            notes: Vec::new(),
            message: None,
            position: Some(Position {
                row: 41_208,
                total: Total::Known(1_204_331),
                column: None,
            }),
            hints: vec![Hint::new("n/N", "Next"), Hint::new("Esc", "Clear")],
            help: Some("?"),
            spinner: "|",
        }
    }

    #[test]
    fn at_rest_only_help_shows_at_the_right() {
        let footer = Footer {
            help: Some("?"),
            spinner: "|",
            ..Footer::default()
        };
        assert_eq!(line(&footer, 40).trim(), "? keys");
    }

    #[test]
    fn the_line_reads_in_pipeline_order() {
        let text = line(&busy_footer(), 160);
        let at = |s: &str| text.find(s).unwrap_or_else(|| panic!("{s:?} in {text:?}"));
        assert!(at("weather/daily") < at("query"));
        assert!(at("query") < at("prcp > 0"));
        assert!(at("prcp > 0") < at("date"));
        assert!(at("date") < at("41,208 / 1,204,331"));
        assert!(at("41,208") < at("n/N Next"));
        assert!(text.ends_with("? keys"), "{text:?}");
    }

    /// Each width keeps the segments the priority order says it keeps.
    #[test]
    fn segments_yield_in_priority_order() {
        let footer = busy_footer();
        let wide = line(&footer, 160);
        assert!(
            wide.contains("weather/daily") && wide.contains("prcp > 0"),
            "{wide}"
        );

        let w120 = line(&footer, 120);
        assert!(
            w120.contains("prcp > 0") && w120.contains("41,208 / 1,204,331"),
            "the name is cut first: {w120}"
        );
        assert!(!w120.contains("weather/daily.parquet"), "{w120}");

        let w80 = line(&footer, 80);
        assert!(w80.contains("n/N Next") && w80.contains("? keys"), "{w80}");
        assert!(w80.contains("41,208"), "{w80}");
        assert!(w80.contains("query"), "{w80}");

        let w60 = line(&footer, 60);
        assert!(
            w60.contains("n/N Next") && w60.contains("Esc Clear"),
            "{w60}"
        );
        assert!(!w60.contains("weather"), "the name went first: {w60}");

        let w40 = line(&footer, 40);
        assert!(w40.contains("n/N Next"), "{w40}");
        assert!(w40.contains('?'), "{w40}");
        assert!(!w40.contains("prcp"), "{w40}");
        for text in [&wide, &w80, &w60, &w40] {
            assert!(!text.contains("Clea "), "nothing is cut mid-word: {text}");
        }
    }

    #[test]
    fn a_message_takes_the_dataset_s_room_first() {
        let mut footer = busy_footer();
        footer.message = Some("Copied 3 rows".to_string());
        let text = line(&footer, 100);
        assert!(text.contains("Copied 3 rows"), "{text}");
        assert!(!text.contains("weather"), "{text}");
        assert!(text.contains("? keys"), "{text}");
    }

    #[test]
    fn filters_shrink_to_counts_before_they_go() {
        let mut footer = busy_footer();
        footer.dataset = None;
        footer.work = None;
        footer.view.filters = vec![
            "temperature > 30".to_string(),
            "station = \"USW00094728\"".to_string(),
        ];
        let text = line(&footer, 80);
        assert!(text.contains("2 filters"), "{text}");
        assert!(text.contains("sorted"), "{text}");
    }

    #[test]
    fn a_long_name_is_cut_in_the_middle() {
        assert_eq!(elide_middle("abcdefghij", 10), "abcdefghij");
        let cut = elide_middle("weather/stations/daily", 12);
        assert_eq!(crate::glyphs::display_width(&cut), 12);
        assert!(cut.starts_with("weath") && cut.ends_with("daily"), "{cut}");
    }

    #[test]
    fn the_position_shortens_before_it_goes() {
        let footer = Footer {
            position: Some(Position {
                row: 41_208,
                total: Total::Known(1_204_331),
                column: None,
            }),
            hints: vec![Hint::new("+/-", "Filter"), Hint::new("F", "Counts")],
            help: Some("?"),
            spinner: "|",
            ..Footer::default()
        };
        let text = line(&footer, 48);
        assert!(text.contains("41,208 / 1.2M"), "{text}");
    }

    /// With no fill, the footer's colors sit on the terminal's background: in the dark
    /// and the light palette, and once degraded to 256 colors, each part keeps a color
    /// of its own, apart from the background.
    #[test]
    fn the_footer_reads_on_both_palettes_and_at_256_colors() {
        use crate::config::{ColorConfig, rgb_to_256_color};
        let rgb = |hex: &str| -> (u8, u8, u8) {
            let v = u32::from_str_radix(hex.trim_start_matches('#'), 16).expect(hex);
            ((v >> 16) as u8, (v >> 8) as u8, v as u8)
        };
        for (mode, colors) in [
            ("dark", ColorConfig::dark()),
            ("light", ColorConfig::light()),
        ] {
            // The background is the terminal's own; the text on a key chip matches it.
            let background = rgb(&colors.text_inverse);
            let parts = [
                ("rule", &colors.table_column_separator),
                ("keys", &colors.chip_key),
                ("labels", &colors.chip_label),
                ("secondary", &colors.text_secondary),
                ("separators", &colors.dimmed),
                ("filters", &colors.warning),
            ];
            for (part, hex) in parts {
                let c = rgb(hex);
                let distance = (c.0 as i32 - background.0 as i32).abs()
                    + (c.1 as i32 - background.1 as i32).abs()
                    + (c.2 as i32 - background.2 as i32).abs();
                assert!(
                    distance >= 60,
                    "{mode}: the {part} {hex} on {}",
                    colors.text_inverse
                );
                assert_ne!(
                    rgb_to_256_color(c.0, c.1, c.2),
                    rgb_to_256_color(background.0, background.1, background.2),
                    "{mode} at 256 colors: the {part} {hex} becomes the background"
                );
            }
            assert_ne!(
                rgb_to_256_color(
                    rgb(&colors.chip_key).0,
                    rgb(&colors.chip_key).1,
                    rgb(&colors.chip_key).2
                ),
                rgb_to_256_color(
                    rgb(&colors.chip_label).0,
                    rgb(&colors.chip_label).1,
                    rgb(&colors.chip_label).2
                ),
                "{mode} at 256 colors: a key and its label stay apart"
            );
        }
    }

    #[test]
    fn a_progress_line_counts_draws_a_bar_and_offers_stop() {
        let progress = ProgressLine {
            counts: vec![
                ProgressCount {
                    noun: "rows",
                    done: 412_880_117,
                    total: None,
                },
                ProgressCount {
                    noun: "files",
                    done: 18_402,
                    total: Some(126_033),
                },
            ],
            stoppable: true,
        };
        let area = Rect::new(0, 0, 100, 1);
        let mut buf = Buffer::empty(area);
        render_progress(&progress, area, &mut buf, &RenderContext::for_test());
        let text: String = (0..100).map(|x| buf[(x, 0)].symbol().to_string()).collect();
        assert!(text.contains("rows 412,880,117"), "{text}");
        assert!(text.contains("files 18,402 / 126,033"), "{text}");
        assert!(text.contains("15%"), "{text}");
        assert!(text.trim_end().ends_with("Esc Stop"), "{text}");
    }
}
