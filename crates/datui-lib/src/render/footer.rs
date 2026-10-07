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
use datui_cli::keys::{Context, Key};
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
    fn new(key: impl Into<Cow<'static, str>>, label: impl Into<Cow<'static, str>>) -> Self {
        Self {
            key: key.into(),
            label: label.into(),
            accented: false,
        }
    }

    /// The same key, its label padded to `width` columns: a slot whose neighbors do
    /// not move as its word changes.
    pub fn padded(mut self, width: usize) -> Self {
        self.label = format!("{:<width$}", self.label).into();
        self
    }

    /// The slot with nothing in it: blanks as wide as the key, and `width` for the
    /// label, so what follows does not move.
    pub fn blank(self, width: usize) -> Self {
        Self::new(
            " ".repeat(crate::glyphs::display_width(&self.key)),
            " ".repeat(width),
        )
    }

    /// The same key with its label in the accent.
    pub fn accented(mut self) -> Self {
        self.accented = true;
        self
    }

    fn width(&self) -> usize {
        crate::glyphs::display_width(&self.key) + 1 + crate::glyphs::display_width(&self.label)
    }
}

/// A key from the key registry, as a hint: the keys as the hint names them, written
/// compactly, beside the entry's label. The registry is the one place a label is
/// spelled, so the footers and the help cannot disagree: there is no other way to
/// build a hint.
///
/// `keys` names the entry as written (`n / N`) or some of its keys (`d` of `d /
/// Del`, `↑ / ↓` of `↑ / ↓ (j/k)`).
pub fn registry_hint(context: Context, keys: &str) -> Hint {
    registry_hint_in(context, None, keys)
}

/// [`registry_hint`] for the entry in one group of the screen, where the screen
/// lists the keys twice (Find's `Esc`: Cancel in the prompt, Clear at the table).
pub fn registry_hint_in(context: Context, group: Option<&str>, keys: &str) -> Hint {
    let label = entry(context, group, keys).map_or("", |k| k.label);
    Hint::new(chip_keys(keys), label)
}

/// [`registry_hint_in`] saying `label`, which must be one of the entry's words: what
/// the key does there turns on the screen's state.
pub fn registry_hint_as(
    context: Context,
    group: Option<&str>,
    keys: &str,
    label: &'static str,
) -> Hint {
    let entry = entry(context, group, keys);
    debug_assert!(
        entry.is_none_or(|k| k.says(label)),
        "{keys:?} in the {context:?} registry does not say {label:?}"
    );
    Hint::new(chip_keys(keys), label)
}

fn entry(context: Context, group: Option<&str>, keys: &str) -> Option<&'static Key> {
    let entry = datui_cli::keys::lookup(context, group, keys);
    debug_assert!(
        entry.is_some(),
        "{keys:?} is not in the {context:?} registry"
    );
    entry
}

/// Keys as a hint writes them: `↑↓` for `↑ / ↓` in the glyph set in use, `n/N` for
/// `n / N`, `^G` for `Ctrl+G`, `type` for `(type)`.
fn chip_keys(keys: &str) -> String {
    let g = crate::glyphs::get();
    match keys {
        "↑ / ↓" => g.updown.to_string(),
        "← / →" => g.updown_lr.to_string(),
        "(type)" => "type".to_string(),
        "Delete" => "Del".to_string(),
        _ => keys.replace(" / ", "/").replace("Ctrl+", "^"),
    }
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
    /// The rows so far of standard input still arriving: `12,400+`.
    Partial(usize),
    /// From a sample of a dataset's files, until they are counted: `~4.12B (est.)`.
    Estimated(usize),
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
            Total::Known(n) if short => crate::home::discover::format_rows(n),
            Total::Known(n) => crate::numfmt::group_chrome(n),
            Total::Partial(n) if short => format!("{}+", crate::home::discover::format_rows(n)),
            Total::Partial(n) => format!("{}+", crate::numfmt::group_chrome(n)),
            // As precise as a sample is, whatever the room.
            Total::Estimated(n) if short => format!("~{}", crate::home::discover::format_rows(n)),
            Total::Estimated(n) => format!("~{} (est.)", crate::home::discover::format_rows(n)),
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
    /// The columns the view gave a type, first: it types them before it filters.
    pub typed: Vec<String>,
    pub filters: Vec<String>,
    pub sort: Option<String>,
}

impl ViewState {
    /// `typed zip`, or `typed 3`.
    fn typed(&self) -> Option<String> {
        match self.typed.as_slice() {
            [] => None,
            [one] => Some(format!("typed {one}")),
            many => Some(format!("typed {}", many.len())),
        }
    }

    fn full(&self) -> Vec<String> {
        let mut parts: Vec<String> = self.typed().into_iter().collect();
        parts.extend(self.filters.clone());
        parts.extend(self.sort.clone());
        parts
    }

    fn counted(&self) -> Vec<String> {
        let mut parts: Vec<String> = match self.typed.len() {
            0 => Vec::new(),
            n => vec![format!("typed {n}")],
        };
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
    /// The numbers are bytes, written with a unit (`41 MB / 133 MB`).
    pub in_bytes: bool,
}

impl ProgressCount {
    /// `done` of `total` things called `noun`.
    pub fn of(noun: &'static str, done: u64, total: Option<u64>) -> Self {
        Self {
            noun,
            done,
            total,
            in_bytes: false,
        }
    }

    /// `done` of `total` bytes.
    pub fn bytes(noun: &'static str, done: u64, total: Option<u64>) -> Self {
        Self {
            in_bytes: true,
            ..Self::of(noun, done, total)
        }
    }

    fn number(&self, n: u64) -> String {
        if self.in_bytes {
            crate::numfmt::bytes(n)
        } else {
            crate::numfmt::group_chrome(n as usize)
        }
    }
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

/// Where a segment the user can click landed, and the key a click on it presses: a
/// hint's key, help's, or the `query` stage's and the filters' (see [`Footer::query_key`]).
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
    /// Where a path the message ends in starts: a cut keeps its file name.
    pub message_path: Option<usize>,
    pub position: Option<Position>,
    /// The active mode's keys.
    pub hints: Vec<Hint>,
    /// The key that opens help: `?`, or `F1` where `?` types.
    pub help: Option<&'static str>,
    /// The key a click on the `query` stage presses: the command line, on its text.
    pub query_key: Option<&'static str>,
    /// The key a click on the filters and sort presses: the sidebar that lists them.
    pub view_key: Option<&'static str>,
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

    /// Draw the status line into `area` (one row). Returns where each key landed,
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
            steps.push(vec![Span::styled(
                crate::glyphs::fit_middle(name, w),
                label,
            )]);
        }
        // The steps a click presses a key on, by their place among the steps.
        let mut step_keys: Vec<(usize, &'static str)> = Vec::new();
        if fit.stages {
            for (i, stage) in self.stages.iter().enumerate() {
                let style = if i == 0 && stage == QUERY_STAGE {
                    if let Some(key) = self.query_key {
                        step_keys.push((steps.len(), key));
                    }
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
            if let Some(key) = self.view_key {
                step_keys.push((steps.len(), key));
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
        let mut keys: Vec<(usize, usize, String)> = Vec::new();
        for (i, spans) in steps.into_iter().enumerate() {
            if i > 0 {
                left.push(step.clone());
            }
            let at: usize = left.iter().map(Span::width).sum();
            let w: usize = spans.iter().map(Span::width).sum();
            if let Some((_, key)) = step_keys.iter().find(|(s, _)| *s == i) {
                keys.push((at, w, key.to_string()));
            }
            left.extend(spans);
        }
        // Where the left's keys landed, cut at the edge of the line.
        let mut drawn: Drawn = keys
            .into_iter()
            .filter_map(|(at, w, key)| {
                let x = area.x.checked_add(at as u16)?;
                (x < area.right()).then(|| {
                    (
                        Rect::new(x, area.y, (w as u16).min(area.right() - x), 1),
                        key,
                    )
                })
            })
            .collect();
        if let (Some(w), Some(message)) = (fit.message, &self.message) {
            if any_step {
                left.push(Span::raw("   "));
            }
            left.push(Span::styled(
                cut_message(message, self.message_path, w),
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
        drawn.extend(keys.into_iter().filter_map(|(at, w, key)| {
            let x = x.checked_add(at as u16)?;
            (x < area.right()).then(|| {
                (
                    Rect::new(x, area.y, (w as u16).min(area.right() - x), 1),
                    key,
                )
            })
        }));
        drawn
    }
}

/// The stage a query in effect shows as.
pub const QUERY_STAGE: &str = "query";

/// A message in `width` columns. One that ends in a path keeps what it says and
/// the path's end, `Exported to …daily/out.csv`; any other is cut at its end.
pub fn cut_message(message: &str, path_from: Option<usize>, width: usize) -> String {
    use crate::glyphs::{display_width, fit, fit_start, get};
    if display_width(message) <= width {
        return message.to_string();
    }
    if let Some((prefix, path)) = path_from.and_then(|at| message.split_at_checked(at)) {
        let room = width.saturating_sub(display_width(prefix));
        // Room for a file name's worth of the path, or the plain cut.
        if room >= display_width(get().ellipsis) + 8 {
            return format!("{prefix}{}", fit_start(path, room));
        }
    }
    fit(message, width)
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
        spans.push(Span::styled(count.number(count.done), label));
        if let Some(total) = count.total {
            spans.push(Span::styled(
                format!(" / {}", count.number(total)),
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
mod tests;
