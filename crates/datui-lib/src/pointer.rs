//! The mouse, as shortcuts to keys (nothing here changes what a key does): the wheel
//! presses arrows, a footer key click presses it, a form row click focuses and presses
//! Space, a tab click selects it, a table or home click moves the cursor (double click:
//! Enter), a plot click places the crosshair. Header drag moves a column (`H`/`L`),
//! dragging the gap after sets width (`<`/`>`); header double click sorts (`[`/`]`), gap
//! double click fits (`=`); right click opens a cell's key menu. Mouse input is never
//! held: where a key would wait, it is dropped
//! ([`crate::event_pump::EventPump::terminal_mouse`]). Click targets are recorded while
//! drawing ([`record`]) by the widgets that lay them out.

use std::cell::RefCell;
use std::time::{Duration, Instant};

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers, MouseButton, MouseEvent, MouseEventKind};
use ratatui::layout::{Position, Rect};

use crate::form::{FieldKind, Form};
use crate::table::CellHit;
use crate::{App, InputMode, Overlay};

/// Rows a notch of the wheel moves.
pub const WHEEL_ROWS: usize = 3;

/// Two clicks on one cell this close together are a double click.
pub const DOUBLE_CLICK: Duration = Duration::from_millis(500);

/// The mouse events the app acts on: a left press, its drag and release, a right
/// press, and the wheel. Motion with no button down and the middle button are left
/// alone.
pub fn wanted(mouse: &MouseEvent) -> bool {
    matches!(
        mouse.kind,
        MouseEventKind::Down(MouseButton::Left | MouseButton::Right)
            | MouseEventKind::Drag(MouseButton::Left)
            | MouseEventKind::Up(MouseButton::Left)
            | MouseEventKind::ScrollDown
            | MouseEventKind::ScrollUp
            | MouseEventKind::ScrollLeft
            | MouseEventKind::ScrollRight
    )
}

/// Ask for presses, releases, drag motion and the wheel, SGR-encoded for wide screens;
/// not `EnableMouseCapture`, which also streams buttonless motion the app would discard.
/// Undone by `DisableMouseCapture`.
pub struct EnableMouse;

impl crossterm::Command for EnableMouse {
    fn write_ansi(&self, f: &mut impl std::fmt::Write) -> std::fmt::Result {
        f.write_str("\x1b[?1000h\x1b[?1002h\x1b[?1006h")
    }

    #[cfg(windows)]
    fn execute_winapi(&self) -> std::io::Result<()> {
        crossterm::Command::execute_winapi(&crossterm::event::EnableMouseCapture)
    }

    #[cfg(windows)]
    fn is_ansi_code_supported(&self) -> bool {
        crossterm::Command::is_ansi_code_supported(&crossterm::event::EnableMouseCapture)
    }
}

/// Take the mouse from the terminal when `on` (`display.mouse`, `--mouse`); off, the
/// terminal keeps it, and selecting text works as it does anywhere.
pub fn capture(on: bool, out: &mut impl std::io::Write) {
    if on {
        let _ = crossterm::execute!(out, EnableMouse);
    }
}

/// A form's field as drawn: the form, by its type, and the field, by name. Kept as
/// text so a frame's regions need not know every form's field type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldId {
    form: &'static str,
    field: String,
}

impl FieldId {
    pub fn of<T: Form>(field: T::Field) -> Self {
        Self {
            form: std::any::type_name::<T>(),
            field: format!("{field:?}"),
        }
    }

    /// Focus this field in `form` when it is that form's and shown there. Says what
    /// the field is and whether the click acts on it too.
    pub fn focus_in<T: Form>(&self, form: &mut T) -> Option<Clicked> {
        if self.form != std::any::type_name::<T>() {
            return None;
        }
        let (field, kind) = form
            .fields()
            .into_iter()
            .find(|(f, _)| format!("{f:?}") == self.field)?;
        let focused_already = form.focused() == field;
        let acts = !form.list_row(field) || focused_already;
        form.focus(field).then_some(Clicked { kind, acts })
    }
}

/// A field a click focused: its kind, and whether the click acts on it as well (a
/// list row's first click only focuses it: [`Form::list_row`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Clicked {
    pub kind: FieldKind,
    pub acts: bool,
}

/// What a region of the frame is, for a click.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Hit {
    /// A form's row: a click focuses it, then acts as Space.
    Field(FieldId),
    /// One of a choice's values drawn side by side, a tab bar's tab: a click focuses
    /// the field, when there is one, then steps it there with ← / →.
    Option {
        field: Option<FieldId>,
        index: usize,
        current: usize,
    },
    /// A line of an open picker: its place in the list as narrowed, the cursor's, and
    /// whether the list takes several. A click moves the cursor there and chooses.
    PickerItem {
        visible: usize,
        selected: usize,
        multi: bool,
    },
    /// An open picker's whole area. While one is open only it takes clicks.
    Picker,
    /// A row edited in place, such as a filter: while it is, nothing else takes
    /// clicks.
    Editor,
    /// A line of the analysis tools list.
    Tool(usize),
    /// Something a key names, a chart kind's tab (`1`-`6`): a click presses it.
    Key(KeyEvent),
    /// A dialog's footer chip: a click presses its key, even while a picker or an
    /// editor has the other clicks, or a question is up.
    Chip(KeyEvent),
    /// A line of the context menu.
    MenuItem(usize),
    /// The context menu's frame.
    Menu,
    /// A dialog that owns the keys, drawn over what came before: a click outside
    /// it does nothing, and only what is drawn after it, inside it, takes clicks.
    Modal,
}

thread_local! {
    /// The regions of the frame being drawn; `None` outside one, so a widget drawn on
    /// its own (a test's buffer) records nothing.
    static DRAWING: RefCell<Option<Vec<(Rect, Hit)>>> = const { RefCell::new(None) };
}

/// Say what a region of the frame being drawn is. Later regions lie on top.
pub fn record(rect: Rect, hit: Hit) {
    if rect.width == 0 || rect.height == 0 {
        return;
    }
    DRAWING.with_borrow_mut(|drawing| {
        if let Some(regions) = drawing {
            regions.push((rect, hit));
        }
    });
}

/// Say that `rect` is form `T`'s row for `field`.
pub fn record_field<T: Form>(rect: Rect, field: T::Field) {
    record(rect, Hit::Field(FieldId::of::<T>(field)));
}

/// Record a line's spans that a click picks among: each `(span, hit)` names a span of
/// `line`, drawn left-aligned from `area.x` on `area.y`. Their places are summed from
/// the spans drawn, so they agree with the line on screen.
pub fn record_spans(area: Rect, line: &ratatui::text::Line, spans: Vec<(usize, Hit)>) {
    let mut x = area.x;
    let mut at = Vec::with_capacity(line.spans.len());
    for span in &line.spans {
        let w = span.width() as u16;
        at.push((x, w));
        x = x.saturating_add(w);
    }
    for (i, hit) in spans {
        let Some(&(x, w)) = at.get(i) else { continue };
        if x >= area.right() {
            continue;
        }
        record(Rect::new(x, area.y, w.min(area.right() - x), 1), hit);
    }
}

/// What `draw` records, as a frame would: for a widget's tests.
#[cfg(test)]
pub(crate) fn recording(draw: impl FnOnce()) -> Vec<(Rect, Hit)> {
    begin_recording();
    draw();
    end_recording()
}

fn begin_recording() {
    DRAWING.with_borrow_mut(|drawing| *drawing = Some(Vec::new()));
}

fn end_recording() -> Vec<(Rect, Hit)> {
    DRAWING.with_borrow_mut(|drawing| drawing.take().unwrap_or_default())
}

/// What a mouse event comes to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Pointer {
    Nothing,
    /// Press these keys in order, as if typed, as far as typed keys would act now.
    Keys(Vec<KeyEvent>),
    /// Move to what was clicked, then press the key if there is one (a double
    /// click's Enter).
    Point(Target, Option<KeyEvent>),
    /// Focus a form's field, when one is named, then press keys: with `act`, the key
    /// for what the field is ([`act_key`]; `true` for a right click), else `keys`.
    Form {
        field: Option<FieldId>,
        act: Option<bool>,
        keys: Vec<KeyEvent>,
    },
    /// The analysis tools list: put its cursor on this tool and press Enter.
    Tool(usize),
    /// Start setting a column's width by dragging the gap after it.
    Resize {
        column: String,
        x: u16,
    },
    /// A width dragged to: set it, as `<` and `>` do.
    Width {
        column: String,
        width: u16,
    },
    /// A header let go over another column: move it there, as `H` and `L` do.
    Drop {
        column: String,
        onto: String,
    },
    /// Open the context menu on this cell, at this point.
    Menu(CellHit, Position),
    /// A line of the context menu: close it and press its key.
    MenuChoose(usize),
    /// A click outside the open menu: close it, nothing more.
    CloseMenu,
    /// Nothing to press, but what is drawn changed (a header carried over another
    /// column): a frame is due.
    Redraw,
}

/// Where a click or the wheel moves the cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// A cell of the table as drawn.
    Table(CellHit),
    /// A row of the home list, by its place among those listed.
    HomeRow(usize),
    /// The home list's selection, moved this many rows and stopped at the ends: the
    /// arrows there go round, which a wheel must not.
    HomeStep(isize),
    /// The chart's crosshair, on the point drawn nearest this column; the plot takes
    /// the keys, as `x` gives them to it.
    ChartColumn(u16),
}

/// A drag in progress, from a press on the table's header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Drag {
    /// A header picked up: the column, and the column it is over now.
    Move { column: String, over: String },
    /// The gap after a header: the column, where the press was, and its width then.
    Resize { column: String, x: u16, width: u16 },
}

/// What the last frame drew where, the last click, and a drag in progress.
#[derive(Debug, Default)]
pub struct Pointing {
    /// The footer's keys and the key each presses.
    chips: Vec<(Rect, KeyEvent)>,
    /// The home list: its area and the row on each of its lines.
    home_list: Option<(Rect, Vec<Option<usize>>)>,
    /// Everything else the frame said a click can land on, bottom first.
    hits: Vec<(Rect, Hit)>,
    last_click: Option<(u16, u16, Instant)>,
    drag: Option<Drag>,
    /// Something was handled after the last frame was painted, so what it shows may
    /// not be what a click there would now land on.
    changed: bool,
}

impl Pointing {
    /// Something was handled: the frame on screen may be out of date.
    pub fn changed(&mut self) {
        self.changed = true;
    }

    /// A frame was painted: the screen shows the app as it is.
    pub fn painted(&mut self) {
        self.changed = false;
    }

    /// Whether the frame on screen shows the app as it is, so the regions recorded
    /// while drawing it are where a click lands.
    pub fn on_screen(&self) -> bool {
        !self.changed
    }

    /// A frame begins: what it does not draw cannot be clicked.
    pub fn forget_drawn(&mut self) {
        self.chips.clear();
        self.home_list = None;
        self.hits.clear();
        begin_recording();
    }

    /// The frame is drawn: keep what it recorded.
    pub fn drawn(&mut self) {
        self.hits = end_recording();
    }

    /// The footer's keys as drawn. A key that is not one key (`↑↓`) presses nothing.
    pub fn chips_drawn(&mut self, chips: Vec<(Rect, &str)>) {
        self.chips = chips
            .into_iter()
            .filter_map(|(rect, key)| chip_key(key).map(|key| (rect, key)))
            .collect();
    }

    /// The home list as drawn: `lines[i]` is the row on the list's line `i`.
    pub fn home_list_drawn(&mut self, area: Rect, lines: Vec<Option<usize>>) {
        self.home_list = Some((area, lines));
    }

    /// The drag in progress, if any.
    pub fn drag(&self) -> Option<&Drag> {
        self.drag.as_ref()
    }

    fn chip_at(&self, at: Position) -> Option<KeyEvent> {
        self.chips
            .iter()
            .find(|(rect, _)| rect.contains(at))
            .map(|(_, key)| *key)
    }

    fn home_row_at(&self, at: Position) -> Option<usize> {
        let (area, lines) = self.home_list.as_ref()?;
        if !area.contains(at) {
            return None;
        }
        lines.get(usize::from(at.y - area.y)).copied().flatten()
    }

    /// What the frame drew at `at`, topmost first. Under a dialog, only what it
    /// drew; while a picker or an editor is open, only a picker's lines.
    fn hit_at(&self, at: Position) -> Option<&Hit> {
        let hits = match self.hits.iter().rposition(|(_, hit)| *hit == Hit::Modal) {
            Some(i) if !self.hits[i].0.contains(at) => return None,
            Some(i) => &self.hits[i + 1..],
            None => &self.hits[..],
        };
        let open = hits
            .iter()
            .any(|(_, hit)| matches!(hit, Hit::Picker | Hit::Editor));
        hits.iter()
            .rev()
            .filter(|(_, hit)| {
                !open || matches!(hit, Hit::Picker | Hit::PickerItem { .. } | Hit::Chip(_))
            })
            .find(|(rect, _)| rect.contains(at))
            .map(|(_, hit)| hit)
    }

    /// Whether the frame has an open picker under `at`.
    fn over_picker(&self, at: Position) -> bool {
        self.hits
            .iter()
            .any(|(rect, hit)| *hit == Hit::Picker && rect.contains(at))
    }

    /// Whether a click at `at` is the second of a double click. A third click starts
    /// over, so a triple click is a double click and then a click.
    fn second_click(&mut self, at: Position, now: Instant) -> bool {
        let double = self.last_click.is_some_and(|(x, y, then)| {
            (x, y) == (at.x, at.y) && now.saturating_duration_since(then) <= DOUBLE_CLICK
        });
        self.last_click = if double {
            None
        } else {
            Some((at.x, at.y, now))
        };
        double
    }
}

/// The key a double click on `column`'s header presses: `[` sorts up by it, `]` turns
/// that sort down, and `]` again takes it away, as the same key again does.
fn header_sort_key(state: &crate::table::DataTableState, column: &str) -> char {
    if state.view_sort_columns() == [column] {
        ']'
    } else {
        '['
    }
}

/// The key a chip's label names, when it names one key: `Enter`, `^O`, `q`, `F1`.
pub fn chip_key(label: &str) -> Option<KeyEvent> {
    let named = |code| Some(KeyEvent::new(code, KeyModifiers::NONE));
    match label {
        "Enter" => return named(KeyCode::Enter),
        "Esc" => return named(KeyCode::Esc),
        "Tab" => return named(KeyCode::Tab),
        "Space" => return named(KeyCode::Char(' ')),
        "F1" => return named(KeyCode::F(1)),
        _ => {}
    }
    let mut chars = label.chars();
    match (chars.next(), chars.next(), chars.next()) {
        (Some('^'), Some(c), None) if c.is_ascii_alphabetic() => Some(KeyEvent::new(
            KeyCode::Char(c.to_ascii_lowercase()),
            KeyModifiers::CONTROL,
        )),
        // As a terminal reports it: an upper-case letter comes with Shift.
        (Some(c), None, None) if c.is_ascii_uppercase() => {
            Some(KeyEvent::new(KeyCode::Char(c), KeyModifiers::SHIFT))
        }
        (Some(c), None, None) if c.is_ascii_graphic() => named(KeyCode::Char(c)),
        _ => None,
    }
}

fn press(code: KeyCode) -> KeyEvent {
    KeyEvent::new(code, KeyModifiers::NONE)
}

/// `delta` presses of `forward`, or of `back` when it is negative.
fn steps(delta: isize, back: KeyCode, forward: KeyCode) -> Vec<KeyEvent> {
    let code = if delta < 0 { back } else { forward };
    vec![press(code); delta.unsigned_abs()]
}

/// The key a click presses on a field it focused: Space acts on it (toggles,
/// steps, opens, presses); a right click steps a choice back, as ← does, and only
/// focuses anything else. A text field only takes focus.
pub fn act_key(kind: FieldKind, back: bool) -> Option<KeyEvent> {
    match kind {
        FieldKind::Text | FieldKind::MultilineText => None,
        FieldKind::Choice | FieldKind::Picker { multi: false } if back => {
            Some(press(KeyCode::Left))
        }
        _ if back => None,
        _ => Some(press(KeyCode::Char(' '))),
    }
}

/// What a click on `hit` does; `back` for a right click.
fn click_hit(hit: &Hit, back: bool) -> Pointer {
    match hit {
        Hit::Field(id) => Pointer::Form {
            field: Some(id.clone()),
            act: Some(back),
            keys: Vec::new(),
        },
        Hit::Option {
            field,
            index,
            current,
        } => Pointer::Form {
            field: field.clone(),
            act: None,
            keys: steps(
                *index as isize - *current as isize,
                KeyCode::Left,
                KeyCode::Right,
            ),
        },
        Hit::PickerItem {
            visible,
            selected,
            multi,
        } => {
            let mut keys = steps(
                *visible as isize - *selected as isize,
                KeyCode::Up,
                KeyCode::Down,
            );
            keys.push(press(if *multi {
                KeyCode::Char(' ')
            } else {
                KeyCode::Enter
            }));
            Pointer::Keys(keys)
        }
        Hit::Tool(i) => Pointer::Tool(*i),
        Hit::Key(key) | Hit::Chip(key) => Pointer::Keys(vec![*key]),
        Hit::MenuItem(i) => Pointer::MenuChoose(*i),
        Hit::Picker | Hit::Editor | Hit::Menu | Hit::Modal => Pointer::Nothing,
    }
}

impl App {
    /// What a mouse event means on the screen last drawn.
    pub fn pointer(&mut self, mouse: &MouseEvent, now: Instant) -> Pointer {
        let shift = mouse.modifiers.contains(KeyModifiers::SHIFT);
        // A menu something else has covered since is gone, as for a key.
        if self.context_menu.is_some() && !self.menu_showing() {
            self.close_context_menu();
        }
        let at = Position {
            x: mouse.column,
            y: mouse.row,
        };
        match mouse.kind {
            MouseEventKind::ScrollDown if shift => self.wheel_across(true),
            MouseEventKind::ScrollUp if shift => self.wheel_across(false),
            MouseEventKind::ScrollRight => self.wheel_across(true),
            MouseEventKind::ScrollLeft => self.wheel_across(false),
            MouseEventKind::ScrollDown => self.wheel(true, at),
            MouseEventKind::ScrollUp => self.wheel(false, at),
            MouseEventKind::Down(MouseButton::Left) => {
                self.pointer.drag = None;
                self.click(at, now)
            }
            MouseEventKind::Down(MouseButton::Right) => {
                self.pointer.drag = None;
                self.right_click(at)
            }
            MouseEventKind::Drag(MouseButton::Left) => self.drag_to(at),
            MouseEventKind::Up(MouseButton::Left) => self.release(),
            _ => Pointer::Nothing,
        }
    }

    /// A key was pressed: a header being carried is put back, and its mark goes.
    pub fn cancel_drag(&mut self) {
        self.pointer.drag = None;
    }

    /// Forget the last click and any drag: it was dropped, so the next is not its
    /// second, and nothing was picked up.
    pub fn forget_click(&mut self) {
        self.pointer.last_click = None;
        self.pointer.drag = None;
    }

    /// Move the cursor to what a click or the wheel landed on. A press on a header
    /// picks the column up, for a drag to move.
    pub fn point(&mut self, target: &Target) {
        // As a key does: the line about the last action is stale now.
        self.flash = None;
        match target {
            Target::Table(hit) => {
                if let Some(state) = self.data_table_state.as_mut() {
                    state.point_at(hit);
                }
                if let (None, Some(column)) = (hit.row, &hit.column) {
                    self.pointer.drag = Some(Drag::Move {
                        column: column.clone(),
                        over: column.clone(),
                    });
                }
            }
            Target::HomeRow(row) => {
                self.home.status = None;
                self.home.select(*row);
            }
            Target::HomeStep(rows) => {
                self.home.status = None;
                self.home.page_selection(*rows);
            }
            Target::ChartColumn(column) => {
                self.chart.modal.plot_focus = true;
                self.move_crosshair_to(Some(*column));
            }
        }
    }

    /// Put the cursor on the cell a right click landed on, for the menu. Returns
    /// whether it is there: rows that moved since they were drawn are not the ones
    /// clicked, and the menu would act on another cell.
    pub fn point_for_menu(&mut self, hit: &CellHit) -> bool {
        self.flash = None;
        self.data_table_state
            .as_mut()
            .is_some_and(|state| state.point_at(hit))
    }

    /// Focus the field a click landed on, in whichever form drew it.
    pub fn focus_field(&mut self, id: &FieldId) -> Option<Clicked> {
        self.flash = None;
        if let Some(kind) = id.focus_in(&mut self.chart.modal) {
            // The option rows take the keys back from the plot's crosshair.
            self.chart.modal.plot_focus = false;
            return Some(kind);
        }
        let shown = id
            .focus_in(&mut self.export_modal)
            .or_else(|| id.focus_in(&mut self.copy_modal))
            .or_else(|| id.focus_in(&mut self.chart.export_modal))
            .or_else(|| id.focus_in(&mut self.pivot_melt_modal))
            .or_else(|| id.focus_in(&mut self.sort_filter_modal))
            .or_else(|| id.focus_in(&mut self.view_modal))
            .or_else(|| {
                self.column_forms
                    .combine
                    .as_mut()
                    .and_then(|c| id.focus_in(c))
            })
            .or_else(|| self.sample.form.as_mut().and_then(|f| id.focus_in(f)));
        if shown.is_some() {
            return shown;
        }
        let analysis = &mut self.analysis_modal;
        let kind = analysis
            .sample_form
            .as_mut()
            .and_then(|f| id.focus_in(f))
            .or_else(|| {
                analysis
                    .quality
                    .expected_form
                    .as_mut()
                    .and_then(|f| id.focus_in(f))
            })
            .or_else(|| {
                analysis
                    .quality
                    .intent_form
                    .as_mut()
                    .and_then(|f| id.focus_in(f))
            })?;
        // A form in the result pane takes the keys once the pane has them.
        analysis.focus = crate::analysis::analysis_modal::AnalysisFocus::Main;
        Some(kind)
    }

    /// Put the analysis tools list's cursor on `tool`, the list focused, as Tab and
    /// the arrows would.
    pub fn point_at_tool(&mut self, tool: usize) {
        self.flash = None;
        self.analysis_modal.focus = crate::analysis::analysis_modal::AnalysisFocus::Sidebar;
        self.analysis_modal.sidebar_state.select(Some(tool));
    }

    /// Start a width drag on `column`, from the width it has now.
    pub fn start_resize(&mut self, column: String, x: u16) {
        use crate::widgets::column_widths::{UNSEEN_WIDTH, WidthChoice};
        let width = self
            .data_table_state
            .as_ref()
            .and_then(|state| match state.width_choice(&column) {
                WidthChoice::Manual(w) => Some(w),
                _ => state.on_screen_width(&column),
            })
            .unwrap_or(UNSEEN_WIDTH);
        self.pointer.drag = Some(Drag::Resize { column, x, width });
    }

    /// Set a column's width by hand, as `<` and `>` do, within their bounds.
    pub fn set_dragged_width(&mut self, column: String, width: u16) {
        use crate::widgets::column_widths::{MAX_WIDTH, MIN_WIDTH, WidthChoice};
        self.flash = None;
        if let Some(state) = self.data_table_state.as_mut() {
            let width = width.clamp(MIN_WIDTH, MAX_WIDTH);
            state.set_width_choices([(column, WidthChoice::Manual(width))]);
        }
    }

    /// The home screen is up with nothing over it, so its keys go to it.
    fn home_has_the_keys(&self) -> bool {
        self.input_mode == InputMode::Home
            && !self.help_visible()
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// The chart view is up with nothing over it: no export dialog, Picker, help or
    /// modal.
    fn chart_has_the_keys(&self) -> bool {
        self.overlay == Overlay::Chart
            && self.chart.modal.picker.is_none()
            && !self.help_visible()
            && !self.error_modal.active
            && !self.confirmation_modal.active
    }

    /// A dialog that takes every key is over the screen: the help, an error, a
    /// question. What is drawn under it takes no clicks.
    fn dialog_over_all(&self) -> bool {
        self.help_visible() || self.error_modal.active || self.confirmation_modal.active
    }

    /// The wheel: the arrows of whatever has the keys. Not where ↑↓ walk a text
    /// field's history, and not in the prompt for a path on the home screen; over an
    /// open picker, its list.
    fn wheel(&self, down: bool, at: Position) -> Pointer {
        let code = if down { KeyCode::Down } else { KeyCode::Up };
        let arrows = Pointer::Keys(vec![press(code); WHEEL_ROWS]);
        if self.context_menu.is_some() {
            return Pointer::Nothing;
        }
        if self.home_has_the_keys() {
            if self.home.path_input_active {
                return Pointer::Nothing;
            }
            let rows = WHEEL_ROWS as isize;
            return Pointer::Point(Target::HomeStep(if down { rows } else { -rows }), None);
        }
        if !self.dialog_over_all() && self.pointer.over_picker(at) {
            return arrows;
        }
        if self.text_field_focused() {
            return Pointer::Nothing;
        }
        arrows
    }

    /// The wheel across, or Shift with it: the column cursor, at the table only.
    /// Elsewhere ←→ switch tabs or step rows, which a sideways scroll should not.
    fn wheel_across(&self, right: bool) -> Pointer {
        if !self.in_normal_table_view() || self.data_table_state.is_none() {
            return Pointer::Nothing;
        }
        let code = if right { KeyCode::Right } else { KeyCode::Left };
        Pointer::Keys(vec![press(code)])
    }

    fn click(&mut self, at: Position, now: Instant) -> Pointer {
        if self.context_menu.is_some() {
            self.pointer.last_click = None;
            return match self.pointer.hit_at(at) {
                Some(Hit::MenuItem(i)) => Pointer::MenuChoose(*i),
                Some(Hit::Menu) => Pointer::Nothing,
                _ => Pointer::CloseMenu,
            };
        }
        if let Some(key) = self.pointer.chip_at(at) {
            self.pointer.last_click = None;
            return Pointer::Keys(vec![key]);
        }
        // Over help, an error or a question, only its footer's keys take clicks.
        if self.dialog_over_all() {
            self.pointer.last_click = None;
            return match self.pointer.hit_at(at) {
                Some(Hit::Chip(key)) => Pointer::Keys(vec![*key]),
                _ => Pointer::Nothing,
            };
        }
        if let Some(hit) = self.pointer.hit_at(at).cloned() {
            self.pointer.last_click = None;
            return click_hit(&hit, false);
        }
        let double = self.pointer.second_click(at, now);
        let enter = double.then(|| press(KeyCode::Enter));
        if self.home_has_the_keys() {
            return match self.pointer.home_row_at(at) {
                Some(row) if !self.home.path_input_active => {
                    Pointer::Point(Target::HomeRow(row), enter)
                }
                _ => Pointer::Nothing,
            };
        }
        if self.chart_has_the_keys()
            && self.chart.modal.has_crosshair()
            && self
                .chart
                .modal
                .plot
                .is_some_and(|plot| plot.graph.contains(at))
        {
            return Pointer::Point(Target::ChartColumn(at.x), None);
        }
        if self.in_normal_table_view()
            && let Some(state) = self.data_table_state.as_ref()
        {
            if let Some(column) = state.drawn_edge(at.x, at.y) {
                // A double click on the gap fits the column to its rows, as `=` does
                // on the cursor's column; the first click started a drag that did not
                // move.
                if double {
                    let hit = CellHit {
                        row: None,
                        column: Some(column),
                    };
                    return Pointer::Point(Target::Table(hit), Some(press(KeyCode::Char('='))));
                }
                return Pointer::Resize { column, x: at.x };
            }
            if let Some(hit) = state.drawn_cell(at.x, at.y) {
                // Enter acts on a row. A double click on a header sorts by it, as
                // `[` and `]` do: up, then down, then off.
                let key = match (&hit.row, &hit.column) {
                    (Some(_), _) => enter,
                    (None, Some(column)) if double => {
                        Some(press(KeyCode::Char(header_sort_key(state, column))))
                    }
                    (None, _) => None,
                };
                return Pointer::Point(Target::Table(hit), key);
            }
        }
        Pointer::Nothing
    }

    /// A right click: on a table cell, the context menu; on a form's choice, a step
    /// back. Anywhere else, nothing.
    fn right_click(&mut self, at: Position) -> Pointer {
        self.pointer.last_click = None;
        if self.context_menu.is_some() {
            return Pointer::CloseMenu;
        }
        if self.dialog_over_all() {
            return Pointer::Nothing;
        }
        if let Some(hit) = self.pointer.hit_at(at).cloned() {
            return match hit {
                Hit::Field(_) => click_hit(&hit, true),
                _ => Pointer::Nothing,
            };
        }
        if self.in_normal_table_view()
            && let Some(hit) = self
                .data_table_state
                .as_ref()
                .and_then(|state| state.drawn_cell(at.x, at.y))
            && hit.row.is_some()
            && hit.column.is_some()
        {
            return Pointer::Menu(hit, at);
        }
        Pointer::Nothing
    }

    /// The pointer moved with the button down: a header carried over the columns, or
    /// the gap after one pulled to a width.
    fn drag_to(&mut self, at: Position) -> Pointer {
        match self.pointer.drag.clone() {
            Some(Drag::Move { column, .. }) => {
                // Off the table's columns it is over itself: let go there, nothing
                // moves.
                let over = self
                    .data_table_state
                    .as_ref()
                    .and_then(|state| state.drawn_cell(at.x, at.y))
                    .and_then(|hit| hit.column)
                    .unwrap_or_else(|| column.clone());
                if !matches!(&self.pointer.drag, Some(Drag::Move { over: was, .. }) if *was == over)
                {
                    // The drop mark moves: a frame is due.
                    self.pointer.drag = Some(Drag::Move { column, over });
                    return Pointer::Redraw;
                }
                Pointer::Nothing
            }
            Some(Drag::Resize { column, x, width }) => {
                let width = (i32::from(width) + i32::from(at.x) - i32::from(x)).max(0);
                Pointer::Width {
                    column,
                    width: u16::try_from(width).unwrap_or(u16::MAX),
                }
            }
            None => Pointer::Nothing,
        }
    }

    /// The button let go: a header carried to another column moves there.
    fn release(&mut self) -> Pointer {
        match self.pointer.drag.take() {
            Some(Drag::Move { column, over }) if column != over => {
                Pointer::Drop { column, onto: over }
            }
            // Back where it was picked up: the mark it may have shown goes.
            Some(Drag::Move { .. }) => Pointer::Redraw,
            _ => Pointer::Nothing,
        }
    }
}

#[cfg(test)]
mod tests;
