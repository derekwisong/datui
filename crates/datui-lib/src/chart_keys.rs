//! The chart view's keys.

use crate::chart_export::{ChartExportFormat, ChartExportRequest};
use crate::chart_export_modal::ChartExportFocus;
use crate::chart_modal::{ChartFocus, ChartKind};
use crate::output_file::Overwrite;
use crate::widgets::crosshair::{self, Move};
use crate::{App, AppEvent, InputMode, home};
use crate::{ChartPrepared, ChartRequest};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the chart view.
    pub(crate) fn chart_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        // Chart export modal (sub-dialog within Chart mode)
        if self.chart_export_modal.active {
            match event.code {
                KeyCode::Esc if event.is_press() => {
                    self.chart_export_modal.close();
                }
                KeyCode::Tab if event.is_press() => {
                    self.chart_export_modal.next_focus();
                }
                KeyCode::BackTab if event.is_press() => {
                    self.chart_export_modal.prev_focus();
                }
                KeyCode::Up | KeyCode::Char('k')
                    if event.is_press()
                        && self.chart_export_modal.focus == ChartExportFocus::FormatSelector =>
                {
                    let idx = ChartExportFormat::ALL
                        .iter()
                        .position(|&f| f == self.chart_export_modal.selected_format)
                        .unwrap_or(0);
                    let prev = if idx == 0 {
                        ChartExportFormat::ALL.len() - 1
                    } else {
                        idx - 1
                    };
                    self.chart_export_modal.selected_format = ChartExportFormat::ALL[prev];
                }
                KeyCode::Down | KeyCode::Char('j')
                    if event.is_press()
                        && self.chart_export_modal.focus == ChartExportFocus::FormatSelector =>
                {
                    let idx = ChartExportFormat::ALL
                        .iter()
                        .position(|&f| f == self.chart_export_modal.selected_format)
                        .unwrap_or(0);
                    let next = (idx + 1) % ChartExportFormat::ALL.len();
                    self.chart_export_modal.selected_format = ChartExportFormat::ALL[next];
                }
                // Enter applies from anywhere in the form: build the
                // export from the state every row already echoes. A blank
                // path exports nothing.
                KeyCode::Enter if event.is_press() => {
                    let path_str = self.chart_export_modal.path_input.value().trim();
                    if !path_str.is_empty() {
                        let title = self
                            .chart_export_modal
                            .title_input
                            .value()
                            .trim()
                            .to_string();
                        let (width, height) = self.chart_export_modal.export_dimensions();
                        // `~` and `$VAR` expand as everywhere else a path
                        // is typed; unexpanded, the PNG/EPS writer fails
                        // with NotFound on the literal `~` directory.
                        let mut path = home::expand_user_path(path_str);
                        let format = self.chart_export_modal.selected_format;
                        // Only add default extension when user did not provide one
                        if path.extension().is_none() {
                            path.set_extension(format.extension());
                        }
                        let path_display = path.display().to_string();
                        let request = ChartExportRequest {
                            path,
                            format,
                            title,
                            width,
                            height,
                            overwrite: Overwrite::Forbid,
                        };
                        if request.path.exists() {
                            self.pending_chart_export = Some(request);
                            // Suspended, not closed: declining returns to
                            // the filled form with the typed path intact.
                            self.chart_export_modal.suspend();
                            self.confirmation_modal.show_destructive(
                                format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                                "Overwrite",
                            );
                        } else {
                            self.chart_export_modal.close();
                            return Some(AppEvent::ChartExport(request));
                        }
                    }
                }
                _ => {
                    if event.is_press() {
                        if self.chart_export_modal.focus == ChartExportFocus::TitleInput {
                            let _ = self.chart_export_modal.title_input.handle_key(event, None);
                        } else if self.chart_export_modal.focus == ChartExportFocus::PathInput {
                            let _ = self.chart_export_modal.path_input.handle_key(event, None);
                        } else if self.chart_export_modal.focus == ChartExportFocus::WidthInput {
                            let allow = match event.code {
                                KeyCode::Char(c) if c.is_ascii_digit() => true,
                                KeyCode::Backspace
                                | KeyCode::Delete
                                | KeyCode::Left
                                | KeyCode::Right
                                | KeyCode::Home
                                | KeyCode::End => true,
                                _ => false,
                            };
                            if allow {
                                let _ = self.chart_export_modal.width_input.handle_key(event, None);
                            }
                        } else if self.chart_export_modal.focus == ChartExportFocus::HeightInput {
                            let allow = match event.code {
                                KeyCode::Char(c) if c.is_ascii_digit() => true,
                                KeyCode::Backspace
                                | KeyCode::Delete
                                | KeyCode::Left
                                | KeyCode::Right
                                | KeyCode::Home
                                | KeyCode::End => true,
                                _ => false,
                            };
                            if allow {
                                let _ =
                                    self.chart_export_modal.height_input.handle_key(event, None);
                            }
                        }
                    }
                }
            }
            return None;
        }

        // The open Picker owns the keys: type to narrow, ↑↓ move, Space
        // toggles on the Y series row, Enter chooses, and Esc backs out
        // of the Picker and only the Picker.
        if self.chart_modal.picker.is_some() {
            match event.code {
                KeyCode::Esc if event.is_press() => self.chart_modal.picker = None,
                KeyCode::Enter if event.is_press() => self.chart_modal.picker_choose(),
                KeyCode::Tab if event.is_press() => {
                    self.chart_modal.picker_choose();
                    self.chart_modal.next_focus();
                }
                KeyCode::BackTab if event.is_press() => {
                    self.chart_modal.picker_choose();
                    self.chart_modal.prev_focus();
                }
                KeyCode::Up if event.is_press() => {
                    if let Some(picker) = self.chart_modal.picker.as_mut() {
                        picker.move_up();
                    }
                }
                KeyCode::Down if event.is_press() => {
                    if let Some(picker) = self.chart_modal.picker.as_mut() {
                        picker.move_down();
                    }
                }
                KeyCode::Char(' ')
                    if event.is_press()
                        && self.chart_modal.is_multi_row(self.chart_modal.focus) =>
                {
                    self.chart_modal.picker_toggle();
                }
                // On a pick-one row Space chooses like Enter — what Space
                // always did on these lists. It must never reach the
                // narrowing filter: a typed space matches nothing, and
                // the list blanking under the key that just opened it
                // reads as breakage.
                KeyCode::Char(' ') if event.is_press() => {
                    self.chart_modal.picker_choose();
                }
                KeyCode::Backspace if event.is_press() => {
                    if let Some(picker) = self.chart_modal.picker.as_mut() {
                        picker.backspace();
                    }
                }
                KeyCode::Char(c) if event.is_press() => {
                    if let Some(picker) = self.chart_modal.picker.as_mut() {
                        picker.filter_key(c, event.modifiers);
                    }
                }
                _ => {}
            }
            return None;
        }

        // The plot has the keys: ←→ step the crosshair, Home and End go to the ends,
        // and x, Tab, Shift+Tab or Esc hand the keys back to the option rows. The keys
        // that act from anywhere still do; the rest would edit a row with no rail on
        // it, so they do nothing.
        if self.chart_modal.plot_focus {
            let to = match event.code {
                KeyCode::Left | KeyCode::Char('h') => Some(Move::Left),
                KeyCode::Right | KeyCode::Char('l') => Some(Move::Right),
                KeyCode::Home => Some(Move::First),
                KeyCode::End => Some(Move::Last),
                _ => None,
            };
            if let Some(to) = to {
                if event.is_press() {
                    self.move_crosshair(to);
                }
                return None;
            }
            match event.code {
                KeyCode::Char('x') | KeyCode::Esc | KeyCode::Tab | KeyCode::BackTab => {
                    if event.is_press() {
                        self.chart_modal.plot_focus = false;
                    }
                    return None;
                }
                KeyCode::Char('1'..='6' | '[' | ']' | 'g' | 'e' | 't' | '?') => {}
                _ => return None,
            }
        }

        match event.code {
            // The crosshair: the plot takes the keys.
            KeyCode::Char('x') if event.is_press() && self.chart_modal.has_crosshair() => {
                self.chart_modal.plot_focus = true;
                self.move_crosshair_to(None);
            }
            // The chart kind switches from anywhere: 1-6 name a tab in
            // order, [ and ] cycle. Safe as plain keys — with the Picker
            // closed, nothing on this screen types.
            KeyCode::Char(c @ '1'..='6') if event.is_press() => {
                let idx = c as usize - '1' as usize;
                self.chart_modal.set_chart_kind(ChartKind::ALL[idx]);
            }
            KeyCode::Char('[') if event.is_press() => {
                self.chart_modal.prev_chart_kind();
            }
            KeyCode::Char(']') if event.is_press() => {
                self.chart_modal.next_chart_kind();
            }
            // The grid at the major ticks, on the kinds that have axes.
            KeyCode::Char('g') if event.is_press() && self.chart_modal.has_grid() => {
                self.chart_modal.toggle_grid();
            }
            KeyCode::Char('e') if event.is_press() => {
                // Open chart export modal when there is something visible to export
                if self.data_table_state.is_some() && self.chart_modal.can_export() {
                    self.chart_export_modal
                        .open(&self.theme, self.history_limit);
                }
            }
            // The chart keeps the rows it was drawn from; `t` draws the new ones
            // too.
            KeyCode::Char('t') if event.is_press() && self.follow_rows_waiting() => {
                self.take_follow_rows(false);
                self.chart_cache.clear();
            }
            // q/Q do nothing in chart view (no exit)
            KeyCode::Char('?') if event.is_press() => {
                self.open_help_overlay();
            }
            KeyCode::Esc if event.is_press() => {
                self.chart_modal.close();
                self.reset_chart_state();
                self.input_mode = InputMode::Normal;
            }
            KeyCode::Tab if event.is_press() => {
                self.chart_modal.next_focus();
            }
            KeyCode::BackTab if event.is_press() => {
                self.chart_modal.prev_focus();
            }
            // Enter or Space edits the focused row: a column row opens
            // its Picker, a toggle flips, the style cycles.
            KeyCode::Enter | KeyCode::Char(' ') if event.is_press() => {
                match self.chart_modal.focus {
                    ChartFocus::YStartsAtZero => self.chart_modal.toggle_y_starts_at_zero(),
                    ChartFocus::LogScale => self.chart_modal.toggle_log_scale(),
                    ChartFocus::ShowLegend => self.chart_modal.toggle_show_legend(),
                    ChartFocus::Grid => self.chart_modal.toggle_grid(),
                    ChartFocus::Style => self.chart_modal.next_chart_type(),
                    ChartFocus::Range => self.chart_modal.cycle_value_range(1),
                    ChartFocus::Order => self.chart_modal.cycle_bar_order(1),
                    focus if self.chart_modal.is_picker_row(focus) => {
                        self.chart_modal.open_picker();
                    }
                    _ => {}
                }
            }
            KeyCode::Char('+') | KeyCode::Char('=') if event.is_press() => {
                self.chart_modal.adjust_number_row(1);
            }
            KeyCode::Char('-') if event.is_press() => {
                self.chart_modal.adjust_number_row(-1);
            }
            KeyCode::Left | KeyCode::Char('h') if event.is_press() => {
                match self.chart_modal.focus {
                    ChartFocus::Style => self.chart_modal.prev_chart_type(),
                    _ => self.chart_modal.adjust_number_row(-1),
                }
            }
            KeyCode::Right | KeyCode::Char('l') if event.is_press() => {
                match self.chart_modal.focus {
                    ChartFocus::Style => self.chart_modal.next_chart_type(),
                    _ => self.chart_modal.adjust_number_row(1),
                }
            }
            KeyCode::PageUp if event.is_press() => {
                if self.chart_modal.focus == ChartFocus::LimitRows {
                    self.chart_modal.adjust_row_limit_page(1);
                }
            }
            KeyCode::PageDown if event.is_press() => {
                if self.chart_modal.focus == ChartFocus::LimitRows {
                    self.chart_modal.adjust_row_limit_page(-1);
                }
            }
            KeyCode::Up | KeyCode::Char('k') if event.is_press() => {
                self.chart_modal.prev_focus();
            }
            KeyCode::Down | KeyCode::Char('j') if event.is_press() => {
                self.chart_modal.next_focus();
            }
            _ => {}
        }
        None
    }

    /// The XY series on screen, before any log.
    fn chart_xy_series(&self) -> Option<&Vec<Vec<(f64, f64)>>> {
        let request = ChartRequest::from_modal(&self.chart_modal)?;
        match self.chart_cache.prepared(&request)? {
            ChartPrepared::XY(xy) => Some(&xy.series),
            _ => None,
        }
    }

    /// Step the crosshair, from where it stands or the middle of the plot.
    fn move_crosshair(&mut self, to: Move) {
        let (Some(place), Some(series)) = (self.chart_modal.plot, self.chart_xy_series()) else {
            return;
        };
        let xs = crosshair::xs(series);
        let from = self
            .chart_modal
            .cursor_x
            .or_else(|| crosshair::at_column(&xs, &place, middle(place.graph)));
        let to = from.and_then(|from| crosshair::step(&xs, &place, from, to));
        if to.is_some() {
            self.chart_modal.cursor_x = to;
        }
    }

    /// Put the crosshair on the point nearest `column`, or with none, back on the
    /// point it stood on (the nearest one now), or in the middle of the plot.
    pub(crate) fn move_crosshair_to(&mut self, column: Option<u16>) {
        let (Some(place), Some(series)) = (self.chart_modal.plot, self.chart_xy_series()) else {
            return;
        };
        let xs = crosshair::xs(series);
        let at = match (column, self.chart_modal.cursor_x) {
            (Some(column), _) => crosshair::at_column(&xs, &place, column),
            (None, Some(x)) => crosshair::nearest(&xs, x),
            (None, None) => crosshair::at_column(&xs, &place, middle(place.graph)),
        };
        if at.is_some() {
            self.chart_modal.cursor_x = at;
        }
    }
}

/// The middle column of `area`.
fn middle(area: ratatui::layout::Rect) -> u16 {
    area.left() + area.width / 2
}
