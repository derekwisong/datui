//! The chart view's keys. The panel and the export dialog take the shared form keys
//! (`crate::form`); the chart's own keys come after them.

use crate::chart_export::{ChartExportFormat, ChartExportRequest};
use crate::chart_export_modal::{ChartExportFocus, ExportDefaults};
use crate::chart_modal::{ChartFocus, Mark};
use crate::form::{FormKey, PickerKey};
use crate::logging::LogFailure;
use crate::output_file::Overwrite;
use crate::widgets::crosshair::{self, Move};
use crate::{App, AppEvent, InputMode, home};
use crate::{ChartPrepared, ChartRequest};
use crossterm::event::{KeyCode, KeyEvent};

impl App {
    /// Keys in the chart view.
    pub(crate) fn chart_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        if self.chart_export_modal.active {
            return self.chart_export_key(event);
        }

        let multi = self.chart_modal.picker_multi();
        if let Some(picker) = self.chart_modal.picker.as_mut() {
            match crate::form::picker_key(picker, multi, event) {
                PickerKey::Close => self.chart_modal.close_picker(),
                PickerKey::Choose => self.chart_modal.picker_choose(),
                PickerKey::Toggle => self.chart_modal.picker_toggle(),
                PickerKey::ChooseAndMove(forward) => {
                    self.chart_modal.picker_choose();
                    crate::form::Form::move_focus(
                        &mut self.chart_modal,
                        if forward { 1 } else { -1 },
                    );
                }
                PickerKey::Handled | PickerKey::Other => {}
            }
            return None;
        }

        // The plot has the keys: ←→ step the crosshair, Home and End go to the ends,
        // and x, Tab, Shift+Tab or Esc hand the keys back to the panel. The keys that
        // act from anywhere still do; the rest would edit a row with no rail on it, so
        // they do nothing.
        if self.chart_modal.plot_focus {
            let to = match event.code {
                KeyCode::Left | KeyCode::Char('h') => Some(Move::Left),
                KeyCode::Right | KeyCode::Char('l') => Some(Move::Right),
                KeyCode::Home => Some(Move::First),
                KeyCode::End => Some(Move::Last),
                _ => None,
            };
            if let Some(to) = to {
                self.move_crosshair(to);
                return None;
            }
            match event.code {
                KeyCode::Char('x') | KeyCode::Esc | KeyCode::Tab | KeyCode::BackTab => {
                    self.chart_modal.plot_focus = false;
                    return None;
                }
                KeyCode::Char('1'..='7' | '[' | ']' | 'g' | 'e' | 't' | '?') => {}
                _ => return None,
            }
        }

        // The panel is a form, but one that applies as it changes: Enter acts on the
        // focused row as Space does, since there is nothing left to submit.
        match crate::form::key(&mut self.chart_modal, event) {
            FormKey::Cancel => {
                self.chart_modal.close();
                self.reset_chart_state();
                self.input_mode = InputMode::Normal;
                return None;
            }
            FormKey::Submit | FormKey::Act(_) => {
                self.chart_act(self.chart_modal.focus);
                return None;
            }
            FormKey::Step(row, delta) => {
                self.chart_modal.step(row, delta);
                return None;
            }
            FormKey::Moved | FormKey::Text(_) => return None,
            FormKey::Other => {}
        }

        match event.code {
            // The crosshair: the plot takes the keys.
            KeyCode::Char('x') if self.chart_modal.has_crosshair() => {
                self.chart_modal.plot_focus = true;
                self.move_crosshair_to(None);
            }
            // The type switches from anywhere: 1-7 name one in order, [ and ] step.
            // Safe as plain keys: with the Picker closed, nothing on this screen types.
            KeyCode::Char(c @ '1'..='7') => {
                let idx = c as usize - '1' as usize;
                self.chart_modal.set_mark(Mark::ALL[idx]);
            }
            KeyCode::Char('[') => self.chart_modal.step_mark(-1),
            KeyCode::Char(']') => self.chart_modal.step_mark(1),
            // The grid at the major ticks, on the kinds that have axes.
            KeyCode::Char('g') if self.chart_modal.has_grid() => {
                self.chart_modal.toggle_grid();
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.chart_modal.can_export() {
                    self.open_chart_export();
                }
            }
            // The chart keeps the rows it was drawn from; `t` draws the new ones
            // too.
            KeyCode::Char('t') if self.follow_rows_waiting() => {
                self.take_follow_rows(false);
                self.chart_cache.clear();
            }
            // q/Q do nothing in chart view (no exit)
            KeyCode::Char('?') => self.open_help_overlay(),
            KeyCode::Char('+') | KeyCode::Char('=') => self.chart_modal.adjust_number_row(1),
            KeyCode::Char('-') => self.chart_modal.adjust_number_row(-1),
            KeyCode::PageUp if self.chart_modal.focus == ChartFocus::LimitRows => {
                self.chart_modal.adjust_row_limit_page(1);
            }
            KeyCode::PageDown if self.chart_modal.focus == ChartFocus::LimitRows => {
                self.chart_modal.adjust_row_limit_page(-1);
            }
            _ => {}
        }
        None
    }

    /// Space (or Enter) on a panel row: a shelf opens its Picker, a toggle flips, a
    /// stepped row takes its next value.
    fn chart_act(&mut self, focus: ChartFocus) {
        if self.chart_modal.picker_for(focus).is_some() {
            self.chart_modal.open_picker();
        } else {
            self.chart_modal.step(focus, 1);
        }
    }

    /// Open the export dialog, its words started from the chart: what it is, and
    /// where its data comes from.
    fn open_chart_export(&mut self) {
        let (main, sub) = self.chart_modal.title();
        let description = if sub.is_empty() {
            main
        } else {
            format!("{main}, {sub}")
        };
        self.chart_export_modal.open(
            &self.theme,
            self.history_limit,
            ExportDefaults {
                description,
                source: self
                    .catalog_entry
                    .as_ref()
                    .map(|(_, entry)| entry.credit())
                    .unwrap_or_default(),
                legend: self.chart_modal.show_legend,
            },
        );
    }

    /// Keys in the chart's export dialog.
    fn chart_export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match crate::form::key(&mut self.chart_export_modal, event) {
            FormKey::Cancel => self.chart_export_modal.close(),
            FormKey::Submit => return self.submit_chart_export(),
            FormKey::Step(field, delta) => self.chart_export_modal.step(field, delta),
            FormKey::Text(field) => {
                // The size takes digits and the keys that move through them.
                let size = matches!(
                    field,
                    ChartExportFocus::WidthInput | ChartExportFocus::HeightInput
                );
                let allowed = !size
                    || match event.code {
                        KeyCode::Char(c) => c.is_ascii_digit(),
                        KeyCode::Backspace
                        | KeyCode::Delete
                        | KeyCode::Left
                        | KeyCode::Right
                        | KeyCode::Home
                        | KeyCode::End => true,
                        _ => false,
                    };
                if allowed && let Some(input) = self.chart_export_modal.focused_input_mut() {
                    let _ = input.handle_key(event, Some(&self.cache));
                    if size {
                        self.chart_export_modal.size_typed();
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Enter, from any field of the chart's export dialog: build the export from the
    /// state every row already echoes. A blank path exports nothing.
    fn submit_chart_export(&mut self) -> Option<AppEvent> {
        let modal = &self.chart_export_modal;
        let path_str = modal.path_input.value().trim();
        if path_str.is_empty() {
            crate::form::Form::focus(&mut self.chart_export_modal, ChartExportFocus::PathInput);
            return None;
        }
        // `~` and `$VAR` expand as everywhere else a path is typed.
        let mut path = home::expand_user_path(path_str);
        // A path that names a format takes it; one that names none takes the
        // format's extension.
        let format = match ChartExportFormat::from_extension(&path) {
            Some(format) => format,
            None => {
                let format = modal.format;
                if path.extension().is_none() {
                    path.set_extension(format.extension());
                }
                format
            }
        };
        let (width, height) = modal.export_dimensions();
        let options = crate::chart_export::ExportOptions {
            width,
            height,
            dpi: modal.size.dpi(),
            palette: crate::chart_export::Palette::for_style(
                modal.style,
                &self.app_config.theme.colors,
            ),
            legend: modal.legend,
            title: modal.title_input.value().trim().to_string(),
            description: modal.description_input.value().trim().to_string(),
            notes: modal.notes_input.value().trim().to_string(),
            source: modal.source_input.value().trim().to_string(),
            byline: modal.byline_input.value().trim().to_string(),
        };
        self.chart_export_modal
            .path_input
            .save_to_history(&self.cache)
            .or_log("save the chart export path");
        let path_display = path.display().to_string();
        let request = ChartExportRequest {
            path,
            format,
            options,
            overwrite: Overwrite::Forbid,
        };
        if request.path.exists() {
            self.pending_chart_export = Some(request);
            // Suspended, not closed: declining returns to the filled form with the
            // typed path intact.
            self.chart_export_modal.suspend();
            self.confirmation_modal.show_destructive(
                format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                "Overwrite",
            );
            return None;
        }
        self.chart_export_modal.suspend();
        Some(AppEvent::ChartExport(request))
    }

    /// The line or scatter series on screen, before any log.
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
