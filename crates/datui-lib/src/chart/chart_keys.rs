//! The chart view's keys. The panel and the export dialog take the shared form keys
//! (`crate::app::form`); the chart's own keys come after them.

use crate::ChartRequest;
use crate::app::feedback::Confirm;
use crate::app::form::FormKey;
use crate::chart::chart_export::{ChartExportFormat, ChartExportRequest};
use crate::chart::chart_export_modal::{ChartExportFocus, ExportDefaults};
use crate::chart::chart_modal::{ChartFocus, Mark};
use crate::chart::chart_plot::PlotData;
use crate::export::output_file::Overwrite;
use crate::logging::LogFailure;
use crate::widgets::crosshair::{self, Move};
use crate::{App, AppEvent, Overlay, home};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Keys in the chart view.
    pub(crate) fn chart_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let out = self.chart_key_inner(event);
        // A Rows change is read once focus leaves the row, however it left.
        if self.chart.modal.focus != ChartFocus::LimitRows || self.chart.modal.plot_focus {
            self.chart.modal.leave_rows();
        }
        out
    }

    fn chart_key_inner(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        if !event.is_press() {
            return None;
        }
        if self.overlay == Overlay::ChartExport {
            return self.chart_export_key(event);
        }

        if crate::app::form::picker_form_key(&mut self.chart.modal, event) {
            return None;
        }

        // The plot has the keys: ←→ step the crosshair, Home/End go to the ends, and x,
        // Tab, Shift+Tab or Esc return them to the panel. Global keys still act; the rest
        // do nothing (they would edit a row without a rail).
        if self.chart.modal.plot_focus {
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
                    self.chart.modal.plot_focus = false;
                    return None;
                }
                KeyCode::Char('1'..='7' | '[' | ']' | 'g' | 'e' | 't' | '?') => {}
                _ => return None,
            }
        }

        if self.chart.modal.focus == ChartFocus::LimitRows
            && !self.chart.modal.plot_focus
            && self.chart_rows_key(event)
        {
            return None;
        }

        // The panel applies as it changes, so Enter acts on the focused row like Space.
        match crate::app::form::key(&mut self.chart.modal, event) {
            FormKey::Cancel => {
                self.close_overlay();
                return None;
            }
            FormKey::Submit | FormKey::Act(_) => {
                self.chart_act(self.chart.modal.focus);
                return None;
            }
            FormKey::Step(row, delta) => {
                self.chart.modal.step(row, delta);
                return None;
            }
            FormKey::Moved | FormKey::Text(_) => return None,
            FormKey::Other => {}
        }

        match event.code {
            // The crosshair: the plot takes the keys.
            KeyCode::Char('x') if self.chart.modal.has_crosshair() => {
                self.chart.modal.plot_focus = true;
                self.move_crosshair_to(None);
            }
            // The type switches from anywhere (1-7, [ and ]): with the Picker closed, nothing
            // here types.
            KeyCode::Char(c @ '1'..='7') => {
                let idx = c as usize - '1' as usize;
                self.chart.modal.set_mark(Mark::ALL[idx]);
            }
            KeyCode::Char('[') => self.chart.modal.step_mark(-1),
            KeyCode::Char(']') => self.chart.modal.step_mark(1),
            // The grid at the major ticks, on the kinds that have axes.
            KeyCode::Char('g') if self.chart.modal.has_grid() => {
                self.chart.modal.toggle_grid();
            }
            KeyCode::Char('e') => {
                if self.data_table_state.is_some() && self.chart.modal.can_export() {
                    self.open_chart_export();
                }
            }
            // The chart keeps the rows it was drawn from; `t` draws the new ones too.
            KeyCode::Char('t') if self.follow_rows_waiting() => {
                self.take_follow_rows(false);
                self.chart.cache.clear();
            }

            KeyCode::Char('?') => self.open_help_overlay(),
            KeyCode::Char('+') | KeyCode::Char('=') => self.chart.modal.adjust_number_row(1),
            KeyCode::Char('-') => self.chart.modal.adjust_number_row(-1),
            _ => {}
        }
        None
    }

    /// The Rows row's keys: digits type a sample size (`50k`, `2m`), Backspace edits,
    /// Enter reads, Esc reverts a pending change instead of closing. Returns whether
    /// the key was the row's.
    fn chart_rows_key(&mut self, event: &KeyEvent) -> bool {
        let plain = !event
            .modifiers
            .intersects(KeyModifiers::CONTROL | KeyModifiers::ALT);
        let modal = &mut self.chart.modal;
        let typing = modal.typing_rows();
        match event.code {
            KeyCode::Char(c)
                if plain
                    && (c.is_ascii_digit()
                        || (typing && matches!(c, 'k' | 'K' | 'm' | 'M' | ',' | '_' | '.'))) =>
            {
                modal.type_rows(c);
            }
            KeyCode::Backspace if typing => modal.backspace_rows(),
            KeyCode::Esc if modal.rows_draft.is_some() => {
                // A draft back where it started holds nothing to undo: Esc closes.
                let pending = modal.rows_pending();
                modal.discard_rows();
                if !pending {
                    return false;
                }
            }
            KeyCode::Enter => {
                modal.commit_rows();
            }
            _ => return false,
        }
        true
    }

    /// Space (or Enter) on a panel row: a shelf opens its Picker, a toggle flips, a
    /// stepped row takes its next value.
    fn chart_act(&mut self, focus: ChartFocus) {
        if self.chart.modal.picker_for(focus).is_some() {
            self.chart.modal.open_picker();
        } else {
            self.chart.modal.step(focus, 1);
        }
    }

    /// Open the export dialog, its text started from the chart: how it was made and
    /// where its data comes from. The figure names Y at its axis.
    fn open_chart_export(&mut self) {
        let description = sentence_case(&self.chart.modal.how());
        self.chart.export_modal.open(
            &self.theme,
            self.display.history_limit,
            ExportDefaults {
                description,
                source: self
                    .info
                    .catalog_entry
                    .as_ref()
                    .map(|(_, entry)| entry.credit())
                    .unwrap_or_default(),
                legend: self.chart.modal.show_legend,
                mark: self.chart.modal.mark(),
                y_from_zero: self.chart.modal.y_starts_at_zero,
            },
        );
        self.open_overlay(Overlay::ChartExport);
    }

    /// Keys in the chart's export dialog.
    fn chart_export_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        match crate::app::form::key(&mut self.chart.export_modal, event) {
            FormKey::Cancel => self.close_overlay(),
            FormKey::Submit => return self.submit_chart_export(),
            FormKey::Step(field, delta) => self.chart.export_modal.step(field, delta),
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
                if field == ChartExportFocus::PathInput {
                    // Typing is the correction the message asked for.
                    self.chart.export_modal.error = None;
                }
                if allowed && let Some(input) = self.chart.export_modal.input_mut(field) {
                    let _ = input.handle_key(event, Some(&self.cache));
                    if size {
                        self.chart.export_modal.size_typed();
                    }
                }
            }
            _ => {}
        }
        None
    }

    /// Enter from any field of the chart export dialog: build the export. A blank path
    /// says so inline.
    fn submit_chart_export(&mut self) -> Option<AppEvent> {
        let modal = &self.chart.export_modal;
        let path_str = modal.path_input.value().trim();
        if path_str.is_empty() {
            self.chart.export_modal.error = Some("Enter a file path.".to_string());
            crate::app::form::Form::focus(
                &mut self.chart.export_modal,
                ChartExportFocus::PathInput,
            );
            return None;
        }
        // `~` and `$VAR` expand as everywhere else a path is typed.
        let mut path = home::expand_user_path(path_str);
        // A path naming a format takes it; one naming none gets the format's extension.
        let format = match ChartExportFormat::from_extension(&path) {
            Some(format) => format,
            // Any other ending is part of the name (`chart.v2`): the format's extension goes
            // after it.
            None => {
                let format = modal.format;
                let mut name = path.into_os_string();
                name.push(".");
                name.push(format.extension());
                path = name.into();
                format
            }
        };
        let (width, height) = modal.export_dimensions();
        let recipe = modal.recipe;
        let options = crate::chart::chart_export::ExportOptions {
            width,
            height,
            dpi: modal.dpi,
            palette: crate::chart::chart_export::Palette::for_style(
                modal.style,
                &self.app_config.theme.colors,
            ),
            legend: modal.legend,
            title: modal.title_input.value().trim().to_string(),
            description: modal.description_input.value().trim().to_string(),
            notes: modal.notes_input.value().trim().to_string(),
            source: modal.source_input.value().trim().to_string(),
            byline: modal.byline_input.value().trim().to_string(),
            point_opacity: modal.point_opacity,
            point_size: modal.point_size,
            line_width: modal.line_width,
            y_from_zero: modal.y_from_zero_option(),
            // Written in when the export starts, from the chart as it is then.
            recipe: None,
        };
        self.chart
            .export_modal
            .path_input
            .save_to_history(&self.cache)
            .or_log("save the chart export path");
        let path_display = path.display().to_string();
        let request = ChartExportRequest {
            path,
            format,
            options,
            overwrite: Overwrite::Forbid,
            recipe,
        };
        if request.path.exists() {
            // Suspended, not closed: declining returns to the filled form.
            self.step_back();
            self.confirmation_modal.show_destructive(
                format!("File already exists:\n{path_display}\n\nOverwrite it?"),
                "Overwrite",
                Confirm::ChartExport(Box::new(request)),
            );
            return None;
        }
        self.step_back();
        Some(AppEvent::ChartExport(request))
    }

    /// Every X of the line or scatter series on screen, in order, each once.
    fn chart_xs(&self) -> Option<&[f64]> {
        let request = ChartRequest::from_modal(&self.chart.modal)?;
        match self.chart.cache.prepared(&request)? {
            PlotData::Lines(xy) => Some(&xy.xs),
            _ => None,
        }
    }

    /// Step the crosshair, from where it stands or the middle of the plot.
    fn move_crosshair(&mut self, to: Move) {
        let (Some(place), Some(xs)) = (self.chart.modal.plot, self.chart_xs()) else {
            return;
        };
        let from = self
            .chart
            .modal
            .cursor_x
            .or_else(|| crosshair::at_column(xs, &place, middle(place.graph)));
        let to = from.and_then(|from| crosshair::step(xs, &place, from, to));
        if to.is_some() {
            self.chart.modal.cursor_x = to;
        }
    }

    /// Put the crosshair on the point nearest `column`; with none, on the point nearest
    /// where it stood, or mid-plot.
    pub(crate) fn move_crosshair_to(&mut self, column: Option<u16>) {
        let (Some(place), Some(xs)) = (self.chart.modal.plot, self.chart_xs()) else {
            return;
        };
        let at = match (column, self.chart.modal.cursor_x) {
            (Some(column), _) => crosshair::at_column(xs, &place, column),
            (None, Some(x)) => crosshair::nearest(xs, x),
            (None, None) => crosshair::at_column(xs, &place, middle(place.graph)),
        };
        if at.is_some() {
            self.chart.modal.cursor_x = at;
        }
    }
}

/// `phrase` with its first letter capitalized, to start a sentence.
fn sentence_case(phrase: &str) -> String {
    let mut chars = phrase.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().chain(chars).collect(),
        None => String::new(),
    }
}

/// The middle column of `area`.
fn middle(area: ratatui::layout::Rect) -> u16 {
    area.left() + area.width / 2
}
