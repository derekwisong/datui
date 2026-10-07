//! The Pivot & Melt builder's keys: the shared form keys (`crate::form`), then what
//! each row does with them; and its live preview, rerun as the spec changes.

use crate::form::FormKey;
use crate::jobs::{Answer, Job};
use crate::pivot_melt_modal::{
    PREVIEW_INPUT_ROWS, PivotMeltFocus, PivotMeltTab, PreviewFrame, PreviewInput,
};
use crate::{App, AppEvent, Overlay};
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};

impl App {
    /// Open the builder over the view's columns, and preview what it stages.
    pub(crate) fn open_pivot_builder(&mut self) {
        let Some(state) = &self.data_table_state else {
            return;
        };
        let modal = &mut self.pivot_melt_modal;
        modal.available_columns = state.schema().iter_names().map(|s| s.to_string()).collect();
        modal.column_dtypes = state
            .schema()
            .iter()
            .map(|(n, d)| (n.to_string(), d.clone()))
            .collect();
        let view_rows = state.num_rows_if_valid();
        let sorted = state.is_sorted();
        modal.open(self.display.history_limit, &self.theme);
        modal.preview.view_rows = view_rows;
        modal.preview.sorted = sorted;
        self.open_overlay(Overlay::PivotMelt);
        self.request_reshape_preview();
    }

    /// Keys in the Pivot & Melt builder. Whatever the key changed, the preview
    /// follows the spec it leaves staged.
    pub(crate) fn pivot_melt_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        let out = self.pivot_melt_form_key(event);
        if self.overlay == Overlay::PivotMelt {
            self.request_reshape_preview();
        }
        out
    }

    /// Ask for a preview of the staged spec, unless it is the one already asked for.
    /// One worker runs at a time: an edit made while it runs is previewed when it
    /// ends.
    pub fn request_reshape_preview(&mut self) {
        let modal = &mut self.pivot_melt_modal;
        let wanted = modal.staged_spec();
        let preview = &mut modal.preview;
        if wanted == preview.wanted {
            return;
        }
        preview.token = preview.token.wrapping_add(1);
        preview.wanted = wanted;
        if preview.running.is_none() {
            self.spawn_reshape_preview();
        }
    }

    /// Run the preview of the staged spec over the view's head, reading the head
    /// first if this opening of the builder has not yet.
    fn spawn_reshape_preview(&mut self) {
        let preview = &self.pivot_melt_modal.preview;
        let Some(spec) = preview.wanted.clone() else {
            return;
        };
        let (epoch, token) = (preview.epoch, preview.token);
        let input = preview.input.clone();
        let read = match &input {
            Some(_) => None,
            None => {
                let Some(state) = self.data_table_state.as_ref() else {
                    // Nothing to read: say so rather than wait on an answer.
                    self.pivot_melt_modal.preview.shown =
                        Some((spec, Err("Nothing to preview".to_string())));
                    return;
                };
                // A small view is read in its order: sorting it costs nothing, and
                // the preview's rows are then the ones Enter makes.
                let small = state
                    .num_rows_if_valid()
                    .is_some_and(|rows| rows <= PREVIEW_INPUT_ROWS);
                let sorted = (state.is_sorted() && !small).then(|| state.visible_lf());
                let lf = if small {
                    state.visible_lf()
                } else {
                    state.preview_lf()
                };
                Some((lf, sorted, state.polars_streaming()))
            }
        };
        self.pivot_melt_modal.preview.running = Some(token);
        self.spawn_job(Job::ReshapePreview { epoch, token }, None, move |_| {
            let (input, read) = match (input, read) {
                (Some(input), _) => (input, None),
                (None, Some((lf, sorted, streaming))) => {
                    let message = |e: polars::prelude::PolarsError| {
                        crate::error_display::user_message_from_polars(&e)
                    };
                    // One more than the preview takes says whether there is more.
                    let head_of = |lf: polars::prelude::LazyFrame| {
                        crate::statistics::collect_lazy(
                            lf.slice(0, PREVIEW_INPUT_ROWS as u32 + 1),
                            streaming,
                        )
                    };
                    let mut head = head_of(lf).map_err(message)?;
                    // The unsorted head turned out to be the whole view: small enough
                    // to read again in the view's order.
                    if head.height() <= PREVIEW_INPUT_ROWS
                        && let Some(sorted) = sorted
                    {
                        head = head_of(sorted).map_err(message)?;
                    }
                    let input = PreviewInput {
                        whole: head.height() <= PREVIEW_INPUT_ROWS,
                        rows: std::sync::Arc::new(head.head(Some(PREVIEW_INPUT_ROWS))),
                    };
                    (input.clone(), Some(input))
                }
                (None, None) => return Err("Nothing to preview".to_string()),
            };
            let result = crate::pivot_melt_modal::run_preview(&input.rows, &spec);
            Ok(Answer::ReshapePreviewed {
                input: read,
                result,
            })
        });
    }

    /// A preview's worker ended, with the head it read and its result, or `Err` with
    /// why the head could not be read. An answer for an earlier opening is dropped.
    pub(crate) fn reshape_preview_ended(
        &mut self,
        epoch: u64,
        token: u64,
        input: Option<PreviewInput>,
        result: Result<PreviewFrame, String>,
    ) {
        let preview = &mut self.pivot_melt_modal.preview;
        if self.overlay != Overlay::PivotMelt || preview.epoch != epoch {
            return;
        }
        if preview.running == Some(token) {
            preview.running = None;
        }
        if preview.input.is_none() {
            preview.input = input;
        }
        if token == preview.token {
            if let Some(spec) = preview.wanted.clone() {
                preview.shown = Some((spec, result));
            }
        } else if preview.running.is_none() && preview.stale() {
            // The spec changed while this one ran: preview what is staged now.
            self.spawn_reshape_preview();
        }
    }

    /// Whether the builder's preview has an answer still to come.
    pub fn reshape_preview_pending(&self) -> bool {
        self.overlay == Overlay::PivotMelt && self.pivot_melt_modal.preview.running.is_some()
    }

    /// Keys in the builder's form.
    fn pivot_melt_form_key(&mut self, event: &KeyEvent) -> Option<AppEvent> {
        // Acts at once (see `hard_escape_while_busy`), ahead of the keys held
        // behind the pivot; a second Esc closes the form.
        if event.code == KeyCode::Esc && self.pivot_computing() {
            self.cancel_pivot();
            return None;
        }
        if event.code == KeyCode::Char('?') && event.modifiers.contains(KeyModifiers::CONTROL) {
            self.open_help_overlay();
            return None;
        }

        if crate::form::picker_form_key(&mut self.pivot_melt_modal, event) {
            return None;
        }

        // Whatever this key does, the form is being edited again: the
        // re-accented gap line goes back to plain (Enter below re-arms it).
        self.pivot_melt_modal.attention = false;

        match crate::form::key(&mut self.pivot_melt_modal, event) {
            FormKey::Cancel => {
                self.close_overlay();
            }
            // Enter applies from anywhere in the form; what it will do has
            // been echoed on the spec line all along.
            FormKey::Submit => return self.submit_pivot_melt(),
            FormKey::Step(PivotMeltFocus::TabBar, _) => self.pivot_melt_modal.switch_tab(),
            FormKey::Step(row, delta) => {
                if self.pivot_melt_modal.is_choice_row(row) {
                    self.pivot_melt_modal.step_choice(delta);
                } else {
                    self.pivot_melt_modal.step_picker_row(delta);
                }
            }
            // A column row edits through the Picker scoped to that row alone.
            FormKey::Act(_) => self.pivot_melt_modal.open_picker(),
            // A text row is an ordinary text field, readline included.
            FormKey::Text(_) => {
                if let Some(input) = self.pivot_melt_modal.focused_text_input_mut() {
                    let _ = input.handle_key(event, None);
                }
            }
            FormKey::Other if event.code == KeyCode::Char('?') => self.open_help_overlay(),
            FormKey::Moved | FormKey::Other => {}
        }
        None
    }

    /// Enter: run the staged pivot or melt, or re-accent the spec line that names
    /// what is missing rather than a modal repeating it.
    fn submit_pivot_melt(&mut self) -> Option<AppEvent> {
        let modal = &mut self.pivot_melt_modal;
        match modal.active_tab {
            PivotMeltTab::Pivot => {
                if modal.pivot_validation_error().is_some() {
                    modal.attention = true;
                    None
                } else {
                    modal.build_pivot_spec().map(AppEvent::Pivot)
                }
            }
            PivotMeltTab::Melt => {
                if modal.melt_validation_error().is_some() {
                    modal.attention = true;
                    None
                } else {
                    modal.build_melt_spec().map(AppEvent::Melt)
                }
            }
        }
    }
}
