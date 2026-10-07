//! The overlay `input_mode` names over the table, and the one way back from it.

use crate::{App, InputMode};

impl App {
    /// Close the dialog or panel `input_mode` names and go back to the table.
    pub(crate) fn close_overlay(&mut self) {
        match self.input_mode {
            InputMode::Copy => self.copy_modal.close(),
            InputMode::Info => self.info_modal.close(),
            InputMode::Inspect => self.inspector_modal.close(),
            InputMode::SortFilter => self.sort_filter_modal.close(),
            InputMode::PivotMelt => self.pivot_melt_modal.close(),
            InputMode::Export => self.export_modal.close(),
            InputMode::Sample => self.sample.form = None,
            _ => {}
        }
        self.input_mode = InputMode::Normal;
    }
}
