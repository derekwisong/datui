//! What is open over the table: one dialog, panel or view at a time, and the one way
//! back from it.

use crate::{App, InputMode};

/// The dialog, panel or view over the table. The one place that says which is open:
/// keys go to it, and it is drawn over the table.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub enum Overlay {
    /// Nothing: the table, the query line or home, as `input_mode` says.
    #[default]
    None,
    SortFilter,
    PivotMelt,
    Export,
    /// The copy dialog.
    Copy,
    /// The row inspector.
    Inspect,
    /// The column picker: type a column's name to go to it.
    GoToColumn,
    /// The format picker over a table read through a spec: read it with another.
    PickFormat,
    /// A column's type, picked from the Info panel's Schema tab or the cell menu.
    Retype,
    /// A datetime made from columns, as a spec's derived column.
    Combine,
    /// The table picker over a table of a file of several: open another.
    PickTable,
    Info,
    Chart,
    /// Value Counts: how often each value of one column occurs in the view.
    ValueCounts,
    /// The hex view: a file's bytes.
    Hex,
    /// The Sample form (`S`).
    Sample,
}

impl App {
    /// Open `overlay`. It is over the table: the query line, or home, is left.
    pub(crate) fn open_overlay(&mut self, overlay: Overlay) {
        self.input_mode = InputMode::Normal;
        self.overlay = overlay;
    }

    /// Close the overlay and go back to the table.
    pub(crate) fn close_overlay(&mut self) {
        match self.overlay {
            Overlay::Copy => self.copy_modal.close(),
            Overlay::Info => self.info_modal.close(),
            Overlay::Inspect => self.inspector_modal.close(),
            Overlay::SortFilter => self.sort_filter_modal.close(),
            Overlay::PivotMelt => self.pivot_melt_modal.close(),
            Overlay::Export => self.export_modal.close(),
            Overlay::Sample => self.sample.form = None,
            _ => {}
        }
        self.overlay = Overlay::None;
    }

    /// The plain table or the query line: no overlay, and not home.
    pub fn at_table(&self) -> bool {
        self.overlay == Overlay::None && self.input_mode == InputMode::Normal
    }
}
