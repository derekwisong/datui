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
    /// The export dialog, over the table or Value Counts.
    Export {
        returns_to: Box<Overlay>,
    },
    /// The copy dialog.
    Copy,
    /// The row inspector.
    Inspect,
    /// The column picker: type a column's name to go to it.
    GoToColumn,
    /// The format picker over a table read through a spec: read it with another.
    PickFormat,
    /// A column's type, picked from the Info panel's Schema tab or the cell menu.
    Retype {
        returns_to: Box<Overlay>,
    },
    /// A datetime made from columns, as a spec's derived column.
    Combine {
        returns_to: Box<Overlay>,
    },
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

impl Overlay {
    /// What closing this overlay goes back to.
    fn back(self) -> Overlay {
        match self {
            Overlay::Export { returns_to }
            | Overlay::Retype { returns_to }
            | Overlay::Combine { returns_to } => *returns_to,
            _ => Overlay::None,
        }
    }

    /// Whether `overlay` is open: this one, or one under it that it goes back to,
    /// which is drawn beneath it.
    pub fn shows(&self, overlay: &Overlay) -> bool {
        self == overlay
            || match self {
                Overlay::Export { returns_to }
                | Overlay::Retype { returns_to }
                | Overlay::Combine { returns_to } => returns_to.shows(overlay),
                _ => false,
            }
    }
}

impl App {
    /// Open `overlay`. It is over the table: the query line, or home, is left.
    pub(crate) fn open_overlay(&mut self, overlay: Overlay) {
        self.input_mode = InputMode::Normal;
        self.overlay = overlay;
    }

    /// Open an overlay over the one open now, which closing it goes back to.
    pub(crate) fn open_over(&mut self, overlay: impl FnOnce(Box<Overlay>) -> Overlay) {
        let under = std::mem::take(&mut self.overlay);
        self.open_overlay(overlay(Box::new(under)));
    }

    /// Close the overlay, dropping what it held for this opening, and go back to
    /// what it was opened over.
    pub(crate) fn close_overlay(&mut self) {
        match self.overlay {
            Overlay::Copy => self.copy_modal.close(),
            Overlay::Info => self.info_modal.close(),
            Overlay::Inspect => self.inspector_modal.close(),
            Overlay::SortFilter => self.sort_filter_modal.close(),
            Overlay::PivotMelt => self.pivot_melt_modal.close(),
            Overlay::Export { .. } => self.forget_export(),
            Overlay::Retype { .. } | Overlay::Combine { .. } => {
                self.column_forms.retype = None;
                self.column_forms.combine = None;
            }
            Overlay::Sample => self.sample.form = None,
            _ => {}
        }
        self.step_back();
    }

    /// Go back to what the overlay was opened over, keeping what it holds: it waits
    /// behind a question, or while it runs, to come back as it was.
    pub(crate) fn step_back(&mut self) {
        self.overlay = std::mem::take(&mut self.overlay).back();
    }

    /// The export dialog's form and the counts it was to write, put down.
    pub(crate) fn forget_export(&mut self) {
        self.export_modal.close();
        self.export_counts = None;
    }

    /// The plain table or the query line: no overlay, and not home.
    pub fn at_table(&self) -> bool {
        self.overlay == Overlay::None && self.input_mode == InputMode::Normal
    }
}
