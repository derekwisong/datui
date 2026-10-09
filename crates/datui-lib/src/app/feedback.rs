//! What the user is told: the error modal, the completion flash, and the confirmation
//! modal.

#[derive(Default)]
pub struct ErrorModal {
    pub active: bool,
    pub message: String,
    /// How far a long message is scrolled; the render clamps it.
    pub scroll: usize,
}

impl ErrorModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, message: String) {
        // Every failure the user is shown, background work's included, so a bug
        // report has the message after the modal is gone.
        log::error!(target: "datui", "{message}");
        self.active = true;
        self.message = message;
        self.scroll = 0;
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
        self.scroll = 0;
    }
}

/// A completion flash (the Feedback rules' second rung): one plain sentence on
/// the footer, cleared by the next keypress or after two seconds,
/// whichever comes first. Every screen's completions go here, the home screen's
/// included. The home screen's own status line beside the filter is for what a
/// key could not do and why, which has to survive until it is read.
pub struct Flash {
    pub message: String,
    /// Where a path the message ends in starts: cut short, the path loses its
    /// beginning rather than its file name.
    pub(crate) path_from: Option<usize>,
    pub(crate) expires: std::time::Instant,
}

impl Flash {
    pub(crate) fn new(message: String) -> Self {
        Self {
            message,
            path_from: None,
            expires: std::time::Instant::now() + std::time::Duration::from_secs(2),
        }
    }

    /// `prefix` then `path`, as in `Exported to /data/out.csv`.
    pub(crate) fn path(prefix: &str, path: &std::path::Path) -> Self {
        let mut flash = Self::new(format!("{prefix}{}", path.display()));
        flash.path_from = Some(prefix.len());
        flash
    }

    pub(crate) fn expired(&self) -> bool {
        std::time::Instant::now() >= self.expires
    }
}

/// What a confirmation's Yes does. One question is up at a time, so one of these is
/// armed at a time, and closing the question disarms it.
pub(crate) enum Confirm {
    /// Read every row, as the sample.
    ReadAll,
    OpenLink(String),
    ClearRecents,
    /// Run Setup's full scan, past the question.
    QualityFullScan,
    HideExamples,
    DeleteView(String),
    ForgetPlace(std::path::PathBuf),
    /// Overwrite the file the report's export asked about.
    QualityExport(
        std::path::PathBuf,
        crate::analysis::quality_export::ReportFormat,
    ),
    ChartExport(Box<crate::chart::chart_export::ChartExportRequest>),
    Export(Box<crate::ExportRequest>),
    Copy(crate::clipboard::CopyFormat, bool),
    /// Download a remote file, or read a large one whole: the open in flight asks.
    Download,
    /// Read the dataset again, where it was (or where the failed read was going): a
    /// file it listed is gone.
    Reopen(Option<Box<crate::loading::open_options::KeptPlace>>),
    /// Stop a recording or keep it, on the way out; either choice leaves.
    Leave(crate::Leaving),
}

pub struct ConfirmationModal {
    pub active: bool,
    pub message: String,
    pub focus_yes: bool, // true = Yes focused, false = No focused
    /// What Enter-on-Yes does, named: "Overwrite", not a generic "Yes".
    pub yes_label: &'static str,
    /// What Enter-on-No does: "No", unless declining does something of its own.
    pub no_label: &'static str,
    /// How far a long message is scrolled; the render clamps it.
    pub scroll: usize,
    /// What is being asked about.
    pub(crate) asking: Option<Confirm>,
}

impl Default for ConfirmationModal {
    fn default() -> Self {
        Self {
            active: false,
            message: String::new(),
            focus_yes: true,
            yes_label: "Yes",
            no_label: "No",
            scroll: 0,
            asking: None,
        }
    }
}

impl ConfirmationModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub(crate) fn show(&mut self, message: String, asking: Confirm) {
        self.active = true;
        self.message = message;
        self.focus_yes = true; // Default to Yes
        self.yes_label = "Yes";
        self.no_label = "No";
        self.scroll = 0;
        self.asking = Some(asking);
    }

    /// A choice between two things to do, each named; Esc does neither.
    pub(crate) fn show_choice(
        &mut self,
        message: String,
        yes_label: &'static str,
        no_label: &'static str,
        asking: Confirm,
    ) {
        self.show(message, asking);
        self.yes_label = yes_label;
        self.no_label = no_label;
    }

    /// A confirmation whose Yes destroys something: it starts on No, so a
    /// reflexive second Enter declines, and the action is named on the choice.
    pub(crate) fn show_destructive(
        &mut self,
        message: String,
        yes_label: &'static str,
        asking: Confirm,
    ) {
        self.show(message, asking);
        self.focus_yes = false;
        self.yes_label = yes_label;
    }

    /// Close the question, and hand back what it was asking about.
    pub(crate) fn take(&mut self) -> Option<Confirm> {
        let asking = self.asking.take();
        self.hide();
        asking
    }

    /// Whether the question up is the full-scan one Setup's Run asks.
    pub fn asks_full_scan(&self) -> bool {
        self.active && matches!(self.asking, Some(Confirm::QualityFullScan))
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
        self.focus_yes = true;
        self.yes_label = "Yes";
        self.no_label = "No";
        self.scroll = 0;
        self.asking = None;
    }
}
