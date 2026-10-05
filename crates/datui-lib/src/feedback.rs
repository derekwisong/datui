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
/// the control bar, cleared by the next keypress or after two seconds,
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
        }
    }
}

impl ConfirmationModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn show(&mut self, message: String) {
        self.active = true;
        self.message = message;
        self.focus_yes = true; // Default to Yes
        self.yes_label = "Yes";
        self.no_label = "No";
        self.scroll = 0;
    }

    /// A choice between two things to do, each named; Esc does neither.
    pub fn show_choice(
        &mut self,
        message: String,
        yes_label: &'static str,
        no_label: &'static str,
    ) {
        self.show(message);
        self.yes_label = yes_label;
        self.no_label = no_label;
    }

    /// A confirmation whose Yes destroys something: it starts on No, so a
    /// reflexive second Enter declines, and the action is named on the choice.
    pub fn show_destructive(&mut self, message: String, yes_label: &'static str) {
        self.active = true;
        self.message = message;
        self.focus_yes = false;
        self.yes_label = yes_label;
        self.no_label = "No";
        self.scroll = 0;
    }

    pub fn hide(&mut self) {
        self.active = false;
        self.message.clear();
        self.focus_yes = true;
        self.yes_label = "Yes";
        self.no_label = "No";
        self.scroll = 0;
    }
}
