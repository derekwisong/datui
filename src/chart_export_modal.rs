//! Chart export modal: format (PNG/EPS) and path. Used from Chart view only.

use crate::chart_export::ChartExportFormat;
use crate::widgets::text_input::TextInput;
use std::path::Path;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartExportFocus {
    #[default]
    FormatSelector,
    PathInput,
    ExportButton,
    CancelButton,
}

pub struct ChartExportModal {
    pub active: bool,
    pub focus: ChartExportFocus,
    pub selected_format: ChartExportFormat,
    pub path_input: TextInput,
}

impl ChartExportModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self, theme: &crate::config::Theme, history_limit: usize) {
        self.active = true;
        self.focus = ChartExportFocus::PathInput;
        self.path_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.path_input.clear();
    }

    /// Reopen the modal with path pre-filled (e.g. after cancel overwrite or export error). Focus is PathInput.
    pub fn reopen_with_path(&mut self, path: &Path, format: ChartExportFormat) {
        self.active = true;
        self.focus = ChartExportFocus::PathInput;
        self.selected_format = format;
        self.path_input.set_value(path.display().to_string());
    }

    pub fn close(&mut self) {
        self.active = false;
        self.focus = ChartExportFocus::FormatSelector;
        self.path_input.clear();
    }

    pub fn next_focus(&mut self) {
        self.focus = match self.focus {
            ChartExportFocus::FormatSelector => ChartExportFocus::PathInput,
            ChartExportFocus::PathInput => ChartExportFocus::ExportButton,
            ChartExportFocus::ExportButton => ChartExportFocus::CancelButton,
            ChartExportFocus::CancelButton => ChartExportFocus::FormatSelector,
        };
    }

    pub fn prev_focus(&mut self) {
        self.focus = match self.focus {
            ChartExportFocus::FormatSelector => ChartExportFocus::CancelButton,
            ChartExportFocus::PathInput => ChartExportFocus::FormatSelector,
            ChartExportFocus::ExportButton => ChartExportFocus::PathInput,
            ChartExportFocus::CancelButton => ChartExportFocus::ExportButton,
        };
    }
}

impl Default for ChartExportModal {
    fn default() -> Self {
        Self {
            active: false,
            focus: ChartExportFocus::FormatSelector,
            selected_format: ChartExportFormat::Png,
            path_input: TextInput::new(),
        }
    }
}
