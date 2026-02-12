//! Chart export modal: format (PNG/EPS), optional chart title, and file path. Used from Chart view only.

use crate::chart_export::ChartExportFormat;
use crate::widgets::text_input::TextInput;
use std::path::Path;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartExportFocus {
    #[default]
    FormatSelector,
    PathInput,
    TitleInput,
    WidthInput,
    HeightInput,
    ExportButton,
    CancelButton,
}

pub struct ChartExportModal {
    pub active: bool,
    pub focus: ChartExportFocus,
    pub selected_format: ChartExportFormat,
    pub title_input: TextInput,
    pub path_input: TextInput,
    pub width_input: TextInput,
    pub height_input: TextInput,
}

impl ChartExportModal {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn open(&mut self, theme: &crate::config::Theme, history_limit: usize) {
        self.active = true;
        self.focus = ChartExportFocus::PathInput;
        self.title_input = TextInput::new().with_theme(theme);
        self.title_input.clear();
        self.path_input = TextInput::new()
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.path_input.clear();
        self.width_input = TextInput::new().with_theme(theme);
        self.width_input.set_value("1024".to_string());
        self.height_input = TextInput::new().with_theme(theme);
        self.height_input.set_value("768".to_string());
    }

    /// Reopen the modal with path pre-filled (e.g. after cancel overwrite or export error). Focus is PathInput.
    pub fn reopen_with_path(&mut self, path: &Path, format: ChartExportFormat) {
        self.active = true;
        self.focus = ChartExportFocus::PathInput;
        self.selected_format = format;
        self.title_input.clear();
        self.path_input.set_value(path.display().to_string());
    }

    pub fn close(&mut self) {
        self.active = false;
        self.focus = ChartExportFocus::FormatSelector;
        self.title_input.clear();
        self.path_input.clear();
    }

    pub fn next_focus(&mut self) {
        self.focus = match self.focus {
            ChartExportFocus::FormatSelector => ChartExportFocus::PathInput,
            ChartExportFocus::PathInput => ChartExportFocus::TitleInput,
            ChartExportFocus::TitleInput => ChartExportFocus::WidthInput,
            ChartExportFocus::WidthInput => ChartExportFocus::HeightInput,
            ChartExportFocus::HeightInput => ChartExportFocus::ExportButton,
            ChartExportFocus::ExportButton => ChartExportFocus::CancelButton,
            ChartExportFocus::CancelButton => ChartExportFocus::FormatSelector,
        };
    }

    pub fn prev_focus(&mut self) {
        self.focus = match self.focus {
            ChartExportFocus::FormatSelector => ChartExportFocus::CancelButton,
            ChartExportFocus::PathInput => ChartExportFocus::FormatSelector,
            ChartExportFocus::TitleInput => ChartExportFocus::PathInput,
            ChartExportFocus::WidthInput => ChartExportFocus::TitleInput,
            ChartExportFocus::HeightInput => ChartExportFocus::WidthInput,
            ChartExportFocus::ExportButton => ChartExportFocus::HeightInput,
            ChartExportFocus::CancelButton => ChartExportFocus::ExportButton,
        };
    }

    /// Parse width/height from inputs; default to 1024x768 on parse error, clamped to 1..=8192.
    pub fn export_dimensions(&self) -> (u32, u32) {
        const MIN: u32 = 1;
        const MAX: u32 = 8192;
        const DEFAULT_W: u32 = 1024;
        const DEFAULT_H: u32 = 768;
        let w = self
            .width_input
            .value()
            .trim()
            .parse::<u32>()
            .ok()
            .map(|n| n.clamp(MIN, MAX))
            .unwrap_or(DEFAULT_W);
        let h = self
            .height_input
            .value()
            .trim()
            .parse::<u32>()
            .ok()
            .map(|n| n.clamp(MIN, MAX))
            .unwrap_or(DEFAULT_H);
        (w, h)
    }
}

impl Default for ChartExportModal {
    fn default() -> Self {
        Self {
            active: false,
            focus: ChartExportFocus::FormatSelector,
            selected_format: ChartExportFormat::Png,
            title_input: TextInput::new(),
            path_input: TextInput::new(),
            width_input: TextInput::new(),
            height_input: TextInput::new(),
        }
    }
}
