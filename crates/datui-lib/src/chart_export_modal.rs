//! Chart export modal: format, file path, optional chart title, and size. Used from the
//! chart view only; its keys are the shared form keys (`crate::form`).

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
        // Ctrl+P / Ctrl+N recall the paths exported to before.
        self.path_input = TextInput::new()
            .with_history("chart_export_path".to_string())
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.path_input.clear();
        self.width_input = TextInput::new().with_theme(theme);
        self.width_input.suggest("1024");
        self.height_input = TextInput::new().with_theme(theme);
        self.height_input.suggest("768");
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

    /// Hide behind a child confirmation without discarding the form; `resume`
    /// brings it back exactly as typed. `close` is the discard.
    pub fn suspend(&mut self) {
        self.active = false;
    }

    pub fn resume(&mut self) {
        self.active = true;
    }

    /// Step the format through what a chart exports as.
    pub fn step_format(&mut self, delta: i8) {
        self.selected_format =
            crate::form::step_value(&ChartExportFormat::ALL, self.selected_format, delta);
    }

    /// The focused field's input, for a key the form hands it.
    pub fn focused_input_mut(&mut self) -> Option<&mut TextInput> {
        match self.focus {
            ChartExportFocus::FormatSelector => None,
            ChartExportFocus::PathInput => Some(&mut self.path_input),
            ChartExportFocus::TitleInput => Some(&mut self.title_input),
            ChartExportFocus::WidthInput => Some(&mut self.width_input),
            ChartExportFocus::HeightInput => Some(&mut self.height_input),
        }
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

impl crate::form::Form for ChartExportModal {
    type Field = ChartExportFocus;

    fn fields(&self) -> Vec<(ChartExportFocus, crate::form::FieldKind)> {
        use crate::form::FieldKind::{Choice, Text};
        vec![
            (ChartExportFocus::FormatSelector, Choice),
            (ChartExportFocus::PathInput, Text),
            (ChartExportFocus::TitleInput, Text),
            (ChartExportFocus::WidthInput, Text),
            (ChartExportFocus::HeightInput, Text),
        ]
    }

    fn focused(&self) -> ChartExportFocus {
        self.focus
    }

    fn set_focused(&mut self, field: ChartExportFocus) {
        self.focus = field;
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
