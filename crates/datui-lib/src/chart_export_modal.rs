//! Chart export dialog: where, in what format, style and size, with what legend,
//! and the words around the chart. Its keys are the shared form keys
//! (`crate::form`).

use crate::chart_export::{ChartExportFormat, ExportStyle, LegendPlace, SizePreset};
use crate::widgets::text_input::TextInput;
use std::path::Path;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum ChartExportFocus {
    #[default]
    PathInput,
    Format,
    Style,
    Size,
    WidthInput,
    HeightInput,
    Legend,
    TitleInput,
    DescriptionInput,
    NotesInput,
    SourceInput,
    BylineInput,
}

impl ChartExportFocus {
    /// The field's label.
    pub fn label(self) -> &'static str {
        match self {
            Self::PathInput => "Path:",
            Self::Format => "Format:",
            Self::Style => "Style:",
            Self::Size => "Size:",
            Self::WidthInput => "Width:",
            Self::HeightInput => "Height:",
            Self::Legend => "Legend:",
            Self::TitleInput => "Title:",
            Self::DescriptionInput => "Description:",
            Self::NotesInput => "Notes:",
            Self::SourceInput => "Source:",
            Self::BylineInput => "Byline:",
        }
    }
}

/// The fields in order, top to bottom.
pub const FIELDS: [ChartExportFocus; 12] = [
    ChartExportFocus::PathInput,
    ChartExportFocus::Format,
    ChartExportFocus::Style,
    ChartExportFocus::Size,
    ChartExportFocus::WidthInput,
    ChartExportFocus::HeightInput,
    ChartExportFocus::Legend,
    ChartExportFocus::TitleInput,
    ChartExportFocus::DescriptionInput,
    ChartExportFocus::NotesInput,
    ChartExportFocus::SourceInput,
    ChartExportFocus::BylineInput,
];

/// What the chart being exported brings to the dialog.
#[derive(Debug, Clone, Default)]
pub struct ExportDefaults {
    /// What the chart is: the description, until it is changed.
    pub description: String,
    /// Where the data comes from: the catalog entry's publisher and license.
    pub source: String,
    /// Whether the chart shows its legend; off carries over to the file.
    pub legend: bool,
}

pub struct ChartExportModal {
    pub active: bool,
    pub focus: ChartExportFocus,
    pub format: ChartExportFormat,
    pub style: ExportStyle,
    pub size: SizePreset,
    pub legend: LegendPlace,
    pub path_input: TextInput,
    pub width_input: TextInput,
    pub height_input: TextInput,
    pub title_input: TextInput,
    pub description_input: TextInput,
    pub notes_input: TextInput,
    pub source_input: TextInput,
    pub byline_input: TextInput,
}

impl ChartExportModal {
    pub fn new() -> Self {
        Self::default()
    }

    /// Open the dialog for a chart. The format, style, size and byline stay as
    /// they were last used; the words about the chart start from `defaults`.
    pub fn open(
        &mut self,
        theme: &crate::config::Theme,
        history_limit: usize,
        defaults: ExportDefaults,
    ) {
        self.active = true;
        self.focus = ChartExportFocus::PathInput;
        let input = || TextInput::new().with_theme(theme);
        // Ctrl+P / Ctrl+N recall the paths exported to before.
        self.path_input = TextInput::new()
            .with_history("chart_export_path".to_string())
            .with_history_limit(history_limit)
            .with_theme(theme);
        self.title_input = input();
        self.description_input = input();
        self.description_input.set_value(defaults.description);
        self.notes_input = input();
        self.source_input = input();
        self.source_input.set_value(defaults.source);
        let byline = self.byline_input.value().to_string();
        self.byline_input = input();
        self.byline_input.set_value(byline);
        let (width, height) = (
            self.width_input.value().to_string(),
            self.height_input.value().to_string(),
        );
        self.width_input = input();
        self.height_input = input();
        self.width_input.set_value(width);
        self.height_input.set_value(height);
        self.legend = if defaults.legend {
            LegendPlace::LineEnds
        } else {
            LegendPlace::Off
        };
        self.apply_size();
    }

    /// Reopen after an overwrite declined or a failed write: the form as it was,
    /// focus on the path.
    pub fn reopen_with_path(&mut self, path: &Path, format: ChartExportFormat) {
        self.active = true;
        self.focus = ChartExportFocus::PathInput;
        self.format = format;
        self.path_input.set_value(path.display().to_string());
    }

    pub fn close(&mut self) {
        self.active = false;
        self.focus = ChartExportFocus::PathInput;
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

    /// ←/→ on a choice.
    pub fn step(&mut self, field: ChartExportFocus, delta: i8) {
        use crate::form::step_value;
        match field {
            ChartExportFocus::Format => {
                self.format = step_value(&ChartExportFormat::ALL, self.format, delta)
            }
            ChartExportFocus::Style => {
                self.style = step_value(&ExportStyle::ALL, self.style, delta)
            }
            ChartExportFocus::Size => {
                self.size = step_value(&SizePreset::ALL, self.size, delta);
                self.apply_size();
            }
            ChartExportFocus::Legend => {
                self.legend = step_value(&LegendPlace::ALL, self.legend, delta)
            }
            _ => {}
        }
    }

    /// Fill the width and height from the preset; a custom size keeps what is typed.
    fn apply_size(&mut self) {
        if let Some((w, h)) = self.size.size() {
            self.width_input.set_value(w.to_string());
            self.height_input.set_value(h.to_string());
        }
    }

    /// A width or height typed: the size is custom from then on.
    pub fn size_typed(&mut self) {
        if self.size.size() != Some(self.export_dimensions()) {
            self.size = SizePreset::Custom;
        }
    }

    /// The focused field's input, for a key the form hands it.
    pub fn focused_input_mut(&mut self) -> Option<&mut TextInput> {
        Some(match self.focus {
            ChartExportFocus::PathInput => &mut self.path_input,
            ChartExportFocus::WidthInput => &mut self.width_input,
            ChartExportFocus::HeightInput => &mut self.height_input,
            ChartExportFocus::TitleInput => &mut self.title_input,
            ChartExportFocus::DescriptionInput => &mut self.description_input,
            ChartExportFocus::NotesInput => &mut self.notes_input,
            ChartExportFocus::SourceInput => &mut self.source_input,
            ChartExportFocus::BylineInput => &mut self.byline_input,
            ChartExportFocus::Format
            | ChartExportFocus::Style
            | ChartExportFocus::Size
            | ChartExportFocus::Legend => return None,
        })
    }

    pub fn input(&self, field: ChartExportFocus) -> Option<&TextInput> {
        Some(match field {
            ChartExportFocus::PathInput => &self.path_input,
            ChartExportFocus::WidthInput => &self.width_input,
            ChartExportFocus::HeightInput => &self.height_input,
            ChartExportFocus::TitleInput => &self.title_input,
            ChartExportFocus::DescriptionInput => &self.description_input,
            ChartExportFocus::NotesInput => &self.notes_input,
            ChartExportFocus::SourceInput => &self.source_input,
            ChartExportFocus::BylineInput => &self.byline_input,
            _ => return None,
        })
    }

    /// The width and height typed, each 16 to 8,192 px; the preset's where a field
    /// does not read as a number.
    pub fn export_dimensions(&self) -> (u32, u32) {
        const MIN: u32 = 16;
        const MAX: u32 = 8192;
        let (dw, dh) = self.size.size().unwrap_or((1600, 1000));
        let read = |input: &TextInput, default: u32| {
            input
                .value()
                .trim()
                .parse::<u32>()
                .map(|n| n.clamp(MIN, MAX))
                .unwrap_or(default)
        };
        (read(&self.width_input, dw), read(&self.height_input, dh))
    }
}

impl crate::form::Form for ChartExportModal {
    type Field = ChartExportFocus;

    fn fields(&self) -> Vec<(ChartExportFocus, crate::form::FieldKind)> {
        use crate::form::FieldKind::{Choice, Text};
        FIELDS
            .iter()
            .map(|&f| {
                let kind = match f {
                    ChartExportFocus::Format
                    | ChartExportFocus::Style
                    | ChartExportFocus::Size
                    | ChartExportFocus::Legend => Choice,
                    _ => Text,
                };
                (f, kind)
            })
            .collect()
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
        let size = SizePreset::Document;
        let (w, h) = size.size().unwrap_or((1600, 1000));
        let mut width_input = TextInput::new();
        width_input.set_value(w.to_string());
        let mut height_input = TextInput::new();
        height_input.set_value(h.to_string());
        Self {
            active: false,
            focus: ChartExportFocus::PathInput,
            format: ChartExportFormat::Png,
            style: ExportStyle::Light,
            size,
            legend: LegendPlace::LineEnds,
            path_input: TextInput::new(),
            width_input,
            height_input,
            title_input: TextInput::new(),
            description_input: TextInput::new(),
            notes_input: TextInput::new(),
            source_input: TextInput::new(),
            byline_input: TextInput::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::form::Form;

    fn opened() -> ChartExportModal {
        let config = crate::config::AppConfig::default();
        let theme = crate::config::Theme::from_config(&config.theme).unwrap();
        let mut modal = ChartExportModal::new();
        modal.open(
            &theme,
            10,
            ExportDefaults {
                description: "delay, mean by month".to_string(),
                source: "NYC flights · CC0".to_string(),
                legend: false,
            },
        );
        modal
    }

    #[test]
    fn presets_set_the_size_and_typing_makes_it_custom() {
        let mut modal = opened();
        assert_eq!(modal.export_dimensions(), (1600, 1000));
        modal.step(ChartExportFocus::Size, -1);
        assert_eq!(modal.size, SizePreset::Slide);
        assert_eq!(modal.export_dimensions(), (1920, 1080));
        modal.step(ChartExportFocus::Size, 3);
        assert_eq!(modal.size, SizePreset::SingleColumn);
        assert_eq!(modal.width_input.value(), "1050");
        modal.width_input.set_value("800");
        modal.size_typed();
        assert_eq!(modal.size, SizePreset::Custom);
        assert_eq!(modal.export_dimensions(), (800, 788));
    }

    #[test]
    fn the_chart_fills_the_words_and_legend_off_carries_over() {
        let modal = opened();
        assert_eq!(modal.description_input.value(), "delay, mean by month");
        assert_eq!(modal.source_input.value(), "NYC flights · CC0");
        assert_eq!(modal.legend, LegendPlace::Off);
        assert_eq!(modal.fields().len(), FIELDS.len());
    }
}
