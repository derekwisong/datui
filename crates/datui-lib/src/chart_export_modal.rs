//! Chart export dialog: where, in what format, style and size, with what legend,
//! and the words around the chart. Its keys are the shared form keys
//! (`crate::form`).

use crate::chart_export::{
    ChartExportFormat, ExportStyle, LegendPlace, LineWidth, PointOpacity, PointSize, SizePreset,
};
use crate::chart_modal::Mark;
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
    PointOpacity,
    PointSize,
    LineWidth,
    YFromZero,
    TitleInput,
    DescriptionInput,
    NotesInput,
    SourceInput,
    BylineInput,
    Recipe,
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
            Self::PointOpacity => "Opacity:",
            Self::PointSize => "Point size:",
            Self::LineWidth => "Line width:",
            Self::YFromZero => "Y from zero:",
            Self::TitleInput => "Title:",
            Self::DescriptionInput => "Description:",
            Self::NotesInput => "Notes:",
            Self::SourceInput => "Source:",
            Self::BylineInput => "Byline:",
            Self::Recipe => "Recipe:",
        }
    }
}

/// The fields in order, top to bottom; the marks' rows show only for the chart
/// types they change ([`ChartExportModal::shows`]).
pub const FIELDS: [ChartExportFocus; 17] = [
    ChartExportFocus::PathInput,
    ChartExportFocus::Format,
    ChartExportFocus::Style,
    ChartExportFocus::Size,
    ChartExportFocus::WidthInput,
    ChartExportFocus::HeightInput,
    ChartExportFocus::Legend,
    ChartExportFocus::PointOpacity,
    ChartExportFocus::PointSize,
    ChartExportFocus::LineWidth,
    ChartExportFocus::YFromZero,
    ChartExportFocus::TitleInput,
    ChartExportFocus::DescriptionInput,
    ChartExportFocus::NotesInput,
    ChartExportFocus::SourceInput,
    ChartExportFocus::BylineInput,
    ChartExportFocus::Recipe,
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
    /// The chart's type: which mark rows the dialog offers.
    pub mark: Mark,
    /// A line chart's Y from zero, as drawn.
    pub y_from_zero: bool,
}

pub struct ChartExportModal {
    pub active: bool,
    pub focus: ChartExportFocus,
    pub format: ChartExportFormat,
    pub style: ExportStyle,
    pub size: SizePreset,
    /// Pixels per inch: the last preset's, which a custom size keeps, so typing a
    /// width does not change the text's size on the page.
    pub dpi: f32,
    pub legend: LegendPlace,
    /// The chart being exported, for which mark rows apply.
    pub mark: Mark,
    pub point_opacity: PointOpacity,
    pub point_size: PointSize,
    pub line_width: LineWidth,
    pub y_from_zero: bool,
    pub path_input: TextInput,
    pub width_input: TextInput,
    pub height_input: TextInput,
    pub title_input: TextInput,
    pub description_input: TextInput,
    pub notes_input: TextInput,
    pub source_input: TextInput,
    pub byline_input: TextInput,
    /// Whether the file carries how the chart was made: the source path, query,
    /// chart and sample. Starts from `chart.export_recipe`, then stays as last set.
    pub recipe: bool,
    /// A view's export settings, put in place the next time the dialog opens.
    pub restore: Option<crate::view::SavedChartExport>,
    /// Why Enter did not write: a blank path, or the failed write's reason. Said
    /// on the dialog's status line; cleared by typing in the path.
    pub error: Option<String>,
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
        self.error = None;
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
        // The marks' sizes stay as last used; the axis follows the chart.
        self.mark = defaults.mark;
        self.y_from_zero = defaults.y_from_zero;
        self.apply_size();
        if let Some(saved) = self.restore.take() {
            self.put_back(saved);
        }
    }

    /// The dialog as a view keeps it: everything but the path.
    pub fn saved(&self) -> crate::view::SavedChartExport {
        let (width, height) = self.export_dimensions();
        crate::view::SavedChartExport {
            format: self.format,
            style: self.style,
            size: self.size,
            width,
            height,
            dpi: self.dpi,
            legend: self.legend,
            point_opacity: self.point_opacity,
            point_size: self.point_size,
            line_width: self.line_width,
            y_from_zero: self.y_from_zero,
            title: self.title_input.value().to_string(),
            description: self.description_input.value().to_string(),
            notes: self.notes_input.value().to_string(),
            source: self.source_input.value().to_string(),
            byline: self.byline_input.value().to_string(),
            recipe: self.recipe,
        }
    }

    /// Put back a view's settings. Words it left blank keep the chart's own.
    fn put_back(&mut self, saved: crate::view::SavedChartExport) {
        self.format = saved.format;
        self.style = saved.style;
        self.size = saved.size;
        self.dpi = saved.dpi;
        self.width_input.set_value(saved.width.to_string());
        self.height_input.set_value(saved.height.to_string());
        self.legend = saved.legend;
        self.point_opacity = saved.point_opacity;
        self.point_size = saved.point_size;
        self.line_width = saved.line_width;
        self.y_from_zero = saved.y_from_zero;
        self.recipe = saved.recipe;
        for (input, text) in [
            (&mut self.title_input, saved.title),
            (&mut self.description_input, saved.description),
            (&mut self.notes_input, saved.notes),
            (&mut self.source_input, saved.source),
            (&mut self.byline_input, saved.byline),
        ] {
            if !text.is_empty() {
                input.set_value(text);
            }
        }
    }

    /// Whether `field` applies to the chart being exported: the points' rows to a
    /// scatter, the line's width and Y from zero to a line. Bars always start at
    /// zero: a bar's length is its value.
    pub fn shows(&self, field: ChartExportFocus) -> bool {
        match field {
            ChartExportFocus::PointOpacity | ChartExportFocus::PointSize => {
                self.mark == Mark::Scatter
            }
            ChartExportFocus::LineWidth | ChartExportFocus::YFromZero => self.mark == Mark::Line,
            _ => true,
        }
    }

    /// The fields on screen, top to bottom.
    pub fn shown(&self) -> Vec<ChartExportFocus> {
        FIELDS.into_iter().filter(|f| self.shows(*f)).collect()
    }

    /// What a choice row shows.
    pub fn choice(&self, field: ChartExportFocus) -> Option<&'static str> {
        Some(match field {
            ChartExportFocus::Format => self.format.as_str(),
            ChartExportFocus::Style => self.style.label(),
            ChartExportFocus::Size => self.size.label(),
            ChartExportFocus::Legend => self.legend.label(),
            ChartExportFocus::PointOpacity => self.point_opacity.label(),
            ChartExportFocus::PointSize => self.point_size.label(),
            ChartExportFocus::LineWidth => self.line_width.label(),
            ChartExportFocus::YFromZero => {
                if self.y_from_zero {
                    "On"
                } else {
                    "Off"
                }
            }
            // What is embedded is said with the choice, so nobody is surprised by it.
            ChartExportFocus::Recipe => {
                if self.recipe {
                    "Include: source path, query, chart, sample"
                } else {
                    "Omit: no datui metadata"
                }
            }
            _ => return None,
        })
    }

    /// Y from zero for the file: what the dialog says, on a line chart.
    pub fn y_from_zero_option(&self) -> Option<bool> {
        self.shows(ChartExportFocus::YFromZero)
            .then_some(self.y_from_zero)
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
        self.error = None;
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
            ChartExportFocus::PointOpacity => {
                self.point_opacity = step_value(&PointOpacity::ALL, self.point_opacity, delta)
            }
            ChartExportFocus::PointSize => {
                self.point_size = step_value(&PointSize::ALL, self.point_size, delta)
            }
            ChartExportFocus::LineWidth => {
                self.line_width = step_value(&LineWidth::ALL, self.line_width, delta)
            }
            ChartExportFocus::YFromZero => self.y_from_zero = !self.y_from_zero,
            ChartExportFocus::Recipe => self.recipe = !self.recipe,
            _ => {}
        }
    }

    /// Fill the width and height from the preset; a custom size keeps what is typed.
    fn apply_size(&mut self) {
        if let Some((w, h)) = self.size.size() {
            self.width_input.set_value(w.to_string());
            self.height_input.set_value(h.to_string());
            self.dpi = self.size.dpi();
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
            _ => return None,
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
        self.shown()
            .into_iter()
            .map(|f| {
                let kind = if self.choice(f).is_some() {
                    Choice
                } else {
                    Text
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
            dpi: size.dpi(),
            legend: LegendPlace::LineEnds,
            mark: Mark::default(),
            point_opacity: PointOpacity::default(),
            point_size: PointSize::default(),
            line_width: LineWidth::default(),
            y_from_zero: false,
            path_input: TextInput::new(),
            width_input,
            height_input,
            title_input: TextInput::new(),
            description_input: TextInput::new(),
            notes_input: TextInput::new(),
            source_input: TextInput::new(),
            byline_input: TextInput::new(),
            recipe: true,
            restore: None,
            error: None,
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
                description: "Mean by month".to_string(),
                source: "NYC flights · CC0".to_string(),
                legend: false,
                mark: Mark::Line,
                y_from_zero: false,
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
        assert_eq!(modal.dpi, 300.0, "the column's resolution stays");
        assert_eq!(modal.export_dimensions(), (800, 788));
    }

    #[test]
    fn the_chart_fills_the_words_and_legend_off_carries_over() {
        let modal = opened();
        assert_eq!(modal.description_input.value(), "Mean by month");
        assert_eq!(modal.source_input.value(), "NYC flights · CC0");
        assert_eq!(modal.legend, LegendPlace::Off);
        // A line takes its width and Y from zero, not the points' rows.
        assert_eq!(modal.fields().len(), FIELDS.len() - 2);
    }

    /// A mark row shows only for the chart types it changes, so focus never lands
    /// on one that would do nothing.
    #[test]
    fn mark_rows_follow_the_chart_type() {
        use ChartExportFocus::*;
        let mut modal = opened();
        let has = |modal: &ChartExportModal, f| modal.fields().iter().any(|(g, _)| *g == f);
        assert!(has(&modal, LineWidth) && has(&modal, YFromZero));
        assert!(!has(&modal, PointOpacity) && !has(&modal, PointSize));
        modal.mark = Mark::Scatter;
        assert!(has(&modal, PointOpacity) && has(&modal, PointSize));
        assert!(!has(&modal, LineWidth) && !has(&modal, YFromZero));
        assert_eq!(modal.y_from_zero_option(), None, "a scatter keeps its axis");
        modal.mark = Mark::Histogram;
        for f in [PointOpacity, PointSize, LineWidth, YFromZero] {
            assert!(!has(&modal, f), "{f:?}");
        }
        modal.mark = Mark::Bar;
        assert!(!has(&modal, YFromZero) && !has(&modal, LineWidth));
        assert_eq!(modal.y_from_zero_option(), None, "bars start at zero");
        modal.mark = Mark::Line;
        modal.y_from_zero = true;
        modal.step(YFromZero, 1);
        assert_eq!(modal.y_from_zero_option(), Some(false));
        modal.mark = Mark::Scatter;
        assert_eq!(modal.point_opacity, crate::chart_export::PointOpacity::Auto);
        modal.step(PointOpacity, 1);
        assert_eq!(modal.choice(PointOpacity), Some("100%"));
        modal.step(PointSize, -1);
        assert_eq!(modal.choice(PointSize), Some("Small"));
    }
}
