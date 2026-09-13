use crate::config::Theme;
use crate::numfmt::NumberFormatSettings;
use ratatui::style::Color;

/// Snapshot of theme colors and display configuration for rendering.
/// Passed to widgets to avoid threading many individual parameters.
#[derive(Debug, Clone)]
pub struct RenderContext {
    pub keybind_hints: Color,
    pub keybind_labels: Color,
    pub controls_bg: Color,
    pub background: Color,
    pub text_primary: Color,
    pub text_secondary: Color,
    pub text_inverse: Color,
    pub dimmed: Color,
    pub label: Color,
    pub success: Color,
    pub warning: Color,
    pub error: Color,
    pub modal_border: Color,
    pub modal_border_active: Color,
    pub modal_border_error: Color,
    pub surface: Color,
    pub throbber: Color,
    pub primary_chart_series_color: Color,

    /// The accent: key chips, focused titles, the selection rail.
    pub accent: Color,
    /// A brighter accent for the section the cursor is in.
    pub accent_bright: Color,
    /// The wordmark gradient, first and last stop.
    pub gradient_start: Color,
    pub gradient_end: Color,

    pub table_header: Color,
    pub table_header_bg: Color,
    pub row_numbers: Color,
    pub column_separator: Color,
    pub alternate_row_color: Option<Color>,
    /// Tint under the row the cursor is on; `None` means the old reversed-video look.
    pub table_selected: Option<Color>,
    /// Whether the data table shows its second header row of column types.
    pub dtype_row: bool,

    pub str_col: Color,
    pub int_col: Color,
    pub float_col: Color,
    pub bool_col: Color,
    pub temporal_col: Color,
    /// Placeholder color for binary-column cells. Applied regardless of `column_colors` since the
    /// `‹binary›` stub is a placeholder, not data.
    pub binary_col: Color,

    pub table_cell_padding: u16,
    pub column_colors: bool,
    /// Resolved number formatting, including the runtime `F` toggle state.
    pub number_format: NumberFormatSettings,
}

impl RenderContext {
    /// A context with default colors, for tests about layout rather than colour.
    #[cfg(test)]
    pub fn for_test() -> Self {
        let theme = Theme::from_config(&crate::config::ThemeConfig::default())
            .expect("default theme colors must resolve");
        Self::from_theme_and_config(&theme, 2, true, NumberFormatSettings::default())
    }

    /// Style of the row or item the cursor is on: the theme's tint, or reversed video
    /// when the theme asks for that.
    pub fn highlight_style(&self) -> ratatui::style::Style {
        match self.table_selected {
            Some(bg) => ratatui::style::Style::default().bg(bg),
            None => {
                ratatui::style::Style::default().add_modifier(ratatui::style::Modifier::REVERSED)
            }
        }
    }

    /// The same context with the type row switched on or off.
    pub fn with_dtype_row(mut self, on: bool) -> Self {
        self.dtype_row = on;
        self
    }

    /// Build render context from app theme and config.
    /// This is a snapshot; changes to theme won't affect this instance.
    pub fn from_theme_and_config(
        theme: &Theme,
        table_cell_padding: u16,
        column_colors: bool,
        number_format: NumberFormatSettings,
    ) -> Self {
        Self {
            keybind_hints: theme.get("keybind_hints"),
            keybind_labels: theme.get("keybind_labels"),
            controls_bg: theme.get("controls_bg"),
            background: theme.get("background"),
            text_primary: theme.get("text_primary"),
            text_secondary: theme.get("text_secondary"),
            text_inverse: theme.get("text_inverse"),
            dimmed: theme.get("dimmed"),
            label: theme.get("label"),
            success: theme.get("success"),
            warning: theme.get("warning"),
            error: theme.get("error"),
            modal_border: theme.get("modal_border"),
            modal_border_active: theme.get("modal_border_active"),
            modal_border_error: theme.get("modal_border_error"),
            surface: theme.get("surface"),
            throbber: theme.get("throbber"),
            primary_chart_series_color: theme.get("primary_chart_series_color"),

            accent: theme.get("accent"),
            accent_bright: theme.get("accent_bright"),
            gradient_start: theme.get("gradient_start"),
            gradient_end: theme.get("gradient_end"),

            table_header: theme.get("table_header"),
            table_header_bg: theme.get("table_header_bg"),
            row_numbers: theme.get("row_numbers"),
            column_separator: theme.get("column_separator"),
            alternate_row_color: theme.get_optional("alternate_row_color"),
            table_selected: theme.get_optional("table_selected"),
            dtype_row: true,

            str_col: if column_colors {
                theme.get("str_col")
            } else {
                Color::Reset
            },
            int_col: if column_colors {
                theme.get("int_col")
            } else {
                Color::Reset
            },
            float_col: if column_colors {
                theme.get("float_col")
            } else {
                Color::Reset
            },
            bool_col: if column_colors {
                theme.get("bool_col")
            } else {
                Color::Reset
            },
            temporal_col: if column_colors {
                theme.get("temporal_col")
            } else {
                Color::Reset
            },
            binary_col: theme.get("binary_col"),

            table_cell_padding,
            column_colors,
            number_format,
        }
    }
}
