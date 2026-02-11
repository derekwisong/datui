use crate::config::Theme;
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

    pub table_header: Color,
    pub table_header_bg: Color,
    pub row_numbers: Color,
    pub column_separator: Color,
    pub alternate_row_color: Option<Color>,

    pub str_col: Color,
    pub int_col: Color,
    pub float_col: Color,
    pub bool_col: Color,
    pub temporal_col: Color,

    pub table_cell_padding: u16,
    pub column_colors: bool,
}

impl RenderContext {
    /// Build render context from app theme and config.
    /// This is a snapshot; changes to theme won't affect this instance.
    pub fn from_theme_and_config(
        theme: &Theme,
        table_cell_padding: u16,
        column_colors: bool,
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

            table_header: theme.get("table_header"),
            table_header_bg: theme.get("table_header_bg"),
            row_numbers: theme.get("row_numbers"),
            column_separator: theme.get("column_separator"),
            alternate_row_color: theme.get_optional("alternate_row_color"),

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

            table_cell_padding,
            column_colors,
        }
    }
}
