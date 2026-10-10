use crate::config::Theme;
use crate::numfmt::NumberFormatSettings;
use ratatui::style::Color;

/// How the table is drawn this session: from the config, with the session's own toggles.
pub struct DisplaySettings {
    pub(crate) history_limit: usize, // History limit for all text inputs (from config.query.history_limit)
    pub(crate) table_cell_padding: u16, // Spaces between columns (from config.display.cell_padding)
    pub(crate) column_colors: bool, // When true, colorize table cells by column type (from config.display.column_colors)
    /// Second header row of column types. Starts from `display.type_row`; `D` flips it.
    pub(crate) dtype_row: bool,
    // Resolved display-time number formatting. `enabled` is flipped by the F key.
    pub(crate) number_format: NumberFormatSettings,
    /// The terminal should be asked for its background before the next frame.
    pub(crate) background_query: bool,
    /// The next frame repaints: the terminal was resized or came back to the front,
    /// and what it shows may have drifted from what was drawn.
    pub(crate) repaint: Option<Repaint>,
}

/// How much of the screen the next frame redraws whole.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Repaint {
    /// Every cell, in the next frame the terminal would move lines in (back in focus):
    /// a move would carry along whatever drifted on screen meanwhile.
    BeforeMoving,
    /// Every cell (resized).
    Whole,
}

/// The theme's colors and the display settings, handed to every widget a frame draws
/// in one value.
#[derive(Debug, Clone)]
pub struct RenderContext {
    pub keybind_hints: Color,
    pub keybind_labels: Color,
    pub controls_bg: Color,
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
    /// The cell a find landed on, from the theme's `find_match_style`.
    pub find_match: ratatui::style::Style,
    /// The column cursor: its cells' tint, and its header's and the current cell's.
    pub column_cursor: Option<Color>,
    pub cell_cursor: Option<Color>,
    /// Whether the data table shows its second header row of column types.
    pub dtype_row: bool,
    /// Stripes follow the rows, so a scroll moved by the terminal keeps them
    /// (`display.scroll_region`); off, they stay on screen lines, so a scroll redrawn
    /// in place rewrites only the text.
    pub stripes_follow_rows: bool,

    pub str_col: Color,
    pub int_col: Color,
    pub float_col: Color,
    pub bool_col: Color,
    pub temporal_col: Color,
    /// Placeholder color for binary-column cells. Applied regardless of `column_colors` since the
    /// `‹binary›` stub is a placeholder, not data.
    pub binary_col: Color,
    /// The hex view's bytes by class: 0x00, printable, whitespace, control, 0x80 to
    /// 0xFE, and 0xFF.
    pub hex_null: Color,
    pub hex_printable: Color,
    pub hex_whitespace: Color,
    pub hex_control: Color,
    pub hex_high: Color,
    pub hex_ff: Color,

    pub table_cell_padding: u16,
    pub column_colors: bool,
    /// Resolved number formatting, including the runtime `,` toggle state.
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

    /// Style of the column cursor's cells, from the theme's helper.
    pub fn column_cursor_style(&self) -> ratatui::style::Style {
        crate::config::column_cursor_style(self.column_cursor)
    }

    /// Style of the column cursor's header and the current cell, from the theme's
    /// helper.
    pub fn cell_cursor_style(&self) -> ratatui::style::Style {
        crate::config::cell_cursor_style(self.cell_cursor)
    }

    /// A column type's color, from the palette the table uses, so a column reads
    /// the same wherever it is listed.
    pub fn type_color(&self, dtype: &polars::prelude::DataType) -> ratatui::style::Color {
        use polars::prelude::DataType as D;
        match dtype {
            D::String | D::Categorical(_, _) | D::Enum(_, _) => self.str_col,
            D::Int8 | D::Int16 | D::Int32 | D::Int64 | D::Int128 => self.int_col,
            D::UInt8 | D::UInt16 | D::UInt32 | D::UInt64 => self.int_col,
            D::Float32 | D::Float64 | D::Decimal(_, _) => self.float_col,
            D::Boolean => self.bool_col,
            D::Date | D::Datetime(_, _) | D::Duration(_) | D::Time => self.temporal_col,
            D::Binary | D::BinaryOffset => self.binary_col,
            _ => self.text_secondary,
        }
    }

    /// The same context with the type row switched on or off.
    pub fn with_dtype_row(mut self, on: bool) -> Self {
        self.dtype_row = on;
        self
    }

    /// The same context with stripes on rows or on screen lines.
    pub fn with_stripes_follow_rows(mut self, on: bool) -> Self {
        self.stripes_follow_rows = on;
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
            keybind_hints: theme.chip_key(),
            keybind_labels: theme.chip_label(),
            controls_bg: theme.controls_bg(),
            text_primary: theme.text_primary(),
            text_secondary: theme.text_secondary(),
            text_inverse: theme.text_inverse(),
            dimmed: theme.dimmed(),
            label: theme.label(),
            success: theme.success(),
            warning: theme.warning(),
            error: theme.error(),
            modal_border: theme.modal_border(),
            modal_border_active: theme.modal_border_active(),
            modal_border_error: theme.modal_border_error(),
            throbber: theme.throbber(),
            primary_chart_series_color: theme.chart_1(),

            accent: theme.accent(),
            accent_bright: theme.accent_bright(),
            gradient_start: theme.gradient_start(),
            gradient_end: theme.gradient_end(),

            table_header: theme.table_header(),
            table_header_bg: theme.table_header_bg(),
            row_numbers: theme.table_row_numbers(),
            column_separator: theme.table_column_separator(),
            alternate_row_color: theme.get_optional("table_alternate_row"),
            table_selected: theme.get_optional("table_selected"),
            find_match: theme.find_match_style(),
            column_cursor: theme.get_optional("table_column_cursor"),
            cell_cursor: theme.get_optional("table_cell_cursor"),
            dtype_row: true,
            stripes_follow_rows: true,

            str_col: if column_colors {
                theme.type_str()
            } else {
                Color::Reset
            },
            int_col: if column_colors {
                theme.type_int()
            } else {
                Color::Reset
            },
            float_col: if column_colors {
                theme.type_float()
            } else {
                Color::Reset
            },
            bool_col: if column_colors {
                theme.type_bool()
            } else {
                Color::Reset
            },
            temporal_col: if column_colors {
                theme.type_temporal()
            } else {
                Color::Reset
            },
            binary_col: theme.type_binary(),
            hex_null: theme.hex_null(),
            hex_printable: theme.hex_printable(),
            hex_whitespace: theme.hex_whitespace(),
            hex_control: theme.hex_control(),
            hex_high: theme.hex_high(),
            hex_ff: theme.hex_ff(),

            table_cell_padding,
            column_colors,
            number_format,
        }
    }
}
