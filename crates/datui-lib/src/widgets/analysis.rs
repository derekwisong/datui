use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Axis, Bar, BarChart, BarGroup, Block, Borders, Cell, Chart, Dataset, GraphType, List,
        ListItem, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::analysis_modal::{AnalysisFocus, AnalysisTool, AnalysisView, HistogramScale};
use crate::config::Theme;
use crate::distribution_fit::{FitOutcome, FitTest};
use crate::glyphs::PlotMarks;
use crate::numfmt::{self, NumberFormatSettings};
use crate::statistics::{
    AnalysisContext, AnalysisResults, CategoricalStatistics, ColumnStatistics,
    DistributionAnalysis, DistributionType, NumericStatistics, TemporalStatistics,
};
use crate::widgets::datatable::DataTableState;
use polars::prelude::{AnyValue, DataType};

pub struct AnalysisWidgetConfig<'a> {
    pub state: &'a DataTableState,
    pub results: Option<&'a AnalysisResults>,
    pub context: &'a AnalysisContext,
    pub view: AnalysisView,
    pub selected_tool: Option<AnalysisTool>,
    pub column_offset: usize,
    pub selected_correlation: Option<(usize, usize)>,
    pub focus: AnalysisFocus,
    pub selected_theoretical_distribution: DistributionType,
    pub histogram_scale: HistogramScale,
    pub theme: &'a Theme,
    pub table_cell_padding: u16,
    /// Display-time number formatting, so counts here match the data table.
    pub number_format: &'a NumberFormatSettings,
    /// The shared sample the results were read with, for the header.
    pub sample: &'a crate::sampling::Sample,
}

pub struct AnalysisWidget<'a> {
    _state: &'a DataTableState,
    results: Option<&'a AnalysisResults>,
    _context: &'a AnalysisContext,
    view: AnalysisView,
    selected_tool: Option<AnalysisTool>,
    table_state: &'a mut TableState,
    distribution_table_state: &'a mut TableState,
    correlation_table_state: &'a mut TableState,
    sidebar_state: &'a mut TableState,
    column_offset: usize,
    selected_correlation: Option<(usize, usize)>,
    focus: AnalysisFocus,
    selected_theoretical_distribution: DistributionType,
    distribution_selector_state: &'a mut TableState,
    histogram_scale: HistogramScale,
    theme: &'a Theme,
    table_cell_padding: u16,
    number_format: &'a NumberFormatSettings,
    sample: &'a crate::sampling::Sample,
}

impl<'a> AnalysisWidget<'a> {
    pub fn new(
        config: AnalysisWidgetConfig<'a>,
        table_state: &'a mut TableState,
        distribution_table_state: &'a mut TableState,
        correlation_table_state: &'a mut TableState,
        sidebar_state: &'a mut TableState,
        distribution_selector_state: &'a mut TableState,
    ) -> Self {
        Self {
            _state: config.state,
            results: config.results,
            _context: config.context,
            view: config.view,
            selected_tool: config.selected_tool,
            table_state,
            distribution_table_state,
            correlation_table_state,
            sidebar_state,
            column_offset: config.column_offset,
            selected_correlation: config.selected_correlation,
            focus: config.focus,
            selected_theoretical_distribution: config.selected_theoretical_distribution,
            distribution_selector_state,
            histogram_scale: config.histogram_scale,
            theme: config.theme,
            table_cell_padding: config.table_cell_padding,
            number_format: config.number_format,
            sample: config.sample,
        }
    }
}

impl<'a> Widget for AnalysisWidget<'a> {
    fn render(self, area: Rect, buf: &mut Buffer) {
        match self.view {
            AnalysisView::Main => self.render_main_view(area, buf),
            AnalysisView::DistributionDetail => self.render_distribution_detail(area, buf),
            AnalysisView::CorrelationDetail => self.render_correlation_detail(area, buf),
        }
    }
}

impl<'a> AnalysisWidget<'a> {
    fn render_main_view(self, area: Rect, buf: &mut Buffer) {
        // The tool list never takes more than a third of the screen: the
        // results are what the screen is for.
        let sidebar_width = sidebar_width(area.width);

        // Full-screen layout: breadcrumb, main area (no separate keybind hints line)
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Breadcrumb
                Constraint::Fill(1),   // Main area + sidebar
            ])
            .split(area);

        // Breadcrumb: tool name when a tool is selected, or "Analysis" when none selected
        let tool_name = match self.selected_tool {
            Some(AnalysisTool::Describe) => "Describe",
            Some(AnalysisTool::DistributionAnalysis) => "Distribution Analysis",
            Some(AnalysisTool::CorrelationMatrix) => "Correlation Matrix",
            Some(AnalysisTool::DataQuality) => "Data Quality",
            None => "Analysis",
        };

        // What the numbers are of, stated rather than implied: a sample says how big,
        // of how many, and of which rows, so a surprising figure can be told apart
        // from a rare one.
        let breadcrumb_text = match self.results {
            Some(results) if self.selected_tool.is_some() => format!(
                "{tool_name} {} {}",
                crate::glyphs::get().middot,
                self.sample
                    .outcome(results.total_rows, results.sample_size, results.per_value)
            ),
            _ => tool_name.to_string(),
        };

        let header_row_style = header_style(self.theme, "controls_bg", "table_header");
        Paragraph::new(breadcrumb_text)
            .style(header_row_style)
            .render(layout[0], buf);

        // Split main area into content area and sidebar
        let main_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Fill(1),               // Main content area
                Constraint::Length(sidebar_width), // Sidebar
            ])
            .split(layout[1]);

        // Main content area: instructions when no tool selected, else selected tool (or "Computing...")
        match self.selected_tool {
            None => {
                const INSTRUCTION_LINES: u16 = 1;
                let inner = Layout::default()
                    .direction(Direction::Vertical)
                    .constraints([
                        Constraint::Min(0),
                        Constraint::Length(INSTRUCTION_LINES),
                        Constraint::Min(0),
                    ])
                    .split(main_layout[0]);
                Paragraph::new("Select an analysis tool from the sidebar.")
                    .centered()
                    .style(Style::default().fg(self.theme.get("text_primary")))
                    .render(inner[1], buf);
            }
            Some(tool) => {
                if let Some(results) = self.results {
                    match tool {
                        AnalysisTool::Describe => {
                            render_statistics_table(
                                results,
                                self.table_state,
                                self.column_offset,
                                main_layout[0],
                                buf,
                                self.theme,
                                self.table_cell_padding,
                                self.number_format,
                            );
                        }
                        AnalysisTool::DistributionAnalysis => {
                            render_distribution_table(
                                results,
                                self.distribution_table_state,
                                self.column_offset,
                                main_layout[0],
                                buf,
                                self.theme,
                            );
                        }
                        AnalysisTool::CorrelationMatrix => {
                            render_correlation_matrix(
                                results,
                                self.correlation_table_state,
                                &self.selected_correlation,
                                self.column_offset,
                                main_layout[0],
                                buf,
                                self.theme,
                            );
                        }
                        AnalysisTool::DataQuality => {
                            Paragraph::new("Data Quality")
                                .centered()
                                .render(main_layout[0], buf);
                        }
                    }
                }
                // No result yet: the Sample form fills this pane until the first run,
                // and the progress overlay covers it during one.
            }
        }

        // Sidebar: Tool list
        render_sidebar(
            main_layout[1],
            buf,
            self.sidebar_state,
            self.selected_tool,
            self.focus,
            self.theme,
        );

        // Keybind hints are now shown on the main bottom bar (see lib.rs)
    }

    fn render_distribution_detail(self, area: Rect, buf: &mut Buffer) {
        // Get selected distribution
        let selected_idx = self.distribution_table_state.selected();
        let dist_analysis: Option<&DistributionAnalysis> = self.results.and_then(|results| {
            selected_idx.and_then(|idx| results.distribution_analyses.get(idx))
        });

        if let Some(dist) = dist_analysis {
            // Layout: breadcrumb, main content (no keybind hints line)
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Breadcrumb
                    Constraint::Fill(1),   // Main content
                ])
                .split(area);

            // The breadcrumb carries the name alone; the control bar says Esc.
            let title_text = format!("Distribution Analysis: {}", dist.column_name);
            let header_row_style = header_style(self.theme, "controls_bg", "table_header");
            Paragraph::new(title_text)
                .style(header_row_style)
                .render(layout[0], buf);

            // Main content area - optimized layout
            // Split into: condensed stats header, charts and selector area
            let main_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(1), // Condensed stats header (single line)
                    Constraint::Fill(1),   // Charts and selector
                ])
                .split(layout[1]);

            // Condensed header: Key statistics in one or two lines
            // Use selected theoretical distribution type (dynamic)
            render_condensed_statistics(
                dist,
                self.selected_theoretical_distribution,
                main_layout[0],
                buf,
                self.theme,
            );

            // Split charts and selector horizontally
            let content_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Percentage(75), // Q-Q plot and histogram
                    Constraint::Percentage(25), // Distribution selector and settings
                ])
                .split(main_layout[1]);

            // Right side: Split into distribution selector and settings
            let right_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Fill(1),   // Distribution selector (takes remaining space)
                    Constraint::Length(4), // Settings box (4 lines: border + 2 content + border)
                ])
                .split(content_layout[1]);

            // Left side: Q-Q plot and histogram with spacing
            let charts_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Percentage(52), // Q-Q plot (slightly reduced to make room for spacing)
                    Constraint::Length(1),      // Vertical spacing between charts
                    Constraint::Percentage(47), // Histogram (slightly reduced to make room for spacing)
                ])
                .split(content_layout[0]);

            // Add padding around chart areas for better visual separation
            let chart_padding = 1u16; // 1 character padding on all sides
            let right_padding_extra = 1u16; // Extra padding on right side to separate from distribution box
            let top_padding_extra = 1u16; // Extra padding at top to separate title from chart
            let qq_plot_area = Rect::new(
                charts_layout[0].left() + chart_padding,
                charts_layout[0].top() + chart_padding + top_padding_extra, // Extra top padding
                charts_layout[0]
                    .width
                    .saturating_sub(chart_padding) // Left padding
                    .saturating_sub(right_padding_extra), // Extra right padding
                charts_layout[0]
                    .height
                    .saturating_sub(chart_padding * 2)
                    .saturating_sub(top_padding_extra), // Account for extra top padding
            );
            let histogram_area = Rect::new(
                charts_layout[2].left() + chart_padding,
                charts_layout[2].top() + chart_padding + top_padding_extra, // Extra top padding
                charts_layout[2]
                    .width
                    .saturating_sub(chart_padding) // Left padding
                    .saturating_sub(right_padding_extra), // Extra right padding
                charts_layout[2]
                    .height
                    .saturating_sub(chart_padding * 2)
                    .saturating_sub(top_padding_extra), // Account for extra top padding
            );

            // Calculate maximum label width for both charts to ensure alignment
            // This needs to account for both Q-Q plot labels (data values) and histogram labels (counts)
            let sorted_data = &dist.sorted_sample_values;
            let max_label_width = if sorted_data.is_empty() {
                1
            } else {
                let data_min = sorted_data[0];
                let data_max = sorted_data[sorted_data.len() - 1];

                // Q-Q plot labels: data_min, (data_min+data_max)/2, data_max formatted as {:.1}
                let qq_label_bottom = format!("{:.1}", data_min);
                let qq_label_mid = format!("{:.1}", (data_min + data_max) / 2.0);
                let qq_label_top = format!("{:.1}", data_max);
                let qq_max_width = qq_label_bottom
                    .chars()
                    .count()
                    .max(qq_label_mid.chars().count())
                    .max(qq_label_top.chars().count());

                // Histogram labels: 0, global_max/2, global_max (formatted as integers)
                // We need to estimate global_max - it's roughly the max of data bin counts and theory bin counts
                // For estimation, use the data size as a proxy for maximum counts
                let estimated_global_max = sorted_data.len();
                let hist_label_0 = format!("{}", 0);
                let hist_label_mid = format!("{}", estimated_global_max / 2);
                let hist_label_max = format!("{}", estimated_global_max);
                let hist_max_width = hist_label_0
                    .chars()
                    .count()
                    .max(hist_label_mid.chars().count())
                    .max(hist_label_max.chars().count());

                // Use the maximum of both, adding 1 for padding
                qq_max_width.max(hist_max_width)
            };

            let shared_y_axis_label_width = (max_label_width as u16).max(1) + 1; // Max label width + 1 char padding

            // Calculate unified X-axis range for visual alignment between Q-Q plot and histogram
            // This ensures both charts use the same X-axis scale for easy comparison
            // Calculate unified X-axis range for both Q-Q plot and histogram
            // Use ONLY actual data range (no padding, no theoretical extensions)
            // This ensures log scale works correctly and both charts stay in sync
            let unified_x_range = if !sorted_data.is_empty() {
                let data_min = sorted_data[0];
                let data_max = sorted_data[sorted_data.len() - 1];
                // Use strict data range - no padding, no theoretical extensions
                (data_min, data_max)
            } else {
                (0.0, 1.0) // Fallback for empty data
            };

            // Both plots of the selected theoretical distribution, on one x range.
            let plot = DistributionPlotConfig {
                dist,
                dist_type: self.selected_theoretical_distribution,
                area: qq_plot_area,
                shared_y_axis_label_width,
                theme: self.theme,
                unified_x_range: Some(unified_x_range),
                histogram_scale: self.histogram_scale,
                glyphs: crate::glyphs::get(),
            };
            render_qq_plot(plot, buf);

            // Check if log scale is requested but can't be used
            // Use actual data values, not unified range (which may include theoretical bounds and padding)
            let sorted_data = &dist.sorted_sample_values;
            let can_use_log_scale = !sorted_data.is_empty() && sorted_data.iter().all(|&v| v > 0.0);
            let log_scale_requested_but_unavailable =
                matches!(self.histogram_scale, HistogramScale::Log) && !can_use_log_scale;

            render_distribution_histogram(
                DistributionPlotConfig {
                    area: histogram_area,
                    ..plot
                },
                buf,
            );

            // Right side: Distribution selector
            render_distribution_selector(
                dist,
                self.selected_theoretical_distribution,
                self.distribution_selector_state,
                self.focus,
                right_layout[0],
                buf,
                self.theme,
            );

            // Settings box below distribution selector
            render_distribution_settings(
                self.histogram_scale,
                log_scale_requested_but_unavailable,
                right_layout[1],
                buf,
                self.theme,
            );

        // No keybind hints line - removed
        } else {
            Paragraph::new("No distribution selected")
                .centered()
                .render(area, buf);
        }
    }

    fn render_correlation_detail(self, area: Rect, buf: &mut Buffer) {
        let matrix = self
            .results
            .and_then(|results| results.correlation_matrix.as_ref());
        let pair = self.selected_correlation.and_then(|(row, col)| {
            matrix.and_then(|m| {
                (row < m.columns.len() && col < m.columns.len()).then_some((row, col))
            })
        });

        let (Some(matrix), Some((row, col))) = (matrix, pair) else {
            Paragraph::new("No correlation pair selected")
                .centered()
                .render(area, buf);
            return;
        };

        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([Constraint::Length(1), Constraint::Fill(1)])
            .split(area);

        // The breadcrumb carries the pair alone; the control bar says Esc.
        let title_text = format!(
            "Correlation: {} vs {}",
            matrix.columns[row], matrix.columns[col]
        );
        let header_row_style = header_style(self.theme, "controls_bg", "table_header");
        Paragraph::new(title_text)
            .style(header_row_style)
            .render(layout[0], buf);

        let total_rows = self.results.map(|r| r.total_rows).unwrap_or(0);
        render_correlation_pair_summary(
            matrix,
            (row, col),
            total_rows,
            layout[1],
            buf,
            self.theme,
            self.number_format,
        );
    }
}

/// A short reading of a coefficient, using the same 0.05/0.3 boundaries as the
/// matrix's colors so the word never disagrees with the color.
fn describe_correlation(r: f64) -> &'static str {
    let strength = r.abs();
    if strength < 0.05 {
        "none"
    } else if strength < 0.3 {
        if r > 0.0 {
            "weak positive"
        } else {
            "weak negative"
        }
    } else if strength < 0.7 {
        if r > 0.0 {
            "moderate positive"
        } else {
            "moderate negative"
        }
    } else if r > 0.0 {
        "strong positive"
    } else {
        "strong negative"
    }
}

/// The body of the correlation pair detail: everything the matrix already knows
/// about the pair. Nothing is collected here — a scatter or per-column moments
/// would need the pair's values, which the correlation results do not carry.
fn render_correlation_pair_summary(
    matrix: &crate::statistics::CorrelationMatrix,
    (row, col): (usize, usize),
    total_rows: usize,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
    number_format: &NumberFormatSettings,
) {
    let r = matrix.correlations[row][col];
    let pairs = matrix.sample_sizes[row][col];
    let p_value = matrix.p_values.as_ref().map(|p| p[row][col]);

    let label_style = Style::default().fg(theme.get("text_secondary"));
    let value_style = Style::default().fg(theme.get("text_primary"));

    let mut lines: Vec<Line> = Vec::new();
    if r.is_nan() {
        let why = if pairs < 3 {
            "Not enough overlapping values to correlate (needs 3 pairs)."
        } else {
            "One of the columns has a single value, so there is nothing to correlate."
        };
        lines.push(Line::from(vec![Span::styled(why, value_style)]));
    } else {
        lines.push(Line::from(vec![
            Span::styled("Pearson r: ", label_style),
            Span::styled(
                format!("{:.3}", r),
                Style::default().fg(get_correlation_color(r, theme)),
            ),
            Span::styled(format!("  ({})", describe_correlation(r)), value_style),
        ]));
        lines.push(Line::from(vec![
            Span::styled(format!("{}: ", crate::glyphs::get().r_squared), label_style),
            Span::styled(format!("{:.3}", r * r), value_style),
        ]));
        if let Some(p) = p_value {
            lines.push(Line::from(vec![
                Span::styled("P-value: ", label_style),
                Span::styled(format_pvalue(p), value_style),
            ]));
        }
    }
    lines.push(Line::from(vec![
        Span::styled("Pairs used: ", label_style),
        Span::styled(
            format!(
                "{} of {} rows",
                format_count(pairs, number_format),
                format_count(total_rows, number_format)
            ),
            value_style,
        ),
    ]));

    // One character of margin, like the distribution detail's charts.
    let inner = Rect::new(
        area.left() + 1,
        area.top() + 1,
        area.width.saturating_sub(2),
        area.height.saturating_sub(1),
    );
    Paragraph::new(lines).render(inner, buf);
}

#[allow(clippy::too_many_arguments)]
fn render_statistics_table(
    results: &AnalysisResults,
    table_state: &mut TableState,
    column_offset: usize,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
    table_cell_padding: u16,
    number_format: &NumberFormatSettings,
) {
    let num_columns = results.column_statistics.len();
    if num_columns == 0 {
        Paragraph::new("No columns to display")
            .centered()
            .render(area, buf);
        return;
    }

    // Statistics to display (in order) - internal names for matching data
    let stat_names = vec![
        "count",
        "null_count",
        "mean",
        "std",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
    ];
    // Display names in Title case for headers
    let stat_display_names = vec![
        "Count", "Nulls", "Mean", "Std", "Min", "25%", "50%", "75%", "Max",
    ];
    let num_stats = stat_names.len();

    // Calculate column widths based on header names and content (minimal spacing)
    // First, determine minimum width for each column based on header length
    // Note: ratatui Table adds 1 space between columns by default, so we don't add extra padding
    let mut min_col_widths: Vec<u16> = stat_display_names
        .iter()
        .map(|name| name.chars().count() as u16) // header length (no extra padding - table handles spacing)
        .collect();

    // Scan all data to find maximum width needed for each column
    for col_stat in &results.column_statistics {
        for (stat_idx, stat_name) in stat_names.iter().enumerate() {
            let value_str = describe_value(col_stat, stat_name, number_format);
            let value_len = value_str.chars().count() as u16;
            // Ensure width is at least the header length (already initialized) AND value length
            // This preserves header widths even if all data values are shorter
            let header_len = stat_display_names[stat_idx].chars().count() as u16;
            min_col_widths[stat_idx] = min_col_widths[stat_idx].max(value_len).max(header_len);
            // must fit both header and content (no padding - table handles spacing)
        }
    }

    // Locked column width (column name) - calculate from header text AND actual column names
    let header_text = "Column";
    let header_len = header_text.chars().count() as u16;
    let max_col_name_len = results
        .column_statistics
        .iter()
        .map(|cs| cs.name.chars().count() as u16)
        .max()
        .unwrap_or(header_len);
    let locked_col_width = max_col_name_len.max(header_len).max(10); // min 10, must fit both header and data (no padding - table handles spacing)

    // Calculate which columns can fit using same cell padding as main datatable
    let column_spacing = table_cell_padding;

    // Available width for stat columns = total width - locked column - spacing between locked and first stat
    let available_width = area
        .width
        .saturating_sub(locked_col_width)
        .saturating_sub(column_spacing);

    let mut used_width_from_zero = 0u16;
    let mut max_visible_from_zero = 0;

    for width in min_col_widths.iter() {
        let spacing_needed = if max_visible_from_zero > 0 {
            column_spacing
        } else {
            0
        };
        let total_needed = spacing_needed + width;

        if used_width_from_zero + total_needed <= available_width {
            used_width_from_zero += total_needed;
            max_visible_from_zero += 1;
        } else {
            break;
        }
    }

    max_visible_from_zero = max_visible_from_zero.max(1);

    let effective_offset = if max_visible_from_zero >= num_stats {
        0
    } else {
        column_offset.min(num_stats.saturating_sub(1))
    };

    let start_stat = effective_offset;

    let mut used_width = 0u16;
    let mut max_visible_stats = 0;

    for width in min_col_widths
        .iter()
        .skip(start_stat)
        .take(num_stats - start_stat)
    {
        let spacing_needed = if max_visible_stats > 0 {
            column_spacing
        } else {
            0
        };
        let total_needed = spacing_needed + width;

        if used_width + total_needed <= available_width {
            used_width += total_needed;
            max_visible_stats += 1;
        } else {
            break;
        }
    }

    max_visible_stats = max_visible_stats.max(1); // At least show 1 column

    let end_stat = (start_stat + max_visible_stats).min(num_stats);
    let visible_stats: Vec<usize> = (start_stat..end_stat).collect();

    if visible_stats.is_empty() {
        return;
    }

    let mut rows = Vec::new();

    let mut header_cells = vec![Cell::from("Column").style(Style::default())];
    for &stat_idx in &visible_stats {
        header_cells.push(Cell::from(stat_display_names[stat_idx]).style(Style::default()));
    }
    let header_row_style = header_style(theme, "controls_bg", "table_header");
    let header_row = Row::new(header_cells.clone()).style(header_row_style);

    for col_stat in &results.column_statistics {
        let mut cells = vec![
            Cell::from(col_stat.name.as_str())
                .style(Style::default().fg(theme.get("text_primary"))),
        ];
        for &stat_idx in &visible_stats {
            let stat_name = stat_names[stat_idx];
            let value = describe_value(col_stat, stat_name, number_format);

            cells.push(Cell::from(value));
        }

        rows.push(Row::new(cells));
    }

    let mut constraints = vec![Constraint::Length(locked_col_width)];
    for &stat_idx in &visible_stats {
        // Use minimum width needed (ratatui will add spacing between columns)
        constraints.push(Constraint::Length(min_col_widths[stat_idx]));
    }

    let table = Table::new(rows, constraints)
        .header(header_row)
        .column_spacing(table_cell_padding)
        .row_highlight_style(theme.highlight_style());

    // Use StatefulWidget for row selection
    StatefulWidget::render(table, area, buf, table_state);
}

/// One Describe cell. A date, time or duration column gets its range, quartiles
/// and mean in its own format; the statistics it has no value for, and nulls, read `-`.
fn describe_value(
    col_stat: &ColumnStatistics,
    stat_name: &str,
    number_format: &NumberFormatSettings,
) -> String {
    let numeric = |f: fn(&NumericStatistics) -> f64| {
        col_stat.numeric_stats.as_ref().map(|n| format_num(f(n)))
    };
    let temporal = |f: fn(&TemporalStatistics) -> &Option<String>| {
        col_stat.temporal_stats.as_ref().and_then(|t| f(t).clone())
    };
    let categorical = |f: fn(&CategoricalStatistics) -> &Option<String>| {
        col_stat
            .categorical_stats
            .as_ref()
            .and_then(|c| f(c).clone())
    };
    match stat_name {
        "count" => Some(format_count(col_stat.count, number_format)),
        "null_count" => Some(format_count(col_stat.null_count, number_format)),
        "mean" => numeric(|n| n.mean).or_else(|| temporal(|t| &t.mean)),
        "std" => numeric(|n| n.std),
        "min" => numeric(|n| n.min)
            .or_else(|| temporal(|t| &t.min))
            .or_else(|| categorical(|c| &c.min)),
        "25%" => numeric(|n| n.q25).or_else(|| temporal(|t| &t.q25)),
        "50%" => numeric(|n| n.median).or_else(|| temporal(|t| &t.median)),
        "75%" => numeric(|n| n.q75).or_else(|| temporal(|t| &t.q75)),
        "max" => numeric(|n| n.max)
            .or_else(|| temporal(|t| &t.max))
            .or_else(|| categorical(|c| &c.max)),
        _ => None,
    }
    .unwrap_or_else(|| "-".to_string())
}

/// Format a row/null count, following the same grouping setting as the data
/// table so a user who turned formatting on sees it everywhere they read
/// numbers. Float statistics go through `format_num`, which switches to
/// scientific notation well before grouping would apply.
fn format_count(n: usize, settings: &NumberFormatSettings) -> String {
    let fmt = settings.formatter_for("", &DataType::UInt64);
    let mut scratch = String::new();
    numfmt::format_any_value(&fmt, &AnyValue::UInt64(n as u64), &mut scratch).into_owned()
}

fn format_num(n: f64) -> String {
    if n.is_nan() {
        "-".to_string()
    } else if n.abs() >= 1000.0 || (n.abs() < 0.01 && n != 0.0) {
        format!("{:.2e}", n)
    } else {
        format!("{:.2}", n)
    }
}

// Phase 6: Format p-value with special handling for very small values
fn format_pvalue(p: f64) -> String {
    if p < 0.001 {
        "<0.001".to_string()
    } else {
        format!("{:.3}", p)
    }
}

/// The p-value beside a column's verdict: the chosen family's, or with no clear fit the
/// best any family managed. A bound reads as one.
fn verdict_pvalue(dist: &DistributionAnalysis) -> String {
    let chosen = dist.fit(dist.distribution_type).and_then(FitOutcome::test);
    let best = || {
        dist.fits
            .iter()
            .filter_map(|(_, outcome)| outcome.test())
            .max_by(|a, b| a.p_value.total_cmp(&b.p_value))
    };
    match chosen.or_else(best) {
        Some(test) => format_fit_pvalue(test),
        None => "N/A".to_string(),
    }
}

/// A fit test's p-value: `<0.005` when no simulated sample reached the column's
/// statistic, since then the p-value is only a bound.
fn format_fit_pvalue(test: &FitTest) -> String {
    if test.at_bound() {
        format!("<{:.3}", test.p_value)
    } else {
        format!("{:.3}", test.p_value)
    }
}

/// Holds, marginal, rejected.
fn pvalue_style(p: f64, theme: &Theme) -> Style {
    if p >= 0.05 {
        Style::default().fg(theme.get("distribution_normal"))
    } else if p > 0.01 {
        Style::default().fg(theme.get("distribution_skewed"))
    } else {
        Style::default().fg(theme.get("outlier_marker"))
    }
}

/// Build header-style: bg+fg when bg_key is not Reset, else fg-only.
pub(crate) fn header_style(theme: &Theme, bg_key: &str, fg_key: &str) -> Style {
    let bg = theme.get(bg_key);
    let fg = theme.get(fg_key);
    if bg == Color::Reset {
        Style::default().fg(fg)
    } else {
        Style::default().bg(bg).fg(fg)
    }
}

fn render_distribution_table(
    results: &AnalysisResults,
    table_state: &mut TableState,
    column_offset: usize,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    if results.distribution_analyses.is_empty() {
        Paragraph::new("No numeric columns for distribution analysis")
            .centered()
            .render(area, buf);
        return;
    }

    // Column headers for width calculation (excluding "Column" which will be locked)
    // Phase 6: Add P-value column after Distribution
    let column_names = [
        "Distribution",
        "P-value",
        "Shapiro-Francia",
        "SF p-value",
        "CV",
        "Outliers",
        "Skewness",
        "Kurtosis",
    ];
    let num_stats = column_names.len();

    // Calculate column widths based on header names and content (minimal spacing)
    // Note: ratatui Table adds 1 space between columns by default, so we don't add extra padding
    let mut min_col_widths: Vec<u16> = column_names
        .iter()
        .map(|name| name.chars().count() as u16) // header length (no extra padding - table handles spacing)
        .collect();

    // Calculate column name width (for locked column)
    let header_text = "Column";
    let header_len = header_text.chars().count() as u16;
    let max_col_name_len = results
        .distribution_analyses
        .iter()
        .map(|da| da.column_name.chars().count() as u16)
        .max()
        .unwrap_or(header_len);
    let locked_col_width = max_col_name_len.max(header_len).max(10);

    // Scan all data to find maximum width needed for each column (excluding Column)
    for dist_analysis in &results.distribution_analyses {
        // Outlier count with percentage
        let outlier_text = if dist_analysis.outliers.total_count > 0 {
            format!(
                "{} ({:.1}%)",
                dist_analysis.outliers.total_count, dist_analysis.outliers.percentage
            )
        } else {
            "0 (0.0%)".to_string()
        };

        // Shapiro-Wilk statistic and p-value formatting
        let sw_stat_text = dist_analysis
            .characteristics
            .shapiro_wilk_stat
            .map(|s| format!("{:.3}", s))
            .unwrap_or_else(|| "N/A".to_string());
        let sw_pvalue_text = dist_analysis
            .characteristics
            .shapiro_wilk_pvalue
            .map(format_pvalue)
            .unwrap_or_else(|| "N/A".to_string());

        let pvalue_text = verdict_pvalue(dist_analysis);

        // Update minimum widths based on content (skip column name)
        let col_values = [
            format!("{}", dist_analysis.distribution_type),
            pvalue_text.clone(),
            sw_stat_text.clone(),
            sw_pvalue_text.clone(),
            format!(
                "{:.4}",
                dist_analysis.characteristics.coefficient_of_variation
            ),
            outlier_text.clone(),
            format_num(dist_analysis.characteristics.skewness),
            format_num(dist_analysis.characteristics.kurtosis),
        ];

        for (idx, value) in col_values.iter().enumerate() {
            let value_len = value.chars().count() as u16;
            let header_len = column_names[idx].chars().count() as u16;
            min_col_widths[idx] = min_col_widths[idx].max(value_len).max(header_len);
        }
    }

    // Calculate which columns can fit (similar to describe table)
    let column_spacing = 1u16;
    let available_width = area
        .width
        .saturating_sub(locked_col_width)
        .saturating_sub(column_spacing); // Space between locked column and first stat column

    // Determine which statistics to show (column_offset refers to stat columns, not column name)
    let start_stat = column_offset.min(num_stats.saturating_sub(1));

    // Calculate how many stat columns can fit starting from start_stat
    let mut used_width = 0u16;
    let mut max_visible_stats = 0;

    for width in min_col_widths
        .iter()
        .skip(start_stat)
        .take(num_stats - start_stat)
    {
        let spacing_needed = if max_visible_stats > 0 {
            column_spacing
        } else {
            0
        };
        let total_needed = spacing_needed + width;

        if used_width + total_needed <= available_width {
            used_width += total_needed;
            max_visible_stats += 1;
        } else {
            break;
        }
    }

    max_visible_stats = max_visible_stats.max(1); // At least show 1 column
    let end_stat = (start_stat + max_visible_stats).min(num_stats);
    let visible_stats: Vec<usize> = (start_stat..end_stat).collect();

    if visible_stats.is_empty() {
        return;
    }

    let mut rows = Vec::new();

    let mut header_cells = vec![Cell::from("Column").style(Style::default())];
    for &stat_idx in &visible_stats {
        header_cells.push(Cell::from(column_names[stat_idx]).style(Style::default()));
    }
    let header_row_style = header_style(theme, "controls_bg", "table_header");
    let header_row = Row::new(header_cells).style(header_row_style);
    for dist_analysis in &results.distribution_analyses {
        // The verdict in the colors of its p-value; no clear fit in the rejected one.
        let type_color = match dist_analysis.distribution_type {
            DistributionType::Unknown => theme.get("outlier_marker"),
            DistributionType::Constant => theme.get("text_primary"),
            _ => pvalue_style(dist_analysis.confidence, theme)
                .fg
                .unwrap_or_else(|| theme.get("text_primary")),
        };

        // Outlier count with percentage
        let outlier_text = if dist_analysis.outliers.total_count > 0 {
            format!(
                "{} ({:.1}%)",
                dist_analysis.outliers.total_count, dist_analysis.outliers.percentage
            )
        } else {
            "0 (0.0%)".to_string()
        };

        // Relaxed outlier color thresholds - red only for very high percentages that might indicate data errors
        let outlier_style = if dist_analysis.outliers.percentage > 20.0 {
            // Red: very high outlier percentage (>20%) - might indicate data errors
            Style::default().fg(theme.get("outlier_marker"))
        } else if dist_analysis.outliers.percentage > 5.0 {
            // Yellow for moderate outliers (5-20%)
            Style::default().fg(theme.get("distribution_skewed"))
        } else {
            // Default (white) for low outlier percentages (0-5%)
            Style::default()
        };

        // Get skewness and kurtosis values for styling
        let skewness_value = dist_analysis.characteristics.skewness.abs();
        let kurtosis_value = dist_analysis.characteristics.kurtosis;

        // Skewness color coding: similar to describe table
        let skewness_style = if skewness_value >= 3.0 {
            Style::default().fg(theme.get("outlier_marker"))
        } else if skewness_value >= 1.0 {
            Style::default().fg(theme.get("distribution_skewed"))
        } else {
            Style::default()
        };

        // Kurtosis color coding: 3.0 is normal, high/low is notable
        let kurtosis_style = if (kurtosis_value - 3.0).abs() >= 3.0 {
            Style::default().fg(theme.get("outlier_marker"))
        } else if (kurtosis_value - 3.0).abs() >= 1.0 {
            Style::default().fg(theme.get("distribution_skewed"))
        } else {
            Style::default()
        };

        let pvalue_text = verdict_pvalue(dist_analysis);
        let pvalue_style = pvalue_style(dist_analysis.confidence, theme);

        // Shapiro-Wilk statistic and p-value formatting
        let sw_stat_text = dist_analysis
            .characteristics
            .shapiro_wilk_stat
            .map(|s| format!("{:.3}", s))
            .unwrap_or_else(|| "N/A".to_string());
        let sw_pvalue_text = dist_analysis
            .characteristics
            .shapiro_wilk_pvalue
            .map(format_pvalue)
            .unwrap_or_else(|| "N/A".to_string());

        // Color coding for SW p-value: same semantics as p-value column
        // Green = normal (>0.05), Yellow = moderate (0.01-0.05), Red = non-normal (≤0.01)
        let sw_pvalue_style = dist_analysis
            .characteristics
            .shapiro_wilk_pvalue
            .map(|p| {
                if p > 0.05 {
                    Style::default().fg(theme.get("distribution_normal"))
                } else if p > 0.01 {
                    Style::default().fg(theme.get("distribution_skewed"))
                } else {
                    Style::default().fg(theme.get("outlier_marker"))
                }
            })
            .unwrap_or_default();

        // Build row with locked column name + visible stat values
        // Use explicit text_primary so column names stay visible (avoids black-on-black)
        let mut cells = vec![
            Cell::from(dist_analysis.column_name.as_str())
                .style(Style::default().fg(theme.get("text_primary"))),
        ];

        // Add visible statistic values
        for &stat_idx in &visible_stats {
            let cell = match stat_idx {
                0 => Cell::from(format!("{}", dist_analysis.distribution_type))
                    .style(Style::default().fg(type_color)),
                1 => Cell::from(pvalue_text.clone()).style(pvalue_style),
                2 => Cell::from(sw_stat_text.clone()),
                3 => Cell::from(sw_pvalue_text.clone()).style(sw_pvalue_style),
                4 => Cell::from(format!(
                    "{:.4}",
                    dist_analysis.characteristics.coefficient_of_variation
                ))
                .style(
                    if dist_analysis.characteristics.coefficient_of_variation > 1.0 {
                        Style::default().fg(theme.get("distribution_skewed")) // High variability
                    } else {
                        Style::default()
                    },
                ),
                5 => Cell::from(outlier_text.clone()).style(outlier_style),
                6 => Cell::from(format_num(dist_analysis.characteristics.skewness))
                    .style(skewness_style),
                7 => Cell::from(format_num(dist_analysis.characteristics.kurtosis))
                    .style(kurtosis_style),
                _ => Cell::from(""),
            };
            cells.push(cell);
        }

        rows.push(Row::new(cells));
    }

    let mut constraints = vec![Constraint::Length(locked_col_width)];
    for &stat_idx in &visible_stats {
        constraints.push(Constraint::Length(min_col_widths[stat_idx]));
    }

    if visible_stats.len() == num_stats && constraints.len() > 1 {
        let last_idx = constraints.len() - 1;
        constraints[last_idx] = Constraint::Fill(1);
    }

    let table = Table::new(rows, constraints)
        .header(header_row)
        .row_highlight_style(theme.highlight_style());

    StatefulWidget::render(table, area, buf, table_state);
}

fn render_correlation_matrix(
    results: &AnalysisResults,
    table_state: &mut TableState,
    selected_cell: &Option<(usize, usize)>,
    column_offset: usize,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    let correlation_matrix = match &results.correlation_matrix {
        Some(cm) => cm,
        None => {
            Paragraph::new("No correlation matrix available (need at least 2 numeric columns)")
                .centered()
                .render(area, buf);
            return;
        }
    };

    if correlation_matrix.columns.is_empty() {
        Paragraph::new("No numeric columns for correlation matrix")
            .centered()
            .render(area, buf);
        return;
    }

    let n = correlation_matrix.columns.len();

    // Calculate column widths - ensure they're wide enough for content
    let row_header_width = 20u16;
    let cell_width = 12u16; // Wide enough for "-1.00" format
    let column_spacing = 1u16; // Table widget adds 1 space between columns

    // Calculate how many columns can fit
    let available_width = area.width.saturating_sub(row_header_width);
    let mut used_width = 0u16;
    let mut visible_cols = 0usize;

    // Start from column_offset
    let start_col = column_offset.min(n.saturating_sub(1));

    for _col_idx in start_col..n {
        let needed = if visible_cols > 0 {
            column_spacing + cell_width
        } else {
            cell_width
        };

        if used_width + needed <= available_width {
            used_width += needed;
            visible_cols += 1;
        } else {
            break;
        }
    }

    visible_cols = visible_cols.max(1);
    let end_col = (start_col + visible_cols).min(n);

    let (selected_row, selected_col) = selected_cell.unwrap_or((n, n));

    let header_row_style = header_style(theme, "controls_bg", "table_header");
    let dim_header_style = header_style(theme, "controls_bg", "table_header");

    let mut header_cells = vec![Cell::from("")];
    for j in start_col..end_col {
        let col_name = &correlation_matrix.columns[j];
        let is_selected_col = selected_cell.is_some() && j == selected_col;
        let cell_style = if is_selected_col {
            dim_header_style
        } else {
            header_row_style
        };
        header_cells.push(Cell::from(col_name.as_str()).style(cell_style));
    }

    let header_row = Row::new(header_cells).style(header_row_style);

    // Data rows - only render visible rows (handled by TableState's visible_rows)
    // But we render all rows and let Table widget handle vertical scrolling
    let mut rows = Vec::new();
    for (i, col_name) in correlation_matrix.columns.iter().enumerate() {
        // Determine if this is the selected row
        let is_selected_row = selected_cell.is_some() && i == selected_row;

        // Row header cell - dim highlight if selected row
        let row_header_style = if is_selected_row {
            Style::default().bg(theme.get("surface"))
        } else {
            Style::default()
        };
        let mut cells = vec![Cell::from(col_name.as_str()).style(row_header_style)];

        for col_idx in start_col..end_col {
            let correlation = correlation_matrix.correlations[i][col_idx];
            let text_color = get_correlation_color(correlation, theme);

            let cell_text = if i == col_idx {
                "1.00".to_string()
            } else if correlation.is_nan() {
                "-".to_string()
            } else {
                format!("{:.2}", correlation)
            };

            let is_selected_cell =
                selected_cell.is_some() && i == selected_row && col_idx == selected_col;
            let is_in_selected_col = selected_cell.is_some() && col_idx == selected_col;

            let cell_style = if is_selected_cell {
                // Selected cell: use bright background with inverted text for visibility
                Style::default()
                    .fg(theme.get("text_inverse"))
                    .bg(theme.get("modal_border_active"))
            } else if is_selected_row || is_in_selected_col {
                // Selected row or column: dim background with colored text
                Style::default().fg(text_color).bg(theme.get("surface"))
            } else {
                // Normal cell: just text color
                Style::default().fg(text_color)
            };

            cells.push(Cell::from(cell_text).style(cell_style));
        }

        let row_style = if is_selected_row {
            Style::default().bg(theme.get("surface"))
        } else {
            Style::default()
        };

        rows.push(Row::new(cells).style(row_style));
    }

    // Build constraints - fixed widths to prevent clipping
    let mut constraints = vec![Constraint::Length(row_header_width)];
    for _ in 0..visible_cols {
        constraints.push(Constraint::Length(cell_width));
    }

    let last_idx = constraints.len().saturating_sub(1);
    if visible_cols == n && constraints.len() > 1 {
        constraints[last_idx] = Constraint::Fill(1);
    }

    let table = Table::new(rows, constraints)
        .header(header_row)
        .column_spacing(1);

    StatefulWidget::render(table, area, buf, table_state);
}

fn get_correlation_color(correlation: f64, theme: &Theme) -> Color {
    let abs_corr = correlation.abs();

    if abs_corr < 0.05 {
        // No correlation (close to 0) - dimmed
        theme.get("dimmed")
    } else if abs_corr < 0.3 {
        // Low correlation - normal text
        theme.get("text_primary")
    } else if correlation > 0.0 {
        // Positive correlation - keybind hints color (UI element, not chart)
        theme.get("keybind_hints")
    } else {
        // Negative correlation - error/warning color
        theme.get("outlier_marker")
    }
}

fn render_distribution_selector(
    dist: &DistributionAnalysis,
    selected_dist: DistributionType,
    selector_state: &mut TableState,
    focus: AnalysisFocus,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    // Tested families by p-value, then the ones that do not apply; the same order
    // the modal's ↑↓ walks.
    let distribution_scores: Vec<(DistributionType, Option<&FitOutcome>)> =
        crate::distribution_fit::listing_order(&dist.fits)
            .into_iter()
            .map(|family| (family, dist.fit(family)))
            .collect();

    // Find position of selected distribution in sorted list
    let selected_pos = distribution_scores
        .iter()
        .position(|(family, _)| *family == selected_dist)
        .unwrap_or(0);

    // Only sync selector state when absolutely necessary to prevent jumping during navigation
    // Trust the user's navigation state - only fix if selection is uninitialized or out of bounds
    let current_selection = selector_state.selected();
    if current_selection.is_none() {
        // Initial state: set to selected distribution position
        selector_state.select(Some(selected_pos));
    } else if let Some(current_idx) = current_selection {
        // Only fix if index is out of bounds - otherwise trust the current selection
        // This prevents the sync logic from interfering with user navigation
        if current_idx >= distribution_scores.len() {
            selector_state.select(Some(selected_pos));
        }
        // Otherwise, keep current selection (user is navigating or selection is valid)
    }

    // Create table rows from sorted list
    let rows: Vec<Row> = distribution_scores
        .iter()
        .enumerate()
        .map(|(sorted_idx, (family, outcome))| {
            let is_focused = focus == AnalysisFocus::DistributionSelector
                && selector_state.selected() == Some(sorted_idx);

            let name_style = if is_focused {
                header_style(theme, "controls_bg", "table_header")
            } else {
                Style::default().fg(theme.get("text_primary"))
            };

            // A family that does not apply has no p-value, and says so rather than
            // ranking a placeholder.
            let (p_text, pvalue_style) = match outcome.and_then(|outcome| outcome.test()) {
                Some(test) => (format_fit_pvalue(test), pvalue_style(test.p_value, theme)),
                None => ("n/a".to_string(), Style::default().fg(theme.get("dimmed"))),
            };

            Row::new(vec![
                Cell::from(family.to_string()).style(name_style),
                Cell::from(p_text).style(pvalue_style),
            ])
        })
        .collect();

    let h = header_style(theme, "controls_bg", "table_header");
    let header = Row::new(vec![
        Cell::from("Name").style(h),
        Cell::from("P-value").style(h),
    ]);

    let table = Table::new(
        rows,
        vec![
            Constraint::Fill(1),   // Name column takes remaining space
            Constraint::Length(7), // P-value column: "<0.001" or "0.000" = 7 chars max
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title("Distribution")
            .title_style(ratatui::style::Style::reset())
            .borders(Borders::ALL)
            .border_set(crate::glyphs::get().border)
            .border_style(Style::default().fg(theme.get("sidebar_border"))),
    )
    .row_highlight_style(theme.highlight_style());

    StatefulWidget::render(table, area, buf, selector_state);
}

/// What the Distribution detail's two plots draw: the Q-Q plot and the histogram.
#[derive(Clone, Copy)]
struct DistributionPlotConfig<'a> {
    dist: &'a DistributionAnalysis,
    dist_type: DistributionType,
    area: Rect,
    shared_y_axis_label_width: u16,
    theme: &'a Theme,
    unified_x_range: Option<(f64, f64)>,
    histogram_scale: HistogramScale,
    glyphs: &'a crate::glyphs::Glyphs,
}

fn render_distribution_settings(
    histogram_scale: HistogramScale,
    log_scale_unavailable: bool,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    let block = Block::default()
        .title("Settings")
        .title_style(ratatui::style::Style::reset())
        .borders(Borders::ALL)
        .border_set(crate::glyphs::get().border)
        .border_style(Style::default().fg(theme.get("sidebar_border")));

    // Settings content: Scale option
    let scale_label = "Scale:";
    let (scale_value, scale_value_style) = if log_scale_unavailable {
        // Log scale requested but can't be used (e.g., negative values)
        // Show "Linear" in warning color to indicate fallback
        ("Linear", Style::default().fg(theme.get("warning")))
    } else {
        match histogram_scale {
            HistogramScale::Linear => ("Linear", Style::default().fg(theme.get("text_primary"))),
            HistogramScale::Log => ("Log", Style::default().fg(theme.get("text_primary"))),
        }
    };

    // Layout for settings content (inside block)
    let inner_area = block.inner(area);
    let settings_layout = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(1), // Scale setting line
            Constraint::Fill(1),   // Remaining space
        ])
        .split(inner_area);

    // Scale setting: label on left, value on right
    let scale_layout = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Length(scale_label.chars().count() as u16 + 1), // Label + spacing
            Constraint::Fill(1),                                        // Value
        ])
        .split(settings_layout[0]);

    let scale_label_style = Style::default().fg(theme.get("text_secondary"));

    Paragraph::new(scale_label)
        .style(scale_label_style)
        .render(scale_layout[0], buf);

    Paragraph::new(scale_value)
        .style(scale_value_style)
        .render(scale_layout[1], buf);

    block.render(area, buf);
}

/// The tool list's width beside a result: a third of the screen, at most 32.
pub(crate) fn sidebar_width(width: u16) -> u16 {
    32u16.min(width / 3)
}

/// Where a tool's result goes: under the one-line header, left of the tool list.
/// The Sample form fills it before a tool's first run.
pub(crate) fn main_pane(area: Rect) -> Rect {
    Rect {
        y: area.y + 1,
        height: area.height.saturating_sub(1),
        width: area.width.saturating_sub(sidebar_width(area.width)),
        ..area
    }
}

/// The Analysis Tools list, the same beside every tool: the cursor carries the
/// rail and the tint, the tool on screen carries the accent.
pub(crate) fn render_sidebar(
    area: Rect,
    buf: &mut Buffer,
    sidebar_state: &mut TableState,
    selected_tool: Option<AnalysisTool>,
    focus: AnalysisFocus,
    theme: &Theme,
) {
    let tools = [
        ("Describe", AnalysisTool::Describe),
        ("Distribution Analysis", AnalysisTool::DistributionAnalysis),
        ("Correlation Matrix", AnalysisTool::CorrelationMatrix),
        ("Data Quality", AnalysisTool::DataQuality),
    ];

    let text_primary = theme.get("text_primary");
    // The focused row takes the theme's highlight, like the main table,
    // even when controls_bg is "default"/none.
    let focused_style = theme.highlight_style();

    // The one selection idiom: the cursor carries the rail and the tint,
    // and the tool whose results are on screen carries the accent, the way
    // an active tab does.
    let g = crate::glyphs::get();
    let accent = theme.get("accent_bright");
    let rail_color = theme.get("accent");
    let items: Vec<ListItem> = tools
        .iter()
        .enumerate()
        .map(|(idx, (name, tool))| {
            let is_selected = selected_tool == Some(*tool);
            let is_focused =
                focus == AnalysisFocus::Sidebar && sidebar_state.selected() == Some(idx);
            let rail = if is_focused { g.rail } else { " " };
            let name_style = if is_selected {
                Style::default()
                    .fg(accent)
                    .add_modifier(ratatui::style::Modifier::BOLD)
            } else {
                Style::default().fg(text_primary)
            };
            let line = ratatui::text::Line::from(vec![
                ratatui::text::Span::styled(rail, Style::default().fg(rail_color)),
                ratatui::text::Span::styled(format!(" {}", name), name_style),
            ]);
            let style = if is_focused {
                focused_style
            } else {
                Style::default()
            };
            ListItem::new(line).style(style)
        })
        .collect();

    let border_color = if focus == AnalysisFocus::Sidebar {
        theme.get("modal_border_active")
    } else {
        theme.get("modal_border")
    };
    let block = Block::default()
        .title("Analysis Tools")
        .title_style(ratatui::style::Style::reset())
        .borders(Borders::ALL)
        .border_set(crate::glyphs::get().border)
        .border_style(Style::default().fg(border_color));

    let list = List::new(items).block(block);

    Widget::render(list, area, buf);
}

fn render_distribution_histogram(config: DistributionPlotConfig, buf: &mut Buffer) {
    // Use BarChart widget to show histogram comparing data vs theoretical distribution
    // Use fixed-width bins that span both data range and theoretical distribution range
    let DistributionPlotConfig {
        dist,
        dist_type,
        area,
        shared_y_axis_label_width,
        theme,
        unified_x_range,
        histogram_scale,
        glyphs: g,
    } = config;
    let sorted_data = &dist.sorted_sample_values;

    if sorted_data.is_empty() || sorted_data.len() < 3 {
        Paragraph::new("Insufficient data for histogram")
            .centered()
            .render(area, buf);
        return;
    }

    let n = sorted_data.len();

    // Determine bin range: use percentile-based robust range (P1-P99) for all distributions
    // This is a best practice that gives more visual space to the bulk of data while
    // still showing outliers in edge bins. Matches professional tools like Observable Canvases.
    let data_min = sorted_data[0];
    let data_max = sorted_data[n - 1];
    let data_range = data_max - data_min;

    if data_range <= 0.0 {
        // Constant data: all values are the same
        Paragraph::new("Constant data: all values are identical")
            .centered()
            .render(area, buf);
        return;
    }

    // Use unified X-axis range (strict data range, no padding or extensions)
    // This keeps both Q-Q plot and histogram in sync and ensures log scale works correctly
    let (hist_min, hist_max, hist_range) = if let Some((unified_min, unified_max)) = unified_x_range
    {
        // Use unified range directly - it's already the strict data range
        let range = unified_max - unified_min;
        (unified_min, unified_max, range)
    } else {
        // Fallback: use actual data range (shouldn't happen if unified_x_range is always provided)
        (data_min, data_max, data_range)
    };

    // Calculate dynamic number of bins based on available width
    // This ensures bars fill the horizontal space and look dense at all widths

    let y_axis_gap = 1u16; // Minimal gap between labels and plot area (needed to prevent bars from extending outside)
    let total_y_axis_space = shared_y_axis_label_width + y_axis_gap;

    // Calculate available width for bars - must match Chart widget's plot area exactly
    // Chart widget reserves space for Y-axis labels internally, using remaining width for plot
    // Less the axis line itself, which the plot starts after.
    let available_width = area.width.saturating_sub(total_y_axis_space + 1);
    let bar_gap = 1u16;
    let group_gap = 1u16;
    let gap_width = bar_gap + group_gap;

    // Target bar width: aim for 6-8 pixels per bar for good density
    // Calculate optimal number of bins to fill available width
    // Formula: available_width = num_bins * bar_width + (num_bins - 1) * gap_width
    // Rearranging: num_bins = (available_width + gap_width) / (bar_width + gap_width)
    let target_bar_width = 7.0; // Target bar width in pixels
    let optimal_num_bins = ((available_width as f64 + gap_width as f64)
        / (target_bar_width + gap_width as f64)) as usize;

    // Clamp to reasonable bounds: minimum 5 bins, maximum 60 bins
    // Fewer bins for very narrow displays, more bins for wide displays
    // Increased max to 60 to better utilize ultrawide displays
    let num_bins = optimal_num_bins.clamp(5, 60);

    // Use log-scale binning if user has selected log scale and data is positive
    // Log-scale binning is standard practice for power law distributions and wide dynamic ranges
    // Check actual data values, not histogram range (which may include padding or theoretical bounds)
    let all_data_positive = sorted_data.iter().all(|&v| v > 0.0);
    // For log scale, ensure hist_min is positive (adjust if needed)
    let (log_hist_min, log_hist_max) =
        if matches!(histogram_scale, HistogramScale::Log) && all_data_positive {
            // Use actual data min/max for log scale to avoid issues with padding or theoretical bounds
            let actual_min = sorted_data[0];
            let actual_max = sorted_data[sorted_data.len() - 1];
            // Ensure minimum is positive for log scale
            if actual_min > 0.0 {
                (actual_min, actual_max)
            } else {
                // Can't use log scale if data includes 0
                (hist_min, hist_max)
            }
        } else {
            (hist_min, hist_max)
        };
    let use_log_scale = matches!(histogram_scale, HistogramScale::Log)
        && all_data_positive
        && log_hist_min > 0.0
        && log_hist_max > log_hist_min;

    let (bin_boundaries, bin_width): (Vec<f64>, f64) = if use_log_scale {
        // Log-scale binning: bins with equal width in log space
        // This ensures each bin represents roughly equal multiplicative range
        // Use adjusted range based on actual data values
        let log_min = log_hist_min.ln();
        let log_max = log_hist_max.ln();
        let log_range = log_max - log_min;
        let log_bin_width = log_range / num_bins as f64;

        let boundaries: Vec<f64> = (0..=num_bins)
            .map(|i| {
                let log_value = log_min + (i as f64) * log_bin_width;
                log_value.exp()
            })
            .collect();

        // For log scale, calculate average bin width for use in theoretical PDF calculations
        // This is approximate but needed for compatibility
        let log_range_linear = log_hist_max - log_hist_min;
        let avg_bin_width = log_range_linear / num_bins as f64;
        (boundaries, avg_bin_width)
    } else {
        // Linear binning for all other distributions
        let bin_width = hist_range / num_bins as f64;
        let boundaries: Vec<f64> = (0..=num_bins)
            .map(|i| hist_min + (i as f64) * bin_width)
            .collect();
        (boundaries, bin_width)
    };

    // Count data points in each bin
    let mut data_bin_counts = vec![0; num_bins];
    for &val in sorted_data {
        for (i, boundaries) in bin_boundaries.windows(2).enumerate().take(num_bins) {
            if val >= boundaries[0]
                && (val < boundaries[1] || (i == num_bins - 1 && val <= boundaries[1]))
            {
                data_bin_counts[i] += 1;
                break;
            }
        }
    }

    // Expected counts from the fit every view of this family uses, by the CDF across
    // each bin: exact for log-scaled and whole-number bins, where a density at the
    // center is not. A family that does not apply draws no overlay.
    let fitted = dist
        .fit(dist_type)
        .and_then(|outcome| outcome.test())
        .map(|test| test.fitted);
    let theory_probs: Vec<f64> = match &fitted {
        Some(fitted) => bin_boundaries
            .windows(2)
            .enumerate()
            .map(|(i, edges)| {
                let upper = if i + 1 == num_bins {
                    fitted.cdf(edges[1])
                } else {
                    fitted.cdf_below(edges[1])
                };
                (upper - fitted.cdf_below(edges[0])).max(0.0)
            })
            .collect(),
        None => vec![0.0; num_bins],
    };

    // Convert probabilities to expected counts
    let theory_bin_counts: Vec<f64> = theory_probs.iter().map(|&prob| prob * n as f64).collect();

    // Normalize values for display (find the maximum for scaling)
    let max_data = data_bin_counts.iter().cloned().fold(0, usize::max);
    let max_theory = theory_bin_counts.iter().cloned().fold(0.0, f64::max);
    // A row of headroom above the tallest bar, where the axis title sits; the labels
    // are read off this, so the scale stays true.
    let plot_rows = area.height.saturating_sub(3).max(2) as f64;
    let global_max =
        (max_data.max(max_theory as usize).max(1) as f64 * plot_rows / (plot_rows - 1.0)).ceil();

    // Use the shared label width calculated in the caller
    // This ensures both histogram and Q-Q plot use the same padding for alignment
    let y_axis_label_width = shared_y_axis_label_width;

    // Recalculate total_y_axis_space using the shared width
    let total_y_axis_space = y_axis_label_width + y_axis_gap;

    // Bin centers for x-axis positioning (value at center of each bin)
    let bin_centers: Vec<f64> = (0..num_bins)
        .map(|i| (bin_boundaries[i] + bin_boundaries[i + 1]) / 2.0)
        .collect();

    // Create data bars - use BarChart for actual bars
    let mut data_bars = Vec::new();

    for (&data_count, _) in data_bin_counts.iter().zip(bin_centers.iter()) {
        // Calculate normalized bar height (0-100 scale for BarChart)
        let data_height = if global_max > 0.0 {
            ((data_count as f64 / global_max) * 100.0) as u64
        } else {
            0
        };

        // No bar labels - Chart widget overlay provides x-axis labels
        // This prevents duplicate labels overlapping with Chart's x-axis labels
        // No value or label: the axes say what a bar's height and place mean.
        let data_bar = Bar::default()
            .value(data_height)
            .text_value(String::new())
            .style(Style::default().fg(theme.get("primary_chart_series_color")));

        data_bars.push(data_bar);
    }

    // Calculate dynamic bar width to use available space
    // num_bins is dynamic, so recalculate bar_width to fill the space optimally
    // Ensure bars extend all the way to the right edge by using all available width
    let total_gaps = (num_bins - 1) as u16 * gap_width;
    let total_bar_space = available_width.saturating_sub(total_gaps);

    // Calculate bar width to fill available space - ensure minimum width of 1 pixel
    // Use floor to ensure we don't exceed available space, but recalculate to use full width
    let calculated_bar_width = (total_bar_space as f64 / num_bins as f64).floor() as u16;
    let bar_width = calculated_bar_width.max(1);

    // Recalculate to ensure we're using full width - adjust if there's leftover space
    // This ensures bars extend all the way to the right edge without gaps
    let total_used_width = (bar_width * num_bins as u16) + total_gaps;
    let remaining_space = available_width.saturating_sub(total_used_width);

    // If there's leftover space, distribute it to bars to fill the width completely
    // At large widths, ensure all space is utilized by distributing evenly
    let final_bar_width = if remaining_space > 0 && num_bins > 0 {
        // Distribute all remaining space across bars
        // Calculate exact extra width per bar to fill completely
        let extra_per_bar = remaining_space / num_bins as u16;
        bar_width + extra_per_bar
    } else {
        bar_width
    };

    // Render data bars using BarChart
    // The rows of the chart's plot: below its title, above its axis line and labels.
    let title_height = 1u16;
    let x_axis_height = 2u16;
    let chart_inner_top = area.top() + title_height;
    let chart_inner_height = area
        .height
        .saturating_sub(title_height)
        .saturating_sub(x_axis_height);

    // Exactly the overlay's plot area: bar `i` starts where bin `i` does. Shifting the
    // bars right to meet the overlay put the first bin's bar over the second bin and
    // drew the last one past the axis, onto whatever sits beside the chart.
    let bar_plot_left = area.left().saturating_add(total_y_axis_space + 1);
    let bar_plot_area = Rect::new(
        bar_plot_left,
        chart_inner_top,
        available_width.min(area.right().saturating_sub(bar_plot_left)),
        chart_inner_height,
    );

    let barchart = BarChart::default()
        .block(Block::default()) // No borders in sub-area - borders handled separately
        .data(BarGroup::default().bars(&data_bars))
        // The same 0-100 scale the curve and the labels use; left to itself the chart
        // scales to its tallest bar and the curve no longer measures against the bars.
        .max(100)
        .bar_set(g.plot.column_set())
        .bar_width(final_bar_width)
        .bar_gap(bar_gap)
        .group_gap(group_gap);

    // The fit's expected counts, drawn behind the bars; sampled densely enough that
    // braille renders it as a line.
    let num_samples = (available_width as usize * 15).clamp(1500, 10000);

    let height = |count: f64| {
        if global_max > 0.0 {
            count / global_max * 100.0
        } else {
            0.0
        }
    };
    let theory_points: Vec<(f64, f64)> = match &fitted {
        // A continuous family on linear bins is drawn as its density, scaled to a bin's
        // count: a smooth curve rather than a staircase.
        Some(fitted) if !fitted.discrete() && !use_log_scale && hist_range > 0.0 => (0
            ..num_samples)
            .map(|i| {
                let x = hist_min + i as f64 / (num_samples - 1) as f64 * hist_range;
                (x, height(fitted.density(x) * bin_width * n as f64))
            })
            .filter(|(_, y)| y.is_finite())
            .collect(),
        // Counts, and log-scaled bins, by each bin's expected count at its center.
        Some(_) => bin_centers
            .iter()
            .zip(&theory_bin_counts)
            .map(|(center, count)| (*center, height(*count)))
            .collect(),
        None => Vec::new(),
    };

    // Dense points in the line mark read as a continuous curve.
    let marker = g.plot.line;

    let theory_dataset = Dataset::default()
        .name("") // Empty name to prevent legend from appearing
        .marker(marker)
        .graph_type(GraphType::Scatter)
        .style(Style::default().fg(theme.get("secondary_chart_series_color")))
        .data(&theory_points);

    // Create Chart widget with scatter plot overlay
    // Configure axes to match BarChart coordinate system exactly:
    // - X-axis: range (hist_min to hist_max) - matches bin range
    // - Y-axis: normalized height range (0 to 100) - matches bar normalization
    // Use same border style as BarChart for coordinate alignment
    // Add x-axis labels with more tick marks for better readability
    // Use same x-axis label format as Q-Q plot: 3 labels (min, middle, max) with {:.1} formatting
    // Use histogram range values to align with bars
    // hist_min is already clamped to >= 0 for non-negative data, so use it directly
    let x_labels = vec![
        Span::styled(
            format!("{:.1}", hist_min),
            Style::default()
                .fg(theme.get("text_secondary"))
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.1}", (hist_min + hist_max) / 2.0)),
        Span::styled(
            format!("{:.1}", hist_max),
            Style::default()
                .fg(theme.get("text_secondary"))
                .add_modifier(Modifier::BOLD),
        ),
    ];

    let theory_chart = Chart::new(vec![theory_dataset])
        .block(
            Block::default()
                .title(format!("Histogram vs {dist_type}"))
                .title_style(ratatui::style::Style::reset())
                .title_alignment(ratatui::layout::Alignment::Center)
                .padding(ratatui::widgets::Padding::new(1, 0, 0, 0)), // Extra top padding to separate title from chart
        )
        .x_axis(
            Axis::default()
                .bounds([hist_min, hist_max]) // Use histogram range to align with bars (hist_min already clamped for non-negative data)
                .style(Style::default().fg(theme.get("text_secondary")))
                .labels(x_labels), // Show x-axis labels with histogram range
        )
        .y_axis(
            Axis::default()
                .title("Counts")
                .style(Style::default().fg(theme.get("text_secondary")))
                .bounds([0.0, 100.0])
                .labels({
                    // Use dynamic label width calculated earlier
                    // y_axis_label_width already includes +1 for padding, so use it directly for formatting
                    // This ensures alignment with Q-Q plot using actual label lengths
                    let label_width = y_axis_label_width as usize;
                    vec![
                        // Bottom label: 0 counts (right-aligned to fixed width)
                        Span::styled(
                            format!("{:>width$}", 0, width = label_width),
                            Style::default()
                                .fg(theme.get("text_secondary"))
                                .add_modifier(Modifier::BOLD),
                        ),
                        // Middle label: half of max counts (right-aligned)
                        Span::styled(
                            format!(
                                "{:>width$}",
                                (global_max / 2.0) as usize,
                                width = label_width
                            ),
                            Style::default().fg(theme.get("text_secondary")),
                        ),
                        // Top label: max counts (right-aligned)
                        Span::styled(
                            format!("{:>width$}", global_max as usize, width = label_width),
                            Style::default()
                                .fg(theme.get("text_secondary"))
                                .add_modifier(Modifier::BOLD),
                        ),
                    ]
                }),
        )
        .hidden_legend_constraints((Constraint::Length(0), Constraint::Length(0))); // Hide legend

    // Render Chart overlay to full area (no borders)
    // Chart widget will automatically handle its own inner layout for x-axis labels
    // The bars, then the chart laid over them from a buffer of its own: its axes,
    // labels and curve, except where the curve crosses a bar. Drawn straight over the
    // bars, each braille cell of the curve replaced a block and cut a notch in the bar;
    // drawn under them, the bar chart's blank cells erased the curve and the axis title.
    barchart.render(bar_plot_area, buf);
    let mut overlay = Buffer::empty(area);
    theory_chart.render(area, &mut overlay);
    g.plot.redraw_axes(area, &mut overlay);
    let is_bar = |symbol: &str| g.plot.column_eighths.contains(&symbol);
    for y in area.top()..area.bottom() {
        for x in area.left()..area.right() {
            let cell = &overlay[(x, y)];
            let symbol = cell.symbol();
            if symbol == " " || (PlotMarks::is_mark(marker, symbol) && is_bar(buf[(x, y)].symbol()))
            {
                continue;
            }
            buf[(x, y)] = cell.clone();
        }
    }
}

fn render_qq_plot(config: DistributionPlotConfig, buf: &mut Buffer) {
    let DistributionPlotConfig {
        dist,
        dist_type,
        area,
        shared_y_axis_label_width,
        theme,
        unified_x_range,
        glyphs: g,
        ..
    } = config;
    // Use Chart widget for Q-Q plot: Data quantiles vs Theoretical quantiles
    // Use sorted_sample_values and position-based quantiles (not just 5 percentiles)
    let sorted_data = &dist.sorted_sample_values;

    if sorted_data.is_empty() || sorted_data.len() < 3 {
        Paragraph::new("Insufficient data for Q-Q plot (need at least 3 points)")
            .centered()
            .render(area, buf);
        return;
    }

    // The fit's quantiles at each plotting position, computed with the fit.
    let Some(theoretical) = dist.qq(dist_type) else {
        let reason = match dist.fit(dist_type) {
            Some(FitOutcome::NotApplicable(reason)) => format!("{dist_type} {reason}"),
            _ => format!("{dist_type} was not fitted"),
        };
        Paragraph::new(reason).centered().render(area, buf);
        return;
    };
    let qq_data: Vec<(f64, f64)> = theoretical
        .iter()
        .zip(sorted_data)
        .map(|(t, d)| (*t, *d))
        .filter(|(t, _)| t.is_finite())
        .collect();
    if qq_data.len() < 3 {
        Paragraph::new("Insufficient data for Q-Q plot (need at least 3 points)")
            .centered()
            .render(area, buf);
        return;
    }
    let n = qq_data.len();

    // Find data ranges for both axes
    // X-axis (Theoretical): calculated from probability percentiles via inverse CDF
    // Y-axis (Empirical): raw sorted sample data (preserve all values, even if "impossible")
    let theory_min = qq_data
        .iter()
        .map(|(t, _)| *t)
        .fold(f64::INFINITY, f64::min);
    let theory_max = qq_data
        .iter()
        .map(|(t, _)| *t)
        .fold(f64::NEG_INFINITY, f64::max);
    let theory_range = theory_max - theory_min;

    let data_min = qq_data
        .iter()
        .map(|(_, d)| *d)
        .fold(f64::INFINITY, f64::min);
    let data_max = qq_data
        .iter()
        .map(|(_, d)| *d)
        .fold(f64::NEG_INFINITY, f64::max);
    let data_range = data_max - data_min;

    // Only require data_range > 0 (allow plotting even if theoretical range is small/zero)
    // This handles cases where distribution doesn't match (e.g., negative data vs strictly positive distribution)
    if data_range <= 0.0 {
        Paragraph::new("Insufficient data range for Q-Q plot")
            .centered()
            .render(area, buf);
        return;
    }

    // Use unified X-axis range if provided for visual alignment with histogram
    // Otherwise, handle case where all theoretical quantiles are the same (theory_range = 0)
    let (theory_min_plot, theory_max_plot) =
        if let Some((unified_min, unified_max)) = unified_x_range {
            // Use unified range to align with histogram
            (unified_min, unified_max)
        } else if theory_range <= 0.0 || !theory_min.is_finite() || !theory_max.is_finite() {
            // Fallback: use data range (no padding)
            (data_min, data_max)
        } else {
            // Use theoretical range, but clamp to data range to keep charts in sync
            (theory_min.max(data_min), theory_max.min(data_max))
        };

    // Create robust reference line through Q1 and Q3 quartiles
    // This works even when domains don't overlap (e.g., negative data vs positive distribution)
    let q1_idx = (n as f64 * 0.25).floor() as usize;
    let q3_idx = (n as f64 * 0.75).floor() as usize;
    let q1_idx = q1_idx.min(n - 1);
    let q3_idx = q3_idx.min(n - 1);

    let (theory_q1, data_q1) = if q1_idx < qq_data.len() {
        qq_data[q1_idx]
    } else {
        qq_data[0]
    };
    let (theory_q3, data_q3) = if q3_idx < qq_data.len() {
        qq_data[q3_idx]
    } else {
        qq_data[qq_data.len() - 1]
    };

    // Calculate robust reference line through (theory_q1, data_q1) and (theory_q3, data_q3)
    // This works even when domains don't overlap (e.g., negative data vs positive distribution)
    let theory_diff = theory_q3 - theory_q1;
    let reference_line = if theory_diff.abs() > 1e-10 {
        // Normal case: calculate slope and extend line to cover plot range (no padding)
        let slope = (data_q3 - data_q1) / theory_diff;
        let x_start = theory_min_plot;
        let x_end = theory_max_plot;
        let y_start = slope * (x_start - theory_q1) + data_q1;
        let y_end = slope * (x_end - theory_q1) + data_q1;
        vec![(x_start, y_start), (x_end, y_end)]
    } else {
        // Degenerate case: all theoretical quantiles are the same (theory_range ≈ 0)
        // Use horizontal line through data median to show the mismatch (no padding)
        let y_median = (data_q1 + data_q3) / 2.0;
        vec![(theory_min_plot, y_median), (theory_max_plot, y_median)]
    };

    // Create datasets
    // Use appropriate marker based on point density
    let marker = if qq_data.len() > 100 {
        g.plot.line
    } else {
        g.plot.point
    };

    let datasets = vec![
        // Diagonal reference line
        Dataset::default()
            .name("") // Empty name to hide from legend
            .marker(marker)
            .style(Style::default().fg(theme.get("secondary_chart_series_color")))
            .graph_type(GraphType::Line)
            .data(&reference_line),
        // Q-Q plot data points
        Dataset::default()
            .name("") // Empty name to hide from legend
            .marker(marker)
            .style(Style::default().fg(theme.get("primary_chart_series_color")))
            .graph_type(GraphType::Scatter)
            .data(&qq_data),
    ];

    // Create X-axis labels using plot range
    let x_labels = vec![
        Span::styled(
            format!("{:.1}", theory_min_plot),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.1}", (theory_min_plot + theory_max_plot) / 2.0)),
        Span::styled(
            format!("{:.1}", theory_max_plot),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    // Use the shared label width calculated in the caller
    // This ensures both histogram and Q-Q plot use the same padding for alignment
    let label_width = shared_y_axis_label_width as usize;
    let y_labels = vec![
        // Bottom label: data_min (right-aligned to fixed width)
        Span::styled(
            format!("{:>width$.1}", data_min, width = label_width),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        // Middle label: average (right-aligned)
        Span::raw(format!(
            "{:>width$.1}",
            (data_min + data_max) / 2.0,
            width = label_width
        )),
        // Top label: data_max (right-aligned)
        Span::styled(
            format!("{:>width$.1}", data_max, width = label_width),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    let chart = Chart::new(datasets)
        .block(
            Block::default()
                .title(format!("Q-Q Plot vs {dist_type}"))
                .title_style(ratatui::style::Style::reset())
                .title_alignment(ratatui::layout::Alignment::Center)
                .padding(ratatui::widgets::Padding::new(1, 0, 0, 0)), // Extra top padding to separate title from chart
        )
        .x_axis(
            Axis::default()
                .title("Theoretical Values")
                .style(Style::default().fg(theme.get("text_secondary")))
                .bounds([theory_min_plot, theory_max_plot])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .title("Data Values")
                .style(Style::default().fg(theme.get("text_secondary"))) // Axis line should be gray
                .bounds([data_min, data_max])
                .labels(y_labels), // Labels styled cyan explicitly above
        )
        .hidden_legend_constraints((Constraint::Length(0), Constraint::Length(0))); // Hide legend

    chart.render(area, buf);
    g.plot.redraw_axes(area, buf);
}

fn render_condensed_statistics(
    dist: &DistributionAnalysis,
    _selected_dist_type: DistributionType,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    // One line: the fit found, Shapiro-Francia, skew, kurtosis, median, mean, std, CV
    // Use explicit theme colors so text is always visible (avoids black-on-black for some themes)
    let chars = &dist.characteristics;
    let label_style = Style::default().fg(theme.get("text_primary"));
    let value_style = Style::default().fg(theme.get("text_primary"));

    let mut line_parts = Vec::new();

    // What the values were found to fit, before anything about the family the plots
    // compare them with: choosing a family below is a comparison, not a finding.
    line_parts.push(Span::styled("Fit: ", label_style));
    line_parts.push(Span::styled(
        match dist.distribution_type {
            DistributionType::Unknown | DistributionType::Constant => {
                dist.distribution_type.to_string()
            }
            family => format!("{family} (p {})", verdict_pvalue(dist)),
        },
        value_style,
    ));
    line_parts.push(Span::styled(" ", value_style));

    if let (Some(sw_stat), Some(sw_p)) = (chars.shapiro_wilk_stat, chars.shapiro_wilk_pvalue) {
        line_parts.push(Span::styled("SF: ", label_style));
        line_parts.push(Span::styled(
            if sw_p < 0.001 {
                format!("{sw_stat:.3} (p<0.001)")
            } else {
                format!("{sw_stat:.3} (p={sw_p:.3})")
            },
            value_style,
        ));
        line_parts.push(Span::styled(" ", value_style));
    }

    line_parts.push(Span::styled("Skew: ", label_style));
    line_parts.push(Span::styled(format!("{:.2}", chars.skewness), value_style));
    line_parts.push(Span::styled(" ", value_style));

    line_parts.push(Span::styled("Kurt: ", label_style));
    line_parts.push(Span::styled(format!("{:.2}", chars.kurtosis), value_style));
    line_parts.push(Span::styled(" ", value_style));

    line_parts.push(Span::styled("Median: ", label_style));
    line_parts.push(Span::styled(
        format!("{:.2}", dist.percentiles.p50),
        value_style,
    ));
    line_parts.push(Span::styled(" ", value_style));

    line_parts.push(Span::styled("Mean: ", label_style));
    line_parts.push(Span::styled(format!("{:.2}", chars.mean), value_style));
    line_parts.push(Span::styled(" ", value_style));

    line_parts.push(Span::styled("Std: ", label_style));
    line_parts.push(Span::styled(format!("{:.2}", chars.std_dev), value_style));
    line_parts.push(Span::styled(" ", value_style));

    line_parts.push(Span::styled("CV: ", label_style));
    line_parts.push(Span::styled(
        format!("{:.3}", chars.coefficient_of_variation),
        value_style,
    ));

    let line = Line::from(line_parts);
    let lines = vec![line];

    Paragraph::new(lines).render(area, buf);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::numfmt::NumberFormat;

    fn settings(preset: &str, enabled: bool) -> NumberFormatSettings {
        NumberFormatSettings {
            format: NumberFormat::preset(preset).unwrap(),
            enabled,
            exclude: Vec::new(),
            align_numeric_right: true,
        }
    }

    fn analysis(mean: f64, std_dev: f64, sorted: Vec<f64>) -> DistributionAnalysis {
        use crate::statistics::{
            DistributionCharacteristics, OutlierAnalysis, PercentileBreakdown,
        };
        DistributionAnalysis {
            column_name: "close".into(),
            distribution_type: DistributionType::Normal,
            confidence: 0.0,
            fit_quality: 0.0,
            characteristics: DistributionCharacteristics {
                shapiro_wilk_stat: None,
                shapiro_wilk_pvalue: None,
                skewness: 0.0,
                kurtosis: 3.0,
                mean,
                median: mean,
                std_dev,
                variance: std_dev * std_dev,
                coefficient_of_variation: std_dev / mean,
                mode: None,
            },
            outliers: OutlierAnalysis {
                total_count: 0,
                percentage: 0.0,
                iqr_count: 0,
                zscore_count: 0,
                outlier_rows: Vec::new(),
            },
            percentiles: PercentileBreakdown {
                p1: 0.0,
                p5: 0.0,
                p25: 0.0,
                p50: 0.0,
                p75: 0.0,
                p95: 0.0,
                p99: 0.0,
            },
            sample_size: sorted.len(),
            sorted_sample_values: sorted,
            is_sampled: false,
            fits: Vec::new(),
            qq: Vec::new(),
        }
    }

    /// The histogram's bars sit on the axis they are drawn against: the first bin's
    /// bar in the first plot column, and nothing past the chart's right edge. They were
    /// shifted right by a bar and a half, onto the next bin and off the end.
    /// Busy at the low end, so the first bin has a bar, with a Normal fit.
    fn skewed_normal_fit() -> DistributionAnalysis {
        let mut values: Vec<f64> = (0..400).map(|i| 23.0 + (i % 20) as f64).collect();
        values.extend((0..100).map(|i| 23.0 + 3.18 * i as f64));
        values.sort_by(f64::total_cmp);
        let mut dist = analysis(100.0, 80.0, values);
        dist.fits = vec![(
            DistributionType::Normal,
            FitOutcome::Tested(FitTest {
                fitted: crate::distribution_fit::Fitted::Normal {
                    mean: 100.0,
                    sd: 80.0,
                },
                p_value: 0.005,
                beyond: 0,
                replicates: 199,
                tested_on: 500,
                aic: 0.0,
            }),
        )];
        dist
    }

    /// One of the Distribution detail's plots, in a 60x20 area of an 80x20 buffer.
    fn render_distribution_plot(
        dist: &DistributionAnalysis,
        g: &crate::glyphs::Glyphs,
        render: fn(DistributionPlotConfig, &mut Buffer),
    ) -> Buffer {
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let mut buf = Buffer::empty(Rect::new(0, 0, 80, 20));
        render(
            DistributionPlotConfig {
                dist,
                dist_type: DistributionType::Normal,
                area: Rect::new(0, 0, 60, 20),
                shared_y_axis_label_width: 5,
                theme: &theme,
                unified_x_range: Some((23.0, 341.1)),
                histogram_scale: HistogramScale::Linear,
                glyphs: g,
            },
            &mut buf,
        );
        buf
    }

    #[test]
    fn histogram_bars_stay_on_their_axis() {
        let dist = skewed_normal_fit();
        for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
            let buf = render_distribution_plot(&dist, g, render_distribution_histogram);
            let full = g.plot.column_eighths[7];
            let is_bar = |x: u16| (0..20).any(|y| buf[(x, y)].symbol() == full);
            // Labels, a space, the axis line: the plot's first column.
            assert!(is_bar(5 + 2), "the first bin starts at the axis");
            assert!(
                (60..80).all(|x| !is_bar(x)),
                "nothing is drawn past the chart"
            );
            // The fit's curve is drawn, and goes behind the bars rather than through
            // them: below the top of a bar, every cell is solid.
            let curve = |x: u16, y: u16| PlotMarks::is_mark(g.plot.line, buf[(x, y)].symbol());
            assert!(
                (0..80).any(|x| (0..20).any(|y| curve(x, y) && buf[(x, y)].symbol() != "\u{2800}")),
                "the curve is drawn"
            );
            for x in 0..80 {
                if let Some(top) = (0..20).find(|y| buf[(x, *y)].symbol() == full) {
                    assert!(
                        (top..20).all(|y| !curve(x, y)),
                        "a notch in the bar at column {x}"
                    );
                }
            }
        }
    }

    /// Under the ASCII set, both of the Distribution detail's plots draw ASCII only:
    /// bars, the fit's curve, the Q-Q points and every axis line.
    #[test]
    fn distribution_plots_are_ascii_under_the_ascii_set() {
        let g = crate::glyphs::ascii();
        let mut dist = skewed_normal_fit();
        let qq: Vec<f64> = (0..dist.sorted_sample_values.len())
            .map(|i| 23.0 + 318.0 * i as f64 / 499.0)
            .collect();
        dist.qq = vec![(DistributionType::Normal, qq)];
        for (name, render) in [
            (
                "histogram",
                render_distribution_histogram as fn(DistributionPlotConfig, &mut Buffer),
            ),
            ("Q-Q plot", render_qq_plot),
        ] {
            let text = rendered_text(&render_distribution_plot(&dist, g, render));
            assert!(text.is_ascii(), "{name}:\n{text}");
            assert!(
                text.contains('|') && text.contains("+-"),
                "{name} axes:\n{text}"
            );
        }
        let qq = rendered_text(&render_distribution_plot(&dist, g, render_qq_plot));
        assert!(qq.contains('*'), "the Q-Q points:\n{qq}");
    }

    #[test]
    fn counts_follow_the_data_table_grouping_setting() {
        // A user who turned grouping on should see it wherever they read
        // numbers, not just in the main table.
        assert_eq!(
            format_count(3_088_269, &settings("thousands", true)),
            "3,088,269"
        );
        assert_eq!(
            format_count(3_088_269, &settings("european", true)),
            "3.088.269"
        );
    }

    #[test]
    fn counts_are_raw_when_formatting_is_off() {
        assert_eq!(
            format_count(3_088_269, &settings("thousands", false)),
            "3088269"
        );
        assert_eq!(format_count(0, &settings("thousands", false)), "0");
    }

    #[test]
    fn counts_group_uniformly_with_no_magnitude_threshold() {
        assert_eq!(format_count(42, &settings("thousands", true)), "42");
        assert_eq!(format_count(1000, &settings("thousands", true)), "1,000");
        assert_eq!(format_count(10_000, &settings("thousands", true)), "10,000");
    }

    fn correlation_matrix(r: f64, pairs: usize) -> crate::statistics::CorrelationMatrix {
        crate::statistics::CorrelationMatrix {
            columns: vec!["price".to_string(), "volume".to_string()],
            correlations: vec![vec![1.0, r], vec![r, 1.0]],
            p_values: Some(vec![vec![0.0, 0.004], vec![0.004, 0.0]]),
            sample_sizes: vec![vec![0, pairs], vec![pairs, 0]],
        }
    }

    fn rendered_text(buf: &Buffer) -> String {
        let mut text = String::new();
        for y in 0..buf.area.height {
            for x in 0..buf.area.width {
                text.push_str(buf[(x, y)].symbol());
            }
            text.push('\n');
        }
        text
    }

    #[test]
    fn describe_shows_a_datetime_range_and_leaves_std_blank() {
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let results = crate::statistics::compute_describe_single_aggregation(
            &crate::statistics::describe_tests::temporal_frame(),
            &crate::statistics::describe_tests::temporal_frame()
                .schema()
                .clone(),
            6,
            None,
            0,
            false,
        )
        .unwrap();
        let area = Rect::new(0, 0, 220, 6);
        let mut buf = Buffer::empty(area);
        render_statistics_table(
            &results,
            &mut TableState::default(),
            0,
            area,
            &mut buf,
            &theme,
            1,
            &settings("thousands", false),
        );
        let text = rendered_text(&buf);
        let mut lines = text.lines();
        let header = lines.next().unwrap();
        let pickup = lines
            .find(|l| l.starts_with("pickup"))
            .unwrap_or_else(|| panic!("{text}"));
        // Each value sits under its own header.
        for (stat, value) in [
            ("Mean", "2024-12-31 22:47:55"),
            ("Std", "- "),
            ("Min", "2024-12-31 20:47:55"),
            ("25%", "2024-12-31 21:47:55"),
            ("50%", "2024-12-31 22:47:55"),
            ("75%", "2024-12-31 23:47:55"),
            ("Max", "2025-01-01 00:47:55"),
        ] {
            let x = header.find(stat).unwrap();
            assert!(pickup[x..].starts_with(value), "{stat}:\n{text}");
        }
    }

    #[test]
    fn correlation_detail_shows_the_pair_facts_the_matrix_holds() {
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let area = Rect::new(0, 0, 60, 8);
        let mut buf = Buffer::empty(area);
        render_correlation_pair_summary(
            &correlation_matrix(0.874, 42),
            (0, 1),
            50,
            area,
            &mut buf,
            &theme,
            &settings("thousands", false),
        );
        let text = rendered_text(&buf);
        assert!(text.contains("Pearson r: 0.874"), "{text}");
        assert!(text.contains("strong positive"), "{text}");
        let r_squared = crate::glyphs::get().r_squared;
        assert!(text.contains(&format!("{r_squared}: 0.764")), "{text}");
        assert!(text.contains("P-value: 0.004"), "{text}");
        assert!(text.contains("Pairs used: 42 of 50 rows"), "{text}");
    }

    #[test]
    fn correlation_detail_says_when_too_few_pairs_overlap() {
        // A pair with fewer than 3 overlapping values holds NaN in the matrix.
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let area = Rect::new(0, 0, 70, 8);
        let mut buf = Buffer::empty(area);
        render_correlation_pair_summary(
            &correlation_matrix(f64::NAN, 2),
            (0, 1),
            50,
            area,
            &mut buf,
            &theme,
            &settings("thousands", false),
        );
        let text = rendered_text(&buf);
        assert!(text.contains("Not enough overlapping values"), "{text}");
        assert!(!text.contains("Pearson r:"), "{text}");
    }

    #[test]
    fn correlation_words_match_the_color_boundaries() {
        assert_eq!(describe_correlation(0.01), "none");
        assert_eq!(describe_correlation(0.2), "weak positive");
        assert_eq!(describe_correlation(-0.5), "moderate negative");
        assert_eq!(describe_correlation(0.9), "strong positive");
        assert_eq!(describe_correlation(-0.9), "strong negative");
    }
}
