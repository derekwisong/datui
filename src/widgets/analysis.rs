use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Bar, BarChart, BarGroup, Block, Borders, Cell, Chart, Dataset, GraphType, List,
        ListItem, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::analysis_modal::{AnalysisFocus, AnalysisTool, AnalysisView, HistogramScale};
use crate::config::Theme;
use crate::statistics::{
    beta_pdf, chi_squared_pdf, gamma_pdf, gamma_quantile, geometric_pmf, geometric_quantile,
    students_t_pdf, weibull_pdf, AnalysisContext, AnalysisResults, DistributionAnalysis,
    DistributionType,
};
use crate::widgets::datatable::DataTableState;

pub struct AnalysisWidgetConfig<'a> {
    pub state: &'a DataTableState,
    pub results: Option<&'a AnalysisResults>,
    pub context: &'a AnalysisContext,
    pub view: AnalysisView,
    pub selected_tool: AnalysisTool,
    pub column_offset: usize,
    pub selected_correlation: Option<(usize, usize)>,
    pub focus: AnalysisFocus,
    pub selected_theoretical_distribution: DistributionType,
    pub histogram_scale: HistogramScale,
    pub theme: &'a Theme,
}

pub struct AnalysisWidget<'a> {
    _state: &'a DataTableState,
    results: Option<&'a AnalysisResults>,
    _context: &'a AnalysisContext,
    view: AnalysisView,
    selected_tool: AnalysisTool,
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
        // Sidebar width (~30 characters)
        let sidebar_width = 32u16;

        // Full-screen layout: breadcrumb, main area (no separate keybind hints line)
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Breadcrumb
                Constraint::Fill(1),   // Main area + sidebar
            ])
            .split(area);

        // Breadcrumb with style matching main window column headers
        // Show tool name for all analysis tools, with "(sampled)" if data is sampled
        let tool_name = match self.selected_tool {
            AnalysisTool::Describe => "Describe",
            AnalysisTool::DistributionAnalysis => "Distribution Analysis",
            AnalysisTool::CorrelationMatrix => "Correlation Matrix",
        };

        let breadcrumb_text = if let Some(results) = self.results {
            if results.sample_size.is_some() {
                format!("{} (sampled)", tool_name)
            } else {
                tool_name.to_string()
            }
        } else {
            tool_name.to_string()
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

        // Main content area: Show selected tool
        if let Some(results) = self.results {
            match self.selected_tool {
                AnalysisTool::Describe => {
                    render_statistics_table(
                        results,
                        self.table_state,
                        self.column_offset,
                        main_layout[0],
                        buf,
                        self.theme,
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
            }
        } else {
            Paragraph::new("Computing statistics...")
                .centered()
                .render(main_layout[0], buf);
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

        if dist_analysis.is_none() {
            Paragraph::new("No distribution selected")
                .centered()
                .render(area, buf);
            return;
        }

        let dist = dist_analysis.unwrap();

        // Layout: breadcrumb, main content (no keybind hints line)
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Breadcrumb
                Constraint::Fill(1),   // Main content
            ])
            .split(area);

        // Breadcrumb with column name and Escape hint on top right
        // Split breadcrumb area into left (title) and right (Escape hint)
        let breadcrumb_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Fill(1),   // Title on left
                Constraint::Length(8), // Escape hint on right ("Esc Back" = 8 chars)
            ])
            .split(layout[0]);

        let title_text = format!("Distribution Analysis: {}", dist.column_name);
        let header_row_style = header_style(self.theme, "controls_bg", "table_header");
        Paragraph::new(title_text)
            .style(header_row_style)
            .render(breadcrumb_layout[0], buf);

        Paragraph::new("Esc Back")
            .style(header_row_style)
            .right_aligned()
            .render(breadcrumb_layout[1], buf);

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

        // Q-Q plot approximation (larger, better aspect ratio)
        // Use selected theoretical distribution from selector
        render_qq_plot(
            dist,
            self.selected_theoretical_distribution,
            qq_plot_area,
            buf,
            shared_y_axis_label_width,
            self.theme,
            Some(unified_x_range),
        );

        // Histogram comparison (vertical bars)
        // Use selected theoretical distribution from selector
        // Check if log scale is requested but can't be used
        // Use actual data values, not unified range (which may include theoretical bounds and padding)
        let sorted_data = &dist.sorted_sample_values;
        let can_use_log_scale = !sorted_data.is_empty() && sorted_data.iter().all(|&v| v > 0.0);
        let log_scale_requested_but_unavailable =
            matches!(self.histogram_scale, HistogramScale::Log) && !can_use_log_scale;

        let histogram_config = HistogramRenderConfig {
            dist,
            dist_type: self.selected_theoretical_distribution,
            area: histogram_area,
            shared_y_axis_label_width,
            theme: self.theme,
            unified_x_range: Some(unified_x_range),
            histogram_scale: self.histogram_scale,
        };
        render_distribution_histogram(histogram_config, buf);

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
    }

    fn render_correlation_detail(self, _area: Rect, _buf: &mut Buffer) {
        // TODO: Implement correlation pair detail view
        // This will show relationship summary, scatter plot, and key statistics
    }
}

fn render_statistics_table(
    results: &AnalysisResults,
    table_state: &mut TableState,
    column_offset: usize,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
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
            let value_str = match *stat_name {
                "count" => col_stat.count.to_string(),
                "null_count" => col_stat.null_count.to_string(),
                "mean" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.mean))
                    .unwrap_or_else(|| "-".to_string()),
                "std" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.std))
                    .unwrap_or_else(|| "-".to_string()),
                "min" => {
                    if let Some(ref num_stats) = col_stat.numeric_stats {
                        format_num(num_stats.min)
                    } else if let Some(ref cat_stats) = col_stat.categorical_stats {
                        cat_stats.min.clone().unwrap_or_else(|| "-".to_string())
                    } else {
                        "-".to_string()
                    }
                }
                "25%" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.q25))
                    .unwrap_or_else(|| "-".to_string()),
                "50%" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.median))
                    .unwrap_or_else(|| "-".to_string()),
                "75%" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.q75))
                    .unwrap_or_else(|| "-".to_string()),
                "max" => {
                    if let Some(ref num_stats) = col_stat.numeric_stats {
                        format_num(num_stats.max)
                    } else if let Some(ref cat_stats) = col_stat.categorical_stats {
                        cat_stats.max.clone().unwrap_or_else(|| "-".to_string())
                    } else {
                        "-".to_string()
                    }
                }
                _ => "-".to_string(),
            };
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

    // Calculate which columns can fit with minimal spacing
    // Ratatui Table adds 1 space between columns by default
    // Account for spacing: total_width = locked_col + 1 (space) + sum(stat_cols) + (num_stat_cols - 1) * 1 (spacing between stat cols)
    let column_spacing = 1u16; // Default spacing between columns in ratatui Table

    // Available width for stat columns = total width - locked column - spacing between locked and first stat
    let available_width = area
        .width
        .saturating_sub(locked_col_width)
        .saturating_sub(column_spacing); // Space between locked column and first stat column

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
        let mut cells = vec![Cell::from(col_stat.name.as_str())
            .style(Style::default().fg(theme.get("text_primary")))];
        for &stat_idx in &visible_stats {
            let stat_name = stat_names[stat_idx];
            let value = match stat_name {
                "count" => col_stat.count.to_string(),
                "null_count" => col_stat.null_count.to_string(),
                "mean" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.mean))
                    .unwrap_or_else(|| "-".to_string()),
                "std" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.std))
                    .unwrap_or_else(|| "-".to_string()),
                "min" => {
                    if let Some(ref num_stats) = col_stat.numeric_stats {
                        format_num(num_stats.min)
                    } else if let Some(ref cat_stats) = col_stat.categorical_stats {
                        cat_stats.min.clone().unwrap_or_else(|| "-".to_string())
                    } else {
                        "-".to_string()
                    }
                }
                "25%" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.q25))
                    .unwrap_or_else(|| "-".to_string()),
                "50%" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.median))
                    .unwrap_or_else(|| "-".to_string()),
                "75%" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.q75))
                    .unwrap_or_else(|| "-".to_string()),
                "max" => {
                    if let Some(ref num_stats) = col_stat.numeric_stats {
                        format_num(num_stats.max)
                    } else if let Some(ref cat_stats) = col_stat.categorical_stats {
                        cat_stats.max.clone().unwrap_or_else(|| "-".to_string())
                    } else {
                        "-".to_string()
                    }
                }
                _ => "-".to_string(),
            };

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
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    // Use StatefulWidget for row selection
    StatefulWidget::render(table, area, buf, table_state);
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

/// Build header-style: bg+fg when bg_key is not Reset, else fg-only.
fn header_style(theme: &Theme, bg_key: &str, fg_key: &str) -> Style {
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
        "Shapiro-Wilk",
        "SW p-value",
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
            .map(|p| format!("{:.3}", p))
            .unwrap_or_else(|| "N/A".to_string());

        // Phase 6: Add p-value to column values
        let pvalue_text = format_pvalue(dist_analysis.confidence);

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
        // Color coding for distribution type based on fit quality only
        // Green = good fit (>0.75), Yellow = moderate (0.5-0.75), Red = poor (<0.5)
        let type_color = if dist_analysis.fit_quality > 0.75 {
            theme.get("distribution_normal")
        } else if dist_analysis.fit_quality > 0.5 {
            theme.get("distribution_skewed")
        } else {
            theme.get("outlier_marker")
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

        // Format p-value with color coding
        // Green = good (>0.05), Yellow = moderate (0.01-0.05), Red = poor (≤0.01)
        let pvalue_text = format_pvalue(dist_analysis.confidence);
        let pvalue_style = if dist_analysis.confidence > 0.05 {
            Style::default().fg(theme.get("distribution_normal"))
        } else if dist_analysis.confidence > 0.01 {
            Style::default().fg(theme.get("distribution_skewed"))
        } else {
            Style::default().fg(theme.get("outlier_marker"))
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
            .map(|p| format!("{:.3}", p))
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
        let mut cells = vec![Cell::from(dist_analysis.column_name.as_str())
            .style(Style::default().fg(theme.get("text_primary")))];

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
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

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
    let distributions = [
        ("Normal", DistributionType::Normal),
        ("Log-Normal", DistributionType::LogNormal),
        ("Uniform", DistributionType::Uniform),
        ("Power Law", DistributionType::PowerLaw),
        ("Exponential", DistributionType::Exponential),
        ("Beta", DistributionType::Beta),
        ("Gamma", DistributionType::Gamma),
        ("Chi-Squared", DistributionType::ChiSquared),
        ("Student's t", DistributionType::StudentsT),
        ("Poisson", DistributionType::Poisson),
        ("Bernoulli", DistributionType::Bernoulli),
        ("Binomial", DistributionType::Binomial),
        ("Geometric", DistributionType::Geometric),
        ("Weibull", DistributionType::Weibull),
    ];

    // Use stored p-values from initial analysis - no recalculation needed
    // These were calculated during infer_distribution() with the same data and method
    let mut distribution_scores: Vec<(usize, &str, DistributionType, f64)> = distributions
        .iter()
        .enumerate()
        .map(|(idx, (name, dist_type))| {
            // Use stored p-values from initial analysis - no recalculation needed
            let p_value = dist
                .all_distribution_pvalues
                .get(dist_type)
                .copied()
                .unwrap_or_else(|| {
                    // Fallback: if not in stored values (e.g., Geometric skipped), use placeholder
                    if *dist_type == DistributionType::Geometric {
                        0.01 // Placeholder to prevent freezes
                    } else {
                        0.0 // Default for untested distributions
                    }
                });
            (idx, *name, *dist_type, p_value)
        })
        .collect();

    // Sort by p-value (descending) - best fit on top
    distribution_scores.sort_by(|a, b| b.3.partial_cmp(&a.3).unwrap_or(std::cmp::Ordering::Equal));

    // Find position of selected distribution in sorted list
    let selected_pos = distribution_scores
        .iter()
        .position(|(_, _, dt, _)| *dt == selected_dist)
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
        .map(|(sorted_idx, (_, name, _dist_type, p_value))| {
            let is_focused = focus == AnalysisFocus::DistributionSelector
                && selector_state.selected() == Some(sorted_idx);

            let name_style = if is_focused {
                header_style(theme, "controls_bg", "table_header")
            } else {
                Style::default().fg(theme.get("text_primary"))
            };

            // Style based on p-value
            let pvalue_style = if *p_value > 0.05 {
                Style::default().fg(theme.get("distribution_normal")) // Good fit
            } else if *p_value > 0.01 {
                Style::default().fg(theme.get("distribution_skewed")) // Marginal fit
            } else {
                Style::default().fg(theme.get("outlier_marker")) // Poor fit
            };

            Row::new(vec![
                Cell::from(name.to_string()).style(name_style),
                Cell::from(format_pvalue(*p_value)).style(pvalue_style),
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
            .borders(Borders::ALL)
            .border_style(Style::default().fg(theme.get("sidebar_border"))),
    )
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    StatefulWidget::render(table, area, buf, selector_state);
}

struct HistogramRenderConfig<'a> {
    dist: &'a DistributionAnalysis,
    dist_type: DistributionType,
    area: Rect,
    shared_y_axis_label_width: u16,
    theme: &'a Theme,
    unified_x_range: Option<(f64, f64)>,
    histogram_scale: HistogramScale,
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
        .borders(Borders::ALL)
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

fn render_sidebar(
    area: Rect,
    buf: &mut Buffer,
    sidebar_state: &mut TableState,
    selected_tool: AnalysisTool,
    focus: AnalysisFocus,
    theme: &Theme,
) {
    let tools = [
        ("Describe", AnalysisTool::Describe),
        ("Distribution Analysis", AnalysisTool::DistributionAnalysis),
        ("Correlation Matrix", AnalysisTool::CorrelationMatrix),
    ];

    let text_primary = theme.get("text_primary");
    // Use REVERSED for focused row (like main table) so selection is always visible,
    // even when controls_bg is "default"/none.
    let focused_style = Style::default().add_modifier(Modifier::REVERSED);

    let items: Vec<ListItem> = tools
        .iter()
        .enumerate()
        .map(|(idx, (name, tool))| {
            let is_selected = *tool == selected_tool;
            let is_focused =
                focus == AnalysisFocus::Sidebar && sidebar_state.selected() == Some(idx);
            let prefix = if is_selected { "> " } else { "  " };
            let style = if is_focused {
                focused_style
            } else {
                Style::default().fg(text_primary)
            };
            ListItem::new(format!("{}{}", prefix, name)).style(style)
        })
        .collect();

    let block = Block::default()
        .title("Analysis Tools")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(theme.get("sidebar_border")));

    let list = List::new(items).block(block);

    Widget::render(list, area, buf);
}

fn render_distribution_histogram(config: HistogramRenderConfig, buf: &mut Buffer) {
    // Use BarChart widget to show histogram comparing data vs theoretical distribution
    // Use fixed-width bins that span both data range and theoretical distribution range
    let HistogramRenderConfig {
        dist,
        dist_type,
        area,
        shared_y_axis_label_width,
        theme,
        unified_x_range,
        histogram_scale,
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
    let available_width = area.width.saturating_sub(total_y_axis_space);
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

    // Calculate theoretical bin probabilities using CDF for the selected distribution
    let theory_probs = crate::statistics::calculate_theoretical_bin_probabilities(
        dist,
        dist_type,
        &bin_boundaries,
    );

    // Convert probabilities to expected counts
    let theory_bin_counts: Vec<f64> = theory_probs.iter().map(|&prob| prob * n as f64).collect();

    // Normalize values for display (find the maximum for scaling)
    let max_data = data_bin_counts.iter().cloned().fold(0, usize::max);
    let max_theory = theory_bin_counts.iter().cloned().fold(0.0, f64::max);
    let global_max = max_data.max(max_theory as usize).max(1) as f64;

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
        let data_bar = Bar::default()
            .value(data_height)
            // Remove text_value to prevent cyan count labels from appearing on bars
            // Remove .label() to prevent bar labels from overlapping Chart's x-axis labels
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
    // Create a sub-area for BarChart that matches Chart widget's inner plot area
    // This ensures bars align with the theoretical distribution overlay
    // Calculate area for bars: need to reserve space for Y-axis labels and x-axis labels
    // Chart widget automatically reserves space for both, so we need to match that
    // Fixed height for x-axis labels: 1 line (to match Chart widget)
    // Note: No borders now, so use area directly (no need for Block::bordered().inner())
    // Chart widget with Block title reserves 1 line at top for title
    // Block also has 1 line of top padding to separate title from chart content
    let title_height = 1u16;
    let top_padding = 1u16; // Extra padding below title (from Block padding)
    let x_axis_label_height = 1u16;
    let chart_inner_top = area.top() + title_height + top_padding; // Start below title and padding
    let chart_inner_height = area
        .height
        .saturating_sub(title_height)
        .saturating_sub(top_padding)
        .saturating_sub(x_axis_label_height); // Reserve space for title, padding, and x-axis labels

    // Shift bar plot area right by 1.5 bar widths so bars align to the right side of their bins
    // This ensures proper alignment with the theoretical distribution overlay
    // BarChart renders bars starting from the left edge, so shifting the area right will
    // make the bars' right edges align with the right edges of their bins
    let bar_width_offset = final_bar_width + (final_bar_width / 2); // 1.5 bar widths
    let bar_plot_left = area
        .left()
        .saturating_add(total_y_axis_space)
        .saturating_add(bar_width_offset); // Shift right by 1.5 bar widths for right alignment
    let bar_plot_width = available_width + bar_width_offset; // Extend width to accommodate shift

    let bar_plot_area = Rect::new(
        bar_plot_left,      // Shifted right for right-aligned bars
        chart_inner_top,    // Start below title
        bar_plot_width,     // Extended width to accommodate shift
        chart_inner_height, // Use calculated height that accounts for title
    );

    let barchart = BarChart::default()
        .block(Block::default()) // No borders in sub-area - borders handled separately
        .data(BarGroup::default().bars(&data_bars))
        .bar_width(final_bar_width)
        .bar_gap(bar_gap)
        .group_gap(group_gap);

    // Render bar chart to sub-area matching Chart's plot area (excluding x-axis label space)
    // Bars are now right-aligned within their bins
    barchart.render(bar_plot_area, buf);

    // No border - chart renders without surrounding box

    // Overlay theory distribution as dense scatter plot (dot plot) on top of bar chart
    // Evaluate theoretical PDF directly at each x point for accurate smooth curve
    // This ensures the theoretical distribution shows the correct shape (e.g., bell curve for normal)
    // Use very dense sampling for smooth continuous appearance
    // Braille markers create 2x4 dot patterns per character, need high density
    let num_samples = (available_width as usize * 15).clamp(1500, 10000); // Very dense for smooth Braille lines

    let theory_points: Vec<(f64, f64)> = if num_bins > 0
        && !theory_bin_counts.is_empty()
        && num_samples > 1
        && hist_range > 0.0
        && dist.characteristics.std_dev > 0.0
    {
        // Evaluate theoretical PDF directly at each x point for accurate smooth curve
        // Get distribution parameters
        let mean = dist.characteristics.mean;
        let std = dist.characteristics.std_dev;

        // Evaluate theoretical PDF directly at each x point for accurate smooth curve
        // Sample across the full range, but use a small epsilon to avoid exact boundary conditions
        // that can cause issues with domain-restricted distributions (e.g., Gamma at x=0, Beta at x=0 or x=1)
        // The epsilon is very small (0.1% of range) so the curve still extends nearly to the edges
        let epsilon = hist_range * 0.001; // 0.1% of range - small enough to be visually negligible
        let effective_min = hist_min + epsilon;
        let effective_max = hist_max - epsilon;
        let effective_range = effective_max - effective_min;

        (0..num_samples)
            .map(|i| {
                // Sample x values across the histogram range, avoiding exact boundaries
                let x = if num_samples > 1 && effective_range > 0.0 {
                    effective_min + (i as f64 / (num_samples - 1) as f64) * effective_range
                } else if num_samples > 1 {
                    // Fallback if range is too small
                    hist_min + (i as f64 / (num_samples - 1) as f64) * hist_range
                } else {
                    (hist_min + hist_max) / 2.0
                };

                // Calculate theoretical PDF at x value, then convert to expected count
                // PDF gives us density (probability per unit), convert to count: PDF(x) * bin_width * n
                let theory_count = match dist_type {
                    DistributionType::Normal => {
                        // Normal PDF: (1 / (σ * sqrt(2π))) * exp(-0.5 * ((x - μ) / σ)²)
                        let z = (x - mean) / std;
                        let pdf = (1.0 / (std * (2.0 * std::f64::consts::PI).sqrt()))
                            * (-0.5 * z * z).exp();
                        pdf * bin_width * n as f64
                    }
                    DistributionType::LogNormal => {
                        // LogNormal PDF: show theoretical distribution over [0, ∞) even if data is negative
                        if x > 0.0 {
                            let (mu, sigma) = if mean > 0.0 && std >= 0.0 {
                                let variance = std * std;
                                let sigma_sq = (1.0 + variance / (mean * mean)).ln();
                                let mu_val = mean.ln() - sigma_sq / 2.0;
                                let sigma_val = sigma_sq.sqrt();
                                (mu_val, sigma_val)
                            } else {
                                // Data doesn't match LogNormal: use default parameters (mu=0, sigma=1)
                                (0.0, 1.0)
                            };
                            let z = (x.ln() - mu) / sigma;
                            let pdf = (1.0 / (x * sigma * (2.0 * std::f64::consts::PI).sqrt()))
                                * (-0.5 * z * z).exp();
                            pdf * bin_width * n as f64
                        } else {
                            // LogNormal is strictly positive, return 0 for x <= 0
                            0.0
                        }
                    }
                    DistributionType::Exponential => {
                        // Exponential PDF: show theoretical distribution over [0, ∞) even if data is negative
                        if x >= 0.0 {
                            let lambda = if mean > 0.0 {
                                1.0 / mean
                            } else {
                                // Data doesn't match Exponential: use default lambda=1
                                1.0
                            };
                            let pdf = lambda * (-lambda * x).exp();
                            pdf * bin_width * n as f64
                        } else {
                            // Exponential is strictly non-negative, return 0 for x < 0
                            0.0
                        }
                    }
                    DistributionType::Uniform => {
                        if !sorted_data.is_empty() && x >= data_min && x <= data_max {
                            let data_range = data_max - data_min;
                            if data_range > 0.0 {
                                let pdf = 1.0 / data_range;
                                pdf * bin_width * n as f64
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    }
                    DistributionType::Gamma => {
                        // Gamma PDF: evaluate directly for smooth curve
                        // Show theoretical distribution over its valid domain [0, ∞) even if data is negative
                        if x > 0.0 {
                            let variance = std * std;
                            let (shape, scale) = if mean > 0.0 && variance > 0.0 {
                                let s = (mean * mean) / variance;
                                let sc = variance / mean;
                                if s > 0.0 && sc > 0.0 {
                                    (s, sc)
                                } else {
                                    // Invalid parameters: use default (exponential with scale=1)
                                    (1.0, 1.0)
                                }
                            } else {
                                // Data doesn't match Gamma (e.g., negative mean): use default parameters
                                // This ensures we still show the theoretical distribution shape
                                (1.0, 1.0)
                            };
                            let pdf = gamma_pdf(x, shape, scale);
                            pdf * bin_width * n as f64
                        } else {
                            // Gamma is strictly non-negative, return 0 for x <= 0
                            0.0
                        }
                    }
                    DistributionType::Geometric => {
                        // Geometric PMF: evaluate directly for smooth curve
                        if x >= 0.0 && mean > 0.0 {
                            let p_param = 1.0 / (mean + 1.0);
                            if p_param > 0.0 && p_param < 1.0 {
                                // Use PMF for continuous approximation
                                let pmf = geometric_pmf(x, p_param);
                                // Convert PMF to expected count: PMF * n
                                // Note: For discrete distributions, we use PMF directly rather than PDF * bin_width
                                pmf * n as f64
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    }
                    DistributionType::Weibull => {
                        // Weibull PDF: evaluate directly for smooth curve
                        if x > 0.0 && mean > 0.0 && std > 0.0 {
                            // Approximate shape from CV
                            let cv = std / mean;
                            let shape = if cv < 1.0 { 1.0 / cv } else { 1.0 };
                            // Scale from mean
                            let gamma_1_over_shape = 1.0 + 1.0 / shape; // Approximation
                            let scale = mean / gamma_1_over_shape;
                            if shape > 0.0 && scale > 0.0 {
                                let pdf = weibull_pdf(x, shape, scale);
                                pdf * bin_width * n as f64
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    }
                    DistributionType::Beta => {
                        // Beta PDF: evaluate directly for smooth curve
                        if x > 0.0 && x < 1.0 {
                            let variance = std * std;
                            let mean_val = mean;
                            if mean_val > 0.0 && mean_val < 1.0 && variance > 0.0 {
                                let max_var = mean_val * (1.0 - mean_val);
                                if variance < max_var {
                                    // Estimate alpha and beta using method of moments
                                    let sum = mean_val * (1.0 - mean_val) / variance - 1.0;
                                    let alpha = mean_val * sum;
                                    let beta = (1.0 - mean_val) * sum;
                                    if alpha > 0.0 && beta > 0.0 {
                                        let pdf = beta_pdf(x, alpha, beta);
                                        pdf * bin_width * n as f64
                                    } else {
                                        0.0
                                    }
                                } else {
                                    0.0
                                }
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    }
                    DistributionType::ChiSquared => {
                        // ChiSquared PDF: evaluate directly for smooth curve (uses gamma_pdf)
                        if x > 0.0 {
                            let df = mean; // For chi-squared, mean = df
                            if df > 0.0 {
                                let pdf = chi_squared_pdf(x, df);
                                pdf * bin_width * n as f64
                            } else {
                                0.0
                            }
                        } else {
                            0.0
                        }
                    }
                    DistributionType::StudentsT => {
                        // StudentsT PDF: evaluate directly for smooth curve
                        let variance = std * std;
                        let df = if variance > 1.0 {
                            2.0 * variance / (variance - 1.0)
                        } else {
                            30.0
                        };
                        if df > 0.0 {
                            // StudentsT is centered at mean, but PDF is typically for standard t (mean=0, std=1)
                            // Adjust x to account for data mean and scale
                            let x_standardized = if std > 0.0 { (x - mean) / std } else { 0.0 };
                            let pdf_standard = students_t_pdf(x_standardized, df);
                            // Convert back to data scale: PDF_standard / std
                            let pdf = if std > 0.0 { pdf_standard / std } else { 0.0 };
                            pdf * bin_width * n as f64
                        } else {
                            0.0
                        }
                    }
                    DistributionType::PowerLaw => {
                        // PowerLaw: use bin-based values from CDF calculations
                        // Power law PDF is complex and depends on x_min parameter
                        // For log-scale binning, find which bin x belongs to using binary search
                        if use_log_scale && x > 0.0 {
                            // Binary search to find the correct bin for log-scale boundaries
                            let mut left = 0;
                            let mut right = num_bins;
                            while left < right {
                                let mid = (left + right) / 2;
                                if x < bin_boundaries[mid] {
                                    right = mid;
                                } else {
                                    left = mid + 1;
                                }
                            }
                            let bin_idx = if left > 0 { left - 1 } else { 0 };
                            if bin_idx < num_bins {
                                theory_bin_counts[bin_idx]
                            } else {
                                theory_bin_counts[num_bins - 1]
                            }
                        } else {
                            // Linear binning fallback
                            let bin_idx = ((x - hist_min) / bin_width).floor() as usize;
                            if bin_idx < num_bins {
                                theory_bin_counts[bin_idx]
                            } else if bin_idx == num_bins {
                                theory_bin_counts[num_bins - 1]
                            } else {
                                0.0
                            }
                        }
                    }
                    // REMOVED: All individual PDF implementations below caused issues with plateaus
                    // Keeping only the bin-based approach above which uses CDF-calculated values
                    _ => {
                        // Fallback: Use bin-based approach for distributions without PDF implementation
                        let bin_idx = ((x - hist_min) / bin_width).floor() as usize;
                        let bin_idx = bin_idx.min(num_bins - 1);
                        if bin_idx < theory_bin_counts.len() {
                            theory_bin_counts[bin_idx]
                        } else {
                            0.0
                        }
                    }
                };
                let normalized_height = if global_max > 0.0 {
                    (theory_count / global_max) * 100.0
                } else {
                    0.0
                };
                (x, normalized_height)
            })
            .collect()
    } else {
        // Fallback: use bin centers with theory_bin_counts if PDF evaluation fails
        let theory_normalized_heights: Vec<f64> = theory_bin_counts
            .iter()
            .map(|&theory_count| {
                if global_max > 0.0 {
                    (theory_count / global_max) * 100.0
                } else {
                    0.0
                }
            })
            .collect();
        bin_centers
            .iter()
            .zip(theory_normalized_heights.iter())
            .map(|(&bin_center, &normalized_height)| (bin_center, normalized_height))
            .collect()
    };

    // Create scatter plot dataset for theoretical distribution
    // Use Braille marker for dense, continuous appearance
    let marker = symbols::Marker::Braille;

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
                .title("Histogram")
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
    theory_chart.render(area, buf);
}

// REMOVED ALL DUPLICATE PDF CODE - it was causing plateaus and jumps
// The bin-based approach using CDF-calculated theory_bin_counts works better

fn render_qq_plot(
    dist: &DistributionAnalysis,
    dist_type: DistributionType,
    area: Rect,
    buf: &mut Buffer,
    shared_y_axis_label_width: u16,
    theme: &Theme,
    unified_x_range: Option<(f64, f64)>,
) {
    // Use Chart widget for Q-Q plot: Data quantiles vs Theoretical quantiles
    // Use sorted_sample_values and position-based quantiles (not just 5 percentiles)
    let sorted_data = &dist.sorted_sample_values;

    if sorted_data.is_empty() || sorted_data.len() < 3 {
        Paragraph::new("Insufficient data for Q-Q plot (need at least 3 points)")
            .centered()
            .render(area, buf);
        return;
    }

    let n = sorted_data.len();

    // Calculate Q-Q plot data points using position-based quantiles
    // For each position i, probability p = (i+1)/(n+1), theoretical quantile at p, data quantile = sorted_data[i]
    let qq_data: Vec<(f64, f64)> = sorted_data
        .iter()
        .enumerate()
        .map(|(i, &data_value)| {
            let position = i + 1; // 1-based position
            let probability = (position as f64) / (n as f64 + 1.0);
            let theoretical_quantile =
                calculate_theoretical_quantile_at_probability(dist, dist_type, probability);
            (theoretical_quantile, data_value)
        })
        .collect();

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
        symbols::Marker::Braille // Better for dense scatter plots
    } else {
        symbols::Marker::Dot
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
                .title("Q-Q Plot")
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
}

fn render_condensed_statistics(
    dist: &DistributionAnalysis,
    _selected_dist_type: DistributionType,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    // Display statistics in single line: SW score, skew, kurtosis, median, mean, std, CV
    // Use explicit theme colors so text is always visible (avoids black-on-black for some themes)
    let chars = &dist.characteristics;
    let label_style = Style::default().fg(theme.get("text_primary"));
    let value_style = Style::default().fg(theme.get("text_primary"));

    let mut line_parts = Vec::new();

    if let (Some(sw_stat), Some(sw_p)) = (chars.shapiro_wilk_stat, chars.shapiro_wilk_pvalue) {
        line_parts.push(Span::styled("SW: ", label_style));
        line_parts.push(Span::styled(
            format!("{:.3} (p={:.3})", sw_stat, sw_p),
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

// Calculate theoretical quantile at any probability (for Q-Q plots)
pub fn calculate_theoretical_quantile_at_probability(
    dist: &DistributionAnalysis,
    dist_type: DistributionType,
    probability: f64,
) -> f64 {
    let chars = &dist.characteristics;
    let p = probability.clamp(0.0, 1.0); // Clamp to [0, 1]

    match dist_type {
        DistributionType::Normal => {
            let z = approximate_normal_quantile(p);
            chars.mean + chars.std_dev * z
        }
        DistributionType::LogNormal => {
            let z = approximate_normal_quantile(p);
            // Convert from mean (m) and std dev (s) on original scale to lognormal parameters (μ, σ)
            // Where X ~ Lognormal(μ, σ²) means ln(X) ~ Normal(μ, σ)
            // Formulas: σ = sqrt(ln(1 + s²/m²)), μ = ln(m) - σ²/2
            // Quantile: q(p) = exp(μ + σ*z)
            // Even if data doesn't match (e.g., negative values), still calculate quantiles over [0, ∞)
            let m = chars.mean;
            let s = chars.std_dev;
            if m > 0.0 && s >= 0.0 {
                let variance = s * s;
                let sigma = (1.0 + variance / (m * m)).ln().sqrt();
                let mu = m.ln() - (sigma * sigma) / 2.0;
                (mu + sigma * z).exp()
            } else {
                // Data doesn't match LogNormal (e.g., negative mean): use default parameters
                // Default: mu=0, sigma=1 gives mean≈1.65, which provides a reasonable range
                (z).exp()
            }
        }
        DistributionType::Uniform => {
            // Estimate min/max from mean and std: for uniform, std = (max-min) / sqrt(12)
            let range = chars.std_dev * (12.0_f64).sqrt();
            let min_est = chars.mean - range / 2.0;
            let max_est = chars.mean + range / 2.0;
            min_est + (max_est - min_est) * p
        }
        DistributionType::Exponential => {
            // Exponential quantile: q(p) = -ln(1-p) / lambda, where lambda = 1/mean
            // Even if data doesn't match (e.g., negative values), still calculate quantiles over [0, ∞)
            if chars.mean > 0.0 {
                -chars.mean * (1.0 - p).ln()
            } else {
                // Data doesn't match Exponential (e.g., negative mean): use default lambda=1
                // This ensures we still get a range of quantiles
                -(1.0 - p).ln()
            }
        }
        DistributionType::Beta => {
            // Beta quantile: use approximation
            // Estimate parameters from mean and variance
            let mean = chars.mean;
            let variance = chars.std_dev * chars.std_dev;
            if mean > 0.0 && mean < 1.0 && variance > 0.0 {
                let max_var = mean * (1.0 - mean);
                if variance < max_var {
                    // Estimate alpha and beta using method of moments
                    let sum = mean * (1.0 - mean) / variance - 1.0;
                    let alpha = mean * sum;
                    let beta = (1.0 - mean) * sum;
                    if alpha > 0.0 && beta > 0.0 && alpha + beta > 50.0 {
                        // Normal approximation
                        let normal_mean = alpha / (alpha + beta);
                        let normal_std = ((alpha * beta)
                            / ((alpha + beta).powi(2) * (alpha + beta + 1.0)))
                            .sqrt();
                        let z = approximate_normal_quantile(p);
                        normal_mean + normal_std * z
                    } else {
                        // Use simple linear interpolation across [0, 1] range
                        // Clamp to [0, 1] for beta distribution
                        p.clamp(0.0, 1.0)
                    }
                } else {
                    // Use linear interpolation across [0, 1] range
                    p.clamp(0.0, 1.0)
                }
            } else {
                // Fallback: use empirical percentile interpolation
                interpolate_empirical_quantile(dist, p)
            }
        }
        DistributionType::Gamma => {
            // Gamma quantile: estimate parameters and use proper quantile function
            // Even if data doesn't match (e.g., negative values), still calculate quantiles
            // over the distribution's natural domain [0, ∞)
            let mean = chars.mean;
            let variance = chars.std_dev * chars.std_dev;
            if mean > 0.0 && variance > 0.0 {
                let shape = (mean * mean) / variance;
                let scale = variance / mean;
                // Check for edge cases: very small shape or very large scale can cause numerical issues
                // Also check if parameters are reasonable (shape >= 0.01, scale < 1e6)
                if shape > 0.01
                    && scale > 0.0
                    && scale < 1e6
                    && shape.is_finite()
                    && scale.is_finite()
                {
                    gamma_quantile(p, shape, scale)
                } else {
                    // Invalid or extreme parameters: use default Gamma distribution to still show a range
                    // Use shape=1 (exponential) with reasonable scale
                    let default_scale = if mean > 0.0 && mean < 1e6 {
                        mean.max(0.1) // Ensure scale is reasonable
                    } else {
                        1.0
                    };
                    gamma_quantile(p, 1.0, default_scale)
                }
            } else {
                // Data doesn't match Gamma (e.g., negative mean): use default parameters
                // This ensures we still get a range of quantiles over [0, ∞)
                let default_scale = 1.0;
                gamma_quantile(p, 1.0, default_scale)
            }
        }
        DistributionType::ChiSquared => {
            // Chi-squared quantile: special case of gamma with shape = df/2, scale = 2
            // Estimate df from mean (mean = df for chi-squared)
            // Even if data doesn't match (e.g., negative values), still calculate quantiles over [0, ∞)
            let df = chars.mean;
            if df > 0.0 {
                if df > 30.0 {
                    // Normal approximation
                    let normal_mean = df;
                    let normal_std = (2.0 * df).sqrt();
                    let z = approximate_normal_quantile(p);
                    (normal_mean + normal_std * z).max(0.0)
                } else {
                    // Use gamma quantile with shape = df/2, scale = 2
                    gamma_quantile(p, df / 2.0, 2.0)
                }
            } else {
                // Data doesn't match ChiSquared (e.g., negative mean): use default df=1
                gamma_quantile(p, 0.5, 2.0)
            }
        }
        DistributionType::StudentsT => {
            // Student's t quantile: for large df, approximate with normal
            // Estimate df from variance (variance = df/(df-2) for t-distribution)
            let variance = chars.std_dev * chars.std_dev;
            let df = if variance > 1.0 {
                2.0 * variance / (variance - 1.0)
            } else {
                30.0
            };
            if df > 30.0 {
                // Normal approximation
                let z = approximate_normal_quantile(p);
                chars.mean + chars.std_dev * z
            } else {
                // For small df, use normal approximation anyway (better than constant)
                let z = approximate_normal_quantile(p);
                chars.mean + chars.std_dev * z
            }
        }
        DistributionType::Poisson => {
            // Poisson quantile: use normal approximation for large lambda
            // Even if data doesn't match (e.g., negative values), still calculate quantiles over [0, ∞)
            let lambda = chars.mean;
            if lambda > 0.0 {
                if lambda > 20.0 {
                    // Normal approximation for large lambda
                    let z = approximate_normal_quantile(p);
                    (lambda + z * lambda.sqrt()).max(0.0)
                } else {
                    // For small lambda, use normal approximation anyway to get a range
                    // This ensures we still get quantiles even when lambda is small
                    let z = approximate_normal_quantile(p);
                    (lambda + z * lambda.sqrt()).max(0.0)
                }
            } else {
                // Data doesn't match Poisson (e.g., negative mean): use default lambda=10
                // This ensures we still get a range of quantiles
                let default_lambda: f64 = 10.0;
                let z = approximate_normal_quantile(p);
                (default_lambda + z * default_lambda.sqrt()).max(0.0)
            }
        }
        DistributionType::Bernoulli => {
            // Bernoulli quantile: simple binary
            // For Bernoulli, quantile function is: 0 if p < (1-p_param), 1 otherwise
            // But to get a range for Q-Q plot, use a continuous approximation
            // We'll use linear interpolation between 0 and 1 based on probability
            let mean = chars.mean; // mean = p_param for Bernoulli
            if mean <= 0.0 {
                // Degenerate case: all 0s
                interpolate_empirical_quantile(dist, p)
            } else if mean >= 1.0 {
                // Degenerate case: all 1s
                interpolate_empirical_quantile(dist, p)
            } else {
                // For Q-Q plot, use a continuous approximation
                // Map probability to [0, 1] range linearly
                // This gives us a range even though Bernoulli is discrete
                let threshold = 1.0 - mean;
                if p < threshold {
                    0.0
                } else if p > mean {
                    1.0
                } else {
                    // Interpolate in the middle range for smoother Q-Q plot
                    (p - threshold) / (mean - threshold) * (1.0 - 0.0)
                }
            }
        }
        DistributionType::Binomial => {
            // Binomial quantile: use normal approximation
            // Even if data doesn't match, still calculate quantiles to show a range
            let mean = chars.mean;
            let variance = chars.std_dev * chars.std_dev;
            if variance > 0.0 {
                let z = approximate_normal_quantile(p);
                (mean + z * variance.sqrt()).max(0.0)
            } else {
                // No variance: use default parameters to still show a range
                // Estimate n from mean (assuming p=0.5 for default)
                let default_n = (mean * 2.0).max(10.0);
                let default_p = 0.5;
                let default_mean = default_n * default_p;
                let default_variance = default_n * default_p * (1.0 - default_p);
                let z = approximate_normal_quantile(p);
                (default_mean + z * default_variance.sqrt()).max(0.0)
            }
        }
        DistributionType::Geometric => {
            // Geometric quantile: use proper quantile function
            let mean = chars.mean; // mean = (1-p)/p for geometric
            if mean > 0.0 {
                let p_param = 1.0 / (mean + 1.0);
                if p_param > 0.0 && p_param < 1.0 {
                    geometric_quantile(p, p_param)
                } else {
                    // Fallback: use empirical percentile interpolation
                    interpolate_empirical_quantile(dist, p)
                }
            } else {
                // Fallback: use empirical percentile interpolation
                interpolate_empirical_quantile(dist, p)
            }
        }
        DistributionType::Weibull => {
            // Weibull quantile: q(p) = scale * (-ln(1-p))^(1/shape)
            // Estimate parameters from data characteristics
            // Even if data doesn't match (e.g., negative values), still calculate quantiles over [0, ∞)
            let sorted_data = &dist.sorted_sample_values;
            let mean = chars.mean;
            let variance = chars.std_dev * chars.std_dev;

            let (shape_est, scale_est) = if !sorted_data.is_empty()
                && sorted_data[0] > 0.0
                && mean > 0.0
                && variance > 0.0
            {
                // Estimate shape and scale from data
                // Approximate shape from CV
                let cv = chars.std_dev / mean;
                let shape = if cv < 1.0 {
                    // Approximation for shape parameter
                    1.0 / cv
                } else {
                    1.0
                };
                // Scale from mean
                let gamma_1_over_shape = 1.0 + 1.0 / shape; // Approximation
                let scale = mean / gamma_1_over_shape;
                if scale > 0.0 && shape > 0.0 {
                    (shape, scale)
                } else {
                    // Invalid parameters: use defaults
                    (1.0, 1.0)
                }
            } else {
                // Data doesn't match Weibull (e.g., negative values or invalid parameters): use defaults
                // Default: shape=1 (exponential), scale=1
                (1.0, 1.0)
            };

            scale_est * (-(1.0 - p).ln()).powf(1.0 / shape_est)
        }
        DistributionType::PowerLaw | DistributionType::Unknown => {
            // Fallback: use empirical quantiles from percentiles
            interpolate_empirical_quantile(dist, p)
        }
    }
}

// Helper function to interpolate empirical quantiles from known percentiles
fn interpolate_empirical_quantile(dist: &DistributionAnalysis, p: f64) -> f64 {
    // Interpolate between known percentiles
    if p <= 0.05 {
        dist.percentiles.p5
    } else if p <= 0.25 {
        dist.percentiles.p5 + (dist.percentiles.p25 - dist.percentiles.p5) * ((p - 0.05) / 0.20)
    } else if p <= 0.50 {
        dist.percentiles.p25 + (dist.percentiles.p50 - dist.percentiles.p25) * ((p - 0.25) / 0.25)
    } else if p <= 0.75 {
        dist.percentiles.p50 + (dist.percentiles.p75 - dist.percentiles.p50) * ((p - 0.50) / 0.25)
    } else if p <= 0.95 {
        dist.percentiles.p75 + (dist.percentiles.p95 - dist.percentiles.p75) * ((p - 0.75) / 0.20)
    } else {
        dist.percentiles.p95
    }
}

fn approximate_normal_quantile(p: f64) -> f64 {
    // Approximation of inverse CDF for standard normal distribution
    // Beasley-Springer-Moro algorithm (simplified)
    if p < 0.5 {
        -approximate_normal_quantile(1.0 - p)
    } else {
        let t = ((p - 0.5).ln() * -2.0).sqrt();
        t - (2.515517 + 0.802853 * t + 0.010328 * t * t)
            / (1.0 + 1.432788 * t + 0.189269 * t * t + 0.001308 * t * t * t)
    }
}
