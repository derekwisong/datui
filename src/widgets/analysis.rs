use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style, Stylize},
    symbols,
    text::{Line, Span},
    widgets::{
        Axis, Bar, BarChart, BarGroup, Block, Borders, Cell, Chart, Dataset, GraphType, List,
        ListItem, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::analysis_modal::{AnalysisFocus, AnalysisTool, AnalysisView};
use crate::statistics::{AnalysisContext, AnalysisResults, DistributionAnalysis, DistributionType};
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
}

pub struct AnalysisWidget<'a> {
    _state: &'a DataTableState,
    results: Option<&'a AnalysisResults>,
    context: &'a AnalysisContext,
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
            context: config.context,
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
        }
    }

    fn generate_breadcrumb(&self) -> String {
        let mut parts = Vec::new();

        if self.context.has_query {
            parts.push(format!("Query: {}", self.context.query));
        }

        if self.context.has_filters {
            parts.push(format!("{} filter(s)", self.context.filter_count));
        }

        if self.context.is_drilled_down {
            if let (Some(keys), Some(cols)) = (&self.context.group_key, &self.context.group_columns)
            {
                let group_desc = keys
                    .iter()
                    .zip(cols.iter())
                    .map(|(k, c)| format!("{}={}", c, k))
                    .collect::<Vec<_>>()
                    .join(", ");
                parts.push(format!("Group: {}", group_desc));
            }
        }

        if parts.is_empty() {
            "Full Dataset".to_string()
        } else {
            parts.join(" + ")
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

        // Full-screen layout: breadcrumb, main area, keybind hints
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Breadcrumb
                Constraint::Fill(1),   // Main area + sidebar
                Constraint::Length(1), // Keybind hints
            ])
            .split(area);

        // Breadcrumb with background to visually separate it
        let breadcrumb = self.generate_breadcrumb();
        Paragraph::new(breadcrumb.as_str())
            .style(Style::default().fg(Color::Cyan).bg(Color::DarkGray))
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
                    );
                }
                AnalysisTool::DistributionAnalysis => {
                    render_distribution_table(
                        results,
                        self.distribution_table_state,
                        main_layout[0],
                        buf,
                    );
                }
                AnalysisTool::CorrelationMatrix => {
                    render_correlation_matrix(
                        results,
                        self.correlation_table_state,
                        &self.selected_correlation,
                        main_layout[0],
                        buf,
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
        );

        // Keybind hints (cyan labels, white descriptions)
        use ratatui::text::{Line, Span};
        let mut hint_line = Line::default();
        hint_line.spans.push(Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Back "));
        hint_line.spans.push(Span::styled(
            "↑↓",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Navigate "));
        hint_line.spans.push(Span::styled(
            "←→",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Scroll Columns "));
        hint_line.spans.push(Span::styled(
            "Tab",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Sidebar "));
        hint_line.spans.push(Span::styled(
            "Enter",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Select "));
        hint_line.spans.push(Span::styled(
            "Ctrl+h",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Help "));
        hint_line.spans.push(Span::styled(
            "r",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Resample"));

        Paragraph::new(vec![hint_line]).render(layout[2], buf);
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

        // Layout: breadcrumb, main content, keybind hints
        let layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(1), // Breadcrumb
                Constraint::Fill(1),   // Main content
                Constraint::Length(1), // Keybind hints
            ])
            .split(area);

        // Breadcrumb with Escape hint
        let breadcrumb = format!("Distribution Analysis: {}  [Esc] Back", dist.column_name);
        Paragraph::new(breadcrumb.as_str())
            .style(Style::default().fg(Color::Cyan).bg(Color::DarkGray))
            .render(layout[0], buf);

        // Main content area - optimized layout
        // Split into: condensed stats header, charts and selector area
        let main_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(2), // Condensed stats header
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
        );

        // Split charts and selector horizontally
        let content_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(75), // Q-Q plot and histogram
                Constraint::Percentage(25), // Distribution selector
            ])
            .split(main_layout[1]);

        // Left side: Q-Q plot and histogram
        let charts_layout = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(55), // Q-Q plot
                Constraint::Percentage(45), // Histogram
            ])
            .split(content_layout[0]);

        // Q-Q plot approximation (larger, better aspect ratio)
        // Use selected theoretical distribution from selector
        render_qq_plot(
            dist,
            self.selected_theoretical_distribution,
            charts_layout[0],
            buf,
        );

        // Histogram comparison (vertical bars)
        // Use selected theoretical distribution from selector
        render_distribution_histogram(
            dist,
            self.selected_theoretical_distribution,
            charts_layout[1],
            buf,
        );

        // Right side: Distribution selector
        render_distribution_selector(
            self.selected_theoretical_distribution,
            self.distribution_selector_state,
            self.focus,
            content_layout[1],
            buf,
        );

        // Keybind hints
        use ratatui::text::{Line, Span};
        let mut hint_line = Line::default();
        hint_line.spans.push(Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line
            .spans
            .push(Span::raw(" Back to Distribution Analysis "));
        hint_line.spans.push(Span::styled(
            "Ctrl+h",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        hint_line.spans.push(Span::raw(" Help"));

        Paragraph::new(vec![hint_line]).render(layout[2], buf);
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
) {
    let num_columns = results.column_statistics.len();
    if num_columns == 0 {
        Paragraph::new("No columns to display")
            .centered()
            .render(area, buf);
        return;
    }

    // Statistics to display (in order)
    // Distribution is the 3rd column, then outliers and skewness after it, then mean, etc.
    let stat_names = vec![
        "count",
        "null_count",
        "distribution",
        "outliers",
        "skewness",
        "kurtosis",
        "mean",
        "std",
        "min",
        "25%",
        "50%",
        "75%",
        "max",
    ];
    let num_stats = stat_names.len();

    // Calculate column widths based on header names and content (minimal spacing)
    // First, determine minimum width for each column based on header length
    // Note: ratatui Table adds 1 space between columns by default, so we don't add extra padding
    let mut min_col_widths: Vec<u16> = stat_names
        .iter()
        .map(|name| name.chars().count() as u16) // header length (no extra padding - table handles spacing)
        .collect();

    // Scan all data to find maximum width needed for each column
    for col_stat in &results.column_statistics {
        for (stat_idx, stat_name) in stat_names.iter().enumerate() {
            let value_str = match *stat_name {
                "count" => col_stat.count.to_string(),
                "null_count" => col_stat.null_count.to_string(),
                "distribution" => {
                    if let Some(ref dist_info) = col_stat.distribution_info {
                        format!("{}", dist_info.distribution_type)
                    } else {
                        "-".to_string()
                    }
                }
                "outliers" => {
                    if let Some(ref num_stats) = col_stat.numeric_stats {
                        let outlier_count = num_stats.outliers_iqr.max(num_stats.outliers_zscore);
                        let outlier_pct = if col_stat.count > 0 {
                            (outlier_count as f64 / col_stat.count as f64) * 100.0
                        } else {
                            0.0
                        };
                        format!("{:.1}%", outlier_pct)
                    } else {
                        "-".to_string()
                    }
                }
                "skewness" => {
                    if let Some(ref num_stats) = col_stat.numeric_stats {
                        format_num(num_stats.skewness)
                    } else {
                        "-".to_string()
                    }
                }
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
                "kurtosis" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.kurtosis))
                    .unwrap_or_else(|| "-".to_string()),
                _ => "-".to_string(),
            };
            let value_len = value_str.chars().count() as u16;
            // Ensure width is at least the header length (already initialized) AND value length
            // This preserves header widths even if all data values are shorter
            let header_len = stat_names[stat_idx].chars().count() as u16;
            min_col_widths[stat_idx] = min_col_widths[stat_idx].max(value_len).max(header_len);
            // must fit both header and content (no padding - table handles spacing)
        }
    }

    // Locked column width (column name) - calculate from header text AND actual column names
    let header_text = "column";
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

    // Determine which statistics to show (column_offset now refers to statistics, not data columns)
    let start_stat = column_offset.min(num_stats.saturating_sub(1));

    // Calculate how many stat columns can fit starting from start_stat
    let mut used_width = 0u16;
    let mut max_visible_stats = 0;

    // Calculate max visible stats starting from start_stat
    for width in min_col_widths
        .iter()
        .skip(start_stat)
        .take(num_stats - start_stat)
    {
        // Add spacing before this column (except the first one after start_stat)
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

    // Build header row: "column" (locked) + visible statistic names
    let mut header_cells =
        vec![Cell::from("column").style(Style::default().add_modifier(Modifier::BOLD))];
    for &stat_idx in &visible_stats {
        header_cells.push(
            Cell::from(stat_names[stat_idx]).style(Style::default().add_modifier(Modifier::BOLD)),
        );
    }
    let header_row =
        Row::new(header_cells.clone()).style(Style::default().add_modifier(Modifier::BOLD));
    // Don't add header to rows - it will be set via .header() method only

    // Build data rows: one row per data column
    // Note: rows vector does NOT include the header - header is set separately via .header()
    for col_stat in &results.column_statistics {
        let mut cells = vec![Cell::from(col_stat.name.as_str())];

        // Calculate outlier percentage for this column
        let outlier_percentage = if let Some(ref num_stats) = col_stat.numeric_stats {
            let outlier_count = num_stats.outliers_iqr.max(num_stats.outliers_zscore);
            if col_stat.count > 0 {
                (outlier_count as f64 / col_stat.count as f64) * 100.0
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Get skewness value for styling
        let skewness_value = col_stat
            .numeric_stats
            .as_ref()
            .map(|n| n.skewness.abs())
            .unwrap_or(0.0);

        // Add statistic values for visible statistics
        for &stat_idx in &visible_stats {
            let stat_name = stat_names[stat_idx];
            let value = match stat_name {
                "count" => col_stat.count.to_string(),
                "null_count" => col_stat.null_count.to_string(),
                "distribution" => {
                    if let Some(ref dist_info) = col_stat.distribution_info {
                        format!("{}", dist_info.distribution_type)
                    } else {
                        "-".to_string()
                    }
                }
                "outliers" => {
                    if col_stat.numeric_stats.is_some() {
                        format!("{:.1}%", outlier_percentage)
                    } else {
                        "-".to_string()
                    }
                }
                "skewness" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.skewness))
                    .unwrap_or_else(|| "-".to_string()),
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
                "kurtosis" => col_stat
                    .numeric_stats
                    .as_ref()
                    .map(|n| format_num(n.kurtosis))
                    .unwrap_or_else(|| "-".to_string()),
                _ => "-".to_string(),
            };

            // Apply color coding for specific columns (distribution, outliers, skewness)
            let cell_style = if stat_name == "distribution" {
                if let Some(ref dist_info) = col_stat.distribution_info {
                    match dist_info.distribution_type {
                        DistributionType::Normal | DistributionType::LogNormal => {
                            Style::default().fg(Color::Green)
                        }
                        DistributionType::Uniform | DistributionType::PowerLaw => {
                            Style::default().fg(Color::Cyan)
                        }
                        DistributionType::Exponential => Style::default().fg(Color::Yellow),
                        DistributionType::Unknown => Style::default().fg(Color::Yellow),
                    }
                } else {
                    Style::default()
                }
            } else if stat_name == "outliers" {
                // Outlier color gradient: white (0-5%), yellow (5-20%), red (>20%)
                if outlier_percentage > 20.0 {
                    Style::default().fg(Color::Red)
                } else if outlier_percentage > 5.0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default() // Default (white) for low percentages
                }
            } else if stat_name == "skewness" {
                // Skewness color gradient based on absolute value
                // |skew| < 1: default (near symmetric)
                // 1 <= |skew| < 2: yellow (moderate skew)
                // 2 <= |skew| < 3: yellow (high skew)
                // |skew| >= 3: red (very high skew - might indicate data errors)
                if skewness_value >= 3.0 {
                    Style::default().fg(Color::Red)
                } else if skewness_value >= 1.0 {
                    Style::default().fg(Color::Yellow)
                } else {
                    Style::default() // Default (white) for low skewness
                }
            } else {
                Style::default()
            };

            cells.push(Cell::from(value).style(cell_style));
        }

        // No row styling - colors are on individual cells only
        rows.push(Row::new(cells));
    }

    // Build constraints: locked column name + visible statistics with minimal spacing
    // Ratatui Table handles spacing between columns, so we just use the minimum width needed
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

fn render_distribution_table(
    results: &AnalysisResults,
    table_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    if results.distribution_analyses.is_empty() {
        Paragraph::new("No numeric columns for distribution analysis")
            .centered()
            .render(area, buf);
        return;
    }

    // Column headers for width calculation
    let column_names = [
        "Column",
        "Distribution",
        "Fit",
        "Confidence",
        "Score",
        "Outliers",
    ];

    // Calculate column widths based on header names and content (minimal spacing)
    // Note: ratatui Table adds 1 space between columns by default, so we don't add extra padding
    let mut min_col_widths: Vec<u16> = column_names
        .iter()
        .map(|name| name.chars().count() as u16) // header length (no extra padding - table handles spacing)
        .collect();

    // Scan all data to find maximum width needed for each column
    for dist_analysis in &results.distribution_analyses {
        // Calculate combined score: fit_quality * 0.6 + confidence * 0.4 (same as selection logic)
        let combined_score = dist_analysis.fit_quality * 0.6 + dist_analysis.confidence * 0.4;

        // Outlier count with percentage
        let outlier_text = if dist_analysis.outliers.total_count > 0 {
            format!(
                "{} ({:.1}%)",
                dist_analysis.outliers.total_count, dist_analysis.outliers.percentage
            )
        } else {
            "0 (0.0%)".to_string()
        };

        // Update minimum widths based on content
        let col_values = [
            dist_analysis.column_name.clone(),
            format!("{}", dist_analysis.distribution_type),
            format!("{:.2}", dist_analysis.fit_quality),
            format!("{:.2}", dist_analysis.confidence),
            format!("{:.4}", combined_score),
            outlier_text,
        ];

        for (idx, value) in col_values.iter().enumerate() {
            let value_len = value.chars().count() as u16;
            min_col_widths[idx] = min_col_widths[idx].max(value_len); // content width (no padding - table handles spacing)
        }
    }

    // Build constraints from calculated widths
    // Ratatui Table handles spacing between columns automatically (1 space by default)
    let constraints: Vec<Constraint> = min_col_widths
        .iter()
        .map(|&width| Constraint::Length(width))
        .collect();

    let mut rows = Vec::new();

    // Header row
    let header_row = Row::new(vec![
        Cell::from("Column").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Distribution").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Fit").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Confidence").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Score").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Outliers").style(Style::default().add_modifier(Modifier::BOLD)),
    ]);

    // Data rows
    for dist_analysis in &results.distribution_analyses {
        // Calculate combined score: fit_quality * 0.6 + confidence * 0.4 (same as selection logic)
        let combined_score = dist_analysis.fit_quality * 0.6 + dist_analysis.confidence * 0.4;

        // Color coding for distribution type
        let type_color = match dist_analysis.distribution_type {
            DistributionType::Normal | DistributionType::LogNormal => {
                if dist_analysis.confidence > 0.85 {
                    Color::Green
                } else {
                    Color::Yellow
                }
            }
            DistributionType::Uniform | DistributionType::PowerLaw => {
                if dist_analysis.fit_quality > 0.75 {
                    Color::Cyan
                } else {
                    Color::Yellow
                }
            }
            DistributionType::Exponential => Color::Cyan,
            DistributionType::Unknown => Color::Yellow,
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
            Style::default().fg(Color::Red)
        } else if dist_analysis.outliers.percentage > 5.0 {
            // Yellow for moderate outliers (5-20%)
            Style::default().fg(Color::Yellow)
        } else {
            // Default (white) for low outlier percentages (0-5%)
            Style::default()
        };

        rows.push(Row::new(vec![
            Cell::from(dist_analysis.column_name.as_str()),
            Cell::from(format!("{}", dist_analysis.distribution_type))
                .style(Style::default().fg(type_color)),
            Cell::from(format!("{:.2}", dist_analysis.fit_quality)),
            Cell::from(format!("{:.2}", dist_analysis.confidence)),
            Cell::from(format!("{:.4}", combined_score)),
            Cell::from(outlier_text).style(outlier_style),
        ]));
    }

    let table = Table::new(rows, constraints)
        .header(header_row)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    StatefulWidget::render(table, area, buf, table_state);
}

fn render_correlation_matrix(
    results: &AnalysisResults,
    table_state: &mut TableState,
    _selected_cell: &Option<(usize, usize)>,
    area: Rect,
    buf: &mut Buffer,
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
    let mut rows = Vec::new();

    // Header row
    let mut header_cells =
        vec![Cell::from("").style(Style::default().add_modifier(Modifier::BOLD))];
    for col_name in &correlation_matrix.columns {
        header_cells.push(
            Cell::from(col_name.as_str()).style(Style::default().add_modifier(Modifier::BOLD)),
        );
    }
    let header_row = Row::new(header_cells);

    // Data rows
    for (i, col_name) in correlation_matrix.columns.iter().enumerate() {
        let mut cells = vec![
            Cell::from(col_name.as_str()).style(Style::default().add_modifier(Modifier::BOLD))
        ];

        for j in 0..n {
            let correlation = correlation_matrix.correlations[i][j];
            let (bg_color, text_color) = get_correlation_color(correlation);
            let bar = get_correlation_bar(correlation);

            let cell_text = if i == j {
                "1.00".to_string()
            } else {
                format!("{:.2}", correlation)
            };

            cells.push(
                Cell::from(format!("{}\n{}", cell_text, bar))
                    .style(Style::default().fg(text_color).bg(bg_color)),
            );
        }

        rows.push(Row::new(cells));
    }

    let col_width = 12u16;
    let mut constraints = vec![Constraint::Length(20)]; // Row header
    for _ in 0..n {
        constraints.push(Constraint::Length(col_width));
    }

    let table = Table::new(rows, constraints)
        .header(header_row)
        .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    StatefulWidget::render(table, area, buf, table_state);
}

fn get_correlation_color(correlation: f64) -> (Color, Color) {
    let abs_corr = correlation.abs();

    if abs_corr < 0.1 {
        (Color::DarkGray, Color::White)
    } else if correlation > 0.0 {
        if abs_corr >= 0.7 {
            (Color::Green, Color::White)
        } else if abs_corr >= 0.3 {
            (Color::LightGreen, Color::Black)
        } else {
            (Color::Rgb(200, 255, 200), Color::Black)
        }
    } else if abs_corr >= 0.7 {
        (Color::Red, Color::White)
    } else if abs_corr >= 0.3 {
        (Color::Rgb(255, 200, 200), Color::Black)
    } else {
        (Color::Rgb(255, 230, 230), Color::Black)
    }
}

fn get_correlation_bar(correlation: f64) -> String {
    let abs_corr = correlation.abs();
    let _bar_length = 8;

    if abs_corr < 0.1 {
        "░░░░░░░░".to_string()
    } else if abs_corr >= 0.7 {
        "████████".to_string()
    } else if abs_corr >= 0.3 {
        "████░░░░".to_string()
    } else {
        "██░░░░░░".to_string()
    }
}

fn render_distribution_selector(
    selected_dist: DistributionType,
    selector_state: &mut TableState,
    focus: AnalysisFocus,
    area: Rect,
    buf: &mut Buffer,
) {
    let distributions = [
        ("Normal", DistributionType::Normal),
        ("LogNormal", DistributionType::LogNormal),
        ("Uniform", DistributionType::Uniform),
        ("PowerLaw", DistributionType::PowerLaw),
        ("Exponential", DistributionType::Exponential),
    ];

    // Use selector_state.selected() if available, otherwise sync with selected_dist
    let current_selection = selector_state
        .selected()
        .or_else(|| {
            distributions
                .iter()
                .position(|(_, dt)| *dt == selected_dist)
        })
        .unwrap_or(0);

    // Only sync state if it's not set (initial state)
    if selector_state.selected().is_none() {
        selector_state.select(Some(current_selection));
    }

    let items: Vec<ListItem> = distributions
        .iter()
        .enumerate()
        .map(|(idx, (name, dist_type))| {
            let is_selected = *dist_type == selected_dist;
            let is_focused = focus == AnalysisFocus::DistributionSelector
                && selector_state.selected() == Some(idx);
            let prefix = if is_selected { "> " } else { "  " };
            let style = if is_focused {
                Style::default().add_modifier(Modifier::REVERSED)
            } else if is_selected {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default()
            };
            ListItem::new(format!("{}{}", prefix, name)).style(style)
        })
        .collect();

    let block = Block::default()
        .title("Distribution")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::White));

    let list = List::new(items).block(block);

    Widget::render(list, area, buf);
}

fn render_sidebar(
    area: Rect,
    buf: &mut Buffer,
    sidebar_state: &mut TableState,
    selected_tool: AnalysisTool,
    focus: AnalysisFocus,
) {
    let tools = [
        ("Describe", AnalysisTool::Describe),
        ("Distribution Analysis", AnalysisTool::DistributionAnalysis),
        ("Correlation Matrix", AnalysisTool::CorrelationMatrix),
    ];

    let items: Vec<ListItem> = tools
        .iter()
        .enumerate()
        .map(|(idx, (name, tool))| {
            let is_selected = *tool == selected_tool;
            let is_focused =
                focus == AnalysisFocus::Sidebar && sidebar_state.selected() == Some(idx);
            let prefix = if is_selected { "> " } else { "  " };
            let style = if is_focused {
                Style::default().add_modifier(Modifier::REVERSED)
            } else if is_selected {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default()
            };
            ListItem::new(format!("{}{}", prefix, name)).style(style)
        })
        .collect();

    let block = Block::default()
        .title("Analysis Tools")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::White));

    let list = List::new(items).block(block);

    Widget::render(list, area, buf);
}

fn render_distribution_histogram(
    dist: &DistributionAnalysis,
    dist_type: DistributionType,
    area: Rect,
    buf: &mut Buffer,
) {
    // Use BarChart widget to show histogram comparing data vs theoretical distribution
    // Use fixed-width bins that span both data range and theoretical distribution range
    let sorted_data = &dist.sorted_sample_values;

    if sorted_data.is_empty() || sorted_data.len() < 3 {
        Paragraph::new("Insufficient data for histogram")
            .centered()
            .render(area, buf);
        return;
    }

    let n = sorted_data.len();
    let mean = dist.characteristics.mean;
    let std = dist.characteristics.std_dev;

    // Determine bin range: extend beyond data to show full theoretical distribution
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

    // For theoretical distribution visualization, extend range to include distribution spread
    // Use mean ± 5*std for normal/log-normal, or extend data range appropriately for others
    let (hist_min, hist_max) = match dist_type {
        DistributionType::Normal | DistributionType::LogNormal => {
            if std > 0.0 {
                let theoretical_min = (mean - 5.0 * std).min(data_min);
                let theoretical_max = (mean + 5.0 * std).max(data_max);
                (theoretical_min, theoretical_max)
            } else {
                (data_min, data_max)
            }
        }
        DistributionType::Exponential => {
            // Exponential is only defined for x >= 0
            // Extend range to show exponential decay pattern
            let hist_min = data_min.max(0.0);
            // For exponential, most probability is in [0, 3*mean], extend to 5*mean
            let hist_max = if mean > 0.0 {
                (5.0 * mean).max(data_max)
            } else {
                data_max
            };
            (hist_min, hist_max)
        }
        DistributionType::PowerLaw => {
            // Power law is only defined for x > xmin (usually xmin > 0)
            let hist_min = data_min.max(0.0);
            let extension = data_range * 0.2;
            (hist_min, data_max + extension)
        }
        _ => {
            // For other distributions (Uniform, Unknown), extend data range slightly
            let extension = data_range * 0.1;
            (data_min - extension, data_max + extension)
        }
    };

    let hist_range = hist_max - hist_min;

    // Use 10 bins for better granularity
    let num_bins = 10;
    let bin_width = hist_range / num_bins as f64;

    // Create bin boundaries with fixed width
    let bin_boundaries: Vec<f64> = (0..=num_bins)
        .map(|i| hist_min + (i as f64) * bin_width)
        .collect();

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

    // Normalize values to percentages (scale to 0-100)
    let max_data = data_bin_counts.iter().cloned().fold(0, usize::max);
    let max_theory = theory_bin_counts.iter().cloned().fold(0.0, f64::max);
    let global_max = max_data.max(max_theory as usize).max(1) as f64;

    // Create bin labels (show value ranges for each bar)
    let bin_labels: Vec<String> = (0..num_bins)
        .map(|i| {
            let lower = bin_boundaries[i];
            let upper = bin_boundaries[i + 1];
            // Format range compactly
            if hist_range / (num_bins as f64) < 1.0 {
                format!("{:.2}", (lower + upper) / 2.0)
            } else {
                format!("{:.1}", (lower + upper) / 2.0)
            }
        })
        .collect();

    // Create bars alternating data and theory for each bin
    let mut all_bars = Vec::new();

    for (((&data_count, &theory_count), label), _idx) in data_bin_counts
        .iter()
        .zip(theory_bin_counts.iter())
        .zip(bin_labels.iter())
        .zip(0..)
    {
        // Data bar (cyan) with range label
        let data_bar = Bar::default()
            .value((data_count as f64 / global_max * 100.0) as u64)
            .label(Line::from(label.clone()))
            .text_value(format!("{}", data_count))
            .style(Style::default().fg(Color::Cyan));
        all_bars.push(data_bar);

        // Theory bar (grey/DarkGray to match Q-Q plot) with range label
        let theory_bar = Bar::default()
            .value((theory_count / global_max * 100.0) as u64)
            .label(Line::from(label.clone()))
            .text_value(format!("{:.0}", theory_count))
            .style(Style::default().fg(Color::DarkGray));
        all_bars.push(theory_bar);
    }

    // Create title with legend information (no typo - "Histogram")
    let title = format!("Histogram: {} Distribution", dist_type);

    // Create legend as part of title
    let legend = Line::from(vec![
        Span::raw("Data "),
        Span::styled("█", Style::default().fg(Color::Cyan)),
        Span::raw(" Theory "),
        Span::styled("█", Style::default().fg(Color::DarkGray)),
    ]);

    let chart = BarChart::default()
        .block(Block::bordered().title(title).title_bottom(legend))
        .data(BarGroup::default().bars(&all_bars))
        .bar_width(5)
        .bar_gap(1)
        .group_gap(2);

    chart.render(area, buf);
}

fn render_qq_plot(
    dist: &DistributionAnalysis,
    dist_type: DistributionType,
    area: Rect,
    buf: &mut Buffer,
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

    if theory_range <= 0.0 || data_range <= 0.0 {
        Paragraph::new("Insufficient data range for Q-Q plot")
            .centered()
            .render(area, buf);
        return;
    }

    // Create diagonal reference line (y=x if perfect match)
    // Use min/max of both ranges for reference line
    let range_min = data_min.min(theory_min);
    let range_max = data_max.max(theory_max);
    let reference_line = vec![(range_min, range_min), (range_max, range_max)];

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
            .name("Reference")
            .marker(marker)
            .style(Style::default().fg(Color::DarkGray))
            .graph_type(GraphType::Line)
            .data(&reference_line),
        // Q-Q plot data points
        Dataset::default()
            .name("Data")
            .marker(marker)
            .style(Style::default().fg(Color::Cyan))
            .graph_type(GraphType::Scatter)
            .data(&qq_data),
    ];

    // Create X-axis labels
    let x_labels = vec![
        Span::styled(
            format!("{:.1}", theory_min),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.1}", (theory_min + theory_max) / 2.0)),
        Span::styled(
            format!("{:.1}", theory_max),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    // Create Y-axis labels
    let y_labels = vec![
        Span::styled(
            format!("{:.1}", data_min),
            Style::default().add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.1}", (data_min + data_max) / 2.0)),
        Span::styled(
            format!("{:.1}", data_max),
            Style::default().add_modifier(Modifier::BOLD),
        ),
    ];

    let chart = Chart::new(datasets)
        .block(Block::bordered().title("Q-Q Plot: Data (cyan) vs Theory (gray diagonal)"))
        .x_axis(
            Axis::default()
                .title("Theoretical Values")
                .style(Style::default().fg(Color::Gray))
                .bounds([theory_min, theory_max])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .title("Data Values")
                .style(Style::default().fg(Color::Gray))
                .bounds([data_min, data_max])
                .labels(y_labels),
        );

    chart.render(area, buf);
}

fn render_condensed_statistics(
    dist: &DistributionAnalysis,
    selected_dist_type: DistributionType,
    area: Rect,
    buf: &mut Buffer,
) {
    // Display condensed statistics in 1-2 lines at top
    // Statistics are dynamic based on selected distribution type
    let chars = &dist.characteristics;

    // Calculate fit quality for selected distribution type
    let sorted_values = &dist.sorted_sample_values;
    let fit_quality_for_selected = if !sorted_values.is_empty() {
        crate::statistics::calculate_fit_quality(
            sorted_values,
            selected_dist_type,
            chars.mean,
            chars.std_dev,
        )
    } else {
        0.0
    };

    let mut parts = Vec::new();
    parts.push(format!("Type: {:?}", selected_dist_type));
    // Confidence: use detection confidence if selected type matches detected, otherwise use fit quality as confidence
    let confidence_display = if selected_dist_type == dist.distribution_type {
        dist.confidence
    } else {
        fit_quality_for_selected // Use fit quality as confidence-like metric for other distributions
    };
    parts.push(format!("Confidence: {:.2}", confidence_display));
    parts.push(format!("Fit: {:.2}", fit_quality_for_selected));

    if let (Some(sw_stat), Some(sw_p)) = (chars.shapiro_wilk_stat, chars.shapiro_wilk_pvalue) {
        parts.push(format!("SW: {:.3} (p={:.3})", sw_stat, sw_p));
    }

    parts.push(format!("Skew: {:.2}", chars.skewness));
    parts.push(format!("Kurt: {:.2}", chars.kurtosis));

    let line1 = Line::from(parts.join(" | ")).add_modifier(Modifier::BOLD);

    // Second line: basic statistics
    let line2_parts = [
        format!("Mean: {:.2}", chars.mean),
        format!("Std: {:.2}", chars.std_dev),
        format!("CV: {:.3}", chars.coefficient_of_variation),
    ];
    let line2 = Line::from(line2_parts.join(" | "));

    Paragraph::new(vec![line1, line2]).render(area, buf);
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
            let m = chars.mean;
            let s = chars.std_dev;
            let variance = s * s;
            let sigma = (1.0 + variance / (m * m)).ln().sqrt();
            let mu = m.ln() - (sigma * sigma) / 2.0;
            (mu + sigma * z).exp()
        }
        DistributionType::Uniform => {
            // Estimate min/max from mean and std: for uniform, std = (max-min) / sqrt(12)
            let range = chars.std_dev * (12.0_f64).sqrt();
            let min_est = chars.mean - range / 2.0;
            let max_est = chars.mean + range / 2.0;
            min_est + (max_est - min_est) * p
        }
        DistributionType::PowerLaw | DistributionType::Exponential | DistributionType::Unknown => {
            // Fallback: use empirical quantiles from percentiles
            // Interpolate between known percentiles
            if p <= 0.05 {
                dist.percentiles.p5
            } else if p <= 0.25 {
                dist.percentiles.p5
                    + (dist.percentiles.p25 - dist.percentiles.p5) * ((p - 0.05) / 0.20)
            } else if p <= 0.50 {
                dist.percentiles.p25
                    + (dist.percentiles.p50 - dist.percentiles.p25) * ((p - 0.25) / 0.25)
            } else if p <= 0.75 {
                dist.percentiles.p50
                    + (dist.percentiles.p75 - dist.percentiles.p50) * ((p - 0.50) / 0.25)
            } else if p <= 0.95 {
                dist.percentiles.p75
                    + (dist.percentiles.p95 - dist.percentiles.p75) * ((p - 0.75) / 0.20)
            } else {
                dist.percentiles.p95
            }
        }
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
