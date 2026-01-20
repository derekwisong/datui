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

        // Breadcrumb with style matching distribution detail page
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

        // Style matching distribution detail page: black background (default), bold cyan text
        use ratatui::text::{Line, Span};
        let mut breadcrumb_line = Line::default();
        breadcrumb_line.spans.push(Span::styled(
            breadcrumb_text,
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        Paragraph::new(breadcrumb_line).render(layout[0], buf);

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
        // For DistributionAnalysis, omit Ctrl+H help tip and only show r Resample if sampled
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

        // Ctrl+H help tip removed from analysis pages

        // Only show r Resample if data is sampled (for all analysis tools)
        if let Some(results) = self.results {
            if results.sample_size.is_some() {
                hint_line.spans.push(Span::styled(
                    "r",
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                ));
                hint_line.spans.push(Span::raw(" Resample"));
            }
        }

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

        // Title: "Distribution Analysis: [column]" - cyan bold label, bold column name
        use ratatui::text::{Line, Span};
        let mut title_line = Line::default();
        title_line.spans.push(Span::styled(
            "Distribution Analysis: ",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        title_line.spans.push(Span::styled(
            &dist.column_name,
            Style::default().add_modifier(Modifier::BOLD),
        ));
        Paragraph::new(title_line).render(breadcrumb_layout[0], buf);

        // Escape hint on right
        let mut escape_hint = Line::default();
        escape_hint.spans.push(Span::styled(
            "Esc",
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD),
        ));
        escape_hint.spans.push(Span::raw(" Back"));
        Paragraph::new(escape_hint).render(breadcrumb_layout[1], buf);

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
        );

        // Split charts and selector horizontally
        let content_layout = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(75), // Q-Q plot and histogram
                Constraint::Percentage(25), // Distribution selector
            ])
            .split(main_layout[1]);

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

        // Q-Q plot approximation (larger, better aspect ratio)
        // Use selected theoretical distribution from selector
        render_qq_plot(
            dist,
            self.selected_theoretical_distribution,
            qq_plot_area,
            buf,
        );

        // Histogram comparison (vertical bars)
        // Use selected theoretical distribution from selector
        render_distribution_histogram(
            dist,
            self.selected_theoretical_distribution,
            histogram_area,
            buf,
        );

        // Right side: Distribution selector
        render_distribution_selector(
            dist,
            self.selected_theoretical_distribution,
            self.distribution_selector_state,
            self.focus,
            content_layout[1],
            buf,
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

    // Build header row: "Column" (locked) + visible statistic names in Title case
    let mut header_cells =
        vec![Cell::from("Column").style(Style::default().add_modifier(Modifier::BOLD))];
    for &stat_idx in &visible_stats {
        header_cells.push(
            Cell::from(stat_display_names[stat_idx])
                .style(Style::default().add_modifier(Modifier::BOLD)),
        );
    }
    let header_row =
        Row::new(header_cells.clone()).style(Style::default().add_modifier(Modifier::BOLD));
    // Don't add header to rows - it will be set via .header() method only

    // Build data rows: one row per data column
    // Note: rows vector does NOT include the header - header is set separately via .header()
    for col_stat in &results.column_statistics {
        let mut cells = vec![Cell::from(col_stat.name.as_str())];

        // Add statistic values for visible statistics
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
        "Shapiro-Wilk",
        "SW p-value",
        "CV",
        "Outliers",
        "Skewness",
        "Kurtosis",
    ];

    // Calculate column widths based on header names and content (minimal spacing)
    // Note: ratatui Table adds 1 space between columns by default, so we don't add extra padding
    let mut min_col_widths: Vec<u16> = column_names
        .iter()
        .map(|name| name.chars().count() as u16) // header length (no extra padding - table handles spacing)
        .collect();

    // Scan all data to find maximum width needed for each column
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

        // Update minimum widths based on content
        let col_values = [
            dist_analysis.column_name.clone(),
            format!("{}", dist_analysis.distribution_type),
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
        Cell::from("Shapiro-Wilk").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("SW p-value").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("CV").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Outliers").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Skewness").style(Style::default().add_modifier(Modifier::BOLD)),
        Cell::from("Kurtosis").style(Style::default().add_modifier(Modifier::BOLD)),
    ]);

    // Data rows
    for dist_analysis in &results.distribution_analyses {
        // Color coding for distribution type based on fit quality
        let type_color = match dist_analysis.distribution_type {
            DistributionType::Normal | DistributionType::LogNormal => {
                if dist_analysis.fit_quality > 0.75 {
                    Color::Green
                } else {
                    Color::Yellow
                }
            }
            DistributionType::Uniform
            | DistributionType::PowerLaw
            | DistributionType::Exponential
            | DistributionType::Beta
            | DistributionType::Gamma
            | DistributionType::ChiSquared
            | DistributionType::StudentsT
            | DistributionType::Weibull => {
                if dist_analysis.fit_quality > 0.75 {
                    Color::Cyan
                } else {
                    Color::Yellow
                }
            }
            DistributionType::Poisson
            | DistributionType::Bernoulli
            | DistributionType::Binomial
            | DistributionType::Geometric => {
                if dist_analysis.fit_quality > 0.75 {
                    Color::Cyan
                } else {
                    Color::Yellow
                }
            }
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

        // Get skewness and kurtosis values for styling
        let skewness_value = dist_analysis.characteristics.skewness.abs();
        let kurtosis_value = dist_analysis.characteristics.kurtosis;

        // Skewness color coding: similar to describe table
        let skewness_style = if skewness_value >= 3.0 {
            Style::default().fg(Color::Red)
        } else if skewness_value >= 1.0 {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
        };

        // Kurtosis color coding: 3.0 is normal, high/low is notable
        let kurtosis_style = if (kurtosis_value - 3.0).abs() >= 3.0 {
            Style::default().fg(Color::Red)
        } else if (kurtosis_value - 3.0).abs() >= 1.0 {
            Style::default().fg(Color::Yellow)
        } else {
            Style::default()
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

        rows.push(Row::new(vec![
            Cell::from(dist_analysis.column_name.as_str()),
            Cell::from(format!("{}", dist_analysis.distribution_type))
                .style(Style::default().fg(type_color)),
            Cell::from(sw_stat_text),
            Cell::from(sw_pvalue_text),
            Cell::from(format!(
                "{:.4}",
                dist_analysis.characteristics.coefficient_of_variation
            )),
            Cell::from(outlier_text).style(outlier_style),
            Cell::from(format_num(dist_analysis.characteristics.skewness)).style(skewness_style),
            Cell::from(format_num(dist_analysis.characteristics.kurtosis)).style(kurtosis_style),
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
    dist: &DistributionAnalysis,
    selected_dist: DistributionType,
    selector_state: &mut TableState,
    focus: AnalysisFocus,
    area: Rect,
    buf: &mut Buffer,
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

    // Calculate fit quality for each distribution
    let sorted_values = &dist.sorted_sample_values;
    let chars = &dist.characteristics;

    // Create table rows with Name, Fit
    let rows: Vec<Row> = distributions
        .iter()
        .enumerate()
        .map(|(idx, (name, dist_type))| {
            // Calculate fit quality for this distribution
            let fit = if !sorted_values.is_empty() {
                crate::statistics::calculate_fit_quality(
                    sorted_values,
                    *dist_type,
                    chars.mean,
                    chars.std_dev,
                )
            } else {
                0.0
            };

            let is_selected = *dist_type == selected_dist;
            let is_focused = focus == AnalysisFocus::DistributionSelector
                && selector_state.selected() == Some(idx);

            // Style cells based on selection/focus
            let name_style = if is_focused {
                Style::default().add_modifier(Modifier::REVERSED)
            } else if is_selected {
                Style::default().fg(Color::Cyan)
            } else {
                Style::default()
            };

            Row::new(vec![
                Cell::from(name.to_string()).style(name_style),
                Cell::from(format!("{:.3}", fit)), // 3 digits precision
            ])
        })
        .collect();

    // Create table with columns: Name, Fit
    // Fit column should be minimal width (just enough for 3 digits + decimal point)
    let header = Row::new(vec![Cell::from("Name"), Cell::from("Fit")])
        .style(Style::default().add_modifier(Modifier::BOLD));

    let table = Table::new(
        rows,
        vec![
            Constraint::Fill(1),   // Name column takes remaining space
            Constraint::Length(5), // Fit column: "0.000" = 5 chars
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title("Distribution")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::White)),
    )
    .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED));

    StatefulWidget::render(table, area, buf, selector_state);
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

    // Determine bin range: ALWAYS use data range (same bins regardless of distribution)
    // The data histogram should remain constant; only the theoretical overlay changes
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

    // Always use the actual data range with a small extension for edge bins
    // This ensures the data histogram is consistent regardless of selected distribution
    // For non-negative data (counts, prices, etc.), don't extend below 0
    let extension = data_range * 0.05; // Small 5% extension on each side
    let hist_min = if data_min >= 0.0 {
        // For non-negative data, don't extend below 0
        (data_min - extension).max(0.0)
    } else {
        // For data that can be negative, allow extension below minimum
        data_min - extension
    };
    let hist_max = data_max + extension;
    let hist_range = hist_max - hist_min;

    // Calculate actual Y-axis label widths dynamically
    // We need to check both histogram labels (counts) and Q-Q plot labels (data values)
    // For histogram: labels are 0, global_max/2, global_max (will calculate after global_max)
    // For Q-Q plot: labels are data_min, (data_min+data_max)/2, data_max formatted as {:.1}
    // Calculate Q-Q plot label widths first (we have data_min and data_max here)
    let qq_label_bottom = format!("{:.1}", data_min);
    let qq_label_mid = format!("{:.1}", (data_min + data_max) / 2.0);
    let qq_label_top = format!("{:.1}", data_max);
    let qq_max_label_width = qq_label_bottom
        .chars()
        .count()
        .max(qq_label_mid.chars().count())
        .max(qq_label_top.chars().count());

    // Calculate dynamic number of bins based on available width
    // This ensures bars fill the horizontal space and look dense at all widths

    // For initial calculation, estimate histogram label width (will refine after global_max)
    // Estimate: histogram labels are counts, typically smaller than Q-Q plot labels
    // Use Q-Q plot width as initial estimate, will refine after we calculate global_max
    let initial_max_label_width = qq_max_label_width;

    // Calculate initial Y-axis space (will refine after histogram labels calculated)
    let initial_y_axis_label_width = (initial_max_label_width as u16).max(1) + 1;
    let y_axis_gap = 1u16; // Minimal gap between labels and plot area (needed to prevent bars from extending outside)
    let total_y_axis_space = initial_y_axis_label_width + y_axis_gap;

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

    // Normalize values for display (find the maximum for scaling)
    let max_data = data_bin_counts.iter().cloned().fold(0, usize::max);
    let max_theory = theory_bin_counts.iter().cloned().fold(0.0, f64::max);
    let global_max = max_data.max(max_theory as usize).max(1) as f64;

    // Calculate actual Y-axis label widths for histogram
    // Labels are: 0, global_max/2, global_max (formatted as integers)
    let hist_label_0 = format!("{}", 0);
    let hist_label_mid = format!("{}", (global_max / 2.0) as usize);
    let hist_label_max = format!("{}", global_max as usize);
    let hist_max_label_width = hist_label_0
        .chars()
        .count()
        .max(hist_label_mid.chars().count())
        .max(hist_label_max.chars().count());

    // Refine Y-axis label width calculation using actual histogram labels
    // Calculate maximum label width across both histogram and Q-Q plot
    let max_label_width = hist_max_label_width.max(qq_max_label_width);

    // Recalculate Y-axis label width with actual maximum (may update from initial estimate)
    let y_axis_label_width = (max_label_width as u16).max(1) + 1; // Max label width + 1 char padding
                                                                  // Note: total_y_axis_space is already calculated above, using this for alignment in labels

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
            .style(Style::default().fg(Color::Cyan));

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

    // Recalculate total_y_axis_space with refined y_axis_label_width if it changed
    let refined_total_y_axis_space = y_axis_label_width + y_axis_gap;
    // If width changed, adjust available_width and bar_plot_area
    let adjusted_available_width = if refined_total_y_axis_space != total_y_axis_space {
        area.width.saturating_sub(refined_total_y_axis_space)
    } else {
        available_width
    };

    let bar_plot_area = Rect::new(
        area.left() + refined_total_y_axis_space, // Start where Chart's plot area starts (use refined value)
        chart_inner_top,                          // Start below title
        adjusted_available_width,                 // Width matching Chart's plot area exactly
        chart_inner_height,                       // Use calculated height that accounts for title
    );

    let barchart = BarChart::default()
        .block(Block::default()) // No borders in sub-area - borders handled separately
        .data(BarGroup::default().bars(&data_bars))
        .bar_width(final_bar_width)
        .bar_gap(bar_gap)
        .group_gap(group_gap);

    // Render bar chart to sub-area matching Chart's plot area (excluding x-axis label space)
    barchart.render(bar_plot_area, buf);

    // No border - chart renders without surrounding box

    // Overlay theory distribution as dense scatter plot (dot plot) on top of bar chart
    // Use the same bin-based calculations as the bars for consistency and correct scaling
    // Interpolate from discrete bin values to create smooth overlay curve
    let num_samples = 250; // Dense sampling for continuous curve appearance

    // Pre-calculate normalized heights for all bins (same as bars use)
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

    let theory_points: Vec<(f64, f64)> = if num_bins > 0
        && !theory_bin_counts.is_empty()
        && !bin_centers.is_empty()
        && !theory_normalized_heights.is_empty()
        && num_samples > 1
        && hist_range > 0.0
    {
        // Interpolate from discrete bin centers and normalized heights to create smooth curve
        (0..num_samples)
            .map(|i| {
                // Sample x values across the histogram range
                // For non-negative data, sampling starts from clamped hist_min (>= 0)
                let x = hist_min + (i as f64 / (num_samples - 1) as f64) * hist_range;

                // Find which bins this x value falls between for interpolation
                let normalized_height = if x <= bin_centers[0] {
                    // Before first bin center: use first bin's normalized height
                    theory_normalized_heights[0]
                } else if x >= bin_centers[bin_centers.len() - 1] {
                    // After last bin center: use last bin's normalized height
                    theory_normalized_heights[theory_normalized_heights.len() - 1]
                } else {
                    // Between bin centers: linear interpolation
                    // Find the two bin centers that bracket this x value
                    let mut lower_idx = 0;
                    for (idx, &center) in bin_centers.iter().enumerate() {
                        if center <= x {
                            lower_idx = idx;
                        } else {
                            break;
                        }
                    }

                    // Clamp to valid range
                    let lower_idx = lower_idx.min(bin_centers.len() - 1);
                    let upper_idx = (lower_idx + 1).min(bin_centers.len() - 1);

                    if lower_idx == upper_idx {
                        // At exact bin center or edge case
                        theory_normalized_heights[lower_idx]
                    } else {
                        // Linear interpolation between two bins
                        let x_lower = bin_centers[lower_idx];
                        let x_upper = bin_centers[upper_idx];
                        let y_lower = theory_normalized_heights[lower_idx];
                        let y_upper = theory_normalized_heights[upper_idx];

                        // Linear interpolation: y = y_lower + (y_upper - y_lower) * (x - x_lower) / (x_upper - x_lower)
                        let t = if x_upper > x_lower {
                            (x - x_lower) / (x_upper - x_lower)
                        } else {
                            0.0
                        };
                        y_lower + (y_upper - y_lower) * t
                    }
                };

                (x, normalized_height)
            })
            .collect()
    } else {
        // Fallback: use bin centers directly if interpolation fails
        bin_centers
            .iter()
            .zip(theory_normalized_heights.iter())
            .map(|(&bin_center, &normalized_height)| (bin_center, normalized_height))
            .collect()
    };

    // Create scatter plot dataset for theoretical distribution
    // Use Braille marker for dense dot plot (like Q-Q plot)
    let marker = symbols::Marker::Braille; // Dense, continuous appearance

    let theory_dataset = Dataset::default()
        .name("") // Empty name to prevent legend from appearing
        .marker(marker)
        .graph_type(GraphType::Scatter)
        .style(Style::default().fg(Color::DarkGray))
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
                .fg(Color::Gray)
                .add_modifier(Modifier::BOLD),
        ),
        Span::raw(format!("{:.1}", (hist_min + hist_max) / 2.0)),
        Span::styled(
            format!("{:.1}", hist_max),
            Style::default()
                .fg(Color::Gray)
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
                .style(Style::default().fg(Color::Gray))
                .labels(x_labels), // Show x-axis labels with histogram range
        )
        .y_axis(
            Axis::default()
                .title("Counts")
                .style(Style::default().fg(Color::Gray))
                .bounds([0.0, 100.0])
                .labels({
                    // Use dynamic label width calculated earlier (y_axis_label_width - 1 for padding)
                    // This ensures alignment with Q-Q plot using actual label lengths
                    let label_width = (y_axis_label_width - 1) as usize; // Remove padding char (used for alignment)
                    vec![
                        // Bottom label: 0 counts (right-aligned to fixed width)
                        Span::styled(
                            format!("{:>width$}", 0, width = label_width),
                            Style::default()
                                .fg(Color::Gray)
                                .add_modifier(Modifier::BOLD),
                        ),
                        // Middle label: half of max counts (right-aligned)
                        Span::styled(
                            format!(
                                "{:>width$}",
                                (global_max / 2.0) as usize,
                                width = label_width
                            ),
                            Style::default().fg(Color::Gray),
                        ),
                        // Top label: max counts (right-aligned)
                        Span::styled(
                            format!("{:>width$}", global_max as usize, width = label_width),
                            Style::default()
                                .fg(Color::Gray)
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
            .name("") // Empty name to hide from legend
            .marker(marker)
            .style(Style::default().fg(Color::DarkGray))
            .graph_type(GraphType::Line)
            .data(&reference_line),
        // Q-Q plot data points
        Dataset::default()
            .name("") // Empty name to hide from legend
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

    // Create Y-axis labels with dynamic width for alignment with histogram
    // Calculate actual label widths - will match histogram's calculation
    // Q-Q plot labels: data_min, (data_min+data_max)/2, data_max formatted as {:.1}
    let qq_label_bottom_len = format!("{:.1}", data_min).chars().count();
    let qq_label_mid_len = format!("{:.1}", (data_min + data_max) / 2.0)
        .chars()
        .count();
    let qq_label_top_len = format!("{:.1}", data_max).chars().count();
    let qq_max_width = qq_label_bottom_len
        .max(qq_label_mid_len)
        .max(qq_label_top_len);

    // Use dynamic width based on actual labels (histogram calculates same way and takes max)
    // Both charts should align using the maximum width across all labels
    // Histogram will calculate its own labels and use max(qq_max_width, hist_max_width)
    let label_width = qq_max_width.max(1) + 1; // Max width + 1 padding (histogram may override if wider)
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
                .style(Style::default().fg(Color::Gray))
                .bounds([theory_min, theory_max])
                .labels(x_labels),
        )
        .y_axis(
            Axis::default()
                .title("Data Values")
                .style(Style::default().fg(Color::Gray)) // Axis line should be gray
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
) {
    // Display statistics in single line: SW score, skew, kurtosis, median, mean, std, CV
    // Cyan labels, plain values, no background
    let chars = &dist.characteristics;

    let mut line_parts = Vec::new();

    // Shapiro-Wilk score
    if let (Some(sw_stat), Some(sw_p)) = (chars.shapiro_wilk_stat, chars.shapiro_wilk_pvalue) {
        line_parts.push(Span::styled("SW: ", Style::default().fg(Color::Cyan)));
        line_parts.push(Span::raw(format!("{:.3} (p={:.3})", sw_stat, sw_p)));
        line_parts.push(Span::raw(" ")); // Space separator
    }

    // Skew
    line_parts.push(Span::styled("Skew: ", Style::default().fg(Color::Cyan)));
    line_parts.push(Span::raw(format!("{:.2}", chars.skewness)));
    line_parts.push(Span::raw(" ")); // Space separator

    // Kurtosis
    line_parts.push(Span::styled("Kurt: ", Style::default().fg(Color::Cyan)));
    line_parts.push(Span::raw(format!("{:.2}", chars.kurtosis)));
    line_parts.push(Span::raw(" ")); // Space separator

    // Median
    line_parts.push(Span::styled("Median: ", Style::default().fg(Color::Cyan)));
    line_parts.push(Span::raw(format!("{:.2}", dist.percentiles.p50)));
    line_parts.push(Span::raw(" ")); // Space separator

    // Mean
    line_parts.push(Span::styled("Mean: ", Style::default().fg(Color::Cyan)));
    line_parts.push(Span::raw(format!("{:.2}", chars.mean)));
    line_parts.push(Span::raw(" ")); // Space separator

    // Std
    line_parts.push(Span::styled("Std: ", Style::default().fg(Color::Cyan)));
    line_parts.push(Span::raw(format!("{:.2}", chars.std_dev)));
    line_parts.push(Span::raw(" ")); // Space separator

    // CV
    line_parts.push(Span::styled("CV: ", Style::default().fg(Color::Cyan)));
    line_parts.push(Span::raw(format!("{:.3}", chars.coefficient_of_variation)));

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
        DistributionType::Exponential => {
            // Exponential quantile: q(p) = -ln(1-p) / lambda, where lambda = 1/mean
            if chars.mean > 0.0 {
                -chars.mean * (1.0 - p).ln()
            } else {
                dist.percentiles.p50
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
            // Gamma quantile: estimate parameters and use approximation
            // For large shape, use normal approximation
            let mean = chars.mean;
            let variance = chars.std_dev * chars.std_dev;
            if mean > 0.0 && variance > 0.0 {
                let shape = (mean * mean) / variance;
                let scale = variance / mean;
                if shape > 30.0 {
                    // Normal approximation
                    let normal_mean = shape * scale;
                    let normal_std = (shape * scale * scale).sqrt();
                    let z = approximate_normal_quantile(p);
                    normal_mean + normal_std * z
                } else {
                    // Use empirical percentiles for small shape
                    dist.percentiles.p50
                }
            } else {
                dist.percentiles.p50
            }
        }
        DistributionType::ChiSquared => {
            // Chi-squared quantile: special case of gamma with shape = df/2, scale = 2
            // Estimate df from mean (mean = df for chi-squared)
            let df = chars.mean;
            if df > 30.0 {
                // Normal approximation
                let normal_mean = df;
                let normal_std = (2.0 * df).sqrt();
                let z = approximate_normal_quantile(p);
                normal_mean + normal_std * z
            } else {
                // Use empirical percentiles
                dist.percentiles.p50
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
            let lambda = chars.mean;
            if lambda > 20.0 {
                let z = approximate_normal_quantile(p);
                (lambda + z * lambda.sqrt()).max(0.0)
            } else {
                // Use empirical percentiles for small lambda
                dist.percentiles.p50
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
            let mean = chars.mean;
            let variance = chars.std_dev * chars.std_dev;
            if variance > 0.0 {
                let z = approximate_normal_quantile(p);
                (mean + z * variance.sqrt()).max(0.0)
            } else {
                dist.percentiles.p50
            }
        }
        DistributionType::Geometric => {
            // Geometric quantile: q(p) = floor(ln(1-p) / ln(1-p_param))
            let mean = chars.mean; // mean = (1-p)/p for geometric
            if mean > 0.0 {
                let p_param = 1.0 / (mean + 1.0);
                if p_param > 0.0 && p_param < 1.0 {
                    (-(1.0 - p).ln() / (1.0 - p_param).ln()).max(0.0)
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
            let sorted_data = &dist.sorted_sample_values;
            if !sorted_data.is_empty() && sorted_data[0] > 0.0 {
                // Estimate shape and scale from data
                let mean = chars.mean;
                let variance = chars.std_dev * chars.std_dev;
                if mean > 0.0 && variance > 0.0 {
                    // Approximate shape from CV
                    let cv = chars.std_dev / mean;
                    let shape_est = if cv < 1.0 {
                        // Approximation for shape parameter
                        1.0 / cv
                    } else {
                        1.0
                    };
                    // Scale from mean
                    let gamma_1_over_shape = 1.0 + 1.0 / shape_est; // Approximation
                    let scale_est = mean / gamma_1_over_shape;
                    if scale_est > 0.0 && shape_est > 0.0 {
                        scale_est * (-(1.0 - p).ln()).powf(1.0 / shape_est)
                    } else {
                        dist.percentiles.p50
                    }
                } else {
                    dist.percentiles.p50
                }
            } else {
                dist.percentiles.p50
            }
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
