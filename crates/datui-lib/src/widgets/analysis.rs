use ratatui::{
    buffer::Buffer,
    layout::{Constraint, Direction, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Bar, BarChart, BarGroup, Block, Cell, Chart, Dataset, GraphType, HighlightSpacing,
        Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::analysis_modal::{
    AnalysisFocus, AnalysisTool, AnalysisView, ColumnScroll, HistogramScale,
};
use crate::chart_data::{AxisFormat, AxisNumbers};
use crate::config::Theme;
use crate::distribution_fit::{FitOutcome, FitTest};
use crate::glyphs::PlotMarks;
use crate::numfmt::{self, NumberFormatSettings};
use crate::render::context::RenderContext;
use crate::statistics::{
    AnalysisContext, AnalysisResults, CategoricalStatistics, ColumnStatistics, CorrelationMethod,
    DistributionAnalysis, DistributionType, NumericStatistics, TemporalStatistics,
};
use crate::widgets::axes::{AxisSpec, PlotAxes};
use crate::widgets::datatable::DataTableState;
use crate::widgets::ui::Surface;
use polars::prelude::{AnyValue, DataType};

pub struct AnalysisWidgetConfig<'a> {
    pub state: &'a DataTableState,
    pub results: Option<&'a AnalysisResults>,
    pub context: &'a AnalysisContext,
    pub view: AnalysisView,
    pub selected_tool: Option<AnalysisTool>,
    pub selected_correlation: Option<(usize, usize)>,
    pub correlation_method: CorrelationMethod,
    pub focus: AnalysisFocus,
    pub selected_theoretical_distribution: DistributionType,
    pub histogram_scale: HistogramScale,
    pub theme: &'a Theme,
    pub table_cell_padding: u16,
    /// Display-time number formatting, so counts here match the data table.
    pub number_format: &'a NumberFormatSettings,
    /// The shared sample the results were read with, for the header.
    pub sample: &'a crate::sampling::Sample,
    pub ctx: &'a RenderContext,
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
    selected_correlation: Option<(usize, usize)>,
    correlation_method: CorrelationMethod,
    focus: AnalysisFocus,
    selected_theoretical_distribution: DistributionType,
    distribution_selector_state: &'a mut TableState,
    histogram_scale: HistogramScale,
    theme: &'a Theme,
    table_cell_padding: u16,
    number_format: &'a NumberFormatSettings,
    sample: &'a crate::sampling::Sample,
    ctx: &'a RenderContext,
    /// The selected tool's statistic scroll; the table sets how far it goes.
    column_scroll: &'a mut ColumnScroll,
}

impl<'a> AnalysisWidget<'a> {
    pub fn new(
        config: AnalysisWidgetConfig<'a>,
        table_state: &'a mut TableState,
        distribution_table_state: &'a mut TableState,
        correlation_table_state: &'a mut TableState,
        sidebar_state: &'a mut TableState,
        distribution_selector_state: &'a mut TableState,
        column_scroll: &'a mut ColumnScroll,
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
            selected_correlation: config.selected_correlation,
            correlation_method: config.correlation_method,
            focus: config.focus,
            selected_theoretical_distribution: config.selected_theoretical_distribution,
            distribution_selector_state,
            histogram_scale: config.histogram_scale,
            theme: config.theme,
            table_cell_padding: config.table_cell_padding,
            number_format: config.number_format,
            sample: config.sample,
            ctx: config.ctx,
            column_scroll,
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
            Some(AnalysisTool::Describe) => "Describe".to_string(),
            Some(AnalysisTool::DistributionAnalysis) => "Distribution Analysis".to_string(),
            // The coefficient is part of the title: the cells do not say which it is.
            Some(AnalysisTool::CorrelationMatrix) => format!(
                "Correlation Matrix {} {}",
                crate::glyphs::get().middot,
                coefficient_name(self.correlation_method)
            ),
            Some(AnalysisTool::DataQuality) => "Data Quality".to_string(),
            None => "Analysis".to_string(),
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
            _ => tool_name,
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
                Paragraph::new("Pick a tool in the sidebar")
                    .centered()
                    .style(Style::default().fg(self.theme.get("text_primary")))
                    .render(inner[1], buf);
            }
            Some(tool) => {
                if let Some(results) = self.results {
                    match tool {
                        AnalysisTool::Describe => {
                            StatisticsTable {
                                results,
                                focused: self.focus == AnalysisFocus::Main,
                                theme: self.theme,
                                table_cell_padding: self.table_cell_padding,
                                number_format: self.number_format,
                            }
                            .render(
                                main_layout[0],
                                buf,
                                self.table_state,
                                self.column_scroll,
                            );
                        }
                        AnalysisTool::DistributionAnalysis => {
                            render_distribution_table(
                                results,
                                self.distribution_table_state,
                                self.column_scroll,
                                self.focus == AnalysisFocus::Main,
                                main_layout[0],
                                buf,
                                self.theme,
                            );
                        }
                        AnalysisTool::CorrelationMatrix => {
                            render_correlation_matrix(
                                results.correlation_matrix.as_ref().map(|matrix| Shown {
                                    matrix,
                                    method: self.correlation_method,
                                }),
                                self.correlation_table_state,
                                &self.selected_correlation,
                                self.column_scroll,
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

            // The key figures over the charts, wrapped rather than cut: at 60
            // columns they take three lines.
            let stats = condensed_statistics_lines(
                &condensed_statistics(dist),
                layout[1].width,
                self.theme,
            );
            let stats_height = (stats.len() as u16).clamp(1, 3);
            let main_layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(stats_height), Constraint::Fill(1)])
                .split(layout[1]);
            Paragraph::new(stats).render(main_layout[0], buf);

            // Charts on the left; the family list on the right, never narrower than
            // its longest name and p-value.
            let content_layout = Layout::default()
                .direction(Direction::Horizontal)
                .constraints([
                    Constraint::Fill(1),
                    Constraint::Length(SELECTOR_WIDTH.max(main_layout[1].width / 4)),
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

            // The value axes, both x axes and the Q-Q plot's y, read the column's
            // numbers over the sample's range; the histogram's y reads counts.
            let values = AxisNumbers::measure(self.number_format, &dist.column_name);
            let counts = AxisNumbers::count(self.number_format);
            let sorted_data = &dist.sorted_sample_values;
            let unified_x_range = match (sorted_data.first(), sorted_data.last()) {
                (Some(&lo), Some(&hi)) => (lo, hi),
                _ => (0.0, 1.0),
            };

            // Both plots' y labels take one width, so the plots start in the same
            // column: the widest Q-Q value, or the widest count the histogram could
            // reach, the sample's size.
            let (lo, hi) = unified_x_range;
            let qq_format = AxisFormat::ends_and_middle([lo, hi], &values);
            let qq_width = [lo, (lo + hi) / 2.0, hi]
                .iter()
                .filter_map(|&v| qq_format.label(v, 0))
                .map(|l| l.chars().count())
                .max()
                .unwrap_or(1);
            let n = sorted_data.len() as f64;
            let count_width = AxisFormat::new(&[0.0, n], &counts)
                .label(n, 0)
                .map_or(1, |l| l.chars().count());
            let shared_y_axis_label_width = (qq_width.max(count_width) as u16).max(1) + 1;

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
                values: &values,
                counts: &counts,
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

            render_distribution_selector(
                SelectorConfig {
                    dist,
                    selected: self.selected_theoretical_distribution,
                    histogram_scale: self.histogram_scale,
                    log_scale_unavailable: log_scale_requested_but_unavailable,
                    theme: self.theme,
                    ctx: self.ctx,
                },
                self.distribution_selector_state,
                content_layout[1],
                buf,
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
            Shown {
                matrix,
                method: self.correlation_method,
            },
            (row, col),
            total_rows,
            layout[1],
            buf,
            self.theme,
            self.number_format,
        );
    }
}

/// The correlation matrix as the screen shows it: by the method chosen.
#[derive(Clone, Copy)]
struct Shown<'a> {
    matrix: &'a crate::statistics::CorrelationMatrix,
    method: CorrelationMethod,
}

/// The coefficient's name and symbol, as the matrix title and the pair detail
/// give it.
fn coefficient_name(method: CorrelationMethod) -> String {
    match method {
        CorrelationMethod::Pearson => "Pearson r".to_string(),
        CorrelationMethod::Spearman => format!("Spearman {}", crate::glyphs::get().rho),
    }
}

/// `r` at `decimals` places, where rounding never makes it ±1 unless it is: a
/// near-perfect 0.9996 reads 0.999 at three places, not a perfect 1.000.
fn format_coefficient(r: f64, decimals: usize) -> String {
    let text = format!("{r:.decimals$}");
    if r.abs() < 1.0 && text.trim_start_matches('-').starts_with('1') {
        let sign = if r < 0.0 { "-" } else { "" };
        format!("{sign}0.{}", "9".repeat(decimals))
    } else {
        text
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
    Shown { matrix, method }: Shown,
    (row, col): (usize, usize),
    total_rows: usize,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
    number_format: &NumberFormatSettings,
) {
    let r = matrix.coefficient(method, row, col);
    let pairs = matrix.sample_sizes[row][col];
    let p_value = matrix.p_value(method, row, col);

    let label_style = Style::default().fg(theme.get("text_secondary"));
    let value_style = Style::default().fg(theme.get("text_primary"));

    let mut lines: Vec<Line> = Vec::new();
    if r.is_nan() {
        let why = if pairs < 3 {
            "Fewer than 3 overlapping pairs"
        } else {
            "A column holds one value"
        };
        lines.push(Line::from(vec![Span::styled(why, value_style)]));
    } else {
        lines.push(Line::from(vec![
            Span::styled(format!("{}: ", coefficient_name(method)), label_style),
            Span::styled(
                format_coefficient(r, 4),
                Style::default().fg(get_correlation_color(r, theme)),
            ),
            Span::styled(format!("  ({})", describe_correlation(r)), value_style),
        ]));
        lines.push(Line::from(vec![
            Span::styled(format!("{}: ", crate::glyphs::get().r_squared), label_style),
            Span::styled(format_coefficient(r * r, 4), value_style),
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

/// The Describe tool's table: a row per column, a column per statistic.
struct StatisticsTable<'a> {
    results: &'a AnalysisResults,
    focused: bool,
    theme: &'a Theme,
    table_cell_padding: u16,
    number_format: &'a NumberFormatSettings,
}

impl StatisticsTable<'_> {
    fn render(
        self,
        area: Rect,
        buf: &mut Buffer,
        table_state: &mut TableState,
        columns: &mut ColumnScroll,
    ) {
        let StatisticsTable {
            results,
            focused,
            theme,
            table_cell_padding,
            number_format,
        } = self;
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

        let column_spacing = table_cell_padding;
        // The rail's column comes first, then the locked names.
        let available_width = area
            .width
            .saturating_sub(RAIL_WIDTH + locked_col_width)
            .saturating_sub(column_spacing);
        let (start_stat, end_stat) =
            stat_window(&min_col_widths, available_width, column_spacing, columns);
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
            .row_highlight_style(cursor_style(focused, theme))
            .highlight_symbol(cursor_rail(focused, theme))
            .highlight_spacing(HighlightSpacing::Always);

        StatefulWidget::render(table, area, buf, table_state);
        draw_scroll_marks(
            area,
            buf,
            RAIL_WIDTH + locked_col_width,
            (start_stat, end_stat, num_stats),
            theme,
        );
    }
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
    columns: &mut ColumnScroll,
    focused: bool,
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

    let column_spacing = 1u16;
    // The rail's column comes first, then the locked names.
    let available_width = area
        .width
        .saturating_sub(RAIL_WIDTH + locked_col_width)
        .saturating_sub(column_spacing);
    let (start_stat, end_stat) =
        stat_window(&min_col_widths, available_width, column_spacing, columns);
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
        .row_highlight_style(cursor_style(focused, theme))
        .highlight_symbol(cursor_rail(focused, theme))
        .highlight_spacing(HighlightSpacing::Always);

    StatefulWidget::render(table, area, buf, table_state);
    draw_scroll_marks(
        area,
        buf,
        RAIL_WIDTH + locked_col_width,
        (start_stat, end_stat, num_stats),
        theme,
    );
}

/// The column the cursor's rail sits in, kept whether or not the table has focus
/// so focus arriving moves nothing.
const RAIL_WIDTH: u16 = 1;

/// The rail beside the row the cursor is on while the table has focus: the
/// tint alone vanishes on a 16-color terminal whose black is the background.
fn cursor_rail(focused: bool, theme: &Theme) -> Span<'static> {
    let g = crate::glyphs::get();
    Span::styled(
        if focused { g.rail } else { " " },
        Style::default().fg(theme.get("accent")),
    )
}

/// The row the cursor is on: the tint while the table has focus, the accent
/// alone while the tool list has it, so the cursor stays visible without
/// claiming focus.
fn cursor_style(focused: bool, theme: &Theme) -> Style {
    if focused {
        theme.highlight_style()
    } else {
        Style::default().fg(theme.get("accent"))
    }
}

/// The mark that counts statistics hidden to the right, as the data table's does.
fn more_mark(hidden: usize) -> String {
    format!(" +{hidden} {}", crate::glyphs::get().arrow_right)
}

/// Which statistics fit beside the locked name column, as `start..end`, from the
/// scroll's offset. Sets the scroll's `max` to the first start that brings the
/// last statistic into view, and clamps the offset to it, so a key press past the
/// end does nothing and the first press back always moves. A window that leaves
/// statistics out to the right keeps room for the mark that counts them.
fn stat_window(
    widths: &[u16],
    available: u16,
    spacing: u16,
    columns: &mut ColumnScroll,
) -> (usize, usize) {
    let n = widths.len();
    if n == 0 {
        *columns = ColumnScroll::default();
        return (0, 0);
    }
    let fits = |from: usize, room: u16| {
        let mut used = 0u16;
        let mut count = 0usize;
        for width in &widths[from..] {
            let needed = width + if count > 0 { spacing } else { 0 };
            if used + needed > room {
                break;
            }
            used += needed;
            count += 1;
        }
        count.max(1)
    };
    let mark = crate::glyphs::display_width(&more_mark(n)) as u16;
    let shown = |from: usize| {
        let all = fits(from, available);
        if from + all >= n {
            all
        } else {
            fits(from, available.saturating_sub(mark))
        }
    };
    let max = (0..n)
        .find(|&from| from + shown(from) >= n)
        .unwrap_or(n - 1);
    columns.max = max;
    columns.offset = columns.offset.min(max);
    let start = columns.offset;
    (start, (start + shown(start)).min(n))
}

/// Say that statistics are out of view: an arrow at the end of the locked
/// column's header when some are to the left, and the count at the right edge of
/// the header when some are to the right.
fn draw_scroll_marks(
    area: Rect,
    buf: &mut Buffer,
    locked_width: u16,
    (start, end, total): (usize, usize, usize),
    theme: &Theme,
) {
    if area.height == 0 || area.width == 0 {
        return;
    }
    let style = header_style(theme, "controls_bg", "accent").add_modifier(Modifier::BOLD);
    if start > 0 && locked_width > 0 && locked_width <= area.width {
        Paragraph::new(crate::glyphs::get().arrow_left)
            .style(style)
            .render(
                Rect {
                    x: area.x + locked_width - 1,
                    width: 1,
                    height: 1,
                    ..area
                },
                buf,
            );
    }
    if end < total {
        let mark = more_mark(total - end);
        let width = crate::glyphs::display_width(&mark) as u16;
        if width <= area.width {
            Paragraph::new(mark).style(style).render(
                Rect {
                    x: area.x + area.width - width,
                    width,
                    height: 1,
                    ..area
                },
                buf,
            );
        }
    }
}

fn render_correlation_matrix(
    shown: Option<Shown>,
    table_state: &mut TableState,
    selected_cell: &Option<(usize, usize)>,
    columns: &mut ColumnScroll,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    let (correlation_matrix, method) = match shown {
        Some(Shown { matrix, method }) => (matrix, method),
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
    let cell_width = 12u16; // Wide enough for "-0.999" and most names
    let column_spacing = 1u16; // Table widget adds 1 space between columns

    let available_width = area
        .width
        .saturating_sub(row_header_width)
        .saturating_sub(column_spacing);
    let widths = vec![cell_width; n];
    let (mut start_col, mut end_col) =
        stat_window(&widths, available_width, column_spacing, columns);
    // Scroll to the selected cell, whatever moved it: a key, or a resize.
    if let Some((_, col)) = *selected_cell {
        let col = col.min(n - 1);
        while col < start_col || (col >= end_col && columns.offset < columns.max) {
            columns.offset = if col < start_col {
                col
            } else {
                columns.offset + 1
            };
            (start_col, end_col) = stat_window(&widths, available_width, column_spacing, columns);
        }
    }
    let visible_cols = end_col - start_col;

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
            let correlation = correlation_matrix.coefficient(method, i, col_idx);
            let text_color = get_correlation_color(correlation, theme);

            let cell_text = if i == col_idx {
                "1.000".to_string()
            } else if correlation.is_nan() {
                "-".to_string()
            } else {
                format_coefficient(correlation, 3)
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
        .column_spacing(column_spacing);

    StatefulWidget::render(table, area, buf, table_state);
    draw_scroll_marks(area, buf, row_header_width, (start_col, end_col, n), theme);
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
        theme.get("chip_key")
    } else {
        // Negative correlation - error/warning color
        theme.get("outlier_marker")
    }
}

/// What the family list shows: the column's fits, the family on the plots, and
/// the scale the histogram is drawn in.
struct SelectorConfig<'a> {
    dist: &'a DistributionAnalysis,
    selected: DistributionType,
    histogram_scale: HistogramScale,
    /// Log was asked for on values that cannot take it, so the histogram is linear.
    log_scale_unavailable: bool,
    theme: &'a Theme,
    ctx: &'a RenderContext,
}

/// The families to compare with, one Surface on the right of the detail: each
/// family and its p-value, the one on the plots on the rail, and the scale the
/// histogram is drawn in on the last row.
fn render_distribution_selector(
    config: SelectorConfig,
    selector_state: &mut TableState,
    area: Rect,
    buf: &mut Buffer,
) {
    let SelectorConfig {
        dist,
        selected: selected_dist,
        histogram_scale,
        log_scale_unavailable,
        theme,
        ctx,
    } = config;
    // Tested families by p-value, then the ones that do not apply; the same order
    // the modal's ↑↓ walks.
    let distribution_scores: Vec<(DistributionType, Option<&FitOutcome>)> =
        crate::distribution_fit::listing_order(&dist.fits)
            .into_iter()
            .map(|family| (family, dist.fit(family)))
            .collect();

    let selected_pos = distribution_scores
        .iter()
        .position(|(family, _)| *family == selected_dist)
        .unwrap_or(0);
    // Trust the cursor while it is on the list; place it only when it is unset or
    // has fallen off the end.
    match selector_state.selected() {
        Some(idx) if idx < distribution_scores.len() => {}
        _ => selector_state.select(Some(selected_pos)),
    }
    let selected = selector_state.selected().unwrap_or(0);

    let content = Surface::new("Distribution").render(area, buf, ctx);
    if content.height < 3 || content.width < 8 {
        return;
    }
    let g = crate::glyphs::get();
    // The p-value column is as wide as its widest value, "<0.005" or "n/a".
    const PVALUE_WIDTH: u16 = 7;
    let name_width = content.width.saturating_sub(1 + PVALUE_WIDTH);
    let line = |rail: &str, name: &str, pvalue: &str| {
        format!(
            "{rail}{name:<w$}{pvalue:>p$}",
            name = crate::render::loading_view::truncate(name, name_width as usize),
            w = name_width as usize,
            p = PVALUE_WIDTH as usize,
        )
    };
    let row = |y: u16| Rect {
        y,
        height: 1,
        ..content
    };

    Paragraph::new(line(" ", "Name", "P-value"))
        .style(Style::default().fg(ctx.text_secondary))
        .render(row(content.y), buf);

    // The last row is the scale; the list scrolls in what is between, and counts
    // what it cannot show rather than cutting a family in half.
    let scale_y = content.y + content.height - 1;
    let list_height = (content.height - 2) as usize;
    let total = distribution_scores.len();
    let (offset, shown) = list_window(selected, total, list_height);
    let below = total - offset - shown;
    if below > 0 && shown < list_height {
        Paragraph::new(format!(" {} {below} more", g.ellipsis))
            .style(Style::default().fg(ctx.dimmed))
            .render(row(content.y + 1 + shown as u16), buf);
    }
    for (i, (family, outcome)) in distribution_scores
        .iter()
        .enumerate()
        .skip(offset)
        .take(shown)
    {
        let y = content.y + 1 + (i - offset) as u16;
        // A family that does not apply has no p-value, and says so rather than
        // ranking a placeholder.
        let (p_text, p_style) = match outcome.and_then(|outcome| outcome.test()) {
            Some(test) => (format_fit_pvalue(test), pvalue_style(test.p_value, theme)),
            None => ("n/a".to_string(), Style::default().fg(ctx.dimmed)),
        };
        let is_cursor = i == selected;
        let name = crate::render::loading_view::truncate(&family.to_string(), name_width as usize);
        let spans = vec![
            Span::styled(
                if is_cursor { g.rail } else { " " },
                Style::default().fg(ctx.accent),
            ),
            Span::styled(
                format!("{name:<w$}", w = name_width as usize),
                Style::default().fg(ctx.text_primary),
            ),
            Span::styled(format!("{p_text:>p$}", p = PVALUE_WIDTH as usize), p_style),
        ];
        let mut paragraph = Paragraph::new(Line::from(spans));
        if is_cursor {
            paragraph = paragraph.style(ctx.highlight_style());
        }
        paragraph.render(row(y), buf);
    }

    // Log asked for on values that cannot take it falls back to linear, in the
    // warning color so the fallback is not mistaken for the choice.
    let (scale, scale_style) = match (histogram_scale, log_scale_unavailable) {
        (_, true) => ("Linear", Style::default().fg(ctx.warning)),
        (HistogramScale::Linear, false) => ("Linear", Style::default().fg(ctx.text_primary)),
        (HistogramScale::Log, false) => ("Log", Style::default().fg(ctx.text_primary)),
    };
    if scale_y > content.y + 1 {
        Paragraph::new(Line::from(vec![
            Span::styled(" Scale: ", Style::default().fg(ctx.label)),
            Span::styled(scale, scale_style),
        ]))
        .render(row(scale_y), buf);
    }
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
    /// The column's numbers, on every value axis.
    values: &'a AxisNumbers,
    counts: &'a AxisNumbers,
}

/// The family list's least width: the frame, the rail, "Exponential" and a p-value.
const SELECTOR_WIDTH: u16 = 24;

/// Which of `total` items a list of `rows` shows around the cursor, as the
/// first and how many. While some are out of view below, the last row is kept
/// to count them, so the cursor never sits on it.
fn list_window(selected: usize, total: usize, rows: usize) -> (usize, usize) {
    if total <= rows || rows == 0 {
        return (0, total.min(rows));
    }
    let room = rows.saturating_sub(1).max(1);
    let offset = selected.saturating_sub(room - 1);
    if offset + rows >= total {
        (total - rows, rows)
    } else {
        (offset, room)
    }
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

/// The Analysis Tools list, the same beside every tool: one Surface, the
/// cursor carrying the rail and the tint while the list has focus, and the tool
/// on screen carrying the accent.
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
    // Built here rather than passed: Data Quality draws this list too, from its
    // theme.
    let ctx =
        RenderContext::from_theme_and_config(theme, 0, false, NumberFormatSettings::default());
    let content = Surface::new("Analysis Tools").render(area, buf, &ctx);
    let g = crate::glyphs::get();
    let list_focused = focus == AnalysisFocus::Sidebar;
    for (idx, (name, tool)) in tools.iter().enumerate().take(content.height as usize) {
        let is_cursor = list_focused && sidebar_state.selected() == Some(idx);
        let name_style = if selected_tool == Some(*tool) {
            Style::default()
                .fg(ctx.accent_bright)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_primary)
        };
        let mut line = Paragraph::new(Line::from(vec![
            Span::styled(
                if is_cursor { g.rail } else { " " },
                Style::default().fg(ctx.accent),
            ),
            // Cut with a mark on a narrow screen, never silently.
            Span::styled(
                crate::render::loading_view::truncate(
                    name,
                    content.width.saturating_sub(1) as usize,
                ),
                name_style,
            ),
        ]));
        if is_cursor {
            line = line.style(ctx.highlight_style());
        }
        line.render(
            Rect {
                y: content.y + idx as u16,
                height: 1,
                ..content
            },
            buf,
        );
    }
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
        values,
        counts,
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
    // One blank column between neighboring bars.
    let gap_width = 1u16;

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
    // Even, so the middle label is a whole count.
    let global_max = (max_data.max(max_theory.ceil() as usize).max(1) as f64 / 2.0).ceil() * 2.0;

    // Use the shared label width calculated in the caller
    // This ensures both histogram and Q-Q plot use the same padding for alignment
    let y_axis_label_width = shared_y_axis_label_width;

    // On Log the bins are equal in log space, and so is the x axis: a position is the
    // log of the value it stands for, and a bin's center is its geometric middle.
    let position = |x: f64| if use_log_scale { x.ln() } else { x };
    let bin_centers: Vec<f64> = (0..num_bins)
        .map(|i| {
            let (lo, hi) = (bin_boundaries[i], bin_boundaries[i + 1]);
            if use_log_scale {
                (lo * hi).sqrt()
            } else {
                (lo + hi) / 2.0
            }
        })
        .collect();

    // Each bin's bar on the 0-100 scale the curve and the count labels use. No value
    // or label: the axes say what a bar's height and place mean.
    let data_bars: Vec<Bar> = data_bin_counts
        .iter()
        .map(|&data_count| {
            let data_height = if global_max > 0.0 {
                ((data_count as f64 / global_max) * 100.0) as u64
            } else {
                0
            };
            Bar::default()
                .value(data_height)
                .text_value(String::new())
                .style(Style::default().fg(theme.get("chart_1")))
        })
        .collect();

    // The labels are padded to the width shared with the Q-Q plot, so both plots
    // start in the same column. The bars stand on a 0-100 scale; their labels read
    // counts.
    let label_width = y_axis_label_width as usize;
    let count_axis = AxisSpec::numbers_as([0.0, 100.0], counts, "Counts", move |v| {
        v * global_max / 100.0
    });
    let x_axis = if use_log_scale {
        AxisSpec::numbers_as([log_hist_min.ln(), log_hist_max.ln()], values, "", f64::exp)
    } else {
        AxisSpec::numbers([hist_min, hist_max], values, "")
    };
    let axes = distribution_axes(theme, x_axis, count_axis.padded(label_width), g.plot.line);
    let block = distribution_block(format!("Histogram vs {dist_type}"));
    let chart_area = block.inner(area);

    // Exactly the overlay's plot area: bar `i` starts where bin `i` does. Shifting the
    // bars right to meet the overlay put the first bin's bar over the second bin and
    // drew the last one past the axis, onto whatever sits beside the chart.
    let bar_plot_area = axes.frame(chart_area).graph;

    // Bin `i` takes the plot columns its values map to, as the labels and the curve
    // map them, and its bar fills them less a gap before the next bar. Bars of one
    // shared width stopped short of the right end by up to a bar, leaving each bar
    // left of the values it counts.
    let plot_width = bar_plot_area.width as usize;
    let bin_edge = |i: usize| ((2 * i * plot_width + num_bins) / (2 * num_bins)) as u16;
    let bar_charts: Vec<(Rect, BarChart)> = data_bars
        .into_iter()
        .enumerate()
        .filter_map(|(i, bar)| {
            let (start, end) = (bin_edge(i), bin_edge(i + 1));
            let span = end - start;
            let width = if i + 1 < num_bins && span > gap_width {
                span - gap_width
            } else {
                span
            };
            let rect = Rect {
                x: bar_plot_area.x + start,
                width,
                ..bar_plot_area
            };
            let chart = BarChart::default()
                .data(BarGroup::default().bars(&[bar]))
                // The same 0-100 scale the curve and the labels use; left to itself the
                // chart scales to its tallest bar and the curve no longer measures
                // against the bars.
                .max(100)
                .bar_set(g.plot.column_set())
                .bar_width(width)
                .bar_gap(0);
            (width > 0).then_some((rect, chart))
        })
        .collect();

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
            .map(|(center, count)| (position(*center), height(*count)))
            .collect(),
        None => Vec::new(),
    };

    // Dense points in the line mark read as a continuous curve.
    let marker = g.plot.line;

    let theory_dataset = Dataset::default()
        .name("") // Empty name to prevent legend from appearing
        .marker(marker)
        .graph_type(GraphType::Scatter)
        .style(Style::default().fg(theme.get("dimmed")))
        .data(&theory_points);

    let theory_chart = Chart::new(vec![theory_dataset])
        .hidden_legend_constraints((Constraint::Length(0), Constraint::Length(0)));

    // Render Chart overlay to full area (no borders)
    // Chart widget will automatically handle its own inner layout for x-axis labels
    // The bars, then the chart laid over them from a buffer of its own: its axes,
    // labels and curve, except where the curve crosses a bar. Drawn straight over the
    // bars, each braille cell of the curve replaced a block and cut a notch in the bar;
    // drawn under them, the bar chart's blank cells erased the curve and the axis title.
    for (rect, chart) in bar_charts {
        chart.render(rect, buf);
    }
    let mut overlay = Buffer::empty(area);
    block.render(area, &mut overlay);
    axes.render(theory_chart, chart_area, &mut overlay, g);
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
        values,
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
        Paragraph::new(reason)
            .centered()
            .wrap(ratatui::widgets::Wrap { trim: true })
            .render(area, buf);
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
            .style(Style::default().fg(theme.get("dimmed")))
            .graph_type(GraphType::Line)
            .data(&reference_line),
        // Q-Q plot data points
        Dataset::default()
            .name("") // Empty name to hide from legend
            .marker(marker)
            .style(Style::default().fg(theme.get("chart_1")))
            .graph_type(GraphType::Scatter)
            .data(&qq_data),
    ];

    // Padded to the width shared with the histogram, so both plots start in the same
    // column.
    let label_width = shared_y_axis_label_width as usize;
    let axes = distribution_axes(
        theme,
        AxisSpec::numbers(
            [theory_min_plot, theory_max_plot],
            values,
            "Theoretical Values",
        ),
        AxisSpec::numbers([data_min, data_max], values, "Data Values").padded(label_width),
        marker,
    );
    let block = distribution_block(format!("Q-Q Plot vs {dist_type}"));
    let chart_area = block.inner(area);
    block.render(area, buf);
    let chart = Chart::new(datasets)
        .hidden_legend_constraints((Constraint::Length(0), Constraint::Length(0)));
    axes.render(chart, chart_area, buf, g);
}

/// A Distribution plot's frame: its title centered above, a cell of air on the left.
fn distribution_block<'a>(title: String) -> Block<'a> {
    Block::default()
        .title(title)
        .title_style(ratatui::style::Style::reset())
        .title_alignment(ratatui::layout::Alignment::Center)
        .padding(ratatui::widgets::Padding::left(1))
}

fn distribution_axes<'a>(
    theme: &Theme,
    x: AxisSpec<'a>,
    y: AxisSpec<'a>,
    marker: ratatui::symbols::Marker,
) -> PlotAxes<'a> {
    let secondary = Style::default().fg(theme.get("text_secondary"));
    PlotAxes {
        titles: Style::default(),
        ..PlotAxes::new(x, y, secondary, marker)
    }
}

/// The detail's key figures, label and value: the fit found, Shapiro-Francia,
/// skew, kurtosis, median, mean, std and CV.
fn condensed_statistics(dist: &DistributionAnalysis) -> Vec<(&'static str, String)> {
    let chars = &dist.characteristics;
    // What the values were found to fit, before anything about the family the plots
    // compare them with: choosing a family below is a comparison, not a finding.
    let mut figures = vec![(
        "Fit",
        match dist.distribution_type {
            DistributionType::Unknown | DistributionType::Constant => {
                dist.distribution_type.to_string()
            }
            family => format!("{family} (p {})", verdict_pvalue(dist)),
        },
    )];
    if let (Some(sw_stat), Some(sw_p)) = (chars.shapiro_wilk_stat, chars.shapiro_wilk_pvalue) {
        figures.push((
            "SF",
            if sw_p < 0.001 {
                format!("{sw_stat:.3} (p<0.001)")
            } else {
                format!("{sw_stat:.3} (p={sw_p:.3})")
            },
        ));
    }
    figures.extend([
        ("Skew", format!("{:.2}", chars.skewness)),
        ("Kurt", format!("{:.2}", chars.kurtosis)),
        ("Median", format!("{:.2}", dist.percentiles.p50)),
        ("Mean", format!("{:.2}", chars.mean)),
        ("Std", format!("{:.2}", chars.std_dev)),
        ("CV", format!("{:.3}", chars.coefficient_of_variation)),
    ]);
    figures
}

/// The figures packed into lines of `width`, never splitting a label from its
/// value, so a narrow terminal wraps them rather than cutting the last ones off.
fn condensed_statistics_lines(
    figures: &[(&'static str, String)],
    width: u16,
    theme: &Theme,
) -> Vec<Line<'static>> {
    let style = Style::default().fg(theme.get("text_primary"));
    let mut lines = Vec::new();
    let mut spans = Vec::new();
    let mut used = 0usize;
    for (label, value) in figures {
        let figure = format!("{label}: {value}");
        let w = crate::glyphs::display_width(&figure);
        if used > 0 && used + 1 + w > width as usize {
            lines.push(Line::from(std::mem::take(&mut spans)));
            used = 0;
        }
        if used > 0 {
            spans.push(Span::styled(" ", style));
            used += 1;
        }
        spans.push(Span::styled(figure, style));
        used += w;
    }
    if !spans.is_empty() {
        lines.push(Line::from(spans));
    }
    lines
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
        render_distribution_plot_in(dist, g, render, 60)
    }

    fn render_distribution_plot_in(
        dist: &DistributionAnalysis,
        g: &crate::glyphs::Glyphs,
        render: fn(DistributionPlotConfig, &mut Buffer),
        width: u16,
    ) -> Buffer {
        let numbers = NumberFormatSettings::default();
        render_distribution_plot_as(dist, g, render, width, &numbers)
    }

    /// The plot with its numbers in `numbers`.
    fn render_distribution_plot_as(
        dist: &DistributionAnalysis,
        g: &crate::glyphs::Glyphs,
        render: fn(DistributionPlotConfig, &mut Buffer),
        width: u16,
        numbers: &NumberFormatSettings,
    ) -> Buffer {
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let mut buf = Buffer::empty(Rect::new(0, 0, 80, 20));
        let values = AxisNumbers::measure(numbers, &dist.column_name);
        let counts = AxisNumbers::count(numbers);
        render(
            DistributionPlotConfig {
                dist,
                dist_type: DistributionType::Normal,
                area: Rect::new(0, 0, width, 20),
                shared_y_axis_label_width: 5,
                theme: &theme,
                unified_x_range: Some((23.0, 341.1)),
                histogram_scale: HistogramScale::Linear,
                glyphs: g,
                values: &values,
                counts: &counts,
            },
            &mut buf,
        );
        buf
    }

    /// The histogram's counts group as the table groups numbers: a bin of thousands
    /// reads `9,600`, not `9600`.
    #[test]
    fn distribution_counts_follow_the_table_number_format() {
        let mut dist = skewed_normal_fit();
        let values = dist.sorted_sample_values.clone();
        dist.sorted_sample_values = values
            .iter()
            .cycle()
            .take(values.len() * 24)
            .copied()
            .collect();
        dist.sorted_sample_values.sort_by(f64::total_cmp);
        let labels = |numbers: &NumberFormatSettings| -> Vec<String> {
            let g = crate::glyphs::unicode();
            let buf =
                render_distribution_plot_as(&dist, g, render_distribution_histogram, 60, numbers);
            (0..20)
                .filter_map(|y| {
                    let row: String = (0..60).map(|x| buf[(x, y)].symbol()).collect();
                    let label = row.split_once(['│', '┤'])?.0.trim().to_string();
                    (!label.is_empty()).then_some(label)
                })
                .collect()
        };
        let grouped = labels(&settings("thousands", true));
        assert_eq!(grouped.len(), 3, "{grouped:?}");
        assert!(
            grouped[0].contains(',') && grouped[0].len() > 4,
            "{grouped:?}"
        );
        let plain = labels(&settings("thousands", false));
        assert_eq!(plain[0], grouped[0].replace(',', ""), "{plain:?}");
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

    /// The bars span the plot, first column to last, each bin's bar over the columns
    /// its values map to: bars of one width stopped up to a bar short of the right
    /// end, every bar left of the values the labels give.
    #[test]
    fn histogram_bars_span_the_plot() {
        let g = crate::glyphs::unicode();
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let numbers = NumberFormatSettings::default();
        // Spread evenly over the axis, linear or logarithmic, so every bin has a bar.
        let linear: Vec<f64> = (0..500).map(|i| 23.0 + 318.1 * i as f64 / 499.0).collect();
        let log: Vec<f64> = (0..500)
            .map(|i| 10f64.powf(4.0 * i as f64 / 499.0))
            .collect();
        for (scale, values, range) in [
            (HistogramScale::Linear, linear, (23.0, 341.1)),
            (HistogramScale::Log, log, (1.0, 10_000.0)),
        ] {
            let dist = analysis(100.0, 80.0, values);
            for width in [60u16, 80, 120] {
                // Wider than the plot, to catch a bar drawn past it.
                let mut buf = Buffer::empty(Rect::new(0, 0, width + 10, 20));
                render_distribution_histogram(
                    DistributionPlotConfig {
                        dist: &dist,
                        dist_type: DistributionType::Normal,
                        area: Rect::new(0, 0, width, 20),
                        shared_y_axis_label_width: 5,
                        theme: &theme,
                        unified_x_range: Some(range),
                        histogram_scale: scale,
                        glyphs: g,
                        values: &AxisNumbers::measure(&numbers, &dist.column_name),
                        counts: &AxisNumbers::count(&numbers),
                    },
                    &mut buf,
                );
                let text = rendered_text(&buf);
                let what = format!("{scale:?} at {width}:\n{text}");
                let axis_row = (0..20)
                    .rfind(|y| (0..width).any(|x| buf[(x, *y)].symbol() == g.plot.axis.bottom_left))
                    .expect(&what);
                let corner = (0..width)
                    .find(|x| buf[(*x, axis_row)].symbol() == g.plot.axis.bottom_left)
                    .unwrap();
                let (left, right) = (corner + 1, width - 1);
                assert!(
                    [g.plot.axis.horizontal, g.plot.tick_x]
                        .contains(&buf[(right, axis_row)].symbol()),
                    "{what}"
                );
                let is_bar = |x: u16| {
                    (0..axis_row).any(|y| g.plot.column_eighths.contains(&buf[(x, y)].symbol()))
                };
                let bars: Vec<u16> = (0..width + 10).filter(|x| is_bar(*x)).collect();
                assert_eq!(
                    bars.first(),
                    Some(&left),
                    "the first bar starts the plot\n{what}"
                );
                assert_eq!(
                    bars.last(),
                    Some(&right),
                    "the last bar ends the plot\n{what}"
                );
                // Each bar and the gap after it, or the last bar alone, take an even share
                // of the plot.
                let mut starts = vec![left];
                starts.extend(bars.windows(2).filter(|w| w[1] > w[0] + 1).map(|w| w[1]));
                let shares: Vec<u16> = starts
                    .windows(2)
                    .map(|w| w[1] - w[0])
                    .chain([right + 1 - starts[starts.len() - 1]])
                    .collect();
                let (least, most) = (shares.iter().min().unwrap(), shares.iter().max().unwrap());
                assert!(most - least <= 1, "{shares:?}\n{what}");
            }
        }
    }

    /// On Log the bins are equal in log space and the labels sit where their values
    /// do: over 1 to 10,000 the middle of the axis is 100, not the linear 5,000.
    #[test]
    fn log_histogram_labels_sit_at_their_values() {
        let values: Vec<f64> = (0..400)
            .map(|i| 10f64.powf(4.0 * i as f64 / 399.0))
            .collect();
        let dist = analysis(1_000.0, 2_000.0, values);
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let numbers = NumberFormatSettings::default();
        let g = crate::glyphs::unicode();
        let mut buf = Buffer::empty(Rect::new(0, 0, 80, 20));
        render_distribution_histogram(
            DistributionPlotConfig {
                dist: &dist,
                dist_type: DistributionType::Normal,
                area: Rect::new(0, 0, 80, 20),
                shared_y_axis_label_width: 5,
                theme: &theme,
                unified_x_range: Some((1.0, 10_000.0)),
                histogram_scale: HistogramScale::Log,
                glyphs: g,
                values: &AxisNumbers::measure(&numbers, &dist.column_name),
                counts: &AxisNumbers::count(&numbers),
            },
            &mut buf,
        );
        let text = rendered_text(&buf);
        let rows: Vec<&str> = text.lines().collect();
        let axis = rows
            .iter()
            .rposition(|r| r.contains(g.plot.axis.bottom_left))
            .expect(&text);
        let row = rows[axis + 1];
        let labels: Vec<(usize, f64)> = row
            .split_whitespace()
            .map(|l| {
                let at = row.find(l).unwrap() + l.len() / 2;
                (at, l.replace(',', "").parse::<f64>().expect(&text))
            })
            .collect();
        assert_eq!(labels.len(), 3, "{text}");
        let [(left, first), (at, middle), (right, last)] = labels[..] else {
            unreachable!()
        };
        assert_eq!((first, middle, last), (1.0, 100.0, 10_000.0), "{text}");
        assert!(
            at.abs_diff((left + right) / 2) <= 1,
            "the middle label is at the axis's middle:\n{text}"
        );
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

    /// The Distribution plots follow the rule every chart does: x labels a space
    /// apart with both ends kept, and axis titles on rows that hold nothing else.
    #[test]
    fn distribution_axes_follow_the_chart_rule() {
        let mut dist = skewed_normal_fit();
        let qq: Vec<f64> = (0..dist.sorted_sample_values.len())
            .map(|i| 23.0 + 318.0 * i as f64 / 499.0)
            .collect();
        dist.qq = vec![(DistributionType::Normal, qq)];
        let is_number = |t: &str| t.trim_end_matches(['k', 'M']).parse::<f64>().is_ok();
        for width in [40, 60, 80] {
            for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
                for (name, render, y_title, x_title) in [
                    (
                        "histogram",
                        render_distribution_histogram as fn(DistributionPlotConfig, &mut Buffer),
                        "Counts",
                        None,
                    ),
                    (
                        "Q-Q plot",
                        render_qq_plot,
                        "Data Values",
                        Some("Theoretical Values"),
                    ),
                ] {
                    let text = rendered_text(&render_distribution_plot_in(&dist, g, render, width));
                    let rows: Vec<&str> = text.lines().collect();
                    let what = format!("{name} at {width}:\n{text}");
                    let axis = rows
                        .iter()
                        .rposition(|r| r.contains(g.plot.axis.bottom_left))
                        .expect(&what);
                    let labels: Vec<&str> = rows[axis + 1].split_whitespace().collect();
                    assert!(labels.len() >= 2, "both ends: {what}");
                    assert!(labels.iter().all(|l| is_number(l)), "apart: {what}");
                    // The plot's title, then the y axis's on a row of its own.
                    assert_eq!(rows[1].trim(), y_title, "{what}");
                    if let Some(x_title) = x_title {
                        assert_eq!(rows[axis + 2].trim(), x_title, "{what}");
                    }
                }
            }
        }
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
            rank_correlations: vec![vec![1.0, 0.5], vec![0.5, 1.0]],
            rank_p_values: Some(vec![vec![0.0, 0.03], vec![0.03, 0.0]]),
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

    /// The furthest scroll is the first that shows the last statistic: one short
    /// of it leaves the last out, and nothing scrolls past it.
    #[test]
    fn the_statistics_scroll_stops_where_the_last_comes_into_view() {
        let widths = [5, 5, 6, 3, 10, 6, 6, 6, 6];
        for available in [12u16, 20, 30, 45, 80] {
            let mut columns = ColumnScroll {
                offset: usize::MAX,
                max: 0,
            };
            let (start, end) = stat_window(&widths, available, 2, &mut columns);
            assert_eq!(start, columns.max, "clamped to the furthest start");
            assert_eq!(end, widths.len(), "the last is in view at {available}");
            if columns.max > 0 {
                columns.offset = columns.max - 1;
                let (_, end) = stat_window(&widths, available, 2, &mut columns);
                assert!(end < widths.len(), "one short leaves it out at {available}");
            }
        }
        let mut columns = ColumnScroll::default();
        assert_eq!(stat_window(&widths, 200, 2, &mut columns), (0, 9));
        assert_eq!(columns.max, 0, "everything fits, so nothing scrolls");
    }

    /// The family list keeps its cursor in view and, while families are out of
    /// view below, a row to count them: the cursor never takes that row.
    #[test]
    fn the_family_list_counts_what_is_below_the_cursor() {
        assert_eq!(list_window(3, 5, 8), (0, 5), "everything fits");
        for selected in 0..14 {
            let (offset, shown) = list_window(selected, 14, 12);
            assert!(
                (offset..offset + shown).contains(&selected),
                "{selected} is drawn"
            );
            let below = 14 - offset - shown;
            if below > 0 {
                assert_eq!(shown, 11, "a row is left to count {below} at {selected}");
            } else {
                assert_eq!(shown, 12);
            }
        }
        assert_eq!(list_window(11, 14, 12), (1, 11), "not the last row");
        assert_eq!(list_window(13, 14, 12), (2, 12), "the end needs no count");
        assert_eq!(list_window(4, 14, 1), (4, 1), "one row is the cursor's");
    }

    /// The matrix scrolls to the selected cell however it got there, and counts
    /// the columns it cannot show.
    #[test]
    fn the_correlation_matrix_keeps_the_selected_column_in_view() {
        let names: Vec<String> = (0..6).map(|i| format!("col_{i}")).collect();
        let n = names.len();
        let matrix = crate::statistics::CorrelationMatrix {
            columns: names,
            correlations: vec![vec![0.5; n]; n],
            p_values: None,
            sample_sizes: vec![vec![10; n]; n],
            rank_correlations: vec![vec![0.5; n]; n],
            rank_p_values: None,
        };
        let results = AnalysisResults {
            column_statistics: vec![],
            total_rows: 10,
            sample_size: None,
            per_value: None,
            sample_seed: 0,
            correlation_matrix: Some(matrix),
            distribution_analyses: vec![],
        };
        let theme = Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let area = Rect::new(0, 0, 60, 10);
        let mut columns = ColumnScroll::default();
        let mut state = TableState::default();
        let mut header = |selected: (usize, usize), columns: &mut ColumnScroll| {
            let mut buf = Buffer::empty(area);
            state.select(Some(selected.0));
            render_correlation_matrix(
                results.correlation_matrix.as_ref().map(|matrix| Shown {
                    matrix,
                    method: CorrelationMethod::Pearson,
                }),
                &mut state,
                &Some(selected),
                columns,
                area,
                &mut buf,
                &theme,
            );
            rendered_text(&buf).lines().next().unwrap().to_string()
        };
        let first = header((0, 0), &mut columns);
        assert!(
            first.contains("col_0") && !first.contains("col_5"),
            "{first:?}"
        );
        assert!(
            first.contains('+'),
            "the hidden columns are counted: {first:?}"
        );
        let last = header((0, 5), &mut columns);
        assert!(
            last.contains("col_5"),
            "the selected column is drawn: {last:?}"
        );
        let back = header((0, 0), &mut columns);
        assert!(
            back.contains("col_0"),
            "and so is the first again: {back:?}"
        );
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
        StatisticsTable {
            results: &results,
            focused: false,
            theme: &theme,
            table_cell_padding: 1,
            number_format: &settings("thousands", false),
        }
        .render(
            area,
            &mut buf,
            &mut TableState::default(),
            &mut crate::analysis_modal::ColumnScroll::default(),
        );
        let text = rendered_text(&buf);
        let mut lines = text.lines();
        let header = lines.next().unwrap();
        let pickup = lines
            .find(|l| l.trim_start().starts_with("pickup"))
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
            Shown {
                matrix: &correlation_matrix(0.874, 42),
                method: CorrelationMethod::Pearson,
            },
            (0, 1),
            50,
            area,
            &mut buf,
            &theme,
            &settings("thousands", false),
        );
        let text = rendered_text(&buf);
        assert!(text.contains("Pearson r: 0.8740"), "{text}");
        assert!(text.contains("strong positive"), "{text}");
        let r_squared = crate::glyphs::get().r_squared;
        assert!(text.contains(&format!("{r_squared}: 0.7639")), "{text}");
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
            Shown {
                matrix: &correlation_matrix(f64::NAN, 2),
                method: CorrelationMethod::Pearson,
            },
            (0, 1),
            50,
            area,
            &mut buf,
            &theme,
            &settings("thousands", false),
        );
        let text = rendered_text(&buf);
        assert!(text.contains("Fewer than 3 overlapping pairs"), "{text}");
        assert!(!text.contains("Pearson r:"), "{text}");
    }

    #[test]
    fn correlation_detail_shows_the_chosen_method() {
        let theme =
            crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
        let area = Rect::new(0, 0, 60, 8);
        let mut buf = Buffer::empty(area);
        render_correlation_pair_summary(
            Shown {
                matrix: &correlation_matrix(0.874, 42),
                method: CorrelationMethod::Spearman,
            },
            (0, 1),
            50,
            area,
            &mut buf,
            &theme,
            &settings("thousands", false),
        );
        let text = rendered_text(&buf);
        let rho = crate::glyphs::get().rho;
        assert!(text.contains(&format!("Spearman {rho}: 0.5000")), "{text}");
        assert!(text.contains("P-value: 0.03"), "{text}");
    }

    /// Rounding never shows a perfect relation that is not one.
    #[test]
    fn a_coefficient_rounds_to_one_only_when_it_is_one() {
        assert_eq!(format_coefficient(0.9996, 3), "0.999");
        assert_eq!(format_coefficient(-0.9996, 3), "-0.999");
        assert_eq!(format_coefficient(0.99996, 4), "0.9999");
        assert_eq!(format_coefficient(0.9994, 3), "0.999");
        assert_eq!(format_coefficient(1.0, 3), "1.000");
        assert_eq!(format_coefficient(-1.0, 4), "-1.0000");
        assert_eq!(format_coefficient(0.12345, 3), "0.123");
        assert_eq!(format_coefficient(-0.5, 3), "-0.500");
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
