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

use crate::analysis::analysis_modal::{
    AnalysisFocus, AnalysisTool, AnalysisView, ColumnScroll, HistogramScale,
};
use crate::analysis::distribution_fit::{FitOutcome, FitTest};
use crate::analysis::statistics::{
    AnalysisResults, CategoricalStatistics, ColumnStatistics, CorrelationMethod,
    DistributionAnalysis, DistributionType, HistogramKey, NumericStatistics, TemporalStatistics,
};
use crate::config::Theme;
use crate::glyphs::PlotMarks;
use crate::numfmt::{self, NumberFormatSettings};
use crate::render::context::RenderContext;
use crate::widgets::axes::{AxisSpec, PlotAxes};
use crate::widgets::axis_numbers::{AxisFormat, AxisNumbers};
use crate::widgets::ui::Surface;
use polars::prelude::{AnyValue, DataType};

pub struct AnalysisWidgetConfig<'a> {
    pub results: Option<&'a AnalysisResults>,
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
    pub sample: &'a crate::analysis::sampling::Sample,
    pub ctx: &'a RenderContext,
}

pub struct AnalysisWidget<'a> {
    results: Option<&'a AnalysisResults>,
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
    sample: &'a crate::analysis::sampling::Sample,
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
            results: config.results,
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

        // What the numbers are of: a sample says its size, of how many, and which rows.
        let breadcrumb_text = match self.results {
            Some(results) if self.selected_tool.is_some() => format!(
                "{tool_name} {} {}",
                crate::glyphs::get().middot,
                self.sample
                    .outcome(results.total_rows, results.sample_size, results.per_value)
            ),
            _ => tool_name,
        };

        let header_row_style = header_style(self.theme.controls_bg(), self.theme.table_header());
        Paragraph::new(breadcrumb_text)
            .style(header_row_style)
            .render(layout[0], buf);

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
                    .style(Style::default().fg(self.theme.text_primary()))
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
                                MatrixCursor {
                                    cell: self.selected_correlation,
                                    focused: self.focus == AnalysisFocus::Main,
                                },
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

            // The breadcrumb carries the name alone; the footer says Esc.
            let title_text = format!("Distribution Analysis: {}", dist.column_name);
            let header_row_style =
                header_style(self.theme.controls_bg(), self.theme.table_header());
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

            // Both plots share a y-label width so they start in the same column.
            let (lo, hi) = unified_x_range;
            let qq_format = AxisFormat::ends_and_middle([lo, hi], &values);
            let qq_width = [lo, (lo + hi) / 2.0, hi]
                .iter()
                .filter_map(|&v| qq_format.label(v, 0))
                .map(|l| crate::glyphs::display_width(&l))
                .max()
                .unwrap_or(1);
            let n = sorted_data.len() as f64;
            let count_width = AxisFormat::new(&[0.0, n], &counts)
                .label(n, 0)
                .map_or(1, |l| crate::glyphs::display_width(&l));
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

        // The breadcrumb carries the pair alone; the footer says Esc.
        let title_text = format!(
            "Correlation: {} vs {}",
            matrix.columns[row], matrix.columns[col]
        );
        let header_row_style = header_style(self.theme.controls_bg(), self.theme.table_header());
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
    matrix: &'a crate::analysis::statistics::CorrelationMatrix,
    method: CorrelationMethod,
}

/// Why a matrix has no Spearman: the rows read hold more values than it ranks.
const SPEARMAN_TOO_MANY: &str = "Too many values to rank for Spearman; s chooses a smaller sample";

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

/// The correlation pair detail: what the matrix knows of the pair (no scatter or
/// per-column moments: the results lack the values).
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

    let label_style = Style::default().fg(theme.text_secondary());
    let value_style = Style::default().fg(theme.text_primary());

    let mut lines: Vec<Line> = Vec::new();
    if r.is_nan() {
        let unranked = method == CorrelationMethod::Spearman && matrix.rank_correlations.is_none();
        let why = if unranked {
            SPEARMAN_TOO_MANY
        } else if pairs < 3 {
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

        // Column widths from header names and content; ratatui's Table adds one space between
        // columns.
        let mut min_col_widths: Vec<u16> = stat_display_names
            .iter()
            .map(|name| crate::glyphs::display_width(name) as u16) // header length (no extra padding - table handles spacing)
            .collect();

        // Scan all data to find maximum width needed for each column
        for col_stat in &results.column_statistics {
            for (stat_idx, stat_name) in stat_names.iter().enumerate() {
                let value_str = describe_value(col_stat, stat_name, number_format);
                let value_len = crate::glyphs::display_width(&value_str) as u16;
                // Ensure width is at least the header length (already initialized) AND value length
                // This preserves header widths even if all data values are shorter
                let header_len = crate::glyphs::display_width(stat_display_names[stat_idx]) as u16;
                min_col_widths[stat_idx] = min_col_widths[stat_idx].max(value_len).max(header_len);
                // must fit both header and content (no padding - table handles spacing)
            }
        }

        // Locked column width (column name) - calculate from header text AND actual column names
        let header_text = "Column";
        let header_len = crate::glyphs::display_width(header_text) as u16;
        let max_col_name_len = results
            .column_statistics
            .iter()
            .map(|cs| crate::glyphs::display_width(&cs.name) as u16)
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
        let header_row_style = header_style(theme.controls_bg(), theme.table_header());
        let header_row = Row::new(header_cells.clone()).style(header_row_style);

        for col_stat in &results.column_statistics {
            let mut cells = vec![
                Cell::from(col_stat.name.as_str()).style(Style::default().fg(theme.text_primary())),
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

/// A row or null count, grouped as the data table is; float statistics use
/// `format_num`, which switches to scientific notation first.
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
        Style::default().fg(theme.distribution_normal())
    } else if p > 0.01 {
        Style::default().fg(theme.distribution_skewed())
    } else {
        Style::default().fg(theme.outlier_marker())
    }
}

/// A header's style: `bg` behind `fg`, or `fg` alone when `bg` is the terminal's own.
pub(crate) fn header_style(bg: Color, fg: Color) -> Style {
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
        .map(|name| crate::glyphs::display_width(name) as u16) // header length (no extra padding - table handles spacing)
        .collect();

    let header_text = "Column";
    let header_len = crate::glyphs::display_width(header_text) as u16;
    let max_col_name_len = results
        .distribution_analyses
        .iter()
        .map(|da| crate::glyphs::display_width(&da.column_name) as u16)
        .max()
        .unwrap_or(header_len);
    let locked_col_width = max_col_name_len.max(header_len).max(10);

    // Each row's values, formatted once for the widths and the cells both.
    let texts: Vec<[String; 8]> = results
        .distribution_analyses
        .iter()
        .map(stat_texts)
        .collect();
    for row in &texts {
        for (idx, value) in row.iter().enumerate() {
            min_col_widths[idx] =
                min_col_widths[idx].max(crate::glyphs::display_width(value) as u16);
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
    let header_row_style = header_style(theme.controls_bg(), theme.table_header());
    let header_row = Row::new(header_cells).style(header_row_style);
    for (dist_analysis, texts) in results.distribution_analyses.iter().zip(texts) {
        // The verdict in the colors of its p-value; no clear fit in the rejected one.
        let type_color = match dist_analysis.distribution_type {
            DistributionType::Unknown => theme.outlier_marker(),
            DistributionType::Constant => theme.text_primary(),
            _ => pvalue_style(dist_analysis.confidence, theme)
                .fg
                .unwrap_or_else(|| theme.text_primary()),
        };

        // Relaxed outlier color thresholds - red only for very high percentages that might indicate data errors
        let outlier_style = if dist_analysis.outliers.percentage > 20.0 {
            // Red: very high outlier percentage (>20%) - might indicate data errors
            Style::default().fg(theme.outlier_marker())
        } else if dist_analysis.outliers.percentage > 5.0 {
            // Yellow for moderate outliers (5-20%)
            Style::default().fg(theme.distribution_skewed())
        } else {
            // Default (white) for low outlier percentages (0-5%)
            Style::default()
        };

        let skewness_value = dist_analysis.characteristics.skewness.abs();
        let kurtosis_value = dist_analysis.characteristics.kurtosis;

        // Skewness color coding: similar to describe table
        let skewness_style = if skewness_value >= 3.0 {
            Style::default().fg(theme.outlier_marker())
        } else if skewness_value >= 1.0 {
            Style::default().fg(theme.distribution_skewed())
        } else {
            Style::default()
        };

        // Kurtosis color coding: 3.0 is normal, high/low is notable
        let kurtosis_style = if (kurtosis_value - 3.0).abs() >= 3.0 {
            Style::default().fg(theme.outlier_marker())
        } else if (kurtosis_value - 3.0).abs() >= 1.0 {
            Style::default().fg(theme.distribution_skewed())
        } else {
            Style::default()
        };

        let pvalue_style = pvalue_style(dist_analysis.confidence, theme);

        // Color coding for SW p-value: same semantics as p-value column
        // Green = normal (>0.05), Yellow = moderate (0.01-0.05), Red = non-normal (≤0.01)
        let sw_pvalue_style = dist_analysis
            .characteristics
            .shapiro_wilk_pvalue
            .map(|p| {
                if p > 0.05 {
                    Style::default().fg(theme.distribution_normal())
                } else if p > 0.01 {
                    Style::default().fg(theme.distribution_skewed())
                } else {
                    Style::default().fg(theme.outlier_marker())
                }
            })
            .unwrap_or_default();

        // Build row with locked column name + visible stat values
        // Use explicit text_primary so column names stay visible (avoids black-on-black)
        let mut cells = vec![
            Cell::from(dist_analysis.column_name.as_str())
                .style(Style::default().fg(theme.text_primary())),
        ];

        let cv_style = if dist_analysis.characteristics.coefficient_of_variation > 1.0 {
            // High variability.
            Style::default().fg(theme.distribution_skewed())
        } else {
            Style::default()
        };
        let styles = [
            Style::default().fg(type_color),
            pvalue_style,
            Style::default(),
            sw_pvalue_style,
            cv_style,
            outlier_style,
            skewness_style,
            kurtosis_style,
        ];
        let mut texts = texts.map(Some);
        for &stat_idx in &visible_stats {
            let text = texts[stat_idx].take().unwrap_or_default();
            cells.push(Cell::from(text).style(styles[stat_idx]));
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

/// One distribution's values in the table's column order, after its name.
fn stat_texts(dist_analysis: &DistributionAnalysis) -> [String; 8] {
    let characteristics = &dist_analysis.characteristics;
    let outliers = if dist_analysis.outliers.total_count > 0 {
        format!(
            "{} ({:.1}%)",
            dist_analysis.outliers.total_count, dist_analysis.outliers.percentage
        )
    } else {
        "0 (0.0%)".to_string()
    };
    [
        dist_analysis.distribution_type.to_string(),
        verdict_pvalue(dist_analysis),
        characteristics
            .shapiro_wilk_stat
            .map(|s| format!("{:.3}", s))
            .unwrap_or_else(|| "N/A".to_string()),
        characteristics
            .shapiro_wilk_pvalue
            .map(format_pvalue)
            .unwrap_or_else(|| "N/A".to_string()),
        format!("{:.4}", characteristics.coefficient_of_variation),
        outliers,
        format_num(characteristics.skewness),
        format_num(characteristics.kurtosis),
    ]
}

/// The column the cursor's rail sits in, kept whether or not the table has focus
/// so focus arriving moves nothing.
const RAIL_WIDTH: u16 = 1;

/// The rail beside the cursor's row: accented while the table has focus, dimmed while
/// the tool list does (a tint alone vanishes on 16 colors).
fn cursor_rail(focused: bool, theme: &Theme) -> Span<'static> {
    Span::styled(crate::glyphs::get().rail, rail_style(focused, theme))
}

/// The rail's color: the accent with focus, dimmed without.
pub(crate) fn rail_style(focused: bool, theme: &Theme) -> Style {
    Style::default().fg(if focused {
        theme.accent()
    } else {
        theme.dimmed()
    })
}

/// The row the cursor is on: the tint while the table has focus; without it,
/// only the dimmed rail marks it.
fn cursor_style(focused: bool, theme: &Theme) -> Style {
    if focused {
        theme.highlight_style()
    } else {
        Style::default()
    }
}

/// The mark that counts statistics hidden to the right, as the data table's does.
fn more_mark(hidden: usize) -> String {
    format!(" +{hidden} {}", crate::glyphs::get().arrow_right)
}

/// Which statistics fit beside the locked name column, as `start..end` from the scroll
/// offset. Sets the scroll's `max` to the first start showing the last statistic and
/// clamps to it, so a press past the end does nothing and the first press back moves.
/// Leaves room for the count mark when statistics are cut off right.
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

/// Mark statistics out of view: an arrow at the locked column header's end for the
/// left, the count at the header's right edge for the right.
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
    let style = header_style(theme.controls_bg(), theme.accent()).add_modifier(Modifier::BOLD);
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

/// The matrix's cell cursor, and whether the matrix has the focus.
struct MatrixCursor {
    cell: Option<(usize, usize)>,
    focused: bool,
}

fn render_correlation_matrix(
    shown: Option<Shown>,
    table_state: &mut TableState,
    cursor: MatrixCursor,
    columns: &mut ColumnScroll,
    area: Rect,
    buf: &mut Buffer,
    theme: &Theme,
) {
    let MatrixCursor {
        cell: selected_cell,
        focused,
    } = cursor;
    let (correlation_matrix, method) = match shown {
        Some(Shown { matrix, method }) => (matrix, method),
        None => {
            Paragraph::new("No correlation matrix available (need at least 2 numeric columns)")
                .centered()
                .render(area, buf);
            return;
        }
    };

    if method == CorrelationMethod::Spearman && correlation_matrix.rank_correlations.is_none() {
        Paragraph::new(SPEARMAN_TOO_MANY)
            .centered()
            .render(area, buf);
        return;
    }

    if correlation_matrix.columns.is_empty() {
        Paragraph::new("No numeric columns for correlation matrix")
            .centered()
            .render(area, buf);
        return;
    }

    let n = correlation_matrix.columns.len();

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
    if let Some((_, col)) = selected_cell {
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

    let header_row_style = header_style(theme.controls_bg(), theme.table_header());
    let dim_header_style = header_style(theme.controls_bg(), theme.table_header());

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
        let is_selected_row = selected_cell.is_some() && i == selected_row;

        // Row header cell - dim highlight if selected row
        let row_header_style = if is_selected_row {
            Style::default().bg(theme.surface())
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

            let cell_style = if is_selected_cell && focused {
                // The cell cursor, as the table draws its own.
                Style::default()
                    .fg(text_color)
                    .patch(theme.cell_cursor_style())
            } else if is_selected_cell {
                // The matrix without focus: its cell stays marked, quietly.
                Style::default()
                    .fg(text_color)
                    .patch(theme.column_cursor_style())
                    .add_modifier(Modifier::BOLD | Modifier::UNDERLINED)
            } else if is_selected_row || is_in_selected_col {
                // Selected row or column: dim background with colored text
                Style::default().fg(text_color).bg(theme.surface())
            } else {
                // Normal cell: just text color
                Style::default().fg(text_color)
            };

            cells.push(Cell::from(cell_text).style(cell_style));
        }

        let row_style = if is_selected_row {
            Style::default().bg(theme.surface())
        } else {
            Style::default()
        };

        rows.push(Row::new(cells).style(row_style));
    }

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
        theme.dimmed()
    } else if abs_corr < 0.3 {
        // Low correlation - normal text
        theme.text_primary()
    } else if correlation > 0.0 {
        // Positive correlation - keybind hints color (UI element, not chart)
        theme.chip_key()
    } else {
        // Negative correlation - error/warning color
        theme.outlier_marker()
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

/// The families to compare with, one Surface right of the detail: each family and its
/// p-value, the plotted one on the rail, and the histogram scale on the last row.
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
        crate::analysis::distribution_fit::listing_order(&dist.fits)
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
            name = crate::glyphs::fit(name, name_width as usize),
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
        let name = crate::glyphs::fit(&family.to_string(), name_width as usize);
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

/// Which of `total` items `rows` rows show around the cursor (first, count). With items
/// below, the last row counts them and the cursor never sits there.
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

/// The Analysis Tools list, the same beside every tool: the cursor's rail and tint while
/// focused, the tool on screen accented.
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
        let on_screen = selected_tool == Some(*tool);
        // The tool on screen is bold; the accent is the cursor's alone.
        let name_style = if on_screen {
            Style::default()
                .fg(ctx.text_primary)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(ctx.text_primary)
        };
        // The tool on screen keeps a dimmed rail while its pane has the focus.
        let rail = if is_cursor || (on_screen && !list_focused) {
            g.rail
        } else {
            " "
        };
        let mut line = Paragraph::new(Line::from(vec![
            Span::styled(rail, rail_style(is_cursor, theme)),
            // Cut with a mark on a narrow screen, never silently.
            Span::styled(
                crate::glyphs::fit(name, content.width.saturating_sub(1) as usize),
                name_style,
            ),
        ]));
        if is_cursor {
            line = line.style(ctx.highlight_style());
        }
        let row = Rect {
            y: content.y + idx as u16,
            height: 1,
            ..content
        };
        line.render(row, buf);
        crate::app::pointer::record(row, crate::app::pointer::Hit::Tool(idx));
    }
}

fn render_distribution_histogram(config: DistributionPlotConfig, buf: &mut Buffer) {
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

    // The range the Q-Q plot shares, so both plots read the same values at the same
    // column.
    let (hist_min, hist_max) = unified_x_range.unwrap_or((data_min, data_max));

    // Calculate dynamic number of bins based on available width
    // This ensures bars fill the horizontal space and look dense at all widths

    let y_axis_gap = 1u16; // Minimal gap between labels and plot area (needed to prevent bars from extending outside)
    let total_y_axis_space = shared_y_axis_label_width + y_axis_gap;

    // Bar width must match the Chart's plot area exactly: minus its y-axis labels and the
    // axis line.
    let available_width = area.width.saturating_sub(total_y_axis_space + 1);
    // One blank column between neighboring bars.
    let gap_width = 1u16;

    // Aim for ~7-cell bars: num_bins = (available + gap) / (bar + gap).
    let target_bar_width = 7.0; // Target bar width in pixels
    let optimal_num_bins = ((available_width as f64 + gap_width as f64)
        / (target_bar_width + gap_width as f64)) as usize;

    // Between 5 and 60 bins (more for ultrawide displays).
    let num_bins = optimal_num_bins.clamp(5, 60);

    // Log-scale bins when chosen and the data (not the padded range) is positive.
    let all_data_positive = data_min > 0.0;
    // For log scale, ensure hist_min is positive (adjust if needed)
    let (log_hist_min, log_hist_max) =
        if matches!(histogram_scale, HistogramScale::Log) && all_data_positive {
            // Use actual data min/max for log scale to avoid issues with padding or theoretical bounds
            let actual_min = sorted_data[0];
            let actual_max = sorted_data[sorted_data.len() - 1];
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

    // The fit's expected counts are drawn behind the bars, sampled densely enough
    // that braille renders them as a line.
    let histogram = dist.histogram(HistogramKey {
        family: dist_type,
        bins: num_bins,
        log: use_log_scale,
        range: if use_log_scale {
            (log_hist_min, log_hist_max)
        } else {
            (hist_min, hist_max)
        },
        samples: (available_width as usize * 15).clamp(1500, 10000),
    });
    let global_max = histogram.top;

    // Use the shared label width calculated in the caller
    // This ensures both histogram and Q-Q plot use the same padding for alignment
    let y_axis_label_width = shared_y_axis_label_width;

    // Each bin's bar on the 0-100 scale the curve and the count labels use. No value
    // or label: the axes say what a bar's height and place mean.
    let data_bars: Vec<Bar> = histogram
        .counts
        .iter()
        .map(|&data_count| {
            let data_height = ((data_count as f64 / global_max) * 100.0) as u64;
            Bar::default()
                .value(data_height)
                .text_value(String::new())
                .style(Style::default().fg(theme.chart_1()))
        })
        .collect();

    // Labels padded to the width shared with the Q-Q plot; bars stand on a 0-100 scale,
    // labeled in counts.
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

    // Exactly the overlay's plot area: bar `i` starts where bin `i` does.
    let bar_plot_area = axes.frame(chart_area).graph;

    // Bin `i` spans the plot columns its values map to (as labels and curve map them),
    // less a gap, so bars reach the right end.
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
                // The curve's 0-100 scale, not the tallest bar's, so the curve measures against bars.
                .max(100)
                .bar_set(g.plot.column_set())
                .bar_width(width)
                .bar_gap(0);
            (width > 0).then_some((rect, chart))
        })
        .collect();

    // Dense points in the line mark read as a continuous curve.
    let marker = g.plot.line;

    let theory_dataset = Dataset::default()
        .name("") // Empty name to prevent legend from appearing
        .marker(marker)
        .graph_type(GraphType::Scatter)
        .style(Style::default().fg(theme.dimmed()))
        .data(&histogram.curve);

    let theory_chart = Chart::new(vec![theory_dataset])
        .hidden_legend_constraints((Constraint::Length(0), Constraint::Length(0)));

    // The bars, then the chart overlaid from its own buffer except where the curve
    // crosses a bar: drawn directly, braille cells would notch bars; drawn under, blank
    // bar cells would erase the curve and title.
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

    // Axis ranges: X theoretical (inverse CDF of percentiles), Y the sorted sample as is.
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
            .style(Style::default().fg(theme.dimmed()))
            .graph_type(GraphType::Line)
            .data(&reference_line),
        // Q-Q plot data points
        Dataset::default()
            .name("") // Empty name to hide from legend
            .marker(marker)
            .style(Style::default().fg(theme.chart_1()))
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
    let secondary = Style::default().fg(theme.text_secondary());
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
    let style = Style::default().fg(theme.text_primary());
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
mod tests;
