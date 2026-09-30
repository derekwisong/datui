//! Chart export to PNG (plotters bitmap) and EPS (minimal PostScript, no deps).

use color_eyre::Result;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::chart_data::{
    AxisFormat, AxisNumbers, Bar, BarData, BoxPlotData, HeatmapData, XAxisTemporalKind,
    x_axis_label_at,
};
use crate::chart_modal::ChartType;
use crate::numfmt::NumberFormat;

/// Escape a string for PostScript ( and ) and \.
fn ps_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('(', "\\(")
        .replace(')', "\\)")
}

/// The chart's notes on its input (a sample, values a range left out), small and gray
/// under the plot, right-aligned to its edge at `right`: an exported chart says what
/// the chart view says.
fn write_eps_notes(f: &mut File, notes: &[String], right: f64) -> Result<()> {
    if notes.is_empty() {
        return Ok(());
    }
    writeln!(f, "0.4 setgray")?;
    writeln!(f, "/Helvetica findfont 7 scalefont setfont")?;
    for (i, note) in notes.iter().rev().enumerate() {
        writeln!(
            f,
            "({}) dup stringwidth pop {} exch sub {} moveto show",
            ps_escape(note),
            right,
            2.0 + i as f64 * 8.0
        )?;
    }
    writeln!(f, "0 setgray")?;
    Ok(())
}

/// [`write_eps_notes`] for a PNG: in the bottom margin, right-aligned to the plot.
fn draw_png_notes(
    root: &plotters::drawing::DrawingArea<
        plotters::prelude::BitMapBackend<'_>,
        plotters::coord::Shift,
    >,
    notes: &[String],
    margin: i32,
) -> Result<()> {
    use plotters::prelude::*;
    use plotters::style::text_anchor::{HPos, Pos, VPos};
    let (width, height) = root.dim_in_pixel();
    let style = ("sans-serif", 13)
        .into_font()
        .color(&RGBColor(100, 100, 100))
        .pos(Pos::new(HPos::Right, VPos::Bottom));
    for (i, note) in notes.iter().rev().enumerate() {
        root.draw(&Text::new(
            note.as_str(),
            (width as i32 - margin, height as i32 - 4 - i as i32 * 14),
            style.clone(),
        ))?;
    }
    Ok(())
}

/// Generate "nice" tick values in [min, max] with roughly max_ticks steps.
fn nice_ticks(min: f64, max: f64, max_ticks: usize) -> Vec<f64> {
    let range = if max > min { max - min } else { 1.0 };
    if range <= 0.0 || max_ticks == 0 {
        return vec![min];
    }
    let raw_step = range / (max_ticks as f64).max(1.0);
    let mag = 10.0_f64.powf(raw_step.log10().floor());
    let norm = if mag > 0.0 { raw_step / mag } else { raw_step };
    let step = if norm <= 1.0 {
        1.0 * mag
    } else if norm <= 2.0 {
        2.0 * mag
    } else if norm <= 5.0 {
        5.0 * mag
    } else {
        10.0 * mag
    };
    let step = step.max(f64::EPSILON);
    let start = (min / step).floor() * step;
    let mut ticks = Vec::new();
    let mut v = start;
    while v <= max + step * 0.001 {
        if v >= min - step * 0.001 {
            ticks.push(v);
        }
        v += step;
        if ticks.len() > max_ticks + 2 {
            break;
        }
    }
    if ticks.is_empty() {
        ticks.push(min);
    }
    ticks
}

/// Bounds and options for rendering the chart to a file.
pub struct ChartExportBounds {
    pub x_min: f64,
    pub x_max: f64,
    pub y_min: f64,
    pub y_max: f64,
    /// X-axis column name (for axis title).
    pub x_label: String,
    /// Y-axis column name(s), e.g. "col" or "a, b" (for axis title).
    pub y_label: String,
    /// How to format x-axis tick labels (date/datetime/time vs numeric).
    pub x_axis_kind: XAxisTemporalKind,
    /// If true, y values in data/bounds are ln(1+y); y-axis labels must be shown in linear space (exp_m1).
    pub log_scale: bool,
    /// Optional chart title shown on export. None or empty = no title.
    pub chart_title: Option<String>,
    /// What the chart says under the plot about its input (`chart_data::chart_notes`).
    pub notes: Vec<String>,
    /// What the x axis holds, so its ticks print as the table prints the column.
    pub x_numbers: AxisNumbers,
    /// The same for the y axis: counts, or the y columns.
    pub y_numbers: AxisNumbers,
}

impl ChartExportBounds {
    /// The x axis, ticked at `ticks`.
    fn x_axis(&self, ticks: &[f64]) -> TickLabels {
        TickLabels::new(ticks, &self.x_numbers, false, self.x_axis_kind)
    }

    /// The y axis, ticked at `ticks`: in linear space on a log scale.
    fn y_axis(&self, ticks: &[f64]) -> TickLabels {
        TickLabels::new(
            ticks,
            &self.y_numbers,
            self.log_scale,
            XAxisTemporalKind::Numeric,
        )
    }

    fn x_whole(&self) -> bool {
        self.x_numbers.whole
    }

    fn y_whole(&self) -> bool {
        self.y_numbers.whole && !self.log_scale
    }
}

/// Bounds and options for rendering a box plot export.
pub struct BoxPlotExportBounds {
    pub y_min: f64,
    pub y_max: f64,
    pub x_labels: Vec<String>,
    pub x_label: String,
    pub y_label: String,
    pub chart_title: Option<String>,
    pub notes: Vec<String>,
    /// What the columns hold, so the y ticks print as the table prints them.
    pub y_numbers: AxisNumbers,
}

/// One series: name and (x, y) points (y already log-transformed if log scale).
pub struct ChartExportSeries {
    pub name: String,
    pub points: Vec<(f64, f64)>,
    /// Where a line starts again after a gap (see `chart_data::segments`).
    pub breaks: Vec<usize>,
}

/// Export format for chart: PNG or EPS.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartExportFormat {
    Png,
    Eps,
}

impl ChartExportFormat {
    pub const ALL: [Self; 2] = [Self::Png, Self::Eps];

    pub fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Eps => "eps",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Png => "PNG",
            Self::Eps => "EPS",
        }
    }
}

/// Write chart to EPS (Encapsulated PostScript). No external dependencies.
pub fn write_chart_eps(
    path: &Path,
    series: &[ChartExportSeries],
    chart_type: ChartType,
    bounds: &ChartExportBounds,
) -> Result<()> {
    if series.is_empty() || series.iter().all(|s| s.points.is_empty()) {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    const W: f64 = 400.0;
    const H: f64 = 300.0;
    const MARGIN_LEFT: f64 = 50.0;
    const MARGIN_BOTTOM: f64 = 40.0;
    const PLOT_W: f64 = W - MARGIN_LEFT - 40.0;
    const PLOT_H: f64 = H - MARGIN_BOTTOM - 30.0;

    let x_min = bounds.x_min;
    let x_max = bounds.x_max;
    let y_min = bounds.y_min;
    let y_max = bounds.y_max;
    let x_range = if x_max > x_min { x_max - x_min } else { 1.0 };
    let y_range = if y_max > y_min { y_max - y_min } else { 1.0 };

    let to_x = |x: f64| MARGIN_LEFT + (x - x_min) / x_range * PLOT_W;
    let to_y = |y: f64| MARGIN_BOTTOM + (y - y_min) / y_range * PLOT_H;

    let mut f = File::create(path)?;

    writeln!(f, "%!PS-Adobe-3.0 EPSF-3.0")?;
    writeln!(
        f,
        "%%BoundingBox: 0 0 {} {}",
        W.ceil() as i32,
        H.ceil() as i32
    )?;
    writeln!(f, "%%Creator: datui")?;
    writeln!(f, "%%EndComments")?;
    writeln!(f, "gsave")?;
    writeln!(f, "1 setlinewidth")?;

    // Optional chart title at top center
    if let Some(ref title) = bounds.chart_title
        && !title.is_empty()
    {
        const CHAR_W: f64 = 6.0;
        writeln!(f, "/Helvetica findfont 12 scalefont setfont")?;
        let title_w = title.len() as f64 * CHAR_W;
        let tx = (W / 2.0 - title_w / 2.0).max(4.0).min(W - title_w - 4.0);
        writeln!(f, "{} {} moveto ({}) show", tx, H - 15.0, ps_escape(title))?;
        writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    }

    // Tick positions for grid, ticks, and labels
    const MAX_TICKS: usize = 8;
    let x_axis = bounds.x_axis(&nice_ticks(x_min, x_max, MAX_TICKS));
    let y_axis = bounds.y_axis(&nice_ticks(y_min, y_max, MAX_TICKS));
    let x_ticks = x_axis.ticks();
    let y_ticks = y_axis.ticks();

    // Grid (light gray, behind plot)
    writeln!(f, "0.9 setgray")?;
    writeln!(f, "0.5 setlinewidth")?;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            writeln!(
                f,
                "{} {} moveto 0 {} rlineto stroke",
                px, MARGIN_BOTTOM, PLOT_H
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            writeln!(
                f,
                "{} {} moveto {} 0 rlineto stroke",
                MARGIN_LEFT, py, PLOT_W
            )?;
        }
    }
    writeln!(f, "1 setlinewidth")?;
    writeln!(f, "0 setgray")?;

    // Axis box
    writeln!(f, "{} {} moveto", MARGIN_LEFT, MARGIN_BOTTOM)?;
    writeln!(f, "{} 0 rlineto", PLOT_W)?;
    writeln!(f, "0 {} rlineto", PLOT_H)?;
    writeln!(f, "{} 0 rlineto", -PLOT_W)?;
    writeln!(f, "closepath stroke")?;

    // Tick marks (short lines on axes)
    const TICK_LEN: f64 = 4.0;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            writeln!(
                f,
                "{} {} moveto 0 {} rlineto stroke",
                px, MARGIN_BOTTOM, -TICK_LEN
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            writeln!(
                f,
                "{} {} moveto {} 0 rlineto stroke",
                MARGIN_LEFT, py, -TICK_LEN
            )?;
        }
    }

    // Tick labels and axis titles (text)
    writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    let char_w: f64 = 5.0;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            let s = x_axis.label(v).unwrap_or_default();
            let label_w = s.len() as f64 * char_w;
            let tx = (px - label_w / 2.0)
                .max(MARGIN_LEFT)
                .min(MARGIN_LEFT + PLOT_W - label_w);
            writeln!(
                f,
                "{} {} moveto ({}) show",
                tx,
                MARGIN_BOTTOM - 12.0,
                ps_escape(&s)
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            let s = y_axis.label(v).unwrap_or_default();
            let label_w = s.len() as f64 * char_w;
            let tx = (MARGIN_LEFT - label_w - 4.0).max(2.0);
            writeln!(f, "{} {} moveto ({}) show", tx, py - 3.0, ps_escape(&s))?;
        }
    }

    // Axis titles (x_label below tick labels, y_label left of plot)
    writeln!(f, "/Helvetica findfont 10 scalefont setfont")?;
    let x_label = &bounds.x_label;
    let y_label = &bounds.y_label;
    if !x_label.is_empty() {
        let x_center = MARGIN_LEFT + PLOT_W / 2.0;
        let x_str_approx_len = x_label.len() as f64 * char_w;
        writeln!(
            f,
            "{} {} moveto ({}) show",
            (x_center - x_str_approx_len / 2.0).max(MARGIN_LEFT),
            MARGIN_BOTTOM - 24.0,
            ps_escape(x_label)
        )?;
    }
    if !y_label.is_empty() {
        writeln!(f, "gsave")?;
        writeln!(
            f,
            "12 {} translate -90 rotate",
            MARGIN_BOTTOM + PLOT_H / 2.0
        )?;
        let y_str_approx_len = y_label.len() as f64 * char_w;
        writeln!(
            f,
            "{} 0 moveto ({}) show",
            -y_str_approx_len / 2.0,
            ps_escape(y_label)
        )?;
        writeln!(f, "grestore")?;
    }

    // Fixed palette (RGB 0–1)
    let palette: [(f64, f64, f64); 7] = [
        (0.0, 0.7, 0.9), // cyan
        (0.9, 0.0, 0.5), // magenta
        (0.0, 0.7, 0.0), // green
        (0.9, 0.8, 0.0), // yellow
        (0.0, 0.0, 0.9), // blue
        (0.9, 0.0, 0.0), // red
        (0.5, 0.9, 0.9), // light cyan
    ];

    for (idx, s) in series.iter().enumerate() {
        if s.points.is_empty() {
            continue;
        }
        let (r, g, b) = palette[idx % palette.len()];
        writeln!(f, "{} {} {} setrgbcolor", r, g, b)?;

        match chart_type {
            ChartType::Line => {
                for segment in crate::chart_data::segments(&s.points, &s.breaks) {
                    let (px, py) = segment[0];
                    writeln!(f, "{} {} moveto", to_x(px), to_y(py))?;
                    for &(px, py) in &segment[1..] {
                        writeln!(f, "{} {} lineto", to_x(px), to_y(py))?;
                    }
                    writeln!(f, "stroke")?;
                }
            }
            ChartType::Scatter => {
                let rad = 3.0;
                for &(px, py) in &s.points {
                    writeln!(f, "{} {} {} 0 360 arc fill", to_x(px), to_y(py), rad)?;
                }
            }
            ChartType::Bar => {
                let n = s.points.len() as f64;
                let bar_w = (PLOT_W / n).clamp(1.0, 20.0) * 0.7;
                for &(px, py) in &s.points {
                    let cx = to_x(px) - bar_w / 2.0;
                    let cy = to_y(0.0_f64.max(y_min));
                    let h = to_y(py) - cy;
                    writeln!(f, "{} {} {} {} rectfill", cx, cy, bar_w, h)?;
                }
            }
        }
    }

    write_eps_notes(&mut f, &bounds.notes, MARGIN_LEFT + PLOT_W)?;
    writeln!(f, "grestore")?;
    writeln!(f, "%%EOF")?;
    f.sync_all()?;
    Ok(())
}

/// Write chart to PNG using plotters bitmap backend. Size is (width, height) in pixels.
pub fn write_chart_png(
    path: &Path,
    series: &[ChartExportSeries],
    chart_type: ChartType,
    bounds: &ChartExportBounds,
    (width, height): (u32, u32),
) -> Result<()> {
    use plotters::prelude::*;

    if series.is_empty() || series.iter().all(|s| s.points.is_empty()) {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    let root = BitMapBackend::new(path, (width, height)).into_drawing_area();
    root.fill(&WHITE)?;

    let x_min = bounds.x_min;
    let x_max = bounds.x_max;
    let y_min = bounds.y_min;
    let y_max = bounds.y_max;

    let mut binding = ChartBuilder::on(&root);
    let builder = binding.margin(30);
    let builder = if let Some(t) = bounds.chart_title.as_ref().filter(|s| !s.is_empty()) {
        builder.caption(t.as_str(), ("sans-serif", 20))
    } else {
        builder
    };
    let mut chart = builder
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(x_min..x_max, y_min..y_max)?;

    let x_count = tick_count(x_min, x_max, bounds.x_whole());
    let y_count = tick_count(y_min, y_max, bounds.y_whole());
    let x_axis = bounds.x_axis(&png_ticks(x_min, x_max, x_count));
    let y_axis = bounds.y_axis(&png_ticks(y_min, y_max, y_count));
    let x_formatter = |v: &f64| x_axis.label(*v).unwrap_or_default();
    let y_formatter = |v: &f64| y_axis.label(*v).unwrap_or_default();
    chart
        .configure_mesh()
        .x_labels(x_count)
        .y_labels(y_count)
        .x_desc(bounds.x_label.as_str())
        .y_desc(bounds.y_label.as_str())
        .x_label_formatter(&x_formatter)
        .y_label_formatter(&y_formatter)
        .draw()?;

    let colors = [
        CYAN,
        MAGENTA,
        GREEN,
        YELLOW,
        BLUE,
        RED,
        RGBColor(128, 255, 255),
    ];

    for (idx, s) in series.iter().enumerate() {
        if s.points.is_empty() {
            continue;
        }
        let color = colors[idx % colors.len()];
        match chart_type {
            ChartType::Line => {
                // One line per run between gaps; the legend names the first.
                let segments = crate::chart_data::segments(&s.points, &s.breaks);
                for (i, segment) in segments.into_iter().enumerate() {
                    let drawn =
                        chart.draw_series(LineSeries::new(segment.iter().copied(), color))?;
                    if i == 0 {
                        drawn.label(s.name.as_str()).legend(move |(x, y)| {
                            PathElement::new(vec![(x, y), (x + 20, y)], color)
                        });
                    }
                }
            }
            ChartType::Scatter => {
                chart.draw_series(PointSeries::of_element(
                    s.points.iter().copied(),
                    3,
                    color,
                    &|c, s, _| EmptyElement::at(c) + Circle::new((0, 0), s, color.filled()),
                ))?;
            }
            ChartType::Bar => {
                chart.draw_series(s.points.iter().map(|&(x, y)| {
                    let x0 = x - 0.3;
                    let x1 = x + 0.3;
                    Rectangle::new([(x0, 0.0), (x1, y)], color.filled())
                }))?;
            }
        }
    }

    chart
        .configure_series_labels()
        .background_style(WHITE.mix(0.8))
        .border_style(BLACK)
        .draw()?;

    draw_png_notes(&root, &bounds.notes, 30)?;
    root.present()?;
    Ok(())
}

/// Write box plot to PNG using plotters bitmap backend. Size is (width, height) in pixels.
pub fn write_box_plot_png(
    path: &Path,
    data: &BoxPlotData,
    bounds: &BoxPlotExportBounds,
    (width, height): (u32, u32),
) -> Result<()> {
    use plotters::prelude::*;

    if data.stats.is_empty() {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    let root = BitMapBackend::new(path, (width, height)).into_drawing_area();
    root.fill(&WHITE)?;

    let x_min = -0.5;
    let x_max = (data.stats.len() as f64 - 1.0).max(0.0) + 0.5;
    let mut binding = ChartBuilder::on(&root);
    let builder = binding.margin(30);
    let builder = if let Some(t) = bounds.chart_title.as_ref().filter(|s| !s.is_empty()) {
        builder.caption(t.as_str(), ("sans-serif", 20))
    } else {
        builder
    };
    let mut chart = builder
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(x_min..x_max, bounds.y_min..bounds.y_max)?;

    let labels = bounds.x_labels.clone();
    let label_span = (x_max - x_min).max(f64::EPSILON);
    let y_count = tick_count(bounds.y_min, bounds.y_max, bounds.y_numbers.whole);
    let y_axis = TickLabels::numbers(
        &png_ticks(bounds.y_min, bounds.y_max, y_count),
        &bounds.y_numbers,
    );
    chart
        .configure_mesh()
        .x_labels(labels.len())
        .y_labels(y_count)
        .y_label_formatter(&|v: &f64| y_axis.label(*v).unwrap_or_default())
        .x_desc(bounds.x_label.as_str())
        .y_desc(bounds.y_label.as_str())
        .x_label_formatter(&move |v: &f64| {
            let label_count = labels.len().saturating_sub(1) as f64;
            let idx = if label_count > 0.0 {
                ((v - x_min) / label_span * label_count).round() as isize
            } else {
                0
            };
            if idx >= 0 && (idx as usize) < labels.len() {
                labels[idx as usize].clone()
            } else {
                String::new()
            }
        })
        .draw()?;

    let colors = [
        CYAN,
        MAGENTA,
        GREEN,
        YELLOW,
        BLUE,
        RED,
        RGBColor(128, 255, 255),
    ];
    let box_half = 0.3;
    let cap_half = 0.2;

    for (idx, stat) in data.stats.iter().enumerate() {
        let x = idx as f64;
        let color = colors[idx % colors.len()];
        let outline = ShapeStyle::from(&color).stroke_width(1);
        chart.draw_series(std::iter::once(Rectangle::new(
            [(x - box_half, stat.q1), (x + box_half, stat.q3)],
            outline,
        )))?;
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(x - box_half, stat.median), (x + box_half, stat.median)],
            color,
        )))?;
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(x, stat.min), (x, stat.q1)],
            color,
        )))?;
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(x, stat.q3), (x, stat.max)],
            color,
        )))?;
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(x - cap_half, stat.min), (x + cap_half, stat.min)],
            color,
        )))?;
        chart.draw_series(std::iter::once(PathElement::new(
            vec![(x - cap_half, stat.max), (x + cap_half, stat.max)],
            color,
        )))?;
    }

    draw_png_notes(&root, &bounds.notes, 30)?;
    root.present()?;
    Ok(())
}

/// Write heatmap to PNG using plotters bitmap backend. Size is (width, height) in pixels.
pub fn write_heatmap_png(
    path: &Path,
    data: &HeatmapData,
    bounds: &ChartExportBounds,
    (width, height): (u32, u32),
) -> Result<()> {
    use plotters::prelude::*;

    if data.counts.is_empty() || data.max_count <= 0.0 {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    let root = BitMapBackend::new(path, (width, height)).into_drawing_area();
    root.fill(&WHITE)?;

    let mut binding = ChartBuilder::on(&root);
    let builder = binding.margin(30);
    let builder = if let Some(t) = bounds.chart_title.as_ref().filter(|s| !s.is_empty()) {
        builder.caption(t.as_str(), ("sans-serif", 20))
    } else {
        builder
    };
    let mut chart = builder
        .x_label_area_size(40)
        .y_label_area_size(50)
        .build_cartesian_2d(bounds.x_min..bounds.x_max, bounds.y_min..bounds.y_max)?;

    let x_step = (bounds.x_max - bounds.x_min) / data.x_bins.max(1) as f64;
    let y_step = (bounds.y_max - bounds.y_min) / data.y_bins.max(1) as f64;
    for y in 0..data.y_bins {
        for x in 0..data.x_bins {
            let count = data.counts[y][x];
            let intensity = (count / data.max_count).clamp(0.0, 1.0);
            let shade = (255.0 * (1.0 - intensity)) as u8;
            let color = RGBColor(shade, shade, 255);
            let x0 = bounds.x_min + x as f64 * x_step;
            let x1 = x0 + x_step;
            let y0 = bounds.y_min + y as f64 * y_step;
            let y1 = y0 + y_step;
            chart.draw_series(std::iter::once(Rectangle::new(
                [(x0, y0), (x1, y1)],
                color.filled(),
            )))?;
        }
    }

    let x_count = tick_count(bounds.x_min, bounds.x_max, bounds.x_whole());
    let y_count = tick_count(bounds.y_min, bounds.y_max, bounds.y_whole());
    let x_axis = bounds.x_axis(&png_ticks(bounds.x_min, bounds.x_max, x_count));
    let y_axis = bounds.y_axis(&png_ticks(bounds.y_min, bounds.y_max, y_count));
    chart
        .configure_mesh()
        .x_desc(bounds.x_label.as_str())
        .y_desc(bounds.y_label.as_str())
        .x_labels(x_count)
        .y_labels(y_count)
        .x_label_formatter(&|v| x_axis.label(*v).unwrap_or_default())
        .y_label_formatter(&|v| y_axis.label(*v).unwrap_or_default())
        .draw()?;

    draw_png_notes(&root, &bounds.notes, 30)?;
    root.present()?;
    Ok(())
}

/// Write box plot to EPS (Encapsulated PostScript). No external dependencies.
pub fn write_box_plot_eps(
    path: &Path,
    data: &BoxPlotData,
    bounds: &BoxPlotExportBounds,
) -> Result<()> {
    if data.stats.is_empty() {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    const W: f64 = 400.0;
    const H: f64 = 300.0;
    const MARGIN_LEFT: f64 = 50.0;
    const MARGIN_BOTTOM: f64 = 40.0;
    const PLOT_W: f64 = W - MARGIN_LEFT - 40.0;
    const PLOT_H: f64 = H - MARGIN_BOTTOM - 30.0;

    let x_min = -0.5;
    let x_max = (data.stats.len() as f64 - 1.0).max(0.0) + 0.5;
    let y_min = bounds.y_min;
    let y_max = bounds.y_max;
    let x_range = if x_max > x_min { x_max - x_min } else { 1.0 };
    let y_range = if y_max > y_min { y_max - y_min } else { 1.0 };

    let to_x = |x: f64| MARGIN_LEFT + (x - x_min) / x_range * PLOT_W;
    let to_y = |y: f64| MARGIN_BOTTOM + (y - y_min) / y_range * PLOT_H;

    let mut f = File::create(path)?;
    writeln!(f, "%!PS-Adobe-3.0 EPSF-3.0")?;
    writeln!(
        f,
        "%%BoundingBox: 0 0 {} {}",
        W.ceil() as i32,
        H.ceil() as i32
    )?;
    writeln!(f, "%%Creator: datui")?;
    writeln!(f, "%%EndComments")?;
    writeln!(f, "gsave")?;
    writeln!(f, "1 setlinewidth")?;

    if let Some(ref title) = bounds.chart_title
        && !title.is_empty()
    {
        const CHAR_W: f64 = 6.0;
        writeln!(f, "/Helvetica findfont 12 scalefont setfont")?;
        let title_w = title.len() as f64 * CHAR_W;
        let tx = (W / 2.0 - title_w / 2.0).max(4.0).min(W - title_w - 4.0);
        writeln!(f, "{} {} moveto ({}) show", tx, H - 15.0, ps_escape(title))?;
        writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    }

    const MAX_TICKS: usize = 8;
    let y_axis = TickLabels::numbers(&nice_ticks(y_min, y_max, MAX_TICKS), &bounds.y_numbers);
    let y_ticks = y_axis.ticks();
    let x_ticks: Vec<f64> = (0..data.stats.len()).map(|i| i as f64).collect();

    writeln!(f, "0.9 setgray")?;
    writeln!(f, "0.5 setlinewidth")?;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            writeln!(
                f,
                "{} {} moveto 0 {} rlineto stroke",
                px, MARGIN_BOTTOM, PLOT_H
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            writeln!(
                f,
                "{} {} moveto {} 0 rlineto stroke",
                MARGIN_LEFT, py, PLOT_W
            )?;
        }
    }
    writeln!(f, "1 setlinewidth")?;
    writeln!(f, "0 setgray")?;

    writeln!(f, "{} {} moveto", MARGIN_LEFT, MARGIN_BOTTOM)?;
    writeln!(f, "{} 0 rlineto", PLOT_W)?;
    writeln!(f, "0 {} rlineto", PLOT_H)?;
    writeln!(f, "{} 0 rlineto", -PLOT_W)?;
    writeln!(f, "closepath stroke")?;

    const TICK_LEN: f64 = 4.0;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            writeln!(
                f,
                "{} {} moveto 0 {} rlineto stroke",
                px, MARGIN_BOTTOM, -TICK_LEN
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            writeln!(
                f,
                "{} {} moveto {} 0 rlineto stroke",
                MARGIN_LEFT, py, -TICK_LEN
            )?;
        }
    }

    writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    let char_w: f64 = 5.0;
    for (i, &v) in x_ticks.iter().enumerate() {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            let label = bounds.x_labels.get(i).map(|s| s.as_str()).unwrap_or("");
            let label_w = label.len() as f64 * char_w;
            let tx = (px - label_w / 2.0)
                .max(MARGIN_LEFT)
                .min(MARGIN_LEFT + PLOT_W - label_w);
            writeln!(
                f,
                "{} {} moveto ({}) show",
                tx,
                MARGIN_BOTTOM - 12.0,
                ps_escape(label)
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            let s = y_axis.label(v).unwrap_or_default();
            let label_w = s.len() as f64 * char_w;
            let tx = (MARGIN_LEFT - label_w - 4.0).max(2.0);
            writeln!(f, "{} {} moveto ({}) show", tx, py - 3.0, ps_escape(&s))?;
        }
    }

    writeln!(f, "/Helvetica findfont 10 scalefont setfont")?;
    if !bounds.x_label.is_empty() {
        let x_center = MARGIN_LEFT + PLOT_W / 2.0;
        let x_str_approx_len = bounds.x_label.len() as f64 * char_w;
        writeln!(
            f,
            "{} {} moveto ({}) show",
            (x_center - x_str_approx_len / 2.0).max(MARGIN_LEFT),
            MARGIN_BOTTOM - 24.0,
            ps_escape(&bounds.x_label)
        )?;
    }
    if !bounds.y_label.is_empty() {
        writeln!(f, "gsave")?;
        writeln!(
            f,
            "12 {} translate -90 rotate",
            MARGIN_BOTTOM + PLOT_H / 2.0
        )?;
        let y_str_approx_len = bounds.y_label.len() as f64 * char_w;
        writeln!(
            f,
            "{} 0 moveto ({}) show",
            -y_str_approx_len / 2.0,
            ps_escape(&bounds.y_label)
        )?;
        writeln!(f, "grestore")?;
    }

    let palette: [(f64, f64, f64); 7] = [
        (0.0, 0.7, 0.9),
        (0.9, 0.0, 0.5),
        (0.0, 0.7, 0.0),
        (0.9, 0.8, 0.0),
        (0.0, 0.0, 0.9),
        (0.9, 0.0, 0.0),
        (0.5, 0.9, 0.9),
    ];
    let box_half = 0.3;
    let cap_half = 0.2;

    for (idx, stat) in data.stats.iter().enumerate() {
        let (r, g, b) = palette[idx % palette.len()];
        writeln!(f, "{} {} {} setrgbcolor", r, g, b)?;
        let x = idx as f64;
        let x_left = to_x(x - box_half);
        let x_right = to_x(x + box_half);
        let y_q1 = to_y(stat.q1);
        let y_q3 = to_y(stat.q3);
        writeln!(f, "{} {} moveto", x_left, y_q1)?;
        writeln!(f, "{} {} lineto", x_right, y_q1)?;
        writeln!(f, "{} {} lineto", x_right, y_q3)?;
        writeln!(f, "{} {} lineto", x_left, y_q3)?;
        writeln!(f, "closepath stroke")?;
        writeln!(f, "{} {} moveto", x_left, to_y(stat.median))?;
        writeln!(f, "{} {} lineto stroke", x_right, to_y(stat.median))?;
        writeln!(f, "{} {} moveto", to_x(x), to_y(stat.min))?;
        writeln!(f, "{} {} lineto stroke", to_x(x), to_y(stat.q1))?;
        writeln!(f, "{} {} moveto", to_x(x), to_y(stat.q3))?;
        writeln!(f, "{} {} lineto stroke", to_x(x), to_y(stat.max))?;
        writeln!(f, "{} {} moveto", to_x(x - cap_half), to_y(stat.min))?;
        writeln!(f, "{} {} lineto stroke", to_x(x + cap_half), to_y(stat.min))?;
        writeln!(f, "{} {} moveto", to_x(x - cap_half), to_y(stat.max))?;
        writeln!(f, "{} {} lineto stroke", to_x(x + cap_half), to_y(stat.max))?;
    }

    write_eps_notes(&mut f, &bounds.notes, MARGIN_LEFT + PLOT_W)?;
    writeln!(f, "grestore")?;
    writeln!(f, "%%EOF")?;
    f.sync_all()?;
    Ok(())
}

/// Write heatmap to EPS (Encapsulated PostScript). No external dependencies.
pub fn write_heatmap_eps(
    path: &Path,
    data: &HeatmapData,
    bounds: &ChartExportBounds,
) -> Result<()> {
    if data.counts.is_empty() || data.max_count <= 0.0 {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    const W: f64 = 400.0;
    const H: f64 = 300.0;
    const MARGIN_LEFT: f64 = 50.0;
    const MARGIN_BOTTOM: f64 = 40.0;
    const PLOT_W: f64 = W - MARGIN_LEFT - 40.0;
    const PLOT_H: f64 = H - MARGIN_BOTTOM - 30.0;

    let x_min = bounds.x_min;
    let x_max = bounds.x_max;
    let y_min = bounds.y_min;
    let y_max = bounds.y_max;
    let x_range = if x_max > x_min { x_max - x_min } else { 1.0 };
    let y_range = if y_max > y_min { y_max - y_min } else { 1.0 };
    let to_x = |x: f64| MARGIN_LEFT + (x - x_min) / x_range * PLOT_W;
    let to_y = |y: f64| MARGIN_BOTTOM + (y - y_min) / y_range * PLOT_H;

    let mut f = File::create(path)?;
    writeln!(f, "%!PS-Adobe-3.0 EPSF-3.0")?;
    writeln!(
        f,
        "%%BoundingBox: 0 0 {} {}",
        W.ceil() as i32,
        H.ceil() as i32
    )?;
    writeln!(f, "%%Creator: datui")?;
    writeln!(f, "%%EndComments")?;
    writeln!(f, "gsave")?;
    writeln!(f, "1 setlinewidth")?;

    if let Some(ref title) = bounds.chart_title
        && !title.is_empty()
    {
        const CHAR_W: f64 = 6.0;
        writeln!(f, "/Helvetica findfont 12 scalefont setfont")?;
        let title_w = title.len() as f64 * CHAR_W;
        let tx = (W / 2.0 - title_w / 2.0).max(4.0).min(W - title_w - 4.0);
        writeln!(f, "{} {} moveto ({}) show", tx, H - 15.0, ps_escape(title))?;
        writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    }

    const MAX_TICKS: usize = 8;
    let x_axis = bounds.x_axis(&nice_ticks(x_min, x_max, MAX_TICKS));
    let y_axis = bounds.y_axis(&nice_ticks(y_min, y_max, MAX_TICKS));
    let x_ticks = x_axis.ticks();
    let y_ticks = y_axis.ticks();

    writeln!(f, "0.9 setgray")?;
    writeln!(f, "0.5 setlinewidth")?;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            writeln!(
                f,
                "{} {} moveto 0 {} rlineto stroke",
                px, MARGIN_BOTTOM, PLOT_H
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            writeln!(
                f,
                "{} {} moveto {} 0 rlineto stroke",
                MARGIN_LEFT, py, PLOT_W
            )?;
        }
    }
    writeln!(f, "1 setlinewidth")?;
    writeln!(f, "0 setgray")?;

    writeln!(f, "{} {} moveto", MARGIN_LEFT, MARGIN_BOTTOM)?;
    writeln!(f, "{} 0 rlineto", PLOT_W)?;
    writeln!(f, "0 {} rlineto", PLOT_H)?;
    writeln!(f, "{} 0 rlineto", -PLOT_W)?;
    writeln!(f, "closepath stroke")?;

    let x_step = (x_max - x_min) / data.x_bins.max(1) as f64;
    let y_step = (y_max - y_min) / data.y_bins.max(1) as f64;
    for y in 0..data.y_bins {
        for x in 0..data.x_bins {
            let count = data.counts[y][x];
            let intensity = (count / data.max_count).clamp(0.0, 1.0);
            let shade = 1.0 - intensity;
            writeln!(f, "{} {} {} setrgbcolor", shade, shade, 1.0)?;
            let x0 = to_x(x_min + x as f64 * x_step);
            let x1 = to_x(x_min + (x + 1) as f64 * x_step);
            let y0 = to_y(y_min + y as f64 * y_step);
            let y1 = to_y(y_min + (y + 1) as f64 * y_step);
            writeln!(f, "{} {} {} {} rectfill", x0, y0, x1 - x0, y1 - y0)?;
        }
    }
    writeln!(f, "0 setgray")?;

    const TICK_LEN: f64 = 4.0;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            writeln!(
                f,
                "{} {} moveto 0 {} rlineto stroke",
                px, MARGIN_BOTTOM, -TICK_LEN
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            writeln!(
                f,
                "{} {} moveto {} 0 rlineto stroke",
                MARGIN_LEFT, py, -TICK_LEN
            )?;
        }
    }

    writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    let char_w: f64 = 5.0;
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            let s = x_axis.label(v).unwrap_or_default();
            let label_w = s.len() as f64 * char_w;
            let tx = (px - label_w / 2.0)
                .max(MARGIN_LEFT)
                .min(MARGIN_LEFT + PLOT_W - label_w);
            writeln!(
                f,
                "{} {} moveto ({}) show",
                tx,
                MARGIN_BOTTOM - 12.0,
                ps_escape(&s)
            )?;
        }
    }
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            let s = y_axis.label(v).unwrap_or_default();
            let label_w = s.len() as f64 * char_w;
            let tx = (MARGIN_LEFT - label_w - 4.0).max(2.0);
            writeln!(f, "{} {} moveto ({}) show", tx, py - 3.0, ps_escape(&s))?;
        }
    }

    writeln!(f, "/Helvetica findfont 10 scalefont setfont")?;
    if !bounds.x_label.is_empty() {
        let x_center = MARGIN_LEFT + PLOT_W / 2.0;
        let x_str_approx_len = bounds.x_label.len() as f64 * char_w;
        writeln!(
            f,
            "{} {} moveto ({}) show",
            (x_center - x_str_approx_len / 2.0).max(MARGIN_LEFT),
            MARGIN_BOTTOM - 24.0,
            ps_escape(&bounds.x_label)
        )?;
    }
    if !bounds.y_label.is_empty() {
        writeln!(f, "gsave")?;
        writeln!(
            f,
            "12 {} translate -90 rotate",
            MARGIN_BOTTOM + PLOT_H / 2.0
        )?;
        let y_str_approx_len = bounds.y_label.len() as f64 * char_w;
        writeln!(
            f,
            "{} 0 moveto ({}) show",
            -y_str_approx_len / 2.0,
            ps_escape(&bounds.y_label)
        )?;
        writeln!(f, "grestore")?;
    }

    write_eps_notes(&mut f, &bounds.notes, MARGIN_LEFT + PLOT_W)?;
    writeln!(f, "grestore")?;
    writeln!(f, "%%EOF")?;
    f.sync_all()?;
    Ok(())
}

/// A bar chart's value axis: from zero, or from the most negative value, to the
/// largest.
fn bar_bounds(data: &BarData) -> (f64, f64) {
    let lo = data.bars.iter().map(|b| b.value).fold(0.0_f64, f64::min);
    let hi = data.bars.iter().map(|b| b.value).fold(0.0_f64, f64::max);
    if hi > lo { (lo, hi) } else { (lo, lo + 1.0) }
}

/// What a bar chart's value axis holds: the value column's numbers in `format`, whole
/// for an integer column or a count.
fn bar_numbers(data: &BarData, format: &NumberFormat) -> AxisNumbers {
    AxisNumbers {
        format: format.clone(),
        whole: data.value_dtype.is_integer(),
    }
}

/// A bar's category as the export writes it, cut to `max` characters.
fn bar_label(bar: &Bar, max: usize) -> String {
    let label = bar.label.as_deref().unwrap_or("null");
    if label.chars().count() <= max {
        return label.to_string();
    }
    let kept: String = label.chars().take(max.saturating_sub(3)).collect();
    format!("{kept}...")
}

/// An export axis's tick labels, all in one format chosen from its ticks, in the
/// table's number style. A whole-number axis (counts, an integer column) has no tick
/// between two whole numbers.
struct TickLabels {
    ticks: Vec<f64>,
    format: AxisFormat,
    whole: bool,
    /// A log scale's, where a tick at `v` stands for `exp_m1(v)`.
    log: bool,
    kind: XAxisTemporalKind,
}

impl TickLabels {
    /// Labels for the ticks among `ticks` an axis holding `numbers` keeps.
    fn new(ticks: &[f64], numbers: &AxisNumbers, log: bool, kind: XAxisTemporalKind) -> Self {
        let whole = numbers.whole && !log;
        let ticks: Vec<f64> = ticks
            .iter()
            .copied()
            .filter(|&v| !whole || is_whole(v))
            .collect();
        let shown: Vec<f64> = ticks
            .iter()
            .map(|&v| if log { v.exp_m1() } else { v })
            .collect();
        Self {
            format: AxisFormat::new(&shown, numbers),
            ticks,
            whole,
            log,
            kind,
        }
    }

    /// A plain numeric axis's.
    fn numbers(ticks: &[f64], numbers: &AxisNumbers) -> Self {
        Self::new(ticks, numbers, false, XAxisTemporalKind::Numeric)
    }

    /// The ticks kept.
    fn ticks(&self) -> Vec<f64> {
        self.ticks.clone()
    }

    /// The tick at `v`'s label, or `None` where the axis has no tick.
    fn label(&self, v: f64) -> Option<String> {
        if self.whole && !is_whole(v) {
            return None;
        }
        let v = if self.log { v.exp_m1() } else { v };
        x_axis_label_at(v, self.kind, (v, v), 0, &self.format)
    }
}

/// Whether `v` is a whole number: ticks are stepped in floating point, so a whole one
/// can be a hair off.
fn is_whole(v: f64) -> bool {
    (v - v.round()).abs() <= 1e-9 * v.round().abs().max(1.0)
}

/// The ticks plotters puts on an axis from `lo` to `hi` for `count` labels, so the
/// labels' format is chosen from the ticks it draws.
fn png_ticks(lo: f64, hi: f64, count: usize) -> Vec<f64> {
    use plotters::coord::ranged1d::Ranged;
    plotters::coord::types::RangedCoordf64::from(lo..hi).key_points(count)
}

/// Ticks plotters puts on an axis from `lo` to `hi`: on a whole-number axis no more
/// than the whole numbers in the range, so each tick can be one.
fn tick_count(lo: f64, hi: f64, whole: bool) -> usize {
    if whole {
        ((hi - lo).floor() as usize + 1).clamp(2, 10)
    } else {
        10
    }
}

/// Longest category label an export writes, so a long one cannot run into the bars.
const BAR_LABEL_MAX: usize = 30;

/// The category axis title, with the categories past the cap counted.
fn bar_category_title(data: &BarData) -> String {
    if data.more > 0 {
        format!(
            "{} (+ {} more)",
            data.category,
            crate::numfmt::group_chrome(data.more)
        )
    } else {
        data.category.clone()
    }
}

/// Write a bar chart to PNG: one horizontal bar per category, top to bottom in the
/// chart's order, and the chart's notes under it. Size is (width, height) in pixels.
pub fn write_bar_png(
    path: &Path,
    data: &BarData,
    format: &NumberFormat,
    title: Option<&str>,
    notes: &[String],
    (width, height): (u32, u32),
) -> Result<()> {
    use plotters::prelude::*;

    if data.bars.is_empty() {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }
    let n = data.bars.len();
    let labels: Vec<String> = data
        .bars
        .iter()
        .map(|b| bar_label(b, BAR_LABEL_MAX))
        .collect();
    let longest = labels.iter().map(|l| l.chars().count()).max().unwrap_or(1) as u32;
    let (lo, hi) = bar_bounds(data);
    // Air past the longest bars, so none runs into the frame.
    let pad = (hi - lo) * 0.04;
    let lo = if lo < 0.0 { lo - pad } else { lo };
    let hi = if hi > 0.0 { hi + pad } else { hi };

    let root = BitMapBackend::new(path, (width, height)).into_drawing_area();
    root.fill(&WHITE)?;
    let mut binding = ChartBuilder::on(&root);
    let builder = binding.margin(30);
    let builder = match title.filter(|t| !t.is_empty()) {
        Some(t) => builder.caption(t, ("sans-serif", 20)),
        None => builder,
    };
    let mut chart = builder
        .x_label_area_size(40)
        .y_label_area_size((longest * 7 + 30).clamp(50, width / 3))
        // An i32 range holds both ends, so n segments are 0..n-1.
        .build_cartesian_2d(lo..hi, (0..n as i32 - 1).into_segmented())?;

    // The first bar sits in the top segment, as on screen.
    let key_label = |v: &SegmentValue<i32>| match v {
        SegmentValue::CenterOf(k) => labels
            .get(n.wrapping_sub(1).wrapping_sub(*k as usize))
            .cloned()
            .unwrap_or_default(),
        _ => String::new(),
    };
    let category_title = bar_category_title(data);
    let numbers = bar_numbers(data, format);
    let ticks = tick_count(lo, hi, numbers.whole);
    let value_axis = TickLabels::numbers(&png_ticks(lo, hi, ticks), &numbers);
    let value_tick = |v: &f64| value_axis.label(*v).unwrap_or_default();
    chart
        .configure_mesh()
        .disable_y_mesh()
        .y_labels(n)
        .x_labels(ticks)
        .x_desc(data.value_column.as_str())
        .y_desc(category_title.as_str())
        .x_label_formatter(&value_tick)
        .y_label_formatter(&key_label)
        .draw()?;

    chart.draw_series(data.bars.iter().enumerate().map(|(i, bar)| {
        let key = (n - 1 - i) as i32;
        let mut rect = Rectangle::new(
            [
                (0.0, SegmentValue::Exact(key)),
                (bar.value, SegmentValue::Exact(key + 1)),
            ],
            CYAN.filled(),
        );
        rect.set_margin(2, 2, 0, 0);
        rect
    }))?;

    draw_png_notes(&root, notes, 30)?;
    root.present()?;
    Ok(())
}

/// Write a bar chart to EPS: one horizontal bar per category, the value past its end
/// (right of the zero line for a negative bar, clear of the labels).
/// The page grows with the number of bars. No external dependencies.
pub fn write_bar_eps(
    path: &Path,
    data: &BarData,
    format: &NumberFormat,
    title: Option<&str>,
    notes: &[String],
) -> Result<()> {
    if data.bars.is_empty() {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }
    let values = data.labels_in(format);
    const W: f64 = 500.0;
    const ROW_H: f64 = 14.0;
    const CHAR_W: f64 = 5.0;
    // Room for the longest value past the longest bar.
    let longest_value = values.iter().map(|v| v.chars().count()).max().unwrap_or(0) as f64;
    let margin_right = (longest_value * CHAR_W + 10.0).max(50.0);
    // Ticks, the axis titles, and the notes below them.
    const MARGIN_BOTTOM: f64 = 48.0;
    let title = title.filter(|t| !t.is_empty());
    let margin_top = if title.is_some() { 30.0 } else { 12.0 };

    let labels: Vec<String> = data
        .bars
        .iter()
        .map(|b| bar_label(b, BAR_LABEL_MAX))
        .collect();
    let longest = labels.iter().map(|l| l.chars().count()).max().unwrap_or(1) as f64;
    let margin_left = (longest * CHAR_W + 16.0).clamp(40.0, W / 3.0);
    let plot_w = W - margin_left - margin_right;
    let plot_h = data.bars.len() as f64 * ROW_H;
    let h = margin_top + plot_h + MARGIN_BOTTOM;
    let (lo, hi) = bar_bounds(data);
    let to_x = |v: f64| margin_left + (v - lo) / (hi - lo) * plot_w;

    let mut f = File::create(path)?;
    writeln!(f, "%!PS-Adobe-3.0 EPSF-3.0")?;
    writeln!(
        f,
        "%%BoundingBox: 0 0 {} {}",
        W.ceil() as i32,
        h.ceil() as i32
    )?;
    writeln!(f, "%%Creator: datui")?;
    writeln!(f, "%%EndComments")?;
    writeln!(f, "gsave")?;
    writeln!(f, "1 setlinewidth")?;

    if let Some(title) = title {
        writeln!(f, "/Helvetica findfont 12 scalefont setfont")?;
        let title_w = title.len() as f64 * 6.0;
        let tx = (W / 2.0 - title_w / 2.0).max(4.0);
        writeln!(f, "{} {} moveto ({}) show", tx, h - 18.0, ps_escape(title))?;
    }

    // Value grid and ticks.
    writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
    let value_axis = TickLabels::numbers(&nice_ticks(lo, hi, 6), &bar_numbers(data, format));
    for v in value_axis.ticks() {
        let px = to_x(v);
        let Some(s) = value_axis.label(v) else {
            continue;
        };
        if !(margin_left..=margin_left + plot_w).contains(&px) {
            continue;
        }
        writeln!(f, "0.9 setgray 0.5 setlinewidth")?;
        writeln!(
            f,
            "{} {} moveto 0 {} rlineto stroke",
            px, MARGIN_BOTTOM, plot_h
        )?;
        writeln!(f, "0 setgray 1 setlinewidth")?;
        let label_w = s.len() as f64 * CHAR_W;
        writeln!(
            f,
            "{} {} moveto ({}) show",
            px - label_w / 2.0,
            MARGIN_BOTTOM - 12.0,
            ps_escape(&s)
        )?;
    }

    // Bars, first at the top, each with its category left of the plot and its value
    // past its end.
    for (i, ((bar, label), value)) in data.bars.iter().zip(&labels).zip(&values).enumerate() {
        let top = MARGIN_BOTTOM + plot_h - i as f64 * ROW_H;
        let (x0, x1) = (to_x(bar.value.min(0.0)), to_x(bar.value.max(0.0)));
        writeln!(f, "0.0 0.7 0.9 setrgbcolor")?;
        writeln!(
            f,
            "{} {} {} {} rectfill",
            x0,
            top - ROW_H + 2.0,
            x1 - x0,
            ROW_H - 4.0
        )?;
        writeln!(f, "0 setgray")?;
        let baseline = top - ROW_H / 2.0 - 3.0;
        let label_w = label.chars().count() as f64 * CHAR_W;
        writeln!(
            f,
            "{} {} moveto ({}) show",
            (margin_left - 6.0 - label_w).max(2.0),
            baseline,
            ps_escape(label)
        )?;
        let value_x = x1 + 4.0;
        writeln!(
            f,
            "{} {} moveto ({}) show",
            value_x,
            baseline,
            ps_escape(value)
        )?;
    }

    // The zero line and the plot's bottom edge.
    writeln!(
        f,
        "{} {} moveto 0 {} rlineto stroke",
        to_x(0.0),
        MARGIN_BOTTOM,
        plot_h
    )?;
    writeln!(
        f,
        "{} {} moveto {} 0 rlineto stroke",
        margin_left, MARGIN_BOTTOM, plot_w
    )?;

    writeln!(f, "/Helvetica findfont 10 scalefont setfont")?;
    let value_title = &data.value_column;
    writeln!(
        f,
        "{} {} moveto ({}) show",
        margin_left + plot_w / 2.0 - value_title.len() as f64 * CHAR_W / 2.0,
        MARGIN_BOTTOM - 26.0,
        ps_escape(value_title)
    )?;
    writeln!(
        f,
        "4 {} moveto ({}) show",
        MARGIN_BOTTOM - 26.0,
        ps_escape(&bar_category_title(data))
    )?;

    write_eps_notes(&mut f, notes, margin_left + plot_w)?;
    writeln!(f, "grestore")?;
    writeln!(f, "%%EOF")?;
    f.sync_all()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart_modal::ChartType;
    use std::io::Read;

    /// Verifies that EPS output contains expected structural elements: header, grid, axis box,
    /// tick marks, tick labels, axis titles, and series data.
    #[test]
    fn eps_contains_desired_elements() {
        let series = vec![ChartExportSeries {
            name: "s1".to_string(),
            points: vec![(0.0, 1.0), (1.0, 2.0), (2.0, 1.5)],
            breaks: Vec::new(),
        }];
        let bounds = ChartExportBounds {
            x_min: 0.0,
            x_max: 2.0,
            y_min: 0.0,
            y_max: 2.5,
            x_label: "x_col".to_string(),
            y_label: "y_col".to_string(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            log_scale: false,
            chart_title: None,
            notes: vec![
                "sample of 1,000 of 50k rows".to_string(),
                "4 values outside p1-p99".to_string(),
            ],
            x_numbers: AxisNumbers::default(),
            y_numbers: AxisNumbers::default(),
        };

        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("chart.eps");
        write_chart_eps(&path, &series, ChartType::Line, &bounds).expect("write_chart_eps");

        let mut content = String::new();
        std::fs::File::open(&path)
            .expect("open")
            .read_to_string(&mut content)
            .expect("read");

        // Header and bounding box
        assert!(content.contains("%!PS-Adobe-3.0 EPSF-3.0"), "EPS header");
        assert!(content.contains("%%BoundingBox:"), "BoundingBox");
        assert!(content.contains("%%Creator: datui"), "Creator");

        // Grid (light gray lines)
        assert!(content.contains("0.9 setgray"), "grid color");
        assert!(
            content.contains("rlineto stroke") && content.matches("rlineto stroke").count() > 2,
            "grid/axis lines"
        );

        // Axis box
        assert!(content.contains("closepath stroke"), "axis box");

        // Tick marks (short outward lines; we draw moveto then rlineto then stroke)
        assert!(content.contains("moveto"), "tick/line moveto");
        assert!(content.contains("stroke"), "stroke");

        // Tick labels (numeric text)
        assert!(content.contains(") show"), "tick or axis label show");

        // Axis titles (column names)
        assert!(content.contains("(x_col)"), "x axis title");
        assert!(content.contains("(y_col)"), "y axis title");

        // Series data (color and drawing)
        assert!(content.contains("setrgbcolor"), "series color");
        assert!(content.contains("lineto"), "line series");

        // The notes on the chart's input, as the chart view shows them
        assert!(
            content.contains("(sample of 1,000 of 50k rows)"),
            "sample note"
        );
        assert!(content.contains("(4 values outside p1-p99)"), "range note");
    }

    /// Walks a PostScript line and returns the text that sits *outside* string
    /// literals, which is the part a interpreter executes as code.
    ///
    /// Deliberately a real scanner rather than a substring search: the whole
    /// question is whether a `)` in the data terminates a literal early, and
    /// only tracking `\` escaping answers that.
    fn code_outside_strings(line: &str) -> String {
        let mut out = String::new();
        let mut chars = line.chars();
        let mut in_string = false;
        while let Some(c) = chars.next() {
            match c {
                // A backslash escapes the next character, inside a string or not.
                '\\' => {
                    chars.next();
                }
                '(' if !in_string => in_string = true,
                ')' if in_string => in_string = false,
                _ if !in_string => out.push(c),
                _ => {}
            }
        }
        out
    }

    /// Chart labels come from column names and cell values, so they are
    /// untrusted. PostScript is a programming language, and an exported chart
    /// gets opened by other people in a viewer, so a label that escapes its
    /// string literal becomes code running on someone else's machine.
    ///
    /// `ps_escape` handles this today. This test exists so that a future `show`
    /// call added without it fails here rather than shipping.
    #[test]
    fn chart_labels_cannot_escape_postscript_string_literals() {
        // Each payload closes the literal and leaves the marker as a bare
        // token, which is where an interpreter would read it as code. The
        // marker deliberately sits *outside* any parentheses: text inside a
        // literal is inert no matter what surrounds it, so a payload shaped
        // like `) (INJECTED) show (` would pass this test while still being a
        // real injection.
        let payloads = [
            ") INJECTED 0 0 moveto (",
            "\\) INJECTED (",
            "a) INJECTED (b",
            "trailing backslash \\",
            "unbalanced ( open",
            "unbalanced ) close",
        ];

        for payload in payloads {
            let series = vec![ChartExportSeries {
                name: payload.to_string(),
                points: vec![(0.0, 1.0), (1.0, 2.0)],
                breaks: Vec::new(),
            }];
            let bounds = ChartExportBounds {
                x_min: 0.0,
                x_max: 2.0,
                y_min: 0.0,
                y_max: 2.5,
                x_label: payload.to_string(),
                y_label: payload.to_string(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                log_scale: false,
                chart_title: Some(payload.to_string()),
                notes: vec![payload.to_string()],
                x_numbers: AxisNumbers::default(),
                y_numbers: AxisNumbers::default(),
            };

            let dir = tempfile::tempdir().expect("temp dir");
            let path = dir.path().join("chart.eps");
            write_chart_eps(&path, &series, ChartType::Line, &bounds).expect("write_chart_eps");

            let mut content = String::new();
            std::fs::File::open(&path)
                .expect("open")
                .read_to_string(&mut content)
                .expect("read");

            for (i, line) in content.lines().enumerate() {
                // DSC comments are not executed, and carry no data anyway.
                if line.starts_with('%') {
                    continue;
                }
                let code = code_outside_strings(line);
                assert!(
                    !code.contains("INJECTED"),
                    "payload {:?} escaped its string literal on line {}: {:?}",
                    payload,
                    i + 1,
                    line
                );
            }
        }
    }

    #[test]
    fn ps_escape_neutralises_literal_delimiters() {
        // Backslash must be escaped first, or escaping the parens would
        // introduce backslashes that then get doubled and stop escaping.
        assert_eq!(ps_escape("a(b)c"), "a\\(b\\)c");
        assert_eq!(ps_escape("back\\slash"), "back\\\\slash");
        assert_eq!(ps_escape("\\)"), "\\\\\\)");
        assert_eq!(ps_escape("plain"), "plain");
    }

    fn bar_data() -> BarData {
        BarData {
            category: "carrier".to_string(),
            value_column: "delay".to_string(),
            bars: vec![
                Bar {
                    label: Some("F9".to_string()),
                    value: 21.92,
                },
                Bar {
                    label: None,
                    value: 3.0,
                },
                Bar {
                    label: Some("AS) INJECTED (".to_string()),
                    value: -9.93,
                },
            ],
            more: 12,
            no_value: 0,
            rows: Default::default(),
            value_dtype: polars::prelude::DataType::Float64,
            counted: None,
        }
    }

    /// A bar chart exports to both formats: every category and value in the EPS, the
    /// categories past the cap counted, a label unable to escape its string.
    #[test]
    fn bar_charts_export_to_png_and_eps() {
        let dir = tempfile::tempdir().expect("temp dir");
        let data = bar_data();
        let png = dir.path().join("bars.png");
        let notes = ["sample of 10,000 of 50k rows".to_string()];
        let format = NumberFormat::preset("european").unwrap();
        write_bar_png(
            &png,
            &data,
            &format,
            Some("Delay by carrier"),
            &notes,
            (640, 480),
        )
        .expect("png");
        assert!(std::fs::metadata(&png).unwrap().len() > 0);

        let eps = dir.path().join("bars.eps");
        write_bar_eps(&eps, &data, &format, Some("Delay by carrier"), &notes).expect("eps");
        let content = std::fs::read_to_string(&eps).unwrap();
        for text in [
            "(sample of 10,000 of 50k rows)",
            "(F9)",
            "(null)",
            "(21,92)",
            "(-9,93)",
            "(delay)",
            "(carrier (+ 12 more))",
        ] {
            let text = text.replace("carrier (+ 12 more)", "carrier \\(+ 12 more\\)");
            assert!(content.contains(&text), "{text} in {content}");
        }
        for line in content.lines().filter(|l| !l.starts_with('%')) {
            assert!(!code_outside_strings(line).contains("INJECTED"), "{line}");
        }
    }

    /// Counts, and an integer column's values, tick in whole numbers in the table's
    /// format: `5,000`, never `5000.00`, and no tick between two whole numbers.
    #[test]
    fn a_whole_number_value_axis_has_whole_ticks() {
        let format = NumberFormat::preset("thousands").unwrap();
        let whole = AxisNumbers {
            format: format.clone(),
            whole: true,
        };
        let axis = TickLabels::numbers(&[0.0, 5_000.0, 10_000.0], &whole);
        assert_eq!(axis.label(10_000.0).as_deref(), Some("10,000"));
        assert_eq!(axis.label(2.000_000_000_000_4).as_deref(), Some("2"));
        assert_eq!(axis.label(0.5), None);
        let axis = TickLabels::numbers(&[0.0, 0.5, 1.0], &AxisNumbers::default());
        assert_eq!(axis.label(0.5).as_deref(), Some("0.50"));

        let dir = tempfile::tempdir().expect("temp dir");
        let counts = |values: &[f64]| BarData {
            category: "carrier".to_string(),
            value_column: "count".to_string(),
            bars: values
                .iter()
                .map(|&value| Bar {
                    label: Some("UA".to_string()),
                    value,
                })
                .collect(),
            more: 0,
            no_value: 0,
            rows: Default::default(),
            value_dtype: polars::prelude::DataType::UInt64,
            counted: None,
        };
        for (values, ticks) in [
            (
                &[30_000.0, 15_000.0][..],
                &["(0)", "(10,000)", "(30,000)"][..],
            ),
            (&[3.0, 1.0][..], &["(0)", "(1)", "(2)", "(3)"][..]),
        ] {
            let data = counts(values);
            let eps = dir.path().join("counts.eps");
            write_bar_eps(&eps, &data, &format, None, &[]).expect("eps");
            let content = std::fs::read_to_string(&eps).unwrap();
            for tick in ticks {
                assert!(content.contains(tick), "{tick} in {content}");
            }
            assert!(!content.contains(".5)"), "{content}");
            assert!(!content.contains(".00)"), "{content}");
            let png = dir.path().join("counts.png");
            write_bar_png(&png, &data, &format, None, &[], (640, 480)).expect("png");
        }
    }

    /// Any whole-number axis, not only a bar chart's: a histogram's counts and an
    /// integer x column tick whole in the table's format, in PNG and EPS alike.
    #[test]
    fn whole_number_axes_have_whole_ticks_on_every_chart() {
        let format = NumberFormat::preset("thousands").unwrap();
        let series = vec![ChartExportSeries {
            name: "count".to_string(),
            points: vec![(0.0, 30_000.0), (1.5, 12_000.0), (3.0, 0.0)],
            breaks: Vec::new(),
        }];
        let bounds = ChartExportBounds {
            x_min: 0.0,
            x_max: 3.0,
            y_min: 0.0,
            y_max: 30_000.0,
            x_label: "passengers".to_string(),
            y_label: "Count".to_string(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            log_scale: false,
            chart_title: None,
            notes: Vec::new(),
            x_numbers: AxisNumbers {
                format: format.clone(),
                whole: true,
            },
            y_numbers: AxisNumbers {
                format,
                whole: true,
            },
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let eps = dir.path().join("whole.eps");
        write_chart_eps(&eps, &series, ChartType::Bar, &bounds).expect("eps");
        let content = std::fs::read_to_string(&eps).unwrap();
        for tick in ["(0)", "(1)", "(3)", "(10,000)", "(30,000)"] {
            assert!(content.contains(tick), "{tick} in {content}");
        }
        assert!(!content.contains(".5)"), "{content}");
        assert!(!content.contains(".00)"), "{content}");
        let png = dir.path().join("whole.png");
        write_chart_png(&png, &series, ChartType::Bar, &bounds, (640, 480)).expect("png");

        let y_axis = bounds.y_axis(&[0.0, 15_000.0, 30_000.0]);
        assert_eq!(y_axis.label(15_000.0).as_deref(), Some("15,000"));
        assert_eq!(bounds.x_axis(&[0.0, 0.5, 1.0]).label(0.5), None);
    }

    /// An export's axis takes one format from its ticks, in the table's number
    /// style: densities around 0.01 keep their places all the way up, never switching
    /// to scientific notation, and counts group as the table groups them.
    #[test]
    fn export_axes_keep_one_format_in_the_table_style() {
        let european = NumberFormat::preset("european").unwrap();
        let series = vec![ChartExportSeries {
            name: "price".to_string(),
            points: vec![(0.0, 0.0), (6_000.0, 0.012), (12_000.0, 0.0)],
            breaks: Vec::new(),
        }];
        let bounds = ChartExportBounds {
            x_min: 0.0,
            x_max: 12_000.0,
            y_min: 0.0,
            y_max: 0.012,
            x_label: "price".to_string(),
            y_label: "Density".to_string(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            log_scale: false,
            chart_title: None,
            notes: Vec::new(),
            x_numbers: AxisNumbers {
                format: european,
                whole: false,
            },
            y_numbers: AxisNumbers::default(),
        };
        let dir = tempfile::tempdir().expect("temp dir");
        let eps = dir.path().join("kde.eps");
        write_chart_eps(&eps, &series, ChartType::Line, &bounds).expect("eps");
        let content = std::fs::read_to_string(&eps).unwrap();
        for tick in ["(0.0000)", "(0.0060)", "(0.0120)", "(6.000)", "(12.000)"] {
            assert!(content.contains(tick), "{tick} in {content}");
        }
        // Per tick, the lower ones read `(2.00e-3)`.
        assert!(!content.contains("e-3)"), "{content}");
        let png = dir.path().join("kde.png");
        write_chart_png(&png, &series, ChartType::Line, &bounds, (640, 480)).expect("png");

        // plotters ticks where these say, so its labels are chosen from its ticks.
        assert_eq!(png_ticks(0.0, 0.012, 10).len(), 7);
        let y_axis = bounds.y_axis(&png_ticks(0.0, 0.012, 10));
        assert_eq!(y_axis.label(0.002).as_deref(), Some("0.0020"));
    }

    /// A long category is cut rather than run into the bars.
    #[test]
    fn a_long_bar_label_is_cut() {
        let long = Bar {
            label: Some("x".repeat(80)),
            value: 1.0,
        };
        let cut = bar_label(&long, BAR_LABEL_MAX);
        assert_eq!(cut.chars().count(), BAR_LABEL_MAX);
        assert!(cut.ends_with("..."), "{cut}");
    }
}
