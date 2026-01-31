//! Chart export to PNG (plotters bitmap) and EPS (minimal PostScript, no deps).

use color_eyre::Result;
use std::fs::File;
use std::io::Write;
use std::path::Path;

use crate::chart_data::{format_axis_label, format_x_axis_label, XAxisTemporalKind};
use crate::chart_modal::ChartType;

/// Escape a string for PostScript ( and ) and \.
fn ps_escape(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('(', "\\(")
        .replace(')', "\\)")
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

/// Format a numeric tick for display (used for y when not log scale).
fn format_tick(v: f64) -> String {
    format_axis_label(v)
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
}

/// One series: name and (x, y) points (y already log-transformed if log scale).
pub struct ChartExportSeries {
    pub name: String,
    pub points: Vec<(f64, f64)>,
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
    if let Some(ref title) = bounds.chart_title {
        if !title.is_empty() {
            const CHAR_W: f64 = 6.0;
            writeln!(f, "/Helvetica findfont 12 scalefont setfont")?;
            let title_w = title.len() as f64 * CHAR_W;
            let tx = (W / 2.0 - title_w / 2.0).max(4.0).min(W - title_w - 4.0);
            writeln!(f, "{} {} moveto ({}) show", tx, H - 15.0, ps_escape(title))?;
            writeln!(f, "/Helvetica findfont 9 scalefont setfont")?;
        }
    }

    // Tick positions for grid, ticks, and labels
    const MAX_TICKS: usize = 8;
    let x_ticks = nice_ticks(x_min, x_max, MAX_TICKS);
    let y_ticks = nice_ticks(y_min, y_max, MAX_TICKS);

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
    let format_x_tick = |v: f64| format_x_axis_label(v, bounds.x_axis_kind);
    for &v in &x_ticks {
        let px = to_x(v);
        if (MARGIN_LEFT..=MARGIN_LEFT + PLOT_W).contains(&px) {
            let s = format_x_tick(v);
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
    let format_y_tick = |v: f64| {
        if bounds.log_scale {
            format_axis_label(v.exp_m1())
        } else {
            format_tick(v)
        }
    };
    for &v in &y_ticks {
        let py = to_y(v);
        if (MARGIN_BOTTOM..=MARGIN_BOTTOM + PLOT_H).contains(&py) {
            let s = format_y_tick(v);
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
                let (px, py) = s.points[0];
                writeln!(f, "{} {} moveto", to_x(px), to_y(py))?;
                for &(px, py) in &s.points[1..] {
                    writeln!(f, "{} {} lineto", to_x(px), to_y(py))?;
                }
                writeln!(f, "stroke")?;
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

    writeln!(f, "grestore")?;
    writeln!(f, "%%EOF")?;
    f.sync_all()?;
    Ok(())
}

/// Write chart to PNG using plotters bitmap backend.
pub fn write_chart_png(
    path: &Path,
    series: &[ChartExportSeries],
    chart_type: ChartType,
    bounds: &ChartExportBounds,
) -> Result<()> {
    use plotters::prelude::*;

    if series.is_empty() || series.iter().all(|s| s.points.is_empty()) {
        return Err(color_eyre::eyre::eyre!("No data to export"));
    }

    let root = BitMapBackend::new(path, (640, 480)).into_drawing_area();
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

    let x_axis_kind = bounds.x_axis_kind;
    let log_scale = bounds.log_scale;
    let x_formatter = move |v: &f64| format_x_axis_label(*v, x_axis_kind);
    let y_formatter = move |v: &f64| {
        if log_scale {
            format_axis_label(v.exp_m1())
        } else {
            format_axis_label(*v)
        }
    };
    chart
        .configure_mesh()
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
                chart
                    .draw_series(LineSeries::new(s.points.iter().copied(), color))?
                    .label(s.name.as_str())
                    .legend(move |(x, y)| PathElement::new(vec![(x, y), (x + 20, y)], color));
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

    root.present()?;
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
    }
}
