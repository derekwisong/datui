//! Chart export: the chart is drawn once as SVG, in the bundled font (IBM Plex Sans,
//! under the SIL Open Font License, in `assets/fonts`), then written as SVG with
//! its text as outlines, rasterized to PNG, or written as PDF from the same
//! outlines (`chart_pdf`). The same figure comes out the same on every machine;
//! text the bundled font lacks falls back to a system font.

use std::sync::{Arc, OnceLock};

use color_eyre::Result;
use resvg::{tiny_skia, usvg};

use crate::chart_data::{
    AxisFormat, AxisNumbers, BarData, BoxPlotData, HeatmapData, HistogramData, KdeData,
    XAxisTemporalKind, segments, x_axis_label_at,
};
use crate::widgets::ticks;

const FONT_REGULAR: &[u8] = include_bytes!("../assets/fonts/IBMPlexSans-Regular.ttf");
const FONT_SEMIBOLD: &[u8] = include_bytes!("../assets/fonts/IBMPlexSans-SemiBold.ttf");
const FONT_FAMILY: &str = "IBM Plex Sans";

/// Export format for a chart.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChartExportFormat {
    Png,
    Svg,
    Pdf,
}

impl ChartExportFormat {
    pub const ALL: [Self; 3] = [Self::Png, Self::Svg, Self::Pdf];

    pub fn extension(self) -> &'static str {
        match self {
            Self::Png => "png",
            Self::Svg => "svg",
            Self::Pdf => "pdf",
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Png => "PNG",
            Self::Svg => "SVG",
            Self::Pdf => "PDF",
        }
    }

    /// The format a path's extension names, if it names one.
    pub fn from_extension(path: &std::path::Path) -> Option<Self> {
        let ext = path.extension()?.to_str()?.to_ascii_lowercase();
        Self::ALL.into_iter().find(|f| f.extension() == ext)
    }
}

/// The colors an export is drawn in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExportStyle {
    /// White, with a print-safe palette that stays apart for color-blind readers.
    Light,
    /// The terminal theme's colors.
    Dark,
    /// The light palette with no background, for a slide or page of any color.
    Transparent,
}

impl ExportStyle {
    pub const ALL: [Self; 3] = [Self::Light, Self::Dark, Self::Transparent];

    pub fn label(self) -> &'static str {
        match self {
            Self::Light => "Light",
            Self::Dark => "Dark",
            Self::Transparent => "Transparent",
        }
    }
}

/// A size an export is made at: pixels, and the resolution that makes them a
/// physical size (a PDF's page, an SVG's inches, the text's points).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SizePreset {
    Slide,
    Document,
    Square,
    /// One column of a two-column journal page, about 3.5 in at 300 dpi.
    SingleColumn,
    /// The width of a journal page, about 7 in at 300 dpi.
    DoubleColumn,
    Custom,
}

impl SizePreset {
    pub const ALL: [Self; 6] = [
        Self::Slide,
        Self::Document,
        Self::Square,
        Self::SingleColumn,
        Self::DoubleColumn,
        Self::Custom,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Slide => "Slide 16:9",
            Self::Document => "Document",
            Self::Square => "Square",
            Self::SingleColumn => "Single column",
            Self::DoubleColumn => "Double column",
            Self::Custom => "Custom",
        }
    }

    /// Width and height in pixels; `None` for a custom size.
    pub fn size(self) -> Option<(u32, u32)> {
        match self {
            Self::Slide => Some((1920, 1080)),
            Self::Document => Some((1600, 1000)),
            Self::Square => Some((1200, 1200)),
            Self::SingleColumn => Some((1050, 788)),
            Self::DoubleColumn => Some((2100, 1300)),
            Self::Custom => None,
        }
    }

    /// Pixels per inch: print presets at 300, screen sizes as a 10 in wide page
    /// at twice a screen's density.
    pub fn dpi(self) -> f32 {
        match self {
            Self::SingleColumn | Self::DoubleColumn => 300.0,
            Self::Slide => 192.0,
            Self::Document => 160.0,
            Self::Square => 150.0,
            Self::Custom => 96.0,
        }
    }
}

/// Where the series are named.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegendPlace {
    /// Each line named at its right end; a chart without lines takes a box at the
    /// top right.
    LineEnds,
    TopRight,
    TopLeft,
    BottomRight,
    BottomLeft,
    Off,
}

impl LegendPlace {
    pub const ALL: [Self; 6] = [
        Self::LineEnds,
        Self::TopRight,
        Self::TopLeft,
        Self::BottomRight,
        Self::BottomLeft,
        Self::Off,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::LineEnds => "Line ends",
            Self::TopRight => "Top right",
            Self::TopLeft => "Top left",
            Self::BottomRight => "Bottom right",
            Self::BottomLeft => "Bottom left",
            Self::Off => "Off",
        }
    }
}

/// An sRGB color.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Rgb(pub u8, pub u8, pub u8);

impl Rgb {
    fn hex(self) -> String {
        format!("#{:02x}{:02x}{:02x}", self.0, self.1, self.2)
    }

    /// A ratatui color as sRGB: true colors as they are, the 256 and 16 colors as
    /// xterm draws them. `None` for the terminal's own default.
    pub fn of(color: ratatui::style::Color) -> Option<Self> {
        use ratatui::style::Color;
        const ANSI: [(u8, u8, u8); 16] = [
            (0, 0, 0),
            (205, 0, 0),
            (0, 205, 0),
            (205, 205, 0),
            (0, 0, 238),
            (205, 0, 205),
            (0, 205, 205),
            (229, 229, 229),
            (127, 127, 127),
            (255, 0, 0),
            (0, 255, 0),
            (255, 255, 0),
            (92, 92, 255),
            (255, 0, 255),
            (0, 255, 255),
            (255, 255, 255),
        ];
        let ansi = |i: usize| {
            let (r, g, b) = ANSI[i];
            Some(Rgb(r, g, b))
        };
        match color {
            Color::Rgb(r, g, b) => Some(Rgb(r, g, b)),
            Color::Reset => None,
            Color::Black => ansi(0),
            Color::Red => ansi(1),
            Color::Green => ansi(2),
            Color::Yellow => ansi(3),
            Color::Blue => ansi(4),
            Color::Magenta => ansi(5),
            Color::Cyan => ansi(6),
            Color::Gray => ansi(7),
            Color::DarkGray => ansi(8),
            Color::LightRed => ansi(9),
            Color::LightGreen => ansi(10),
            Color::LightYellow => ansi(11),
            Color::LightBlue => ansi(12),
            Color::LightMagenta => ansi(13),
            Color::LightCyan => ansi(14),
            Color::White => ansi(15),
            Color::Indexed(i) if i < 16 => ansi(i as usize),
            Color::Indexed(i) if i < 232 => {
                let i = i - 16;
                let level = |v: u8| if v == 0 { 0 } else { 55 + v * 40 };
                Some(Rgb(level(i / 36), level((i / 6) % 6), level(i % 6)))
            }
            Color::Indexed(i) => {
                let v = 8 + (i - 232) * 10;
                Some(Rgb(v, v, v))
            }
        }
    }
}

/// The colors a figure is drawn in.
#[derive(Debug, Clone, PartialEq)]
pub struct Palette {
    /// `None` leaves the background transparent.
    pub background: Option<Rgb>,
    pub text: Rgb,
    pub text_secondary: Rgb,
    pub grid: Rgb,
    pub series: [Rgb; 7],
    /// Other: the rows of every value of a color without a series of its own.
    pub other: Rgb,
    /// A heatmap's cells, from the fewest rows to the most.
    pub ramp: [Rgb; 7],
    /// Whether the background is dark: the ramp then starts dark.
    pub dark: bool,
}

/// The light style's series colors: a categorical order whose neighbors stay
/// apart under the common color-vision deficiencies (checked with a CVD
/// simulation: worst adjacent pair 9.1 ΔE). Three of them sit under 3:1 on white,
/// so lines are named at their ends and bars carry a legend.
const LIGHT_SERIES: [Rgb; 7] = [
    Rgb(0x2a, 0x78, 0xd6),
    Rgb(0xeb, 0x68, 0x34),
    Rgb(0x1b, 0xaf, 0x7a),
    Rgb(0xed, 0xa1, 0x00),
    Rgb(0xe8, 0x7b, 0xa4),
    Rgb(0x00, 0x83, 0x00),
    Rgb(0x4a, 0x3a, 0xa7),
];

/// One blue, light to dark.
const BLUE_RAMP: [Rgb; 7] = [
    Rgb(0xcd, 0xe2, 0xfb),
    Rgb(0x9e, 0xc5, 0xf4),
    Rgb(0x6d, 0xa7, 0xec),
    Rgb(0x39, 0x87, 0xe5),
    Rgb(0x25, 0x6a, 0xbf),
    Rgb(0x18, 0x4f, 0x95),
    Rgb(0x0d, 0x36, 0x6b),
];

impl Palette {
    pub fn light() -> Self {
        Self {
            background: Some(Rgb(0xff, 0xff, 0xff)),
            text: Rgb(0x1f, 0x24, 0x30),
            text_secondary: Rgb(0x5b, 0x61, 0x70),
            grid: Rgb(0xe3, 0xe5, 0xea),
            series: LIGHT_SERIES,
            other: Rgb(0xa8, 0xad, 0xb8),
            ramp: BLUE_RAMP,
            dark: false,
        }
    }

    pub fn transparent() -> Self {
        Self {
            background: None,
            ..Self::light()
        }
    }

    /// The terminal theme's colors as configured (before any terminal's fewer
    /// colors): its background (or a dark surface where the theme leaves it to the
    /// terminal), text, grid and chart series.
    pub fn dark(colors: &crate::config::ColorConfig) -> Self {
        let parser = crate::config::ColorParser::new();
        // A hex value as written, whatever the terminal shows of it; a name or an
        // index as xterm draws it.
        let get = |value: &str, fallback: Rgb| {
            let value = value.trim();
            let hex = value
                .strip_prefix('#')
                .filter(|h| h.len() == 6)
                .and_then(|h| u32::from_str_radix(h, 16).ok())
                .map(|n| Rgb((n >> 16) as u8, (n >> 8) as u8, n as u8));
            hex.or_else(|| parser.parse(value).ok().and_then(Rgb::of))
                .unwrap_or(fallback)
        };
        let defaults = [
            Rgb(0x7d, 0xcf, 0xff),
            Rgb(0xbb, 0x9a, 0xf7),
            Rgb(0x9e, 0xce, 0x6a),
            Rgb(0xe0, 0xaf, 0x68),
            Rgb(0x7a, 0xa2, 0xf7),
            Rgb(0xf7, 0x76, 0x8e),
            Rgb(0xff, 0x9e, 0x64),
        ];
        let configured = [
            &colors.chart_1,
            &colors.chart_2,
            &colors.chart_3,
            &colors.chart_4,
            &colors.chart_5,
            &colors.chart_6,
            &colors.chart_7,
        ];
        let mut series = defaults;
        for (slot, value) in series.iter_mut().zip(configured) {
            *slot = get(value, *slot);
        }
        let mut ramp = BLUE_RAMP;
        ramp.reverse();
        Self {
            background: Some(get(&colors.background, Rgb(0x1a, 0x1b, 0x26))),
            text: get(&colors.text_primary, Rgb(0xc0, 0xca, 0xf5)),
            text_secondary: get(&colors.text_secondary, Rgb(0x9a, 0xa5, 0xce)),
            grid: get(&colors.chart_grid, Rgb(0x3d, 0x47, 0x85)),
            series,
            other: get(&colors.dimmed, Rgb(0x56, 0x5f, 0x89)),
            ramp,
            dark: true,
        }
    }

    pub fn for_style(style: ExportStyle, colors: &crate::config::ColorConfig) -> Self {
        match style {
            ExportStyle::Light => Self::light(),
            ExportStyle::Dark => Self::dark(colors),
            ExportStyle::Transparent => Self::transparent(),
        }
    }
}

/// How an export is made: its size, colors, legend and words.
#[derive(Debug, Clone, PartialEq)]
pub struct ExportOptions {
    pub width: u32,
    pub height: u32,
    pub dpi: f32,
    pub palette: Palette,
    pub legend: LegendPlace,
    pub title: String,
    pub description: String,
    pub notes: String,
    pub source: String,
    pub byline: String,
}

impl Default for ExportOptions {
    fn default() -> Self {
        let preset = SizePreset::Document;
        let (width, height) = preset.size().unwrap_or((1600, 1000));
        Self {
            width,
            height,
            dpi: preset.dpi(),
            palette: Palette::light(),
            legend: LegendPlace::LineEnds,
            title: String::new(),
            description: String::new(),
            notes: String::new(),
            source: String::new(),
            byline: String::new(),
        }
    }
}

/// What a chart export asks for: where, in what format, how.
#[derive(Debug, Clone)]
pub struct ChartExportRequest {
    pub path: std::path::PathBuf,
    pub format: ChartExportFormat,
    pub options: ExportOptions,
    /// Whether an existing file may be replaced (asked before the export started).
    pub overwrite: crate::output_file::Overwrite,
}

/// One axis: its title, what its numbers are, and whether they are dates.
#[derive(Debug, Clone, Default)]
pub struct Axis {
    pub title: String,
    pub numbers: AxisNumbers,
    pub kind: XAxisTemporalKind,
    /// Values are `ln(1 + y)`; ticks name `y`.
    pub log: bool,
}

/// One line or scatter series.
#[derive(Debug, Clone)]
pub struct Series {
    pub name: String,
    pub points: Vec<(f64, f64)>,
    /// Where a line starts again after a gap (see `chart_data::segments`).
    pub breaks: Vec<usize>,
    /// Other: every value of a color without a series of its own.
    pub other: bool,
}

/// What a figure plots.
#[derive(Debug, Clone)]
pub enum Plot {
    Lines {
        series: Vec<Series>,
        scatter: bool,
        x: Axis,
        y: Axis,
        y_from_zero: bool,
    },
    Bars {
        data: BarData,
        value: Axis,
    },
    Histogram {
        data: HistogramData,
        x: Axis,
        y: Axis,
    },
    Kde {
        data: KdeData,
        x: Axis,
        y: Axis,
    },
    Box {
        data: BoxPlotData,
        x_title: String,
        y: Axis,
    },
    Heatmap {
        data: HeatmapData,
        x: Axis,
        y: Axis,
    },
}

/// A chart ready to draw: what it plots, and what it says about its rows (a
/// sample, values a range left out).
#[derive(Debug, Clone)]
pub struct Figure {
    pub plot: Plot,
    pub chart_notes: Vec<String>,
    pub grid: bool,
}

/// The figure in `format`, as the bytes of the file.
pub fn render(
    figure: &Figure,
    options: &ExportOptions,
    format: ChartExportFormat,
) -> Result<Vec<u8>> {
    let tree = tree(&svg(figure, options)?)?;
    Ok(match format {
        // usvg writes the size in pixels; the page's inches say how large it prints.
        ChartExportFormat::Svg => tree
            .to_string(&usvg::WriteOptions::default())
            .replacen(
                &format!("width=\"{}\" height=\"{}\"", options.width, options.height),
                &format!(
                    "width=\"{:.3}in\" height=\"{:.3}in\" viewBox=\"0 0 {} {}\"",
                    f64::from(options.width) / f64::from(options.dpi),
                    f64::from(options.height) / f64::from(options.dpi),
                    options.width,
                    options.height,
                ),
                1,
            )
            .into_bytes(),
        ChartExportFormat::Png => {
            let mut pixmap = tiny_skia::Pixmap::new(options.width, options.height)
                .ok_or_else(|| color_eyre::eyre::eyre!("cannot draw a chart of that size"))?;
            resvg::render(
                &tree,
                tiny_skia::Transform::identity(),
                &mut pixmap.as_mut(),
            );
            let png = pixmap
                .encode_png()
                .map_err(|e| color_eyre::eyre::eyre!("PNG: {e}"))?;
            with_resolution(png, options.dpi)
        }
        ChartExportFormat::Pdf => {
            crate::chart_pdf::write(&tree, options.width, options.height, options.dpi)?
        }
    })
}

/// `png` with its resolution recorded (a `pHYs` chunk after `IHDR`), so a column
/// figure prints at its inches.
fn with_resolution(mut png: Vec<u8>, dpi: f32) -> Vec<u8> {
    // The signature, then IHDR: its length, type, 13 bytes and checksum.
    const AFTER_IHDR: usize = 8 + 4 + 4 + 13 + 4;
    if png.len() < AFTER_IHDR || &png[12..16] != b"IHDR" {
        return png;
    }
    let per_meter = (f64::from(dpi) / 0.0254).round() as u32;
    let mut chunk = b"pHYs".to_vec();
    chunk.extend_from_slice(&per_meter.to_be_bytes());
    chunk.extend_from_slice(&per_meter.to_be_bytes());
    chunk.push(1); // the unit is the meter
    let crc = crc::Crc::<u32>::new(&crc::CRC_32_ISO_HDLC).checksum(&chunk);
    let mut bytes = 9u32.to_be_bytes().to_vec();
    bytes.extend_from_slice(&chunk);
    bytes.extend_from_slice(&crc.to_be_bytes());
    png.splice(AFTER_IHDR..AFTER_IHDR, bytes);
    png
}

/// The fonts an export draws with: the bundled family, then the system's for what it
/// lacks. Read once.
fn fonts() -> Arc<usvg::fontdb::Database> {
    static FONTS: OnceLock<Arc<usvg::fontdb::Database>> = OnceLock::new();
    FONTS
        .get_or_init(|| {
            let mut db = usvg::fontdb::Database::new();
            db.load_font_data(FONT_REGULAR.to_vec());
            db.load_font_data(FONT_SEMIBOLD.to_vec());
            db.load_system_fonts();
            db.set_sans_serif_family(FONT_FAMILY);
            Arc::new(db)
        })
        .clone()
}

/// The SVG read with the bundled fonts.
fn tree(svg: &str) -> Result<usvg::Tree> {
    let options = usvg::Options {
        font_family: FONT_FAMILY.to_string(),
        fontdb: fonts(),
        ..Default::default()
    };
    usvg::Tree::from_str(svg, &options).map_err(|e| color_eyre::eyre::eyre!("chart SVG: {e}"))
}

/// Text in SVG.
fn esc(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '"' => out.push_str("&quot;"),
            // Control characters are not allowed in XML.
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

/// About how wide `text` sets at `size` px: the bundled face's average advance.
fn text_width(text: &str, size: f64) -> f64 {
    text.chars()
        .map(|c| match c {
            'i' | 'l' | 'j' | '.' | ',' | ':' | ';' | '\'' | '|' | '!' | ' ' => 0.3,
            'm' | 'w' | 'M' | 'W' => 0.85,
            c if c.is_ascii_uppercase() || c.is_ascii_digit() => 0.62,
            c if c.is_ascii() => 0.53,
            // Wide scripts set about square.
            _ => 0.95,
        })
        .sum::<f64>()
        * size
}

/// `text` broken into lines no wider than `width` at `size`, at spaces.
fn wrap(text: &str, size: f64, width: f64) -> Vec<String> {
    let mut lines = Vec::new();
    for paragraph in text.lines() {
        let mut line = String::new();
        for word in paragraph.split_whitespace() {
            let candidate = if line.is_empty() {
                word.to_string()
            } else {
                format!("{line} {word}")
            };
            if !line.is_empty() && text_width(&candidate, size) > width {
                lines.push(std::mem::take(&mut line));
                line = word.to_string();
            } else {
                line = candidate;
            }
        }
        if !line.is_empty() {
            lines.push(line);
        }
    }
    lines
}

/// The SVG being written, with the sizes it is set in.
struct Canvas<'a> {
    out: String,
    palette: &'a Palette,
    /// Pixels per point.
    pt: f64,
    /// Body text size in px.
    body: f64,
    /// Where Other is among the series, drawn in the palette's `other`.
    other: Option<usize>,
}

impl Canvas<'_> {
    /// `text` at `(x, y)`, anchored `start`, `middle` or `end`, in `weight`.
    fn text(
        &mut self,
        (x, y): (f64, f64),
        size: f64,
        color: Rgb,
        (anchor, weight): (&str, u16),
        text: &str,
    ) {
        self.out.push_str(&format!(
            "<text x=\"{x:.1}\" y=\"{y:.1}\" font-size=\"{size:.1}\" font-weight=\"{weight}\" \
             text-anchor=\"{anchor}\" fill=\"{}\">{}</text>\n",
            color.hex(),
            esc(text)
        ));
    }

    fn line(&mut self, (x1, y1): (f64, f64), (x2, y2): (f64, f64), color: Rgb, width: f64) {
        self.out.push_str(&format!(
            "<line x1=\"{x1:.1}\" y1=\"{y1:.1}\" x2=\"{x2:.1}\" y2=\"{y2:.1}\" stroke=\"{}\" \
             stroke-width=\"{width:.2}\"/>\n",
            color.hex()
        ));
    }

    fn rect(&mut self, x: f64, y: f64, w: f64, h: f64, fill: Rgb, opacity: f64) {
        if w <= 0.0 || h <= 0.0 {
            return;
        }
        self.out.push_str(&format!(
            "<rect x=\"{x:.2}\" y=\"{y:.2}\" width=\"{w:.2}\" height=\"{h:.2}\" fill=\"{}\" \
             fill-opacity=\"{opacity:.2}\"/>\n",
            fill.hex()
        ));
    }

    fn polyline(&mut self, points: &[(f64, f64)], color: Rgb, width: f64) {
        if points.len() < 2 {
            if let Some(&(x, y)) = points.first() {
                self.dot(x, y, width, color);
            }
            return;
        }
        let pts: Vec<String> = points
            .iter()
            .map(|(x, y)| format!("{x:.1},{y:.1}"))
            .collect();
        self.out.push_str(&format!(
            "<polyline points=\"{}\" fill=\"none\" stroke=\"{}\" stroke-width=\"{width:.2}\" \
             stroke-linejoin=\"round\" stroke-linecap=\"round\"/>\n",
            pts.join(" "),
            color.hex()
        ));
    }

    fn dot(&mut self, x: f64, y: f64, r: f64, color: Rgb) {
        self.out.push_str(&format!(
            "<circle cx=\"{x:.1}\" cy=\"{y:.1}\" r=\"{r:.2}\" fill=\"{}\"/>\n",
            color.hex()
        ));
    }

    fn color(&self, i: usize) -> Rgb {
        if self.other == Some(i) {
            return self.palette.other;
        }
        self.palette.series[i % self.palette.series.len()]
    }
}

/// Where the plot is drawn, in px.
#[derive(Clone, Copy, Debug)]
struct Area {
    left: f64,
    top: f64,
    right: f64,
    bottom: f64,
}

impl Area {
    fn width(&self) -> f64 {
        self.right - self.left
    }
    fn height(&self) -> f64 {
        self.bottom - self.top
    }
}

/// A linear scale from data to px.
#[derive(Clone, Copy, Debug)]
struct Scale {
    lo: f64,
    hi: f64,
    from: f64,
    to: f64,
}

impl Scale {
    fn at(&self, v: f64) -> f64 {
        let span = self.hi - self.lo;
        if span.abs() < f64::EPSILON {
            return (self.from + self.to) / 2.0;
        }
        self.from + (v - self.lo) / span * (self.to - self.from)
    }
}

/// An axis's ticks and their labels from `lo` to `hi`, at most `most` of them: on
/// nice numbers, or on calendar boundaries for dates.
fn axis_ticks(lo: f64, hi: f64, axis: &Axis, most: usize) -> Vec<(f64, String)> {
    let most = most.max(2);
    if axis.kind != XAxisTemporalKind::Numeric
        && axis.kind != XAxisTemporalKind::Time
        && let (Some(a), Some(b)) = (
            ticks::to_datetime(lo, axis.kind),
            ticks::to_datetime(hi, axis.kind),
        )
    {
        for step in ticks::calendar_steps(axis.kind) {
            let at = ticks::calendar_ticks(a, b, step);
            if at.is_empty() || at.len() > most {
                continue;
            }
            let labels = ticks::calendar_labels(&at, step.unit, axis.kind, true);
            return at
                .iter()
                .zip(labels)
                .filter_map(|(t, label)| Some((ticks::from_datetime(*t, axis.kind)?, label)))
                .collect();
        }
    }
    let (show_lo, show_hi) = if axis.log {
        (lo.exp_m1(), hi.exp_m1())
    } else {
        (lo, hi)
    };
    let whole = axis.numbers.whole && !axis.log;
    let steps = ticks::nice_steps(show_lo, show_hi, 0.0, whole);
    let step = steps
        .iter()
        .copied()
        .find(|s| (((show_hi - show_lo) / s) as usize) < most)
        .or_else(|| steps.last().copied());
    let values = match step {
        Some(step) => ticks::multiples(show_lo, show_hi, step),
        None => vec![show_lo],
    };
    let format = AxisFormat::new(&values, &axis.numbers);
    values
        .iter()
        .filter_map(|&v| {
            let label = x_axis_label_at(v, axis.kind, (show_lo, show_hi), 0, &format)?;
            let at = if axis.log { v.max(0.0).ln_1p() } else { v };
            Some((at, label))
        })
        .collect()
}

/// `lo..hi` padded so a flat series or a single point still has room.
fn span(lo: f64, hi: f64) -> (f64, f64) {
    if !(lo.is_finite() && hi.is_finite()) {
        return (0.0, 1.0);
    }
    if hi > lo {
        (lo, hi)
    } else {
        (lo - 0.5, hi + 0.5)
    }
}

/// The SVG for `figure`, before its text is set.
pub fn svg(figure: &Figure, options: &ExportOptions) -> Result<String> {
    let (w, h) = (f64::from(options.width), f64::from(options.height));
    if options.width == 0 || options.height == 0 {
        return Err(color_eyre::eyre::eyre!(
            "a chart needs a width and a height"
        ));
    }
    let palette = &options.palette;
    let pt = f64::from(options.dpi) / 72.0;
    // Text size follows the page: small on a journal column, larger on a slide.
    let width_in = w / f64::from(options.dpi);
    let base_pt = (width_in * 1.25).clamp(7.0, 13.0);
    let body = base_pt * pt;
    let mut c = Canvas {
        out: String::new(),
        palette,
        pt,
        body,
        other: None,
    };
    let margin = (body * 2.0).min(w / 10.0);
    if let Some(bg) = palette.background {
        c.rect(0.0, 0.0, w, h, bg, 1.0);
    }

    // Header: title, then the description under it.
    let title_size = body * 1.45;
    let small = body * 0.82;
    let text_width_max = w - 2.0 * margin;
    let mut y = margin;
    for line in wrap(&options.title, title_size, text_width_max) {
        y += title_size;
        c.text((margin, y), title_size, palette.text, ("start", 600), &line);
        y += title_size * 0.25;
    }
    for line in wrap(&options.description, body, text_width_max) {
        y += body * 1.1;
        c.text(
            (margin, y),
            body,
            palette.text_secondary,
            ("start", 400),
            &line,
        );
    }
    if y > margin {
        y += body * 0.9;
    }

    // Footer, from the bottom up: the source and byline, notes, what the chart
    // says about its rows.
    let mut footer: Vec<(String, Rgb)> = Vec::new();
    if !figure.chart_notes.is_empty() {
        footer.push((figure.chart_notes.join(" · "), palette.text_secondary));
    }
    for line in wrap(&options.notes, small, text_width_max) {
        footer.push((line, palette.text_secondary));
    }
    let mut credit = Vec::new();
    if !options.source.trim().is_empty() {
        credit.push(format!("Source: {}", options.source.trim()));
    }
    if !options.byline.trim().is_empty() {
        credit.push(options.byline.trim().to_string());
    }
    if !credit.is_empty() {
        for line in wrap(&credit.join(" · "), small, text_width_max) {
            footer.push((line, palette.text_secondary));
        }
    }
    let line_h = small * 1.35;
    let footer_top = h - margin - footer.len() as f64 * line_h;
    for (i, (line, color)) in footer.iter().enumerate() {
        let baseline = footer_top + (i as f64 + 1.0) * line_h - small * 0.3;
        c.text((margin, baseline), small, *color, ("start", 400), line);
    }
    let bottom = if footer.is_empty() {
        h - margin
    } else {
        footer_top - body * 0.8
    };
    let frame = Area {
        left: margin,
        top: y,
        right: w - margin,
        bottom,
    };
    if frame.height() < body * 4.0 || frame.width() < body * 6.0 {
        return Err(color_eyre::eyre::eyre!(
            "the chart does not fit at {}x{}: make it larger or the text shorter",
            options.width,
            options.height
        ));
    }
    draw_plot(&mut c, figure, options, frame);
    Ok(format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{w}\" height=\"{h}\" \
         viewBox=\"0 0 {w} {h}\" font-family=\"{FONT_FAMILY}\">\n{}</svg>\n",
        c.out,
    ))
}

/// The names a legend lists, with each one's color index.
fn legend_names(figure: &Figure) -> Vec<String> {
    match &figure.plot {
        Plot::Lines { series, .. } => series.iter().map(|s| s.name.clone()).collect(),
        Plot::Bars { data, .. } => data.groups.clone(),
        Plot::Histogram { data, .. } => data.groups.iter().map(|g| g.name.clone()).collect(),
        Plot::Kde { data, .. } => data.series.iter().map(|s| s.name.clone()).collect(),
        Plot::Box { .. } | Plot::Heatmap { .. } => Vec::new(),
    }
}

/// Where Other is among the figure's series: last, when it has one.
fn other_at(figure: &Figure) -> Option<usize> {
    let (other, n) = match &figure.plot {
        Plot::Lines { series, .. } => return series.iter().position(|s| s.other),
        Plot::Bars { data, .. } => (data.other, data.groups.len()),
        Plot::Histogram { data, .. } => (data.other, data.groups.len()),
        Plot::Kde { data, .. } => (data.other, data.series.len()),
        Plot::Box { .. } | Plot::Heatmap { .. } => (false, 0),
    };
    (other && n > 0).then(|| n - 1)
}

/// The plot in `frame`: axes, grid, marks, and the legend.
fn draw_plot(c: &mut Canvas<'_>, figure: &Figure, options: &ExportOptions, frame: Area) {
    let names = legend_names(figure);
    c.other = other_at(figure);
    let is_lines = matches!(
        figure.plot,
        Plot::Lines { scatter: false, .. } | Plot::Kde { .. }
    );
    // A single series is named by the axis title; two or more get a legend.
    let legend = if names.len() < 2 {
        LegendPlace::Off
    } else if options.legend == LegendPlace::LineEnds && !is_lines {
        LegendPlace::TopRight
    } else {
        options.legend
    };
    let tick = c.body * 0.9;
    let mut frame = frame;
    if legend == LegendPlace::LineEnds {
        let widest = names
            .iter()
            .map(|n| text_width(n, tick))
            .fold(0.0, f64::max);
        frame.right -= (widest + tick).min(frame.width() / 3.0);
    }
    match &figure.plot {
        Plot::Lines {
            series,
            scatter,
            x,
            y,
            y_from_zero,
        } => {
            let all = series.iter().flat_map(|s| s.points.iter());
            let (x_lo, x_hi) = all
                .clone()
                .fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), p| {
                    (a.min(p.0), b.max(p.0))
                });
            let (mut y_lo, y_hi) = all.fold((f64::INFINITY, f64::NEG_INFINITY), |(a, b), p| {
                (a.min(p.1), b.max(p.1))
            });
            if *y_from_zero {
                y_lo = y_lo.min(0.0);
            }
            let (sx, sy, plot) = axes(
                c,
                frame,
                span(x_lo, x_hi),
                span(y_lo, y_hi),
                x,
                y,
                figure.grid,
            );
            let width = 1.5 * c.pt;
            let mut ends = Vec::new();
            // Other first, under the series drawn over it.
            let mut order: Vec<(usize, &Series)> = series.iter().enumerate().collect();
            order.sort_by_key(|(_, s)| !s.other);
            for (i, s) in order {
                let color = c.color(i);
                if *scatter {
                    for &(px, py) in &s.points {
                        c.dot(sx.at(px), sy.at(py), 2.4 * c.pt, color);
                    }
                } else {
                    for run in segments(&s.points, &s.breaks) {
                        let pts: Vec<(f64, f64)> =
                            run.iter().map(|&(px, py)| (sx.at(px), sy.at(py))).collect();
                        c.polyline(&pts, color, width);
                    }
                }
                if let Some(&(px, py)) = s.points.last() {
                    ends.push((sy.at(py), sx.at(px), i));
                }
            }
            if legend == LegendPlace::LineEnds {
                line_end_labels(c, &names, ends, plot);
            }
        }
        Plot::Kde { data, x, y } => {
            let (sx, sy, plot) = axes(
                c,
                frame,
                span(data.x_min, data.x_max),
                (0.0, data.y_max),
                x,
                y,
                figure.grid,
            );
            let mut ends = Vec::new();
            for (i, s) in data.series.iter().enumerate() {
                let pts: Vec<(f64, f64)> = s
                    .points
                    .iter()
                    .map(|&(px, py)| (sx.at(px), sy.at(py)))
                    .collect();
                if let Some(&(px, py)) = pts.last() {
                    ends.push((py, px, i));
                }
                c.polyline(&pts, c.color(i), 1.5 * c.pt);
            }
            if legend == LegendPlace::LineEnds {
                line_end_labels(c, &names, ends, plot);
            }
        }
        Plot::Histogram { data, x, y } => {
            let max = if data.max_count > 0.0 {
                data.max_count
            } else {
                1.0
            };
            let (sx, sy, _) = axes(
                c,
                frame,
                span(data.x_min, data.x_max),
                (0.0, max),
                x,
                y,
                figure.grid,
            );
            let n = data.bins.len().max(1);
            let bin = (data.x_max - data.x_min) / n as f64;
            if data.groups.is_empty() {
                // Filled bars, a hairline of the background between them.
                let gap = 1.0 * c.pt;
                for (i, b) in data.bins.iter().enumerate() {
                    let x0 = sx.at(data.x_min + i as f64 * bin);
                    let x1 = sx.at(data.x_min + (i + 1) as f64 * bin);
                    let top = sy.at(b.count);
                    c.rect(
                        x0 + gap / 2.0,
                        top,
                        (x1 - x0 - gap).max(0.5),
                        sy.at(0.0) - top,
                        c.color(0),
                        1.0,
                    );
                }
            } else {
                // Groups overlaid as step outlines: filled bars would hide each other.
                for (g, group) in data.groups.iter().enumerate() {
                    let mut pts = vec![(sx.at(data.x_min), sy.at(0.0))];
                    for (i, &count) in group.counts.iter().enumerate() {
                        let x0 = sx.at(data.x_min + i as f64 * bin);
                        let x1 = sx.at(data.x_min + (i + 1) as f64 * bin);
                        pts.push((x0, sy.at(count)));
                        pts.push((x1, sy.at(count)));
                    }
                    pts.push((sx.at(data.x_max), sy.at(0.0)));
                    c.polyline(&pts, c.color(g), 1.5 * c.pt);
                }
            }
        }
        Plot::Box { data, x_title, y } => {
            let n = data.stats.len().max(1);
            let x_axis = Axis {
                title: x_title.clone(),
                ..Default::default()
            };
            let (lo, hi) = span(data.y_min, data.y_max);
            let pad = (hi - lo) * 0.04;
            let (_, sy, plot) = category_axes(
                c,
                frame,
                &data
                    .stats
                    .iter()
                    .map(|s| s.name.clone())
                    .collect::<Vec<_>>(),
                (lo - pad, hi + pad),
                &x_axis,
                y,
                figure.grid,
            );
            let slot = plot.width() / n as f64;
            for (i, s) in data.stats.iter().enumerate() {
                let color = c.color(i);
                let mid = plot.left + slot * (i as f64 + 0.5);
                let half = (slot * 0.3).min(c.body * 3.0);
                let stroke = 1.25 * c.pt;
                c.line((mid, sy.at(s.max)), (mid, sy.at(s.q3)), color, stroke);
                c.line((mid, sy.at(s.q1)), (mid, sy.at(s.min)), color, stroke);
                c.line(
                    (mid - half / 2.0, sy.at(s.max)),
                    (mid + half / 2.0, sy.at(s.max)),
                    color,
                    stroke,
                );
                c.line(
                    (mid - half / 2.0, sy.at(s.min)),
                    (mid + half / 2.0, sy.at(s.min)),
                    color,
                    stroke,
                );
                let top = sy.at(s.q3);
                c.rect(mid - half, top, half * 2.0, sy.at(s.q1) - top, color, 0.18);
                c.out.push_str(&format!(
                    "<rect x=\"{:.2}\" y=\"{top:.2}\" width=\"{:.2}\" height=\"{:.2}\" \
                     fill=\"none\" stroke=\"{}\" stroke-width=\"{stroke:.2}\"/>\n",
                    mid - half,
                    half * 2.0,
                    (sy.at(s.q1) - top).max(0.0),
                    color.hex()
                ));
                c.line(
                    (mid - half, sy.at(s.median)),
                    (mid + half, sy.at(s.median)),
                    color,
                    stroke * 2.0,
                );
            }
        }
        Plot::Heatmap { data, x, y } => {
            let (sx, sy, _) = axes(
                c,
                frame,
                span(data.x_min, data.x_max),
                span(data.y_min, data.y_max),
                x,
                y,
                false,
            );
            let xw = (data.x_max - data.x_min) / data.x_bins.max(1) as f64;
            let yh = (data.y_max - data.y_min) / data.y_bins.max(1) as f64;
            let ramp = c.palette.ramp;
            for (yi, row) in data.counts.iter().enumerate() {
                for (xi, &count) in row.iter().enumerate() {
                    if count <= 0.0 || data.max_count <= 0.0 {
                        continue;
                    }
                    let level =
                        ((count / data.max_count) * (ramp.len() - 1) as f64).round() as usize;
                    let x0 = sx.at(data.x_min + xi as f64 * xw);
                    let x1 = sx.at(data.x_min + (xi + 1) as f64 * xw);
                    let y0 = sy.at(data.y_min + (yi + 1) as f64 * yh);
                    let y1 = sy.at(data.y_min + yi as f64 * yh);
                    c.rect(
                        x0,
                        y0,
                        x1 - x0,
                        y1 - y0,
                        ramp[level.min(ramp.len() - 1)],
                        1.0,
                    );
                }
            }
        }
        Plot::Bars { data, value } => draw_bars(c, frame, data, value, figure.grid),
    }
    match legend {
        LegendPlace::Off | LegendPlace::LineEnds => {}
        // Above the plot, on the axis title's row, where it covers nothing.
        LegendPlace::TopRight | LegendPlace::TopLeft => {
            legend_row(c, &names, legend, frame, axis_title(figure), is_lines)
        }
        LegendPlace::BottomRight | LegendPlace::BottomLeft => {
            legend_box(c, &names, legend, frame, is_lines)
        }
    }
}

/// The title written over the plot's left edge: the Y axis's, or a bar chart's
/// category.
fn axis_title(figure: &Figure) -> &str {
    match &figure.plot {
        Plot::Lines { y, .. }
        | Plot::Histogram { y, .. }
        | Plot::Kde { y, .. }
        | Plot::Box { y, .. }
        | Plot::Heatmap { y, .. } => &y.title,
        Plot::Bars { data, .. } => &data.category,
    }
}

/// The legend as one row over the plot: a swatch and a name per series, at the
/// right edge, or after the axis title at the left.
fn legend_row(
    c: &mut Canvas<'_>,
    names: &[String],
    place: LegendPlace,
    frame: Area,
    title: &str,
    lines: bool,
) {
    let tick = c.body * 0.85;
    let swatch = tick * 1.2;
    let gap = tick * 1.2;
    let item = |name: &str| swatch + tick * 0.4 + text_width(name, tick);
    let width: f64 =
        names.iter().map(|n| item(n)).sum::<f64>() + gap * names.len().saturating_sub(1) as f64;
    let mut x = match place {
        LegendPlace::TopLeft => frame.left + text_width(title, c.body * 0.9) + gap * 1.5,
        _ => (frame.right - width).max(frame.left),
    };
    let baseline = frame.top + c.body * 0.9;
    let middle = baseline - tick * 0.35;
    for (i, name) in names.iter().enumerate() {
        let color = c.color(i);
        if lines {
            c.line((x, middle), (x + swatch, middle), color, 1.75 * c.pt);
        } else {
            c.rect(x, middle - tick * 0.35, swatch, tick * 0.7, color, 1.0);
        }
        c.text(
            (x + swatch + tick * 0.4, baseline),
            tick,
            c.palette.text,
            ("start", 400),
            name,
        );
        x += item(name) + gap;
    }
}

/// Axes in `frame` for X over `xs` and Y over `ys`: ticks, labels, the grid, the
/// baseline, and the titles. Returns the scales and the plot's own area.
fn axes(
    c: &mut Canvas<'_>,
    frame: Area,
    xs: (f64, f64),
    ys: (f64, f64),
    x: &Axis,
    y: &Axis,
    grid: bool,
) -> (Scale, Scale, Area) {
    axes_with(c, frame, xs, ys, (x, true), y, grid)
}

/// [`axes`], with X's ticks left out when `x.1` is false (a category axis names its
/// slots itself).
fn axes_with(
    c: &mut Canvas<'_>,
    frame: Area,
    xs: (f64, f64),
    ys: (f64, f64),
    (x, x_ticked): (&Axis, bool),
    y: &Axis,
    grid: bool,
) -> (Scale, Scale, Area) {
    let tick = c.body * 0.9;
    let palette = c.palette.clone();
    // The Y title sits over the axis, not turned on its side.
    let top = frame.top + tick * 2.2;
    let x_title_h = if x.title.is_empty() { 0.0 } else { tick * 1.5 };
    let bottom = frame.bottom - tick * 1.6 - x_title_h;
    let y_ticks = axis_ticks(ys.0, ys.1, y, ((bottom - top) / (tick * 3.0)) as usize);
    let y_label_w = y_ticks
        .iter()
        .map(|(_, l)| text_width(l, tick))
        .fold(0.0, f64::max);
    let plot = Area {
        left: frame.left + y_label_w + tick * 0.8,
        top,
        right: frame.right,
        bottom,
    };
    let sx = Scale {
        lo: xs.0,
        hi: xs.1,
        from: plot.left,
        to: plot.right,
    };
    let sy = Scale {
        lo: ys.0,
        hi: ys.1,
        from: plot.bottom,
        to: plot.top,
    };
    let widest_x = |labels: &[(f64, String)]| {
        labels
            .iter()
            .map(|(_, l)| text_width(l, tick))
            .fold(0.0, f64::max)
    };
    let mut most = (plot.width() / (tick * 6.0)) as usize;
    let mut x_ticks = if x_ticked {
        axis_ticks(xs.0, xs.1, x, most)
    } else {
        Vec::new()
    };
    while x_ticked && most > 2 && widest_x(&x_ticks) * x_ticks.len() as f64 * 1.4 > plot.width() {
        most -= 1;
        x_ticks = axis_ticks(xs.0, xs.1, x, most);
    }
    let hair = 0.6 * c.pt;
    for (v, label) in &y_ticks {
        let py = sy.at(*v);
        if grid {
            c.line((plot.left, py), (plot.right, py), palette.grid, hair);
        }
        c.text(
            (plot.left - tick * 0.5, py + tick * 0.35),
            tick,
            palette.text_secondary,
            ("end", 400),
            label,
        );
    }
    for (v, label) in &x_ticks {
        let px = sx.at(*v);
        if grid {
            c.line((px, plot.top), (px, plot.bottom), palette.grid, hair);
        }
        c.line(
            (px, plot.bottom),
            (px, plot.bottom + tick * 0.35),
            palette.text_secondary,
            hair,
        );
        c.text(
            (px, plot.bottom + tick * 1.35),
            tick,
            palette.text_secondary,
            ("middle", 400),
            label,
        );
    }
    c.line(
        (plot.left, plot.bottom),
        (plot.right, plot.bottom),
        palette.text_secondary,
        hair,
    );
    if !y.title.is_empty() {
        c.text(
            (frame.left, frame.top + tick),
            tick,
            palette.text,
            ("start", 600),
            &y.title,
        );
    }
    if !x.title.is_empty() {
        c.text(
            ((plot.left + plot.right) / 2.0, frame.bottom - tick * 0.2),
            tick,
            palette.text,
            ("middle", 600),
            &x.title,
        );
    }
    (sx, sy, plot)
}

/// Axes with a category on X, one slot per name, and Y as `axes` draws it.
fn category_axes(
    c: &mut Canvas<'_>,
    frame: Area,
    names: &[String],
    ys: (f64, f64),
    x: &Axis,
    y: &Axis,
    grid: bool,
) -> (Scale, Scale, Area) {
    let n = names.len().max(1) as f64;
    // The names go under their slots, in place of ticks.
    let (sx, sy, plot) = axes_with(c, frame, (0.0, n), ys, (x, false), y, grid);
    let tick = c.body * 0.9;
    let slot = plot.width() / n;
    let max_chars = (slot / (tick * 0.55)).max(3.0) as usize;
    for (i, name) in names.iter().enumerate() {
        let label = if name.chars().count() > max_chars {
            let kept: String = name.chars().take(max_chars.saturating_sub(1)).collect();
            format!("{kept}…")
        } else {
            name.clone()
        };
        c.text(
            (
                plot.left + slot * (i as f64 + 0.5),
                plot.bottom + tick * 1.35,
            ),
            tick,
            c.palette.text_secondary,
            ("middle", 400),
            &label,
        );
    }
    (sx, sy, plot)
}

/// Each line named at its right end, in its color, nudged apart so no two names
/// overlap.
fn line_end_labels(
    c: &mut Canvas<'_>,
    names: &[String],
    mut ends: Vec<(f64, f64, usize)>,
    plot: Area,
) {
    let tick = c.body * 0.9;
    ends.sort_by(|a, b| a.0.total_cmp(&b.0));
    let mut last = f64::NEG_INFINITY;
    for (y, _, _) in &mut ends {
        *y = y.max(last + tick * 1.15).max(plot.top);
        last = *y;
    }
    // Pushed past the bottom: shift the whole stack back up.
    if let Some(over) = ends
        .last()
        .map(|(y, _, _)| *y - plot.bottom)
        .filter(|o| *o > 0.0)
    {
        for (y, _, _) in &mut ends {
            *y -= over;
        }
    }
    for (y, _, i) in ends {
        c.text(
            (plot.right + tick * 0.5, y + tick * 0.35),
            tick,
            c.color(i),
            ("start", 600),
            &names[i],
        );
    }
}

/// A legend box in a corner of the plot: a swatch and a name per series, on the
/// background so marks under it do not show through.
fn legend_box(c: &mut Canvas<'_>, names: &[String], place: LegendPlace, frame: Area, lines: bool) {
    let tick = c.body * 0.85;
    let row = tick * 1.4;
    let swatch = tick * 1.2;
    let width = names
        .iter()
        .map(|n| text_width(n, tick))
        .fold(0.0, f64::max)
        + swatch
        + tick * 1.5;
    let height = row * names.len() as f64 + tick * 0.6;
    let pad = tick * 0.6;
    let plot_top = frame.top + tick * 2.4;
    let plot_bottom = frame.bottom - tick * 3.3;
    let (x, y) = match place {
        LegendPlace::TopLeft => (frame.left + tick * 4.0, plot_top + pad),
        LegendPlace::BottomRight => (frame.right - width - pad, plot_bottom - height - pad),
        LegendPlace::BottomLeft => (frame.left + tick * 4.0, plot_bottom - height - pad),
        _ => (frame.right - width - pad, plot_top + pad),
    };
    if let Some(bg) = c.palette.background {
        c.rect(x, y, width, height, bg, 0.9);
    }
    for (i, name) in names.iter().enumerate() {
        let cy = y + tick * 0.3 + row * (i as f64 + 0.5);
        let color = c.color(i);
        if lines {
            c.line((x + pad, cy), (x + pad + swatch, cy), color, 1.75 * c.pt);
        } else {
            c.rect(x + pad, cy - tick * 0.35, swatch, tick * 0.7, color, 1.0);
        }
        c.text(
            (x + pad + swatch + tick * 0.5, cy + tick * 0.35),
            tick,
            c.palette.text,
            ("start", 400),
            name,
        );
    }
}

/// Horizontal bars: a row per category, its name on the left, the value axis under
/// them; split by a color, a thin bar per group in each row.
fn draw_bars(c: &mut Canvas<'_>, frame: Area, data: &BarData, value: &Axis, grid: bool) {
    let tick = c.body * 0.9;
    let palette = c.palette.clone();
    let top = frame.top + tick * 2.2;
    let x_title_h = if value.title.is_empty() {
        0.0
    } else {
        tick * 1.5
    };
    let bottom = frame.bottom - tick * 1.6 - x_title_h;
    let groups = data.groups.len().max(1);
    // Rows no thinner than the text, so a long chart is cut and counted rather
    // than squeezed.
    let row_min = (tick * 1.3).max(tick * 0.5 * groups as f64);
    let fits = (((bottom - top) / row_min) as usize).max(1);
    let mut bars: Vec<&crate::chart_data::Bar> = data.bars.iter().collect();
    let mut more = data.more;
    if bars.len() > fits {
        more += bars.len() - (fits - 1);
        bars.truncate(fits - 1);
    }
    let null = "null".to_string();
    let label_of = |b: &crate::chart_data::Bar| b.label.clone().unwrap_or_else(|| null.clone());
    let more_label = format!("+ {} more", crate::numfmt::group_chrome(more));
    let label_w = bars
        .iter()
        .map(|b| text_width(&label_of(b), tick))
        .chain((more > 0).then(|| text_width(&more_label, tick)))
        .fold(0.0, f64::max)
        .min(frame.width() * 0.35);
    let values = || {
        bars.iter().flat_map(|b| {
            if b.by_group.is_empty() {
                vec![b.value]
            } else {
                b.by_group.iter().flatten().copied().collect()
            }
        })
    };
    let lo = values().fold(0.0_f64, f64::min);
    let hi = values().fold(0.0_f64, f64::max);
    let (lo, hi) = if hi > lo { (lo, hi) } else { (lo, lo + 1.0) };
    let plot = Area {
        left: frame.left + label_w + tick,
        top,
        right: frame.right,
        bottom,
    };
    let sx = Scale {
        lo,
        hi,
        from: plot.left,
        to: plot.right,
    };
    let x_ticks = axis_ticks(lo, hi, value, (plot.width() / (tick * 6.0)) as usize);
    let hair = 0.6 * c.pt;
    for (v, label) in &x_ticks {
        let px = sx.at(*v);
        if grid {
            c.line((px, plot.top), (px, plot.bottom), palette.grid, hair);
        }
        c.text(
            (px, plot.bottom + tick * 1.35),
            tick,
            palette.text_secondary,
            ("middle", 400),
            label,
        );
    }
    if !value.title.is_empty() {
        c.text(
            ((plot.left + plot.right) / 2.0, frame.bottom - tick * 0.2),
            tick,
            palette.text,
            ("middle", 600),
            &value.title,
        );
    }
    c.text(
        (frame.left, frame.top + tick),
        tick,
        palette.text,
        ("start", 600),
        &data.category,
    );
    let rows = bars.len() + usize::from(more > 0);
    let row_h = (plot.height() / rows.max(1) as f64).min(tick * 2.5 * groups as f64);
    let zero = sx.at(0.0);
    let gap = 1.0 * c.pt;
    for (i, bar) in bars.iter().enumerate() {
        let y0 = plot.top + row_h * i as f64;
        let label = label_of(bar);
        let max_chars = (label_w / (tick * 0.5)).max(3.0) as usize;
        let label = if label.chars().count() > max_chars {
            let kept: String = label.chars().take(max_chars.saturating_sub(1)).collect();
            format!("{kept}…")
        } else {
            label
        };
        c.text(
            (plot.left - tick * 0.5, y0 + row_h / 2.0 + tick * 0.35),
            tick,
            palette.text,
            ("end", 400),
            &label,
        );
        let body = row_h * 0.75;
        let pieces: Vec<(usize, f64)> = if bar.by_group.is_empty() {
            vec![(0, bar.value)]
        } else {
            bar.by_group
                .iter()
                .enumerate()
                .filter_map(|(g, v)| v.map(|v| (g, v)))
                .collect()
        };
        let each = body / groups as f64;
        for (g, v) in pieces {
            let slot = if bar.by_group.is_empty() { 0 } else { g };
            let y = y0 + (row_h - body) / 2.0 + each * slot as f64;
            let end = sx.at(v);
            let (x, w) = if end >= zero {
                (zero, end - zero)
            } else {
                (end, zero - end)
            };
            c.rect(
                x,
                y + gap / 2.0,
                w.max(hair),
                (each - gap).max(hair),
                c.color(g),
                1.0,
            );
        }
    }
    if more > 0 {
        let y0 = plot.top + row_h * bars.len() as f64;
        c.text(
            (plot.left - tick * 0.5, y0 + row_h / 2.0 + tick * 0.35),
            tick,
            palette.text_secondary,
            ("end", 400),
            &more_label,
        );
    }
    c.line(
        (zero, plot.top),
        (zero, plot.bottom),
        palette.text_secondary,
        hair,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chart_data::{Bar, BoxPlotStats, HistogramBin, HistogramGroup, OTHER, RowsRead};

    fn lines(names: &[&str]) -> Figure {
        Figure {
            plot: Plot::Lines {
                series: names
                    .iter()
                    .enumerate()
                    .map(|(i, n)| Series {
                        name: n.to_string(),
                        points: (0..10).map(|x| (x as f64, (x * (i + 1)) as f64)).collect(),
                        breaks: Vec::new(),
                        other: *n == OTHER,
                    })
                    .collect(),
                scatter: false,
                x: Axis {
                    title: "x".to_string(),
                    ..Default::default()
                },
                y: Axis {
                    title: "value".to_string(),
                    ..Default::default()
                },
                y_from_zero: false,
            },
            chart_notes: vec!["sample of 1,000 of 50k rows".to_string()],
            grid: true,
        }
    }

    fn options() -> ExportOptions {
        ExportOptions {
            title: "Cumulative return by symbol".to_string(),
            description: "Mean monthly return, compounded".to_string(),
            notes: "Illustrative values".to_string(),
            source: "NYC flights, public domain".to_string(),
            byline: "Chart: datui".to_string(),
            ..Default::default()
        }
    }

    /// Every word of the dialog lands in the file, and lines are named at their ends
    /// by default.
    #[test]
    fn the_svg_carries_the_words_and_names_line_ends() {
        let svg = svg(&lines(&["AAPL", "MSFT"]), &options()).unwrap();
        for text in [
            "Cumulative return by symbol",
            "Mean monthly return, compounded",
            "Illustrative values",
            "Source: NYC flights, public domain · Chart: datui",
            "sample of 1,000 of 50k rows",
            ">AAPL<",
            ">MSFT<",
        ] {
            assert!(svg.contains(text), "{text} missing:\n{svg}");
        }
        assert!(svg.contains("fill=\"#2a78d6\""), "light palette: {svg}");
        assert!(svg.contains("fill=\"#ffffff\""), "a white background");
        roxmltree_ok(&svg);
    }

    /// Other is the legend's last entry, in the palette's neutral color and not a
    /// series color, and a scatter draws it under the rest.
    #[test]
    fn an_export_names_other_last_in_its_neutral_color() {
        let mut figure = lines(&["AAPL", "MSFT", OTHER]);
        if let Plot::Lines { scatter, .. } = &mut figure.plot {
            *scatter = true;
        }
        let svg = svg(
            &figure,
            &ExportOptions {
                legend: LegendPlace::TopRight,
                ..options()
            },
        )
        .unwrap();
        let (aapl, other) = (svg.find(">AAPL<").unwrap(), svg.find(">Other<").unwrap());
        assert!(aapl < other, "Other last: {svg}");
        let grey = format!("fill=\"{}\"", Palette::light().other.hex());
        let third = format!("fill=\"{}\"", Palette::light().series[2].hex());
        assert!(svg.contains(&grey), "{svg}");
        assert!(!svg.contains(&third), "Other takes no series color: {svg}");
        // Its dots come before the first series' dots.
        let blue = format!("fill=\"{}\"", Palette::light().series[0].hex());
        assert!(svg.find(&grey).unwrap() < svg.find(&blue).unwrap(), "{svg}");
        roxmltree_ok(&svg);
    }

    fn roxmltree_ok(svg: &str) {
        tree(svg).expect("usvg reads the SVG");
    }

    /// Legend off draws no names; a box legend draws them once, at the corner.
    #[test]
    fn legend_off_draws_no_legend() {
        let figure = lines(&["AAPL", "MSFT"]);
        let off = svg(
            &figure,
            &ExportOptions {
                legend: LegendPlace::Off,
                ..options()
            },
        )
        .unwrap();
        assert!(!off.contains(">AAPL<") && !off.contains(">MSFT<"), "{off}");
        let boxed = svg(
            &figure,
            &ExportOptions {
                legend: LegendPlace::BottomLeft,
                ..options()
            },
        )
        .unwrap();
        assert_eq!(boxed.matches(">AAPL<").count(), 1);
        // One series is named by its axis.
        let one = svg(&lines(&["AAPL"]), &options()).unwrap();
        assert!(!one.contains(">AAPL<"));
    }

    #[test]
    fn transparent_has_no_background_and_dark_uses_the_theme() {
        let clear = svg(
            &lines(&["a", "b"]),
            &ExportOptions {
                palette: Palette::transparent(),
                ..options()
            },
        )
        .unwrap();
        assert!(!clear.contains("fill=\"#ffffff\" fill-opacity=\"1.00\"/>\n<text"));
        assert!(!clear.contains("width=\"1600.00\""), "no full-page rect");
        let config = crate::config::AppConfig::default();
        let dark = Palette::dark(&config.theme.colors);
        assert_eq!(dark.series[0], Rgb(0x7d, 0xcf, 0xff), "chart_1");
        assert!(dark.dark);
    }

    /// The three formats come out as what they say: a PNG of the size asked, an
    /// SVG whose text is outlines, a PDF.
    #[test]
    fn png_svg_and_pdf_are_what_they_say() {
        let figure = lines(&["AAPL", "MSFT"]);
        let options = ExportOptions {
            width: 600,
            height: 400,
            dpi: 96.0,
            ..options()
        };
        let png = render(&figure, &options, ChartExportFormat::Png).unwrap();
        assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
        // IHDR: width and height, big-endian, at bytes 16..24.
        assert_eq!(&png[16..20], &600u32.to_be_bytes());
        assert_eq!(&png[20..24], &400u32.to_be_bytes());
        // pHYs: 96 dpi is 3,780 pixels a meter.
        assert_eq!(&png[37..41], b"pHYs");
        assert_eq!(&png[41..45], &3780u32.to_be_bytes());
        let decoded = resvg::tiny_skia::Pixmap::decode_png(&png).expect("a valid PNG");
        assert_eq!((decoded.width(), decoded.height()), (600, 400));

        let svg =
            String::from_utf8(render(&figure, &options, ChartExportFormat::Svg).unwrap()).unwrap();
        assert!(svg.starts_with("<svg"), "{svg}");
        assert!(svg.contains("width=\"6.250in\""), "printed size: {svg}");
        assert!(!svg.contains("<text"), "text set as outlines");
        usvg::Tree::from_str(&svg, &usvg::Options::default()).expect("valid SVG");

        let pdf = render(&figure, &options, ChartExportFormat::Pdf).unwrap();
        assert!(pdf.starts_with(b"%PDF-"));
        // 600 x 400 px at 96 dpi: 450 x 300 pt.
        assert!(String::from_utf8_lossy(&pdf).contains("/MediaBox [0 0 450 300]"));
    }

    #[test]
    fn presets_set_sizes() {
        assert_eq!(SizePreset::Slide.size(), Some((1920, 1080)));
        assert_eq!(SizePreset::Document.size(), Some((1600, 1000)));
        assert_eq!(SizePreset::Square.size(), Some((1200, 1200)));
        // 3.5 in and 7 in at 300 dpi.
        let (w, _) = SizePreset::SingleColumn.size().unwrap();
        assert_eq!(
            f64::from(w) / f64::from(SizePreset::SingleColumn.dpi()),
            3.5
        );
        let (w, _) = SizePreset::DoubleColumn.size().unwrap();
        assert_eq!(
            f64::from(w) / f64::from(SizePreset::DoubleColumn.dpi()),
            7.0
        );
        assert_eq!(SizePreset::Custom.size(), None);
    }

    /// Every plot draws into a valid SVG, at the smallest preset too.
    #[test]
    fn every_plot_draws() {
        let rows = RowsRead::default();
        let bars = BarData {
            category: "carrier".to_string(),
            value_column: "mean delay".to_string(),
            bars: vec![
                Bar {
                    label: Some("UA".to_string()),
                    value: 12.0,
                    by_group: vec![Some(5.0), Some(7.0)],
                },
                Bar {
                    label: None,
                    value: -3.0,
                    by_group: vec![Some(-3.0), None],
                },
            ],
            more: 3,
            no_value: 0,
            rows,
            value_dtype: polars::prelude::DataType::Float64,
            counted: None,
            groups: vec!["EWR".to_string(), "Other".to_string()],
            other: true,
            rows_note: None,
        };
        let histogram = HistogramData {
            column: "delay".to_string(),
            bins: (0..4)
                .map(|i| HistogramBin {
                    center: i as f64 + 0.5,
                    count: i as f64,
                })
                .collect(),
            groups: vec![
                HistogramGroup {
                    name: "a".to_string(),
                    counts: vec![0.1, 0.2, 0.3, 0.4],
                },
                HistogramGroup {
                    name: "b".to_string(),
                    counts: vec![0.4, 0.3, 0.2, 0.1],
                },
            ],
            other: true,
            share: true,
            x_min: 0.0,
            x_max: 4.0,
            max_count: 0.4,
            rows,
            clipped: None,
        };
        let boxes = BoxPlotData {
            stats: vec![BoxPlotStats {
                name: "UA".to_string(),
                min: 0.0,
                q1: 1.0,
                median: 2.0,
                q3: 3.0,
                max: 4.0,
            }],
            y_min: 0.0,
            y_max: 4.0,
            rows,
            clipped: None,
            of: 0,
        };
        let heatmap = HeatmapData {
            x_column: "a".to_string(),
            y_column: "b".to_string(),
            x_min: 0.0,
            x_max: 1.0,
            y_min: 0.0,
            y_max: 1.0,
            x_bins: 2,
            y_bins: 2,
            counts: vec![vec![1.0, 2.0], vec![0.0, 4.0]],
            max_count: 4.0,
            rows,
        };
        let plots = [
            Plot::Bars {
                data: bars,
                value: Axis::default(),
            },
            Plot::Histogram {
                data: histogram,
                x: Axis::default(),
                y: Axis::default(),
            },
            Plot::Box {
                data: boxes,
                x_title: "carrier".to_string(),
                y: Axis::default(),
            },
            Plot::Heatmap {
                data: heatmap,
                x: Axis::default(),
                y: Axis::default(),
            },
        ];
        let (w, h) = SizePreset::SingleColumn.size().unwrap();
        for plot in plots {
            let figure = Figure {
                plot,
                chart_notes: Vec::new(),
                grid: true,
            };
            let options = ExportOptions {
                width: w,
                height: h,
                dpi: SizePreset::SingleColumn.dpi(),
                ..options()
            };
            let svg = svg(&figure, &options).unwrap();
            roxmltree_ok(&svg);
        }
    }

    #[test]
    fn dates_tick_on_the_calendar() {
        // 2024-01-01 to 2026-01-01, as days since the epoch.
        let axis = Axis {
            kind: XAxisTemporalKind::Date,
            ..Default::default()
        };
        let ticks = axis_ticks(19723.0, 20454.0, &axis, 6);
        let labels: Vec<&str> = ticks.iter().map(|(_, l)| l.as_str()).collect();
        assert!(labels.contains(&"2025"), "{labels:?}");
        assert!(ticks.len() <= 6);
    }

    #[test]
    fn too_small_says_so() {
        let err = svg(
            &lines(&["a"]),
            &ExportOptions {
                width: 60,
                height: 40,
                dpi: 96.0,
                ..options()
            },
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not fit"), "{err}");
    }
}
