use super::*;
use ratatui::buffer::Buffer;
use std::borrow::Cow;

/// What a test draws: the data as the chart cache holds it, with the axes' numbers.
/// Turned into the chart view's plot under the modal it is drawn with, so the titles
/// are the view's own.
enum Draw<'a> {
    XY {
        /// As drawn: logged on a log scale.
        series: Option<&'a Vec<Vec<(f64, f64)>>>,
        breaks: Option<&'a Vec<Vec<usize>>>,
        values: Option<&'a Vec<Vec<(f64, f64)>>>,
        names: &'a [String],
        x_axis_kind: XAxisTemporalKind,
        x_bounds: Option<(f64, f64)>,
        numbers: PlotNumbers,
        other: bool,
    },
    Histogram {
        data: Option<&'a HistogramData>,
        x: AxisNumbers,
    },
    BoxPlot {
        data: Option<&'a BoxPlotData>,
        y: AxisNumbers,
    },
    Kde {
        data: Option<&'a KdeData>,
        x: AxisNumbers,
    },
    Heatmap {
        data: Option<&'a HeatmapData>,
        numbers: PlotNumbers,
    },
    Bar {
        data: Option<&'a BarData>,
    },
}

#[derive(Default)]
struct PlotNumbers {
    x: AxisNumbers,
    y: AxisNumbers,
}

impl<'a> Draw<'a> {
    fn plot(self, modal: &ChartModal, ctx: &RenderContext) -> Option<Plot<'a>> {
        use crate::chart_jobs::{ChartPrepared, PlotContext};
        // The data says what kind of chart it is, whatever the modal's type.
        let mut spec = modal.effective_spec();
        spec.mark = match &self {
            Draw::XY { .. } if spec.mark == Mark::Scatter => Mark::Scatter,
            Draw::XY { .. } => Mark::Line,
            Draw::Histogram { .. } => Mark::Histogram,
            Draw::BoxPlot { .. } => Mark::Box,
            Draw::Kde { .. } => Mark::Kde,
            Draw::Heatmap { .. } => Mark::Heatmap,
            Draw::Bar { .. } => Mark::Bar,
        };
        let plot = |prepared: Option<ChartPrepared>| {
            crate::chart_jobs::plot(
                prepared.as_ref(),
                &PlotContext {
                    modal,
                    spec: &spec,
                    numbers: &ctx.number_format,
                    schema: None,
                },
            )
            .map(Plot::into_owned)
        };
        match self {
            Draw::XY {
                series,
                breaks,
                values,
                names,
                x_axis_kind,
                x_bounds,
                numbers,
                other,
            } => {
                let Some(Plot::Lines(lines)) = plot(None) else {
                    unreachable!("a line or scatter modal plots lines")
                };
                let series = series.map_or(Cow::Owned(Vec::new()), |s| Cow::Borrowed(&s[..]));
                Some(Plot::Lines(Lines {
                    values: values.map_or(series.clone(), |v| Cow::Borrowed(&v[..])),
                    series,
                    breaks: breaks.map_or(Cow::Owned(Vec::new()), |b| Cow::Borrowed(&b[..])),
                    names: Cow::Borrowed(names),
                    other,
                    x_bounds,
                    x: Axis {
                        kind: x_axis_kind,
                        numbers: numbers.x,
                        ..lines.x
                    },
                    y: Axis {
                        numbers: numbers.y,
                        ..lines.y
                    },
                    ..lines
                }))
            }
            Draw::Histogram { data, x } => match plot(data.cloned().map(ChartPrepared::Histogram))?
            {
                Plot::Histogram { data, x: axis, y } => Some(Plot::Histogram {
                    data,
                    x: Axis { numbers: x, ..axis },
                    y,
                }),
                _ => None,
            },
            Draw::BoxPlot { data, y } => match plot(data.cloned().map(ChartPrepared::BoxPlot))? {
                Plot::Box {
                    data,
                    x_title,
                    y: axis,
                } => Some(Plot::Box {
                    data,
                    x_title,
                    y: Axis { numbers: y, ..axis },
                }),
                _ => None,
            },
            Draw::Kde { data, x } => match plot(data.cloned().map(ChartPrepared::Kde))? {
                Plot::Kde { data, x: axis, y } => Some(Plot::Kde {
                    data,
                    x: Axis {
                        numbers: x.fractional(),
                        ..axis
                    },
                    y,
                }),
                _ => None,
            },
            Draw::Heatmap { data, numbers } => {
                match plot(data.cloned().map(ChartPrepared::Heatmap))? {
                    Plot::Heatmap { data, x, y } => Some(Plot::Heatmap {
                        data,
                        x: Axis {
                            numbers: numbers.x,
                            ..x
                        },
                        y: Axis {
                            numbers: numbers.y,
                            ..y
                        },
                    }),
                    _ => None,
                }
            }
            Draw::Bar { data } => plot(data.cloned().map(ChartPrepared::Bar)),
        }
    }
}

/// A chart view as a test sets it up.
struct View<'a> {
    data: Draw<'a>,
    notes: Vec<String>,
    error: Option<&'a str>,
    working: Option<Working<'a>>,
    schema: Option<&'a Schema>,
}

fn render_plot(
    area: Rect,
    buf: &mut Buffer,
    modal: &ChartModal,
    theme: &Theme,
    ctx: &RenderContext,
    data: Draw<'_>,
    g: &Glyphs,
) -> Option<PlotPlace> {
    let plot = data.plot(modal, ctx);
    super::render_plot(area, buf, modal, theme, ctx, plot, g)
}

fn render_chart_view(
    area: Rect,
    buf: &mut Buffer,
    modal: &mut ChartModal,
    theme: &Theme,
    ctx: &RenderContext,
    view: View<'_>,
) {
    let plot = view.data.plot(modal, ctx);
    super::render_chart_view(
        area,
        buf,
        modal,
        theme,
        ctx,
        ChartView {
            plot,
            notes: view.notes,
            error: view.error,
            working: view.working,
            schema: view.schema,
        },
    );
}

fn open_modal() -> ChartModal {
    let mut modal = ChartModal::new();
    modal.open(
        crate::chart_modal::ChartColumns {
            numeric: &["price".to_string(), "volume".to_string()],
            datetime: &["date".to_string()],
            bucketable: &["date".to_string()],
            category: &["carrier".to_string()],
        },
        None,
        Some(10_000),
        false,
        1,
    );
    modal
}

/// Series names for test plots: as many as any test draws.
fn names() -> &'static [String] {
    static NAMES: std::sync::OnceLock<Vec<String>> = std::sync::OnceLock::new();
    NAMES.get_or_init(|| {
        ["price", "volume", "c", "d", "e", "f", "g"]
            .iter()
            .map(|s| s.to_string())
            .collect()
    })
}

fn render_rows(modal: &mut ChartModal, width: u16, height: u16) -> Vec<String> {
    render_view(
        modal,
        View {
            data: Draw::XY {
                series: None,
                breaks: None,
                values: None,
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
                numbers: PlotNumbers::default(),
            },
            notes: Vec::new(),
            error: None,
            working: None,
            schema: None,
        },
        width,
        height,
    )
}

/// The panel: every shelf in the same place for every type, then the options;
/// no border inside it.
#[test]
fn the_panel_shows_every_shelf_and_echoes_every_option() {
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("date".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    let rows = render_rows(&mut modal, 100, 30);
    let text = rows.join("\n");
    for row in &rows {
        assert!(
            !row.contains('╭') && !row.contains('╰'),
            "a border inside the panel: {row:?}"
        );
    }
    let line = |label: &str| {
        rows.iter()
            .find(|r| {
                let head: String = r.chars().take(15).collect();
                head.trim_start().trim_start_matches('▎').trim() == label
            })
            .cloned()
            .unwrap_or_else(|| panic!("{label} in {text}"))
    };
    assert!(rows[0].contains("Chart"));
    assert!(line("Type").contains("Line"));
    assert!(line("X").contains("date"));
    assert!(line("Y").contains("price"));
    assert!(line("Color").contains("none"));
    assert!(text.contains("by row"), "the bucket under a date X: {text}");
    assert!(text.contains("Options"));
    assert!(line("Y from zero").contains("off"));
    assert!(line("Rows").contains("Sample 10,000"));
    assert!(line("Aggregate").contains("none"));
    assert!(
        !text.contains("type a size"),
        "the row's keys only under focus"
    );

    // Focused, the line under Rows names its keys; a change waits for Enter.
    modal.focus = ChartFocus::LimitRows;
    modal.view_rows = Some(36_800_000);
    let under = |rows: &[String]| {
        let at = rows
            .iter()
            .position(|r| r.contains("Sample") || r.contains("Every row"));
        rows[at.unwrap() + 1].clone()
    };
    let rows = render_rows(&mut modal, 100, 30);
    assert!(under(&rows).contains("switch · type a size"), "{rows:?}");
    modal.step(ChartFocus::LimitRows, 1);
    let rows = render_rows(&mut modal, 100, 30);
    assert!(
        rows.iter().any(|r| r.contains("Every row (36.8M)")),
        "{rows:?}"
    );
    assert!(under(&rows).contains("Enter to read"), "{rows:?}");
    modal.type_rows('5');
    modal.type_rows('x');
    modal.commit_rows();
    let rows = render_rows(&mut modal, 100, 30);
    assert!(rows.iter().any(|r| r.contains("Sample 5x")), "{rows:?}");
    assert!(under(&rows).contains("not a size"), "{rows:?}");
}

/// With a row's picker open, the picker's line carries the one rail on
/// screen; closed, the row has it back.
#[test]
fn an_open_picker_has_the_only_rail() {
    let g = crate::glyphs::get();
    let rails = |rows: &[String]| {
        rows.iter()
            .map(|row| row.matches(g.rail).count())
            .sum::<usize>()
    };
    let mut modal = open_modal();
    modal.set_mark(Mark::Line);
    modal.focus = ChartFocus::X;
    assert_eq!(rails(&render_rows(&mut modal, 100, 30)), 1);
    modal.open_picker();
    assert!(modal.picker.is_some());
    let rows = render_rows(&mut modal, 100, 30);
    assert_eq!(rails(&rows), 1, "{rows:#?}");
    let x = rows
        .iter()
        .find(|row| row.chars().skip(2).collect::<String>().starts_with("X "))
        .expect("the X row");
    assert!(!x.starts_with(g.rail), "the row gave the rail up: {x:?}");
}

/// The aggregate is a row of its own under Y, labeled, so `none` does not read
/// as a second Y column; focused, it carries the rail and the accent like any
/// row.
#[test]
fn the_aggregate_row_is_labeled() {
    let ctx = RenderContext::for_test();
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let mut modal = open_modal();
    modal.set_mark(Mark::Scatter);
    modal.spec.encoding.x.field = Some("volume".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    modal.focus = ChartFocus::Aggregate;
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    render_chart_view(
        area,
        &mut buf,
        &mut modal,
        &theme,
        &ctx,
        View {
            data: Draw::XY {
                series: None,
                breaks: None,
                values: None,
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
                numbers: PlotNumbers::default(),
            },
            notes: Vec::new(),
            error: None,
            working: None,
            schema: None,
        },
    );
    // Each panel row past the rail gutter: its label column and its value.
    let cells = |y: u16, xs: std::ops::Range<u16>| -> String {
        xs.map(|x| buf[(x, y)].symbol()).collect::<String>()
    };
    let label_end = 2 + LABEL_WIDTH;
    let at = |label: &str| {
        (0..area.height)
            .find(|&y| cells(y, 2..label_end).trim_end() == label)
            .unwrap_or_else(|| panic!("{label} in the panel"))
    };
    let (y, row) = (at("Y"), at("Aggregate"));
    assert_eq!(row, y + 1, "right under Y");
    assert_eq!(cells(row, label_end..SIDEBAR_WIDTH).trim_end(), "none");
    assert!(cells(y, label_end..SIDEBAR_WIDTH).contains("price"));
    assert_eq!(buf[(0, row)].symbol(), crate::glyphs::get().rail);
    assert_eq!(buf[(2, row)].fg, ctx.accent, "the focused label");
}

/// The title row says only how the rows were made; the Y column is named once,
/// at its axis. With nothing to say the row is blank.
#[test]
fn the_title_says_how_and_y_is_named_at_its_axis() {
    let mut modal = open_modal();
    modal.set_mark(Mark::Scatter);
    modal.spec.encoding.x.field = Some("volume".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    let plot = |r: &String| -> String { r.chars().skip(SIDEBAR_WIDTH as usize + 1).collect() };
    let check = |modal: &mut ChartModal, how: &str| {
        let rows = render_rows(modal, 100, 30);
        assert_eq!(plot(&rows[0]).trim(), how, "{:?}", rows[0]);
        let named: Vec<String> = rows
            .iter()
            .map(plot)
            .filter(|r| r.contains("price"))
            .collect();
        assert_eq!(named.len(), 1, "price once, at its axis: {named:#?}");
        assert_eq!(named[0].trim(), "price", "{named:#?}");
    };
    check(&mut modal, "");
    modal.spec.encoding.color.field = Some("carrier".to_string());
    check(&mut modal, "colored by carrier");
    modal.spec.encoding.color.field = None;
    modal.spec.encoding.y.aggregate = Aggregate::Mean;
    check(&mut modal, "mean by volume");
    modal.set_mark(Mark::Line);
    modal.spec.encoding.y.cumulative = Cumulative::Sum;
    check(&mut modal, "by volume, running sum");
}

/// A shelf the type does not use stays, dimmed, with why.
#[test]
fn a_shelf_that_does_not_apply_is_dimmed_not_hidden() {
    let ctx = RenderContext::for_test();
    let mut modal = open_modal();
    modal.set_mark(Mark::Kde);
    modal.spec.encoding.x.field = Some("price".to_string());
    let rows = render_rows(&mut modal, 100, 30);
    let text = rows.join("\n");
    assert!(text.contains("density"), "{text}");
    assert!(text.contains("Bandwidth") && !text.contains("Log scale"));

    modal.set_mark(Mark::Box);
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    render_chart_view(
        area,
        &mut buf,
        &mut modal,
        &theme,
        &ctx,
        View {
            data: Draw::BoxPlot {
                data: None,
                y: AxisNumbers::default(),
            },
            notes: Vec::new(),
            error: None,
            working: None,
            schema: None,
        },
    );
    let row = (0..area.height)
        .find(|&y| {
            (0..14)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
                .trim()
                == "Color"
        })
        .expect("the Color shelf stays");
    assert_eq!(buf[(2, row)].fg, ctx.dimmed, "dimmed");
    let text: String = (0..area.height)
        .map(|y| (0..40).map(|x| buf[(x, y)].symbol()).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(text.contains("same as X"), "{text}");
}

/// The line under Color says how many values Other gathers while it is on: on by
/// default for a scatter, off for a line; ←/→ on the line turn it over.
#[test]
fn the_values_line_counts_other() {
    let mut modal = open_modal();
    modal.set_mark(Mark::Scatter);
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    modal.spec.encoding.color.field = Some("carrier".to_string());
    modal.color_counts = Some(crate::chart_modal::ColorCounts {
        column: "carrier".to_string(),
        values: (0..16)
            .map(|i| (Some(format!("C{i}")), 100 - i as u64))
            .collect(),
    });
    let line = |modal: &mut ChartModal| -> String {
        let rows = render_rows(modal, 100, 30);
        let at = rows.iter().position(|r| r.contains("Color")).unwrap();
        rows[at + 1]
            .chars()
            .take(40)
            .collect::<String>()
            .trim()
            .to_string()
    };
    assert_eq!(line(&mut modal), "top 10 + 6 other");
    modal.step(ChartFocus::ColorValues, 1);
    assert_eq!(line(&mut modal), "top 10 of 16 by rows");
    modal.spec.encoding.color.other = None;
    modal.set_mark(Mark::Line);
    assert_eq!(line(&mut modal), "top 10 of 16 by rows");
    modal.step(ChartFocus::ColorValues, -1);
    assert_eq!(line(&mut modal), "top 10 + 6 other");
    modal.spec.encoding.color.values = vec![Some("C3".to_string()), Some("C9".to_string())];
    assert_eq!(line(&mut modal), "2 picked + 14 other");
}

/// A quantile's percentile sits on the line under Aggregate; first and last say
/// the order they read the rows in, the sort's own words when there is one, and
/// only then read the view sorted.
#[test]
fn the_aggregate_row_says_percentile_and_order() {
    let mut modal = open_modal();
    modal.set_mark(Mark::Line);
    modal.spec.encoding.x.field = Some("volume".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    let panel = |modal: &mut ChartModal| -> Vec<String> {
        render_rows(modal, 100, 30)
            .iter()
            .map(|r| r.chars().take(40).collect::<String>().trim().to_string())
            .collect()
    };
    let g = crate::glyphs::get();
    modal.spec.encoding.y.aggregate = Aggregate::Quantile;
    let rows = panel(&mut modal);
    let at = rows
        .iter()
        .position(|r| r.starts_with("Aggregate"))
        .unwrap();
    assert_eq!(rows[at], "Aggregate    quantile");
    assert_eq!(rows[at + 1], "p90");
    modal.step(ChartFocus::Quantile, -1);
    assert_eq!(panel(&mut modal)[at + 1], "p75");

    modal.spec.encoding.y.aggregate = Aggregate::Last;
    let rows = panel(&mut modal);
    assert_eq!(
        rows[at],
        format!("Aggregate    last {} by row order", g.middot)
    );
    let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
    assert!(!request.sorted);
    modal.row_order = Some(format!("time {}", g.sort_asc));
    let rows = panel(&mut modal);
    assert_eq!(
        rows[at],
        format!("Aggregate    last {} by time {}", g.middot, g.sort_asc)
    );
    let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
    assert!(request.sorted, "first and last read the view sorted");
    modal.spec.encoding.y.aggregate = Aggregate::Mean;
    let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
    assert!(!request.sorted, "a mean needs no order");
}

/// Ten series take ten colors, each its own, and the legend names all ten.
#[test]
fn ten_series_take_ten_colors() {
    let mut modal = open_modal();
    modal.set_mark(Mark::Line);
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    modal.show_legend = true;
    let series: Vec<Vec<(f64, f64)>> = (0..10)
        .map(|s| (0..5).map(|i| (i as f64, (i * 10 + s) as f64)).collect())
        .collect();
    let names: Vec<String> = (0..10).map(|i| format!("s{i}")).collect();
    let ctx = RenderContext::for_test();
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    // Hex all the way, as a true-color terminal shows them.
    let mut theme = theme;
    let config = crate::config::ColorConfig::default();
    let hex = [
        &config.chart_1,
        &config.chart_2,
        &config.chart_3,
        &config.chart_4,
        &config.chart_5,
        &config.chart_6,
        &config.chart_7,
        &config.chart_8,
        &config.chart_9,
        &config.chart_10,
    ];
    for (i, value) in hex.iter().enumerate() {
        let channel = |at: usize| u8::from_str_radix(&value[at..at + 2], 16).unwrap();
        let color = ratatui::style::Color::Rgb(channel(1), channel(3), channel(5));
        theme.colors.insert(format!("chart_{}", i + 1), color);
    }
    assert_eq!(theme.series_colors().len(), 10);
    let area = Rect::new(0, 0, 80, 30);
    let mut buf = Buffer::empty(area);
    render_plot(
        area,
        &mut buf,
        &modal,
        &theme,
        &ctx,
        Draw::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            names: &names,
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            numbers: PlotNumbers::default(),
            other: false,
        },
        crate::glyphs::unicode(),
    );
    let mut swatches = Vec::new();
    for y in 0..area.height {
        let row: String = (0..area.width).map(|x| buf[(x, y)].symbol()).collect();
        if let Some(name) = names.iter().find(|n| row.contains(&format!("█ {n} "))) {
            let x = (0..area.width)
                .find(|&x| buf[(x, y)].symbol() == "█")
                .unwrap();
            swatches.push((name.clone(), buf[(x, y)].fg));
        }
    }
    assert_eq!(swatches.len(), 10, "{swatches:?}");
    let mut colors: Vec<_> = swatches.iter().map(|(_, c)| format!("{c:?}")).collect();
    colors.dedup();
    assert_eq!(colors.len(), 10, "{swatches:?}");
}

/// A 16-color terminal shows the ten slots as fewer colors: the chart draws one
/// series per color, and the line under Color says how many.
#[test]
fn sixteen_colors_cap_the_series() {
    let sixteen = |hex: &str| {
        let channel = |i: usize| u8::from_str_radix(&hex[i..i + 2], 16).unwrap();
        crate::config::rgb_to_basic_ansi(channel(1), channel(3), channel(5))
    };
    let config = crate::config::ColorConfig::default();
    let mut theme =
        crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    for (i, value) in [
        &config.chart_1,
        &config.chart_2,
        &config.chart_3,
        &config.chart_4,
        &config.chart_5,
        &config.chart_6,
        &config.chart_7,
        &config.chart_8,
        &config.chart_9,
        &config.chart_10,
    ]
    .iter()
    .enumerate()
    {
        theme
            .colors
            .insert(format!("chart_{}", i + 1), sixteen(value));
    }
    let colors = theme.series_colors();
    assert!((2..10).contains(&colors.len()), "{colors:?}");
    let mut modal = open_modal();
    modal.series_cap = Some(colors.len());
    modal.set_mark(Mark::Line);
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    modal.spec.encoding.color.field = Some("carrier".to_string());
    modal.color_counts = Some(crate::chart_modal::ColorCounts {
        column: "carrier".to_string(),
        values: (0..16)
            .map(|i| (Some(format!("C{i}")), 100 - i as u64))
            .collect(),
    });
    let rows = render_rows(&mut modal, 100, 30);
    let at = rows.iter().position(|r| r.contains("Color")).unwrap();
    assert!(
        rows[at + 1].contains(&format!("top {} of 16 by rows", colors.len())),
        "{}",
        rows[at + 1]
    );
    let request = crate::chart_jobs::ChartRequest::from_modal(&modal).unwrap();
    assert_eq!(request.series_cap, colors.len());
}

/// Other is the legend's last entry and is drawn in `dimmed`, under the series.
#[test]
fn other_is_the_legends_last_entry() {
    let mut modal = open_modal();
    modal.set_mark(Mark::Scatter);
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    modal.show_legend = true;
    let series: Vec<Vec<(f64, f64)>> = (0..3)
        .map(|s| (0..5).map(|i| (i as f64, (i * 10 + s) as f64)).collect())
        .collect();
    let names: Vec<String> = ["UA", "B6", crate::chart_data::OTHER]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let ctx = RenderContext::for_test();
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let area = Rect::new(0, 0, 60, 24);
    let mut buf = Buffer::empty(area);
    render_plot(
        area,
        &mut buf,
        &modal,
        &theme,
        &ctx,
        Draw::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            names: &names,
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            numbers: PlotNumbers::default(),
            other: true,
        },
        crate::glyphs::unicode(),
    );
    let rows: Vec<String> = crate::tests::buffer_lines(&buf);
    let at = |name: &str| {
        rows.iter()
            .position(|r| r.contains(&format!("█ {name}")))
            .unwrap_or_else(|| panic!("{name}: {rows:#?}"))
    };
    assert_eq!(at("UA") + 1, at("B6"));
    assert_eq!(at("B6") + 1, at("Other"));
    let row = at("Other") as u16;
    let swatch = (0..area.width)
        .find(|&x| buf[(x, row)].symbol() == "█")
        .unwrap();
    assert_eq!(buf[(swatch, row)].fg, theme.get("dimmed"));
}

/// The open Picker drops over the panel under its row, with a checkbox per item
/// where it takes several, and each value's rows beside it.
#[test]
fn the_value_picker_lists_values_by_rows() {
    let g = crate::glyphs::get();
    let mut modal = open_modal();
    modal.spec.encoding.color.field = Some("carrier".to_string());
    modal.color_counts = Some(crate::chart_modal::ColorCounts {
        column: "carrier".to_string(),
        values: vec![(Some("UA".to_string()), 2514), (Some("B6".to_string()), 12)],
    });
    modal.focus = ChartFocus::ColorValues;
    modal.open_picker();
    modal.picker_toggle(); // UA in
    let rows = render_rows(&mut modal, 100, 30);
    let body = rows.join("\n");
    assert!(
        body.contains(&format!("{} UA", g.checkbox_on)) && body.contains("2,514"),
        "{body}"
    );
    assert!(body.contains(&format!("{} B6", g.checkbox_off)), "{body}");
    assert!(body.contains("1 picked"), "{body}");
}

/// An open Picker owns the clicks, drawn or not: the panel's rows take none.
#[test]
fn an_open_picker_with_no_room_still_takes_the_clicks() {
    let mut modal = open_modal();
    modal.focus = ChartFocus::Y;
    modal.open_picker();
    let hits = crate::pointer::recording(|| {
        render_rows(&mut modal, 100, 7);
    });
    assert!(hits.iter().any(|(_, h)| *h == Hit::Picker), "{hits:?}");
}

fn render_view(modal: &mut ChartModal, view: View<'_>, w: u16, h: u16) -> Vec<String> {
    let ctx = RenderContext::for_test();
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
        .expect("default theme colors must resolve");
    let area = Rect::new(0, 0, w, h);
    let mut buf = Buffer::empty(area);
    render_chart_view(area, &mut buf, modal, &theme, &ctx, view);
    (0..h)
        .map(|y| (0..w).map(|x| buf[(x, y)].symbol().to_string()).collect())
        .collect()
}

/// A sampled or clipped chart says so at the right of the title row, at 80x24
/// too; the bottom right holds only the X axis's title.
#[test]
fn notes_sit_at_the_title_rows_right() {
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    let series = vec![vec![(0.0, 1.0), (1.0, 2.0)]];
    let note = "sample of 10,000 of 3.5M rows";
    let rows = render_view(
        &mut modal,
        View {
            data: Draw::XY {
                series: Some(&series),
                breaks: None,
                values: None,
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
                numbers: PlotNumbers::default(),
            },
            notes: vec![note.to_string()],
            error: None,
            working: None,
            schema: None,
        },
        120,
        24,
    );
    assert!(rows[0].trim_end().ends_with(note), "{:?}", rows[0]);
    assert!(rows[23].trim_end().ends_with("price"), "{:?}", rows[23]);
    let canvas = |r: &String| r.chars().skip(42).collect::<String>();
    assert!(
        rows[1..].iter().all(|r| !canvas(r).contains("sample")),
        "{rows:#?}"
    );
}

/// On a narrow canvas the how keeps its room and the notes are cut to the rest,
/// with the ellipsis.
#[test]
fn notes_give_way_to_the_how() {
    let mut modal = open_modal();
    modal.set_mark(Mark::Histogram);
    let notes = [
        "sample of 1,000,000 of 36.8M rows",
        "1,207 values outside p1-p99",
    ];
    let rows = render_view(
        &mut modal,
        View {
            data: Draw::Histogram {
                data: None,
                x: AxisNumbers::default(),
            },
            notes: notes.iter().map(|n| n.to_string()).collect(),
            error: None,
            working: None,
            schema: None,
        },
        100,
        20,
    );
    let g = crate::glyphs::get();
    let title = rows[0].chars().skip(42).collect::<String>();
    assert!(title.starts_with("count per bin  "), "{title:?}");
    assert!(title.contains("sample of 1,000,000"), "{title:?}");
    assert!(title.trim_end().ends_with(g.ellipsis), "{title:?}");
    let canvas = |r: &String| r.chars().skip(42).collect::<String>();
    assert!(
        rows[1..].iter().all(|r| !canvas(r).contains("sample")),
        "{rows:#?}"
    );
}

/// A failed preparation shows its message where the plot would be.
#[test]
fn an_error_replaces_the_plot() {
    let mut modal = open_modal();
    let rows = render_view(
        &mut modal,
        View {
            data: Draw::XY {
                series: None,
                breaks: None,
                values: None,
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
                numbers: PlotNumbers::default(),
            },
            notes: Vec::new(),
            error: Some("column not found: gone"),
            working: None,
            schema: None,
        },
        100,
        24,
    );
    assert!(rows.join("\n").contains("column not found: gone"));
}

/// A line breaks at a gap: nothing is drawn between the runs on either side.
#[test]
fn a_line_does_not_bridge_a_gap() {
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    modal.show_legend = false;
    let series = vec![vec![(0.0, 0.0), (1.0, 0.0), (9.0, 0.0), (10.0, 0.0)]];
    let draw = |modal: &mut ChartModal, breaks: &Vec<Vec<usize>>| {
        render_view(
            modal,
            View {
                data: Draw::XY {
                    series: Some(&series),
                    breaks: Some(breaks),
                    values: None,
                    names: names(),
                    x_axis_kind: XAxisTemporalKind::Numeric,
                    x_bounds: None,
                    other: false,
                    numbers: PlotNumbers::default(),
                },
                notes: Vec::new(),
                error: None,
                working: None,
                schema: None,
            },
            100,
            24,
        )
    };
    let blank = |c: char| c == ' ' || c == '\u{2800}';
    let marks = |rows: &[String]| -> usize {
        rows.iter()
            .map(|r| r.chars().skip(44).filter(|c| !blank(*c)).count())
            .sum()
    };
    let joined = draw(&mut modal, &vec![Vec::new()]);
    let broken = draw(&mut modal, &vec![vec![2]]);
    assert!(
        marks(&broken) < marks(&joined),
        "the run from 1 to 9 is not drawn"
    );
}

fn bar_data(n: usize) -> BarData {
    use crate::chart_data::Bar;
    BarData {
        category: "carrier".to_string(),
        value_column: "delay".to_string(),
        bars: (0..n)
            .map(|i| Bar {
                label: Some(format!("C{i}")),
                value: (n - i) as f64,
                by_group: Vec::new(),
            })
            .collect(),
        more: 0,
        no_value: 0,
        rows: Default::default(),
        value_dtype: polars::prelude::DataType::Float64,
        counted: None,
        groups: Vec::new(),
        other: false,
        rows_note: None,
    }
}

fn render_bars(data: &BarData, w: u16, h: u16) -> Vec<String> {
    let mut modal = open_modal();
    modal.set_mark(Mark::Bar);
    render_view(
        &mut modal,
        View {
            data: Draw::Bar { data: Some(data) },
            notes: Vec::new(),
            error: None,
            working: None,
            schema: None,
        },
        w,
        h,
    )
}

/// A bar per category: its label, its value beside it, and a bar as long as the
/// value, the largest filling the width.
#[test]
fn bars_draw_label_value_and_length() {
    let g = crate::glyphs::get();
    let full = g.bar_eighths[7];
    let data = bar_data(3);
    let rows = render_bars(&data, 100, 24);
    // The canvas starts past the 42-column sidebar.
    let canvas: Vec<String> = rows.iter().map(|r| r.chars().skip(42).collect()).collect();
    assert!(canvas[1].trim_start().starts_with("carrier") && canvas[1].contains("delay"));
    let bar_len = |row: &str| row.matches(full).count();
    let words = |row: &str| row.split_whitespace().take(2).collect::<Vec<_>>().join(" ");
    assert_eq!(words(&canvas[2]), "C0 3.00", "{:?}", canvas[2]);
    assert_eq!(words(&canvas[4]), "C2 1.00", "{:?}", canvas[4]);
    let (a, c) = (bar_len(&canvas[2]), bar_len(&canvas[4]));
    assert!(
        a > 40 && (a as f64 / c as f64 - 3.0).abs() < 0.2,
        "{a} vs {c}"
    );
}

/// More bars than rows: the ones that fit, then a chip counting the rest, which
/// includes those the preparation already left out.
#[test]
fn bars_past_the_height_are_counted_on_a_chip() {
    let mut data = bar_data(50);
    data.more = 200;
    let rows = render_bars(&data, 80, 24);
    let body = rows.join("\n");
    // 23 rows under the tab line: a header, 21 bars, the chip.
    assert!(body.contains("C20 "), "{body}");
    assert!(!body.contains("C21 "), "{body}");
    assert!(rows[23].contains(" + 229 more "), "{:?}", rows[23]);
}

/// A negative value draws left of the zero line; a null category reads as null.
#[test]
fn negative_bars_grow_left_and_null_categories_show() {
    use crate::chart_data::Bar;
    let g = crate::glyphs::get();
    let full = g.bar_eighths[7];
    let mut data = bar_data(0);
    data.bars = vec![
        Bar {
            label: Some("UA".to_string()),
            value: 10.0,
            by_group: Vec::new(),
        },
        Bar {
            label: None,
            value: -10.0,
            by_group: Vec::new(),
        },
    ];
    let rows = render_bars(&data, 100, 24);
    let canvas: Vec<String> = rows.iter().map(|r| r.chars().skip(42).collect()).collect();
    let first = |row: &str| row.find(full).map(|i| row[..i].chars().count());
    let pos = first(&canvas[2]).unwrap();
    let neg = first(&canvas[3]).unwrap();
    assert!(neg < pos, "the negative bar starts left of zero");
    assert!(
        canvas[3].trim_start().starts_with(g.null),
        "{:?}",
        canvas[3]
    );
}

/// A value far smaller than the rest still draws a mark, either side of zero, so
/// no bar that is not zero reads as zero.
#[test]
fn a_tiny_value_still_draws_a_mark() {
    use crate::chart_data::Bar;
    let g = crate::glyphs::get();
    let mut data = bar_data(0);
    data.bars = [
        ("big", 10_000.0),
        ("tiny", 0.1),
        ("zero", 0.0),
        ("neg", -0.1),
    ]
    .into_iter()
    .map(|(label, value)| Bar {
        label: Some(label.to_string()),
        value,
        by_group: Vec::new(),
    })
    .collect();
    let rows = render_bars(&data, 100, 24);
    let canvas: Vec<String> = rows.iter().map(|r| r.chars().skip(42).collect()).collect();
    // The bar zone: the row with its label and value taken out.
    let marks = |row: &str| {
        let mut words = row.split_whitespace();
        let (label, value) = (words.next().unwrap(), words.next().unwrap());
        let zone = row.replacen(label, "", 1).replacen(value, "", 1);
        g.bar_eighths
            .iter()
            .filter(|m| !m.trim().is_empty() && zone.contains(**m))
            .count()
    };
    assert!(marks(&canvas[3]) > 0, "tiny: {:?}", canvas[3]);
    assert_eq!(marks(&canvas[4]), 0, "zero: {:?}", canvas[4]);
    assert!(marks(&canvas[5]) > 0, "neg: {:?}", canvas[5]);
}

fn plot_text(modal: &ChartModal, data: Draw<'_>, g: &Glyphs) -> String {
    plot_text_in(modal, data, g, Rect::new(0, 0, 60, 20))
}

fn plot_text_in(modal: &ChartModal, data: Draw<'_>, g: &Glyphs, area: Rect) -> String {
    plot_text_with(&RenderContext::for_test(), modal, data, g, area)
}

fn plot_text_with(
    ctx: &RenderContext,
    modal: &ChartModal,
    data: Draw<'_>,
    g: &Glyphs,
    area: Rect,
) -> String {
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
        .expect("default theme colors must resolve");
    let mut buf = Buffer::empty(area);
    render_plot(area, &mut buf, modal, &theme, ctx, data, g);
    crate::tests::buffer_text(&buf)
}

/// At 40, 60 and 80 columns, in both glyph sets, every plot's x labels stand a
/// space apart with both ends kept, and its axis titles sit on rows of their own
/// that hold nothing of the plot.
#[test]
fn axis_labels_stand_apart_and_titles_keep_their_rows() {
    /// A plot, its x and y titles, and what its x labels look like.
    type Case<'a> = (
        &'a str,
        Draw<'a>,
        &'a str,
        &'a str,
        &'a dyn Fn(&str) -> bool,
    );
    use crate::chart_data::{HistogramBin, KdeSeries};
    // 2020-01-01 to 2024-12-31, in days since the epoch.
    let dates: Vec<Vec<(f64, f64)>> = vec![
        (0..=100)
            .map(|i| (18262.0 + f64::from(i) * 18.26, f64::from(i % 7) * 1000.0))
            .collect(),
    ];
    let numbers: Vec<Vec<(f64, f64)>> = vec![
        (0..20)
            .map(|i| (i as f64 * 1234.5, (i * i) as f64))
            .collect(),
    ];
    let histogram = HistogramData {
        column: "price".to_string(),
        groups: Vec::new(),
        other: false,
        share: false,
        bins: (0..10)
            .map(|i| HistogramBin {
                center: i as f64 * 250.0 + 125.0,
                count: (1 + i % 4) as f64 * 1000.0,
            })
            .collect(),
        x_min: 0.0,
        x_max: 2500.0,
        max_count: 4000.0,
        rows: Default::default(),
        clipped: None,
    };
    let kde = KdeData {
        other: false,
        series: vec![KdeSeries {
            name: "price".to_string(),
            points: (0..=100)
                .map(|i| {
                    let x = i as f64 * 100.0 - 5000.0;
                    (x, (-x * x / 2e6).exp())
                })
                .collect(),
        }],
        x_min: -5000.0,
        x_max: 5000.0,
        y_max: 1.0,
        rows: Default::default(),
        clipped: None,
    };
    let is_date = |t: &str| {
        [4, 5, 7, 10].contains(&t.len()) && t.chars().all(|c| c.is_ascii_digit() || c == '-')
    };
    let is_number = |t: &str| {
        t.trim_end_matches(['k', 'M', 'G', 'T'])
            .parse::<f64>()
            .is_ok()
    };

    let mut modal = open_modal();
    modal.spec.encoding.y.field = vec!["price".to_string()];
    modal.show_legend = false;
    for columns in [40u16, 60, 80] {
        let area = Rect::new(0, 0, columns - SIDEBAR_WIDTH.min(columns / 2), 18);
        for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
            let cases: [Case; 4] = [
                (
                    "date",
                    Draw::XY {
                        series: Some(&dates),
                        breaks: None,
                        values: None,
                        names: names(),
                        x_axis_kind: XAxisTemporalKind::Date,
                        x_bounds: None,
                        other: false,
                        numbers: PlotNumbers::default(),
                    },
                    "date",
                    "price",
                    &is_date,
                ),
                (
                    "number",
                    Draw::XY {
                        series: Some(&numbers),
                        breaks: None,
                        values: None,
                        names: names(),
                        x_axis_kind: XAxisTemporalKind::Numeric,
                        x_bounds: None,
                        other: false,
                        numbers: PlotNumbers::default(),
                    },
                    "volume",
                    "price",
                    &is_number,
                ),
                (
                    "histogram",
                    Draw::Histogram {
                        data: Some(&histogram),
                        x: AxisNumbers::default(),
                    },
                    "price",
                    "Count",
                    &is_number,
                ),
                (
                    "KDE",
                    Draw::Kde {
                        data: Some(&kde),
                        x: AxisNumbers::default(),
                    },
                    "Value",
                    "Density",
                    &is_number,
                ),
            ];
            for (what, data, x_title, y_title, is_label) in cases {
                modal.spec.encoding.x.field = Some(x_title.to_string());
                let text = plot_text_in(&modal, data, g, area);
                let rows: Vec<&str> = text.lines().collect();
                let what = format!("{what} at {columns} columns:\n{text}");
                let axis = rows
                    .iter()
                    .rposition(|r| r.contains(g.plot.axis.bottom_left))
                    .expect(&what);
                let labels: Vec<&str> = rows[axis + 1].split_whitespace().collect();
                assert!(labels.len() >= 2, "both ends: {what}");
                assert!(labels.iter().all(|l| is_label(l)), "apart: {what}");
                assert_eq!(rows[0].trim_end(), y_title, "y title row: {what}");
                assert_eq!(rows[axis + 2].trim_start(), x_title, "x title row: {what}");
            }
        }
    }
}

/// Every label on an axis in one format, in the table's number style: KDE
/// densities around 0.01 keep one precision all the way up, and under the
/// european format a narrow axis's short form reads `12,5k`.
#[test]
fn an_axis_writes_every_label_in_one_format() {
    use crate::chart_data::KdeSeries;
    use crate::numfmt::NumberFormat;
    let g = crate::glyphs::unicode();
    let y_labels = |text: &str| -> Vec<String> {
        text.lines()
            .filter_map(|r| r.split_once(['│', '┤'])?.0.split_whitespace().next())
            .map(String::from)
            .collect()
    };
    let mut modal = open_modal();
    modal.show_legend = false;

    let kde = KdeData {
        other: false,
        series: vec![KdeSeries {
            name: "price".to_string(),
            points: (0..=100)
                .map(|i| {
                    let x = f64::from(i);
                    (x, 0.0126 * (-(x - 50.0).powi(2) / 400.0).exp())
                })
                .collect(),
        }],
        x_min: 0.0,
        x_max: 100.0,
        y_max: 0.0126,
        rows: Default::default(),
        clipped: None,
    };
    let data = Draw::Kde {
        data: Some(&kde),
        x: AxisNumbers::default(),
    };
    let text = plot_text_in(&modal, data, g, Rect::new(0, 0, 40, 12));
    assert_eq!(y_labels(&text), ["0.02", "0.01", "0.00"], "{text}");

    let european = NumberFormat::preset("european").unwrap();
    let mut ctx = RenderContext::for_test();
    ctx.number_format.format = european.clone();
    modal.spec.encoding.x.field = Some("volume".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    modal.y_starts_at_zero = false;
    let series = vec![vec![(0.0, 12_000.0), (5.0, 12_600.0)]];
    let xy = || Draw::XY {
        series: Some(&series),
        breaks: None,
        values: None,
        names: names(),
        x_axis_kind: XAxisTemporalKind::Numeric,
        x_bounds: None,
        other: false,
        numbers: PlotNumbers {
            x: AxisNumbers::default(),
            y: AxisNumbers {
                format: european.clone(),
                whole: false,
            },
        },
    };
    // 12,000 to 12,600 fits in steps of 200, not out to 13,000 in steps of 500.
    let text = plot_text_with(&ctx, &modal, xy(), g, Rect::new(0, 0, 40, 12));
    assert_eq!(
        y_labels(&text),
        ["12.600", "12.400", "12.200", "12.000"],
        "{text}"
    );
    // Too narrow for those: the short form, each in the same unit and places.
    let text = plot_text_with(&ctx, &modal, xy(), g, Rect::new(0, 0, 16, 12));
    assert_eq!(
        y_labels(&text),
        ["12,6k", "12,4k", "12,2k", "12,0k"],
        "{text}"
    );
}

/// A heatmap's y labels in one format; an integer column's middle label is left
/// out when it falls between two whole numbers, where it read `0` twice.
#[test]
fn heatmap_y_labels_are_whole_where_the_column_is() {
    let heatmap = |(y_min, y_max)| HeatmapData {
        x_column: "price".to_string(),
        y_column: "flag".to_string(),
        x_min: 0.0,
        x_max: 10.0,
        y_min,
        y_max,
        x_bins: 4,
        y_bins: 4,
        counts: vec![vec![1.0; 4]; 4],
        max_count: 1.0,
        rows: Default::default(),
    };
    let y_labels = |text: &str| -> Vec<String> {
        // The plot's rows, each starting with its label if it has one.
        text.lines()
            .filter(|r| r.contains('@'))
            .filter_map(|r| r.split_whitespace().next())
            .filter(|l| !l.starts_with('@'))
            .map(String::from)
            .collect()
    };
    let whole = PlotNumbers {
        x: AxisNumbers::default(),
        y: AxisNumbers {
            whole: true,
            ..AxisNumbers::default()
        },
    };
    let g = crate::glyphs::unicode();
    let modal = open_modal();
    let area = Rect::new(0, 0, 40, 12);
    let data = heatmap((0.0, 1.0));
    let text = plot_text_in(
        &modal,
        Draw::Heatmap {
            data: Some(&data),
            numbers: whole,
        },
        g,
        area,
    );
    assert_eq!(y_labels(&text), ["1", "0"], "{text}");
    let data = heatmap((0.0, 0.0126));
    let text = plot_text_in(
        &modal,
        Draw::Heatmap {
            data: Some(&data),
            numbers: PlotNumbers::default(),
        },
        g,
        area,
    );
    assert_eq!(y_labels(&text), ["0.010", "0.005", "0.000"], "{text}");
}

/// A count, or an integer column, ticks in whole numbers as the table prints
/// them: never `4321.00`, and never a tick between two whole numbers.
#[test]
fn whole_number_axes_tick_whole_in_the_table_format() {
    use crate::chart_data::HistogramBin;
    use crate::numfmt::NumberFormat;
    let thousands = NumberFormat::preset("thousands").unwrap();
    let whole = AxisNumbers {
        format: thousands.clone(),
        whole: true,
    };
    let mut ctx = RenderContext::for_test();
    ctx.number_format.format = thousands.clone();
    let g = crate::glyphs::unicode();
    let area = Rect::new(0, 0, 40, 12);
    // What sits left of the y axis.
    let y_labels = |text: &str| -> Vec<String> {
        text.lines()
            .filter_map(|r| r.split_once(['│', '┤'])?.0.split_whitespace().next())
            .map(String::from)
            .collect()
    };
    let axis_row = |text: &str| {
        let rows: Vec<&str> = text.lines().collect();
        let axis = rows.iter().rposition(|r| r.contains('└')).expect(text);
        rows[axis + 1]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };

    // Counts up the side; an integer column's values, 0 to 7, along the bottom.
    let histogram = HistogramData {
        column: "passengers".to_string(),
        groups: Vec::new(),
        other: false,
        share: false,
        bins: (0..7)
            .map(|i| HistogramBin {
                center: f64::from(i) + 0.5,
                count: [4321.0, 12.0, 900.0][i as usize % 3],
            })
            .collect(),
        x_min: 0.0,
        x_max: 7.0,
        max_count: 4321.0,
        rows: Default::default(),
        clipped: None,
    };
    let data = Draw::Histogram {
        data: Some(&histogram),
        x: whole.clone(),
    };
    let text = plot_text_with(&ctx, &open_modal(), data, g, area);
    // Up to the nice value past the tallest bar, in thousands-grouped whole steps.
    assert_eq!(y_labels(&text), ["5,000", "2,500", "0"], "{text}");
    // Four labels fit, so not the two of the step nearest the spacing.
    assert_eq!(axis_row(&text), "0 2 4 6", "{text}");

    // The same for an XY chart of integer columns.
    let series = vec![vec![(0.0, 3.0), (5.0, 10.0)]];
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("volume".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    modal.show_legend = false;
    let xy = |numbers| Draw::XY {
        series: Some(&series),
        breaks: None,
        values: None,
        names: names(),
        x_axis_kind: XAxisTemporalKind::Numeric,
        x_bounds: None,
        other: false,
        numbers,
    };
    let numbers = PlotNumbers {
        x: whole.clone(),
        y: whole,
    };
    let text = plot_text_with(&ctx, &modal, xy(numbers), g, area);
    // 3 to 10 widens out to whole steps either side.
    assert_eq!(y_labels(&text), ["10", "5", "0"], "{text}");
    assert_eq!(axis_row(&text), "0 2 4", "{text}");
    // A float column ticks at the same nice values, written as exactly as they are.
    let text = plot_text_with(&ctx, &modal, xy(PlotNumbers::default()), g, area);
    assert_eq!(axis_row(&text), "0 2 4", "{text}");
}

/// Under the ASCII set every plot draws ASCII only, and still draws: its marks,
/// its bars, its axes and its legend. The Unicode set keeps its own.
#[test]
fn every_plot_is_ascii_under_the_ascii_set() {
    use crate::chart_data::{BoxPlotStats, HistogramBin, KdeSeries};
    let (ascii, unicode) = (crate::glyphs::ascii(), crate::glyphs::unicode());
    let check = |what: &str, text: &str, marks: &[char]| {
        assert!(text.is_ascii(), "{what}:\n{text}");
        assert!(text.contains("+-"), "{what} has its axis corner:\n{text}");
        for mark in marks {
            assert!(text.contains(*mark), "{what} draws {mark:?}:\n{text}");
        }
    };

    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string(), "volume".to_string()];
    modal.show_legend = true;
    let series = vec![
        (0..20).map(|i| (i as f64, (i * i) as f64)).collect(),
        (0..20)
            .map(|i| (i as f64, 400.0 - (i * i) as f64))
            .collect(),
    ];
    let xy = |series| Draw::XY {
        series,
        breaks: None,
        values: None,
        names: names(),
        x_axis_kind: XAxisTemporalKind::Numeric,
        x_bounds: None,
        other: false,
        numbers: PlotNumbers::default(),
    };
    for (chart_type, mark) in [(Mark::Line, '*'), (Mark::Scatter, 'o')] {
        modal.spec.mark = chart_type;
        let text = plot_text(&modal, xy(Some(&series)), ascii);
        check(chart_type.label(), &text, &[mark]);
        // The legend's swatches, ASCII too.
        assert!(text.contains("# price"), "{text}");
        let text = plot_text(&modal, xy(Some(&series)), unicode);
        assert!(text.contains("█ price"), "{text}");
    }
    // Axes before the data is in.
    check("placeholder", &plot_text(&modal, xy(None), ascii), &[]);

    let histogram = HistogramData {
        column: "price".to_string(),
        groups: Vec::new(),
        other: false,
        share: false,
        bins: (0..10)
            .map(|i| HistogramBin {
                center: i as f64 + 0.5,
                count: (1 + i % 4) as f64,
            })
            .collect(),
        x_min: 0.0,
        x_max: 10.0,
        max_count: 4.0,
        rows: Default::default(),
        clipped: None,
    };
    let data = Draw::Histogram {
        data: Some(&histogram),
        x: AxisNumbers::default(),
    };
    check("histogram", &plot_text(&modal, data, ascii), &['#']);

    let box_plot = BoxPlotData {
        stats: ["price", "volume"]
            .iter()
            .map(|name| BoxPlotStats {
                name: name.to_string(),
                min: 0.0,
                q1: 2.0,
                median: 5.0,
                q3: 7.0,
                max: 10.0,
            })
            .collect(),
        of: 0,
        y_min: 0.0,
        y_max: 10.0,
        rows: Default::default(),
        clipped: None,
    };
    let data = Draw::BoxPlot {
        data: Some(&box_plot),
        y: AxisNumbers::default(),
    };
    check("box plot", &plot_text(&modal, data, ascii), &['o']);

    let kde = KdeData {
        other: false,
        series: vec![KdeSeries {
            name: "price".to_string(),
            points: (0..=100)
                .map(|i| {
                    let x = i as f64 / 10.0 - 5.0;
                    (x, (-x * x / 2.0).exp())
                })
                .collect(),
        }],
        x_min: -5.0,
        x_max: 5.0,
        y_max: 1.0,
        rows: Default::default(),
        clipped: None,
    };
    let data = Draw::Kde {
        data: Some(&kde),
        x: AxisNumbers::default(),
    };
    check("KDE", &plot_text(&modal, data, ascii), &['*']);

    // Bars draw no axes of their own.
    let bars = bar_data(5);
    let text = plot_text(&modal, Draw::Bar { data: Some(&bars) }, ascii);
    assert!(text.is_ascii() && text.contains('#'), "bars:\n{text}");
}

/// The legend reads clean over a full plot, with no frame: a short name's row is
/// blank past the name, not the marks behind it.
#[test]
fn the_legend_hides_the_plot_behind_it() {
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string(), "volume".to_string()];
    modal.show_legend = true;
    // A line up and down in every column fills the plot, legend corner included.
    let series: Vec<Vec<(f64, f64)>> = vec![
        (0..240)
            .map(|i| (i as f64 / 2.0, if i % 2 == 0 { 0.0 } else { 100.0 }))
            .collect();
        2
    ];
    for g in [crate::glyphs::ascii(), crate::glyphs::unicode()] {
        let text = plot_text(
            &modal,
            Draw::XY {
                series: Some(&series),
                breaks: None,
                values: None,
                names: names(),
                x_axis_kind: XAxisTemporalKind::Numeric,
                x_bounds: None,
                other: false,
                numbers: PlotNumbers::default(),
            },
            g,
        );
        let swatch = g.bar_eighths[7];
        let price = format!(" {swatch} price  ");
        let volume = format!(" {swatch} volume ");
        let lines: Vec<&str> = text.lines().collect();
        let at = lines
            .iter()
            .position(|l| l.contains(&price))
            .unwrap_or_else(|| panic!("{text}"));
        assert!(lines[at + 1].contains(&volume), "{text}");
        let corner = g.plot.axis.top_left;
        assert!(!lines[at - 1].contains(corner), "no frame: {text}");
    }
}

/// Ten years of daily dates against a value, as the XY view gets them.
fn decade() -> Vec<Vec<(f64, f64)>> {
    // 2015-01-01 on, in days since the epoch.
    vec![
        (0..3653)
            .map(|i| (16436.0 + f64::from(i), 1000.0 + f64::from(i % 400) * 6.0))
            .collect(),
    ]
}

fn xy_dates(series: &Vec<Vec<(f64, f64)>>) -> Draw<'_> {
    Draw::XY {
        series: Some(series),
        breaks: None,
        values: None,
        names: names(),
        x_axis_kind: XAxisTemporalKind::Date,
        x_bounds: None,
        other: false,
        numbers: PlotNumbers::default(),
    }
}

/// On a 300-column terminal a line chart labels its x axis 15 to 20 times, on
/// calendar boundaries, and its y axis about once per four rows, at round values.
#[test]
fn a_wide_chart_carries_ticks_scaled_to_the_space() {
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("date".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    let series = decade();
    let rows = render_view(
        &mut modal,
        View {
            data: xy_dates(&series),
            notes: Vec::new(),
            error: None,
            working: None,
            schema: None,
        },
        300,
        60,
    );
    // The canvas, right of the sidebar.
    let rows: Vec<String> = rows
        .iter()
        .map(|r| r.chars().skip(SIDEBAR_WIDTH as usize + 2).collect())
        .collect();
    let axis = rows.iter().rposition(|r| r.contains('└')).unwrap();
    let x_labels: Vec<&str> = rows[axis + 1].split_whitespace().collect();
    assert!(
        (15..=20).contains(&x_labels.len()),
        "{} x labels: {x_labels:?}",
        x_labels.len()
    );
    assert!(
        x_labels
            .iter()
            .all(|l| l.len() == 4 || ["Apr", "Jul", "Oct"].contains(l)),
        "{x_labels:?}"
    );
    let y_labels: Vec<f64> = rows[..axis]
        .iter()
        .filter_map(|r| r.split_once('┤')?.0.split_whitespace().last()?.parse().ok())
        .collect();
    let plot_rows = axis - 2; // the tab line and the y title
    let per_label = plot_rows as f64 / y_labels.len() as f64;
    assert!(
        (3.0..=5.0).contains(&per_label),
        "{per_label}: {y_labels:?}"
    );
    let step = y_labels[0] - y_labels[1];
    let mantissa = step / 10f64.powf(step.log10().floor());
    assert!([1.0, 2.0, 2.5, 5.0].contains(&mantissa), "{y_labels:?}");
    assert!(
        y_labels.windows(2).all(|w| w[0] - w[1] == step)
            && y_labels.iter().all(|v| v % step == 0.0),
        "{y_labels:?}"
    );
}

/// The grid shows in the view only while it is on, in the `chart_grid` color.
#[test]
fn the_grid_follows_the_toggle() {
    let g = crate::glyphs::unicode();
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("date".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    let series = decade();
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default())
        .expect("default theme colors must resolve");
    let draw = |modal: &ChartModal| {
        let area = Rect::new(0, 0, 100, 30);
        let mut buf = Buffer::empty(area);
        let ctx = RenderContext::for_test();
        render_plot(area, &mut buf, modal, &theme, &ctx, xy_dates(&series), g);
        buf
    };
    let grid_cells = |buf: &Buffer| {
        buf.content()
            .iter()
            .filter(|c| c.symbol() == g.plot.grid_down || c.symbol() == g.plot.grid_across)
            .collect::<Vec<_>>()
            .len()
    };
    assert_eq!(grid_cells(&draw(&modal)), 0);
    modal.toggle_grid();
    let on = draw(&modal);
    assert!(grid_cells(&on) > 100);
    let grid = theme.get("chart_grid");
    assert!(
        on.content()
            .iter()
            .filter(|c| c.symbol() == g.plot.grid_across)
            .all(|c| c.fg == grid)
    );
}

/// The light theme's grid stays a color under 16 colors, not the white of the
/// background, and the grid draws in it.
#[test]
fn the_light_grid_survives_sixteen_colors() {
    use ratatui::style::Color;
    let g = crate::glyphs::unicode();
    let sixteen = |hex: &str| {
        let channel = |i: usize| u8::from_str_radix(&hex[i..i + 2], 16).unwrap();
        crate::config::rgb_to_basic_ansi(channel(1), channel(3), channel(5))
    };
    let light = crate::config::ColorConfig::light();
    let grid = sixteen(&light.chart_grid);
    assert!(
        !matches!(grid, Color::White | Color::Black | Color::Reset),
        "{grid:?}"
    );
    let dark = sixteen(&crate::config::ColorConfig::default().chart_grid);
    assert!(!matches!(dark, Color::White | Color::Black), "{dark:?}");

    let mut theme =
        crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    theme.colors.insert("chart_grid".to_string(), grid);
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("date".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string()];
    modal.toggle_grid();
    let series = decade();
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    let ctx = RenderContext::for_test();
    render_plot(area, &mut buf, &modal, &theme, &ctx, xy_dates(&series), g);
    let cells: Vec<_> = buf
        .content()
        .iter()
        .filter(|c| c.symbol() == g.plot.grid_across || c.symbol() == g.plot.grid_down)
        .collect();
    assert!(cells.len() > 50);
    assert!(cells.iter().all(|c| c.fg == grid));
}

/// A log scale ticks at the powers of ten, and 2 and 5 between when there is
/// room, every label in one format; the short form names each tick's own k or M.
#[test]
fn log_scale_ticks_fall_on_the_decades() {
    let g = crate::glyphs::unicode();
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("x".to_string());
    modal.spec.encoding.y.field = vec!["count".to_string()];
    modal.log_scale = true;
    // 0 to 300,000, as the view has it: ln(1 + y).
    let linear: Vec<Vec<(f64, f64)>> = vec![
        (0..=300)
            .map(|i| (f64::from(i), f64::from(i).powi(2) * 3.333))
            .collect(),
    ];
    let logged = crate::chart_jobs::log_series(&linear);
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let ctx = RenderContext::for_test();
    let y_labels = |height: u16| {
        let area = Rect::new(0, 0, 60, height);
        let mut buf = Buffer::empty(area);
        let data = Draw::XY {
            series: Some(&logged),
            breaks: None,
            values: Some(&linear),
            names: names(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            other: false,
            numbers: PlotNumbers::default(),
        };
        render_plot(area, &mut buf, &modal, &theme, &ctx, data, g);
        let rows = crate::tests::buffer_lines(&buf);
        let labels: Vec<String> = rows
            .iter()
            .filter_map(|row| {
                let (label, _) = row.split_once(g.plot.tick_y)?;
                let label = label.trim();
                (!label.is_empty()).then(|| label.to_string())
            })
            .collect();
        (labels, rows)
    };
    // Top down: falling, each a 1, 2 or 5 a power of ten up, or zero.
    let nice = |labels: &[String]| {
        let values: Vec<f64> = labels.iter().map(|l| l.parse().unwrap()).collect();
        values.windows(2).all(|w| w[0] > w[1])
            && values.iter().all(|&v| {
                let decade = 10f64.powf(v.log10().floor());
                v == 0.0 || [1.0, 2.0, 5.0].contains(&(v / decade))
            })
    };
    let (tall, rows) = y_labels(40);
    assert!(nice(&tall), "{rows:#?}");
    // Every power of ten up to the top, which the axis widens to.
    for decade in ["0", "10", "100", "1000", "10000", "100000", "1000000"] {
        assert!(tall.contains(&decade.to_string()), "{decade}: {rows:#?}");
    }
    assert_eq!(tall[0], "1000000", "{rows:#?}");
    assert_eq!(tall.last().unwrap(), "0", "{rows:#?}");
    let (short, rows) = y_labels(12);
    assert!(nice(&short), "{rows:#?}");
    assert!(short.len() >= 3 && short.len() < tall.len(), "{rows:#?}");
}

/// With the plot focused the crosshair stands on a point: a line down its column
/// in the accent, and under the plot each series' value there.
#[test]
fn the_crosshair_reads_out_every_series() {
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("date".to_string());
    modal.spec.encoding.y.field = vec!["price".to_string(), "volume".to_string()];
    let series: Vec<Vec<(f64, f64)>> = vec![
        (0..10)
            .map(|i| (19_783.0 + f64::from(i), 1.5 * f64::from(i)))
            .collect(),
        (0..10)
            .filter(|i| *i != 4)
            .map(|i| (19_783.0 + f64::from(i), f64::from(i * 100)))
            .collect(),
    ];
    let theme = crate::config::Theme::from_config(&crate::config::ThemeConfig::default()).unwrap();
    let ctx = RenderContext::for_test();
    let draw = |modal: &ChartModal, g: &Glyphs| {
        let area = Rect::new(0, 0, 60, 20);
        let mut buf = Buffer::empty(area);
        let data = Draw::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            names: names(),
            x_axis_kind: XAxisTemporalKind::Date,
            x_bounds: None,
            other: false,
            numbers: PlotNumbers::default(),
        };
        let place = render_plot(area, &mut buf, modal, &theme, &ctx, data, g);
        (buf, place)
    };
    for g in [crate::glyphs::unicode(), crate::glyphs::ascii()] {
        // The options have the keys: no crosshair, no readout.
        modal.plot_focus = false;
        modal.cursor_x = Some(19_786.0);
        let (off, place) = draw(&modal, g);
        let place = place.expect("an XY plot with points says where it is");
        assert!(!crate::tests::buffer_lines(&off).concat().contains("price:"));

        modal.plot_focus = true;
        let (on, _) = draw(&modal, g);
        let rows = crate::tests::buffer_lines(&on);
        let last = rows.last().unwrap().trim_end();
        assert_eq!(
            last, "date: 2024-03-04   price: 4.5   volume: 300",
            "{rows:#?}"
        );
        let column = place.column(19_786.0);
        let accent = theme.get("accent");
        let marks = (place.graph.top()..place.graph.bottom())
            .filter(|&y| on[(column, y)].symbol() == g.plot.axis.vertical)
            .inspect(|&y| assert_eq!(on[(column, y)].fg, accent))
            .count();
        assert!(marks > 5, "{rows:#?}");

        // A gap in a series reads as one.
        modal.cursor_x = Some(19_787.0);
        let (gap, _) = draw(&modal, g);
        let rows = crate::tests::buffer_lines(&gap);
        assert!(
            rows.last()
                .unwrap()
                .contains(&format!("volume: {}", g.null)),
            "{rows:#?}"
        );
    }
}

/// A scatter of a few points marks each with a whole-cell dot; a dense one
/// switches to braille, which keeps neighbors apart.
#[test]
fn a_scatter_picks_its_marker_by_density() {
    let g = crate::glyphs::unicode();
    let mut modal = open_modal();
    modal.spec.encoding.x.field = Some("price".to_string());
    modal.spec.encoding.y.field = vec!["volume".to_string()];
    modal.spec.mark = Mark::Scatter;
    let draw = |n: usize| {
        let series = vec![
            (0..n)
                .map(|i| (i as f64, ((i * 7919) % 1000) as f64))
                .collect::<Vec<_>>(),
        ];
        let data = Draw::XY {
            series: Some(&series),
            breaks: None,
            values: None,
            names: names(),
            x_axis_kind: XAxisTemporalKind::Numeric,
            x_bounds: None,
            other: false,
            numbers: PlotNumbers::default(),
        };
        plot_text_in(&modal, data, g, Rect::new(0, 0, 60, 20))
    };
    let braille = |text: &str| text.chars().any(|c| ('\u{2801}'..='\u{28ff}').contains(&c));
    let sparse = draw(20);
    assert!(
        sparse.contains(ratatui::symbols::DOT) && !braille(&sparse),
        "{sparse}"
    );
    let dense = draw(2000);
    assert!(
        braille(&dense) && !dense.contains(ratatui::symbols::DOT),
        "{dense}"
    );
}

/// A histogram's bins fill their columns, with a column of air between bins wide
/// enough to spare one.
#[test]
fn histogram_bins_fill_their_columns() {
    use crate::chart_data::HistogramBin;
    let histogram = HistogramData {
        column: "price".to_string(),
        groups: Vec::new(),
        other: false,
        share: false,
        bins: (0..4)
            .map(|i| HistogramBin {
                center: f64::from(i) * 25.0 + 12.5,
                count: 10.0,
            })
            .collect(),
        x_min: 0.0,
        x_max: 100.0,
        max_count: 10.0,
        rows: Default::default(),
        clipped: None,
    };
    let points = bin_columns(&histogram, [0.0, 100.0], 41);
    // Four bins of about ten columns, less a gap after each but the last.
    assert_eq!(points.len(), 41 - 3, "{points:?}");
    let text = plot_text_in(
        &open_modal(),
        Draw::Histogram {
            data: Some(&histogram),
            x: AxisNumbers::default(),
        },
        crate::glyphs::unicode(),
        Rect::new(0, 0, 60, 20),
    );
    let row = text.lines().find(|r| r.contains('█')).expect(&text);
    let bars: Vec<&str> = row.split(' ').filter(|r| r.contains('█')).collect();
    assert_eq!(bars.len(), 4, "{text}");
}

#[test]
fn a_tiny_area_never_panics() {
    for (w, h) in [(0, 0), (3, 2), (10, 4), (20, 6), (60, 20), (80, 24)] {
        let mut modal = open_modal();
        modal.focus = ChartFocus::X;
        modal.open_picker();
        let _ = render_rows(&mut modal, w, h);
        let mut data = bar_data(30);
        data.bars[3].value = -4.0;
        data.bars[4].label = Some("a label longer than the whole canvas is".to_string());
        let _ = render_bars(&data, w, h);
    }
}
