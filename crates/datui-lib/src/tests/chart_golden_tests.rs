//! Charts as the screen draws them and as an export writes them, from one small
//! table through the whole path: the chart key, preparation, the figure and the
//! file. Compared with the files in `golden/charts`; `DATUI_BLESS_GOLDEN=1` writes
//! them anew after a change meant to alter what a chart looks like.

use super::chart_prepare_tests::{open, pump};
use crate::chart_export::{self, ChartExportFormat, ExportOptions};
use crate::chart_modal::{Aggregate, Mark};
use crate::*;
use std::sync::mpsc;

/// One chart: its name and how the panel is set.
struct Scenario {
    name: &'static str,
    set: fn(&mut ChartModal),
}

fn columns(modal: &mut ChartModal, mark: Mark, x: &str, y: &[&str]) {
    modal.set_mark(mark);
    modal.spec.encoding.x.field = Some(x.to_string());
    modal.spec.encoding.y.field = y.iter().map(|y| y.to_string()).collect();
}

const SCENARIOS: &[Scenario] = &[
    Scenario {
        name: "line",
        set: |m| columns(m, Mark::Line, "t", &["a", "b"]),
    },
    Scenario {
        name: "scatter_by_color",
        set: |m| {
            columns(m, Mark::Scatter, "a", &["b"]);
            m.spec.encoding.color.field = Some("g".to_string());
        },
    },
    Scenario {
        name: "line_mean_log",
        set: |m| {
            columns(m, Mark::Line, "n", &["a"]);
            m.spec.encoding.y.aggregate = Aggregate::Mean;
            m.log_scale = true;
        },
    },
    Scenario {
        name: "histogram",
        set: |m| {
            columns(m, Mark::Histogram, "a", &[]);
            m.hist_bins = 8;
        },
    },
    Scenario {
        name: "kde",
        set: |m| columns(m, Mark::Kde, "a", &[]),
    },
    Scenario {
        name: "box",
        set: |m| columns(m, Mark::Box, "g", &["a"]),
    },
    Scenario {
        name: "heatmap",
        set: |m| columns(m, Mark::Heatmap, "a", &["b"]),
    },
    Scenario {
        name: "bar_counts",
        set: |m| columns(m, Mark::Bar, "g", &[]),
    },
    Scenario {
        name: "bar_mean",
        set: |m| {
            columns(m, Mark::Bar, "g", &["a"]);
            m.spec.encoding.y.aggregate = Aggregate::Mean;
        },
    },
];

fn table(dir: &std::path::Path) -> PathBuf {
    let path = dir.join("golden.csv");
    let mut body = String::from("t,a,b,g,n\n");
    for t in 0..300i64 {
        let a = (t * 37 % 101) as f64 / 10.0 + t as f64 / 10.0;
        let b = (t * t % 53) as f64;
        let g = ["p", "q", "r", "s"][(t % 7 % 4) as usize];
        body.push_str(&format!("{t},{a},{b},{g},{}\n", t % 7 + 1));
    }
    std::fs::write(&path, body).unwrap();
    path
}

/// FNV-1a: a stable digest of a binary file, so the goldens stay text.
fn digest(bytes: &[u8]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0100_0000_01b3);
    }
    format!("{hash:016x}")
}

fn golden(name: &str, actual: &str) {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("src/tests/golden/charts")
        .join(name);
    if std::env::var_os("DATUI_BLESS_GOLDEN").is_some() {
        std::fs::write(&path, actual).unwrap();
        return;
    }
    let expected = std::fs::read_to_string(&path)
        .unwrap_or_else(|_| panic!("no golden {name}; run with DATUI_BLESS_GOLDEN=1"));
    if expected != actual {
        let line = expected
            .lines()
            .zip(actual.lines())
            .position(|(e, a)| e != a)
            .unwrap_or(0);
        panic!(
            "{name} differs from its golden at line {}:\n- {}\n+ {}",
            line + 1,
            expected.lines().nth(line).unwrap_or(""),
            actual.lines().nth(line).unwrap_or("")
        );
    }
}

/// Every scenario, on screen at 80x24 and exported to SVG, PNG and PDF, is what it
/// was.
#[test]
fn charts_draw_and_export_as_their_goldens() {
    let dir = tempfile::tempdir().unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), crate::tests::test_runtime());
    open(&mut app, &rx, &tx, table(dir.path()));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Resize(80, 24));
    let options = ExportOptions {
        width: 480,
        height: 320,
        title: "Golden".to_string(),
        ..ExportOptions::default()
    };
    let mut screens = String::new();
    let mut binaries = String::new();
    for scenario in SCENARIOS {
        // As `c` opened it: the settings a scenario changes, back at their defaults.
        let modal = &mut app.chart_modal;
        modal.spec = crate::chart_modal::ChartSpec::default();
        modal.log_scale = false;
        modal.hist_bins = crate::chart_modal::HISTOGRAM_DEFAULT_BINS;
        (scenario.set)(&mut app.chart_modal);
        app.chart_modal.row_limit = None;
        let request = ChartRequest::from_modal(&app.chart_modal).expect(scenario.name);
        app.event(&AppEvent::Resize(80, 24));
        pump(&mut app, &rx, &tx, |a| a.chart_cache.satisfies(&request));
        let area = ratatui::layout::Rect::new(0, 0, 80, 24);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        app.render(area, &mut buf);
        // Every cell's colors and modifiers, in one digest; the footer, which names
        // the temporary file, left out.
        let styles = (0..area.height - 1)
            .flat_map(|y| (0..area.width).map(move |x| (x, y)))
            .map(|(x, y)| format!("{:?}", buf[(x, y)].style()))
            .collect::<String>();
        screens.push_str(&format!(
            "== {} styles {}\n",
            scenario.name,
            digest(styles.as_bytes())
        ));
        for y in 0..area.height - 1 {
            let row: String = (0..area.width).map(|x| buf[(x, y)].symbol()).collect();
            screens.push_str(row.trim_end());
            screens.push('\n');
        }
        let figure = app
            .build_chart_figure()
            .expect(scenario.name)
            .expect(scenario.name);
        golden(
            &format!("{}.svg", scenario.name),
            &chart_export::svg(&figure, &options).unwrap(),
        );
        for format in [ChartExportFormat::Png, ChartExportFormat::Pdf] {
            let bytes = chart_export::render(&figure, &options, format).unwrap();
            binaries.push_str(&format!(
                "{} {} {}\n",
                scenario.name,
                format.extension(),
                digest(&bytes)
            ));
        }
    }
    golden("screen.txt", &screens);
    golden("binary.txt", &binaries);
}
