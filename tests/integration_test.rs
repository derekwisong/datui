use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::event_pump::EventPump;
use datui::{App, AppEvent, InputMode, OpenOptions, QueryMode};
use polars::prelude::*;
use ratatui::buffer::Buffer;
use ratatui::layout::Rect;
use ratatui::widgets::Widget;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

#[cfg(feature = "cloud")]
#[path = "cloud/download.rs"]
mod cloud_download;
mod common;
#[cfg(feature = "cloud")]
#[path = "common/fake_s3.rs"]
mod fake_s3;
#[cfg(feature = "cloud")]
#[path = "quality/remote.rs"]
mod remote_quality;

use common::{drain_events, next_event, pump_open_until_loaded, work_pending};

/// Enter on a tool in the Analysis sidebar. A tool with no result yet shows its
/// Sample form in the pane rather than running; the next Enter runs it.
fn show_sample_form(app: &mut App) {
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
}

/// Ticks for a loop that polls the way `run()` does until what it waits for is true.
/// Bounded by a hang guard rather than a count: on a loaded machine a count of ticks
/// runs out before the work does. The guard fails the test instead of ending the loop,
/// so a wait that never came true cannot fall through to asserts that pass anyway.
#[track_caller]
fn ticks() -> impl Iterator<Item = usize> {
    let caller = std::panic::Location::caller();
    let deadline = std::time::Instant::now() + common::HANG_GUARD;
    (0..).inspect(move |_| {
        assert!(
            std::time::Instant::now() < deadline,
            "the wait at {caller} never finished"
        );
    })
}

/// Hand results and their follow-ups back through the channel, as `run()` does, until
/// `done`. Waits on the channel between checks, so a slow machine costs time and
/// never the answer.
#[track_caller]
fn pump_until(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    done: impl Fn(&App) -> bool,
) {
    for _ in ticks() {
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        // As `run()` paints after every update.
        app.frame_painted();
        if done(app) {
            return;
        }
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50))
            && let Some(next) = app.event(&ev)
        {
            let _ = tx.send(next);
        }
    }
}

/// As `pump_open_until_loaded`, but hands back the message a failed open ended with.
///
/// A load that fails reports it as `BackgroundFailed` — the reading happens off the
/// event thread — and only the paths that never get that far crash outright.
fn pump_open_until_error(
    app: &mut App,
    rx: &std::sync::mpsc::Receiver<AppEvent>,
    paths: Vec<PathBuf>,
    options: OpenOptions,
) -> Option<String> {
    let mut next: Option<AppEvent> = Some(AppEvent::Open(paths, options));
    loop {
        match next.take() {
            Some(AppEvent::Crash(message)) => return Some(message),
            Some(AppEvent::BackgroundFailed { message, .. }) => return Some(message),
            Some(ev) => next = app.event(&ev),
            None => next = Some(next_event(app, rx)?),
        }
    }
}

/// The harness handles what is already queued before it calls the app settled.
#[test]
fn test_drain_events_handles_queued_events_first() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('o'),
        KeyModifiers::CONTROL,
    )))
    .unwrap();
    drain_events(&mut app, &rx);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A quiet channel is not completion: the harness waits for the result work owes.
#[test]
fn test_drain_events_waits_for_owed_result() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    app.set_loading_phase("test worker", 0);
    let generation = app.task_generation();
    let worker = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(200));
        tx.send(AppEvent::BackgroundFailed {
            generation,
            job: datui::Job::Load,
            message: "test worker failed".into(),
            panicked: false,
        })
        .unwrap();
    });
    drain_events(&mut app, &rx);
    worker.join().unwrap();
    assert!(!app.is_busy(), "the worker's result was handled");
    assert!(!work_pending(&app));
}

/// A wait that runs out fails the test rather than falling through to the asserts.
#[test]
#[should_panic(expected = "background work never reported back")]
fn test_wait_past_its_guard_fails() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("nothing will answer", 0);
    common::next_event_within(&app, &rx, std::time::Duration::from_millis(1));
}

#[test]
fn test_app_creation() {
    let (tx, _) = mpsc::channel();
    let app = App::new(tx, common::test_runtime());
    assert_eq!(app.input_mode, InputMode::Normal);
}

#[test]
fn test_full_workflow() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    // 1. Create test CSV file inline
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("large_test.csv");

    let mut df = df!(
        "a" => (0..100).collect::<Vec<i32>>(),
        "b" => (0..100).map(|i| format!("text_{}", i)).collect::<Vec<String>>(),
        "c" => (0..100).map(|i| i % 3).collect::<Vec<i32>>(),
        "d" => (0..100).map(|i| i % 5).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    // 2. Open the file (pump full load chain)
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.clone()],
        OpenOptions::default(),
    );

    assert!(app.data_table_state.is_some());
    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.num_rows(), 100);

    // 2. Filter the data (s = Sort & Filter, switch to Filter tab, configure, Apply)
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_event));
    assert!(app.sort_filter_modal.active);

    app.sort_filter_modal.switch_tab(); // Filter tab
    app.sort_filter_modal.filter.available_columns =
        app.data_table_state.as_ref().unwrap().headers();
    let column = app.sort_filter_modal.filter.available_columns[2].clone();
    app.sort_filter_modal
        .filter
        .statements
        .push(datui::filter_modal::FilterStatement {
            column,
            operator: datui::filter_modal::FilterOperator::Eq,
            value: "1".to_string(),
            logical_op: datui::filter_modal::LogicalOperator::And,
        });
    // On the Filters tab Enter means add/edit; Ctrl+Enter is the apply.
    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::CONTROL);
    if let Some(next_event) = app.event(&AppEvent::Key(key_event)) {
        app.event(&next_event);
    }
    drain_events(&mut app, &rx);
    assert!(!app.sort_filter_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    assert_eq!(datatable.lf().clone().collect().unwrap().shape().0, 33);

    // 3. Sort the data (s = Sort & Filter, Sort tab, configure, Apply)
    let key_event = KeyEvent::new(KeyCode::Char('s'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_event));
    assert!(app.sort_filter_modal.active);

    app.sort_filter_modal.sort.columns = app
        .data_table_state
        .as_ref()
        .unwrap()
        .headers()
        .iter()
        .enumerate()
        .map(|(i, h)| datui::sort_modal::SortColumn {
            name: h.clone(),
            sort_order: None,
            sort_descending: false,
            display_order: i,
            is_locked: false,
            is_to_be_locked: false,
            is_visible: true,
        })
        .collect();
    app.sort_filter_modal.sort.table_state.select(Some(0));
    // Space cycles none -> ascending -> descending.
    app.sort_filter_modal.sort.cycle_sort();
    app.sort_filter_modal.sort.cycle_sort();
    app.sort_filter_modal.focus = datui::sort_filter_modal::SortFilterFocus::TabBar;

    let key_event = KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE);
    if let Some(next_event) = app.event(&AppEvent::Key(key_event)) {
        app.event(&next_event);
    }
    drain_events(&mut app, &rx);
    assert!(!app.sort_filter_modal.active);

    let datatable = app.data_table_state.as_ref().unwrap();
    let df = datatable.lf().clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
}

#[test]
fn test_chart_open_and_esc_back() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("chart_integration_test.csv");

    let mut df = df!(
        "x" => (0..10).collect::<Vec<i32>>(),
        "y" => (0..10).map(|i| i * 2).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.clone()],
        OpenOptions::default(),
    );
    assert!(app.data_table_state.is_some());
    assert_eq!(app.input_mode, InputMode::Normal);

    let key_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_c));
    assert_eq!(app.input_mode, InputMode::Chart);
    assert!(app.chart_modal.active);

    let key_esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    app.event(&AppEvent::Key(key_esc));
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(!app.chart_modal.active);
}

#[test]
fn test_chart_q_does_not_exit() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("chart_q_test.csv");

    let mut df = df!("a" => &[1_i32], "b" => &[2_i32]).unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Chart);

    // q does nothing in chart view (no exit)
    let key_q = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    let out = app.event(&AppEvent::Key(key_q));
    assert!(out.is_none());
    assert_eq!(app.input_mode, InputMode::Chart);
}

/// The chart type switches from anywhere: 1-6 name a tab in order, [ and ]
/// cycle, with no focus dance through a tab bar.
#[test]
fn test_chart_type_switches_from_anywhere() {
    use datui::chart_modal::{ChartFocus, ChartKind};
    let (mut app, _rx, _tx) = open_chart_view("chart_direct_type_test.csv");
    let press = |app: &mut App, c: char| {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char(c),
            KeyModifiers::NONE,
        )));
    };

    press(&mut app, '4');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::Kde);
    assert_eq!(
        app.chart_modal.focus,
        ChartFocus::Column,
        "focus lands on the new form's first row"
    );

    // Deep in the KDE form, a number key still switches.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    press(&mut app, '2');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::Histogram);

    press(&mut app, ']');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::BoxPlot);
    press(&mut app, '[');
    press(&mut app, '[');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::XY);
    press(&mut app, '[');
    assert_eq!(
        app.chart_modal.chart_kind,
        ChartKind::Bar,
        "[ wraps to the last tab"
    );
    press(&mut app, '6');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::Bar);
    assert_eq!(app.chart_modal.focus, ChartFocus::Category);
    press(&mut app, ']');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::XY);

    // While the column Picker is open, digits narrow instead of switching.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    ))); // Style -> X axis
    press(&mut app, ' '); // open the Picker
    assert!(app.chart_modal.picker.is_some());
    press(&mut app, '3');
    assert_eq!(app.chart_modal.chart_kind, ChartKind::XY);
    assert_eq!(app.chart_modal.picker.as_ref().unwrap().filter, "3");
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        app.chart_modal.picker.is_none(),
        "Esc closes only the Picker"
    );
    assert_eq!(app.input_mode, InputMode::Chart);
}

/// Columns are picked through the shared Picker: Space opens it on a column
/// row, Enter chooses, and the choice is remembered on the row.
#[test]
fn test_chart_columns_picked_through_the_picker() {
    use datui::chart_modal::ChartFocus;
    let (mut app, _rx, _tx) = open_chart_view("chart_picker_test.csv");
    let press = |app: &mut App, code: KeyCode| {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };

    press(&mut app, KeyCode::Tab); // Style -> X axis
    assert_eq!(app.chart_modal.focus, ChartFocus::XColumn);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter); // choose "x", the cursor's item
    assert_eq!(app.chart_modal.x_column.as_deref(), Some("x"));

    press(&mut app, KeyCode::Tab); // -> Y series
    press(&mut app, KeyCode::Char(' ')); // open the Picker
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char(' ')); // toggle "y"
    press(&mut app, KeyCode::Enter); // done
    assert_eq!(app.chart_modal.y_columns, vec!["y".to_string()]);
    assert!(app.chart_modal.can_export());
}

/// Space on a pick-one row's open Picker chooses the highlighted column — it
/// must never type into the narrow filter, where a space matches nothing and
/// the list blanks under the key that just opened it.
#[test]
fn test_space_chooses_in_a_pick_one_chart_picker() {
    use datui::chart_modal::ChartFocus;
    let (mut app, _rx, _tx) = open_chart_view("chart_space_chooses_test.csv");
    let press = |app: &mut App, code: KeyCode| {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };

    press(&mut app, KeyCode::Tab); // Style -> X axis
    press(&mut app, KeyCode::Char(' ')); // open the Picker
    press(&mut app, KeyCode::Down); // highlight "y"
    press(&mut app, KeyCode::Char(' ')); // chooses, like Enter
    assert!(app.chart_modal.picker.is_none());
    assert_eq!(app.chart_modal.x_column.as_deref(), Some("y"));
    // The form has the keys back at once: the arrows walk the rows again.
    press(&mut app, KeyCode::Down);
    assert_eq!(app.chart_modal.focus, ChartFocus::YColumns);
}

/// A typed export path expands `~` like every other typed path; unexpanded it
/// reaches the PNG/EPS writer as a literal `~` directory and fails NotFound.
#[test]
fn test_chart_export_path_expands_tilde() {
    let (mut app, _rx, _tx) = open_chart_view("chart_tilde_test.csv");
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert!(app.chart_export_modal.active);
    app.chart_export_modal
        .path_input
        .set_value("~/datui_tilde_test_dir/chart.png");
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    let Some(AppEvent::ChartExport(datui::chart_export::ChartExportRequest { path, .. })) = out
    else {
        panic!("Enter starts the export");
    };
    assert!(
        path.is_absolute() && !path.to_string_lossy().contains('~'),
        "the tilde expands: {path:?}"
    );
}

/// Opens a small x/y dataset in the chart view. Nothing is selected yet.
fn open_chart_view(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join(name);
    let mut df = df!(
        "x" => (0..5).collect::<Vec<i32>>(),
        "y" => (0..5).map(|i| i * 3).collect::<Vec<i32>>()
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.is_some());

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Chart);
    (app, rx, tx)
}

/// Feed background results back until the chart for the current selection is prepared.
#[track_caller]
fn pump_until_chart_ready(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
) {
    pump_until(app, rx, tx, App::chart_data_ready);
}

/// Chart data is prepared off the render path: selecting columns starts a background
/// computation (with the throbber up), render draws nothing until it lands, and then
/// draws the prepared series. Nothing here collects on the calling thread.
#[test]
fn test_chart_data_is_prepared_in_the_background() {
    let (mut app, rx, tx) = open_chart_view("chart_render_cache_test.csv");

    // Nothing selected: render draws the empty view and asks for nothing.
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    assert!(!app.chart_preparing());

    // Select x and y, then let any event go through so the selection is noticed.
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    assert!(
        app.chart_preparing(),
        "a selection starts a background prepare"
    );
    assert!(!app.chart_data_ready());
    assert!(
        !app.is_busy(),
        "chart preparation must not lock the keyboard"
    );

    // Render while it computes must not block or panic; it just has no data yet.
    Widget::render(&mut app, area, &mut buf);

    pump_until_chart_ready(&mut app, &rx, &tx);
    assert!(!app.chart_preparing());
    Widget::render(&mut app, area, &mut buf);

    // Closing the chart drops the cache and any late result.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.input_mode, InputMode::Normal);
    assert!(!app.chart_preparing());
}

/// Holding a key through the options must not fan out into a collect per step: one
/// preparation runs at a time, and when it lands the newest selection is the one prepared.
#[test]
fn test_chart_prepares_one_selection_at_a_time() {
    use datui::chart_modal::ChartKind;
    let (mut app, rx, tx) = open_chart_view("chart_one_at_a_time_test.csv");
    app.chart_modal.chart_kind = ChartKind::Histogram;
    app.chart_modal.hist_column = Some("x".to_string());
    app.event(&AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    // Five more distinct requests while the first is still out.
    for _ in 0..5 {
        app.chart_modal.hist_bins += 1;
        app.event(&AppEvent::Resize(80, 24));
    }

    let mut results = 0;
    let mut handle = |app: &mut App, ev: AppEvent| {
        if matches!(ev, AppEvent::BackgroundChartReady) {
            results += 1;
        }
        if let Some(next) = app.event(&ev) {
            let _ = tx.send(next);
        }
    };
    for _ in ticks() {
        while let Ok(ev) = rx.try_recv() {
            handle(&mut app, ev);
        }
        if app.chart_data_ready() {
            break;
        }
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            handle(&mut app, ev);
        }
    }
    assert!(app.chart_data_ready());
    assert_eq!(
        results, 2,
        "the first request, then the newest; the four in between were never spawned"
    );
}

/// A 400x300 EPS chart export to `path`, where nothing is yet.
fn eps_request(path: &Path) -> datui::chart_export::ChartExportRequest {
    datui::chart_export::ChartExportRequest {
        path: path.to_path_buf(),
        format: datui::chart_export::ChartExportFormat::Eps,
        title: String::new(),
        width: 400,
        height: 300,
        overwrite: datui::output_file::Overwrite::Forbid,
    }
}

/// An export parked while a *different*, failing selection is in flight is not failed
/// with that selection's error: it waits for the current selection's data and completes.
#[test]
fn test_chart_export_waits_for_the_current_selection_not_a_failed_one() {
    let (mut app, rx, tx) = open_chart_view("chart_export_after_failure_test.csv");
    // x against x cannot be charted (duplicate column) and takes a moment to fail.
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["x".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    // Move on to a valid selection while that one is still out, and ask for an export.
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chart.eps");
    let next = app
        .event(&AppEvent::ChartExport(eps_request(&path)))
        .expect("ChartExport defers to DoChartExport");
    app.event(&next);

    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        path.exists(),
        "the export completed from the valid selection"
    );
    assert!(
        !app.chart_export_modal.active,
        "no error reopened the modal"
    );
    assert!(app.chart_data_ready());
}

/// A chart export uses the prepared data and writes the file off-thread; if the data is
/// not ready yet the export waits for it rather than collecting on the UI thread.
#[test]
fn test_chart_export_waits_for_prepared_data_and_writes_in_background() {
    let (mut app, rx, tx) = open_chart_view("chart_export_bg_test.csv");
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chart.eps");
    // Asked for while the data is still being prepared.
    let next = app
        .event(&AppEvent::ChartExport(eps_request(&path)))
        .expect("ChartExport defers to DoChartExport");
    app.event(&next);
    assert!(
        app.is_busy(),
        "an export owns the busy state until it finishes"
    );

    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        path.exists(),
        "the export was written once its data arrived"
    );
    assert!(
        !app.chart_export_modal.active,
        "the export modal closes on success"
    );
}

/// A chart export lands whole or not at all, like a data export: PNG and EPS
/// replace a file only where that was agreed to, and a file that appeared
/// meanwhile is left alone with the error in the app.
#[test]
fn test_chart_export_replaces_only_what_was_agreed() {
    use datui::chart_export::{ChartExportFormat, ChartExportRequest};
    use datui::output_file::Overwrite;
    let (mut app, rx, tx) = open_chart_view("chart_export_overwrite_test.csv");
    app.chart_modal.x_column = Some("x".to_string());
    app.chart_modal.y_columns = vec!["y".to_string()];
    app.event(&AppEvent::Resize(80, 24));
    pump_until_idle(&mut app, &rx, &tx);
    let dir = tempfile::tempdir().unwrap();

    for (name, format) in [
        ("chart.png", ChartExportFormat::Png),
        ("chart.eps", ChartExportFormat::Eps),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, "theirs").unwrap();
        let request = ChartExportRequest {
            format,
            ..eps_request(&path)
        };
        run_to_idle(&mut app, &rx, &tx, AppEvent::ChartExport(request.clone()));
        assert!(
            app.error_message().is_some_and(|m| m.contains("appeared")),
            "{name}: the clash reaches the app"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "theirs", "{name}");
        press(&mut app, KeyCode::Esc);
        assert_eq!(app.error_message(), None);

        let replace = ChartExportRequest {
            overwrite: Overwrite::Replace,
            ..request
        };
        run_to_idle(&mut app, &rx, &tx, AppEvent::ChartExport(replace));
        assert_eq!(app.error_message(), None, "{name}");
        let bytes = std::fs::read(&path).unwrap();
        let magic: &[u8] = match format {
            ChartExportFormat::Png => b"\x89PNG",
            ChartExportFormat::Eps => b"%!PS",
        };
        assert!(bytes.starts_with(magic), "{name} was replaced");
    }
    assert!(leftovers(dir.path(), &["chart.png", "chart.eps"]).is_empty());
}

/// Wait for the outcome of a background scan.
///
/// Scanning runs off the event thread so a slow one cannot freeze the interface, so
/// its result arrives over the channel rather than as a return value.
fn await_scan_outcome(rx: &mpsc::Receiver<AppEvent>) -> AppEvent {
    rx.recv_timeout(std::time::Duration::from_secs(30))
        .expect("background scan should report an outcome")
}

#[test]
fn test_open_s3_url_returns_crash_or_loads() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("s3://my-bucket/path/to/file.parquet");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));

    // The scan is spawned, so this returns nothing; the outcome comes over the channel.
    assert!(
        app.event(&ev).is_none(),
        "scan should be spawned, not run inline"
    );

    match await_scan_outcome(&rx) {
        AppEvent::BackgroundFailed { message, .. } => {
            // "Could not read from S3" without credentials; with them, the store's own
            // error naming the s3:// URL.
            assert!(
                message.to_lowercase().contains("s3"),
                "error should mention S3: {message}"
            );
        }
        AppEvent::BackgroundLazyFrameReady { .. } => {
            // With cloud feature and valid credentials/bucket, the scan can succeed.
        }
        _ => panic!("expected a scan outcome for an S3 URL"),
    }
}

#[test]
fn test_open_http_url_attempts_load_or_returns_friendly_error() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("https://example.com/data.csv");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));
    let mut next = app.event(&ev);
    while let Some(ref e) = next {
        if matches!(
            e,
            AppEvent::Crash(_) | AppEvent::DoLoadSchema(..) | AppEvent::DoLoadSchemaBlocking(..)
        ) {
            break;
        }
        next = app.event(e);
    }
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => {
            assert!(
                m.contains("HTTP")
                    || m.contains("HTTPS")
                    || m.contains("download")
                    || m.contains("Failed")
                    || m.contains("failed")
                    || m.contains("not yet supported"),
                "error should mention HTTP/download/failure: {}",
                m
            );
        }
        Some(AppEvent::DoLoadSchema(..)) | Some(AppEvent::DoLoadSchemaBlocking(..)) => {
            // With http feature: download can succeed; schema load is the next phase.
        }
        None => {
            // DoLoadScanPaths shows download confirmation modal and returns None; app is waiting for user.
        }
        _ => panic!("expected Crash, DoLoadSchema, or None (confirmation) when opening HTTP URL"),
    }
}

#[test]
fn test_multiple_remote_paths_returns_error() {
    let (tx, _) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let paths = vec![
        PathBuf::from("s3://bucket/a.parquet"),
        PathBuf::from("s3://bucket/b.parquet"),
    ];
    let next = app.event(&AppEvent::Open(paths, OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));
    let next = app.event(&ev);
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => assert!(
            m.contains("one S3") || m.contains("one at a time"),
            "error should mention single S3 path: {}",
            m
        ),
        _ => panic!("expected Crash when opening multiple S3 URLs"),
    }
    let (tx2, _) = mpsc::channel();
    let mut app = App::new(tx2, common::test_runtime());
    let paths = vec![
        PathBuf::from("https://example.com/a.csv"),
        PathBuf::from("https://example.com/b.csv"),
    ];
    let next = app.event(&AppEvent::Open(paths, OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    let next = app.event(&ev);
    match next.as_ref() {
        Some(AppEvent::Crash(m)) => assert!(
            m.contains("one") && (m.contains("HTTP") || m.contains("URL")),
            "error should mention single URL: {}",
            m
        ),
        _ => panic!("expected Crash when opening multiple HTTP URLs"),
    }
}

#[test]
fn test_open_gs_url_returns_friendly_error_or_attempts_load() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("gs://my-bucket/path/file.parquet");
    let next = app.event(&AppEvent::Open(vec![path], OpenOptions::default()));
    let ev = next.expect("Open should emit DoLoadScanPaths");
    assert!(matches!(ev, AppEvent::DoLoadScanPaths(_, _)));

    assert!(
        app.event(&ev).is_none(),
        "scan should be spawned, not run inline"
    );

    match await_scan_outcome(&rx) {
        AppEvent::BackgroundFailed { message, .. } => {
            assert!(
                message.contains("GCS")
                    || message.contains("gs://")
                    || message.contains("not enabled"),
                "error should mention GCS or gs:// or not enabled: {message}"
            );
        }
        AppEvent::BackgroundLazyFrameReady { .. } => {}
        _ => panic!("expected a scan outcome for a gs:// URL"),
    }
}

#[test]
fn test_csv_null_values_global() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("null_values_test.csv");
    std::fs::write(&csv_path, "x,y\n1,NA\n2,3\n4,N/A\n").unwrap();

    let opts = OpenOptions {
        null_values: Some(vec!["NA".to_string(), "N/A".to_string()]),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], opts);

    assert!(app.data_table_state.is_some());
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let y = df.column("y").unwrap();
    assert_eq!(
        y.null_count(),
        2,
        "NA and N/A should be parsed as null in column y"
    );
}

#[test]
fn test_csv_null_values_per_column() {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("null_values_per_col_test.csv");
    std::fs::write(&csv_path, "a,b\nx,1\nempty,2\nz,3\n").unwrap();

    let opts = OpenOptions {
        null_values: Some(vec!["a=empty".to_string()]),
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], opts);

    assert!(app.data_table_state.is_some());
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    let a = df.column("a").unwrap();
    let b = df.column("b").unwrap();
    assert_eq!(a.null_count(), 1, "only 'empty' in column a should be null");
    assert_eq!(b.null_count(), 0, "column b has no per-column null spec");
}

/// Simulate the real main-loop startup: process events from the channel, render
/// the widget (which sets visible_rows and needs_recollect), then check the flag
/// and spawn another async collect.  Repeat many times to shake out races between
/// the initial tiny-buffer collect (visible_rows=0) and the corrected one.
#[test]
fn test_startup_buffer_race_does_not_lose_rows() {
    common::ensure_sample_data();
    let csv_path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let terminal_area = Rect::new(0, 0, 120, 50); // 50 rows → ~48 visible

    for iteration in 0..50 {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());

        // Simulate what run() does: send Open, mark busy.
        tx.send(AppEvent::Open(
            vec![csv_path.clone()],
            OpenOptions::default(),
        ))
        .unwrap();

        // Process events like the main loop: drain channel, render, check needs_recollect.
        let mut completed = false;
        for _tick in ticks() {
            // Drain all pending events.
            loop {
                match rx.try_recv() {
                    Ok(AppEvent::Crash(msg)) => panic!("iteration {iteration}: Crash: {msg}"),
                    Ok(event) => {
                        if let Some(next) = app.event(&event) {
                            tx.send(next).unwrap();
                        }
                    }
                    Err(_) => break,
                }
            }

            // Render into a buffer (this sets visible_rows and may set needs_recollect).
            let mut buf = Buffer::empty(terminal_area);
            app.render(terminal_area, &mut buf);

            // After render, check needs_recollect — same as the real main loop.
            let needs = app
                .data_table_state
                .as_mut()
                .map(|s| {
                    let n = s.needs_recollect;
                    s.needs_recollect = false;
                    n
                })
                .unwrap_or(false);
            if needs {
                app.spawn_async_collect("Loading buffer...");
            }

            // Check if we have data and are no longer busy.
            if app.data_table_state.is_some() && !app.is_busy() {
                completed = true;
                break;
            }

            // Brief sleep to let background tasks run (simulates poll timeout).
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        assert!(
            completed,
            "iteration {iteration}: timed out waiting for data to load"
        );

        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.visible_rows > 0,
            "iteration {iteration}: visible_rows should be set by render"
        );

        // The buffer must cover at least the visible window.
        let buffered_rows = state.buffered_end().saturating_sub(state.buffered_start());
        assert!(
            buffered_rows >= state.visible_rows || buffered_rows >= state.num_rows(),
            "iteration {iteration}: buffer too small: {buffered_rows} buffered but \
             {visible} visible, {total} total rows",
            visible = state.visible_rows,
            total = state.num_rows(),
        );

        // display_slice_df must be Some (not None = no data to show).
        assert!(
            state.display_slice_df().is_some(),
            "iteration {iteration}: display_slice_df is None — buffer not sliced into display"
        );
    }
}

/// A `Background*Ready` event whose `generation` no longer matches the App's
/// `task_generation` must NOT mutate visible state. This guards every non-collect
/// `Background*` handler against the same stale-generation race that the collect
/// path got in commit e4d65e3.
#[test]
fn test_stale_background_events_are_ignored() {
    use datui::data_quality::{DataQualityResults, QualityPrecision};
    use datui::statistics::AnalysisResults;

    common::ensure_sample_data();
    let csv_path = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // After load, generation is non-zero. Anything tagged generation=0 is stale.
    let stale_gen: u64 = 0;
    assert!(
        app.task_generation() > stale_gen,
        "expected generation to advance past 0 after load"
    );

    // Build dummy results we can identify in modal slots.
    let dummy = AnalysisResults {
        column_statistics: vec![],
        total_rows: 999_999,
        sample_size: None,
        per_value: None,
        sample_seed: 0,
        correlation_matrix: None,
        distribution_analyses: vec![],
    };

    // Each variant: send with stale generation, assert nothing landed in the modal.
    app.analysis_modal.describe_results = None;
    app.event(&AppEvent::BackgroundDescribeReady {
        generation: stale_gen,
        results: dummy.clone(),
    });
    assert!(
        app.analysis_modal.describe_results.is_none(),
        "stale BackgroundDescribeReady should not write describe_results"
    );

    app.analysis_modal.distribution_results = None;
    app.event(&AppEvent::BackgroundDistributionReady {
        generation: stale_gen,
        results: dummy.clone(),
    });
    assert!(
        app.analysis_modal.distribution_results.is_none(),
        "stale BackgroundDistributionReady should not write distribution_results"
    );

    app.analysis_modal.correlation_results = None;
    app.event(&AppEvent::BackgroundCorrelationReady {
        generation: stale_gen,
        results: dummy,
    });
    assert!(
        app.analysis_modal.correlation_results.is_none(),
        "stale BackgroundCorrelationReady should not write correlation_results"
    );

    app.analysis_modal.data_quality_results = None;
    app.event(&AppEvent::BackgroundDataQualityReady {
        generation: stale_gen,
        results: Box::new(DataQualityResults {
            total_rows: Some(999_999),
            evaluated_rows: 1,
            precision: QualityPrecision::Sampled,
            sample_seed: 1,
            columns: vec![],
            observations: vec![],
            segments: vec![],
            temporal: vec![],
            identity: None,
            category_variants: vec![],
            shared_nulls: vec![],
            source_files: None,
            per_value: None,
            footers_read: None,
            reads: None,
            examples: vec![],
            unsampled_segments: vec![],
            intent: None,
            source: None,
        }),
        kept: None,
        plan: Box::default(),
    });
    assert!(
        app.analysis_modal.data_quality_results.is_none(),
        "stale BackgroundDataQualityReady should not write data-quality results"
    );
}

/// Esc cancels an analysis while it runs, for every tool: it acts at once rather than
/// queueing behind the run, the bar says so first, and the answer that arrives later
/// is dropped rather than installed.
#[test]
fn test_esc_cancels_a_distribution_analysis_in_flight() {
    use datui::analysis_modal::AnalysisTool;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(&key(KeyCode::Char('a')));
    app.analysis_modal.sidebar_state.select(Some(1));
    show_sample_form(&mut app);
    let next = app.event(&key(KeyCode::Enter));
    assert!(matches!(next, Some(AppEvent::AnalysisDistributionCompute)));
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DistributionAnalysis)
    );
    // The run starts on a worker.
    app.event(&next.unwrap());
    assert!(app.analysis_modal.computing.is_some());

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(screen.contains("Cancel"), "{screen:?}");
    assert!(!screen.contains("0 / 1"), "no gauge that cannot move");

    let esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    assert!(app.hard_escape_while_busy(&esc), "Esc jumps the queue");
    app.event(&AppEvent::Key(esc));
    assert!(app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.selected_tool, None);
    assert!(app.analysis_modal.active, "still on the analysis screen");
    assert_eq!(app.flash_message(), Some("Analysis cancelled"));

    // The worker finishes anyway; what it sends is stale.
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.selected_tool, None);
}

#[test]
fn test_data_quality_plan_runs_in_background_and_opens_overview() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityPage;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // Choosing Data Quality opens its Setup with the cursor in it; Enter runs the
    // default plan and leads with the result.
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    assert!(app.analysis_modal.data_quality_results.is_some());

    // A changed plan runs again from Setup, one e away, and e brings the cursor with
    // it, so the Enter that runs needs no Tab first.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    app.analysis_modal.data_quality_plan.sample_seed = 7_119;
    let next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);

    assert!(app.analysis_modal.data_quality_results.is_some());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    assert!(!app.is_busy());

    app.analysis_modal
        .data_quality_results
        .as_mut()
        .unwrap()
        .observations
        .push(datui::data_quality::QualityObservation {
            kind: datui::data_quality::ObservationKind::Nulls,
            column: "example".to_string(),
            affected_rows: 1,
            evaluated_rows: 10,
            fact: "1 null row".to_string(),
            normalized_category: None,
            files: Vec::new(),
            time_format: None,
        });
    app.analysis_modal.data_quality_table_state.select(Some(0));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_observation_detail);
    let area = Rect::new(0, 0, 80, 24);
    let mut detail_buffer = Buffer::empty(area);
    app.render(area, &mut detail_buffer);
    assert!(
        detail_buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .contains("Check: clustered"),
        "the finding says what to check, not only its formula"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.data_quality_observation_detail);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);

    for area in [
        Rect::new(0, 0, 120, 32),
        Rect::new(0, 0, 80, 24),
        Rect::new(0, 0, 50, 18),
    ] {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(
            screen.contains("Data Quality"),
            "quality breadcrumb should survive a {width}x{height} layout",
            width = area.width,
            height = area.height
        );
    }

    for page in [
        QualityPage::Columns,
        QualityPage::Segments,
        QualityPage::Trends,
        QualityPage::Intervals,
    ] {
        app.analysis_modal.set_quality_page(page);
        for area in [Rect::new(0, 0, 120, 32), Rect::new(0, 0, 50, 18)] {
            let mut buffer = Buffer::empty(area);
            app.render(area, &mut buffer);
            let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
            assert!(screen.contains("Data Quality"));
            match page {
                QualityPage::Columns => assert!(screen.contains("Findings")),
                // One segment is no comparison: the page says what makes one.
                QualityPage::Segments => assert!(screen.contains("one segment")),
                QualityPage::Trends => assert!(screen.contains("Across segments")),
                QualityPage::Intervals => assert!(screen.contains("Time between dates")),
                _ => {}
            }
        }
    }

    // The largest measured move between segments is on the screen, not only in the
    // profile: #196 asks for it and nothing read it before.
    // Result pages read the plan the result was measured with.
    let measured = app.analysis_modal.data_quality_last_plan.as_mut().unwrap();
    measured.grain = datui::data_quality::QualityGrain::RowChunks(5);
    measured.comparison = datui::data_quality::QualityComparison::Previous;
    app.analysis_modal.set_quality_page(QualityPage::Segments);
    let wide = Rect::new(0, 0, 160, 40);
    let mut buffer = Buffer::empty(wide);
    app.render(wide, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("Largest change"),
        "Segments should name the column and measurement that moved"
    );
    // Enter shows a segment's every column and measure; Esc goes back to it.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::SegmentDetail
    );
    let mut buffer = Buffer::empty(wide);
    app.render(wide, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains("current view"),
        "the drill-in names its segment"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Segments);

    // Every remaining page and popup must say its own piece at each width, so a
    // clipped label or a screen that renders nothing at all fails here.
    for (page, expected) in [
        (QualityPage::Setup, "Time roles"),
        (QualityPage::TimeRoles, "Date, time and text columns"),
        (QualityPage::Detail, "Missing"),
    ] {
        app.analysis_modal.set_quality_page(page);
        for area in [
            Rect::new(0, 0, 120, 32),
            Rect::new(0, 0, 80, 24),
            Rect::new(0, 0, 50, 18),
        ] {
            let mut buffer = Buffer::empty(area);
            app.render(area, &mut buffer);
            let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
            assert!(
                screen.contains(expected),
                "{expected:?} should survive a {}x{} layout",
                area.width,
                area.height
            );
        }
    }

    // Once a run exists the header says what was measured on one line, as every
    // tool's does, and the sidebar is the tool list every tool has.
    app.analysis_modal.set_quality_page(QualityPage::Overview);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    let header = &screen[..area.width as usize];
    assert!(
        header.starts_with("Data Quality") && header.contains(" rows"),
        "the header says what was measured: {header:?}"
    );
    assert!(
        !screen.contains("Result"),
        "no second verdict in the sidebar"
    );
    assert!(screen.contains("clean"));

    app.analysis_modal.set_quality_page(QualityPage::Setup);
    for (popup, expected) in [
        ("access", "Estimate basis"),
        // The extra reads a full scan makes for the values a type conflict hides are
        // promised before anything runs, like every other read on this page.
        ("access", "Conflict values"),
        ("confirm", "remote writes are 0 B."),
    ] {
        app.analysis_modal.data_quality_show_access = popup == "access";
        app.analysis_modal.data_quality_confirm_run = popup == "confirm";
        let area = Rect::new(0, 0, 120, 32);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(screen.contains(expected), "{popup} popup should not clip");
    }
    app.analysis_modal.data_quality_show_access = false;
    app.analysis_modal.data_quality_confirm_run = false;

    app.analysis_modal.set_quality_page(QualityPage::Segments);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('b'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_plan.comparison,
        datui::data_quality::QualityComparison::Baseline
    );
    assert!(
        app.analysis_modal
            .data_quality_plan
            .baseline_segment
            .is_some()
    );
    assert!(!app.is_busy());
    // Column and measure are the Trends chart's; Segments shows every column.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('4'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    // The measure the Trends table draws; every column is on it at once.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('m'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_metric,
        datui::data_quality::QualityMetric::EmptyRate
    );

    // Enter on a highlighted column must open that column, not the first one.
    app.analysis_modal.set_quality_page(QualityPage::Columns);
    app.analysis_modal.data_quality_table_state.select(Some(3));
    let fourth = app
        .analysis_modal
        .data_quality_results
        .as_ref()
        .unwrap()
        .columns[3]
        .name
        .clone();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Detail);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(
        screen.contains(&fourth),
        "Detail should open the highlighted column {fourth}"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Columns);
    assert_eq!(
        app.analysis_modal.data_quality_table_state.selected(),
        Some(3),
        "returning from Detail should land back on the same column"
    );

    app.analysis_modal.close();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_from_cache);
    assert_eq!(app.analysis_modal.data_quality_plan.sample_seed, 7_119);
    assert!(app.analysis_modal.data_quality_results.is_some());
    assert!(!app.is_busy());
    // Selecting the tool no longer moves focus; cross into the result as the
    // user would, with Tab.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);

    // A drift observation's detail is the files themselves: which ones, how many rows
    // each cost the column, the type each holds, and the values the conflict hid.
    {
        let results = app.analysis_modal.data_quality_results.as_mut().unwrap();
        results.observations = vec![datui::data_quality::QualityObservation {
            kind: datui::data_quality::ObservationKind::TypeConflict,
            column: "fee".to_string(),
            affected_rows: 2,
            evaluated_rows: 7,
            fact: "1 of 3 files holds a type the scan cannot read".to_string(),
            normalized_category: None,
            files: vec![datui::data_quality::QualityFileEvidence {
                number: 2,
                name: "b.parquet".to_string(),
                rows: 2,
                stored_type: Some("str".to_string()),
                examples: vec!["sixty".to_string()],
            }],
            time_format: None,
        }];
        app.analysis_modal.set_quality_page(QualityPage::Overview);
        app.analysis_modal.data_quality_table_state.select(Some(0));
        app.analysis_modal.data_quality_observation_detail = true;
        let area = Rect::new(0, 0, 120, 32);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        for expected in [
            "#2 b.parquet",
            "as str",
            "sixty",
            "rows of the loaded source",
        ] {
            assert!(
                screen.contains(expected),
                "the conflict detail should show {expected:?}"
            );
        }
        app.analysis_modal.data_quality_observation_detail = false;
    }

    let column = app
        .data_table_state
        .as_ref()
        .unwrap()
        .schema()
        .iter_names()
        .next()
        .unwrap()
        .to_string();
    let original_view = app.data_table_state.as_ref().unwrap().len_generation();
    let results = app.analysis_modal.data_quality_results.as_mut().unwrap();
    results.precision = datui::data_quality::QualityPrecision::Exact;
    results.observations = vec![datui::data_quality::QualityObservation {
        kind: datui::data_quality::ObservationKind::Nulls,
        column,
        affected_rows: 0,
        evaluated_rows: results.evaluated_rows,
        fact: "matching rows".to_string(),
        normalized_category: None,
        files: Vec::new(),
        time_format: None,
    }];
    app.analysis_modal.set_quality_page(QualityPage::Overview);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_observation_detail);
    // The run's rows are kept: they are cut in memory, off the UI thread.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    drain_events(&mut app, &rx);
    assert!(!app.analysis_modal.active);
    let area = Rect::new(0, 0, 80, 24);
    let mut evidence_buffer = Buffer::empty(area);
    app.render(area, &mut evidence_buffer);
    assert!(
        evidence_buffer
            .content()
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
            .contains("Back")
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.active);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.active);
    assert!(app.analysis_modal.data_quality_observation_detail);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().len_generation(),
        original_view
    );

    app.analysis_modal.close();
    let state = app.data_table_state.as_mut().unwrap();
    state.deferred(|s| s.reverse());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.data_quality_from_cache);
    assert!(app.analysis_modal.data_quality_results.is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
}

/// The Data Quality scope input is a text field: Ctrl-C must reach the
/// textarea's Copy binding instead of quitting, and `?` must type into the
/// scope command instead of opening help.
#[test]
fn test_data_quality_scope_input_owns_ctrl_c_and_question_mark() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityPage;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );

    // e to Setup, Space on the Sample row opens the Sample form, whose first row
    // is the scope typed as text.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char(' '),
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.sample_form.is_some());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    // Rows from is a choice; a row range brings rows that are typed into.
    assert!(!app.text_field_focused());
    for code in [KeyCode::Right, KeyCode::Down] {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    }
    assert_eq!(
        app.analysis_modal.sample_form.as_ref().unwrap().field,
        datui::sample_modal::SampleField::RangeFrom
    );
    assert!(app.text_field_focused());

    let quit = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
    )));
    assert!(
        !matches!(quit, Some(AppEvent::Exit)),
        "Ctrl-C in a sample text row must not quit"
    );

    let scope = |app: &App| {
        app.analysis_modal
            .sample_form
            .as_ref()
            .unwrap()
            .range_from
            .value()
            .to_string()
    };
    let before = scope(&app);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        scope(&app),
        format!("{before}?"),
        "? in a sample text row must type, not open help"
    );
}

/// The table's letter keys are unmodified keys: Ctrl+E must not open Export
/// and Ctrl+R must not reverse. Paging (Ctrl+F/B/D/U) keeps its modifiers.
#[test]
fn modified_letters_are_not_table_feature_keys() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::CONTROL,
    )));
    assert!(!app.export_modal.active, "Ctrl+E is not e");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('y'),
        KeyModifiers::CONTROL,
    )));
    assert!(!app.copy_modal.active, "Ctrl+Y is not y");

    // And the plain letter still works. (Paging keeps Ctrl+F/B/D/U: those
    // four are the guard's explicit exceptions, matching their declared arms.)
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert!(app.export_modal.active, "e still opens Export");
}

/// Declining an overwrite returns to the filled export form: the typed path
/// survives, the prompt starts on No, and the existing file is untouched.
#[test]
fn declining_an_overwrite_keeps_the_export_form() {
    common::ensure_sample_data();
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("already.csv");
    std::fs::write(&target, "old contents").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );

    let key =
        |app: &mut App, code| app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));

    key(&mut app, KeyCode::Char('e'));
    assert!(app.export_modal.active);

    // Enter on the empty form says why inline instead of doing nothing,
    // and typing is the correction that clears it.
    key(&mut app, KeyCode::Enter);
    assert!(app.export_modal.active, "an empty path raises no modal");
    assert_eq!(app.export_modal.path_error, Some("Enter a file path."));
    key(&mut app, KeyCode::Char('x'));
    assert_eq!(app.export_modal.path_error, None);
    key(&mut app, KeyCode::Backspace);

    let typed = target.display().to_string();
    app.export_modal.path_input.set_value(&typed);
    key(&mut app, KeyCode::Enter);

    assert!(app.confirmation_modal.active, "an existing file asks first");
    assert!(
        !app.confirmation_modal.focus_yes,
        "a destructive confirmation starts on No"
    );
    assert_eq!(app.confirmation_modal.yes_label, "Overwrite");

    // A reflexive second Enter declines, and the form comes back as typed.
    key(&mut app, KeyCode::Enter);
    assert!(!app.confirmation_modal.active);
    assert!(app.export_modal.active, "No returns to the form");
    assert_eq!(app.export_modal.path_input.value(), typed);
    assert_eq!(app.input_mode, InputMode::Export);
    assert_eq!(
        std::fs::read_to_string(&target).unwrap(),
        "old contents",
        "declining wrote nothing"
    );

    // Esc from the confirmation does the same.
    key(&mut app, KeyCode::Enter);
    assert!(app.confirmation_modal.active);
    key(&mut app, KeyCode::Esc);
    assert!(app.export_modal.active, "Esc returns to the form");
    assert_eq!(app.export_modal.path_input.value(), typed);

    // Esc from the form itself discards it.
    key(&mut app, KeyCode::Esc);
    assert!(!app.export_modal.active);
    assert_eq!(app.input_mode, InputMode::Normal);
}

/// Selecting a tool runs it but leaves focus on the sidebar: focus moves only
/// when the user presses Tab, never as a side effect of Enter or of results
/// arriving. Reviewers kept landing in the wrong tool because it jumped.
#[test]
fn selecting_a_tool_keeps_the_sidebar_focus() {
    use datui::analysis_modal::AnalysisFocus;

    let (mut app, rx, _tx) = open_query_filter_fixture("analysis_focus.csv");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);

    // Enter on Describe shows its Sample form in the pane, and the cursor goes
    // into it: the form is what the pane is for until the first run.
    show_sample_form(&mut app);
    assert!(app.analysis_modal.sample_form.is_some());
    assert!(app.analysis_modal.describe_results.is_none());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    assert_eq!(
        app.analysis_modal.sample_form.as_ref().unwrap().field,
        datui::sample_modal::SampleField::Rows,
        "the cursor lands on the first setting"
    );
    // Esc hands the cursor back to the list and leaves the form waiting; Enter
    // there runs it as it stands.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);
    assert!(app.analysis_modal.sample_form.is_some());
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.describe_results.is_some());
    assert_eq!(
        app.analysis_modal.focus,
        AnalysisFocus::Sidebar,
        "running a tool must not move focus"
    );

    // Tab is the one move: into the result, and back.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);
}

/// On a local file, choosing Data Quality opens its Setup, and Enter there runs the
/// default plan and leads with the result; Setup stays one e away. Tab crosses
/// sidebar and result here exactly as in the other tools.
#[test]
fn data_quality_on_a_local_file_leads_with_the_result() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityPage;

    let (mut app, rx, _tx) = open_query_filter_fixture("dq_local_lead.csv");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(next, Some(AppEvent::AnalysisDataQualityCompute)),
        "a local default plan runs without ceremony"
    );
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);

    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert!(app.analysis_modal.data_quality_results.is_some());
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::Overview,
        "the result leads; the plan stays an Esc away"
    );
    // The run started from the form, so the cursor is on the result it made.
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    for expected in [AnalysisFocus::Sidebar, AnalysisFocus::Main] {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Tab,
            KeyModifiers::NONE,
        )));
        assert_eq!(app.analysis_modal.focus, expected, "Tab crosses both ways");
    }

    // e from the result opens Setup.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);

    // The control bar is the one hint surface: the widget draws no key rows
    // or prose of its own.
    let area = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        !screen.contains("The plan is inert"),
        "the dimmed prose line is gone"
    );
    assert!(
        screen.contains("Run") && screen.contains("Space"),
        "the global bar names Setup's keys"
    );
}

/// The overview is a report: a verdict first, problems ranked above notes, columns
/// that go missing together said once, the clean columns named, and a grouped
/// finding still opens exactly its rows.
#[test]
fn data_quality_reads_as_a_report() {
    use datui::data_quality::QualityPage;

    let dir = common::fixture_dir();
    let path = dir.join("dq_report.parquet");
    let gap = |row: i64| row % 50 == 7;
    let mut df = df!(
        "id" => (0..200i64).collect::<Vec<_>>(),
        "open" => (0..200i64).map(|row| (!gap(row)).then_some(row as f64)).collect::<Vec<_>>(),
        "close" => (0..200i64).map(|row| (!gap(row)).then_some(row as f64 + 0.5)).collect::<Vec<_>>(),
        "region" => (0..200i64).map(|row| ["West", "west ", "East"][row as usize % 3]).collect::<Vec<_>>(),
        "market" => vec!["US"; 200],
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // Before the first run the pane is Setup: every setting, and what a run reads,
    // before anything is read.
    let first = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(first);
    app.render(first, &mut buffer);
    let screen = rendered_text(&buffer);
    for section in [
        "Rows & sample",
        "Columns",
        "Study",
        "Read",
        "Time roles",
        "Grain",
    ] {
        assert!(
            screen.contains(section),
            "Setup shows {section:?}:\n{screen}"
        );
    }
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    // The run shows the progress every tool shows: the phase, the clock and what
    // it reads, in place of the view. No plan page, no popup over it.
    let mut buffer = Buffer::empty(first);
    app.render(first, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains("Preparing the plan") && screen.contains("random rows"),
        "the shared progress view"
    );
    for gone in ["PROFILE PLAN", "Running", "Planned"] {
        assert!(!screen.contains(gone), "{gone:?} shows during a run");
    }
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    // The run started from the form, so the cursor is already on the report.
    assert_eq!(
        app.analysis_modal.focus,
        datui::analysis_modal::AnalysisFocus::Main
    );

    let area = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    for expected in [
        "1 problem  2 notes  1 of 5 columns clean",
        "Data Quality",
        "all 200 rows",
        "Overview",
        "Columns",
        "Segments",
        "Trends",
        "Mixed spellings",
        "Missing together",
        "4 rows (2.0%)",
        "Single value",
        "No findings",
    ] {
        assert!(
            screen.contains(expected),
            "overview should show {expected:?}"
        );
    }
    let problem = screen.find("Mixed spellings").unwrap();
    assert!(
        problem < screen.find("Missing together").unwrap(),
        "problems rank above notes"
    );
    assert!(
        !screen.contains("Measured fact"),
        "no raw observation table on the overview"
    );
    for gone in ["Result", "checked", "seed"] {
        assert!(!screen.contains(gone), "{gone:?} repeats the header");
    }

    // The arrows walk the tabs, and every page's bar has one shape: the way out,
    // the page's own action, then the shared keys in one order.
    let bar = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let bar_row = (area.height as usize - 1) * area.width as usize;
        buffer.content()[bar_row..]
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
    };
    let shared = format!(
        "e  Setup   s  Sample   {}  Page",
        datui::glyphs::get().updown_lr
    );
    for (page, own) in [
        (QualityPage::Overview, "Enter  Details"),
        (QualityPage::Columns, "Enter  Inspect"),
        (QualityPage::Segments, "Enter  Set Grain"),
        (QualityPage::Trends, "Enter  Set Grain"),
    ] {
        if page != QualityPage::Overview {
            app.event(&AppEvent::Key(KeyEvent::new(
                KeyCode::Right,
                KeyModifiers::NONE,
            )));
        }
        assert_eq!(app.analysis_modal.data_quality_page, page);
        assert!(
            bar(&mut app).starts_with(&format!(" Esc  Back   {own}   {shared}")),
            "{page:?}: {:?}",
            bar(&mut app)
        );
    }
    // An empty Trends page names the setting that fills it, and Enter opens it.
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(screen.contains("Set Grain") && !screen.contains("Metric"));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    // Straight to the Grain choices, in Setup, in the words the header uses.
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(
        app.analysis_modal.setup_row(),
        datui::analysis_modal::SetupRow::Grain
    );
    assert!(app.analysis_modal.data_quality_picker.is_some());
    let bar_now = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let bar_row = (area.height as usize - 1) * area.width as usize;
        buffer.content()[bar_row..]
            .iter()
            .map(|cell| cell.symbol())
            .collect::<String>()
    };
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(screen.contains("whole dataset") && screen.contains("in chunks of"));
    assert!(bar_now(&mut app).contains("Choose"));
    // Choosing stages the edit; the report keeps the plan it was measured with, and
    // nothing runs.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_picker.is_none());
    assert_ne!(
        app.analysis_modal.data_quality_plan.grain,
        datui::data_quality::QualityGrain::Dataset
    );
    assert!(app.analysis_modal.quality_plan_pending());
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    let bar = bar_now(&mut app);
    assert!(
        bar.contains("Discard") && bar.contains("Run") && bar.contains("Space  Choose"),
        "{bar}"
    );
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(rendered_text(&buffer).contains("Edited: Enter runs it, Esc discards"));
    // Esc discards the staged edit and goes back to the report.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.analysis_modal.quality_plan_pending());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    // Each row names what Space does with it.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert!(bar_now(&mut app).contains("Sample Form"));
    for _ in 0..2 {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    assert_eq!(
        app.analysis_modal.setup_row(),
        datui::analysis_modal::SetupRow::TimeRoles
    );
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains("needs an interval to measure"),
        "no threshold without an interval:\n{screen}"
    );
    // Text columns can take a role, read through a format.
    assert!(bar_now(&mut app).contains("Time Roles"));
    // Enter runs from any row; the plan is the one measured, so the report opens.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('1'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    assert!(app.analysis_modal.data_quality_results.is_some());
    // With the tool list focused the bar names its keys, not the page's.
    let tab = AppEvent::Key(KeyEvent::new(KeyCode::Tab, KeyModifiers::NONE));
    app.event(&tab);
    let bar = bar_now(&mut app);
    assert!(bar.contains("Select") && !bar.contains("Details"), "{bar}");
    app.event(&tab);
    assert!(bar_now(&mut app).contains("Details"));

    // The clean entry lists what was checked: the most important few, then all.
    for code in [KeyCode::End, KeyCode::Enter] {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    }
    assert!(app.analysis_modal.data_quality_observation_detail);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains("Checks"),
        "the clean entry names its checks"
    );
    assert!(screen.contains("Duplicate rows"));
    assert!(screen.contains("4 more checks"));
    assert!(
        screen.contains("All Checks"),
        "the bar says Enter shows the rest"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        app.analysis_modal.data_quality_observation_detail,
        "Enter on the clean entry expands, it does not close"
    );
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(screen.contains("Nearly unique") && screen.contains("Fewer Checks"));
    for code in [KeyCode::Esc, KeyCode::Home] {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    }

    // The grouped finding opens every row it counts, and only those.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.data_quality_observation_detail);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains("null in both columns")
            && screen.contains("No row misses one without the other"),
        "the detail says the columns go missing together"
    );
    assert!(
        !screen.contains("Why it matters"),
        "facts and advice as a list, not a lecture"
    );
    assert!(
        screen.contains("Show Rows"),
        "the bar names what Enter does"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    drain_events(&mut app, &rx);
    assert!(!app.analysis_modal.active);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), 4);
}

/// A finding with more evidence than the screen holds scrolls inside its popup,
/// counts what is below, and lists the values with the most rows first.
#[test]
fn a_long_finding_scrolls() {
    use datui::data_quality::QualityPage;

    let dir = common::fixture_dir();
    let path = dir.join("dq_long_finding.parquet");
    // Sixty names, each also written in capitals; name 0 has the most rows.
    let names = (0..3_000usize)
        .map(|row| {
            let name = format!("Company {:02}", row % 60);
            if row % 120 < 60 {
                name
            } else {
                name.to_uppercase()
            }
        })
        .chain(std::iter::repeat_n("Company 00".to_string(), 500))
        .collect::<Vec<_>>();
    let rows = names.len() as i64;
    let mut df = df!("id" => (0..rows).collect::<Vec<_>>(), "name" => names).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    let press = |app: &mut App, code| {
        let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
        while let Some(ev) = next {
            next = app.event(&ev);
        }
    };
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    press(&mut app, KeyCode::Enter);
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    app.analysis_modal.data_quality_table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.data_quality_observation_detail);

    let area = Rect::new(0, 0, 100, 20);
    let render = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };
    let screen = render(&mut app);
    assert!(screen.contains("Mixed spellings"));
    assert!(
        screen.contains("\"Company 00\" (525)  \"COMPANY 00\" (25)"),
        "the value with the most rows leads, its commonest spelling first"
    );
    assert!(screen.contains(" more "), "the frame counts what is below");
    assert!(screen.contains("Scroll"), "the bar says the popup scrolls");
    press(&mut app, KeyCode::End);
    let screen = render(&mut app);
    assert!(!screen.contains(" more "), "nothing left below at the end");
    assert!(screen.contains("Show Rows"), "the Enter line is reachable");
    press(&mut app, KeyCode::Home);
    assert_eq!(app.analysis_modal.data_quality_detail_scroll.offset, 0);
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.data_quality_observation_detail);
}

/// A finding measured on a sample opens the sample's matching rows, drawn again
/// from its seed: the count the popup promises is the count in the table.
#[test]
fn a_sampled_finding_opens_its_sampled_rows() {
    use datui::data_quality::{QualityPage, QualityPrecision};

    let dir = common::fixture_dir();
    let path = dir.join("dq_sampled_evidence.parquet");
    let mut df = df!(
        "id" => (0..5_000i64).collect::<Vec<_>>(),
        "v" => (0..5_000i64).map(|row| (row % 10 != 3).then_some(row)).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    // A table sorted on screen: the sample is drawn from the unsorted rows, as
    // every tool draws it, so the sort changes nothing about which rows it holds.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort(vec!["id".to_string()], false);
    pump_until_idle(&mut app, &rx, &tx);
    app.analysis_modal.sample.rows = 1_000;

    let enter = || AppEvent::Key(KeyEvent::new(KeyCode::Enter, KeyModifiers::NONE));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(&enter());
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.precision, QualityPrecision::Sampled);
    let report = datui::quality_report::build_report(results);
    let index = report
        .findings
        .iter()
        .position(|finding| finding.title == "Missing values")
        .expect("v is missing in a tenth of the rows");
    let expected = report.findings[index].affected_rows;
    assert!(expected > 0 && expected < 1_000);

    app.analysis_modal
        .data_quality_table_state
        .select(Some(index));
    app.event(&enter());
    assert!(app.analysis_modal.data_quality_observation_detail);
    let area = Rect::new(0, 0, 110, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains(&format!("Enter shows the {expected} sampled rows.")),
        "the popup says which rows open"
    );
    assert!(screen.contains("Show Rows"));
    assert!(!screen.contains("full profile"));

    let mut next = app.event(&enter());
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.analysis_modal.active);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.num_rows(),
        expected,
        "exactly the rows the finding counted"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(app.analysis_modal.active, "Esc goes back to the report");
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 5_000);
}

/// A table with whole rows copied (but for their bytes), text that parses but for a
/// few values, codes that all parse, and two columns missing at different rates.
fn open_findings_fixture(
    name: &str,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    PathBuf,
) {
    let dir = common::fixture_dir();
    let path = dir.join(name);
    // A thousand distinct rows, then two more copies of the first three hundred.
    let rows = (0..1_000i64)
        .chain(0..300)
        .chain(0..300)
        .collect::<Vec<_>>();
    let mut df = df!(
        "id" => &rows,
        "code" => rows.iter().map(|row| if row % 50 == 0 { "n/a".to_string() } else { (1_000 + row).to_string() }).collect::<Vec<_>>(),
        "zip" => rows.iter().map(|row| format!("{:05}", row % 97)).collect::<Vec<_>>(),
        "a" => rows.iter().map(|row| (row % 9 != 0).then_some(*row as f64)).collect::<Vec<_>>(),
        "b" => rows.iter().map(|row| (row % 4 != 1).then_some(*row as f64)).collect::<Vec<_>>(),
    )
    .unwrap();
    // Bytes that differ on every row, copies too: the checks read binary as one stub,
    // so they do not tell copies apart, and neither may the rows a finding opens.
    let blob = (0..rows.len() as u32)
        .map(|position| position.to_le_bytes().to_vec())
        .collect::<Vec<_>>();
    df.with_column(Column::new("blob".into(), blob)).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx, path)
}

/// Nothing was started: the app is idle and nothing but the run before's lease
/// coming back is on the channel. The lease is dropped after the run's answer is
/// sent, so on a loaded machine it can still be on its way.
#[track_caller]
fn assert_nothing_started(app: &mut App, rx: &mpsc::Receiver<AppEvent>) {
    assert!(!app.is_busy(), "nothing is running");
    while let Ok(event) = rx.try_recv() {
        assert!(
            matches!(event, AppEvent::BackgroundWorkFinished { .. }),
            "no background work was started"
        );
        app.event(&event);
    }
}

/// Every character on screen that is not ASCII is a glyph slot, which has an ASCII
/// twin under `LANG=C`.
fn assert_glyph_slots(screen: &str) {
    let g = datui::glyphs::get();
    let slots = [
        g.rail,
        g.rule_h,
        g.middot,
        g.ellipsis,
        g.warning,
        g.check,
        g.dash,
        g.times,
        g.updown,
        g.updown_lr,
        g.null,
        g.scroll_thumb,
        g.binary_stub,
    ]
    .concat();
    for c in screen.chars().filter(|c| !c.is_ascii()) {
        assert!(
            slots.contains(c) || "╭╮╰╯│─".contains(c),
            "{c:?} is not a glyph slot:\n{screen}"
        );
    }
}

/// The findings list narrows by column and by type and orders by rows or rate from
/// the report on screen: nothing is read and nothing is measured again. Duplicate
/// rows and text that does not parse open exactly the rows the run counted, from
/// the rows it kept, even once the file is gone; a finding with no rows says why.
#[test]
fn findings_narrow_order_and_open_kept_evidence_without_a_read() {
    use datui::data_quality::{QualityPage, QualityPrecision};
    use datui::quality_report::FindingOrder;

    let name = "dq_findings_kept.parquet";
    let (mut app, rx, tx, path) = open_findings_fixture(name);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // A sample smaller than the table, so what opens is the sample's.
    app.analysis_modal.data_quality_plan.dataset_rows = 800;
    app.analysis_modal.data_quality_plan.sample_seed = 11;
    let next = press(&mut app, KeyCode::Enter);
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    let results = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(results.precision, QualityPrecision::Sampled);
    let report = datui::quality_report::build_report(&results);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        (0..height)
            .map(|y| {
                (0..width)
                    .map(|x| buffer[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    // Narrow to a column, then a type; order by rows, then by rate.
    assert!(press(&mut app, KeyCode::Char('c')).is_none());
    assert!(app.analysis_modal.data_quality_picker.is_some());
    type_text(&mut app, "code");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    let view = app.analysis_modal.data_quality_findings.clone();
    assert_eq!(view.column.as_deref(), Some("code"));
    let listed = app.analysis_modal.quality_row_count();
    assert!(listed >= 1 && listed < report.findings.len());
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        assert!(
            screen.contains(&format!("{listed} of ")) && screen.contains("column code"),
            "the list says it is narrowed at {width}x{height}:\n{screen}"
        );
        assert!(screen.contains("Numbers as text"), "{screen}");
        assert!(!screen.contains("Missing values"), "{screen}");
        assert_glyph_slots(&screen);
    }
    assert!(
        render(&mut app, 120, 30).contains("All Findings"),
        "Esc says what it does"
    );
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.data_quality_findings.narrowed());
    assert!(
        app.analysis_modal.active,
        "Esc showed every finding; it did not leave"
    );
    press(&mut app, KeyCode::Char('t'));
    type_text(&mut app, "missing");
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.analysis_modal.data_quality_findings.check,
        Some("Missing values")
    );
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(
        app.analysis_modal.data_quality_findings.order,
        FindingOrder::Rows
    );
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(
        app.analysis_modal.data_quality_findings.order,
        FindingOrder::Rate
    );
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        let middot = datui::glyphs::get().middot;
        assert!(
            screen.contains(&format!("Missing values {middot} by rate")),
            "{screen}"
        );
        assert_glyph_slots(&screen);
    }
    // The grouped finding: each column's count, and the rows with any of them
    // bounded, not summed.
    press(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.data_quality_observation_detail);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("Null rate in 2 columns"), "{screen}");
    assert!(screen.contains("Rows with any of them"), "{screen}");
    assert!(screen.contains("not counted"), "{screen}");
    assert_glyph_slots(&screen);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Char('o'));
    assert_eq!(
        app.analysis_modal.data_quality_findings.order,
        FindingOrder::Ranked
    );

    // None of it read or measured anything.
    assert_nothing_started(&mut app, &rx);
    let after = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(after.observations.len(), results.observations.len());
    assert_eq!(
        datui::quality_report::verdict(&datui::quality_report::build_report(after)),
        datui::quality_report::verdict(&report)
    );

    // From here on only the kept rows can answer.
    std::fs::remove_file(&path).unwrap();
    let open = |app: &mut App, check: &'static str| {
        app.analysis_modal.data_quality_findings.check = Some(check);
        app.analysis_modal.data_quality_table_state.select(Some(0));
        press(app, KeyCode::Enter);
        assert!(app.analysis_modal.data_quality_observation_detail);
    };
    let identity = results.identity.clone().unwrap();
    assert!(identity.rows_involved > 0, "the sample holds copies");
    open(&mut app, "Duplicate rows");
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("Most copied"), "{screen}");
    assert!(
        screen.contains(&format!(
            "{} sampled rows that have a copy",
            identity.rows_involved
        )),
        "{screen}"
    );
    assert!(screen.contains("Show Rows"), "{screen}");
    assert_glyph_slots(&screen);
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.analysis_modal.active);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().num_rows(),
        identity.rows_involved
    );
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.active);
    assert!(
        app.analysis_modal.data_quality_observation_detail,
        "Esc from the rows is the finding again"
    );
    press(&mut app, KeyCode::Esc);

    // The code column's values that do not parse, and nothing else.
    let (_, finding) = {
        app.analysis_modal.data_quality_findings.check = Some("Numbers as text");
        app.analysis_modal.data_quality_findings.column = Some("code".to_string());
        app.analysis_modal.data_quality_table_state.select(Some(0));
        app.analysis_modal.selected_finding().unwrap()
    };
    let failures = finding.failures(&results).unwrap();
    assert!(failures > 0);
    press(&mut app, KeyCode::Enter);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("do not parse, such as \"n/a\""), "{screen}");
    assert!(
        screen.contains(&format!("{failures} sampled rows that do not parse")),
        "{screen}"
    );
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows(), failures);
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);

    // Codes that all parse: no rows to show, and the finding says why.
    app.analysis_modal.data_quality_findings.column = Some("zip".to_string());
    app.analysis_modal.data_quality_table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("every value parses"), "{screen}");
    assert!(
        screen.contains("Close") && !screen.contains("Show Rows"),
        "{screen}"
    );
    press(&mut app, KeyCode::Enter);
    assert!(!app.analysis_modal.data_quality_observation_detail);
    assert!(app.analysis_modal.active && !app.is_busy());
}

/// A full scan keeps no rows, so a finding's rows are a read of their own: Enter
/// shows what it would read, Esc reads nothing, and only Enter on that reads.
#[test]
fn full_scan_evidence_is_read_only_on_confirm() {
    use datui::data_quality::QualityPrecision;

    let (mut app, rx, tx, _path) = open_findings_fixture("dq_findings_full.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    app.analysis_modal.data_quality_plan.method = datui::sampling::SampleMethod::EveryRow;
    app.analysis_modal.data_quality_plan.compute = datui::data_quality::QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.data_quality_confirm_run);
    let next = press(&mut app, KeyCode::Enter);
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);
    let results = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(results.precision, QualityPrecision::Exact);
    let identity = results.identity.clone().unwrap();
    assert_eq!(
        identity.rows_involved, 900,
        "three copies of three hundred rows"
    );
    assert!(
        identity.examples.is_empty(),
        "a full scan keeps no examples"
    );

    app.analysis_modal.data_quality_findings.check = Some("Duplicate rows");
    app.analysis_modal.data_quality_table_state.select(Some(0));
    press(&mut app, KeyCode::Enter);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("A full scan keeps no rows"), "{screen}");
    assert!(screen.contains("Read Rows"), "{screen}");

    // Enter stages the read and shows it; nothing reads yet.
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.data_quality_evidence_read.is_some());
    assert_nothing_started(&mut app, &rx);
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        for label in [
            "Read Rows",
            "Why",
            "Reads",
            "Shows",
            "900 rows",
            "read only",
        ] {
            assert!(
                screen.contains(label),
                "{label} at {width}x{height}: {screen}"
            );
        }
        assert!(
            screen.contains("Enter") && screen.contains("Cancel"),
            "{screen}"
        );
        assert_glyph_slots(&screen);
    }
    // Esc reads nothing and goes back to the finding.
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.data_quality_evidence_read.is_none());
    assert!(app.analysis_modal.data_quality_observation_detail);
    assert_nothing_started(&mut app, &rx);

    // Enter, and Enter again: the read, and exactly the rows the check counted.
    press(&mut app, KeyCode::Enter);
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.analysis_modal.active);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 900);
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.active);
}

/// One sample for every tool: chosen once in Describe, it is the rows Data Quality
/// reads too, and the header says which rows those are. Esc in the form discards.
#[test]
fn one_sample_serves_every_analysis_tool() {
    use datui::analysis_modal::AnalysisTool;
    use datui::data_quality::QualityScope;

    let dir = common::fixture_dir();
    let path = dir.join("shared_sample.parquet");
    let sizes = [("a", 900usize), ("b", 90), ("c", 10)];
    let part: Vec<&str> = sizes
        .iter()
        .flat_map(|(name, size)| std::iter::repeat_n(*name, *size))
        .collect();
    let mut df = df!(
        "part" => &part,
        "value" => (0..part.len() as i64).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let key =
        |app: &mut App, code| app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    let run = |app: &mut App, next: Option<AppEvent>| {
        let mut next = next;
        while let Some(ev) = next {
            next = app.event(&ev);
        }
        drain_events(app, &rx);
    };

    key(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(0));
    show_sample_form(&mut app);
    let next = key(&mut app, KeyCode::Enter);
    run(&mut app, next);
    key(&mut app, KeyCode::Tab);

    // The bar names the key at the baseline width, or the sample is a feature
    // nobody finds.
    let narrow = Rect::new(0, 0, 80, 24);
    let mut buffer = Buffer::empty(narrow);
    app.render(narrow, &mut buffer);
    let bar: String = (0..narrow.width)
        .map(|x| buffer[(x, narrow.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Sample"),
        "s Sample on the bar at 80 columns: {bar}"
    );

    // Esc discards the form's edits.
    key(&mut app, KeyCode::Char('s'));
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Right);
    key(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.sample_form.is_none());
    assert_eq!(
        app.analysis_modal.sample.method,
        datui::sampling::SampleMethod::Spread
    );

    key(&mut app, KeyCode::Char('s'));
    app.analysis_modal
        .sample_form
        .as_mut()
        .unwrap()
        .set_scope(&QualityScope::parse_command("partition part=b,c").unwrap());
    let next = key(&mut app, KeyCode::Enter);
    run(&mut app, next);
    let describe = app.analysis_modal.describe_results.as_ref().unwrap();
    assert_eq!(describe.total_rows, 100, "only partitions b and c");
    let area = Rect::new(0, 0, 100, 24);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains("all 100 rows") && screen.contains("part=b,c"),
        "the header names the rows read"
    );

    // v shows the sample itself in the table viewer; Esc brings back the table
    // and the tool as they were.
    let next = key(&mut app, KeyCode::Char('v'));
    run(&mut app, next);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.analysis_modal.active, "the sample replaces the tool");
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 100);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen = rendered_text(&buffer);
    assert!(
        screen.contains("Sample") && screen.contains("Esc") && screen.contains("Back"),
        "the view says what it is and the way out"
    );
    key(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.active);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 1_000);
    assert!(app.analysis_modal.describe_results.is_some());

    // Data Quality starts from the same rows without being told again, but opens
    // its Setup rather than reading: only its Run reads.
    app.analysis_modal.focus = datui::analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(3));
    let next = key(&mut app, KeyCode::Enter);
    assert!(
        app.analysis_modal.sample_form.is_none(),
        "the sample already chosen is not asked for again"
    );
    assert!(next.is_none(), "choosing Data Quality reads nothing");
    assert!(!app.is_busy());
    assert_eq!(
        app.analysis_modal.data_quality_page,
        datui::data_quality::QualityPage::Setup
    );
    let next = key(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    run(&mut app, next);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(
        app.analysis_modal.data_quality_plan.scope,
        QualityScope::SourcePartition {
            column: "part".to_string(),
            value: "b,c".to_string()
        }
    );
    let quality = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(quality.total_rows, Some(100));
    let mut buffer = Buffer::empty(narrow);
    app.render(narrow, &mut buffer);
    let bar: String = (0..narrow.width)
        .map(|x| buffer[(x, narrow.height - 1)].symbol().to_string())
        .collect();
    // At 80 columns the report's bar keeps Setup, whose first row is the sample.
    assert!(
        bar.contains("e  Setup"),
        "e Setup on the Data Quality bar at 80 columns: {bar}"
    );
    // The seed is typed: arriving on it selects it, so one key makes it that key.
    key(&mut app, KeyCode::Char('s'));
    for _ in 0..8 {
        if app.analysis_modal.sample_form.as_ref().unwrap().field
            == datui::sample_modal::SampleField::Seed
        {
            break;
        }
        key(&mut app, KeyCode::Down);
    }
    assert_eq!(
        app.analysis_modal.sample_form.as_ref().unwrap().field,
        datui::sample_modal::SampleField::Seed
    );
    key(&mut app, KeyCode::Char('7'));
    // The form's Enter applies to Setup's draft; the sample every tool reads
    // changes only when Run commits it.
    let next = key(&mut app, KeyCode::Enter);
    assert!(next.is_none(), "applying the sample reads nothing");
    assert_eq!(app.analysis_modal.data_quality_plan.sample_seed, 7);
    assert_ne!(app.analysis_modal.sample.seed, 7);
    let next = key(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    assert_eq!(app.analysis_modal.sample.seed, 7);
    run(&mut app, next);

    // The tool list is the same beside every tool: the active one carries the
    // accent, not a dot only Data Quality drew.
    let screen = rendered_text(&buffer);
    assert!(!screen.contains(&format!("{} Data Quality", datui::glyphs::get().middot)));
}

/// A plan that needs a run is a form the user answers with Enter, which only the
/// main pane hears. The cursor must move into it when the ceremony opens: left on
/// the sidebar, ↑↓ went on moving the tool selector and Enter only reselected the
/// tool, and there was no visible way to run the plan at all.
#[test]
fn the_data_quality_ceremony_takes_the_cursor_with_it() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityPage;

    let (mut app, _rx, _tx) = open_query_filter_fixture("dq_ceremony_focus.csv");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    // A full-scan plan requires confirmation, so running the tool opens the
    // ceremony instead of reading at once.
    app.analysis_modal.sample.method = datui::sampling::SampleMethod::EveryRow;
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        next.is_none(),
        "a confirming plan must not run on selection"
    );
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(
        app.analysis_modal.focus,
        AnalysisFocus::Main,
        "the ceremony owns the keys"
    );

    // The run asked for confirmation, and Enter confirms — no Tab required.
    assert!(app.analysis_modal.data_quality_confirm_run);
    let next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
}

/// e opens the plan editor, and the editor owns the keyboard: ↑↓ change the field,
/// never the sidebar's tool selector. Tab is swallowed while the editor is open,
/// so unless the cursor moves in with e, no key could ever reach a field.
#[test]
fn e_moves_the_cursor_into_the_plan_editor() {
    use datui::analysis_modal::AnalysisFocus;

    let (mut app, rx, _tx) = open_query_filter_fixture("dq_editor_focus.csv");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.data_quality_results.is_some());
    // From the tool list: the case where e has to bring the cursor along.
    app.analysis_modal.focus = AnalysisFocus::Sidebar;

    let tool_row = app.analysis_modal.sidebar_state.selected();
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_page,
        datui::data_quality::QualityPage::Setup
    );
    assert_eq!(
        app.analysis_modal.focus,
        AnalysisFocus::Main,
        "e moves the cursor onto the plan"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.analysis_modal.data_quality_plan_field, 1,
        "the arrow changes the field"
    );
    assert_eq!(
        app.analysis_modal.sidebar_state.selected(),
        tool_row,
        "the tool selector never moves"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Up,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.analysis_modal.data_quality_plan_field, 0);
}

/// r works from the sidebar too, on a sampled report: it runs at once, with a new
/// seed. A plan that reads every row has no sample to draw again, so r there
/// raises no confirmation over the report and changes nothing.
#[test]
fn r_from_the_sidebar_runs_a_sampled_report_again() {
    use datui::analysis_modal::AnalysisFocus;

    let (mut app, rx, _tx) = open_query_filter_fixture("dq_run_key_focus.csv");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    app.analysis_modal.focus = AnalysisFocus::Sidebar;

    let full = {
        let mut plan = app.analysis_modal.data_quality_plan.clone();
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = datui::data_quality::QualityCompute::Full;
        plan
    };
    let sampled = std::mem::replace(&mut app.analysis_modal.data_quality_plan, full.clone());
    assert!(press(&mut app, KeyCode::Char('r')).is_none());
    assert!(!app.analysis_modal.data_quality_confirm_run);
    assert_eq!(app.analysis_modal.data_quality_plan, full);

    app.analysis_modal.data_quality_plan = sampled;
    let seed = app.analysis_modal.data_quality_plan.sample_seed;
    let next = press(&mut app, KeyCode::Char('r'));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    assert_ne!(app.analysis_modal.data_quality_plan.sample_seed, seed);
}

/// A table whose times are text in a US format, the way many CSV exports write
/// them: nothing reads them as time until Setup is told how.
fn open_text_times_fixture(
    name: &str,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    PathBuf,
) {
    let dir = common::fixture_dir();
    let path = dir.join(name);
    let rows = 600i64;
    let created = (0..rows)
        .map(|row| {
            (row % 150 != 7).then(|| {
                format!(
                    "01/{:02}/2024 {:02}:{:02}:00",
                    1 + row % 5,
                    8 + row % 10,
                    row % 60
                )
            })
        })
        .collect::<Vec<_>>();
    let mut created = created;
    created[11] = Some("not a time".to_string());
    let sent = (0..rows)
        .map(|row| {
            Some(format!(
                "01/{:02}/2024 {:02}:{:02}:00",
                1 + row % 5,
                9 + row % 10,
                row % 60
            ))
        })
        .collect::<Vec<_>>();
    let mut df = df!(
        "id" => (0..rows).collect::<Vec<_>>(),
        "created" => created,
        "sent" => sent,
        "region" => (0..rows).map(|row| ["West", "East"][row as usize % 2]).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx, path)
}

/// Handle events until the work is done, counting the Data Quality runs that
/// finished and the stages that were reported on the way.
fn drain_quality(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    first: Option<AppEvent>,
) -> (usize, usize) {
    let (mut finished, mut stages) = (0, 0);
    let mut handle = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            match &event {
                AppEvent::BackgroundDataQualityReady { .. } => finished += 1,
                AppEvent::BackgroundQualityPhase { generation, .. }
                    if *generation == app.task_generation() =>
                {
                    stages += 1
                }
                _ => {}
            }
            next = app.event(&event);
        }
    };
    if let Some(event) = first {
        handle(app, event);
    }
    while let Some(event) = next_event(app, rx) {
        handle(app, event);
    }
    (finished, stages)
}

/// Nothing Setup does reads: not choosing Data Quality after another tool
/// sampled, not the Sample form's Enter, not a row's choice. Esc puts back all of
/// it, the shared sample included; declining a full read leaves the sample and
/// the report as they were; one Run is one read; and the same setup again is the
/// report already here.
#[test]
fn data_quality_reads_nothing_until_setup_runs() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{QualityGrain, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_setup_reads_nothing.parquet");
    press(&mut app, KeyCode::Char('a'));
    // Describe first: its sample is the one every tool reads from now on.
    app.analysis_modal.sidebar_state.select(Some(0));
    show_sample_form(&mut app);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert!(app.analysis_modal.describe_results.is_some());
    let shared = app.analysis_modal.sample.clone();

    // Choosing Data Quality opens Setup on that sample, and reads nothing.
    app.analysis_modal.focus = datui::analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.data_quality_plan.sample(), shared);
    assert!(app.analysis_modal.data_quality_results.is_none());

    // The Sample form's Enter applies to Setup and returns to it.
    press(&mut app, KeyCode::Char('s'));
    assert!(app.analysis_modal.sample_form.is_some());
    let form = app.analysis_modal.sample_form.as_mut().unwrap();
    form.seed.set_value("99");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.sample_form.is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.data_quality_plan.sample_seed, 99);
    assert_eq!(
        app.analysis_modal.sample, shared,
        "the shared sample waits for Run"
    );
    // A row's choice, in place and from its list, reads nothing either.
    app.analysis_modal.data_quality_plan_field = SetupRow::Compare.index();
    assert!(press(&mut app, KeyCode::Right).is_none());
    app.analysis_modal.data_quality_plan_field = SetupRow::Grain.index();
    assert!(press(&mut app, KeyCode::Char(' ')).is_none());
    type_text(&mut app, "chunks of 100,");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(
        app.analysis_modal.data_quality_plan.grain,
        QualityGrain::RowChunks(100_000)
    );
    assert!(press(&mut app, KeyCode::Char('p')).is_none());
    press(&mut app, KeyCode::Esc);
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert!(app.analysis_modal.describe_results.is_some());

    // Esc discards every staged edit, the sample's included.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_plan.sample(), shared);
    assert_eq!(
        app.analysis_modal.data_quality_plan.grain,
        QualityGrain::Dataset
    );
    assert_eq!(
        app.analysis_modal.focus,
        datui::analysis_modal::AnalysisFocus::Sidebar,
        "with no report yet, the cursor goes back to the tools"
    );

    // One Run: one read, one report.
    press(&mut app, KeyCode::Tab);
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    let (finished, stages) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1, "one Run dispatches once");
    assert!(stages > 0, "the run names its stages");
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    let report = app.analysis_modal.data_quality_results.clone().unwrap();

    // Declining a full read leaves the sample and the report as they were, and
    // the draft staged.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.method = datui::sampling::SampleMethod::EveryRow;
    app.analysis_modal.data_quality_plan.compute = datui::data_quality::QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.data_quality_confirm_run);
    assert!(press(&mut app, KeyCode::Esc).is_none());
    assert!(!app.analysis_modal.data_quality_confirm_run);
    assert_eq!(app.analysis_modal.sample, shared);
    assert_eq!(
        app.analysis_modal
            .data_quality_results
            .as_ref()
            .unwrap()
            .evaluated_rows,
        report.evaluated_rows
    );
    assert!(
        app.analysis_modal.setup_edited(),
        "the draft is still staged"
    );
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);

    // The setup the report was measured with is the report: no read.
    press(&mut app, KeyCode::Char('e'));
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(rendered_text(&buffer).contains("Run shows it, no read"));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);

    // Reopened, the unchanged report comes back from the session cache.
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.active);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert!(app.analysis_modal.data_quality_from_cache);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
}

/// Run from Setup, handle events until the work is done, and return the stages
/// that read the source, in order. Empty when the run read nothing.
fn run_quality_reads(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
) -> Vec<datui::data_quality::QualityStage> {
    let first = press(app, KeyCode::Enter);
    assert!(
        matches!(first, Some(AppEvent::AnalysisDataQualityCompute)),
        "Enter runs"
    );
    let mut reads = Vec::new();
    let mut handle = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            if let AppEvent::BackgroundQualityPhase { generation, phase } = &event
                && *generation == app.task_generation()
                && phase.reads_source
            {
                reads.push(phase.stage);
            }
            next = app.event(&event);
        }
    };
    handle(app, first.unwrap());
    while let Some(event) = next_event(app, rx) {
        handle(app, event);
    }
    assert!(app.analysis_modal.data_quality_results.is_some());
    reads
}

/// Each edit reads only what it must, counted by the stages of each run that read
/// the source: #415's "What edits should cost", on a CSV the sampler streams. The
/// first daily run counts its days in the pass that samples. Roles, a coarser window
/// the days nest in, and row chunks read nothing. A partition is counted once. A new
/// seed, size or scope is a new sample, and an earlier seed's rows are still here.
/// Setup says each of these before Run.
#[test]
fn data_quality_edits_read_only_what_they_must() {
    use datui::data_quality::{
        QualityGrain, QualityScope, QualityStage, TemporalRole, TemporalRoleAssignment,
    };

    let dir = common::fixture_dir();
    let path = dir.join("dq_reuse_reads.csv");
    // Every 97 minutes from 2024-01-01, across weeks and months.
    let minute = 60_000_000i64;
    let start = 1_704_067_200_000_000i64;
    let micros = |offset: i64| {
        (0..3_000i64)
            .map(|row| start + row * 97 * minute + offset)
            .collect::<Vec<_>>()
    };
    let datetime = DataType::Datetime(TimeUnit::Microseconds, None);
    let mut df = df!(
        "id" => (0..3_000i64).collect::<Vec<_>>(),
        "at" => micros(0),
        "sent" => micros(40_000_000),
        "region" => (0..3_000).map(|row| ["North", "South", "East"][row % 3]).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_columns([col("at").cast(datetime.clone()), col("sent").cast(datetime)])
    .collect()
    .unwrap();
    CsvWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let setup = |app: &mut App| {
        let area = Rect::new(0, 0, 120, 40);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };
    let window = |every: &str| QualityGrain::TimeWindows {
        column: "at".into(),
        every: every.into(),
    };
    let seed = app.analysis_modal.data_quality_plan.sample_seed;
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.dataset_rows = 300;
        plan.grain = window("1d");
    }
    assert!(setup(&mut app).contains("Counts every row by at in that pass"));
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [QualityStage::ReadingSample],
        "one pass samples and counts the days"
    );

    // Edits that change only the report.
    let edit = |app: &mut App, change: &dyn Fn(&mut datui::data_quality::DataQualityPlan)| {
        press(app, KeyCode::Char('e'));
        change(&mut app.analysis_modal.data_quality_plan);
    };
    edit(&mut app, &|plan| {
        plan.temporal_roles = vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "at".into(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "sent".into(),
                timezone: None,
            },
        ]
    });
    let text = setup(&mut app);
    assert!(text.contains("Uses the rows a run already read"), "{text}");
    assert!(text.contains("Segment totals from a count already read"));
    assert!(run_quality_reads(&mut app, &rx).is_empty(), "a role edit");
    assert!(
        !app.analysis_modal
            .data_quality_results
            .as_ref()
            .unwrap()
            .temporal
            .is_empty()
    );
    edit(&mut app, &|plan| plan.grain = window("1w"));
    assert!(setup(&mut app).contains("summed from the daily counts already read"));
    assert!(
        run_quality_reads(&mut app, &rx).is_empty(),
        "weeks from days"
    );
    edit(&mut app, &|plan| plan.grain = QualityGrain::RowChunks(500));
    assert!(run_quality_reads(&mut app, &rx).is_empty(), "row chunks");
    edit(&mut app, &|plan| {
        plan.grain = QualityGrain::Partition("region".into())
    });
    assert!(setup(&mut app).contains("Plus one count of region"));
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [QualityStage::CountingSegments],
        "a partition is counted once"
    );

    // Other rows: a new sample, counted in its pass.
    for change in [
        &(|plan: &mut datui::data_quality::DataQualityPlan| plan.sample_seed = 7)
            as &dyn Fn(&mut datui::data_quality::DataQualityPlan),
        &|plan| plan.dataset_rows = 400,
        &|plan| plan.scope = QualityScope::FirstRows(2_000),
    ] {
        edit(&mut app, change);
        let text = setup(&mut app);
        assert!(
            text.contains("One pass that streams every eligible row"),
            "{text}"
        );
        assert!(
            text.contains("Counts every row by region in that pass"),
            "{text}"
        );
        assert_eq!(
            run_quality_reads(&mut app, &rx),
            [QualityStage::ReadingSample]
        );
    }

    // The first sample's rows are still held, with their counts.
    edit(&mut app, &|plan| {
        plan.scope = QualityScope::CurrentView;
        plan.dataset_rows = 300;
        plan.sample_seed = seed;
        plan.grain = window("1mo");
    });
    let text = setup(&mut app);
    assert!(text.contains("Uses the rows a run already read"), "{text}");
    assert!(
        text.contains("summed from the daily counts already read"),
        "{text}"
    );
    assert!(
        run_quality_reads(&mut app, &rx).is_empty(),
        "an earlier seed"
    );
}

/// On one Parquet file the sampler reads seeded runs, which see too few rows to
/// count segments, so the run counts them in a pass of its own, and Setup says so
/// before Run: a sort leaves the unsorted scan the sample reads, and the whole
/// source is read as loaded whatever the view shows. A filtered view streams, and
/// counts in that pass.
#[test]
fn data_quality_setup_names_every_count_pass_on_one_parquet_file() {
    use datui::data_quality::{QualityGrain, QualityScope, QualityStage};
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

    let dir = common::fixture_dir();
    let path = dir.join("dq_reuse_blocks.parquet");
    let minute = 60_000_000i64;
    let start = 1_704_067_200_000_000i64;
    let mut df = df!(
        "id" => (0..20_000i64).collect::<Vec<_>>(),
        "at" => (0..20_000i64).map(|row| start + row * 97 * minute).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("at").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
    .collect()
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let setup = |app: &mut App| {
        let area = Rect::new(0, 0, 120, 40);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };
    let sorted = || AppEvent::Sort(vec!["id".into()], vec![true]);
    let filtered = || {
        AppEvent::Filter(vec![FilterStatement {
            column: "id".into(),
            operator: FilterOperator::Gt,
            value: "10".into(),
            logical_op: LogicalOperator::And,
        }])
    };
    for (name, view, scope, blocks) in [
        ("as loaded", None, QualityScope::CurrentView, true),
        ("sorted", Some(sorted()), QualityScope::CurrentView, true),
        ("sorted", Some(sorted()), QualityScope::WholeSource, true),
        (
            "filtered",
            Some(filtered()),
            QualityScope::WholeSource,
            true,
        ),
        (
            "filtered",
            Some(filtered()),
            QualityScope::CurrentView,
            false,
        ),
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        if let Some(view) = &view {
            let mut next = app.event(view);
            while let Some(event) = next {
                next = app.event(&event);
            }
            pump_until_idle(&mut app, &rx, &tx);
        }
        press(&mut app, KeyCode::Char('a'));
        app.analysis_modal.sidebar_state.select(Some(3));
        show_sample_form(&mut app);
        {
            let plan = &mut app.analysis_modal.data_quality_plan;
            plan.dataset_rows = 300;
            plan.scope = scope.clone();
            plan.grain = QualityGrain::TimeWindows {
                column: "at".into(),
                every: "1d".into(),
            };
        }
        let text = setup(&mut app);
        let reads = run_quality_reads(&mut app, &rx);
        if blocks {
            assert!(
                text.contains("Seeded runs of the file") && text.contains("Plus one count of at"),
                "{name} {scope:?}: Setup said\n{text}"
            );
            assert_eq!(
                reads,
                [QualityStage::ReadingSample, QualityStage::CountingSegments],
                "{name} {scope:?}"
            );
        } else {
            assert!(
                text.contains("One pass that streams")
                    && text.contains("Counts every row by at in that pass"),
                "{name} {scope:?}: Setup said\n{text}"
            );
            assert_eq!(reads, [QualityStage::ReadingSample], "{name} {scope:?}");
        }
    }
}

/// Every report carries its coverage under the verdict: a sampled run says which
/// checks ran on the sample, which it could not answer and why, and the rows behind
/// it; a full run says every row was read and how many rows its passes traversed.
#[test]
fn data_quality_coverage_sits_under_every_verdict() {
    use datui::data_quality::{QualityCompute, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_coverage.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    let draw = |app: &mut App| {
        let area = Rect::new(0, 0, 80, 24);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };

    app.analysis_modal.data_quality_plan.dataset_rows = 100;
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    let text = draw(&mut app);
    assert!(text.contains("Checks  "), "{text}");
    assert!(text.contains("unavailable"), "{text}");
    assert!(text.contains("100 of 600 sampled (16.7%)"), "{text}");
    assert!(
        text.contains("100 traversed"),
        "seeded runs of one Parquet file read only the rows they keep: {text}"
    );
    assert!(
        text.contains("Nearly unique: needs every row checked"),
        "{text}"
    );

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.method = datui::sampling::SampleMethod::EveryRow;
    app.analysis_modal.data_quality_plan.compute = QualityCompute::Full;
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.data_quality_confirm_run);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    let text = draw(&mut app);
    assert!(text.contains("all 600 read, exact"), "{text}");
    assert!(!text.contains("unavailable"), "{text}");
    let reads = app
        .analysis_modal
        .data_quality_results
        .as_ref()
        .and_then(|results| results.reads)
        .unwrap();
    assert!(
        reads.counted > 0 && reads.rows >= 600,
        "every watched pass counted: {reads:?}"
    );
}

/// However Setup is reached, Esc discards what was staged in it: after Esc handed
/// the cursor to the tools and Tab brought it back, and after a report tab key
/// pressed in Setup, which does not leave it. A draft never rides out of Setup
/// into `r`.
#[test]
fn data_quality_setup_edits_never_outlive_esc() {
    use datui::analysis_modal::{AnalysisFocus, SetupRow};
    use datui::data_quality::{QualityComparison, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_setup_esc_discards.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);

    // No report yet: Esc goes to the tools, Tab comes back, and an edit there is
    // still a staged one.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Sidebar);
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    app.analysis_modal.data_quality_plan_field = SetupRow::Compare.index();
    press(&mut app, KeyCode::Right);
    assert_ne!(
        app.analysis_modal.data_quality_plan.comparison,
        QualityComparison::None
    );
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.data_quality_plan.comparison,
        QualityComparison::None,
        "Esc discards an edit made after Tab"
    );

    // A report, then a staged edit and a report tab's key: Setup stays, and so does
    // the draft, until Esc takes it away.
    press(&mut app, KeyCode::Tab);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    let seed = app.analysis_modal.data_quality_plan.sample_seed;
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan_field = SetupRow::Compare.index();
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Char('2'));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    assert_eq!(
        app.analysis_modal.data_quality_plan.comparison,
        QualityComparison::None
    );

    // r on the report runs the plan the report was measured with, a new seed aside.
    let next = press(&mut app, KeyCode::Char('r'));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    drain_quality(&mut app, &rx, next);
    let plan = &app.analysis_modal.data_quality_plan;
    assert_ne!(plan.sample_seed, seed);
    assert_eq!(plan.comparison, QualityComparison::None);

    // A report of every row has no sample to draw again: r does nothing there.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.method = datui::sampling::SampleMethod::EveryRow;
    app.analysis_modal.data_quality_plan.compute = datui::data_quality::QualityCompute::Full;
    press(&mut app, KeyCode::Enter);
    let next = press(&mut app, KeyCode::Enter);
    drain_quality(&mut app, &rx, next);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
    let full = app.analysis_modal.data_quality_plan.clone();
    assert!(press(&mut app, KeyCode::Char('r')).is_none());
    assert!(!app.analysis_modal.data_quality_confirm_run);
    assert_eq!(app.analysis_modal.data_quality_plan, full);
}

/// Time stored as text: a role on it says, before Run, that it needs a format;
/// Text as time offers the formats that read the values on screen; and the run
/// then windows by it and measures the time between two of them, counting what
/// the format does not read on its own.
#[test]
fn text_read_as_time_in_setup_gives_windows_and_intervals() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{ObservationKind, QualityPage};

    let (mut app, rx, _tx, _path) = open_text_times_fixture("dq_setup_text_times.parquet");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);

    // Roles on the two text columns: event on created, received on sent.
    app.analysis_modal.data_quality_plan_field = SetupRow::TimeRoles.index();
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::TimeRoles);
    let candidates = |app: &App| app.analysis_modal.data_quality_plan.temporal_roles.clone();
    press(&mut app, KeyCode::Right);
    while candidates(&app)[0].column != "created" {
        press(&mut app, KeyCode::Right);
    }
    for _ in 0..5 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Right);
    while candidates(&app)
        .iter()
        .find(|role| role.role == datui::data_quality::TemporalRole::Received)
        .unwrap()
        .column
        != "sent"
    {
        press(&mut app, KeyCode::Right);
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    let area = Rect::new(0, 0, 100, 30);
    let render = |app: &mut App| {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };
    let screen = render(&mut app);
    assert!(
        screen.contains("created is text: choose its format under Text as time"),
        "{screen}"
    );

    // Text as time: the column, then the format that reads what is on screen.
    for column in ["created", "sent"] {
        app.analysis_modal.data_quality_plan_field = SetupRow::TextAsTime.index();
        assert!(press(&mut app, KeyCode::Char(' ')).is_none());
        type_text(&mut app, column);
        press(&mut app, KeyCode::Enter);
        let picker = app.analysis_modal.data_quality_picker.as_ref().unwrap();
        assert_eq!(picker.title, format!("Read {column} As"));
        assert!(
            picker.state.filtered()[0]
                .1
                .ends_with("datetime %m/%d/%Y %H:%M:%S"),
            "the format that reads the values on screen leads: {:?}",
            picker.state.filtered()[0]
        );
        assert!(press(&mut app, KeyCode::Enter).is_none());
    }
    let screen = render(&mut app);
    assert!(!screen.contains("is text: choose"), "{screen}");
    assert!(screen.contains("created read as datetime %m/%d/%Y %H:%M:%S"));
    assert!(
        screen.contains("Intervals      event to received"),
        "{screen}"
    );

    // A day window of created, now that it reads as time.
    app.analysis_modal.data_quality_plan_field = SetupRow::Grain.index();
    press(&mut app, KeyCode::Char(' '));
    type_text(&mut app, "day of created");
    press(&mut app, KeyCode::Enter);
    assert!(!app.is_busy());

    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    drain_quality(&mut app, &rx, next);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    let labels = results
        .segments
        .iter()
        .map(|segment| segment.label.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        labels,
        [
            "2024-01-01",
            "2024-01-02",
            "2024-01-03",
            "2024-01-04",
            "2024-01-05",
            "created ∅"
        ]
    );
    assert!(!results.temporal.is_empty(), "the interval is measured");
    let unparsed: usize = results.temporal.iter().map(|t| t.unparsed_start).sum();
    let missing: usize = results.temporal.iter().map(|t| t.missing_start).sum();
    assert_eq!((unparsed, missing), (1, 4), "unread and missing apart");
    let finding = results
        .observations
        .iter()
        .find(|observation| observation.kind == ObservationKind::UnparsedTime)
        .expect("text the format does not read is a finding");
    assert_eq!(finding.column, "created");
    assert_eq!(finding.affected_rows, 1);
    // The column itself is still text to every other check.
    let created = results
        .columns
        .iter()
        .find(|column| column.name == "created")
        .unwrap();
    assert_eq!(created.dtype, polars::prelude::DataType::String);
}

/// Intervals from Setup to a count's rows: a start and end the roles do not
/// suggest is chosen in Setup, with the roles it leaves out named first; the
/// window clock and threshold are set there too. The report's list and an
/// interval's detail read nothing, and a count's rows open from the sample the
/// run kept, even once the file is gone.
#[test]
fn intervals_are_chosen_in_setup_and_inspected_without_a_read() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{IntervalClock, IntervalFact, QualityPage, QualityPrecision};

    let name = "dq_intervals_detail.parquet";
    let (mut app, rx, tx, path) = open_text_times_fixture(name);
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };

    for column in ["created", "sent"] {
        app.analysis_modal.data_quality_plan_field = SetupRow::TextAsTime.index();
        press(&mut app, KeyCode::Char(' '));
        type_text(&mut app, column);
        press(&mut app, KeyCode::Enter);
        press(&mut app, KeyCode::Enter);
    }
    // Created on created, processed on sent: roles no suggested pair joins.
    app.analysis_modal.data_quality_plan_field = SetupRow::TimeRoles.index();
    press(&mut app, KeyCode::Char(' '));
    for _ in 0..3 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Right);
    for _ in 0..3 {
        press(&mut app, KeyCode::Down);
    }
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Enter);
    let plan = &app.analysis_modal.data_quality_plan;
    assert_eq!(
        plan.role_column(datui::data_quality::TemporalRole::Created),
        Some("created")
    );
    assert_eq!(
        plan.role_column(datui::data_quality::TemporalRole::Processed),
        Some("sent")
    );
    let screen = render(&mut app, 100, 30);
    assert!(
        screen.contains("In no interval: created, processed."),
        "Setup names the roles that measure nothing: {screen}"
    );

    // The pair, chosen: Esc puts the list back, Enter keeps it.
    app.analysis_modal.data_quality_plan_field = SetupRow::Intervals.index();
    assert!(press(&mut app, KeyCode::Char(' ')).is_none());
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::IntervalPairs
    );
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(
        app.analysis_modal.data_quality_plan.interval_pairs().len(),
        1
    );
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_plan.intervals, None);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.setup_row(), SetupRow::Intervals);
    let screen = render(&mut app, 100, 30);
    assert!(screen.contains("created to processed"), "{screen}");
    assert!(!screen.contains("In no interval"), "{screen}");

    // A threshold, a daily grain, and intervals by the day they ended.
    app.analysis_modal.data_quality_plan_field = SetupRow::Latency.index();
    press(&mut app, KeyCode::Right);
    app.analysis_modal.data_quality_plan_field = SetupRow::Grain.index();
    press(&mut app, KeyCode::Char(' '));
    type_text(&mut app, "day of created");
    press(&mut app, KeyCode::Enter);
    app.analysis_modal.data_quality_plan_field = SetupRow::WindowBy.index();
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    let plan = &app.analysis_modal.data_quality_plan;
    assert_eq!(plan.interval_clock, IntervalClock::End);
    assert_eq!(plan.latency_threshold_seconds, Some(3_600));
    assert!(!app.is_busy(), "nothing in Setup reads");

    // A sample smaller than the file, so the rows a count opens are the sample's.
    app.analysis_modal.data_quality_plan.dataset_rows = 300;
    app.analysis_modal.data_quality_plan.sample_seed = 7;
    let next = press(&mut app, KeyCode::Enter);
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.precision, QualityPrecision::Sampled);
    assert!(!results.temporal.is_empty());
    assert!(
        results
            .temporal
            .iter()
            .all(|interval| interval.label() == "created to processed"
                && interval.threshold_seconds == Some(3_600))
    );

    // Every sent is an hour after its created: exactly the threshold, never over it.
    assert!(
        results
            .temporal
            .iter()
            .all(|interval| interval.above_threshold_count == Some(0))
    );
    // A count with rows behind it: a start missing or unread in some segment.
    let plan = app.analysis_modal.quality_result_plan().clone();
    let (index, fact, count) = results
        .temporal
        .iter()
        .enumerate()
        .find_map(|(index, interval)| {
            [IntervalFact::MissingStart, IntervalFact::UnparsedStart]
                .into_iter()
                .find_map(|fact| {
                    let (count, _) = interval.count(fact, &plan)?;
                    (count > 0).then_some((index, fact, count))
                })
        })
        .expect("the sample holds a missing or unread start");

    // The list and the detail are the report's measurements: no read.
    assert!(press(&mut app, KeyCode::Char('5')).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Intervals);
    let screen = render(&mut app, 80, 24);
    assert!(screen.contains("Over: duration > 1 hour"), "{screen}");
    assert!(screen.contains("created to processed"), "{screen}");
    for _ in 0..index {
        press(&mut app, KeyCode::Down);
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::IntervalDetail
    );
    assert!(!app.is_busy());
    for (width, height) in [(80, 24), (60, 20)] {
        let screen = render(&mut app, width, height);
        for label in [
            "Start",
            "End",
            "Segment",
            "Rows",
            "Both ends",
            "Missing start",
        ] {
            assert!(
                screen.contains(label),
                "{label} at {width}x{height}: {screen}"
            );
        }
    }

    // The count under the cursor; Enter opens its rows from memory.
    let position = app
        .analysis_modal
        .interval_facts()
        .iter()
        .position(|listed| *listed == fact)
        .unwrap();
    for _ in 0..position {
        press(&mut app, KeyCode::Down);
    }
    assert_eq!(app.analysis_modal.selected_interval_fact(), Some(fact));
    assert!(render(&mut app, 100, 30).contains("Show Rows"));
    std::fs::remove_file(&path).unwrap();
    let mut next = press(&mut app, KeyCode::Enter);
    while let Some(event) = next {
        next = app.event(&event);
    }
    drain_events(&mut app, &rx);
    pump_until_idle(&mut app, &rx, &tx);
    // With the file gone, only the kept sample could have answered.
    assert!(!app.analysis_modal.active);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), count);
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.active, "Esc goes back to the detail");
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::IntervalDetail
    );
    // Esc from the detail is the list, the interval still selected.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Intervals);
    assert_eq!(
        app.analysis_modal.data_quality_table_state.selected(),
        Some(index)
    );
}

/// Weekday rows over eight weeks, forty a day, the second week missing: what a
/// business feed looks like. Written as CSV, which the sampler streams.
fn open_weekday_feed(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let dir = common::fixture_dir();
    let path = dir.join(name);
    // 2024-01-01 is a Monday.
    let days = (0..56)
        .filter(|day| day % 7 < 5 && !(7..14).contains(day))
        .collect::<Vec<i32>>();
    let day = days
        .iter()
        .flat_map(|day| std::iter::repeat_n(19_723 + day, 40))
        .collect::<Vec<_>>();
    let rows = day.len();
    let mut df = df!(
        "day" => day,
        "amount" => (0..rows).map(|row| (row % 3 != 0).then_some(row as f64)).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("day").cast(DataType::Date))
    .collect()
    .unwrap();
    CsvWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// #415's trends and gaps. A trend's bar opens to what it spans, how much of it the
/// sample reached, its rate and how sure that is, with no read. A thin sample offers
/// a coarser window, staged in Setup for Enter, never run. Gaps appear only once
/// Setup states which windows rows are expected in, and stating them reads nothing.
#[test]
fn trends_and_gaps_are_inspected_without_a_read() {
    use datui::analysis_modal::SetupRow;
    use datui::data_quality::{QualityGrain, QualityPage, QualityStage};

    let (mut app, rx, _tx) = open_weekday_feed("dq_trends_gaps.csv");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    let render = |app: &mut App, width, height| {
        let area = Rect::new(0, 0, width, height);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };
    let daily = QualityGrain::TimeWindows {
        column: "day".into(),
        every: "1d".into(),
    };
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.dataset_rows = 30;
        plan.sample_seed = 415;
        plan.grain = daily.clone();
    }
    // No stated windows: the row says so, and Setup reads nothing for it.
    app.analysis_modal.data_quality_plan_field = SetupRow::Expected.index();
    assert!(render(&mut app, 100, 30).contains("none: no window is a gap"));
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [QualityStage::ReadingSample],
        "one pass samples and counts the days"
    );
    let results = app.analysis_modal.data_quality_results.clone().unwrap();
    assert!(
        !results.unsampled_segments.is_empty(),
        "thirty rows miss some of 35 days"
    );

    // Trends says how many days the sample missed, and offers a coarser window.
    press(&mut app, KeyCode::Char('4'));
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    let trends = render(&mut app, 80, 24);
    assert!(trends.contains("not sampled"), "{trends}");
    assert!(trends.contains("w stages weekly"), "{trends}");
    assert!(
        !trends.contains("Expected"),
        "no gaps without stated windows"
    );
    assert!(press(&mut app, KeyCode::Char('g')).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);

    // A bar opens to its facts, and the bars walk, from what the report holds.
    let amount = datui::quality_trends::trend_view(
        &results,
        datui::data_quality::QualityMetric::NullRate,
        1,
    )
    .lines
    .iter()
    .position(|line| line.names == ["amount"])
    .expect("an amount line");
    app.analysis_modal
        .data_quality_table_state
        .select(Some(amount));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::TrendDetail
    );
    assert_eq!(app.analysis_modal.data_quality_trend_line, amount);
    for size in [(80, 24), (60, 20)] {
        let detail = render(&mut app, size.0, size.1);
        for label in ["Span", "Segments", "Rows", "Null rate", "95% interval"] {
            assert!(detail.contains(label), "{label} at {size:?}:\n{detail}");
        }
        assert!(detail.contains("bar 1 of"), "{detail}");
    }
    assert!(press(&mut app, KeyCode::Down).is_none());
    let second = render(&mut app, 80, 24);
    assert!(second.contains("bar 2 of"), "{second}");
    assert!(second.contains("Previous bar"), "{second}");
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    assert_eq!(
        app.analysis_modal.data_quality_table_state.selected(),
        Some(amount)
    );

    // `w` stages weeks in Setup: Read says the days' counts serve them, nothing runs,
    // and Esc puts the grain back.
    assert!(press(&mut app, KeyCode::Char('w')).is_none());
    assert!(!app.is_busy());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.setup_row(), SetupRow::Grain);
    assert_eq!(
        app.analysis_modal.data_quality_plan.grain,
        QualityGrain::TimeWindows {
            column: "day".into(),
            every: "1w".into(),
        }
    );
    let staged = render(&mut app, 100, 40);
    assert!(
        staged.contains("summed from the daily counts already read"),
        "{staged}"
    );
    assert!(staged.contains("Edited: Enter runs it"), "{staged}");
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_plan.grain, daily);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);

    // Expected windows, stated in Setup: weekdays, typed dates past the data. The
    // editor's fields type every key, `q` and `?` included.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan_field = SetupRow::Expected.index();
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(
        app.analysis_modal.data_quality_page,
        QualityPage::ExpectedWindows
    );
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Down);
    type_text(&mut app, "q?");
    assert!(app.analysis_modal.active, "q typed, not quit");
    press(&mut app, KeyCode::Enter);
    let editor = render(&mut app, 80, 24);
    assert!(
        editor.contains("q? is not a date or UTC timestamp"),
        "{editor}"
    );
    for _ in 0..2 {
        press(&mut app, KeyCode::Backspace);
    }
    type_text(&mut app, "2024-01-01");
    press(&mut app, KeyCode::Down);
    type_text(&mut app, "2024-03-04");
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    let expected = app
        .analysis_modal
        .data_quality_plan
        .expected
        .clone()
        .unwrap();
    assert!(expected.weekdays);
    assert_eq!(expected.before.as_deref(), Some("2024-03-04"));
    let setup = render(&mut app, 100, 40);
    assert!(setup.contains("Only Expected changed"), "{setup}");

    // Run checks them against the report on screen: no read, no run.
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    let trends = render(&mut app, 80, 24);
    assert!(trends.contains("Expected weekdays"), "{trends}");
    assert!(press(&mut app, KeyCode::Char('g')).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Gaps);
    let gaps = render(&mut app, 100, 30);
    // The missing week and the week after the data are empty; days the sample
    // missed are not.
    assert!(gaps.contains("2024-01-08 to 2024-01-12 empty"), "{gaps}");
    assert!(gaps.contains("2024-02-26 to 2024-03-01 empty"), "{gaps}");
    assert!(gaps.contains("not sampled"), "{gaps}");
    assert!(gaps.contains("on weekends, not expected"), "{gaps}");
    assert!(render(&mut app, 60, 20).contains("empty"));
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
}

/// A full scan asks before it reads. Expected windows are checked against the
/// report on screen, which reads nothing, so Run does not ask.
#[test]
fn expected_windows_on_a_full_scan_run_without_asking() {
    use datui::data_quality::{ExpectedWindows, QualityCompute, QualityGrain, QualityPage};

    let (mut app, rx, _tx) = open_weekday_feed("dq_expected_full.csv");
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
        plan.grain = QualityGrain::TimeWindows {
            column: "day".into(),
            every: "1d".into(),
        };
    }
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(
        app.analysis_modal.data_quality_confirm_run,
        "a full scan asks"
    );
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 1);

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.expected = Some(ExpectedWindows {
        weekdays: true,
        ..ExpectedWindows::default()
    });
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(
        !app.analysis_modal.data_quality_confirm_run,
        "nothing to confirm"
    );
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Trends);
    assert!(
        datui::quality_trends::expected_gaps(
            app.analysis_modal.quality_result_plan(),
            app.analysis_modal.data_quality_results.as_ref().unwrap(),
        )
        .is_some()
    );
}

/// `rows` rows written to `<name>` in a fresh fixture directory, as CSV or Parquet by its
/// extension: an id, a region, and an amount missing on every `gap`th row (never
/// with a gap of 0). Opened, with Data Quality's Setup on screen.
fn open_quality_fixture(
    name: &str,
    rows: usize,
    gap: usize,
) -> (
    App,
    mpsc::Receiver<AppEvent>,
    mpsc::Sender<AppEvent>,
    PathBuf,
) {
    let path = common::fixture_dir().join(name);
    write_quality_fixture(&path, rows, gap);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    open_quality_setup(&mut app);
    (app, rx, tx, path)
}

fn write_quality_fixture(path: &Path, rows: usize, gap: usize) {
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let amounts = (0..rows)
        .map(|row| (gap == 0 || row % gap != 0).then_some(row as f64 * 1.5))
        .collect::<Vec<_>>();
    let mut df = df!(
        "id" => (0..rows as i64).collect::<Vec<_>>(),
        "region" => (0..rows).map(|row| ["North", "South"][row % 2]).collect::<Vec<_>>(),
        "amount" => amounts,
    )
    .unwrap();
    let file = File::create(path).unwrap();
    if path.extension().is_some_and(|ext| ext == "csv") {
        CsvWriter::new(file).finish(&mut df).unwrap();
    } else {
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }
}

/// `a`, Data Quality, Enter: its Setup, which reads nothing.
fn open_quality_setup(app: &mut App) {
    press(app, KeyCode::Char('a'));
    app.analysis_modal.focus = datui::analysis_modal::AnalysisFocus::Sidebar;
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
}

fn amount_nulls(app: &App) -> usize {
    app.analysis_modal
        .data_quality_results
        .as_ref()
        .unwrap()
        .columns
        .iter()
        .find(|column| column.name == "amount")
        .unwrap()
        .null_count
}

/// A rerun that fails leaves the last report on screen, labeled with the setup it
/// was measured with, and says why it failed; the rows the first run kept still
/// serve the setup that made them, with no read.
#[test]
fn a_failed_rerun_keeps_the_last_report() {
    let (mut app, rx, _tx, path) = open_quality_fixture("dq_rerun_fails.parquet", 2_000, 10);
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.dataset_rows = 500;
        plan.sample_seed = 3;
    }
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [datui::data_quality::QualityStage::ReadingSample]
    );
    let report = format!("{:?}", app.analysis_modal.data_quality_results);
    let measured = app.analysis_modal.data_quality_last_plan.clone().unwrap();

    // The source goes away, and a new seed needs it.
    std::fs::remove_file(&path).unwrap();
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.sample_seed = 4;
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    let (finished, _) = drain_quality(&mut app, &rx, next);
    assert_eq!(finished, 0, "the rerun failed");
    assert!(app.modal_showing(), "and says why");
    assert!(!app.is_busy() && app.analysis_modal.computing.is_none());
    assert_eq!(
        format!("{:?}", app.analysis_modal.data_quality_results),
        report,
        "the last report stays"
    );
    assert_eq!(app.analysis_modal.quality_result_plan(), &measured);
    // The error is dismissed onto Setup, where the run started; Esc there is the
    // report, under the setup it was measured with.
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.data_quality_page.is_setup());
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.data_quality_page.is_setup());
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let text = rendered_text(&buffer);
    assert!(text.contains("sample of 500"), "{text}");

    // The setup the report was measured with is still served from its rows.
    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.sample_seed = 3;
    app.analysis_modal.data_quality_plan.grain =
        datui::data_quality::QualityGrain::RowChunks(100_000);
    assert!(run_quality_reads(&mut app, &rx).is_empty(), "no read");
    assert!(!app.modal_showing());
}

/// `d` in Setup releases the rows runs kept: Read names them before, the report on
/// screen stays, and an edit that would have reused them reads its sample again,
/// as Read says before Run.
#[test]
fn kept_rows_are_released_from_setup() {
    let (mut app, rx, _tx, _path) = open_quality_fixture("dq_release_rows.parquet", 2_000, 10);
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.dataset_rows = 500;
        plan.sample_seed = 3;
    }
    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [datui::data_quality::QualityStage::ReadingSample]
    );
    let screen = |app: &mut App| {
        let area = Rect::new(0, 0, 160, 40);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        rendered_text(&buffer)
    };

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan.grain = datui::data_quality::QualityGrain::RowChunks(100);
    let text = screen(&mut app);
    assert!(text.contains("500 rows kept"), "{text}");
    assert!(text.contains("Release Rows"), "{text}");
    assert!(text.contains("Uses the rows a run already read"), "{text}");

    assert!(press(&mut app, KeyCode::Char('d')).is_none());
    assert!(!app.is_busy(), "releasing reads nothing");
    let flash = app.flash_message().unwrap_or_default().to_string();
    assert!(
        flash.starts_with("Released 500 kept rows") && flash.ends_with("the next run reads again"),
        "{flash}"
    );
    assert!(
        app.analysis_modal.data_quality_results.is_some(),
        "the report stays"
    );
    assert!(app.analysis_modal.data_quality_page.is_setup(), "and Setup");
    let text = screen(&mut app);
    assert!(!text.contains("rows kept"), "{text}");
    assert!(!text.contains("Release Rows"), "{text}");
    assert!(text.contains("Read before and released since"), "{text}");
    press(&mut app, KeyCode::Char('d'));
    assert_eq!(app.flash_message(), Some("Nothing kept to release"));

    assert_eq!(
        run_quality_reads(&mut app, &rx),
        [datui::data_quality::QualityStage::ReadingSample],
        "the sample is read again"
    );
    press(&mut app, KeyCode::Char('e'));
    assert!(screen(&mut app).contains("500 rows kept"), "and kept again");
}

/// The rows a run read are a snapshot of the session: a file changed on disk is
/// not noticed until it is opened again, as the reference says. Reopened, the
/// dataset is new, and Run reads the file as it is now.
#[test]
fn a_file_changed_on_disk_is_read_again_once_opened_again() {
    let (mut app, rx, tx, path) = open_quality_fixture("dq_changed_on_disk.csv", 1_000, 0);
    assert!(!run_quality_reads(&mut app, &rx).is_empty());
    assert_eq!(amount_nulls(&app), 0);

    write_quality_fixture(&path, 1_000, 2);
    // Back to the report: the session's, from its cache.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    assert!(!app.analysis_modal.active);
    open_quality_setup(&mut app);
    assert!(app.analysis_modal.data_quality_from_cache);
    assert_eq!(amount_nulls(&app), 0, "the snapshot, not the file");

    // Opened again: a new dataset, and a run that reads it.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    open_quality_setup(&mut app);
    assert!(!app.analysis_modal.data_quality_from_cache);
    assert!(app.analysis_modal.data_quality_page.is_setup());
    assert!(!run_quality_reads(&mut app, &rx).is_empty());
    assert_eq!(amount_nulls(&app), 500, "the file as it is now");
}

/// File metadata only reads no value, from Setup to the report: with the file gone
/// from disk, Setup still lays out and Run still reports what the schema says,
/// with no stage that reads the source and no row evaluated.
#[test]
fn metadata_only_reads_no_values() {
    let (mut app, rx, _tx, path) = open_quality_fixture("dq_metadata_only.parquet", 1_000, 7);
    app.analysis_modal.data_quality_plan.compute = datui::data_quality::QualityCompute::Metadata;
    std::fs::remove_file(&path).unwrap();
    let area = Rect::new(0, 0, 100, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(rendered_text(&buffer).contains("File metadata only: no values read"));
    assert!(run_quality_reads(&mut app, &rx).is_empty());
    assert!(!app.modal_showing(), "nothing was read, so nothing failed");
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(
        results.precision,
        datui::data_quality::QualityPrecision::Metadata
    );
    assert_eq!(results.evaluated_rows, 0);
    assert_eq!(results.columns.len(), 3);
}

/// Conflict evidence costs a read only where one was promised. Footers name the
/// file that stores a column in another type, and the file missing a column, on
/// every run; the values the conflict hides are read only by a full scan, which
/// says so in its stages, and are shown beside their file.
#[test]
fn conflict_evidence_is_read_only_by_a_full_scan() {
    use datui::data_quality::{ObservationKind, QualityCompute, QualityStage};
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "day=1",
        df!("id" => &[1i64, 2], "n" => &[10i64, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "day=2",
        df!("id" => &[3i64, 4], "n" => &["sixty", "seventy"], "extra" => &["x", "y"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "day=3",
        df!("id" => &[5i64, 6], "n" => &[50i64, 60], "extra" => &[Some("z"), None]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    pump_until_idle(&mut app, &rx, &tx);
    open_quality_setup(&mut app);
    let finding = |app: &App, kind: ObservationKind| {
        app.analysis_modal
            .data_quality_results
            .as_ref()
            .unwrap()
            .observations
            .iter()
            .find(|observation| observation.kind == kind)
            .cloned()
            .unwrap_or_else(|| panic!("a {kind:?} finding"))
    };

    let reads = run_quality_reads(&mut app, &rx);
    assert!(
        !reads.contains(&QualityStage::ReadingConflicts),
        "{reads:?}"
    );
    let conflict = finding(&app, ObservationKind::TypeConflict);
    assert_eq!(conflict.column, "n");
    assert_eq!(conflict.files.len(), 1);
    assert_eq!(conflict.files[0].number, 2);
    assert!(
        conflict.files[0].examples.is_empty(),
        "not read on a sample"
    );
    let absent = finding(&app, ObservationKind::Absent);
    assert_eq!(absent.column, "extra");
    assert_eq!(absent.files[0].number, 1);

    press(&mut app, KeyCode::Char('e'));
    {
        let plan = &mut app.analysis_modal.data_quality_plan;
        plan.method = datui::sampling::SampleMethod::EveryRow;
        plan.compute = QualityCompute::Full;
    }
    assert!(
        press(&mut app, KeyCode::Enter).is_none(),
        "a full scan asks"
    );
    let first = press(&mut app, KeyCode::Enter);
    let mut reads = Vec::new();
    let mut next = first;
    loop {
        match next.take() {
            Some(event) => {
                if let AppEvent::BackgroundQualityPhase { generation, phase } = &event
                    && *generation == app.task_generation()
                    && phase.reads_source
                {
                    reads.push(phase.stage);
                }
                next = app.event(&event);
            }
            None => match next_event(&app, &rx) {
                Some(event) => next = Some(event),
                None => break,
            },
        }
    }
    assert!(reads.contains(&QualityStage::ReadingConflicts), "{reads:?}");
    let conflict = finding(&app, ObservationKind::TypeConflict);
    assert_eq!(conflict.files[0].examples, ["sixty", "seventy"]);
}

/// An empty scope is never a clean report. Rows chosen past the table's end are
/// an error naming them, a second Run included, and no report. A dataset with no
/// rows at all, sampled or scanned, reports that nothing about its values is known:
/// no column called clean, no check passed.
#[test]
fn an_empty_scope_is_never_a_clean_report() {
    let (mut app, rx, _tx, _path) = open_quality_fixture("dq_empty_scope.csv", 1_000, 0);
    app.analysis_modal.data_quality_plan.scope = datui::data_quality::QualityScope::ViewRows {
        start: 5_000,
        end: 6_000,
    };
    for _ in 0..2 {
        let first = press(&mut app, KeyCode::Enter);
        let (finished, _) = drain_quality(&mut app, &rx, first);
        assert_eq!(finished, 0);
        assert!(app.modal_showing());
        let area = Rect::new(0, 0, 100, 30);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let text = rendered_text(&buffer);
        assert!(
            text.contains("No rows match view rows 5,000-6,000"),
            "{text}"
        );
        assert!(app.analysis_modal.data_quality_results.is_none());
        press(&mut app, KeyCode::Esc);
    }

    common::ensure_sample_data();
    for full in [false, true] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        let path = PathBuf::from("tests/sample-data/empty.parquet");
        pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
        pump_until_idle(&mut app, &rx, &tx);
        open_quality_setup(&mut app);
        let plan = &mut app.analysis_modal.data_quality_plan;
        // A declared key and a required column are no more checked than the rest.
        plan.intent = datui::quality_intent::DeclaredIntent {
            key: vec!["id".into()],
            columns: vec![datui::quality_intent::ColumnIntent {
                column: "name".into(),
                required: true,
                ..Default::default()
            }],
        };
        if full {
            plan.method = datui::sampling::SampleMethod::EveryRow;
            plan.compute = datui::data_quality::QualityCompute::Full;
        }
        let mut first = press(&mut app, KeyCode::Enter);
        if app.analysis_modal.data_quality_confirm_run {
            first = press(&mut app, KeyCode::Enter);
        }
        let (finished, _) = drain_quality(&mut app, &rx, first);
        assert_eq!(finished, 1);
        let results = app.analysis_modal.data_quality_results.clone().unwrap();
        assert_eq!(results.evaluated_rows, 0);
        let area = Rect::new(0, 0, 100, 30);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let text = rendered_text(&buffer);
        assert!(!text.contains("No problems found"), "full {full}: {text}");
        assert!(!text.contains("clean"), "full {full}: {text}");
        assert!(text.contains("No rows to check"), "full {full}: {text}");
        // Columns marks none of them clean either.
        app.analysis_modal
            .show_quality_tab(datui::data_quality::QualityPage::Columns);
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let lines = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buffer[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>();
        let check = datui::glyphs::get().check;
        for column in ["id", "name", "value", "date"] {
            let line = lines
                .iter()
                .find(|line| line.split_whitespace().any(|word| word == column))
                .unwrap_or_else(|| panic!("{column} on Columns: {lines:#?}"));
            let before = &line[..line.find(&format!(" {column} ")).unwrap()];
            assert!(!before.contains(check), "full {full}: {line}");
        }
        let report = datui::quality_report::build_report(&results);
        for check in datui::quality_report::checks(&results, &report) {
            assert!(!check.outcome.ran(), "{} ran on no rows", check.name);
        }
    }
}

#[test]
fn test_data_quality_scope_editor_runs_selected_view_rows() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::{QualityPage, QualityScope};

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/large_dataset.parquet")],
        OpenOptions::default(),
    );
    let key =
        |app: &mut App, code| app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    key(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // A run of the default plan settles before the scope edit.
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    app.analysis_modal.focus = AnalysisFocus::Main;
    // s opens the Sample form over Setup; its scope row takes the rows.
    key(&mut app, KeyCode::Char('s'));
    assert!(app.analysis_modal.sample_form.is_some());
    for area in [Rect::new(0, 0, 120, 32), Rect::new(0, 0, 60, 20)] {
        let mut buffer = Buffer::empty(area);
        app.render(area, &mut buffer);
        let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
        assert!(
            screen.contains("Rows from:")
                && screen.contains("All rows")
                && screen.contains("Random"),
            "the form's values name themselves"
        );
    }
    let set_scope = |app: &mut App, text: &str| {
        let (from, to) = text.trim_start_matches("rows ").split_once("..").unwrap();
        let form = app.analysis_modal.sample_form.as_mut().unwrap();
        form.kind = datui::sample_modal::RowsKind::Range;
        form.range_from.set_value(from);
        form.range_to.set_value(to);
    };
    set_scope(&mut app, "rows 0..3");
    key(&mut app, KeyCode::Enter);
    let form = app.analysis_modal.sample_form.as_ref().unwrap();
    assert!(form.error.is_some(), "a bad scope stays in the form");
    set_scope(&mut app, "rows 2..3");
    // Enter applies the sample to Setup, and Setup's Enter runs it.
    let next = key(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.sample_form.is_none());
    assert!(next.is_none(), "applying the form reads nothing");
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(
        app.analysis_modal.data_quality_plan.scope,
        QualityScope::ViewRows { start: 2, end: 3 }
    );
    let next = key(&mut app, KeyCode::Enter);
    assert_eq!(
        app.analysis_modal.sample.scope,
        QualityScope::ViewRows { start: 2, end: 3 }
    );
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal
            .data_quality_results
            .as_ref()
            .unwrap()
            .total_rows,
        Some(2)
    );
    // An edit waits for Enter; Esc puts back the plan the result was measured with.
    key(&mut app, KeyCode::Char('e'));
    for _ in 0..5 {
        key(&mut app, KeyCode::Down);
    }
    assert_eq!(
        app.analysis_modal.setup_row(),
        datui::analysis_modal::SetupRow::Grain
    );
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Enter);
    assert_ne!(
        app.analysis_modal.data_quality_plan.grain,
        datui::data_quality::QualityGrain::Dataset
    );
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert!(app.analysis_modal.quality_plan_pending());
    key(&mut app, KeyCode::Esc);
    assert_eq!(
        app.analysis_modal.data_quality_plan.grain,
        datui::data_quality::QualityGrain::Dataset
    );
    assert!(app.analysis_modal.data_quality_results.is_some());
    key(&mut app, KeyCode::Char('s'));
    set_scope(&mut app, "rows 1..1");
    assert!(key(&mut app, KeyCode::Enter).is_none());
    // The last report stays until a run replaces it.
    let next = key(&mut app, KeyCode::Enter);
    assert!(app.analysis_modal.data_quality_results.is_some());
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);

    // The earlier sample again is the session cache's, not another read.
    key(&mut app, KeyCode::Char('s'));
    set_scope(&mut app, "rows 2..3");
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(key(&mut app, KeyCode::Enter).is_none());
    assert!(!app.is_busy());
    assert!(app.analysis_modal.data_quality_from_cache);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Overview);
}

#[test]
fn test_data_quality_source_file_scope_uses_loaded_file_order() {
    use datui::analysis_modal::{AnalysisFocus, AnalysisTool};
    use datui::data_quality::QualityScope;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "region=one", df!("id" => &[1i32, 2]).unwrap());
    write_parquet(dir.path(), "region=two", df!("id" => &[3i32, 4]).unwrap());
    let (mut app, rx, _) = open_local_dataset_with_channel(dir.path());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('a'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.sidebar_state.select(Some(3));
    show_sample_form(&mut app);
    // A run of the default plan settles first.
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert_eq!(
        app.analysis_modal.selected_tool,
        Some(AnalysisTool::DataQuality)
    );
    app.analysis_modal.focus = AnalysisFocus::Main;
    // e leaves the result for Setup; the scoped run starts there.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    app.analysis_modal.data_quality_plan.scope = QualityScope::SourceFiles(vec![2]);
    let next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    app.event(&next.unwrap());
    drain_events(&mut app, &rx);
    let results = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(results.total_rows, Some(2));
    assert_eq!(results.evaluated_rows, 2);
    let id = results
        .columns
        .iter()
        .find(|column| column.name == "id")
        .unwrap();
    assert_eq!(id.min.as_deref(), Some("3"));
    assert_eq!(id.max.as_deref(), Some("4"));

    // The run reads the plan it is given.
    let plan = &mut app.analysis_modal.data_quality_plan;
    plan.scope = QualityScope::WholeSource;
    plan.dataset_rows = 1;
    app.event(&AppEvent::AnalysisDataQualityCompute);
    drain_events(&mut app, &rx);
    let sampled = app.analysis_modal.data_quality_results.as_ref().unwrap();
    // The sampler counts the whole scope as it spreads the sample over it.
    assert_eq!(sampled.total_rows, Some(4));
    assert_eq!(sampled.evaluated_rows, 1);
    assert_eq!(
        sampled.precision,
        datui::data_quality::QualityPrecision::Sampled
    );

    let plan = &mut app.analysis_modal.data_quality_plan;
    plan.method = datui::sampling::SampleMethod::EveryRow;
    plan.compute = datui::data_quality::QualityCompute::Full;
    app.event(&AppEvent::AnalysisDataQualityCompute);
    drain_events(&mut app, &rx);
    let full = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(full.total_rows, Some(4));
    assert_eq!(full.evaluated_rows, 4);

    // By file, the segments are the shared sample's rows split by the file each
    // came from; a file's size is its footer's, not a guess from the sample.
    let plan = &mut app.analysis_modal.data_quality_plan;
    plan.method = datui::sampling::SampleMethod::Spread;
    plan.compute = datui::data_quality::QualityCompute::Sample;
    plan.dataset_rows = 3;
    plan.grain = datui::data_quality::QualityGrain::File;
    app.event(&AppEvent::AnalysisDataQualityCompute);
    drain_events(&mut app, &rx);
    let by_file = app.analysis_modal.data_quality_results.as_ref().unwrap();
    assert_eq!(by_file.total_rows, Some(4));
    assert_eq!(by_file.evaluated_rows, 3);
    assert_eq!(by_file.segments.len(), 2);
    assert!(
        by_file
            .segments
            .iter()
            .all(|segment| segment.total_rows == Some(2))
    );
    assert_eq!(
        by_file
            .segments
            .iter()
            .map(|segment| segment.evaluated_rows)
            .sum::<usize>(),
        3
    );
}

/// Regression for commit 7b7bfe8: holding PageDown at the end of the data once
/// pushed `start_row` past `num_rows`, leaving the app `busy` because every spawn
/// no-op'd (buffer already valid after clamp) but the handler used to gate on
/// `needs && spawn`. Now `slide_table` clamps forward scroll, and the App
/// `handle_scroll` clears `busy` whether or not the spawn actually ran.
#[test]
fn test_scroll_past_end_does_not_hang_busy() {
    // Inline 200-row CSV so the test stays cheap and self-contained.
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("scroll_past_end_test.csv");
    let mut df = polars::df!(
        "id" => (0..200i64).collect::<Vec<_>>(),
        "value" => (0..200i64).map(|i| i * 10).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let terminal_area = Rect::new(0, 0, 80, 30);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // Render once so visible_rows is set for real, then settle the post-render bounce.
    let settle = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>| {
        for _ in ticks() {
            let mut buf = Buffer::empty(terminal_area);
            app.render(terminal_area, &mut buf);
            app.frame_painted();
            let needs = app
                .data_table_state
                .as_mut()
                .map(|s| {
                    let n = s.needs_recollect;
                    s.needs_recollect = false;
                    n
                })
                .unwrap_or(false);
            if needs {
                app.spawn_async_collect("Loading buffer...");
            }
            while let Ok(ev) = rx.try_recv() {
                if let Some(next) = app.event(&ev) {
                    let _ = tx.send(next);
                }
            }
            if !app.is_busy() && !needs {
                return;
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    };
    settle(&mut app, &rx, &tx);

    let total = app.data_table_state.as_ref().unwrap().num_rows();
    assert!(total > 0, "test data should have rows");

    // Jump to end via End key, then hammer PageDown a bunch — same sequence that
    // used to wedge the app. Each PageDown sets `busy=true` in the key handler;
    // DoScrollDown must clear it once the spawn no-ops past the bottom.
    if let Some(next) = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::End,
        KeyModifiers::NONE,
    ))) {
        let _ = tx.send(next);
    }
    settle(&mut app, &rx, &tx);

    for i in 0..15 {
        if let Some(next) = app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::PageDown,
            KeyModifiers::NONE,
        ))) {
            let _ = tx.send(next);
        }
        settle(&mut app, &rx, &tx);
        assert!(
            !app.is_busy(),
            "iteration {i}: PageDown past end must not leave busy stuck"
        );
    }
}

/// After a transform that invalidates `num_rows`, `spawn_async_collect` should
/// dispatch a background `len()` first (no UI thread blocking) and then chain
/// into the actual buffer collect. This test verifies the two-phase load
/// completes and yields a valid buffer.
#[test]
fn test_async_collect_handles_invalidated_num_rows() {
    use datui::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("invalidated_num_rows_test.csv");
    let mut df = polars::df!(
        "id" => (0..500i64).collect::<Vec<_>>(),
        "value" => (0..500i64).map(|i| i * 2).collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(&csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let terminal_area = Rect::new(0, 0, 80, 30);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());

    // Render so visible_rows is set; settle the bounce.
    let mut buf = Buffer::empty(terminal_area);
    app.render(terminal_area, &mut buf);
    drain_events(&mut app, &rx);

    // Apply a filter via the public event. This invalidates num_rows.
    let filter = FilterStatement {
        column: "value".to_string(),
        operator: FilterOperator::Lt,
        value: "200".to_string(),
        logical_op: LogicalOperator::And,
    };
    app.event(&AppEvent::Filter(vec![filter]));

    // Drain BackgroundLenReady then BackgroundCollectReady.
    for _ in ticks() {
        let mut buf = Buffer::empty(terminal_area);
        app.render(terminal_area, &mut buf);
        app.frame_painted();
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        if !needs && !work_pending(&app) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    assert!(
        !app.is_busy(),
        "filter + async len + collect should complete"
    );
    let state = app.data_table_state.as_ref().unwrap();
    // value < 200 → ids 0..100 → 100 rows
    assert_eq!(state.num_rows(), 100, "filtered row count should be 100");
    assert!(
        state.display_slice_df().is_some(),
        "display buffer should be populated after async len + collect"
    );
}

/// Opening a Parquet hive directory should paint the first buffer and resolve the exact
/// total row count via the footer-sum path (Fix 1 + Fix 2), without a full data scan.
#[test]
fn test_hive_dir_loads_and_counts_via_footers() {
    let dir = tempfile::tempdir().unwrap();
    let mk = |sub: &str, n: i64| {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let mut df = df!("v" => (0..n).collect::<Vec<i64>>()).unwrap();
        let f = File::create(d.join("data.parquet")).unwrap();
        ParquetWriter::new(f).finish(&mut df).unwrap();
    };
    // Hive layout across two partition keys; 30 + 12 + 8 = 50 rows total.
    mk("form_type=a/year=2020", 30);
    mk("form_type=a/year=2021", 12);
    mk("form_type=b/year=2020", 8);

    let terminal_area = Rect::new(0, 0, 80, 30);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);

    // Drive render -> buffer collect -> background count to completion.
    let mut counted = false;
    for _ in ticks() {
        let mut buf = Buffer::empty(terminal_area);
        app.render(terminal_area, &mut buf);
        app.frame_painted();
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        while let Ok(ev) = rx.try_recv() {
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        counted = app
            .data_table_state
            .as_ref()
            .and_then(|s| s.num_rows_if_valid())
            == Some(50);
        if !app.is_busy() && !needs && counted {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    assert!(counted, "exact total should resolve to the footer sum (50)");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.num_rows(),
        50,
        "hive dir total should equal footer sum"
    );
    assert!(
        state.display_slice_df().is_some(),
        "first buffer should be populated"
    );
}

/// The open's worker, not the install, finds out that a hive path is a directory, so
/// the dataset arrives already counting by its footers (#457).
#[test]
fn test_hive_dir_is_known_from_the_open() {
    let dir = tempfile::tempdir().unwrap();
    let part = dir.path().join("year=2020");
    std::fs::create_dir_all(&part).unwrap();
    let mut df = df!("v" => [1i64, 2, 3]).unwrap();
    ParquetWriter::new(File::create(part.join("data.parquet")).unwrap())
        .finish(&mut df)
        .unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.path().to_path_buf()], opts);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().parquet_count_dir(),
        Some(dir.path().to_path_buf())
    );
}

/// Open a local directory of Parquet files and return the loaded app, or `None` if the
/// open never finished.
fn open_local_dataset(dir: &std::path::Path) -> App {
    open_local_dataset_with_channel(dir).0
}

/// The same, keeping the app's own event channel so a test can drive background work.
fn open_local_dataset_with_channel(
    dir: &std::path::Path,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    let opts = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    pump_open_until_loaded(&mut app, &rx, vec![dir.to_path_buf()], opts);
    (app, rx, tx)
}

/// Write one Parquet file at `sub/data.parquet` under `dir`.
fn write_parquet(dir: &std::path::Path, sub: &str, mut df: polars::prelude::DataFrame) {
    let d = dir.join(sub);
    std::fs::create_dir_all(&d).unwrap();
    let f = File::create(d.join("data.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut df).unwrap();
}

/// Render a loaded app until no collect is owed and no background work is still to
/// report, and return what the table area shows. Idle alone is not enough: a row
/// count landing later can widen the buffer after the app first goes quiet. Drains the
/// app's events each pass: a buffer fill lands as one, so without it the screen is
/// whatever the first synchronous collect managed.
#[track_caller]
fn painted(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    area: Rect,
) -> String {
    let mut buf = Buffer::empty(area);
    for _ in ticks() {
        app.render(area, &mut buf);
        app.frame_painted();
        let mut handled = false;
        while let Ok(ev) = rx.try_recv() {
            handled = true;
            if let Some(next) = app.event(&ev) {
                let _ = tx.send(next);
            }
        }
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        // Something that landed since the frame was drawn needs a frame of its own
        // before the view can be called finished.
        if !handled && !needs && !work_pending(app) {
            break;
        }
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    app.render(area, &mut buf);
    buf.content().iter().map(|cell| cell.symbol()).collect()
}

/// Three kinds of empty cell that used to look identical: a null the data holds, a
/// column the file was written without, and a column the file stores as text while the
/// dataset reads it as a number.
#[test]
fn test_absent_null_and_conflicting_cells_differ_on_screen() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    // File one has `note` and a real null in it; it has no `extra` at all.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "note" => &[None::<&str>], "n" => &[10i64]).unwrap(),
    );
    // File two has every column, and stores `n` as text, which the dataset reads as
    // the integer that most of its rows are.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "note" => &["hi"], "extra" => &["x"], "n" => &["oops"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "note" => &["yo"], "extra" => &["y"], "n" => &[30i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    let text = painted(&mut app, &rx, &tx, area);

    assert!(
        text.contains(g.absent),
        "a file written without `extra` should show the absent glyph {:?}, got:\n{}",
        g.absent,
        text
    );
    assert!(
        text.contains(g.null),
        "the real null in `note` should still show the null glyph {:?}",
        g.null
    );
    assert!(
        text.contains(g.conflict),
        "the file storing `n` as text should show the conflict glyph {:?}",
        g.conflict
    );
    assert!(
        text.contains(&format!("extra{}", g.drift_mark)),
        "and `extra` is marked in the header as not being in every file"
    );
    assert!(
        !text.contains(&format!("id{}", g.drift_mark)),
        "while `id`, which every file has, is not"
    );
}

/// Sorting reorders rows across files, so a row's position no longer says which file
/// it came from. The scan's drift column rides along with the row, so the distinction
/// survives.
#[test]
fn test_absent_cells_still_read_as_absent_after_a_sort() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "note" => &[None::<&str>, None]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "note" => &["hi", "yo"], "extra" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "absent before the sort"
    );

    // Descending by id interleaves the two files: 4, 3, 2, 1.
    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], false);
    state.collect();
    assert!(state.error().is_none(), "the sort itself must succeed");

    let text = painted(&mut app, &rx, &tx, area);
    assert!(
        text.contains(g.absent),
        "the rows from the file without `extra` are still absent, not null"
    );
    assert!(text.contains(g.null), "and the real nulls are still nulls");
}

/// The accent reaches the bar from the dataset, and the config can turn it off.
///
/// `controls.rs` proves the accent is only a colour on the Info chip, but it is handed
/// a flag by hand; the app-side tests read `notes_unseen()`, an accessor. Nothing
/// joined the two, so an accent that never reached the bar — or one that ignored the
/// config — passed both.
#[test]
fn test_the_notes_accent_reaches_the_control_bar_and_the_config_can_stop_it() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let area = Rect::new(0, 0, 120, 24);
    let bar_of = |app: &mut App| -> (Vec<ratatui::style::Color>, String) {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let row = area.height - 1;
        (
            (0..area.width).map(|x| buf[(x, row)].fg).collect(),
            (0..area.width)
                .map(|x| buf[(x, row)].symbol().to_string())
                .collect(),
        )
    };

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        app.data_table_state.as_ref().unwrap().notes_unseen(),
        "the directories disagree, so there is a note and it has not been read"
    );
    let (accented, accented_text) = bar_of(&mut app);

    // Opening the panel clears it, and the bar goes back to its ordinary colours.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    let _ = painted(&mut app, &rx, &tx, area);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    let (plain, plain_text) = bar_of(&mut app);
    let accented_cells: Vec<usize> = (0..plain.len())
        .filter(|x| accented[*x] != plain[*x])
        .collect();
    assert!(
        !accented_cells.is_empty(),
        "the accent was on the bar, and reading the notes took it off"
    );
    // And it is the Info key that carries it, not the row count changing width or a
    // status message appearing — either of which would also colour some cells.
    let accented_word: String = accented_cells
        .iter()
        .map(|x| accented_text.chars().nth(*x).unwrap_or(' '))
        .collect();
    assert!(
        accented_word.contains("Info"),
        "the cells that changed spell the Info key: {accented_word:?}"
    );
    assert_eq!(
        accented_text, plain_text,
        "and the accent is only a colour — the bar says the same thing either way"
    );

    // And a user who does not want it never sees it, however many notes there are.
    let mut config = datui::config::AppConfig::default();
    config.display.notes_accent = false;
    let (tx2, rx2) = mpsc::channel();
    let mut off = App::new_with_config(
        tx2.clone(),
        common::test_runtime(),
        datui::config::Theme::from_config(&datui::config::AppConfig::default().theme)
            .expect("the default theme"),
        config,
    );
    pump_open_until_loaded(
        &mut off,
        &rx2,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let _ = painted(&mut off, &rx2, &tx2, area);
    assert!(
        off.data_table_state.as_ref().unwrap().notes_unseen(),
        "there is still a note to accent"
    );
    let (unaccented, unaccented_text) = bar_of(&mut off);
    assert_eq!(
        unaccented_text, plain_text,
        "the same bar, so the colours below are comparable"
    );
    let still_accented: Vec<usize> = accented_cells
        .iter()
        .copied()
        .filter(|x| unaccented[*x] != plain[*x])
        .collect();
    assert!(
        still_accented.is_empty(),
        "and the cells that carry the accent are the ordinary colour at {still_accented:?}"
    );
}

/// All three kinds of empty still read right after a filter.
///
/// The sort case above is the other half of the same criterion. A filter is the one
/// that rebuilds the frame rather than reordering it, and it is the one no test looked
/// at on screen: the row → file mapping has to survive a predicate, not just a reorder.
/// The conflicting column is here rather than in the sort fixture because a filter that
/// does not name it must keep its rows — leaving them out is for a filter that does.
#[test]
fn test_absent_null_and_conflicting_cells_still_differ_after_a_filter() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    // Three rows against two, so the majority type for `price` is the number and the
    // text file's rows are the ones left unread.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!(
            "id" => &[1i64, 4, 5],
            "note" => &[None::<&str>, None, None],
            "price" => &[10i64, 40, 50],
        )
        .unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!(
            "id" => &[2i64, 3],
            "note" => &["hi", "yo"],
            "extra" => &["x", "y"],
            "price" => &["cheap", "dear"],
        )
        .unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 120, 20);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        before.contains(g.absent) && before.contains(g.null) && before.contains(g.conflict),
        "all three before the filter: {before}"
    );

    // On `id`, which both files hold as the same type — so nothing is left out for
    // being unreadable, and rows from both files survive.
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt(
        "id",
        datui::filter_modal::FilterOperator::GtEq,
        "2",
    )]);
    assert!(state.error().is_none(), "the filter itself must succeed");
    // And a filter actually happened: without this the test passes when the predicate
    // is dropped on the floor, because an unfiltered screen carries all three glyphs
    // too. The criterion is about surviving a predicate, so there has to be one.
    assert_eq!(current_rows(&app), 4, "1 is gone; 2, 3, 4 and 5 are left");

    let after = painted(&mut app, &rx, &tx, area);
    assert!(
        after.contains(g.absent),
        "the rows of the file without `extra` still say absent: {after}"
    );
    assert!(
        after.contains(g.null),
        "and the real nulls are still nulls: {after}"
    );
    assert!(
        after.contains(g.conflict),
        "and the file that holds `price` as text still says so: {after}"
    );
}

/// The very first frame must mark the absent cells too.
///
/// Every other test here paints through `painted`, which renders up to sixty times, so
/// a mark that only arrives on the second frame looks identical to one that was always
/// there. In the app there is no second frame until something happens: datui draws the
/// dataset and waits. So this renders exactly once, into a fresh buffer, and reads the
/// glyph off it.
#[test]
fn test_the_first_frame_of_a_drifting_dataset_marks_its_absent_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let area = Rect::new(0, 0, 80, 12);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .flat_map(|y| {
            (0..area.width)
                .map(move |x| (x, y))
                .map(|(x, y)| buf[(x, y)].symbol().to_string())
                .chain(std::iter::once("\n".to_string()))
        })
        .collect();

    assert!(
        screen.contains(g.absent),
        "the row from the file without `extra` is absent, not null, on the first \
         frame as much as the second:\n{screen}"
    );
}

/// Pins the row arithmetic that everything else rests on.
///
/// The scan numbers each run's rows from where that run's first file begins in the
/// dataset. Every other fixture here has files of one or two rows, which makes a run's
/// starting row and its file's *index* the same number — so using one for the other
/// would go unnoticed. These files hold 3, 5 and 2 rows, and the third conflicts, which
/// splits the scan after row 8.
#[test]
fn test_each_row_takes_its_glyph_from_the_file_it_came_from() {
    let dir = tempfile::tempdir().unwrap();
    // No `n` at all: its cells are absent.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    // `n` as an integer, and the most rows, so the dataset reads it as one.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4, 5, 6, 7], "n" => &[30i64, 40, 50, 60, 70]).unwrap(),
    );
    // `n` as text: it cannot be read from here, so its cells conflict.
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[8i64, 9], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    let dataset = state.dataset_schema().expect("read from the footers");
    let (first, middle, last) = (
        dataset.file_group[0],
        dataset.file_group[1],
        dataset.file_group[2],
    );
    assert_eq!(middle, 0, "the middle file is missing nothing");
    assert_ne!(first, middle, "the first file has no `n`");
    assert_ne!(last, middle, "the last file holds `n` as text");

    let groups = state.display_drift(area.height as usize);
    assert_eq!(
        groups,
        vec![
            first, first, first, middle, middle, middle, middle, middle, last, last
        ],
        "three rows from the first file, five from the second, two from the third"
    );

    // Scrolled, and to a row inside the middle file rather than onto a boundary: the
    // window starts where the view does, so the groups have to shift with it.
    let state = app.data_table_state.as_mut().unwrap();
    state.scroll_to(4);
    state.collect();
    assert_eq!(
        state.display_drift(6),
        vec![middle, middle, middle, middle, last, last],
        "from row 4: four more rows of the second file, then the third"
    );
}

/// A conflicting column has no value to order by, so a sort on it leaves those rows
/// out rather than gathering them at one end as though they belonged there — and says
/// how many went.
#[test]
fn test_a_sort_leaves_out_the_rows_its_column_is_not_read_from() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4, 5, 6, 7], "n" => &[30i64, 40, 50, 60, 70]).unwrap(),
    );
    // `n` as text here, so it is not read from this file: two rows of conflict.
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[8i64, 9], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 10, "every row is there to begin with");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["n".to_string()], true);
    assert!(state.error().is_none(), "the sort itself must succeed");

    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(
        ids.len(),
        8,
        "the two rows from the file that stores `n` as text are gone: {ids:?}"
    );
    assert!(
        !ids.contains(&8) && !ids.contains(&9),
        "and it is those two, not two others: {ids:?}"
    );
    assert!(
        ids.contains(&0) && ids.contains(&1) && ids.contains(&2),
        "the file with no `n` at all keeps its rows: its cells are absent, not a \
         value in another type: {ids:?}"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("is not read from"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n is not read from 1 file, so the 2 rows there are left out of the sort"
    );
    assert_eq!(left_out.scope, "in all 3 footers");
    assert!(
        state.notes_unseen(),
        "and the `i` accent comes back for a note the user has not been offered"
    );
}

/// The filter half: the other two wordings the note has, and the row a filter's own
/// terms matched but its file cannot stand behind.
///
/// A sidebar filter of `id = 3 or n = 0` matches the row whose `id` is 3 — but that
/// row's file stores `n` as text, so its `n` was never read and the view cannot
/// answer either half of the question. It goes, and the note says why.
#[test]
fn test_a_filter_leaves_out_the_rows_its_column_is_not_read_from() {
    use datui::filter_modal::{FilterOperator, LogicalOperator};

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let mut or_id_3 = filter_stmt("id", FilterOperator::Eq, "3");
    or_id_3.logical_op = LogicalOperator::Or;
    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt("n", FilterOperator::Eq, "0"), or_id_3]);
    assert!(state.error().is_none(), "the filter itself must succeed");

    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(
        ids,
        vec![0],
        "id 3 matched a term of its own, but its file's `n` was never read"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("is not read from"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n is not read from 1 file, so the 2 rows there are left out of the filter"
    );

    // Sorting by the same column too: one note, naming both.
    state.sort(vec!["n".to_string()], true);
    let notes = state.notes();
    let both: Vec<&str> = notes
        .iter()
        .filter(|note| note.summary.contains("is not read from"))
        .map(|note| note.summary.as_str())
        .collect();
    assert_eq!(
        both,
        ["n is not read from 1 file, so the 2 rows there are left out of the filter and sort"],
        "one note for the column, not one for each of the two things naming it"
    );
}

/// A column the feed started sending is named by where it starts, not only by a count.
///
/// "in 2 of 3 files" says a column is unusual; "none before date=2024-01-02" says when
/// it began, which for a field added to a feed is the whole question. Through a real
/// directory rather than a hand-built footer list, because the partition it names comes
/// from the file's own path.
#[test]
fn test_a_column_that_starts_partway_through_says_where_it_starts() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "fee" => &[10i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "fee" => &[30i64]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about_fee = notes
        .iter()
        .find(|note| note.summary.starts_with("fee is in"))
        .unwrap_or_else(|| panic!("no note about `fee`: {notes:#?}"));
    assert_eq!(
        about_fee.summary,
        "fee is in 2 of 3 files, none before date=2024-01-02; absent from the rest, \
         not null"
    );
}

/// And a column that belongs to one partition is named by that partition.
#[test]
fn test_a_column_only_one_partition_has_says_which() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-03-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-03-02",
        df!("id" => &[2i64], "oops" => &["x"]).unwrap(),
    );
    write_parquet(dir.path(), "date=2024-03-03", df!("id" => &[3i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about_oops = notes
        .iter()
        .find(|note| note.summary.starts_with("oops is in"))
        .unwrap_or_else(|| panic!("no note about `oops`: {notes:#?}"));
    assert_eq!(
        about_oops.summary,
        "oops is in 1 of 3 files, only date=2024-03-02; absent from the rest, \
         not null"
    );
}

/// Files in the directory that are not Parquet are counted, so a silent drop is not one.
///
/// A `.csv` sitting in a directory of Parquet is a file somebody thought was in the
/// table. datui reads none of it and, until now, said nothing at all about it — which is
/// the shape of problem this whole issue is about.
#[test]
fn test_files_that_are_not_parquet_are_counted_rather_than_dropped_in_silence() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    std::fs::write(
        dir.path().join("date=2024-01-01/extra.csv"),
        "id
1
",
    )
    .unwrap();
    std::fs::write(dir.path().join("notes.txt"), "read me").unwrap();
    // A writer's bookkeeping, which is not a file anyone meant as data.
    std::fs::write(dir.path().join("_SUCCESS"), "").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let skipped = notes
        .iter()
        .find(|note| note.summary.contains("not Parquet"))
        .unwrap_or_else(|| panic!("no note about the files that were not read: {notes:#?}"));
    assert_eq!(
        skipped.summary,
        "in the directory, 2 files are not Parquet, 1 file a writer left behind"
    );
    assert_eq!(skipped.scope, "in this directory's listing");
}

/// A flat mixed directory's read is reported once, not by the open and again by the
/// footer walk.
///
/// Both count the same stray: the open says "read as the commonest; 1 csv not read"
/// and the walk behind the footers said "in the directory, 1 file is not Parquet"
/// right under it — the same fact twice, in two wordings. The open's sentence names
/// the format and says why, so it is the one kept.
#[test]
fn test_a_mixed_parquet_directory_says_what_it_left_out_once() {
    let dir = tempfile::tempdir().unwrap();
    for name in ["a.parquet", "b.parquet"] {
        let f = File::create(dir.path().join(name)).unwrap();
        ParquetWriter::new(f)
            .finish(&mut df!("id" => &[1i64]).unwrap())
            .unwrap();
    }
    std::fs::write(dir.path().join("extra.csv"), "id\n1\n").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let about: Vec<String> = state
        .notes()
        .iter()
        .filter(|n| n.summary.contains("not read") || n.summary.contains("not Parquet"))
        .map(|n| n.summary.clone())
        .collect();
    assert_eq!(
        about,
        [concat!(
            "the directory holds more than one format and was read as the commonest; ",
            "1 csv not read"
        )],
        "one fact, said once"
    );
}

/// And a directory holding only what a writer leaves behind says nothing.
///
/// `_SUCCESS` beside the data is a job reporting that it finished. A note about it on
/// every directory any job ever wrote would put an accent on the Info key for the most
/// ordinary thing a directory can contain.
#[test]
fn test_a_writers_own_bookkeeping_is_not_worth_a_note() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    std::fs::write(dir.path().join("_SUCCESS"), "").unwrap();
    std::fs::write(dir.path().join("date=2024-01-01/.data.parquet.crc"), "").unwrap();
    // A table format's own log. Everything in here belongs to the writer, whatever it
    // is called — a directory of three hundred commits is six hundred files, and counting
    // them as somebody's mistake would put an accent on the Info key for the most
    // ordinary thing a directory of Parquet can be.
    std::fs::create_dir_all(dir.path().join("_delta_log")).unwrap();
    for commit in 0..5 {
        std::fs::write(
            dir.path().join(format!("_delta_log/{commit:020}.json")),
            "{}",
        )
        .unwrap();
        std::fs::write(
            dir.path()
                .join(format!("_delta_log/.{commit:020}.json.crc")),
            "",
        )
        .unwrap();
    }

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|note| note.summary.contains("not Parquet")),
        "nothing to say: {:#?}",
        state.notes()
    );
}

/// A table format's own Parquet is not the table's rows.
///
/// Delta writes its checkpoints as Parquet inside `_delta_log/`, with the table's own
/// columns among its own. Read as data they are extra rows in a table that does not
/// have them and columns nobody asked for — a wrong row count, silently. This is about
/// what the table *contains*, not about what a note says.
#[test]
fn test_a_table_formats_own_parquet_is_not_part_of_the_table() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    // A checkpoint, which is Parquet and is not the table.
    write_parquet(
        dir.path(),
        "_delta_log",
        df!("id" => &[99i64], "txn" => &["commit"]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id"],
        "the checkpoint's own columns are not the table's"
    );
    let ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .expect("the table reads")
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(ids, [1], "and its rows are not the table's rows");
}

/// An object with nothing in it, whose name said it was data, is a write that stopped.
///
/// The note leads with it because it is the one skip that is a fault rather than a
/// tidy-up — and because nothing else can see it: a file that holds no bytes has no
/// footer to fail to read.
#[test]
fn test_a_write_that_stopped_is_said_to_have_stopped() {
    use datui::schema_union::SkippedFiles;
    let note = datui::notes::from_dataset(
        &datui::schema_union::union_file_schemas(
            &[],
            datui::schema_union::SchemaOrigin::AllFooters(0),
        )
        .with_skipped(SkippedFiles {
            bookkeeping: 2,
            not_parquet: 1,
            empty: 1,
        }),
    );
    let said: Vec<&str> = note.iter().map(|n| n.summary.as_str()).collect();
    assert_eq!(
        said,
        [
            "in the directory, 1 file is empty and was not read, 1 file is not Parquet, \
          2 files a writer left behind"
        ],
        "the stopped write first, then the mistake, then the tidy-up"
    );
}

/// The same rule on disk as in a bucket: a directory with no data in it is nobody's
/// table.
///
/// The cloud half of this has a test; the local half had none, and a mutant that made
/// the rule never fire survived the whole suite. Iceberg keeps its log in a plain
/// `metadata/` — no underscore, no dot — so the name convention alone reads a table's
/// own files as somebody's mistakes.
#[test]
fn test_a_local_directory_with_no_data_in_it_is_nobodys_table() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "data/date=1", df!("id" => &[1i64]).unwrap());
    std::fs::create_dir_all(dir.path().join("metadata")).unwrap();
    for name in ["v1.metadata.json", "v2.metadata.json", "snap-123.avro"] {
        std::fs::write(dir.path().join("metadata").join(name), "{}").unwrap();
    }
    // And a day that landed as CSV, which is part of the dataset and is a mistake.
    std::fs::create_dir_all(dir.path().join("data/date=2")).unwrap();
    std::fs::write(dir.path().join("data/date=2/part-0.csv"), "id\n2\n").unwrap();

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let about = notes
        .iter()
        .find(|note| note.summary.starts_with("in the directory"))
        .unwrap_or_else(|| panic!("no note about what was not read: {notes:#?}"));
    assert_eq!(
        about.summary, "in the directory, 1 file is not Parquet, 3 files a writer left behind",
        "the csv in the partition that has no data of its own, and Iceberg's three"
    );
}

/// The offer in the Notes tab, taken: the values a type conflict hid appear on screen.
#[test]
fn test_the_notes_tab_offers_to_read_a_conflicting_column_as_text() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // `n` as text here, so it is not read from this file at all.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let before = painted(&mut app, &rx, &tx, area);
    assert!(
        before.contains(g.conflict),
        "the row whose file stores `n` as text is a conflict to begin with"
    );
    assert!(
        !before.contains("sixty"),
        "and its value cannot be seen: {before}"
    );

    // Open the Info panel and walk to the Notes tab.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // Walked by key rather than by setting the tab, so the keys the user presses are
    // the ones under test. Tab first: the panel opens on the body, where the arrows
    // move the schema table rather than the tab bar. The conflict note's own words say
    // when we have arrived.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    let mut panel = painted(&mut app, &rx, &tx, area);
    for _ in 0..6 {
        if panel.contains("and not read there") {
            break;
        }
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
        panel = painted(&mut app, &rx, &tx, area);
    }
    assert!(
        panel.contains("and not read there"),
        "the Notes tab, showing the conflict note: {panel}"
    );

    // Walk to the note that carries the offer, and take it.
    let offered = |app: &App| -> Option<usize> {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .position(|note| note.read_as_text.is_some())
    };
    let at = offered(&app).expect("the conflict note offers to read the column as text");
    for _ in 0..at {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    let panel = painted(&mut app, &rx, &tx, area);
    assert!(
        panel.contains("Enter  read n as text"),
        "the panel says the offer is there: {panel}"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    let after = painted(&mut app, &rx, &tx, area);

    assert!(
        after.contains("sixty"),
        "the value the conflict hid is on screen: {after}"
    );
    assert!(
        after.contains("10") && after.contains("20"),
        "and so are the ones that were always readable: {after}"
    );
    assert!(
        !after.contains(g.conflict),
        "nothing conflicts any more: {after}"
    );

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.read_as_text(),
        [polars::prelude::PlSmallStr::from("n")],
        "and the state says which column it is reading that way"
    );
    assert!(
        !state.notes().iter().any(|note| note.read_as_text.is_some()),
        "the offer is gone, having been taken: {:#?}",
        state.notes()
    );
}

/// The offer is not made where datui could not honour it.
///
/// Reading a column as text needs to know where each file's rows begin, and datui does
/// not for a dataset whose footers could not all be read — the same datasets that
/// cannot draw the marks. The note is still worth saying; the offer on it is not.
#[test]
fn test_no_offer_to_read_as_text_where_the_files_were_not_all_counted() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "n" => &["sixty"]).unwrap(),
    );
    // A third file datui cannot read the footer of.
    let broken = dir.path().join("date=2024-01-03");
    std::fs::create_dir_all(&broken).unwrap();
    std::fs::write(broken.join("data.parquet"), b"not a parquet file at all").unwrap();

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|note| note.summary.contains("not read there")),
        "the conflict is still worth saying: {notes:#?}"
    );
    assert!(
        notes.iter().all(|note| note.read_as_text.is_none()),
        "but datui cannot act on it, so it does not offer to: {notes:#?}"
    );
}

/// The offer and the count of notes out of view share the last row without landing on
/// top of each other.
///
/// Both are drawn into the panel's bottom row. A `Paragraph` leaves the cells its text
/// does not reach alone, so two of them in one place is not a layout that loses — it is
/// one string written over another.
#[test]
fn test_the_offer_and_the_hidden_count_do_not_overwrite_each_other() {
    let dir = tempfile::tempdir().unwrap();
    // Several drifting columns, so there are more notes than a short panel can show.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!(
            "id" => &[0i64, 1],
            "measurement_value" => &[10i64, 20],
            "b" => &[1i64, 2],
            "c" => &[1i64, 2],
            "d" => &[1i64, 2],
        )
        .unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!(
            "id" => &[2i64],
            "measurement_value" => &["sixty"],
            "b" => &["x"],
            "c" => &["x"],
            "d" => &["x"],
        )
        .unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    // Narrow, and short enough that the notes do not all fit: both halves of the last
    // row have something to say, and not enough room to say it in. 74 wide because
    // the sidebar clamp leaves the table 30 columns, so the panel itself gets the
    // 44 this test is about.
    let area = Rect::new(0, 0, 74, 12);
    let _ = painted(&mut app, &rx, &tx, area);
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    let mut panel = painted(&mut app, &rx, &tx, area);
    for _ in 0..6 {
        if panel.contains("and not read there") {
            break;
        }
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Right,
            KeyModifiers::NONE,
        )));
        panel = painted(&mut app, &rx, &tx, area);
    }

    // Walk to a note carrying the offer, so the panel has both things to say.
    let offered = |app: &App| -> Option<usize> {
        app.data_table_state
            .as_ref()
            .unwrap()
            .notes()
            .iter()
            .position(|note| note.read_as_text.is_some())
    };
    let at = offered(&app).expect("a conflict note offers to read its column as text");
    for _ in 0..at {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    let panel = painted(&mut app, &rx, &tx, area);

    let count = (1..9)
        .flat_map(|n| [format!("{n} below"), format!("{n} above")])
        .find(|text| panel.contains(text.as_str()))
        .unwrap_or_else(|| panic!("the panel is short enough to be hiding notes: {panel}"));
    assert!(
        panel.contains("Enter  read"),
        "the offer shares the row with the count: {panel}"
    );
    // The blank column between them is the whole of it. Drawn into the same rect, the
    // count lands on the offer's last characters and there is no gap — the offer's
    // text runs straight into "2 below" with no way to tell where one ends.
    assert!(
        panel.contains(&format!(" {count}")),
        "the two must not run together where they meet: {panel}"
    );
}

/// A filter on a column read as text compares text, and the panel says so.
///
/// `n > 5` was written for a number. Read as text it keeps `"sixty"` and drops `"10"`,
/// which is a different question with the same words — so the note that arrives in
/// place of the conflict note is the one thing standing between the user and a view
/// they would read wrongly.
#[test]
fn test_reading_a_filtered_column_as_text_says_the_comparison_changed() {
    use datui::filter_modal::FilterOperator;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[1i64, 10, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.filter(vec![filter_stmt("n", FilterOperator::Gt, "5")]);
    assert_eq!(current_rows(&app), 2, "10 and 20 are greater than 5");

    let state = app.data_table_state.as_mut().unwrap();
    state.mark_notes_seen();
    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert!(
        notes.iter().any(
            |note| note.summary == "n is read as text, so a filter or sort on it compares text"
        ),
        "the filter means something else now, and the panel says so: {notes:#?}"
    );
    assert!(
        state.notes_unseen(),
        "and the `i` accent comes back, since the user has not been told yet"
    );
}

/// Taking the offer from the Info panel rebuilds the scan on the UI thread and reads
/// its rows in the background, like any other change to the view (#458).
#[test]
fn test_reading_a_column_as_text_from_the_panel_reads_in_the_background() {
    use datui::widgets::info::InfoTab;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[1i64, 10, 20]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.info_modal.active_tab, InfoTab::Notes);
    let state = app.data_table_state.as_ref().unwrap();
    app.info_modal.notes_selected_index = state
        .notes()
        .iter()
        .position(|note| note.read_as_text.is_some())
        .expect("the conflict note offers to read n as text");
    assert!(state.is_num_rows_valid());
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.schema().get("n"), Some(&DataType::String));
    assert!(
        !state.is_num_rows_valid(),
        "the new frame is not counted on this thread"
    );
    assert!(app.is_busy(), "its rows are being read");
    drain_events(&mut app, &rx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.num_rows_if_valid(), Some(4));
    let shown = state.display_df().expect("the rows are read");
    assert_eq!(shown.column("n").unwrap().dtype(), &DataType::String);
}

/// Reading a column as text does not undo the widening, so the note about it stays.
///
/// One file wrote `n` as an integer and another as a float, which widen together — so
/// the column is read as a float and the integer file's `7` shows as `7.0`, text read
/// or not. Only the types that *conflict* are read at their own type. The note that
/// explains the `7.0` is the widening note, and an earlier version of this deleted it.
#[test]
fn test_reading_as_text_keeps_the_note_about_a_widened_type() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64], "n" => &[7i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[1i64, 2], "n" => &[1.5f64, 2.5]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "n" => &["sixty"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    let widening = "n is stored as more than one type";
    assert!(
        state
            .notes()
            .iter()
            .any(|n| n.summary.starts_with(widening)),
        "the integer and the float widened together to begin with"
    );
    assert!(
        state.read_column_as_text("n").unwrap(),
        "the offer is taken"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let text: Vec<String> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("n")
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|value| value.unwrap_or("null").to_string())
        .collect();
    assert_eq!(
        text,
        ["7.0", "1.5", "2.5", "sixty"],
        "the file that wrote 7 still reads 7.0: widening is not what the text read undoes"
    );
    let notes = state.notes();
    assert!(
        notes.iter().any(|n| n.summary.starts_with(widening)),
        "so the note explaining that 7.0 has to stay: {notes:#?}"
    );
}

/// A partition written for a day nothing happened holds no rows, and datui says so.
///
/// Worth saying because the dataset then has fewer days of data than it has directories,
/// and the file is invisible in every other way: it adds no rows, changes no schema,
/// and moves nothing on screen.
#[test]
fn test_a_partition_that_holds_no_rows_is_worth_a_note() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1], "n" => &[10i64, 20]).unwrap(),
    );
    // A day nothing happened: the directory is there, the file is there, the rows are
    // not.
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => Vec::<i64>::new(), "n" => Vec::<i64>::new()).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[2i64], "n" => &[30i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(current_rows(&app), 3, "the empty day adds nothing");
    let notes = state.notes();
    let empty = notes
        .iter()
        .find(|note| note.summary.contains("no rows"))
        .unwrap_or_else(|| panic!("nothing said about the empty day: {notes:#?}"));
    assert_eq!(empty.summary, "1 file holds no rows");
    assert_eq!(empty.scope, "in all 3 footers");
}

/// The control: every file holding rows says nothing.
#[test]
fn test_a_dataset_whose_files_all_hold_rows_says_nothing_about_empty_ones() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64], "n" => &[10i64]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[1i64], "n" => &[20i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state.notes().iter().any(|n| n.summary.contains("no rows")),
        "{:#?}",
        state.notes()
    );
}

/// A pipeline that renamed its partition key partway through.
///
/// The note says the shape and claims nothing about what it costs. What it costs varies:
/// this directory does not open at all, because the scan reads its partition columns off
/// one branch of the tree and the files under the other key fail it — but which branch
/// wins is whatever the filesystem hands back first, so this asserts that *something*
/// went wrong rather than which key won. An earlier version asserted the key, passed
/// here and failed on CI.
#[test]
fn test_a_directory_whose_partition_key_changed_says_the_directories_differ() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }
    // The day the pipeline changed.
    write_parquet(
        dir.path(),
        "dt=2024-01-04",
        df!("id" => &[1i64], "n" => &[2i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(
        frame.contains("Schema field not found"),
        "a renamed key stops this directory opening, whichever key the scan took — the \
         note exists to explain a screen like this one:\n{frame}"
    );

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let layout = notes
        .iter()
        .find(|note| note.summary.contains("partition by the same keys"))
        .unwrap_or_else(|| panic!("nothing said about the changed key: {notes:#?}"));
    assert_eq!(
        layout.summary,
        "the directories do not all partition by the same keys: 3 files by date, 1 file by dt"
    );
    assert_eq!(layout.scope, "in the names of 4 files");
}

/// The very same disagreement, and this one opens.
///
/// What decides it is not which branch the scan reads by — it is which file name sorts
/// first. The paths are handed to Polars sorted and it takes the hive schema from the
/// first of them, so a `data.parquet` at the root (which sorts above both `date=` and
/// `dt=`) means no file's key is ever checked and the column comes back null. Name it
/// `loose.parquet` and the same directory will not open at all.
///
/// A byte sort of path strings, so this holds on any filesystem — and it is why the
/// note says the shape and not the cost: it cannot see a filename's spelling.
#[test]
fn test_directories_that_differ_may_still_open_and_the_note_claims_only_the_shape() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }
    write_parquet(
        dir.path(),
        "dt=2024-01-04",
        df!("id" => &[1i64], "n" => &[2i64]).unwrap(),
    );
    // At the root, and named so that it sorts before both partition directories.
    write_parquet(
        dir.path(),
        "",
        df!("id" => &[9i64], "n" => &[9i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(
        !frame.contains("Error"),
        "the same disagreement as the test above, and this one opens: {frame}"
    );
    assert_eq!(current_rows(&app), 5, "every file is read");

    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    let layout = notes
        .iter()
        .find(|note| note.summary.contains("partition by the same keys"))
        .unwrap_or_else(|| panic!("the directories still differ: {notes:#?}"));
    assert_eq!(
        layout.summary,
        "the directories do not all partition by the same keys: 3 files by date, 1 file by dt",
        "said of a dataset that opened, which is why it says nothing about cost"
    );
    assert_eq!(
        layout.scope, "in the names of 5 files",
        "the file at the root is one of the names read, though it is no layout"
    );
}

/// The control: a directory partitioned the one way opens, and says nothing about keys.
#[test]
fn test_a_directory_partitioned_the_one_way_says_nothing_about_its_keys() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let frame = painted(&mut app, &rx, &tx, area);
    assert!(!frame.contains("Error"), "it opens: {frame}");

    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|note| note.summary.contains("partition by the same keys")),
        "{:#?}",
        state.notes()
    );
}

/// The counter the loading screen reads is the one the real open writes to.
///
/// Every other test here drives the counter from one side: the render tests set it by
/// hand, the pass test calls the pass directly with a counter of its own. Neither says
/// the two are connected — with only those, pointing the open at the non-reporting
/// pass leaves the feature completely dead in the running app and the suite green.
#[test]
fn test_opening_a_directory_reports_its_footers_to_the_app() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(
            dir.path(),
            day,
            df!("id" => &[0i64], "n" => &[1i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));

    assert_eq!(
        app.footer_progress.last_pass().begun,
        1,
        "the open ran its footer pass against the app's own counter"
    );
    assert_eq!(
        app.footer_progress.last_pass().read,
        3,
        "and counted each of the three footers off it"
    );
    assert_eq!(
        app.footer_progress.reading(),
        None,
        "with nothing left on screen once they landed"
    );
}

/// A second open starts its own count rather than inheriting the first one's.
///
/// Abandoning a load cancels nothing — the footers keep being read — so a counter
/// shared across loads reports the abandoned directory's progress under the next file's
/// name, which is what a user opening a small CSV after a large directory would see.
#[test]
fn test_each_open_counts_its_own_footers() {
    let first = tempfile::tempdir().unwrap();
    for day in ["date=2024-01-01", "date=2024-01-02", "date=2024-01-03"] {
        write_parquet(first.path(), day, df!("id" => &[0i64]).unwrap());
    }
    let second = tempfile::tempdir().unwrap();
    write_parquet(
        second.path(),
        "date=2024-01-01",
        df!("id" => &[0i64]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(first.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    let counter_of_the_first = app.footer_progress.clone();
    assert_eq!(counter_of_the_first.last_pass().read, 3);

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![second.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));

    // The pointer comparison is the one that bites, and it is first because of that.
    // `begin` stores zero, so a second pass resets a shared counter as surely as a
    // fresh one and the count below reads 1 either way — it says what the counter
    // should hold, and the line under it is what makes holding it mean anything.
    assert!(
        !Arc::ptr_eq(&counter_of_the_first, &app.footer_progress),
        "the second open has a counter of its own"
    );
    assert_eq!(
        app.footer_progress.last_pass().read,
        1,
        "counting its one footer, not the three before it"
    );

    // And the behaviour, not just the mechanism: the first open's pass goes on running
    // after it is abandoned, so a shared counter would paint its progress onto the
    // screen of the file that replaced it. Driven by hand, since a real abandoned pass
    // finishes too fast to catch — and rendered while the app is still *loading*,
    // because an idle app never consults the counter at all and the assertion would
    // hold however broken the wiring was.
    counter_of_the_first.begin(6541);
    for _ in 0..4102 {
        counter_of_the_first.advance();
    }
    app.set_loading_phase("Caching schema", 40);
    let mut buf = ratatui::buffer::Buffer::empty(Rect::new(0, 0, 100, 24));
    app.render(Rect::new(0, 0, 100, 24), &mut buf);
    let frame: String = (0..24)
        .flat_map(|y| {
            (0..100)
                .map(move |x| (x, y))
                .map(|(x, y)| buf[(x, y)].symbol().to_string())
                .chain(std::iter::once("\n".to_string()))
        })
        .collect();
    assert!(
        frame.contains("Caching schema"),
        "the app is on its loading screen, where the counter is read:\n{frame}"
    );
    assert!(
        !frame.contains("6,541"),
        "and the abandoned directory's count does not appear under the file that \
         replaced it:\n{frame}"
    );
}

/// The control bar shows the same count the loading body does.
///
/// Both derive it from `App::loading_phase`, and the point of that is that one wait
/// cannot be described two ways. The truncation test in `controls.rs` builds the bar
/// with a hand-written string, so it says the bar cuts a long message properly and
/// nothing about whether the bar is ever given the count at all: deleting the line
/// that hands it over leaves the body counting and the bar still saying "Caching
/// schema", with the suite green.
#[test]
fn test_the_control_bar_counts_the_footers_the_loading_screen_does() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Caching schema", 40);
    app.footer_progress.begin(6541);
    for _ in 0..1203 {
        app.footer_progress.advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = ratatui::buffer::Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect()
        })
        .collect();

    let body = rows.iter().find(|r| r.contains("Reading footers"));
    assert!(body.is_some(), "the body counts them:\n{}", rows.join("\n"));
    let bar = rows.last().expect("a control bar");
    assert!(
        bar.contains("Reading footers: 1,203 of 6,541"),
        "and so does the bar, rather than the phase the body has stopped showing: \
         {bar:?}"
    );
    // Not beside the per-phase constant, which is 40 here and would read as this
    // count's progress. 1,203 of 6,541 is 18%.
    assert!(
        !bar.contains('%'),
        "a real fraction is not to be shown beside a made-up percentage: {bar:?}"
    );
}

/// The bar says the footers are still arriving, after the dataset is on screen.
///
/// A cloud prefix of many files opens from two of them and reads the rest behind the
/// data. Nothing is blocked and nothing is wrong, so it is said in the control bar
/// rather than on a loading screen — but it is said, because otherwise columns appear
/// minutes later with no explanation.
#[test]
fn test_the_bar_says_the_footers_are_still_arriving_while_the_data_is_up() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let _ = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    // Said only for a dataset that is itself waiting. The counter is shared with every
    // open, and one abandoned half way through goes on counting: without the dataset's
    // own say-so this bar would count a directory the user walked away from.
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .set_footers_pending(std::sync::Arc::new(|_| None));
    app.footer_progress.begin(6541);
    for _ in 0..1203 {
        app.footer_progress.advance();
    }

    let area = Rect::new(0, 0, 100, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Reading footers: 1,203 of 6,541"),
        "the bar says what is still arriving: {bar:?}"
    );
    // And it prints the count, because this dataset has one: every file of it was read
    // at the open. A spinner here would be spinning over a number in hand. What is not
    // shown is a count that has not been taken — see
    // `a_count_that_has_arrived_is_not_held_back_with_the_columns`, where the dataset
    // says a count is still coming exactly while it has none.
    assert!(
        bar.contains("3 rows"),
        "the count it does have is shown: {bar:?}"
    );

    // And says nothing for a dataset that is not the one waiting: the directory this user
    // gave up on goes on reading its footers, and this is not it.
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .give_up_on_pending_footers();
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let other: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !other.contains("Reading footers"),
        "a count belonging to a directory the user left is not this dataset's: {other:?}"
    );
    app.data_table_state
        .as_mut()
        .expect("a dataset")
        .set_footers_pending(std::sync::Arc::new(|_| None));

    // And stops saying it the moment they have.
    app.footer_progress.done();
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Reading footers"),
        "and nothing once they are all in: {bar:?}"
    );
}

/// One frame, one number — while the pass is still running.
///
/// The body and the bar are painted a millisecond apart, with the threads reading the
/// footers moving the counter in between. Reading it once each let them print
/// different numbers for the same wait: measured at four thousand frames out of four
/// thousand. It also let the bar decide there was no count running just after the body
/// had shown one, putting the phase's flat percentage back beside it.
#[test]
fn test_one_frame_says_one_number_while_the_footers_are_still_arriving() {
    let (tx, _rx) = std::sync::mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.set_loading_phase("Caching schema", 40);
    app.footer_progress.begin(200_000);

    // A reader, going as fast as the real ones do between two paints.
    let counter = app.footer_progress.clone();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stopping = stop.clone();
    let reading = std::thread::spawn(move || {
        while !stopping.load(std::sync::atomic::Ordering::Relaxed) {
            counter.advance();
        }
    });

    let area = Rect::new(0, 0, 100, 24);
    // The count itself, not the line: the body is centred and the bar carries the rest
    // of the status beside it, so the two lines differ everywhere except here.
    let number = |row: &str| -> Option<String> {
        row.split("Reading footers: ").nth(1).map(|rest| {
            rest.chars()
                .take_while(|c| c.is_ascii_digit() || *c == ',')
                .collect()
        })
    };
    for frame in 0..200 {
        let mut buf = ratatui::buffer::Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect()
            })
            .collect();
        let body = rows
            .iter()
            .find(|r| r.contains("Reading footers"))
            .and_then(|r| number(r));
        let bar = rows.last().and_then(|r| number(r));
        assert_eq!(
            body,
            bar,
            "frame {frame} said two things about one wait:\n{}",
            rows.join("\n")
        );
        assert!(
            body.is_some(),
            "frame {frame} stopped counting mid-pass:\n{}",
            rows.join("\n")
        );
    }

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    reading.join().unwrap();
}

/// The accent is about the note being *new*: a sort that has something to say brings
/// it back after the panel has already been opened once.
#[test]
fn test_a_sort_that_leaves_rows_out_offers_its_note_afresh() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.mark_notes_seen();
    assert!(
        !state.notes_unseen(),
        "the dataset's own notes have been offered"
    );

    state.sort(vec!["n".to_string()], true);
    assert!(
        state.notes_unseen(),
        "the note about the rows the sort left out has not been"
    );

    state.mark_notes_seen();
    state.sort(vec!["n".to_string()], false);
    assert!(
        !state.notes_unseen(),
        "and sorting the same column the other way says nothing new, so the accent \
         stays away"
    );
}

/// A conflicting file that is not the last one, several of them, and two stretches
/// that do not touch.
///
/// The last file is where a run's end and the end of the dataset are the same number,
/// so a dataset whose only conflict is there cannot tell a right implementation from
/// one that drops everything from the first conflict onwards.
#[test]
fn test_the_rows_left_out_are_the_conflicting_files_own_wherever_they_sit() {
    let dir = tempfile::tempdir().unwrap();
    // Read as an integer: six of the ten rows hold it that way.
    let int = |ids: &[i64], ns: &[i64]| df!("id" => ids, "n" => ns).unwrap();
    let text = |ids: &[i64], ns: &[&str]| df!("id" => ids, "n" => ns).unwrap();
    write_parquet(dir.path(), "date=2024-01-01", int(&[0, 1], &[0, 1]));
    write_parquet(dir.path(), "date=2024-01-02", text(&[2, 3], &["a", "b"]));
    write_parquet(dir.path(), "date=2024-01-03", text(&[4], &["c"]));
    write_parquet(dir.path(), "date=2024-01-04", int(&[5, 6], &[5, 6]));
    write_parquet(dir.path(), "date=2024-01-05", text(&[7], &["d"]));
    write_parquet(dir.path(), "date=2024-01-06", int(&[8, 9], &[8, 9]));

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 10, "every row is there to begin with");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["n".to_string()], true);
    assert!(state.error().is_none(), "the sort itself must succeed");

    let mut ids: Vec<i64> = state
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    ids.sort_unstable();
    assert_eq!(
        ids,
        vec![0, 1, 5, 6, 8, 9],
        "the two stretches that store `n` as text go, and nothing after them does"
    );

    let notes = state.notes();
    let left_out = notes
        .iter()
        .find(|note| note.summary.contains("is not read from"))
        .unwrap_or_else(|| panic!("no note about the rows that went: {notes:#?}"));
    assert_eq!(
        left_out.summary,
        "n is not read from 3 files, so the 4 rows there are left out of the sort"
    );
}

/// Clearing the sort brings the rows back and takes the note with it, and a sort on a
/// column the files agree on never took any rows to begin with.
#[test]
fn test_only_the_conflicting_column_costs_rows_and_only_while_it_is_sorted() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[0i64, 1, 2], "n" => &[0i64, 1, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[3i64, 4], "n" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 24);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(vec!["id".to_string()], true);
    assert_eq!(
        current_rows(&app),
        5,
        "`id` is the same type everywhere, so a sort on it leaves nothing out"
    );
    let state = app.data_table_state.as_mut().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|n| n.summary.contains("is not read from")),
        "and says nothing about rows going"
    );

    state.sort(vec!["n".to_string()], true);
    assert_eq!(current_rows(&app), 3, "sorting by `n` leaves the two out");

    let state = app.data_table_state.as_mut().unwrap();
    state.sort(Vec::new(), true);
    assert_eq!(
        current_rows(&app),
        5,
        "and clearing the sort brings them back"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        !state
            .notes()
            .iter()
            .any(|n| n.summary.contains("is not read from")),
        "with nothing left saying they went: {:#?}",
        state.notes()
    );
}

/// The control for the test above: a directory whose files agree shows neither glyph, so
/// the assertions there are about the data and not about some other part of the screen.
#[test]
fn test_a_uniform_dataset_shows_no_absent_or_conflicting_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "note" => &[None::<&str>]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "note" => &["hi"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let text = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 20));
    assert!(text.contains(g.null), "the real null still shows");
    assert!(!text.contains(g.absent), "nothing is absent here");
    assert!(!text.contains(g.conflict), "nothing conflicts here");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.drifts(), "and the scan stamped no drift column");
}

/// The drift column is the state's own bookkeeping. It must not reach the schema, the
/// column order, or an export.
#[test]
fn test_the_hidden_drift_column_is_never_part_of_the_data() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.drifts(), "this dataset does drift");

    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "extra"],
        "no hidden column in the schema"
    );
    let source: Vec<&str> = state
        .source_schema()
        .iter_names()
        .map(|n| n.as_str())
        .collect();
    assert_eq!(
        source,
        ["date", "id", "extra"],
        "nor in the columns a saved view matches on"
    );
    assert!(
        !state.get_column_order().iter().any(|c| c.starts_with("__")),
        "nor in the column order"
    );

    // What actually reaches a file is what matters, so drive the real export rather
    // than the accessor the export is supposed to use.
    let out = dir.path().join("out.csv");
    let header = export_csv_header(&mut app, &rx, &tx, &out);
    assert_eq!(header, "date,id,extra", "nor in what an export writes");
}

/// Run a CSV export through the app's own export events and return the header line
/// of the file it wrote.
fn export_csv_header(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
) -> String {
    export_csv(app, rx, tx, path, false)
        .lines()
        .next()
        .expect("with a header")
        .to_string()
}

/// Run a CSV export through the app's own export events and return the whole file.
/// `source_file` asks it to name the file each row came from.
fn export_csv(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
    source_file: bool,
) -> String {
    export_as(
        app,
        rx,
        tx,
        path,
        datui::export_modal::ExportFormat::Csv,
        source_file,
    );
    std::fs::read_to_string(path).expect("the export wrote a file")
}

/// Run an export in `format` through the app's own export events.
fn export_as(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    path: &std::path::Path,
    format: datui::export_modal::ExportFormat,
    source_file: bool,
) {
    let options = datui::ExportOptions {
        source_file,
        csv_delimiter: b',',
        csv_include_header: true,
        csv_compression: None,
        json_compression: None,
        ndjson_compression: None,
    };
    let start = AppEvent::Export(datui::ExportRequest {
        path: path.to_path_buf(),
        format,
        options,
        overwrite: datui::output_file::Overwrite::Forbid,
    });
    run_to_idle(app, rx, tx, start);
    assert_eq!(app.error_message(), None, "the export to {path:?} failed");
}

/// Feed `first` to the app and pump until nothing is left to do.
fn run_to_idle(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
    first: AppEvent,
) {
    if let Some(next) = app.event(&first) {
        let _ = tx.send(next);
    }
    pump_until_idle(app, rx, tx);
}

/// Names in `dir` besides `keep`: what an export left behind.
fn leftovers(dir: &Path, keep: &[&str]) -> Vec<String> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| !keep.contains(&name.as_str()))
        .collect()
}

fn csv_request(path: &Path, overwrite: datui::output_file::Overwrite) -> datui::ExportRequest {
    datui::ExportRequest {
        path: path.to_path_buf(),
        format: datui::export_modal::ExportFormat::Csv,
        options: datui::ExportOptions {
            source_file: false,
            csv_delimiter: b',',
            csv_include_header: true,
            csv_compression: Some(datui::CompressionFormat::Gzip),
            json_compression: None,
            ndjson_compression: None,
        },
        overwrite,
    }
}

/// Overwrite agreed to in the dialog: the file is replaced whole, keeps its
/// permission bits, and success is said once it has landed.
#[test]
fn test_an_export_replaces_a_file_after_the_overwrite_is_agreed() {
    let (mut app, rx, tx) = open_query_filter_fixture("export_overwrite_ok.csv");
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");
    std::fs::write(&target, "old contents").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o640)).unwrap();
    }

    press(&mut app, KeyCode::Char('e'));
    app.export_modal.selected_format = datui::export_modal::ExportFormat::Csv;
    app.export_modal
        .path_input
        .set_value(target.display().to_string());
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.active, "an existing file asks first");
    press(&mut app, KeyCode::Left);
    let export = press(&mut app, KeyCode::Enter).expect("Overwrite starts the export");
    run_to_idle(&mut app, &rx, &tx, export);

    assert_eq!(app.error_message(), None);
    assert!(
        app.flash_message()
            .is_some_and(|m| m.starts_with("Exported to ")),
        "success is said once the file is in place"
    );
    let written = std::fs::read_to_string(&target).unwrap();
    assert!(written.starts_with("a,c,name\n"), "{written}");
    assert_eq!(written.lines().count(), 101);
    assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o640, "the replaced file's mode carries over");
    }
}

/// Nothing was there when Enter was pressed, so nothing was agreed to be
/// replaced: a file that appears before the export lands is left alone, and
/// the app says so instead of saying it exported.
#[test]
fn test_a_file_that_appears_during_an_export_is_left_alone() {
    let (mut app, rx, tx) = open_query_filter_fixture("export_appears.csv");
    let dir = tempfile::tempdir().unwrap();
    let target = dir.path().join("out.csv");

    press(&mut app, KeyCode::Char('e'));
    app.export_modal
        .path_input
        .set_value(target.display().to_string());
    let export = press(&mut app, KeyCode::Enter).expect("no file there: the export starts");
    assert!(matches!(
        &export,
        AppEvent::Export(request) if request.overwrite == datui::output_file::Overwrite::Forbid
    ));
    std::fs::write(&target, "theirs").unwrap();
    run_to_idle(&mut app, &rx, &tx, export);

    assert!(
        app.error_message().is_some_and(|m| m.contains("appeared")),
        "the failure reaches the app: {:?}",
        app.error_message()
    );
    assert!(
        !app.flash_message()
            .is_some_and(|m| m.starts_with("Exported to ")),
        "no success for an export that did not land"
    );
    assert!(!app.is_busy());
    assert_eq!(std::fs::read_to_string(&target).unwrap(), "theirs");
    assert!(leftovers(dir.path(), &["out.csv"]).is_empty());
}

/// A view whose rows fail part way through, over an agreed overwrite, by both
/// routes: streamed to an uncompressed CSV, collected for a gzipped one. The error
/// reaches the app, and the old file's bytes, its mode, and nothing else are left.
#[test]
fn test_a_failed_export_keeps_the_old_file() {
    let (mut app, rx, tx) = open_query_filter_fixture("export_fails.csv");
    // Past the first of the streaming engine's 100,000-row batches, so the
    // streamed route has written rows before the failure.
    let rows = 300_000i64;
    let failing = df!("id" => (0..rows).collect::<Vec<_>>())
        .unwrap()
        .lazy()
        .with_column(col("id").map(
            move |c| {
                if c.i64()?.max().is_some_and(|id| id >= rows - 1) {
                    polars_bail!(ComputeError: "injected failure at the last row");
                }
                Ok(c)
            },
            |_, field| Ok(field.clone()),
        ));
    let state =
        datui::widgets::datatable::DataTableState::new(failing, None, None, None, None, true)
            .unwrap();
    app.data_table_state = Some(state);
    let dir = tempfile::tempdir().unwrap();

    for (name, compression) in [
        ("out.csv", None),
        ("out.csv.gz", Some(datui::CompressionFormat::Gzip)),
    ] {
        let target = dir.path().join(name);
        std::fs::write(&target, "old contents").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&target, std::fs::Permissions::from_mode(0o604)).unwrap();
        }
        let mut request = csv_request(&target, datui::output_file::Overwrite::Replace);
        request.options.csv_compression = compression;
        run_to_idle(&mut app, &rx, &tx, AppEvent::Export(request));

        assert!(
            app.error_message().is_some_and(|m| m.contains("injected")),
            "{name}: the failure reaches the app: {:?}",
            app.error_message()
        );
        assert!(!app.is_busy());
        assert!(
            !app.flash_message()
                .is_some_and(|m| m.starts_with("Exported to "))
        );
        assert_eq!(std::fs::read_to_string(&target).unwrap(), "old contents");
        assert!(leftovers(dir.path(), &["out.csv", "out.csv.gz"]).is_empty());
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode = std::fs::metadata(&target).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode, 0o604);
        }
        press(&mut app, KeyCode::Esc);
    }
}

/// With the streaming engine off, a CSV or Parquet export is collected and still
/// writes what the streamed one does.
#[test]
fn test_an_export_without_the_streaming_engine_writes_the_same_file() {
    use datui::export_modal::ExportFormat;
    let dir = tempfile::tempdir().unwrap();
    let mut written = Vec::new();
    for streaming in [true, false] {
        let mut config = datui::AppConfig::default();
        config.performance.polars_streaming = streaming;
        let (mut app, rx, tx) =
            open_query_filter_fixture_with(&format!("export_engine_{streaming}.csv"), config);
        app.data_table_state
            .as_mut()
            .unwrap()
            .query("select a, name where c = 1".to_string());
        pump_until_idle(&mut app, &rx, &tx);
        let csv = dir.path().join(format!("{streaming}.csv"));
        export_as(&mut app, &rx, &tx, &csv, ExportFormat::Csv, false);
        let parquet = dir.path().join(format!("{streaming}.parquet"));
        export_as(&mut app, &rx, &tx, &parquet, ExportFormat::Parquet, false);
        let back = ParquetReader::new(File::open(&parquet).unwrap())
            .finish()
            .unwrap();
        written.push((std::fs::read_to_string(&csv).unwrap(), back));
    }
    let (streamed, collected) = (&written[0], &written[1]);
    assert_eq!(streamed.0, collected.0);
    assert!(
        streamed.0.starts_with("a,name\n1,beta_1\n4,alpha_4\n"),
        "{}",
        streamed.0
    );
    assert_eq!(streamed.0.lines().count(), 34);
    assert!(streamed.1.equals_missing(&collected.1));
}

/// Read a CSV export back with every column as text, sorted by `key`.
fn read_csv_as_text(path: &std::path::Path, key: &str) -> DataFrame {
    CsvReadOptions::default()
        .with_infer_schema_length(Some(0))
        .try_into_reader_with_file_path(Some(path.to_path_buf()))
        .unwrap()
        .finish()
        .unwrap()
        .sort([key], Default::default())
        .unwrap()
}

fn text_column(df: &DataFrame, name: &str) -> Vec<Option<String>> {
    let text = df.column(name).unwrap().str().unwrap();
    (0..text.len())
        .map(|i| text.get(i).map(str::to_string))
        .collect()
}

/// A q-style `by` result holds list columns, which CSV cannot: they are written
/// as JSON text, while Parquet keeps them as lists.
#[test]
fn test_csv_export_writes_a_by_result_lists_as_json() {
    use datui::export_modal::ExportFormat;
    let (mut app, rx, tx) = open_query_filter_fixture("export_nested_by.csv");
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select a, name by c where a < 6".to_string());
    press(&mut app, KeyCode::Char('e'));
    assert!(
        app.export_modal.nested_columns,
        "the dialog knows to say how lists are written"
    );
    let area = Rect::new(0, 0, 80, 24);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    assert!(
        rendered_text(&buffer).contains("Lists and structs are written as JSON."),
        "{}",
        rendered_text(&buffer)
    );
    press(&mut app, KeyCode::Esc);
    let dir = tempfile::tempdir().unwrap();

    let out = dir.path().join("by.csv");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Csv, false);
    let back = read_csv_as_text(&out, "c");
    assert_eq!(
        text_column(&back, "a"),
        ["[0,3]", "[1,4]", "[2,5]"].map(|s| Some(s.to_string()))
    );
    assert_eq!(
        text_column(&back, "name"),
        [
            r#"["alpha_0","beta_3"]"#,
            r#"["beta_1","alpha_4"]"#,
            r#"["alpha_2","beta_5"]"#,
        ]
        .map(|s| Some(s.to_string()))
    );

    let out = dir.path().join("by.parquet");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Parquet, false);
    let back = ParquetReader::new(File::open(&out).unwrap())
        .finish()
        .unwrap();
    assert_eq!(
        back.column("a").unwrap().dtype(),
        &DataType::List(Box::new(DataType::Int64)),
        "Parquet keeps the real type"
    );
}

/// SQL's ARRAY_AGG and a struct column both reach a CSV as
/// JSON; a null list stays an empty field.
#[cfg(feature = "sql")]
#[test]
fn test_csv_export_writes_sql_arrays_and_structs_as_json() {
    let dir = tempfile::tempdir().unwrap();
    let point = StructChunked::from_series(
        "point".into(),
        3,
        [
            Series::new("x".into(), [1i64, 2, 3]),
            Series::new("label".into(), [Some("a"), None, Some("c,d")]),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let df = df!(
        "g" => ["p", "q", "p"],
        "v" => [Some(1i64), None, Some(3)],
    )
    .unwrap()
    .hstack(&[point.into()])
    .unwrap();
    write_parquet(dir.path(), "src", df);
    let (mut app, rx, tx) = open_local_dataset_with_channel(&dir.path().join("src"));
    app.data_table_state.as_mut().unwrap().sql_query(
        "select g, array_agg(v) as vs, first(point) as point from df group by g".to_string(),
    );

    let out = dir.path().join("sql.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, false);
    let back = read_csv_as_text(&out, "g");
    assert_eq!(
        text_column(&back, "vs"),
        [Some("[1,3]".to_string()), Some("[null]".to_string())],
        "{csv}"
    );
    assert_eq!(
        text_column(&back, "point"),
        [
            Some(r#"{"x":1,"label":"a"}"#.to_string()),
            Some(r#"{"x":2,"label":null}"#.to_string()),
        ],
        "{csv}"
    );
}

/// JSON has no binary type: JSON and NDJSON write binary as base64 text, alone
/// and inside a list or struct, and CSV spells it the same way.
#[test]
fn test_json_export_writes_binary_as_base64() {
    use datui::export_modal::ExportFormat;
    let dir = tempfile::tempdir().unwrap();
    let blob = Series::new("blob".into(), [Some(b"hi\xff".as_slice()), None]);
    let blobs = Series::new(
        "blobs".into(),
        [Some(Series::new("".into(), [b"x".as_slice()])), None],
    );
    let meta = StructChunked::from_series(
        "meta".into(),
        2,
        [
            Series::new("raw".into(), [b"ab".as_slice(), b"".as_slice()]),
            Series::new("n".into(), [1i64, 2]),
        ]
        .iter(),
    )
    .unwrap()
    .into_series();
    let df = df!("id" => [1i64, 2])
        .unwrap()
        .hstack(&[blob.into(), blobs.into(), meta.into()])
        .unwrap();
    write_parquet(dir.path(), "src", df);
    let (mut app, rx, tx) = open_local_dataset_with_channel(&dir.path().join("src"));

    for (file, format, json) in [
        ("out.json", ExportFormat::Json, JsonFormat::Json),
        ("out.jsonl", ExportFormat::Ndjson, JsonFormat::JsonLines),
    ] {
        let out = dir.path().join(file);
        export_as(&mut app, &rx, &tx, &out, format, false);
        let back = JsonReader::new(File::open(&out).unwrap())
            .with_json_format(json)
            .finish()
            .unwrap_or_else(|e| panic!("{file}: {e}"));
        let blob = back.column("blob").unwrap().str().unwrap().clone();
        assert_eq!((blob.get(0), blob.get(1)), (Some("aGn/"), None), "{file}");
        let blobs = back.column("blobs").unwrap().list().unwrap().clone();
        let first = blobs.get_as_series(0).unwrap();
        assert_eq!(first.str().unwrap().get(0), Some("eA=="), "{file}");
        assert!(blobs.get_as_series(1).is_none(), "{file}: a null list");
        let raw = back
            .column("meta")
            .unwrap()
            .struct_()
            .unwrap()
            .field_by_name("raw")
            .unwrap();
        assert_eq!(
            (raw.str().unwrap().get(0), raw.str().unwrap().get(1)),
            (Some("YWI="), Some("")),
            "{file}"
        );
    }

    let out = dir.path().join("out.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, false);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(
        lines,
        [
            "id,blob,blobs,meta",
            r#"1,aGn/,"[""eA==""]","{""raw"":""YWI="",""n"":1}""#,
            r#"2,,,"{""raw"":"""",""n"":2}""#,
        ]
    );
}

/// Avro has no fixed-size array or categorical type: the export writes them as
/// a list and as strings, inside a list too, and a view read from two files
/// (two chunks) still makes one readable file.
#[test]
fn test_avro_export_writes_arrays_and_categoricals() {
    use datui::export_modal::ExportFormat;
    use polars::io::avro::AvroReader;
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src");
    std::fs::create_dir_all(&src).unwrap();
    for (file, offset) in [("a.parquet", 0i64), ("b.parquet", 2)] {
        let mut df = df!(
            "id" => [offset, offset + 1],
            "tag" => ["x", "y"],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("tag").cast(DataType::from_categories(Categories::global())),
            concat_list([col("id"), col("id")])
                .unwrap()
                .cast(DataType::Array(Box::new(DataType::Int64), 2))
                .alias("pair"),
        ])
        .with_columns([concat_list([col("tag")]).unwrap().alias("tags")])
        .collect()
        .unwrap();
        ParquetWriter::new(File::create(src.join(file)).unwrap())
            .finish(&mut df)
            .unwrap();
    }
    let (mut app, rx, tx) = open_local_dataset_with_channel(&src);
    let schema = app.data_table_state.as_ref().unwrap().schema().clone();
    assert!(
        matches!(schema.get("pair"), Some(DataType::Array(..)))
            && matches!(schema.get("tag"), Some(DataType::Categorical(..))),
        "the view has the types Avro lacks: {schema:?}"
    );

    let out = dir.path().join("out.avro");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Avro, false);
    let back = AvroReader::new(File::open(&out).unwrap())
        .finish()
        .unwrap()
        .sort(["id"], Default::default())
        .unwrap();
    assert_eq!(back.height(), 4);
    assert_eq!(
        back.column("pair").unwrap().dtype(),
        &DataType::List(Box::new(DataType::Int64))
    );
    assert_eq!(back.column("tag").unwrap().dtype(), &DataType::String);
    assert_eq!(
        back.column("tags").unwrap().dtype(),
        &DataType::List(Box::new(DataType::String))
    );
    let tag = back.column("tag").unwrap().str().unwrap().clone();
    assert_eq!(
        (0..4).map(|i| tag.get(i)).collect::<Vec<_>>(),
        [Some("x"), Some("y"), Some("x"), Some("y")]
    );
}

/// The writer schema from an Avro file's header.
fn avro_schema(path: &Path) -> serde_json::Value {
    fn long(bytes: &[u8], at: &mut usize) -> i64 {
        let (mut value, mut shift) = (0u64, 0);
        loop {
            let byte = bytes[*at];
            *at += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return (value >> 1) as i64 ^ -((value & 1) as i64);
            }
            shift += 7;
        }
    }
    fn bytes_at<'a>(bytes: &'a [u8], at: &mut usize) -> &'a [u8] {
        let len = long(bytes, at) as usize;
        *at += len;
        &bytes[*at - len..*at]
    }
    let bytes = std::fs::read(path).unwrap();
    assert_eq!(&bytes[..4], b"Obj\x01");
    let mut at = 4;
    loop {
        let count = long(&bytes, &mut at);
        assert_ne!(count, 0, "no avro.schema in the header");
        if count < 0 {
            long(&bytes, &mut at);
        }
        for _ in 0..count.abs() {
            let key = bytes_at(&bytes, &mut at);
            let value = bytes_at(&bytes, &mut at);
            if key == b"avro.schema" {
                return serde_json::from_slice(value).unwrap();
            }
        }
    }
}

/// Every record and field name in an Avro schema.
fn avro_schema_names(schema: &serde_json::Value, names: &mut Vec<String>) {
    use serde_json::Value;
    match schema {
        Value::Array(branches) => branches.iter().for_each(|b| avro_schema_names(b, names)),
        Value::Object(map) => {
            if let Some(Value::String(name)) = map.get("name") {
                names.push(name.clone());
            }
            for field in map
                .get("fields")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                names.push(field["name"].as_str().unwrap().to_string());
                avro_schema_names(&field["type"], names);
            }
            for key in ["type", "items"] {
                if let Some(inner) = map.get(key) {
                    avro_schema_names(inner, names);
                }
            }
        }
        _ => {}
    }
}

/// Avro names are `[A-Za-z_][A-Za-z0-9_]*`: the export renames columns and
/// struct fields to fit, the record gets a name, and the values stay.
#[test]
fn test_avro_export_writes_valid_names() {
    use datui::export_modal::ExportFormat;
    use polars::io::avro::AvroReader;
    let dir = tempfile::tempdir().unwrap();
    let df = df!(
        "my col" => [1i64, 2],
        "2024" => ["x", "y"],
        "a-b" => [1.5f64, 2.5],
        "a_b" => [true, false],
    )
    .unwrap()
    .lazy()
    .with_columns([
        as_struct(vec![col("my col").alias("x y"), col("2024").alias("x-y")]).alias("délai"),
    ])
    .collect()
    .unwrap();
    write_parquet(dir.path(), "src", df);
    let (mut app, rx, tx) = open_local_dataset_with_channel(&dir.path().join("src"));
    press(&mut app, KeyCode::Char('e'));
    assert!(app.export_modal.avro_renames, "the dialog says so");
    press(&mut app, KeyCode::Esc);

    let out = dir.path().join("out.avro");
    export_as(&mut app, &rx, &tx, &out, ExportFormat::Avro, false);
    let schema = avro_schema(&out);
    let mut names = Vec::new();
    avro_schema_names(&schema, &mut names);
    assert!(names.contains(&"Row".to_string()), "{names:?}");
    for name in &names {
        let mut chars = name.chars();
        assert!(
            chars
                .next()
                .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
                && chars.all(|c| c.is_ascii_alphanumeric() || c == '_'),
            "{name:?} in {names:?}"
        );
    }
    let docs: Vec<(&str, Option<&str>)> = schema["fields"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| (f["name"].as_str().unwrap(), f["doc"].as_str()))
        .collect();
    assert_eq!(
        docs,
        [
            ("my_col", Some("my col")),
            ("_2024", Some("2024")),
            ("a_b_2", Some("a-b")),
            ("a_b", None),
            ("d_lai", Some("délai")),
        ],
        "the original names stay in the file"
    );

    let back = AvroReader::new(File::open(&out).unwrap()).finish().unwrap();
    let columns: Vec<&str> = back.get_column_names().iter().map(|n| n.as_str()).collect();
    assert_eq!(columns, ["my_col", "_2024", "a_b_2", "a_b", "d_lai"]);
    assert_eq!(
        back.column("_2024").unwrap().str().unwrap().get(1),
        Some("y")
    );
    assert_eq!(
        back.column("a_b_2").unwrap().f64().unwrap().get(0),
        Some(1.5)
    );
    assert_eq!(
        back.column("a_b").unwrap().bool().unwrap().get(0),
        Some(true)
    );
    let point = back.column("d_lai").unwrap().struct_().unwrap().clone();
    assert_eq!(
        point.field_by_name("x_y").unwrap().i64().unwrap().get(1),
        Some(2)
    );
    assert_eq!(
        point.field_by_name("x_y_2").unwrap().str().unwrap().get(0),
        Some("x")
    );
}

/// Every copy format takes list cells as JSON, the same text a CSV export
/// writes, instead of failing on them.
#[test]
fn test_copy_writes_list_cells_as_json() {
    use datui::clipboard::{Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let (mut app, rx, tx) = open_query_filter_fixture("copy_nested_by.csv");
    app.data_table_state
        .as_mut()
        .unwrap()
        .query("select name by c where a < 2".to_string());
    pump_until_idle(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    pump_until_idle(&mut app, &rx, &tx);
    app.render(area, &mut buffer);
    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));

    // The table scope, collected off-thread, as TSV with its header.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('t'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let text = copies.lock().unwrap().last().expect("a copy").text.clone();
    let mut lines: Vec<&str> = text.lines().collect();
    lines[1..].sort();
    assert_eq!(
        lines,
        [
            "c\tname",
            "0\t\"[\"\"alpha_0\"\"]\"",
            "1\t\"[\"\"beta_1\"\"]\""
        ]
    );

    // The same scope as Markdown.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char('m'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let text = copies.lock().unwrap().last().expect("a copy").text.clone();
    assert!(text.contains(r#"["alpha_0"]"#), "{text}");
}

/// A query builds its own rows, and its schema becomes the column order — so a query
/// root that still carried the hidden drift column turned it into one of the data's,
/// visible in the table and the sidebar.
#[test]
fn test_a_query_never_turns_the_drift_column_into_a_real_one() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );
    let expected = ["date", "id", "extra"];

    for (what, run) in [
        ("a fuzzy search", 0),
        ("a DSL query", 1),
        ("a SQL query", 2),
        ("a reset", 3),
    ] {
        let mut app = open_local_dataset(dir.path());
        let state = app.data_table_state.as_mut().unwrap();
        match run {
            0 => state.fuzzy_search("x".to_string()),
            1 => state.query("select where id > 0".to_string()),
            2 => state.sql_query("select * from df".to_string()),
            _ => state.reset(),
        }
        assert!(state.error().is_none(), "{what}: {:?}", state.error());
        state.collect();
        assert!(
            state.error().is_none(),
            "{what} collect: {:?}",
            state.error()
        );

        let order: Vec<&str> = state
            .get_column_order()
            .iter()
            .map(|s| s.as_str())
            .collect();
        assert_eq!(order, expected, "column order after {what}");
        let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
        assert_eq!(names, expected, "schema after {what}");
    }
}

/// A reset returns to the data as opened, so the cells that stand for a file the
/// column was never in read as absent again.
#[test]
fn test_a_reset_brings_back_the_absent_cells() {
    let g = datui::glyphs::get();
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "extra" => &["x", "y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 20);
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "absent at open"
    );

    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("select * from df".to_string());
    state.collect();
    assert!(state.error().is_none(), "the query: {:?}", state.error());
    assert!(
        !state.drifts(),
        "a query's rows stand for no file, so nulls are plain nulls"
    );

    let state = app.data_table_state.as_mut().unwrap();
    state.reset();
    state.collect();
    assert!(state.error().is_none(), "the reset: {:?}", state.error());
    assert!(state.drifts(), "and the reset puts the files back");
    assert!(
        painted(&mut app, &rx, &tx, area).contains(g.absent),
        "so the absent cells read as absent again"
    );
}

/// Counting a many-file scan's rows must not kill the app.
///
/// A dataset whose files disagree on a column's type is read as a union of scans, one
/// per run of files. `len()` is `UInt32`, and summing it across a union widens to
/// `UInt128`, which the streaming engine panics on rather than erroring — and datui
/// runs streaming by default. Anything that invalidates the row count (a filter, a
/// query, a reset) took the whole process down with it.
#[test]
fn test_counting_a_union_of_scans_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    // `n` is text in one file and a number in the other, so the two are scanned apart.
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "n" => &["a", "b"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "n" => &[10i64, 20]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_mut().unwrap();
    state.fuzzy_search("a".to_string());
    assert!(state.error().is_none(), "fuzzy search: {:?}", state.error());
    state.collect();
    assert!(
        state.error().is_none(),
        "collect after the search: {:?}",
        state.error()
    );
}

/// Analysis counts the rows itself, which is the same `len()` over a union of scans
/// that crashed the table. Its panic is worse: it happens inside `spawn_blocking`,
/// where tokio swallows it, so the panel never finishes and the app wedges on
/// "Computing statistics…" with the panic text over the raw-mode screen.
#[test]
fn test_analysing_a_union_of_scans_does_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64, 4], "n" => &["a", "b"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64, 3], "n" => &[10i64, 20]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let results = datui::statistics::compute_statistics_with_options(
        &state.lf_clone(),
        None,
        0,
        datui::statistics::ComputeOptions {
            polars_streaming: true,
            ..Default::default()
        },
    )
    .expect("analysis runs over a many-file scan");
    assert_eq!(results.total_rows, 4, "and counts every row");
}

/// Opening a directory whose files disagree leaves something to say, and the Info key
/// carries a quiet accent until the panel has been opened.
#[test]
fn test_a_drifting_dataset_has_notes_and_offers_them_once() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let notes = state.notes();
    assert_eq!(notes.len(), 1, "one column is not in every file");
    assert_eq!(
        notes[0].summary,
        "extra is in 1 of 2 files, only date=2024-01-02; absent from the rest, \
         not null"
    );
    assert_eq!(
        notes[0].scope, "in all 2 footers",
        "and says what it is based on"
    );
    assert!(state.notes_unseen(), "not offered yet");

    // Pressing i opens the panel, which is the offer being taken up.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.notes_unseen(), "the accent has done its job");
}

/// A query builds its own rows, so notes about the files behind the dataset no longer
/// describe what is on screen. They come back on a reset.
#[test]
fn test_a_query_puts_the_notes_away_and_a_reset_brings_them_back() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_mut().unwrap();
    assert_eq!(state.notes().len(), 1, "the dataset has something to say");

    state.sql_query("select id from df".to_string());
    state.collect();
    assert!(state.error().is_none(), "the query: {:?}", state.error());
    assert!(
        state.notes().is_empty(),
        "a note about `extra` would describe a column the frame no longer has"
    );

    state.reset();
    state.collect();
    assert!(state.error().is_none(), "the reset: {:?}", state.error());
    assert_eq!(state.notes().len(), 1, "and the reset brings them back");
}

/// More notes than the panel is tall must not be dropped on the floor: the panel says
/// how many are out of view, and the cursor reaches them.
#[test]
fn test_notes_past_the_fold_are_counted_and_reachable() {
    let dir = tempfile::tempdir().unwrap();
    // Six columns, each arriving one day later, is six notes.
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "a" => &["x"], "b" => &["x"], "c" => &["x"],
            "d" => &["x"], "e" => &["x"], "f" => &["x"])
        .unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    assert_eq!(app.data_table_state.as_ref().unwrap().notes().len(), 6);

    // Unread notes put their tab in front, so opening the panel is the whole walk.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // A short panel cannot show six notes at two lines each plus a gap.
    let area = Rect::new(0, 0, 100, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();

    // Derived rather than counted out here: how many notes fit depends on how long
    // they are, and a note says more now than it used to. What has to hold is that the
    // ones that do not fit are counted rather than dropped.
    let shown = ["a", "b", "c", "d", "e", "f"]
        .iter()
        .filter(|name| screen.contains(&format!("{name} is in 1 of 2 files")))
        .count();
    assert!(
        (2..6).contains(&shown),
        "some notes fit and some do not, which is what this is about — and a panel \
         this tall holds at least two, or the panel has got much greedier than the \
         note got longer: {shown} of 6"
    );
    assert!(
        screen.contains(&format!("{} below", 6 - shown)),
        "and the ones out of view are counted, got:\n{screen}"
    );
    assert!(
        screen.contains("a is in 1 of 2 files"),
        "the first note is shown"
    );
    assert!(
        screen.contains("in all 2 footers"),
        "and the scope of the note the cursor is on, got:\n{screen}"
    );

    // The cursor reaches the last note, which scrolls it into view.
    for _ in 0..6 {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char('j'),
            KeyModifiers::NONE,
        )));
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(
        screen.contains("f is in 1 of 2 files"),
        "the last note is reachable, got:\n{screen}"
    );
    assert!(
        screen.contains("above"),
        "and the panel says what scrolled off the top, got:\n{screen}"
    );

    // Too short to hold a note is not the same as having none to hold: the panel
    // says which it is, and never claims there is nothing to say.
    for height in 4u16..26 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
        let drew_a_note = screen.contains("is in 1 of");
        let said_no_room = screen.contains("no room");
        assert!(
            drew_a_note ^ said_no_room,
            "a {height}-row panel draws a note or says it has no room for one, \
             exactly one of the two: {screen:?}"
        );
    }

    // A summary without its scope line under it is the misreading the scope line
    // exists to prevent, so no height may produce one.
    for height in 4u16..26 {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect();
        for (i, row) in rows.iter().enumerate() {
            if !row.contains("is in 1 of 2 files") {
                continue;
            }
            // The line a note rests on follows its summary, and always before the
            // next note begins.
            let found = rows[i + 1..]
                .iter()
                .take_while(|later| !later.contains("is in 1 of 2 files"))
                .any(|later| later.contains("in all 2 footers"));
            assert!(
                found,
                "at height {height}, a note is drawn with no basis under it:\n{}",
                row.trim_end()
            );
        }
    }

    // A note needs its summary and its basis, so one row cannot hold one. Say that
    // rather than draw half a note.
    let area = Rect::new(0, 0, 100, 4);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(
        screen.contains("6 notes; no room for this one"),
        "too short for a whole note says so, got:\n{screen}"
    );
}

/// A panel exactly as tall as one note draws it, rather than reporting no room.
#[test]
fn test_a_note_that_fills_the_panel_is_drawn_not_refused() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "a" => &["x"], "b" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    // Unread notes put their tab in front, so opening the panel is the whole walk.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    // The shortest panel that draws a note, found rather than written down: how tall
    // that is depends on how long a note is, and a note says more than it used to.
    let mut drawn_at = |height: u16| -> String {
        let area = Rect::new(0, 0, 100, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    };
    let shortest = (4u16..14)
        .find(|height| drawn_at(*height).contains("is in 1 of 2 files"))
        .expect("some panel in this range draws a note");
    assert!(
        shortest <= 7,
        "a note fits in a short panel; {shortest} rows to draw one means the panel has \
         got greedier, and the loop below would pass on one height and prove nothing"
    );

    // From there up, every height draws one. The bug this guards is a panel that has
    // the room and refuses anyway, which showed as a gap in the middle of this range.
    for height in shortest..14 {
        let screen = drawn_at(height);
        assert!(
            screen.contains("is in 1 of 2 files"),
            "a {height}-row panel has room for a note — {shortest} rows is enough — so \
             it draws one: {screen:?}"
        );
        assert!(
            !screen.contains("no room"),
            "and does not claim otherwise: {screen:?}"
        );
    }
}

/// A directory whose files agree has nothing to say, and nothing to show for it.
#[test]
fn test_a_uniform_dataset_has_no_notes() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(dir.path(), "date=2024-01-02", df!("id" => &[2i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.notes().is_empty());
    assert!(!state.notes_unseen(), "so no accent either");
}

/// A `/`-separated path as this platform writes it.
fn native(path: &str) -> String {
    path.replace('/', std::path::MAIN_SEPARATOR_STR)
}

/// Asking an export to name each row's file keeps the absent-versus-null distinction
/// once the data has left datui: `extra` is empty in both rows, but only one of them
/// came from a file that had the column.
#[test]
fn test_an_export_can_name_the_file_each_row_came_from() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &[None::<&str>]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .can_name_source_files(),
        "the files disagree, so there is something to name"
    );

    let out = dir.path().join("named.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let lines: Vec<&str> = csv.lines().collect();
    assert_eq!(lines[0], "date,id,extra,source_file");
    assert!(
        lines[1].ends_with(&native("date=2024-01-01/data.parquet")),
        "the first row came from the file without `extra`: {}",
        lines[1]
    );
    assert!(
        lines[2].ends_with(&native("date=2024-01-02/data.parquet")),
        "and the second from the one that has it, holding a real null: {}",
        lines[2]
    );
    // Both write null — an absent cell and a real null are the same thing to a CSV, and
    // the source file is what tells them apart once the data has left. Asserting the
    // whole line rather than its end, because `extra` being empty is the half of this
    // the doc comment claims and nothing checked.
    assert!(
        lines[1].starts_with("2024-01-01,1,,"),
        "the absent cell is written as null: {}",
        lines[1]
    );
    assert!(
        lines[2].starts_with("2024-01-02,2,,"),
        "and so is the real one: {}",
        lines[2]
    );

    // Streamed to Parquet, the names are the same.
    let parquet = dir.path().join("named.parquet");
    export_as(
        &mut app,
        &rx,
        &tx,
        &parquet,
        datui::export_modal::ExportFormat::Parquet,
        true,
    );
    let back = ParquetReader::new(File::open(&parquet).unwrap())
        .finish()
        .unwrap();
    assert_eq!(
        back.get_column_names(),
        ["date", "id", "extra", "source_file"]
    );
    let files = back.column("source_file").unwrap().str().unwrap().clone();
    assert!(
        files
            .get(0)
            .is_some_and(|f| f.ends_with(&native("date=2024-01-01/data.parquet")))
            && files
                .get(1)
                .is_some_and(|f| f.ends_with(&native("date=2024-01-02/data.parquet"))),
        "{files:?}"
    );

    // Off by default, and then the hidden index must not leak in its place.
    let plain = dir.path().join("plain.csv");
    let csv = export_csv(&mut app, &rx, &tx, &plain, false);
    let plain_lines: Vec<&str> = csv.lines().collect();
    assert_eq!(plain_lines[0], "date,id,extra");
    assert_eq!(
        &plain_lines[1..],
        ["2024-01-01,1,", "2024-01-02,2,"],
        "and without it both are still null, and nothing else has appeared"
    );
}

/// Avro widens every `u32`; the files are named from the hidden row index before
/// that, through the collected route.
#[test]
fn test_an_avro_export_can_name_the_file_each_row_came_from() {
    use polars::io::avro::AvroReader;
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &[None::<&str>]).unwrap(),
    );
    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());

    let out = dir.path().join("named.avro");
    export_as(
        &mut app,
        &rx,
        &tx,
        &out,
        datui::export_modal::ExportFormat::Avro,
        true,
    );
    let back = AvroReader::new(File::open(&out).unwrap()).finish().unwrap();
    let files = back.column("source_file").unwrap().str().unwrap().clone();
    assert!(
        files
            .get(0)
            .unwrap()
            .ends_with(&native("date=2024-01-01/data.parquet"))
            && files
                .get(1)
                .unwrap()
                .ends_with(&native("date=2024-01-02/data.parquet")),
        "{files:?}"
    );
}

/// A dataset may already have a column called `source_file` — a directory of per-file
/// extracts is exactly this feature's audience — and adding one by that name would
/// replace it, silently, in the file the user takes away.
#[test]
fn test_naming_source_files_never_overwrites_a_column_of_that_name() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("id" => &[1i64], "source_file" => &["mine-A"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "source_file" => &["mine-B"], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let out = dir.path().join("collide.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let lines: Vec<&str> = csv.lines().collect();

    assert_eq!(
        lines[0], "date,id,source_file,extra,source_file_1",
        "the dataset keeps its own column and datui's goes on the end under another name"
    );
    assert!(
        lines[1].contains("mine-A"),
        "the dataset's own values survive: {}",
        lines[1]
    );
    assert!(lines[2].contains("mine-B"), "both of them: {}", lines[2]);
}

/// Asking for source files on a frame that no longer has them must not leak datui's
/// bookkeeping instead.
///
/// The option is on but the dataset cannot honor it, so the export plans the view
/// without the index at all.
#[test]
fn test_asking_to_name_files_on_a_query_result_leaks_nothing() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    // A query replaces the frame, so the rows no longer stand for rows of a file and
    // naming them is refused — but the export still runs.
    let state = app.data_table_state.as_mut().unwrap();
    state.sql_query("select * from df".to_string());
    state.collect();
    assert!(!state.can_name_source_files(), "nothing to name any more");

    let out = dir.path().join("refused.csv");
    let csv = export_csv(&mut app, &rx, &tx, &out, true);
    let header = csv.lines().next().unwrap();
    assert!(
        !header.contains("__datui_row"),
        "datui's own bookkeeping must not reach the file: {header}"
    );
}

/// The Options panel must read as a panel at every format: no empty box, and the
/// source-file checkbox under the format's own options rather than adrift at the foot.
#[test]
fn test_the_export_options_panel_reads_as_one_for_every_format() {
    use datui::export_modal::ExportFormat;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    // The modal opens with focus on the path, where Down does not change the format.
    for key in [KeyCode::Char('e'), KeyCode::BackTab] {
        app.event(&AppEvent::Key(KeyEvent::new(key, KeyModifiers::NONE)));
    }
    assert!(app.export_modal.offer_source_file, "the files disagree");
    assert_eq!(
        app.export_modal.focus,
        datui::export_modal::ExportFocus::FormatSelector,
        "so the walk below really does change format"
    );

    let area = Rect::new(0, 0, 120, 30);
    let mut seen = Vec::new();
    let mut wrong = Vec::new();
    for _ in 0..ExportFormat::ALL.len() {
        let format = app.export_modal.selected_format;
        seen.push(format);
        // The last row each format draws of its own. The checkbox goes directly under
        // it, so asking for this row by name pins the form's row list: a row missing
        // and the format's last option is gone, a row extra and a gap opens up.
        // Either way this row is no longer the one above the checkbox.
        let last_of_its_own = match format {
            // All three end on their compression row.
            ExportFormat::Csv | ExportFormat::Json | ExportFormat::Ndjson => "Compression:",
            // No options of their own, so the checkbox sits right under the path.
            ExportFormat::Parquet | ExportFormat::Ipc | ExportFormat::Avro => "Path:",
        };

        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let rows: Vec<String> = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol().to_string())
                    .collect::<String>()
            })
            .collect();
        match rows.iter().position(|r| r.contains("Source file:")) {
            None => wrong.push(format!("{format:?}: no Source file row at all")),
            Some(checkbox) if !rows[checkbox - 1].contains(last_of_its_own) => wrong.push(format!(
                "{format:?}: the row above the checkbox should be the one holding \
                 {last_of_its_own:?}, and is {:?}",
                rows[checkbox - 1].trim_end()
            )),
            Some(_) => {}
        }
        // Move to the next format.
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Down,
            KeyModifiers::NONE,
        )));
    }
    // Collected rather than asserted in the loop: the formats fail in families, and
    // one report naming every bad format beats six runs that each name the first.
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    assert_eq!(
        seen,
        ExportFormat::ALL.to_vec(),
        "the walk must visit every format once, in order"
    );
}

/// A column only a middle file has used to vanish: the schema was one file's, and that
/// file did not have it.
#[test]
fn test_a_column_only_one_local_file_has_is_shown() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-03-01",
        df!("id" => &[1i64, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-03-02",
        df!("id" => &[3i64], "oops" => &["x"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-03-03",
        df!("id" => &[4i64, 5]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(
        names,
        ["date", "id", "oops"],
        "the partition key, then every column any file has"
    );
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.origin.to_string(), "all 3 footers");
    let drifting: Vec<String> = dataset.drifting().map(|c| c.name.to_string()).collect();
    assert_eq!(drifting, ["oops"]);
}

/// Files written with different integer widths used to fail the strict local scan.
#[test]
fn test_local_files_of_different_integer_widths_open_as_the_wider_one() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "date=2024-01-01",
        df!("n" => &[1i32, 2]).unwrap(),
    );
    write_parquet(dir.path(), "date=2024-01-02", df!("n" => &[3i64]).unwrap());

    let app = open_local_dataset(dir.path());
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.schema().get("n"),
        Some(&polars::prelude::DataType::Int64)
    );
}

/// One unreadable file must not stop the rest of the directory from opening.
#[test]
fn test_one_unreadable_local_file_does_not_stop_the_open() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    let bad = dir.path().join("date=2024-01-02");
    std::fs::create_dir_all(&bad).unwrap();
    std::fs::write(bad.join("data.parquet"), b"not a parquet file").unwrap();
    write_parquet(dir.path(), "date=2024-01-03", df!("id" => &[3i64]).unwrap());

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let screen = painted(&mut app, &rx, &tx, Rect::new(0, 0, 100, 24));
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<&str> = state.schema().iter_names().map(|n| n.as_str()).collect();
    assert_eq!(names, ["date", "id"]);
    let dataset = state.dataset_schema().expect("read from the footers");
    assert_eq!(dataset.unreadable, [1], "named, and left out of the scan");
    // The dataset opening is the claim, so the rows are what has to be there. A schema
    // computed over a file that would not parse says nothing about whether the other
    // two can be read through it.
    assert!(
        !screen.contains("Error"),
        "and on screen, without an error: {screen}"
    );
    // The ids, not the partition names: `date=2024-01-01` and `date=2024-01-03` put a
    // `1` and a `3` on screen whatever the rows say, so checking for those characters
    // is a check on the directory's own names.
    let state = app.data_table_state.as_ref().unwrap();
    let ids = state
        .lf()
        .clone()
        .collect()
        .expect("the two readable files are readable")
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect::<Vec<i64>>();
    assert_eq!(ids, [1, 3], "the two readable files' rows are there");
}

// ---------------------------------------------------------------------------
// Abandoning an in-flight load (Ctrl+O to the home screen)
// ---------------------------------------------------------------------------

/// Drains like the real main loop does: a handler that returns a follow-up event
/// queues it and *ends the pass*, so one frame is drawn between chain steps.
///
/// `pump_open_until_loaded` above chases the chain without breaking, which cannot
/// reproduce a keypress landing between two steps — exactly the window abandonment
/// has to survive. Returns the number of chain steps taken this pass.
fn drain_like_main_loop(
    app: &mut App,
    tx: &mpsc::Sender<AppEvent>,
    rx: &mpsc::Receiver<AppEvent>,
) -> usize {
    let mut steps = 0;
    loop {
        match rx.try_recv() {
            Ok(AppEvent::Crash(msg)) => panic!("Crash during load: {msg}"),
            Ok(event) => {
                if let Some(next) = app.event(&event) {
                    tx.send(next).unwrap();
                    steps += 1;
                    break;
                }
            }
            Err(_) => break,
        }
    }
    steps
}

fn ctrl_o() -> AppEvent {
    AppEvent::Key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
}

fn key(code: KeyCode) -> AppEvent {
    AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE))
}

/// Every glyph in the buffer, in row order — enough to ask whether some text is on
/// screen, which is all these tests need.
fn rendered_text(buf: &Buffer) -> String {
    buf.content().iter().map(|cell| cell.symbol()).collect()
}

/// The same, without the control bar on the last row. The bar reports a load on its
/// own; assertions about what the *view* shows have to exclude it.
fn main_area_text(buf: &Buffer, area: Rect) -> String {
    let cells = (area.width as usize) * (area.height as usize - 1);
    buf.content()
        .iter()
        .take(cells)
        .map(|cell| cell.symbol())
        .collect()
}

/// Ctrl+O at any point during a load must abandon it: whatever is on screen when the
/// user goes home is what is still there afterwards. The load runs to completion in
/// the background and its results are dropped.
///
/// Parameterised over how many chain steps have run, because the pipeline has many
/// interstitial frames and each one is a place the user can press the key.
#[test]
fn test_abandoned_load_never_installs_itself_afterwards() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let area = Rect::new(0, 0, 120, 50);

    for abandon_after in 0..8 {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx.clone(), common::test_runtime());
        tx.send(AppEvent::Open(vec![path.clone()], OpenOptions::default()))
            .unwrap();

        let mut steps = 0usize;
        let mut abandoned_at: Option<(Option<PathBuf>, bool)> = None;
        let mut ticks_since_abandon = 0usize;
        // The loop waits on the chain and on the abandoned work, never on a tick
        // count a loaded machine can outrun; the clock is only a safety net.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);

        loop {
            assert!(
                std::time::Instant::now() < deadline,
                "abandon_after {abandon_after}: timed out, abandoned: {}",
                abandoned_at.is_some()
            );
            let drained = drain_like_main_loop(&mut app, &tx, &rx);
            steps += drained;

            // Abandon once the chain has taken `abandon_after` steps, or as soon as it
            // has finished if it was shorter than that — going home after a completed
            // load must be just as inert.
            let chain_done = !app.is_busy() && app.data_table_state.is_some();
            if abandoned_at.is_none() && (steps >= abandon_after || chain_done) {
                app.event(&ctrl_o());
                abandoned_at = Some((
                    app.open_path().map(Path::to_path_buf),
                    app.data_table_state.is_some(),
                ));
            }

            let mut buf = Buffer::empty(area);
            app.render(area, &mut buf);

            // Done when the abandoned scan and schema have reported back and their
            // results were handled: they had every chance to install themselves.
            // A few ticks more give the unleased row count its chance too.
            if abandoned_at.is_some() {
                ticks_since_abandon += 1;
                if ticks_since_abandon >= 40 && drained == 0 && !app.background_work_in_flight() {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }

        let (path_at_abandon, had_state) = abandoned_at.expect("never reached the abandon point");

        assert_eq!(
            app.input_mode,
            InputMode::Home,
            "abandon_after {abandon_after}: should still be at home"
        );
        assert!(
            !app.is_busy(),
            "abandon_after {abandon_after}: abandoning a load must clear busy"
        );
        assert_eq!(
            app.open_path().map(Path::to_path_buf),
            path_at_abandon,
            "abandon_after {abandon_after}: the abandoned load swapped its dataset in afterwards"
        );
        assert_eq!(
            app.data_table_state.is_some(),
            had_state,
            "abandon_after {abandon_after}: data_table_state changed after abandonment"
        );
    }
}

/// The regression this whole change exists for: abandon a slow load, open something
/// else, and the abandoned load must not overwrite the dataset you actually asked
/// for — including its row count, which is counted by a separate background task.
#[test]
fn test_abandoned_load_does_not_corrupt_the_next_open() {
    common::ensure_sample_data();
    let big = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let small = PathBuf::from("tests/sample-data/sales.parquet");
    let area = Rect::new(0, 0, 120, 50);

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![big], OpenOptions::default()))
        .unwrap();

    // Let the big load get underway, then leave.
    for _tick in 0..3 {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
    }
    app.event(&ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);

    tx.send(AppEvent::Open(vec![small.clone()], OpenOptions::default()))
        .unwrap();

    // Until the second open has settled and the abandoned work has reported back,
    // however long a loaded machine takes; the clock is only a safety net.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(120);
    for tick in 0.. {
        assert!(
            std::time::Instant::now() < deadline,
            "the second open never settled"
        );
        let drained = drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        app.frame_painted();
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        let settled = drained == 0
            && !needs
            && !app.is_busy()
            && !app.background_work_in_flight()
            && !app.row_count_pending()
            && app.data_table_state.is_some();
        if tick >= 40 && settled {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    assert_eq!(
        app.open_path(),
        Some(small.as_path()),
        "the dataset opened after abandoning should be the one on screen"
    );
    let state = app
        .data_table_state
        .as_ref()
        .expect("second open should have installed a dataset");
    assert_eq!(
        state.num_rows_if_valid(),
        Some(5000),
        "row count belongs to the dataset that is open, not the abandoned one"
    );
}

/// Abandoning drops the incoming dataset, not the one already on screen. Esc from
/// home has to put the user back where they were.
#[test]
fn test_escape_from_home_returns_to_the_dataset_that_was_open() {
    common::ensure_sample_data();
    let open_first = PathBuf::from("tests/sample-data/people.parquet");
    let abandoned = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let area = Rect::new(0, 0, 120, 50);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(
        vec![open_first.clone()],
        OpenOptions::default(),
    ))
    .unwrap();

    // Render as we go: `visible_rows` is set by the render, and without it there is
    // no display slice to assert on later.
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        if app.data_table_state.is_some() && !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(app.open_path(), Some(open_first.as_path()));
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.display_slice_df().is_some()),
        "first dataset should be displayable before we abandon anything"
    );

    // Start a second load and leave before it can install.
    tx.send(AppEvent::Open(vec![abandoned], OpenOptions::default()))
        .unwrap();
    drain_like_main_loop(&mut app, &tx, &rx);
    app.event(&ctrl_o());

    // Until the abandoned load has reported everything it was going to.
    for _tick in ticks() {
        let stepped = drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        if stepped == 0 && !app.background_work_in_flight() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));

    assert_eq!(
        app.input_mode,
        InputMode::Normal,
        "Esc from home should return to the open dataset"
    );
    assert_eq!(
        app.open_path(),
        Some(open_first.as_path()),
        "the abandoned load must not have replaced what was open"
    );
    assert!(
        app.data_table_state
            .as_ref()
            .is_some_and(|s| s.display_slice_df().is_some()),
        "the dataset we returned to should still have its buffer"
    );
}

/// A key typed while an analysis runs is held, and Esc cancelling the run drops it: an
/// impatient second Enter replayed after the cancel started the run again behind it.
#[test]
fn test_keys_held_during_an_analysis_do_not_outlive_its_cancel() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");
    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), common::test_runtime());
    let mut pump = EventPump::new(app, tx, rx);
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    for _ in ticks() {
        if !pump.app.is_busy() && pump.app.data_table_state.is_some() {
            break;
        }
        pump.wait_and_drain(std::time::Duration::from_millis(100))
            .unwrap();
    }

    let press = |pump: &mut EventPump, code| {
        pump.terminal_key(KeyEvent::new(code, KeyModifiers::NONE))
            .unwrap();
    };
    press(&mut pump, KeyCode::Char('a'));
    pump.app.analysis_modal.sidebar_state.select(Some(1));
    // The first Enter shows the tool's Sample form; the second runs it.
    press(&mut pump, KeyCode::Enter);
    press(&mut pump, KeyCode::Enter);
    pump.drain().unwrap();
    assert!(
        pump.app.analysis_modal.computing.is_some(),
        "the run started"
    );
    press(&mut pump, KeyCode::Enter);
    assert_eq!(pump.held_keys().count(), 1, "typed while busy: held");

    press(&mut pump, KeyCode::Esc);
    while pump.replay_one().unwrap() {}
    assert_eq!(pump.held_keys().count(), 0);
    assert!(
        pump.app.analysis_modal.computing.is_none(),
        "the held Enter did not start it again"
    );
    assert_eq!(pump.app.analysis_modal.selected_tool, None);
}

/// Going home clears the *load's* busy state, and leaves `task_generation` alone —
/// that counter also gates analysis and export results, which keep running. The
/// loading screen has nothing to type ahead into, so keys typed there are dropped
/// rather than held (a held stray key used to queue `q` behind it).
#[test]
fn test_entering_home_clears_load_state_but_not_task_generation() {
    common::ensure_sample_data();
    let path = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), common::test_runtime());
    let mut pump = EventPump::new(app, tx, rx);
    pump.send(AppEvent::Open(vec![path], OpenOptions::default()))
        .unwrap();
    pump.drain().unwrap();
    assert!(pump.app.is_busy(), "a load in flight should be busy");
    for code in [KeyCode::Char('j'), KeyCode::Enter] {
        pump.terminal_key(KeyEvent::new(code, KeyModifiers::NONE))
            .unwrap();
    }
    assert_eq!(
        pump.held_keys().count(),
        0,
        "the loading screen holds nothing: stray keys are dropped"
    );

    let generation_before = pump.app.task_generation();
    pump.terminal_key(KeyEvent::new(KeyCode::Char('o'), KeyModifiers::CONTROL))
        .unwrap();

    assert_eq!(pump.app.input_mode, InputMode::Home);
    assert!(
        !pump.app.is_busy(),
        "abandoning should clear the load's busy flag"
    );
    assert_eq!(
        pump.app.task_generation(),
        generation_before,
        "going home must not cancel an in-flight export or analysis"
    );
}

/// Opening a remote URL raises a "Continue with download?" confirmation. Declining it
/// used to quit datui, and Ctrl+O was swallowed while it was up, which made a remote
/// open the one thing in the app you could not back out of.
#[cfg(feature = "http")]
fn app_awaiting_download_confirmation() -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    // Refused immediately, so the size probe does not sit on its timeout.
    let url = PathBuf::from("http://127.0.0.1:1/data.csv");
    let mut next = app.event(&AppEvent::Open(vec![url], OpenOptions::default()));
    while let Some(ev) = next {
        if matches!(ev, AppEvent::Crash(_)) {
            break;
        }
        next = app.event(&ev);
    }
    // The size probe runs on a background thread now, so the modal arrives by event
    // rather than before the open call returns.
    //
    // The budget is deliberately far longer than the probe should ever need. The
    // first HTTP agent built in a process loads the platform certificate store,
    // which is slow on a cold Windows runner, and a second was not enough: both
    // tests using this helper failed there on the v0.3.2 release commit, the first
    // time Windows had run them. What is being asserted is that datui asks before
    // downloading, not that it asks within any particular time.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(30);
    while !app.awaiting_download_confirmation() && std::time::Instant::now() < deadline {
        while let Ok(ev) = rx.try_recv() {
            if let Some(follow_up) = app.event(&ev) {
                app.event(&follow_up);
            }
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    (app, rx)
}

#[cfg(feature = "http")]
#[test]
fn test_declining_a_download_goes_home_instead_of_quitting() {
    let (mut app, _rx) = app_awaiting_download_confirmation();
    assert!(
        app.awaiting_download_confirmation(),
        "opening a remote URL should ask before downloading"
    );

    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));

    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "declining a download must not quit datui"
    );
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "declining a download should leave the user at home"
    );
    assert!(!app.awaiting_download_confirmation());
}

#[cfg(feature = "http")]
#[test]
fn test_ctrl_o_escapes_the_download_confirmation() {
    let (mut app, _rx) = app_awaiting_download_confirmation();
    assert!(app.awaiting_download_confirmation());

    app.event(&ctrl_o());

    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "Ctrl+O should work while the download confirmation is up"
    );
    assert!(
        !app.awaiting_download_confirmation(),
        "leaving should clear the pending download, not leave it armed"
    );
}

/// Every `DataTableState` must get its own `len_generation`. They used to all start at
/// zero, so an exact row count still running for the dataset you just closed matched
/// the one you just opened and set its row count to the wrong number.
#[test]
fn test_len_generations_are_unique_across_datasets() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );
    let first = app
        .data_table_state
        .as_ref()
        .expect("first dataset should load")
        .len_generation();

    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/sales.parquet")],
        OpenOptions::default(),
    );
    let second = app
        .data_table_state
        .as_ref()
        .expect("second dataset should load")
        .len_generation();

    assert_ne!(
        first, second,
        "two datasets must not share a row-count generation"
    );
}

/// q pops the context: a dataset opened from the home screen returns there,
/// one launched straight from the command line quits as it always has. Q is
/// unconditional, and the control bar says which meaning q carries.
#[test]
fn q_pops_to_home_only_when_home_is_in_the_stack() {
    // Launched straight onto a file: q quits.
    let (mut app, _rx, _tx) = open_query_filter_fixture("q_direct.csv");
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('q'),
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(out, Some(AppEvent::Exit)),
        "a direct launch keeps q as quit"
    );
    let area = Rect::new(0, 0, 110, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    assert!(
        rendered_text(&buf).contains(" Quit"),
        "the bar says q quits here"
    );

    // Opened from the home screen: q returns there.
    let path = common::fixture_dir().join("q_from_home.csv");
    let (mut app, rx, _tx) = open_query_filter_fixture_at(&path, datui::AppConfig::default());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    // As `home_open_path` does before it emits the `Open`.
    app.input_mode = InputMode::Normal;

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar = rendered_text(&buf);
    assert!(
        bar.contains("q  Home"),
        "the bar says q goes home here: {bar:?}"
    );

    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('q'),
        KeyModifiers::NONE,
    )));
    assert!(out.is_none(), "q does not quit with home in the stack");
    assert_eq!(app.input_mode, InputMode::Home, "q pops to home");

    // Q stays unconditional, from the same stack.
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    app.input_mode = InputMode::Normal;
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('Q'),
        KeyModifiers::SHIFT,
    )));
    assert!(matches!(out, Some(AppEvent::Exit)), "Q always quits");
}

/// The Columns list is workable with keys alone from the moment it opens:
/// the render draws the cursor on the first row, so the first Space must sort
/// it. The state used to hold no selection while the rail showed one, and
/// Space, L and v silently did nothing until an arrow press.
#[test]
fn the_sort_list_cursor_is_real_on_open() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_cursor_open.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab);
    assert_eq!(
        app.sort_filter_modal.sort.table_state.selected(),
        Some(0),
        "the cursor the rail shows is the cursor the keys act on"
    );
    press(&mut app, KeyCode::Char(' '));
    let sorted: Vec<String> = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .filter(|c| c.sort_order.is_some())
        .map(|c| c.name.clone())
        .collect();
    assert_eq!(sorted, vec!["a".to_string()], "the first Space sorts");
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_sort_columns(),
        &["a".to_string()],
        "Enter applies the staged sort"
    );
}

/// PgUp/PgDn at home move a screenful, like the table, not a fixed ten rows.
#[test]
fn home_paging_moves_a_screenful() {
    let tmp = tempfile::TempDir::new().unwrap();
    for i in 0..80 {
        std::fs::write(tmp.path().join(format!("f{i:03}.csv")), b"a,b\n1,2\n").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.enter_home();

    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        app.home.visible().len() >= 80 && app.home.view_height > 0
    });

    let page = app.home.view_height;
    assert!(page > 10, "the fixture should give more than the old ten");
    let before = app.home.selected;
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::PageDown,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.selected,
        (before + page).min(app.home.visible().len() - 1),
        "PgDn moves what one screen holds"
    );
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::PageUp,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.selected, before, "PgUp comes back the same amount");
}

/// Browsing a directory of more than 64 subdirectories: nothing is looked into while the
/// listing is built, so no row is labelled from where it sits, and the rows the frame
/// draws are looked into after it — on a worker, a screenful at a time.
///
/// The bug this covers is #270: the first 64 directories of a 6,241-partition share read
/// `multi`, and every identical one after them read `dir`.
#[test]
fn test_a_big_listing_is_labelled_from_the_viewport_not_from_directory_order() {
    let tmp = tempfile::TempDir::new().unwrap();
    for i in 0..200 {
        let partition = tmp.path().join(format!("d{i:03}")).join("year=2024");
        std::fs::create_dir_all(&partition).unwrap();
        std::fs::write(partition.join("part.parquet"), b"").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.enter_home();

    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);

    // The listing lands first, and the frame that draws it says of every row only
    // that nothing has looked into it.
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        !app.home.visible().is_empty()
    });
    let unlooked_at = text_of(&buf);
    assert!(
        !unlooked_at.contains(" dir"),
        "no row should be called a plain directory before anything looked into one:\n\
         {unlooked_at}"
    );
    let unlooked_at_row = format!("d000/ {}", datui::glyphs::get().ellipsis);
    assert!(
        unlooked_at.contains(&unlooked_at_row),
        "an unlooked-at row should read `{unlooked_at_row}`:\n{unlooked_at}"
    );

    // Then the rows that frame drew are looked into, and say what they are. Asked of
    // the rows on screen rather than the selected one: the cursor starts on the row that
    // opens the whole directory, which is not one of the two hundred being looked into.
    pump_home(&mut app, &rx, area, &mut buf, |app| {
        app.home.visible().iter().any(|row| match row {
            datui::home::Row::Entry { entry, .. } => entry.kind == datui::discover::EntryKind::Hive,
            _ => false,
        })
    });
    let looked_at = text_of(&buf);
    assert!(
        looked_at.contains("hive"),
        "the rows on screen should have been looked into:\n{looked_at}"
    );

    // And the bottom of the listing still has not been, which is the point: the
    // budget follows the viewport rather than directory order.
    let kinds: Vec<datui::discover::EntryKind> = app
        .home
        .visible()
        .iter()
        .filter_map(|row| match row {
            datui::home::Row::Entry { entry, .. } => Some(entry.kind),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds.last(),
        Some(&datui::discover::EntryKind::Unknown),
        "the bottom of a two-hundred-row listing is nobody's viewport"
    );
}

/// Draw, ask for what the frame needs, take one answer, draw again — the shape of
/// `run()` around `terminal.draw`, so a state the real loop passes through for one
/// frame can be caught here too.
#[track_caller]
fn pump_home(
    app: &mut App,
    rx: &mpsc::Receiver<AppEvent>,
    area: Rect,
    buf: &mut Buffer,
    done: impl Fn(&App) -> bool,
) {
    for _ in ticks() {
        buf.reset();
        Widget::render(&mut *app, area, buf);
        app.frame_painted();
        app.request_what_the_frame_needs();
        if done(app) {
            return;
        }
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(500)) {
            let mut next = Some(ev);
            while let Some(ev) = next {
                next = app.event(&ev);
            }
        }
    }
}

/// Everything a buffer has drawn, as lines.
fn text_of(buf: &Buffer) -> String {
    (0..buf.area.height)
        .map(|y| {
            (0..buf.area.width)
                .filter_map(|x| buf.cell((x, y)).map(|c| c.symbol().to_string()))
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Modals render over the home screen, but home used to consume every key, so one
/// raised while the user was at home could not be dismissed: Esc went to home_escape,
/// which at the time quit when nothing was loaded. The only way past an error was to
/// leave datui.
#[test]
fn test_error_modal_over_home_is_dismissable() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    // A load chosen here fails.
    app.set_loading_phase("Scanning input", 10);
    app.event(&AppEvent::BackgroundFailed {
        generation: app.task_generation(),
        job: datui::Job::Load,
        message: "could not read the file".to_string(),
        panicked: false,
    });

    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "Esc should dismiss the modal, not quit out from under it"
    );

    // With the modal gone, Esc is home's again. An empty home has nowhere left to
    // back out to, so it does nothing; Ctrl+C is what quits.
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        !matches!(out, Some(AppEvent::Exit)),
        "Esc at the top of the home screen must not quit"
    );
    assert_eq!(app.input_mode, InputMode::Home);
    let out = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::CONTROL,
    )));
    assert!(
        matches!(out, Some(AppEvent::Exit)),
        "Ctrl+C quits from the home screen"
    );
}

/// Opening a second dataset from the home screen must not show the first one's rows
/// while the second is still loading. Between the keypress and the new dataset being
/// installed, the old table was still on screen — a page of one file's data under the
/// filename of another, for as long as the load took.
#[test]
fn test_opening_from_home_does_not_show_the_previous_dataset() {
    common::ensure_sample_data();
    let first = PathBuf::from("tests/sample-data/people.parquet");
    let second = PathBuf::from("tests/sample-data/large_dataset.parquet");

    let area = Rect::new(0, 0, 120, 50);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    tx.send(AppEvent::Open(vec![first.clone()], OpenOptions::default()))
        .unwrap();

    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        if app.data_table_state.is_some() && !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    assert!(
        rendered_text(&buf).contains("first_name"),
        "the first dataset should be on screen before we go home"
    );

    // Home, then open the second dataset the way a user does: through the path
    // prompt, so the real key path runs rather than a synthesised event.
    app.event(&ctrl_o());
    assert_eq!(app.input_mode, InputMode::Home);
    app.event(&key(KeyCode::Char('~')));
    for c in second.to_str().unwrap().chars() {
        app.event(&key(KeyCode::Char(c)));
    }
    if let Some(next) = app.event(&key(KeyCode::Enter)) {
        tx.send(next).unwrap();
    }
    assert_eq!(
        app.input_mode,
        InputMode::Normal,
        "opening from home should leave the home screen"
    );

    // Every frame from here until the second dataset is installed.
    let mut frames = 0usize;
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        frames += 1;
        let text = main_area_text(&buf, area);
        assert!(
            !text.contains("first_name") && !text.contains("job_title"),
            "frame {frames} showed the previous dataset while the next one was loading"
        );
        assert!(
            text.contains("large_dataset.parquet") || text.contains("dist_normal"),
            "frame {frames} named neither the dataset being loaded nor the one that arrived"
        );
        let needs = app
            .data_table_state
            .as_mut()
            .map(|s| {
                let n = s.needs_recollect;
                s.needs_recollect = false;
                n
            })
            .unwrap_or(false);
        if needs {
            app.spawn_async_collect("Loading buffer...");
        }
        if app.open_path() == Some(second.as_path()) && !app.is_busy() && !needs {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(
        app.open_path(),
        Some(second.as_path()),
        "the second dataset should have loaded"
    );
    assert!(
        frames > 1,
        "the load finished in one frame; nothing was tested"
    );
}

/// A file datui cannot read is hidden until Ctrl+A shows it, and says why on Enter.
#[test]
fn a_file_datui_cannot_read_is_hidden_until_shown() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("sales.csv"), "a\n1\n").unwrap();
    std::fs::write(dir.path().join("model.onnx"), "onnx").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    app.enter_home();
    app.event(&key(KeyCode::Char('~')));
    for c in dir.path().to_str().unwrap().chars() {
        app.event(&key(KeyCode::Char(c)));
    }
    app.event(&key(KeyCode::Enter));
    let ctrl_a = AppEvent::Key(KeyEvent::new(KeyCode::Char('a'), KeyModifiers::CONTROL));
    app.event(&ctrl_a);
    assert!(app.home.filter.is_empty(), "Ctrl+A is not typed");

    let row_of = |app: &App, name: &str| {
        app.home.visible().iter().position(
            |row| matches!(row, datui::home::Row::Entry { entry, .. } if entry.name == name),
        )
    };
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        if row_of(&app, "model.onnx").is_some() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    let model = row_of(&app, "model.onnx").expect("listed");
    app.home.selected = model;
    // Enter does nothing: the row is dimmed and its pane says why.
    app.home.status = None;
    assert!(app.event(&key(KeyCode::Enter)).is_none(), "nothing opened");
    assert_eq!(app.home.status, None);

    app.event(&ctrl_a);
    assert!(row_of(&app, "model.onnx").is_none());
    assert!(row_of(&app, "sales.csv").is_some());
}

/// A load chosen at home that fails is reported at home. It used to put the error over
/// the dataset open before, which is not where the user was when they chose.
#[test]
fn a_load_chosen_at_home_fails_at_home() {
    common::ensure_sample_data();
    let first = PathBuf::from("tests/sample-data/people.parquet");
    let dir = tempfile::tempdir().unwrap();
    let broken = dir.path().join("broken.parquet");
    std::fs::write(&broken, "not parquet").unwrap();

    let area = Rect::new(0, 0, 120, 40);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    // Its own recents: in the shared store, fifty opens elsewhere push these out.
    let cache = datui::CacheManager::with_dir(dir.path().join("cache"));
    app.use_cache(cache.clone());
    tx.send(AppEvent::Open(vec![first], OpenOptions::default()))
        .unwrap();
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        if app.data_table_state.is_some() && !app.is_busy() {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(app.data_table_state.is_some());

    app.event(&ctrl_o());
    let type_at_prompt = |app: &mut App, path: &std::path::Path| {
        app.event(&key(KeyCode::Char('~')));
        for c in path.to_str().unwrap().chars() {
            app.event(&key(KeyCode::Char(c)));
        }
        if let Some(next) = app.event(&key(KeyCode::Enter)) {
            tx.send(next).unwrap();
        }
    };
    type_at_prompt(&mut app, &broken);
    for _tick in ticks() {
        drain_like_main_loop(&mut app, &tx, &rx);
        if !app.is_busy() && app.input_mode == InputMode::Home {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert_eq!(app.input_mode, InputMode::Home);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    assert!(rendered_text(&buf).contains("Failed to load"));

    // Nor is it a recent: recorded when a dataset installs, not when it is asked for.
    // The one that did load is, and recording is off-thread, so that is waited for.
    let recorded = |path: &std::path::Path| {
        let path = datui::canonical::canonicalize(path).unwrap();
        cache.load_recents().contains(&path)
    };
    for _ in ticks() {
        if recorded(std::path::Path::new("tests/sample-data/people.parquet")) {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert!(recorded(std::path::Path::new(
        "tests/sample-data/people.parquet"
    )));
    std::thread::sleep(std::time::Duration::from_millis(100));
    assert!(!recorded(&broken), "a file that failed is not a recent");

    // Dismissed, the reason stays beside the prompt.
    app.event(&key(KeyCode::Enter));
    assert_eq!(app.input_mode, InputMode::Home);
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("broken.parquet"))
    );

    // A file no reader takes is refused before anything is read.
    let model = dir.path().join("model.onnx");
    std::fs::write(&model, "onnx").unwrap();
    app.home.status = None;
    while rx.try_recv().is_ok() {}
    type_at_prompt(&mut app, &model);
    assert!(rx.try_recv().is_err(), "nothing was opened");
    // A typed path has no row to dim, so the line says it.
    assert_eq!(app.home.status.as_deref(), Some(datui::discover::NO_READER));
}

/// The one-file schema types partition columns the way a full scan does.
#[test]
fn test_hive_partition_types_match_full_scan() {
    use datui::widgets::datatable::DataTableState;

    let dir = tempfile::tempdir().unwrap();
    for sub in [
        "region=eu/year=2020/day=2020-01-01",
        "region=us/year=2021/day=2021-06-30",
    ] {
        let d = dir.path().join(sub);
        std::fs::create_dir_all(&d).unwrap();
        let mut df = df!("v" => [1i64, 2]).unwrap();
        ParquetWriter::new(File::create(d.join("data.parquet")).unwrap())
            .finish(&mut df)
            .unwrap();
    }

    let (fast, parts) = DataTableState::schema_from_one_hive_parquet(dir.path()).unwrap();
    assert_eq!(parts, ["region", "year", "day"]);
    let mut full = LazyFrame::scan_parquet(
        PlRefPath::try_from_path(dir.path()).unwrap(),
        ScanArgsParquet {
            hive_options: polars::io::HiveOptions::new_enabled(),
            ..Default::default()
        },
    )
    .unwrap();
    let full = full.collect_schema().unwrap();
    for name in parts {
        assert_eq!(fast.get(&name), full.get(&name), "{name}");
    }
    assert_eq!(fast.get("year"), Some(&DataType::Int64));

    let lf = DataTableState::scan_parquet_hive_with_schema(dir.path(), fast).unwrap();
    let df = lf.filter(col("year").gt(lit(2020))).collect().unwrap();
    assert_eq!(df.height(), 2);
}

/// Esc at a bucket's top goes back to the home listing, not to a directory named `gs:`.
#[test]
fn test_escape_from_a_bucket_returns_home() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(PathBuf::from("gs://bucket"));

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A remote location shows that it is being listed, not "No datasets here.", until its
/// listing arrives.
#[test]
fn test_remote_listing_shows_progress_until_it_arrives() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let dir = PathBuf::from("gs://bucket/demo");
    app.home.browsing = Some(dir.clone());

    let area = Rect::new(0, 0, 100, 20);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    };

    let waiting = screen(&mut app);
    assert!(waiting.contains("Listing gs://bucket/demo"), "{waiting}");
    assert!(!waiting.contains("No datasets here."), "{waiting}");

    app.home.probe_ready(dir, Vec::new());
    let done = screen(&mut app);
    assert!(!done.contains("Listing gs://bucket/demo"), "{done}");
    assert_eq!(app.home.waiting_since, None);
}

/// Esc retraces a browse back to the listing and stops there, however deep the user
/// went — it does not climb above the directory the browse began at.
#[test]
fn test_escape_stops_at_where_browsing_began() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let tmp = tempfile::tempdir().unwrap();
    let start = tmp.path().join("a");
    let deeper = start.join("b");
    std::fs::create_dir_all(&deeper).unwrap();
    let key = |app: &mut App, code| {
        app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };

    app.home.path_input_active = true;
    app.home.path_input = start.display().to_string();
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.home.browsing.as_deref(), Some(start.as_path()));

    // As if Enter had descended into `b`.
    app.home.browsing = Some(deeper);
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing.as_deref(), Some(start.as_path()));
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing, None);
    key(&mut app, KeyCode::Esc);
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.input_mode, InputMode::Home);
}

/// Backspace still climbs above where a browse began, and Esc from there goes back to
/// the listing rather than on up the tree.
#[test]
fn test_escape_after_backspace_above_the_start_returns_home() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    let tmp = tempfile::tempdir().unwrap();
    let start = tmp.path().join("a");
    std::fs::create_dir_all(&start).unwrap();
    app.home.browsing = Some(start.clone());
    app.home.browse_start = Some(start);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Backspace,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing.as_deref(), Some(tmp.path()));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert_eq!(app.home.browsing, None);
}

/// Feed background results back into the app until it is no longer busy.
#[track_caller]
fn pump_until_idle(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>) {
    pump_until(app, rx, tx, |app| !app.is_busy());
}

/// A 100-row table: `a` 0..100, `c` = a % 3, `name` "alpha_N" for even and "beta_N" for odd `a`.
fn open_query_filter_fixture(
    name: &str,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    open_query_filter_fixture_with(name, datui::AppConfig::default())
}

fn open_query_filter_fixture_with(
    name: &str,
    config: datui::AppConfig,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    open_query_filter_fixture_at(&common::fixture_dir().join(name), config)
}

/// The same table, written at `csv_path`.
fn open_query_filter_fixture_at(
    csv_path: &Path,
    config: datui::AppConfig,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let mut df = df!(
        "a" => (0..100i64).collect::<Vec<_>>(),
        "c" => (0..100i64).map(|i| i % 3).collect::<Vec<_>>(),
        "name" => (0..100i64)
            .map(|i| if i % 2 == 0 { format!("alpha_{i}") } else { format!("beta_{i}") })
            .collect::<Vec<_>>(),
    )
    .unwrap();
    let mut file = File::create(csv_path).unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    let (tx, rx) = mpsc::channel();
    let theme = datui::Theme::from_config(&config.theme).unwrap();
    let mut app = App::new_with_config(tx.clone(), common::test_runtime(), theme, config);
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![csv_path.to_path_buf()],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.data_table_state.as_ref().unwrap().num_rows(), 100);
    (app, rx, tx)
}

fn current_rows(app: &App) -> usize {
    let state = app.data_table_state.as_ref().unwrap();
    state.lf().clone().collect().unwrap().height()
}

fn filter_stmt(
    column: &str,
    operator: datui::filter_modal::FilterOperator,
    value: &str,
) -> datui::filter_modal::FilterStatement {
    datui::filter_modal::FilterStatement {
        column: column.to_string(),
        operator,
        value: value.to_string(),
        logical_op: datui::filter_modal::LogicalOperator::And,
    }
}

/// A sidebar filter applies on top of the active DSL query rather than replacing it, and
/// clearing the filters returns to the query result. Reset still clears everything.
#[test]
fn test_sidebar_filter_applies_on_top_of_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("query_then_filter.csv");

    app.event(&AppEvent::Search("select where a < 50".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "1",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    // a in 0..50 with a % 3 == 1: 1, 4, ..., 49
    assert_eq!(
        current_rows(&app),
        17,
        "filter must apply to the query result"
    );
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select where a < 50");
    assert_eq!(state.get_filters().len(), 1);

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        current_rows(&app),
        50,
        "clearing filters returns to the query result"
    );

    app.event(&AppEvent::Reset);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 100);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_query()
            .is_empty()
    );
}

/// The q-style additions run through the app: `distinct`, the word operators and a
/// computed group key.
#[test]
fn test_q_style_distinct_like_mod_and_xbar() {
    let (mut app, rx, tx) = open_query_filter_fixture("query_q_style_additions.csv");

    app.event(&AppEvent::Search("select distinct c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().error().is_none());
    assert_eq!(current_rows(&app), 3);

    // alpha_0 .. alpha_8, then 0 = (a mod 4) keeps 0, 4 and 8.
    app.event(&AppEvent::Search(
        "select where name like \"alpha_?\", 0 = a mod 4".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 3);

    app.event(&AppEvent::Search(
        "select n: count a by b: 10 xbar a".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("b").unwrap().get(9).unwrap(), AnyValue::Int64(90));
    assert_eq!(
        df.column("n").unwrap().get(9).unwrap(),
        AnyValue::UInt32(10)
    );
}

/// Same for a fuzzy search: sort and filter stack on it, and clearing them keeps it.
#[test]
fn test_sidebar_filter_and_sort_keep_fuzzy_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("fuzzy_then_filter.csv");

    app.event(&AppEvent::FuzzySearch("alpha".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "a",
        FilterOperator::Lt,
        "20",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 10);

    app.event(&AppEvent::Sort(vec!["a".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(18));

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 50);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_fuzzy_query(),
        "alpha"
    );
}

/// And for SQL.
#[test]
fn test_sidebar_filter_keeps_sql_query() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("sql_then_filter.csv");

    app.event(&AppEvent::SqlSearch(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    // a in 0..30 with a % 3 == 0: 0, 3, ..., 27
    assert_eq!(current_rows(&app), 10);

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);
}

/// SQL runs against the data as loaded, like the DSL and fuzzy queries: a sidebar filter
/// that was active when the SQL ran is not baked into its result, so clearing the
/// filters afterwards shows the SQL result over the whole table.
#[test]
fn test_sql_runs_against_the_loaded_data_not_the_filtered_view() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("filter_then_sql.csv");

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 34);

    app.event(&AppEvent::SqlSearch(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30, "the SQL replaces the filter");
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_filters()
            .is_empty()
    );

    app.event(&AppEvent::Filter(vec![]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 30);
}

/// Opens an inline CSV with the given options and settles the load.
fn open_csv_with(
    name: &str,
    contents: &str,
    options: OpenOptions,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    open_csv_at(&common::fixture_dir().join(name), contents, options)
}

/// The same, written at `csv_path`.
fn open_csv_at(
    csv_path: &Path,
    contents: &str,
    options: OpenOptions,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    std::fs::write(csv_path, contents).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path.to_path_buf()], options);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.is_some());
    (app, rx, tx)
}

/// Footer rows dropped with `skip_tail_rows` stay dropped after a sidebar sort: the
/// load-time trimming is part of the pipeline's root, not just of the first view.
#[test]
fn test_skip_tail_rows_survives_a_sidebar_sort() {
    let mut csv = String::from("a,b\n");
    for i in 0..100 {
        csv.push_str(&format!("{i},{}\n", i * 2));
    }
    // Two summary rows at the end, the kind `skip_tail_rows` exists for.
    csv.push_str("9999,-1\n9998,-2\n");
    let options = OpenOptions {
        skip_tail_rows: Some(2),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open_csv_with("skip_tail_then_sort.csv", &csv, options);
    assert_eq!(current_rows(&app), 100);

    app.event(&AppEvent::Sort(vec!["b".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    assert_eq!(df.height(), 100, "the footer rows must not come back");
    assert_eq!(
        df.column("b").unwrap().get(0).unwrap(),
        AnyValue::Int64(198)
    );
}

/// Numbers parsed out of padded strings with `parse_strings` are still numbers when a
/// sidebar filter compares them.
#[test]
fn test_parse_strings_survives_a_sidebar_filter() {
    use datui::ParseStringsTarget;
    use datui::filter_modal::FilterOperator;
    let mut csv = String::from("id,amount\n");
    for i in 0..100 {
        csv.push_str(&format!("{i},\" {} \"\n", i * 3));
    }
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let (mut app, rx, tx) = open_csv_with("parse_strings_then_filter.csv", &csv, options);
    {
        let state = app.data_table_state.as_ref().unwrap();
        assert!(
            state.schema().get("amount").unwrap().is_integer(),
            "parse_strings should have made amount numeric"
        );
    }

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "amount",
        FilterOperator::Gt,
        "150",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    // amount = 3 * id > 150 for id 51..100
    assert_eq!(current_rows(&app), 49);
}

/// ISO 8601 timestamps with `Z`, fractional seconds or an offset, as web APIs write them.
/// `mixed` has an offset on one value and none on the other.
const ISO_TIMESTAMPS_CSV: &str = "\
id,z,frac,offset,space,minutes,mixed
1,2013-01-01T10:00:00Z,2026-09-30T13:27:00.220Z,2026-09-30T13:27:00-05:00,2026-09-30 13:27:00+00,2026-09-30T13:27Z,2013-01-01T10:00:00Z
2,2013-01-01T11:00:00Z,2026-09-30T13:28:00.5Z,2026-09-30T13:27:00+00:00,2026-09-30 13:28:00+00,2026-09-30T13:28Z,2013-01-01T11:00:00
";

/// The first row's value of `name`, in microseconds since the epoch.
fn first_micros(app: &App, name: &str) -> i64 {
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    df.column(name)
        .unwrap()
        .cast(&DataType::Int64)
        .unwrap()
        .get(0)
        .unwrap()
        .extract::<i64>()
        .unwrap()
}

fn assert_iso_timestamps_typed(app: &App) {
    let utc = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
    let schema = &app.data_table_state.as_ref().unwrap().schema();
    for name in ["z", "frac", "offset", "space", "minutes"] {
        assert_eq!(schema.get(name), Some(&utc), "{name}");
    }
    assert_eq!(schema.get("mixed"), Some(&DataType::String));
    assert_eq!(first_micros(app, "z"), 1_357_034_400_000_000);
    assert_eq!(first_micros(app, "frac"), 1_790_774_820_220_000);
    // -05:00 is five hours behind UTC.
    assert_eq!(first_micros(app, "offset"), 1_790_792_820_000_000);
    assert_eq!(first_micros(app, "minutes"), 1_790_774_820_000_000);
}

/// Timestamps with `Z` or an offset load as UTC Datetime, with the string typing on
/// (the default) and with Polars' own date inference.
#[test]
fn test_iso_timestamps_with_offsets_load_as_utc_datetime() {
    use datui::ParseStringsTarget;
    for parse_strings in [Some(ParseStringsTarget::All), None] {
        let options = OpenOptions {
            parse_strings: parse_strings.clone(),
            ..OpenOptions::default()
        };
        let (app, _rx, _tx) = open_csv_with("iso_timestamps.csv", ISO_TIMESTAMPS_CSV, options);
        assert_iso_timestamps_typed(&app);
    }
}

/// `parse_dates = false` keeps them text, string typing or not.
#[test]
fn test_iso_timestamps_stay_text_without_parse_dates() {
    use datui::ParseStringsTarget;
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        parse_dates: false,
        ..OpenOptions::default()
    };
    let (app, _rx, _tx) = open_csv_with("iso_timestamps_as_text.csv", ISO_TIMESTAMPS_CSV, options);
    let schema = &app.data_table_state.as_ref().unwrap().schema();
    assert_eq!(schema.get("z"), Some(&DataType::String));
    assert_eq!(schema.get("id"), Some(&DataType::Int64));
}

/// NDJSON strings get the same timestamps; a string of digits stays a string.
#[test]
fn test_iso_timestamps_in_ndjson_load_as_utc_datetime() {
    let mut jsonl = String::new();
    for line in ISO_TIMESTAMPS_CSV.lines().skip(1) {
        let v: Vec<&str> = line.split(',').collect();
        jsonl.push_str(&format!(
            "{{\"id\":{},\"zip\":\"0{}\",\"z\":\"{}\",\"frac\":\"{}\",\"offset\":\"{}\",\"space\":\"{}\",\"minutes\":\"{}\",\"mixed\":\"{}\"}}\n",
            v[0], v[0], v[1], v[2], v[3], v[4], v[5], v[6]
        ));
    }
    let (app, _rx, _tx) = open_csv_with("iso_timestamps.jsonl", &jsonl, OpenOptions::default());
    assert_iso_timestamps_typed(&app);
    let schema = &app.data_table_state.as_ref().unwrap().schema();
    assert_eq!(schema.get("zip"), Some(&DataType::String));
}

/// A column that gives seconds on some values and not others stays text: a format
/// read from the first value must not match the front of a longer one and drop the
/// rest.
#[test]
fn test_datetimes_of_mixed_precision_stay_text() {
    use datui::ParseStringsTarget;
    let csv = "t\n2024-01-01 10:00\n2024-01-02 11:30:15\n";
    let options = OpenOptions {
        parse_strings: Some(ParseStringsTarget::All),
        ..OpenOptions::default()
    };
    let (app, _rx, _tx) = open_csv_with("datetimes_mixed_precision.csv", csv, options);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.schema().get("t"), Some(&DataType::String));
    let df = state.lf().clone().collect().unwrap();
    let t = df.column("t").unwrap().str().unwrap();
    assert_eq!(t.get(1), Some("2024-01-02 11:30:15"));
}

/// SQL after a pivot sees the pivoted columns: the reshape is the root the query runs
/// against, so `SELECT` of a pivoted column works and the reshape stays in the view.
#[test]
fn test_sql_after_pivot_sees_the_pivoted_columns() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    let (mut app, rx, tx) = open_csv_with("pivot_then_sql.csv", &csv, OpenOptions::default());

    app.event(&AppEvent::Pivot(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::First,
        sort_columns: None,
    }));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 10);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema()
            .contains("k1")
    );

    app.event(&AppEvent::SqlSearch(
        "SELECT id, k2 FROM df WHERE k1 > 4".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 5, "ids 5..9");
    let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
    assert_eq!(names, vec!["id", "k2"]);
}

/// While drilled into a group, a sidebar filter or sort applies within the group and
/// leaves the drill-down in place; drilling back up restores the grouped view.
#[test]
fn test_sidebar_filter_and_sort_stay_inside_a_drill_down() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_down_filter.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 3, "one row per group");

    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_down_into_group(0)
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert_eq!(current_rows(&app), 34, "c == 0: 0, 3, ..., 99");

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "a",
        FilterOperator::Lt,
        "30",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    assert!(
        state.is_drilled_down(),
        "the filter must not undo the drill-down"
    );
    assert_eq!(current_rows(&app), 10);

    app.event(&AppEvent::Sort(vec!["a".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(27));

    app.data_table_state.as_mut().unwrap().drill_up().unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert!(
        state.get_filters().is_empty(),
        "the group's filter stays with the group"
    );
    assert_eq!(current_rows(&app), 3);
}

/// A fuzzy search after a DSL query that renamed columns works on the data as loaded
/// and installs that schema, so a sidebar sort afterwards finds its columns.
#[test]
fn test_fuzzy_after_an_aliasing_query_then_sort_has_no_error() {
    let (mut app, rx, tx) = open_query_filter_fixture("alias_then_fuzzy.csv");

    app.event(&AppEvent::Search("select a, label: name".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema()
            .iter_names()
            .map(|s| s.to_string())
            .collect::<Vec<_>>(),
        vec!["a", "label"]
    );

    app.event(&AppEvent::FuzzySearch("alpha".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    assert_eq!(current_rows(&app), 50);
    assert!(state.schema().contains("name") && state.schema().contains("c"));
    assert_eq!(state.headers(), vec!["a", "c", "name"]);

    app.event(&AppEvent::Sort(vec!["a".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 50);
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(98));
}

/// A DSL query after a pivot shows the loaded columns again, so SQL afterwards must run
/// against the loaded data, not against a pivot the user no longer sees.
#[test]
fn test_query_after_pivot_drops_the_reshape_for_sql() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!("{id},k1,{id}\n{id},k2,{}\n", id * 10));
    }
    let (mut app, rx, tx) = open_csv_with("pivot_query_sql.csv", &csv, OpenOptions::default());

    app.event(&AppEvent::Pivot(PivotSpec {
        index: vec!["id".to_string()],
        pivot_column: "key".to_string(),
        value_column: "val".to_string(),
        aggregation: PivotAggregation::First,
        sort_columns: None,
    }));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .schema()
            .contains("k1")
    );

    app.event(&AppEvent::Search("select id, key".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 20);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .last_pivot_spec()
            .is_none()
    );

    app.event(&AppEvent::SqlSearch("SELECT * FROM df".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    let names: Vec<String> = state.schema().iter_names().map(|s| s.to_string()).collect();
    assert_eq!(names, vec!["id", "key", "val"], "the unpivoted columns");
    assert_eq!(current_rows(&app), 20);
}

/// Drilling into a group swaps the applied filters and sort for the group's; the Sort &
/// Filter sidebar must follow, or Apply would re-send the grouped view's filter against
/// a List column. Drilling back up brings the grouped view's settings back.
#[test]
fn test_drill_down_resyncs_the_sort_filter_sidebar() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sidebar.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let statement = filter_stmt("c", FilterOperator::Gt, "0");
    app.event(&AppEvent::Filter(vec![statement.clone()]));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Sort(vec!["c".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 2, "groups c = 1 and c = 2");
    // What Apply would have left in the sidebar.
    app.sort_filter_modal.filter.statements = vec![statement];
    app.sort_filter_modal.sort.columns = vec![datui::sort_modal::SortColumn {
        name: "c".to_string(),
        sort_order: Some(0),
        sort_descending: false,
        display_order: 0,
        is_locked: false,
        is_to_be_locked: false,
        is_visible: true,
    }];

    // Enter on the highlighted group row drills in.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert!(
        app.sort_filter_modal.filter.statements.is_empty(),
        "no filter applies inside the group yet"
    );
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.sort_order.is_none())
    );
    assert_eq!(
        app.sort_filter_modal.filter.available_columns,
        app.data_table_state.as_ref().unwrap().headers()
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
    let statements = &app.sort_filter_modal.filter.statements;
    assert_eq!(statements.len(), 1);
    assert_eq!(statements[0].column, "c");
    assert_eq!(statements[0].value, "0");
    let sorted: Vec<&str> = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .filter(|c| c.sort_order.is_some())
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(sorted, vec!["c"]);
    let c = app
        .sort_filter_modal
        .sort
        .columns
        .iter()
        .find(|c| c.name == "c")
        .unwrap();
    assert!(c.sort_descending, "the applied direction arrives staged");
}

/// A template saved while drilled into a group describes the grouped view, which is what
/// it will reproduce: the getters return the grouped view's filters and sort, while the
/// view getters describe the frame on screen.
#[test]
fn test_template_getters_describe_the_grouped_view_while_drilled() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("drill_template_getters.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Gt,
        "0",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Sort(vec!["c".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);

    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(0).unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(state.get_filters().len(), 1);
    assert_eq!(state.get_sort_columns(), ["c".to_string()]);
    assert!(!state.get_sort_ascending());
    assert!(state.view_filters().is_empty());
    assert!(state.view_sort_columns().is_empty());

    state.drill_up().unwrap();
    assert_eq!(state.get_filters().len(), 1);
    assert_eq!(state.view_filters().len(), 1);
    assert_eq!(state.get_sort_columns(), ["c".to_string()]);
}

/// One key press, with whatever it asks for sent on as the event loop would.
fn press_and_send(app: &mut App, tx: &mpsc::Sender<AppEvent>, code: KeyCode) {
    if let Some(next) = press(app, code) {
        tx.send(next).unwrap();
    }
}

/// One column of the rows on screen, as text.
fn on_screen(app: &App, column: &str) -> Vec<String> {
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .copy_view_df()
        .unwrap();
    let series = df.column(column).unwrap().as_materialized_series().clone();
    series
        .iter()
        .map(|v| match v {
            AnyValue::Null => "null".to_string(),
            v => v.str_value().into_owned(),
        })
        .collect()
}

/// Esc from a group taller than the screen draws the grouped rows again, not the
/// group's: the group's buffer covered the view, so it used to be kept. The cursor
/// comes back to the group drilled into, and the key column is frozen again.
#[test]
fn test_esc_from_a_drill_down_shows_the_grouped_rows() {
    let mut csv = String::from("a,c\n");
    for i in 0..1200 {
        csv.push_str(&format!("{i},{}\n", i % 3));
    }
    let (mut app, rx, tx) = open_csv_with("drill_esc_rows.csv", &csv, OpenOptions::default());
    let area = Rect::new(0, 0, 100, 30);
    painted(&mut app, &rx, &tx, area);

    app.event(&AppEvent::Search("select a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "c"), ["0", "1", "2"]);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        1
    );

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
    assert!(on_screen(&app, "c").iter().all(|c| c == "2"));
    assert_eq!(on_screen(&app, "a")[..2], ["2", "5"]);

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(on_screen(&app, "c"), ["0", "1", "2"]);
    assert_eq!(
        state.table_state.selected(),
        Some(2),
        "on the group drilled into"
    );
    assert_eq!(state.locked_columns_count(), 1, "the key stays frozen");
}

/// Drilling from a grouped view taller than the screen into a small group draws the
/// group, not the grouped rows the buffer held.
#[test]
fn test_drill_into_a_small_group_shows_its_rows() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_small_group.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(&AppEvent::Search("select name by a".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "a").len(), 27, "a screen of the 100 groups");

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "name"), ["beta_1"]);
}

/// Enter on an aggregated `by` result, which holds no rows of its groups, drills into
/// the source rows of the group, after the query's `where`; Esc brings the aggregate
/// back.
#[test]
fn test_enter_drills_from_an_aggregated_result() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_aggregate.csv");
    let area = Rect::new(0, 0, 100, 30);

    app.event(&AppEvent::Search(
        "select n: count a, total: sum a by c where a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "n"), ["10", "10", "10"]);

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["1".to_string()])
    );
    assert_eq!(
        state.headers(),
        ["c", "a", "name"],
        "the source's columns, key first"
    );
    assert_eq!(
        on_screen(&app, "a"),
        ["1", "4", "7", "10", "13", "16", "19", "22", "25", "28"],
        "c = 1 and a < 30"
    );

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(on_screen(&app, "total"), ["135", "145", "155"]);
    assert_eq!(state.table_state.selected(), Some(1));
}

/// A computed, renamed key drills by the expression that computed it, and a null key
/// drills into the rows whose key is null.
#[test]
fn test_drill_from_an_aggregate_by_a_computed_key_and_a_null_key() {
    let csv = "k,v\nx,1\n,2\ny,3\n,4\nx,5\n,6\n";
    let (mut app, rx, tx) = open_csv_with("drill_null_key.csv", csv, OpenOptions::default());

    app.event(&AppEvent::Search("select n: count v by key: k".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    // Nulls sort last.
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(2).unwrap();
    assert_eq!(state.headers(), ["k", "v"]);
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.column("k").unwrap().null_count(), 3);
    assert_eq!(df.column("v").unwrap().i64().unwrap().sum(), Some(12));
    state.drill_up().unwrap();

    app.event(&AppEvent::Search(
        "select n: count v by big: v > 3".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(1).unwrap();
    assert_eq!(
        state
            .drilled_group_key()
            .map(|(columns, _)| columns.to_vec()),
        Some(vec!["big".to_string()])
    );
    let df = state.lf().clone().collect().unwrap();
    let v: Vec<i64> = df
        .column("v")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(v, [4, 5, 6], "big = true");
}

/// Every group of an aggregate drills into as many rows as it counted, whatever the
/// key's type: floats with NaN and signed zeros, dates, zoned datetimes, categoricals,
/// and nulls of each.
#[test]
fn test_drill_from_an_aggregate_by_typed_keys() {
    let dir = common::fixture_dir().join("drill_typed_keys");
    let tz = TimeZone::opt_try_new(Some("America/New_York")).unwrap();
    let df = df!(
        "f" => [Some(1.5), Some(1.5), Some(f64::NAN), Some(f64::NAN), None, Some(-0.0), Some(0.0), Some(0.1 + 0.2)],
        "d" => [Some(19000), Some(19000), Some(19001), None, None, Some(19001), Some(19002), Some(19000)],
        "t" => [Some(1_700_000_000_000_000i64), Some(1_700_000_000_000_000), None, Some(1_700_000_000_000_001), None, Some(1), Some(1), Some(1)],
        "s" => [Some("a"), Some("b"), Some("a"), None, Some("b"), Some("a"), None, Some("c")],
        "v" => [1i64, 2, 3, 4, 5, 6, 7, 8],
    )
    .unwrap()
    .lazy()
    .with_columns([
        col("d").cast(DataType::Date),
        col("t").cast(DataType::Datetime(TimeUnit::Microseconds, tz)),
        col("s").cast(DataType::from_categories(Categories::global())),
    ])
    .collect()
    .unwrap();
    write_parquet(&dir, "", df);
    let path = dir.join("data.parquet");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let schema = app.data_table_state.as_ref().unwrap().schema().clone();
    assert!(matches!(schema.get("s"), Some(DataType::Categorical(..))));
    assert!(matches!(
        schema.get("t"),
        Some(DataType::Datetime(_, Some(_)))
    ));

    for key in ["f", "d", "t", "s", "f, s", "day: d, late: t > 5"] {
        app.event(&AppEvent::Search(format!("select n: count v by {key}")));
        pump_until_idle(&mut app, &rx, &tx);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.error().is_none(), "{key}: {:?}", state.error());
        let counts: Vec<u32> = state
            .lf()
            .clone()
            .collect()
            .unwrap()
            .column("n")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        for (group, counted) in counts.into_iter().enumerate() {
            state.drill_down_into_group(group).unwrap();
            let rows = state.lf().clone().collect().unwrap().height();
            assert_eq!(rows as u32, counted, "by {key}, group {group}");
            state.drill_up().unwrap();
        }
    }
}

/// Enter on an aggregate reads the row's keys from the rows on screen, so the drill
/// happens at once without computing the aggregate again. With a key column hidden
/// the row is read off the UI thread, and the drill lands when it comes back.
#[test]
fn test_enter_on_an_aggregate_drills_from_the_buffer_or_reads_the_row() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_from_buffer.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(&AppEvent::Search(
        "select n: count a, total: sum a by c".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);

    press(&mut app, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(
        state.is_drilled_down(),
        "drilled before any event is pumped"
    );
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["1".to_string()])
    );
    pump_until_idle(&mut app, &rx, &tx);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);

    // Hide the key: the buffer no longer holds it.
    app.data_table_state
        .as_mut()
        .unwrap()
        .set_column_order(vec!["n".to_string(), "total".to_string()]);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert!(app.is_busy(), "reading the row");
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["1".to_string()])
    );
    assert!(on_screen(&app, "c").iter().all(|c| c == "1"));
}

/// A sort on the aggregate reorders its rows; Enter drills into the row on screen, not
/// the one that was there before the sort.
#[test]
fn test_drill_from_a_sorted_aggregate_takes_the_row_on_screen() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sorted_aggregate.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(&AppEvent::Search("select n: count a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&AppEvent::Sort(vec!["c".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    assert_eq!(on_screen(&app, "c"), ["2", "1", "0"]);

    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["2".to_string()])
    );
    assert!(on_screen(&app, "c").iter().all(|c| c == "2"));
}

/// Columns hidden and frozen inside a drill belong to the drill: Esc puts back the
/// grouped view's own column order and frozen key.
#[test]
fn test_esc_restores_the_grouped_columns_changed_inside_the_drill() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_columns_restored.csv");
    let area = Rect::new(0, 0, 100, 30);
    app.event(&AppEvent::Search(
        "select n: count a, total: sum a by c".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);

    let state = app.data_table_state.as_mut().unwrap();
    state.set_column_order(vec!["name".to_string(), "a".to_string()]);
    state.set_locked_columns(2);
    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["c", "n", "total"]);
    assert_eq!(state.locked_columns_count(), 1);
    assert_eq!(on_screen(&app, "total"), ["1683", "1617", "1650"]);
}

/// A list-form group whose key is null drills into rows whose key is null, not the
/// text "null"; a list form that also aggregates names only its keys in the breadcrumb.
#[test]
fn test_drill_from_lists_keeps_a_null_key_and_names_only_keys() {
    let csv = "k,v\nx,1\n,2\ny,3\n,4\n";
    let (mut app, rx, tx) = open_csv_with("drill_list_null_key.csv", csv, OpenOptions::default());
    app.event(&AppEvent::Search("select v, n: count v by k".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.is_grouped());
    // Nulls sort last.
    state.drill_down_into_group(2).unwrap();
    assert_eq!(
        state
            .drilled_group_key()
            .map(|(columns, _)| columns.to_vec()),
        Some(vec!["k".to_string()])
    );
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 2);
    assert_eq!(df.column("k").unwrap().null_count(), 2);
    assert_eq!(df.column("k").unwrap().dtype(), &DataType::String);
}

/// Enter where there is nothing to drill into says so on the control bar.
#[test]
fn test_enter_with_nothing_to_drill_into_flashes() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_nothing.csv");
    let area = Rect::new(0, 0, 100, 30);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(app.flash_message(), Some("Nothing to drill into"));

    app.event(&AppEvent::Search("select n: count a by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    assert_eq!(
        app.flash_message(),
        Some("Already in a group; Esc goes back")
    );
    assert!(app.data_table_state.as_ref().unwrap().is_drilled_down());
}

/// Salaries by department, 40 rows: `dept` cycles eng, ops, sales and a null every
/// fourth row; `salary` climbs by 5,000 from 60,000; `ts` is the hour `i % 24`.
fn open_salary_fixture(name: &str) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let dir = common::fixture_dir().join(name);
    let n = 40i64;
    let df = df!(
        "id" => (0..n).collect::<Vec<_>>(),
        "dept" => (0..n)
            .map(|i| ["eng", "ops", "sales"].get((i % 4) as usize).copied())
            .collect::<Vec<_>>(),
        "salary" => (0..n).map(|i| 60_000 + i * 5_000).collect::<Vec<_>>(),
        "ts" => (0..n).map(|i| (i % 24) * 3_600_000_000).collect::<Vec<_>>(),
    )
    .unwrap()
    .lazy()
    .with_column(col("ts").cast(DataType::Datetime(TimeUnit::Microseconds, None)))
    .collect()
    .unwrap();
    write_parquet(&dir, "", df);
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.join("data.parquet")],
        OpenOptions::default(),
    );
    pump_until_idle(&mut app, &rx, &tx);
    (app, rx, tx)
}

/// Run `sql` as the SQL prompt would and wait for its rows.
fn run_sql(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>, sql: &str) {
    app.event(&AppEvent::SqlSearch(sql.to_string()));
    pump_until_idle(app, rx, tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{sql}: {:?}", state.error());
}

/// The acceptance case: Enter on a department of a SQL `GROUP BY` shows that
/// department's rows that passed the `WHERE`, key first, and Esc brings the grouped
/// rows back with the cursor on the department.
#[test]
fn test_sql_group_by_drills_into_rows_after_where() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_where");
    let area = Rect::new(0, 0, 100, 30);
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, AVG(salary) AS avg_salary FROM df WHERE salary > 100000 GROUP BY dept",
    );
    painted(&mut app, &rx, &tx, area);
    let depts = on_screen(&app, "dept");
    assert_eq!(depts.len(), 4, "eng, ops, sales and null: {depts:?}");
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        1,
        "the key is frozen, as a `by` key is"
    );

    press_and_send(&mut app, &tx, KeyCode::Down);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.is_drilled_down());
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec![depts[1].clone()])
    );
    assert_eq!(state.headers(), ["dept", "id", "salary", "ts"]);
    let df = state.lf().clone().collect().unwrap();
    let salaries = df.column("salary").unwrap().i64().unwrap();
    assert!(salaries.into_no_null_iter().all(|s| s > 100_000));
    let expected = (0..40i64)
        .filter(|i| 60_000 + i * 5_000 > 100_000)
        .filter(|i| {
            ["eng", "ops", "sales"]
                .get((i % 4) as usize)
                .map_or("null", |d| d)
                == depts[1]
        })
        .count();
    assert_eq!(df.height(), expected);
    assert!(on_screen(&app, "dept").iter().all(|d| *d == depts[1]));

    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    painted(&mut app, &rx, &tx, area);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(!state.is_drilled_down());
    assert_eq!(state.headers(), ["dept", "avg_salary"]);
    assert_eq!(on_screen(&app, "dept"), depts);
    assert_eq!(state.table_state.selected(), Some(1));
}

/// Every group of a SQL aggregate drills into as many rows as it counted, whatever the
/// key: a null, a renamed column, a computed key named by its alias, by its expression
/// or by ordinal, several keys, and with HAVING, ORDER BY and LIMIT on the result.
#[test]
fn test_sql_group_by_drills_by_null_computed_and_aliased_keys() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_keys");
    for sql in [
        "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept",
        "SELECT COUNT(*) AS n, dept AS d FROM df GROUP BY dept",
        "SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM df GROUP BY h",
        "SELECT EXTRACT(HOUR FROM ts) AS h, COUNT(*) AS n FROM df GROUP BY EXTRACT(HOUR FROM ts)",
        "SELECT salary > 150000 AS high, COUNT(*) AS n FROM df GROUP BY 1",
        "SELECT dept, id % 2 = 0 AS even, COUNT(*) AS n FROM df GROUP BY dept, even",
        "SELECT dept, COUNT(*) AS n FROM df WHERE id > 5 GROUP BY dept \
         HAVING COUNT(*) > 3 ORDER BY n DESC, dept LIMIT 3",
        "SELECT t.dept, COUNT(*) AS n FROM df AS t WHERE t.salary < 200000 GROUP BY t.dept",
    ] {
        run_sql(&mut app, &rx, &tx, sql);
        let state = app.data_table_state.as_mut().unwrap();
        assert!(state.can_drill_down(), "{sql}");
        let counts: Vec<u32> = state
            .lf()
            .clone()
            .collect()
            .unwrap()
            .column("n")
            .unwrap()
            .u32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert!(!counts.is_empty(), "{sql}");
        for (group, counted) in counts.into_iter().enumerate() {
            state.drill_down_into_group(group).unwrap();
            let rows = state.lf().clone().collect().unwrap();
            assert_eq!(rows.height() as u32, counted, "{sql}, group {group}");
            assert!(
                rows.get_column_names()
                    .iter()
                    .all(|c| !c.starts_with("__datui")),
                "{sql}: no scratch columns"
            );
            state.drill_up().unwrap();
        }
    }
}

/// A null key drills into the rows whose key is null, and a computed key into the
/// rows that compute it, with the source's own columns.
#[test]
fn test_sql_group_by_null_and_computed_key_rows() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_null");
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept ORDER BY dept NULLS LAST",
    );
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(3).unwrap();
    assert_eq!(
        state
            .drilled_group_key()
            .map(|(columns, _)| columns.to_vec()),
        Some(vec!["dept".to_string()])
    );
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.height(), 10);
    assert_eq!(df.column("dept").unwrap().null_count(), 10);
    state.drill_up().unwrap();

    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT EXTRACT(HOUR FROM ts) AS h, SUM(salary) AS total FROM df \
         GROUP BY h ORDER BY h",
    );
    let state = app.data_table_state.as_mut().unwrap();
    state.drill_down_into_group(3).unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["3".to_string()])
    );
    assert_eq!(state.headers(), ["id", "dept", "salary", "ts"]);
    let df = state.lf().clone().collect().unwrap();
    let ids: Vec<i64> = df
        .column("id")
        .unwrap()
        .i64()
        .unwrap()
        .into_no_null_iter()
        .collect();
    assert_eq!(ids, [3, 27], "hour 3");
}

/// `ARRAY_AGG` lists are values the statement computed, not the group's rows: a drill
/// still shows the source rows.
#[test]
fn test_sql_group_by_with_lists_drills_into_source_rows() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_lists");
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, ARRAY_AGG(id) AS ids, MAX(salary) AS top FROM df \
         WHERE dept IS NOT NULL GROUP BY dept ORDER BY dept",
    );
    let state = app.data_table_state.as_mut().unwrap();
    assert!(state.is_grouped(), "a list column");
    state.drill_down_into_group(0).unwrap();
    assert_eq!(state.headers(), ["dept", "id", "salary", "ts"]);
    assert_eq!(state.lf().clone().collect().unwrap().height(), 10);
}

/// A statement whose rows cannot be traced back reliably does not drill: Enter says
/// there is nothing to drill into.
#[test]
fn test_sql_shapes_without_a_source_do_not_drill() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_unsupported");
    let area = Rect::new(0, 0, 100, 30);
    for sql in [
        "SELECT dept, salary FROM df",
        "SELECT dept, COUNT(*) AS n FROM df \
         WHERE dept IN (SELECT dept FROM df WHERE salary > 200000) GROUP BY dept",
        "SELECT a.dept, COUNT(*) AS n FROM df AS a JOIN df AS b ON a.id = b.id GROUP BY a.dept",
        "WITH t AS (SELECT * FROM df) SELECT dept, COUNT(*) AS n FROM t GROUP BY dept",
        "SELECT AVG(salary) AS avg FROM df GROUP BY dept",
    ] {
        run_sql(&mut app, &rx, &tx, sql);
        painted(&mut app, &rx, &tx, area);
        assert!(
            !app.data_table_state.as_ref().unwrap().can_drill_down(),
            "{sql}"
        );
        press_and_send(&mut app, &tx, KeyCode::Enter);
        assert_eq!(app.flash_message(), Some("Nothing to drill into"), "{sql}");
        assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
    }
}

/// Enter on a grouped result with no rows says there is nothing to drill into rather
/// than doing nothing.
#[test]
fn test_enter_on_an_empty_sql_group_by_flashes() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_empty");
    let area = Rect::new(0, 0, 100, 30);
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, COUNT(*) AS n FROM df WHERE salary < 0 GROUP BY dept",
    );
    painted(&mut app, &rx, &tx, area);
    assert_eq!(current_rows(&app), 0);
    press_and_send(&mut app, &tx, KeyCode::Enter);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.flash_message(), Some("Nothing to drill into"));
    assert!(!app.data_table_state.as_ref().unwrap().is_drilled_down());
}

/// Polars returns groups in any order. A grouping without ORDER BY comes back sorted by
/// its keys, as a `by` result does, so each read of it (a page, the count, Esc from a
/// drill) shows the same rows in the same places; ORDER BY is left as written.
#[test]
fn test_sql_group_by_without_order_by_is_sorted_by_its_keys() {
    let (mut app, rx, tx) = open_salary_fixture("sql_group_order");
    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT EXTRACT(HOUR FROM ts) AS h, dept, COUNT(*) AS n FROM df GROUP BY dept, h",
    );
    let df = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap();
    let sorted = df
        .sort(
            ["h", "dept"],
            SortMultipleOptions::default().with_nulls_last(true),
        )
        .unwrap();
    assert!(df.equals_missing(&sorted), "{df}");
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        2,
        "both keys lead, so both are frozen"
    );

    run_sql(
        &mut app,
        &rx,
        &tx,
        "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept ORDER BY dept DESC NULLS FIRST",
    );
    assert_eq!(
        current_rows(&app),
        4,
        "the order as written: {:?}",
        app.data_table_state
            .as_ref()
            .unwrap()
            .lf()
            .clone()
            .collect()
    );
    let first = app
        .data_table_state
        .as_ref()
        .unwrap()
        .lf()
        .clone()
        .collect()
        .unwrap()
        .column("dept")
        .unwrap()
        .get(0)
        .unwrap()
        .is_null();
    assert!(first, "ORDER BY is kept");
}

/// A GROUP BY that fails once it runs is not applied, and the view left in place drills
/// as before: not at all over the rows as loaded, and by its own keys, not the failed
/// statement's, over a grouped view.
#[test]
fn test_a_failed_group_by_leaves_the_grouped_view_drilling_by_its_keys() {
    let (mut app, rx, tx) = open_salary_fixture("sql_drill_rollback");
    let failing = "SELECT CAST(dept AS INT) AS dept, COUNT(*) AS n FROM df GROUP BY 1";
    // Over the rows as loaded, the failed statement leaves nothing to drill into.
    app.event(&AppEvent::SqlSearch(failing.to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.modal_showing(), "the failure is said");
    assert!(!app.data_table_state.as_ref().unwrap().can_drill_down());
    press_and_send(&mut app, &tx, KeyCode::Esc);
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.modal_showing());

    let grouped = "SELECT dept, COUNT(*) AS n FROM df GROUP BY dept";
    run_sql(&mut app, &rx, &tx, grouped);
    app.event(&AppEvent::SqlSearch(failing.to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    assert!(app.modal_showing(), "the failure is said");
    let state = app.data_table_state.as_mut().unwrap();
    assert_eq!(state.get_active_sql_query(), grouped);
    state.drill_down_into_group(0).unwrap();
    assert_eq!(
        state.drilled_group_key().map(|(_, values)| values.to_vec()),
        Some(vec!["eng".to_string()])
    );
    assert_eq!(current_rows(&app), 10);
}

/// SQL inside a drill-down runs on the group, like the sidebar does, not on the whole
/// loaded table.
#[test]
fn test_sql_inside_a_drill_down_stays_in_the_group() {
    let (mut app, rx, tx) = open_query_filter_fixture("drill_sql.csv");

    app.event(&AppEvent::Search("select by c".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    app.data_table_state
        .as_mut()
        .unwrap()
        .drill_down_into_group(0)
        .unwrap();
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 34);

    app.event(&AppEvent::SqlSearch(
        "SELECT * FROM df WHERE a < 30".to_string(),
    ));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    assert_eq!(
        current_rows(&app),
        10,
        "a in 0, 3, ..., 27: within the group"
    );
}

/// Phase 2 promised that aggregations show nulls: a column some files were written
/// without must read as null everywhere it is absent, not vanish from the aggregate and
/// not stop it. Describe is the aggregation a user reaches for first, so this asserts on
/// the panel it paints — the `Nulls` figure beside `extra` — rather than on the results
/// struct behind it. `extra` is in two of the three files, so it counts two values and
/// one null, and that one null is an absence: no file wrote a null into `extra`.
#[test]
fn test_an_aggregation_counts_an_absent_column_as_null() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "date=2024-01-03",
        df!("id" => &[3i64], "extra" => &["y"]).unwrap(),
    );

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 120, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    // `a` opens the analysis modal; Enter on Describe, where the sidebar starts,
    // shows its Sample form, and Enter again runs it.
    if let Some(next) = app.event(&key(KeyCode::Char('a'))) {
        let _ = tx.send(next);
    }
    app.event(&key(KeyCode::Enter));
    if let Some(next) = app.event(&key(KeyCode::Enter)) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect();

    // That the Describe table is the thing on screen, before reading figures off it.
    // Without this the fallback is the data table, whose header also begins with a
    // column name, and the failure would be about the wrong screen.
    assert!(
        rows.iter()
            .any(|line| line.contains("Count") && line.contains("Nulls")),
        "Describe should be on screen with its Count and Nulls columns; got:\n{}",
        rows.join("\n")
    );
    let extra = rows
        .iter()
        .find(|line| line.trim_start().starts_with("extra"))
        .unwrap_or_else(|| {
            panic!(
                "describe should list `extra`, the column two of the three files have; got:\n{}",
                rows.join("\n")
            )
        });
    // `skip(1)` steps over the column name, which this fixture keeps to a single token
    // on purpose: a name with a space in it would put its second half where Count is.
    // The row also runs into the sidebar at the right, which is harmless while only the
    // first two figures are read.
    let figures: Vec<&str> = extra.split_whitespace().skip(1).collect();
    assert_eq!(
        figures.first().copied(),
        Some("2"),
        "two files wrote `extra`, so it counts two values; row was {extra:?}"
    );
    assert_eq!(
        figures.get(1).copied(),
        Some("1"),
        "the third file was written without `extra`, and that absence counts as a null \
         in the aggregate; row was {extra:?}"
    );
}

/// Describe scrolls its statistics as far as the last one and no further. → past
/// the end does nothing, so the first ← always moves back, however many times →
/// was pressed. The bound is what the table drew, not a count kept beside it.
#[test]
fn test_describe_scrolls_to_its_last_statistic_and_back_in_one_press() {
    use datui::analysis_modal::AnalysisFocus;

    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![PathBuf::from("tests/sample-data/people.parquet")],
        OpenOptions::default(),
    );
    app.event(&key(KeyCode::Char('a')));
    show_sample_form(&mut app);
    let mut next = app.event(&key(KeyCode::Enter));
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    drain_events(&mut app, &rx);
    assert!(app.analysis_modal.describe_results.is_some());
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);

    // 80 columns fit a handful of the nine statistics beside the tool list.
    let area = Rect::new(0, 0, 80, 24);
    let header = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        (0..area.width)
            .map(|x| buf[(x, 1)].symbol().to_string())
            .collect::<String>()
    };
    let first = header(&mut app);
    assert!(first.contains("Count"), "{first:?}");
    assert!(!first.contains("Max"), "not everything fits: {first:?}");
    assert!(
        first.contains('+'),
        "the hidden statistics are counted: {first:?}"
    );
    let max = app.analysis_modal.describe_columns.max;
    assert!(max > 0);

    for _ in 0..20 {
        app.event(&key(KeyCode::Right));
        header(&mut app);
    }
    assert_eq!(app.analysis_modal.describe_columns.offset, max);
    let end = header(&mut app);
    assert!(
        end.contains("Max"),
        "the last statistic is reached: {end:?}"
    );
    assert!(
        !end.contains('+'),
        "and nothing is counted past it: {end:?}"
    );

    app.event(&key(KeyCode::Left));
    let back = header(&mut app);
    assert_eq!(app.analysis_modal.describe_columns.offset, max - 1);
    assert_ne!(back, end, "one press back moves the table");
    assert!(
        back.contains("+1"),
        "the last statistic is out of view: {back:?}"
    );
}

/// Opening a directory measures what it cost, and the Info panel says so.
///
/// The end of the wiring rather than any one link in it: the open records into the
/// meter the app holds, the panel is handed that same meter, and the Resources tab
/// prints it. Each of those is guarded on its own elsewhere; none of those guards would
/// notice the panel being handed a meter nobody wrote to, which is the failure a user
/// would actually see — a Measurements section that says a dataset of three files was
/// found in no time at all.
///
/// Only the counts are asserted. The times are real elapsed times, so the only claim
/// that holds on every machine is that they were taken.
#[test]
fn test_opening_a_directory_measures_it_and_the_info_panel_says_so() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            dir.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }

    let (mut app, rx, tx) = open_local_dataset_with_channel(dir.path());
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    // `i` opens the panel on the Schema tab with the focus in its body; Tab moves the
    // focus to the tab bar, and one step right from Schema is Resources.
    for k in [KeyCode::Char('i'), KeyCode::Tab, KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);

    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);

    assert!(
        text.contains("Measurements"),
        "the Resources tab says what the open cost; got:\n{text}"
    );
    assert!(
        text.contains("Listing:") && text.contains("Footers:"),
        "naming the two stretches it timed; got:\n{text}"
    );
    assert!(
        text.contains("3 files"),
        "the listing found three files; got:\n{text}"
    );
    // Six, not three. The open reads a footer from each file, and then the count pass
    // re-walks the directory and reads every footer again to settle the row count — which
    // happens on an ordinary three-file open, not only in some corner. Both are footer
    // reads and both cost what they cost, so the row says six.
    assert!(
        text.contains("6 footers read"),
        "and the footer row counts the open's pass and the count's, which is six reads \
         over three files; got:\n{text}"
    );
    assert!(
        !text.contains("requests"),
        "a local directory is read, not requested, so no request count is claimed; got:\n{text}"
    );
}

/// A route that measures and then gives up leaves nothing on the dataset another route
/// built.
///
/// The routes are tried cheapest first, and the early ones measure before they discover
/// they cannot finish. A directory whose only Parquet sits under a `_delta_log` is the
/// case that reaches the screen: the hive route walks it, counts the checkpoint as the
/// writer's own bookkeeping, records a listing of no files and a footer pass over none,
/// and then gives up — while the full scan's glob does match the checkpoint and opens it.
/// Sharing one meter across the attempts paints `0 files` on a dataset showing rows.
#[test]
fn test_a_route_that_gave_up_leaves_no_figures_on_the_dataset_that_opened() {
    let dir = tempfile::tempdir().unwrap();
    let log = dir.path().join("_delta_log");
    std::fs::create_dir_all(&log).unwrap();
    let mut frame = df!("id" => &[1i64, 2, 3]).unwrap();
    let f = File::create(log.join("00000.checkpoint.parquet")).unwrap();
    ParquetWriter::new(f).finish(&mut frame).unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app
        .data_table_state
        .as_ref()
        .expect("the full scan opened the checkpoint");
    let listing = state.measurements().listing();
    assert!(
        listing.is_none_or(|c| c.files != Some(0)),
        "the hive route walked, found nothing it would read, and gave up — its empty \
         listing must not end up on the dataset that did open; got {listing:?}"
    );

    for k in [KeyCode::Char('i'), KeyCode::Tab, KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);
    assert!(
        !text.contains("0 files"),
        "and the Resources tab shows no listing of no files; got:\n{text}"
    );
}

/// A dataset Polars opened shows no measurements, even though its rows are counted
/// afterwards.
///
/// `--single-spine-schema=false` turns off the footer pass and hands the directory
/// straight to Polars, so there is nothing for datui to report. But the row count is
/// still taken from the footers afterwards, against the same dataset — and that pass
/// writing into the meter would raise a section out of nothing, headed by what opening
/// the dataset cost and containing only work done after it was already on screen.
#[test]
fn test_a_dataset_polars_opened_reports_nothing_about_opening_it() {
    let dir = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            dir.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            single_spine_schema: false,
            ..OpenOptions::default()
        },
    );
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);

    let state = app.data_table_state.as_ref().expect("the directory opened");
    assert_eq!(
        state.measurements().footers(),
        None,
        "the count pass ran, and belongs to no open this meter measured"
    );
    assert_eq!(
        state.measurements().total(),
        None,
        "and with neither stretch there is nothing to total"
    );

    for k in [KeyCode::Char('i'), KeyCode::Tab, KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until_idle(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);
    assert!(
        !text.contains("Listing:") && !text.contains("Footers:") && !text.contains("Total:"),
        "so the Resources tab says nothing about what the open cost; got:\n{text}"
    );
    // The page it is showing is datui's own work on any route, and is reported.
    assert!(
        text.contains("Last page:"),
        "while what the page on screen cost is measured whichever route opened the \
         dataset; got:\n{text}"
    );
}

/// A second open does not inherit the first's figures, and a *failed* second open does
/// not give its figures to the dataset it left on screen.
///
/// The meter belongs to the dataset, not to the app, which is what makes the second
/// half true: a load that never reaches the screen never has its meter installed, so
/// the dataset still up keeps its own. An app-held meter gets this wrong in a way that
/// is hard to see — the panel keeps its heading and its shape, and only the numbers
/// underneath belong to something else.
#[test]
fn test_a_second_open_measures_itself_and_not_the_dataset_before_it() {
    let first = tempfile::tempdir().unwrap();
    for day in 1..=3 {
        write_parquet(
            first.path(),
            &format!("date=2024-01-0{day}"),
            df!("id" => &[day as i64]).unwrap(),
        );
    }
    let second = tempfile::tempdir().unwrap();
    write_parquet(
        second.path(),
        "date=2024-02-01",
        df!("id" => &[9i64]).unwrap(),
    );
    // Seven files that are named like Parquet and are not, so the open fails after its
    // listing and its footer pass have both been measured.
    let broken = tempfile::tempdir().unwrap();
    let broken_part = broken.path().join("date=2024-03-01");
    std::fs::create_dir_all(&broken_part).unwrap();
    for i in 0..7 {
        std::fs::write(broken_part.join(format!("f{i}.parquet")), b"not parquet").unwrap();
    }

    let hive = || OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    let files_of = |app: &App| {
        app.data_table_state
            .as_ref()
            .and_then(|s| s.measurements().listing())
            .and_then(|c| c.files)
    };

    let (mut app, rx, tx) = open_local_dataset_with_channel(first.path());
    let area = Rect::new(0, 0, 100, 30);
    let _ = painted(&mut app, &rx, &tx, area);
    let meter_of_the_first = app
        .data_table_state
        .as_ref()
        .unwrap()
        .measurements()
        .clone();
    assert_eq!(
        files_of(&app),
        Some(3),
        "the first open measured its own three files"
    );

    // A second open that succeeds replaces both the dataset and its figures.
    pump_open_until_loaded(&mut app, &rx, vec![second.path().to_path_buf()], hive());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        !std::sync::Arc::ptr_eq(
            &meter_of_the_first,
            app.data_table_state.as_ref().unwrap().measurements()
        ),
        "the second dataset was installed with a meter of its own"
    );
    assert_eq!(
        files_of(&app),
        Some(1),
        "and reports the one file it found, not the four both directories hold between them"
    );

    // A third open that fails leaves the second dataset up — and leaves its figures
    // alone. The failed load measured seven files; none of them may appear here.
    let meter_of_the_second = app
        .data_table_state
        .as_ref()
        .unwrap()
        .measurements()
        .clone();
    pump_open_until_loaded(&mut app, &rx, vec![broken.path().to_path_buf()], hive());
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        std::sync::Arc::ptr_eq(
            &meter_of_the_second,
            app.data_table_state.as_ref().unwrap().measurements()
        ),
        "the dataset on screen is still the second one, with the meter it was \
         installed with"
    );
    assert_eq!(
        files_of(&app),
        Some(1),
        "so the panel still says one file — not the seven the load that failed walked"
    );
}

/// `→` goes inside a local directory that opens as one dataset, as it has always done in
/// a bucket.
///
/// The gate was `is_object_store_url`, so on a local hive tree or a local directory of
/// part files there was no way in at all: Enter opened the whole thing, `←`/`→` folded
/// the section, and the files inside were unreachable from the home screen. That is the
/// escape hatch for a directory classified wrongly, and locally there was none.
#[test]
fn test_right_goes_inside_a_local_multi_file_directory() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("sales");
    std::fs::create_dir_all(&directory).unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(directory.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the directory is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::MultiFile),
        "a directory of part files is offered as one dataset"
    );

    // The bar says the door is there, since nothing else on screen does.
    // Wide on purpose: the bar is cut from the right, and this assertion is about
    // what the bar says, not about where the fitting loop stops.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the key: {bar:?}");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));

    assert_eq!(
        app.home.browsing.as_deref(),
        Some(directory.as_path()),
        "→ browsed into the directory rather than folding the section"
    );
}

/// Recents grouped by place: two recents in one directory, one in another, so the
/// home screen shows two place rows. Returns the app with the cursor on the first
/// place row, the three recents, and the app's cache.
///
/// `seed_store` records them in the recents store too, for a test that reads it back.
/// The store is this test's own, under `tmp`: the one the process shares is capped at
/// fifty recents, and tests opening files in parallel push these out of it.
fn app_with_recents_in_two_places(
    tmp: &tempfile::TempDir,
    seed_store: bool,
) -> (App, Vec<PathBuf>, datui::CacheManager) {
    common::isolate_cache();
    // As the store keeps them: `/var` is `/private/var` on macOS, and a Windows temp
    // directory is named `RUNNER~1` until canonicalized.
    let root = datui::canonical::canonicalize(tmp.path()).unwrap();
    let here = root.join("here");
    let there = root.join("there");
    std::fs::create_dir_all(&here).unwrap();
    std::fs::create_dir_all(&there).unwrap();
    let recents = vec![
        here.join("a.parquet"),
        here.join("b.parquet"),
        there.join("c.parquet"),
    ];
    for path in &recents {
        std::fs::write(path, b"x").unwrap();
    }
    // Recorded the way an open records them, so what the test forgets is what the
    // store holds. Oldest first: `push_recent` puts each at the front.
    let cache = datui::CacheManager::with_dir(root.join("cache"));
    if seed_store {
        for path in recents.iter().rev() {
            assert_eq!(
                cache.push_recent(path),
                datui::cache::HistoryUpdate::Written
            );
        }
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.use_cache(cache.clone());
    app.enter_home();
    app.home.rebuild(&[], &recents);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == here))
        .expect("the directory two recents live in is a place row");
    app.home.selected = row;
    (app, recents, cache)
}

/// `Enter` on a place row browses the place: the way back to a directory found by
/// hand, now that a recent's directory is no longer a section of its own.
#[test]
fn test_enter_on_a_place_row_browses_it() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents, _) = app_with_recents_in_two_places(&tmp, false);
    let here = recents[0].parent().unwrap().to_path_buf();

    // The bar says → goes inside, the same as on any directory.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the door: {bar:?}");

    app.event(&key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(here.as_path()));
    assert_eq!(
        app.home.browse_start.as_deref(),
        Some(here.as_path()),
        "Esc comes back from here to the listing"
    );

    // → is the other door to the same place.
    app.event(&key(KeyCode::Esc));
    assert_eq!(app.home.browsing, None);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == here))
        .expect("back at the listing");
    app.home.selected = row;
    app.event(&key(KeyCode::Right));
    assert_eq!(app.home.browsing.as_deref(), Some(here.as_path()));
}

/// `Delete` on a place row forgets every recent under it and nothing else, after
/// asking. What is checked is the store, which is what the next launch reads.
#[test]
fn test_delete_on_a_place_row_forgets_exactly_its_recents_after_confirming() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents, cache) = app_with_recents_in_two_places(&tmp, true);
    let holds =
        |cache: &datui::CacheManager, path: &Path| cache.load_recents().iter().any(|p| p == path);
    assert!(recents.iter().all(|p| holds(&cache, p)));

    app.event(&key(KeyCode::Delete));
    let area = Rect::new(0, 0, 120, 30);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(
        screen.contains("Forget 2 recently opened datasets under"),
        "asked first, and told how many: {screen:?}"
    );
    assert!(
        recents.iter().all(|p| holds(&cache, p)),
        "nothing is forgotten until the question is answered"
    );

    // Declined: the store is untouched, and a later confirmation is not armed.
    app.event(&key(KeyCode::Esc));
    assert!(recents.iter().all(|p| holds(&cache, p)));

    app.event(&key(KeyCode::Delete));
    app.event(&key(KeyCode::Enter));
    assert!(
        !holds(&cache, &recents[0]),
        "forgotten: {:?}",
        cache.load_recents()
    );
    assert!(
        !holds(&cache, &recents[1]),
        "forgotten: {:?}",
        cache.load_recents()
    );
    assert!(
        holds(&cache, &recents[2]),
        "the other place's recent is left alone: {:?}",
        cache.load_recents()
    );
}

fn ctrl(c: char) -> AppEvent {
    AppEvent::Key(KeyEvent::new(KeyCode::Char(c), KeyModifiers::CONTROL))
}

/// Ctrl+D keeps the place under the cursor on the home screen, and pressed again stops
/// keeping it. A file row stands for the directory it is in. What is checked is the
/// store, which is what the next listing reads.
#[test]
fn test_ctrl_d_remembers_and_forgets_the_place_under_the_cursor() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let (mut app, recents, cache) = app_with_recents_in_two_places(&tmp, false);
    let here = recents[0].parent().unwrap().to_path_buf();
    let there = recents[2].parent().unwrap().to_path_buf();
    let kept = |cache: &datui::CacheManager, path: &Path| {
        cache.load_remembered_places().iter().any(|p| p == path)
    };

    app.event(&ctrl('d'));
    assert!(kept(&cache, &here), "{:?}", cache.load_remembered_places());
    assert!(
        app.flash_message()
            .is_some_and(|s| s.starts_with("Remembered")),
        "{:?}",
        app.flash_message()
    );

    app.event(&ctrl('d'));
    assert!(!kept(&cache, &here), "a second press forgets it");
    assert!(app.flash_message().is_some_and(|s| s.starts_with("Forgot")));

    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == recents[2]),
        )
        .expect("the recent in the other place is listed");
    app.home.selected = row;
    app.event(&ctrl('d'));
    assert!(kept(&cache, &there), "a file row remembers its directory");
}

/// A remembered directory is listed like a configured one, marked `remembered`, and
/// `Delete` on its heading forgets it. One that is also configured is listed once,
/// as configured, and Ctrl+D on it points at the config rather than doing nothing.
#[test]
fn test_a_remembered_place_is_listed_and_delete_on_its_heading_forgets_it() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = datui::canonical::canonicalize(tmp.path()).unwrap();
    let kept = root.join("kept");
    let configured = root.join("configured");
    std::fs::create_dir_all(&kept).unwrap();
    std::fs::create_dir_all(&configured).unwrap();
    std::fs::write(kept.join("a.parquet"), b"x").unwrap();
    let cache = datui::CacheManager::new("datui").expect("cache");
    cache.remember_place(&kept);

    let mut config = datui::AppConfig::default();
    config.data.directories = vec![configured.to_string_lossy().into_owned()];
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new_with_config(
        tx,
        common::test_runtime(),
        datui::Theme {
            colors: std::collections::HashMap::new(),
        },
        config,
    );
    app.enter_home();
    app.home
        .apply_listing(datui::home::build_listing(&datui::home::ListingRequest {
            collections: Vec::new(),
            config_dirs: vec![configured.clone()],
            remembered_dirs: vec![kept.clone(), configured.clone()],
            recents: Vec::new(),
            desktop_dirs: Vec::new(),
            browsing: None,
            probed: Default::default(),
            unreachable: Default::default(),
            listing_so_far: Default::default(),
            cut_short: Default::default(),
            probe_errors: Default::default(),
            network_check: app.home.network_check,
            cloud: Vec::new(),
            known: Default::default(),
        }));

    let heading = |app: &App, dir: &Path| {
        app.home
            .visible()
            .iter()
            .position(|r| {
                matches!(r, datui::home::Row::Header { section, .. }
                    if app.home.sections[*section].root.as_deref() == Some(dir))
            })
            .expect("the directory has a section")
    };
    let origin = |app: &App, dir: &Path| {
        app.home
            .sections
            .iter()
            .filter(|s| s.root.as_deref() == Some(dir))
            .map(|s| s.origin)
            .collect::<Vec<_>>()
    };
    assert_eq!(origin(&app, &kept), vec![Some("remembered")]);
    assert_eq!(
        origin(&app, &configured),
        vec![Some("configured")],
        "listed once, as configured"
    );

    app.home.selected = heading(&app, &configured);
    app.event(&ctrl('d'));
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("[data] directories")),
        "{:?}",
        app.home.status
    );
    assert!(!cache.load_remembered_places().contains(&configured));

    // A file under the place is a file on disk: Delete points at the heading.
    app.home.selected = heading(&app, &kept) + 1;
    app.event(&key(KeyCode::Delete));
    assert!(cache.load_remembered_places().contains(&kept));
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.starts_with("Delete on the heading")),
        "{:?}",
        app.home.status
    );

    app.home.selected = heading(&app, &kept);
    app.event(&key(KeyCode::Delete));
    assert!(
        !cache.load_remembered_places().contains(&kept),
        "forgotten: {:?}",
        cache.load_remembered_places()
    );
}

/// Inside a directory of notes the one row says what is hidden and the bar offers to
/// show it. Enter does, and Ctrl+A hides them again with a flash on the bar rather than
/// a line beside the filter that outlives it.
#[test]
fn test_enter_on_the_hidden_row_shows_the_files() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for i in 0..3 {
        std::fs::write(tmp.path().join(format!("note{i}.md")), b"x").unwrap();
    }
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    app.home.select_first_entry();

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(screen.contains("3 files datui can't open"), "{screen:?}");
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Show"), "{bar:?}");

    app.event(&key(KeyCode::Enter));
    assert!(!app.home.hide_unreadable);
    assert!(matches!(
        app.home.selected_row(),
        Some(datui::home::Row::Entry { entry, .. }) if entry.name == "note0.md"
    ));

    app.event(&ctrl('a'));
    assert!(app.home.hide_unreadable);
    assert_eq!(app.home.status, None);
    assert_eq!(app.flash_message(), Some("Hiding files datui can't open"));
}

/// `Enter` and `→` on the place of a recent opened over HTTP say why they do nothing,
/// rather than listing a URL and reporting it unreachable.
#[test]
fn test_the_place_of_an_http_recent_says_it_cannot_be_browsed() {
    common::isolate_cache();
    let url = PathBuf::from("https://example.com/data/y.csv");
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], std::slice::from_ref(&url));
    let place = PathBuf::from("https://example.com/data");
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { path, .. } if *path == place))
        .expect("the URL's prefix is its place");
    app.home.selected = row;

    // The bar does not offer the door.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(!bar.contains("Inside"), "{bar:?}");
    // And the details pane says why, before Enter is pressed.
    let screen: String = buf.content().iter().map(|c| c.symbol()).collect();
    assert!(screen.contains("No listing over HTTP"), "{screen}");

    app.event(&key(KeyCode::Enter));
    assert_eq!(app.home.browsing, None);
    assert!(
        app.home
            .status
            .as_deref()
            .is_some_and(|s| s.contains("HTTP")),
        "{:?}",
        app.home.status
    );
    // The line answers the last key: the next one takes it down.
    app.event(&key(KeyCode::Right));
    assert_eq!(app.home.browsing, None);
    assert_eq!(app.home.status, None, "gone at the next key");
}

/// The bar's count is of what is listed, which the header and the `more` row agree on.
#[test]
fn test_the_bar_counts_datasets_past_the_cap() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], &recents);
    for section in 1..app.home.sections.len() {
        app.home.set_collapsed(section, true);
    }
    let area = Rect::new(0, 0, 200, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(screen.contains("more in"), "the cap is drawn: {screen:?}");
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("12 datasets"), "{bar:?}");
}

/// The rendered list, one string per screen row, without the control bar.
fn list_rows(buf: &Buffer, area: Rect) -> Vec<String> {
    (0..area.height - 1)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect()
}

/// With thirty list rows or more, a blank line precedes every section header but the
/// first; below that, none. The spacers are counted against the cap and the scroll,
/// so the selected row is always on screen.
#[test]
fn test_a_tall_list_spaces_its_sections_and_a_short_one_does_not() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();
    let configured = tmp.path().join("configured");
    std::fs::write(
        {
            std::fs::create_dir_all(&configured).unwrap();
            configured.join("c.parquet")
        },
        b"x",
    )
    .unwrap();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home
        .rebuild(std::slice::from_ref(&configured), &recents);

    let is_header = |line: &str| {
        line.contains("RECENT") || line.contains("current directory") || line.contains("configured")
    };
    // Tall: the wordmark and prompt take the top rows; the list below has room.
    let area = Rect::new(0, 0, 100, 50);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows = list_rows(&buf, area);
    let headers: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, l)| is_header(l))
        .map(|(i, _)| i)
        .collect();
    assert!(headers.len() >= 2, "{rows:?}");
    for at in &headers[1..] {
        assert!(
            rows[at - 1].trim().is_empty(),
            "a blank line precedes the header at {at}: {:?}",
            rows[at - 1]
        );
    }
    assert!(
        !rows[headers[0] - 1].trim().is_empty()
            || headers[0] == 0
            || rows[headers[0] - 1].contains("filter"),
        "no blank line before the first header"
    );
    let spacers = headers.len() - 1;
    let list_height = app.home.view_height + spacers;
    assert!(list_height >= 30, "{list_height}");
    assert_eq!(
        app.home.view_height,
        list_height - spacers,
        "the spacers come off the height the cap is a share of"
    );

    // The last row on screen, selected: drawn, spacers and all.
    let last = app.home.visible().len() - 1;
    app.home.selected = last;
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    let name = match app.home.selected_row() {
        Some(datui::home::Row::Entry { entry, .. }) => entry.name.clone(),
        other => panic!("{other:?}"),
    };
    assert!(
        screen.contains(&name),
        "the selected row is on screen: {screen:?}"
    );

    // Short: dense.
    let area = Rect::new(0, 0, 100, 24);
    app.home.selected = 0;
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows = list_rows(&buf, area);
    let headers: Vec<usize> = rows
        .iter()
        .enumerate()
        .filter(|(_, l)| is_header(l))
        .map(|(i, _)| i)
        .collect();
    assert!(headers.len() >= 2, "{rows:?}");
    for at in &headers[1..] {
        assert!(
            !rows[at - 1].trim().is_empty(),
            "no blank line before the header at {at} on a short screen"
        );
    }
}

/// The `… N more` row stands for the places the cap hides. `Enter` on it shows them
/// all, and nothing is opened.
#[test]
fn test_enter_on_the_more_row_expands_recent() {
    common::isolate_cache();
    let tmp = tempfile::tempdir().expect("tempdir");
    let recents: Vec<PathBuf> = (0..12)
        .map(|i| {
            let dir = tmp.path().join(format!("place{i}"));
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("data.parquet");
            std::fs::write(&path, b"x").unwrap();
            path
        })
        .collect();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], &recents);
    // A short screen, so the cap bites: one place, then the more row.
    let area = Rect::new(0, 0, 120, 14);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen = rendered_text(&buf);
    assert!(screen.contains("more in"), "the cap is drawn: {screen:?}");

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::More { .. }))
        .expect("the more row is listed");
    app.home.selected = row;
    app.event(&key(KeyCode::Enter));

    assert_eq!(app.input_mode, InputMode::Home, "nothing was opened");
    assert_eq!(app.home.browsing, None);
    let places = app
        .home
        .visible()
        .iter()
        .filter(|r| matches!(r, datui::home::Row::Place { .. }))
        .count();
    assert_eq!(places, 12, "every place is shown");
    assert!(
        !app.home
            .visible()
            .iter()
            .any(|r| matches!(r, datui::home::Row::More { .. }))
    );
}

/// The hint, and the descent, are only offered on a row that is a dataset directory.
/// `→` elsewhere goes on expanding the section, which on a visible row is already
/// expanded and so does nothing.
#[test]
fn test_right_does_not_browse_from_an_ordinary_row() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("one.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "one.parquet"),
        )
        .expect("the file is listed");
    app.home.selected = row;

    // Wide on purpose: the bar is cut from the right, and this assertion is about
    // what the bar says, not about where the fitting loop stops.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Inside"),
        "a file is not a directory to go inside: {bar:?}"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(tmp.path()),
        "→ on a file does not browse anywhere"
    );
}

/// The note on the heading of the directory being browsed, once its listing is in:
/// where entering a lake table says the table itself is not read.
fn lake_heading(app: &mut App) -> String {
    app.home.rebuild(&[], &[]);
    app.home
        .sections
        .first()
        .and_then(|section| section.subtitle.clone())
        .unwrap_or_default()
}

/// A Delta table's root is not a directory of Parquet files, and the home screen says so.
///
/// Its data files agree on a schema, so the one-table rule called it `multi` and Enter
/// read every file under it as one table — tombstoned rows back, every rewritten
/// version together, compaction counted twice. Nothing warned.
#[test]
fn test_a_delta_table_is_labelled_and_not_opened_as_one_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Delta),
        "the log says what this is"
    );

    let area = Rect::new(0, 0, 200, 24);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n")
    };
    let listing = screen(&mut app);
    assert!(
        listing.contains("delta"),
        "the row says what it is: {listing}"
    );
    assert!(
        !listing.contains("multi"),
        "and does not offer it as a directory of files: {listing}"
    );

    // Enter goes inside rather than reading every file under it as one table.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "Enter went inside the table"
    );
    assert!(app.data_table_state.is_none(), "and opened nothing");
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A lake table typed at `~` is not opened as one table either.
///
/// `home_open_selected` learned to go inside one; the path input had no check at all, so
/// `~` and the table's path loaded every Parquet under the root as one table — the whole
/// of the silent wrong answer, reached one keystroke differently.
#[test]
fn test_a_lake_table_typed_as_a_path_is_gone_inside_not_opened() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.path_input_active = true;
    app.home.path_input = table.to_string_lossy().into_owned();

    let follow = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    assert!(follow.is_none(), "nothing was opened");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the path went inside the table"
    );
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A directory of lake tables is not an empty home screen.
///
/// The guidance block is appended under the rows rather than shown instead of them, so a
/// warehouse of fifty `delta` rows printed "No datasets here." underneath them.
#[test]
fn test_a_directory_of_lake_tables_does_not_say_there_is_nothing_here() {
    let tmp = tempfile::tempdir().expect("tempdir");
    for name in ["orders", "customers"] {
        let table = tmp.path().join(name);
        std::fs::create_dir_all(table.join("_delta_log")).unwrap();
        std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
        std::fs::write(table.join("part-0.parquet"), b"x").unwrap();
        std::fs::write(table.join("part-1.parquet"), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let area = Rect::new(0, 0, 120, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(screen.contains("orders"), "the tables are listed: {screen}");
    assert!(
        !screen.contains("No datasets here."),
        "and the screen does not say there is nothing here: {screen}"
    );
}

/// `→` goes inside a lake table, as it does a hive or multi directory.
///
/// A cloud Delta root used to be labelled `multi`, where `→` descended; recognizing it
/// made `→` fold the section instead. Enter goes inside either way, so nothing was
/// unreachable, but the key that means "look inside this directory" stopped meaning it on
/// the one row where looking inside is all datui can do.
#[test]
fn test_right_goes_inside_a_lake_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Delta)
    );

    // Wide on purpose: this is about what the bar says, not where it is cut.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("Inside"),
        "the key is offered here too: {bar:?}"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "→ went inside the table rather than folding the section"
    );
}

/// A sampled column count shows as a floor on the row and in the details pane.
///
/// The `Entry` flag had a test; what reaches the user had none — reverting either render
/// site left the suite green.
#[test]
fn test_a_sampled_column_count_is_marked_on_screen() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("events");
    std::fs::create_dir_all(&directory).unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "events"))
        .expect("the directory is listed");
    app.home.selected = row;

    // As a directory past the footer budget comes back from measurement.
    for section in app.home.sections.iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "events") {
            entry.kind = datui::discover::EntryKind::MultiFile;
            entry.rows = None;
            entry.cols = Some(39);
            entry.cols_sampled = true;
            entry.columns = vec!["id".to_string()];
        }
    }

    let area = Rect::new(0, 0, 160, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let screen: String = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        screen.contains("39+"),
        "the row and the pane say the count is a floor: {screen}"
    );
    // Both render sites, named separately: a `contains` over the whole screen would be
    // satisfied by either one alone.
    let times = datui::glyphs::get().times;
    assert!(
        screen.contains(&format!("? {times} 39+")),
        "the row's shape says the width is a floor: {screen}"
    );
    assert!(
        screen.contains("columns   39+"),
        "and so does the details pane: {screen}"
    );
}

/// An unexamined directory is classified before Enter opens it.
///
/// A cached kind this build will not take leaves the row `Unknown`, whose `is_dataset()`
/// is true — so Enter fell through to opening the path as one dataset. For a lake root
/// that is the whole of #237, restored from a cache written by an older datui.
#[test]
fn test_an_unexamined_lake_root_is_classified_before_it_is_opened() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;

    // As a row restored from a cache this build will not take its kind from.
    for section in app.home.sections.iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "orders") {
            entry.kind = datui::discover::EntryKind::Unknown;
        }
    }
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Unknown),
        "the row the cursor is on is the unexamined one"
    );

    let follow = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));

    assert!(follow.is_none(), "nothing was opened as one table");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "it was looked at first, found to be a Delta root, and gone inside"
    );
}

/// `→` into a lake table says the same thing `Enter` does.
///
/// The control bar advertises `→` on that row, and `home_browse_into` clears the status
/// line — so the door the bar points at was the one that arrived inside with no
/// explanation.
#[test]
fn test_right_into_a_lake_table_says_why() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    std::fs::write(table.join("part-0.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "orders"))
        .expect("the table is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Right,
        KeyModifiers::NONE,
    )));

    assert_eq!(app.home.browsing.as_deref(), Some(table.as_path()));
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "→ says why it is showing files rather than a table: {status:?}"
    );
}

/// A Delta root on a mount that may not answer is looked at on a worker, and recognized.
///
/// `EntryKind::Unknown` — the only thing a remote row that has never been probed can be —
/// is offered as openable, so Enter read the whole root as one table. Classifying it
/// where the keys are read is the other half of the trap: `exists`, `is_dir` and a
/// `read_dir` on a hard-mounted share that has gone away is an uninterruptible freeze,
/// with Ctrl+C on the same thread.
#[test]
fn test_an_unexamined_remote_lake_root_is_classified_off_the_event_thread() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        std::fs::write(table.join(part), b"x").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    // A share, as the mount table would have it — and the table in Recent, which is how
    // a row on one comes to be listed without anything having looked at it.
    app.home.network_check = |_| true;
    app.home.rebuild(&[], std::slice::from_ref(&table));

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == table))
        .expect("the table is listed under Recent");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Unknown),
        "nothing has looked at it, which is the whole point"
    );

    // The key itself decides nothing: it asks.
    let asked = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    assert!(
        matches!(asked, Some(AppEvent::ClassifyThenOpen { .. })),
        "Enter handed the look to a worker rather than doing it here"
    );
    let mut follow = asked;
    while let Some(event) = follow {
        follow = app.event(&event);
    }
    assert!(app.is_busy(), "and says so while the worker is out");

    // The worker's answer comes back on the channel.
    let mut opened = false;
    while let Some(event) = next_event(&app, &rx) {
        if matches!(event, AppEvent::Open(..)) {
            opened = true;
        }
        let mut follow = app.event(&event);
        while let Some(next) = follow {
            if matches!(next, AppEvent::Open(..)) {
                opened = true;
            }
            follow = app.event(&next);
        }
        if !app.is_busy() {
            break;
        }
    }

    assert!(!opened, "it was never opened as one table");
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the worker found a Delta root, and Enter went inside it"
    );
    let status = lake_heading(&mut app);
    assert!(
        status.contains("delta") && status.contains("not read"),
        "and says why: {status:?}"
    );
}

/// A background probe answering does not cancel the open the user asked for.
///
/// The first gate was `home_generation`, which means "the listing was rebuilt" and not
/// "the user navigated": a probe of some other root answering bumps it. On a home screen
/// with network roots — the only kind where this path runs at all — that made Enter do
/// nothing, at random, with no message.
#[test]
fn test_a_probe_answering_does_not_cancel_an_open_in_flight() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let table = tmp.path().join("orders");
    std::fs::create_dir_all(table.join("_delta_log")).unwrap();
    std::fs::write(table.join("_delta_log/00000000000000000000.json"), b"{}").unwrap();
    std::fs::write(table.join("part-0.parquet"), b"x").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.network_check = |_| true;
    app.home.rebuild(&[], std::slice::from_ref(&table));
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == table))
        .expect("the table is listed under Recent");
    app.home.selected = row;

    let mut follow = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(event) = follow {
        follow = app.event(&event);
    }

    // A listing the user did not ask for lands while the look is out. Through the event,
    // because it is the handler that refreshes the home screen — which is what the first
    // gate mistook for the user having navigated.
    app.event(&AppEvent::HomeProbeReady {
        root: PathBuf::from("/mnt/somewhere-else"),
        rows: Some(Vec::new()),
        cut_short: false,
    });

    while let Some(event) = next_event(&app, &rx) {
        let mut follow = app.event(&event);
        while let Some(next) = follow {
            follow = app.event(&next);
        }
        if !app.is_busy() {
            break;
        }
    }

    assert_eq!(
        app.home.browsing.as_deref(),
        Some(table.as_path()),
        "the answer was still the one the user was waiting for"
    );
}

/// Opening a hive directory from the home screen still reads it as one dataset.
///
/// `home_open_path` used to work that out with a `stat`, which on a share that has gone
/// away is the freeze this whole path exists to avoid. It is told now, from the kind the
/// caller already has — so the thing to pin is that the answer did not change.
#[test]
fn test_a_hive_directory_from_home_still_opens_as_one_dataset() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let hive = tmp.path().join("sales");
    for part in ["year=2024", "year=2025"] {
        std::fs::create_dir_all(hive.join(part)).unwrap();
        std::fs::write(hive.join(part).join("part-0.parquet"), b"x").unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the directory is listed");
    app.home.selected = row;
    // A listing looks into nothing, so what this row is has to be found before it
    // can be acted on. In the app a background pass does it, highlighted row first;
    // here the same call does it on the spot.
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Hive)
    );

    let opened = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    match opened {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![hive.clone()]);
            assert!(options.hive, "read as one partitioned dataset");
        }
        _ => panic!("Enter on a hive directory opens it"),
    }

    // And a single file is not. (The open above left the home screen.)
    app.enter_home();
    std::fs::write(tmp.path().join("one.parquet"), b"x").unwrap();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let row = app
        .home
        .visible()
        .iter()
        .position(
            |r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "one.parquet"),
        )
        .expect("the file is listed");
    app.home.selected = row;
    match app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    ))) {
        Some(AppEvent::Open(_, options)) => assert!(!options.hive, "a file is not a hive tree"),
        _ => panic!("Enter on a file opens it"),
    }

    // And the case that proves the answer is told rather than stat'ed: a row whose kind
    // says hive but whose path no longer answers, which is how a dropped mount presents
    // itself. `is_dir()` is false there, so a stat would call it a single file.
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    let gone = PathBuf::from("/mnt/gone/sales");
    for section in app.home.sections.iter_mut() {
        for entry in section.rows.iter_mut().filter(|e| e.name == "sales") {
            entry.path = gone.clone();
        }
    }
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.path == gone))
        .expect("the row is listed");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Hive),
        "the row still says hive"
    );
    match app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    ))) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![gone]);
            assert!(
                options.hive,
                "told from the kind, not worked out with a stat that cannot reach it"
            );
        }
        other => panic!("Enter opens it: {}", other.is_some()),
    }
}

/// A directory datui offers as one dataset is read as whatever is actually in it.
///
/// A directory the home screen labels `multi` is opened with `hive: true`, and every such
/// directory used to go straight to the Parquet scanner however it was filled. A
/// directory of CSVs therefore failed the way a directory of `.json.gz` did: Parquet
/// seeks to the last four bytes looking for `PAR1`, finds something else, and says the
/// file must end with it — a complaint about files that were never the problem.
#[test]
fn test_a_directory_opened_as_one_dataset_is_read_as_what_it_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("part-0.csv"), "a,b\n1,x\n2,y\n").unwrap();
    std::fs::write(tmp.path().join("part-1.csv"), "a,b\n3,z\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![tmp.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let state = app
        .data_table_state
        .as_ref()
        .expect("a directory of CSVs should open as one table of CSVs");
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(
        df.height(),
        3,
        "both files' rows, concatenated: {:?}",
        df.get_column_names()
    );
}

/// A directory holding two different formats is not one table, and the complaint names
/// the directory rather than Parquet's magic number.
///
/// Asserting on the message, not merely on the failure: opening this directory failed
/// before the fix too — with "must end with PAR1", about files nobody asked to be
/// Parquet. A test that only checked that nothing loaded would pass either way and be
/// about nothing.
#[test]
fn test_a_directory_of_two_formats_is_read_as_the_one_it_mostly_holds() {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("a.csv"), "a\n1\n").unwrap();
    std::fs::write(tmp.path().join("b.csv"), "a\n2\n").unwrap();
    std::fs::write(tmp.path().join("c.json"), "[{\"a\":1}]").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![tmp.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let table = app
        .data_table_state
        .as_ref()
        .expect("two CSVs and a stray JSON is a directory of CSVs");
    assert_eq!(table.headers(), vec!["a"], "read as CSV, not as JSON");
    assert_eq!(
        table.num_rows_if_valid(),
        Some(2),
        "both CSVs, and not the JSON beside them"
    );
}

/// A directory of CSVs with some unrelated directory beside them is still a directory of
/// CSVs.
///
/// The other edge of the same rule: what sends a directory to the Parquet hive scan is a
/// `key=value` partition under it, not merely having a subdirectory. Treating any
/// subdirectory as "the data is deeper" handed an ordinary directory of CSVs back to the
/// scan that cannot read them.
#[test]
fn test_a_directory_of_csvs_beside_an_unrelated_directory_still_reads_as_csvs() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("part-0.csv"), "id\n1\n2\n").unwrap();
    std::fs::write(dir.path().join("part-1.csv"), "id\n3\n").unwrap();
    std::fs::create_dir_all(dir.path().join("archive")).unwrap();

    let app = open_local_dataset(dir.path());
    let state = app
        .data_table_state
        .as_ref()
        .expect("a directory of CSVs should open as one table of CSVs");
    assert_eq!(state.lf().clone().collect().unwrap().height(), 3);
}

/// A hive dataset of something other than Parquet says which files it holds.
///
/// Hive partitioning is a Parquet-only capability in the reader datui uses, so a tree
/// of `date=…/part.json` cannot be read as one table here. It used to reach the
/// Parquet scan anyway and fail with "the file must end with PAR1" — a complaint
/// about files nobody asked to be Parquet, naming neither the directory nor the format.
#[test]
fn test_a_hive_of_json_names_the_files_rather_than_parquets_magic_number() {
    let dir = tempfile::tempdir().unwrap();
    for day in ["2009-03-07", "2009-03-08"] {
        let part = dir.path().join(format!("date={day}"));
        std::fs::create_dir_all(&part).unwrap();
        std::fs::write(part.join("part.json"), "[{\"id\":1}]").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let complaint = pump_open_until_error(
        &mut app,
        &rx,
        vec![dir.path().to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    )
    .expect("a hive of JSON cannot be read as one table");

    assert!(
        complaint.contains(".json") && complaint.contains("Parquet"),
        "it should say what the files are and why that is a problem: {complaint}"
    );
    assert!(
        !complaint.contains("PAR1"),
        "nothing here was ever Parquet: {complaint}"
    );
}

/// And a hive of Parquet still opens, however deeply it is partitioned.
///
/// The check above follows the partitions down to see what they hold; it must not
/// change what happens to the datasets that were always fine.
#[test]
fn test_a_nested_hive_of_parquet_still_opens() {
    let dir = tempfile::tempdir().unwrap();
    write_parquet(
        dir.path(),
        "year=2024/month=01",
        df!("id" => &[1i64, 2]).unwrap(),
    );
    write_parquet(
        dir.path(),
        "year=2024/month=02",
        df!("id" => &[3i64]).unwrap(),
    );

    let app = open_local_dataset(dir.path());
    let state = app
        .data_table_state
        .as_ref()
        .expect("a nested hive of Parquet is a dataset");
    assert_eq!(state.lf().clone().collect().unwrap().height(), 3);
}

/// Two doors, on a directory datui does not recognize as anything.
///
/// A directory holding a CSV and a JSON is `mixed`: no label datui has says it is one
/// table, and before this the row could only be folded — `→` did nothing and `Enter`
/// tried to open it as a dataset and said it could not. A directory whose storage
/// convention datui does not know is exactly the directory a user most needs to get into,
/// so both doors are open on it now: `→` steps inside, and the first row in there reads
/// the whole of it.
#[test]
fn test_both_doors_are_open_on_a_directory_datui_cannot_name() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("exports");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("sales.csv"), b"a,b\n1,2\n").unwrap();
    std::fs::write(directory.join("notes.json"), b"{}").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "exports"))
        .expect("the directory is listed");
    app.home.selected = row;
    app.home.classify_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.label().to_string()),
        Some("mixed".to_string()),
        "nothing datui knows calls this a dataset"
    );

    // The bar says the door is there, on a row no label offers as a dataset.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(bar.contains("Inside"), "the bar offers the key: {bar:?}");

    // One key in.
    app.event(&key(KeyCode::Right));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(directory.as_path()),
        "→ went inside a directory datui has no name for"
    );

    // And the first row in there is the other door. (The app rebuilds the listing on
    // the event this returns; here the same call does it on the spot.)
    app.home.rebuild(&[], &[]);
    let names: Vec<String> = app
        .home
        .visible()
        .iter()
        .filter_map(|r| match r {
            datui::home::Row::Entry { entry, .. } | datui::home::Row::Door { entry, .. } => {
                Some(entry.name.clone())
            }
            _ => None,
        })
        .collect();
    assert_eq!(
        names.first().map(String::as_str),
        Some("exports (all files)"),
        "got {names:?}"
    );
}

/// The `(all files)` row opens the directory it names, whatever the directory is
/// labelled.
///
/// The label describes; this row is the promise that the description cannot lock you
/// out. A `mixed` directory is the case: `open_what_it_is` reads the label back, and a
/// `Directory` sent through it goes *inside* — which, on a row that is already inside,
/// is nowhere.
#[test]
fn test_enter_on_the_whole_directory_row_opens_rather_than_descending() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("sales.csv"), b"a,b\n1,2\n").unwrap();
    std::fs::write(tmp.path().join("notes.json"), b"{}").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("every directory carries the row");
    app.home.selected = row;

    let was = app.home.browsing.clone();

    // → does nothing here. This row is inside the directory it opens, so going inside is
    // nowhere: it would re-enter the listing already on screen and lose the cursor and
    // the filter on the way. ← / → fold, the way they do on a file row. Checked before
    // Enter, because Enter puts the app in its loading view and the home bar is gone.
    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        !bar.contains("Inside"),
        "the bar offers a door to nowhere: {bar:?}"
    );
    app.event(&key(KeyCode::Right));
    assert_eq!(
        app.home.browsing, was,
        "→ on the row that opens this directory must not re-enter it"
    );
    assert_eq!(
        app.home.selected, row,
        "and must not move the cursor off it"
    );

    // Enter opens it.
    let next = app.event(&key(KeyCode::Enter));
    assert!(
        matches!(next, Some(AppEvent::Open(..))),
        "Enter should open the directory rather than move the cursor"
    );
    assert_eq!(
        app.home.browsing, was,
        "and did not step into the directory it is already in"
    );
}

/// The door into a lake table reads its files, and says they are not the table.
///
/// A lake table is not a directory of Parquet files however much it looks like one:
/// reading one as a union counts tombstoned rows, every rewritten version and both
/// sides of a compaction. Refusing it, though, left a directory the user could see and
/// could not read at all — and this row is the promise that no label locks you out.
/// So the read is labelled instead of refused: a note in the panel, a chip beside the
/// row count, and the row one level up still goes inside and says datui does not read
/// the table itself yet. All three, because each on its own is missable.
#[test]
fn test_the_door_into_a_lake_table_says_its_files_are_not_the_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let events = tmp.path().join("events");
    std::fs::create_dir_all(events.join("_delta_log")).unwrap();
    std::fs::write(events.join("_delta_log").join("00000000.json"), b"{}").unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        let file = std::fs::File::create(events.join(part)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(events.clone());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the row");
    app.home.selected = row;
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Delta),
        "the listing under it is a Delta table"
    );

    // It opens, and the open carries what it is.
    let options = match app.event(&key(KeyCode::Enter)) {
        Some(AppEvent::Open(_, options)) => options,
        _ => panic!("the door opens the directory it names, whatever the label says"),
    };
    assert_eq!(
        options.read_as_plain_files_of,
        Some("Delta"),
        "and the open says these are a Delta table's files, not the table"
    );

    // The note and the chip, from that one field. Both, because the note is a tab away
    // and the chip is in the corner: each on its own is missable.
    let notes = datui::notes::from_the_open(
        &[],
        options.read_as_plain_files_of,
        Default::default(),
        false,
    );
    assert_eq!(notes.len(), 1, "one note, about the read");
    assert!(
        notes[0].summary.contains("Delta") && notes[0].summary.contains("deleted rows"),
        "it names the format and what the count includes: {:?}",
        notes[0].summary
    );

    // And the row one level up still goes inside rather than reading it.
    let (tx, _rx) = mpsc::channel();
    let mut up = App::new(tx, common::test_runtime());
    up.enter_home();
    up.home.browsing = Some(tmp.path().to_path_buf());
    up.home.rebuild(&[], &[]);
    let row = up
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "events"))
        .expect("the directory is listed");
    up.home.selected = row;
    assert!(up.event(&key(KeyCode::Enter)).is_none());
    assert_eq!(up.home.browsing.as_deref(), Some(events.as_path()));
    let said = lake_heading(&mut up);
    assert!(
        said.contains("delta") && said.contains("not read"),
        "the row above is where datui says it does not read the table: {said:?}"
    );
}

/// `hive: true` is what puts the open on the local directory route at all: without it a
/// directory is `Unsupported file type`, and the whole of `directory_format`'s dispatch
/// is behind it. The door row builds its own open, so nothing else pins the flag.
#[test]
fn test_the_door_opens_a_directory_by_the_directory_route() {
    let tmp = tempfile::tempdir().expect("tempdir");
    std::fs::write(tmp.path().join("a.csv"), b"x,y\n1,2\n").unwrap();
    std::fs::write(tmp.path().join("b.csv"), b"x,y\n3,4\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the row");
    app.home.selected = row;

    match app.event(&key(KeyCode::Enter)) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![tmp.path().to_path_buf()]);
            assert!(
                options.hive,
                "without this the open is `Unsupported file type`"
            );
        }
        _ => panic!("Enter on the door should open the directory"),
    }
}

/// → goes inside a row nothing has looked into yet.
///
/// On a share that is most rows: `entry_for_path` calls a remote path with no data
/// extension `Unknown`, and a listing looks into nothing. Excluding `Unknown` from the
/// door would put the directories that cost most to reach back behind a classification —
/// the label deciding access again, one indirection along. A remote file with an odd
/// extension is browsed into and shows an empty listing, which `Esc` backs out of.
#[test]
fn test_right_goes_inside_a_row_nothing_has_looked_into() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let directory = tmp.path().join("archive");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("one.parquet"), b"x").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "archive"))
        .expect("the directory is listed");
    app.home.selected = row;
    // Deliberately not classified: this is what a listing hands over before anything
    // has looked into it, and what every row on a share looks like.
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Unknown),
    );

    app.event(&key(KeyCode::Right));
    assert_eq!(
        app.home.browsing.as_deref(),
        Some(directory.as_path()),
        "→ went inside without needing to know what it is first"
    );
}

/// Each route's directories are classified in that route's vocabulary.
///
/// The door used to ask the cloud classifier about a local directory, which got two
/// answers wrong in opposite directions. `scan_dir` drops dotted names, so `.hoodie`
/// never reached it and a local Hudi table came back `MultiFile` — the door then read
/// its tombstones, two keystrokes after the row above said datui does not read Hudi
/// tables yet. And the cloud Iceberg rule is the looser of the two on purpose, names
/// only, so a plain directory holding `data/` beside `metadata/` was refused as a lake
/// table it is not: the second door closing on a false verdict, which is the whole
/// thing phase 3 exists to stop.
#[test]
fn test_the_door_reads_a_local_directory_with_the_local_rules() {
    let tmp = tempfile::tempdir().expect("tempdir");

    // A Hudi table: the marker is a dotted name.
    let trips = tmp.path().join("trips");
    std::fs::create_dir_all(trips.join(".hoodie")).unwrap();
    std::fs::write(trips.join(".hoodie").join("hoodie.properties"), b"x").unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for part in ["part-0.parquet", "part-1.parquet"] {
        let file = std::fs::File::create(trips.join(part)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    // And a plain directory that merely looks like an Iceberg table by its names.
    let project = tmp.path().join("project");
    std::fs::create_dir_all(project.join("data")).unwrap();
    std::fs::create_dir_all(project.join("metadata")).unwrap();
    std::fs::write(project.join("data").join("a.parquet"), b"x").unwrap();
    std::fs::write(project.join("metadata").join("notes.md"), b"x").unwrap();

    let door_kind = |dir: &std::path::Path| {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.browsing = Some(dir.to_path_buf());
        app.home.rebuild(&[], &[]);
        app.home
            .visible()
            .iter()
            .find_map(|r| match r {
                datui::home::Row::Door { entry, .. } => Some(entry.kind),
                _ => None,
            })
            .expect("the directory carries the row")
    };

    assert_eq!(
        door_kind(&trips),
        datui::discover::EntryKind::Hudi,
        "a dotted marker is not in the listing, so only the local rule can see it"
    );
    assert_eq!(
        door_kind(&project),
        datui::discover::EntryKind::Directory,
        "two directory names are not an Iceberg table: the local rule wants a \
         .metadata.json in one of them"
    );
}

/// A prefix in an object store is read with the reader its own listing calls for.
///
/// Every cloud path went to `scan_parquet` whatever was under it, so a prefix of CSV
/// answered "Could not read from S3. Check credentials and URL" — a false statement
/// about a login that is fine. The listing has already counted what is there and it is
/// on screen, so picking the reader from it costs no request. What is left refused is
/// a prefix holding nothing datui has a multi-file reader for, and that refusal names
/// what is there rather than blaming the connection.
///
/// A fresh app per shape, because opening sets `busy` and the next key would be read
/// against a screen that is no longer the home screen.
#[cfg(feature = "cloud")]
#[test]
fn test_the_cloud_door_reads_a_prefix_with_the_reader_its_listing_calls_for() {
    use datui::discover::{Entry, EntryKind};
    use std::path::PathBuf;

    // Press Enter on the door of a prefix holding these names, and say what happened.
    // A name with a dot in it stands for an object, the rest for sub-prefixes; a name
    // with an `=` in it is a partition, the way a listing hands one over.
    fn door(prefix: &str, names: &[&str]) -> (Option<OpenOptions>, String) {
        let place = PathBuf::from(prefix);
        let rows: Vec<Entry> = names
            .iter()
            .map(|name| {
                let mut entry = Entry::directory(&place.join(name));
                entry.name = (*name).to_string();
                if name.contains('.') {
                    entry.kind = EntryKind::File;
                    entry.size = Some(1_000);
                }
                entry
            })
            .collect();

        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.network_check = |_| true;
        app.home.probe_ready(place.clone(), rows);
        app.home.browsing = Some(place);
        app.home.rebuild(&[], &[]);
        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Door { .. }))
            .expect("the prefix carries the row");
        app.home.selected = row;
        let event = app.event(&key(KeyCode::Enter));
        let options = match event {
            Some(AppEvent::Open(_, options)) => Some(options),
            _ => None,
        };
        (options, app.home.status.clone().unwrap_or_default())
    }

    // Data files, none of them Parquet: read as what they are.
    let (options, said) = door("s3://bucket/exports", &["a.csv", "b.csv", "c.csv"]);
    let options = options.expect("a prefix of CSV is a prefix datui can read");
    assert_eq!(options.format, Some(datui::FileFormat::Csv));
    assert!(
        !said.to_lowercase().contains("credential"),
        "and nothing blames a login that is fine: {said:?}"
    );

    // Two formats, neither Parquet: the commonest is the reader, and the rest is said.
    // `label()` would call this `mixed`, a word rather than a count.
    let (options, _) = door("s3://bucket/pair", &["a.csv", "a2.csv", "b.json"]);
    let options = options.expect("a prefix of mostly CSV reads as CSV");
    assert_eq!(options.format, Some(datui::FileFormat::Csv));
    assert_eq!(
        options.left_out,
        vec![(datui::FileFormat::Json, 1)],
        "and the dataset can say what it passed over"
    );

    // Nothing datui has a reader for. `holds.formats` is empty here, so a test written
    // over the formats alone let it through and the scan came back blaming the login.
    let (options, said) = door("s3://bucket/docs", &["README.md", "notes.txt"]);
    assert!(options.is_none());
    assert!(said.contains("nothing datui can read"), "{said:?}");
    assert!(!said.to_lowercase().contains("credential"), "{said:?}");

    // Parquet opens by its own route, which is the only one with hive partitioning
    // behind it and the one every cloud dataset took before any of this. The listing
    // already calls this prefix a dataset, so no reader is named and the scan makes the
    // Parquet call it always made. Without this case, a change that named a reader for
    // everything would pass every other assertion here.
    let (options, _) = door("s3://bucket/parts", &["part-0.parquet", "part-1.parquet"]);
    assert_eq!(
        options.map(|o| o.format),
        Some(None),
        "a prefix of Parquet is what a cloud directory reads as"
    );

    // No data files at all: tried, because the files below may be Parquet and nothing
    // here has looked.
    let (options, _) = door("s3://bucket/warehouse", &["by_year", "by_station"]);
    assert!(
        options.is_some(),
        "nothing counted directly inside is not a reason to refuse"
    );

    // Including with unreadable files beside the sub-prefixes: a README at the top says
    // nothing about what is under `by_year/`.
    let (options, _) = door("s3://bucket/warehouse2", &["README.md", "by_year"]);
    assert!(
        options.is_some(),
        "a sub-prefix may hold Parquet, and nothing here has looked"
    );

    // And a hive root with one stray data file beside its partitions. `formats` holds
    // only the stray, so picking the reader from it would read the whole root as CSV —
    // a prefix the listing already calls a dataset keeps the route its label named.
    let (options, said) = door(
        "s3://bucket/events",
        &["date=2024-01-01", "date=2024-01-02", "manifest.csv"],
    );
    assert_eq!(
        options.map(|o| o.format),
        Some(None),
        "a hive root is read through its partitions, not as the stray beside them: {said:?}"
    );
}

/// The `N datasets` caption counts what is listed, not the way out of the directory.
///
/// The door's kind is the directory's, so it counts as a dataset — and it is the same
/// dataset as the directory it opens, counted a second time, in the figure whose own
/// comment says counting a place-to-look makes it a lie.
#[test]
fn test_the_caption_does_not_count_the_door_as_a_dataset() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let parts = tmp.path().join("parts");
    std::fs::create_dir_all(&parts).unwrap();
    let mut frame = polars::prelude::DataFrame::new(
        1,
        vec![polars::prelude::Column::new("id".into(), &[1i32])],
    )
    .unwrap();
    for name in ["part-0.parquet", "part-1.parquet", "part-2.parquet"] {
        let file = std::fs::File::create(parts.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(parts);
    app.home.rebuild(&[], &[]);
    app.home.classify_now(16);

    let area = Rect::new(0, 0, 200, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let bar: String = (0..area.width)
        .map(|x| buf[(x, area.height - 1)].symbol().to_string())
        .collect();
    assert!(
        bar.contains("3 datasets"),
        "three files, and the door is not a fourth: {bar:?}"
    );
}

/// #275's done-when for this phase: from a directory the rule turns away, one table is
/// still reachable in two keystrokes.
///
/// A directory whose files each bring a column the other lacks is a place to look inside
/// rather than a dataset — that is `is_nested`, and it is stricter than the containment
/// threshold it replaced. What makes a strict rule affordable is the other door: `→`
/// steps in, and the first row in there opens the union anyway.
#[test]
fn test_a_directory_the_nesting_rule_turns_away_is_still_two_keys_from_one_table() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let sales = tmp.path().join("sales");
    std::fs::create_dir_all(&sales).unwrap();
    for (name, last) in [("old.parquet", "amount"), ("new.parquet", "amt")] {
        let mut frame = polars::prelude::DataFrame::new(
            1,
            vec![
                polars::prelude::Column::new("id".into(), &[1i32]),
                polars::prelude::Column::new(last.into(), &[2i32]),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(sales.join(name)).unwrap();
        polars::prelude::ParquetWriter::new(file)
            .finish(&mut frame)
            .unwrap();
    }

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == "sales"))
        .expect("the directory is listed");
    app.home.selected = row;
    app.home.classify_now(8);
    app.home.measure_now(8);
    assert_eq!(
        app.home.selected_entry().map(|e| e.kind),
        Some(datui::discover::EntryKind::Directory),
        "neither file's columns are in the other's"
    );

    // One key in.
    app.event(&key(KeyCode::Right));
    assert_eq!(app.home.browsing.as_deref(), Some(sales.as_path()));
    app.home.rebuild(&[], &[]);

    // The second key opens the union.
    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the row");
    app.home.selected = row;
    assert!(
        matches!(app.event(&key(KeyCode::Enter)), Some(AppEvent::Open(..))),
        "the door opens what the rule declined to open in one key"
    );
}

/// `datui <dir>` does what `Enter` on that directory's row does.
///
/// A directory used to be `Unsupported file type` unless `--hive` was passed, while
/// pyarrow, Polars, pandas and Spark all open one. Naming a directory is the request to
/// read it, so the command line answers the same as the other two doors onto a path.
#[test]
fn test_the_command_line_reads_a_directory_the_way_enter_does() {
    let tmp = tempfile::TempDir::new().unwrap();
    let parquet = |dir: &Path, name: &str, mut frame: DataFrame| {
        std::fs::create_dir_all(dir).unwrap();
        ParquetWriter::new(File::create(dir.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    };
    let table = |cols: &[&str]| {
        DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap()
    };

    // An app and the channel its background work answers on. The look at a directory is
    // an event now, not a call, so the test drives the same chain `run()` does.
    let app = || {
        let (tx, rx) = mpsc::channel();
        (App::new(tx, common::test_runtime()), rx)
    };
    let named_with =
        |app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path, options: OpenOptions| {
            let mut next = Some(AppEvent::OpenNamed(vec![dir.to_path_buf()], options));
            // Follow the chain to whatever it settles on: the look goes to a worker and
            // answers here, and what it answers with is the decision. Settling on the
            // home screen is an outcome, not a timeout — a helper that could only tell
            // the two apart by waiting would make every such case cost the wait, and
            // would quietly pass a chain that had stopped.
            loop {
                if app.home.browsing.is_some() {
                    return None;
                }
                match next.take() {
                    Some(AppEvent::Open(paths, options)) => {
                        return Some(AppEvent::Open(paths, options));
                    }
                    Some(ev) => next = app.event(&ev),
                    None => match next_event(app, rx) {
                        Some(ev) => next = Some(ev),
                        None => panic!("the chain stopped without settling on anything"),
                    },
                }
            }
        };
    // The options the binary really passes, not `OpenOptions::default()`. They differ
    // in exactly the fields a guard over "did the user set anything" reads — the CLI
    // fills in `infer_schema_length` and `parse_strings` on every run with no flags —
    // so a test built on the defaults cannot see a guard that fires on every open.
    let as_the_binary_does = |dir: &Path| {
        use clap::Parser;
        let args =
            datui_cli::Args::try_parse_from(["datui", dir.to_str().unwrap()]).expect("parses");
        OpenOptions::from_args_and_config(&args, &datui::config::AppConfig::default())
    };
    let named = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, dir: &Path| {
        let options = as_the_binary_does(dir);
        named_with(app, rx, dir, options)
    };

    // One table across several files: read as one, on the directory route.
    let one = tmp.path().join("one");
    parquet(&one, "a.parquet", table(&["id", "ts"]));
    parquet(&one, "b.parquet", table(&["id", "ts"]));
    let (mut a, rx_a) = app();
    match named(&mut a, &rx_a, &one) {
        Some(AppEvent::Open(paths, options)) => {
            assert_eq!(paths, vec![one.clone()]);
            assert!(
                options.hive,
                "the look-then-open route is what reads a directory"
            );
        }
        _ => panic!("a directory of one table opens as one table"),
    }

    // Separate tables: a place to look inside, browsed into rather than refused. The
    // `(all files)` row in there is the keystroke that unions them anyway.
    let several = tmp.path().join("several");
    parquet(&several, "by_block.parquet", table(&["block", "fee"]));
    parquet(
        &several,
        "daily.parquet",
        table(&["day", "price", "volume"]),
    );
    let (mut b, rx_b) = app();
    assert!(
        named(&mut b, &rx_b, &several).is_none(),
        "a directory of separate tables is somewhere to look, not a refusal"
    );
    assert_eq!(b.home.browsing.as_deref(), Some(several.as_path()));
    assert_eq!(b.input_mode, InputMode::Home);

    // A hive root reads as one table too, and still by the directory route.
    let hive = tmp.path().join("hive");
    parquet(&hive.join("day=1"), "part.parquet", table(&["id"]));
    parquet(&hive.join("day=2"), "part.parquet", table(&["id"]));
    let (mut c, rx_c) = app();
    assert!(
        matches!(named(&mut c, &rx_c, &hive), Some(AppEvent::Open(_, o)) if o.hive),
        "a hive root is read through its partitions"
    );

    // A lake root is not a directory of Parquet files, however much it looks like one.
    let delta = tmp.path().join("delta");
    std::fs::create_dir_all(delta.join("_delta_log")).unwrap();
    std::fs::write(
        delta.join("_delta_log").join("00000000000000000000.json"),
        "{}",
    )
    .unwrap();
    parquet(&delta, "part-00000.parquet", table(&["id"]));
    let (mut d, rx_d) = app();
    assert!(
        named(&mut d, &rx_d, &delta).is_none(),
        "it is not read as Parquet"
    );
    assert_eq!(d.home.browsing.as_deref(), Some(delta.as_path()));
    assert!(
        lake_heading(&mut d).contains("delta"),
        "and its heading says why"
    );

    // And the read really happens: the whole chain, look and all, is the table.
    let (mut loaded, rx) = app();
    let event = named(&mut loaded, &rx, &one).expect("a directory of one table opens");
    let AppEvent::Open(paths, options) = event else {
        panic!("the rule opens it")
    };
    pump_open_until_loaded(&mut loaded, &rx, paths, options);
    assert_eq!(
        loaded.data_table_state.as_ref().map(|s| s.num_rows()),
        Some(2),
        "one row from each file, read as one table"
    );

    // A file is untouched, and not even looked at: it goes straight to the open.
    assert!(matches!(
        App::route_named_paths(vec![one.join("a.parquet")], OpenOptions::default()),
        AppEvent::Open(..)
    ));
    // `--hive` is an answer already given, so it is not second-guessed either.
    let forced = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    assert!(
        matches!(
            App::route_named_paths(vec![several.clone()], forced),
            AppEvent::Open(..)
        ),
        "--hive still means read this as one, whatever the directory looks like"
    );

    // And so is `--no-header`. The rule takes each file's first row of data for its
    // column names, finds them all different and calls the directory separate tables — so
    // without this it sends the user to the home screen, for a directory the flag reads
    // perfectly as one table.
    let headless = tmp.path().join("headless");
    std::fs::create_dir_all(&headless).unwrap();
    std::fs::write(headless.join("a.csv"), "alice,30\nbob,25\n").unwrap();
    std::fs::write(headless.join("b.csv"), "carol,41\n").unwrap();
    let (mut g, rx_g) = app();
    let told = OpenOptions {
        has_header: Some(false),
        ..OpenOptions::default()
    };
    assert!(
        named_with(&mut g, &rx_g, &headless, told).is_some(),
        "the user has said how to read these; datui does not judge them by another reading"
    );
}

/// A directory of several formats is read as the commonest, and says what it left out.
///
/// Refusing the whole of a thousand CSVs over one stray JSON was datui deciding that a
/// directory it could read was not worth reading. It reads it now — and a read that
/// silently drops a file is the other half of the same mistake, so the dataset says
/// which formats were passed over and how many of each.
#[test]
fn test_a_mixed_directory_reads_as_the_commonest_format_and_says_what_it_left_out() {
    let tmp = tempfile::TempDir::new().unwrap();
    let dir = tmp.path();
    for name in ["a.csv", "b.csv", "c.csv"] {
        std::fs::write(dir.join(name), b"x,y\n1,2\n").unwrap();
    }
    std::fs::write(dir.join("notes.json"), b"{\"x\": 1}").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![dir.to_path_buf()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );

    let state = app
        .data_table_state
        .as_ref()
        .expect("the directory opens rather than being refused over the stray");
    assert_eq!(state.num_rows(), 3, "one row from each CSV");

    let notes = state.notes();
    let said = notes
        .iter()
        .find(|n| n.summary.contains("more than one format"))
        .unwrap_or_else(|| panic!("the read says what it left out, got {notes:?}"));
    assert!(
        said.summary.contains("1 json"),
        "by format and count: {:?}",
        said.summary
    );
    assert!(
        state.has_notes(),
        "and the Info key offers it, which is the only way anyone finds out"
    );
}

/// A remote row datui has no reader for is named a file, not left Unknown.
///
/// `entry_for_path` had a name and nothing else to go on, and left anything whose
/// extension it did not recognize as `Unknown` — which → enters. So a Recent of
/// `s3://bucket/data.dat` took → into an empty prefix listing with no explanation and
/// Esc as the only way out (#283). Excluding `Unknown` from what → enters was the other
/// way to fix it, and it is the label deciding access one indirection along: before
/// anything has looked into it, every row on a share is `Unknown`, including every
/// directory that costs most to reach. So the row is named instead.
#[test]
fn test_a_remote_name_datui_cannot_read_is_still_a_file_not_a_prefix() {
    let kind = |url: &str| {
        let mut home = datui::home::HomeState {
            network_check: |_| true,
            ..Default::default()
        };
        home.rebuild(&[], &[PathBuf::from(url)]);
        home.sections
            .iter()
            .flat_map(|s| s.rows.iter())
            .find(|r| r.path == Path::new(url))
            .map(|r| r.kind)
            .unwrap_or_else(|| panic!("{url} is listed under RECENT"))
    };

    // An extension datui reads: a file, as it always was.
    assert_eq!(
        kind("s3://bucket/data.psv"),
        datui::discover::EntryKind::File
    );
    // One it does not: still a file. It is certainly not a prefix.
    assert_eq!(
        kind("s3://bucket/data.dat"),
        datui::discover::EntryKind::File,
        "→ must not offer to go inside it"
    );
    // No extension: genuinely ambiguous — it may be a prefix, or a part file written
    // without one — so it stays Unknown and → goes in, which is the trade #279 made.
    assert_eq!(
        kind("s3://bucket/exports"),
        datui::discover::EntryKind::Unknown
    );
    // A trailing slash is a prefix whatever the name has in it.
    assert_eq!(
        kind("s3://bucket/2024.01.15/"),
        datui::discover::EntryKind::Unknown,
        "a dotted prefix is not a file"
    );
}

/// A directory of files written without extensions opens as one table.
///
/// Spark and GBIF both write part files with no extension. `occurrence.parquet/000001` is
/// read by its directory's name; the same files under a directory named anything else
/// were not data at all as far as datui was concerned — nothing listed them and nothing
/// opened them. The bytes say what the names do not. On a local disk a listing asks them
/// too, a few bytes a file, so the row agrees with the open.
#[test]
fn test_a_directory_of_files_written_without_extensions_still_opens() {
    let tmp = tempfile::TempDir::new().unwrap();
    let parts = tmp.path().join("parts");
    std::fs::create_dir_all(&parts).unwrap();
    let mut frame = DataFrame::new(1, vec![Column::new("id".into(), &[1i32])]).unwrap();
    for name in ["000000", "000001"] {
        ParquetWriter::new(File::create(parts.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    }

    // No name in there says data, and the signatures do.
    assert_eq!(
        datui::discover::classify_directory(&parts),
        datui::discover::EntryKind::MultiFile,
        "the bytes say one Parquet table"
    );

    // And the read finds them anyway.
    match datui::discover::directory_format(&parts) {
        datui::discover::DirectoryFormat::One(format, files) => {
            assert_eq!(format, datui::FileFormat::Parquet);
            assert_eq!(files.len(), 2, "both of them");
        }
        other => panic!("the bytes say Parquet, got {other:?}"),
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![parts.clone()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    assert_eq!(
        app.data_table_state.as_ref().map(|s| s.num_rows()),
        Some(2),
        "one row from each part"
    );

    // And it is reachable from the home screen: the directory is a place to look inside,
    // and the door inside it reads the whole of what it holds. Two keys, which is the
    // rule for every directory the nesting test turns away — not a dead end, which is
    // what a directory nothing listed and nothing opened was.
    let (tx, _rx) = mpsc::channel();
    let mut home = App::new(tx, common::test_runtime());
    home.enter_home();
    home.home.browsing = Some(parts.clone());
    home.home.rebuild(&[], &[]);
    let door = home
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the door");
    home.home.selected = door;
    assert!(
        matches!(home.event(&key(KeyCode::Enter)), Some(AppEvent::Open(..))),
        "the door opens it"
    );

    // A directory whose names do say something is not opened file by file to find out.
    // `LICENSE` beside the Parquet is not sniffed, and not read.
    let named = tmp.path().join("named");
    std::fs::create_dir_all(&named).unwrap();
    ParquetWriter::new(File::create(named.join("a.parquet")).unwrap())
        .finish(&mut frame)
        .unwrap();
    std::fs::write(named.join("LICENSE"), b"MIT").unwrap();
    match datui::discover::directory_format(&named) {
        datui::discover::DirectoryFormat::One(datui::FileFormat::Parquet, files) => {
            assert_eq!(files.len(), 1, "the LICENSE is not one of them");
        }
        other => panic!("the names settled it, got {other:?}"),
    }
}

/// The nesting rule reaches the formats that have no footer.
///
/// A directory of forty unrelated CSVs was labelled `40 csv`, `Enter` promised one table
/// because nothing had looked, and the read then refused it — the permissive rule with
/// the strict reader, which is the pairing #275 exists to stop. The names at the front
/// of a CSV are the same evidence `is_nested` takes from a Parquet footer, so the same
/// rule now answers for both.
#[test]
fn test_a_directory_of_csv_is_judged_by_its_headers_like_one_of_parquet() {
    let tmp = tempfile::TempDir::new().unwrap();
    let directory = |name: &str, files: &[&str]| {
        let dir = tmp.path().join(name);
        std::fs::create_dir_all(&dir).unwrap();
        for (i, body) in files.iter().enumerate() {
            std::fs::write(dir.join(format!("part-{i}.csv")), body).unwrap();
        }
        dir
    };
    let looked_at = |dir: &Path| {
        let mut entry = datui::discover::Entry::directory(dir);
        entry.kind = datui::discover::EntryKind::Unknown;
        datui::home::look_into(&entry)
    };

    // Files that agree: one table, as before.
    let same = directory("same", &["a,b\n1,2\n", "a,b\n3,4\n", "a,b\n5,6\n"]);
    assert_eq!(
        looked_at(&same).kind,
        datui::discover::EntryKind::MultiFile,
        "identical headers are one table"
    );

    // A column added along the way: still one table. This is the shape the rule is for,
    // and the one a stricter test would refuse.
    let drift = directory(
        "drift",
        &["id,ts\n1,5\n", "id,ts\n2,6\n", "id,ts,region\n3,7,eu\n"],
    );
    assert_eq!(
        looked_at(&drift).kind,
        datui::discover::EntryKind::MultiFile,
        "a column added later is schema drift, not separate tables"
    );

    // Separate tables: somewhere to look inside, not one table.
    let apart = directory("apart", &["a,b\n1,2\n", "x,y,z\n3,4,5\n", "q\n9\n"]);
    let judged = looked_at(&apart);
    assert_eq!(
        judged.kind,
        datui::discover::EntryKind::Directory,
        "files that each bring something the others lack are not one table"
    );
    assert_eq!(
        judged.rows, None,
        "and no row count, which would be a sum of unrelated things"
    );
    assert!(
        judged.columns.iter().any(|c| c == "q"),
        "but the union of columns, so a column search still finds the directory: {:?}",
        judged.columns
    );

    // A headerless file gives its first row of *data* as the names, because datui
    // reads a CSV as having a header. datui cannot read such a directory as one table at
    // all without `--no-header`: every file would contribute its own first row as
    // column names and the union would be a wide sheet of nulls. So it is not offered
    // as one — and the data values are not offered as column names either.
    let headless = directory("headless", &["1,2\n3,4\n", "5,6\n", "7,8\n"]);
    let judged = looked_at(&headless);
    assert_eq!(
        judged.kind,
        datui::discover::EntryKind::Directory,
        "a directory datui can only read as nulls is not one table"
    );
    assert!(
        judged.columns.is_empty(),
        "and data values are not offered as column names: {:?}",
        judged.columns
    );
    // Opened anyway, through the door, it says what it is seeing and what to do.
    let said = datui::notes::from_the_open(
        &[],
        None,
        datui::schema_union::Disagreement {
            headerless: true,
            ..Default::default()
        },
        false,
    );
    assert!(
        said.iter()
            .any(|n| n.summary.contains("no header row") && n.summary.contains("--no-header")),
        "got {said:?}"
    );

    // Compression does not hide the header: it is still at the front of the file.
    let zipped = tmp.path().join("zipped");
    std::fs::create_dir_all(&zipped).unwrap();
    for (name, body) in [("a.csv.gz", "a,b\n1,2\n"), ("b.csv.gz", "x,y,z\n3,4,5\n")] {
        let file = File::create(zipped.join(name)).unwrap();
        let mut gz = flate2::write::GzEncoder::new(file, flate2::Compression::default());
        std::io::Write::write_all(&mut gz, body.as_bytes()).unwrap();
        gz.finish().unwrap();
    }
    assert_eq!(
        looked_at(&zipped).kind,
        datui::discover::EntryKind::Directory,
        "a gzipped CSV is judged by its header like any other"
    );

    // Two files are the fewest that can disagree; one decides nothing.
    let alone = directory("alone", &["a,b\n1,2\n"]);
    assert_eq!(
        looked_at(&alone).kind,
        datui::discover::EntryKind::Directory,
        "a directory of one data file was never a multi-file dataset"
    );
}

/// The control bar says what Enter will really do, on a row of every shape.
///
/// `WhatEnter` is a prediction the renderer reads and `home_open_selected` is the thing
/// that decides, so the two can drift. This is what stops them: one row of each shape,
/// Enter pressed on it, and the prediction checked against what actually happened.
#[test]
fn test_the_bar_says_what_enter_will_really_do() {
    let tmp = tempfile::TempDir::new().unwrap();
    let table = |cols: &[&str]| {
        DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap()
    };
    let parquet = |dir: &Path, name: &str, mut frame: DataFrame| {
        std::fs::create_dir_all(dir).unwrap();
        ParquetWriter::new(File::create(dir.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    };

    // One table across two files; separate tables; a lake root; and a plain file.
    let one = tmp.path().join("one");
    parquet(&one, "a.parquet", table(&["id", "ts"]));
    parquet(&one, "b.parquet", table(&["id", "ts"]));
    let apart = tmp.path().join("apart");
    parquet(&apart, "by_block.parquet", table(&["block", "fee"]));
    parquet(&apart, "daily.parquet", table(&["day", "price"]));
    let delta = tmp.path().join("delta");
    std::fs::create_dir_all(delta.join("_delta_log")).unwrap();
    std::fs::write(delta.join("_delta_log").join("0.json"), "{}").unwrap();
    parquet(&delta, "part-0.parquet", table(&["id"]));
    parquet(tmp.path(), "loose.parquet", table(&["id"]));

    // Each row, classified the way the background pass would, then Enter pressed on it.
    for (name, expected) in [
        ("one", datui::WhatEnter::OpensDirectory),
        ("apart", datui::WhatEnter::GoesInside),
        ("delta", datui::WhatEnter::GoesInside),
        ("loose.parquet", datui::WhatEnter::OpensFile),
    ] {
        let (tx, _rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        app.enter_home();
        app.home.browsing = Some(tmp.path().to_path_buf());
        app.home.rebuild(&[], &[]);
        app.home.measure_now(16);
        app.home.classify_now(16);
        app.home.rebuild(&[], &[]);

        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("{name} is listed"));
        app.home.selected = row;

        let predicted = app.what_enter_does();
        assert_eq!(predicted, expected, "prediction for {name}");

        let was = app.home.browsing.clone();
        let opened = matches!(app.event(&key(KeyCode::Enter)), Some(AppEvent::Open(..)));
        let went_inside = app.home.browsing != was;
        match expected {
            datui::WhatEnter::OpensDirectory | datui::WhatEnter::OpensFile => assert!(
                opened && !went_inside,
                "{name}: the bar promised an open and Enter did {opened}/{went_inside}"
            ),
            datui::WhatEnter::GoesInside => assert!(
                went_inside && !opened,
                "{name}: the bar promised to go inside and Enter did {opened}/{went_inside}"
            ),
            _ => unreachable!("no other shape is asserted here"),
        }
    }

    // The shapes that are not entries at all. Each does something different and each
    // said "Open" before, which is the wrong first impression on three more rows.
    let (tx, _rx) = mpsc::channel();
    let mut other = App::new(tx, common::test_runtime());
    other.enter_home();
    other.home.browsing = Some(tmp.path().to_path_buf());
    other.home.rebuild(&[], &[]);
    let at = |app: &mut App, want: fn(&datui::home::Row) -> bool| {
        app.home.visible().iter().position(want)
    };
    if let Some(i) = at(&mut other, |r| matches!(r, datui::home::Row::Header { .. })) {
        other.home.selected = i;
        assert_eq!(
            other.what_enter_does(),
            datui::WhatEnter::FoldsSection,
            "Enter folds a section header; it does not open anything"
        );
    }

    // And the door, which reads whatever it is standing in.
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(apart.clone());
    app.home.rebuild(&[], &[]);
    let door = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Door { .. }))
        .expect("the directory carries the door");
    app.home.selected = door;
    assert_eq!(app.what_enter_does(), datui::WhatEnter::OpensDirectory);
    assert!(matches!(
        app.event(&key(KeyCode::Enter)),
        Some(AppEvent::Open(..))
    ));
}

/// A place row under `RECENT` gets the verb its key actually has.
///
/// Enter browses into the place, which is what → does on it too. The bar read
/// `Enter Open … → Inside`: the wrong verb, plus the two-chips-for-one-outcome the
/// labelling exists to remove. Neither the key-pumping test nor the bar's own tests
/// covered a place row, because both were written over entries.
#[test]
fn test_a_place_row_says_inside_and_says_it_once() {
    let tmp = tempfile::TempDir::new().unwrap();
    let held = tmp.path().join("exports");
    std::fs::create_dir_all(&held).unwrap();
    let file = held.join("sales.csv");
    std::fs::write(&file, "a,b\n1,2\n").unwrap();

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.rebuild(&[], std::slice::from_ref(&file));

    let row = app
        .home
        .visible()
        .iter()
        .position(|r| matches!(r, datui::home::Row::Place { .. }))
        .expect("a recent under a place row");
    app.home.selected = row;

    assert_eq!(
        app.what_enter_does(),
        datui::WhatEnter::GoesInside,
        "Enter browses the place, which is what → does"
    );

    // And Enter really does browse, so the label is not a guess.
    app.event(&key(KeyCode::Enter));
    assert_eq!(app.home.browsing.as_deref(), Some(held.as_path()));
}

/// The pane does not point at a door that will not be there.
///
/// `whole_directory_row` gives no `(all files)` row to a directory with nothing in it,
/// nor to any directory while a filter is typed — and the pane said "the first row in
/// there reads the whole directory as one table" for every plain directory regardless.
#[test]
fn test_the_pane_only_promises_a_door_that_exists() {
    let tmp = tempfile::TempDir::new().unwrap();
    let empty = tmp.path().join("empty");
    std::fs::create_dir_all(&empty).unwrap();
    let full = tmp.path().join("full");
    std::fs::create_dir_all(&full).unwrap();
    std::fs::write(full.join("a.csv"), "a,b\n1,2\n").unwrap();
    std::fs::write(full.join("b.csv"), "x,y,z\n3,4,5\n").unwrap();

    let pane = |app: &mut App, name: &str| {
        let row = app
            .home
            .visible()
            .iter()
            .position(|r| matches!(r, datui::home::Row::Entry { entry, .. } if entry.name == name))
            .unwrap_or_else(|| panic!("{name} is listed"));
        app.home.selected = row;
        let area = ratatui::layout::Rect::new(0, 0, 120, 24);
        let mut buf = ratatui::buffer::Buffer::empty(area);
        ratatui::widgets::Widget::render(&mut *app, area, &mut buf);
        // The pane wraps and pads, so a sentence spans rows with a border and a run of
        // spaces in the middle. Flattened to single spaces so the text can be looked
        // for as it reads.
        let raw = (0..area.height)
            .map(|y| {
                (0..area.width)
                    .map(|x| buf[(x, y)].symbol())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join(" ");
        // `|` is the border in the ASCII glyph set.
        raw.replace(['│', '|'], " ")
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ")
    };

    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.browsing = Some(tmp.path().to_path_buf());
    app.home.rebuild(&[], &[]);
    app.home.measure_now(16);
    app.home.classify_now(16);
    app.home.rebuild(&[], &[]);

    let shown = pane(&mut app, "full");
    assert!(
        shown.contains("first row inside"),
        "a directory with something in it has the door to point at: {shown}"
    );
    assert!(
        !pane(&mut app, "empty").contains("first row inside"),
        "an empty directory has none, so nothing points at one"
    );
}

/// A read that widened a column's type says so, and one with no rule behind it does
/// not widen at all.
///
/// `to_supertypes` was added for exactly this case — one `N/A` makes `amount` a String
/// in one file and an Int64 in the next — and the note was written from the column
/// *names*, which agree. So the directory opened with `amount` silently text for every
/// row, where before it had failed loudly.
#[test]
fn test_widening_a_column_is_never_silent() {
    let tmp = tempfile::TempDir::new().unwrap();
    let drifted = tmp.path().join("drifted");
    std::fs::create_dir_all(&drifted).unwrap();
    std::fs::write(drifted.join("a.csv"), "id,amount\n1,10\n").unwrap();
    std::fs::write(drifted.join("b.csv"), "id,amount\n2,N/A\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(
        &mut app,
        &rx,
        vec![drifted.clone()],
        OpenOptions {
            hive: true,
            ..OpenOptions::default()
        },
    );
    let state = app.data_table_state.as_ref().expect("it opens");
    let notes = state.notes();
    assert!(
        notes
            .iter()
            .any(|n| n.summary.contains("more than one type")),
        "the widening is reported: {notes:?}"
    );
    assert!(
        !notes.iter().any(|n| n.summary.contains("same columns")),
        "and not as a disagreement about columns, which these files do not have: {notes:?}"
    );

    // The names agreeing is what made this invisible, so assert they do.
    assert_eq!(state.headers(), vec!["id", "amount"]);
}

/// Naming a path on the command line draws a frame before anything asks the filesystem
/// about it.
///
/// Looking at a directory reads footers, or the front of a spread of its files, and for a
/// directory of large Parquet that is seconds — 4.6 of them on a real one, and seventeen
/// on one with a deep subtree. It used to happen in `run()` before the first
/// `terminal.draw`, so the whole of it was a blank terminal: no name, no spinner, and
/// no key that worked. So did the `exists` and `is_dir` that decide whether there is
/// anything to look at, and a local-looking path can be a mount that does not answer.
/// All of it is a worker's now, after the first frame.
#[test]
fn test_looking_at_a_directory_happens_after_the_first_frame() {
    let tmp = tempfile::TempDir::new().unwrap();
    let directory = tmp.path().join("data");
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(directory.join("a.csv"), "a,b\n1,2\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let follow_up = app.event(&AppEvent::OpenNamed(
        vec![directory.clone()],
        OpenOptions::default(),
    ));
    // Handling it decided nothing: no home screen, no load, and a spinner while a
    // worker asks the filesystem.
    assert!(follow_up.is_none(), "the question goes to a worker");
    assert!(app.is_busy(), "and the screen says something is happening");
    assert!(
        app.home.browsing.is_none() && app.data_table_state.is_none(),
        "nothing has been opened or browsed into"
    );

    // The worker's answer is that a directory was named, which is looked at next.
    let mut next = None;
    while next.is_none() {
        let event = next_event(&app, &rx).expect("the worker answers");
        next = app.event(&event);
    }
    match next {
        Some(AppEvent::LookThenOpenDirectory(dir, _)) => assert_eq!(dir, directory),
        _ => panic!("a named directory is looked at before it is opened"),
    }

    // A file is not looked at at all — there is nothing to find out — so it goes
    // straight to the open.
    assert!(matches!(
        App::route_named_paths(vec![directory.join("a.csv")], OpenOptions::default()),
        AppEvent::Open(..)
    ));
}

/// A path named at startup that is not there ends the session, as it always has; the
/// check is a worker's, behind the first frame.
#[test]
fn test_a_missing_named_path_is_found_on_a_worker() {
    let tmp = tempfile::TempDir::new().unwrap();
    let missing = tmp.path().join("nope.csv");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    assert!(
        app.event(&AppEvent::OpenNamed(
            vec![missing.clone()],
            OpenOptions::default()
        ))
        .is_none()
    );
    let mut found = None;
    while found.is_none() {
        match next_event(&app, &rx).expect("the worker answers") {
            AppEvent::NamedPathMissing { path, .. } => found = Some(path),
            other => {
                app.event(&other);
            }
        }
    }
    assert_eq!(found, Some(missing));
    // A URL or a glob is the open's to judge.
    assert_eq!(
        App::missing_named_path(&[PathBuf::from("https://example.com/x.csv")]),
        None
    );
    assert_eq!(App::missing_named_path(&[tmp.path().join("*.csv")]), None);
}

/// Going home while a directory is being looked at is not undone when the look lands.
///
/// The look can take seconds, and Ctrl+O works throughout — which is the point of
/// moving it off the startup thread. So the user can be somewhere else by the time it
/// answers, and the answer must not take them back.
#[test]
fn test_a_look_that_lands_after_the_user_left_is_dropped() {
    let tmp = tempfile::TempDir::new().unwrap();
    let directory = tmp.path().join("one");
    std::fs::create_dir_all(&directory).unwrap();
    for name in ["a.csv", "b.csv"] {
        std::fs::write(directory.join(name), "id,ts\n1,2\n").unwrap();
    }

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let event = App::route_named_paths(vec![directory.clone()], OpenOptions::default());
    app.event(&event);

    // Ctrl+O while the look is out: the user is at the home screen now.
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    // The look lands. It must find nothing waiting for it.
    let mut landed = None;
    for _ in ticks() {
        if let Ok(ev) = rx.recv_timeout(std::time::Duration::from_millis(50))
            && matches!(ev, AppEvent::DirectoryLookedAt { .. })
        {
            landed = Some(app.event(&ev));
            break;
        }
    }
    let landed = landed.expect("the look reports back");
    assert!(
        landed.is_none(),
        "the answer to a question the user walked away from does not open anything"
    );
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "and does not take them off the screen they chose"
    );
    assert!(
        app.home.browsing.is_none(),
        "nor browse them into the directory they left: {:?}",
        app.home.browsing
    );
}

/// A setting that agrees with what the rule assumed is not a reason to stop asking it.
///
/// `has_header` and the skips reach `OpenOptions` from the config file as well as the
/// command line, so a guard over "did anyone set this" is true on every run for anyone
/// with `has_header = true` in `~/.config/datui/config.toml` — and every directory they
/// name is then forced down the one-table route, `datui .` included. The question is
/// whether the header is somewhere other than where the rule looked.
#[test]
fn test_a_setting_that_agrees_with_the_rule_changes_nothing() {
    let tmp = tempfile::TempDir::new().unwrap();
    let apart = tmp.path().join("apart");
    std::fs::create_dir_all(&apart).unwrap();
    std::fs::write(apart.join("a.csv"), "id,ts\n1,2\n").unwrap();
    std::fs::write(apart.join("b.csv"), "x,y,z\n3,4,5\n").unwrap();

    let settle = |options: OpenOptions| {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let mut next = Some(AppEvent::OpenNamed(vec![apart.clone()], options));
        loop {
            if app.home.browsing.is_some() {
                return None;
            }
            match next.take() {
                Some(AppEvent::Open(paths, options)) => return Some((paths, options)),
                Some(ev) => next = app.event(&ev),
                None => match next_event(&app, &rx) {
                    Some(ev) => next = Some(ev),
                    None => panic!("the chain stopped without settling"),
                },
            }
        }
    };

    // A header where one is expected, and a skip of nothing: the same thing the rule
    // assumed, so the directory of separate tables is still somewhere to look inside.
    for agrees in [
        OpenOptions {
            has_header: Some(true),
            ..OpenOptions::default()
        },
        OpenOptions {
            skip_rows: Some(0),
            skip_lines: Some(0),
            ..OpenOptions::default()
        },
    ] {
        assert!(
            settle(agrees).is_none(),
            "a setting the rule already assumed does not force the one-table route"
        );
    }

    // Moving the header does change it — not by overriding the rule, but because the
    // rule now reads the files the way the open will: headerless, every file's columns
    // are `column_1..N` and the narrower nests inside the wider.
    assert!(
        settle(OpenOptions {
            has_header: Some(false),
            ..OpenOptions::default()
        })
        .is_some(),
        "read as headerless, these files are one table"
    );
}

/// A CSV setting does not decide a directory of Parquet.
///
/// `has_header` and the skips are reader settings for delimited text. Nothing about
/// them can change how a Parquet file is read, so a directory of Parquet must reach the
/// same verdict whatever they say — which it does because the rule reads Parquet
/// through its footers and the settings never touch that path.
#[test]
fn test_a_csv_setting_does_not_decide_a_directory_of_parquet() {
    let tmp = tempfile::TempDir::new().unwrap();
    let apart = tmp.path().join("apart");
    std::fs::create_dir_all(&apart).unwrap();
    for (name, cols) in [
        ("by_block.parquet", vec!["block", "fee"]),
        ("daily.parquet", vec!["day", "price", "volume"]),
    ] {
        let mut frame = DataFrame::new(
            1,
            cols.iter()
                .map(|c| Column::new((*c).into(), &[1i32]))
                .collect(),
        )
        .unwrap();
        ParquetWriter::new(File::create(apart.join(name)).unwrap())
            .finish(&mut frame)
            .unwrap();
    }

    for options in [
        OpenOptions::default(),
        OpenOptions {
            has_header: Some(false),
            skip_rows: Some(3),
            ..OpenOptions::default()
        },
    ] {
        let (tx, rx) = mpsc::channel();
        let mut app = App::new(tx, common::test_runtime());
        let mut next = Some(AppEvent::OpenNamed(vec![apart.clone()], options));
        loop {
            if app.home.browsing.is_some() {
                break;
            }
            match next.take() {
                Some(AppEvent::Open(..)) => {
                    panic!("a CSV setting cannot make two Parquet tables into one")
                }
                Some(ev) => next = app.event(&ev),
                None => match next_event(&app, &rx) {
                    Some(ev) => next = Some(ev),
                    None => panic!("the chain stopped without settling"),
                },
            }
        }
        assert_eq!(app.home.browsing.as_deref(), Some(apart.as_path()));
    }
}

/// A directory with no data files in it is never forced down the one-table route.
///
/// `EntryKind::Directory` covers a directory of separate tables *and* one with nothing
/// readable in it. Opening the second as one table reaches the Parquet hive scan on a
/// tree that has no Parquet, so `datui ~/src --no-header` ended in an error modal
/// rather than the home screen it used to give.
#[test]
fn test_a_directory_with_no_data_is_not_forced_open() {
    let tmp = tempfile::TempDir::new().unwrap();
    let src = tmp.path().join("src");
    std::fs::create_dir_all(src.join("nested")).unwrap();
    std::fs::write(src.join("main.rs"), "fn main() {}\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let mut next = Some(AppEvent::OpenNamed(
        vec![src.clone()],
        OpenOptions {
            has_header: Some(false),
            ..OpenOptions::default()
        },
    ));
    loop {
        if app.home.browsing.is_some() {
            break;
        }
        match next.take() {
            Some(AppEvent::Open(..)) => {
                panic!("a directory with nothing readable in it has no table to open")
            }
            Some(ev) => next = app.event(&ev),
            None => match next_event(&app, &rx) {
                Some(ev) => next = Some(ev),
                None => panic!("the chain stopped without settling"),
            },
        }
    }
    assert_eq!(app.home.browsing.as_deref(), Some(src.as_path()));
}

/// Options as the binary builds them from these arguments and this config text.
fn options_as_the_binary_does(argv: &[&str], config: &str) -> OpenOptions {
    use clap::Parser;
    let args = datui_cli::Args::try_parse_from(argv).expect("parses");
    let config: datui::config::AppConfig = toml::from_str(config).expect("config parses");
    OpenOptions::from_args_and_config(&args, &config)
}

fn open_and_collect(paths: Vec<PathBuf>, options: OpenOptions) -> (App, DataFrame) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, paths, options);
    let df = app
        .data_table_state
        .as_ref()
        .expect("the file opened")
        .lf()
        .clone()
        .collect()
        .unwrap();
    (app, df)
}

fn names(df: &DataFrame) -> Vec<String> {
    df.get_column_names()
        .iter()
        .map(|n| n.to_string())
        .collect()
}

/// `--delimiter` is what the file is read with (#290), on every CSV route: one file,
/// several, and a compressed one read both lazily and in memory.
#[test]
fn test_delimiter_flag_splits_the_columns() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let body = "id|name|city\n1|ann|oslo\n2|bob|rome\n";
    let one = tmp.path().join("one.csv");
    let two = tmp.path().join("two.csv");
    std::fs::write(&one, body).unwrap();
    std::fs::write(&two, body).unwrap();
    let gz = tmp.path().join("zipped.csv.gz");
    let mut enc =
        flate2::write::GzEncoder::new(File::create(&gz).unwrap(), flate2::Compression::default());
    std::io::Write::write_all(&mut enc, body.as_bytes()).unwrap();
    enc.finish().unwrap();

    let (_, df) = open_and_collect(
        vec![one.clone()],
        options_as_the_binary_does(&["datui", "x"], ""),
    );
    assert_eq!(names(&df), ["id|name|city"], "without the flag: one column");

    let with_flag = |extra: &[&str]| {
        let mut argv = vec!["datui", "x", "--delimiter", "124"];
        argv.extend_from_slice(extra);
        options_as_the_binary_does(&argv, "")
    };
    for (what, paths, opts) in [
        ("one file", vec![one.clone()], with_flag(&[])),
        ("two files", vec![one.clone(), two.clone()], with_flag(&[])),
        ("gzip, lazily", vec![gz.clone()], with_flag(&[])),
        (
            "gzip, in memory",
            vec![gz.clone()],
            with_flag(&["--decompress-in-memory"]),
        ),
    ] {
        let rows = 2 * paths.len();
        let (_, df) = open_and_collect(paths, opts);
        assert_eq!(names(&df), ["id", "name", "city"], "{what}");
        assert_eq!(df.height(), rows, "{what}");
    }
}

/// The flag outranks the separator a `.tsv` implies, and export offers it. Without the
/// flag export offers a comma, not the tab: a `.tsv` exports as CSV, to a `.csv` by
/// default, and a tab there reopens as one column.
#[test]
fn test_delimiter_flag_overrides_the_format_and_reaches_export() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let tsv = tmp.path().join("data.tsv");
    std::fs::write(&tsv, "a;b\tc\n1;2\t3\n").unwrap();

    let export_default = |app: &mut App| {
        app.event(&key(KeyCode::Char('e')));
        app.export_modal.csv_delimiter_input.value().to_string()
    };

    let (mut app, df) = open_and_collect(
        vec![tsv.clone()],
        options_as_the_binary_does(&["datui", "x"], ""),
    );
    assert_eq!(names(&df), ["a;b", "c"]);
    assert_eq!(export_default(&mut app), ",");

    let (mut app, df) = open_and_collect(
        vec![tsv],
        options_as_the_binary_does(&["datui", "x", "--delimiter", "59"], ""),
    );
    assert_eq!(names(&df), ["a", "b\tc"]);
    assert_eq!(export_default(&mut app), ";");
}

/// A file's layout in `[file_loading]` no longer reaches the open (#289). It used to
/// apply to every file, and `skip_rows = 2` turned this one's third row into its header.
#[test]
fn test_layout_keys_in_config_do_not_reach_the_open() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    let csv = tmp.path().join("plain.csv");
    std::fs::write(&csv, "id,name\n1,ann\n2,bob\n3,cid\n").unwrap();
    let config = "[file_loading]\n\
                  delimiter = 59\n\
                  has_header = false\n\
                  skip_lines = 1\n\
                  skip_rows = 2\n\
                  skip_tail_rows = 1\n";

    let opts = options_as_the_binary_does(&["datui", "x"], config);
    assert_eq!(opts.delimiter, None);
    assert_eq!(opts.has_header, None);
    assert_eq!(opts.skip_lines, None);
    assert_eq!(opts.skip_rows, None);
    assert_eq!(opts.skip_tail_rows, None);

    let (_, df) = open_and_collect(vec![csv], opts);
    assert_eq!(names(&df), ["id", "name"]);
    assert_eq!(df.height(), 3);
}

/// TSV and PSV are read by the CSV reader, so the CSV options mean the same for them:
/// trimmed names, null values, the tail skip. They used to honor only the header and
/// the leading skips.
#[test]
fn test_tsv_and_psv_take_every_csv_option() {
    common::isolate_cache();
    let tmp = tempfile::TempDir::new().unwrap();
    for (name, sep) in [("data.tsv", "\t"), ("data.psv", "|")] {
        let path = tmp.path().join(name);
        let body = format!("id{sep} name\n1{sep}NA\n2{sep}bob\n3{sep}FOOTER\n");
        std::fs::write(&path, body).unwrap();
        let opts = options_as_the_binary_does(
            &["datui", "x", "--null-value", "NA", "--skip-tail-rows", "1"],
            "",
        );
        let (_, df) = open_and_collect(vec![path], opts);
        assert_eq!(names(&df), ["id", "name"], "{name}");
        assert_eq!(df.height(), 2, "{name}");
        assert_eq!(df.column("name").unwrap().null_count(), 1, "{name}");
    }
}

/// Run the app's events, from `first`, until it is idle with nothing left to do,
/// agreeing to any download it asks about.
fn settle_from(app: &mut App, rx: &mpsc::Receiver<AppEvent>, first: AppEvent) {
    let mut next = Some(first);
    loop {
        match next.take() {
            Some(ev) => next = app.event(&ev),
            // A download is asked about first; Yes has the focus.
            None if app.awaiting_download_confirmation() => next = Some(key(KeyCode::Enter)),
            None => match next_event(app, rx) {
                Some(ev) => next = Some(ev),
                None => return,
            },
        }
    }
}

fn column_names(app: &App) -> Vec<String> {
    app.data_table_state
        .as_ref()
        .expect("a dataset")
        .schema()
        .iter_names()
        .map(|n| n.to_string())
        .collect()
}

/// `H` reads a headerless CSV's first row as data, under generated names, and back.
/// The Info notes say so first: every column name a number is a first row of data.
#[test]
fn h_turns_a_csv_header_off_and_on() {
    common::isolate_cache();
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("adult.csv");
    std::fs::write(&path, "39,77516\n50,83311\n38,215646\n").unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![path], OpenOptions::default()),
    );
    assert_eq!(column_names(&app), ["39", "77516"]);
    let notes = app.data_table_state.as_ref().unwrap().notes();
    assert!(
        notes.iter().any(|n| n.summary.contains("press H")),
        "{notes:?}"
    );

    settle_from(&mut app, &rx, key(KeyCode::Char('H')));
    assert_eq!(column_names(&app), ["column_1", "column_2"]);
    let notes = app.data_table_state.as_ref().unwrap().notes();
    assert!(
        !notes.iter().any(|n| n.summary.contains("press H")),
        "read as data, there is nothing to say: {notes:?}"
    );

    settle_from(&mut app, &rx, key(KeyCode::Char('H')));
    assert_eq!(column_names(&app), ["39", "77516"]);
}

/// A format that carries its own column names has no header to turn off.
#[test]
fn h_does_nothing_on_parquet() {
    common::ensure_sample_data();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let path = PathBuf::from("tests/sample-data/people.parquet");
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![path], OpenOptions::default()),
    );
    let before = column_names(&app);
    assert!(app.event(&key(KeyCode::Char('H'))).is_none());
    assert!(!app.is_busy());
    assert_eq!(column_names(&app), before);
}

/// Serve `body` at `http://127.0.0.1:<port>/<name>` to every request. Returns the URL
/// and a count of the GETs, which is how many downloads there were.
fn serve_over_http(
    name: &str,
    body: Vec<u8>,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    serve_over_http_stalling(name, body, None)
}

/// [`serve_over_http`], going quiet for `stall.1` after the first `stall.0` bytes of
/// each body.
fn serve_over_http_stalling(
    name: &str,
    body: Vec<u8>,
    stall: Option<(usize, std::time::Duration)>,
) -> (String, std::sync::Arc<std::sync::atomic::AtomicUsize>) {
    use std::io::{Read, Write};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/{name}", listener.local_addr().unwrap());
    let fetched = Arc::new(AtomicUsize::new(0));
    let counter = fetched.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut head = Vec::new();
            let mut byte = [0u8; 1];
            while !head.ends_with(b"\r\n\r\n") && stream.read(&mut byte).unwrap_or(0) == 1 {
                head.push(byte[0]);
            }
            let get = head.starts_with(b"GET");
            if get {
                counter.fetch_add(1, Ordering::SeqCst);
            }
            let _ = write!(
                stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\n\
                 Content-Length: {}\r\nConnection: close\r\n\r\n",
                body.len(),
            );
            if !get {
                continue;
            }
            let (first, rest) = body.split_at(stall.map_or(0, |(at, _)| at.min(body.len())));
            let _ = stream.write_all(first).and_then(|()| stream.flush());
            if let Some((_, quiet)) = stall {
                std::thread::sleep(quiet);
            }
            let _ = stream.write_all(rest);
        }
    });
    (url, fetched)
}

/// Ctrl+O while an HTTP server has gone quiet mid-body stops the download and
/// removes its file, long before the server would have sent the rest.
#[test]
fn an_abandoned_http_download_stops_while_the_server_is_silent() {
    use std::time::{Duration, Instant};
    common::isolate_cache();
    let quiet = Duration::from_secs(20);
    let mut body = b"id,name\n".to_vec();
    body.resize(64 * 1024, b'x');
    let (url, _) = serve_over_http_stalling("stalled.csv", body, Some((1024, quiet)));
    let dir = tempfile::tempdir().unwrap();
    let options = OpenOptions {
        temp_dir: Some(dir.path().to_path_buf()),
        ..OpenOptions::default()
    };
    let files = || {
        std::fs::read_dir(dir.path())
            .unwrap()
            .map(|f| f.unwrap().metadata().unwrap().len())
            .collect::<Vec<_>>()
    };

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut next = Some(AppEvent::Open(vec![PathBuf::from(&url)], options));
    while files() != [1024] {
        assert!(Instant::now() < deadline, "the first KiB never landed");
        next = match next.take() {
            Some(event) => app.event(&event),
            None if app.awaiting_download_confirmation() => Some(key(KeyCode::Enter)),
            None => rx.recv_timeout(Duration::from_millis(10)).ok(),
        };
    }

    let began = Instant::now();
    let mut next = Some(ctrl_o());
    while let Some(event) = next {
        next = app.event(&event);
    }
    while !files().is_empty() {
        assert!(
            began.elapsed() < quiet / 4,
            "still downloading: {:?}",
            files()
        );
        std::thread::sleep(Duration::from_millis(10));
    }
    // The stopped worker reports, for a load nobody is waiting on.
    loop {
        let event = rx
            .recv_timeout(Duration::from_secs(10))
            .expect("the stopped download reports");
        let failed = matches!(event, AppEvent::BackgroundFailed { .. });
        let mut next = Some(event);
        while let Some(event) = next {
            next = app.event(&event);
        }
        if failed {
            break;
        }
    }
    assert_eq!(app.error_message(), None);
}

/// A CSV over HTTP is read again from the copy already downloaded, not fetched again.
#[test]
fn h_rereads_a_download_from_the_copy_on_hand() {
    use std::sync::atomic::Ordering;
    common::isolate_cache();
    let (url, fetched) = serve_over_http("adult.csv", b"39,77516\n50,83311\n38,215646\n".to_vec());

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![PathBuf::from(&url)], OpenOptions::default()),
    );
    assert_eq!(column_names(&app), ["39", "77516"]);
    let downloads = fetched.load(Ordering::SeqCst);
    assert!(downloads >= 1, "it was downloaded");

    settle_from(&mut app, &rx, key(KeyCode::Char('H')));
    assert_eq!(column_names(&app), ["column_1", "column_2"]);
    assert_eq!(
        fetched.load(Ordering::SeqCst),
        downloads,
        "read again from the copy on hand"
    );
}

/// A compressed CSV over HTTP is downloaded and decompressed into temporary files, but
/// the dataset is the URL: the header, the Info panel and a view saved on it name that.
#[test]
fn a_compressed_csv_over_http_is_its_url() {
    use flate2::{Compression, write::GzEncoder};
    use std::io::Write;
    common::isolate_cache();
    let mut gz = GzEncoder::new(Vec::new(), Compression::default());
    gz.write_all(b"id,name\n1,a\n2,b\n").unwrap();
    let (url, _) = serve_over_http("http_gz_location.csv.gz", gz.finish().unwrap());

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    settle_from(
        &mut app,
        &rx,
        AppEvent::Open(vec![PathBuf::from(&url)], OpenOptions::default()),
    );
    assert_eq!(column_names(&app), ["id", "name"]);
    assert_eq!(app.open_path(), Some(Path::new(&url)));

    // Reread from the copy on hand, still under the URL.
    settle_from(&mut app, &rx, key(KeyCode::Char('H')));
    assert_eq!(column_names(&app), ["column_1", "column_2"]);
    assert_eq!(app.open_path(), Some(Path::new(&url)));

    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["column_1".to_string()], vec![true]);
    app.event(&key(KeyCode::Char('v')));
    app.event(&key(KeyCode::Char('s')));
    assert_eq!(
        app.template_modal.name_input.value(),
        "http_gz_location.csv"
    );
    assert_eq!(app.template_modal.exact_path_input.value(), url);
}

// ---------------------------------------------------------------------------
// Help on the home screen
// ---------------------------------------------------------------------------

/// `?` before typing opens help, and the overlay owns the keys while it is up.
/// It used to be unreachable there (`?` typed into the filter) and, opened with
/// F1, unclosable: Esc went to the home screen underneath and backed out of
/// directories behind the overlay.
#[test]
fn home_help_opens_with_question_mark_and_esc_closes_it() {
    common::isolate_cache();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    assert_eq!(app.input_mode, InputMode::Home);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert!(app.help_visible(), "? on an empty filter opens help");
    assert!(app.home.filter.is_empty(), "? must not land in the filter");

    // Keys reach the overlay, not the list underneath.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Down,
        KeyModifiers::NONE,
    )));
    assert!(app.help_visible());

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.help_visible(), "Esc closes the overlay");
    assert_eq!(
        app.input_mode,
        InputMode::Home,
        "home is still up behind it"
    );
}

/// Once a filter is being typed, `?` is an ordinary character again — a help
/// key that ate letters would break "searching for anything with a ? in it",
/// and, more importantly, the promise that typing always filters.
#[test]
fn home_question_mark_types_into_a_started_filter() {
    common::isolate_cache();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.filter = "sal".to_string();

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('?'),
        KeyModifiers::NONE,
    )));
    assert!(!app.help_visible());
    assert_eq!(app.home.filter, "sal?");
}

/// F1 opens home help even while the filter has text, and closing it leaves
/// the filter as typed.
#[test]
fn home_f1_opens_help_mid_filter() {
    common::isolate_cache();
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();
    app.home.filter = "sal".to_string();

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::F(1),
        KeyModifiers::NONE,
    )));
    assert!(app.help_visible(), "F1 opens help mid-filter");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.help_visible());
    assert_eq!(app.home.filter, "sal", "the filter survives the overlay");
}

// ---------------------------------------------------------------------------
// Views: V falls back to the list, and the modal never outlives the dataset
// ---------------------------------------------------------------------------

/// With no view whose criteria match the open dataset, V opens the views
/// list instead of silently applying the best-scored stranger (scores carry
/// usage and recency, so some view always scores highest) or doing nothing.
#[test]
fn v_with_no_matching_view_opens_the_list() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("t_fallback.csv");
    assert!(!app.template_modal.active);

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('V'),
        KeyModifiers::SHIFT,
    )));
    assert!(
        app.template_modal.active,
        "V without a match shows what exists rather than staying silent"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(!app.template_modal.active);
}

/// The template modal keys and renders off its own `active`, not the input mode,
/// so Ctrl+O must take it down: left up, it came back over the next dataset as a
/// zombie that swallowed keys.
#[test]
fn template_modal_does_not_survive_going_home() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("t_zombie.csv");

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('v'),
        KeyModifiers::NONE,
    )));
    assert!(app.template_modal.active);

    app.enter_home();
    assert!(
        !app.template_modal.active,
        "going home closes the template modal"
    );
}

/// A helper for the modal tests: one key press with no modifiers.
fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// An edit staged in the Sort & Filter modal and then canceled dies with the modal:
/// reopening `s` rebuilds it from the table's applied state, so nothing arrives
/// pre-staged and Apply commits nothing stale.
#[test]
fn test_sort_filter_esc_discards_staged_edits() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_filter_esc_discards.csv");
    let headers_before = app.data_table_state.as_ref().unwrap().headers();

    // Open the modal, walk to the column list, and hide the first column — staged only.
    press(&mut app, KeyCode::Char('s'));
    assert!(app.sort_filter_modal.active);
    press(&mut app, KeyCode::Tab); // tab bar -> body (search field)
    press(&mut app, KeyCode::Tab); // search field -> column list
    press(&mut app, KeyCode::Down); // select the first column
    press(&mut app, KeyCode::Char('v'));
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .any(|c| !c.is_visible),
        "the toggle staged a hidden column"
    );

    press(&mut app, KeyCode::Esc);
    assert!(!app.sort_filter_modal.active);

    // Reopen: the canceled hide is gone and nothing is staged as sorted.
    press(&mut app, KeyCode::Char('s'));
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.is_visible),
        "a canceled hide must not be staged on reopen"
    );
    assert!(
        app.sort_filter_modal
            .sort
            .columns
            .iter()
            .all(|c| c.sort_order.is_none())
    );

    // And Enter straight after applies and commits nothing.
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.headers(),
        headers_before,
        "Apply after a canceled edit changes nothing"
    );
    assert!(state.view_sort_columns().is_empty());
}

/// The other half of the same contract: what IS applied comes back staged. A hide
/// that was applied shows as hidden on reopen, and an applied sort shows its order.
#[test]
fn test_sort_filter_reopen_reflects_applied_state() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_filter_reopen.csv");

    // Hide the first column and apply.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab);
    // The list opens with the first column ("a") already under the cursor.
    press(&mut app, KeyCode::Char('v'));
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), vec!["c".to_string(), "name".to_string()]);

    // Sort by "c", applied through the event the modal would send.
    app.event(&AppEvent::Sort(vec!["c".to_string()], vec![false]));
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('s'));
    let staged = &app.sort_filter_modal.sort.columns;
    let a = staged.iter().find(|c| c.name == "a").unwrap();
    assert!(!a.is_visible, "the applied hide arrives staged");
    let c = staged.iter().find(|c| c.name == "c").unwrap();
    assert!(c.is_visible);
    assert_eq!(c.sort_order, Some(1), "the applied sort arrives staged");
}

/// Sort & Filter: `v` is visibility only (#379). A column hidden and shown again
/// returns to its place, in the same session or after the hide was applied, and
/// hiding never reorders the list under the cursor.
#[test]
fn showing_a_hidden_column_puts_it_back_in_place() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_unhide_in_place.csv");
    let abc = |names: &[&str]| names.iter().map(|s| s.to_string()).collect::<Vec<_>>();
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };
    let open_list = |app: &mut App| {
        press(app, KeyCode::Char('s'));
        press(app, KeyCode::Tab);
        press(app, KeyCode::Tab);
    };

    // Hide c and show it again before applying: nothing moves.
    open_list(&mut app);
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["a", "c", "name"])
    );

    // Hide c and apply; reopened, it is still listed second and comes back there.
    // The cursor stays on the second row across opens.
    open_list(&mut app);
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["a", "name"])
    );
    open_list(&mut app);
    let modal = &app.sort_filter_modal.sort;
    let under_cursor = modal.filtered_columns()[modal.table_state.selected().unwrap()]
        .1
        .name
        .clone();
    assert_eq!(under_cursor, "c", "the hidden column keeps its row");
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["a", "c", "name"])
    );

    // Hiding a leaves the rows where they were, so Down v hides c.
    open_list(&mut app);
    press(&mut app, KeyCode::Up);
    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().headers(),
        abc(&["name"])
    );
}

/// Frozen columns too wide for the window (#462): the table keeps as many frozen as
/// fit beside a usable scrolling column and breaks the rule to say so; the freeze
/// stays set, Sort & Filter still changes it at that size, and a wider window
/// freezes every column asked for again.
#[test]
fn a_freeze_survives_a_narrow_window() {
    let names = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];
    let test_data_dir = common::fixture_dir();
    let csv_path = test_data_dir.join("frozen_narrow_window.csv");
    let columns: Vec<Column> = names
        .iter()
        .map(|n| {
            let values: Vec<String> = (0..50).map(|i| format!("{n}-value-{i:03}")).collect();
            Series::new((*n).into(), values).into()
        })
        .collect();
    let mut df = DataFrame::new_infer_height(columns).unwrap();
    CsvWriter::new(&mut File::create(&csv_path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![csv_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    let g = datui::glyphs::get();
    let draw = |app: &mut App, width: u16, height: u16| {
        app.event(&AppEvent::Resize(width, height));
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        rendered_text(&buf)
    };
    let freeze_through = |app: &mut App, name: &str| {
        press(app, KeyCode::Char('s'));
        press(app, KeyCode::Tab);
        press(app, KeyCode::Tab);
        let sort = &mut app.sort_filter_modal.sort;
        let row = sort
            .filtered_columns()
            .iter()
            .position(|(_, c)| c.name == name)
            .unwrap();
        sort.table_state.select(Some(row));
        press(app, KeyCode::Char('L'));
    };
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };

    draw(&mut app, 60, 20);
    freeze_through(&mut app, "delta");
    apply(&mut app);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.locked_columns_count(), 4);

    let narrow = draw(&mut app, 60, 20);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.locked_columns_count(), 4, "the freeze is kept");
    assert!(state.frozen_shown() < 4, "{narrow}");
    assert!(narrow.contains(g.rule_broken), "{narrow}");
    // A view saved now keeps the freeze asked for, not what this window fits.
    let view = app
        .create_template_from_current_state(
            "narrow".to_string(),
            None,
            datui::template::MatchCriteria {
                exact_path: None,
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
            },
        )
        .unwrap();
    assert_eq!(view.settings.locked_columns_count, 4);

    // Sort & Filter opens and changes the freeze at this size: L on a frozen
    // column pulls the boundary back to it.
    freeze_through(&mut app, "delta");
    let with_sidebar = draw(&mut app, 60, 20);
    assert!(app.sort_filter_modal.active);
    assert!(with_sidebar.contains("Sort & Filter"), "{with_sidebar}");
    apply(&mut app);
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .locked_columns_count(),
        3
    );

    let wide = draw(&mut app, 160, 30);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.frozen_shown(), 3, "{wide}");
    assert!(wide.contains(g.rule), "{wide}");
    assert!(!wide.contains(g.rule_broken), "{wide}");
}

/// Sort & Filter (#379): a column hidden after the sidebar reordered the table
/// comes back after the column it followed there, not where the file has it; and
/// hiding the last frozen column keeps its lock for when it is shown again.
#[test]
fn a_hidden_column_returns_to_the_applied_order_and_lock() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_unhide_applied_order.csv");
    let apply = |app: &mut App| {
        if let Some(next) = press(app, KeyCode::Enter) {
            let _ = tx.send(next);
        }
        pump_until_idle(app, &rx, &tx);
    };
    let open_list = |app: &mut App| {
        press(app, KeyCode::Char('s'));
        press(app, KeyCode::Tab);
        press(app, KeyCode::Tab);
    };
    let select = |app: &mut App, name: &str| {
        let sort = &mut app.sort_filter_modal.sort;
        let row = sort
            .filtered_columns()
            .iter()
            .position(|(_, c)| c.name == name)
            .unwrap();
        sort.table_state.select(Some(row));
    };

    // name moves to the front, name and a freeze, then a is hidden.
    open_list(&mut app);
    select(&mut app, "name");
    press(&mut app, KeyCode::Char('+'));
    press(&mut app, KeyCode::Char('+'));
    select(&mut app, "a");
    press(&mut app, KeyCode::Char('L'));
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["name", "c"]);
    assert_eq!(state.locked_columns_count(), 1);

    open_list(&mut app);
    assert_eq!(
        app.sort_filter_modal.sort.get_full_column_order(),
        ["name", "a", "c"],
        "a is listed after name, where it was applied"
    );
    select(&mut app, "a");
    press(&mut app, KeyCode::Char('v'));
    apply(&mut app);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.headers(), ["name", "a", "c"]);
    assert_eq!(state.locked_columns_count(), 2, "a is frozen again");
}

/// Each column of a multi-sort runs its own way: Space cycles one column
/// none → ascending → descending, Enter applies from the list, and the header
/// carries each column's own mark. `r` still reverses the whole view.
#[test]
fn test_sort_filter_per_column_directions_reach_the_table() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_per_column.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab); // tab bar -> find
    press(&mut app, KeyCode::Tab); // find -> column list, cursor on a
    press(&mut app, KeyCode::Char(' ')); // ascending
    press(&mut app, KeyCode::Char(' ')); // descending
    press(&mut app, KeyCode::Down); // c
    press(&mut app, KeyCode::Char(' ')); // ascending
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.sort_filter_modal.active, "Enter applies and closes");

    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.view_sort_columns(),
        ["a".to_string(), "c".to_string()]
    );
    assert_eq!(state.view_sort_descending(), [true, false]);
    let df = state.lf().clone().collect().unwrap();
    let first = df.column("a").unwrap().get(0).unwrap();
    assert_eq!(first, AnyValue::Int64(99), "a runs descending");

    // The header says which way each column runs.
    let g = datui::glyphs::get();
    let area = Rect::new(0, 0, 100, 20);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let header: String = (0..area.width)
        .map(|x| buf[(x, 0)].symbol().to_string())
        .collect();
    assert!(
        header.contains(&format!("a{}", g.sort_desc)),
        "got {header:?}"
    );
    assert!(
        header.contains(&format!("c{}", g.sort_asc)),
        "got {header:?}"
    );

    // `r` flips every direction at once.
    press(&mut app, KeyCode::Char('r'));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.view_sort_descending(), [false, true]);
    let df = state.lf().clone().collect().unwrap();
    assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int64(0));
}

/// The Filters tab is driven entirely from the keyboard: Enter on the add row
/// opens the editor, typing narrows the column Picker, Enter walks the steps,
/// Ctrl+Enter applies, and `d` deletes the statement under the cursor.
#[test]
fn test_filter_editor_keyboard_flow() {
    let (mut app, rx, tx) = open_query_filter_fixture("filter_editor_flow.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Right); // Columns -> Filters
    press(&mut app, KeyCode::Tab); // tab bar -> the list (the add row)
    press(&mut app, KeyCode::Enter); // open the editor
    assert!(app.sort_filter_modal.filter.editor.is_some());
    for ch in "na".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows to "name"
    }
    press(&mut app, KeyCode::Enter); // choose the column
    for ch in "co".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows operators to "contains"
    }
    press(&mut app, KeyCode::Enter); // choose the operator
    for ch in "alpha".chars() {
        press(&mut app, KeyCode::Char(ch)); // the value
    }
    press(&mut app, KeyCode::Enter); // commit the statement
    assert!(app.sort_filter_modal.filter.editor.is_none());
    assert_eq!(app.sort_filter_modal.filter.statements.len(), 1);

    // `a` applies from the Filters tab without a modifier — Ctrl+Enter is
    // byte-identical to Enter on terminals without the kitty protocol.
    if let Some(next) = press(&mut app, KeyCode::Char('a')) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.sort_filter_modal.active);
    assert_eq!(current_rows(&app), 50, "only the alpha_ rows remain");

    // Reopen: the statement is staged; d deletes it; Ctrl+Enter clears the filter.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Up); // add row -> the statement
    press(&mut app, KeyCode::Delete); // Del deletes like d
    assert!(app.sort_filter_modal.filter.statements.is_empty());
    let apply = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::CONTROL,
    )));
    if let Some(next) = apply {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 100);
}

/// Ctrl+J applies mid-edit on every terminal, committing the row in progress,
/// and the editor's footer names it.
#[test]
fn test_ctrl_j_applies_from_the_filter_editor() {
    let (mut app, rx, tx) = open_query_filter_fixture("filter_editor_ctrl_j.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Right); // Columns -> Filters
    press(&mut app, KeyCode::Tab); // tab bar -> the list (the add row)
    press(&mut app, KeyCode::Enter); // open the editor
    for ch in "na".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Enter); // the column
    for ch in "co".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    press(&mut app, KeyCode::Enter); // the operator
    for ch in "alpha".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    let screen = screen_text(&mut app);
    assert!(screen.contains("^J") && screen.contains("Apply"));

    if let Some(next) = press_ctrl(&mut app, 'j') {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert!(!app.sort_filter_modal.active);
    assert_eq!(current_rows(&app), 50, "the row in progress was applied");
}

/// In the filter editor's column and operator steps, Space chooses like
/// Enter instead of typing into the narrow filter, where a space matches
/// nothing and blanks the list. The value field below keeps Space for typing.
#[test]
fn test_space_chooses_in_the_filter_editor_steps() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filter_editor_space.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Right); // Columns -> Filters
    press(&mut app, KeyCode::Tab); // tab bar -> the list (the add row)
    press(&mut app, KeyCode::Enter); // open the editor
    for ch in "na".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows to "name"
    }
    press(&mut app, KeyCode::Char(' ')); // chooses the column, like Enter
    for ch in "co".chars() {
        press(&mut app, KeyCode::Char(ch)); // narrows operators to "contains"
    }
    press(&mut app, KeyCode::Char(' ')); // chooses the operator
    for ch in "al pha".chars() {
        press(&mut app, KeyCode::Char(ch)); // the value types spaces as text
    }
    press(&mut app, KeyCode::Enter); // commit the statement
    assert!(app.sort_filter_modal.filter.editor.is_none());
    let statement = &app.sort_filter_modal.filter.statements[0];
    assert_eq!(statement.column, "name");
    assert_eq!(statement.value, "al pha");
}

/// Del on the Columns list is the sort's delete: the column leaves the sort
/// outright, wherever in the Space cycle it stands, and the rest renumber.
#[test]
fn test_del_removes_a_column_from_the_sort() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("del_unsorts.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab); // cursor opens on a
    press(&mut app, KeyCode::Char(' ')); // 1, ascending
    press(&mut app, KeyCode::Down); // c
    press(&mut app, KeyCode::Char(' ')); // 2
    press(&mut app, KeyCode::Char(' ')); // descending
    press(&mut app, KeyCode::Up); // back to a
    press(&mut app, KeyCode::Delete);

    let (names, directions) = app.sort_filter_modal.sort.sorted_columns_and_directions();
    assert_eq!(names, ["c"], "a left the sort and c renumbered to 1");
    assert_eq!(directions, [true], "keeping its own direction");
}

/// The first filter is reachable the obvious way: s, →, Enter. The sidebar
/// opens with focus on the tab bar, and Enter there used to apply-and-close —
/// the one path a first session actually takes.
#[test]
fn test_enter_on_the_filters_tab_bar_starts_a_filter() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filters_tab_bar_enter.csv");
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Enter);
    assert!(app.sort_filter_modal.active, "the sidebar stays open");
    assert!(
        app.sort_filter_modal.filter.editor.is_some(),
        "and the editor is up, on the add row"
    );
}

/// Esc backs out one layer at a time: an open editor dies alone, the sidebar
/// survives it, and the next Esc discards the staged edit with the sidebar.
#[test]
fn test_filter_editor_esc_ends_the_edit_not_the_sidebar() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("filter_editor_esc.csv");

    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Char('n'));
    assert!(app.sort_filter_modal.filter.editor.is_some());

    press(&mut app, KeyCode::Esc);
    assert!(
        app.sort_filter_modal.filter.editor.is_none(),
        "the edit dies"
    );
    assert!(app.sort_filter_modal.active, "the sidebar does not");
    assert!(app.sort_filter_modal.filter.statements.is_empty());

    press(&mut app, KeyCode::Esc);
    assert!(!app.sort_filter_modal.active);
}

/// The sidebar is one Surface: one border, no bordered buttons, the actions in
/// the footer, and no radio glyphs anywhere.
#[test]
fn test_sort_filter_sidebar_is_one_surface() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("sort_filter_surface.csv");
    press(&mut app, KeyCode::Char('s'));

    let area = Rect::new(0, 0, 100, 28);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect();

    assert!(rows.iter().any(|r| r.contains("Sort & Filter")));
    assert_eq!(
        rows.iter().map(|r| r.matches('╭').count()).sum::<usize>(),
        1,
        "one border on the sidebar and none inside it"
    );
    let bottom = rows
        .iter()
        .position(|r| r.contains('╰'))
        .expect("the frame closes");
    for (key, label) in [("Enter", "Apply"), ("Esc", "Cancel")] {
        assert!(
            rows[bottom - 1].contains(key) && rows[bottom - 1].contains(label),
            "{key} {label} is a chip on the footer row: {:?}",
            rows[bottom - 1]
        );
    }
    for radio in [
        datui::glyphs::unicode().radio_on,
        datui::glyphs::unicode().radio_off,
    ] {
        assert!(
            rows.iter().all(|r| !r.contains(radio)),
            "a radio glyph survived: {radio:?}"
        );
    }
}

/// Typing a path whose extension names a format moves the format radio with it, so
/// the export never writes Parquet bytes into a file named `out.csv`.
#[test]
fn test_export_format_follows_typed_extension() {
    use datui::export_modal::ExportFormat;
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_ext_follows.csv");

    press(&mut app, KeyCode::Char('e'));
    assert!(app.export_modal.active);
    app.export_modal.selected_format = ExportFormat::Parquet;

    let out = common::fixture_dir().join("export_ext_follows_out.csv");
    for ch in out.to_str().unwrap().chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    assert_eq!(
        app.export_modal.selected_format,
        ExportFormat::Csv,
        "typing a .csv path switches the format radio"
    );

    // And the export the Enter key builds carries that format.
    match press(&mut app, KeyCode::Enter) {
        Some(AppEvent::Export(datui::ExportRequest { path, format, .. })) => {
            assert_eq!(format, ExportFormat::Csv);
            assert_eq!(path, out);
        }
        _ => panic!("Enter from the path input did not build an export"),
    }
}

/// An extension that names no format leaves an explicit choice alone.
#[test]
fn test_export_unknown_extension_keeps_the_chosen_format() {
    use datui::export_modal::ExportFormat;
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_ext_unknown.csv");

    press(&mut app, KeyCode::Char('e'));
    app.export_modal.selected_format = ExportFormat::Parquet;
    for ch in "out.dat".chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    assert_eq!(app.export_modal.selected_format, ExportFormat::Parquet);
}

/// Enter applies from anywhere in the export form — there is no button to
/// walk to — and Space still toggles the checkbox under the cursor.
#[test]
fn test_export_enter_applies_from_any_row() {
    use datui::export_modal::{ExportFocus, ExportFormat};
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_enter_anywhere.csv");

    press(&mut app, KeyCode::Char('e'));
    assert!(app.export_modal.active);
    let out = common::fixture_dir().join("export_enter_anywhere_out.csv");
    for ch in out.to_str().unwrap().chars() {
        press(&mut app, KeyCode::Char(ch));
    }
    // Walk to the Include header checkbox: Path → Delimiter → Include header.
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.export_modal.focus, ExportFocus::CsvIncludeHeader);
    press(&mut app, KeyCode::Char(' '));
    assert!(!app.export_modal.csv_include_header, "Space toggles");

    match press(&mut app, KeyCode::Enter) {
        Some(AppEvent::Export(datui::ExportRequest {
            path,
            format,
            options,
            ..
        })) => {
            assert_eq!(format, ExportFormat::Csv);
            assert_eq!(path, out);
            assert!(
                !options.csv_include_header,
                "the toggled state reached the export"
            );
        }
        _ => panic!("Enter on a checkbox builds the export"),
    }
    assert!(!app.export_modal.active, "and the dialog is gone");
}

/// The export dialog is one Surface: one border, no bordered buttons, the
/// actions as chips in the footer, and no radio glyphs anywhere.
#[test]
fn test_export_modal_is_one_surface() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("export_one_surface.csv");
    press(&mut app, KeyCode::Char('e'));

    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let rows: Vec<String> = (0..area.height)
        .map(|y| {
            (0..area.width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect();

    assert!(
        rows.iter().any(|r| r.contains("Export Data")),
        "the dialog is up"
    );
    // Ratatui draws rounded corners whatever the glyph set, so one top-left
    // corner on screen means one border on the surface and none inside it.
    assert_eq!(
        rows.iter().map(|r| r.matches('╭').count()).sum::<usize>(),
        1,
        "exactly one border: {rows:#?}"
    );
    let bottom = rows
        .iter()
        .position(|r| r.contains('╰'))
        .expect("the frame closes");
    for (key, label) in [("Enter", "Export"), ("Esc", "Cancel")] {
        assert!(
            rows[bottom - 1].contains(key) && rows[bottom - 1].contains(label),
            "{key} {label} is a chip on the footer row: {:?}",
            rows[bottom - 1]
        );
    }
    // The hand-rolled radio list is gone; the picker marks the selection with
    // the rail instead.
    let radios = [
        datui::glyphs::unicode().radio_on,
        datui::glyphs::unicode().radio_off,
        datui::glyphs::ascii().radio_on,
        datui::glyphs::ascii().radio_off,
    ];
    for radio in radios {
        assert!(
            rows.iter().all(|r| !r.contains(radio)),
            "a radio glyph survived: {radio:?}"
        );
    }
}

/// The Info panel opens with the body focused, and the arrows switch tabs from
/// there too: a fresh `i` then `→` reaches the next tab without a Tab first.
#[test]
fn test_info_panel_arrows_switch_tabs_from_the_body() {
    use datui::widgets::info::InfoTab;
    let (mut app, _rx, _tx) = open_query_filter_fixture("info_arrows.csv");

    press(&mut app, KeyCode::Char('i'));
    assert!(app.info_modal.active);
    assert_eq!(app.info_modal.active_tab, InfoTab::Schema);

    press(&mut app, KeyCode::Right);
    assert_ne!(
        app.info_modal.active_tab,
        InfoTab::Schema,
        "Right switches tabs with the body focused"
    );

    press(&mut app, KeyCode::Left);
    assert_eq!(app.info_modal.active_tab, InfoTab::Schema);
}

/// The Info panel's file size and Parquet footer are read on a worker after `i`, and
/// drawn once they land; a file gone since it was opened says so instead (#457).
#[test]
fn test_info_panel_reads_the_file_facts_off_the_ui_thread() {
    use datui::widgets::info::FileFacts;
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("facts.parquet");
    let mut frame = df!("id" => (0..50i64).collect::<Vec<_>>()).unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut frame)
        .unwrap();
    let size = std::fs::metadata(&path).unwrap().len();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    let area = Rect::new(0, 0, 80, 24);
    let _ = painted(&mut app, &rx, &tx, area);
    assert!(
        app.file_facts().is_none(),
        "nothing is read before it is asked"
    );

    for k in [KeyCode::Char('i'), KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    assert!(!app.is_busy(), "the read holds no keys");
    pump_until(&mut app, &rx, &tx, |app| {
        !matches!(app.file_facts(), Some(FileFacts::Reading))
    });
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);
    assert!(
        text.contains(&datui::widgets::info::format_bytes(size)),
        "the Resources tab shows the file's size; got:\n{text}"
    );
    assert!(
        text.contains("Row groups:") && text.contains("Parquet version:"),
        "and what its footer says; got:\n{text}"
    );

    // The same file, gone before the panel asks: the next open's read fails, once.
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path.clone()], OpenOptions::default());
    let _ = painted(&mut app, &rx, &tx, area);
    std::fs::remove_file(&path).unwrap();
    for k in [KeyCode::Char('i'), KeyCode::Right] {
        if let Some(next) = app.event(&key(k)) {
            let _ = tx.send(next);
        }
    }
    pump_until(&mut app, &rx, &tx, |app| {
        !matches!(app.file_facts(), Some(FileFacts::Reading))
    });
    assert!(
        matches!(app.file_facts(), Some(FileFacts::Failed(_))),
        "a file that is gone is a failed read: {:?}",
        app.file_facts()
    );
    assert!(!app.is_busy() && !app.modal_showing());
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    let text = rendered_text(&buf);
    assert!(
        text.contains("File size:") && !text.contains("reading..."),
        "the panel stops waiting; got:\n{text}"
    );
}

/// The control bar's "of" total: none while pristine, the dataset's count under a
/// filter or query, and gone again when the filter clears. Never a fresh read — only
/// the count the pristine frame already resolved.
#[test]
fn test_total_rows_offered_only_under_a_subset() {
    use datui::filter_modal::FilterOperator;
    let (mut app, rx, tx) = open_query_filter_fixture("total_under_filter.csv");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(
        state.total_rows_when_subset(),
        None,
        "a pristine view offers no pair"
    );

    app.event(&AppEvent::Filter(vec![filter_stmt(
        "c",
        FilterOperator::Eq,
        "1",
    )]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.total_rows_when_subset(), Some(100));

    // A sort is not a subset: same rows, other order.
    app.event(&AppEvent::Filter(vec![]));
    app.event(&AppEvent::Sort(vec!["a".to_string()], vec![true]));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.total_rows_when_subset(), None);

    // A query is.
    app.event(&AppEvent::Search("select where a < 50".to_string()));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.total_rows_when_subset(), Some(100));

    app.event(&AppEvent::Reset);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.total_rows_when_subset(), None);
}

/// The accented `i` chip promises unread notes; pressing it lands on the Notes
/// tab. A second open, nothing unread, lands on Schema as before.
#[test]
fn i_opens_on_notes_while_they_are_unread() {
    use datui::widgets::info::InfoTab;

    let dir = tempfile::tempdir().unwrap();
    write_parquet(dir.path(), "date=2024-01-01", df!("id" => &[1i64]).unwrap());
    write_parquet(
        dir.path(),
        "date=2024-01-02",
        df!("id" => &[2i64], "extra" => &["x"]).unwrap(),
    );

    let mut app = open_local_dataset(dir.path());
    assert!(app.data_table_state.as_ref().unwrap().notes_unseen());

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.info_modal.active_tab,
        InfoTab::Notes,
        "unread notes put their tab in front"
    );

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('i'),
        KeyModifiers::NONE,
    )));
    assert_eq!(
        app.info_modal.active_tab,
        InfoTab::Schema,
        "read notes stay where they were; the panel opens on Schema"
    );
}

fn q_style_config() -> datui::AppConfig {
    let mut config = datui::AppConfig::default();
    config.query.default_mode = QueryMode::QStyle;
    config
}

fn press_key(app: &mut App, code: KeyCode, modifiers: KeyModifiers) {
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(code, modifiers)));
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
}

fn run_and_settle(
    app: &mut App,
    event: AppEvent,
    rx: &mpsc::Receiver<AppEvent>,
    tx: &mpsc::Sender<AppEvent>,
) {
    let mut next = app.event(&event);
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    pump_until_idle(app, rx, tx);
}

/// A new user pressing `/` gets SQL; a build without SQL opens on Search.
#[test]
fn the_query_prompt_opens_on_sql() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("prompt_default.csv");
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    #[cfg(feature = "sql")]
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    #[cfg(not(feature = "sql"))]
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Search));
}

/// `[query] default_mode` chooses where `/` opens, read from the config file.
#[test]
fn the_preferred_query_mode_is_where_the_prompt_opens() {
    let config: datui::AppConfig = toml::from_str("[query]\ndefault_mode = \"q-style\"\n").unwrap();
    let (mut app, _rx, _tx) = open_query_filter_fixture_with("prompt_preferred.csv", config);
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::QStyle));

    // Typed there, a q-style query runs as one.
    for c in "select a where a > 10".chars() {
        press_key(&mut app, KeyCode::Char(c), KeyModifiers::NONE);
    }
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_query(), "select a where a > 10");
    assert!(state.get_active_sql_query().is_empty());
}

/// Editing an active query reopens its own mode, whatever the preference:
/// q-style text is never offered up as SQL, or the other way round.
#[test]
fn reopening_the_prompt_selects_the_active_query_mode() {
    let (mut app, rx, tx) = open_query_filter_fixture("prompt_reopen_mode.csv");

    run_and_settle(
        &mut app,
        AppEvent::Search("select a where a > 10".to_string()),
        &rx,
        &tx,
    );
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::QStyle));
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), None);

    run_and_settle(
        &mut app,
        AppEvent::FuzzySearch("alpha".to_string()),
        &rx,
        &tx,
    );
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Search));
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    #[cfg(feature = "sql")]
    {
        run_and_settle(
            &mut app,
            AppEvent::SqlSearch("SELECT a FROM df WHERE a > 90".to_string()),
            &rx,
            &tx,
        );
        press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
        assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
        press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    }

    // Clearing the query returns `/` to the default.
    run_and_settle(&mut app, AppEvent::Search(String::new()), &rx, &tx);
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    assert_eq!(
        app.query_prompt_mode(),
        Some(datui::AppConfig::default().query.default_mode.resolve())
    );
}

/// Ctrl+T switches the mode from inside the input, in tab order and around,
/// and what is then typed runs in the mode on screen.
#[test]
fn ctrl_t_switches_the_query_mode() {
    let (mut app, rx, tx) = open_query_filter_fixture("prompt_chord.csv");
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    let modes = QueryMode::available();
    assert_eq!(app.query_prompt_mode(), Some(modes[0]));
    for &mode in modes[1..].iter().chain(&modes[..1]) {
        press_key(&mut app, KeyCode::Char('t'), KeyModifiers::CONTROL);
        assert_eq!(app.query_prompt_mode(), Some(mode));
    }

    while app.query_prompt_mode() != Some(QueryMode::Search) {
        press_key(&mut app, KeyCode::Char('t'), KeyModifiers::CONTROL);
    }
    for c in "alpha".chars() {
        press_key(&mut app, KeyCode::Char(c), KeyModifiers::NONE);
    }
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.get_active_fuzzy_query(), "alpha");
    assert_eq!(current_rows(&app), 50);
}

fn type_text(app: &mut App, text: &str) {
    for c in text.chars() {
        press_key(app, KeyCode::Char(c), KeyModifiers::NONE);
    }
}

#[cfg(feature = "sql")]
fn screen_at(app: &mut App, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    app.render(area, &mut buf);
    (0..height)
        .map(|y| {
            (0..width)
                .map(|x| buf[(x, y)].symbol().to_string())
                .collect::<String>()
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// Under a SQL statement the prompt lists the columns of `df` with their types,
/// narrowed to the word being typed, and Tab completes it: the one name that
/// fits, then the table name. Shift+Tab is the way to the tab bar.
#[cfg(feature = "sql")]
#[test]
fn tab_completes_column_names_in_sql() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_complete.csv");
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    let screen = screen_at(&mut app, 80, 24);
    assert!(screen.contains("Columns"), "{screen}");
    assert!(
        screen.contains("a i64") && screen.contains("name str"),
        "{screen}"
    );

    type_text(&mut app, "SELECT na");
    let screen = screen_at(&mut app, 80, 24);
    assert!(
        screen.contains("name str") && !screen.contains("a i64"),
        "{screen}"
    );
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_text(), Some("SELECT name"));
    type_text(&mut app, " FROM d");
    press_key(&mut app, KeyCode::Tab, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_text(), Some("SELECT name FROM df"));
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None, "the statement ran");
    assert_eq!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .get_active_sql_query(),
        "SELECT name FROM df"
    );

    // Shift+Tab reaches the tab bar, where ←/→ change mode.
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::BackTab, KeyModifiers::SHIFT);
    press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Search));
}

/// Alt+Enter breaks the line; Enter runs the statement, line breaks and all.
#[cfg(feature = "sql")]
#[test]
fn alt_enter_breaks_a_sql_line_and_enter_runs_it() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_newline.csv");
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    type_text(&mut app, "SELECT a FROM df");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::ALT);
    type_text(&mut app, "WHERE a < 10");
    assert_eq!(
        app.query_prompt_text(),
        Some("SELECT a FROM df\nWHERE a < 10")
    );
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql), "not run yet");
    let screen = screen_at(&mut app, 80, 24);
    assert!(
        screen.contains("SELECT a FROM df") && screen.contains("WHERE a < 10"),
        "{screen}"
    );

    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None);
    assert_eq!(current_rows(&app), 10);
}

/// A statement that plans but fails on the data keeps the prompt open with the
/// reason under it, in datui's words; the table stays as it was, no modal
/// takes the keys, and the statement can be fixed where it is.
#[cfg(feature = "sql")]
#[test]
fn a_sql_statement_that_fails_while_running_stays_in_the_prompt() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_runtime_error.csv");
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    type_text(&mut app, "SELECT CAST(name AS INT) AS n FROM df");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);

    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    assert!(!app.modal_showing(), "no modal over the prompt");
    let error = app.query_prompt_error().expect("the reason is shown");
    // Counted in the batch that failed, so "100 of 100" or, read in pieces, a floor.
    assert!(
        error.starts_with("name: 100 of 100 values are not") || error.starts_with("At least"),
        "{error}"
    );
    assert!(
        error.contains("in name are not whole numbers, such as \"alpha_0\"")
            || error.contains("values are not whole numbers, such as \"alpha_0\""),
        "{error}"
    );
    assert!(error.contains("TRY_CAST(name AS INT)"), "{error}");
    let screen = screen_at(&mut app, 80, 24);
    assert!(screen.contains("are not whole numbers"), "{screen}");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.get_active_sql_query().is_empty(), "nothing ran");
    assert_eq!(current_rows(&app), 100, "the table is as it was");

    // Fixed in place: the statement is still there to edit.
    press_key(&mut app, KeyCode::Char('a'), KeyModifiers::CONTROL);
    for _ in 0.."SELECT ".len() {
        press_key(&mut app, KeyCode::Right, KeyModifiers::NONE);
    }
    type_text(&mut app, "TRY_");
    assert_eq!(
        app.query_prompt_text(),
        Some("SELECT TRY_CAST(name AS INT) AS n FROM df")
    );
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), None);
    assert_eq!(app.query_prompt_error(), None);
    assert_eq!(current_rows(&app), 100);
}

/// A date that does not parse is said as a format problem, with the values.
#[cfg(feature = "sql")]
#[test]
fn a_date_that_does_not_parse_is_explained_in_the_prompt() {
    let (mut app, rx, tx) = open_query_filter_fixture("sql_date_error.csv");
    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    type_text(&mut app, "SELECT STRPTIME(name, '%Y-%m-%d') AS d FROM df");
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    let error = app.query_prompt_error().expect("the reason is shown");
    assert!(error.contains("match the format"), "{error}");
    assert!(error.contains("SUBSTR(name, 1, n)"), "{error}");
    // Counted in the batch the run stopped in: exact only when that was all of it.
    assert!(
        error.starts_with("name: 100 of 100 values do not match the format")
            || error.starts_with("At least"),
        "{error}"
    );
    assert!(!error.contains("strict=False"), "{error}");
}

/// #400: a query that plans but fails on its first rows is not applied. The
/// table, its schema and its row count stay those of the view before it, and a
/// sort afterwards works on that view instead of failing the same way again.
#[cfg(feature = "sql")]
#[test]
fn a_query_that_fails_when_collected_is_not_installed() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("dated.csv");
    let mut csv = String::from("id,ds,v\n");
    for i in 0..30 {
        csv.push_str(&format!("{i},2024-01-{:02},{}\n", i % 28 + 1, 30 - i));
    }
    std::fs::write(&path, csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let names = |app: &App| -> Vec<String> {
        let state = app.data_table_state.as_ref().unwrap();
        state.schema().iter_names().map(|n| n.to_string()).collect()
    };
    assert_eq!(names(&app), ["id", "ds", "v"]);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().schema().get("ds"),
        Some(&DataType::Date),
        "the repro needs ds read as a date"
    );

    press_key(&mut app, KeyCode::Char('/'), KeyModifiers::NONE);
    type_text(
        &mut app,
        "SELECT CAST(SUBSTR(ds, 1, 10) AS DATE) AS d, COUNT(*) FROM df GROUP BY d",
    );
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    pump_until_idle(&mut app, &rx, &tx);

    // The prompt says why, with the statement still there to fix.
    assert_eq!(app.query_prompt_mode(), Some(QueryMode::Sql));
    assert!(!app.modal_showing());
    let error = app.query_prompt_error().expect("the reason is shown");
    assert!(error.contains("String"), "{error}");
    // Nothing of the failed query is installed.
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(names(&app), ["id", "ds", "v"]);
    assert!(state.get_active_sql_query().is_empty());
    assert!(
        state.is_num_rows_valid(),
        "the row count is not left unknown"
    );
    assert_eq!(state.num_rows(), 30);

    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    let screen = screen_at(&mut app, 80, 24);
    assert!(!screen.contains("? rows"), "{screen}");
    assert!(screen.contains("30 rows"), "{screen}");

    // A sort works on the data as it was.
    run_and_settle(
        &mut app,
        AppEvent::Sort(vec!["v".to_string()], vec![false]),
        &rx,
        &tx,
    );
    assert!(!app.modal_showing(), "the sort does not fail");
    let state = app.data_table_state.as_ref().unwrap();
    let sorted = state.lf().clone().collect().unwrap();
    assert_eq!(sorted.height(), 30);
    assert_eq!(
        sorted.column("v").unwrap().i64().unwrap().get(0),
        Some(1),
        "sorted ascending on v"
    );
}

/// The same holds for a query sent without the prompt, as a view applies one:
/// the failure is a dialog, over the view as it was.
#[cfg(feature = "sql")]
#[test]
fn a_query_sent_without_the_prompt_that_fails_leaves_the_view() {
    let (mut app, rx, tx) = open_query_filter_fixture("query_fails_no_prompt.csv");
    run_and_settle(
        &mut app,
        AppEvent::SqlSearch("SELECT CAST(name AS INT) AS n FROM df".to_string()),
        &rx,
        &tx,
    );
    assert!(app.modal_showing(), "no prompt to put it in: a dialog");
    let state = app.data_table_state.as_ref().unwrap();
    let names: Vec<String> = state.schema().iter_names().map(|n| n.to_string()).collect();
    assert_eq!(names, ["a", "c", "name"]);
    assert!(state.get_active_sql_query().is_empty());
    assert_eq!(current_rows(&app), 100);
}

/// Reopening `/` restores the last query selected, so typing states a new
/// question instead of appending to the tail of the old one.
#[test]
fn reopening_the_query_prompt_selects_the_old_query() {
    let (mut app, rx, tx) = open_query_filter_fixture_with("reopen_query.csv", q_style_config());

    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('/'),
        KeyModifiers::NONE,
    )));
    for c in "select a where a > 10".chars() {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char(c),
            KeyModifiers::NONE,
        )));
    }
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(current_rows(&app), 89);

    // Reopen and type a fresh query: the first character replaces the old text.
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char('/'),
        KeyModifiers::NONE,
    )));
    for c in "select a where a > 50".chars() {
        app.event(&AppEvent::Key(KeyEvent::new(
            KeyCode::Char(c),
            KeyModifiers::NONE,
        )));
    }
    let mut next = app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    while let Some(ev) = next.take() {
        next = app.event(&ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        current_rows(&app),
        49,
        "typing replaced the restored query rather than appending to it"
    );
}

/// The wordmark yields on small terminals — short ones (it costs two dataset
/// rows) and narrow ones (its fourteen columns leave the path beside it all
/// ellipsis) — and the one-line title bar comes back.
#[test]
fn the_wordmark_yields_to_small_terminals() {
    let Some(wordmark) = datui::glyphs::get().wordmark else {
        return; // ASCII locale: there is no wordmark to yield.
    };
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    app.enter_home();

    let drawn = |w: u16, h: u16, app: &mut App| -> String {
        let area = Rect::new(0, 0, w, h);
        let mut buf = Buffer::empty(area);
        app.render(area, &mut buf);
        buf.content().iter().map(|c| c.symbol()).collect()
    };

    assert!(
        drawn(100, 50, &mut app).contains(wordmark[0]),
        "a big terminal gets the wordmark"
    );
    assert!(
        !drawn(100, 20, &mut app).contains(wordmark[0]),
        "a short terminal gets the rows back"
    );
    assert!(
        !drawn(36, 50, &mut app).contains(wordmark[0]),
        "a narrow terminal gives the path the columns"
    );
    assert!(
        drawn(36, 50, &mut app).contains("datui"),
        "the one-line title stands in"
    );
}

/// The copy dialog end to end: `y` opens it, the scopes copy what they say
/// through whatever destination the app holds, choices are sticky, and a
/// null copies as empty, never as the UI's glyph.
#[test]
fn test_copy_dialog_sends_each_scope_to_the_destination() {
    use datui::clipboard::{Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capture(Arc<Mutex<Vec<Payload>>>);
    impl Destination for Capture {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "test"
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("copy_test.csv");
    std::fs::write(&path, "city,pop\nOslo,700000\nParis,2100000\nQuito,\n").unwrap();

    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());

    // A frame must have been drawn for the view scope to know its rows.
    let area = Rect::new(0, 0, 120, 32);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);

    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capture(copies.clone())));

    let key =
        |app: &mut App, code| app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));

    // Row scope is the default, header off: the current row as bare TSV.
    key(&mut app, KeyCode::Char('y'));
    assert_eq!(app.input_mode, InputMode::Copy);
    assert!(app.copy_modal.active);
    key(&mut app, KeyCode::Enter);
    assert_eq!(app.input_mode, InputMode::Normal);
    assert_eq!(copies.lock().unwrap()[0].text, "Oslo\t700000");

    // The view scope: header on by default, every buffered screen row, and
    // the HTML flavor beside the TSV.
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Char(' ')); // open the scope picker
    key(&mut app, KeyCode::Down); // Row -> View
    key(&mut app, KeyCode::Enter); // choose
    key(&mut app, KeyCode::Enter); // copy
    {
        let copies = copies.lock().unwrap();
        assert_eq!(
            copies[1].text,
            "city\tpop\nOslo\t700000\nParis\t2100000\nQuito\t"
        );
        let html = copies[1].html.as_deref().expect("tsv carries html");
        assert!(html.contains("<th>city</th>"), "{html}");
    }

    // The cell scope narrows by typing: 'c' leaves only Cell, 'p' only pop.
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::Char('c'));
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Tab); // Scope -> Column
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::Char('p'));
    key(&mut app, KeyCode::Enter);
    key(&mut app, KeyCode::Enter); // copy
    assert_eq!(copies.lock().unwrap()[2].text, "700000");

    // Scope and column are sticky, and a null cell copies as empty.
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Down);
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Enter);
    assert_eq!(copies.lock().unwrap()[3].text, "");

    // The table scope collects off-thread, then lands on the same destination
    // with the header the scope defaults to.
    key(&mut app, KeyCode::Char('y'));
    key(&mut app, KeyCode::Char(' '));
    key(&mut app, KeyCode::Char('t'));
    key(&mut app, KeyCode::Enter);
    let mut next = key(&mut app, KeyCode::Enter);
    while let Some(ev) = next {
        next = app.event(&ev);
    }
    pump_until_idle(&mut app, &rx, &tx);
    {
        let copies = copies.lock().unwrap();
        assert_eq!(copies.len(), 5, "the background copy landed");
        assert_eq!(
            copies[4].text,
            "city\tpop\nOslo\t700000\nParis\t2100000\nQuito\t"
        );
    }

    // The completion is a flash on the control bar, not a modal.
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    let screen: String = buffer.content().iter().map(|cell| cell.symbol()).collect();
    assert!(screen.contains("Copied 3 rows as TSV"), "no flash drawn");
}

/// A destination with a cap, as the terminal path has, is sent text alone: no HTML
/// is built for it. A table copy over the cap is refused with the cap's message
/// and leaves the last copy in place; under it, the whole table arrives.
#[test]
fn test_a_capped_destination_gets_text_within_its_cap() {
    use datui::clipboard::{Accepts, Destination, Payload};
    use std::sync::{Arc, Mutex};

    struct Capped(Arc<Mutex<Vec<Payload>>>, usize);
    impl Destination for Capped {
        fn write(&mut self, payload: Payload) -> Result<(), String> {
            self.0.lock().unwrap().push(payload);
            Ok(())
        }
        fn describe(&self) -> &'static str {
            "terminal"
        }
        fn accepts(&self) -> Accepts {
            Accepts {
                html: false,
                base64_limit: Some(self.1),
            }
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("capped.csv");
    let mut csv = String::from("id,name\n");
    for i in 0..5_000 {
        csv.push_str(&format!("{i},name {i}\n"));
    }
    std::fs::write(&path, &csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    let area = Rect::new(0, 0, 120, 32);
    app.render(area, &mut Buffer::empty(area));
    pump_until_idle(&mut app, &rx, &tx);

    let copies: Arc<Mutex<Vec<Payload>>> = Arc::new(Mutex::new(Vec::new()));
    app.set_clipboard_destination(Box::new(Capped(copies.clone(), 4 * 1024)));

    // The view scope: TSV with no HTML beside it.
    press_key(&mut app, KeyCode::Char('y'), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Char(' '), KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Down, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    press_key(&mut app, KeyCode::Enter, KeyModifiers::NONE);
    {
        let copies = copies.lock().unwrap();
        assert!(copies[0].text.starts_with("id\tname\n0\tname 0"));
        assert!(copies[0].html.is_none(), "no HTML for a capped destination");
    }

    // The whole table is about 60 KB, over a 4 KB cap: refused, nothing sent.
    let copy_table = |app: &mut App| {
        press_key(app, KeyCode::Char('y'), KeyModifiers::NONE);
        press_key(app, KeyCode::Char(' '), KeyModifiers::NONE);
        press_key(app, KeyCode::Char('t'), KeyModifiers::NONE);
        press_key(app, KeyCode::Enter, KeyModifiers::NONE);
        press_key(app, KeyCode::Enter, KeyModifiers::NONE);
        pump_until_idle(app, &rx, &tx);
    };
    copy_table(&mut app);
    let message = app.error_message().expect("refused out loud").to_string();
    assert!(
        message.contains("over 4 KB of base64") && message.contains("osc52_limit_kb"),
        "{message}"
    );
    assert_eq!(copies.lock().unwrap().len(), 1, "the last copy stays");
    press_key(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    // Under a 1 MB cap the whole table goes, as text alone.
    app.set_clipboard_destination(Box::new(Capped(copies.clone(), 1024 * 1024)));
    copy_table(&mut app);
    assert!(app.error_message().is_none(), "{:?}", app.error_message());
    let copies = copies.lock().unwrap();
    assert_eq!(copies.len(), 2);
    assert_eq!(copies[1].text, csv.trim_end().replace(',', "\t"));
    assert!(copies[1].html.is_none());
}

/// Press `c` with Ctrl held, as a text field receives it.
fn press_ctrl(app: &mut App, c: char) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(
        KeyCode::Char(c),
        KeyModifiers::CONTROL,
    )))
}

fn screen_text(app: &mut App) -> String {
    let area = Rect::new(0, 0, 120, 30);
    let mut buffer = Buffer::empty(area);
    app.render(area, &mut buffer);
    rendered_text(&buffer)
}

/// Ctrl+U kills from the cursor back to the start of the line, keeping what
/// follows, and Ctrl+Z puts it back: readline's bindings, in the query prompt.
#[test]
fn test_query_prompt_ctrl_u_kills_to_line_start_and_ctrl_z_undoes() {
    let (mut app, rx, tx) =
        open_query_filter_fixture_with("ctrl_u_query_prompt.csv", q_style_config());

    press(&mut app, KeyCode::Char('/'));
    assert_eq!(app.input_mode, InputMode::Editing);
    for c in "select name where c = 1".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    for _ in 0.."where c = 1".len() {
        press(&mut app, KeyCode::Left);
    }

    press_ctrl(&mut app, 'u');
    let screen = screen_text(&mut app);
    assert!(
        screen.contains("where c = 1"),
        "the text after the cursor stays"
    );
    assert!(
        !screen.contains("select name"),
        "the text before it is gone"
    );

    press_ctrl(&mut app, 'z');
    assert!(screen_text(&mut app).contains("select name where c = 1"));

    // What runs is what the field holds after the undo.
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().get_active_query(),
        "select name where c = 1"
    );
    assert_eq!(current_rows(&app), 33);
}

/// The same bindings in a form field: the view save form's name.
#[test]
fn test_form_field_ctrl_u_kills_to_line_start_and_ctrl_z_undoes() {
    let (mut app, _rx, _tx) = open_query_filter_fixture("ctrl_u_form_field.csv");
    // The save gate wants something to save.
    app.data_table_state
        .as_mut()
        .unwrap()
        .sort_by(vec!["a".to_string()], vec![false]);

    press(&mut app, KeyCode::Char('v'));
    press(&mut app, KeyCode::Char('s'));
    assert_eq!(
        app.template_modal.form_focus,
        datui::widgets::template_modal::FormFocus::Name
    );
    let suggested = app.template_modal.name_input.value().to_string();
    // The suggested name is selected; End keeps it so typing extends it.
    press(&mut app, KeyCode::End);
    for c in " by a".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    for _ in 0.." by a".len() {
        press(&mut app, KeyCode::Left);
    }

    press_ctrl(&mut app, 'u');
    assert_eq!(app.template_modal.name_input.value(), " by a");
    assert_eq!(app.template_modal.name_input.cursor(), 0);

    press_ctrl(&mut app, 'z');
    assert_eq!(
        app.template_modal.name_input.value(),
        format!("{suggested} by a")
    );

    // Esc discards the form, so the test saves nothing.
    press(&mut app, KeyCode::Esc);
    assert_eq!(
        app.template_modal.mode,
        datui::widgets::template_modal::TemplateModalMode::List
    );
}

/// `0` takes a column out of the sort and stages that as a change, so Apply
/// has something to apply; a digit past the end of the order says why it did
/// nothing, on the sidebar's own status line, until the next key.
#[test]
fn test_sort_digits_stage_zero_and_explain_out_of_range() {
    let (mut app, rx, tx) = open_query_filter_fixture("sort_digits.csv");

    // Sort by the first column through its digit, and apply.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab); // tab bar -> find
    press(&mut app, KeyCode::Tab); // find -> column list
    press(&mut app, KeyCode::Char('1'));
    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert_eq!(
        app.data_table_state.as_ref().unwrap().view_sort_columns(),
        vec!["a".to_string()]
    );

    // Reopen: the applied sort arrives staged and nothing is pending.
    press(&mut app, KeyCode::Char('s'));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab);
    assert!(!app.sort_filter_modal.sort.has_unapplied_changes);

    // A digit past the end of the order does nothing and says so.
    press(&mut app, KeyCode::Char('5'));
    assert_eq!(
        app.sort_filter_modal.sort.columns[0].sort_order,
        Some(1),
        "an out-of-range digit changes nothing"
    );
    assert!(!app.sort_filter_modal.sort.has_unapplied_changes);
    assert!(
        screen_text(&mut app).contains("Position 5 is past the end; use 1."),
        "the status line says why"
    );
    assert!(!app.modal_showing(), "validation is not a modal");

    // `0` removes it and stages the change.
    press(&mut app, KeyCode::Char('0'));
    assert!(
        app.sort_filter_modal.sort.status.is_none(),
        "the next key clears the status line"
    );
    assert!(!screen_text(&mut app).contains("past the end"));
    assert_eq!(app.sort_filter_modal.sort.columns[0].sort_order, None);
    assert!(
        app.sort_filter_modal.sort.has_unapplied_changes,
        "0 is a change to apply"
    );

    if let Some(next) = press(&mut app, KeyCode::Enter) {
        let _ = tx.send(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        app.data_table_state
            .as_ref()
            .unwrap()
            .view_sort_columns()
            .is_empty(),
        "Apply took the column out of the sort"
    );
}

/// `id,key,val` in long form: ten ids, each with a `k1` and a `k2` row, values scaled by
/// `scale` so two files with the same columns give different results.
fn long_csv(scale: i64) -> String {
    let mut csv = String::from("id,key,val\n");
    for id in 0..10 {
        csv.push_str(&format!(
            "{id},k1,{}\n{id},k2,{}\n",
            id * scale,
            id * 10 * scale
        ));
    }
    csv
}

/// Run `steps` on one file and save a view matching a second; then apply the view to
/// the second file and, in another app, run the same steps on it by hand. Returns the
/// view, what applying it showed, and what the steps showed.
fn view_and_steps_on_the_next_file(
    name: &str,
    steps: &[AppEvent],
) -> (datui::Template, DataFrame, DataFrame) {
    let next_path = common::fixture_dir().join(format!("{name}_next.csv"));
    let run = |app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>| {
        for step in steps {
            app.event(step);
            pump_until_idle(app, rx, tx);
            let state = app.data_table_state.as_ref().unwrap();
            assert!(state.error().is_none(), "{:?}", state.error());
        }
    };
    let shown = |app: &App| {
        let state = app.data_table_state.as_ref().unwrap();
        state.visible_lf().collect().unwrap()
    };

    let (mut by_hand, rx, tx) = open_csv_at(&next_path, &long_csv(3), OpenOptions::default());
    run(&mut by_hand, &rx, &tx);
    let expected = shown(&by_hand);

    let (mut app, rx, tx) = open_csv_with(
        &format!("{name}_first.csv"),
        &long_csv(1),
        OpenOptions::default(),
    );
    run(&mut app, &rx, &tx);
    let template = app
        .create_template_from_current_state(
            name.to_string(),
            None,
            datui::template::MatchCriteria {
                exact_path: Some(next_path.clone()),
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
            },
        )
        .unwrap();
    pump_open_until_loaded(&mut app, &rx, vec![next_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    app.event(&key(KeyCode::Char('V')));
    pump_until_idle(&mut app, &rx, &tx);
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.error().is_none(), "{:?}", state.error());
    (template, shown(&app), expected)
}

/// A view saved after a query, a filter and then a pivot replays all three: the pivot
/// clears the query bar, but the view keeps what the pivot ran over and runs it first.
#[test]
fn test_a_view_replays_the_query_before_the_pivot() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let steps = [
        AppEvent::SqlSearch("SELECT id, key, val FROM df WHERE id >= 4".to_string()),
        AppEvent::Filter(vec![filter_stmt(
            "id",
            datui::filter_modal::FilterOperator::Lt,
            "8",
        )]),
        AppEvent::Pivot(PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "val".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: None,
        }),
    ];
    let (template, applied, expected) = view_and_steps_on_the_next_file("view_query_pivot", &steps);

    let source = template
        .settings
        .reshape_source
        .as_ref()
        .expect("the source");
    assert!(source.sql_query.is_some());
    assert_eq!(source.filters.len(), 1);
    assert_eq!(template.settings.sql_query, None);
    assert_eq!(expected.height(), 4, "ids 4..7");
    assert!(
        applied.equals_missing(&expected),
        "{applied:?}\n{expected:?}"
    );
}

/// SQL after a pivot runs on the pivot's result, so the view replays it after the
/// pivot.
#[test]
fn test_a_view_replays_sql_on_the_pivot_after_it() {
    use datui::pivot_melt_modal::{PivotAggregation, PivotSpec};
    let steps = [
        AppEvent::Pivot(PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "val".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: None,
        }),
        AppEvent::SqlSearch("SELECT id, k2 FROM df WHERE k1 > 12".to_string()),
    ];
    let (template, applied, expected) = view_and_steps_on_the_next_file("view_pivot_sql", &steps);

    assert!(template.settings.reshape_source.is_none());
    assert_eq!(expected.height(), 5, "ids 5..9");
    assert!(
        applied.equals_missing(&expected),
        "{applied:?}\n{expected:?}"
    );
}

/// The same for a melt: the query it ran over comes first.
#[test]
fn test_a_view_replays_the_query_before_the_melt() {
    use datui::pivot_melt_modal::MeltSpec;
    let steps = [
        AppEvent::SqlSearch("SELECT id, val, val * 2 AS doubled FROM df WHERE id < 3".to_string()),
        AppEvent::Melt(MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["val".to_string(), "doubled".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        }),
    ];
    let (template, applied, expected) = view_and_steps_on_the_next_file("view_query_melt", &steps);

    assert!(template.settings.reshape_source.is_some());
    assert_eq!(expected.height(), 12, "six rows, two columns each");
    assert!(
        applied.equals_missing(&expected),
        "{applied:?}\n{expected:?}"
    );
}

/// A reshape over another keeps no source: a view saved after a pivot then a melt holds
/// only the melt. Applying it fails on the next file, whose columns the melt never saw,
/// and leaves the table as it was rather than showing a melt of the wrong data.
#[test]
fn test_a_view_of_a_melted_pivot_fails_to_apply_and_changes_nothing() {
    use datui::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};
    let steps = [
        AppEvent::SqlSearch("SELECT * FROM df WHERE id >= 4".to_string()),
        AppEvent::Pivot(PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "val".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: None,
        }),
        AppEvent::Melt(MeltSpec {
            index: vec!["id".to_string()],
            value_columns: vec!["k1".to_string(), "k2".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        }),
    ];
    let next_path = common::fixture_dir().join("view_pivot_melt_next.csv");
    std::fs::write(&next_path, long_csv(3)).unwrap();
    let (mut app, rx, tx) = open_csv_with(
        "view_pivot_melt_first.csv",
        &long_csv(1),
        OpenOptions::default(),
    );
    for step in &steps {
        app.event(step);
        pump_until_idle(&mut app, &rx, &tx);
        let state = app.data_table_state.as_ref().unwrap();
        assert!(state.error().is_none(), "{:?}", state.error());
    }
    let template = app
        .create_template_from_current_state(
            "view_pivot_melt".to_string(),
            None,
            datui::template::MatchCriteria {
                exact_path: Some(next_path.clone()),
                relative_path: None,
                path_pattern: None,
                filename_pattern: None,
                schema_columns: None,
                schema_types: None,
            },
        )
        .unwrap();
    assert!(template.settings.melt.is_some());
    assert!(template.settings.reshape_source.is_none());

    pump_open_until_loaded(&mut app, &rx, vec![next_path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let before = app
        .data_table_state
        .as_ref()
        .unwrap()
        .visible_lf()
        .collect()
        .unwrap();
    app.event(&key(KeyCode::Char('V')));
    pump_until_idle(&mut app, &rx, &tx);

    assert!(app.modal_showing(), "the view says it could not apply");
    let state = app.data_table_state.as_ref().unwrap();
    assert!(state.last_melt_spec().is_none());
    let after = state.visible_lf().collect().unwrap();
    assert!(after.equals_missing(&before), "{after:?}\n{before:?}");
}
