//! Charts: the builder, what they draw, and their export.

use super::*;

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
    assert!(app.at_table());

    let key_c = KeyEvent::new(KeyCode::Char('c'), KeyModifiers::NONE);
    app.event(AppEvent::Key(key_c));
    assert_eq!(app.overlay, Overlay::Chart);
    assert!(app.overlay.shows(&Overlay::Chart));

    let key_esc = KeyEvent::new(KeyCode::Esc, KeyModifiers::NONE);
    app.event(AppEvent::Key(key_esc));
    assert!(app.at_table());
    assert!(!app.overlay.shows(&Overlay::Chart));
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
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('c'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.overlay, Overlay::Chart);

    // q does nothing in chart view (no exit)
    let key_q = KeyEvent::new(KeyCode::Char('q'), KeyModifiers::NONE);
    let out = app.event(AppEvent::Key(key_q));
    assert!(out.is_none());
    assert_eq!(app.overlay, Overlay::Chart);
}

/// The chart type switches from anywhere: 1-7 name one in order, [ and ] step,
/// and an open Picker takes digits as letters to narrow by.
#[test]
fn test_chart_type_switches_from_anywhere() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, _rx, _tx) = open_chart_view("chart_direct_type_test.csv");
    let press = |app: &mut App, c: char| {
        app.event(AppEvent::Key(KeyEvent::new(
            KeyCode::Char(c),
            KeyModifiers::NONE,
        )));
    };

    press(&mut app, '6');
    assert_eq!(app.chart.modal.mark(), Mark::Kde);
    // Deep in the panel, a number key still switches.
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Tab,
        KeyModifiers::NONE,
    )));
    press(&mut app, '4');
    assert_eq!(app.chart.modal.mark(), Mark::Histogram);
    press(&mut app, ']');
    assert_eq!(app.chart.modal.mark(), Mark::Box);
    press(&mut app, '[');
    press(&mut app, '[');
    press(&mut app, '[');
    press(&mut app, '[');
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    press(&mut app, '[');
    assert_eq!(app.chart.modal.mark(), Mark::Heatmap, "[ wraps to the last");
    press(&mut app, ']');
    assert_eq!(app.chart.modal.mark(), Mark::Line);

    // While the column Picker is open, digits narrow instead of switching.
    app.chart.modal.focus = ChartFocus::X;
    press(&mut app, ' ');
    assert!(app.chart.modal.picker.is_some());
    press(&mut app, '3');
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    assert_eq!(app.chart.modal.picker.as_ref().unwrap().filter, "3");
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(
        app.chart.modal.picker.is_none(),
        "Esc closes only the Picker"
    );
    assert_eq!(app.overlay, Overlay::Chart);
}

/// `g` toggles the grid from anywhere on a chart with axes, as the Grid row's
/// Space does; the heatmap and the bar chart have no grid, and `g` leaves it be.
/// An open Picker takes `g` as a letter to narrow by.
#[test]
fn test_chart_g_toggles_the_grid() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, _rx, _tx) = open_chart_view("chart_grid_key_test.csv");
    let press = |app: &mut App, code: KeyCode| {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };
    assert!(!app.chart.modal.grid, "off by default");
    press(&mut app, KeyCode::Char('g'));
    assert!(app.chart.modal.grid);
    press(&mut app, KeyCode::Char('1'));
    press(&mut app, KeyCode::Char('g'));
    assert!(!app.chart.modal.grid, "one setting across the types");

    // The Grid row toggles it too.
    app.chart.modal.focus = ChartFocus::Grid;
    press(&mut app, KeyCode::Char(' '));
    assert!(app.chart.modal.grid);

    press(&mut app, KeyCode::Char('7'));
    assert_eq!(app.chart.modal.mark(), Mark::Heatmap);
    press(&mut app, KeyCode::Char('g'));
    assert!(app.chart.modal.grid, "the heatmap has no grid to toggle");

    press(&mut app, KeyCode::Char('1'));
    app.chart.modal.focus = ChartFocus::X;
    press(&mut app, KeyCode::Char(' ')); // open the Picker
    press(&mut app, KeyCode::Char('g'));
    assert!(app.chart.modal.grid);
    assert_eq!(app.chart.modal.picker.as_ref().unwrap().filter, "g");

    // Reopened from the same column, the chart keeps its grid.
    press(&mut app, KeyCode::Esc);
    press(&mut app, KeyCode::Esc);
    assert!(app.at_table());
    press(&mut app, KeyCode::Char('c'));
    assert!(app.chart.modal.grid);
}

/// `x` gives the plot the keys: ←→ (h/l) step the crosshair from point to point,
/// Home and End go to the ends, and the readout under the plot names each value.
/// Tab, `x` or Esc hand the keys back to the panel, where ←→ change the row again;
/// the crosshair comes back where it was. A click on the plot puts it there.
#[test]
fn test_chart_crosshair_keys_and_click() {
    use crossterm::event::{MouseButton, MouseEvent, MouseEventKind};
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, rx, tx) = open_chart_view("chart_crosshair_test.csv");
    select_line(&mut app);
    app.chart.modal.focus = ChartFocus::Type;
    app.event(AppEvent::Resize(80, 24));
    pump_until_chart_ready(&mut app, &rx, &tx);
    // Wide enough for the bar to name x beside the rest.
    let area = Rect::new(0, 0, 120, 30);
    let draw = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(&mut *app, area, &mut buf);
        common::buffer_lines(&buf)
    };
    let press = |app: &mut App, code: KeyCode| {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };
    let screen = draw(&mut app);
    assert!(screen.concat().contains("x Crosshair"), "{screen:#?}");
    assert!(!app.chart.modal.plot_focus);

    // In the middle of the plot, on x = 2 of 0..4.
    press(&mut app, KeyCode::Char('x'));
    assert!(app.chart.modal.plot_focus);
    assert_eq!(app.chart.modal.cursor_x, Some(2.0));
    let screen = draw(&mut app);
    assert!(
        screen.iter().any(|row| row.contains("x: 2   y: 6")),
        "{screen:#?}"
    );
    for (key, at) in [
        (KeyCode::Right, 3.0),
        (KeyCode::Char('l'), 4.0),
        (KeyCode::Right, 4.0),
        (KeyCode::Home, 0.0),
        (KeyCode::Left, 0.0),
        (KeyCode::End, 4.0),
        (KeyCode::Char('h'), 3.0),
    ] {
        press(&mut app, key);
        assert_eq!(app.chart.modal.cursor_x, Some(at), "{key:?}");
    }
    // The arrows never reached the Type row.
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    let screen = draw(&mut app);
    assert!(
        screen.iter().any(|row| row.contains("x: 3   y: 9")),
        "{screen:#?}"
    );

    // Tab hands the keys back: → steps the Type row again.
    press(&mut app, KeyCode::Tab);
    assert!(!app.chart.modal.plot_focus);
    press(&mut app, KeyCode::Right);
    assert_eq!(app.chart.modal.mark(), Mark::Scatter);
    assert_eq!(app.chart.modal.cursor_x, Some(3.0));
    pump_until_chart_ready(&mut app, &rx, &tx);
    let screen = draw(&mut app);
    assert!(!screen.concat().contains("y: 9"), "no readout: {screen:#?}");

    // Back where it was; x hands the keys back as well, and so does Esc, which then
    // leaves the chart as before.
    press(&mut app, KeyCode::Char('x'));
    assert_eq!(app.chart.modal.cursor_x, Some(3.0));
    press(&mut app, KeyCode::Char('x'));
    assert!(!app.chart.modal.plot_focus);
    press(&mut app, KeyCode::Char('x'));
    press(&mut app, KeyCode::Esc);
    assert!(!app.chart.modal.plot_focus);
    assert_eq!(app.overlay, Overlay::Chart);

    // A click on the plot: the crosshair on the point drawn nearest it.
    draw(&mut app);
    let plot = app.chart.modal.plot.expect("the plot was drawn");
    let mut pump = EventPump::new(app, tx, rx);
    let column = plot.column(1.0);
    let click = MouseEvent {
        kind: MouseEventKind::Down(MouseButton::Left),
        column,
        row: plot.graph.top() + 1,
        modifiers: KeyModifiers::NONE,
    };
    assert!(pump.terminal_mouse(click).unwrap());
    assert!(pump.app.chart.modal.plot_focus);
    assert_eq!(pump.app.chart.modal.cursor_x, Some(1.0));

    press(&mut pump.app, KeyCode::Esc);
    press(&mut pump.app, KeyCode::Esc);
    assert!(pump.app.at_table());
}

/// Shelves take columns through the shared Picker: Space opens it on a shelf,
/// Enter chooses, and the choice is echoed on the row.
#[test]
fn test_chart_columns_picked_through_the_picker() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, _rx, _tx) = open_chart_view("chart_picker_test.csv");
    let press = |app: &mut App, code: KeyCode| {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };
    press(&mut app, KeyCode::Char('1'));
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    // A line keeps the histogram's column, as Y.
    assert_eq!(app.chart.modal.y(), ["x"]);

    press(&mut app, KeyCode::Tab); // Type -> X
    assert_eq!(app.chart.modal.focus, ChartFocus::X);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter); // choose "x", the cursor's item
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("x"));
    assert!(app.chart.modal.y().is_empty(), "X is not also a series");

    press(&mut app, KeyCode::Tab); // -> Y
    press(&mut app, KeyCode::Char(' ')); // open the Picker
    press(&mut app, KeyCode::Char(' ')); // toggle "y"
    press(&mut app, KeyCode::Enter); // done
    assert_eq!(app.chart.modal.y(), ["y"]);
    assert!(app.chart.modal.can_export());
}

/// Space on a pick-one shelf's open Picker chooses the highlighted column — it
/// must never type into the narrow filter, where a space matches nothing and
/// the list blanks under the key that just opened it.
#[test]
fn test_space_chooses_in_a_pick_one_chart_picker() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, _rx, _tx) = open_chart_view("chart_space_chooses_test.csv");
    let press = |app: &mut App, code: KeyCode| {
        app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)));
    };
    assert_eq!(app.chart.modal.mark(), Mark::Histogram);
    press(&mut app, KeyCode::Tab); // Type -> X
    press(&mut app, KeyCode::Char(' ')); // open the Picker
    press(&mut app, KeyCode::Down); // highlight "y"
    press(&mut app, KeyCode::Char(' ')); // chooses, like Enter
    assert!(app.chart.modal.picker.is_none());
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("y"));
    // The form has the keys back at once: the arrows walk the rows again.
    press(&mut app, KeyCode::Down);
    assert_eq!(app.chart.modal.focus, ChartFocus::Bins);
}

/// A typed export path expands `~` like every other typed path.
#[test]
fn test_chart_export_path_expands_tilde() {
    let (mut app, _rx, _tx) = open_chart_view("chart_tilde_test.csv");
    select_line(&mut app);

    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Char('e'),
        KeyModifiers::NONE,
    )));
    assert_eq!(app.overlay, Overlay::ChartExport);
    app.chart
        .export_modal
        .path_input
        .set_value("~/datui_tilde_test_dir/chart.png");
    let out = app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Enter,
        KeyModifiers::NONE,
    )));
    let Some(AppEvent::Applied(datui::Applied::ChartExport(
        datui::chart::chart_export::ChartExportRequest { path, .. },
    ))) = out
    else {
        panic!("Enter starts the export");
    };
    assert!(
        path.is_absolute() && !path.to_string_lossy().contains('~'),
        "the tilde expands: {path:?}"
    );
}

/// Chart data is prepared off the render path: selecting columns starts a background
/// computation (with the throbber up), render draws nothing until it lands, and then
/// draws the prepared series. Nothing here collects on the calling thread.
#[test]
fn test_chart_data_is_prepared_in_the_background() {
    let (mut app, rx, tx) = open_chart_view("chart_render_cache_test.csv");
    pump_until_chart_ready(&mut app, &rx, &tx);

    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);

    // Another chart, then let any event go through so the selection is noticed.
    select_line(&mut app);
    app.event(AppEvent::Resize(80, 24));
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
    app.event(AppEvent::Key(KeyEvent::new(
        KeyCode::Esc,
        KeyModifiers::NONE,
    )));
    assert!(app.at_table());
    assert!(!app.chart_preparing());
}

/// A chart being computed says so, and never asks for a column it has: after an
/// option changes, the chart before it stays up under the spinner; for a column not
/// yet drawn, the plot gives way to the message.
#[test]
fn a_chart_being_computed_says_so() {
    use datui::chart::chart_modal::Mark;
    let (mut app, rx, tx) = open_chart_view("chart_computing_test.csv");
    let area = Rect::new(0, 0, 100, 24);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(app, area, &mut buf);
        common::buffer_text(&buf)
    };
    pump_until_chart_ready(&mut app, &rx, &tx);
    for (i, mark) in [Mark::Histogram, Mark::Box, Mark::Kde]
        .into_iter()
        .enumerate()
    {
        app.chart.modal.set_mark(mark);
        app.chart.modal.spec.encoding.x.field = (mark != Mark::Box).then(|| "x".to_string());
        app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
        app.chart.modal.row_limit = Some(1_000 + i);
        app.event(AppEvent::Resize(area.width, area.height));
        assert!(app.chart_preparing(), "{mark:?}");
        let text = screen(&mut app);
        assert!(text.contains("Computing chart..."), "{mark:?}: {text}");
        assert!(!text.contains("Pick a column"), "{mark:?}: {text}");

        pump_until_chart_ready(&mut app, &rx, &tx);
        let drawn = screen(&mut app);
        assert!(!drawn.contains("Computing chart..."), "{mark:?}: {drawn}");

        // Another sample size: the chart drawn stays, with the spinner over it.
        app.chart.modal.row_limit = Some(2_000 + i);
        app.event(AppEvent::Resize(area.width, area.height));
        assert!(app.chart_preparing(), "{mark:?}");
        let text = screen(&mut app);
        assert!(text.contains("Computing chart..."), "{mark:?}: {text}");
        let axis = match mark {
            Mark::Histogram => "Count",
            Mark::Kde => "Density",
            _ => "y",
        };
        assert!(text.contains(axis), "{mark:?} keeps its chart: {text}");

        pump_until_chart_ready(&mut app, &rx, &tx);
        assert!(!screen(&mut app).contains("Computing chart..."), "{mark:?}");
    }
}

/// Holding a key through the options must not fan out into a collect per step: one
/// preparation runs at a time, and when it lands the newest selection is the one prepared.
#[test]
fn test_chart_prepares_one_selection_at_a_time() {
    let (mut app, rx, tx) = open_chart_view("chart_one_at_a_time_test.csv");
    // `c` on x suggested its histogram, which is being prepared.
    app.event(AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    // Five more distinct requests while the first is still out.
    for _ in 0..5 {
        app.chart.modal.hist_bins += 1;
        app.event(AppEvent::Resize(80, 24));
    }

    let mut results = 0;
    let mut handle = |app: &mut App, ev: AppEvent| {
        if matches!(ev, AppEvent::JobEnded(t) if t.kind() == JobKind::ChartPrepare) {
            results += 1;
        }
        if let Some(next) = app.event(ev) {
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

/// An export parked while a *different*, failing selection is in flight is not failed
/// with that selection's error: it waits for the current selection's data and completes.
#[test]
fn test_chart_export_waits_for_the_current_selection_not_a_failed_one() {
    use datui::chart::chart_export::ChartExportFormat;
    let (mut app, rx, tx) = open_chart_view("chart_export_after_failure_test.csv");
    pump_until_chart_ready(&mut app, &rx, &tx);
    // A column the view does not have cannot be charted, and takes a moment to fail.
    select_line(&mut app);
    app.chart.modal.spec.encoding.y.field = vec!["gone".to_string()];
    app.event(AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    // Move on to a valid selection while that one is still out, and ask for an export.
    app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
    app.event(AppEvent::Resize(80, 24));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chart.svg");
    let next = app
        .event(AppEvent::Applied(datui::Applied::ChartExport(
            chart_export_request(&path, ChartExportFormat::Svg),
        )))
        .expect("ChartExport defers to DoChartExport");
    app.event(next);

    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        path.exists(),
        "the export completed from the valid selection"
    );
    assert_ne!(
        app.overlay,
        Overlay::ChartExport,
        "no error reopened the modal"
    );
    assert!(app.chart_data_ready());
}

/// Enter on the chart's export dialog with no path says so on the dialog's status
/// line, and typing a path takes the reason away.
#[test]
fn test_chart_export_with_a_blank_path_says_why() {
    let (mut app, rx, tx) = open_chart_view("chart_export_blank_path_test.csv");
    select_line(&mut app);
    app.event(AppEvent::Resize(80, 24));
    pump_until_chart_ready(&mut app, &rx, &tx);
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.overlay, Overlay::ChartExport);
    assert!(app.chart.export_modal.path_input.value().is_empty());
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.overlay, Overlay::ChartExport, "the dialog stays");
    assert_eq!(
        app.chart.export_modal.error.as_deref(),
        Some("Enter a file path.")
    );
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.export_modal.error, None);
}

/// The footer offers `e Export` only where `e` exports: a chart whose rows say what
/// to draw.
#[test]
fn test_chart_footer_offers_export_only_when_there_is_a_chart() {
    let (mut app, rx, tx) = open_chart_view("chart_export_hint_test.csv");
    select_line(&mut app);
    app.event(AppEvent::Resize(120, 40));
    pump_until_chart_ready(&mut app, &rx, &tx);
    let footer = |app: &mut App| {
        draw_sized(app, (120, 40))
            .lines()
            .last()
            .unwrap_or_default()
            .to_string()
    };
    let drawn = footer(&mut app);
    assert!(drawn.contains("Export"), "{drawn}");
    app.chart.modal.spec.encoding.y.field = Vec::new();
    assert!(!app.chart.modal.can_export());
    let empty = footer(&mut app);
    assert!(!empty.contains("Export"), "{empty}");
    assert!(press(&mut app, KeyCode::Char('e')).is_none());
    assert_ne!(
        app.overlay,
        Overlay::ChartExport,
        "and e does nothing there"
    );
}

/// A chart export uses the prepared data and writes the file off-thread; if the data is
/// not ready yet the export waits for it rather than collecting on the UI thread.
#[test]
fn test_chart_export_waits_for_prepared_data_and_writes_in_background() {
    use datui::chart::chart_export::ChartExportFormat;
    let (mut app, rx, tx) = open_chart_view("chart_export_bg_test.csv");
    pump_until_chart_ready(&mut app, &rx, &tx);
    select_line(&mut app);
    app.event(AppEvent::Resize(80, 24));
    assert!(app.chart_preparing());

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chart.pdf");
    // Asked for while the data is still being prepared.
    let next = app
        .event(AppEvent::Applied(datui::Applied::ChartExport(
            chart_export_request(&path, ChartExportFormat::Pdf),
        )))
        .expect("ChartExport defers to DoChartExport");
    app.event(next);
    assert!(
        app.is_busy(),
        "an export owns the busy state until it finishes"
    );

    pump_until_idle(&mut app, &rx, &tx);
    assert!(
        std::fs::read(&path).unwrap().starts_with(b"%PDF-"),
        "the export was written once its data arrived"
    );
    assert_ne!(
        app.overlay,
        Overlay::ChartExport,
        "the export modal closes on success"
    );
}

/// A chart export lands whole or not at all, like a data export: every format
/// replaces a file only where that was agreed to, and a file that appeared
/// meanwhile is left alone with the error in the app. What lands is what the
/// format says: a PNG, an SVG, a PDF.
#[test]
fn test_chart_export_replaces_only_what_was_agreed() {
    use datui::chart::chart_export::{ChartExportFormat, ChartExportRequest};
    use datui::export::output_file::Overwrite;
    let (mut app, rx, tx) = open_chart_view("chart_export_overwrite_test.csv");
    select_line(&mut app);
    app.event(AppEvent::Resize(80, 24));
    pump_until_idle(&mut app, &rx, &tx);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let dir = tempfile::tempdir().unwrap();

    for (name, format) in [
        ("chart.png", ChartExportFormat::Png),
        ("chart.svg", ChartExportFormat::Svg),
        ("chart.pdf", ChartExportFormat::Pdf),
    ] {
        let path = dir.path().join(name);
        std::fs::write(&path, "theirs").unwrap();
        let request = chart_export_request(&path, format);
        run_to_idle(
            &mut app,
            &rx,
            &tx,
            AppEvent::Applied(datui::Applied::ChartExport(request.clone())),
        );
        // The form comes back with the reason on its status line, not a modal.
        assert_eq!(app.error_message(), None, "{name}");
        assert_eq!(app.overlay, Overlay::ChartExport, "{name}");
        assert!(
            app.chart
                .export_modal
                .error
                .as_deref()
                .is_some_and(|m| m.contains("appeared")),
            "{name}: the clash reaches the app"
        );
        assert_eq!(std::fs::read_to_string(&path).unwrap(), "theirs", "{name}");
        press(&mut app, KeyCode::Esc);
        assert_ne!(app.overlay, Overlay::ChartExport);

        let replace = ChartExportRequest {
            overwrite: Overwrite::Replace,
            ..request
        };
        run_to_idle(
            &mut app,
            &rx,
            &tx,
            AppEvent::Applied(datui::Applied::ChartExport(replace)),
        );
        assert_eq!(app.error_message(), None, "{name}");
        let bytes = std::fs::read(&path).unwrap();
        match format {
            ChartExportFormat::Png => assert!(bytes.starts_with(b"\x89PNG\r\n\x1a\n")),
            ChartExportFormat::Pdf => assert!(bytes.starts_with(b"%PDF-")),
            ChartExportFormat::Svg => {
                let text = String::from_utf8(bytes).unwrap();
                assert!(
                    text.starts_with("<svg") && text.trim_end().ends_with("</svg>"),
                    "{text}"
                );
                assert!(text.contains("viewBox=\"0 0 400 300\""), "{text}");
            }
        }
    }
    assert!(leftovers(dir.path(), &["chart.png", "chart.svg", "chart.pdf"]).is_empty());
}

/// Show Me: `c` chooses the chart from the cursor column's type, and says so.
#[test]
fn quick_chart_picks_the_type_from_the_cursor_column() {
    use datui::chart::chart_modal::{Aggregate, Mark};
    let (mut app, rx, tx) = open_flights("chart_quick_test.parquet");
    // carrier, origin, day, delay: the cursor starts on carrier.
    for (steps, mark, x, suggested) in [
        (0, Mark::Bar, "carrier", "str"),
        (2, Mark::Line, "day", "date"),
        (3, Mark::Histogram, "delay", "f64"),
    ] {
        for _ in 0..steps {
            table_key(&mut app, &rx, &tx, 'l');
        }
        press(&mut app, KeyCode::Char('c'));
        assert_eq!(app.chart.modal.mark(), mark, "{x}");
        assert_eq!(app.chart.modal.x().map(String::as_str), Some(x));
        assert_eq!(app.chart.modal.suggested.as_deref(), Some(suggested));
        match mark {
            Mark::Bar => assert_eq!(app.chart.modal.aggregate(), Aggregate::Count),
            Mark::Line => assert_eq!(app.chart.modal.y(), ["delay"], "the first number"),
            _ => {}
        }
        let area = Rect::new(0, 0, 100, 30);
        let mut buf = Buffer::empty(area);
        Widget::render(&mut app, area, &mut buf);
        let text = common::buffer_text(&buf);
        assert!(text.contains(&format!("suggested for {suggested}")), "{x}");
        // The first frame's plot is the chart the panel names, before its data lands:
        // a histogram only where the panel says Histogram.
        assert_eq!(
            text.contains("count per bin"),
            mark == Mark::Histogram,
            "{x}: {text}"
        );
        press(&mut app, KeyCode::Esc);
        // Back to the first column.
        for _ in 0..steps {
            table_key(&mut app, &rx, &tx, 'h');
        }
    }
}

/// A duration is an X in time: `c` on it suggests a line over it, ticked in lengths
/// of time.
#[test]
fn a_duration_x_charts_a_line_ticked_in_time() {
    use datui::chart::chart_modal::Mark;
    let path = common::fixture_dir().join("chart_duration_x.parquet");
    // A sample a second for ten minutes, in nanoseconds as MangoHud logs it.
    let mut df = df!(
        "elapsed" => (0..600).map(|i| i * 1_000_000_000).collect::<Vec<i64>>(),
        "fps" => (0..600).map(|i| 60.0 + (i % 7) as f64).collect::<Vec<f64>>()
    )
    .unwrap();
    df.apply("elapsed", |c| {
        c.cast(&DataType::Duration(TimeUnit::Nanoseconds)).unwrap()
    })
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("elapsed"));
    assert_eq!(app.chart.modal.y(), ["fps"]);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(text.contains("2m") && text.contains("4m"), "{text}");
}

/// A duration is a line's Y as well: frame times in milliseconds tick as such, and
/// the Y shelf of a histogram does not take them.
#[test]
fn a_duration_y_charts_ticked_in_milliseconds() {
    use datui::chart::chart_modal::Mark;
    let path = common::fixture_dir().join("chart_duration_y.parquet");
    let mut df = df!(
        "elapsed" => (0..600).map(|i| i * 1_000_000_000).collect::<Vec<i64>>(),
        "frametime" => (0..600).map(|i| 14_000 + (i % 7) * 1_000).collect::<Vec<i64>>(),
        "fps" => (0..600).map(|i| 60.0 + (i % 7) as f64).collect::<Vec<f64>>()
    )
    .unwrap();
    df.apply("elapsed", |c| {
        c.cast(&DataType::Duration(TimeUnit::Nanoseconds)).unwrap()
    })
    .unwrap();
    df.apply("frametime", |c| {
        c.cast(&DataType::Duration(TimeUnit::Microseconds)).unwrap()
    })
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    app.chart.modal.spec.encoding.y.field = vec!["frametime".to_string()];
    // Through a mark change, which keeps only the Y columns the chart takes.
    app.chart.modal.set_mark(Mark::Scatter);
    app.chart.modal.set_mark(Mark::Line);
    assert_eq!(app.chart.modal.y(), ["frametime"]);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 100, 30);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut app, area, &mut buf);
    let text = common::buffer_text(&buf);
    assert!(text.contains("16ms") && text.contains("2m"), "{text}");

    app.chart.modal.set_mark(Mark::Box);
    assert!(app.chart.modal.y().is_empty(), "{:?}", app.chart.modal.y());
}

/// Every type shows the same shelves; one it does not use is dimmed, with why, and
/// focus passes over it.
#[test]
fn shelves_dim_by_type() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, rx, tx) = open_flights("chart_shelves_test.parquet");
    press(&mut app, KeyCode::Char('c'));
    let area = Rect::new(0, 0, 100, 30);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(&mut *app, area, &mut buf);
        common::buffer_text(&buf)
    };
    for (key, mark, dimmed) in [
        ('6', Mark::Kde, Some("density")),
        ('5', Mark::Box, Some("same as X")),
        ('7', Mark::Heatmap, Some("density")),
        ('3', Mark::Bar, None),
    ] {
        // Off Rows, where digits type a sample size.
        app.chart.modal.focus = ChartFocus::Type;
        press(&mut app, KeyCode::Char(key));
        assert_eq!(app.chart.modal.mark(), mark);
        let text = screen(&mut app);
        for shelf in ["Type", "X", "Y", "Color"] {
            assert!(text.contains(shelf), "{mark:?} shows {shelf}: {text}");
        }
        if let Some(why) = dimmed {
            assert!(text.contains(why), "{mark:?}: {text}");
        }
        // Tab walks the rows the type uses, and never a dimmed shelf.
        let mut seen = Vec::new();
        for _ in 0..20 {
            press(&mut app, KeyCode::Tab);
            seen.push(app.chart.modal.focus);
        }
        match mark {
            Mark::Kde => assert!(!seen.contains(&ChartFocus::Y)),
            Mark::Box | Mark::Heatmap => assert!(!seen.contains(&ChartFocus::Color)),
            _ => assert!(seen.contains(&ChartFocus::Color)),
        }
    }
    pump_until(&mut app, &rx, &tx, |a| !a.chart_preparing());
}

/// Color splits a chart: one series per value, the largest by rows first; the value
/// picker lists every value by rows, and picking some charts those.
#[test]
fn chart_color_splits_and_the_value_picker_lists_by_rows() {
    use datui::chart::chart_modal::{Aggregate, ChartFocus, Mark, TimeUnit};
    let (mut app, rx, tx) = open_flights("chart_color_test.parquet");
    // `c` on day: a line of delay over it.
    table_key(&mut app, &rx, &tx, 'l');
    table_key(&mut app, &rx, &tx, 'l');
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    // By month, the mean, split by carrier.
    app.chart.modal.focus = ChartFocus::TimeUnit;
    for _ in 0..3 {
        press(&mut app, KeyCode::Right);
    }
    assert_eq!(app.chart.modal.spec.encoding.x.time_unit, TimeUnit::Month);
    assert_eq!(app.chart.modal.aggregate(), Aggregate::Mean);
    app.chart.modal.focus = ChartFocus::Color;
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(
        app.chart.modal.picker.as_ref().unwrap().items(),
        ["none", "carrier", "origin"]
    );
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.chart.modal.color(), ["carrier"]);
    pump_until_chart_ready(&mut app, &rx, &tx);

    let names = |app: &App| {
        let request = app.chart_names();
        request.expect("a line chart is prepared")
    };
    // Nine carriers of 100 rows each: by rows (equal counts in the column's
    // order), as many as the terminal has colors to tell apart.
    let carriers = ["AA", "B6", "DL", "EV", "F9", "MQ", "UA", "US", "WN"];
    let cap = app.chart.modal.series_max();
    assert_eq!(names(&app), carriers[..cap.min(9)]);

    // The value picker: every value with its rows.
    app.chart.modal.focus = ChartFocus::ColorValues;
    press(&mut app, KeyCode::Char(' '));
    let picker = app
        .chart
        .modal
        .picker
        .as_ref()
        .expect("the values are counted");
    assert_eq!(picker.items().len(), 9);
    assert_eq!(app.chart.modal.picker_details[0], "100");
    // Narrow to WN and pick it, then US.
    for c in "wn".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    assert_eq!(
        app.chart.modal.spec.encoding.color.values,
        [Some("WN".to_string())]
    );
    pump_until_chart_ready(&mut app, &rx, &tx);
    assert_eq!(names(&app), ["WN"]);

    // Over to origin and back: the carrier chart is cached, and so are its values.
    app.chart.modal.focus = ChartFocus::Color;
    press(&mut app, KeyCode::Right);
    assert_eq!(app.chart.modal.color(), ["origin"]);
    pump_until_chart_ready(&mut app, &rx, &tx);
    press(&mut app, KeyCode::Left);
    app.chart.modal.spec.encoding.color.values = vec![Some("WN".to_string())];
    app.event(AppEvent::Resize(80, 24));
    assert!(app.chart_data_ready(), "cached");
    assert!(
        app.chart.modal.has_color_counts(),
        "its values came back with it"
    );
}

/// Color by two columns, picked in order with Space: a series per combination,
/// named by their values joined, in the legend too.
#[test]
fn chart_color_by_several_columns_names_each_combination() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, rx, tx) = open_flights("chart_color_columns_test.parquet");
    // `c` on day: a line of delay over it.
    table_key(&mut app, &rx, &tx, 'l');
    table_key(&mut app, &rx, &tx, 'l');
    press(&mut app, KeyCode::Char('c'));
    assert_eq!(app.chart.modal.mark(), Mark::Line);
    app.chart.modal.focus = ChartFocus::Color;
    press(&mut app, KeyCode::Char(' '));
    assert_eq!(
        app.chart.modal.picker.as_ref().unwrap().items(),
        ["none", "carrier", "origin"]
    );
    for _ in 0..2 {
        press(&mut app, KeyCode::Down);
        press(&mut app, KeyCode::Char(' '));
    }
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.chart.modal.color(), ["carrier", "origin"]);
    pump_until_chart_ready(&mut app, &rx, &tx);

    // Each carrier flies from one origin, 100 rows each: equal counts, in the
    // columns' order.
    let combinations = [
        "AA · JFK",
        "B6 · JFK",
        "DL · EWR",
        "EV · LGA",
        "F9 · LGA",
        "MQ · LGA",
        "UA · EWR",
        "US · EWR",
        "WN · JFK",
    ];
    let cap = app.chart.modal.series_max();
    let names = app.chart_names().expect("a line chart is prepared");
    assert_eq!(names, combinations[..cap.min(9)]);
    let text = screen_text(&mut app);
    assert!(text.contains("AA · JFK"), "{text}");
}

/// Rows: a typed size, or Every row, is read on Enter and not before; while it
/// waits the chart stays as drawn and the row says so.
#[test]
fn chart_rows_are_read_on_enter() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let (mut app, rx, tx) = open_flights("chart_rows_enter_test.parquet");
    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Histogram);
    app.chart.modal.spec.encoding.x.field = Some("delay".to_string());
    app.chart.modal.row_limit = Some(100);
    app.chart.modal.focus = ChartFocus::LimitRows;
    app.event(AppEvent::Resize(120, 30));
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 120, 30);
    let screen = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(app, area, &mut buf);
        common::buffer_text(&buf)
    };
    assert!(screen(&mut app).contains("sample of 100 of 900 rows"));

    // A size typed waits for Enter.
    for c in "250".chars() {
        press(&mut app, KeyCode::Char(c));
    }
    assert!(!app.chart_preparing(), "a pending size reads nothing");
    assert_eq!(app.chart.modal.row_limit, Some(100));
    let text = screen(&mut app);
    assert!(text.contains("Sample 250"), "{text}");
    assert!(text.contains("Enter to read"), "{text}");
    assert!(text.contains("sample of 100 of 900 rows"), "{text}");
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.chart.modal.row_limit, Some(250));
    assert!(app.chart_preparing());
    pump_until_chart_ready(&mut app, &rx, &tx);
    let text = screen(&mut app);
    assert!(text.contains("sample of 250 of 900 rows · seed "), "{text}");
    assert!(text.contains("Sample 250"), "{text}");

    // Every row is one key away, and read on Enter too.
    press(&mut app, KeyCode::Right);
    assert!(!app.chart_preparing(), "a pending switch reads nothing");
    let text = screen(&mut app);
    assert!(text.contains("Every row (900)"), "{text}");
    assert!(text.contains("sample of 250 of 900 rows"), "{text}");
    press(&mut app, KeyCode::Enter);
    assert_eq!(app.chart.modal.row_limit, None);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let text = screen(&mut app);
    assert!(!text.contains("sample of"), "{text}");

    // Esc puts a pending change back and leaves the chart open.
    press(&mut app, KeyCode::Char('5'));
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.overlay, Overlay::Chart);
    assert_eq!(app.chart.modal.row_limit, None);
    assert!(screen(&mut app).contains("Every row (900)"));
    // A switch there and back holds nothing to undo: one Esc closes.
    press(&mut app, KeyCode::Right);
    press(&mut app, KeyCode::Left);
    press(&mut app, KeyCode::Esc);
    assert!(app.at_table());
}

/// The export dialog: the chart's legend setting carries over, a size preset sets
/// the pixels, and the file written is the format and the size asked for.
#[test]
fn chart_export_dialog_presets_and_legend() {
    use datui::chart::chart_export::{LegendPlace, SizePreset};
    use datui::chart::chart_export_modal::ChartExportFocus;
    use datui::chart::chart_modal::ChartFocus;
    let (mut app, rx, tx) = open_chart_view("chart_export_dialog_test.csv");
    select_line(&mut app);
    app.chart.modal.focus = ChartFocus::ShowLegend;
    press(&mut app, KeyCode::Char(' '));
    assert!(!app.chart.modal.show_legend);
    app.event(AppEvent::Resize(80, 24));
    pump_until_chart_ready(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.overlay, Overlay::ChartExport);
    assert_eq!(
        app.chart.export_modal.legend,
        LegendPlace::Off,
        "legend off carries"
    );
    // The description is how the chart was made; a plain line has none to say, and
    // the figure names y at its axis.
    assert_eq!(app.chart.export_modal.description_input.value(), "");
    press(&mut app, KeyCode::Esc);
    app.chart.modal.spec.encoding.y.aggregate = datui::chart::chart_modal::Aggregate::Mean;
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(
        app.chart.export_modal.description_input.value(),
        "Mean by x"
    );
    app.chart.modal.spec.encoding.y.aggregate = datui::chart::chart_modal::Aggregate::None;
    // Size: Document -> Slide 16:9.
    datui::app::form::Form::focus(&mut app.chart.export_modal, ChartExportFocus::Size);
    press(&mut app, KeyCode::Left);
    assert_eq!(app.chart.export_modal.size, SizePreset::Slide);
    assert_eq!(app.chart.export_modal.export_dimensions(), (1920, 1080));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("slide.png");
    app.chart
        .export_modal
        .path_input
        .set_value(path.display().to_string());
    let out = press(&mut app, KeyCode::Enter).expect("Enter exports");
    run_to_idle(&mut app, &rx, &tx, out);
    assert_eq!(app.error_message(), None);
    let png = std::fs::read(&path).unwrap();
    assert!(png.starts_with(b"\x89PNG"));
    assert_eq!(&png[16..20], &1920u32.to_be_bytes());
    assert_eq!(&png[20..24], &1080u32.to_be_bytes());

    // An ending that is no format is part of the name: the format's extension
    // goes after it.
    press(&mut app, KeyCode::Char('e'));
    let v2 = dir.path().join("chart.v2");
    app.chart
        .export_modal
        .path_input
        .set_value(v2.display().to_string());
    let out = press(&mut app, KeyCode::Enter).expect("Enter exports");
    run_to_idle(&mut app, &rx, &tx, out);
    assert!(
        std::fs::read(dir.path().join("chart.v2.png"))
            .unwrap()
            .starts_with(b"\x89PNG")
    );
    assert!(!v2.exists());

    // A path that names a format takes it.
    press(&mut app, KeyCode::Char('e'));
    let svg = dir.path().join("figure.svg");
    app.chart
        .export_modal
        .path_input
        .set_value(svg.display().to_string());
    let out = press(&mut app, KeyCode::Enter).expect("Enter exports");
    run_to_idle(&mut app, &rx, &tx, out);
    assert!(std::fs::read_to_string(&svg).unwrap().starts_with("<svg"));
}

/// Range on a line chart spans the Y axis over the points that are not outliers, so
/// one spike no longer flattens the rest, and the title row counts the outliers it
/// drew at the edge.
#[test]
fn a_line_charts_range_clips_its_y_axis_to_the_percentiles() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let path = common::fixture_dir().join("chart_y_range.parquet");
    let mut df = df!(
        "x" => (0..200).collect::<Vec<i64>>(),
        "y" => (0..200).map(|i| if i == 100 { 1_000_000 } else { i }).collect::<Vec<i64>>()
    )
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Line);
    app.chart.modal.spec.encoding.x.field = Some("x".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 160, 30);
    let draw = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(app, area, &mut buf);
        common::buffer_text(&buf)
    };
    let text = draw(&mut app);
    assert!(text.contains("800000"), "{text}");
    assert!(text.contains("Range") && text.contains("All"), "{text}");

    app.chart.modal.focus = ChartFocus::Range;
    press(&mut app, KeyCode::Right);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(text.contains("outliers at edge"), "{text}");
    assert!(text.contains("1 outlier at the edge"), "{text}");
    assert!(
        !text.contains("1,000,000") && !text.contains("1M"),
        "{text}"
    );
}

/// Smooth on a line over time steps lengths of time the X's spacing and span
/// offer; the panel echoes the window, the notes say the line is a trailing mean,
/// and the crosshair reads the smoothed value with the raw one beside it.
#[test]
fn a_line_charts_smooth_steps_lengths_of_time() {
    use datui::chart::chart_modal::{ChartFocus, Mark, Smooth};
    let path = common::fixture_dir().join("chart_smooth.parquet");
    // A reading a second for an hour.
    let mut df = df!(
        "t" => (0..3_600i64).map(|s| 1_700_000_000_000 + s * 1_000).collect::<Vec<i64>>(),
        "y" => (0..3_600i64).map(|s| s % 10).collect::<Vec<i64>>()
    )
    .unwrap();
    df.apply("t", |c| {
        c.cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            .unwrap()
    })
    .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Line);
    app.chart.modal.spec.encoding.x.field = Some("t".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
    app.chart.modal.row_limit = None;
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 160, 34);
    let draw = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(app, area, &mut buf);
        common::buffer_text(&buf)
    };
    let text = draw(&mut app);
    assert!(
        text.contains("Smooth") && !text.contains("trailing"),
        "{text}"
    );

    app.chart.modal.focus = ChartFocus::Smooth;
    // 5s 10s 30s 1m: wider than a second, at most half the hour.
    for _ in 0..4 {
        press(&mut app, KeyCode::Right);
    }
    pump_until_chart_ready(&mut app, &rx, &tx);
    assert_eq!(app.chart.modal.smooth, Smooth::Seconds(60));
    let text = draw(&mut app);
    assert!(text.contains("1m"), "{text}");
    assert!(text.contains("raw auto"), "{text}");
    assert!(text.contains("mean of the trailing 1m"), "{text}");

    press(&mut app, KeyCode::Char('x'));
    let text = draw(&mut app);
    assert!(text.contains("(raw "), "{text}");
}

/// Sizes from 1 to 100,000, three of them 0 or less and two not finite (dropped
/// before any axis sees them, as a null X is), with a
/// date, numbers and a category beside them, written to `name`.
fn log_x_fixture(name: &str) -> PathBuf {
    let path = common::fixture_dir().join(name);
    let mut size: Vec<f64> = (0..=50).map(|i| 10f64.powf(f64::from(i) / 10.0)).collect();
    size.extend([0.0, -1.0, 0.0, f64::NAN, f64::INFINITY]);
    let n = size.len() as i64;
    let mut df = df!(
        "size" => size,
        "ms" => (0..n).map(|i| (i * 7 % 13) as f64).collect::<Vec<f64>>(),
        "day" => (0..n as i32).map(|d| 19_700 + d).collect::<Vec<i32>>(),
        "neg" => (0..n).map(|i| -(i as f64)).collect::<Vec<f64>>(),
        "kind" => (0..n).map(|i| if i % 2 == 0 { "even" } else { "odd" }).collect::<Vec<&str>>()
    )
    .unwrap();
    df.apply("day", |c| c.cast(&DataType::Date).unwrap())
        .unwrap();
    ParquetWriter::new(File::create(&path).unwrap())
        .finish(&mut df)
        .unwrap();
    path
}

/// X's log scale on a scatter of a number X: Space on the line under Log scale
/// turns it on, the axis ticks the powers of ten, and the notes count the points
/// it leaves out. Over a date X the line is dimmed and Tab skips it.
#[test]
fn a_scatter_takes_a_log_x() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let path = log_x_fixture("chart_log_x.parquet");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Scatter);
    app.chart.modal.spec.encoding.x.field = Some("day".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["ms".to_string()];
    app.chart.modal.row_limit = None;
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 160, 34);
    let draw = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(app, area, &mut buf);
        common::buffer_text(&buf)
    };
    let text = draw(&mut app);
    assert!(text.contains("X numbers only"), "{text}");
    app.chart.modal.focus = ChartFocus::LogScale;
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.chart.modal.focus, ChartFocus::Range, "Tab skips it");

    // ←/→ on X steps its column.
    app.chart.modal.focus = ChartFocus::X;
    for _ in 0..3 {
        if app.chart.modal.x().map(String::as_str) == Some("size") {
            break;
        }
        press(&mut app, KeyCode::Right);
    }
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("size"));
    pump_until_chart_ready(&mut app, &rx, &tx);
    app.chart.modal.focus = ChartFocus::LogScale;
    press(&mut app, KeyCode::Tab);
    assert_eq!(app.chart.modal.focus, ChartFocus::LogX);
    press(&mut app, KeyCode::Char(' '));
    pump_until_chart_ready(&mut app, &rx, &tx);
    assert!(app.chart.modal.log_x);
    let text = draw(&mut app);
    assert!(text.contains("Y off") && text.contains("X on"), "{text}");
    let at_most = datui::glyphs::get().at_most;
    assert!(
        text.contains(&format!("3 points with X {at_most} 0 left out")),
        "{text}"
    );
    let labels = text
        .lines()
        .find(|l| l.contains(" 10 ") && l.contains(" 100 "))
        .unwrap_or_else(|| panic!("powers of ten under the plot: {text}"));
    // Five powers of ten: the short form, 1 to 100k.
    assert!(
        labels.contains(" 1k ") && labels.contains("100k"),
        "{labels}"
    );

    // The crosshair stands on X's values, never on one left out.
    press(&mut app, KeyCode::Char('x'));
    press(&mut app, KeyCode::Home);
    assert_eq!(app.chart.modal.cursor_x, Some(1.0));
    press(&mut app, KeyCode::End);
    assert_eq!(app.chart.modal.cursor_x, Some(100_000.0));
    press(&mut app, KeyCode::Char('x'));

    // The export draws the same axis and says the same.
    let dir = tempfile::tempdir().unwrap();
    let svg = dir.path().join("chart.svg");
    let next = app.event(AppEvent::Applied(datui::Applied::ChartExport(
        chart_export_request(&svg, datui::chart::chart_export::ChartExportFormat::Svg),
    )));
    if let Some(next) = next {
        app.event(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    // Its axis and notes are the chart golden's `scatter_log_x`.
    assert!(std::fs::read_to_string(&svg).unwrap().starts_with("<svg"));

    // With every X at 0 or less, the plot says why it is empty.
    app.chart.modal.spec.encoding.x.field = Some("neg".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["size".to_string()];
    app.event(AppEvent::Wake);
    pump_until_chart_ready(&mut app, &rx, &tx);
    assert_eq!(app.chart.modal.x().map(String::as_str), Some("neg"));
    let text = draw(&mut app);
    assert!(
        text.contains("No X above 0 to draw on a log scale"),
        "{text}"
    );
    assert!(text.contains("size: every point left out"), "{text}");
}

/// Y's log scale on a line: Space on Log scale turns it on, Y from zero dims, the
/// notes count the points at Y of 0 or less it leaves out, the crosshair passes
/// them, and with nothing above 0 the plot says why it is empty.
#[test]
fn a_line_takes_a_log_y() {
    use datui::chart::chart_export_modal::ChartExportFocus;
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let path = log_x_fixture("chart_log_y.parquet");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    let g = datui::glyphs::get();

    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Line);
    app.chart.modal.spec.encoding.x.field = Some("day".to_string());
    // 0 on every thirteenth row: five of them.
    app.chart.modal.spec.encoding.y.field = vec!["ms".to_string()];
    app.chart.modal.row_limit = None;
    app.chart.modal.y_starts_at_zero = true;
    pump_until_chart_ready(&mut app, &rx, &tx);
    let area = Rect::new(0, 0, 160, 34);
    let draw = |app: &mut App| {
        let mut buf = Buffer::empty(area);
        Widget::render(app, area, &mut buf);
        common::buffer_text(&buf)
    };
    app.chart.modal.focus = ChartFocus::LogScale;
    press(&mut app, KeyCode::Char(' '));
    pump_until_chart_ready(&mut app, &rx, &tx);
    assert!(app.chart.modal.log_scale);
    let text = draw(&mut app);
    assert!(text.contains("Y on"), "{text}");
    assert!(
        text.contains(&format!("off {} log scale", g.middot)),
        "{text}"
    );
    assert!(
        text.contains(&format!("5 points with Y {} 0 left out", g.at_most)),
        "{text}"
    );
    press(&mut app, KeyCode::BackTab);
    assert_ne!(
        app.chart.modal.focus,
        ChartFocus::YStartsAtZero,
        "Tab passes Y from zero"
    );

    // The crosshair never stands on a point left out: the first row's is 0.
    press(&mut app, KeyCode::Char('x'));
    press(&mut app, KeyCode::Home);
    assert_eq!(app.chart.modal.cursor_x, Some(19_701.0));
    press(&mut app, KeyCode::Char('x'));

    // The export dialog offers no Y from zero for a log Y.
    press(&mut app, KeyCode::Char('e'));
    assert_eq!(app.overlay, Overlay::ChartExport);
    assert!(!app.chart.export_modal.shows(ChartExportFocus::YFromZero));
    assert!(app.chart.export_modal.shows(ChartExportFocus::LineWidth));
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.overlay, Overlay::Chart);

    let dir = tempfile::tempdir().unwrap();
    let svg = dir.path().join("chart.svg");
    let next = app.event(AppEvent::Applied(datui::Applied::ChartExport(
        chart_export_request(&svg, datui::chart::chart_export::ChartExportFormat::Svg),
    )));
    if let Some(next) = next {
        app.event(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    // The file's axis and points are `a_log_y_exports_its_decades`'.
    assert!(std::fs::read_to_string(&svg).unwrap().starts_with("<svg"));

    // Off again, Y from zero is back as it was.
    app.chart.modal.focus = ChartFocus::LogScale;
    press(&mut app, KeyCode::Char(' '));
    pump_until_chart_ready(&mut app, &rx, &tx);
    assert!(!app.chart.modal.log_scale && app.chart.modal.y_from_zero_shown());
    let text = draw(&mut app);
    let row = text.lines().find(|l| l.contains("Y from zero")).unwrap();
    assert!(row.contains("on") && !row.contains("log scale"), "{row}");
    press(&mut app, KeyCode::BackTab);
    assert_eq!(app.chart.modal.focus, ChartFocus::YStartsAtZero);

    app.chart.modal.log_scale = true;
    app.chart.modal.spec.encoding.y.field = vec!["neg".to_string()];
    app.event(AppEvent::Wake);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(
        text.contains("No Y above 0 to draw on a log scale"),
        "{text}"
    );
    assert!(text.contains("neg: every point left out"), "{text}");

    // Both axes logged: X leaves out some, Y the rest.
    app.chart.modal.set_mark(Mark::Scatter);
    app.chart.modal.spec.encoding.x.field = Some("size".to_string());
    app.chart.modal.log_x = true;
    app.event(AppEvent::Wake);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(
        text.contains("No point with X and Y above 0 to draw on a log scale"),
        "{text}"
    );
}

/// Y's log scale over a colored line: each color's series leaves out its own
/// zeros, the note counts them all, and the legend names both.
#[test]
fn a_colored_line_takes_a_log_y() {
    use datui::chart::chart_modal::Mark;
    let path = log_x_fixture("chart_log_y_color.parquet");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);

    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Line);
    app.chart.modal.spec.encoding.x.field = Some("day".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["ms".to_string()];
    app.chart.modal.spec.encoding.color.field = vec!["kind".to_string()];
    app.chart.modal.log_scale = true;
    app.event(AppEvent::Wake);
    pump_until_chart_ready(&mut app, &rx, &tx);
    let mut buf = Buffer::empty(Rect::new(0, 0, 160, 34));
    Widget::render(&mut app, Rect::new(0, 0, 160, 34), &mut buf);
    let text = common::buffer_text(&buf);
    let at_most = datui::glyphs::get().at_most;
    // Zeros at rows 0, 13, 26, 39 and 52, both colors among them.
    assert!(
        text.contains(&format!("5 points with Y {at_most} 0 left out")),
        "{text}"
    );
    assert!(text.contains("even") && text.contains("odd"), "{text}");
}

/// Keys typed back to back through the pump, as a fast typist sends them, right
/// after X changes the panel's rows: the Tabs reach X's log scale and Space turns
/// it on, however the chart's preparation falls between them.
#[test]
fn keys_typed_fast_turn_on_a_log_x() {
    use datui::chart::chart_modal::{ChartFocus, Mark};
    let path = log_x_fixture("chart_log_x_fast.parquet");
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Scatter);
    app.chart.modal.spec.encoding.x.field = Some("day".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["ms".to_string()];
    app.chart.modal.row_limit = None;
    pump_until_chart_ready(&mut app, &rx, &tx);
    app.chart.modal.focus = ChartFocus::X;

    let mut pump = EventPump::new(app, tx, rx);
    let key = |pump: &mut EventPump, code| {
        pump.terminal_key(KeyEvent::new(code, KeyModifiers::NONE))
            .unwrap();
    };
    // ←/→ on X steps its column: from the date to a number, which brings the line.
    let mut steps = 0;
    while pump.app.chart.modal.x().map(String::as_str) != Some("size") {
        key(&mut pump, KeyCode::Right);
        while pump.replay_one().unwrap() {}
        steps += 1;
        assert!(steps < 4, "X reaches size");
    }
    let rows = pump.app.chart.modal.row_order();
    let at = |f: ChartFocus| rows.iter().position(|r| *r == f).unwrap();
    for _ in at(ChartFocus::X)..at(ChartFocus::LogX) {
        key(&mut pump, KeyCode::Tab);
    }
    key(&mut pump, KeyCode::Char(' '));
    for _ in ticks() {
        while pump.replay_one().unwrap() {}
        if pump.held_keys().count() == 0 && !pump.app.is_busy() && pump.app.chart_data_ready() {
            break;
        }
        pump.wait_and_drain(std::time::Duration::from_millis(50))
            .unwrap();
    }
    assert_eq!(pump.app.chart.modal.focus, ChartFocus::LogX);
    assert!(pump.app.chart.modal.log_x, "Space turned it on");
}

/// Until the chart's trend has been fitted, and the chart drawn.
fn pump_until_trended(app: &mut App, rx: &mpsc::Receiver<AppEvent>, tx: &mpsc::Sender<AppEvent>) {
    pump_until(app, rx, tx, |app| {
        app.chart_data_ready() && !app.chart_trend_pending()
    });
}

/// A scatter of `x` against `y` from `csv`, every row read.
fn open_trend_scatter(
    name: &str,
    csv: &str,
) -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    use datui::chart::chart_modal::Mark;
    let path = common::fixture_dir().join(name);
    std::fs::write(&path, csv).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx.clone(), common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], OpenOptions::default());
    pump_until_idle(&mut app, &rx, &tx);
    press(&mut app, KeyCode::Char('c'));
    app.chart.modal.set_mark(Mark::Scatter);
    app.chart.modal.spec.encoding.x.field = Some("x".to_string());
    app.chart.modal.spec.encoding.y.field = vec!["y".to_string()];
    app.chart.modal.row_limit = None;
    app.event(AppEvent::Wake);
    pump_until_chart_ready(&mut app, &rx, &tx);
    (app, rx, tx)
}

fn trend_csv(rows: i64) -> String {
    let mut csv = "x,y,g\n".to_string();
    for i in 0..rows {
        let y = if i % 10 == 5 && i < 35 {
            String::new()
        } else {
            (3 * i + 10 + (i * 37) % 11 - 5).to_string()
        };
        csv.push_str(&format!("{i},{y},{}\n", if i % 2 == 0 { "a" } else { "b" }));
    }
    csv
}

fn draw_at(app: &mut App, width: u16, height: u16) -> String {
    let area = Rect::new(0, 0, width, height);
    let mut buf = Buffer::empty(area);
    Widget::render(app, area, &mut buf);
    common::buffer_text(&buf)
}

/// A scatter's trend: Trend steps the method, the reading sits under the plot and
/// the notes say what was fitted; the crosshair reads the line, `y` copies the fit,
/// and Over fits per color or over every point.
#[test]
fn a_scatter_draws_reads_and_copies_its_trend() {
    use datui::chart::chart_modal::{ChartFocus, Trend, TrendOver};
    let (mut app, rx, tx) = open_trend_scatter("chart_trend.csv", &trend_csv(100));
    let copies: Copies = Default::default();
    app.set_clipboard_destination(Box::new(KeptCopies(copies.clone())));
    let draw = |app: &mut App| draw_at(app, 160, 34);
    let text = draw(&mut app);
    assert!(text.contains("Trend") && text.contains("off"), "{text}");
    // Nothing to copy yet.
    press_and_send(&mut app, &tx, KeyCode::Char('y'));
    assert!(copies.lock().unwrap().is_empty());

    app.chart.modal.focus = ChartFocus::Trend;
    press(&mut app, KeyCode::Right);
    assert_eq!(app.chart.modal.trend, Trend::Linear);
    // Every fit is a job.
    assert!(app.chart_trend_pending());
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(
        text.contains("+3.00 per unit of x · strong, R² 1.00 · typically ±3.22 off the line"),
        "{text}"
    );
    assert!(text.contains("fit on 97 · 3 null"), "{text}");

    press(&mut app, KeyCode::Char('x'));
    let text = draw(&mut app);
    assert!(
        text.contains("fit "),
        "the line's value at the crosshair: {text}"
    );
    press(&mut app, KeyCode::Esc);

    press_and_send(&mut app, &tx, KeyCode::Char('y'));
    pump_until_idle(&mut app, &rx, &tx);
    let copied = copies.lock().unwrap().last().cloned().unwrap();
    assert!(
        copied.starts_with("Trend: linear (least squares)\n"),
        "{copied}"
    );
    assert!(copied.contains("x: x, y: y"), "{copied}");
    assert!(copied.contains("  points: 97 used"), "{copied}");

    // While a fit runs, `y` says so rather than copying the last.
    for _ in 0..3 {
        press(&mut app, KeyCode::Right);
    }
    assert_eq!(app.chart.modal.trend, Trend::Robust);
    let copied = copies.lock().unwrap().len();
    press(&mut app, KeyCode::Char('y'));
    assert_eq!(copies.lock().unwrap().len(), copied);
    assert!(draw(&mut app).contains("Still fitting the trend"));
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(text.contains("half the points within"), "{text}");

    // A Color: one line per series, read as a range, or one over every point.
    app.chart.modal.spec.encoding.color.field = vec!["g".to_string()];
    app.chart.modal.trend = Trend::Linear;
    // Set by hand: an event has the chart asked for.
    app.event(AppEvent::Wake);
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(text.contains("2 fits · slopes +"), "{text}");
    assert!(text.contains("over per color"), "{text}");
    app.chart.modal.focus = ChartFocus::TrendOver;
    press(&mut app, KeyCode::Right);
    assert_eq!(app.chart.modal.trend_over, TrendOver::Overall);
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw(&mut app);
    assert!(text.contains("over all points"), "{text}");
    assert!(
        !text.contains("2 fits") && text.contains("strong"),
        "{text}"
    );

    // The export draws the line too.
    let dir = tempfile::tempdir().unwrap();
    let svg = dir.path().join("trend.svg");
    let next = app.event(AppEvent::Applied(datui::Applied::ChartExport(
        chart_export_request(&svg, datui::chart::chart_export::ChartExportFormat::Svg),
    )));
    if let Some(next) = next {
        app.event(next);
    }
    pump_until_idle(&mut app, &rx, &tx);
    // Its words are outlines here; `chart_export`'s tests read them as text.
    let svg = std::fs::read_to_string(&svg).unwrap();
    assert!(svg.starts_with("<svg"), "{svg}");
}

/// One fit at a time: a fit no longer wanted is stopped, its answer dropped, and the
/// one wanted now lands once it has ended.
#[test]
fn a_trend_asked_for_mid_fit_lands_after_the_one_running() {
    use datui::chart::chart_modal::{ChartFocus, Trend};
    let (mut app, rx, tx) = open_trend_scatter("chart_trend_queue.csv", &trend_csv(3_000));
    app.chart.modal.focus = ChartFocus::Trend;
    for _ in 0..4 {
        press(&mut app, KeyCode::Right);
    }
    assert_eq!(app.chart.modal.trend, Trend::Robust);
    assert!(app.chart_trend_pending());
    // Local before robust has answered.
    press(&mut app, KeyCode::Right);
    assert_eq!(app.chart.modal.trend, Trend::Local);
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw_at(&mut app, 160, 34);
    assert!(text.contains("local curve"), "{text}");
    assert!(
        !text.contains("half the points"),
        "robust's answer dropped: {text}"
    );
    // Back to robust: fitted again, for the chart on screen.
    press(&mut app, KeyCode::Left);
    pump_until_trended(&mut app, &rx, &tx);
    assert!(draw_at(&mut app, 160, 34).contains("half the points within"));
}

/// Theil–Sen's pairs are drawn with the chart's own sample's seed, named once.
#[test]
fn a_robust_fit_names_its_seed_once() {
    use datui::chart::chart_modal::{ChartFocus, Trend};
    let (mut app, rx, tx) = open_trend_scatter("chart_trend_seed.csv", &trend_csv(3_000));
    app.chart.modal.row_limit = Some(2_000);
    app.chart.modal.focus = ChartFocus::Trend;
    for _ in 0..4 {
        press(&mut app, KeyCode::Right);
    }
    assert_eq!(app.chart.modal.trend, Trend::Robust);
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw_at(&mut app, 220, 34);
    let title = text.lines().next().unwrap();
    assert!(
        title.contains("sample of 2,000 of 3,000 rows · seed "),
        "{title}"
    );
    assert!(title.contains("pairs"), "{title}");
    assert_eq!(title.matches("seed ").count(), 1, "{title}");
    // Every row: no sample, so the pairs name theirs.
    app.chart.modal.row_limit = None;
    app.event(AppEvent::Wake);
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw_at(&mut app, 220, 34);
    let title = text.lines().next().unwrap();
    assert!(title.contains("pairs · seed "), "{title}");
}

/// On a narrow terminal the panel's Trend lines stay inside its border.
#[test]
fn the_trend_rows_fit_a_narrow_panel() {
    use datui::chart::chart_modal::{ChartFocus, Trend};
    let (mut app, rx, tx) = open_trend_scatter("chart_trend_narrow.csv", &trend_csv(100));
    app.chart.modal.spec.encoding.color.field = vec!["g".to_string()];
    app.chart.modal.trend = Trend::Linear;
    app.chart.modal.focus = ChartFocus::TrendOver;
    press(&mut app, KeyCode::Right);
    pump_until_trended(&mut app, &rx, &tx);
    let text = draw_at(&mut app, 60, 24);
    let over = text
        .lines()
        .find(|l| l.contains("all points"))
        .unwrap_or_else(|| panic!("{text}"));
    let (panel, _) = over.split_once('│').unwrap_or_else(|| panic!("{over}"));
    assert!(panel.ends_with(' '), "a gap before the border: {over:?}");
}
