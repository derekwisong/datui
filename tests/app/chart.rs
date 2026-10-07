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
    let Some(AppEvent::ChartExport(datui::chart::chart_export::ChartExportRequest {
        path, ..
    })) = out
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
        .event(AppEvent::ChartExport(chart_export_request(
            &path,
            ChartExportFormat::Svg,
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
        .event(AppEvent::ChartExport(chart_export_request(
            &path,
            ChartExportFormat::Pdf,
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
        run_to_idle(&mut app, &rx, &tx, AppEvent::ChartExport(request.clone()));
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
        run_to_idle(&mut app, &rx, &tx, AppEvent::ChartExport(replace));
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
    assert_eq!(app.chart.modal.color().map(String::as_str), Some("carrier"));
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
    assert_eq!(app.chart.modal.color().map(String::as_str), Some("origin"));
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
    datui::form::Form::focus(&mut app.chart.export_modal, ChartExportFocus::Size);
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
