//! Data Quality's declared column intent and its report export, driven through the
//! app's keys: intent is staged in Setup and reads nothing until Run, reuses the rows
//! a run already read, and an export writes the report on screen without reading the
//! source, which these tests delete first.

use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use datui::analysis_modal::{AnalysisFocus, SetupRow};
use datui::data_quality::{QualityPage, QualityPrecision};
use datui::quality_export::{REPORT_FORMAT, REPORT_VERSION, ReportFile};
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::mpsc;

mod common;

fn work_pending(app: &App) -> bool {
    app.is_busy() || app.row_count_pending()
}

/// The next event on the channel, or one background work still owes; `None` once
/// nothing is there and nothing is owed.
fn next_event(app: &App, rx: &mpsc::Receiver<AppEvent>) -> Option<AppEvent> {
    // Only a hang guard; nothing here is timed.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
    loop {
        if let Ok(event) = rx.try_recv() {
            return Some(event);
        }
        if !work_pending(app) {
            return None;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "background work never reported back"
        );
        if let Ok(event) = rx.recv_timeout(std::time::Duration::from_millis(50)) {
            return Some(event);
        }
    }
}

/// Handle `first` and every event it chains to, then whatever background work owes,
/// until nothing is pending. Returns how many Data Quality runs finished.
fn drain(app: &mut App, rx: &mpsc::Receiver<AppEvent>, first: Option<AppEvent>) -> usize {
    let mut runs = 0;
    let mut handle = |app: &mut App, event: AppEvent| {
        let mut next = Some(event);
        while let Some(event) = next {
            if matches!(event, AppEvent::BackgroundDataQualityReady { .. }) {
                runs += 1;
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
    runs
}

fn press(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(&AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

fn type_text(app: &mut App, text: &str) {
    for c in text.chars() {
        assert!(press(app, KeyCode::Char(c)).is_none());
    }
}

/// `rows` orders: an id that repeats every 997th row, a status with a value outside
/// open and closed, an amount from -10 to 119, and a code that is sometimes not a
/// number.
fn write_orders(path: &Path, rows: i64) {
    let mut df = df!(
        "id" => (0..rows).map(|row| if row % 997 == 0 && row > 0 { row - 1 } else { row }).collect::<Vec<_>>(),
        "status" => (0..rows).map(|row| ["open", "closed", "void"][row as usize % 3]).collect::<Vec<_>>(),
        "amount" => (0..rows).map(|row| (row % 130) as f64 - 10.0).collect::<Vec<_>>(),
        "code" => (0..rows).map(|row| if row % 101 == 0 { "n/a".to_string() } else { row.to_string() }).collect::<Vec<_>>(),
    )
    .unwrap();
    ParquetWriter::new(File::create(path).unwrap())
        .finish(&mut df)
        .unwrap();
}

/// The app with `path` open and Data Quality's Setup on screen, the shared sample
/// `sample_rows` rows.
fn open_setup(path: PathBuf, sample_rows: usize) -> (App, mpsc::Receiver<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let mut app = App::new(tx, common::test_runtime());
    drain(
        &mut app,
        &rx,
        Some(AppEvent::Open(vec![path], OpenOptions::default())),
    );
    app.analysis_modal.sample.rows = sample_rows;
    press(&mut app, KeyCode::Char('a'));
    app.analysis_modal.sidebar_state.select(Some(3));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.focus, AnalysisFocus::Main);
    (app, rx)
}

/// Nothing that reads, and nothing waiting to.
fn assert_no_read(app: &App, rx: &mpsc::Receiver<AppEvent>) {
    assert!(!app.is_busy(), "nothing runs");
    assert!(app.analysis_modal.computing.is_none());
    assert!(rx.try_recv().is_err(), "nothing was sent");
}

/// Column intent is a Setup row like any other: its list and form stage edits in the
/// draft, a bound the type cannot read is refused on the form, Esc in the list puts
/// the list's edits back and Esc in Setup every edit, and none of it reads.
#[test]
fn column_intent_is_staged_in_setup_and_reads_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("orders.parquet");
    write_orders(&path, 3_000);
    let (mut app, rx) = open_setup(path, 1_000);

    app.analysis_modal.data_quality_plan_field = SetupRow::Intent.index();
    assert!(press(&mut app, KeyCode::Char(' ')).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Intent);

    // id: the key, and required.
    assert!(press(&mut app, KeyCode::Char(' ')).is_none());
    assert!(app.analysis_modal.data_quality_intent_form.is_some());
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Char(' '));
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.analysis_modal.data_quality_intent_form.is_none());

    // status: an allowed set, typed. `?` and `q` are text there.
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char(' '));
    for _ in 0..3 {
        press(&mut app, KeyCode::Tab);
    }
    assert!(app.text_field_focused());
    type_text(&mut app, "open, closed, q?");
    assert!(press(&mut app, KeyCode::Enter).is_none());

    // amount: a bound that is not a number is refused, on the form.
    press(&mut app, KeyCode::Down);
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Tab);
    type_text(&mut app, "ten");
    press(&mut app, KeyCode::Enter);
    let form = app
        .analysis_modal
        .data_quality_intent_form
        .as_ref()
        .unwrap();
    assert!(form.error.as_deref().unwrap().contains("not a number"));
    // Esc drops the form's edits only.
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.data_quality_intent_form.is_none());

    let intent = app.analysis_modal.data_quality_plan.intent.clone();
    assert_eq!(intent.key, vec!["id"]);
    assert!(intent.column("id").unwrap().required);
    assert_eq!(
        intent.column("status").unwrap().allowed,
        vec!["open", "closed", "q?"]
    );
    assert!(intent.column("amount").is_none());
    assert_no_read(&app, &rx);

    // Esc in the list puts back what it held when it opened: nothing.
    press(&mut app, KeyCode::Esc);
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert!(app.analysis_modal.data_quality_plan.intent.is_empty());

    // Enter keeps the list's edits in the draft; Setup's Esc discards them.
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert_eq!(app.analysis_modal.data_quality_page, QualityPage::Setup);
    assert_eq!(app.analysis_modal.data_quality_plan.intent.key, vec!["id"]);
    assert!(app.analysis_modal.setup_edited());
    press(&mut app, KeyCode::Esc);
    assert!(app.analysis_modal.data_quality_plan.intent.is_empty());
    assert_no_read(&app, &rx);
}

/// Intent declared after a sampled run is measured on the rows that run kept: the
/// next Run reads nothing from the source, and its key speaks for those rows only.
#[test]
fn intent_after_a_run_reuses_the_rows_it_read() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("orders.parquet");
    write_orders(&path, 5_000);
    let (mut app, rx) = open_setup(path, 1_000);
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    assert_eq!(drain(&mut app, &rx, next), 1);
    let first = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(first.precision, QualityPrecision::Sampled);
    assert!(
        first.reads.unwrap().reads > 0,
        "the first run reads its sample"
    );
    assert!(first.intent.is_none());

    press(&mut app, KeyCode::Char('e'));
    app.analysis_modal.data_quality_plan_field = SetupRow::Intent.index();
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Char(' '));
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Enter);
    assert_no_read(&app, &rx);

    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::AnalysisDataQualityCompute)));
    assert_eq!(drain(&mut app, &rx, next), 1);
    let results = app.analysis_modal.data_quality_results.clone().unwrap();
    assert_eq!(results.reads.unwrap().reads, 0, "no source read");
    let intent = results.intent.as_ref().unwrap();
    assert_eq!(intent.precision, QualityPrecision::Sampled);
    assert_eq!(intent.evaluated_rows, 1_000);
    let report = datui::quality_report::build_report(&results);
    let checks = datui::quality_report::checks(&results, &report);
    let limits = datui::quality_report::coverage(
        &results,
        &checks,
        app.analysis_modal.quality_result_plan(),
    )
    .limits();
    assert!(
        limits.contains(&"key repeats among 1,000 sampled rows only".to_string()),
        "{limits:?}"
    );
}

/// The report on screen goes to a file with the source deleted: nothing is read.
/// JSON reads back as the versioned schema; Markdown names the findings; a file that
/// exists is overwritten only once confirmed, and declining keeps the dialog.
#[test]
fn the_report_exports_without_reading_the_source() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("orders.parquet");
    write_orders(&path, 3_000);
    let (mut app, rx) = open_setup(path.clone(), 10_000);
    app.analysis_modal.data_quality_plan.intent = datui::quality_intent::DeclaredIntent {
        key: vec!["id".to_string()],
        columns: vec![datui::quality_intent::ColumnIntent {
            allowed: vec!["open".to_string(), "closed".to_string()],
            ..datui::quality_intent::ColumnIntent::new("status")
        }],
    };
    let next = press(&mut app, KeyCode::Enter);
    assert_eq!(drain(&mut app, &rx, next), 1);
    assert!(app.analysis_modal.data_quality_results.is_some());

    std::fs::remove_file(&path).unwrap();

    // Not over Setup: the report on screen, not a draft.
    press(&mut app, KeyCode::Char('x'));
    let form = app.analysis_modal.data_quality_export.as_mut().unwrap();
    assert_eq!(form.path.value(), "orders-quality.json");
    let json = dir.path().join("report.json");
    form.path.set_value(json.display().to_string());
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::QualityReportExport(..))));
    assert!(app.analysis_modal.data_quality_export.is_none());
    drain(&mut app, &rx, next);
    assert!(
        app.flash_message()
            .unwrap()
            .starts_with("Report written to")
    );
    let file: ReportFile = serde_json::from_str(&std::fs::read_to_string(&json).unwrap()).unwrap();
    assert_eq!(file.format, REPORT_FORMAT);
    assert_eq!(file.version, REPORT_VERSION);
    assert_eq!(file.setup.intent.key, vec!["id"]);
    assert_eq!(file.run.precision, "exact");
    assert_eq!(file.run.evaluated_rows, 3_000);
    let source = file.source.unwrap();
    assert_eq!(source.location.unwrap(), path.display().to_string());
    assert!(source.bytes.is_some() && source.modified.is_some());
    assert!(
        file.findings
            .iter()
            .any(|finding| finding.title == "Not allowed")
    );

    // Markdown, the extension following the form.
    press(&mut app, KeyCode::Char('x'));
    press(&mut app, KeyCode::Tab);
    press(&mut app, KeyCode::Right);
    let form = app.analysis_modal.data_quality_export.as_mut().unwrap();
    assert_eq!(form.path.value(), "orders-quality.md");
    form.path
        .set_value(dir.path().join("report").display().to_string());
    let next = press(&mut app, KeyCode::Enter);
    drain(&mut app, &rx, next);
    let markdown = std::fs::read_to_string(dir.path().join("report.md")).unwrap();
    assert!(markdown.starts_with("# Data quality report"), "{markdown}");
    assert!(markdown.contains("**Not allowed** (status)"), "{markdown}");
    assert!(markdown.contains("| Key | id |"), "{markdown}");

    // Over a file that exists: asked first; No keeps the dialog as typed.
    press(&mut app, KeyCode::Char('x'));
    let form = app.analysis_modal.data_quality_export.as_mut().unwrap();
    form.path.set_value(json.display().to_string());
    std::fs::write(&json, "earlier").unwrap();
    assert!(press(&mut app, KeyCode::Enter).is_none());
    assert!(app.confirmation_modal.active);
    press(&mut app, KeyCode::Esc);
    assert!(!app.confirmation_modal.active);
    assert!(app.analysis_modal.data_quality_export.is_some());
    assert_eq!(std::fs::read_to_string(&json).unwrap(), "earlier");
    press(&mut app, KeyCode::Enter);
    press(&mut app, KeyCode::Left);
    let next = press(&mut app, KeyCode::Enter);
    assert!(matches!(next, Some(AppEvent::QualityReportExport(..))));
    drain(&mut app, &rx, next);
    assert!(
        std::fs::read_to_string(&json)
            .unwrap()
            .contains(REPORT_FORMAT)
    );
    assert!(!path.exists(), "the source was never needed");
}
