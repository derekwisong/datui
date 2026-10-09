//! Every confirmation, driven to Yes, to No and to Esc: what each answer does next.

use crate::app::feedback::Confirm;
use crate::export::output_file::Overwrite;
use crate::*;
use crossterm::event::{KeyCode, KeyEvent, KeyModifiers};
use std::path::PathBuf;
use std::sync::mpsc;

fn new_app() -> App {
    let (tx, _rx) = mpsc::channel();
    App::new(tx, crate::tests::test_runtime())
}

fn key(app: &mut App, code: KeyCode) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, KeyModifiers::NONE)))
}

/// Yes: Enter with Yes focused.
fn yes(app: &mut App) -> Option<AppEvent> {
    key(app, KeyCode::Left);
    key(app, KeyCode::Enter)
}

/// No: Enter with No focused.
fn no(app: &mut App) -> Option<AppEvent> {
    key(app, KeyCode::Right);
    key(app, KeyCode::Enter)
}

fn export_request() -> ExportRequest {
    ExportRequest {
        path: PathBuf::from("out.csv"),
        format: crate::export::export_modal::ExportFormat::Csv,
        options: ExportOptions {
            source_file: false,
            csv_delimiter: b',',
            csv_include_header: true,
            csv_compression: None,
            json_compression: None,
            ndjson_compression: None,
        },
        overwrite: Overwrite::Forbid,
    }
}

fn chart_request() -> crate::chart::chart_export::ChartExportRequest {
    crate::chart::chart_export::ChartExportRequest {
        path: PathBuf::from("out.png"),
        format: crate::chart::chart_export::ChartExportFormat::Png,
        options: Default::default(),
        overwrite: Overwrite::Forbid,
        recipe: false,
    }
}

/// Builds one question's `Confirm`.
type Asks = fn() -> Confirm;

/// Every question but leaving, each built fresh.
fn questions() -> Vec<(&'static str, Asks)> {
    vec![
        ("read all", || Confirm::ReadAll),
        ("open link", || {
            Confirm::OpenLink("https://example.com".into())
        }),
        ("clear recents", || Confirm::ClearRecents),
        ("full scan", || Confirm::QualityFullScan),
        ("hide examples", || Confirm::HideExamples),
        ("delete view", || Confirm::DeleteView("no-such-view".into())),
        ("forget place", || {
            Confirm::ForgetPlace(PathBuf::from("/nowhere"))
        }),
        ("quality export", || {
            Confirm::QualityExport(PathBuf::from("r.json"), Default::default())
        }),
        ("chart export", || {
            Confirm::ChartExport(Box::new(chart_request()))
        }),
        ("export", || Confirm::Export(Box::new(export_request()))),
        ("copy", || Confirm::Copy(Default::default(), true)),
        ("download", || Confirm::Download),
        ("reopen", || Confirm::Reopen(None)),
    ]
}

/// No and Esc do nothing that was asked about, and leave nothing armed for the next
/// question to fire.
#[test]
fn no_and_esc_disarm_every_confirmation() {
    for decline in [no as fn(&mut App) -> Option<AppEvent>, |app: &mut App| {
        key(app, KeyCode::Esc)
    }] {
        for (name, asking) in questions() {
            let mut app = new_app();
            app.confirmation_modal.show("?".into(), asking());
            assert!(
                decline(&mut app).is_none(),
                "{name}: declining does nothing"
            );
            assert!(
                !app.confirmation_modal.active,
                "{name}: the question closes"
            );
            assert!(app.confirmation_modal.asking.is_none(), "{name}: disarmed");
        }
    }
}

/// Yes carries on with what was asked about: the overwrites replace, the link opens,
/// the copy runs.
#[test]
fn yes_carries_on_with_what_was_asked() {
    let mut app = new_app();
    app.confirmation_modal
        .show("?".into(), Confirm::OpenLink("https://x.org".into()));
    assert!(
        matches!(yes(&mut app), Some(AppEvent::Applied(crate::Applied::OpenLink(url))) if url == "https://x.org")
    );

    app.confirmation_modal.show(
        "?".into(),
        Confirm::QualityExport(PathBuf::from("r.json"), Default::default()),
    );
    assert!(matches!(
        yes(&mut app),
        Some(AppEvent::Applied(crate::Applied::QualityReportExport(
            _,
            _,
            Overwrite::Replace
        )))
    ));

    app.confirmation_modal
        .show("?".into(), Confirm::ChartExport(Box::new(chart_request())));
    assert!(matches!(
        yes(&mut app),
        Some(AppEvent::Applied(crate::Applied::ChartExport(r))) if r.overwrite == Overwrite::Replace
    ));

    app.confirmation_modal
        .show("?".into(), Confirm::Export(Box::new(export_request())));
    assert!(matches!(
        yes(&mut app),
        Some(AppEvent::Applied(crate::Applied::Export(r))) if r.overwrite == Overwrite::Replace
    ));

    app.confirmation_modal
        .show("?".into(), Confirm::Copy(Default::default(), false));
    assert!(matches!(
        yes(&mut app),
        Some(AppEvent::Applied(crate::Applied::CopyTable {
            header: false,
            ..
        }))
    ));

    app.confirmation_modal
        .show("?".into(), Confirm::Reopen(None));
    assert!(matches!(
        yes(&mut app),
        Some(AppEvent::Applied(crate::Applied::Reopen(None)))
    ));
    assert!(!app.confirmation_modal.active);
}

/// Yes on the home screen's questions: the status line they leave is cleared, and the
/// question closes.
#[test]
fn yes_on_the_home_questions_closes_them() {
    for asking in [
        Confirm::ClearRecents,
        Confirm::ForgetPlace(PathBuf::from("/nowhere")),
        Confirm::HideExamples,
        Confirm::DeleteView("no-such-view".into()),
    ] {
        let mut app = new_app();
        app.home.status = Some("Forget them?".into());
        let forgets = matches!(asking, Confirm::ClearRecents | Confirm::ForgetPlace(_));
        app.confirmation_modal.show("?".into(), asking);
        assert!(yes(&mut app).is_none());
        assert!(!app.confirmation_modal.active);
        if forgets {
            assert_eq!(app.home.status, None);
        }
    }
}

/// A declined overwrite returns to the filled form behind it.
#[test]
fn a_declined_overwrite_returns_to_its_form() {
    for decline in [no as fn(&mut App) -> Option<AppEvent>, |app: &mut App| {
        key(app, KeyCode::Esc)
    }] {
        let mut app = new_app();
        app.confirmation_modal
            .show("?".into(), Confirm::Export(Box::new(export_request())));
        decline(&mut app);
        assert!(matches!(app.overlay, Overlay::Export { .. }));

        let mut app = new_app();
        app.overlay = Overlay::Chart;
        app.confirmation_modal
            .show("?".into(), Confirm::ChartExport(Box::new(chart_request())));
        decline(&mut app);
        assert_eq!(app.overlay, Overlay::ChartExport);
    }
}

/// Leaving while recording: both choices leave, and Esc stays.
#[test]
fn leaving_while_recording_leaves_on_either_choice_and_stays_on_esc() {
    for answer in [yes as fn(&mut App) -> Option<AppEvent>, no] {
        let mut app = new_app();
        app.confirmation_modal.show_choice(
            "?".into(),
            "Stop recording",
            "Keep recording",
            Confirm::Leave(Leaving::Quit),
        );
        assert!(matches!(answer(&mut app), Some(AppEvent::Exit)));
        assert!(!app.confirmation_modal.active);
    }
    let mut app = new_app();
    app.confirmation_modal.show_choice(
        "?".into(),
        "Stop recording",
        "Keep recording",
        Confirm::Leave(Leaving::Quit),
    );
    assert!(key(&mut app, KeyCode::Esc).is_none());
    assert!(!app.confirmation_modal.active);
}

/// Yes on Read all reads every row; on a full scan, runs it.
#[test]
fn yes_reads_all_and_runs_the_full_scan() {
    let mut app = new_app();
    app.confirmation_modal.show("?".into(), Confirm::ReadAll);
    yes(&mut app);
    assert_eq!(
        app.analysis_modal.sample.method,
        crate::analysis::sampling::SampleMethod::EveryRow
    );

    let mut app = new_app();
    app.confirmation_modal
        .show("?".into(), Confirm::QualityFullScan);
    assert!(matches!(
        yes(&mut app),
        Some(AppEvent::AnalysisCompute(
            analysis::analysis_modal::AnalysisTool::DataQuality
        ))
    ));
    assert!(app.analysis_modal.computing.is_some());
}

/// Yes on an open's question reads what was asked about.
#[test]
fn yes_on_an_open_reads_it() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("small.json");
    std::fs::write(&path, r#"[{"id":1},{"id":2}]"#).unwrap();
    let mut config = AppConfig::default();
    // Every in-memory read asks.
    config.read.memory_warning = crate::config::ByteSize(1);
    let theme = Theme::from_config(&config.theme).unwrap();
    let (tx, rx) = mpsc::channel();
    let mut app = App::new_with_config(tx, crate::tests::test_runtime(), theme, config);
    let mut next = Some(AppEvent::Open(vec![path], OpenOptions::default()));
    let pump = |app: &mut App, mut next: Option<AppEvent>, done: &dyn Fn(&App) -> bool| {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(60);
        while !done(app) {
            assert!(
                std::time::Instant::now() < deadline,
                "the open never got there"
            );
            if let Some(event) = next
                .take()
                .or_else(|| rx.recv_timeout(std::time::Duration::from_millis(50)).ok())
            {
                next = app.event(event);
            }
        }
    };
    pump(&mut app, next.take(), &|app| {
        app.awaiting_open_confirmation()
    });
    assert!(app.data_table_state.is_none(), "nothing read before Yes");
    let next = yes(&mut app);
    pump(&mut app, next, &|app| app.data_table_state.is_some());
    assert!(!app.confirmation_modal.active);
}
