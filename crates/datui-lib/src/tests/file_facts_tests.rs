use crate::table::DataTableState;
use crate::*;
use polars::prelude::{IntoLazy, ParquetWriter};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;

type Answer = std::result::Result<FileFacts, String>;

/// A reader that counts its calls, notes any made on the test's own thread, and
/// answers only when the test sends it an answer.
struct Gate {
    calls: Arc<AtomicUsize>,
    on_ui_thread: Arc<AtomicUsize>,
    answer: mpsc::Sender<Answer>,
}

fn gated(app: &mut App) -> Gate {
    let calls = Arc::new(AtomicUsize::new(0));
    let on_ui_thread = Arc::new(AtomicUsize::new(0));
    let (answer, answers) = mpsc::channel::<Answer>();
    let answers = Mutex::new(answers);
    let ui = std::thread::current().id();
    let (counted, misplaced) = (calls.clone(), on_ui_thread.clone());
    app.file_facts_reader = Some(Arc::new(
        move |_path: &Path, _facts: Option<crate::formats::readers::Facts>| {
            counted.fetch_add(1, Ordering::SeqCst);
            if std::thread::current().id() == ui {
                misplaced.fetch_add(1, Ordering::SeqCst);
            }
            answers
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .recv()
                .unwrap_or_else(|_| Err("the test ended".to_string()))
        },
    ));
    Gate {
        calls,
        on_ui_thread,
        answer,
    }
}

fn app() -> (App, mpsc::Receiver<AppEvent>, mpsc::Sender<AppEvent>) {
    let (tx, rx) = mpsc::channel();
    let app = App::new(tx.clone(), crate::tests::test_runtime());
    (app, rx, tx)
}

/// Install a three-row dataset as though it was opened from `path`, which nothing
/// here reads: the reader is the test's.
fn install(app: &mut App, path: &str) {
    let lf = polars::df!("a" => [1i64, 2, 3]).unwrap().lazy();
    let state = DataTableState::from_lazyframe(lf, &OpenOptions::default()).unwrap();
    app.install_for_tests(
        state,
        Some(PathBuf::from(path)),
        &OpenOptions::default(),
        None,
    );
    app.status_message = None;
    app.input_mode = InputMode::Normal;
}

fn press(app: &mut App, code: KeyCode, modifiers: KeyModifiers) -> Option<AppEvent> {
    app.event(AppEvent::Key(KeyEvent::new(code, modifiers)))
}

/// `i`, then → to the Resources tab, where the file size is: past a Parquet tab.
fn open_resources(app: &mut App) {
    press(app, KeyCode::Char('i'), KeyModifiers::NONE);
    assert_eq!(app.overlay, Overlay::Info);
    for _ in 0..3 {
        press(app, KeyCode::Right, KeyModifiers::NONE);
        if app.info_modal.active_tab == crate::widgets::info::InfoTab::Resources {
            break;
        }
    }
}

/// A frame at 80×24, as text.
fn screen(app: &mut App) -> String {
    let area = Rect::new(0, 0, 80, 24);
    let mut buf = Buffer::empty(area);
    Widget::render(&mut *app, area, &mut buf);
    crate::tests::buffer_text(&buf)
}

fn file_size_line(text: &str) -> &str {
    text.lines()
        .find(|line| line.contains("File size:"))
        .unwrap_or_else(|| panic!("no File size row:\n{text}"))
}

fn reading(app: &App) -> bool {
    matches!(app.file_facts(), Some(FileFacts::Reading))
}

fn read(size: u64) -> Answer {
    Ok(FileFacts::Read {
        size: Some(size),
        footer: None,
        detail: None,
    })
}

/// Opening Info reads nothing where keys are handled or frames drawn. While the
/// read waits, frames draw the wait, keys act without being held, and asking
/// again — reopening the panel, drawing more frames — starts no second read.
#[test]
fn info_draws_and_answers_keys_while_its_read_waits() {
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "/nowhere/facts.parquet");

    open_resources(&mut app);
    assert!(reading(&app), "the read was asked for");
    assert!(!app.is_busy(), "and nothing waits on it: no key is held");

    for _ in 0..3 {
        let text = screen(&mut app);
        assert!(
            file_size_line(&text).contains("reading..."),
            "the panel draws the wait:\n{text}"
        );
    }
    // Esc and `i` act at once, and the panel opened again asks nothing new.
    press(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    assert!(app.at_table());
    open_resources(&mut app);
    let _ = screen(&mut app);

    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |_| {
        gate.calls.load(Ordering::SeqCst) == 1
    });
    assert_eq!(
        gate.on_ui_thread.load(Ordering::SeqCst),
        0,
        "the read was made on a worker"
    );

    gate.answer.send(read(2048)).unwrap();
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a));
    let text = screen(&mut app);
    assert!(
        file_size_line(&text).contains("2.0 KiB"),
        "and draws the answer once it lands:\n{text}"
    );
    press(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    open_resources(&mut app);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1, "one read per dataset");
}

/// Quit and home act at once from the panel while the read is still out, and its
/// answer landing later moves nothing.
#[test]
fn quit_and_home_do_not_wait_on_the_read() {
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "/nowhere/facts.parquet");
    open_resources(&mut app);
    assert!(reading(&app));

    assert!(matches!(
        press(&mut app, KeyCode::Char('q'), KeyModifiers::CONTROL),
        Some(AppEvent::Exit)
    ));
    press(&mut app, KeyCode::Char('o'), KeyModifiers::CONTROL);
    assert_eq!(app.input_mode, InputMode::Home, "home, without waiting");
    assert!(!app.is_busy());

    gate.answer.send(read(1)).unwrap();
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a));
    assert_eq!(app.input_mode, InputMode::Home);
}

/// A read that fails says why on the panel, once: drawing does not ask again, and
/// neither does reopening the panel. The next dataset does.
#[test]
fn a_failed_read_is_shown_and_not_retried() {
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "/nowhere/facts.parquet");
    open_resources(&mut app);
    gate.answer
        .send(Err("Permission denied (os error 13)".to_string()))
        .unwrap();
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a));
    assert!(!app.is_busy(), "the failure leaves nothing pending");
    assert!(!app.error_modal.active, "the panel says it; no modal");

    for _ in 0..3 {
        let text = screen(&mut app);
        assert!(
            file_size_line(&text).contains("Permission denied"),
            "the panel says why:\n{text}"
        );
    }
    press(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    open_resources(&mut app);
    let _ = screen(&mut app);
    assert_eq!(gate.calls.load(Ordering::SeqCst), 1, "not asked again");

    // The panel is still up as the next dataset arrives, so that one is asked.
    install(&mut app, "/nowhere/next.parquet");
    assert!(reading(&app), "a new dataset is a new read");
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |_| {
        gate.calls.load(Ordering::SeqCst) == 2
    });
}

/// An answer for a dataset that has since been replaced is dropped, whether it is
/// a result or a failure; the dataset on screen keeps waiting on its own.
#[test]
fn an_answer_for_a_replaced_dataset_is_dropped() {
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "/nowhere/old.parquet");
    let old = app.dataset_generation;
    open_resources(&mut app);
    press(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    install(&mut app, "/nowhere/new.parquet");
    assert!(
        app.file_facts().is_none(),
        "the old read is not the new one's"
    );
    // The old worker answers into a dataset that is gone.
    gate.answer.send(read(1)).unwrap();
    loop {
        let event = rx
            .recv_timeout(std::time::Duration::from_secs(300))
            .expect("the old read answers");
        let answered =
            matches!(event, AppEvent::JobEnded(t) if t.kind() == crate::JobKind::FileFacts);
        app.event(event);
        if answered {
            break;
        }
    }
    assert!(app.file_facts().is_none(), "and its answer is dropped");

    open_resources(&mut app);
    assert!(reading(&app));
    // Late answers for the old dataset, of either kind, leave the new read waiting.
    app.answer_for_tests(
        Job::FileFacts { dataset: old },
        crate::Answer::FileFacts(FileFacts::Read {
            size: Some(1),
            footer: None,
            detail: None,
        }),
    );
    let late = app.job_for_tests(Job::FileFacts { dataset: old }, None);
    let ticket = late.ticket();
    late.end(Outcome::Failed {
        message: "not this one".to_string(),
        panicked: false,
    });
    app.event(AppEvent::JobEnded(ticket));
    assert!(reading(&app), "the new dataset is still waiting on its own");

    gate.answer.send(read(5)).unwrap();
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a));
    assert!(matches!(
        app.file_facts(),
        Some(FileFacts::Read { size: Some(5), .. })
    ));
    assert_eq!(gate.calls.load(Ordering::SeqCst), 2);
}

/// A worker that panics ends the read the way a failure does: the panel says so,
/// nothing stays pending, and nothing asks again.
#[test]
fn a_read_whose_worker_dies_fails_once() {
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    app.jobs.worker_dies =
        crate::tests::worker_dies_once(|job| matches!(job, Job::FileFacts { .. }));
    install(&mut app, "/nowhere/facts.parquet");
    open_resources(&mut app);
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a));
    assert!(matches!(app.file_facts(), Some(FileFacts::Failed(_))));
    assert!(!app.is_busy());
    assert!(!app.error_modal.active);
    let text = screen(&mut app);
    assert!(
        file_size_line(&text).contains("see the log"),
        "the panel says it could not read:\n{text}"
    );
    press(&mut app, KeyCode::Esc, KeyModifiers::NONE);
    open_resources(&mut app);
    assert!(matches!(app.file_facts(), Some(FileFacts::Failed(_))));
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0, "no second read");
}

/// A glob, or several files, is no one file to stat: the panel shows no size and
/// starts no read, rather than a failure or the first file's size.
#[test]
fn a_glob_or_several_files_read_nothing() {
    let (mut app, _rx, _tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "/nowhere/*.parquet");
    open_resources(&mut app);
    assert!(app.file_facts().is_none());
    press(&mut app, KeyCode::Esc, KeyModifiers::NONE);

    install(&mut app, "/nowhere/a.parquet");
    app.source.opened = Some((
        vec![
            PathBuf::from("/nowhere/a.parquet"),
            PathBuf::from("/nowhere/b.parquet"),
        ],
        OpenOptions::default(),
    ));
    open_resources(&mut app);
    assert!(app.file_facts().is_none());
    let text = screen(&mut app);
    let line = file_size_line(&text);
    assert!(line.contains(crate::glyphs::get().dash), "{line}");
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
}

/// The Parquet tab and the Compression column have their room before the footer
/// lands, so nothing on the tab bar or the Schema tab moves when it does.
#[test]
fn nothing_moves_when_the_footer_lands() {
    let dir = tempfile::tempdir().unwrap();
    let file = dir.path().join("facts.parquet");
    let mut df = polars::df!("a" => [1i64, 2, 3]).unwrap();
    ParquetWriter::new(std::fs::File::create(&file).unwrap())
        .finish(&mut df)
        .unwrap();
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    install(&mut app, &file.to_string_lossy());

    let row_of = |text: &str, label: &str| {
        text.lines()
            .position(|line| line.contains(label))
            .unwrap_or_else(|| panic!("no {label} row:\n{text}"))
    };
    press(&mut app, KeyCode::Char('i'), KeyModifiers::NONE);
    let schema = screen(&mut app);
    // The Compression column is up before there is anything to put in it.
    let header = schema.lines().nth(row_of(&schema, "Compression")).unwrap();
    let header = header.to_string();
    // The footer's own tab is on the bar, named, before the footer is.
    let bar = row_of(&schema, "Resources");
    assert!(
        schema.lines().nth(bar).unwrap().contains("Parquet"),
        "{schema}"
    );
    press(&mut app, KeyCode::Right, KeyModifiers::NONE);
    let waiting = screen(&mut app);
    assert_eq!(row_of(&waiting, "Resources"), bar, "{waiting}");
    assert!(waiting.contains("reading..."), "{waiting}");

    let facts = crate::formats::readers::of(crate::FileFormat::Parquet).facts;
    gate.answer.send(FileFacts::read(&file, facts)).unwrap();
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a) && !a.is_busy());
    let landed = screen(&mut app);
    assert_eq!(row_of(&landed, "Resources"), bar, "{landed}");
    assert!(landed.contains("3 rows in 1 row group"), "{landed}");
    press(&mut app, KeyCode::Left, KeyModifiers::NONE);
    let schema = screen(&mut app);
    assert_eq!(
        schema.lines().nth(row_of(&schema, "Compression")).unwrap(),
        header
    );
}

/// A reason longer than the panel is cut with a mark, never silently.
#[test]
fn a_long_reason_is_cut_with_a_mark() {
    let (mut app, rx, tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "/nowhere/facts.csv");
    open_resources(&mut app);
    gate.answer.send(Err("word ".repeat(40))).unwrap();
    super::chart_prepare_tests::pump(&mut app, &rx, &tx, |a| !reading(a));
    let text = screen(&mut app);
    let line = file_size_line(&text);
    assert!(line.contains(crate::glyphs::get().ellipsis), "{line}");
}

/// Installing a hive dataset takes what the open's worker found and looks at
/// nothing itself: a directory on the path is not counted by its footers unless the
/// worker said so.
#[test]
fn installing_a_hive_dataset_looks_at_no_directory() {
    let (mut app, _rx, _tx) = app();
    let dir = tempfile::tempdir().unwrap();
    let lf = polars::df!("a" => [1i64]).unwrap().lazy();
    let options = OpenOptions {
        hive: true,
        ..OpenOptions::default()
    };
    let state = DataTableState::from_lazyframe(lf, &options).unwrap();
    app.install_for_tests(state, Some(dir.path().to_path_buf()), &options, None);
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.parquet_count_dir(), None);
}

/// A source with no file on this machine has nothing to read: the panel shows no
/// size and starts no read.
#[test]
fn a_remote_source_reads_nothing() {
    let (mut app, _rx, _tx) = app();
    let gate = gated(&mut app);
    install(&mut app, "s3://bucket/facts.parquet");
    open_resources(&mut app);
    assert!(app.file_facts().is_none());
    assert!(!app.is_busy());
    let text = screen(&mut app);
    let line = file_size_line(&text);
    assert!(
        line.contains(crate::glyphs::get().dash) && !line.contains("reading"),
        "no size, and no wait for one: {line}"
    );
    assert_eq!(gate.calls.load(Ordering::SeqCst), 0);
}
