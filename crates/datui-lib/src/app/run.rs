//! The process around the app: the terminal session, the event loop's thread, the
//! signals that end it, and handing a file to the system's opener.

use crate::app::terminal::{
    QuietTerminal, TakenTerminal, follow_focus, push_keyboard_flags, restore_terminal, take_screen,
};
use crate::config::AppConfig;
use crate::view::Views;
use crate::{
    App, AppEvent, RunInput, app::event_pump, app::pointer, app::startup, app::terminal_color,
    app::terminal_input, glyphs, inspector::external_open, logging,
};
use color_eyre::Result;
use crossterm::event::{KeyCode, KeyModifiers};
use polars::prelude::LazyFrame;

/// What datui hands to other programs: values to open, where they are written, and the
/// clipboard.
#[derive(Default)]
pub struct External {
    /// A value the inspector wrote for another program, for the run loop to open (it
    /// owns the terminal a waiting program takes).
    pub(crate) open: Option<crate::inspector::external_open::ExternalOpen>,
    /// Where those values are written; removed when the app is.
    pub(crate) open_dir: Option<tempfile::TempDir>,
    /// Where copies go, built at the first copy and kept: on Wayland and X11 the
    /// clipboard offer dies with its owner.
    pub(crate) clipboard: Option<Box<dyn crate::clipboard::Destination>>,
}

/// Standard input and output when datui sits in a pipe, and the recording of what it read.
#[derive(Default)]
pub struct Pipes {
    /// What `-` reads in place of standard input: a test's pipe.
    pub(crate) stdin_reader: Option<Box<dyn std::io::Read + Send>>,
    /// Where `--tee -` passes the stream on: standard output as the process got it.
    pub(crate) stdout_pass: Option<Box<dyn std::io::Write + Send>>,
    /// The follow mark as last drawn, so its clock redraws only when it changes.
    pub(crate) follow_drawn: Option<crate::render::footer::FollowMark>,
    /// A recording kept going after the user went home or quit, until its stream ends.
    pub(crate) recording_on: Option<std::sync::Arc<crate::loading::follow::SpoolHandle>>,
    /// A recording's end has been said: once, in the bar or the error dialog.
    pub(crate) recording_end_said: bool,
}

/// Restore the terminal and turn how the loop ended into `run_impl`'s result. The
/// reader stops first so nothing typed afterwards is read; the capture comes after,
/// so a refused one still leaves the terminal usable.
fn conclude(
    end: event_pump::Ended,
    app: &App,
    capture: bool,
    reader: &mut terminal_input::TerminalInput,
    screen: &mut TakenTerminal,
) -> Result<Option<LazyFrame>> {
    reader.stop();
    screen.restore();
    match end {
        event_pump::Ended::Quit if capture => app.capture_view(),
        event_pump::Ended::Quit => Ok(None),
        event_pump::Ended::Crash(msg) => Err(color_eyre::eyre::eyre!(msg)),
        event_pump::Ended::NotFound(path) => Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("File not found: {}", path.display()),
        )
        .into()),
    }
}

/// The exit status the ending signal calls for, set once; 0 until then.
static ENDED_BY_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// The exit status when a signal ended the session [`run`] returned from: `128 + n`
/// for SIGTERM, SIGHUP or SIGINT (as `datui_cli::exit`), and on Windows a closed
/// console's status.
pub fn ended_by_signal() -> Option<i32> {
    let status = ENDED_BY_SIGNAL.load(std::sync::atomic::Ordering::SeqCst);
    (status != 0).then_some(status)
}

/// A session-ending signal: quit as `q` does, handing back the screen and removing
/// temp files. A second signal, or a session still running after a few seconds,
/// ends the process at once, so a stuck loop cannot make datui unkillable. Called on
/// the runtime.
fn end_session(status: i32, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use std::sync::atomic::Ordering;
    // Longer than the exit sweep's grace, shorter than Windows' five seconds for closing
    // a console.
    const STRAGGLE: std::time::Duration = std::time::Duration::from_secs(3);
    fn end_now(status: i32) -> ! {
        restore_terminal();
        std::process::exit(status)
    }
    if ENDED_BY_SIGNAL
        .compare_exchange(0, status, Ordering::SeqCst, Ordering::SeqCst)
        .is_err()
    {
        end_now(ENDED_BY_SIGNAL.load(Ordering::SeqCst));
    }
    let _ = tx.send(AppEvent::Exit);
    tokio::spawn(async move {
        tokio::time::sleep(STRAGGLE).await;
        end_now(status);
    });
}

/// End the session on SIGTERM, SIGHUP (the terminal closing) or SIGINT (`kill -INT`;
/// Ctrl+C at the terminal is a key, not this signal).
#[cfg(unix)]
fn quit_on_signals(runtime: &tokio::runtime::Handle, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use tokio::signal::unix::{SignalKind, signal};
    // `signal` registers with the runtime it is called in.
    let _runtime = runtime.enter();
    for kind in [
        SignalKind::terminate(),
        SignalKind::hangup(),
        SignalKind::interrupt(),
    ] {
        let Ok(mut arrivals) = signal(kind) else {
            continue;
        };
        let tx = tx.clone();
        runtime.spawn(async move {
            while arrivals.recv().await.is_some() {
                end_session(128 + kind.as_raw_value(), &tx);
            }
        });
    }
}

/// End the session when the console closes, the user logs off or the machine shuts
/// down. Tokio holds the control handler until exit, so the quit runs first.
#[cfg(windows)]
fn quit_on_signals(runtime: &tokio::runtime::Handle, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use tokio::signal::windows::{ctrl_close, ctrl_logoff, ctrl_shutdown};
    /// STATUS_CONTROL_C_EXIT, what a console process ends with when closed.
    const CLOSED: i32 = 0xC000_013A_u32 as i32;
    // Each registers with the runtime it is called in.
    let _runtime = runtime.enter();
    // Three listener types with one shape and no trait in common.
    macro_rules! quit_on {
        ($listen:expr) => {
            if let Ok(mut arrivals) = $listen {
                let tx = tx.clone();
                runtime.spawn(async move {
                    while arrivals.recv().await.is_some() {
                        end_session(CLOSED, &tx);
                    }
                });
            }
        };
    }
    quit_on!(ctrl_close());
    quit_on!(ctrl_logoff());
    quit_on!(ctrl_shutdown());
}

/// Run the TUI with file paths or an existing LazyFrame: the one event loop for the
/// CLI and the Python binding.
pub fn run(input: RunInput, config: Option<AppConfig>) -> Result<()> {
    run_impl(input, config, false).map(|_| ())
}

/// As `run`, but a normal quit returns the active table's final view (the Python
/// binding's `capture=True`); `None` with no dataset. See `App::capture_view`.
pub fn run_captured(input: RunInput, config: Option<AppConfig>) -> Result<Option<LazyFrame>> {
    run_impl(input, config, true)
}

fn run_impl(
    input: RunInput,
    config: Option<AppConfig>,
    capture: bool,
) -> Result<Option<LazyFrame>> {
    use event_pump::EventPump;
    use std::io::Write;

    // First, so a missing file is named with the home directory expanded.
    let input = startup::expand_home(input);
    use std::sync::{Mutex, Once, mpsc};

    // Saved views are read on a worker; the first user waits for the rest.
    let views = Views::read_in_background();

    // Install color_eyre at most once per process (e.g. repeated datui.view() in Python).
    static COLOR_EYRE_INIT: Once = Once::new();
    static INSTALL_RESULT: Mutex<Option<Result<(), color_eyre::Report>>> = Mutex::new(None);
    COLOR_EYRE_INIT.call_once(|| {
        *INSTALL_RESULT.lock().unwrap_or_else(|e| e.into_inner()) = Some(color_eyre::install());
    });
    if let Some(Err(e)) = INSTALL_RESULT
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
    {
        return Err(color_eyre::eyre::eyre!(e.to_string()));
    }
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .map_err(|e| color_eyre::eyre::eyre!("Failed to create tokio runtime: {}", e))?;

    // Dropping the runtime joins its blocking pool, so quit would wait on an in-flight
    // count (minutes over a huge hive). Shut it down in the background instead; the
    // read-only tasks die with the process. Covers every return path.
    struct RtGuard(Option<tokio::runtime::Runtime>);
    impl Drop for RtGuard {
        fn drop(&mut self) {
            if let Some(rt) = self.0.take() {
                rt.shutdown_background();
            }
        }
    }
    let rt_guard = RtGuard(Some(rt));
    let rt_handle = rt_guard
        .0
        .as_ref()
        .expect("runtime present")
        .handle()
        .clone();

    // `--tee -` passes the stream to stdout, so the screen is drawn on the terminal;
    // the original stdout is kept for the copy.
    let passed = match &input {
        RunInput::Cli(args)
            if args
                .tee
                .as_deref()
                .is_some_and(crate::loading::stdin::is_stdin) =>
        {
            Some(crate::loading::tee::pass_stdout_on().map_err(|e| color_eyre::eyre::eyre!(e))?)
        }
        _ => None,
    };
    let mut terminal = match take_screen() {
        Ok(terminal) => QuietTerminal::new(terminal),
        Err(e) => {
            // No screen to keep up: an unusable config or a missing named file is said first.
            if config.is_none() {
                startup::load_config(&input)?;
            }
            // Without a screen nothing is opened, so no specs are loaded to look.
            if let Some(missing) =
                App::missing_named_path(startup::named_paths(&input), &Default::default())
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("File not found: {}", missing.display()),
                )
                .into());
            }
            return Err(color_eyre::eyre::eyre!(
                "datui requires an interactive terminal (TTY). No terminal detected: {}. \
                 There is no TTY inside a Jupyter notebook or when output is piped or \
                 redirected; run from a terminal with stdout connected to it.",
                e
            ));
        }
    };
    // Handed back on every way out, after the reader below lets go.
    let mut screen = TakenTerminal { restored: false };
    // stderr goes to the log until this drops, so nothing draws over the screen.
    let session = logging::TuiSession::begin(restore_terminal);
    push_keyboard_flags();
    // Asked before reading settings, so the answer is usually in by then; dropped under
    // an explicit `theme.mode`. The reader takes it off the input stream.
    let asked = terminal_color::supported()
        && config.as_ref().is_none_or(|c| c.theme.follow)
        && terminal_color::ask(&mut std::io::stdout());
    let mut background = None;
    let (tx, rx) = mpsc::channel::<AppEvent>();
    {
        let tx = tx.clone();
        session.wake_with(move || {
            let _ = tx.send(AppEvent::Wake);
        });
    }
    let mut reader = terminal_input::TerminalInput::start(tx.clone())?;
    // Only for the datui binary: handlers last the process, and hosts like Python keep
    // their own.
    #[cfg(any(unix, windows))]
    if matches!(input, RunInput::Cli(_)) {
        quit_on_signals(&rt_handle, &tx);
    }

    // Settings are files, read on a worker while keys are read: a slow mount shows a
    // screen, and Ctrl+C or Ctrl+Q leave it.
    let waiting_on = startup::named(&input);
    {
        let tx = tx.clone();
        std::thread::Builder::new()
            .name("datui-settings".into())
            .spawn(move || {
                let read = logging::catch_panic(|| startup::read(input, config))
                    .unwrap_or_else(|panic| Err(color_eyre::eyre::eyre!(panic)));
                let _ = tx.send(AppEvent::SettingsRead(Box::new(read)));
            })?;
    }
    let mut backlog = Vec::new();
    let grace_ends = std::time::Instant::now() + startup::GRACE;
    let mut waiting_shown = false;
    let settings = loop {
        let timeout = if waiting_shown {
            std::time::Duration::MAX
        } else {
            grace_ends.saturating_duration_since(std::time::Instant::now())
        };
        match rx.recv_timeout(timeout) {
            Ok(AppEvent::SettingsRead(read)) => break *read,
            Ok(AppEvent::TerminalBackground(mode)) => background = Some(mode),
            Ok(AppEvent::Terminal(crossterm::event::Event::Key(key)))
                if key.modifiers.contains(KeyModifiers::CONTROL)
                    && matches!(key.code, KeyCode::Char('c') | KeyCode::Char('q')) =>
            {
                reader.stop();
                screen.restore();
                return Ok(None);
            }
            Ok(AppEvent::Crash(msg)) => {
                reader.stop();
                screen.restore();
                return Err(color_eyre::eyre::eyre!(msg));
            }
            // A signal, before there was an app to quit.
            Ok(AppEvent::Exit) => {
                reader.stop();
                screen.restore();
                return Ok(None);
            }
            Ok(event) => {
                if waiting_shown
                    && matches!(
                        event,
                        AppEvent::Terminal(crossterm::event::Event::Resize(..))
                    )
                {
                    terminal.draw(|frame| startup::draw_waiting(frame, waiting_on.as_deref()))?;
                }
                // Typed before there was an app to take it: handled, in order, first.
                backlog.push(event);
            }
            Err(_) => {
                terminal.draw(|frame| startup::draw_waiting(frame, waiting_on.as_deref()))?;
                waiting_shown = true;
            }
        }
    };
    let startup::Settings {
        config,
        theme,
        input,
        opts,
        notes,
    } = match settings {
        Ok(settings) => settings,
        Err(e) => {
            reader.stop();
            screen.restore();
            return Err(e);
        }
    };

    // Polars sizes its pool from the environment at its first compute, after this.
    // Only in the binary's own process: a host's threads may read the environment
    // outside std's lock, and it is not datui's to change.
    let asked_threads = std::env::var_os("POLARS_MAX_THREADS");
    if matches!(input, RunInput::Cli(_))
        && let Some(threads) =
            startup::polars_threads(config.performance.threads, asked_threads.as_deref())
    {
        // SAFETY: the other threads alive now (the key reader, the runtime's idle
        // workers, the saved-views reader) read the environment only through std,
        // which locks it, and none is in foreign code that reads it unlocked.
        unsafe { std::env::set_var("POLARS_MAX_THREADS", threads) };
    }

    // The first frame does not wait for the terminal's answer: one already in, else
    // this terminal's last one (see `App::settle_first_palette`).
    let background = (asked && config.theme.follow)
        .then(|| startup::take_answer(&rx, background, &mut backlog))
        .flatten();
    // Focus reports: under `auto` the background is asked again, and a frame back in
    // focus is repainted whole, since a scroll moved by the terminal carries along
    // whatever drifted on screen meanwhile.
    let focus_reports =
        (config.theme.follow && terminal_color::supported()) || config.display.scroll_region;
    if focus_reports {
        follow_focus(&mut std::io::stdout());
    }

    // Choose glyphs before the first frame: without UTF-8, box drawing renders as
    // replacement boxes.
    glyphs::init_with_overrides(config.display.unicode, &config.glyphs.overrides);
    crate::limits::set(config.limits);

    // Taken once the settings say so; handed back with the screen.
    pointer::capture(config.display.mouse, &mut std::io::stdout());
    terminal.scroll_with_region(config.display.scroll_region);

    let mut app = App::new_with_views(tx.clone(), rt_handle, theme, config, views);
    app.settle_first_palette(background);
    if let Some(out) = passed {
        app.pass_stdout_to(out);
    }
    app.source.startup_view = opts.view.clone();
    // A developer's overlay: an environment variable, not a flag.
    let debug_env = std::env::var_os("DATUI_DEBUG").is_some_and(|v| !v.is_empty() && v != "0");
    if opts.debug || debug_env {
        app.enable_debug();
    }

    // Show the first frame immediately; the open it announces is handled right after.
    let open = match input {
        // No paths: open the home screen instead of loading anything.
        RunInput::Paths(paths, _) if paths.is_empty() => {
            app.enter_home();
            None
        }
        RunInput::Paths(paths, opts) => {
            // Whether each path exists, and is a directory, is asked on a worker after this
            // frame, which names what is being opened.
            app.set_loading_phase("Scanning input", 10);
            if let [path] = paths.as_slice() {
                app.name_what_is_loading(path.clone());
            }
            Some(AppEvent::OpenNamed(paths, opts))
        }
        RunInput::LazyFrame(lf, opts) => {
            app.set_loading_phase("Scanning input", 10);
            Some(AppEvent::OpenLazyFrame(lf, opts))
        }
        RunInput::Cli(_) | RunInput::Host(..) => {
            unreachable!("read_settings resolves the command line")
        }
    };
    // Declared before the pump so it drops after: the app's files go with it, then
    // this removes what workers were still writing.
    let _sweep = app.exit_sweep();
    let input_tx = tx.clone();
    let mut pump = EventPump::new(app, tx, rx);
    // The open goes before keys typed during settings, so they meet it as an open in
    // flight: Ctrl+O puts it down, `q` quits.
    pump.handle_first(backlog.into_iter().chain(open));
    let end = pump.run(|app| {
        if let Some(open) = app.take_external_open() {
            let mouse = app.mouse_enabled();
            let note = open_externally(
                &open,
                &mut reader,
                &input_tx,
                mouse,
                focus_reports,
                &mut terminal,
            );
            app.external_opened(&open, note);
        }
        // Between frames, so the question is never written into the middle of one.
        if app.take_background_query() && terminal_color::supported() {
            terminal_color::ask(&mut std::io::stdout());
        }
        if app.take_repaint() {
            terminal.repaint();
        }
        terminal.draw(|frame| frame.render_widget(app, frame.area()))?;
        Ok(())
    })?;
    let result = conclude(end, &pump.app, capture, &mut reader, &mut screen);
    // stderr is the terminal again once the session is over.
    drop(session);
    // Not `eprintln!`, which panics when a hangup has taken the terminal away.
    for note in notes {
        let _ = writeln!(std::io::stderr(), "datui: {note}");
    }
    // Quit with the recording kept going: it continues to its stream's end with the
    // terminal handed back.
    if let Some((tee, handle)) = pump.app.recording_after_exit() {
        let to = if tee.to_stdout() {
            format!("passing standard input on to {}", tee.name())
        } else {
            format!("recording standard input to {}", tee.path.display())
        };
        let _ = writeln!(
            std::io::stderr(),
            "datui: {to} until it ends (Ctrl+C stops it)"
        );
        handle.spool().wait();
        let done = if tee.to_stdout() {
            "datui: standard input ended".to_string()
        } else {
            format!("datui: saved {}", tee.path.display())
        };
        let _ = writeln!(std::io::stderr(), "{done}");
    }
    result
}

/// Open a value the inspector wrote: a terminal program gets the terminal (reader
/// stopped, screen and raw mode handed back) until it returns; an opener is only
/// started. Returns what went wrong, if anything.
fn open_externally(
    open: &external_open::ExternalOpen,
    reader: &mut terminal_input::TerminalInput,
    tx: &std::sync::mpsc::Sender<AppEvent>,
    mouse: bool,
    focus: bool,
    terminal: &mut QuietTerminal,
) -> Option<String> {
    let program = external_open::program_for(open.document, |name| std::env::var(name).ok());
    let result = match &program {
        external_open::Program::Opener(_) => external_open::run(&program, &open.path),
        external_open::Program::Wait(_) => {
            reader.stop();
            restore_terminal();
            let result = external_open::run(&program, &open.path);
            let _ = crossterm::terminal::enable_raw_mode();
            let _ = crossterm::execute!(
                std::io::stdout(),
                crossterm::terminal::EnterAlternateScreen,
                crossterm::cursor::Hide
            );
            push_keyboard_flags();
            pointer::capture(mouse, &mut std::io::stdout());
            if focus {
                follow_focus(&mut std::io::stdout());
            }
            let _ = terminal.clear();
            match terminal_input::TerminalInput::start(tx.clone()) {
                Ok(started) => *reader = started,
                Err(e) => {
                    let _ = tx.send(AppEvent::Crash(format!("Could not read keys again: {e}")));
                }
            }
            result
        }
    };
    result.err().map(|e| e.to_string())
}
