//! The process around the app: the terminal session, the event loop's thread, the
//! signals that end it, and handing a file to the system's opener.

use crate::config::AppConfig;
use crate::terminal::{
    QuietTerminal, TakenTerminal, follow_focus, push_keyboard_flags, restore_terminal,
};
use crate::view::Views;
use crate::{
    App, AppEvent, RunInput, event_pump, external_open, glyphs, logging, pointer, startup,
    terminal_color, terminal_input,
};
use color_eyre::Result;
use crossterm::event::{KeyCode, KeyModifiers};
use polars::prelude::LazyFrame;

/// Restore the terminal, then turn how the loop ended into what `run_impl` returns.
/// The reader stops first, so nothing typed after the screen is handed back is read
/// here. The capture is taken after the screen is handed back, so a refused capture
/// still leaves the terminal usable.
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

/// The exit status the session's ending signal calls for, once one has ended it;
/// 0 until then. Set once.
static ENDED_BY_SIGNAL: std::sync::atomic::AtomicI32 = std::sync::atomic::AtomicI32::new(0);

/// The exit status for the binary when a signal ended the session [`run`] returned
/// from: `128 + n` for SIGTERM, SIGHUP or SIGINT, as if it had not been caught (the
/// statuses of `datui_cli::exit`), and on
/// Windows the status a console process closed by its window ends with.
pub fn ended_by_signal() -> Option<i32> {
    let status = ENDED_BY_SIGNAL.load(std::sync::atomic::Ordering::SeqCst);
    (status != 0).then_some(status)
}

/// A signal that ends the session arrived: quit as `q` does, so the screen is handed
/// back and an open's temp files are removed (#510). A second one, or a session still
/// running a few seconds after the first, ends the process at once, as the signal
/// would have: a stuck event loop cannot make datui unkillable. Called on the runtime.
fn end_session(status: i32, tx: &std::sync::mpsc::Sender<AppEvent>) {
    use std::sync::atomic::Ordering;
    // Longer than the exit sweep's grace, which is part of a normal quit, and short
    // of the five seconds Windows allows a console process it is closing.
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

/// End the session when its console window is closed, or the user logs off or the
/// machine shuts down. Tokio holds the control handler until the process exits, so
/// the quit runs before Windows ends it.
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

/// Run the TUI with either file paths or an existing LazyFrame. Single event loop
/// used by the CLI and the Python binding.
pub fn run(input: RunInput, config: Option<AppConfig>) -> Result<()> {
    run_impl(input, config, false).map(|_| ())
}

/// As `run`, but a normal quit hands back the active table's final view for the
/// caller to keep working with (the Python binding's `capture=True`). `None` when no
/// dataset was open at quit. See `App::capture_view` for what is refused and why.
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

    // First, so a missing file is named as the home directory has it.
    let input = startup::expand_home(input);
    use std::sync::{Mutex, Once, mpsc};

    // The saved views are read on a worker from here; the first thing that needs them
    // waits for the rest of the read, if any.
    let views = Views::read_in_background();

    // Install color_eyre at most once per process (e.g. first datui.view() in Python).
    // Subsequent run() calls skip install and reuse the result; no error-message detection.
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

    // Background work (e.g. the row-count `len()` over a huge or remote dataset) runs on
    // the runtime's blocking pool. Dropping the runtime normally *joins* those threads, so
    // quitting would hang until an in-flight count finished — minutes for a 474 GB hive
    // set. Shut the runtime down in the background instead: exit is immediate and the
    // abandoned read-only task dies with the process. This guard covers every return path
    // (Exit, Crash, `?`-propagated errors, channel disconnect).
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

    // `--tee -` passes the stream on to standard output, so the screen is drawn on the
    // terminal itself; standard output as it was is kept for the copy.
    let passed = match &input {
        RunInput::Cli(args) if args.tee.as_deref().is_some_and(crate::stdin::is_stdin) => {
            Some(crate::tee::pass_stdout_on().map_err(|e| color_eyre::eyre::eyre!(e))?)
        }
        _ => None,
    };
    let mut terminal = match ratatui::try_init() {
        Ok(terminal) => QuietTerminal(Some(terminal)),
        Err(e) => {
            // No screen to keep up, so nothing to wait behind: a configuration that
            // cannot be used, or a named file that is not there, is the more useful
            // thing to say, as each always came first.
            if config.is_none() {
                startup::load_config(&input)?;
            }
            // Without a screen nothing is opened, so the specs are not loaded to look.
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
    // Handed back on every way out of this function, after the reader below has let go.
    let mut screen = TakenTerminal { restored: false };
    // Anything written to stderr from here on would be drawn over the screen; it goes
    // to the log until this drops, on every way out of this function.
    let session = logging::TuiSession::begin(restore_terminal);
    push_keyboard_flags();
    // Asked before the settings are read, so the answer is usually in by the time they
    // are; under an explicit `theme.mode` it is read and dropped. The reader takes it
    // off the input stream, so nothing waits here.
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
    // Only for the datui binary: the handlers stay for the life of the process, and a
    // host such as Python keeps its own.
    #[cfg(any(unix, windows))]
    if matches!(input, RunInput::Cli(_)) {
        quit_on_signals(&rt_handle, &tx);
    }

    // The settings are files, so they are read on a worker while the keys are already
    // being read: a slow mount shows a screen saying so, and Ctrl+C or Ctrl+Q leave it.
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
                    terminal
                        .get()
                        .draw(|frame| startup::draw_waiting(frame, waiting_on.as_deref()))?;
                }
                // Typed before there was an app to take it: handled, in order, first.
                backlog.push(event);
            }
            Err(_) => {
                terminal
                    .get()
                    .draw(|frame| startup::draw_waiting(frame, waiting_on.as_deref()))?;
                let _ = std::io::stdout().flush();
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

    // The first frame is not held for the terminal's answer: one that is already in
    // is used, else this terminal's last one (see `App::settle_first_palette`).
    let background = (asked && config.theme.follow)
        .then(|| startup::take_answer(&rx, background, &mut backlog))
        .flatten();
    if config.theme.follow && terminal_color::supported() {
        follow_focus(&mut std::io::stdout());
    }

    // Choose the glyph alphabet before the first frame: on a terminal that is not
    // doing UTF-8, box-drawing characters render as replacement boxes and make the
    // UI harder to read rather than prettier.
    glyphs::init_with_overrides(config.display.unicode, &config.glyphs.overrides);

    // Taken once the settings say so; handed back with the screen.
    pointer::capture(config.display.mouse, &mut std::io::stdout());

    let mut app = App::new_with_views(tx.clone(), rt_handle, theme, config, views);
    app.settle_first_palette(background);
    if let Some(out) = passed {
        app.pass_stdout_to(out);
    }
    app.startup_view = opts.view.clone();
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
            // Whether each path is there, and whether a directory was named, is asked
            // after this frame, on a worker; the frame says what is being opened.
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
    // Declared before the pump, so it drops after it: the app's own files go with the
    // app, and this then removes what a worker was still writing.
    let _sweep = app.exit_sweep();
    let input_tx = tx.clone();
    let mut pump = EventPump::new(app, tx, rx);
    // The open goes out before the keys typed while the settings were read, so they
    // meet it as they would any open in flight: Ctrl+O puts it down, `q` quits. Sent
    // on the channel instead, it lost to a Ctrl+O offered ahead of the channel and
    // opened behind the home screen, or behind whatever was opened from there.
    pump.handle_first(backlog.into_iter().chain(open));
    let end = pump.run(|app| {
        if let Some(open) = app.take_external_open() {
            let mouse = app.mouse_enabled();
            let focus = app.follows_terminal() && terminal_color::supported();
            let note = open_externally(&open, &mut reader, &input_tx, mouse, focus, terminal.get());
            app.external_opened(&open, note);
        }
        // Between frames, so the question is never written into the middle of one.
        if app.take_background_query() && terminal_color::supported() {
            terminal_color::ask(&mut std::io::stdout());
        }
        terminal
            .get()
            .draw(|frame| frame.render_widget(app, frame.area()))?;
        let _ = std::io::stdout().flush();
        Ok(())
    })?;
    let result = conclude(end, &pump.app, capture, &mut reader, &mut screen);
    // stderr is the terminal again once the session is over.
    drop(session);
    // Not `eprintln!`, which panics when a hangup has taken the terminal away.
    for note in notes {
        let _ = writeln!(std::io::stderr(), "datui: {note}");
    }
    // Quit with the recording kept going: it goes on until its stream ends, with the
    // terminal handed back. A signal now ends the process as it always would.
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

/// Open a value the inspector wrote: a program that takes the terminal gets it
/// (the key reader stopped, the screen and raw mode handed back) until it
/// returns; an opener is only started. Says what went wrong, if anything.
fn open_externally(
    open: &external_open::ExternalOpen,
    reader: &mut terminal_input::TerminalInput,
    tx: &std::sync::mpsc::Sender<AppEvent>,
    mouse: bool,
    focus: bool,
    terminal: &mut ratatui::DefaultTerminal,
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
