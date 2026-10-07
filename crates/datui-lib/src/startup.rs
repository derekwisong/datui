//! What `run` reads before it can build the app, read behind the first frame.
//!
//! The configuration, its imports, `[cloud] env_files` and the log all live in files,
//! and a file can sit on a mount that does not answer. They are read on a worker
//! while the terminal is already taken, its reader running: a slow read shows a
//! screen that says so, and Ctrl+C or Ctrl+Q leave it. Read promptly, as they almost
//! always are, the app's own first frame is the first thing drawn.

use std::path::{Path, PathBuf};
use std::sync::mpsc::Receiver;
use std::time::Duration;

use color_eyre::Result;
use ratatui::Frame;
use ratatui::layout::Rect;
use ratatui::text::Line;
use ratatui::widgets::Paragraph;

use crate::config::ThemeMode;
use crate::{APP_NAME, AppConfig, AppEvent, Args, OpenOptions, RunInput, Theme, logging};

/// How long `run` waits for the settings before drawing a screen of its own. Under
/// it, nobody sees the difference and the app's first frame is the first one.
pub(crate) const GRACE: Duration = Duration::from_millis(25);

/// Everything read before the app can be built.
pub struct Settings {
    pub(crate) config: AppConfig,
    pub(crate) theme: Theme,
    pub(crate) input: RunInput,
    pub(crate) opts: OpenOptions,
    /// Things to say on stderr once the terminal is handed back: an env file that
    /// could not be read, a log that could not be opened.
    pub(crate) notes: Vec<String>,
}

/// The value for `POLARS_MAX_THREADS` that `performance.threads` asks for: none for 0
/// (every core), or when the environment already names one, which wins.
pub(crate) fn polars_threads(threads: usize, env: Option<&std::ffi::OsStr>) -> Option<String> {
    (threads > 0 && env.is_none()).then(|| threads.to_string())
}

/// Read the settings: the configuration (unless one was given), the command line over
/// it, `[cloud] env_files`, the log.
pub(crate) fn read(input: RunInput, config: Option<AppConfig>) -> Result<Settings> {
    // `--log-level`, then `-c log.level`, then `DATUI_LOG`; the files' `log.level` is
    // read below, under all three.
    let log_level = match &input {
        RunInput::Cli(args) | RunInput::Host(args, _) => args.log_level.clone().or_else(|| {
            args.config
                .iter()
                .rev()
                .find(|o| o.key == "log.level")
                .and_then(|o| o.value.as_str().map(str::to_string))
        }),
        _ => None,
    }
    .or_else(|| std::env::var("DATUI_LOG").ok());
    let config = match config {
        Some(config) => config,
        None => load_config(&input)?,
    };
    crate::user_agent::configure(&config.http.user_agent);
    let (input, config) = match input {
        RunInput::Cli(args) => {
            let mut config = config;
            apply_args(&mut config, &args);
            let opts = OpenOptions::from_args_and_config(&args, &config);
            // Only the command line reads standard input: a host such as Python has
            // its own, and it is not data.
            let piped = crate::stdin::piped();
            let paths = crate::stdin::paths_or_stdin(args.paths, piped);
            if let Some(refused) = crate::stdin::refuse(&paths, piped) {
                return Err(color_eyre::eyre::eyre!(refused));
            }
            if let Some(tee) = &opts.tee {
                if !paths.iter().any(|path| crate::stdin::is_stdin(path)) {
                    return Err(color_eyre::eyre::eyre!(
                        "--tee records standard input: pipe data in, as in: some_logger | datui --tee run1.csv -"
                    ));
                }
                if !opts.force && !crate::stdin::is_stdin(tee) && tee.exists() {
                    return Err(color_eyre::eyre::eyre!(crate::tee::refusal(tee)));
                }
            }
            if opts.follow && paths.is_empty() {
                return Err(color_eyre::eyre::eyre!(
                    "--follow needs a file to follow, or data piped in: some_logger | datui -f -"
                ));
            }
            (RunInput::Paths(paths, opts), config)
        }
        RunInput::Host(args, frame) => {
            let mut config = config;
            apply_args(&mut config, &args);
            let opts = OpenOptions::from_args_and_config(&args, &config);
            match frame {
                Some(lf) => (RunInput::LazyFrame(lf, opts), config),
                None => {
                    let paths = args.paths.into_iter().map(crate::stdin::as_file).collect();
                    (RunInput::Paths(paths, opts), config)
                }
            }
        }
        RunInput::Paths(paths, opts) => {
            let paths = paths.into_iter().map(crate::stdin::as_file).collect();
            (RunInput::Paths(paths, opts), config)
        }
        input => (input, config),
    };
    let opts = match &input {
        RunInput::Paths(_, o) | RunInput::LazyFrame(_, o) => o.clone(),
        RunInput::Cli(_) | RunInput::Host(..) => unreachable!("resolved above"),
    };
    // The home screen has no `OpenOptions` of its own, so the CLI and environment S3
    // overrides are folded into the config here, once, for discovery, listing and
    // opens started from a listed bucket.
    let mut config = config;
    let mut notes = Vec::new();
    // Variables from `[cloud] env_files` first, so everything below sees them.
    if let Ok(dir) = std::env::current_dir() {
        notes.extend(crate::cloud_env::load(&config.cloud, &dir));
    }
    config.cloud = opts.effective_cloud(&config.cloud);
    // Said on stderr while it was the log: said again once the terminal is back.
    notes.extend(config.theme.warnings());

    let cache_dir = crate::cache::CacheManager::new(APP_NAME).ok();
    notes.extend(logging::init(&logging::LogSettings::resolve(
        config.log.file.as_deref(),
        log_level.as_deref().or(config.log.level.as_deref()),
        cache_dir.as_ref().map(|c| c.cache_dir()),
    )));
    for secret in [
        &config.cloud.s3_access_key_id,
        &config.cloud.s3_secret_access_key,
    ]
    .into_iter()
    .flatten()
    {
        logging::keep_out_of_log(secret);
    }
    // A connection's keys come from variables it names, which need not look secret.
    for connection in &config.cloud.connections {
        for name in [
            &connection.secret_access_key_env,
            &connection.session_token_env,
            &connection.account_key_env,
        ]
        .into_iter()
        .flatten()
        {
            if let Some(value) = crate::cloud_env::var(name) {
                logging::keep_out_of_log(&value);
            }
        }
    }

    let theme = Theme::from_config(&config.theme)
        .or_else(|e| Theme::from_config(&AppConfig::default().theme).map_err(|_| e))?;
    Ok(Settings {
        config,
        theme,
        input,
        opts,
        notes,
    })
}

/// The configuration file, read. From the command line its error says how to get
/// past it; a library caller gets the error as it is.
pub(crate) fn load_config(input: &RunInput) -> Result<AppConfig> {
    let overrides = match input {
        RunInput::Cli(args) | RunInput::Host(args, _) => args.config.as_slice(),
        _ => &[],
    };
    AppConfig::load_with(APP_NAME, overrides).map_err(|e| match input {
        // In full: a TOML parse error's later lines show the offending line and why.
        RunInput::Cli(_) => color_eyre::eyre::eyre!(
            "{e}\nFix the configuration and try again, or remove/rename the config file to \
             use defaults."
        ),
        _ => e,
    })
}

/// `input` with a leading `~` expanded in the paths its command line names: the
/// datasets, `--format FILE`, `--dict` and `--temp-dir`. `--log-file` expands with
/// `[log] file`.
pub(crate) fn expand_home(input: RunInput) -> RunInput {
    match input {
        RunInput::Cli(mut args) => {
            let spec = match args.format.as_mut() {
                Some(crate::cli::FormatChoice::File(path)) => Some(path),
                _ => None,
            };
            for path in args
                .paths
                .iter_mut()
                .chain(spec)
                .chain(args.dict.iter_mut())
                .chain(args.temp_dir.as_mut())
            {
                *path = crate::config::expand_home(path);
            }
            RunInput::Cli(args)
        }
        input => input,
    }
}

/// The command line's flags that set config keys, over the configuration (which
/// `-c` is already in). The open's own flags go to `OpenOptions`.
fn apply_args(config: &mut AppConfig, args: &Args) {
    if let Some(nf) = args.number_format.as_deref() {
        config.display.number_format = config.display.number_format.with_grouping_override(nf);
    }
    if let Some(row_numbers) = args.row_numbers {
        config.display.row_numbers = row_numbers.into();
    }
    if let Some(mouse) = args.mouse {
        config.display.mouse = mouse;
    }
    if let Some(rows) = args.sample_rows {
        config.analysis.sample_rows = rows;
    }
    if let Some(path) = &args.log_file {
        config.log.file = Some(path.to_string_lossy().into_owned());
    }
}

/// The paths `input` names, if any.
pub(crate) fn named_paths(input: &RunInput) -> &[PathBuf] {
    match input {
        RunInput::Cli(args) | RunInput::Host(args, None) => &args.paths,
        RunInput::Host(_, Some(_)) => &[],
        RunInput::Paths(paths, _) => paths,
        RunInput::LazyFrame(..) => &[],
    }
}

/// The path to name on the screen drawn while the settings are read.
pub(crate) fn named(input: &RunInput) -> Option<PathBuf> {
    named_paths(input)
        .first()
        .map(|path| crate::stdin::named(path))
}

/// The screen while the settings are slow to read. The theme and the glyph set are
/// among them, so it uses neither: the terminal's own colors and plain ASCII.
pub(crate) fn draw_waiting(frame: &mut Frame, path: Option<&Path>) {
    let area = frame.area();
    let mut lines = vec![Line::from("Reading settings...")];
    if let Some(path) = path {
        lines.push(Line::from(""));
        lines.push(Line::from(crate::home::display_path(path)));
    }
    let height = (lines.len() as u16).min(area.height);
    let top = area.y + area.height.saturating_sub(height) / 2;
    frame.render_widget(
        Paragraph::new(lines).centered(),
        Rect {
            y: top,
            height,
            ..area
        },
    );
}

/// The terminal's answer about its background, if it is in by the first frame: the
/// one taken while the settings were read, else one already on `rx`. Never waits;
/// everything else found on `rx` goes on `backlog`, in order.
pub(crate) fn take_answer(
    rx: &Receiver<AppEvent>,
    answered: Option<ThemeMode>,
    backlog: &mut Vec<AppEvent>,
) -> Option<ThemeMode> {
    let mut answered = answered;
    while let Ok(event) = rx.try_recv() {
        match event {
            AppEvent::TerminalBackground(mode) => answered = Some(mode),
            event => backlog.push(event),
        }
    }
    answered
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    /// A terminal that never answers holds nothing up: with the sender alive and
    /// nothing sent, the first frame's palette is settled at once.
    #[test]
    fn a_silent_terminal_does_not_hold_the_first_frame() {
        let (tx, rx) = std::sync::mpsc::channel::<AppEvent>();
        let mut backlog = Vec::new();
        let started = std::time::Instant::now();
        assert_eq!(take_answer(&rx, None, &mut backlog), None);
        assert!(started.elapsed() < Duration::from_millis(20));
        assert!(backlog.is_empty());
        drop(tx);
    }

    /// An answer already in is taken; keys around it keep their order.
    #[test]
    fn an_answer_already_in_is_taken() {
        use crossterm::event::{Event, KeyCode, KeyEvent, KeyModifiers};
        let (tx, rx) = std::sync::mpsc::channel::<AppEvent>();
        let key = |c| {
            AppEvent::Terminal(Event::Key(KeyEvent::new(
                KeyCode::Char(c),
                KeyModifiers::NONE,
            )))
        };
        tx.send(key('j')).unwrap();
        tx.send(AppEvent::TerminalBackground(ThemeMode::Light))
            .unwrap();
        tx.send(key('k')).unwrap();
        let mut backlog = Vec::new();
        assert_eq!(
            take_answer(&rx, Some(ThemeMode::Dark), &mut backlog),
            Some(ThemeMode::Light)
        );
        let typed: Vec<_> = backlog
            .iter()
            .map(|event| match event {
                AppEvent::Terminal(Event::Key(key)) => key.code,
                _ => KeyCode::Null,
            })
            .collect();
        assert_eq!(typed, [KeyCode::Char('j'), KeyCode::Char('k')]);
        assert_eq!(
            take_answer(&rx, Some(ThemeMode::Dark), &mut Vec::new()),
            Some(ThemeMode::Dark)
        );
    }

    /// `--mouse=false` leaves the mouse to the terminal over a config that takes it,
    /// `--mouse` takes it over one that does not, and no flag keeps the config's.
    #[test]
    fn the_mouse_flag_overrides_the_config() {
        let after = |flags: &[&str], configured: bool| {
            let args = Args::try_parse_from(std::iter::once("datui").chain(flags.iter().copied()))
                .expect("parses");
            let mut config = AppConfig::default();
            config.display.mouse = configured;
            apply_args(&mut config, &args);
            config.display.mouse
        };
        assert!(!after(&["--mouse=false"], true));
        assert!(after(&["--mouse"], false));
        assert!(after(&["--mouse=true"], false));
        assert!(after(&[], true));
        assert!(!after(&[], false));
    }

    /// A key is read from the file, `-c` beats the file, and a flag beats `-c`.
    #[test]
    fn a_flag_beats_dash_c_which_beats_the_file() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("config.toml");
        std::fs::write(
            &file,
            "[analysis]\nsample_rows = 10\n[display]\nrow_numbers_start = 5\n",
        )
        .unwrap();
        let effective = |flags: &[&str]| {
            let args = Args::try_parse_from(std::iter::once("datui").chain(flags.iter().copied()))
                .expect("parses");
            let mut config = AppConfig::load_from_file_with(&file, &args.config).unwrap();
            apply_args(&mut config, &args);
            (
                config.analysis.sample_rows,
                config.display.row_numbers_start,
            )
        };
        assert_eq!(effective(&[]), (10, 5));
        assert_eq!(effective(&["-c", "analysis.sample_rows=20"]), (20, 5));
        assert_eq!(
            effective(&["-c", "analysis.sample_rows=20", "--sample-rows", "30"]),
            (30, 5)
        );
        // The last `-c` of a key wins, and keys it does not name keep the file's.
        assert_eq!(
            effective(&[
                "-c",
                "display.row_numbers_start=0",
                "-c",
                "display.row_numbers_start=2"
            ]),
            (10, 2)
        );
    }

    /// A `-c` value of the right shape for its key but not for datui says it came
    /// from `-c`.
    #[test]
    fn a_dash_c_the_config_cannot_read_is_named() {
        let args =
            Args::try_parse_from(["datui", "-c", "display.number_format=[1, 2]"]).expect("parses");
        let dir = tempfile::tempdir().unwrap();
        let error = AppConfig::load_from_file_with(&dir.path().join("none.toml"), &args.config)
            .unwrap_err()
            .to_string();
        assert!(error.starts_with("-c:"), "{error}");
    }

    #[test]
    fn the_command_line_s_paths_expand_a_leading_tilde() {
        let home = dirs::home_dir().expect("a home directory");
        let args = Args::try_parse_from([
            "datui",
            "~/a.csv",
            "b.csv",
            "--temp-dir",
            "~/scratch",
            "--format",
            "~/l2feed.toml",
            "--dict",
            "~/car.dbc",
        ])
        .unwrap();
        let RunInput::Cli(args) = expand_home(RunInput::Cli(Box::new(args))) else {
            panic!("still the command line");
        };
        assert_eq!(args.paths, [home.join("a.csv"), PathBuf::from("b.csv")]);
        assert_eq!(args.temp_dir, Some(home.join("scratch")));
        assert_eq!(
            args.format,
            Some(crate::cli::FormatChoice::File(home.join("l2feed.toml")))
        );
        assert_eq!(args.dict, [home.join("car.dbc")]);
    }

    /// `performance.threads` becomes POLARS_MAX_THREADS, unless the environment
    /// already says, and 0 leaves Polars every core.
    #[test]
    fn performance_threads_caps_polars_unless_the_environment_says() {
        assert_eq!(polars_threads(4, None).as_deref(), Some("4"));
        assert_eq!(polars_threads(0, None), None);
        let asked = std::ffi::OsString::from("2");
        assert_eq!(polars_threads(4, Some(&asked)), None);
    }
}
