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
    crate::cloud::user_agent::configure(&config.http.user_agent);
    let (input, config) = match input {
        RunInput::Cli(args) => {
            let mut config = config;
            apply_args(&mut config, &args);
            let opts = OpenOptions::from_args_and_config(&args, &config);
            // Only the command line reads standard input: a host such as Python has
            // its own, and it is not data.
            let piped = crate::loading::stdin::piped();
            let paths = crate::loading::stdin::paths_or_stdin(args.paths, piped);
            if let Some(refused) = crate::loading::stdin::refuse(&paths, piped) {
                return Err(color_eyre::eyre::eyre!(refused));
            }
            if let Some(tee) = &opts.tee {
                if !paths
                    .iter()
                    .any(|path| crate::loading::stdin::is_stdin(path))
                {
                    return Err(color_eyre::eyre::eyre!(
                        "--tee records standard input: pipe data in, as in: some_logger | datui --tee run1.csv -"
                    ));
                }
                if !opts.force && !crate::loading::stdin::is_stdin(tee) && tee.exists() {
                    return Err(color_eyre::eyre::eyre!(crate::loading::tee::refusal(tee)));
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
                    let paths = args
                        .paths
                        .into_iter()
                        .map(crate::loading::stdin::as_file)
                        .collect();
                    (RunInput::Paths(paths, opts), config)
                }
            }
        }
        RunInput::Paths(paths, opts) => {
            let paths = paths
                .into_iter()
                .map(crate::loading::stdin::as_file)
                .collect();
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
        notes.extend(crate::cloud::cloud_env::load(&config.cloud, &dir));
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
            if let Some(value) = crate::cloud::cloud_env::var(name) {
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
        .map(|path| crate::loading::stdin::named(path))
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
mod tests;
