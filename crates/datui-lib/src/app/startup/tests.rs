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
