//! The log file, and keeping stray stderr off the screen: while the TUI runs, fd 2
//! points at the log, Polars warnings go through `log`, and non-fatal errors (cache,
//! history) land here.

use std::cell::Cell;
use std::collections::{HashSet, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex};

pub use log::LevelFilter;
use log::{Log, Metadata, Record};

/// The log's name in the cache directory.
pub const LOG_FILE_NAME: &str = "datui.log";

/// Past this the log is moved aside to `<name>.1`, replacing the previous one.
const MAX_BYTES: u64 = 1024 * 1024;

/// The level when `DATUI_LOG` is unset.
const DEFAULT_LEVEL: LevelFilter = LevelFilter::Warn;

/// Where the log goes and how much of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogSettings {
    /// `None` when logging is off; stray stderr is then discarded.
    pub path: Option<PathBuf>,
    pub level: LevelFilter,
    /// `DATUI_LOG` held something that is not a level; said in the log once it opens.
    pub unknown_level: Option<String>,
}

impl LogSettings {
    /// Resolve the file from `[log] file` (which `--log-file` overrides) or the
    /// cache directory, and the level from `DATUI_LOG`.
    pub fn resolve(
        configured: Option<&str>,
        level: Option<&str>,
        cache_dir: Option<&Path>,
    ) -> Self {
        let (level, unknown_level) = match level.map(str::trim).filter(|l| !l.is_empty()) {
            None => (DEFAULT_LEVEL, None),
            Some(text) => match text.parse::<LevelFilter>() {
                Ok(level) => (level, None),
                Err(_) => (DEFAULT_LEVEL, Some(text.to_string())),
            },
        };
        let path = match configured.map(str::trim).filter(|p| !p.is_empty()) {
            Some(path) => Some(crate::config::expand_config_path(path)),
            None => cache_dir.map(|dir| dir.join(LOG_FILE_NAME)),
        };
        Self {
            path: path.filter(|_| level != LevelFilter::Off),
            level,
            unknown_level,
        }
    }
}

/// The rotated copy of `path`: `datui.log` → `datui.log.1`.
pub fn rotated_path(path: &Path) -> PathBuf {
    let mut name = path.as_os_str().to_owned();
    name.push(".1");
    PathBuf::from(name)
}

/// A size-capped log file with one rotation.
pub struct FileLog {
    path: PathBuf,
    file: File,
    cap: u64,
}

impl FileLog {
    /// Open (creating) `path` for appending, rotating it first when already over `cap`.
    pub fn open(path: &Path, cap: u64) -> std::io::Result<Self> {
        if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
            std::fs::create_dir_all(dir)?;
        }
        let file = Self::append(path)?;
        let mut log = Self {
            path: path.to_path_buf(),
            file,
            cap,
        };
        log.rotate_if_full()?;
        Ok(log)
    }

    fn append(path: &Path) -> std::io::Result<File> {
        // Append mode, so two sessions sharing the file interleave lines rather than
        // overwrite each other.
        OpenOptions::new().create(true).append(true).open(path)
    }

    /// Write one line, then rotate if that filled the file. Returns whether it rotated.
    pub fn write_line(&mut self, line: &str) -> std::io::Result<bool> {
        self.file.write_all(line.as_bytes())?;
        if !line.ends_with('\n') {
            self.file.write_all(b"\n")?;
        }
        self.rotate_if_full()
    }

    /// Measured on the file, since another session may share it. The rename happens under
    /// a lock and only when the file at the path is full (another session may already have
    /// rotated, leaving this one writing `<name>.1`); the path is reopened either way.
    fn rotate_if_full(&mut self) -> std::io::Result<bool> {
        if self.file.metadata()?.len() <= self.cap {
            return Ok(false);
        }
        let mut lock_name = self.path.as_os_str().to_owned();
        lock_name.push(".lock");
        let Some(_lock) =
            crate::cache::lock_file(Path::new(&lock_name), std::time::Duration::from_millis(200))?
        else {
            // Busy: the next line tries again.
            return Ok(false);
        };
        let full = std::fs::metadata(&self.path).is_ok_and(|m| m.len() > self.cap);
        if full {
            std::fs::rename(&self.path, rotated_path(&self.path))?;
        }
        self.file = Self::append(&self.path)?;
        Ok(full)
    }
}

struct State {
    level: LevelFilter,
    file: Option<FileLog>,
    /// Whether this session has written its header line yet.
    started: bool,
    /// Values never written to the log as they are: keys, tokens, passwords.
    secrets: Vec<String>,
}

static STATE: Mutex<State> = Mutex::new(State {
    level: LevelFilter::Off,
    file: None,
    started: false,
    secrets: Vec::new(),
});

fn state() -> std::sync::MutexGuard<'static, State> {
    STATE.lock().unwrap_or_else(|e| e.into_inner())
}

struct Logger;

static LOGGER: Logger = Logger;

impl Log for Logger {
    fn enabled(&self, metadata: &Metadata) -> bool {
        metadata.level() <= state().level
    }

    fn log(&self, record: &Record) {
        if self.enabled(record.metadata()) {
            write_record(record.level().as_str(), record.target(), record.args());
        }
    }

    fn flush(&self) {
        if let Some(file) = state().file.as_mut() {
            let _ = file.file.flush();
        }
    }
}

thread_local! {
    /// Set while this thread writes a record. A panic in the middle of one runs the
    /// hook, which logs; without this it would wait on the lock the thread holds.
    static WRITING: Cell<bool> = const { Cell::new(false) };
}

/// Clears [`WRITING`] however the write ends.
struct Writing;

impl Writing {
    fn enter() -> Option<Self> {
        (!WRITING.with(|w| w.replace(true))).then_some(Self)
    }
}

impl Drop for Writing {
    fn drop(&mut self) {
        WRITING.with(|w| w.set(false));
    }
}

/// Write one record whatever the level, when the log is open.
fn write_record(level: &str, target: &str, message: impl std::fmt::Display) {
    let Some(_writing) = Writing::enter() else {
        return;
    };
    // Formatted before the lock, in case formatting logs something itself.
    let message = message.to_string().replace('\n', "\n    ");
    let now = chrono::Local::now()
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string();
    let mut state = state();
    if state.file.is_none() {
        return;
    }
    let mut text = String::new();
    if !state.started {
        state.started = true;
        text.push_str(&format!(
            "{now} ----- datui {} (pid {})\n",
            env!("CARGO_PKG_VERSION"),
            std::process::id()
        ));
    }
    text.push_str(&format!(
        "{now} {level:<5} {target}: {}",
        redact(&message, &state.secrets)
    ));
    if let Some(file) = state.file.as_mut() {
        // A log that cannot be written has nowhere to say so; stderr is the screen.
        let _ = file.write_line(&text);
    }
}

/// Open the log and install the logger and Polars warning hook; repeatable (the Python
/// binding runs the TUI per `view`), latest settings winning. Returns the message for
/// the user if the log cannot open, unprinted since stderr may be that log.
pub fn init(settings: &LogSettings) -> Option<String> {
    static INSTALLED: std::sync::Once = std::sync::Once::new();
    INSTALLED.call_once(|| {
        let _ = log::set_logger(&LOGGER);
        polars_error::set_warning_function(polars_warning);
    });
    let mut note = None;
    let file = settings
        .path
        .as_deref()
        .and_then(|path| match FileLog::open(path, MAX_BYTES) {
            Ok(file) => Some(file),
            Err(e) => {
                note = Some(format!("cannot write the log {}: {e}", path.display()));
                None
            }
        });
    let level = if file.is_some() {
        settings.level
    } else {
        LevelFilter::Off
    };
    {
        let mut state = state();
        state.file = file;
        state.level = level;
        state.started = false;
    }
    log::set_max_level(level);
    keep_out_of_log_from_env();
    if let Some(text) = &settings.unknown_level {
        log::warn!(target: "datui", "DATUI_LOG={text} is not a level; using warn");
    }
    note
}

/// The file the log is writing to, if it is open.
pub fn current_path() -> Option<PathBuf> {
    state().file.as_ref().map(|f| f.path.clone())
}

/// Never write `secret` to the log as it is.
pub fn keep_out_of_log(secret: &str) {
    // Short values would mask ordinary words.
    if secret.len() < 8 {
        return;
    }
    let mut state = state();
    if !state.secrets.iter().any(|s| s == secret) {
        state.secrets.push(secret.to_string());
    }
}

/// Values of variables whose names say they hold a credential, the `[cloud] env_files`
/// ones included.
fn keep_out_of_log_from_env() {
    for (name, value) in crate::cloud_env::vars() {
        if holds_a_credential(&name, &value) {
            keep_out_of_log(&value);
        }
    }
}

/// Whether an environment variable's value is a credential to mask. The name decides,
/// except that a file or directory is not the secret itself: masking the path in
/// `AWS_WEB_IDENTITY_TOKEN_FILE` would hide every mention of it.
fn holds_a_credential(name: &str, value: &str) -> bool {
    let name = name.to_ascii_uppercase();
    let named = [
        "SECRET",
        "TOKEN",
        "PASSWORD",
        "PASSWD",
        "ACCESS_KEY",
        "ACCOUNT_KEY",
        "CONNECTION_STRING",
    ]
    .iter()
    .any(|word| name.contains(word))
        || name.ends_with("_KEY");
    let names_a_place = [
        "_FILE",
        "_PATH",
        "_DIR",
        "_DIRECTORY",
        "_HOME",
        "_URL",
        "_URI",
    ]
    .iter()
    .any(|suffix| name.ends_with(suffix));
    let path = Path::new(value);
    named && !names_a_place && !(path.is_absolute() && path.exists())
}

/// Log a failure not worth stopping for (cache, history) instead of dropping it.
pub trait LogFailure {
    fn or_log(self, what: &str);
}

impl<T, E: std::fmt::Display> LogFailure for Result<T, E> {
    fn or_log(self, what: &str) {
        if let Err(e) = self {
            log::warn!(target: "datui", "{what}: {e:#}");
        }
    }
}

/// Mask credentials in a log line: known secret values, the user and password in a
/// URL, signed-URL and SAS query parameters, and authorization headers.
pub fn redact(text: &str, secrets: &[String]) -> String {
    static PATTERNS: LazyLock<Vec<regex::Regex>> = LazyLock::new(|| {
        [
            // scheme://user:password@host
            r"(?i)(\b[a-z][a-z0-9+.-]*://)[^/\s:@]+:[^/\s@]+@",
            // ?X-Amz-Signature=..., &sig=..., &token=..., or a SAS token on its own
            r"(?i)((?:^|[?&;])(?:x-amz-signature|x-amz-credential|x-amz-security-token|x-goog-signature|x-goog-credential|signature|sig|token|access_token|api_key|apikey|key|password|secret)=)[^&\s]+",
            // Authorization: Bearer ..., "authorization": "..."
            r#"(?i)(authorization"?\s*[:=]\s*"?)[^"\r\n]+"#,
            // Case-sensitive Basic, or "basic statistics" would lose its noun.
            r"(\b(?i:bearer)\s+|\bBasic\s+)[A-Za-z0-9._~+/=-]{8,}",
            // secret_access_key = ..., "session_token": "...", AccountKey=...
            r#"(?i)((?:secret[_-]?access[_-]?key|session[_-]?token|account[_-]?key|client[_-]?secret|sas[_-]?token|password)"?\s*[:=]\s*"?)[^\s",;}]+"#,
        ]
        .iter()
        .filter_map(|p| regex::Regex::new(p).ok())
        .collect()
    });
    let mut out = text.to_string();
    for secret in secrets {
        if out.contains(secret.as_str()) {
            out = out.replace(secret.as_str(), "***");
        }
    }
    for pattern in PATTERNS.iter() {
        out = pattern.replace_all(&out, "${1}***").into_owned();
    }
    out
}

/// Polars warnings already seen this session, and user warnings not yet flashed.
struct PolarsWarnings {
    seen: HashSet<String>,
    unshown: VecDeque<String>,
}

static POLARS: Mutex<Option<PolarsWarnings>> = Mutex::new(None);

/// A warning repeats for every chunk and every collect; past this many distinct
/// ones, the rest are dropped rather than let them fill the log.
const MAX_POLARS_WARNINGS: usize = 256;

/// Where `polars_warn!` goes: each distinct warning logged once per session; user
/// warnings (not deprecations) are also queued for the footer.
fn polars_warning(message: &str, kind: polars_error::PolarsWarning) {
    use polars_error::PolarsWarning as W;
    let text = message.split_whitespace().collect::<Vec<_>>().join(" ");
    {
        let mut polars = POLARS.lock().unwrap_or_else(|e| e.into_inner());
        let polars = polars.get_or_insert_with(|| PolarsWarnings {
            seen: HashSet::new(),
            unshown: VecDeque::new(),
        });
        if polars.seen.len() >= MAX_POLARS_WARNINGS || !polars.seen.insert(text.clone()) {
            return;
        }
        if matches!(kind, W::UserWarning | W::CategoricalRemappingWarning) {
            polars.unshown.push_back(text.clone());
            tell_the_loop();
        }
    }
    log::warn!(target: "polars", "{kind:?}: {text}");
}

/// What wakes the run loop when there is news it has to come and look for: a warning
/// queued for the footer, a background panic nothing reported. The loop only
/// wakes for events and deadlines, so without this either would wait for a key.
static NEWS: Mutex<Option<Box<dyn Fn() + Send + Sync>>> = Mutex::new(None);

fn tell_the_loop() {
    if let Some(wake) = NEWS.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
        wake();
    }
}

/// The next Polars user warning not yet shown, for the footer.
pub fn next_polars_warning() -> Option<String> {
    POLARS
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_mut()
        .and_then(|p| p.unshown.pop_front())
}

/// Forget which Polars warnings were seen, so a new session logs them again.
fn reset_polars_warnings() {
    *POLARS.lock().unwrap_or_else(|e| e.into_inner()) = None;
}

/// Set while a TUI session owns the terminal.
static TUI_ACTIVE: AtomicBool = AtomicBool::new(false);

/// The last panic on a background thread, to print if it takes the TUI thread down.
static BACKGROUND_PANIC: Mutex<Option<String>> = Mutex::new(None);

/// Background panics nothing has told the user about yet: those on threads that do
/// not report their own (see [`catch_panic`]).
static UNREPORTED_PANICS: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// Set while [`catch_panic`] runs work on this thread, which then reports its own.
    static REPORTS_ITS_PANICS: Cell<bool> = const { Cell::new(false) };
}

/// Run `work`, turning a panic into a message for the user. The hook has already
/// logged the panic with its backtrace; the message says where. A panic on a Polars
/// thread that this work was waiting on is resumed here, so it is reported here too.
pub fn catch_panic<T>(work: impl FnOnce() -> T) -> Result<T, String> {
    let reported = REPORTS_ITS_PANICS.with(|r| r.replace(true));
    let unreported = UNREPORTED_PANICS.load(Ordering::SeqCst);
    let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(work));
    REPORTS_ITS_PANICS.with(|r| r.set(reported));
    caught.map_err(|payload| {
        // Those counted meanwhile were the Polars threads this work waited on.
        UNREPORTED_PANICS.fetch_min(unreported, Ordering::SeqCst);
        let what = payload
            .downcast_ref::<&str>()
            .map(|s| s.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "panic".to_string());
        match current_path() {
            Some(path) => format!("Internal error: {what}\n\nDetails: {}", path.display()),
            None => format!("Internal error: {what}"),
        }
    })
}

/// What to flash when a background thread panicked with nothing to say so: a raw
/// thread's result simply never arrives. `None` when none did since the last call.
pub fn take_unreported_panic() -> Option<String> {
    if UNREPORTED_PANICS.swap(0, Ordering::SeqCst) == 0 {
        return None;
    }
    Some(match current_path() {
        Some(path) => format!(
            "A background task failed; see {}",
            path.file_name().unwrap_or_default().to_string_lossy()
        ),
        None => "A background task failed".to_string(),
    })
}

/// The span during which the TUI owns the terminal: stderr goes to the log (Unix),
/// and a panic on a background thread is logged instead of printed over the screen.
/// Dropping it, on every exit path, hands stderr back.
pub struct TuiSession {
    restore_terminal: fn(),
}

impl TuiSession {
    /// Begin right after the terminal is taken. `restore_terminal` hands the screen
    /// back if the TUI thread unwinds from a panic the hook never saw.
    pub fn begin(restore_terminal: fn()) -> Self {
        reset_polars_warnings();
        *BACKGROUND_PANIC.lock().unwrap_or_else(|e| e.into_inner()) = None;
        UNREPORTED_PANICS.store(0, Ordering::SeqCst);
        #[cfg(unix)]
        {
            // Through the log's pipe even with no log open yet: the settings that open
            // it are read after the session begins. Lines with no log are dropped.
            stderr::redirect(true);
        }
        TUI_ACTIVE.store(true, Ordering::SeqCst);
        install_panic_hook();
        Self { restore_terminal }
    }

    /// Call `wake` whenever a background panic or a Polars warning is waiting for
    /// [`take_unreported_panic`] or [`next_polars_warning`], for the session's length.
    pub fn wake_with(&self, wake: impl Fn() + Send + Sync + 'static) {
        *NEWS.lock().unwrap_or_else(|e| e.into_inner()) = Some(Box::new(wake));
    }
}

impl Drop for TuiSession {
    fn drop(&mut self) {
        NEWS.lock().unwrap_or_else(|e| e.into_inner()).take();
        // Already inactive when the hook saw this thread panic: it restored stderr, and
        // the hooks below it the terminal. Restoring again would pop the shell's
        // keyboard flags rather than ours.
        let hook_handled_it = !TUI_ACTIVE.swap(false, Ordering::SeqCst);
        #[cfg(unix)]
        stderr::restore();
        // A panic resumed from another thread (a Polars worker's, say) unwinds this one
        // without running the hook, so the terminal and the message are ours to handle.
        if std::thread::panicking() && !hook_handled_it {
            (self.restore_terminal)();
            if let Some(message) = BACKGROUND_PANIC
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take()
            {
                eprintln!("{message}");
            }
        }
    }
}

/// Wraps whatever hook is installed (color-eyre's, under ratatui's). Installed per
/// session, above the hook `ratatui::try_init` adds each time.
fn install_panic_hook() {
    let tui_thread = std::thread::current().id();
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        if !TUI_ACTIVE.load(Ordering::SeqCst) {
            previous(info);
            return;
        }
        if std::thread::current().id() != tui_thread {
            // A worker's panic is caught (tokio's blocking pool, or a catch_unwind)
            // and the app carries on, so it must not tear down the screen.
            let message = format!(
                "a background thread panicked at {}: {}\n{}",
                info.location()
                    .map(|l| l.to_string())
                    .unwrap_or_else(|| "?".into()),
                panic_payload(info),
                std::backtrace::Backtrace::force_capture()
            );
            log::error!(target: "datui::panic", "{message}");
            *BACKGROUND_PANIC.lock().unwrap_or_else(|e| e.into_inner()) = Some(message);
            if !REPORTS_ITS_PANICS.with(Cell::get) {
                UNREPORTED_PANICS.fetch_add(1, Ordering::SeqCst);
                tell_the_loop();
            }
            return;
        }
        // The TUI thread: hand stderr back so the report reaches the terminal. Inactive
        // from here, so an earlier session's hook further down the chain (the Python
        // binding, run from another thread) passes the report on instead of keeping it.
        TUI_ACTIVE.store(false, Ordering::SeqCst);
        // The hooks below hand back the screen but not the mouse, whose reporting
        // outlives the alternate screen: the shell would read every click as text.
        // Unlike popping the keyboard flags, this is safe to repeat.
        let _ = crossterm::execute!(std::io::stdout(), crossterm::event::DisableMouseCapture);
        BACKGROUND_PANIC
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        #[cfg(unix)]
        stderr::restore();
        previous(info);
    }));
}

fn panic_payload(info: &std::panic::PanicHookInfo<'_>) -> String {
    let payload = info.payload();
    payload
        .downcast_ref::<&str>()
        .map(|s| s.to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "panic".to_string())
}

/// One line someone wrote to stderr while the TUI was up.
#[cfg(unix)]
fn write_stray_line(line: &[u8]) {
    let text = String::from_utf8_lossy(line);
    let text = text.trim_end_matches(['\n', '\r']);
    if !text.trim().is_empty() {
        write_record("WARN", "stderr", text);
    }
}

/// Pointing fd 2 away and back (not on Windows, where the Polars hook suffices). With the
/// log open, fd 2 is a pipe a thread copies into the log line by line, counted against
/// the cap.
#[cfg(unix)]
mod stderr {
    use std::io::BufRead;
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
    use std::sync::Mutex;
    use std::sync::mpsc::{Receiver, channel};
    use std::time::Duration;

    struct Redirect {
        /// A duplicate of the real stderr.
        terminal: OwnedFd,
        /// Answers once the reader has read the pipe to its end; `None` for /dev/null.
        drained: Option<Receiver<()>>,
    }

    static REDIRECT: Mutex<Option<Redirect>> = Mutex::new(None);

    fn current() -> std::sync::MutexGuard<'static, Option<Redirect>> {
        REDIRECT.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Point fd 2 at the log, or at `/dev/null` with logging off.
    pub(super) fn redirect(logging: bool) {
        let mut current = current();
        if current.is_some() {
            return;
        }
        // SAFETY: fcntl(F_DUPFD_CLOEXEC) reads no memory; on failure it returns -1.
        // Close-on-exec, so a child process never inherits the terminal through it.
        let copy = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_DUPFD_CLOEXEC, 0) };
        if copy < 0 {
            return;
        }
        // SAFETY: `copy` is a new descriptor that nothing else owns.
        let terminal = unsafe { OwnedFd::from_raw_fd(copy) };
        let target = logging.then(into_the_log).flatten().or_else(|| {
            std::fs::OpenOptions::new()
                .write(true)
                .open("/dev/null")
                .ok()
                .map(|null| (OwnedFd::from(null), None))
        });
        // `target` closes at the end of this function, which leaves fd 2 as the pipe's
        // only writer: once fd 2 points back at the terminal, the reader sees the end.
        if let Some((target, drained)) = target
            && point(target.as_raw_fd())
        {
            *current = Some(Redirect { terminal, drained });
        }
    }

    /// A pipe whose other end a thread copies into the log, line by line.
    fn into_the_log() -> Option<(OwnedFd, Option<Receiver<()>>)> {
        let (reader, writer) = std::io::pipe().ok()?;
        let (done, drained) = channel();
        std::thread::Builder::new()
            .name("datui-stderr".into())
            .spawn(move || {
                let mut reader = std::io::BufReader::new(reader);
                let mut line = Vec::new();
                loop {
                    line.clear();
                    match reader.read_until(b'\n', &mut line) {
                        Ok(0) => break,
                        // Caught, because a reader that died would close the pipe and
                        // turn every later `eprintln!` into a panic of its own.
                        Ok(_) => {
                            let _ = std::panic::catch_unwind(|| super::write_stray_line(&line));
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                        Err(_) => break,
                    }
                }
                let _ = done.send(());
            })
            .ok()?;
        Some((writer.into(), Some(drained)))
    }

    /// Hand the real stderr back. A no-op when it is not redirected.
    pub(super) fn restore() {
        let Some(redirect) = current().take() else {
            return;
        };
        point(redirect.terminal.as_raw_fd());
        // Let the reader finish what was written just before, so a panic report or a
        // last warning is in the log. Bounded: a child process that inherited fd 2
        // holds the pipe open for as long as it runs.
        if let Some(drained) = redirect.drained {
            let _ = drained.recv_timeout(Duration::from_millis(500));
        }
    }

    fn point(fd: RawFd) -> bool {
        // SAFETY: dup2 reads no memory; `fd` is open, as the caller holds it. fd 2 is
        // replaced atomically, so a concurrent write lands on one file or the other.
        unsafe { libc::dup2(fd, libc::STDERR_FILENO) >= 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_log_goes_to_the_cache_unless_configured() {
        let cache = Path::new("/cache/datui");
        let default = LogSettings::resolve(None, None, Some(cache));
        assert_eq!(default.path, Some(cache.join("datui.log")));
        assert_eq!(default.level, LevelFilter::Warn);

        let chosen = LogSettings::resolve(Some("/elsewhere/x.log"), Some("debug"), Some(cache));
        assert_eq!(chosen.path, Some(PathBuf::from("/elsewhere/x.log")));
        assert_eq!(chosen.level, LevelFilter::Debug);

        let blank = LogSettings::resolve(Some("  "), Some(" "), Some(cache));
        assert_eq!(blank.path, Some(cache.join("datui.log")));
        assert_eq!(blank.level, LevelFilter::Warn);
    }

    #[test]
    fn off_means_no_file_and_a_typo_means_the_default() {
        let off = LogSettings::resolve(None, Some("OFF"), Some(Path::new("/c")));
        assert_eq!(off.path, None);

        let typo = LogSettings::resolve(None, Some("loud"), Some(Path::new("/c")));
        assert_eq!(typo.level, LevelFilter::Warn);
        assert_eq!(typo.unknown_level.as_deref(), Some("loud"));
        assert!(typo.path.is_some());
    }

    #[test]
    fn a_full_log_moves_aside_once() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("sub").join("datui.log");
        let mut log = FileLog::open(&path, 100).unwrap();
        let line = "x".repeat(60);
        assert!(!log.write_line(&line).unwrap());
        assert!(log.write_line(&line).unwrap(), "past the cap it rotates");
        assert!(rotated_path(&path).exists());
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);
        // A second rotation replaces the first: one old file, never more.
        log.write_line(&"y".repeat(120)).unwrap();
        let old = std::fs::read_to_string(rotated_path(&path)).unwrap();
        assert!(old.starts_with('y'), "{old:?}");
        assert!(!dir.path().join("sub").join("datui.log.1.1").exists());
    }

    /// Two sessions share the log. One moves it aside; the other, still writing into
    /// the moved file, must not then move the first one's fresh log over it.
    #[test]
    fn two_sessions_never_rotate_each_others_lines_away() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui.log");
        let mut a = FileLog::open(&path, 100).unwrap();
        let mut b = FileLog::open(&path, 100).unwrap();
        let mut written = Vec::new();
        // Under two caps' worth in all: one rotation, so nothing may be lost.
        for n in 0..2 {
            for (who, log) in [("a", &mut a), ("b", &mut b)] {
                let line = format!("{who}{n} {}", "x".repeat(40));
                log.write_line(&line).unwrap();
                written.push(line);
            }
        }
        let all = std::fs::read_to_string(rotated_path(&path)).unwrap_or_default()
            + &std::fs::read_to_string(&path).unwrap();
        for line in &written {
            assert!(all.contains(line.as_str()), "lost {line:?} from {all:?}");
        }
    }

    #[test]
    fn credentials_are_masked() {
        let secrets = vec!["wJalrXUtnFEMI/K7MDENG".to_string()];
        let cases = [
            ("key wJalrXUtnFEMI/K7MDENG refused", "wJalrXUtnFEMI"),
            ("GET https://alice:hunter22@host/x failed", "hunter22"),
            (
                "403 for https://b.s3.amazonaws.com/k?X-Amz-Credential=AKIA123%2F&X-Amz-Signature=abcdef12",
                "abcdef12",
            ),
            (
                "https://acct.blob.core.windows.net/c?sv=2022&sig=Zm9vYmFy",
                "Zm9vYmFy",
            ),
            ("Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.x.y", "eyJhbGci"),
            (
                "DefaultEndpointsProtocol=https;AccountKey=c2VjcmV0a2V5;",
                "c2VjcmV0a2V5",
            ),
        ];
        for (line, secret) in cases {
            let masked = redact(line, &secrets);
            assert!(!masked.contains(secret), "{line} -> {masked}");
            assert!(masked.contains("***"), "{masked}");
        }
        assert_eq!(
            redact("s3://bucket/key.parquet: not found", &secrets),
            "s3://bucket/key.parquet: not found"
        );
        for plain in [
            "basic statistics failed for column_with_long_name",
            "abfss://container@account.dfs.core.windows.net/data.parquet",
        ] {
            assert_eq!(redact(plain, &secrets), plain);
        }
        assert!(!redact("Basic YWxpY2U6aHVudGVyMg==", &secrets).contains("YWxpY2U6"));
        assert!(!redact("sig=Zm9vYmFyYmF6&sv=2022", &secrets).contains("Zm9vYmFy"));
    }

    #[test]
    fn a_variable_is_masked_by_its_name_but_not_when_it_names_a_place() {
        let key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
        for name in [
            "AWS_SECRET_ACCESS_KEY",
            "AWS_SESSION_TOKEN",
            "AZURE_STORAGE_ACCOUNT_KEY",
            "AZURE_STORAGE_CONNECTION_STRING",
            "MINIO_ROOT_PASSWORD",
            "hf_token",
            "OPENAI_API_KEY",
        ] {
            assert!(holds_a_credential(name, key), "{name}");
        }
        for name in [
            "AWS_WEB_IDENTITY_TOKEN_FILE",
            "GITHUB_TOKEN_PATH",
            "PASSWORD_STORE_DIR",
            "VAULT_TOKEN_URL",
        ] {
            assert!(!holds_a_credential(name, key), "{name}");
        }
        assert!(!holds_a_credential("AWS_REGION", key));
        let dir = tempfile::tempdir().unwrap();
        let place = dir.path().to_string_lossy();
        assert!(
            !holds_a_credential("SOME_SECRET", &place),
            "a path that exists is not the secret"
        );
    }

    #[test]
    fn a_caught_panic_becomes_a_message() {
        assert_eq!(catch_panic(|| 7), Ok(7));
        let message = catch_panic::<()>(|| panic!("worker died")).unwrap_err();
        assert!(
            message.starts_with("Internal error: worker died"),
            "{message}"
        );
        let formatted = catch_panic::<()>(|| panic!("row {} of {}", 3, 9)).unwrap_err();
        assert!(formatted.contains("row 3 of 9"), "{formatted}");
    }

    /// The only test in this binary that sets the global logger and the Polars hook, so
    /// nothing else races it for the file.
    #[test]
    fn a_polars_warning_lands_in_the_log_once_and_a_user_warning_is_queued() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui.log");
        init(&LogSettings {
            path: Some(path.clone()),
            level: LevelFilter::Warn,
            unknown_level: None,
        });
        assert_eq!(current_path(), Some(path.clone()));

        for _ in 0..3 {
            polars_error::polars_warn!(
                Deprecation,
                "casting in test {} is deprecated.\nUse something else.",
                std::process::id()
            );
        }
        polars_error::polars_warn!(UserWarning, "remapped categories in test {}", 7);
        log::info!("below the level");
        log::logger().flush();

        let text = std::fs::read_to_string(&path).unwrap();
        let deprecation = format!(
            "Deprecation: casting in test {} is deprecated. Use something else.",
            std::process::id()
        );
        assert_eq!(text.matches(&deprecation).count(), 1, "{text}");
        assert!(
            text.contains("UserWarning: remapped categories in test 7"),
            "{text}"
        );
        assert!(!text.contains("below the level"), "{text}");

        // The user warning reaches the footer, once, when the bar is free.
        let (tx, _rx) = std::sync::mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());
        app.busy = true;
        assert!(!app.flash_polars_warning(), "a busy message outranks it");
        app.busy = false;
        app.error_modal.active = true;
        assert!(!app.flash_polars_warning(), "a modal would hide it");
        app.error_modal.active = false;
        let mut flashed = Vec::new();
        while app.flash_polars_warning() {
            flashed.extend(app.flash_message().map(str::to_string));
            app.flash = None;
        }
        assert!(
            flashed.contains(&"Polars: remapped categories in test 7".to_string()),
            "{flashed:?}"
        );
        assert!(!flashed.iter().any(|w| w.contains("casting in test")));
        polars_error::polars_warn!(UserWarning, "remapped categories in test {}", 7);
        assert!(
            std::iter::from_fn(next_polars_warning).all(|w| !w.contains("in test 7")),
            "a user warning is flashed once"
        );
    }
}
