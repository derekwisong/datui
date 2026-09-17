//! Running the commands other tools provide for credentials: `aws`, a profile's
//! `credential_process`, and later `gcloud` and `az`.
//!
//! Asking a cloud's own CLI is how SSO, assume-role and MFA work without datui
//! reimplementing any of them. Every call here blocks, so it only ever runs on a
//! worker, never on the thread that draws. Arguments are passed as a list, with no
//! shell in between, and every call has a deadline.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

/// Why a command did not produce its output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandError {
    /// The program is not installed, or not on `PATH`.
    Missing(String),
    /// It ran and failed; the text is its error output.
    Failed(String),
    /// It did not finish in time and was stopped.
    TimedOut(String),
    /// An argument could not be passed safely.
    Refused(String),
}

impl std::fmt::Display for CommandError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CommandError::Missing(program) => write!(f, "needs {program}"),
            CommandError::Failed(message) => f.write_str(message),
            CommandError::TimedOut(program) => write!(f, "{program} did not answer in time"),
            CommandError::Refused(message) => f.write_str(message),
        }
    }
}

/// Runs one command: the real [`run`] with a deadline, or a stand-in in tests.
pub type Runner<'a> = dyn Fn(&str, &[&str]) -> Result<String, CommandError> + 'a;

/// How long a credential command may take. An SSO refresh is a network round trip;
/// anything slower than this is waiting on something that is not coming.
pub const CREDENTIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Run `program` with `args` and return what it printed.
pub fn run(program: &str, args: &[&str], timeout: Duration) -> Result<String, CommandError> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let pathext = std::env::var("PATHEXT").ok();
    let resolved = find_program(program, &path, pathext.as_deref(), cfg!(windows))
        .ok_or_else(|| CommandError::Missing(program.to_string()))?;

    let mut command = if is_batch_file(&resolved) {
        // A `.cmd` or `.bat` file cannot be started directly, only through `cmd`, and
        // `cmd` parses its arguments again with rules no escaping survives. Arguments
        // that reach it are held to characters it treats as plain text.
        if let Some(bad) = args.iter().find(|a| !is_plain_argument(a)) {
            return Err(CommandError::Refused(format!(
                "\"{bad}\" cannot be passed to {program}: use only letters, digits and . _ - : / ="
            )));
        }
        let mut command = Command::new("cmd");
        command.arg("/C").arg(&resolved);
        command
    } else {
        Command::new(&resolved)
    };
    command
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .map_err(|_| CommandError::Missing(program.to_string()))?;
    // Read both pipes on their own threads, so a chatty command cannot fill one and
    // stall waiting for a reader while this thread waits for it to exit.
    let stdout = child.stdout.take().map(read_to_end_on_thread);
    let stderr = child.stderr.take().map(read_to_end_on_thread);

    let deadline = Instant::now() + timeout;
    let status = loop {
        match child.try_wait() {
            Ok(Some(status)) => break status,
            Ok(None) if Instant::now() >= deadline => {
                let _ = child.kill();
                let _ = child.wait();
                return Err(CommandError::TimedOut(program.to_string()));
            }
            Ok(None) => std::thread::sleep(Duration::from_millis(20)),
            Err(e) => return Err(CommandError::Failed(format!("{program}: {e}"))),
        }
    };
    let stdout = stdout.and_then(|h| h.join().ok()).unwrap_or_default();
    let stderr = stderr.and_then(|h| h.join().ok()).unwrap_or_default();
    if status.success() {
        Ok(stdout)
    } else {
        let message = stderr.trim();
        Err(CommandError::Failed(if message.is_empty() {
            format!("{program} exited with {status}")
        } else {
            message.to_string()
        }))
    }
}

fn read_to_end_on_thread<R: Read + Send + 'static>(
    mut reader: R,
) -> std::thread::JoinHandle<String> {
    std::thread::spawn(move || {
        let mut text = String::new();
        let _ = reader.read_to_string(&mut text);
        text
    })
}

/// Where `program` would be run from: itself when it names a path, else the first
/// match on `path`. On Windows each `PATHEXT` extension is tried too, which is how
/// `gcloud` finds `gcloud.cmd`.
pub fn find_program(
    program: &str,
    path: &std::ffi::OsStr,
    pathext: Option<&str>,
    windows: bool,
) -> Option<PathBuf> {
    let candidate = Path::new(program);
    if candidate.components().count() > 1 {
        return candidate.is_file().then(|| candidate.to_path_buf());
    }
    let extensions: Vec<String> = if windows && candidate.extension().is_none() {
        pathext
            .unwrap_or(".COM;.EXE;.BAT;.CMD")
            .split(';')
            .filter(|e| !e.is_empty())
            .map(|e| e.to_ascii_lowercase())
            .collect()
    } else {
        vec![String::new()]
    };
    for dir in std::env::split_paths(path) {
        for extension in &extensions {
            let file = dir.join(format!("{program}{extension}"));
            if file.is_file() {
                return Some(file);
            }
        }
    }
    None
}

fn is_batch_file(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .is_some_and(|e| e.eq_ignore_ascii_case("cmd") || e.eq_ignore_ascii_case("bat"))
}

/// Characters `cmd` passes through untouched.
fn is_plain_argument(arg: &str) -> bool {
    arg.chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-' | ':' | '/' | '=' | '\\'))
}

/// Split a command line into a program and its arguments, the way a POSIX shell does
/// for words and quotes, without doing anything else a shell does: no variables, no
/// globs, no pipes. Enough for `credential_process = "op read ..." --flag 'a b'`.
pub fn split_command_line(line: &str) -> Option<Vec<String>> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut in_word = false;
    let mut chars = line.chars();
    while let Some(c) = chars.next() {
        match c {
            '\'' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '\'' => break,
                        other => word.push(other),
                    }
                }
            }
            '"' => {
                in_word = true;
                loop {
                    match chars.next()? {
                        '"' => break,
                        '\\' => word.push(chars.next()?),
                        other => word.push(other),
                    }
                }
            }
            '\\' => {
                in_word = true;
                word.push(chars.next()?);
            }
            c if c.is_whitespace() => {
                if in_word {
                    words.push(std::mem::take(&mut word));
                    in_word = false;
                }
            }
            other => {
                in_word = true;
                word.push(other);
            }
        }
    }
    if in_word {
        words.push(word);
    }
    (!words.is_empty()).then_some(words)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_command_line_splits_like_a_shell_without_being_one() {
        assert_eq!(
            split_command_line(r#"op read "op://Private/AWS lab/json" --no-newline"#).unwrap(),
            ["op", "read", "op://Private/AWS lab/json", "--no-newline"]
        );
        assert_eq!(
            split_command_line("/opt/bin/creds 'a b' c\\ d $HOME").unwrap(),
            ["/opt/bin/creds", "a b", "c d", "$HOME"]
        );
        assert_eq!(split_command_line("   "), None);
        assert_eq!(split_command_line("unterminated 'quote"), None);
    }

    #[test]
    fn only_plain_text_reaches_a_batch_file() {
        assert!(is_plain_argument("--profile"));
        assert!(is_plain_argument("research-prod_2"));
        assert!(!is_plain_argument("work&calc"));
        assert!(!is_plain_argument("a b"));
        assert!(!is_plain_argument("%PATH%"));
        assert!(is_batch_file(Path::new(r"C:\sdk\bin\gcloud.cmd")));
        assert!(!is_batch_file(Path::new(
            r"C:\Program Files\Amazon\AWSCLIV2\aws.exe"
        )));
    }

    #[test]
    fn a_program_is_found_on_path_with_windows_extensions() {
        let dir = tempfile::TempDir::new().unwrap();
        std::fs::write(dir.path().join("gcloud.cmd"), "").unwrap();
        std::fs::write(dir.path().join("aws"), "").unwrap();
        let path = std::env::join_paths([dir.path()]).unwrap();

        let windows = find_program("gcloud", &path, Some(".EXE;.CMD"), true);
        assert_eq!(windows, Some(dir.path().join("gcloud.cmd")));
        assert_eq!(find_program("gcloud", &path, None, false), None);
        assert_eq!(
            find_program("aws", &path, None, false),
            Some(dir.path().join("aws"))
        );
        assert_eq!(find_program("az", &path, Some(".CMD"), true), None);
    }

    #[cfg(unix)]
    #[test]
    fn output_failure_missing_and_timeout_are_told_apart() {
        assert_eq!(
            run(
                "sh",
                &["-c", "printf '{\"Version\": 1}'"],
                Duration::from_secs(5)
            ),
            Ok("{\"Version\": 1}".to_string())
        );
        assert_eq!(
            run(
                "sh",
                &["-c", "echo expired >&2; exit 3"],
                Duration::from_secs(5)
            ),
            Err(CommandError::Failed("expired".to_string()))
        );
        assert_eq!(
            run("datui-no-such-program", &[], Duration::from_secs(5)),
            Err(CommandError::Missing("datui-no-such-program".to_string()))
        );
        let started = Instant::now();
        assert_eq!(
            run("sh", &["-c", "sleep 10"], Duration::from_millis(200)),
            Err(CommandError::TimedOut("sh".to_string()))
        );
        assert!(started.elapsed() < Duration::from_secs(5));
    }
}
