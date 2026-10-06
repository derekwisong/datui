//! A value handed to another program: the inspector's `o`.
//!
//! The value is written read-only to a file of its own, with an extension that
//! says what it is. Text goes to the user's `$VISUAL`, `$EDITOR` or `$PAGER`
//! (then `less`), run in the terminal while datui steps aside, and the file is
//! removed when it returns; nothing is read back. Images and PDFs, and text when
//! no program is named, go to the system's opener (`xdg-open`, `open`, or
//! Windows' `start`, which asks which program to use when none is set), which
//! returns at once, so their files stay until datui quits.

use std::path::{Path, PathBuf};

/// A file to open, written and waiting for the run loop.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalOpen {
    pub path: PathBuf,
    /// An image or a document: for the system's viewer, not a text editor.
    pub document: bool,
}

/// How a file is opened.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Program {
    /// Run in the terminal, which datui hands over until it returns.
    Wait(Vec<String>),
    /// Started, not waited on: it opens a window of its own.
    Opener(Vec<String>),
}

/// The system's opener for a file.
fn opener() -> Vec<String> {
    if cfg!(windows) {
        // `start`'s first quoted argument is the window title.
        ["cmd", "/c", "start", ""].map(String::from).to_vec()
    } else if cfg!(target_os = "macos") {
        vec!["open".to_string()]
    } else {
        vec!["xdg-open".to_string()]
    }
}

/// The program for a file, from the environment `env` looks up: a document goes
/// to the opener; text to `$VISUAL`, `$EDITOR`, `$PAGER`, then `less` outside
/// Windows, then the opener. A program set to blank counts as unset. Its words
/// are split at spaces, so `code --wait` runs `code` with `--wait`.
pub fn program_for(document: bool, env: impl Fn(&str) -> Option<String>) -> Program {
    if document {
        return Program::Opener(opener());
    }
    for name in ["VISUAL", "EDITOR", "PAGER"] {
        if let Some(value) = env(name).filter(|v| !v.trim().is_empty()) {
            return Program::Wait(value.split_whitespace().map(String::from).collect());
        }
    }
    if cfg!(windows) {
        Program::Opener(opener())
    } else {
        Program::Wait(vec!["less".to_string()])
    }
}

/// A file name for a value: the field's name with anything a path cannot hold
/// replaced, the row, and the extension.
pub fn file_name(field: &str, row: usize, extension: &str) -> String {
    let safe: String = field
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .take(60)
        .collect();
    let safe = safe.trim_matches('.');
    let safe = if safe.is_empty() { "value" } else { safe };
    format!("{safe}-row{row}.{extension}")
}

/// Write `bytes` to `name` in `dir`, read-only. A name already taken, by an
/// earlier open of the same value, takes a number.
pub fn write_value(dir: &Path, name: &str, bytes: &[u8]) -> std::io::Result<PathBuf> {
    let mut path = dir.join(name);
    let mut n = 1;
    while path.exists() {
        n += 1;
        path = dir.join(format!("{n}-{name}"));
    }
    std::fs::write(&path, bytes)?;
    // Read-only where that does not stop the file being removed afterward.
    if cfg!(unix) {
        let mut perms = std::fs::metadata(&path)?.permissions();
        perms.set_readonly(true);
        std::fs::set_permissions(&path, perms)?;
    }
    Ok(path)
}

/// Run `program` on `path`: wait for one that takes the terminal, start one that
/// does not. The caller has handed the terminal over for a wait.
pub fn run(program: &Program, path: &Path) -> std::io::Result<()> {
    let (argv, wait) = match program {
        Program::Wait(argv) => (argv, true),
        Program::Opener(argv) => (argv, false),
    };
    let Some((first, rest)) = argv.split_first() else {
        return Err(std::io::Error::other("no program to open the value with"));
    };
    if !wait {
        let mut argv: Vec<&std::ffi::OsStr> = argv.iter().map(|a| a.as_ref()).collect();
        argv.push(path.as_os_str());
        return start(&argv);
    }
    let status = std::process::Command::new(first)
        .args(rest)
        .arg(path)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "{first} exited with {status}"
        )));
    }
    Ok(())
}

/// Start `argv` as a program, no shell between, and return without waiting: it
/// opens a window of its own. A thread reaps it when it exits.
pub fn start<S: AsRef<std::ffi::OsStr>>(argv: &[S]) -> std::io::Result<()> {
    let Some((first, rest)) = argv.split_first() else {
        return Err(std::io::Error::other("no program to open with"));
    };
    let mut child = std::process::Command::new(first)
        .args(rest)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()?;
    std::thread::spawn(move || child.wait());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let pairs: Vec<(String, String)> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| {
            pairs
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v.clone())
        }
    }

    #[test]
    fn text_goes_to_the_editor_the_environment_names() {
        assert_eq!(
            program_for(false, env(&[("EDITOR", "code --wait"), ("PAGER", "less")])),
            Program::Wait(vec!["code".into(), "--wait".into()])
        );
        assert_eq!(
            program_for(false, env(&[("VISUAL", "vim"), ("EDITOR", "nano")])),
            Program::Wait(vec!["vim".into()])
        );
        assert_eq!(
            program_for(false, env(&[("EDITOR", " "), ("PAGER", "most")])),
            Program::Wait(vec!["most".into()]),
            "blank is unset"
        );
        let none = program_for(false, env(&[]));
        if cfg!(windows) {
            assert_eq!(none, Program::Opener(opener()));
        } else {
            assert_eq!(none, Program::Wait(vec!["less".into()]));
        }
        // An image goes to the system's viewer, whatever the editor.
        assert_eq!(
            program_for(true, env(&[("EDITOR", "vim")])),
            Program::Opener(opener())
        );
    }

    #[test]
    fn names_are_safe_and_files_read_only() {
        assert_eq!(
            file_name("payload json", 3, "json"),
            "payload_json-row3.json"
        );
        assert_eq!(file_name("../x", 1, "txt"), "_x-row1.txt");
        assert_eq!(file_name("", 1, "bin"), "value-row1.bin");
        let dir = tempfile::tempdir().unwrap();
        let path = write_value(dir.path(), "a.txt", b"one").unwrap();
        if cfg!(unix) {
            assert!(std::fs::metadata(&path).unwrap().permissions().readonly());
        }
        let again = write_value(dir.path(), "a.txt", b"two").unwrap();
        assert_ne!(again, path);
        assert_eq!(std::fs::read(again).unwrap(), b"two");
    }
}
