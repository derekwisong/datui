//! What the library's tests and the integration tests share: the generated fixtures in
//! `tests/sample-data`, a drawn buffer as text, and the tokio runtime. `tests/common`
//! includes this file.

use std::ffi::OsString;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::OnceLock;

use fs2::FileExt;
use sha2::{Digest, Sha256};

/// What a contributor runs to set up the fixtures' Python environment.
const SETUP: &str = "scripts/dev/test.sh setup (on Windows: python scripts\\setup_dev.py)";

/// The repository: the nearest directory above the crate under test that holds the
/// fixture generator.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|dir| dir.join("scripts/generate_sample_data.py").exists())
        .expect("the repository holding scripts/generate_sample_data.py")
        .to_path_buf()
}

/// `tests/sample-data`, generated first when missing or stale.
pub fn sample_data_dir() -> PathBuf {
    ensure_sample_data();
    repo_root().join("tests/sample-data")
}

/// The digest the generator writes to `tests/sample-data/.generated`: SHA-256 over its
/// inputs, in the order `INPUTS` in `generate_sample_data.py` lists them.
fn inputs_digest(root: &Path) -> String {
    let mut hasher = Sha256::new();
    for input in [
        "scripts/generate_sample_data.py",
        "scripts/requirements-fixtures.txt",
    ] {
        let bytes =
            std::fs::read(root.join(input)).unwrap_or_else(|e| panic!("reading {input}: {e}"));
        hasher.update(&bytes);
    }
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn is_current(dir: &Path, digest: &str) -> bool {
    std::fs::read_to_string(dir.join(".generated")).is_ok_and(|stamp| stamp.trim() == digest)
}

/// Makes sure `tests/sample-data` was generated from the generator and pins in this
/// checkout, generating it when not. Test processes run side by side (nextest starts
/// one per test), so the check and the generation happen under a lock file, and the
/// generator replaces each fixture by a rename: a process with one mapped keeps it.
/// Every test in a process that could not generate them fails with the same message.
pub fn ensure_sample_data() {
    static READY: OnceLock<Result<(), String>> = OnceLock::new();
    if let Err(message) = READY.get_or_init(prepare) {
        panic!("{message}");
    }
}

fn prepare() -> Result<(), String> {
    let root = repo_root();
    let dir = root.join("tests/sample-data");
    let digest = inputs_digest(&root);
    if is_current(&dir, &digest) {
        return Ok(());
    }
    let lock_path = root.join("tests/.sample-data.lock");
    let lock = File::create(&lock_path)
        .and_then(|lock| lock.lock_exclusive().map(|()| lock))
        .map_err(|e| format!("locking {}: {e}", lock_path.display()))?;
    // Another process may have generated them while this one waited.
    let generated = if is_current(&dir, &digest) {
        Ok(())
    } else {
        generate(&root, &dir)
    };
    drop(lock);
    generated.map_err(|detail| {
        format!(
            "Could not generate the test fixtures in {}.\n\
             Run {SETUP} to set up .venv and generate them.\n\n{detail}",
            dir.display()
        )
    })
}

fn generate(root: &Path, dir: &Path) -> Result<(), String> {
    eprintln!(
        "{} is missing or out of date; generating it...",
        dir.display()
    );
    let venv_python = if cfg!(windows) {
        root.join(".venv/Scripts/python.exe")
    } else {
        root.join(".venv/bin/python")
    };
    // The project virtualenv has the pinned dependencies; a system Python seldom does.
    let pythons: Vec<OsString> = if venv_python.exists() {
        vec![venv_python.into()]
    } else {
        vec!["python3".into(), "python".into()]
    };
    let output = pythons
        .iter()
        .find_map(|python| {
            Command::new(python)
                .arg(root.join("scripts/generate_sample_data.py"))
                .arg("--out")
                .arg(dir)
                .output()
                .ok()
        })
        .ok_or("No Python found.")?;
    if !output.status.success() {
        return Err(format!(
            "generate_sample_data.py exited with {}:\n{}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim_end()
        ));
    }
    eprintln!("Generated {}.", dir.display());
    Ok(())
}

/// Each row of `buf` as drawn.
pub fn buffer_lines(buf: &ratatui::buffer::Buffer) -> Vec<String> {
    let area = buf.area;
    (area.top()..area.bottom())
        .map(|y| {
            (area.left()..area.right())
                .map(|x| buf[(x, y)].symbol())
                .collect()
        })
        .collect()
}

/// `buf` as drawn, a line a row.
pub fn buffer_text(buf: &ratatui::buffer::Buffer) -> String {
    buffer_lines(buf).join("\n")
}

/// The tests' one tokio runtime. Every test that builds an App comes through here, so
/// it is the one place that keeps them all out of the developer's real cache.
pub fn test_runtime() -> tokio::runtime::Handle {
    super::isolate_cache();
    static RT: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(1)
            .enable_all()
            .build()
            .expect("test tokio runtime")
    })
    .handle()
    .clone()
}
