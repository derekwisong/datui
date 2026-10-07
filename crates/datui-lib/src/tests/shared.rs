//! What the library's tests and the integration tests share: the generated fixtures in
//! `tests/sample-data`, a drawn buffer as text, and the tokio runtime. `tests/common`
//! includes this file.

use std::fs::File;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;

use fs2::FileExt;
use sha2::{Digest, Sha256};

/// What a contributor runs to set up the fixtures' Python environment.
const SETUP: &str = "./scripts/dev/setup-test-data.sh";

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
pub fn ensure_sample_data() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let root = repo_root();
        let dir = root.join("tests/sample-data");
        let digest = inputs_digest(&root);
        if is_current(&dir, &digest) {
            return;
        }
        let lock_path = root.join("tests/.sample-data.lock");
        let lock = File::create(&lock_path)
            .unwrap_or_else(|e| panic!("creating {}: {e}", lock_path.display()));
        lock.lock_exclusive()
            .unwrap_or_else(|e| panic!("locking {}: {e}", lock_path.display()));
        // Another process may have generated them while this one waited.
        if !is_current(&dir, &digest) {
            generate(&root, &dir);
        }
        // Dropping the file releases the lock.
    });
}

fn generate(root: &Path, dir: &Path) {
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
    let pythons: Vec<std::ffi::OsString> = if venv_python.exists() {
        vec![venv_python.into()]
    } else {
        vec!["python3".into(), "python".into()]
    };
    let fail = |detail: String| -> ! {
        panic!(
            "The test fixtures in {} are missing or out of date, and generating them \
             failed.\nRun {SETUP} to set up .venv and generate them.\n\n{detail}",
            dir.display()
        )
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
        .unwrap_or_else(|| fail("No Python found.".to_string()));
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        fail(format!(
            "generate_sample_data.py exited with {}:\n{}",
            output.status,
            stderr.trim_end()
        ));
    }
    eprintln!("Generated {}.", dir.display());
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
