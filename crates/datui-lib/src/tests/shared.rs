//! What the library's tests and the integration tests share: the generated fixtures in
//! `tests/sample-data`, a drawn buffer as text, and the tokio runtime. `tests/common`
//! includes this file.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Once;

/// The repository: the nearest directory above the crate under test that holds the
/// fixture generator.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .find(|dir| dir.join("scripts/generate_sample_data.py").exists())
        .expect("the repository holding scripts/generate_sample_data.py")
        .to_path_buf()
}

/// `tests/sample-data`, generated first when its key files are missing.
pub fn sample_data_dir() -> PathBuf {
    ensure_sample_data();
    repo_root().join("tests/sample-data")
}

/// Ensures that sample data files are generated before tests run.
/// This function uses `std::sync::Once` to ensure it only runs once,
/// even if called from multiple tests.
pub fn ensure_sample_data() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let root = repo_root();
        let sample_data_dir = root.join("tests/sample-data");

        // Check if key files exist to determine if we need to generate data
        // We check for a few representative files that should always be generated
        let key_files = [
            "people.parquet",
            "sales.parquet",
            "large_dataset.parquet",
            "empty.parquet",
            "pivot_long.parquet",
            "melt_wide.parquet",
            "models/tiny.gguf",
            "people_stream.arrow",
            "dialect_padded_log.csv",
            "gps/drive.nmea",
            "audio/loop.aiff",
            "midi/song.mid",
            "sqlite/shop.db",
            "numpy/packed.npz",
            "elf/tiny.elf",
            "flight/00000042.BIN",
            "can/dbc/body.toml",
            "hf_cache/people-test.arrow",
            "arrow_mixed/b.arrow",
            "hf_dict/dataset_dict.json",
            "sheets.xlsx",
        ];

        let needs_generation = !sample_data_dir.exists()
            || key_files
                .iter()
                .any(|file| !sample_data_dir.join(file).exists());

        if needs_generation {
            eprintln!("Sample data not found. Generating test data...");

            // Get the path to the Python script
            let script_path = root.join("scripts/generate_sample_data.py");
            if !script_path.exists() {
                panic!(
                    "Sample data generation script not found at: {}. \
                    Please ensure you're running tests from the repository root.",
                    script_path.display()
                );
            }

            // Prefer the project virtualenv: the generator needs Polars and friends,
            // which a system Python almost never has. Falling straight through to
            // `python3` produces a bare ImportError that tells nobody what to do.
            let venv_python = if cfg!(windows) {
                root.join(".venv/Scripts/python.exe")
            } else {
                root.join(".venv/bin/python")
            };

            let python_cmd = if venv_python.exists() {
                venv_python.to_string_lossy().into_owned()
            } else if Command::new("python3").arg("--version").output().is_ok() {
                "python3".to_string()
            } else if Command::new("python").arg("--version").output().is_ok() {
                "python".to_string()
            } else {
                panic!(
                    "Python not found, and no project virtualenv at {}.\n\
                     Run ./scripts/dev/setup-test-data.sh to create one and generate \
                     the fixtures these tests read.",
                    venv_python.display()
                );
            };

            // Run the generation script
            let output = Command::new(python_cmd)
                .arg(&script_path)
                .output()
                .unwrap_or_else(|e| {
                    panic!(
                        "Failed to run sample data generation script: {}. \
                        Make sure Python is installed and the script is executable.",
                        e
                    );
                });

            if !output.status.success() {
                let stderr = String::from_utf8_lossy(&output.stderr);
                let stdout = String::from_utf8_lossy(&output.stdout);
                let hint = if venv_python.exists() {
                    String::new()
                } else {
                    format!(
                        "\n\nNo virtualenv at {}. This usually means the generator's \
                         dependencies (Polars, NumPy, pyarrow, fastavro, openpyxl) are \
                         missing.\nRun ./scripts/dev/setup-test-data.sh to set it up.",
                        venv_python.display()
                    )
                };
                panic!(
                    "Sample data generation failed!\n\
                    Exit code: {:?}\n\
                    stdout:\n{}\n\
                    stderr:\n{}{}",
                    output.status.code(),
                    stdout,
                    stderr,
                    hint
                );
            }

            eprintln!("Sample data generation complete!");
        }
    });
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
