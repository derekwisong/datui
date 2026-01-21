use std::path::Path;
use std::process::Command;
use std::sync::Once;

#[allow(dead_code)]
static INIT: Once = Once::new();

/// Ensures that sample data files are generated before tests run.
/// This function uses `std::sync::Once` to ensure it only runs once,
/// even if called from multiple tests.
#[allow(dead_code)]
pub fn ensure_sample_data() {
    INIT.call_once(|| {
        let sample_data_dir = Path::new("tests/sample-data");

        // Check if key files exist to determine if we need to generate data
        // We check for a few representative files that should always be generated
        let key_files = [
            "people.parquet",
            "sales.parquet",
            "large_dataset.parquet",
            "empty.parquet",
        ];

        let needs_generation = !sample_data_dir.exists()
            || key_files
                .iter()
                .any(|file| !sample_data_dir.join(file).exists());

        if needs_generation {
            eprintln!("Sample data not found. Generating test data...");

            // Get the path to the Python script
            let script_path = Path::new("scripts/generate_sample_data.py");
            if !script_path.exists() {
                panic!(
                    "Sample data generation script not found at: {}. \
                    Please ensure you're running tests from the repository root.",
                    script_path.display()
                );
            }

            // Try to find Python (python3 or python)
            let python_cmd = if Command::new("python3").arg("--version").output().is_ok() {
                "python3"
            } else if Command::new("python").arg("--version").output().is_ok() {
                "python"
            } else {
                panic!(
                    "Python not found. Please install Python 3 to generate test data. \
                    The script requires: polars>=0.20.0 and numpy>=1.24.0"
                );
            };

            // Run the generation script
            let output = Command::new(python_cmd)
                .arg(script_path)
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
                panic!(
                    "Sample data generation failed!\n\
                    Exit code: {:?}\n\
                    stdout:\n{}\n\
                    stderr:\n{}",
                    output.status.code(),
                    stdout,
                    stderr
                );
            }

            eprintln!("Sample data generation complete!");
        }
    });
}
