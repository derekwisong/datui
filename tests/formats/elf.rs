//! ELF files as their symbol and section tables: what uses flash and RAM.
//!
//! `tests/sample-data/elf/tiny.elf` is written by hand by
//! `scripts/generate_sample_data.py`; a second ELF is built with `cc` when there is
//! one. Anything a test writes goes to `fixture_dir()`.

use crate::common::{self, pump_open_until_loaded};
use datui::{App, AppEvent, OpenOptions};
use polars::prelude::*;
use std::path::PathBuf;
use std::sync::mpsc;

fn tiny() -> PathBuf {
    common::ensure_sample_data();
    PathBuf::from("tests/sample-data/elf/tiny.elf")
}

fn open_with(path: PathBuf, options: OpenOptions) -> App {
    let (tx, rx) = mpsc::channel::<AppEvent>();
    let mut app = App::new(tx, common::test_runtime());
    pump_open_until_loaded(&mut app, &rx, vec![path], options);
    app
}

fn frame(app: &App) -> DataFrame {
    app.data_table_state
        .as_ref()
        .expect("a dataset is open")
        .lf()
        .clone()
        .collect()
        .expect("collect")
}

fn strings(df: &DataFrame, column: &str) -> Vec<String> {
    df.column(column)
        .unwrap()
        .str()
        .unwrap()
        .iter()
        .map(|v| v.unwrap_or_default().to_string())
        .collect()
}

#[test]
fn an_elf_file_opens_as_its_symbols() {
    let app = open_with(tiny(), OpenOptions::default());
    assert_eq!(app.error_message(), None);
    let df = frame(&app);
    assert_eq!(
        df.get_column_names()
            .iter()
            .map(|n| n.as_str())
            .collect::<Vec<_>>(),
        ["name", "addr", "size", "kind", "bind", "section", "region"]
    );
    let names = strings(&df, "name");
    let at = |n: &str| names.iter().position(|v| v == n).unwrap();
    assert!(names.contains(&"core::fmt::write".to_string()), "{names:?}");
    let region = strings(&df, "region");
    assert_eq!(region[at("main")], "flash");
    assert_eq!(region[at("buffer")], "ram");
    assert_eq!(strings(&df, "section")[at("buffer")], ".bss");
    let state = app.data_table_state.as_ref().unwrap();
    assert_eq!(state.format_detail().map(|d| d.tab), Some("ELF"));
}

/// Sorting by size and grouping by region says what fills flash and RAM.
#[test]
fn what_uses_flash_and_ram() {
    let df = frame(&open_with(tiny(), OpenOptions::default()));
    let by = df
        .lazy()
        .filter(col("region").is_not_null())
        .group_by([col("region")])
        .agg([col("size").sum()])
        .sort(["region"], Default::default())
        .collect()
        .unwrap();
    assert_eq!(strings(&by, "region"), ["flash", "ram"]);
    assert_eq!(
        by.column("size").unwrap().u64().unwrap().to_vec(),
        [Some(64 + 256 + 128 + 8), Some(4 + 1024)]
    );
}

#[test]
fn sections_are_the_other_table() {
    for app in [
        open_with(
            tiny(),
            OpenOptions {
                table: Some("sections".to_string()),
                ..OpenOptions::default()
            },
        ),
        open_with(tiny().join("sections"), OpenOptions::default()),
    ] {
        assert_eq!(app.error_message(), None);
        let df = frame(&app);
        let names = strings(&df, "name");
        let flags = strings(&df, "flags");
        let at = names.iter().position(|n| n == ".text").unwrap();
        assert_eq!(flags[at], "AX");
        let at = names.iter().position(|n| n == ".bss").unwrap();
        assert_eq!(strings(&df, "region")[at], "ram");
    }
}

/// A file with no extension is known by its first bytes.
#[test]
fn an_elf_file_is_known_by_its_first_bytes() {
    let dir = common::fixture_dir().join(format!("elf-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let firmware = dir.join("firmware");
    std::fs::copy(tiny(), &firmware).unwrap();
    let app = open_with(firmware, OpenOptions::default());
    assert_eq!(app.error_message(), None);
    assert_eq!(frame(&app).height(), 6);
}

/// A program built with `cc`, when there is one: its globals land in the sections and
/// regions their declarations say.
#[test]
fn a_program_built_with_cc() {
    let dir = common::fixture_dir().join(format!("elf-cc-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let source = dir.join("prog.c");
    std::fs::write(
        &source,
        "const int lookup[64] = {1};\n\
         int counter = 7;\n\
         char scratch[4096];\n\
         int add(int a, int b) { return a + b + counter + lookup[a & 63] + scratch[b & 4095]; }\n\
         int main(void) { return add(1, 2); }\n",
    )
    .unwrap();
    let binary = dir.join("prog.elf");
    let built = std::process::Command::new("cc")
        .arg("-O0")
        .arg("-o")
        .arg(&binary)
        .arg(&source)
        .status();
    if !built.is_ok_and(|s| s.success()) {
        eprintln!("no working cc: the hand-written fixture covers ELF reading");
        return;
    }
    let df = frame(&open_with(binary, OpenOptions::default()));
    let names = strings(&df, "name");
    let at = |n: &str| names.iter().position(|v| v == n).unwrap();
    let region = strings(&df, "region");
    let section = strings(&df, "section");
    assert_eq!(region[at("add")], "flash");
    assert_eq!(region[at("lookup")], "flash");
    assert_eq!(region[at("counter")], "ram");
    assert_eq!(section[at("scratch")], ".bss");
    let sizes = df.column("size").unwrap().u64().unwrap();
    assert_eq!(sizes.get(at("scratch")), Some(4096));
}
