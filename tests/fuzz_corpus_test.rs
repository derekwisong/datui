//! Replays every committed fuzz corpus through the fuzz targets' own bodies.
//!
//! The same check as `scripts/code/fuzz.sh replay`, without the instrumented build:
//! the bodies in `fuzz/src/` are included as they are, and each input is decoded the
//! way libfuzzer-sys decodes it. A panic fails the test, as it would crash the fuzzer.
//! Its own target: `config_parse` removes `NO_COLOR` from the environment.

// The bodies name the library as the fuzz crate does.
extern crate datui as datui_lib;

#[path = "../fuzz/src/config_parse.rs"]
mod config_parse;
#[path = "../fuzz/src/fuzzy_match.rs"]
mod fuzzy_match;
#[path = "../fuzz/src/glob_match.rs"]
mod glob_match;
#[path = "../fuzz/src/number_format.rs"]
mod number_format;
#[path = "../fuzz/src/parse_query.rs"]
mod parse_query;
#[cfg(feature = "sql")]
#[path = "../fuzz/src/sql_group_plan.rs"]
mod sql_group_plan;

use arbitrary::{Arbitrary, Unstructured};
use std::collections::BTreeSet;
use std::panic::{self, AssertUnwindSafe};
use std::path::Path;

const FUZZ_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/fuzz");

/// libfuzzer-sys's `fuzz_target!` for a typed input: an input shorter than the type's
/// minimum, or one that fails to decode, is rejected without running the body.
fn fuzz<'a, T: Arbitrary<'a>>(bytes: &'a [u8], run: fn(T)) {
    if bytes.len() < T::size_hint(0).0 {
        return;
    }
    if let Ok(input) = T::arbitrary_take_rest(Unstructured::new(bytes)) {
        run(input);
    }
}

/// What libFuzzer runs with `-runs=0`: the empty input, then every corpus file. A
/// `&[u8]` target would take each one as it is, without `fuzz`.
fn replay(target: &str, failures: &mut Vec<String>, run: impl Fn(&[u8])) {
    let dir = Path::new(FUZZ_DIR).join("corpus").join(target);
    let mut inputs = vec![("<empty>".to_string(), Vec::new())];
    for entry in std::fs::read_dir(&dir).unwrap_or_else(|e| panic!("{}: {e}", dir.display())) {
        let path = entry.expect("corpus entry").path();
        let name = path.file_name().unwrap().to_string_lossy().into_owned();
        inputs.push((name, std::fs::read(&path).expect("corpus file")));
    }
    assert!(inputs.len() > 1, "{} holds no inputs", dir.display());
    for (name, bytes) in inputs {
        if let Err(payload) = panic::catch_unwind(AssertUnwindSafe(|| run(&bytes))) {
            let message = payload
                .downcast_ref::<&str>()
                .map(|s| s.to_string())
                .or_else(|| payload.downcast_ref::<String>().cloned())
                .unwrap_or_default();
            failures.push(format!("{target}/{name}: {message}"));
        }
    }
}

/// One test, so `config_parse`'s environment change never races another test's thread.
#[test]
fn every_corpus_input_passes_its_target() {
    let mut failures = Vec::new();
    replay("config_parse", &mut failures, |b| {
        fuzz::<config_parse::Input>(b, config_parse::run)
    });
    replay("fuzzy_match", &mut failures, |b| {
        fuzz::<(&str, &str)>(b, fuzzy_match::run)
    });
    replay("glob_match", &mut failures, |b| {
        fuzz::<(&str, &str)>(b, glob_match::run)
    });
    replay("number_format", &mut failures, |b| {
        fuzz::<number_format::Input>(b, number_format::run)
    });
    replay("parse_query", &mut failures, |b| {
        fuzz::<&str>(b, parse_query::run)
    });
    #[cfg(feature = "sql")]
    replay("sql_group_plan", &mut failures, |b| {
        fuzz::<&str>(b, sql_group_plan::run)
    });
    assert!(
        failures.is_empty(),
        "inputs that panicked:\n{}",
        failures.join("\n")
    );
}

/// A new target's corpus must be replayed here too.
#[test]
fn every_corpus_has_a_replay() {
    let corpora: BTreeSet<String> = std::fs::read_dir(Path::new(FUZZ_DIR).join("corpus"))
        .expect("fuzz/corpus")
        .map(|e| {
            e.expect("corpus dir")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    let replayed: BTreeSet<String> = [
        "config_parse",
        "fuzzy_match",
        "glob_match",
        "number_format",
        "parse_query",
        "sql_group_plan",
    ]
    .map(String::from)
    .into();
    assert_eq!(corpora, replayed);
}

/// Inputs decode the same here as in the fuzzer only while both lockfiles agree on
/// `arbitrary`, whose encoding has changed between releases.
#[test]
fn arbitrary_matches_the_fuzz_lockfile() {
    let versions = |lockfile: &Path| -> BTreeSet<(String, String)> {
        let text = std::fs::read_to_string(lockfile).expect("lockfile");
        let lock: toml::Table = toml::from_str(&text).expect("lockfile parses");
        lock["package"]
            .as_array()
            .expect("packages")
            .iter()
            .filter_map(|p| {
                let name = p["name"].as_str()?;
                matches!(name, "arbitrary" | "derive_arbitrary")
                    .then(|| (name.to_string(), p["version"].as_str().unwrap().to_string()))
            })
            .collect()
    };
    let main = versions(&Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.lock"));
    let fuzz = versions(&Path::new(FUZZ_DIR).join("Cargo.lock"));
    assert!(!main.is_empty(), "arbitrary is missing from Cargo.lock");
    assert_eq!(
        main, fuzz,
        "Cargo.lock and fuzz/Cargo.lock resolve arbitrary differently"
    );
}
