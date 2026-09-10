//! Config deserialisation, validation and colour parsing.
//!
//! The config file is user-authored TOML, and datui also loads theme files written by
//! other tools. Deserialisation itself is serde's problem, but everything after it is
//! datui's: `validate` checks ranges, `merge` layers imports over defaults, and
//! `ColorParser::parse` slices colour strings by byte offset after checking a byte
//! length, which is only sound while those strings stay ASCII.
#![no_main]

use arbitrary::Arbitrary;
use datui_lib::config::{AppConfig, ColorParser};
use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;

/// Built once. `ColorParser::new` probes terminal capabilities, which is not something
/// to repeat on every iteration.
fn parser() -> &'static ColorParser {
    static PARSER: OnceLock<ColorParser> = OnceLock::new();
    PARSER.get_or_init(|| {
        // `parse` short-circuits to `Color::Reset` when NO_COLOR is set, which would
        // hide every path this target exists to reach.
        std::env::remove_var("NO_COLOR");
        ColorParser::new()
    })
}

#[derive(Arbitrary, Debug)]
struct Input<'a> {
    /// Interpreted as the contents of a config.toml.
    toml: &'a str,
    /// Interpreted as colour values, the way a theme file supplies them.
    colors: Vec<&'a str>,
}

const MAX_TOML_LEN: usize = 16 * 1024;

fuzz_target!(|input: Input| {
    if input.toml.len() > MAX_TOML_LEN {
        return;
    }

    let parser = parser();
    for color in input.colors.iter().take(64) {
        let _ = parser.parse(color);
    }

    // A config that does not deserialise is an expected outcome, not a finding.
    let Ok(parsed) = toml::from_str::<AppConfig>(input.toml) else {
        return;
    };

    // Validation runs on every load and must reject rather than panic.
    let _ = parsed.validate();

    // Merging is how imports and themes layer onto the defaults. It reads fields from
    // both sides, so a config that deserialised but holds nonsense goes through here
    // before anything has rejected it.
    let mut base = AppConfig::default();
    base.merge(parsed);
    let _ = base.validate();
});
