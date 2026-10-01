//! Config deserialisation, validation and colour parsing.
//!
//! The config file is user-authored TOML, and datui also loads theme files written by
//! other tools. Deserialisation itself is serde's problem, but everything after it is
//! datui's: `validate` checks ranges, `ConfigLayer` layers imports over defaults, and
//! `ColorParser::parse` slices colour strings by byte offset after checking a byte
//! length, which is only sound while those strings stay ASCII.

use arbitrary::Arbitrary;
use datui_lib::config::{AppConfig, ColorParser, ConfigLayer};
use std::sync::OnceLock;

/// Built once. `ColorParser::new` probes terminal capabilities, which is not something
/// to repeat on every iteration.
fn parser() -> &'static ColorParser {
    static PARSER: OnceLock<ColorParser> = OnceLock::new();
    PARSER.get_or_init(|| {
        // `parse` short-circuits to `Color::Reset` when NO_COLOR is set, which would
        // hide every path this target exists to reach.
        // SAFETY: libFuzzer calls the target on a single thread, and the corpus
        // replay test runs every target from one test.
        unsafe { std::env::remove_var("NO_COLOR") };
        ColorParser::new()
    })
}

#[derive(Arbitrary, Debug)]
pub struct Input<'a> {
    /// Interpreted as the contents of a config.toml.
    toml: &'a str,
    /// Interpreted as colour values, the way a theme file supplies them.
    colors: Vec<&'a str>,
}

const MAX_TOML_LEN: usize = 16 * 1024;

pub fn run(input: Input) {
    if input.toml.len() > MAX_TOML_LEN {
        return;
    }

    let parser = parser();
    for color in input.colors.iter().take(64) {
        let _ = parser.parse(color);
    }

    // A config that does not parse is an expected outcome, not a finding. The gate is
    // the loader's own: a layer is valid TOML that also types as a config. Typing
    // alone is laxer, since a table datui does not know is skipped unread, and an
    // integer past i64 there deserialises although it is not TOML.
    let Ok(layer) = ConfigLayer::parse(input.toml) else {
        return;
    };
    let parsed: AppConfig = toml::from_str(input.toml).expect("a layer types as a config");

    // Validation runs on every load and must reject rather than panic.
    let _ = parsed.validate();

    // Layering is how imports and themes combine. A layer is checked when it is
    // parsed, so laying it over itself and resolving the result must succeed: a
    // failure here would surface as an error blamed on no particular file.
    let resolved =
        AppConfig::from_layers([layer.clone(), layer]).expect("layers that parse resolve");
    let _ = resolved.validate();
}
