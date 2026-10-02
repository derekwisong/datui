//! See `datui_fuzz::config_parse`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::config_parse::{Input, run};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: Input| run(input));
