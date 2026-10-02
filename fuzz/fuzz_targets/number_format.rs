//! See `datui_fuzz::number_format`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::number_format::{Input, run};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: Input| run(input));
