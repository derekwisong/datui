//! See `datui_fuzz::format_spec`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::format_spec::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| run(input));
