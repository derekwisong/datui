//! See `datui_fuzz::hex_input`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::hex_input::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: &[u8]| run(input));
