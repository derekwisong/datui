//! See `datui_fuzz::glob_match`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::glob_match::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (&str, &str)| run(input));
