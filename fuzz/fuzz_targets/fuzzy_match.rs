//! See `datui_fuzz::fuzzy_match`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::fuzzy_match::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|input: (&str, &str)| run(input));
