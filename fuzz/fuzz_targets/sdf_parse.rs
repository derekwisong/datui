//! See `datui_fuzz::sdf_parse`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::sdf_parse::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| run(bytes));
