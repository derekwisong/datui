//! See `datui_fuzz::flight_log`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::flight_log::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| run(bytes));
