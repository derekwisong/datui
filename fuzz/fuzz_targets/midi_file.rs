//! See `datui_fuzz::midi_file`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::midi_file::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| run(bytes));
