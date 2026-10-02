//! See `datui_fuzz::audio_header`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::audio_header::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| run(bytes));
