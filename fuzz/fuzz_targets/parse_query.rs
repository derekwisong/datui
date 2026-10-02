//! See `datui_fuzz::parse_query`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::parse_query::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|query: &str| run(query));
