//! See `datui_fuzz::sql_group_plan`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::sql_group_plan::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|sql: &str| run(sql));
