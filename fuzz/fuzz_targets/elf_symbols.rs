//! See `datui_fuzz::elf_symbols`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::elf_symbols::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|bytes: &[u8]| run(bytes));
