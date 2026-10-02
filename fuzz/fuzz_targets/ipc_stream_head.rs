//! See `datui_fuzz::ipc_stream_head`, which the corpus replay test shares.
#![no_main]

use datui_fuzz::ipc_stream_head::run;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|head: &[u8]| run(head));
