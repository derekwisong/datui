//! The fuzz targets' bodies, one module per target.
//!
//! Each `fuzz_targets/<target>.rs` lets libfuzzer-sys decode the input and calls `run`
//! here. `tests/fuzz_corpus_test.rs` in the main workspace includes these same files
//! and replays every committed corpus input through them as an ordinary test, so a
//! check added to `run` reaches both.
pub mod audio_header;
pub mod can_parse;
pub mod config_parse;
pub mod elf_symbols;
pub mod fix_dict;
pub mod fix_parse;
pub mod flight_log;
pub mod format_spec;
pub mod fuzzy_match;
pub mod glob_match;
pub mod gps_parse;
pub mod hex_input;
pub mod ipc_stream_head;
pub mod midi_file;
pub mod model_header;
pub mod number_format;
pub mod numpy_header;
pub mod parse_query;
pub mod sdf_parse;
pub mod sql_group_plan;
pub mod vcd_parse;
