//! Configuration: settings and their layers, the command line's flags, themes and
//! colors, and saved views. One test executable; filter by module, as in
//! `scripts/dev/test.sh integration config colors::`.
//!
//! The environment is the process's: tests here only ever remove `NO_COLOR` (a
//! test that needs it set builds its parser with it), and give any variable they
//! set a name no other test reads.

#[path = "../common/mod.rs"]
mod common;

mod colors;
mod flags;
mod indexed_colors;
mod settings;
mod themes;
mod view_store;
mod views;
