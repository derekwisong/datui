//! The App end to end, apart from `integration_test`: the captured view, escape
//! sequences in what is drawn, catalogs, public datasets, and Data Quality's intent
//! and export. One test executable; filter by module, as in
//! `scripts/dev/test.sh integration app catalog::`.
//!
//! `table_sample.rs` in this directory is a module of `integration_test`.

#[path = "../common/mod.rs"]
mod common;

mod capture;
#[cfg(all(feature = "cloud", feature = "http"))]
mod catalog;
#[cfg(all(feature = "cloud", feature = "http"))]
mod public_datasets;
mod quality_export;
mod terminal_escape;
