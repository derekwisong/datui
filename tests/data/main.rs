//! Data: statistics and distribution analysis, pivot and melt, Excel. One test
//! executable, since each links the whole app; filter by module, as in
//! `scripts/dev/test.sh integration data statistics::`.
//!
//! `statistics` installs a counting global allocator for the whole executable.

#[path = "../common/mod.rs"]
mod common;

mod distribution;
mod excel;
mod reshape;
mod statistics;
