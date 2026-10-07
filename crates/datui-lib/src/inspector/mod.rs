//! The row inspector: one row's fields, a long value read a screen at a time, drilling
//! into nested values, and handing a value to another program.

pub mod external_open;
pub mod inspector_bytes;
pub(crate) mod inspector_copy;
pub mod inspector_drill;
pub(crate) mod inspector_keys;
pub mod inspector_modal;
pub mod inspector_reader;
pub(crate) mod inspector_reads;
pub(crate) mod inspector_value_keys;
