//! The NumPy `.npy` header and the array behind it, run on arbitrary bytes.
//!
//! The header is a Python dict literal parsed by hand, with nested structured types
//! whose offsets, itemsizes and subarray shapes come from the file. A corrupt header
//! must be an error, never a panic, an overflow, or an allocation sized by a number the
//! file made up. A header that parses must give columns inside the bytes on hand, and
//! the rows it counts must decode.

use datui_lib::formats::fixed_records::Bytes;
use datui_lib::formats::numpy::{looks_like, open_in, parse_header, parse_literal};
use std::sync::Arc;

pub fn run(bytes: &[u8]) {
    let _ = looks_like(bytes);
    // The literal alone, from wherever the dict would start.
    if let Some(text) = bytes.get(10..).and_then(|t| std::str::from_utf8(t).ok()) {
        let _ = parse_literal(text);
    }
    if parse_header(bytes).is_err() {
        return;
    }
    let Ok(array) = open_in(Arc::new(Bytes::Owned(bytes.to_vec())), 0, bytes.len(), "x") else {
        return;
    };
    let rows = array.records.rows();
    let df = array
        .records
        .collect(rows.min(64))
        .expect("the rows on hand decode");
    assert_eq!(df.height(), rows.min(64));
    if rows > 0 {
        let window = array
            .records
            .window(rows - 1, 1)
            .expect("the last row decodes");
        assert_eq!(window.height(), 1);
    }
}
