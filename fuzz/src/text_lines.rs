//! The text sniffer and the line index, run on arbitrary bytes.
//!
//! `guess` says what text no signature claims is, from a head that may be whole or cut
//! anywhere; it must answer, never panic. The index gives a row per line, the last
//! whether or not it ends in a newline; indexed in two parts, as a followed file grows,
//! it must match an index of the whole; and every row decodes.

use datui_lib::formats::fixed_records::Bytes;
use datui_lib::formats::lines::{LineIndex, Lines, guess};
use std::sync::Arc;

pub fn run(bytes: &[u8]) {
    let _ = guess(bytes, true);
    let _ = guess(bytes, false);

    let lines = bytes.iter().filter(|&&b| b == b'\n').count()
        + usize::from(bytes.last().is_some_and(|&b| b != b'\n'));
    let whole = LineIndex::of(bytes);
    assert_eq!(whole.lines(), lines);
    let cut = bytes.first().map_or(0, |&b| b as usize % (bytes.len() + 1));
    let mut grown = LineIndex::of(&bytes[..cut]);
    grown.extend(bytes);
    assert_eq!(grown.lines(), whole.lines());
    assert_eq!(grown.complete(), whole.complete());
    assert_eq!(grown.invalid, whole.invalid);

    let source = Lines::from_bytes(vec![("f".into(), Arc::new(Bytes::Owned(bytes.to_vec())))]);
    let df = source.collect_window(0, 256).expect("every line decodes");
    assert_eq!(df.height(), lines.min(256));
}
