//! The hex view's parsers and search, run on arbitrary input: text up to the first
//! NUL is what was typed (an offset, a pattern), and the rest is the file.
//!
//! An offset that parses must be inside the file. A pattern that parses must have a
//! byte that is not `??` and fit the bound. A match the search reports must be one,
//! the first from the start must be the first a plain scan finds, and the inspector
//! must read any bytes without a panic.

use datui_lib::app::hex_view::{
    MAX_PATTERN, Pattern, find, parse_offset, parse_pattern, readings, stride, varint,
};
use std::sync::atomic::AtomicBool;

fn matches_at(hay: &[u8], pattern: &Pattern, at: usize) -> bool {
    hay.get(at..at + pattern.bytes.len()).is_some_and(|w| {
        w.iter()
            .zip(&pattern.bytes)
            .all(|(b, p)| p.is_none_or(|p| p == *b))
    })
}

pub fn run(input: &[u8]) {
    let split = input.iter().position(|&b| b == 0).unwrap_or(input.len());
    let text = String::from_utf8_lossy(&input[..split]);
    let hay = input.get(split + 1..).unwrap_or_default();
    let len = hay.len() as u64;
    let cursor = len / 2;
    if let Ok(at) = parse_offset(&text, cursor, len) {
        assert!(at < len, "offset {at} past a file of {len}");
    }
    let stop = AtomicBool::new(false);
    for utf16 in [false, true] {
        let Ok(pattern) = parse_pattern(&text, utf16) else {
            continue;
        };
        assert!(!pattern.bytes.is_empty() && pattern.bytes.len() <= MAX_PATTERN);
        assert!(pattern.bytes.iter().any(Option::is_some));
        for forward in [true, false] {
            let hit = find(hay, &pattern, cursor, forward, &stop, |_| {}).expect("not stopped");
            if let Some(at) = hit.at {
                assert!(matches_at(hay, &pattern, at as usize), "no match at {at}");
            }
        }
        let first = find(hay, &pattern, 0, true, &stop, |_| {})
            .expect("not stopped")
            .at;
        let naive = (0..hay.len()).find(|&at| matches_at(hay, &pattern, at));
        assert_eq!(first, naive.map(|at| at as u64));
        if let Some(at) = first {
            let _ = stride(hay, &pattern, at, &stop);
        }
    }
    let _ = readings(&hay[..hay.len().min(64)]);
    if let Some((_, n)) = varint(hay) {
        assert!((1..=10).contains(&n) && n <= hay.len());
    }
}
