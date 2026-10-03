//! Format specs, and the records they decode, from one input.
//!
//! A spec is TOML a person or a team writes, and the file it reads is whatever is on
//! disk, so neither is trusted. The input is the spec's text, a NUL byte, then the
//! file's bytes: a seed stays a spec someone can read. Parsing must give a spec or an
//! error with a line and column; reading must give rows or an error; and the rows
//! counted must decode, every one, in any window. A delimited spec's header lines
//! must read or fail, and its metadata line must parse or stay raw.

use datui_lib::delimited_spec::parse_metadata;
use datui_lib::fixed_records::Bytes;
use datui_lib::formats::{Layout, Registry, Spec};
use std::path::Path;
use std::sync::Arc;

const MAX_SPEC_LEN: usize = 16 * 1024;

/// Rows decoded per input: enough to cross a record boundary many times over.
const MAX_ROWS: usize = 256;

pub fn run(input: &[u8]) {
    let (spec, data) = match input.iter().position(|&b| b == 0) {
        Some(at) => (&input[..at], &input[at + 1..]),
        None => (input, &[][..]),
    };
    if spec.len() > MAX_SPEC_LEN {
        return;
    }
    let Ok(text) = std::str::from_utf8(spec) else {
        return;
    };
    let spec = match Spec::parse(text, None) {
        Ok(spec) => spec,
        Err(e) => {
            // A problem in the text says where it is.
            assert!(e.line <= text.lines().count() + 1, "{e}");
            return;
        }
    };
    let _ = spec.match_summary();
    let _ = spec.glob_matches(Path::new("dir/day.l2"));
    let _ = spec.magic_matches(data);
    let _ = spec.header_matches(data);
    let registry = Registry::of(vec![spec.clone()]);
    let _ = registry.matching(Path::new("f.bin"), false, |reach| {
        Some(data[..data.len().min(reach as usize)].to_vec())
    });
    if let Some(delimited) = spec.delimited.as_deref() {
        // A delimited spec reads only its header lines apart from the CSV reader.
        let _ = delimited.summary();
        if let Ok(facts) = delimited.facts(data, b',', " ") {
            assert!(facts.units.iter().all(|(_, unit)| !unit.is_empty()));
        }
        let first = data.split(|&b| b == b'\n').next().unwrap_or_default();
        let metadata = parse_metadata(&String::from_utf8_lossy(first));
        assert!(metadata.pairs.iter().all(|(key, _)| !key.is_empty()));
        return;
    }
    if spec.layout != Layout::Rows {
        return;
    }
    let Ok(opened) = spec.open_rows(Arc::new(Bytes::Owned(data.to_vec())), "f.bin") else {
        return;
    };
    let records = opened.records;
    // Every record takes at least a byte, past the header.
    assert!(records.rows() <= data.len());
    let head = records
        .collect(MAX_ROWS)
        .expect("the rows the reader counted decode");
    assert_eq!(head.height(), records.rows().min(MAX_ROWS));
    // A window starts where it says: its first row is that row of the head.
    if head.height() > 1 {
        let window = records
            .window(1, 1)
            .expect("a window of counted rows decodes");
        assert!(window.equals_missing(&head.slice(1, 1)));
    }
}
