//! The VCD reader, which takes a value change dump a piece at a time.
//!
//! The first byte picks the size of the pieces the rest arrives in, so tokens, header
//! sections and UTF-8 sequences are cut at every place a read can cut them. A dump
//! must never panic or hold more than its bounds: every batch has the table's schema,
//! the rows add up, and the header stays within its limits.

use datui_lib::formats::vcd::{self, MAX_EXTEND, MAX_TEXT, MAX_TOKEN, VcdReader};

pub fn run(bytes: &[u8]) {
    let Some((&first, rest)) = bytes.split_first() else {
        return;
    };
    let piece = 1 + usize::from(first) * 7;
    let _ = vcd::looks_like(rest);
    let mut reader = VcdReader::new();
    let mut rows = 0;
    for chunk in rest.chunks(piece) {
        reader.push(chunk);
        if let Some(df) = reader.take_batch().expect("a batch builds") {
            assert_eq!(Some(df.schema().as_ref().clone()), reader.schema());
            rows += df.height();
        }
    }
    match reader.finish() {
        Ok(df) => {
            assert_eq!(Some(df.schema().as_ref().clone()), reader.schema());
            rows += df.height();
            let values = df.column("value").expect("value").str().expect("text");
            for v in (0..values.len()).filter_map(|i| values.get(i)) {
                assert!(v.len() <= MAX_TOKEN.max(MAX_EXTEND as usize), "{}", v.len());
            }
        }
        Err(_) => assert_eq!(rows, 0),
    }
    assert_eq!(rows as u64, reader.stats().rows);
    let header = reader.header();
    assert!(header.vars.len() <= datui_lib::limits::get().vcd_signals);
    assert!(header.vars.iter().all(|v| v.path.len() <= MAX_TEXT));
    let detail = vcd::detail(&reader);
    assert!(detail.list.len() <= header.vars.len() + 1);
}
