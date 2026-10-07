//! The FIX log reader, which takes a log a piece at a time.
//!
//! The first byte picks the size of the pieces the rest arrives in, so messages,
//! length-tagged values and UTF-8 sequences are cut at every place a read can cut them.
//! A log must never panic or hold more than its bounds; the rows add up to the messages,
//! and the last batch, which has every column, renames and types into a frame that
//! collects.

use datui_lib::fix::dict::Layers;
use datui_lib::fix::{self, FixReader};

pub fn run(bytes: &[u8]) {
    let Some((&first, rest)) = bytes.split_first() else {
        return;
    };
    let piece = 1 + usize::from(first) * 7;
    let _ = fix::looks_like(rest);
    let mut reader = FixReader::new(Layers::default());
    let mut rows = 0;
    for chunk in rest.chunks(piece) {
        reader.push(chunk);
        if let Some(df) = reader.take_batch().expect("a batch builds") {
            rows += df.height();
        }
    }
    let last = reader.finish().expect("the last batch builds");
    rows += last.height();
    assert_eq!(rows as u64, reader.stats().messages);
    assert!(last.width() <= 5 + 3 * datui_lib::limits::get().fix_tags);
    let height = last.height();
    let df = reader.finished(last).expect("the frame renames and types");
    assert_eq!(df.height(), height);
    let _ = fix::detail(&reader);
}
