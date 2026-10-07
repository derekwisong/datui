//! The SDF reader, which takes a compound file a piece at a time.
//!
//! The first byte picks the size of the pieces the rest arrives in, so lines, records
//! and UTF-8 sequences are cut at every place a read can cut them. A file must never
//! panic or hold more than its bounds: the rows add up to the records, the fields stay
//! within their limit, and no value is longer than a value may be.

use datui_lib::sdf::{self, CORE, MAX_VALUE, SdfReader};

pub fn run(bytes: &[u8]) {
    let Some((&first, rest)) = bytes.split_first() else {
        return;
    };
    let piece = 1 + usize::from(first) * 7;
    let _ = sdf::looks_like(rest);
    let mut reader = SdfReader::new();
    let mut rows = 0;
    // A macro, so this crate need not depend on Polars to name the frame's type.
    macro_rules! check {
        ($df:expr) => {{
            let df = $df;
            assert_eq!(df.width(), CORE.len() + reader.fields().len());
            for column in df.columns().iter().skip(CORE.len()) {
                let values = column.str().expect("fields are text until typed");
                assert!(
                    (0..values.len())
                        .filter_map(|i| values.get(i))
                        .all(|v| v.len() <= MAX_VALUE)
                );
            }
            df.height()
        }};
    }
    for chunk in rest.chunks(piece) {
        reader.push(chunk);
        if let Some(df) = reader.take_batch().expect("a batch builds") {
            rows += check!(df);
        }
    }
    let df = reader.finish().expect("the last batch builds");
    rows += check!(df);
    assert_eq!(rows as u64, reader.stats().records);
    assert!(reader.fields().len() <= datui_lib::limits::get().sdf_fields);
    let _ = sdf::detail(&reader);
}
