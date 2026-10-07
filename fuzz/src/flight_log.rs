//! ULog and DataFlash flight logs, run on arbitrary bytes.
//!
//! Each reader walks messages by sizes and type ids the file gives, defines types from
//! the file's own format records, and records where each type's messages start. The
//! first byte picks the reader. A corrupt log must be passed over or end the pass,
//! never a panic or an allocation sized by the file; every message the index records
//! must decode, all of its columns.

use datui_lib::formats::fixed_records::Bytes;
use datui_lib::formats::indexed::{IndexedRecords, Log};
use std::sync::Arc;

pub fn run(bytes: &[u8]) {
    let Some((&pick, rest)) = bytes.split_first() else {
        return;
    };
    let shared = Arc::new(Bytes::Owned(rest.to_vec()));
    if pick % 2 == 0 {
        let mut data = datui_lib::formats::ulog::MAGIC.to_vec();
        data.push(1);
        data.extend([0u8; 8]);
        data.extend(rest);
        let shared = Arc::new(Bytes::Owned(data.clone()));
        let Ok(index) = datui_lib::formats::ulog::index(&data) else {
            return;
        };
        let _ = index.tables();
        let _ = index.detail();
        for topic in index.topics.values() {
            let records =
                IndexedRecords::new(shared.clone(), topic.offsets.clone(), topic.columns.clone())
                    .expect("columns of distinct names");
            let rows = records.rows();
            let df = records
                .collect_window(0, rows.min(64))
                .expect("every indexed message decodes");
            assert_eq!(df.height(), rows.min(64));
        }
    } else {
        let Ok(index) = datui_lib::formats::dataflash::index(rest) else {
            return;
        };
        let _ = index.detail();
        for (_, id) in index.names() {
            let t = &index.types[&id];
            let (columns, _) = datui_lib::formats::dataflash::columns(&index, t);
            let Ok(records) = IndexedRecords::new(shared.clone(), t.offsets.clone(), columns)
            else {
                // Two labels alike: a log can say so.
                continue;
            };
            let rows = records.rows();
            let df = records
                .collect_window(0, rows.min(64))
                .expect("every indexed record decodes");
            assert_eq!(df.height(), rows.min(64));
        }
    }
}
