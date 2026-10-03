//! candump logs and DBC files, run on arbitrary text.
//!
//! The input is split at its first NUL: a DBC file, then a log. The DBC parser reads
//! statements across lines with bounded counts and lengths; the log's lines are read
//! as frames; each message the DBC names is decoded from the log's frames, Intel and
//! Motorola bits, signed and multiplexed. Never a panic, and every decoded table has a
//! row per frame of its message.

use datui_lib::candump::{Decoded, Layers, Listing, index};
use datui_lib::fixed_records::Bytes;
use std::sync::Arc;

pub fn run(bytes: &[u8]) {
    let (dbc, log) = match bytes.iter().position(|&b| b == 0) {
        Some(at) => (&bytes[..at], &bytes[at + 1..]),
        None => (bytes, &b""[..]),
    };
    let dbc = String::from_utf8_lossy(dbc);
    let dbc = datui_lib::dbc::parse(&dbc, "fuzz", None).ok();
    let _ = datui_lib::candump::looks_like(log);
    let Ok(index) = index(log) else {
        return;
    };
    let shared = Arc::new(Bytes::Owned(log.to_vec()));
    let layers = Layers {
        dbcs: dbc.into_iter().map(Arc::new).collect(),
    };
    let listing = Listing::resolve(&index, layers);
    for (message, rows) in listing.messages.values() {
        let Ok(decoded) = Decoded::new(shared.clone(), &index, rows.clone(), message.clone(), true)
        else {
            continue;
        };
        let df = decoded
            .collect_window(0, rows.len().min(64))
            .expect("every frame of a message decodes");
        assert_eq!(df.height(), rows.len().min(64));
    }
}
