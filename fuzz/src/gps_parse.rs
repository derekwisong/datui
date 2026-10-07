//! The NMEA 0183 and GPX readers, which take a GPS log a piece at a time.
//!
//! Every input goes to both. The first byte picks the NMEA table and the size of the
//! pieces the rest arrives in, so lines, tags, entities and UTF-8 sequences are cut
//! at every place a read can cut them. A log must never panic or hold more than its
//! bounds: whatever it reads gives frames of the table's own schema, with every
//! coordinate that is not null on the globe.

use datui_lib::gps::gpx::{self, GpxReader};
use datui_lib::gps::nmea::{NmeaReader, Table};

/// Every latitude and longitude in a frame that is not null is in range. A macro, so
/// this crate need not depend on Polars to name the frame's type.
macro_rules! on_the_globe {
    ($df:expr) => {
        for (name, limit) in [("lat", 90.0), ("lon", 180.0)] {
            if let Ok(column) = $df.column(name) {
                let values = column.f64().expect("coordinates are Float64");
                for v in (0..values.len()).filter_map(|i| values.get(i)) {
                    assert!(v.is_finite() && f64::abs(v) <= limit, "{name} {v}");
                }
            }
        }
    };
}

pub fn run(bytes: &[u8]) {
    let Some((&first, rest)) = bytes.split_first() else {
        return;
    };
    let table = Table::ALL[usize::from(first) % Table::ALL.len()];
    let piece = 1 + usize::from(first / 9) * 37;

    let mut log = NmeaReader::new(table);
    let mut rows = 0;
    for chunk in rest.chunks(piece) {
        log.push(chunk);
        if let Some(df) = log.take_batch().expect("a batch builds") {
            assert_eq!(df.schema().as_ref(), &table.schema());
            on_the_globe!(df);
            rows += df.height();
        }
    }
    let df = log.finish().expect("the last batch builds");
    assert_eq!(df.schema().as_ref(), &table.schema());
    on_the_globe!(df);
    rows += df.height();
    assert_eq!(rows as u64, log.stats().rows);
    assert!(log.stats().skipped + log.stats().sentences <= log.stats().lines);

    let mut reader = GpxReader::new();
    for chunk in rest.chunks(piece) {
        if reader.push(chunk).is_err() {
            return;
        }
        if let Some(df) = reader.take_batch().expect("a batch builds") {
            on_the_globe!(df);
        }
    }
    if let Ok(df) = reader.finish() {
        assert_eq!(df.width(), gpx::CORE.len() + reader.fields().len());
        assert!(reader.fields().len() <= datui_lib::limits::get().gpx_fields);
        on_the_globe!(df);
    }
}
