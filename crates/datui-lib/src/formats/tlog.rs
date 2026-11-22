//! MAVLink telemetry logs (`.tlog`), as QGroundControl, Mission Planner and MAVProxy
//! save them.
//!
//! A tlog is every frame the ground station received, each after an 8-byte
//! big-endian timestamp in microseconds since the epoch. `mavlink-tlog` reads it:
//! each frame is checked against its message's CRC_EXTRA, and a damaged stretch is
//! passed over to the next valid record and counted. The log opens as one row per
//! frame: `time`, sender, sequence, message id and name, and the payload's bytes.

use std::sync::Arc;

use color_eyre::Result;
use mavlink_tlog::{LinkStats, TlogSlice};
use polars::prelude::*;

use crate::formats::columns::{Builder, Cell, Kind};
use crate::formats::model_files::MetaValue;
use crate::formats::text_formats::Detail;

/// What datui does with a tlog: see [`crate::formats::readers`].
pub(crate) const READER: crate::formats::readers::Reader = crate::formats::readers::Reader {
    scan,
    ..crate::formats::readers::BASE
};

/// A tlog read: a row per frame, its Info tab, and what was passed over.
pub struct Tlog {
    pub frames: DataFrame,
    pub detail: Detail,
    pub skipped_bytes: u64,
    pub bad_crc: u64,
}

/// Read the frames of the tlog in `data`.
pub fn read(data: &[u8]) -> std::result::Result<Tlog, String> {
    let mut log = TlogSlice::new(data);
    let mut link = LinkStats::default();
    let mut rows = Builder::new(&[
        ("time", Kind::DatetimeUs),
        ("sys", Kind::U8),
        ("comp", Kind::U8),
        ("seq", Kind::U8),
        ("msg_id", Kind::U32),
        ("message", Kind::Label),
        ("payload", Kind::Binary),
    ]);
    for rec in log.by_ref() {
        link.observe(rec.timestamp_us, &rec.frame);
        let f = rec.frame;
        rows.push([
            Cell::DatetimeUs(i64::try_from(rec.timestamp_us).ok()),
            Cell::U8(Some(f.sys_id())),
            Cell::U8(Some(f.comp_id())),
            Cell::U8(Some(f.seq())),
            Cell::U32(Some(f.msg_id())),
            Cell::Label(f.message_name()),
            Cell::Binary(Some(f.payload().to_vec())),
        ]);
    }
    let stats = log.stats();
    if stats.records == 0 && !data.is_empty() {
        return Err("not a MAVLink tlog: no frame passes its checksum".into());
    }
    let frames = rows.take().map_err(|e| e.to_string())?;
    let mut lines = vec![format!(
        "{} frames from {} senders",
        crate::numfmt::group_chrome(stats.records as usize),
        link.sources().len()
    )];
    if stats.skipped_bytes > 0 {
        lines.push(format!(
            "{} damaged bytes passed over ({} bad checksums)",
            crate::numfmt::group_chrome(stats.skipped_bytes as usize),
            stats.bad_crc
        ));
    }
    let list = link
        .sources()
        .into_iter()
        .map(|((sys, comp), s)| {
            (
                format!("{sys}/{comp}"),
                MetaValue::Text(format!("{} frames, {} lost", s.received, s.lost)),
            )
        })
        .collect();
    Ok(Tlog {
        frames,
        detail: Detail {
            tab: crate::formats::text_formats::tab(crate::FileFormat::Tlog),
            lines,
            list_title: "Senders",
            list,
            ..Default::default()
        },
        skipped_bytes: stats.skipped_bytes,
        bad_crc: stats.bad_crc,
    })
}

fn scan(input: crate::formats::readers::ScanIn<'_>) -> Result<crate::loading::scan::Scan> {
    let path = input.path().to_path_buf();
    let bytes = crate::formats::fixed_records::Bytes::map(&path)?;
    let tlog =
        read(bytes.as_slice()).map_err(|e| crate::error_display::FileError::new(&path, e))?;
    let mut notes = Vec::new();
    if tlog.skipped_bytes > 0 {
        notes.push(crate::formats::text_formats::note(
            format!(
                "{} damaged bytes passed over",
                crate::numfmt::group_chrome(tlog.skipped_bytes as usize)
            ),
            "the log".to_string(),
        ));
    }
    input.report.opened = Some(Arc::new(crate::formats::members::Opened {
        detail: Some(Arc::new(tlog.detail)),
        notes,
        ..Default::default()
    }));
    Ok(tlog.frames.lazy().into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use mavlink_tlog::{Header, TlogWriter, Version, encode};

    fn heartbeat(seq: u8) -> Vec<u8> {
        let header = Header {
            seq,
            sys_id: 1,
            comp_id: 1,
            msg_id: 0,
        };
        let mut frame = [0u8; 280];
        let len = encode(
            Version::V2,
            header,
            &[0, 0, 0, 0, 2, 3, 81, 4, 3],
            None,
            &mut frame,
        )
        .unwrap();
        frame[..len].to_vec()
    }

    #[test]
    fn a_row_per_frame_and_damage_counted() {
        let mut w = TlogWriter::new(Vec::new());
        w.write_frame(1_700_000_000_000_000, &heartbeat(0)).unwrap();
        let mut bytes = w.into_inner();
        // A frame whose checksum is wrong is passed over, not read as a row.
        let mut bad = vec![0u8; 8];
        let mut frame = heartbeat(1);
        let last = frame.len() - 1;
        frame[last] ^= 0xFF;
        bad.extend(frame);
        bytes.extend(&bad);
        let mut w = TlogWriter::new(bytes);
        w.write_frame(1_700_000_000_100_000, &heartbeat(2)).unwrap();
        let tlog = read(&w.into_inner()).unwrap();
        let df = &tlog.frames;
        assert_eq!(df.height(), 2);
        let names: Vec<_> = df
            .column("message")
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .collect();
        assert_eq!(names, [Some("HEARTBEAT"), Some("HEARTBEAT")]);
        let seq: Vec<_> = df.column("seq").unwrap().u8().unwrap().iter().collect();
        assert_eq!(seq, [Some(0), Some(2)]);
        assert_eq!(tlog.skipped_bytes, bad.len() as u64);
    }

    #[test]
    fn not_a_tlog() {
        assert!(read(b"hello there, this is not telemetry").is_err());
    }
}
