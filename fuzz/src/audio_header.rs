//! The WAV, RF64 and AIFF chunk walker and the sample decoder, run on arbitrary bytes.
//!
//! The walker reads chunk sizes, counts and offsets from the file and slices by them,
//! and the decoder reads every sample at an offset worked out from the header. A
//! corrupt header must be an error, never a panic, an overflow or an allocation sized
//! by a number the file made up; a header that parses must give frames that lie inside
//! the file, and decoding any of them must work.

use datui_lib::audio::{AudioSource, read_header};

/// The most frames decoded per input, at each end.
const WINDOW: u64 = 4096;

pub fn run(bytes: &[u8]) {
    let Ok(header) = read_header(bytes) else {
        return;
    };
    let channels = header.channels as usize;
    assert_eq!(header.channel_names.len(), channels);
    assert!(header.frame_bytes >= channels * header.sample.bytes());
    let len = bytes.len() as u64;
    let frames = header.frames(len);
    let end = frames
        .checked_mul(header.frame_bytes as u64)
        .and_then(|n| n.checked_add(header.data_offset))
        .expect("the frames' extent fits in u64");
    assert!(frames == 0 || end <= len, "the frames lie inside the file");

    for normalize in [false, true] {
        let source = AudioSource::from_bytes(bytes, normalize).expect("a header that parses opens");
        assert_eq!(source.frames(), frames);
        let head = source
            .window(0, WINDOW, None)
            .expect("the first frames decode");
        assert_eq!(head.height() as u64, frames.min(WINDOW));
        assert_eq!(head.width(), channels + 2);
        let from = frames.saturating_sub(WINDOW);
        let tail = source
            .window(from, WINDOW, None)
            .expect("the last frames decode");
        assert_eq!(tail.height() as u64, frames - from);
        let reports = source.signal_report(&|| false).expect("nothing stops it");
        assert_eq!(reports.len(), channels);
    }
}
