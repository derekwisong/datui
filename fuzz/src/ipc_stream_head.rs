//! Telling an Arrow IPC stream by its first bytes.
//!
//! A stream has no magic, only its schema message, so `is_stream_head` reads a length
//! from the bytes and reads the flatbuffer it names. It runs on the start of any file in
//! a directory being opened and on whatever is piped in, so any bytes must give an
//! answer, never a panic, and a pipe must be read as the stream it is.

use datui_lib::formats::ipc_stream::is_stream_head;
use datui_lib::{FileFormat, stdin::sniff};

/// The most a pipe's sniff looks at.
const MAX_LEN: usize = 4096;

pub fn run(head: &[u8]) {
    if head.len() > MAX_LEN {
        return;
    }
    let stream = is_stream_head(head);
    let (format, compression) = sniff(head);
    // A compression magic is looked for first, and wins.
    if stream && compression.is_none() {
        assert_eq!(format, FileFormat::Arrow, "a stream sniffed as {format:?}");
    }
}
