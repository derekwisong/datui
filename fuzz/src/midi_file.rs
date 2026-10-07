//! The Standard MIDI File parser, run on arbitrary bytes.
//!
//! It reads chunk lengths, variable-length deltas and event lengths from the file and
//! slices by them, and keeps running status between events, so a corrupt file must be
//! an error: never a panic, an overflow, or an allocation sized by a number the file
//! made up. A file that parses must build its table, one row per event.

use datui_lib::formats::midi::{build, looks_like_midi, parse};

pub fn run(bytes: &[u8]) {
    let _ = looks_like_midi(bytes);
    let Ok(smf) = parse(bytes) else {
        return;
    };
    let events: usize = smf.tracks.iter().map(Vec::len).sum();
    let tracks = smf.tracks.len();
    let (lf, summary) =
        build(&[("fuzz.mid".to_string(), smf)]).expect("a file that parses builds its table");
    assert_eq!(summary.events, events);
    assert_eq!(summary.track_count, tracks);
    assert_eq!(summary.tracks.len(), tracks);
    assert!(summary.unended <= summary.notes);
    let df = lf.collect().expect("the table collects");
    assert_eq!(df.height(), events);
}
