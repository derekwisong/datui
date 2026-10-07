//! The SafeTensors and GGUF header parsers, run on arbitrary bytes.
//!
//! Both read lengths and counts from the file and allocate or skip by them, so a
//! corrupt header must be an error: never a panic, an overflow, or an allocation sized
//! by a number the file made up. Every input is given to both parsers, whatever its
//! first bytes, and a header that parses must build its table. Read again by range,
//! as a remote file is, in ranges a few bytes long, each finds the same header or fails
//! as the file reader does.

use datui_lib::FileFormat;
use datui_lib::formats::model_files::{
    RangeError, RangeSource, build, read_gguf, read_header_ranged_from, read_safetensors,
};

/// The input, served by range.
struct Slice<'a>(&'a [u8]);

impl RangeSource for Slice<'_> {
    fn get(&mut self, start: u64, end: u64) -> Result<(Vec<u8>, u64), RangeError> {
        let len = self.0.len() as u64;
        if start >= len {
            return Err(RangeError::Failed("past the end".to_string()));
        }
        Ok((self.0[start as usize..end.min(len) as usize].to_vec(), len))
    }
}

pub fn run(bytes: &[u8]) {
    let len = bytes.len() as u64;
    let gguf = read_gguf(bytes, len);
    let safetensors = read_safetensors(bytes, len);
    // The first range from the input itself, so the corpus covers many.
    let first = u64::from(bytes.first().copied().unwrap_or(1) % 16) + 1;
    for (format, read) in [
        (FileFormat::Gguf, &gguf),
        (FileFormat::Safetensors, &safetensors),
    ] {
        let ranged = read_header_ranged_from(&mut Slice(bytes), format, first, &|| false);
        match (read, ranged) {
            (Ok(header), Ok(ranged)) => assert_eq!(header, &ranged, "{format:?}"),
            (Err(_), Err(_)) => {}
            (read, ranged) => panic!(
                "{format:?}: the file reader said {:?}, the ranged one {:?}",
                read.as_ref().map(|_| ()),
                ranged.map(|_| ())
            ),
        }
    }
    for header in [gguf, safetensors].into_iter().flatten() {
        let tensors = header.tensors.len();
        let (_, summary) = build(&[header], &["fuzz".to_string()], Vec::new())
            .expect("a header that parses builds its table");
        assert_eq!(summary.tensors, tensors);
        let typed: usize = summary.types.iter().map(|t| t.tensors).sum();
        assert_eq!(typed, tensors, "every tensor has a type in the mix");
    }
}
