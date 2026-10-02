//! The SafeTensors and GGUF header parsers, run on arbitrary bytes.
//!
//! Both read lengths and counts from the file and allocate or skip by them, so a
//! corrupt header must be an error: never a panic, an overflow, or an allocation sized
//! by a number the file made up. Every input is given to both parsers, whatever its
//! first bytes, and a header that parses must build its table.

use datui_lib::model_files::{build, read_gguf, read_safetensors};

pub fn run(bytes: &[u8]) {
    let len = bytes.len() as u64;
    let gguf = read_gguf(bytes, len);
    let safetensors = read_safetensors(bytes, len);
    for header in [gguf, safetensors].into_iter().flatten() {
        let tensors = header.tensors.len();
        let (_, summary) = build(&[header], &["fuzz".to_string()], Vec::new())
            .expect("a header that parses builds its table");
        assert_eq!(summary.tensors, tensors);
        let typed: usize = summary.types.iter().map(|t| t.tensors).sum();
        assert_eq!(typed, tensors, "every tensor has a type in the mix");
    }
}
