//! The ELF symbol and section tables, run on arbitrary bytes.
//!
//! The `object` crate reads the headers and tables at offsets and sizes the file
//! gives; the rows and the Info tab are built from them, names demangled. A corrupt
//! file must be an error, never a panic or an allocation sized by the file; a file
//! that reads gives a row per symbol and per section.

use datui_lib::elf::{demangle, read};

pub fn run(bytes: &[u8]) {
    if let Ok(text) = std::str::from_utf8(bytes) {
        let _ = demangle(text);
    }
    let Ok(elf) = read(bytes) else {
        return;
    };
    assert_eq!(elf.symbols.width(), 7);
    assert_eq!(elf.sections.width(), 6);
    assert!(elf.detail.list.len() <= datui_lib::limits::get().detail_rows + 1);
}
