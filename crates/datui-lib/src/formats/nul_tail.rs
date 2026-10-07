//! A text file that ends in a run of NUL bytes ends where the run starts. Loggers that
//! preallocate a file at a fixed size and never write past their last line leave the
//! rest as NULs, which a CSV reader would take as one last row of junk text.
//!
//! Finding the run costs one byte read for a file that has none: its last byte. Only
//! a file whose last byte is NUL is mapped and scanned back to its last other byte.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

/// Where the text of `bytes` ends: before its trailing run of NULs, or `None` when it
/// has none, or when the NULs are part of the text (UTF-16 or UTF-32, by its byte-order
/// mark, or by a NUL beside the last character).
pub fn text_end(bytes: &[u8]) -> Option<usize> {
    if bytes.last() != Some(&0) {
        return None;
    }
    // Text in two- or four-byte units carries NULs inside it, and its last unit can
    // end in one: `\n` in UTF-16LE is `0A 00`.
    if bytes.starts_with(b"\xFF\xFE") || bytes.starts_with(b"\xFE\xFF") {
        return None;
    }
    // A compressed file is not text, and its trailer can end in a zero: gzip's length.
    const COMPRESSED: [&[u8]; 4] = [b"\x1F\x8B", b"\x28\xB5\x2F\xFD", b"BZh", b"\xFD7zXZ\x00"];
    if COMPRESSED.iter().any(|magic| bytes.starts_with(magic)) {
        return None;
    }
    let end = bytes.iter().rposition(|&b| b != 0).map_or(0, |at| at + 1);
    if end >= 2 && bytes[end - 2] == 0 {
        return None;
    }
    Some(end)
}

/// The text of the file `file`, `len` bytes long, mapped and cut before its trailing
/// NULs; `None` when it has none, so the file is read as it is.
fn mapped(file: &File, len: u64) -> std::io::Result<Option<(memmap2::Mmap, usize)>> {
    if len == 0 || !ends_in_nul(file)? {
        return Ok(None);
    }
    // SAFETY: read-only, as Polars maps a file it scans; a file changed under the map
    // is read as it stands, as it would be by Polars.
    let map = unsafe { memmap2::Mmap::map(file)? };
    Ok(text_end(&map).map(|end| (map, end)))
}

/// Whether the last byte of `file` is NUL: the one read a file without a NUL tail costs.
fn ends_in_nul(mut file: &File) -> std::io::Result<bool> {
    let mut last = [1u8];
    file.seek(SeekFrom::End(-1))?;
    file.read_exact(&mut last)?;
    file.rewind()?;
    Ok(last[0] == 0)
}

/// How many bytes of the file at `file` are text, when it ends in NULs.
pub fn text_len(file: &File) -> std::io::Result<Option<u64>> {
    let len = file.metadata()?.len();
    Ok(mapped(file, len)?.map(|(_, end)| end as u64))
}

/// Whether the file at `path` holds no text: empty, or nothing but NULs. One byte read
/// for a file that ends in anything else.
pub fn holds_nothing(path: &Path) -> bool {
    let Ok(file) = File::open(path) else {
        return false;
    };
    match file.metadata() {
        Ok(m) if m.len() == 0 => true,
        Ok(_) => text_len(&file).is_ok_and(|len| len == Some(0)),
        Err(_) => false,
    }
}

/// The text of the file at `path` as a buffer Polars scans in place, when the file
/// ends in NULs; `None` when it does not, and the file is scanned by its path.
pub fn text_buffer(path: &Path) -> std::io::Result<Option<polars_buffer::Buffer<u8>>> {
    let file = File::open(path)?;
    let len = file.metadata()?.len();
    Ok(mapped(&file, len)?.map(|(map, end)| polars_buffer::Buffer::from_owner(map).sliced(..end)))
}

/// `bytes`, decompressed into memory, without a trailing run of NULs.
pub fn trim(bytes: &mut Vec<u8>) {
    if let Some(end) = text_end(bytes) {
        bytes.truncate(end);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_trailing_run_of_nuls_is_cut() {
        assert_eq!(text_end(b"a,b\n1,2\n\0\0\0\0"), Some(8));
        assert_eq!(text_end(&[0u8; 4096]), Some(0), "all NULs: no text");
        assert_eq!(text_end(b"a\n1\0"), Some(3));
    }

    /// A normal file is read as it is: one byte looked at, and no cut.
    #[test]
    fn a_file_without_a_nul_tail_is_left_alone() {
        assert_eq!(text_end(b"a,b\n1,2\n"), None);
        assert_eq!(text_end(b""), None);
        assert_eq!(
            text_end(b"a,\0,b\n1,2"),
            None,
            "an interior NUL is the text's"
        );
        assert_eq!(
            text_end(b"\x1F\x8B\x08\x00rest\x46\x00\x00\x00"),
            None,
            "gzip's length ends in zeros"
        );
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("plain.csv");
        std::fs::write(&path, "a,b\n1,2\n").unwrap();
        assert!(text_buffer(&path).unwrap().is_none());
        let empty = dir.path().join("empty.csv");
        std::fs::write(&empty, "").unwrap();
        assert!(text_buffer(&empty).unwrap().is_none());
    }

    #[test]
    fn utf16_text_keeps_its_nuls() {
        let le: Vec<u8> = "a,b\n1,2\n"
            .encode_utf16()
            .flat_map(u16::to_le_bytes)
            .collect();
        assert_eq!(le.last(), Some(&0));
        assert_eq!(text_end(&le), None, "no byte-order mark");
        let mut bom = b"\xFF\xFE".to_vec();
        bom.extend_from_slice(&le);
        bom.extend_from_slice(&[0, 0, 0, 0]);
        assert_eq!(text_end(&bom), None, "a byte-order mark");
        let be: Vec<u8> = "a,b\n".encode_utf16().flat_map(u16::to_be_bytes).collect();
        assert_eq!(text_end(&be), None);
    }

    #[test]
    fn a_padded_file_is_scanned_up_to_its_text() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("padded.csv");
        let mut bytes = b"a,b\n1,2\n".to_vec();
        bytes.resize(4096 + 8, 0);
        std::fs::write(&path, &bytes).unwrap();
        let buffer = text_buffer(&path).unwrap().expect("a NUL tail");
        assert_eq!(buffer.as_slice(), b"a,b\n1,2\n");
        let file = File::open(&path).unwrap();
        assert_eq!(text_len(&file).unwrap(), Some(8));
        let mut owned = bytes.clone();
        trim(&mut owned);
        assert_eq!(owned, b"a,b\n1,2\n");
    }
}
