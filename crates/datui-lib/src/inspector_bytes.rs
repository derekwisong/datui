//! What a binary value holds, told from its first bytes, and the text in it
//! where that is cheap to get: UTF-8 stored as bytes, or gzip and zstd of text.
//! Nothing here runs or renders what it finds; decompression stops at
//! [`DECODE_MAX`] bytes of output, whatever the input claims.

use std::io::Read;

/// The most text decoded from a value: a decompression bomb stops here.
pub const DECODE_MAX: usize = 64 * 1024;

/// A kind of content, named from its magic bytes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Sniffed {
    Png { width: u32, height: u32 },
    Jpeg { width: u32, height: u32 },
    Gif { width: u32, height: u32 },
    Pdf,
    Gzip,
    Zstd,
    Zip,
    Parquet,
    Arrow,
    Utf8,
}

impl Sniffed {
    /// As the rule names it: `PNG 4x4`.
    pub fn label(self) -> String {
        match self {
            Sniffed::Png { width, height } => format!("PNG {width}x{height}"),
            Sniffed::Jpeg { width, height } if width > 0 => format!("JPEG {width}x{height}"),
            Sniffed::Jpeg { .. } => "JPEG".to_string(),
            Sniffed::Gif { width, height } => format!("GIF {width}x{height}"),
            Sniffed::Pdf => "PDF".to_string(),
            Sniffed::Gzip => "gzip".to_string(),
            Sniffed::Zstd => "zstd".to_string(),
            Sniffed::Zip => "zip".to_string(),
            Sniffed::Parquet => "Parquet".to_string(),
            Sniffed::Arrow => "Arrow".to_string(),
            Sniffed::Utf8 => "UTF-8 text".to_string(),
        }
    }

    /// The extension a file of it takes, so the program it opens in knows it.
    pub fn extension(self) -> &'static str {
        match self {
            Sniffed::Png { .. } => "png",
            Sniffed::Jpeg { .. } => "jpg",
            Sniffed::Gif { .. } => "gif",
            Sniffed::Pdf => "pdf",
            Sniffed::Gzip => "gz",
            Sniffed::Zstd => "zst",
            Sniffed::Zip => "zip",
            Sniffed::Parquet => "parquet",
            Sniffed::Arrow => "arrow",
            Sniffed::Utf8 => "txt",
        }
    }

    /// Shown by the system's viewer rather than a text editor.
    pub fn is_document(self) -> bool {
        matches!(
            self,
            Sniffed::Png { .. } | Sniffed::Jpeg { .. } | Sniffed::Gif { .. } | Sniffed::Pdf
        )
    }
}

fn be32(b: &[u8], at: usize) -> Option<u32> {
    Some(u32::from_be_bytes(b.get(at..at + 4)?.try_into().ok()?))
}

fn le16(b: &[u8], at: usize) -> Option<u32> {
    Some(u16::from_le_bytes(b.get(at..at + 2)?.try_into().ok()?) as u32)
}

fn be16(b: &[u8], at: usize) -> Option<u32> {
    Some(u16::from_be_bytes(b.get(at..at + 2)?.try_into().ok()?) as u32)
}

/// A JPEG's size, from its first frame header within the first 64 KB.
fn jpeg_size(b: &[u8]) -> Option<(u32, u32)> {
    let mut i = 2;
    let end = b.len().min(64 * 1024);
    while i + 9 < end {
        if b[i] != 0xff {
            i += 1;
            continue;
        }
        let marker = b[i + 1];
        let len = be16(b, i + 2)? as usize;
        if matches!(marker, 0xc0..=0xcf) && !matches!(marker, 0xc4 | 0xc8 | 0xcc) {
            return Some((be16(b, i + 7)?, be16(b, i + 5)?));
        }
        i += 2 + len;
    }
    None
}

/// What `bytes` hold, from the bytes alone. UTF-8 is text that decodes whole
/// and has no NUL in it.
pub fn sniff(b: &[u8]) -> Option<Sniffed> {
    if b.starts_with(b"\x89PNG\r\n\x1a\n") {
        return Some(Sniffed::Png {
            width: be32(b, 16).unwrap_or(0),
            height: be32(b, 20).unwrap_or(0),
        });
    }
    if b.starts_with(&[0xff, 0xd8, 0xff]) {
        let (width, height) = jpeg_size(b).unwrap_or((0, 0));
        return Some(Sniffed::Jpeg { width, height });
    }
    if b.starts_with(b"GIF87a") || b.starts_with(b"GIF89a") {
        return Some(Sniffed::Gif {
            width: le16(b, 6).unwrap_or(0),
            height: le16(b, 8).unwrap_or(0),
        });
    }
    if b.starts_with(b"%PDF-") {
        return Some(Sniffed::Pdf);
    }
    if b.starts_with(&[0x1f, 0x8b]) {
        return Some(Sniffed::Gzip);
    }
    if b.starts_with(&[0x28, 0xb5, 0x2f, 0xfd]) {
        return Some(Sniffed::Zstd);
    }
    if b.starts_with(b"PK\x03\x04") || b.starts_with(b"PK\x05\x06") {
        return Some(Sniffed::Zip);
    }
    if b.len() >= 8 && b.starts_with(b"PAR1") && b.ends_with(b"PAR1") {
        return Some(Sniffed::Parquet);
    }
    if b.starts_with(b"ARROW1") {
        return Some(Sniffed::Arrow);
    }
    if !b.is_empty() && !b.contains(&0) && std::str::from_utf8(b).is_ok() {
        return Some(Sniffed::Utf8);
    }
    None
}

/// Text decoded from bytes: what it came from, and whether it was cut at
/// [`DECODE_MAX`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Decoded {
    pub text: String,
    pub from: &'static str,
    pub cut: bool,
}

/// The longest whole-UTF-8 prefix of `out`, or None when it is not text.
fn text_of(mut out: Vec<u8>, cut: bool) -> Option<String> {
    match String::from_utf8(out.clone()) {
        Ok(text) if !text.contains('\0') => Some(text),
        Ok(_) => None,
        // Cut inside a character: keep what is whole.
        Err(e) if cut && e.utf8_error().error_len().is_none() => {
            out.truncate(e.utf8_error().valid_up_to());
            String::from_utf8(out).ok().filter(|t| !t.contains('\0'))
        }
        Err(_) => None,
    }
}

/// The text in `bytes`: themselves when they are UTF-8, or what gzip or zstd
/// decompress them to, up to [`DECODE_MAX`] bytes. None when there is no text.
pub fn decode_text(bytes: &[u8], kind: Option<Sniffed>) -> Option<Decoded> {
    let (reader, from): (Box<dyn Read + '_>, &'static str) = match kind? {
        Sniffed::Utf8 => {
            let cut = bytes.len() > DECODE_MAX;
            let head = &bytes[..bytes.len().min(DECODE_MAX)];
            return text_of(head.to_vec(), cut).map(|text| Decoded {
                text,
                from: "UTF-8",
                cut,
            });
        }
        Sniffed::Gzip => (Box::new(flate2::read::GzDecoder::new(bytes)), "gzip"),
        Sniffed::Zstd => (Box::new(zstd::Decoder::new(bytes).ok()?), "zstd"),
        _ => return None,
    };
    let mut out = Vec::new();
    reader
        .take(DECODE_MAX as u64 + 1)
        .read_to_end(&mut out)
        .ok()?;
    let cut = out.len() > DECODE_MAX;
    out.truncate(DECODE_MAX);
    text_of(out, cut).map(|text| Decoded { text, from, cut })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn png(width: u32, height: u32) -> Vec<u8> {
        let mut b = b"\x89PNG\r\n\x1a\n\0\0\0\x0dIHDR".to_vec();
        b.extend(width.to_be_bytes());
        b.extend(height.to_be_bytes());
        b.extend([8, 2, 0, 0, 0]);
        b
    }

    fn gzip(text: &str) -> Vec<u8> {
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(text.as_bytes()).unwrap();
        e.finish().unwrap()
    }

    /// The review's samples, by their first bytes.
    #[test]
    fn sniffs_the_common_kinds_from_their_magic() {
        let jpeg = [
            0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x00,
            0x20, 0x00, 0x40, 0x03, 0, 0, 0, 0, 0, 0,
        ];
        let zstd_bytes = zstd::encode_all(&b"hello zstd"[..], 3).unwrap();
        let cases: Vec<(Vec<u8>, Option<&str>)> = vec![
            (png(4, 4), Some("PNG 4x4")),
            (jpeg.to_vec(), Some("JPEG 64x32")),
            (b"GIF89a\x10\x00\x08\x00".to_vec(), Some("GIF 16x8")),
            (b"%PDF-1.7\n".to_vec(), Some("PDF")),
            (gzip("hello"), Some("gzip")),
            (zstd_bytes, Some("zstd")),
            (b"PK\x03\x04rest".to_vec(), Some("zip")),
            (b"PAR1xxxxPAR1".to_vec(), Some("Parquet")),
            (b"ARROW1\0\0".to_vec(), Some("Arrow")),
            ("Grüße, world".as_bytes().to_vec(), Some("UTF-8 text")),
            (vec![0xe9, 0x91, 0x23, 0xbe, 0x00], None),
            (Vec::new(), None),
            (b"a\0b".to_vec(), None),
        ];
        for (bytes, want) in cases {
            assert_eq!(
                sniff(&bytes).map(Sniffed::label).as_deref(),
                want,
                "{bytes:02x?}"
            );
        }
    }

    #[test]
    fn decodes_utf8_and_compressed_text_within_the_cap() {
        let d = decode_text("ünï\nline".as_bytes(), Some(Sniffed::Utf8)).unwrap();
        assert_eq!(
            (d.text.as_str(), d.from, d.cut),
            ("ünï\nline", "UTF-8", false)
        );
        let d = decode_text(&gzip("hello gzip"), Some(Sniffed::Gzip)).unwrap();
        assert_eq!(d.text, "hello gzip");
        let z = zstd::encode_all(&b"hello zstd"[..], 3).unwrap();
        assert_eq!(
            decode_text(&z, Some(Sniffed::Zstd)).unwrap().text,
            "hello zstd"
        );
        // A bomb: a megabyte of zeros-free text compresses small and stops at the cap.
        let big = "a".repeat(4 << 20);
        let d = decode_text(&gzip(&big), Some(Sniffed::Gzip)).unwrap();
        assert!(d.cut);
        assert_eq!(d.text.len(), DECODE_MAX);
        // Compressed bytes that are not text offer no text.
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(&[0u8, 1, 2, 0xff]).unwrap();
        assert!(decode_text(&e.finish().unwrap(), Some(Sniffed::Gzip)).is_none());
        assert!(decode_text(&png(1, 1), sniff(&png(1, 1))).is_none());
    }
}
