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
        0xff, 0xd8, 0xff, 0xe0, 0x00, 0x04, 0x00, 0x00, 0xff, 0xc0, 0x00, 0x11, 0x08, 0x00, 0x20,
        0x00, 0x40, 0x03, 0, 0, 0, 0, 0, 0,
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
