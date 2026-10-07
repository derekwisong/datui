use crate::fixed_records::Bytes;
use crate::formats::{Opened, Spec};
use polars::prelude::*;
use std::sync::Arc;

fn open(spec: &str, bytes: Vec<u8>) -> Opened {
    let spec = Spec::parse(spec, None).unwrap_or_else(|e| panic!("{e}"));
    spec.open_rows(Arc::new(Bytes::Owned(bytes)), "f")
        .unwrap_or_else(|e| panic!("{e}"))
}

fn all(opened: &Opened) -> DataFrame {
    opened
        .records
        .clone()
        .into_lazy()
        .unwrap()
        .collect()
        .unwrap()
}

fn cell(df: &DataFrame, column: &str, row: usize) -> String {
    match df.column(column).unwrap().get(row).unwrap() {
        AnyValue::String(s) => s.to_string(),
        AnyValue::StringOwned(s) => s.to_string(),
        AnyValue::Categorical(..) | AnyValue::CategoricalOwned(..) => df
            .column(column)
            .unwrap()
            .cast(&DataType::String)
            .unwrap()
            .get(row)
            .unwrap()
            .to_string()
            .trim_matches('"')
            .to_string(),
        v => v.to_string(),
    }
}

/// Every window of `opened` is the same rows as the whole read. A window walks
/// the records; the whole read is decoded column by column, from the row table
/// when there is one.
fn windows_agree(opened: &Opened) {
    let whole = all(opened);
    let rows = opened.records.rows();
    assert_eq!(whole.height(), rows);
    let walked = opened.records.window(0, rows).unwrap().collect().unwrap();
    assert!(walked.equals_missing(&whole), "{walked}\n{whole}");
    for start in [
        0,
        1,
        rows / 2,
        rows.saturating_sub(3),
        1023,
        1024,
        1025,
        2049,
    ] {
        if start >= rows {
            continue;
        }
        let window = opened.records.window(start, 3).unwrap().collect().unwrap();
        let expected = whole.slice(start as i64, 3);
        assert!(
            window.equals_missing(&expected),
            "window at {start}:\n{window}\n{expected}"
        );
    }
}

const ITCH: &str = r#"
name = "t.itch"
endian = "be"
[records]
framing = "length_prefixed"
size = "len"
size_adjust = 2
fields = [{ name = "len", type = "u2" }, { name = "kind", type = "str", size = 1 }]
type = "kind"

[[variants]]
name = "add"
when = "A"
fields = [{ name = "ref", type = "u8" }, { name = "shares", type = "u4" }, { name = "stock", type = "str", size = 8 }, { name = "price", type = "u4", scale = 4 }]

[[variants]]
name = "exec"
when = ["E", "C"]
fields = [{ name = "ref", type = "u8" }, { name = "shares", type = "u4" }]
"#;

fn itch_message(kind: u8, body: &[u8]) -> Vec<u8> {
    let mut out = ((body.len() + 1) as u16).to_be_bytes().to_vec();
    out.push(kind);
    out.extend(body);
    out
}

fn add(r: u64, shares: u32, stock: &str, price: u32) -> Vec<u8> {
    let mut body = r.to_be_bytes().to_vec();
    body.extend(shares.to_be_bytes());
    let mut s = stock.as_bytes().to_vec();
    s.resize(8, b' ');
    body.extend(s);
    body.extend(price.to_be_bytes());
    itch_message(b'A', &body)
}

fn exec(r: u64, shares: u32) -> Vec<u8> {
    let mut body = r.to_be_bytes().to_vec();
    body.extend(shares.to_be_bytes());
    itch_message(b'E', &body)
}

#[test]
fn length_prefixed_variants_make_one_table_with_a_type_column() {
    let mut bytes = Vec::new();
    for i in 0..3000u64 {
        if i % 3 == 0 {
            bytes.extend(add(i, 100, "AAPL", 1_234_500));
        } else {
            bytes.extend(exec(i, 7));
        }
    }
    // A type no variant names still has a length: its row shows the type.
    bytes.extend(itch_message(b'Z', &[1, 2, 3]));
    let opened = open(ITCH, bytes);
    assert!(opened.notes.is_empty(), "{:?}", opened.notes);
    let df = all(&opened);
    assert_eq!(df.height(), 3001);
    assert_eq!(
        df.get_column_names(),
        ["len", "kind", "type", "ref", "shares", "stock", "price"]
    );
    assert_eq!(cell(&df, "type", 0), "add");
    assert_eq!(cell(&df, "type", 1), "exec");
    assert_eq!(cell(&df, "stock", 0), "AAPL");
    assert_eq!(cell(&df, "price", 0), "123.4500");
    assert_eq!(cell(&df, "stock", 1), "null");
    assert_eq!(cell(&df, "shares", 1), "7");
    assert_eq!(cell(&df, "type", 3000), "?Z");
    assert_eq!(cell(&df, "ref", 3000), "null");
    windows_agree(&opened);
    // A projection decodes the one column.
    let one = opened
        .records
        .clone()
        .into_lazy()
        .unwrap()
        .select([col("ref")])
        .collect()
        .unwrap();
    assert_eq!(one.width(), 1);
    assert_eq!(cell(&one, "ref", 2999), "2999");
}

#[test]
fn one_variant_reads_alone() {
    let mut bytes = Vec::new();
    for i in 0..2500u64 {
        if i % 5 == 0 {
            bytes.extend(add(i, 100, "MSFT", 1));
        } else {
            bytes.extend(exec(i, 7));
        }
    }
    let spec = Spec::parse(ITCH, None)
        .unwrap()
        .with_variant("add")
        .unwrap();
    let opened = spec.open_rows(Arc::new(Bytes::Owned(bytes)), "f").unwrap();
    let df = all(&opened);
    assert_eq!(df.height(), 500);
    assert_eq!(
        df.get_column_names(),
        ["len", "kind", "ref", "shares", "stock", "price"]
    );
    assert_eq!(cell(&df, "ref", 499), "2495");
    windows_agree(&opened);
    assert!(
        Spec::parse(ITCH, None)
            .unwrap()
            .with_variant("nope")
            .is_err()
    );
}

#[test]
fn a_variant_sizes_its_record_and_an_unknown_type_stops_the_read() {
    let spec = r#"
name = "t.var"
[records]
framing = "variant"
type = { field = "kind", type = "u1" }
[[variants]]
name = "a"
when = 1
fields = [{ name = "x", type = "u2" }]
[[variants]]
name = "b"
when = 2
fields = [{ name = "y", type = "u4" }, { name = "z", type = "u1" }]
"#;
    let bytes = vec![1, 5, 0, 2, 9, 0, 0, 0, 3, 1, 6, 0, 7, 0xaa];
    let opened = open(spec, bytes);
    let df = all(&opened);
    assert_eq!(df.height(), 3);
    assert_eq!(cell(&df, "x", 2), "6");
    assert_eq!(cell(&df, "y", 1), "9");
    assert_eq!(cell(&df, "z", 1), "3");
    assert!(opened.notes[0].contains("type 7"), "{:?}", opened.notes);
    windows_agree(&opened);
}

#[test]
fn sync_markers_find_frames_through_garbage() {
    let spec = r#"
name = "t.sync"
[records]
framing = "sync"
sync = "1ACFFC1D"
fields = [{ name = "seq", type = "u2" }, { name = "v", type = "u1" }]
"#;
    let mut bytes = vec![0xff, 0xee];
    for i in 0..3u16 {
        bytes.extend([0x1a, 0xcf, 0xfc, 0x1d]);
        bytes.extend(i.to_le_bytes());
        bytes.push(i as u8 * 10);
        if i == 1 {
            bytes.extend([1, 2, 3]);
        }
    }
    let opened = open(spec, bytes);
    let df = all(&opened);
    assert_eq!(df.height(), 3);
    assert_eq!(cell(&df, "v", 2), "20");
    windows_agree(&opened);
    assert!(
        opened.notes.iter().any(|n| n.starts_with("5 bytes")),
        "{:?}",
        opened.notes
    );
}

#[test]
fn checksums_bits_and_groups() {
    let spec = r#"
name = "t.ck"
[records]
framing = "length_prefixed"
size = "len"
fields = [
  { name = "len", type = "u1" },
  { name = "status", type = "u2", bits = [{ name = "valid", bit = 0 }, { name = "mode", bit = 4, width = 3, enum = { 0 = "IDLE", 1 = "RUN" } }] },
  { name = "n", type = "u1" },
  { name = "levels", group = { count = "n", fields = [{ name = "px", type = "s2" }, { name = "qty", type = "u1" }] } },
  { name = "crc", type = "u2" },
]
checksum = { algo = "crc16-ccitt", field = "crc", from = "status" }
"#;
    let record = |status: u16, levels: &[(i16, u8)], corrupt: bool| {
        let mut body = status.to_le_bytes().to_vec();
        body.push(levels.len() as u8);
        for (px, q) in levels {
            body.extend(px.to_le_bytes());
            body.push(*q);
        }
        let mut crc = crate::formats::ChecksumAlgo::Crc16Ccitt.compute(&body) as u16;
        if corrupt {
            crc ^= 1;
        }
        let mut out = vec![(1 + body.len() + 2) as u8];
        out.extend(body);
        out.extend(crc.to_le_bytes());
        out
    };
    let mut bytes = record(0x0011, &[(-5, 1), (7, 2)], false);
    bytes.extend(record(0x0000, &[], true));
    let opened = open(spec, bytes);
    let df = all(&opened);
    assert_eq!(df.height(), 2);
    assert_eq!(cell(&df, "valid", 0), "true");
    assert_eq!(cell(&df, "mode", 0), "RUN");
    assert_eq!(cell(&df, "mode", 1), "IDLE");
    assert_eq!(cell(&df, "checksum_ok", 0), "true");
    assert_eq!(cell(&df, "checksum_ok", 1), "false");
    let levels = df.column("levels").unwrap();
    assert!(
        matches!(levels.dtype(), DataType::List(inner) if matches!(**inner, DataType::Struct(_)))
    );
    let first = levels.get(0).unwrap().to_string();
    assert!(first.contains("-5") && first.contains('7'), "{first}");
    assert_eq!(levels.list().unwrap().lst_lengths().get(1), Some(0));
    windows_agree(&opened);
}

#[test]
fn fortran_records_have_their_length_on_both_ends() {
    let spec = r#"
name = "t.fortran"
[records]
framing = "length_prefixed"
size = "n"
size_adjust = 4
length_suffix = true
fields = [{ name = "n", type = "u4" }, { name = "v", type = "f8", count = "n_values" }]
"#;
    assert!(Spec::parse(spec, None).is_err());
    let spec = r#"
name = "t.fortran"
[records]
framing = "length_prefixed"
size = "n"
size_adjust = 4
length_suffix = true
fields = [{ name = "n", type = "u4" }, { name = "data", type = "bytes", size = "rest" }]
"#;
    let mut bytes = Vec::new();
    for payload in [&b"abc"[..], b"hello"] {
        bytes.extend((payload.len() as u32).to_le_bytes());
        bytes.extend(payload);
        bytes.extend((payload.len() as u32).to_le_bytes());
    }
    let opened = open(spec, bytes.clone());
    let df = all(&opened);
    assert_eq!(df.height(), 2);
    assert_eq!(
        df.column("data").unwrap().binary().unwrap().get(1),
        Some(&b"hello"[..])
    );
    // A suffix that disagrees stops the read there.
    let n = bytes.len();
    bytes[n - 1] = 9;
    let opened = open(spec, bytes);
    assert_eq!(opened.records.rows(), 1);
    assert!(
        opened.notes[0].contains("ends with length"),
        "{:?}",
        opened.notes
    );
}

#[test]
fn tagged_chunks_with_text_types_and_even_alignment() {
    let spec = r#"
name = "t.riff"
[records]
framing = "length_prefixed"
size = "size"
size_adjust = 8
align = 2
fields = [{ name = "id", type = "str", size = 4 }, { name = "size", type = "u4" }]
type = "id"
[[variants]]
name = "fmt"
when = "fmt "
fields = [{ name = "channels", type = "u2" }]
[[variants]]
name = "data"
when = "data"
fields = [{ name = "payload", type = "bytes", size = "rest" }]
"#;
    let mut bytes = b"fmt ".to_vec();
    bytes.extend(2u32.to_le_bytes());
    bytes.extend(2u16.to_le_bytes());
    bytes.extend(b"data");
    bytes.extend(3u32.to_le_bytes());
    bytes.extend([1, 2, 3, 0]);
    bytes.extend(b"LIST");
    bytes.extend(0u32.to_le_bytes());
    let opened = open(spec, bytes);
    let df = all(&opened);
    assert_eq!(df.height(), 3, "{:?}", opened.notes);
    assert_eq!(cell(&df, "channels", 0), "2");
    assert_eq!(
        df.column("payload").unwrap().binary().unwrap().get(1),
        Some(&[1u8, 2, 3][..])
    );
    assert_eq!(cell(&df, "type", 2), "?LIST");
    windows_agree(&opened);
}

#[test]
fn strz_heap_strings_varints_and_deltas() {
    let spec = r#"
name = "t.mixed"
[header]
fields = [{ name = "str_off", type = "u4" }, { name = "str_size", type = "u4" }, { name = "n", type = "u4" }]
[sections.strings]
offset = "header.str_off"
size = "header.str_size"
[records]
count = "header.n"
fields = [
  { name = "name", type = "strz" },
  { name = "label", type = "u2", string_at = "strings" },
  { name = "ts", type = "vu", delta = true, time = "ms" },
  { name = "dv", type = "vs" },
]
"#;
    let heap = b"alpha\0beta\0";
    let n = 2500u32;
    let mut records = Vec::new();
    for i in 0..n {
        records.extend(format!("r{i}\0").as_bytes());
        records.extend(if i % 2 == 0 { 0u16 } else { 6u16 }.to_le_bytes());
        // A delta of 1000 ms, as LEB128.
        records.extend([0xe8, 0x07]);
        // Zigzag -3 is 5.
        records.push(5);
    }
    let off = 12 + records.len() as u32;
    let mut bytes = off.to_le_bytes().to_vec();
    bytes.extend((heap.len() as u32).to_le_bytes());
    bytes.extend(n.to_le_bytes());
    bytes.extend(&records);
    bytes.extend(heap);
    let opened = open(spec, bytes);
    let df = all(&opened);
    assert_eq!(df.height(), 2500);
    assert_eq!(cell(&df, "name", 7), "r7");
    assert_eq!(cell(&df, "label", 0), "alpha");
    assert_eq!(cell(&df, "label", 1), "beta");
    assert_eq!(cell(&df, "dv", 0), "-3");
    assert_eq!(
        df.column("ts").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Milliseconds, None)
    );
    let ts = df.column("ts").unwrap().cast(&DataType::Int64).unwrap();
    assert_eq!(ts.get(2048).unwrap(), AnyValue::Int64(2_049_000));
    windows_agree(&opened);
}

#[test]
fn the_byte_order_comes_from_the_magic() {
    let spec = r#"
name = "t.auto"
endian = "auto"
match = { magic = [0xd4, 0xc3, 0xb2, 0xa1] }
[header]
fields = [{ type = "pad", size = 4 }]
[records]
fields = [{ name = "v", type = "u4" }]
"#;
    let le = [vec![0xd4, 0xc3, 0xb2, 0xa1], 7u32.to_le_bytes().to_vec()].concat();
    let be = [vec![0xa1, 0xb2, 0xc3, 0xd4], 7u32.to_be_bytes().to_vec()].concat();
    let parsed = Spec::parse(spec, None).unwrap();
    assert!(parsed.magic_matches(&be));
    for bytes in [le, be] {
        let df = all(&open(spec, bytes));
        assert_eq!(cell(&df, "v", 0), "7");
    }
}

#[test]
fn a_footer_counts_the_records_and_checks_the_file() {
    let spec = r#"
name = "t.foot"
[records]
count = "footer.n"
fields = [{ name = "v", type = "u2" }]
[footer]
fields = [{ name = "n", type = "u4" }, { name = "crc", type = "u4" }]
checksum = { algo = "crc32", field = "crc" }
"#;
    let mut bytes: Vec<u8> = (0..5u16).flat_map(|v| v.to_le_bytes()).collect();
    let crc = crate::formats::ChecksumAlgo::Crc32.compute(&bytes) as u32;
    bytes.extend(4u32.to_le_bytes());
    let mut good = bytes.clone();
    good.extend(crc.to_le_bytes());
    let opened = open(spec, good);
    assert_eq!(opened.records.rows(), 4);
    // A checksum that matches says nothing.
    assert!(
        !opened.notes.iter().any(|n| n.contains("checksum")),
        "{:?}",
        opened.notes
    );
    bytes.extend((crc ^ 1).to_le_bytes());
    let opened = open(spec, bytes);
    assert!(
        opened.notes.iter().any(|n| n.contains("the file's is")),
        "{:?}",
        opened.notes
    );
}

fn compress(codec: &str, raw: &[u8]) -> Vec<u8> {
    use std::io::Write;
    match codec {
        "zstd" => zstd::encode_all(raw, 3).unwrap(),
        "gzip" => {
            let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(raw).unwrap();
            e.finish().unwrap()
        }
        "zlib" => {
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(raw).unwrap();
            e.finish().unwrap()
        }
        "deflate" => {
            let mut e = flate2::write::DeflateEncoder::new(Vec::new(), flate2::Compression::fast());
            e.write_all(raw).unwrap();
            e.finish().unwrap()
        }
        "lz4" => {
            let mut e = lz4::EncoderBuilder::new().build(Vec::new()).unwrap();
            e.write_all(raw).unwrap();
            let (out, r) = e.finish();
            r.unwrap();
            out
        }
        "lz4_block" => lz4::block::compress(raw, None, false).unwrap(),
        "snappy" => snap::raw::Encoder::new().compress_vec(raw).unwrap(),
        "snappy_framed" => {
            let mut e = snap::write::FrameEncoder::new(Vec::new());
            e.write_all(raw).unwrap();
            e.into_inner().unwrap()
        }
        "brotli" => {
            let mut out = Vec::new();
            {
                let mut e = brotli::CompressorWriter::new(&mut out, 4096, 5, 22);
                e.write_all(raw).unwrap();
            }
            out
        }
        "bzip2" => {
            let mut e = bzip2::write::BzEncoder::new(Vec::new(), bzip2::Compression::fast());
            e.write_all(raw).unwrap();
            e.finish().unwrap()
        }
        "xz" => {
            let mut e = xz2::write::XzEncoder::new(Vec::new(), 1);
            e.write_all(raw).unwrap();
            e.finish().unwrap()
        }
        _ => raw.to_vec(),
    }
}

#[test]
fn a_running_sum_in_one_plain_block_reads_the_same_in_any_window() {
    let spec = r#"
name = "t.one"
[blocks]
header = [{ name = "clen", type = "u4" }]
size = "clen"
[records]
fields = [{ name = "v", type = "u4", delta = "block" }]
"#;
    let mut bytes = 40u32.to_le_bytes().to_vec();
    bytes.extend((0..10u32).flat_map(|_| 1u32.to_le_bytes()));
    let opened = open(spec, bytes);
    let df = all(&opened);
    assert_eq!(cell(&df, "v", 9), "10");
    windows_agree(&opened);
}

#[test]
fn every_codec_reads_a_block() {
    for codec in [
        "none",
        "gzip",
        "deflate",
        "zlib",
        "zstd",
        "lz4",
        "lz4_block",
        "snappy",
        "snappy_framed",
        "brotli",
        "bzip2",
        "xz",
    ] {
        let spec = format!(
            r#"
name = "t.blocks"
[blocks]
header = [{{ name = "clen", type = "u4" }}, {{ name = "rawlen", type = "u4" }}]
size = "clen"
compression = "{codec}"
uncompressed = "rawlen"
[records]
fields = [{{ name = "v", type = "u4", delta = "block" }}]
"#
        );
        let mut bytes = Vec::new();
        for block in 0..3u32 {
            let raw: Vec<u8> = (0..1500u32)
                .flat_map(|_| (block + 1).to_le_bytes())
                .collect();
            let packed = compress(codec, &raw);
            bytes.extend((packed.len() as u32).to_le_bytes());
            bytes.extend((raw.len() as u32).to_le_bytes());
            bytes.extend(packed);
        }
        let opened = open(&spec, bytes);
        assert!(opened.notes.is_empty(), "{codec}: {:?}", opened.notes);
        let df = all(&opened);
        assert_eq!(df.height(), 4500, "{codec}");
        // The running sum starts again in each block.
        assert_eq!(cell(&df, "v", 1499), "1500", "{codec}");
        assert_eq!(cell(&df, "v", 1500), "2", "{codec}");
        assert_eq!(cell(&df, "v", 4499), "4500", "{codec}");
        windows_agree(&opened);
    }
}

#[test]
fn a_block_index_in_the_footer_and_a_codec_per_block() {
    let spec = r#"
name = "t.indexed"
[blocks]
header = [{ name = "codec", type = "u1" }, { name = "clen", type = "u4" }]
size = "clen"
compression = { field = "codec", values = { 0 = "none", 1 = "zstd" } }
index = { at = "footer.index_off", count = "footer.n_blocks", fields = [{ name = "offset", type = "u8" }, { name = "rows", type = "u4" }] }
[records]
fields = [{ name = "v", type = "u2" }]
[footer]
fields = [{ name = "index_off", type = "u8" }, { name = "n_blocks", type = "u4" }]
"#;
    let mut bytes = Vec::new();
    let mut index = Vec::new();
    for block in 0..4u16 {
        let raw: Vec<u8> = (0..100u16)
            .flat_map(|i| (block * 100 + i).to_le_bytes())
            .collect();
        let (codec, body) = if block % 2 == 0 {
            (0u8, raw.clone())
        } else {
            (1u8, zstd::encode_all(&raw[..], 1).unwrap())
        };
        index.extend((bytes.len() as u64).to_le_bytes());
        index.extend(100u32.to_le_bytes());
        bytes.push(codec);
        bytes.extend((body.len() as u32).to_le_bytes());
        bytes.extend(body);
    }
    let index_off = bytes.len() as u64;
    bytes.extend(index);
    bytes.extend(index_off.to_le_bytes());
    bytes.extend(4u32.to_le_bytes());
    let opened = open(spec, bytes);
    assert!(opened.notes.is_empty(), "{:?}", opened.notes);
    let df = all(&opened);
    assert_eq!(df.height(), 400);
    assert_eq!(cell(&df, "v", 399), "399");
    windows_agree(&opened);
}

#[test]
fn columns_at_offsets_in_one_file() {
    let spec = r#"
name = "t.cols"
layout = "columns"
[header]
fields = [{ name = "n", type = "u4" }, { name = "a_off", type = "u4" }, { name = "b_off", type = "u4" }]
[records]
count = "header.n"
fields = [{ name = "a", type = "u2", offset = "header.a_off" }, { name = "b", type = "f8", offset = "header.b_off" }]
"#;
    let mut bytes = Vec::new();
    bytes.extend(3u32.to_le_bytes());
    bytes.extend(12u32.to_le_bytes());
    bytes.extend(18u32.to_le_bytes());
    for v in [1u16, 2, 3] {
        bytes.extend(v.to_le_bytes());
    }
    for v in [0.5f64, 1.5, 2.5] {
        bytes.extend(v.to_le_bytes());
    }
    let df = all(&open(spec, bytes));
    assert_eq!(df.height(), 3);
    assert_eq!(cell(&df, "a", 2), "3");
    assert_eq!(cell(&df, "b", 1), "1.5");
}

#[test]
fn a_ring_buffer_starts_at_its_oldest_record() {
    let spec = r#"
name = "t.ring"
[header]
fields = [{ name = "head", type = "u4" }]
[records]
ring = "header.head"
fields = [{ name = "v", type = "u1" }]
"#;
    let mut bytes = 2u32.to_le_bytes().to_vec();
    bytes.extend([30, 40, 10, 20]);
    let df = all(&open(spec, bytes));
    let values: Vec<String> = (0..4).map(|i| cell(&df, "v", i)).collect();
    assert_eq!(values, ["10", "20", "30", "40"]);
}

#[test]
fn half_floats_and_text_encodings() {
    let spec = r#"
name = "t.enc"
[records]
fields = [
  { name = "h", type = "f2" }, { name = "b", type = "bf2" },
  { name = "latin", type = "str", size = 4, encoding = "latin1" },
  { name = "wide", type = "str", size = 6, encoding = "utf16le" },
]
"#;
    let mut bytes = half::f16::from_f32(1.5).to_bits().to_le_bytes().to_vec();
    bytes.extend(half::bf16::from_f32(-2.0).to_bits().to_le_bytes());
    bytes.extend([b'c', 0xe9, b' ', b' ']);
    bytes.extend([b'h', 0, b'i', 0, 0, 0]);
    let df = all(&open(spec, bytes));
    assert_eq!(cell(&df, "h", 0), "1.5");
    assert_eq!(cell(&df, "b", 0), "-2.0");
    assert_eq!(cell(&df, "latin", 0), "c\u{e9}");
    assert_eq!(cell(&df, "wide", 0), "hi");
}

/// A UDP packet in Ethernet and IPv4, carrying `payload`.
fn udp_frame(payload: &[u8]) -> Vec<u8> {
    let mut f = vec![0u8; 12];
    f.extend([0x08, 0x00]);
    let total = (20 + 8 + payload.len()) as u16;
    f.extend([0x45, 0]);
    f.extend(total.to_be_bytes());
    f.extend([0, 0, 0x40, 0, 64, 17, 0, 0]);
    f.extend([10, 0, 0, 1, 10, 0, 0, 2]);
    f.extend([0x30, 0x39, 0x30, 0x39]);
    f.extend(((8 + payload.len()) as u16).to_be_bytes());
    f.extend([0, 0]);
    f.extend(payload);
    f
}

#[test]
fn a_capture_s_udp_payloads_hold_the_records() {
    let spec = r#"
name = "t.mold"
endian = "be"
[capture]
header = [{ name = "session", type = "str", size = 10 }, { name = "seq", type = "u8" }, { name = "count", type = "u2" }]
count = "count"
time = "captured"
[records]
framing = "length_prefixed"
size = "len"
size_adjust = 2
fields = [{ name = "len", type = "u2" }, { name = "msg", type = "str", size = "rest" }]
"#;
    let mut pcap = vec![0xd4, 0xc3, 0xb2, 0xa1, 2, 0, 4, 0];
    pcap.extend([0u8; 8]);
    pcap.extend(65535u32.to_le_bytes());
    pcap.extend(1u32.to_le_bytes());
    for (i, msgs) in [vec!["hi", "there"], vec!["x"]].into_iter().enumerate() {
        let mut payload = b"SESSION001".to_vec();
        payload.extend((i as u64).to_be_bytes());
        payload.extend((msgs.len() as u16).to_be_bytes());
        for m in &msgs {
            payload.extend((m.len() as u16).to_be_bytes());
            payload.extend(m.as_bytes());
        }
        let frame = udp_frame(&payload);
        pcap.extend((1_700_000_000 + i as u32).to_le_bytes());
        pcap.extend(5u32.to_le_bytes());
        pcap.extend((frame.len() as u32).to_le_bytes());
        pcap.extend((frame.len() as u32).to_le_bytes());
        pcap.extend(frame);
    }
    let opened = open(spec, pcap);
    let df = all(&opened);
    assert_eq!(df.height(), 3, "{:?}", opened.notes);
    assert_eq!(cell(&df, "msg", 1), "there");
    assert_eq!(cell(&df, "msg", 2), "x");
    assert_eq!(cell(&df, "captured", 2), "2023-11-14 22:13:21.000005");
}

#[test]
fn a_variant_s_fields_past_its_record_s_end_are_null() {
    // The second add says it is 15 bytes: its stock and price are past its end.
    let mut short = add(2, 2, "B", 2);
    short[..2].copy_from_slice(&13u16.to_be_bytes());
    short.truncate(15);
    let bytes = [add(1, 1, "A", 1), short, exec(3, 3), add(4, 4, "D", 4)].concat();
    let opened = open(ITCH, bytes);
    let df = all(&opened);
    assert_eq!(df.height(), 4, "{:?}", opened.notes);
    assert_eq!(cell(&df, "shares", 1), "2");
    assert_eq!(cell(&df, "stock", 1), "null");
    assert_eq!(cell(&df, "stock", 3), "D");
    windows_agree(&opened);
}

/// A file's walk is kept: opened again with the same spec it is not walked again,
/// and with another variant it is walked and kept in its place.
#[test]
fn a_files_walk_is_kept_for_its_next_open() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("kept.itch");
    let bytes: Vec<u8> = (0..3000u64)
        .flat_map(|i| [add(i, i as u32, "S", 1), exec(i, 2)].concat())
        .collect();
    std::fs::write(&path, bytes).unwrap();
    let spec = Spec::parse(ITCH, None).unwrap();
    let first = spec.open(&path, "kept.itch").unwrap();
    let kept = crate::indexed::peek::<super::KeptWalk>(&path).expect("kept");
    assert_eq!(kept.rows, 6000);
    let again = spec.open(&path, "kept.itch").unwrap();
    let same = crate::indexed::peek::<super::KeptWalk>(&path).unwrap();
    assert!(Arc::ptr_eq(&kept, &same), "not walked again");
    assert!(all(&first).equals_missing(&all(&again)));
    windows_agree(&again);
    let exec_only = spec
        .with_variant("exec")
        .unwrap()
        .open(&path, "kept.itch")
        .unwrap();
    assert_eq!(exec_only.records.rows(), 3000);
    let replaced = crate::indexed::peek::<super::KeptWalk>(&path).unwrap();
    assert_eq!(replaced.rows, 3000);
    windows_agree(&exec_only);
}

#[test]
fn a_record_cut_short_is_left_out_and_said() {
    let opened = open(
        ITCH,
        [add(1, 1, "A", 1), add(2, 2, "B", 2)[..10].to_vec()].concat(),
    );
    assert_eq!(opened.records.rows(), 1);
    assert!(
        opened.notes[0].contains("not a whole record"),
        "{:?}",
        opened.notes
    );
}
