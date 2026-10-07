use super::*;
use polars::prelude::*;

const L2: &str = r#"
name = "acme.l2feed"
match = { glob = ["*.l2"], magic = "L2FD" }
endian = "le"

[header]
fields = [{ name = "magic", type = "str", size = 4 }, { name = "count", type = "u8" }]

[records]
count = "header.count"
fields = [
  { name = "ts",     type = "u8", time = "ns" },
  { name = "symbol", type = "str", size = 8 },
  { name = "side",   type = "u1", enum = { 1 = "BUY", 2 = "SELL" } },
  { name = "price",  type = "u4", scale = 4 },
]
"#;

fn l2_file(records: &[(u64, &str, u8, u32)], count: u64, trailing: &[u8]) -> Vec<u8> {
    let mut out = b"L2FD".to_vec();
    out.extend(count.to_le_bytes());
    for (ts, symbol, side, price) in records {
        out.extend(ts.to_le_bytes());
        let mut sym = symbol.as_bytes().to_vec();
        sym.resize(8, b' ');
        out.extend(sym);
        out.push(*side);
        out.extend(price.to_le_bytes());
    }
    out.extend(trailing);
    out
}

fn open(spec: &Spec, bytes: Vec<u8>) -> Opened {
    spec.open_rows(Arc::new(Bytes::Owned(bytes)), "f").unwrap()
}

/// Records past the most a table holds are not shown, and a note says so.
#[test]
fn records_past_the_table_limit_are_noted() {
    let records = FixedRecords::new(
        vec![Arc::new(Bytes::Owned(vec![0; 4]))],
        vec![ColumnLayout::new("a", 0, 1, Physical::Unsigned(1), 1)],
        usize::MAX,
    )
    .unwrap();
    let mut notes = Vec::new();
    past_limit(&mut notes, 4, &records);
    assert!(notes.is_empty());
    past_limit(&mut notes, 9, &records);
    assert_eq!(notes, ["last 5 records not shown: past the table limit"]);
}

fn collect(opened: &Opened) -> DataFrame {
    Arc::clone(&opened.records)
        .into_lazy()
        .unwrap()
        .collect()
        .unwrap()
}

fn cell(df: &DataFrame, column: &str, row: usize) -> String {
    df.column(column).unwrap().get(row).unwrap().to_string()
}

#[test]
fn the_issue_s_example_reads() {
    let spec = Spec::parse(L2, None).unwrap();
    assert_eq!(spec.name, "acme.l2feed");
    assert!(spec.glob_matches(Path::new("/x/day.l2")));
    let bytes = l2_file(&[(5, "AAPL", 1, 1_234_500), (6, "MSFT", 2, 7)], 2, &[]);
    assert!(spec.magic_matches(&bytes));
    let opened = open(&spec, bytes);
    assert!(opened.notes.is_empty(), "{:?}", opened.notes);
    let df = collect(&opened);
    assert_eq!(df.height(), 2);
    assert_eq!(cell(&df, "symbol", 0), "\"AAPL\"");
    assert_eq!(cell(&df, "side", 1), "\"SELL\"");
    assert_eq!(
        df.column("price").unwrap().dtype(),
        &DataType::Decimal(38, 4)
    );
    assert_eq!(cell(&df, "price", 0), "123.4500");
    assert_eq!(
        df.column("ts").unwrap().dtype(),
        &DataType::Datetime(TimeUnit::Nanoseconds, None)
    );
}

#[test]
fn errors_point_at_the_line_and_column() {
    let text = "name = \"a.b\"\n[records]\nfields = [{ name = \"x\", type = \"u9\" }]\n";
    let e = Spec::parse(text, Some(Path::new("a.toml"))).unwrap_err();
    assert_eq!((e.line, e.column), (3, 32), "{e}");
    assert!(
        e.to_string()
            .starts_with("\"a.toml\":3:32: type: expected u1"),
        "{e}"
    );
    let e = Spec::parse(
        "name = \"a.b\"\n[records]\nfields = [{ name = \"x\", type = \"u4\", colour = 1 }]",
        None,
    )
    .unwrap_err();
    assert!(e.message.contains("unknown key `colour`"), "{e}");
    assert_eq!(e.line, 3);
    let e = Spec::parse("name = \"a.b\"\n[records\n", None).unwrap_err();
    assert_eq!(e.line, 2, "{e}");
}

#[test]
fn bad_specs_are_refused_by_name() {
    let record = |fields: &str| format!("name = \"a.b\"\n[records]\nfields = [{fields}]");
    for (text, said) in [
            ("name = \"a.b\"\n[records]\nframing = \"length_prefixed\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "size names the field"),
            ("name = \"a.b\"\n[records]\nframing = \"sync\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "needs sync"),
            ("name = \"a.b\"\n[blocks]\nheader = [{ name = \"n\", type = \"u4\" }]\nsize = \"n\"\ncompression = \"lzma9\"\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "expected one of none"),
            ("name = \"a.b\"\n[footer]\nfields = [{ name = \"n\", type = \"u4\" }]\n[records]\ncount = \"footer.m\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "no footer field named `m`"),
            ("name = \"a.b\"\n[records]\ntype = \"k\"\nfields = [{ name = \"k\", type = \"u1\" }]\n[[variants]]\nname = \"a\"\nwhen = \"x\"\nfields = []".to_string(), "`k` is a u1"),
            ("name = \"a.b\"\n[records]\ntype = \"k\"\nfields = [{ name = \"k\", type = \"u1\" }]\n[[variants]]\nname = \"a\"\nwhen = []\nfields = []".to_string(), "empty list picks no record"),
            ("name = \"a.b\"\n[records]\nframing = \"sync\"\nsync = \"\u{1bb}a\"\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "expected hex"),
            ("name = \"ab\"\n[records]\nfields = [{ name = \"x\", type = \"u1\" }]".to_string(), "namespaced"),
            ("name = \"a.b\"\n[records]\nsize = 2\nfields = [{ name = \"x\", type = \"u4\" }]".to_string(), "more than 2"),
            (record("{ name = \"x\", type = \"f4\", scale = 2 }"), "integer types"),
            (record("{ name = \"x\", type = \"u4\", scale = 2, enum = { 1 = \"a\" } }"), "one of time"),
            (record("{ name = \"x\", type = \"f4\", null = \"min\" }"), "does not fit"),
            (record("{ name = \"x\", type = \"u4\", flatten = true }"), "flatten"),
            (record("{ name = \"x\", type = \"u4\", of_day = true }"), "of_day goes with time"),
            (record("{ name = \"x\", type = \"u4\", date = \"ddmmyy\" }"), "yyyymmdd"),
            (record("{ name = \"x\", type = \"u4\", time = \"ns\", of_day = true, date = \"trade\" }"), "header field"),
            (record("{ type = \"pad\", size = 2, name = \"p\" }"), "only a size"),
            (record("{ name = \"x\", type = \"u1\", count = 67108864, flatten = true }"), "at most 1024"),
            (record("{ name = \"x\", type = \"u4\", null = -1 }"), "holds 0 to 4294967295"),
            (record("{ name = \"x\", type = \"s1\", null = 128 }"), "holds -128 to 127"),
            ("name = \"a.b\"\nlayout = \"columns\"\n[records]\nfields = [{ name = \"x\", type = \"u1\", file = \"../x\" }]".to_string(), "name of a file"),
            ("name = \"a.b\"\nlayout = \"columns\"\n[records]\nfields = [{ name = \"/etc/x\", type = \"u1\" }]".to_string(), "cannot hold a path"),
        ] {
            let e = Spec::parse(&text, None).unwrap_err();
            assert!(e.message.contains(said), "{text}: {e}");
        }
}

/// Every type and meaning, in both byte orders, from one record.
#[test]
fn every_field_type_in_both_byte_orders() {
    for (endian, big) in [("le", false), ("be", true)] {
        let text = format!(
            r#"name = "t.all"
endian = "{endian}"
[records]
fields = [
  {{ name = "u1", type = "u1" }}, {{ name = "u2", type = "u2" }}, {{ name = "u3", type = "u3" }},
  {{ name = "u4", type = "u4" }}, {{ name = "u5", type = "u5" }}, {{ name = "u8", type = "u8" }},
  {{ name = "s1", type = "s1" }}, {{ name = "s2", type = "s2" }}, {{ name = "s3", type = "s3" }},
  {{ name = "s4", type = "s4" }}, {{ name = "s6", type = "s6" }}, {{ name = "s8", type = "s8" }},
  {{ name = "f4", type = "f4" }}, {{ name = "f8", type = "f8" }},
  {{ name = "flag", type = "bool" }},
  {{ name = "s", type = "str", size = 3 }}, {{ name = "b", type = "bytes", size = 2 }},
  {{ type = "pad", size = 1 }},
  {{ name = "t", type = "s4", time = "s", epoch = 2000-01-01 }},
  {{ name = "day", type = "u2", time = "days", epoch = "2000-01-01" }},
  {{ name = "ymd", type = "u4", date = "yyyymmdd" }},
  {{ name = "tod", type = "u4", time = "ms", of_day = true }},
  {{ name = "serial", type = "f8", time = "days", epoch = "1899-12-30" }},
  {{ name = "d", type = "s2", scale = 2 }},
  {{ name = "c", type = "s2", factor = 0.5, offset = -40.0 }},
  {{ name = "e", type = "u1", enum = {{ 7 = "seven" }} }},
  {{ name = "n", type = "s4", null = "min" }},
  {{ name = "le", type = "u2le" }}, {{ name = "be", type = "u2be" }},
  {{ name = "arr", type = "u1", count = 3 }},
  {{ name = "lv", type = "u1", count = 2, flatten = true }},
]"#
        );
        let spec = Spec::parse(&text, None).unwrap();
        let mut bytes = Vec::new();
        let put = |bytes: &mut Vec<u8>, le: &[u8]| {
            if big {
                bytes.extend(le.iter().rev());
            } else {
                bytes.extend(le);
            }
        };
        bytes.push(200);
        put(&mut bytes, &60_000u16.to_le_bytes());
        put(&mut bytes, &16_000_000u32.to_le_bytes()[..3]);
        put(&mut bytes, &4_000_000_000u32.to_le_bytes());
        put(&mut bytes, &1_099_511_627_775u64.to_le_bytes()[..5]);
        put(&mut bytes, &u64::MAX.to_le_bytes());
        bytes.push(-5i8 as u8);
        put(&mut bytes, &(-300i16).to_le_bytes());
        put(&mut bytes, &(-70_000i32).to_le_bytes()[..3]);
        put(&mut bytes, &(-70_000i32).to_le_bytes());
        put(&mut bytes, &(-1i64).to_le_bytes()[..6]);
        put(&mut bytes, &i64::MIN.to_le_bytes());
        put(&mut bytes, &1.5f32.to_le_bytes());
        put(&mut bytes, &(-2.25f64).to_le_bytes());
        bytes.push(9);
        bytes.extend(b"hi\0");
        bytes.extend([0xde, 0xad]);
        bytes.push(0xff);
        put(&mut bytes, &86_400i32.to_le_bytes());
        put(&mut bytes, &2u16.to_le_bytes());
        put(&mut bytes, &20240229u32.to_le_bytes());
        put(&mut bytes, &34_200_000u32.to_le_bytes());
        put(&mut bytes, &2.5f64.to_le_bytes());
        put(&mut bytes, &(-1234i16).to_le_bytes());
        put(&mut bytes, &100i16.to_le_bytes());
        bytes.push(7);
        put(&mut bytes, &i32::MIN.to_le_bytes());
        bytes.extend(513u16.to_le_bytes());
        bytes.extend(513u16.to_be_bytes());
        bytes.extend([1, 2, 3]);
        bytes.extend([4, 5]);
        let df = collect(&open(&spec, bytes));
        let row: Vec<String> = df
            .columns()
            .iter()
            .map(|c| c.get(0).unwrap().to_string())
            .collect();
        assert_eq!(
            row,
            [
                "200",
                "60000",
                "16000000",
                "4000000000",
                "1099511627775",
                "18446744073709551615",
                "-5",
                "-300",
                "-70000",
                "-70000",
                "-1",
                "-9223372036854775808",
                "1.5",
                "-2.25",
                "true",
                "\"hi\"",
                "b\"\\xde\\xad\"",
                "2000-01-02 00:00:00",
                "2000-01-03",
                "2024-02-29",
                "09:30:00",
                "1900-01-01 12:00:00",
                "-12.34",
                "10.0",
                "\"seven\"",
                "null",
                "513",
                "513",
                "[1, 2, 3]",
                "4",
                "5",
            ],
            "{endian}"
        );
        assert_eq!(
            df.get_column_names(),
            [
                "u1", "u2", "u3", "u4", "u5", "u8", "s1", "s2", "s3", "s4", "s6", "s8", "f4", "f8",
                "flag", "s", "b", "t", "day", "ymd", "tod", "serial", "d", "c", "e", "n", "le",
                "be", "arr", "lv_0", "lv_1"
            ]
        );
    }
}

#[test]
fn a_time_of_day_takes_its_date_from_the_header() {
    let text = r#"name = "t.tod"
[header]
fields = [{ name = "trade_date", type = "u4", date = "yyyymmdd" }]
[records]
fields = [{ name = "ts", type = "u6be", time = "ns", of_day = true, date = "header.trade_date" }]"#;
    let spec = Spec::parse(text, None).unwrap();
    let mut bytes = 20240102u32.to_le_bytes().to_vec();
    bytes.extend(&34_200_000_000_123u64.to_be_bytes()[2..]);
    let df = collect(&open(&spec, bytes));
    assert_eq!(cell(&df, "ts", 0), "2024-01-02 09:30:00.000000123");
}

#[test]
fn a_bad_magic_is_refused_saying_what_was_found() {
    let spec = Spec::parse(L2, None).unwrap();
    let mut bytes = l2_file(&[(1, "A", 1, 1)], 1, &[]);
    bytes[..4].copy_from_slice(b"NOPE");
    let e = spec
        .open_rows(Arc::new(Bytes::Owned(bytes)), "x.l2")
        .err()
        .unwrap();
    assert!(
        e.contains("expected magic 4c 32 46 44 at byte 0, found 4e 4f 50 45"),
        "{e}"
    );
}

#[test]
fn a_truncated_last_record_is_left_out_and_shown() {
    let text = "name = \"t.x\"\n[records]\nfields = [{ name = \"a\", type = \"u4\" }]";
    let spec = Spec::parse(text, None).unwrap();
    let bytes = vec![1, 0, 0, 0, 2, 0, 0, 0, 0xab, 0xcd];
    let opened = spec
        .open_rows(Arc::new(Bytes::Owned(bytes)), "x.bin")
        .unwrap();
    assert_eq!(collect(&opened).height(), 2);
    assert_eq!(
        opened.notes,
        ["x.bin: 2 trailing bytes left out, not a whole record: ab cd"]
    );
    // The header's count says more than is there: the whole records are shown.
    let spec = Spec::parse(L2, None).unwrap();
    let opened = open(&spec, l2_file(&[(1, "A", 1, 1)], 3, &[9]));
    assert_eq!(collect(&opened).height(), 1);
    assert!(
        opened.notes[0].contains("says 3 records"),
        "{:?}",
        opened.notes
    );
}

#[test]
fn sizes_come_from_the_header_and_are_bounded() {
    let text = r#"name = "t.h"
[header]
fields = [{ name = "len", type = "u1" }, { name = "title", type = "str", size = "len" }, { name = "rec", type = "u2" }]
[records]
size = "header.rec"
size_adjust = 1
fields = [{ name = "a", type = "u1" }, { name = "b", type = "bytes", size = "header.len" }]"#;
    let spec = Spec::parse(text, None).unwrap();
    let mut bytes = vec![2, b'h', b'i', 3, 0];
    bytes.extend([1, 0xa, 0xb, 0xff, 2, 0xc, 0xd, 0xff]);
    let opened = open(&spec, bytes);
    let df = collect(&opened);
    assert_eq!(df.height(), 2);
    assert_eq!(
        df.column("a").unwrap().u8().unwrap().to_vec(),
        [Some(1), Some(2)]
    );
    assert_eq!(opened.header.text("title").as_deref(), Some("hi"));
    // A size the file gives past the bound is refused, not believed.
    let text = r#"name = "t.h"
[header]
fields = [{ name = "len", type = "u8" }]
[records]
fields = [{ name = "b", type = "bytes", size = "header.len" }]"#;
    let spec = Spec::parse(text, None).unwrap();
    let e = spec
        .open_rows(Arc::new(Bytes::Owned(u64::MAX.to_le_bytes().to_vec())), "h")
        .err()
        .unwrap();
    assert!(e.contains("outside 0 to"), "{e}");
}

#[test]
fn a_columns_layout_reads_one_file_per_field() {
    let dir = tempfile::tempdir().unwrap();
    let text = r#"name = "kdb.trades"
layout = "columns"
match = { glob = "trades" }
[header]
size = 8
[records]
fields = [{ name = "price", type = "f8" }, { name = "size", type = "s4", file = "qty" }]"#;
    let spec = Spec::parse(text, None).unwrap();
    let mut price = vec![0u8; 8];
    for p in [1.5f64, 2.5, 3.5] {
        price.extend(p.to_le_bytes());
    }
    let mut qty = vec![0u8; 8];
    for q in [10i32, 20] {
        qty.extend(q.to_le_bytes());
    }
    std::fs::write(dir.path().join("price"), price).unwrap();
    std::fs::write(dir.path().join("qty"), qty).unwrap();
    let opened = spec.open(dir.path(), "trades").unwrap();
    let df = collect(&opened);
    assert_eq!(df.height(), 2);
    assert_eq!(
        df.column("size").unwrap().i32().unwrap().to_vec(),
        [Some(10), Some(20)]
    );
    assert!(
        opened.notes[0].contains("price 3, qty 2"),
        "{:?}",
        opened.notes
    );
    let registry = Registry::of(vec![spec]);
    assert_eq!(registry.by_glob(Path::new("/db/trades"), true).len(), 1);
    assert!(registry.by_glob(Path::new("/db/trades"), false).is_empty());
}

/// A header sized by its own field may differ from file to file; each column
/// starts after its own file's header.
#[test]
fn each_column_file_starts_after_its_own_header() {
    let dir = tempfile::tempdir().unwrap();
    let text = r#"name = "t.cols"
layout = "columns"
[header]
fields = [{ name = "len", type = "u1" }]
size = "len"
[records]
fields = [{ name = "a", type = "u1" }, { name = "b", type = "u1" }]"#;
    let spec = Spec::parse(text, None).unwrap();
    std::fs::write(dir.path().join("a"), [2, 0, 7, 8]).unwrap();
    std::fs::write(dir.path().join("b"), [4, 0, 0, 0, 9, 10]).unwrap();
    let df = collect(&spec.open(dir.path(), "t").unwrap());
    assert_eq!(
        df.column("a").unwrap().u8().unwrap().to_vec(),
        [Some(7), Some(8)]
    );
    assert_eq!(
        df.column("b").unwrap().u8().unwrap().to_vec(),
        [Some(9), Some(10)]
    );
}

#[test]
fn the_search_path_keeps_the_first_of_each_name() {
    let a = tempfile::tempdir().unwrap();
    let b = tempfile::tempdir().unwrap();
    std::fs::write(a.path().join("l2.toml"), L2).unwrap();
    std::fs::write(b.path().join("l2.toml"), L2.replace("*.l2", "*.lvl2")).unwrap();
    std::fs::write(b.path().join("bad.toml"), "name = 3").unwrap();
    std::fs::write(b.path().join("notes.txt"), "not a spec").unwrap();
    let path = vec![a.path().to_path_buf(), b.path().to_path_buf()];
    let registry = Registry::load(&path);
    assert_eq!(registry.specs.len(), 1);
    assert_eq!(registry.specs[0].spec.globs, ["*.l2"]);
    assert_eq!(registry.specs[0].overrides, [b.path().join("l2.toml")]);
    assert_eq!(registry.errors.len(), 1);
    let listing = registry.listing(&path);
    assert!(listing.contains("overrides"), "{listing}");
    assert!(
        listing.contains("bad.toml\":1:8: name: expected a string."),
        "{listing}"
    );
}

#[test]
fn glob_comes_before_magic_and_ties_are_kept() {
    let one = Spec::parse(L2, None).unwrap();
    let two = Spec::parse(&L2.replace("acme.l2feed", "acme.other"), None).unwrap();
    let magic_only = Spec::parse(
        &L2.replace("acme.l2feed", "acme.magic")
            .replace("glob = [\"*.l2\"], ", ""),
        None,
    )
    .unwrap();
    let registry = Registry::of(vec![one, two, magic_only]);
    let matched = registry
        .matching(Path::new("a.l2"), false, |_| {
            panic!("no read for a glob match")
        })
        .unwrap();
    assert_eq!(matched.by, Chosen::Glob);
    assert_eq!(matched.specs.len(), 2);
    let matched = registry
        .matching(Path::new("a.dat"), false, |n| {
            assert_eq!(n, 4);
            Some(b"L2FD".to_vec())
        })
        .unwrap();
    assert_eq!(matched.by, Chosen::Magic);
    assert_eq!(matched.specs.len(), 3);
    assert!(
        registry
            .matching(Path::new("a.dat"), false, |_| Some(b"nope".to_vec()))
            .is_none()
    );
}

/// Fixed records say their columns from the spec alone, as the open finds them;
/// framed records and a size from the header leave it to the open.
#[test]
fn fixed_records_say_their_columns_without_a_file() {
    let spec = Spec::parse(L2, None).unwrap();
    let opened = open(&spec, l2_file(&[(1, "A", 1, 1)], 1, &[]));
    let schema: Vec<(String, DataType)> = opened
        .records
        .schema()
        .iter()
        .map(|(n, t)| (n.to_string(), t.clone()))
        .collect();
    assert_eq!(spec.static_columns(), Some(schema));
    let sized = L2.replace("size = 8 }", "size = \"header.count\" }");
    assert_eq!(Spec::parse(&sized, None).unwrap().static_columns(), None);
    let framed = r#"name = "acme.f"
[records]
framing = "length_prefixed"
size = "len"
fields = [{ name = "len", type = "u2" }, { name = "x", type = "u1" }]"#;
    assert_eq!(Spec::parse(framed, None).unwrap().static_columns(), None);
}

/// A listing names a file by a spec's magic and `where` from the bytes it read
/// already, never more; a glob still comes first, and a name that says a format
/// datui reads keeps it.
#[test]
fn a_listing_names_a_file_from_the_head_it_read() {
    let versioned = r#"name = "acme.v1"
match = { magic = "L2FD", where = { "header.version" = 1 } }
[header]
fields = [{ type = "pad", size = 4 }, { name = "version", type = "u1" }]
[records]
fields = [{ name = "x", type = "u1" }]"#;
    let globbed = r#"name = "acme.named"
match = { glob = "named.bin" }
[records]
fields = [{ name = "x", type = "u1" }]"#;
    let registry = Registry::of(vec![
        Spec::parse(versioned, None).unwrap(),
        Spec::parse(globbed, None).unwrap(),
    ]);
    let name = |path: &str, head: &[u8], whole: bool| {
        registry
            .listed(Path::new(path), head, whole)
            .map(|s| s.name.clone())
    };
    let v1 = b"L2FD\x01rest";
    assert_eq!(name("data.bin", v1, true).as_deref(), Some("acme.v1"));
    assert_eq!(name("data", v1, false).as_deref(), Some("acme.v1"));
    assert_eq!(name("data.bin", b"L2FD\x02rest", true), None);
    assert_eq!(name("data.bin", b"nope", true), None);
    // Shorter than the header, and the file goes on: not settled, not named.
    assert_eq!(name("data.bin", b"L2FD", false), None);
    assert_eq!(name("named.bin", v1, true).as_deref(), Some("acme.named"));
    assert_eq!(name("data.csv", v1, true), None);
    assert_eq!(name("data.bin.gz", v1, true), None);
}

/// One spec per version, told apart by a header field after the magic.
#[test]
fn a_header_version_picks_the_spec() {
    let version = |v: u8| {
        format!(
            r#"name = "acme.v{v}"
match = {{ glob = "*.l2", magic = "L2FD", where = {{ "header.version" = {v} }} }}
[header]
fields = [{{ type = "pad", size = 4 }}, {{ name = "version", type = "u1" }}]
[records]
fields = [{{ name = "x", type = "u1" }}]"#
        )
    };
    let registry = Registry::of(vec![
        Spec::parse(&version(2), None).unwrap(),
        Spec::parse(&version(3), None).unwrap(),
    ]);
    for v in [2u8, 3] {
        let matched = registry
            .matching(Path::new("a.l2"), false, |n| {
                assert_eq!(n, 5);
                Some([b"L2FD".as_slice(), &[v]].concat())
            })
            .unwrap();
        let names: Vec<&str> = matched.specs.iter().map(|s| s.name.as_str()).collect();
        assert_eq!(names, [format!("acme.v{v}")]);
    }
    let e = Spec::parse(
        &version(2).replace("\"header.version\"", "\"header.nope\""),
        None,
    )
    .unwrap_err();
    assert!(e.message.contains("no header field named `nope`"), "{e}");
}

#[test]
fn the_search_path_is_config_dir_then_env_then_config() {
    let env = std::env::join_paths(["/org/a", "/org/b"]).unwrap();
    let path = search_path(Some(Path::new("/cfg")), Some(env), &["/c".to_string()]);
    assert_eq!(
        path,
        [
            PathBuf::from("/cfg/formats"),
            PathBuf::from("/org/a"),
            PathBuf::from("/org/b"),
            PathBuf::from("/c")
        ]
    );
}

#[test]
fn formats_check_validates_and_prints_the_first_rows() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("l2.toml");
    std::fs::write(&spec, L2).unwrap();
    let data = dir.path().join("day.l2");
    std::fs::write(&data, l2_file(&[(1, "AAPL", 1, 10_000)], 1, &[7])).unwrap();
    let registry = Registry::default();
    let text = check(
        &spec.to_string_lossy(),
        Some(&data),
        &registry,
        &crate::OpenOptions::default(),
    )
    .unwrap();
    assert!(text.starts_with("acme.l2feed: ok"), "{text}");
    assert!(text.contains("warning: day.l2 has 1 byte after"), "{text}");
    assert!(
        text.contains("AAPL") && text.contains("1 records"),
        "{text}"
    );
    std::fs::write(&spec, "name = \"a.b\"\n[records]\nfields = 1").unwrap();
    let e = check(
        &spec.to_string_lossy(),
        None,
        &registry,
        &crate::OpenOptions::default(),
    )
    .unwrap_err();
    assert!(
        e.contains("l2.toml\":3:10: fields: expected an array"),
        "{e}"
    );
    assert!(
        check(
            "acme.nothing",
            None,
            &registry,
            &crate::OpenOptions::default()
        )
        .is_err()
    );
}

const LOG: &str = r##"
name = "acme.instrument-log"
kind = "delimited"
match = { magic = "#device_info" }

comment = "#"
skip_initial_space = true
header_rows = { name = 3, unit = 2 }
metadata_line = 1

[columns]
time = { from = ["Lcl Date", "Lcl Time", "UTCOfst"], as = "datetime" }
"##;

const LOG_TEXT: &str = "#device_info, log_version=\"1.03\", model=\"X\"\n#yyyy-mm-dd, hh:mm:ss, hh:mm, deg F\n  Lcl Date, Lcl Time, UTCOfst, E1 CHT1\n          ,         ,       ,   187.2\n2024-03-01, 10:00:00, -05:00,   180.0\n";

#[test]
fn the_delimited_example_parses() {
    use crate::formats::delimited_spec::{DerivedKind, HeaderRows};
    let spec = Spec::parse(LOG, None).unwrap();
    assert!(spec.is_delimited());
    assert_eq!(spec.magic, b"#device_info");
    let d = spec.delimited.as_deref().unwrap();
    assert_eq!(d.comment_char.as_deref(), Some("#"));
    assert_eq!(d.skip_initial_space, Some(true));
    assert_eq!(
        d.header_rows,
        Some(HeaderRows {
            name: vec![3],
            unit: Some(2)
        })
    );
    assert_eq!(d.metadata_line, Some(1));
    assert_eq!(d.columns[0].name, "time");
    assert_eq!(d.columns[0].kind, DerivedKind::Datetime);
    assert_eq!(d.columns[0].from.len(), 3);
    // A list of name lines joins them, as Frictionless does.
    let joined = Spec::parse(
            "name = \"a.b\"\nkind = \"delimited\"\nheader_rows = [1, 2]\ndelimiter = \"\\t\"\nnull_values = [\"NA\", \"x=-1\"]",
            None,
        )
        .unwrap();
    let d = joined.delimited.as_deref().unwrap();
    assert_eq!(d.header_rows.as_ref().unwrap().name, [1, 2]);
    assert_eq!(d.delimiter, Some(b'\t'));
    assert_eq!(d.null_values, ["NA", "x=-1"]);
    // A binary spec is not one.
    assert!(!Spec::parse(L2, None).unwrap().is_delimited());
}

#[test]
fn delimited_spec_errors_point_at_the_line_and_column() {
    let head = "name = \"a.b\"\nkind = \"delimited\"\n";
    for (rest, said) in [
        (
            "records = 1",
            "3:1: Unknown key `records` in a delimited spec",
        ),
        ("header_rows = { unit = 2 }", "header_rows: missing `name`"),
        (
            "header_rows = { name = 2, unit = 2 }",
            "header_rows.unit: line 2 is also a name line",
        ),
        (
            "header_rows = { name = 2, description = 1 }",
            "header_rows.description is not yet supported",
        ),
        (
            "header_rows = 0",
            "header_rows: expected a line from 1 to 1000",
        ),
        ("header_rows = [2, 2]", "header_rows: line 2 is named twice"),
        (
            "header_rows = 2\nmetadata_line = 5",
            "4:17: metadata_line: line 5 would be read as data",
        ),
        (
            "header_rows = 2\nmetadata_line = 2",
            "metadata_line: line 2 is a header line",
        ),
        (
            "delimiter = \"ab\"",
            "delimiter: \"ab\" is not one ASCII character",
        ),
        ("comment_char = \"#\"", "comment_char"),
        ("comment = \"\"", "comment: must not be empty"),
        (
            "match = { where = { \"header.v\" = 1 } }",
            "`where` compares a binary header's fields",
        ),
        (
            "[columns]\nt = { from = [\"a\", \"b\"], as = \"date\" }",
            "columns.t.from: as = \"date\" takes one column",
        ),
        (
            "[columns]\nt = { from = \"a\", as = \"instant\" }",
            "columns.t.as: expected datetime, date or time",
        ),
        (
            "[columns]\nt = { as = \"date\" }",
            "columns.t: missing `from`",
        ),
        ("kind = \"text\"", "kind: expected binary or delimited"),
    ] {
        let text = format!("{head}{rest}");
        let text = text.replacen(
            "kind = \"delimited\"\nkind = \"text\"",
            "kind = \"text\"",
            1,
        );
        let e = Spec::parse(&text, None).unwrap_err().to_string();
        assert!(e.contains(said), "{rest}: {e}");
    }
    // A metadata line below the header lines is fine when it is a comment line.
    let text = format!("{head}header_rows = 2\ncomment = \"#\"\nmetadata_line = 5");
    assert!(Spec::parse(&text, None).is_ok());
}

#[test]
fn a_delimited_spec_matches_csv_names_and_a_binary_spec_does_not() {
    let dir = tempfile::tempdir().unwrap();
    let csv = dir.path().join("flight.csv");
    std::fs::write(&csv, LOG_TEXT).unwrap();
    let binary = "name = \"acme.raw\"\nmatch = { glob = \"*.csv\", magic = \"#dev\" }\n[records]\nfields = [{ name = \"b\", type = \"u1\" }]";
    let registry = Registry::of(vec![
        Spec::parse(binary, None).unwrap(),
        Spec::parse(LOG, None).unwrap(),
    ]);
    let asked = Asked::default();
    match route(&csv, &asked, &registry).unwrap() {
        Route::Delimited(choice) => {
            assert_eq!(choice.spec.name, "acme.instrument-log");
            assert_eq!(choice.by, Chosen::Magic);
        }
        _ => panic!("the delimited spec reads the CSV"),
    }
    // Several files: only a delimited spec is asked about.
    let several = Asked {
        spec_name: Some("acme.raw".into()),
        text_only: true,
        ..Asked::default()
    };
    let Err(e) = route(&csv, &several, &registry) else {
        panic!("a binary spec does not read several files");
    };
    assert!(e.contains("reads one file"), "{e}");
    // The magic is the start of the first line, after a byte-order mark.
    let bom = dir.path().join("bom.csv");
    std::fs::write(&bom, format!("\u{feff}{LOG_TEXT}")).unwrap();
    assert!(matches!(
        route(&bom, &asked, &registry).unwrap(),
        Route::Delimited(_)
    ));
    // A CSV whose first line is not the magic is read as it always was.
    let plain = dir.path().join("plain.csv");
    std::fs::write(&plain, "a,b\n1,2\n").unwrap();
    assert!(matches!(
        route(&plain, &asked, &registry).unwrap(),
        Route::Elsewhere
    ));
}

#[test]
fn formats_check_prints_a_delimited_file_s_metadata_units_and_rows() {
    let dir = tempfile::tempdir().unwrap();
    let spec = dir.path().join("log.toml");
    std::fs::write(&spec, LOG).unwrap();
    let data = dir.path().join("flight.csv");
    std::fs::write(&data, LOG_TEXT).unwrap();
    let registry = Registry::default();
    let text = check(
        &spec.to_string_lossy(),
        Some(&data),
        &registry,
        &crate::OpenOptions::default(),
    )
    .unwrap();
    assert!(text.starts_with("acme.instrument-log: ok"), "{text}");
    assert!(
        text.contains("delimited: names on line 3, units on line 2, metadata on line 1"),
        "{text}"
    );
    assert!(
        text.contains("time = datetime from Lcl Date, Lcl Time, UTCOfst"),
        "{text}"
    );
    assert!(
        text.contains("metadata: device_info: log_version = 1.03, model = X"),
        "{text}"
    );
    assert!(text.contains("E1 CHT1 = deg F"), "{text}");
    assert!(text.contains("2024-03-01 15:00:00 UTC"), "{text}");
    let listing = Registry::of(vec![Spec::parse(LOG, None).unwrap()]).listing(&[]);
    assert!(
        listing.contains("acme.instrument-log  (delimited; magic #device_info)"),
        "{listing}"
    );
}

/// DBC files share the search path: a `.dbc` file for every interface, a
/// `kind = "dbc"` TOML file that names one for an interface, and one that does not
/// parse is an error with its line.
#[test]
fn dbc_files_on_the_search_path() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("l2.toml"), L2).unwrap();
    std::fs::write(
        dir.path().join("car.dbc"),
        "BO_ 291 ENGINE: 8 ECU\n SG_ Speed : 0|16@1+ (0.125,0) [0|8191] \"rpm\" GW\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("body.toml"),
        "kind = \"dbc\"\nfile = \"body/body.dbc\"\n[match]\ninterface = \"can1\"\n",
    )
    .unwrap();
    std::fs::create_dir(dir.path().join("body")).unwrap();
    std::fs::write(
        dir.path().join("body/body.dbc"),
        "BO_ 512 DOORS: 1 GW\n SG_ Open : 0|1@1+ (1,0) [0|1] \"\" ECU\n",
    )
    .unwrap();
    std::fs::write(dir.path().join("bad.dbc"), "BO_ 1 A: 8 X\n SG_ nope\n").unwrap();
    let path = vec![dir.path().to_path_buf()];
    let registry = Registry::load(&path);
    assert_eq!(registry.specs.len(), 1);
    let names: Vec<(&str, Option<&str>)> = registry
        .dbc
        .iter()
        .map(|f| (f.dbc.name.as_str(), f.dbc.interface.as_deref()))
        .collect();
    assert_eq!(names, [("body", Some("can1")), ("car", None)]);
    assert_eq!(registry.errors.len(), 1, "{:?}", registry.errors);
    assert_eq!(registry.errors[0].line, 2);
    let listing = registry.listing(&path);
    assert!(listing.contains("Dictionaries (DBC):"), "{listing}");
    assert!(
        listing.contains("body  (1 message, interface can1)"),
        "{listing}"
    );
}

/// `formats check` takes a DBC file by name, as a `.dbc` file or as the `kind = "dbc"`
/// TOML file that names one; with a candump log, it says how many frames it names.
/// One that does not parse is refused at its line.
#[test]
fn formats_check_reads_dbc_files() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
            dir.path().join("car.dbc"),
            "BO_ 291 ENGINE: 8 ECU\n SG_ Speed : 0|16@1+ (0.125,0) [0|8191] \"rpm\" GW\n SG_ Temp : 16|8@1+ (1,-40) [-40|215] \"C\" GW\n",
        )
        .unwrap();
    std::fs::write(
        dir.path().join("body.toml"),
        "kind = \"dbc\"\nfile = \"car.dbc\"\n[match]\ninterface = \"can1\"\n",
    )
    .unwrap();
    let bad = dir.path().join("bad.dbc");
    std::fs::write(&bad, "BO_ 1 A: 8 X\n SG_ nope\n").unwrap();
    let registry = Registry::load(&[dir.path().to_path_buf()]);
    let options = crate::OpenOptions::default();

    let text = check("car", None, &registry, &options).unwrap();
    assert!(text.starts_with("car: ok\n"), "{text}");
    assert!(text.contains("car.dbc"), "{text}");
    assert!(text.contains("1 message, 2 signals"), "{text}");

    let toml = dir.path().join("body.toml");
    let text = check(&toml.to_string_lossy(), None, &registry, &options).unwrap();
    assert!(text.contains("matches interface can1"), "{text}");

    let log = dir.path().join("drive.log");
    std::fs::write(
            &log,
            "(1706689000.100000) can0 123#B80B280000000000\n(1706689000.200000) can0 456#00\n(1706689000.300000) can0 123#C00B290000000000\n",
        )
        .unwrap();
    let text = check("car", Some(&log), &registry, &options).unwrap();
    assert!(text.contains("3 frames, 2 of them named by car"), "{text}");
    assert!(text.contains("messages in the log: ENGINE (2)"), "{text}");

    let e = check(&bad.to_string_lossy(), None, &registry, &options).unwrap_err();
    assert!(e.starts_with("error: ") && e.contains("bad.dbc\":2"), "{e}");
}

/// FIX dictionaries share the search path: a `kind = "fix"` TOML file and a
/// QuickFIX XML file are listed apart from the specs, an XML file that is not one is
/// passed over, and `formats check` reads a log with one.
#[test]
fn fix_dictionaries_on_the_search_path() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("l2.toml"), L2).unwrap();
    std::fs::write(
            dir.path().join("broker.toml"),
            "name = \"acme.fix.broker-x\"\nkind = \"fix\"\nmatch = { sender = \"BROKERX\" }\ntags = { 9001 = \"AlgoName\" }\n",
        )
        .unwrap();
    std::fs::write(
            dir.path().join("FIX44-custom.xml"),
            "<fix major='4' minor='4'><fields><field number='5001' name='Desk' type='STRING'/></fields></fix>",
        )
        .unwrap();
    std::fs::write(dir.path().join("other.xml"), "<gpx/>").unwrap();
    std::fs::write(
        dir.path().join("broken.toml"),
        "name = \"acme.fix.bad\"\nkind = \"fix\"\ntags = { nine = \"X\" }\n",
    )
    .unwrap();
    let path = vec![dir.path().to_path_buf()];
    let registry = Registry::load(&path);
    assert_eq!(registry.specs.len(), 1);
    let names: Vec<&str> = registry.fix.iter().map(|f| f.dict.name.as_str()).collect();
    assert_eq!(names, ["FIX44-custom", "acme.fix.broker-x"]);
    assert_eq!(registry.errors.len(), 1, "{:?}", registry.errors);
    assert!(registry.errors[0].to_string().contains("tags.nine"));
    let listing = registry.listing(&path);
    assert!(listing.contains("Dictionaries (FIX):"), "{listing}");
    assert!(
        listing.contains("acme.fix.broker-x  (sender BROKERX)"),
        "{listing}"
    );
    assert!(
        listing.contains("FIX44-custom  (begin string FIX.4.4)"),
        "{listing}"
    );

    let log = dir.path().join("session.log");
    std::fs::write(
        &log,
        "8=FIX.4.4|9=20|35=0|49=BROKERX|9001=x|10=000|\n8=FIX.4.4|9=5|35=0|49=OTHER|10=000|\n",
    )
    .unwrap();
    let text = check(
        "acme.fix.broker-x",
        Some(&log),
        &registry,
        &crate::OpenOptions::default(),
    )
    .unwrap();
    assert!(text.starts_with("acme.fix.broker-x: ok"), "{text}");
    assert!(text.contains("matches sender BROKERX"), "{text}");
    assert!(text.contains("2 messages, 1 of them matched"), "{text}");
    assert!(text.contains("names in the log: 9001 AlgoName"), "{text}");
    let e = check(
        &dir.path().join("broken.toml").to_string_lossy(),
        None,
        &registry,
        &crate::OpenOptions::default(),
    )
    .unwrap_err();
    assert!(e.contains("broken.toml\":3:1: tags.nine"), "{e}");
}
