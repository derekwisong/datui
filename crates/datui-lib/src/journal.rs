//! The systemd journal as `journalctl -o json` writes it: NDJSON whose records carry
//! `__CURSOR` and `__REALTIME_TIMESTAMP`.
//!
//! journalctl has already parsed the journal, so this is the NDJSON reader with a few
//! expressions on top: `time` from `__REALTIME_TIMESTAMP`, `level` from `PRIORITY` in
//! order of severity, `MESSAGE` as text where it came as bytes, and the columns put in
//! the order a reader of logs looks for them. Every field stays. A file's records are
//! read whole into memory, up to `limits.journal_bytes`, with the schema inferred from
//! all of them, so a field first seen late is a column too. A pipe or a followed file is scanned as it grows
//! (`crate::follow::lines`); a pipe's fields first seen after the open join once it
//! ends.

use std::path::PathBuf;
use std::sync::Arc;

use color_eyre::Result;
use polars::prelude::*;

use crate::model_files::MetaValue;
use crate::text_formats::Detail;

/// What datui does with journal JSON: see [`crate::readers`].
pub(crate) const READER: crate::readers::Reader = crate::readers::Reader {
    scan,
    signatures: &[crate::readers::Signature {
        says: |head, _| looks_like(head),
        kind: crate::readers::Kind::Text,
        trusted: crate::readers::Trusted {
            // A listing does not parse text.
            listing: false,
            ..crate::readers::EVERYWHERE
        },
    }],
    refines: &[crate::FileFormat::Jsonl, crate::FileFormat::Json],
    python: Some(crate::python_script::Python {
        call: "pl.scan_ndjson",
        eager: false,
        glob_flag: false,
        arguments: Some(python_arguments),
    }),
    export: Some(crate::export_modal::ExportFormat::Ndjson),
    ..crate::readers::BASE
};

pub const TIME: &str = "time";
pub const LEVEL: &str = "level";
const REALTIME: &str = "__REALTIME_TIMESTAMP";
const PRIORITY: &str = "PRIORITY";
const MESSAGE: &str = "MESSAGE";
const UNIT: &str = "_SYSTEMD_UNIT";
const IDENTIFIER: &str = "SYSLOG_IDENTIFIER";

/// The levels `PRIORITY` 0 to 7 names, most severe first: the order a sort and
/// `level <= "err"` go by.
pub const LEVELS: [&str; 8] = [
    "emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
];

/// Whether `head` begins journal JSON: its first line is an object with `__CURSOR`
/// and `__REALTIME_TIMESTAMP`. A first line longer than the head is judged on the keys
/// it names so far.
pub fn looks_like(head: &[u8]) -> bool {
    let text = head.strip_prefix(b"\xef\xbb\xbf").unwrap_or(head);
    let start = text
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .unwrap_or(text.len());
    let text = &text[start..];
    if text.first() != Some(&b'{') {
        return false;
    }
    let names = |keys: &dyn Fn(&str) -> bool| keys("__CURSOR") && keys(REALTIME);
    match memchr::memchr(b'\n', text) {
        Some(end) => match serde_json::from_slice::<serde_json::Value>(&text[..end]) {
            Ok(serde_json::Value::Object(object)) => names(&|k| object.contains_key(k)),
            _ => false,
        },
        None => {
            let line = String::from_utf8_lossy(text);
            names(&|k| line.contains(&format!("\"{k}\":")))
        }
    }
}

/// The records of `paths`, read whole up to `most` bytes in all, every record's fields
/// among the columns; and the bytes left out past `most`, which end at a whole record.
fn read_all(paths: &[PathBuf], most: u64) -> Result<(DataFrame, u64)> {
    use std::io::Read;
    fn read(reader: impl polars::io::mmap::MmapBytesReader) -> PolarsResult<DataFrame> {
        JsonReader::new(reader)
            .with_json_format(JsonFormat::JsonLines)
            .infer_schema_len(None)
            .finish()
    }
    // One file under the cap goes to the reader as it is, mapped rather than copied.
    if let [path] = paths {
        let file = std::fs::File::open(path)?;
        if file.metadata()?.len() <= most {
            return Ok((read(file)?, 0));
        }
    }
    // One read of them all, so the schema is every file's fields.
    let mut bytes = Vec::new();
    let mut left_out = 0;
    for path in paths {
        let file = std::fs::File::open(path)?;
        let len = file.metadata()?.len();
        // Once one file is cut short, the rest are left out whole.
        let room = if left_out > 0 {
            0
        } else {
            most.saturating_sub(bytes.len() as u64)
        };
        file.take(room).read_to_end(&mut bytes)?;
        if len > room {
            left_out += len - room;
            // The record cut through is left out whole.
            let whole = memchr::memrchr(b'\n', &bytes).map_or(0, |at| at + 1);
            left_out += (bytes.len() - whole) as u64;
            bytes.truncate(whole);
        }
        if bytes.last().is_some_and(|&b| b != b'\n') {
            bytes.push(b'\n');
        }
    }
    Ok((read(std::io::Cursor::new(bytes))?, left_out))
}

fn scan(input: crate::readers::ScanIn<'_>) -> Result<crate::scan::Scan> {
    let path = input.paths[0].clone();
    let failed = |e: &dyn std::fmt::Display| -> color_eyre::Report {
        crate::error_display::FileError::new(&path, format!("not journal JSON: {e}")).into()
    };
    let mut left_out = 0;
    let raw = if input.options.follow {
        crate::follow::scan_lines(&path, input.options, true, &mut input.report.read_python)?
    } else {
        let most = crate::limits::get().journal_bytes.bytes();
        let (df, past) = read_all(input.paths, most).map_err(|e| failed(&e))?;
        left_out = past;
        df.lazy()
    };
    let schema = raw.clone().collect_schema().map_err(|e| failed(&e))?;
    let bytes = came_as_bytes(&raw, &schema).map_err(|e| failed(&e))?;
    let (lf, python) = derive(raw, &schema);
    input.report.read_python.extend(python);
    let detail = summary(&lf).map_err(|e| failed(&e))?;
    let mut notes = Vec::new();
    if left_out > 0 {
        let size = crate::numfmt::bytes;
        notes.push(crate::text_formats::note(
            format!(
                "{} left out: past the first {} {} limits.journal_bytes raises it",
                size(left_out),
                size(crate::limits::get().journal_bytes.bytes()),
                crate::glyphs::get().middot
            ),
            "the journal".to_string(),
        ));
    }
    if bytes > 0 {
        notes.push(crate::text_formats::note(
            format!(
                "{} stored as bytes {} shown as text, invalid UTF-8 as \u{fffd}",
                crate::text_formats::count(bytes as u64, "message", "messages"),
                crate::glyphs::get().middot
            ),
            "the journal".to_string(),
        ));
    }
    input.report.opened = Some(Arc::new(crate::members::Opened {
        detail: Some(Arc::new(detail)),
        notes,
        ..Default::default()
    }));
    // Each entry's place in the journal, for `#`. Read whole, so the index costs no
    // pushdown; a followed journal is scanned, and goes without.
    let lf = if input.options.follow {
        lf
    } else {
        lf.with_row_index(crate::row_index::INDEX, None)
    };
    Ok(lf.into())
}

/// The journal's columns: `time` and `level` derived, `MESSAGE` as text, in the order
/// a reader of logs looks for them, bookkeeping last. Also the same as Python method
/// calls, for Copy as Python.
pub(crate) fn derive(lf: LazyFrame, schema: &Schema) -> (LazyFrame, Vec<String>) {
    let has = |name: &str| schema.contains(name);
    let mut exprs = Vec::new();
    let mut python = Vec::new();
    if has(REALTIME) {
        exprs.push(
            col(REALTIME)
                .cast(DataType::Int64)
                .cast(DataType::Datetime(
                    TimeUnit::Microseconds,
                    Some(polars::prelude::TimeZone::UTC),
                ))
                .alias(TIME),
        );
        python.push(format!(
            "pl.col(\"{REALTIME}\").cast(pl.Int64).cast(pl.Datetime(\"us\", \"UTC\")).alias(\"{TIME}\")"
        ));
    }
    if has(PRIORITY) {
        exprs.push(level(col(PRIORITY).cast(DataType::String)));
        let names: Vec<String> = LEVELS.iter().map(|l| format!("\"{l}\"")).collect();
        let numbers: Vec<String> = (0..LEVELS.len()).map(|i| format!("\"{i}\"")).collect();
        python.push(format!(
            "pl.col(\"{PRIORITY}\").cast(pl.String).replace_strict([{}], [{}], default=None, return_dtype=pl.Enum([{}])).alias(\"{LEVEL}\")",
            numbers.join(", "),
            names.join(", "),
            names.join(", ")
        ));
    }
    if let Some(dtype) = schema.get(MESSAGE)
        && (dtype.is_string() || matches!(dtype, DataType::List(_)))
    {
        exprs.push(
            col(MESSAGE)
                .map(
                    |c: Column| as_text(&c).map(|s| s.into_column()),
                    |_, field| Ok(Field::new(field.name().clone(), DataType::String)),
                )
                .alias(MESSAGE),
        );
        python.push(format!(
            "pl.col(\"{MESSAGE}\").map_elements(lambda m: bytes(int(b) for b in m.strip(\"[]\").split(\",\")).decode(\"utf-8\", \"replace\") if m.startswith(\"[\") and m.endswith(\"]\") else m, return_dtype=pl.String)"
        ));
    }
    let lf = if exprs.is_empty() {
        lf
    } else {
        lf.with_columns(exprs)
    };
    let mut names: Vec<String> = schema.iter_names().map(|n| n.to_string()).collect();
    for derived in [TIME, LEVEL] {
        if (derived == TIME && has(REALTIME)) || (derived == LEVEL && has(PRIORITY)) {
            names.retain(|n| n != derived);
            names.push(derived.to_string());
        }
    }
    let order = order(&names);
    let quoted: Vec<String> = order.iter().map(|n| format!("\"{n}\"")).collect();
    let mut calls = Vec::new();
    if !python.is_empty() {
        calls.push(format!(".with_columns({})", python.join(", ")));
    }
    calls.push(format!(".select([{}])", quoted.join(", ")));
    (
        lf.select(order.iter().map(|n| col(n.as_str())).collect::<Vec<_>>()),
        calls,
    )
}

/// `PRIORITY` 0 to 7 as its level, an enum in order of severity; anything else null.
fn level(priority: Expr) -> Expr {
    let levels = FrozenCategories::new(LEVELS).expect("eight distinct names");
    let mut expr = lit(NULL).cast(DataType::String);
    for (i, name) in LEVELS.iter().enumerate().rev() {
        expr = when(priority.clone().eq(lit(i.to_string())))
            .then(lit(*name))
            .otherwise(expr);
    }
    expr.cast(DataType::from_frozen_categories(levels))
        .alias(LEVEL)
}

/// The columns in reading order: time, level, the unit (or the identifier when there
/// is no unit), the PID and the message, then the rest as they came, then bookkeeping.
fn order(names: &[String]) -> Vec<String> {
    let has = |n: &str| names.iter().any(|m| m == n);
    let unit = if has(UNIT) { UNIT } else { IDENTIFIER };
    let first: Vec<&str> = [TIME, LEVEL, unit, "_PID", MESSAGE]
        .into_iter()
        .filter(|n| has(n))
        .collect();
    let bookkeeping =
        |n: &str| n.starts_with("__") || matches!(n, "_BOOT_ID" | "_MACHINE_ID" | "_RUNTIME_SCOPE");
    let mut out: Vec<String> = first.iter().map(|n| n.to_string()).collect();
    out.extend(
        names
            .iter()
            .filter(|n| !first.contains(&n.as_str()) && !bookkeeping(n))
            .cloned(),
    );
    out.extend(names.iter().filter(|n| bookkeeping(n)).cloned());
    out
}

/// A message journalctl wrote as bytes: an array of numbers, which Polars reads into a
/// text column as `[115, 116]`, or a list column when every message was one.
fn bytes_of(text: &str) -> Option<Vec<u8>> {
    let inner = text.strip_prefix('[')?.strip_suffix(']')?;
    inner
        .split(',')
        .map(|b| b.trim().parse::<u8>().ok())
        .collect()
}

/// `MESSAGE` as text: bytes decoded, lossily.
fn as_text(column: &Column) -> PolarsResult<Series> {
    let name = column.name().clone();
    match column.dtype() {
        DataType::List(_) => {
            let lists = column.list()?;
            let out: StringChunked = (0..lists.len())
                .map(|i| {
                    let item = lists.get_as_series(i)?;
                    let item = item.cast(&DataType::UInt8).ok()?;
                    let bytes: Vec<u8> = item.u8().ok()?.iter().map(|b| b.unwrap_or(0)).collect();
                    Some(String::from_utf8_lossy(&bytes).into_owned())
                })
                .collect();
            Ok(out.with_name(name).into_series())
        }
        _ => {
            let text = column.str()?;
            let out: StringChunked = text
                .iter()
                .map(|v| {
                    v.map(|v| match bytes_of(v) {
                        Some(bytes) => String::from_utf8_lossy(&bytes).into_owned(),
                        None => v.to_string(),
                    })
                })
                .collect();
            Ok(out.with_name(name).into_series())
        }
    }
}

/// How many messages came as bytes rather than text.
fn came_as_bytes(raw: &LazyFrame, schema: &Schema) -> PolarsResult<usize> {
    let Some(dtype) = schema.get(MESSAGE) else {
        return Ok(0);
    };
    let df = raw.clone().select([col(MESSAGE)]).collect()?;
    let column = df.column(MESSAGE)?;
    Ok(match dtype {
        DataType::List(_) => column.len() - column.null_count(),
        DataType::String => column
            .str()?
            .iter()
            .filter(|v| v.is_some_and(|v| bytes_of(v).is_some()))
            .count(),
        _ => 0,
    })
}

/// The Info panel's tab: the span, the entries, units, boots and hosts.
pub(crate) fn summary(lf: &LazyFrame) -> PolarsResult<Detail> {
    let schema = lf.clone().collect_schema()?;
    let has = |n: &str| schema.contains(n);
    let distinct = |n: &str| {
        if has(n) {
            col(n)
                .drop_nulls()
                .n_unique()
                .cast(DataType::UInt64)
                .alias(n)
        } else {
            lit(0u64).alias(n)
        }
    };
    let mut aggs = vec![
        len().cast(DataType::UInt64).alias("entries"),
        distinct(UNIT),
        distinct("_BOOT_ID"),
        distinct("_HOSTNAME"),
    ];
    if has(TIME) {
        aggs.push(col(TIME).min().alias("from"));
        aggs.push(col(TIME).max().alias("to"));
    }
    let totals = lf.clone().select(aggs).collect()?;
    let number = |n: &str| -> u64 {
        totals
            .column(n)
            .ok()
            .and_then(|c| c.u64().ok()?.get(0))
            .unwrap_or(0)
    };
    let group = |n: u64| crate::numfmt::group_chrome(usize::try_from(n).unwrap_or(usize::MAX));
    let mut lines = vec![format!("Entries: {}", group(number("entries")))];
    if has(TIME) {
        let at = |n: &str| {
            totals
                .column(n)
                .ok()
                .and_then(|c| c.get(0).ok())
                .map(|v| v.to_string())
                .filter(|v| v != "null")
        };
        if let (Some(from), Some(to)) = (at("from"), at("to")) {
            lines.push(format!("From: {from}"));
            lines.push(format!("To: {to}"));
        }
    }
    lines.push(format!("Units: {}", group(number(UNIT))));
    lines.push(format!("Boots: {}", group(number("_BOOT_ID"))));
    lines.push(format!("Hosts: {}", group(number("_HOSTNAME"))));
    let mut list = Vec::new();
    let mut units = 0;
    if has(UNIT) {
        let counts = lf
            .clone()
            .filter(col(UNIT).is_not_null())
            .group_by([col(UNIT)])
            .agg([len().alias("n")])
            .sort(
                ["n"],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .collect()?;
        units = counts.height();
        let names = counts.column(UNIT)?.str()?.clone();
        let n = counts.column("n")?.cast(&DataType::UInt64)?;
        let n = n.u64()?;
        list = names
            .iter()
            .zip(n.iter())
            .map(|(name, n)| {
                (
                    name.unwrap_or_default().to_string(),
                    MetaValue::Text(crate::text_formats::count(
                        n.unwrap_or(0),
                        "entry",
                        "entries",
                    )),
                )
            })
            .collect();
    }
    let detail = Detail {
        tab: crate::text_formats::tab(crate::FileFormat::Journal),
        lines,
        list_title: "Units",
        list: crate::text_formats::capped_list(list.into_iter(), units),
        ..Default::default()
    };
    Ok(detail)
}

/// Copy as Python: every record's fields, as the open read them.
fn python_arguments(
    call: &mut crate::python_script::Call<'_>,
) -> Option<crate::python_script::Source> {
    call.args.push("infer_schema_length=None".to_string());
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Past `most` bytes the records are left out whole, and counted; a later file is
    /// left out entirely.
    #[test]
    fn a_read_stops_at_its_cap_on_a_whole_record() {
        let dir = tempfile::tempdir().unwrap();
        let one = dir.path().join("one.json");
        let two = dir.path().join("two.json");
        let record = |n: u32| format!("{{\"MESSAGE\":\"m{n}\"}}\n");
        std::fs::write(&one, (0..3).map(record).collect::<String>()).unwrap();
        std::fs::write(&two, record(9)).unwrap();
        let line = record(0).len() as u64;
        let (df, left_out) = read_all(&[one.clone(), two.clone()], line + 3).unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(left_out, 3 * line);
        let (df, left_out) = read_all(&[one, two], u64::MAX).unwrap();
        assert_eq!((df.height(), left_out), (4, 0));
    }

    #[test]
    fn journal_json_by_its_first_record() {
        let entry = br#"{"__CURSOR":"s=1;i=1","__REALTIME_TIMESTAMP":"1","MESSAGE":"hi"}"#;
        let mut head = entry.to_vec();
        head.push(b'\n');
        assert!(looks_like(&head));
        // Cut before its end: by the keys it names.
        assert!(looks_like(&entry[..50]));
        assert!(!looks_like(b"{\"a\": 1}\n"));
        assert!(!looks_like(b"__CURSOR __REALTIME_TIMESTAMP\n"));
        let piped = crate::readers::sniff(&head, None, crate::readers::Asked::Pipe, |_| true);
        assert_eq!(piped, Some(crate::FileFormat::Journal));
    }

    #[test]
    fn columns_in_reading_order() {
        let names: Vec<String> = [
            "__CURSOR",
            "_BOOT_ID",
            "SYSLOG_IDENTIFIER",
            "MESSAGE",
            "_PID",
            "_HOSTNAME",
            "time",
            "level",
        ]
        .map(String::from)
        .to_vec();
        assert_eq!(
            order(&names),
            [
                "time",
                "level",
                "SYSLOG_IDENTIFIER",
                "_PID",
                "MESSAGE",
                "_HOSTNAME",
                "__CURSOR",
                "_BOOT_ID"
            ]
        );
    }

    #[test]
    fn bytes_are_numbers_in_brackets() {
        assert_eq!(bytes_of("[104, 105]"), Some(b"hi".to_vec()));
        assert_eq!(bytes_of("[104,105]"), Some(b"hi".to_vec()));
        assert_eq!(bytes_of("[256]"), None);
        assert_eq!(bytes_of("[]"), None);
        assert_eq!(bytes_of("hi [1]"), None);
    }

    #[test]
    fn the_info_tab_sums_the_journal() {
        let df = df!(
            REALTIME => ["1767225600000000", "1767225660000000", "1767225720000000"],
            PRIORITY => ["6", "3", "9"],
            UNIT => [Some("a.service"), Some("a.service"), None],
            "_BOOT_ID" => ["b1", "b2", "b2"],
            "_HOSTNAME" => ["h", "h", "h"],
            MESSAGE => ["x", "[104, 105]", "y"],
        )
        .unwrap();
        let lf = df.lazy();
        let schema = lf.clone().collect_schema().unwrap();
        assert_eq!(came_as_bytes(&lf, &schema).unwrap(), 1);
        let (lf, python) = derive(lf, &schema);
        assert!(python.last().unwrap().starts_with(".select("));
        let out = lf.clone().collect().unwrap();
        let level: Vec<Option<String>> = out
            .column(LEVEL)
            .unwrap()
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .iter()
            .map(|v| v.map(str::to_string))
            .collect();
        assert_eq!(level, [Some("info".into()), Some("err".into()), None]);
        let detail = summary(&lf).unwrap();
        assert_eq!(detail.tab, "Journal");
        for line in ["Entries: 3", "Units: 1", "Boots: 2", "Hosts: 1"] {
            assert!(detail.lines.iter().any(|l| l == line), "{:?}", detail.lines);
        }
        assert!(
            detail
                .lines
                .iter()
                .any(|l| l.starts_with("From: 2026-01-01 00:00:00"))
        );
        assert_eq!(detail.list.len(), 1);
    }
}
