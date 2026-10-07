//! Excel workbooks: the sheets of one, listed on the home screen and summed up on the
//! Info panel.
//!
//! A sheet is read whole by calamine when it is opened (see [`read`]). What is said of
//! the others comes from what that open read anyway, or from the start of each sheet:
//! an `.xlsx` sheet declares its range (`<dimension ref="A1:D100"/>`) before its
//! cells, so its size costs no read of them.
//!
//! The home screen lists an `.xlsx` or `.xlsm` workbook's sheets from its directory and
//! `xl/workbook.xml`, a few KB however large the workbook. An `.xls` or `.xlsb` file
//! keeps its sheet names in a binary stream that only a read of the workbook finds, so
//! it opens its first sheet and `--table` names another.

use std::io::Read;
use std::path::Path;

use calamine::{Data, Dimensions, Range, Reader, Sheets, open_workbook_auto};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use color_eyre::Result;
use polars::prelude::*;

use crate::model_files::MetaValue;
use crate::sqlite::Table;
use crate::text_formats::{Detail, count};
use crate::{FileFormat, OpenOptions};

/// The workbook part of an `.xlsx` or `.xlsm` file, and its relationships.
const WORKBOOK: &str = "xl/workbook.xml";
const WORKBOOK_RELS: &str = "xl/_rels/workbook.xml.rels";
/// The most of either part read: a workbook of thousands of sheets is still far less.
const MAX_PART: u64 = 4 << 20;

/// Whether `head`, the first bytes of `file`, begin a workbook whose sheets can be listed
/// without reading it: a zip file holding `xl/workbook.xml`.
pub fn is_listable(head: &[u8], file: Option<&Path>) -> bool {
    head.starts_with(b"PK\x03\x04")
        && file.is_some_and(|file| {
            std::fs::File::open(file)
                .ok()
                .and_then(|f| ::zip::ZipArchive::new(f).ok())
                .is_some_and(|zip| zip.index_for_name(WORKBOOK).is_some())
        })
}

/// The sheets of the workbook at `path`, in the workbook's order, for the home screen
/// and `--table`. A hidden sheet is the workbook's own, listed after Ctrl+A; a chart
/// sheet holds no cells and is left out.
pub fn sheets(path: &Path) -> Result<Vec<Table>> {
    let mut zip = ::zip::ZipArchive::new(std::fs::File::open(path)?)?;
    let workbook = part(&mut zip, WORKBOOK)?;
    let rels = part(&mut zip, WORKBOOK_RELS).unwrap_or_default();
    let charts: Vec<String> = tags(&rels, "Relationship")
        .into_iter()
        .filter(|attrs| attr(attrs, "Type").is_some_and(|t| t.ends_with("/chartsheet")))
        .filter_map(|attrs| attr(&attrs, "Id"))
        .collect();
    Ok(tags(&workbook, "sheet")
        .into_iter()
        .filter(|attrs| attr(attrs, "id").is_none_or(|id| !charts.contains(&id)))
        .filter_map(|attrs| {
            Some(Table {
                name: attr(&attrs, "name")?,
                kind: "worksheet".to_string(),
                internal: attr(&attrs, "state").is_some_and(|s| s != "visible"),
                columns: Vec::new(),
            })
        })
        .collect())
}

/// One part of a zip file as text.
fn part(zip: &mut ::zip::ZipArchive<std::fs::File>, name: &str) -> Result<String> {
    let mut text = String::new();
    zip.by_name(name)?
        .take(MAX_PART)
        .read_to_string(&mut text)?;
    Ok(text)
}

/// The attributes of each element named `name` (with or without a namespace prefix) in
/// `xml`, as written: `<sheet name="Sales" sheetId="1" r:id="rId1"/>`.
fn tags<'a>(xml: &'a str, name: &str) -> Vec<Vec<(&'a str, String)>> {
    let mut found = Vec::new();
    let mut rest = xml;
    while let Some(at) = rest.find('<') {
        rest = &rest[at + 1..];
        let end = rest.find('>').unwrap_or(rest.len());
        let tag = &rest[..end];
        rest = &rest[end..];
        let element = tag.split(|c: char| c.is_whitespace() || c == '/').next();
        let local = element.map(|e| e.rsplit(':').next().unwrap_or(e));
        if local == Some(name) {
            found.push(attributes(&tag[element.map_or(0, str::len)..]));
        }
    }
    found
}

/// The attributes of a tag, after its name, with entities resolved. A name keeps no
/// prefix: `r:id` is `id`.
fn attributes(text: &str) -> Vec<(&str, String)> {
    let mut attrs = Vec::new();
    let mut rest = text;
    while let Some(eq) = rest.find('=') {
        let key = rest[..eq].trim().trim_start_matches('/');
        let key = key.rsplit(':').next().unwrap_or(key);
        let value = rest[eq + 1..].trim_start();
        let Some(quote) = value.chars().next().filter(|q| *q == '"' || *q == '\'') else {
            break;
        };
        let Some(close) = value[1..].find(quote) else {
            break;
        };
        attrs.push((key, unescape(&value[1..1 + close])));
        rest = &value[close + 2..];
    }
    attrs
}

fn attr(attrs: &[(&str, String)], key: &str) -> Option<String> {
    attrs
        .iter()
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v.clone())
}

/// XML's five entities and character references.
fn unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_string();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let Some(end) = rest.find(';') else {
            break;
        };
        let entity = &rest[1..end];
        let ch = match entity {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => entity
                .strip_prefix("#x")
                .map(|hex| u32::from_str_radix(hex, 16))
                .or_else(|| entity.strip_prefix('#').map(str::parse))
                .and_then(|n| n.ok())
                .and_then(char::from_u32),
        };
        match ch {
            Some(ch) => {
                out.push(ch);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// The Excel tab of the Info panel for a workbook whose sheet `opened` was read as
/// `range`: each sheet's range, the opened one's from its cells and the others' as
/// their files declare it or as the open already parsed them.
pub fn detail<RS: std::io::Read + std::io::Seek>(
    workbook: &mut Sheets<RS>,
    opened: &str,
    range: &Range<Data>,
) -> Detail {
    let meta: Vec<calamine::Sheet> = workbook.sheets_metadata().to_vec();
    let mut list = Vec::with_capacity(meta.len());
    let mut tables = Vec::new();
    let mut hidden = 0u64;
    let mut charts = 0u64;
    for sheet in &meta {
        let mut said = Vec::new();
        let other = match sheet.typ {
            calamine::SheetType::WorkSheet => None,
            calamine::SheetType::ChartSheet => Some("chart sheet"),
            calamine::SheetType::DialogSheet => Some("dialog sheet"),
            calamine::SheetType::MacroSheet => Some("macro sheet"),
            calamine::SheetType::Vba => Some("VBA module"),
        };
        if other.is_none() {
            tables.push(sheet.name.clone());
        }
        if let Some(other) = other {
            charts += 1;
            said.push(other.to_string());
        } else if sheet.name == opened {
            said.push(size(range.start().zip(range.end())));
            said.push("opened".to_string());
        } else if let Some(dims) =
            // A sheet that declares no range reads as the default one.
            declared(workbook, &sheet.name).filter(|d| *d != Dimensions::default())
        {
            said.push(size(Some((dims.start, dims.end))));
        }
        if sheet.visible != calamine::SheetVisible::Visible {
            hidden += 1;
            said.push("hidden".to_string());
        }
        list.push((sheet.name.clone(), MetaValue::Text(said.join(", "))));
    }
    let mut first = count(meta.len() as u64, "worksheet", "worksheets");
    let middot = crate::glyphs::get().middot;
    if hidden > 0 {
        first.push_str(&format!(" {middot} {hidden} hidden"));
    }
    if charts > 0 {
        first.push_str(&format!(
            " {middot} {}",
            count(charts, "without cells", "without cells")
        ));
    }
    Detail {
        tab: crate::text_formats::tab(FileFormat::Excel),
        lines: vec![first, format!("Opened: {opened}")],
        list_title: "Worksheets",
        list,
        tables,
        table: Some(opened.to_string()),
        ..Default::default()
    }
}

/// The range a sheet other than the one opened says it covers, read no further than
/// its cells: an `.xlsx` or `.xlsb` sheet's declared range, or an `.xls` or `.ods`
/// sheet as the open already parsed it.
fn declared<RS: std::io::Read + std::io::Seek>(
    workbook: &mut Sheets<RS>,
    name: &str,
) -> Option<Dimensions> {
    match workbook {
        Sheets::Xlsx(xlsx) => xlsx
            .worksheet_cells_reader(name)
            .ok()
            .map(|r| r.dimensions()),
        Sheets::Xlsb(xlsb) => xlsb
            .worksheet_cells_reader(name)
            .ok()
            .map(|r| r.dimensions()),
        Sheets::Xls(_) | Sheets::Ods(_) => {
            let range = workbook.worksheet_range(name).ok()?;
            let (start, end) = range.start().zip(range.end())?;
            Some(Dimensions { start, end })
        }
    }
}

/// A range as a sheet names it, and its size: `A1:D100, 100 × 4`. Empty: `empty`.
fn size(range: Option<((u32, u32), (u32, u32))>) -> String {
    let Some(((r0, c0), (r1, c1))) = range.filter(|((r0, c0), (r1, c1))| r1 >= r0 && c1 >= c0)
    else {
        return "empty".to_string();
    };
    let times = crate::glyphs::get().times;
    format!(
        "{}{}:{}{}, {} {times} {}",
        column(c0),
        r0 + 1,
        column(c1),
        r1 + 1,
        crate::numfmt::group_chrome((r1 - r0 + 1) as usize),
        crate::numfmt::group_chrome((c1 - c0 + 1) as usize)
    )
}

/// A 0-based column as a sheet letters it: 0 is `A`, 26 is `AA`.
fn column(mut c: u32) -> String {
    let mut letters = Vec::new();
    loop {
        letters.push(b'A' + (c % 26) as u8);
        if c < 26 {
            break;
        }
        c = c / 26 - 1;
    }
    letters.reverse();
    String::from_utf8(letters).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn columns_are_lettered_as_a_sheet_letters_them() {
        for (c, letters) in [
            (0, "A"),
            (3, "D"),
            (25, "Z"),
            (26, "AA"),
            (701, "ZZ"),
            (702, "AAA"),
        ] {
            assert_eq!(column(c), letters, "{c}");
        }
        assert_eq!(
            size(Some(((0, 0), (99, 3)))),
            "A1:D100, 100 × 4".replace('×', crate::glyphs::get().times)
        );
        assert_eq!(size(None), "empty");
    }

    #[test]
    fn sheets_are_read_from_the_workbook_part() {
        let workbook = r#"<?xml version="1.0"?>
<workbook xmlns:r="x"><sheets>
<sheet name="Sales &amp; Costs" sheetId="1" r:id="rId1"/>
<sheet name='2023' sheetId="2" r:id="rId2" state="hidden"/>
<x:sheet name="Chart" sheetId="3" r:id="rId3"/>
</sheets></workbook>"#;
        let rels = r#"<Relationships>
<Relationship Id="rId1" Type="http://x/worksheet" Target="worksheets/sheet1.xml"/>
<Relationship Id="rId2" Type="http://x/worksheet" Target="worksheets/sheet2.xml"/>
<Relationship Id="rId3" Type="http://x/chartsheet" Target="chartsheets/sheet1.xml"/>
</Relationships>"#;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("book.xlsx");
        let mut zip = ::zip::ZipWriter::new(std::fs::File::create(&path).unwrap());
        for (name, text) in [(WORKBOOK, workbook), (WORKBOOK_RELS, rels)] {
            zip.start_file(name, ::zip::write::SimpleFileOptions::default())
                .unwrap();
            std::io::Write::write_all(&mut zip, text.as_bytes()).unwrap();
        }
        zip.finish().unwrap();
        let head = std::fs::read(&path).unwrap();
        assert!(is_listable(&head, Some(&path)));
        let sheets = sheets(&path).unwrap();
        let named: Vec<(&str, bool)> = sheets
            .iter()
            .map(|t| (t.name.as_str(), t.internal))
            .collect();
        assert_eq!(named, [("Sales & Costs", false), ("2023", true)]);
    }

    #[test]
    fn entities_resolve() {
        assert_eq!(unescape("a&lt;b&gt;&#65;&#x42;&bogus;"), "a<b>AB&bogus;");
    }
}

/// One sheet of a workbook (xls, xlsx, xlsm, xlsb), read whole by calamine, with the
/// workbook's Excel tab for the Info panel. The sheet is the one `options.table`
/// (`--table`) names, or by 0-based index when no sheet is so named.
pub fn read(path: &Path, options: &OpenOptions) -> Result<(LazyFrame, Detail)> {
    let mut workbook =
        open_workbook_auto(path).map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?;
    let sheet_names = workbook.sheet_names().to_vec();
    if sheet_names.is_empty() {
        return Err(color_eyre::eyre::eyre!("Excel file has no worksheets"));
    }
    // Named so a bad --table says what to ask for instead: "0 'Sales', 1 'Summary'".
    let sheets_on_offer = || {
        sheet_names
            .iter()
            .enumerate()
            .map(|(i, name)| format!("{} '{}'", i, name))
            .collect::<Vec<_>>()
            .join(", ")
    };
    // A sheet's name before an index: the home screen names a sheet called `2023`.
    let opened = match options.table.as_deref() {
        None => sheet_names[0].clone(),
        Some(name) if sheet_names.iter().any(|n| n == name) => name.to_string(),
        Some(sheet_sel) => match sheet_sel.parse::<usize>() {
            Ok(idx) => sheet_names.get(idx).cloned().ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "Excel: no worksheet at index {}; this file has: {}",
                    idx,
                    sheets_on_offer()
                )
            })?,
            Err(_) => {
                return Err(color_eyre::eyre::eyre!(
                    "Excel: no worksheet named '{}'; this file has: {}",
                    sheet_sel,
                    sheets_on_offer()
                ));
            }
        },
    };
    let range = workbook
        .worksheet_range(&opened)
        .map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?;
    let detail = crate::excel::detail(&mut workbook, &opened, &range);
    drop(workbook);
    let rows: Vec<Vec<Data>> = range.rows().map(|r| r.to_vec()).collect();
    if rows.is_empty() {
        let empty_df = DataFrame::empty();
        return Ok((empty_df.lazy(), detail));
    }
    let headers: Vec<String> = rows[0]
        .iter()
        .map(|c| calamine::DataType::as_string(c).unwrap_or_else(|| c.to_string()))
        .collect();
    let n_cols = headers.len();
    let mut series_vec = Vec::with_capacity(n_cols);
    for (col_idx, header) in headers.iter().enumerate() {
        let col_cells: Vec<Option<&Data>> = rows[1..].iter().map(|row| row.get(col_idx)).collect();
        let inferred = excel_infer_column_type(&col_cells);
        let name = if header.is_empty() {
            format!("column_{}", col_idx + 1)
        } else {
            header.clone()
        };
        let series = excel_column_to_series(name.as_str(), &col_cells, inferred)?;
        series_vec.push(series.into());
    }
    let df = DataFrame::new_infer_height(series_vec)?;
    Ok((df.lazy(), detail))
}

/// Infers column type: prefers Int64 for whole-number floats; infers Date/Datetime for
/// calamine DateTime/DateTimeIso or for string columns that parse as ISO date/datetime.
fn excel_infer_column_type(cells: &[Option<&Data>]) -> ExcelColType {
    use calamine::DataType as CalamineTrait;
    let mut has_string = false;
    let mut has_float = false;
    let mut has_int = false;
    let mut has_bool = false;
    let mut has_datetime = false;
    for cell in cells.iter().flatten() {
        if CalamineTrait::is_string(*cell) {
            has_string = true;
            break;
        }
        if CalamineTrait::is_float(*cell)
            || CalamineTrait::is_datetime(*cell)
            || CalamineTrait::is_datetime_iso(*cell)
        {
            has_float = true;
        }
        if CalamineTrait::is_int(*cell) {
            has_int = true;
        }
        if CalamineTrait::is_bool(*cell) {
            has_bool = true;
        }
        if CalamineTrait::is_datetime(*cell) || CalamineTrait::is_datetime_iso(*cell) {
            has_datetime = true;
        }
    }
    if has_string {
        let any_parsed = cells
            .iter()
            .flatten()
            .any(|c| excel_cell_to_naive_datetime(c).is_some());
        let all_non_empty_parse = cells
            .iter()
            .flatten()
            .all(|c| CalamineTrait::is_empty(*c) || excel_cell_to_naive_datetime(c).is_some());
        if any_parsed && all_non_empty_parse {
            if excel_parsed_cells_all_midnight(cells) {
                ExcelColType::Date
            } else {
                ExcelColType::Datetime
            }
        } else {
            ExcelColType::Utf8
        }
    } else if has_int {
        ExcelColType::Int64
    } else if has_datetime {
        if excel_parsed_cells_all_midnight(cells) {
            ExcelColType::Date
        } else {
            ExcelColType::Datetime
        }
    } else if has_float {
        let all_whole = cells.iter().flatten().all(|cell| {
            cell.as_f64()
                .is_none_or(|f| f.is_finite() && (f - f.trunc()).abs() < 1e-10)
        });
        if all_whole {
            ExcelColType::Int64
        } else {
            ExcelColType::Float64
        }
    } else if has_bool {
        ExcelColType::Boolean
    } else {
        ExcelColType::Utf8
    }
}

/// True if every cell that parses as datetime has time 00:00:00.
fn excel_parsed_cells_all_midnight(cells: &[Option<&Data>]) -> bool {
    let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("valid time");
    cells
        .iter()
        .flatten()
        .filter_map(|c| excel_cell_to_naive_datetime(c))
        .all(|dt| dt.time() == midnight)
}

/// Converts a calamine cell to NaiveDateTime (Excel serial, DateTimeIso, or parseable string).
fn excel_cell_to_naive_datetime(cell: &Data) -> Option<NaiveDateTime> {
    use calamine::DataType;
    if let Some(dt) = cell.as_datetime() {
        return Some(dt);
    }
    let s = cell.get_datetime_iso().or_else(|| cell.get_string())?;
    parse_naive_datetime_str(s)
}

/// Parses an ISO-style date/datetime string; tries FORMATS in order.
fn parse_naive_datetime_str(s: &str) -> Option<NaiveDateTime> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    const FORMATS: &[&str] = &[
        "%Y-%m-%dT%H:%M:%S%.f",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ];
    for fmt in FORMATS {
        if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
            return Some(dt);
        }
    }
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return Some(d.and_hms_opt(0, 0, 0).expect("midnight"));
    }
    None
}

/// Build a Polars Series from a column of calamine cells using the inferred type.
fn excel_column_to_series(
    name: &str,
    cells: &[Option<&Data>],
    col_type: ExcelColType,
) -> Result<Series> {
    use calamine::DataType as CalamineTrait;
    use polars::datatypes::TimeUnit;
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
    let series = match col_type {
        ExcelColType::Int64 => values(name, cells, |c| c.as_i64()),
        ExcelColType::Float64 => values(name, cells, |c| c.as_f64()),
        ExcelColType::Boolean => values(name, cells, |c| c.get_bool()),
        ExcelColType::Utf8 => values(name, cells, |c| c.as_string()),
        ExcelColType::Date => values(name, cells, |c| {
            excel_cell_to_naive_datetime(c).map(|dt| (dt.date() - epoch).num_days() as i32)
        })
        .cast(&DataType::Date)?,
        ExcelColType::Datetime => values(name, cells, |c| {
            excel_cell_to_naive_datetime(c).map(|dt| dt.and_utc().timestamp_micros())
        })
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?,
    };
    Ok(series)
}

/// The column of `cells` as `get` reads each, empty cells null.
fn values<T>(name: &str, cells: &[Option<&Data>], get: impl Fn(&Data) -> Option<T>) -> Series
where
    Series: NamedFrom<Vec<Option<T>>, [Option<T>]>,
{
    let values: Vec<Option<T>> = cells.iter().map(|c| c.and_then(&get)).collect();
    Series::new(name.into(), values)
}

/// Inferred type for an Excel column (preserves numbers, bools, dates; avoids stringifying).
#[derive(Clone, Copy)]
enum ExcelColType {
    Int64,
    Float64,
    Boolean,
    Utf8,
    Date,
    Datetime,
}
