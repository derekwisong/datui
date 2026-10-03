//! The parts of a CSV's dialect that Polars' reader does not have, named as the
//! Frictionless Table Dialect names them.
//!
//! | Frictionless | Here | Done by |
//! |---|---|---|
//! | `commentChar` | `--comment` | Polars' `comment_prefix`, before the header and in the data |
//! | `headerRows`, `headerJoin` | `--header-rows`, `header_join` | [`header_names`] reads those lines; Polars reads the rest without a header |
//! | `skipInitialSpace` | `--skip-initial-space` | [`skip_initial_space`], lazy expressions over the text columns |
//!
//! Header names are trimmed whatever the dialect: [`shown_names`].

use std::io::BufRead;

use polars::prelude::*;

/// What `header_join` is when nothing sets it: Frictionless' `headerJoin` default.
pub const DEFAULT_HEADER_JOIN: &str = " ";

pub use datui_cli::check_comment_char;

/// The longest header line [`header_names`] reads: far wider than any real header,
/// and a bound on what a file with no line breaks can make it hold.
const MAX_HEADER_LINE: u64 = 16 << 20;

/// The names of the columns, from the lines `rows` names (1-based, counted from the top
/// of the file before anything is skipped), each split on `separator` and trimmed.
///
/// A column's name is its pieces from those lines, in the order `rows` gives them,
/// joined with `join`; a blank piece adds nothing. A line that starts with `comment`
/// is a header line all the same, since the user named it, and loses the prefix. A
/// file that ends before the last line named is an error: it has no header there.
pub fn header_names(
    source: impl BufRead,
    rows: &[usize],
    join: &str,
    separator: u8,
    comment: Option<&str>,
) -> color_eyre::Result<Vec<String>> {
    let lines = named_lines(source, rows)?;
    let mut columns: Vec<Vec<String>> = Vec::new();
    for (&row, line) in rows.iter().zip(&lines) {
        for (i, field) in header_fields(line, row, separator, comment)
            .into_iter()
            .enumerate()
        {
            if columns.len() <= i {
                columns.resize_with(i + 1, Vec::new);
            }
            if !field.is_empty() {
                columns[i].push(field);
            }
        }
    }
    Ok(columns
        .into_iter()
        .map(|pieces| pieces.join(join))
        .collect())
}

/// The lines `rows` names (1-based, from the top of the file), in the order `rows`
/// gives them, each with its line break. Only those lines are held, each up to a
/// bound; a file that ends before the last of them is an error.
pub fn named_lines(mut source: impl BufRead, rows: &[usize]) -> color_eyre::Result<Vec<Vec<u8>>> {
    use std::io::Read;
    let last = rows.iter().copied().max().unwrap_or(0);
    // Only the named lines are kept; the others are passed over without being held.
    let mut lines: Vec<Vec<u8>> = vec![Vec::new(); last];
    for (i, line) in lines.iter_mut().enumerate() {
        let n = i + 1;
        let read = if rows.contains(&n) {
            (&mut source)
                .take(MAX_HEADER_LINE + 1)
                .read_until(b'\n', line)?
        } else {
            source.skip_until(b'\n')?
        };
        if read == 0 {
            return Err(color_eyre::eyre::eyre!(
                "header line {last} is past the end of the file"
            ));
        }
        if line.len() as u64 > MAX_HEADER_LINE {
            return Err(color_eyre::eyre::eyre!(
                "header line {n} is longer than {} MiB",
                MAX_HEADER_LINE >> 20
            ));
        }
    }
    Ok(rows
        .iter()
        .map(|&row| {
            row.checked_sub(1)
                .and_then(|i| lines.get(i))
                .cloned()
                .unwrap_or_default()
        })
        .collect())
}

/// Header line `row`'s fields, trimmed: without a byte-order mark on line 1, its line
/// break, or `comment`'s prefix, split on `separator`.
pub fn header_fields(line: &[u8], row: usize, separator: u8, comment: Option<&str>) -> Vec<String> {
    let mut line = line;
    if row == 1 {
        line = line.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(line);
    }
    line = line.strip_suffix(b"\n").unwrap_or(line);
    line = line.strip_suffix(b"\r").unwrap_or(line);
    if let Some(prefix) = comment.filter(|c| !c.is_empty()) {
        line = line.strip_prefix(prefix.as_bytes()).unwrap_or(line);
    }
    split_fields(line, separator)
        .into_iter()
        .map(|f| f.trim().to_string())
        .collect()
}

/// One line's fields, split on `separator` outside double quotes, with the quotes
/// removed and a doubled quote read as one. Padding before an opening quote is
/// dropped, so `  "Lcl Date"` is one quoted field.
fn split_fields(line: &[u8], separator: u8) -> Vec<String> {
    let mut fields = Vec::new();
    let mut field: Vec<u8> = Vec::new();
    let mut quoted = false;
    let mut i = 0;
    while i < line.len() {
        let b = line[i];
        if quoted {
            if b == b'"' {
                if line.get(i + 1) == Some(&b'"') {
                    field.push(b'"');
                    i += 1;
                } else {
                    quoted = false;
                }
            } else {
                field.push(b);
            }
        } else if b == separator {
            fields.push(String::from_utf8_lossy(&field).into_owned());
            field.clear();
        } else if b == b'"' && field.iter().all(u8::is_ascii_whitespace) {
            field.clear();
            quoted = true;
        } else {
            field.push(b);
        }
        i += 1;
    }
    fields.push(String::from_utf8_lossy(&field).into_owned());
    fields
}

/// The names a read shows for the columns Polars named `raw`: the header lines'
/// names by position when `--header-rows` gave them, else Polars' own, trimmed. A
/// blank name is `column_N`, as Polars names a headerless file's; a name already
/// taken gets `_duplicated_K`, as Polars marks a repeated header.
pub fn shown_names(raw: &[PlSmallStr], header: Option<&[String]>) -> Vec<String> {
    let names: Vec<String> = raw
        .iter()
        .enumerate()
        .map(|(i, name)| {
            let name = match header {
                Some(header) => header.get(i).map_or("", String::as_str),
                None => name.as_str(),
            }
            .trim();
            if name.is_empty() {
                format!("column_{}", i + 1)
            } else {
                name.to_string()
            }
        })
        .collect();
    let mut taken: PlHashSet<String> = PlHashSet::with_capacity(names.len());
    let mut seen: PlHashMap<String, usize> = PlHashMap::with_capacity(names.len());
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        let count = seen.entry(name.clone()).or_insert(0);
        let mut candidate = name.clone();
        while !taken.insert(candidate.clone()) {
            candidate = format!("{name}_duplicated_{count}");
            *count += 1;
        }
        out.push(candidate);
    }
    out
}

/// `lf` with its columns named as [`shown_names`] says. Nothing is read: Polars has
/// the schema from the scan's own inference.
///
/// With `header`, a file with nothing after its header lines (a log with no rows yet)
/// is a table with those columns and no rows, as Polars reads a file that is only a
/// header; Polars itself finds no data past the lines it skipped.
pub fn name_columns(mut lf: LazyFrame, header: Option<&[String]>) -> PolarsResult<LazyFrame> {
    let schema = match (lf.collect_schema(), header) {
        (Err(PolarsError::NoData(_)), Some(header)) => return header_only(header),
        (Ok(schema), Some(header)) if schema.is_empty() => return header_only(header),
        (schema, _) => schema?,
    };
    let raw: Vec<PlSmallStr> = schema.iter_names().cloned().collect();
    let shown = shown_names(&raw, header);
    if raw.iter().zip(&shown).all(|(r, s)| r.as_str() == s) {
        return Ok(lf);
    }
    Ok(lf.rename(raw.iter().map(|s| s.as_str()), shown.iter(), true))
}

/// No rows, a text column for each name `header` gives.
fn header_only(header: &[String]) -> PolarsResult<LazyFrame> {
    let raw: Vec<PlSmallStr> = (1..=header.len().max(1))
        .map(|i| format!("column_{i}").into())
        .collect();
    let columns: Vec<Column> = shown_names(&raw, Some(header))
        .into_iter()
        .map(|name| Column::new_empty(name.into(), &DataType::String))
        .collect();
    Ok(DataFrame::new(0, columns)?.lazy())
}

/// An eager read of the lines after `header`'s, with nothing there read as no
/// columns, which [`name_columns`] makes the header's columns with no rows.
pub fn read_after_header(
    read: PolarsResult<DataFrame>,
    header: Option<&[String]>,
) -> PolarsResult<DataFrame> {
    match read {
        Err(PolarsError::NoData(_)) if header.is_some() => Ok(DataFrame::empty()),
        read => read,
    }
}

/// `skipInitialSpace`: the spaces after a delimiter are not part of a text value, so
/// `"   152.6"` is `"152.6"` and a cell of spaces is empty, which reads as null like an
/// empty cell. `nulls` are the null values this column takes, matched after the
/// padding is gone, as they would be against the unpadded file.
pub fn skip_initial_space(
    mut lf: LazyFrame,
    nulls: impl Fn(&str) -> Vec<String>,
) -> PolarsResult<LazyFrame> {
    let schema = lf.collect_schema()?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| **dtype == DataType::String)
        .map(|(name, _)| {
            let stripped = col(name.clone())
                .str()
                .strip_chars_start(lit(PlSmallStr::from_static(" ")));
            let null = nulls(name.as_str())
                .into_iter()
                .fold(stripped.clone().eq(lit("")), |any, value| {
                    any.or(stripped.clone().eq(lit(value)))
                });
            when(null)
                .then(Null {}.lit().cast(DataType::String))
                .otherwise(stripped)
                .alias(name.clone())
        })
        .collect();
    if exprs.is_empty() {
        return Ok(lf);
    }
    Ok(lf.with_columns(exprs))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(text: &str, rows: &[usize], comment: Option<&str>) -> Vec<String> {
        header_names(text.as_bytes(), rows, " ", b',', comment).unwrap()
    }

    #[test]
    fn one_header_line_is_split_and_trimmed() {
        let text = "#info\n  Lcl Date, Lcl Time,     Latitude\n1,2,3\n";
        assert_eq!(
            names(text, &[2], None),
            ["Lcl Date", "Lcl Time", "Latitude"]
        );
    }

    #[test]
    fn several_lines_join_in_the_order_given_and_skip_blank_pieces() {
        let text = "#yyyy-mm-dd, hh:mm:ss, degrees\n  Lcl Date, Lcl Time, Latitude\n";
        assert_eq!(
            names(text, &[2, 1], Some("#")),
            [
                "Lcl Date yyyy-mm-dd",
                "Lcl Time hh:mm:ss",
                "Latitude degrees"
            ]
        );
        let text = "a,,c\nx,y\n";
        assert_eq!(
            header_names(text.as_bytes(), &[1, 2], "_", b',', None).unwrap(),
            ["a_x", "y", "c"]
        );
    }

    #[test]
    fn quotes_bom_and_carriage_returns() {
        let text = "\u{FEFF}id,  \"last, first\",\"say \"\"hi\"\"\"\r\n";
        assert_eq!(names(text, &[1], None), ["id", "last, first", "say \"hi\""]);
    }

    #[test]
    fn a_file_that_ends_before_the_header_is_an_error() {
        let err = header_names("a,b\n".as_bytes(), &[1, 5], " ", b',', None).unwrap_err();
        assert!(err.to_string().contains("past the end"), "{err}");
        assert!(header_names("".as_bytes(), &[1], " ", b',', None).is_err());
        // The last line needs no line break.
        assert_eq!(names("#u\na,b", &[2], None), ["a", "b"]);
    }

    #[test]
    fn a_header_line_is_read_up_to_a_bound() {
        let wide = "x".repeat(MAX_HEADER_LINE as usize + 1);
        let err = header_names(wide.as_bytes(), &[1], " ", b',', None).unwrap_err();
        assert!(err.to_string().contains("header line 1"), "{err}");
        // A line that is not named is passed over however long it is.
        let text = format!("{wide}\na,b\n");
        assert_eq!(names(&text, &[2], None), ["a", "b"]);
    }

    #[test]
    fn shown_names_trim_fill_and_deduplicate() {
        let raw: Vec<PlSmallStr> = ["  a", "a", " ", "b"].map(PlSmallStr::from).to_vec();
        assert_eq!(
            shown_names(&raw, None),
            ["a", "a_duplicated_0", "column_3", "b"]
        );
        let raw: Vec<PlSmallStr> = (1..=4).map(|i| format!("column_{i}").into()).collect();
        let header = ["x".to_string(), String::new(), "column_1".to_string()];
        assert_eq!(
            shown_names(&raw, Some(&header)),
            ["x", "column_2", "column_1", "column_4"]
        );
    }

    #[test]
    fn padding_is_skipped_and_blank_or_null_values_are_null() {
        let df = df!(
            "a" => ["   1.5", "    ", "  NA", " x y "],
            "n" => [1i64, 2, 3, 4],
        )
        .unwrap();
        let out = skip_initial_space(df.lazy(), |_| vec!["NA".into()])
            .unwrap()
            .collect()
            .unwrap();
        let a: Vec<Option<&str>> = out.column("a").unwrap().str().unwrap().iter().collect();
        assert_eq!(a, [Some("1.5"), None, None, Some("x y ")]);
        assert_eq!(out.column("n").unwrap().dtype(), &DataType::Int64);
    }
}
