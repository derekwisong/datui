//! Format specs of `kind = "delimited"`: the reading options for a family of CSV-like
//! files, with header rows that have roles (names, units), a metadata line, and a few
//! derived columns.
//!
//! A spec is parsed in [`crate::formats`], beside the binary specs, and matched by the
//! same `match`. Reading it is the CSV reader's: [`Delimited::apply`] sets the
//! dialect options a matched file is opened with, and [`Delimited::derive`] adds the
//! derived columns to the frame. The only lines read apart from the scan are the
//! header lines and the metadata line ([`Delimited::facts`]).

use crate::formats::{Chosen, Spec};
use polars::prelude::*;
use std::io::BufRead;
use std::sync::Arc;

/// What the lines before the data hold, by role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeaderRows {
    /// The 1-based lines whose pieces, joined, name the columns.
    pub name: Vec<usize>,
    /// The line that gives each column's unit.
    pub unit: Option<usize>,
}

impl HeaderRows {
    /// The last header line: the data starts after it.
    pub fn last(&self) -> usize {
        self.name
            .iter()
            .copied()
            .chain(self.unit)
            .max()
            .unwrap_or(0)
    }
}

pub use crate::column_types::{Derived, DerivedKind};

/// A delimited spec's reading options. Each one left out keeps what the command line
/// or the config says.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Delimited {
    pub delimiter: Option<u8>,
    pub comment_char: Option<String>,
    pub skip_initial_space: Option<bool>,
    pub header_rows: Option<HeaderRows>,
    pub header_join: Option<String>,
    pub metadata_line: Option<usize>,
    pub null_values: Vec<String>,
    pub skip_lines: Option<usize>,
    pub columns: Vec<Derived>,
    /// Columns of the file read as a declared type, by name.
    pub types: Vec<(String, crate::column_types::ColumnType)>,
}

/// The `key="value"` line at the top of a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metadata {
    /// The line as it is in the file, without its comment prefix and line break.
    pub raw: String,
    /// The leading item with no `=`, such as `device_info` in `#device_info, a="1"`.
    pub title: Option<String>,
    /// Key and value, in the order the line has them. Empty when the line does not
    /// parse, and then `raw` is shown.
    pub pairs: Vec<(String, String)>,
}

/// What a file's header lines say besides its column names.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeadFacts {
    /// Each column's unit, by the name the column is shown with.
    pub units: Vec<(String, String)>,
    pub metadata: Option<Metadata>,
}

/// What a read through a delimited spec found, as the open carries it to the dataset.
#[derive(Debug, Clone)]
pub struct DelimitedRead {
    pub spec: Arc<Spec>,
    pub by: Chosen,
    /// The other specs that matched as well as `spec`, by the same rule.
    pub also: Vec<String>,
    /// Each column's unit, by the name the column is shown with.
    pub units: Vec<(String, String)>,
    pub metadata: Option<Metadata>,
    /// The file the header lines were read from, when more than one file was read.
    pub facts_from: Option<String>,
}

impl DelimitedRead {
    /// The read before its header lines are read: the spec and why.
    pub fn chosen(spec: Arc<Spec>, by: Chosen, also: Vec<String>) -> Self {
        Self {
            spec,
            by,
            also,
            units: Vec::new(),
            metadata: None,
            facts_from: None,
        }
    }

    pub fn delimited(&self) -> &Delimited {
        self.spec
            .delimited
            .as_deref()
            .expect("a delimited read has a delimited spec")
    }

    /// The unit of the column shown as `column`.
    pub fn unit_of(&self, column: &str) -> Option<&str> {
        self.units
            .iter()
            .find(|(name, _)| name == column)
            .map(|(_, unit)| unit.as_str())
    }

    /// The dataset's notes about the read: which spec read it and why, and what else
    /// matched.
    pub fn notes(&self) -> Vec<crate::notes::Note> {
        let note = |summary: String, scope: String| crate::notes::Note {
            summary,
            scope,
            read_as_text: None,
            passed_over: None,
        };
        let from = self
            .spec
            .path
            .as_ref()
            .map_or_else(|| "the spec".to_string(), |p| p.display().to_string());
        let mut notes = vec![note(
            format!(
                "read as {}, {}",
                self.spec.name,
                crate::formats::chosen_words(&self.spec, self.by)
            ),
            format!("from {from}"),
        )];
        if !self.also.is_empty() {
            notes.push(note(
                format!(
                    "{} also {} this file",
                    self.also.join(", "),
                    if self.also.len() == 1 {
                        "matches"
                    } else {
                        "match"
                    }
                ),
                format!("by {}", self.by.words()),
            ));
        }
        notes
    }
}

/// The most lines [`Delimited::facts`] reads: the header lines and the metadata line
/// sit at the top of a file.
pub const MAX_HEAD_LINE: usize = 1000;

impl Delimited {
    /// `options` with the spec's dialect: each option the spec gives replaces what the
    /// config said, but not a flag typed on the command line (#651), and the file is
    /// read as CSV unless its name says TSV or PSV.
    pub fn apply(&self, options: &mut crate::OpenOptions) {
        let typed = options.typed_dialect;
        if let Some(d) = self.delimiter.filter(|_| !typed.delimiter) {
            options.delimiter = Some(d);
        }
        if let Some(c) = self.comment_char.as_ref().filter(|_| !typed.comment_char) {
            options.comment_char = Some(c.clone());
        }
        if let Some(s) = self
            .skip_initial_space
            .filter(|_| !typed.skip_initial_space)
        {
            options.skip_initial_space = s;
        }
        if let Some(rows) = self.header_rows.as_ref().filter(|_| !typed.header_rows) {
            options.header_rows = rows.name.clone();
            // The unit line is a header line too: the data starts after the last one.
            options.skip_lines = Some(options.skip_lines.unwrap_or(0).max(rows.last()));
        }
        if let Some(join) = &self.header_join {
            options.header_join = join.clone();
        }
        if let Some(n) = self.skip_lines.filter(|_| !typed.skip_lines) {
            options.skip_lines = Some(options.skip_lines.unwrap_or(0).max(n));
        }
        if !self.null_values.is_empty() {
            // Applied again to a read again's options, so each value once.
            let values = options.null_values.get_or_insert_with(Vec::new);
            for value in &self.null_values {
                if !values.contains(value) {
                    values.push(value.clone());
                }
            }
        }
        if options
            .format
            .is_none_or(|f| crate::FileFormat::separator(f).is_none())
        {
            options.format = Some(crate::FileFormat::Csv);
        }
    }

    /// The lines [`Self::facts`] reads, 1-based.
    pub fn head_lines(&self) -> Vec<usize> {
        let mut lines: Vec<usize> = self
            .header_rows
            .iter()
            .flat_map(|rows| rows.name.iter().copied().chain(rows.unit))
            .chain(self.metadata_line)
            .collect();
        lines.sort_unstable();
        lines.dedup();
        lines
    }

    /// The units and the metadata from the top of the text `source` holds, split on
    /// `separator`. Only the lines the spec names are read.
    pub fn facts(
        &self,
        source: impl BufRead,
        separator: u8,
        join: &str,
    ) -> color_eyre::Result<HeadFacts> {
        let wanted = self.head_lines();
        if wanted.is_empty() {
            return Ok(HeadFacts::default());
        }
        let lines = crate::csv_dialect::named_lines(source, &wanted)?;
        Ok(self.facts_of(&wanted, &lines, separator, join))
    }

    /// [`Self::facts`] from `lines`, the lines `wanted` names as
    /// [`crate::csv_dialect::named_lines`] read them; a line not among them is blank.
    pub fn facts_of(
        &self,
        wanted: &[usize],
        lines: &[Vec<u8>],
        separator: u8,
        join: &str,
    ) -> HeadFacts {
        let line = |n: usize| -> &[u8] {
            wanted
                .iter()
                .position(|&w| w == n)
                .map_or(&[][..], |i| lines[i].as_slice())
        };
        let comment = self.comment_char.as_deref();
        let mut units = Vec::new();
        if let Some(rows) = &self.header_rows
            && let Some(unit) = rows.unit
        {
            let mut pieces: Vec<Vec<String>> = Vec::new();
            for &row in &rows.name {
                for (i, field) in
                    crate::csv_dialect::header_fields(line(row), row, separator, comment)
                        .into_iter()
                        .enumerate()
                {
                    if pieces.len() <= i {
                        pieces.resize_with(i + 1, Vec::new);
                    }
                    if !field.is_empty() {
                        pieces[i].push(field);
                    }
                }
            }
            let names: Vec<String> = pieces.into_iter().map(|p| p.join(join)).collect();
            let raw: Vec<PlSmallStr> = (1..=names.len())
                .map(|i| format!("column_{i}").into())
                .collect();
            let shown = crate::csv_dialect::shown_names(&raw, Some(&names));
            let unit_fields =
                crate::csv_dialect::header_fields(line(unit), unit, separator, comment);
            for (name, unit) in shown.into_iter().zip(unit_fields) {
                // A derived column of the same name takes the column's place, and the
                // unit was the text's.
                if !unit.is_empty() && !self.columns.iter().any(|d| d.name == name) {
                    units.push((name, unit));
                }
            }
        }
        let metadata = self.metadata_line.map(|n| {
            let mut text = line(n);
            if n == 1 {
                text = text.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(text);
            }
            let text = String::from_utf8_lossy(text);
            let text = text.trim_end_matches(['\n', '\r']);
            let text = comment.and_then(|c| text.strip_prefix(c)).unwrap_or(text);
            parse_metadata(text)
        });
        HeadFacts { units, metadata }
    }

    /// `lf` with the derived columns, each before the first column it is made from.
    /// Lazy: nothing is read.
    pub fn derive(&self, mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
        if self.columns.is_empty() {
            return Ok(lf);
        }
        let schema = lf.collect_schema()?;
        let mut order: Vec<PlSmallStr> = schema.iter_names().cloned().collect();
        let mut exprs = Vec::with_capacity(self.columns.len());
        for derived in &self.columns {
            for from in &derived.from {
                if !schema.contains(from) {
                    polars_bail!(
                        ColumnNotFound: "\"{}\" is made from \"{from}\", which the file has no column of",
                        derived.name
                    );
                }
            }
            let name = PlSmallStr::from(derived.name.as_str());
            if !order.contains(&name) {
                let at = order
                    .iter()
                    .position(|c| c.as_str() == derived.from[0])
                    .unwrap_or(order.len());
                order.insert(at, name.clone());
            }
            exprs.push(derived.expr().alias(name));
        }
        Ok(lf
            .with_columns(exprs)
            .select(order.into_iter().map(col).collect::<Vec<_>>()))
    }
}

/// `read` with the units and metadata from the header lines of the first of
/// `paths`, read in `options`' dialect.
pub fn read_facts(
    read: &DelimitedRead,
    paths: &[std::path::PathBuf],
    options: &crate::OpenOptions,
) -> color_eyre::Result<DelimitedRead> {
    let separator = options.separator_or(
        options
            .format
            .and_then(crate::FileFormat::separator)
            .unwrap_or(b','),
    );
    let facts_of = |file: &std::path::Path| -> color_eyre::Result<HeadFacts> {
        let compression = options
            .compression
            .or_else(|| crate::CompressionFormat::from_extension(file));
        let source = crate::readers::csv::text_source(file, compression)
            .map_err(|e| crate::error_display::in_file(file, e.into()))?;
        read.delimited()
            .facts(source, separator, &options.header_join)
            .map_err(|e| crate::error_display::in_file(file, e))
    };
    // From the first file with a header: of several, one with nothing in it is
    // passed over by the read too.
    let mut found = None;
    for file in paths {
        match facts_of(file) {
            Err(e) if paths.len() > 1 && crate::csv_dialect::is_blank_file(&e) => continue,
            facts => {
                found = Some((file, facts?));
                break;
            }
        }
    }
    let Some((file, HeadFacts { units, metadata })) = found else {
        return Ok(read.clone());
    };
    Ok(DelimitedRead {
        units,
        metadata,
        facts_from: (paths.len() > 1).then(|| {
            file.file_name().map_or_else(
                || file.display().to_string(),
                |n| n.to_string_lossy().into_owned(),
            )
        }),
        ..read.clone()
    })
}

/// What `datui formats check` prints for a delimited spec and, given `file`, the
/// file's metadata, units and first `rows` rows as read with `base`, the command
/// line's options, whose typed dialect flags win over the spec's.
pub fn check(
    spec: &Arc<Spec>,
    file: Option<&std::path::Path>,
    rows: usize,
    base: &crate::OpenOptions,
) -> Result<String, String> {
    let delimited = spec
        .delimited
        .as_deref()
        .ok_or_else(|| format!("error: {} is not a delimited spec\n", spec.name))?;
    let mut out = String::new();
    out.push_str(&format!("  delimited: {}\n", delimited.summary()));
    for derived in &delimited.columns {
        out.push_str(&format!(
            "  {} = {} from {}\n",
            derived.name,
            derived.kind.name(),
            derived.from.join(", ")
        ));
    }
    for (name, ty) in &delimited.types {
        let format = ty
            .format
            .as_ref()
            .map_or_else(String::new, |f| format!(", format {f:?}"));
        out.push_str(&format!("  {name}: {}{format}\n", ty.name()));
    }
    let typed = typed_summary(base);
    if !typed.is_empty() {
        out.push_str(&format!("  command line, over the spec: {typed}\n"));
    }
    let Some(file) = file else {
        return Ok(out);
    };
    let fail = |out: &str, e: &color_eyre::Report| {
        let said = crate::error_display::user_message_from_report(e, Some(file));
        format!("{out}error: {said}\n")
    };
    let mut options = crate::OpenOptions {
        format: crate::FileFormat::from_path(file),
        ..base.clone()
    };
    delimited.apply(&mut options);
    let chosen = DelimitedRead::chosen(spec.clone(), Chosen::SpecFile, Vec::new());
    options.delimited = Some(Arc::new(chosen.clone()));
    let read = read_facts(&chosen, &[file.to_path_buf()], &options).map_err(|e| fail(&out, &e))?;
    if let Some(metadata) = &read.metadata {
        if metadata.pairs.is_empty() {
            out.push_str(&format!(
                "metadata (not key=value pairs): {}\n",
                metadata.raw
            ));
        } else {
            let pairs: Vec<String> = metadata
                .pairs
                .iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect();
            let title = metadata
                .title
                .as_ref()
                .map_or_else(String::new, |t| format!("{t}: "));
            out.push_str(&format!("metadata: {title}{}\n", pairs.join(", ")));
        }
    }
    if !read.units.is_empty() {
        let units: Vec<String> = read
            .units
            .iter()
            .map(|(column, unit)| format!("{column} = {unit}"))
            .collect();
        out.push_str(&format!("units: {}\n", units.join(", ")));
    }
    let separator = options.separator_or(b',');
    let read = crate::readers::csv::read_delimited(file, separator, &options, &Default::default())
        .map_err(|e| fail(&out, &crate::error_display::in_file(file, e)))?;
    let df = read
        .lf
        .limit(rows as IdxSize)
        .collect()
        .map_err(|e| fail(&out, &crate::error_display::in_file(file, e.into())))?;
    out.push_str(&crate::formats::text_table(&df));
    Ok(out)
}

/// The dialect flags typed on the command line, in a line: `delimiter ','`.
fn typed_summary(options: &crate::OpenOptions) -> String {
    let typed = options.typed_dialect;
    let mut said = Vec::new();
    if let Some(d) = options.delimiter.filter(|_| typed.delimiter) {
        said.push(format!("delimiter {:?}", d as char));
    }
    if let Some(c) = options.comment_char.as_ref().filter(|_| typed.comment_char) {
        said.push(format!("comment {c:?}"));
    }
    if typed.skip_initial_space {
        said.push(format!("skip initial space {}", options.skip_initial_space));
    }
    if typed.header_rows {
        let rows: Vec<String> = options.header_rows.iter().map(usize::to_string).collect();
        said.push(format!("header rows {}", rows.join(",")));
    }
    if let Some(n) = options.skip_lines.filter(|_| typed.skip_lines) {
        said.push(format!("skip lines {n}"));
    }
    said.join(", ")
}

impl Delimited {
    /// The options the spec sets, in a line: `header line 3, unit line 2, ...`.
    pub fn summary(&self) -> String {
        let lines = |rows: &[usize]| {
            let rows: Vec<String> = rows.iter().map(usize::to_string).collect();
            rows.join(" + ")
        };
        let mut said = Vec::new();
        if let Some(d) = self.delimiter {
            said.push(format!("delimiter {:?}", d as char));
        }
        if let Some(rows) = &self.header_rows {
            said.push(format!("names on line {}", lines(&rows.name)));
            if let Some(unit) = rows.unit {
                said.push(format!("units on line {unit}"));
            }
        }
        if let Some(n) = self.metadata_line {
            said.push(format!("metadata on line {n}"));
        }
        if let Some(c) = &self.comment_char {
            said.push(format!("comments start {c:?}"));
        }
        if self.skip_initial_space == Some(true) {
            said.push("skip initial space".to_string());
        }
        if let Some(n) = self.skip_lines {
            said.push(format!("skip {n} lines"));
        }
        if !self.null_values.is_empty() {
            said.push(format!("null {}", self.null_values.join(", ")));
        }
        if said.is_empty() {
            "CSV with a header line".to_string()
        } else {
            said.join(", ")
        }
    }
}

/// `name, key="value", key=value`: the items separated by commas outside quotes. A
/// first item with no `=` is the title. A line with anything else, or with no pairs,
/// is kept raw only.
pub fn parse_metadata(line: &str) -> Metadata {
    let raw = line.trim().to_string();
    let mut items = Vec::new();
    let mut item = String::new();
    let mut quoted = false;
    for c in line.chars() {
        match c {
            '"' => {
                quoted = !quoted;
                item.push(c);
            }
            ',' if !quoted => items.push(std::mem::take(&mut item)),
            c => item.push(c),
        }
    }
    items.push(item);
    let unparsed = |raw: String| Metadata {
        raw,
        title: None,
        pairs: Vec::new(),
    };
    if quoted {
        return unparsed(raw);
    }
    let mut title = None;
    let mut pairs = Vec::new();
    for (i, item) in items.iter().map(|s| s.trim()).enumerate() {
        if item.is_empty() {
            continue;
        }
        match item.split_once('=') {
            Some((key, value)) => {
                let key = key.trim();
                if key.is_empty() || key.contains('"') {
                    return unparsed(raw);
                }
                pairs.push((key.to_string(), unquote(value.trim())));
            }
            None if i == 0 && !item.contains('"') => title = Some(item.to_string()),
            None => return unparsed(raw),
        }
    }
    if pairs.is_empty() {
        return unparsed(raw);
    }
    Metadata { raw, title, pairs }
}

/// `"a ""b"""` is `a "b"`; a value with no quotes around it is itself.
fn unquote(value: &str) -> String {
    match value.strip_prefix('"').and_then(|v| v.strip_suffix('"')) {
        Some(inner) => inner.replace("\"\"", "\""),
        None => value.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_metadata_line_parses_into_pairs_and_a_title() {
        let m = parse_metadata(r#"device_info, log_version="1.03", model="X, Y", serial=123,"#);
        assert_eq!(m.title.as_deref(), Some("device_info"));
        assert_eq!(
            m.pairs,
            [
                ("log_version".to_string(), "1.03".to_string()),
                ("model".to_string(), "X, Y".to_string()),
                ("serial".to_string(), "123".to_string()),
            ]
        );
    }

    #[test]
    fn a_line_that_does_not_parse_is_kept_raw() {
        for line in [
            "just some words",
            "a=1, stray",
            r#"a="open"#,
            "=1",
            "title only",
        ] {
            let m = parse_metadata(line);
            assert!(m.pairs.is_empty(), "{line}: {m:?}");
            assert_eq!(m.raw, line.trim());
        }
    }

    fn spec() -> Delimited {
        Delimited {
            comment_char: Some("#".into()),
            header_rows: Some(HeaderRows {
                name: vec![3],
                unit: Some(2),
            }),
            metadata_line: Some(1),
            ..Default::default()
        }
    }

    #[test]
    fn units_follow_the_shown_names_and_the_metadata_line_is_read() {
        let text = "#device_info, version=\"2\"\n#yyyy-mm-dd, hh:mm, volts,\n  Date, Time,  Volts, Volts,\n";
        let HeadFacts { units, metadata } = spec().facts(text.as_bytes(), b',', " ").unwrap();
        assert_eq!(
            units,
            [
                ("Date".to_string(), "yyyy-mm-dd".to_string()),
                ("Time".to_string(), "hh:mm".to_string()),
                ("Volts".to_string(), "volts".to_string()),
            ]
        );
        let metadata = metadata.unwrap();
        assert_eq!(metadata.title.as_deref(), Some("device_info"));
        assert_eq!(metadata.pairs, [("version".to_string(), "2".to_string())]);
    }

    #[test]
    fn a_derived_column_that_takes_a_name_does_not_take_its_unit() {
        let text = "#m=1\nyyyy-mm-dd, hh:mm\nDate, Time\n";
        let spec = Delimited {
            columns: vec![Derived {
                name: "Time".into(),
                from: vec!["Date".into(), "Time".into()],
                kind: DerivedKind::Datetime,
                format: None,
            }],
            ..spec()
        };
        let facts = spec.facts(text.as_bytes(), b',', " ").unwrap();
        assert_eq!(
            facts.units,
            [("Date".to_string(), "yyyy-mm-dd".to_string())]
        );
    }

    #[test]
    fn check_names_the_file_and_why_it_could_not_be_read() {
        let dir = tempfile::tempdir().unwrap();
        let short = dir.path().join("short.csv");
        std::fs::write(&short, "a,b\n1,2\n").unwrap();
        let text = "name = \"a.log\"\nkind = \"delimited\"\nheader_rows = 3";
        let spec = Arc::new(Spec::parse(text, None).unwrap());
        for (file, said) in [
            (
                short,
                "short.csv\": Header line 3 is past the end of the file.",
            ),
            (
                dir.path().join("none.csv"),
                "none.csv\": File or directory not found.",
            ),
        ] {
            let e = check(&spec, Some(&file), 5, &crate::OpenOptions::default()).unwrap_err();
            assert!(e.contains(said), "{e}");
        }
    }

    /// A flag typed on the command line keeps its value; the rest come from the spec
    /// (#651).
    #[test]
    fn apply_leaves_typed_flags() {
        let mut options = crate::OpenOptions {
            delimiter: Some(b','),
            comment_char: Some("%".into()),
            typed_dialect: crate::TypedDialect {
                delimiter: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let spec = Delimited {
            delimiter: Some(b';'),
            ..spec()
        };
        spec.apply(&mut options);
        assert_eq!(options.delimiter, Some(b','), "typed");
        assert_eq!(options.comment_char.as_deref(), Some("#"), "not typed");
    }

    #[test]
    fn apply_sets_the_dialect_and_skips_every_header_line() {
        let mut options = crate::OpenOptions::default();
        let spec = Delimited {
            header_rows: Some(HeaderRows {
                name: vec![2],
                unit: Some(3),
            }),
            null_values: vec!["NA".into()],
            ..spec()
        };
        spec.apply(&mut options);
        assert_eq!(options.header_rows, [2]);
        assert_eq!(options.skip_lines, Some(3));
        assert_eq!(options.comment_char.as_deref(), Some("#"));
        assert_eq!(options.null_values, Some(vec!["NA".to_string()]));
        assert_eq!(options.format, Some(crate::FileFormat::Csv));
    }

    #[test]
    fn a_datetime_from_date_time_and_offset_is_utc() {
        let df = df!(
            "d" => [Some("2024-03-05"), None, Some("2024-03-05")],
            "t" => [Some("14:03:22"), None, Some("23:30:00")],
            "o" => [Some("-05:00"), None, Some("+0530")],
            "v" => [1, 2, 3],
        )
        .unwrap();
        let spec = Delimited {
            columns: vec![Derived {
                name: "time".into(),
                from: vec!["d".into(), "t".into(), "o".into()],
                kind: DerivedKind::Datetime,
                format: None,
            }],
            ..Default::default()
        };
        let out = spec.derive(df.lazy()).unwrap().collect().unwrap();
        assert_eq!(
            out.get_column_names()
                .iter()
                .map(|n| n.as_str())
                .collect::<Vec<_>>(),
            ["time", "d", "t", "o", "v"],
            "before its first source"
        );
        let time = out.column("time").unwrap();
        assert_eq!(
            time.dtype(),
            &DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC))
        );
        let shown: Vec<String> = (0..3).map(|i| time.get(i).unwrap().to_string()).collect();
        assert_eq!(shown[0], "2024-03-05 19:03:22 UTC");
        assert_eq!(shown[1], "null");
        assert_eq!(shown[2], "2024-03-05 18:00:00 UTC");
    }

    /// Every way a delimited spec or the file it reads is refused names the file, in
    /// the one shape; a spec's problem with its line and column.
    #[test]
    fn errors_name_the_file() {
        use crate::readers::bad_input::assert_shape;
        let dir = tempfile::tempdir().unwrap();
        let spec_path = dir.path().join("log.toml");
        let text = "name = \"a.log\"\nkind = \"delimited\"\nheader_rows = \"three\"\n";
        let e = Spec::parse(text, Some(&spec_path)).unwrap_err().to_string();
        eprintln!("{e}");
        assert_shape(&e, &spec_path);
        assert!(e.contains(":3:"), "{e}");

        let data = dir.path().join("a.csv");
        std::fs::write(&data, "a\n1\n").unwrap();
        let spec = Delimited {
            columns: vec![Derived {
                name: "day".into(),
                from: vec!["date".into()],
                kind: DerivedKind::Date,
                format: None,
            }],
            ..Default::default()
        };
        let lf = LazyCsvReader::new(PlRefPath::new(data.to_string_lossy().as_ref()))
            .finish()
            .unwrap();
        let Err(e) = spec.derive(lf) else {
            panic!("a missing column is an error");
        };
        let e = crate::error_display::user_message_from_report(&e.into(), Some(&data));
        eprintln!("{e}");
        assert_shape(&e, &data);
        assert!(e.contains("\"date\""), "{e}");

        let text = "name = \"a.log\"\nkind = \"delimited\"\nheader_rows = 3";
        let spec = Arc::new(Spec::parse(text, None).unwrap());
        let read = DelimitedRead::chosen(spec, Chosen::SpecFile, Vec::new());
        for file in [data.clone(), dir.path().join("none.csv")] {
            let e = read_facts(
                &read,
                std::slice::from_ref(&file),
                &crate::OpenOptions::default(),
            )
            .unwrap_err();
            let e = crate::error_display::user_message_from_report(&e, Some(&file));
            eprintln!("{e}");
            assert_shape(&e, &file);
        }
    }

    #[test]
    fn a_missing_source_column_is_named() {
        let df = df!("a" => [1]).unwrap();
        let spec = Delimited {
            columns: vec![Derived {
                name: "day".into(),
                from: vec!["date".into()],
                kind: DerivedKind::Date,
                format: None,
            }],
            ..Default::default()
        };
        let Err(err) = spec.derive(df.lazy()) else {
            panic!("a missing column is an error");
        };
        let err = err.to_string();
        assert!(
            err.contains("\"day\" is made from \"date\", which the file has no column of"),
            "{err}"
        );
    }
}
