//! A directory of one spec's files, `{date}/{venue}/trades.bin`: one table, the parts
//! of each file's path as columns. A delimited spec's `[files]` adds the same columns to
//! each file it reads ([`dress_file`]).

use super::{Bytes, Files, Opened, PathPart, Spec, SpecRecords};
use polars::prelude::*;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The most files one tree is read from.
const MAX_FILES: usize = 100_000;

/// Each file's records and the values of its path's parts.
pub struct SpecFiles {
    parts: Vec<(Arc<dyn SpecRecords>, Vec<AnyValue<'static>>)>,
    part_fields: Vec<(PlSmallStr, DataType)>,
    /// The first row of each file.
    starts: Vec<usize>,
    rows: usize,
    schema: SchemaRef,
    sources: Vec<Arc<Bytes>>,
}

/// One part's pattern, compiled: its regex, and the part names it captures. A dated
/// part matches what its format writes, so `{game}_{start:%Y-%m-%d}` splits `a_b_2024-01-02`
/// at the date.
fn component_regex(component: &str) -> Result<(regex::Regex, Vec<String>), String> {
    let mut out = String::from("^");
    let mut names = Vec::new();
    let mut rest = component;
    while let Some(open) = rest.find('{') {
        out.push_str(&regex::escape(&rest[..open]));
        let after = &rest[open + 1..];
        let close = after.find('}').ok_or("a `{` without its `}`")?;
        let inside = &after[..close];
        let (name, format) = match inside.split_once(':') {
            Some((name, format)) => (name, Some(format)),
            None => (inside, None),
        };
        out.push('(');
        out.push_str(&format.map_or_else(|| ".+?".to_string(), format_regex));
        out.push(')');
        names.push(name.to_string());
        rest = &after[close + 1..];
    }
    out.push_str(&regex::escape(rest));
    out.push('$');
    Ok((regex::Regex::new(&out).map_err(|e| e.to_string())?, names))
}

/// What a chrono `format` writes, as a regex: digits for its numbers, its literals as
/// they are.
fn format_regex(format: &str) -> String {
    use chrono::format::{Item, StrftimeItems};
    StrftimeItems::new(format)
        .map(|item| match item {
            Item::Literal(s) | Item::Space(s) => regex::escape(s),
            Item::OwnedLiteral(s) | Item::OwnedSpace(s) => regex::escape(&s),
            Item::Numeric(..) => r"\s*\d+".to_string(),
            _ => ".+?".to_string(),
        })
        .collect()
}

impl PathPart {
    /// Whether the part's format reads a time of day as well as a date.
    fn has_time(&self) -> bool {
        use chrono::format::{Item, Numeric, StrftimeItems};
        self.date.as_deref().is_some_and(|format| {
            StrftimeItems::new(format).any(|item| {
                matches!(
                    item,
                    Item::Numeric(
                        Numeric::Hour
                            | Numeric::Hour12
                            | Numeric::Minute
                            | Numeric::Second
                            | Numeric::Timestamp,
                        _
                    )
                )
            })
        })
    }

    /// The part's column type: a datetime when its format has a time, a date when it
    /// has none, text when it has no format.
    pub(crate) fn dtype(&self) -> DataType {
        match &self.date {
            None => DataType::String,
            Some(_) if self.has_time() => DataType::Datetime(TimeUnit::Microseconds, None),
            Some(_) => DataType::Date,
        }
    }

    /// The part's value, from the text of it in a path.
    fn value(&self, text: &str) -> Result<AnyValue<'static>, String> {
        let Some(format) = &self.date else {
            return Ok(AnyValue::StringOwned(text.into()));
        };
        let wrong = || format!("`{text}` is not a date as {format}");
        if self.has_time() {
            let at = chrono::NaiveDateTime::parse_from_str(text, format).map_err(|_| wrong())?;
            Ok(AnyValue::Datetime(
                at.and_utc().timestamp_micros(),
                TimeUnit::Microseconds,
                None,
            ))
        } else {
            let date = chrono::NaiveDate::parse_from_str(text, format).map_err(|_| wrong())?;
            let days =
                (date - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("a date")).num_days();
            Ok(AnyValue::Date(days as i32))
        }
    }
}

/// `lf` with the path's `values` in front, a column each of `fields`.
fn dress(lf: LazyFrame, fields: &[(PlSmallStr, DataType)], values: &[AnyValue]) -> LazyFrame {
    let mut exprs: Vec<Expr> = fields
        .iter()
        .zip(values)
        .map(|((name, dtype), value)| {
            let value = match value {
                AnyValue::Date(d) => lit(*d).cast(DataType::Date),
                AnyValue::Datetime(us, ..) => lit(*us).cast(dtype.clone()),
                AnyValue::StringOwned(s) => lit(s.as_str()),
                _ => lit(NULL).cast(dtype.clone()),
            };
            value.alias(name.clone())
        })
        .collect();
    exprs.push(all().as_expr());
    lf.select(exprs)
}

/// `lf`, the rows of one file a delimited spec reads at `path`, with the parts of its
/// path `files` names in front. The end of the path is matched, a component of it to a
/// component of the pattern. A part that does not match or parse is null, with a note,
/// and one the file has as a column is left to the file's.
pub(crate) fn dress_file(
    files: &Files,
    path: &Path,
    mut lf: LazyFrame,
) -> Result<(LazyFrame, Vec<String>), String> {
    let components: Vec<(regex::Regex, Vec<String>)> = files
        .pattern
        .split('/')
        .map(component_regex)
        .collect::<Result<_, _>>()?;
    let shown = path.file_name().map_or_else(
        || path.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    );
    let mut notes = Vec::new();
    // Absolute, so a pattern with `/` sees the directories of a file opened by a bare
    // name; not canonical, so a link is matched by the name it was opened under.
    let path = std::path::absolute(path).unwrap_or_else(|_| path.to_path_buf());
    let names: Vec<String> = path
        .components()
        .map(|c| c.as_os_str().to_string_lossy().into_owned())
        .collect();
    let mut texts: Vec<PartText> = Vec::new();
    let tail = names
        .len()
        .checked_sub(components.len())
        .map(|at| &names[at..]);
    let matched = tail.is_some_and(|tail| {
        tail.iter().zip(&components).all(|(name, (re, parts))| {
            let Some(caps) = re.captures(name) else {
                return false;
            };
            for (i, part) in parts.iter().enumerate() {
                let text = caps.get(i + 1).map_or("", |m| m.as_str());
                texts.push((part.clone(), text.to_string()));
            }
            true
        })
    });
    if !matched {
        notes.push(format!(
            "{shown}: its path does not match {}; its path columns are empty",
            files.pattern
        ));
        texts.clear();
    }
    let schema = lf.collect_schema().map_err(|e| e.to_string())?;
    let mut fields = Vec::new();
    let mut values = Vec::new();
    for part in &files.parts {
        if schema.contains(&part.name) {
            notes.push(format!(
                "{shown}: `{}` is a column of the file; the path's part is left out",
                part.name
            ));
            continue;
        }
        let value = match texts.iter().find(|(n, _)| *n == part.name) {
            None => AnyValue::Null,
            Some((_, text)) => part.value(text).unwrap_or_else(|e| {
                notes.push(format!("{shown}: {e}; `{}` is empty", part.name));
                AnyValue::Null
            }),
        };
        fields.push((PlSmallStr::from(part.name.as_str()), part.dtype()));
        values.push(value);
    }
    Ok((dress(lf, &fields, &values), notes))
}

/// A part's name and the text it matched in a path.
type PartText = (String, String);

/// The files under `dir` the pattern names, each with its parts' text.
fn matching(dir: &Path, pattern: &str) -> Result<Vec<(PathBuf, Vec<PartText>)>, String> {
    let components: Vec<(regex::Regex, Vec<String>)> = pattern
        .split('/')
        .map(component_regex)
        .collect::<Result<_, _>>()?;
    let mut found = Vec::new();
    let mut stack = vec![(dir.to_path_buf(), 0usize, Vec::<PartText>::new())];
    while let Some((at, depth, parts)) = stack.pop() {
        let Ok(listing) = std::fs::read_dir(&at) else {
            continue;
        };
        let (re, names) = &components[depth];
        let last = depth + 1 == components.len();
        for entry in listing.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            let Some(caps) = re.captures(&name) else {
                continue;
            };
            let mut parts = parts.clone();
            for (i, part) in names.iter().enumerate() {
                parts.push((
                    part.clone(),
                    caps.get(i + 1).map_or("", |m| m.as_str()).to_string(),
                ));
            }
            let path = entry.path();
            if last {
                if path.is_file() {
                    found.push((path, parts));
                    if found.len() > MAX_FILES {
                        return Err(format!("more than {MAX_FILES} files match"));
                    }
                }
            } else if path.is_dir() {
                stack.push((path, depth + 1, parts));
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(found)
}

/// Open the tree of `spec`'s files under `dir`.
pub fn open(spec: &Spec, dir: &Path) -> Result<Opened, String> {
    let files = spec.files.as_ref().expect("a spec of files");
    if !dir.is_dir() {
        return Err(format!(
            "{} reads a directory of its files ({}), and {} is not one",
            spec.name,
            files.pattern,
            dir.display()
        ));
    }
    let mut one = spec.clone();
    one.files = None;
    let mut parts = Vec::new();
    let mut notes = Vec::new();
    let mut header = None;
    let mut sources = Vec::new();
    let mut schema: Option<SchemaRef> = None;
    let found = matching(dir, &files.pattern)?;
    if found.is_empty() {
        return Err(format!(
            "no files under {} match {}",
            dir.display(),
            files.pattern
        ));
    }
    for (path, texts) in found {
        let shown = path
            .strip_prefix(dir)
            .unwrap_or(&path)
            .display()
            .to_string();
        let mut values = Vec::new();
        let mut ok = true;
        for part in &files.parts {
            let text = texts
                .iter()
                .find(|(n, _)| *n == part.name)
                .map_or("", |(_, t)| t.as_str());
            match part.value(text) {
                Ok(value) => values.push(value),
                Err(e) => {
                    notes.push(format!("{shown}: {e}; left out"));
                    ok = false;
                    break;
                }
            }
        }
        if !ok {
            continue;
        }
        let opened = match one.open(&path, &shown) {
            Ok(o) => o,
            Err(e) => {
                notes.push(format!("{shown}: {e}; left out"));
                continue;
            }
        };
        let theirs = opened.records.schema();
        match &schema {
            Some(s) if *s != theirs => {
                notes.push(format!(
                    "{shown}: its columns differ from the first file's; left out"
                ));
                continue;
            }
            Some(_) => {}
            None => schema = Some(theirs),
        }
        notes.extend(opened.notes.into_iter().map(|n| format!("{shown}: {n}")));
        sources.extend(opened.records.sources().iter().cloned());
        if header.is_none() {
            header = Some(opened.header);
        }
        parts.push((opened.records, values));
    }
    let Some(record_schema) = schema else {
        return Err(format!(
            "none of the files under {} could be read",
            dir.display()
        ));
    };
    let part_fields: Vec<(PlSmallStr, DataType)> = files
        .parts
        .iter()
        .map(|p| (PlSmallStr::from(p.name.as_str()), p.dtype()))
        .collect();
    let mut fields: Vec<Field> = part_fields
        .iter()
        .map(|(n, d)| Field::new(n.clone(), d.clone()))
        .collect();
    fields.extend(record_schema.iter_fields());
    let mut starts = Vec::with_capacity(parts.len());
    let mut rows = 0usize;
    for (records, _) in &parts {
        starts.push(rows);
        rows = rows.saturating_add(records.rows());
    }
    let records = SpecFiles {
        parts,
        part_fields,
        starts,
        rows: rows.min(IdxSize::MAX as usize),
        schema: Arc::new(Schema::from_iter(fields)),
        sources,
    };
    Ok(Opened {
        records: Arc::new(records),
        notes,
        header: header.unwrap_or_default(),
    })
}

impl std::fmt::Debug for SpecFiles {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpecFiles")
            .field("files", &self.parts.len())
            .field("rows", &self.rows)
            .finish()
    }
}

impl SpecFiles {
    /// `lf`, the rows of part `i`, with the part's path values in front.
    fn dressed(&self, i: usize, lf: LazyFrame) -> LazyFrame {
        dress(lf, &self.part_fields, &self.parts[i].1)
    }
}

impl crate::formats::pushdown::Windowed for SpecFiles {
    fn window(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let end = start.saturating_add(len).min(self.rows);
        let mut frames = Vec::new();
        let first = self
            .starts
            .partition_point(|s| *s <= start)
            .saturating_sub(1);
        for i in first..self.parts.len() {
            let begin = self.starts[i];
            if begin >= end {
                break;
            }
            let rows = self.parts[i].0.rows();
            let from = start.saturating_sub(begin).min(rows);
            let take = (end - begin).min(rows) - from;
            if take == 0 {
                continue;
            }
            frames.push(self.dressed(i, self.parts[i].0.window(from, take)?));
        }
        if frames.is_empty() {
            return Ok(DataFrame::empty_with_schema(&self.schema).lazy());
        }
        concat(frames, UnionArgs::default())
    }
}

impl SpecRecords for SpecFiles {
    fn rows(&self) -> usize {
        self.rows
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn into_lazy(self: Arc<Self>) -> PolarsResult<LazyFrame> {
        let frames = (0..self.parts.len())
            .map(|i| Ok(self.dressed(i, self.parts[i].0.clone().into_lazy()?)))
            .collect::<PolarsResult<Vec<_>>>()?;
        concat(frames, UnionArgs::default())
    }
    fn collect(&self, rows: usize) -> PolarsResult<DataFrame> {
        crate::formats::pushdown::Windowed::window(self, 0, rows)?.collect()
    }
    fn sources(&self) -> &[Arc<Bytes>] {
        &self.sources
    }
}
