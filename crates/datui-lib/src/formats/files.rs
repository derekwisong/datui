//! A directory of one spec's files, `{date}/{venue}/trades.bin`: one table, the parts
//! of each file's path as columns.

use super::{Bytes, Opened, Spec, SpecRecords};
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

/// One part's pattern, compiled: its regex, and the part names it captures.
fn component_regex(component: &str) -> Result<(regex::Regex, Vec<String>), String> {
    let mut out = String::from("^");
    let mut names = Vec::new();
    let mut rest = component;
    while let Some(open) = rest.find('{') {
        out.push_str(&regex::escape(&rest[..open]));
        let after = &rest[open + 1..];
        let close = after.find('}').ok_or("a `{` without its `}`")?;
        let inside = &after[..close];
        let name = inside.split_once(':').map_or(inside, |(n, _)| n);
        out.push_str("(.+?)");
        names.push(name.to_string());
        rest = &after[close + 1..];
    }
    out.push_str(&regex::escape(rest));
    out.push('$');
    Ok((regex::Regex::new(&out).map_err(|e| e.to_string())?, names))
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
            match &part.date {
                Some(format) => match chrono::NaiveDate::parse_from_str(text, format) {
                    Ok(date) => {
                        let days = (date
                            - chrono::NaiveDate::from_ymd_opt(1970, 1, 1).expect("a date"))
                        .num_days();
                        values.push(AnyValue::Date(days as i32));
                    }
                    Err(_) => {
                        notes.push(format!(
                            "{shown}: `{text}` is not a date as {format}; left out"
                        ));
                        ok = false;
                        break;
                    }
                },
                None => values.push(AnyValue::StringOwned(text.into())),
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
        .map(|p| {
            (
                PlSmallStr::from(p.name.as_str()),
                if p.date.is_some() {
                    DataType::Date
                } else {
                    DataType::String
                },
            )
        })
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
        let mut exprs: Vec<Expr> = self
            .part_fields
            .iter()
            .zip(&self.parts[i].1)
            .map(|((name, dtype), value)| {
                let value = match value {
                    AnyValue::Date(d) => lit(*d).cast(DataType::Date),
                    AnyValue::StringOwned(s) => lit(s.as_str()),
                    _ => lit(NULL).cast(dtype.clone()),
                };
                value.alias(name.clone())
            })
            .collect();
        exprs.push(all().as_expr());
        lf.select(exprs)
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
