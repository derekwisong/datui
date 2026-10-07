//! `T` at the table: the other tables of the source on screen, to open one in its
//! place. Listed from what the open already holds (a workbook's or a database's tab of
//! the Info panel, a spec's record types, a cache's splits), so nothing is read to list
//! them; opening one is an open of the file with `--table`, as home's row for it is.

use std::path::PathBuf;

use crate::OpenOptions;
use crate::table::DataTableState;

/// One table of the source, as the picker lists it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Table {
    /// What `--table` names; `None` for the whole file (every record type).
    pub table: Option<String>,
    /// The picker's line.
    pub label: String,
    /// What is cheap to say of it: a sheet's range and size, a table's columns.
    pub detail: String,
}

/// The tables of the source on screen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Tables {
    /// The file (or a cache's directory) the tables are in.
    pub file: PathBuf,
    pub tables: Vec<Table>,
    /// The one on screen.
    pub current: Option<usize>,
}

impl Tables {
    /// Whether there is another to switch to.
    pub fn several(&self) -> bool {
        self.tables.len() > 1
    }
}

/// The tables of the dataset `state` opened from `paths` with `options`: `None` for a
/// source of one table, or several files.
pub fn of(state: &DataTableState, paths: &[PathBuf], options: &OpenOptions) -> Option<Tables> {
    let [file] = paths else {
        return None;
    };
    let file = file.clone();
    if let Some(detail) = state.format_detail()
        && !detail.tables.is_empty()
    {
        return Some(from_detail(file, detail, options.table.as_deref()));
    }
    if let Some(read) = state.format_read()
        && !read.spec.is_delimited()
        && read.spec.records.variants.len() > 1
    {
        return Some(record_types(file, &read.spec));
    }
    let splits = options.splits.as_deref()?;
    let current = splits.split.clone()?;
    if splits.others.is_empty() {
        return None;
    }
    let mut names: Vec<String> = splits.others.clone();
    names.push(current.clone());
    names.sort();
    names.dedup();
    let at = names.iter().position(|n| *n == current);
    Some(Tables {
        file,
        tables: names
            .into_iter()
            .map(|name| Table {
                table: Some(name.clone()),
                label: name,
                detail: "split".to_string(),
            })
            .collect(),
        current: at,
    })
}

/// Whether [`of`] would list more than one table, without listing them: asked by the
/// footer each frame.
pub fn several(state: &DataTableState, paths: &[PathBuf], options: &OpenOptions) -> bool {
    if paths.len() != 1 {
        return false;
    }
    if let Some(detail) = state.format_detail()
        && !detail.tables.is_empty()
    {
        return detail.tables.len() > 1;
    }
    if let Some(read) = state.format_read()
        && !read.spec.is_delimited()
        && read.spec.records.variants.len() > 1
    {
        return true;
    }
    options
        .splits
        .as_deref()
        .is_some_and(|s| s.split.is_some() && s.others.iter().any(|o| Some(o) != s.split.as_ref()))
}

/// A workbook's worksheets or a database's tables, as its tab of the Info panel lists
/// them: what that says of each, less that it is the one opened, which the picker marks.
fn from_detail(file: PathBuf, detail: &crate::text_formats::Detail, asked: Option<&str>) -> Tables {
    let said = |name: &str| {
        detail
            .list
            .iter()
            .find(|(key, _)| key == name)
            .map(|(_, value)| match value {
                crate::model_files::MetaValue::Text(text) => text
                    .split(", ")
                    .filter(|part| *part != "opened")
                    .collect::<Vec<_>>()
                    .join(", "),
                _ => String::new(),
            })
            .unwrap_or_default()
    };
    let opened = detail.table.as_deref().or(asked);
    let current = opened.and_then(|name| detail.tables.iter().position(|t| t == name));
    Tables {
        file,
        tables: detail
            .tables
            .iter()
            .map(|name| Table {
                table: Some(name.clone()),
                label: name.clone(),
                detail: said(name),
            })
            .collect(),
        current,
    }
}

/// A format spec's record types, after the whole file (every type, a `type` column
/// saying which).
fn record_types(file: PathBuf, spec: &crate::formats::Spec) -> Tables {
    let current = spec.variant.as_deref();
    let whole = Table {
        table: None,
        label: file
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| file.display().to_string()),
        detail: "every record type".to_string(),
    };
    let types = crate::members::variant_tables(spec)
        .into_iter()
        .map(|t| Table {
            detail: crate::text_formats::count(t.columns.len() as u64, "column", "columns"),
            table: Some(t.name.clone()),
            label: t.name,
        });
    let tables: Vec<Table> = std::iter::once(whole).chain(types).collect();
    let at = tables.iter().position(|t| t.table.as_deref() == current);
    Tables {
        file,
        tables,
        current: at,
    }
}
