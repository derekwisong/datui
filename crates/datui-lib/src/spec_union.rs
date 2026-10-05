//! Several files read through a delimited spec as one table: a family of logs whose
//! columns change between writer versions, stacked by column name.
//!
//! Each file's header pass reads on through the lines its scan infers types from (the
//! window, [`crate::csv_dialect::window`]), so a column that is blank there, which the
//! scan can only call text, takes the type the other files give it rather than making
//! the column text in all of them. Units come from the first file that has the column;
//! the notes say which columns not every file has, and where the units disagree.

use std::path::{Path, PathBuf};

use color_eyre::Result;
use polars::prelude::*;

use crate::OpenOptions;
use crate::delimited_spec::Delimited;
use crate::notes::Note;

/// What a file's header pass read: its names, its units, and its window.
pub(crate) struct FileHead {
    pub file: PathBuf,
    /// The names its header lines give, when the spec names header lines.
    pub names: Option<Vec<String>>,
    pub units: Vec<(String, String)>,
    /// The data lines the scan infers types from, split and trimmed.
    pub window: Vec<Vec<String>>,
}

/// The rows Polars infers a CSV's types from when nothing says how many.
const POLARS_INFER_ROWS: usize = 100;

/// The header lines, units and window of the file at `file`, in one pass over its top.
pub(crate) fn read_head(file: &Path, options: &OpenOptions, spec: &Delimited) -> Result<FileHead> {
    use crate::csv_dialect::{named_lines, names_of, skip_lines, window};
    let separator = options.separator_or(b',');
    let comment = options.comment_char.as_deref();
    let rows = options.header_rows();
    let mut wanted: Vec<usize> = rows.unwrap_or_default().to_vec();
    wanted.extend(spec.head_lines());
    wanted.sort_unstable();
    wanted.dedup();
    let mut source = crate::widgets::datatable::DataTableState::text_source(file, None)?;
    let lines = named_lines(&mut source, &wanted)?;
    let names = rows.map(|rows| {
        let picked: Vec<Vec<u8>> = rows
            .iter()
            .map(|row| {
                wanted
                    .iter()
                    .position(|w| w == row)
                    .map_or_else(Vec::new, |i| lines[i].clone())
            })
            .collect();
        names_of(&picked, rows, &options.header_join, separator, comment)
    });
    let units = spec
        .facts_of(&wanted, &lines, separator, &options.header_join)
        .units;
    // On to the data: past the lines skipped beyond the header, and Polars' own header
    // line when the spec names none.
    let read = wanted.last().copied().unwrap_or(0);
    skip_lines(
        &mut source,
        options.skip_lines.unwrap_or(0).saturating_sub(read),
    )?;
    if rows.is_none() {
        window(&mut source, 1, separator, comment)?;
    }
    if let Some(n) = options.skip_rows {
        window(&mut source, n, separator, comment)?;
    }
    let infer = options.infer_schema_length.unwrap_or(POLARS_INFER_ROWS);
    let window = window(&mut source, infer, separator, comment)?;
    Ok(FileHead {
        file: file.to_path_buf(),
        names,
        units,
        window,
    })
}

/// What a column's values in a file's window are, after trimming.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Seen {
    /// Every value blank or a null value: the window says nothing of its type.
    Blank,
    Int,
    Float,
    Text,
}

fn seen(window: &[Vec<String>], at: usize, nulls: &[String]) -> Seen {
    let mut seen = Seen::Blank;
    for row in window {
        let Some(value) = row.get(at) else { continue };
        if value.is_empty() || nulls.iter().any(|n| n == value) {
            continue;
        }
        let this = if value.parse::<i64>().is_ok() {
            Seen::Int
        } else if value.parse::<f64>().is_ok() {
            Seen::Float
        } else {
            return Seen::Text;
        };
        seen = match (seen, this) {
            (Seen::Blank, this) => this,
            (Seen::Int, Seen::Int) => Seen::Int,
            _ => Seen::Float,
        };
    }
    seen
}

/// The type two files' columns take together: the wider of two numbers, text when
/// either is text or they have no common type.
fn wider(a: &DataType, b: &DataType) -> DataType {
    if a == b {
        return a.clone();
    }
    if *a == DataType::String || *b == DataType::String {
        return DataType::String;
    }
    crate::schema_union::widen(a, b).unwrap_or(DataType::String)
}

/// Whether `--infer-types` (`read.infer_types`) types the text column `name`.
fn typed_by_inference(options: &OpenOptions, name: &str) -> bool {
    match &options.parse_strings {
        Some(crate::ParseStringsTarget::All) => true,
        Some(crate::ParseStringsTarget::Columns(columns)) => columns.iter().any(|c| c == name),
        None => false,
    }
}

/// The name a file goes by in a note or an error: its file name.
fn file_name(file: &Path) -> String {
    file.file_name().map_or_else(
        || file.display().to_string(),
        |n| n.to_string_lossy().into_owned(),
    )
}

/// The frames of several files read through a spec, lined up to be stacked by name,
/// and what the read has to say of them.
pub(crate) struct LinedUp {
    pub frames: Vec<LazyFrame>,
    pub notes: Vec<Note>,
    /// Each column's unit, from the first file with the column.
    pub units: Vec<(String, String)>,
}

/// `frames`, one a file of `heads`, each with its columns cast to the type the column
/// takes across the files: the wider of the types the files give it, where a file
/// whose window is blank in the column gives none.
pub(crate) fn line_up(
    mut frames: Vec<LazyFrame>,
    heads: &[FileHead],
    options: &OpenOptions,
) -> Result<LinedUp> {
    let mut schemas = Vec::with_capacity(frames.len());
    for frame in &mut frames {
        schemas.push(frame.collect_schema()?);
    }
    // Every column, in the order the files first have it.
    let mut columns: Vec<PlSmallStr> = Vec::new();
    for schema in &schemas {
        for name in schema.iter_names() {
            if !columns.contains(name) {
                columns.push(name.clone());
            }
        }
    }
    // What each file says of each column it has: its type, or nothing for a column
    // the scan could only call text because its window is blank.
    let says = |file: usize, name: &PlSmallStr| -> Option<Option<DataType>> {
        let schema = &schemas[file];
        let (at, _, dtype) = schema.get_full(name)?;
        if *dtype != DataType::String {
            return Some(Some(dtype.clone()));
        }
        let nulls = crate::widgets::datatable::DataTableState::csv_null_values_for(options, name);
        Some(match seen(&heads[file].window, at, &nulls) {
            Seen::Blank => None,
            Seen::Int if typed_by_inference(options, name) => Some(DataType::Int64),
            Seen::Float if typed_by_inference(options, name) => Some(DataType::Float64),
            _ => Some(DataType::String),
        })
    };
    let mut targets: Vec<(PlSmallStr, DataType)> = Vec::new();
    for name in &columns {
        let target = (0..frames.len())
            .filter_map(|file| says(file, name).flatten())
            .reduce(|a, b| wider(&a, &b));
        if let Some(target) = target.filter(|t| *t != DataType::String) {
            targets.push((name.clone(), target));
        }
    }
    for (file, frame) in frames.iter_mut().enumerate() {
        let schema = &schemas[file];
        let mut casts = Vec::new();
        for (name, target) in &targets {
            let Some(dtype) = schema.get(name) else {
                continue;
            };
            if dtype == target {
                continue;
            }
            if *dtype == DataType::String {
                let nulls =
                    crate::widgets::datatable::DataTableState::csv_null_values_for(options, name);
                casts.push(parsed(
                    name,
                    target.clone(),
                    file_name(&heads[file].file),
                    nulls,
                ));
            } else {
                casts.push(col(name.clone()).cast(target.clone()));
            }
        }
        if !casts.is_empty() {
            *frame = std::mem::take(frame).with_columns(casts);
        }
    }

    let mut notes = Vec::new();
    let missing: Vec<&str> = columns
        .iter()
        .filter(|name| !schemas.iter().all(|s| s.contains(name)))
        .map(PlSmallStr::as_str)
        .collect();
    if !missing.is_empty() {
        notes.push(Note {
            summary: format!(
                "columns not in every file: {}",
                crate::notes::some_names(&missing)
            ),
            scope: format!("by name, across the {} files read", frames.len()),
            read_as_text: None,
            passed_over: None,
        });
    }
    let (units, differ) = units_of(heads);
    if !differ.is_empty() {
        let said: Vec<String> = differ
            .iter()
            .map(|(name, seen)| format!("{name} ({})", seen.join(", ")))
            .collect();
        notes.push(Note {
            summary: format!(
                "units differ across files: {}",
                crate::notes::some_names(&said)
            ),
            scope: "each column shows its unit in the first file that has it".to_string(),
            read_as_text: None,
            passed_over: None,
        });
    }
    Ok(LinedUp {
        frames,
        notes,
        units,
    })
}

/// Each column's unit from the first file that gives it one, and the columns whose
/// files give more than one, with every unit seen.
type Units = Vec<(String, String)>;
fn units_of(heads: &[FileHead]) -> (Units, Vec<(String, Vec<String>)>) {
    let mut units: Units = Vec::new();
    let mut differ: Vec<(String, Vec<String>)> = Vec::new();
    for head in heads {
        for (name, unit) in &head.units {
            match units.iter().find(|(n, _)| n == name) {
                None => units.push((name.clone(), unit.clone())),
                Some((_, first)) if first != unit => {
                    match differ.iter_mut().find(|(n, _)| n == name) {
                        Some((_, seen)) if !seen.contains(unit) => seen.push(unit.clone()),
                        Some(_) => {}
                        None => differ.push((name.clone(), vec![first.clone(), unit.clone()])),
                    }
                }
                Some(_) => {}
            }
        }
    }
    (units, differ)
}

/// The text column `name` read as `to`: trimmed, blank or a null value is null, and a
/// value that is not a `to` stops the read with the file and the column named.
fn parsed(name: &PlSmallStr, to: DataType, file: String, nulls: Vec<String>) -> Expr {
    let column = name.to_string();
    let out = to.clone();
    col(name.clone()).map(
        move |c| {
            let values = c.as_materialized_series().str()?;
            let trimmed = StringChunked::from_iter_options(
                c.name().clone(),
                values.iter().map(|v| {
                    v.map(str::trim)
                        .filter(|v| !v.is_empty() && !nulls.iter().any(|n| n == v))
                }),
            );
            let cast = trimmed.clone().into_series().cast(&to)?;
            if cast.null_count() != trimmed.null_count() {
                let value = trimmed
                    .iter()
                    .zip(cast.iter())
                    .find(|(text, value)| text.is_some() && value.is_null())
                    .and_then(|(text, _)| text)
                    .unwrap_or_default();
                let what = if to.is_primitive_numeric() {
                    "not a number".to_string()
                } else {
                    format!("not {to}")
                };
                polars_bail!(
                    ComputeError: "{file}: {column} holds '{value}', {what}; the other files read it as {to}"
                );
            }
            Ok(cast.into_column())
        },
        move |_, field| Ok(Field::new(field.name().clone(), out.clone())),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rows(lines: &[&[&str]]) -> Vec<Vec<String>> {
        lines
            .iter()
            .map(|r| r.iter().map(|v| v.to_string()).collect())
            .collect()
    }

    #[test]
    fn a_window_says_what_a_column_holds() {
        let window = rows(&[&["", "1", "1.5", "x"], &["NA", "2", "2", ""]]);
        let nulls = ["NA".to_string()];
        assert_eq!(seen(&window, 0, &nulls), Seen::Blank);
        assert_eq!(seen(&window, 1, &nulls), Seen::Int);
        assert_eq!(seen(&window, 2, &nulls), Seen::Float);
        assert_eq!(seen(&window, 3, &nulls), Seen::Text);
        assert_eq!(seen(&window, 9, &nulls), Seen::Blank, "past a short row");
    }

    #[test]
    fn units_come_from_the_first_file_and_disagreements_are_kept() {
        let head = |units: &[(&str, &str)]| FileHead {
            file: PathBuf::new(),
            names: None,
            units: units
                .iter()
                .map(|(n, u)| (n.to_string(), u.to_string()))
                .collect(),
            window: Vec::new(),
        };
        let heads = [
            head(&[("OAT", "deg C"), ("IAS", "kt")]),
            head(&[("OAT", "deg F"), ("volt1", "volts")]),
            head(&[("OAT", "deg F")]),
        ];
        let (units, differ) = units_of(&heads);
        assert_eq!(
            units,
            [
                ("OAT".to_string(), "deg C".to_string()),
                ("IAS".to_string(), "kt".to_string()),
                ("volt1".to_string(), "volts".to_string()),
            ]
        );
        assert_eq!(
            differ,
            [(
                "OAT".to_string(),
                vec!["deg C".to_string(), "deg F".to_string()]
            )]
        );
    }

    #[test]
    fn a_value_that_does_not_parse_names_the_file_and_the_column() {
        let df = df!("Latitude" => ["  40.1", "  ", "N/A"]).unwrap();
        let err = df
            .lazy()
            .select([parsed(
                &"Latitude".into(),
                DataType::Float64,
                "log_x.csv".into(),
                Vec::new(),
            )])
            .collect()
            .unwrap_err()
            .to_string();
        assert!(
            err.contains(
                "log_x.csv: Latitude holds 'N/A', not a number; the other files read it as f64"
            ),
            "{err}"
        );
    }
}
