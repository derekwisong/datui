//! Several files read through a delimited spec as one table: a family of logs whose
//! columns change between writer versions, stacked by column name.
//!
//! Each file's header pass ([`crate::formats::csv_dialect::head`]) reads on through the
//! lines its scan infers types from (the window), so a column that is blank there, which the
//! scan can only call text, takes the type the other files give it rather than making
//! the column text in all of them. Units come from the first file that has the column;
//! the notes say which columns not every file has, and where the units disagree.

use std::path::Path;

use color_eyre::Result;
use polars::prelude::*;

use crate::OpenOptions;
use crate::formats::csv_dialect::FileHead;
use crate::notes::Note;

/// What the files' windows say of the text column `name`, at `at` in each row, as the
/// read's string inference ([`infer_string_type`]) would type it: `None` when every
/// value is blank or a null value, so the window says nothing of its type.
///
/// [`infer_string_type`]: crate::formats::readers::csv::infer_string_type
fn seen(
    window: &[Vec<String>],
    at: usize,
    name: &str,
    options: &OpenOptions,
) -> Result<Option<DataType>> {
    let nulls = crate::formats::readers::csv::csv_null_values_for(options, name);
    // As the read samples them: trimmed (the window already is), blanks null.
    let values = StringChunked::from_iter_options(
        name.into(),
        window.iter().map(|row| {
            row.get(at)
                .map(String::as_str)
                .filter(|v| !v.is_empty() && !nulls.iter().any(|n| n == v))
        }),
    );
    if values.null_count() == values.len() {
        return Ok(None);
    }
    if !typed_by_inference(options, name) {
        return Ok(Some(DataType::String));
    }
    let types = crate::formats::readers::csv::StringTypes {
        dates: options.parse_dates,
        numbers: true,
    };
    let inferred = crate::formats::readers::csv::infer_string_type(&values.into_column(), types)?;
    // Only a number is lined up across files; any other type is typed once stacked.
    Ok(Some(match inferred.map(|ty| ty.dtype) {
        Some(dtype @ (DataType::Int64 | DataType::Float64)) => dtype,
        _ => DataType::String,
    }))
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
    crate::formats::schema_union::widen(a, b).unwrap_or(DataType::String)
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
    let says = |file: usize, name: &PlSmallStr| -> Result<Option<DataType>> {
        let Some((at, _, dtype)) = schemas[file].get_full(name) else {
            return Ok(None);
        };
        if *dtype != DataType::String {
            return Ok(Some(dtype.clone()));
        }
        seen(&heads[file].window, at, name, options)
    };
    // A column the spec types is read as text in every file and typed once stacked.
    let declared: Vec<&str> = options
        .delimited
        .as_ref()
        .map(|read| {
            read.delimited()
                .types
                .iter()
                .map(|(name, _)| name.as_str())
                .collect()
        })
        .unwrap_or_default();
    let mut targets: Vec<(PlSmallStr, DataType)> = Vec::new();
    for name in columns.iter().filter(|n| !declared.contains(&n.as_str())) {
        let mut target: Option<DataType> = None;
        for file in 0..frames.len() {
            if let Some(dtype) = says(file, name)? {
                target = Some(target.map_or_else(|| dtype.clone(), |t| wider(&t, &dtype)));
            }
        }
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
                let nulls = crate::formats::readers::csv::csv_null_values_for(options, name);
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
    for head in heads.iter().filter(|h| h.lossy) {
        notes.push(lossy_note(&head.file));
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

/// The note for a file whose lines read so far hold bytes that are not UTF-8.
pub(crate) fn lossy_note(file: &Path) -> Note {
    Note {
        summary: format!(
            "{}: bytes that aren't UTF-8 read as \u{FFFD}",
            file_name(file)
        ),
        scope: "in the lines its types are inferred from".to_string(),
        read_as_text: None,
        passed_over: None,
    }
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
    fn a_window_says_what_string_inference_would() {
        let window = rows(&[
            &["", "1", "1.5", "x", "007", "2024-01-02"],
            &["NA", "2", "2", "", "008", "2024-01-03"],
        ]);
        let options = OpenOptions {
            null_values: Some(vec!["NA".to_string()]),
            parse_strings: Some(crate::ParseStringsTarget::All),
            parse_dates: true,
            ..OpenOptions::default()
        };
        let seen = |at: usize| seen(&window, at, &format!("c{at}"), &options).unwrap();
        assert_eq!(seen(0), None);
        assert_eq!(seen(1), Some(DataType::Int64));
        assert_eq!(seen(2), Some(DataType::Float64));
        assert_eq!(seen(3), Some(DataType::String));
        assert_eq!(seen(4), Some(DataType::String), "a leading zero stays text");
        assert_eq!(seen(5), Some(DataType::String), "typed once stacked");
        assert_eq!(seen(9), None, "past a short row");
        let untyped = OpenOptions {
            parse_strings: None,
            ..options.clone()
        };
        assert_eq!(
            super::seen(&window, 1, "c1", &untyped).unwrap(),
            Some(DataType::String)
        );
    }

    #[test]
    fn units_come_from_the_first_file_and_disagreements_are_kept() {
        let head = |units: &[(&str, &str)]| FileHead {
            file: std::path::PathBuf::new(),
            names: None,
            units: units
                .iter()
                .map(|(n, u)| (n.to_string(), u.to_string()))
                .collect(),
            window: Vec::new(),
            lossy: false,
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
