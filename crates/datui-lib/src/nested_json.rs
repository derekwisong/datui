//! List, array and struct cells as JSON text, for the destinations that hold
//! one value per field: a CSV export and every clipboard copy. JSON rather than
//! the table's own rendering (`[1, 2]`, `{1,"a"}`) because it keeps struct field
//! names and reads back with any JSON parser, Polars' `str.json_decode` included.
//!
//! The text is Polars' own JSON writer, the one NDJSON export uses, so a list
//! reads the same in a CSV as in a `.jsonl` written from the same view.

use polars::prelude::*;

/// Whether a column holds values a delimited writer cannot write.
pub fn is_nested(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::List(_) | DataType::Array(_, _) | DataType::Struct(_)
    )
}

/// Whether the JSON writer handles every leaf of `dtype`. It has no binary
/// encoding and panics on one, so a nested binary column is left as it is and
/// refused by `ensure_delimitable` instead.
fn json_writable(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(inner) | DataType::Array(inner, _) => json_writable(inner),
        DataType::Struct(fields) => fields.iter().all(|f| json_writable(f.dtype())),
        DataType::Binary | DataType::BinaryOffset | DataType::Unknown(_) => false,
        _ => true,
    }
}

fn converts(dtype: &DataType) -> bool {
    is_nested(dtype) && json_writable(dtype)
}

/// An error naming the first nested column with binary inside, which neither
/// the delimited writer nor JSON can write. Polars' own "does not support
/// nested data" would name neither the column nor a way out.
pub fn ensure_delimitable(schema: &Schema) -> PolarsResult<()> {
    match schema
        .iter()
        .find(|(_, dtype)| is_nested(dtype) && !json_writable(dtype))
    {
        Some((name, _)) => polars_bail!(
            ComputeError: "Column \"{name}\" has binary data inside a list or struct, \
            which CSV and TSV cannot hold. Hide the column (s), or export as Parquet or Arrow"
        ),
        None => Ok(()),
    }
}

/// One nested column as a String column of JSON, null where the value is null.
pub fn column_as_json(column: &Column) -> PolarsResult<Column> {
    let series = column.as_materialized_series();
    let chunks = (0..series.n_chunks()).map(|i| {
        let array = series.to_arrow(i, CompatLevel::newest());
        // The writer spells a missing value `null`; the cell stays empty
        // instead, as a null does everywhere else in a CSV.
        polars_json::json::write::serialize_to_utf8(array.as_ref())
            .with_validity(array.validity().cloned())
    });
    Ok(StringChunked::from_chunk_iter(series.name().clone(), chunks).into_column())
}

/// `lf` with every nested column replaced by its JSON text, in place and under
/// its own name. Planned, not run: the text is built as the rows are collected.
pub fn lazy_as_json(mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let schema = lf.collect_schema()?;
    ensure_delimitable(&schema)?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| converts(dtype))
        .map(|(name, _)| {
            col(name.clone()).map(
                |c| column_as_json(&c),
                |_, field| Ok(Field::new(field.name().clone(), DataType::String)),
            )
        })
        .collect();
    Ok(if exprs.is_empty() {
        lf
    } else {
        lf.with_columns(exprs)
    })
}

/// `df` with every nested column replaced by its JSON text, for frames already
/// in memory (the rows a copy takes from the buffer).
pub fn frame_as_json(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut out = df.clone();
    for column in df.columns() {
        if converts(column.dtype()) {
            out.with_column(column_as_json(column)?)?;
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn nested() -> DataFrame {
        let ids = Series::new("ids".into(), [1i64, 2, 3]);
        let lists = Series::new(
            "xs".into(),
            [
                Some(Series::new("".into(), ["a", "b"])),
                None,
                Some(Series::new("".into(), ["say \"hi\", then go"])),
            ],
        );
        let point = StructChunked::from_series(
            "point".into(),
            3,
            [
                Series::new("x".into(), [1i64, 2, 3]),
                Series::new("y".into(), [Some("a"), None, Some("c")]),
            ]
            .iter(),
        )
        .unwrap()
        .into_series();
        let pairs = Series::new(
            "pair".into(),
            [
                Some(Series::new("".into(), [1.5f64, f64::NAN])),
                Some(Series::new("".into(), [0.0f64, 2.0])),
                None,
            ],
        )
        .cast(&DataType::Array(Box::new(DataType::Float64), 2))
        .unwrap();
        DataFrame::new_infer_height(vec![ids.into(), lists.into(), point.into(), pairs.into()])
            .unwrap()
    }

    #[test]
    fn nested_columns_become_json_and_the_rest_stay() {
        let df = frame_as_json(&nested()).unwrap();
        assert_eq!(df.column("ids").unwrap().dtype(), &DataType::Int64);
        let xs = df.column("xs").unwrap().str().unwrap().clone();
        assert_eq!(xs.get(0), Some(r#"["a","b"]"#));
        assert_eq!(xs.get(1), None, "a null list stays null");
        assert_eq!(xs.get(2), Some(r#"["say \"hi\", then go"]"#));
        let point = df.column("point").unwrap().str().unwrap().clone();
        assert_eq!(point.get(0), Some(r#"{"x":1,"y":"a"}"#));
        assert_eq!(point.get(1), Some(r#"{"x":2,"y":null}"#));
        let pair = df.column("pair").unwrap().str().unwrap().clone();
        assert_eq!(pair.get(0), Some("[1.5,null]"), "NaN has no JSON spelling");
        assert_eq!(pair.get(1), Some("[0.0,2.0]"));
        assert_eq!(pair.get(2), None);
    }

    #[test]
    fn lazy_and_in_memory_agree() {
        let eager = frame_as_json(&nested()).unwrap();
        let lazy = lazy_as_json(nested().lazy()).unwrap().collect().unwrap();
        assert!(eager.equals_missing(&lazy), "{eager}\n{lazy}");
    }

    /// Dates, datetimes and categoricals inside a struct or list read the same
    /// in a CSV cell as in an NDJSON export of the same frame.
    #[test]
    fn cells_match_an_ndjson_export() {
        let df = df!(
            "d" => [Some("2024-01-02"), None],
            "c" => [Some("a"), None],
        )
        .unwrap()
        .lazy()
        .with_columns([
            col("d").str().to_date(StrptimeOptions::default()),
            col("c").cast(DataType::from_categories(Categories::global())),
        ])
        .with_columns([col("d")
            .cast(DataType::Datetime(
                TimeUnit::Microseconds,
                Some(TimeZone::UTC),
            ))
            .alias("dt")])
        .select([
            as_struct(vec![col("d"), col("dt"), col("c")]).alias("s"),
            col("c").implode(true).alias("lc"),
        ])
        .collect()
        .unwrap();
        let mut ndjson = Vec::new();
        JsonWriter::new(&mut ndjson)
            .with_json_format(JsonFormat::JsonLines)
            .finish(&mut df.clone())
            .unwrap();
        let cells = frame_as_json(&df).unwrap();
        let (s, lc) = (
            cells.column("s").unwrap().str().unwrap(),
            cells.column("lc").unwrap().str().unwrap(),
        );
        let rebuilt: String = (0..cells.height())
            .map(|i| {
                format!(
                    "{{\"s\":{},\"lc\":{}}}\n",
                    s.get(i).unwrap(),
                    lc.get(i).unwrap()
                )
            })
            .collect();
        assert_eq!(rebuilt, String::from_utf8(ndjson).unwrap());
        assert!(rebuilt.contains(r#""dt":"2024-01-02T00:00:00+00:00","c":"a""#));
    }

    /// Binary inside a list has no JSON spelling: the frame keeps it, and a
    /// CSV export or a TSV copy says which column and what to do instead of
    /// Polars' bare "does not support nested data".
    #[test]
    fn a_nested_binary_column_is_refused_by_name() {
        let bytes = Series::new(
            "blobs".into(),
            [Some(Series::new("".into(), [b"x".as_slice()]))],
        );
        let df = DataFrame::new_infer_height(vec![bytes.into()]).unwrap();
        let out = frame_as_json(&df).unwrap();
        assert!(matches!(
            out.column("blobs").unwrap().dtype(),
            DataType::List(_)
        ));
        let Err(export) = lazy_as_json(df.clone().lazy()) else {
            panic!("a CSV export of a nested binary column plans")
        };
        let export = export.to_string();
        assert!(
            export.contains("\"blobs\"") && export.contains("Parquet"),
            "{export}"
        );
        let copy = crate::clipboard::tabular_payload(&df, crate::clipboard::CopyFormat::Tsv, true)
            .unwrap_err();
        assert_eq!(copy, export);
        assert!(
            crate::clipboard::tabular_payload(&df, crate::clipboard::CopyFormat::Markdown, true)
                .is_ok(),
            "Markdown writes any cell as text"
        );
    }
}
