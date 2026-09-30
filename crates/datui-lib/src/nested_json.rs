//! List, array and struct cells as JSON text, for the destinations that hold
//! one value per field: a CSV export and every clipboard copy. JSON rather than
//! the table's own rendering (`[1, 2]`, `{1,"a"}`) because it keeps struct field
//! names and reads back with any JSON parser, Polars' `str.json_decode` included.
//!
//! The text is Polars' own JSON writer, the one NDJSON export uses, so a list
//! reads the same in a CSV as in a `.jsonl` written from the same view.
//!
//! Binary has no JSON or CSV form, and Polars' JSON writer panics on it, so it
//! is written as standard base64 text wherever it sits: a CSV, JSON or NDJSON
//! export and a copy all spell the same bytes the same way.

use base64::Engine as _;
use polars::prelude::*;

/// Whether a column holds values a delimited writer cannot write.
pub fn is_nested(dtype: &DataType) -> bool {
    matches!(
        dtype,
        DataType::List(_) | DataType::Array(_, _) | DataType::Struct(_)
    )
}

fn is_binary(dtype: &DataType) -> bool {
    matches!(dtype, DataType::Binary | DataType::BinaryOffset)
}

/// Whether `dtype` is binary or has binary anywhere inside.
fn has_binary(dtype: &DataType) -> bool {
    match dtype {
        DataType::List(inner) | DataType::Array(inner, _) => has_binary(inner),
        DataType::Struct(fields) => fields.iter().any(|f| has_binary(f.dtype())),
        dtype => is_binary(dtype),
    }
}

/// `dtype` with every binary leaf as a string, the type [`binary_as_base64`] returns.
fn base64_dtype(dtype: &DataType) -> DataType {
    match dtype {
        DataType::List(inner) => DataType::List(Box::new(base64_dtype(inner))),
        DataType::Array(inner, width) => DataType::Array(Box::new(base64_dtype(inner)), *width),
        DataType::Struct(fields) => DataType::Struct(
            fields
                .iter()
                .map(|f| Field::new(f.name().clone(), base64_dtype(f.dtype())))
                .collect(),
        ),
        dtype if is_binary(dtype) => DataType::String,
        dtype => dtype.clone(),
    }
}

/// `series` with every binary value, at any depth, as its base64 text. Lists,
/// arrays and structs keep their shape and their nulls.
pub fn binary_as_base64(series: &Series) -> PolarsResult<Series> {
    Ok(match series.dtype() {
        dtype if !has_binary(dtype) => series.clone(),
        DataType::List(_) => series
            .list()?
            .apply_to_inner(&|inner| binary_as_base64(&inner))?
            .into_series(),
        DataType::Array(..) => series
            .array()?
            .apply_to_inner(&|inner| binary_as_base64(&inner))?
            .into_series(),
        DataType::Struct(_) => series
            .struct_()?
            .try_apply_fields(binary_as_base64)?
            .into_series(),
        _ => {
            let engine = base64::engine::general_purpose::STANDARD;
            let bytes = series.cast(&DataType::Binary)?;
            bytes
                .binary()?
                .iter()
                .map(|value| value.map(|b| engine.encode(b)))
                .collect::<StringChunked>()
                .with_name(series.name().clone())
                .into_series()
        }
    })
}

/// `lf` with every column that has binary in it written as base64 text, in
/// place and under its own name. Planned, not run.
pub fn lazy_binary_as_base64(mut lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let schema = lf.collect_schema()?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| has_binary(dtype))
        .map(|(name, _)| {
            col(name.clone()).map(
                |c| binary_as_base64(c.as_materialized_series()).map(Column::from),
                |_, field| {
                    Ok(Field::new(
                        field.name().clone(),
                        base64_dtype(field.dtype()),
                    ))
                },
            )
        })
        .collect();
    Ok(if exprs.is_empty() {
        lf
    } else {
        lf.with_columns(exprs)
    })
}

/// One nested column as a String column of JSON, null where the value is null.
pub fn column_as_json(column: &Column) -> PolarsResult<Column> {
    let series = binary_as_base64(column.as_materialized_series())?;
    let chunks = (0..series.n_chunks()).map(|i| {
        let array = series.to_arrow(i, CompatLevel::newest());
        // The writer spells a missing value `null`; the cell stays empty
        // instead, as a null does everywhere else in a CSV.
        polars_json::json::write::serialize_to_utf8(array.as_ref())
            .with_validity(array.validity().cloned())
    });
    Ok(StringChunked::from_chunk_iter(series.name().clone(), chunks).into_column())
}

/// `lf` with every nested column replaced by its JSON text, and every binary
/// column by its base64 text, in place and under its own name. Planned, not
/// run: the text is built as the rows are collected.
pub fn lazy_as_json(lf: LazyFrame) -> PolarsResult<LazyFrame> {
    let mut lf = lazy_binary_as_base64(lf)?;
    let schema = lf.collect_schema()?;
    let exprs: Vec<Expr> = schema
        .iter()
        .filter(|(_, dtype)| is_nested(dtype))
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

/// `df` with every nested column replaced by its JSON text, and every binary
/// column by its base64 text, for frames already in memory (a copy's rows).
pub fn frame_as_json(df: &DataFrame) -> PolarsResult<DataFrame> {
    let mut out = df.clone();
    for column in df.columns() {
        if is_nested(column.dtype()) {
            out.with_column(column_as_json(column)?)?;
        } else if is_binary(column.dtype()) {
            out.with_column(binary_as_base64(column.as_materialized_series())?.into_column())?;
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

    /// Binary is base64 at any depth, with its nulls, and the same in a copy,
    /// a CSV cell and an NDJSON export.
    #[test]
    fn binary_is_base64_everywhere() {
        let blob = Series::new("blob".into(), [Some(b"hi\xff".as_slice()), None]);
        let blobs = Series::new(
            "blobs".into(),
            [Some(Series::new("".into(), [b"x".as_slice()])), None],
        );
        let pair = Series::new(
            "pair".into(),
            [
                Some(Series::new("".into(), [b"a".as_slice(), b"b".as_slice()])),
                None,
            ],
        )
        .cast(&DataType::Array(Box::new(DataType::Binary), 2))
        .unwrap();
        let meta = StructChunked::from_series(
            "meta".into(),
            2,
            [Series::new(
                "raw".into(),
                [b"ab".as_slice(), b"".as_slice()],
            )]
            .iter(),
        )
        .unwrap()
        .with_outer_validity(Some([true, false].into_iter().collect()))
        .into_series();
        let df =
            DataFrame::new_infer_height(vec![blob.into(), blobs.into(), pair.into(), meta.into()])
                .unwrap();

        for column in df.columns() {
            let text = binary_as_base64(column.as_materialized_series()).unwrap();
            assert_eq!(text.dtype(), &base64_dtype(column.dtype()));
            assert_eq!(text.null_count(), 1, "{text}");
        }

        let cells = frame_as_json(&df).unwrap();
        let cell = |name: &str| cells.column(name).unwrap().str().unwrap().get(0);
        assert_eq!(cell("blob"), Some("aGn/"));
        assert_eq!(cell("blobs"), Some(r#"["eA=="]"#));
        assert_eq!(cell("pair"), Some(r#"["YQ==","Yg=="]"#));
        assert_eq!(cell("meta"), Some(r#"{"raw":"YWI="}"#));
        let lazy = lazy_as_json(df.clone().lazy()).unwrap().collect().unwrap();
        assert!(cells.equals_missing(&lazy), "{cells}\n{lazy}");

        let mut ndjson = Vec::new();
        JsonWriter::new(&mut ndjson)
            .with_json_format(JsonFormat::JsonLines)
            .finish(
                &mut lazy_binary_as_base64(df.clone().lazy())
                    .unwrap()
                    .collect()
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(
            String::from_utf8(ndjson).unwrap().lines().next(),
            Some(
                r#"{"blob":"aGn/","blobs":["eA=="],"pair":["YQ==","Yg=="],"meta":{"raw":"YWI="}}"#
            )
        );

        let copy = crate::clipboard::tabular_payload(&df, crate::clipboard::CopyFormat::Tsv, true)
            .unwrap();
        let row: Vec<&str> = copy.text.lines().nth(1).unwrap().split('\t').collect();
        assert_eq!(
            row,
            [
                "aGn/",
                r#""[""eA==""]""#,
                r#""[""YQ=="",""Yg==""]""#,
                r#""{""raw"":""YWI=""}""#
            ]
        );
    }
}
