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
/// the delimited writer refuses it with an error instead.
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

    #[test]
    fn a_nested_binary_column_is_left_alone() {
        let bytes = Series::new(
            "b".into(),
            [Some(Series::new("".into(), [b"x".as_slice()]))],
        );
        let df = DataFrame::new_infer_height(vec![bytes.into()]).unwrap();
        let out = frame_as_json(&df).unwrap();
        assert!(matches!(
            out.column("b").unwrap().dtype(),
            DataType::List(_)
        ));
    }
}
