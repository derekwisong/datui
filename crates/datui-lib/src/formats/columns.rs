//! Rows built a row at a time into typed columns, handed over as a `DataFrame`.
//!
//! A reader that knows each column's type up front (the GPS logs, a candump's frames,
//! an ELF file's symbols) fills plain vectors here and never asks Polars to infer.

use polars::prelude::*;

/// The kinds of column, each with the value a cell holds, its type, and how a column of
/// them becomes a series.
macro_rules! kinds {
    ($($(#[$doc:meta])* $kind:ident($value:ty) => $dtype:expr, $build:expr;)*) => {
        /// The type of one column.
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        pub enum Kind {
            $($(#[$doc])* $kind,)*
        }

        impl Kind {
            pub fn dtype(self) -> DataType {
                match self {
                    $(Kind::$kind => $dtype,)*
                }
            }
        }

        /// One value of a row, of the column's kind; `None` is null.
        #[derive(Debug, Clone, PartialEq)]
        pub enum Cell {
            $($kind(Option<$value>),)*
        }

        #[derive(Debug)]
        enum Values {
            $($kind(Vec<Option<$value>>),)*
        }

        impl Values {
            fn new(kind: Kind) -> Self {
                match kind {
                    $(Kind::$kind => Values::$kind(Vec::new()),)*
                }
            }

            /// Append `cell`, or a null when it is of another kind: a reader's mistake,
            /// which costs a value rather than misaligning the columns.
            fn push(&mut self, cell: Cell) {
                match (self, cell) {
                    $((Values::$kind(v), Cell::$kind(x)) => v.push(x),)*
                    (values, _) => {
                        debug_assert!(false, "a cell of the wrong kind for {values:?}");
                        values.push_null();
                    }
                }
            }

            fn push_null(&mut self) {
                match self {
                    $(Values::$kind(v) => v.push(None),)*
                }
            }

            fn take(&mut self, name: PlSmallStr) -> PolarsResult<Series> {
                match self {
                    $(Values::$kind(v) => {
                        let build: fn(PlSmallStr, Vec<Option<$value>>) -> PolarsResult<Series> =
                            $build;
                        build(name, std::mem::take(v))
                    })*
                }
            }
        }
    };
}

kinds! {
    /// Milliseconds since the epoch, UTC.
    Time(i64) => DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC)), |name, v| {
        Ok(Int64Chunked::from_iter_options(name, v.into_iter())
            .into_datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC))
            .into_series())
    };
    /// Microseconds since the epoch, with no time zone.
    DatetimeUs(i64) => DataType::Datetime(TimeUnit::Microseconds, None), |name, v| {
        Ok(Int64Chunked::from_iter_options(name, v.into_iter())
            .into_datetime(TimeUnit::Microseconds, None)
            .into_series())
    };
    /// Microseconds since a start the file gives.
    DurationUs(i64) => DataType::Duration(TimeUnit::Microseconds), |name, v| {
        Ok(Int64Chunked::from_iter_options(name, v.into_iter())
            .into_duration(TimeUnit::Microseconds)
            .into_series())
    };
    F64(f64) => DataType::Float64, |name, v| Ok(Series::new(name, v));
    U8(u8) => DataType::UInt8, |name, v| Ok(Series::new(name, v));
    U16(u16) => DataType::UInt16, |name, v| Ok(Series::new(name, v));
    U32(u32) => DataType::UInt32, |name, v| Ok(Series::new(name, v));
    U64(u64) => DataType::UInt64, |name, v| Ok(Series::new(name, v));
    I32(i32) => DataType::Int32, |name, v| Ok(Series::new(name, v));
    Str(String) => DataType::String, |name, v| Ok(Series::new(name, v));
    /// Text from a fixed set the reader names: no allocation a value.
    Label(&'static str) => DataType::String, |name, v| Ok(Series::new(name, v));
    /// Text many rows repeat (an ELF symbol's section), shared rather than copied.
    Shared(std::sync::Arc<str>) => DataType::String, |name, v| {
        Ok(StringChunked::from_iter_options(name, v.iter().map(|s| s.as_deref())).into_series())
    };
    Bool(bool) => DataType::Boolean, |name, v| Ok(Series::new(name, v));
    Binary(Vec<u8>) => DataType::Binary, |name, v| {
        Ok(BinaryChunked::from_iter_options(name, v.into_iter()).into_series())
    };
    /// A list of unsigned integers: the satellites a GSA sentence names.
    ListU32(Vec<u32>) => DataType::List(Box::new(DataType::UInt32)), |name, v| {
        let list: ListChunked = v
            .into_iter()
            .map(|items| items.map(|items| Series::new(PlSmallStr::EMPTY, items)))
            .collect();
        // A batch of nulls only would otherwise be a list of nulls, and one segment's
        // type must be every batch's.
        list.with_name(name)
            .into_series()
            .cast(&DataType::List(Box::new(DataType::UInt32)))
    };
}

/// One column of `kind` named `name`, of `cells`.
pub fn series(
    name: &str,
    kind: Kind,
    cells: impl IntoIterator<Item = Cell>,
) -> PolarsResult<Series> {
    let mut values = Values::new(kind);
    for cell in cells {
        values.push(cell);
    }
    values.take(name.into())
}

/// Columns being filled, a row at a time.
#[derive(Debug)]
pub struct Builder {
    names: Vec<String>,
    values: Vec<Values>,
    len: usize,
}

impl Builder {
    pub fn new(columns: &[(&str, Kind)]) -> Self {
        let mut builder = Self {
            names: Vec::new(),
            values: Vec::new(),
            len: 0,
        };
        for (name, kind) in columns {
            builder.add_column(name, *kind);
        }
        builder
    }

    /// Rows held, not yet taken.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// A column added after rows were: null in each of them.
    pub fn add_column(&mut self, name: &str, kind: Kind) -> usize {
        let mut values = Values::new(kind);
        for _ in 0..self.len {
            values.push_null();
        }
        self.names.push(name.to_string());
        self.values.push(values);
        self.values.len() - 1
    }

    /// One row, a cell per column in order. Missing cells are null; extra ones are
    /// dropped.
    pub fn push(&mut self, row: impl IntoIterator<Item = Cell>) {
        let mut row = row.into_iter();
        for values in &mut self.values {
            match row.next() {
                Some(cell) => values.push(cell),
                None => values.push_null(),
            }
        }
        self.len += 1;
    }

    /// One row given as the column each value goes to; every other column is null.
    pub fn push_sparse(&mut self, cells: impl IntoIterator<Item = (usize, Cell)>) {
        let mut row: Vec<Option<Cell>> = vec![None; self.values.len()];
        for (at, cell) in cells {
            if let Some(slot) = row.get_mut(at) {
                *slot = Some(cell);
            }
        }
        for (values, cell) in self.values.iter_mut().zip(row) {
            match cell {
                Some(cell) => values.push(cell),
                None => values.push_null(),
            }
        }
        self.len += 1;
    }

    /// Set the time in column `column` of a row not yet taken. Used to date the rows
    /// read before the log said what day it is.
    pub fn set_time(&mut self, column: usize, row: usize, ms: i64) {
        if let Some(Values::Time(v)) = self.values.get_mut(column)
            && let Some(slot) = v.get_mut(row)
        {
            *slot = Some(ms);
        }
    }

    /// The rows held, as a frame; the builder is left empty with the same columns.
    pub fn take(&mut self) -> PolarsResult<DataFrame> {
        let height = self.len;
        let columns = self
            .values
            .iter_mut()
            .zip(&self.names)
            .map(|(values, name)| Ok(values.take(name.as_str().into())?.into_column()))
            .collect::<PolarsResult<Vec<_>>>()?;
        self.len = 0;
        DataFrame::new(height, columns)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rows_become_typed_columns() {
        let mut b = Builder::new(&[("t", Kind::Time), ("x", Kind::F64), ("s", Kind::ListU32)]);
        b.push([
            Cell::Time(Some(1_000)),
            Cell::F64(Some(1.5)),
            Cell::ListU32(Some(vec![3, 7])),
        ]);
        b.push([Cell::Time(None)]);
        let late = b.add_column("late", Kind::Str);
        b.push_sparse([(late, Cell::Str(Some("y".into())))]);
        b.set_time(0, 1, 2_000);
        let df = b.take().unwrap();
        assert_eq!(df.height(), 3);
        assert_eq!(df.column("t").unwrap().dtype(), &Kind::Time.dtype());
        assert_eq!(df.column("s").unwrap().dtype(), &Kind::ListU32.dtype());
        assert_eq!(df.column("late").unwrap().null_count(), 2);
        assert_eq!(df.column("t").unwrap().null_count(), 1);
        assert!(b.is_empty());
        // Nulls only still give the declared type.
        b.push([]);
        let df = b.take().unwrap();
        assert_eq!(df.column("s").unwrap().dtype(), &Kind::ListU32.dtype());
    }
}
