//! Rows built a column at a time, handed over as a `DataFrame` a batch at a time.
//!
//! The GPS readers know each column's type up front (or, for a GPX extension field,
//! hold it as text), so they fill plain vectors and never ask Polars to infer.

use polars::prelude::*;

/// The type of one column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// Milliseconds since the epoch, UTC.
    Time,
    F64,
    U32,
    U64,
    I32,
    Str,
    Bool,
    /// A list of unsigned integers: the satellites a GSA sentence names.
    ListU32,
}

impl Kind {
    pub fn dtype(self) -> DataType {
        match self {
            Kind::Time => DataType::Datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC)),
            Kind::F64 => DataType::Float64,
            Kind::U32 => DataType::UInt32,
            Kind::U64 => DataType::UInt64,
            Kind::I32 => DataType::Int32,
            Kind::Str => DataType::String,
            Kind::Bool => DataType::Boolean,
            Kind::ListU32 => DataType::List(Box::new(DataType::UInt32)),
        }
    }
}

/// One value of a row, of the column's kind; `None` is null.
#[derive(Debug, Clone, PartialEq)]
pub enum Cell {
    Time(Option<i64>),
    F64(Option<f64>),
    U32(Option<u32>),
    U64(Option<u64>),
    I32(Option<i32>),
    Str(Option<String>),
    Bool(Option<bool>),
    ListU32(Option<Vec<u32>>),
}

#[derive(Debug)]
enum Values {
    Time(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    U32(Vec<Option<u32>>),
    U64(Vec<Option<u64>>),
    I32(Vec<Option<i32>>),
    Str(Vec<Option<String>>),
    Bool(Vec<Option<bool>>),
    ListU32(Vec<Option<Vec<u32>>>),
}

impl Values {
    fn new(kind: Kind) -> Self {
        match kind {
            Kind::Time => Values::Time(Vec::new()),
            Kind::F64 => Values::F64(Vec::new()),
            Kind::U32 => Values::U32(Vec::new()),
            Kind::U64 => Values::U64(Vec::new()),
            Kind::I32 => Values::I32(Vec::new()),
            Kind::Str => Values::Str(Vec::new()),
            Kind::Bool => Values::Bool(Vec::new()),
            Kind::ListU32 => Values::ListU32(Vec::new()),
        }
    }

    /// Append `cell`, or a null when it is of another kind: a reader's mistake, which
    /// costs a value rather than misaligning the columns.
    fn push(&mut self, cell: Cell) {
        match (self, cell) {
            (Values::Time(v), Cell::Time(x)) => v.push(x),
            (Values::F64(v), Cell::F64(x)) => v.push(x),
            (Values::U32(v), Cell::U32(x)) => v.push(x),
            (Values::U64(v), Cell::U64(x)) => v.push(x),
            (Values::I32(v), Cell::I32(x)) => v.push(x),
            (Values::Str(v), Cell::Str(x)) => v.push(x),
            (Values::Bool(v), Cell::Bool(x)) => v.push(x),
            (Values::ListU32(v), Cell::ListU32(x)) => v.push(x),
            (values, _) => {
                debug_assert!(false, "a cell of the wrong kind for {values:?}");
                values.push_null();
            }
        }
    }

    fn push_null(&mut self) {
        match self {
            Values::Time(v) => v.push(None),
            Values::F64(v) => v.push(None),
            Values::U32(v) => v.push(None),
            Values::U64(v) => v.push(None),
            Values::I32(v) => v.push(None),
            Values::Str(v) => v.push(None),
            Values::Bool(v) => v.push(None),
            Values::ListU32(v) => v.push(None),
        }
    }

    fn take(&mut self, name: &str, kind: Kind) -> PolarsResult<Column> {
        let name = PlSmallStr::from(name);
        let series = match self {
            Values::Time(v) => Int64Chunked::from_iter_options(name, std::mem::take(v).into_iter())
                .into_datetime(TimeUnit::Milliseconds, Some(TimeZone::UTC))
                .into_series(),
            Values::F64(v) => {
                Float64Chunked::from_iter_options(name, std::mem::take(v).into_iter()).into_series()
            }
            Values::U32(v) => {
                UInt32Chunked::from_iter_options(name, std::mem::take(v).into_iter()).into_series()
            }
            Values::U64(v) => {
                UInt64Chunked::from_iter_options(name, std::mem::take(v).into_iter()).into_series()
            }
            Values::I32(v) => {
                Int32Chunked::from_iter_options(name, std::mem::take(v).into_iter()).into_series()
            }
            Values::Str(v) => {
                StringChunked::from_iter_options(name, std::mem::take(v).into_iter()).into_series()
            }
            Values::Bool(v) => {
                BooleanChunked::from_iter_options(name, std::mem::take(v).into_iter()).into_series()
            }
            Values::ListU32(v) => {
                let list: ListChunked = std::mem::take(v)
                    .into_iter()
                    .map(|items| items.map(|items| Series::new(PlSmallStr::EMPTY, items)))
                    .collect();
                // A batch of nulls only would otherwise be a list of nulls, and one
                // segment's type must be every batch's.
                list.with_name(name).into_series().cast(&kind.dtype())?
            }
        };
        Ok(series.into_column())
    }
}

/// Columns being filled, a row at a time.
#[derive(Debug)]
pub struct Builder {
    names: Vec<String>,
    kinds: Vec<Kind>,
    values: Vec<Values>,
    len: usize,
}

impl Builder {
    pub fn new(columns: &[(&str, Kind)]) -> Self {
        let mut builder = Self {
            names: Vec::new(),
            kinds: Vec::new(),
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

    pub fn names(&self) -> &[String] {
        &self.names
    }

    pub fn position(&self, name: &str) -> Option<usize> {
        self.names.iter().position(|n| n == name)
    }

    /// A column added after rows were: null in each of them.
    pub fn add_column(&mut self, name: &str, kind: Kind) -> usize {
        let mut values = Values::new(kind);
        for _ in 0..self.len {
            values.push_null();
        }
        self.names.push(name.to_string());
        self.kinds.push(kind);
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
            .zip(&self.kinds)
            .map(|((values, name), kind)| values.take(name, *kind))
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
