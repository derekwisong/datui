use color_eyre::Result;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::{fs, fs::File, path::Path};

use polars::io::HiveOptions;
use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Padding, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::error_display::user_message_from_polars;
use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};
use crate::query::parse_query;
use crate::{CompressionFormat, OpenOptions};
use polars::lazy::frame::pivot::pivot_stable;
use std::io::{BufReader, Read};

use calamine::{open_workbook_auto, Data, Reader};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use orc_rust::ArrowReaderBuilder;
use tempfile::NamedTempFile;

use arrow::array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
    TimestampMillisecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;

fn pivot_agg_expr(agg: PivotAggregation) -> Result<Expr> {
    let e = col(PlSmallStr::from_static(""));
    let expr = match agg {
        PivotAggregation::Last => e.last(),
        PivotAggregation::First => e.first(),
        PivotAggregation::Min => e.min(),
        PivotAggregation::Max => e.max(),
        PivotAggregation::Avg => e.mean(),
        PivotAggregation::Med => e.median(),
        PivotAggregation::Std => e.std(1),
        PivotAggregation::Count => e.len(),
    };
    Ok(expr)
}

pub struct DataTableState {
    pub lf: LazyFrame,
    original_lf: LazyFrame,
    df: Option<DataFrame>,        // Scrollable columns dataframe
    locked_df: Option<DataFrame>, // Locked columns dataframe
    pub table_state: TableState,
    pub start_row: usize,
    pub visible_rows: usize,
    pub termcol_index: usize,
    pub visible_termcols: usize,
    pub error: Option<PolarsError>,
    pub suppress_error_display: bool, // When true, don't show errors in main view (e.g., when query input is active)
    pub schema: Arc<Schema>,
    pub num_rows: usize,
    /// When true, collect() skips the len() query.
    num_rows_valid: bool,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_ascending: bool,
    pub active_query: String,
    column_order: Vec<String>,   // Order of columns for display
    locked_columns_count: usize, // Number of locked columns (from left)
    grouped_lf: Option<LazyFrame>,
    drilled_down_group_index: Option<usize>, // Index of the group we're viewing
    pub drilled_down_group_key: Option<Vec<String>>, // Key values of the drilled down group
    pub drilled_down_group_key_columns: Option<Vec<String>>, // Key column names of the drilled down group
    pages_lookahead: usize,
    pages_lookback: usize,
    max_buffered_rows: usize, // 0 = no limit
    max_buffered_mb: usize,   // 0 = no limit
    buffered_start_row: usize,
    buffered_end_row: usize,
    proximity_threshold: usize,
    row_numbers: bool,
    row_start_index: usize,
    /// Last applied pivot spec, if current lf is result of a pivot. Used for templates.
    last_pivot_spec: Option<PivotSpec>,
    /// Last applied melt spec, if current lf is result of a melt. Used for templates.
    last_melt_spec: Option<MeltSpec>,
    /// When set, dataset was loaded with hive partitioning; partition column names for Info panel and predicate pushdown.
    pub partition_columns: Option<Vec<String>>,
    /// When set, decompressed CSV was written to this temp file; kept alive so the file exists for lazy scan.
    decompress_temp_file: Option<NamedTempFile>,
}

/// Inferred type for an Excel column (preserves numbers, bools, dates; avoids stringifying).
#[derive(Clone, Copy)]
enum ExcelColType {
    Int64,
    Float64,
    Boolean,
    Utf8,
    Date,
    Datetime,
}

impl DataTableState {
    pub fn new(
        lf: LazyFrame,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
    ) -> Result<Self> {
        let schema = lf.clone().collect_schema()?;
        let column_order: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        Ok(Self {
            original_lf: lf.clone(),
            lf,
            df: None,
            locked_df: None,
            table_state: TableState::default(),
            start_row: 0,
            visible_rows: 0,
            termcol_index: 0,
            visible_termcols: 0,
            error: None,
            suppress_error_display: false,
            schema,
            num_rows: 0,
            num_rows_valid: false,
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_ascending: true,
            active_query: String::new(),
            column_order,
            locked_columns_count: 0,
            grouped_lf: None,
            drilled_down_group_index: None,
            drilled_down_group_key: None,
            drilled_down_group_key_columns: None,
            pages_lookahead: pages_lookahead.unwrap_or(3),
            pages_lookback: pages_lookback.unwrap_or(3),
            max_buffered_rows: max_buffered_rows.unwrap_or(100_000),
            max_buffered_mb: max_buffered_mb.unwrap_or(512),
            buffered_start_row: 0,
            buffered_end_row: 0,
            proximity_threshold: 0, // Will be set when visible_rows is known
            row_numbers: false,     // Will be set from options
            row_start_index: 1,     // Will be set from options
            last_pivot_spec: None,
            last_melt_spec: None,
            partition_columns: None,
            decompress_temp_file: None,
        })
    }

    pub fn reset(&mut self) {
        self.invalidate_num_rows();
        self.lf = self.original_lf.clone();
        self.schema = self
            .original_lf
            .clone()
            .collect_schema()
            .unwrap_or_else(|_| Arc::new(Schema::with_capacity(0)));
        self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();

        self.locked_columns_count = 0;
        self.filters.clear();
        self.sort_columns.clear();
        self.sort_ascending = true;
        self.start_row = 0;
        self.termcol_index = 0;
        self.active_query.clear();
        self.error = None;
        self.suppress_error_display = false;

        self.drilled_down_group_index = None;
        self.drilled_down_group_key = None;
        self.drilled_down_group_key_columns = None;
        self.grouped_lf = None;
        self.last_pivot_spec = None;
        self.last_melt_spec = None;
        // partition_columns is preserved across reset (same dataset)

        // Invalidate buffer on reset
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;

        self.table_state.select(Some(0));

        self.collect();
        if self.num_rows > 0 {
            self.start_row = 0;
        }
    }

    pub fn from_parquet(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyFrame::scan_parquet(pl_path, Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load multiple Parquet files and concatenate them into one LazyFrame (same schema assumed).
    pub fn from_parquet_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_parquet(
                paths[0].as_ref(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                row_numbers,
                row_start_index,
            );
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlPath::Local(Arc::from(p.as_ref()));
            let lf = LazyFrame::scan_parquet(pl_path, Default::default())?;
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load a single Arrow IPC / Feather v2 file (lazy).
    pub fn from_ipc(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyFrame::scan_ipc(pl_path, Default::default(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load multiple Arrow IPC / Feather files and concatenate into one LazyFrame.
    pub fn from_ipc_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_ipc(
                paths[0].as_ref(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                row_numbers,
                row_start_index,
            );
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlPath::Local(Arc::from(p.as_ref()));
            let lf = LazyFrame::scan_ipc(pl_path, Default::default(), Default::default())?;
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load a single Avro file (eager read, then lazy).
    pub fn from_avro(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let file = File::open(path)?;
        let df = polars::io::avro::AvroReader::new(file).finish()?;
        let lf = df.lazy();
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load multiple Avro files and concatenate into one LazyFrame.
    pub fn from_avro_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_avro(
                paths[0].as_ref(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                row_numbers,
                row_start_index,
            );
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let file = File::open(p.as_ref())?;
            let df = polars::io::avro::AvroReader::new(file).finish()?;
            lazy_frames.push(df.lazy());
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load a single Excel file (xls, xlsx, xlsm, xlsb) using calamine (eager read, then lazy).
    /// Sheet is selected by 0-based index or name via `excel_sheet`.
    #[allow(clippy::too_many_arguments)]
    pub fn from_excel(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
        excel_sheet: Option<&str>,
    ) -> Result<Self> {
        let mut workbook =
            open_workbook_auto(path).map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?;
        let sheet_names = workbook.sheet_names().to_vec();
        if sheet_names.is_empty() {
            return Err(color_eyre::eyre::eyre!("Excel file has no worksheets"));
        }
        let range = if let Some(sheet_sel) = excel_sheet {
            if let Ok(idx) = sheet_sel.parse::<usize>() {
                workbook
                    .worksheet_range_at(idx)
                    .ok_or_else(|| color_eyre::eyre::eyre!("Excel: no sheet at index {}", idx))?
                    .map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?
            } else {
                workbook
                    .worksheet_range(sheet_sel)
                    .map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?
            }
        } else {
            workbook
                .worksheet_range_at(0)
                .ok_or_else(|| color_eyre::eyre::eyre!("Excel: no first sheet"))?
                .map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?
        };
        let rows: Vec<Vec<Data>> = range.rows().map(|r| r.to_vec()).collect();
        if rows.is_empty() {
            let empty_df = DataFrame::new(vec![])?;
            let mut state = Self::new(
                empty_df.lazy(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
            )?;
            state.row_numbers = row_numbers;
            state.row_start_index = row_start_index;
            return Ok(state);
        }
        let headers: Vec<String> = rows[0]
            .iter()
            .map(|c| calamine::DataType::as_string(c).unwrap_or_else(|| c.to_string()))
            .collect();
        let n_cols = headers.len();
        let mut series_vec = Vec::with_capacity(n_cols);
        for (col_idx, header) in headers.iter().enumerate() {
            let col_cells: Vec<Option<&Data>> =
                rows[1..].iter().map(|row| row.get(col_idx)).collect();
            let inferred = Self::excel_infer_column_type(&col_cells);
            let name = if header.is_empty() {
                format!("column_{}", col_idx + 1)
            } else {
                header.clone()
            };
            let series = Self::excel_column_to_series(name.as_str(), &col_cells, inferred)?;
            series_vec.push(series.into());
        }
        let df = DataFrame::new(series_vec)?;
        let mut state = Self::new(
            df.lazy(),
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Infers column type: prefers Int64 for whole-number floats; infers Date/Datetime for
    /// calamine DateTime/DateTimeIso or for string columns that parse as ISO date/datetime.
    fn excel_infer_column_type(cells: &[Option<&Data>]) -> ExcelColType {
        use calamine::DataType as CalamineTrait;
        let mut has_string = false;
        let mut has_float = false;
        let mut has_int = false;
        let mut has_bool = false;
        let mut has_datetime = false;
        for cell in cells.iter().flatten() {
            if CalamineTrait::is_string(*cell) {
                has_string = true;
                break;
            }
            if CalamineTrait::is_float(*cell)
                || CalamineTrait::is_datetime(*cell)
                || CalamineTrait::is_datetime_iso(*cell)
            {
                has_float = true;
            }
            if CalamineTrait::is_int(*cell) {
                has_int = true;
            }
            if CalamineTrait::is_bool(*cell) {
                has_bool = true;
            }
            if CalamineTrait::is_datetime(*cell) || CalamineTrait::is_datetime_iso(*cell) {
                has_datetime = true;
            }
        }
        if has_string {
            let any_parsed = cells
                .iter()
                .flatten()
                .any(|c| Self::excel_cell_to_naive_datetime(c).is_some());
            let all_non_empty_parse = cells.iter().flatten().all(|c| {
                CalamineTrait::is_empty(*c) || Self::excel_cell_to_naive_datetime(c).is_some()
            });
            if any_parsed && all_non_empty_parse {
                if Self::excel_parsed_cells_all_midnight(cells) {
                    ExcelColType::Date
                } else {
                    ExcelColType::Datetime
                }
            } else {
                ExcelColType::Utf8
            }
        } else if has_int {
            ExcelColType::Int64
        } else if has_datetime {
            if Self::excel_parsed_cells_all_midnight(cells) {
                ExcelColType::Date
            } else {
                ExcelColType::Datetime
            }
        } else if has_float {
            let all_whole = cells.iter().flatten().all(|cell| {
                cell.as_f64()
                    .is_none_or(|f| f.is_finite() && (f - f.trunc()).abs() < 1e-10)
            });
            if all_whole {
                ExcelColType::Int64
            } else {
                ExcelColType::Float64
            }
        } else if has_bool {
            ExcelColType::Boolean
        } else {
            ExcelColType::Utf8
        }
    }

    /// True if every cell that parses as datetime has time 00:00:00.
    fn excel_parsed_cells_all_midnight(cells: &[Option<&Data>]) -> bool {
        let midnight = NaiveTime::from_hms_opt(0, 0, 0).expect("valid time");
        cells
            .iter()
            .flatten()
            .filter_map(|c| Self::excel_cell_to_naive_datetime(c))
            .all(|dt| dt.time() == midnight)
    }

    /// Converts a calamine cell to NaiveDateTime (Excel serial, DateTimeIso, or parseable string).
    fn excel_cell_to_naive_datetime(cell: &Data) -> Option<NaiveDateTime> {
        use calamine::DataType;
        if let Some(dt) = cell.as_datetime() {
            return Some(dt);
        }
        let s = cell.get_datetime_iso().or_else(|| cell.get_string())?;
        Self::parse_naive_datetime_str(s)
    }

    /// Parses an ISO-style date/datetime string; tries FORMATS in order.
    fn parse_naive_datetime_str(s: &str) -> Option<NaiveDateTime> {
        let s = s.trim();
        if s.is_empty() {
            return None;
        }
        const FORMATS: &[&str] = &[
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ];
        for fmt in FORMATS {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Some(dt);
            }
        }
        if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
            return Some(d.and_hms_opt(0, 0, 0).expect("midnight"));
        }
        None
    }

    /// Build a Polars Series from a column of calamine cells using the inferred type.
    fn excel_column_to_series(
        name: &str,
        cells: &[Option<&Data>],
        col_type: ExcelColType,
    ) -> Result<Series> {
        use calamine::DataType as CalamineTrait;
        use polars::datatypes::TimeUnit;
        let series = match col_type {
            ExcelColType::Int64 => {
                let v: Vec<Option<i64>> = cells
                    .iter()
                    .map(|c| c.and_then(|cell| cell.as_i64()))
                    .collect();
                Series::new(name.into(), v)
            }
            ExcelColType::Float64 => {
                let v: Vec<Option<f64>> = cells
                    .iter()
                    .map(|c| c.and_then(|cell| cell.as_f64()))
                    .collect();
                Series::new(name.into(), v)
            }
            ExcelColType::Boolean => {
                let v: Vec<Option<bool>> = cells
                    .iter()
                    .map(|c| c.and_then(|cell| cell.get_bool()))
                    .collect();
                Series::new(name.into(), v)
            }
            ExcelColType::Utf8 => {
                let v: Vec<Option<String>> = cells
                    .iter()
                    .map(|c| c.and_then(|cell| cell.as_string()))
                    .collect();
                Series::new(name.into(), v)
            }
            ExcelColType::Date => {
                let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).expect("valid date");
                let v: Vec<Option<i32>> = cells
                    .iter()
                    .map(|c| {
                        c.and_then(Self::excel_cell_to_naive_datetime)
                            .map(|dt| (dt.date() - epoch).num_days() as i32)
                    })
                    .collect();
                Series::new(name.into(), v).cast(&DataType::Date)?
            }
            ExcelColType::Datetime => {
                let v: Vec<Option<i64>> = cells
                    .iter()
                    .map(|c| {
                        c.and_then(Self::excel_cell_to_naive_datetime)
                            .map(|dt| dt.and_utc().timestamp_micros())
                    })
                    .collect();
                Series::new(name.into(), v)
                    .cast(&DataType::Datetime(TimeUnit::Microseconds, None))?
            }
        };
        Ok(series)
    }

    /// Load a single ORC file (eager read via orc-rust → Arrow, then convert to Polars, then lazy).
    /// ORC is read fully into memory; see loading-data docs for large-file notes.
    pub fn from_orc(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let file = File::open(path)?;
        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?
            .build();
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?;
        let df = Self::arrow_record_batches_to_dataframe(&batches)?;
        let lf = df.lazy();
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load multiple ORC files and concatenate into one LazyFrame.
    pub fn from_orc_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_orc(
                paths[0].as_ref(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                row_numbers,
                row_start_index,
            );
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let file = File::open(p.as_ref())?;
            let reader = ArrowReaderBuilder::try_new(file)
                .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?
                .build();
            let batches: Vec<RecordBatch> = reader
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?;
            let df = Self::arrow_record_batches_to_dataframe(&batches)?;
            lazy_frames.push(df.lazy());
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Convert Arrow (arrow crate 57) RecordBatches to Polars DataFrame by value (ORC uses
    /// arrow 57; Polars uses polars-arrow, so we cannot use Series::from_arrow).
    fn arrow_record_batches_to_dataframe(batches: &[RecordBatch]) -> Result<DataFrame> {
        if batches.is_empty() {
            return Ok(DataFrame::new(vec![])?);
        }
        let mut all_dfs = Vec::with_capacity(batches.len());
        for batch in batches {
            let n_cols = batch.num_columns();
            let schema = batch.schema();
            let mut series_vec = Vec::with_capacity(n_cols);
            for (i, col) in batch.columns().iter().enumerate() {
                let name = schema.field(i).name().as_str();
                let s = Self::arrow_array_to_polars_series(name, col)?;
                series_vec.push(s.into());
            }
            let df = DataFrame::new(series_vec)?;
            all_dfs.push(df);
        }
        let mut out = all_dfs.remove(0);
        for df in all_dfs {
            out = out.vstack(&df)?;
        }
        Ok(out)
    }

    fn arrow_array_to_polars_series(name: &str, array: &dyn Array) -> Result<Series> {
        use arrow::datatypes::DataType as ArrowDataType;
        let len = array.len();
        match array.data_type() {
            ArrowDataType::Int8 => {
                let a = array
                    .as_primitive_opt::<Int8Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int8 array"))?;
                let v: Vec<Option<i8>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Int16 => {
                let a = array
                    .as_primitive_opt::<Int16Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int16 array"))?;
                let v: Vec<Option<i16>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Int32 => {
                let a = array
                    .as_primitive_opt::<Int32Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int32 array"))?;
                let v: Vec<Option<i32>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Int64 => {
                let a = array
                    .as_primitive_opt::<Int64Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Int64 array"))?;
                let v: Vec<Option<i64>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::UInt8 => {
                let a = array
                    .as_primitive_opt::<UInt8Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt8 array"))?;
                let v: Vec<Option<i64>> = (0..len)
                    .map(|i| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i) as i64)
                        }
                    })
                    .collect();
                Ok(Series::new(name.into(), v).cast(&DataType::UInt8)?)
            }
            ArrowDataType::UInt16 => {
                let a = array
                    .as_primitive_opt::<UInt16Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt16 array"))?;
                let v: Vec<Option<i64>> = (0..len)
                    .map(|i| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i) as i64)
                        }
                    })
                    .collect();
                Ok(Series::new(name.into(), v).cast(&DataType::UInt16)?)
            }
            ArrowDataType::UInt32 => {
                let a = array
                    .as_primitive_opt::<UInt32Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt32 array"))?;
                let v: Vec<Option<u32>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::UInt64 => {
                let a = array
                    .as_primitive_opt::<UInt64Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected UInt64 array"))?;
                let v: Vec<Option<u64>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Float32 => {
                let a = array
                    .as_primitive_opt::<Float32Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Float32 array"))?;
                let v: Vec<Option<f32>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Float64 => {
                let a = array
                    .as_primitive_opt::<Float64Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Float64 array"))?;
                let v: Vec<Option<f64>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Boolean => {
                let a = array
                    .as_boolean_opt()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Boolean array"))?;
                let v: Vec<Option<bool>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Utf8 => {
                let a = array
                    .as_string_opt::<i32>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Utf8 array"))?;
                let v: Vec<Option<String>> = (0..len)
                    .map(|i| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i).to_string())
                        }
                    })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::LargeUtf8 => {
                let a = array
                    .as_string_opt::<i64>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected LargeUtf8 array"))?;
                let v: Vec<Option<String>> = (0..len)
                    .map(|i| {
                        if a.is_null(i) {
                            None
                        } else {
                            Some(a.value(i).to_string())
                        }
                    })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Date32 => {
                let a = array
                    .as_primitive_opt::<Date32Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Date32 array"))?;
                let v: Vec<Option<i32>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Date64 => {
                let a = array
                    .as_primitive_opt::<Date64Type>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Date64 array"))?;
                let v: Vec<Option<i64>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            ArrowDataType::Timestamp(_, _) => {
                let a = array
                    .as_primitive_opt::<TimestampMillisecondType>()
                    .ok_or_else(|| color_eyre::eyre::eyre!("ORC: expected Timestamp array"))?;
                let v: Vec<Option<i64>> = (0..len)
                    .map(|i| if a.is_null(i) { None } else { Some(a.value(i)) })
                    .collect();
                Ok(Series::new(name.into(), v))
            }
            other => Err(color_eyre::eyre::eyre!(
                "ORC: unsupported column type {:?} for column '{}'",
                other,
                name
            )),
        }
    }

    /// Discover hive partition column names from a directory path by walking a single
    /// "spine" (one branch) of key=value directories. Partition keys are uniform across
    /// the tree, so we only need one path to infer [year, month, day] etc. Returns columns
    /// in path order. Stops after max_depth levels to avoid runaway on malformed trees.
    fn discover_partition_columns_from_path(path: &Path) -> Vec<String> {
        const MAX_PARTITION_DEPTH: usize = 64;
        let mut columns = Vec::<String>::new();
        let mut seen = HashSet::<String>::new();
        Self::discover_partition_columns_spine(
            path,
            &mut columns,
            &mut seen,
            0,
            MAX_PARTITION_DEPTH,
        );
        columns
    }

    /// Walk one branch: at this directory, find the first child that is a key=value dir,
    /// record the key (if not already seen), then recurse into that one child only.
    /// This does O(depth) read_dir calls instead of walking the entire tree.
    fn discover_partition_columns_spine(
        path: &Path,
        columns: &mut Vec<String>,
        seen: &mut HashSet<String>,
        depth: usize,
        max_depth: usize,
    ) {
        if depth >= max_depth {
            return;
        }
        let Ok(entries) = fs::read_dir(path) else {
            return;
        };
        let mut first_partition_child: Option<std::path::PathBuf> = None;
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                if let Some(name) = child.file_name().and_then(|n| n.to_str()) {
                    if let Some((key, _)) = name.split_once('=') {
                        if !key.is_empty() && seen.insert(key.to_string()) {
                            columns.push(key.to_string());
                        }
                        if first_partition_child.is_none() {
                            first_partition_child = Some(child);
                        }
                        break;
                    }
                }
            }
        }
        if let Some(one) = first_partition_child {
            Self::discover_partition_columns_spine(&one, columns, seen, depth + 1, max_depth);
        }
    }

    /// Infer partition column names from a glob pattern path (e.g. "data/year=*/month=*/*.parquet").
    fn discover_partition_columns_from_glob_pattern(path: &Path) -> Vec<String> {
        let path_str = path.as_os_str().to_string_lossy();
        let mut columns = Vec::<String>::new();
        let mut seen = HashSet::<String>::new();
        for segment in path_str.split('/') {
            if let Some((key, rest)) = segment.split_once('=') {
                if !key.is_empty()
                    && (rest == "*" || !rest.contains('*'))
                    && seen.insert(key.to_string())
                {
                    columns.push(key.to_string());
                }
            }
        }
        columns
    }

    /// Load Parquet with Hive partitioning from a directory or glob path.
    /// When path is a directory, partition columns are discovered from path structure.
    /// When path contains glob (e.g. `**/*.parquet`), partition columns are inferred from the pattern (e.g. `year=*/month=*`).
    /// Partition columns are moved to the left in the initial LazyFrame before state is created.
    ///
    /// **Performance**: The slow part is Polars, not our code. `scan_parquet` + `collect_schema()` trigger
    /// path expansion (full directory tree or glob) and parquet metadata reads; we only do a single-spine
    /// walk for partition key discovery and cheap schema/select work.
    pub fn from_parquet_hive(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let path_str = path.as_os_str().to_string_lossy();
        let is_glob = path_str.contains('*');
        let pl_path = PlPath::Local(Arc::from(path));
        let args = ScanArgsParquet {
            hive_options: HiveOptions::new_enabled(),
            glob: is_glob,
            ..Default::default()
        };
        let mut lf = LazyFrame::scan_parquet(pl_path, args)?;
        let schema = lf.collect_schema()?;

        let mut discovered = if path.is_dir() {
            Self::discover_partition_columns_from_path(path)
        } else {
            Self::discover_partition_columns_from_glob_pattern(path)
        };

        // Fallback: glob like "**/*.parquet" has no key= in the pattern, so discovery is empty.
        // Try discovering from a directory prefix (e.g. path.parent() or walk up until we find a dir).
        if discovered.is_empty() {
            let mut dir = path;
            while !dir.is_dir() {
                match dir.parent() {
                    Some(p) => dir = p,
                    None => break,
                }
            }
            if dir.is_dir() {
                discovered = Self::discover_partition_columns_from_path(dir);
            }
        }

        let partition_columns: Vec<String> = discovered
            .into_iter()
            .filter(|c| schema.contains(c.as_str()))
            .collect();

        let new_order: Vec<String> = if partition_columns.is_empty() {
            schema.iter_names().map(|s| s.to_string()).collect()
        } else {
            let part_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
            let all_names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
            let rest: Vec<String> = all_names
                .into_iter()
                .filter(|c| !part_set.contains(c.as_str()))
                .collect();
            partition_columns.iter().cloned().chain(rest).collect()
        };

        if !partition_columns.is_empty() {
            let exprs: Vec<Expr> = new_order.iter().map(|s| col(s.as_str())).collect();
            lf = lf.select(exprs);
        }

        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        state.partition_columns = if partition_columns.is_empty() {
            None
        } else {
            Some(partition_columns)
        };
        // Ensure display order is partition-first (Self::new uses schema order; be explicit).
        state.set_column_order(new_order);
        Ok(state)
    }

    pub fn set_row_numbers(&mut self, enabled: bool) {
        self.row_numbers = enabled;
    }

    pub fn toggle_row_numbers(&mut self) {
        self.row_numbers = !self.row_numbers;
    }

    /// Row number display start (0 or 1); used by go-to-line to interpret user input.
    pub fn row_start_index(&self) -> usize {
        self.row_start_index
    }

    /// Decompress a compressed file to a temp file for lazy CSV scan.
    fn decompress_compressed_csv_to_temp(
        path: &Path,
        compression: CompressionFormat,
        temp_dir: &Path,
    ) -> Result<NamedTempFile> {
        let mut temp = NamedTempFile::new_in(temp_dir)?;
        let out = temp.as_file_mut();
        let mut reader: Box<dyn Read> = match compression {
            CompressionFormat::Gzip => {
                let f = File::open(path)?;
                Box::new(flate2::read::GzDecoder::new(BufReader::new(f)))
            }
            CompressionFormat::Zstd => {
                let f = File::open(path)?;
                Box::new(zstd::Decoder::new(BufReader::new(f))?)
            }
            CompressionFormat::Bzip2 => {
                let f = File::open(path)?;
                Box::new(bzip2::read::BzDecoder::new(BufReader::new(f)))
            }
            CompressionFormat::Xz => {
                let f = File::open(path)?;
                Box::new(xz2::read::XzDecoder::new(BufReader::new(f)))
            }
        };
        std::io::copy(&mut reader, out)?;
        out.sync_all()?;
        Ok(temp)
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        // Determine compression format: explicit option, or auto-detect from extension
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path));

        if let Some(compression) = compression {
            if options.decompress_in_memory {
                // Eager read: decompress into memory, then CSV read
                match compression {
                    CompressionFormat::Gzip | CompressionFormat::Zstd => {
                        let mut read_options = CsvReadOptions::default();
                        if let Some(skip_lines) = options.skip_lines {
                            read_options.skip_lines = skip_lines;
                        }
                        if let Some(skip_rows) = options.skip_rows {
                            read_options.skip_rows = skip_rows;
                        }
                        if let Some(has_header) = options.has_header {
                            read_options.has_header = has_header;
                        }
                        read_options = read_options.map_parse_options(|opts| {
                            opts.with_try_parse_dates(options.parse_dates)
                        });
                        let df = read_options
                            .try_into_reader_with_file_path(Some(path.into()))?
                            .finish()?;
                        let lf = df.lazy();
                        let mut state = Self::new(
                            lf,
                            options.pages_lookahead,
                            options.pages_lookback,
                            options.max_buffered_rows,
                            options.max_buffered_mb,
                        )?;
                        state.row_numbers = options.row_numbers;
                        state.row_start_index = options.row_start_index;
                        Ok(state)
                    }
                    CompressionFormat::Bzip2 => {
                        let file = File::open(path)?;
                        let mut decoder = bzip2::read::BzDecoder::new(BufReader::new(file));
                        let mut decompressed = Vec::new();
                        decoder.read_to_end(&mut decompressed)?;
                        let mut read_options = CsvReadOptions::default();
                        if let Some(skip_lines) = options.skip_lines {
                            read_options.skip_lines = skip_lines;
                        }
                        if let Some(skip_rows) = options.skip_rows {
                            read_options.skip_rows = skip_rows;
                        }
                        if let Some(has_header) = options.has_header {
                            read_options.has_header = has_header;
                        }
                        read_options = read_options.map_parse_options(|opts| {
                            opts.with_try_parse_dates(options.parse_dates)
                        });
                        let df = CsvReader::new(std::io::Cursor::new(decompressed))
                            .with_options(read_options)
                            .finish()?;
                        let lf = df.lazy();
                        let mut state = Self::new(
                            lf,
                            options.pages_lookahead,
                            options.pages_lookback,
                            options.max_buffered_rows,
                            options.max_buffered_mb,
                        )?;
                        state.row_numbers = options.row_numbers;
                        state.row_start_index = options.row_start_index;
                        Ok(state)
                    }
                    CompressionFormat::Xz => {
                        let file = File::open(path)?;
                        let mut decoder = xz2::read::XzDecoder::new(BufReader::new(file));
                        let mut decompressed = Vec::new();
                        decoder.read_to_end(&mut decompressed)?;
                        let mut read_options = CsvReadOptions::default();
                        if let Some(skip_lines) = options.skip_lines {
                            read_options.skip_lines = skip_lines;
                        }
                        if let Some(skip_rows) = options.skip_rows {
                            read_options.skip_rows = skip_rows;
                        }
                        if let Some(has_header) = options.has_header {
                            read_options.has_header = has_header;
                        }
                        read_options = read_options.map_parse_options(|opts| {
                            opts.with_try_parse_dates(options.parse_dates)
                        });
                        let df = CsvReader::new(std::io::Cursor::new(decompressed))
                            .with_options(read_options)
                            .finish()?;
                        let lf = df.lazy();
                        let mut state = Self::new(
                            lf,
                            options.pages_lookahead,
                            options.pages_lookback,
                            options.max_buffered_rows,
                            options.max_buffered_mb,
                        )?;
                        state.row_numbers = options.row_numbers;
                        state.row_start_index = options.row_start_index;
                        Ok(state)
                    }
                }
            } else {
                // Decompress to temp file, then lazy scan
                let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
                let temp = Self::decompress_compressed_csv_to_temp(path, compression, &temp_dir)?;
                let mut state = Self::from_csv_customize(
                    temp.path(),
                    options.pages_lookahead,
                    options.pages_lookback,
                    options.max_buffered_rows,
                    options.max_buffered_mb,
                    |mut reader| {
                        if let Some(skip_lines) = options.skip_lines {
                            reader = reader.with_skip_lines(skip_lines);
                        }
                        if let Some(skip_rows) = options.skip_rows {
                            reader = reader.with_skip_rows(skip_rows);
                        }
                        if let Some(has_header) = options.has_header {
                            reader = reader.with_has_header(has_header);
                        }
                        reader = reader.with_try_parse_dates(options.parse_dates);
                        reader
                    },
                )?;
                state.row_numbers = options.row_numbers;
                state.row_start_index = options.row_start_index;
                state.decompress_temp_file = Some(temp);
                Ok(state)
            }
        } else {
            // For uncompressed files, use lazy scanning (more efficient)
            let mut state = Self::from_csv_customize(
                path,
                options.pages_lookahead,
                options.pages_lookback,
                options.max_buffered_rows,
                options.max_buffered_mb,
                |mut reader| {
                    if let Some(skip_lines) = options.skip_lines {
                        reader = reader.with_skip_lines(skip_lines);
                    }
                    if let Some(skip_rows) = options.skip_rows {
                        reader = reader.with_skip_rows(skip_rows);
                    }
                    if let Some(has_header) = options.has_header {
                        reader = reader.with_has_header(has_header);
                    }
                    reader = reader.with_try_parse_dates(options.parse_dates);
                    reader
                },
            )?;
            state.row_numbers = options.row_numbers;
            Ok(state)
        }
    }

    pub fn from_csv_customize<F>(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        func: F,
    ) -> Result<Self>
    where
        F: FnOnce(LazyCsvReader) -> LazyCsvReader,
    {
        let pl_path = PlPath::Local(Arc::from(path));
        let reader = LazyCsvReader::new(pl_path);
        let lf = func(reader).finish()?;
        Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )
    }

    /// Load multiple CSV files (uncompressed) and concatenate into one LazyFrame.
    pub fn from_csv_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_csv(paths[0].as_ref(), options);
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlPath::Local(Arc::from(p.as_ref()));
            let mut reader = LazyCsvReader::new(pl_path);
            if let Some(skip_lines) = options.skip_lines {
                reader = reader.with_skip_lines(skip_lines);
            }
            if let Some(skip_rows) = options.skip_rows {
                reader = reader.with_skip_rows(skip_rows);
            }
            if let Some(has_header) = options.has_header {
                reader = reader.with_has_header(has_header);
            }
            reader = reader.with_try_parse_dates(options.parse_dates);
            let lf = reader.finish()?;
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            options.pages_lookahead,
            options.pages_lookback,
            options.max_buffered_rows,
            options.max_buffered_mb,
        )?;
        state.row_numbers = options.row_numbers;
        state.row_start_index = options.row_start_index;
        Ok(state)
    }

    pub fn from_ndjson(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyJsonLineReader::new(pl_path).finish()?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load multiple NDJSON files and concatenate into one LazyFrame.
    pub fn from_ndjson_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_ndjson(
                paths[0].as_ref(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                row_numbers,
                row_start_index,
            );
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlPath::Local(Arc::from(p.as_ref()));
            let lf = LazyJsonLineReader::new(pl_path).finish()?;
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    pub fn from_json(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        Self::from_json_with_format(
            path,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            row_numbers,
            row_start_index,
            JsonFormat::Json,
        )
    }

    pub fn from_json_lines(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        Self::from_json_with_format(
            path,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            row_numbers,
            row_start_index,
            JsonFormat::JsonLines,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_json_with_format(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
        format: JsonFormat,
    ) -> Result<Self> {
        let file = File::open(path)?;
        let lf = JsonReader::new(file)
            .with_json_format(format)
            .finish()?
            .lazy();
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Load multiple JSON (array) files and concatenate into one LazyFrame.
    pub fn from_json_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        Self::from_json_with_format_paths(
            paths,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            row_numbers,
            row_start_index,
            JsonFormat::Json,
        )
    }

    /// Load multiple JSON Lines files and concatenate into one LazyFrame.
    pub fn from_json_lines_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        Self::from_json_with_format_paths(
            paths,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            row_numbers,
            row_start_index,
            JsonFormat::JsonLines,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn from_json_with_format_paths(
        paths: &[impl AsRef<Path>],
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
        format: JsonFormat,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_json_with_format(
                paths[0].as_ref(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                row_numbers,
                row_start_index,
                format,
            );
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let file = File::open(p.as_ref())?;
            let lf = match &format {
                JsonFormat::Json => JsonReader::new(file)
                    .with_json_format(JsonFormat::Json)
                    .finish()?
                    .lazy(),
                JsonFormat::JsonLines => JsonReader::new(file)
                    .with_json_format(JsonFormat::JsonLines)
                    .finish()?
                    .lazy(),
            };
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn from_delimited(
        path: &Path,
        delimiter: u8,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let reader = LazyCsvReader::new(pl_path).with_separator(delimiter);
        let lf = reader.finish()?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Returns true if a scroll by `rows` would trigger a collect (view would leave the buffer).
    /// Used so the UI only shows the throbber when actual data loading will occur.
    pub fn scroll_would_trigger_collect(&self, rows: i64) -> bool {
        if rows < 0 && self.start_row == 0 {
            return false;
        }
        let new_start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.df.as_ref() {
                if rows > 0 && df.shape().0 <= self.visible_rows {
                    return false;
                }
            }
            (self.start_row as i64 + rows) as usize
        };
        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;
        !within_buffer
    }

    fn slide_table(&mut self, rows: i64) {
        if rows < 0 && self.start_row == 0 {
            return;
        }

        let new_start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.df.as_ref() {
                if rows > 0 && df.shape().0 <= self.visible_rows {
                    return;
                }
            }
            (self.start_row as i64 + rows) as usize
        };

        // Call collect() only when view is outside buffer; otherwise just update start_row.
        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        if within_buffer {
            self.start_row = new_start_row;
            return;
        }

        self.start_row = new_start_row;
        self.collect();
    }

    pub fn collect(&mut self) {
        // Update proximity threshold based on visible rows
        if self.visible_rows > 0 {
            self.proximity_threshold = self.visible_rows;
        }

        // Run len() only when lf has changed (query, filter, sort, pivot, melt, reset, drill).
        if !self.num_rows_valid {
            self.num_rows = match self.lf.clone().select([len()]).collect() {
                Ok(df) => {
                    if let Some(col) = df.get(0) {
                        if let Some(AnyValue::UInt32(len)) = col.first() {
                            *len as usize
                        } else {
                            0
                        }
                    } else {
                        0
                    }
                }
                Err(_) => 0,
            };
            self.num_rows_valid = true;
        }

        if self.num_rows > 0 {
            let max_start = self.num_rows.saturating_sub(1);
            if self.start_row > max_start {
                self.start_row = max_start;
            }
        } else {
            self.start_row = 0;
            self.buffered_start_row = 0;
            self.buffered_end_row = 0;
            self.df = None;
            self.locked_df = None;
            return;
        }

        // Proximity-based buffer logic
        let view_start = self.start_row;
        let view_end = self.start_row + self.visible_rows.min(self.num_rows - self.start_row);

        // Check if current view is within buffered range
        let within_buffer = view_start >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        // Buffer grows incrementally: initial load and each expansion add only a few pages (lookahead + lookback).
        // clamp_buffer_to_max_size caps at max_buffered_rows and slides the window when at cap.
        let page_rows = self.visible_rows.max(1);

        if within_buffer {
            let dist_to_start = view_start.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);

            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.buffered_end_row < self.num_rows;

            if !needs_expansion_back && !needs_expansion_forward {
                // Rebuild displayed columns from current buffer (termcol_index may have changed via scroll_left/scroll_right)
                self.load_buffer(self.buffered_start_row, self.buffered_end_row);
                if self.table_state.selected().is_none() {
                    self.table_state.select(Some(0));
                }
                return;
            }

            let mut new_buffer_start = if needs_expansion_back {
                view_start.saturating_sub(self.pages_lookback * page_rows)
            } else {
                self.buffered_start_row
            };

            let mut new_buffer_end = if needs_expansion_forward {
                (view_end + self.pages_lookahead * page_rows).min(self.num_rows)
            } else {
                self.buffered_end_row
            };

            self.clamp_buffer_to_max_size(
                view_start,
                view_end,
                &mut new_buffer_start,
                &mut new_buffer_end,
            );
            self.load_buffer(new_buffer_start, new_buffer_end);
        } else {
            // Outside buffer: either extend the previous buffer (so it grows) or load a fresh small window.
            // Only extend when the view is "close" to the existing buffer (e.g. user paged down a bit).
            // A big jump (e.g. jump to end) should load just a window around the new view, not extend
            // the buffer across the whole dataset.
            let mut new_buffer_start;
            let mut new_buffer_end;

            let had_buffer = self.buffered_end_row > 0;
            let scrolled_past_end = had_buffer && view_start >= self.buffered_end_row;
            let scrolled_past_start = had_buffer && view_end <= self.buffered_start_row;

            let extend_forward_ok = scrolled_past_end
                && (view_start - self.buffered_end_row) <= self.pages_lookahead * page_rows;
            let extend_backward_ok = scrolled_past_start
                && (self.buffered_start_row - view_end) <= self.pages_lookback * page_rows;

            if extend_forward_ok {
                // View is just a few pages past buffer end; extend forward.
                new_buffer_start = self.buffered_start_row;
                new_buffer_end = (view_end + self.pages_lookahead * page_rows).min(self.num_rows);
            } else if extend_backward_ok {
                // View is just a few pages before buffer start; extend backward.
                new_buffer_start = view_start.saturating_sub(self.pages_lookback * page_rows);
                new_buffer_end = self.buffered_end_row;
            } else if scrolled_past_end || scrolled_past_start {
                // Big jump (e.g. jump to end or jump to start): load a fresh window around the view.
                new_buffer_start = view_start.saturating_sub(self.pages_lookback * page_rows);
                new_buffer_end = (view_end + self.pages_lookahead * page_rows).min(self.num_rows);
                let min_initial_len = (1 + self.pages_lookahead + self.pages_lookback) * page_rows;
                let current_len = new_buffer_end.saturating_sub(new_buffer_start);
                if current_len < min_initial_len {
                    let need = min_initial_len.saturating_sub(current_len);
                    let can_extend_end = self.num_rows.saturating_sub(new_buffer_end);
                    let can_extend_start = new_buffer_start;
                    if can_extend_end >= need {
                        new_buffer_end = (new_buffer_end + need).min(self.num_rows);
                    } else if can_extend_start >= need {
                        new_buffer_start = new_buffer_start.saturating_sub(need);
                    } else {
                        new_buffer_end = (new_buffer_end + can_extend_end).min(self.num_rows);
                        new_buffer_start =
                            new_buffer_start.saturating_sub(need.saturating_sub(can_extend_end));
                    }
                }
            } else {
                // No buffer yet or big jump: load a fresh small window (view ± a few pages).
                new_buffer_start = view_start.saturating_sub(self.pages_lookback * page_rows);
                new_buffer_end = (view_end + self.pages_lookahead * page_rows).min(self.num_rows);

                // Ensure at least (1 + lookahead + lookback) pages so buffer size is consistent (e.g. 364 at 52 visible).
                let min_initial_len = (1 + self.pages_lookahead + self.pages_lookback) * page_rows;
                let current_len = new_buffer_end.saturating_sub(new_buffer_start);
                if current_len < min_initial_len {
                    let need = min_initial_len.saturating_sub(current_len);
                    let can_extend_end = self.num_rows.saturating_sub(new_buffer_end);
                    let can_extend_start = new_buffer_start;
                    if can_extend_end >= need {
                        new_buffer_end = (new_buffer_end + need).min(self.num_rows);
                    } else if can_extend_start >= need {
                        new_buffer_start = new_buffer_start.saturating_sub(need);
                    } else {
                        new_buffer_end = (new_buffer_end + can_extend_end).min(self.num_rows);
                        new_buffer_start =
                            new_buffer_start.saturating_sub(need.saturating_sub(can_extend_end));
                    }
                }
            }

            self.clamp_buffer_to_max_size(
                view_start,
                view_end,
                &mut new_buffer_start,
                &mut new_buffer_end,
            );
            self.load_buffer(new_buffer_start, new_buffer_end);
        }

        self.slice_from_buffer();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
    }

    /// Invalidate num_rows cache when lf is mutated.
    fn invalidate_num_rows(&mut self) {
        self.num_rows_valid = false;
    }

    /// Clamp buffer to max_buffered_rows; when at cap, slide window to keep view inside.
    fn clamp_buffer_to_max_size(
        &self,
        view_start: usize,
        view_end: usize,
        buffer_start: &mut usize,
        buffer_end: &mut usize,
    ) {
        if self.max_buffered_rows == 0 {
            return;
        }
        let max_len = self.max_buffered_rows;
        let requested_len = buffer_end.saturating_sub(*buffer_start);
        if requested_len <= max_len {
            return;
        }
        let view_len = view_end.saturating_sub(view_start);
        if view_len >= max_len {
            *buffer_start = view_start;
            *buffer_end = (view_start + max_len).min(self.num_rows);
        } else {
            let half = (max_len - view_len) / 2;
            *buffer_end = (view_end + half).min(self.num_rows);
            *buffer_start = (*buffer_end).saturating_sub(max_len);
            if *buffer_start > view_start {
                *buffer_start = view_start;
            }
            *buffer_end = (*buffer_start + max_len).min(self.num_rows);
        }
    }

    fn load_buffer(&mut self, buffer_start: usize, buffer_end: usize) {
        let buffer_size = buffer_end.saturating_sub(buffer_start);
        if buffer_size == 0 {
            return;
        }

        let all_columns: Vec<_> = self
            .column_order
            .iter()
            .map(|name| col(name.as_str()))
            .collect();

        let mut full_df = match self
            .lf
            .clone()
            .select(all_columns)
            .slice(buffer_start as i64, buffer_size as u32)
            .collect()
        {
            Ok(df) => df,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };

        let mut effective_buffer_end = buffer_end;
        if self.max_buffered_mb > 0 {
            let size = full_df.estimated_size();
            let max_bytes = self.max_buffered_mb * 1024 * 1024;
            if size > max_bytes {
                let rows = full_df.height();
                if rows > 0 {
                    let bytes_per_row = size / rows;
                    let max_rows = (max_bytes / bytes_per_row.max(1)).min(rows);
                    if max_rows < rows {
                        full_df = full_df.slice(0, max_rows);
                        effective_buffer_end = buffer_start + max_rows;
                    }
                }
            }
        }

        if self.locked_columns_count > 0 {
            let locked_names: Vec<&str> = self
                .column_order
                .iter()
                .take(self.locked_columns_count)
                .map(|s| s.as_str())
                .collect();
            let locked_df = match full_df.select(locked_names) {
                Ok(df) => df,
                Err(e) => {
                    self.error = Some(e);
                    return;
                }
            };
            self.locked_df = if self.is_grouped() {
                match self.format_grouped_dataframe(locked_df) {
                    Ok(formatted_df) => Some(formatted_df),
                    Err(e) => {
                        self.error = Some(PolarsError::ComputeError(
                            crate::error_display::user_message_from_report(&e, None).into(),
                        ));
                        return;
                    }
                }
            } else {
                Some(locked_df)
            };
        } else {
            self.locked_df = None;
        }

        let scroll_names: Vec<&str> = self
            .column_order
            .iter()
            .skip(self.locked_columns_count + self.termcol_index)
            .map(|s| s.as_str())
            .collect();
        if scroll_names.is_empty() {
            self.df = None;
        } else {
            let scroll_df = match full_df.select(scroll_names) {
                Ok(df) => df,
                Err(e) => {
                    self.error = Some(e);
                    return;
                }
            };
            self.df = if self.is_grouped() {
                match self.format_grouped_dataframe(scroll_df) {
                    Ok(formatted_df) => Some(formatted_df),
                    Err(e) => {
                        self.error = Some(PolarsError::ComputeError(
                            crate::error_display::user_message_from_report(&e, None).into(),
                        ));
                        return;
                    }
                }
            } else {
                Some(scroll_df)
            };
        }
        if self.error.is_some() {
            self.error = None;
        }
        self.buffered_start_row = buffer_start;
        self.buffered_end_row = effective_buffer_end;
    }

    fn slice_from_buffer(&mut self) {
        // Buffer contains the full range [buffered_start_row, buffered_end_row)
        // The displayed portion [start_row, start_row + visible_rows) is a subset
        // We'll slice the displayed portion when rendering based on offset
        // No action needed here - the buffer is stored, slicing happens at render time
    }

    fn format_grouped_dataframe(&self, df: DataFrame) -> Result<DataFrame> {
        let schema = df.schema();
        let mut new_series = Vec::new();

        for (col_name, dtype) in schema.iter() {
            let col = df.column(col_name)?;
            if matches!(dtype, DataType::List(_)) {
                let string_series: Series = col
                    .list()?
                    .into_iter()
                    .map(|opt_list| {
                        opt_list.map(|list_series| {
                            let values: Vec<String> = list_series
                                .iter()
                                .take(10)
                                .map(|v| v.str_value().to_string())
                                .collect();
                            if list_series.len() > 10 {
                                format!("[{}...] ({} items)", values.join(", "), list_series.len())
                            } else {
                                format!("[{}]", values.join(", "))
                            }
                        })
                    })
                    .collect();
                new_series.push(string_series.with_name(col_name.as_str().into()).into());
            } else {
                new_series.push(col.clone());
            }
        }

        Ok(DataFrame::new(new_series)?)
    }

    pub fn select_next(&mut self) {
        self.table_state.select_next();
        if let Some(selected) = self.table_state.selected() {
            if selected >= self.visible_rows && self.visible_rows > 0 {
                self.slide_table(1);
            }
        }
    }

    pub fn page_down(&mut self) {
        self.slide_table(self.visible_rows as i64);
    }

    pub fn select_previous(&mut self) {
        if let Some(selected) = self.table_state.selected() {
            self.table_state.select_previous();
            if selected == 0 && self.start_row > 0 {
                self.slide_table(-1);
            }
        } else {
            self.table_state.select(Some(0));
        }
    }

    pub fn scroll_to(&mut self, index: usize) {
        if self.start_row == index {
            return;
        }

        if index == 0 {
            self.start_row = 0;
            self.collect();
            self.start_row = 0;
        } else {
            self.start_row = index;
            self.collect();
        }
    }

    /// Scroll so that the given row index is centered in the view when possible (respects table bounds).
    /// Selects that row. Used by go-to-line.
    pub fn scroll_to_row_centered(&mut self, row_index: usize) {
        self.ensure_num_rows();
        if self.num_rows == 0 || self.visible_rows == 0 {
            return;
        }
        let center_offset = self.visible_rows / 2;
        let mut start_row = row_index.saturating_sub(center_offset);
        let max_start = self.num_rows.saturating_sub(self.visible_rows);
        start_row = start_row.min(max_start);

        if self.start_row == start_row {
            let display_idx = row_index
                .saturating_sub(start_row)
                .min(self.visible_rows.saturating_sub(1));
            self.table_state.select(Some(display_idx));
            return;
        }

        self.start_row = start_row;
        self.collect();
        let display_idx = row_index
            .saturating_sub(start_row)
            .min(self.visible_rows.saturating_sub(1));
        self.table_state.select(Some(display_idx));
    }

    /// Ensure num_rows is up to date (runs len() query if needed). Used before scroll_to_end.
    fn ensure_num_rows(&mut self) {
        if self.num_rows_valid {
            return;
        }
        if self.visible_rows > 0 {
            self.proximity_threshold = self.visible_rows;
        }
        self.num_rows = match self.lf.clone().select([len()]).collect() {
            Ok(df) => {
                if let Some(col) = df.get(0) {
                    if let Some(AnyValue::UInt32(len)) = col.first() {
                        *len as usize
                    } else {
                        0
                    }
                } else {
                    0
                }
            }
            Err(_) => 0,
        };
        self.num_rows_valid = true;
    }

    /// Jump to the last page; buffer is trimmed/loaded as needed. Selects the last row.
    pub fn scroll_to_end(&mut self) {
        self.ensure_num_rows();
        if self.num_rows == 0 {
            self.start_row = 0;
            self.buffered_start_row = 0;
            self.buffered_end_row = 0;
            return;
        }
        let end_start = self.num_rows.saturating_sub(self.visible_rows);
        if self.start_row == end_start {
            self.select_last_visible_row();
            return;
        }
        self.start_row = end_start;
        self.collect();
        self.select_last_visible_row();
    }

    /// Set table selection to the last row in the current view (for use after scroll_to_end).
    fn select_last_visible_row(&mut self) {
        if self.num_rows == 0 {
            return;
        }
        let last_row_display_idx = (self.num_rows - 1).saturating_sub(self.start_row);
        let sel = last_row_display_idx.min(self.visible_rows.saturating_sub(1));
        self.table_state.select(Some(sel));
    }

    pub fn half_page_down(&mut self) {
        let half = (self.visible_rows / 2).max(1) as i64;
        self.slide_table(half);
    }

    pub fn half_page_up(&mut self) {
        if self.start_row == 0 {
            return;
        }
        let half = (self.visible_rows / 2).max(1) as i64;
        self.slide_table(-half);
    }

    pub fn page_up(&mut self) {
        if self.start_row == 0 {
            return;
        }
        self.slide_table(-(self.visible_rows as i64));
    }

    pub fn scroll_right(&mut self) {
        let max_scroll = self
            .column_order
            .len()
            .saturating_sub(self.locked_columns_count);
        if self.termcol_index < max_scroll.saturating_sub(1) {
            self.termcol_index += 1;
            self.collect();
        }
    }

    pub fn scroll_left(&mut self) {
        if self.termcol_index > 0 {
            self.termcol_index -= 1;
            self.collect();
        }
    }

    pub fn headers(&self) -> Vec<String> {
        self.column_order.clone()
    }

    pub fn set_column_order(&mut self, order: Vec<String>) {
        self.column_order = order;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.collect();
    }

    pub fn set_locked_columns(&mut self, count: usize) {
        self.locked_columns_count = count.min(self.column_order.len());
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.collect();
    }

    pub fn locked_columns_count(&self) -> usize {
        self.locked_columns_count
    }

    // Getter methods for template creation
    pub fn get_filters(&self) -> &[FilterStatement] {
        &self.filters
    }

    pub fn get_sort_columns(&self) -> &[String] {
        &self.sort_columns
    }

    pub fn get_sort_ascending(&self) -> bool {
        self.sort_ascending
    }

    pub fn get_column_order(&self) -> &[String] {
        &self.column_order
    }

    pub fn get_active_query(&self) -> &str {
        &self.active_query
    }

    pub fn last_pivot_spec(&self) -> Option<&PivotSpec> {
        self.last_pivot_spec.as_ref()
    }

    pub fn last_melt_spec(&self) -> Option<&MeltSpec> {
        self.last_melt_spec.as_ref()
    }

    pub fn is_grouped(&self) -> bool {
        self.schema
            .iter()
            .any(|(_, dtype)| matches!(dtype, DataType::List(_)))
    }

    pub fn group_key_columns(&self) -> Vec<String> {
        self.schema
            .iter()
            .filter(|(_, dtype)| !matches!(dtype, DataType::List(_)))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    pub fn group_value_columns(&self) -> Vec<String> {
        self.schema
            .iter()
            .filter(|(_, dtype)| matches!(dtype, DataType::List(_)))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// Estimated heap size in bytes of the currently buffered slice (locked + scrollable), if collected.
    pub fn buffered_memory_bytes(&self) -> Option<usize> {
        let locked = self
            .locked_df
            .as_ref()
            .map(|df| df.estimated_size())
            .unwrap_or(0);
        let scroll = self.df.as_ref().map(|df| df.estimated_size()).unwrap_or(0);
        if locked == 0 && scroll == 0 {
            None
        } else {
            Some(locked + scroll)
        }
    }

    /// Number of rows currently in the buffer. 0 if no buffer loaded.
    pub fn buffered_rows(&self) -> usize {
        self.buffered_end_row
            .saturating_sub(self.buffered_start_row)
    }

    /// Maximum buffer size in rows (0 = no limit).
    pub fn max_buffered_rows(&self) -> usize {
        self.max_buffered_rows
    }

    /// Maximum buffer size in MiB (0 = no limit).
    pub fn max_buffered_mb(&self) -> usize {
        self.max_buffered_mb
    }

    pub fn drill_down_into_group(&mut self, group_index: usize) -> Result<()> {
        if !self.is_grouped() {
            return Ok(());
        }

        self.grouped_lf = Some(self.lf.clone());

        let grouped_df = self.lf.clone().collect()?;

        if group_index >= grouped_df.height() {
            return Err(color_eyre::eyre::eyre!("Group index out of bounds"));
        }

        let key_columns = self.group_key_columns();
        let mut key_values = Vec::new();
        for col_name in &key_columns {
            let col = grouped_df.column(col_name)?;
            let value = col.get(group_index).map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Group index {} out of bounds for column {}: {}",
                    group_index,
                    col_name,
                    e
                )
            })?;
            key_values.push(value.str_value().to_string());
        }
        self.drilled_down_group_key = Some(key_values.clone());
        self.drilled_down_group_key_columns = Some(key_columns.clone());

        let value_columns = self.group_value_columns();
        if value_columns.is_empty() {
            return Err(color_eyre::eyre::eyre!("No value columns in grouped data"));
        }

        let mut columns = Vec::new();

        let first_value_col = grouped_df.column(&value_columns[0])?;
        let first_list_value = first_value_col.get(group_index).map_err(|e| {
            color_eyre::eyre::eyre!("Group index {} out of bounds: {}", group_index, e)
        })?;
        let row_count = if let AnyValue::List(list_series) = first_list_value {
            list_series.len()
        } else {
            0
        };

        for col_name in &key_columns {
            let col = grouped_df.column(col_name)?;
            let value = col.get(group_index).map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Group index {} out of bounds for column {}: {}",
                    group_index,
                    col_name,
                    e
                )
            })?;
            let constant_series = match value {
                AnyValue::Int32(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                AnyValue::Int64(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                AnyValue::UInt32(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                AnyValue::UInt64(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                AnyValue::Float32(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                AnyValue::Float64(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                AnyValue::String(v) => {
                    Series::new(col_name.as_str().into(), vec![v.to_string(); row_count])
                }
                AnyValue::Boolean(v) => Series::new(col_name.as_str().into(), vec![v; row_count]),
                _ => {
                    let str_val = value.str_value().to_string();
                    Series::new(col_name.as_str().into(), vec![str_val; row_count])
                }
            };
            columns.push(constant_series.into());
        }

        for col_name in &value_columns {
            let col = grouped_df.column(col_name)?;
            let value = col.get(group_index).map_err(|e| {
                color_eyre::eyre::eyre!(
                    "Group index {} out of bounds for column {}: {}",
                    group_index,
                    col_name,
                    e
                )
            })?;
            if let AnyValue::List(list_series) = value {
                let named_series = list_series.with_name(col_name.as_str().into());
                columns.push(named_series.into());
            }
        }

        let group_df = DataFrame::new(columns)?;

        self.invalidate_num_rows();
        self.lf = group_df.lazy();
        self.schema = self.lf.clone().collect_schema()?;
        self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();
        self.drilled_down_group_index = Some(group_index);
        self.start_row = 0;
        self.termcol_index = 0;
        self.locked_columns_count = 0;
        self.table_state.select(Some(0));
        self.collect();

        Ok(())
    }

    pub fn drill_up(&mut self) -> Result<()> {
        if let Some(grouped_lf) = self.grouped_lf.take() {
            self.invalidate_num_rows();
            self.lf = grouped_lf;
            self.schema = self.lf.clone().collect_schema()?;
            self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();
            self.drilled_down_group_index = None;
            self.drilled_down_group_key = None;
            self.drilled_down_group_key_columns = None;
            self.start_row = 0;
            self.termcol_index = 0;
            self.locked_columns_count = 0;
            self.table_state.select(Some(0));
            self.collect();
            Ok(())
        } else {
            Err(color_eyre::eyre::eyre!("Not in drill-down mode"))
        }
    }

    pub fn get_analysis_dataframe(&self) -> Result<DataFrame> {
        Ok(self.lf.clone().collect()?)
    }

    pub fn get_analysis_context(&self) -> crate::statistics::AnalysisContext {
        crate::statistics::AnalysisContext {
            has_query: !self.active_query.is_empty(),
            query: self.active_query.clone(),
            has_filters: !self.filters.is_empty(),
            filter_count: self.filters.len(),
            is_drilled_down: self.is_drilled_down(),
            group_key: self.drilled_down_group_key.clone(),
            group_columns: self.drilled_down_group_key_columns.clone(),
        }
    }

    /// Pivot the current `LazyFrame` (long → wide). Never uses `original_lf`.
    /// Collects current `lf`, runs `pivot_stable`, then replaces `lf` with result.
    /// We use pivot_stable for all aggregation types: Polars' non-stable pivot() prints
    /// "unstable pivot not yet supported, using stable pivot" to stdout, which corrupts the TUI.
    pub fn pivot(&mut self, spec: &PivotSpec) -> Result<()> {
        let df = self.lf.clone().collect()?;
        let agg_expr = pivot_agg_expr(spec.aggregation)?;
        let index_str: Vec<&str> = spec.index.iter().map(|s| s.as_str()).collect();
        let index_opt = if index_str.is_empty() {
            None
        } else {
            Some(index_str)
        };
        let pivoted = pivot_stable(
            &df,
            [spec.pivot_column.as_str()],
            index_opt,
            Some([spec.value_column.as_str()]),
            spec.sort_columns,
            Some(agg_expr),
            None,
        )?;
        self.last_pivot_spec = Some(spec.clone());
        self.last_melt_spec = None;
        self.replace_lf_after_reshape(pivoted.lazy())?;
        Ok(())
    }

    /// Melt the current `LazyFrame` (wide → long). Never uses `original_lf`.
    pub fn melt(&mut self, spec: &MeltSpec) -> Result<()> {
        let on = cols(spec.value_columns.iter().map(|s| s.as_str()));
        let index = cols(spec.index.iter().map(|s| s.as_str()));
        let args = UnpivotArgsDSL {
            on,
            index,
            variable_name: Some(PlSmallStr::from(spec.variable_name.as_str())),
            value_name: Some(PlSmallStr::from(spec.value_name.as_str())),
        };
        let lf = self.lf.clone().unpivot(args);
        self.last_melt_spec = Some(spec.clone());
        self.last_pivot_spec = None;
        self.replace_lf_after_reshape(lf)?;
        Ok(())
    }

    fn replace_lf_after_reshape(&mut self, lf: LazyFrame) -> Result<()> {
        self.invalidate_num_rows();
        self.lf = lf;
        self.schema = self.lf.clone().collect_schema()?;
        self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();
        self.filters.clear();
        self.sort_columns.clear();
        self.active_query.clear();
        self.error = None;
        self.df = None;
        self.locked_df = None;
        self.grouped_lf = None;
        self.drilled_down_group_index = None;
        self.drilled_down_group_key = None;
        self.drilled_down_group_key_columns = None;
        self.start_row = 0;
        self.termcol_index = 0;
        self.locked_columns_count = 0;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.table_state.select(Some(0));
        self.collect();
        Ok(())
    }

    pub fn is_drilled_down(&self) -> bool {
        self.drilled_down_group_index.is_some()
    }

    fn apply_transformations(&mut self) {
        let mut lf = self.lf.clone();
        let mut final_expr: Option<Expr> = None;

        for filter in &self.filters {
            let col_expr = col(&filter.column);
            let val_lit = if let Some(dtype) = self.schema.get(&filter.column) {
                match dtype {
                    DataType::Float32 | DataType::Float64 => filter
                        .value
                        .parse::<f64>()
                        .map(lit)
                        .unwrap_or_else(|_| lit(filter.value.as_str())),
                    DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => filter
                        .value
                        .parse::<i64>()
                        .map(lit)
                        .unwrap_or_else(|_| lit(filter.value.as_str())),
                    DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                        filter
                            .value
                            .parse::<u64>()
                            .map(lit)
                            .unwrap_or_else(|_| lit(filter.value.as_str()))
                    }
                    DataType::Boolean => filter
                        .value
                        .parse::<bool>()
                        .map(lit)
                        .unwrap_or_else(|_| lit(filter.value.as_str())),
                    _ => lit(filter.value.as_str()),
                }
            } else {
                lit(filter.value.as_str())
            };

            let op_expr = match filter.operator {
                FilterOperator::Eq => col_expr.eq(val_lit),
                FilterOperator::NotEq => col_expr.neq(val_lit),
                FilterOperator::Gt => col_expr.gt(val_lit),
                FilterOperator::Lt => col_expr.lt(val_lit),
                FilterOperator::GtEq => col_expr.gt_eq(val_lit),
                FilterOperator::LtEq => col_expr.lt_eq(val_lit),
                FilterOperator::Contains => {
                    let val = filter.value.clone();
                    col_expr.str().contains_literal(lit(val))
                }
                FilterOperator::NotContains => {
                    let val = filter.value.clone();
                    col_expr.str().contains_literal(lit(val)).not()
                }
            };

            if let Some(current) = final_expr {
                final_expr = Some(match filter.logical_op {
                    LogicalOperator::And => current.and(op_expr),
                    LogicalOperator::Or => current.or(op_expr),
                });
            } else {
                final_expr = Some(op_expr);
            }
        }

        if let Some(e) = final_expr {
            lf = lf.filter(e);
        }

        if !self.sort_columns.is_empty() {
            let options = SortMultipleOptions {
                descending: self
                    .sort_columns
                    .iter()
                    .map(|_| !self.sort_ascending)
                    .collect(),
                ..Default::default()
            };
            lf = lf.sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                options,
            );
        } else if !self.sort_ascending {
            lf = lf.reverse();
        }

        self.invalidate_num_rows();
        self.lf = lf;
        self.collect();
    }

    pub fn sort(&mut self, columns: Vec<String>, ascending: bool) {
        self.sort_columns = columns;
        self.sort_ascending = ascending;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.apply_transformations();
    }

    pub fn reverse(&mut self) {
        self.sort_ascending = !self.sort_ascending;

        self.buffered_start_row = 0;
        self.buffered_end_row = 0;

        if !self.sort_columns.is_empty() {
            let options = SortMultipleOptions {
                descending: self
                    .sort_columns
                    .iter()
                    .map(|_| !self.sort_ascending)
                    .collect(),
                ..Default::default()
            };
            self.invalidate_num_rows();
            self.lf = self.lf.clone().sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                options,
            );
            self.collect();
        } else {
            self.invalidate_num_rows();
            self.lf = self.lf.clone().reverse();
            self.collect();
        }
    }

    pub fn filter(&mut self, filters: Vec<FilterStatement>) {
        self.filters = filters;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.apply_transformations();
    }

    pub fn query(&mut self, query: String) {
        self.error = None;

        let trimmed_query = query.trim();
        if trimmed_query.is_empty() {
            self.invalidate_num_rows();
            self.lf = self.original_lf.clone();
            self.schema = self
                .original_lf
                .clone()
                .collect_schema()
                .unwrap_or_else(|_| Arc::new(Schema::with_capacity(0)));
            self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();
            self.active_query.clear();
            self.locked_columns_count = 0;
            self.filters.clear();
            self.sort_columns.clear();
            self.sort_ascending = true;
            self.start_row = 0;
            self.termcol_index = 0;
            self.drilled_down_group_index = None;
            self.drilled_down_group_key = None;
            self.drilled_down_group_key_columns = None;
            self.grouped_lf = None;
            self.buffered_start_row = 0;
            self.buffered_end_row = 0;
            self.table_state.select(Some(0));
            self.collect();
            return;
        }

        match parse_query(&query) {
            Ok((cols, filter, group_by_cols, group_by_col_names)) => {
                let mut lf = self.original_lf.clone();

                // Apply filter first (where clause)
                if let Some(f) = filter {
                    lf = lf.filter(f);
                }

                if !group_by_cols.is_empty() {
                    if !cols.is_empty() {
                        lf = lf.group_by(group_by_cols.clone()).agg(cols);
                        lf = lf.sort_by_exprs(group_by_cols.clone(), Default::default());
                    } else {
                        let schema = match lf.clone().collect_schema() {
                            Ok(s) => s,
                            Err(e) => {
                                self.error = Some(e);
                                return; // Don't modify state on error
                            }
                        };
                        let all_columns: Vec<String> =
                            schema.iter_names().map(|s| s.to_string()).collect();

                        // In Polars, when you group_by and aggregate columns without explicit aggregation functions,
                        // Polars automatically collects the values as lists. We need to aggregate all columns
                        // except the group columns to avoid duplicates.
                        let mut agg_exprs = Vec::new();
                        for col_name in &all_columns {
                            if !group_by_col_names.contains(col_name) {
                                agg_exprs.push(col(col_name));
                            }
                        }

                        lf = lf.group_by(group_by_cols.clone()).agg(agg_exprs);
                        lf = lf.sort_by_exprs(group_by_cols.clone(), Default::default());
                    }
                } else if !cols.is_empty() {
                    lf = lf.select(cols);
                }

                let schema = match lf.collect_schema() {
                    Ok(schema) => schema,
                    Err(e) => {
                        self.error = Some(e);
                        return;
                    }
                };

                self.schema = schema;
                self.invalidate_num_rows();
                self.lf = lf;
                self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();

                // Lock grouped columns if by clause was used
                // Only lock the columns specified in the 'by' clause, not the value columns
                if !group_by_col_names.is_empty() {
                    // Group columns appear first in Polars results, so count consecutive
                    // columns from the start that are in group_by_col_names
                    let mut locked_count = 0;
                    for col_name in &self.column_order {
                        if group_by_col_names.contains(col_name) {
                            locked_count += 1;
                        } else {
                            // Once we hit a non-group column, we've passed all group columns
                            break;
                        }
                    }
                    self.locked_columns_count = locked_count;
                } else {
                    self.locked_columns_count = 0;
                }

                // Clear filters when using query
                self.filters.clear();
                self.sort_columns.clear();
                self.sort_ascending = true;
                self.start_row = 0;
                self.termcol_index = 0;
                self.active_query = query;
                self.buffered_start_row = 0;
                self.buffered_end_row = 0;
                // Reset drill-down state when applying new query
                self.drilled_down_group_index = None;
                self.drilled_down_group_key = None;
                self.drilled_down_group_key_columns = None;
                self.grouped_lf = None;
                // Reset table state selection
                self.table_state.select(Some(0));
                // Collect will clamp start_row to valid range, but we want to ensure it's 0
                // So we set it to 0, collect (which may clamp it), then ensure it's 0 again
                self.collect();
                // After collect(), ensure we're at the top (collect() may have clamped if num_rows was wrong)
                // But if num_rows > 0, we want start_row = 0 to show the first row
                if self.num_rows > 0 {
                    self.start_row = 0;
                }
            }
            Err(e) => {
                // Parse errors are already user-facing strings; store as ComputeError
                self.error = Some(PolarsError::ComputeError(e.into()));
            }
        }
    }
}

pub struct DataTable {
    pub header_bg: Color,
    pub header_fg: Color,
    pub row_numbers_fg: Color,
    pub separator_fg: Color,
    pub table_cell_padding: u16,
    pub alternate_row_bg: Option<Color>,
}

impl Default for DataTable {
    fn default() -> Self {
        Self {
            header_bg: Color::Indexed(236),
            header_fg: Color::White,
            row_numbers_fg: Color::DarkGray,
            separator_fg: Color::White,
            table_cell_padding: 1,
            alternate_row_bg: None,
        }
    }
}

/// Parameters for rendering the row numbers column.
struct RowNumbersParams {
    start_row: usize,
    visible_rows: usize,
    num_rows: usize,
    row_start_index: usize,
    selected_row: Option<usize>,
}

impl DataTable {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_colors(
        mut self,
        header_bg: Color,
        header_fg: Color,
        row_numbers_fg: Color,
        separator_fg: Color,
    ) -> Self {
        self.header_bg = header_bg;
        self.header_fg = header_fg;
        self.row_numbers_fg = row_numbers_fg;
        self.separator_fg = separator_fg;
        self
    }

    pub fn with_cell_padding(mut self, padding: u16) -> Self {
        self.table_cell_padding = padding;
        self
    }

    pub fn with_alternate_row_bg(mut self, color: Option<Color>) -> Self {
        self.alternate_row_bg = color;
        self
    }

    fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        _row_numbers: bool,
        _start_row_offset: usize,
    ) {
        // make each column as wide as it needs to be to fit the content
        let (height, cols) = df.shape();

        // widths starts at the length of each column naame
        let mut widths: Vec<u16> = df
            .get_column_names()
            .iter()
            .map(|name| name.chars().count() as u16)
            .collect();

        let mut used_width = 0;

        // rows is a vector initialized to a vector of lenth "height" empty rows
        let mut rows: Vec<Vec<Cell>> = vec![vec![]; height];
        let mut visible_columns = 0;

        for col_index in 0..cols {
            let mut max_len = widths[col_index];
            let col_data = &df[col_index];

            for (row_index, row) in rows
                .iter_mut()
                .take(height.min(if area.height > 1 {
                    area.height as usize - 1
                } else {
                    0
                }))
                .enumerate()
            {
                let value = col_data.get(row_index).unwrap();
                let val_str: Cow<str> = if matches!(value, AnyValue::Null) {
                    Cow::Borrowed("")
                } else {
                    value.str_value()
                };
                let len = val_str.chars().count() as u16;
                max_len = max_len.max(len);
                row.push(Cell::from(Line::from(val_str)));
            }

            // Use > not >= so the last column is shown when it fits exactly (no padding needed after it)
            let overflows = (used_width + max_len) > area.width;

            if overflows && col_data.dtype() == &DataType::String {
                let visible_width = area.width.saturating_sub(used_width);
                visible_columns += 1;
                widths[col_index] = visible_width;
                break;
            } else if !overflows {
                visible_columns += 1;
                widths[col_index] = max_len;
                used_width += max_len + self.table_cell_padding;
            } else {
                break;
            }
        }

        widths.truncate(visible_columns);
        // convert rows to a vector of Row, with optional alternate row background
        let rows: Vec<Row> = rows
            .into_iter()
            .enumerate()
            .map(|(row_index, mut row)| {
                row.truncate(visible_columns);
                let row_style = if row_index % 2 == 1 {
                    self.alternate_row_bg
                        .map(|c| Style::default().bg(c))
                        .unwrap_or_default()
                } else {
                    Style::default()
                };
                Row::new(row).style(row_style)
            })
            .collect();

        let header_row_style = if self.header_bg == Color::Reset {
            Style::default().fg(self.header_fg)
        } else {
            Style::default().bg(self.header_bg).fg(self.header_fg)
        };
        let headers: Vec<Span> = df
            .get_column_names()
            .iter()
            .take(visible_columns)
            .map(|name| Span::styled(name.to_string(), Style::default()))
            .collect();

        StatefulWidget::render(
            Table::new(rows, widths)
                .column_spacing(self.table_cell_padding)
                .header(Row::new(headers).style(header_row_style))
                .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
            area,
            buf,
            state,
        );
    }

    fn render_row_numbers(&self, area: Rect, buf: &mut Buffer, params: RowNumbersParams) {
        // Header row: same style as the rest of the column headers (fill full width so color matches)
        let header_style = if self.header_bg == Color::Reset {
            Style::default().fg(self.header_fg)
        } else {
            Style::default().bg(self.header_bg).fg(self.header_fg)
        };
        let header_fill = " ".repeat(area.width as usize);
        Paragraph::new(header_fill).style(header_style).render(
            Rect {
                x: area.x,
                y: area.y,
                width: area.width,
                height: 1,
            },
            buf,
        );

        // Only render up to the actual number of rows in the data
        let rows_to_render = params
            .visible_rows
            .min(params.num_rows.saturating_sub(params.start_row));

        if rows_to_render == 0 {
            return;
        }

        // Calculate width needed for largest row number
        let max_row_num =
            params.start_row + rows_to_render.saturating_sub(1) + params.row_start_index;
        let max_width = max_row_num.to_string().len();

        // Render row numbers
        for row_idx in 0..rows_to_render.min(area.height.saturating_sub(1) as usize) {
            let row_num = params.start_row + row_idx + params.row_start_index;
            let row_num_text = row_num.to_string();

            // Right-align row numbers within the available width
            let padding = max_width.saturating_sub(row_num_text.len());
            let padded_text = format!("{}{}", " ".repeat(padding), row_num_text);

            // Match main table background: default when row is even (or no alternate);
            // when alternate_row_bg is set, odd rows use that background.
            // When selected: same background as row (no inversion), foreground = terminal default.
            let is_selected = params.selected_row == Some(row_idx);
            let (fg, bg) = if is_selected {
                (
                    Color::Reset,
                    self.alternate_row_bg.filter(|_| row_idx % 2 == 1),
                )
            } else {
                (
                    self.row_numbers_fg,
                    self.alternate_row_bg.filter(|_| row_idx % 2 == 1),
                )
            };
            let row_num_style = match bg {
                Some(bg_color) => Style::default().fg(fg).bg(bg_color),
                None => Style::default().fg(fg),
            };

            let y = area.y + row_idx as u16 + 1; // +1 for header row
            if y < area.y + area.height {
                Paragraph::new(padded_text).style(row_num_style).render(
                    Rect {
                        x: area.x,
                        y,
                        width: area.width,
                        height: 1,
                    },
                    buf,
                );
            }
        }
    }
}

impl StatefulWidget for DataTable {
    type State = DataTableState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        state.visible_termcols = area.width as usize;
        let new_visible_rows = if area.height > 0 {
            (area.height - 1) as usize
        } else {
            0
        };
        let needs_collect = new_visible_rows != state.visible_rows;
        state.visible_rows = new_visible_rows;

        if let Some(selected) = state.table_state.selected() {
            if selected >= state.visible_rows {
                state.table_state.select(Some(state.visible_rows - 1))
            }
        }

        if needs_collect {
            state.collect();
        }

        // Only show errors in main view if not suppressed (e.g., when query input is active)
        // Query errors should only be shown in the query input frame
        if let Some(error) = state.error.as_ref() {
            if !state.suppress_error_display {
                Paragraph::new(format!("Error: {}", user_message_from_polars(error)))
                    .centered()
                    .block(
                        Block::default()
                            .borders(Borders::NONE)
                            .padding(Padding::top(area.height / 2)),
                    )
                    .wrap(ratatui::widgets::Wrap { trim: true })
                    .render(area, buf);
                return;
            }
            // If suppress_error_display is true, continue rendering the table normally
        }

        // Calculate row number column width if enabled
        let row_num_width = if state.row_numbers {
            let max_row_num = state.start_row + state.visible_rows.saturating_sub(1) + 1; // +1 for 1-based, +1 for potential
            max_row_num.to_string().len().max(1) as u16 + 1 // +1 for spacing
        } else {
            0
        };

        // Calculate locked columns width if any
        let mut locked_width = row_num_width;
        if let Some(locked_df) = state.locked_df.as_ref() {
            let (_, cols) = locked_df.shape();
            for col_index in 0..cols {
                let col_name = locked_df.get_column_names()[col_index];
                let mut max_len = col_name.chars().count() as u16;
                let col_data = &locked_df[col_index];
                for row_index in 0..locked_df.height().min(state.visible_rows) {
                    let value = col_data.get(row_index).unwrap();
                    let val_str: Cow<str> = if matches!(value, AnyValue::Null) {
                        Cow::Borrowed("")
                    } else {
                        value.str_value()
                    };
                    let len = val_str.chars().count() as u16;
                    max_len = max_len.max(len);
                }
                locked_width += max_len + 1;
            }
        }

        // Split area into locked and scrollable parts
        if locked_width > row_num_width && locked_width < area.width {
            let locked_area = Rect {
                x: area.x,
                y: area.y,
                width: locked_width,
                height: area.height,
            };
            let separator_x = locked_area.x + locked_area.width;

            // If row numbers are enabled, render them first in a separate area
            if state.row_numbers {
                let row_num_area = Rect {
                    x: area.x,
                    y: area.y,
                    width: row_num_width,
                    height: area.height,
                };
                self.render_row_numbers(
                    row_num_area,
                    buf,
                    RowNumbersParams {
                        start_row: state.start_row,
                        visible_rows: state.visible_rows,
                        num_rows: state.num_rows,
                        row_start_index: state.row_start_index,
                        selected_row: state.table_state.selected(),
                    },
                );
            }
            let scrollable_area = Rect {
                x: separator_x + 1,
                y: area.y,
                width: area.width.saturating_sub(locked_width + 1),
                height: area.height,
            };

            // Render locked columns (no background shading, just the vertical separator)
            if let Some(locked_df) = state.locked_df.as_ref() {
                // Adjust locked_area to account for row numbers if present
                let adjusted_locked_area = if state.row_numbers {
                    Rect {
                        x: area.x + row_num_width,
                        y: area.y,
                        width: locked_width - row_num_width,
                        height: area.height,
                    }
                } else {
                    locked_area
                };

                // Slice buffer to visible portion
                let offset = state.start_row.saturating_sub(state.buffered_start_row);
                let slice_len = state
                    .visible_rows
                    .min(locked_df.height().saturating_sub(offset));
                if offset < locked_df.height() && slice_len > 0 {
                    let sliced_df = locked_df.slice(offset as i64, slice_len);
                    self.render_dataframe(
                        &sliced_df,
                        adjusted_locked_area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                }
            }

            // Draw vertical separator line
            let separator_x_adjusted = if state.row_numbers {
                area.x + row_num_width + (locked_width - row_num_width)
            } else {
                separator_x
            };
            for y in area.y..area.y + area.height {
                let cell = &mut buf[(separator_x_adjusted, y)];
                cell.set_char('│');
                cell.set_style(Style::default().fg(self.separator_fg));
            }

            // Adjust scrollable area to account for row numbers
            let adjusted_scrollable_area = if state.row_numbers {
                Rect {
                    x: separator_x_adjusted + 1,
                    y: area.y,
                    width: area.width.saturating_sub(locked_width + 1),
                    height: area.height,
                }
            } else {
                scrollable_area
            };

            // Render scrollable columns
            if let Some(df) = state.df.as_ref() {
                // Slice buffer to visible portion
                let offset = state.start_row.saturating_sub(state.buffered_start_row);
                let slice_len = state.visible_rows.min(df.height().saturating_sub(offset));
                if offset < df.height() && slice_len > 0 {
                    let sliced_df = df.slice(offset as i64, slice_len);
                    self.render_dataframe(
                        &sliced_df,
                        adjusted_scrollable_area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                }
            }
        } else if let Some(df) = state.df.as_ref() {
            // No locked columns, render normally
            // If row numbers are enabled, render them first
            if state.row_numbers {
                let row_num_area = Rect {
                    x: area.x,
                    y: area.y,
                    width: row_num_width,
                    height: area.height,
                };
                self.render_row_numbers(
                    row_num_area,
                    buf,
                    RowNumbersParams {
                        start_row: state.start_row,
                        visible_rows: state.visible_rows,
                        num_rows: state.num_rows,
                        row_start_index: state.row_start_index,
                        selected_row: state.table_state.selected(),
                    },
                );

                // Adjust data area to exclude row number column
                let data_area = Rect {
                    x: area.x + row_num_width,
                    y: area.y,
                    width: area.width.saturating_sub(row_num_width),
                    height: area.height,
                };

                // Slice buffer to visible portion
                let offset = state.start_row.saturating_sub(state.buffered_start_row);
                let slice_len = state.visible_rows.min(df.height().saturating_sub(offset));
                if offset < df.height() && slice_len > 0 {
                    let sliced_df = df.slice(offset as i64, slice_len);
                    self.render_dataframe(
                        &sliced_df,
                        data_area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                }
            } else {
                // Slice buffer to visible portion
                let offset = state.start_row.saturating_sub(state.buffered_start_row);
                let slice_len = state.visible_rows.min(df.height().saturating_sub(offset));
                if offset < df.height() && slice_len > 0 {
                    let sliced_df = df.slice(offset as i64, slice_len);
                    self.render_dataframe(
                        &sliced_df,
                        area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                }
            }
        } else if !state.column_order.is_empty() {
            // Empty result (0 rows) but we have a schema - show empty table with header, no rows
            let empty_columns: Vec<_> = state
                .column_order
                .iter()
                .map(|name| Series::new(name.as_str().into(), Vec::<String>::new()).into())
                .collect();
            if let Ok(empty_df) = DataFrame::new(empty_columns) {
                if state.row_numbers {
                    let row_num_area = Rect {
                        x: area.x,
                        y: area.y,
                        width: row_num_width,
                        height: area.height,
                    };
                    self.render_row_numbers(
                        row_num_area,
                        buf,
                        RowNumbersParams {
                            start_row: 0,
                            visible_rows: state.visible_rows,
                            num_rows: 0,
                            row_start_index: state.row_start_index,
                            selected_row: None,
                        },
                    );
                    let data_area = Rect {
                        x: area.x + row_num_width,
                        y: area.y,
                        width: area.width.saturating_sub(row_num_width),
                        height: area.height,
                    };
                    self.render_dataframe(
                        &empty_df,
                        data_area,
                        buf,
                        &mut state.table_state,
                        false,
                        0,
                    );
                } else {
                    self.render_dataframe(&empty_df, area, buf, &mut state.table_state, false, 0);
                }
            } else {
                Paragraph::new("No data").render(area, buf);
            }
        } else {
            // Truly empty: no schema, not loaded, or blank file
            Paragraph::new("No data").render(area, buf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};

    fn create_test_lf() -> LazyFrame {
        df! (
            "a" => &[1, 2, 3],
            "b" => &["x", "y", "z"]
        )
        .unwrap()
        .lazy()
    }

    fn create_large_test_lf() -> LazyFrame {
        df! (
            "a" => (0..100).collect::<Vec<i32>>(),
            "b" => (0..100).map(|i| format!("text_{}", i)).collect::<Vec<String>>(),
            "c" => (0..100).map(|i| i % 3).collect::<Vec<i32>>(),
            "d" => (0..100).map(|i| i % 5).collect::<Vec<i32>>()
        )
        .unwrap()
        .lazy()
    }

    #[test]
    fn test_from_csv() {
        // Ensure sample data is generated before running test
        crate::tests::ensure_sample_data();
        // Test uncompressed CSV loading
        let path = Path::new("tests/sample-data/3-sfd-header.csv");
        let state = DataTableState::from_csv(path, &Default::default()).unwrap(); // Uses default buffer params from options
        assert_eq!(state.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
    }

    #[test]
    fn test_from_csv_gzipped() {
        // Ensure sample data is generated before running test
        crate::tests::ensure_sample_data();
        // Test gzipped CSV loading
        let path = Path::new("tests/sample-data/mixed_types.csv.gz");
        let state = DataTableState::from_csv(path, &Default::default()).unwrap(); // Uses default buffer params from options
        assert_eq!(state.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
    }

    #[test]
    fn test_from_parquet() {
        // Ensure sample data is generated before running test
        crate::tests::ensure_sample_data();
        let path = Path::new("tests/sample-data/people.parquet");
        let state = DataTableState::from_parquet(path, None, None, None, None, false, 1).unwrap();
        assert!(!state.schema.is_empty());
    }

    #[test]
    fn test_from_ipc() {
        use polars::prelude::IpcWriter;
        use std::io::BufWriter;
        let mut df = df!(
            "x" => &[1_i32, 2, 3],
            "y" => &["a", "b", "c"]
        )
        .unwrap();
        let dir = std::env::temp_dir();
        let path = dir.join("datui_test_ipc.arrow");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        IpcWriter::new(&mut writer).finish(&mut df).unwrap();
        drop(writer);
        let state = DataTableState::from_ipc(&path, None, None, None, None, false, 1).unwrap();
        assert_eq!(state.schema.len(), 2);
        assert!(state.schema.contains("x"));
        assert!(state.schema.contains("y"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_from_avro() {
        use polars::io::avro::AvroWriter;
        use std::io::BufWriter;
        let mut df = df!(
            "id" => &[1_i32, 2, 3],
            "name" => &["alice", "bob", "carol"]
        )
        .unwrap();
        let dir = std::env::temp_dir();
        let path = dir.join("datui_test_avro.avro");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        AvroWriter::new(&mut writer).finish(&mut df).unwrap();
        drop(writer);
        let state = DataTableState::from_avro(&path, None, None, None, None, false, 1).unwrap();
        assert_eq!(state.schema.len(), 2);
        assert!(state.schema.contains("id"));
        assert!(state.schema.contains("name"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_from_orc() {
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use orc_rust::ArrowWriterBuilder;
        use std::io::BufWriter;
        use std::sync::Arc;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let id_array = Arc::new(Int64Array::from(vec![1_i64, 2, 3]));
        let name_array = Arc::new(StringArray::from(vec!["a", "b", "c"]));
        let batch = RecordBatch::try_new(schema.clone(), vec![id_array, name_array]).unwrap();

        let dir = std::env::temp_dir();
        let path = dir.join("datui_test_orc.orc");
        let file = std::fs::File::create(&path).unwrap();
        let writer = BufWriter::new(file);
        let mut orc_writer = ArrowWriterBuilder::new(writer, schema).try_build().unwrap();
        orc_writer.write(&batch).unwrap();
        orc_writer.close().unwrap();

        let state = DataTableState::from_orc(&path, None, None, None, None, false, 1).unwrap();
        assert_eq!(state.schema.len(), 2);
        assert!(state.schema.contains("id"));
        assert!(state.schema.contains("name"));
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_filter() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let filters = vec![FilterStatement {
            column: "a".to_string(),
            operator: FilterOperator::Gt,
            value: "2".to_string(),
            logical_op: LogicalOperator::And,
        }];
        state.filter(filters);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.shape().0, 1);
        assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
    }

    #[test]
    fn test_sort() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        state.sort(vec!["a".to_string()], false);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
    }

    #[test]
    fn test_query() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        state.query("select b where a = 2".to_string());
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.shape(), (1, 1));
        assert_eq!(
            df.column("b").unwrap().get(0).unwrap(),
            AnyValue::String("y")
        );
    }

    #[test]
    fn test_query_date_accessors() {
        use chrono::NaiveDate;
        let df = df!(
            "event_date" => [
                NaiveDate::from_ymd_opt(2024, 1, 15).unwrap(),
                NaiveDate::from_ymd_opt(2024, 6, 20).unwrap(),
                NaiveDate::from_ymd_opt(2024, 12, 31).unwrap(),
            ],
            "name" => &["a", "b", "c"],
        )
        .unwrap();
        let lf = df.lazy();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();

        // Select with date accessors
        state.query("select name, year: event_date.year, month: event_date.month".to_string());
        assert!(
            state.error.is_none(),
            "query should succeed: {:?}",
            state.error
        );
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.shape(), (3, 3));
        assert_eq!(
            df.column("year").unwrap().get(0).unwrap(),
            AnyValue::Int32(2024)
        );
        assert_eq!(
            df.column("month").unwrap().get(0).unwrap(),
            AnyValue::Int8(1)
        );
        assert_eq!(
            df.column("month").unwrap().get(1).unwrap(),
            AnyValue::Int8(6)
        );

        // Filter with date accessor
        state.query("select name, event_date where event_date.month = 12".to_string());
        assert!(
            state.error.is_none(),
            "filter should succeed: {:?}",
            state.error
        );
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(
            df.column("name").unwrap().get(0).unwrap(),
            AnyValue::String("c")
        );

        // Filter with YYYY.MM.DD date literal
        state.query("select name, event_date where event_date.date > 2024.06.15".to_string());
        assert!(
            state.error.is_none(),
            "date literal filter should succeed: {:?}",
            state.error
        );
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(
            df.height(),
            2,
            "2024-06-20 and 2024-12-31 are after 2024-06-15"
        );

        // String accessors: upper, lower, len, ends_with
        state.query(
            "select name, upper_name: name.upper, name_len: name.len where name.ends_with[\"c\"]"
                .to_string(),
        );
        assert!(
            state.error.is_none(),
            "string accessors should succeed: {:?}",
            state.error
        );
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.height(), 1, "only 'c' ends with 'c'");
        assert_eq!(
            df.column("upper_name").unwrap().get(0).unwrap(),
            AnyValue::String("C")
        );

        // Query that returns 0 rows: df and locked_df must be cleared for correct empty-table render
        state.query("select where event_date.date = 2020.01.01".to_string());
        assert!(state.error.is_none());
        assert_eq!(state.num_rows, 0);
        state.visible_rows = 10;
        state.collect();
        assert!(state.df.is_none(), "df must be cleared when num_rows is 0");
        assert!(
            state.locked_df.is_none(),
            "locked_df must be cleared when num_rows is 0"
        );
    }

    #[test]
    fn test_select_next_previous() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        state.visible_rows = 10;
        state.table_state.select(Some(5));

        state.select_next();
        assert_eq!(state.table_state.selected(), Some(6));

        state.select_previous();
        assert_eq!(state.table_state.selected(), Some(5));
    }

    #[test]
    fn test_page_up_down() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        state.visible_rows = 20;
        state.collect();

        assert_eq!(state.start_row, 0);
        state.page_down();
        assert_eq!(state.start_row, 20);
        state.page_down();
        assert_eq!(state.start_row, 40);
        state.page_up();
        assert_eq!(state.start_row, 20);
        state.page_up();
        assert_eq!(state.start_row, 0);
    }

    #[test]
    fn test_scroll_left_right() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        assert_eq!(state.termcol_index, 0);
        state.scroll_right();
        assert_eq!(state.termcol_index, 1);
        state.scroll_right();
        assert_eq!(state.termcol_index, 2);
        state.scroll_left();
        assert_eq!(state.termcol_index, 1);
        state.scroll_left();
        assert_eq!(state.termcol_index, 0);
    }

    #[test]
    fn test_reverse() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        state.sort(vec!["a".to_string()], true);
        assert_eq!(
            state
                .lf
                .clone()
                .collect()
                .unwrap()
                .column("a")
                .unwrap()
                .get(0)
                .unwrap(),
            AnyValue::Int32(1)
        );
        state.reverse();
        assert_eq!(
            state
                .lf
                .clone()
                .collect()
                .unwrap()
                .column("a")
                .unwrap()
                .get(0)
                .unwrap(),
            AnyValue::Int32(3)
        );
    }

    #[test]
    fn test_filter_multiple() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let filters = vec![
            FilterStatement {
                column: "c".to_string(),
                operator: FilterOperator::Eq,
                value: "1".to_string(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                column: "d".to_string(),
                operator: FilterOperator::Eq,
                value: "2".to_string(),
                logical_op: LogicalOperator::And,
            },
        ];
        state.filter(filters);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.shape().0, 7);
    }

    #[test]
    fn test_filter_and_sort() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let filters = vec![FilterStatement {
            column: "c".to_string(),
            operator: FilterOperator::Eq,
            value: "1".to_string(),
            logical_op: LogicalOperator::And,
        }];
        state.filter(filters);
        state.sort(vec!["a".to_string()], false);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(97));
    }

    /// Minimal long-format data for pivot tests: id, date, key, value.
    /// Includes duplicates for aggregation (e.g. (1,d1,A) appears twice).
    fn create_pivot_long_lf() -> LazyFrame {
        let df = df!(
            "id" => &[1_i32, 1, 1, 2, 2, 2, 1, 2],
            "date" => &["d1", "d1", "d1", "d1", "d1", "d1", "d1", "d1"],
            "key" => &["A", "B", "C", "A", "B", "C", "A", "B"],
            "value" => &[10.0_f64, 20.0, 30.0, 40.0, 50.0, 60.0, 11.0, 51.0],
        )
        .unwrap();
        df.lazy()
    }

    /// Wide-format data for melt tests: id, date, c1, c2, c3.
    fn create_melt_wide_lf() -> LazyFrame {
        let df = df!(
            "id" => &[1_i32, 2, 3],
            "date" => &["d1", "d2", "d3"],
            "c1" => &[10.0_f64, 20.0, 30.0],
            "c2" => &[11.0, 21.0, 31.0],
            "c3" => &[12.0, 22.0, 32.0],
        )
        .unwrap();
        df.lazy()
    }

    #[test]
    fn test_pivot_basic() {
        let lf = create_pivot_long_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: false,
        };
        state.pivot(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert!(names.contains(&"id"));
        assert!(names.contains(&"date"));
        assert!(names.contains(&"A"));
        assert!(names.contains(&"B"));
        assert!(names.contains(&"C"));
        assert_eq!(df.height(), 2);
    }

    #[test]
    fn test_pivot_aggregation_last() {
        let lf = create_pivot_long_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: false,
        };
        state.pivot(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        let a_col = df.column("A").unwrap();
        let row0 = a_col.get(0).unwrap();
        let row1 = a_col.get(1).unwrap();
        assert_eq!(row0, AnyValue::Float64(11.0));
        assert_eq!(row1, AnyValue::Float64(40.0));
    }

    #[test]
    fn test_pivot_aggregation_first() {
        let lf = create_pivot_long_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: false,
        };
        state.pivot(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        let a_col = df.column("A").unwrap();
        assert_eq!(a_col.get(0).unwrap(), AnyValue::Float64(10.0));
        assert_eq!(a_col.get(1).unwrap(), AnyValue::Float64(40.0));
    }

    #[test]
    fn test_pivot_aggregation_min_max() {
        let lf = create_pivot_long_lf();
        let mut state_min = DataTableState::new(lf.clone(), None, None, None, None).unwrap();
        state_min
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Min,
                sort_columns: false,
            })
            .unwrap();
        let df_min = state_min.lf.clone().collect().unwrap();
        assert_eq!(
            df_min.column("A").unwrap().get(0).unwrap(),
            AnyValue::Float64(10.0)
        );

        let mut state_max = DataTableState::new(lf, None, None, None, None).unwrap();
        state_max
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Max,
                sort_columns: false,
            })
            .unwrap();
        let df_max = state_max.lf.clone().collect().unwrap();
        assert_eq!(
            df_max.column("A").unwrap().get(0).unwrap(),
            AnyValue::Float64(11.0)
        );
    }

    #[test]
    fn test_pivot_aggregation_avg_count() {
        let lf = create_pivot_long_lf();
        let mut state_avg = DataTableState::new(lf.clone(), None, None, None, None).unwrap();
        state_avg
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Avg,
                sort_columns: false,
            })
            .unwrap();
        let df_avg = state_avg.lf.clone().collect().unwrap();
        let a = df_avg.column("A").unwrap().get(0).unwrap();
        if let AnyValue::Float64(x) = a {
            assert!((x - 10.5).abs() < 1e-6);
        } else {
            panic!("expected float");
        }

        let mut state_count = DataTableState::new(lf, None, None, None, None).unwrap();
        state_count
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Count,
                sort_columns: false,
            })
            .unwrap();
        let df_count = state_count.lf.clone().collect().unwrap();
        let a = df_count.column("A").unwrap().get(0).unwrap();
        assert_eq!(a, AnyValue::UInt32(2));
    }

    #[test]
    fn test_pivot_string_first_last() {
        let df = df!(
            "id" => &[1_i32, 1, 2, 2],
            "key" => &["X", "Y", "X", "Y"],
            "value" => &["low", "mid", "high", "mid"],
        )
        .unwrap();
        let lf = df.lazy();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: false,
        };
        state.pivot(&spec).unwrap();
        let out = state.lf.clone().collect().unwrap();
        assert_eq!(
            out.column("X").unwrap().get(0).unwrap(),
            AnyValue::String("low")
        );
        assert_eq!(
            out.column("Y").unwrap().get(0).unwrap(),
            AnyValue::String("mid")
        );
    }

    #[test]
    fn test_melt_basic() {
        let lf = create_melt_wide_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let spec = MeltSpec {
            index: vec!["id".to_string(), "date".to_string()],
            value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        };
        state.melt(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.height(), 9);
        let names: Vec<&str> = df.get_column_names().iter().map(|s| s.as_str()).collect();
        assert!(names.contains(&"variable"));
        assert!(names.contains(&"value"));
        assert!(names.contains(&"id"));
        assert!(names.contains(&"date"));
    }

    #[test]
    fn test_melt_all_except_index() {
        let lf = create_melt_wide_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        let spec = MeltSpec {
            index: vec!["id".to_string(), "date".to_string()],
            value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
            variable_name: "var".to_string(),
            value_name: "val".to_string(),
        };
        state.melt(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        assert!(df.column("var").is_ok());
        assert!(df.column("val").is_ok());
    }

    #[test]
    fn test_pivot_on_current_view_after_filter() {
        let lf = create_pivot_long_lf();
        let mut state = DataTableState::new(lf, None, None, None, None).unwrap();
        state.filter(vec![FilterStatement {
            column: "id".to_string(),
            operator: FilterOperator::Eq,
            value: "1".to_string(),
            logical_op: LogicalOperator::And,
        }]);
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: false,
        };
        state.pivot(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.height(), 1);
        let id_col = df.column("id").unwrap();
        assert_eq!(id_col.get(0).unwrap(), AnyValue::Int32(1));
    }
}
