use color_eyre::Result;
use std::sync::Arc;
use std::{fs::File, path::Path};

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

use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};
use crate::query::parse_query;
use crate::{CompressionFormat, OpenOptions};
use polars::lazy::frame::pivot::{pivot, pivot_stable};
use std::io::{BufReader, Read};

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
    // Buffer tracking for proximity-based loading
    pages_lookahead: usize,     // Maximum pages to buffer ahead
    pages_lookback: usize,      // Maximum pages to buffer behind
    buffered_start_row: usize,  // Start row of currently buffered data
    buffered_end_row: usize,    // End row (exclusive) of currently buffered data
    proximity_threshold: usize, // Rows from buffer edge before triggering expansion
    row_numbers: bool,          // Whether to display row numbers
    row_start_index: usize,     // Starting index for row numbers (0 or 1)
    /// Last applied pivot spec, if current lf is result of a pivot. Used for templates.
    last_pivot_spec: Option<PivotSpec>,
    /// Last applied melt spec, if current lf is result of a melt. Used for templates.
    last_melt_spec: Option<MeltSpec>,
}

impl DataTableState {
    pub fn new(
        lf: LazyFrame,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
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
            buffered_start_row: 0,
            buffered_end_row: 0,
            proximity_threshold: 0, // Will be set when visible_rows is known
            row_numbers: false,     // Will be set from options
            row_start_index: 1,     // Will be set from options
            last_pivot_spec: None,
            last_melt_spec: None,
        })
    }

    pub fn reset(&mut self) {
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
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyFrame::scan_parquet(pl_path, Default::default())?;
        let mut state = Self::new(lf, pages_lookahead, pages_lookback)?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    pub fn set_row_numbers(&mut self, enabled: bool) {
        self.row_numbers = enabled;
    }

    pub fn toggle_row_numbers(&mut self) {
        self.row_numbers = !self.row_numbers;
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        // Determine compression format: explicit option, or auto-detect from extension
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path));

        if let Some(compression) = compression {
            // For compressed files, we need to use eager reading
            // Polars natively supports gzip and zstd via the decompress feature
            // For bzip2 and xz, we need to decompress manually
            match compression {
                CompressionFormat::Gzip | CompressionFormat::Zstd => {
                    // Polars natively handles gzip and zstd
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

                    let df = read_options
                        .try_into_reader_with_file_path(Some(path.into()))?
                        .finish()?;
                    let lf = df.lazy();
                    let mut state = Self::new(lf, options.pages_lookahead, options.pages_lookback)?;
                    state.row_numbers = options.row_numbers;
                    state.row_start_index = options.row_start_index;
                    Ok(state)
                }
                CompressionFormat::Bzip2 => {
                    // Decompress bzip2 manually, then read CSV
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

                    let df = CsvReader::new(std::io::Cursor::new(decompressed))
                        .with_options(read_options)
                        .finish()?;
                    let lf = df.lazy();
                    let mut state = Self::new(lf, options.pages_lookahead, options.pages_lookback)?;
                    state.row_numbers = options.row_numbers;
                    state.row_start_index = options.row_start_index;
                    Ok(state)
                }
                CompressionFormat::Xz => {
                    // Decompress xz manually, then read CSV
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

                    let df = CsvReader::new(std::io::Cursor::new(decompressed))
                        .with_options(read_options)
                        .finish()?;
                    let lf = df.lazy();
                    Self::new(lf, options.pages_lookahead, options.pages_lookback)
                }
            }
        } else {
            // For uncompressed files, use lazy scanning (more efficient)
            let mut state = Self::from_csv_customize(
                path,
                options.pages_lookahead,
                options.pages_lookback,
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
        func: F,
    ) -> Result<Self>
    where
        F: FnOnce(LazyCsvReader) -> LazyCsvReader,
    {
        let pl_path = PlPath::Local(Arc::from(path));
        let reader = LazyCsvReader::new(pl_path);
        let lf = func(reader).finish()?;
        Self::new(lf, pages_lookahead, pages_lookback)
    }

    pub fn from_ndjson(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyJsonLineReader::new(pl_path).finish()?;
        let mut state = Self::new(lf, pages_lookahead, pages_lookback)?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    pub fn from_json(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        Self::from_json_with_format(
            path,
            pages_lookahead,
            pages_lookback,
            row_numbers,
            row_start_index,
            JsonFormat::Json,
        )
    }

    pub fn from_json_lines(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        Self::from_json_with_format(
            path,
            pages_lookahead,
            pages_lookback,
            row_numbers,
            row_start_index,
            JsonFormat::JsonLines,
        )
    }

    fn from_json_with_format(
        path: &Path,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
        format: JsonFormat,
    ) -> Result<Self> {
        let file = File::open(path)?;
        let lf = JsonReader::new(file)
            .with_json_format(format)
            .finish()?
            .lazy();
        let mut state = Self::new(lf, pages_lookahead, pages_lookback)?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    pub fn from_delimited(
        path: &Path,
        delimiter: u8,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        row_numbers: bool,
        row_start_index: usize,
    ) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let reader = LazyCsvReader::new(pl_path).with_separator(delimiter);
        let lf = reader.finish()?;
        let mut state = Self::new(lf, pages_lookahead, pages_lookback)?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
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

        // Check if new position is within buffer and not approaching edges
        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        if within_buffer {
            // Check proximity to buffer edges
            let dist_to_start = new_start_row.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);

            // Only skip collect if well within buffer (not approaching edges)
            if dist_to_start > self.proximity_threshold && dist_to_end > self.proximity_threshold {
                // Fast path: just update start_row, no collect needed
                self.start_row = new_start_row;
                return;
            }
        }

        // Need to collect (either outside buffer or approaching edge)
        self.start_row = new_start_row;
        self.collect();

        if self.num_rows > 0 {
            let max_start = self.num_rows.saturating_sub(1);
            if self.start_row > max_start {
                self.start_row = max_start;
                self.collect();
            }
        } else {
            self.start_row = 0;
        }
    }

    pub fn collect(&mut self) {
        // Update proximity threshold based on visible rows
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

        if self.num_rows > 0 {
            let max_start = self.num_rows.saturating_sub(1);
            if self.start_row > max_start {
                self.start_row = max_start;
            }
        } else {
            self.start_row = 0;
            self.buffered_start_row = 0;
            self.buffered_end_row = 0;
            return;
        }

        // Proximity-based buffer logic
        let view_start = self.start_row;
        let view_end = self.start_row + self.visible_rows.min(self.num_rows - self.start_row);

        // Check if current view is within buffered range
        let within_buffer = view_start >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        if within_buffer {
            // Check proximity to buffer edges
            let dist_to_start = view_start.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);

            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.buffered_end_row < self.num_rows;

            // If not approaching edges, use existing buffer (fast path)
            if !needs_expansion_back && !needs_expansion_forward {
                // Fast path: just slice from existing buffer
                self.slice_from_buffer();
                return;
            }

            // Need to expand buffer
            let new_buffer_start = if needs_expansion_back {
                view_start.saturating_sub(self.pages_lookback * self.visible_rows.max(1))
            } else {
                self.buffered_start_row
            };

            let new_buffer_end = if needs_expansion_forward {
                (view_end + self.pages_lookahead * self.visible_rows.max(1)).min(self.num_rows)
            } else {
                self.buffered_end_row
            };

            self.load_buffer(new_buffer_start, new_buffer_end);
        } else {
            // View is outside buffer or buffer doesn't exist - calculate new buffer
            let new_buffer_start =
                view_start.saturating_sub(self.pages_lookback * self.visible_rows.max(1));
            let new_buffer_end =
                (view_end + self.pages_lookahead * self.visible_rows.max(1)).min(self.num_rows);

            self.load_buffer(new_buffer_start, new_buffer_end);
        }

        // Slice displayed portion from buffered DataFrame
        self.slice_from_buffer();
    }

    fn load_buffer(&mut self, buffer_start: usize, buffer_end: usize) {
        let buffer_size = buffer_end.saturating_sub(buffer_start);
        if buffer_size == 0 {
            return;
        }

        self.buffered_start_row = buffer_start;
        self.buffered_end_row = buffer_end;

        // Load locked columns buffer
        if self.locked_columns_count > 0 {
            let locked_columns: Vec<_> = self
                .column_order
                .iter()
                .take(self.locked_columns_count)
                .map(|name| col(name.as_str()))
                .collect();

            match self
                .lf
                .clone()
                .select(locked_columns)
                .slice(buffer_start as i64, buffer_size as u32)
                .collect()
            {
                Ok(df) => {
                    if self.is_grouped() {
                        match self.format_grouped_dataframe(df) {
                            Ok(formatted_df) => {
                                self.locked_df = Some(formatted_df);
                            }
                            Err(e) => {
                                self.error = Some(PolarsError::ComputeError(e.to_string().into()));
                                return;
                            }
                        }
                    } else {
                        self.locked_df = Some(df);
                    }
                }
                Err(e) => {
                    self.error = Some(e);
                    return;
                }
            }
        } else {
            self.locked_df = None;
        }

        // Load scrollable columns buffer
        let columns_to_select: Vec<_> = self
            .column_order
            .iter()
            .skip(self.locked_columns_count + self.termcol_index)
            .map(|name| col(name.as_str()))
            .collect();

        match self
            .lf
            .clone()
            .select(columns_to_select)
            .slice(buffer_start as i64, buffer_size as u32)
            .collect()
        {
            Ok(df) => {
                if self.is_grouped() {
                    match self.format_grouped_dataframe(df) {
                        Ok(formatted_df) => {
                            self.df = Some(formatted_df);
                            if self.error.is_none() {
                                self.error = None;
                            }
                        }
                        Err(e) => {
                            self.error = Some(PolarsError::ComputeError(e.to_string().into()));
                        }
                    }
                } else {
                    self.df = Some(df);
                    if self.error.is_none() {
                        self.error = None;
                    }
                }
            }
            Err(e) => {
                self.error = Some(e);
            }
        }
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

    /// Estimated heap size in bytes of the currently buffered (visible) slice, if collected.
    pub fn buffered_memory_bytes(&self) -> Option<usize> {
        self.df.as_ref().map(|df| df.estimated_size())
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
    pub fn pivot(&mut self, spec: &PivotSpec) -> Result<()> {
        let df = self.lf.clone().collect()?;
        let agg_expr = pivot_agg_expr(spec.aggregation)?;
        let index_str: Vec<&str> = spec.index.iter().map(|s| s.as_str()).collect();
        let index_opt = if index_str.is_empty() {
            None
        } else {
            Some(index_str)
        };
        let pivoted = if matches!(
            spec.aggregation,
            PivotAggregation::First | PivotAggregation::Last
        ) {
            pivot_stable(
                &df,
                [spec.pivot_column.as_str()],
                index_opt,
                Some([spec.value_column.as_str()]),
                spec.sort_columns,
                Some(agg_expr),
                None,
            )?
        } else {
            pivot(
                &df,
                [spec.pivot_column.as_str()],
                index_opt,
                Some([spec.value_column.as_str()]),
                spec.sort_columns,
                Some(agg_expr),
                None,
            )?
        };
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
            self.lf = self.lf.clone().sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                options,
            );
            self.collect();
        } else {
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
                // Only set error, don't modify any state
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
}

impl Default for DataTable {
    fn default() -> Self {
        Self {
            header_bg: Color::Indexed(236),
            header_fg: Color::White,
            row_numbers_fg: Color::DarkGray,
            separator_fg: Color::White,
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
                let val_str = value.str_value();
                let len = val_str.chars().count() as u16;
                max_len = max_len.max(len);
                row.push(Cell::from(Line::from(val_str)));
            }

            let overflows = (used_width + max_len) >= area.width;

            if overflows && col_data.dtype() == &DataType::String {
                let visible_width = area.width - used_width;
                visible_columns += 1;
                widths[col_index] = visible_width;
                break;
            } else if !overflows {
                visible_columns += 1;
                widths[col_index] = max_len;
                used_width += max_len + 1;
            } else {
                break;
            }
        }

        widths.truncate(visible_columns);
        // convert rows to a vector of Row
        let rows = rows
            .into_iter()
            .map(|mut row| {
                row.truncate(visible_columns);
                Row::new(row)
            })
            .collect::<Vec<Row>>();

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
                .column_spacing(1)
                .header(Row::new(headers).style(header_row_style))
                .row_highlight_style(Style::default().add_modifier(Modifier::REVERSED)),
            area,
            buf,
            state,
        );
    }

    fn render_row_numbers(&self, area: Rect, buf: &mut Buffer, params: RowNumbersParams) {
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

            // Highlight row number if this row is selected
            let is_selected = params.selected_row == Some(row_idx);
            let row_num_style = if is_selected {
                // Must match the table's row_highlight_style exactly. Using a different
                // style (e.g. fg + REVERSED) creates a style boundary at the row-number
                // | first-data-column edge; some terminals (VS Code, xterm-256) then
                // render that boundary with artifacts (bleeding, cut-off glyphs).
                Style::default().add_modifier(Modifier::REVERSED)
            } else {
                Style::default().fg(self.row_numbers_fg)
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
                Paragraph::new(format!("Error: {}", error))
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
                    let val_str = value.str_value();
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
        } else {
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
        let state = DataTableState::from_parquet(path, None, None, false, 1).unwrap();
        assert!(!state.schema.is_empty());
    }

    #[test]
    fn test_filter() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
        state.sort(vec!["a".to_string()], false);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
    }

    #[test]
    fn test_query() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None).unwrap();
        state.query("select b where a = 2".to_string());
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.shape(), (1, 1));
        assert_eq!(
            df.column("b").unwrap().get(0).unwrap(),
            AnyValue::String("y")
        );
    }

    #[test]
    fn test_select_next_previous() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state_min = DataTableState::new(lf.clone(), None, None).unwrap();
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

        let mut state_max = DataTableState::new(lf, None, None).unwrap();
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
        let mut state_avg = DataTableState::new(lf.clone(), None, None).unwrap();
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

        let mut state_count = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
        let mut state = DataTableState::new(lf, None, None).unwrap();
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
