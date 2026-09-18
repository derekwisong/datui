use color_eyre::Result;
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;
use std::{fs, fs::File, path::Path, path::PathBuf};

use polars::frame::PivotColumnNaming;
use polars::io::HiveOptions;
use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span, Text},
    widgets::{
        Block, Borders, Cell, Padding, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::error_display::user_message_from_polars;
use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::numfmt::{self, CellFormatter, NumberFormatSettings};
use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};
use crate::query::parse_query;
use crate::schema_union::FileSchema;
use crate::statistics::collect_lazy;
use crate::{CompressionFormat, OpenOptions, ParseStringsTarget};
use polars::io::csv::read::NullValues;
use polars::prelude::StrptimeOptions;
use std::io::{BufReader, Read};

use calamine::{Data, Reader, open_workbook_auto};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use orc_rust::ArrowReaderBuilder;
use tempfile::NamedTempFile;

use arrow::array::types::{
    Date32Type, Date64Type, Float32Type, Float64Type, Int8Type, Int16Type, Int32Type, Int64Type,
    TimestampMillisecondType, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
};
use arrow::array::{Array, AsArray};
use arrow::record_batch::RecordBatch;

fn pivot_agg_expr(agg: PivotAggregation) -> Result<Expr> {
    // The lazy pivot only allows the value column to be referenced as `element()`.
    let e = element();
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
    /// What the sidebar filters and sort are applied to: the active query's result (DSL,
    /// SQL or fuzzy), the last pivot/melt, or `original_lf` when there is none. The
    /// pipeline is original → query/reshape (`base_lf`) → filters → sort (`lf`) → column
    /// order (at collect). Filters therefore never discard the query.
    base_lf: LazyFrame,
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
    /// Bumped whenever `lf` changes (via `invalidate_num_rows`). A background `len()`
    /// count carries the generation it was spawned under; a result whose generation no
    /// longer matches is stale (the data changed) and is dropped. Decoupled from
    /// `task_generation` so a mere scroll doesn't invalidate / restart an in-flight count.
    ///
    /// Seeded from a process-wide counter rather than zero, so the value is unique
    /// across datasets as well as across mutations of one. Starting every state at
    /// zero meant a count still running for the dataset you just closed matched the
    /// one you just opened, and set its row count to the wrong number.
    len_generation: u64,
    /// The local Parquet hive directory the data was loaded from, whose per-file footer
    /// counts sum to the exact row count while the frame is the scan as loaded
    /// (`is_pristine`) — far cheaper than a `len()` data scan over a huge/partitioned set.
    parquet_count_dir: Option<PathBuf>,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_ascending: bool,
    /// Last executed DSL query. At most one of the three `active_*` queries is set: running
    /// one clears the other two.
    pub active_query: String,
    /// Last executed SQL (Sql tab).
    pub active_sql_query: String,
    /// Last executed fuzzy search (Fuzzy tab).
    pub active_fuzzy_query: String,
    column_order: Vec<String>,   // Order of columns for display
    locked_columns_count: usize, // Number of locked columns (from left)
    /// The grouped view a drill-down left, restored exactly by `drill_up`.
    grouped: Option<GroupedView>,
    /// The last pivot/melt result, while one is in effect. SQL runs against it rather
    /// than the data as loaded (see `query_root`).
    reshaped_lf: Option<LazyFrame>,
    drilled_down_group_index: Option<usize>, // Index of the group we're viewing
    pub drilled_down_group_key: Option<Vec<String>>, // Key values of the drilled down group
    pub drilled_down_group_key_columns: Option<Vec<String>>, // Key column names of the drilled down group
    pages_lookahead: usize,
    pages_lookback: usize,
    max_buffered_rows: usize, // 0 = no limit
    max_buffered_mb: usize,   // 0 = no limit
    /// True for a scan of an object store, where a buffer fill is a ranged read of
    /// whole row groups. See `set_remote_source`.
    remote_source: bool,
    /// Where each row group of a remote Parquet object starts, with the total as the
    /// last entry, from its footer. See `set_row_groups`.
    row_group_offsets: Option<Vec<usize>>,
    /// The files of a remote dataset, when it is many. See `RemoteFiles`.
    remote_files: Option<RemoteFiles>,
    /// What the footers said about a many-file dataset's columns: where the schema came
    /// from, and which columns are not in every file. `None` for a single file.
    dataset_schema: Option<crate::schema_union::DatasetSchema>,
    /// Whether the frame still carries the scan's hidden drift column. True from the
    /// open of a dataset whose files differ; false once a query or reshape has built a
    /// new frame, which has no file behind each row any more.
    drift_column_present: bool,
    /// What each drift group is missing, shared with the renderer so a frame costs no
    /// allocation. Indexed by the drift column's values.
    drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    /// The two above as the dataset was opened, so a reset returns to them.
    drift_at_open: bool,
    groups_at_open: Arc<Vec<crate::schema_union::DriftGroup>>,
    /// Where each file's rows begin in the dataset, and the drift group of each file.
    /// Together they turn a row's place in the dataset into what its file was missing.
    drift_file_starts: Vec<usize>,
    drift_file_group: Vec<u32>,
    /// Each file's path or URL, in scan order, so a row can be traced to the file it
    /// came from and an export can name it.
    drift_files: Vec<String>,
    /// What datui noticed about the dataset, from the footers it had to read anyway.
    notes: Vec<crate::notes::Note>,
    /// Whether the Info panel has been opened since the notes were gathered. Belongs to
    /// the dataset, so opening another one offers its notes afresh.
    notes_seen: bool,
    /// The notes as the dataset was opened, so a reset and a drill up restore them.
    notes_at_open: Vec<crate::notes::Note>,
    /// Uncompressed bytes per row of each column, from the Parquet footer, for
    /// `bytes_per_row` before anything has been collected.
    column_widths: Vec<(String, usize)>,
    /// Bytes per row of the last buffer collected, which outranks the estimate from
    /// the schema.
    observed_bytes_per_row: Option<usize>,
    buffered_start_row: usize,
    buffered_end_row: usize,
    /// Full buffered DataFrame (all columns in column_order) for the current buffer range.
    /// When set, column scroll (scroll_left/scroll_right) only re-slices columns without re-collecting from LazyFrame.
    buffered_df: Option<DataFrame>,
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
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    pub polars_streaming: bool,
    /// When true, cast Date/Datetime pivot index columns to Int32 before pivot (workaround for Polars 0.52).
    /// When true, `collect()` / `apply_transformations()` skip the blocking collect.
    /// The caller is responsible for triggering an async collect afterwards.
    pub defer_collect: bool,
    /// Set by the render code when `visible_rows` changes. The App event loop checks this
    /// after each render and triggers an async collect if needed.
    pub needs_recollect: bool,
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

/// The grouped view and the pipeline state that produced it, saved by a drill-down so
/// filters and sort inside the group work on the group and `drill_up` restores the
/// grouped view as it was.
struct GroupedView {
    lf: LazyFrame,
    base_lf: LazyFrame,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_ascending: bool,
    /// Whether `lf` carries the hidden drift column, and what its groups mean. Saved
    /// with the frame so drilling back up restores the cells it explains, along with
    /// the notes that explain them.
    drift: bool,
    drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    notes: Vec<crate::notes::Note>,
}

/// The query bar a result came from, with its text. At most one is active at a time.
enum ActiveQuery {
    Dsl(String),
    Sql(String),
    Fuzzy(String),
}

/// Parameters for a background buffer load. Produced by `prepare_async_collect()`.
pub struct CollectRequest {
    /// LazyFrame to collect (sliced to the buffer range, with column selection applied).
    pub lf: LazyFrame,
    /// Whether to use Polars streaming engine.
    pub polars_streaming: bool,
    /// Buffer start row in the full dataset.
    pub buffer_start: usize,
    /// Buffer end row in the full dataset.
    pub buffer_end: usize,
    /// Row count for the full (unsliced) dataset. Only meaningful when `count_known`.
    pub num_rows: usize,
    /// Whether `num_rows` is the true total. False for a first buffer rendered before
    /// the background `len()` count has resolved; in that case `num_rows` is provisional.
    pub count_known: bool,
}

/// Builds a scan of some of a dataset's files, as the full scan reads them.
pub type FileScan = Arc<dyn Fn(&[String]) -> PolarsResult<LazyFrame> + Send + Sync>;
/// Counts the rows in each row group of every file of a dataset. Blocks.
pub type FileCounter = Arc<dyn Fn() -> Result<Vec<Vec<usize>>, String> + Send + Sync>;

/// A remote dataset of many files, and how to read only some of them.
///
/// Polars reads a scan of many files in order: row 900,000 is reached by reading every
/// file before it, and a count is a read of all of them. Once each file's rows are
/// known, from its footer, a buffer is a scan of just the files holding its rows.
#[derive(Clone)]
pub struct RemoteFiles {
    /// Every file, in scan order.
    pub urls: Arc<Vec<String>>,
    pub scan: FileScan,
    pub count: FileCounter,
    /// Where each file's rows start, with the total last. Known once counted.
    pub offsets: Option<Vec<usize>>,
}

/// Result of a background buffer load. Consumed by `apply_async_collect()`.
pub struct CollectResult {
    pub df: DataFrame,
    pub buffer_start: usize,
    pub buffer_end: usize,
    pub num_rows: usize,
    /// See `CollectRequest::count_known`.
    pub count_known: bool,
}

/// Rows the display buffer may hold when `display.max_buffered_rows` is not set. Also
/// the window a remote scan buffers when the cap is switched off.
pub const DEFAULT_MAX_BUFFERED_ROWS: usize = 100_000;

/// Seeds `DataTableState::len_generation`. Unique per state, so a row count spawned
/// for one dataset can never be mistaken for a valid result for another.
static NEXT_LEN_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_len_generation() -> u64 {
    NEXT_LEN_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// Options for sorting by `n` columns. Nulls go last in both directions, as in pandas,
/// DuckDB and spreadsheets; Polars would otherwise put them first either way.
fn sort_options(n: usize, descending: bool) -> SortMultipleOptions {
    SortMultipleOptions::default()
        .with_order_descending_multi(vec![descending; n])
        .with_nulls_last_multi(vec![true; n])
}

/// A string's in-memory width when nothing says otherwise: the view plus a short value.
const STRING_BYTES_GUESS: usize = 40;

/// Bytes a row of `columns` takes in memory, estimated from the schema: the width of
/// each fixed-size type; for a string the footer's average in `column_widths` (or a
/// guess) plus its view; for a nested column the footer's average, else a guess.
/// Binary columns are buffered as a stub (see `binary_stub_exprs`).
fn estimate_bytes_per_row(
    schema: &Schema,
    columns: &[String],
    column_widths: &[(String, usize)],
) -> usize {
    let footer_width = |name: &String| {
        column_widths
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, w)| *w)
    };
    columns
        .iter()
        .map(|name| match schema.get(name.as_str()) {
            Some(DataType::String) => 16 + footer_width(name).unwrap_or(STRING_BYTES_GUESS - 16),
            Some(DataType::Binary) => 16 + BINARY_STUB.len(),
            Some(DataType::Boolean) => 1,
            Some(DataType::Null) => 0,
            Some(dtype) if dtype.is_primitive_numeric() || dtype.is_temporal() => {
                match dtype.to_physical() {
                    DataType::Int8 | DataType::UInt8 => 1,
                    DataType::Int16 | DataType::UInt16 => 2,
                    DataType::Int32 | DataType::UInt32 | DataType::Float32 => 4,
                    DataType::Int128 => 16,
                    _ => 8,
                }
            }
            Some(DataType::Decimal(..)) => 16,
            _ => footer_width(name).unwrap_or(64),
        })
        .sum::<usize>()
        .max(1)
}

/// Shrink `[buffer_start, buffer_end)` to at most `max_len` rows, kept around the view
/// `[view_start, view_end)` and inside `[floor, ceil)`.
fn shrink_around_view(
    view_start: usize,
    view_end: usize,
    max_len: usize,
    floor: usize,
    ceil: usize,
    buffer_start: &mut usize,
    buffer_end: &mut usize,
) {
    if buffer_end.saturating_sub(*buffer_start) <= max_len {
        return;
    }
    let view_len = view_end.saturating_sub(view_start);
    if view_len >= max_len {
        *buffer_start = view_start;
        *buffer_end = (view_start + max_len).min(ceil);
        return;
    }
    let half = (max_len - view_len) / 2;
    *buffer_end = (view_end + half).min(ceil);
    *buffer_start = buffer_end.saturating_sub(max_len).max(floor);
    if *buffer_start > view_start {
        *buffer_start = view_start;
    }
    *buffer_end = (*buffer_start + max_len).min(ceil);
}

/// The most files one buffer read opens, beyond those the view itself spans.
const MAX_FILES_PER_BUFFER: usize = 16;

/// Narrow `[start, end)` to at most `max_files` files, keeping every file the view
/// `[view_start, view_end)` lies in and adding the ones after it first.
fn limit_files(
    offsets: &[usize],
    view_start: usize,
    view_end: usize,
    start: usize,
    end: usize,
    max_files: usize,
) -> (usize, usize) {
    let (Some((first, last)), Some((view_first, view_last))) = (
        files_holding(offsets, start, end.saturating_sub(start)),
        files_holding(
            offsets,
            view_start,
            view_end.saturating_sub(view_start).max(1),
        ),
    ) else {
        return (start, end);
    };
    if last - first < max_files {
        return (start, end);
    }
    let (mut lo, mut hi) = (view_first.max(first), view_last.min(last));
    while hi - lo + 1 < max_files && (hi < last || lo > first) {
        if hi < last {
            hi += 1;
        }
        if hi - lo + 1 < max_files && lo > first {
            lo -= 1;
        }
    }
    (start.max(offsets[lo]), end.min(offsets[hi + 1]))
}

/// The first and last files holding rows `[start, start + len)`, given where each file's
/// rows start (`offsets`, with the total last). `None` when the rows lie past the end.
fn files_holding(offsets: &[usize], start: usize, len: usize) -> Option<(usize, usize)> {
    let files = offsets.len().checked_sub(1)?;
    let total = *offsets.last()?;
    if files == 0 || len == 0 || start >= total {
        return None;
    }
    let end = (start + len).min(total);
    // The file a row is in: the last one starting at or before it. Empty files start
    // where the next one does and are skipped over.
    let file_of = |row: usize| offsets.partition_point(|&o| o <= row).saturating_sub(1);
    Some((file_of(start), file_of(end - 1).min(files - 1)))
}

/// Snap `[start, end)` outward to the row groups it touches, given where each group
/// starts (`offsets`, with the total last).
///
/// Polars fetches a row group whole for any slice that touches it, so the groups the
/// view `[view_start, view_end)` lies in are always taken whole: paging inside them then
/// costs nothing. The other groups the window reaches into are added while the result
/// stays within `cap` rows (0 for no cap), the ones ahead of the view first.
fn align_to_row_groups(
    offsets: &[usize],
    view_start: usize,
    view_end: usize,
    start: usize,
    end: usize,
    cap: usize,
) -> (usize, usize) {
    let Some(groups) = offsets.len().checked_sub(1).filter(|n| *n > 0) else {
        return (start, end);
    };
    let group_of = |row: usize| {
        offsets
            .partition_point(|&o| o <= row)
            .saturating_sub(1)
            .min(groups - 1)
    };
    let last_row = |s: usize, e: usize| e.saturating_sub(1).max(s);
    let (mut lo, mut hi) = (
        group_of(view_start),
        group_of(last_row(view_start, view_end)),
    );
    let (want_lo, want_hi) = (group_of(start), group_of(last_row(start, end)));
    let fits = |lo: usize, hi: usize| cap == 0 || offsets[hi + 1] - offsets[lo] <= cap;
    loop {
        if hi < want_hi && fits(lo, hi + 1) {
            hi += 1;
        } else if lo > want_lo && fits(lo - 1, hi) {
            lo -= 1;
        } else {
            break;
        }
    }
    (offsets[lo], offsets[hi + 1])
}

impl DataTableState {
    pub fn new(
        lf: LazyFrame,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        polars_streaming: bool,
    ) -> Result<Self> {
        let schema = lf.clone().collect_schema()?;
        let column_order: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        Ok(Self {
            original_lf: lf.clone(),
            base_lf: lf.clone(),
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
            len_generation: next_len_generation(),
            parquet_count_dir: None,
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_ascending: true,
            active_query: String::new(),
            active_sql_query: String::new(),
            active_fuzzy_query: String::new(),
            column_order,
            locked_columns_count: 0,
            grouped: None,
            reshaped_lf: None,
            drilled_down_group_index: None,
            drilled_down_group_key: None,
            drilled_down_group_key_columns: None,
            pages_lookahead: pages_lookahead.unwrap_or(3),
            pages_lookback: pages_lookback.unwrap_or(3),
            max_buffered_rows: max_buffered_rows.unwrap_or(DEFAULT_MAX_BUFFERED_ROWS),
            max_buffered_mb: max_buffered_mb.unwrap_or(512),
            remote_source: false,
            row_group_offsets: None,
            remote_files: None,
            dataset_schema: None,
            drift_column_present: false,
            drift_groups: Arc::new(Vec::new()),
            drift_at_open: false,
            groups_at_open: Arc::new(Vec::new()),
            drift_file_starts: Vec::new(),
            drift_file_group: Vec::new(),
            drift_files: Vec::new(),
            notes: Vec::new(),
            notes_seen: false,
            notes_at_open: Vec::new(),
            column_widths: Vec::new(),
            observed_bytes_per_row: None,
            buffered_start_row: 0,
            buffered_end_row: 0,
            buffered_df: None,
            proximity_threshold: 0, // Will be set when visible_rows is known
            row_numbers: false,     // Will be set from options
            row_start_index: 1,     // Will be set from options
            last_pivot_spec: None,
            last_melt_spec: None,
            partition_columns: None,
            decompress_temp_file: None,
            polars_streaming,
            defer_collect: false,
            needs_recollect: false,
        })
    }

    /// Create state from an existing LazyFrame (e.g. from Python or in-memory). Uses OpenOptions for display/buffer settings.
    pub fn from_lazyframe(lf: LazyFrame, options: &crate::OpenOptions) -> Result<Self> {
        let mut state = Self::new(
            lf,
            options.pages_lookahead,
            options.pages_lookback,
            options.max_buffered_rows,
            options.max_buffered_mb,
            options.polars_streaming,
        )?;
        state.row_numbers = options.row_numbers;
        state.row_start_index = options.row_start_index;
        Ok(state)
    }

    /// Create state from a pre-collected schema and LazyFrame (for phased loading). Does not call collect_schema();
    /// df is None so the UI can render headers while the first collect() runs.
    /// When `partition_columns` is Some (e.g. hive), column order is partition cols first.
    pub fn from_schema_and_lazyframe(
        schema: Arc<Schema>,
        lf: LazyFrame,
        options: &crate::OpenOptions,
        partition_columns: Option<Vec<String>>,
    ) -> Result<Self> {
        let column_order: Vec<String> = if let Some(ref part) = partition_columns {
            let part_set: HashSet<&str> = part.iter().map(String::as_str).collect();
            let rest: Vec<String> = schema
                .iter_names()
                .map(|s| s.to_string())
                .filter(|c| !part_set.contains(c.as_str()))
                .collect();
            part.iter().cloned().chain(rest).collect()
        } else {
            schema.iter_names().map(|s| s.to_string()).collect()
        };
        Ok(Self {
            original_lf: lf.clone(),
            base_lf: lf.clone(),
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
            len_generation: next_len_generation(),
            parquet_count_dir: None,
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_ascending: true,
            active_query: String::new(),
            active_sql_query: String::new(),
            active_fuzzy_query: String::new(),
            column_order,
            locked_columns_count: 0,
            grouped: None,
            reshaped_lf: None,
            drilled_down_group_index: None,
            drilled_down_group_key: None,
            drilled_down_group_key_columns: None,
            pages_lookahead: options.pages_lookahead.unwrap_or(3),
            pages_lookback: options.pages_lookback.unwrap_or(3),
            max_buffered_rows: options
                .max_buffered_rows
                .unwrap_or(DEFAULT_MAX_BUFFERED_ROWS),
            max_buffered_mb: options.max_buffered_mb.unwrap_or(512),
            remote_source: false,
            row_group_offsets: None,
            remote_files: None,
            dataset_schema: None,
            drift_column_present: false,
            drift_groups: Arc::new(Vec::new()),
            drift_at_open: false,
            groups_at_open: Arc::new(Vec::new()),
            drift_file_starts: Vec::new(),
            drift_file_group: Vec::new(),
            drift_files: Vec::new(),
            notes: Vec::new(),
            notes_seen: false,
            notes_at_open: Vec::new(),
            column_widths: Vec::new(),
            observed_bytes_per_row: None,
            buffered_start_row: 0,
            buffered_end_row: 0,
            buffered_df: None,
            proximity_threshold: 0,
            row_numbers: options.row_numbers,
            row_start_index: options.row_start_index,
            last_pivot_spec: None,
            last_melt_spec: None,
            partition_columns,
            decompress_temp_file: None,
            polars_streaming: options.polars_streaming,
            defer_collect: false,
            needs_recollect: false,
        })
    }

    /// Make `lf` the data as loaded: the root of the pipeline (`original_lf` and
    /// `base_lf`), the frame shown, and the schema. For load-time options such as header
    /// trimming, string parsing and dropped footer rows, which have to survive a later
    /// filter or sort.
    fn replace_original_lf(&mut self, lf: &LazyFrame) -> Result<()> {
        self.original_lf = lf.clone();
        self.base_lf = lf.clone();
        self.schema = lf.clone().collect_schema()?;
        self.lf = lf.clone();
        Ok(())
    }

    /// Make `lf` the frame shown and the base the sidebar filters and sort go on top of,
    /// with `schema` as its schema and every column in view. Row counts are invalidated.
    fn install_base(&mut self, lf: LazyFrame, schema: Arc<Schema>) {
        self.invalidate_num_rows();
        // A new frame is the user's own projection of the data; its rows no longer
        // stand for rows of a file, so nulls in it are just nulls, no column is marked
        // as missing from one, and notes about the files behind it no longer describe
        // what is on screen.
        self.drift_column_present = false;
        self.drift_groups = Arc::new(Vec::new());
        self.notes = Vec::new();
        // Rows of the new shape are measured afresh; the old width would plan the
        // window of a wide frame from a narrow one, or the reverse.
        self.observed_bytes_per_row = None;
        self.base_lf = lf.clone();
        self.lf = lf;
        self.schema = schema;
        self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();
    }

    /// The view state for a new pipeline root: no query bar text, no sidebar filters or
    /// sort, not drilled, the first `locked_columns_count` columns frozen, the buffer
    /// dropped and the cursor at the top left.
    fn reset_view_state(&mut self, locked_columns_count: usize) {
        self.active_query.clear();
        self.active_sql_query.clear();
        self.active_fuzzy_query.clear();
        self.locked_columns_count = locked_columns_count;
        self.filters.clear();
        self.sort_columns.clear();
        self.sort_ascending = true;
        self.start_row = 0;
        self.termcol_index = 0;
        self.drilled_down_group_index = None;
        self.drilled_down_group_key = None;
        self.drilled_down_group_key_columns = None;
        self.grouped = None;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.table_state.select(Some(0));
    }

    /// Install a query's result as the pipeline root with `query` as the one active
    /// query bar. Whether a pivot or melt in effect survives is the caller's call: SQL
    /// runs against it, the others run over the data as loaded. The caller collects.
    fn install_query_result(
        &mut self,
        lf: LazyFrame,
        schema: Arc<Schema>,
        query: ActiveQuery,
        locked_columns_count: usize,
    ) {
        self.install_base(lf, schema);
        self.reset_view_state(locked_columns_count);
        match query {
            ActiveQuery::Dsl(q) => self.active_query = q,
            ActiveQuery::Sql(q) => self.active_sql_query = q,
            ActiveQuery::Fuzzy(q) => self.active_fuzzy_query = q,
        }
    }

    /// The view no longer shows the pivot or melt, so nothing may run against it.
    fn forget_reshape(&mut self) {
        self.reshaped_lf = None;
        self.last_pivot_spec = None;
        self.last_melt_spec = None;
    }

    /// Reset LazyFrame and view state to original_lf. Schema is re-fetched so it matches
    /// after a previous query/SQL that may have changed columns. Caller should call
    /// collect() afterward if display update is needed (reset/query/fuzzy do; sql_query
    /// relies on event loop Collect).
    fn reset_lf_to_original(&mut self) {
        let schema = self
            .query_source()
            .collect_schema()
            .unwrap_or_else(|_| Arc::new(Schema::with_capacity(0)));
        self.install_base(self.original_lf.clone(), schema);
        // A reset is a return to the data as opened, so the rows stand for files again
        // and what datui noticed about them applies once more.
        self.drift_column_present = self.drift_at_open;
        self.drift_groups = self.groups_at_open.clone();
        self.notes = self.notes_at_open.clone();
        self.reshaped_lf = None;
        self.reset_view_state(0);
        self.restore_footer_count();
    }

    pub fn reset(&mut self) {
        self.reset_lf_to_original();
        self.error = None;
        self.suppress_error_display = false;
        self.last_pivot_spec = None;
        self.last_melt_spec = None;
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
        let path_str = path.as_os_str().to_string_lossy();
        let is_glob = path_str.contains('*');
        let pl_path = PlRefPath::try_from_path(path)?;
        let args = ScanArgsParquet {
            glob: is_glob,
            ..Default::default()
        };
        let lf = LazyFrame::scan_parquet(pl_path, args)?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            true,
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
            let pl_path = PlRefPath::try_from_path(p.as_ref())?;
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
            true,
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
        let pl_path = PlRefPath::try_from_path(path)?;
        let lf = LazyFrame::scan_ipc(pl_path, Default::default(), Default::default())?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            true,
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
            let pl_path = PlRefPath::try_from_path(p.as_ref())?;
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
            true,
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
            true,
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
            true,
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
            let empty_df = DataFrame::empty();
            let mut state = Self::new(
                empty_df.lazy(),
                pages_lookahead,
                pages_lookback,
                max_buffered_rows,
                max_buffered_mb,
                true,
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
        let df = DataFrame::new_infer_height(series_vec)?;
        let mut state = Self::new(
            df.lazy(),
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            true,
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
            true,
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
            true,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    /// Convert Arrow (arrow crate 57) RecordBatches to Polars DataFrame by value (ORC uses
    /// arrow 57; Polars uses polars-arrow, so we cannot use Series::from_arrow).
    fn arrow_record_batches_to_dataframe(batches: &[RecordBatch]) -> Result<DataFrame> {
        if batches.is_empty() {
            return Ok(DataFrame::empty());
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
            let df = DataFrame::new_infer_height(series_vec)?;
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

    /// Build a LazyFrame for hive-partitioned Parquet only (no schema collection, no partition discovery).
    /// Use this for phased loading so "Scanning input" is instant; schema and partition handling happen in DoLoadSchema.
    pub fn scan_parquet_hive(path: &Path) -> Result<LazyFrame> {
        let path_str = path.as_os_str().to_string_lossy();
        let is_glob = path_str.contains('*');
        let pl_path = PlRefPath::try_from_path(path)?;
        let args = ScanArgsParquet {
            hive_options: HiveOptions::new_enabled(),
            glob: is_glob,
            ..Default::default()
        };
        LazyFrame::scan_parquet(pl_path, args).map_err(Into::into)
    }

    /// Build a LazyFrame for hive-partitioned Parquet with a pre-computed schema (avoids slow collect_schema across all files).
    pub fn scan_parquet_hive_with_schema(path: &Path, schema: Arc<Schema>) -> Result<LazyFrame> {
        let path_str = path.as_os_str().to_string_lossy();
        let is_glob = path_str.contains('*');
        let pl_path = PlRefPath::try_from_path(path)?;
        let args = ScanArgsParquet {
            schema: Some(schema),
            hive_options: HiveOptions::new_enabled(),
            glob: is_glob,
            ..Default::default()
        };
        LazyFrame::scan_parquet(pl_path, args).map_err(Into::into)
    }

    /// Find the first parquet file along a single spine of a hive-partitioned directory (same walk as partition discovery).
    /// Returns `None` if the directory is empty or has no parquet files along that spine.
    fn first_parquet_file_in_hive_dir(path: &Path) -> Option<std::path::PathBuf> {
        const MAX_DEPTH: usize = 64;
        Self::first_parquet_file_spine(path, 0, MAX_DEPTH)
    }

    fn first_parquet_file_spine(
        path: &Path,
        depth: usize,
        max_depth: usize,
    ) -> Option<std::path::PathBuf> {
        if depth >= max_depth {
            return None;
        }
        let entries = fs::read_dir(path).ok()?;
        let mut first_partition_child: Option<std::path::PathBuf> = None;
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_file() {
                if child
                    .extension()
                    .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
                {
                    return Some(child);
                }
            } else if child.is_dir()
                && let Some(name) = child.file_name().and_then(|n| n.to_str())
                && name.contains('=')
                && first_partition_child.is_none()
            {
                first_partition_child = Some(child);
            }
        }
        first_partition_child.and_then(|p| Self::first_parquet_file_spine(&p, depth + 1, max_depth))
    }

    /// Recursively collect every `*.parquet` file under `dir`. Unlike the single-spine
    /// walks used for schema/partition discovery, this visits the whole tree because an
    /// exact row count needs every file. Bounded depth guards against pathological trees.
    fn collect_parquet_files(dir: &Path, out: &mut Vec<PathBuf>, depth: usize, max_depth: usize) {
        if depth >= max_depth {
            return;
        }
        let Ok(entries) = fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let child = entry.path();
            if child.is_dir() {
                Self::collect_parquet_files(&child, out, depth + 1, max_depth);
            } else if child
                .extension()
                .is_some_and(|e| e.eq_ignore_ascii_case("parquet"))
            {
                out.push(child);
            }
        }
    }

    /// Every Parquet file under `dir`, and what the footers of the ones worth reading
    /// say.
    ///
    /// The paths are sorted, so the dataset reads in the same order Polars would list
    /// it and the newest file is last. Returns the files, the indices whose footers
    /// were read — all of them, or a spread sample past
    /// [`crate::schema_union::MAX_FOOTER_READS`] — and those footers. A footer that
    /// cannot be read is `None`: one file mid-write must not stop the dataset from
    /// opening. Metadata only — no data is read.
    pub fn footers_of_parquet_dir(
        dir: &Path,
    ) -> (Vec<PathBuf>, Vec<usize>, Vec<Option<FileSchema>>) {
        const MAX_DEPTH: usize = 64;
        let mut files = Vec::new();
        Self::collect_parquet_files(dir, &mut files, 0, MAX_DEPTH);
        files.sort();
        let read = crate::schema_union::footers_to_read(files.len());
        let wanted: Vec<&Path> = read
            .iter()
            .filter_map(|i| files.get(*i).map(PathBuf::as_path))
            .collect();
        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(wanted.len().max(1))
            .max(1);
        let chunk_size = wanted.len().div_ceil(workers).max(1);
        let footers: Vec<Option<FileSchema>> = std::thread::scope(|scope| {
            let handles: Vec<_> = wanted
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || chunk.iter().map(|p| Self::footer_of(p)).collect())
                })
                .collect();
            handles
                .into_iter()
                // A worker that panicked must still account for its files, or every
                // footer after it would line up with the wrong file.
                .zip(wanted.chunks(chunk_size))
                .flat_map(|(h, chunk)| {
                    h.join()
                        .unwrap_or_else(|_| vec![None::<FileSchema>; chunk.len()])
                })
                .collect()
        });
        (files, read, footers)
    }

    /// One local Parquet file's columns and row count, from its footer.
    fn footer_of(path: &Path) -> Option<FileSchema> {
        let file = File::open(path).ok()?;
        let mut reader = ParquetReader::new(file);
        let arrow_schema = reader.schema().ok()?;
        let rows = reader.num_rows().ok()?;
        Some(FileSchema {
            schema: Arc::new(Schema::from_arrow_schema(arrow_schema.as_ref())),
            rows,
        })
    }

    /// Exact row count for a local Parquet hive directory, computed by summing per-file
    /// footer row counts (metadata only — no data is read). This is the cheap alternative
    /// to a `len()` data scan: footers are tiny, so the cost is one metadata read per file,
    /// fanned out across threads. Files that fail to read (e.g. an in-progress write) are
    /// skipped so a single bad file can't force the slow path; `Err` only if the directory
    /// has no readable Parquet files at all.
    pub fn count_rows_from_parquet_dir(dir: &Path) -> Result<usize> {
        const MAX_DEPTH: usize = 64;
        let mut files = Vec::new();
        Self::collect_parquet_files(dir, &mut files, 0, MAX_DEPTH);
        if files.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "No parquet files found under {}",
                dir.display()
            ));
        }

        let workers = std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(4)
            .min(files.len())
            .max(1);
        let chunk_size = files.len().div_ceil(workers);

        let (total, read_ok) = std::thread::scope(|scope| {
            let handles: Vec<_> = files
                .chunks(chunk_size)
                .map(|chunk| {
                    scope.spawn(move || {
                        let mut sum = 0usize;
                        let mut ok = 0usize;
                        for path in chunk {
                            if let Ok(file) = File::open(path)
                                && let Ok(n) = ParquetReader::new(file).num_rows()
                            {
                                sum += n;
                                ok += 1;
                            }
                        }
                        (sum, ok)
                    })
                })
                .collect();
            handles.into_iter().fold((0usize, 0usize), |(s, o), h| {
                let (cs, co) = h.join().unwrap_or((0, 0));
                (s + cs, o + co)
            })
        });

        if read_ok == 0 {
            return Err(color_eyre::eyre::eyre!(
                "Could not read any parquet footers under {}",
                dir.display()
            ));
        }
        Ok(total)
    }

    /// Read schema from a single parquet file (metadata only, no data scan). Used to avoid collect_schema() over many files.
    fn read_schema_from_single_parquet(path: &Path) -> Result<Arc<Schema>> {
        let file = File::open(path)?;
        let mut reader = ParquetReader::new(file);
        let arrow_schema = reader.schema()?;
        let schema = Schema::from_arrow_schema(arrow_schema.as_ref());
        Ok(Arc::new(schema))
    }

    /// Infer schema from one parquet file in a hive directory and merge with partition columns.
    /// Returns (merged_schema, partition_columns). Use with scan_parquet_hive_with_schema to avoid slow collect_schema().
    /// Only supported when path is a directory (not a glob). Returns Err if no parquet file found or read fails.
    pub fn schema_from_one_hive_parquet(path: &Path) -> Result<(Arc<Schema>, Vec<String>)> {
        let partition_columns = Self::discover_hive_partition_columns(path);
        let one_file = Self::first_parquet_file_in_hive_dir(path)
            .ok_or_else(|| color_eyre::eyre::eyre!("No parquet file found in hive directory"))?;
        let file_schema = Self::read_schema_from_single_parquet(&one_file)?;
        let values = Self::hive_partition_values(path, &one_file);
        let part_set: HashSet<&str> = partition_columns.iter().map(String::as_str).collect();
        let mut merged = Schema::with_capacity(partition_columns.len() + file_schema.len());
        for name in &partition_columns {
            merged.with_column(
                name.clone().into(),
                partition_dtype(name, &file_schema, &values),
            );
        }
        for (name, dtype) in file_schema.iter() {
            if !part_set.contains(name.as_str()) {
                merged.with_column(name.clone(), dtype.clone());
            }
        }
        Ok((Arc::new(merged), partition_columns))
    }

    /// `key=value` names of the partition directories beside each one on the path to `file`.
    pub(crate) fn hive_partition_values(root: &Path, file: &Path) -> Vec<(String, String)> {
        let mut out = Vec::new();
        let Some(rel) = file.strip_prefix(root).ok().and_then(Path::parent) else {
            return out;
        };
        let mut dir = root.to_path_buf();
        for component in rel.components() {
            let Some(segment) = component.as_os_str().to_str() else {
                break;
            };
            if let Some((key, _)) = segment.split_once('=') {
                for entry in fs::read_dir(&dir).into_iter().flatten().flatten() {
                    let name = entry.file_name();
                    let Some((k, v)) = name.to_str().and_then(|n| n.split_once('=')) else {
                        continue;
                    };
                    if k == key && entry.path().is_dir() {
                        out.push((k.to_string(), v.to_string()));
                    }
                }
            }
            dir.push(segment);
        }
        out
    }

    /// Discover hive partition column names (public for phased loading). Directory: single-spine walk; glob: parse pattern.
    pub fn discover_hive_partition_columns(path: &Path) -> Vec<String> {
        if path.is_dir() {
            Self::discover_partition_columns_from_path(path)
        } else {
            Self::discover_partition_columns_from_glob_pattern(path)
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
            if child.is_dir()
                && let Some(name) = child.file_name().and_then(|n| n.to_str())
                && let Some((key, _)) = name.split_once('=')
            {
                if !key.is_empty() && seen.insert(key.to_string()) {
                    columns.push(key.to_string());
                }
                if first_partition_child.is_none() {
                    first_partition_child = Some(child);
                }
                break;
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
            if let Some((key, rest)) = segment.split_once('=')
                && !key.is_empty()
                && (rest == "*" || !rest.contains('*'))
                && seen.insert(key.to_string())
            {
                columns.push(key.to_string());
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
        let pl_path = PlRefPath::try_from_path(path)?;
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
            true,
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

    /// Parse null value specs: "VAL" -> global, "COL=VAL" -> per-column (first '=' separates).
    fn parse_null_value_specs(specs: &[String]) -> (Vec<String>, Vec<(String, String)>) {
        let mut global = Vec::new();
        let mut per_column = Vec::new();
        for s in specs {
            if let Some(i) = s.find('=') {
                let (col, val) = (s[..i].to_string(), s[i + 1..].to_string());
                per_column.push((col, val));
            } else {
                global.push(s.clone());
            }
        }
        (global, per_column)
    }

    /// Build Polars NullValues from parsed specs. When both global and per_column are set, schema is required (caller does schema scan).
    fn build_polars_null_values(
        global: &[String],
        per_column: &[(String, String)],
        schema: Option<&Schema>,
    ) -> Option<NullValues> {
        if global.is_empty() && per_column.is_empty() {
            return None;
        }
        if per_column.is_empty() {
            let vals: Vec<PlSmallStr> = global
                .iter()
                .map(|s| PlSmallStr::from(s.as_str()))
                .collect();
            return Some(if vals.len() == 1 {
                NullValues::AllColumnsSingle(vals[0].clone())
            } else {
                NullValues::AllColumns(vals)
            });
        }
        if global.is_empty() {
            let pairs: Vec<(PlSmallStr, PlSmallStr)> = per_column
                .iter()
                .map(|(c, v)| (PlSmallStr::from(c.as_str()), PlSmallStr::from(v.as_str())))
                .collect();
            return Some(NullValues::Named(pairs));
        }
        let schema = schema?;
        let mut pairs: Vec<(PlSmallStr, PlSmallStr)> = Vec::new();
        let first_global = PlSmallStr::from(global[0].as_str());
        for (name, _) in schema.iter() {
            let col_name = name.as_str();
            let val = per_column
                .iter()
                .rev()
                .find(|(c, _)| c == col_name)
                .map(|(_, v)| PlSmallStr::from(v.as_str()))
                .unwrap_or_else(|| first_global.clone());
            pairs.push((PlSmallStr::from(col_name), val));
        }
        Some(NullValues::Named(pairs))
    }

    /// Infer CSV schema with minimal read (one row) for building null_values when both global and per-column are set.
    fn csv_schema_for_null_values(path: &Path, options: &OpenOptions) -> Result<Arc<Schema>> {
        let pl_path = PlRefPath::try_from_path(path)?;
        let mut reader = LazyCsvReader::new(pl_path).with_n_rows(Some(1));
        if let Some(skip_lines) = options.skip_lines {
            reader = reader.with_skip_lines(skip_lines);
        }
        if let Some(skip_rows) = options.skip_rows {
            reader = reader.with_skip_rows(skip_rows);
        }
        if let Some(has_header) = options.has_header {
            reader = reader.with_has_header(has_header);
        }
        reader = reader.with_try_parse_dates(options.csv_try_parse_dates());
        let mut lf = reader.finish()?;
        lf.collect_schema().map_err(color_eyre::eyre::Report::from)
    }

    /// Build Polars NullValues from options; path_for_schema required when both global and per-column specs are set.
    fn build_null_values_for_csv(
        options: &OpenOptions,
        path_for_schema: Option<&Path>,
    ) -> Result<Option<NullValues>> {
        let specs = match &options.null_values {
            None => return Ok(None),
            Some(s) if s.is_empty() => return Ok(None),
            Some(s) => s.as_slice(),
        };
        let (global, per_column) = Self::parse_null_value_specs(specs);
        let nv = if !global.is_empty() && !per_column.is_empty() {
            let path = path_for_schema.ok_or_else(|| {
                color_eyre::eyre::eyre!(
                    "Internal error: path required for null_values with both global and per-column"
                )
            })?;
            let schema = Self::csv_schema_for_null_values(path, options)?;
            Self::build_polars_null_values(&global, &per_column, Some(schema.as_ref()))
        } else {
            Self::build_polars_null_values(&global, &per_column, None)
        };
        Ok(nv)
    }

    /// Trim leading/trailing whitespace from CSV column names. Applied whenever we have a CSV LazyFrame.
    fn trim_csv_column_names(mut lf: LazyFrame) -> Result<LazyFrame> {
        let schema = lf.collect_schema()?;
        let names: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        let trimmed: Vec<String> = names.iter().map(|s| s.trim().to_string()).collect();
        if names == trimmed {
            return Ok(lf);
        }
        Ok(lf.rename(
            names.iter().map(|s| s.as_str()),
            trimmed.iter().map(|s| s.as_str()),
            false,
        ))
    }

    /// If options.skip_tail_rows is set, run a count query and slice the LazyFrame to drop that many rows from the end. Used for CSV with trailing garbage/footer.
    fn apply_skip_tail_rows_csv(lf: LazyFrame, options: &OpenOptions) -> Result<LazyFrame> {
        let n = match options.skip_tail_rows {
            None | Some(0) => return Ok(lf),
            Some(n) => n,
        };
        let count_df = collect_lazy(lf.clone().select([len()]), options.polars_streaming)
            .map_err(color_eyre::eyre::Report::from)?;
        let total: u32 = match count_df.get(0) {
            Some(col) => match col.first() {
                Some(AnyValue::UInt32(v)) => *v,
                _ => return Ok(lf),
            },
            _ => {
                return Ok(lf);
            }
        };
        let keep = total.saturating_sub(n as u32);
        Ok(lf.slice(0, keep))
    }

    /// Try to detect a date format from a sample string (first format that parses).
    /// Returns None if no format matches, so we can avoid passing format: None to Polars (which can error).
    fn infer_date_format_from_sample(sample: &str) -> Option<&'static str> {
        const DATE_FMTS: &[&str] = &[
            "%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y%m%d", "%d-%m-%Y", "%d/%m/%Y", "%d.%m.%Y",
            "%m-%d-%Y", "%m/%d/%Y",
        ];
        DATE_FMTS
            .iter()
            .find(|fmt| NaiveDate::parse_from_str(sample, fmt).is_ok())
            .copied()
    }

    /// Try to detect a datetime format from a sample string.
    fn infer_datetime_format_from_sample(sample: &str) -> Option<&'static str> {
        const DATETIME_FMTS: &[&str] = &[
            "%Y-%m-%dT%H:%M:%S%.f",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d-%m-%YT%H:%M:%S%.f",
            "%d-%m-%YT%H:%M:%S",
            "%d-%m-%Y %H:%M:%S%.f",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%YT%H:%M:%S%.f",
            "%d/%m/%YT%H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%Y%m%dT%H%M%S%.f",
            "%Y%m%d %H%M%S",
        ];
        DATETIME_FMTS
            .iter()
            .find(|fmt| NaiveDateTime::parse_from_str(sample, fmt).is_ok())
            .copied()
    }

    /// Parse a string ChunkedArray into a Duration ChunkedArray (nanoseconds). Uses Polars duration
    /// format (e.g. `1d`, `2h30m`, `-1w2d`). Invalid or null inputs become null in the output.
    fn string_chunked_to_duration_ns(str_ca: &StringChunked) -> DurationChunked {
        let name = str_ca.name().clone();
        let vals: Vec<Option<i64>> = str_ca
            .iter()
            .map(|opt_s| {
                opt_s.and_then(|s| {
                    polars::time::Duration::try_parse(s)
                        .ok()
                        .map(|d| d.duration_ns())
                })
            })
            .collect();
        let int_ca = Int64Chunked::from_iter_options(name, vals.into_iter());
        int_ca.into_duration(TimeUnit::Nanoseconds)
    }

    /// Try to detect a time format from a sample string (HH:MM:SS, HH:MM, with optional fractional seconds).
    fn infer_time_format_from_sample(sample: &str) -> Option<&'static str> {
        const TIME_FMTS: &[&str] = &[
            "%H:%M:%S%.9f",
            "%H:%M:%S%.6f",
            "%H:%M:%S%.3f",
            "%H:%M:%S",
            "%H:%M",
        ];
        TIME_FMTS
            .iter()
            .find(|fmt| NaiveTime::parse_from_str(sample, fmt).is_ok())
            .copied()
    }

    /// Apply trim and type inference to CSV string columns when --parse-strings is enabled.
    /// Samples up to `options.parse_strings_sample_rows` rows to infer types, then overlays lazy exprs (trim then cast) on the LazyFrame.
    fn apply_parse_strings_to_csv_lazyframe(
        lf: LazyFrame,
        options: &OpenOptions,
    ) -> Result<LazyFrame> {
        let target = match &options.parse_strings {
            None => return Ok(lf),
            Some(t) => t,
        };
        let sample_rows = options.parse_strings_sample_rows;
        let sample_df = lf.clone().limit(sample_rows as u32).collect()?;
        let schema = sample_df.schema();
        let string_cols: Vec<String> = schema
            .iter()
            .filter(|(_name, dtype)| **dtype == DataType::String)
            .map(|(name, _)| name.to_string())
            .collect();
        let target_cols: Vec<String> = match target {
            ParseStringsTarget::All => string_cols,
            ParseStringsTarget::Columns(c) => c
                .iter()
                .filter(|name| string_cols.contains(name))
                .cloned()
                .collect(),
        };
        if target_cols.is_empty() {
            return Ok(lf);
        }
        use polars::datatypes::TimeUnit;
        let whitespace_pat = lit(PlSmallStr::from_static(" \t\n\r"));
        // Re-collect sample with values trimmed so inference sees "1" not " 1 "
        let trim_sample_exprs: Vec<Expr> = target_cols
            .iter()
            .map(|c| {
                col(PlSmallStr::from(c.as_str()))
                    .str()
                    .strip_chars(whitespace_pat.clone())
                    .alias(PlSmallStr::from(c.as_str()))
            })
            .collect();
        // Treat blank (empty string after trim) as null so "all null" and accept_type use normalized semantics.
        let blank_to_null_exprs: Vec<Expr> = target_cols
            .iter()
            .map(|c| {
                let name = PlSmallStr::from(c.as_str());
                when(col(name.clone()).eq(lit(PlSmallStr::from_static(""))))
                    .then(Null {}.lit())
                    .otherwise(col(name.clone()))
                    .alias(name)
            })
            .collect();
        let sample_df = lf
            .clone()
            .limit(sample_rows as u32)
            .with_columns(trim_sample_exprs)
            .with_columns(blank_to_null_exprs)
            .collect()?;
        let mut exprs = Vec::with_capacity(target_cols.len());
        for col_name in &target_cols {
            let s = sample_df.column(col_name.as_str())?;
            let null_before = s.null_count();
            let len = s.len();
            // Accept type if we didn't introduce new nulls (null_after <= null_before).
            let accept_type = |null_after: usize| null_after <= null_before;
            // Inference order: Date → Datetime → Time → Duration → Int64 → Float64 → String.
            enum InferredType {
                Date,
                Datetime,
                Time,
                Duration,
                Int64,
                Float64,
                String,
            }
            let (inferred, date_fmt, datetime_fmt, time_fmt) = if null_before == len {
                // Column is all null (including blanks treated as null): leave as string.
                (InferredType::String, None, None, None)
            } else {
                match s.str() {
                    Err(_) => (InferredType::String, None, None, None),
                    Ok(str_ca) => {
                        let first_val: Option<&str> = str_ca
                            .iter()
                            .find_map(|o: Option<&str>| o.filter(|s: &&str| !s.is_empty()));
                        let (mut t, mut date_fmt, mut datetime_fmt, mut time_fmt) = match str_ca
                            .as_date(None, true)
                        {
                            Ok(as_date) if accept_type(as_date.null_count()) => {
                                let fmt = first_val.and_then(Self::infer_date_format_from_sample);
                                if fmt.is_some() {
                                    (InferredType::Date, fmt.map(String::from), None, None)
                                } else {
                                    (InferredType::String, None, None, None)
                                }
                            }
                            _ => (InferredType::String, None, None, None),
                        };
                        if matches!(t, InferredType::String) {
                            let amb_name: &str = str_ca.name().as_ref();
                            let amb_series = Series::new(
                                PlSmallStr::from(amb_name),
                                vec!["raise"; str_ca.len()],
                            );
                            let amb_ca =
                                amb_series.str().map_err(color_eyre::eyre::Report::from)?;
                            (t, date_fmt, datetime_fmt, time_fmt) = match str_ca.as_datetime(
                                None,
                                TimeUnit::Microseconds,
                                true,
                                false,
                                None,
                                amb_ca,
                            ) {
                                Ok(as_dt) if accept_type(as_dt.null_count()) => {
                                    let fmt =
                                        first_val.and_then(Self::infer_datetime_format_from_sample);
                                    if fmt.is_some() {
                                        (InferredType::Datetime, None, fmt.map(String::from), None)
                                    } else {
                                        (InferredType::String, None, None, None)
                                    }
                                }
                                _ => (InferredType::String, None, None, None),
                            };
                        }
                        if matches!(t, InferredType::String) {
                            (t, date_fmt, datetime_fmt, time_fmt) = match str_ca.as_time(None, true)
                            {
                                Ok(as_time) if accept_type(as_time.null_count()) => {
                                    let fmt =
                                        first_val.and_then(Self::infer_time_format_from_sample);
                                    if fmt.is_some() {
                                        (InferredType::Time, None, None, fmt.map(String::from))
                                    } else {
                                        (InferredType::String, None, None, None)
                                    }
                                }
                                _ => (InferredType::String, None, None, None),
                            };
                        }
                        if matches!(t, InferredType::String) {
                            let duration_ca = Self::string_chunked_to_duration_ns(str_ca);
                            (t, date_fmt, datetime_fmt, time_fmt) =
                                if accept_type(duration_ca.null_count()) {
                                    (InferredType::Duration, None, None, None)
                                } else {
                                    (InferredType::String, None, None, None)
                                };
                        }
                        if matches!(t, InferredType::String) {
                            (t, date_fmt, datetime_fmt, time_fmt) =
                                match s.strict_cast(&DataType::Int64) {
                                    Ok(as_int) if accept_type(as_int.null_count()) => {
                                        (InferredType::Int64, None, None, None)
                                    }
                                    _ => (InferredType::String, None, None, None),
                                };
                        }
                        if matches!(t, InferredType::String) {
                            (t, date_fmt, datetime_fmt, time_fmt) =
                                match s.strict_cast(&DataType::Float64) {
                                    Ok(as_float) if accept_type(as_float.null_count()) => {
                                        (InferredType::Float64, None, None, None)
                                    }
                                    _ => (InferredType::String, None, None, None),
                                };
                        }
                        (t, date_fmt, datetime_fmt, time_fmt)
                    }
                }
            };
            let base = col(PlSmallStr::from(col_name.as_str()))
                .str()
                .strip_chars(whitespace_pat.clone());
            // Treat blank as null in the applied pipeline so blanks become null in the result.
            let base_with_nulls = when(base.clone().eq(lit(PlSmallStr::from_static(""))))
                .then(Null {}.lit())
                .otherwise(base.clone());
            let expr = match inferred {
                InferredType::Date => {
                    let opts = StrptimeOptions {
                        format: date_fmt.as_deref().map(PlSmallStr::from),
                        strict: false,
                        exact: false,
                        cache: true,
                    };
                    base_with_nulls
                        .clone()
                        .str()
                        .to_date(opts)
                        .alias(PlSmallStr::from(col_name.as_str()))
                }
                InferredType::Datetime => {
                    let opts = StrptimeOptions {
                        format: datetime_fmt.as_deref().map(PlSmallStr::from),
                        strict: false,
                        exact: false,
                        cache: true,
                    };
                    base_with_nulls
                        .clone()
                        .str()
                        .to_datetime(
                            Some(TimeUnit::Microseconds),
                            None,
                            opts,
                            lit(PlSmallStr::from_static("raise")),
                        )
                        .alias(PlSmallStr::from(col_name.as_str()))
                }
                InferredType::Time => {
                    let opts = StrptimeOptions {
                        format: time_fmt.as_deref().map(PlSmallStr::from),
                        strict: false,
                        exact: true,
                        cache: true,
                    };
                    base_with_nulls
                        .clone()
                        .str()
                        .to_time(opts)
                        .alias(PlSmallStr::from(col_name.as_str()))
                }
                // No strptime for Duration in Polars; parse via map using Duration::try_parse.
                InferredType::Duration => base_with_nulls
                    .clone()
                    .map(
                        |c: Column| {
                            let str_ca = c.str()?;
                            let duration_ca = Self::string_chunked_to_duration_ns(str_ca);
                            Ok(duration_ca.into_column())
                        },
                        |_schema: &Schema, field: &Field| {
                            Ok(Field::new(
                                field.name().clone(),
                                DataType::Duration(TimeUnit::Nanoseconds),
                            ))
                        },
                    )
                    .alias(PlSmallStr::from(col_name.as_str())),
                InferredType::Int64 => base_with_nulls
                    .clone()
                    .cast(DataType::Int64)
                    .alias(PlSmallStr::from(col_name.as_str())),
                InferredType::Float64 => base_with_nulls
                    .cast(DataType::Float64)
                    .alias(PlSmallStr::from(col_name.as_str())),
                InferredType::String => base.alias(PlSmallStr::from(col_name.as_str())),
            };
            exprs.push(expr);
        }
        Ok(lf.with_columns(exprs))
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        let nv = Self::build_null_values_for_csv(options, Some(path))?;

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
                        if let Some(n) = options.infer_schema_length {
                            read_options.infer_schema_length = Some(n);
                        }
                        read_options.ignore_errors = options.ignore_errors;
                        read_options = read_options.map_parse_options(|opts| {
                            let o = opts.with_try_parse_dates(options.csv_try_parse_dates());
                            match &nv {
                                Some(n) => o.with_null_values(Some(n.clone())),
                                None => o,
                            }
                        });
                        let df = read_options
                            .try_into_reader_with_file_path(Some(path.into()))?
                            .finish()?;
                        let mut lf = Self::trim_csv_column_names(df.lazy())?;
                        lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options)?;
                        lf = Self::apply_skip_tail_rows_csv(lf, options)?;
                        let mut state = Self::new(
                            lf,
                            options.pages_lookahead,
                            options.pages_lookback,
                            options.max_buffered_rows,
                            options.max_buffered_mb,
                            options.polars_streaming,
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
                        if let Some(n) = options.infer_schema_length {
                            read_options.infer_schema_length = Some(n);
                        }
                        read_options.ignore_errors = options.ignore_errors;
                        read_options = read_options.map_parse_options(|opts| {
                            let o = opts.with_try_parse_dates(options.csv_try_parse_dates());
                            match &nv {
                                Some(n) => o.with_null_values(Some(n.clone())),
                                None => o,
                            }
                        });
                        let df = CsvReader::new(std::io::Cursor::new(decompressed))
                            .with_options(read_options)
                            .finish()?;
                        let mut lf = Self::trim_csv_column_names(df.lazy())?;
                        lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options)?;
                        lf = Self::apply_skip_tail_rows_csv(lf, options)?;
                        let mut state = Self::new(
                            lf,
                            options.pages_lookahead,
                            options.pages_lookback,
                            options.max_buffered_rows,
                            options.max_buffered_mb,
                            options.polars_streaming,
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
                        if let Some(n) = options.infer_schema_length {
                            read_options.infer_schema_length = Some(n);
                        }
                        read_options.ignore_errors = options.ignore_errors;
                        read_options = read_options.map_parse_options(|opts| {
                            let o = opts.with_try_parse_dates(options.csv_try_parse_dates());
                            match &nv {
                                Some(n) => o.with_null_values(Some(n.clone())),
                                None => o,
                            }
                        });
                        let df = CsvReader::new(std::io::Cursor::new(decompressed))
                            .with_options(read_options)
                            .finish()?;
                        let mut lf = Self::trim_csv_column_names(df.lazy())?;
                        lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options)?;
                        lf = Self::apply_skip_tail_rows_csv(lf, options)?;
                        let mut state = Self::new(
                            lf,
                            options.pages_lookahead,
                            options.pages_lookback,
                            options.max_buffered_rows,
                            options.max_buffered_mb,
                            options.polars_streaming,
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
                let nv_temp = Self::build_null_values_for_csv(options, Some(temp.path()))?;
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
                        if let Some(n) = options.infer_schema_length {
                            reader = reader.with_infer_schema_length(Some(n));
                        }
                        reader = reader.with_ignore_errors(options.ignore_errors);
                        reader = reader.with_try_parse_dates(options.csv_try_parse_dates());
                        reader = match &nv_temp {
                            Some(n) => reader
                                .map_parse_options(|opts| opts.with_null_values(Some(n.clone()))),
                            None => reader,
                        };
                        reader
                    },
                )?;
                let mut lf = Self::trim_csv_column_names(std::mem::take(&mut state.lf))?;
                state.replace_original_lf(&lf)?;
                if options.parse_strings.is_some() {
                    lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options)?;
                    state.replace_original_lf(&lf)?;
                }
                lf = Self::apply_skip_tail_rows_csv(lf, options)?;
                state.replace_original_lf(&lf)?;
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
                    if let Some(n) = options.infer_schema_length {
                        reader = reader.with_infer_schema_length(Some(n));
                    }
                    reader = reader.with_ignore_errors(options.ignore_errors);
                    reader = reader.with_try_parse_dates(options.csv_try_parse_dates());
                    reader = match &nv {
                        Some(n) => {
                            reader.map_parse_options(|opts| opts.with_null_values(Some(n.clone())))
                        }
                        None => reader,
                    };
                    reader
                },
            )?;
            let mut lf = Self::trim_csv_column_names(std::mem::take(&mut state.lf))?;
            state.replace_original_lf(&lf)?;
            if options.parse_strings.is_some() {
                lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options)?;
                state.replace_original_lf(&lf)?;
            }
            lf = Self::apply_skip_tail_rows_csv(lf, options)?;
            state.replace_original_lf(&lf)?;
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
        let pl_path = PlRefPath::try_from_path(path)?;
        let reader = LazyCsvReader::new(pl_path);
        let lf = func(reader).finish()?;
        Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            true,
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
        let nv = Self::build_null_values_for_csv(options, Some(paths[0].as_ref()))?;
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlRefPath::try_from_path(p.as_ref())?;
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
            if let Some(n) = options.infer_schema_length {
                reader = reader.with_infer_schema_length(Some(n));
            }
            reader = reader.with_ignore_errors(options.ignore_errors);
            reader = reader.with_try_parse_dates(options.csv_try_parse_dates());
            reader = match &nv {
                Some(n) => reader.map_parse_options(|opts| opts.with_null_values(Some(n.clone()))),
                None => reader,
            };
            let lf = reader.finish()?;
            lazy_frames.push(lf);
        }
        let mut lf = Self::trim_csv_column_names(polars::prelude::concat(
            lazy_frames.as_slice(),
            Default::default(),
        )?)?;
        lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options)?;
        lf = Self::apply_skip_tail_rows_csv(lf, options)?;
        let mut state = Self::new(
            lf,
            options.pages_lookahead,
            options.pages_lookback,
            options.max_buffered_rows,
            options.max_buffered_mb,
            options.polars_streaming,
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
        let pl_path = PlRefPath::try_from_path(path)?;
        let lf = LazyJsonLineReader::new(pl_path).finish()?;
        let mut state = Self::new(
            lf,
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            true,
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
            let pl_path = PlRefPath::try_from_path(p.as_ref())?;
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
            true,
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
            true,
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
            true,
        )?;
        state.row_numbers = row_numbers;
        state.row_start_index = row_start_index;
        Ok(state)
    }

    pub fn from_delimited(path: &Path, delimiter: u8, options: &OpenOptions) -> Result<Self> {
        let pl_path = PlRefPath::try_from_path(path)?;
        let mut reader = LazyCsvReader::new(pl_path).with_separator(delimiter);
        if let Some(skip_lines) = options.skip_lines {
            reader = reader.with_skip_lines(skip_lines);
        }
        if let Some(skip_rows) = options.skip_rows {
            reader = reader.with_skip_rows(skip_rows);
        }
        if let Some(has_header) = options.has_header {
            reader = reader.with_has_header(has_header);
        }
        let lf = reader.finish()?;
        let mut state = Self::new(
            lf,
            options.pages_lookahead,
            options.pages_lookback,
            options.max_buffered_rows,
            options.max_buffered_mb,
            true,
        )?;
        state.row_numbers = options.row_numbers;
        state.row_start_index = options.row_start_index;
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
            if let Some(df) = self.df.as_ref()
                && rows > 0
                && df.shape().0 <= self.visible_rows
            {
                return false;
            }
            let unclamped = (self.start_row as i64 + rows) as usize;
            if rows > 0 {
                unclamped.min(self.num_rows.saturating_sub(self.visible_rows))
            } else {
                unclamped
            }
        };
        if new_start_row == self.start_row {
            return false;
        }
        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;
        !within_buffer
    }

    /// Update scroll position. If the view is within the buffer, re-slices display.
    /// If outside the buffer, sets the position but the caller must trigger a collect
    /// (synchronous or async) to load the new buffer range.
    /// Returns true if a collect is needed (view is outside the current buffer).
    pub fn slide_table(&mut self, rows: i64) -> bool {
        if rows < 0 && self.start_row == 0 {
            return false;
        }

        let new_start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.df.as_ref()
                && rows > 0
                && df.shape().0 <= self.visible_rows
            {
                return false;
            }
            let unclamped = (self.start_row as i64 + rows) as usize;
            if rows > 0 {
                // Clamp forward scroll to keep at least visible_rows of data in view.
                // Without this, holding PageDown at the bottom pushes start_row past
                // num_rows, which makes scroll_would_trigger_collect fire repeatedly
                // and can leave busy stuck if the resulting collect is a no-op.
                unclamped.min(self.num_rows.saturating_sub(self.visible_rows))
            } else {
                unclamped
            }
        };

        if new_start_row == self.start_row {
            return false;
        }

        let view_end = new_start_row
            + self
                .visible_rows
                .min(self.num_rows.saturating_sub(new_start_row));
        let within_buffer = new_start_row >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        self.start_row = new_start_row;

        if within_buffer {
            // Re-slice display from existing buffer.
            self.slice_from_buffer();
            if self.table_state.selected().is_none() {
                self.table_state.select(Some(0));
            }
            false
        } else {
            true // caller must collect
        }
    }

    pub fn collect(&mut self) {
        if self.defer_collect {
            return;
        }
        // Update proximity threshold based on visible rows
        if self.visible_rows > 0 {
            self.proximity_threshold = self.visible_rows;
        }

        // Run len() only when lf has changed (query, filter, sort, pivot, melt, reset, drill).
        if !self.num_rows_valid {
            self.num_rows = match collect_lazy(row_count_lf(&self.lf), self.polars_streaming) {
                Ok(df) => match df.get(0) {
                    Some(col) => match col.first() {
                        Some(AnyValue::UInt64(len)) => *len as usize,
                        _ => 0,
                    },
                    _ => 0,
                },
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
            self.buffered_df = None;
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
        // fit_window caps at max_buffered_rows and slides the window when at cap.

        if within_buffer {
            let dist_to_start = view_start.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);

            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.buffered_end_row < self.num_rows;

            if !needs_expansion_back && !needs_expansion_forward {
                // Column scroll only: reuse cached full buffer and re-slice into locked/scroll columns.
                let expected_len = self
                    .buffered_end_row
                    .saturating_sub(self.buffered_start_row);
                if self
                    .buffered_df
                    .as_ref()
                    .is_some_and(|b| b.height() == expected_len)
                {
                    self.slice_buffer_into_display();
                    if self.table_state.selected().is_none() {
                        self.table_state.select(Some(0));
                    }
                    return;
                }
                self.load_buffer(self.buffered_start_row, self.buffered_end_row);
                if self.table_state.selected().is_none() {
                    self.table_state.select(Some(0));
                }
                return;
            }

            let mut new_buffer_start = if needs_expansion_back {
                view_start.saturating_sub(self.reach_rows(self.pages_lookback))
            } else {
                self.buffered_start_row
            };

            let mut new_buffer_end = if needs_expansion_forward {
                (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows)
            } else {
                self.buffered_end_row
            };

            self.fit_window(
                view_start,
                view_end,
                &mut new_buffer_start,
                &mut new_buffer_end,
            );
            if self.holds_buffer(new_buffer_start, new_buffer_end) {
                // Fitting the expansion gave back the row group already held.
                self.slice_buffer_into_display();
                if self.table_state.selected().is_none() {
                    self.table_state.select(Some(0));
                }
                return;
            }
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
                && (view_start - self.buffered_end_row) <= self.reach_rows(self.pages_lookahead);
            let extend_backward_ok = scrolled_past_start
                && (self.buffered_start_row - view_end) <= self.reach_rows(self.pages_lookback);

            if extend_forward_ok {
                // View is just a few pages past buffer end; extend forward.
                new_buffer_start = self.buffered_start_row;
                new_buffer_end =
                    (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows);
            } else if extend_backward_ok {
                // View is just a few pages before buffer start; extend backward.
                new_buffer_start = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                new_buffer_end = self.buffered_end_row;
            } else if scrolled_past_end || scrolled_past_start {
                // Big jump (e.g. jump to end or jump to start): load a fresh window around the view.
                new_buffer_start = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                new_buffer_end =
                    (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows);
                let min_initial_len = self.min_buffer_len();
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
                new_buffer_start = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                new_buffer_end =
                    (view_end + self.reach_rows(self.pages_lookahead)).min(self.num_rows);

                // Ensure at least (1 + lookahead + lookback) pages so buffer size is consistent (e.g. 364 at 52 visible).
                let min_initial_len = self.min_buffer_len();
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

            self.fit_window(
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

    /// Prepare the LazyFrame and parameters for an async collect, without blocking.
    /// Updates internal state (proximity, start_row clamping) then returns
    /// a `CollectRequest` if a new buffer load is needed, or `None` if the current
    /// buffer is sufficient (in which case display slices are already updated).
    ///
    /// Caller must ensure `num_rows_valid` (via `set_num_rows`) before calling.
    /// `num_rows_override`, when supplied, applies that value first.
    /// Column expressions for every column in `column_order`, with binary columns replaced by a
    /// stub literal ([`BINARY_STUB`]) so their blobs are never read. Used both for the display
    /// buffer (keeps scroll/jump collects fast) and for analysis (describe/distribution/
    /// correlation), where reading multi-GB blobs across partitions would otherwise exhaust
    /// memory and freeze the process. The full bytes stay available through `lf` for export.
    pub(crate) fn binary_stub_exprs(&self) -> Vec<Expr> {
        self.column_order
            .iter()
            .map(|name| {
                if matches!(self.schema.get(name.as_str()), Some(DataType::Binary)) {
                    lit(BINARY_STUB).alias(name.as_str())
                } else {
                    col(name.as_str())
                }
            })
            .collect()
    }

    pub fn prepare_async_collect(
        &mut self,
        num_rows_override: Option<usize>,
    ) -> Option<CollectRequest> {
        if self.visible_rows > 0 {
            self.proximity_threshold = self.visible_rows;
        }

        if let Some(n) = num_rows_override {
            self.num_rows = n;
            self.num_rows_valid = true;
        }

        // `bound` is the exact total when known, or `usize::MAX` while the background
        // `len()` is still running. Using it instead of `self.num_rows` lets us plan a
        // top-of-data window for first paint without waiting for the count. See
        // `num_rows_bound`.
        let count_known = self.num_rows_valid;
        let bound = self.num_rows_bound();

        if count_known {
            if self.num_rows > 0 {
                let max_start = self.num_rows.saturating_sub(1);
                if self.start_row > max_start {
                    self.start_row = max_start;
                }
            } else {
                // Confirmed-empty dataset: clear everything.
                self.start_row = 0;
                self.buffered_start_row = 0;
                self.buffered_end_row = 0;
                self.buffered_df = None;
                self.df = None;
                self.locked_df = None;
                return None;
            }
        }

        let view_start = self.start_row;
        let view_end = self.start_row + self.visible_rows.min(bound - self.start_row);
        let within_buffer = view_start >= self.buffered_start_row
            && view_end <= self.buffered_end_row
            && self.buffered_end_row > 0;

        // Compute the buffer range using the same logic as collect().
        let (new_buffer_start, new_buffer_end) = if within_buffer {
            let dist_to_start = view_start.saturating_sub(self.buffered_start_row);
            let dist_to_end = self.buffered_end_row.saturating_sub(view_end);
            let needs_expansion_back =
                dist_to_start <= self.proximity_threshold && self.buffered_start_row > 0;
            let needs_expansion_forward =
                dist_to_end <= self.proximity_threshold && self.buffered_end_row < bound;

            if !needs_expansion_back && !needs_expansion_forward {
                // Buffer is fine, just re-slice display.
                (self.buffered_start_row, self.buffered_end_row)
            } else {
                let mut s = if needs_expansion_back {
                    view_start.saturating_sub(self.reach_rows(self.pages_lookback))
                } else {
                    self.buffered_start_row
                };
                let mut e = if needs_expansion_forward {
                    (view_end + self.reach_rows(self.pages_lookahead)).min(bound)
                } else {
                    self.buffered_end_row
                };
                self.fit_window(view_start, view_end, &mut s, &mut e);
                (s, e)
            }
        } else {
            let had_buffer = self.buffered_end_row > 0;
            let scrolled_past_end = had_buffer && view_start >= self.buffered_end_row;
            let scrolled_past_start = had_buffer && view_end <= self.buffered_start_row;
            let extend_forward_ok = scrolled_past_end
                && (view_start - self.buffered_end_row) <= self.reach_rows(self.pages_lookahead);
            let extend_backward_ok = scrolled_past_start
                && (self.buffered_start_row - view_end) <= self.reach_rows(self.pages_lookback);

            let mut s;
            let mut e;
            if extend_forward_ok {
                s = self.buffered_start_row;
                e = (view_end + self.reach_rows(self.pages_lookahead)).min(bound);
            } else if extend_backward_ok {
                s = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                e = self.buffered_end_row;
            } else {
                s = view_start.saturating_sub(self.reach_rows(self.pages_lookback));
                e = (view_end + self.reach_rows(self.pages_lookahead)).min(bound);
                let min_initial_len = self.min_buffer_len();
                let current_len = e.saturating_sub(s);
                if current_len < min_initial_len {
                    let need = min_initial_len.saturating_sub(current_len);
                    let can_extend_end = bound.saturating_sub(e);
                    let can_extend_start = s;
                    if can_extend_end >= need {
                        e = (e + need).min(bound);
                    } else if can_extend_start >= need {
                        s = s.saturating_sub(need);
                    } else {
                        e = (e + can_extend_end).min(bound);
                        s = s.saturating_sub(need.saturating_sub(can_extend_end));
                    }
                }
            }
            self.fit_window(view_start, view_end, &mut s, &mut e);
            (s, e)
        };

        let buffer_size = new_buffer_end.saturating_sub(new_buffer_start);
        if buffer_size == 0 {
            return None;
        }
        // Already held: the view fits, or fitting the expansion to whole row groups
        // gave back the group on hand.
        if self.holds_buffer(new_buffer_start, new_buffer_end) {
            self.slice_buffer_into_display();
            if self.table_state.selected().is_none() {
                self.table_state.select(Some(0));
            }
            return None;
        }

        let lf = match self.buffer_lf(new_buffer_start, buffer_size) {
            Ok(lf) => lf,
            Err(e) => {
                self.error = Some(e);
                return None;
            }
        };

        Some(CollectRequest {
            lf,
            polars_streaming: self.polars_streaming,
            buffer_start: new_buffer_start,
            buffer_end: new_buffer_end,
            // When the count isn't known yet, `num_rows` is provisional (the planned end of
            // this buffer). `apply_async_collect` keeps `num_rows_valid` false so the
            // background `len()` corrects it, unless the short read reveals the true end.
            num_rows: if count_known {
                self.num_rows
            } else {
                new_buffer_end
            },
            count_known,
        })
    }

    /// Apply the result of a background buffer load.
    pub fn apply_async_collect(&mut self, result: CollectResult) {
        let full_df = result.df;
        let returned_rows = full_df.height();
        let requested_rows = result.buffer_end.saturating_sub(result.buffer_start);

        if result.count_known {
            self.num_rows = result.num_rows;
            self.num_rows_valid = true;
        } else if returned_rows < requested_rows && (result.buffer_start == 0 || returned_rows > 0)
        {
            // Short read: the slice ran off the end, so we now know the exact total
            // without waiting for the background len() count. A slice deep in the
            // frame that found nothing may lie past the data entirely; only the count
            // can say where it ends.
            self.num_rows = result.buffer_start + returned_rows;
            self.num_rows_valid = true;
        } else if !self.num_rows_valid {
            // Full buffer with the count still unresolved: render with a provisional
            // total (at least this buffer's end) and leave num_rows_valid false so the
            // in-flight background len() corrects it via set_num_rows().
            self.num_rows = self.num_rows.max(result.buffer_end);
        }
        // else: the background len() already resolved the exact count between this
        // buffer being requested and applied — keep it; don't downgrade to provisional.
        self.error = None;

        self.observe_bytes_per_row(&full_df);
        // A fill planned to be stitched on to rows since replaced (a synchronous
        // collect re-planned while it was out) neither abuts what is held nor holds the
        // view's first row: installing it would draw rows under the wrong numbers. Keep
        // what is held and plan again. A fill that holds the first row but not the whole
        // view (the terminal grew while it was out) is kept, and the rest fetched; a
        // downloaded row group is too costly to throw away for a resize.
        let view_end = self.start_row + self.visible_rows.max(1);
        let shows_view = result.buffer_start <= self.start_row
            && (self.start_row < result.buffer_start + returned_rows
                || returned_rows < requested_rows);
        let stitched = self.abuts_buffer(result.buffer_start, returned_rows);
        if !shows_view && !stitched {
            self.needs_recollect = true;
            return;
        }
        let (full_df, buffer_start) = self.stitch_buffer(full_df, result.buffer_start);
        let union_rows = full_df.height();
        let (mut full_df, eff_start, eff_end) = self.clamp_buffer_bytes(full_df, buffer_start);
        if stitched && eff_end - eff_start < union_rows {
            // A trim of the stitched union is a slice: without this the whole of both
            // chunks stays allocated behind it. A plain fill is left as collected; a
            // copy of it would be the very spike the budget guards against.
            full_df.rechunk_mut();
        }

        self.buffered_start_row = eff_start;
        self.buffered_end_row = eff_end;
        self.buffered_df = Some(full_df);
        // Slice the buffered DataFrame into display DataFrames (locked + scroll columns).
        self.slice_buffer_into_display();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
        if view_end > eff_end && eff_end < self.num_rows {
            self.needs_recollect = true;
        }
    }

    /// True when `rows` rows fetched from `start` run on from the rows on hand or up to
    /// them, so `stitch_buffer` will join them.
    fn abuts_buffer(&self, start: usize, rows: usize) -> bool {
        self.stitches_buffer()
            && (start == self.buffered_end_row || start + rows == self.buffered_start_row)
    }

    /// Join a fetched row group `df`, starting at `buffer_start`, on to the rows on hand
    /// when it runs on from them or up to them: the fill `fit_window` planned for a view
    /// straddling two groups. Returns the buffer to keep and its first row. A shape
    /// mismatch (the columns changed underneath) keeps the fetched rows alone.
    fn stitch_buffer(&mut self, df: DataFrame, buffer_start: usize) -> (DataFrame, usize) {
        if !self.abuts_buffer(buffer_start, df.height()) {
            return (df, buffer_start);
        }
        let Some(mut held) = self.buffered_df.take() else {
            return (df, buffer_start);
        };
        if buffer_start == self.buffered_end_row {
            if held.vstack_mut(&df).is_ok() {
                return (held, self.buffered_start_row);
            }
        } else {
            if let Ok(joined) = df.vstack(&held) {
                return (joined, buffer_start);
            }
        }
        (df, buffer_start)
    }

    /// Invalidate num_rows cache when lf is mutated. Takes a fresh `len_generation` so any
    /// in-flight background count for the previous `lf` is recognized as stale. Also drops
    /// the cheap Parquet-footer count source: once `lf` carries a filter/query/group, the
    /// row count no longer equals the sum of file footers.
    ///
    /// Draws from the shared counter rather than incrementing, so a mutation here can
    /// never land on the value a later dataset is about to be seeded with.
    fn invalidate_num_rows(&mut self) {
        self.num_rows_valid = false;
        self.len_generation = next_len_generation();
    }

    /// True while `lf` is the data as loaded: no sidebar filter or sort, no query in
    /// any bar, no pivot or melt, no drill-down. Derived rather than kept, so clearing
    /// the filters or un-sorting makes the frame pristine again by itself.
    fn is_pristine(&self) -> bool {
        self.filters.is_empty()
            && self.sort_columns.is_empty()
            && self.sort_ascending
            && self.active_query.is_empty()
            && self.active_sql_query.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.reshaped_lf.is_none()
            && self.grouped.is_none()
            && self.drilled_down_group_index.is_none()
    }

    /// A pristine scan's count is its footer's: take it back, without a `len()`, when
    /// the frame is the scan as loaded again.
    fn restore_footer_count(&mut self) {
        if !self.is_pristine() {
            return;
        }
        if let Some(total) = self.row_group_offsets.as_ref().and_then(|o| o.last()) {
            self.set_num_rows(*total);
        }
    }

    /// Record that the data was loaded from `dir` (a local Parquet hive directory),
    /// enabling the cheap footer-sum row count while the frame is pristine.
    pub fn set_parquet_count_dir(&mut self, dir: PathBuf) {
        self.parquet_count_dir = Some(dir);
    }

    /// The directory whose Parquet footers can be summed for an exact row count, if the
    /// current `lf` still allows it. See `parquet_count_dir`.
    pub fn parquet_count_dir(&self) -> Option<PathBuf> {
        self.parquet_count_dir
            .clone()
            .filter(|_| self.is_pristine())
    }

    /// Current count generation. A background `len()` task captures this; its result is
    /// only applied if the generation still matches (i.e. the data hasn't changed since).
    pub fn len_generation(&self) -> u64 {
        self.len_generation
    }

    /// Returns the cached row count when valid (same value shown in the control bar). Use this to
    /// avoid an extra full scan for analysis/describe when the table has already been collected.
    pub fn num_rows_if_valid(&self) -> Option<usize> {
        if self.num_rows_valid {
            Some(self.num_rows)
        } else {
            None
        }
    }

    /// True when num_rows reflects the current `lf`. Used by App to decide whether
    /// to dispatch a background len() query before planning the buffer collect.
    pub fn is_num_rows_valid(&self) -> bool {
        self.num_rows_valid
    }

    /// Effective upper bound on row indices for buffer planning. When the exact count
    /// is known, that's `num_rows`; when it isn't yet (first paint before the background
    /// `len()` resolves), treat the dataset as unbounded so we plan a top-of-data window
    /// (`slice(0, N)`) instead of clamping everything to a stale/zero count.
    fn num_rows_bound(&self) -> usize {
        if self.num_rows_valid {
            self.num_rows
        } else {
            usize::MAX
        }
    }

    /// Apply a row count computed in the background (so prepare_async_collect doesn't
    /// have to fall back to a blocking len() on the UI thread).
    pub fn set_num_rows(&mut self, n: usize) {
        self.num_rows = n;
        self.num_rows_valid = true;
        // A view past the end of a frame that turned out smaller comes back to it.
        if self.start_row > 0 && self.start_row >= n {
            self.start_row = n.saturating_sub(self.visible_rows);
            self.needs_recollect = true;
        }
    }

    /// Clone of the LazyFrame for off-thread queries (e.g. background len()).
    pub fn lf_clone(&self) -> LazyFrame {
        self.lf.clone()
    }

    /// Whether the current LazyFrame should use Polars streaming engine.
    pub fn polars_streaming_enabled(&self) -> bool {
        self.polars_streaming
    }

    /// True when a fill that runs on from the rows on hand, or up to them, will be
    /// stitched on to them rather than replace them. See `stitch_buffer`.
    pub(crate) fn stitches_buffer(&self) -> bool {
        self.remote_window() && self.buffer_on_hand()
    }

    /// True when every row of the buffered range is on hand.
    fn buffer_on_hand(&self) -> bool {
        self.buffered_end_row > self.buffered_start_row
            && self
                .buffered_df
                .as_ref()
                .is_some_and(|b| b.height() == self.buffered_end_row - self.buffered_start_row)
    }

    /// True when the rows on hand include `[start, end)`. The buffer is then cut down
    /// to that range, so a row group stitched on to cross into it is let go once the
    /// view has left it, rather than fetched again when the view comes back.
    fn holds_buffer(&mut self, start: usize, end: usize) -> bool {
        if !self.buffer_on_hand()
            || start < self.buffered_start_row
            || end > self.buffered_end_row
            || end <= start
        {
            return false;
        }
        if (start, end) != (self.buffered_start_row, self.buffered_end_row) {
            let offset = (start - self.buffered_start_row) as i64;
            // Rechunked, so the rows let go are freed rather than kept behind a slice.
            self.buffered_df = self.buffered_df.as_ref().map(|b| {
                let mut kept = b.slice(offset, end - start);
                kept.rechunk_mut();
                kept
            });
            self.buffered_start_row = start;
            self.buffered_end_row = end;
        }
        true
    }

    /// Start row of the currently buffered range.
    pub fn buffered_start(&self) -> usize {
        self.buffered_start_row
    }

    /// End row (exclusive) of the currently buffered range.
    pub fn buffered_end(&self) -> usize {
        self.buffered_end_row
    }

    /// Mark the scan as reading an object store in place.
    ///
    /// Polars fetches a Parquet row group whole for any slice that touches it and keeps
    /// nothing between collects, so the small, proximity-driven refills that suit a
    /// local file each download the same row group again: paging through one row group
    /// cost a fetch of it every few pages. A remote buffer is planned as a single window
    /// of `max_buffered_rows` around the view instead. Scrolling inside it costs
    /// nothing; leaving it, or a jump, costs one fetch.
    pub fn set_remote_source(&mut self) {
        self.remote_source = true;
    }

    /// True for a scan of an object store. See `set_remote_source`.
    pub fn is_remote_source(&self) -> bool {
        self.remote_source
    }

    /// Record the row groups of a remote Parquet object, `rows` in each, so a buffer
    /// fill is planned as whole groups (see `align_to_row_groups`). Also the row count.
    pub fn set_row_groups(&mut self, rows: &[usize]) {
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for n in rows {
            offsets.push(offsets.last().unwrap_or(&0) + n);
        }
        self.set_num_rows(*offsets.last().unwrap_or(&0));
        self.row_group_offsets = Some(offsets);
    }

    /// Record that the data is a remote dataset of many files. See `RemoteFiles`.
    pub fn set_remote_files(&mut self, files: RemoteFiles) {
        self.remote_files = Some(files);
    }

    /// Record what the footers said about the dataset's columns. See `DatasetSchema`.
    /// `file_rows` is each file's row count, in scan order, and empty when they are not
    /// all known — the same condition under which the scan numbers its rows.
    pub fn set_dataset_schema(
        &mut self,
        schema: crate::schema_union::DatasetSchema,
        file_rows: &[usize],
        files: &[String],
    ) {
        self.drift_files = files.to_vec();
        // The scan numbers rows exactly when the files differ and every one is counted.
        self.drift_column_present = schema.drifts() && file_rows.len() == schema.file_group.len();
        self.drift_groups = Arc::new(schema.groups.clone());
        self.drift_file_group = schema.file_group.clone();
        self.drift_file_starts = Vec::with_capacity(file_rows.len());
        let mut row = 0usize;
        for rows in file_rows {
            self.drift_file_starts.push(row);
            row += rows;
        }
        self.drift_at_open = self.drift_column_present;
        self.groups_at_open = self.drift_groups.clone();
        self.notes = crate::notes::from_dataset(&schema);
        self.notes_at_open = self.notes.clone();
        self.notes_seen = false;
        self.dataset_schema = Some(schema);
    }

    /// The frame as the user sees it: `lf` without the hidden row-index column.
    ///
    /// Everything that exports, reshapes, groups or analyses the data reads this. The
    /// buffer reads `lf` itself and keeps the column, which is how `display_drift`
    /// traces a row back to its file; it stays invisible because the display is only
    /// ever a projection of `column_order`, which never names it.
    pub fn visible_lf(&self) -> LazyFrame {
        Self::without_drift(self.lf.clone())
    }

    /// `lf` without the hidden drift column. A non-strict drop, so it is a no-op on a
    /// frame that never had one and no caller has to know which it holds.
    fn without_drift(lf: LazyFrame) -> LazyFrame {
        lf.drop(by_name([crate::schema_union::DRIFT_COLUMN], false, false))
    }

    /// The frame a query, a SQL statement or a fuzzy search builds on. Never carries
    /// the drift column: a query's rows are its own, and its schema becomes the
    /// column order, so the column would otherwise become one of the data's.
    pub fn query_source(&self) -> LazyFrame {
        Self::without_drift(self.original_lf.clone())
    }

    /// Whether rows still know which file they came from.
    pub fn drifts(&self) -> bool {
        self.drift_column_present
    }

    /// What each drift group is missing, for the renderer. Empty when nothing drifts.
    pub fn drift_groups(&self) -> Arc<Vec<crate::schema_union::DriftGroup>> {
        self.drift_groups.clone()
    }

    /// Put back what a frame was carrying, alongside the frame itself. Rolling one
    /// back without this would leave the flag and the frame disagreeing.
    pub fn restore_drift(
        &mut self,
        present: bool,
        groups: Arc<Vec<crate::schema_union::DriftGroup>>,
        notes: Vec<crate::notes::Note>,
    ) {
        self.drift_column_present = present;
        self.drift_groups = groups;
        self.notes = notes;
    }

    /// The name of the column an export adds when asked to say where each row is from.
    pub const SOURCE_FILE_COLUMN: &'static str = "source_file";

    /// Whether an export can name each row's file: the frame has to still carry the
    /// scan's row index, and the dataset has to have files to name.
    pub fn can_name_source_files(&self) -> bool {
        self.drift_column_present
            && !self.drift_files.is_empty()
            && self.drift_files.len() == self.drift_file_starts.len()
    }

    /// A name for the source-file column that no column of `df` already has.
    ///
    /// `source_file` is a name a dataset may well use itself — a folder of per-file
    /// extracts is exactly this feature's audience — and adding a column by a name
    /// already present replaces it, silently, in the file the user takes away.
    fn free_source_file_name(df: &DataFrame) -> String {
        let taken: Vec<String> = df
            .get_column_names()
            .iter()
            .map(|n| n.to_string())
            .collect();
        if !taken.iter().any(|n| n == Self::SOURCE_FILE_COLUMN) {
            return Self::SOURCE_FILE_COLUMN.to_string();
        }
        (1..)
            .map(|n| format!("{}_{n}", Self::SOURCE_FILE_COLUMN))
            .find(|candidate| !taken.contains(candidate))
            .expect("some suffix is free")
    }

    /// Replace the scan's hidden row index with the path of the file each row came
    /// from. The frame must have been collected with the index still on it.
    pub fn name_source_files(&self, mut df: DataFrame) -> PolarsResult<DataFrame> {
        let name = Self::free_source_file_name(&df);
        let rows = df.drop_in_place(crate::schema_union::DRIFT_COLUMN)?;
        let rows = rows.u32()?;
        let names: Vec<Option<&str>> = rows
            .iter()
            .map(|row| {
                let row = row? as usize;
                let file = self
                    .drift_file_starts
                    .partition_point(|&start| start <= row)
                    .saturating_sub(1);
                self.drift_files.get(file).map(String::as_str)
            })
            .collect();
        let column = Column::new(name.into(), names).cast(&DataType::String)?;
        df.with_column(column)?;
        Ok(df)
    }

    /// Take the scan's hidden row index off a collected frame, if it is there.
    ///
    /// An export that asked to name each row's file collects with the index still on,
    /// so every path out of that — including the ones where naming fails — has to
    /// remove it, or datui's own bookkeeping ends up in the user's file.
    pub fn drop_row_index(mut df: DataFrame) -> DataFrame {
        let _ = df.drop_in_place(crate::schema_union::DRIFT_COLUMN);
        df
    }

    /// What datui noticed about the dataset. Empty when there is nothing to say.
    pub fn notes(&self) -> &[crate::notes::Note] {
        &self.notes
    }

    /// Whether there is something to say that has not been offered yet.
    pub fn notes_unseen(&self) -> bool {
        !self.notes.is_empty() && !self.notes_seen
    }

    /// The Info panel has been opened; the quiet accent has done its job.
    pub fn mark_notes_seen(&mut self) {
        self.notes_seen = true;
    }

    /// What the footers said about the dataset's columns, when it is many files.
    pub fn dataset_schema(&self) -> Option<&crate::schema_union::DatasetSchema> {
        self.dataset_schema.as_ref()
    }

    /// The counter for a remote dataset's files, while its count would be the data's:
    /// the frame is the scan as loaded, and the files have not been counted yet.
    pub fn remote_files_counter(&self) -> Option<FileCounter> {
        self.remote_files
            .as_ref()
            .filter(|f| f.offsets.is_none() && self.is_pristine())
            .map(|f| f.count.clone())
    }

    /// Record the rows in each row group of each file of a remote dataset: the total,
    /// the row groups a buffer is planned in, and which files hold which rows.
    pub fn set_file_row_groups(&mut self, groups: &[Vec<usize>]) {
        let Some(files) = self.remote_files.as_mut() else {
            return;
        };
        if groups.len() != files.urls.len() {
            return;
        }
        let mut offsets = Vec::with_capacity(groups.len() + 1);
        offsets.push(0);
        for file in groups {
            offsets.push(offsets.last().unwrap_or(&0) + file.iter().sum::<usize>());
        }
        files.offsets = Some(offsets);
        let flat: Vec<usize> = groups.iter().flatten().copied().collect();
        if self.is_pristine() {
            self.set_row_groups(&flat);
        } else {
            // Kept for when the frame is the scan again (`restore_footer_count`).
            let mut row_offsets = Vec::with_capacity(flat.len() + 1);
            row_offsets.push(0);
            for n in &flat {
                row_offsets.push(row_offsets.last().unwrap_or(&0) + n);
            }
            self.row_group_offsets = Some(row_offsets);
        }
    }

    /// The frame for buffer rows `[start, start + len)`, columns in display order. For a
    /// remote dataset whose files are counted, a scan of only the files holding them.
    fn buffer_lf(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let mut all_columns = self.binary_stub_exprs();
        if self.drift_column_present {
            all_columns.push(col(crate::schema_union::DRIFT_COLUMN));
        }
        if let Some((files, offsets)) = self
            .remote_files
            .as_ref()
            .filter(|_| self.remote_window())
            .and_then(|f| f.offsets.as_ref().map(|o| (f, o)))
            && let Some((first, last)) = files_holding(offsets, start, len)
        {
            let lf = (files.scan)(&files.urls[first..=last])?;
            return Ok(lf
                .select(all_columns)
                .slice((start - offsets[first]) as i64, len as u32));
        }
        Ok(self
            .lf
            .clone()
            .select(all_columns)
            .slice(start as i64, len as u32))
    }

    /// Record the footer's average uncompressed width of each column, for the byte
    /// estimate of a buffer before one has been collected.
    pub fn set_column_widths(&mut self, widths: Vec<(String, usize)>) {
        self.column_widths = widths;
    }

    /// Bytes a buffered row takes: measured on the last buffer collected, or until
    /// then estimated from the schema.
    fn bytes_per_row(&self) -> usize {
        self.observed_bytes_per_row.unwrap_or_else(|| {
            estimate_bytes_per_row(&self.schema, &self.column_order, &self.column_widths)
        })
    }

    /// Rows the `max_buffered_mb` budget allows a buffer, never fewer than a screen;
    /// 0 for no budget. Planning to this, rather than trimming the collected frame to
    /// it, keeps a wide window from being materialized only to be cut down.
    fn byte_cap_rows(&self) -> usize {
        if self.max_buffered_mb == 0 {
            return 0;
        }
        let max_bytes = self.max_buffered_mb * 1024 * 1024;
        (max_bytes / self.bytes_per_row()).max(self.visible_rows.max(1))
    }

    /// Take the bytes per row of a collected buffer as the measure for the next plan.
    fn observe_bytes_per_row(&mut self, df: &DataFrame) {
        if df.height() > 0 {
            self.observed_bytes_per_row = Some((df.estimated_size() / df.height()).max(1));
        }
    }

    /// True while the buffer is planned as a remote window: a scan of an object store
    /// that nothing has been applied to. A query, filter, sort or reshape reads the
    /// object through a predicate, and `slice(0, N)` then stops at the first N matches,
    /// so the page-based window costs a row group where the remote one would read forty.
    fn remote_window(&self) -> bool {
        self.remote_source && self.is_pristine()
    }

    /// Rows the buffer reaches past the view in one direction: `pages` of it for a local
    /// file, a remote scan with something applied to it (see `remote_window`), or a
    /// remote dataset of many files; half the window for a pristine remote object
    /// (`fit_window` trims the two halves plus the view back to the cap).
    ///
    /// Many files are read a few at a time instead: what a read costs there is the
    /// files it opens, not its rows, and a wide window over a dataset of small files
    /// (a day of blocks in 2009 is a few rows) is hundreds of downloads.
    fn reach_rows(&self, pages: usize) -> usize {
        if !self.remote_window() || self.remote_files.is_some() {
            return pages * self.visible_rows.max(1);
        }
        let window = if self.max_buffered_rows > 0 {
            self.max_buffered_rows
        } else {
            DEFAULT_MAX_BUFFERED_ROWS
        };
        window / 2
    }

    /// The smallest buffer worth filling: a page plus the reach either side.
    fn min_buffer_len(&self) -> usize {
        self.visible_rows.max(1)
            + self.reach_rows(self.pages_lookahead)
            + self.reach_rows(self.pages_lookback)
    }

    /// True when the view already shows the last page, so End has nothing to load.
    pub fn at_end(&self) -> bool {
        self.start_row == self.num_rows.saturating_sub(self.visible_rows)
    }

    /// Fit a planned buffer `[buffer_start, buffer_end)` to the caps: `max_buffered_rows`
    /// and the byte budget around the view, then for a remote object whose footer is
    /// known the row groups the view lies in, cut back to the caps inside them.
    fn fit_window(
        &self,
        view_start: usize,
        view_end: usize,
        buffer_start: &mut usize,
        buffer_end: &mut usize,
    ) {
        let byte_cap = self.byte_cap_rows();
        let cap = match (self.max_buffered_rows, byte_cap) {
            (0, cap) | (cap, 0) => cap,
            (rows, bytes) => rows.min(bytes),
        };
        if cap > 0 {
            shrink_around_view(
                view_start,
                view_end,
                cap,
                0,
                self.num_rows_bound(),
                buffer_start,
                buffer_end,
            );
        }
        let Some(offsets) = self
            .row_group_offsets
            .as_deref()
            .filter(|_| self.remote_window())
        else {
            return;
        };
        (*buffer_start, *buffer_end) = align_to_row_groups(
            offsets,
            view_start,
            view_end,
            *buffer_start,
            *buffer_end,
            cap,
        );
        // The caps hold inside a group too: a group over them is read one window at
        // a time, the window kept inside the group so it never pulls the next one
        // before the view reaches it.
        if cap > 0 {
            let (floor, ceil) = (*buffer_start, *buffer_end);
            shrink_around_view(
                view_start,
                view_end,
                cap,
                floor,
                ceil,
                buffer_start,
                buffer_end,
            );
        }
        // Over many files, at most a few of them, around the view's.
        if let Some(file_offsets) = self.remote_files.as_ref().and_then(|f| f.offsets.as_ref()) {
            (*buffer_start, *buffer_end) = limit_files(
                file_offsets,
                view_start,
                view_end,
                *buffer_start,
                *buffer_end,
                MAX_FILES_PER_BUFFER,
            );
        }
        // A view straddling two groups needs both, but one is on hand: fetch the other
        // alone and stitch it on (see `apply_async_collect`).
        if self.buffer_on_hand() {
            let (held_start, held_end) = (self.buffered_start_row, self.buffered_end_row);
            if held_start <= *buffer_start && *buffer_start < held_end && held_end < *buffer_end {
                *buffer_start = held_end;
            } else if *buffer_start < held_start
                && held_start < *buffer_end
                && *buffer_end <= held_end
            {
                *buffer_end = held_start;
            }
        }
    }

    /// Trim a freshly collected buffer `df` (spanning `[buffer_start, buffer_start + df.height())`)
    /// down to the `max_buffered_mb` byte budget. Crucially, the kept window is centered on the
    /// current view rather than always taken from the buffer's head — otherwise a jump near the
    /// END of the dataset drops exactly the rows the view needs, leaving the table blank.
    /// Returns the (possibly sliced) df and its new `[start, end)` row range.
    fn clamp_buffer_bytes(&self, df: DataFrame, buffer_start: usize) -> (DataFrame, usize, usize) {
        let total = df.height();
        let full_end = buffer_start + total;
        if total == 0 {
            return (df, buffer_start, full_end);
        }
        // The row cap as well: a row group stitched on to the rows on hand can run over it.
        let mut max_rows = total;
        if self.max_buffered_rows > 0 {
            max_rows = max_rows.min(self.max_buffered_rows);
        }
        if self.max_buffered_mb > 0 {
            let bytes_per_row = (df.estimated_size() / total).max(1);
            max_rows = max_rows.min(self.max_buffered_mb * 1024 * 1024 / bytes_per_row);
        }
        let max_rows = max_rows.max(1);
        if max_rows >= total {
            return (df, buffer_start, full_end);
        }
        // Keep `max_rows` rows centered on the view so the visible window survives the trim.
        let view_off = self.start_row.saturating_sub(buffer_start).min(total);
        let view_len = self.visible_rows.max(1).min(total);
        let view_center = view_off + view_len / 2;
        let mut keep_start = view_center.saturating_sub(max_rows / 2);
        if keep_start + max_rows > total {
            keep_start = total - max_rows;
        }
        let kept = max_rows.min(total - keep_start);
        let sliced = df.slice(keep_start as i64, kept);
        (
            sliced,
            buffer_start + keep_start,
            buffer_start + keep_start + kept,
        )
    }

    fn load_buffer(&mut self, buffer_start: usize, buffer_end: usize) {
        let buffer_size = buffer_end.saturating_sub(buffer_start);
        if buffer_size == 0 {
            return;
        }

        let use_streaming = self.polars_streaming;
        let lf = match self.buffer_lf(buffer_start, buffer_size) {
            Ok(lf) => lf,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };
        let full_df = match collect_lazy(lf, use_streaming) {
            Ok(df) => df,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };

        // Trim to the byte budget while keeping the view in range (see clamp_buffer_bytes).
        self.observe_bytes_per_row(&full_df);
        let (full_df, buffer_start) = self.stitch_buffer(full_df, buffer_start);
        let (full_df, effective_buffer_start, effective_buffer_end) =
            self.clamp_buffer_bytes(full_df, buffer_start);

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
        self.buffered_start_row = effective_buffer_start;
        self.buffered_end_row = effective_buffer_end;
        self.buffered_df = Some(full_df);
    }

    /// Recompute locked_df and df from the cached full buffer. Used when only termcol_index (or locked columns) changed.
    fn slice_buffer_into_display(&mut self) {
        let full_df = match self.buffered_df.as_ref() {
            Some(df) => df,
            None => return,
        };

        if self.locked_columns_count > 0 {
            let locked_names: Vec<&str> = self
                .column_order
                .iter()
                .take(self.locked_columns_count)
                .map(|s| s.as_str())
                .collect();
            if let Ok(locked_df) = full_df.select(locked_names) {
                self.locked_df = if self.is_grouped() {
                    self.format_grouped_dataframe(locked_df).ok()
                } else {
                    Some(locked_df)
                };
            }
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
            if let Ok(scroll_df) = full_df.select(scroll_names) {
                self.df = if self.is_grouped() {
                    self.format_grouped_dataframe(scroll_df).ok()
                } else {
                    Some(scroll_df)
                };
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
                    .amortized_iter()
                    .map(|opt_list| {
                        opt_list.map(|list_series| {
                            let list_series = list_series.as_ref();
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

        Ok(DataFrame::new_infer_height(new_series)?)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn select_next(&mut self) -> bool {
        self.table_state.select_next();
        if let Some(selected) = self.table_state.selected()
            && selected >= self.visible_rows
            && self.visible_rows > 0
        {
            return self.slide_table(1);
        }
        false
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn page_down(&mut self) -> bool {
        self.slide_table(self.visible_rows as i64)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn select_previous(&mut self) -> bool {
        if let Some(selected) = self.table_state.selected() {
            self.table_state.select_previous();
            if selected == 0 && self.start_row > 0 {
                return self.slide_table(-1);
            }
        } else {
            self.table_state.select(Some(0));
        }
        false
    }

    /// Returns true if a buffer collect is needed.
    pub fn scroll_to(&mut self, index: usize) -> bool {
        if self.start_row == index {
            return false;
        }
        self.start_row = index;
        true // caller must collect
    }

    /// Set scroll position for go-to-line (centered). Returns true if a collect is needed.
    pub fn scroll_to_row_centered(&mut self, row_index: usize) -> bool {
        if self.num_rows == 0 || self.visible_rows == 0 {
            return false;
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
            return false;
        }

        self.start_row = start_row;
        let display_idx = row_index
            .saturating_sub(start_row)
            .min(self.visible_rows.saturating_sub(1));
        self.table_state.select(Some(display_idx));
        true // caller must collect
    }

    /// Jump to the first page. Returns true if a collect is needed.
    pub fn scroll_to_start(&mut self) -> bool {
        self.table_state.select(Some(0));
        self.scroll_to(0)
    }

    /// Jump to the last page. Returns true if a collect is needed.
    pub fn scroll_to_end(&mut self) -> bool {
        if self.num_rows == 0 {
            self.start_row = 0;
            self.buffered_start_row = 0;
            self.buffered_end_row = 0;
            return false;
        }
        let end_start = self.num_rows.saturating_sub(self.visible_rows);
        if self.start_row == end_start {
            self.select_last_visible_row();
            return false;
        }
        self.start_row = end_start;
        self.select_last_visible_row();
        true // caller must collect
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

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn half_page_down(&mut self) -> bool {
        let half = (self.visible_rows / 2).max(1) as i64;
        self.slide_table(half)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn half_page_up(&mut self) -> bool {
        if self.start_row == 0 {
            return false;
        }
        let half = (self.visible_rows / 2).max(1) as i64;
        self.slide_table(-half)
    }

    /// Returns true if a buffer collect is needed after the scroll.
    pub fn page_up(&mut self) -> bool {
        if self.start_row == 0 {
            return false;
        }
        self.slide_table(-(self.visible_rows as i64))
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
        self.buffered_df = None;
        self.collect();
    }

    pub fn set_locked_columns(&mut self, count: usize) {
        self.locked_columns_count = count.min(self.column_order.len());
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.collect();
    }

    pub fn locked_columns_count(&self) -> usize {
        self.locked_columns_count
    }

    // Getter methods for template creation
    /// Filters for a template: while drilled into a group these are the grouped view's,
    /// which is what a template reproduces (it cannot express a drill-down).
    pub fn get_filters(&self) -> &[FilterStatement] {
        match &self.grouped {
            Some(view) => &view.filters,
            None => &self.filters,
        }
    }

    pub fn get_sort_columns(&self) -> &[String] {
        match &self.grouped {
            Some(view) => &view.sort_columns,
            None => &self.sort_columns,
        }
    }

    pub fn get_sort_ascending(&self) -> bool {
        match &self.grouped {
            Some(view) => view.sort_ascending,
            None => self.sort_ascending,
        }
    }

    /// Filters applied to the frame on screen (inside the group while drilled). This is
    /// what the Sort & Filter sidebar shows and edits.
    pub fn view_filters(&self) -> &[FilterStatement] {
        &self.filters
    }

    pub fn view_sort_columns(&self) -> &[String] {
        &self.sort_columns
    }

    pub fn view_sort_ascending(&self) -> bool {
        self.sort_ascending
    }

    /// The pivot/melt result in effect, for a snapshot that may need to put it back.
    pub fn reshaped_lf_clone(&self) -> Option<LazyFrame> {
        self.reshaped_lf.clone()
    }

    /// Put back a reshape taken with `reshaped_lf_clone` / `last_pivot_spec` /
    /// `last_melt_spec`, e.g. when a template fails to apply.
    pub fn restore_reshape(
        &mut self,
        lf: Option<LazyFrame>,
        pivot: Option<PivotSpec>,
        melt: Option<MeltSpec>,
    ) {
        self.reshaped_lf = lf;
        self.last_pivot_spec = pivot;
        self.last_melt_spec = melt;
    }

    pub fn get_column_order(&self) -> &[String] {
        &self.column_order
    }

    pub fn get_active_query(&self) -> &str {
        &self.active_query
    }

    pub fn get_active_sql_query(&self) -> &str {
        &self.active_sql_query
    }

    pub fn get_active_fuzzy_query(&self) -> &str {
        &self.active_fuzzy_query
    }

    /// The frame filters and sort are applied to (see `base_lf`).
    pub fn base_lf_clone(&self) -> LazyFrame {
        self.base_lf.clone()
    }

    /// Restore a `base_lf` taken with `base_lf_clone`, e.g. when a template fails to apply.
    pub fn set_base_lf(&mut self, lf: LazyFrame) {
        self.base_lf = lf;
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

    /// Names of binary columns in the source schema. Their values are not read into the display
    /// buffer (the `‹binary›` stub stands in); the renderer uses this to style those cells.
    pub fn binary_column_names(&self) -> std::collections::HashSet<String> {
        self.schema
            .iter()
            .filter(|(_, dtype)| matches!(dtype, DataType::Binary))
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

    /// Current scrollable display buffer. None until first collect().
    pub fn display_df(&self) -> Option<&DataFrame> {
        self.df.as_ref()
    }

    /// Visible-window slice of the display buffer (same as passed to render_dataframe).
    pub fn display_slice_df(&self) -> Option<DataFrame> {
        let df = self.df.as_ref()?;
        let offset = self.start_row.saturating_sub(self.buffered_start_row);
        let slice_len = self.visible_rows.min(df.height().saturating_sub(offset));
        if offset < df.height() && slice_len > 0 {
            Some(df.slice(offset as i64, slice_len))
        } else {
            None
        }
    }

    /// The drift group of each row on screen, when the dataset's files differ.
    ///
    /// Empty once a query or reshape has replaced the frame: those rows stand for no
    /// file, so their nulls are ordinary nulls. The window is a screen tall, so this
    /// is a few dozen values.
    pub fn display_drift(&self) -> Vec<u32> {
        if !self.drift_column_present {
            return Vec::new();
        }
        let Some(df) = self.buffered_df.as_ref() else {
            return Vec::new();
        };
        let Ok(column) = df.column(crate::schema_union::DRIFT_COLUMN) else {
            return Vec::new();
        };
        let offset = self.start_row.saturating_sub(self.buffered_start_row);
        let len = self.visible_rows.min(column.len().saturating_sub(offset));
        if len == 0 {
            return Vec::new();
        }
        let slice = column.slice(offset as i64, len);
        let Ok(rows) = slice.u32() else {
            return Vec::new();
        };
        // The column holds each row's place in the dataset. The file it came from is
        // the last one starting at or before it, and the file says what it is missing.
        let starts = &self.drift_file_starts;
        let groups = &self.drift_file_group;
        rows.iter()
            .map(|row| {
                let row = row.unwrap_or(0) as usize;
                let file = starts
                    .partition_point(|&start| start <= row)
                    .saturating_sub(1);
                groups.get(file).copied().unwrap_or(0)
            })
            .collect()
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

        let grouped_df = collect_lazy(self.visible_lf(), self.polars_streaming)?;

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

        let group_df = DataFrame::new_infer_height(columns)?;

        // The group becomes the pipeline root while drilled in, so a sidebar filter or
        // sort applies within it instead of rebuilding the grouped view underneath.
        self.grouped = Some(GroupedView {
            lf: self.lf.clone(),
            base_lf: self.base_lf.clone(),
            filters: std::mem::take(&mut self.filters),
            sort_columns: std::mem::take(&mut self.sort_columns),
            sort_ascending: self.sort_ascending,
            drift: self.drift_column_present,
            drift_groups: self.drift_groups.clone(),
            notes: self.notes.clone(),
        });
        self.sort_ascending = true;
        let lf = group_df.lazy();
        let schema = lf.clone().collect_schema()?;
        self.install_base(lf, schema);
        self.drilled_down_group_index = Some(group_index);
        self.start_row = 0;
        self.termcol_index = 0;
        self.locked_columns_count = 0;
        self.table_state.select(Some(0));
        self.collect();

        Ok(())
    }

    pub fn drill_up(&mut self) -> Result<()> {
        match self.grouped.take() {
            Some(view) => {
                self.invalidate_num_rows();
                self.lf = view.lf;
                self.base_lf = view.base_lf;
                self.filters = view.filters;
                self.sort_columns = view.sort_columns;
                self.sort_ascending = view.sort_ascending;
                self.drift_column_present = view.drift;
                self.drift_groups = view.drift_groups;
                self.notes = view.notes;
                self.schema = self.visible_lf().collect_schema()?;
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
            }
            _ => Err(color_eyre::eyre::eyre!("Not in drill-down mode")),
        }
    }

    pub fn get_analysis_dataframe(&self) -> Result<DataFrame> {
        Ok(collect_lazy(self.visible_lf(), self.polars_streaming)?)
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
    /// The lazy pivot needs the new column set before it runs, so one distinct pass on the
    /// pivot column comes first, sorted so the new columns come out alphabetical with a
    /// trailing `null` column, as the eager pivot ordered them. Index rows keep first-seen
    /// order.
    pub fn pivot(&mut self, spec: &PivotSpec) -> Result<()> {
        let on = spec.pivot_column.as_str();
        let value = spec.value_column.as_str();
        let on_columns = collect_lazy(
            self.lf
                .clone()
                .select([col(on)])
                .unique(None, UniqueKeepStrategy::Any)
                .sort([on], SortMultipleOptions::default().with_nulls_last(true)),
            self.polars_streaming,
        )?;
        // Names are literal: a header may contain `*` or `^`, so no pattern expansion.
        let index = if spec.index.is_empty() {
            all() - by_name([on, value], true, false)
        } else {
            by_name(spec.index.iter().map(String::as_str), true, false)
        };
        let pivoted = self.visible_lf().pivot(
            by_name([on], true, false),
            Arc::new(on_columns),
            index,
            by_name([value], true, false),
            pivot_agg_expr(spec.aggregation)?,
            true,
            PlSmallStr::from_static("_"),
            PivotColumnNaming::Auto,
        );

        self.last_pivot_spec = Some(spec.clone());
        self.last_melt_spec = None;
        self.replace_lf_after_reshape(pivoted)?;
        Ok(())
    }

    /// Melt the current `LazyFrame` (wide → long). Never uses `original_lf`.
    pub fn melt(&mut self, spec: &MeltSpec) -> Result<()> {
        let on = cols(spec.value_columns.iter().map(|s| s.as_str()));
        let index = cols(spec.index.iter().map(|s| s.as_str()));
        let args = UnpivotArgsDSL {
            on: Some(on),
            index,
            variable_name: Some(PlSmallStr::from(spec.variable_name.as_str())),
            value_name: Some(PlSmallStr::from(spec.value_name.as_str())),
        };
        let lf = self.visible_lf().unpivot(args);
        self.last_melt_spec = Some(spec.clone());
        self.last_pivot_spec = None;
        self.replace_lf_after_reshape(lf)?;
        Ok(())
    }

    fn replace_lf_after_reshape(&mut self, lf: LazyFrame) -> Result<()> {
        let schema = lf.clone().collect_schema()?;
        self.reshaped_lf = Some(lf.clone());
        self.install_base(lf, schema);
        self.reset_view_state(0);
        self.error = None;
        self.df = None;
        self.locked_df = None;
        self.collect();
        Ok(())
    }

    pub fn is_drilled_down(&self) -> bool {
        self.drilled_down_group_index.is_some()
    }

    /// Rebuild `lf` as `base_lf` → filters → sort. Column order is applied at collect.
    fn apply_transformations(&mut self) {
        let mut lf = self.base_lf.clone();
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
            lf = lf.sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                sort_options(self.sort_columns.len(), !self.sort_ascending),
            );
        } else if !self.sort_ascending {
            lf = lf.reverse();
        }

        self.invalidate_num_rows();
        self.lf = lf;
        self.restore_footer_count();
        self.collect();
    }

    pub fn sort(&mut self, columns: Vec<String>, ascending: bool) {
        self.sort_columns = columns;
        self.sort_ascending = ascending;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.apply_transformations();
    }

    pub fn reverse(&mut self) {
        self.sort_ascending = !self.sort_ascending;

        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;

        if !self.sort_columns.is_empty() {
            self.invalidate_num_rows();
            self.lf = self.lf.clone().sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                sort_options(self.sort_columns.len(), !self.sort_ascending),
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
        // A new result set, viewed from the top: a position deep in the old one would
        // plan a slice past a smaller result, which reads nothing.
        self.start_row = 0;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.apply_transformations();
    }

    pub fn query(&mut self, query: String) {
        self.error = None;

        let trimmed_query = query.trim();
        if trimmed_query.is_empty() {
            self.reset_lf_to_original();
            self.collect();
            return;
        }

        match parse_query(&query) {
            Ok((cols, filter, group_by_cols, group_by_col_names)) => {
                let mut lf = self.query_source();
                let mut schema_opt: Option<Arc<Schema>> = None;

                // Apply filter first (where clause)
                if let Some(f) = filter {
                    lf = lf.filter(f);
                }

                if !group_by_cols.is_empty() {
                    if !cols.is_empty() {
                        lf = lf.group_by(group_by_cols.clone()).agg(cols);
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
                    }
                    // Sort by the result's group-key column names (first N columns after agg).
                    // Works for aliased or plain names without relying on parser-derived names.
                    let schema = match lf.collect_schema() {
                        Ok(s) => s,
                        Err(e) => {
                            self.error = Some(e);
                            return;
                        }
                    };
                    schema_opt = Some(schema.clone());
                    let sort_exprs: Vec<Expr> = schema
                        .iter_names()
                        .take(group_by_cols.len())
                        .map(|n| col(n.as_str()))
                        .collect();
                    let options = sort_options(sort_exprs.len(), false);
                    lf = lf.sort_by_exprs(sort_exprs, options);
                } else if !cols.is_empty() {
                    lf = lf.select(cols);
                }

                let schema = match schema_opt {
                    Some(s) => s,
                    None => match lf.collect_schema() {
                        Ok(s) => s,
                        Err(e) => {
                            self.error = Some(e);
                            return;
                        }
                    },
                };

                // Group columns come first in the result; lock that leading run.
                let locked = schema
                    .iter_names()
                    .take_while(|c| group_by_col_names.iter().any(|g| g.as_str() == c.as_str()))
                    .count();
                self.install_query_result(lf, schema, ActiveQuery::Dsl(query), locked);
                self.forget_reshape();
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

    /// The data a query runs against: the drilled group while drilled into one, else the
    /// pivot/melt result while one is in effect, otherwise the data as loaded. Never the
    /// sidebar filters or sort, which go on top, and never a previous SQL result.
    pub fn query_root(&self) -> LazyFrame {
        if self.grouped.is_some() {
            // While drilled, `base_lf` is the group (see `drill_down_into_group`).
            return self.base_lf.clone();
        }
        Self::without_drift(
            self.reshaped_lf
                .clone()
                .unwrap_or_else(|| self.original_lf.clone()),
        )
    }

    /// Execute a SQL query against `query_root` (registered as table "df"): the drilled
    /// group or the reshaped data when one is in effect, otherwise the data as loaded —
    /// never the sidebar filters or a previous SQL result. Sidebar filters and sort are
    /// not baked into the result, they go on top of it. Empty SQL resets to original
    /// state. Does not call collect(); the event loop does that via AppEvent::Collect.
    pub fn sql_query(&mut self, sql: String) {
        self.error = None;
        let trimmed = sql.trim();
        if trimmed.is_empty() {
            self.reset_lf_to_original();
            return;
        }

        #[cfg(feature = "sql")]
        {
            use polars_sql::SQLContext;
            let mut ctx = SQLContext::new();
            ctx.register("df", self.query_root());
            match ctx.execute(trimmed) {
                Ok(result_lf) => {
                    let schema = match result_lf.clone().collect_schema() {
                        Ok(s) => s,
                        Err(e) => {
                            self.error = Some(e);
                            return;
                        }
                    };
                    self.install_query_result(result_lf, schema, ActiveQuery::Sql(sql), 0);
                }
                Err(e) => {
                    self.error = Some(e);
                }
            }
        }

        #[cfg(not(feature = "sql"))]
        {
            self.error = Some(PolarsError::ComputeError(
                format!("SQL support not compiled in (build with --features sql)").into(),
            ));
        }
    }

    /// Fuzzy search: filter rows where any string column matches the query.
    /// Query is split on whitespace; each token must match (in order, case-insensitive) in some string column.
    /// Empty query resets to original_lf.
    pub fn fuzzy_search(&mut self, query: String) {
        self.error = None;
        let trimmed = query.trim();
        if trimmed.is_empty() {
            self.reset_lf_to_original();
            self.collect();
            return;
        }
        // The search runs over the data as loaded, so its columns come from there too,
        // not from a DSL query's possibly renamed schema.
        let schema = match self.query_source().collect_schema() {
            Ok(schema) => schema,
            Err(e) => {
                self.error = Some(e);
                return;
            }
        };
        let string_cols: Vec<String> = schema
            .iter()
            .filter(|(_, dtype)| dtype.is_string())
            .map(|(name, _)| name.to_string())
            .collect();
        if string_cols.is_empty() {
            self.error = Some(PolarsError::ComputeError(
                "Fuzzy search requires at least one string column".into(),
            ));
            return;
        }
        let tokens: Vec<&str> = trimmed
            .split_whitespace()
            .filter(|s| !s.is_empty())
            .collect();
        let token_exprs: Vec<Expr> = tokens
            .iter()
            .map(|token| {
                let pattern = fuzzy_token_regex(token);
                string_cols
                    .iter()
                    .map(|c| col(c.as_str()).str().contains(lit(pattern.as_str()), false))
                    .reduce(|a, b| a.or(b))
                    .unwrap()
            })
            .collect();
        let combined = token_exprs.into_iter().reduce(|a, b| a.and(b)).unwrap();
        let lf = self.query_source().filter(combined);
        self.install_query_result(lf, schema, ActiveQuery::Fuzzy(query), 0);
        self.forget_reshape();
        self.collect();
    }
}

/// Case-insensitive regex for one token: chars in order with `.*` between.
pub(crate) fn fuzzy_token_regex(token: &str) -> String {
    let inner: String =
        token
            .chars()
            .map(|c| regex::escape(&c.to_string()))
            .fold(String::new(), |mut s, e| {
                if !s.is_empty() {
                    s.push_str(".*");
                }
                s.push_str(&e);
                s
            });
    format!("(?i).*{}.*", inner)
}

pub struct DataTable {
    pub header_bg: Color,
    pub header_fg: Color,
    pub row_numbers_fg: Color,
    pub separator_fg: Color,
    pub table_cell_padding: u16,
    pub alternate_row_bg: Option<Color>,
    /// When true, colorize cells by column type using the optional colors below.
    pub column_colors: bool,
    pub str_col: Option<Color>,
    pub int_col: Option<Color>,
    pub float_col: Option<Color>,
    pub bool_col: Option<Color>,
    pub temporal_col: Option<Color>,
    /// Color for binary-column placeholder cells (the `‹binary›` stub). Applied with italic,
    /// independent of `column_colors`, so stubs always read as "placeholder, not data".
    pub binary_col: Option<Color>,
    /// Names of columns that are binary in the source schema. Their cells hold the `‹binary›`
    /// stub (see [`BINARY_STUB`]) and are styled with `binary_col` + italic.
    pub binary_cols: std::collections::HashSet<String>,
    /// Display-time number formatting (digit grouping, separators, alignment).
    pub number_format: NumberFormatSettings,
    /// Draw a second header row naming each column's type.
    pub dtype_row: bool,
    /// Tint under the row the cursor is on. `None` falls back to reversed video.
    pub selected_bg: Option<Color>,
    /// The rail beside the selected row and the off-screen column hints.
    pub accent: Color,
    /// Null cells and the type row.
    pub dimmed: Color,
    /// Per row on screen, its file's drift group. Empty when the dataset's files agree,
    /// or when the rows no longer stand for rows of a file.
    pub drift_rows: Vec<u32>,
    /// What each drift group is missing. Indexed by the values in `drift_rows`.
    pub drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
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
            column_colors: false,
            str_col: None,
            int_col: None,
            float_col: None,
            bool_col: None,
            temporal_col: None,
            binary_col: None,
            binary_cols: std::collections::HashSet::new(),
            number_format: NumberFormatSettings::default(),
            dtype_row: false,
            selected_bg: None,
            accent: Color::Cyan,
            dimmed: Color::DarkGray,
            drift_rows: Vec::new(),
            drift_groups: Arc::new(Vec::new()),
        }
    }
}

/// The frame that counts `lf`'s rows.
///
/// `len()` is `UInt32`, and summing it over the union a many-file scan builds widens to
/// `UInt128`, which Polars 0.55 cannot reduce: the in-memory engine errors and the
/// streaming one panics, taking the whole app with it. Counting in `UInt64` stays
/// inside what both engines implement.
pub(crate) fn row_count_lf(lf: &LazyFrame) -> LazyFrame {
    lf.clone().select([len().cast(DataType::UInt64)])
}

/// The short name of a column's type, as the type row and the schema pane spell it.
///
/// Polars' own `Display` says `Datetime(Microseconds, None)`; the row under the header
/// has room for one word.
pub fn dtype_label(dtype: &DataType) -> String {
    match dtype {
        DataType::String => "str".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Int8 => "i8".to_string(),
        DataType::Int16 => "i16".to_string(),
        DataType::Int32 => "i32".to_string(),
        DataType::Int64 => "i64".to_string(),
        DataType::UInt8 => "u8".to_string(),
        DataType::UInt16 => "u16".to_string(),
        DataType::UInt32 => "u32".to_string(),
        DataType::UInt64 => "u64".to_string(),
        DataType::Float32 => "f32".to_string(),
        DataType::Float64 => "f64".to_string(),
        DataType::Date => "date".to_string(),
        DataType::Datetime(_, _) => "datetime".to_string(),
        DataType::Time => "time".to_string(),
        DataType::Duration(_) => "duration".to_string(),
        DataType::Binary => "binary".to_string(),
        DataType::Null => "null".to_string(),
        DataType::List(inner) => format!("list[{}]", dtype_label(inner)),
        DataType::Struct(_) => "struct".to_string(),
        other if other.is_categorical() => "cat".to_string(),
        other if other.is_enum() => "enum".to_string(),
        other if other.is_decimal() => "decimal".to_string(),
        other => other.to_string().to_ascii_lowercase(),
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

/// Placeholder shown in the table for binary columns. Their values (often large blobs, e.g.
/// raw document bytes) are never read into the display buffer — only this stub is — which keeps
/// scrolling and jump-to-end fast. The real bytes remain in `lf` for export/analysis.
const BINARY_STUB: &str = "‹binary›";

/// Whether a column whose value doesn't fully fit may be shown truncated. Textual columns
/// (strings, raw bytes, categorical/enum labels) are fine to clip — a partial value still reads
/// as a clipped string. Numeric, temporal and boolean columns are excluded: a truncated number or
/// timestamp reads as a different (wrong) value, so those are dropped until scrolled into view.
fn is_truncatable_dtype(dtype: &DataType) -> bool {
    match dtype {
        DataType::String | DataType::Binary => true,
        other => other.is_categorical() || other.is_enum(),
    }
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

    /// Enable column-type coloring and set colors for string, int, float, bool, and temporal columns.
    pub fn with_column_type_colors(
        mut self,
        str_col: Color,
        int_col: Color,
        float_col: Color,
        bool_col: Color,
        temporal_col: Color,
    ) -> Self {
        self.column_colors = true;
        self.str_col = Some(str_col);
        self.int_col = Some(int_col);
        self.float_col = Some(float_col);
        self.bool_col = Some(bool_col);
        self.temporal_col = Some(temporal_col);
        self
    }

    /// Set the color used for binary-column placeholder cells.
    pub fn with_binary_col(mut self, color: Color) -> Self {
        self.binary_col = Some(color);
        self
    }

    /// Set the names of binary columns, whose cells render the `‹binary›` stub.
    pub fn with_binary_columns(mut self, names: std::collections::HashSet<String>) -> Self {
        self.binary_cols = names;
        self
    }

    /// Set display-time number formatting (digit grouping and alignment).
    pub fn with_number_format(mut self, settings: NumberFormatSettings) -> Self {
        self.number_format = settings;
        self
    }

    /// Show or hide the second header row of column types.
    pub fn with_dtype_row(mut self, on: bool) -> Self {
        self.dtype_row = on;
        self
    }

    /// Tell the table which rows came from files missing which columns, so a cell the
    /// file never had draws differently from a null the data holds.
    pub fn with_drift(
        mut self,
        rows: Vec<u32>,
        groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    ) -> Self {
        self.drift_rows = rows;
        self.drift_groups = groups;
        self
    }

    /// The footnote mark after a column's name, when it is not in every file or the
    /// files disagree on its type. Empty otherwise.
    fn drift_mark_for(&self, column: &str, drifting: &HashSet<&str>) -> &'static str {
        if drifting.contains(column) {
            crate::glyphs::get().drift_mark
        } else {
            ""
        }
    }

    /// Every column some file is missing, gathered once a frame. Most columns are in
    /// every file, and this keeps them to one hash lookup rather than a walk of every
    /// group's lists.
    fn drifting_columns(&self) -> HashSet<&str> {
        self.drift_groups
            .iter()
            .flat_map(|group| group.absent.iter().chain(group.unread.iter()))
            .map(|name| name.as_str())
            .collect()
    }

    /// What a null in `column` draws as, per drift group: the plain null glyph, the
    /// absent glyph for a group whose files never had the column, or the conflict
    /// glyph for one whose files hold it in another type. Empty when nothing drifts.
    fn null_glyphs_for(
        &self,
        column: &str,
        g: &'static crate::glyphs::Glyphs,
        drifting: &HashSet<&str>,
    ) -> Vec<&'static str> {
        if self.drift_rows.is_empty() || !drifting.contains(column) {
            return Vec::new();
        }
        self.drift_groups
            .iter()
            .map(|group| {
                if group.absent.iter().any(|c| c == column) {
                    g.absent
                } else if group.unread.iter().any(|c| c == column) {
                    g.conflict
                } else {
                    g.null
                }
            })
            .collect()
    }

    /// The tint under the selected row, the rail colour, and the dim colour for nulls.
    pub fn with_selection_colors(
        mut self,
        selected_bg: Option<Color>,
        accent: Color,
        dimmed: Color,
    ) -> Self {
        self.selected_bg = selected_bg;
        self.accent = accent;
        self.dimmed = dimmed;
        self
    }

    /// How many rows the header takes: the names, plus the type row when it is on.
    pub fn header_height(&self) -> u16 {
        if self.dtype_row { 2 } else { 1 }
    }

    /// Style of the highlighted row: a tint when the theme gives one, else reversed video.
    fn highlight_style(&self) -> Style {
        match self.selected_bg {
            Some(bg) => Style::default().bg(bg),
            None => Style::default().add_modifier(Modifier::REVERSED),
        }
    }

    /// Return the color for a column dtype when column_colors is enabled.
    fn column_type_color(&self, dtype: &DataType) -> Option<Color> {
        if !self.column_colors {
            return None;
        }
        match dtype {
            DataType::String => self.str_col,
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => self.int_col,
            DataType::Float32 | DataType::Float64 => self.float_col,
            DataType::Boolean => self.bool_col,
            DataType::Date | DataType::Datetime(_, _) | DataType::Time | DataType::Duration(_) => {
                self.temporal_col
            }
            _ => None,
        }
    }

    /// Render the dataframe into `area`, returning the number of columns that were actually
    /// shown (which may be fewer than `df`'s column count when they don't all fit). The caller
    /// uses this to decide whether to draw an "more columns off-screen" indicator.
    fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        _row_numbers: bool,
        _start_row_offset: usize,
    ) -> usize {
        // make each column as wide as it needs to be to fit the content
        let (height, cols) = df.shape();

        let header_h = self.header_height();
        // The type row is part of the header, so a column is at least as wide as its
        // type name; "datetime" under a column called "ts" would otherwise clip.
        let dtype_labels: Vec<String> = if self.dtype_row {
            df.dtypes().iter().map(dtype_label).collect()
        } else {
            Vec::new()
        };

        let drifting = self.drifting_columns();

        // widths starts at the length of each column name
        let mut widths: Vec<u16> = df
            .get_column_names()
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let mark_w = self
                    .drift_mark_for(name.as_str(), &drifting)
                    .chars()
                    .count() as u16;
                let name_w = name.chars().count() as u16 + mark_w;
                let type_w = dtype_labels
                    .get(i)
                    .map(|l| l.chars().count() as u16)
                    .unwrap_or(0);
                name_w.max(type_w)
            })
            .collect();

        let mut used_width = 0;

        // rows is a vector initialized to a vector of lenth "height" empty rows
        let mut rows: Vec<Vec<Cell>> = vec![vec![]; height];
        let mut visible_columns = 0;

        let max_rows = height.min((area.height as usize).saturating_sub(header_h as usize));
        let g = crate::glyphs::get();
        // A null is drawn as a glyph in the dim colour, so it can never be mistaken
        // for an empty string or a zero that happens to be blank.
        let null_style = Style::default()
            .fg(self.dimmed)
            .add_modifier(Modifier::ITALIC);

        // Reused across every cell in the frame so formatting allocates only
        // the destination string each cell already needs.
        let mut scratch = String::new();
        // Headers follow their column's alignment; a left-aligned heading over
        // right-aligned digits reads as a rendering bug.
        let mut right_aligned_cols: Vec<bool> = vec![false; cols];

        let col_names = df.get_column_names();
        for col_index in 0..cols {
            let mut max_len = widths[col_index];
            let col_data = &df[col_index];
            // Binary columns hold the `‹binary›` stub: style them with binary_col + italic so they
            // read as a placeholder rather than data, regardless of the column_colors setting.
            let is_binary = self.binary_cols.contains(col_names[col_index].as_str());
            let cell_style = if is_binary {
                let mut s = Style::default().add_modifier(Modifier::ITALIC);
                if let Some(c) = self.binary_col {
                    s = s.fg(c);
                }
                Some(s)
            } else {
                self.column_type_color(col_data.dtype())
                    .map(|c| Style::default().fg(c))
            };

            // Resolved once per column: dtype eligibility and the include/exclude
            // globs never touch the per-cell path. A binary column holds the
            // `‹binary›` stub, not a number, so it is always passthrough.
            let col_fmt = if is_binary {
                CellFormatter::Passthrough
            } else {
                self.number_format
                    .formatter_for(col_names[col_index].as_str(), col_data.dtype())
            };
            // Numeric columns render flush-right so magnitudes line up; strings,
            // booleans, temporals and binary stubs stay left.
            let right_align = self.number_format.align_numeric_right
                && !is_binary
                && numfmt::is_right_aligned_dtype(col_data.dtype());
            right_aligned_cols[col_index] = right_align;

            // A null in this column means different things in different files: the
            // data's own null, a file written without the column, or a file that
            // stores it in another type. Resolved once per column, by group.
            let null_glyph_by_group =
                self.null_glyphs_for(col_names[col_index].as_str(), g, &drifting);

            for (row_index, row) in rows.iter_mut().take(max_rows).enumerate() {
                let value = col_data.get(row_index).unwrap();
                if matches!(value, AnyValue::Null) {
                    let glyph = self
                        .drift_rows
                        .get(row_index)
                        .and_then(|group| null_glyph_by_group.get(*group as usize))
                        .copied()
                        .unwrap_or(g.null);
                    max_len = max_len.max(glyph.chars().count() as u16);
                    let line = Line::from(Span::styled(glyph, null_style));
                    row.push(Cell::from(if right_align {
                        line.right_aligned()
                    } else {
                        line
                    }));
                    continue;
                }
                let val_str: Cow<str> = numfmt::format_any_value(&col_fmt, &value, &mut scratch);
                let len = val_str.chars().count() as u16;
                max_len = max_len.max(len);
                let line = match cell_style {
                    Some(s) => Line::from(Span::styled(val_str.into_owned(), s)),
                    None => Line::from(val_str.into_owned()),
                };
                row.push(Cell::from(if right_align {
                    line.right_aligned()
                } else {
                    line
                }));
            }

            // Use > not >= so the last column is shown when it fits exactly (no padding needed after it)
            let overflows = (used_width + max_len) > area.width;

            if !overflows {
                visible_columns += 1;
                widths[col_index] = max_len;
                used_width += max_len + self.table_cell_padding;
            } else {
                // The column doesn't fully fit. A string column is shown truncated to the
                // remaining width so the horizontal space is used and (most importantly) its
                // heading is visible — as long as there's enough room to be meaningful. Numeric
                // and temporal columns are NOT truncated: a partial value reads as a wrong value,
                // so they're left for the next scroll (the off-screen indicator still flags them).
                // Either way nothing past this column can fit, so stop here.
                const MIN_PARTIAL_COLUMN_WIDTH: u16 = 3;
                let remaining = area.width.saturating_sub(used_width);
                if is_truncatable_dtype(col_data.dtype()) && remaining >= MIN_PARTIAL_COLUMN_WIDTH {
                    visible_columns += 1;
                    widths[col_index] = remaining;
                }
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
        // The name takes the column's own colour, bold, so the header says what the
        // cells say without a mark in front of it; the type row beneath repeats the
        // colour in plain weight and spells the type out.
        let dtypes = df.dtypes();
        let headers: Vec<Cell> = df
            .get_column_names()
            .iter()
            .take(visible_columns)
            .enumerate()
            .map(|(i, name)| {
                let is_binary = self.binary_cols.contains(name.as_str());
                let colour = if is_binary {
                    self.binary_col
                } else {
                    self.column_type_color(&dtypes[i])
                };
                let name_style = match colour {
                    Some(c) => Style::default().fg(c).add_modifier(Modifier::BOLD),
                    None => Style::default().add_modifier(Modifier::BOLD),
                };
                let mut heading = vec![Span::styled(name.to_string(), name_style)];
                let mark = self.drift_mark_for(name.as_str(), &drifting);
                if !mark.is_empty() {
                    heading.push(Span::styled(mark, Style::default().fg(self.dimmed)));
                }
                let mut lines = vec![Line::from(heading)];
                if self.dtype_row {
                    let type_style = match colour {
                        Some(c) => Style::default().fg(c),
                        None => Style::default().fg(self.dimmed),
                    };
                    lines.push(Line::from(Span::styled(
                        dtype_labels.get(i).cloned().unwrap_or_default(),
                        type_style,
                    )));
                }
                let text = if right_aligned_cols[i] {
                    Text::from(lines).right_aligned()
                } else {
                    Text::from(lines)
                };
                Cell::from(text)
            })
            .collect();

        StatefulWidget::render(
            Table::new(rows, widths)
                .column_spacing(self.table_cell_padding)
                .header(Row::new(headers).style(header_row_style).height(header_h))
                .row_highlight_style(self.highlight_style()),
            area,
            buf,
            state,
        );

        visible_columns
    }

    fn render_row_numbers(&self, area: Rect, buf: &mut Buffer, params: RowNumbersParams) {
        // Header row: same style as the rest of the column headers (fill full width so color matches)
        let header_style = if self.header_bg == Color::Reset {
            Style::default().fg(self.header_fg)
        } else {
            Style::default().bg(self.header_bg).fg(self.header_fg)
        };
        let header_h = self.header_height().min(area.height);
        let header_fill = " ".repeat(area.width as usize);
        for dy in 0..header_h {
            Paragraph::new(header_fill.clone())
                .style(header_style)
                .render(
                    Rect {
                        x: area.x,
                        y: area.y + dy,
                        width: area.width,
                        height: 1,
                    },
                    buf,
                );
        }

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
        for row_idx in 0..rows_to_render.min(area.height.saturating_sub(header_h) as usize) {
            let row_num = params.start_row + row_idx + params.row_start_index;
            let row_num_text = row_num.to_string();

            // Right-align row numbers within the available width
            let padding = max_width.saturating_sub(row_num_text.len());
            let padded_text = format!("{}{}", " ".repeat(padding), row_num_text);

            // Match main table background: default when row is even (or no alternate);
            // when alternate_row_bg is set, odd rows use that background. The selected
            // row carries the same tint as the table's own highlight.
            let is_selected = params.selected_row == Some(row_idx);
            let (fg, bg) = if is_selected {
                (
                    Color::Reset,
                    self.selected_bg
                        .or(self.alternate_row_bg.filter(|_| row_idx % 2 == 1)),
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

            let y = area.y + row_idx as u16 + header_h;
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
        // One column on the left is the rail: blank on every row but the one the
        // cursor is on, where it carries the accent. It also holds the "columns off to
        // the left" hint in the header, so no header name ever gets a character
        // overwritten.
        let rail_area = Rect {
            x: area.x,
            y: area.y,
            width: 1.min(area.width),
            height: area.height,
        };
        let area = Rect {
            x: area.x.saturating_add(1),
            y: area.y,
            width: area.width.saturating_sub(1),
            height: area.height,
        };
        let header_h = self.header_height();
        state.visible_termcols = area.width as usize;
        let new_visible_rows = (area.height as usize).saturating_sub(header_h as usize);
        let visible_rows_changed = new_visible_rows != state.visible_rows;
        state.visible_rows = new_visible_rows;

        if let Some(selected) = state.table_state.selected()
            && selected >= state.visible_rows
            && state.visible_rows > 0
        {
            state.table_state.select(Some(state.visible_rows - 1))
        }

        if visible_rows_changed {
            // Flag that the buffer needs re-collection for the new visible_rows.
            // The App event loop checks this flag after each render and triggers an async collect.
            state.needs_recollect = true;
        }

        // Only show errors in main view if not suppressed (e.g., when query input is active)
        // Query errors should only be shown in the query input frame
        if let Some(error) = state.error.as_ref()
            && !state.suppress_error_display
        {
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

        // Captures the scrollable area plus whether columns exist off-screen to the left/right,
        // so a header-row indicator can be drawn after the table is rendered.
        // Tuple: (scrollable_area, more_columns_left, columns_hidden_to_the_right).
        let mut scroll_indicator: Option<(Rect, bool, usize)> = None;

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
            // This pass only needs widths, so integers take numfmt's arithmetic
            // path instead of building a string per cell and throwing it away.
            let mut scratch = String::new();
            for col_index in 0..cols {
                let col_name = locked_df.get_column_names()[col_index];
                let mut max_len = col_name.chars().count() as u16;
                let col_data = &locked_df[col_index];
                let col_fmt = if self.binary_cols.contains(col_name.as_str()) {
                    CellFormatter::Passthrough
                } else {
                    self.number_format
                        .formatter_for(col_name.as_str(), col_data.dtype())
                };
                for row_index in 0..locked_df.height().min(state.visible_rows) {
                    let value = col_data.get(row_index).unwrap();
                    let len = numfmt::display_width(&col_fmt, &value, &mut scratch) as u16;
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
                    let total_cols = sliced_df.width();
                    let shown = self.render_dataframe(
                        &sliced_df,
                        adjusted_scrollable_area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                    scroll_indicator = Some((
                        adjusted_scrollable_area,
                        state.termcol_index > 0,
                        total_cols.saturating_sub(shown),
                    ));
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
                    let total_cols = sliced_df.width();
                    let shown = self.render_dataframe(
                        &sliced_df,
                        data_area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                    scroll_indicator = Some((
                        data_area,
                        state.termcol_index > 0,
                        total_cols.saturating_sub(shown),
                    ));
                }
            } else {
                // Slice buffer to visible portion
                let offset = state.start_row.saturating_sub(state.buffered_start_row);
                let slice_len = state.visible_rows.min(df.height().saturating_sub(offset));
                if offset < df.height() && slice_len > 0 {
                    let sliced_df = df.slice(offset as i64, slice_len);
                    let total_cols = sliced_df.width();
                    let shown = self.render_dataframe(
                        &sliced_df,
                        area,
                        buf,
                        &mut state.table_state,
                        false,
                        state.start_row,
                    );
                    scroll_indicator = Some((
                        area,
                        state.termcol_index > 0,
                        total_cols.saturating_sub(shown),
                    ));
                }
            }
        } else if !state.column_order.is_empty() {
            // Empty result (0 rows) but we have a schema - show empty table with header, no rows
            let empty_columns: Vec<_> = state
                .column_order
                .iter()
                .map(|name| Series::new(name.as_str().into(), Vec::<String>::new()).into())
                .collect();
            match DataFrame::new_infer_height(empty_columns) {
                Ok(empty_df) => {
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
                        self.render_dataframe(
                            &empty_df,
                            area,
                            buf,
                            &mut state.table_state,
                            false,
                            0,
                        );
                    }
                }
                _ => {
                    Paragraph::new("No data").render(area, buf);
                }
            }
        } else {
            // Truly empty: no schema, not loaded, or blank file
            Paragraph::new("No data").render(area, buf);
        }

        // The rail: the header rows take the header fill so the bar runs edge to edge,
        // and the selected row gets the accent mark.
        if rail_area.width > 0 && rail_area.height > 0 {
            let g = crate::glyphs::get();
            let header_style = if self.header_bg == Color::Reset {
                Style::default().fg(self.header_fg)
            } else {
                Style::default().bg(self.header_bg).fg(self.header_fg)
            };
            for dy in 0..header_h.min(rail_area.height) {
                let cell = &mut buf[(rail_area.x, rail_area.y + dy)];
                cell.set_char(' ');
                cell.set_style(header_style);
            }
            if state.df.is_some()
                && let Some(sel) = state.table_state.selected()
            {
                let y = rail_area.y + header_h + sel as u16;
                if y < rail_area.y + rail_area.height {
                    let cell = &mut buf[(rail_area.x, y)];
                    cell.set_symbol(g.rail.trim_end());
                    let mut style = Style::default()
                        .fg(self.accent)
                        .add_modifier(Modifier::BOLD);
                    if let Some(bg) = self.selected_bg {
                        style = style.bg(bg);
                    }
                    cell.set_style(style);
                }
            }
        }

        // Hints that more columns exist off-screen. The left one sits in the rail,
        // where nothing else lives. The right one says how many are hidden, and goes
        // on the type row when that row is on (its short labels leave room), else on
        // the name row, right-aligned into the slack after the last column.
        if let Some((scroll_area, more_left, hidden)) = scroll_indicator
            && scroll_area.width > 0
            && scroll_area.height > 0
        {
            let g = crate::glyphs::get();
            let more_right = hidden > 0;
            let hint_style = if self.header_bg == Color::Reset {
                Style::default()
                    .fg(self.accent)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
                    .bg(self.header_bg)
                    .fg(self.accent)
                    .add_modifier(Modifier::BOLD)
            };
            if more_left && rail_area.width > 0 {
                let cell = &mut buf[(rail_area.x, rail_area.y)];
                cell.set_symbol(g.arrow_left);
                cell.set_style(hint_style);
            }
            if more_right {
                // The count when there is room for it, the arrow alone when not.
                let mut text = format!(" +{hidden} {}", g.arrow_right);
                if scroll_area.width <= text.chars().count() as u16 {
                    text = g.arrow_right.to_string();
                }
                let w = text.chars().count() as u16;
                if scroll_area.width >= w {
                    let x0 = scroll_area.x + scroll_area.width - w;
                    let y = if header_h > 1 {
                        scroll_area.y + 1
                    } else {
                        scroll_area.y
                    };
                    for (i, ch) in text.chars().enumerate() {
                        let cell = &mut buf[(x0 + i as u16, y)];
                        cell.set_char(ch);
                        cell.set_style(hint_style);
                    }
                }
            }
        }
    }
}

/// A partition column's type: the file's own if stored there, else inferred from its
/// values the way Polars does for a full scan.
pub(crate) fn partition_dtype(
    name: &str,
    file_schema: &Schema,
    values: &[(String, String)],
) -> DataType {
    use polars::io::csv::read::schema_inference::{finish_infer_field_schema, infer_field_schema};
    if let Some(dtype) = file_schema.get(name) {
        return dtype.clone();
    }
    let seen: PlIndexSet<DataType> = values
        .iter()
        .filter(|(k, v)| k == name && !v.is_empty() && v != "__HIVE_DEFAULT_PARTITION__")
        .map(|(_, v)| infer_field_schema(v, true, false))
        .collect();
    if seen.is_empty() {
        DataType::String
    } else {
        finish_infer_field_schema(&seen)
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

    /// Pump Open(path, opts) and subsequent events until no event is returned.
    /// Returns true if a Crash event was seen.
    fn pump_open_until_done(
        app: &mut crate::App,
        rx: &std::sync::mpsc::Receiver<crate::AppEvent>,
        path: std::path::PathBuf,
        opts: crate::OpenOptions,
    ) -> bool {
        use crate::AppEvent;
        let mut next: Option<AppEvent> = Some(AppEvent::Open(vec![path], opts));
        let mut saw_crash = false;
        loop {
            match next.take() {
                Some(ev) => {
                    if matches!(ev, AppEvent::Crash(_)) {
                        saw_crash = true;
                        break;
                    }
                    next = app.event(&ev);
                }
                _ => match rx.recv_timeout(std::time::Duration::from_millis(5000)) {
                    Ok(ev) => {
                        next = Some(ev);
                    }
                    Err(_) => break,
                },
            }
        }
        saw_crash
    }

    /// CSV with 100 int-like rows then "N/A" then more ints. With infer_schema_length=100, Polars
    /// infers Int from the first 100 rows; the parse error surfaces from the async collect as a
    /// BackgroundError event, which the app shows in the error modal rather than crashing.
    #[test]
    fn test_infer_schema_length_csv_short_inference_shows_error_modal() {
        use std::sync::mpsc;

        let path = crate::tests::sample_data_dir().join("infer_schema_length_data.csv");
        let opts = crate::OpenOptions {
            infer_schema_length: Some(100),
            ..Default::default()
        };

        let (tx, rx) = mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());

        assert!(
            !pump_open_until_done(&mut app, &rx, path, opts),
            "load should not crash; parse failure should be surfaced via error modal"
        );
        assert!(
            app.error_modal.active,
            "error modal should be shown after parse failure"
        );
        assert!(!app.busy, "busy flag should be cleared after error");
    }

    #[test]
    fn test_infer_schema_length_csv_succeeds_with_longer_inference() {
        use std::sync::mpsc;

        let path = crate::tests::sample_data_dir().join("infer_schema_length_data.csv");
        let opts = crate::OpenOptions {
            infer_schema_length: Some(101),
            ..Default::default()
        };

        let (tx, rx) = mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());

        assert!(
            !pump_open_until_done(&mut app, &rx, path, opts),
            "load with infer_schema_length=101 should not crash"
        );
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.schema.len(), 1);
        assert!(state.schema.contains("column"));
        assert_eq!(state.num_rows, 201);
    }

    #[test]
    fn test_infer_schema_length_csv_succeeds_with_default() {
        use std::sync::mpsc;

        let path = crate::tests::sample_data_dir().join("infer_schema_length_data.csv");
        let opts = crate::OpenOptions {
            infer_schema_length: Some(1000),
            ..Default::default()
        };

        let (tx, rx) = mpsc::channel();
        let mut app = crate::App::new(tx, crate::tests::test_runtime());

        assert!(
            !pump_open_until_done(&mut app, &rx, path, opts),
            "load with infer_schema_length=1000 (default) should not crash"
        );
        let state = app.data_table_state.as_ref().unwrap();
        assert_eq!(state.schema.len(), 1);
        assert_eq!(state.num_rows, 201);
    }

    #[test]
    fn test_from_csv() {
        // Ensure sample data is generated before running test
        // Test uncompressed CSV loading
        let path = crate::tests::sample_data_dir().join("3-sfd-header.csv");
        let state = DataTableState::from_csv(&path, &Default::default()).unwrap(); // Uses default buffer params from options
        assert_eq!(state.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
    }

    #[test]
    fn test_from_csv_gzipped() {
        // Ensure sample data is generated before running test
        // Test gzipped CSV loading
        let path = crate::tests::sample_data_dir().join("mixed_types.csv.gz");
        let state = DataTableState::from_csv(&path, &Default::default()).unwrap(); // Uses default buffer params from options
        assert_eq!(state.schema.len(), 6); // id, integer_col, float_col, string_col, boolean_col, date_col
    }

    #[test]
    fn test_from_parquet() {
        // Ensure sample data is generated before running test
        let path = crate::tests::sample_data_dir().join("people.parquet");
        let state = DataTableState::from_parquet(&path, None, None, None, None, false, 1).unwrap();
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
    fn test_from_delimited_tsv_has_header() {
        let dir = std::env::temp_dir();
        let path = dir.join("datui_test_tsv_header.tsv");
        let content = "a\tb\tc\td\n1\t2\t3\t4\n5\t6\t7\t8\n";
        std::fs::write(&path, content).unwrap();
        let opts = OpenOptions {
            has_header: Some(true),
            ..Default::default()
        };
        let mut state = DataTableState::from_delimited(&path, b'\t', &opts).unwrap();
        state.collect();
        assert_eq!(state.schema.len(), 4);
        assert!(state.schema.contains("a"));
        assert!(state.schema.contains("b"));
        assert!(state.schema.contains("c"));
        assert!(state.schema.contains("d"));
        assert_eq!(state.num_rows, 2);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_from_delimited_tsv_no_header() {
        let dir = std::env::temp_dir();
        let path = dir.join("datui_test_tsv_no_header.tsv");
        let content = "a\tb\tc\td\n1\t2\t3\t4\n5\t6\t7\t8\n";
        std::fs::write(&path, content).unwrap();
        let opts = OpenOptions {
            has_header: Some(false),
            ..Default::default()
        };
        let mut state = DataTableState::from_delimited(&path, b'\t', &opts).unwrap();
        state.collect();
        assert_eq!(state.schema.len(), 4);
        assert!(state.schema.contains("column_1"));
        assert!(state.schema.contains("column_2"));
        assert!(state.schema.contains("column_3"));
        assert!(state.schema.contains("column_4"));
        assert_eq!(state.num_rows, 3);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_from_delimited_psv_no_header() {
        let dir = std::env::temp_dir();
        let path = dir.join("datui_test_psv_no_header.psv");
        let content = "x|y|z\n10|20|30\n40|50|60\n";
        std::fs::write(&path, content).unwrap();
        let opts = OpenOptions {
            has_header: Some(false),
            ..Default::default()
        };
        let mut state = DataTableState::from_delimited(&path, b'|', &opts).unwrap();
        state.collect();
        assert_eq!(state.schema.len(), 3);
        assert!(state.schema.contains("column_1"));
        assert!(state.schema.contains("column_2"));
        assert!(state.schema.contains("column_3"));
        assert_eq!(state.num_rows, 3);
        let _ = std::fs::remove_file(&path);
    }

    /// Every way out of an export that collected the scan's row index has to take it
    /// off again, including the ones where the file names cannot be worked out.
    #[test]
    fn dropping_the_row_index_leaves_the_data_alone() {
        let with = df!(
            "id" => &[1i64, 2],
            crate::schema_union::DRIFT_COLUMN => &[0u32, 1],
        )
        .unwrap();
        let without = DataTableState::drop_row_index(with);
        assert_eq!(
            without.get_column_names(),
            ["id"],
            "the index goes and nothing else does"
        );

        // A frame that never had one is handed back unchanged.
        let plain = df!("id" => &[1i64]).unwrap();
        assert_eq!(
            DataTableState::drop_row_index(plain).get_column_names(),
            ["id"]
        );
    }

    #[test]
    fn test_filter() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.sort(vec!["a".to_string()], false);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
    }

    #[test]
    fn test_query() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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

    fn column_values(state: &DataTableState, name: &str) -> Vec<Option<i64>> {
        let df = state.lf.clone().collect().unwrap();
        df.column(name)
            .unwrap()
            .cast(&DataType::Int64)
            .unwrap()
            .i64()
            .unwrap()
            .iter()
            .collect()
    }

    #[test]
    fn test_sort_puts_nulls_last_in_both_directions() {
        let lf = df!("a" => &[Some(2i64), None, Some(3), None, Some(1)])
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

        state.sort(vec!["a".to_string()], true);
        assert_eq!(
            column_values(&state, "a"),
            [Some(1), Some(2), Some(3), None, None]
        );

        state.sort(vec!["a".to_string()], false);
        assert_eq!(
            column_values(&state, "a"),
            [Some(3), Some(2), Some(1), None, None]
        );

        state.reverse();
        assert_eq!(
            column_values(&state, "a"),
            [Some(1), Some(2), Some(3), None, None]
        );
    }

    #[test]
    fn test_multi_column_sort_puts_nulls_last_in_every_column() {
        let lf = df!(
            "a" => &[Some(1i64), None, Some(1), Some(2), Some(1)],
            "b" => &[Some(5i64), Some(9), None, Some(7), Some(6)],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();

        state.sort(vec!["a".to_string(), "b".to_string()], false);
        assert_eq!(
            column_values(&state, "a"),
            [Some(2), Some(1), Some(1), Some(1), None]
        );
        assert_eq!(
            column_values(&state, "b"),
            [Some(7), Some(6), Some(5), None, Some(9)]
        );
    }

    #[test]
    fn test_by_query_puts_null_group_last() {
        let lf = df!(
            "g" => &[Some(2i64), None, Some(1), Some(2)],
            "v" => &[1i64, 2, 3, 4],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.query("select sum v by g".to_string());
        assert!(state.error.is_none(), "{:?}", state.error);
        assert_eq!(column_values(&state, "g"), [Some(1), Some(2), None]);
    }

    #[test]
    fn test_filter_multiple() {
        let lf = create_large_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: None,
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: None,
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string(), "date".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::First,
            sort_columns: None,
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
        let mut state_min = DataTableState::new(lf.clone(), None, None, None, None, true).unwrap();
        state_min
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Min,
                sort_columns: None,
            })
            .unwrap();
        let df_min = state_min.lf.clone().collect().unwrap();
        assert_eq!(
            df_min.column("A").unwrap().get(0).unwrap(),
            AnyValue::Float64(10.0)
        );

        let mut state_max = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state_max
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Max,
                sort_columns: None,
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
        let mut state_avg = DataTableState::new(lf.clone(), None, None, None, None, true).unwrap();
        state_avg
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Avg,
                sort_columns: None,
            })
            .unwrap();
        let df_avg = state_avg.lf.clone().collect().unwrap();
        let a = df_avg.column("A").unwrap().get(0).unwrap();
        if let AnyValue::Float64(x) = a {
            assert!((x - 10.5).abs() < 1e-6);
        } else {
            panic!("expected float");
        }

        let mut state_count = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state_count
            .pivot(&PivotSpec {
                index: vec!["id".to_string(), "date".to_string()],
                pivot_column: "key".to_string(),
                value_column: "value".to_string(),
                aggregation: PivotAggregation::Count,
                sort_columns: None,
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        let spec = PivotSpec {
            index: vec!["id".to_string()],
            pivot_column: "key".to_string(),
            value_column: "value".to_string(),
            aggregation: PivotAggregation::Last,
            sort_columns: None,
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
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
            sort_columns: None,
        };
        state.pivot(&spec).unwrap();
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.height(), 1);
        let id_col = df.column("id").unwrap();
        assert_eq!(id_col.get(0).unwrap(), AnyValue::Int32(1));
    }

    #[test]
    fn test_fuzzy_token_regex() {
        assert_eq!(fuzzy_token_regex("foo"), "(?i).*f.*o.*o.*");
        assert_eq!(fuzzy_token_regex("a"), "(?i).*a.*");
        // Regex-special characters are escaped
        let pat = fuzzy_token_regex("[");
        assert!(pat.contains("\\["));
    }

    #[test]
    fn test_fuzzy_search() {
        // Filter logic is covered by test_fuzzy_search_regex_direct. This test runs the full
        // path through DataTableState; it requires sample data (CSV with string column).
        crate::tests::ensure_sample_data();
        let path = crate::tests::sample_data_dir().join("3-sfd-header.csv");
        let mut state = DataTableState::from_csv(&path, &Default::default()).unwrap();
        state.visible_rows = 10;
        state.collect();
        let before = state.num_rows;
        state.fuzzy_search("string".to_string());
        assert!(state.error.is_none(), "{:?}", state.error);
        assert!(state.num_rows <= before, "fuzzy search should filter rows");
        state.fuzzy_search("".to_string());
        state.collect();
        assert_eq!(state.num_rows, before, "empty fuzzy search should reset");
        assert!(state.get_active_fuzzy_query().is_empty());
    }

    #[test]
    fn test_fuzzy_search_regex_direct() {
        // Sanity check: Polars str().contains with our regex matches "alice" for pattern ".*a.*l.*i.*"
        let lf = df!("name" => &["alice", "bob", "carol"]).unwrap().lazy();
        let pattern = fuzzy_token_regex("alice");
        let out = lf
            .filter(col("name").str().contains(lit(pattern.clone()), false))
            .collect()
            .unwrap();
        assert_eq!(out.height(), 1, "regex {:?} should match alice", pattern);

        // Two columns OR (as in fuzzy_search)
        let lf2 = df!(
            "id" => &[1i32, 2, 3],
            "name" => &["alice", "bob", "carol"],
            "city" => &["NYC", "LA", "Boston"]
        )
        .unwrap()
        .lazy();
        let pat = fuzzy_token_regex("alice");
        let expr = col("name")
            .str()
            .contains(lit(pat.clone()), false)
            .or(col("city").str().contains(lit(pat), false));
        let out2 = lf2.clone().filter(expr).collect().unwrap();
        assert_eq!(out2.height(), 1);

        // Replicate exact fuzzy_search logic: schema from original_lf, string_cols, then filter
        let schema = lf2.clone().collect_schema().unwrap();
        let string_cols: Vec<String> = schema
            .iter()
            .filter(|(_, dtype)| dtype.is_string())
            .map(|(name, _)| name.to_string())
            .collect();
        assert!(
            !string_cols.is_empty(),
            "df! string cols should be detected"
        );
        let pattern = fuzzy_token_regex("alice");
        let token_expr = string_cols
            .iter()
            .map(|c| col(c.as_str()).str().contains(lit(pattern.clone()), false))
            .reduce(|a, b| a.or(b))
            .unwrap();
        let out3 = lf2.filter(token_expr).collect().unwrap();
        assert_eq!(
            out3.height(),
            1,
            "fuzzy_search-style filter should match 1 row"
        );
    }

    /// A fuzzy search replaces the sort along with the rest of the pipeline: a descending
    /// sort before it must not leave the result reversed with no sort column to show it.
    #[test]
    fn fuzzy_search_after_a_descending_sort_is_not_reversed() {
        let lf = df!(
            "id" => &[1i32, 2, 3],
            "name" => &["alice", "bob", "carol"]
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.sort(vec!["id".to_string()], false);
        state.fuzzy_search("a".to_string());
        assert!(state.error.is_none(), "{:?}", state.error);
        assert!(state.view_sort_columns().is_empty());
        assert!(state.view_sort_ascending());

        // The sidebar re-applies the (empty) filters and sort over the result.
        state.filter(Vec::new());
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.height(), 2);
        assert_eq!(df.column("id").unwrap().get(0).unwrap(), AnyValue::Int32(1));
    }

    /// A reshape likewise replaces the sort. It runs over the sorted view, so its rows
    /// come out descending, and a sidebar action afterwards must leave them that way
    /// rather than reverse a frame that shows no sort column.
    #[test]
    fn melt_after_a_descending_sort_is_not_reversed() {
        let lf = df!("id" => &[1i32, 2, 3], "c1" => &[10i32, 20, 30])
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.sort(vec!["id".to_string()], false);
        state
            .melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["c1".to_string()],
                variable_name: "var".to_string(),
                value_name: "val".to_string(),
            })
            .unwrap();
        assert!(state.view_sort_columns().is_empty());
        assert!(state.view_sort_ascending());
        let melted = state.lf.clone().collect().unwrap();
        assert_eq!(
            melted.column("id").unwrap().get(0).unwrap(),
            AnyValue::Int32(3)
        );

        state.filter(Vec::new());
        assert!(state.lf.clone().collect().unwrap().equals(&melted));
    }

    #[test]
    fn test_fuzzy_search_no_string_columns() {
        let lf = df!("a" => &[1i32, 2, 3], "b" => &[10i64, 20, 30])
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.fuzzy_search("x".to_string());
        assert!(state.error.is_some());
    }

    /// By-queries must produce results sorted by the group columns (age_group, then team)
    /// so that output order is deterministic and practical. Raw data is deliberately out of order.
    #[test]
    fn test_by_query_result_sorted_by_group_columns() {
        // Build a small table: age_group (1-5, out of order), team (Red/Blue/Green), score (0-100)
        let df = df!(
            "age_group" => &[3i64, 1, 5, 2, 4, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5],
            "team" => &[
                "Red", "Blue", "Green", "Red", "Blue", "Green", "Green", "Red", "Blue",
                "Green", "Red", "Blue", "Red", "Blue", "Green",
            ],
            "score" => &[50.0f64, 10.0, 90.0, 20.0, 30.0, 40.0, 60.0, 70.0, 80.0, 15.0, 25.0, 35.0, 45.0, 55.0, 65.0],
        )
        .unwrap();
        let lf = df.lazy();
        let options = crate::OpenOptions::default();
        let mut state = DataTableState::from_lazyframe(lf, &options).unwrap();
        state.query("select avg score by age_group, team".to_string());
        assert!(
            state.error.is_none(),
            "query should succeed: {:?}",
            state.error
        );
        let result = state.lf.collect().unwrap();
        // Result must be sorted by group columns (age_group, then team)
        let sorted = result
            .sort(
                ["age_group", "team"],
                SortMultipleOptions::default().with_order_descending(false),
            )
            .unwrap();
        assert_eq!(
            result, sorted,
            "by-query result must be sorted by (age_group, team)"
        );
    }

    /// Computed group keys (e.g. Fare: 1+floor Fare % 25) must be sorted by their result column
    /// values, not by re-evaluating the expression on the result.
    #[test]
    fn test_by_query_computed_group_key_sorted_by_result_column() {
        let df = df!(
            "x" => &[7.0f64, 12.0, 3.0, 22.0, 17.0, 8.0],
            "v" => &[1.0f64, 2.0, 3.0, 4.0, 5.0, 6.0],
        )
        .unwrap();
        let lf = df.lazy();
        let options = crate::OpenOptions::default();
        let mut state = DataTableState::from_lazyframe(lf, &options).unwrap();
        // bucket: 1+floor(x)%3 -> values 1,2,3; raw x order 7,12,3,22,17,8 -> buckets 2,2,1,2,2,2
        state.query("select sum v by bucket: 1+floor x % 3".to_string());
        assert!(
            state.error.is_none(),
            "query should succeed: {:?}",
            state.error
        );
        let result = state.lf.collect().unwrap();
        let bucket = result.column("bucket").unwrap();
        // Must be sorted by bucket (1, 2, 3)
        for i in 1..result.height() {
            let prev: i64 = bucket.get(i - 1).unwrap().try_extract().unwrap_or(0);
            let curr: i64 = bucket.get(i).unwrap().try_extract().unwrap_or(0);
            assert!(
                curr >= prev,
                "bucket column must be sorted: {} then {}",
                prev,
                curr
            );
        }
    }

    fn write_parquet(path: &Path, n: i64) {
        let mut df = df!("v" => (0..n).collect::<Vec<i64>>()).unwrap();
        let file = File::create(path).unwrap();
        ParquetWriter::new(file).finish(&mut df).unwrap();
    }

    #[test]
    fn test_count_rows_from_parquet_dir_sums_footers() {
        let dir = tempfile::tempdir().unwrap();
        // Hive-style layout: two partitions, multiple files each.
        let p1 = dir.path().join("year=2020");
        let p2 = dir.path().join("year=2021");
        fs::create_dir_all(&p1).unwrap();
        fs::create_dir_all(&p2).unwrap();
        write_parquet(&p1.join("a.parquet"), 10);
        write_parquet(&p1.join("b.parquet"), 5);
        write_parquet(&p2.join("c.parquet"), 7);

        let n = DataTableState::count_rows_from_parquet_dir(dir.path()).unwrap();
        assert_eq!(n, 22, "should sum footer row counts across all files");
    }

    #[test]
    fn test_count_rows_skips_non_parquet_and_corrupt_files() {
        let dir = tempfile::tempdir().unwrap();
        write_parquet(&dir.path().join("good.parquet"), 8);
        // A non-parquet file must be ignored entirely.
        fs::write(dir.path().join("notes.txt"), b"ignore me").unwrap();
        // A corrupt .parquet must be skipped, not abort the whole count.
        fs::write(dir.path().join("bad.parquet"), b"not a parquet footer").unwrap();

        let n = DataTableState::count_rows_from_parquet_dir(dir.path()).unwrap();
        assert_eq!(n, 8, "non-parquet and unreadable files should be skipped");
    }

    #[test]
    fn test_count_rows_errors_when_no_readable_parquet() {
        let dir = tempfile::tempdir().unwrap();
        fs::write(dir.path().join("only.txt"), b"nothing here").unwrap();
        assert!(
            DataTableState::count_rows_from_parquet_dir(dir.path()).is_err(),
            "a directory with no parquet files should error so the caller can fall back"
        );
    }

    // Read the header (top) row of a rendered buffer as a string.
    fn header_row_string(buf: &Buffer, area: Rect) -> String {
        (area.x..area.x + area.width)
            .map(|x| buf[(x, area.y)].symbol().to_string())
            .collect()
    }

    // Read an arbitrary row of a rendered buffer as a string (y = 0 is the header).
    fn row_string(buf: &Buffer, area: Rect, y: u16) -> String {
        (area.x..area.x + area.width)
            .map(|x| buf[(x, area.y + y)].symbol().to_string())
            .collect()
    }

    fn table_with_format(preset: &str, align: bool) -> DataTable {
        DataTable::default().with_number_format(NumberFormatSettings {
            format: crate::numfmt::NumberFormat::preset(preset).unwrap(),
            enabled: true,
            exclude: Vec::new(),
            align_numeric_right: align,
        })
    }

    #[test]
    fn grouping_is_off_by_default() {
        // Upgrading must not change how anything renders.
        let table = DataTable::default();
        let df = df!("pos" => &[1234567i64]).unwrap();
        let area = Rect::new(0, 0, 30, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let row = row_string(&buf, area, 1);
        assert!(row.contains("1234567"), "got: {row:?}");
        assert!(!row.contains("1,234,567"), "got: {row:?}");
    }

    #[test]
    fn thousands_separators_are_applied_to_integer_columns() {
        let table = table_with_format("thousands", false);
        // Genomic coordinates, the case from issue #51.
        let df = df!("chromStart" => &[248956422i64, 3088269832]).unwrap();
        let area = Rect::new(0, 0, 30, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert!(row_string(&buf, area, 1).contains("248,956,422"));
        assert!(row_string(&buf, area, 2).contains("3,088,269,832"));
    }

    #[test]
    fn column_width_accounts_for_separators() {
        // The separators widen the column; the heading must not be clipped and
        // the value must render in full.
        let table = table_with_format("thousands", false);
        let df = df!("n" => &[1234567i64]).unwrap();
        let area = Rect::new(0, 0, 12, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(shown, 1);
        assert!(row_string(&buf, area, 1).contains("1,234,567"));
    }

    #[test]
    fn strings_are_untouched_and_integers_group_uniformly() {
        let table = table_with_format("thousands", false);
        let df = df!(
            "chrom" => &["chr1"],
            "n" => &[2024i32],
        )
        .unwrap();
        let area = Rect::new(0, 0, 30, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let row = row_string(&buf, area, 1);
        assert!(row.contains("chr1"), "got: {row:?}");
        // No magnitude threshold: a column must not mix grouped and ungrouped
        // values, so four-digit numbers group like everything else.
        assert!(row.contains("2,024"), "got: {row:?}");
    }

    #[test]
    fn excluding_a_column_is_how_identifier_columns_stay_plain() {
        // The replacement for a digit threshold: name the columns that hold
        // identifiers rather than quantities.
        let table = DataTable::default().with_number_format(NumberFormatSettings {
            format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
            enabled: true,
            exclude: vec![crate::numfmt::Glob::new("year")],
            align_numeric_right: false,
        });
        let df = df!(
            "year" => &[2024i32],
            "count" => &[2024i32],
        )
        .unwrap();
        let area = Rect::new(0, 0, 40, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let row = row_string(&buf, area, 1);
        assert!(row.contains("2024"), "excluded column stays plain: {row:?}");
        assert!(row.contains("2,024"), "other column groups: {row:?}");
    }

    #[test]
    fn numeric_columns_and_their_headers_render_flush_right() {
        let table = table_with_format("none", true);
        // Header "value" is 5 wide; the values are shorter, so they must be
        // padded on the left, not the right.
        let df = df!("value" => &[7i64, 42]).unwrap();
        let area = Rect::new(0, 0, 5, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(row_string(&buf, area, 1), "    7");
        assert_eq!(row_string(&buf, area, 2), "   42");
        assert_eq!(header_row_string(&buf, area), "value");
    }

    #[test]
    fn header_follows_its_column_alignment() {
        // A wide numeric column: the heading must sit flush right over the
        // digits rather than floating left.
        let table = table_with_format("none", true);
        let df = df!("n" => &[1234567i64]).unwrap();
        let area = Rect::new(0, 0, 7, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(header_row_string(&buf, area), "      n");
        assert_eq!(row_string(&buf, area, 1), "1234567");
    }

    #[test]
    fn non_numeric_columns_stay_left_aligned() {
        let table = table_with_format("none", true);
        let df = df!("name" => &["ab"]).unwrap();
        let area = Rect::new(0, 0, 4, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(row_string(&buf, area, 1), "ab  ");
        assert_eq!(header_row_string(&buf, area), "name");
    }

    #[test]
    fn alignment_can_be_turned_off() {
        let table = table_with_format("none", false);
        let df = df!("value" => &[7i64]).unwrap();
        let area = Rect::new(0, 0, 5, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(row_string(&buf, area, 1), "7    ");
    }

    #[test]
    fn excluded_columns_are_not_grouped() {
        let table = DataTable::default().with_number_format(NumberFormatSettings {
            format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
            enabled: true,
            exclude: vec![crate::numfmt::Glob::new("*_id")],
            align_numeric_right: false,
        });
        let df = df!(
            "sample_id" => &[1234567i64],
            "count" => &[1234567i64],
        )
        .unwrap();
        let area = Rect::new(0, 0, 40, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let row = row_string(&buf, area, 1);
        assert!(row.contains("1234567"), "excluded column raw: {row:?}");
        assert!(row.contains("1,234,567"), "other column grouped: {row:?}");
    }

    #[test]
    fn disabled_formatting_renders_raw_digits() {
        // What the F toggle does: same settings, enabled = false.
        let mut settings = NumberFormatSettings {
            format: crate::numfmt::NumberFormat::preset("thousands").unwrap(),
            enabled: true,
            exclude: Vec::new(),
            align_numeric_right: false,
        };
        settings.enabled = false;
        let table = DataTable::default().with_number_format(settings);
        let df = df!("n" => &[1234567i64]).unwrap();
        let area = Rect::new(0, 0, 20, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert!(row_string(&buf, area, 1).contains("1234567"));
    }

    #[test]
    fn binary_stub_columns_are_never_formatted_or_aligned() {
        // The stub is a placeholder, not data.
        let table = DataTable {
            binary_cols: std::collections::HashSet::from(["blob".to_string()]),
            ..table_with_format("thousands", true)
        };
        let df = df!("blob" => &[BINARY_STUB]).unwrap();
        let area = Rect::new(0, 0, 10, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert!(row_string(&buf, area, 1).starts_with(BINARY_STUB));
    }

    #[test]
    fn trailing_overflow_string_column_is_truncated_not_dropped() {
        // A string trailing column that doesn't fully fit should still be shown truncated
        // (filling the remaining width) rather than dropped entirely (leaving blank space).
        let table = DataTable::default();
        let df = df!(
            "a" => &[1i32, 2, 3],
            "wide_text" => &["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"],
        )
        .unwrap();
        // "a" needs width 1; with padding that's used_width 2, leaving 6 for "wide_text",
        // whose full width (10) overflows -> it must be shown truncated to the remaining 6.
        let area = Rect::new(0, 0, 8, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(
            shown, 2,
            "the overflowing trailing string column should be kept (truncated)"
        );
        // Part of the heading should be visible so the user knows what the column is.
        assert!(
            header_row_string(&buf, area).contains("wide"),
            "truncated column heading should be visible: {:?}",
            header_row_string(&buf, area)
        );
    }

    #[test]
    fn binary_stub_cells_are_styled_with_binary_color_and_italic() {
        // Binary columns render the `‹binary›` stub; those cells should be colored with
        // binary_col and italicized so they read as a placeholder, while ordinary columns
        // keep their normal (non-italic) styling.
        let table = DataTable {
            binary_col: Some(Color::DarkGray),
            binary_cols: std::collections::HashSet::from(["blob".to_string()]),
            ..DataTable::default()
        };
        let df = df!(
            "a" => &[1i32, 2],
            "blob" => &[BINARY_STUB, BINARY_STUB],
        )
        .unwrap();
        let area = Rect::new(0, 0, 20, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);

        // A data row (y = 1; y = 0 is the header). The stub cells should be dark gray + italic.
        let stub_styled = (area.x..area.x + area.width).any(|x| {
            let cell = &buf[(x, 1)];
            cell.fg == Color::DarkGray && cell.modifier.contains(Modifier::ITALIC)
        });
        assert!(
            stub_styled,
            "binary stub cells should be colored with binary_col and italicized"
        );
        // The non-binary "a" column must not be italicized.
        let any_italic_non_darkgray = (area.x..area.x + area.width).any(|x| {
            let cell = &buf[(x, 1)];
            cell.modifier.contains(Modifier::ITALIC) && cell.fg != Color::DarkGray
        });
        assert!(
            !any_italic_non_darkgray,
            "only binary columns should be italicized"
        );
    }

    #[test]
    fn trailing_overflow_numeric_column_is_dropped_not_truncated() {
        // A numeric column must NOT be shown truncated: a partial number reads as a wrong value.
        let table = DataTable::default();
        let df = df!(
            "a" => &[1i32, 2, 3],
            "wide_number" => &[111_111_111i64, 222_222_222, 333_333_333],
        )
        .unwrap();
        let area = Rect::new(0, 0, 8, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(
            shown, 1,
            "an overflowing numeric column should be dropped, not truncated"
        );
    }

    #[test]
    fn byte_clamp_keeps_view_near_end_of_buffer() {
        // Regression: jumping to the END of a large dataset used to go blank because the
        // max_buffered_mb byte-clamp trimmed the buffer's HEAD (slice(0, max_rows)), discarding
        // exactly the tail rows the view needed. The clamp must keep the view in range.
        let lf = df!("a" => &["seed"]).unwrap().lazy();
        // 1 MB byte budget.
        let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
        state.num_rows = 1000;
        state.num_rows_valid = true;
        state.visible_rows = 40;
        state.start_row = 960; // jump-to-end position (num_rows - visible_rows)

        // Buffer spans [900, 1000); the view [960, 1000) sits at its tail. ~2 MB of data forces
        // a trim to roughly half the rows.
        let buffer_start = 900;
        let big: Vec<String> = (0..100).map(|_| "z".repeat(20_000)).collect();
        let df = df!("a" => big).unwrap();

        let (sliced, eff_start, eff_end) = state.clamp_buffer_bytes(df, buffer_start);
        assert!(
            sliced.height() < 100,
            "expected a trim below the byte budget"
        );
        assert!(
            eff_start <= state.start_row,
            "view start {} fell before kept buffer start {}",
            state.start_row,
            eff_start
        );
        assert!(
            eff_end >= state.start_row + state.visible_rows,
            "view end {} fell after kept buffer end {}",
            state.start_row + state.visible_rows,
            eff_end
        );
        assert_eq!(
            sliced.height(),
            eff_end - eff_start,
            "df height must match range"
        );
    }

    #[test]
    fn the_files_holding_a_range_of_rows() {
        let offsets = [0, 100, 100, 250, 400];
        assert_eq!(files_holding(&offsets, 0, 10), Some((0, 0)));
        assert_eq!(
            files_holding(&offsets, 95, 10),
            Some((0, 2)),
            "the empty file is skipped"
        );
        assert_eq!(files_holding(&offsets, 100, 10), Some((2, 2)));
        assert_eq!(files_holding(&offsets, 390, 50), Some((3, 3)));
        assert_eq!(files_holding(&offsets, 400, 10), None);
        assert_eq!(files_holding(&[0], 0, 10), None);
    }

    #[test]
    fn a_buffer_over_many_small_files_opens_a_few() {
        // A thousand files of ten rows each.
        let offsets: Vec<usize> = (0..=1000).map(|i| i * 10).collect();
        // A window of 2,000 rows around row 5,000 spans 200 files; it keeps 16.
        let (start, end) = limit_files(&offsets, 5_000, 5_040, 4_000, 6_000, 16);
        assert_eq!(
            files_holding(&offsets, start, end - start).map(|(a, b)| b - a + 1),
            Some(16)
        );
        assert!(
            start <= 5_000 && 5_040 <= end,
            "the view stays: {start}..{end}"
        );
        // A view spanning more files than the limit keeps all of them.
        let (start, end) = limit_files(&offsets, 0, 400, 0, 400, 16);
        assert_eq!((start, end), (0, 400));
        // Few files: unchanged.
        assert_eq!(limit_files(&offsets, 0, 40, 0, 100, 16), (0, 100));
    }

    #[test]
    fn a_counted_remote_dataset_reads_only_the_files_a_buffer_needs() {
        use polars::prelude::IntoLazy;
        let part = |from: i32| {
            polars::df!("n" => (from..from + 100).collect::<Vec<i32>>())
                .unwrap()
                .lazy()
        };
        let urls: Vec<String> = (0..5).map(|i| format!("file{i}")).collect();
        let asked = Arc::new(std::sync::Mutex::new(Vec::<Vec<String>>::new()));
        let scan: FileScan = {
            let asked = asked.clone();
            Arc::new(move |urls: &[String]| {
                asked.lock().unwrap().push(urls.to_vec());
                let frames: Vec<LazyFrame> = urls
                    .iter()
                    .map(|u| part(u.trim_start_matches("file").parse::<i32>().unwrap() * 100))
                    .collect();
                polars::prelude::concat(frames, Default::default())
            })
        };
        let full = scan(&urls).unwrap();
        asked.lock().unwrap().clear();
        let mut state =
            DataTableState::from_lazyframe(full, &crate::OpenOptions::default()).unwrap();
        state.set_remote_source();
        state.set_remote_files(RemoteFiles {
            urls: Arc::new(urls),
            scan,
            count: Arc::new(|| Ok(vec![vec![50, 50]; 5])),
            offsets: None,
        });
        let groups = (state.remote_files_counter().unwrap())().unwrap();
        state.set_file_row_groups(&groups);
        assert_eq!(state.num_rows_if_valid(), Some(500));
        assert!(state.remote_files_counter().is_none(), "counted once");

        let df = collect_lazy(state.buffer_lf(350, 20).unwrap(), false).unwrap();
        let values: Vec<i32> = df
            .column("n")
            .unwrap()
            .i32()
            .unwrap()
            .into_no_null_iter()
            .collect();
        assert_eq!(values, (350..370).collect::<Vec<i32>>());
        assert_eq!(*asked.lock().unwrap(), vec![vec!["file3".to_string()]]);

        asked.lock().unwrap().clear();
        let df = collect_lazy(state.buffer_lf(190, 20).unwrap(), false).unwrap();
        assert_eq!(df.height(), 20);
        assert_eq!(
            *asked.lock().unwrap(),
            vec![vec!["file1".to_string(), "file2".to_string()]],
            "a range across a boundary reads both files"
        );
    }

    #[test]
    fn a_remote_source_buffers_one_window_and_pages_inside_it_for_free() {
        // Every buffer fill of an object-store scan downloads whole row groups, so the
        // buffer is one window of `max_buffered_rows` rather than a few pages: paging
        // inside it asks for nothing, and a jump asks once.
        let lf = df!("a" => &[0i32]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true).unwrap();
        state.set_remote_source();
        state.num_rows = 1_000_000;
        state.num_rows_valid = true;
        state.visible_rows = 40;
        let window = |start: usize| CollectResult {
            df: df!("a" => (0..10_000).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: start + 10_000,
            num_rows: 1_000_000,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, 10_000));
        state.apply_async_collect(window(0));
        for _ in 0..20 {
            assert!(!state.page_down(), "a page inside the window needs no fill");
        }

        assert!(state.scroll_to_end());
        let request = state
            .prepare_async_collect(None)
            .expect("the jump fills once");
        assert_eq!(
            (request.buffer_start, request.buffer_end),
            (990_000, 1_000_000)
        );
        state.apply_async_collect(window(990_000));

        assert!(state.scroll_to_start(), "Home after End must fill again");
        let request = state
            .prepare_async_collect(None)
            .expect("one fill at the top");
        assert_eq!((request.buffer_start, request.buffer_end), (0, 10_000));
    }

    #[test]
    fn align_to_row_groups_takes_the_view_groups_whole_and_lookahead_within_the_cap() {
        let offsets = [0, 1_000_000, 2_000_000, 3_000_000, 3_500_000];
        // A window inside one group is that group, whatever the row cap.
        assert_eq!(
            align_to_row_groups(&offsets, 50, 97, 0, 100_000, 100_000),
            (0, 1_000_000)
        );
        // A view straddling a boundary takes both groups, over the cap.
        assert_eq!(
            align_to_row_groups(&offsets, 999_980, 1_000_020, 950_000, 1_050_000, 100_000),
            (0, 2_000_000)
        );
        // Groups the window reaches into come along while they fit, the one ahead first.
        assert_eq!(
            align_to_row_groups(
                &offsets, 1_500_000, 1_500_047, 950_000, 2_050_000, 2_000_000
            ),
            (1_000_000, 3_000_000)
        );
        assert_eq!(
            align_to_row_groups(&offsets, 1_500_000, 1_500_047, 950_000, 2_050_000, 0),
            (0, 3_000_000)
        );
        // The last, short group; and a window past the data is clamped to it.
        assert_eq!(
            align_to_row_groups(
                &offsets, 3_400_000, 3_400_047, 3_350_000, 3_450_000, 100_000
            ),
            (3_000_000, 3_500_000)
        );
        // No groups known: the window is left alone.
        assert_eq!(align_to_row_groups(&[0], 5, 10, 0, 100, 50), (0, 100));
    }

    #[test]
    fn a_remote_object_is_read_inside_its_row_group() {
        // Ten 1M-row groups with the default 100k-row cap: the window is planned inside
        // the group the view is in, so it never pulls the next group before the view
        // reaches it; crossing fetches rows of the next group alone, stitched on to the
        // ones on hand and trimmed back to the cap.
        const G: usize = 1_000_000;
        const CAP: usize = DEFAULT_MAX_BUFFERED_ROWS;
        let lf = df!("a" => &[0i32]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[G; 10]);
        assert_eq!(state.num_rows, 10 * G);
        state.visible_rows = 40;
        let rows = |start: usize, end: usize| CollectResult {
            df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 10 * G,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, CAP));
        state.apply_async_collect(rows(0, CAP));
        for _ in 0..20 {
            assert!(!state.page_down(), "a page inside the window needs no fill");
        }

        // A jump to the end of group 0 is clipped to it: group 1 is not touched yet.
        assert!(state.scroll_to(G - 60));
        let request = state
            .prepare_async_collect(None)
            .expect("the end of group 0");
        assert_eq!((request.buffer_start, request.buffer_end), (G - CAP, G));
        state.apply_async_collect(rows(G - CAP, G));

        // A view straddling the boundary fetches rows of group 1 alone.
        assert!(state.scroll_to(G - 20));
        let request = state.prepare_async_collect(None).expect("into group 1");
        assert_eq!((request.buffer_start, request.buffer_end), (G, G + CAP / 2));
        state.apply_async_collect(rows(G, G + CAP / 2));
        let (held_start, held_end) = (state.buffered_start(), state.buffered_end());
        assert!(
            held_start <= G - 20 && G + 20 <= held_end,
            "the view is on hand"
        );
        assert_eq!(held_end - held_start, CAP, "trimmed back to the cap");
        let held = state.buffered_df.as_ref().unwrap();
        assert_eq!(held.height(), CAP);
        assert_eq!(
            held.column("a").unwrap().i32().unwrap().get(G - held_start),
            Some(G as i32),
            "stitched in order"
        );
        assert!(!state.page_down());

        // End and Home are one window each.
        assert!(state.scroll_to_end());
        let request = state.prepare_async_collect(None).expect("the last window");
        assert_eq!(
            (request.buffer_start, request.buffer_end),
            (10 * G - CAP, 10 * G)
        );
        state.apply_async_collect(rows(10 * G - CAP, 10 * G));
        assert!(state.scroll_to_start());
        let request = state.prepare_async_collect(None).expect("the first window");
        assert_eq!((request.buffer_start, request.buffer_end), (0, CAP));
    }

    #[test]
    fn small_row_groups_are_fetched_whole() {
        // 40k-row groups under a 100k cap: a fill is whole groups, as many as fit,
        // and crossing into the next fetches exactly that group.
        const G: usize = 40_000;
        let lf = df!("a" => &[0i32]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[G; 25]);
        state.visible_rows = 40;
        let rows = |start: usize, end: usize| CollectResult {
            df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 25 * G,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, 2 * G));
        state.apply_async_collect(rows(0, 2 * G));

        assert!(state.scroll_to(2 * G - 20));
        let request = state.prepare_async_collect(None).expect("the next group");
        assert_eq!((request.buffer_start, request.buffer_end), (2 * G, 3 * G));
        state.apply_async_collect(rows(2 * G, 3 * G));
        let (held_start, held_end) = (state.buffered_start(), state.buffered_end());
        assert!(held_start <= 2 * G - 20 && 2 * G + 20 <= held_end);
        assert!(held_end - held_start <= DEFAULT_MAX_BUFFERED_ROWS);
    }

    #[test]
    fn a_wide_schema_is_budgeted_before_the_collect() {
        // 1,000 Float64 columns are 8,000 bytes a row: a 512 MB budget allows 67,108
        // rows, so the planned window is that and not the 100k row cap. The rows are
        // never materialized only to be trimmed after the collect.
        let columns: Vec<Column> = (0..1000)
            .map(|i| Series::new(format!("f{i}").into(), &[0.0f64]).into())
            .collect();
        let lf = DataFrame::new(1, columns).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        assert_eq!(
            estimate_bytes_per_row(&state.schema, &state.column_order, &[]),
            8_000
        );
        state.set_remote_source();
        state.set_row_groups(&[1_000_000; 3]);
        state.visible_rows = 40;

        let request = state.prepare_async_collect(None).expect("first fill");
        let planned = request.buffer_end - request.buffer_start;
        assert!(
            planned <= 512 * 1024 * 1024 / 8_000,
            "planned {planned} rows over the byte budget"
        );
        assert!(planned >= 40, "never below a screen");
        assert_eq!(
            request.buffer_start, 0,
            "a window at the top starts at the top"
        );

        // The screen is the floor, whatever the budget.
        let mut tiny = DataTableState::new(
            df!("a" => &["x".repeat(2_000)]).unwrap().lazy(),
            None,
            None,
            None,
            Some(1),
            true,
        )
        .unwrap();
        tiny.set_column_widths(vec![("a".to_string(), 2_000)]);
        tiny.visible_rows = 40;
        assert_eq!(tiny.byte_cap_rows(), 1024 * 1024 / 2_016);
        tiny.set_column_widths(vec![("a".to_string(), 1 << 20)]);
        assert_eq!(tiny.byte_cap_rows(), 40);
    }

    #[test]
    fn a_collected_buffer_measures_the_next_plan() {
        // The schema guesses 40 bytes for a string; the first buffer shows the strings
        // are 2 KB, and the next window is planned on that.
        let big: Vec<String> = (0..100).map(|_| "z".repeat(2_000)).collect();
        let lf = df!("a" => &big).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
        state.num_rows = 1_000_000;
        state.num_rows_valid = true;
        state.visible_rows = 40;
        let guessed = state.byte_cap_rows();
        assert_eq!(guessed, 1024 * 1024 / STRING_BYTES_GUESS);
        state.apply_async_collect(CollectResult {
            df: df!("a" => &big).unwrap(),
            buffer_start: 0,
            buffer_end: 100,
            num_rows: 1_000_000,
            count_known: true,
        });
        let measured = state.byte_cap_rows();
        assert!(
            (400..=600).contains(&measured),
            "about 1 MB / 2 KB rows, got {measured}"
        );
    }

    #[test]
    fn a_filtered_remote_scan_falls_back_to_the_page_window() {
        // `filter(..).slice(0, N)` stops at the first N matches, so a window of a few
        // pages stops at the first row group with any; the 100k window read forty.
        use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
        let lf = df!("a" => (0..1_000i32).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[500, 500]);
        state.visible_rows = 40;
        state.defer_collect = true;

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!(
            (request.buffer_start, request.buffer_end),
            (0, 1_000),
            "both groups fit the remote window"
        );

        state.filter(vec![FilterStatement {
            column: "a".to_string(),
            operator: FilterOperator::Gt,
            value: "990".to_string(),
            logical_op: LogicalOperator::And,
        }]);
        let request = state.prepare_async_collect(None).expect("filtered fill");
        assert_eq!(
            (request.buffer_start, request.buffer_end),
            (0, 7 * 40),
            "a page plus three either side, not the remote window"
        );

        state.filter(Vec::new());
        assert!(
            state.remote_window(),
            "with the filters cleared the frame is the scan as loaded again"
        );
        assert_eq!(
            state.num_rows_if_valid(),
            Some(1_000),
            "and its footer answers the count"
        );

        // The local hive footer count is gated the same way.
        state.set_parquet_count_dir(PathBuf::from("/hive"));
        state.sort(vec!["a".to_string()], true);
        assert!(state.parquet_count_dir().is_none());
        state.sort(Vec::new(), true);
        assert_eq!(state.parquet_count_dir(), Some(PathBuf::from("/hive")));
        state.reverse();
        assert!(!state.remote_window(), "reversed is not as loaded");
    }

    #[test]
    fn a_count_below_the_view_brings_the_view_back() {
        // A filter applied deep in the data: the frame turns out to have 100 rows and
        // the view was at 9,990. It comes back to the data and asks for a fill.
        let lf = df!("a" => (0..10_000i32).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 10;
        state.num_rows = 10_000;
        state.num_rows_valid = true;
        assert!(state.scroll_to_end());
        assert_eq!(state.start_row, 9_990);
        state.set_num_rows(100);
        assert_eq!(state.start_row, 90);
        assert!(state.needs_recollect);

        // A slice deep in the frame that found nothing is not the count.
        state.needs_recollect = false;
        state.num_rows_valid = false;
        state.start_row = 9_990;
        state.apply_async_collect(CollectResult {
            df: df!("a" => Vec::<i32>::new()).unwrap(),
            buffer_start: 9_990,
            buffer_end: 10_060,
            num_rows: 10_060,
            count_known: false,
        });
        assert!(
            !state.num_rows_valid,
            "only the count can say where it ends"
        );
    }

    #[test]
    fn a_stale_stitch_keeps_the_rows_on_hand() {
        // A fill planned to run on from rows since replaced neither abuts what is held
        // nor shows the view: installing it would draw rows under the wrong numbers.
        const G: usize = 1_000_000;
        let lf = df!("a" => &[0i32]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[G; 10]);
        state.visible_rows = 40;
        let rows = |start: usize, end: usize| CollectResult {
            df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 10 * G,
            count_known: true,
        };
        assert!(state.scroll_to(G - 60));
        let request = state
            .prepare_async_collect(None)
            .expect("the end of group 0");
        state.apply_async_collect(rows(request.buffer_start, request.buffer_end));
        assert!(state.scroll_to(G - 20));
        let stitch = state.prepare_async_collect(None).expect("into group 1");
        assert_eq!(stitch.buffer_start, G);

        // Meanwhile a synchronous collect moved the view and replaced the buffer.
        assert!(state.scroll_to(500_000));
        state.apply_async_collect(rows(450_000, 550_000));
        state.needs_recollect = false;

        state.apply_async_collect(rows(stitch.buffer_start, stitch.buffer_end));
        assert_eq!(
            (state.buffered_start(), state.buffered_end()),
            (450_000, 550_000),
            "the rows on hand stay"
        );
        assert!(state.needs_recollect, "and a fill is asked for");
    }

    #[test]
    fn a_fill_that_holds_the_first_row_is_kept_when_the_view_grew() {
        // The terminal grew while a row group was downloading: the fill holds the view's
        // first row but not its last. It is installed, and the rest asked for, rather than
        // thrown away.
        const G: usize = 1_000_000;
        let lf = df!("a" => &[0i32]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[G; 10]);
        state.visible_rows = 40;
        assert!(state.scroll_to(G - 60));
        let request = state
            .prepare_async_collect(None)
            .expect("the end of group 0");
        assert!(request.buffer_end <= G);

        state.visible_rows = 120; // resized while the fetch was out
        state.needs_recollect = false;
        state.apply_async_collect(CollectResult {
            df: df!("a" => (request.buffer_start as i32..request.buffer_end as i32)
                .collect::<Vec<i32>>())
            .unwrap(),
            buffer_start: request.buffer_start,
            buffer_end: request.buffer_end,
            num_rows: 10 * G,
            count_known: true,
        });
        assert_eq!(
            (state.buffered_start(), state.buffered_end()),
            (request.buffer_start, request.buffer_end),
            "the downloaded rows are kept"
        );
        assert!(state.needs_recollect, "and the rest of the view is fetched");
    }

    #[test]
    fn the_byte_cap_governs_the_alignment() {
        // Rows of a kilobyte under a 64 MB budget allow 66k rows, fewer than the 100k row
        // cap; with 40k-row groups the first fill must be group 0 exactly, not a window
        // cut through group 1 that a later refill downloads again.
        const G: usize = 40_000;
        let lf = df!("a" => &["x"]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, Some(64), true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[G; 5]);
        state.set_column_widths(vec![("a".to_string(), 1_000)]);
        state.visible_rows = 40;
        let cap = state.byte_cap_rows();
        assert!(
            (G..2 * G).contains(&cap),
            "cap {cap} between one and two groups"
        );
        let rows = |start: usize, end: usize| CollectResult {
            df: df!("a" => (start..end).map(|i| "x".repeat(8 + i % 3)).collect::<Vec<_>>())
                .unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 5 * G,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, G));
        state.apply_async_collect(rows(0, G));

        assert!(state.scroll_to(G - 20));
        let request = state.prepare_async_collect(None).expect("into group 1");
        assert_eq!(request.buffer_start, G, "group 1 alone");
        assert!(request.buffer_end <= 2 * G);
    }

    #[test]
    fn a_new_base_is_measured_afresh() {
        // The width measured on a buffer of the old frame does not plan the new one.
        let big: Vec<String> = (0..100).map(|_| "z".repeat(2_000)).collect();
        let lf = df!("a" => &big, "b" => (0..100i32).collect::<Vec<i32>>())
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 10;
        state.defer_collect = true;
        state.apply_async_collect(CollectResult {
            df: df!("a" => &big, "b" => (0..100i32).collect::<Vec<i32>>()).unwrap(),
            buffer_start: 0,
            buffer_end: 100,
            num_rows: 100,
            count_known: true,
        });
        assert!(state.observed_bytes_per_row.is_some());
        state.query("select b".to_string());
        assert!(state.observed_bytes_per_row.is_none());
        assert_eq!(
            state.bytes_per_row(),
            4,
            "the narrow frame, from its schema"
        );
    }

    #[test]
    fn a_nested_column_takes_its_width_from_the_footer() {
        let schema = Schema::from_iter([Field::new(
            "l".into(),
            DataType::List(Box::new(DataType::Float64)),
        )]);
        let columns = vec!["l".to_string()];
        assert_eq!(estimate_bytes_per_row(&schema, &columns, &[]), 64);
        assert_eq!(
            estimate_bytes_per_row(&schema, &columns, &[("l".to_string(), 800)]),
            800
        );
    }

    #[test]
    fn a_reset_remote_scan_is_pristine_again() {
        // A query makes the frame a predicate over the object, so the row-group window
        // and footer count stand down; clearing it brings both back without a len().
        let lf = df!("a" => (0..100).collect::<Vec<i32>>()).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.set_remote_source();
        state.set_row_groups(&[60, 40]);
        assert!(state.remote_window());
        state.query("select a where a > 50".to_string());
        assert!(!state.remote_window());
        state.query(String::new());
        assert!(state.remote_window());
        assert_eq!(state.num_rows_if_valid(), Some(100));
    }

    #[test]
    fn binary_columns_are_stubbed_in_display_buffer() {
        // Binary columns must not be materialized into the display buffer (their blobs can be huge
        // and reading them is what stalls jump-to-end); the buffer holds a stub instead.
        let a = Series::new("a".into(), &[1i32, 2, 3]);
        let blob = Series::new("blob".into(), &["aaaa", "bbbb", "cccc"])
            .cast(&DataType::Binary)
            .unwrap();
        let lf = DataFrame::new_infer_height(vec![a.into(), blob.into()])
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 10;
        state.collect();

        let df = state.df.as_ref().expect("display df present");
        let col = df.column("blob").expect("blob column present in buffer");
        assert_eq!(
            col.dtype(),
            &DataType::String,
            "binary column should be stubbed (not read as binary)"
        );
        assert_eq!(col.str().unwrap().get(0).unwrap(), BINARY_STUB);
        // A non-binary column is untouched.
        assert_eq!(df.column("a").unwrap().dtype(), &DataType::Int32);
    }

    #[test]
    fn analysis_describe_stubs_binary_columns_without_reading_blobs() {
        // Describe (and the other analysis tools) route the frame through `binary_stub_exprs`
        // so binary blobs are never materialized — reading multi-GB blobs across partitions is
        // what froze the process. The binary column still appears, as a stub.
        let a = Series::new("a".into(), &[1i32, 2, 3]);
        let blob = Series::new("blob".into(), &["aaaa", "bbbb", "cccc"])
            .cast(&DataType::Binary)
            .unwrap();
        let lf = DataFrame::new_infer_height(vec![a.into(), blob.into()])
            .unwrap()
            .lazy();
        let state = DataTableState::new(lf, None, None, None, None, true).unwrap();

        let analysis_lf = state.lf.clone().select(state.binary_stub_exprs());
        let results =
            crate::statistics::compute_describe_from_lazy(&analysis_lf, 3, None, 0, false)
                .expect("describe should not fail on binary columns");

        let blob_stat = results
            .column_statistics
            .iter()
            .find(|c| c.name == "blob")
            .expect("binary column present in describe");
        // Stubbed to a constant string, so describe treats it as categorical and its min/max is
        // the stub — never the raw bytes.
        assert_eq!(blob_stat.dtype, DataType::String);
        let cat = blob_stat
            .categorical_stats
            .as_ref()
            .expect("stubbed binary column has categorical stats");
        assert_eq!(cat.min.as_deref(), Some(BINARY_STUB));
        assert_eq!(cat.max.as_deref(), Some(BINARY_STUB));
        // The numeric column is still described normally.
        let a_stat = results
            .column_statistics
            .iter()
            .find(|c| c.name == "a")
            .expect("numeric column present in describe");
        assert!(a_stat.numeric_stats.is_some());
    }

    #[test]
    fn trailing_overflow_binary_column_is_truncated() {
        // Binary columns render as text (e.g. b"...") and should be truncated like strings —
        // this is the EDGAR `txt_bytes` case where a wide Binary column was wrongly dropped.
        let table = DataTable::default();
        let a = Series::new("a".into(), &[1i32, 2, 3]);
        let bin = Series::new(
            "wide_bytes".into(),
            &["aaaaaaaaaa", "bbbbbbbbbb", "cccccccccc"],
        )
        .cast(&DataType::Binary)
        .unwrap();
        let df = DataFrame::new_infer_height(vec![a.into(), bin.into()]).unwrap();
        let area = Rect::new(0, 0, 8, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(
            shown, 2,
            "an overflowing binary column should be shown truncated"
        );
    }

    #[test]
    fn tiny_remaining_width_drops_overflow_string_column() {
        // Even a string column should not render a useless 1-2 char sliver.
        let table = DataTable::default();
        let df = df!(
            "abcd" => &[1i32, 2, 3],
            "next" => &["yyyy", "yyyy", "yyyy"],
        )
        .unwrap();
        // "abcd" is 4 wide; used_width becomes 5, leaving only 1 (< MIN) for "next".
        let area = Rect::new(0, 0, 5, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        let shown = table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert_eq!(shown, 1, "a sub-minimal sliver should not be shown");
    }

    #[test]
    fn more_columns_indicator_appears_and_tracks_scroll() {
        // 4 columns that can't all fit: a right-edge marker should signal off-screen columns,
        // and after scrolling right a left-edge marker should appear too.
        let mut state =
            DataTableState::new(create_large_test_lf(), None, None, None, None, true).unwrap();
        state.visible_rows = 3;
        state.collect();

        let area = Rect::new(0, 0, 6, 4); // narrow: not all 4 columns fit
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, &mut state);
        let header = header_row_string(&buf, area);
        // The arrows come from the glyph set for the locale, which is ASCII on Windows CI.
        let g = crate::glyphs::get();
        assert!(
            header.contains(g.arrow_right),
            "expected right indicator, header: {header:?}"
        );
        assert!(
            !header.contains(g.arrow_left),
            "should not show left indicator at offset 0: {header:?}"
        );

        // Scroll right: now columns exist both left and right of the viewport.
        state.scroll_right();
        let mut buf2 = Buffer::empty(area);
        DataTable::default().render(area, &mut buf2, &mut state);
        let header2 = header_row_string(&buf2, area);
        assert!(
            header2.contains(g.arrow_left),
            "expected left indicator after scroll: {header2:?}"
        );
    }
}
