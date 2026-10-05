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
        Block, Borders, Cell, HighlightSpacing, Padding, Paragraph, Row, StatefulWidget, Table,
        TableState, Widget,
    },
};

use crate::error_display::user_message_from_polars;
use crate::filter_modal::FilterStatement;
use crate::local_copy::RemoteObject;
use crate::numfmt::{self, CellFormatter, NumberFormatSettings};
use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec, ReshapeSource};
use crate::python_script::{SidebarFilter, Step, py_str};
use crate::query::{ParsedQuery, parse_query_over};
use crate::statistics::collect_lazy;
use crate::unfinished::{Claim, Writer};
use crate::widgets::column_paging::{ColumnMove, CursorMove, OnScreen, Room};
use crate::widgets::column_widths::{ColumnWidths, PageMeasure, WidthChoice};
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

/// `agg` over `values`, one cell of a pivot.
fn pivot_agg_expr(agg: PivotAggregation, values: Expr) -> Expr {
    match agg {
        PivotAggregation::Last => values.last(),
        PivotAggregation::First => values.first(),
        PivotAggregation::Min => values.min(),
        PivotAggregation::Max => values.max(),
        PivotAggregation::Avg => values.mean(),
        PivotAggregation::Med => values.median(),
        PivotAggregation::Std => values.std(1),
        PivotAggregation::Count => values.len(),
    }
}

/// The most columns a pivot may make. Past it the table, the schema and every view
/// of them slow to a crawl; a pivot on a column with this many values is almost
/// always a mistake (an id or a timestamp picked for Columns).
pub const PIVOT_COLUMN_LIMIT: usize = 10_000;

/// A pivot of the view as it was when planned, to be read off the UI thread.
pub struct PivotJob {
    view: LazyFrame,
    spec: PivotSpec,
    streaming: bool,
}

impl PivotJob {
    /// A pivot of `view`: the builder's preview runs one over a few rows in memory.
    pub(crate) fn new(view: LazyFrame, spec: PivotSpec, streaming: bool) -> Self {
        Self {
            view,
            spec,
            streaming,
        }
    }

    /// The pivoted frame, in one pass over the view.
    ///
    /// The lazy pivot has to be told its new columns before it runs, which would mean a
    /// distinct pass over the view and then the pivot over it again. Instead each cell is
    /// aggregated by a group-by on the index and pivot columns in one pass; the new
    /// columns are read off that result and the pivot runs over it in memory. The new
    /// columns come out alphabetical with a trailing `null` column, as the eager pivot
    /// ordered them, and index rows keep first-seen order.
    pub fn run(self) -> Result<DataFrame> {
        let on = self.spec.pivot_column.as_str();
        let value = self.spec.value_column.as_str();
        let index: Vec<PlSmallStr> = if self.spec.index.is_empty() {
            self.view
                .clone()
                .collect_schema()?
                .iter_names()
                .filter(|name| name.as_str() != on && name.as_str() != value)
                .cloned()
                .collect()
        } else {
            self.spec.index.iter().map(PlSmallStr::from).collect()
        };
        // `Expr::Column`, not `col`: a header may contain `*` or `^`, and names are
        // literal.
        let keys: Vec<Expr> = index
            .iter()
            .cloned()
            .chain([PlSmallStr::from(on)])
            .map(Expr::Column)
            .collect();
        let cells = collect_lazy(
            self.view.group_by_stable(keys).agg([pivot_agg_expr(
                self.spec.aggregation,
                Expr::Column(PlSmallStr::from(value)),
            )
            .alias(value)]),
            self.streaming,
        )?;
        let on_columns = cells
            .clone()
            .lazy()
            .select([Expr::Column(PlSmallStr::from(on))])
            .unique(None, UniqueKeepStrategy::Any)
            .sort([on], SortMultipleOptions::default().with_nulls_last(true))
            .collect()?;
        // Refused before the pivot builds them: the cells are already in memory, the
        // columns would be the expensive part.
        if on_columns.height() > PIVOT_COLUMN_LIMIT {
            return Err(color_eyre::eyre::eyre!(
                "Pivot would make {} columns from {on}; the limit is {}. Filter first, or pivot a column with fewer values",
                numfmt::group_chrome(on_columns.height()),
                numfmt::group_chrome(PIVOT_COLUMN_LIMIT),
            ));
        }
        let (cells, on_columns) = Self::pivot_dates_as_text(cells, on_columns, on)?;
        // One row per index and pivot value now, so `first` is that cell. A count sums
        // instead, so a pair with no rows counts 0 rather than null, as it always did.
        let cell = match self.spec.aggregation {
            PivotAggregation::Count => element().sum(),
            _ => element().first(),
        };
        let pivoted = cells
            .lazy()
            .pivot(
                by_name([on], true, false),
                Arc::new(on_columns),
                by_name(index, true, false),
                by_name([value], true, false),
                cell,
                true,
                PlSmallStr::from_static("_"),
                PivotColumnNaming::Auto,
            )
            .collect()?;
        Ok(pivoted)
    }

    /// Polars names the new columns by casting the pivot values to text, which
    /// panics on a date past the calendar. When one is there, the values become
    /// their text first, such a date its stored number, after they are ordered as
    /// dates. Both frames are already in memory.
    fn pivot_dates_as_text(
        cells: DataFrame,
        on_columns: DataFrame,
        on: &str,
    ) -> Result<(DataFrame, DataFrame)> {
        let values = on_columns.column(on)?.as_materialized_series();
        if crate::exact::calendar_without_out_of_range(values)?.is_none() {
            return Ok((cells, on_columns));
        }
        let text = |mut df: DataFrame| -> Result<DataFrame> {
            let values = df.column(on)?.as_materialized_series();
            let values = crate::past_calendar::cast_text(
                values,
                polars::chunked_array::cast::CastOptions::NonStrict,
            )?;
            df.with_column(values.into_column())?;
            Ok(df)
        };
        Ok((text(cells)?, text(on_columns)?))
    }
}

pub struct DataTableState {
    lf: LazyFrame,
    /// `lf` before its sort, when it has one. See [`DataTableState::analysis_lf`].
    unsorted_lf: Option<LazyFrame>,
    original_lf: LazyFrame,
    original_schema: Arc<Schema>,
    /// What the sidebar filters and sort are applied to: the active query's result (DSL,
    /// SQL or fuzzy), the last pivot/melt, or `original_lf` when there is none. The
    /// pipeline is original → query/reshape (`base_lf`) → filters → sort (`lf`) → column
    /// order (at collect). Filters therefore never discard the query.
    base_lf: LazyFrame,
    df: Option<DataFrame>,        // Scrollable columns dataframe
    locked_df: Option<DataFrame>, // Locked columns dataframe
    pub table_state: TableState,
    start_row: usize,
    pub visible_rows: usize,
    pub termcol_index: usize,
    /// The column cursor's column, by name, so it follows hide, reorder and freeze.
    /// `None` is the first column. See [`Self::current_column`].
    cursor_column: Option<String>,
    /// Where the cursor stood in `column_order` when placed: a column hidden from
    /// under it hands the cursor to the one now in its place.
    cursor_at: usize,
    /// The cursor may be off screen (the order, the frozen count or the room
    /// changed): the next draw brings it back, scrolling as little as it takes.
    reveal_cursor: bool,
    pub visible_termcols: usize,
    /// The scrolling side as last drawn, which a sideways page is planned in. `None`
    /// before the first draw.
    scroll_room: Option<Room>,
    /// Sideways moves waiting on the next draw to measure columns not drawn yet, in
    /// the order asked. See [`Self::scroll_columns`].
    column_moves: Vec<WaitingMove>,
    /// The pages `]` went, from and to, so `[` straight after goes back exactly.
    page_trail: Vec<(usize, usize)>,
    /// Which columns the last draw showed, while some are off screen.
    on_screen: Option<OnScreen>,
    /// Where the last frame drew the rows and columns, for a click.
    drawn: Option<DrawnTable>,
    error: Option<PolarsError>,
    pub suppress_error_display: bool, // When true, don't show errors in main view (e.g., when query input is active)
    schema: Arc<Schema>,
    num_rows: usize,
    /// When true, collect() skips the len() query.
    num_rows_valid: bool,
    /// The dataset's own row count, remembered from the last moment the frame was
    /// pristine. Lets the control bar say "417 of 1,000" under a filter or query
    /// without a second count; `None` until a pristine count has resolved.
    pristine_rows: Option<usize>,
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
    /// Taken afresh whenever `original_lf` is replaced. A checkpoint records it, so one
    /// taken over other data is never put back over this data.
    root_generation: u64,
    /// The local Parquet hive directory the data was loaded from, whose per-file footer
    /// counts sum to the exact row count while the frame is the scan as loaded
    /// (`is_pristine`) — far cheaper than a `len()` data scan over a huge/partitioned set.
    parquet_count_dir: Option<PathBuf>,
    /// What finding and reading this dataset cost.
    ///
    /// On the dataset rather than on the app, for the reason `dataset_generation` is
    /// bumped per dataset that reaches the screen rather than per open started: an open
    /// that fails leaves the last dataset up, and its figures have to stay with it. A
    /// meter the app held would by then be the failed load's.
    measurements: Arc<crate::measurements::Meter>,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    /// Per entry of `sort_columns`, whether that column runs descending. Always the
    /// same length as `sort_columns`.
    sort_descending: Vec<bool>,
    sort_ascending: bool,
    /// Last executed DSL query. At most one of the three `active_*` queries is set: running
    /// one clears the other two.
    active_query: String,
    /// Last executed SQL (Sql tab).
    active_sql_query: String,
    /// The leading columns the SQL in effect orders by, as named in its result, and
    /// whether each runs descending: the header's sort marks while the sidebar sorts
    /// nothing. Empty for an ORDER BY of an expression.
    query_order: Vec<(String, bool)>,
    /// Last executed fuzzy search (Fuzzy tab).
    active_fuzzy_query: String,
    column_order: Vec<String>,   // Order of columns for display
    locked_columns_count: usize, // Number of locked columns (from left)
    /// What the last layout made of the frozen columns: the count asked for, and how
    /// many of them fit frozen beside a usable scrolling column. The rest scroll until
    /// a wider window has room again; a different count asked for starts over.
    frozen_fit: (usize, usize),
    /// The width each column is drawn at, by column identity, so paging, reordering,
    /// hiding and opening a sidebar move nothing. Learned by the renderer from rows it
    /// formats anyway; not part of a rollback, since it describes columns, not a view.
    widths: ColumnWidths,
    /// The grouped view a drill-down left, restored exactly by `drill_up`.
    grouped: Option<GroupedView>,
    /// The rows behind a grouped query result, so Enter can drill from an aggregate.
    group_source: Option<GroupSource>,
    /// The last pivot/melt result, while one is in effect. SQL runs against it rather
    /// than the data as loaded (see `query_root`).
    reshaped_lf: Option<LazyFrame>,
    drilled_down_group_index: Option<usize>, // Index of the group we're viewing
    drilled_down_group_key: Option<Vec<String>>, // Key values of the drilled down group
    drilled_down_group_key_columns: Option<Vec<String>>, // Key column names of the drilled down group
    pages_lookahead: usize,
    pages_lookback: usize,
    max_buffered_rows: usize, // 0 = no limit
    max_buffered_mb: usize,   // 0 = no limit
    /// True for a scan of an object store, where a buffer fill is a ranged read of
    /// whole row groups. See `is_remote_source`.
    remote_source: bool,
    /// Where each row group of a remote Parquet object starts, with the total as the
    /// last entry, from its footer. See `record_row_groups`.
    row_group_offsets: Option<Vec<usize>>,
    /// The files of a remote dataset, when it is many. See `RemoteFiles`.
    remote_files: Option<RemoteFiles>,
    /// Each remote object the dataset reads, by URL, with its size and tag from the
    /// listing or the footer read that opened it. What a Data Quality local copy
    /// would fetch.
    remote_objects: Option<Arc<std::collections::HashMap<String, RemoteObject>>>,
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
    /// The data as loaded carries each row's place in the source in the hidden row
    /// index (lines), which `#` shows while the frame is the scan's.
    source_rows_at_open: bool,
    /// The sorted or filtered view numbers its rows itself, `#` being on and the data
    /// as loaded carrying no place of its own: a row index over the base, under the
    /// filters and sort. Taken only while `#` is on, because a row index between a
    /// scan and a filter keeps the filter from being pushed into the scan.
    view_numbered: bool,
    /// Lines still being indexed behind the first rows: the frames grow as they are.
    indexing: Option<Arc<crate::lines::Lines>>,
    /// The lines of several files, which `#` numbers by their line in their own file.
    numbering: Option<Arc<crate::lines::Lines>>,
    /// The dataset's row count from a sample of its footers, until it is counted.
    row_estimate: Option<crate::schema_union::RowEstimate>,
    /// The notes the lines gave when they opened, replaced once they are all indexed.
    indexing_notes: Vec<crate::notes::Note>,
    /// Whether the open guessed the lines were text, which their notes say.
    indexing_guessed: bool,
    /// Where each file's rows begin in the dataset, and the drift group of each file.
    /// Together they turn a row's place in the dataset into what its file was missing.
    drift_file_starts: Vec<usize>,
    drift_file_group: Vec<u32>,
    /// Rows in the dataset as the footers counted them, so the last file's length is
    /// known without asking what the view currently holds.
    drift_dataset_rows: usize,
    /// The dataset as its footers found it, kept beside the view because reading a
    /// column as text needs the types the files actually hold — which is the very
    /// thing the view no longer says.
    dataset_at_open: Option<crate::schema_union::DatasetSchema>,
    /// Columns being read as text from every file rather than as the type most rows
    /// have. Empty for a dataset as opened.
    read_as_text: Vec<PlSmallStr>,
    /// Each file's path or URL, in scan order, so a row can be traced to the file it
    /// came from and an export can name it.
    drift_files: Vec<String>,
    /// Set while the dataset is on screen from a footer or two and the rest are still
    /// to be read. Cleared when their answer joins. See [`FootersJoin`].
    footers_pending: Option<FootersJoin>,
    /// What datui noticed about the dataset, from the footers it had to read anyway.
    notes: Vec<crate::notes::Note>,
    /// Whether the Info panel has been opened since the notes were gathered. Belongs to
    /// the dataset, so opening another one offers its notes afresh.
    notes_seen: bool,
    /// The notes as the dataset was opened, so a reset and a drill up restore them.
    notes_at_open: Vec<crate::notes::Note>,
    /// Notes about the view rather than the dataset: what the filter and sort on
    /// screen are leaving out. Recomputed whenever either changes, so clearing them
    /// takes the note away with them.
    view_notes: Vec<crate::notes::Note>,
    /// Notes about the read itself rather than about what it found: which files this
    /// open passed over, and whether it is reading a lake table's plain files.
    ///
    /// Their own list because they are settled before a footer is read, and
    /// [`Self::notes`] is written from the footers when those land — so a note put
    /// there at open time would be overwritten by the dataset's own. They also outlive
    /// a reshape, which the footer notes do not: a query changes what is on screen, not
    /// which files were read to get it.
    open_notes: Vec<crate::notes::Note>,
    /// The lake format whose plain files this dataset is, if it is one. See
    /// [`crate::OpenOptions::read_as_plain_files_of`].
    not_the_table: Option<&'static str>,
    /// What a read through a format spec found: the spec, why, and its notes.
    format_read: Option<Arc<crate::formats::Read>>,
    /// What a read through a delimited spec found: units and metadata.
    delimited: Option<Arc<crate::delimited_spec::DelimitedRead>>,
    /// The fixed records the data as loaded is, while it still is: a window of a
    /// pristine view starts its columns at the window rather than decoding from row 0.
    fixed_window: Option<Arc<dyn crate::pushdown::Windowed>>,
    /// A source that runs the sidebar's filters and sort itself (a SQLite table), while
    /// the data as loaded is the root: see [`Self::pushed_view`].
    pushdown: Option<Arc<dyn crate::pushdown::Pushdown>>,
    /// Stops what the source runs when this state goes.
    source_hold: Option<crate::sqlite::Hold>,
    /// How the open reads the data. See [`crate::OpenOptions::read_mode`].
    read_mode: Option<crate::ReadMode>,
    /// The format the open read. See [`OpenFacts::read_as`].
    read_as: Option<crate::FileFormat>,
    /// The data was downloaded from a remote source before it was read.
    fetched: bool,
    /// What the file said besides its rows. See [`OpenFacts::detail`].
    detail: Option<Arc<crate::text_formats::Detail>>,
    /// Each loaded column's unit, from the file. See [`OpenFacts::units`].
    file_units: Arc<Vec<(String, String)>>,
    /// Uncompressed bytes per row of each column, from the Parquet footer, for
    /// `bytes_per_row` before anything has been collected.
    column_bytes: Vec<(String, usize)>,
    /// Bytes per row of the last buffer collected, which outranks the estimate from
    /// the schema.
    observed_bytes_per_row: Option<usize>,
    buffered_start_row: usize,
    buffered_end_row: usize,
    /// Full buffered DataFrame (all columns in column_order) for the current buffer range.
    /// When set, column scroll (scroll_left/scroll_right) only re-slices columns without re-collecting from LazyFrame.
    buffered_df: Option<DataFrame>,
    proximity_threshold: usize,
    /// The first row of the last page drawn whole. See [`Self::start_to_draw`].
    drawn_start: usize,
    row_numbers: bool,
    row_start_index: usize,
    /// Last applied pivot spec, if current lf is result of a pivot. Used for views.
    last_pivot_spec: Option<PivotSpec>,
    /// Last applied melt spec, if current lf is result of a melt. Used for views.
    last_melt_spec: Option<MeltSpec>,
    /// The query, filters and sort the pivot or melt in effect ran over, for a view to
    /// replay before it. `None` while none is in effect, or when it ran over the data as
    /// loaded.
    reshape_source: Option<ReshapeSource>,
    /// How `base_lf` was built from the data as loaded, step by step, for Copy as
    /// Python. Set with every new base; empty for the data as loaded.
    base_steps: Vec<Step>,
    /// What the open did to the rows its reader gave, as Python method calls: names
    /// trimmed, text columns typed.
    read_python: Vec<String>,
    /// What the read of several files has to say of them: files passed over, columns
    /// not every file has. Carried to the dataset's notes.
    read_notes: Vec<crate::notes::Note>,
    /// Each column's unit, from the first of several files read through a spec that
    /// has the column; `None` when the first file's header said them all.
    read_units: Option<Vec<(String, String)>>,
    /// The columns the read gave a type, and the frame before it did.
    typing: Typing,
    /// The notes on the values the types made null, once counted.
    unfit_notes: Option<Vec<crate::notes::Note>>,
    /// The view's own column types and columns made from others, in the order asked:
    /// a step of `lf`, before the filters, as a spec's `[columns]` would say them.
    column_changes: Vec<crate::column_types::ColumnChange>,
    /// Bumped with every change to `column_changes`, so a count of what they made null
    /// answers for the changes it was asked about.
    changes_version: u64,
    /// The notes on the values the view's types made null: for the version counted.
    changes_unfit: Option<(u64, Vec<crate::notes::Note>)>,
    /// Steps of a saved view whose columns this data does not have.
    changes_dropped: Vec<crate::notes::Note>,
    /// How `reshaped_lf` was built, while there is one: what SQL runs over.
    reshape_steps: Option<Vec<Step>>,
    /// Which loaded column each column of the base is (see [`Lineage`]).
    lineage: Lineage,
    /// The same for the pivot or melt in effect, which SQL runs against.
    reshape_lineage: Lineage,
    /// When set, dataset was loaded with hive partitioning; partition column names for Info panel and predicate pushdown.
    partition_columns: Option<Vec<String>>,
    /// When set, decompressed CSV was written to this temp file; kept alive so the file exists for lazy scan.
    /// Shared with any view that scans it, and removed with the last.
    decompress_temp_file: Option<Arc<Decompressed>>,
    /// The downloaded remote file this dataset was opened from, held while it is scanned.
    download: Option<crate::download::TempDownload>,
    /// The files a GPS log was read into, which the frame scans; held as `download` is.
    converted: Vec<crate::download::TempDownload>,
    /// The file's other tables, as `--table` names them; see [`OpenFacts::other_tables`].
    other_tables: Vec<String>,
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    polars_streaming: bool,
    /// When true, `collect()` / `apply_transformations()` skip the blocking collect.
    /// The caller is responsible for triggering an async collect afterwards.
    defer_collect: bool,
    /// Set by the render code when `visible_rows` changes. The App event loop checks this
    /// after each render and triggers an async collect if needed.
    pub needs_recollect: bool,
    /// The watcher of the file this dataset follows (`--follow`), while it does.
    follow: Option<crate::follow::Follow>,
    /// For a followed view that filters or sorts the file's rows: points where the
    /// view's rows before a file row are known (view rows, file row), ascending, for
    /// the count generation they hold for. The next count reads on from the last; a
    /// filtered window from the one before it.
    follow_known: Option<(u64, Vec<(usize, usize)>)>,
    /// The sample this view's rows are, and the view it was drawn from, while the
    /// view has one: the step between the source and the query.
    sampled: Option<Box<Sampled>>,
}

/// A view's sample: the step between the source and the query. The view's frames
/// scan [`Self::frame`], the chunks kept so far, which grows as the draw goes on.
pub struct Sampled {
    /// The view the sample was drawn from, as it stood: what clearing the sample
    /// returns to.
    source: Box<DataTableState>,
    sample: crate::sampling::Sample,
    rows: Arc<crate::table_sample::SampleRows>,
    /// The frame the view's plans scan: the chunks taken so far, on their buffers.
    frame: Arc<DataFrame>,
    /// Drawn from the view's query or filters, which the sample then stands for,
    /// rather than from the source under them.
    through: bool,
    /// What the draw read, once it ended; `None` while it runs.
    drawn: Option<crate::table_sample::Drawn>,
}

impl Sampled {
    pub fn sample(&self) -> &crate::sampling::Sample {
        &self.sample
    }

    /// The view the sample was drawn from.
    pub fn source(&self) -> &DataTableState {
        &self.source
    }

    /// Whether the sample was drawn from the view's query or filters.
    pub fn through(&self) -> bool {
        self.through
    }

    /// Whether these are the rows `rows` holds: the sample a draw fills.
    pub(crate) fn holds(&self, rows: &Arc<crate::table_sample::SampleRows>) -> bool {
        Arc::ptr_eq(&self.rows, rows)
    }

    /// Whether rows are still arriving.
    pub fn drawing(&self) -> bool {
        self.drawn.is_none()
    }

    pub fn drawn(&self) -> Option<&crate::table_sample::Drawn> {
        self.drawn.as_ref()
    }

    /// Rows the view has taken of the sample.
    pub fn rows(&self) -> usize {
        self.frame.height()
    }

    /// Bytes the sample's rows take.
    pub fn bytes(&self) -> usize {
        self.rows.bytes()
    }

    /// Why memory stopped the draw, if it did.
    pub fn stopped(&self) -> Option<String> {
        self.rows.stopped()
    }

    /// The footer's segment: `sample 100,000 of 36.8M`, `sample 1,234+` while it is
    /// drawn, `sample about 100,000 of 36.8M` when kept row by row by chance.
    pub fn label(&self) -> String {
        let rows = crate::numfmt::group_chrome(self.rows());
        let Some(drawn) = &self.drawn else {
            return format!("sample {rows}+");
        };
        let about = if drawn.about { "about " } else { "" };
        let cut = if drawn.cut { ", stopped" } else { "" };
        match drawn.total {
            Some(total) if total > self.rows() => {
                format!(
                    "sample {about}{rows} of {}{cut}",
                    crate::discover::format_rows(total)
                )
            }
            _ => format!("sample {rows}{cut}"),
        }
    }
}

/// What string-column inference may turn a column into, besides Time.
#[derive(Clone, Copy)]
struct StringTypes {
    /// Date and Datetime.
    dates: bool,
    /// Duration, Int64 and Float64, and trimming the columns that stay text.
    numbers: bool,
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

/// Which loaded column each shown column is, as (shown name, loaded name), so a
/// delimited spec's unit stays on a column that holds the loaded values, renamed or
/// not, and never lands on a computed column that reuses a name. `None` while every
/// column is the loaded column of its name.
type Lineage = Option<Arc<Vec<(String, String)>>>;

/// `pairs`, each a shown name and the name of a column of a frame whose lineage is
/// `root`, traced back to the loaded columns. A name `root` does not know is dropped.
fn traced(root: &Lineage, pairs: Vec<(String, String)>) -> Lineage {
    let pairs = match root {
        None => pairs,
        Some(root) => pairs
            .into_iter()
            .filter_map(|(shown, from)| {
                root.iter()
                    .find(|(name, _)| *name == from)
                    .map(|(_, loaded)| (shown, loaded.clone()))
            })
            .collect(),
    };
    Some(Arc::new(pairs))
}

/// Each of `exprs` that is a column unchanged, renamed or not: its output name and
/// the column's.
fn passed_through(exprs: &[Expr]) -> Vec<(String, String)> {
    exprs
        .iter()
        .filter_map(|e| {
            let Expr::Column(from) = e.clone().meta().undo_aliases() else {
                return None;
            };
            let shown = e.clone().meta().output_name().ok()?;
            Some((shown.to_string(), from.to_string()))
        })
        .collect()
}

/// The grouped view and the pipeline state that produced it, saved by a drill-down so
/// filters and sort inside the group work on the group and `drill_up` restores the
/// grouped view as it was.
#[derive(Clone)]
struct GroupedView {
    lf: LazyFrame,
    base_lf: LazyFrame,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_descending: Vec<bool>,
    sort_ascending: bool,
    /// Whether `lf` carries the hidden drift column, and what its groups mean. Saved
    /// with the frame so drilling back up restores the cells it explains, along with
    /// the notes that explain them.
    drift: bool,
    drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    /// Whether `lf` numbers its rows itself (`#`).
    view_numbered: bool,
    notes: Vec<crate::notes::Note>,
    group_source: Option<GroupSource>,
    /// Where the user was, so coming back puts the cursor on the group drilled into
    /// with the key columns still frozen.
    column_order: Vec<String>,
    locked_columns_count: usize,
    start_row: usize,
    termcol_index: usize,
    cursor_column: Option<String>,
    selected: Option<usize>,
    /// Drilled into from Value Counts rather than from a grouped row.
    by_value: bool,
    /// How `base_lf` was built, for Copy as Python.
    base_steps: Vec<Step>,
    lineage: Lineage,
}

/// The rows a grouped result was computed from and how its keys were computed, so a
/// drill-down can find a group's rows even when the result holds only aggregates.
/// Recorded by the query that grouped rather than inferred from the result's columns,
/// which may be renamed or computed.
#[derive(Clone)]
struct GroupSource {
    /// The rows before grouping, after any filter the query applied first.
    rows: LazyFrame,
    /// Each key's column in the result, with the expression that computes it from `rows`.
    keys: Vec<(PlSmallStr, Expr)>,
    /// Columns `rows` carries only to compute keys, left out of a drill.
    scratch: Vec<PlSmallStr>,
    /// Whether the result's list columns are each group's rows, as a `by` query's are.
    /// A SQL result's lists are values it computed, such as `ARRAY_AGG`.
    rows_in_lists: bool,
    /// The same as Copy as Python steps: how `rows` was built, and each key as
    /// Python code, aliases undone. None where the script cannot say.
    python_rows: Option<Vec<Step>>,
    python_keys: Vec<Option<String>>,
    /// Which loaded column each column of `rows` is.
    lineage: Lineage,
}

/// One field the row inspector lists: a column of the frame on screen.
#[derive(Debug, Clone, PartialEq)]
pub struct InspectField {
    pub name: String,
    pub dtype: DataType,
    /// Hidden from the table, so not among the rows read for it.
    pub hidden: bool,
}

impl InspectField {
    /// Whether the table's rows hold this field's value: a hidden column is not
    /// read for them, and a binary one is read as a stub.
    pub fn buffered(&self) -> bool {
        !self.hidden && !matches!(self.dtype, DataType::Binary)
    }
}

/// The selected row as the buffer holds it; see [`DataTableState::inspect_row`].
#[derive(Clone)]
pub struct InspectRow {
    /// The row's index in the view.
    pub row: usize,
    /// The frame it is a row of (`len_generation`), which a sort, a filter or a
    /// query replaces.
    pub frame: u64,
    /// The row number the table shows.
    pub display_row: usize,
    /// The row, one column per table column, raw.
    pub values: DataFrame,
    /// Which drift group its file is in, when the files differ.
    pub drift_group: Option<u32>,
}

/// What a null means where the files of a dataset differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NullKind {
    Null,
    Absent,
    Conflict,
}

/// The row a drill into a group reads, from [`DataTableState::drill_row`].
pub enum DrillRow {
    /// Taken from the rows on screen.
    Buffered(DataFrame),
    /// Not on hand: the one-row frame to collect, off the UI thread.
    Read(Box<LazyFrame>),
}

/// A group's rows, drilled from its row of a grouped view.
struct GroupRows {
    lf: LazyFrame,
    /// The key columns of the grouped view and the row's values in them, as text.
    key_columns: Vec<String>,
    key_values: Vec<String>,
    /// Columns of `lf` that hold the keys as they stand, to lead the view.
    lead: Vec<String>,
    /// How `lf` is built, as Copy as Python steps.
    steps: Vec<Step>,
    /// Which loaded column each column of `lf` is.
    lineage: Lineage,
}

/// The view as it stood before a query or view replaced it: a checkpoint. A query plans
/// without reading anything and can still fail once it runs — a value that will not
/// cast — and then the table goes back to this, rows and all, rather than keep a
/// frame that fails on every scroll. Frames and buffers are shared, not copied.
///
/// Taken by [`DataTableState::rollback_point`] or [`DataTableState::try_transition`],
/// put back by [`DataTableState::roll_back`].
pub struct ViewRollback {
    /// The data as loaded when this was taken; see [`DataTableState::roll_back`].
    root_generation: u64,
    /// A count of this frame that came back after it was replaced, to return with it.
    counted: Option<CountedRows>,
    drawn_start: usize,
    lf: LazyFrame,
    unsorted_lf: Option<LazyFrame>,
    base_lf: LazyFrame,
    df: Option<DataFrame>,
    locked_df: Option<DataFrame>,
    table_state: TableState,
    start_row: usize,
    termcol_index: usize,
    cursor_column: Option<String>,
    cursor_at: usize,
    schema: Arc<Schema>,
    num_rows: usize,
    num_rows_valid: bool,
    len_generation: u64,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    sort_descending: Vec<bool>,
    sort_ascending: bool,
    active_query: String,
    active_sql_query: String,
    query_order: Vec<(String, bool)>,
    active_fuzzy_query: String,
    column_order: Vec<String>,
    locked_columns_count: usize,
    /// The frozen fit `df` was sliced for; a later layout may have changed it.
    frozen_fit: (usize, usize),
    grouped: Option<GroupedView>,
    reshaped_lf: Option<LazyFrame>,
    last_pivot_spec: Option<PivotSpec>,
    last_melt_spec: Option<MeltSpec>,
    reshape_source: Option<ReshapeSource>,
    base_steps: Vec<Step>,
    reshape_steps: Option<Vec<Step>>,
    lineage: Lineage,
    reshape_lineage: Lineage,
    group_source: Option<GroupSource>,
    drilled_down_group_index: Option<usize>,
    drilled_down_group_key: Option<Vec<String>>,
    drilled_down_group_key_columns: Option<Vec<String>>,
    drift_column_present: bool,
    view_numbered: bool,
    drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    notes: Vec<crate::notes::Note>,
    notes_seen: bool,
    view_notes: Vec<crate::notes::Note>,
    column_changes: Vec<crate::column_types::ColumnChange>,
    changes_version: u64,
    changes_dropped: Vec<crate::notes::Note>,
    observed_bytes_per_row: Option<usize>,
    buffered_start_row: usize,
    buffered_end_row: usize,
    buffered_df: Option<DataFrame>,
}

impl ViewRollback {
    /// A background count of frame `len_generation` came back while this checkpoint
    /// was waiting. Kept when the frame is the one this restores, so the rows and the
    /// count return together; returns whether it was.
    pub fn count_landed(
        &mut self,
        len_generation: u64,
        rows: usize,
        file_row_groups: Option<&[Vec<usize>]>,
    ) -> bool {
        let ours = len_generation == self.len_generation;
        if ours {
            self.counted = Some(CountedRows {
                rows,
                file_row_groups: file_row_groups.map(<[_]>::to_vec),
            });
        }
        ours
    }
}

/// A row count read in the background: the total, and for a remote dataset of many
/// files, the rows in each row group of each file.
struct CountedRows {
    rows: usize,
    file_row_groups: Option<Vec<Vec<usize>>>,
}

/// The query bar a result came from, with its text. At most one is active at a time.
enum ActiveQuery {
    Dsl(String),
    #[cfg(feature = "sql")]
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
    /// How the worker fits the rows it reads to the buffer: [`FillPlan::fit`].
    pub plan: FillPlan,
}

/// What the worker that reads a fill needs to make it the buffer: the rows on hand it
/// runs on from or up to, the view, and the caps, as they were when it was planned.
///
/// A trim copies the rows it keeps when a slice would keep the fill allocated behind
/// them (see [`trim_rows`]): up to the byte budget, too long for the UI thread (#483).
/// The worker does it, and `apply_async_collect` installs what it hands back as it is.
pub struct FillPlan {
    buffer_start: usize,
    buffer_end: usize,
    num_rows: usize,
    count_known: bool,
    /// Lines were still being indexed when the read was planned: a short read ends
    /// where the indexing had got to, not the file.
    indexing: bool,
    /// The rows on hand and their first row, when the fill is planned to be stitched
    /// on to them. Shared, not copied.
    held: Option<(DataFrame, usize)>,
    view_start: usize,
    view_len: usize,
    max_rows: usize,
    max_mb: usize,
}

impl FillPlan {
    /// Make the buffer of `df`, the rows read for the planned range: stitched on to
    /// the rows on hand when it runs on from them or up to them, then cut to the caps
    /// around the view.
    pub fn fit(mut self, df: DataFrame) -> CollectResult {
        let returned = df.height();
        let bytes_per_row = (returned > 0).then(|| (df.estimated_size() / returned).max(1));
        // A shape mismatch (the columns changed underneath) keeps the fetched rows alone.
        let (df, start, seam) = match self.held.take() {
            Some((mut held, held_start)) if held_start + held.height() == self.buffer_start => {
                let seam = held.height();
                match held.vstack_mut(&df) {
                    Ok(_) => (held, held_start, Some(seam)),
                    Err(_) => (df, self.buffer_start, None),
                }
            }
            Some((held, held_start))
                if returned > 0 && self.buffer_start + returned == held_start =>
            {
                match df.vstack(&held) {
                    Ok(joined) => (joined, self.buffer_start, Some(returned)),
                    Err(_) => (df, self.buffer_start, None),
                }
            }
            _ => (df, self.buffer_start, None),
        };
        let (df, start) = self.cut_to_caps(df, start, seam);
        CollectResult {
            df,
            start,
            returned,
            bytes_per_row,
            buffer_start: self.buffer_start,
            buffer_end: self.buffer_end,
            num_rows: self.num_rows,
            count_known: self.count_known,
            indexing: self.indexing,
        }
    }

    /// Cut `df`, spanning `[start, start + df.height())`, to the row cap and the byte
    /// budget. The rows kept are centered on the view rather than taken from the head:
    /// a jump near the end of the dataset would otherwise drop exactly the rows the
    /// view needs. Returns the rows kept and their first row.
    ///
    /// The budget bounds the rows held between collects, not the collect itself: the
    /// fill, the operators upstream of it and an eager source frame all take memory of
    /// their own.
    fn cut_to_caps(&self, df: DataFrame, start: usize, seam: Option<usize>) -> (DataFrame, usize) {
        let total = df.height();
        if total == 0 {
            return (df, start);
        }
        // The row cap as well: a row group stitched on to the rows on hand can run over it.
        let mut max_rows = total;
        if self.max_rows > 0 {
            max_rows = max_rows.min(self.max_rows);
        }
        if self.max_mb > 0 {
            let bytes_per_row = (df.estimated_size() / total).max(1);
            max_rows = max_rows.min(self.max_mb * 1024 * 1024 / bytes_per_row);
        }
        let max_rows = max_rows.max(1);
        if max_rows >= total {
            return (df, start);
        }
        let view_off = self.view_start.saturating_sub(start).min(total);
        let view_len = self.view_len.max(1).min(total);
        let view_center = view_off + view_len / 2;
        let mut keep_start = view_center.saturating_sub(max_rows / 2);
        if keep_start + max_rows > total {
            keep_start = total - max_rows;
        }
        let kept = max_rows.min(total - keep_start);
        (trim_rows(df, keep_start, kept, seam), start + keep_start)
    }
}

/// Builds a scan of some of a dataset's files, as the full scan reads them, with the
/// columns named in the second argument read as text from every file rather than as
/// the type most rows have.
pub type FileScan = Arc<dyn Fn(&[String], &[PlSmallStr]) -> PolarsResult<LazyFrame> + Send + Sync>;
/// Counts the rows in each row group of every file of a dataset. Blocks.
pub type FileCounter = Arc<
    dyn Fn(&Arc<crate::schema_union::FooterProgress>) -> Result<Vec<Vec<usize>>, String>
        + Send
        + Sync,
>;
/// Reads every footer of a dataset that opened from a couple of them, and returns what
/// they say. `None` when they could not be read, in which case the dataset stays as it
/// opened. Blocks, and counts itself off against the progress it is given.
pub type FootersJoin =
    Arc<dyn Fn(&Arc<crate::schema_union::FooterProgress>) -> Option<FootersFound> + Send + Sync>;
/// What reading every footer turned up, and everything built from it that the dataset
/// has to be given together — the schema and the scans that read at that schema.
pub struct FootersFound {
    /// Every column every file has, and which files disagree about what.
    pub dataset: crate::schema_union::DatasetSchema,
    /// The scan that reads the dataset whole.
    pub lf: LazyFrame,
    /// Each file's rows, in scan order.
    pub file_rows: Vec<usize>,
    /// Every file listed, in scan order — including any whose footer would not read.
    /// The dataset's per-file findings index this, so it is the whole list.
    pub files: Vec<String>,
    /// Each file's row groups, or empty if a footer would not parse.
    pub row_groups: Vec<Vec<usize>>,
    /// How to read part of a remote dataset rather than all of it. `None` for one that
    /// does not read by file.
    pub remote: Option<RemoteRead>,
    /// The row count the footers read say, when they were a sample.
    pub estimate: Option<crate::schema_union::RowEstimate>,
}

/// How a remote dataset reads some of its files, as the pass behind an open found them.
///
/// The three travel together because they describe one list. The scan is built at a
/// schema — the one the dataset opened with has never heard of the columns this pass
/// found — and the counter answers one entry per file it was given, which has to be the
/// same list `urls` holds or the answer is dropped on a length check and the dataset
/// never learns its own size.
pub struct RemoteRead {
    /// The files that will open, which is not every file listed.
    pub urls: Vec<String>,
    pub scan: FileScan,
    pub count: FileCounter,
}

impl From<RemoteRead> for RemoteFiles {
    fn from(read: RemoteRead) -> Self {
        RemoteFiles {
            urls: Arc::new(read.urls),
            scan: read.scan,
            count: read.count,
            offsets: None,
        }
    }
}

/// A compressed CSV's decompressed copy, then the open's claim on it: dropped in that
/// order, so the claim goes only once the file has. See [`crate::unfinished`].
struct Decompressed {
    file: NamedTempFile,
    _claim: Claim,
}

impl Decompressed {
    fn path(&self) -> &Path {
        self.file.path()
    }
}

/// A dataset of many files, and how to read only some of them: a remote one, or a
/// local Hive directory once every footer is known.
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

/// What an open learned about a dataset besides its frame and schema: given to the
/// state once, by [`DataTableState::with_open`], so the count, the row groups, the
/// files and the notes all describe the same open. Each field's default means the open
/// did not find it.
#[derive(Default)]
pub struct OpenFacts {
    /// A scan of an object store in place: a buffer is one window of whole row groups.
    pub remote_source: bool,
    /// Each file's row groups in scan order, from the footers; one entry for a single
    /// object. Gives the count. With `remote_files`, one entry per file it lists.
    pub row_groups: Vec<Vec<usize>>,
    /// The files of a remote dataset of many, and how to read some of them.
    pub remote_files: Option<RemoteFiles>,
    /// Each remote object the dataset reads, as the listing or footer found it.
    pub remote_objects: Vec<RemoteObject>,
    /// What the footers said about a many-file dataset's columns.
    pub dataset: Option<DatasetAtOpen>,
    /// The pass that reads the rest of the footers, for a dataset opened from a few.
    pub footers_pending: Option<FootersJoin>,
    /// Each column's uncompressed bytes per row, from the footers.
    pub column_bytes: Vec<(String, usize)>,
    /// The local Parquet hive directory whose footers sum to the count.
    pub parquet_count_dir: Option<PathBuf>,
    /// What finding and reading the dataset cost.
    pub measurements: Arc<crate::measurements::Meter>,
    /// What the open itself has to say. See [`DataTableState::open_notes`].
    pub open_notes: Vec<crate::notes::Note>,
    /// The lake format whose plain files this dataset is. See
    /// [`DataTableState::not_the_table`].
    pub not_the_table: Option<&'static str>,
    /// What a read through a format spec found.
    pub format_read: Option<Arc<crate::formats::Read>>,
    /// What a read through a delimited spec found.
    pub delimited: Option<Arc<crate::delimited_spec::DelimitedRead>>,
    /// The downloaded file the frame scans, held for as long as the state lives.
    pub download: Option<crate::download::TempDownload>,
    /// The files a GPS log was read into, which the frame scans.
    pub converted: Vec<crate::download::TempDownload>,
    /// The file's other tables, each as `--table` names it with how many rows it holds
    /// where that is known, for the Info panel's Schema tab. Empty for a file of one.
    pub other_tables: Vec<String>,
    /// A source that runs the sidebar's filters and sort itself: a SQLite table.
    pub pushdown: Option<Arc<dyn crate::pushdown::Pushdown>>,
    /// What stops that source's statements when the dataset goes.
    pub hold: Option<crate::sqlite::Hold>,
    /// How the open reads the data. See [`crate::OpenOptions::read_mode`].
    pub read_mode: Option<crate::ReadMode>,
    /// The format the open read the data as, after sniffing and spec matching: what
    /// the scan chose, which a file's name may not say. Copy as Python and the export
    /// default follow it.
    pub read_as: Option<crate::FileFormat>,
    /// The data was downloaded from a remote source before it was read: not a local
    /// stream's conversion or standard input's spool, which are held as downloads are.
    pub fetched: bool,
    /// What the file said besides its rows, for the Info panel.
    pub detail: Option<Arc<crate::text_formats::Detail>>,
    /// Rows read straight from a reader that decodes them from the file (a NumPy
    /// array, an audio file's frames), and how many it holds: a page deep in the table,
    /// and the count, need no row index.
    pub records: Option<(Arc<dyn crate::pushdown::Windowed>, usize)>,
    /// Each column's unit, where the file says one.
    pub units: Vec<(String, String)>,
    /// Lines still being indexed behind the first rows: the frames grow as they are.
    pub indexing: Option<Arc<crate::lines::Lines>>,
    /// The lines of several files, which `#` numbers by their line in their own file.
    pub numbering: Option<Arc<crate::lines::Lines>>,
    /// The columns the read gave a type, for the count of what did not fit.
    pub typing: Typing,
}

/// The footers' account of a dataset of many files.
pub struct DatasetAtOpen {
    pub schema: crate::schema_union::DatasetSchema,
    /// Each file's row count in scan order; empty unless every one is known, which is
    /// when the scan numbers its rows.
    pub file_rows: Vec<usize>,
    /// Every file's path or URL, in scan order.
    pub files: Vec<String>,
}

/// The rows an export writes. See [`DataTableState::export_frame`].
pub struct ExportFrame {
    lf: LazyFrame,
    files: Option<SourceFiles>,
}

impl ExportFrame {
    /// Rows that are not the view's, such as a column's value counts.
    pub fn of(lf: LazyFrame) -> Self {
        Self { lf, files: None }
    }
}

/// The dataset's files in scan order, and the row each starts at.
struct SourceFiles {
    names: Arc<Vec<String>>,
    starts: Arc<Vec<usize>>,
}

impl ExportFrame {
    /// The name of the column an export adds when asked to say where each row is from.
    pub const SOURCE_FILE_COLUMN: &'static str = "source_file";

    /// The plan. Naming the files reads the schema, which may resolve the scan, so
    /// this belongs off the UI thread.
    ///
    /// The names are mapped from the row index batch by batch, so a streamed export
    /// still never holds every row.
    pub fn into_lazy(self) -> PolarsResult<LazyFrame> {
        let Some(SourceFiles { names, starts }) = self.files else {
            return Ok(self.lf);
        };
        let mut lf = self.lf;
        let schema = lf.collect_schema()?;
        let name = Self::free_name(schema.iter_names().map(|n| n.as_str()));
        let index = crate::schema_union::DRIFT_COLUMN;
        let file_of = move |rows: Column| -> PolarsResult<Column> {
            let rows = rows.strict_cast(&DataType::UInt64)?;
            let named: StringChunked = rows
                .u64()?
                .iter()
                .map(|row| {
                    let row = row? as usize;
                    let file = starts
                        .partition_point(|&start| start <= row)
                        .saturating_sub(1);
                    names.get(file).map(String::as_str)
                })
                .collect();
            Ok(named.with_name(rows.name().clone()).into_column())
        };
        // Added last, after the dataset's own columns, as the index comes off.
        Ok(lf
            .with_column(
                col(index)
                    .map(file_of, |_, field| {
                        Ok(Field::new(field.name().clone(), DataType::String))
                    })
                    .alias(name),
            )
            .drop(by_name([index], true, false)))
    }

    /// A name for the source-file column that no column already has.
    ///
    /// `source_file` is a name a dataset may well use itself — a directory of per-file
    /// extracts is exactly this feature's audience — and adding a column by a name
    /// already present replaces it, silently, in the file the user takes away.
    fn free_name<'a>(taken: impl Iterator<Item = &'a str>) -> String {
        let taken: HashSet<&str> = taken.collect();
        std::iter::once(Self::SOURCE_FILE_COLUMN.to_string())
            .chain((1..).map(|n| format!("{}_{n}", Self::SOURCE_FILE_COLUMN)))
            .find(|candidate| !taken.contains(candidate.as_str()))
            .expect("some suffix is free")
    }
}

/// Result of a background buffer load, made by [`FillPlan::fit`] on the worker and
/// installed as it is by `apply_async_collect()`.
pub struct CollectResult {
    /// The buffer: the rows read, stitched and cut to the caps.
    df: DataFrame,
    /// The first row of `df`.
    start: usize,
    /// Rows the read returned, before the stitch and the cut.
    returned: usize,
    /// Bytes per row of the rows read, to plan the next fill by.
    bytes_per_row: Option<usize>,
    /// The range the read was planned for.
    buffer_start: usize,
    buffer_end: usize,
    num_rows: usize,
    /// See `CollectRequest::count_known`.
    count_known: bool,
    /// See `FillPlan::indexing`.
    indexing: bool,
}

impl CollectResult {
    /// The rows read, as the buffer will hold them.
    pub(crate) fn rows(&self) -> &DataFrame {
        &self.df
    }
}

/// Rows the display buffer may hold when `performance.max_buffered_rows` is not set. Also
/// the window a remote scan buffers when the cap is switched off.
pub const DEFAULT_MAX_BUFFERED_ROWS: usize = 100_000;

/// Seeds `DataTableState::len_generation`. Unique per state, so a row count spawned
/// for one dataset can never be mistaken for a valid result for another.
static NEXT_LEN_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_len_generation() -> u64 {
    NEXT_LEN_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

/// The `[start, end)` row ranges of the files flagged in `conflicts`, merged where
/// they touch.
///
/// `starts[i]` is where file `i`'s rows begin in the dataset and `total` is how many
/// rows the dataset has, so the last file's end is known without a start after it.
///
/// Runs, not files: a vendor who wrote a column as text for a month wrote a
/// contiguous stretch of files, and the predicate built from this is one term per run
/// however many files the stretch holds. Merging changes no row's fate — three
/// touching ranges keep out exactly what one joined range does — which is why it is
/// pinned here, where the runs themselves can be counted, rather than by a test of
/// what ends up on screen.
///
/// Post-conditions, for any `starts` ascending and `conflicts` of the same length:
/// - a row is in some run exactly when the file it belongs to is flagged;
/// - the runs are ascending and no two of them touch or overlap;
/// - a file of no rows produces no run of its own, and never splits one.
fn conflicting_row_runs(starts: &[usize], total: usize, conflicts: &[bool]) -> Vec<(usize, usize)> {
    let mut runs: Vec<(usize, usize)> = Vec::new();
    for (file, start) in starts.iter().copied().enumerate() {
        if !conflicts.get(file).copied().unwrap_or(false) {
            continue;
        }
        let end = starts.get(file + 1).copied().unwrap_or(total);
        match runs.last_mut() {
            Some(last) if last.1 == start => last.1 = end,
            _ => runs.push((start, end)),
        }
    }
    // A file of no rows leaves an empty range, which keeps no row out and would make
    // the "no two touch" post-condition depend on which files happen to be empty.
    runs.retain(|(start, end)| start < end);
    runs
}

/// Options for a sort, one direction per column. Nulls go last in both directions, as
/// in pandas, DuckDB and spreadsheets; Polars would otherwise put them first either way.
/// Ties keep their order: each page is its own sort-then-slice, and an unstable sort
/// orders ties differently for a slice at the top (a top-k) than for one further
/// down, so pages would repeat and skip rows, and the inspector's one-row read would
/// find another row.
fn sort_options(descending: Vec<bool>) -> SortMultipleOptions {
    let n = descending.len();
    SortMultipleOptions::default()
        .with_order_descending_multi(descending)
        .with_nulls_last_multi(vec![true; n])
        .with_maintain_order(true)
}

/// The columns a SQL statement's plan orders its result by, leading ones first, and
/// whether each runs descending: down from the top through what keeps the order (a
/// LIMIT, a projection of plain columns) to the sort. Stops at the first key that
/// is an expression rather than a column; empty when no sort is on top.
#[cfg(feature = "sql")]
fn ordered_by(plan: &polars::lazy::dsl::DslPlan) -> Vec<(String, bool)> {
    use polars::lazy::dsl::DslPlan;
    let mut node = plan;
    loop {
        node = match node {
            DslPlan::Slice { input, .. }
            | DslPlan::Filter { input, .. }
            | DslPlan::Cache { input, .. } => input,
            DslPlan::IR { dsl, .. } => dsl,
            DslPlan::Select { expr, input, .. }
                if expr.iter().all(|e| matches!(e, Expr::Column(_))) =>
            {
                input
            }
            DslPlan::Sort {
                by_column,
                sort_options,
                ..
            } => {
                let descending = &sort_options.descending;
                return by_column
                    .iter()
                    .map_while(|e| match e {
                        Expr::Column(name) => Some(name.to_string()),
                        _ => None,
                    })
                    .enumerate()
                    .map(|(i, name)| {
                        let down = descending
                            .get(i)
                            .or(descending.first())
                            .copied()
                            .unwrap_or(false);
                        (name, down)
                    })
                    .collect();
            }
            _ => return Vec::new(),
        };
    }
}

/// `plan` giving its rows in one order on every read. Each page is its own read of
/// the view, so a node free to return rows in any order lets pages repeat some rows
/// and skip others, a `LIMIT` keep different groups on each read, and a sort's ties
/// arrive in a different order each time. Every sort keeps tied rows in the order
/// they come, as [`sort_options`] does (Polars SQL sorts unstably and offers no
/// option), and every grouping, distinct, union and join keeps its input's order,
/// except a grouping sorted by all its keys (see [`sorts_by_group_keys`]). Only the
/// parts of the plan holding such a node are rewritten.
#[cfg(feature = "sql")]
fn stable_order(plan: &mut polars::lazy::dsl::DslPlan) {
    order_stably(plan, false);
}

/// [`stable_order`], where `groups_sorted` says a sort above `plan` orders the rows
/// of the grouping it reads through `plan`.
#[cfg(feature = "sql")]
fn order_stably(plan: &mut polars::lazy::dsl::DslPlan, groups_sorted: bool) {
    use polars::lazy::dsl::DslPlan;
    let unordered = |node: &DslPlan| match node {
        DslPlan::Sort { sort_options, .. } => !sort_options.maintain_order,
        DslPlan::GroupBy { maintain_order, .. } => !maintain_order,
        DslPlan::Distinct { options, .. } => !options.maintain_order,
        DslPlan::Union { args, .. } => !args.maintain_order,
        DslPlan::Join { options, .. } => options.args.maintain_order == MaintainOrderJoin::None,
        _ => false,
    };
    if !plan.into_iter().any(unordered) {
        return;
    }
    // Passed down the path sorts_by_group_keys walked to the grouping, and no other.
    let inputs_sorted = match plan {
        DslPlan::Sort { .. } => sorts_by_group_keys(plan),
        DslPlan::Select { .. } | DslPlan::IR { .. } => groups_sorted,
        _ => false,
    };
    match plan {
        DslPlan::Sort { sort_options, .. } => sort_options.maintain_order = true,
        DslPlan::GroupBy { maintain_order, .. } if !groups_sorted => *maintain_order = true,
        DslPlan::Distinct { options, .. } => options.maintain_order = true,
        DslPlan::Union { args, .. } => args.maintain_order = true,
        DslPlan::Join { options, .. } => {
            Arc::make_mut(options).args.maintain_order = MaintainOrderJoin::LeftRight;
        }
        _ => {}
    }
    if let DslPlan::IR { dsl, .. } = plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // rewrite the plan it came from, and leave the IR behind.
        let mut inner = Arc::unwrap_or_clone(dsl.clone());
        order_stably(&mut inner, inputs_sorted);
        *plan = inner;
        return;
    }
    for_each_input(plan, &mut |input| order_stably(input, inputs_sorted));
}

/// Whether `sort` sorts the rows of a grouping by every one of its keys, so the
/// order the groups arrive in never shows and keeping it is wasted time (#523).
/// Keys are unique per group, so such a sort has no ties, wherever it puts NULLs: a
/// NULL key is one group, and NaN and -0.0 group as the sort compares them. The
/// groups must reach the sort through projections that only pass or rename
/// columns: a filter or a computed column could depend on the order they arrive
/// in, as `ROW_NUMBER() OVER ()` does. A key counts only as a plain column of the
/// sort, which is what polars-sql makes of an alias, an ordinal or a key's own
/// name. It evaluates any other expression against the grouped columns, which
/// already hold the keys: `ORDER BY x % 4 * 2` over `GROUP BY x % 4 * 2` sorts by
/// the key's `% 4 * 2`, and ties keys that differ.
#[cfg(feature = "sql")]
fn sorts_by_group_keys(sort: &polars::lazy::dsl::DslPlan) -> bool {
    use polars::lazy::dsl::DslPlan;
    let DslPlan::Sort {
        input, by_column, ..
    } = sort
    else {
        return false;
    };
    // The sort's columns, under the names they have at each node on the way down.
    let mut names: Vec<PlSmallStr> = by_column
        .iter()
        .filter_map(|e| match e {
            Expr::Column(name) => Some(name.clone()),
            _ => None,
        })
        .collect();
    let mut node: &DslPlan = input;
    loop {
        match node {
            DslPlan::Select { input, expr, .. } => {
                // (output name, input name) of each column.
                let Some(renames) = expr
                    .iter()
                    .map(|e| match e {
                        Expr::Column(c) => Some((c, c)),
                        Expr::Alias(inner, alias) => match &**inner {
                            Expr::Column(c) => Some((alias, c)),
                            _ => None,
                        },
                        _ => None,
                    })
                    .collect::<Option<Vec<_>>>()
                else {
                    return false;
                };
                names = names
                    .iter()
                    .filter_map(|name| {
                        renames
                            .iter()
                            .find(|(out, _)| *out == name)
                            .map(|(_, source)| (*source).clone())
                    })
                    .collect();
                node = input;
            }
            DslPlan::IR { dsl, .. } => node = dsl,
            DslPlan::GroupBy {
                keys,
                options,
                apply: None,
                ..
            } if **options == GroupbyOptions::default() => {
                return keys.iter().all(|key| {
                    let meta = key.clone().meta();
                    !meta.has_multiple_outputs()
                        && meta.output_name().is_ok_and(|key| names.contains(&key))
                });
            }
            _ => return false,
        }
    }
}

/// `plan` with an `IN (SELECT …)` subquery's values counted once instead of once per
/// row. polars-sql adds the values as a one-row list column and filters on
/// `col.first().list.len()` and `col.first().list.contains(NULL)`. The streaming
/// engine repeats that `first()` for every row of a batch and the list kernels copy
/// the list into each: rows times values, 26 GB for a page of a 100k-row table where
/// a third of the rows match (#509). Asked of the values exploded, the questions read
/// the one list; an empty list explodes to no rows, so both answers are unchanged.
/// Only those questions are rewritten: a user's own `ARRAY_LENGTH(FIRST(l))` differs
/// once exploded when the first list is NULL.
#[cfg(feature = "sql")]
fn count_subquery_values_once(plan: &mut polars::lazy::dsl::DslPlan) {
    use polars::lazy::dsl::{DslPlan, FunctionExpr, ListFunction};
    fn ask_once(e: Expr, names: &[PlSmallStr]) -> Expr {
        if !asks_of_subquery_values(&e, names) {
            return e;
        }
        let Expr::Function {
            mut input,
            function: FunctionExpr::ListExpr(function),
        } = e
        else {
            return e;
        };
        let values = input.swap_remove(0).explode(ExplodeOptions {
            empty_as_null: false,
            keep_nulls: true,
        });
        match function {
            ListFunction::Length => values.len(),
            _ => values.null_count().gt(lit(0)),
        }
    }
    if !plan.into_iter().any(asks_per_row) {
        return;
    }
    match plan {
        // A plan asked for its schema is wrapped as IR, which would run as converted:
        // rewrite the plan it came from, and leave the IR behind.
        DslPlan::IR { dsl, .. } => {
            let mut inner = Arc::unwrap_or_clone(dsl.clone());
            count_subquery_values_once(&mut inner);
            *plan = inner;
            return;
        }
        DslPlan::Filter { input, predicate } => {
            let names = subquery_value_columns(input);
            *predicate = predicate.clone().map_expr(|e| ask_once(e, &names));
        }
        _ => {}
    }
    for_each_input(plan, &mut count_subquery_values_once);
}

/// The columns of `schema`, `plan`'s columns, that carry what polars-sql added to
/// hold `IN` subqueries' values (see [`subquery_value_columns`]). A WHERE's projection
/// drops them, but a QUALIFY keeps them in its result, a list of every value on every
/// row, and a statement reading its result as a table carries them on (#519), under
/// a join's suffix when both sides hold one. Matched by the name polars-sql gave
/// them, which is unique to the process, so no column of the user's is taken for one.
#[cfg(feature = "sql")]
fn leftover_subquery_value_columns(
    plan: &mut polars::lazy::dsl::DslPlan,
    schema: &Schema,
) -> Vec<PlSmallStr> {
    use polars::lazy::dsl::DslPlan;
    fn find(plan: &mut DslPlan, values: &mut Vec<PlSmallStr>, suffixes: &mut Vec<PlSmallStr>) {
        match plan {
            DslPlan::IR { dsl, .. } => {
                let mut inner = Arc::unwrap_or_clone(dsl.clone());
                find(&mut inner, values, suffixes);
                return;
            }
            DslPlan::Join { options, .. } => suffixes.push(options.args.suffix().clone()),
            _ => {}
        }
        values.extend(subquery_value_columns(plan));
        for_each_input(plan, &mut |input| find(input, values, suffixes));
    }
    fn carries(name: &str, values: &[PlSmallStr], suffixes: &[PlSmallStr]) -> bool {
        values.iter().any(|v| v == name)
            || suffixes.iter().any(|s| {
                name.strip_suffix(s.as_str())
                    .is_some_and(|rest| carries(rest, values, suffixes))
            })
    }
    let (mut values, mut suffixes) = (Vec::new(), Vec::new());
    find(plan, &mut values, &mut suffixes);
    if values.is_empty() {
        return Vec::new();
    }
    suffixes.retain(|s| !s.is_empty());
    schema
        .iter_names()
        .filter(|name| carries(name, &values, &suffixes))
        .cloned()
        .collect()
}

/// Whether `node` filters on a question of an `IN` subquery's values that the
/// streaming engine answers once per row.
#[cfg(feature = "sql")]
fn asks_per_row(node: &polars::lazy::dsl::DslPlan) -> bool {
    let polars::lazy::dsl::DslPlan::Filter { input, predicate } = node else {
        return false;
    };
    let names = subquery_value_columns(input);
    !names.is_empty()
        && predicate
            .into_iter()
            .any(|e| asks_of_subquery_values(e, &names))
}

/// The columns polars-sql adds beside `plan` to hold `IN` subqueries' values: each
/// subquery is selected as one aliased list and concatenated horizontally, broadcast
/// to the frame's rows (`SQLContext::process_subqueries`).
#[cfg(feature = "sql")]
fn subquery_value_columns(plan: &polars::lazy::dsl::DslPlan) -> Vec<PlSmallStr> {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::HConcat { inputs, options } if options.broadcast_unit_length => inputs
            .iter()
            .skip(1)
            .filter_map(|input| match input {
                DslPlan::Select { expr, .. } => match expr.as_slice() {
                    [Expr::Alias(_, name)] => Some(name.clone()),
                    _ => None,
                },
                _ => None,
            })
            .collect(),
        DslPlan::IR { dsl, .. } => subquery_value_columns(dsl),
        _ => Vec::new(),
    }
}

/// Whether `e` is polars-sql asking how many values an `IN` subquery returned, or
/// whether one is NULL: `list.len` or `list.contains(NULL)` of `col(name).first()`,
/// where `name` is one of `names`, the columns holding the values.
#[cfg(feature = "sql")]
fn asks_of_subquery_values(e: &Expr, names: &[PlSmallStr]) -> bool {
    use polars::lazy::dsl::{FunctionExpr, ListFunction};
    let values = |e: &Expr| {
        matches!(e, Expr::Agg(AggExpr::First(c))
            if matches!(&**c, Expr::Column(name) if names.contains(name)))
    };
    match e {
        Expr::Function {
            input,
            function: FunctionExpr::ListExpr(ListFunction::Length),
        } => matches!(input.as_slice(), [set] if values(set)),
        Expr::Function {
            input,
            function: FunctionExpr::ListExpr(ListFunction::Contains { nulls_equal: true }),
        } => {
            matches!(input.as_slice(), [set, Expr::Literal(item)] if values(set) && item.is_null())
        }
        _ => false,
    }
}

/// The in-memory frame `lf` scans, when it is a scan of one.
fn scanned_frame(lf: &LazyFrame) -> Option<Arc<DataFrame>> {
    match &lf.logical_plan {
        polars::lazy::dsl::DslPlan::DataFrameScan { df, .. } => Some(df.clone()),
        _ => None,
    }
}

/// Calls `f` on each plan `plan` reads from.
pub(crate) fn for_each_input(
    plan: &mut polars::lazy::dsl::DslPlan,
    f: &mut dyn FnMut(&mut polars::lazy::dsl::DslPlan),
) {
    use polars::lazy::dsl::DslPlan;
    match plan {
        DslPlan::Sort { input, .. }
        | DslPlan::Select { input, .. }
        | DslPlan::GroupBy { input, .. }
        | DslPlan::Filter { input, .. }
        | DslPlan::Distinct { input, .. }
        | DslPlan::Slice { input, .. }
        | DslPlan::HStack { input, .. }
        | DslPlan::MatchToSchema { input, .. }
        | DslPlan::MapFunction { input, .. }
        | DslPlan::Sink { input, .. }
        | DslPlan::Cache { input, .. }
        | DslPlan::Pivot { input, .. } => f(Arc::make_mut(input)),
        DslPlan::Union { inputs, .. }
        | DslPlan::HConcat { inputs, .. }
        | DslPlan::SinkMultiple { inputs } => inputs.iter_mut().for_each(f),
        DslPlan::PipeWithSchema { input, .. } => {
            let mut inputs = input.to_vec();
            inputs.iter_mut().for_each(&mut *f);
            *input = inputs.into();
        }
        DslPlan::Join {
            input_left,
            input_right,
            ..
        } => {
            f(Arc::make_mut(input_left));
            f(Arc::make_mut(input_right));
        }
        DslPlan::Gather { input, idxs, .. } => {
            f(Arc::make_mut(input));
            f(Arc::make_mut(idxs));
        }
        DslPlan::ExtContext { input, contexts } => {
            f(Arc::make_mut(input));
            contexts.iter_mut().for_each(f);
        }
        _ => {}
    }
}

/// A string's in-memory width when nothing says otherwise: the view plus a short value.
const STRING_BYTES_GUESS: usize = 40;

/// Bytes a row of `columns` takes in memory, estimated from the schema: the width of
/// each fixed-size type; for a string the footer's average in `column_bytes` (or a
/// guess) plus its view; for a nested column the footer's average, else a guess.
/// Binary columns are buffered as a stub (see `binary_stub_exprs`).
fn estimate_bytes_per_row(
    schema: &Schema,
    columns: &[String],
    column_bytes: &[(String, usize)],
) -> usize {
    let footer_width = |name: &String| {
        column_bytes
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, w)| *w)
    };
    columns
        .iter()
        .map(|name| match schema.get(name.as_str()) {
            Some(DataType::String) => 16 + footer_width(name).unwrap_or(STRING_BYTES_GUESS - 16),
            Some(DataType::Binary) => 16 + binary_stub().len(),
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

/// The rows `[offset, offset + len)` of `df`, copied when a slice of them would keep
/// much more allocated than they are. A `seam` inside them, where a stitch joined two
/// fills, stays a chunk boundary (see [`compact_rows`]).
///
/// A slice keeps every chunk it touches. A fill read in many chunks (a Parquet or CSV
/// scan) lets the rest go with a slice alone; one read in a single chunk, a stitched
/// union or a string column sharing its parent's data would keep the whole fill. A
/// chunk that is itself a slice of more is not seen through.
fn trim_rows(df: DataFrame, offset: usize, len: usize, seam: Option<usize>) -> DataFrame {
    if backing_rows(&df, offset, len) > len + len / 4 {
        compact_rows(df, offset, len, seam)
    } else {
        df.slice(offset as i64, len)
    }
}

/// The most rows any column of `df` keeps allocated behind the slice `[offset, offset
/// + len)`: every chunk the slice touches, whole.
fn backing_rows(df: &DataFrame, offset: usize, len: usize) -> usize {
    let end = offset + len;
    df.columns()
        .iter()
        .filter_map(Column::as_series)
        .map(|s| {
            let mut start = 0;
            let mut touched = 0;
            for chunk in s.chunks() {
                let chunk_end = start + chunk.len();
                if start < end && offset < chunk_end {
                    touched += chunk.len();
                }
                start = chunk_end;
            }
            touched
        })
        .max()
        .unwrap_or(len)
}

/// The rows `[offset, offset + len)` of `df` in storage of their own: one chunk a
/// column, or two when `seam` falls inside them, so a later cut down to one side of a
/// stitch (`holds_buffer`) is a slice that lets the other side go.
///
/// A slice keeps the whole of its parent allocated, and neither `rechunk` (a lone chunk
/// is left as it is) nor `take` (a string column keeps its parent's data buffers) is
/// sure to let go of it. Polars' builders with `ShareStrategy::Never` copy every
/// physical type, nested children and string bytes included. A constant column stays
/// one value: built out, it would be a copy of the value per row.
///
/// Each column of `df` is let go of once it is copied, so the copy costs about one
/// column's kept rows over `df` rather than all of them. On the collect worker the
/// rows on screen are still held meanwhile (#483).
fn compact_rows(df: DataFrame, offset: usize, len: usize, seam: Option<usize>) -> DataFrame {
    use polars::series::builder::SeriesBuilder;
    use polars_arrow::array::builder::ShareStrategy;
    #[cfg(test)]
    tests::COMPACTIONS.with(|count| count.set(count.get() + 1));
    let len = len.min(df.height().saturating_sub(offset));
    let pieces = match seam.filter(|&seam| offset < seam && seam < offset + len) {
        Some(seam) => vec![(offset, seam - offset), (seam, offset + len - seam)],
        None => vec![(offset, len)],
    };
    let copy = |series: &Series, (offset, len): (usize, usize)| {
        let mut builder = SeriesBuilder::new(series.dtype().clone());
        builder.reserve(len);
        builder.subslice_extend(series, offset, len, ShareStrategy::Never);
        builder.freeze(series.name().clone())
    };
    let columns = df
        .into_columns()
        .into_iter()
        .map(|column| match column {
            Column::Scalar(constant) => {
                Column::new_scalar(constant.name().clone(), constant.scalar().clone(), len)
            }
            Column::Series(series) => {
                let mut kept = copy(&series, pieces[0]);
                for &piece in &pieces[1..] {
                    if kept.append_owned(copy(&series, piece)).is_err() {
                        kept = copy(&series, (offset, len));
                        break;
                    }
                }
                kept.into_column()
            }
        })
        .collect();
    // Cannot fail: the names are one frame's and every column was built to `len` rows.
    DataFrame::new(len, columns).unwrap_or_else(|_| DataFrame::empty_with_height(len))
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
    // An empty file is not opened (see `window_of`), so it costs nothing to reach past.
    let opened = |from: usize, to: usize| (from..=to).filter(|&i| holds_rows(offsets, i)).count();
    if opened(first, last) <= max_files {
        return (start, end);
    }
    let (mut lo, mut hi) = (view_first.max(first), view_last.min(last));
    let mut files = opened(lo, hi);
    while files < max_files && (hi < last || lo > first) {
        if hi < last {
            hi += 1;
            files += usize::from(holds_rows(offsets, hi));
        }
        if files < max_files && lo > first {
            lo -= 1;
            files += usize::from(holds_rows(offsets, lo));
        }
    }
    (start.max(offsets[lo]), end.min(offsets[hi + 1]))
}

/// Whether file `i` has any rows, given where each file's rows start.
fn holds_rows(offsets: &[usize], i: usize) -> bool {
    offsets[i + 1] > offsets[i]
}

/// The files from `first` to `last` that hold rows: a window reads these and passes
/// over the empty ones, which a dataset written a file a day can be mostly made of.
fn files_with_rows(offsets: &[usize], first: usize, last: usize) -> Vec<usize> {
    (first..=last).filter(|&i| holds_rows(offsets, i)).collect()
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

/// Rows `[start, start + len)` of `lf` as `all_columns`. With `files` counted, a scan
/// of only the files holding them, so a window deep in a remote dataset does not read
/// every file before it. With `records`, the rows read straight from the source.
fn window_of(
    lf: &LazyFrame,
    files: Option<&RemoteFiles>,
    records: Option<&dyn crate::pushdown::Windowed>,
    read_as_text: &[PlSmallStr],
    start: usize,
    len: usize,
    all_columns: Vec<Expr>,
) -> PolarsResult<LazyFrame> {
    // Polars gives an anonymous scan no row offset, so a slice deep in the view would
    // read every row before it; the source starts the window there instead.
    if let Some(records) = records {
        return Ok(records.window(start, len)?.select(all_columns));
    }
    if let Some((files, offsets)) = files.and_then(|f| f.offsets.as_ref().map(|o| (f, o)))
        && let Some((first, last)) = files_holding(offsets, start, len)
    {
        // The window's first file holds its first row, so leaving out the empty files
        // after it does not move the slice.
        let urls: Vec<String> = files_with_rows(offsets, first, last)
            .into_iter()
            .map(|i| files.urls[i].clone())
            .collect();
        let lf = (files.scan)(&urls, read_as_text)?;
        return Ok(lf
            .select(all_columns)
            .slice((start - offsets[first]) as i64, len as u32));
    }
    Ok(lf
        .clone()
        .select(all_columns)
        .slice(start as i64, len as u32))
}

/// The rows of a view, for a reader off the UI thread: read a window at a time as a
/// page is, or from the buffer the table already holds.
#[derive(Clone)]
pub(crate) struct ViewRows {
    lf: LazyFrame,
    files: Option<RemoteFiles>,
    /// See [`DataTableState::window_now`].
    records: Option<Arc<dyn crate::pushdown::Windowed>>,
    read_as_text: Vec<PlSmallStr>,
    /// The buffer on hand and the view row it starts at.
    pub(crate) buffer: Option<(DataFrame, usize)>,
    /// The view's row count, when it is known.
    pub(crate) num_rows: Option<usize>,
    pub(crate) streaming: bool,
    /// Any window of the view reads all of it: see [`sees_every_row_first`].
    pub(crate) whole: bool,
    /// A window of the view reads every row before it: see [`reads_up_to_a_window`].
    pub(crate) reads_up_to: bool,
}

/// Whether `lf` has to see every row before it gives its first: a sort, a group by or a
/// pivot under it. Then a window of it costs as much as all of it.
pub(crate) fn sees_every_row_first(lf: &LazyFrame) -> bool {
    use polars::lazy::dsl::DslPlan;
    lf.logical_plan.into_iter().any(|node| {
        matches!(
            node,
            DslPlan::Sort { .. } | DslPlan::GroupBy { .. } | DslPlan::Pivot { .. }
        )
    })
}

/// Whether a window of `lf` reads every row before it: a filter, which has to test
/// them to know which row is the window's first, or a scan with no row index to skip
/// by, such as a CSV. Parquet and IPC skip to a window.
pub(crate) fn reads_up_to_a_window(lf: &LazyFrame) -> bool {
    use polars::lazy::dsl::{DslPlan, FileScanDsl};
    lf.logical_plan.into_iter().any(|node| match node {
        DslPlan::Filter { .. } => true,
        DslPlan::Scan { scan_type, .. } => !matches!(
            **scan_type,
            FileScanDsl::Parquet { .. } | FileScanDsl::Ipc { .. }
        ),
        _ => false,
    })
}

impl ViewRows {
    /// Rows `[start, start + len)` of the view as `exprs`.
    pub(crate) fn window(
        &self,
        start: usize,
        len: usize,
        exprs: Vec<Expr>,
    ) -> PolarsResult<LazyFrame> {
        window_of(
            &self.lf,
            self.files.as_ref(),
            self.records.as_deref(),
            &self.read_as_text,
            start,
            len,
            exprs,
        )
    }

    /// The view `lf`, with `buffer` on hand from row `buffer_start`.
    #[cfg(test)]
    pub(crate) fn of(lf: LazyFrame, buffer: Option<(DataFrame, usize)>) -> Self {
        Self {
            whole: sees_every_row_first(&lf),
            reads_up_to: reads_up_to_a_window(&lf),
            lf,
            files: None,
            records: None,
            read_as_text: Vec::new(),
            buffer,
            num_rows: None,
            streaming: false,
        }
    }
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
        let (schema, source_rows_at_open) = Self::without_source_rows(lf.clone().collect_schema()?);
        let column_order: Vec<String> = schema.iter_names().map(|s| s.to_string()).collect();
        Ok(Self {
            unsorted_lf: None,
            original_lf: lf.clone(),
            original_schema: schema.clone(),
            base_lf: lf.clone(),
            lf,
            df: None,
            locked_df: None,
            table_state: TableState::default(),
            start_row: 0,
            visible_rows: 0,
            termcol_index: 0,
            visible_termcols: 0,
            scroll_room: None,
            column_moves: Vec::new(),
            page_trail: Vec::new(),
            on_screen: None,
            drawn: None,
            error: None,
            suppress_error_display: false,
            schema,
            num_rows: 0,
            num_rows_valid: false,
            pristine_rows: None,
            len_generation: next_len_generation(),
            root_generation: next_len_generation(),
            parquet_count_dir: None,
            measurements: Arc::new(crate::measurements::Meter::default()),
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_descending: Vec::new(),
            sort_ascending: true,
            cursor_column: None,
            cursor_at: 0,
            reveal_cursor: false,
            active_query: String::new(),
            active_sql_query: String::new(),
            query_order: Vec::new(),
            active_fuzzy_query: String::new(),
            column_order,
            locked_columns_count: 0,
            frozen_fit: (0, 0),
            widths: ColumnWidths::default(),
            grouped: None,
            group_source: None,
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
            remote_objects: None,
            dataset_schema: None,
            drift_column_present: false,
            drift_groups: Arc::new(Vec::new()),
            drift_at_open: false,
            groups_at_open: Arc::new(Vec::new()),
            source_rows_at_open,
            view_numbered: false,
            indexing: None,
            numbering: None,
            row_estimate: None,
            indexing_notes: Vec::new(),
            indexing_guessed: false,
            drift_file_starts: Vec::new(),
            drift_file_group: Vec::new(),
            drift_files: Vec::new(),
            footers_pending: None,
            notes: Vec::new(),
            open_notes: Vec::new(),
            not_the_table: None,
            format_read: None,
            delimited: None,
            fixed_window: None,
            pushdown: None,
            source_hold: None,
            read_mode: None,
            read_as: None,
            fetched: false,
            detail: None,
            file_units: Arc::new(Vec::new()),
            notes_seen: false,
            notes_at_open: Vec::new(),
            view_notes: Vec::new(),
            drift_dataset_rows: 0,
            dataset_at_open: None,
            read_as_text: Vec::new(),
            column_bytes: Vec::new(),
            observed_bytes_per_row: None,
            buffered_start_row: 0,
            buffered_end_row: 0,
            buffered_df: None,
            proximity_threshold: 0, // Will be set when visible_rows is known
            drawn_start: 0,
            row_numbers: false, // Will be set from options
            row_start_index: 1, // Will be set from options
            last_pivot_spec: None,
            last_melt_spec: None,
            reshape_source: None,
            base_steps: Vec::new(),
            read_python: Vec::new(),
            read_notes: Vec::new(),
            read_units: None,
            typing: Typing::default(),
            unfit_notes: None,
            column_changes: Vec::new(),
            changes_version: 0,
            changes_unfit: None,
            changes_dropped: Vec::new(),
            reshape_steps: None,
            lineage: None,
            reshape_lineage: None,
            partition_columns: None,
            decompress_temp_file: None,
            download: None,
            converted: Vec::new(),
            other_tables: Vec::new(),
            polars_streaming,
            defer_collect: false,
            needs_recollect: false,
            follow: None,
            follow_known: None,
            sampled: None,
        })
    }

    /// `schema` without the hidden row index, and whether it had one: the rows' place
    /// in the source, which `#` shows, never a column of theirs.
    fn without_source_rows(schema: Arc<Schema>) -> (Arc<Schema>, bool) {
        if !schema.contains(crate::schema_union::DRIFT_COLUMN) {
            return (schema, false);
        }
        let mut schema = (*schema).clone();
        schema.shift_remove(crate::schema_union::DRIFT_COLUMN);
        (Arc::new(schema), true)
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
        let (schema, source_rows_at_open) = Self::without_source_rows(schema);
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
            unsorted_lf: None,
            original_lf: lf.clone(),
            original_schema: schema.clone(),
            base_lf: lf.clone(),
            lf,
            df: None,
            locked_df: None,
            table_state: TableState::default(),
            start_row: 0,
            visible_rows: 0,
            termcol_index: 0,
            visible_termcols: 0,
            scroll_room: None,
            column_moves: Vec::new(),
            page_trail: Vec::new(),
            on_screen: None,
            drawn: None,
            error: None,
            suppress_error_display: false,
            schema,
            num_rows: 0,
            num_rows_valid: false,
            pristine_rows: None,
            len_generation: next_len_generation(),
            root_generation: next_len_generation(),
            parquet_count_dir: None,
            measurements: Arc::new(crate::measurements::Meter::default()),
            filters: Vec::new(),
            sort_columns: Vec::new(),
            sort_descending: Vec::new(),
            sort_ascending: true,
            cursor_column: None,
            cursor_at: 0,
            reveal_cursor: false,
            active_query: String::new(),
            active_sql_query: String::new(),
            query_order: Vec::new(),
            active_fuzzy_query: String::new(),
            column_order,
            locked_columns_count: 0,
            frozen_fit: (0, 0),
            widths: ColumnWidths::default(),
            grouped: None,
            group_source: None,
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
            remote_objects: None,
            dataset_schema: None,
            drift_column_present: false,
            drift_groups: Arc::new(Vec::new()),
            drift_at_open: false,
            groups_at_open: Arc::new(Vec::new()),
            source_rows_at_open,
            view_numbered: false,
            indexing: None,
            numbering: None,
            row_estimate: None,
            indexing_notes: Vec::new(),
            indexing_guessed: false,
            drift_file_starts: Vec::new(),
            drift_file_group: Vec::new(),
            drift_files: Vec::new(),
            footers_pending: None,
            notes: Vec::new(),
            open_notes: Vec::new(),
            not_the_table: None,
            format_read: None,
            delimited: None,
            fixed_window: None,
            pushdown: None,
            source_hold: None,
            read_mode: None,
            read_as: None,
            fetched: false,
            detail: None,
            file_units: Arc::new(Vec::new()),
            notes_seen: false,
            notes_at_open: Vec::new(),
            view_notes: Vec::new(),
            drift_dataset_rows: 0,
            dataset_at_open: None,
            read_as_text: Vec::new(),
            column_bytes: Vec::new(),
            observed_bytes_per_row: None,
            buffered_start_row: 0,
            buffered_end_row: 0,
            buffered_df: None,
            proximity_threshold: 0,
            drawn_start: 0,
            row_numbers: options.row_numbers,
            row_start_index: options.row_start_index,
            last_pivot_spec: None,
            last_melt_spec: None,
            reshape_source: None,
            base_steps: Vec::new(),
            read_python: Vec::new(),
            read_notes: Vec::new(),
            read_units: None,
            typing: Typing::default(),
            unfit_notes: None,
            column_changes: Vec::new(),
            changes_version: 0,
            changes_unfit: None,
            changes_dropped: Vec::new(),
            reshape_steps: None,
            lineage: None,
            reshape_lineage: None,
            partition_columns,
            decompress_temp_file: None,
            download: None,
            converted: Vec::new(),
            other_tables: Vec::new(),
            polars_streaming: options.polars_streaming,
            defer_collect: false,
            needs_recollect: false,
            follow: None,
            follow_known: None,
            sampled: None,
        })
    }

    /// The state as its open found the dataset: everything in `facts`, given at once.
    ///
    /// The one way an open's findings reach a state, taken while it is still the data as
    /// loaded. Applied in the order they depend on each other: the files before their row
    /// groups, which set the count. Once on screen, a dataset learns more only through
    /// [`Self::join_dataset_schema`] and [`Self::count_landed`].
    pub fn with_open(mut self, facts: OpenFacts) -> Self {
        let OpenFacts {
            remote_source,
            row_groups,
            remote_files,
            remote_objects,
            dataset,
            footers_pending,
            column_bytes,
            parquet_count_dir,
            measurements,
            open_notes,
            not_the_table,
            format_read,
            delimited,
            download,
            converted,
            other_tables,
            pushdown,
            hold,
            read_mode,
            read_as,
            fetched,
            detail,
            records,
            units,
            indexing,
            numbering,
            typing,
        } = facts;
        self.numbering = numbering;
        self.typing = typing;
        debug_assert!(
            self.is_pristine(),
            "an open's facts are for the data as loaded"
        );
        self.remote_source = remote_source;
        self.remote_files = remote_files;
        self.remote_objects = (!remote_objects.is_empty()).then(|| {
            Arc::new(
                remote_objects
                    .into_iter()
                    .map(|object| (object.url.clone(), object))
                    .collect(),
            )
        });
        if !row_groups.is_empty() {
            if self.remote_files.is_some() {
                self.record_file_row_groups(&row_groups);
            } else {
                let flat: Vec<usize> = row_groups.into_iter().flatten().collect();
                self.record_row_groups(&flat);
            }
        }
        if let Some(DatasetAtOpen {
            schema,
            file_rows,
            files,
        }) = dataset
        {
            self.record_dataset_schema(schema, &file_rows, &files);
        }
        self.footers_pending = footers_pending;
        self.column_bytes = column_bytes;
        self.parquet_count_dir = parquet_count_dir;
        self.measurements = measurements;
        self.open_notes = open_notes;
        self.not_the_table = not_the_table;
        self.fixed_window = format_read
            .as_ref()
            .map(|read| read.records.clone() as Arc<dyn crate::pushdown::Windowed>);
        if let Some(read) = &format_read {
            // As for audio: the reader counted the records from the file's size, and a
            // count through the frame would build its row index whole.
            self.set_num_rows(read.records.rows());
        }
        self.pushdown = pushdown;
        self.source_hold = hold;
        self.format_read = format_read;
        self.delimited = delimited;
        self.download = download;
        self.converted = converted;
        self.other_tables = other_tables;
        self.read_mode = read_mode;
        self.read_as = read_as;
        self.fetched = fetched;
        self.detail = detail;
        if let Some((window, rows)) = records {
            // The reader knows its rows; a count through the frame would build its row
            // index whole. Lines still being indexed know only some of theirs.
            if indexing.is_none() {
                self.set_num_rows(rows);
            }
            self.fixed_window = Some(window);
        }
        if let Some(lines) = &indexing {
            // The lines' own notes, as the open wrote them: replaced once every line
            // is in, when they can say what the whole file holds.
            self.indexing_guessed = self
                .open_notes
                .iter()
                .any(|n| n.summary.starts_with(crate::lines::GUESSED));
            self.indexing_notes = crate::lines::notes(lines, self.indexing_guessed);
        }
        self.indexing = indexing;
        self.file_units = Arc::new(units);
        self
    }

    /// Make `lf` the data as loaded, with `schema`: the root, the base and the frame
    /// shown, until the caller lays the filters and sort back on. The rows and count
    /// read through the old root are dropped, and checkpoints taken over it no longer
    /// apply.
    fn replace_root(&mut self, lf: LazyFrame, schema: Arc<Schema>) {
        // The records, or the table, no longer stand for the root.
        self.fixed_window = None;
        self.pushdown = None;
        self.root_generation = next_len_generation();
        self.invalidate_num_rows();
        self.original_schema = schema.clone();
        self.schema = schema;
        self.original_lf = lf.clone();
        self.base_lf = lf.clone();
        self.lf = lf;
        self.unsorted_lf = None;
        self.base_steps = Vec::new();
        self.reshape_steps = None;
        self.drop_buffer();
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
        self.view_numbered = false;
        self.drift_groups = Arc::new(Vec::new());
        self.notes = Vec::new();
        self.view_notes = Vec::new();
        // Rows of the new shape are measured afresh; the old width would plan the
        // window of a wide frame from a narrow one, or the reverse.
        self.observed_bytes_per_row = None;
        // A new frame is in no order a query named; `sql_query` names it after.
        self.query_order = Vec::new();
        // A column may keep its name and type and hold other values now.
        self.widths.relearn();
        self.base_lf = lf.clone();
        self.lf = lf;
        self.unsorted_lf = None;
        // Every caller says how the base was built; one that does not leaves a script
        // that says so rather than one that computes something else.
        self.base_steps = vec![Step::Unreproducible(
            "datui built the view from here in a way it cannot write as Python".to_string(),
        )];
        self.schema = schema;
        self.column_order = self.schema.iter_names().map(|s| s.to_string()).collect();
        // No column is a loaded one until the caller says which are.
        self.lineage = Some(Arc::default());
        self.settle_cursor();
        // A query that groups records its source after installing its result.
        self.group_source = None;
        self.drop_buffer();
    }

    /// Forget the rows read through the frame being replaced, so the next collect reads
    /// the new one. Without this a view that fits in the old buffer keeps drawing it.
    fn drop_buffer(&mut self) {
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
    }

    /// The view state for a new pipeline root: no query bar text, no sidebar filters or
    /// sort, not drilled, the first `locked_columns_count` columns frozen, the buffer
    /// dropped and the cursor at the top left.
    fn reset_view_state(&mut self, locked_columns_count: usize) {
        self.forget_column_changes();
        self.active_query.clear();
        self.active_sql_query.clear();
        self.active_fuzzy_query.clear();
        self.locked_columns_count = locked_columns_count;
        self.filters.clear();
        self.sort_columns.clear();
        self.sort_descending.clear();
        self.sort_ascending = true;
        self.start_row = 0;
        self.termcol_index = 0;
        self.clear_column_moves();
        self.place_cursor_at(0);
        self.drilled_down_group_index = None;
        self.drilled_down_group_key = None;
        self.drilled_down_group_key_columns = None;
        self.grouped = None;
        self.drop_buffer();
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
        steps: Vec<Step>,
    ) {
        self.install_base(lf, schema);
        self.base_steps = steps;
        self.reset_view_state(locked_columns_count);
        match query {
            ActiveQuery::Dsl(q) => self.active_query = q,
            #[cfg(feature = "sql")]
            ActiveQuery::Sql(q) => self.active_sql_query = q,
            ActiveQuery::Fuzzy(q) => self.active_fuzzy_query = q,
        }
    }

    /// The view no longer shows the pivot or melt, so nothing may run against it.
    fn forget_reshape(&mut self) {
        self.reshaped_lf = None;
        self.reshape_lineage = None;
        self.reshape_steps = None;
        self.last_pivot_spec = None;
        self.last_melt_spec = None;
        self.reshape_source = None;
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
        self.base_steps = Vec::new();
        self.reshape_steps = None;
        self.lineage = None;
        self.reshape_lineage = None;
        // A reset is a return to the data as opened, so the rows stand for files again
        // and what datui noticed about them applies once more.
        self.drift_column_present = self.drift_at_open;
        self.drift_groups = self.groups_at_open.clone();
        self.notes = self.notes_at_open.clone();
        self.reshaped_lf = None;
        self.reshape_source = None;
        self.reset_view_state(0);
        self.restore_footer_count();
    }

    /// Back to the data as loaded, with nothing applied and no error showing.
    fn return_to_root(&mut self) {
        self.reset_lf_to_original();
        self.error = None;
        self.suppress_error_display = false;
        self.last_pivot_spec = None;
        self.last_melt_spec = None;
    }

    /// Back to the data as loaded with nothing applied, for a view's steps to be laid
    /// on again. Reads nothing.
    pub(crate) fn reset_view_for_replay(&mut self) {
        self.return_to_root();
    }

    /// Back to the table as opened: the data as loaded, nothing applied, and every
    /// column's width learned afresh from the first page.
    pub fn reset(&mut self) {
        self.widths = ColumnWidths::default();
        self.return_to_root();
        self.collect();
        if self.num_rows > 0 {
            self.start_row = 0;
        }
    }

    /// A file reader's state, with the open's paging and row numbers.
    ///
    /// Streaming is always on here, whatever `options.polars_streaming` says: the
    /// per-format readers have always built that way.
    fn read_with(lf: LazyFrame, options: &OpenOptions) -> Result<Self> {
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

    pub fn from_parquet(path: &Path, options: &OpenOptions) -> Result<Self> {
        let is_glob = crate::source::expands_as_glob(path);
        let pl_path = PlRefPath::try_from_path(path)?;
        let args = ScanArgsParquet {
            glob: is_glob,
            ..Default::default()
        };
        let lf = LazyFrame::scan_parquet(pl_path, args)?;
        Self::read_with(lf, options)
    }

    /// Load multiple Parquet files and concatenate them into one LazyFrame (same schema assumed).
    /// How the files of one dataset are stacked into one table.
    ///
    /// `diagonal`, so a file written before a column existed brings the rest of its
    /// rows instead of refusing the whole directory; the column reads null for it, and
    /// the Notes say which files have it. `to_supertypes`, because a CSV column is
    /// typed by inference per file — one `N/A` makes `amount` a String in one file and
    /// an Int64 in the next — and without widening, name agreement is not enough to
    /// stack them.
    ///
    /// Both are opt-ins everywhere else: DuckDB's `union_by_name`, pyarrow's
    /// `unify_schemas`, Spark's `mergeSchema`. They are the default here because a
    /// library that unions silently becomes wrong analysis downstream, while datui
    /// says what it did in the Notes and keeps `Enter` on the row conservative — a
    /// directory whose files are not one table is gone inside, not unioned, and this is
    /// what the `(all files)` row behind it reads with.
    ///
    /// **Only for the formats that rule can judge**, which is CSV and NDJSON here, and
    /// Parquet through `lenient_scan` elsewhere. Arrow, Avro, ORC and `.json` keep
    /// their columns nowhere cheap to reach, so nothing looks at them before the open
    /// and nothing could say what a union of them had done — a silent union with no
    /// gate in front of it and no note behind it is the pairing this whole change
    /// exists to remove, not something to spread further.
    ///
    /// Identical schemas stack exactly as before: diagonal over one schema is vertical,
    /// and nothing is widened where nothing differs. Arrow streams converted beside IPC
    /// files read in place stack with it too: the streams and the files are one
    /// directory's table, read two ways (`App::scan_arrow_parts`).
    pub(crate) fn union_of_files() -> polars::prelude::UnionArgs {
        polars::prelude::UnionArgs {
            diagonal: true,
            to_supertypes: true,
            ..Default::default()
        }
    }

    pub fn from_parquet_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_parquet(paths[0].as_ref(), options);
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlRefPath::try_from_path(p.as_ref())?;
            let args = ScanArgsParquet {
                glob: crate::source::expands_as_glob(p.as_ref()),
                ..Default::default()
            };
            let lf = LazyFrame::scan_parquet(pl_path, args)?;
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        Self::read_with(lf, options)
    }

    /// Load a single Arrow IPC / Feather v2 file (lazy).
    pub fn from_ipc(path: &Path, options: &OpenOptions) -> Result<Self> {
        let pl_path = PlRefPath::try_from_path(path)?;
        let args = UnifiedScanArgs {
            glob: crate::source::expands_as_glob(path),
            ..Default::default()
        };
        let lf = LazyFrame::scan_ipc(pl_path, Default::default(), args)?;
        Self::read_with(lf, options)
    }

    /// Load multiple Arrow IPC / Feather files and concatenate into one LazyFrame.
    pub fn from_ipc_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_ipc(paths[0].as_ref(), options);
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let pl_path = PlRefPath::try_from_path(p.as_ref())?;
            let args = UnifiedScanArgs {
                glob: crate::source::expands_as_glob(p.as_ref()),
                ..Default::default()
            };
            let lf = LazyFrame::scan_ipc(pl_path, Default::default(), args)?;
            lazy_frames.push(lf);
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        Self::read_with(lf, options)
    }

    /// Load a single Avro file (eager read, then lazy).
    pub fn from_avro(path: &Path, options: &OpenOptions) -> Result<Self> {
        let file = File::open(path)?;
        let df = polars::io::avro::AvroReader::new(file).finish()?;
        let lf = df.lazy();
        Self::read_with(lf, options)
    }

    /// Load multiple Avro files and concatenate into one LazyFrame.
    pub fn from_avro_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_avro(paths[0].as_ref(), options);
        }
        let mut lazy_frames = Vec::with_capacity(paths.len());
        for p in paths {
            let file = File::open(p.as_ref())?;
            let df = polars::io::avro::AvroReader::new(file).finish()?;
            lazy_frames.push(df.lazy());
        }
        let lf = polars::prelude::concat(lazy_frames.as_slice(), Default::default())?;
        Self::read_with(lf, options)
    }

    /// Load a single Excel file (xls, xlsx, xlsm, xlsb) using calamine (eager read, then lazy).
    /// Sheet is selected by name, or by 0-based index when no sheet is so named, via
    /// `options.table` (`--table`).
    pub fn from_excel(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_excel_with_detail(path, options).map(|(state, _)| state)
    }

    /// [`Self::from_excel`], with the workbook's Excel tab for the Info panel.
    pub(crate) fn from_excel_with_detail(
        path: &Path,
        options: &OpenOptions,
    ) -> Result<(Self, crate::text_formats::Detail)> {
        let mut workbook =
            open_workbook_auto(path).map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?;
        let sheet_names = workbook.sheet_names().to_vec();
        if sheet_names.is_empty() {
            return Err(color_eyre::eyre::eyre!("Excel file has no worksheets"));
        }
        // Named so a bad --table says what to ask for instead: "0 'Sales', 1 'Summary'".
        let sheets_on_offer = || {
            sheet_names
                .iter()
                .enumerate()
                .map(|(i, name)| format!("{} '{}'", i, name))
                .collect::<Vec<_>>()
                .join(", ")
        };
        // A sheet's name before an index: the home screen names a sheet called `2023`.
        let opened = match options.table.as_deref() {
            None => sheet_names[0].clone(),
            Some(name) if sheet_names.iter().any(|n| n == name) => name.to_string(),
            Some(sheet_sel) => match sheet_sel.parse::<usize>() {
                Ok(idx) => sheet_names.get(idx).cloned().ok_or_else(|| {
                    color_eyre::eyre::eyre!(
                        "Excel: no worksheet at index {}; this file has: {}",
                        idx,
                        sheets_on_offer()
                    )
                })?,
                Err(_) => {
                    return Err(color_eyre::eyre::eyre!(
                        "Excel: no worksheet named '{}'; this file has: {}",
                        sheet_sel,
                        sheets_on_offer()
                    ));
                }
            },
        };
        let range = workbook
            .worksheet_range(&opened)
            .map_err(|e| color_eyre::eyre::eyre!("Excel: {}", e))?;
        let detail = crate::excel::detail(&mut workbook, &opened, &range);
        drop(workbook);
        let rows: Vec<Vec<Data>> = range.rows().map(|r| r.to_vec()).collect();
        if rows.is_empty() {
            let empty_df = DataFrame::empty();
            return Ok((Self::read_with(empty_df.lazy(), options)?, detail));
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
        Ok((Self::read_with(df.lazy(), options)?, detail))
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
    /// ORC is read fully into memory; see `docs/formats/columnar-and-json.md`.
    pub fn from_orc(path: &Path, options: &OpenOptions) -> Result<Self> {
        let file = File::open(path)?;
        let reader = ArrowReaderBuilder::try_new(file)
            .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?
            .build();
        let batches: Vec<RecordBatch> = reader
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|e| color_eyre::eyre::eyre!("ORC: {}", e))?;
        let df = Self::arrow_record_batches_to_dataframe(&batches)?;
        let lf = df.lazy();
        Self::read_with(lf, options)
    }

    /// Load multiple ORC files and concatenate into one LazyFrame.
    pub fn from_orc_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_orc(paths[0].as_ref(), options);
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
        Self::read_with(lf, options)
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
    /// Use this for phased loading so "Scanning input" is instant; schema and partition handling are the schema phase's.
    pub fn scan_parquet_hive(path: &Path) -> Result<LazyFrame> {
        let is_glob = crate::source::expands_as_glob(path);
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
        let is_glob = crate::source::expands_as_glob(path);
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
                if crate::discover::is_parquet_path(&child) {
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
        let is_glob = crate::source::expands_as_glob(path);
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

    /// `#` on or off. Returns whether the view's frame changed and its rows need
    /// reading again: a sorted or filtered view of data with no place of its own
    /// numbers its rows once `#` is on.
    pub fn toggle_row_numbers(&mut self) -> bool {
        self.row_numbers = !self.row_numbers;
        if self.row_numbers && self.wants_view_numbers() && !self.view_numbered {
            self.drop_buffer();
            self.apply_transformations();
            return true;
        }
        false
    }

    /// Whether the view would number its rows itself with `#` on: it is sorted or
    /// filtered over the scan, and the scan's rows do not carry their place.
    fn wants_view_numbers(&self) -> bool {
        // A followed file's view is read from a mark, where a row index would count
        // from the mark rather than the file's start.
        // Nor one in a store or of many files, where a row index between the scan and
        // the filter would read every file; nor past what a row index counts to.
        let too_many = self
            .pristine_rows
            .or(self.num_rows_if_valid())
            .is_some_and(|rows| rows > crate::row_index::MAX_ROWS);
        self.scan_is_the_root()
            && self.follow.is_none()
            && !self.remote_source
            && self.remote_files.is_none()
            && self.parquet_count_dir.is_none()
            && !too_many
            && !self.drift_column_present
            && !self.source_rows_at_open
            && self.pushed_view().is_none()
            && (!self.filters.is_empty() || !self.sort_columns.is_empty() || !self.sort_ascending)
    }

    /// Whether the row-number column is shown.
    pub fn row_numbers(&self) -> bool {
        self.row_numbers
    }

    /// Row number display start (0 or 1); used by go-to-line to interpret user input.
    pub fn row_start_index(&self) -> usize {
        self.row_start_index
    }

    /// Decompress `path` into a new file in `temp_dir`, claimed through `writer` (see
    /// [`crate::unfinished`]) and given up, removed, once its open is stopped.
    fn decompress_compressed_csv_to_temp(
        path: &Path,
        compression: CompressionFormat,
        temp_dir: &Path,
        writer: &Writer,
    ) -> Result<Decompressed> {
        let stopped = || color_eyre::eyre::eyre!("Decompressing was stopped.");
        let Some((file, claim)) = writer.create(|| NamedTempFile::new_in(temp_dir))? else {
            return Err(stopped());
        };
        // Held from here, so a failure drops the file before the claim.
        let mut temp = Decompressed {
            file,
            _claim: claim,
        };
        let out = temp.file.as_file_mut();
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
        // A chunk at a time, so a stopped open stops writing rather than finishing a
        // file nobody will read.
        let mut chunk = vec![0u8; 1 << 20];
        loop {
            if writer.stopped() {
                return Err(stopped());
            }
            let read = match reader.read(&mut chunk) {
                Ok(0) => break,
                Ok(read) => read,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };
            std::io::Write::write_all(out, &chunk[..read])?;
        }
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

    /// Every reader-level CSV option, set one way on every route that scans lazily: a
    /// file, several files, a decompressed temp file, and a prefix in a bucket.
    pub(crate) fn configure_csv_reader(
        mut reader: LazyCsvReader,
        options: &OpenOptions,
        null_values: Option<&NullValues>,
    ) -> LazyCsvReader {
        reader = reader
            .with_separator(options.separator_or(b','))
            .with_comment_prefix(options.comment_char.as_deref().map(PlSmallStr::from));
        if let Some(rows) = options.header_rows() {
            // The header lines are read apart (`csv_header_names`); Polars starts
            // after the last of them, with `--skip-lines` counted from the same top
            // and `--skip-rows` counted after.
            let last = rows.iter().copied().max().unwrap_or(0);
            reader = reader
                .with_has_header(false)
                .with_skip_lines(last.max(options.skip_lines.unwrap_or(0)))
                .with_skip_rows_after_header(options.skip_rows.unwrap_or(0));
        } else {
            if let Some(skip_lines) = options.skip_lines {
                reader = reader.with_skip_lines(skip_lines);
            }
            if let Some(skip_rows) = options.skip_rows {
                reader = reader.with_skip_rows(skip_rows);
            }
            if let Some(has_header) = options.has_header {
                reader = reader.with_has_header(has_header);
            }
        }
        if let Some(n) = options.infer_schema_length {
            reader = reader.with_infer_schema_length(Some(n));
        }
        reader
            .with_ignore_errors(options.ignore_errors)
            // A followed file's later rows may have a field too many; they are counted
            // as not fitting rather than failing the read.
            .with_truncate_ragged_lines(options.follow)
            .with_try_parse_dates(options.csv_try_parse_dates())
            .with_null_values(null_values.cloned())
            // One byte that is not UTF-8 is a U+FFFD where it stands, not a file that
            // cannot be read past it.
            .with_encoding(CsvEncoding::LossyUtf8)
    }

    /// [`Self::configure_csv_reader`] for the in-memory readers, which take options
    /// rather than a builder.
    fn eager_csv_read_options(
        options: &OpenOptions,
        null_values: Option<&NullValues>,
    ) -> CsvReadOptions {
        let mut read_options = CsvReadOptions::default();
        if let Some(rows) = options.header_rows() {
            let last = rows.iter().copied().max().unwrap_or(0);
            read_options.has_header = false;
            read_options.skip_lines = last.max(options.skip_lines.unwrap_or(0));
            read_options.skip_rows_after_header = options.skip_rows.unwrap_or(0);
        } else {
            if let Some(skip_lines) = options.skip_lines {
                read_options.skip_lines = skip_lines;
            }
            if let Some(skip_rows) = options.skip_rows {
                read_options.skip_rows = skip_rows;
            }
            if let Some(has_header) = options.has_header {
                read_options.has_header = has_header;
            }
        }
        if let Some(n) = options.infer_schema_length {
            read_options.infer_schema_length = Some(n);
        }
        read_options.ignore_errors = options.ignore_errors;
        read_options.map_parse_options(|opts| {
            opts.with_separator(options.separator_or(b','))
                .with_comment_prefix(
                    options
                        .comment_char
                        .as_deref()
                        .map(polars::io::csv::read::CommentPrefix::new_from_str),
                )
                .with_try_parse_dates(options.csv_try_parse_dates())
                .with_null_values(null_values.cloned())
                .with_encoding(CsvEncoding::LossyUtf8)
        })
    }

    /// The columns a CSV reader will produce, from one row, for building null_values
    /// when both global and per-column are set.
    pub(crate) fn csv_schema_for_null_values(
        reader: LazyCsvReader,
        options: &OpenOptions,
    ) -> Result<Arc<Schema>> {
        let mut lf =
            Self::configure_csv_reader(reader.with_n_rows(Some(1)), options, None).finish()?;
        lf.collect_schema().map_err(color_eyre::eyre::Report::from)
    }

    /// Build Polars NullValues from options, for the CSV at `path` with the header
    /// lines `header` read from it.
    fn build_null_values_for_csv(
        options: &OpenOptions,
        path: &Path,
        header: Option<&[String]>,
    ) -> Result<Option<NullValues>> {
        Self::build_null_values_with(options, header, || {
            Self::csv_schema_for_null_values(Self::csv_reader_of(path)?, options)
        })
    }

    /// Build Polars NullValues from options. `schema` is the reader's own columns, read
    /// only when a spec names a column: the user names it as it is shown (trimmed, or
    /// from `--header-rows`), and the reader knows it by what it parsed.
    pub(crate) fn build_null_values_with(
        options: &OpenOptions,
        header: Option<&[String]>,
        schema: impl FnOnce() -> Result<Arc<Schema>>,
    ) -> Result<Option<NullValues>> {
        let specs = match &options.null_values {
            None => return Ok(None),
            Some(s) if s.is_empty() => return Ok(None),
            Some(s) => s.as_slice(),
        };
        let (global, mut per_column) = Self::parse_null_value_specs(specs);
        if per_column.is_empty() {
            return Ok(Self::build_polars_null_values(&global, &per_column, None));
        }
        let schema = match schema() {
            Ok(schema) => schema,
            // Nothing follows the header lines: no value to read as null.
            Err(e)
                if header.is_some()
                    && matches!(
                        e.downcast_ref::<PolarsError>(),
                        Some(PolarsError::NoData(_))
                    ) =>
            {
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let raw: Vec<PlSmallStr> = schema.iter_names().cloned().collect();
        let shown = crate::csv_dialect::shown_names(&raw, header);
        for (column, _) in per_column.iter_mut() {
            if let Some(i) = shown.iter().position(|s| s == column) {
                *column = raw[i].to_string();
            }
        }
        Ok(Self::build_polars_null_values(
            &global,
            &per_column,
            Some(schema.as_ref()),
        ))
    }

    /// The null values `--null` gives the column shown as `column`.
    pub(crate) fn csv_null_values_for(options: &OpenOptions, column: &str) -> Vec<String> {
        let (global, per_column) =
            Self::parse_null_value_specs(options.null_values.as_deref().unwrap_or_default());
        let mut values: Vec<String> = per_column
            .into_iter()
            .filter(|(c, _)| c == column)
            .map(|(_, v)| v)
            .collect();
        values.extend(global);
        values
    }

    /// The names `--header-rows` gives the columns of the CSV `source` holds, or
    /// `None` when it is not in effect.
    fn csv_header_names<R: std::io::BufRead>(
        options: &OpenOptions,
        source: impl FnOnce() -> std::io::Result<R>,
    ) -> Result<Option<Vec<String>>> {
        let Some(rows) = options.header_rows() else {
            return Ok(None);
        };
        Ok(Some(crate::csv_dialect::header_names(
            source()?,
            rows,
            &options.header_join,
            options.separator_or(b','),
            options.comment_char.as_deref(),
        )?))
    }

    /// A lazy CSV reader of the file at `path`, by its path; or, when the file ends in
    /// a run of NULs, of its text before them, mapped and read in place.
    pub(crate) fn csv_reader_of(path: &Path) -> Result<LazyCsvReader> {
        let glob = crate::source::expands_as_glob(path);
        if !glob
            && path.is_file()
            && let Ok(Some(text)) = crate::nul_tail::text_buffer(path)
        {
            return Ok(LazyCsvReader::new_with_sources(
                polars::lazy::dsl::ScanSources::Buffers(Arc::from([text])),
            ));
        }
        Ok(LazyCsvReader::new(PlRefPath::try_from_path(path)?).with_glob(glob))
    }

    /// [`Self::csv_header_names`] for a file on disk, compressed with `compression`
    /// or not.
    pub(crate) fn csv_header_names_of(
        options: &OpenOptions,
        path: &Path,
        compression: Option<CompressionFormat>,
    ) -> Result<Option<Vec<String>>> {
        Self::csv_header_names(options, || Self::text_source(path, compression))
    }

    /// The text of the file at `path`, through its decompressor when it has one.
    pub(crate) fn text_source(
        path: &Path,
        compression: Option<CompressionFormat>,
    ) -> std::io::Result<Box<dyn std::io::BufRead>> {
        let file = File::open(path)?;
        if compression.is_none()
            && let Some(len) = crate::nul_tail::text_len(&file)?
        {
            return Ok(Box::new(BufReader::new(file.take(len))));
        }
        let file = BufReader::new(file);
        Ok(match compression {
            None => Box::new(file),
            Some(CompressionFormat::Gzip) => {
                Box::new(BufReader::new(flate2::read::GzDecoder::new(file)))
            }
            Some(CompressionFormat::Zstd) => {
                Box::new(BufReader::new(zstd::Decoder::with_buffer(file)?))
            }
            Some(CompressionFormat::Bzip2) => {
                Box::new(BufReader::new(bzip2::read::BzDecoder::new(file)))
            }
            Some(CompressionFormat::Xz) => {
                Box::new(BufReader::new(xz2::read::XzDecoder::new(file)))
            }
        })
    }

    /// What every CSV read does once Polars has parsed it: name the columns (trimmed,
    /// or from `--header-rows`), skip the padding after a delimiter, type the text
    /// columns, and drop the footer.
    /// `read` gets the steps that Python can repeat.
    fn finish_csv_frame(
        lf: LazyFrame,
        options: &OpenOptions,
        header: Option<&[String]>,
        read: &mut Vec<String>,
        typing: &mut Typing,
    ) -> Result<LazyFrame> {
        let lf = Self::name_csv_columns(lf, header, Some(read))?;
        Self::finish_csv_values(lf, options, read, typing)
    }

    /// [`crate::csv_dialect::name_columns`], with the renames as Python in `read`.
    /// Names from `--header-rows` are not recorded: Copy as Python does not write that
    /// read.
    fn name_csv_columns(
        mut lf: LazyFrame,
        header: Option<&[String]>,
        read: Option<&mut Vec<String>>,
    ) -> Result<LazyFrame> {
        if let (None, Some(read)) = (header, read) {
            let raw: Vec<PlSmallStr> = lf.collect_schema()?.iter_names().cloned().collect();
            let shown = crate::csv_dialect::shown_names(&raw, None);
            let renames: Vec<String> = raw
                .iter()
                .zip(&shown)
                .filter(|(raw, shown)| raw.as_str() != shown.as_str())
                .map(|(raw, shown)| format!("{}: {}", py_str(raw), py_str(shown)))
                .collect();
            if !renames.is_empty() {
                read.push(format!(".rename({{{}}})", renames.join(", ")));
            }
        }
        Ok(crate::csv_dialect::name_columns(lf, header)?)
    }

    /// [`Self::finish_csv_frame`] after the names, for frames already named: several
    /// files are named one at a time and stacked first.
    fn finish_csv_values(
        mut lf: LazyFrame,
        options: &OpenOptions,
        read: &mut Vec<String>,
        typing: &mut Typing,
    ) -> Result<LazyFrame> {
        if options.skip_initial_space {
            lf = crate::csv_dialect::skip_initial_space(lf, |column| {
                Self::csv_null_values_for(options, column)
            })?;
        }
        // Read without a header (`H`), the columns have no names to derive from, or to
        // type by. Derived columns read the file's text, before any column is typed.
        let spec = options
            .delimited
            .as_ref()
            .filter(|_| options.has_header != Some(false))
            .map(|read| read.delimited());
        if let Some(spec) = spec {
            lf = spec.derive(lf)?;
            lf = Self::declare_types(lf, &spec.types, typing)?;
        }
        let typed: Vec<String> = typing.typed.iter().map(|t| t.column.clone()).collect();
        lf = Self::apply_parse_strings_to_csv_lazyframe(lf, options, read, &typed, typing)?;
        Self::apply_skip_tail_rows_csv(lf, options)
    }

    /// `lf` with each column `types` names read as its type, lazily; `typing` records
    /// them and the frame before, for the count of the values that did not fit, and a
    /// note names the ones the frame does not have.
    fn declare_types(
        mut lf: LazyFrame,
        types: &[(String, crate::column_types::ColumnType)],
        typing: &mut Typing,
    ) -> Result<LazyFrame> {
        if types.is_empty() {
            return Ok(lf);
        }
        let schema = lf.collect_schema()?;
        let mut exprs = Vec::with_capacity(types.len());
        let mut missing = Vec::new();
        for (name, ty) in types {
            match schema.get(name.as_str()) {
                Some(from) => {
                    exprs.push(ty.expr(name, from).alias(name.as_str()));
                    typing.typed.push(crate::column_types::Typed {
                        column: name.clone(),
                        ty: ty.clone(),
                        from: from.clone(),
                    });
                }
                None => missing.push(name.as_str()),
            }
        }
        if !missing.is_empty() {
            typing.notes.push(crate::notes::Note {
                summary: format!(
                    "typed in the spec, not in the file: {}",
                    crate::notes::some_names(&missing)
                ),
                scope: "the spec's [columns]".to_string(),
                read_as_text: None,
                passed_over: None,
            });
        }
        if exprs.is_empty() {
            return Ok(lf);
        }
        typing.source = Some(lf.clone());
        Ok(lf.with_columns(exprs))
    }

    /// `reader`, the scan of the file at `path`, reading some columns as text: those a
    /// spec gives a type, which [`Self::declare_types`] types, a value that does not fit
    /// null rather than a failed read; and, while `read.infer_types` types text, those
    /// whose first rows hold a number with a leading zero (`02134`), which Polars would
    /// read as an integer and lose. `window` is those rows when the read has them;
    /// otherwise they are read, up to the rows a scan infers its types from.
    pub(crate) fn scan_some_as_text(
        reader: LazyCsvReader,
        options: &OpenOptions,
        header: Option<&[String]>,
        path: &Path,
        window: Option<&[Vec<String>]>,
        text: &mut Vec<String>,
    ) -> Result<LazyCsvReader> {
        if options.has_header == Some(false) {
            return Ok(reader);
        }
        let names: Vec<String> = options
            .delimited
            .as_ref()
            .map(|read| {
                read.delimited()
                    .types
                    .iter()
                    .map(|(name, _)| name.clone())
                    .collect()
            })
            .unwrap_or_default();
        let zeros: Vec<usize> = match &options.parse_strings {
            None => Vec::new(),
            Some(_) => {
                let read;
                let window = match window {
                    Some(window) => window,
                    None => {
                        read = crate::spec_union::head_window(path, options).unwrap_or_default();
                        &read
                    }
                };
                let width = window.iter().map(Vec::len).max().unwrap_or(0);
                (0..width)
                    .filter(|&at| {
                        window.iter().any(|row| {
                            row.get(at)
                                .is_some_and(|v| crate::column_types::has_leading_zero(v))
                        })
                    })
                    .collect()
            }
        };
        if names.is_empty() && zeros.is_empty() {
            return Ok(reader);
        }
        let header = header.map(<[String]>::to_vec);
        let target = options.parse_strings.clone();
        let read_as_text = Arc::new(std::sync::Mutex::new(Vec::new()));
        let said = read_as_text.clone();
        let reader = reader.with_schema_modify(move |mut schema| {
            let raw: Vec<PlSmallStr> = schema.iter_names().cloned().collect();
            let shown = crate::csv_dialect::shown_names(&raw, header.as_deref());
            for (at, (raw, shown)) in raw.iter().zip(&shown).enumerate() {
                let inferred = match &target {
                    Some(ParseStringsTarget::All) => true,
                    Some(ParseStringsTarget::Columns(columns)) => columns.contains(shown),
                    None => false,
                };
                if names.contains(shown) || (inferred && zeros.contains(&at)) {
                    schema.with_column(raw.clone(), DataType::String);
                    if let Ok(mut said) = said.lock() {
                        said.push(raw.to_string());
                    }
                }
            }
            Ok(schema)
        })?;
        if let Ok(mut read) = read_as_text.lock() {
            text.append(&mut read);
        }
        Ok(reader)
    }

    /// If options.skip_tail_rows is set, run a count query and slice the LazyFrame to drop that many rows from the end. Used for CSV with trailing garbage/footer.
    pub(crate) fn apply_skip_tail_rows_csv(
        lf: LazyFrame,
        options: &OpenOptions,
    ) -> Result<LazyFrame> {
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

    /// The first date format `sample` reads in. `None` when none does, so Polars is
    /// never handed `format: None`, which can fail.
    fn infer_date_format_from_sample(sample: &str) -> Option<&'static str> {
        crate::column_types::formats_reading(&DataType::Date, sample)
            .first()
            .copied()
    }

    fn infer_datetime_format_from_sample(sample: &str) -> Option<&'static str> {
        crate::column_types::formats_reading(
            &DataType::Datetime(TimeUnit::Microseconds, None),
            sample,
        )
        .first()
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

    fn infer_time_format_from_sample(sample: &str) -> Option<&'static str> {
        crate::column_types::formats_reading(&DataType::Time, sample)
            .first()
            .copied()
    }

    /// Apply trim and type inference to CSV string columns when --infer-types is enabled.
    /// Samples up to `options.parse_strings_sample_rows` rows to infer types, then overlays lazy exprs (trim then cast) on the LazyFrame.
    fn apply_parse_strings_to_csv_lazyframe(
        lf: LazyFrame,
        options: &OpenOptions,
        read: &mut Vec<String>,
        except: &[String],
        typing: &mut Typing,
    ) -> Result<LazyFrame> {
        let Some(target) = &options.parse_strings else {
            return Ok(lf);
        };
        let before = lf.clone();
        let mut typed = Vec::new();
        let lf = Self::type_string_columns(
            lf,
            target,
            options.parse_strings_sample_rows,
            StringTypes {
                dates: options.parse_dates,
                numbers: true,
            },
            read,
            except,
            &mut typed,
        )?;
        // The columns it typed are counted as the spec's are, over the frame before
        // either: the spec's typing leaves these columns as they were read.
        if !typed.is_empty() {
            typing.source.get_or_insert(before);
            typing.typed.extend(typed);
        }
        Ok(lf)
    }

    /// Dates and timestamps a JSON file holds as strings, typed the way a CSV's are.
    /// JSON already says which values are numbers, so a string only ever becomes a
    /// date, datetime or time, and one that is none of those is left as it was read.
    pub(crate) fn apply_parse_dates_to_json_lazyframe(
        lf: LazyFrame,
        options: &OpenOptions,
        read: &mut Vec<String>,
    ) -> Result<LazyFrame> {
        if !options.parse_dates {
            return Ok(lf);
        }
        Self::type_string_columns(
            lf,
            &ParseStringsTarget::All,
            options.parse_strings_sample_rows,
            StringTypes {
                dates: true,
                numbers: false,
            },
            read,
            &[],
            &mut Vec::new(),
        )
    }

    /// A string column read as a microsecond Datetime with `format`. Exact, so a naive
    /// format cannot match the front of a value that carries an offset and drop it; not
    /// strict, so a value past the sample that does not parse is null.
    fn datetime_from_str(expr: Expr, format: &str) -> Expr {
        expr.str().to_datetime(
            Some(TimeUnit::Microseconds),
            None,
            StrptimeOptions {
                format: Some(PlSmallStr::from(format)),
                strict: false,
                exact: true,
                cache: true,
            },
            lit(PlSmallStr::from_static("raise")),
        )
    }

    /// The rows string inference reads: the first `sample_rows` of the `targets` only,
    /// trimmed so inference sees "1" not " 1 ", with blanks as null so "all null" and
    /// the accept test see normalized values. Only the targets are selected, so the
    /// other columns are neither decoded nor held; the frame the table shows keeps them.
    fn string_inference_sample(
        lf: LazyFrame,
        targets: &[String],
        sample_rows: usize,
    ) -> PolarsResult<DataFrame> {
        let whitespace_pat = lit(PlSmallStr::from_static(" \t\n\r"));
        let trimmed: Vec<Expr> = targets
            .iter()
            .map(|c| {
                let name = PlSmallStr::from(c.as_str());
                col(name.clone())
                    .str()
                    .strip_chars(whitespace_pat.clone())
                    .alias(name)
            })
            .collect();
        let blank_to_null: Vec<Expr> = targets
            .iter()
            .map(|c| {
                let name = PlSmallStr::from(c.as_str());
                when(col(name.clone()).eq(lit(PlSmallStr::from_static(""))))
                    .then(Null {}.lit())
                    .otherwise(col(name.clone()))
                    .alias(name)
            })
            .collect();
        lf.limit(sample_rows as u32)
            .select(trimmed)
            .with_columns(blank_to_null)
            .collect()
    }

    /// Type string columns from the first `sample_rows` rows: trim, then keep the first
    /// of Date, Datetime, Time, Duration, Int64 and Float64 (as `types` allows) that
    /// parses every sampled value, as lazy expressions over `lf`.
    fn type_string_columns(
        lf: LazyFrame,
        target: &ParseStringsTarget,
        sample_rows: usize,
        types: StringTypes,
        read: &mut Vec<String>,
        except: &[String],
        typed: &mut Vec<crate::column_types::Typed>,
    ) -> Result<LazyFrame> {
        // The scan already inferred the schema; the sample below is the one read.
        let schema = lf.clone().collect_schema()?;
        let string_cols: Vec<String> = schema
            .iter()
            // A column given a type keeps it.
            .filter(|(name, _)| !except.iter().any(|e| e == name.as_str()))
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
        let sample_df = Self::string_inference_sample(lf.clone(), &target_cols, sample_rows)?;
        log::debug!(
            target: "datui",
            "string inference sample: {} rows x {} columns for {} targets, {} bytes",
            sample_df.height(),
            sample_df.width(),
            target_cols.len(),
            sample_df.estimated_size()
        );
        let mut exprs = Vec::with_capacity(target_cols.len());
        // The same typing as Python, for Copy as Python.
        let mut python = Vec::with_capacity(target_cols.len());
        for col_name in &target_cols {
            let name = PlSmallStr::from(col_name.as_str());
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
                        // `02134`, `007`: a ZIP code or an ID, not a number.
                        let zeros = str_ca
                            .iter()
                            .flatten()
                            .any(crate::column_types::has_leading_zero);
                        let (mut t, mut date_fmt, mut datetime_fmt, mut time_fmt) = match str_ca
                            .as_date(None, true)
                        {
                            Ok(as_date) if types.dates && accept_type(as_date.null_count()) => {
                                let fmt = first_val.and_then(Self::infer_date_format_from_sample);
                                if fmt.is_some() {
                                    (InferredType::Date, fmt.map(String::from), None, None)
                                } else {
                                    (InferredType::String, None, None, None)
                                }
                            }
                            _ => (InferredType::String, None, None, None),
                        };
                        if matches!(t, InferredType::String)
                            && types.dates
                            && let Some(fmt) =
                                first_val.and_then(Self::infer_datetime_format_from_sample)
                        {
                            // Judged by the expression the table will run, so a column
                            // whose values disagree (an offset on some, none on others)
                            // fails here and stays text.
                            let parsed = sample_df
                                .clone()
                                .lazy()
                                .select([Self::datetime_from_str(col(name.clone()), fmt)])
                                .collect()?;
                            if accept_type(parsed.column(col_name.as_str())?.null_count()) {
                                (t, date_fmt, datetime_fmt, time_fmt) =
                                    (InferredType::Datetime, None, Some(fmt.to_string()), None);
                            }
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
                        if matches!(t, InferredType::String) && types.numbers {
                            let duration_ca = Self::string_chunked_to_duration_ns(str_ca);
                            (t, date_fmt, datetime_fmt, time_fmt) =
                                if accept_type(duration_ca.null_count()) {
                                    (InferredType::Duration, None, None, None)
                                } else {
                                    (InferredType::String, None, None, None)
                                };
                        }
                        if matches!(t, InferredType::String) && types.numbers && !zeros {
                            (t, date_fmt, datetime_fmt, time_fmt) =
                                match s.strict_cast(&DataType::Int64) {
                                    Ok(as_int) if accept_type(as_int.null_count()) => {
                                        (InferredType::Int64, None, None, None)
                                    }
                                    _ => (InferredType::String, None, None, None),
                                };
                        }
                        if matches!(t, InferredType::String) && types.numbers && !zeros {
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
            let trimmed = format!(
                "pl.col({}).str.strip_chars(\" \\t\\n\\r\")",
                py_str(col_name)
            );
            let blank_null = format!("{trimmed}.replace(\"\", None)");
            let format_arg = |f: &Option<String>| match f {
                Some(f) => format!("{}, ", py_str(f)),
                None => String::new(),
            };
            python.push(match &inferred {
                InferredType::Date => format!(
                    "{blank_null}.str.to_date({}strict=False)",
                    format_arg(&date_fmt)
                ),
                InferredType::Datetime => format!(
                    "{blank_null}.str.to_datetime({}time_unit=\"us\", strict=False)",
                    format_arg(&datetime_fmt)
                ),
                InferredType::Time => format!(
                    "{blank_null}.str.to_time({}strict=False)",
                    format_arg(&time_fmt)
                ),
                InferredType::Duration => crate::python_script::py_comment(&format!(
                    "{col_name}: datui reads these as durations (\"1d2h\"); Polars has no parser for them"
                )),
                InferredType::Int64 => {
                    format!("{blank_null}.cast(pl.Int64, strict=False)")
                }
                InferredType::Float64 => {
                    format!("{blank_null}.cast(pl.Float64, strict=False)")
                }
                InferredType::String if types.numbers => trimmed.clone(),
                InferredType::String => String::new(),
            });
            // The one way a column is given a type: the spec's and the table's too.
            let ty = |dtype: DataType, format: Option<String>| crate::column_types::ColumnType {
                dtype,
                format,
            };
            let ty = match inferred {
                InferredType::Date => ty(DataType::Date, date_fmt),
                InferredType::Datetime => ty(
                    DataType::Datetime(TimeUnit::Microseconds, None),
                    datetime_fmt,
                ),
                InferredType::Time => ty(DataType::Time, time_fmt),
                InferredType::Duration => ty(DataType::Duration(TimeUnit::Nanoseconds), None),
                InferredType::Int64 => ty(DataType::Int64, None),
                InferredType::Float64 => ty(DataType::Float64, None),
                // Trimmed where every column is text; left as read where the
                // writer chose a string.
                InferredType::String if types.numbers => {
                    exprs.push(base.alias(name));
                    continue;
                }
                InferredType::String => continue,
            };
            let expr = ty.expr(col_name, &DataType::String).alias(name);
            typed.push(crate::column_types::Typed {
                column: col_name.clone(),
                ty,
                from: DataType::String,
            });
            exprs.push(expr);
        }
        let python: Vec<String> = python.into_iter().filter(|p| !p.is_empty()).collect();
        if !python.is_empty() {
            read.push(".with_columns(".to_string());
            read.extend(python.into_iter().map(|p| {
                if p.starts_with('#') {
                    format!("    {p}")
                } else {
                    format!("    {p},")
                }
            }));
            read.push(")".to_string());
        }
        Ok(lf.with_columns(exprs))
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_delimited(path, b',', options)
    }

    /// `path` decompressed to a temporary copy in `temp_dir`, written through `writer`:
    /// a stopped open stops the copy, and quitting removes it.
    pub(crate) fn decompress_to_copy(
        path: &Path,
        compression: CompressionFormat,
        temp_dir: &Path,
        writer: &Writer,
    ) -> Result<crate::download::TempDownload> {
        let Decompressed { file, _claim } =
            Self::decompress_compressed_csv_to_temp(path, compression, temp_dir, writer)?;
        Ok(crate::download::TempDownload::held(file, Some(_claim)))
    }

    /// As [`Self::from_delimited`], for an open: a compressed file is decompressed
    /// through `writer`, so the open's stop and quitting reach the copy.
    pub(crate) fn from_delimited_for_open(
        path: &Path,
        delimiter: u8,
        options: &OpenOptions,
        writer: &Writer,
    ) -> Result<Self> {
        Self::read_delimited(path, delimiter, options, writer)
    }

    /// A delimited text file, split on `delimiter` (its format's separator) unless
    /// `--delimiter` says otherwise. CSV, TSV and PSV are one reader, so every CSV
    /// option means the same thing for all three.
    pub fn from_delimited(path: &Path, delimiter: u8, options: &OpenOptions) -> Result<Self> {
        Self::read_delimited(path, delimiter, options, &Writer::default())
    }

    fn read_delimited(
        path: &Path,
        delimiter: u8,
        options: &OpenOptions,
        writer: &Writer,
    ) -> Result<Self> {
        // Settled once here: every reader below asks `options.separator_or(b',')`.
        let options = &OpenOptions {
            delimiter: Some(options.separator_or(delimiter)),
            ..options.clone()
        };

        // Determine compression format: explicit option, or auto-detect from extension
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path));

        if let Some(compression) = compression {
            if options.decompress_in_memory {
                // Eager read: decompress into memory, then CSV read
                let (df, header) = match compression {
                    CompressionFormat::Gzip | CompressionFormat::Zstd => {
                        let header = Self::csv_header_names_of(options, path, Some(compression))?;
                        let nv = Self::build_null_values_for_csv(options, path, header.as_deref())?;
                        let read_options = Self::eager_csv_read_options(options, nv.as_ref());
                        let df = crate::csv_dialect::read_after_header(
                            read_options
                                .try_into_reader_with_file_path(Some(path.into()))?
                                .finish(),
                            header.as_deref(),
                        )?;
                        (df, header)
                    }
                    CompressionFormat::Bzip2 | CompressionFormat::Xz => {
                        let file = BufReader::new(File::open(path)?);
                        let mut decompressed = Vec::new();
                        if compression == CompressionFormat::Bzip2 {
                            bzip2::read::BzDecoder::new(file).read_to_end(&mut decompressed)?;
                        } else {
                            xz2::read::XzDecoder::new(file).read_to_end(&mut decompressed)?;
                        }
                        crate::nul_tail::trim(&mut decompressed);
                        let header = Self::csv_header_names(options, || {
                            Ok(std::io::Cursor::new(decompressed.as_slice()))
                        })?;
                        // Column names for per-column null values come from the bytes: the file on
                        // disk is still compressed.
                        let nv = Self::build_null_values_with(options, header.as_deref(), || {
                            let one_row =
                                Self::eager_csv_read_options(options, None).with_n_rows(Some(1));
                            let df = CsvReader::new(std::io::Cursor::new(decompressed.as_slice()))
                                .with_options(one_row)
                                .finish()?;
                            Ok(df.schema().clone())
                        })?;
                        let read_options = Self::eager_csv_read_options(options, nv.as_ref());
                        let df = crate::csv_dialect::read_after_header(
                            CsvReader::new(std::io::Cursor::new(decompressed))
                                .with_options(read_options)
                                .finish(),
                            header.as_deref(),
                        )?;
                        (df, header)
                    }
                };
                let mut read = Vec::new();
                let mut typing = Typing::default();
                let lf = Self::finish_csv_frame(
                    df.lazy(),
                    options,
                    header.as_deref(),
                    &mut read,
                    &mut typing,
                )?;
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
                state.read_python = read;
                state.take_typing(typing);
                Ok(state)
            } else {
                // Decompress to temp file, then lazy scan
                let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
                let temp =
                    Self::decompress_compressed_csv_to_temp(path, compression, &temp_dir, writer)?;
                let mut state = Self::scan_csv_file(temp.path(), options)?;
                state.decompress_temp_file = Some(Arc::new(temp));
                Ok(state)
            }
        } else {
            // For uncompressed files, use lazy scanning (more efficient)
            Self::scan_csv_file(path, options)
        }
    }

    /// A compressed file read as lines: decompressed once to a file in `--temp-dir`, or
    /// into memory with `[read] decompress_in_memory`, then indexed.
    pub(crate) fn from_lines_decompressed(
        path: &Path,
        options: &OpenOptions,
        writer: &Writer,
    ) -> Result<(Self, crate::members::Opened)> {
        let compression = options
            .compression
            .or_else(|| CompressionFormat::from_extension(path))
            .ok_or_else(|| color_eyre::eyre::eyre!("{} is not compressed", path.display()))?;
        let (lines, temp) = if options.decompress_in_memory {
            let mut bytes = Vec::new();
            let read = std::sync::atomic::AtomicU64::new(0);
            crate::gps::open_reader(path, options, &read)?.read_to_end(&mut bytes)?;
            let name = path
                .file_stem()
                .map_or_else(String::new, |n| n.to_string_lossy().into_owned());
            let bytes = Arc::new(crate::fixed_records::Bytes::Owned(bytes));
            (crate::lines::Lines::from_bytes(vec![(name, bytes)]), None)
        } else {
            let temp_dir = options.temp_dir.clone().unwrap_or_else(std::env::temp_dir);
            let temp =
                Self::decompress_compressed_csv_to_temp(path, compression, &temp_dir, writer)?;
            let lines = crate::lines::Lines::open(&[temp.path().to_path_buf()], false)?;
            (lines, Some(Arc::new(temp)))
        };
        let lines = Arc::new(lines);
        let opened = crate::lines::opened(&lines, options);
        let mut state = Self::new(
            lines.lazy(),
            options.pages_lookahead,
            options.pages_lookback,
            options.max_buffered_rows,
            options.max_buffered_mb,
            options.polars_streaming,
        )?;
        state.row_numbers = options.row_numbers;
        state.row_start_index = options.row_start_index;
        state.decompress_temp_file = temp;
        Ok((state, opened))
    }

    /// One uncompressed delimited file, scanned lazily. The frame is finished before
    /// the state is made from it, so the column order is of the names shown.
    fn scan_csv_file(path: &Path, options: &OpenOptions) -> Result<Self> {
        let header = Self::csv_header_names_of(options, path, None)?;
        let nv = Self::build_null_values_for_csv(options, path, header.as_deref())?;
        let reader = Self::csv_reader_of(path)?;
        let reader = Self::configure_csv_reader(reader, options, nv.as_ref());
        let mut typing = Typing::default();
        let lf = Self::scan_some_as_text(
            reader,
            options,
            header.as_deref(),
            path,
            None,
            &mut typing.text,
        )?
        .finish()?;
        let mut read = Vec::new();
        let lf = Self::finish_csv_frame(lf, options, header.as_deref(), &mut read, &mut typing)?;
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
        state.read_python = read;
        state.take_typing(typing);
        Ok(state)
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
        let reader = LazyCsvReader::new(pl_path).with_glob(crate::source::expands_as_glob(path));
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
        // Each file is named from its own header, so files whose names are padded
        // differently, or whose header lines say the same thing, stack by name.
        let mut lazy_frames = Vec::with_capacity(paths.len());
        // Python reads the files as one scan: the first file's renames stand for all.
        let mut read = Vec::new();
        // Files with nothing in them: no header, so no columns to stack.
        let mut no_header: Vec<&Path> = Vec::new();
        // Read through a spec, each file's header pass reads its units and the lines its
        // types are inferred from too, for lining the files up by name.
        let spec = options.delimited.as_ref().map(|read| read.delimited());
        let mut heads = Vec::new();
        let mut read_text = Vec::new();
        for p in paths {
            let p = p.as_ref();
            let in_file = |e: color_eyre::Report| crate::error_display::in_file(p, e);
            let head_read = match spec {
                Some(spec) => crate::spec_union::read_head(p, options, spec).map(Some),
                None => Ok(None),
            };
            let head = match head_read {
                Err(e) if crate::csv_dialect::is_blank_file(&e) => {
                    no_header.push(p);
                    continue;
                }
                head => head.map_err(in_file)?,
            };
            let header = match &head {
                Some(head) => head.names.clone(),
                None => match Self::csv_header_names_of(options, p, None) {
                    Err(e) if crate::csv_dialect::is_blank_file(&e) => {
                        no_header.push(p);
                        continue;
                    }
                    header => header.map_err(in_file)?,
                },
            };
            let nv =
                Self::build_null_values_for_csv(options, p, header.as_deref()).map_err(in_file)?;
            let reader = Self::csv_reader_of(p).map_err(in_file)?;
            let reader = Self::configure_csv_reader(reader, options, nv.as_ref());
            let window = head.as_ref().map(|head| head.window.as_slice());
            // Python reads the files as one scan: the first file's columns stand for all.
            let mut text = Vec::new();
            let lf =
                Self::scan_some_as_text(reader, options, header.as_deref(), p, window, &mut text)
                    .map_err(in_file)?
                    .finish()
                    .map_err(|e| in_file(e.into()))?;
            if lazy_frames.is_empty() {
                read_text = text;
            }
            let record = lazy_frames.is_empty().then_some(&mut read);
            // Polars reads the header line itself: a file with none has no columns, or
            // one with a blank name.
            if header.is_none() {
                let raw = lf.clone().collect_schema();
                let headless = match &raw {
                    Err(PolarsError::NoData(_)) => true,
                    Ok(schema) => {
                        schema.is_empty()
                            || (schema.len() == 1
                                && schema.iter_names().all(|n| n.trim().is_empty()))
                    }
                    Err(_) => false,
                };
                if headless && Self::is_blank_text(p) {
                    no_header.push(p);
                    continue;
                }
            }
            let named = Self::name_csv_columns(lf, header.as_deref(), record).map_err(in_file)?;
            lazy_frames.push(named);
            heads.extend(head);
        }
        if lazy_frames.is_empty() {
            return Err(color_eyre::eyre::eyre!(
                "none of these {} files has a header: each is empty, or blank",
                paths.len()
            ));
        }
        let mut notes: Vec<crate::notes::Note> =
            crate::notes::no_header(&no_header).into_iter().collect();
        let mut units = None;
        if spec.is_some() && lazy_frames.len() > 1 {
            let lined = crate::spec_union::line_up(lazy_frames, &heads, options)?;
            lazy_frames = lined.frames;
            notes.extend(lined.notes);
            units = Some(lined.units);
        }
        let mut typing = Typing {
            text: read_text,
            ..Typing::default()
        };
        let lf = Self::finish_csv_values(
            polars::prelude::concat(lazy_frames.as_slice(), Self::union_of_files())?,
            options,
            &mut read,
            &mut typing,
        )?;
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
        state.read_python = read;
        state.read_notes = notes;
        state.read_units = units;
        state.take_typing(typing);
        Ok(state)
    }

    /// Whether the text of the file at `path`, to its NUL padding, is no more than
    /// white space. Read up to a bound: past it, the file holds something.
    fn is_blank_text(path: &Path) -> bool {
        const MOST: u64 = 64 << 10;
        let mut text = Vec::new();
        Self::text_source(path, None)
            .and_then(|source| source.take(MOST + 1).read_to_end(&mut text))
            .is_ok_and(|n| n as u64 <= MOST && text.iter().all(u8::is_ascii_whitespace))
    }

    pub fn from_json(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_json_with_format(path, options, JsonFormat::Json)
    }

    pub fn from_json_lines(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_json_with_format(path, options, JsonFormat::JsonLines)
    }

    fn from_json_with_format(
        path: &Path,
        options: &OpenOptions,
        format: JsonFormat,
    ) -> Result<Self> {
        let file = File::open(path)?;
        let lf = JsonReader::new(file)
            .with_json_format(format)
            .finish()?
            .lazy();
        Self::read_with(lf, options)
    }

    /// Load multiple JSON (array) files and concatenate into one LazyFrame.
    pub fn from_json_paths(paths: &[impl AsRef<Path>], options: &OpenOptions) -> Result<Self> {
        Self::from_json_with_format_paths(paths, options, JsonFormat::Json)
    }

    /// Load multiple JSON Lines files and concatenate into one LazyFrame.
    pub fn from_json_lines_paths(
        paths: &[impl AsRef<Path>],
        options: &OpenOptions,
    ) -> Result<Self> {
        Self::from_json_with_format_paths(paths, options, JsonFormat::JsonLines)
    }

    fn from_json_with_format_paths(
        paths: &[impl AsRef<Path>],
        options: &OpenOptions,
        format: JsonFormat,
    ) -> Result<Self> {
        if paths.is_empty() {
            return Err(color_eyre::eyre::eyre!("No paths provided"));
        }
        if paths.len() == 1 {
            return Self::from_json_with_format(paths[0].as_ref(), options, format);
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
        Self::read_with(lf, options)
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
            self.proximity_threshold = self.proximity();
        }

        // Run len() only when lf has changed (query, filter, sort, pivot, melt, reset, drill).
        if !self.num_rows_valid {
            self.num_rows = match collect_lazy(row_count_lf(&self.lf), self.polars_streaming) {
                Ok(df) => {
                    // The frame counts, so there is nothing wrong with it: retire a
                    // failure left by the frame this one replaced. `load_buffer` ends
                    // the same way, but the zero-row path below returns before it.
                    self.error = None;
                    match df.get(0) {
                        Some(col) => match col.first() {
                            Some(AnyValue::UInt64(len)) => *len as usize,
                            _ => 0,
                        },
                        _ => 0,
                    }
                }
                // A count that fails means the frame itself is broken — a sort or a
                // column order naming a column the query removed, say. Zero rows is the
                // wrong thing to report: it blanks the table and returns below, before
                // `load_buffer`, the only other place that records a failure. The caller
                // is then told nothing, so a broken frame reads as an empty one. Say what
                // went wrong instead.
                Err(e) => {
                    self.error = Some(e);
                    0
                }
            };
            self.num_rows_valid = true;
            self.remember_pristine_count();
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
    /// stub literal ([`binary_stub`]) so their blobs are never read. Used both for the display
    /// buffer (keeps scroll/jump collects fast) and for analysis (describe/distribution/
    /// correlation), where reading multi-GB blobs across partitions would otherwise exhaust
    /// memory and freeze the process. The full bytes stay available through `lf` for export.
    pub(crate) fn binary_stub_exprs(&self) -> Vec<Expr> {
        self.column_order
            .iter()
            .map(|name| {
                if matches!(self.schema.get(name.as_str()), Some(DataType::Binary)) {
                    lit(binary_stub()).alias(name.as_str())
                } else {
                    col(name.as_str())
                }
            })
            .collect()
    }

    /// What the footers said about each file, for the checks that measure which files
    /// hold which columns. Taken from the dataset as it is read *now*, so a column
    /// already read as text is no longer a conflict.
    ///
    /// `row_index_column` is left empty: only the caller knows which index its own
    /// frame carries, and every caller fills it in.
    fn quality_source_drift(&self) -> crate::data_quality::QualitySourceContext {
        crate::data_quality::QualitySourceContext {
            file_names: self.drift_files.clone(),
            file_starts: self.drift_file_starts.clone(),
            row_index_column: String::new(),
            file_group: self.drift_file_group.clone(),
            drift_groups: self.drift_groups.clone(),
            file_omitted: self
                .dataset_schema
                .as_ref()
                .map(|dataset| dataset.omitted.clone())
                .unwrap_or_default(),
            dataset_rows: self.drift_dataset_rows,
            footers_read: self
                .dataset_schema
                .as_ref()
                .map(|dataset| dataset.files)
                .unwrap_or_default(),
            // Attached by the run that promised the extra reads, not by every frame.
            conflict_scan: None,
        }
    }

    /// How many extra one-column file reads a full data-quality run would make for the
    /// values a type conflict hides. Zero when the dataset's files agree.
    pub(crate) fn quality_conflict_reads(&self) -> usize {
        if !self.drift_column_present
            || !self
                .dataset_at_open
                .as_ref()
                .is_some_and(crate::schema_union::DatasetSchema::drifts)
        {
            return 0;
        }
        crate::data_quality::conflict_reads(&self.drift_file_group, &self.drift_groups)
    }

    /// Reads one column of named files at the type each of them wrote it in, for the
    /// values a type conflict hides. `None` when the dataset's files all agree, or
    /// when this frame is not the dataset as it opened.
    pub(crate) fn quality_conflict_scan(&self) -> Option<crate::data_quality::QualityConflictScan> {
        let dataset = self.dataset_at_open.clone()?;
        if !self.drift_column_present || !dataset.drifts() {
            return None;
        }
        if let Some(remote) = self.remote_files.as_ref() {
            return Some(crate::data_quality::QualityConflictScan(
                remote.scan.clone(),
            ));
        }
        // Built from the dataset as its footers found it, for the same reason
        // `read_column_as_text` is: only the footers know the type each file wrote.
        let drift =
            crate::schema_union::ScanDrift::new(&self.drift_files, &dataset, &self.file_rows())
                .map(Arc::new);
        let partition_columns = self.partition_columns.clone();
        Some(crate::data_quality::QualityConflictScan(Arc::new(
            move |files: &[String], as_text: &[PlSmallStr]| {
                let drifts = drift.is_some();
                let lf = crate::schema_union::lenient_scan(
                    files,
                    dataset.schema.clone(),
                    None,
                    drift.as_deref(),
                    as_text,
                )?;
                Ok(crate::hoist_partition_columns(
                    lf,
                    &dataset.schema,
                    partition_columns.as_deref().unwrap_or(&[]),
                    drifts,
                ))
            },
        )))
    }

    /// Frame and optional row-to-file map used by the data-quality worker. The hidden
    /// scan index is projected only while it still identifies source files; the worker
    /// replaces it with file names before profiling and never exposes it as user data.
    /// Unsorted unless `ordered`, as every tool's sample reads it: a sample drawn by
    /// position is then the same rows whichever tool drew it. A row range is the one
    /// scope whose meaning is the order on screen.
    pub(crate) fn data_quality_scan(
        &self,
        ordered: bool,
    ) -> (LazyFrame, Option<crate::data_quality::QualitySourceContext>) {
        let known_files =
            !self.drift_files.is_empty() && self.drift_files.len() == self.drift_file_starts.len();
        let source = if self.can_name_source_files() {
            Some(crate::data_quality::QualitySourceContext {
                row_index_column: crate::schema_union::DRIFT_COLUMN.to_string(),
                ..self.quality_source_drift()
            })
        } else if self.is_pristine() && known_files {
            Some(crate::data_quality::QualitySourceContext {
                row_index_column: "__datui_quality_row".to_string(),
                ..self.quality_source_drift()
            })
        } else {
            None
        };
        let mut expressions = self.binary_stub_exprs();
        if self.can_name_source_files() {
            expressions.push(col(crate::schema_union::DRIFT_COLUMN));
        }
        let lf = if ordered {
            self.lf.clone()
        } else {
            self.analysis_lf()
        }
        .select(expressions);
        let lf = if source
            .as_ref()
            .is_some_and(|mapping| mapping.row_index_column == "__datui_quality_row")
        {
            lf.with_row_index("__datui_quality_row", None)
        } else {
            lf
        };
        (lf, source)
    }

    /// Source-level profiling starts from the loaded scan, independent of the
    /// current query, filters, sort, and column projection. Schema projection is
    /// deferred to the background worker so opening the plan performs no I/O.
    pub(crate) fn data_quality_source_scan(
        &self,
    ) -> (LazyFrame, Option<crate::data_quality::QualitySourceContext>) {
        let known_files =
            !self.drift_files.is_empty() && self.drift_files.len() == self.drift_file_starts.len();
        let source = if known_files {
            Some(crate::data_quality::QualitySourceContext {
                row_index_column: if self.drift_at_open {
                    crate::schema_union::DRIFT_COLUMN.to_string()
                } else {
                    "__datui_quality_row".to_string()
                },
                ..self.quality_source_drift()
            })
        } else {
            None
        };
        (self.original_lf.clone(), source)
    }

    pub(crate) fn quality_source_file_count(&self) -> usize {
        if self.drift_files.len() == self.drift_file_starts.len() {
            self.drift_files.len()
        } else {
            0
        }
    }

    pub(crate) fn quality_source_file_names(&self) -> &[String] {
        if self.quality_source_file_count() > 0 {
            &self.drift_files
        } else {
            &[]
        }
    }

    /// The columns a Data Quality scope reads: the loaded source's for a source
    /// scope, the view's otherwise.
    pub(crate) fn quality_schema(&self, scope: &crate::data_quality::QualityScope) -> &Schema {
        if scope.uses_source() {
            &self.original_schema
        } else {
            &self.schema
        }
    }

    pub(crate) fn quality_temporal_columns(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> Vec<String> {
        self.quality_schema(scope)
            .iter()
            .filter(|(name, dtype)| {
                name.as_str() != crate::schema_union::DRIFT_COLUMN && dtype.is_temporal()
            })
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// The scope's text columns: the ones Data Quality Setup can read as time
    /// through a format.
    pub(crate) fn quality_text_columns(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> Vec<String> {
        self.quality_schema(scope)
            .iter()
            .filter(|(name, dtype)| {
                name.as_str() != crate::schema_union::DRIFT_COLUMN
                    && matches!(dtype, DataType::String | DataType::Categorical(..))
            })
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// Build a temporary filtered table without changing the current pipeline. The
    /// caller keeps this state to restore its query, filters, sort, and buffer.
    /// A table of rows already read: an analysis's sample, shown in the table viewer
    /// with everything it offers (sort, filter, query, copy, export). The rows are in
    /// memory, so nothing here reads the source again.
    pub(crate) fn sample_view(&self, df: DataFrame) -> Result<Self> {
        let options = crate::OpenOptions {
            pages_lookahead: Some(self.pages_lookahead),
            pages_lookback: Some(self.pages_lookback),
            max_buffered_rows: Some(self.max_buffered_rows),
            max_buffered_mb: Some(self.max_buffered_mb),
            row_numbers: self.row_numbers,
            row_start_index: self.row_start_index,
            polars_streaming: self.polars_streaming,
            ..crate::OpenOptions::default()
        };
        let schema = df.schema().clone();
        let mut view = Self::from_schema_and_lazyframe(schema, df.lazy(), &options, None)?;
        view.visible_rows = self.visible_rows;
        Ok(view)
    }

    pub(crate) fn quality_evidence_view(
        &self,
        scope: &crate::data_quality::QualityScope,
        predicate: Expr,
    ) -> Result<Self> {
        let options = crate::OpenOptions {
            pages_lookahead: Some(self.pages_lookahead),
            pages_lookback: Some(self.pages_lookback),
            max_buffered_rows: Some(self.max_buffered_rows),
            max_buffered_mb: Some(self.max_buffered_mb),
            row_numbers: self.row_numbers,
            row_start_index: self.row_start_index,
            polars_streaming: self.polars_streaming,
            ..crate::OpenOptions::default()
        };
        let (lf, schema) = self.quality_scope_frame(scope)?;
        let mut view = Self::from_schema_and_lazyframe(
            schema,
            lf.filter(predicate),
            &options,
            self.partition_columns.clone(),
        )?;
        if !scope.uses_source() {
            view.column_order = self.column_order.clone();
            view.locked_columns_count = self.locked_columns_count;
        }
        view.visible_rows = self.visible_rows;
        view.remote_source = self.remote_source;
        // It scans the same file, and a view captured from it must be refused too.
        view.decompress_temp_file = self.decompress_temp_file.clone();
        view.download = self.download.clone();
        view.converted = self.converted.clone();
        Ok(view)
    }

    /// The rows of a Data Quality scope as a lazy frame, and their schema: the
    /// source's for a source scope, the view's otherwise. Nothing is read here.
    pub(crate) fn quality_scope_frame(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> Result<(LazyFrame, Arc<Schema>)> {
        Ok(if scope.uses_source() {
            let mut lf = self.query_source();
            let source = if matches!(scope, crate::data_quality::QualityScope::SourceFiles(_)) {
                lf = lf.with_row_index("__datui_quality_row", None);
                Some(crate::data_quality::QualitySourceContext {
                    row_index_column: "__datui_quality_row".to_string(),
                    ..self.quality_source_drift()
                })
            } else {
                None
            };
            let lf = crate::data_quality::apply_quality_scope(lf, scope, source.as_ref())?;
            let lf = if source.is_some() {
                lf.drop(by_name(["__datui_quality_row"], false, false))
            } else {
                lf
            };
            (lf, self.original_schema.clone())
        } else {
            (
                crate::data_quality::apply_quality_scope(self.visible_lf(), scope, None)?,
                self.schema.clone(),
            )
        })
    }

    pub fn prepare_async_collect(
        &mut self,
        num_rows_override: Option<usize>,
    ) -> Option<CollectRequest> {
        if self.visible_rows > 0 {
            self.proximity_threshold = self.proximity();
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

        // When the count isn't known yet, `num_rows` is provisional (the planned end of
        // this buffer). `apply_async_collect` keeps `num_rows_valid` false so the
        // background `len()` corrects it, unless the short read reveals the true end.
        let num_rows = if count_known {
            self.num_rows
        } else {
            new_buffer_end
        };
        Some(CollectRequest {
            lf,
            polars_streaming: self.polars_streaming,
            buffer_start: new_buffer_start,
            buffer_end: new_buffer_end,
            num_rows,
            count_known,
            plan: self.fill_plan(new_buffer_start, new_buffer_end, num_rows, count_known),
        })
    }

    /// How a fill of `[buffer_start, buffer_end)` is to be made the buffer, from what
    /// is held and shown now. See [`FillPlan`].
    fn fill_plan(
        &self,
        buffer_start: usize,
        buffer_end: usize,
        num_rows: usize,
        count_known: bool,
    ) -> FillPlan {
        let held = self
            .abuts_buffer(buffer_start, buffer_end.saturating_sub(buffer_start))
            .then(|| self.buffered_df.clone())
            .flatten()
            .map(|df| (df, self.buffered_start_row));
        FillPlan {
            buffer_start,
            buffer_end,
            num_rows,
            count_known,
            indexing: self.indexing().is_some(),
            held,
            view_start: self.start_row,
            view_len: self.visible_rows,
            max_rows: self.max_buffered_rows,
            max_mb: self.max_buffered_mb,
        }
    }

    /// Apply the result of a background buffer load. The worker has already stitched
    /// and cut it ([`FillPlan::fit`]): installing it copies nothing.
    pub fn apply_async_collect(&mut self, result: CollectResult) {
        let CollectResult {
            df,
            start,
            returned: returned_rows,
            bytes_per_row,
            buffer_start,
            buffer_end,
            num_rows,
            count_known,
            indexing,
        } = result;
        let requested_rows = buffer_end.saturating_sub(buffer_start);

        if count_known {
            self.num_rows = num_rows;
            self.num_rows_valid = true;
        } else if returned_rows < requested_rows
            && (buffer_start == 0 || returned_rows > 0)
            // Lines still being indexed end where the indexing has got to, not the file.
            && !indexing
            && self.indexing().is_none()
        {
            // Short read: the slice ran off the end, so we now know the exact total
            // without waiting for the background len() count. A slice deep in the
            // frame that found nothing may lie past the data entirely; only the count
            // can say where it ends.
            self.num_rows = buffer_start + returned_rows;
            self.num_rows_valid = true;
        } else if !self.num_rows_valid {
            // Full buffer with the count still unresolved: render with a provisional
            // total (at least this buffer's end) and leave num_rows_valid false so the
            // in-flight background len() corrects it via count_landed().
            self.num_rows = self.num_rows.max(buffer_end);
        }
        // else: the background len() already resolved the exact count between this
        // buffer being requested and applied — keep it; don't downgrade to provisional.
        self.error = None;
        self.remember_pristine_count();

        if bytes_per_row.is_some() {
            self.observed_bytes_per_row = bytes_per_row;
        }
        // A fill that does not hold the view's first row was planned for rows since
        // replaced (a synchronous collect re-planned while it was out, or the view
        // jumped past what the cut kept): installing it would draw rows under the wrong
        // numbers. Keep what is held and plan again. A fill that holds the first row
        // but not the whole view (the terminal grew while it was out) is kept, and the
        // rest fetched; a downloaded row group is too costly to throw away for a resize.
        // A read that came back short ends the data, so a view past it is shown by the
        // rows kept up to that end, and only by them: a cut may have dropped the end.
        let end = start + df.height();
        let view_end = self.start_row + self.visible_rows.max(1);
        let reaches_end = end >= buffer_start + returned_rows;
        let shows_view = start <= self.start_row
            && (self.start_row < end || (returned_rows < requested_rows && reaches_end));
        if !shows_view {
            self.needs_recollect = true;
            return;
        }
        self.release_display_buffer();
        self.buffered_start_row = start;
        self.buffered_end_row = end;
        self.buffered_df = Some(df);
        // Slice the buffered DataFrame into display DataFrames (locked + scroll columns).
        self.slice_buffer_into_display();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
        if view_end > end && end < self.num_rows {
            self.needs_recollect = true;
        }
    }

    /// True when `rows` rows fetched from `start` run on from the rows on hand or up to
    /// them, so a fill of them is planned to be stitched on (see [`FillPlan`]).
    fn abuts_buffer(&self, start: usize, rows: usize) -> bool {
        self.stitches_buffer()
            && (start == self.buffered_end_row || start + rows == self.buffered_start_row)
    }

    /// Invalidate num_rows cache when lf is mutated. Takes a fresh `len_generation` so any
    /// in-flight background count for the previous `lf` is recognized as stale. Also drops
    /// the cheap Parquet-footer count source: once `lf` carries a filter/query/group, the
    /// row count no longer equals the sum of file footers.
    ///
    /// A view of `sample`, drawn from `source` into `rows`, whose rows have the
    /// columns of `schema`. It starts empty and takes rows with
    /// [`Self::sample_grew`]. `through` when the sample was drawn from the view's
    /// query or filters, rather than the source under them.
    pub(crate) fn sampled_from(
        source: DataTableState,
        sample: crate::sampling::Sample,
        schema: &Schema,
        rows: Arc<crate::table_sample::SampleRows>,
        through: bool,
    ) -> Result<Self> {
        let mut view = source.sample_view(DataFrame::empty_with_schema(schema))?;
        let frame = scanned_frame(&view.original_lf)
            .ok_or_else(|| color_eyre::eyre::eyre!("a sample's frame has no rows to scan"))?;
        view.sampled = Some(Box::new(Sampled {
            source: Box::new(source),
            sample,
            rows,
            frame,
            through,
            drawn: None,
        }));
        Ok(view)
    }

    /// The view's sample, while it has one.
    pub fn sampled(&self) -> Option<&Sampled> {
        self.sampled.as_deref()
    }

    /// The view the sample was drawn from, or this one when it has none: where a new
    /// sample is drawn from.
    pub fn unsampled(&self) -> &DataTableState {
        self.sampled
            .as_ref()
            .map_or(self, |sampled| sampled.source.as_ref())
    }

    /// The view the sample was drawn from, putting the sample down; `self` when it
    /// has none.
    pub(crate) fn into_unsampled(mut self) -> DataTableState {
        match self.sampled.take() {
            Some(sampled) => *sampled.source,
            None => self,
        }
    }

    /// Take the chunks the draw kept since the last call: every frame reads them, so
    /// the query, filters and sort run over them too. Returns whether there were any.
    /// The view stays where it is, and the rows on hand stand while nothing reorders
    /// them, since the new rows come after them.
    pub(crate) fn sample_grew(&mut self) -> bool {
        let Some(sampled) = self.sampled.as_ref() else {
            return false;
        };
        let chunks = sampled.rows.take_new();
        if chunks.is_empty() {
            return false;
        }
        let mut frame = (*sampled.frame).clone();
        for chunk in &chunks {
            if frame.vstack_mut(chunk).is_err() {
                return false;
            }
        }
        self.rebind_sample(Arc::new(frame), false);
        true
    }

    /// The draw ended, having read what `drawn` says: the rows go into the order the
    /// source holds them, once.
    pub(crate) fn sample_drawn(&mut self, drawn: crate::table_sample::Drawn) {
        let Some(sampled) = self.sampled.as_mut() else {
            return;
        };
        // Taken so the chunks are not stacked a second time later.
        let _ = sampled.rows.take_new();
        let ordered = sampled.rows.in_source_order().ok().flatten();
        sampled.drawn = Some(drawn);
        if let Some(frame) = ordered {
            self.rebind_sample(Arc::new(frame), true);
        }
    }

    /// Every frame scans `frame` in place of the sample's last one. `reordered` when
    /// the rows already shown changed places.
    fn rebind_sample(&mut self, frame: Arc<DataFrame>, reordered: bool) {
        let Some(old) = self.sampled.as_ref().map(|sampled| sampled.frame.clone()) else {
            return;
        };
        let rows_stand = !reordered
            && self.sort_columns.is_empty()
            && self.sort_ascending
            && self.scan_is_the_root();
        let rows = frame.height();
        self.each_frame(|lf| crate::table_sample::rebind(&mut lf.logical_plan, &old, &frame));
        if let Some(sampled) = self.sampled.as_mut() {
            sampled.frame = frame;
        }
        self.invalidate_num_rows();
        if self.is_pristine() {
            self.set_num_rows(rows);
        } else if self.scan_is_the_root() {
            self.pristine_rows = Some(rows);
        }
        if !rows_stand {
            self.drop_buffer();
        }
        self.needs_recollect = true;
    }

    /// Draws from the shared counter rather than incrementing, so a mutation here can
    /// never land on the value a later dataset is about to be seeded with.
    pub(crate) fn invalidate_num_rows(&mut self) {
        self.num_rows_valid = false;
        self.len_generation = next_len_generation();
    }

    /// True while `lf` is the data as loaded: no sidebar filter or sort, no query in
    /// any bar, no pivot or melt, no drill-down. Derived rather than kept, so clearing
    /// the filters or un-sorting makes the frame pristine again by itself.
    /// Whether the table shows other rows than its source holds: a filter, a query, a
    /// reshape or a drill. A sort alone reorders the same rows.
    pub(crate) fn changes_rows(&self) -> bool {
        !self.filters.is_empty()
            || !self.active_query.is_empty()
            || !self.active_sql_query.is_empty()
            || !self.active_fuzzy_query.is_empty()
            || self.reshaped_lf.is_some()
            || self.grouped.is_some()
            || self.drilled_down_group_index.is_some()
    }

    /// Whether the view may still take its rows straight from the scan: nothing
    /// that picks rows (a filter, a search, a reshape, a group, a drill). A query
    /// may only choose columns, so it may.
    pub(crate) fn may_keep_scan_rows(&self) -> bool {
        self.filters.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.reshaped_lf.is_none()
            && self.grouped.is_none()
            && self.drilled_down_group_index.is_none()
    }

    fn is_pristine(&self) -> bool {
        self.column_changes.is_empty()
            && self.filters.is_empty()
            && self.sort_columns.is_empty()
            && self.sort_ascending
            && self.active_query.is_empty()
            && self.active_sql_query.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.reshaped_lf.is_none()
            && self.grouped.is_none()
            && self.drilled_down_group_index.is_none()
    }

    /// Whether the frame on screen still grows from the dataset's own scan.
    ///
    /// A filter and a sort do: `apply_transformations` rebuilds them over whatever the
    /// root is, so widening the root under them is exactly what should happen. A query,
    /// a SQL statement, a fuzzy search, a pivot, a melt and a drill-down do not — each
    /// makes its own result the root, with its own columns, and replacing the root
    /// underneath one leaves the view naming columns the frame no longer has.
    ///
    /// `grouped` and `drilled_down_group_index` are set together by a drill down and
    /// cleared together by a drill up, so asking both is belt and braces — kept because
    /// what they guard is the frame being rebuilt under a view of one group of it.
    /// The frame an analysis reads: the view as filtered and queried, without its
    /// order. No statistic depends on the order, and a sort is the one step that makes
    /// a sampled read of a huge table read all of it.
    pub fn analysis_lf(&self) -> LazyFrame {
        self.unsorted_lf.clone().unwrap_or_else(|| self.lf.clone())
    }

    /// What the Pivot & Melt builder previews a few rows of: the view as the user
    /// sees its columns, without its order. A sort would make the head of a large
    /// table a read of all of it.
    pub fn preview_lf(&self) -> LazyFrame {
        Self::without_drift(self.analysis_lf())
    }

    /// Whether the view has a sort, which [`Self::preview_lf`] leaves out.
    pub fn is_sorted(&self) -> bool {
        self.unsorted_lf.is_some()
    }

    pub fn scan_is_the_root(&self) -> bool {
        self.active_query.is_empty()
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

    /// What finding and reading this dataset cost.
    pub fn measurements(&self) -> &Arc<crate::measurements::Meter> {
        &self.measurements
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
    fn set_num_rows(&mut self, n: usize) {
        self.num_rows = n;
        self.num_rows_valid = true;
        self.remember_pristine_count();
        // A view past the end of a frame that turned out smaller comes back to it.
        if self.start_row > 0 && self.start_row >= n {
            self.start_row = n.saturating_sub(self.visible_rows);
            self.needs_recollect = true;
        }
    }

    /// Keep the pristine frame's count for the control bar's "417 of 1,000". Only a
    /// count already resolved for the data as loaded — never a reason to run one.
    fn remember_pristine_count(&mut self) {
        if self.num_rows_valid && self.error.is_none() && self.is_pristine() {
            self.pristine_rows = Some(self.num_rows);
        }
    }

    /// The dataset's full row count for the control bar, when the rows on screen are a
    /// subset of it: a sidebar filter, a query in any bar or a drill-down is active and
    /// the count from before it was applied is known. A pivot or melt makes rows that
    /// are not the dataset's, so the comparison would mislead and none is offered.
    /// Cheap by construction: it only reads what a pristine collect already knew.
    pub fn total_rows_when_subset(&self) -> Option<usize> {
        let subsetting = !self.filters.is_empty()
            || !self.active_query.is_empty()
            || !self.active_sql_query.is_empty()
            || !self.active_fuzzy_query.is_empty()
            || self.drilled_down_group_index.is_some();
        if subsetting && self.reshaped_lf.is_none() {
            self.pristine_rows
        } else {
            None
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
    /// stitched on to them rather than replace them. See [`FillPlan`].
    pub(crate) fn stitches_buffer(&self) -> bool {
        self.remote_window() && self.buffer_on_hand()
    }

    /// The rows on hand and the view row the first of them is, when every row of
    /// the buffered range is: what a find lights up as it is typed, without a read.
    pub(crate) fn rows_on_hand(&self) -> Option<(&DataFrame, usize)> {
        self.buffered_df
            .as_ref()
            .filter(|_| self.buffer_on_hand())
            .map(|df| (df, self.buffered_start_row))
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
            let offset = start - self.buffered_start_row;
            // Trimmed so the rows let go are freed rather than kept behind a slice; the
            // display frames alias the old buffer and go with it.
            self.locked_df = None;
            self.df = None;
            self.buffered_df = self
                .buffered_df
                .take()
                .map(|b| trim_rows(b, offset, end - start, None));
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

    /// True for a scan of an object store in place.
    ///
    /// Polars fetches a Parquet row group whole for any slice that touches it and keeps
    /// nothing between collects, so the small, proximity-driven refills that suit a
    /// local file each download the same row group again: paging through one row group
    /// cost a fetch of it every few pages. A remote buffer is planned as a single window
    /// of `max_buffered_rows` around the view instead. Scrolling inside it costs
    /// nothing; leaving it, or a jump, costs one fetch.
    pub fn is_remote_source(&self) -> bool {
        self.remote_source
    }

    /// The schema of the data as loaded, before any query or reshape: what a view's
    /// settings run on, and so what its schema rule records and matches.
    pub fn source_schema(&self) -> &Arc<Schema> {
        &self.original_schema
    }

    /// Record the row groups of a remote Parquet object, `rows` in each, so a buffer
    /// fill is planned as whole groups (see `align_to_row_groups`). Also the row count.
    fn record_row_groups(&mut self, rows: &[usize]) {
        let mut offsets = Vec::with_capacity(rows.len() + 1);
        offsets.push(0);
        for n in rows {
            offsets.push(offsets.last().unwrap_or(&0) + n);
        }
        self.set_num_rows(*offsets.last().unwrap_or(&0));
        self.row_group_offsets = Some(offsets);
    }

    /// Whether a Data Quality run over `scope` reads every row and every byte-bearing
    /// column of the source: the case where a copy of the whole objects costs no more
    /// than one of its passes. A filter or a hidden column may let a pass read less
    /// than the objects, and a binary column is never read at all.
    pub(crate) fn quality_reads_whole_source(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> bool {
        use crate::data_quality::QualityScope;
        let columns = || {
            self.original_schema
                .iter()
                .filter(|(name, _)| name.as_str() != crate::schema_union::DRIFT_COLUMN)
        };
        if columns().any(|(_, dtype)| matches!(dtype, DataType::Binary)) {
            return false;
        }
        match scope {
            QualityScope::WholeSource => true,
            QualityScope::CurrentView => {
                let shown = self
                    .column_order
                    .iter()
                    .map(String::as_str)
                    .collect::<HashSet<_>>();
                !self.changes_rows() && columns().all(|(name, _)| shown.contains(name.as_str()))
            }
            _ => false,
        }
    }

    /// Each remote object the dataset reads, in scan order: every file of a remote
    /// dataset, or the one object. `None` in place of one the open did not size.
    pub(crate) fn each_remote_object(
        &self,
    ) -> Option<Box<dyn Iterator<Item = Option<&RemoteObject>> + '_>> {
        let objects = self.remote_objects.as_ref()?;
        Some(match &self.remote_files {
            Some(remote) => Box::new(remote.urls.iter().map(|url| objects.get(url))),
            None => Box::new(objects.values().map(Some)),
        })
    }

    /// The remote objects this dataset reads. `None` when any is unknown.
    pub(crate) fn remote_objects(&self) -> Option<Vec<RemoteObject>> {
        let objects = self
            .each_remote_object()?
            .map(|object| object.cloned())
            .collect::<Option<Vec<_>>>()?;
        (!objects.is_empty()).then_some(objects)
    }

    /// The bytes and count of [`Self::remote_objects`], without copying them out:
    /// Setup asks on every frame.
    pub(crate) fn remote_objects_size(&self) -> Option<(u64, usize)> {
        let (bytes, count) = self
            .each_remote_object()?
            .try_fold((0u64, 0usize), |(bytes, count), object| {
                object.map(|object| (bytes + object.size, count + 1))
            })?;
        (count > 0).then_some((bytes, count))
    }

    /// Record what the footers said about the dataset's columns. See `DatasetSchema`.
    /// `file_rows` is each file's row count, in scan order, and empty when they are not
    /// all known — the same condition under which the scan numbers its rows.
    fn record_dataset_schema(
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
        self.drift_dataset_rows = row;
        self.drift_at_open = self.drift_column_present;
        self.groups_at_open = self.drift_groups.clone();
        self.notes = Self::notes_datui_can_act_on(&schema, self.drift_column_present);
        self.notes_at_open = self.notes.clone();
        self.notes_seen = false;
        self.read_as_text = Vec::new();
        self.dataset_at_open = Some(schema.clone());
        self.dataset_schema = Some(schema);
    }

    /// The pass that is still reading this dataset's footers, if one is.
    pub fn footers_pending(&self) -> Option<FootersJoin> {
        self.footers_pending.clone()
    }

    /// Whether *this frame's* row count is already on its way.
    ///
    /// The frame matters: what the pass is bringing is the dataset's count, which is
    /// not the count of a query's result. Asking this about the wrong frame is how a
    /// count nobody else was going to take gets declined.
    ///
    /// A staged open is still reading every footer, and those footers hold the count.
    /// Asking for it separately would read all of them a second time, so the dataset
    /// says it will have one shortly and the caller does not start a count of its own.
    pub fn counts_itself_later(&self) -> bool {
        // Lines still being indexed: any frame's count is of the lines so far, and the
        // indexing is bringing the rest.
        if self.indexing().is_some() && !self.num_rows_valid {
            return true;
        }
        // Only while it does not have one, and only while the frame is the scan. What
        // the pass is bringing is the *dataset's* count; a query's result has a count
        // of its own that nobody else is going to take. Declining it there means the
        // row count spins for as long as the query is open and `End` says it is
        // counting while nothing is — and it costs nothing to take, because a frame
        // that is not the scan does not read footers for it either.
        self.footers_pending.is_some() && !self.num_rows_valid && self.is_pristine()
    }

    /// The lines being indexed behind the first rows, if they still are.
    pub fn indexing(&self) -> Option<&Arc<crate::lines::Lines>> {
        // Asked of the lines, so a dataset set aside while they finished (the quality
        // evidence view) does not wait for them for good.
        self.indexing.as_ref().filter(|lines| lines.indexing())
    }

    /// The lines this dataset opened from in part, until it has been told they are all
    /// in, though their indexing is paused: what an indexing thread works on.
    pub fn lines_to_index(&self) -> Option<&Arc<crate::lines::Lines>> {
        self.indexing.as_ref()
    }

    /// The dataset's row count from a sample of its footers, while the frame is the
    /// dataset as loaded and its count is not known. `pass` is the estimate of the
    /// footer pass still reading, which the dataset has not been given yet.
    pub fn row_estimate(
        &self,
        pass: Option<crate::schema_union::RowEstimate>,
    ) -> Option<crate::schema_union::RowEstimate> {
        if self.num_rows_valid || !self.is_pristine() {
            return None;
        }
        self.row_estimate
            .or_else(|| pass.filter(|_| self.footers_pending.is_some()))
    }

    /// Where each of the dataset's files starts in the view, with the total last: while
    /// the view keeps the dataset's rows and every file's rows are known.
    pub fn file_row_starts(&self) -> Option<Vec<usize>> {
        if self.changes_rows() {
            return None;
        }
        self.remote_files.as_ref()?.offsets.clone()
    }

    /// How many files a count of the dataset reads footers of, when it reads them.
    pub fn files_to_count(&self) -> Option<usize> {
        self.remote_files
            .as_ref()
            .filter(|f| f.offsets.is_none())
            .map(|f| f.urls.len())
    }

    /// Whether `#` is on for this dataset when the config leaves it to the format:
    /// text and logs, whose rows carry their place in the file.
    pub fn numbered_by_default(&self) -> bool {
        matches!(
            self.read_as,
            Some(crate::FileFormat::Text | crate::FileFormat::Journal)
        )
    }

    /// Whether `#` is on and numbers the rows by their place in the view, because
    /// the view's rows do not carry their place in the source: a sorted or filtered
    /// view of data in a store, of many files, or too large to number.
    pub fn row_numbers_count_the_view(&self) -> bool {
        self.row_numbers
            && !self.carries_source_rows()
            && self.scan_is_the_root()
            && (!self.filters.is_empty() || !self.sort_columns.is_empty() || !self.sort_ascending)
    }

    /// Every line is indexed, `rows` of them: the count of the lines in order, and the
    /// notes that say what the whole file holds. The frames already read every line
    /// (their height waits for the indexing), so nothing read through them is stale.
    /// Returns whether the dataset was waiting for them.
    pub(crate) fn lines_indexed(&mut self, rows: usize) -> bool {
        let Some(lines) = self.indexing.take() else {
            return false;
        };
        let notes = crate::lines::notes(&lines, self.indexing_guessed);
        let opened = std::mem::take(&mut self.indexing_notes);
        self.open_notes.retain(|n| !opened.contains(n));
        self.open_notes.extend(notes);
        // A file that shrank has no count to give: the lines so far are not all of it.
        if lines.shrank() {
            self.open_notes.push(crate::text_formats::note(
                crate::lines::SHRANK.to_string(),
                "the file".to_string(),
            ));
            return true;
        }
        // The "of" in `417 of 1,000` under a filter.
        self.pristine_rows = Some(rows);
        if self.is_pristine() {
            self.set_num_rows(rows);
        }
        true
    }

    /// Give up on the rest of the footers: the pass could not read them.
    ///
    /// The dataset stays as it opened — a working view of it, built from two footers —
    /// and stops waiting. That matters beyond the columns: while a pass is pending the
    /// dataset declines to count itself, because the pass was going to bring the count
    /// with it. One failed pass would otherwise cost it an exact row count, and its
    /// windowed reads, for the rest of the session.
    pub fn give_up_on_pending_footers(&mut self) {
        self.footers_pending = None;
    }

    /// Every footer's answer, joined to the dataset already on screen.
    ///
    /// The open painted from the first file and the newest; this is what the rest of
    /// them say. Columns only ever join: a name the opening schema did not have goes on
    /// the end, and every name already there keeps its place — including the places a
    /// user has since moved them to — so nothing moves under the cursor except to make
    /// room for what arrived.
    ///
    /// The frame is rebuilt rather than widened in place, because the scan itself
    /// differs: it now knows which files hold a column in a type the dataset cannot
    /// keep, and with every file's row count it can number the rows, which is what
    /// tells an absent cell from a null.
    ///
    /// Gives them back as `Err` rather than taking them, while the user is looking at
    /// something built on top of the scan instead of the scan itself — see
    /// [`Self::scan_is_the_root`]. Rebuilding the root under a query takes away the
    /// columns the query named; handing them back lets the caller keep them and offer
    /// them again when the view comes back to the data.
    pub fn join_dataset_schema(
        &mut self,
        mut found: FootersFound,
    ) -> std::result::Result<(), Box<FootersFound>> {
        if !self.scan_is_the_root() {
            // The columns must wait; what the footers said about the files need not.
            // `record_file_row_groups` keeps the offsets without touching the count of a
            // frame that is a query's result rather than the dataset — so letting the
            // query go gets the total back without going and fetching it.
            // Taken, not borrowed: this runs on every event for as long as the view
            // stays off the scan, and applying the same row groups on each keystroke is
            // a walk of every file in the dataset for nothing.
            let row_groups = std::mem::take(&mut found.row_groups);
            if !row_groups.is_empty() {
                self.record_file_row_groups(&row_groups);
            }
            // Boxed because what comes back is most of a dataset's worth of schema, and
            // an `Err` that size would be carried by every call that succeeds too.
            return Err(Box::new(found));
        }
        let FootersFound {
            dataset,
            lf,
            file_rows,
            files,
            row_groups,
            remote,
            estimate,
        } = found;
        self.row_estimate = if row_groups.is_empty() {
            estimate
        } else {
            None
        };
        let (file_rows, files) = (file_rows.as_slice(), files.as_slice());
        let known: std::collections::HashSet<&str> =
            self.column_order.iter().map(String::as_str).collect();
        let joining: Vec<String> = dataset
            .schema
            .iter_names()
            .map(|name| name.to_string())
            .filter(|name| {
                name != crate::schema_union::DRIFT_COLUMN && !known.contains(name.as_str())
            })
            .collect();
        drop(known);
        self.column_order.extend(joining);
        // Columns only join — but a name can still go, if the footer that was the only
        // evidence for it would not parse this time round. Every read projects
        // `column_order`, so a name the new schema does not have is not a missing
        // column on screen, it is a scan that cannot run at all.
        self.column_order
            .retain(|name| dataset.schema.contains(name.as_str()));
        let schema = dataset.schema.clone();
        // The scan is built at a schema, and the one this dataset opened with has never
        // heard of the columns that just arrived. Left in place, the first windowed
        // page read asks it for a column it does not have and the table stops showing
        // rows at the moment it was supposed to show more of them.
        match (remote, self.remote_files.as_mut()) {
            (Some(found), Some(remote)) => {
                remote.urls = Arc::new(found.urls);
                remote.scan = found.scan;
                // The counter too, and for the same reason the scan is replaced: it
                // answers one entry per file it was given, and the one the dataset
                // opened with was given every file listed. Left beside a shorter `urls`
                // its answer is dropped on a length check without a word, and the
                // dataset spends the rest of the session re-counting itself and never
                // reaching an end to jump to.
                remote.count = found.count;
            }
            // A local directory reads by file only once every footer is known.
            (Some(found), None) => self.remote_files = Some(found.into()),
            (None, _) => {}
        }
        // Takes the notes, the drift groups and the row starts with it, and clears
        // `read_as_text` — sound only because the offer to read a column as text is
        // not made until the footers are all in, so there is nothing to clear.
        self.record_dataset_schema(dataset, file_rows, files);
        self.footers_pending = None;
        // The rows on screen were read through the old frame. Dropping the buffer has
        // the next collect read them through the new one, at the row the user is still
        // sitting on — `start_row` and the column scroll are left exactly as they are.
        self.replace_root(lf, schema);
        // The joined scan may hold rows the two-footer open never saw, so the count
        // remembered for the narrow root no longer describes the dataset.
        self.pristine_rows = None;
        // Measured on the frame that just went. A dataset that opened two columns wide
        // and gained thirty would plan its first page after the join from the two-column
        // width, which against a bucket is a read many times the budget the user set.
        self.observed_bytes_per_row = None;
        // Every file's row groups are known now, so this is the dataset's count. Set
        // before the rebuild so the count is in place the moment the frame is, rather
        // than for any ordering the lines below depend on.
        if !row_groups.is_empty() {
            self.record_file_row_groups(&row_groups);
        }
        // Rebuilt but not read. This runs on the thread drawing the screen, and
        // `apply_transformations` ends in a `collect` — against a dataset in a bucket
        // that is a page fetched, and with no count yet it is a `len()` over every file
        // in the dataset, which is the whole cost this staging exists to avoid. The
        // buffer is gone and the caller reads it back off the event loop. This line is
        // what keeps the collect off this thread; do not take it away.
        self.deferred(Self::apply_transformations);
        Ok(())
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

    /// Whether the frame scans a temporary file this state holds (a decompressed
    /// archive). A view captured at exit must not reference it: the file is removed
    /// when the last state holding it drops, and the plan would scan a path that no
    /// longer exists.
    pub fn scans_a_temp_file(&self) -> bool {
        self.decompress_temp_file.is_some() || !self.converted.is_empty()
    }

    /// Whether the frame scans a downloaded remote file, removed when datui lets go of
    /// it; see [`Self::scans_a_temp_file`].
    pub fn scans_a_download(&self) -> bool {
        self.download.is_some()
    }

    /// How the open reads the data, when an open found it; `None` for a frame handed
    /// in whole, such as one from Python.
    pub fn read_mode(&self) -> Option<crate::ReadMode> {
        self.read_mode
    }

    /// The format the open read the data as. See [`OpenFacts::read_as`].
    pub fn read_as(&self) -> Option<crate::FileFormat> {
        self.read_as
    }

    /// Whether the data was downloaded from a remote source before it was read.
    pub fn fetched(&self) -> bool {
        self.fetched
    }

    /// The temporary files this state holds, which an error from reading it may name:
    /// a decompressed copy, a download.
    pub(crate) fn temp_files(&self) -> Vec<&Path> {
        let files = self.decompress_temp_file.iter().map(|file| file.path());
        let files = files.chain(self.download.iter().map(|download| download.path()));
        let files = files.chain(self.converted.iter().map(|file| file.path()));
        files.collect()
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

    /// Whether an export can name each row's file: the frame has to still carry the
    /// scan's row index, and the dataset has to have files to name.
    pub fn can_name_source_files(&self) -> bool {
        self.drift_column_present
            && !self.drift_files.is_empty()
            && self.drift_files.len() == self.drift_file_starts.len()
    }

    /// The rows an export writes, planned and not run: the view, and when
    /// `name_files` asks and the dataset can, a column naming each row's file in
    /// place of the scan's hidden row index. Otherwise the index is dropped, so
    /// datui's own bookkeeping never lands in the user's file.
    pub fn export_frame(&self, name_files: bool) -> ExportFrame {
        if name_files && self.can_name_source_files() {
            ExportFrame {
                lf: self.lf.clone(),
                files: Some(SourceFiles {
                    names: Arc::new(self.drift_files.clone()),
                    starts: Arc::new(self.drift_file_starts.clone()),
                }),
            }
        } else {
            ExportFrame {
                lf: self.visible_lf(),
                files: None,
            }
        }
    }

    /// What datui noticed about the dataset itself, as its footers were read.
    ///
    /// Separate from [`Self::notes`] because this is the half that belongs to the
    /// data: a snapshot taken to roll a view back has to put back these and not
    /// the view's, which describe a filter and sort that the rollback is undoing.
    pub fn dataset_notes(&self) -> &[crate::notes::Note] {
        &self.notes
    }

    /// What datui noticed: about the dataset when it opened, then about the view the
    /// filter and sort have made of it. Empty when there is nothing to say.
    pub fn notes(&self) -> Vec<crate::notes::Note> {
        // What the read did first, because it is the frame everything below is about:
        // a directory read as CSV with a JSON file left out, or a lake table read as its
        // plain files, changes what every other note is a note about. `merged` rather
        // than a plain chain, because the open and the footer walk each count the
        // files a mixed directory's read passed over, and this is the one place both
        // tallies are in hand.
        let mut notes = crate::notes::merged(
            &self.open_notes,
            &self.notes,
            &self.view_notes,
            self.dataset_schema.as_ref(),
        );
        // What reading a table in place has found, which may grow after the open.
        if let Some(pushdown) = &self.pushdown {
            notes.extend(pushdown.notes());
        }
        notes.extend(self.unfit_notes.iter().flatten().cloned());
        notes.extend(self.changes_dropped.iter().cloned());
        if let Some((version, unfit)) = &self.changes_unfit
            && *version == self.changes_version
        {
            notes.extend(unfit.iter().cloned());
        }
        notes
    }

    /// The source a full quality run over `scope` reads every row of, for the checks
    /// that read a source whole (an audio file's signal): the records of the data as
    /// loaded, or a view with nothing applied.
    pub(crate) fn window_for_quality(
        &self,
        scope: &crate::data_quality::QualityScope,
    ) -> Option<Arc<dyn crate::pushdown::Windowed>> {
        use crate::data_quality::QualityScope;
        matches!(scope, QualityScope::WholeSource | QualityScope::CurrentView)
            .then(|| {
                self.fixed_window
                    .clone()
                    .filter(|_| self.is_pristine() && self.indexing().is_none())
            })
            .flatten()
    }

    /// The lake format whose plain files this dataset is, if it is one.
    ///
    /// For the chip in the control bar. The note says the same at length; this is what
    /// keeps the row count from reading as the table's.
    pub fn not_the_table(&self) -> Option<&'static str> {
        self.not_the_table
    }

    /// The file's other tables, as `--table` names them; empty for a file of one.
    pub fn other_tables(&self) -> &[String] {
        &self.other_tables
    }

    /// What a read through a format spec found, when the dataset was read through one.
    pub fn format_read(&self) -> Option<&Arc<crate::formats::Read>> {
        self.format_read.as_ref()
    }

    /// The source a window of the view is read straight from, when there is one: the
    /// records or audio frames of the data as loaded, or the view a source runs itself.
    fn window_now(&self) -> Option<Arc<dyn crate::pushdown::Windowed>> {
        if let Some(window) = self.follow_window() {
            return Some(Arc::new(window));
        }
        if let Some(records) = self.fixed_window.as_ref().filter(|_| self.is_pristine()) {
            return Some(records.clone());
        }
        self.pushed_view().map(|view| view.window)
    }

    /// The view as the source runs it, when it runs it: the data as loaded is the root
    /// (no query, reshape or drill), and the source can say the sidebar's filters and
    /// sort. Derived from them each time, so it never disagrees with them.
    pub(crate) fn pushed_view(&self) -> Option<crate::pushdown::PushedView> {
        let pushdown = self.pushdown.as_ref()?;
        if !self.scan_is_the_root() || self.drift_column_present {
            return None;
        }
        let sort: Vec<(String, bool)> = self
            .sort_columns
            .iter()
            .cloned()
            .zip(self.sort_descending.iter().copied())
            .collect();
        pushdown.view(&self.filters, &sort, !self.sort_ascending)
    }

    /// The view's own count, from a source that runs the view, or for a followed file
    /// whose view is known up to a row, that count and the rows after it.
    pub(crate) fn source_counter(&self) -> Option<crate::pushdown::Counter> {
        if let Some(counter) = self.follow_counter() {
            return Some(counter);
        }
        self.pushed_view().map(|view| view.counter)
    }

    /// The points where a followed view's rows are known, when they hold for the view
    /// on screen.
    fn follow_known(&self) -> Option<&[(usize, usize)]> {
        self.follow_known
            .as_ref()
            .filter(|(generation, _)| *generation == self.len_generation)
            .map(|(_, known)| known.as_slice())
    }

    /// A followed file's windows, read from the mark before each: the rows as they are,
    /// or filtered with no sort, read on from where the view's rows are known.
    fn follow_window(&self) -> Option<crate::follow::Window> {
        let follow = self.follow.as_ref()?;
        let known = if self.is_pristine() {
            None
        } else if self.scan_is_the_root() && self.sort_columns.is_empty() && self.sort_ascending {
            Some(self.follow_known()?.to_vec())
        } else {
            return None;
        };
        Some(crate::follow::Window {
            lf: self.lf.clone(),
            path: follow.path().to_path_buf(),
            marks: follow.marks().clone(),
            known,
        })
    }

    /// The count of a followed view known up to a file row: what was known, and the
    /// rows of the view among those after it, read from the mark before them.
    fn follow_counter(&self) -> Option<crate::pushdown::Counter> {
        let follow = self.follow.as_ref()?;
        let &(before, row) = self.follow_known()?.last()?;
        let rest = crate::follow::from_marks(&self.lf, follow.path(), follow.marks(), row, None)?;
        let streaming = self.polars_streaming;
        Some(Arc::new(move || {
            let df = crate::statistics::collect_lazy(row_count_lf(&rest), streaming)?;
            let after = match df.get(0).and_then(|row| row.first().cloned()) {
                Some(AnyValue::UInt64(n)) => n as usize,
                _ => 0,
            };
            Ok(before + after)
        }))
    }

    /// What a read through a delimited spec found, when the dataset was read through
    /// one.
    pub fn delimited_read(&self) -> Option<&Arc<crate::delimited_spec::DelimitedRead>> {
        self.delimited.as_ref()
    }

    /// The unit of the column named `column`, from a delimited spec's unit row or the
    /// file itself. A filter, sort, drill or query that keeps the loaded column, renamed
    /// or not, keeps its unit; a column a query computes has none, whatever it is called.
    pub fn unit_of(&self, column: &str) -> Option<&str> {
        if self.delimited.is_none() && self.file_units.is_empty() {
            return None;
        }
        let loaded = match &self.lineage {
            None => column,
            Some(lineage) => lineage
                .iter()
                .find(|(shown, _)| shown == column)
                .map(|(_, loaded)| loaded.as_str())?,
        };
        match &self.delimited {
            Some(read) => read.unit_of(loaded),
            None => self
                .file_units
                .iter()
                .find(|(name, _)| name == loaded)
                .map(|(_, unit)| unit.as_str()),
        }
    }

    /// Each column of the view that has a unit, with it.
    pub fn units(&self) -> Vec<(String, String)> {
        if self.delimited.is_none() && self.file_units.is_empty() {
            return Vec::new();
        }
        self.schema
            .iter_names()
            .filter_map(|name| Some((name.to_string(), self.unit_of(name)?.to_string())))
            .collect()
    }

    /// What the file said besides its rows: its Info panel tab.
    pub fn format_detail(&self) -> Option<&crate::text_formats::Detail> {
        self.detail.as_deref()
    }

    /// The Info panel tab of a followed pipe's journal, read again once it has ended
    /// and its rows are all on hand: the frame that reads every entry. `None` for any
    /// other dataset, or when it has been asked for already.
    pub(crate) fn ended_journal_to_describe(&mut self) -> Option<LazyFrame> {
        let follow = self.follow.as_mut()?;
        if follow.described
            || follow.live()
            || follow.behind()
            || follow.spool().is_none()
            || self.read_as != Some(crate::FileFormat::Journal)
        {
            return None;
        }
        follow.described = true;
        Some(self.original_lf.clone())
    }

    pub(crate) fn set_format_detail(&mut self, detail: crate::text_formats::Detail) {
        self.detail = Some(Arc::new(detail));
    }

    /// Whether datui noticed anything at all. Answers what `notes()` is usually asked
    /// — whether to offer the tab — without building the list to find out.
    pub fn has_notes(&self) -> bool {
        // A view note needs a column the files disagree on, and such a column always
        // draws a note of its own when the dataset opens. So the view half can never
        // be the only half, and the Notes tab does not appear and disappear as the
        // user sorts.
        //
        // The open's own notes count: a directory read as one format with another left
        // out may have nothing else worth saying, and that is exactly the dataset whose
        // reader the user most wants to know about.
        !self.notes.is_empty()
            || !self.open_notes.is_empty()
            || self.unfit_notes.as_ref().is_some_and(|n| !n.is_empty())
            || !self.changes_dropped.is_empty()
            || self
                .changes_unfit
                .as_ref()
                .is_some_and(|(v, n)| *v == self.changes_version && !n.is_empty())
            || self
                .pushdown
                .as_ref()
                .is_some_and(|p| !p.notes().is_empty())
    }

    /// Whether there is something to say that has not been offered yet.
    pub fn notes_unseen(&self) -> bool {
        self.has_notes() && !self.notes_seen
    }

    /// The rows a filter or sort on `column` has to leave out: every row of every file
    /// that holds the column in a type it is not read in.
    fn unread_row_runs(&self, column: &str) -> Vec<(usize, usize)> {
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let conflicts: Vec<bool> = (0..self.drift_file_starts.len())
            .map(|file| {
                dataset
                    .file_group
                    .get(file)
                    .and_then(|group| dataset.groups.get(*group as usize))
                    .is_some_and(|group| group.unread.iter().any(|name| name == column))
            })
            .collect();
        conflicting_row_runs(&self.drift_file_starts, self.drift_dataset_rows, &conflicts)
    }

    /// Columns the filter or sort names that some file holds in another type, in the
    /// dataset's own column order and each named once however many times the view
    /// mentions it.
    fn view_columns_with_conflicts(&self) -> Vec<crate::schema_union::ColumnDrift> {
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let named: HashSet<&str> = self
            .filters
            .iter()
            .map(|filter| filter.column.as_str())
            .chain(self.sort_columns.iter().map(String::as_str))
            .collect();
        dataset
            .columns
            .iter()
            .filter(|column| column.conflicting_files > 0 && named.contains(column.name.as_str()))
            .cloned()
            .collect()
    }

    /// Leave out the rows whose files do not hold a filtered or sorted column in the
    /// type it is read as, and say how many.
    ///
    /// Those rows read as null in that column, and a null is not a value the column
    /// can be compared or ordered by: a filter drops them already, and a sort would
    /// otherwise gather them at one end as though they belonged there. They are left
    /// out of both, and the note says how many so the smaller count is never a
    /// surprise.
    fn view_exclusions(&self) -> Vec<(Vec<(usize, usize)>, crate::notes::Note)> {
        if !self.drift_column_present {
            return Vec::new();
        }
        let Some(dataset) = self.dataset_schema.as_ref() else {
            return Vec::new();
        };
        let mut out = Vec::new();
        for column in self.view_columns_with_conflicts() {
            let runs = self.unread_row_runs(&column.name);
            let rows: usize = runs.iter().map(|(start, end)| end - start).sum();
            if rows == 0 {
                continue;
            }
            let filtered = self
                .filters
                .iter()
                .any(|filter| filter.column.as_str() == column.name.as_str());
            let sorted = self
                .sort_columns
                .iter()
                .any(|sorted| sorted.as_str() == column.name.as_str());
            out.push((
                runs,
                crate::notes::left_out_note(&column, dataset, rows, filtered, sorted),
            ));
        }
        out
    }

    /// The notes for what the filter and sort on screen leave out, for a caller that
    /// is putting a frame back that already leaves those rows out rather than building
    /// one. Derived, never stored across a change of view: a note that outlives the
    /// sort that earned it is the fault this is shaped to avoid.
    fn view_notes_only(&self) -> Vec<crate::notes::Note> {
        self.view_exclusions()
            .into_iter()
            .map(|(_, note)| note)
            .collect()
    }

    fn leave_out_unread_rows(&self, mut lf: LazyFrame) -> (LazyFrame, Vec<crate::notes::Note>) {
        let mut notes = Vec::new();
        for (runs, note) in self.view_exclusions() {
            let keep = runs
                .iter()
                .map(|(start, end)| {
                    col(crate::schema_union::DRIFT_COLUMN)
                        .lt(lit(*start as u32))
                        .or(col(crate::schema_union::DRIFT_COLUMN).gt_eq(lit(*end as u32)))
                })
                .reduce(Expr::and);
            if let Some(keep) = keep {
                lf = lf.filter(keep);
            }
            notes.push(note);
        }
        (lf, notes)
    }

    /// Whether the notes have been offered. Exact, where `!notes_unseen()` would also
    /// be true of a dataset that has nothing to say.
    pub fn notes_seen(&self) -> bool {
        self.notes_seen
    }

    /// The Info panel has been opened; the quiet accent has done its job.
    pub fn mark_notes_seen(&mut self) {
        self.notes_seen = true;
    }

    /// Read `column` as text from every file, so the values a type conflict hid can be
    /// seen.
    ///
    /// Rebuilds the scan rather than re-opening the dataset: everything it needs is
    /// already here. The file list, each file's row count and — since the footers were
    /// read — the type each file holds each conflicting column in are all on hand, so
    /// this costs no directory listing, no footer read and no request. The view goes
    /// with it: a filter and sort in force are re-applied to the new frame.
    ///
    /// Returns whether anything happened. `false` for a column that is not on offer,
    /// which is what the panel only ever asks about, and for one already read this way.
    /// A scan that cannot be built is kept as the error showing, and returned.
    pub fn read_column_as_text(&mut self, column: &str) -> PolarsResult<bool> {
        let name = PlSmallStr::from(column);
        let Some(dataset) = self.dataset_at_open.clone() else {
            return Ok(false);
        };
        if !self.drift_column_present || self.read_as_text.contains(&name) {
            return Ok(false);
        }
        if !dataset
            .columns
            .iter()
            .any(|drift| drift.name == name && drift.can_read_as_text())
        {
            return Ok(false);
        }

        let mut as_text = self.read_as_text.clone();
        as_text.push(name);

        // Built from the dataset as its footers found it, never from the view below.
        // The view has the column as text and nothing conflicting, so it no longer
        // holds the one thing the scan needs: the type each file actually wrote.
        let drift =
            crate::schema_union::ScanDrift::new(&self.drift_files, &dataset, &self.file_rows());
        let scanned = match self.remote_files.as_ref() {
            Some(remote) => (remote.scan)(&remote.urls, &as_text),
            None => crate::schema_union::lenient_scan(
                &self.drift_files,
                dataset.schema.clone(),
                None,
                drift.as_ref(),
                &as_text,
            ),
        };
        let lf = match scanned {
            Ok(lf) => lf,
            Err(e) => {
                // Shown where any failed read is; the view is as it was.
                self.error = Some(e.clone());
                return Err(e);
            }
        };
        // The remote scan hoists inside its own closure, as it does for the frame the
        // dataset opened with; only the local branch has it left to do.
        let lf = if self.remote_files.is_some() {
            lf
        } else {
            crate::hoist_partition_columns(
                lf,
                &dataset.schema,
                self.partition_columns.as_deref().unwrap_or(&[]),
                drift.is_some(),
            )
        };

        let view = dataset.reading_as_text(&as_text);
        self.read_as_text = as_text;
        // `text_schema` keeps the columns in their places, so the order the user
        // arranged still names every one of them and still means what it did.
        let schema = view.schema.clone();
        self.drift_groups = Arc::new(view.groups.clone());
        self.groups_at_open = self.drift_groups.clone();
        self.notes = Self::notes_datui_can_act_on(&view, self.drift_column_present);
        self.notes_at_open = self.notes.clone();
        // One note went and another arrived, and the new one is about how the column
        // now compares — which matters most to a user who has a filter on it.
        self.notes_seen = false;
        self.dataset_schema = Some(view);
        self.replace_root(lf, schema);
        // Re-applies the filter and sort over the new frame, and with them the note
        // about what they leave out — which is one note shorter now.
        self.apply_transformations();
        Ok(true)
    }

    /// The dataset's notes, with the offer to read a column as text left on only where
    /// taking it would work.
    ///
    /// A note is written from the footers' schema, which says whether a column *could*
    /// be shown as text. Whether it can be read that way is a second question: the scan
    /// has to know where each file's rows begin, and it does not for a dataset too large
    /// to read every footer, or one where a footer would not parse. Those are the same
    /// datasets that cannot draw the marks. An offer the panel shows and the action
    /// then declines is worse than no offer, so it is taken off here rather than
    /// refused later.
    fn notes_datui_can_act_on(
        dataset: &crate::schema_union::DatasetSchema,
        counted: bool,
    ) -> Vec<crate::notes::Note> {
        let mut notes = crate::notes::from_dataset(dataset);
        if !counted {
            for note in &mut notes {
                note.read_as_text = None;
            }
        }
        notes
    }

    /// Each file's row count, as the footers gave them. The starts are kept rather than
    /// the counts, so this is their differences with the dataset's total closing the
    /// last one.
    fn file_rows(&self) -> Vec<usize> {
        self.drift_file_starts
            .iter()
            .enumerate()
            .map(|(file, start)| {
                self.drift_file_starts
                    .get(file + 1)
                    .copied()
                    .unwrap_or(self.drift_dataset_rows)
                    .saturating_sub(*start)
            })
            .collect()
    }

    /// The columns being read as text rather than as the type most rows have.
    pub fn read_as_text(&self) -> &[PlSmallStr] {
        &self.read_as_text
    }

    /// What the footers said about the dataset's columns, when it is many files.
    pub fn dataset_schema(&self) -> Option<&crate::schema_union::DatasetSchema> {
        self.dataset_schema.as_ref()
    }

    /// The counter for a remote dataset's files, while its count would be the data's:
    /// the frame is the scan as loaded, and the files have not been counted yet.
    ///
    /// Not while a pass is already reading every footer of this dataset. That pass
    /// brings the row groups back with it, and this counter reads the same footers a
    /// second time — for a prefix of 6,541 files, 6,541 ranged reads to learn what is
    /// already on its way. Staging the open to save round trips and then spending them
    /// here would be worse than not staging it at all.
    pub fn remote_files_counter(&self) -> Option<FileCounter> {
        if self.footers_pending.is_some() {
            return None;
        }
        self.remote_files
            .as_ref()
            .filter(|f| f.offsets.is_none() && self.is_pristine())
            .map(|f| f.count.clone())
    }

    /// Record the rows in each row group of each file of a remote dataset: the total,
    /// the row groups a buffer is planned in, and which files hold which rows.
    ///
    /// A local dataset has no files to window over, and takes the total and the groups.
    fn record_file_row_groups(&mut self, groups: &[Vec<usize>]) {
        if let Some(files) = self.remote_files.as_mut() {
            if groups.len() != files.urls.len() {
                return;
            }
            let mut offsets = Vec::with_capacity(groups.len() + 1);
            offsets.push(0);
            for file in groups {
                offsets.push(offsets.last().unwrap_or(&0) + file.iter().sum::<usize>());
            }
            files.offsets = Some(offsets);
        }
        let flat: Vec<usize> = groups.iter().flatten().copied().collect();
        if self.is_pristine() {
            self.record_row_groups(&flat);
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
    /// How many of the dataset's files a page at `start` would read.
    ///
    /// A windowed remote scan reads only the files holding those rows; everything else
    /// hands the whole scan to Polars, which reads what it decides to and does not say.
    /// `None` is that second case — not zero, which would claim a page came from
    /// nowhere.
    pub fn files_a_page_reads(&self, start: usize, len: usize) -> Option<usize> {
        let offsets = self.files_window().and_then(|f| f.offsets.as_ref())?;
        let (first, last) = files_holding(offsets, start, len)?;
        Some(files_with_rows(offsets, first, last).len())
    }

    /// The frame for buffer rows `[start, start + len)`, columns in display order. For a
    /// remote dataset whose files are counted, a scan of only the files holding them.
    fn buffer_lf(&self, start: usize, len: usize) -> PolarsResult<LazyFrame> {
        let mut all_columns = self.binary_stub_exprs();
        if self.carries_source_rows() {
            all_columns.push(col(crate::schema_union::DRIFT_COLUMN));
        }
        self.window_lf(start, len, all_columns)
    }

    /// Whether the frame's rows carry their place in the source, for `#`: a dataset's
    /// rows that know their file, or lines, while the frame is still the scan's. A
    /// query's rows, a reshape's and a group's stand for no row of the source.
    pub fn carries_source_rows(&self) -> bool {
        self.drift_column_present
            || (self.scan_is_the_root() && (self.source_rows_at_open || self.view_numbered))
    }

    /// What `#` shows for `rows` rows from `start`: each row's place in the source
    /// where the rows carry it, else its place in the view, counted from
    /// `row_start_index`. A pristine view's places are the source's either way.
    pub fn row_numbers_from(&self, start: usize, rows: usize) -> Vec<usize> {
        let view = |i: usize| start + i + self.row_start_index;
        let places = self
            .buffered_df
            .as_ref()
            .filter(|_| self.carries_source_rows())
            .and_then(|df| df.column(crate::schema_union::DRIFT_COLUMN).ok())
            .and_then(|column| {
                let offset = start.checked_sub(self.buffered_start_row)?;
                let len = rows.min(column.len().saturating_sub(offset));
                let slice = column.slice(offset as i64, len);
                let places = slice.u32().ok()?;
                // Several files' lines are numbered in their own file.
                let place = |p: usize| {
                    self.numbering
                        .as_ref()
                        .and_then(|lines| lines.line_in_file(p))
                        .unwrap_or(p)
                };
                Some(
                    places
                        .iter()
                        .map(|p| p.map(|p| place(p as usize) + self.row_start_index))
                        .collect::<Vec<_>>(),
                )
            });
        (0..rows)
            .map(|i| {
                places
                    .as_ref()
                    .and_then(|p| p.get(i).copied().flatten())
                    .unwrap_or_else(|| view(i))
            })
            .collect()
    }

    /// The frame for rows `[start, start + len)` of the view, as `all_columns`. For a
    /// remote dataset whose files are counted, a scan of only the files holding them.
    fn window_lf(
        &self,
        start: usize,
        len: usize,
        all_columns: Vec<Expr>,
    ) -> PolarsResult<LazyFrame> {
        window_of(
            &self.lf,
            self.files_window(),
            self.window_now().as_deref(),
            &self.read_as_text,
            start,
            len,
            all_columns,
        )
    }

    /// The view's rows as a find reads them: a window at a time, the way a page is
    /// read, and the buffer already on hand.
    pub(crate) fn view_rows(&self) -> ViewRows {
        ViewRows {
            lf: self.lf.clone(),
            files: self.files_window().cloned(),
            // A find reads every row it can reach: lines still being indexed are read
            // through the frame, which waits for them, not the window of those so far.
            records: self.window_now().filter(|_| self.indexing().is_none()),
            read_as_text: self.read_as_text.clone(),
            buffer: self
                .buffered_df
                .as_ref()
                .filter(|_| self.buffer_on_hand())
                .map(|df| (df.clone(), self.buffered_start_row)),
            num_rows: self.num_rows_valid.then_some(self.num_rows),
            streaming: self.polars_streaming,
            whole: sees_every_row_first(&self.lf),
            reads_up_to: reads_up_to_a_window(&self.lf),
        }
    }

    /// Put the cursor on view row `row`, centered, for a find that matched there.
    /// Returns true if a collect is needed. A row past a provisional total is one the
    /// find read, so the total reaches it until the count lands.
    pub(crate) fn go_to_found_row(&mut self, row: usize) -> bool {
        if !self.num_rows_valid && self.num_rows <= row {
            self.num_rows = row + 1;
        }
        self.scroll_to_row_centered(row)
    }

    /// The view row the cursor is on.
    pub(crate) fn cursor_row(&self) -> usize {
        self.start_row + self.table_state.selected().unwrap_or(0)
    }

    /// Bytes a buffered row takes: measured on the last buffer collected, or until
    /// then estimated from the schema.
    fn bytes_per_row(&self) -> usize {
        self.observed_bytes_per_row.unwrap_or_else(|| {
            estimate_bytes_per_row(&self.schema, &self.column_order, &self.column_bytes)
        })
    }

    /// Best available in-memory width estimate for one logical row.
    ///
    /// Data Quality uses this only for a preflight estimate and labels the result as
    /// approximate. Buffer planning uses the same source so the two surfaces do not
    /// disagree about the shape of the current view.
    pub fn estimated_row_bytes(&self) -> usize {
        self.bytes_per_row()
    }

    /// Number of source files known to participate in the pristine dataset scan.
    /// Returns `None` after a query or reshape has broken the row-to-file mapping.
    pub fn source_file_count(&self) -> Option<usize> {
        self.is_pristine().then(|| self.loaded_file_count())
    }

    /// Files the dataset was loaded from, whatever the view does with their rows.
    pub(crate) fn loaded_file_count(&self) -> usize {
        if !self.drift_files.is_empty() {
            self.drift_files.len()
        } else if let Some(remote) = &self.remote_files {
            remote.urls.len()
        } else {
            1
        }
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

    /// True while the buffer is planned as a remote window: a scan of an object store
    /// that nothing has been applied to. A query, filter, sort or reshape reads the
    /// object through a predicate, and `slice(0, N)` then stops at the first N matches,
    /// so the page-based window costs a row group where the remote one would read forty.
    fn remote_window(&self) -> bool {
        self.remote_source && self.is_pristine()
    }

    /// The files a page reads by, while the frame is the scan as loaded: a filter or
    /// sort reads every file before its window, so it goes through the whole scan.
    fn files_window(&self) -> Option<&RemoteFiles> {
        self.remote_files.as_ref().filter(|_| self.is_pristine())
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

        // Stitched and cut as a background fill is, here on the spot, with the old
        // rows let go first: the plan has taken any it stitches on to.
        let plan = self.fill_plan(buffer_start, buffer_end, self.num_rows, self.num_rows_valid);
        self.release_display_buffer();
        let fitted = plan.fit(full_df);
        if fitted.bytes_per_row.is_some() {
            self.observed_bytes_per_row = fitted.bytes_per_row;
        }
        let full_df = fitted.df;
        let effective_buffer_start = fitted.start;
        let effective_buffer_end = fitted.start + full_df.height();

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
            self.locked_df = Some(locked_df);
        } else {
            self.locked_df = None;
        }

        let scroll_names: Vec<&str> = self
            .column_order
            .iter()
            .skip(self.frozen_shown() + self.termcol_index)
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
            self.df = Some(scroll_df);
        }
        if self.error.is_some() {
            self.error = None;
        }
        self.buffered_start_row = effective_buffer_start;
        self.buffered_end_row = effective_buffer_end;
        self.buffered_df = Some(full_df);
    }

    /// Let go of the buffer being replaced and the display frames cut from it. A
    /// synchronous load does so before its cut, so the cut's copy is not made while
    /// the old rows are still held; a stitch has already taken the rows it keeps.
    /// The view's rows come next, so a relearn asked for takes effect.
    fn release_display_buffer(&mut self) {
        self.widths.rows_arrived();
        self.buffered_df = None;
        self.locked_df = None;
        self.df = None;
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
                self.locked_df = Some(locked_df);
            }
        } else {
            self.locked_df = None;
        }

        let scroll_names: Vec<&str> = self
            .column_order
            .iter()
            .skip(self.frozen_shown() + self.termcol_index)
            .map(|s| s.as_str())
            .collect();
        if scroll_names.is_empty() {
            self.df = None;
        } else {
            if let Ok(scroll_df) = full_df.select(scroll_names) {
                self.df = Some(scroll_df);
            }
        }
    }

    /// Whether the view is inside the buffer and within a page of one of its ends, with
    /// more data past that end: where a collect would grow the buffer, if one ran. A
    /// scroll that stays inside the buffer runs none, so the growing waited until the
    /// view had left it — and the page was blank while it happened.
    pub fn wants_to_load_ahead(&self) -> bool {
        if self.visible_rows == 0
            || self.buffered_df.is_none()
            || !self.page_on_hand(self.start_row)
        {
            return false;
        }
        let near = self.proximity();
        let view_end = self.start_row
            + self
                .visible_rows
                .min(self.num_rows_bound().saturating_sub(self.start_row));
        let behind =
            self.start_row - self.buffered_start_row <= near && self.buffered_start_row > 0;
        let ahead = self.buffered_end_row - view_end <= near
            && self.buffered_end_row < self.num_rows_bound();
        behind || ahead
    }

    /// How close the view comes to an end of the buffer before the buffer grows past
    /// it: half the reach ahead, and never under a page. A page was the whole margin, and
    /// a cloud fetch takes longer than the next PageDown does to cross it.
    fn proximity(&self) -> usize {
        (self.reach_rows(self.pages_lookahead) / 2).max(self.visible_rows)
    }

    /// Where the view and the buffer are, to tell one load-ahead attempt from the next.
    pub fn buffer_position(&self) -> (u64, usize, usize, usize) {
        (
            self.len_generation(),
            self.start_row,
            self.buffered_start_row,
            self.buffered_end_row,
        )
    }

    /// Whether every row of the page starting at `start` is in the buffer.
    fn page_on_hand(&self, start: usize) -> bool {
        let bound = self.num_rows_bound();
        let end = start + self.visible_rows.min(bound.saturating_sub(start));
        self.buffered_df.is_some()
            && self.buffered_end_row > 0
            && start >= self.buffered_start_row
            && end <= self.buffered_end_row
    }

    /// The first row to draw: the view's own once its rows are on hand, and until then
    /// the last page that was drawn whole. The view moves the moment a key asks, before
    /// its rows are fetched, and drawn from there it was half a page of rows over half a
    /// page of nothing until the fetch landed.
    fn start_to_draw(&mut self) -> usize {
        if self.page_on_hand(self.start_row) {
            self.drawn_start = self.start_row;
            self.start_row
        } else if self.page_on_hand(self.drawn_start) {
            self.drawn_start
        } else {
            self.start_row
        }
    }

    fn slice_from_buffer(&mut self) {
        // Buffer contains the full range [buffered_start_row, buffered_end_row)
        // The displayed portion [start_row, start_row + visible_rows) is a subset
        // We'll slice the displayed portion when rendering based on offset
        // No action needed here - the buffer is stored, slicing happens at render time
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
        self.scroll_columns(ColumnMove::StepRight);
    }

    pub fn scroll_left(&mut self) {
        self.scroll_columns(ColumnMove::StepLeft);
    }

    /// Which shown columns the table last drew, and the cursor's: what the control
    /// bar's column position says.
    pub fn columns_on_screen(&self) -> Option<OnScreen> {
        self.on_screen
    }

    /// How many columns scroll: the shown ones right of those drawn frozen.
    fn scroll_count(&self) -> usize {
        self.column_order.len().saturating_sub(self.frozen_shown())
    }

    /// Move the view sideways, leaving the cursor where it is unless the view leaves
    /// it behind on the left. Planned from the widths the columns were last drawn at;
    /// reads nothing. A page that needs a column not drawn yet waits for the next
    /// draw, which measures it from the rows on hand; a relative move typed behind it
    /// waits too and lands after it, in order, so no key is lost or planned on a guess.
    pub fn scroll_columns(&mut self, mv: ColumnMove) {
        if matches!(
            mv,
            ColumnMove::First | ColumnMove::Last | ColumnMove::Reveal(_)
        ) {
            // Where these go does not depend on where the moves before them went.
            self.column_moves.clear();
        }
        if self.column_moves.is_empty()
            && let Some(start) = self.plan_known(mv)
        {
            self.apply_column_move(mv, start);
        } else {
            self.wait(WaitingMove::View(mv));
        }
    }

    /// Move the column cursor (`h` `l` `[` `]` `{` `}`), the view following only when
    /// the cursor would leave the screen. Reads nothing; a move that needs a column
    /// not drawn yet waits for the next draw, in order, as [`Self::scroll_columns`]
    /// says.
    pub fn move_cursor(&mut self, mv: CursorMove) {
        if matches!(mv, CursorMove::First | CursorMove::Last) {
            self.column_moves.clear();
        }
        if !self.column_moves.is_empty()
            || !self.land_cursor_move(mv, &mut |state: &mut Self, view| state.plan_known(view))
        {
            self.wait(WaitingMove::Cursor(mv));
        }
    }

    /// Put the cursor on the shown column `name` and show it as `g` does: left where
    /// it is when already whole on screen, else first after the frozen columns, or on
    /// the last page when it is there. A frozen column is on screen already.
    pub fn go_to_column(&mut self, name: &str) {
        let Some(at) = self.column_order.iter().position(|c| c == name) else {
            return;
        };
        self.column_moves.clear();
        self.place_cursor_at(at);
        if let Some(index) = at.checked_sub(self.frozen_shown()) {
            self.scroll_columns(ColumnMove::Reveal(index));
        }
    }

    /// Put the cursor on the shown column `name`, scrolling as little as it takes to
    /// show it.
    pub fn set_current_column(&mut self, name: &str) {
        let Some(at) = self.column_order.iter().position(|c| c == name) else {
            return;
        };
        self.column_moves.clear();
        self.place_cursor_at(at);
        self.follow_cursor(&mut |state: &mut Self, view| state.plan_known(view));
    }

    /// The column cursor's column: the one the per-column keys act on (value counts,
    /// copying a cell, the sidebar and inspector opening on it, a find in one column).
    /// The first shown column until the cursor moves; `None` with no columns shown.
    pub fn current_column(&self) -> Option<&str> {
        self.cursor_index().map(|at| self.column_order[at].as_str())
    }

    /// The cursor's place among the shown columns, from 0, frozen ones first.
    pub fn current_column_index(&self) -> Option<usize> {
        self.cursor_index()
    }

    fn cursor_index(&self) -> Option<usize> {
        let last = self.column_order.len().checked_sub(1)?;
        Some(
            self.cursor_column
                .as_deref()
                .and_then(|name| self.column_order.iter().position(|c| c == name))
                .unwrap_or(self.cursor_at.min(last)),
        )
    }

    fn place_cursor_at(&mut self, at: usize) {
        self.cursor_column = self.column_order.get(at).cloned();
        self.cursor_at = at;
    }

    /// After the shown columns changed: the cursor stays on its column by name, or,
    /// where that was hidden, takes the one now in its place; the next draw shows it.
    fn settle_cursor(&mut self) {
        let at = self.cursor_index().unwrap_or(0);
        self.place_cursor_at(at);
        self.reveal_cursor = true;
    }

    /// Queue a move for the next draw, behind any already waiting.
    fn wait(&mut self, mv: WaitingMove) {
        if self.column_moves.len() < MAX_WAITING_MOVES {
            self.column_moves.push(mv);
        }
    }

    /// Scroll as little as it takes to show the cursor's column whole, with `plan`;
    /// a plan that needs a width not drawn yet waits for the next draw.
    fn follow_cursor(&mut self, plan: &mut impl FnMut(&mut Self, ColumnMove) -> Option<usize>) {
        let Some(index) = self
            .cursor_index()
            .and_then(|at| at.checked_sub(self.frozen_shown()))
        else {
            return;
        };
        let view = ColumnMove::Keep(index);
        match plan(self, view) {
            // On screen already: nothing moves, and the trail `[` retraces stays.
            Some(start) if start == self.termcol_index => {}
            Some(start) => self.apply_column_move(view, start),
            None => self.wait(WaitingMove::View(view)),
        }
    }

    /// Land a cursor move, the view planned with `plan`. Returns false, changing
    /// nothing, when a page cannot be planned yet: where it lands decides the cursor.
    fn land_cursor_move(
        &mut self,
        mv: CursorMove,
        plan: &mut impl FnMut(&mut Self, ColumnMove) -> Option<usize>,
    ) -> bool {
        let Some(cursor) = self.cursor_index() else {
            return true;
        };
        let last = self.column_order.len() - 1;
        let frozen = self.frozen_shown();
        match mv {
            CursorMove::Left | CursorMove::Right => {
                let at = if mv == CursorMove::Left {
                    cursor.saturating_sub(1)
                } else {
                    (cursor + 1).min(last)
                };
                self.place_cursor_at(at);
                self.follow_cursor(plan);
            }
            CursorMove::First | CursorMove::Last => {
                let (at, view) = if mv == CursorMove::First {
                    (0, ColumnMove::First)
                } else {
                    (last, ColumnMove::Last)
                };
                self.place_cursor_at(at);
                match plan(self, view) {
                    Some(start) => self.apply_column_move(view, start),
                    None => self.wait(WaitingMove::View(view)),
                }
            }
            CursorMove::PageLeft | CursorMove::PageRight => {
                let view = if mv == CursorMove::PageLeft {
                    ColumnMove::PageLeft
                } else {
                    ColumnMove::PageRight
                };
                let Some(start) = plan(self, view) else {
                    return false;
                };
                let from = self.termcol_index;
                self.apply_column_move(view, start);
                let at = if self.termcol_index != from {
                    // The new page, from its first column.
                    frozen + self.termcol_index
                } else if mv == CursorMove::PageRight {
                    // On the last page already: its last column.
                    last
                } else if cursor > frozen {
                    // On the first page: its first column, then the first of all.
                    frozen
                } else {
                    0
                };
                self.place_cursor_at(at.min(last));
            }
        }
        true
    }

    /// The scrolling columns, by name.
    fn scrolling_names(&self) -> &[String] {
        &self.column_order[self.frozen_shown().min(self.column_order.len())..]
    }

    /// `[` straight after the `]` that came here goes back where that one started,
    /// whatever the widths say, so a page and back is the page left.
    fn retrace(&self, mv: ColumnMove) -> Option<usize> {
        let &(back, to) = self.page_trail.last()?;
        (mv == ColumnMove::PageLeft && to == self.termcol_index).then_some(back)
    }

    /// Where `mv` lands on the widths drawn in this view, or `None` when it needs one
    /// not drawn yet (or the room, before the first draw).
    fn plan_known(&self, mv: ColumnMove) -> Option<usize> {
        if let Some(back) = self.retrace(mv) {
            return Some(back);
        }
        let needs_widths = match mv {
            ColumnMove::StepLeft | ColumnMove::StepRight | ColumnMove::First => false,
            // Back to a column at or left of the first shown needs no width.
            ColumnMove::Keep(column) => column > self.termcol_index,
            _ => true,
        };
        let room = match self.scroll_room {
            Some(room) => room,
            None if needs_widths => return None,
            None => Room::default(),
        };
        let names = self.scrolling_names();
        crate::widgets::column_paging::plan(mv, self.termcol_index, names.len(), room, |i| {
            self.drawn_width(&names[i])
        })
    }

    /// Land `mv` at `start`, keeping the trail `[` retraces. A cursor the view leaves
    /// behind on the left comes along, to the first column shown.
    fn apply_column_move(&mut self, mv: ColumnMove, start: usize) {
        let from = self.termcol_index;
        let start = start.min(self.scroll_count().saturating_sub(1));
        match mv {
            ColumnMove::PageRight => {
                if start > from {
                    self.page_trail.push((from, start));
                }
            }
            ColumnMove::PageLeft if self.retrace(mv) == Some(start) => {
                self.page_trail.pop();
            }
            _ => self.page_trail.clear(),
        }
        self.scroll_columns_to(start);
        let frozen = self.frozen_shown();
        if let Some(cursor) = self.cursor_index()
            && cursor >= frozen
            && cursor < frozen + self.termcol_index
        {
            self.place_cursor_at(frozen + self.termcol_index);
        }
    }

    /// Forget sideways moves waiting on a draw and the trail `[` retraces: the
    /// columns they were counted over are gone.
    fn clear_column_moves(&mut self) {
        self.column_moves.clear();
        self.page_trail.clear();
    }

    /// Start the scrolling columns at `start`, re-slicing the buffer held.
    fn scroll_columns_to(&mut self, start: usize) {
        let start = start.min(self.scroll_count().saturating_sub(1));
        if start != self.termcol_index {
            self.termcol_index = start;
            self.rescroll_columns();
        }
    }

    /// Record the scrolling side as the renderer lays it out, land the moves waiting
    /// on it, in order, and bring the cursor back on screen when it may have left,
    /// with `width`, which measures a column not drawn yet from the rows on hand.
    /// Called while drawing, before the scrolling columns are drawn; reads nothing,
    /// and measures only the columns a move crosses. With no rows on hand the moves
    /// wait for a draw that has them.
    fn land_column_moves(&mut self, room: Room, mut width: impl FnMut(&mut Self, &str) -> u16) {
        if self.scroll_room != Some(room) {
            // A resize, or a frozen column given back: the cursor may be off screen.
            self.reveal_cursor = true;
        }
        self.scroll_room = Some(room);
        if (self.column_moves.is_empty() && !self.reveal_cursor)
            || !self.buffer_on_hand()
            || self.defer_collect
        {
            return;
        }
        let mut plan = |state: &mut Self, mv: ColumnMove| -> Option<usize> {
            if let Some(back) = state.retrace(mv) {
                return Some(back);
            }
            let from = state.termcol_index;
            let count = state.scroll_count();
            Some(
                crate::widgets::column_paging::plan(mv, from, count, room, |i| {
                    let name = state.scrolling_names()[i].clone();
                    Some(width(state, &name))
                })
                .unwrap_or(from),
            )
        };
        for mv in std::mem::take(&mut self.column_moves) {
            match mv {
                WaitingMove::View(mv) => {
                    let start = plan(self, mv).unwrap_or(self.termcol_index);
                    self.apply_column_move(mv, start);
                }
                WaitingMove::Cursor(mv) => {
                    self.land_cursor_move(mv, &mut plan);
                }
            }
        }
        if std::mem::take(&mut self.reveal_cursor) {
            self.follow_cursor(&mut plan);
        }
    }

    /// Show the new column window, reading nothing.
    ///
    /// A sideways move changes which columns are on screen, not which rows, so it
    /// re-slices the buffer already held. It must not go through [`collect`], which
    /// counts the rows when the count is not yet known: `App::handle` calls
    /// `scroll_right` inline on the thread that draws and reads keys, and
    /// `key_acts_while_busy` lets Left and Right through while other work runs. On a
    /// staged-open cloud hive that count is a metadata read per object, and taken
    /// there it is a freeze no keystroke can interrupt.
    ///
    /// Nothing is drawn when there is no buffer to re-slice, or when what is held
    /// does not match the range it claims. [`collect`] reloaded the page in that
    /// second case; this does not, because `load_buffer` is a collect of that page
    /// and on a cloud hive that is row groups over the wire — the same freeze in a
    /// smaller size.
    ///
    /// The index still moves, so presses before the first buffer lands are spent on
    /// a view that cannot show them yet, and the first frame drawn is already scrolled
    /// to wherever they left it. That is the pre-existing behaviour: the old path
    /// redrew each press, but only by paying the wait this exists to avoid.
    ///
    /// [`collect`]: Self::collect
    fn rescroll_columns(&mut self) {
        if self.defer_collect || !self.buffer_on_hand() {
            return;
        }
        self.slice_buffer_into_display();
        if self.table_state.selected().is_none() {
            self.table_state.select(Some(0));
        }
    }

    pub fn headers(&self) -> Vec<String> {
        self.column_order.clone()
    }

    pub fn set_column_order(&mut self, order: Vec<String>) {
        self.column_order = order;
        self.clear_column_moves();
        // Fewer columns shown may leave the scroll past the last; keep one on screen.
        self.termcol_index = self
            .termcol_index
            .min(self.scroll_count().saturating_sub(1));
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.settle_cursor();
        self.collect();
    }

    pub fn set_locked_columns(&mut self, count: usize) {
        self.locked_columns_count = count.min(self.column_order.len());
        self.clear_column_moves();
        self.settle_cursor();
        self.termcol_index = self
            .termcol_index
            .min(self.scroll_count().saturating_sub(1));
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.collect();
    }

    pub fn locked_columns_count(&self) -> usize {
        self.locked_columns_count
    }

    /// How many columns are drawn frozen: the count asked for, or fewer while the
    /// last layout could not fit them all beside a usable scrolling column. The ones
    /// left out lead the scrolling columns, so every column stays reachable.
    pub fn frozen_shown(&self) -> usize {
        let (asked, shown) = self.frozen_fit;
        if asked == self.locked_columns_count {
            shown.min(asked)
        } else {
            self.locked_columns_count
        }
    }

    /// Take the layout's word for how many frozen columns fit, and re-slice the
    /// scrolling columns to start after them. Unscrolled, the frozen columns left out
    /// lead the scrolling ones; scrolled, the column the scroll started at stays
    /// first where it can, so a resize does not also move the view. Reads nothing;
    /// called while drawing, and only re-selects columns of the buffer already held.
    fn fit_frozen(&mut self, shown: usize) {
        let before = self.frozen_shown();
        let shown = shown.min(self.locked_columns_count);
        if shown == before {
            self.frozen_fit = (self.locked_columns_count, shown);
            return;
        }
        if self.defer_collect || !self.buffer_on_hand() {
            return;
        }
        self.frozen_fit = (self.locked_columns_count, shown);
        // The scrolling indices the trail was kept in shift with the frozen count.
        self.page_trail.clear();
        if self.termcol_index > 0 {
            let first = before + self.termcol_index;
            let last = self.column_order.len().saturating_sub(1);
            self.termcol_index = first.min(last).saturating_sub(shown);
        }
        self.slice_buffer_into_display();
    }

    /// The type a column has in the frame on screen: with its name, the identity its
    /// width is kept under.
    fn width_dtype(&self, name: &str) -> DataType {
        self.schema.get(name).cloned().unwrap_or(DataType::Null)
    }

    /// How a column's width is chosen.
    pub fn width_choice(&self, name: &str) -> WidthChoice {
        self.widths.choice(name, &self.width_dtype(name))
    }

    /// The width a column was last drawn at, if it has been drawn.
    pub fn shown_width(&self, name: &str) -> Option<u16> {
        self.widths.shown(name, &self.width_dtype(name))
    }

    /// The width the column takes on screen, the room it filled at the right edge
    /// included.
    pub fn on_screen_width(&self, name: &str) -> Option<u16> {
        self.widths.on_screen(name, &self.width_dtype(name))
    }

    /// The width a column draws at in this view, if it has been drawn since the
    /// widths were last relearned. What a sideways page is planned with.
    fn drawn_width(&self, name: &str) -> Option<u16> {
        self.widths.drawn(name, &self.width_dtype(name))
    }

    /// Set how each named column's width is chosen. Reads nothing: a fit is taken
    /// from the rows on screen when the table is next drawn.
    pub fn set_width_choices(&mut self, choices: impl IntoIterator<Item = (String, WidthChoice)>) {
        for (name, choice) in choices {
            let dtype = self.width_dtype(&name);
            self.widths.set_choice(&name, &dtype, choice);
        }
    }

    /// One column's rows on screen, from the buffer already held, as the table draws
    /// them. For fitting a column that may be scrolled out of view.
    fn page_column(&self, name: &str, offset: usize, len: usize) -> Option<DataFrame> {
        let column = self.buffered_df.as_ref()?.select([name]).ok()?;
        visible_slice(&column, offset, len)
    }

    // Getter methods for view creation
    /// Filters for a view: while drilled into a group these are the grouped view's,
    /// which is what a view reproduces (it cannot express a drill-down).
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

    pub fn get_sort_descending(&self) -> &[bool] {
        match &self.grouped {
            Some(view) => &view.sort_descending,
            None => &self.sort_descending,
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

    pub fn view_sort_descending(&self) -> &[bool] {
        &self.sort_descending
    }

    /// The header's sort marks: the sidebar's sort, or else the ORDER BY of the SQL
    /// in effect, while its own rows are on screen (not a group drilled into).
    pub fn header_sort(&self) -> (Vec<String>, Vec<bool>) {
        if self.sort_columns.is_empty() && self.grouped.is_none() {
            self.query_order.iter().cloned().unzip()
        } else {
            (self.sort_columns.clone(), self.sort_descending.clone())
        }
    }

    /// The pivot/melt result in effect, for a snapshot that may need to put it back.
    pub fn reshaped_lf_clone(&self) -> Option<LazyFrame> {
        self.reshaped_lf.clone()
    }

    pub fn get_column_order(&self) -> &[String] {
        &self.column_order
    }

    /// Whether the table shows its defaults: no query, filters, sort or
    /// reshape, every column in file order, nothing locked. A view saved
    /// from this state would carry nothing — and, matching by schema, it
    /// would shadow real views in the apply gate as a well-used no-op.
    pub fn is_at_defaults(&self) -> bool {
        self.sampled.is_none()
            && self.column_changes.is_empty()
            && self.active_query.is_empty()
            && self.active_sql_query.is_empty()
            && self.active_fuzzy_query.is_empty()
            && self.filters.is_empty()
            && self.sort_columns.is_empty()
            && self.last_pivot_spec.is_none()
            && self.last_melt_spec.is_none()
            && self.locked_columns_count() == 0
            && self
                .column_order
                .iter()
                .map(String::as_str)
                .eq(self.schema.iter_names().map(|s| s.as_str()))
    }

    pub fn get_active_query(&self) -> &str {
        &self.active_query
    }

    pub fn get_active_sql_query(&self) -> &str {
        &self.active_sql_query
    }

    /// Whether the rows on screen can be read at all: a sort, a filter or a column
    /// named in the layout that the frame does not have fails here. Resolves the plan
    /// and reads no rows.
    pub fn check_plan(&self) -> PolarsResult<()> {
        self.lf
            .clone()
            .select(self.binary_stub_exprs())
            .collect_schema()
            .map(|_| ())
    }

    /// The view as it is now, to go back to if a query fails while running.
    pub fn rollback_point(&self) -> ViewRollback {
        ViewRollback {
            root_generation: self.root_generation,
            counted: None,
            drawn_start: self.drawn_start,
            lf: self.lf.clone(),
            unsorted_lf: self.unsorted_lf.clone(),
            base_lf: self.base_lf.clone(),
            df: self.df.clone(),
            locked_df: self.locked_df.clone(),
            table_state: self.table_state,
            start_row: self.start_row,
            termcol_index: self.termcol_index,
            cursor_column: self.cursor_column.clone(),
            cursor_at: self.cursor_at,
            schema: self.schema.clone(),
            num_rows: self.num_rows,
            num_rows_valid: self.num_rows_valid,
            len_generation: self.len_generation,
            filters: self.filters.clone(),
            sort_columns: self.sort_columns.clone(),
            sort_descending: self.sort_descending.clone(),
            sort_ascending: self.sort_ascending,
            active_query: self.active_query.clone(),
            active_sql_query: self.active_sql_query.clone(),
            query_order: self.query_order.clone(),
            active_fuzzy_query: self.active_fuzzy_query.clone(),
            column_order: self.column_order.clone(),
            locked_columns_count: self.locked_columns_count,
            frozen_fit: self.frozen_fit,
            grouped: self.grouped.clone(),
            reshaped_lf: self.reshaped_lf.clone(),
            last_pivot_spec: self.last_pivot_spec.clone(),
            last_melt_spec: self.last_melt_spec.clone(),
            reshape_source: self.reshape_source.clone(),
            base_steps: self.base_steps.clone(),
            reshape_steps: self.reshape_steps.clone(),
            lineage: self.lineage.clone(),
            reshape_lineage: self.reshape_lineage.clone(),
            group_source: self.group_source.clone(),
            drilled_down_group_index: self.drilled_down_group_index,
            drilled_down_group_key: self.drilled_down_group_key.clone(),
            drilled_down_group_key_columns: self.drilled_down_group_key_columns.clone(),
            drift_column_present: self.drift_column_present,
            view_numbered: self.view_numbered,
            drift_groups: self.drift_groups.clone(),
            notes: self.notes.clone(),
            notes_seen: self.notes_seen,
            view_notes: self.view_notes.clone(),
            column_changes: self.column_changes.clone(),
            changes_version: self.changes_version,
            changes_dropped: self.changes_dropped.clone(),
            observed_bytes_per_row: self.observed_bytes_per_row,
            buffered_start_row: self.buffered_start_row,
            buffered_end_row: self.buffered_end_row,
            buffered_df: self.buffered_df.clone(),
        }
    }

    /// Put back the view `rollback_point` saved, with no error showing. Its row count
    /// and buffer come back with it, and a count of it that landed meanwhile (see
    /// [`ViewRollback::count_landed`]), so nothing is read again. Nothing is read here:
    /// a view with no rows on hand has them read by the caller's next collect.
    ///
    /// A checkpoint taken over data since replaced — a join of the remaining footers,
    /// a column read as text — holds frames built on data no longer loaded. Putting
    /// those back would mix two roots, so the view returns to the data as loaded instead.
    pub fn roll_back(&mut self, saved: ViewRollback) {
        if saved.root_generation != self.root_generation {
            self.return_to_root();
            return;
        }
        self.widths.keep_learned();
        self.drawn_start = saved.drawn_start;
        self.lf = saved.lf;
        self.unsorted_lf = saved.unsorted_lf;
        self.base_lf = saved.base_lf;
        self.df = saved.df;
        self.locked_df = saved.locked_df;
        self.table_state = saved.table_state;
        self.start_row = saved.start_row;
        self.termcol_index = saved.termcol_index;
        self.clear_column_moves();
        self.schema = saved.schema;
        self.num_rows = saved.num_rows;
        self.num_rows_valid = saved.num_rows_valid;
        self.len_generation = saved.len_generation;
        self.filters = saved.filters;
        self.sort_columns = saved.sort_columns;
        self.sort_descending = saved.sort_descending;
        self.sort_ascending = saved.sort_ascending;
        self.active_query = saved.active_query;
        self.active_sql_query = saved.active_sql_query;
        self.query_order = saved.query_order;
        self.active_fuzzy_query = saved.active_fuzzy_query;
        self.column_order = saved.column_order;
        self.locked_columns_count = saved.locked_columns_count;
        self.frozen_fit = saved.frozen_fit;
        self.cursor_column = saved.cursor_column;
        self.cursor_at = saved.cursor_at;
        self.reveal_cursor = true;
        self.grouped = saved.grouped;
        // A q query or a search forgets the pivot or melt it replaces.
        self.reshaped_lf = saved.reshaped_lf;
        self.last_pivot_spec = saved.last_pivot_spec;
        self.last_melt_spec = saved.last_melt_spec;
        self.reshape_source = saved.reshape_source;
        self.base_steps = saved.base_steps;
        self.reshape_steps = saved.reshape_steps;
        self.lineage = saved.lineage;
        self.reshape_lineage = saved.reshape_lineage;
        self.group_source = saved.group_source;
        self.drilled_down_group_index = saved.drilled_down_group_index;
        self.drilled_down_group_key = saved.drilled_down_group_key;
        self.drilled_down_group_key_columns = saved.drilled_down_group_key_columns;
        self.drift_column_present = saved.drift_column_present;
        self.view_numbered = saved.view_numbered;
        self.drift_groups = saved.drift_groups;
        self.column_changes = saved.column_changes;
        self.changes_version = saved.changes_version;
        self.changes_dropped = saved.changes_dropped;
        self.notes = saved.notes;
        self.notes_seen = saved.notes_seen;
        self.view_notes = saved.view_notes;
        self.observed_bytes_per_row = saved.observed_bytes_per_row;
        self.buffered_start_row = saved.buffered_start_row;
        self.buffered_end_row = saved.buffered_end_row;
        self.buffered_df = saved.buffered_df;
        self.error = None;
        // After the frame, so the count is taken as this frame's.
        if let Some(counted) = saved.counted {
            self.take_count(counted.rows, counted.file_row_groups.as_deref());
        }
    }

    /// Run `steps` as one transition of the view: planned, never read (no collect
    /// runs while they do), starting with no error showing. If a step fails, the view
    /// before them is put back and the error returned. If they all plan, the view
    /// before them comes back with the result, for the caller to restore with
    /// [`Self::roll_back`] should reading the new view's rows fail.
    pub fn try_transition<T, E>(
        &mut self,
        steps: impl FnOnce(&mut Self) -> std::result::Result<T, E>,
    ) -> std::result::Result<(T, ViewRollback), E> {
        let saved = self.rollback_point();
        self.error = None;
        match self.deferred(steps) {
            Ok(value) => Ok((value, saved)),
            Err(e) => {
                self.roll_back(saved);
                Err(e)
            }
        }
    }

    /// Run `steps` with every collect they would make left to the caller, who reads
    /// the rows off the UI thread (`prepare_async_collect`).
    pub fn deferred<R>(&mut self, steps: impl FnOnce(&mut Self) -> R) -> R {
        let deferred = std::mem::replace(&mut self.defer_collect, true);
        let result = steps(self);
        self.defer_collect = deferred;
        result
    }

    /// A background count of frame `len_generation` came back. Taken when that frame is
    /// the one on screen; returns whether it was.
    pub fn count_landed(
        &mut self,
        len_generation: u64,
        rows: usize,
        file_row_groups: Option<&[Vec<usize>]>,
    ) -> bool {
        let current = len_generation == self.len_generation;
        if current {
            self.take_count(rows, file_row_groups);
        }
        current
    }

    /// What a staged open leaves: a row total from however far the buffer reached,
    /// with no count taken.
    #[cfg(test)]
    pub(crate) fn set_provisional_rows(&mut self, n: usize) {
        self.num_rows = n;
    }

    /// The count of the frame on screen: from the files' row groups when there are
    /// some, else the total.
    fn take_count(&mut self, rows: usize, file_row_groups: Option<&[Vec<usize>]>) {
        match file_row_groups {
            Some(groups) => self.record_file_row_groups(groups),
            None => self.set_num_rows(rows),
        }
    }

    /// The follow of the file this dataset reads, while it is followed.
    pub fn follow(&self) -> Option<&crate::follow::Follow> {
        self.follow.as_ref()
    }

    pub fn follow_mut(&mut self) -> Option<&mut crate::follow::Follow> {
        self.follow.as_mut()
    }

    /// Join `fields`, which arrived in a followed pipe's NDJSON after the open, to the
    /// dataset: its scan reads them, and they go on the end of the column order, as a
    /// dataset's footers join theirs. `Err` while the view is a query, a reshape or a
    /// group, which would lose the columns it is built from: the caller holds them
    /// until the view is back on the data. `Ok(false)` when there is nothing to join.
    pub(crate) fn join_followed_fields(
        &mut self,
        fields: &[Field],
    ) -> std::result::Result<bool, ()> {
        if !self.scan_is_the_root() {
            return Err(());
        }
        let (Some(follow), Some(format)) = (self.follow.as_ref(), self.read_as) else {
            return Ok(false);
        };
        let (path, rows) = (follow.path().to_path_buf(), follow.shown());
        let Some(mut lf) = crate::follow::widen(&self.original_lf, &path, format, fields, rows)
        else {
            return Ok(false);
        };
        let Ok(schema) = lf.collect_schema() else {
            return Ok(false);
        };
        let known: std::collections::HashSet<&str> =
            self.column_order.iter().map(String::as_str).collect();
        let joining: Vec<String> = schema
            .iter_names()
            .map(|name| name.to_string())
            .filter(|name| !known.contains(name.as_str()))
            .collect();
        drop(known);
        self.column_order.extend(joining);
        self.replace_root(lf, schema);
        if self.is_pristine() {
            // The same rows the watcher counted, with more columns.
            self.set_num_rows(rows);
        }
        // Rebuilt but not read, as a footer join is: the caller reads the rows on
        // screen off the event loop.
        self.deferred(Self::apply_transformations);
        Ok(true)
    }

    /// Follow the file this dataset reads with `follow`, whose watcher is running.
    pub fn start_following(&mut self, follow: crate::follow::Follow) {
        self.follow = Some(follow);
    }

    /// Stop following. The rows read so far stay.
    pub fn stop_following(&mut self) {
        if let Some(mut follow) = self.follow.take() {
            follow.end();
        }
    }

    /// Put the view on the last page, leaving the cursor where it is until the rows of
    /// that page are read: the next read is of that page alone.
    pub(crate) fn aim_at_end(&mut self) {
        if self.num_rows_valid && self.visible_rows > 0 {
            self.start_row = self.num_rows.saturating_sub(self.visible_rows);
        }
    }

    /// Whether the cursor is on the last row of a view whose length is known.
    pub fn on_last_row(&self) -> bool {
        self.num_rows_valid
            && (self.num_rows == 0
                || self.start_row + self.table_state.selected().unwrap_or(0) + 1 >= self.num_rows)
    }

    /// Every frame the view holds that carries the scan of the data as loaded.
    fn each_frame(&mut self, mut f: impl FnMut(&mut LazyFrame)) {
        f(&mut self.original_lf);
        f(&mut self.base_lf);
        f(&mut self.lf);
        if let Some(lf) = self.unsorted_lf.as_mut() {
            f(lf);
        }
        if let Some(lf) = self.reshaped_lf.as_mut() {
            f(lf);
        }
        if let Some(source) = self.group_source.as_mut() {
            f(&mut source.rows);
        }
        if let Some(grouped) = self.grouped.as_mut() {
            f(&mut grouped.lf);
            f(&mut grouped.base_lf);
            if let Some(source) = grouped.group_source.as_mut() {
                f(&mut source.rows);
            }
        }
    }

    /// The followed file holds `rows` complete rows now: every frame reads that many,
    /// so the query, filters and sort run over the new ones too. `restarted` when the
    /// file was read again from its start. Returns whether the rows on hand still
    /// stand: a view that only filters, with nothing reordered, keeps the rows it had,
    /// since rows only arrive after them.
    pub(crate) fn follow_to(&mut self, rows: usize, restarted: bool) -> bool {
        let Some(path) = self.follow.as_ref().map(|f| f.path().to_path_buf()) else {
            return true;
        };
        let rows_stand = !restarted
            && self.sort_columns.is_empty()
            && self.sort_ascending
            && self.scan_is_the_root();
        let known = self.known_before_follow(&path, restarted);
        self.each_frame(|lf| crate::follow::bound(lf, &path, rows));
        self.invalidate_num_rows();
        self.follow_known = known.map(|known| (self.len_generation, known));
        if self.is_pristine() {
            // The watcher counted them as the scan reads them: nothing to count again.
            self.set_num_rows(rows);
        } else if self.scan_is_the_root() {
            self.pristine_rows = Some(rows);
        }
        if restarted {
            self.start_row = 0;
            self.table_state.select(Some(0));
        }
        if !rows_stand {
            self.drop_buffer();
        }
        rows_stand
    }

    /// Where the view's rows are known, before the frames read more of the followed file
    /// at `path`: what was known for the count on screen, and the count itself when it
    /// is exact. Only for a view whose rows are each kept or not by itself (filters and
    /// a sort over the file's rows), so the rows that arrive are counted alone.
    fn known_before_follow(&mut self, path: &Path, restarted: bool) -> Option<Vec<(usize, usize)>> {
        if restarted || self.is_pristine() || !self.scan_is_the_root() {
            return None;
        }
        let mut known = self
            .follow_known
            .take()
            .filter(|(generation, _)| *generation == self.len_generation)
            .map(|(_, known)| known);
        if self.num_rows_valid
            && let Some(row) = crate::follow::bound_of(&self.lf, path)
        {
            let known = known.get_or_insert_with(Vec::new);
            // One point per stretch of marks is enough to read on from.
            if let [.., before, last] = known.as_slice()
                && last.1 - before.1 < crate::follow::MARK_ROWS as usize
            {
                known.pop();
            }
            if known.last().is_none_or(|&(_, at)| at < row) {
                known.push((self.num_rows, row));
            }
        }
        known
    }

    /// The followed file was deleted: every frame reads it through `file`, a handle
    /// held on it, which still reads what it held.
    pub(crate) fn read_followed_through(&mut self, file: &std::fs::File) {
        let Some(path) = self.follow.as_ref().map(|f| f.path().to_path_buf()) else {
            return;
        };
        self.each_frame(|lf| crate::follow::read_through(lf, &path, file));
    }

    /// The frame on screen: the root, then the query or reshape, the filters and the
    /// sort. Column order is applied when rows are read.
    pub fn lf(&self) -> &LazyFrame {
        &self.lf
    }

    /// Hand back the frame on screen, for a caller that built a state only to load it.
    pub fn into_lf(self) -> LazyFrame {
        self.lf
    }

    /// The schema of the frame on screen.
    pub fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    /// The rows the frame holds: exact when [`Self::is_num_rows_valid`], else as far as
    /// the reads so far have reached.
    pub fn num_rows(&self) -> usize {
        self.num_rows
    }

    /// Why the last query, step or read failed, while it is still showing.
    pub fn error(&self) -> Option<&PolarsError> {
        self.error.as_ref()
    }

    /// Stop showing the last failure. The view is as it was; only the message goes.
    pub fn dismiss_error(&mut self) {
        self.error = None;
    }

    /// The first row of the page on screen.
    pub fn start_row(&self) -> usize {
        self.start_row
    }

    /// The hive partition columns the dataset was loaded with.
    pub fn partition_columns(&self) -> Option<&[String]> {
        self.partition_columns.as_deref()
    }

    /// Whether reads use Polars' streaming engine.
    pub fn polars_streaming(&self) -> bool {
        self.polars_streaming
    }

    /// The group drilled into, as its key columns and their values.
    pub fn drilled_group_key(&self) -> Option<(&[String], &[String])> {
        let values = self.drilled_down_group_key.as_deref()?;
        let columns = self
            .drilled_down_group_key_columns
            .as_deref()
            .unwrap_or_default();
        Some((columns, values))
    }

    /// The columns of `df`, the table SQL runs against, with their types. From the
    /// schema already known for the data as loaded; a drilled group or a reshape only
    /// has its plan resolved, which reads nothing.
    pub fn sql_table_columns(&self) -> Vec<(String, DataType)> {
        let schema = if self.grouped.is_none() && self.reshaped_lf.is_none() {
            Some(self.original_schema.clone())
        } else {
            self.query_root().collect_schema().ok()
        };
        schema
            .map(|schema| {
                schema
                    .iter()
                    .filter(|(name, _)| name.as_str() != crate::schema_union::DRIFT_COLUMN)
                    .map(|(name, dtype)| (name.to_string(), dtype.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Rows `df` holds, when that is known without counting: the data as loaded,
    /// once its count has come back.
    pub fn sql_table_rows(&self) -> Option<usize> {
        if self.grouped.is_some() || self.reshaped_lf.is_some() {
            return None;
        }
        self.pristine_rows
    }

    pub fn get_active_fuzzy_query(&self) -> &str {
        &self.active_fuzzy_query
    }

    pub fn last_pivot_spec(&self) -> Option<&PivotSpec> {
        self.last_pivot_spec.as_ref()
    }

    pub fn last_melt_spec(&self) -> Option<&MeltSpec> {
        self.last_melt_spec.as_ref()
    }

    /// What the pivot or melt in effect ran over. See the field.
    pub fn reshape_source(&self) -> Option<&ReshapeSource> {
        self.reshape_source.as_ref()
    }

    /// Whether the view is a grouping's result (a `by` query, a SQL GROUP BY), so its
    /// rows drill into groups. Recorded by the query, never inferred from list columns:
    /// a table loaded with one is not grouped.
    pub fn is_grouped(&self) -> bool {
        self.group_source.is_some()
    }

    /// Whether any column holds lists.
    fn has_list_columns(&self) -> bool {
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

    /// The first `limit` distinct non-null values of `column` among the rows already
    /// buffered for display, as text. Reads nothing: a column the buffer lacks gives
    /// none.
    pub(crate) fn buffered_values(&self, column: &str, limit: usize) -> Vec<String> {
        let Some(series) = [self.df.as_ref(), self.locked_df.as_ref()]
            .into_iter()
            .flatten()
            .find_map(|df| df.column(column).ok())
        else {
            return Vec::new();
        };
        let series = series.as_materialized_series();
        let mut values = Vec::new();
        for value in (0..series.len()).filter_map(|index| series.get(index).ok()) {
            if values.len() == limit {
                break;
            }
            let text = match value {
                AnyValue::Null => continue,
                AnyValue::String(text) => text.to_string(),
                AnyValue::List(items) => crate::exact::list_preview(&items),
                // Drawn on the UI thread: Polars' display panics on a date past
                // the calendar.
                value => {
                    crate::exact::past_calendar_text(&value).unwrap_or_else(|| value.to_string())
                }
            };
            if !values.contains(&text) {
                values.push(text);
            }
        }
        values
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

    /// The selected row with every display column, raw and in display order:
    /// what a row copy carries.
    pub fn copy_row_df(&self) -> Option<DataFrame> {
        let df = self.buffered_df.as_ref()?;
        let absolute = self.start_row + self.table_state.selected()?;
        let offset = absolute.checked_sub(self.buffered_start_row)?;
        if offset >= df.height() {
            return None;
        }
        let names: Vec<&str> = self.column_order.iter().map(|s| s.as_str()).collect();
        df.select(names).ok().map(|d| d.slice(offset as i64, 1))
    }

    /// The rows on screen with every display column, raw, in display order and
    /// untouched by the column scroll: a copy that lost the columns scrolled
    /// past would deny exactly the identifiers that make the rows readable.
    pub fn copy_view_df(&self) -> Option<DataFrame> {
        let df = self.buffered_df.as_ref()?;
        let names: Vec<&str> = self.column_order.iter().map(|s| s.as_str()).collect();
        let selected = df.select(names).ok()?;
        let offset = self.start_row.saturating_sub(self.buffered_start_row);
        let len = self
            .visible_rows
            .min(selected.height().saturating_sub(offset));
        (len > 0).then(|| selected.slice(offset as i64, len))
    }

    /// The selected row's value in one column, exactly as stored (see
    /// [`crate::exact`]): a float as the decimal that reads back to it, never
    /// the table's rounded preview. A null is an empty string, like a null in
    /// an export — never the UI's glyph. A list or struct is JSON, as in a CSV
    /// export.
    pub fn copy_cell_value(&self, column: &str) -> Option<String> {
        let row = self.copy_row_df()?;
        crate::exact::copy_text(row.column(column).ok()?).ok()
    }

    /// The selected row's number as the row-numbers column would print it.
    pub fn selected_display_row(&self) -> Option<usize> {
        Some(self.start_row + self.table_state.selected()? + self.row_start_index)
    }

    /// Rows times estimated row width, for the copy guard: what collecting the whole
    /// view would hold, with binary at the base64 size a copy writes. None until the
    /// count has run, and while a binary column's width is unknown: the buffer holds a
    /// stub for it, so only a footer says how wide it is.
    pub fn estimated_copy_bytes(&self) -> Option<usize> {
        let rows = self.num_rows_if_valid()?;
        if rows == 0 {
            return Some(0);
        }
        let base64 = |bytes: usize| bytes.div_ceil(3) * 4;
        let footer_width = |name: &str| {
            self.column_bytes
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, w)| *w)
        };
        let mut row = self.bytes_per_row();
        for name in &self.column_order {
            match self.schema.get(name.as_str()) {
                Some(DataType::Binary) => row += base64(footer_width(name)?),
                // Buffered whole, so the buffer measured it with the row; base64 adds
                // a third on top.
                Some(dtype) if crate::nested_json::has_binary(dtype) => {
                    let buffered = self.buffered_df.as_ref().and_then(|df| {
                        let column = df.column(name).ok()?;
                        (df.height() > 0)
                            .then(|| column.as_materialized_series().estimated_size() / df.height())
                    });
                    row += buffered.or_else(|| footer_width(name)).unwrap_or(0) / 3;
                }
                _ => {}
            }
        }
        Some(rows.saturating_mul(row))
    }

    /// The drift group of each row from the top of the view down, for a frame
    /// `frame_rows` tall, when the dataset's files differ.
    ///
    /// Sized by the frame about to be drawn rather than by `visible_rows`, which is
    /// what the *last* frame drew and is 0 before there has been one. Taking it from
    /// `visible_rows` left the opening frame of every drifting dataset — the one the
    /// user is looking at when nothing has been pressed yet — with no groups at all,
    /// so every absent cell fell back to the plain null glyph.
    ///
    /// Callers pass the whole frame's height, a header more than the rows it draws.
    /// Deliberately: the table reads this by row index and ignores what it does not
    /// reach, so a group too many costs a `u32` and a group too few costs a mark.
    ///
    /// Empty once a query or reshape has replaced the frame: those rows stand for no
    /// file, so their nulls are ordinary nulls. A frame is a screen tall, so this is
    /// a few dozen values.
    pub fn display_drift(&self, frame_rows: usize) -> Vec<u32> {
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
        let len = frame_rows.min(column.len().saturating_sub(offset));
        if len == 0 {
            return Vec::new();
        }
        let slice = column.slice(offset as i64, len);
        let Ok(rows) = slice.u32() else {
            return Vec::new();
        };
        // The column holds each row's place in the dataset. The file it came from is
        // the last one starting at or before it, and the file says what it is missing.
        rows.iter()
            .map(|row| self.file_group_of(row.unwrap_or(0) as usize))
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

    /// Whether Enter on a row drills into its group: the view is a grouped result and
    /// not already a group's rows.
    pub fn can_drill_down(&self) -> bool {
        !self.is_drilled_down() && self.is_grouped()
    }

    /// Whether a drill shows the lists of a row as the group's rows rather than
    /// filtering the source: a `by` result that holds its groups as lists.
    fn drills_lists(&self) -> bool {
        self.has_list_columns() && self.group_source.as_ref().is_some_and(|s| s.rows_in_lists)
    }

    /// The columns of a row that a drill into its group reads: every column of a result
    /// holding its groups as lists, the keys of one holding aggregates.
    fn drill_columns(&self) -> Vec<String> {
        if self.drills_lists() {
            return self.schema.iter_names().map(|n| n.to_string()).collect();
        }
        self.group_source
            .iter()
            .flat_map(|source| source.keys.iter().map(|(name, _)| name.to_string()))
            .collect()
    }

    /// The columns the inspector lists for a row: the table's, in its order, then
    /// the ones hidden from it, in schema order. Never the scan's own row index.
    pub fn inspect_fields(&self) -> Vec<InspectField> {
        let shown = self.column_order.iter().filter_map(|name| {
            Some(InspectField {
                name: name.clone(),
                dtype: self.schema.get(name.as_str())?.clone(),
                hidden: false,
            })
        });
        let hidden = self
            .schema
            .iter()
            .filter(|(name, _)| {
                name.as_str() != crate::schema_union::DRIFT_COLUMN
                    && !self.column_order.iter().any(|c| c == name.as_str())
            })
            .map(|(name, dtype)| InspectField {
                name: name.to_string(),
                dtype: dtype.clone(),
                hidden: true,
            });
        shown.chain(hidden).collect()
    }

    /// The selected row as the buffer holds it, with the file group that says what
    /// its nulls are. Reads nothing; `None` while the row is not on hand.
    pub fn inspect_row(&self) -> Option<InspectRow> {
        self.inspect_row_at(self.start_row + self.table_state.selected()?)
    }

    /// Row `row` of the view as the buffer holds it, as [`Self::inspect_row`] does
    /// the selected one: Compare's next row. `None` while it is not on hand.
    pub fn inspect_row_at(&self, row: usize) -> Option<InspectRow> {
        let df = self.buffered_df.as_ref()?;
        let offset = row.checked_sub(self.buffered_start_row)?;
        if offset >= df.height() {
            return None;
        }
        let names: Vec<&str> = self.column_order.iter().map(|s| s.as_str()).collect();
        let values = df.select(names).ok()?.slice(offset as i64, 1);
        let drift_group = self
            .drift_column_present
            .then(|| df.column(crate::schema_union::DRIFT_COLUMN).ok())
            .flatten()
            .and_then(|c| c.get(offset).ok())
            .and_then(|v| v.extract::<usize>())
            .map(|place| self.file_group_of(place));
        Some(InspectRow {
            row,
            frame: self.len_generation,
            display_row: row + self.row_start_index,
            values,
            drift_group,
        })
    }

    /// The drift group of the file holding the dataset's row `place`.
    fn file_group_of(&self, place: usize) -> u32 {
        let file = self
            .drift_file_starts
            .partition_point(|&start| start <= place)
            .saturating_sub(1);
        self.drift_file_group.get(file).copied().unwrap_or(0)
    }

    /// What a null in `column` is, for a row of file group `group`: the data's own,
    /// a file without the column, or a file holding it in another type.
    pub fn null_kind(&self, column: &str, group: Option<u32>) -> NullKind {
        let Some(group) = group.and_then(|g| self.drift_groups.get(g as usize)) else {
            return NullKind::Null;
        };
        if group.absent.iter().any(|c| c == column) {
            NullKind::Absent
        } else if group.unread.iter().any(|c| c == column) {
            NullKind::Conflict
        } else {
            NullKind::Null
        }
    }

    /// The frame that reads `columns` of row `row` of the view: for the inspector's
    /// fields the buffer does not hold. One row, through the same window the buffer
    /// reads, so a remote dataset reads only the file holding it. Run off this thread.
    pub fn inspect_read_lf(&self, row: usize, columns: &[String]) -> PolarsResult<LazyFrame> {
        let exprs = columns.iter().map(|c| col(c.as_str())).collect();
        self.window_lf(row, 1, exprs)
    }

    /// What drilling into the group on row `group_index` of the view reads, or `None`
    /// when the view is not grouped. The row is on screen, so the buffer holds it and
    /// nothing is computed again. A column the buffer lacks (hidden, or a binary stub)
    /// means reading the row, and one row of an aggregate is the whole aggregate, so the
    /// caller reads it off the UI thread.
    pub fn drill_row(&self, group_index: usize) -> Option<DrillRow> {
        if !self.can_drill_down() {
            return None;
        }
        let columns = self.drill_columns();
        let buffered = self
            .buffered_df
            .as_ref()
            .filter(|_| (self.buffered_start_row..self.buffered_end_row).contains(&group_index))
            .filter(|_| {
                columns
                    .iter()
                    .all(|c| !matches!(self.schema.get(c.as_str()), Some(DataType::Binary)))
            })
            .and_then(|df| df.select(columns.iter().map(|c| c.as_str())).ok())
            .map(|df| df.slice((group_index - self.buffered_start_row) as i64, 1))
            .filter(|row| row.height() == 1);
        Some(match buffered {
            Some(row) => DrillRow::Buffered(row),
            None => DrillRow::Read(Box::new(
                self.visible_lf()
                    .select(columns.iter().map(|c| col(c.as_str())).collect::<Vec<_>>())
                    .slice(group_index as i64, 1),
            )),
        })
    }

    /// Show the rows of the group on row `group_index` of the view, reading the row on
    /// this thread if the buffer does not hold it. The app goes through
    /// [`Self::drill_row`] instead, so that read never holds up a key.
    pub fn drill_down_into_group(&mut self, group_index: usize) -> Result<()> {
        let row = match self.drill_row(group_index) {
            None => return Ok(()),
            Some(DrillRow::Buffered(row)) => row,
            Some(DrillRow::Read(lf)) => collect_lazy(*lf, self.polars_streaming)?,
        };
        self.drill_down_with_row(group_index, &row)
    }

    /// Show the rows of the group whose row `group_index` of the view is `row`, as
    /// [`Self::drill_row`] gave it. A result that holds each group as lists shows those
    /// lists as rows; one that holds only aggregates shows the source rows sharing the
    /// group's keys, key columns first.
    pub fn drill_down_with_row(&mut self, group_index: usize, row: &DataFrame) -> Result<()> {
        if !self.can_drill_down() {
            return Ok(());
        }
        if row.height() == 0 {
            return Err(color_eyre::eyre::eyre!("Group index out of bounds"));
        }
        let mut group = if self.drills_lists() {
            Self::group_from_lists(
                row,
                self.group_key_columns(),
                self.group_value_columns(),
                self.lineage.clone(),
            )?
        } else if let Some(source) = &self.group_source {
            Self::group_from_source(source, row)?
        } else {
            return Ok(());
        };
        // A list form that also aggregates (`select a, n: count a by k`) holds its
        // aggregates beside the keys; the query knows which columns are keys.
        if let Some(source) = self.group_source.as_ref().filter(|_| self.drills_lists()) {
            let keys: Vec<&str> = source.keys.iter().map(|(n, _)| n.as_str()).collect();
            (group.key_columns, group.key_values) = group
                .key_columns
                .into_iter()
                .zip(group.key_values)
                .filter(|(name, _)| keys.contains(&name.as_str()))
                .unzip();
        }
        self.enter_group(group, group_index, false)
    }

    /// Show the rows of the view holding `value` in `column` (null matching nulls),
    /// as a drill into a group does: the breadcrumb names the value, and Esc comes
    /// back to the view. Inside a group already, it narrows that group, and Esc goes
    /// back to the view the group was drilled from.
    pub fn drill_into_value(&mut self, column: &str, value: AnyValue<'static>) -> Result<()> {
        let dtype = self
            .schema
            .get(column)
            .cloned()
            .ok_or_else(|| color_eyre::eyre::eyre!("no column {column}"))?;
        let label = crate::exact::str_value(&value).to_string();
        let mut steps = self.view_steps();
        steps.push(match crate::python_script::py_value(&value) {
            Some(literal) => Step::Matching(vec![(
                format!("pl.col({})", crate::python_script::py_str(column)),
                literal,
            )]),
            None => Step::Unreproducible(format!(
                "drilled down to the rows where {column} is {label}, a value of a type not written as Python"
            )),
        });
        let matches = col(column).eq_missing(lit(Scalar::new(dtype, value)));
        let group = GroupRows {
            lf: self.visible_lf().filter(matches),
            key_columns: vec![column.to_string()],
            key_values: vec![label],
            lead: vec![column.to_string()],
            steps,
            lineage: self.lineage.clone(),
        };
        if !self.is_drilled_down() {
            let index = self.start_row + self.table_state.selected().unwrap_or(0);
            return self.enter_group(group, index, true);
        }
        let schema = group.lf.clone().collect_schema()?;
        let order = std::mem::take(&mut self.column_order);
        if let Some(keys) = self.drilled_down_group_key_columns.as_mut() {
            keys.extend(group.key_columns);
        }
        if let Some(values) = self.drilled_down_group_key.as_mut() {
            values.extend(group.key_values);
        }
        // The group's filters and sort are in the frame now.
        self.filters.clear();
        self.sort_columns.clear();
        self.sort_descending.clear();
        self.sort_ascending = true;
        self.install_base(group.lf, schema);
        self.base_steps = group.steps;
        self.lineage = group.lineage;
        self.column_order = order;
        self.start_row = 0;
        self.termcol_index = 0;
        self.clear_column_moves();
        self.settle_cursor();
        self.table_state.select(Some(0));
        self.collect();
        Ok(())
    }

    /// Whether the drill on screen came from Value Counts.
    pub fn drilled_into_value(&self) -> bool {
        self.grouped.as_ref().is_some_and(|view| view.by_value)
    }

    /// Show `group`, the group on row `group_index` of the view, keeping the view to
    /// come back to.
    fn enter_group(&mut self, group: GroupRows, group_index: usize, by_value: bool) -> Result<()> {
        let schema = group.lf.clone().collect_schema()?;
        self.drilled_down_group_key = Some(group.key_values);
        self.drilled_down_group_key_columns = Some(group.key_columns);

        // The group becomes the pipeline root while drilled in, so a sidebar filter or
        // sort applies within it instead of rebuilding the grouped view underneath.
        self.grouped = Some(GroupedView {
            lf: self.lf.clone(),
            base_lf: self.base_lf.clone(),
            filters: std::mem::take(&mut self.filters),
            sort_columns: std::mem::take(&mut self.sort_columns),
            sort_descending: std::mem::take(&mut self.sort_descending),
            sort_ascending: self.sort_ascending,
            drift: self.drift_column_present,
            drift_groups: self.drift_groups.clone(),
            view_numbered: self.view_numbered,
            notes: self.notes.clone(),
            group_source: self.group_source.take(),
            column_order: self.column_order.clone(),
            locked_columns_count: self.locked_columns_count,
            start_row: self.start_row,
            termcol_index: self.termcol_index,
            cursor_column: self.cursor_column.clone(),
            selected: self.table_state.selected(),
            by_value,
            base_steps: std::mem::take(&mut self.base_steps),
            lineage: self.lineage.clone(),
        });
        self.sort_ascending = true;
        self.install_base(group.lf, schema);
        self.base_steps = group.steps;
        self.lineage = group.lineage;
        // Led by the keys, as a group drilled from lists is.
        let rest: Vec<String> = std::mem::take(&mut self.column_order)
            .into_iter()
            .filter(|c| !group.lead.contains(c))
            .collect();
        self.column_order = group.lead.into_iter().chain(rest).collect();
        self.drilled_down_group_index = Some(group_index);
        self.start_row = 0;
        self.termcol_index = 0;
        self.clear_column_moves();
        self.locked_columns_count = 0;
        self.settle_cursor();
        self.table_state.select(Some(0));
        self.collect();

        Ok(())
    }

    /// A group held as lists in `row`: the keys repeated beside the lists as columns.
    fn group_from_lists(
        row: &DataFrame,
        key_columns: Vec<String>,
        value_columns: Vec<String>,
        lineage: Lineage,
    ) -> Result<GroupRows> {
        if value_columns.is_empty() {
            return Err(color_eyre::eyre::eyre!("No value columns in grouped data"));
        }
        let row_count = match row.column(&value_columns[0])?.get(0)? {
            AnyValue::List(list_series) => list_series.len(),
            _ => 0,
        };

        let mut columns = Vec::new();
        let mut key_values = Vec::new();
        for col_name in &key_columns {
            let key = row.column(col_name)?;
            key_values.push(crate::exact::str_value(&key.get(0)?).to_string());
            // Repeated in its own type, so a date stays a date and a null key null.
            columns.push(key.new_from_index(0, row_count));
        }
        for col_name in &value_columns {
            if let AnyValue::List(list_series) = row.column(col_name)?.get(0)? {
                columns.push(list_series.with_name(col_name.as_str().into()).into());
            }
        }
        let group = key_columns
            .iter()
            .zip(&key_values)
            .map(|(c, v)| format!("{c} = {v}"))
            .collect::<Vec<_>>()
            .join(", ");
        Ok(GroupRows {
            lf: DataFrame::new_infer_height(columns)?.lazy(),
            key_columns,
            key_values,
            // Already first.
            lead: Vec::new(),
            steps: vec![Step::Unreproducible(format!(
                "drilled down into the group {group}, read from the grouped result's lists: \
                 not written as Python"
            ))],
            // The lists keep the result's names.
            lineage,
        })
    }

    /// A group of an aggregated result in `row`: the source rows whose keys equal the
    /// row's, a null key matching nulls.
    fn group_from_source(source: &GroupSource, row: &DataFrame) -> Result<GroupRows> {
        let mut predicate: Option<Expr> = None;
        let mut key_columns = Vec::new();
        let mut key_values = Vec::new();
        let mut lead = Vec::new();
        let mut matching = Vec::new();
        for (i, (name, expr)) in source.keys.iter().enumerate() {
            let column = row.column(name)?;
            let value = column.get(0)?.into_static();
            matching.push(
                source
                    .python_keys
                    .get(i)
                    .cloned()
                    .flatten()
                    .zip(crate::python_script::py_value(&value)),
            );
            key_columns.push(name.to_string());
            key_values.push(crate::exact::str_value(&value).to_string());
            // The key as the query computed it, against the value it produced; the alias
            // only named the result's column.
            let key = expr.clone().meta().undo_aliases();
            if let Expr::Column(source_column) = &key
                && !source.scratch.contains(source_column)
            {
                lead.push(source_column.to_string());
            }
            let matches = key.eq_missing(lit(Scalar::new(column.dtype().clone(), value)));
            predicate = Some(match predicate {
                Some(all) => all.and(matches),
                None => matches,
            });
        }
        let rows = source.rows.clone();
        let mut lf = match predicate {
            Some(predicate) => rows.filter(predicate),
            None => rows,
        };
        if !source.scratch.is_empty() {
            lf = lf.drop(by_name(source.scratch.iter().cloned(), true, false));
        }
        let matching: Option<Vec<(String, String)>> = matching.into_iter().collect();
        let steps = match (&source.python_rows, matching) {
            (Some(rows), Some(matching)) => {
                let mut steps = rows.clone();
                steps.push(Step::Matching(matching));
                if !source.scratch.is_empty() {
                    steps.push(Step::Drop(
                        source.scratch.iter().map(|c| c.to_string()).collect(),
                    ));
                }
                steps
            }
            _ => vec![Step::Unreproducible(format!(
                "drilled down into the group {}: not written as Python",
                key_columns
                    .iter()
                    .zip(&key_values)
                    .map(|(c, v)| format!("{c} = {v}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            ))],
        };
        Ok(GroupRows {
            lf,
            key_columns,
            key_values,
            lead,
            steps,
            lineage: source.lineage.clone(),
        })
    }

    pub fn drill_up(&mut self) -> Result<()> {
        let Some(view) = self.grouped.take() else {
            return Err(color_eyre::eyre::eyre!("Not in drill-down mode"));
        };
        let schema = Self::without_drift(view.lf.clone()).collect_schema()?;
        self.invalidate_num_rows();
        // The buffer holds the group's rows; kept, it would stand in for the grouped
        // view wherever the view fits inside it.
        self.drop_buffer();
        self.observed_bytes_per_row = None;
        self.widths.relearn();
        self.lf = view.lf;
        self.unsorted_lf = None;
        self.base_lf = view.base_lf;
        self.base_steps = view.base_steps;
        self.lineage = view.lineage;
        self.filters = view.filters;
        self.sort_columns = view.sort_columns;
        self.sort_descending = view.sort_descending;
        self.sort_ascending = view.sort_ascending;
        self.drift_column_present = view.drift;
        self.drift_groups = view.drift_groups;
        self.view_numbered = view.view_numbered;
        self.notes = view.notes;
        self.group_source = view.group_source;
        // The frame put back here already leaves out whatever its filter and sort left
        // out, so the notes saying so have to come back with it. They are derived rather
        // than saved, so they cannot go stale against a frame that changed while it was
        // drilled into.
        self.view_notes = self.view_notes_only();
        self.schema = schema;
        self.column_order = view.column_order;
        self.locked_columns_count = view.locked_columns_count;
        self.drilled_down_group_index = None;
        self.drilled_down_group_key = None;
        self.drilled_down_group_key_columns = None;
        self.start_row = view.start_row;
        self.termcol_index = view.termcol_index;
        self.clear_column_moves();
        self.cursor_column = view.cursor_column;
        self.settle_cursor();
        self.table_state.select(view.selected);
        self.collect();
        Ok(())
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

    /// Plan a pivot of the view (long → wide). Nothing is read until the job runs.
    /// Never uses `original_lf`.
    pub fn plan_pivot(&self, spec: &PivotSpec) -> PivotJob {
        PivotJob {
            view: self.visible_lf(),
            spec: spec.clone(),
            streaming: self.polars_streaming,
        }
    }

    /// Show `pivoted`, the result of `spec`'s [`PivotJob`], as the new pipeline root.
    pub fn install_pivot(&mut self, spec: &PivotSpec, pivoted: DataFrame) -> Result<()> {
        let index = if spec.index.is_empty() {
            // What the pivot itself took as the index: every other column of the view.
            self.schema
                .iter_names()
                .map(|n| n.to_string())
                .filter(|n| {
                    n != &spec.pivot_column
                        && n != &spec.value_column
                        && n != crate::schema_union::DRIFT_COLUMN
                })
                .collect()
        } else {
            spec.index.clone()
        };
        let kept = index.clone();
        let step = Step::Pivot {
            index,
            on: spec.pivot_column.clone(),
            values: spec.value_column.clone(),
            aggregation: spec.aggregation,
        };
        self.last_pivot_spec = Some(spec.clone());
        self.last_melt_spec = None;
        self.replace_lf_after_reshape(pivoted.lazy(), step, &kept)
    }

    /// Pivot the view here and now, reading it on this thread. The Pivot & Melt builder
    /// and views run the [`PivotJob`] in the background instead.
    pub fn pivot(&mut self, spec: &PivotSpec) -> Result<()> {
        let pivoted = self.plan_pivot(spec).run()?;
        self.install_pivot(spec, pivoted)
    }

    /// `view` melted by `spec`, planned only: the table's melt and the builder's
    /// preview build it the same way.
    pub(crate) fn melt_lf(view: LazyFrame, spec: &MeltSpec) -> Result<LazyFrame> {
        let on = cols(spec.value_columns.iter().map(|s| s.as_str()));
        let index = cols(spec.index.iter().map(|s| s.as_str()));
        let args = UnpivotArgsDSL {
            on: Some(on),
            index,
            variable_name: Some(PlSmallStr::from(spec.variable_name.as_str())),
            value_name: Some(PlSmallStr::from(spec.value_name.as_str())),
        };
        Ok(Self::melt_dates_as_text(view, spec, &args)?.unpivot(args))
    }

    /// Melt the current `LazyFrame` (wide → long). Never uses `original_lf`.
    pub fn melt(&mut self, spec: &MeltSpec) -> Result<()> {
        let lf = Self::melt_lf(self.visible_lf(), spec)?;
        let step = Step::Melt {
            index: spec.index.clone(),
            on: spec.value_columns.clone(),
            variable_name: spec.variable_name.clone(),
            value_name: spec.value_name.clone(),
        };
        self.last_melt_spec = Some(spec.clone());
        self.last_pivot_spec = None;
        self.replace_lf_after_reshape(lf, step, &spec.index)?;
        Ok(())
    }

    /// A melt of dates with text casts the dates to text, which panics on one past
    /// the calendar. Those columns become text first, such a date its stored
    /// number, as the rows are read.
    fn melt_dates_as_text(
        view: LazyFrame,
        spec: &MeltSpec,
        args: &UnpivotArgsDSL,
    ) -> Result<LazyFrame> {
        let schema = view.clone().collect_schema()?;
        let melted = view.clone().unpivot(args.clone()).collect_schema()?;
        if melted.get(spec.value_name.as_str()) != Some(&DataType::String) {
            return Ok(view);
        }
        let texts: Vec<Expr> = spec
            .value_columns
            .iter()
            .filter(|name| {
                schema
                    .get(name.as_str())
                    .is_some_and(crate::past_calendar::can_leave_calendar)
            })
            .map(|name| {
                crate::past_calendar::text_expr(
                    Expr::Column(PlSmallStr::from(name.as_str())),
                    polars::chunked_array::cast::CastOptions::NonStrict,
                )
            })
            .collect();
        Ok(if texts.is_empty() {
            view
        } else {
            view.with_columns(texts)
        })
    }

    /// Show `lf`, the view reshaped, as the new pipeline root. `kept` are the view's
    /// columns it carries as they were: a pivot's index, a melt's id columns.
    fn replace_lf_after_reshape(
        &mut self,
        lf: LazyFrame,
        step: Step,
        kept: &[String],
    ) -> Result<()> {
        let schema = lf.clone().collect_schema()?;
        let lineage = traced(
            &self.lineage,
            kept.iter().map(|c| (c.clone(), c.clone())).collect(),
        );
        let mut steps = self.view_steps();
        steps.push(step);
        // Taken before the view state below is reset. Over an earlier reshape there is
        // no source a view could replay, so none is kept.
        let text = |q: &str| Some(q.trim().to_string()).filter(|q| !q.is_empty());
        let source = ReshapeSource {
            query: text(&self.active_query),
            sql_query: text(&self.active_sql_query),
            fuzzy_query: text(&self.active_fuzzy_query),
            filters: self.filters.clone(),
            sort_columns: self.sort_columns.clone(),
            sort_descending: self.sort_descending.clone(),
        };
        self.reshape_source = (self.reshaped_lf.is_none() && !source.is_empty()).then_some(source);
        self.reshaped_lf = Some(lf.clone());
        self.install_base(lf, schema);
        self.base_steps = steps.clone();
        self.reshape_steps = Some(steps);
        self.lineage = lineage.clone();
        self.reshape_lineage = lineage;
        self.reset_view_state(0);
        self.error = None;
        self.df = None;
        self.locked_df = None;
        self.collect();
        Ok(())
    }

    /// The sidebar filters with their values typed against the columns they test.
    fn typed_filters(&self) -> Vec<SidebarFilter> {
        self.filters
            .iter()
            .map(|f| SidebarFilter::typed_in(f, &self.schema, &self.column_order))
            .collect()
    }

    /// What the open did to the rows its reader gave, as Python method calls.
    pub fn read_python(&self) -> &[String] {
        &self.read_python
    }

    /// See the field: the notes the read made, for the open to carry to the dataset.
    pub fn read_notes(&self) -> &[crate::notes::Note] {
        &self.read_notes
    }

    /// See the field.
    pub fn read_units(&self) -> Option<&[(String, String)]> {
        self.read_units.as_deref()
    }

    /// The read's typing: its notes go with the read's, its columns are counted later.
    fn take_typing(&mut self, mut typing: Typing) {
        self.read_notes.append(&mut typing.notes);
        self.typing = typing;
    }

    /// See [`Self::take_typing`]: what the scan hands the open, for the dataset.
    pub(crate) fn typing(&self) -> &Typing {
        &self.typing
    }

    /// What is left to count of the values the read's types made null: the frame
    /// before the types, and the columns. `None` once counted, or with nothing typed.
    pub(crate) fn unfit_to_count(&self) -> Option<(LazyFrame, Vec<crate::column_types::Typed>)> {
        if self.unfit_notes.is_some() || self.typing.typed.is_empty() {
            return None;
        }
        Some((self.typing.source.clone()?, self.typing.typed.clone()))
    }

    /// The view's column types and made columns, in the order asked.
    pub fn column_changes(&self) -> &[crate::column_types::ColumnChange] {
        &self.column_changes
    }

    /// The columns the view gave a type: the type row draws them in the accent.
    pub fn retyped_columns(&self) -> Vec<String> {
        self.column_changes
            .iter()
            .filter(|c| matches!(c.change, crate::column_types::Change::Typed(_)))
            .map(|c| c.name.clone())
            .collect()
    }

    /// `column`'s type before the view's: as the read gave it, or as the view made it.
    pub fn type_as_read(&self, column: &str) -> Option<DataType> {
        let base = self.base_lf.clone().collect_schema().ok()?;
        base.get(column)
            .or_else(|| self.schema.get(column))
            .cloned()
    }

    /// Up to `n` of `column`'s values as read that are not blank, as text: from the
    /// rows on hand, or from the first rows when the view has typed the column.
    pub fn values_on_screen(&self, column: &str, n: usize) -> Vec<String> {
        let from_buffer = self.column_type_of(column).is_none();
        let df = if from_buffer {
            self.buffered_df.clone()
        } else {
            self.base_lf
                .clone()
                .select([col(column)])
                .limit(n as IdxSize * 10)
                .collect()
                .ok()
        };
        let Some(values) = df.and_then(|df| df.column(column).ok().cloned()) else {
            return Vec::new();
        };
        let Ok(text) = values.cast(&DataType::String) else {
            return Vec::new();
        };
        let Ok(text) = text.str().cloned() else {
            return Vec::new();
        };
        text.iter()
            .flatten()
            .map(str::trim)
            .filter(|v| !v.is_empty())
            .take(n)
            .map(str::to_string)
            .collect()
    }

    /// The type the view gives `column`, if it gives one.
    pub fn column_type_of(&self, column: &str) -> Option<&crate::column_types::ColumnType> {
        self.column_changes.iter().find_map(|c| match &c.change {
            crate::column_types::Change::Typed(ty) if c.name == column => Some(ty),
            _ => None,
        })
    }

    /// `column` as `ty`, or as read again with `None`. The view's own type wins over
    /// what the read gave the column. Lazy: the next rows read are typed.
    pub fn set_column_type(&mut self, column: &str, ty: Option<crate::column_types::ColumnType>) {
        use crate::column_types::{Change, ColumnChange};
        self.column_changes
            .retain(|c| !(c.name == column && matches!(c.change, Change::Typed(_))));
        if let Some(ty) = ty {
            self.column_changes.push(ColumnChange {
                name: column.to_string(),
                change: Change::Typed(ty),
            });
        }
        self.column_changes_changed();
    }

    /// A column made from others, as a spec's derived column is, before the first
    /// column it is made from, which stays. Its name may not be taken.
    pub fn add_made_column(
        &mut self,
        derived: crate::column_types::Derived,
    ) -> std::result::Result<(), String> {
        use crate::column_types::{Change, ColumnChange};
        if self.schema.contains(&derived.name) {
            return Err(format!("a column is named {} already", derived.name));
        }
        for from in &derived.from {
            if !self.schema.contains(from) {
                return Err(format!("no column {from}"));
            }
        }
        let first = derived.from[0].clone();
        let at = self
            .column_order
            .iter()
            .position(|c| *c == first)
            .unwrap_or(self.column_order.len());
        self.column_order.insert(at, derived.name.clone());
        self.column_changes.push(ColumnChange {
            name: derived.name,
            change: Change::Made {
                from: derived.from,
                kind: derived.kind.name().to_string(),
                format: derived.format,
            },
        });
        self.column_changes_changed();
        Ok(())
    }

    /// A saved view's column changes, in place of the view's own. A change whose column
    /// this data does not have is left out, with a note; the names left out are
    /// returned.
    pub fn set_column_changes(
        &mut self,
        changes: &[crate::column_types::ColumnChange],
    ) -> Vec<String> {
        self.column_changes = Vec::new();
        let base = self
            .base_lf
            .clone()
            .collect_schema()
            .unwrap_or_else(|_| self.schema.clone());
        let mut known: Vec<String> = base.iter_names().map(|n| n.to_string()).collect();
        let mut dropped = Vec::new();
        for change in changes {
            let fits = match &change.change {
                crate::column_types::Change::Typed(_) => known.contains(&change.name),
                crate::column_types::Change::Made { from, .. } => {
                    from.iter().all(|f| known.contains(f)) && change.derived().is_some()
                }
            };
            if fits {
                if !known.contains(&change.name) {
                    known.push(change.name.clone());
                }
                self.column_changes.push(change.clone());
            } else {
                dropped.push(change.name.clone());
            }
        }
        self.changes_dropped = if dropped.is_empty() {
            Vec::new()
        } else {
            vec![crate::notes::Note {
                summary: format!(
                    "view steps left out, no such column: {}",
                    crate::notes::some_names(&dropped)
                ),
                scope: "the view's column types".to_string(),
                read_as_text: None,
                passed_over: None,
            }]
        };
        // The made columns go before their first source, as they did when made.
        for change in &self.column_changes {
            if let crate::column_types::Change::Made { from, .. } = &change.change
                && !self.column_order.contains(&change.name)
            {
                let at = self
                    .column_order
                    .iter()
                    .position(|c| *c == from[0])
                    .unwrap_or(self.column_order.len());
                self.column_order.insert(at, change.name.clone());
            }
        }
        self.column_changes_changed();
        dropped
    }

    /// Drop the view's column changes and their notes, as a new pipeline root does.
    fn forget_column_changes(&mut self) {
        if self.column_changes.is_empty() && self.changes_dropped.is_empty() {
            return;
        }
        self.column_changes.clear();
        self.changes_dropped.clear();
        self.changes_version += 1;
        self.changes_unfit = None;
    }

    /// After the column changes change: the schema shows them, a made column gone
    /// leaves the column order, and the rows are read again.
    fn column_changes_changed(&mut self) {
        self.changes_version += 1;
        let (changed, _) = self.with_column_changes(self.base_lf.clone());
        if let Ok(schema) = changed.clone().collect_schema() {
            self.schema = schema;
        }
        let schema = self.schema.clone();
        self.column_order.retain(|c| schema.contains(c));
        for name in schema.iter_names() {
            if !self.column_order.iter().any(|c| c == name.as_str()) {
                self.column_order.push(name.to_string());
            }
        }
        self.widths.relearn();
        self.drop_buffer();
        self.apply_transformations();
    }

    /// `lf` with the view's column changes, in order, and what the count of the values
    /// they made null needs: the frame with only the made columns, and the typed
    /// columns with the types they had there. A change whose column is not in `lf` is
    /// passed over.
    fn with_column_changes(
        &self,
        mut lf: LazyFrame,
    ) -> (
        LazyFrame,
        Option<(LazyFrame, Vec<crate::column_types::Typed>)>,
    ) {
        use crate::column_types::Change;
        if self.column_changes.is_empty() {
            return (lf, None);
        }
        let Ok(schema) = lf.collect_schema() else {
            return (lf, None);
        };
        let mut schema = (*schema).clone();
        let mut made = lf.clone();
        let mut typed = Vec::new();
        for change in &self.column_changes {
            let name = PlSmallStr::from(change.name.as_str());
            match &change.change {
                Change::Typed(ty) => {
                    let Some(from) = schema.get(&name).cloned() else {
                        continue;
                    };
                    lf = lf.with_column(ty.expr(&change.name, &from).alias(name.clone()));
                    typed.push(crate::column_types::Typed {
                        column: change.name.clone(),
                        ty: ty.clone(),
                        from,
                    });
                    schema.with_column(name, ty.dtype.clone());
                }
                Change::Made { from, .. } => {
                    let Some(derived) = change.derived() else {
                        continue;
                    };
                    if !from.iter().all(|f| schema.contains(f.as_str())) {
                        continue;
                    }
                    lf = lf.with_column(derived.expr().alias(name.clone()));
                    made = made.with_column(derived.expr().alias(name.clone()));
                    schema.with_column(name, DataType::Null);
                }
            }
        }
        let count = (!typed.is_empty()).then_some((made, typed));
        (lf, count)
    }

    /// What is left to count of the values the view's column types made null: the
    /// frame, the columns and the version of the changes it is for.
    pub(crate) fn changes_unfit_to_count(
        &self,
    ) -> Option<(LazyFrame, Vec<crate::column_types::Typed>, u64)> {
        if self
            .changes_unfit
            .as_ref()
            .is_some_and(|(version, _)| *version == self.changes_version)
        {
            return None;
        }
        let (_, count) = self.with_column_changes(self.base_lf.clone());
        let (source, typed) = count?;
        Some((source, typed, self.changes_version))
    }

    /// The counts for the view's column types at `version`, as notes.
    pub(crate) fn changes_unfit_counted(
        &mut self,
        version: u64,
        unfit: &[crate::column_types::Unfit],
    ) {
        if version == self.changes_version {
            // Something new to say: the `i` chip lights again.
            if !unfit.is_empty() {
                self.notes_seen = false;
            }
            self.changes_unfit = Some((
                version,
                crate::column_types::unfit_notes(unfit, "the view's column types"),
            ));
        }
    }

    /// The counts of the values the types made null, as notes.
    pub(crate) fn unfit_counted(&mut self, unfit: &[crate::column_types::Unfit]) {
        if !unfit.is_empty() {
            self.notes_seen = false;
        }
        self.unfit_notes = Some(crate::column_types::unfit_notes(
            unfit,
            "counted over every row",
        ));
    }

    /// How `lf` was built: the base's steps, then the filters and the sort.
    fn view_steps(&self) -> Vec<Step> {
        let mut steps = self.base_steps.clone();
        if !self.column_changes.is_empty() {
            let said: Vec<String> = self
                .column_changes
                .iter()
                .map(crate::column_types::ColumnChange::to_toml)
                .collect();
            steps.push(Step::Unreproducible(format!(
                "datui typed columns as a format spec would: {}",
                said.join("; ")
            )));
        }
        if !self.filters.is_empty() {
            let typed = self.typed_filters();
            let durations: Vec<String> = typed
                .iter()
                .flat_map(|f| f.unscriptable_columns())
                .collect();
            // Said before the filter: from there the script cannot keep the rows
            // datui keeps.
            if !durations.is_empty() {
                steps.push(Step::Unreproducible(format!(
                    "a kept find matches {} as datui writes durations",
                    durations.join(", ")
                )));
            }
            steps.push(Step::Filter(typed));
        }
        // Rows of files that hold a filtered or sorted column as another type: datui
        // leaves them out by where they were read, which a script cannot know.
        let left_out: Vec<String> = self
            .view_exclusions()
            .into_iter()
            .map(|(_, note)| note.summary)
            .collect();
        if !left_out.is_empty() {
            steps.push(Step::Unreproducible(left_out.join("; ")));
        }
        if !self.sort_columns.is_empty() {
            steps.push(Step::Sort {
                columns: self.sort_columns.clone(),
                descending: self.sort_descending.clone(),
            });
        } else if !self.sort_ascending {
            steps.push(Step::Reverse);
        }
        steps
    }

    /// The view as Copy as Python writes it: every step from the data as loaded to
    /// the columns shown, in their order.
    pub fn python_steps(&self) -> Vec<Step> {
        let mut steps = self.view_steps();
        let in_order = self.column_order.iter().map(String::as_str).eq(self
            .schema
            .iter_names()
            .map(|s| s.as_str())
            .filter(|s| *s != crate::schema_union::DRIFT_COLUMN));
        if !in_order {
            steps.push(Step::Select(self.column_order.clone()));
        }
        steps
    }

    pub fn is_drilled_down(&self) -> bool {
        self.drilled_down_group_index.is_some()
    }

    /// Rebuild `lf` as `base_lf` → filters → sort. Column order is applied at collect.
    /// A source that runs the filters and sort itself gives the frame instead.
    fn apply_transformations(&mut self) {
        if let Some(view) = self.pushed_view() {
            let sorted = !self.sort_columns.is_empty() || !self.sort_ascending;
            self.unsorted_lf = sorted
                .then(|| {
                    self.pushdown
                        .as_ref()
                        .and_then(|p| p.view(&self.filters, &[], false))
                        .map(|unsorted| unsorted.lf)
                })
                .flatten();
            self.view_notes = Vec::new();
            self.view_numbered = false;
            self.invalidate_num_rows();
            self.lf = self.with_column_changes(view.lf).0;
            self.restore_footer_count();
            self.collect();
            return;
        }
        let mut lf = self.with_column_changes(self.base_lf.clone()).0;
        self.view_numbered = self.row_numbers && self.wants_view_numbers();
        if self.view_numbered {
            lf = lf.with_row_index(crate::schema_union::DRIFT_COLUMN, None);
        }
        if let Some(e) = crate::python_script::filters_expr(&self.typed_filters()) {
            lf = lf.filter(e);
        }

        // Before the sort, so the rows it would have placed among the ordered ones are
        // already gone rather than ordered and then dropped.
        let (excluded, view_notes) = self.leave_out_unread_rows(lf);
        lf = excluded;
        // A new thing to say, so the quiet accent on `i` earns its place again.
        if !view_notes.is_empty() && view_notes != self.view_notes {
            self.notes_seen = false;
        }
        self.view_notes = view_notes;

        // What an analysis reads: the view before its order, which no statistic
        // depends on and every sampled read of a sorted frame would pay for.
        self.unsorted_lf =
            (!self.sort_columns.is_empty() || !self.sort_ascending).then(|| lf.clone());
        if !self.sort_columns.is_empty() {
            lf = lf.sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                sort_options(self.sort_descending.clone()),
            );
        } else if !self.sort_ascending {
            lf = lf.reverse();
        }

        self.invalidate_num_rows();
        self.lf = lf;
        self.restore_footer_count();
        self.collect();
    }

    /// Sort with one direction for every column. `ascending` also sets the natural
    /// order when `columns` is empty.
    pub fn sort(&mut self, columns: Vec<String>, ascending: bool) {
        let descending = vec![!ascending; columns.len()];
        self.sort_ascending = ascending;
        self.sort_by(columns, descending);
    }

    /// Sort with a direction per column.
    pub fn sort_by(&mut self, columns: Vec<String>, descending: Vec<bool>) {
        debug_assert_eq!(columns.len(), descending.len());
        // The one-direction flag lives on as the primary column's, for the places
        // that still speak it: views written for older readers, and `r`'s
        // natural-order fallback (which an empty sort leaves alone).
        if let Some(first) = descending.first() {
            self.sort_ascending = !first;
        }
        // Other rows come first. The sidebar sends the sort again on any apply, so
        // only a sort that changed counts.
        if columns != self.sort_columns || descending != self.sort_descending {
            self.widths.relearn();
        }
        self.sort_columns = columns;
        self.sort_descending = descending;
        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;
        self.apply_transformations();
    }

    pub fn reverse(&mut self) {
        // The order is laid on top of what is there, so what is there is the frame
        // before it — unless an order was already laid, whose own frame is kept.
        if self.unsorted_lf.is_none() {
            self.unsorted_lf = Some(self.lf.clone());
        }
        self.sort_ascending = !self.sort_ascending;
        self.widths.relearn();
        // Reversing a sorted view flips every column's direction, so `r` twice is
        // always the identity whatever mix of directions was applied.
        for direction in &mut self.sort_descending {
            *direction = !*direction;
        }

        self.buffered_start_row = 0;
        self.buffered_end_row = 0;
        self.buffered_df = None;

        // A source that runs the order runs it backward too.
        if self.pushed_view().is_some() {
            self.apply_transformations();
            return;
        }
        if !self.sort_columns.is_empty() {
            self.invalidate_num_rows();
            self.lf = self.lf.clone().sort_by_exprs(
                self.sort_columns.iter().map(col).collect::<Vec<_>>(),
                sort_options(self.sort_descending.clone()),
            );
            self.collect();
        } else {
            self.invalidate_num_rows();
            self.lf = self.lf.clone().reverse();
            self.collect();
        }
    }

    pub fn filter(&mut self, filters: Vec<FilterStatement>) {
        // The sidebar sends the filters again on any apply; only a change is new rows.
        if filters != self.filters {
            self.widths.relearn();
        }
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

        let source_schema = self.query_source().collect_schema().ok();
        let parsed = parse_query_over(&query, source_schema.as_deref())
            .map(|parsed| parsed.past_calendar_safe(source_schema.as_deref()));
        match parsed {
            Ok(ParsedQuery {
                cols,
                filter,
                group_by: group_by_cols,
                group_by_names: group_by_col_names,
                distinct,
            }) => {
                let mut lf = self.query_source();
                let mut schema_opt: Option<Arc<Schema>> = None;
                // The query runs over the data as loaded: a column selected as it is, or
                // renamed, is still a loaded one; a computed one is not.
                let lineage = if cols.is_empty() && group_by_cols.is_empty() {
                    None
                } else {
                    let mut kept = passed_through(&group_by_cols);
                    if cols.is_empty() {
                        // Every other column, as each group's list of its values.
                        kept.extend(
                            source_schema
                                .iter()
                                .flat_map(|schema| schema.iter_names())
                                .filter(|n| !group_by_col_names.iter().any(|g| g == n.as_str()))
                                .map(|n| (n.to_string(), n.to_string())),
                        );
                    } else {
                        kept.extend(passed_through(&cols));
                    }
                    Some(Arc::new(kept))
                };

                // Apply filter first (where clause)
                if let Some(f) = filter {
                    lf = lf.filter(f);
                }
                // What a drill-down into one of the groups shows.
                let group_rows = lf.clone();

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
                    let options = sort_options(vec![false; sort_exprs.len()]);
                    lf = lf.sort_by_exprs(sort_exprs, options);
                } else if !cols.is_empty() {
                    lf = lf.select(cols);
                }
                if distinct {
                    // Stable, so a grouped result keeps its sorted order.
                    lf = lf.unique_stable(None, UniqueKeepStrategy::First);
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
                // The keys lead the result in `by` order, whatever they were named.
                let keys: Vec<(PlSmallStr, Expr)> =
                    schema.iter_names().cloned().zip(group_by_cols).collect();
                // Python's division depends on the types the query read.
                let input = source_schema.unwrap_or_default();
                let steps = vec![Step::Query {
                    query: query.clone(),
                    input: input.clone(),
                    keys: keys.iter().map(|(name, _)| name.to_string()).collect(),
                }];
                // The same keys as Python, for a drill into one of the groups.
                let python_keys: Vec<Option<String>> = match crate::query::parse_nodes(&query) {
                    Ok(mut nodes) => {
                        nodes.resolve_division(&input);
                        nodes
                            .group_by
                            .iter()
                            .map(|key| Some(key.without_aliases().python()))
                            .collect()
                    }
                    Err(_) => vec![None; keys.len()],
                };
                let python_rows = Some(vec![Step::QueryRows {
                    query: query.clone(),
                    input,
                }]);
                self.install_query_result(lf, schema, ActiveQuery::Dsl(query), locked, steps);
                self.lineage = lineage;
                if !keys.is_empty() {
                    self.group_source = Some(GroupSource {
                        rows: group_rows,
                        keys,
                        scratch: Vec::new(),
                        rows_in_lists: true,
                        python_rows,
                        python_keys,
                        // The data as loaded, filtered.
                        lineage: None,
                    });
                }
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

    /// Which loaded column each column of [`Self::query_root`] is.
    #[cfg(feature = "sql")]
    fn root_lineage(&self) -> Lineage {
        if self.grouped.is_some() {
            self.lineage.clone()
        } else if self.reshaped_lf.is_some() {
            self.reshape_lineage.clone()
        } else {
            None
        }
    }

    /// How [`Self::query_root`] was built, as Copy as Python steps.
    #[cfg(feature = "sql")]
    fn query_root_steps(&self) -> Vec<Step> {
        if self.grouped.is_some() {
            return self.base_steps.clone();
        }
        match (&self.reshaped_lf, &self.reshape_steps) {
            (None, _) => Vec::new(),
            (Some(_), Some(steps)) => steps.clone(),
            (Some(_), None) => vec![Step::Unreproducible(
                "datui reshaped the data in a way it cannot write as Python".to_string(),
            )],
        }
    }

    /// Execute a SQL query against `query_root` (registered as table "df"): the drilled
    /// group or the reshaped data when one is in effect, otherwise the data as loaded —
    /// never the sidebar filters or a previous SQL result. Running a query starts a
    /// fresh view: `install_query_result` clears the sidebar filters and sort, and they
    /// are applied after it. Empty SQL resets to original state. Does not call
    /// collect(); the event loop does that via AppEvent::Collect.
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
            let root = self.query_root();
            let root_steps = self.query_root_steps();
            ctx.register("df", root.clone());
            match ctx.execute(trimmed) {
                Ok(mut result_lf) => {
                    // First, so the schema and the group source read the plan that
                    // runs. It changes expressions in place and only adds a projection
                    // over a union's inputs: the nodes stable_order orders and the
                    // filter count_subquery_values_once rewrites keep their shape.
                    crate::past_calendar::guard_plan(&mut result_lf.logical_plan);
                    // Read before datui orders the plan stably or by group keys: the
                    // marks say what the statement asked for.
                    let order = ordered_by(&result_lf.logical_plan);
                    let mut schema = match result_lf.clone().collect_schema() {
                        Ok(s) => s,
                        Err(e) => {
                            self.error = Some(e);
                            return;
                        }
                    };
                    let leftover =
                        leftover_subquery_value_columns(&mut result_lf.logical_plan, &schema);
                    if !leftover.is_empty() {
                        let shown = Arc::make_mut(&mut schema);
                        for name in &leftover {
                            shown.shift_remove(name);
                        }
                        result_lf = result_lf.drop(Selector::ByName {
                            names: leftover.into(),
                            strict: true,
                        });
                    }
                    let root_lineage = self.root_lineage();
                    let lineage = {
                        let columns = root.clone().collect_schema().unwrap_or_default();
                        let names: Vec<&str> = columns.iter_names().map(|n| n.as_str()).collect();
                        let shown: Vec<&str> = schema.iter_names().map(|n| n.as_str()).collect();
                        traced(
                            &root_lineage,
                            crate::sql_group::passed_through(trimmed, &names, &shown),
                        )
                    };
                    let group_source = Self::sql_group_source(
                        &mut ctx,
                        trimmed,
                        root,
                        &root_steps,
                        &mut result_lf,
                        &schema,
                        root_lineage,
                    );
                    // Groups sorted by their keys have no ties, and a statement simple
                    // enough to trace holds nothing else that gives rows in any order.
                    // Keeping the groups' order too would double the grouping's time.
                    if !group_source.as_ref().is_some_and(|(_, by_keys)| *by_keys) {
                        stable_order(&mut result_lf.logical_plan);
                    }
                    count_subquery_values_once(&mut result_lf.logical_plan);
                    let ordered_by = match &group_source {
                        Some((source, true)) => schema
                            .iter_names()
                            .filter(|name| source.keys.iter().any(|(key, _)| key == *name))
                            .map(|name| name.to_string())
                            .collect(),
                        _ => Vec::new(),
                    };
                    let mut steps = root_steps;
                    steps.push(Step::Sql {
                        sql: trimmed.to_string(),
                        ordered_by,
                    });
                    let query_order = order
                        .into_iter()
                        .take_while(|(name, _)| schema.contains(name))
                        .collect();
                    self.install_query_result(result_lf, schema, ActiveQuery::Sql(sql), 0, steps);
                    self.query_order = query_order;
                    self.lineage = lineage;
                    self.install_sql_group_source(group_source.map(|(source, _)| source));
                }
                Err(e) => {
                    self.error = Some(e);
                }
            }
        }

        #[cfg(not(feature = "sql"))]
        {
            self.error = Some(PolarsError::ComputeError(
                "SQL is not supported in this build. Rebuild with default features.".into(),
            ));
        }
    }

    /// What a SQL `GROUP BY` result was grouped from, when the statement is a grouping
    /// of `df` simple enough to trace (see [`crate::sql_group`]). Plans without reading.
    /// A grouping with no ORDER BY or LIMIT has its rows sorted by key, as a `by`
    /// query's are: Polars returns groups in any order, and every read of the result
    /// (each page, the row count, coming back from a drill) would otherwise be free to
    /// shuffle them. Also says whether it sorted them.
    #[cfg(feature = "sql")]
    fn sql_group_source(
        ctx: &mut polars_sql::SQLContext,
        sql: &str,
        root: LazyFrame,
        root_steps: &[Step],
        result_lf: &mut LazyFrame,
        result: &Schema,
        lineage: Lineage,
    ) -> Option<(GroupSource, bool)> {
        use crate::sql_group::KeySource;
        let columns = root.clone().collect_schema().ok()?;
        let names: Vec<&str> = columns.iter_names().map(|n| n.as_str()).collect();
        let plan = crate::sql_group::plan(sql, &names, result.len())?;
        let mut rows = ctx.execute(&plan.source_sql).ok()?;
        crate::past_calendar::guard_plan(&mut rows.logical_plan);
        let source_schema = rows.clone().collect_schema().ok()?;
        let mut scratch = Vec::new();
        let mut keys = Vec::with_capacity(plan.keys.len());
        let mut python_keys = Vec::with_capacity(plan.keys.len());
        for key in plan.keys {
            let (name, dtype) = result.get_at_index(key.result_index)?;
            let column = match key.source {
                KeySource::Column(c) => PlSmallStr::from(c),
                KeySource::Computed(c) => {
                    let c = PlSmallStr::from(c);
                    scratch.push(c.clone());
                    c
                }
            };
            // A key the grouping changed the type of would never compare equal.
            if source_schema.get(&column) != Some(dtype) {
                return None;
            }
            python_keys.push(Some(format!(
                "pl.col({})",
                crate::python_script::py_str(&column)
            )));
            keys.push((name.clone(), col(column)));
        }
        if !plan.ordered {
            // In the order they are shown.
            let by: Vec<Expr> = result
                .iter_names()
                .filter(|name| keys.iter().any(|(key, _)| key == *name))
                .map(|name| col(name.clone()))
                .collect();
            let options = sort_options(vec![false; by.len()]);
            *result_lf = result_lf.clone().sort_by_exprs(by, options);
        }
        let mut python_rows = root_steps.to_vec();
        python_rows.push(Step::Sql {
            sql: plan.source_sql.clone(),
            ordered_by: Vec::new(),
        });
        let source = GroupSource {
            rows,
            keys,
            scratch,
            rows_in_lists: false,
            python_rows: Some(python_rows),
            python_keys,
            // `SELECT *` of the root, the scratch keys left out of a drill.
            lineage,
        };
        Some((source, !plan.ordered))
    }

    /// Record a SQL result's group source and freeze the keys that lead it, as a `by`
    /// query's are.
    #[cfg(feature = "sql")]
    fn install_sql_group_source(&mut self, source: Option<GroupSource>) {
        let Some(source) = source else {
            return;
        };
        self.locked_columns_count = self
            .schema
            .iter_names()
            .take_while(|c| source.keys.iter().any(|(k, _)| k == *c))
            .count();
        self.group_source = Some(source);
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
                "A text match needs at least one text column".into(),
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
        let steps = vec![Step::Search {
            patterns: tokens.iter().map(|t| fuzzy_token_regex(t)).collect(),
            columns: string_cols.clone(),
        }];
        self.install_query_result(lf, schema, ActiveQuery::Fuzzy(query), 0, steps);
        // Rows of the data as loaded, every column as it is.
        self.lineage = None;
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
    /// stub (see [`binary_stub`]) and are styled with `binary_col` + italic.
    pub binary_cols: std::collections::HashSet<String>,
    /// Display-time number formatting (digit grouping, separators, alignment).
    pub number_format: NumberFormatSettings,
    /// Draw a second header row naming each column's type.
    pub dtype_row: bool,
    /// Tint under the row the cursor is on. `None` falls back to reversed video.
    pub selected_bg: Option<Color>,
    /// The full selected-row style, from the theme's `highlight_style` helper.
    pub selection_style: Style,
    /// The rail beside the selected row and the off-screen column hints.
    pub accent: Color,
    /// Null cells and the type row.
    pub dimmed: Color,
    /// Per row on screen, its file's drift group. Empty when the dataset's files agree,
    /// or when the rows no longer stand for rows of a file.
    pub drift_rows: Vec<u32>,
    /// What each drift group is missing. Indexed by the values in `drift_rows`.
    pub drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    /// Columns the view is sorted by; each carries a direction mark in the header.
    /// Filled from the state at render, so the marks always describe the frame drawn.
    pub sort_columns: Vec<String>,
    /// Which way each of them runs, per column, as it is applied.
    pub sort_descending: Vec<bool>,
    /// The column cursor's column: its header and cells are tinted.
    pub current_column: Option<String>,
    /// The column cursor's cells, from the theme's `column_cursor_style` helper.
    pub column_cursor_style: Style,
    /// The column cursor's header and the current cell, from the theme's
    /// `cell_cursor_style` helper.
    pub cell_cursor_style: Style,
    /// The glyph set the table draws with: the terminal's, unless a test asks for one.
    pub glyphs: &'static crate::glyphs::Glyphs,
    /// The terminal's width, which bounds automatic text widths (see
    /// [`crate::widgets::column_widths::text_cap`]). 0 takes the table's own width.
    pub screen_width: u16,
    /// The cell a find landed on: its view row and column.
    pub find_cell: Option<(usize, String)>,
    /// How that cell is drawn, from the theme's `find_match_style`.
    pub find_style: Style,
    /// The found cell's column, while the cursor is on its row: set at render.
    find_column: Option<String>,
    /// The cells a find being typed matches, by view row and column, drawn as
    /// found.
    pub match_cells: Option<std::sync::Arc<crate::find::MatchCells>>,
    /// The view row the first row drawn is: set at render.
    drawn_from: usize,
    /// Each column's unit from a delimited spec's unit row, for the type row: set at
    /// render.
    units: Vec<(String, String)>,
    /// The columns the view gave a type: their type row is in the accent.
    retyped: Vec<String>,
}

impl Default for DataTable {
    fn default() -> Self {
        Self {
            header_bg: Color::Reset,
            header_fg: Color::Reset,
            row_numbers_fg: Color::Reset,
            separator_fg: Color::Reset,
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
            selection_style: Style::default(),
            accent: Color::Reset,
            dimmed: Color::Reset,
            drift_rows: Vec::new(),
            drift_groups: Arc::new(Vec::new()),
            sort_columns: Vec::new(),
            sort_descending: Vec::new(),
            current_column: None,
            column_cursor_style: Style::default(),
            cell_cursor_style: Style::default(),
            glyphs: crate::glyphs::get(),
            screen_width: 0,
            find_cell: None,
            find_style: Style::default(),
            find_column: None,
            match_cells: None,
            drawn_from: 0,
            units: Vec::new(),
            retyped: Vec::new(),
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

pub use crate::column_types::dtype_label;

/// The columns a read gave a type, the frame before it did, and its notes.
#[derive(Clone, Default)]
pub struct Typing {
    pub(crate) source: Option<LazyFrame>,
    pub(crate) typed: Vec<crate::column_types::Typed>,
    pub(crate) notes: Vec<crate::notes::Note>,
    /// The columns the scan read as text, by the names it read them under, for Copy
    /// as Python's `schema_overrides`.
    pub(crate) text: Vec<String>,
}

impl std::fmt::Debug for Typing {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Typing")
            .field("typed", &self.typed)
            .field("notes", &self.notes)
            .finish_non_exhaustive()
    }
}

/// Parameters for rendering the row numbers column.
struct RowNumbersParams {
    start_row: usize,
    visible_rows: usize,
    num_rows: usize,
    /// The number each row on screen shows: see [`DataTableState::row_numbers_from`].
    numbers: Vec<usize>,
    selected_row: Option<usize>,
}

/// Placeholder shown in the table for binary columns. Their values (often large blobs, e.g.
/// raw document bytes) are never read into the display buffer — only this stub is — which keeps
/// scrolling and jump-to-end fast. The real bytes remain in `lf` for export/analysis.
///
/// The text comes from the active glyph set (`binary_stub`), so ASCII terminals get a
/// readable `<binary>` instead of mojibake.
pub(crate) fn binary_stub() -> &'static str {
    crate::glyphs::get().binary_stub
}

/// One column of the rows on screen, formatted once: what the layout measures and
/// what the table draws.
struct ColumnSlice {
    name: String,
    drift_mark: &'static str,
    sort_mark: &'static str,
    /// Cells for the name and both marks.
    header_width: u16,
    type_label: Option<String>,
    type_width: u16,
    cells: Vec<SliceCell>,
    /// Cells for the widest value on screen.
    value_width: u16,
    /// Whether any row on screen holds a value rather than a null.
    has_values: bool,
    /// The width the column is laid out at: its stable width once sized (see
    /// [`ColumnWidths`]), until then what shows this page whole.
    width: u16,
    right_align: bool,
    /// Whether a value may be shown clipped beside other columns (see
    /// [`is_truncatable_dtype`]).
    clips: bool,
    cell_style: Option<Style>,
    /// The column's type colour, for its heading.
    colour: Option<Color>,
}

impl ColumnSlice {
    /// The width the column asks for: whole within it, or clipped behind the marker.
    fn natural_width(&self) -> u16 {
        self.width
    }

    fn measure(&self) -> PageMeasure {
        PageMeasure {
            header: self.header_width,
            type_label: self.type_width,
            values: self.value_width,
            has_values: self.has_values,
            clips: self.clips,
        }
    }
}

/// What sizes columns for one draw: the stable widths, the types that key them, and
/// the cap on automatic text.
struct Sizing<'a> {
    widths: &'a mut ColumnWidths,
    schema: &'a Schema,
    cap: u16,
}

enum SliceCell {
    /// A null, drawn as the glyph for its kind of empty.
    Null(&'static str),
    Value(String),
}

/// The most sideways moves held for a draw: a held key typed faster than frames
/// can land, bounded so the draw that lands them stays a frame's work.
const MAX_WAITING_MOVES: usize = 32;

/// A sideways move waiting on a draw: the view's own, or the column cursor's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WaitingMove {
    View(ColumnMove),
    Cursor(CursorMove),
}

/// Where the scrolling columns are, for the off-screen hints over the header.
struct ScrollCue {
    area: Rect,
    more_left: bool,
    /// Columns not drawn at all, right of the last drawn.
    more_right: usize,
}

/// Columns laid out for one side of the table: the columns that fit, the width each
/// gets, and the rows they are drawn for.
struct FittedColumns {
    cols: Vec<ColumnSlice>,
    widths: Vec<u16>,
    rows: usize,
    /// The last column ends at the table's right edge with more columns after it:
    /// its heading leaves its last cell to the off-screen hint, which would
    /// otherwise cover the heading's last character or clip marker.
    hint_cell: bool,
}

/// The least the scrolling side keeps beside frozen columns, and the most: a third of
/// the table in between, and never over half. Enough for any number or timestamp whole,
/// and for the start of a text column.
const MIN_SCROLL_RESERVE: u16 = 12;
const MAX_SCROLL_RESERVE: u16 = 40;

/// Narrower than this, a column is not drawn as a clipped sliver: the clip marker plus
/// at least one cell of what it marks.
fn min_partial_width(g: &crate::glyphs::Glyphs) -> u16 {
    let marker = u16::try_from(crate::glyphs::cell_width(g.ellipsis)).unwrap_or(u16::MAX);
    marker.saturating_add(1).max(3)
}

/// Which side of the frozen separator a layout is for. Both follow one sizing rule;
/// they differ in what they do with a column that does not fit whole.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    /// Only the first column may be clipped, and never as a numeric preview: any other
    /// column that does not fit whole scrolls instead, where it can be read whole.
    Frozen,
    /// The last column shown may be clipped, and a first column with no room for its
    /// values shows a marked preview rather than nothing.
    Scrolling,
}

/// The width a column gets with `remaining` cells left, or `None` to leave it for the
/// next scroll. The heading and the values are fitted separately: a long heading is
/// clipped over values that fit whole, and never hides them. Text may be clipped, since
/// a clipped string still reads as its start. A number or timestamp that does not fit
/// is left for the scroll, as a cut one reads as a different value, unless it is the
/// first scrolling column and nothing else would show: then it is drawn clipped,
/// behind the clip marker.
fn fit_column(
    col: &ColumnSlice,
    remaining: u16,
    min_partial: u16,
    first: bool,
    side: Side,
) -> Option<u16> {
    if col.natural_width() <= remaining {
        return Some(col.natural_width());
    }
    if side == Side::Frozen && !first {
        return None;
    }
    if remaining >= min_partial && (col.value_width <= remaining || col.clips) {
        return Some(remaining);
    }
    (side == Side::Scrolling && first && remaining > 0).then_some(remaining)
}

/// Fitted `spans` as one line of a `width`-cell column, flush right when `right`.
/// ratatui places a span by its whole string's width, which can differ from the cells
/// it draws (`لا` draws two, a halfwidth sound mark one): its right alignment then
/// pushes the last grapheme off the cell, and a span after such a one overwrites it.
/// So the padding is counted in drawn cells, and spans that disagree are drawn as one.
fn cell_line(mut spans: Vec<Span<'static>>, width: u16, right: bool) -> Line<'static> {
    use unicode_width::UnicodeWidthStr;
    let drawn = |s: &Span| crate::glyphs::cell_width(&s.content);
    if spans.len() > 1 && spans.iter().any(|s| drawn(s) != s.content.width()) {
        let style = spans[0].style;
        let joined: String = spans.iter().map(|s| s.content.as_ref()).collect();
        spans = vec![Span::styled(joined, style)];
    }
    let used: usize = spans.iter().map(drawn).sum();
    let pad = usize::from(width).saturating_sub(used);
    if right && pad > 0 {
        spans.insert(0, Span::raw(" ".repeat(pad)));
    }
    Line::from(spans)
}

/// The rows of `df` on screen: `len` of them from `offset`, or `None` past its end.
/// As [`visible_slice`], or the frame's columns with no rows when none of its rows is
/// on screen, so a table of no rows still draws its header.
fn visible_or_header(df: &DataFrame, offset: usize, len: usize) -> Option<DataFrame> {
    if df.width() == 0 {
        return None;
    }
    Some(visible_slice(df, offset, len).unwrap_or_else(|| df.clear()))
}

fn visible_slice(df: &DataFrame, offset: usize, len: usize) -> Option<DataFrame> {
    let len = len.min(df.height().saturating_sub(offset));
    (offset < df.height() && len > 0).then(|| df.slice(offset as i64, len))
}

/// Whether a column whose value doesn't fully fit may be shown truncated. Textual columns
/// (strings, raw bytes, categorical/enum labels) and nested previews (structs, lists,
/// arrays) are fine to clip: a partial value still reads as a clipped string, and a
/// nested value left whole would hold one long value's width on every page after it.
/// Numeric, temporal and boolean columns are excluded: a truncated number or
/// timestamp reads as a different (wrong) value, so those are dropped until scrolled into view.
fn is_truncatable_dtype(dtype: &DataType) -> bool {
    match dtype {
        DataType::String | DataType::Binary => true,
        other => other.is_categorical() || other.is_enum() || other.is_nested(),
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

    /// The terminal's width, so automatic widths do not change when a sidebar opens.
    pub fn with_screen_width(mut self, width: u16) -> Self {
        self.screen_width = width;
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

    /// The columns the view is sorted by, and which way each runs, for the header
    /// marks. The stateful render fills this from the state itself; the builder is
    /// for direct callers of `render_dataframe`, such as tests.
    pub fn with_sort(mut self, columns: Vec<String>, descending: Vec<bool>) -> Self {
        debug_assert_eq!(columns.len(), descending.len());
        self.sort_columns = columns;
        self.sort_descending = descending;
        self
    }

    /// The direction mark after a column's name when the view is sorted by it. Every
    /// column of a multi-sort carries one — the mark alone, no position number — and
    /// each shows its own column's direction. Empty for unsorted columns.
    fn sort_mark_for(&self, column: &str) -> &'static str {
        if let Some(i) = self.sort_columns.iter().position(|c| c == column) {
            let g = self.glyphs;
            if self.sort_descending.get(i).copied().unwrap_or(false) {
                g.sort_desc
            } else {
                g.sort_asc
            }
        } else {
            ""
        }
    }

    /// The footnote mark after a column's name, when it is not in every file or the
    /// files disagree on its type. Empty otherwise.
    fn drift_mark_for(&self, column: &str, drifting: &HashSet<&str>) -> &'static str {
        if drifting.contains(column) {
            self.glyphs.drift_mark
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

    /// The selected row's style and tint, the rail colour, and the dim colour
    /// for nulls. The style comes from the theme's `highlight_style` helper,
    /// so this widget never invents a fallback of its own.
    pub fn with_selection_colors(
        mut self,
        selection_style: Style,
        selected_bg: Option<Color>,
        accent: Color,
        dimmed: Color,
    ) -> Self {
        self.selection_style = selection_style;
        self.selected_bg = selected_bg;
        self.accent = accent;
        self.dimmed = dimmed;
        self
    }

    /// Mark the cell a find landed on, drawn in `style` in place of the current cell's
    /// style while the cursor is on it.
    /// Draw these cells (view row, column) as found: a find's matches as it is typed.
    pub fn with_match_cells(
        mut self,
        cells: Option<std::sync::Arc<crate::find::MatchCells>>,
    ) -> Self {
        self.match_cells = cells;
        self
    }

    pub fn with_find_cell(mut self, cell: Option<(usize, String)>, style: Style) -> Self {
        self.find_cell = cell;
        self.find_style = style;
        self
    }

    /// The column cursor's styles, from the theme's helpers: its cells, and its
    /// header and the current cell.
    pub fn with_cursor_styles(mut self, column: Style, cell: Style) -> Self {
        self.column_cursor_style = column;
        self.cell_cursor_style = cell;
        self
    }

    /// How many rows the header takes: the names, plus the type row when it is on.
    pub fn header_height(&self) -> u16 {
        if self.dtype_row { 2 } else { 1 }
    }

    /// Style of the highlighted row, from the theme's `highlight_style` helper.
    fn highlight_style(&self) -> Style {
        self.selection_style
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

    /// Render `df` into `area` on its own, with widths learned from this page alone,
    /// returning how many columns were shown. For tests of the layout rules.
    ///
    /// `leading_gap` keeps the first column one cell off the left edge: the columns right
    /// of the frozen separator, which would otherwise touch it.
    #[cfg(test)]
    fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        leading_gap: bool,
        _start_row_offset: usize,
    ) -> usize {
        let mut widths = ColumnWidths::default();
        let sizing = Sizing {
            widths: &mut widths,
            schema: df.schema(),
            cap: self.text_cap(area.width),
        };
        self.render_scrolling(df, area, buf, state, leading_gap, sizing)
            .0
    }

    /// The cap on automatic text widths, from the terminal's width when known.
    fn text_cap(&self, table_width: u16) -> u16 {
        let basis = if self.screen_width > 0 {
            self.screen_width
        } else {
            table_width
        };
        crate::widgets::column_widths::text_cap(basis)
    }

    /// Lay out and draw the scrolling columns, at their stable widths. Returns how
    /// many were drawn.
    fn render_scrolling(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        leading_gap: bool,
        mut sizing: Sizing,
    ) -> (usize, Vec<(u16, u16, String)>, usize) {
        let rows = df
            .height()
            .min((area.height as usize).saturating_sub(self.header_height() as usize));
        let lead = u16::from(leading_gap);
        let mut fitted = self.fit_columns(df, rows, area.width, lead, Side::Scrolling, &mut sizing);
        let shown = fitted.cols.len();
        let mut used = fitted
            .widths
            .iter()
            .fold(lead, |used, &w| used.saturating_add(w))
            + self
                .table_cell_padding
                .saturating_mul(u16::try_from(shown.saturating_sub(1)).unwrap_or(u16::MAX));
        if let Some(filled) =
            self.fill_last_column(&mut fitted, area.width.saturating_sub(used), &mut sizing)
        {
            used = used.saturating_add(filled);
        }
        fitted.hint_cell = shown > 0 && shown < df.width() && used >= area.width;
        let (columns, rows) = self.draw_columns(&fitted, area, buf, state, leading_gap);
        (shown, columns, rows)
    }

    /// Widen the last column drawn by the `room` left at the table's right edge, so a
    /// long text on the far right runs to the edge rather than stopping at its cap.
    /// Only a column drawn whole at an automatic width, and not a right-aligned number,
    /// which would only move away from its heading. Returns the cells it took.
    fn fill_last_column(
        &self,
        fitted: &mut FittedColumns,
        room: u16,
        sizing: &mut Sizing,
    ) -> Option<u16> {
        let (col, width) = fitted.cols.last().zip(fitted.widths.last_mut())?;
        if room == 0 || col.right_align || *width < col.natural_width() {
            return None;
        }
        let dtype = sizing.schema.get(col.name.as_str())?;
        if sizing.widths.choice(&col.name, dtype) != WidthChoice::Auto {
            return None;
        }
        *width = width.saturating_add(room);
        sizing.widths.fill(&col.name, dtype, *width);
        Some(room)
    }

    /// The frozen columns that fit beside a usable scrolling column, with their widths.
    ///
    /// `width` is the room right of the row numbers. Whenever anything scrolls, the
    /// scrolling side keeps a third of the table (within bounds), so a wide frozen prefix
    /// can never leave it a sliver; the frozen columns that do not fit in the rest are
    /// the caller's to hand to the scrolling side, where each can be read whole. Only
    /// the first frozen column is ever clipped to stay frozen, and never a number.
    fn fit_frozen_columns(
        &self,
        locked: &DataFrame,
        rows: usize,
        width: u16,
        nothing_else_scrolls: bool,
        table_width: u16,
        sizing: &mut Sizing,
    ) -> FittedColumns {
        // The space before the separator, and the separator. The gap after it is the
        // scrolling side's.
        let room = width.saturating_sub(2);
        if nothing_else_scrolls {
            let fitted = self.fit_columns(locked, rows, room, 0, Side::Frozen, sizing);
            if fitted.cols.len() == locked.width() {
                return fitted;
            }
        }
        let reserve = (table_width / 3)
            .clamp(MIN_SCROLL_RESERVE, MAX_SCROLL_RESERVE)
            .min(table_width / 2);
        self.fit_columns(
            locked,
            rows,
            room.saturating_sub(reserve),
            0,
            Side::Frozen,
            sizing,
        )
    }

    /// Lay `df`'s columns out left to right in `width` cells, `lead` of them taken first,
    /// formatting a column's first `rows` values only once it is reached. The one sizing
    /// rule for frozen and scrolling columns alike: each column at its stable width,
    /// learned from the rows on screen in terminal cells the first time it is drawn.
    fn fit_columns(
        &self,
        df: &DataFrame,
        rows: usize,
        width: u16,
        lead: u16,
        side: Side,
        sizing: &mut Sizing,
    ) -> FittedColumns {
        let drifting = self.drifting_columns();
        let min_partial = min_partial_width(self.glyphs);
        // Reused across every cell so formatting allocates only the string each cell keeps.
        let mut scratch = String::new();
        let mut fitted = FittedColumns {
            cols: Vec::new(),
            widths: Vec::new(),
            rows,
            hint_cell: false,
        };
        let mut used = lead;
        for col_index in 0..df.width() {
            let remaining = width.saturating_sub(used);
            if remaining == 0 {
                break;
            }
            let mut col = self.slice_column(df, col_index, rows, &drifting, &mut scratch);
            let dtype = sizing
                .schema
                .get(col.name.as_str())
                .unwrap_or_else(|| df[col_index].dtype());
            col.width = sizing
                .widths
                .width(&col.name, dtype, col.measure(), sizing.cap);
            let first = fitted.cols.is_empty();
            let Some(w) = fit_column(&col, remaining, min_partial, first, side) else {
                break;
            };
            let whole = w >= col.natural_width();
            used = used
                .saturating_add(w)
                .saturating_add(self.table_cell_padding);
            fitted.cols.push(col);
            fitted.widths.push(w);
            if !whole {
                // A clipped column took everything left; nothing after it can fit.
                break;
            }
        }
        fitted
    }

    /// One column's heading, type and first `rows` values, formatted and measured.
    fn slice_column(
        &self,
        df: &DataFrame,
        col_index: usize,
        rows: usize,
        drifting: &HashSet<&str>,
        scratch: &mut String,
    ) -> ColumnSlice {
        let g = self.glyphs;
        let col_data = &df[col_index];
        let name = col_data.name().as_str();
        let dtype = col_data.dtype();
        // Binary columns hold the `‹binary›` stub: style them with binary_col + italic so they
        // read as a placeholder rather than data, regardless of the column_colors setting.
        let is_binary = self.binary_cols.contains(name);
        let cell_style = if is_binary {
            let mut s = Style::default().add_modifier(Modifier::ITALIC);
            if let Some(c) = self.binary_col {
                s = s.fg(c);
            }
            Some(s)
        } else {
            self.column_type_color(dtype)
                .map(|c| Style::default().fg(c))
        };
        // Resolved once per column: dtype eligibility and the include/exclude globs never
        // touch the per-cell path. A binary column holds the stub, not a number, so it is
        // always passthrough.
        let col_fmt = if is_binary {
            CellFormatter::Passthrough
        } else {
            self.number_format.formatter_for(name, dtype)
        };
        // Numeric columns render flush-right so magnitudes line up; strings, booleans,
        // temporals and binary stubs stay left.
        let right_align = self.number_format.align_numeric_right
            && !is_binary
            && numfmt::is_right_aligned_dtype(dtype);
        // A null in this column means different things in different files: the data's
        // own null, a file written without the column, or a file that stores it in
        // another type. Resolved once per column, by group.
        let null_glyph_by_group = self.null_glyphs_for(name, g, drifting);

        let mut cells = Vec::with_capacity(rows);
        let mut value_width = 0usize;
        for row_index in 0..rows.min(col_data.len()) {
            let value = col_data.get(row_index).unwrap();
            if matches!(value, AnyValue::Null) {
                let glyph = self
                    .drift_rows
                    .get(row_index)
                    .and_then(|group| null_glyph_by_group.get(*group as usize))
                    .copied()
                    .unwrap_or(g.null);
                value_width = value_width.max(crate::glyphs::cell_width(glyph));
                cells.push(SliceCell::Null(glyph));
                continue;
            }
            // A list is previewed here, for the cells on screen only: the buffer keeps
            // it a list, as formatting a whole row group's lists stalled every scroll.
            let text = match &value {
                AnyValue::List(items) => Cow::Owned(crate::exact::list_preview(items)),
                value => numfmt::format_any_value(&col_fmt, value, scratch),
            };
            // A break or a tab would vanish from a cell and run the text together.
            // Only a cell's start can be drawn: measuring a huge value whole would
            // cost every frame what the value costs.
            let text = crate::exact::cell_preview(&text, g);
            value_width = value_width.max(crate::glyphs::cell_width(&text));
            cells.push(SliceCell::Value(text));
        }

        let drift_mark = self.drift_mark_for(name, drifting);
        let sort_mark = self.sort_mark_for(name);
        // Both header marks widen the column, or a sorted or drifting column's last
        // character would be pushed out of its cell.
        let header_width = crate::glyphs::cell_width(name)
            + crate::glyphs::cell_width(drift_mark)
            + crate::glyphs::cell_width(sort_mark);
        // The type row is part of the header, so a column is at least as wide as its
        // type name; "datetime" under a column called "ts" would otherwise clip.
        // A binary column's buffer holds the stub text; the type is the source's.
        // A unit from a delimited spec's unit row sits beside the type: `f64 · deg F`.
        let type_label = self.dtype_row.then(|| {
            let label = if is_binary {
                dtype_label(&DataType::Binary)
            } else {
                dtype_label(dtype)
            };
            match self.units.iter().find(|(column, _)| column == name) {
                Some((_, unit)) => format!("{label} {} {unit}", self.glyphs.middot),
                None => label,
            }
        });
        let type_width = type_label
            .as_deref()
            .map(crate::glyphs::cell_width)
            .unwrap_or(0);
        let cells_u16 = |w: usize| u16::try_from(w).unwrap_or(u16::MAX);
        let has_values = cells.iter().any(|c| matches!(c, SliceCell::Value(_)));
        ColumnSlice {
            name: name.to_string(),
            drift_mark,
            sort_mark,
            header_width: cells_u16(header_width),
            type_label,
            type_width: cells_u16(type_width),
            cells,
            value_width: cells_u16(value_width),
            has_values,
            width: cells_u16(header_width.max(type_width).max(value_width)),
            right_align,
            clips: is_binary || is_truncatable_dtype(dtype),
            cell_style,
            colour: if is_binary {
                self.binary_col
            } else {
                self.column_type_color(dtype)
            },
        }
    }

    /// Draw fitted columns into `area` as a table. Every heading, type and value is
    /// fitted to its column here, at a grapheme boundary and marked where cut, so
    /// ratatui never truncates one itself: it cuts a right-aligned value from the left,
    /// which turns `1234567` into `34567`.
    fn draw_columns(
        &self,
        fitted: &FittedColumns,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
        leading_gap: bool,
    ) -> (Vec<(u16, u16, String)>, usize) {
        let g = self.glyphs;
        let fit = |text: &str, width: u16| -> String {
            crate::glyphs::fit_cells(text, usize::from(width), g.ellipsis).into_owned()
        };
        // A null is drawn as a glyph in the dim colour, so it can never be mistaken
        // for an empty string or a zero that happens to be blank.
        let null_style = Style::default()
            .fg(self.dimmed)
            .add_modifier(Modifier::ITALIC);
        let columns = || fitted.cols.iter().zip(fitted.widths.iter().copied());

        let rows: Vec<Row> = (0..fitted.rows)
            .map(|row_index| {
                let cells: Vec<Cell> = columns()
                    .map(|(col, w)| {
                        let span = match col.cells.get(row_index) {
                            Some(SliceCell::Null(glyph)) => Span::styled(fit(glyph, w), null_style),
                            Some(SliceCell::Value(text)) => {
                                let mut style = col.cell_style.unwrap_or_default();
                                if self.match_cells.as_ref().is_some_and(|cells| {
                                    cells.get(col.name.as_str()).is_some_and(|rows| {
                                        rows.contains(&(self.drawn_from + row_index))
                                    })
                                }) {
                                    style = style.patch(self.find_style);
                                }
                                Span::styled(fit(text, w), style)
                            }
                            None => return Cell::default(),
                        };
                        Cell::from(cell_line(vec![span], w, col.right_align))
                    })
                    .collect();
                let row_style = if row_index % 2 == 1 {
                    self.alternate_row_bg
                        .map(|c| Style::default().bg(c))
                        .unwrap_or_default()
                } else {
                    Style::default()
                };
                Row::new(cells).style(row_style)
            })
            .collect();

        let header_row_style = if self.header_bg == Color::Reset {
            Style::default().fg(self.header_fg)
        } else {
            Style::default().bg(self.header_bg).fg(self.header_fg)
        };
        // The name takes the column's own colour, bold, so the header says what the
        // cells say without a mark in front of it; the type row beneath repeats the
        // colour in plain weight and spells the type out. Headings follow their
        // column's alignment; a left-aligned heading over right-aligned digits reads
        // as a rendering bug.
        let last = fitted.cols.len().saturating_sub(1);
        // The column cursor, when its column is among these.
        let cursor = self
            .current_column
            .as_deref()
            .and_then(|name| fitted.cols.iter().position(|c| c.name == name));
        // The current cell is drawn as found while a find's cell is the cursor's: a find
        // moves the cursor to the cell it lands on.
        let cell_style = if cursor.is_some() && self.find_column == self.current_column {
            self.find_style
        } else {
            self.cell_cursor_style
        };
        // Under a reversed row (`table_selected = "reversed"`), a reversed cell would
        // read as the rest of the row: the current cell is the one drawn upright.
        let cell_style = if self
            .highlight_style()
            .add_modifier
            .contains(Modifier::REVERSED)
        {
            cell_style.remove_modifier(Modifier::REVERSED)
        } else {
            cell_style
        };
        let headers: Vec<Cell> = columns()
            .enumerate()
            .map(|(i, (col, w))| {
                // The off-screen hint goes on the type row when there is one, else on
                // the name row: that line of the last column stops a cell short.
                let hint = u16::from(fitted.hint_cell && i == last && w > 1);
                let (name_w, type_w) = if col.type_label.is_some() {
                    (w, w - hint)
                } else {
                    (w - hint, w)
                };
                let name_style = match col.colour {
                    Some(c) => Style::default().fg(c).add_modifier(Modifier::BOLD),
                    None => Style::default().add_modifier(Modifier::BOLD),
                };
                // The marks are state, so a long name gives way to them: the name is
                // what gets clipped, never the sort direction or the drift footnote.
                let marks = crate::glyphs::cell_width(col.drift_mark)
                    + crate::glyphs::cell_width(col.sort_mark);
                let mut heading = Vec::with_capacity(3);
                match u16::try_from(marks).ok().filter(|&m| m < name_w) {
                    Some(marks) => {
                        heading.push(Span::styled(fit(&col.name, name_w - marks), name_style));
                        if !col.drift_mark.is_empty() {
                            heading.push(Span::styled(
                                col.drift_mark,
                                Style::default().fg(self.dimmed),
                            ));
                        }
                        // In the name's own style: the mark says how this column's
                        // values run, so it reads as part of the heading.
                        if !col.sort_mark.is_empty() {
                            heading.push(Span::styled(col.sort_mark, name_style));
                        }
                    }
                    None => heading.push(Span::styled(fit(&col.name, name_w), name_style)),
                }
                let mut lines = vec![cell_line(heading, name_w, col.right_align)];
                if let Some(label) = &col.type_label {
                    let type_style = match col.colour {
                        // A type the view gave, not the read: it shows.
                        _ if self.retyped.contains(&col.name) => Style::default().fg(self.accent),
                        Some(c) => Style::default().fg(c),
                        None => Style::default().fg(self.dimmed),
                    };
                    let label = Span::styled(fit(label, type_w), type_style);
                    lines.push(cell_line(vec![label], type_w, col.right_align));
                }
                let cell = Cell::from(Text::from(lines));
                if cursor == Some(i) {
                    cell.style(self.cell_cursor_style)
                } else {
                    cell
                }
            })
            .collect();

        let mut table = Table::new(rows, fitted.widths.clone())
            .column_spacing(self.table_cell_padding)
            .header(
                Row::new(headers)
                    .style(header_row_style)
                    .height(self.header_height()),
            )
            .row_highlight_style(self.highlight_style())
            .column_highlight_style(self.column_cursor_style)
            .cell_highlight_style(cell_style);
        if leading_gap {
            // A blank selection column on every row: the Table offsets the header and
            // the cells past it and paints each row's tint across it, so the gap
            // stripes and highlights like the rest of the row.
            table = table
                .highlight_symbol(" ")
                .highlight_spacing(HighlightSpacing::Always);
        }
        // The frozen and scrolling sides share the row selection; the column is each
        // side's own, so it is set for this draw only.
        state.select_column(cursor);
        StatefulWidget::render(table, area, buf, state);
        state.select_column(None);
        // Where the Table put each column: past the gap's selection column, laid out
        // as it lays them out, so a click finds the column it drew.
        let lead = u16::from(leading_gap).min(area.width);
        let columns_area = Rect {
            x: area.x + lead,
            width: area.width - lead,
            ..area
        };
        let spans = ratatui::layout::Layout::horizontal(
            fitted
                .widths
                .iter()
                .map(|&w| ratatui::layout::Constraint::Length(w)),
        )
        .flex(ratatui::layout::Flex::Start)
        .spacing(self.table_cell_padding)
        .split(columns_area);
        let columns = spans
            .iter()
            .zip(&fitted.cols)
            .map(|(span, col)| (span.x, span.right(), col.name.clone()))
            .collect();
        (
            columns,
            fitted.rows.min(usize::from(
                area.height.saturating_sub(self.header_height()),
            )),
        )
    }

    /// The width a scrolling column is drawn at: the width it was last drawn at in
    /// this view, or, for one not drawn since, measured from the rows on screen in the
    /// buffer held and learned as drawing it would learn it. What a sideways page is
    /// planned with.
    fn measure_column(
        &self,
        state: &mut DataTableState,
        name: &str,
        offset: usize,
        rows: usize,
        cap: u16,
    ) -> u16 {
        if let Some(width) = state.drawn_width(name) {
            return width;
        }
        let Some(page) = state.page_column(name, offset, rows) else {
            return crate::widgets::column_widths::UNSEEN_WIDTH;
        };
        let col = self.slice_column(
            &page,
            0,
            page.height(),
            &self.drifting_columns(),
            &mut String::new(),
        );
        let dtype = state.width_dtype(name);
        state.widths.width(name, &dtype, col.measure(), cap)
    }

    /// Fit each column waiting for it to the rows on screen, from the buffer already
    /// held, so a column scrolled out of view is fitted to this page too.
    fn fit_pending(&self, state: &mut DataTableState, offset: usize, rows: usize, cap: u16) {
        let pending = state.widths.fits_pending();
        if pending.is_empty() || !state.buffer_on_hand() {
            return;
        }
        let drifting = self.drifting_columns();
        let mut scratch = String::new();
        for (name, dtype) in pending {
            let Some(page) = state.page_column(&name, offset, rows) else {
                continue;
            };
            let col = self.slice_column(&page, 0, page.height(), &drifting, &mut scratch);
            state.widths.fit(&name, &dtype, col.measure(), cap);
        }
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

        let number = |row_idx: usize| params.numbers.get(row_idx).copied().unwrap_or_default();
        // Calculate width needed for largest row number
        let max_row_num = (0..rows_to_render).map(number).max().unwrap_or_default();
        let max_width = max_row_num.to_string().len();

        // Render row numbers
        for row_idx in 0..rows_to_render.min(area.height.saturating_sub(header_h) as usize) {
            let row_num_text = number(row_idx).to_string();

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

/// The table as the last frame drew it: what a click on it lands on.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DrawnTable {
    /// The whole table, rail and header included.
    area: Rect,
    header: u16,
    /// The first row drawn and how many under the header.
    start_row: usize,
    rows: usize,
    columns: DrawnColumns,
}

/// Each column drawn: its cells across, `[from, to)`, and its name.
pub type DrawnColumns = Vec<(u16, u16, String)>;

/// What a click on the table lands on: the row on screen, counted from the top (none
/// on the header), and the column (none on the rail or the row numbers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellHit {
    pub row: Option<usize>,
    pub column: Option<String>,
}

impl DataTableState {
    /// Forget where the table was drawn: a frame that does not draw it leaves nothing
    /// there to click.
    pub fn forget_drawn(&mut self) {
        self.drawn = None;
    }

    /// What the cell at `(x, y)` showed in the last frame. `None` off the table, or
    /// below its last row.
    pub fn drawn_cell(&self, x: u16, y: u16) -> Option<CellHit> {
        let drawn = self.drawn.as_ref()?;
        if !drawn.area.contains(ratatui::layout::Position { x, y }) {
            return None;
        }
        let below_header = usize::from(y - drawn.area.y).checked_sub(usize::from(drawn.header));
        let row = match below_header {
            Some(row) if row >= drawn.rows => return None,
            row => row,
        };
        let column = drawn
            .columns
            .iter()
            .find(|(from, to, _)| (*from..*to).contains(&x))
            .map(|(_, _, name)| name.clone());
        Some(CellHit { row, column })
    }

    /// The column whose right edge the header cell at `(x, y)` is: the first cell
    /// of the gap after a column's last, where a drag resizes it.
    pub fn drawn_edge(&self, x: u16, y: u16) -> Option<String> {
        let drawn = self.drawn.as_ref()?;
        let header = drawn.area.y..drawn.area.y + drawn.header;
        if !header.contains(&y) || !drawn.area.contains(ratatui::layout::Position { x, y }) {
            return None;
        }
        if let Some(name) = Self::right_edge(drawn, x) {
            return Some(name);
        }
        if drawn
            .columns
            .iter()
            .any(|(from, to, _)| (*from..*to).contains(&x))
        {
            return None;
        }
        drawn
            .columns
            .iter()
            .find(|(_, to, _)| *to == x)
            .map(|(_, _, name)| name.clone())
    }

    /// The edge of a column that reaches the table's right side, filled or cut
    /// there: no gap follows it, so its last header cell is its edge.
    fn right_edge(drawn: &DrawnTable, x: u16) -> Option<String> {
        let (_, to, name) = drawn.columns.iter().max_by_key(|(_, to, _)| *to)?;
        (*to >= drawn.area.right() && x + 1 == *to).then(|| name.clone())
    }

    /// The column drawn across `x`, whatever the row: where a header dragged sideways
    /// is over.
    pub fn drawn_column_across(&self, x: u16) -> Option<String> {
        let drawn = self.drawn.as_ref()?;
        drawn
            .columns
            .iter()
            .find(|(from, to, _)| (*from..*to).contains(&x))
            .map(|(_, _, name)| name.clone())
    }

    /// The header rows as drawn and each column's cells across, `[from, to)`: where a
    /// header drag draws its drop mark.
    pub fn drawn_header(&self) -> Option<(Rect, DrawnColumns)> {
        let drawn = self.drawn.as_ref()?;
        Some((
            Rect {
                height: drawn.header.min(drawn.area.height),
                ..drawn.area
            },
            drawn.columns.clone(),
        ))
    }

    /// Put the cursor on what a click landed on: the row, when the rows drawn are
    /// still the view's, and the column. Returns whether it is there now, row and
    /// column both.
    pub fn point_at(&mut self, hit: &CellHit) -> bool {
        let mut landed = true;
        if let Some(row) = hit.row {
            if let Some(drawn) = self.drawn.as_ref()
                && drawn.start_row == self.start_row
                && row < drawn.rows
            {
                self.table_state.select(Some(row));
            } else {
                landed = false;
            }
        }
        if let Some(name) = &hit.column {
            self.set_current_column(name);
            landed &= self.current_column() == Some(name.as_str());
        }
        landed
    }
}

impl StatefulWidget for DataTable {
    type State = DataTableState;

    fn render(mut self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        // The view's own sort, not the grouped original's: it is what ordered the
        // rows being drawn, so the header marks can never disagree with them.
        (self.sort_columns, self.sort_descending) = state.header_sort();
        self.current_column = state.current_column().map(str::to_string);
        self.units = state.units();
        self.retyped = state.retyped_columns();
        // One column on the left is the rail: blank on every row but the one the
        // cursor is on, where it carries the accent. It also holds the "columns off to
        // the left" hint in the header, so no header name ever gets a character
        // overwritten.
        let cap = self.text_cap(area.width);
        let whole = area;
        state.drawn = None;
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

        // Fewer rows (the footer grew a line): the page starts that much later, so the
        // row the cursor is on stays the row it is on.
        if let Some(selected) = state.table_state.selected()
            && selected >= state.visible_rows
            && state.visible_rows > 0
        {
            let overflow = selected - (state.visible_rows - 1);
            state.start_row += overflow;
            state.table_state.select(Some(state.visible_rows - 1));
        }

        // Only a page the rows on hand do not cover needs a read: the footer growing
        // and shrinking a line must not re-read the buffer each time.
        if visible_rows_changed && !state.page_on_hand(state.start_row) {
            // The App event loop checks this flag after each render and triggers an
            // async collect.
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

        let start_row = state.start_to_draw();
        self.drawn_from = start_row;
        state.on_screen = None;
        // Only on the cursor's row (and, when drawn, its column): the cursor is what a
        // find moves, and a mark left behind would read as a second match.
        let selected = state.table_state.selected();
        self.find_column = self.find_cell.take().and_then(|(row, name)| {
            (row.checked_sub(start_row) == selected && selected.is_some()).then_some(name)
        });

        // Where the scrolling columns are, for the cue drawn over the header after them.
        let mut scroll_indicator: Option<ScrollCue> = None;

        // The numbers `#` shows, and the column wide enough for the widest.
        let numbers = if state.row_numbers {
            state.row_numbers_from(start_row, state.visible_rows)
        } else {
            Vec::new()
        };
        let row_num_width = if state.row_numbers {
            let widest = numbers.iter().max().copied().unwrap_or(1);
            widest.to_string().len().max(1) as u16 + 1 // +1 for spacing
        } else {
            0
        };
        let row_num_width = row_num_width.min(area.width);
        let data_area = Rect {
            x: area.x + row_num_width,
            width: area.width - row_num_width,
            ..area
        };
        let row_num_area = Rect {
            width: row_num_width,
            ..area
        };
        let visible_rows = state.visible_rows;
        let row_numbers = |start_row, num_rows, numbers, selected_row| RowNumbersParams {
            start_row,
            visible_rows,
            num_rows,
            numbers,
            selected_row,
        };
        let row_number_params = row_numbers(
            start_row,
            state.num_rows,
            numbers,
            state.table_state.selected(),
        );

        // Both sides are cut to the same rows on screen, so the frozen columns are
        // measured on what they show, not on the head of the buffer.
        let offset = start_row.saturating_sub(state.buffered_start_row);
        let rows_room = (area.height as usize).saturating_sub(header_h as usize);
        let locked_slice = state
            .locked_df
            .as_ref()
            .and_then(|df| visible_or_header(df, offset, state.visible_rows));
        self.fit_pending(state, offset, state.visible_rows.min(rows_room), cap);

        if state.df.is_some() || state.locked_df.is_some() {
            if state.row_numbers {
                self.render_row_numbers(row_num_area, buf, row_number_params);
            }
            let mut drawn_columns = Vec::new();
            let mut drawn_rows = 0;
            let mut scroll_area = data_area;
            let mut leading_gap = false;
            if let Some(locked) = locked_slice {
                let asked = state.locked_columns_count();
                // The rule runs down the header and the rows on screen, and stops
                // under the last: below it is no table to divide.
                let rule_bottom = (area.y + header_h)
                    .saturating_add(locked.height().min(rows_room) as u16)
                    .min(area.bottom());
                let mut fitted = self.fit_frozen_columns(
                    &locked,
                    locked.height().min(rows_room),
                    data_area.width,
                    state.column_order.len() <= asked,
                    area.width,
                    &mut Sizing {
                        widths: &mut state.widths,
                        schema: &state.schema,
                        cap,
                    },
                );
                state.fit_frozen(fitted.cols.len());
                // The state may not take the fit while no buffer is on hand; it then
                // keeps its count, and only what fits of it is drawn.
                let shown = state.frozen_shown().min(fitted.cols.len());
                fitted.cols.truncate(shown);
                fitted.widths.truncate(shown);
                let mut separator_x = data_area.x;
                if shown > 0 {
                    let gaps = self.table_cell_padding.saturating_mul(shown as u16 - 1);
                    let columns_width = fitted.widths.iter().sum::<u16>().saturating_add(gaps);
                    // One cell more than the columns: the space before the separator,
                    // which takes the header fill and the row tints.
                    let frozen_area = Rect {
                        width: columns_width.saturating_add(1).min(data_area.width),
                        ..data_area
                    };
                    let (columns, rows) =
                        self.draw_columns(&fitted, frozen_area, buf, &mut state.table_state, false);
                    drawn_columns.extend(columns);
                    drawn_rows = drawn_rows.max(rows);
                    separator_x = frozen_area.right();
                }
                if separator_x < data_area.right() {
                    // A broken rule while some frozen columns had to scroll: the window
                    // holds fewer than were asked for, and they come back with room.
                    let rule = if shown < asked {
                        self.glyphs.rule_broken
                    } else {
                        self.glyphs.rule
                    };
                    for y in area.y..rule_bottom {
                        let cell = &mut buf[(separator_x, y)];
                        cell.set_symbol(rule);
                        cell.set_style(Style::default().fg(self.separator_fg));
                    }
                }
                let scroll_x = separator_x.saturating_add(1).min(data_area.right());
                scroll_area = Rect {
                    x: scroll_x,
                    width: data_area.right() - scroll_x,
                    ..data_area
                };
                leading_gap = true;
            }
            // A page asked for lands here, where the room it is planned in is known.
            let room = Room {
                width: scroll_area.width,
                lead: u16::from(leading_gap),
                padding: self.table_cell_padding,
            };
            let rows = state.visible_rows.min(rows_room);
            state.land_column_moves(room, |state, name| {
                self.measure_column(state, name, offset, rows, cap)
            });
            if let Some(sliced_df) = state
                .df
                .as_ref()
                .and_then(|df| visible_or_header(df, offset, state.visible_rows))
            {
                let total_cols = sliced_df.width();
                let (shown, columns, rows) = self.render_scrolling(
                    &sliced_df,
                    scroll_area,
                    buf,
                    &mut state.table_state,
                    leading_gap,
                    Sizing {
                        widths: &mut state.widths,
                        schema: &state.schema,
                        cap,
                    },
                );
                drawn_columns.extend(columns);
                drawn_rows = drawn_rows.max(rows);
                let more_left = state.termcol_index > 0;
                let more_right = total_cols.saturating_sub(shown);
                let first = state.frozen_shown() + state.termcol_index + 1;
                let total = state.column_order.len();
                state.on_screen =
                    state
                        .cursor_index()
                        .filter(|_| total > 1)
                        .map(|cursor| OnScreen {
                            first,
                            last: first + shown.saturating_sub(1),
                            cursor: cursor + 1,
                            total,
                        });
                scroll_indicator = Some(ScrollCue {
                    area: scroll_area,
                    more_left,
                    more_right,
                });
            } else {
                // Every column frozen: all on screen, and the cursor walks them.
                let total = state.column_order.len();
                state.on_screen =
                    state
                        .cursor_index()
                        .filter(|_| total > 1)
                        .map(|cursor| OnScreen {
                            first: 1,
                            last: total,
                            cursor: cursor + 1,
                            total,
                        });
            }
            state.drawn = Some(DrawnTable {
                area: whole,
                header: header_h,
                start_row,
                rows: drawn_rows,
                columns: drawn_columns,
            });
        } else if !state.column_order.is_empty() {
            // No rows on hand, but a schema: the header alone, each column its own type.
            let empty_columns: Vec<_> = state
                .column_order
                .iter()
                .map(|name| {
                    let dtype = state
                        .schema
                        .get(name.as_str())
                        .cloned()
                        .unwrap_or(DataType::String);
                    Series::new_empty(name.as_str().into(), &dtype).into()
                })
                .collect();
            match DataFrame::new_infer_height(empty_columns) {
                Ok(empty_df) => {
                    if state.row_numbers {
                        self.render_row_numbers(
                            row_num_area,
                            buf,
                            row_numbers(0, 0, Vec::new(), None),
                        );
                    }
                    self.render_scrolling(
                        &empty_df,
                        data_area,
                        buf,
                        &mut state.table_state,
                        false,
                        Sizing {
                            widths: &mut state.widths,
                            schema: &state.schema,
                            cap,
                        },
                    );
                }
                _ => {
                    Paragraph::new("No data").render(area, buf);
                }
            }
        } else {
            // Truly empty: no schema, not loaded, or blank file
            Paragraph::new("No data").render(area, buf);
        }

        // A table known to hold no rows says so under its header.
        let empty = state.num_rows_valid && state.num_rows == 0 && !state.column_order.is_empty();
        if empty && area.height > header_h && data_area.width > 0 {
            let line = Rect {
                y: area.y + header_h,
                height: 1,
                ..data_area
            };
            Paragraph::new("No rows")
                .style(Style::default().fg(self.dimmed))
                .render(line, buf);
        }

        // The rail: the header rows take the header fill so the bar runs edge to edge,
        // and the selected row gets the accent mark.
        if rail_area.width > 0 && rail_area.height > 0 {
            let g = self.glyphs;
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
                && !empty
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
        if let Some(cue) = scroll_indicator
            && cue.area.width > 0
            && cue.area.height > 0
        {
            let g = self.glyphs;
            let scroll_area = cue.area;
            let hidden = cue.more_right;
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
            if cue.more_left && rail_area.width > 0 {
                let cell = &mut buf[(rail_area.x, rail_area.y)];
                cell.set_symbol(g.arrow_left);
                cell.set_style(hint_style);
            }
            if hidden > 0 {
                let y = if header_h > 1 {
                    scroll_area.y + 1
                } else {
                    scroll_area.y
                };
                // The count when the blank run at the end of the row holds it, the arrow
                // alone when not, so the count never covers a heading or a type.
                let right = scroll_area.x + scroll_area.width;
                let free = (scroll_area.x..right)
                    .rev()
                    .take_while(|&x| buf[(x, y)].symbol() == " ")
                    .count();
                let mut text = format!(" +{hidden} {}", g.arrow_right);
                if text.chars().count() > free {
                    text = g.arrow_right.to_string();
                }
                let w = text.chars().count() as u16;
                if scroll_area.width >= w {
                    let x0 = right - w;
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

/// Everything a checkpoint promises to put back, as a test can compare it: the rows
/// each frame of the pipeline reads, the unsorted one analyses read among them
/// (collected here, on the test's thread), the schema,
/// the query, filters, sort and layout, the reshape, the drill, what the notes say, the
/// selection, and the count and buffer the view holds.
#[cfg(test)]
#[derive(Debug, PartialEq)]
pub(crate) struct ViewSnapshot {
    rows: std::result::Result<DataFrame, String>,
    analysis_rows: std::result::Result<DataFrame, String>,
    base_rows: std::result::Result<DataFrame, String>,
    reshaped_rows: Option<std::result::Result<DataFrame, String>>,
    schema: Arc<Schema>,
    queries: [String; 3],
    filters: String,
    sort: (Vec<String>, Vec<bool>, bool),
    layout: (Vec<String>, usize),
    reshape: String,
    grouped: (bool, bool),
    drill: (Option<usize>, Option<Vec<String>>, Option<Vec<String>>),
    drift: (bool, Arc<Vec<crate::schema_union::DriftGroup>>),
    notes: (Vec<crate::notes::Note>, bool, Vec<crate::notes::Note>),
    selection: (Option<usize>, usize, usize),
    count: (usize, bool, u64),
    buffer: (usize, usize, Option<DataFrame>),
    shown: Option<DataFrame>,
    error: Option<String>,
}

#[cfg(test)]
impl ViewSnapshot {
    /// Whether the view's rows and count had been read.
    pub(crate) fn has_rows(&self) -> bool {
        self.buffer.2.is_some() && self.count.1
    }
}

#[cfg(test)]
impl DataTableState {
    pub(crate) fn snapshot(&self) -> ViewSnapshot {
        let rows = |lf: &LazyFrame| lf.clone().collect().map_err(|e| e.to_string());
        ViewSnapshot {
            rows: rows(&self.lf),
            analysis_rows: rows(&self.analysis_lf()),
            base_rows: rows(&self.base_lf),
            reshaped_rows: self.reshaped_lf.as_ref().map(rows),
            schema: self.schema.clone(),
            queries: [
                self.active_query.clone(),
                self.active_sql_query.clone(),
                self.active_fuzzy_query.clone(),
            ],
            filters: format!("{:?}", self.filters),
            sort: (
                self.sort_columns.clone(),
                self.sort_descending.clone(),
                self.sort_ascending,
            ),
            layout: (self.column_order.clone(), self.locked_columns_count),
            reshape: format!(
                "{:?} {:?} {:?}",
                self.last_pivot_spec, self.last_melt_spec, self.reshape_source
            ),
            grouped: (self.grouped.is_some(), self.group_source.is_some()),
            drill: (
                self.drilled_down_group_index,
                self.drilled_down_group_key.clone(),
                self.drilled_down_group_key_columns.clone(),
            ),
            drift: (self.drift_column_present, self.drift_groups.clone()),
            notes: (self.notes.clone(), self.notes_seen, self.view_notes.clone()),
            selection: (
                self.table_state.selected(),
                self.start_row,
                self.termcol_index,
            ),
            count: (self.num_rows, self.num_rows_valid, self.len_generation),
            buffer: (
                self.buffered_start_row,
                self.buffered_end_row,
                self.buffered_df.clone(),
            ),
            shown: self.df.clone(),
            error: self.error.as_ref().map(|e| e.to_string()),
        }
    }
}

#[cfg(test)]
mod checkpoint_tests {
    use super::*;
    use crate::filter_modal::{FilterOperator, LogicalOperator};

    fn state() -> DataTableState {
        let lf = df!(
            "id" => (0..20i64).collect::<Vec<_>>(),
            "key" => (0..20).map(|i| if i % 2 == 0 { "a" } else { "b" }).collect::<Vec<_>>(),
            "val" => (0..20i64).map(|i| i * 10).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::from_lazyframe(lf, &crate::OpenOptions::default()).unwrap();
        state.visible_rows = 4;
        state.collect();
        state
    }

    fn filter(column: &str, op: FilterOperator, value: &str) -> FilterStatement {
        FilterStatement {
            columns: Vec::new(),
            column: column.to_string(),
            operator: op,
            value: value.to_string(),
            logical_op: LogicalOperator::And,
        }
    }

    /// A view with something at every stage: a query, a filter, a sort, a layout, a
    /// selection off the top, and its rows and count read.
    fn busy_view() -> DataTableState {
        let mut state = state();
        state.query("select id, val, key where val >= 20".to_string());
        state.filter(vec![filter("val", FilterOperator::Lt, "170")]);
        state.sort_by(vec!["val".to_string()], vec![true]);
        state.set_column_order(vec!["val".to_string(), "id".to_string(), "key".to_string()]);
        state.set_locked_columns(1);
        state.scroll_to(3);
        state.collect();
        state.table_state.select(Some(2));
        assert!(state.error().is_none(), "{:?}", state.error());
        assert!(state.is_num_rows_valid());
        assert!(state.display_df().is_some());
        state
    }

    /// The same view over a melt, so a reshape is what a failure must leave in place.
    fn melted_view() -> DataTableState {
        let mut state = state();
        state
            .melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["val".to_string()],
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .unwrap();
        state.filter(vec![filter("id", FilterOperator::Gt, "3")]);
        state.collect();
        state.table_state.select(Some(1));
        assert!(state.error().is_none(), "{:?}", state.error());
        state
    }

    /// Fails as a view's step does: by the error the step leaves, or at the plan.
    fn planned(state: &mut DataTableState) -> std::result::Result<(), String> {
        if let Some(e) = state.error() {
            return Err(e.to_string());
        }
        state.check_plan().map_err(|e| e.to_string())
    }

    /// Each step a view replays, ending in one that fails: every one puts back the
    /// whole view, and none of them reads a row on the way.
    #[test]
    fn a_transition_failing_after_each_step_puts_the_view_back() {
        type Steps = fn(&mut DataTableState) -> std::result::Result<(), String>;
        let cases: [(&str, Steps); 8] = [
            ("the query", |s| {
                s.query("select nope".to_string());
                planned(s)
            }),
            ("a filter after the query", |s| {
                s.query("select id, val".to_string());
                s.filter(vec![filter("key", FilterOperator::Eq, "a")]);
                planned(s)
            }),
            ("a sort after the filter", |s| {
                s.query("select id, val".to_string());
                s.filter(vec![filter("val", FilterOperator::Gt, "0")]);
                s.sort_by(vec!["key".to_string()], vec![false]);
                planned(s)
            }),
            ("a melt after the query", |s| {
                s.query("select id, val".to_string());
                s.melt(&MeltSpec {
                    index: vec!["id".to_string()],
                    value_columns: vec!["nope".to_string()],
                    variable_name: "variable".to_string(),
                    value_name: "value".to_string(),
                })
                .map_err(|e| e.to_string())?;
                planned(s)
            }),
            ("a filter after the melt", |s| {
                s.melt(&MeltSpec {
                    index: vec!["id".to_string()],
                    value_columns: vec!["val".to_string()],
                    variable_name: "variable".to_string(),
                    value_name: "value".to_string(),
                })
                .map_err(|e| e.to_string())?;
                s.filter(vec![filter("val", FilterOperator::Gt, "0")]);
                planned(s)
            }),
            ("a sort after the melt", |s| {
                s.melt(&MeltSpec {
                    index: vec!["id".to_string()],
                    value_columns: vec!["val".to_string()],
                    variable_name: "variable".to_string(),
                    value_name: "value".to_string(),
                })
                .map_err(|e| e.to_string())?;
                s.sort_by(vec!["key".to_string()], vec![false]);
                planned(s)
            }),
            ("the layout after the sort", |s| {
                s.query("select id, val".to_string());
                s.sort_by(vec!["val".to_string()], vec![false]);
                s.set_column_order(vec!["key".to_string()]);
                planned(s)
            }),
            ("a reset, then a query", |s| {
                s.reset();
                s.query("select id where nope > 1".to_string());
                planned(s)
            }),
        ];
        for prior in [busy_view as fn() -> DataTableState, melted_view] {
            for (step, steps) in cases {
                let mut state = prior();
                let before = state.snapshot();
                let failed = state.try_transition(steps);
                assert!(failed.is_err(), "{step}: the steps fail");
                assert_eq!(state.snapshot(), before, "{step}: the view is put back");
                assert!(
                    state.prepare_async_collect(None).is_none(),
                    "{step}: the rows on hand serve it, so nothing is read again"
                );
            }
        }
    }

    /// Steps that all plan read nothing; when the new view's rows then fail, the
    /// checkpoint they returned puts back the view before them, rows and all.
    #[test]
    fn a_planned_view_whose_rows_fail_rolls_back_without_reading() {
        for prior in [busy_view as fn() -> DataTableState, melted_view] {
            let mut state = prior();
            let before = state.snapshot();
            let ((), saved) = state
                .try_transition(|s| {
                    s.query("select id, val where id > 4".to_string());
                    s.filter(vec![filter("val", FilterOperator::Lt, "150")]);
                    s.sort_by(vec!["id".to_string()], vec![false]);
                    planned(s)
                })
                .unwrap();
            assert!(!state.is_num_rows_valid(), "the view's count is not read");
            assert!(
                state.prepare_async_collect(None).is_some(),
                "nor its rows: both are left to the background read"
            );

            state.roll_back(saved);
            assert_eq!(state.snapshot(), before);
        }
    }

    /// The view before a query had no count yet; the count lands while the query's
    /// rows are read and they fail. The count comes back with the view, as its own.
    #[test]
    fn a_count_that_lands_meanwhile_comes_back_with_its_view() {
        let mut state = busy_view();
        state.invalidate_num_rows();
        let counting = state.len_generation();
        let ((), mut saved) = state
            .try_transition(|s| {
                s.query("select id".to_string());
                planned(s)
            })
            .unwrap();

        assert!(
            !state.count_landed(counting, 7, None),
            "not a count of the frame on screen"
        );
        assert!(
            !saved.count_landed(state.len_generation(), 99, None),
            "nor is the query's count the view's"
        );
        assert!(saved.count_landed(counting, 7, None));

        state.roll_back(saved);
        assert_eq!(state.len_generation(), counting);
        assert_eq!(state.num_rows_if_valid(), Some(7));
    }

    /// A checkpoint over data that has since been replaced does not put its frames
    /// over the new data; the view returns to the data as loaded instead, with no
    /// rows on hand, for the next background read.
    #[test]
    fn a_checkpoint_over_replaced_data_returns_to_the_data() {
        let mut state = busy_view();
        let saved = state.rollback_point();
        let wider = df!(
            "id" => &[1i64, 2],
            "key" => &["a", "b"],
            "val" => &[10i64, 20],
            "more" => &[true, false],
        )
        .unwrap()
        .lazy();
        let schema = wider.clone().collect_schema().unwrap();
        state.replace_root(wider.clone(), schema.clone());

        state.roll_back(saved);
        assert_eq!(state.schema(), &schema);
        assert!(state.get_active_query().is_empty());
        assert!(state.get_filters().is_empty());
        assert!(state.get_sort_columns().is_empty());
        assert_eq!(
            state.lf().clone().collect().unwrap(),
            wider.collect().unwrap()
        );
        assert!(
            state.prepare_async_collect(None).is_some(),
            "its rows are read in the background"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    thread_local! {
        /// Calls of `compact_rows` on this thread: a test's own thread is the UI thread.
        pub(super) static COMPACTIONS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    }

    fn compactions() -> usize {
        COMPACTIONS.with(std::cell::Cell::get)
    }

    /// Rows a worker read for `[buffer_start, buffer_end)`, before they are fit.
    struct Fill {
        df: DataFrame,
        buffer_start: usize,
        buffer_end: usize,
        num_rows: usize,
        count_known: bool,
    }

    impl DataTableState {
        /// Hand `fill` over as the collect worker does: fit as planned from what is
        /// held and shown now, then installed.
        fn land(&mut self, fill: Fill) {
            let plan = self.fill_plan(
                fill.buffer_start,
                fill.buffer_end,
                fill.num_rows,
                fill.count_known,
            );
            self.apply_async_collect(plan.fit(fill.df));
        }
    }

    fn names(df: &DataFrame) -> Vec<String> {
        df.get_column_names()
            .iter()
            .map(|name| name.to_string())
            .collect()
    }

    fn mixed_strings() -> LazyFrame {
        df!(
            "id" => &[1i64, 2, 3, 4],
            "amount" => &[" 10 ", "20", "", " 40"],
            "day" => &["2024-01-01", "2024-01-02", " ", "2024-01-04"],
            "at" => &["2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z", "", "2024-01-01T12:00:00Z"],
            "score" => &[0.5f64, 1.5, 2.5, 3.5],
            "word" => &["a", " b", "c ", ""],
            "empty" => &[None::<&str>, None, None, None],
        )
        .unwrap()
        .lazy()
    }

    /// String inference reads only the columns it types, and reads them exactly as
    /// the whole-frame sample it replaced did: trimmed, blanks null, same rows.
    #[test]
    fn the_inference_sample_holds_only_its_targets() {
        let targets: Vec<String> = ["amount", "day", "at", "word", "empty"]
            .map(String::from)
            .to_vec();
        let sample = DataTableState::string_inference_sample(mixed_strings(), &targets, 3).unwrap();
        assert_eq!(names(&sample), ["amount", "day", "at", "word", "empty"]);
        assert_eq!(sample.height(), 3);

        // The sample as it was taken before: every column, the targets normalized.
        let blank = lit(PlSmallStr::from_static(""));
        let wide = mixed_strings()
            .limit(3)
            .with_columns(
                targets
                    .iter()
                    .map(|c| {
                        col(c.as_str())
                            .str()
                            .strip_chars(lit(PlSmallStr::from_static(" \t\n\r")))
                    })
                    .collect::<Vec<_>>(),
            )
            .with_columns(
                targets
                    .iter()
                    .map(|c| {
                        when(col(c.as_str()).eq(blank.clone()))
                            .then(Null {}.lit())
                            .otherwise(col(c.as_str()))
                            .alias(c.as_str())
                    })
                    .collect::<Vec<_>>(),
            )
            .collect()
            .unwrap();
        assert_eq!(wide.width(), 7);
        assert!(sample.equals_missing(&wide.select(targets.iter().map(String::as_str)).unwrap()));

        let one =
            DataTableState::string_inference_sample(mixed_strings(), &["day".to_string()], 1_000)
                .unwrap();
        assert_eq!(names(&one), ["day"]);
        assert_eq!(one.height(), 4);
    }

    /// The frame string inference returns keeps every column in its place, and types
    /// the targets as before: numbers, dates, timestamps, text left as text, an
    /// all-null column left alone.
    #[test]
    fn string_inference_keeps_the_whole_frame() {
        let utc = DataType::Datetime(TimeUnit::Microseconds, Some(TimeZone::UTC));
        let typed = |target: &ParseStringsTarget, types: StringTypes| {
            DataTableState::type_string_columns(
                mixed_strings(),
                target,
                1_000,
                types,
                &mut Vec::new(),
                &[],
                &mut Vec::new(),
            )
            .unwrap()
            .collect()
            .unwrap()
        };
        let all = StringTypes {
            dates: true,
            numbers: true,
        };

        let df = typed(&ParseStringsTarget::All, all);
        let schema = df.schema();
        let order = names(&df);
        assert_eq!(
            order,
            ["id", "amount", "day", "at", "score", "word", "empty"]
        );
        assert_eq!(schema.get("id"), Some(&DataType::Int64));
        assert_eq!(schema.get("amount"), Some(&DataType::Int64));
        assert_eq!(schema.get("day"), Some(&DataType::Date));
        assert_eq!(schema.get("at"), Some(&utc));
        assert_eq!(schema.get("score"), Some(&DataType::Float64));
        assert_eq!(schema.get("word"), Some(&DataType::String));
        assert_eq!(schema.get("empty"), Some(&DataType::String));
        let amount: Vec<Option<i64>> = df.column("amount").unwrap().i64().unwrap().iter().collect();
        assert_eq!(amount, [Some(10), Some(20), None, Some(40)]);
        assert_eq!(df.column("day").unwrap().null_count(), 1);
        assert_eq!(df.column("at").unwrap().null_count(), 1);
        let word: Vec<Option<&str>> = df.column("word").unwrap().str().unwrap().iter().collect();
        assert_eq!(word, [Some("a"), Some("b"), Some("c"), Some("")]);
        let score: Vec<Option<f64>> = df.column("score").unwrap().f64().unwrap().iter().collect();
        assert_eq!(score, [Some(0.5), Some(1.5), Some(2.5), Some(3.5)]);

        // Named columns: only those that are text are typed; the rest are as read.
        let some = typed(
            &ParseStringsTarget::Columns(vec!["day".into(), "id".into(), "missing".into()]),
            all,
        );
        assert_eq!(names(&some), order);
        assert_eq!(some.schema().get("day"), Some(&DataType::Date));
        assert_eq!(some.schema().get("amount"), Some(&DataType::String));
        assert_eq!(
            some.column("amount").unwrap().str().unwrap().get(0),
            Some(" 10 ")
        );

        // Nothing to type: the frame comes back as it was.
        let none = typed(&ParseStringsTarget::Columns(vec!["id".into()]), all);
        assert!(none.equals_missing(&mixed_strings().collect().unwrap()));

        // JSON: dates only. Numbers in strings stay text, untrimmed.
        let json = DataTableState::apply_parse_dates_to_json_lazyframe(
            mixed_strings(),
            &crate::OpenOptions::default(),
            &mut Vec::new(),
        )
        .unwrap()
        .collect()
        .unwrap();
        assert_eq!(names(&json), order);
        assert_eq!(json.schema().get("day"), Some(&DataType::Date));
        assert_eq!(json.schema().get("at"), Some(&utc));
        assert_eq!(json.schema().get("amount"), Some(&DataType::String));
        assert_eq!(
            json.column("word").unwrap().str().unwrap().get(1),
            Some(" b")
        );
    }

    /// Data Quality's evidence rows read the downloaded file the dataset scans, so
    /// they hold it too: a view captured there is refused like the dataset's own.
    #[test]
    fn evidence_rows_hold_the_download_they_scan() {
        let dir = tempfile::tempdir().unwrap();
        let mut file = crate::download::TempDownload::create(Some(dir.path()), Some("csv"))
            .expect("a temp file");
        std::io::Write::write_all(&mut file, b"id\n1\n2\n").unwrap();
        let download = crate::download::TempDownload::keep(file);
        let state = DataTableState::from_csv(download.path(), &Default::default())
            .unwrap()
            .with_open(OpenFacts {
                download: Some(download.clone()),
                ..Default::default()
            });
        let path = download.path().to_path_buf();
        drop(download);

        let view = state
            .quality_evidence_view(&crate::data_quality::QualityScope::WholeSource, lit(true))
            .unwrap();
        assert!(view.scans_a_download());
        drop(state);
        assert!(path.exists(), "the view still scans it");
        assert_eq!(collect_lazy(view.lf.clone(), false).unwrap().height(), 2);
        drop(view);
        assert!(!path.exists());
    }

    /// The same for a compressed CSV: the evidence rows scan the decompressed copy,
    /// so they hold it, and it goes with the last state that scans it.
    #[test]
    fn evidence_rows_hold_the_decompressed_file_they_scan() {
        let source = tempfile::tempdir().unwrap();
        let scratch = tempfile::tempdir().unwrap();
        let gz = source.path().join("rows.csv.gz");
        let mut encoder = flate2::write::GzEncoder::new(
            std::fs::File::create(&gz).unwrap(),
            flate2::Compression::default(),
        );
        std::io::Write::write_all(&mut encoder, b"id\n1\n2\n").unwrap();
        encoder.finish().unwrap();
        let options = crate::OpenOptions {
            temp_dir: Some(scratch.path().to_path_buf()),
            ..Default::default()
        };
        let state = DataTableState::from_csv(&gz, &options).unwrap();
        let path = state
            .decompress_temp_file
            .as_ref()
            .expect("decompressed to a temp file")
            .path()
            .to_path_buf();
        assert!(path.starts_with(scratch.path()));

        let view = state
            .quality_evidence_view(&crate::data_quality::QualityScope::WholeSource, lit(true))
            .unwrap();
        assert!(view.scans_a_temp_file());
        drop(state);
        assert!(path.exists(), "the view still scans it");
        assert_eq!(collect_lazy(view.lf.clone(), false).unwrap().height(), 2);
        drop(view);
        assert!(!path.exists());
    }

    /// Scrolling sideways must not count the rows.
    ///
    /// A staged open shows a screen before the count is known, and the count on a
    /// cloud hive is a metadata read per object. `scroll_right` runs on the thread
    /// that draws and reads keys — `App::handle`'s `RIGHT_KEYS` arm calls it
    /// directly — so a count taken there is a freeze no keystroke can interrupt.
    /// The busy-key classifier admits Left/Right on the stated premise that column
    /// scroll never collects, which is what this holds.
    #[test]
    fn scrolling_sideways_does_not_count_the_rows() {
        use polars::prelude::*;

        let frame = || {
            df!(
                "a" => &[1i64, 2, 3],
                "b" => &[4i64, 5, 6],
                "c" => &[7i64, 8, 9],
            )
            .unwrap()
            .lazy()
        };
        let mut lf = frame();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            frame(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        assert_eq!(
            state.num_rows_if_valid(),
            None,
            "a staged open starts without a count"
        );

        state.scroll_right();
        assert_eq!(
            state.num_rows_if_valid(),
            None,
            "scrolling right must leave the count to the background pass"
        );
        state.scroll_left();
        assert_eq!(
            state.num_rows_if_valid(),
            None,
            "and so must scrolling back"
        );
    }

    /// And it must still scroll. Not counting is only the right answer if the new
    /// columns actually reach the screen, so this holds the other half: with a
    /// buffer in hand, Right moves the window the display is sliced from.
    #[test]
    fn scrolling_sideways_still_moves_the_columns() {
        use polars::prelude::*;

        let frame = || {
            df!(
                "a" => &[1i64, 2, 3],
                "b" => &[4i64, 5, 6],
                "c" => &[7i64, 8, 9],
            )
            .unwrap()
            .lazy()
        };
        let mut lf = frame();
        let schema = std::sync::Arc::new((*lf.collect_schema().unwrap()).clone());
        let mut state = DataTableState::from_schema_and_lazyframe(
            schema,
            frame(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        // As the app has it once a page has landed: a count and a buffer.
        state.set_num_rows(3);
        state.visible_rows = 3;
        state.visible_termcols = 1;
        state.collect();
        let first = state
            .display_df()
            .map(|df| {
                df.get_column_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();

        state.scroll_right();
        let second = state
            .display_df()
            .map(|df| {
                df.get_column_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        assert_ne!(first, second, "Right should show a different column window");
        assert!(!second.is_empty(), "and should show something");

        state.scroll_left();
        let back = state
            .display_df()
            .map(|df| {
                df.get_column_names()
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .unwrap_or_default();
        assert_eq!(back, first, "Left should come back to where it started");
    }

    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
    use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec};

    /// The join drops the rows it read through the frame it replaced.
    ///
    /// The buffer holds rows read at the old schema, through the old scan. Kept, the
    /// next paint draws them under the new column order — and `display_slice_df` offsets
    /// into them from `buffered_start_row`, so where the user is deep in a dataset it is
    /// the wrong rows under the right numbers.
    #[test]
    fn the_join_drops_the_rows_read_through_the_frame_it_replaced() {
        let narrow = || df!("id" => &[1i64, 2]).unwrap().lazy();
        let wider = || {
            df!("id" => &[1i64, 2], "oops" => &["a", "b"])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileFooter {
                schema,
                row_group_rows: vec![2],
                file_bytes: 0,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(narrow()).schema.clone(),
            narrow(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        state.visible_rows = 2;
        state.collect();
        assert!(
            state.buffered_df.is_some(),
            "there are rows on hand, read at the old schema"
        );

        assert!(
            state
                .join_dataset_schema(FootersFound {
                    estimate: None,
                    dataset: dataset_of(wider()),
                    lf: wider(),
                    file_rows: Vec::new(),
                    files: Vec::new(),
                    row_groups: Vec::new(),
                    remote: None,
                })
                .is_ok()
        );

        assert!(
            state.buffered_df.is_none(),
            "and they are let go, rather than drawn under the columns that replaced them"
        );
        assert_eq!(
            (state.buffered_start_row, state.buffered_end_row),
            (0, 0),
            "with nothing left saying which rows they were"
        );
    }

    /// The join throws away a width measured on the frame it replaced.
    ///
    /// `bytes_per_row` prefers what was observed over what the schema estimates, and the
    /// observation belongs to the frame that just went. A dataset that opened two
    /// columns wide and gained thirty would plan its first page after the join from the
    /// two-column width — against a bucket, a read many times the budget the user set.
    /// `install_base` clears it for the same reason.
    #[test]
    fn the_join_does_not_keep_a_width_measured_on_the_frame_it_replaced() {
        let narrow = || df!("id" => &[1i64, 2]).unwrap().lazy();
        let wide = || {
            df!("id" => &[1i64, 2], "a" => &["x", "y"], "b" => &["x", "y"])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileFooter {
                schema,
                row_group_rows: vec![2],
                file_bytes: 0,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(narrow()).schema.clone(),
            narrow(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        state.observed_bytes_per_row = Some(8);
        let measured_narrow = state.bytes_per_row();

        assert!(
            state
                .join_dataset_schema(FootersFound {
                    estimate: None,
                    dataset: dataset_of(wide()),
                    lf: wide(),
                    file_rows: Vec::new(),
                    files: Vec::new(),
                    row_groups: Vec::new(),
                    remote: None,
                })
                .is_ok(),
            "nothing is built on the scan here, so the columns go straight in"
        );

        assert!(
            state.bytes_per_row() > measured_narrow,
            "a row of the widened dataset is not planned at the width of the old one: \
             {} vs {measured_narrow}",
            state.bytes_per_row()
        );
    }

    /// A dataset stops saying a count is coming once one has arrived.
    ///
    /// While a pass is reading its footers the dataset declines to count itself, because
    /// that pass is bringing the count — and the control bar shows a spinner in place of
    /// a number it would otherwise print as fact. When the pass lands on a view built on
    /// the scan the columns have to wait, but the row groups describe the same files the
    /// frame is already reading, so the number need not: without this the user watches
    /// that spinner over a count the dataset is holding.
    #[test]
    fn a_count_that_has_arrived_is_not_held_back_with_the_columns() {
        let rows = || df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap().lazy();
        let wider = || {
            df!("id" => (0..100i64).collect::<Vec<_>>(), "oops" => vec!["a"; 100])
                .unwrap()
                .lazy()
        };
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileFooter {
                schema,
                row_group_rows: vec![100],
                file_bytes: 0,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(rows()).schema.clone(),
            rows(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(vec!["one".to_string()]),
                scan: Arc::new(move |_u: &[String], _t: &[PlSmallStr]| Ok(rows())),
                count: Arc::new(|_| Ok(vec![vec![100]])),
                offsets: None,
            }),
            footers_pending: Some(Arc::new(|_| None)),
            ..Default::default()
        });
        assert!(
            state.counts_itself_later(),
            "the pass is bringing a count, so the dataset is not going to fetch one"
        );

        // The user is in a query when it lands, so the columns cannot go in.
        state.active_query = "select doubled: id * 2".to_string();
        let held = state.join_dataset_schema(FootersFound {
            estimate: None,
            dataset: dataset_of(wider()),
            lf: wider(),
            file_rows: vec![100],
            files: vec!["one".to_string()],
            row_groups: vec![vec![100]],
            remote: None,
        });
        assert!(held.is_err(), "the columns wait for the query to be let go");

        // What the footers said about the files stays, so letting the query go gets the
        // total back rather than sending anyone to fetch it again.
        state.active_query.clear();
        state.restore_footer_count();
        assert_eq!(
            state.num_rows_if_valid(),
            Some(100),
            "the count is there, without a read to find it"
        );
        assert!(
            !state.counts_itself_later(),
            "so the dataset stops saying one is coming, and the bar prints a number \
             rather than a spinner over one it is holding"
        );
    }

    /// A column the second pass could not see goes, rather than breaking every read.
    ///
    /// Columns only join — but the pass reads footers again, and a footer that parsed
    /// at the open can fail the second time: a transient error, or the object replaced
    /// between the two reads. If that was the only file with a column, the name is left
    /// naming nothing. Every page projects `column_order`, so a name the new schema
    /// does not have is not a blank column on screen, it is a scan that will not plan.
    #[test]
    fn a_column_the_second_pass_could_not_see_leaves_the_order() {
        let at_open = || {
            df!("id" => &[1i64], "only_the_first_file_had_this" => &["x"])
                .unwrap()
                .lazy()
        };
        let second_time = || df!("id" => &[1i64], "oops" => &["a"]).unwrap().lazy();
        let dataset_of = |lf: LazyFrame| {
            let mut lf = lf;
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileFooter {
                schema,
                row_group_rows: vec![1],
                file_bytes: 0,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            };
            crate::schema_union::union_sampled(1, &[0], &[Some(footer)])
        };

        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset_of(at_open()).schema.clone(),
            at_open(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap();
        assert!(
            state
                .get_column_order()
                .iter()
                .any(|c| c == "only_the_first_file_had_this"),
            "the dataset opened with it"
        );

        assert!(
            state
                .join_dataset_schema(FootersFound {
                    estimate: None,
                    dataset: dataset_of(second_time()),
                    lf: second_time(),
                    file_rows: Vec::new(),
                    files: Vec::new(),
                    row_groups: Vec::new(),
                    remote: None,
                })
                .is_ok()
        );

        assert_eq!(
            state.get_column_order(),
            ["id", "oops"],
            "the name the pass can no longer account for is not left naming nothing"
        );
    }

    /// Every way of building on the scan holds the arriving columns off, not just one.
    ///
    /// Each of these replaces the frame's root with a result of its own, whose columns
    /// are not the dataset's. The doc on `scan_is_the_root` says so of all of them; the
    /// query is the one the app-level test exercises, so this is the rest.
    #[test]
    fn anything_built_on_the_scan_holds_the_arriving_columns_off() {
        // A string column because a fuzzy search needs one to search.
        let narrow = || {
            df!("id" => &[1i64, 2], "v" => &[10i64, 20], "name" => &["one", "two"])
                .unwrap()
                .lazy()
        };
        let wider = || {
            df!(
                "id" => &[1i64, 2],
                "v" => &[10i64, 20],
                "name" => &["one", "two"],
                "oops" => &["a", "b"],
            )
            .unwrap()
            .lazy()
        };
        let found = || {
            let mut lf = wider();
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            let footer = crate::schema_union::FileFooter {
                schema,
                row_group_rows: vec![2],
                file_bytes: 0,
                row_group_bytes: Vec::new(),
                column_bytes: Vec::new(),
            };
            FootersFound {
                estimate: None,
                dataset: crate::schema_union::union_sampled(1, &[0], &[Some(footer)]),
                lf: wider(),
                file_rows: Vec::new(),
                files: Vec::new(),
                row_groups: Vec::new(),
                remote: None,
            }
        };
        let fresh = || {
            let mut lf = narrow();
            let schema = Arc::new((*lf.collect_schema().unwrap()).clone());
            DataTableState::from_schema_and_lazyframe(
                schema,
                narrow(),
                &crate::OpenOptions::default(),
                None,
            )
            .unwrap()
        };

        // A filter and a sort are not built on the scan, they are the scan with
        // something done to it, and `apply_transformations` puts them back over
        // whatever the root becomes. Those the columns may join under.
        let mut sorted = fresh();
        sorted.sort(vec!["id".to_string()], true);
        assert!(
            sorted.join_dataset_schema(found()).is_ok(),
            "a sort is rebuilt over the wider scan, so the columns go in under it"
        );

        let mut queried = fresh();
        queried.query("select doubled: v * 2".to_string());
        assert!(
            queried.join_dataset_schema(found()).is_err(),
            "a query's columns are its own"
        );

        #[cfg(feature = "sql")]
        {
            let mut sql = fresh();
            sql.sql_query("SELECT id FROM df".to_string());
            assert!(sql.error.is_none(), "the statement runs: {:?}", sql.error);
            assert!(
                sql.join_dataset_schema(found()).is_err(),
                "and a SQL statement's are too"
            );
        }

        let mut fuzzy = fresh();
        fuzzy.fuzzy_search("10".to_string());
        assert!(
            fuzzy.join_dataset_schema(found()).is_err(),
            "and what a fuzzy search matched is a result, not the dataset"
        );

        let mut melted = fresh();
        melted
            .melt(&MeltSpec {
                index: vec!["id".to_string()],
                value_columns: vec!["v".to_string()],
                variable_name: "variable".to_string(),
                value_name: "value".to_string(),
            })
            .expect("the melt runs");
        assert!(
            melted.join_dataset_schema(found()).is_err(),
            "a melt's rows are not the dataset's rows"
        );

        let mut pivoted = fresh();
        pivoted
            .pivot(&PivotSpec {
                index: vec!["id".to_string()],
                pivot_column: "name".to_string(),
                value_column: "v".to_string(),
                aggregation: PivotAggregation::First,
                sort_columns: None,
            })
            .expect("the pivot runs");
        assert!(
            pivoted.join_dataset_schema(found()).is_err(),
            "and a pivot's columns are made from the data, not read from it"
        );

        let mut drilled = fresh();
        // The field rather than the drill itself, which needs a grouped frame to drill
        // into: what is being asked here is whether the clause is consulted.
        drilled.drilled_down_group_index = Some(0);
        assert!(
            drilled.join_dataset_schema(found()).is_err(),
            "and a drill-down is showing one group of it, not it"
        );
    }

    /// Three files of two rows each, and every way they can conflict.
    ///
    /// Each case names the files that hold the column in a type it is not read in, and
    /// the rows those files own. The last file is the one that has no start after it,
    /// so a run ending there is the case an implementation is most likely to get wrong
    /// — and the one that two files of the same length hide, since dropping to the end
    /// of the dataset and dropping to the next file's start agree there.
    #[test]
    fn conflicting_files_become_the_runs_of_rows_they_own() {
        let starts = [0, 2, 4];
        let runs = |conflicts: [bool; 3]| conflicting_row_runs(&starts, 6, &conflicts);

        assert_eq!(runs([false, false, false]), vec![], "nothing conflicts");
        assert_eq!(runs([true, false, false]), vec![(0, 2)], "the first file");
        assert_eq!(
            runs([false, true, false]),
            vec![(2, 4)],
            "a file in the middle ends where the next one begins, not at the end of \
             the dataset"
        );
        assert_eq!(
            runs([false, false, true]),
            vec![(4, 6)],
            "and the last one ends at the end of the dataset"
        );
        assert_eq!(
            runs([true, true, false]),
            vec![(0, 4)],
            "files that touch are one run"
        );
        assert_eq!(runs([true, true, true]), vec![(0, 6)], "as are all of them");
        assert_eq!(
            runs([true, false, true]),
            vec![(0, 2), (4, 6)],
            "files that do not touch are not"
        );
    }

    /// A file of no rows owns no rows, so it neither makes a run nor breaks one.
    ///
    /// Zero-row Parquet files are written by any pipeline that partitions on a key
    /// with no data for some value, so this is a shape datui meets rather than one it
    /// has to imagine.
    #[test]
    fn a_file_of_no_rows_neither_makes_a_run_nor_splits_one() {
        // Files of 2, 0 and 2 rows: the middle one begins and ends at row 2.
        let starts = [0, 2, 2];
        assert_eq!(
            conflicting_row_runs(&starts, 4, &[false, true, false]),
            vec![],
            "a conflicting file with no rows keeps no row out"
        );
        assert_eq!(
            conflicting_row_runs(&starts, 4, &[true, false, true]),
            vec![(0, 4)],
            "and an empty file between two that conflict does not part them"
        );
        assert_eq!(
            conflicting_row_runs(&starts, 4, &[true, true, true]),
            vec![(0, 4)],
            "however it is flagged itself"
        );
    }

    fn file_schema(
        columns: &[(&str, polars::prelude::DataType)],
        rows: usize,
    ) -> Option<crate::schema_union::FileFooter> {
        let mut schema = polars::prelude::Schema::with_capacity(columns.len());
        for (name, dtype) in columns {
            schema.with_column((*name).into(), dtype.clone());
        }
        Some(crate::schema_union::FileFooter {
            schema: Arc::new(schema),
            row_group_rows: vec![rows],
            file_bytes: 0,
            row_group_bytes: Vec::new(),
            column_bytes: Vec::new(),
        })
    }

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
        // Done once nothing is chained, queued or still owed; the deadline is only a
        // hang guard.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(300);
        loop {
            match next.take() {
                Some(ev) => {
                    if matches!(ev, AppEvent::Crash(_)) {
                        saw_crash = true;
                        break;
                    }
                    next = app.event(&ev);
                }
                _ => match rx.try_recv() {
                    Ok(ev) => next = Some(ev),
                    Err(_) if app.count_waits_for_a_frame() => next = Some(AppEvent::FramePainted),
                    Err(_) if !crate::tests::work_pending(app) => break,
                    Err(_) => {
                        assert!(
                            std::time::Instant::now() < deadline,
                            "background work never reported back"
                        );
                        next = rx.recv_timeout(std::time::Duration::from_millis(50)).ok();
                    }
                },
            }
        }
        saw_crash
    }

    /// CSV with 100 int-like rows then "N/A" then more ints. With infer_schema_length=100, Polars
    /// infers Int from the first 100 rows; the parse error surfaces from the async collect as a
    /// collect failure, which the app shows in the error modal rather than crashing.
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
        let state = DataTableState::from_parquet(&path, &OpenOptions::default()).unwrap();
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui_test_ipc.arrow");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        IpcWriter::new(&mut writer).finish(&mut df).unwrap();
        drop(writer);
        let state = DataTableState::from_ipc(&path, &OpenOptions::default()).unwrap();
        assert_eq!(state.schema.len(), 2);
        assert!(state.schema.contains("x"));
        assert!(state.schema.contains("y"));
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
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui_test_avro.avro");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = BufWriter::new(file);
        AvroWriter::new(&mut writer).finish(&mut df).unwrap();
        drop(writer);
        let state = DataTableState::from_avro(&path, &OpenOptions::default()).unwrap();
        assert_eq!(state.schema.len(), 2);
        assert!(state.schema.contains("id"));
        assert!(state.schema.contains("name"));
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

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui_test_orc.orc");
        let file = std::fs::File::create(&path).unwrap();
        let writer = BufWriter::new(file);
        let mut orc_writer = ArrowWriterBuilder::new(writer, schema).try_build().unwrap();
        orc_writer.write(&batch).unwrap();
        orc_writer.close().unwrap();

        let state = DataTableState::from_orc(&path, &OpenOptions::default()).unwrap();
        assert_eq!(state.schema.len(), 2);
        assert!(state.schema.contains("id"));
        assert!(state.schema.contains("name"));
    }

    /// `--delimiter` reaches the in-memory readers of every compression, and the
    /// one-row read that per-column null values are built from (#290), including
    /// for a file that is only readable once decompressed.
    #[test]
    fn test_delimiter_reaches_every_csv_reader() {
        use std::io::Write;
        let dir = tempfile::tempdir().unwrap();
        let body = b"id|name\n1|NA\n2|x\n";
        let bz = dir.path().join("t.csv.bz2");
        let mut enc =
            bzip2::write::BzEncoder::new(File::create(&bz).unwrap(), bzip2::Compression::best());
        enc.write_all(body).unwrap();
        enc.finish().unwrap();
        let xz = dir.path().join("t.csv.xz");
        let mut enc = xz2::write::XzEncoder::new(File::create(&xz).unwrap(), 6);
        enc.write_all(body).unwrap();
        enc.finish().unwrap();
        let plain = dir.path().join("t.csv");
        std::fs::write(&plain, body).unwrap();

        let in_memory = OpenOptions {
            delimiter: Some(b'|'),
            decompress_in_memory: true,
            ..Default::default()
        };
        // A global and a per-column value together make the read ask for the schema;
        // the column's own value replaces the global one, so only `x` is null.
        let null_values = OpenOptions {
            delimiter: Some(b'|'),
            null_values: Some(vec!["NA".into(), "name=x".into()]),
            ..Default::default()
        };
        // The same in memory, where the column names have to come from the
        // decompressed bytes rather than the file on disk.
        let null_values_in_memory = OpenOptions {
            decompress_in_memory: true,
            ..null_values.clone()
        };
        for (what, path, opts) in [
            ("bzip2", &bz, &in_memory),
            ("xz", &xz, &in_memory),
            ("null values", &plain, &null_values),
            ("null values, bzip2", &bz, &null_values_in_memory),
            ("null values, xz", &xz, &null_values_in_memory),
        ] {
            let state = DataTableState::from_csv(path, opts).unwrap();
            let df = state.lf.clone().collect().unwrap();
            let names: Vec<_> = df
                .get_column_names()
                .iter()
                .map(|n| n.to_string())
                .collect();
            assert_eq!(names, ["id", "name"], "{what}");
            if opts.null_values.is_some() {
                assert_eq!(df.column("name").unwrap().null_count(), 1, "{what}");
            }
        }
    }

    #[test]
    fn test_from_delimited_tsv_has_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui_test_tsv_header.tsv");
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
    }

    #[test]
    fn test_from_delimited_tsv_no_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui_test_tsv_no_header.tsv");
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
    }

    #[test]
    fn test_from_delimited_psv_no_header() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("datui_test_psv_no_header.psv");
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
    }

    #[test]
    fn test_filter() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        let filters = vec![FilterStatement {
            columns: Vec::new(),
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

    /// Ties keep their order, so the page read at the top (a top-k to Polars) and
    /// the page read below it agree on the rows they share.
    #[test]
    fn a_sort_with_ties_reads_the_same_rows_page_by_page() {
        let df = df!(
            "k" => (0..3000i64).map(|i| i % 3).collect::<Vec<_>>(),
            "v" => (0..3000i64).collect::<Vec<_>>(),
        )
        .unwrap();
        let sorted = df
            .lazy()
            .sort_by_exprs([col("k")], sort_options(vec![false]));
        let top = sorted.clone().slice(0, 200).collect().unwrap();
        let below = sorted.clone().slice(100, 100).collect().unwrap();
        assert!(top.slice(100, 100).equals(&below));
        let one = sorted.slice(5, 1).collect().unwrap();
        assert!(top.slice(5, 1).equals(&one));
    }

    /// `IN (SELECT …)` reads the subquery's values once rather than once per row,
    /// which on the streaming engine ran out of memory for a page of 100k rows
    /// (#509), and still returns what polars-sql's own plan does, NULLs included.
    /// The data is small, so that without the fix the test fails on the plan rather
    /// than on memory. A user's own list question of the same shape is left alone.
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_in_subquery_reads_its_values_once() {
        let mut df = df!(
            "k" => (0..3000i64).map(|i| i % 3).collect::<Vec<_>>(),
            "v" => (0..3000i64).map(|i| (i % 7 != 0).then_some(i)).collect::<Vec<_>>(),
        )
        .unwrap();
        // A first value that is a NULL list: its length is NULL, not 1 as exploded.
        let lists: ListChunked = (0..3000i64)
            .map(|i| (i != 0).then(|| Series::new(PlSmallStr::EMPTY, [i])))
            .collect();
        df.with_column(lists.into_column().with_name("l".into()))
            .unwrap();
        let n = df.height();
        for (sql, rows) in [
            (
                "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 1)",
                None,
            ),
            // The values hold a NULL, so no row is NOT IN them.
            (
                "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 1)",
                Some(0),
            ),
            (
                "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 1 AND v IS NOT NULL)",
                None,
            ),
            // No values: every row, a NULL `v` too.
            (
                "SELECT * FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 5)",
                Some(n),
            ),
            (
                "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 5)",
                Some(0),
            ),
            (
                "SELECT * FROM df WHERE k = 2 OR v IN (SELECT v FROM df WHERE k = 1 LIMIT 100)",
                None,
            ),
            (
                "SELECT * FROM df WHERE v IN (SELECT MIN(v) FROM df GROUP BY v % 100)",
                None,
            ),
            (
                "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE v NOT IN (SELECT v FROM df WHERE k = 0 AND v IS NOT NULL))",
                None,
            ),
            // Unknown, not false, for a value outside a set holding a NULL, and for a
            // NULL value.
            (
                "SELECT * FROM df WHERE (v NOT IN (SELECT v FROM df WHERE k = 1)) IS NULL",
                None,
            ),
            // Only NULLs: every row unknown.
            (
                "SELECT * FROM df WHERE (v IN (SELECT v FROM df WHERE v IS NULL)) IS NULL",
                Some(n),
            ),
            // No values: false, a NULL `v` too.
            (
                "SELECT * FROM df WHERE (v IN (SELECT v FROM df WHERE k = 5)) IS NULL",
                Some(0),
            ),
            (
                "SELECT * FROM df WHERE ARRAY_LENGTH(FIRST(l)) IS NULL AND v NOT IN (SELECT v FROM df WHERE k = 5)",
                Some(n),
            ),
        ] {
            let mut ctx = polars_sql::SQLContext::new();
            ctx.register("df", df.clone().lazy());
            let raw = ctx.execute(sql).unwrap();
            // Else the check on the view's plan below would pass for nothing.
            assert!(
                (&raw.logical_plan).into_iter().any(asks_per_row),
                "{sql}: polars-sql's plan"
            );
            let expected = raw.collect().unwrap();
            if let Some(rows) = rows {
                assert_eq!(expected.height(), rows, "{sql}");
            }
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            assert!(
                !(&state.lf.logical_plan).into_iter().any(asks_per_row),
                "{sql}"
            );
            for streaming in [false, cfg!(feature = "streaming")] {
                let got = collect_lazy(state.lf.clone(), streaming).unwrap();
                assert!(
                    got.equals_missing(&expected),
                    "{sql}, streaming {streaming}"
                );
            }
        }
    }

    /// An `IN (SELECT …)` subquery returns exactly the statement's columns, after a
    /// QUALIFY as after a WHERE: polars-sql leaves the column holding the values in a
    /// QUALIFY's result (#519). The rows are polars-sql's own, and a user's column
    /// named like polars-sql's is kept.
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_in_subquery_returns_only_the_statements_columns() {
        const LOOKALIKE: &str = "_POLARS_TMP_999999999";
        let df = df!(
            "k" => (0..300i64).map(|i| i % 3).collect::<Vec<_>>(),
            "i" => (0..300i64).collect::<Vec<_>>(),
            "w" => (0..300i64).map(|i| (i % 7 != 0).then_some(i % 11)).collect::<Vec<_>>(),
            LOOKALIKE => (0..300i64).collect::<Vec<_>>(),
        )
        .unwrap();
        let all = ["k", "i", "w", LOOKALIKE];
        for (sql, columns) in [
            (
                "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
                 QUALIFY r IN (SELECT w FROM df WHERE w < 3)",
                &["k", "i", "r"][..],
            ),
            (
                "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
                 QUALIFY r NOT IN (SELECT w FROM df WHERE w > 3 AND w IS NOT NULL) \
                 ORDER BY i DESC LIMIT 50",
                &["k", "i", "r"][..],
            ),
            (
                "SELECT * FROM df \
                 QUALIFY ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) IN (SELECT w FROM df)",
                &all[..],
            ),
            (
                "SELECT DISTINCT k, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r, _POLARS_TMP_999999999 FROM df \
                 QUALIFY r IN (SELECT w FROM df WHERE w < 3)",
                &["k", "r", LOOKALIKE][..],
            ),
            (
                "SELECT * FROM (SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
                 QUALIFY r IN (SELECT w FROM df WHERE w < 3)) WHERE i > 1",
                &["k", "i", "r"][..],
            ),
            (
                "SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
                 QUALIFY r IN (SELECT w FROM df WHERE w < 3) AND i IN (SELECT w FROM df)",
                &["k", "i", "r"][..],
            ),
            (
                "WITH q AS (SELECT k, i, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
                 QUALIFY r IN (SELECT w FROM df WHERE w < 3)) \
                 SELECT * FROM q UNION ALL SELECT * FROM q",
                &["k", "i", "r"][..],
            ),
            // A join suffixes the right side's copy of both polars-sql's column and
            // the user's.
            (
                "WITH q AS (SELECT *, ROW_NUMBER() OVER (PARTITION BY k ORDER BY i) AS r FROM df \
                 QUALIFY r IN (SELECT w FROM df WHERE w < 3)) \
                 SELECT * FROM q a JOIN q b ON a.i = b.i JOIN q c ON a.i = c.i ORDER BY a.i",
                &[
                    "k",
                    "i",
                    "w",
                    LOOKALIKE,
                    "r",
                    "k:b",
                    "i:b",
                    "w:b",
                    "_POLARS_TMP_999999999:b",
                    "r:b",
                    "k:c",
                    "i:c",
                    "w:c",
                    "_POLARS_TMP_999999999:c",
                    "r:c",
                ][..],
            ),
            ("SELECT * FROM df WHERE i IN (SELECT w FROM df)", &all[..]),
            (
                "SELECT k, i FROM df WHERE i NOT IN (SELECT w FROM df WHERE w IS NOT NULL)",
                &["k", "i"][..],
            ),
        ] {
            let mut ctx = polars_sql::SQLContext::new();
            ctx.register("df", df.clone().lazy());
            let raw = ctx.execute(sql).unwrap().collect().unwrap();
            assert!(raw.height() > 0, "{sql}");
            let expected = raw.select(columns.iter().copied()).unwrap();
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            let names: Vec<&str> = state.schema.iter_names().map(|n| n.as_str()).collect();
            assert_eq!(names, columns, "{sql}");
            for streaming in [false, cfg!(feature = "streaming")] {
                let got = collect_lazy(state.lf.clone(), streaming).unwrap();
                assert!(
                    got.equals_missing(&expected),
                    "{sql}, streaming {streaming}: {got:?}"
                );
            }
        }
    }

    /// The ORDER BY of the SQL in effect is state, so it leaves its mark: the sort
    /// mark on each column it orders by, as named in the result, until the sidebar
    /// sorts or another query runs (#688, item 13).
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_order_by_marks_the_header_until_the_sidebar_sorts() {
        let df = df!("k" => [1i64, 2, 3], "v" => [3i64, 2, 1]).unwrap();
        let marks = |sql: &str| {
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            state.header_sort()
        };
        let owned = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        assert_eq!(
            marks("SELECT v, k FROM df ORDER BY k DESC"),
            (owned(&["k"]), vec![true])
        );
        assert_eq!(
            marks("SELECT * FROM df ORDER BY k DESC, v LIMIT 2"),
            (owned(&["k", "v"]), vec![true, false])
        );
        assert_eq!(
            marks("SELECT v AS w, k FROM df ORDER BY w"),
            (owned(&["w"]), vec![false]),
            "named as in the result"
        );
        assert_eq!(
            marks("SELECT k, SUM(v) AS s FROM df GROUP BY k ORDER BY s DESC"),
            (owned(&["s"]), vec![true])
        );
        // An expression, or a column the result leaves out, leaves no mark.
        assert_eq!(marks("SELECT * FROM df ORDER BY k + 1"), (vec![], vec![]));
        assert_eq!(marks("SELECT v FROM df ORDER BY k"), (vec![], vec![]));
        assert_eq!(marks("SELECT * FROM df"), (vec![], vec![]));

        // On the header, and the sidebar's sort replaces it.
        let mut state =
            DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
        state.sql_query("SELECT * FROM df ORDER BY k DESC".to_string());
        state.collect();
        let area = Rect::new(0, 0, 30, 6);
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, &mut state);
        let header = row_string(&buf, area, 0);
        let g = crate::glyphs::get();
        assert!(header.contains(&format!("k{}", g.sort_desc)), "{header:?}");
        state.sort_by(vec!["v".to_string()], vec![false]);
        assert_eq!(state.header_sort(), (owned(&["v"]), vec![false]));
        // A new query names its own order, or none.
        state.sql_query("SELECT * FROM df".to_string());
        assert_eq!(state.header_sort(), (vec![], vec![]));
    }

    /// A SQL ORDER BY keeps tied rows in order, as the sidebar's sort does: the page
    /// read at the top (a top-k to Polars) and the next page agree on the rows they
    /// share, through a LIMIT and a subquery, and over a grouping, a join, a union or
    /// a DISTINCT, whose rows would otherwise come in any order (#495). With either
    /// engine.
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_order_by_with_ties_reads_the_same_rows_page_by_page() {
        let df = df!(
            "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
            "v" => (0..5000i64).collect::<Vec<_>>(),
        )
        .unwrap();
        for sql in [
            "SELECT * FROM df ORDER BY k",
            "SELECT v, k FROM df ORDER BY k DESC",
            "SELECT * FROM df ORDER BY k LIMIT 4000",
            "SELECT * FROM (SELECT * FROM df ORDER BY k) WHERE v >= 0",
            "SELECT v % 1000 AS g, COUNT(*) AS n, MIN(v) AS v FROM df GROUP BY g ORDER BY n",
            "SELECT a.k, a.v FROM df a JOIN df b ON a.v = b.v ORDER BY a.k",
            "SELECT a.k, a.v FROM df a LEFT JOIN df b ON a.v = b.v + 1 ORDER BY a.k",
            "SELECT k, v FROM df UNION SELECT k, v FROM df ORDER BY k",
            "SELECT DISTINCT v % 1000 AS g, v % 1000 AS v FROM df ORDER BY g % 3",
        ] {
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            let unstable = (&state.lf.logical_plan).into_iter().any(|node| {
                matches!(
                    node,
                    polars::lazy::dsl::DslPlan::Sort { sort_options, .. }
                        if !sort_options.maintain_order
                )
            });
            assert!(!unstable, "{sql}");
            // The app pages with the streaming engine by default, which runs the top
            // page's top-k its own way.
            for streaming in [false, cfg!(feature = "streaming")] {
                let page = |offset, len| {
                    collect_lazy(state.lf.clone().slice(offset, len), streaming).unwrap()
                };
                let top = page(0, 200);
                let next = page(100, 200);
                assert!(top.slice(100, 100).equals(&next.slice(0, 100)), "{sql}");
                let one = page(1500, 1);
                let around = page(1400, 200);
                assert!(around.slice(100, 1).equals(&one), "{sql}");
                // Ties keep the order they were read in.
                let v = top.column("v").unwrap().i64().unwrap();
                assert!(
                    v.into_no_null_iter().is_sorted(),
                    "{sql}, streaming {streaming}: {:?}",
                    v.head(Some(10))
                );
            }
        }
    }

    /// A SQL result with no ORDER BY reads the same rows page by page, and a Sort &
    /// Filter sort over it keeps its ties in that order: groupings, distincts, unions
    /// and joins give their rows in one order, so every page agrees with a read of
    /// the whole result, and a LIMIT keeps the same rows (#508). Both engines give the
    /// same order. A grouping sorted by its keys leaves its groups' order to the sort.
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_result_without_order_by_reads_the_same_rows_page_by_page() {
        let df = df!(
            "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
            "v" => (0..5000i64).collect::<Vec<_>>(),
        )
        .unwrap();
        for sql in [
            "SELECT a.k, a.v FROM df a JOIN df b ON a.v = b.v",
            "SELECT a.k, a.v, b.v AS w FROM df a LEFT JOIN df b ON a.v = b.v + 1",
            "SELECT k, v FROM df UNION ALL SELECT k, v FROM df",
            "SELECT k, v FROM df UNION SELECT k, v FROM df",
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g LIMIT 300",
            "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g",
            "SELECT g, COUNT(*) AS n FROM (SELECT v % 1000 AS g FROM df) GROUP BY g",
            "SELECT DISTINCT v % 1000 AS g FROM df",
            "SELECT * FROM df WHERE v IN (SELECT v FROM df WHERE k = 1) LIMIT 1000",
        ] {
            for sort in [false, true] {
                let mut state =
                    DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default())
                        .unwrap();
                state.sql_query(sql.to_string());
                assert!(state.error.is_none(), "{sql}: {:?}", state.error);
                if sort {
                    // By the first column, which most of these results repeat.
                    let first = state.schema.get_at_index(0).unwrap().0.to_string();
                    state.sort(vec![first], true);
                    assert!(state.error.is_none(), "{sql}: {:?}", state.error);
                }
                let mut fulls = Vec::new();
                for streaming in [false, cfg!(feature = "streaming")] {
                    let read = |lf: LazyFrame| collect_lazy(lf, streaming).unwrap();
                    let full = read(state.lf.clone());
                    let middle = full.height() as i64 / 2;
                    for offset in [0, 100, middle] {
                        let page = read(state.lf.clone().slice(offset, 200));
                        assert!(
                            page.equals_missing(&full.slice(offset, 200)),
                            "{sql}, sort {sort}, streaming {streaming}, offset {offset}"
                        );
                    }
                    let one = read(state.lf.clone().slice(middle + 7, 1));
                    assert!(
                        one.equals_missing(&full.slice(middle + 7, 1)),
                        "{sql}, sort {sort}, streaming {streaming}"
                    );
                    fulls.push(full);
                }
                assert!(
                    fulls[0].equals_missing(&fulls[1]),
                    "{sql}, sort {sort}: the engines disagree"
                );
            }
        }
        // Keeping the groups' order as well would double a large grouping's time.
        let mut state = DataTableState::from_lazyframe(df.lazy(), &OpenOptions::default()).unwrap();
        state.sql_query("SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g".to_string());
        assert!((&state.lf.logical_plan).into_iter().any(|node| matches!(
            node,
            polars::lazy::dsl::DslPlan::GroupBy {
                maintain_order: false,
                ..
            }
        )));
    }

    /// A SQL grouping whose ORDER BY covers every key, by alias, ordinal or name,
    /// leaves its groups' order to the sort (#523), and still reads the same rows page
    /// by page, in the order polars-sql's own plan gives, with either engine, NULL and
    /// NaN keys too. A sort that leaves any key out, sorts by an expression of one, or
    /// reads the groups through a LIMIT, a filter or a computed column keeps the
    /// groups' order.
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_grouping_sorted_by_its_keys_leaves_the_order_to_the_sort() {
        use polars::lazy::dsl::DslPlan;
        // NaNs group as one and sort as one, as do 0.0 and -0.0.
        let floats = [0.0, -0.0, f64::NAN, -f64::NAN, 1.0, f64::INFINITY, -1.5];
        let df = df!(
            "k" => (0..5000i64).map(|i| i % 3).collect::<Vec<_>>(),
            "v" => (0..5000i64).collect::<Vec<_>>(),
            "w" => (0..5000i64).map(|i| (i % 11 != 0).then_some(i % 700)).collect::<Vec<_>>(),
            "f" => (0..5000usize)
                .map(|i| (i % 9 != 0).then_some(floats[i % floats.len()]))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let groups_ordered = |plan: &DslPlan| -> Vec<bool> {
            plan.into_iter()
                .filter_map(|node| match node {
                    DslPlan::GroupBy { maintain_order, .. } => Some(*maintain_order),
                    _ => None,
                })
                .collect()
        };
        for (sql, ordered) in [
            (
                "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g ORDER BY g",
                false,
            ),
            (
                "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY v % 1000 ORDER BY 1 DESC LIMIT 300",
                false,
            ),
            (
                "SELECT k AS kk, v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY k, g ORDER BY n, g, kk",
                false,
            ),
            (
                "SELECT k AS d, COUNT(*) AS n FROM df GROUP BY k ORDER BY k DESC",
                false,
            ),
            (
                "SELECT w, MIN(v) AS v FROM df GROUP BY w HAVING COUNT(*) > 1 ORDER BY ALL",
                false,
            ),
            (
                "SELECT w, COUNT(*) AS n FROM df GROUP BY w ORDER BY w DESC NULLS FIRST",
                false,
            ),
            (
                "SELECT k, f, COUNT(*) AS n FROM df GROUP BY k, f ORDER BY f DESC NULLS FIRST, k",
                false,
            ),
            (
                "SELECT w % 7 AS w, COUNT(*) AS n FROM df GROUP BY w % 7 ORDER BY w",
                false,
            ),
            (
                "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g ORDER BY n",
                true,
            ),
            (
                "SELECT k, w, COUNT(*) AS n FROM df GROUP BY k, w ORDER BY k, w + 0",
                true,
            ),
            (
                "SELECT * FROM (SELECT w, COUNT(*) AS n FROM df GROUP BY w) WHERE n > 7 ORDER BY w",
                true,
            ),
            (
                "SELECT k, v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY k, g ORDER BY g",
                true,
            ),
            (
                "SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY v % 1000 ORDER BY v % 1000",
                true,
            ),
            (
                "SELECT * FROM (SELECT v % 1000 AS g, COUNT(*) AS n FROM df GROUP BY g LIMIT 300) ORDER BY g",
                true,
            ),
            (
                "SELECT g, ROW_NUMBER() OVER () AS r FROM (SELECT v % 1000 AS g FROM df GROUP BY g) ORDER BY g",
                true,
            ),
        ] {
            let mut state =
                DataTableState::from_lazyframe(df.clone().lazy(), &OpenOptions::default()).unwrap();
            state.sql_query(sql.to_string());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            assert_eq!(groups_ordered(&state.lf.logical_plan), [ordered], "{sql}");
            let mut ctx = polars_sql::SQLContext::new();
            ctx.register("df", df.clone().lazy());
            let raw = ctx.execute(sql).unwrap();
            for streaming in [false, cfg!(feature = "streaming")] {
                let read = |lf: LazyFrame| collect_lazy(lf, streaming).unwrap();
                let full = read(state.lf.clone());
                if !ordered {
                    // No ties, so polars-sql's unstable sort gives the one order too.
                    assert!(
                        full.equals_missing(&read(raw.clone())),
                        "{sql}, streaming {streaming}"
                    );
                }
                let middle = full.height() as i64 / 2;
                for offset in [0, 100, middle] {
                    let page = read(state.lf.clone().slice(offset, 200));
                    assert!(
                        page.equals_missing(&full.slice(offset, 200)),
                        "{sql}, streaming {streaming}, offset {offset}"
                    );
                }
            }
        }
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
                columns: Vec::new(),
                column: "c".to_string(),
                operator: FilterOperator::Eq,
                value: "1".to_string(),
                logical_op: LogicalOperator::And,
            },
            FilterStatement {
                columns: Vec::new(),
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
            columns: Vec::new(),
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

    /// Dates past the calendar in the second row: a date, and datetimes in ms and
    /// us with and without a zone. The first row is 1970-01-01.
    fn past_calendar_lf() -> LazyFrame {
        let paris = TimeZone::opt_try_new(Some("Europe/Paris")).unwrap();
        let datetime = |name: &str, unit, zone: Option<TimeZone>| {
            Series::new(name.into(), [0, i64::MIN + 1])
                .cast(&DataType::Datetime(unit, zone))
                .unwrap()
                .into_column()
        };
        DataFrame::new_infer_height(vec![
            Column::new("id".into(), [1i32, 2]),
            Column::new("s".into(), ["a", "b"]),
            Series::new("d".into(), [0, i32::MAX])
                .cast(&DataType::Date)
                .unwrap()
                .into_column(),
            datetime("t_ms", TimeUnit::Milliseconds, None),
            datetime("t_us", TimeUnit::Microseconds, None),
            datetime("t_ms_tz", TimeUnit::Milliseconds, paris.clone()),
            datetime("t_us_tz", TimeUnit::Microseconds, paris),
        ])
        .unwrap()
        .lazy()
    }

    const PAST_CALENDAR: [&str; 5] = ["d", "t_ms", "t_us", "t_ms_tz", "t_us_tz"];

    /// `column`'s two values as text: Polars' own for the first, and the stored
    /// number the table shows for the one past the calendar.
    fn past_calendar_text(column: &str) -> [String; 2] {
        let df = past_calendar_lf().collect().unwrap();
        let values = df.column(column).unwrap();
        let first = values
            .slice(0, 1)
            .cast(&DataType::String)
            .unwrap()
            .str()
            .unwrap()
            .get(0)
            .unwrap()
            .to_string();
        let past = crate::exact::past_calendar_text(&values.get(1).unwrap()).unwrap();
        [first, past]
    }

    /// Pivoted on a date past the calendar, its new column is named by the stored
    /// number, the columns still in date order; Polars' own naming panicked (#506).
    /// Without one, the columns are named as they always were.
    #[test]
    fn a_pivot_on_a_date_past_the_calendar_names_it_by_its_stored_number() {
        for on in PAST_CALENDAR {
            let [first, past] = past_calendar_text(on);
            for with_past in [true, false] {
                let lf = past_calendar_lf()
                    .filter(col("id").eq(lit(1)).or(lit(with_past)))
                    .select([col("id"), col(on), col("s")]);
                let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
                state
                    .pivot(&PivotSpec {
                        index: vec!["id".to_string()],
                        pivot_column: on.to_string(),
                        value_column: "s".to_string(),
                        aggregation: PivotAggregation::First,
                        sort_columns: None,
                    })
                    .unwrap();
                let df = state.lf.clone().collect().unwrap();
                let names: Vec<&str> = df.get_column_names().iter().map(|n| n.as_str()).collect();
                let dates = match (with_past, on) {
                    (false, _) => vec![first.as_str()],
                    // i32::MAX days is after 1970, i64::MIN + 1 before it.
                    (true, "d") => vec![first.as_str(), past.as_str()],
                    (true, _) => vec![past.as_str(), first.as_str()],
                };
                assert_eq!(names[1..], dates, "{on}");
                assert_eq!(
                    df.column(&first).unwrap().str().unwrap().get(0),
                    Some("a"),
                    "{on}"
                );
                if with_past {
                    assert_eq!(
                        df.column(&past).unwrap().str().unwrap().get(1),
                        Some("b"),
                        "{on}"
                    );
                }
            }
        }
    }

    /// Melted with text, a date past the calendar is its stored number, where the
    /// cast to text panicked (#506), and one in range Polars' own text. Melted with
    /// dates only, the values stay dates.
    #[test]
    fn a_melt_of_dates_with_text_writes_a_date_past_the_calendar_as_its_number() {
        let melt = |columns: [&str; 2]| {
            let mut state =
                DataTableState::new(past_calendar_lf(), None, None, None, None, true).unwrap();
            state
                .melt(&MeltSpec {
                    index: vec!["id".to_string()],
                    value_columns: columns.map(String::from).to_vec(),
                    variable_name: "variable".to_string(),
                    value_name: "value".to_string(),
                })
                .unwrap();
            state.lf.clone().collect().unwrap()
        };
        for column in PAST_CALENDAR {
            let [first, past] = past_calendar_text(column);
            let df = melt([column, "s"]);
            let values: Vec<Option<&str>> =
                df.column("value").unwrap().str().unwrap().iter().collect();
            assert_eq!(
                values,
                [
                    Some(first.as_str()),
                    Some(past.as_str()),
                    Some("a"),
                    Some("b")
                ],
                "{column}"
            );
        }
        let df = melt(["t_ms", "t_us"]);
        assert!(matches!(
            df.column("value").unwrap().dtype(),
            DataType::Datetime(..)
        ));
    }

    /// SQL's three plan rewrites compose: in a join filtered by an `IN` subquery, a
    /// date past the calendar met with text is its stored number (#506), the join
    /// keeps one row order (#508), and the subquery's values are read once (#509).
    #[cfg(feature = "sql")]
    #[test]
    fn a_sql_join_with_an_in_subquery_and_a_date_past_the_calendar() {
        use polars::lazy::dsl::DslPlan;
        for c in PAST_CALENDAR {
            let [first, past] = past_calendar_text(c);
            let sql = format!(
                "SELECT a.id, COALESCE(a.{c}, b.s) AS x FROM df a JOIN df b ON a.id = b.id \
                 WHERE a.id IN (SELECT t.id FROM df t WHERE t.s <> 'z')"
            );
            let mut state =
                DataTableState::new(past_calendar_lf(), None, None, None, None, true).unwrap();
            state.sql_query(sql.clone());
            assert!(state.error.is_none(), "{sql}: {:?}", state.error);
            let plan = &state.lf.logical_plan;
            assert!(!plan.into_iter().any(asks_per_row), "{sql}");
            let joins: Vec<_> = plan
                .into_iter()
                .filter_map(|node| match node {
                    DslPlan::Join { options, .. } => Some(options.args.maintain_order),
                    _ => None,
                })
                .collect();
            assert!(!joins.is_empty(), "{sql}");
            assert!(
                joins.iter().all(|order| *order != MaintainOrderJoin::None),
                "{sql}"
            );
            for streaming in [false, cfg!(feature = "streaming")] {
                let df = collect_lazy(state.lf.clone(), streaming).unwrap();
                let ids: Vec<Option<i32>> =
                    df.column("id").unwrap().i32().unwrap().iter().collect();
                assert_eq!(ids, [Some(1), Some(2)], "{sql}, streaming {streaming}");
                let x: Vec<Option<&str>> = df.column("x").unwrap().str().unwrap().iter().collect();
                assert_eq!(
                    x,
                    [Some(first.as_str()), Some(past.as_str())],
                    "{sql}, streaming {streaming}"
                );
                let page = collect_lazy(state.lf.clone().slice(1, 1), streaming).unwrap();
                assert!(page.equals_missing(&df.slice(1, 1)), "{sql}");
            }
        }
    }

    /// A q query forgets the melt it replaces; rolled back, the melt is
    /// what SQL runs against again, not only what the table shows.
    #[test]
    fn a_rollback_brings_back_the_melt_a_query_forgot() {
        let lf = create_melt_wide_lf();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        let spec = MeltSpec {
            index: vec!["id".to_string(), "date".to_string()],
            value_columns: vec!["c1".to_string(), "c2".to_string(), "c3".to_string()],
            variable_name: "variable".to_string(),
            value_name: "value".to_string(),
        };
        state.melt(&spec).unwrap();
        let saved = state.rollback_point();
        state.query("select id".to_string());
        assert!(state.last_melt_spec().is_none());
        state.roll_back(saved);
        assert!(state.last_melt_spec().is_some());
        assert_eq!(state.query_root().collect().unwrap().height(), 9);
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
            columns: Vec::new(),
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

    /// The one-pass pivot gives what the lazy pivot over the whole view gave, for every
    /// aggregation, with nulls in the index, the pivot column and the values, pairs with
    /// no rows, and more rows than one streaming morsel, so `first` and `last` are
    /// checked for order across batches.
    #[test]
    fn a_pivot_in_one_pass_matches_the_lazy_pivot() {
        let n = 250_000usize;
        let view = df!(
            "g" => (0..n)
                .map(|i| (i % 13 != 0).then_some((i % 97) as i64))
                .collect::<Vec<_>>(),
            "key" => (0..n)
                .map(|i| (i % 17 != 0).then(|| format!("k{}", (i * 7) % 11)))
                .collect::<Vec<_>>(),
            "v" => (0..n)
                .map(|i| (i % 7 != 0).then_some((i % 1_000) as f64))
                .collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy()
        // Pairs with no rows at all: `k3` never meets a `g` divisible by five.
        .filter(
            (col("g") % lit(5i64))
                .neq(lit(0i64))
                .or(col("key").neq(lit("k3")))
                .fill_null(lit(true)),
        );

        // The pivot as it ran before: the new columns read first, then the lazy pivot.
        let lazy_pivot = |spec: &PivotSpec| {
            let on = spec.pivot_column.as_str();
            let value = spec.value_column.as_str();
            let on_columns = view
                .clone()
                .select([col(on)])
                .unique(None, UniqueKeepStrategy::Any)
                .sort([on], SortMultipleOptions::default().with_nulls_last(true))
                .collect()
                .unwrap();
            let index = if spec.index.is_empty() {
                all() - by_name([on, value], true, false)
            } else {
                by_name(spec.index.iter().map(String::as_str), true, false)
            };
            view.clone()
                .pivot(
                    by_name([on], true, false),
                    Arc::new(on_columns),
                    index,
                    by_name([value], true, false),
                    pivot_agg_expr(spec.aggregation, element()),
                    true,
                    PlSmallStr::from_static("_"),
                    PivotColumnNaming::Auto,
                )
                .collect()
                .unwrap()
        };
        let close = |a: &DataFrame, b: &DataFrame| {
            a.get_column_names() == b.get_column_names()
                && a.height() == b.height()
                && a.columns().iter().zip(b.columns()).all(|(x, y)| {
                    if x.dtype().is_float() {
                        let (x, y) = (x.f64().unwrap(), y.f64().unwrap());
                        x.iter().zip(y.iter()).all(|pair| match pair {
                            (Some(x), Some(y)) => (x - y).abs() <= 1e-9 * x.abs().max(1.0),
                            (x, y) => x.is_none() && y.is_none(),
                        })
                    } else {
                        x.as_materialized_series()
                            .equals_missing(y.as_materialized_series())
                    }
                })
        };

        for index in [vec!["g".to_string()], Vec::new()] {
            for aggregation in PivotAggregation::ALL {
                let spec = PivotSpec {
                    index: index.clone(),
                    pivot_column: "key".to_string(),
                    value_column: "v".to_string(),
                    aggregation,
                    sort_columns: None,
                };
                let expected = lazy_pivot(&spec);
                for streaming in [false, true] {
                    let pivoted = PivotJob {
                        view: view.clone(),
                        spec: spec.clone(),
                        streaming,
                    }
                    .run()
                    .unwrap();
                    assert!(
                        close(&pivoted, &expected),
                        "{aggregation:?}, index {index:?}, streaming {streaming}:\n\
                         {pivoted:?}\n{expected:?}"
                    );
                }
            }
        }
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

    /// What the Search hint promises: every word's letters in order, in any text
    /// column. Each word may match a different column; letters out of order do not.
    #[test]
    fn search_matches_every_words_letters_in_order_in_any_text_column() {
        let rows = |query: &str| {
            let lf = df!(
                "name" => &["Smith", "Marion", "Smith"],
                "city" => &["London", "London", "Paris"]
            )
            .unwrap()
            .lazy();
            let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
            state.fuzzy_search(query.to_string());
            assert!(state.error.is_none(), "{:?}", state.error);
            state.lf.clone().collect().unwrap().height()
        };
        assert_eq!(rows("smth"), 2, "letters in order, not adjacent, any case");
        assert_eq!(rows("smth ldn"), 1, "every word, each in its own column");
        assert_eq!(rows("htims"), 0, "letters out of order");
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
        // What the `,` toggle does: same settings, enabled = false.
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
        let df = df!("blob" => &[binary_stub()]).unwrap();
        let area = Rect::new(0, 0, 10, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        assert!(row_string(&buf, area, 1).starts_with(binary_stub()));
    }

    /// The buffer holds a binary column as stub text; the type row still says binary.
    #[test]
    fn a_binary_column_s_type_row_says_binary() {
        let table = DataTable {
            binary_cols: std::collections::HashSet::from(["blob".to_string()]),
            dtype_row: true,
            ..table_with_format("thousands", true)
        };
        let df = df!("blob" => &[binary_stub()], "s" => &["x"]).unwrap();
        let area = Rect::new(0, 0, 30, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let types = row_string(&buf, area, 1);
        assert!(types.contains("binary"), "{types:?}");
        assert_eq!(types.matches("str").count(), 1, "{types:?}");
    }

    /// A line break or a tab in a value is marked in the one-line cell; drawn as
    /// is, ratatui drops it and `line1\nline2` reads `line1line2`.
    #[test]
    fn breaks_tabs_and_controls_are_marked_in_a_cell() {
        let table = DataTable::default();
        let df = df!("s" => ["line1\nline2", "tab\tseparated", "esc\u{1b}[0m"]).unwrap();
        let area = Rect::new(0, 0, 30, 4);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let g = table.glyphs;
        assert!(
            row_string(&buf, area, 1).starts_with(&format!("line1{}line2", g.newline_mark)),
            "{:?}",
            row_string(&buf, area, 1)
        );
        assert!(row_string(&buf, area, 2).starts_with(&format!("tab{}separated", g.tab_mark)));
        assert!(row_string(&buf, area, 3).starts_with(&format!("esc{}[0m", g.control_mark)));
    }

    /// A direction control in a value is marked too: drawn, a terminal that lays
    /// out bidirectional text would reverse the rest of the row.
    #[test]
    fn direction_controls_are_marked_in_a_cell() {
        let table = DataTable::default();
        let df = df!("s" => ["a\u{202e}evil\u{202c}z"]).unwrap();
        let area = Rect::new(0, 0, 30, 3);
        let mut buf = Buffer::empty(area);
        table.render_dataframe(&df, area, &mut buf, &mut TableState::default(), false, 0);
        let m = table.glyphs.control_mark;
        let row = row_string(&buf, area, 1);
        assert!(row.starts_with(&format!("a{m}evil{m}z")), "{row:?}");
        assert!(
            !buf.content()
                .iter()
                .any(|c| c.symbol().contains('\u{202e}'))
        );
    }

    /// A cell measures only the start of a huge value, and says it goes on.
    #[test]
    fn a_huge_value_is_measured_by_its_start() {
        let table = DataTable::default();
        let huge = "x".repeat(crate::exact::CELL_PREVIEW_BYTES * 4);
        let df = df!("s" => [huge.as_str()]).unwrap();
        let mut scratch = String::new();
        let slice = table.slice_column(&df, 0, 1, &HashSet::new(), &mut scratch);
        let ellipsis = crate::glyphs::cell_width(table.glyphs.ellipsis);
        assert_eq!(
            usize::from(slice.value_width),
            crate::exact::CELL_PREVIEW_BYTES + ellipsis
        );
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
        // Part of the heading should be visible so the user knows what the column is,
        // behind the clip marker (three cells in the ASCII set).
        assert!(
            header_row_string(&buf, area).contains("wid"),
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
            "blob" => &[binary_stub(), binary_stub()],
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

        let fitted = state.fill_plan(buffer_start, 1000, 1000, true).fit(df);
        let (sliced, eff_start) = (fitted.df, fitted.start);
        let eff_end = eff_start + sliced.height();
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

    /// The address range of every buffer behind `arr`: values, validity, offsets, string
    /// views and data, and nested children. Panics on a type it cannot walk, so a test
    /// cannot pass by skipping a column.
    fn array_storage(arr: &dyn polars_arrow::array::Array, out: &mut Vec<(usize, usize)>) {
        use polars_arrow::array::{
            BinaryViewArray, BooleanArray, FixedSizeListArray, ListArray, NullArray,
            PrimitiveArray, StructArray, Utf8ViewArray,
        };
        fn push<T>(out: &mut Vec<(usize, usize)>, s: &[T]) {
            if !s.is_empty() {
                let start = s.as_ptr() as usize;
                out.push((start, start + std::mem::size_of_val(s)));
            }
        }
        let any = arr.as_any();
        // A null array's validity is Polars' shared zeroed bitmap, owned by no frame.
        if any.downcast_ref::<NullArray>().is_some() {
            return;
        }
        if let Some(validity) = arr.validity() {
            push(out, validity.as_slice().0);
        }
        macro_rules! primitive {
            ($($t:ty),*) => {$(
                if let Some(a) = any.downcast_ref::<PrimitiveArray<$t>>() {
                    return push(out, a.values().as_slice());
                }
            )*};
        }
        primitive!(i8, i16, i32, i64, i128, u8, u16, u32, u64, f32, f64);
        if let Some(a) = any.downcast_ref::<BooleanArray>() {
            push(out, a.values().as_slice().0);
        } else if let Some(a) = any.downcast_ref::<Utf8ViewArray>() {
            push(out, a.views().as_slice());
            a.data_buffers()
                .iter()
                .for_each(|b| push(out, b.as_slice()));
        } else if let Some(a) = any.downcast_ref::<BinaryViewArray>() {
            push(out, a.views().as_slice());
            a.data_buffers()
                .iter()
                .for_each(|b| push(out, b.as_slice()));
        } else if let Some(a) = any.downcast_ref::<ListArray<i64>>() {
            push(out, a.offsets().as_slice());
            array_storage(a.values().as_ref(), out);
        } else if let Some(a) = any.downcast_ref::<FixedSizeListArray>() {
            array_storage(a.values().as_ref(), out);
        } else if let Some(a) = any.downcast_ref::<StructArray>() {
            a.values()
                .iter()
                .for_each(|v| array_storage(v.as_ref(), out));
        } else {
            panic!("no storage walk for {:?}", arr.dtype());
        }
    }

    fn frame_storage(df: &DataFrame) -> Vec<(usize, usize)> {
        let mut out = Vec::new();
        for column in df.columns() {
            for chunk in column.as_materialized_series().chunks() {
                array_storage(chunk.as_ref(), &mut out);
            }
        }
        out
    }

    /// Whether any buffer behind `held` lies inside one behind `source`.
    fn shares_storage(held: &DataFrame, source: &DataFrame) -> bool {
        let source = frame_storage(source);
        frame_storage(held)
            .iter()
            .any(|&(s, e)| source.iter().any(|&(ss, se)| s < se && ss < e))
    }

    /// Every kind of column a buffer holds: fixed width, booleans, short and long
    /// strings, binary, nulls in each, a categorical, an enum, a datetime, a decimal, a
    /// list of strings, a fixed-size array, a struct and an all-null column.
    fn mixed_frame(start: usize, end: usize) -> DataFrame {
        let rows = start..end;
        let long = |i: usize| format!("{i:>8}-{}", "x".repeat(40));
        let mut df = df!(
            "id" => rows.clone().map(|i| i as i64).collect::<Vec<_>>(),
            "maybe" => rows.clone().map(|i| (i % 3 != 0).then_some(i as f64)).collect::<Vec<_>>(),
            "flag" => rows.clone().map(|i| (i % 5 != 0).then_some(i % 2 == 0)).collect::<Vec<_>>(),
            "short" => rows.clone().map(|i| format!("s{}", i % 100)).collect::<Vec<_>>(),
            "long" => rows.clone().map(|i| (i % 7 != 0).then(|| long(i))).collect::<Vec<_>>(),
        )
        .unwrap();
        let n = end - start;
        let cat = df
            .column("short")
            .unwrap()
            .cast(&DataType::from_categories(Categories::global()))
            .unwrap()
            .with_name("cat".into());
        let when = df
            .column("id")
            .unwrap()
            .cast(&DataType::Datetime(TimeUnit::Milliseconds, None))
            .unwrap()
            .with_name("when".into());
        let list: ListChunked = rows
            .clone()
            .map(|i| Some(Series::new("".into(), [long(i), format!("t{i}")])))
            .collect();
        let nested = StructChunked::from_columns(
            "nested".into(),
            n,
            &[
                df.column("id").unwrap().clone(),
                df.column("long").unwrap().clone(),
            ],
        )
        .unwrap();
        let labels: Vec<String> = (0..100).map(|i| format!("s{i}")).collect();
        let labels = FrozenCategories::new(labels.iter().map(String::as_str)).unwrap();
        let label = df
            .column("short")
            .unwrap()
            .cast(&DataType::from_frozen_categories(labels))
            .unwrap()
            .with_name("label".into());
        let bytes = df
            .column("long")
            .unwrap()
            .cast(&DataType::Binary)
            .unwrap()
            .with_name("bytes".into());
        let price = df
            .column("maybe")
            .unwrap()
            .cast(&DataType::Decimal(18, 2))
            .unwrap()
            .with_name("price".into());
        let tags = list.with_name("tags".into()).into_column();
        let pair = tags
            .cast(&DataType::Array(Box::new(DataType::String), 2))
            .unwrap()
            .with_name("pair".into());
        for column in [
            cat,
            label,
            bytes,
            when,
            price,
            tags,
            pair,
            nested.into_column(),
        ] {
            df.with_column(column).unwrap();
        }
        df.with_column(Series::new_null("nothing".into(), n).into_column())
            .unwrap();
        df
    }

    #[test]
    fn compacted_rows_own_their_storage() {
        // A slice of a fill keeps the fill's buffers, rechunked or not; the compacted
        // rows share none of them and read the same.
        let source = mixed_frame(0, 20_000);
        let slice = source.slice(1_000, 1_000);
        assert!(shares_storage(&slice, &source), "the probe sees a slice");
        let mut rechunked = slice.clone();
        rechunked.rechunk_mut();
        assert!(
            shares_storage(&rechunked, &source),
            "a lone chunk rechunked is still the slice"
        );

        let kept = compact_rows(source.clone(), 1_000, 1_000, None);
        assert!(!shares_storage(&kept, &source));
        assert!(kept.equals_missing(&slice));
        assert_eq!(kept.schema(), source.schema());

        // Chunks a slice keeps whole are sliced; past a quarter more, the rows are copied.
        let mut chunked = mixed_frame(0, 1_000);
        for i in 1..4 {
            chunked
                .vstack_mut(&mixed_frame(i * 1_000, (i + 1) * 1_000))
                .unwrap();
        }
        let chunk = |i: usize| chunked.slice(i as i64 * 1_000, 1_000);
        let sliced = trim_rows(chunked.clone(), 1_000, 2_000, None);
        assert!(shares_storage(&sliced, &chunk(1)) && shares_storage(&sliced, &chunk(2)));
        assert!(!shares_storage(&sliced, &chunk(0)) && !shares_storage(&sliced, &chunk(3)));
        let copied = trim_rows(chunked.clone(), 1_500, 1_000, None);
        assert!((0..4).all(|i| !shares_storage(&copied, &chunk(i))));
        assert!(copied.equals_missing(&chunked.slice(1_500, 1_000)));

        // Two chunks: rows from one of them, and rows across both. Across a seam, the
        // rows either side of it are copied apart.
        let mut stitched = mixed_frame(0, 3_000);
        stitched.vstack_mut(&mixed_frame(3_000, 6_000)).unwrap();
        for (offset, len) in [(3_500, 1_000), (2_500, 1_000), (0, 6_000)] {
            for seam in [None, Some(3_000)] {
                let kept = compact_rows(stitched.clone(), offset, len, seam);
                assert!(!shares_storage(&kept, &stitched), "{offset}+{len}");
                assert!(kept.equals_missing(&stitched.slice(offset as i64, len)));
                assert_eq!(kept.schema(), stitched.schema());
                let across = seam.is_some() && offset < 3_000 && 3_000 < offset + len;
                let chunks = if across { 2 } else { 1 };
                assert!(
                    kept.columns()
                        .iter()
                        .filter_map(Column::as_series)
                        .all(|s| s.n_chunks() == chunks),
                    "{offset}+{len} {seam:?}"
                );
            }
        }

        // A constant column is cut to length, not written out a row at a time.
        let constant = "k".repeat(200);
        let with_constant = mixed_frame(0, 2_000)
            .lazy()
            .with_column(lit(constant.as_str()).alias("constant"))
            .collect()
            .unwrap();
        assert!(matches!(
            with_constant.column("constant").unwrap(),
            Column::Scalar(_)
        ));
        let kept = compact_rows(with_constant.clone(), 500, 1_000, None);
        let Column::Scalar(cut) = kept.column("constant").unwrap() else {
            panic!("the constant column was built out");
        };
        assert_eq!(cut.len(), 1_000);
        assert!(kept.equals_missing(&with_constant.slice(500, 1_000)));
    }

    /// A state with a 1 MB byte budget and the first column locked.
    fn trimming_state(lf: LazyFrame) -> DataTableState {
        let mut state = DataTableState::new(lf, None, None, None, Some(1), true).unwrap();
        state.locked_columns_count = 1;
        state.visible_rows = 40;
        state
    }

    fn assert_view_rows(state: &DataTableState, source: &DataFrame) {
        let (start, end) = (state.buffered_start(), state.buffered_end());
        assert!(start <= state.start_row && state.start_row + 40 <= end);
        let held = state.buffered_df.as_ref().unwrap();
        assert_eq!(held.height(), end - start);
        assert!(held.equals_missing(&source.slice(start as i64, end - start)));
        let id = |df: &DataFrame, row: usize| df.column("id").unwrap().i64().unwrap().get(row);
        let locked = state.locked_df.as_ref().unwrap();
        assert_eq!(locked.get_column_names(), ["id"]);
        assert_eq!(
            id(locked, state.start_row - start),
            Some(state.start_row as i64)
        );
        let shown = state.df.as_ref().unwrap();
        assert_eq!(shown.height(), held.height());
        assert!(
            shown.column("id").is_err(),
            "the locked column is not repeated"
        );
        for frame in [held, locked, shown] {
            assert!(!shares_storage(frame, source), "trimmed rows are let go");
        }
    }

    #[test]
    fn a_trimmed_fill_lets_go_of_the_rows_it_drops() {
        const N: usize = 10_000;
        let source = mixed_frame(0, N);
        let budget_rows = 1024 * 1024 / (source.estimated_size() / N);
        assert!(budget_rows < N * 3 / 4, "the budget trims the fill");

        // Asynchronous: a plain fill over the budget.
        let mut state = trimming_state(source.clone().lazy());
        state.num_rows = N;
        state.num_rows_valid = true;
        state.start_row = 7_500;
        // The worker makes the copy; the UI thread installs it as it is.
        let plan = state.fill_plan(0, N, N, true);
        let fill = source.clone();
        let result = std::thread::spawn(move || {
            let result = plan.fit(fill);
            assert_eq!(compactions(), 1, "the worker copies the rows it keeps");
            result
        })
        .join()
        .unwrap();
        let before = compactions();
        state.apply_async_collect(result);
        assert_eq!(compactions(), before, "the install copies nothing");
        assert!(state.buffered_end() - state.buffered_start() <= budget_rows);
        assert_view_rows(&state, &source);

        // Synchronous: the same fill collected on the spot from an eager source, which
        // stays held by the frame itself; the buffer's copy does not. A binary column
        // is drawn as a stub there, so it is left out.
        let source = source.drop("bytes").unwrap();
        let mut state = trimming_state(source.clone().lazy());
        state.num_rows = N;
        state.num_rows_valid = true;
        state.start_row = 300;
        state.load_buffer(0, N);
        assert!(state.error.is_none());
        assert_view_rows(&state, &source);
    }

    #[test]
    fn a_fill_cut_around_a_view_since_left_is_not_installed() {
        // The worker cuts around the view the fill was planned for. A view that jumped
        // past the rows kept, while it was out, keeps what is held and asks again. So
        // does a read that came back short, whose end the cut dropped.
        const N: usize = 10_000;
        let source = mixed_frame(0, N);
        for short in [false, true] {
            let mut state = trimming_state(source.clone().lazy());
            state.num_rows = N;
            state.num_rows_valid = !short;
            state.start_row = 300;
            let asked = if short { N + 5_000 } else { N };
            let result = state.fill_plan(0, asked, asked, !short).fit(source.clone());
            assert!(
                result.start + result.df.height() < 9_000,
                "the fill was cut"
            );
            state.start_row = 9_000;
            state.needs_recollect = false;
            state.apply_async_collect(result);
            assert!(
                state.buffered_df.is_none(),
                "nothing drawn under wrong numbers (short {short})"
            );
            assert!(state.needs_recollect);
            assert_eq!((state.num_rows, state.num_rows_valid), (N, true));
        }
    }

    #[test]
    fn an_untrimmed_fill_is_kept_as_collected() {
        // Ordinary paging copies nothing: the buffer is the fill itself.
        let source = mixed_frame(0, 200);
        let mut state = trimming_state(source.clone().lazy());
        state.land(Fill {
            df: source.clone(),
            buffer_start: 0,
            buffer_end: 200,
            num_rows: 200,
            count_known: true,
        });
        let held = state.buffered_df.as_ref().unwrap();
        assert_eq!(frame_storage(held), frame_storage(&source));
    }

    #[test]
    fn a_stitched_trim_lets_go_of_both_groups() {
        // Forward and back across a row group boundary: the union is trimmed to the
        // cap, and neither fetched group is held behind the kept rows. The copy is the
        // worker's; installing it copies nothing. Leaving the stitched rows cuts the
        // buffer down to one group and lets the other go, again without a copy.
        const G: usize = 1_000_000;
        const CAP: usize = DEFAULT_MAX_BUFFERED_ROWS;
        let rows = |start: usize, end: usize| {
            Fill {
            df: df!(
                "id" => (start as i64..end as i64).collect::<Vec<i64>>(),
                "name" => (start..end).map(|i| format!("{i:>8}-{}", "y".repeat(24))).collect::<Vec<_>>(),
            )
            .unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 10 * G,
            count_known: true,
        }
        };
        let check = |state: &DataTableState, fetched: &[&DataFrame]| {
            let (start, end) = (state.buffered_start(), state.buffered_end());
            assert!(start <= state.start_row && state.start_row + 40 <= end);
            assert_eq!(end - start, CAP, "trimmed back to the cap");
            let held = state.buffered_df.as_ref().unwrap();
            let ids = held.column("id").unwrap().i64().unwrap();
            assert_eq!(ids.get(0), Some(start as i64));
            assert_eq!(ids.get(CAP - 1), Some(end as i64 - 1));
            for df in fetched {
                assert!(!shares_storage(held, df));
                assert!(!shares_storage(state.locked_df.as_ref().unwrap(), df));
                assert!(!shares_storage(state.df.as_ref().unwrap(), df));
            }
        };
        let lf = rows(0, 1).df.lazy();
        for forward in [true, false] {
            let mut state = DataTableState::new(lf.clone(), None, None, None, None, true)
                .unwrap()
                .with_open(OpenFacts {
                    remote_source: true,
                    row_groups: vec![vec![G; 10]],
                    ..Default::default()
                });
            state.locked_columns_count = 1;
            state.visible_rows = 40;
            let (first, view) = if forward {
                (G - 60, G - 20)
            } else {
                (G + 20, G - 20)
            };
            assert!(state.scroll_to(first));
            let request = state.prepare_async_collect(None).expect("one group");
            let first = rows(request.buffer_start, request.buffer_end);
            let first_df = first.df.clone();
            state.land(first);

            assert!(state.scroll_to(view));
            let request = state.prepare_async_collect(None).expect("the other group");
            assert_eq!(
                request.buffer_start == G,
                forward,
                "fetched alone, to stitch on"
            );
            let second = rows(request.buffer_start, request.buffer_end);
            let second_df = second.df.clone();
            // Fit on a worker, as the app does, against the rows held when planned.
            let plan = request.plan;
            let result = std::thread::spawn(move || {
                let result = plan.fit(second.df);
                assert!(compactions() > 0, "the worker copies the rows it keeps");
                result
            })
            .join()
            .unwrap();
            let before = compactions();
            state.apply_async_collect(result);
            assert_eq!(compactions(), before, "the install copies nothing");
            check(&state, &[&first_df, &second_df]);

            // A window inside one group: the rows of the other are let go.
            let stitched = state.buffered_df.clone().unwrap();
            let (start, end) = (state.buffered_start(), state.buffered_end());
            let (start, end, other) = if forward {
                (G, end, stitched.slice(0, G - start))
            } else {
                (start, G, stitched.slice((G - start) as i64, end - G))
            };
            assert!(state.scroll_to(if forward { G } else { G - 40 }));
            assert!(state.holds_buffer(start, end));
            assert_eq!(compactions(), before, "the cut falls on the seam: a slice");
            state.slice_buffer_into_display();
            assert_eq!((state.buffered_start(), state.buffered_end()), (start, end));
            let held = state.buffered_df.as_ref().unwrap();
            assert_eq!(held.height(), end - start);
            assert!(!shares_storage(held, &other));
            assert!(!shares_storage(state.df.as_ref().unwrap(), &other));
            let ids = held.column("id").unwrap().i64().unwrap();
            assert_eq!(ids.get(0), Some(state.buffered_start() as i64));
        }
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

    /// A dataset written a file a day is mostly empty files in its quiet years; a window
    /// opens only the files with rows, and the limit counts only those (#659).
    #[test]
    fn a_window_passes_over_empty_files() {
        // Every other file empty: file 2i holds rows 10i..10i+10.
        let offsets: Vec<usize> = (0..=1000_usize).map(|i| i.div_ceil(2) * 10).collect();
        let (first, last) = files_holding(&offsets, 0, 40).unwrap();
        assert_eq!((first, last), (0, 6));
        assert_eq!(files_with_rows(&offsets, first, last), vec![0, 2, 4, 6]);
        let (start, end) = limit_files(&offsets, 0, 40, 0, 2_000, 16);
        let (first, last) = files_holding(&offsets, start, end - start).unwrap();
        assert_eq!(
            files_with_rows(&offsets, first, last).len(),
            16,
            "sixteen files with rows, not eight and the empty ones between"
        );
    }

    /// The two checks that read footers rather than values: a column a file never had,
    /// and a column a file holds in a type the scan cannot read.
    ///
    /// Both are invisible to every measurement over values — an absent cell arrives as
    /// a null and a conflicting one is not read at all — so the only way to test them
    /// is through a dataset whose files genuinely disagree.
    #[test]
    fn absent_columns_and_type_conflicts_are_measured_from_the_footers() {
        use crate::data_quality::{DataQualityPlan, ObservationKind, QualityCompute, QualityScope};
        use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
        use polars::prelude::{DataType, IntoLazy, df};

        let urls: Vec<String> = vec!["a".to_string(), "b".to_string(), "c".to_string()];
        // `a` agrees with the schema; `b` holds `n` as text, which the scan cannot read
        // as the Int64 the majority wrote; `c` has no `fee` column at all.
        let scan: FileScan = Arc::new(move |urls: &[String], as_text: &[PlSmallStr]| {
            let reading_text = as_text.contains(&PlSmallStr::from("n"));
            let frames: Vec<LazyFrame> = urls
                .iter()
                .map(|url| match url.as_str() {
                    "a" => df!(
                        "id" => &[0i64, 1, 2],
                        "n" => &[10i64, 20, 30],
                        "fee" => &[1.5f64, 2.5, 3.5],
                        crate::schema_union::DRIFT_COLUMN => &[0u32, 1, 2],
                    )
                    .unwrap()
                    .lazy()
                    .with_column(col("n").cast(if reading_text {
                        DataType::String
                    } else {
                        DataType::Int64
                    })),
                    "b" => {
                        let frame = df!(
                            "id" => &[3i64, 4],
                            "n" => &["sixty", "seventy"],
                            "fee" => &[4.5f64, 5.5],
                            crate::schema_union::DRIFT_COLUMN => &[3u32, 4],
                        )
                        .unwrap()
                        .lazy();
                        if reading_text {
                            frame
                        } else {
                            // Not read from this file at all, as the real scan leaves it.
                            frame.with_column(lit(NULL).cast(DataType::Int64).alias("n"))
                        }
                    }
                    _ => df!(
                        "id" => &[5i64, 6],
                        "n" => &[50i64, 60],
                        crate::schema_union::DRIFT_COLUMN => &[5u32, 6],
                    )
                    .unwrap()
                    .lazy()
                    // A column the file never had reads as null, which is exactly why
                    // no measurement over values can tell it from one.
                    .with_column(lit(NULL).cast(DataType::Float64).alias("fee"))
                    .select([
                        col("id"),
                        col("n").cast(if reading_text {
                            DataType::String
                        } else {
                            DataType::Int64
                        }),
                        col("fee"),
                        col(crate::schema_union::DRIFT_COLUMN),
                    ]),
                })
                .collect();
            polars::prelude::concat(frames, Default::default())
        });

        let dataset: DatasetSchema = union_file_schemas(
            &[
                file_schema(
                    &[
                        ("id", DataType::Int64),
                        ("n", DataType::Int64),
                        ("fee", DataType::Float64),
                    ],
                    3,
                ),
                file_schema(
                    &[
                        ("id", DataType::Int64),
                        ("n", DataType::String),
                        ("fee", DataType::Float64),
                    ],
                    2,
                ),
                file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 2),
            ],
            SchemaOrigin::AllFooters(3),
        );
        let state = DataTableState::from_schema_and_lazyframe(
            dataset.schema.clone(),
            scan(&urls, &[]).unwrap(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(urls.clone()),
                scan,
                count: Arc::new(|_| Ok(vec![vec![3], vec![2], vec![2]])),
                offsets: None,
            }),
            dataset: Some(DatasetAtOpen {
                schema: dataset,
                file_rows: vec![3, 2, 2],
                files: urls.clone(),
            }),
            ..Default::default()
        });
        assert!(state.drifts(), "the three files do not agree");

        let (lf, source) = state.data_quality_source_scan();
        let mut source = source.expect("every file is counted, so rows map to files");
        source.conflict_scan = state.quality_conflict_scan();
        let lf = crate::data_quality::prepare_source_quality_scan(lf, Some(&source)).unwrap();
        let plan = DataQualityPlan {
            scope: QualityScope::WholeSource,
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results =
            crate::data_quality::compute_data_quality(&lf, Some(7), &plan, Some(&source), false)
                .unwrap();

        let absent = results
            .observations
            .iter()
            .find(|observation| observation.kind == ObservationKind::Absent)
            .expect("`fee` is absent from the third file");
        assert_eq!(absent.column, "fee");
        assert_eq!(
            (absent.affected_rows, absent.evaluated_rows),
            (2, 7),
            "the third file's two rows, out of the source's seven"
        );
        assert_eq!(
            absent
                .files
                .iter()
                .map(|file| file.number)
                .collect::<Vec<_>>(),
            vec![3],
            "named by the number the Scope page gives it"
        );
        assert_eq!(
            absent.fact, "1 of 3 files has no such column",
            "every footer was read, so the count is a total rather than a floor"
        );

        let conflict = results
            .observations
            .iter()
            .find(|observation| observation.kind == ObservationKind::TypeConflict)
            .expect("`n` is text in the second file");
        assert_eq!(conflict.column, "n");
        assert_eq!((conflict.affected_rows, conflict.evaluated_rows), (2, 7));
        let file = conflict.files.first().expect("the file that disagrees");
        assert_eq!(file.number, 2);
        assert_eq!(file.stored_type.as_deref(), Some("str"));
        assert_eq!(
            file.examples,
            vec!["sixty".to_string(), "seventy".to_string()],
            "the values the conflict hides, read at the type that file wrote"
        );

        // The same two checks at the budget that reads no values at all: the footers
        // were read when the dataset opened, so there is nothing left to pay for.
        let metadata = crate::data_quality::compute_data_quality(
            &lf,
            Some(7),
            &DataQualityPlan {
                scope: QualityScope::WholeSource,
                compute: QualityCompute::Metadata,
                ..DataQualityPlan::default()
            },
            Some(&source),
            false,
        )
        .unwrap();
        assert_eq!(metadata.evaluated_rows, 0, "no value was read");
        assert_eq!(
            metadata
                .observations
                .iter()
                .map(|observation| (observation.kind, observation.affected_rows))
                .collect::<Vec<_>>(),
            vec![
                (ObservationKind::Absent, 2),
                (ObservationKind::TypeConflict, 2),
            ],
            "both are reported without reading a value"
        );

        // The drill-in is the files themselves: an absent cell has no value to filter.
        let scope = absent.evidence_scope().expect("a scope, not a predicate");
        assert_eq!(scope, QualityScope::SourceFiles(vec![3]));
        assert!(absent.evidence_predicate().is_none());
        let evidence = state
            .quality_evidence_view(&scope, lit(true))
            .expect("the rows the third file contributed");
        let rows = collect_lazy(evidence.lf.clone(), false).unwrap();
        assert_eq!(
            rows.column("id")
                .unwrap()
                .i64()
                .unwrap()
                .into_no_null_iter()
                .collect::<Vec<_>>(),
            vec![5, 6],
            "the file that has no `fee`, and only that file"
        );
        assert!(
            rows.column(crate::schema_union::DRIFT_COLUMN).is_err(),
            "the hidden scan index is never handed back as user data"
        );
    }

    /// The local route to the values a type conflict hides: real Parquet files that
    /// disagree, read through `lenient_scan` rather than through a dataset's own scan
    /// closure. The remote route above shares none of this code.
    #[test]
    fn a_local_dataset_reads_the_values_a_type_conflict_hides() {
        use crate::data_quality::{DataQualityPlan, ObservationKind, QualityCompute, QualityScope};
        use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
        use polars::prelude::{DataType, ParquetWriter, df};

        let dir = tempfile::tempdir().unwrap();
        let write = |name: &str, mut frame: polars::prelude::DataFrame| -> String {
            let path = dir.path().join(name);
            let file = std::fs::File::create(&path).unwrap();
            ParquetWriter::new(file).finish(&mut frame).unwrap();
            path.to_string_lossy().to_string()
        };
        // `n` is an integer in the first file and text in the second, so the scan reads
        // it as Int64 and leaves the second file's values behind entirely.
        let files = vec![
            write(
                "a.parquet",
                df!("id" => &[0i64, 1, 2], "n" => &[10i64, 20, 30]).unwrap(),
            ),
            write(
                "b.parquet",
                df!("id" => &[3i64, 4], "n" => &["sixty", "seventy"]).unwrap(),
            ),
        ];
        let dataset: DatasetSchema = union_file_schemas(
            &[
                file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
                file_schema(&[("id", DataType::Int64), ("n", DataType::String)], 2),
            ],
            SchemaOrigin::AllFooters(2),
        );
        let file_rows = vec![3usize, 2];
        let drift = crate::schema_union::ScanDrift::new(&files, &dataset, &file_rows);
        let lf = crate::schema_union::lenient_scan(
            &files,
            dataset.schema.clone(),
            None,
            drift.as_ref(),
            &[],
        )
        .unwrap();
        let state = DataTableState::from_schema_and_lazyframe(
            dataset.schema.clone(),
            lf,
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            dataset: Some(DatasetAtOpen {
                schema: dataset,
                file_rows,
                files,
            }),
            ..Default::default()
        });
        assert!(state.drifts(), "the two files disagree on `n`");
        assert_eq!(
            state.quality_conflict_reads(),
            1,
            "one column, in one file, before anything runs"
        );

        let (lf, source) = state.data_quality_source_scan();
        let mut source = source.expect("every file is counted");
        source.conflict_scan = state.quality_conflict_scan();
        let lf = crate::data_quality::prepare_source_quality_scan(lf, Some(&source)).unwrap();
        let plan = DataQualityPlan {
            scope: QualityScope::WholeSource,
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results =
            crate::data_quality::compute_data_quality(&lf, Some(5), &plan, Some(&source), false)
                .unwrap();
        let conflict = results
            .observations
            .iter()
            .find(|observation| observation.kind == ObservationKind::TypeConflict)
            .expect("`n` is text in the second file");
        let file = conflict.files.first().expect("the file that disagrees");
        assert_eq!(file.number, 2);
        assert_eq!(
            file.examples,
            vec!["sixty".to_string(), "seventy".to_string()],
            "read at the type that file wrote, not as the null the scan hands back"
        );
    }

    /// A remote dataset reads its column as text too, and its windowed reads with it.
    ///
    /// The remote branch takes a different route: the scan closure it was opened with
    /// is asked again with the column named, and `buffer_lf` passes the same names on
    /// every later window. Miss that second half and a cloud dataset would read the
    /// first screen as text and the next one as it was, disagreeing with its own
    /// schema.
    #[test]
    fn a_remote_dataset_reads_a_conflicting_column_as_text_on_every_window() {
        use crate::schema_union::{DatasetSchema, SchemaOrigin, union_file_schemas};
        use polars::prelude::{DataType, IntoLazy, df};

        let urls: Vec<String> = vec!["a".to_string(), "b".to_string()];
        // The scan the dataset was opened with, standing in for the cloud one: the
        // first file holds `n` as an integer and the second as text.
        let scan: FileScan = Arc::new(move |urls: &[String], as_text: &[PlSmallStr]| {
            let frames: Vec<LazyFrame> = urls
                .iter()
                .map(|url| {
                    // The hidden row index the real scan stamps, numbered from where
                    // the file's rows begin in the dataset so a window keeps its place.
                    let frame = if url == "a" {
                        df!(
                            "id" => &[0i64, 1, 2],
                            "n" => &[10i64, 20, 30],
                            crate::schema_union::DRIFT_COLUMN => &[0u32, 1, 2],
                        )
                        .unwrap()
                    } else {
                        df!(
                            "id" => &[3i64, 4],
                            "n" => &["sixty", "seventy"],
                            crate::schema_union::DRIFT_COLUMN => &[3u32, 4],
                        )
                        .unwrap()
                    };
                    let lf = frame.lazy();
                    if as_text.contains(&PlSmallStr::from("n")) {
                        lf.with_column(col("n").cast(DataType::String))
                    } else if url == "a" {
                        lf
                    } else {
                        // Not read from this file at all, as the real scan leaves it.
                        lf.with_column(lit(NULL).cast(DataType::Int64).alias("n"))
                    }
                })
                .collect();
            polars::prelude::concat(frames, Default::default())
        });

        let dataset: DatasetSchema = union_file_schemas(
            &[
                file_schema(&[("id", DataType::Int64), ("n", DataType::Int64)], 3),
                file_schema(&[("id", DataType::Int64), ("n", DataType::String)], 2),
            ],
            SchemaOrigin::AllFooters(2),
        );
        // The dataset's schema, which does not name the hidden row index the frame
        // carries — as the real open does it.
        let mut state = DataTableState::from_schema_and_lazyframe(
            dataset.schema.clone(),
            scan(&urls, &[]).unwrap(),
            &crate::OpenOptions::default(),
            None,
        )
        .unwrap()
        .with_open(OpenFacts {
            remote_source: true,
            remote_files: Some(RemoteFiles {
                urls: Arc::new(urls.clone()),
                scan,
                count: Arc::new(|_| Ok(vec![vec![3], vec![2]])),
                offsets: None,
            }),
            dataset: Some(DatasetAtOpen {
                schema: dataset,
                file_rows: vec![3, 2],
                files: urls.clone(),
            }),
            ..Default::default()
        });
        let groups = (state.remote_files_counter().unwrap())(&Default::default()).unwrap();
        assert!(state.count_landed(state.len_generation(), 5, Some(&groups)));
        assert!(state.drifts(), "the two files disagree on `n`");

        assert!(
            state.read_column_as_text("n").unwrap(),
            "the offer is taken"
        );
        assert_eq!(
            state.schema.get("n"),
            Some(&DataType::String),
            "the column is text now"
        );

        // Every window, not only the first: the second file's rows are on their own
        // page, and they are the ones the conflict was hiding.
        let text = |state: &DataTableState, start: usize, rows: usize| -> Vec<String> {
            collect_lazy(state.buffer_lf(start, rows).unwrap(), false)
                .unwrap()
                .column("n")
                .unwrap()
                .str()
                .unwrap()
                .iter()
                .map(|value| value.unwrap_or("null").to_string())
                .collect()
        };
        assert_eq!(text(&state, 0, 3), ["10", "20", "30"]);
        assert_eq!(
            text(&state, 3, 2),
            ["sixty", "seventy"],
            "the page that needed the text read most"
        );
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
            Arc::new(move |urls: &[String], _as_text: &[PlSmallStr]| {
                asked.lock().unwrap().push(urls.to_vec());
                let frames: Vec<LazyFrame> = urls
                    .iter()
                    .map(|u| part(u.trim_start_matches("file").parse::<i32>().unwrap() * 100))
                    .collect();
                polars::prelude::concat(frames, Default::default())
            })
        };
        let full = scan(&urls, &[]).unwrap();
        asked.lock().unwrap().clear();
        let mut state = DataTableState::from_lazyframe(full, &crate::OpenOptions::default())
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(urls),
                    scan,
                    count: Arc::new(|_| Ok(vec![vec![50, 50]; 5])),
                    offsets: None,
                }),
                ..Default::default()
            });
        let groups = (state.remote_files_counter().unwrap())(&Default::default()).unwrap();
        assert!(state.count_landed(state.len_generation(), 500, Some(&groups)));
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

    /// What an open finds arrives in one step, in the order it depends on: a many-file
    /// dataset's row groups land after its files, so they count it and place each page
    /// in its files; a single object's footer counts it.
    #[test]
    fn an_open_s_findings_arrive_together() {
        let lf = || df!("a" => (0..100i32).collect::<Vec<_>>()).unwrap().lazy();
        let many = DataTableState::from_lazyframe(lf(), &crate::OpenOptions::default())
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                remote_files: Some(RemoteFiles {
                    urls: Arc::new(vec!["one".to_string(), "two".to_string()]),
                    scan: Arc::new(move |_: &[String], _: &[PlSmallStr]| Ok(lf())),
                    count: Arc::new(|_| Err("counted at the open".to_string())),
                    offsets: None,
                }),
                row_groups: vec![vec![30, 30], vec![40]],
                ..Default::default()
            });
        assert_eq!(many.num_rows_if_valid(), Some(100));
        assert_eq!(
            many.files_a_page_reads(50, 20),
            Some(2),
            "rows 50..70 span both"
        );
        assert!(
            many.remote_files_counter().is_none(),
            "nothing left to count"
        );

        let one = DataTableState::from_lazyframe(lf(), &crate::OpenOptions::default())
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![60, 40]],
                ..Default::default()
            });
        assert_eq!(one.num_rows_if_valid(), Some(100));
        assert!(one.is_remote_source());
    }

    #[test]
    fn a_remote_source_buffers_one_window_and_pages_inside_it_for_free() {
        // Every buffer fill of an object-store scan downloads whole row groups, so the
        // buffer is one window of `max_buffered_rows` rather than a few pages: paging
        // inside it asks for nothing, and a jump asks once.
        let lf = df!("a" => &[0i32]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                ..Default::default()
            });
        state.num_rows = 1_000_000;
        state.num_rows_valid = true;
        state.visible_rows = 40;
        let window = |start: usize| Fill {
            df: df!("a" => (0..10_000).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: start + 10_000,
            num_rows: 1_000_000,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, 10_000));
        state.land(window(0));
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
        state.land(window(990_000));

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
        let mut state = DataTableState::new(lf, None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![G; 10]],
                ..Default::default()
            });
        assert_eq!(state.num_rows, 10 * G);
        state.visible_rows = 40;
        let rows = |start: usize, end: usize| Fill {
            df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 10 * G,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, CAP));
        state.land(rows(0, CAP));
        for _ in 0..20 {
            assert!(!state.page_down(), "a page inside the window needs no fill");
        }

        // A jump to the end of group 0 is clipped to it: group 1 is not touched yet.
        assert!(state.scroll_to(G - 60));
        let request = state
            .prepare_async_collect(None)
            .expect("the end of group 0");
        assert_eq!((request.buffer_start, request.buffer_end), (G - CAP, G));
        state.land(rows(G - CAP, G));

        // A view straddling the boundary fetches rows of group 1 alone.
        assert!(state.scroll_to(G - 20));
        let request = state.prepare_async_collect(None).expect("into group 1");
        assert_eq!((request.buffer_start, request.buffer_end), (G, G + CAP / 2));
        state.land(rows(G, G + CAP / 2));
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
        state.land(rows(10 * G - CAP, 10 * G));
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
        let mut state = DataTableState::new(lf, None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![G; 25]],
                ..Default::default()
            });
        state.visible_rows = 40;
        let rows = |start: usize, end: usize| Fill {
            df: df!("a" => (start as i32..end as i32).collect::<Vec<i32>>()).unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 25 * G,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, 2 * G));
        state.land(rows(0, 2 * G));

        assert!(state.scroll_to(2 * G - 20));
        let request = state.prepare_async_collect(None).expect("the next group");
        assert_eq!((request.buffer_start, request.buffer_end), (2 * G, 3 * G));
        state.land(rows(2 * G, 3 * G));
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
        let mut state = DataTableState::new(lf, None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![1_000_000; 3]],
                ..Default::default()
            });
        assert_eq!(
            estimate_bytes_per_row(&state.schema, &state.column_order, &[]),
            8_000
        );
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
        let tiny = |bytes: usize| {
            let mut tiny = DataTableState::new(
                df!("a" => &["x".repeat(2_000)]).unwrap().lazy(),
                None,
                None,
                None,
                Some(1),
                true,
            )
            .unwrap()
            .with_open(OpenFacts {
                column_bytes: vec![("a".to_string(), bytes)],
                ..Default::default()
            });
            tiny.visible_rows = 40;
            tiny.byte_cap_rows()
        };
        assert_eq!(tiny(2_000), 1024 * 1024 / 2_016);
        assert_eq!(tiny(1 << 20), 40);
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
        state.land(Fill {
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
        let mut state = DataTableState::new(lf, None, None, Some(10_000), None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![500, 500]],
                parquet_count_dir: Some(PathBuf::from("/hive")),
                ..Default::default()
            });
        state.visible_rows = 40;
        state.defer_collect = true;

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!(
            (request.buffer_start, request.buffer_end),
            (0, 1_000),
            "both groups fit the remote window"
        );

        state.filter(vec![FilterStatement {
            columns: Vec::new(),
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
        assert_eq!(state.parquet_count_dir(), Some(PathBuf::from("/hive")));
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
        state.land(Fill {
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
        let mut state = DataTableState::new(lf, None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![G; 10]],
                ..Default::default()
            });
        state.visible_rows = 40;
        let rows = |start: usize, end: usize| Fill {
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
        state.land(rows(request.buffer_start, request.buffer_end));
        assert!(state.scroll_to(G - 20));
        let stitch = state.prepare_async_collect(None).expect("into group 1");
        assert_eq!(stitch.buffer_start, G);

        // Meanwhile a synchronous collect moved the view and replaced the buffer.
        assert!(state.scroll_to(500_000));
        state.land(rows(450_000, 550_000));
        state.needs_recollect = false;

        // Fit as the worker did, against the rows on hand when it was planned.
        let fill = rows(stitch.buffer_start, stitch.buffer_end);
        state.apply_async_collect(stitch.plan.fit(fill.df));
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
        let mut state = DataTableState::new(lf, None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![G; 10]],
                ..Default::default()
            });
        state.visible_rows = 40;
        assert!(state.scroll_to(G - 60));
        let request = state
            .prepare_async_collect(None)
            .expect("the end of group 0");
        assert!(request.buffer_end <= G);

        state.visible_rows = 120; // resized while the fetch was out
        state.needs_recollect = false;
        state.land(Fill {
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
        let mut state = DataTableState::new(lf, None, None, None, Some(64), true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![G; 5]],
                column_bytes: vec![("a".to_string(), 1_000)],
                ..Default::default()
            });
        state.visible_rows = 40;
        let cap = state.byte_cap_rows();
        assert!(
            (G..2 * G).contains(&cap),
            "cap {cap} between one and two groups"
        );
        let rows = |start: usize, end: usize| Fill {
            df: df!("a" => (start..end).map(|i| "x".repeat(8 + i % 3)).collect::<Vec<_>>())
                .unwrap(),
            buffer_start: start,
            buffer_end: end,
            num_rows: 5 * G,
            count_known: true,
        };

        let request = state.prepare_async_collect(None).expect("first fill");
        assert_eq!((request.buffer_start, request.buffer_end), (0, G));
        state.land(rows(0, G));

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
        state.land(Fill {
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
        let mut state = DataTableState::new(lf, None, None, None, None, true)
            .unwrap()
            .with_open(OpenFacts {
                remote_source: true,
                row_groups: vec![vec![60, 40]],
                ..Default::default()
            });
        assert!(state.remote_window());
        state.query("select a where a > 50".to_string());
        assert!(!state.remote_window());
        state.query(String::new());
        assert!(state.remote_window());
        assert_eq!(state.num_rows_if_valid(), Some(100));
    }

    #[test]
    fn quality_source_scope_ignores_current_query_and_evidence_matches_scope() {
        let lf = df!("a" => &[1i32, 2, 3, 4]).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.drift_files = vec!["first.parquet".into(), "second.parquet".into()];
        state.drift_file_starts = vec![0, 2];
        state.query("select a where a > 2".to_string());
        let (current, _) = state.data_quality_scan(false);
        let (source, context) = state.data_quality_source_scan();
        let source =
            crate::data_quality::prepare_source_quality_scan(source, context.as_ref()).unwrap();
        assert_eq!(current.collect().unwrap().height(), 2);
        assert_eq!(source.collect().unwrap().height(), 4);
        assert_eq!(state.quality_source_file_count(), 2);
        let (raw, mapping) = state.data_quality_source_scan();
        let indexed =
            crate::data_quality::prepare_source_quality_scan(raw, mapping.as_ref()).unwrap();
        let first_file = crate::data_quality::apply_quality_scope(
            indexed,
            &crate::data_quality::QualityScope::SourceFiles(vec![1]),
            mapping.as_ref(),
        )
        .unwrap()
        .collect()
        .unwrap();
        assert_eq!(first_file.height(), 2);
        assert_eq!(
            first_file.column("a").unwrap().i32().unwrap().get(0),
            Some(1)
        );

        let evidence = state
            .quality_evidence_view(
                &crate::data_quality::QualityScope::WholeSource,
                col("a").eq(lit(1)),
            )
            .unwrap();
        assert_eq!(evidence.visible_lf().collect().unwrap().height(), 1);
        let bounded = state
            .quality_evidence_view(
                &crate::data_quality::QualityScope::FirstRows(1),
                col("a").eq(lit(4)),
            )
            .unwrap();
        assert_eq!(bounded.visible_lf().collect().unwrap().height(), 0);
        let file_evidence = state
            .quality_evidence_view(
                &crate::data_quality::QualityScope::SourceFiles(vec![1]),
                col("a").eq(lit(1)),
            )
            .unwrap();
        assert_eq!(file_evidence.visible_lf().collect().unwrap().height(), 1);
    }

    #[test]
    fn source_time_roles_can_use_columns_hidden_by_current_query() {
        let lf = df!("a" => &[1i32, 2], "event" => &[20_000i32, 20_001])
            .unwrap()
            .lazy()
            .with_columns([col("event").cast(DataType::Date)]);
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.query("select a".to_string());
        assert!(
            state
                .quality_temporal_columns(&crate::data_quality::QualityScope::CurrentView)
                .is_empty()
        );
        assert_eq!(
            state.quality_temporal_columns(&crate::data_quality::QualityScope::WholeSource),
            vec!["event"]
        );
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
        assert_eq!(col.str().unwrap().get(0).unwrap(), binary_stub());
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
        let results = crate::statistics::compute_describe_from_lazy(
            &analysis_lf,
            Some(3),
            &crate::sampling::Sample {
                method: crate::sampling::SampleMethod::EveryRow,
                ..crate::sampling::Sample::default()
            },
            false,
        )
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
        assert_eq!(cat.min.as_deref(), Some(binary_stub()));
        assert_eq!(cat.max.as_deref(), Some(binary_stub()));
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

    #[test]
    fn sorted_column_header_carries_the_direction_mark() {
        // A sorted view must not look identical to an unsorted one: the sorted
        // column's header says so, and only that column's.
        let g = crate::glyphs::get();
        let table = DataTable::default().with_sort(vec!["age".to_string()], vec![false]);
        let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
        let area = Rect::new(0, 0, 20, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let header = header_row_string(&buf, area);
        assert!(
            header.contains(&format!("age{}", g.sort_asc)),
            "the sorted column is marked: {header:?}"
        );
        assert!(
            !header.contains(&format!("name{}", g.sort_asc)),
            "the unsorted column is not: {header:?}"
        );
        assert!(
            !header.contains(g.sort_desc),
            "an ascending sort never shows the descending mark: {header:?}"
        );
    }

    #[test]
    fn the_direction_mark_flips_with_the_sort() {
        let g = crate::glyphs::get();
        let table = DataTable::default().with_sort(vec!["age".to_string()], vec![true]);
        let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
        let area = Rect::new(0, 0, 20, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let header = header_row_string(&buf, area);
        assert!(
            header.contains(&format!("age{}", g.sort_desc)),
            "a descending sort points down: {header:?}"
        );
        assert!(!header.contains(g.sort_asc), "and never up: {header:?}");
    }

    #[test]
    fn every_column_of_a_multi_sort_is_marked() {
        // Marks only, no position numbers: the columns all run the same way.
        let g = crate::glyphs::get();
        let table = DataTable::default().with_sort(
            vec!["name".to_string(), "age".to_string()],
            vec![false, false],
        );
        let df = df!("name" => &["ann"], "age" => &[41i32]).unwrap();
        let area = Rect::new(0, 0, 20, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let header = header_row_string(&buf, area);
        for name in ["name", "age"] {
            assert!(
                header.contains(&format!("{name}{}", g.sort_asc)),
                "{name} carries the mark: {header:?}"
            );
        }
    }

    #[test]
    fn the_sort_mark_composes_with_the_drift_mark() {
        // A column can be sorted and drifting at once; the header carries both
        // marks and the width arithmetic counts both, so nothing is clipped.
        let g = crate::glyphs::get();
        let table = DataTable::default()
            .with_sort(vec!["age".to_string()], vec![false])
            .with_drift(
                Vec::new(),
                Arc::new(vec![crate::schema_union::DriftGroup {
                    absent: vec!["age".into()],
                    unread: Vec::new(),
                }]),
            );
        let df = df!("age" => &[41i32]).unwrap();
        let area = Rect::new(0, 0, 20, 3);
        let mut buf = Buffer::empty(area);
        let mut ts = TableState::default();
        table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
        let header = header_row_string(&buf, area);
        assert!(
            header.contains(&format!("age{}{}", g.drift_mark, g.sort_asc)),
            "footnote first, direction after: {header:?}"
        );
        assert!(
            row_string(&buf, area, 1).contains("41"),
            "the widened header does not clip the value"
        );
    }

    #[test]
    fn the_header_mark_follows_the_state_sort_and_its_reverse() {
        // Through the stateful render: the marks come from the state being drawn,
        // so applying a sort shows them and `reverse` flips them, with no caller
        // wiring in between.
        let g = crate::glyphs::get();
        let mut state =
            DataTableState::new(create_test_lf(), None, None, None, None, true).unwrap();
        state.visible_rows = 3;
        state.sort(vec!["a".to_string()], true);

        let area = Rect::new(0, 0, 20, 5);
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, &mut state);
        let header = header_row_string(&buf, area);
        assert!(
            header.contains(&format!("a{}", g.sort_asc)),
            "sorted ascending: {header:?}"
        );

        state.reverse();
        let mut buf2 = Buffer::empty(area);
        DataTable::default().render(area, &mut buf2, &mut state);
        let header2 = header_row_string(&buf2, area);
        assert!(
            header2.contains(&format!("a{}", g.sort_desc)),
            "reversed: {header2:?}"
        );
        assert!(
            !header2.contains(g.sort_asc),
            "the old direction is gone: {header2:?}"
        );
    }

    /// A state over `id` (frozen), `name`, `city`, `amount`, thirty rows, drawn once at
    /// 80×24 so the room and widths are known.
    fn cursor_fixture() -> (DataTableState, Rect) {
        let n = 30;
        let lf = df!(
            "id" => (0..n).collect::<Vec<i64>>(),
            "name" => (0..n).map(|i| format!("name {i}")).collect::<Vec<_>>(),
            "city" => (0..n).map(|i| format!("city {i}")).collect::<Vec<_>>(),
            "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 22;
        state.set_locked_columns(1);
        state.table_state.select(Some(0));
        let area = Rect::new(0, 0, 80, 24);
        DataTable::default().render(area, &mut Buffer::empty(area), &mut state);
        (state, area)
    }

    /// The x range of the column headed `name` on the header row.
    fn column_span(buf: &Buffer, area: Rect, name: &str) -> std::ops::Range<u16> {
        let header = row_string(buf, area, 0);
        let at = header.find(name).expect("the column is drawn") as u16;
        at..at + name.len() as u16
    }

    /// A click finds the cell drawn under it, at 80×24 and on a wide screen: each
    /// column where its heading is, frozen or scrolling, and each row where its cells
    /// are, with row numbers on and the view scrolled down.
    #[test]
    fn a_click_finds_the_cell_drawn_under_it() {
        for (width, height) in [(80, 24), (200, 50)] {
            let n = 400;
            let mut columns = vec![Column::new("id".into(), (0..n).collect::<Vec<i64>>())];
            for c in 0..30 {
                columns.push(Column::new(
                    format!("col_{c:02}").as_str().into(),
                    (0..n).map(|i| format!("v{i}_{c}")).collect::<Vec<_>>(),
                ));
            }
            let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
            let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
            state.set_locked_columns(1);
            state.toggle_row_numbers();
            let area = Rect::new(0, 0, width, height);
            let render = |state: &mut DataTableState| {
                let mut buf = Buffer::empty(area);
                DataTable::default().render(area, &mut buf, state);
                buf
            };
            // The first frame sets how many rows fit; the rows are read for it.
            render(&mut state);
            state.collect();
            for scrolled in [false, true] {
                if scrolled {
                    state.page_down();
                    state.collect();
                }
                let buf = render(&mut state);
                let drawn = state.drawn.clone().expect("the table was drawn");
                let header = row_string(&buf, area, 0);
                assert!(drawn.columns.len() > 3, "{width}x{height}: {header:?}");
                assert_eq!(drawn.columns[0].2, "id", "the frozen column first");
                for (_, _, name) in &drawn.columns {
                    // In cells, not bytes: the frozen separator is a wide glyph.
                    let at = header.find(name.as_str()).expect("heading drawn");
                    let from = header[..at].chars().count() as u16;
                    for x in [from, from + name.len() as u16 - 1] {
                        let hit = state.drawn_cell(x, 0).expect("on the table");
                        assert_eq!(hit.row, None, "the header is no row");
                        assert_eq!(hit.column.as_deref(), Some(name.as_str()), "at {x}");
                    }
                }
                // The rail and the row numbers are no column, but are the row.
                let y = drawn.header + 5;
                assert_eq!(
                    state.drawn_cell(0, y),
                    Some(CellHit {
                        row: Some(5),
                        column: None
                    })
                );
                // A cell: the row and column whose value is drawn there.
                let (from, to, name) = drawn.columns[2].clone();
                let c: usize = name["col_".len()..].parse().unwrap();
                let hit = state.drawn_cell(from, y).expect("a cell");
                let row = drawn.start_row + 5;
                let text: String = (from..to).map(|x| buf[(x, y)].symbol()).collect();
                assert_eq!(text.trim(), format!("v{row}_{c}"), "{width}x{height}");
                state.point_at(&hit);
                assert_eq!(state.table_state.selected(), Some(5));
                assert_eq!(state.current_column(), Some(name.as_str()));
                // Off the table's right or bottom edge is nothing.
                assert_eq!(state.drawn_cell(width, y), None);
                assert_eq!(state.drawn_cell(0, height), None);
            }
        }
    }

    /// The column cursor at 80×24: its header and cells take the column tint, the
    /// current cell (the cursor's row and column) the cell tint, and the rest of the
    /// current row keeps the row tint. Frozen columns take it the same way.
    #[test]
    fn the_column_cursor_tints_its_header_and_cells() {
        let (mut state, area) = cursor_fixture();
        let row_tint = Color::Rgb(0x28, 0x34, 0x57);
        let column_tint = Color::Rgb(0x29, 0x2e, 0x42);
        let cell_tint = Color::Rgb(0x3b, 0x42, 0x61);
        let table = || {
            DataTable {
                selection_style: Style::default().bg(row_tint),
                ..DataTable::default()
            }
            .with_cursor_styles(
                crate::config::column_cursor_style(Some(column_tint)),
                crate::config::cell_cursor_style(Some(cell_tint)),
            )
        };
        state.move_cursor(CursorMove::Right);
        state.move_cursor(CursorMove::Right);
        assert_eq!(state.current_column(), Some("city"));
        let mut buf = Buffer::empty(area);
        table().render(area, &mut buf, &mut state);
        let city = column_span(&buf, area, "city");
        let name = column_span(&buf, area, "name");
        for x in city.clone() {
            assert_eq!(buf[(x, 0)].bg, cell_tint, "the header, at {x}");
            assert!(buf[(x, 0)].modifier.contains(Modifier::BOLD));
            assert_eq!(buf[(x, 1)].bg, cell_tint, "the current cell, at {x}");
            for y in 2..area.height {
                assert_eq!(buf[(x, y)].bg, column_tint, "the column, at {x},{y}");
            }
        }
        for x in name.clone() {
            assert_eq!(buf[(x, 1)].bg, row_tint, "the rest of the row");
            assert_ne!(buf[(x, 0)].bg, cell_tint, "another header");
            assert_ne!(buf[(x, 2)].bg, column_tint, "another column");
        }

        // Frozen: the same marks, left of the separator.
        state.move_cursor(CursorMove::First);
        assert_eq!(state.current_column(), Some("id"));
        let mut buf = Buffer::empty(area);
        table().render(area, &mut buf, &mut state);
        let id = column_span(&buf, area, "id");
        for x in id {
            assert_eq!(buf[(x, 0)].bg, cell_tint);
            assert_eq!(buf[(x, 1)].bg, cell_tint);
            assert_eq!(buf[(x, 5)].bg, column_tint);
        }
        for x in city {
            assert_ne!(buf[(x, 0)].bg, cell_tint, "city lets go of it");
        }
    }

    /// Where the tints would not show (16 colors, `NO_COLOR`), the header and the
    /// current cell are reversed, so the cursor is still on screen, and nothing else is.
    #[test]
    fn the_column_cursor_shows_without_its_tints() {
        let (mut state, area) = cursor_fixture();
        state.move_cursor(CursorMove::Right);
        for tint in [Color::Black, Color::White, Color::Reset] {
            let mut buf = Buffer::empty(area);
            DataTable::default()
                .with_cursor_styles(
                    crate::config::column_cursor_style(Some(tint)),
                    crate::config::cell_cursor_style(Some(tint)),
                )
                .render(area, &mut buf, &mut state);
            let name = column_span(&buf, area, "name");
            let reversed = |x, y| buf[(x, y)].modifier.contains(Modifier::REVERSED);
            for x in name {
                assert!(reversed(x, 0), "{tint:?}: the header");
                assert!(reversed(x, 1), "{tint:?}: the current cell");
                assert!(!reversed(x, 2), "{tint:?}: not the rest of the column");
            }
            let city = column_span(&buf, area, "city");
            assert!(!reversed(city.start, 0) && !reversed(city.start, 1));
        }
    }

    /// Under a reversed row, the current cell is drawn upright, tinted or not, so it
    /// stands out from the row; the header keeps its own mark.
    #[test]
    fn the_current_cell_stands_out_of_a_reversed_row() {
        let (mut state, area) = cursor_fixture();
        state.move_cursor(CursorMove::Right);
        for tint in [Color::Rgb(0x3b, 0x42, 0x61), Color::Black, Color::Reset] {
            let mut buf = Buffer::empty(area);
            DataTable {
                selection_style: Style::default().add_modifier(Modifier::REVERSED),
                ..DataTable::default()
            }
            .with_cursor_styles(
                crate::config::column_cursor_style(Some(tint)),
                crate::config::cell_cursor_style(Some(tint)),
            )
            .render(area, &mut buf, &mut state);
            let reversed = |x, y| buf[(x, y)].modifier.contains(Modifier::REVERSED);
            for x in column_span(&buf, area, "name") {
                assert!(!reversed(x, 1), "{tint:?}: the current cell is upright");
                assert!(buf[(x, 1)].modifier.contains(Modifier::BOLD));
            }
            let city = column_span(&buf, area, "city");
            assert!(reversed(city.start, 1), "{tint:?}: the rest of the row");
        }
    }

    /// The cursor follows its column by name when the columns are reordered or
    /// frozen, and a column hidden from under it hands it to the one in its place.
    #[test]
    fn the_column_cursor_follows_its_column_by_name() {
        let (mut state, area) = cursor_fixture();
        state.move_cursor(CursorMove::Right);
        state.move_cursor(CursorMove::Right);
        assert_eq!(state.current_column(), Some("city"));
        let order = |names: &[&str]| names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
        state.set_column_order(order(&["city", "id", "name", "amount"]));
        assert_eq!(state.current_column(), Some("city"));
        assert_eq!(state.current_column_index(), Some(0));
        state.set_locked_columns(0);
        state.set_column_order(order(&["id", "name", "city", "amount"]));
        state.set_locked_columns(3);
        assert_eq!(state.current_column(), Some("city"), "frozen now");
        // Hidden: the column now in its place takes it.
        state.set_locked_columns(0);
        state.set_column_order(order(&["id", "name", "amount"]));
        assert_eq!(state.current_column(), Some("amount"));
        // Hidden at the end: the last column.
        state.set_column_order(order(&["id", "name"]));
        assert_eq!(state.current_column(), Some("name"));
        state.set_column_order(Vec::new());
        assert_eq!(state.current_column(), None);
        DataTable::default().render(area, &mut Buffer::empty(area), &mut state);
    }

    /// `h` `l` cross from the frozen columns to the scrolling ones and back in the
    /// shown order; the view moves only when the cursor would leave it, and a page
    /// puts the cursor on the new page's first column.
    #[test]
    fn the_column_cursor_scrolls_only_at_the_edges() {
        let n = 5;
        let names: Vec<String> = (0..40).map(|i| format!("column_{i:02}")).collect();
        let columns: Vec<Column> = names
            .iter()
            .map(|name| Column::new(name.as_str().into(), (0..n).collect::<Vec<i64>>()))
            .collect();
        let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 5;
        state.set_locked_columns(2);
        let area = Rect::new(0, 0, 80, 8);
        let draw = |state: &mut DataTableState| {
            DataTable::default().render(area, &mut Buffer::empty(area), state);
            state.columns_on_screen().unwrap()
        };
        let start = draw(&mut state);
        assert_eq!((start.first, start.cursor), (3, 1));
        state.move_cursor(CursorMove::Right);
        state.move_cursor(CursorMove::Right);
        let on = draw(&mut state);
        assert_eq!((on.first, on.cursor), (3, 3), "into the scrolling side");
        // Walk to the right edge: nothing scrolls until the cursor would leave (a
        // column cut at the edge counts as leaving), then just enough.
        let mut before = on;
        let past = loop {
            state.move_cursor(CursorMove::Right);
            let now = draw(&mut state);
            assert_eq!(now.cursor, before.cursor + 1);
            if now.first != before.first {
                break now;
            }
            before = now;
        };
        assert!(past.first > 3 && past.cursor <= past.last, "{past:?}");
        assert!(past.cursor >= start.last, "not before the edge: {past:?}");
        assert!(past.first <= before.last, "no column skipped: {past:?}");
        // Back to the left edge, and one more scrolls back a column.
        for _ in past.first..past.cursor {
            state.move_cursor(CursorMove::Left);
        }
        assert_eq!(draw(&mut state).first, past.first);
        state.move_cursor(CursorMove::Left);
        assert_eq!(draw(&mut state).first, past.first - 1);
        // Pages: the cursor starts the new page; on the last, it takes the last column.
        state.move_cursor(CursorMove::PageRight);
        let page = draw(&mut state);
        assert_eq!(page.cursor, page.first);
        state.move_cursor(CursorMove::Last);
        let last = draw(&mut state);
        assert_eq!((last.cursor, last.last), (40, 40));
        state.move_cursor(CursorMove::PageRight);
        assert_eq!(draw(&mut state).cursor, 40);
        // From a frozen column, the next is the first scrolling column: the view
        // goes back to it.
        state.go_to_column("column_01");
        assert_eq!(draw(&mut state).first, last.first, "frozen: on screen");
        state.move_cursor(CursorMove::Right);
        let back = draw(&mut state);
        assert_eq!((back.first, back.cursor), (3, 3));
        // `[` on the first page: its first column, then the first of all.
        state.move_cursor(CursorMove::Right);
        state.move_cursor(CursorMove::PageLeft);
        assert_eq!(draw(&mut state).cursor, 3);
        state.move_cursor(CursorMove::PageLeft);
        assert_eq!(draw(&mut state).cursor, 1);
        state.move_cursor(CursorMove::Left);
        assert_eq!(draw(&mut state).cursor, 1, "nothing left of the first");
    }

    /// Every cell right of the frozen separator sits one cell off it, as the cells
    /// left of it do, and the gap takes its row's tint. A right-aligned number as
    /// wide as its column, a negative one most often, used to touch the line:
    /// `│-9.930889` (#386).
    #[test]
    fn the_frozen_separator_has_a_gap_on_both_sides() {
        let lf = df!(
            "carrier" => &["AA", "UA", "9E"],
            "delay" => &[-9.930889f64, 3.5, 12.25],
            "name" => &["American", "United", "Endeavor"],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 3;
        state.set_locked_columns(1);
        state.table_state.select(Some(0));

        let area = Rect::new(0, 0, 40, 6);
        let mut buf = Buffer::empty(area);
        let table = DataTable {
            header_bg: Color::Indexed(238),
            alternate_row_bg: Some(Color::Indexed(236)),
            selection_style: Style::default().bg(Color::Indexed(24)),
            ..DataTable::default()
        };
        table.render(area, &mut buf, &mut state);

        let rows: Vec<String> = (0..area.height)
            .map(|y| row_string(&buf, area, y))
            .collect();
        assert!(
            rows[1].contains(&format!("{} -9.930889", crate::glyphs::get().rule)),
            "{rows:#?}"
        );
        let rule = crate::glyphs::get().rule;
        let sep = (0..area.width)
            .find(|&x| buf[(x, 0)].symbol() == rule)
            .expect("a separator");
        for y in 0..area.height {
            let row = &rows[y as usize];
            assert_eq!(buf[(sep - 1, y)].symbol(), " ", "row {y}: {row:?}");
            assert_eq!(buf[(sep + 1, y)].symbol(), " ", "row {y}: {row:?}");
            assert_eq!(
                buf[(sep + 1, y)].bg,
                buf[(sep + 2, y)].bg,
                "row {y}'s gap takes the row's tint: {row:?}"
            );
        }
        // The header, the highlight and the stripe, not only unstyled rows.
        assert_eq!(buf[(sep + 1, 0)].bg, Color::Indexed(238));
        assert_eq!(buf[(sep + 1, 1)].bg, Color::Indexed(24));
        assert_eq!(buf[(sep + 1, 2)].bg, Color::Indexed(236));
    }

    /// The frozen separator runs down the header and the rows, and stops under the
    /// last: a grouped view of seven rows had it running down the empty screen.
    #[test]
    fn the_frozen_separator_stops_at_the_last_row() {
        let lf = df!(
            "carrier" => &["AA", "UA", "9E"],
            "delay" => &[-9.9f64, 3.5, 12.25],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 3;
        state.set_locked_columns(1);
        state.table_state.select(Some(0));
        let area = Rect::new(0, 0, 30, 10);
        let mut buf = Buffer::empty(area);
        DataTable::default().render(area, &mut buf, &mut state);
        let rule = crate::glyphs::get().rule;
        let sep = (0..area.width)
            .find(|&x| buf[(x, 0)].symbol() == rule)
            .expect("a separator");
        let ruled: Vec<u16> = (0..area.height)
            .filter(|&y| buf[(sep, y)].symbol() == rule)
            .collect();
        let last = ruled.last().copied().unwrap();
        assert_eq!(
            ruled,
            (0..=last).collect::<Vec<_>>(),
            "unbroken to the last row"
        );
        let header = DataTable::default().header_height();
        assert_eq!(last, header + 2, "under the third row, no further");
    }

    /// A frozen column whose type is wider than its name and values still gets its
    /// whole width and the gap before the separator. The width pass left the type row
    /// out, so `id` over `i64` ran onto the line (`i64│`) and a one-letter string
    /// column did not fit at all.
    #[test]
    fn a_frozen_column_is_as_wide_as_its_type() {
        let lf = df!(
            "id" => &[1i64, 2],
            "k" => &["x", "y"],
            "v" => &[-3.5f64, 4.25],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 2;
        state.set_locked_columns(2);
        let area = Rect::new(0, 0, 30, 4);
        let mut buf = Buffer::empty(area);
        DataTable {
            dtype_row: true,
            ..DataTable::default()
        }
        .render(area, &mut buf, &mut state);

        let rows: Vec<String> = (0..area.height)
            .map(|y| row_string(&buf, area, y))
            .collect();
        let rule = crate::glyphs::get().rule;
        assert!(rows[0].contains(&format!(" id k   {rule}")), "{rows:#?}");
        assert!(rows[1].contains(&format!("i64 str {rule}")), "{rows:#?}");
        assert!(rows[2].contains(&format!("  1 x   {rule}")), "{rows:#?}");
    }

    fn list_state() -> DataTableState {
        let many: Vec<String> = (0..12).map(|i| format!("t{i}")).collect();
        let tags = Series::new(
            "tags".into(),
            &[
                Series::new("".into(), &["a", "b"]),
                Series::new("".into(), many),
            ],
        );
        let id = Series::new("id".into(), &[1i64, 2]);
        let more = Series::new(
            "more".into(),
            &[
                Series::new("".into(), &["x"]),
                Series::new("".into(), &["y"]),
            ],
        );
        let lf = DataFrame::new_infer_height(vec![id.into(), tags.into(), more.into()])
            .unwrap()
            .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 2;
        state.collect();
        state
    }

    /// A list cell reads as it did when the buffer held lists as text, and the type
    /// row names the list once the rows have landed, not `str`.
    #[test]
    fn a_list_column_draws_its_items_under_its_list_type() {
        let mut state = list_state();
        let area = Rect::new(0, 0, 80, 4);
        let mut buf = Buffer::empty(area);
        DataTable {
            dtype_row: true,
            ..DataTable::default()
        }
        .render(area, &mut buf, &mut state);
        let rows: Vec<String> = (0..area.height)
            .map(|y| row_string(&buf, area, y))
            .collect();
        assert!(rows[1].contains("list[str]"), "{rows:#?}");
        assert!(!rows[1].contains(" str "), "{rows:#?}");
        assert!(rows[2].contains("[a, b]"), "{rows:#?}");
        // The column's width cap cuts the rest; `exact` tests the whole preview.
        assert!(
            rows[3].contains("[t0, t1, t2, t3, t4, t5, t6, t7"),
            "{rows:#?}"
        );
    }

    /// The display frames keep lists as lists, frozen or scrolling, through a
    /// sideways scroll: a step re-cuts the buffer and formats nothing; only the
    /// cells drawn are formatted.
    #[test]
    fn a_sideways_scroll_keeps_lists_in_the_display_frames() {
        let mut state = list_state();
        state.set_locked_columns(2);
        state.scroll_right();
        let is_list = |df: &DataFrame, name: &str| {
            matches!(df.column(name).unwrap().dtype(), DataType::List(_))
        };
        assert!(is_list(state.locked_df.as_ref().unwrap(), "tags"));
        assert!(is_list(state.df.as_ref().unwrap(), "more"));
    }

    /// A page over columns not drawn yet waits for the draw, which measures them from
    /// the rows on hand and lands it: `Last` ends the page with the last column whole,
    /// and the one before that would not have fitted. Nothing moves before then.
    #[test]
    fn a_page_over_columns_not_drawn_lands_at_the_draw() {
        let names: Vec<String> = (0..30).map(|i| format!("col{i:02}")).collect();
        let columns: Vec<Column> = names
            .iter()
            .enumerate()
            .map(|(i, name)| {
                let value = "x".repeat(3 + i % 5);
                Series::new(name.as_str().into(), vec![value; 3]).into()
            })
            .collect();
        let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 3;
        state.collect();
        let area = Rect::new(0, 0, 50, 4);
        let draw = |state: &mut DataTableState| {
            let mut buf = Buffer::empty(area);
            DataTable::default().render(area, &mut buf, state);
            row_string(&buf, area, 0)
        };
        draw(&mut state);
        assert!(state.drawn_width("col29").is_none(), "not drawn yet");

        state.scroll_columns(ColumnMove::Last);
        assert_eq!(state.termcol_index, 0, "nothing moves before the draw");
        let header = draw(&mut state);
        assert!(header.trim_end().ends_with("col29"), "{header}");
        let start = state.termcol_index;
        assert!(start > 0);
        // The column before the page would not have fitted beside it.
        let room = state.scroll_room.unwrap();
        let used: u16 = names[start - 1..]
            .iter()
            .map(|n| state.shown_width(n).unwrap() + room.padding)
            .sum::<u16>()
            - room.padding;
        assert!(used > room.width, "{used} in {}", room.width);

        // Back, which lands at the draw again; then forward over columns drawn now,
        // which needs none.
        state.scroll_columns(ColumnMove::PageLeft);
        draw(&mut state);
        let back = state.termcol_index;
        assert!(back < start);
        state.scroll_columns(ColumnMove::PageRight);
        assert_eq!(state.termcol_index, start);
        state.scroll_columns(ColumnMove::First);
        assert_eq!(state.termcol_index, 0);
    }

    /// A state over thirty text columns of mixed widths, drawn once at 50 wide.
    fn paging_state() -> (DataTableState, impl Fn(&mut DataTableState)) {
        let columns: Vec<Column> = (0..30)
            .map(|i| {
                let value = "x".repeat(3 + (i * 7) % 11);
                Series::new(format!("col{i:02}").into(), vec![value; 3]).into()
            })
            .collect();
        let lf = DataFrame::new_infer_height(columns).unwrap().lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 3;
        state.collect();
        let draw = |state: &mut DataTableState| {
            let area = Rect::new(0, 0, 50, 4);
            DataTable::default().render(area, &mut Buffer::empty(area), state);
        };
        draw(&mut state);
        (state, draw)
    }

    /// Keys typed faster than frames, over columns not drawn yet, land at the draw in
    /// the order typed, exactly where they land with a frame after each, and keep
    /// waiting while no rows are on hand to measure with.
    #[test]
    fn moves_typed_before_a_draw_land_in_order() {
        use ColumnMove::*;
        let keys = [PageRight, PageRight, StepRight, PageLeft, PageRight];
        let (mut paced, draw) = paging_state();
        for mv in keys {
            paced.scroll_columns(mv);
            draw(&mut paced);
        }
        assert!(paced.termcol_index > 0);

        let (mut typed, draw) = paging_state();
        typed.defer_collect = true;
        for mv in keys {
            typed.scroll_columns(mv);
        }
        draw(&mut typed);
        assert_eq!(typed.termcol_index, 0, "no rows to measure with: they wait");
        typed.defer_collect = false;
        draw(&mut typed);
        assert_eq!(typed.termcol_index, paced.termcol_index);
        assert!(typed.column_moves.is_empty());

        // First drops what waits; nothing is left to land later.
        let (mut first, draw) = paging_state();
        first.scroll_columns(Last);
        first.scroll_columns(StepRight);
        first.scroll_columns(ColumnMove::First);
        draw(&mut first);
        assert_eq!(first.termcol_index, 0);
    }

    /// `[` straight after `]` goes back to the page `]` left, even where the page
    /// before, packed from the right, would start further left.
    #[test]
    fn page_left_after_page_right_retraces() {
        let (mut state, draw) = paging_state();
        let room = state.scroll_room.unwrap();
        let p = room.padding;
        let usable = room.width - room.lead;
        // Three of `a` fill the first page, two of `b` the second with a column too
        // wide for the room after them; packed from the right, the page ending before
        // that wide column would take in two of the `a` columns as well.
        let a = (usable - 2 * p) / 3;
        let b = (usable.saturating_sub(2 * a + 3 * p) / 2)
            .max(crate::widgets::column_widths::MIN_WIDTH);
        let widths = [a, a, a, b, b, usable + 10];
        state.set_width_choices(
            widths
                .iter()
                .enumerate()
                .map(|(i, &w)| (format!("col{i:02}"), WidthChoice::Manual(w))),
        );
        draw(&mut state);
        state.scroll_columns(ColumnMove::PageRight);
        draw(&mut state);
        assert_eq!(state.termcol_index, 3);
        state.scroll_columns(ColumnMove::PageRight);
        draw(&mut state);
        assert_eq!(state.termcol_index, 5);
        let names = state.scrolling_names().to_vec();
        let packed =
            crate::widgets::column_paging::plan(ColumnMove::PageLeft, 5, names.len(), room, |i| {
                state.drawn_width(&names[i])
            });
        assert!(packed < Some(3), "the packed page differs: {packed:?}");
        state.scroll_columns(ColumnMove::PageLeft);
        assert_eq!(state.termcol_index, 3, "back to the page left");
        state.scroll_columns(ColumnMove::PageLeft);
        assert_eq!(state.termcol_index, 0);
        // Not straight after `]`: the page before, packed from the right.
        state.scroll_columns(ColumnMove::PageRight);
        state.scroll_columns(ColumnMove::PageRight);
        state.scroll_columns(ColumnMove::StepRight);
        state.scroll_columns(ColumnMove::StepLeft);
        state.scroll_columns(ColumnMove::PageLeft);
        assert_eq!(Some(state.termcol_index), packed);
    }

    /// The hidden-columns count goes in the blank run after the last column, or not
    /// at all: at no width does it cover the type under a heading. The separator's gap
    /// took the one cell of slack that used to keep `+2 >` clear of `str` at 60
    /// columns, and the count then wrote over the `r`.
    #[test]
    fn the_hidden_count_never_covers_a_type() {
        let lf = df!(
            "k" => &["x"],
            "origin" => &["JFK"],
            "dest" => &["LAX"],
            "tail" => &["N1"],
            "name" => &["Endeavor"],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 1;
        state.set_locked_columns(1);
        let table = || DataTable {
            dtype_row: true,
            ..DataTable::default()
        };
        assert_eq!(table().header_height(), 2, "the count goes on the type row");
        for width in 8..=40 {
            let area = Rect::new(0, 0, width, 3);
            let mut buf = Buffer::empty(area);
            table().render(area, &mut buf, &mut state);
            let names = row_string(&buf, area, 0);
            let types = row_string(&buf, area, 1);
            // Every heading shown whole has its whole type under it; these are all
            // strings, so each starts where its name does.
            for name in ["origin", "dest", "tail", "name"] {
                if let Some(at) = names.find(&format!(" {name}")) {
                    let x = names[..at].chars().count() + 1;
                    let under: String = types.chars().skip(x).take(3).collect();
                    assert_eq!(under, "str", "width {width}:\n{names}\n{types}");
                }
            }
        }
    }

    /// The gap is paid for in the width budget: at every width, the columns right of
    /// the separator are whole or absent. Left out, the last column that fits exactly
    /// comes out one cell short, and a number cut short reads as a different number.
    /// The one exception is a first column with no room for its value at all, which
    /// shows a preview behind the clip marker rather than nothing.
    #[test]
    fn the_separator_gap_never_cuts_a_number_short() {
        let values = ["-987", "654", "-32", "10"];
        let lf = df!(
            "k" => &["x"],
            "a" => &[-987i64],
            "b" => &[654i64],
            "c" => &[-32i64],
            "d" => &[10i64],
        )
        .unwrap()
        .lazy();
        let mut state = DataTableState::new(lf, None, None, None, None, true).unwrap();
        state.visible_rows = 1;
        state.set_locked_columns(1);
        let data_row = DataTable::default().header_height();
        for width in 8..=30 {
            let area = Rect::new(0, 0, width, data_row + 1);
            let mut buf = Buffer::empty(area);
            DataTable::default().render(area, &mut buf, &mut state);
            let row = row_string(&buf, area, data_row);
            let g = crate::glyphs::get();
            let (_, scrolled) = row.split_once(g.rule).expect("a separator");
            for (i, token) in scrolled.split_whitespace().enumerate() {
                let previewed = i == 0 && token.ends_with(g.ellipsis);
                assert!(
                    values.contains(&token) || previewed,
                    "width {width}: {token:?} is cut short in {row:?}"
                );
            }
        }
    }

    fn glyph_sets() -> [&'static crate::glyphs::Glyphs; 2] {
        [crate::glyphs::unicode(), crate::glyphs::ascii()]
    }

    fn set_name(g: &crate::glyphs::Glyphs) -> &'static str {
        if g.unicode { "unicode" } else { "ascii" }
    }

    /// The cells of row `y` from `x0` on, as the terminal shows them: a wide
    /// character once, without the blank its second cell holds.
    fn drawn_from(buf: &Buffer, y: u16, x0: u16) -> String {
        use ratatui::buffer::CellWidth;
        let mut row = String::new();
        let mut x = x0;
        while x < buf.area.right() {
            let symbol = buf[(x, y)].symbol();
            row.push_str(symbol);
            x += symbol.cell_width().max(1);
        }
        row
    }

    /// Draw `state` at `width` × `height` with `table`, row by row as shown.
    fn draw(table: DataTable, state: &mut DataTableState, width: u16, height: u16) -> Vec<String> {
        let area = Rect::new(0, 0, width, height);
        let mut buf = Buffer::empty(area);
        table.render(area, &mut buf, state);
        (0..height).map(|y| drawn_from(&buf, y, 0)).collect()
    }

    /// A state over `df` with its first page buffered for `visible_rows` rows.
    fn state_of(df: &DataFrame, visible_rows: usize) -> DataTableState {
        let mut state =
            DataTableState::new(df.clone().lazy(), None, None, None, None, true).unwrap();
        state.visible_rows = visible_rows;
        state.collect();
        state
    }

    /// A heading wider than the table is clipped, marked, over values that fit whole:
    /// it never leaves the table blank, at any width, in either glyph set, with or
    /// without the type row. It used to drop the column, and with it everything
    /// after, leaving the rail and an off-screen hint over nothing.
    #[test]
    fn a_long_header_never_blanks_the_table() {
        let name = format!("numeric_header_{}", "x".repeat(90));
        let df = DataFrame::new_infer_height(vec![
            Series::new(name.as_str().into(), &[1i64, 22, 333]).into(),
            Series::new("tail".into(), &["t1", "t2", "t3"]).into(),
        ])
        .unwrap();
        for g in glyph_sets() {
            for dtype_row in [false, true] {
                for width in [12u16, 20, 60, 80, 120] {
                    let table = || DataTable {
                        glyphs: g,
                        dtype_row,
                        ..DataTable::default()
                    };
                    let header_h = usize::from(table().header_height());
                    let mut state = state_of(&df, 3);
                    let rows = draw(table(), &mut state, width, header_h as u16 + 3);
                    let ctx = format!(
                        "{} glyphs, type row {dtype_row}, width {width}:\n{}",
                        set_name(g),
                        rows.join("\n")
                    );
                    assert!(rows[0].contains("num"), "{ctx}");
                    for (i, value) in ["1", "22", "333"].iter().enumerate() {
                        assert!(
                            rows[header_h + i].split_whitespace().any(|t| t == *value),
                            "{value} is whole on its row: {ctx}"
                        );
                    }
                    if dtype_row && width >= 20 {
                        assert!(rows[1].contains("i64"), "{ctx}");
                    }
                    // Wider than any cap on automatic widths: always clipped, and from
                    // 60 columns on, with the column after it beside it.
                    assert!(rows[0].contains(g.ellipsis), "{ctx}");
                    if width >= 60 {
                        assert!(rows[0].contains("tail"), "{ctx}");
                    }
                }
            }
        }
    }

    /// A clipped heading gives way to its marks: the name is cut, never the sort
    /// direction, which is state.
    #[test]
    fn a_clipped_heading_keeps_its_sort_mark() {
        let name = format!("numeric_header_{}", "x".repeat(90));
        let df =
            DataFrame::new_infer_height(vec![Series::new(name.as_str().into(), &[1i64]).into()])
                .unwrap();
        for g in glyph_sets() {
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            }
            .with_sort(vec![name.clone()], vec![true]);
            let area = Rect::new(0, 0, 30, 2);
            let mut buf = Buffer::empty(area);
            let mut ts = TableState::default();
            table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
            let header = header_row_string(&buf, area);
            assert!(
                header
                    .trim_end()
                    .ends_with(&format!("{}{}", g.ellipsis, g.sort_desc)),
                "{}: {header:?}",
                set_name(g)
            );
        }
    }

    /// A heading whose drawn width is not its string's width (`لا` draws two cells,
    /// a halfwidth sound mark one) is drawn whole over right-aligned numbers, with its
    /// sort mark after it. ratatui's own alignment pushed the last letter off the cell,
    /// and the mark landed on it.
    #[test]
    fn a_heading_is_placed_by_the_cells_it_draws() {
        for name in ["الاسم", "ｶﾞｷﾞ"] {
            let df = DataFrame::new_infer_height(vec![
                Series::new(name.into(), &[1i64]).into(),
                Series::new("tail".into(), &["x"]).into(),
            ])
            .unwrap();
            let table = DataTable::default().with_sort(vec![name.to_string()], vec![false]);
            let mark = table.glyphs.sort_asc;
            let area = Rect::new(0, 0, 30, 3);
            let mut buf = Buffer::empty(area);
            let mut ts = TableState::default();
            table.render_dataframe(&df, area, &mut buf, &mut ts, false, 0);
            let rows: Vec<String> = (0..3).map(|y| drawn_from(&buf, y, 0)).collect();
            assert!(rows[0].starts_with(&format!("{name}{mark}")), "{rows:#?}");
            let width = crate::glyphs::cell_width(name) + 1;
            assert!(
                rows[1].starts_with(&format!("{:>width$} x", "1")),
                "{rows:#?}"
            );
        }
    }

    /// A number too wide for the whole table shows its leading digits behind the clip
    /// marker: something rather than nothing, and never its trailing digits passing
    /// for a whole number, which is what ratatui's own cut of a right-aligned value
    /// would show.
    #[test]
    fn a_number_wider_than_the_table_is_a_marked_preview() {
        let full = "-1234567890123456789";
        let df = df!("n" => &[-1234567890123456789i64]).unwrap();
        for g in glyph_sets() {
            for width in 3u16..=24 {
                let mut state = state_of(&df, 1);
                let table = DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let rows = draw(table, &mut state, width, 2);
                let shown: String = rows[1].chars().skip(1).collect::<String>();
                let shown = shown.trim();
                let ctx = format!("{} glyphs, width {width}: {rows:?}", set_name(g));
                if usize::from(width) > full.len() {
                    assert_eq!(shown, full, "{ctx}");
                } else if g.ellipsis.starts_with(shown) {
                    // Room for no more than the marker.
                    assert!(!shown.is_empty(), "{ctx}");
                } else {
                    let kept = shown
                        .strip_suffix(g.ellipsis)
                        .unwrap_or_else(|| panic!("a clipped number carries the marker: {ctx}"));
                    assert!(full.starts_with(kept), "{ctx}");
                }
            }
        }
    }

    /// Wide characters are measured in cells: a column of four CJK characters is
    /// eight cells wide, and shows whole beside the columns after it while there is
    /// room. Counted in characters, it was given four and clipped with space to spare.
    #[test]
    fn wide_characters_are_measured_in_cells() {
        let values = ["東京大阪", "京都横浜", "名古屋市"];
        let df = df!(
            "a" => &values,
            "b" => &[1i64, 2, 3],
            "tail" => &["x", "y", "z"],
        )
        .unwrap();
        for g in glyph_sets() {
            for width in [16u16, 30, 80] {
                let mut state = state_of(&df, 3);
                let table = DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let rows = draw(table, &mut state, width, 4);
                let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                assert!(rows[0].contains("tail"), "{ctx}");
                for (i, value) in values.iter().enumerate() {
                    assert!(rows[1 + i].contains(value), "{ctx}");
                }
            }
        }
    }

    /// A value cut where it meets the edge keeps whole graphemes and gains the clip
    /// marker: no half of a wide character, no accent split from its letter, no
    /// joined emoji broken apart, at any width, in either glyph set.
    #[test]
    fn a_clipped_cell_keeps_whole_graphemes() {
        let values = [
            "東京大阪名古屋横浜",
            "e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}e\u{301}",
            "👩\u{200d}👩\u{200d}👧👍🏽🇯🇵 and more",
            "plain text that runs on",
        ];
        let df = df!("id" => &[1i64, 2, 3, 4], "text" => &values).unwrap();
        for g in glyph_sets() {
            for width in 4u16..=30 {
                let mut state = state_of(&df, 4);
                let area = Rect::new(0, 0, width, 5);
                let mut buf = Buffer::empty(area);
                DataTable {
                    glyphs: g,
                    ..DataTable::default()
                }
                .render(area, &mut buf, &mut state);
                // The rail, `id` two cells wide, and one cell of padding.
                let text_x = 4;
                if text_x >= width {
                    continue;
                }
                for (i, value) in values.iter().enumerate() {
                    let y = 1 + i as u16;
                    let shown = drawn_from(&buf, y, text_x);
                    let shown = shown.trim_end();
                    let ctx = format!("{} glyphs, width {width}, row {i}: {shown:?}", set_name(g));
                    if shown.is_empty() || shown == *value {
                        continue;
                    }
                    let kept = shown
                        .strip_suffix(g.ellipsis)
                        .unwrap_or_else(|| panic!("a clipped value is marked: {ctx}"));
                    let span = Span::raw(*value);
                    let mut whole = String::new();
                    for grapheme in span.styled_graphemes(Style::default()) {
                        if whole.len() >= kept.len() {
                            break;
                        }
                        whole.push_str(grapheme.symbol);
                    }
                    assert_eq!(whole, kept, "{ctx}");
                    // And each cell holds a whole grapheme of the value or the marker.
                    for x in text_x..width {
                        let symbol = buf[(x, y)].symbol();
                        assert!(
                            symbol == " "
                                || g.ellipsis.contains(symbol)
                                || span
                                    .styled_graphemes(Style::default())
                                    .any(|gr| gr.symbol == symbol),
                            "cell {x} holds {symbol:?}: {ctx}"
                        );
                    }
                }
            }
        }
    }

    /// A frozen text column wider than the window is clipped, marked and still
    /// frozen, with the column after it beside it. It used to vanish, leaving only
    /// the column after it.
    #[test]
    fn a_frozen_long_text_is_clipped_not_dropped() {
        let url = format!("https://example.com/{}", "long-segment/".repeat(15));
        let df = df!("url" => &[url.as_str(), "short"], "tail" => &[7i64, 8]).unwrap();
        for g in glyph_sets() {
            for width in [40u16, 60, 80, 120] {
                let mut state = state_of(&df, 2);
                state.set_locked_columns(1);
                let table = DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let rows = draw(table, &mut state, width, 3);
                let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                let (frozen, scrolled) = rows[0].split_once(g.rule).expect(&ctx);
                assert!(frozen.contains("url"), "{ctx}");
                assert!(scrolled.contains("tail"), "{ctx}");
                let (frozen, scrolled) = rows[1].split_once(g.rule).expect(&ctx);
                assert!(frozen.contains("https://exa"), "{ctx}");
                assert!(frozen.trim_end().ends_with(g.ellipsis), "{ctx}");
                assert!(scrolled.split_whitespace().any(|t| t == "7"), "{ctx}");
                assert_eq!(state.frozen_shown(), 1, "{ctx}");
            }
        }
    }

    /// The frozen columns are measured on the rows on screen, like the rest. They
    /// were measured on the head of the buffer, which is not the page on screen once
    /// the view has moved into it, so a longer value on screen was cut short.
    #[test]
    fn frozen_columns_are_measured_on_the_rows_on_screen() {
        let long = "a much longer frozen value";
        let names: Vec<String> = (0..400)
            .map(|i| {
                if i == 302 {
                    long.to_string()
                } else {
                    format!("n{i}")
                }
            })
            .collect();
        let df = df!("name" => names, "v" => (0..400i64).collect::<Vec<_>>()).unwrap();
        let mut state = state_of(&df, 5);
        state.set_locked_columns(1);
        state.scroll_to(300);
        state.collect();
        assert!(
            state.buffered_start_row < state.start_row,
            "the page is not the head of the buffer: {} vs {}",
            state.buffered_start_row,
            state.start_row
        );
        let rows = draw(DataTable::default(), &mut state, 80, 6);
        assert!(
            rows.iter().any(|row| row.contains(long)),
            "the value on screen is whole: {rows:#?}"
        );
    }

    /// Frozen columns are spaced like scrolling ones: the configured padding on both
    /// sides of the separator, at every setting. The frozen side used a hardcoded
    /// single space.
    #[test]
    fn frozen_and_scrolling_columns_share_the_padding() {
        let df = df!(
            "id" => &[1i64, 2],
            "k" => &["x", "y"],
            "v" => &[3i64, 4],
            "w" => &[5i64, 6],
        )
        .unwrap();
        for padding in [0u16, 1, 2, 3] {
            let mut state = state_of(&df, 2);
            state.set_locked_columns(2);
            let table = DataTable {
                table_cell_padding: padding,
                ..DataTable::default()
            };
            let rows = draw(table, &mut state, 40, 3);
            let gap = " ".repeat(usize::from(padding));
            let rule = crate::glyphs::get().rule;
            assert!(
                rows[0].starts_with(&format!(" id{gap}k {rule} v{gap}w")),
                "padding {padding}: {rows:#?}"
            );
            assert!(
                rows[1].contains(&format!(" 1{gap}x {rule} 3{gap}5")),
                "padding {padding}: {rows:#?}"
            );
        }
    }

    /// The column names, in order, for the frozen-prefix tests.
    const PHONETIC: [&str; 6] = ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot"];

    fn phonetic_frame() -> DataFrame {
        let columns: Vec<Column> = PHONETIC
            .iter()
            .map(|n| {
                Series::new(
                    (*n).into(),
                    &[format!("{n}-value-one"), format!("{n}-value-two")],
                )
                .into()
            })
            .collect();
        DataFrame::new_infer_height(columns).unwrap()
    }

    /// Every column name some frame shows while scrolling right from the start.
    fn names_reached(
        state: &mut DataTableState,
        g: &'static crate::glyphs::Glyphs,
        width: u16,
    ) -> Vec<&'static str> {
        let mut seen = Vec::new();
        for _ in 0..PHONETIC.len() + 2 {
            let table = DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table, state, width, 3);
            for name in PHONETIC {
                if rows[0].contains(name) && !seen.contains(&name) {
                    seen.push(name);
                }
            }
            state.scroll_right();
        }
        seen
    }

    /// Frozen columns that cannot all fit beside a usable scrolling column: as many
    /// as fit stay frozen, the broken rule says some had to scroll, those lead the
    /// scrolling side, every column stays reachable, and the request stands, so a
    /// wider window freezes them all again.
    #[test]
    fn a_frozen_prefix_too_wide_scrolls_until_there_is_room() {
        let df = phonetic_frame();
        for g in glyph_sets() {
            for row_numbers in [false, true] {
                let mut state = state_of(&df, 2);
                state.row_numbers = row_numbers;
                state.set_locked_columns(4);
                let table = || DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let rows = draw(table(), &mut state, 60, 3);
                let ctx = format!(
                    "{} glyphs, row numbers {row_numbers}: {rows:#?}",
                    set_name(g)
                );
                assert_eq!(state.locked_columns_count(), 4, "{ctx}");
                let shown = state.frozen_shown();
                assert!((1..4).contains(&shown), "{shown} frozen: {ctx}");
                let (frozen, scrolled) = rows[0].split_once(g.rule_broken).expect(&ctx);
                assert!(!rows[0].contains(g.rule), "{ctx}");
                assert!(frozen.contains(PHONETIC[0]), "{ctx}");
                assert!(
                    scrolled.trim_start().starts_with(PHONETIC[shown]),
                    "the first column left out leads the scrolling side: {ctx}"
                );

                let reached = names_reached(&mut state, g, 60);
                assert_eq!(reached.len(), PHONETIC.len(), "{reached:?}: {ctx}");

                let rows = draw(table(), &mut state, 160, 3);
                assert_eq!(state.frozen_shown(), 4, "{rows:#?}");
                let (frozen, _) = rows[0].split_once(g.rule).expect("the plain rule");
                for name in &PHONETIC[..4] {
                    assert!(frozen.contains(name), "{rows:#?}");
                }
            }
        }
    }

    /// A rollback puts back the frozen fit its columns were sliced for: a wider
    /// layout since then must re-slice them, or the columns that had to scroll show
    /// twice, frozen and scrolling.
    #[test]
    fn a_rollback_keeps_the_frozen_fit_its_columns_were_sliced_for() {
        let df = phonetic_frame();
        let mut state = state_of(&df, 2);
        state.set_locked_columns(4);
        draw(DataTable::default(), &mut state, 60, 3);
        assert!(state.frozen_shown() < 4);
        let saved = state.rollback_point();
        draw(DataTable::default(), &mut state, 200, 3);
        assert_eq!(state.frozen_shown(), 4);
        state.roll_back(saved);
        let rows = draw(DataTable::default(), &mut state, 200, 3);
        for name in PHONETIC {
            assert_eq!(rows[0].matches(name).count(), 1, "{name}: {rows:#?}");
        }
    }

    /// With every column frozen there is nothing to scroll, until the window is too
    /// narrow for them all: then the ones that do not fit scroll, and each is still
    /// reachable.
    #[test]
    fn with_every_column_frozen_each_is_still_reachable() {
        let df = phonetic_frame();
        for g in glyph_sets() {
            let mut state = state_of(&df, 2);
            state.set_locked_columns(PHONETIC.len());
            let table = || DataTable {
                glyphs: g,
                ..DataTable::default()
            };
            let rows = draw(table(), &mut state, 200, 3);
            assert_eq!(state.frozen_shown(), PHONETIC.len(), "{rows:#?}");
            assert!(rows[0].contains(g.rule), "{rows:#?}");
            assert!(!rows[0].contains(g.arrow_right), "{rows:#?}");

            let rows = draw(table(), &mut state, 60, 3);
            assert!(state.frozen_shown() < PHONETIC.len(), "{rows:#?}");
            assert!(rows[0].contains(g.rule_broken), "{rows:#?}");
            let reached = names_reached(&mut state, g, 60);
            assert_eq!(reached.len(), PHONETIC.len(), "{reached:?}");
        }
    }

    /// The page from the issue's reproduction: one description runs to a long URL.
    /// That page still shows its rows, with the description clipped and marked
    /// rather than any column vanishing for it.
    #[test]
    fn a_page_with_one_long_value_is_not_blank() {
        let url = format!("https://example.com/{}", "long-segment/".repeat(15));
        let n = 80usize;
        let df = df!(
            "id" => (0..n as i64).collect::<Vec<_>>(),
            "description" => (0..n)
                .map(|i| if i == 24 { url.clone() } else { format!("item {i}") })
                .collect::<Vec<_>>(),
            "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
            "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
        )
        .unwrap();
        for g in glyph_sets() {
            for width in [60u16, 80, 120] {
                let mut state = state_of(&df, 20);
                state.page_down();
                state.collect();
                let table = DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let rows = draw(table, &mut state, width, 21);
                let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                assert!(rows[0].contains("id"), "{ctx}");
                assert!(rows[0].contains("desc"), "{ctx}");
                let long = rows
                    .iter()
                    .find(|row| row.contains("https://example.com/"))
                    .expect(&ctx);
                assert!(long.contains(g.ellipsis), "{ctx}");
                for row in &rows[1..] {
                    assert!(!row.trim().is_empty(), "{ctx}");
                }
            }
        }
    }

    /// The issue's long-URL frame: six columns over 80 rows, every description short
    /// but row 24's.
    fn long_url_frame() -> DataFrame {
        let url = format!("https://example.com/{}", "long-segment/".repeat(15));
        let n = 80usize;
        let start = NaiveDate::from_ymd_opt(2024, 1, 1)
            .unwrap()
            .and_hms_opt(0, 0, 0)
            .unwrap();
        df!(
            "id" => (0..n as i64).collect::<Vec<_>>(),
            "description" => (0..n)
                .map(|i| if i == 24 { url.clone() } else { format!("item {i}") })
                .collect::<Vec<_>>(),
            "amount" => (0..n).map(|i| i as f64 * 1.5).collect::<Vec<_>>(),
            "status" => (0..n).map(|i| if i % 2 == 0 { "open" } else { "closed" }).collect::<Vec<_>>(),
            "timestamp" => (0..n)
                .map(|i| start + chrono::Duration::hours(i as i64))
                .collect::<Vec<_>>(),
            "uuid" => (0..n)
                .map(|i| format!("00000000-0000-0000-0000-{:012x}", i * 7919 + 1))
                .collect::<Vec<_>>(),
        )
        .unwrap()
    }

    /// Paging down past a page with one long value, and back, moves no column: the
    /// heading row is the same on every page, at every width, in both glyph sets.
    /// Measured per page, the long URL turned a six-column view into a two-column one.
    /// On its page the URL is clipped and marked.
    #[test]
    fn widths_hold_still_across_pages() {
        let df = long_url_frame();
        for g in glyph_sets() {
            for width in [60u16, 80, 120] {
                let mut state = state_of(&df, 20);
                let table = || DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let first = draw(table(), &mut state, width, 21);
                let ctx =
                    |rows: &[String]| format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                if width >= 80 {
                    assert!(first[0].contains("timestamp"), "{}", ctx(&first));
                }
                let mut saw_url = false;
                for step in 0..6 {
                    if step < 3 {
                        state.page_down();
                    } else {
                        state.page_up();
                    }
                    state.collect();
                    let rows = draw(table(), &mut state, width, 21);
                    assert_eq!(rows[0], first[0], "page {step}: {}", ctx(&rows));
                    if let Some(row) = rows.iter().find(|r| r.contains("https://")) {
                        saw_url = true;
                        let clipped = row.split_whitespace().nth(1).unwrap();
                        assert!(clipped.ends_with(g.ellipsis), "{}", ctx(&rows));
                    }
                }
                assert!(saw_url, "the long URL's page was drawn");
            }
        }
    }

    /// A frozen column keeps its width across pages, so the number of columns that
    /// stay frozen does not change with the page either.
    #[test]
    fn frozen_columns_hold_still_across_pages() {
        let df = long_url_frame();
        for g in glyph_sets() {
            for width in [60u16, 80, 120] {
                let mut state = state_of(&df, 20);
                state.set_locked_columns(3);
                let table = || DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let first = draw(table(), &mut state, width, 21);
                let frozen = state.frozen_shown();
                for _ in 0..3 {
                    state.page_down();
                    state.collect();
                    let rows = draw(table(), &mut state, width, 21);
                    let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                    assert_eq!(state.frozen_shown(), frozen, "{ctx}");
                    assert_eq!(rows[0], first[0], "{ctx}");
                }
            }
        }
    }

    /// A number's column widens for a wider number on a later page, so it is never
    /// cut, and does not narrow again on a page of narrower ones.
    #[test]
    fn a_number_column_widens_and_stays_wide() {
        let values: Vec<i64> = (0..60)
            .map(|i| if i == 30 { 123_456_789 } else { i })
            .collect();
        let df = df!("n" => values, "t" => (0..60).map(|i| format!("t{i}")).collect::<Vec<_>>())
            .unwrap();
        let mut state = state_of(&df, 20);
        draw(DataTable::default(), &mut state, 60, 21);
        assert_eq!(state.shown_width("n"), Some(2));
        state.page_down();
        state.collect();
        let rows = draw(DataTable::default(), &mut state, 60, 21);
        assert!(rows.iter().any(|r| r.contains("123456789")), "{rows:#?}");
        state.page_down();
        state.collect();
        draw(DataTable::default(), &mut state, 60, 21);
        assert_eq!(state.shown_width("n"), Some(9));
    }

    /// A struct is a preview like text: one long value on a later page is clipped
    /// at the cap, and the columns after it stay on screen on the pages after.
    #[test]
    fn a_long_struct_value_does_not_widen_its_column_for_good() {
        let n = 60usize;
        let y: Vec<String> = (0..n)
            .map(|i| {
                if i == 30 {
                    "a very long struct value ".repeat(6)
                } else {
                    "short".to_string()
                }
            })
            .collect();
        let s = StructChunked::from_series(
            "s".into(),
            n,
            [
                Series::new("x".into(), (0..n as i64).collect::<Vec<_>>()),
                Series::new("y".into(), y),
            ]
            .iter(),
        )
        .unwrap()
        .into_series();
        let df = DataFrame::new_infer_height(vec![
            Series::new("id".into(), (0..n as i64).collect::<Vec<_>>()).into(),
            s.into(),
            Series::new("tail".into(), (0..n as i64).collect::<Vec<_>>()).into(),
        ])
        .unwrap();
        let mut state = state_of(&df, 20);
        for page in 0..3 {
            let rows = draw(DataTable::default(), &mut state, 80, 21);
            assert!(rows[0].contains("tail"), "page {page}: {rows:#?}");
            assert!(
                state.shown_width("s").is_some_and(|w| w <= 32),
                "page {page}: {rows:#?}"
            );
            if page == 1 {
                let long = rows.iter().find(|r| r.contains("{30,")).unwrap();
                assert!(long.contains(crate::glyphs::get().ellipsis), "{rows:#?}");
            }
            state.page_down();
            state.collect();
        }
    }

    /// Automatic text and headings stop at two fifths of the terminal, marked where
    /// cut: 32 cells at 80 columns, 48 at 120.
    #[test]
    fn automatic_text_stops_at_the_cap_and_is_marked() {
        let long = "x".repeat(200);
        let df = df!("text" => &[long.as_str()], "tail" => &[1i64]).unwrap();
        for g in glyph_sets() {
            for (width, cap) in [(80u16, 32u16), (120, 48)] {
                let mut state = state_of(&df, 1);
                let table = DataTable {
                    glyphs: g,
                    ..DataTable::default()
                };
                let rows = draw(table, &mut state, width, 2);
                let ctx = format!("{} glyphs, width {width}: {rows:#?}", set_name(g));
                assert_eq!(state.shown_width("text"), Some(cap), "{ctx}");
                let row: String = rows[1].chars().skip(1).collect();
                let value = row.split_whitespace().next().unwrap();
                assert!(value.ends_with(g.ellipsis), "{ctx}");
                assert_eq!(crate::glyphs::cell_width(value), usize::from(cap), "{ctx}");
                assert!(rows[0].contains("tail"), "{ctx}");
            }
        }
    }

    /// The last column drawn takes the room to the table's right edge: a long text on
    /// the far right runs to the edge rather than stopping at the cap. A width set by
    /// hand is drawn as set, and a number stays under its heading.
    #[test]
    fn the_last_column_runs_to_the_right_edge() {
        let long = "x".repeat(200);
        let df = df!("id" => &[1i64], "text" => &[long.as_str()]).unwrap();
        let ellipsis = crate::glyphs::get().ellipsis;
        let mut state = state_of(&df, 1);
        let rows = draw(DataTable::default(), &mut state, 120, 2);
        let value = rows[1].trim_end();
        assert!(value.ends_with(ellipsis), "{rows:#?}");
        assert_eq!(crate::glyphs::cell_width(value), 120, "{rows:#?}");
        assert_eq!(state.shown_width("text"), Some(48), "learned at the cap");
        assert!(state.on_screen_width("text").unwrap() > 48, "drawn past it");

        state.set_width_choices([("text".to_string(), WidthChoice::Manual(20))]);
        let rows = draw(DataTable::default(), &mut state, 120, 2);
        assert_eq!(state.shown_width("text"), Some(20), "{rows:#?}");
        assert!(
            crate::glyphs::cell_width(rows[1].trim_end()) < 40,
            "{rows:#?}"
        );

        let df = df!("text" => &["ab"], "n" => &[5i64]).unwrap();
        let mut state = state_of(&df, 1);
        let rows = draw(DataTable::default(), &mut state, 120, 2);
        assert!(
            crate::glyphs::cell_width(rows[1].trim_end()) < 20,
            "{rows:#?}"
        );
    }

    /// The cap follows the terminal, not the table: a sidebar narrowing the table
    /// moves no column.
    #[test]
    fn a_sidebar_moves_no_column() {
        let df = long_url_frame();
        let mut state = state_of(&df, 20);
        state.page_down();
        state.collect();
        let table = || DataTable {
            screen_width: 120,
            ..DataTable::default()
        };
        let whole = draw(table(), &mut state, 120, 21);
        let widths: Vec<_> = ["id", "description", "amount"]
            .iter()
            .map(|c| state.shown_width(c))
            .collect();
        let beside = draw(table(), &mut state, 70, 21);
        for (c, before) in ["id", "description", "amount"].iter().zip(&widths) {
            assert_eq!(state.shown_width(c), *before, "{c}: {beside:#?}");
        }
        assert!(
            whole[0].starts_with(&beside[0][..40]),
            "{whole:#?} {beside:#?}"
        );
    }

    /// A width set by hand: exact for text, values clipped and marked; it survives
    /// paging, scrolling, reordering, hiding and resizing; fit and reset change it.
    #[test]
    fn a_manual_width_survives_everything_but_fit_and_reset() {
        let df = long_url_frame();
        let mut state = state_of(&df, 20);
        state.set_width_choices([("description".to_string(), WidthChoice::Manual(6))]);
        let rows = draw(DataTable::default(), &mut state, 80, 21);
        assert_eq!(state.shown_width("description"), Some(6), "{rows:#?}");
        // Six cells, the ellipsis among them: `item …`, or `ite...` in ASCII.
        let ellipsis = crate::glyphs::get().ellipsis;
        let kept = 6 - crate::glyphs::display_width(ellipsis);
        let clipped = format!("{}{ellipsis}", &"item 10"[..kept]);
        assert!(rows.iter().any(|r| r.contains(&clipped)), "{rows:#?}");

        state.page_down();
        state.collect();
        state.scroll_right();
        draw(DataTable::default(), &mut state, 80, 21);
        state.scroll_left();
        // Moved to the end, then hidden and shown again.
        let mut order = state.headers();
        order.retain(|c| c != "description");
        order.push("description".to_string());
        state.set_column_order(order.clone());
        draw(DataTable::default(), &mut state, 200, 21);
        assert_eq!(state.shown_width("description"), Some(6));
        order.pop();
        state.set_column_order(order.clone());
        draw(DataTable::default(), &mut state, 40, 21);
        order.insert(1, "description".to_string());
        state.set_column_order(order);
        draw(DataTable::default(), &mut state, 120, 21);
        assert_eq!(state.width_choice("description"), WidthChoice::Manual(6));
        assert_eq!(state.shown_width("description"), Some(6));

        // Fit takes the rows on screen: page two holds the URL.
        state.set_width_choices([("description".to_string(), WidthChoice::Fit)]);
        draw(DataTable::default(), &mut state, 120, 21);
        let url_width = 20 + 13 * 15;
        assert_eq!(
            state.width_choice("description"),
            WidthChoice::Manual(url_width)
        );
        state.reset();
        assert_eq!(state.width_choice("description"), WidthChoice::Auto);
    }

    /// A column scrolled out of view is fitted to the rows on screen too.
    #[test]
    fn a_column_out_of_view_is_fitted_to_the_page() {
        let df = long_url_frame();
        let mut state = state_of(&df, 20);
        state.page_down();
        state.collect();
        for _ in 0..3 {
            state.scroll_right();
        }
        draw(DataTable::default(), &mut state, 80, 21);
        state.set_width_choices([("description".to_string(), WidthChoice::Fit)]);
        let rows = draw(DataTable::default(), &mut state, 80, 21);
        assert!(!rows[0].contains("description"), "{rows:#?}");
        assert_eq!(
            state.width_choice("description"),
            WidthChoice::Manual(20 + 13 * 15)
        );
    }

    /// A number column set narrower than its numbers still shows them whole.
    #[test]
    fn a_number_column_set_narrow_still_shows_whole_numbers() {
        let df = df!("n" => &[1_234_567i64, 2], "t" => &["a", "b"]).unwrap();
        let mut state = state_of(&df, 2);
        state.set_width_choices([("n".to_string(), WidthChoice::Manual(4))]);
        let rows = draw(DataTable::default(), &mut state, 40, 3);
        assert!(rows[1].contains("1234567"), "{rows:#?}");
    }

    /// The same name with another type, as a query can make it, is another column:
    /// it starts from an automatic width, and the first gets its own back.
    #[test]
    fn a_column_whose_type_changes_starts_afresh() {
        let df = df!("a" => &["x", "y"], "n" => &[1i64, 2]).unwrap();
        let mut state = state_of(&df, 2);
        state.set_width_choices([("a".to_string(), WidthChoice::Manual(9))]);
        state.query("select a: n".to_string());
        assert_eq!(state.width_choice("a"), WidthChoice::Auto);
        state.query("select a, n".to_string());
        assert_eq!(state.width_choice("a"), WidthChoice::Manual(9));
    }

    /// A column ending at the table's edge with more after it leaves its heading's
    /// last cell to the off-screen hint, so the hint covers neither a letter nor the
    /// clip marker, and the values keep the full width: a number that fits is whole,
    /// never a preview.
    #[test]
    fn the_offscreen_hint_takes_a_heading_cell_not_a_value_cell() {
        let df = df!(
            "aaaaaaaaaa" => &["aaaaaaaaaa"],
            "bbbbbbb" => &[1_234_567i64],
            "c" => &["x"],
        )
        .unwrap();
        for g in glyph_sets() {
            for dtype_row in [false, true] {
                let table = DataTable {
                    glyphs: g,
                    dtype_row,
                    ..DataTable::default()
                };
                let header_h = usize::from(table.header_height());
                let mut state = state_of(&df, 1);
                // The rail, ten cells, a gap and seven: the second column ends at the edge.
                let rows = draw(table, &mut state, 19, header_h as u16 + 1);
                let ctx = format!("{} glyphs, type row {dtype_row}: {rows:#?}", set_name(g));
                assert!(rows[header_h].ends_with(" 1234567"), "{ctx}");
                let hint_row = &rows[header_h - 1];
                assert!(hint_row.ends_with(g.arrow_right), "{ctx}");
                let before = hint_row.strip_suffix(g.arrow_right).unwrap();
                if dtype_row {
                    assert!(before.trim_end().ends_with("i64"), "{ctx}");
                } else {
                    assert!(before.ends_with(g.ellipsis), "{ctx}");
                }
            }
        }
    }

    /// A followed file's page near its end, and a filtered view's count after rows
    /// arrive, are read from the marks the watcher made, not from the file's start.
    #[test]
    fn a_followed_view_reads_and_counts_from_its_marks() {
        use std::io::Write as _;
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("grow.csv");
        let mut text = String::from("t,n\n");
        for i in 0..20_000 {
            text.push_str(&format!("{i},{}\n", i % 7));
        }
        std::fs::write(&path, &text).unwrap();
        let scan = LazyCsvReader::new(PlRefPath::try_from_path(&path).unwrap())
            .with_ignore_errors(true)
            .finish()
            .unwrap();
        let options = crate::OpenOptions::default();
        let (lf, tail) =
            crate::follow::bound_to_complete(scan, &path, crate::FileFormat::Csv, &options)
                .unwrap();
        let (tx, rx) = std::sync::mpsc::channel();
        let mut state = DataTableState::new(lf, None, None, None, None, false).unwrap();
        let rows = tail.rows();
        state.start_following(crate::follow::Follow::start(
            tail,
            std::time::Duration::from_secs(3_600),
            tx,
            None,
        ));
        state.follow_to(rows, false);
        let from_marks = |lf: &LazyFrame| format!("{:?}", lf.logical_plan).contains("FOLLOWED");
        let page = state.buffer_lf(19_990, 10).unwrap();
        assert!(from_marks(&page));
        let t = |df: DataFrame| df.column("t").unwrap().i64().unwrap().to_vec();
        assert_eq!(t(page.collect().unwrap()).first(), Some(&Some(19_990)));

        state.defer_collect = true;
        state.filter(vec![FilterStatement {
            columns: Vec::new(),
            column: "n".to_string(),
            operator: crate::filter_modal::FilterOperator::Eq,
            value: "3".to_string(),
            logical_op: crate::filter_modal::LogicalOperator::And,
        }]);
        let matches = |n: usize| (0..n).filter(|i| i % 7 == 3).count();
        // The first count of the filter reads the whole file.
        assert!(state.source_counter().is_none());
        state.set_num_rows(matches(20_000));

        let mut out = std::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .unwrap();
        let more: String = (20_000..20_050)
            .map(|i| format!("{i},{}\n", i % 7))
            .collect();
        out.write_all(more.as_bytes()).unwrap();
        let follow = state.follow_mut().unwrap();
        follow.check_now();
        // A watcher that hears changes may report before the check it was asked for.
        let (rows, restarted) = loop {
            let crate::AppEvent::Followed(news) =
                rx.recv_timeout(std::time::Duration::from_secs(30)).unwrap()
            else {
                panic!("the watcher said something else");
            };
            follow.take(&news.change);
            if follow.waiting() == 50 {
                break follow.catch_up();
            }
        };
        assert_eq!(rows, 20_050);
        state.follow_to(rows, restarted);
        assert!(!state.is_num_rows_valid());
        let counter = state.source_counter().expect("counts the new rows alone");
        assert_eq!(counter().unwrap(), matches(20_050));
        state.set_num_rows(matches(20_050));
        let last = state.buffer_lf(matches(20_050) - 3, 3).unwrap();
        assert!(from_marks(&last));
        let expected: Vec<_> = (0..20_050i64).filter(|i| i % 7 == 3).map(Some).collect();
        assert_eq!(t(last.collect().unwrap()), expected[expected.len() - 3..]);
    }
}
