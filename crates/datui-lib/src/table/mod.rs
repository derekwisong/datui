use color_eyre::Result;
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use polars::frame::PivotColumnNaming;
use polars::prelude::*;
use ratatui::widgets::TableState;

use crate::OpenOptions;
use crate::analysis::statistics::collect_lazy;
use crate::cloud::local_copy::RemoteObject;
use crate::filter_modal::FilterStatement;
use crate::formats::readers::csv::Decompressed;
use crate::formats::readers::{Read, Typing};
use crate::numfmt::{self};
use crate::pivot_melt_modal::{MeltSpec, PivotAggregation, PivotSpec, ReshapeSource};
use crate::python_script::{SidebarFilter, Step};
#[cfg(feature = "sql")]
use crate::query::sql_plan::{
    count_subquery_values_once, leftover_subquery_value_columns, ordered_by, stable_order,
};
use crate::query::{ParsedQuery, parse_query_over};
use crate::widgets::column_paging::{ColumnMove, CursorMove, OnScreen, Room};
use crate::widgets::column_widths::{ColumnWidths, WidthChoice};

/// The view on screen: frames, what built them, rows held and layout. One value, so a
/// checkpoint is a clone and restoring is one assignment.
#[derive(Clone)]
pub(crate) struct View {
    lf: LazyFrame,
    /// `lf` before its sort, when it has one. See [`DataTableState::analysis_lf`].
    unsorted_lf: Option<LazyFrame>,
    /// What filters and sort apply to: the active query's result (DSL, SQL or fuzzy), the
    /// last pivot/melt, or `original_lf`. Pipeline: original → query/reshape (`base_lf`)
    /// → filters → sort (`lf`) → column order (at collect), so filters never discard the
    /// query.
    base_lf: LazyFrame,
    /// `base_lf`'s schema as built, checked against by column changes without resolving
    /// the plan.
    base_schema: Arc<Schema>,
    pub(crate) df: Option<DataFrame>, // Scrollable columns dataframe
    pub(crate) locked_df: Option<DataFrame>, // Locked columns dataframe
    pub(crate) start_row: usize,
    /// The column cursor's column, by name, so it follows hide, reorder and freeze.
    /// `None` is the first column. See [`DataTableState::current_column`].
    cursor_column: Option<String>,
    /// The cursor's position in `column_order` when placed: if its column is hidden, the
    /// cursor goes to the one now there.
    cursor_at: usize,
    pub(crate) schema: Arc<Schema>,
    num_rows: usize,
    /// When true, collect() skips the len() query.
    pub(crate) num_rows_valid: bool,
    /// Bumped whenever `lf` changes (`invalidate_num_rows`); a background `len()` whose
    /// generation no longer matches is dropped. Separate from `task_generation` so a
    /// scroll does not restart a count. Seeded from a process-wide counter, unique across
    /// datasets, so a closed dataset's count never lands on the next.
    len_generation: u64,
    filters: Vec<FilterStatement>,
    sort_columns: Vec<String>,
    /// Per `sort_columns` entry, whether it is descending; always the same length.
    sort_descending: Vec<bool>,
    sort_ascending: bool,
    /// Last executed DSL query. At most one `active_*` query is set; running one clears the
    /// others.
    active_query: String,
    /// Last executed SQL (Sql tab).
    active_sql_query: String,
    /// The leading columns the SQL's ORDER BY names in its result, and their directions:
    /// the header's sort marks while the sidebar sorts nothing. Empty for an expression.
    query_order: Vec<(String, bool)>,
    /// Last executed fuzzy search (Fuzzy tab).
    active_fuzzy_query: String,
    pub(crate) column_order: Vec<String>, // Order of columns for display
    locked_columns_count: usize,          // Number of locked columns (from left)
    /// The last layout's frozen columns: the count asked, and how many fit beside a
    /// usable scrolling column (the rest scroll until there is room). A new count starts
    /// over.
    frozen_fit: (usize, usize),
    /// The grouped view a drill-down left, restored exactly by `drill_up`.
    grouped: Option<GroupedView>,
    /// The rows behind a grouped query result, so Enter can drill from an aggregate.
    group_source: Option<GroupSource>,
    /// The last pivot/melt result while in effect; SQL runs against it (see `query_root`).
    reshaped_lf: Option<LazyFrame>,
    drilled_down_group_index: Option<usize>, // Index of the group we're viewing
    drilled_down_group_key: Option<Vec<String>>, // Key values of the drilled down group
    drilled_down_group_key_columns: Option<Vec<String>>, // Key column names of the drilled down group
    /// Whether the frame still has the scan's hidden drift column: true when a dataset's
    /// files differ, false once a query or reshape builds a new frame.
    drift_column_present: bool,
    /// What each drift group is missing, shared with the renderer (no per-frame
    /// allocation), indexed by the drift column.
    drift_groups: Arc<Vec<crate::formats::schema_union::DriftGroup>>,
    /// The sorted or filtered view numbers its own rows (`#` on, data with no source
    /// position): a row index over the base, under filters and sort. Only while `#` is on,
    /// since it blocks filter pushdown.
    view_numbered: bool,
    /// What datui noticed about the dataset, from the footers it had to read anyway.
    notes: Vec<crate::notes::Note>,
    /// Whether Info has opened since the notes were gathered; per dataset.
    notes_seen: bool,
    /// Notes on what the filter and sort leave out, recomputed when either changes.
    view_notes: Vec<crate::notes::Note>,
    /// Bytes per row of the last buffer, preferred over the schema estimate.
    observed_bytes_per_row: Option<usize>,
    pub(crate) buffered_start_row: usize,
    buffered_end_row: usize,
    /// The full buffered frame (all columns in `column_order`) for the buffer range, so
    /// column scrolling re-slices without collecting.
    buffered_df: Option<DataFrame>,
    /// The first row of the last page drawn whole. See [`DataTableState::start_to_draw`].
    drawn_start: usize,
    /// Last applied pivot spec, if current lf is result of a pivot. Used for views.
    last_pivot_spec: Option<PivotSpec>,
    /// Last applied melt spec, if current lf is result of a melt. Used for views.
    last_melt_spec: Option<MeltSpec>,
    /// The query, filters and sort the pivot or melt ran over, for a view to replay first.
    /// `None` without one, or when it ran over the data as loaded.
    reshape_source: Option<ReshapeSource>,
    /// How `base_lf` was built from the data as loaded, for Copy as Python; empty for the
    /// data as loaded.
    base_steps: Vec<Step>,
    /// The view's column types and derived columns in order: a step of `lf` before the
    /// filters, as a spec's `[columns]` would say.
    column_changes: Vec<crate::formats::column_types::ColumnChange>,
    /// Bumped per change to `column_changes`, so a null count answers for its changes.
    changes_version: u64,
    /// Steps of a saved view whose columns this data does not have.
    changes_dropped: Vec<crate::notes::Note>,
    /// How `reshaped_lf` was built, while there is one: what SQL runs over.
    reshape_steps: Option<Vec<Step>>,
    /// Which loaded column each column of the base is (see [`Lineage`]).
    lineage: Lineage,
    /// The same for the pivot or melt in effect, which SQL runs against.
    reshape_lineage: Lineage,
}

pub struct DataTableState {
    original_lf: LazyFrame,
    original_schema: Arc<Schema>,
    pub table_state: TableState,
    pub visible_rows: usize,
    pub termcol_index: usize,
    /// The cursor may be off screen (order, frozen count or room changed): the next draw
    /// scrolls minimally to show it.
    reveal_cursor: bool,
    pub visible_termcols: usize,
    /// The scrolling side as last drawn, for planning sideways pages; `None` before the
    /// first draw.
    scroll_room: Option<Room>,
    /// Sideways moves waiting for the next draw to measure undrawn columns, in order. See
    /// [`Self::scroll_columns`].
    column_moves: Vec<WaitingMove>,
    /// The pages `]` went, from and to, so `[` straight after goes back exactly.
    page_trail: Vec<(usize, usize)>,
    /// Which columns the last draw showed, while some are off screen.
    pub(crate) on_screen: Option<OnScreen>,
    /// Where the last frame drew the rows and columns, for a click.
    pub(crate) drawn: Option<DrawnTable>,
    /// The cells the last frame formatted, for the next one to draw again.
    pub(crate) page_cells: crate::widgets::table::PageCells,
    error: Option<PolarsError>,
    pub suppress_error_display: bool, // When true, don't show errors in main view (e.g., when query input is active)
    /// The dataset's row count from when the frame was last pristine, for the footer's
    /// "417 of 1,000" without recounting; `None` until known.
    pristine_rows: Option<usize>,
    /// Renewed whenever `original_lf` is replaced; checkpoints record it, so one from
    /// other data is never restored.
    root_generation: u64,
    /// The local Parquet hive directory loaded from, whose footer counts sum to the exact
    /// row count while pristine: far cheaper than a `len()` scan.
    parquet_count_dir: Option<PathBuf>,
    /// What finding and reading this dataset cost. On the dataset, not the app: a failed
    /// open leaves the last dataset up, and its figures stay with it.
    measurements: Arc<crate::loading::measurements::Meter>,
    /// Each column's drawn width by identity, so paging, reordering, hiding and sidebars
    /// move nothing. Learned while formatting; not rolled back (it describes columns).
    pub(crate) widths: ColumnWidths,
    pages_lookahead: usize,
    pages_lookback: usize,
    max_buffered_rows: usize, // 0 = no limit
    max_buffered_mb: usize,   // 0 = no limit
    /// A scan of an object store, where a fill is a ranged read of whole row groups. See
    /// `is_remote_source`.
    remote_source: bool,
    /// Where each row group of a remote Parquet object starts, total last, from the footer.
    /// See `record_row_groups`.
    row_group_offsets: Option<Vec<usize>>,
    /// The files of a remote dataset, when it is many. See `RemoteFiles`.
    remote_files: Option<RemoteFiles>,
    /// Each remote object read, by URL, with size and tag: what a Data Quality local copy
    /// would fetch.
    remote_objects: Option<Arc<std::collections::HashMap<String, RemoteObject>>>,
    /// What the footers said of a many-file dataset's columns (schema origin, columns not
    /// in every file); `None` for one file.
    dataset_schema: Option<crate::formats::schema_union::DatasetSchema>,
    /// The two above as the dataset was opened, so a reset returns to them.
    drift_at_open: bool,
    groups_at_open: Arc<Vec<crate::formats::schema_union::DriftGroup>>,
    /// The data as loaded carries each row's source position in the hidden row index
    /// (lines), shown by `#` while the frame is the scan's.
    source_rows_at_open: bool,
    /// Lines still being indexed behind the first rows: the frames grow as they are.
    indexing: Option<Arc<crate::formats::lines::Lines>>,
    /// The lines of several files, which `#` numbers by their line in their own file.
    numbering: Option<Arc<crate::formats::lines::Lines>>,
    /// The dataset's row count from a sample of its footers, until it is counted.
    row_estimate: Option<crate::formats::schema_union::RowEstimate>,
    /// The notes the lines gave when they opened, replaced once they are all indexed.
    indexing_notes: Vec<crate::notes::Note>,
    /// Whether the open guessed the lines were text, which their notes say.
    indexing_guessed: bool,
    /// Each file's first row and drift group: together they map a row to what its file
    /// lacked.
    drift_file_starts: Vec<usize>,
    drift_file_group: Vec<u32>,
    /// Rows in the dataset per the footers, closing the last file's range.
    drift_dataset_rows: usize,
    /// The dataset as its footers found it, kept because reading a column as text needs
    /// the per-file types the view no longer has.
    dataset_at_open: Option<crate::formats::schema_union::DatasetSchema>,
    /// Columns read as text from every file instead of the majority type; empty as opened.
    read_as_text: Vec<PlSmallStr>,
    /// Each file's path or URL in scan order, to trace rows and name them in exports.
    drift_files: Vec<String>,
    /// Set while the dataset shows from a footer or two and the rest are being read;
    /// cleared when they join. See [`FootersJoin`].
    footers_pending: Option<FootersJoin>,
    /// The notes as the dataset was opened, so a reset and a drill up restore them.
    notes_at_open: Vec<crate::notes::Note>,
    /// Notes about the read itself (files passed over, a lake table's plain files). Kept
    /// apart from [`Self::notes`], which footers overwrite when they land, and they
    /// survive reshapes.
    open_notes: Vec<crate::notes::Note>,
    /// The lake format whose plain files this dataset is, if it is one. See
    /// [`crate::OpenOptions::read_as_plain_files_of`].
    not_the_table: Option<&'static str>,
    /// What a read through a format spec found: the spec, why, and its notes.
    format_read: Option<Arc<crate::formats::Read>>,
    /// What a read through a delimited spec found: units and metadata.
    delimited: Option<Arc<crate::formats::delimited_spec::DelimitedRead>>,
    /// The fixed records the data as loaded is, while pristine: a window starts decoding
    /// at the window, not row 0.
    fixed_window: Option<Arc<dyn crate::formats::pushdown::Windowed>>,
    /// A source that runs the sidebar's filters and sort itself (a SQLite table), while
    /// the data as loaded is the root: see [`Self::pushed_view`].
    pushdown: Option<Arc<dyn crate::formats::pushdown::Pushdown>>,
    /// Stops what the source runs when this state goes.
    source_hold: Option<crate::formats::sqlite::Hold>,
    /// How the open reads the data. See [`crate::OpenOptions::read_mode`].
    read_mode: Option<crate::ReadMode>,
    /// The format the open read. See [`OpenFacts::read_as`].
    read_as: Option<crate::FileFormat>,
    /// The data was downloaded from a remote source before it was read.
    fetched: bool,
    /// What the file said besides its rows. See [`OpenFacts::detail`].
    detail: Option<Arc<crate::formats::text_formats::Detail>>,
    /// Each loaded column's unit, from the file. See [`OpenFacts::units`].
    file_units: Arc<Vec<(String, String)>>,
    /// Uncompressed bytes per row of each column from the footer, for `bytes_per_row`
    /// before any collect.
    column_bytes: Vec<(String, usize)>,
    proximity_threshold: usize,
    row_numbers: bool,
    row_start_index: usize,
    /// What the open did to the reader's rows, as Python calls (names trimmed, text typed).
    read_python: Vec<String>,
    /// The columns the read gave a type, and the frame before it did.
    typing: Typing,
    /// The notes on the values the types made null, once counted.
    unfit_notes: Option<Vec<crate::notes::Note>>,
    /// The notes on the values the view's types made null: for the version counted.
    changes_unfit: Option<(u64, Vec<crate::notes::Note>)>,
    /// When set, dataset was loaded with hive partitioning; partition column names for Info panel and predicate pushdown.
    partition_columns: Option<Vec<String>>,
    /// The temp file decompressed CSV was written to, kept alive for the lazy scan; shared
    /// with views scanning it, removed with the last.
    decompress_temp_file: Option<Arc<Decompressed>>,
    /// The downloaded remote file this dataset was opened from, held while it is scanned.
    download: Option<crate::cloud::download::TempDownload>,
    /// The files a GPS log was read into, which the frame scans; held as `download` is.
    converted: Vec<crate::cloud::download::TempDownload>,
    /// The file's other tables, as `--table` names them; see [`OpenFacts::other_tables`].
    other_tables: Vec<String>,
    /// When true, use Polars streaming engine for LazyFrame collect when the streaming feature is enabled.
    polars_streaming: bool,
    /// When set, `collect()` / `apply_transformations()` skip the blocking collect; the
    /// caller starts an async one.
    defer_collect: bool,
    /// Set by the renderer when `visible_rows` changes; the event loop then starts an async
    /// collect.
    pub needs_recollect: bool,
    /// The watcher of the file this dataset follows (`--follow`), while it does.
    follow: Option<crate::loading::follow::Follow>,
    /// For a followed view that filters or sorts: known points (view rows, file row),
    /// ascending, for the count generation they hold for. Counts read on from the last;
    /// filtered windows from the one before.
    follow_known: Option<(u64, Vec<(usize, usize)>)>,
    /// The sample this view's rows are and the view it was drawn from: the step between
    /// source and query.
    sampled: Option<Box<Sampled>>,
    /// What the view is: everything a checkpoint keeps and puts back.
    pub(crate) view: View,
}

/// A view's sample, between source and query. The frames scan `Self::frame`, the
/// chunks so far, growing as the draw continues.
pub struct Sampled {
    /// The view the sample was drawn from, restored when the sample is cleared.
    source: Box<DataTableState>,
    sample: crate::analysis::sampling::Sample,
    rows: Arc<crate::analysis::table_sample::SampleRows>,
    /// The frame the view's plans scan: the chunks taken so far, on their buffers.
    frame: Arc<DataFrame>,
    /// Drawn through the view's query or filters (which it then stands for), not the
    /// source.
    through: bool,
    /// What the draw read, once it ended; `None` while it runs.
    drawn: Option<crate::analysis::table_sample::Drawn>,
    /// How a random sample of a stream is drawn, which a view keeps.
    path: Option<crate::analysis::table_sample::DrawPath>,
}

impl Sampled {
    pub fn sample(&self) -> &crate::analysis::sampling::Sample {
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
    pub(crate) fn holds(&self, rows: &Arc<crate::analysis::table_sample::SampleRows>) -> bool {
        Arc::ptr_eq(&self.rows, rows)
    }

    /// Whether rows are still arriving.
    pub fn drawing(&self) -> bool {
        self.drawn.is_none()
    }

    /// How a random sample of a stream is drawn: what draws the same rows again.
    pub fn path(&self) -> Option<crate::analysis::table_sample::DrawPath> {
        self.path
    }

    /// The frame the view's plans scan.
    #[cfg(test)]
    pub(crate) fn frame(&self) -> &DataFrame {
        &self.frame
    }

    /// Rows the view has taken of the sample.
    pub fn rows(&self) -> usize {
        self.frame.height()
    }

    /// The footer segment: `sample 100,000 of 36.8M`, `sample 1,234+` while drawing,
    /// `sample about 100,000 of 36.8M` when kept row by row by chance.
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
                    crate::home::discover::format_rows(total)
                )
            }
            _ => format!("sample {rows}{cut}"),
        }
    }
}

/// What an open learned besides frame and schema, given once via
/// [`DataTableState::with_open`] so count, row groups, files and notes agree. Defaults
/// mean not found.
#[derive(Default)]
pub struct OpenFacts {
    /// A scan of an object store in place: a buffer is one window of whole row groups.
    pub remote_source: bool,
    /// Each file's row groups in scan order (one entry for a single object): the count.
    /// With `remote_files`, one per listed file.
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
    pub measurements: Arc<crate::loading::measurements::Meter>,
    /// What the open itself has to say. See `DataTableState::open_notes`.
    pub open_notes: Vec<crate::notes::Note>,
    /// The lake format whose plain files this dataset is. See
    /// [`DataTableState::not_the_table`].
    pub not_the_table: Option<&'static str>,
    /// What a read through a format spec found.
    pub format_read: Option<Arc<crate::formats::Read>>,
    /// What a read through a delimited spec found.
    pub delimited: Option<Arc<crate::formats::delimited_spec::DelimitedRead>>,
    /// The downloaded file the frame scans, held for as long as the state lives.
    pub download: Option<crate::cloud::download::TempDownload>,
    /// The files a GPS log was read into, which the frame scans.
    pub converted: Vec<crate::cloud::download::TempDownload>,
    /// The file's other tables as `--table` names them, with row counts where known, for
    /// Info's Schema tab. Empty for a file of one.
    pub other_tables: Vec<String>,
    /// A source that runs the sidebar's filters and sort itself: a SQLite table.
    pub pushdown: Option<Arc<dyn crate::formats::pushdown::Pushdown>>,
    /// What stops that source's statements when the dataset goes.
    pub hold: Option<crate::formats::sqlite::Hold>,
    /// How the open reads the data. See [`crate::OpenOptions::read_mode`].
    pub read_mode: Option<crate::ReadMode>,
    /// The format the open read as, after sniffing and spec matching (which the name may
    /// not say); Copy as Python and the export default follow it.
    pub read_as: Option<crate::FileFormat>,
    /// Downloaded from a remote source before reading (not a local stream conversion or
    /// stdin spool, though held the same way).
    pub fetched: bool,
    /// What the file said besides its rows, for the Info panel.
    pub detail: Option<Arc<crate::formats::text_formats::Detail>>,
    /// Rows decoded straight from the file by a reader (NumPy array, audio frames), and
    /// how many: deep pages and the count need no row index.
    pub records: Option<(Arc<dyn crate::formats::pushdown::Windowed>, usize)>,
    /// Each column's unit, where the file says one.
    pub units: Vec<(String, String)>,
    /// Lines still being indexed behind the first rows: the frames grow as they are.
    pub indexing: Option<Arc<crate::formats::lines::Lines>>,
    /// The lines of several files, which `#` numbers by their line in their own file.
    pub numbering: Option<Arc<crate::formats::lines::Lines>>,
    /// The columns the read gave a type, for the count of what did not fit.
    pub typing: Typing,
}

/// The footers' account of a dataset of many files.
pub struct DatasetAtOpen {
    pub schema: crate::formats::schema_union::DatasetSchema,
    /// Each file's row count in scan order; empty unless all are known (when the scan
    /// numbers rows).
    pub file_rows: Vec<usize>,
    /// Every file's path or URL, in scan order.
    pub files: Vec<String>,
}

/// Rows the display buffer may hold when `performance.max_buffered_rows` is unset; also
/// a remote scan's window when the cap is off.
pub const DEFAULT_MAX_BUFFERED_ROWS: usize = 100_000;

/// Seeds `DataTableState::len_generation`, unique per state, so a count for one dataset
/// never validates another.
static NEXT_LEN_GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

fn next_len_generation() -> u64 {
    NEXT_LEN_GENERATION.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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

impl DataTableState {
    pub fn new(
        lf: LazyFrame,
        pages_lookahead: Option<usize>,
        pages_lookback: Option<usize>,
        max_buffered_rows: Option<usize>,
        max_buffered_mb: Option<usize>,
        polars_streaming: bool,
    ) -> Result<Self> {
        let options = OpenOptions {
            pages_lookahead,
            pages_lookback,
            max_buffered_rows,
            max_buffered_mb,
            polars_streaming,
            ..OpenOptions::default()
        };
        Self::from_lazyframe(lf, &options)
    }

    /// `schema` without the hidden row index, and whether it had one (the rows' source
    /// position, which `#` shows).
    fn without_source_rows(schema: Arc<Schema>) -> (Arc<Schema>, bool) {
        if !schema.contains(crate::formats::schema_union::DRIFT_COLUMN) {
            return (schema, false);
        }
        let mut schema = (*schema).clone();
        schema.shift_remove(crate::formats::schema_union::DRIFT_COLUMN);
        (Arc::new(schema), true)
    }

    /// Create state from an existing LazyFrame (e.g. from Python or in-memory). Uses OpenOptions for display/buffer settings.
    pub fn from_lazyframe(lf: LazyFrame, options: &crate::OpenOptions) -> Result<Self> {
        let schema = lf.clone().collect_schema()?;
        Self::from_schema_and_lazyframe(schema, lf, options, None)
    }

    /// State from a pre-collected schema and LazyFrame (phased loading), without
    /// `collect_schema()`; `df` is `None` so headers render while the first collect runs.
    /// Hive partition columns come first.
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
            original_lf: lf.clone(),
            original_schema: schema.clone(),
            table_state: TableState::default(),
            visible_rows: 0,
            termcol_index: 0,
            visible_termcols: 0,
            scroll_room: None,
            column_moves: Vec::new(),
            page_trail: Vec::new(),
            on_screen: None,
            drawn: None,
            page_cells: Default::default(),
            error: None,
            suppress_error_display: false,
            pristine_rows: None,
            root_generation: next_len_generation(),
            parquet_count_dir: None,
            measurements: Arc::new(crate::loading::measurements::Meter::default()),
            reveal_cursor: false,
            widths: ColumnWidths::default(),
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
            drift_at_open: false,
            groups_at_open: Arc::new(Vec::new()),
            source_rows_at_open,
            indexing: None,
            numbering: None,
            row_estimate: None,
            indexing_notes: Vec::new(),
            indexing_guessed: false,
            drift_file_starts: Vec::new(),
            drift_file_group: Vec::new(),
            drift_files: Vec::new(),
            footers_pending: None,
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
            notes_at_open: Vec::new(),
            drift_dataset_rows: 0,
            dataset_at_open: None,
            read_as_text: Vec::new(),
            column_bytes: Vec::new(),
            // Set when the rows on screen are known.
            proximity_threshold: 0,
            row_numbers: options.row_numbers,
            row_start_index: options.row_start_index,
            read_python: Vec::new(),
            typing: Typing::default(),
            unfit_notes: None,
            changes_unfit: None,
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
            view: View {
                unsorted_lf: None,
                base_lf: lf.clone(),
                base_schema: schema.clone(),
                lf,
                df: None,
                locked_df: None,
                start_row: 0,
                schema,
                num_rows: 0,
                num_rows_valid: false,
                len_generation: next_len_generation(),
                filters: Vec::new(),
                sort_columns: Vec::new(),
                sort_descending: Vec::new(),
                sort_ascending: true,
                cursor_column: None,
                cursor_at: 0,
                active_query: String::new(),
                active_sql_query: String::new(),
                query_order: Vec::new(),
                active_fuzzy_query: String::new(),
                column_order,
                locked_columns_count: 0,
                frozen_fit: (0, 0),
                grouped: None,
                group_source: None,
                reshaped_lf: None,
                drilled_down_group_index: None,
                drilled_down_group_key: None,
                drilled_down_group_key_columns: None,
                drift_column_present: false,
                drift_groups: Arc::new(Vec::new()),
                view_numbered: false,
                notes: Vec::new(),
                notes_seen: false,
                view_notes: Vec::new(),
                observed_bytes_per_row: None,
                buffered_start_row: 0,
                buffered_end_row: 0,
                buffered_df: None,
                drawn_start: 0,
                last_pivot_spec: None,
                last_melt_spec: None,
                reshape_source: None,
                base_steps: Vec::new(),
                column_changes: Vec::new(),
                changes_version: 0,
                changes_dropped: Vec::new(),
                reshape_steps: None,
                lineage: None,
                reshape_lineage: None,
            },
        })
    }

    /// The state with everything the open found in `facts`: the only way findings reach a
    /// state, while pristine, applied in dependency order (files before row groups,
    /// which set the count). Later learning goes through [`Self::join_dataset_schema`] and
    /// [`Self::count_landed`].
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
            .map(|read| read.records.clone() as Arc<dyn crate::formats::pushdown::Windowed>);
        if let Some(read) = &format_read {
            // The reader counted records from the file size; a frame count would build the whole
            // row index.
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
            // The reader knows its rows (a frame count would build the row index); lines still
            // indexing know only some.
            if indexing.is_none() {
                self.set_num_rows(rows);
            }
            self.fixed_window = Some(window);
        }
        if let Some(lines) = &indexing {
            // The lines' own notes, replaced once all lines are in and can describe the file.
            self.indexing_guessed = self
                .open_notes
                .iter()
                .any(|n| n.summary.starts_with(crate::formats::lines::GUESSED));
            self.indexing_notes = crate::formats::lines::notes(lines, self.indexing_guessed);
        }
        self.indexing = indexing;
        self.file_units = Arc::new(units);
        self
    }

    /// Make `lf` the data as loaded with `schema` (root, base and shown frame) until the
    /// caller relays filters and sort. Rows and counts from the old root drop, and older
    /// checkpoints no longer apply.
    fn replace_root(&mut self, lf: LazyFrame, schema: Arc<Schema>) {
        // The records, or the table, no longer stand for the root.
        self.fixed_window = None;
        self.pushdown = None;
        self.root_generation = next_len_generation();
        self.invalidate_num_rows();
        self.original_schema = schema.clone();
        self.view.base_schema = schema.clone();
        self.view.schema = schema;
        self.original_lf = lf.clone();
        self.view.base_lf = lf.clone();
        self.view.lf = lf;
        self.view.unsorted_lf = None;
        self.view.base_steps = Vec::new();
        self.view.reshape_steps = None;
        self.drop_buffer();
    }

    /// Make `lf` the shown frame and the base for filters and sort, `schema` its schema,
    /// every column in view; row counts invalidated.
    fn install_base(&mut self, lf: LazyFrame, schema: Arc<Schema>) {
        self.invalidate_num_rows();
        // A new frame is the user's own projection: its rows no longer stand for a file's, so
        // no drift marks and file notes no longer apply.
        self.view.drift_column_present = false;
        self.view.view_numbered = false;
        self.view.drift_groups = Arc::new(Vec::new());
        self.view.notes = Vec::new();
        self.view.view_notes = Vec::new();
        // Measure the new shape afresh, not from the old width.
        self.view.observed_bytes_per_row = None;
        // A new frame is in no order a query named; `sql_query` names it after.
        self.view.query_order = Vec::new();
        // A column may keep its name and type and hold other values now.
        self.widths.relearn();
        self.view.base_lf = lf.clone();
        self.view.base_schema = schema.clone();
        self.view.lf = lf;
        self.view.unsorted_lf = None;
        // Callers say how the base was built; one that does not leaves a script saying so.
        self.view.base_steps = vec![Step::Unreproducible(
            "datui built the view from here in a way it cannot write as Python".to_string(),
        )];
        self.view.schema = schema;
        self.view.column_order = self
            .view
            .schema
            .iter_names()
            .map(|s| s.to_string())
            .collect();
        // No column is a loaded one until the caller says which are.
        self.view.lineage = Some(Arc::default());
        self.settle_cursor();
        // A query that groups records its source after installing its result.
        self.view.group_source = None;
        self.drop_buffer();
    }

    /// Forget rows read through the replaced frame so the next collect reads the new one.
    fn drop_buffer(&mut self) {
        self.view.buffered_start_row = 0;
        self.view.buffered_end_row = 0;
        self.view.buffered_df = None;
    }

    /// View state for a new root: no query text, no filters or sort, not drilled, the first
    /// `locked_columns_count` frozen, buffer dropped, cursor at the top left.
    fn reset_view_state(&mut self, locked_columns_count: usize) {
        self.forget_column_changes();
        self.view.active_query.clear();
        self.view.active_sql_query.clear();
        self.view.active_fuzzy_query.clear();
        self.view.locked_columns_count = locked_columns_count;
        self.view.filters.clear();
        self.view.sort_columns.clear();
        self.view.sort_descending.clear();
        self.view.sort_ascending = true;
        self.view.start_row = 0;
        self.termcol_index = 0;
        self.clear_column_moves();
        self.place_cursor_at(0);
        self.view.drilled_down_group_index = None;
        self.view.drilled_down_group_key = None;
        self.view.drilled_down_group_key_columns = None;
        self.view.grouped = None;
        self.drop_buffer();
        self.table_state.select(Some(0));
    }

    /// Install a query's result as the root with `query` the active bar. Whether a pivot or
    /// melt survives is the caller's call (SQL runs on it, others on the data as loaded).
    /// The caller collects.
    fn install_query_result(
        &mut self,
        lf: LazyFrame,
        schema: Arc<Schema>,
        query: ActiveQuery,
        locked_columns_count: usize,
        steps: Vec<Step>,
    ) {
        self.install_base(lf, schema);
        self.view.base_steps = steps;
        self.reset_view_state(locked_columns_count);
        match query {
            ActiveQuery::Dsl(q) => self.view.active_query = q,
            #[cfg(feature = "sql")]
            ActiveQuery::Sql(q) => self.view.active_sql_query = q,
            ActiveQuery::Fuzzy(q) => self.view.active_fuzzy_query = q,
        }
    }

    /// The view no longer shows the pivot or melt, so nothing may run against it.
    fn forget_reshape(&mut self) {
        self.view.reshaped_lf = None;
        self.view.reshape_lineage = None;
        self.view.reshape_steps = None;
        self.view.last_pivot_spec = None;
        self.view.last_melt_spec = None;
        self.view.reshape_source = None;
    }

    /// Reset to `original_lf` with its loaded schema; the caller reads the rows.
    fn reset_lf_to_original(&mut self) {
        self.install_base(self.original_lf.clone(), self.query_source_schema());
        self.view.base_steps = Vec::new();
        self.view.reshape_steps = None;
        self.view.lineage = None;
        self.view.reshape_lineage = None;
        // Back to the data as opened: rows stand for files again, and the notes apply.
        self.view.drift_column_present = self.drift_at_open;
        self.view.drift_groups = self.groups_at_open.clone();
        self.view.notes = self.notes_at_open.clone();
        self.view.reshaped_lf = None;
        self.view.reshape_source = None;
        self.reset_view_state(0);
        self.restore_footer_count();
    }

    /// Back to the data as loaded, with nothing applied and no error showing.
    fn return_to_root(&mut self) {
        self.reset_lf_to_original();
        self.error = None;
        self.suppress_error_display = false;
        self.view.last_pivot_spec = None;
        self.view.last_melt_spec = None;
    }

    /// Back to the data as loaded with nothing applied, for replaying a view's steps.
    /// Reads nothing.
    pub(crate) fn reset_view_for_replay(&mut self) {
        self.return_to_root();
    }

    /// Back to the table as opened: nothing applied, widths relearned from the first page.
    pub fn reset(&mut self) {
        self.widths = ColumnWidths::default();
        self.return_to_root();
        self.collect();
        if self.view.num_rows > 0 {
            self.view.start_row = 0;
        }
    }

    /// The state of what a reader read, with the open's paging and row numbers.
    pub(crate) fn from_read(read: Read, options: &OpenOptions) -> Result<Self> {
        let mut state = Self::from_lazyframe(read.lf, options)?;
        state.read_python = read.python;
        state.typing = read.typing;
        state.decompress_temp_file = read.temp;
        Ok(state)
    }

    pub fn set_row_numbers(&mut self, enabled: bool) {
        self.row_numbers = enabled;
    }

    /// Toggle `#`. Returns whether the frame changed and rows must be reread (a sorted or
    /// filtered view of position-less data numbers its rows).
    pub fn toggle_row_numbers(&mut self) -> bool {
        self.row_numbers = !self.row_numbers;
        if self.row_numbers && self.wants_view_numbers() && !self.view.view_numbered {
            self.drop_buffer();
            self.apply_transformations();
            return true;
        }
        false
    }

    /// Whether the view would number its own rows with `#` on: sorted or filtered over a
    /// scan whose rows lack a position.
    fn wants_view_numbers(&self) -> bool {
        // Not for a followed file (read from a mark, a row index would count from it), nor a
        // store or many files (the index would read every file), nor past its counting range.
        let too_many = self
            .pristine_rows
            .or(self.num_rows_if_valid())
            .is_some_and(|rows| rows > crate::formats::row_index::MAX_ROWS);
        self.scan_is_the_root()
            && self.follow.is_none()
            && !self.remote_source
            && self.remote_files.is_none()
            && self.parquet_count_dir.is_none()
            && !too_many
            && !self.view.drift_column_present
            && !self.source_rows_at_open
            && self.pushed_view().is_none()
            && (!self.view.filters.is_empty()
                || !self.view.sort_columns.is_empty()
                || !self.view.sort_ascending)
    }

    /// Whether the row-number column is shown.
    pub fn row_numbers(&self) -> bool {
        self.row_numbers
    }

    /// Row number display start (0 or 1); used by go-to-line to interpret user input.
    pub fn row_start_index(&self) -> usize {
        self.row_start_index
    }
}

/// The frame counting `lf`'s rows. `len()` is `UInt32`, and summing it over a
/// many-file union widens to `UInt128`, which Polars 0.55 cannot reduce (error or
/// panic), so the count is cast to `UInt64`.
pub(crate) fn row_count_lf(lf: &LazyFrame) -> LazyFrame {
    lf.clone().select([len().cast(DataType::UInt64)])
}

/// The stub shown for binary columns: their blobs are never read into the display
/// buffer (keeping scrolling fast); `lf` still has them for export and analysis. From
/// the glyph set, so ASCII terminals get `<binary>`.
pub(crate) fn binary_stub() -> &'static str {
    crate::glyphs::get().binary_stub
}

/// The most sideways moves held for a draw, bounding the draw's work.
const MAX_WAITING_MOVES: usize = 32;

/// A sideways move waiting on a draw: the view's own, or the column cursor's.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WaitingMove {
    View(ColumnMove),
    Cursor(CursorMove),
}

pub(crate) fn visible_slice(df: &DataFrame, offset: usize, len: usize) -> Option<DataFrame> {
    let len = len.min(df.height().saturating_sub(offset));
    (offset < df.height() && len > 0).then(|| df.slice(offset as i64, len))
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
    drift: (bool, Arc<Vec<crate::formats::schema_union::DriftGroup>>),
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
            rows: rows(&self.view.lf),
            analysis_rows: rows(&self.analysis_lf()),
            base_rows: rows(&self.view.base_lf),
            reshaped_rows: self.view.reshaped_lf.as_ref().map(rows),
            schema: self.view.schema.clone(),
            queries: [
                self.view.active_query.clone(),
                self.view.active_sql_query.clone(),
                self.view.active_fuzzy_query.clone(),
            ],
            filters: format!("{:?}", self.view.filters),
            sort: (
                self.view.sort_columns.clone(),
                self.view.sort_descending.clone(),
                self.view.sort_ascending,
            ),
            layout: (
                self.view.column_order.clone(),
                self.view.locked_columns_count,
            ),
            reshape: format!(
                "{:?} {:?} {:?}",
                self.view.last_pivot_spec, self.view.last_melt_spec, self.view.reshape_source
            ),
            grouped: (
                self.view.grouped.is_some(),
                self.view.group_source.is_some(),
            ),
            drill: (
                self.view.drilled_down_group_index,
                self.view.drilled_down_group_key.clone(),
                self.view.drilled_down_group_key_columns.clone(),
            ),
            drift: (
                self.view.drift_column_present,
                self.view.drift_groups.clone(),
            ),
            notes: (
                self.view.notes.clone(),
                self.view.notes_seen,
                self.view.view_notes.clone(),
            ),
            selection: (
                self.table_state.selected(),
                self.view.start_row,
                self.termcol_index,
            ),
            count: (
                self.view.num_rows,
                self.view.num_rows_valid,
                self.view.len_generation,
            ),
            buffer: (
                self.view.buffered_start_row,
                self.view.buffered_end_row,
                self.view.buffered_df.clone(),
            ),
            shown: self.view.df.clone(),
            error: self.error.as_ref().map(|e| e.to_string()),
        }
    }
}

mod buffer;
mod columns;
mod copy;
mod drawn;
mod facts;
mod quality;
mod query;
mod view;

pub use buffer::*;
pub use copy::*;
pub(crate) use drawn::DrawnTable;
pub use drawn::{CellHit, DrawnColumns};
pub use facts::*;
pub use query::*;
pub use view::*;

#[cfg(test)]
mod checkpoint_tests;
#[cfg(test)]
mod tests;
