//! Drilling into groups and values, the inspector's rows, and what a copy takes.

use super::*;

/// The grouped view and pipeline state a drill-down saved, so filters and sort inside
/// the group apply to it and `drill_up` restores the grouped view.
#[derive(Clone)]
pub(super) struct GroupedView {
    pub(super) lf: LazyFrame,
    pub(super) base_lf: LazyFrame,
    /// The schemas of `base_lf` and of the view, so coming back resolves neither.
    base_schema: Arc<Schema>,
    schema: Arc<Schema>,
    pub(super) filters: Vec<FilterStatement>,
    pub(super) sort_columns: Vec<String>,
    pub(super) sort_descending: Vec<bool>,
    pub(super) sort_ascending: bool,
    /// Whether `lf` has the hidden drift column and its groups' meaning, so drilling up
    /// restores those cells and their notes.
    drift: bool,
    drift_groups: Arc<Vec<crate::schema_union::DriftGroup>>,
    /// Whether `lf` numbers its rows itself (`#`).
    view_numbered: bool,
    notes: Vec<crate::notes::Note>,
    pub(super) group_source: Option<GroupSource>,
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

/// The rows a grouped result came from and how its keys were computed, so a drill-down
/// finds a group's rows even from aggregates. Recorded by the grouping query, not
/// inferred from (possibly renamed) columns.
#[derive(Clone)]
pub(super) struct GroupSource {
    /// The rows before grouping, after any filter the query applied first.
    pub(super) rows: LazyFrame,
    /// Each key's column in the result, with the expression that computes it from `rows`.
    pub(super) keys: Vec<(PlSmallStr, Expr)>,
    /// Columns `rows` carries only to compute keys, left out of a drill.
    pub(super) scratch: Vec<PlSmallStr>,
    /// Whether the result's list columns are each group's rows, as a `by` query's are.
    /// A SQL result's lists are values it computed, such as `ARRAY_AGG`.
    pub(super) rows_in_lists: bool,
    /// The same as Copy as Python steps: how `rows` was built, and each key as
    /// Python code, aliases undone. None where the script cannot say.
    pub(super) python_rows: Option<Vec<Step>>,
    pub(super) python_keys: Vec<Option<String>>,
    /// Which loaded column each column of `rows` is.
    pub(super) lineage: Lineage,
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
pub(super) struct GroupRows {
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

/// The rows an export writes. See [`DataTableState::export_frame`].
pub struct ExportFrame {
    pub(super) lf: LazyFrame,
    pub(super) files: Option<SourceFiles>,
}

/// The dataset's files in scan order, and the row each starts at.
pub(super) struct SourceFiles {
    pub(super) names: Arc<Vec<String>>,
    pub(super) starts: Arc<Vec<usize>>,
}

impl ExportFrame {
    /// Rows that are not the view's, such as a column's value counts.
    pub fn of(lf: LazyFrame) -> Self {
        Self { lf, files: None }
    }
}

impl ExportFrame {
    /// The name of the column an export adds when asked to say where each row is from.
    pub const SOURCE_FILE_COLUMN: &'static str = "source_file";

    /// The plan. Naming files reads the schema (possibly resolving the scan), so off the UI
    /// thread; names map from the row index per batch, so streamed exports never hold
    /// every row.
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

    /// A name for the source-file column no column has: `source_file` may exist already,
    /// and adding a same-named column would silently replace it.
    fn free_name<'a>(taken: impl Iterator<Item = &'a str>) -> String {
        let taken: HashSet<&str> = taken.collect();
        std::iter::once(Self::SOURCE_FILE_COLUMN.to_string())
            .chain((1..).map(|n| format!("{}_{n}", Self::SOURCE_FILE_COLUMN)))
            .find(|candidate| !taken.contains(candidate.as_str()))
            .expect("some suffix is free")
    }
}

impl DataTableState {
    /// The group drilled into, as its key columns and their values.
    pub fn drilled_group_key(&self) -> Option<(&[String], &[String])> {
        let values = self.view.drilled_down_group_key.as_deref()?;
        let columns = self
            .view
            .drilled_down_group_key_columns
            .as_deref()
            .unwrap_or_default();
        Some((columns, values))
    }

    /// The columns and types of `df`, the table SQL runs against, from the known schema; a
    /// drilled group or reshape only resolves its plan, reading nothing.
    pub fn sql_table_columns(&self) -> Vec<(String, DataType)> {
        let schema = if self.view.grouped.is_none() && self.view.reshaped_lf.is_none() {
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
        if self.view.grouped.is_some() || self.view.reshaped_lf.is_some() {
            return None;
        }
        self.pristine_rows
    }

    pub fn get_active_fuzzy_query(&self) -> &str {
        &self.view.active_fuzzy_query
    }

    pub fn last_pivot_spec(&self) -> Option<&PivotSpec> {
        self.view.last_pivot_spec.as_ref()
    }

    pub fn last_melt_spec(&self) -> Option<&MeltSpec> {
        self.view.last_melt_spec.as_ref()
    }

    /// What the pivot or melt in effect ran over. See the field.
    pub fn reshape_source(&self) -> Option<&ReshapeSource> {
        self.view.reshape_source.as_ref()
    }

    /// Whether the view is a grouping's result (a `by` query, SQL GROUP BY) whose rows drill
    /// into groups; recorded by the query, never inferred from list columns.
    pub fn is_grouped(&self) -> bool {
        self.view.group_source.is_some()
    }

    /// Whether any column holds lists.
    fn has_list_columns(&self) -> bool {
        self.view
            .schema
            .iter()
            .any(|(_, dtype)| matches!(dtype, DataType::List(_)))
    }

    fn group_key_columns(&self) -> Vec<String> {
        self.view
            .schema
            .iter()
            .filter(|(_, dtype)| !matches!(dtype, DataType::List(_)))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    fn group_value_columns(&self) -> Vec<String> {
        self.view
            .schema
            .iter()
            .filter(|(_, dtype)| matches!(dtype, DataType::List(_)))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// Names of binary columns in the source schema. Their values are not read into the display
    /// buffer (the `‹binary›` stub stands in); the renderer uses this to style those cells.
    pub fn binary_column_names(&self) -> std::collections::HashSet<String> {
        self.view
            .schema
            .iter()
            .filter(|(_, dtype)| matches!(dtype, DataType::Binary))
            .map(|(name, _)| name.to_string())
            .collect()
    }

    /// Estimated heap size in bytes of the currently buffered slice (locked + scrollable), if collected.
    pub fn buffered_memory_bytes(&self) -> Option<usize> {
        let locked = self
            .view
            .locked_df
            .as_ref()
            .map(|df| df.estimated_size())
            .unwrap_or(0);
        let scroll = self
            .view
            .df
            .as_ref()
            .map(|df| df.estimated_size())
            .unwrap_or(0);
        if locked == 0 && scroll == 0 {
            None
        } else {
            Some(locked + scroll)
        }
    }

    /// Number of rows currently in the buffer. 0 if no buffer loaded.
    /// The rows on hand, first to past the last.
    pub fn buffered_span(&self) -> (usize, usize) {
        (self.view.buffered_start_row, self.view.buffered_end_row)
    }

    pub fn buffered_rows(&self) -> usize {
        self.view
            .buffered_end_row
            .saturating_sub(self.view.buffered_start_row)
    }

    /// The first `limit` distinct non-null values of `column` in the display buffer, as
    /// text. Reads nothing; a column not buffered gives none.
    pub(crate) fn buffered_values(&self, column: &str, limit: usize) -> Vec<String> {
        let Some(series) = [self.view.df.as_ref(), self.view.locked_df.as_ref()]
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
        self.view.df.as_ref()
    }

    /// The display buffer's rows on screen, as the table draws them.
    pub fn display_slice_df(&self) -> Option<DataFrame> {
        let df = self.view.df.as_ref()?;
        let offset = self
            .view
            .start_row
            .saturating_sub(self.view.buffered_start_row);
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
        let df = self.view.buffered_df.as_ref()?;
        let absolute = self.view.start_row + self.table_state.selected()?;
        let offset = absolute.checked_sub(self.view.buffered_start_row)?;
        if offset >= df.height() {
            return None;
        }
        let names: Vec<&str> = self.view.column_order.iter().map(|s| s.as_str()).collect();
        df.select(names).ok().map(|d| d.slice(offset as i64, 1))
    }

    /// The rows on screen with every display column, raw, in display order, ignoring
    /// column scroll (scrolled-past identifiers make the rows readable).
    pub fn copy_view_df(&self) -> Option<DataFrame> {
        let df = self.view.buffered_df.as_ref()?;
        let names: Vec<&str> = self.view.column_order.iter().map(|s| s.as_str()).collect();
        let selected = df.select(names).ok()?;
        let offset = self
            .view
            .start_row
            .saturating_sub(self.view.buffered_start_row);
        let len = self
            .visible_rows
            .min(selected.height().saturating_sub(offset));
        (len > 0).then(|| selected.slice(offset as i64, len))
    }

    /// The selected row's value in `column`, exactly as stored ([`crate::exact`]): floats
    /// as their round-tripping decimal, null as empty (as in exports), lists and structs as
    /// JSON.
    pub fn copy_cell_value(&self, column: &str) -> Option<String> {
        let row = self.copy_row_df()?;
        crate::exact::copy_text(row.column(column).ok()?).ok()
    }

    /// The selected row's number as the row-numbers column would print it.
    pub fn selected_display_row(&self) -> Option<usize> {
        Some(self.view.start_row + self.table_state.selected()? + self.row_start_index)
    }

    /// Rows times estimated row width (binary at base64 size), for the copy guard. `None`
    /// until counted, or while a binary column's width is unknown (only a footer says).
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
        for name in &self.view.column_order {
            match self.view.schema.get(name.as_str()) {
                Some(DataType::Binary) => row += base64(footer_width(name)?),
                // Buffered whole, so the buffer measured it with the row; base64 adds
                // a third on top.
                Some(dtype) if crate::nested_json::has_binary(dtype) => {
                    let buffered = self.view.buffered_df.as_ref().and_then(|df| {
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

    /// Each row's drift group from the top of the view, for a frame `frame_rows` tall (the
    /// whole frame, header included: extra entries are ignored, and `visible_rows` is 0
    /// before the first frame). Empty once a query or reshape replaced the frame (its rows
    /// stand for no file).
    pub fn display_drift(&self, frame_rows: usize) -> Vec<u32> {
        if !self.view.drift_column_present {
            return Vec::new();
        }
        let Some(df) = self.view.buffered_df.as_ref() else {
            return Vec::new();
        };
        let Ok(column) = df.column(crate::schema_union::DRIFT_COLUMN) else {
            return Vec::new();
        };
        let offset = self
            .view
            .start_row
            .saturating_sub(self.view.buffered_start_row);
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
        self.has_list_columns()
            && self
                .view
                .group_source
                .as_ref()
                .is_some_and(|s| s.rows_in_lists)
    }

    /// The columns of a row that a drill into its group reads: every column of a result
    /// holding its groups as lists, the keys of one holding aggregates.
    fn drill_columns(&self) -> Vec<String> {
        if self.drills_lists() {
            return self
                .view
                .schema
                .iter_names()
                .map(|n| n.to_string())
                .collect();
        }
        self.view
            .group_source
            .iter()
            .flat_map(|source| source.keys.iter().map(|(name, _)| name.to_string()))
            .collect()
    }

    /// The columns the inspector lists for a row: the table's, in its order, then
    /// the ones hidden from it, in schema order. Never the scan's own row index.
    pub fn inspect_fields(&self) -> Vec<InspectField> {
        let shown = self.view.column_order.iter().filter_map(|name| {
            Some(InspectField {
                name: name.clone(),
                dtype: self.view.schema.get(name.as_str())?.clone(),
                hidden: false,
            })
        });
        let hidden = self
            .view
            .schema
            .iter()
            .filter(|(name, _)| {
                name.as_str() != crate::schema_union::DRIFT_COLUMN
                    && !self.view.column_order.iter().any(|c| c == name.as_str())
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
        self.inspect_row_at(self.view.start_row + self.table_state.selected()?)
    }

    /// Row `row` of the view as the buffer holds it, as [`Self::inspect_row`] does
    /// the selected one: Compare's next row. `None` while it is not on hand.
    pub fn inspect_row_at(&self, row: usize) -> Option<InspectRow> {
        let df = self.view.buffered_df.as_ref()?;
        let offset = row.checked_sub(self.view.buffered_start_row)?;
        if offset >= df.height() {
            return None;
        }
        let names: Vec<&str> = self.view.column_order.iter().map(|s| s.as_str()).collect();
        let values = df.select(names).ok()?.slice(offset as i64, 1);
        let drift_group = self
            .view
            .drift_column_present
            .then(|| df.column(crate::schema_union::DRIFT_COLUMN).ok())
            .flatten()
            .and_then(|c| c.get(offset).ok())
            .and_then(|v| v.extract::<usize>())
            .map(|place| self.file_group_of(place));
        Some(InspectRow {
            row,
            frame: self.view.len_generation,
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
        let Some(group) = group.and_then(|g| self.view.drift_groups.get(g as usize)) else {
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

    /// The frame reading `columns` of view row `row` for the inspector, through the
    /// buffer's window (a remote dataset reads only that file). Run off this thread.
    pub fn inspect_read_lf(&self, row: usize, columns: &[String]) -> PolarsResult<LazyFrame> {
        let exprs = columns.iter().map(|c| col(c.as_str())).collect();
        self.window_lf(row, 1, exprs)
    }

    /// What drilling into the group on view row `group_index` reads, or `None` when not
    /// grouped. The buffer holds the row; a missing column (hidden, binary stub) means
    /// reading the row, which for an aggregate is the whole aggregate, so off the UI thread.
    pub fn drill_row(&self, group_index: usize) -> Option<DrillRow> {
        if !self.can_drill_down() {
            return None;
        }
        let columns = self.drill_columns();
        let buffered = self
            .view
            .buffered_df
            .as_ref()
            .filter(|_| {
                (self.view.buffered_start_row..self.view.buffered_end_row).contains(&group_index)
            })
            .filter(|_| {
                columns
                    .iter()
                    .all(|c| !matches!(self.view.schema.get(c.as_str()), Some(DataType::Binary)))
            })
            .and_then(|df| df.select(columns.iter().map(|c| c.as_str())).ok())
            .map(|df| df.slice((group_index - self.view.buffered_start_row) as i64, 1))
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

    /// Show the group on view row `group_index`, reading the row on this thread if not
    /// buffered; the app uses [`Self::drill_row`] so keys never wait.
    pub fn drill_down_into_group(&mut self, group_index: usize) -> Result<()> {
        let row = match self.drill_row(group_index) {
            None => return Ok(()),
            Some(DrillRow::Buffered(row)) => row,
            Some(DrillRow::Read(lf)) => collect_lazy(*lf, self.polars_streaming)?,
        };
        self.drill_down_with_row(group_index, &row)
    }

    /// Show the group whose row `group_index` is `row` (from [`Self::drill_row`]): list
    /// columns as rows, or for aggregates the source rows sharing the group's keys, key
    /// columns first.
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
                self.view.lineage.clone(),
            )?
        } else if let Some(source) = &self.view.group_source {
            Self::group_from_source(source, row)?
        } else {
            return Ok(());
        };
        // A list form that also aggregates (`select a, n: count a by k`) holds its
        // aggregates beside the keys; the query knows which columns are keys.
        if let Some(source) = self
            .view
            .group_source
            .as_ref()
            .filter(|_| self.drills_lists())
        {
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

    /// Show the view's rows with `value` in `column` (null matches null), like a group
    /// drill: breadcrumb names the value, Esc returns. Inside a group, it narrows that group
    /// and Esc returns to the view it was drilled from.
    pub fn drill_into_value(&mut self, column: &str, value: AnyValue<'static>) -> Result<()> {
        let dtype = self
            .view
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
            lineage: self.view.lineage.clone(),
        };
        if !self.is_drilled_down() {
            let index = self.view.start_row + self.table_state.selected().unwrap_or(0);
            return self.enter_group(group, index, true);
        }
        let schema = group.lf.clone().collect_schema()?;
        let order = std::mem::take(&mut self.view.column_order);
        if let Some(keys) = self.view.drilled_down_group_key_columns.as_mut() {
            keys.extend(group.key_columns);
        }
        if let Some(values) = self.view.drilled_down_group_key.as_mut() {
            values.extend(group.key_values);
        }
        // The group's filters and sort are in the frame now.
        self.view.filters.clear();
        self.view.sort_columns.clear();
        self.view.sort_descending.clear();
        self.view.sort_ascending = true;
        self.install_base(group.lf, schema);
        self.view.base_steps = group.steps;
        self.view.lineage = group.lineage;
        self.view.column_order = order;
        self.view.start_row = 0;
        self.termcol_index = 0;
        self.clear_column_moves();
        self.settle_cursor();
        self.table_state.select(Some(0));
        self.collect();
        Ok(())
    }

    /// Whether the drill on screen came from Value Counts.
    pub fn drilled_into_value(&self) -> bool {
        self.view.grouped.as_ref().is_some_and(|view| view.by_value)
    }

    /// Show `group`, the group on row `group_index` of the view, keeping the view to
    /// come back to.
    fn enter_group(&mut self, group: GroupRows, group_index: usize, by_value: bool) -> Result<()> {
        let schema = group.lf.clone().collect_schema()?;
        self.view.drilled_down_group_key = Some(group.key_values);
        self.view.drilled_down_group_key_columns = Some(group.key_columns);

        // The group becomes the pipeline root while drilled in, so a sidebar filter or
        // sort applies within it instead of rebuilding the grouped view underneath.
        self.view.grouped = Some(GroupedView {
            lf: self.view.lf.clone(),
            base_lf: self.view.base_lf.clone(),
            base_schema: self.view.base_schema.clone(),
            schema: self.view.schema.clone(),
            filters: std::mem::take(&mut self.view.filters),
            sort_columns: std::mem::take(&mut self.view.sort_columns),
            sort_descending: std::mem::take(&mut self.view.sort_descending),
            sort_ascending: self.view.sort_ascending,
            drift: self.view.drift_column_present,
            drift_groups: self.view.drift_groups.clone(),
            view_numbered: self.view.view_numbered,
            notes: self.view.notes.clone(),
            group_source: self.view.group_source.take(),
            column_order: self.view.column_order.clone(),
            locked_columns_count: self.view.locked_columns_count,
            start_row: self.view.start_row,
            termcol_index: self.termcol_index,
            cursor_column: self.view.cursor_column.clone(),
            selected: self.table_state.selected(),
            by_value,
            base_steps: std::mem::take(&mut self.view.base_steps),
            lineage: self.view.lineage.clone(),
        });
        self.view.sort_ascending = true;
        self.install_base(group.lf, schema);
        self.view.base_steps = group.steps;
        self.view.lineage = group.lineage;
        // Led by the keys, as a group drilled from lists is.
        let rest: Vec<String> = std::mem::take(&mut self.view.column_order)
            .into_iter()
            .filter(|c| !group.lead.contains(c))
            .collect();
        self.view.column_order = group.lead.into_iter().chain(rest).collect();
        self.view.drilled_down_group_index = Some(group_index);
        self.view.start_row = 0;
        self.termcol_index = 0;
        self.clear_column_moves();
        self.view.locked_columns_count = 0;
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
        let Some(view) = self.view.grouped.take() else {
            return Err(color_eyre::eyre::eyre!("Not in drill-down mode"));
        };
        self.invalidate_num_rows();
        // The buffer holds the group's rows; kept, it would stand in for the grouped
        // view wherever the view fits inside it.
        self.drop_buffer();
        self.view.observed_bytes_per_row = None;
        self.widths.relearn();
        self.view.lf = view.lf;
        self.view.unsorted_lf = None;
        self.view.base_lf = view.base_lf;
        self.view.base_schema = view.base_schema;
        self.view.base_steps = view.base_steps;
        self.view.lineage = view.lineage;
        self.view.filters = view.filters;
        self.view.sort_columns = view.sort_columns;
        self.view.sort_descending = view.sort_descending;
        self.view.sort_ascending = view.sort_ascending;
        self.view.drift_column_present = view.drift;
        self.view.drift_groups = view.drift_groups;
        self.view.view_numbered = view.view_numbered;
        self.view.notes = view.notes;
        self.view.group_source = view.group_source;
        // The restored frame already excludes what its filter and sort excluded; rederive
        // those notes so they cannot be stale.
        self.view.view_notes = self.view_notes_only();
        self.view.schema = view.schema;
        self.view.column_order = view.column_order;
        self.view.locked_columns_count = view.locked_columns_count;
        self.view.drilled_down_group_index = None;
        self.view.drilled_down_group_key = None;
        self.view.drilled_down_group_key_columns = None;
        self.view.start_row = view.start_row;
        self.termcol_index = view.termcol_index;
        self.clear_column_moves();
        self.view.cursor_column = view.cursor_column;
        self.settle_cursor();
        self.table_state.select(view.selected);
        self.collect();
        Ok(())
    }
}
