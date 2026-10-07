//! Building the view: reshapes, column types, sort, filter, the query bar, SQL and
//! fuzzy search.

use super::*;

/// `agg` over `values`, one cell of a pivot.
pub(super) fn pivot_agg_expr(agg: PivotAggregation, values: Expr) -> Expr {
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

/// The most columns a pivot may make: past it everything crawls, and it is nearly
/// always a mistake (an id or timestamp chosen for Columns).
pub const PIVOT_COLUMN_LIMIT: usize = 10_000;

/// A pivot of the view as it was when planned, to be read off the UI thread.
pub struct PivotJob {
    pub(super) view: LazyFrame,
    pub(super) spec: PivotSpec,
    pub(super) streaming: bool,
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

    /// The pivoted frame in one pass: a lazy pivot needs its new columns up front (a
    /// distinct pass, then another pass), so cells are aggregated by one group-by on index
    /// and pivot columns, new columns read from that, and the pivot done in memory. New
    /// columns come alphabetical with a trailing `null` column, as the eager pivot did;
    /// index rows keep first-seen order.
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

    /// Polars names pivot columns by casting values to text, which panics on dates past the
    /// calendar: such values become text first (the date its stored number), after
    /// ordering as dates. Both frames are in memory.
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

/// Which loaded column each shown column is (shown, loaded), so a spec's unit stays
/// with the loaded values, renamed or not, never on a computed column reusing a name.
/// `None` while every column is its own loaded column.
pub(super) type Lineage = Option<Arc<Vec<(String, String)>>>;

/// `pairs`, each a shown name and the name of a column of a frame whose lineage is
/// `root`, traced back to the loaded columns. A name `root` does not know is dropped.
pub(super) fn traced(root: &Lineage, pairs: Vec<(String, String)>) -> Lineage {
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
pub(super) fn passed_through(exprs: &[Expr]) -> Vec<(String, String)> {
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

/// The query bar a result came from, with its text. At most one is active at a time.
pub(super) enum ActiveQuery {
    Dsl(String),
    #[cfg(feature = "sql")]
    Sql(String),
    Fuzzy(String),
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

/// Sort options, one direction per column. Nulls last both ways (as pandas, DuckDB and
/// spreadsheets; Polars defaults first). Stable, since each page sorts and slices
/// separately, and an unstable sort orders ties differently at the top (top-k) than
/// deeper, repeating or skipping rows.
pub(super) fn sort_options(descending: Vec<bool>) -> SortMultipleOptions {
    let n = descending.len();
    SortMultipleOptions::default()
        .with_order_descending_multi(descending)
        .with_nulls_last_multi(vec![true; n])
        .with_maintain_order(true)
}

/// What a sidebar apply changed besides the columns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewChange {
    pub filters: bool,
    pub sort: bool,
}

impl DataTableState {
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
            self.view
                .schema
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
        self.view.last_pivot_spec = Some(spec.clone());
        self.view.last_melt_spec = None;
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
        self.view.last_melt_spec = Some(spec.clone());
        self.view.last_pivot_spec = None;
        self.replace_lf_after_reshape(lf, step, &spec.index)?;
        Ok(())
    }

    /// A melt mixing dates with text casts dates to text, which panics past the calendar:
    /// those columns become text first (such dates their stored number).
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
            &self.view.lineage,
            kept.iter().map(|c| (c.clone(), c.clone())).collect(),
        );
        let mut steps = self.view_steps();
        steps.push(step);
        // Taken before the view state below is reset. Over an earlier reshape there is
        // no source a view could replay, so none is kept.
        let text = |q: &str| Some(q.trim().to_string()).filter(|q| !q.is_empty());
        let source = ReshapeSource {
            query: text(&self.view.active_query),
            sql_query: text(&self.view.active_sql_query),
            fuzzy_query: text(&self.view.active_fuzzy_query),
            filters: self.view.filters.clone(),
            sort_columns: self.view.sort_columns.clone(),
            sort_descending: self.view.sort_descending.clone(),
        };
        self.view.reshape_source =
            (self.view.reshaped_lf.is_none() && !source.is_empty()).then_some(source);
        self.view.reshaped_lf = Some(lf.clone());
        self.install_base(lf, schema);
        self.view.base_steps = steps.clone();
        self.view.reshape_steps = Some(steps);
        self.view.lineage = lineage.clone();
        self.view.reshape_lineage = lineage;
        self.reset_view_state(0);
        self.error = None;
        self.view.df = None;
        self.view.locked_df = None;
        self.collect();
        Ok(())
    }

    /// The sidebar filters with their values typed against the columns they test.
    fn typed_filters(&self) -> Vec<SidebarFilter> {
        self.view
            .filters
            .iter()
            .map(|f| SidebarFilter::typed_in(f, &self.view.schema, &self.view.column_order))
            .collect()
    }

    /// What the open did to the rows its reader gave, as Python method calls.
    pub fn read_python(&self) -> &[String] {
        &self.read_python
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
        &self.view.column_changes
    }

    /// The columns the view gave a type: the type row draws them in the accent.
    pub fn retyped_columns(&self) -> Vec<String> {
        self.view
            .column_changes
            .iter()
            .filter(|c| matches!(c.change, crate::column_types::Change::Typed(_)))
            .map(|c| c.name.clone())
            .collect()
    }

    /// `column`'s type before the view's: as the read gave it, or as the view made it.
    pub fn type_as_read(&self, column: &str) -> Option<DataType> {
        self.view
            .base_schema
            .get(column)
            .or_else(|| self.view.schema.get(column))
            .cloned()
    }

    /// Up to `n` of `column`'s values as read that are not blank, as text: from the
    /// rows on hand, or from the first rows when the view has typed the column.
    pub fn values_on_screen(&self, column: &str, n: usize) -> Vec<String> {
        let from_buffer = self.column_type_of(column).is_none();
        let df = if from_buffer {
            self.view.buffered_df.clone()
        } else {
            self.view
                .base_lf
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
        self.view
            .column_changes
            .iter()
            .find_map(|c| match &c.change {
                crate::column_types::Change::Typed(ty) if c.name == column => Some(ty),
                _ => None,
            })
    }

    /// `column` as `ty`, or as read again with `None`. The view's own type wins over
    /// what the read gave the column. Lazy: the next rows read are typed.
    pub fn set_column_type(&mut self, column: &str, ty: Option<crate::column_types::ColumnType>) {
        use crate::column_types::{Change, ColumnChange};
        self.view
            .column_changes
            .retain(|c| !(c.name == column && matches!(c.change, Change::Typed(_))));
        if let Some(ty) = ty {
            self.view.column_changes.push(ColumnChange {
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
        if self.view.schema.contains(&derived.name) {
            return Err(format!("a column is named {} already", derived.name));
        }
        for from in &derived.from {
            if !self.view.schema.contains(from) {
                return Err(format!("no column {from}"));
            }
        }
        let first = derived.from[0].clone();
        let at = self
            .view
            .column_order
            .iter()
            .position(|c| *c == first)
            .unwrap_or(self.view.column_order.len());
        self.view.column_order.insert(at, derived.name.clone());
        self.view.column_changes.push(ColumnChange {
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

    /// Replace the view's column changes with a saved view's; changes whose column is
    /// missing are left out with a note, their names returned.
    pub fn set_column_changes(
        &mut self,
        changes: &[crate::column_types::ColumnChange],
    ) -> Vec<String> {
        self.view.column_changes = Vec::new();
        let base = self.view.base_schema.clone();
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
                self.view.column_changes.push(change.clone());
            } else {
                dropped.push(change.name.clone());
            }
        }
        self.view.changes_dropped = if dropped.is_empty() {
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
        for change in &self.view.column_changes {
            if let crate::column_types::Change::Made { from, .. } = &change.change
                && !self.view.column_order.contains(&change.name)
            {
                let at = self
                    .view
                    .column_order
                    .iter()
                    .position(|c| *c == from[0])
                    .unwrap_or(self.view.column_order.len());
                self.view.column_order.insert(at, change.name.clone());
            }
        }
        self.column_changes_changed();
        dropped
    }

    /// Drop the view's column changes and their notes, as a new pipeline root does.
    pub(super) fn forget_column_changes(&mut self) {
        if self.view.column_changes.is_empty() && self.view.changes_dropped.is_empty() {
            return;
        }
        self.view.column_changes.clear();
        self.view.changes_dropped.clear();
        self.view.changes_version += 1;
        self.changes_unfit = None;
    }

    /// After the column changes change: the schema shows them, a made column gone
    /// leaves the column order, and the rows are read again.
    fn column_changes_changed(&mut self) {
        self.view.changes_version += 1;
        let (changed, _) = self.with_column_changes(self.view.base_lf.clone());
        if let Ok(schema) = changed.clone().collect_schema() {
            self.view.schema = schema;
        }
        let schema = self.view.schema.clone();
        self.view.column_order.retain(|c| schema.contains(c));
        for name in schema.iter_names() {
            if !self.view.column_order.iter().any(|c| c == name.as_str()) {
                self.view.column_order.push(name.to_string());
            }
        }
        self.widths.relearn();
        self.drop_buffer();
        self.apply_transformations();
    }

    /// `lf` with the view's column changes in order, plus what counting their nulls
    /// needs: the frame with only the made columns, and typed columns with their prior
    /// types. Changes for missing columns are skipped.
    fn with_column_changes(
        &self,
        mut lf: LazyFrame,
    ) -> (
        LazyFrame,
        Option<(LazyFrame, Vec<crate::column_types::Typed>)>,
    ) {
        use crate::column_types::Change;
        if self.view.column_changes.is_empty() {
            return (lf, None);
        }
        // The frames passed here have the base's columns.
        let mut schema = (*self.view.base_schema).clone();
        let mut made = lf.clone();
        let mut typed = Vec::new();
        for change in &self.view.column_changes {
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
            .is_some_and(|(version, _)| *version == self.view.changes_version)
        {
            return None;
        }
        let (_, count) = self.with_column_changes(self.view.base_lf.clone());
        let (source, typed) = count?;
        Some((source, typed, self.view.changes_version))
    }

    /// The counts for the view's column types at `version`, as notes.
    pub(crate) fn changes_unfit_counted(
        &mut self,
        version: u64,
        unfit: &[crate::column_types::Unfit],
    ) {
        if version == self.view.changes_version {
            // Something new to say: the `i` chip lights again.
            if !unfit.is_empty() {
                self.view.notes_seen = false;
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
            self.view.notes_seen = false;
        }
        self.unfit_notes = Some(crate::column_types::unfit_notes(
            unfit,
            "counted over every row",
        ));
    }

    /// How `lf` was built: the base's steps, then the filters and the sort.
    pub(super) fn view_steps(&self) -> Vec<Step> {
        let mut steps = self.view.base_steps.clone();
        if !self.view.column_changes.is_empty() {
            let said: Vec<String> = self
                .view
                .column_changes
                .iter()
                .map(crate::column_types::ColumnChange::to_toml)
                .collect();
            steps.push(Step::Unreproducible(format!(
                "datui typed columns as a format spec would: {}",
                said.join("; ")
            )));
        }
        if !self.view.filters.is_empty() {
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
        if !self.view.sort_columns.is_empty() {
            steps.push(Step::Sort {
                columns: self.view.sort_columns.clone(),
                descending: self.view.sort_descending.clone(),
            });
        } else if !self.view.sort_ascending {
            steps.push(Step::Reverse);
        }
        steps
    }

    /// The view as Copy as Python writes it: every step from the data as loaded to
    /// the columns shown, in their order.
    pub fn python_steps(&self) -> Vec<Step> {
        let mut steps = self.view_steps();
        let in_order = self.view.column_order.iter().map(String::as_str).eq(self
            .view
            .schema
            .iter_names()
            .map(|s| s.as_str())
            .filter(|s| *s != crate::schema_union::DRIFT_COLUMN));
        if !in_order {
            steps.push(Step::Select(self.view.column_order.clone()));
        }
        steps
    }

    pub fn is_drilled_down(&self) -> bool {
        self.view.drilled_down_group_index.is_some()
    }

    /// Rebuild `lf` as `base_lf` → filters → sort. Column order is applied at collect.
    /// A source that runs the filters and sort itself gives the frame instead.
    pub(super) fn apply_transformations(&mut self) {
        let lf = match self.pushed_view() {
            Some(view) => {
                let sorted = !self.view.sort_columns.is_empty() || !self.view.sort_ascending;
                self.view.unsorted_lf = sorted
                    .then(|| {
                        self.pushdown
                            .as_ref()
                            .and_then(|p| p.view(&self.view.filters, &[], false))
                            .map(|unsorted| unsorted.lf)
                    })
                    .flatten();
                self.view.view_notes = Vec::new();
                self.view.view_numbered = false;
                self.with_column_changes(view.lf).0
            }
            None => self.build_view(),
        };
        self.invalidate_num_rows();
        self.view.lf = lf;
        self.restore_footer_count();
        self.collect();
    }

    /// `base_lf` with the view's column changes, row numbers, filters and order, in
    /// that order; the frame before the order is kept as `unsorted_lf`.
    fn build_view(&mut self) -> LazyFrame {
        let mut lf = self.with_column_changes(self.view.base_lf.clone()).0;
        self.view.view_numbered = self.row_numbers && self.wants_view_numbers();
        if self.view.view_numbered {
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
        if !view_notes.is_empty() && view_notes != self.view.view_notes {
            self.view.notes_seen = false;
        }
        self.view.view_notes = view_notes;

        // What an analysis reads: the view before its order, which no statistic
        // depends on and every sampled read of a sorted frame would pay for.
        self.view.unsorted_lf =
            (!self.view.sort_columns.is_empty() || !self.view.sort_ascending).then(|| lf.clone());
        if !self.view.sort_columns.is_empty() {
            lf = lf.sort_by_exprs(
                self.view.sort_columns.iter().map(col).collect::<Vec<_>>(),
                sort_options(self.view.sort_descending.clone()),
            );
        } else if !self.view.sort_ascending {
            lf = lf.reverse();
        }
        lf
    }

    /// Sort with one direction for every column. `ascending` also sets the natural
    /// order when `columns` is empty.
    pub fn sort(&mut self, columns: Vec<String>, ascending: bool) {
        let descending = vec![!ascending; columns.len()];
        self.view.sort_ascending = ascending;
        self.sort_by(columns, descending);
    }

    /// Sort with a direction per column.
    pub fn sort_by(&mut self, columns: Vec<String>, descending: Vec<bool>) {
        debug_assert_eq!(columns.len(), descending.len());
        // The one-direction flag survives as the primary column's, for older view readers and
        // `r`'s natural-order fallback.
        if let Some(first) = descending.first() {
            self.view.sort_ascending = !first;
        }
        // Other rows come first. The sidebar sends the sort again on any apply, so
        // only a sort that changed counts.
        if columns != self.view.sort_columns || descending != self.view.sort_descending {
            self.widths.relearn();
        }
        self.view.sort_columns = columns;
        self.view.sort_descending = descending;
        self.drop_buffer();
        self.apply_transformations();
    }

    /// The view the other way round: every sort column's direction flipped, or the
    /// natural order reversed, so `r` twice is always the identity.
    pub fn reverse(&mut self) {
        self.view.sort_ascending = !self.view.sort_ascending;
        self.widths.relearn();
        for direction in &mut self.view.sort_descending {
            *direction = !*direction;
        }
        self.drop_buffer();
        self.apply_transformations();
    }

    /// The sidebar's Apply: the column order, the frozen count, the filters and the
    /// sort as one change, planned once and read from the top.
    pub fn apply_view(
        &mut self,
        order: Vec<String>,
        locked: usize,
        filters: Vec<FilterStatement>,
        columns: Vec<String>,
        descending: Vec<bool>,
    ) -> ViewChange {
        self.set_column_order(order);
        self.set_locked_columns(locked);
        let change = ViewChange {
            filters: filters != self.view.filters,
            sort: columns != self.view.sort_columns || descending != self.view.sort_descending,
        };
        if change.filters || change.sort {
            self.widths.relearn();
        }
        if let Some(first) = descending.first() {
            self.view.sort_ascending = !first;
        }
        self.view.filters = filters;
        self.view.sort_columns = columns;
        self.view.sort_descending = descending;
        self.view.start_row = 0;
        self.drop_buffer();
        self.apply_transformations();
        change
    }

    pub fn filter(&mut self, filters: Vec<FilterStatement>) {
        // The sidebar sends the filters again on any apply; only a change is new rows.
        if filters != self.view.filters {
            self.widths.relearn();
        }
        self.view.filters = filters;
        // A new result set, viewed from the top: a position deep in the old one would
        // plan a slice past a smaller result, which reads nothing.
        self.view.start_row = 0;
        self.drop_buffer();
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

        let source_schema = self.query_source_schema();
        let parsed = parse_query_over(&query, Some(&source_schema))
            .map(|parsed| parsed.past_calendar_safe(Some(&source_schema)));
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
                                .iter_names()
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
                        // Every other column of the source (a filter keeps them all),
                        // each group's values as a list.
                        let agg_exprs: Vec<Expr> = source_schema
                            .iter_names()
                            .filter(|n| !group_by_col_names.iter().any(|g| g == n.as_str()))
                            .map(|n| col(n.clone()))
                            .collect();

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
                let input = source_schema;
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
                self.view.lineage = lineage;
                if !keys.is_empty() {
                    self.view.group_source = Some(GroupSource {
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
                self.collect();
                // The result is viewed from its top, whatever a read clamped.
                if self.view.num_rows > 0 {
                    self.view.start_row = 0;
                }
            }
            Err(e) => {
                // Parse errors are already user-facing strings; store as ComputeError
                self.error = Some(PolarsError::ComputeError(e.into()));
            }
        }
    }

    /// The data a query runs against: the drilled group, else the pivot/melt result, else
    /// the data as loaded; never the sidebar's filters or sort, nor a previous SQL result.
    pub(crate) fn query_root(&self) -> LazyFrame {
        if self.view.grouped.is_some() {
            // While drilled, `base_lf` is the group (see `drill_down_into_group`).
            return self.view.base_lf.clone();
        }
        Self::without_drift(
            self.view
                .reshaped_lf
                .clone()
                .unwrap_or_else(|| self.original_lf.clone()),
        )
    }

    /// Which loaded column each column of [`Self::query_root`] is.
    #[cfg(feature = "sql")]
    fn root_lineage(&self) -> Lineage {
        if self.view.grouped.is_some() {
            self.view.lineage.clone()
        } else if self.view.reshaped_lf.is_some() {
            self.view.reshape_lineage.clone()
        } else {
            None
        }
    }

    /// How [`Self::query_root`] was built, as Copy as Python steps.
    #[cfg(feature = "sql")]
    fn query_root_steps(&self) -> Vec<Step> {
        if self.view.grouped.is_some() {
            return self.view.base_steps.clone();
        }
        match (&self.view.reshaped_lf, &self.view.reshape_steps) {
            (None, _) => Vec::new(),
            (Some(_), Some(steps)) => steps.clone(),
            (Some(_), None) => vec![Step::Unreproducible(
                "datui reshaped the data in a way it cannot write as Python".to_string(),
            )],
        }
    }

    /// Run SQL against `query_root` (registered as table "df"), starting a fresh view:
    /// `install_query_result` clears filters and sort, reapplied after. Empty SQL resets.
    /// Does not collect; the event loop does (`AppEvent::Collect`).
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
                    // First, so schema and group source read the plan that runs: it changes expressions in
                    // place and only projects over union inputs, keeping the shapes `stable_order` and
                    // `count_subquery_values_once` rely on.
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
                    // Groups sorted by key have no ties, and a traceable statement has nothing else
                    // unordered; ordering groups too would double the grouping's time.
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
                    self.view.query_order = query_order;
                    self.view.lineage = lineage;
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

    /// What a SQL `GROUP BY` result was grouped from, when simple enough to trace (see
    /// [`crate::sql_group`]), planned without reading. Without ORDER BY or LIMIT its rows
    /// are sorted by key, as a `by` query's, so pages, counts and drills see one order.
    /// Also says whether it sorted them.
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
        self.view.locked_columns_count = self
            .view
            .schema
            .iter_names()
            .take_while(|c| source.keys.iter().any(|(k, _)| k == *c))
            .count();
        self.view.group_source = Some(source);
    }

    /// Fuzzy search: keep rows where every whitespace-separated token matches (in order,
    /// case-insensitive) in some string column. An empty query resets.
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
        let schema = self.query_source_schema();
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
        self.view.lineage = None;
        self.forget_reshape();
        self.collect();
    }
}
