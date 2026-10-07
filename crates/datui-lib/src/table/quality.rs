//! What Data Quality reads of the dataset: its source, scope and evidence rows.

use super::*;

impl DataTableState {
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
            drift_groups: self.view.drift_groups.clone(),
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
        if !self.view.drift_column_present
            || !self
                .dataset_at_open
                .as_ref()
                .is_some_and(crate::schema_union::DatasetSchema::drifts)
        {
            return 0;
        }
        crate::data_quality::conflict_reads(&self.drift_file_group, &self.view.drift_groups)
    }

    /// Reads one column of named files at the type each of them wrote it in, for the
    /// values a type conflict hides. `None` when the dataset's files all agree, or
    /// when this frame is not the dataset as it opened.
    pub(crate) fn quality_conflict_scan(&self) -> Option<crate::data_quality::QualityConflictScan> {
        let dataset = self.dataset_at_open.clone()?;
        if !self.view.drift_column_present || !dataset.drifts() {
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
            self.view.lf.clone()
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
            &self.view.schema
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
            view.view.column_order = self.view.column_order.clone();
            view.view.locked_columns_count = self.view.locked_columns_count;
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
                self.view.schema.clone(),
            )
        })
    }
}
