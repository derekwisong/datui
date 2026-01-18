use color_eyre::Result;
use std::sync::Arc;
use std::{fs::File, path::Path};

use polars::prelude::*;
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style, Stylize},
    text::{Line, Span},
    widgets::{
        Block, Borders, Cell, Padding, Paragraph, Row, StatefulWidget, Table, TableState, Widget,
    },
};

use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};
use crate::query::parse_query;
use crate::OpenOptions;

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
}

impl DataTableState {
    pub fn new(lf: LazyFrame) -> Result<Self> {
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

        self.table_state.select(Some(0));

        self.collect();
        if self.num_rows > 0 {
            self.start_row = 0;
        }
    }

    pub fn from_parquet(path: &Path) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyFrame::scan_parquet(pl_path, Default::default())?;
        Self::new(lf)
    }

    pub fn from_csv(path: &Path, options: &OpenOptions) -> Result<Self> {
        Self::from_csv_customize(path, |mut reader| {
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
        })
    }

    pub fn from_csv_customize<F>(path: &Path, func: F) -> Result<Self>
    where
        F: FnOnce(LazyCsvReader) -> LazyCsvReader,
    {
        let pl_path = PlPath::Local(Arc::from(path));
        let reader = LazyCsvReader::new(pl_path);
        let lf = func(reader).finish()?;
        Self::new(lf)
    }

    pub fn from_ndjson(path: &Path) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let lf = LazyJsonLineReader::new(pl_path).finish()?;
        Self::new(lf)
    }

    pub fn from_json(path: &Path) -> Result<Self> {
        Self::from_json_with_format(path, JsonFormat::Json)
    }

    pub fn from_json_lines(path: &Path) -> Result<Self> {
        Self::from_json_with_format(path, JsonFormat::JsonLines)
    }

    fn from_json_with_format(path: &Path, format: JsonFormat) -> Result<Self> {
        let file = File::open(path)?;
        let lf = JsonReader::new(file)
            .with_json_format(format)
            .finish()?
            .lazy();
        Self::new(lf)
    }

    pub fn from_delimited(path: &Path, delimiter: u8) -> Result<Self> {
        let pl_path = PlPath::Local(Arc::from(path));
        let reader = LazyCsvReader::new(pl_path).with_separator(delimiter);
        let lf = reader.finish()?;
        Self::new(lf)
    }

    fn slide_table(&mut self, rows: i64) {
        if rows < 0 && self.start_row == 0 {
            return;
        }

        self.start_row = if self.start_row as i64 + rows <= 0 {
            0
        } else {
            if let Some(df) = self.df.as_ref() {
                if rows > 0 && df.shape().0 <= self.visible_rows {
                    return;
                }
            }
            (self.start_row as i64 + rows) as usize
        };

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
        }

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
                .slice(self.start_row as i64, self.visible_rows as u32 + 1)
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
            .slice(self.start_row as i64, self.visible_rows as u32 + 1)
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
        self.collect();
    }

    pub fn set_locked_columns(&mut self, count: usize) {
        self.locked_columns_count = count.min(self.column_order.len());
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
        self.apply_transformations();
    }

    pub fn reverse(&mut self) {
        self.sort_ascending = !self.sort_ascending;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::filter_modal::{FilterOperator, FilterStatement, LogicalOperator};

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
        // This test remains to ensure file-based loading works
        let path = Path::new("tests/sample-data/3-sfd-header.csv");
        let state = DataTableState::from_csv(path, &Default::default()).unwrap();
        assert_eq!(state.schema.len(), 3);
    }

    #[test]
    fn test_from_parquet() {
        let path = Path::new("tests/sample-data/crypto_prices.parquet");
        let state = DataTableState::from_parquet(path).unwrap();
        assert!(!state.schema.is_empty());
    }

    #[test]
    fn test_filter() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
        state.sort(vec!["a".to_string()], false);
        let df = state.lf.clone().collect().unwrap();
        assert_eq!(df.column("a").unwrap().get(0).unwrap(), AnyValue::Int32(3));
    }

    #[test]
    fn test_query() {
        let lf = create_test_lf();
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
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
        let mut state = DataTableState::new(lf).unwrap();
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
}

#[derive(Default)]
pub struct DataTable {}

impl DataTable {
    pub fn new() -> Self {
        Self::default()
    }

    fn render_dataframe(
        &self,
        df: &DataFrame,
        area: Rect,
        buf: &mut Buffer,
        state: &mut TableState,
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

        // for visible columsn
        let headers: Vec<Span> = df
            .get_column_names()
            .iter()
            .take(visible_columns)
            .map(|name| {
                Span::styled(
                    name.to_string(),
                    Style::default().add_modifier(Modifier::BOLD),
                )
            })
            .collect();

        StatefulWidget::render(
            Table::new(rows, widths)
                .column_spacing(1)
                .header(Row::new(headers).bold().underlined())
                .row_highlight_style(Style::new().bg(Color::Blue)),
            area,
            buf,
            state,
        );
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

        // Calculate locked columns width if any
        let mut locked_width = 0;
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
        if locked_width > 0 && locked_width < area.width {
            let locked_area = Rect {
                x: area.x,
                y: area.y,
                width: locked_width,
                height: area.height,
            };
            let separator_x = locked_area.x + locked_area.width;
            let scrollable_area = Rect {
                x: separator_x + 1,
                y: area.y,
                width: area.width.saturating_sub(locked_width + 1),
                height: area.height,
            };

            // Render locked columns (no background shading, just the vertical separator)
            if let Some(locked_df) = state.locked_df.as_ref() {
                self.render_dataframe(locked_df, locked_area, buf, &mut state.table_state);
            }

            // Draw vertical separator line
            for y in area.y..area.y + area.height {
                let cell = &mut buf[(separator_x, y)];
                cell.set_char('│');
                cell.set_style(Style::default().fg(Color::White));
            }

            // Render scrollable columns
            if let Some(df) = state.df.as_ref() {
                self.render_dataframe(df, scrollable_area, buf, &mut state.table_state);
            }
        } else if let Some(df) = state.df.as_ref() {
            // No locked columns, render normally
            self.render_dataframe(df, area, buf, &mut state.table_state);
        } else {
            Paragraph::new("No data").render(area, buf);
        }
    }
}
