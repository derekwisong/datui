use crate::statistics::collect_lazy;
use color_eyre::Result;
use color_eyre::eyre::Report;
use polars::prelude::*;
use std::collections::BTreeMap;

// Sampling is without replacement from a bounded prefix, never a full-source random scan.
const DEFAULT_SAMPLE_ROWS: usize = 10_000;
const DEFAULT_CHUNK_ROWS: usize = 1_000_000;
pub const QUALITY_SOURCE_FILE_COLUMN: &str = "__datui_quality_source_file";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityScope {
    #[default]
    CurrentView,
    WholeSource,
    FirstRows(usize),
}

impl QualityScope {
    pub fn label(self) -> String {
        match self {
            Self::CurrentView => "current view".to_string(),
            Self::WholeSource => "whole source".to_string(),
            Self::FirstRows(rows) => format!("first {rows} view rows"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct QualitySourceContext {
    pub file_names: Vec<String>,
    pub file_starts: Vec<usize>,
    pub row_index_column: String,
}

/// Prepare the loaded source in the worker, keeping only a provenance index
/// and replacing binary payloads before any value collection.
pub fn prepare_source_quality_scan(
    lf: LazyFrame,
    source: Option<&QualitySourceContext>,
) -> Result<LazyFrame> {
    let schema = lf.clone().collect_schema()?;
    let expressions = schema
        .iter()
        .filter_map(|(name, dtype)| {
            let column = name.as_str();
            if column == crate::schema_union::DRIFT_COLUMN
                && !source.is_some_and(|context| context.row_index_column == column)
            {
                return None;
            }
            Some(if matches!(dtype, DataType::Binary) {
                lit(crate::widgets::datatable::BINARY_STUB).alias(column)
            } else {
                col(column)
            })
        })
        .collect::<Vec<_>>();
    let lf = lf.select(expressions);
    Ok(
        if source.is_some_and(|context| context.row_index_column == "__datui_quality_row") {
            lf.with_row_index("__datui_quality_row", None)
        } else {
            lf
        },
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityPage {
    #[default]
    Plan,
    Overview,
    Columns,
    Segments,
    Trends,
    Detail,
    TimeRoles,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityCompute {
    Metadata,
    #[default]
    Sample,
    Full,
}

impl QualityCompute {
    pub fn label(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Sample => "sample",
            Self::Full => "full",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum QualityGrain {
    #[default]
    Dataset,
    File,
    Partition(String),
    RowChunks(usize),
    TimeWindows {
        column: String,
        every: String,
    },
}

impl QualityGrain {
    pub fn label(&self) -> String {
        match self {
            Self::Dataset => "dataset".to_string(),
            Self::File => "file".to_string(),
            Self::Partition(column) => format!("partition:{column}"),
            Self::RowChunks(rows) => format!("{rows} rows (physical order)"),
            Self::TimeWindows { column, every } => format!("{every} on {column}"),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityComparison {
    #[default]
    None,
    Previous,
    Baseline,
}

impl QualityComparison {
    pub fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Previous => "previous",
            Self::Baseline => "baseline (first)",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TemporalRole {
    Event,
    Effective,
    PeriodEnd,
    Created,
    Published,
    Received,
    Processed,
    ValidFrom,
    ValidTo,
}

impl TemporalRole {
    pub const ALL: [Self; 9] = [
        Self::Event,
        Self::Effective,
        Self::PeriodEnd,
        Self::Created,
        Self::Published,
        Self::Received,
        Self::Processed,
        Self::ValidFrom,
        Self::ValidTo,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::Event => "event",
            Self::Effective => "effective/as-of",
            Self::PeriodEnd => "period end",
            Self::Created => "created",
            Self::Published => "published",
            Self::Received => "received",
            Self::Processed => "processed",
            Self::ValidFrom => "valid from",
            Self::ValidTo => "valid to",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TemporalRoleAssignment {
    pub role: TemporalRole,
    pub column: String,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DataQualityPlan {
    pub scope: QualityScope,
    pub compute: QualityCompute,
    pub sample_rows: usize,
    pub sample_seed: u64,
    pub grain: QualityGrain,
    pub comparison: QualityComparison,
    pub baseline_segment: Option<String>,
    pub temporal_roles: Vec<TemporalRoleAssignment>,
    pub latency_threshold_seconds: Option<i64>,
}

impl Default for DataQualityPlan {
    fn default() -> Self {
        Self {
            scope: QualityScope::CurrentView,
            compute: QualityCompute::Sample,
            sample_rows: DEFAULT_SAMPLE_ROWS,
            sample_seed: 42_891,
            grain: QualityGrain::Dataset,
            comparison: QualityComparison::None,
            baseline_segment: None,
            temporal_roles: Vec::new(),
            latency_threshold_seconds: None,
        }
    }
}

impl DataQualityPlan {
    pub fn comparison_label(&self) -> String {
        if self.comparison == QualityComparison::Baseline {
            self.baseline_segment
                .as_ref()
                .map(|label| format!("baseline: {label}"))
                .unwrap_or_else(|| self.comparison.label().to_string())
        } else {
            self.comparison.label().to_string()
        }
    }

    pub fn set_row_chunks(&mut self) {
        self.grain = QualityGrain::RowChunks(DEFAULT_CHUNK_ROWS);
    }

    pub fn compact_summary(&self) -> String {
        format!(
            "scope {} -> grain {} -> compute {} -> compare {}",
            self.scope.label(),
            self.grain.label(),
            match self.compute {
                QualityCompute::Sample => format!("{} rows", self.sample_rows),
                other => other.label().to_string(),
            },
            self.comparison_label()
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QualityPrecision {
    Metadata,
    Sampled,
    Exact,
    Estimated,
}

impl QualityPrecision {
    pub fn label(self) -> &'static str {
        match self {
            Self::Metadata => "metadata",
            Self::Sampled => "sampled",
            Self::Exact => "exact",
            Self::Estimated => "estimated",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum QualityMetric {
    #[default]
    NullRate,
    EmptyRate,
    WhitespaceRate,
    NonFiniteRate,
    DistinctShare,
    IntegerParseShare,
    DecimalParseShare,
}

impl QualityMetric {
    pub const ALL: [Self; 7] = [
        Self::NullRate,
        Self::EmptyRate,
        Self::WhitespaceRate,
        Self::NonFiniteRate,
        Self::DistinctShare,
        Self::IntegerParseShare,
        Self::DecimalParseShare,
    ];

    pub fn label(self) -> &'static str {
        match self {
            Self::NullRate => "Null rate",
            Self::EmptyRate => "Empty rate",
            Self::WhitespaceRate => "Whitespace rate",
            Self::NonFiniteRate => "Non-finite rate",
            Self::DistinctShare => "Distinct share",
            Self::IntegerParseShare => "Integer parse share",
            Self::DecimalParseShare => "Decimal parse share",
        }
    }

    pub fn value(self, column: &ColumnQualityProfile) -> Option<f64> {
        let ratio = |numerator: usize, denominator: usize| {
            (denominator > 0).then(|| numerator as f64 / denominator as f64)
        };
        match self {
            Self::NullRate => ratio(column.null_count, column.evaluated_rows),
            Self::EmptyRate => ratio(column.empty_count?, column.evaluated_rows),
            Self::WhitespaceRate => ratio(column.whitespace_count?, column.evaluated_rows),
            Self::NonFiniteRate => ratio(
                column.nan_count?
                    + column.positive_infinity_count?
                    + column.negative_infinity_count?,
                column.evaluated_rows,
            ),
            Self::DistinctShare => ratio(column.distinct_count?, column.non_null_rows()),
            Self::IntegerParseShare => ratio(column.integer_parse_count?, column.non_null_rows()),
            Self::DecimalParseShare => ratio(column.decimal_parse_count?, column.non_null_rows()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ColumnQualityProfile {
    pub name: String,
    pub dtype: DataType,
    pub evaluated_rows: usize,
    pub null_count: usize,
    pub empty_count: Option<usize>,
    pub whitespace_count: Option<usize>,
    pub nan_count: Option<usize>,
    pub positive_infinity_count: Option<usize>,
    pub negative_infinity_count: Option<usize>,
    pub distinct_count: Option<usize>,
    pub min: Option<String>,
    pub max: Option<String>,
    pub integer_parse_count: Option<usize>,
    pub decimal_parse_count: Option<usize>,
    pub date_parse_count: Option<usize>,
    pub datetime_parse_count: Option<usize>,
    pub dominant_value: Option<String>,
    pub dominant_count: Option<usize>,
    pub min_length: Option<usize>,
    pub max_length: Option<usize>,
}

impl ColumnQualityProfile {
    pub fn null_rate(&self) -> f64 {
        rate(self.null_count, self.evaluated_rows)
    }

    pub fn non_null_rows(&self) -> usize {
        self.evaluated_rows.saturating_sub(self.null_count)
    }

    pub fn uniqueness_rate(&self) -> Option<f64> {
        self.distinct_count
            .map(|count| rate(count, self.non_null_rows()))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationKind {
    Nulls,
    Empty,
    Whitespace,
    NonFinite,
    Constant,
    ParseableText,
    DuplicateRows,
    CategoryVariants,
}

impl ObservationKind {
    pub fn label(self) -> &'static str {
        match self {
            Self::Nulls => "Null",
            Self::Empty => "Empty",
            Self::Whitespace => "Whitespace",
            Self::NonFinite => "Non-finite",
            Self::Constant => "Constant",
            Self::ParseableText => "Stored as text",
            Self::DuplicateRows => "Duplicate rows",
            Self::CategoryVariants => "Category variants",
        }
    }
}

#[derive(Debug, Clone)]
pub struct QualityObservation {
    pub kind: ObservationKind,
    pub column: String,
    pub affected_rows: usize,
    pub evaluated_rows: usize,
    pub fact: String,
    pub normalized_category: Option<String>,
}

impl QualityObservation {
    pub fn evidence_predicate(&self) -> Option<Expr> {
        let value = col(&self.column);
        match self.kind {
            ObservationKind::Nulls => Some(value.is_null()),
            ObservationKind::Empty => Some(value.eq(lit(""))),
            ObservationKind::Whitespace => Some(
                value
                    .clone()
                    .str()
                    .strip_chars(lit(LiteralValue::untyped_null()))
                    .eq(lit(""))
                    .and(value.neq(lit(""))),
            ),
            ObservationKind::NonFinite => Some(
                value
                    .clone()
                    .is_nan()
                    .or(value.clone().eq(lit(f64::INFINITY)))
                    .or(value.eq(lit(f64::NEG_INFINITY))),
            ),
            ObservationKind::Constant => Some(value.is_not_null()),
            ObservationKind::CategoryVariants => Some(
                value
                    .str()
                    .strip_chars(lit(LiteralValue::untyped_null()))
                    .str()
                    .to_lowercase()
                    .eq(lit(self.normalized_category.clone()?)),
            ),
            ObservationKind::ParseableText | ObservationKind::DuplicateRows => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct IdentityProfile {
    pub duplicate_groups: usize,
    pub extra_rows: usize,
    pub rows_involved: usize,
    pub evaluated_rows: usize,
    pub precision: QualityPrecision,
}

#[derive(Debug, Clone)]
pub struct CategoryVariantGroup {
    pub column: String,
    pub normalized: String,
    pub variants: Vec<(String, usize)>,
    pub rows_involved: usize,
    pub complete: bool,
}

#[derive(Debug, Clone)]
pub struct SegmentQualityProfile {
    pub label: String,
    pub total_rows: Option<usize>,
    pub evaluated_rows: usize,
    pub columns: Vec<ColumnQualityProfile>,
    pub null_cells: usize,
    pub null_rate: f64,
    pub compared_with: Option<String>,
    pub largest_change: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TemporalLatencyProfile {
    pub segment: String,
    pub start_role: TemporalRole,
    pub end_role: TemporalRole,
    pub start_column: String,
    pub end_column: String,
    pub evaluated_rows: usize,
    pub missing_start: usize,
    pub missing_end: usize,
    pub negative_count: usize,
    pub p50_seconds: Option<i64>,
    pub p90_seconds: Option<i64>,
    pub p95_seconds: Option<i64>,
    pub p99_seconds: Option<i64>,
    pub max_seconds: Option<i64>,
    pub above_threshold_count: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct DataQualityResults {
    pub total_rows: usize,
    pub evaluated_rows: usize,
    pub precision: QualityPrecision,
    pub sample_seed: u64,
    pub columns: Vec<ColumnQualityProfile>,
    pub observations: Vec<QualityObservation>,
    pub segments: Vec<SegmentQualityProfile>,
    pub temporal: Vec<TemporalLatencyProfile>,
    pub identity: Option<IdentityProfile>,
    pub category_variants: Vec<CategoryVariantGroup>,
}

impl DataQualityResults {
    pub fn compare_segments(&mut self, plan: &DataQualityPlan) {
        apply_comparisons(
            &mut self.segments,
            &plan.grain,
            plan.comparison,
            plan.baseline_segment.as_deref(),
            self.precision,
        );
    }

    pub fn empty(total_rows: usize, plan: &DataQualityPlan, schema: &Schema) -> Self {
        Self {
            total_rows,
            evaluated_rows: 0,
            precision: QualityPrecision::Metadata,
            sample_seed: plan.sample_seed,
            columns: schema
                .iter()
                .map(|(name, dtype)| ColumnQualityProfile {
                    name: name.to_string(),
                    dtype: dtype.clone(),
                    evaluated_rows: 0,
                    null_count: 0,
                    empty_count: None,
                    whitespace_count: None,
                    nan_count: None,
                    positive_infinity_count: None,
                    negative_infinity_count: None,
                    distinct_count: None,
                    min: None,
                    max: None,
                    integer_parse_count: None,
                    decimal_parse_count: None,
                    date_parse_count: None,
                    datetime_parse_count: None,
                    dominant_value: None,
                    dominant_count: None,
                    min_length: None,
                    max_length: None,
                })
                .collect(),
            observations: Vec::new(),
            segments: Vec::new(),
            temporal: Vec::new(),
            identity: None,
            category_variants: Vec::new(),
        }
    }
}

pub fn compute_data_quality(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
) -> Result<DataQualityResults> {
    let collected_schema = lf.clone().collect_schema()?;
    let schema = visible_schema(&collected_schema, source);
    if plan.compute == QualityCompute::Metadata {
        return Ok(DataQualityResults::empty(total_rows, plan, &schema));
    }
    if plan.compute == QualityCompute::Full {
        return compute_full_quality(lf, total_rows, plan, source, &schema, polars_streaming);
    }

    let (profile_df, sample_positions, evaluated_rows, precision) = match plan.compute {
        QualityCompute::Sample if total_rows > plan.sample_rows.min(50_000) => {
            let (df, positions) =
                sample_quality_rows(lf, plan.sample_rows, plan.sample_seed, polars_streaming)?;
            let height = df.height();
            (df, Some(positions), height, QualityPrecision::Sampled)
        }
        QualityCompute::Sample => {
            let df = collect_lazy(lf.clone(), polars_streaming).map_err(Report::from)?;
            let height = df.height();
            (df, None, height, QualityPrecision::Exact)
        }
        QualityCompute::Metadata | QualityCompute::Full => unreachable!(),
    };

    let profile_df = attach_source_file(profile_df, source)?;
    let mut columns = profile_columns(&profile_df, &schema, polars_streaming)?;
    add_value_details(&profile_df, &mut columns)?;
    let identity = profile_identity(&profile_df, &schema, precision)?;
    let category_variants = profile_category_variants(&profile_df, &schema)?;
    let mut observations = observations_from_profiles(&columns);
    observations.extend(identity_observations(&identity, &category_variants));
    let segments = profile_segments(
        &profile_df,
        total_rows,
        plan,
        precision,
        &schema,
        sample_positions.as_deref(),
        polars_streaming,
    )?;
    let temporal = profile_temporal(&profile_df, plan, sample_positions.as_deref())?;

    Ok(DataQualityResults {
        total_rows,
        evaluated_rows,
        precision,
        sample_seed: plan.sample_seed,
        columns,
        observations,
        segments,
        temporal,
        identity: Some(identity),
        category_variants,
    })
}

fn sample_quality_rows(
    lf: &LazyFrame,
    sample_rows: usize,
    seed: u64,
    polars_streaming: bool,
) -> Result<(DataFrame, Vec<u32>)> {
    let requested = sample_rows.min(50_000);
    let candidate_rows = requested.saturating_mul(2).min(50_000);
    let candidates = collect_lazy(lf.clone().limit(candidate_rows as u32), polars_streaming)
        .map_err(Report::from)?;
    if candidates.height() <= requested {
        let positions = (0..candidates.height() as u32).collect();
        return Ok((candidates, positions));
    }
    let mut indices = (0..candidates.height() as u32).collect::<Vec<_>>();
    let mut random = seed;
    for index in 0..requested {
        // SplitMix64 produces a stable selection without adding a runtime dependency.
        random = random.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut value = random;
        value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        value ^= value >> 31;
        let selected = index + value as usize % (indices.len() - index);
        indices.swap(index, selected);
    }
    indices.truncate(requested);
    indices.sort_unstable();
    let sampled = candidates.take(&UInt32Chunked::new(
        "quality_sample".into(),
        indices.clone(),
    ))?;
    Ok((sampled, indices))
}

fn compute_full_quality(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<DataQualityResults> {
    let aggregate = collect_lazy(
        lf.clone().select(build_profile_exprs(schema)),
        polars_streaming,
    )
    .map_err(Report::from)?;
    let mut columns = parse_profiles(&aggregate, schema, total_rows);
    add_dominance_lazy(lf, &mut columns, polars_streaming)?;
    let identity = profile_identity_lazy(lf, schema, total_rows, polars_streaming)?;
    let category_variants = profile_category_variants_lazy(lf, schema, polars_streaming)?;
    let mut observations = observations_from_profiles(&columns);
    observations.extend(identity_observations(&identity, &category_variants));
    let segments = profile_segments_lazy(lf, total_rows, plan, source, schema, polars_streaming)?;
    let temporal = profile_temporal_lazy(lf, plan, source, polars_streaming)?;
    Ok(DataQualityResults {
        total_rows,
        evaluated_rows: total_rows,
        precision: QualityPrecision::Exact,
        sample_seed: plan.sample_seed,
        columns,
        observations,
        segments,
        temporal,
        identity: Some(identity),
        category_variants,
    })
}

fn add_dominance_lazy(
    lf: &LazyFrame,
    profiles: &mut [ColumnQualityProfile],
    polars_streaming: bool,
) -> Result<()> {
    for profile in profiles {
        let count = "__quality_value_count";
        let query = lf
            .clone()
            .filter(col(&profile.name).is_not_null())
            .group_by([col(&profile.name)])
            .agg([len().alias(count)])
            .sort(
                [count],
                SortMultipleOptions::default().with_order_descending(true),
            )
            .limit(1);
        let top = collect_lazy(query, polars_streaming).map_err(Report::from)?;
        profile.dominant_value = string_value_at(&top, &profile.name, 0);
        profile.dominant_count = optional_usize_at(&top, count, 0);
    }
    Ok(())
}

fn profile_category_variants_lazy(
    lf: &LazyFrame,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<CategoryVariantGroup>> {
    let normalized_name = "__quality_normalized";
    let original_name = "__quality_original";
    let count_name = "__quality_variant_rows";
    let variant_count_name = "__quality_variant_count";
    let mut result = Vec::new();
    for (name, dtype) in schema.iter() {
        if !matches!(dtype, DataType::String | DataType::Categorical(..)) {
            continue;
        }
        let original = col(name.as_str());
        let normalized = original
            .clone()
            .str()
            .strip_chars(lit(LiteralValue::untyped_null()))
            .str()
            .to_lowercase();
        let variant_count = col(original_name)
            .n_unique()
            .over([col(normalized_name)])?
            .alias(variant_count_name);
        let query = lf
            .clone()
            .filter(original.clone().is_not_null())
            .select([
                normalized.alias(normalized_name),
                original.alias(original_name),
            ])
            .group_by([col(normalized_name), col(original_name)])
            .agg([len().alias(count_name)])
            .with_columns([variant_count])
            .filter(col(variant_count_name).gt(lit(1u32)))
            .limit(1_001);
        let groups = collect_lazy(query, polars_streaming).map_err(Report::from)?;
        let complete = groups.height() <= 1_000;
        let mut by_normalized = BTreeMap::<String, Vec<(String, usize)>>::new();
        for row in 0..groups.height().min(1_000) {
            let Some(normalized) = string_value_at(&groups, normalized_name, row) else {
                continue;
            };
            let Some(original) = string_value_at(&groups, original_name, row) else {
                continue;
            };
            let count = usize_value_at(&groups, count_name, row);
            by_normalized
                .entry(normalized)
                .or_default()
                .push((original, count));
        }
        for (normalized, variants) in by_normalized {
            if variants.len() < 2 {
                continue;
            }
            let rows_involved = variants.iter().map(|(_, count)| count).sum();
            result.push(CategoryVariantGroup {
                column: name.to_string(),
                normalized,
                variants,
                rows_involved,
                complete,
            });
            if result.len() >= 100 {
                return Ok(result);
            }
        }
    }
    Ok(result)
}

fn profile_identity_lazy(
    lf: &LazyFrame,
    schema: &Schema,
    total_rows: usize,
    polars_streaming: bool,
) -> Result<IdentityProfile> {
    let keys = schema
        .iter_names()
        .map(|name| col(name.as_str()))
        .collect::<Vec<_>>();
    let duplicate_count = "__quality_duplicate_count";
    let grouped = lf
        .clone()
        .group_by(keys)
        .agg([len().alias(duplicate_count)])
        .filter(col(duplicate_count).gt(lit(1u32)))
        .select([
            len().alias("duplicate_groups"),
            (col(duplicate_count) - lit(1u32)).sum().alias("extra_rows"),
            col(duplicate_count).sum().alias("rows_involved"),
        ]);
    let summary = collect_lazy(grouped, polars_streaming).map_err(Report::from)?;
    Ok(IdentityProfile {
        duplicate_groups: usize_value(&summary, "duplicate_groups"),
        extra_rows: usize_value(&summary, "extra_rows"),
        rows_involved: usize_value(&summary, "rows_involved"),
        evaluated_rows: total_rows,
        precision: QualityPrecision::Exact,
    })
}

fn visible_schema(schema: &Schema, source: Option<&QualitySourceContext>) -> Schema {
    let mut visible = Schema::with_capacity(schema.len());
    for (name, dtype) in schema.iter() {
        if source.is_some_and(|context| name.as_str() == context.row_index_column) {
            continue;
        }
        visible.insert(name.clone(), dtype.clone());
    }
    visible
}

fn attach_source_file(
    mut df: DataFrame,
    source: Option<&QualitySourceContext>,
) -> Result<DataFrame> {
    let Some(source) = source else {
        return Ok(df);
    };
    let rows = df.drop_in_place(&source.row_index_column)?;
    let rows = rows.u32()?;
    let names: Vec<Option<&str>> = rows
        .iter()
        .map(|row| {
            let row = row? as usize;
            let file = source
                .file_starts
                .partition_point(|start| *start <= row)
                .saturating_sub(1);
            source.file_names.get(file).map(String::as_str)
        })
        .collect();
    df.with_column(Column::new(QUALITY_SOURCE_FILE_COLUMN.into(), names))?;
    Ok(df)
}

fn profile_columns(
    df: &DataFrame,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<ColumnQualityProfile>> {
    let aggregate = collect_lazy(
        df.clone().lazy().select(build_profile_exprs(schema)),
        polars_streaming,
    )
    .map_err(Report::from)?;
    Ok(parse_profiles(&aggregate, schema, df.height()))
}

fn add_value_details(df: &DataFrame, profiles: &mut [ColumnQualityProfile]) -> Result<()> {
    for profile in profiles {
        let values = df.column(&profile.name)?;
        let mut counts = BTreeMap::<String, usize>::new();
        for row in 0..df.height() {
            let value = values.get(row)?;
            if !value.is_null() {
                *counts.entry(value.str_value().to_string()).or_default() += 1;
            }
        }
        if let Some((value, count)) = counts
            .into_iter()
            .max_by(|left, right| left.1.cmp(&right.1).then_with(|| right.0.cmp(&left.0)))
        {
            profile.dominant_value = Some(value);
            profile.dominant_count = Some(count);
        }
        if matches!(profile.dtype, DataType::String | DataType::Categorical(..)) {
            let mut dates = 0;
            let mut datetimes = 0;
            for row in 0..df.height() {
                let value = values.get(row)?;
                if value.is_null() {
                    continue;
                }
                let text = value.str_value();
                if chrono::NaiveDate::parse_from_str(&text, "%Y-%m-%d").is_ok() {
                    dates += 1;
                }
                if chrono::DateTime::parse_from_rfc3339(&text).is_ok()
                    || chrono::NaiveDateTime::parse_from_str(&text, "%Y-%m-%d %H:%M:%S").is_ok()
                {
                    datetimes += 1;
                }
            }
            profile.date_parse_count = Some(dates);
            profile.datetime_parse_count = Some(datetimes);
        }
    }
    Ok(())
}

fn profile_identity(
    df: &DataFrame,
    schema: &Schema,
    precision: QualityPrecision,
) -> Result<IdentityProfile> {
    let columns = schema
        .iter_names()
        .map(|name| df.column(name))
        .collect::<PolarsResult<Vec<_>>>()?;
    let mut groups = BTreeMap::<Vec<Option<String>>, usize>::new();
    for row in 0..df.height() {
        let mut key = Vec::with_capacity(columns.len());
        for column in &columns {
            let value = column.get(row)?;
            key.push((!value.is_null()).then(|| value.str_value().to_string()));
        }
        *groups.entry(key).or_default() += 1;
    }
    let duplicates = groups.into_values().filter(|count| *count > 1);
    let mut duplicate_groups = 0;
    let mut extra_rows = 0;
    let mut rows_involved = 0;
    for count in duplicates {
        duplicate_groups += 1;
        extra_rows += count - 1;
        rows_involved += count;
    }
    Ok(IdentityProfile {
        duplicate_groups,
        extra_rows,
        rows_involved,
        evaluated_rows: df.height(),
        precision,
    })
}

fn profile_category_variants(df: &DataFrame, schema: &Schema) -> Result<Vec<CategoryVariantGroup>> {
    let mut result = Vec::new();
    for (name, dtype) in schema.iter() {
        if !matches!(dtype, DataType::String | DataType::Categorical(..)) {
            continue;
        }
        let column = df.column(name)?;
        let mut normalized = BTreeMap::<String, BTreeMap<String, usize>>::new();
        for row in 0..df.height() {
            let value = column.get(row)?;
            if value.is_null() {
                continue;
            }
            let original = value.str_value().to_string();
            *normalized
                .entry(original.trim().to_lowercase())
                .or_default()
                .entry(original)
                .or_default() += 1;
        }
        for (normalized, variants) in normalized {
            if variants.len() <= 1 {
                continue;
            }
            let variants = variants.into_iter().collect::<Vec<_>>();
            let rows_involved = variants.iter().map(|(_, count)| count).sum();
            result.push(CategoryVariantGroup {
                column: name.to_string(),
                normalized,
                variants,
                rows_involved,
                complete: true,
            });
            if result.len() >= 100 {
                return Ok(result);
            }
        }
    }
    Ok(result)
}

fn identity_observations(
    identity: &IdentityProfile,
    variants: &[CategoryVariantGroup],
) -> Vec<QualityObservation> {
    let mut observations = Vec::new();
    if identity.duplicate_groups > 0 {
        observations.push(QualityObservation {
            kind: ObservationKind::DuplicateRows,
            column: "all columns".to_string(),
            affected_rows: identity.rows_involved,
            evaluated_rows: identity.evaluated_rows,
            fact: format!(
                "{} groups; {} extra rows ({})",
                identity.duplicate_groups,
                identity.extra_rows,
                identity.precision.label()
            ),
            normalized_category: None,
        });
    }
    observations.extend(variants.iter().map(|group| QualityObservation {
        kind: ObservationKind::CategoryVariants,
        column: group.column.clone(),
        affected_rows: group.rows_involved,
        evaluated_rows: identity.evaluated_rows,
        fact: format!(
            "{}{} variants normalize to {:?}",
            if group.complete { "" } else { "at least " },
            group.variants.len(),
            group.normalized
        ),
        normalized_category: Some(group.normalized.clone()),
    }));
    observations
}

#[derive(Debug)]
struct SegmentRows {
    label: String,
    indices: Vec<u32>,
}

fn segment_rows(
    df: &DataFrame,
    plan: &DataQualityPlan,
    sample_positions: Option<&[u32]>,
) -> Result<Vec<SegmentRows>> {
    let all_rows = || SegmentRows {
        label: "current view".to_string(),
        indices: (0..df.height() as u32).collect(),
    };
    let groups = match &plan.grain {
        QualityGrain::Dataset => vec![all_rows()],
        QualityGrain::RowChunks(size) => {
            let size = (*size).max(1);
            let mut chunks = BTreeMap::<usize, Vec<u32>>::new();
            for row in 0..df.height() {
                let position = sample_positions
                    .and_then(|positions| positions.get(row))
                    .copied()
                    .unwrap_or(row as u32) as usize;
                chunks.entry(position / size).or_default().push(row as u32);
            }
            chunks
                .into_iter()
                .map(|(chunk, indices)| SegmentRows {
                    label: format!(
                        "rows {}-{}",
                        chunk.saturating_mul(size) + 1,
                        (chunk + 1).saturating_mul(size)
                    ),
                    indices,
                })
                .collect()
        }
        QualityGrain::Partition(column) => group_by_value(df, column, "partition")?,
        QualityGrain::TimeWindows { column, every } => group_by_time_window(df, column, every)?,
        QualityGrain::File => {
            if df.column(QUALITY_SOURCE_FILE_COLUMN).is_ok() {
                group_by_value(df, QUALITY_SOURCE_FILE_COLUMN, "file")?
            } else {
                vec![SegmentRows {
                    label: "file mapping unavailable for this view".to_string(),
                    indices: (0..df.height() as u32).collect(),
                }]
            }
        }
    };
    Ok(groups)
}

fn group_by_value(df: &DataFrame, column: &str, kind: &str) -> Result<Vec<SegmentRows>> {
    let values = df.column(column)?;
    let mut groups: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    for row in 0..df.height() {
        let value = values.get(row)?;
        let label = if value.is_null() {
            format!("{kind} ∅")
        } else {
            format!("{kind} {}", value.str_value())
        };
        groups.entry(label).or_default().push(row as u32);
    }
    Ok(groups
        .into_iter()
        .map(|(label, indices)| SegmentRows { label, indices })
        .collect())
}

fn group_by_time_window(df: &DataFrame, column: &str, every: &str) -> Result<Vec<SegmentRows>> {
    let values = df.column(column)?;
    let width = duration_micros(every).ok_or_else(|| {
        color_eyre::eyre::eyre!("unsupported time window {every}; use h, d, or w")
    })?;
    let mut groups: BTreeMap<i64, Vec<u32>> = BTreeMap::new();
    let mut missing = Vec::new();
    for row in 0..df.height() {
        match value_epoch_micros(values.get(row)?) {
            Some(value) => groups
                .entry(value.div_euclid(width) * width)
                .or_default()
                .push(row as u32),
            None => missing.push(row as u32),
        }
    }
    let mut result: Vec<SegmentRows> = groups
        .into_iter()
        .map(|(start, indices)| SegmentRows {
            label: format!("{} / {every}", format_epoch_micros(start)),
            indices,
        })
        .collect();
    if !missing.is_empty() {
        result.push(SegmentRows {
            label: format!("{column} ∅"),
            indices: missing,
        });
    }
    Ok(result)
}

fn duration_micros(value: &str) -> Option<i64> {
    let (number, unit) = value.split_at(value.len().checked_sub(1)?);
    let number = number.parse::<i64>().ok()?;
    let unit = match unit {
        "h" => 3_600_000_000,
        "d" => 86_400_000_000,
        "w" => 604_800_000_000,
        _ => return None,
    };
    number.checked_mul(unit)
}

fn format_epoch_micros(value: i64) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp_micros(value)
        .map(|stamp| stamp.format("%Y-%m-%d %H:%MZ").to_string())
        .unwrap_or_else(|| value.to_string())
}

fn value_epoch_micros(value: AnyValue<'_>) -> Option<i64> {
    match value {
        AnyValue::Date(days) => Some(i64::from(days) * 86_400_000_000),
        AnyValue::Datetime(value, TimeUnit::Nanoseconds, _) => Some(value / 1_000),
        AnyValue::Datetime(value, TimeUnit::Microseconds, _) => Some(value),
        AnyValue::Datetime(value, TimeUnit::Milliseconds, _) => Some(value * 1_000),
        _ => None,
    }
}

fn take_rows(df: &DataFrame, indices: &[u32]) -> PolarsResult<DataFrame> {
    df.take(&UInt32Chunked::new("quality_rows".into(), indices.to_vec()))
}

fn profile_segments(
    df: &DataFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    precision: QualityPrecision,
    schema: &Schema,
    sample_positions: Option<&[u32]>,
    polars_streaming: bool,
) -> Result<Vec<SegmentQualityProfile>> {
    let groups = segment_rows(df, plan, sample_positions)?;
    let mut profiles = Vec::with_capacity(groups.len());
    for group in groups {
        let segment = take_rows(df, &group.indices)?;
        let columns = profile_columns(&segment, schema, polars_streaming)?;
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = segment.height().saturating_mul(columns.len());
        profiles.push(SegmentQualityProfile {
            label: group.label,
            total_rows: if matches!(plan.grain, QualityGrain::Dataset) {
                Some(total_rows)
            } else if precision == QualityPrecision::Exact {
                Some(segment.height())
            } else {
                None
            },
            evaluated_rows: segment.height(),
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
        });
    }
    apply_comparisons(
        &mut profiles,
        &plan.grain,
        plan.comparison,
        plan.baseline_segment.as_deref(),
        precision,
    );
    Ok(profiles)
}

fn profile_segments_lazy(
    lf: &LazyFrame,
    total_rows: usize,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    schema: &Schema,
    polars_streaming: bool,
) -> Result<Vec<SegmentQualityProfile>> {
    if matches!(plan.grain, QualityGrain::Dataset)
        || matches!(plan.grain, QualityGrain::File) && source.is_none()
    {
        let aggregate = collect_lazy(
            lf.clone().select(build_profile_exprs(schema)),
            polars_streaming,
        )
        .map_err(Report::from)?;
        let columns = parse_profiles(&aggregate, schema, total_rows);
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = total_rows.saturating_mul(schema.len());
        return Ok(vec![SegmentQualityProfile {
            label: if matches!(plan.grain, QualityGrain::File) {
                "file mapping unavailable for this view".to_string()
            } else {
                "current view".to_string()
            },
            total_rows: Some(total_rows),
            evaluated_rows: total_rows,
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
        }]);
    }

    let (grouped_lf, group) = grouped_frame(lf, &plan.grain, source)?;
    let mut aggregates = vec![len().alias("__quality_segment_rows")];
    aggregates.extend(build_profile_exprs(schema));
    let grouped = collect_lazy(
        grouped_lf
            .group_by([group.alias("__quality_segment")])
            .agg(aggregates),
        polars_streaming,
    )
    .map_err(Report::from)?;
    let mut segments = Vec::with_capacity(grouped.height());
    for row in 0..grouped.height() {
        let evaluated_rows = usize_value_at(&grouped, "__quality_segment_rows", row);
        let columns = parse_profiles_at(&grouped, schema, evaluated_rows, row);
        let null_cells = columns
            .iter()
            .map(|column| column.null_count)
            .sum::<usize>();
        let denominator = evaluated_rows.saturating_mul(schema.len());
        let raw_label =
            string_value_at(&grouped, "__quality_segment", row).unwrap_or_else(|| "∅".to_string());
        segments.push(SegmentQualityProfile {
            label: segment_label(&plan.grain, &raw_label),
            total_rows: Some(evaluated_rows),
            evaluated_rows,
            columns,
            null_cells,
            null_rate: rate(null_cells, denominator),
            compared_with: None,
            largest_change: None,
        });
    }
    segments.sort_by(|left, right| left.label.cmp(&right.label));
    if matches!(plan.grain, QualityGrain::RowChunks(_)) {
        for segment in &mut segments {
            segment.label = pretty_chunk_label(&segment.label);
        }
    }
    apply_comparisons(
        &mut segments,
        &plan.grain,
        plan.comparison,
        plan.baseline_segment.as_deref(),
        QualityPrecision::Exact,
    );
    Ok(segments)
}

fn grouped_frame(
    lf: &LazyFrame,
    grain: &QualityGrain,
    source: Option<&QualitySourceContext>,
) -> Result<(LazyFrame, Expr)> {
    match grain {
        QualityGrain::Dataset => Err(color_eyre::eyre::eyre!(
            "dataset grain does not need grouping"
        )),
        QualityGrain::Partition(column) => Ok((lf.clone(), col(column))),
        QualityGrain::RowChunks(size) => {
            let row = "__datui_quality_row";
            Ok((
                lf.clone().with_row_index(row, None),
                col(row).cast(DataType::UInt64) / lit((*size).max(1) as u64),
            ))
        }
        QualityGrain::TimeWindows { column, every } => Ok((
            lf.clone(),
            col(column)
                .cast(DataType::Datetime(TimeUnit::Microseconds, None))
                .dt()
                .truncate(lit(every.clone())),
        )),
        QualityGrain::File => {
            let source = source
                .ok_or_else(|| color_eyre::eyre::eyre!("source-file mapping is unavailable"))?;
            let mut file = lit("unknown");
            for (start, name) in source.file_starts.iter().zip(source.file_names.iter()) {
                file = when(col(&source.row_index_column).gt_eq(lit(*start as u32)))
                    .then(lit(name.clone()))
                    .otherwise(file);
            }
            Ok((lf.clone(), file))
        }
    }
}

fn pretty_chunk_label(label: &str) -> String {
    let Some(range) = label.strip_prefix("rows ") else {
        return label.to_string();
    };
    let Some((start, end)) = range.split_once('-') else {
        return label.to_string();
    };
    let start = start.trim_start_matches('0');
    let end = end.trim_start_matches('0');
    format!(
        "rows {}-{}",
        if start.is_empty() { "0" } else { start },
        if end.is_empty() { "0" } else { end }
    )
}

fn segment_label(grain: &QualityGrain, raw: &str) -> String {
    match grain {
        QualityGrain::RowChunks(size) => raw
            .parse::<usize>()
            .map(|chunk| {
                let start = chunk.saturating_mul(*size) + 1;
                let end = start.saturating_add(*size).saturating_sub(1);
                format!("rows {start:012}-{end:012}")
            })
            .unwrap_or_else(|_| format!("rows {raw}")),
        QualityGrain::Partition(_) => format!("partition {raw}"),
        QualityGrain::TimeWindows { every, .. } => format!("{raw} / {every}"),
        QualityGrain::File => format!("file {raw}"),
        QualityGrain::Dataset => "current view".to_string(),
    }
}

fn apply_comparisons(
    segments: &mut [SegmentQualityProfile],
    grain: &QualityGrain,
    comparison: QualityComparison,
    baseline_segment: Option<&str>,
    precision: QualityPrecision,
) {
    for segment in segments.iter_mut() {
        segment.compared_with = None;
        segment.largest_change = None;
    }
    if comparison == QualityComparison::Previous
        && matches!(grain, QualityGrain::File | QualityGrain::Partition(_))
    {
        for segment in segments {
            segment.largest_change = Some("previous unavailable: choose an order".to_string());
        }
        return;
    }
    let baseline_index = baseline_segment
        .and_then(|label| segments.iter().position(|segment| segment.label == label))
        .or_else(|| baseline_segment.is_none().then_some(0));
    if comparison == QualityComparison::Baseline && baseline_index.is_none() {
        for segment in segments {
            segment.largest_change = Some("selected baseline unavailable".to_string());
        }
        return;
    }
    for index in 0..segments.len() {
        let compared = match comparison {
            QualityComparison::None => None,
            QualityComparison::Previous if index > 0 => Some(index - 1),
            QualityComparison::Baseline if Some(index) != baseline_index => baseline_index,
            QualityComparison::Previous | QualityComparison::Baseline => None,
        };
        if let Some(other) = compared {
            let change = (segments[index].null_rate - segments[other].null_rate) * 100.0;
            segments[index].compared_with = Some(segments[other].label.clone());
            segments[index].largest_change = Some(format!(
                "null cells {change:+.2} pp ({})",
                precision.label()
            ));
        }
    }
}

fn profile_temporal(
    df: &DataFrame,
    plan: &DataQualityPlan,
    sample_positions: Option<&[u32]>,
) -> Result<Vec<TemporalLatencyProfile>> {
    let role_column = |role| {
        plan.temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .map(|assignment| assignment.column.as_str())
    };
    let pairs = [
        (TemporalRole::Event, TemporalRole::Published),
        (TemporalRole::Event, TemporalRole::Received),
        (TemporalRole::PeriodEnd, TemporalRole::Published),
        (TemporalRole::Published, TemporalRole::Received),
        (TemporalRole::Received, TemporalRole::Processed),
        (TemporalRole::Event, TemporalRole::Processed),
    ];
    let groups = segment_rows(df, plan, sample_positions)?;
    let mut profiles = Vec::new();
    for group in groups {
        let segment = take_rows(df, &group.indices)?;
        for (start_role, end_role) in pairs {
            let (Some(start_column), Some(end_column)) =
                (role_column(start_role), role_column(end_role))
            else {
                continue;
            };
            profiles.push(latency_profile(
                &segment,
                &group.label,
                start_role,
                end_role,
                start_column,
                end_column,
                plan.latency_threshold_seconds,
            )?);
        }
    }
    Ok(profiles)
}

fn profile_temporal_lazy(
    lf: &LazyFrame,
    plan: &DataQualityPlan,
    source: Option<&QualitySourceContext>,
    polars_streaming: bool,
) -> Result<Vec<TemporalLatencyProfile>> {
    let role_column = |role| {
        plan.temporal_roles
            .iter()
            .find(|assignment| assignment.role == role)
            .map(|assignment| assignment.column.as_str())
    };
    let supported = [
        (TemporalRole::Event, TemporalRole::Published),
        (TemporalRole::Event, TemporalRole::Received),
        (TemporalRole::PeriodEnd, TemporalRole::Published),
        (TemporalRole::Published, TemporalRole::Received),
        (TemporalRole::Received, TemporalRole::Processed),
        (TemporalRole::Event, TemporalRole::Processed),
    ];
    let pairs = supported
        .into_iter()
        .filter_map(|(start_role, end_role)| {
            Some((
                start_role,
                end_role,
                role_column(start_role)?.to_string(),
                role_column(end_role)?.to_string(),
            ))
        })
        .collect::<Vec<_>>();
    if pairs.is_empty() {
        return Ok(Vec::new());
    }

    let mut expressions = vec![len().alias("__quality_temporal_rows")];
    for (index, (_, _, start_column, end_column)) in pairs.iter().enumerate() {
        let prefix = format!("latency::{index}::");
        let start = col(start_column);
        let end = col(end_column);
        let duration = (end
            .clone()
            .cast(DataType::Datetime(TimeUnit::Microseconds, None))
            - start
                .clone()
                .cast(DataType::Datetime(TimeUnit::Microseconds, None)))
        .dt()
        .total_seconds(false);
        expressions.extend([
            start
                .is_null()
                .sum()
                .alias(format!("{prefix}missing_start")),
            end.is_null().sum().alias(format!("{prefix}missing_end")),
            duration
                .clone()
                .lt(lit(0i64))
                .sum()
                .alias(format!("{prefix}negative")),
            duration
                .clone()
                .quantile(lit(0.50), QuantileMethod::Nearest)
                .alias(format!("{prefix}p50")),
            duration
                .clone()
                .quantile(lit(0.90), QuantileMethod::Nearest)
                .alias(format!("{prefix}p90")),
            duration
                .clone()
                .quantile(lit(0.95), QuantileMethod::Nearest)
                .alias(format!("{prefix}p95")),
            duration
                .clone()
                .quantile(lit(0.99), QuantileMethod::Nearest)
                .alias(format!("{prefix}p99")),
            duration.clone().max().alias(format!("{prefix}max")),
        ]);
        if let Some(threshold) = plan.latency_threshold_seconds {
            expressions.push(
                duration
                    .gt(lit(threshold))
                    .sum()
                    .alias(format!("{prefix}above")),
            );
        }
    }

    let ungrouped = matches!(plan.grain, QualityGrain::Dataset)
        || matches!(plan.grain, QualityGrain::File) && source.is_none();
    let aggregate = if ungrouped {
        collect_lazy(lf.clone().select(expressions), polars_streaming).map_err(Report::from)?
    } else {
        let (grouped_lf, group) = grouped_frame(lf, &plan.grain, source)?;
        collect_lazy(
            grouped_lf
                .group_by([group.alias("__quality_segment")])
                .agg(expressions),
            polars_streaming,
        )
        .map_err(Report::from)?
    };

    let mut profiles = Vec::new();
    for row in 0..aggregate.height() {
        let segment = if ungrouped {
            if matches!(plan.grain, QualityGrain::File) {
                "file mapping unavailable for this view".to_string()
            } else {
                "current view".to_string()
            }
        } else {
            let raw = string_value_at(&aggregate, "__quality_segment", row)
                .unwrap_or_else(|| "∅".to_string());
            segment_label(&plan.grain, &raw)
        };
        let evaluated_rows = usize_value_at(&aggregate, "__quality_temporal_rows", row);
        for (index, (start_role, end_role, start_column, end_column)) in pairs.iter().enumerate() {
            let prefix = format!("latency::{index}::");
            profiles.push(TemporalLatencyProfile {
                segment: segment.clone(),
                start_role: *start_role,
                end_role: *end_role,
                start_column: start_column.clone(),
                end_column: end_column.clone(),
                evaluated_rows,
                missing_start: usize_value_at(&aggregate, &format!("{prefix}missing_start"), row),
                missing_end: usize_value_at(&aggregate, &format!("{prefix}missing_end"), row),
                negative_count: usize_value_at(&aggregate, &format!("{prefix}negative"), row),
                p50_seconds: optional_i64_at(&aggregate, &format!("{prefix}p50"), row),
                p90_seconds: optional_i64_at(&aggregate, &format!("{prefix}p90"), row),
                p95_seconds: optional_i64_at(&aggregate, &format!("{prefix}p95"), row),
                p99_seconds: optional_i64_at(&aggregate, &format!("{prefix}p99"), row),
                max_seconds: optional_i64_at(&aggregate, &format!("{prefix}max"), row),
                above_threshold_count: plan
                    .latency_threshold_seconds
                    .map(|_| usize_value_at(&aggregate, &format!("{prefix}above"), row)),
            });
        }
    }
    profiles.sort_by(|left, right| left.segment.cmp(&right.segment));
    Ok(profiles)
}

fn latency_profile(
    df: &DataFrame,
    segment: &str,
    start_role: TemporalRole,
    end_role: TemporalRole,
    start_column: &str,
    end_column: &str,
    threshold_seconds: Option<i64>,
) -> Result<TemporalLatencyProfile> {
    let starts = df.column(start_column)?;
    let ends = df.column(end_column)?;
    let mut missing_start = 0;
    let mut missing_end = 0;
    let mut seconds = Vec::new();
    for row in 0..df.height() {
        let start = value_epoch_micros(starts.get(row)?);
        let end = value_epoch_micros(ends.get(row)?);
        if start.is_none() {
            missing_start += 1;
        }
        if end.is_none() {
            missing_end += 1;
        }
        if let (Some(start), Some(end)) = (start, end) {
            seconds.push((end - start) / 1_000_000);
        }
    }
    seconds.sort_unstable();
    let percentile = |percent: usize| {
        if seconds.is_empty() {
            None
        } else {
            let index = ((seconds.len() - 1) * percent + 50) / 100;
            seconds.get(index).copied()
        }
    };
    Ok(TemporalLatencyProfile {
        segment: segment.to_string(),
        start_role,
        end_role,
        start_column: start_column.to_string(),
        end_column: end_column.to_string(),
        evaluated_rows: df.height(),
        missing_start,
        missing_end,
        negative_count: seconds.iter().filter(|value| **value < 0).count(),
        p50_seconds: percentile(50),
        p90_seconds: percentile(90),
        p95_seconds: percentile(95),
        p99_seconds: percentile(99),
        max_seconds: seconds.last().copied(),
        above_threshold_count: threshold_seconds
            .map(|threshold| seconds.iter().filter(|value| **value > threshold).count()),
    })
}

fn build_profile_exprs(schema: &Schema) -> Vec<Expr> {
    let mut exprs = Vec::new();
    for (name, dtype) in schema.iter() {
        let column = col(name.as_str());
        let prefix = format!("{}::", name);
        exprs.push(column.clone().null_count().alias(format!("{prefix}null")));
        exprs.push(
            column
                .clone()
                .filter(column.clone().is_not_null())
                .n_unique()
                .alias(format!("{prefix}distinct")),
        );

        if supports_range(dtype) {
            exprs.push(column.clone().min().alias(format!("{prefix}min")));
            exprs.push(column.clone().max().alias(format!("{prefix}max")));
        }

        if matches!(dtype, DataType::String | DataType::Categorical(..)) {
            let trimmed = column
                .clone()
                .str()
                .strip_chars(lit(LiteralValue::untyped_null()));
            exprs.push(
                column
                    .clone()
                    .eq(lit(""))
                    .sum()
                    .alias(format!("{prefix}empty")),
            );
            exprs.push(
                trimmed
                    .eq(lit(""))
                    .and(column.clone().neq(lit("")))
                    .sum()
                    .alias(format!("{prefix}whitespace")),
            );
            exprs.push(
                column
                    .clone()
                    .cast(DataType::Int64)
                    .is_not_null()
                    .and(column.clone().is_not_null())
                    .sum()
                    .alias(format!("{prefix}parse_int")),
            );
            exprs.push(
                column
                    .clone()
                    .cast(DataType::Float64)
                    .is_not_null()
                    .and(column.clone().is_not_null())
                    .sum()
                    .alias(format!("{prefix}parse_decimal")),
            );
            exprs.push(
                column
                    .clone()
                    .str()
                    .len_chars()
                    .min()
                    .alias(format!("{prefix}min_length")),
            );
            exprs.push(
                column
                    .clone()
                    .str()
                    .len_chars()
                    .max()
                    .alias(format!("{prefix}max_length")),
            );
        }

        if matches!(dtype, DataType::List(_)) {
            exprs.push(
                column
                    .clone()
                    .list()
                    .len()
                    .min()
                    .alias(format!("{prefix}min_length")),
            );
            exprs.push(
                column
                    .clone()
                    .list()
                    .len()
                    .max()
                    .alias(format!("{prefix}max_length")),
            );
        }

        if dtype.is_float() {
            let float = column.cast(DataType::Float64);
            exprs.push(float.clone().is_nan().sum().alias(format!("{prefix}nan")));
            exprs.push(
                float
                    .clone()
                    .eq(lit(f64::INFINITY))
                    .sum()
                    .alias(format!("{prefix}pos_inf")),
            );
            exprs.push(
                float
                    .eq(lit(f64::NEG_INFINITY))
                    .sum()
                    .alias(format!("{prefix}neg_inf")),
            );
        }
    }
    exprs
}

fn supports_range(dtype: &DataType) -> bool {
    dtype.is_numeric()
        || dtype.is_temporal()
        || matches!(
            dtype,
            DataType::String | DataType::Categorical(..) | DataType::Boolean
        )
}

fn parse_profiles(
    aggregate: &DataFrame,
    schema: &Schema,
    evaluated_rows: usize,
) -> Vec<ColumnQualityProfile> {
    parse_profiles_at(aggregate, schema, evaluated_rows, 0)
}

fn parse_profiles_at(
    aggregate: &DataFrame,
    schema: &Schema,
    evaluated_rows: usize,
    row: usize,
) -> Vec<ColumnQualityProfile> {
    schema
        .iter()
        .map(|(name, dtype)| {
            let prefix = format!("{}::", name);
            ColumnQualityProfile {
                name: name.to_string(),
                dtype: dtype.clone(),
                evaluated_rows,
                null_count: usize_value_at(aggregate, &format!("{prefix}null"), row),
                empty_count: optional_usize_at(aggregate, &format!("{prefix}empty"), row),
                whitespace_count: optional_usize_at(aggregate, &format!("{prefix}whitespace"), row),
                nan_count: optional_usize_at(aggregate, &format!("{prefix}nan"), row),
                positive_infinity_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}pos_inf"),
                    row,
                ),
                negative_infinity_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}neg_inf"),
                    row,
                ),
                distinct_count: optional_usize_at(aggregate, &format!("{prefix}distinct"), row),
                min: string_value_at(aggregate, &format!("{prefix}min"), row),
                max: string_value_at(aggregate, &format!("{prefix}max"), row),
                integer_parse_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}parse_int"),
                    row,
                ),
                decimal_parse_count: optional_usize_at(
                    aggregate,
                    &format!("{prefix}parse_decimal"),
                    row,
                ),
                date_parse_count: None,
                datetime_parse_count: None,
                dominant_value: None,
                dominant_count: None,
                min_length: optional_usize_at(aggregate, &format!("{prefix}min_length"), row),
                max_length: optional_usize_at(aggregate, &format!("{prefix}max_length"), row),
            }
        })
        .collect()
}

fn observations_from_profiles(columns: &[ColumnQualityProfile]) -> Vec<QualityObservation> {
    let mut observations = Vec::new();
    for profile in columns {
        if profile.null_count > 0 {
            observations.push(observation(
                ObservationKind::Nulls,
                profile,
                profile.null_count,
                format!("{:.2}% null", profile.null_rate() * 100.0),
            ));
        }
        if let Some(count) = profile.empty_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::Empty,
                profile,
                count,
                format!("{:.2}% empty", rate(count, profile.evaluated_rows) * 100.0),
            ));
        }
        if let Some(count) = profile.whitespace_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::Whitespace,
                profile,
                count,
                format!(
                    "{:.2}% whitespace only",
                    rate(count, profile.evaluated_rows) * 100.0
                ),
            ));
        }
        let non_finite = profile.nan_count.unwrap_or(0)
            + profile.positive_infinity_count.unwrap_or(0)
            + profile.negative_infinity_count.unwrap_or(0);
        if non_finite > 0 {
            observations.push(observation(
                ObservationKind::NonFinite,
                profile,
                non_finite,
                format!("{non_finite} NaN or infinite"),
            ));
        }
        if profile.distinct_count == Some(1) && profile.non_null_rows() > 0 {
            observations.push(observation(
                ObservationKind::Constant,
                profile,
                profile.non_null_rows(),
                "one non-null value".to_string(),
            ));
        }
        if let Some(parsed) = profile.decimal_parse_count
            && profile.non_null_rows() > 0
            && parsed > 0
        {
            observations.push(observation(
                ObservationKind::ParseableText,
                profile,
                parsed,
                format!(
                    "{:.2}% parse as decimal",
                    rate(parsed, profile.non_null_rows()) * 100.0
                ),
            ));
        }
        if let Some(parsed) = profile.date_parse_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::ParseableText,
                profile,
                parsed,
                format!(
                    "{:.2}% parse as ISO date",
                    rate(parsed, profile.non_null_rows()) * 100.0
                ),
            ));
        }
        if let Some(parsed) = profile.datetime_parse_count.filter(|count| *count > 0) {
            observations.push(observation(
                ObservationKind::ParseableText,
                profile,
                parsed,
                format!(
                    "{:.2}% parse as ISO datetime",
                    rate(parsed, profile.non_null_rows()) * 100.0
                ),
            ));
        }
    }
    observations
}

fn observation(
    kind: ObservationKind,
    profile: &ColumnQualityProfile,
    affected_rows: usize,
    fact: String,
) -> QualityObservation {
    QualityObservation {
        kind,
        column: profile.name.clone(),
        affected_rows,
        evaluated_rows: profile.evaluated_rows,
        fact,
        normalized_category: None,
    }
}

fn rate(numerator: usize, denominator: usize) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn optional_usize(df: &DataFrame, name: &str) -> Option<usize> {
    optional_usize_at(df, name, 0)
}

fn optional_usize_at(df: &DataFrame, name: &str, row: usize) -> Option<usize> {
    let value = df.column(name).ok()?.get(row).ok()?;
    match value {
        AnyValue::UInt32(value) => Some(value as usize),
        AnyValue::UInt64(value) => Some(value as usize),
        AnyValue::Int32(value) => usize::try_from(value).ok(),
        AnyValue::Int64(value) => usize::try_from(value).ok(),
        _ => None,
    }
}

fn usize_value(df: &DataFrame, name: &str) -> usize {
    optional_usize(df, name).unwrap_or(0)
}

fn usize_value_at(df: &DataFrame, name: &str, row: usize) -> usize {
    optional_usize_at(df, name, row).unwrap_or(0)
}

fn optional_i64_at(df: &DataFrame, name: &str, row: usize) -> Option<i64> {
    let value = df.column(name).ok()?.get(row).ok()?;
    match value {
        AnyValue::Int64(value) => Some(value),
        AnyValue::Int32(value) => Some(i64::from(value)),
        AnyValue::UInt64(value) => i64::try_from(value).ok(),
        AnyValue::UInt32(value) => Some(i64::from(value)),
        AnyValue::Float64(value) if value.is_finite() => Some(value.round() as i64),
        AnyValue::Float32(value) if value.is_finite() => Some(value.round() as i64),
        _ => None,
    }
}

fn string_value_at(df: &DataFrame, name: &str, row: usize) -> Option<String> {
    let value = df.column(name).ok()?.get(row).ok()?;
    if value.is_null() {
        None
    } else {
        Some(value.str_value().to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture() -> LazyFrame {
        df!(
            "id" => &[1i64, 2, 3, 4],
            "amount" => &[1.0f64, f64::NAN, f64::INFINITY, 4.0],
            "constant" => &["x", "x", "x", "x"],
            "text_number" => &[Some("1"), Some("2.5"), Some("bad"), None],
            "dirty" => &[Some(""), Some("   "), Some("ok"), None],
        )
        .unwrap()
        .lazy()
    }

    #[test]
    fn full_profile_reports_core_counts() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), 4, &plan, None, false).unwrap();

        assert_eq!(results.precision, QualityPrecision::Exact);
        assert_eq!(results.evaluated_rows, 4);
        let dirty = results
            .columns
            .iter()
            .find(|profile| profile.name == "dirty")
            .unwrap();
        assert_eq!(dirty.null_count, 1);
        assert_eq!(dirty.distinct_count, Some(3));
        assert_eq!(dirty.empty_count, Some(1));
        assert_eq!(dirty.whitespace_count, Some(1));

        let amount = results
            .columns
            .iter()
            .find(|profile| profile.name == "amount")
            .unwrap();
        assert_eq!(amount.nan_count, Some(1));
        assert_eq!(amount.positive_infinity_count, Some(1));

        let constant = results
            .columns
            .iter()
            .find(|profile| profile.name == "constant")
            .unwrap();
        assert_eq!(constant.distinct_count, Some(1));
        assert!(
            results
                .observations
                .iter()
                .any(|item| item.kind == ObservationKind::Constant)
        );
    }

    #[test]
    fn exact_observation_predicates_select_matching_rows() {
        let examples = [
            (ObservationKind::Nulls, "dirty", 1),
            (ObservationKind::Empty, "dirty", 1),
            (ObservationKind::Whitespace, "dirty", 1),
            (ObservationKind::NonFinite, "amount", 2),
            (ObservationKind::Constant, "constant", 4),
        ];
        for (kind, column, expected) in examples {
            let observation = QualityObservation {
                kind,
                column: column.to_string(),
                affected_rows: expected,
                evaluated_rows: 4,
                fact: String::new(),
                normalized_category: None,
            };
            let rows = fixture()
                .filter(observation.evidence_predicate().unwrap())
                .collect()
                .unwrap();
            assert_eq!(rows.height(), expected, "{}", kind.label());
        }
        let category = QualityObservation {
            kind: ObservationKind::CategoryVariants,
            column: "category".to_string(),
            affected_rows: 3,
            evaluated_rows: 3,
            fact: String::new(),
            normalized_category: Some("north".to_string()),
        };
        let rows = df!("category" => &["North", " north ", "NORTH"])
            .unwrap()
            .lazy()
            .filter(category.evidence_predicate().unwrap())
            .collect()
            .unwrap();
        assert_eq!(rows.height(), 3);
    }

    #[test]
    fn sample_is_disclosed_and_bounded() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Sample,
            sample_rows: 2,
            sample_seed: 7,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), 4, &plan, None, false).unwrap();
        assert_eq!(results.precision, QualityPrecision::Sampled);
        assert_eq!(results.total_rows, 4);
        assert_eq!(results.evaluated_rows, 2);
    }

    #[test]
    fn sample_is_seeded_without_replacement_and_does_not_invent_segment_totals() {
        let frame = DataFrame::new(
            100,
            vec![Column::new("id".into(), (0..100).collect::<Vec<_>>())],
        )
        .unwrap()
        .lazy();
        let mut plan = DataQualityPlan {
            sample_rows: 20,
            sample_seed: 1,
            grain: QualityGrain::RowChunks(10),
            ..DataQualityPlan::default()
        };
        let first = sample_quality_rows(&frame, 20, 1, false).unwrap();
        let again = sample_quality_rows(&frame, 20, 1, false).unwrap();
        let other = sample_quality_rows(&frame, 20, 2, false).unwrap();
        assert_eq!(first, again);
        assert_ne!(first, other);
        assert_eq!(first.0.column("id").unwrap().n_unique().unwrap(), 20);

        let results = compute_data_quality(&frame, 100, &plan, None, false).unwrap();
        assert!(
            results
                .segments
                .iter()
                .all(|segment| segment.total_rows.is_none())
        );
        let mut expected_chunks = BTreeMap::<usize, usize>::new();
        for position in first.1 {
            *expected_chunks.entry(position as usize / 10).or_default() += 1;
        }
        for (chunk, count) in expected_chunks {
            let label = format!("rows {}-{}", chunk * 10 + 1, (chunk + 1) * 10);
            assert_eq!(
                results
                    .segments
                    .iter()
                    .find(|segment| segment.label == label)
                    .map(|segment| segment.evaluated_rows),
                Some(count)
            );
        }
        plan.compute = QualityCompute::Full;
        let full = compute_data_quality(&frame, 100, &plan, None, false).unwrap();
        assert!(
            full.segments
                .iter()
                .all(|segment| segment.total_rows.is_some())
        );
    }

    #[test]
    fn metadata_mode_does_not_evaluate_values() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Metadata,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), 4, &plan, None, false).unwrap();
        assert_eq!(results.precision, QualityPrecision::Metadata);
        assert_eq!(results.evaluated_rows, 0);
        assert_eq!(results.columns.len(), 5);
    }

    #[test]
    fn compact_summary_keeps_plan_dimensions_visible() {
        let mut plan = DataQualityPlan::default();
        plan.set_row_chunks();
        plan.comparison = QualityComparison::Previous;
        assert_eq!(
            plan.compact_summary(),
            "scope current view -> grain 1000000 rows (physical order) -> compute 10000 rows -> compare previous"
        );
    }

    #[test]
    fn source_projection_preserves_rows_without_binary_payloads() {
        let source = QualitySourceContext {
            file_names: vec!["one.parquet".to_string()],
            file_starts: vec![0],
            row_index_column: "__datui_quality_row".to_string(),
        };
        let frame = df!(
            "value" => &[1i64, 2, 3],
            "blob" => &[&b"one"[..], &b"two"[..], &b"three"[..]],
        )
        .unwrap()
        .lazy();
        let prepared = prepare_source_quality_scan(frame, Some(&source)).unwrap();
        let collected = prepared.collect().unwrap();
        assert_eq!(collected.height(), 3);
        assert_eq!(
            collected
                .column("__datui_quality_row")
                .unwrap()
                .u32()
                .unwrap()
                .get(2),
            Some(2)
        );
        assert_eq!(
            collected.column("blob").unwrap().str().unwrap().get(0),
            Some(crate::widgets::datatable::BINARY_STUB)
        );
    }

    #[test]
    fn full_profile_accepts_list_columns() {
        let lists = Column::new(
            "items".into(),
            &[
                Series::new("".into(), &[1i32, 2]),
                Series::new("".into(), &[3i32]),
            ],
        );
        let frame = DataFrame::new(2, vec![lists]).unwrap().lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let result = compute_data_quality(&frame, 2, &plan, None, false).unwrap();
        assert_eq!(result.columns[0].null_count, 0);
        assert_eq!(result.columns[0].min_length, Some(1));
        assert_eq!(result.columns[0].max_length, Some(2));
        let sampled =
            compute_data_quality(&frame, 2, &DataQualityPlan::default(), None, false).unwrap();
        assert_eq!(sampled.columns[0].min_length, Some(1));
        assert_eq!(sampled.columns[0].max_length, Some(2));
    }

    #[test]
    fn row_chunks_keep_denominators_and_compare_previous() {
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::RowChunks(2),
            comparison: QualityComparison::Previous,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&fixture(), 4, &plan, None, false).unwrap();
        assert_eq!(results.segments.len(), 2);
        assert_eq!(results.segments[0].evaluated_rows, 2);
        let first_dirty = results.segments[0]
            .columns
            .iter()
            .find(|column| column.name == "dirty")
            .unwrap();
        let second_dirty = results.segments[1]
            .columns
            .iter()
            .find(|column| column.name == "dirty")
            .unwrap();
        assert_eq!(QualityMetric::EmptyRate.value(first_dirty), Some(0.5));
        assert_eq!(QualityMetric::EmptyRate.value(second_dirty), Some(0.0));
        assert_eq!(QualityMetric::NullRate.value(second_dirty), Some(0.5));
        assert_eq!(
            results.segments[1].compared_with.as_deref(),
            Some("rows 1-2")
        );
        assert!(results.segments[1].largest_change.is_some());

        let mut selected = results;
        let mut baseline_plan = plan.clone();
        baseline_plan.comparison = QualityComparison::Baseline;
        baseline_plan.baseline_segment = Some("rows 3-4".to_string());
        selected.compare_segments(&baseline_plan);
        assert_eq!(
            selected.segments[0].compared_with.as_deref(),
            Some("rows 3-4")
        );
        assert!(selected.segments[1].compared_with.is_none());
    }

    #[test]
    fn temporal_roles_produce_latency_without_name_inference() {
        let event = Series::new(
            "happened_at".into(),
            [Some(0i64), Some(3_600_000_000), None, Some(10_800_000_000)],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let received = Series::new(
            "landed_at".into(),
            [
                Some(3_600_000_000i64),
                Some(1_800_000_000),
                Some(7_200_000_000),
                None,
            ],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let frame = DataFrame::new(4, vec![event.into(), received.into()])
            .unwrap()
            .lazy();
        let mut plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };

        let without_roles = compute_data_quality(&frame, 4, &plan, None, false).unwrap();
        assert!(without_roles.temporal.is_empty());

        plan.temporal_roles = vec![
            TemporalRoleAssignment {
                role: TemporalRole::Event,
                column: "happened_at".to_string(),
                timezone: None,
            },
            TemporalRoleAssignment {
                role: TemporalRole::Received,
                column: "landed_at".to_string(),
                timezone: None,
            },
        ];
        let results = compute_data_quality(&frame, 4, &plan, None, false).unwrap();
        assert_eq!(results.temporal.len(), 1);
        let latency = &results.temporal[0];
        assert_eq!(latency.missing_start, 1);
        assert_eq!(latency.missing_end, 1);
        assert_eq!(latency.negative_count, 1);
        assert_eq!(latency.p50_seconds, Some(3_600));
    }

    #[test]
    fn source_row_map_produces_file_segments_without_profiling_hidden_columns() {
        let frame = df!(
            "value" => &[1i64, 2, 3, 4],
            "__row" => &[0u32, 1, 2, 3],
        )
        .unwrap()
        .lazy();
        let source = QualitySourceContext {
            file_names: vec!["a.parquet".to_string(), "b.parquet".to_string()],
            file_starts: vec![0, 2],
            row_index_column: "__row".to_string(),
        };
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::File,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, 4, &plan, Some(&source), false).unwrap();
        assert_eq!(results.columns.len(), 1);
        assert_eq!(results.segments.len(), 2);
        assert_eq!(results.segments[0].evaluated_rows, 2);
        assert!(results.segments[0].label.contains("a.parquet"));
    }

    #[test]
    fn identity_and_category_groups_keep_distinct_duplicate_semantics() {
        let frame = df!(
            "id" => &[1i64, 1, 2, 3],
            "category" => &["North", "North", " north ", "NORTH"],
        )
        .unwrap()
        .lazy();
        let plan = DataQualityPlan {
            compute: QualityCompute::Full,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, 4, &plan, None, false).unwrap();
        let identity = results.identity.unwrap();
        assert_eq!(identity.duplicate_groups, 1);
        assert_eq!(identity.extra_rows, 1);
        assert_eq!(identity.rows_involved, 2);
        assert_eq!(results.category_variants.len(), 1);
        assert_eq!(results.category_variants[0].rows_involved, 4);
        let category = results
            .columns
            .iter()
            .find(|profile| profile.name == "category")
            .unwrap();
        assert_eq!(category.dominant_value.as_deref(), Some("North"));
        assert_eq!(category.dominant_count, Some(2));
    }

    #[test]
    fn sample_identity_does_not_equate_null_with_literal_text() {
        let frame = df!("value" => &[None, Some("<null>"), None])
            .unwrap()
            .lazy();
        let plan = DataQualityPlan {
            sample_rows: 3,
            ..DataQualityPlan::default()
        };
        let results = compute_data_quality(&frame, 3, &plan, None, false).unwrap();
        let identity = results.identity.unwrap();
        assert_eq!(identity.duplicate_groups, 1);
        assert_eq!(identity.rows_involved, 2);
    }

    #[test]
    fn partition_and_time_window_grains_create_ordered_profiles() {
        let timestamps = Series::new(
            "event_at".into(),
            [0i64, 86_400_000_000, 8 * 86_400_000_000],
        )
        .cast(&DataType::Datetime(TimeUnit::Microseconds, None))
        .unwrap();
        let frame = DataFrame::new(
            3,
            vec![
                Column::new("partition".into(), ["a", "a", "b"]),
                Column::new("value".into(), [Some(1i64), None, Some(3)]),
                timestamps.into(),
            ],
        )
        .unwrap()
        .lazy();

        let partition_plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::Partition("partition".to_string()),
            ..DataQualityPlan::default()
        };
        let partitioned = compute_data_quality(&frame, 3, &partition_plan, None, false).unwrap();
        assert_eq!(partitioned.segments.len(), 2);
        assert_eq!(partitioned.segments[0].evaluated_rows, 2);

        let window_plan = DataQualityPlan {
            compute: QualityCompute::Full,
            grain: QualityGrain::TimeWindows {
                column: "event_at".to_string(),
                every: "1w".to_string(),
            },
            ..DataQualityPlan::default()
        };
        let windowed = compute_data_quality(&frame, 3, &window_plan, None, false).unwrap();
        assert_eq!(windowed.segments.len(), 2);
        assert!(windowed.segments[0].label.contains("1w"));
    }
}
