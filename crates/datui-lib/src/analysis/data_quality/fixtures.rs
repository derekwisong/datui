//! Builders the quality tests share: results measured from a frame, or put
//! together by hand from profiles and observations.

use super::*;

/// The engine's results for `lf` under `plan`, with no source to name.
pub(crate) fn measure(
    lf: &LazyFrame,
    total_rows: Option<usize>,
    plan: &DataQualityPlan,
) -> DataQualityResults {
    compute_data_quality(lf, total_rows, plan, None, false).unwrap()
}

/// A column of 100 rows with nothing measured.
pub(crate) fn profile(name: &str, dtype: DataType) -> ColumnQualityProfile {
    ColumnQualityProfile::unmeasured(name, dtype, 100)
}

/// `affected` of 100 rows of `column` observed as `kind`.
pub(crate) fn observation(
    kind: ObservationKind,
    column: &str,
    affected: usize,
) -> QualityObservation {
    QualityObservation {
        kind,
        column: column.to_string(),
        affected_rows: affected,
        evaluated_rows: 100,
        fact: String::new(),
        normalized_category: None,
        files: Vec::new(),
        time_format: None,
        full_scale: None,
    }
}

/// An exact read of 100 rows that found `columns` and `observations`.
pub(crate) fn results_with(
    columns: Vec<ColumnQualityProfile>,
    observations: Vec<QualityObservation>,
) -> DataQualityResults {
    let mut results = DataQualityResults::empty(Some(100), &Schema::default());
    results.evaluated_rows = 100;
    results.precision = QualityPrecision::Exact;
    results.columns = columns;
    results.observations = observations;
    results
}
