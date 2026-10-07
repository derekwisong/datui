use crate::*;
use polars::prelude::IntoLazy;
use std::sync::mpsc;

/// A cached report serves a plan that only expects other windows: Run shows it
/// under that plan, reading nothing, and the cache keeps one report for both.
#[test]
fn a_cached_report_serves_other_expected_windows() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::df!("id" => (0..100i64).collect::<Vec<_>>()).unwrap();
    app.data_table_state = Some(
        crate::table::DataTableState::new(df.clone().lazy(), None, None, None, None, true).unwrap(),
    );
    let plan = data_quality::DataQualityPlan::default();
    let report = data_quality::compute_data_quality(&df.lazy(), None, &plan, None, false).unwrap();
    app.cache_quality_result(&report, plan.clone());
    let expecting = data_quality::DataQualityPlan {
        expected: Some(data_quality::ExpectedWindows::default()),
        ..plan
    };
    assert!(app.quality_cached(&expecting));
    app.analysis_modal.quality.plan = expecting.clone();
    assert!(app.restore_cached_quality(), "no run");
    assert_eq!(
        app.analysis_modal.quality.last_plan.as_ref(),
        Some(&expecting)
    );
    assert_eq!(app.quality_cache.len(), 1);
    assert_eq!(app.quality_cache[0].plan, expecting);
}

/// Past the budget a report that retained rows can remake goes first, then the
/// oldest rows, which Setup then names as released; the newest rows and the newest
/// report stay. Rows read again are no longer released.
#[test]
fn the_budget_releases_remakeable_reports_then_the_oldest_rows() {
    let (tx, _rx) = mpsc::channel();
    let mut app = App::new(tx, crate::tests::test_runtime());
    let df = polars::df!(
        "id" => (0..2_000i64).collect::<Vec<_>>(),
        "label" => (0..2_000).map(|row| format!("row {row}")).collect::<Vec<_>>(),
    )
    .unwrap();
    app.data_table_state = Some(
        crate::table::DataTableState::new(df.clone().lazy(), None, None, None, None, true).unwrap(),
    );
    let view_generation = app.data_table_state.as_ref().unwrap().len_generation();
    let plan = |seed: u64| data_quality::DataQualityPlan {
        dataset_rows: 200,
        sample_seed: seed,
        ..data_quality::DataQualityPlan::default()
    };
    let dataset_generation = app.dataset_generation;
    let read = |seed: u64| {
        let (results, rows) = data_quality::compute_data_quality_kept(
            &df.clone().lazy(),
            None,
            &plan(seed),
            None,
            false,
            None,
        )
        .unwrap();
        let kept = KeptQualitySample {
            dataset_generation,
            view_generation,
            sample: plan(seed).sample(),
            rows: std::sync::Arc::new(rows.unwrap()),
            source: crate::quality_export::SourceIdentity::default(),
        };
        (results, kept)
    };
    let (first_report, first) = read(1);
    let (second_report, second) = read(2);
    let rows = first.rows.estimated_bytes();
    let report = first_report.estimated_bytes();
    assert!(rows > 0 && report > 0);
    // Room for two samples and one report and a half.
    app.quality_memory_budget = 2 * rows + report + report / 2;

    app.retain_quality_sample(&first);
    app.cache_quality_result(&first_report, plan(1));
    app.retain_quality_sample(&second);
    app.cache_quality_result(&second_report, plan(2));
    assert!(app.quality_cached(&plan(2)));
    assert!(
        !app.quality_cached(&plan(1)),
        "the report its rows can remake goes first"
    );
    assert!(app.quality_kept_serves(&plan(1)) && app.quality_kept_serves(&plan(2)));

    let (_, third) = read(3);
    app.retain_quality_sample(&third);
    assert!(
        !app.quality_kept_serves(&plan(1)),
        "the oldest rows go next"
    );
    assert!(app.quality_released(&plan(1)), "and Setup says so");
    assert!(app.quality_kept_serves(&plan(2)) && app.quality_kept_serves(&plan(3)));
    assert!(app.quality_cached(&plan(2)), "the newest report stays");

    app.retain_quality_sample(&first);
    assert!(
        !app.quality_released(&plan(1)),
        "read again, it is held again"
    );
    assert!(
        !app.quality_kept_serves(&plan(2)),
        "and the oldest held goes"
    );
}
